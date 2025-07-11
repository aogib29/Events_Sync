import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from supabase import create_client, Client
from tqdm import tqdm
import uuid
import time

import os


# --- CONFIGURATION ---
FORM_ID = '167650'
PCO_APP_ID = os.environ["PCO_APP_ID"]
PCO_SECRET = os.environ["PCO_SECRET"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
PER_PAGE = 50
START_PAGE = 0  # <-- New! Start from page 100 after failure
MAX_PAGES = 5  # Pull full 581 pages!


## --- SETUP CONNECTIONS ---
auth = HTTPBasicAuth(PCO_APP_ID, PCO_SECRET)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
log_file = open("import_log.txt", "a")

# --- HELPERS ---
def map_person(person_info):
    return {
        "id": str(uuid.uuid4()),
        "planning_center_id": person_info.get('id'),
        "first_name": person_info.get('first_name', ''),
        "last_name": person_info.get('last_name', ''),
        "full_name": person_info.get('name', ''),
        "email": person_info.get('login_identifier'),
        "gender": person_info.get('gender'),
        "membership_status": person_info.get('membership'),
        "avatar_url": person_info.get('avatar'),
        "directory_status": person_info.get('directory_status'),
        "status": person_info.get('status'),
        "created_at": person_info.get('created_at'),
        "updated_at": person_info.get('updated_at')
    }

def map_submission(submission_id, person_id, answers, created_at_str):
    return {
        "submission_id": submission_id,
        "person_id": person_id,
        "submission_date": answers.get('submission_date'),
        "service_time": answers.get('service_time'),
        "attendance_status": answers.get('attendance_status'),
        "welcome_note": answers.get('welcome_note'),
        "prayer_request": answers.get('prayer_request'),
        "phone_number": answers.get('phone_number'),
        "share_with_elders_only": answers.get('share_with_elders_only'),
        "created_at": created_at_str
    }

# --- MAIN SCRIPT ---

print(f"Starting batch import from Planning Center, resuming at page {START_PAGE}...")

base_url = f"https://api.planningcenteronline.com/people/v2/forms/{FORM_ID}/form_submissions"
params = {
    'order': '-created_at',
    'per_page': PER_PAGE,
    'include': 'person'
}

page_count = 0
next_url = base_url

progress_bar = tqdm(total=MAX_PAGES - START_PAGE + 1, desc="Importing Pages")

while next_url and page_count < MAX_PAGES:
    try:
        response = requests.get(next_url, auth=auth, params=params)
        response.raise_for_status()
        data = response.json()

        submissions = data['data']
        included = data.get('included', [])

        if page_count < START_PAGE - 1:
            next_url = data['links'].get('next')
            params = {}
            page_count += 1
            continue

        people_lookup = {
            item['id']: item['attributes']
            for item in included if item['type'] == 'Person'
        }

        people_to_insert = []
        submissions_to_insert = []
        existing_people = {}

        for sub in submissions:
            submission_id = sub['id']
            created_at_str = sub['attributes']['created_at']

            # Skip if already exists
            if supabase.table('submissions').select('id').eq('submission_id', submission_id).execute().data:
                log_file.write(f"⚠️ Skipping duplicate submission {submission_id}\n")
                continue

            person_id_ref = sub['relationships'].get('person', {}).get('data', {}).get('id')
            person_info = people_lookup.get(person_id_ref, {}) if person_id_ref else {}

            if not person_info or not person_id_ref:
                log_file.write(f"⚠️ Skipping submission {submission_id}: no person info\n")
                continue

            # Resolve person in Supabase
            if person_id_ref not in existing_people:
                existing = supabase.table('people').select('id').eq('planning_center_id', person_id_ref).execute()
                if existing.data:
                    existing_people[person_id_ref] = existing.data[0]['id']
                else:
                    new_person = map_person({**person_info, "id": person_id_ref})
                    people_to_insert.append(new_person)
                    existing_people[person_id_ref] = new_person['id']

            # --- Fetch form submission values ---
            values_url = f"https://api.planningcenteronline.com/people/v2/forms/{FORM_ID}/form_submissions/{submission_id}/form_submission_values"
            values_response = requests.get(values_url, auth=auth)
            values_response.raise_for_status()
            values_data = values_response.json()

            answers = {}
            for value in values_data['data']:
                field_id = value['relationships']['form_field']['data']['id']
                display_value = value['attributes'].get('display_value', '')

                if field_id == '1128354':
                    answers['submission_date'] = display_value
                elif field_id == '1128358':
                    answers['service_time'] = display_value
                elif field_id == '1128356':
                    answers['attendance_status'] = display_value
                elif field_id == '1128357':
                    answers['welcome_note'] = display_value
                elif field_id == '1128355':
                    answers['prayer_request'] = display_value
                elif field_id == '1128353':
                    answers['phone_number'] = display_value
                elif field_id == '5027006':
                    answers['share_with_elders_only'] = display_value.strip().lower() == 'true'

            submissions_to_insert.append(
                map_submission(submission_id, existing_people[person_id_ref], answers, created_at_str)
            )

        if people_to_insert:
            print(f"Inserting {len(people_to_insert)} people on page {page_count + 1}")
            log_file.write(f"Inserting {len(people_to_insert)} people on page {page_count + 1}\n")
            supabase.table('people').insert(people_to_insert).execute()

        if submissions_to_insert:
            print(f"Inserting {len(submissions_to_insert)} submissions on page {page_count + 1}")
            log_file.write(f"Inserting {len(submissions_to_insert)} submissions on page {page_count + 1}\n")
            supabase.table('submissions').insert(submissions_to_insert).execute()

        time.sleep(0.3)  # prevent API throttling

        next_url = data['links'].get('next')
        params = {}
        page_count += 1
        progress_bar.update(1)

    except Exception as e:
        error_msg = f"❌ Error on page {page_count + 1}: {str(e)}\n"
        print(error_msg)
        log_file.write(error_msg)
        break

progress_bar.close()
log_file.close()

print("\n✅ Import complete!\n")