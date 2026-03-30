import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests
from requests.auth import HTTPBasicAuth
from supabase import create_client, Client


# -----------------------------
# CONFIG
# -----------------------------
FORM_ID = "167650"
PER_PAGE = 50
MAX_PAGES = 10
LOOKBACK_HOURS = 36
REQUEST_TIMEOUT = 30

PCO_APP_ID = os.environ["PCO_APP_ID"]
PCO_SECRET = os.environ["PCO_SECRET"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]


# -----------------------------
# SETUP
# -----------------------------
auth = HTTPBasicAuth(PCO_APP_ID, PCO_SECRET)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# -----------------------------
# HELPERS
# -----------------------------
def parse_pco_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def map_person(person_id: str, person_info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "planning_center_id": person_id,
        "first_name": person_info.get("first_name", ""),
        "last_name": person_info.get("last_name", ""),
        "full_name": person_info.get("name", ""),
        "email": person_info.get("login_identifier"),
        "gender": person_info.get("gender"),
        "membership_status": person_info.get("membership"),
        "avatar_url": person_info.get("avatar"),
        "directory_status": person_info.get("directory_status"),
        "status": person_info.get("status"),
        "created_at": person_info.get("created_at"),
        "updated_at": person_info.get("updated_at"),
    }


def fetch_submission_values(submission_id: str) -> Dict[str, Any]:
    url = (
        f"https://api.planningcenteronline.com/people/v2/forms/"
        f"{FORM_ID}/form_submissions/{submission_id}/form_submission_values"
    )
    response = requests.get(url, auth=auth, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    answers: Dict[str, Any] = {
        "submission_date": None,
        "service_time": None,
        "attendance_status": None,
        "welcome_note": None,
        "prayer_request": None,
        "phone_number": None,
    }

    for value in payload.get("data", []):
        field_rel = value.get("relationships", {}).get("form_field", {}).get("data", {})
        field_id = field_rel.get("id")
        display_value = value.get("attributes", {}).get("display_value", "")

        if field_id == "1128354":
            answers["submission_date"] = display_value
        elif field_id == "1128358":
            answers["service_time"] = display_value
        elif field_id == "1128356":
            answers["attendance_status"] = display_value
        elif field_id == "1128357":
            answers["welcome_note"] = display_value
        elif field_id == "1128355":
            answers["prayer_request"] = display_value
        elif field_id == "1128353":
            answers["phone_number"] = display_value

    return answers


def find_or_create_person(
    person_id_ref: Optional[str],
    people_lookup: Dict[str, Dict[str, Any]],
    people_cache: Dict[str, Optional[str]],
) -> Optional[str]:
    if not person_id_ref:
        return None

    if person_id_ref in people_cache:
        return people_cache[person_id_ref]

    existing = (
        supabase.table("people")
        .select("id")
        .eq("planning_center_id", person_id_ref)
        .limit(1)
        .execute()
    )

    if existing.data:
        person_uuid = existing.data[0]["id"]
        people_cache[person_id_ref] = person_uuid
        return person_uuid

    person_info = people_lookup.get(person_id_ref)
    if not person_info:
        people_cache[person_id_ref] = None
        return None

    new_person = map_person(person_id_ref, person_info)
    inserted = supabase.table("people").insert(new_person).execute()
    person_uuid = inserted.data[0]["id"]

    people_cache[person_id_ref] = person_uuid
    return person_uuid


def upsert_submission(
    submission_id: str,
    created_at_str: str,
    person_uuid: Optional[str],
    answers: Dict[str, Any],
) -> None:
    row = {
        "submission_id": submission_id,
        "person_id": person_uuid,
        "submission_date": answers.get("submission_date"),
        "service_time": answers.get("service_time"),
        "attendance_status": answers.get("attendance_status"),
        "welcome_note": answers.get("welcome_note"),
        "prayer_request": answers.get("prayer_request"),
        "phone_number": answers.get("phone_number"),
        "created_at": created_at_str,
    }

    supabase.table("submissions").upsert(
        row,
        on_conflict="submission_id",
    ).execute()


# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    print(f"Starting connection card sync for the most recent {MAX_PAGES} pages...")
    print(f"Rolling lookback cutoff: {cutoff.isoformat()}")

    base_url = f"https://api.planningcenteronline.com/people/v2/forms/{FORM_ID}/form_submissions"
    people_cache: Dict[str, Optional[str]] = {}

    total_seen = 0
    total_upserted = 0
    total_skipped_old = 0

    for page_num in range(1, MAX_PAGES + 1):
        offset = (page_num - 1) * PER_PAGE
        params = {
            "order": "-created_at",
            "per_page": PER_PAGE,
            "offset": offset,
            "include": "person",
        }

        print(f"\n--- Page {page_num} ---")
        print(f"Using offset={offset}")

        response = requests.get(base_url, auth=auth, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()

        submissions = payload.get("data", [])
        included = payload.get("included", [])

        if not submissions:
            print("No submissions returned; stopping.")
            break

        people_lookup = {
            item["id"]: item.get("attributes", {})
            for item in included
            if item.get("type") == "Person"
        }

        print(f"Fetched {len(submissions)} submissions")

        first_attrs = submissions[0].get("attributes", {})
        last_attrs = submissions[-1].get("attributes", {})
        print(
            f"PAGE {page_num} RANGE: "
            f"first_created_at={first_attrs.get('created_at')} | "
            f"last_created_at={last_attrs.get('created_at')}"
        )

        page_had_recent_rows = False

        for sub in submissions:
            submission_id = sub["id"]
            created_at_str = sub.get("attributes", {}).get("created_at")
            created_at_dt = parse_pco_datetime(created_at_str)

            total_seen += 1

            if created_at_dt and created_at_dt < cutoff:
                total_skipped_old += 1
                continue

            page_had_recent_rows = True

            person_rel = (
                sub.get("relationships", {})
                .get("person", {})
                .get("data")
            )
            person_id_ref = person_rel.get("id") if person_rel else None

            try:
                answers = fetch_submission_values(submission_id)
                person_uuid = find_or_create_person(person_id_ref, people_lookup, people_cache)
                upsert_submission(submission_id, created_at_str, person_uuid, answers)
                total_upserted += 1
            except Exception as e:
                print(f"Skipping submission {submission_id}: {e}")

            time.sleep(0.05)

        if not page_had_recent_rows:
            print(f"Stopping early: page {page_num} is entirely older than the rolling cutoff.")
            break

        time.sleep(0.2)

    print("\n✅ Sync complete")
    print(f"Submissions seen: {total_seen}")
    print(f"Submissions upserted: {total_upserted}")
    print(f"Submissions skipped as older than cutoff: {total_skipped_old}")


if __name__ == "__main__":
    main()