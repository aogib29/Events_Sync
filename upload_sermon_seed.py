import os
import json
import re
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------- ENV VARS ----------------
WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")
COLLECTION_ID = "6671ed65cb61325256e73270"
SPEAKERS_COLLECTION_ID = "69a336f18b2f1d8207f72087"
GOOGLE_SERVICE_JSON = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
SHEET_ID = os.getenv("SHEET_ID", "1TSlHLDGO0Dn8G0jN8Ji7lmsUc2JxxLUVTvTZfdIHAdA")

# ---------------- GOOGLE SHEET ----------------
def get_sheet_details():
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SERVICE_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SHEET_ID, range="A2:I2").execute()
    values = result.get("values", [])
    if not values:
        raise Exception("No data found in sheet")

    row = values[0]
    row = row + [""] * (9 - len(row))

    return {
        "date": row[0],
        "title": row[1],
        "passage": row[2],
        "preacher": row[3],
        "series": row[4],
        "book": row[5],
        "thumbnail_mode": row[6],
        "thumbnail_url": row[7],
        "webflow_item_id": row[8].strip(),
    }


def write_webflow_item_id_to_sheet(item_id: str):
    if not item_id:
        return

    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SERVICE_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    body = {"values": [[item_id]]}

    sheet.values().update(
        spreadsheetId=SHEET_ID,
        range="I2",
        valueInputOption="RAW",
        body=body,
    ).execute()

    print(f"📝 Wrote webflow_item_id to sheet: {item_id}")


# ---------------- UTILS ----------------
def slugify(title, date_str):
    slug_title = re.sub(r"[^a-zA-Z0-9]+", "-", title.lower()).strip("-")
    dt = datetime.strptime(date_str, "%Y-%m-%d") if "-" in date_str else datetime.strptime(date_str, "%m/%d/%Y")
    return f"{slug_title}-{dt.strftime('%Y-%m-%d')}"


def normalize(value):
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def format_sermon_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d") if "-" in date_str else datetime.strptime(date_str, "%m/%d/%Y")
    return dt.strftime("%Y-%m-%dT12:00:00.000Z")


# ---------------- WEBFLOW LOOKUPS ----------------
def fetch_collection_schema():
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    print("🧹 Webflow collection field slugs (raw):")
    slugs = set()
    for field in data.get("fields", []):
        slug = field.get("slug")
        print(f"- {slug}")
        slugs.add(slug)
    return slugs


def fetch_speakers_lookup():
    url = f"https://api.webflow.com/v2/collections/{SPEAKERS_COLLECTION_ID}/items"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    lookup = {}
    for item in data.get("items", []):
        name = item.get("fieldData", {}).get("name", "")
        lookup[normalize(name)] = item.get("id")
    return lookup


def fetch_series_lookup():
    url = "https://api.webflow.com/v2/collections/686d9f8060fb26d4840bc28c/items"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    lookup = {}
    thumb_lookup = {}
    for item in data.get("items", []):
        field_data = item.get("fieldData", {})
        name = field_data.get("name", "")
        lookup[normalize(name)] = item.get("id")
        thumb_lookup[normalize(name)] = field_data.get("thumbnail-url", "")
    return lookup, thumb_lookup


def create_speaker(name):
    print(f"🌐 Creating new speaker in Webflow: {name}")
    url = f"https://api.webflow.com/v2/collections/{SPEAKERS_COLLECTION_ID}/items/live"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "Content-Type": "application/json",
        "accept-version": "2.0.0",
    }
    data = {
        "items": [
            {
                "fieldData": {
                    "name": name,
                    "slug": re.sub(r"[^a-zA-Z0-9]+", "-", name.lower()).strip("-"),
                },
                "isDraft": False,
                "isArchived": False,
            }
        ]
    }

    resp = requests.post(url, headers=headers, json=data)
    print("Speaker create status:", resp.status_code)
    print(resp.text)
    if not resp.ok:
        raise Exception(f"❌ Speaker create failed: {resp.status_code} {resp.text}")

    result = resp.json()
    items = result.get("items", [])
    if not items:
        raise Exception(f"❌ Speaker create returned no items: {result}")

    speaker_id = items[0].get("id") or items[0].get("_id")
    if not speaker_id:
        raise Exception(f"❌ Speaker create returned item without id: {result}")

    return speaker_id


# ---------------- WEBFLOW UPSERT ----------------
def build_webflow_field_data(
    title,
    slug,
    passage,
    preacher,
    series_id,
    sermon_date_raw,
    speaker_id,
    book,
    thumbnail_url,
):
    all_fields = {
        "name": title,
        "slug": slug,
        "sermon-date": format_sermon_date(sermon_date_raw),
        "description": passage,
        "preacher-2": preacher,
        "speaker": speaker_id,
        "bible-book": book,
        "thumbnail-url": thumbnail_url,
        "series-2": series_id,
    }

    valid_slugs = fetch_collection_schema()
    filtered_fields = {}

    for k, v in all_fields.items():
        if k in valid_slugs and v not in (None, ""):
            filtered_fields[k] = v
        elif k not in valid_slugs:
            print(f"⚠️ Field skipped: '{k}' not found in schema")

    return filtered_fields


def create_webflow_item_live(field_data):
    print("🌐 Creating NEW live Webflow sermon item...")

    data = {
        "items": [
            {
                "fieldData": field_data,
                "isDraft": False,
                "isArchived": False,
            }
        ]
    }

    print("🔦 Create payload to Webflow:")
    print(json.dumps(data, indent=2))

    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items/live"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "Content-Type": "application/json",
        "accept-version": "2.0.0",
    }

    resp = requests.post(url, headers=headers, json=data)
    print("Status:", resp.status_code)
    print(resp.text)

    if not resp.ok:
        raise Exception(f"❌ Webflow create error: {resp.status_code} {resp.text}")

    result = resp.json()
    items = result.get("items", [])
    if not items:
        raise Exception(f"❌ Webflow create returned no items: {result}")

    created_id = items[0].get("id") or items[0].get("_id")
    if not created_id:
        raise Exception(f"❌ Webflow create returned item without id: {result}")

    return result, created_id


def update_webflow_item_live(item_id, field_data):
    print(f"🌐 Updating EXISTING live Webflow sermon item: {item_id}")

    data = {
        "items": [
            {
                "id": item_id,
                "fieldData": field_data,
                "isDraft": False,
                "isArchived": False,
            }
        ]
    }

    print("🔦 Update payload to Webflow:")
    print(json.dumps(data, indent=2))

    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items/live"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "Content-Type": "application/json",
        "accept-version": "2.0.0",
    }

    resp = requests.patch(url, headers=headers, json=data)
    print("Status:", resp.status_code)
    print(resp.text)

    if not resp.ok:
        raise Exception(f"❌ Webflow update error: {resp.status_code} {resp.text}")

    return resp.json()


def upsert_webflow_by_sheet_id(
    webflow_item_id,
    title,
    slug,
    passage,
    preacher,
    series_id,
    sermon_date_raw,
    speaker_id,
    book,
    thumbnail_url,
):
    field_data = build_webflow_field_data(
        title=title,
        slug=slug,
        passage=passage,
        preacher=preacher,
        series_id=series_id,
        sermon_date_raw=sermon_date_raw,
        speaker_id=speaker_id,
        book=book,
        thumbnail_url=thumbnail_url,
    )

    if webflow_item_id:
        return update_webflow_item_live(webflow_item_id, field_data), webflow_item_id

    result, created_id = create_webflow_item_live(field_data)
    write_webflow_item_id_to_sheet(created_id)
    return result, created_id


# ---------------- RESOLVE METADATA ----------------
def resolve_sermon_metadata(details):
    slug = slugify(details["title"], details["date"])

    series_lookup, series_thumb_lookup = fetch_series_lookup()
    speakers_lookup = fetch_speakers_lookup()

    normalized_speaker = normalize(details.get("preacher", ""))
    speaker_id = speakers_lookup.get(normalized_speaker)

    if not speaker_id and details.get("preacher"):
        print(f"➕ Speaker not found. Creating new Speaker: {details['preacher']}")
        speaker_id = create_speaker(details["preacher"])
        speakers_lookup[normalized_speaker] = speaker_id

    print(f"📦 Matched speaker_id: {speaker_id}")

    normalized_series = normalize(details.get("series", ""))
    print(f"🔍 Normalized series from sheet: '{normalized_series}'")
    print(f"🔑 Available normalized series keys: {list(series_lookup.keys())}")

    series_id = series_lookup.get(normalized_series, None)
    series_thumb_url = series_thumb_lookup.get(normalized_series)

    thumb_mode = (details.get("thumbnail_mode") or "default").strip()
    custom_thumb = (details.get("thumbnail_url") or "").strip()

    thumbnail_url = ""
    if thumb_mode == "custom_url" and custom_thumb:
        thumbnail_url = custom_thumb
    elif thumb_mode == "series_thumbnail" and series_thumb_url:
        thumbnail_url = series_thumb_url
    elif thumb_mode == "default":
        thumbnail_url = ""
    else:
        thumbnail_url = ""

    print(f"🖼️ thumb_mode={thumb_mode} -> thumbnail_url={thumbnail_url or '(blank)'}")
    print(f"📦 Matched series_id: {series_id}")

    return {
        "slug": slug,
        "speaker_id": speaker_id,
        "series_id": series_id,
        "thumbnail_url": thumbnail_url,
    }


# ---------------- MAIN ----------------
def main():
    print("🌱 Seed-only mode starting...")
    print("🗓 Fetching sermon details from Google Sheet...")
    details = get_sheet_details()

    resolved = resolve_sermon_metadata(details)

    webflow_result, final_webflow_item_id = upsert_webflow_by_sheet_id(
        details.get("webflow_item_id", ""),
        details["title"],
        resolved["slug"],
        details["passage"],
        details["preacher"],
        resolved["series_id"],
        details["date"],
        resolved["speaker_id"],
        details["book"],
        resolved["thumbnail_url"],
    )

    print(f"✅ Seed complete. Webflow item id: {final_webflow_item_id}")
    print("🌱 Seed-only mode finished.")


if __name__ == "__main__":
    main()