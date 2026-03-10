import os
import json
import re
import requests
import subprocess
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------- ENV VARS ----------------
WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")
COLLECTION_ID = "6671ed65cb61325256e73270"
SPEAKERS_COLLECTION_ID = "69a336f18b2f1d8207f72087"
SPREAKER_SHOW_ID = "2817602"
SPREAKER_ACCESS_TOKEN = os.getenv("SPREAKER_ACCESS_TOKEN")
GOOGLE_SERVICE_JSON = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
SHEET_ID = "1TSlHLDGO0Dn8G0jN8Ji7lmsUc2JxxLUVTvTZfdIHAdA"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# ---------------- GOOGLE SHEET ----------------
def get_sheet_details():
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SERVICE_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
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
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    body = {
        "values": [[item_id]]
    }

    sheet.values().update(
        spreadsheetId=SHEET_ID,
        range="I2",
        valueInputOption="RAW",
        body=body
    ).execute()

    print(f"📝 Wrote webflow_item_id to sheet: {item_id}")

# ---------------- VIMEO ----------------
def get_latest_vimeo_video():
    headers = {"Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}"}
    params = {"sort": "date", "direction": "desc", "per_page": 1}
    resp = requests.get("https://api.vimeo.com/me/videos", headers=headers, params=params)
    resp.raise_for_status()
    video = resp.json()["data"][0]
    return {
        "url": video["link"],
        "uri": video["uri"],
        "download": video.get("download", [{}])[0].get("link")
    }

# ---------------- AUDIO EXTRACTION ----------------
def extract_audio(video_url):
    print("Extracting audio...")
    video_path = "/tmp/temp_video.mp4"
    audio_path = "/tmp/sermon_audio.mp3"
    subprocess.run(["curl", "-L", video_url, "-o", video_path], check=True)
    subprocess.run([
        "ffmpeg", "-i", video_path,
        "-vn", "-acodec", "libmp3lame", "-ac", "2", "-ab", "192k", "-ar", "44100",
        audio_path
    ], check=True)
    return audio_path

# ---------------- SPREAKER ----------------
def upload_to_spreaker(audio_path, title, description):
    print("⬆️ Uploading audio to Spreaker...")
    headers = {"Authorization": f"Bearer {SPREAKER_ACCESS_TOKEN}"}
    with open(audio_path, "rb") as audio_file:
        files = {
            "media_file": audio_file,
            "title": (None, title),
            "description": (None, description)
        }
        url = f"https://api.spreaker.com/v2/shows/{SPREAKER_SHOW_ID}/episodes"
        resp = requests.post(url, headers=headers, files=files)
        resp.raise_for_status()
        episode_data = resp.json().get("response", {})
        episode_obj = episode_data.get("episode", {})
        episode_id = episode_obj.get("episode_id")

        if not episode_id:
            raise Exception(f"❌ Spreaker episode upload succeeded but episode_id not found: {episode_data}")

        episode_url = f"https://api.spreaker.com/v2/episodes/{episode_id}"
        episode_resp = requests.get(episode_url, headers=headers)
        episode_resp.raise_for_status()
        episode_info = episode_resp.json().get("response", {})
        permalink = episode_info.get("site_url") or episode_info.get("permalink_url")

        print(f"✅ Uploaded to Spreaker: {permalink} (episode_id={episode_id})")
        return permalink, episode_id

# ---------------- UTILS ----------------
def slugify(title, date_str):
    slug_title = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
    dt = datetime.strptime(date_str, "%Y-%m-%d") if '-' in date_str else datetime.strptime(date_str, "%m/%d/%Y")
    return f"{slug_title}-{dt.strftime('%Y-%m-%d')}"

def build_embed_code(title, episode_id):
    slug = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
    return f"https://www.spreaker.com/episode/{slug}--{episode_id}"

def format_sermon_date(raw_date):
    dt = datetime.strptime(raw_date, "%Y-%m-%d") if '-' in raw_date else datetime.strptime(raw_date, "%m/%d/%Y")
    return dt.strftime("%Y-%m-%dT00:00:00.000Z")

def normalize(text):
    return re.sub(r'[^a-zA-Z0-9]+', '', text).lower().strip()

# ---------------- FETCH SERIES ----------------
def fetch_series_lookup():
    url = "https://api.webflow.com/v2/collections/6671ee53d920cd99f7d8463f/items"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
    }
    print("🔄 Fetching series lookup from Webflow...")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    lookup_id = {}
    lookup_thumb = {}

    for item in data.get("items", []):
        fd = item.get("fieldData", {}) or {}
        name = fd.get("name")
        if not name:
            continue

        normalized = normalize(name)
        lookup_id[normalized] = item.get("id") or item.get("_id")

        # TRY these common image slugs on your Series collection:
        # If your Series collection uses a different field slug, we’ll adjust after one quick print.
        series_img = (
            fd.get("thumbnail")
            or fd.get("thumbnail-image")
            or fd.get("image")
            or fd.get("series-image")
            or fd.get("cover-image")
        )

        # Webflow image fields can be dicts; handle both str/dict
        if isinstance(series_img, dict):
            series_img = series_img.get("url") or series_img.get("src")
        if isinstance(series_img, str) and series_img.strip():
            lookup_thumb[normalized] = series_img.strip()

    print(f"✅ Found {len(lookup_id)} series options")
    print(f"🖼️ Found {len(lookup_thumb)} series thumbnails")
    return lookup_id, lookup_thumb

def fetch_speakers_lookup():
    """
    Returns dict: normalized speaker name -> speaker item id
    """
    # You need to set this to your Speakers collection id:
    # Grab it the same way you did for Series (or from your print collections script).

    if not SPEAKERS_COLLECTION_ID:
        raise Exception("Missing SPEAKERS_COLLECTION_ID env var")

    url = f"https://api.webflow.com/v2/collections/{SPEAKERS_COLLECTION_ID}/items"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
    }

    print("🔄 Fetching speakers lookup from Webflow...")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    lookup = {}
    for item in data.get("items", []):
        name = item.get("fieldData", {}).get("name")
        if name:
            lookup[normalize(name)] = item.get("id") or item.get("_id")

    print(f"✅ Found {len(lookup)} speaker options")
    return lookup

def create_speaker(name: str) -> str:
    """
    Create a LIVE Speaker item and return its item id.
    """
    url = f"https://api.webflow.com/v2/collections/{SPEAKERS_COLLECTION_ID}/items/live"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "Content-Type": "application/json",
        "accept-version": "2.0.0",
    }

    payload = {
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

    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    if not items:
        raise Exception(f"Speaker create returned no items: {data}")

    return items[0].get("id") or items[0].get("_id")

# ---------------- WEBFLOW ----------------
def fetch_collection_schema():
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0"
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

def build_webflow_field_data(
    title,
    slug,
    passage,
    vimeo_url,
    spreaker_url,
    episode_id,
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
        "embed-code": build_embed_code(title, episode_id),
        "episode-id": str(episode_id) if episode_id else None,
        "video-link": vimeo_url,
        # "audio-link": spreaker_url,  # Uncomment if you add/use this field
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
    vimeo_url,
    spreaker_url,
    episode_id,
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
        vimeo_url=vimeo_url,
        spreaker_url=spreaker_url,
        episode_id=episode_id,
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

    all_fields = {
        "name": title,
        "slug": slug,
        "sermon-date": format_sermon_date(sermon_date_raw),
        "description": passage,
        "preacher-2": preacher, 
        # NEW: structured fields
        "speaker": speaker_id,
        "bible-book": book,
        "thumbnail-url": thumbnail_url,

        "series-2": series_id,
        "embed-code": build_embed_code(title, episode_id),
        "episode-id": str(episode_id),
        "video-link": vimeo_url,
        # "audio-link": spreaker_url, # Uncomment/add if you want this field and it's in your schema
    }

    valid_slugs = fetch_collection_schema()
    filtered_fields = {}
    for k, v in all_fields.items():
        if k in valid_slugs:
            filtered_fields[k] = v
        else:
            print(f"⚠️ Field skipped: '{k}' not found in schema")

    data = {
        "items": [
            {
                "fieldData": filtered_fields,
                "isDraft": False,
                "isArchived": False
            }
        ]
    }

    print("🔦 Payload to Webflow (filtered):")
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
        raise Exception(f"❌ Webflow error: {resp.status_code} {resp.text}")

    return resp.json()

# ---------------- MAIN ----------------
def main():
    print("🗕 Fetching sermon details from Google Sheet...")
    details = get_sheet_details()
    vimeo = get_latest_vimeo_video()
    audio_path = extract_audio(vimeo["download"])
    spreaker_desc = f"{details['passage']} | {details['preacher']}"
    spreaker_url, episode_id = upload_to_spreaker(audio_path, details["title"], spreaker_desc)
    slug = slugify(details["title"], details["date"])
    series_lookup, series_thumb_lookup = fetch_series_lookup()
    speakers_lookup = fetch_speakers_lookup()
    normalized_speaker = normalize(details.get("preacher", ""))
    speaker_id = speakers_lookup.get(normalized_speaker)

    if not speaker_id and details.get("preacher"):
        print(f"➕ Speaker not found. Creating new Speaker: {details['preacher']}")
        speaker_id = create_speaker(details["preacher"])
        speakers_lookup[normalized_speaker] = speaker_id  # cache it

    print(f"📦 Matched speaker_id: {speaker_id}")

    print(f"📦 Matched speaker_id: {speaker_id}")
    normalized_series = normalize(details.get("series", ""))
    print(f"🔍 Normalized series from sheet: '{normalized_series}'")
    print(f"🔑 Available normalized series keys: {list(series_lookup.keys())}")
    series_id = series_lookup.get(normalized_series, None)
    series_thumb_url = series_thumb_lookup.get(normalized_series)
    # ---------------- THUMBNAIL PICKER ----------------
    thumb_mode = (details.get("thumbnail_mode") or "default").strip()
    custom_thumb = (details.get("thumbnail_url") or "").strip()

    vimeo_thumb_url = None
    try:
        # If Vimeo returned a pictures structure, use the largest
        pics = (vimeo.get("pictures") or {}).get("sizes") or []
        if pics:
            vimeo_thumb_url = pics[-1].get("link")
    except Exception:
        pass

    # Your default thumbnail url from Lists tab (optional). If you haven't wired it into code yet, leave None.
    default_thumb_url = None  # we'll wire this later if you want

    thumbnail_url = ""
    if thumb_mode == "custom_url" and custom_thumb:
        thumbnail_url = custom_thumb
    elif thumb_mode == "series_thumbnail" and series_thumb_url:
        thumbnail_url = series_thumb_url
    elif thumb_mode == "auto_vimeo" and vimeo_thumb_url:
        thumbnail_url = vimeo_thumb_url
    elif thumb_mode == "default":
        thumbnail_url = ""  # leave empty; Webflow default/conditional handles it
    else:
        # fallback: if mode was set but source missing, leave blank
        thumbnail_url = ""

    print(f"🖼️ thumb_mode={thumb_mode} -> thumbnail_url={thumbnail_url or '(blank)'}")
    print(f"📦 Matched series_id: {series_id}")
    webflow_result, final_webflow_item_id = upsert_webflow_by_sheet_id(
        details.get("webflow_item_id", ""),
        details["title"],
        slug,
        details["passage"],
        vimeo["url"],
        spreaker_url,
        episode_id,
        details["preacher"],
        series_id,
        details["date"],
        speaker_id,
        details["book"],
        thumbnail_url,
    )

    print(f"✅ Final Webflow item id: {final_webflow_item_id}")

if __name__ == "__main__":
    main()
