import os
import json
import re
import requests
import subprocess
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from webflow import Webflow, CollectionItem, CollectionItemFieldData


# ---------------- ENV VARS ----------------
WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")
COLLECTION_ID = os.getenv("COLLECTION_ID")
SPREAKER_SHOW_ID = "2817602"
SPREAKER_ACCESS_TOKEN = os.getenv("SPREAKER_ACCESS_TOKEN")
GOOGLE_SERVICE_JSON = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
SHEET_ID = "1TSlHLDGO0Dn8G0jN8Ji7lmsUc2JxxLUVTvTZfdIHAdA"
VIMEO_ACCESS_TOKEN = os.getenv("VIMEO_ACCESS_TOKEN")

# ---------------- GOOGLE SHEET ----------------
def get_sheet_details():
    creds = service_account.Credentials.from_service_account_info(GOOGLE_SERVICE_JSON, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SHEET_ID, range="A2:E2").execute()
    values = result.get("values", [])
    if not values:
        raise Exception("No data found in sheet")
    row = values[0]
    return {
        "date": row[0],
        "title": row[1],
        "passage": row[2],
        "preacher": row[3],
        "series": row[4]
    }

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
    files = {
        "media_file": open(audio_path, "rb"),
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
    lookup = {}
    for item in data.get("items", []):
        name = item.get("fieldData", {}).get("name")
        if name:
            normalized = normalize(name)
            lookup[normalized] = item.get("id") or item.get("_id")
    print(f"✅ Found {len(lookup)} series options")
    return lookup

# ---------------- DEBUG FIELD SLUGS ----------------
def fetch_collection_schema():
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    fields = [f['slug'] for f in data.get('fieldDefinitions', [])]
    print("🧹 Webflow collection field slugs:")
    for f in fields:
        print(f"- {f}")
    return set(normalize(f) for f in fields)

# ---------------- WEBFLOW ----------------
def update_webflow(title, slug, passage, vimeo_url, spreaker_url, episode_id, preacher, series_id, sermon_date_raw):
    print("🌐 Updating Webflow CMS...")
    sermon_date = format_sermon_date(sermon_date_raw)
    embed_code = build_embed_code(title, episode_id)

    client = Webflow(access_token=WEBFLOW_TOKEN)

    item_data = CollectionItem(
        is_draft=False,
        is_archived=False,
        field_data=CollectionItemFieldData(
            **{
                "name": title,
                "slug": slug,
                "sermon-date": sermon_date,
                "description": passage,
                "preacher-2": preacher,
                "series-2": series_id,
                "embed-code": embed_code,
                "episode-id": str(episode_id),
                "video-link": vimeo_url
            }
        )
    )

    try:
        result = client.collections.items.create_item_live(
            collection_id=COLLECTION_ID,
            request=item_data
        )
        print("✅ Webflow CMS updated:", result)
    except Exception as e:
        print("❌ Webflow error:", e)
        raise


# ---------------- MAIN ----------------
def main():
    print("🗕 Fetching sermon details from Google Sheet...")
    details = get_sheet_details()
    vimeo = get_latest_vimeo_video()
    audio_path = extract_audio(vimeo["download"])
    spreaker_desc = f"{details['passage']} | {details['preacher']}"
    spreaker_url, episode_id = upload_to_spreaker(audio_path, details["title"], spreaker_desc)
    slug = slugify(details["title"], details["date"])
    series_lookup = fetch_series_lookup()
    normalized_series = normalize(details.get("series", ""))
    print(f"🔍 Normalized series from sheet: '{normalized_series}'")
    print(f"🔑 Available normalized series keys: {list(series_lookup.keys())}")
    series_id = series_lookup.get(normalized_series, None)
    print(f"📦 Matched series_id: {series_id}")
    update_webflow(
        details["title"],
        slug,
        details["passage"],
        vimeo["url"],
        spreaker_url,
        episode_id,
        details["preacher"],
        series_id,
        details["date"]
    )
    print("🎉 All done!")

if __name__ == "__main__":
    main()
