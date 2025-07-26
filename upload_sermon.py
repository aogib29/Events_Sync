import os
import json
import re
import requests
import subprocess
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from boxsdk import JWTAuth, Client

# ---------------- ENV VARS ----------------
WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")
COLLECTION_ID = os.getenv("COLLECTION_ID")
SPREAKER_SHOW_ID = "2817602"
SPREAKER_ACCESS_TOKEN = os.getenv("SPREAKER_ACCESS_TOKEN")
BOX_CONFIG = json.loads(os.getenv("BOX_JWT_JSON"))  # your Box JWT credentials
GOOGLE_SERVICE_JSON = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
SHEET_ID = "1TSlHLDGO0Dn8G0jN8Ji7lmsUc2JxxLUVTvTZfdIHAdA"

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
        "date": row[0],      # sermon date
        "title": row[1],     # sermon title
        "passage": row[2],   # passage
        "preacher": row[3],  # preacher
        "series": row[4]     # series name
    }

# ---------------- BOX ----------------
def get_box_client():
    auth = JWTAuth.from_settings_dictionary(BOX_CONFIG)
    client = Client(auth)
    return client

def download_box_files():
    print("📦 Downloading from Box SermonUpload folder...")
    client = get_box_client()
    folder_id = "332876783252"
    folder = client.folder(folder_id=folder_id).get()
    items = folder.get_items(limit=100)
    video_path = None
    thumb_path = None
    for item in items:
        if item.name.lower().endswith(('.mp4', '.mov')):
            video_path = f"/tmp/{item.name}"
            with open(video_path, "wb") as f:
                client.file(item.id).download_to(f)
        if item.name.lower().endswith(('.png', '.jpg', '.jpeg')):
            thumb_path = f"/tmp/{item.name}"
            with open(thumb_path, "wb") as f:
                client.file(item.id).download_to(f)
    if not video_path:
        raise Exception("No video file found in Box folder.")
    return video_path, thumb_path

# ---------------- VIMEO ----------------
def upload_to_vimeo(video_path, title, description):
    print("⬆️ Uploading video to Vimeo...")
    headers = {
        "Authorization": f"Bearer {os.getenv('VIMEO_ACCESS_TOKEN')}"
    }
    with open(video_path, 'rb') as f:
        resp = requests.post(
            "https://api.vimeo.com/me/videos",
            headers=headers,
            files={"file_data": f},
            data={"name": title, "description": description}
        )
    print("📜 Vimeo response status:", resp.status_code)
    print("📜 Vimeo response body:", resp.text)  # 👈 ADD THIS
    resp.raise_for_status()
    data = resp.json()
    vimeo_link = data.get("link")
    vimeo_thumb = None
    pictures = data.get("pictures", {})
    if "sizes" in pictures and len(pictures["sizes"]) > 0:
        vimeo_thumb = pictures["sizes"][-1]["link"]
    print(f"✅ Uploaded to Vimeo: {vimeo_link}")
    return vimeo_link, vimeo_thumb

# ---------------- AUDIO EXTRACTION ----------------
def extract_audio(video_path):
    print("🎧 Extracting audio...")
    audio_path = "/tmp/sermon_audio.mp3"
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
    data = resp.json()
    response_obj = data.get("response", {})
    # Try various keys for URL
    permalink = response_obj.get("site_url") or response_obj.get("permalink_url") or response_obj.get("download_url")
    episode_id = response_obj.get("episode_id")
    print(f"✅ Uploaded to Spreaker: {permalink} (episode_id={episode_id})")
    return permalink, episode_id

# ---------------- UTILS ----------------
def slugify(title, date_str):
    slug_title = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
    return f"{slug_title}-{dt.strftime('%Y-%m-%d')}"

def build_embed_code(title, episode_id):
    slug = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
    return f"https://www.spreaker.com/episode/{slug}--{episode_id}"

def format_sermon_date(raw_date):
    try:
        dt = datetime.strptime(raw_date, "%Y-%m-%d")
    except ValueError:
        dt = datetime.strptime(raw_date, "%m/%d/%Y")
    return dt.strftime("%Y-%m-%dT00:00:00.000Z")

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
            lookup[name.strip()] = item.get("_id")
    print(f"✅ Found {len(lookup)} series options")
    return lookup

# ---------------- WEBFLOW ----------------
def update_webflow(title, slug, passage, vimeo_url, spreaker_url, episode_id, preacher, series_id, sermon_date_raw):
    print("🌐 Updating Webflow CMS...")
    sermon_date = format_sermon_date(sermon_date_raw)
    embed_code = build_embed_code(title, episode_id)

    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items?skipInvalidFiles=true"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "Content-Type": "application/json",
        "accept-version": "2.0.0",
    }
    data = {
        "fieldData": {
            "sermon-date": sermon_date,
            "description": passage,
            "preacher-2": preacher,
            "series-2": series_id,
            "embed-code": embed_code,
            "episode-id": str(episode_id),
            "video-link": vimeo_url,
            "name": title,
            "slug": slug
        },
        "isDraft": False,
        "isArchived": False
    }
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()
    print("✅ Webflow CMS updated:", resp.json())

# ---------------- MAIN ----------------
def main():
    print("📥 Fetching sermon details from Google Sheet...")
    details = get_sheet_details()

    video_path, thumb_path = download_box_files()
    desc_for_vimeo = f"{details['passage']} | {details['preacher']}"

    vimeo_url, vimeo_thumb = upload_to_vimeo(video_path, details["title"], desc_for_vimeo)

    audio_path = extract_audio(video_path)
    spreaker_url, episode_id = upload_to_spreaker(audio_path, details["title"], details["passage"])

    slug = slugify(details["title"], details["date"])
    series_lookup = fetch_series_lookup()
    series_id = series_lookup.get(details.get("series", "").strip(), None)

    update_webflow(
        details["title"],
        slug,
        details["passage"],
        vimeo_url,
        spreaker_url,
        episode_id,
        details["preacher"],
        series_id,
        details["date"]
    )

    print("🎉 All done!")

if __name__ == "__main__":
    main()
