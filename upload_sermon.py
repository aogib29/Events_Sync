import os
import json
import io
import requests
import subprocess
from googleapiclient.discovery import build
from google.oauth2 import service_account
from boxsdk import JWTAuth, Client

# ✅ Spreaker show ID (replace with your own)
SPREAKER_SHOW_ID = "2817602"  # e.g. "12345678"

# ✅ Load env vars from GitHub Secrets
SPREAKER_CLIENT_ID = os.environ.get("SPREAKER_CLIENT_ID")
SPREAKER_CLIENT_SECRET = os.environ.get("SPREAKER_CLIENT_SECRET")
SPREAKER_ACCESS_TOKEN = os.environ.get("SPREAKER_ACCESS_TOKEN")
SPREAKER_REFRESH_TOKEN = os.environ.get("SPREAKER_REFRESH_TOKEN")

VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
WEBFLOW_TOKEN = os.environ.get("WEBFLOW_TOKEN")
COLLECTION_ID = os.environ.get("COLLECTION_ID")

BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_ENTERPRISE_ID = os.environ.get("BOX_ENTERPRISE_ID")
BOX_JWT_KEY_ID = os.environ.get("BOX_JWT_KEY_ID")
BOX_JWT_PRIVATE_KEY = os.environ.get("BOX_JWT_PRIVATE_KEY")
BOX_JWT_PASSPHRASE = os.environ.get("BOX_JWT_PASSPHRASE")
BOX_FOLDER_ID = "332876783252"  # SermonUpload folder

GOOGLE_SERVICE_JSON = os.environ.get("GOOGLE_SERVICE_JSON")

# ✅ Google Sheet details
SHEET_ID = "1TSlHLDGO0Dn8G0jN8Ji7lmsUc2JxxLUVTvTZfdIHAdA"
SHEET_RANGE = "A2:D2"  # adjust to your row/columns


def get_sheet_details():
    print("📥 Fetching sermon details from Google Sheet...")
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_SERVICE_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds)
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=SHEET_RANGE)
        .execute()
    )
    values = result.get("values", [])
    if not values or not values[0]:
        raise RuntimeError("No details found in Google Sheet")
    row = values[0]
    # Expected columns: Title, Preacher, Passage, Date
    return {
        "title": row[0],
        "preacher": row[1],
        "passage": row[2],
        "date": row[3],
    }


def get_box_client():
    auth = JWTAuth(
        client_id=BOX_CLIENT_ID,
        client_secret=BOX_CLIENT_SECRET,
        enterprise_id=BOX_ENTERPRISE_ID,
        jwt_key_id=BOX_JWT_KEY_ID,
        rsa_private_key_data=BOX_JWT_PRIVATE_KEY.replace("\\n", "\n"),
        rsa_private_key_passphrase=BOX_JWT_PASSPHRASE,
    )
    client = Client(auth)
    return client


def download_box_files():
    print("📦 Downloading from Box SermonUpload folder...")
    client = get_box_client()
    folder = client.folder(folder_id=BOX_FOLDER_ID).get()
    items = folder.get_items(limit=100)
    video_path = None
    thumb_path = None
    for item in items:
        name = item.name.lower()
        if name.endswith(".mp4") or name.endswith(".mov"):
            with open("sermon_video.mp4", "wb") as f:
                f.write(item.content())
            video_path = "sermon_video.mp4"
        elif name.endswith(".jpg") or name.endswith(".jpeg") or name.endswith(".png"):
            with open("thumbnail_image", "wb") as f:
                f.write(item.content())
            thumb_path = "thumbnail_image"
    if not video_path:
        raise RuntimeError("No video file found in Box SermonUpload folder.")
    return video_path, thumb_path


def upload_to_vimeo(video_path, title, description):
    print("⬆️ Uploading video to Vimeo...")
    headers = {
        "Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.vimeo.*+json;version=3.4",
    }

    # Create upload ticket
    create_resp = requests.post(
        "https://api.vimeo.com/me/videos",
        headers=headers,
        json={
            "upload": {"approach": "tus", "size": os.path.getsize(video_path)},
            "name": title,
            "description": description,
        },
    )
    create_resp.raise_for_status()
    upload_link = create_resp.json()["upload"]["upload_link"]
    vimeo_link = create_resp.json()["link"]
    vimeo_thumb = create_resp.json()["pictures"]["sizes"][0]["link"]

    # Upload video binary
    with open(video_path, "rb") as f:
        tus_headers = {
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
            "Content-Type": "application/offset+octet-stream",
        }
        tus_headers.update({"Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}"})
        resp = requests.patch(upload_link, headers=tus_headers, data=f)
        resp.raise_for_status()

    print(f"✅ Uploaded to Vimeo: {vimeo_link}")
    return vimeo_link, vimeo_thumb


def extract_audio(video_path):
    print("🎧 Extracting audio...")
    audio_output = "sermon_audio.mp3"
    subprocess.run([
        "ffmpeg",
        "-i", video_path,
        "-vn",
        "-acodec", "libmp3lame",
        "-ar", "44100",
        "-ac", "2",
        "-b:a", "192k",
        audio_output
    ], check=True)
    return audio_output


def refresh_spreaker_token():
    print("🔄 Refreshing Spreaker access token...")
    resp = requests.post(
        "https://api.spreaker.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": SPREAKER_CLIENT_ID,
            "client_secret": SPREAKER_CLIENT_SECRET,
            "refresh_token": SPREAKER_REFRESH_TOKEN,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def upload_to_spreaker(audio_path, title, description):
    print("⬆️ Uploading audio to Spreaker...")
    headers = {"Authorization": f"Bearer {SPREAKER_ACCESS_TOKEN}"}
    files = {
        "media_file": open(audio_path, "rb"),
        "title": (None, title),
        "description": (None, description),
    }
    url = f"https://api.spreaker.com/v2/shows/{SPREAKER_SHOW_ID}/episodes"
    resp = requests.post(url, headers=headers, files=files)

    if resp.status_code != 200:
        print("❌ Upload failed:", resp.status_code, resp.text)
    else:
        data = resp.json()
        permalink = data["response"]["site_url"]
        episode_id = data["response"]["episode"]["episode_id"]
        print(f"✅ Uploaded to Spreaker: {permalink}")
        return permalink, episode_id


def update_webflow(title, slug, description, vimeo_url, spreaker_url, thumb_url):
    print("🌐 Updating Webflow CMS...")
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {
        "fields": {
            "name": title,
            "slug": slug,
            "description": description,
            "vimeo-url": vimeo_url,
            "spreaker-url": spreaker_url,
            "thumbnail-url": thumb_url,
            "_archived": False,
            "_draft": False,
        }
    }
    resp = requests.post(
        f"https://api.webflow.com/collections/{COLLECTION_ID}/items",
        headers=headers,
        json=data,
    )
    resp.raise_for_status()
    print("✅ Webflow CMS updated.")


def slugify(title, date):
    import re
    base = f"{title}-{date}"
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", base.lower()).strip("-")
    return slug


def main():
    details = get_sheet_details()
    video_path, thumb_path = download_box_files()
    desc = f"{details['passage']} | {details['preacher']}"
    vimeo_url, vimeo_thumb = upload_to_vimeo(video_path, details["title"], desc)
    audio_path = extract_audio(video_path)
    spreaker_url, _ = upload_to_spreaker(audio_path, details["title"], desc)

    slug = slugify(details["title"], details["date"])
    thumb_final = vimeo_thumb
    if thumb_path:
        # You could optionally upload a custom thumb to Vimeo if needed
        pass

    update_webflow(
        details["title"],
        slug,
        desc,
        vimeo_url,
        spreaker_url,
        thumb_final
    )

    print("🎉 All done!")


if __name__ == "__main__":
    main()
