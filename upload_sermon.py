import os
import json
import requests
import mimetypes
import subprocess
from slugify import slugify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from boxsdk import OAuth2, Client

# ========== CONFIG ==========
SHEET_ID = "1TSlHLDGO0Dn8G0jN8Ji7lmsUc2JxxLUVTvTZfdIHAdA"
SERMON_FOLDER_ID = "332876783252"  # Box SermonUpload folder
# ============================

# ========== ENV VARS ==========
GOOGLE_SERVICE_JSON = os.environ.get("GOOGLE_SERVICE_JSON")
BOX_DEVELOPER_TOKEN = os.environ.get("BOX_DEVELOPER_TOKEN")
VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
SPREAKER_CLIENT_ID = os.environ.get("SPREAKER_CLIENT_ID")
SPREAKER_CLIENT_SECRET = os.environ.get("SPREAKER_CLIENT_SECRET")
WEBFLOW_TOKEN = os.environ.get("WEBFLOW_TOKEN")
COLLECTION_ID = os.environ.get("COLLECTION_ID")
# ============================

def get_sermon_details():
    creds_dict = json.loads(GOOGLE_SERVICE_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="A2:E2"
    ).execute()
    row = result.get('values', [])[0]
    return {
        "title": row[0],
        "date": row[1],
        "series": row[2],
        "preacher": row[3],
        "passage": row[4],
    }

def download_box_files():
    oauth = OAuth2(None, None, access_token=BOX_DEVELOPER_TOKEN)
    client = Client(oauth)
    items = client.folder(SERMON_FOLDER_ID).get_items(limit=100)
    video_file = None
    thumb_file = None
    for item in items:
        n = item.name.lower()
        if n.endswith(('.mp4', '.mov')):
            video_file = item
        if n.endswith(('.png', '.jpg', '.jpeg')):
            thumb_file = item
    if not video_file:
        raise Exception("❌ No video file in SermonUpload folder.")
    # download video
    with open(video_file.name, 'wb') as f:
        f.write(video_file.content())
    thumb_path = None
    if thumb_file:
        with open(thumb_file.name, 'wb') as f:
            f.write(thumb_file.content())
        thumb_path = thumb_file.name
    return video_file.name, thumb_path

def upload_to_vimeo(video_path, title, description):
    headers = {"Authorization": f"bearer {VIMEO_ACCESS_TOKEN}"}
    size = os.path.getsize(video_path)
    data = {"upload": {"approach": "tus", "size": size}, "name": title, "description": description}
    resp = requests.post("https://api.vimeo.com/me/videos", headers=headers, json=data)
    resp.raise_for_status()
    upload_link = resp.json()['upload']['upload_link']
    video_uri = resp.json()['uri']
    with open(video_path, 'rb') as f:
        tus_headers = {
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
            "Content-Type": "application/offset+octet-stream",
            "Authorization": f"bearer {VIMEO_ACCESS_TOKEN}"
        }
        requests.patch(upload_link, headers=tus_headers, data=f).raise_for_status()
    vresp = requests.get(f"https://api.vimeo.com{video_uri}", headers=headers)
    vresp.raise_for_status()
    vdata = vresp.json()
    video_link = vdata['link']
    pics = vdata.get('pictures', {}).get('sizes', [])
    thumb = pics[-1]['link'] if pics else None
    return video_link, thumb

def extract_audio(video_path):
    audio_path = "sermon_audio.mp3"
    subprocess.run(["ffmpeg", "-y", "-i", video_path, "-vn", "-acodec", "mp3", "-ab", "192k", "-ar", "44100", audio_path], check=True)
    return audio_path

def upload_to_spreaker(audio_path, title, description):
    auth_resp = requests.post(
        "https://api.spreaker.com/oauth2/token",
        data={"grant_type": "client_credentials"},
        auth=(SPREAKER_CLIENT_ID, SPREAKER_CLIENT_SECRET),
    )
    auth_resp.raise_for_status()
    token = auth_resp.json()['access_token']
    headers = {"Authorization": f"Bearer {token}"}
    files = {'media_file': open(audio_path, 'rb')}
    data = {'title': title, 'description': description, 'type': 'public'}
    r = requests.post("https://api.spreaker.com/v2/episodes", headers=headers, files=files, data=data)
    r.raise_for_status()
    ep = r.json()['response']['episode']
    return ep['permalink_url'], ep['episode_id']

def upload_thumbnail_to_webflow(thumbnail_path):
    site_id = "YOUR_SITE_ID"  # replace with your Webflow site ID
    headers = {"Authorization": f"Bearer {WEBFLOW_TOKEN}"}
    mime_type = mimetypes.guess_type(thumbnail_path)[0] or "image/jpeg"
    with open(thumbnail_path, 'rb') as f:
        files = {'file': (os.path.basename(thumbnail_path), f, mime_type)}
        r = requests.post(f"https://api.webflow.com/sites/{site_id}/assets", headers=headers, files=files)
        r.raise_for_status()
        return r.json()['files'][0]['url']

def create_webflow_item(details, video_link, spreaker_url, episode_id, thumb_url):
    slug = slugify(f"{details['title']}-{details['date']}")
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "1.0.0",
        "Content-Type": "application/json"
    }
    payload = {
        "fields": {
            "name": details['title'],
            "slug": slug,
            "_archived": False,
            "_draft": False,
            "series": details['series'],
            "preacher": details['preacher'],
            "sermon-passage": details['passage'],
            "sermon-date": details['date'],
            "video-link": video_link,
            "video-thumbnail": thumb_url,
            "url": spreaker_url,
            "episode-id": episode_id
        }
    }
    r = requests.post(f"https://api.webflow.com/collections/{COLLECTION_ID}/items?live=true", headers=headers, json=payload)
    r.raise_for_status()
    print("✅ Webflow item created:", r.json()['slug'])

def main():
    print("📥 Fetching sermon details from Google Sheet...")
    details = get_sermon_details()
    desc = f"{details['passage']} | {details['preacher']}"

    print("📦 Downloading from Box SermonUpload folder...")
    video_path, thumb_path = download_box_files()

    print("⬆️ Uploading video to Vimeo...")
    vimeo_link, vimeo_thumb = upload_to_vimeo(video_path, details['title'], desc)

    print("🎧 Extracting audio...")
    audio_path = extract_audio(video_path)

    print("⬆️ Uploading audio to Spreaker...")
    spreaker_url, episode_id = upload_to_spreaker(audio_path, details['title'], desc)

    if thumb_path:
        print("🖼️ Uploading custom thumbnail to Webflow...")
        thumb_url = upload_thumbnail_to_webflow(thumb_path)
    else:
        print("🖼️ Using Vimeo default thumbnail...")
        thumb_url = vimeo_thumb

    print("🌐 Creating Webflow CMS item...")
    create_webflow_item(details, vimeo_link, spreaker_url, episode_id, thumb_url)
    print("✅ Done!")

if __name__ == "__main__":
    main()
