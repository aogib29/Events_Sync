import requests
from datetime import datetime, timezone
from dateutil import parser
from html.parser import HTMLParser
from slugify import slugify

import os

PCO_APP_ID = os.environ["PCO_APP_ID"]
PCO_SECRET = os.environ["PCO_SECRET"]
WEBFLOW_TOKEN = os.environ["WEBFLOW_TOKEN"]
COLLECTION_ID = os.environ["COLLECTION_ID"]
WEBFLOW_API_BASE = "https://api.webflow.com/v2"

# ==== HEADERS ====
webflow_headers = {
    "Authorization": f"Bearer {WEBFLOW_TOKEN}",
    "Content-Type": "application/json",
    "accept-version": "2.0.0"
}

# ==== HELPERS ====
class CleanHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.result = []
        self.href = None

    def handle_starttag(self, tag, attrs):
        if tag == "br":
            self.result.append("\n")
        elif tag == "a":
            for attr in attrs:
                if attr[0] == "href":
                    self.href = attr[1]

    def handle_endtag(self, tag):
        if tag == "a" and self.href:
            self.result.append(f" ({self.href})")
            self.href = None

    def handle_data(self, data):
        self.result.append(data)

    def get_data(self):
        return ''.join(self.result).strip()

def clean_description(html_text):
    if not html_text:
        return "—"
    parser = CleanHTMLParser()
    parser.feed(html_text)
    return parser.get_data()

def build_slug(name, starts_at):
    date_part = starts_at.split("T")[0]  # "YYYY-MM-DD"
    return slugify(f"{name}-{date_part}")

def get_webflow_item_by_slug(slug):
    url = f"{WEBFLOW_API_BASE}/collections/{COLLECTION_ID}/items"
    res = requests.get(url, headers=webflow_headers)
    res.raise_for_status()
    for item in res.json().get("items", []):
        if item["fieldData"]["slug"] == slug:
            return item
    return None

def fetch_visible_pco_events():
    url = "https://api.planningcenteronline.com/calendar/v2/events?filter=future&per_page=100"
    res = requests.get(url, auth=(PCO_APP_ID, PCO_SECRET))
    res.raise_for_status()
    events = res.json().get("data", [])
    return [e for e in events if e["attributes"]["visible_in_church_center"]]

def fetch_first_instance(event_id):
    url = f"https://api.planningcenteronline.com/calendar/v2/events/{event_id}/event_instances"
    res = requests.get(url, auth=(PCO_APP_ID, PCO_SECRET))
    res.raise_for_status()
    now = datetime.now(timezone.utc)
    for inst in res.json().get("data", []):
        starts_at = parser.isoparse(inst["attributes"]["starts_at"])
        if starts_at > now:
            return inst["attributes"]
    return None

def create_or_update_item(event, instance, slug):
    payload = {
        "fieldData": {
            "name": event["attributes"]["name"],
            "slug": slug,
            "start-date-time": instance["starts_at"],
            "end-date-time": instance["ends_at"],
            "location": instance.get("location") or "",
            "description": clean_description(event["attributes"].get("description", "")),
            "short-description": event["attributes"].get("summary", ""),
            "image": event["attributes"].get("image_url", ""),
            "rsvp-link": instance.get("church_center_url", ""),
        }
    }

    existing_item = get_webflow_item_by_slug(slug)

    if existing_item:
        item_id = existing_item["id"]
        url = f"{WEBFLOW_API_BASE}/collections/{COLLECTION_ID}/items/{item_id}/live"
        res = requests.patch(url, headers=webflow_headers, json=payload)
        if res.status_code in [200, 201]:
            print(f"🔁 Updated: {payload['fieldData']['name']}")
        else:
            print(f"❌ Failed to update {slug}: {res.status_code} - {res.text}")
    else:
        url = f"{WEBFLOW_API_BASE}/collections/{COLLECTION_ID}/items/live?skipInvalidFiles=true"
        res = requests.post(url, headers=webflow_headers, json=payload)
        if res.status_code in [200, 201]:
            print(f"✅ Created: {payload['fieldData']['name']}")
        else:
            print(f"❌ Failed to create {slug}: {res.status_code} - {res.text}")

def run():
    events = fetch_visible_pco_events()
    for event in events:
        instance = fetch_first_instance(event["id"])
        if not instance:
            continue
        slug = build_slug(event["attributes"]["name"], instance["starts_at"])
        create_or_update_item(event, instance, slug)

if __name__ == "__main__":
    run()
    