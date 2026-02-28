import os, requests

WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")
COLLECTION_ID = "6671ed65cb61325256e73270"

resp = requests.get(
  f"https://api.webflow.com/v2/collections/{COLLECTION_ID}",
  headers={"Authorization": f"Bearer {WEBFLOW_TOKEN}", "accept-version": "2.0.0"},
)
resp.raise_for_status()
data = resp.json()

print("Field slugs:")
for f in data.get("fields", []):
  print("-", f.get("slug"))