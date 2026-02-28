import os
import requests

WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")

# Your Sermons collection id (matches your upload script)
COLLECTION_ID = "6671ed65cb61325256e73270"

# Exact replacements you approved
REPLACEMENTS = {
    "Joshua de Koning": "Josh de Koning",
    "Shamus": "Shamus Drake",
    "Dr Andy Snider": "Andy Snider",
}

API_BASE = "https://api.webflow.com/v2"


def wf_headers():
    return {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
        "Content-Type": "application/json",
    }


def list_live_items():
    """
    Fetch all LIVE items in the collection.
    Webflow responses typically include pagination; we follow next links/tokens if present.
    """
    url = f"{API_BASE}/collections/{COLLECTION_ID}/items/live"
    items = []
    params = {"limit": 100}

    while True:
        resp = requests.get(url, headers=wf_headers(), params=params)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("items", []) or []
        items.extend(batch)

        # Handle pagination patterns (Webflow may vary)
        pagination = data.get("pagination") or {}
        next_cursor = pagination.get("nextCursor") or pagination.get("next_cursor")
        if next_cursor:
            params = {"limit": 100, "cursor": next_cursor}
            continue

        next_page = pagination.get("nextPage") or pagination.get("next_page")
        if next_page:
            url = next_page
            params = None
            continue

        # Some APIs use offset/total
        offset = pagination.get("offset")
        total = pagination.get("total")
        limit = pagination.get("limit", 100)
        if offset is not None and total is not None:
            new_offset = offset + limit
            if new_offset < total:
                params = {"limit": limit, "offset": new_offset}
                continue

        break

    return items


def patch_live_items(updates):
    """
    Bulk PATCH up to 100 items at a time.
    """
    url = f"{API_BASE}/collections/{COLLECTION_ID}/items/live"
    payload = {"items": updates}
    resp = requests.patch(url, headers=wf_headers(), json=payload)
    resp.raise_for_status()
    return resp.json()


def main():
    if not WEBFLOW_TOKEN:
        raise SystemExit("Missing WEBFLOW_TOKEN env var.")

    print("Fetching live sermon items...")
    items = list_live_items()
    print(f"Found {len(items)} live items.")

    to_update = []

    for it in items:
        item_id = it.get("id")
        field_data = it.get("fieldData") or {}
        current = field_data.get("preacher-2")  # <-- preacher field slug from your system

        if not current:
            continue

        if current in REPLACEMENTS:
            new_val = REPLACEMENTS[current]
            print(f"- Will update {item_id}: '{current}' -> '{new_val}'")
            to_update.append(
                {
                    "id": item_id,
                    "fieldData": {
                        "preacher-2": new_val,
                    },
                }
            )

    if not to_update:
        print("No matching preacher values found. Nothing to update.")
        return

    # Send in chunks of 100
    print(f"Updating {len(to_update)} items...")
    for i in range(0, len(to_update), 100):
        chunk = to_update[i : i + 100]
        patch_live_items(chunk)
        print(f"✅ Updated batch {i//100 + 1} ({len(chunk)} items)")

    print("Done.")


if __name__ == "__main__":
    main()