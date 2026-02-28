import os
import re
import requests

WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")

SERMONS_COLLECTION_ID = "6671ed65cb61325256e73270"  # sermons
SPEAKERS_COLLECTION_ID = os.getenv("SPEAKERS_COLLECTION_ID")  # <-- put in GitHub Action env

API_BASE = "https://api.webflow.com/v2"


def wf_headers():
    return {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
        "Content-Type": "application/json",
    }


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip())


def slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "speaker"


def list_live_items(collection_id: str):
    url = f"{API_BASE}/collections/{collection_id}/items/live"
    items = []
    params = {"limit": 100}

    while True:
        resp = requests.get(url, headers=wf_headers(), params=params)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("items", []) or []
        items.extend(batch)

        pagination = data.get("pagination") or {}
        next_cursor = pagination.get("nextCursor") or pagination.get("next_cursor")
        if next_cursor:
            params = {"limit": 100, "cursor": next_cursor}
            continue

        # If no cursor pagination, stop.
        break

    return items


def patch_live_items(collection_id: str, updates):
    url = f"{API_BASE}/collections/{collection_id}/items/live"
    payload = {"items": updates}
    resp = requests.patch(url, headers=wf_headers(), json=payload)
    resp.raise_for_status()
    return resp.json()


def create_speaker(name: str):
    """
    Create a live Speaker item with name + slug.
    """
    url = f"{API_BASE}/collections/{SPEAKERS_COLLECTION_ID}/items/live"
    payload = {
        "items": [
            {
                "fieldData": {
                    "name": name,
                    "slug": slugify(name),
                },
                "isDraft": False,
                "isArchived": False,
            }
        ]
    }
    resp = requests.post(url, headers=wf_headers(), json=payload)
    resp.raise_for_status()
    data = resp.json()
    # v2 returns created item(s); handle both shapes defensively
    if isinstance(data, dict) and "items" in data and data["items"]:
        return data["items"][0]["id"]
    if isinstance(data, dict) and "id" in data:
        return data["id"]
    raise RuntimeError(f"Unexpected create response: {data}")


def main():
    if not WEBFLOW_TOKEN:
        raise SystemExit("Missing WEBFLOW_TOKEN.")
    if not SPEAKERS_COLLECTION_ID:
        raise SystemExit("Missing SPEAKERS_COLLECTION_ID (set it in Action env).")

    print("Loading Speakers...")
    speakers = list_live_items(SPEAKERS_COLLECTION_ID)
    speaker_by_name = {}
    for sp in speakers:
        fd = sp.get("fieldData") or {}
        nm = fd.get("name")
        if nm:
            speaker_by_name[normalize_name(nm)] = sp.get("id")

    print(f"Found {len(speaker_by_name)} existing speakers.")

    print("Loading Sermons...")
    sermons = list_live_items(SERMONS_COLLECTION_ID)
    print(f"Found {len(sermons)} live sermons.")

    updates = []
    created = 0
    linked = 0
    skipped = 0

    for s in sermons:
        sid = s.get("id")
        fd = s.get("fieldData") or {}

        # Current preacher text field (your existing canonical source)
        preacher = fd.get("preacher-2")  # <- this is the text preacher field your scripts already use :contentReference[oaicite:2]{index=2}
        if not preacher:
            skipped += 1
            continue

        preacher = normalize_name(preacher)

        # Skip if already has a speaker reference set
        # NOTE: we auto-detect the speaker reference field slug below if needed.
        # For now we assume the new Sermons reference field slug is "speaker".
        # If your slug is different, you’ll change SPEAKER_REF_SLUG below after one quick schema check.
        SPEAKER_REF_SLUG = "speaker"
        if fd.get(SPEAKER_REF_SLUG):
            skipped += 1
            continue

        speaker_id = speaker_by_name.get(preacher)
        if not speaker_id:
            speaker_id = create_speaker(preacher)
            speaker_by_name[preacher] = speaker_id
            created += 1
            print(f"✅ Created Speaker: {preacher} ({speaker_id})")

        updates.append(
            {
                "id": sid,
                "fieldData": {
                    SPEAKER_REF_SLUG: speaker_id,
                },
            }
        )
        linked += 1

    if not updates:
        print("Nothing to link. Done.")
        return

    print(f"Linking {linked} sermons to speakers (created {created} new speakers, skipped {skipped})...")

    for i in range(0, len(updates), 100):
        chunk = updates[i : i + 100]
        patch_live_items(SERMONS_COLLECTION_ID, chunk)
        print(f"✅ Linked batch {i//100 + 1} ({len(chunk)} sermons)")

    print("Done.")


if __name__ == "__main__":
    main()