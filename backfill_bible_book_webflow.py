import os
import re
import requests

WEBFLOW_TOKEN = os.getenv("WEBFLOW_TOKEN")
SERMONS_COLLECTION_ID = "6671ed65cb61325256e73270"  # Sermons collection

API_BASE = "https://api.webflow.com/v2"

# Set DRY_RUN=1 to print changes without writing
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"


def wf_headers():
    return {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept-version": "2.0.0",
        "Content-Type": "application/json",
    }


# Canonical Webflow option values (must match your dropdown options exactly)
CANONICAL_BOOKS = [
    "Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth",
    "1 Samuel","2 Samuel","1 Kings","2 Kings","1 Chronicles","2 Chronicles","Ezra","Nehemiah",
    "Esther","Job","Psalms","Proverbs","Ecclesiastes","Song of Solomon","Isaiah","Jeremiah",
    "Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah","Nahum",
    "Habakkuk","Zephaniah","Haggai","Zechariah","Malachi","Matthew","Mark","Luke","John","Acts",
    "Romans","1 Corinthians","2 Corinthians","Galatians","Ephesians","Philippians","Colossians",
    "1 Thessalonians","2 Thessalonians","1 Timothy","2 Timothy","Titus","Philemon","Hebrews",
    "James","1 Peter","2 Peter","1 John","2 John","3 John","Jude","Revelation",
]

# Aliases / abbreviations you might have in "description" (passage)
ALIASES = {
    # Psalms
    "ps": "Psalms", "psalm": "Psalms", "psalms": "Psalms",
    # Song of Solomon
    "song": "Song of Solomon", "song of songs": "Song of Solomon", "song of solomon": "Song of Solomon",
    # John / 1 John etc (we handle numbered separately too)
    # Corinthians etc
    "cor": "Corinthians",
    "thess": "Thessalonians",
    "tim": "Timothy",
    "pet": "Peter",
    "jn": "John",
    "rev": "Revelation",
    "gen": "Genesis", "ex": "Exodus", "lev": "Leviticus", "num": "Numbers", "deut": "Deuteronomy",
    "josh": "Joshua", "judg": "Judges", "neh": "Nehemiah", "esth": "Esther",
    "prov": "Proverbs", "eccl": "Ecclesiastes", "isa": "Isaiah", "jer": "Jeremiah", "lam": "Lamentations",
    "ezek": "Ezekiel", "dan": "Daniel", "hos": "Hosea", "hab": "Habakkuk", "zeph": "Zephaniah",
    "zech": "Zechariah", "mal": "Malachi", "matt": "Matthew", "mk": "Mark", "lk": "Luke",
    "rom": "Romans", "gal": "Galatians", "eph": "Ephesians", "phil": "Philippians", "col": "Colossians",
    "titus": "Titus", "philem": "Philemon", "heb": "Hebrews", "jas": "James", "jude": "Jude",
}


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())


def extract_book(passage: str) -> str | None:
    """
    Try to infer a canonical Bible book from a 'passage' string like:
    - 'Isaiah 59'
    - '1 John 4:7-21'
    - 'Song of Solomon 2:1'
    - 'Ps 23'
    - 'John 3:16'
    """
    if not passage:
        return None

    s = normalize_spaces(passage)
    s_lower = s.lower()

    # Remove leading/trailing junk
    s_lower = re.sub(r"^[^\w\d]+", "", s_lower)
    s_lower = re.sub(r"[^\w\d: -]+$", "", s_lower)

    # Special multi-word books first
    multi_word = [
        ("song of solomon", "Song of Solomon"),
        ("song of songs", "Song of Solomon"),
        ("1 samuel", "1 Samuel"),
        ("2 samuel", "2 Samuel"),
        ("1 kings", "1 Kings"),
        ("2 kings", "2 Kings"),
        ("1 chronicles", "1 Chronicles"),
        ("2 chronicles", "2 Chronicles"),
        ("1 corinthians", "1 Corinthians"),
        ("2 corinthians", "2 Corinthians"),
        ("1 thessalonians", "1 Thessalonians"),
        ("2 thessalonians", "2 Thessalonians"),
        ("1 timothy", "1 Timothy"),
        ("2 timothy", "2 Timothy"),
        ("1 peter", "1 Peter"),
        ("2 peter", "2 Peter"),
        ("1 john", "1 John"),
        ("2 john", "2 John"),
        ("3 john", "3 John"),
    ]
    for k, v in multi_word:
        if s_lower.startswith(k + " ") or s_lower == k:
            return v

    # Handle numbered books like "1 Cor", "2 Tim", "1 Thess", etc.
    m = re.match(r"^(1|2|3)\s+([a-zA-Z]+)", s)
    if m:
        num = m.group(1)
        word = m.group(2).lower()

        # Expand common abbrev word -> canonical base
        base = ALIASES.get(word, None)
        if base in ("Corinthians","Thessalonians","Timothy","Peter","John"):
            candidate = f"{num} {base}"
            if candidate in CANONICAL_BOOKS:
                return candidate

        # If someone wrote "1 Samuel" / "2 Kings" etc. but we missed due to casing
        candidate2 = f"{num} {word.capitalize()}"
        # Not perfect; we rely on the multi-word list above for most.
        if candidate2 in CANONICAL_BOOKS:
            return candidate2

    # Otherwise take the first word(s) up to the first digit/colon
    # Example: "Isaiah 59" -> "isaiah"
    # Example: "John 3:16" -> "john"
    # Example: "Psalms 23" -> "psalms"
    head = re.split(r"\s+\d|:\d", s_lower)[0].strip()
    head = normalize_spaces(head)

    # Some people might type just "Psalm" or "Ps"
    if head in ALIASES:
        return ALIASES[head]

    # Direct canonical match by name
    # Compare lowercased canonical list
    for b in CANONICAL_BOOKS:
        if head == b.lower():
            return b

    # Last chance: handle "ps" without space, etc.
    head_simple = re.sub(r"[^a-z0-9 ]+", "", head).strip()
    if head_simple in ALIASES:
        return ALIASES[head_simple]

    return None


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
        if not next_cursor:
            break
        params = {"limit": 100, "cursor": next_cursor}

    return items


def patch_live_items(collection_id: str, updates):
    url = f"{API_BASE}/collections/{collection_id}/items/live"
    payload = {"items": updates}
    resp = requests.patch(url, headers=wf_headers(), json=payload)
    resp.raise_for_status()
    return resp.json()


def main():
    if not WEBFLOW_TOKEN:
        raise SystemExit("Missing WEBFLOW_TOKEN")

    print("Loading live sermons...")
    sermons = list_live_items(SERMONS_COLLECTION_ID)
    print(f"Found {len(sermons)} sermons")

    updates = []
    misses = 0

    for s in sermons:
        sid = s.get("id")
        fd = s.get("fieldData") or {}

        # only fill if empty
        if fd.get("bible-book"):
            continue

        passage = (fd.get("description") or "").strip()
        book = extract_book(passage)

        if not book:
            misses += 1
            continue

        # Must be one of the option values
        if book not in CANONICAL_BOOKS:
            misses += 1
            continue

        print(f"- {sid}: '{passage}' -> bible-book='{book}'")
        updates.append({"id": sid, "fieldData": {"bible-book": book}})

    print(f"\nWould update {len(updates)} sermons. Misses/unparsed: {misses}. DRY_RUN={DRY_RUN}")

    if DRY_RUN:
        print("Dry run enabled; not writing.")
        return

    for i in range(0, len(updates), 100):
        chunk = updates[i : i + 100]
        patch_live_items(SERMONS_COLLECTION_ID, chunk)
        print(f"✅ Updated batch {i//100 + 1} ({len(chunk)} items)")

    print("Done.")


if __name__ == "__main__":
    main()