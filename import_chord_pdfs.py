import os
import re
from pathlib import Path
from typing import List, Dict, Optional

from dotenv import load_dotenv
from pypdf import PdfReader
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CHARTS_DIR = os.getenv("CHARTS_DIR", "./charts")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

SECTION_NAME_TO_TOKEN = {
    "intro": "Intro",
    "verse 1": "V1",
    "verse 2": "V2",
    "verse 3": "V3",
    "verse 4": "V4",
    "chorus": "C",
    "bridge": "B",
    "turnaround": "Turn",
    "turn": "Turn",
    "tag": "Tag",
    "vamp": "Vamp",
    "vamp 1": "B1",
    "vamp 2": "B2",
    "interlude": "Inter",
    "inter": "Inter",
    "outro": "Outro",
    "ending": "E",
    "end": "E",
}

def response_data(response):
    if response is None:
        return None
    return getattr(response, "data", None)


def slugify(value: str) -> str:
    value = value.lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value


def clean_pdf_text(text: str) -> str:
    text = text.replace("\r", "").replace("\u00A0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_text(filepath: Path) -> str:
    reader = PdfReader(str(filepath))
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return clean_pdf_text("\n".join(parts))


def parse_header(text: str) -> Dict:
    # Example: Lamb Of God [C, 70 bpm, 4/4]
    match = re.search(r"^(.+?)\s+\[([^\]]+)\]", text, re.MULTILINE)
    if not match:
        raise ValueError("Could not find header line with [key, bpm, time signature]")

    title = match.group(1).strip()
    inside = [part.strip() for part in match.group(2).split(",")]

    original_key = inside[0] if len(inside) > 0 else "C"
    bpm_raw = inside[1] if len(inside) > 1 else ""
    time_signature = inside[2] if len(inside) > 2 else None

    bpm_match = re.search(r"([\d.]+)", bpm_raw)
    bpm = float(bpm_match.group(1)) if bpm_match else None

    return {
        "title": title,
        "slug": slugify(title),
        "original_key": original_key,
        "bpm": bpm,
        "time_signature": time_signature,
    }


def find_arrangement_line(text: str, title: str) -> str:
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    for line in lines:
        if any(token in line for token in ["Intro", "V1", "Verse", "Chorus", "B1", "Tag", "Inter"]):
            if "[" in line and "bpm" in line:
                continue

            normalized = re.sub(rf"^{re.escape(title)}\s*-\s*", "", line).strip()

            if normalized.startswith("["):
                continue
            if normalized.startswith("©"):
                continue
            if "publishing" in normalized.lower():
                continue

            if "," in normalized or "×" in normalized or "x2" in normalized.lower():
                return normalized

    raise ValueError("Could not find arrangement/order line")


def expand_arrangement(arrangement_line: str) -> List[str]:
    parts = [part.strip() for part in arrangement_line.split(",") if part.strip()]
    expanded: List[str] = []

    for part in parts:
        match = re.match(r"^(.+?)(?:[x×](\d+))?$", part, re.IGNORECASE)
        if not match:
            continue

        token = match.group(1).strip()
        count = int(match.group(2)) if match.group(2) else 1

        for _ in range(count):
            expanded.append(token)

    return expanded


def title_case(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split())


def to_display_label(cleaned: str) -> str:
    mapping = {
        "intro": "Intro",
        "chorus": "Chorus",
        "bridge": "Bridge",
        "turnaround": "Turnaround",
        "turn": "Turn",
        "tag": "Tag",
        "vamp": "Vamp",
        "vamp 1": "Vamp 1",
        "vamp 2": "Vamp 2",
        "interlude": "Interlude",
        "inter": "Interlude",
        "outro": "Outro",
        "ending": "Ending",
        "end": "Ending",
    }
    return mapping.get(cleaned, title_case(cleaned))


def normalize_section_label(label: str) -> Dict:
    cleaned = label.replace(".", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()

    if cleaned in SECTION_NAME_TO_TOKEN:
        token = SECTION_NAME_TO_TOKEN[cleaned]
        return {
            "label": to_display_label(cleaned),
            "short_label": token,
            "section_token": token,
        }

    verse_match = re.match(r"^verse\s+(\d+)$", cleaned)
    if verse_match:
        n = verse_match.group(1)
        return {
            "label": f"Verse {n}",
            "short_label": f"V{n}",
            "section_token": f"V{n}",
        }

    vamp_match = re.match(r"^vamp\s+(\d+)$", cleaned)
    if vamp_match:
        n = vamp_match.group(1)
        return {
            "label": f"Vamp {n}",
            "short_label": f"B{n}",
            "section_token": f"B{n}",
        }

    bridge_match = re.match(r"^bridge\s+(\d+)$", cleaned)
    if bridge_match:
        n = bridge_match.group(1)
        return {
            "label": f"Bridge {n}",
            "short_label": f"B{n}",
            "section_token": f"B{n}",
        }

    pretty = title_case(cleaned)
    return {
        "label": pretty,
        "short_label": pretty,
        "section_token": pretty,
    }


def is_junk_line(line: str, title: str) -> bool:
    trimmed = line.strip()
    if not trimmed:
        return True
    if re.fullmatch(r"\d+", trimmed):
        return True
    if trimmed.startswith("©"):
        return True
    if "publishing" in trimmed.lower():
        return True
    if trimmed.startswith("[") and trimmed.endswith("]"):
        return True
    if trimmed.startswith(f"{title} - "):
        return True
    if "[" in trimmed and "bpm" in trimmed:
        return True
    return False


def dedupe_and_split_bridge_variants(sections: List[Dict]) -> List[Dict]:
    result: List[Dict] = []
    seen = set()

    for section in sections:
        if section["section_token"] == "B":
            chunks = [chunk.strip() for chunk in re.split(r"\n\s*\n", section["raw_text"]) if chunk.strip()]
            if len(chunks) >= 2:
                for item in [
                    {
                        "label": "Bridge 1",
                        "short_label": "B1",
                        "section_token": "B1",
                        "raw_text": chunks[0],
                    },
                    {
                        "label": "Bridge 2",
                        "short_label": "B2",
                        "section_token": "B2",
                        "raw_text": "\n\n".join(chunks[1:]),
                    },
                ]:
                    if item["section_token"] not in seen:
                        result.append(item)
                        seen.add(item["section_token"])
                continue

        if section["section_token"] not in seen:
            result.append(section)
            seen.add(section["section_token"])

    for idx, section in enumerate(result, start=1):
        section["section_order"] = idx

    return result


def extract_sections(text: str, title: str) -> List[Dict]:
    lines = text.split("\n")
    sections: List[Dict] = []

    section_header_regex = re.compile(
        r"^(Intro|Verse(?:\.\d+|\s+\d+)?|Chorus|Bridge(?:\.\d+|\s+\d+)?|Turnaround|Turn|Tag|Vamp(?:\.\d+|\s+\d+)?|Interlude|Inter|Outro|Ending|End)\s*$",
        re.IGNORECASE,
    )

    current_header: Optional[str] = None
    current_lines: List[str] = []

    for raw_line in lines:
        line = raw_line.strip()

        if is_junk_line(line, title):
            continue

        normalized_header = re.sub(r"\s+", " ", line).strip()
        if section_header_regex.fullmatch(normalized_header):
            if current_header:
                normalized = normalize_section_label(current_header)
                raw_text = "\n".join(current_lines).strip()
                if raw_text:
                    sections.append({**normalized, "raw_text": raw_text})

            current_header = normalized_header.replace(".", " ")
            current_lines = []
            continue

        if current_header:
            current_lines.append(raw_line.rstrip())

    if current_header:
        normalized = normalize_section_label(current_header)
        raw_text = "\n".join(current_lines).strip()
        if raw_text:
            sections.append({**normalized, "raw_text": raw_text})

    return dedupe_and_split_bridge_variants(sections)


def upsert_song(meta: Dict) -> str:
    existing_resp = (
        supabase.table("songs")
        .select("id")
        .eq("slug", meta["slug"])
        .maybe_single()
        .execute()
    )
    existing = response_data(existing_resp)

    payload = {
        "title": meta["title"],
        "slug": meta["slug"],
        "original_key": meta["original_key"],
        "bpm": meta["bpm"],
        "time_signature": meta["time_signature"],
    }

    if existing:
        (
            supabase.table("songs")
            .update(payload)
            .eq("id", existing["id"])
            .execute()
        )
        return existing["id"]

    inserted_resp = supabase.table("songs").insert(payload).execute()
    inserted = response_data(inserted_resp)

    if not inserted:
        raise RuntimeError(f"Failed to insert song: {meta['title']}")

    return inserted[0]["id"]

def replace_song_sections(song_id: str, original_key: str, sections: List[Dict]) -> List[Dict]:
    supabase.table("song_sections").delete().eq("song_id", song_id).execute()

    payload = [
        {
            "song_id": song_id,
            "label": section["label"],
            "short_label": section["short_label"],
            "section_token": section["section_token"],
            "source_key": original_key,
            "raw_text": section["raw_text"],
            "section_order": section["section_order"],
        }
        for section in sections
    ]

    inserted_resp = supabase.table("song_sections").insert(payload).execute()
    inserted = response_data(inserted_resp)

    if not inserted:
        raise RuntimeError("Failed to insert song_sections")

    return sorted(inserted, key=lambda x: x["section_order"])


def upsert_default_chart_plan(song_id: str, title: str, default_key: str) -> str:
    plan_title = f"{title} - Default"

    existing_resp = (
        supabase.table("chart_plans")
        .select("id")
        .eq("song_id", song_id)
        .eq("plan_type", "arrangement")
        .eq("title", plan_title)
        .maybe_single()
        .execute()
    )
    existing = response_data(existing_resp)

    payload = {
        "title": plan_title,
        "plan_type": "arrangement",
        "song_id": song_id,
        "default_key": default_key,
    }

    if existing:
        (
            supabase.table("chart_plans")
            .update({"default_key": default_key})
            .eq("id", existing["id"])
            .execute()
        )
        return existing["id"]

    inserted_resp = supabase.table("chart_plans").insert(payload).execute()
    inserted = response_data(inserted_resp)

    if not inserted:
        raise RuntimeError(f"Failed to insert chart_plan: {plan_title}")

    return inserted[0]["id"]


def replace_chart_plan_items(
    chart_plan_id: str,
    song_id: str,
    arrangement_tokens: List[str],
    inserted_sections: List[Dict],
    default_key: str,
) -> None:
    supabase.table("chart_plan_items").delete().eq("chart_plan_id", chart_plan_id).execute()

    section_map = {s["section_token"].lower(): s for s in inserted_sections}

    items = []
    position = 1

    for raw_token in arrangement_tokens:
        token = raw_token.strip().lower()
        found = (
            section_map.get(token)
            or section_map.get(token.replace(" ", ""))
            or section_map.get("c" if token == "chorus" else token)
            or section_map.get("b" if token == "bridge" else token)
        )

        if not found:
            print(f"  Skipping plan token with no matching section: {raw_token}")
            continue

        items.append(
            {
                "chart_plan_id": chart_plan_id,
                "source_song_id": song_id,
                "source_section_id": found["id"],
                "position": position,
                "target_key": default_key,
                "transition_type": "none",
                "is_linked": True,
                "display_label": raw_token,
            }
        )
        position += 1

    if not items:
        raise RuntimeError("No chart_plan_items were created")

    inserted_resp = supabase.table("chart_plan_items").insert(items).execute()
    inserted = response_data(inserted_resp)

    if inserted is None:
        raise RuntimeError("Failed to insert chart_plan_items")

def import_pdf(filepath: Path) -> None:
    text = extract_pdf_text(filepath)

    header = parse_header(text)
    arrangement_line = find_arrangement_line(text, header["title"])
    arrangement_tokens = expand_arrangement(arrangement_line)
    sections = extract_sections(text, header["title"])

    if not sections:
        raise RuntimeError("No sections extracted")

    song_id = upsert_song(header)
    inserted_sections = replace_song_sections(song_id, header["original_key"], sections)
    chart_plan_id = upsert_default_chart_plan(song_id, header["title"], header["original_key"])
    replace_chart_plan_items(
        chart_plan_id,
        song_id,
        arrangement_tokens,
        inserted_sections,
        header["original_key"],
    )

    print(f"Imported {header['title']}")
    print(
        f"  key={header['original_key']} bpm={header['bpm']} time={header['time_signature']}"
    )
    print(f"  sections={', '.join(s['section_token'] for s in inserted_sections)}")
    print(f"  arrangement={', '.join(arrangement_tokens)}")


def main() -> None:
    charts_path = Path(CHARTS_DIR)
    pdf_files = sorted(charts_path.glob("*.pdf"))

    if not pdf_files:
        raise RuntimeError(f"No PDF files found in {charts_path}")

    for filepath in pdf_files:
        try:
            import_pdf(filepath)
        except Exception as exc:
            print(f"Failed importing {filepath.name}")
            print(exc)


if __name__ == "__main__":
    main()
    