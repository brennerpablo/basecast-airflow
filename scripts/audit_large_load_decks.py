"""Audit the large-load status decks in raw: which pages/slides carry extractable tables and which only
charts or images. Writes docs/large-load-deck-audit.md. A quick check to size the parser, not a parser.

PDF pages (slides exported to PDF): a pdfplumber table counts only when it looks like a real table: >= 3
rows and >= 2 columns, at least half the cells filled, short cells, digits in at least a third of the
filled cells, and not covering the whole page (slide frames and chart gridlines otherwise pass as tables).
Images smaller than 10% of the page are logos and ignored. Chart data labels usually sit in the
text layer: "readable" when numbers like 42,546 appear intact, "garbled" when rotated labels come out as
spaced single digits (e.g. "4 4 4 ,3 3").
PPTX slides: native tables (``a:tbl``), native charts (``c:chart``, whose numbers live in the chart XML
cache and are extractable) and pictures.

Usage: uv run python scripts/audit_large_load_decks.py
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pdfplumber

from basecast_pipelines.common.storage import storage_from_uri
from basecast_pipelines.config import PROJECT_ROOT, load_settings
from basecast_pipelines.sources.ercot.large_load import NOT_STATUS, STATUS_DECK, normalize

OUT = PROJECT_ROOT / "docs" / "large-load-deck-audit.md"
_DIGITS = re.compile(r"\d")
_READABLE = re.compile(r"(?<![\d ])\d{1,3}(?:,\d{3})+(?![\d])")
_GARBLED = re.compile(r"(?:\b\d \d \d\b)")


def _is_real_table(table, page) -> bool:
    rows = table.extract()
    if len(rows) < 3 or max(len(r) for r in rows) < 2:
        return False
    x0, top, x1, bottom = table.bbox
    if (x1 - x0) * (bottom - top) > 0.8 * page.width * page.height:
        return False
    total = sum(len(r) for r in rows)
    cells = [c for r in rows for c in r if c]
    if not cells or len(cells) < 0.5 * total:  # chart gridlines form mostly empty "tables"
        return False
    if sorted(len(c) for c in cells)[len(cells) // 2] > 40:
        return False
    return sum(bool(_DIGITS.search(c)) for c in cells) >= len(cells) / 3


@dataclass
class PageRow:
    deck: str
    meeting_date: str
    page: str
    title: str
    has_table: bool
    note: str


def _title(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() and ln.strip().upper() != "PUBLIC"]
    return (lines[0] if lines else "")[:80]


def audit_pdf(data: bytes, deck: str, when: str) -> list[PageRow]:
    rows = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            numeric = [t for t in page.find_tables() if _is_real_table(t, page)]
            vector = len(page.curves) + len(page.rects) + len(page.lines)
            readable = len(_READABLE.findall(text))
            garbled = len(_GARBLED.findall(text))
            notes = []
            if numeric:
                notes.append(f"{len(numeric)} table(s), {sum(len(t.rows) for t in numeric)} rows")
            big_images = [im for im in page.images if im["width"] * im["height"] > 0.1 * page.width * page.height]
            if big_images:  # small images are logos
                notes.append(f"{len(big_images)} large image(s): raster content, numbers not in text")
            if vector > 40 or garbled:
                labels = "garbled (rotated)" if garbled >= 3 else "readable" if readable >= 3 else "few/none"
                notes.append(f"chart ({vector} shapes); data labels in text: {labels}")
            digits = len(_DIGITS.findall(text))
            if not notes and digits < 15:
                continue  # title, agenda or prose-only page
            if not notes:
                notes.append(f"text with numbers ({readable} values like 1,234)")
            rows.append(PageRow(deck, when, f"p{i}", _title(text), bool(numeric), "; ".join(notes)))
    return rows


def audit_pptx(data: bytes, deck: str, when: str) -> list[PageRow]:
    rows = []
    z = zipfile.ZipFile(io.BytesIO(data))
    slides = sorted(
        (n for n in z.namelist() if re.match(r"ppt/slides/slide\d+\.xml$", n)),
        key=lambda n: int(re.findall(r"\d+", n)[0]),
    )
    for name in slides:
        xml = z.read(name).decode(errors="replace")
        texts = re.findall(r"<a:t>([^<]*)</a:t>", xml)
        tables, charts, pictures = xml.count("<a:tbl>"), len(re.findall(r"<c:chart\b", xml)), xml.count("<p:pic>")
        notes = []
        if tables:
            notes.append(f"{tables} native table(s)")
        if charts:
            notes.append(f"{charts} native chart(s): data in chart XML cache")
        if pictures:
            notes.append(f"{pictures} picture(s)")
        if not notes and len(_DIGITS.findall(" ".join(texts))) < 15:
            continue
        if not notes:
            notes.append("text only")
        number = re.findall(r"\d+", name)[0]
        rows.append(PageRow(deck, when, f"slide {number}", (texts[0] if texts else "")[:80], bool(tables),
                            "; ".join(notes)))
    return rows


def audit_file(name: str, data: bytes, deck: str, when: str) -> list[PageRow]:
    lowered = name.lower()
    if lowered.endswith(".pdf"):
        return audit_pdf(data, deck, when)
    if lowered.endswith(".pptx"):
        return audit_pptx(data, deck, when)
    return [PageRow(deck, when, "-", "", False, f"format not audited ({name.rsplit('.', 1)[-1]})")]


def main() -> None:
    storage = storage_from_uri(load_settings().storage_root)
    decks = []
    for key in storage.list("raw/source=ercot_large_load_decks/"):
        if key.endswith("/_manifest.json"):
            prefix = key.removesuffix("_manifest.json")
            for entry in json.loads(storage.read_bytes(key))["entries"]:
                if entry["meta"].get("type") == "status_deck":
                    decks.append((prefix + entry["file"], entry))
    rows: list[PageRow] = []
    for key, entry in sorted(decks, key=lambda d: d[0].split("/dt=")[1]):
        when = key.split("/dt=")[1][:10]
        data = storage.read_bytes(key)
        if key.lower().endswith(".zip"):
            z = zipfile.ZipFile(io.BytesIO(data))
            for member in entry["meta"].get("zip_members") or []:
                base = member.rsplit("/", 1)[-1]
                if STATUS_DECK.search(normalize(base)) and not NOT_STATUS.search(normalize(base)):
                    rows += audit_file(member, z.read(member), f"{entry['file']} › {base}", when)
        else:
            rows += audit_file(entry["file"], data, entry["file"], when)
    write_report(rows, len(decks))


def write_report(rows: list[PageRow], deck_count: int) -> None:
    by_deck: dict[str, list[PageRow]] = {}
    for row in rows:
        by_deck.setdefault(row.deck, []).append(row)
    with_tables = [d for d, rs in by_deck.items() if any(r.has_table for r in rs)]
    native_charts = [d for d, rs in by_deck.items() if any("native chart" in r.note for r in rs)]
    lines = [
        "# Large-load status deck audit",
        "",
        "Generated by `scripts/audit_large_load_decks.py` from the status decks in "
        "`raw/source=ercot_large_load_decks/`. One row per relevant page (pages with a table, chart, image or "
        "numbers in the text layer; title, agenda and prose pages are skipped).",
        "",
        f"- Decks audited: {deck_count} files ({len(by_deck)} deck documents incl. zip members).",
        f"- Decks with at least one extractable table: {len(with_tables)}.",
        f"- Decks with native PPTX charts (numbers recoverable from chart XML): {len(native_charts)}.",
        "- PDF decks are exported slides: charts are vector drawings whose data labels sit in the text layer, "
        "readable when horizontal and garbled when rotated; garbled pages need chart-aware extraction or "
        "manual/LLM-assisted reading (with `verified=false`).",
        "",
        "## Per deck",
        "",
        "| Deck | Date (dt) | Pages kept | Tables | Native charts | Vector charts (labels readable / garbled) "
        "| Raster charts (images) | Text with numbers |",
        "|---|---|---:|---:|---:|---|---:|---:|",
    ]
    for deck, deck_rows in by_deck.items():
        notes = [r.note for r in deck_rows]
        vector_ok = sum("labels in text: readable" in n for n in notes)
        vector_bad = sum("labels in text: garbled" in n for n in notes)
        raster = sum(("large image" in n) or ("picture" in n) for n in notes)
        lines.append(
            f"| {deck} | {deck_rows[0].meeting_date} | {len(deck_rows)} | {sum(r.has_table for r in deck_rows)} "
            f"| {sum('native chart' in n for n in notes)} | {vector_ok} / {vector_bad} | {raster} "
            f"| {sum(n.startswith('text') for n in notes)} |"
        )
    lines += [
        "",
        "## Per page",
        "",
        "| Deck | Date (dt) | Page | Slide title | has_table | Note |",
        "|---|---|---|---|---|---|",
    ]
    for row in rows:
        title = row.title.replace("|", "/")
        lines.append(f"| {row.deck} | {row.meeting_date} | {row.page} | {title} | {'yes' if row.has_table else 'no'} | "
                     f"{row.note} |")
    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT} ({len(rows)} rows, {len(by_deck)} decks)")


if __name__ == "__main__":
    main()
