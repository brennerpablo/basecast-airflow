"""PUCT Interchange filings of the large-load dockets (58777, 59772, 58481): the TSP large-load request table
of 58777 item 38, one row per filed document, and page text for the commercial module.

Each item has a ZIP (the filer's native files) and one or more PDFs (PUCT's rendition of those files, with a
"Filing Receipt" first page). Page text comes from the renditions; ZIP members that PUCT could not convert
(PPTX) are read from the ZIP; PDF and DOCX members are skipped because the rendition carries the same text
(checked on a 12-item sample). The filings index (``<control>_filings.xlsx``, newest per docket) gives each
item's filed date, filing party and description.

Personal data: filing parties that are individuals are not stored (only organization names are), their
names are removed from descriptions, and the text of their filings is not stored. Emails and phone numbers
are removed from all text.

- ``puct_tsp_large_load_requests``: 58777 item 38, "Attachment A" slides 4-5 (native PPTX tables) and the
  same pages of the matching PDF (pdfplumber tables): TSP large-load RFI submissions by load type and by TSP,
  2026-2032. These are requests, not a forecast.
- ``puct_filing_documents``: one row per raw document (PDF or ZIP).
- ``document_pages``: shared with ``ercot_large_load_decks``.
"""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from collections import Counter
from datetime import date
from pathlib import PurePosixPath

import pdfplumber
import polars as pl

from basecast_pipelines.parsers._documents import (
    PAGES_DESCRIPTION,
    PAGES_SCHEMA,
    DocInfo,
    ExtractedDoc,
    extract,
    flatten,
    frame,
    iter_shapes,
    page_rows,
    pptx_slides,
    scrub,
    slide_title,
    table_grid,
)
from basecast_pipelines.processing.core import Dataset, RawFile, list_raw_files
from basecast_pipelines.processing.tabular import excel_date, find_header_row, read_grid, with_header

SOURCE_ID = "puct_filings"
log = logging.getLogger(__name__)

_DOC_NAME = re.compile(r"^(?P<control>\d+)_(?P<item>\d+)_(?P<doc>\d+)\.(?P<ext>\w+)$")


def _is_document(f: RawFile) -> bool:
    return f.meta.get("kind") == "document" and f.suffix in {".pdf", ".zip"}


def _ids(f: RawFile) -> tuple[str | None, int | None, str | None]:
    match = _DOC_NAME.match(f.name)
    control = str(f.meta.get("control_number") or (match.group("control") if match else "")) or None
    item = f.meta.get("item")
    item = int(item) if item is not None else int(match.group("item")) if match else None
    return control, item, match.group("doc") if match else None


# --- filing parties and personal data ------------------------------------------------------------------

_ORG_WORDS = re.compile(
    r"\b(?:llc|l\.l\.c|inc|corp|corporation|company|co|lp|l\.p|ltd|llp|cooperative|coop|association|coalition|"
    r"council|committee|alliance|foundation|club|fund|group|partners|partnership|energy|power|electric|utilities|"
    r"utility|companies|transmission|infrastructure|datacenters?|data|digital|holdings|economics|resources|city|cities|county|"
    r"puc|puct|ercot|opuc|authority|services?|technologies|technology|systems|labs?|network|consulting|royalties|"
    r"development|petroleum|chemical|oil|gas|steel|mills|ventures|capital|affairs|commission|department|office|"
    r"bureau|university|institute|projects?|rules|market|analysis|director|staff|center|centers|america|usa|"
    r"operations|solutions|industries|strategies|associates|equities|geology|works|agency|board|texas|tx|ai)\b",
    re.IGNORECASE,
)
_HONORIFIC = re.compile(r"\b(?:senator|representative|rep\.|judge|commissioner|mr\.?|mrs\.?|ms\.?|dr\.?)\s", re.IGNORECASE)
_LEGAL_SUFFIX = re.compile(r"^(?:inc\.?|llc\.?|l\.l\.c\.?|l\.p\.?|lp|ltd\.?|corp\.?|co\.?|\(us\)|\(?[A-Z]{2,6}\)?)$",
                           re.IGNORECASE)
_NOT_NAMES = {"state", "senator", "representative", "judge", "commissioner", "the", "and", "for"}
_NAME_TOKEN = re.compile(r"^[A-Z][a-zA-Z'’-]*\.?$|^[A-Z]\.?$")


def _looks_like_person(segment: str) -> bool:
    """2-4 capitalized words, no organization word, no digits: "Jason Buster", "THOMAS L DARTE"."""
    if _HONORIFIC.search(segment + " "):
        return True
    words = segment.split()
    if not 2 <= len(words) <= 4 or re.search(r"\d", segment) or _ORG_WORDS.search(segment):
        return False
    return all(_NAME_TOKEN.match(w) or _NAME_TOKEN.match(w.title()) for w in words)


def classify_party(raw: str | None) -> tuple[str | None, bool, list[str]]:
    """(organization name to store, filed by an individual?, person segments to scrub elsewhere)."""
    text = flatten(raw or "")
    if not text:
        return None, False, []
    segments: list[str] = []
    for part in re.split(r"\s*[,/|;]\s*", text):
        if segments and part and _LEGAL_SUFFIX.match(part):
            segments[-1] = f"{segments[-1]}, {part}"  # "Cholla, Inc." / "SHELL ENERGY NORTH AMERICA, (US), L.P."
        elif part:
            segments.append(part)
    persons = [s for s in segments if _looks_like_person(s)]
    orgs = [s for s in segments if s not in persons]
    if persons:
        # Whatever else a person listed ("EnerShield AI") is kept only when it reads as an organization.
        orgs = [s for s in orgs if _ORG_WORDS.search(s) or re.search(r"\d", s)]
    return (", ".join(orgs) or None), bool(persons), persons


def scrub_names(text: str | None, persons: list[str]) -> str | None:
    """Replace the listed people's names (and each of their name words) with "[individual]"."""
    if not text or not persons:
        return text
    for person in sorted(persons, key=len, reverse=True):
        text = re.sub(re.escape(person), "[individual]", text, flags=re.IGNORECASE)
    words = {w.strip(".") for p in persons for w in _HONORIFIC.sub("", p + " ").split()
             if len(w.strip(".")) >= 3 and w.strip(".").lower() not in _NOT_NAMES}
    for word in sorted(words, key=len, reverse=True):
        text = re.sub(rf"\b{re.escape(word)}\b", "[individual]", text, flags=re.IGNORECASE)
    return re.sub(r"(?:\[individual\]\s*){2,}", "[individual] ", text).strip()


# --- the filings index -------------------------------------------------------------------------------

_INDEX_CACHE: dict[tuple[str, ...], dict[tuple[str, int], dict]] = {}


def filing_index(storage, source: str = SOURCE_ID) -> dict[tuple[str, int], dict]:
    """(docket, item) → filed date, filing party and description, from the newest index of each docket."""
    newest: dict[str, RawFile] = {}
    for f in list_raw_files(storage, source):  # oldest first
        if f.meta.get("kind") == "filings_index":
            newest[str(f.meta.get("control_number"))] = f
    cache_key = tuple(sorted(f.sha256 for f in newest.values()))
    if cache_key in _INDEX_CACHE:
        return _INDEX_CACHE[cache_key]
    index: dict[tuple[str, int], dict] = {}
    for f in newest.values():
        index.update(parse_index(f))
    _INDEX_CACHE[cache_key] = index
    return index


def parse_index(f: RawFile) -> dict[tuple[str, int], dict]:
    with f.local_path() as path:
        grid = read_grid(path, 0)
    header = find_header_row(grid, [r"control", r"item", r"description"])
    if header is None:
        raise ValueError(f"{f.key}: no Control # / Item # / Filing Description header")
    body = with_header(grid, header)
    control = next(c for c in body.columns if c.startswith("control"))
    item = next(c for c in body.columns if c.startswith("item"))
    stamp = next((c for c in body.columns if "stamp" in c or "filed" in c or "date" in c), None)
    party = next((c for c in body.columns if "party" in c), None)
    desc = next(c for c in body.columns if "description" in c)
    rows = body.select(
        pl.col(control).str.strip_chars().alias("docket"),
        pl.col(item).str.strip_chars().cast(pl.Int32, strict=False).alias("item"),
        (excel_date(stamp) if stamp else pl.lit(None, pl.Date)).alias("filed_date"),
        (pl.col(party) if party else pl.lit(None, pl.String)).alias("party"),
        pl.col(desc).alias("description"),
    ).filter(pl.col("item").is_not_null())
    return {(r["docket"], r["item"]): r for r in rows.iter_rows(named=True)}


_RECEIPT_DATE = re.compile(r"Filed Date\s*-\s*(\d{4}-\d{2}-\d{2})")


def _filing(f: RawFile, doc: ExtractedDoc | None = None) -> dict:
    """What the index says about this document's item, with personal data handled."""
    docket, item, doc_id = _ids(f)
    entry = filing_index(f.storage, f.source).get((docket, item), {}) if docket and item is not None else {}
    org, individual, persons = classify_party(entry.get("party"))
    filed = entry.get("filed_date")
    if filed is None and doc is not None and doc.pages:
        match = _RECEIPT_DATE.search(doc.pages[0].text)
        filed = date.fromisoformat(match.group(1)) if match else None
    description = flatten(entry.get("description") or "") or None
    return {
        "docket": docket,
        "item": item,
        "document_id": doc_id,
        "filed_date": filed,
        "filing_party": org,
        "filed_by_individual": individual,
        "description": scrub(scrub_names(description, persons)),
    }


# --- documents -------------------------------------------------------------------------------------------


def _members(f: RawFile) -> list[zipfile.ZipInfo]:
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        return [i for i in z.infolist() if not i.is_dir()]


def _zip_documents(f: RawFile) -> list[tuple[str, str, ExtractedDoc | None, int | None]]:
    """(member, kind, extracted doc or None, page count) of a ZIP: PPTX members are extracted (the rendition
    PDF cannot hold them); PDF members are only counted (their text is in the rendition)."""
    out = []
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        for info in z.infolist():
            lowered = info.filename.lower()
            if lowered.endswith(".pptx"):
                doc = extract("pptx", z.read(info), cache_key=(f.key, f.sha256, info.filename))
                out.append((info.filename, "pptx", doc, doc.page_count))
            elif lowered.endswith(".pdf"):
                try:
                    with pdfplumber.open(io.BytesIO(z.read(info))) as pdf:
                        pages = len(pdf.pages)
                except Exception:  # noqa: BLE001 - a damaged member only loses its page count
                    pages = None
                out.append((info.filename, "pdf", None, pages))
    return out


def _page_ranges(numbers: list[int]) -> str | None:
    """[10, 41, 62, 63, 64] → "10, 41, 62-64"."""
    if not numbers:
        return None
    ranges, start, prev = [], numbers[0], numbers[0]
    for n in numbers[1:] + [None]:
        if n is not None and n == prev + 1:
            prev = n
            continue
        ranges.append(f"{start}-{prev}" if prev != start else str(start))
        if n is not None:
            start = prev = n
    return ", ".join(ranges)


DOCUMENTS_SCHEMA: dict[str, pl.DataType] = {
    "docket": pl.String,
    "item": pl.Int32,
    "document_id": pl.String,
    "file_name": pl.String,
    "file_type": pl.String,
    "content_type": pl.String,
    "bytes": pl.Int64,
    "url": pl.String,
    "filed_date": pl.Date,
    "filing_party": pl.String,
    "filed_by_individual": pl.Boolean,
    "description": pl.String,
    "pages": pl.Int32,
    "pages_extracted": pl.Int32,
    "pages_without_text": pl.Int32,
    "pages_without_text_list": pl.String,
    "n_chars": pl.Int64,
    "has_text_layer": pl.Boolean,
    "truncated": pl.Boolean,
    "text_stored": pl.Boolean,
    "zip_members": pl.Int32,
    "zip_member_types": pl.String,
    "error": pl.String,
}


def parse_documents(f: RawFile) -> pl.DataFrame | None:
    base = {
        "file_name": f.name,
        "file_type": f.suffix.lstrip("."),
        "content_type": f.entry.content_type,
        "bytes": f.entry.bytes,
        "url": f.url,
    }
    if f.suffix == ".pdf":
        with f.local_path() as path:
            doc = extract("pdf", path, cache_key=(f.key, f.sha256, ""))
        filing = _filing(f, doc)
        empty = [p.number for p in doc.pages if not p.text]
        n_chars = doc.n_chars
        row = {
            **base, **filing,
            "pages": doc.page_count,
            "pages_extracted": len(doc.pages),
            "pages_without_text": len(empty),
            "pages_without_text_list": _page_ranges(empty),
            "n_chars": n_chars,
            "has_text_layer": n_chars > 0,
            "truncated": doc.truncated,
            "text_stored": n_chars > 0 and not filing["filed_by_individual"],
            "zip_members": None,
            "zip_member_types": None,
            "error": doc.error,
        }
        return frame([row], DOCUMENTS_SCHEMA)
    # ZIP
    filing = _filing(f)
    try:
        members = _members(f)
        documents = _zip_documents(f)
        error = None
    except zipfile.BadZipFile as exc:
        members, documents, error = [], [], f"BadZipFile: {exc}"
    extracted = [d for _, _, d, _ in documents if d is not None]
    n_chars = sum(d.n_chars for d in extracted)
    pages = [n for _, _, _, n in documents if n is not None]
    types = Counter(PurePosixPath(i.filename).suffix.lower().lstrip(".") or "none" for i in members)
    row = {
        **base, **filing,
        "pages": sum(pages) if pages else None,
        "pages_extracted": sum(len(d.pages) for d in extracted),
        "pages_without_text": None,
        "pages_without_text_list": None,
        "n_chars": n_chars,
        "has_text_layer": None,
        "truncated": any(d.truncated for d in extracted),
        "text_stored": n_chars > 0 and not filing["filed_by_individual"],
        "zip_members": len(members),
        "zip_member_types": json.dumps(dict(sorted(types.items()))),
        "error": error,
    }
    return frame([row], DOCUMENTS_SCHEMA)


def parse_pages(f: RawFile) -> pl.DataFrame | None:
    if f.suffix == ".pdf":
        with f.local_path() as path:
            docs = [(None, extract("pdf", path, cache_key=(f.key, f.sha256, "")))]
        filing = _filing(f, docs[0][1])
    else:
        filing = _filing(f)
        docs = [(m, d) for m, _, d, _ in _zip_documents(f) if d is not None]
    if filing["filed_by_individual"]:
        return None  # personal data: filings by individuals are not stored as text
    rows = []
    for member, doc in docs:
        info = DocInfo(
            source_id=SOURCE_ID, doc_type="puct_filing", file_name=f.name, member=member,
            title=filing["description"], document_date=filing["filed_date"], url=f.url,
            docket=filing["docket"], item=filing["item"],
        )
        rows += page_rows(info, doc)
    return frame(rows, PAGES_SCHEMA)


# --- TSP large-load requests (58777 item 38, Attachment A) ------------------------------------------

_TSP_TITLE = re.compile(r"TSP\s+Large\s+Load\s+RFI\s+Submissions\s+by\s+(TSP|Load\s+Type)", re.IGNORECASE)
_YEAR = re.compile(r"^(20\d{2})$")


def rows_from_grid(grid: list[list[str | None]]) -> list[tuple[str, int, float, bool]]:
    """(name, year, MW, is_total) from a table whose header row is a label then years; "-" means none (0).
    Raises when a cell under a year is not a number."""
    cells = [[flatten(re.sub(r"-\s*\n\s*", "-", c or "")) for c in row] for row in grid]  # "non-\ncrypto"
    header_at = next((i for i, row in enumerate(cells) if sum(bool(_YEAR.match(c)) for c in row[1:]) >= 3), None)
    if header_at is None:
        raise ValueError("no header row with years")
    years = {j: int(c) for j, c in enumerate(cells[header_at]) if j > 0 and _YEAR.match(c)}
    out = []
    for row in cells[header_at + 1:]:
        name = row[0] if row else ""
        if not name:
            continue
        for j, year in years.items():
            raw = row[j] if j < len(row) else ""
            if raw in {"-", "–", "—", ""}:
                mw = 0.0
            else:
                try:
                    mw = float(raw.replace(",", ""))
                except ValueError:
                    raise ValueError(f"{name} {year}: not a number: {raw!r}") from None
            out.append((name, year, mw, bool(re.fullmatch(r"total", name, re.IGNORECASE))))
    return out


def _breakdown(title: str) -> str:
    return "tsp" if _TSP_TITLE.search(title).group(1).upper() == "TSP" else "type"


def _pptx_tables(data: bytes) -> list[tuple[int, str, list[list[str]]]]:
    out = []
    for number, slide in enumerate(pptx_slides(data), 1):
        title = slide_title(slide) or ""
        if not _TSP_TITLE.search(title):
            continue
        for shape in iter_shapes(slide.shapes):
            if getattr(shape, "has_table", False) and shape.has_table:
                out.append((number, title, table_grid(shape)))
    return out


def _pdf_tables(data: bytes) -> list[tuple[int, str, list[list[str]]]]:
    out = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for number, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            match = _TSP_TITLE.search(text)
            if match:
                for table in page.extract_tables():
                    out.append((number, flatten(match.group(0)), table))
            page.close()
    return out


REQUESTS_SCHEMA: dict[str, pl.DataType] = {
    "docket": pl.String,
    "item": pl.Int32,
    "document_id": pl.String,
    "filed_date": pl.Date,
    "member": pl.String,
    "page": pl.Int32,
    "table_title": pl.String,
    "breakdown": pl.String,
    "name": pl.String,
    "is_total": pl.Boolean,
    "year": pl.Int32,
    "mw": pl.Float64,
    "extraction_method": pl.String,
    "verified": pl.Boolean,
}


def parse_tsp_requests(f: RawFile) -> pl.DataFrame | None:
    """Tables titled "TSP Large Load RFI Submissions by TSP / by Load Type" in a ZIP's PPTX, and in the PDF
    member with the same name (the PPTX exported by the filer). Other ZIPs have none (returns None)."""
    if f.suffix != ".zip":
        return None
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        names = {i.filename: i for i in z.infolist()}
        pptx = [n for n in names if n.lower().endswith(".pptx")]
        tables: list[tuple[str, str, int, str, list]] = []
        for name in pptx:
            found = _pptx_tables(z.read(names[name]))
            if not found:
                continue
            tables += [(name, "pptx_table", n, t, g) for n, t, g in found]
            twin = next((n for n in names if n.lower() == name.lower()[:-5] + ".pdf"), None)
            if twin:
                tables += [(twin, "pdf_table", n, t, g) for n, t, g in _pdf_tables(z.read(names[twin]))]
    if not tables:
        return None
    filing = _filing(f)
    rows = []
    for member, method, page, title, grid in tables:
        parsed = rows_from_grid(grid)
        breakdown = _breakdown(title)
        for name, year, mw, is_total in parsed:
            rows.append({
                "docket": filing["docket"], "item": filing["item"], "document_id": filing["document_id"],
                "filed_date": filing["filed_date"], "member": member, "page": page, "table_title": title,
                "breakdown": breakdown, "name": name, "is_total": is_total, "year": year, "mw": mw,
                "extraction_method": method, "verified": False,
            })
        _check_totals(f, member, breakdown, parsed)
    return frame(rows, REQUESTS_SCHEMA)


def _check_totals(f: RawFile, member: str, breakdown: str, parsed: list[tuple[str, int, float, bool]]) -> None:
    """Rows should add up to the Total row (ERCOT's by-TSP table is off by up to 1 MW from rounding)."""
    sums: dict[int, float] = {}
    totals: dict[int, float] = {}
    for _, year, mw, is_total in parsed:
        if is_total:
            totals[year] = mw
        else:
            sums[year] = sums.get(year, 0.0) + mw
    for year, total in totals.items():
        diff = sums.get(year, 0.0) - total
        if abs(diff) > 2:
            log.warning("%s %s by %s %d: rows sum to %.0f, total says %.0f", f.name, member, breakdown, year,
                        sums.get(year, 0.0), total)


DATASETS = [
    Dataset(
        name="puct_tsp_large_load_requests",
        target="postgres",
        mode="by_file",
        description=(
            "TSP large-load RFI submissions to ERCOT for the 2026 LTLF (PUCT 58777 item 38, Attachment A slides 4-5): "
            "MW requested by TSP and by load type, 2026-2032, from the native PPTX tables and the matching PDF. "
            "Requests, not a forecast. verified=false."
        ),
        parse=parse_tsp_requests,
        inputs=lambda f: _is_document(f) and f.suffix == ".zip",
    ),
    Dataset(
        name="puct_filing_documents",
        target="postgres",
        mode="by_file",
        description=(
            "One row per document filed in PUCT dockets 58777, 59772 and 58481 (rendition PDFs and native-file ZIPs): "
            "docket, item, document id, filed date, filing party (organizations only), description, pages, text "
            "characters extracted, pages without a text layer (scanned). Filings by individuals keep no party name "
            "and no text."
        ),
        parse=parse_documents,
        inputs=_is_document,
    ),
    Dataset(
        name="document_pages",
        target="postgres",
        mode="by_file",
        description=PAGES_DESCRIPTION,
        parse=parse_pages,
        inputs=_is_document,
    ),
]

