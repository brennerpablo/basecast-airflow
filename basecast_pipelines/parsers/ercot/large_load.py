"""ERCOT large-load decks (status decks, Board updates, ERCOT Monthly, Batch Zero and hearing decks).

No deck has an extractable table (``docs/large-load-deck-audit.md``): 2022-2024 PDFs draw charts as vectors
whose labels are rotated or garbled in the text layer, one Oct 2024 PPTX has native charts, and later
charts are images. So:

- ``large_load_headlines``: numbers stated in the bullet text ("Of the 9,042 MW that have received Approval
  to Energize, ERCOT has observed a non-simultaneous monthly peak consumption of 4,004 MW in March 2026").
  Each pattern below matches one known sentence form; anything else is left out (precision over recall).
- ``large_load_status``: series of native PPTX charts (numbers from the chart XML cache).
- ``document_pages``: page/slide text of every deck, shared with ``puct_filings``.

``report_date`` is the raw ``dt``: the meeting date (ERCOT Monthly: first day of the issue month). A date
written in the sentence goes to ``as_of_date``; ERCOT's decks sometimes carry the wrong year there (the
January-March 2026 decks say "January 2025" ... "March 2025"), which ``as_of_suspect`` flags. Every number
has ``verified = false`` until a human checks it.
"""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl

from basecast_pipelines.parsers._documents import (
    PAGES_DESCRIPTION,
    PAGES_SCHEMA,
    DocInfo,
    ExtractedDoc,
    doc_kind,
    extract,
    flatten,
    frame,
    iter_shapes,
    page_rows,
    pptx_slides,
    scrub,
    slide_title,
)
from basecast_pipelines.processing.core import Dataset, RawFile
from basecast_pipelines.processing.tabular import excel_serial_to_date, snake
from basecast_pipelines.sources.ercot.large_load import NOT_STATUS, OTHER_LARGE_LOAD, STATUS_DECK, normalize

SOURCE_ID = "ercot_large_load_decks"

# --- which documents of a raw file are decks ---------------------------------------------------------


@dataclass
class Deck:
    member: str | None  # zip member name, or None for a standalone file
    kind: str  # "pdf" | "pptx"
    doc: ExtractedDoc
    data: bytes | None  # raw bytes (PPTX only, for charts)


def _relevant_member(name: str, listed: set[str]) -> bool:
    base = normalize(name.rsplit("/", 1)[-1])
    if name in listed:
        return True
    return bool(STATUS_DECK.search(base) and not NOT_STATUS.search(base)) or bool(OTHER_LARGE_LOAD.search(base))


def decks(f: RawFile) -> list[Deck]:
    """PDF/PPTX documents in a raw file: the file itself, or the large-load members of a TAC zip (those
    discovery listed in ``zip_members`` plus any other member named like a large-load deck)."""
    kind = doc_kind(f.name)
    if kind == "pdf":
        with f.local_path() as path:
            return [Deck(None, "pdf", extract("pdf", path, cache_key=(f.key, f.sha256, "")), None)]
    if kind == "pptx":
        data = f.read_bytes()
        return [Deck(None, "pptx", extract("pptx", data, cache_key=(f.key, f.sha256, "")), data)]
    if f.suffix != ".zip":
        return []
    listed = set(f.meta.get("zip_members") or [])
    out = []
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        for info in z.infolist():
            member_kind = doc_kind(info.filename)
            if member_kind is None or not _relevant_member(info.filename, listed):
                continue
            data = z.read(info)
            doc = extract(member_kind, data, cache_key=(f.key, f.sha256, info.filename))
            out.append(Deck(info.filename, member_kind, doc, data if member_kind == "pptx" else None))
    return out


def _title(f: RawFile, deck: Deck) -> str:
    if deck.member:
        return normalize(deck.member.rsplit("/", 1)[-1])
    return f.meta.get("link_text") or normalize(f.name)


def _posted(f: RawFile) -> date | None:
    try:
        return date.fromisoformat(str(f.meta.get("posted_date")))
    except ValueError:
        return None


def deck_date(deck: Deck) -> date | None:
    """The date printed on the title slide ("Large Load Interconnection Status ... August 4, 2023"): some
    decks were linked from a later meeting page, so it can differ from ``dt``."""
    if not deck.doc.pages:
        return None
    match = _FULL_DATE.search(flatten(deck.doc.pages[0].text))
    return _day_date(match.group("d1")) if match else None


def definition_version(text: str) -> str | None:
    """Which set of queue-stage definitions the deck uses (ERCOT changed them in 2023, 2025 and 2026)."""
    lowered = text.lower()
    if re.search(r"section 9\.4\s*/\s*9\.5 requirements met", lowered):
        return "section_9_4_9_5"
    if "observed energized" in lowered:
        return "observed_energized"
    if "planning studies approved" in lowered:
        return "planning_studies_approved"
    if "met planning" in lowered:
        return "met_planning"
    return None


# --- headline patterns -------------------------------------------------------------------------------

_MONTHS = ("january|february|march|april|may|june|july|august|september|october|november|december"
           "|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec")
_PIECES = {  # "{i}" is replaced by the group number
    "N": r"(?P<n{i}>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)",
    "U": r"\s*(?P<u{i}>MW|GW|megawatts?|gigawatts?)(?:\s*\((?:MW|GW)\))?",
    "Q": r"(?P<q{i}>approximately|approx\.?|about|around|roughly|nearly|almost|over|more\s+than|at\s+least|~)?\s*",
    "M": r"(?P<m{i}>(?:" + _MONTHS + r")\.?\s+\d{4})",
    "D": r"(?P<d{i}>(?:" + _MONTHS + r")\.?\s+\d{1,2},\s*\d{4})",
}


def _compile(template: str) -> re.Pattern:
    """``{N1}`` / ``{U1}`` / ``{Q1}`` / ``{M1}`` / ``{D1}`` become named groups n1, u1, q1, m1, d1."""

    def piece(match: re.Match) -> str:
        return _PIECES[match.group(1)].replace("{i}", match.group(2))

    return re.compile(re.sub(r"\{([NUQMD])(\d)\}", piece, template), re.IGNORECASE)


@dataclass(frozen=True)
class Out:
    metric: str
    i: int = 1  # which number group
    status_bucket: str | None = None
    dimension: str | None = None
    category: str | None = None  # literal, or "group:<name>" to read a named group
    unit: str = "MW"
    as_of: int | None = None  # month group with the data date
    historical: bool = False  # the date is a past reference point, not the report's data date


@dataclass(frozen=True)
class Pattern:
    id: str
    regex: re.Pattern
    outs: tuple[Out, ...]


def _p(pid: str, template: str, *outs: Out) -> Pattern:
    return Pattern(pid, _compile(template), outs)


TRACKED = "all_tracked"
A2E = "approved_to_energize"

PATTERNS: tuple[Pattern, ...] = (
    # Total queue ("MW tracked").
    _p("tracking_of_large_load", r"\btrack(?:ing|s|ed)?\s+{Q1}{N1}{U1}\s+of\s+(?:large\s+loads?|LLI)\b",
       Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("currently_being_tracked", r"{N1}{U1}\s+currently\s+being\s+tracked\b", Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("projects_tracked_total", r"large\s+load\s+projects\s+tracked\s+by\s+ERCOT\s+total\s+{Q1}{N1}{U1}",
       Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("seeking_interconnection", r"{Q1}{N1}{U1}\s+of\s+large\s+loads?\s+(?:currently\s+)?seeking\s+interconnection",
       Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("in_interconnection_process", r"{Q1}{N1}{U1}\s+currently\s+in\s+the\s+large\s+load\s+interconnection\s+process",
       Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("now_totaling", r"large\s+loads?\s+now\s+totaling\s+{Q1}{N1}{U1}", Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("request_queue_increasing_to", r"(?:LLI|large\s+load)\s+request\s+queue\s+increasing\s+to\s+{Q1}{N1}{U1}",
       Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("ercot_tracks", r"{N1}{U1}\s+ERCOT\s+tracks\b", Out("total_mw_tracked", status_bucket=TRACKED)),
    _p("tracked_compared_to", r"\btrack[^.]{0,160}?\(compared\s+to\s+{Q1}{N1}{U1}\s+in\s+{M1}\)",
       Out("total_mw_tracked", status_bucket=TRACKED, as_of=1, historical=True)),
    _p("there_were_requests", r"\bin\s+{M1},\s+there\s+were\s+{Q1}{N1}{U1}\s+of\s+large\s+load\s+interconnection\s+requests",
       Out("total_mw_tracked", status_bucket=TRACKED, as_of=1, historical=True)),
    _p("data_center_share", r"\btrack[^.]{0,200}?of\s+which\s+(?P<q1>~|approximately\s+)?(?P<n1>\d+(?:\.\d+)?)\s*%\s+are\s+data\s+centers",
       Out("data_center_share_of_tracked_pct", status_bucket=TRACKED, unit="percent")),
    _p("queue_capacity_change", r"increas(?:ed|ing)\s+total\s+queue\s+capacity\s+by\s+{N1}{U1}",
       Out("queue_net_change_mw", status_bucket=TRACKED)),
    _p("queue_net_increase", r"net\s+increase\s+of\s+{N1}{U1}\s+in\s+the\s+large\s+load\s+queue",
       Out("queue_net_change_mw", status_bucket=TRACKED)),
    _p("new_requests", r"(?P<q1>approx\.?|approximately)?\s*{N1}{U1}\s+of\s+new\s+interconnect(?:ion)?\s+requests\s+received",
       Out("new_requests_mw", status_bucket=TRACKED)),
    # Approved to energize and observed consumption.
    _p("of_the_approved", r"\bOf\s+the\s+(?:total\s+)?{N1}{U1}\s+(?:that\s+have\s+received\s+)?Approv(?:al|ed)\s+to\s+Energize",
       Out("approved_to_energize_mw", status_bucket=A2E)),
    _p("demand_received_approval", r"{Q1}{N1}{U1}\s+of\s+large\s+load\s+demand\s+has\s+received\s+approval\s+to\s+energize",
       Out("approved_to_energize_mw", status_bucket=A2E)),
    _p("approved_observed_not_operational",
       r"{Q1}{N1}{U1}\s+of\s+large\s+loads?\s+have\s+been\s+approved\s+to\s+energize,\s+with\s+{Q2}{N2}{U2}\s+having\s+been"
       r"\s+observed\s+energized\s+and\s+an\s+additional\s+{Q3}{N3}{U3}\s+having\s+been\s+approved\s+to\s+energize,?\s+but"
       r"\s+not\s+yet\s+operational",
       Out("approved_to_energize_mw", 1, A2E), Out("observed_energized_mw", 2, "observed_energized"),
       Out("approved_not_operational_mw", 3, "approved_not_operational")),
    _p("observed_energized_and_not_operational",
       r"ERCOT\s+has\s+{Q1}{N1}{U1}\s+of\s+Observed\s+Energized\s+Large\s+Loads?,?\s+with\s+another\s+{Q2}{N2}{U2}\s+of\s+loads"
       r"\s+that\s+have\s+Approval\s+to\s+Energize\s+but\s+are\s+not\s+Operational",
       Out("observed_energized_mw", 1, "observed_energized"), Out("approved_not_operational_mw", 2, "approved_not_operational")),
    _p("approved_past_two_years",
       r"{Q1}{N1}{U1}\s+of\s+large\s+loads?\s+have\s+been\s+approved\s+to\s+energize\s+in\s+the\s+past\s+(?:2|two)\s+years",
       Out("approved_to_energize_past_two_years_mw", status_bucket=A2E)),
    _p("studies_approved_past_two_years",
       r"approved\s+studies\s+for\s+the\s+interconnection\s+of\s+{Q1}{N1}{U1}\s+of\s+large\s+loads?\s+in\s+the\s+past\s+(?:2|two)\s+years",
       Out("planning_studies_approved_past_two_years_mw", status_bucket="planning_studies_approved")),
    _p("approved_past_year", r"past\s+year\s+has\s+seen\s+(?:of\s+)?{Q1}{N1}{U1}\s+of\s+load\s+approved\s+to\s+energize",
       Out("approved_to_energize_past_year_mw", status_bucket=A2E)),
    _p("approved_by_load_zone",
       r"{N1}{U1}\s+resides?\s+in\s+(?P<zone>LZ_[A-Z]+)\s+and\s+{N2}{U2}\s+resides?\s+in\s+the\s+other\s+load\s+zones",
       Out("approved_to_energize_mw", 1, A2E, "load_zone", "group:zone"),
       Out("approved_to_energize_mw", 2, A2E, "load_zone", "Other")),
    _p("approved_by_project_type",
       r"{N1}{U1}\s+consists?\s+of\s+standalone\s+projects\s+and\s+{N2}{U2}\s+consists?\s+of\s+co-?\s?located\s+projects",
       Out("approved_to_energize_mw", 1, A2E, "project_type", "Standalone"),
       Out("approved_to_energize_mw", 2, A2E, "project_type", "Co-Located")),
    _p("observed_peak",
       r"observed\s+an?\s+(?P<kind>non-?\s*simultaneous|non-?\s*coincident|simultaneous|coincident)\s+(?P<monthly>monthly\s+)?"
       r"peak\s+(?:consumption|load)\s+of\s+{N1}{U1}(?:\s+in\s+{M1})?"
       r"(?:\s+with\s+an\s+all\s+time\s+peak\s+of\s+{N2}{U2}\s+(?:that\s+)?(?:occurred\s+)?(?:in\s+)?{M2})?",
       Out("observed_peak", 1, A2E, as_of=1), Out("observed_peak_all_time", 2, A2E, as_of=2, historical=True)),
    # Queue stages (2026 definitions).
    _p("under_review_and_9_5_met",
       r"{Q1}{N1}{U1}\s+of\s+Large\s+Load\s+applications\s+are\s+classified\s+as\s+Under\s+ERCOT\s+Review,?\s+and\s+{Q2}{N2}{U2}"
       r"\s+having\s+Section\s+9\.5\s+Requirements\s+Met",
       Out("under_ercot_review_mw", 1, "under_ercot_review"),
       Out("section_9_5_requirements_met_mw", 2, "section_9_5_requirements_met")),
    _p("not_yet_submitted_studies",
       r"{Q1}{N1}{U1}\s+of\s+the\s+{N2}{U2}\s+have\s+not\s+yet\s+submitted\s+their\s+Planning\s+Studies",
       Out("no_studies_submitted_mw", 1, "no_studies_submitted")),
    _p("applications_no_studies",
       r"{Q1}{N1}{U1}\s+of\s+these\s+applications\s+have\s+No\s+Studies\s+Submitted",
       Out("no_studies_submitted_mw", 1, "no_studies_submitted")),
    # TSP RFI (Feb 2023).
    _p("rfi_under_tsp_study", r"RFI\s+reported\s+{Q1}{N1}{U1}\s+of\s+Large\s+Load\s+\(75\s*MW\s+or\s+greater\)\s+currently\s+under"
       r"\s+study\s+by\s+TSPs", Out("tsp_rfi_under_study_mw")),
    # Batch Zero (2026).
    _p("batch_zero_eligible", r"{Q1}{N1}{U1}\s+of\s+Large\s+Load\s+is\s+eligible\s+for\s+inclusion\s+in\s+Batch\s+Zero",
       Out("batch_zero_eligible_mw", status_bucket="batch_zero_eligible")),
    _p("batch_zero_base", r"(?P<n2>\d[\d,]*)\s+projects\s+\({N1}{U1}\)\s+met\s+the\s+qualifications\s+to\s+be\s+conditionally"
       r"\s+included\s+as\s+base\s+load",
       Out("batch_zero_base_load_mw", 1, "batch_zero_base_load"),
       Out("batch_zero_base_load_projects", 2, "batch_zero_base_load", unit="projects")),
    _p("batch_zero_studied", r"(?P<n2>\d[\d,]*)\s+projects\s+\({N1}{U1}\)\s+met\s+the\s+qualifications\s+to\s+be\s+conditionally"
       r"\s+included\s+as\s+studied\s+load",
       Out("batch_zero_studied_load_mw", 1, "batch_zero_studied_load"),
       Out("batch_zero_studied_load_projects", 2, "batch_zero_studied_load", unit="projects")),
    _p("batch_zero_excluded", r"omits\s+excluded\s+load\s+\({N1}{U1},\s*(?P<n2>\d[\d,]*)\s+projects\)",
       Out("batch_zero_excluded_mw", 1, "batch_zero_excluded"),
       Out("batch_zero_excluded_projects", 2, "batch_zero_excluded", unit="projects")),
)

_QUALIFIER = {
    "approximately": "approx", "approx": "approx", "approx.": "approx", "about": "approx", "around": "approx",
    "roughly": "approx", "~": "approx", "nearly": "at_most", "almost": "at_most", "over": "at_least",
    "more than": "at_least", "at least": "at_least",
}
_AS_OF_DAY = _compile(r"\bas\s+of\s+{D1}")
_FULL_DATE = _compile(r"{D1}")
_MONTH_NUMBER = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def _to_number(text: str) -> float:
    return float(text.replace(",", ""))


def _month_date(text: str) -> date | None:
    match = re.match(r"([A-Za-z]+)\.?\s+(\d{4})", text.strip())
    if not match:
        return None
    month = _MONTH_NUMBER.get(match.group(1)[:3].lower())
    return date(int(match.group(2)), month, 1) if month else None


def _day_date(text: str) -> date | None:
    try:
        return datetime.strptime(flatten(text).replace(".", ""), "%B %d, %Y").date()
    except ValueError:
        try:
            return datetime.strptime(flatten(text).replace(".", ""), "%b %d, %Y").date()
        except ValueError:
            return None


def _snippet(text: str, start: int, end: int) -> str:
    """The sentence around a match (bounded), for a human to check the number."""
    starts = [i + 2 for i in (text.rfind(". ", 0, start), text.rfind("• ", 0, start)) if i >= 0]
    left = max([*starts, start - 250, 0])
    stops = [i for i in (text.find(". ", end), text.find(" • ", end)) if i >= 0]
    right = min([*stops, end + 200, len(text)])
    return text[left:right + 1].strip()


def _peak_basis(kind_metric: str, match: re.Match, page_text: str) -> str | None:
    if kind_metric == "observed_peak_all_time":
        return "all_time"
    if match.group("monthly") or match.group("m1"):
        return "monthly"
    lowered = page_text.lower()
    if "regardless of when that maximum occurred" in lowered or "at a single point in time" in lowered:
        return "all_time"  # the page's own definition of the number
    return None


def headlines(page_text: str, *, report_date: date | None = None) -> list[dict]:
    """Numbers stated in one page's text, one row per (pattern output). ``page_text`` may contain line
    breaks; matching runs on the flattened text."""
    text = flatten(page_text)
    rows: list[dict] = []
    for pattern in PATTERNS:
        for match in pattern.regex.finditer(text):
            groups = match.groupdict()
            snippet = scrub(_snippet(text, match.start(), match.end()))
            window = text[max(0, match.start() - 300):match.end()]
            day = _AS_OF_DAY.search(window)
            for out in pattern.outs:
                raw = groups.get(f"n{out.i}")
                if raw is None:
                    continue
                value = _to_number(raw)
                unit = (groups.get(f"u{out.i}") or "").lower()
                if unit.startswith("g"):
                    value *= 1000
                metric = out.metric
                peak_basis = None
                if metric.startswith("observed_peak"):
                    kind = re.sub(r"[\s-]+", "", groups["kind"].lower())
                    peak_basis = _peak_basis(metric, match, text)
                    simultaneous = kind in {"simultaneous", "coincident"}
                    metric = "observed_peak_simultaneous_mw" if simultaneous else "observed_peak_non_simultaneous_mw"
                category = out.category
                if category and category.startswith("group:"):
                    category = groups.get(category.removeprefix("group:"))
                as_of_text = groups.get(f"m{out.as_of}") if out.as_of else None
                as_of_date = _month_date(as_of_text) if as_of_text else None
                if as_of_text is None and day and not out.historical:
                    as_of_text, as_of_date = day.group("d1"), _day_date(day.group("d1"))
                suspect = False
                if as_of_date and report_date and not out.historical:
                    suspect = not (report_date - timedelta(days=120) <= as_of_date <= report_date + timedelta(days=45))
                qualifier = groups.get(f"q{out.i}")
                rows.append({
                    "metric": metric,
                    "status_bucket": out.status_bucket,
                    "dimension": out.dimension,
                    "category": category,
                    "value": value,
                    "unit": out.unit,
                    "qualifier": _QUALIFIER.get(re.sub(r"\s+", " ", qualifier.strip().lower())) if qualifier else None,
                    "peak_basis": peak_basis,
                    "as_of_date": as_of_date,
                    "as_of_text": flatten(as_of_text) if as_of_text else None,
                    "as_of_suspect": suspect,
                    "value_text": flatten(match.group(f"n{out.i}") + (" " + groups[f"u{out.i}"] if groups.get(f"u{out.i}") else "")),
                    "text_snippet": snippet[:600],
                    "pattern": pattern.id,
                })
    unique: dict[tuple, dict] = {}  # overlapping patterns can read the same number twice
    for row in rows:
        unique.setdefault(
            (row["metric"], row["dimension"], row["category"], row["value"], row["peak_basis"], row["as_of_date"]), row)
    return list(unique.values())


HEADLINE_SCHEMA: dict[str, pl.DataType] = {
    "report_date": pl.Date,
    "deck_date": pl.Date,
    "posted_date": pl.Date,
    "doc_type": pl.String,
    "deck_title": pl.String,
    "file_name": pl.String,
    "member": pl.String,
    "page": pl.Int32,
    "page_title": pl.String,
    "metric": pl.String,
    "status_bucket": pl.String,
    "dimension": pl.String,
    "category": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "qualifier": pl.String,
    "peak_basis": pl.String,
    "as_of_date": pl.Date,
    "as_of_text": pl.String,
    "as_of_suspect": pl.Boolean,
    "value_text": pl.String,
    "text_snippet": pl.String,
    "pattern": pl.String,
    "definition_version": pl.String,
    "extraction_method": pl.String,
    "verified": pl.Boolean,
    "url": pl.String,
}


def parse_headlines(f: RawFile) -> pl.DataFrame | None:
    rows = []
    for deck in decks(f):
        version = definition_version("\n".join(p.text for p in deck.doc.pages))
        printed = deck_date(deck)
        seen: dict[tuple, dict] = {}
        for page in deck.doc.pages:
            for row in headlines(page.text, report_date=printed or f.dt):
                key = (row["metric"], row["dimension"], row["category"], row["value"], row["peak_basis"])
                if key in seen:  # the same number repeated (e.g. in a "Key Takeaway"): keep the first, dated
                    first = seen[key]
                    if first["as_of_date"] is None and row["as_of_date"] is not None:
                        first.update({k: row[k] for k in ("as_of_date", "as_of_text", "as_of_suspect")})
                    if first["as_of_date"] == row["as_of_date"] or row["as_of_date"] is None:
                        continue
                seen.setdefault(key, row)
                rows.append({
                    **row,
                    "report_date": f.dt,
                    "deck_date": printed,
                    "posted_date": _posted(f),
                    "doc_type": f.meta.get("type"),
                    "deck_title": _title(f, deck),
                    "file_name": f.name,
                    "member": deck.member,
                    "page": page.number,
                    "page_title": scrub(page.title),
                    "definition_version": version,
                    "extraction_method": f"{deck.kind}_text",
                    "verified": False,
                    "url": f.url,
                })
    return frame(rows, HEADLINE_SCHEMA)


# --- native PPTX charts -----------------------------------------------------------------------------

_SERIES_BUCKET = (
    (r"^observed\s+non-?\s*simultaneous\s+peak", "observed_peak_non_simultaneous"),
    (r"^observed\s+simultaneous\s+peak", "observed_peak_simultaneous"),
    (r"^observed\s+energized", "observed_energized"),
    (r"^remaining\s+approved\s+to\s+energize", "approved_remaining"),
    (r"^approved\s+to\s+energi[sz]ed?\s+but\s+not\s+operational", "approved_not_operational"),
    (r"^approved\s+to\s+energize", "approved_to_energize"),
    (r"^planning\s+studies\s+approved", "planning_studies_approved"),
    (r"^under\s+ercot\s+review", "under_ercot_review"),
    (r"^no\s+studies\s+submitted", "no_studies_submitted"),
    (r"^(?:standalone|co-?\s?located|)$", "all_tracked"),
)
_PROJECT_TYPE = re.compile(r"^(?:standalone|co-?\s?located)$", re.IGNORECASE)
_LOAD_ZONE = re.compile(r"^(?:LZ_[A-Z]+|other)$", re.IGNORECASE)


def _bucket(series_name: str) -> str:
    name = flatten(series_name or "").lower()
    for pattern, bucket in _SERIES_BUCKET:
        if re.search(pattern, name):
            return bucket
    return snake(name)[:80] or "unnamed"


def _axis_title(chart, which: str) -> str:
    try:
        axis = getattr(chart, which)
        return flatten(axis.axis_title.text_frame.text) if axis.has_title else ""
    except (AttributeError, ValueError, KeyError, IndexError):
        return ""  # charts without that axis (pie, doughnut)


def _chart_unit(chart) -> str | None:
    """MW or GW from the value-axis title ("Load Amount (MW)", "Large Load Requests (GW)")."""
    title = _axis_title(chart, "value_axis")
    match = re.search(r"\b(MW|GW)\b", title, re.IGNORECASE)
    return match.group(1).upper() if match else None


def _category_dates(chart, labels: list[str]) -> list[date | None] | None:
    """Excel serial dates when the category axis is a date axis ("Snapshot Date", format mmm-yy)."""
    try:
        numbers = [float(x) for x in labels]
    except (TypeError, ValueError):
        return None
    axis_title = _axis_title(chart, "category_axis").lower()
    try:
        fmt = (chart.category_axis.tick_labels.number_format or "").lower()
    except (AttributeError, ValueError, KeyError, IndexError):
        fmt = ""
    if all(20000 <= n <= 80000 for n in numbers) and ("date" in axis_title or re.search(r"[my]{2}", fmt)):
        return [excel_serial_to_date(n) for n in numbers]
    return None


STATUS_SCHEMA: dict[str, pl.DataType] = {
    "report_date": pl.Date,
    "deck_date": pl.Date,
    "posted_date": pl.Date,
    "doc_type": pl.String,
    "deck_title": pl.String,
    "file_name": pl.String,
    "member": pl.String,
    "page": pl.Int32,
    "chart_title": pl.String,
    "series_name": pl.String,
    "status_bucket": pl.String,
    "dimension": pl.String,
    "snapshot_date": pl.Date,
    "load_zone": pl.String,
    "project_type": pl.String,
    "tsp": pl.String,
    "category_label": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "mw": pl.Float64,
    "definition_version": pl.String,
    "extraction_method": pl.String,
    "verified": pl.Boolean,
    "url": pl.String,
}


def chart_rows(data: bytes) -> list[dict]:
    """Every value of every native chart in a PPTX, one row per (chart, series, category)."""
    rows = []
    for number, slide in enumerate(pptx_slides(data), 1):
        charts = [s for s in iter_shapes(slide.shapes) if getattr(s, "has_chart", False) and s.has_chart]
        if not charts:
            continue
        title = slide_title(slide)
        for shape in charts:
            chart = shape.chart
            chart_title = flatten(chart.chart_title.text_frame.text) if chart.has_title else None
            unit = _chart_unit(chart)
            for plot in chart.plots:
                labels = [str(c) for c in plot.categories]
                dates = _category_dates(chart, labels)
                for series in plot.series:
                    bucket = _bucket(series.name)
                    series_type = flatten(series.name or "")
                    for j, value in enumerate(series.values):
                        if value is None:
                            continue
                        label = labels[j] if j < len(labels) else None
                        snapshot = dates[j] if dates else None
                        is_pivot_label = bool(label and re.match(r"^(?:sum|count|average)\s+of\b", label, re.IGNORECASE))
                        category = None if snapshot or is_pivot_label else label
                        project_type = next((x for x in (category, series_type) if x and _PROJECT_TYPE.match(x)), None)
                        load_zone = category if category and _LOAD_ZONE.match(category) else None
                        dimension = ("snapshot_date" if snapshot else "load_zone" if load_zone
                                     else "project_type" if category and project_type else None)
                        rows.append({
                            "page": number,
                            "chart_title": chart_title or title,
                            "series_name": series_type or None,
                            "status_bucket": bucket,
                            "dimension": dimension,
                            "snapshot_date": snapshot,
                            "load_zone": load_zone.upper() if load_zone and load_zone.lower() != "other" else load_zone,
                            "project_type": project_type,
                            "tsp": None,
                            "category_label": label,
                            "value": float(value),
                            "unit": unit,
                            "mw": float(value) * (1000 if unit == "GW" else 1) if unit else None,
                        })
    return rows


def parse_status(f: RawFile) -> pl.DataFrame | None:
    rows = []
    for deck in decks(f):
        if deck.kind != "pptx" or deck.data is None:
            continue
        version = definition_version("\n".join(p.text for p in deck.doc.pages))
        for row in chart_rows(deck.data):
            rows.append({
                **row,
                "report_date": f.dt,
                "deck_date": deck_date(deck),
                "posted_date": _posted(f),
                "doc_type": f.meta.get("type"),
                "deck_title": _title(f, deck),
                "file_name": f.name,
                "member": deck.member,
                "definition_version": version,
                "extraction_method": "pptx_chart",
                "verified": False,
                "url": f.url,
            })
    return frame(rows, STATUS_SCHEMA)


# --- document pages -----------------------------------------------------------------------------------


def parse_pages(f: RawFile) -> pl.DataFrame | None:
    rows = []
    for deck in decks(f):
        info = DocInfo(
            source_id=SOURCE_ID, doc_type=f.meta.get("type"), file_name=f.name, member=deck.member,
            title=_title(f, deck), document_date=f.dt, url=f.url,
        )
        rows += page_rows(info, deck.doc)
    return frame(rows, PAGES_SCHEMA)


def _is_document(f: RawFile) -> bool:
    return f.suffix in {".pdf", ".pptx", ".zip"}


DATASETS = [
    Dataset(
        name="large_load_headlines",
        target="postgres",
        mode="by_file",
        description=(
            "Headline numbers stated in the text of ERCOT large-load decks (status decks 2022+, Board updates, "
            "ERCOT Monthly, Batch Zero decks): MW tracked, approved to energize (total, by load zone, by project "
            "type), observed peaks, queue changes, stage totals, Batch Zero classes. One row per number, with the "
            "sentence it came from; regex extraction, verified=false."
        ),
        parse=parse_headlines,
        inputs=_is_document,
    ),
    Dataset(
        name="large_load_status",
        target="postgres",
        mode="by_file",
        description=(
            "Series of native PPTX charts in the large-load decks (Oct 2024 status deck: queue by connection type, "
            "approvals, observed peaks by snapshot, load zone and project type; Jul 2026 Batch Zero eligibility). "
            "Values from the chart XML cache, verified=false."
        ),
        parse=parse_status,
        inputs=lambda f: f.suffix in {".pptx", ".zip"},
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
