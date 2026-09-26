"""``large_load_chart_values``: numbers printed on the charts of the large-load decks, read by Gemini.

The 2024+ status decks draw their charts as images and the older PDFs as vector charts with garbled
labels, so no text or table extraction reaches them (``docs/large-load-deck-audit.md``). Gemini reads the
values printed as data labels or table cells, never bar heights. A PDF goes whole in one call; a PPTX goes
as the pictures of each slide with the slide's text. Every row is ``extraction_method = "gemini"`` and
``verified = false``; ``confidence`` is Gemini's own label. Answers are cached in the lake
(``derived/gemini/``), so only new documents are billed.
"""

from __future__ import annotations

import hashlib
import io
import logging
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import polars as pl

from basecast_pipelines.common import gemini
from basecast_pipelines.common.gemini import Blob
from basecast_pipelines.parsers._documents import doc_kind, iter_shapes, pptx_slides, slide_title
from basecast_pipelines.processing.core import RawFile

log = logging.getLogger(__name__)

SOURCE_ID = "ercot_large_load_decks"
TASK = "large_load_charts"
PROMPT_VERSION = "v1"
MAX_SLIDE_PICTURES = 40

PROMPT = """This document comes from ERCOT (Texas grid operator) and is about large loads (data centers, crypto
mining, industrial loads) in the interconnection queue. Read every chart or table that gives megawatts,
gigawatts or project counts of large loads, broken down by queue status, year, month, load zone, project
type or transmission provider.

Rules:
- Only values that are printed on the page: data labels, table cells, or numbers in the chart's text.
  Never estimate a value from a bar height or line position; skip charts without printed values.
- One item per printed value. Keep the status name as printed in status_label and map it to status_bucket,
  one of: observed_energized, approved_to_energize, approved_to_energize_not_operational,
  planning_studies_approved, under_ercot_review, no_studies_submitted, total, other.
- category is what the value belongs to on the other axis (a year like "2027", a month like "2025-06",
  a zone, a project type, or "total"). category_type is one of: year, month, date, load_zone,
  project_type, tsp, other.
- unit is exactly "MW", "GW" or "count", as the chart states.
- page is the 1-based page or slide number.

Return only JSON: {"as_of": "YYYY-MM-DD" or null (the data date the deck states),
"items": [{"page": int, "chart_title": str, "status_label": str, "status_bucket": str,
"category": str, "category_type": str, "value": number, "unit": str,
"confidence": "high" | "medium" | "low"}]}"""

SCHEMA = {
    "report_date": pl.Date,
    "document": pl.String,
    "as_of": pl.Date,
    "page": pl.Int32,
    "chart_title": pl.String,
    "status_label": pl.String,
    "status_bucket": pl.String,
    "category": pl.String,
    "category_type": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "value_mw": pl.Float64,
    "confidence": pl.String,
    "extraction_method": pl.String,
    "model": pl.String,
    "prompt_version": pl.String,
    "verified": pl.Boolean,
}


@dataclass
class _Doc:
    name: str
    kind: str  # "pdf" | "pptx"
    data: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


def _documents(f: RawFile) -> list[_Doc]:
    """The PDFs and PPTXs of a raw file: the file itself, or the large-load members of a TAC zip."""
    from basecast_pipelines.parsers.ercot.large_load import _relevant_member

    kind = doc_kind(f.name)
    if kind in {"pdf", "pptx"}:
        return [_Doc(f.name, kind, f.read_bytes())]
    if f.suffix != ".zip":
        return []
    listed = set(f.meta.get("zip_members") or [])
    out = []
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        for info in z.infolist():
            member_kind = doc_kind(info.filename)
            if member_kind in {"pdf", "pptx"} and _relevant_member(info.filename, listed):
                out.append(_Doc(info.filename.rsplit("/", 1)[-1], member_kind, z.read(info)))
    return out


def _parts(doc: _Doc) -> list[Blob] | None:
    if doc.kind == "pdf":
        return [Blob(doc.data, "application/pdf")]
    parts: list[Blob] = []
    pictures = 0
    for number, slide in enumerate(pptx_slides(doc.data), 1):
        images = [s.image for s in iter_shapes(slide.shapes) if getattr(s, "shape_type", None) == 13]
        if not images:
            continue
        parts.append(Blob(f"Slide {number}: {slide_title(slide) or ''}"))
        for image in images:
            if pictures >= MAX_SLIDE_PICTURES:
                break
            parts.append(Blob(image.blob, image.content_type))
            pictures += 1
    return parts or None


def _ask(f: RawFile, doc: _Doc) -> dict | None:
    parts = _parts(doc)
    if not parts:
        return None
    return gemini.runner().extract_json(
        f.storage, source_id=SOURCE_ID, sha256=doc.sha256, task=TASK, prompt_version=PROMPT_VERSION,
        parts=parts, prompt=PROMPT,
    )


def warm_cache(files: list[RawFile]) -> None:
    """``Dataset.prepare`` hook: ask Gemini about every uncached document of the files about to be parsed,
    in parallel (a deck takes ~50 s), so the per-file parse only reads the cache."""
    jobs = [(f, d) for f in files for d in _documents(f)]
    with ThreadPoolExecutor(max_workers=gemini.workers()) as pool:
        list(pool.map(lambda job: _ask(*job), jobs))
    log.info(gemini.runner().stats.line())


def _date(value) -> str | None:
    text = str(value or "")[:10]
    return text if len(text) == 10 and text[4] == "-" else None


def parse_chart_values(f: RawFile) -> pl.DataFrame | None:
    rows = []
    for doc in _documents(f):
        record = _ask(f, doc)
        if not record:
            continue
        answer = record.get("answer") or {}
        for item in answer.get("items") or []:
            try:
                value = float(item.get("value"))
            except (TypeError, ValueError):
                continue
            unit = str(item.get("unit") or "").strip()
            rows.append({
                "report_date": f.dt,
                "document": doc.name,
                "as_of": _date(answer.get("as_of")),
                "page": item.get("page"),
                "chart_title": item.get("chart_title"),
                "status_label": item.get("status_label"),
                "status_bucket": item.get("status_bucket"),
                "category": None if item.get("category") is None else str(item.get("category")),
                "category_type": item.get("category_type"),
                "value": value,
                "unit": unit,
                "value_mw": value * 1000 if unit == "GW" else value if unit == "MW" else None,
                "confidence": item.get("confidence"),
                "extraction_method": "gemini",
                "model": record.get("model"),
                "prompt_version": record.get("prompt_version"),
                "verified": False,
            })
    if not rows:
        return None
    df = pl.DataFrame(rows, schema={**SCHEMA, "as_of": pl.String})
    return df.with_columns(pl.col("as_of").str.to_date(strict=False))
