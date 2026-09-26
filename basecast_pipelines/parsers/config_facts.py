"""Hand-curated facts versioned in ``config/`` (no raw files): official figures that have no tabular source,
public facts about Base Power's partners and areas, and the weather points per ERCOT weather zone.

Each dataset is rebuilt from its YAML file on every run (``build`` receives no raw files). Values are copied
as written, with their ``verified`` flags: ``manual_official_figures`` entries were checked against the
source documents (see the file header); every ``base_public_facts`` entry is ``verified: false``.
``manual_official_forecasts`` repeats the load-forecast figures in the common forecast shape, so they can be
stacked with the LTLF/CDR tables; ``row_label`` holds the figure id and ``column_label`` its page reference.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import yaml

from basecast_pipelines.config import PROJECT_ROOT
from basecast_pipelines.processing.core import Dataset, RawFile
from basecast_pipelines.parsers._forecasts import OFFICIAL_FORECASTS

CONFIG_DIR = PROJECT_ROOT / "config"
FIGURES = "manual_official_figures.yaml"
FACTS = "base_public_facts.yaml"
POINTS = "weather_points.yaml"


def _load(name: str, key: str, config_dir: Path | None = None) -> list[dict]:
    path = (config_dir or CONFIG_DIR) / name
    payload = yaml.safe_load(path.read_text()) or {}
    items = payload.get(key)
    if not isinstance(items, list):
        raise ValueError(f"{path}: expected a list under {key!r}")
    return items


def _date(value) -> date | None:
    """YAML dates arrive as ``date``; "2026-03" (month precision) becomes the first of the month."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt, size in (("%Y-%m-%d", 10), ("%Y-%m", 7), ("%Y", 4)):
        if len(text) == size:
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                return None
    return None


def _text(value) -> str | None:
    return None if value is None else str(value)


def _source(name: str) -> str:
    return f"config/{name}"


FIGURES_SCHEMA: dict[str, pl.DataType] = {
    "figure_id": pl.String,
    "category": pl.String,
    "metric": pl.String,
    "target": pl.String,
    "as_of_text": pl.String,
    "as_of_date": pl.Date,
    "mw": pl.Float64,
    "approx": pl.Boolean,
    "source": pl.String,
    "source_date": pl.Date,
    "source_url": pl.String,
    "page": pl.String,
    "verified": pl.Boolean,
    "notes": pl.String,
    "extraction_method": pl.String,
    "source_file": pl.String,
}


def build_figures(files: list[RawFile], *, config_dir: Path | None = None) -> pl.DataFrame:
    rows = [
        {
            "figure_id": item["id"],
            "category": item.get("category"),
            "metric": item.get("metric"),
            "target": _text(item.get("target")),
            "as_of_text": _text(item.get("as_of")),
            "as_of_date": _date(item.get("as_of")),
            "mw": float(item["mw"]) if item.get("mw") is not None else None,
            "approx": bool(item.get("approx")),
            "source": item.get("source"),
            "source_date": _date(item.get("source_date")),
            "source_url": item.get("source_url"),
            "page": _text(item.get("page")),
            "verified": bool(item.get("verified")),
            "notes": item.get("notes"),
            "extraction_method": "manual",
            "source_file": _source(FIGURES),
        }
        for item in _load(FIGURES, "figures", config_dir)
    ]
    return pl.DataFrame(rows, schema=FIGURES_SCHEMA)


FACTS_SCHEMA: dict[str, pl.DataType] = {
    "fact_id": pl.String,
    "category": pl.String,
    "fact": pl.String,
    "entity": pl.String,
    "source_url": pl.String,
    "source_date": pl.Date,
    "source_type": pl.String,
    "quote": pl.String,
    "verified": pl.Boolean,
    "source_file": pl.String,
}


def build_facts(files: list[RawFile], *, config_dir: Path | None = None) -> pl.DataFrame:
    rows = [
        {
            "fact_id": item["id"],
            "category": item.get("category"),
            "fact": item.get("fact"),
            "entity": item.get("entity"),
            "source_url": item.get("source_url"),
            "source_date": _date(item.get("source_date")),
            "source_type": item.get("source_type"),
            "quote": item.get("quote"),
            "verified": bool(item.get("verified")),
            "source_file": _source(FACTS),
        }
        for item in _load(FACTS, "facts", config_dir)
    ]
    return pl.DataFrame(rows, schema=FACTS_SCHEMA)


POINTS_SCHEMA: dict[str, pl.DataType] = {
    "point_id": pl.String,
    "weather_zone": pl.String,
    "city": pl.String,
    "icao": pl.String,
    "ghcnh_id": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "weight": pl.Float64,
    "source_file": pl.String,
}


def build_points(files: list[RawFile], *, config_dir: Path | None = None) -> pl.DataFrame:
    rows = [
        {
            "point_id": item["id"],
            "weather_zone": str(item["weather_zone"]).upper(),
            "city": item.get("city"),
            "icao": item.get("icao"),
            "ghcnh_id": item.get("ghcnh_id"),
            "latitude": float(item["latitude"]),
            "longitude": float(item["longitude"]),
            "weight": float(item["weight"]),
            "source_file": _source(POINTS),
        }
        for item in _load(POINTS, "points", config_dir)
    ]
    return pl.DataFrame(rows, schema=POINTS_SCHEMA)


# --- manual figures in the common forecast shape ---------------------------------------------------

FORECAST_SCHEMA: dict[str, pl.DataType] = {
    "vintage": pl.String,
    "vintage_date": pl.Date,
    "publisher": pl.String,
    "product": pl.String,
    "target_year": pl.Int32,
    "target_month": pl.Int32,
    "season": pl.String,
    "region_type": pl.String,
    "region_id": pl.String,
    "metric": pl.String,
    "scenario": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "sheet": pl.String,
    "row_label": pl.String,
    "column_label": pl.String,
    "source_file": pl.String,
}

# The metric names in the YAML map to (metric, scenario) of the common shape.
_FORECAST_METRICS = {
    "peak_demand_forecast": ("peak_demand", "base"),
    "peak_demand_projection_low": ("peak_demand", "range_low"),
    "peak_demand_projection_high": ("peak_demand", "range_high"),
}
_SEASONS = {"summer", "winter", "spring", "fall"}


def _target(text: str | None) -> tuple[int | None, int | None, str | None]:
    """"2029" → (2029, None, None); "2026-summer" → (2026, None, "summer"); "2026-07" → (2026, 7, None)."""
    if not text:
        return None, None, None
    parts = str(text).strip().lower().split("-")
    year = int(parts[0]) if parts[0].isdigit() else None
    rest = parts[1] if len(parts) > 1 else None
    if rest in _SEASONS:
        return year, None, rest
    if rest and rest.isdigit():
        return year, int(rest), None
    return year, None, None


def build_forecasts(files: list[RawFile], *, config_dir: Path | None = None) -> pl.DataFrame:
    rows = []
    for item in _load(FIGURES, "figures", config_dir):
        if item.get("category") != "load_forecast" or item.get("metric") not in _FORECAST_METRICS:
            continue
        metric, scenario = _FORECAST_METRICS[item["metric"]]
        year, month, season = _target(item.get("target"))
        source_date = _date(item.get("source_date"))
        rows.append({
            "vintage": f"{source_date.year} preliminary LTLF" if source_date else None,
            "vintage_date": source_date,
            "publisher": "ERCOT",
            "product": "ltlf",
            "target_year": year,
            "target_month": month,
            "season": season,
            "region_type": "system",
            "region_id": "ERCOT",
            "metric": metric,
            "scenario": scenario,
            "value": float(item["mw"]),
            "unit": "MW",
            "sheet": None,
            "row_label": item["id"],
            "column_label": _text(item.get("page")),
            "source_file": _source(FIGURES),
        })
    return pl.DataFrame(rows, schema=FORECAST_SCHEMA)


DATASETS = [
    Dataset(
        name="manual_official_figures",
        target="postgres",
        mode="replace",
        description=(
            "Official figures with no tabular source (preliminary 2026 LTLF peaks, summer 2026 range, large-load MW "
            "tracked, approved and observed peaks), typed from config/manual_official_figures.yaml with source URL, "
            "page and verified flag."
        ),
        build=build_figures,
    ),
    Dataset(
        name="manual_official_forecasts",
        target="postgres",
        mode="replace",
        description=(
            "The load-forecast entries of config/manual_official_figures.yaml in the common forecast shape (vintage, "
            "target year/season, metric, scenario, value); row_label is the figure id."
        ),
        build=build_forecasts,
    ),
    Dataset(
        name="base_public_facts",
        target="postgres",
        mode="replace",
        description=(
            "Public facts about Base Power's utility partners and service areas (config/base_public_facts.yaml), "
            "one per row with source URL, date, type and a verbatim quote; all verified=false."
        ),
        build=build_facts,
    ),
    Dataset(
        name="weather_points",
        target="postgres",
        mode="replace",
        description="Weather points per ERCOT weather zone (config/weather_points.yaml): city, ICAO, GHCNh id, lat/lon, weight.",
        build=build_points,
    ),
]

SQL_DATASETS = [OFFICIAL_FORECASTS]
