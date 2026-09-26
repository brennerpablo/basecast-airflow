"""ERCOT Long-Term Load Forecast (``ercot_ltlf``): every vintage since 2013 into three tables.

- ``ltlf_forecasts`` (Postgres): the peak and energy workbooks (monthly peak/energy, summer coincident and
  non-coincident peaks by weather zone, 90th-percentile peaks, peak demand by historical weather year,
  winter peaks, weekly P90 peaks, the winter reliability-standard peak) in the forecast shape shared with
  the CDR parser: ``vintage, vintage_date, publisher, product, target_year, target_month, season,
  region_type, region_id, metric, scenario, value, unit, sheet, row_label, column_label``. The metric,
  scenario and season vocabulary is documented in ``_ltlf_tables.py``.
- ``ltlf_hourly`` (Postgres): the hourly forecast workbooks (2021 onward, xlsx and xlsb) in long format by
  region, with the published total in ``mw`` and its components (gross, base economic, EV, rooftop PV,
  large flexible load, contracts, officer letters) as columns; plus the winter 2025-2026 reliability-standard
  forecast by transmission operator.
- ``ltlf_weather_scenarios`` (BigQuery): the 2025 per-zone workbooks with the hourly forecast re-run under
  each historical weather year (1980-2024), long format: one row per zone, weather year and hour for the
  weather-driven ``pred`` component, plus the weather-independent components (EV, PV, LFL, contracts,
  officer letters) once with a null ``weather_year``. Net load for a weather year is the sum of ``pred``
  and those components (checked against the ERCOT Adjusted hourly workbook: same components, same values).

``vintage`` is ``LTLF <year>`` from the page year (or the year in the page section, else in the file's
URL); ``vintage_date`` is the date in the file's ercot.com URL (its posting date: files of 2013-2014 were
re-posted later, so their date is not the original publication date). PDF and Word reports stay in raw.
"""

from __future__ import annotations

import logging
import re
from datetime import date

import fastexcel
import polars as pl

from basecast_pipelines.parsers.ercot import _ltlf_hourly as hourly
from basecast_pipelines.parsers.ercot import _ltlf_tables as tables
from basecast_pipelines.processing.core import Dataset, RawFile

log = logging.getLogger(__name__)

SOURCE_ID = "ercot_ltlf"
PUBLISHER = "ERCOT"
PRODUCT = "LTLF"

SPREADSHEETS = {".xlsx", ".xlsm", ".xlsb", ".xls"}
DOCUMENTS = {".pdf", ".docx", ".doc", ".zip"}
# the hourly workbooks (12-49 MB) hold no peak/energy tables; the peak workbooks are all under 1 MB
_MAX_TABLE_WORKBOOK_BYTES = 5_000_000

_URL_DATE = re.compile(r"/files/docs/(\d{4})/(\d{2})/(\d{2})/")
_YEAR = re.compile(r"\b(20\d{2})\b")
# files whose tables are 90th-percentile values although some titles do not say so (2022 NCP workbook)
_P90_HINT = re.compile(r"90th.?percentile|p90(?!\d)", re.IGNORECASE)

FORECAST_SCHEMA = {
    "vintage": pl.String,
    "vintage_date": pl.Date,
    "publisher": pl.String,
    "product": pl.String,
    "target_year": pl.Int32,
    "target_month": pl.Int8,
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
}


# --- vintage ------------------------------------------------------------------------------------------------


def vintage_year(f: RawFile) -> int:
    if f.meta.get("page_year"):
        return int(f.meta["page_year"])
    for text in (f.meta.get("section") or "", f.meta.get("link_text") or ""):
        m = _YEAR.search(text)
        if m:
            return int(m.group(1))
    m = _URL_DATE.search(f.url)
    if m:
        return int(m.group(1))
    raise ValueError(f"{f.key}: cannot tell the LTLF vintage (no page year, section year or dated URL)")


def vintage_date(f: RawFile) -> date:
    m = _URL_DATE.search(f.url)
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else f.dt


def _vintage_columns(f: RawFile) -> list[pl.Expr]:
    return [
        pl.lit(f"LTLF {vintage_year(f)}").alias("vintage"),
        pl.lit(vintage_date(f)).alias("vintage_date"),
    ]


def _file_variant(f: RawFile) -> str | None:
    return tables.variant_of(f"{f.meta.get('link_text') or ''} {f.name.replace('-', ' ')}")


def is_weather_scenario(f: RawFile) -> bool:
    return "weather year scenario" in (f.meta.get("section") or "").lower()


# --- ltlf_forecasts -----------------------------------------------------------------------------------------


def _workbook_text(path) -> str:
    """Every text cell of a small workbook (explanations such as "75th percentile")."""
    reader = fastexcel.read_excel(path)
    parts = []
    for sheet in reader.sheet_names:
        grid = reader.load_sheet(sheet, header_row=None, dtypes="string", n_rows=50).to_polars()
        parts.extend(str(v) for row in grid.iter_rows() for v in row if v is not None)
    return " ".join(parts)


def parse_forecasts(f: RawFile) -> pl.DataFrame | None:
    if f.suffix in DOCUMENTS:
        return None  # PDF/Word reports stay in raw: their tables repeat the workbooks
    hint = f"{f.meta.get('link_text') or ''} {f.name}".lower()
    if "premise" in hint:
        return None  # premise counts and usage per premise (2014): customer counts, not peak or energy
    year = vintage_year(f)
    rows: list[tables.TableRow] = []
    with f.local_path() as path:
        reader = fastexcel.read_excel(path)
        text = _workbook_text(path)
        for sheet in reader.sheet_names:
            grid = reader.load_sheet(sheet, header_row=None, dtypes="string").to_polars()
            ctx = tables.SheetContext(
                sheet=sheet,
                variant=_file_variant(f),
                weather_hint="p90" if _P90_HINT.search(f"{hint} {sheet}") else None,
                workbook_text=text,
            )
            rows.extend(tables.parse_sheet(ctx, grid.iter_rows(), year))
    if not rows:
        raise ValueError(f"{f.key}: no peak or energy table found")
    df = pl.DataFrame(
        [r.__dict__ for r in rows],
        schema={k: v for k, v in FORECAST_SCHEMA.items() if k not in {"vintage", "vintage_date", "publisher", "product"}},
    )
    df = df.with_columns(*_vintage_columns(f), pl.lit(PUBLISHER).alias("publisher"), pl.lit(PRODUCT).alias("product"))
    return df.select([pl.col(c).cast(t) for c, t in FORECAST_SCHEMA.items()])


# --- ltlf_hourly --------------------------------------------------------------------------------------------

HOURLY_SCHEMA = {  # repeated labels as categoricals (text in Postgres), to keep a 1.6M-row workbook small
    "vintage": pl.Categorical,
    "vintage_date": pl.Date,
    "scenario": pl.Categorical,
    "region_type": pl.Categorical,
    "region_id": pl.Categorical,
    "operating_date": pl.Date,
    "hour_ending": pl.Int8,
    "hour_ending_local": pl.String,
    "dst_flag": pl.Boolean,
    "ts_utc": pl.Datetime("us", "UTC"),
    **{c: pl.Float64 for c in hourly.COMPONENTS},
}


def _rs_frames(f: RawFile, df: pl.DataFrame, weather: str | None) -> list[pl.DataFrame]:
    """The winter reliability-standard sheet: ERCOT with and without large loads, and the transmission
    operators' shares (checked to sum to the ERCOT total they split)."""
    header = list(df.columns)
    skip = {i for i, c in enumerate(header) if c.startswith("_") or c in hourly.TIME_COLUMNS}
    classified = tables.classify_rs_columns(header, skip=skip)
    variant = _file_variant(f)
    with_ll = next(header[i] for i, k, _ in classified if k == "with_ll")
    no_ll = next(header[i] for i, k, _ in classified if k == "no_ll")
    tos = [(header[i], name) for i, k, name in classified if k == "to"]
    first = df.row(0, named=True)
    to_suffix = tables.to_share_basis(sum(first[c] or 0.0 for c, _ in tos), first[with_ll], first[no_ll])
    frames = []
    for column, region_type, region_id, suffix in [
        (with_ll, "ercot", "ERCOT", None),
        (no_ll, "ercot", "ERCOT", "no_large_loads"),
        *[(c, "transmission_operator", name, to_suffix) for c, name in tos],
    ]:
        frames.append(
            df.select(
                *_vintage_columns(f),
                pl.lit(tables.scenario(variant, weather, suffix)).alias("scenario"),
                pl.lit(region_type).alias("region_type"),
                pl.lit(region_id).alias("region_id"),
                *hourly.TIME_COLUMNS,
                hourly.as_float(df, column).alias("mw"),
                *[pl.lit(None, pl.Float64).alias(c) for c in hourly.COMPONENTS if c != "mw"],
            )
        )
    return frames


def parse_hourly(f: RawFile) -> pl.DataFrame | None:
    frames: list[pl.DataFrame] = []
    with f.local_path() as path:
        for sheet in fastexcel.read_excel(path).sheet_names:
            df = hourly.load_hourly(path, sheet, big=f.entry.bytes > hourly.BIG_WORKBOOK_BYTES)
            if df is None:
                continue  # peak/energy tables, descriptions: not hourly
            if any("base load" in tables.norm(c) for c in df.columns):
                frames.extend(_rs_frames(f, df, tables.percentile_hint(_workbook_text(path))))
            else:
                constants = [*_vintage_columns(f), pl.lit(tables.scenario(_file_variant(f))).alias("scenario")]
                frames.extend(hourly.zone_component_frames(df, constants))
            del df
    if not frames:
        return None  # no hourly sheet (the peak and energy workbooks)
    return pl.concat([fr.select([pl.col(c).cast(t) for c, t in HOURLY_SCHEMA.items()]) for fr in frames])


# --- ltlf_weather_scenarios ---------------------------------------------------------------------------------

# Repeated labels are categoricals (4 bytes a row instead of 16): a workbook becomes ~4.8M rows, and the
# Parquet file keeps them as dictionary-encoded strings (STRING in BigQuery).
WEATHER_SCHEMA = {
    "vintage": pl.Categorical,
    "vintage_date": pl.Date,
    "scenario": pl.Categorical,
    "weather_zone": pl.Categorical,
    "weather_year": pl.Int16,
    "component": pl.Categorical,
    "operating_date": pl.Date,
    "hour_ending": pl.Int8,
    "hour_ending_local": pl.String,
    "dst_flag": pl.Boolean,
    "ts_utc": pl.Datetime("us", "UTC"),
    "mw": pl.Float64,
}
_PRED = re.compile(r"^pred_(\d{4})$", re.IGNORECASE)
# The 2025 workbooks' large-load columns (contracts, officer letters) carry the ERCOT Adjusted values, not the
# TSP Provided ones (checked against both hourly workbooks, e.g. NCENT 2030-08-01 HE17: 1,793 MW of contracts
# here and in ErcotAdjustedForecast.xlsb, 3,498 MW in TSP-Provided-Hourly-Forecast.xlsb).
_WEATHER_VARIANT = "ercot_adjusted"


def parse_weather_scenarios(f: RawFile) -> pl.DataFrame | None:
    frames: list[pl.DataFrame] = []
    with f.local_path() as path:
        for sheet in fastexcel.read_excel(path).sheet_names:
            df = hourly.load_hourly(path, sheet, big=f.entry.bytes > hourly.BIG_WORKBOOK_BYTES)
            if df is None:
                continue
            frames.extend(_weather_frames(f, sheet, df))
            del df
    if not frames:
        raise ValueError(f"{f.key}: no hourly weather-scenario sheet")
    return pl.concat(frames)


def _weather_zone(f: RawFile, sheet: str, df: pl.DataFrame, component_zones: set[str]) -> str:
    """The workbook's zone, from the component columns, the ``wzone`` column and the sheet name (must agree)."""
    zones = set(component_zones)
    wz = next((c for c in df.columns if tables.norm(c) == "wzone"), None)
    if wz is not None:
        for v in df[wz].drop_nulls().unique().to_list():
            zones.add(tables.region_of(str(v)) or str(v))
    if tables.region_of(sheet):
        zones.add(tables.region_of(sheet))
    if len(zones) != 1 or next(iter(zones)) not in tables.WEATHER_ZONES:
        raise ValueError(f"{f.key}: sheet {sheet!r} does not name a single weather zone ({sorted(zones)})")
    return next(iter(zones))


def _weather_frames(f: RawFile, sheet: str, df: pl.DataFrame) -> list[pl.DataFrame]:
    preds: list[tuple[int, str]] = []
    components: list[tuple[str, str]] = []
    component_zones: set[str] = set()
    for c in df.columns:
        if c.startswith("_") or c in hourly.TIME_COLUMNS or tables.norm(c) in {"date", "wzone"}:
            continue
        m = _PRED.match(c)
        if m:
            preds.append((int(m.group(1)), c))
            continue
        tokens = [t for t in re.split(r"[^0-9a-z]+", c.lower()) if t]
        zone = tables.region_of(tokens[0]) if tokens else None
        component = hourly.WEATHER_COMPONENTS.get("".join(tokens[1:]))
        if zone is None or component is None:
            raise ValueError(f"{f.key}: unexpected weather-scenario column {c!r}")
        component_zones.add(zone)
        components.append((component, c))
    if not preds:
        raise ValueError(f"{f.key}: sheet {sheet!r} has no Pred_<year> columns")
    constants = [
        *_vintage_columns(f),
        pl.lit(tables.scenario(_file_variant(f) or _WEATHER_VARIANT)).alias("scenario"),
        pl.lit(_weather_zone(f, sheet, df, component_zones)).alias("weather_zone"),
    ]
    series = [(pl.lit(year, pl.Int16), "pred", column) for year, column in preds]
    series += [(pl.lit(None, pl.Int16), component, column) for component, column in components]
    return [
        df.select(
            *constants, year.alias("weather_year"), pl.lit(component).alias("component"), *hourly.TIME_COLUMNS,
            hourly.as_float(df, column).alias("mw"),
        ).select([pl.col(c).cast(t) for c, t in WEATHER_SCHEMA.items()])
        for year, component, column in series
    ]


# --- datasets -----------------------------------------------------------------------------------------------


def _forecast_input(f: RawFile) -> bool:
    if is_weather_scenario(f):
        return False
    return f.suffix in DOCUMENTS or (f.suffix in SPREADSHEETS and f.entry.bytes < _MAX_TABLE_WORKBOOK_BYTES)


DATASETS = [
    Dataset(
        name="ltlf_forecasts",
        target="postgres",
        mode="by_file",
        description="ERCOT LTLF peak and energy forecasts by vintage, target year/month, season and region "
        "(weather zone or ERCOT), common official-forecast shape.",
        parse=parse_forecasts,
        inputs=_forecast_input,
        version=1,
    ),
    Dataset(
        name="ltlf_hourly",
        target="postgres",
        mode="by_file",
        description="ERCOT LTLF hourly forecasts (2021 onward) by vintage, scenario and region, net load and "
        "components; winter 2025-2026 reliability-standard forecast by transmission operator.",
        parse=parse_hourly,
        inputs=lambda f: f.suffix in SPREADSHEETS and not is_weather_scenario(f),
        version=1,
    ),
    Dataset(
        name="ltlf_weather_scenarios",
        target="bigquery",
        mode="by_file",
        description="ERCOT 2025 LTLF hourly forecast by weather zone under each historical weather year "
        "(1980-2024), long format.",
        parse=parse_weather_scenarios,
        inputs=is_weather_scenario,
        partition=("ts_utc", "MONTH"),
        cluster=("weather_zone", "weather_year"),
        version=1,
    ),
]
