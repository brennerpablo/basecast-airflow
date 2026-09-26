"""Actual System Load by Weather Zone (NP6-345-CD) → ``ercot_load_hourly_wz``, the same table and shape as the
Hourly Load Data Archives (``load_archive``), for the days after the last monthly archive update.

Each zip holds one csv for one operating day: ``OperDay, HourEnding, COAST, EAST, FAR_WEST, NORTH, NORTH_C,
SOUTHERN, SOUTH_C, WEST, TOTAL, DSTFlag`` with ``HourEnding`` as ``01:00`` … ``24:00``. ``DSTFlag = Y`` marks
the repeated November hour (hour ending 02:00 appears twice). No DST day was in the listing when this parser
was written (2026-08-24 → 2026-09-24), so the DST handling is shared with the archives and covered by a test,
but not verified on a real NP6-345-CD file.

``ts_utc`` is the **end** of the hour-ending interval in UTC. Written ``by_key`` on (``ts_utc``,
``weather_zone``): whichever of the two sources is processed last wins an overlapping hour, and ``source``
says which one did. The two are not the same measurement: on the 8 overlap days checked (2026-08-24 →
2026-08-31, same hours, no time shift) the ERCOT totals agree within ±0.9% (bias +0.05%), but the zone split
differs systematically (archive minus NP6-345-CD: NORTH −11%, SCENT −1.7%, EAST +1.6%, COAST +1.0%). Expect a
small level step per zone where the series switches source.
"""

from __future__ import annotations

import io
import zipfile

import polars as pl

from basecast_pipelines.parsers.ercot._load_common import NP6345_TABLE, KEY, combined_load_table, hourly_long, zone_columns
from basecast_pipelines.processing.core import Dataset, RawFile

SOURCE_ID = "ercot_load_wz_daily"


def _column(columns: list[str], *needles: str) -> str:
    for c in columns:
        flat = c.lower().replace("_", "").replace(" ", "")
        if any(n in flat for n in needles):
            return c
    raise ValueError(f"no column like {needles} in {columns}")


def parse_load_wz_daily(f: RawFile) -> pl.DataFrame:
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        members = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"{f.key}: no csv in the zip")
        body = pl.concat([pl.read_csv(z.read(n), infer_schema=False) for n in members], how="diagonal")
    body = body.rename({c: c.strip() for c in body.columns})
    day_col = _column(body.columns, "operday", "deliverydate")
    hour_col = _column(body.columns, "hourending")
    dst_col = next((c for c in body.columns if "dst" in c.lower()), None)
    zones = zone_columns(body.columns)
    hour = body[hour_col].str.strip_chars().str.extract_groups(r"^(\d{1,2})(?::(\d{2}))?$")
    frame = body.select(
        pl.col(day_col).str.strip_chars().str.strptime(pl.Date, "%m/%d/%Y", strict=False).alias("operating_date"),
        hour.struct.field("1").cast(pl.Int16).alias("hour_ending"),
        hour.struct.field("2").fill_null("00").alias("_minutes"),
        (
            (pl.col(dst_col).str.strip_chars().str.to_uppercase() == "Y").fill_null(False) if dst_col
            else pl.lit(False)
        ).alias("dst_marker"),
        *zones,
    )
    bad = frame.filter(
        pl.col("operating_date").is_null() | ~pl.col("hour_ending").is_between(1, 24) | (pl.col("_minutes") != "00")
    )
    if bad.height:
        raise ValueError(f"{f.key}: {bad.height} rows with an unreadable operating day or hour ending")
    return hourly_long(frame.drop("_minutes"), zones, source=SOURCE_ID)


DATASETS = [
    Dataset(
        name=NP6345_TABLE,
        target="postgres",
        mode="by_key",
        key=KEY,
        description=(
            "Hourly actual load by ERCOT weather zone plus the ERCOT total (MW) from NP6-345-CD; same table as "
            "the Hourly Load Data Archives (combined in ercot_load_hourly_wz), filling the days after the last archive update."
        ),
        parse=parse_load_wz_daily,
        inputs=lambda f: f.suffix == ".zip" and "csv" in f.name.lower(),
    ),
]

SQL_DATASETS = [combined_load_table()]
