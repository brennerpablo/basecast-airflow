"""Shared pieces of the hourly load parsers (``load_archive`` and ``load_wz_daily``), which both write the
``ercot_load_hourly_wz`` table: weather-zone names, ERCOT hour-ending labels and the local → UTC conversion.

ERCOT publishes load by *operating day* (America/Chicago) and *hour ending* (HE 1–24, HE 24 ends at the next
midnight). Daylight saving time changes the number of hours in a day:

- the spring-forward day has 23 hours. The newer files skip the ``03:00`` label (``01:00, 02:00, 04:00``); the
  old xls archives print the real wall clock and skip ``02:00`` instead (``01:00, 03:00, 04:00``);
- the fall-back day has 25 hours: hour ending ``02:00`` appears twice and the second one is the repeated hour
  (``DSTFlag = Y`` in the MIS files, ``"02:00 DST"`` in the 2017+ archives, a plain duplicate in the xls
  archives).

``ts_utc`` is the **end** of the hour-ending interval, in UTC. It is computed from the local midnight that opens
the operating day plus the number of hours elapsed at the end of the interval, which makes both spring-forward
labelings and the repeated fall hour land on consecutive UTC hours.
"""

from __future__ import annotations

import re

import polars as pl

from basecast_pipelines.processing.tabular import num

TABLE = "ercot_load_hourly_wz"
ARCHIVE_TABLE = "ercot_load_hourly_wz_archive"
NP6345_TABLE = "ercot_load_hourly_wz_np6345"
KEY = ("ts_utc", "weather_zone")
LOCAL_TZ = "America/Chicago"
TOTAL = "ERCOT"
WEATHER_ZONES = ("COAST", "EAST", "FWEST", "NORTH", "NCENT", "SOUTH", "SCENT", "WEST")

# Header spellings seen in the archives (2003-2016: FAR_WEST, NORTH_C, SOUTHERN, SOUTH_C; 2017+: contract codes)
# and in NP6-345-CD (FAR_WEST, NORTH_C, SOUTHERN, SOUTH_C, TOTAL).
_ZONE_ALIASES = {
    "COAST": "COAST",
    "EAST": "EAST",
    "FAR_WEST": "FWEST",
    "FWEST": "FWEST",
    "NORTH": "NORTH",
    "NORTH_C": "NCENT",
    "NCENT": "NCENT",
    "SOUTHERN": "SOUTH",
    "SOUTH": "SOUTH",
    "SOUTH_C": "SCENT",
    "SCENT": "SCENT",
    "WEST": "WEST",
    "ERCOT": TOTAL,
    "TOTAL": TOTAL,
}

SCHEMA = {
    "ts_utc": pl.Datetime("us", "UTC"),
    "operating_date": pl.Date,
    "hour_ending": pl.Int16,
    "hour_ending_local": pl.Datetime("us"),
    "dst_flag": pl.Boolean,
    "weather_zone": pl.String,
    "mw": pl.Float64,
    "source": pl.String,
}


def zone_code(header: object) -> str | None:
    """Contract weather-zone code (or ``ERCOT`` for the total) for a column header, else ``None``."""
    text = re.sub(r"[^A-Z0-9]+", "_", str(header or "").upper()).strip("_")
    return _ZONE_ALIASES.get(text)


def zone_columns(columns: list[str]) -> dict[str, str]:
    """``{column: zone code}``; raises unless every weather zone and the total are present exactly once."""
    found: dict[str, str] = {}
    for c in columns:
        code = zone_code(c)
        if code is None:
            continue
        if code in found.values():
            raise ValueError(f"two columns map to {code}: {columns}")
        found[c] = code
    missing = set(WEATHER_ZONES) | {TOTAL}
    missing -= set(found.values())
    if missing:
        raise ValueError(f"missing zone columns {sorted(missing)} in {columns}")
    return found


_MDY_HOUR = r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})\s*(DST)?$"


def parse_hour_labels(labels: pl.Series) -> pl.DataFrame:
    """Hour-ending labels → ``operating_date``, ``hour_ending`` (1–24) and ``dst_marker``.

    Two spellings: ``"MM/DD/YYYY HH:MM"`` with ``24:00`` and an optional ``" DST"`` suffix on the repeated
    hour (2017+ archives), and an Excel datetime (2003-2016 archives) such as ``2003-01-01 01:59:59.997``,
    rounded to the hour, where midnight is hour ending 24 of the previous day. Raises on anything else."""
    s = labels.cast(pl.String).str.strip_chars()
    groups = s.str.extract_groups(_MDY_HOUR)
    mdy_date = pl.date(
        groups.struct.field("3").cast(pl.Int32), groups.struct.field("1").cast(pl.Int32),
        groups.struct.field("2").cast(pl.Int32),
    )
    stamp = pl.coalesce(
        s.str.strptime(pl.Datetime("ms"), "%Y-%m-%d %H:%M:%S%.f", strict=False),
        s.str.strptime(pl.Datetime("ms"), "%Y-%m-%d %H:%M:%S", strict=False),
        s.str.strptime(pl.Datetime("ms"), "%Y-%m-%d %H:%M", strict=False),
    ).dt.round("1h")
    is_mdy = groups.struct.field("1").is_not_null()
    iso_midnight = stamp.dt.hour() == 0
    out = pl.DataFrame({"label": s}).select(
        "label",
        pl.when(is_mdy).then(mdy_date)
        .when(iso_midnight).then(stamp.dt.date() - pl.duration(days=1))
        .otherwise(stamp.dt.date()).alias("operating_date"),
        pl.when(is_mdy).then(groups.struct.field("4").cast(pl.Int16))
        .when(iso_midnight).then(pl.lit(24, pl.Int16))
        .otherwise(stamp.dt.hour().cast(pl.Int16)).alias("hour_ending"),
        (is_mdy & (groups.struct.field("5") != "00")).alias("_bad_minutes"),
        groups.struct.field("6").is_not_null().alias("dst_marker"),
    )
    bad = out.filter(
        pl.col("operating_date").is_null() | pl.col("_bad_minutes") | ~pl.col("hour_ending").is_between(1, 24)
    )
    if bad.height:
        raise ValueError(f"{bad.height} unreadable hour-ending labels, e.g. {bad['label'].head(3).to_list()}")
    return out.drop("label", "_bad_minutes")


def hourly_long(frame: pl.DataFrame, zones: dict[str, str], *, source: str) -> pl.DataFrame:
    """Wide hourly rows → the long ``ercot_load_hourly_wz`` shape.

    ``frame`` holds ``operating_date``, ``hour_ending``, ``dst_marker`` and the zone columns named in ``zones``
    (``{column: zone code}``, values as text). A second row with the same operating day and hour ending is the
    repeated fall hour even when the source does not mark it. Rows without a value are dropped."""
    df = frame.with_columns(
        (pl.col("dst_marker") | (pl.int_range(pl.len()).over("operating_date", "hour_ending") > 0)).alias("dst_flag")
    )
    df = with_utc(df)
    long = (
        df.select("ts_utc", "operating_date", "hour_ending", "dst_flag", *zones)
        .rename(zones)
        .unpivot(index=["ts_utc", "operating_date", "hour_ending", "dst_flag"], variable_name="weather_zone",
                 value_name="value")
        .with_columns(num("value").alias("mw"))
        .filter(pl.col("mw").is_not_null())
    )
    dupes = long.filter(pl.struct(*KEY).is_duplicated())
    if dupes.height:
        raise ValueError(f"{dupes.height} rows share (ts_utc, weather_zone), e.g. {dupes.head(3).rows()}")
    return long.select(
        "ts_utc",
        "operating_date",
        "hour_ending",
        (pl.col("operating_date").cast(pl.Datetime("us")) + pl.duration(hours=pl.col("hour_ending")))
        .alias("hour_ending_local"),
        "dst_flag",
        "weather_zone",
        "mw",
        pl.lit(source).alias("source"),
    ).cast(SCHEMA)


def with_utc(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``ts_utc`` (end of the hour-ending interval) from ``operating_date``, ``hour_ending`` and
    ``dst_flag`` (see the module docstring). Raises on labels that cannot exist on that day."""

    def local_midnight(day: pl.Expr) -> pl.Expr:
        return day.cast(pl.Datetime("us")).dt.replace_time_zone(LOCAL_TZ).dt.convert_time_zone("UTC")

    day = pl.col("operating_date")
    he = pl.col("hour_ending").cast(pl.Int64)
    start = local_midnight(day)
    day_hours = (local_midnight(day + pl.duration(days=1)) - start).dt.total_hours()
    offset = (
        pl.when((day_hours == 23) & (he >= 3)).then(-1)
        .when((day_hours == 25) & ((he >= 3) | pl.col("dst_flag"))).then(1)
        .otherwise(0)
    )
    out = df.with_columns(
        (start + pl.duration(hours=he + offset)).dt.cast_time_unit("us").alias("ts_utc"),
        day_hours.alias("_day_hours"),
    )
    bad = out.filter(pl.col("dst_flag") & ((pl.col("_day_hours") != 25) | (pl.col("hour_ending") != 2)))
    if bad.height:
        raise ValueError(
            f"{bad.height} repeated-hour rows outside the fall-back hour, e.g. "
            f"{bad.select('operating_date', 'hour_ending').head(3).rows()}"
        )
    return out.drop("_day_hours")


def combined_load_table():
    """``ercot_load_hourly_wz``: the Hourly Load Data Archives, completed by NP6-345-CD only for the hours
    the archive does not have yet. Each source keeps its own table, so the archive always wins the overlap
    (the two split load between zones differently) and reprocessing either source cannot clobber the other.
    Declared by both parser modules, so it is rebuilt after either one runs."""
    from basecast_pipelines.processing.core import SqlDataset

    return SqlDataset(
        name=TABLE,
        sql=f"""
            SELECT a.* FROM {ARCHIVE_TABLE} a
            UNION ALL
            SELECT d.* FROM {NP6345_TABLE} d
            WHERE NOT EXISTS (
                SELECT 1 FROM {ARCHIVE_TABLE} a WHERE a.ts_utc = d.ts_utc AND a.weather_zone = d.weather_zone
            )
        """,
        description=(
            "Hourly load by ERCOT weather zone plus the ERCOT total (MW), long format; ts_utc is the end of the "
            "hour-ending interval. Archive 2003+, NP6-345-CD after the last archive update."
        ),
        indexes=(("weather_zone", "ts_utc"),),
    )
