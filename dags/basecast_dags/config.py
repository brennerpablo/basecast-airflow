"""Schedules and runtime knobs for every source DAG. Crons run in America/Chicago (the DAGs' start_date).

Cadences follow each source's publication rhythm (``catalog.yaml`` ``frequency``): daily for the
short-window listings that would otherwise age out, weekly for monthly reports and daily-regenerated
registries (weekly keeps immutable raw snapshots of big files from piling up), monthly for yearly
files that are overwritten in place, and a cheap monthly check (ETag / document id skips) for annual or
irregular releases. Sources whose release is pinned or frozen are manual only (``schedule=None``)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta

import pendulum

TIMEZONE = "America/Chicago"
START_DATE = pendulum.datetime(2026, 9, 1, tz=TIMEZONE)
DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=15)}

FULL_DAG_ID = "dag_source_full"

# Pools (created by deploy/airflow/pools.json): every ercot.com fetch shares ERCOT's 20 req/min budget,
# Open-Meteo allows ~3 req/min, and the VM leaves ~2.5 GB of memory to tasks, so heavy parses run alone.
ERCOT_POOL = "ercot_http"
OPEN_METEO_POOL = "open_meteo"
HEAVY_POOL = "heavy_process"


@dataclass(frozen=True)
class SourceSchedule:
    cron: str | None
    # Discovery options for the scheduled (incremental) run, from the run's date.
    options: Callable[[date], dict] = field(default=lambda today: {})
    fetch_pool: str = "default_pool"
    heavy: bool = False
    fetch_timeout: timedelta = timedelta(hours=2)
    process_timeout: timedelta = timedelta(hours=2)


def _ercot(cron: str | None, **kw) -> SourceSchedule:
    return SourceSchedule(cron, fetch_pool=ERCOT_POOL, **kw)


SCHEDULES: dict[str, SourceSchedule] = {
    # Daily: listings that keep only ~31 days, or only the latest file.
    "ercot_load_wz_daily": _ercot("15 6 * * *"),
    "ercot_mp_list": _ercot("30 6 * * *"),
    "puct_directories": SourceSchedule("45 6 * * *"),
    # Weekly.
    "ercot_gis": _ercot("0 7 * * 1", options=lambda t: {"include_year_pages": False}, heavy=True),
    "ercot_settlement_points": _ercot("30 7 * * 1"),
    "ercot_large_load_decks": _ercot("0 8 * * 1", options=lambda t: {"since_year": t.year}, heavy=True),
    "puct_filings": SourceSchedule("0 5 * * 2", heavy=True, process_timeout=timedelta(hours=4)),
    "noaa_ghcnh": SourceSchedule(
        "0 4 * * 2", options=lambda t: {"start_year": t.year - 1 if t.month < 4 else t.year}, heavy=True
    ),
    "open_meteo": SourceSchedule("0 3 * * 3", fetch_pool=OPEN_METEO_POOL),
    "tceq_air_permits": SourceSchedule("0 5 * * 3", heavy=True),
    "tx_comptroller": SourceSchedule("30 5 * * 3"),
    # Monthly: yearly files the source overwrites in place, monthly reports.
    "ercot_native_load": _ercot("0 7 10 * *", options=lambda t: {"min_year": t.year - 1}),
    "ercot_demand_energy": _ercot("30 7 10 * *"),
    "ercot_fuel_mix": _ercot("0 8 10 * *", heavy=True),
    "ercot_spp_hist": _ercot("0 9 10 * *", heavy=True, process_timeout=timedelta(hours=4)),
    "ercot_mora": _ercot("0 7 12 * *", options=lambda t: {"min_year": t.year - 1}),
    "ercot_tpit": _ercot("30 8 12 * *"),
    "census_bps": SourceSchedule("0 6 20 * *"),
    "eia_860m": SourceSchedule("0 7 26 * *"),
    # Monthly check of annual or irregular releases (unchanged files are skipped by ETag / doc id).
    "ercot_ltlf": _ercot(
        "0 6 2 * *", options=lambda t: {"include_weather_scenarios": True}, heavy=True,
        fetch_timeout=timedelta(hours=3), process_timeout=timedelta(hours=6),
    ),
    "ercot_cdr": _ercot("30 6 2 * *"),
    "ercot_members": _ercot("0 7 2 * *"),
    "ercot_rtp": _ercot("30 7 2 * *"),
    "ercot_ziptozone": _ercot("0 8 2 * *"),
    "eia_861": SourceSchedule("0 7 3 * *", options=lambda t: {"min_year": t.year - 2}),
    "eia_860": SourceSchedule("30 7 3 * *", options=lambda t: {"min_year": t.year - 1}),
    "census_acs": SourceSchedule("0 8 3 * *"),
    "census_pep": SourceSchedule("30 8 3 * *"),
    "puct_ccn_territories": SourceSchedule("0 9 3 * *"),
    "census_tx_counties_geo": SourceSchedule("30 9 3 * *"),
    # Quarterly.
    "bls_qcew": SourceSchedule("0 7 15 1,4,7,10 *", options=lambda t: {"start_year": t.year - 2}),
    # Manual only: decennial, frozen or pinned releases, and the 1 GB rule (EAGLE-I yearly files).
    "census_zcta_county": SourceSchedule(None),
    "eia_territories": SourceSchedule(None),
    "pudl": SourceSchedule(None),
    "ornl_eaglei": SourceSchedule(None),
}

# Datasets with no raw source (files versioned in this repo): processed on their own schedule.
PROCESS_ONLY_SCHEDULES: dict[str, SourceSchedule] = {
    "config_facts": SourceSchedule("0 6 * * *"),
}
