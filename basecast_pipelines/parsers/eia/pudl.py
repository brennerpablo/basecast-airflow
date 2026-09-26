"""PUDL tables (pinned release, one Parquet file per table) → one Postgres table each, ``pudl_<table>``.

Data: Catalyst Cooperative, Public Utility Data Liberation (PUDL) project, https://catalyst.coop/pudl/,
licensed CC-BY-4.0 (https://creativecommons.org/licenses/by/4.0/). Attribution is required wherever these
tables are shown ("Source: Catalyst Cooperative PUDL, CC-BY-4.0"); the release is in ``pudl_release``.

Columns keep PUDL's names and types except the contract keys: ``utility_id_eia`` → ``utility_id`` (text),
``county_id_fips`` → ``county_fips``, and the EIA ids ``eia_code`` and
``transmission_distribution_owner_id`` as text. Categoricals become text and naive UTC timestamps
become ``timestamptz``.

Large national tables are cut to Texas / ERCOT (the whole file otherwise):

- ``core_eia930__hourly_subregion_demand``: balancing authority ERCO only (its 8 weather-zone
  subregions; ``weather_zone`` maps COAS → COAST, NCEN → NCENT... by code name);
- ``core_eia860m__changelog_generators``, ``out_eia__yearly_plants``,
  ``out_ferc714__respondents_with_fips``: state TX or balancing authority ERCO;
- ``core_eia861__yearly_service_territory``: state TX.

The FERC 714 respondent list, planning-area forecasts and summarized demand are small and kept whole
(ERCOT is ``respondent_id_ferc714`` 58)."""

from __future__ import annotations

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by
from basecast_pipelines.processing.tabular import snake

SOURCE_ID = "pudl"

_TX = pl.col("state") == "TX"
_ERCO = pl.col("balancing_authority_code_eia") == "ERCO"
_TX_OR_ERCO = _TX | _ERCO
_FILTERS: dict[str, tuple[pl.Expr | None, str]] = {
    "core_eia860m__changelog_generators": (_TX_OR_ERCO, "state TX or balancing authority ERCO"),
    "core_eia861__yearly_service_territory": (_TX, "state TX"),
    "core_eia930__hourly_subregion_demand": (_ERCO, "balancing authority ERCO"),
    "core_ferc714__respondent_id": (None, "all rows"),
    "core_ferc714__yearly_planning_area_demand_forecast": (None, "all rows"),
    "out_eia__yearly_plants": (_TX_OR_ERCO, "state TX or balancing authority ERCO"),
    "out_ferc714__respondents_with_fips": (_TX_OR_ERCO, "state TX or balancing authority ERCO"),
    "out_ferc714__summarized_demand": (None, "all rows"),
}
_RENAME = {"utility_id_eia": "utility_id", "county_id_fips": "county_fips"}
_ID_TEXT = ("utility_id", "eia_code", "transmission_distribution_owner_id")
# EIA-930 ERCO subregions are ERCOT's weather zones; the mapping is by code name.
_WEATHER_ZONES = {"COAS": "COAST", "EAST": "EAST", "FWES": "FWEST", "NCEN": "NCENT", "NRTH": "NORTH",
                  "SCEN": "SCENT", "SOUT": "SOUTH", "WEST": "WEST"}  # fmt: skip


def _table(f: RawFile) -> str:
    return f.meta.get("table") or f.name.removesuffix(".parquet")


def _parser(table: str):
    row_filter, _ = _FILTERS[table]

    def parse(f: RawFile) -> pl.DataFrame:
        with f.local_path() as path:
            lf = pl.scan_parquet(path)
            if row_filter is not None:
                lf = lf.filter(row_filter)
            df = lf.collect()
        df = df.rename({c: _RENAME.get(c, snake(c) if c != snake(c) else c) for c in df.columns})
        casts = []
        for name, dtype in df.schema.items():
            if isinstance(dtype, (pl.Categorical, pl.Enum)):
                casts.append(pl.col(name).cast(pl.String))
            elif isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
                casts.append(pl.col(name).dt.cast_time_unit("us").dt.replace_time_zone("UTC"))
            elif isinstance(dtype, (pl.List, pl.Array, pl.Struct)):
                raise ValueError(f"{f.key}: nested column {name!r} ({dtype}); encode it as JSON")
            if name in _ID_TEXT and dtype != pl.String:
                casts.append(pl.col(name).cast(pl.String))
        df = df.with_columns(casts) if casts else df
        if table == "core_eia930__hourly_subregion_demand":
            df = df.with_columns(
                pl.col("balancing_authority_subregion_code_eia")
                .replace_strict(_WEATHER_ZONES, default=None, return_dtype=pl.String)
                .alias("weather_zone")
            )
        return df.with_columns(pl.lit(f.meta.get("release"), pl.String).alias("pudl_release"))

    return parse


DATASETS = [
    Dataset(
        name=f"pudl_{table}",
        target="postgres",
        mode="by_file",
        description=f"PUDL {table} (Catalyst Cooperative, CC-BY-4.0), {how}.",
        parse=_parser(table),
        inputs=lambda f, table=table: f.suffix == ".parquet" and _table(f) == table,
        select=latest_by(_table),
    )
    for table, (_, how) in _FILTERS.items()
]
