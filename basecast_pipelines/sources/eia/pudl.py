"""Catalyst Cooperative PUDL (cleaned EIA / FERC data, CC-BY-4.0): selected Parquet tables from a pinned
release in PUDL's public bucket. FERC 714 planning-area forecasts carry ERCOT's official 10-year peak
forecasts for report years 2006 onward (backtest vintages); the EIA-860M changelog tracks every monthly
status change of planned generators; EIA-861 service territories list counties per utility. Attribution
required. ``dt`` is the fetch date."""

from __future__ import annotations

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.common.s3 import list_objects
from basecast_pipelines.config import local_today

SOURCE_ID = "pudl"
BUCKET = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop"
RELEASE = "v2026.9.0"
TABLES = (
    "core_ferc714__yearly_planning_area_demand_forecast",
    "core_ferc714__respondent_id",
    "out_ferc714__summarized_demand",
    "out_ferc714__respondents_with_fips",
    "core_eia861__yearly_service_territory",
    "out_eia__yearly_plants",
    "core_eia860m__changelog_generators",
    "core_eia930__hourly_subregion_demand",
)


def discover(http: HttpClient, *, release: str = RELEASE) -> list[RemoteFile]:
    today = local_today()
    wanted = {f"{release}/{table}.parquet": table for table in TABLES}
    return [
        RemoteFile(
            url=f"{BUCKET}/{obj.key}",
            dt=today,
            source_page=f"{BUCKET}?list-type=2&prefix={release}/",
            immutable=True,
            meta={"release": release, "table": wanted[obj.key], "listed_bytes": obj.size},
        )
        for obj in list_objects(http, BUCKET, f"{release}/")
        if obj.key in wanted
    ]


run = source_runner(SOURCE_ID, discover)
