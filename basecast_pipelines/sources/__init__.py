"""Source registry: source id (the ``raw/source=<id>/`` prefix, from SCRAPING_RUNBOOK §1) → module with ``run()``.
Listed in the runbook's execution order."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

SOURCE_MODULES: dict[str, str] = {
    "ercot_gis": "basecast_pipelines.sources.ercot.gis",
    "ercot_ziptozone": "basecast_pipelines.sources.ercot.ziptozone",
    "census_zcta_county": "basecast_pipelines.sources.census.zcta_county",
    "census_tx_counties_geo": "basecast_pipelines.sources.census.counties_geo",
    "ercot_native_load": "basecast_pipelines.sources.ercot.load_archive",
    "ercot_load_wz_daily": "basecast_pipelines.sources.ercot.load_wz_daily",
    "ercot_ltlf": "basecast_pipelines.sources.ercot.ltlf",
    "ercot_cdr": "basecast_pipelines.sources.ercot.cdr",
    "ercot_large_load_decks": "basecast_pipelines.sources.ercot.large_load",
    "census_bps": "basecast_pipelines.sources.census.permits",
    "census_acs": "basecast_pipelines.sources.census.housing",
    "open_meteo": "basecast_pipelines.sources.weather.open_meteo",
    "eia_territories": "basecast_pipelines.sources.territories.eia_atlas",
    "eia_861": "basecast_pipelines.sources.territories.eia_861",
    "ercot_spp_hist": "basecast_pipelines.sources.ercot.spp_hist",
    "ercot_mora": "basecast_pipelines.sources.ercot.mora",
}


class UnknownSourceError(KeyError):
    pass


def get_source(source_id: str) -> ModuleType:
    if source_id not in SOURCE_MODULES:
        raise UnknownSourceError(source_id)
    return import_module(SOURCE_MODULES[source_id])


def describe(source_id: str) -> str:
    try:
        doc = get_source(source_id).__doc__ or ""
    except ImportError:
        return "(not implemented yet)"
    return " ".join(doc.strip().split("\n\n")[0].split())
