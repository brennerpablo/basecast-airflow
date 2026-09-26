"""PUCT electric service areas (CCN): the co-op, municipal and IOU polygons behind the PUCT's public
"Electric Service Territory/Outage Map Locator Viewer" (ArcGIS item below), with CCN number, ISO and the
co-op's G&T. The layers describe themselves as unofficial (official maps are county mylars at PUCT Central
Records). FeatureServer URLs are read from the viewer's configuration. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.arcgis import layer_files, with_params
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "puct_ccn_territories"
ITEM_ID = "366445b63acd4dbda6d60f9244e89c23"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}"
LAYERS = ("COOP_DIST", "MUNI", "IOU")

_FEATURE_LAYER = re.compile(r"https://[^\"'\s]+?/rest/services/([A-Za-z_]+)/FeatureServer/\d+")


def discover(http: HttpClient, *, page_size: int = 25) -> list[RemoteFile]:
    today = local_today()
    config = http.get(f"{ITEM_URL}/data", params={"f": "json"}).text
    layer_urls = {m.group(1): m.group(0) for m in _FEATURE_LAYER.finditer(config) if m.group(1) in LAYERS}
    missing = set(LAYERS) - set(layer_urls)
    if missing:
        raise RuntimeError(f"layers {sorted(missing)} not found in the PUCT viewer configuration")
    files = [
        RemoteFile(url=with_params(ITEM_URL, {"f": "json"}), dt=today, filename="item.json", meta={"item_id": ITEM_ID}),
        RemoteFile(url=with_params(f"{ITEM_URL}/data", {"f": "json"}), dt=today, filename="item_data.json",
                   meta={"item_id": ITEM_ID}),
    ]
    for name in LAYERS:
        files += layer_files(http, layer_urls[name], dt=today, prefix=f"{name.lower()}_", page_size=page_size,
                             meta={"item_id": ITEM_ID, "layer": name})
    return files


run = source_runner(SOURCE_ID, discover)
