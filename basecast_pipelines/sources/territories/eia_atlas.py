"""Electric Retail Service Territories (HIFLD layer), every polygon intersecting Texas' bounding box, as
GeoJSON pages at full precision, plus the item, service and layer metadata.

The EIA Energy Atlas page named in the catalog now returns 404 and HIFLD Open was shut down. We read the
ORNL-hosted ArcGIS Online copy (item below), described by its owner as downloaded from HIFLD on 2025-08-21
and no longer updated. A bounding-box query (not ``STATE='TX'``, which is the utility's mailing state)
also catches utilities headquartered out of state. ``dt`` is the fetch date.
"""

from __future__ import annotations

from basecast_pipelines.common.arcgis import layer_files, with_params
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "eia_territories"
ITEM_ID = "597555ce8e4a4892a030784a7c657fdd"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}"
# Texas extent in WGS84 (lon/lat): west, south, east, north.
TX_BBOX = (-106.65, 25.83, -93.50, 36.51)


def discover(http: HttpClient, *, page_size: int = 25) -> list[RemoteFile]:
    today = local_today()
    item = http.get(ITEM_URL, params={"f": "json"}).json()
    service_url = item["url"].rstrip("/")
    service = http.get(service_url, params={"f": "json"}).json()
    layer_url = f"{service_url}/{service['layers'][0]['id']}"
    spatial = {
        "where": "1=1",
        "geometry": ",".join(str(v) for v in TX_BBOX),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
    }
    meta = {"item_id": ITEM_ID, "bbox": TX_BBOX}
    files = [
        RemoteFile(url=with_params(ITEM_URL, {"f": "json"}), dt=today, filename="item.json", meta=meta),
        RemoteFile(url=with_params(service_url, {"f": "json"}), dt=today, filename="service.json", meta=meta),
    ]
    return files + layer_files(http, layer_url, dt=today, prefix="", where=spatial, page_size=page_size, meta=meta)


run = source_runner(SOURCE_ID, discover)
