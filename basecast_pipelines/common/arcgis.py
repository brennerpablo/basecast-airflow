"""ArcGIS feature layers as raw files: the layer metadata plus GeoJSON pages of a query, ordered by OBJECTID."""

from __future__ import annotations

from datetime import date

import httpx

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile


def with_params(url: str, params: dict) -> str:
    return str(httpx.URL(url, params=params))


def layer_files(
    http: HttpClient,
    layer_url: str,
    *,
    dt: date,
    prefix: str,
    where: dict | None = None,
    page_size: int = 25,
    meta: dict | None = None,
) -> list[RemoteFile]:
    """``<prefix>layer.json`` plus ``<prefix>features_<offset>.geojson`` pages covering every matching feature."""
    where = where or {"where": "1=1"}
    count = http.get(f"{layer_url}/query", params={**where, "returnCountOnly": "true", "f": "json"}).json()["count"]
    meta = {**(meta or {}), "layer_url": layer_url, "feature_count": count}
    files = [RemoteFile(url=with_params(layer_url, {"f": "json"}), dt=dt, filename=f"{prefix}layer.json", meta=meta)]
    for offset in range(0, count, int(page_size)):
        params = {
            **where,
            "outFields": "*",
            "outSR": "4326",
            "orderByFields": "OBJECTID",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
            "f": "geojson",
        }
        files.append(
            RemoteFile(
                url=with_params(f"{layer_url}/query", params),
                dt=dt,
                filename=f"{prefix}features_{offset:05d}.geojson",
                source_page=layer_url,
                meta={**meta, "offset": offset, "page_size": int(page_size)},
            )
        )
    return files
