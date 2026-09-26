"""TCEQ Central Registry, air permit rows (program AIRNSR: permits by rule, standard and NSR permits) from
the five regional Central Registry datasets on data.texas.gov (found with the Socrata catalog API).
Data centers register emergency generators here, often before construction, with county and
PENDING/ACTIVE status: an early county-level signal of large loads. CSV pages of 50,000 rows ordered by
``:id``; NAICS is unreliable, match names at parse. ``dt`` is the fetch date."""

from __future__ import annotations

import httpx

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "tceq_air_permits"
CATALOG = "https://api.us.socrata.com/api/catalog/v1"
DOMAIN = "data.texas.gov"
TITLE_PREFIX = "Texas Commission on Environmental Quality - Central Registry Files - "


def discover(http: HttpClient, *, program: str = "AIRNSR", page_size: int = 50000) -> list[RemoteFile]:
    today = local_today()
    catalog = http.get(CATALOG, params={"domains": DOMAIN, "q": "Central Registry", "limit": "100"}).json()
    datasets = [
        (r["resource"]["id"], r["resource"]["name"].removeprefix(TITLE_PREFIX))
        for r in catalog["results"]
        if r["resource"]["name"].startswith(TITLE_PREFIX)
    ]
    where = f"program_code='{program}'"
    files = []
    for dataset_id, region in datasets:
        resource = f"https://{DOMAIN}/resource/{dataset_id}"
        count_rows = http.get(f"{resource}.json", params={"$select": "count(*)", "$where": where}).json()
        count = int(next(iter(count_rows[0].values())))
        slug = region.lower().replace(" / ", "_").replace(" & ", "_").replace(" ", "_")
        for offset in range(0, count, int(page_size)):
            params = {"$where": where, "$order": ":id", "$limit": str(page_size), "$offset": str(offset)}
            files.append(
                RemoteFile(
                    url=str(httpx.URL(f"{resource}.csv", params=params)),
                    dt=today,
                    filename=f"{dataset_id}_{slug}_{program.lower()}_{offset:07d}.csv",
                    source_page=f"https://{DOMAIN}/d/{dataset_id}",
                    meta={"dataset_id": dataset_id, "region": region, "program": program,
                          "row_count": count, "offset": offset},
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
