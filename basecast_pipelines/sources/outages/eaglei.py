"""EAGLE-I power outage data (ORNL / DOE CESER, figshare article 24237376, CC BY 4.0): customers out by
county every 15 minutes, 2014-11 onward, one national CSV per year (~1.1-1.4 GB each, no Texas subset),
plus small side files (MCC: customers per county; coverage history; data quality index by FEMA region).
Files and MD5s come from the figshare API. By default only the side files are taken; yearly files need
``--opt years=2021,2022`` (ask first: they are over 1 GB each). Headers change by year: read them
dynamically. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ornl_eaglei"
ARTICLE_ID = 24237376
API = f"https://api.figshare.com/v2/articles/{ARTICLE_ID}"
SIDE_FILES = ("MCC.csv", "coverage_history.csv", "DQI.csv")

_YEARLY = re.compile(r"^eaglei_outages_(\d{4})\.csv$", re.IGNORECASE)


def discover(http: HttpClient, *, years: str | int | None = None) -> list[RemoteFile]:
    today = local_today()
    wanted_years = {int(y) for y in str(years).split(",")} if years not in (None, "") else set()
    article = http.get(API).json()
    files = []
    for f in article["files"]:
        match = _YEARLY.match(f["name"])
        if f["name"] in SIDE_FILES or (match and int(match.group(1)) in wanted_years):
            files.append(
                RemoteFile(
                    url=f["download_url"],
                    dt=today,
                    filename=f["name"],
                    source_page=f"https://doi.org/{article.get('doi', '10.6084/m9.figshare.24237376')}",
                    immutable=True,
                    meta={"article_version": article.get("version"), "published": article.get("published_date"),
                          "listed_bytes": f.get("size"), "md5": f.get("computed_md5"),
                          "license": (article.get("license") or {}).get("name")},
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
