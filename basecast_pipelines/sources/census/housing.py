"""ACS 5-year, table B25032 (tenure by units in structure), from the ACS table-based Summary File: one
pipe-delimited file per table covering every geography (counties are ``GEO_ID`` 0500000US48xxx), plus the
5-year table shells for labels. No API key needed (the Census Data API now requires one).
Latest vintage by default. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_acs"
SUMMARY_FILE_ROOT = "https://www2.census.gov/programs-surveys/acs/summary_file/"

_YEAR_DIR = re.compile(r"/summary_file/(\d{4})/$")


def _dir_link(http: HttpClient, url: str, name: str) -> str | None:
    return next((link.url for link in fetch_links(http, url) if link.url.rstrip("/").endswith("/" + name)), None)


def discover(http: HttpClient, *, table: str = "b25032", vintages: int = 1) -> list[RemoteFile]:
    today = local_today()
    year_urls: dict[int, str] = {}
    for link in fetch_links(http, SUMMARY_FILE_ROOT):
        match = _YEAR_DIR.search(link.url)
        if match:
            year_urls[int(match.group(1))] = link.url
    files: list[RemoteFile] = []
    for year, year_url in sorted(year_urls.items(), reverse=True):
        if len({f.meta["vintage"] for f in files}) >= int(vintages):
            break
        table_sf = _dir_link(http, year_url, "table-based-SF")
        if table_sf is None:
            continue
        data_dir = _dir_link(http, table_sf, "data")
        five_year = _dir_link(http, data_dir, "5YRData") if data_dir else None
        docs_dir = _dir_link(http, table_sf, "documentation")
        if five_year is None:
            continue
        data_name = f"acsdt5y{year}-{table.lower()}.dat"
        data = [link for link in fetch_links(http, five_year) if link.filename == data_name]
        if not data:
            continue
        files.append(RemoteFile(url=data[0].url, dt=today, source_page=five_year, meta={"vintage": year, "table": table}))
        if docs_dir:
            for link in fetch_links(http, docs_dir):
                if link.filename == f"ACS{year}5YR_Table_Shells.txt":
                    files.append(RemoteFile(url=link.url, dt=today, source_page=docs_dir, meta={"vintage": year}))
    return files


run = source_runner(SOURCE_ID, discover)
