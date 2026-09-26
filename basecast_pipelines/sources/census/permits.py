"""Census Building Permits Survey, county level: every annual file (``coYYYYa.txt``) and, optionally, every
current-month file (``coYYMMc.txt``) from the BPS County directory, reached from the BPS page. National
files; Texas is filtered at parse time. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_bps"
PAGE = "https://www.census.gov/construction/bps/"

_DATA_ROOT = re.compile(r"www2\.census\.gov/econ/bps/?$")
_ANNUAL = re.compile(r"^co(\d{4})a\.txt$")
_MONTHLY = re.compile(r"^co(\d{2})(\d{2})c\.txt$")


def _county_dir(http: HttpClient) -> str:
    root = next((link.url for link in fetch_links(http, PAGE) if _DATA_ROOT.search(link.url)), None)
    if root is None:
        raise RuntimeError(f"BPS data directory link not found on {PAGE}")
    county = next((link.url for link in fetch_links(http, root) if link.url.rstrip("/").endswith("/County")), None)
    if county is None:
        raise RuntimeError(f"County directory not found in {root}")
    return county


def discover(http: HttpClient, *, include_monthly: bool = True) -> list[RemoteFile]:
    today = local_today()
    county_dir = _county_dir(http)
    files = []
    for link in fetch_links(http, county_dir):
        annual, monthly = _ANNUAL.match(link.filename), _MONTHLY.match(link.filename)
        if annual:
            meta = {"kind": "annual", "year": int(annual.group(1))}
        elif monthly and include_monthly:
            meta = {"kind": "monthly", "year": 2000 + int(monthly.group(1)), "month": int(monthly.group(2))}
        else:
            continue
        files.append(RemoteFile(url=link.url, dt=today, source_page=county_dir, meta=meta))
    return files


run = source_runner(SOURCE_ID, discover)
