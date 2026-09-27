"""Census Building Permits Survey, from the BPS data directory reached from the BPS page:

- county level, ``County/``: every annual file (``coYYYYa.txt``) and, optionally, every current-month file
  (``coYYMMc.txt``). National files; Texas is filtered at parse time;
- place level (permit offices), ``Place/South Region/`` (Texas is in the Census South region): the newest
  ``place_annual_years`` annual files (``soYYYYa.txt``), and the newest year-to-date file (``soYYMMy.txt``) with the
  same month a year earlier. Read from the lake by ``marts/muni_places.py`` (city permits and the city
  permit-surge trigger); no parser reads them.

``dt`` is the fetch date."""

from __future__ import annotations

import re
from datetime import date
from urllib.parse import unquote

from basecast_pipelines.common.discovery import Link, fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_bps"
PAGE = "https://www.census.gov/construction/bps/"
PLACE_REGION = "South Region"  # the Census region directory that holds Texas's permit offices

_DATA_ROOT = re.compile(r"www2\.census\.gov/econ/bps/?$")
_ANNUAL = re.compile(r"^co(\d{4})a\.txt$")
_MONTHLY = re.compile(r"^co(\d{2})(\d{2})c\.txt$")
_PLACE_ANNUAL = re.compile(r"^so(\d{4})a\.txt$")
_PLACE_YTD = re.compile(r"^so(\d{2})(\d{2})y\.txt$")


def _data_root(http: HttpClient) -> str:
    root = next((link.url for link in fetch_links(http, PAGE) if _DATA_ROOT.search(link.url)), None)
    if root is None:
        raise RuntimeError(f"BPS data directory link not found on {PAGE}")
    return root


def _subdir(links: list[Link], parent: str, name: str) -> str:
    """The directory named ``name`` among the ``links`` of the listing of ``parent`` (``South%20Region`` matches
    ``South Region``)."""
    url = next((link.url for link in links if unquote(link.url).rstrip("/").endswith("/" + name)), None)
    if url is None:
        raise RuntimeError(f"{name} directory not found in {parent}")
    return url


def _ytd_month(yy: str, mm: str, *, today: date) -> date:
    """First day of the month of ``soYYMMy.txt``. The place files go back to the 1980s, so the century is the one
    that does not put the month after ``today``."""
    year = 2000 + int(yy)
    return date(year if year <= today.year else year - 100, int(mm), 1)


def place_files(names: list[str], *, today: date, annual_years: int = 4) -> list[tuple[str, dict]]:
    """The place files to fetch from a region listing, as ``(file name, manifest meta)``: the newest
    ``annual_years`` annual files, then the same month a year before the newest year-to-date month (when listed) and
    that newest month."""
    annual = sorted(((int(m.group(1)), n) for n in names if (m := _PLACE_ANNUAL.match(n))), reverse=True)
    chosen = [
        (n, {"kind": "place_annual", "region": PLACE_REGION, "year": y})
        for y, n in sorted(annual[: max(int(annual_years), 0)])
    ]
    ytd = {_ytd_month(m.group(1), m.group(2), today=today): n for n in names if (m := _PLACE_YTD.match(n))}
    if ytd:
        newest = max(ytd)
        for month in (date(newest.year - 1, newest.month, 1), newest):
            if month in ytd:
                meta = {"kind": "place_ytd", "region": PLACE_REGION, "year": month.year, "month": month.month}
                chosen.append((ytd[month], meta))
    return chosen


def discover(
    http: HttpClient, *, include_monthly: bool = True, include_places: bool = True, place_annual_years: int = 4
) -> list[RemoteFile]:
    today = local_today()
    root = _data_root(http)
    root_links = fetch_links(http, root)
    county_dir = _subdir(root_links, root, "County")
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
    if include_places:
        place_dir = _subdir(root_links, root, "Place")
        region_dir = _subdir(fetch_links(http, place_dir), place_dir, PLACE_REGION)
        listed = {link.filename: link.url for link in fetch_links(http, region_dir) if link.url.startswith(region_dir)}
        for name, meta in place_files(list(listed), today=today, annual_years=place_annual_years):
            files.append(RemoteFile(url=listed[name], dt=today, source_page=region_dir, meta=meta))
    return files


run = source_runner(SOURCE_ID, discover)
