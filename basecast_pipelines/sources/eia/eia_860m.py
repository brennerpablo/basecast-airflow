"""EIA-860M monthly generator inventory (operating, planned, retired, canceled/postponed generators with
status, planned dates, county and balancing authority). By default the latest month plus every December
since 2015 (a yearly panel); ``--opt all_months=true`` takes all ~130 monthly files (~1 GB). Links come from
the EIA-860M page (future months sit in HTML comments, which the link parser ignores). ``dt`` is the fetch
date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "eia_860m"
PAGE = "https://www.eia.gov/electricity/data/eia860m/"

_MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
           "november", "december"]  # fmt: skip
_FILE = re.compile(r"^([a-z]+)_generator(\d{4})\.xlsx?$", re.IGNORECASE)


def discover(http: HttpClient, *, all_months: bool = False) -> list[RemoteFile]:
    today = local_today()
    candidates = []
    for link in fetch_links(http, PAGE):
        match = _FILE.match(link.filename)
        if match and match.group(1).lower() in _MONTHS:
            month = _MONTHS.index(match.group(1).lower()) + 1
            candidates.append(((int(match.group(2)), month), link))
    if not candidates:
        return []
    latest = max(key for key, _ in candidates)
    files, seen = [], set()
    for (year, month), link in sorted(candidates, key=lambda c: c[0]):
        if link.url in seen or not (all_months or month == 12 or (year, month) == latest):
            continue
        seen.add(link.url)
        files.append(RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"year": year, "month": month}))
    return files


run = source_runner(SOURCE_ID, discover)
