"""ERCOT Fuel Mix Report: generation by fuel for every 15-minute settlement interval, one workbook per year
(sheet per month) plus a 2007-2024 zip, from the "Fuel Mix" section of the Generation page. No EMIL id.
Header text varies even within a workbook: read headers dynamically. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ercot_fuel_mix"
PAGE = "https://www.ercot.com/gridinfo/generation"

_FUEL_MIX = re.compile(r"Fuel Mix", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    return [
        RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"link_text": link.text})
        for link in file_links(fetch_links(http, PAGE), (".xlsx", ".xls", ".zip"))
        if _FUEL_MIX.search(link.text) or _FUEL_MIX.search(link.section)
    ]


run = source_runner(SOURCE_ID, discover)
