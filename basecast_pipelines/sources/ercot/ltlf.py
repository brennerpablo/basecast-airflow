"""Long-Term Load Forecast: every file under the long-term sections of the Load Forecast page and its year
archive pages (2013 onward), i.e. monthly peak/energy, hourly forecasts, peak scenarios and reports.
Mid-Term (short-term backcast/metrics) sections are skipped. The per-weather-zone weather-year scenario
workbooks (~70 MB each) are opt-in. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.ercot._web import LOAD_FORECAST_PAGE, crawl_with_year_pages, year_of_page

SOURCE_ID = "ercot_ltlf"

EXTENSIONS = (".xlsx", ".xls", ".xlsb", ".docx", ".doc", ".pdf", ".zip")
_MID_TERM = re.compile(r"Mid-Term", re.IGNORECASE)
_WEATHER_SCENARIOS = re.compile(r"Weather Year Scenario", re.IGNORECASE)


def discover(http: HttpClient, *, include_weather_scenarios: bool = False) -> list[RemoteFile]:
    today = local_today()
    files: list[RemoteFile] = []
    seen: set[str] = set()
    for page, links in crawl_with_year_pages(http, LOAD_FORECAST_PAGE):
        for link in file_links(links, EXTENSIONS):
            if link.url in seen or _MID_TERM.search(link.section):
                continue
            if _WEATHER_SCENARIOS.search(link.section) and not include_weather_scenarios:
                continue
            seen.add(link.url)
            files.append(
                RemoteFile(
                    url=link.url,
                    dt=today,
                    source_page=page,
                    meta={
                        "page_year": year_of_page(page),
                        "section": link.section,
                        "link_text": link.text,
                    },
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
