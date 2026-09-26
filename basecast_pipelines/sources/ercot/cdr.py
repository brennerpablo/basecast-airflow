"""Capacity, Demand and Reserves (CDR) reports: every spreadsheet in the CDR sections of the Resource Adequacy
page and its year archive pages (2000 onward), including revised re-issues, companion capacity-percentage
workbooks and the May 2026 "Generation Resource Capacity Forecast" that replaced that edition.
PDFs are opt-in. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import file_links, parse_month_year
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.ercot._web import RESOURCE_ADEQUACY_PAGE, crawl_with_year_pages, year_of_page

SOURCE_ID = "ercot_cdr"

SPREADSHEETS = (".xlsx", ".xls", ".xlsb", ".zip")
_CDR = re.compile(r"Capacity,? Demand|\bCDR\b", re.IGNORECASE)


def discover(http: HttpClient, *, include_pdf: bool = False) -> list[RemoteFile]:
    today = local_today()
    extensions = SPREADSHEETS + ((".pdf",) if include_pdf else ())
    files: list[RemoteFile] = []
    seen: set[str] = set()
    for page, links in crawl_with_year_pages(http, RESOURCE_ADEQUACY_PAGE):
        for link in file_links(links, extensions):
            if link.url in seen or not (_CDR.search(link.section) or _CDR.search(link.text)):
                continue
            seen.add(link.url)
            edition = parse_month_year(link.text)
            files.append(
                RemoteFile(
                    url=link.url,
                    dt=today,
                    source_page=page,
                    meta={
                        "page_year": year_of_page(page),
                        "section": link.section,
                        "link_text": link.text,
                        "edition_month": edition.isoformat() if edition else None,
                    },
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
