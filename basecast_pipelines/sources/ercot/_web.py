"""Helpers for ercot.com pages that list files directly in their HTML."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from basecast_pipelines.common.discovery import Link, fetch_links
from basecast_pipelines.common.http import HttpClient

RESOURCE_ADEQUACY_PAGE = "https://www.ercot.com/gridinfo/resource"
LOAD_FORECAST_PAGE = "https://www.ercot.com/gridinfo/load/forecast"


def year_of_page(url: str) -> int | None:
    match = re.search(r"/(\d{4})/?$", urlsplit(url).path)
    return int(match.group(1)) if match else None


def crawl_with_year_pages(
    http: HttpClient, index_url: str, *, min_year: int | None = None
) -> list[tuple[str, list[Link]]]:
    """The index page plus every ``<index path>/<yyyy>`` archive page it links to, as (page, links)."""
    index_links = fetch_links(http, index_url)
    base_path = urlsplit(index_url).path.rstrip("/")
    pattern = re.compile(re.escape(base_path) + r"/(\d{4})/?$")
    year_urls = []
    for link in index_links:
        match = pattern.search(urlsplit(link.url).path)
        if match and (min_year is None or int(match.group(1)) >= min_year) and link.url not in year_urls:
            year_urls.append(link.url)
    pages = [(index_url, index_links)]
    pages += [(url, fetch_links(http, url)) for url in sorted(year_urls, key=year_of_page)]
    return pages
