"""Census Population Estimates Program, county population 2000 onward, with each file's layout PDF.

Series (the ``series`` and ``vintage`` of each file are in the manifest):
- ``postcensal_latest``: newest ``YYYY-YYYY`` period, ``counties/totals/co-est*-alldata.csv`` (2020 onward,
  components of change);
- ``intercensal_2000_2010``: ``2000-2010/intercensal/county/co-est00int-tot.csv`` (all counties, totals);
- ``intercensal_2010_2020``: ``2010-2020/intercensal/county/asrh/cc-est2020int-agesex-48.csv`` (Texas;
  ``YEAR`` is a code, see the layout);
- ``postcensal_v2020``: ``2010-2020/counties/totals/co-est2020-alldata.csv`` (vintage 2020, components).

Everything is reached by walking the popest ``datasets/`` and ``technical-documentation/file-layouts/``
directory listings. Files are Latin-1. ``dt`` is the fetch date.
"""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import Link, fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_pep"
DATASETS = "https://www2.census.gov/programs-surveys/popest/datasets/"
LAYOUTS = "https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/"

# series, vintage, dataset path, data file pattern, layout path, layout file pattern
SERIES = (
    ("intercensal_2000_2010", "2000-2010 intercensal", ("2000-2010", "intercensal", "county"),
     r"^co-est00int-tot\.csv$", ("2000-2010", "intercensal", "county"), r"^co-est00int-tot.*\.pdf$"),
    ("intercensal_2010_2020", "2010-2020 intercensal", ("2010-2020", "intercensal", "county", "asrh"),
     r"^cc-est2020int-agesex-48\.csv$", ("2010-2020", "intercensal", "county"), r"^cc-est2020int-agesex.*\.pdf$"),
    ("postcensal_v2020", "Vintage 2020", ("2010-2020", "counties", "totals"),
     r"^co-est2020-alldata\.csv$", ("2010-2020",), r"^co-est2020-alldata\.pdf$"),
)  # fmt: skip

_PERIOD = re.compile(r"/(\d{4})-(\d{4})/$")
_LATEST = re.compile(r"^co-est(\d{4})-alldata\.(csv|pdf)$", re.IGNORECASE)


def _walk(http: HttpClient, start: str, segments: tuple[str, ...]) -> tuple[str, list[Link]] | None:
    """Follow directory links ``start/seg1/seg2/...`` as listed; return the last directory and its links."""
    url, links = start, fetch_links(http, start)
    for segment in segments:
        nxt = next((link.url for link in links if link.url.startswith(url) and link.url.endswith(f"/{segment}/")), None)
        if nxt is None:
            return None
        url, links = nxt, fetch_links(http, nxt)
    return url, links


def _matching(found: tuple[str, list[Link]] | None, pattern: str) -> list[Link]:
    if found is None:
        return []
    directory, links = found
    return [link for link in links if link.url.startswith(directory) and re.match(pattern, link.filename, re.I)]


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    files: list[RemoteFile] = []

    def add(links: list[Link], series: str, vintage: str, kind: str) -> None:
        for link in links:
            files.append(RemoteFile(url=link.url, dt=today, source_page=link.url.rsplit("/", 1)[0] + "/",
                                    meta={"series": series, "vintage": vintage, "kind": kind}))

    periods = sorted(
        (int(m.group(2)), int(m.group(1)), link.url.rsplit("/", 2)[-2])
        for link in fetch_links(http, DATASETS)
        if (m := _PERIOD.search(link.url))
    )
    if periods:
        end, _, period = periods[-1]
        vintage = f"Vintage {end}"
        data = _matching(_walk(http, DATASETS, (period, "counties", "totals")), r"^co-est\d{4}-alldata\.csv$")
        add(data, "postcensal_latest", vintage, "data")
        add(_matching(_walk(http, LAYOUTS, (period,)), r"^co-est\d{4}-alldata\.pdf$"), "postcensal_latest", vintage,
            "layout")
    for series, vintage, data_path, data_pattern, layout_path, layout_pattern in SERIES:
        add(_matching(_walk(http, DATASETS, data_path), data_pattern), series, vintage, "data")
        add(_matching(_walk(http, LAYOUTS, layout_path), layout_pattern), series, vintage, "layout")
    return files


run = source_runner(SOURCE_ID, discover)
