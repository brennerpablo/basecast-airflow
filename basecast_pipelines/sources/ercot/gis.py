"""GIS Report (EMIL PG7-200-ER): monthly generator interconnection status workbooks.

Two discovery paths, both without login:
- the product page's document listing (Report Type 15933): GIS reports since 2019, plus the co-located
  battery identification reports ERCOT files under the same report type;
- the Resource Adequacy year pages, which still link older monthly GIS workbooks (2014-2018).
``dt`` is the first day of the report month.
"""

from __future__ import annotations

import re
from datetime import date

from basecast_pipelines.common.discovery import file_links, parse_month_year
from basecast_pipelines.common.ercot_mis import discover_product, list_documents
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import LOCAL_TZ
from basecast_pipelines.sources.ercot._web import RESOURCE_ADEQUACY_PAGE, crawl_with_year_pages

SOURCE_ID = "ercot_gis"
EMIL_ID = "pg7-200-er"

_GIS_TEXT = re.compile(r"\bGIS\b", re.IGNORECASE)


def _family(name: str) -> str:
    lowered = name.lower()
    if lowered.startswith("gis_report"):
        return "gis_report"
    if "battery" in lowered:
        return "co_located_battery"
    return "other"


def discover(
    http: HttpClient, *, include_colocated_battery: bool = True, include_year_pages: bool = True
) -> list[RemoteFile]:
    product = discover_product(http, EMIL_ID)
    files = []
    for doc in list_documents(http, product):
        family = _family(doc.friendly_name)
        if family == "co_located_battery" and not include_colocated_battery:
            continue
        report_month = parse_month_year(doc.friendly_name)
        files.append(
            RemoteFile(
                url=product.doc_download_url(doc),
                dt=report_month or doc.publish_date.astimezone(LOCAL_TZ).date(),
                filename=doc.filename,
                source_page=product.page_url,
                doc_id=doc.doc_id,
                report_type_id=doc.report_type_id,
                meta={
                    "family": family,
                    "friendly_name": doc.friendly_name,
                    "publish_date": doc.publish_date.isoformat(),
                    "dt_basis": "report_month" if report_month else "publish_date",
                },
            )
        )
    if include_year_pages:
        for page, links in crawl_with_year_pages(http, RESOURCE_ADEQUACY_PAGE):
            for link in file_links(links, (".xls", ".xlsx", ".zip")):
                if not _GIS_TEXT.search(link.text):
                    continue
                report_month: date | None = parse_month_year(link.text) or parse_month_year(link.filename)
                if report_month is None:
                    continue
                files.append(
                    RemoteFile(
                        url=link.url,
                        dt=report_month,
                        source_page=page,
                        meta={"family": "gis_report", "link_text": link.text, "dt_basis": "report_month"},
                    )
                )
    return files


run = source_runner(SOURCE_ID, discover)
