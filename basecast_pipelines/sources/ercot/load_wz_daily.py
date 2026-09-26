"""Actual System Load by Weather Zone (EMIL NP6-345-CD): daily files that bridge the gap between the last
monthly archive update and yesterday. The listing only keeps about a month, so this must run regularly.
``dt`` is the publish date (America/Chicago); each file holds the previous operating day."""

from __future__ import annotations

from basecast_pipelines.common.ercot_mis import discover_product, list_documents
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import LOCAL_TZ

SOURCE_ID = "ercot_load_wz_daily"
EMIL_ID = "NP6-345-CD"


def discover(http: HttpClient, *, include_xml: bool = False) -> list[RemoteFile]:
    product = discover_product(http, EMIL_ID)
    files = []
    for doc in list_documents(http, product):
        if not include_xml and doc.friendly_name.lower().endswith("_xml"):
            continue
        files.append(
            RemoteFile(
                url=product.doc_download_url(doc),
                dt=doc.publish_date.astimezone(LOCAL_TZ).date(),
                filename=doc.filename,
                source_page=product.page_url,
                doc_id=doc.doc_id,
                report_type_id=doc.report_type_id,
                meta={
                    "friendly_name": doc.friendly_name,
                    "publish_date": doc.publish_date.isoformat(),
                    "dt_basis": "publish_date",
                },
            )
        )
    return files


run = source_runner(SOURCE_ID, discover)
