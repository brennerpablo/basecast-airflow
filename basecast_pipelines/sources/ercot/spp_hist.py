"""Historical settlement point prices for hubs and load zones: real-time (EMIL NP6-785-ER) and day-ahead
(EMIL NP4-180-ER) annual files, from the products' document listings. ``dt`` is the publish date."""

from __future__ import annotations

from basecast_pipelines.common.ercot_mis import discover_product, list_documents
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import LOCAL_TZ

SOURCE_ID = "ercot_spp_hist"
EMIL_IDS = {"rtm": "NP6-785-ER", "dam": "NP4-180-ER"}


def discover(http: HttpClient) -> list[RemoteFile]:
    files = []
    for market, emil_id in EMIL_IDS.items():
        product = discover_product(http, emil_id)
        for doc in list_documents(http, product):
            files.append(
                RemoteFile(
                    url=product.doc_download_url(doc),
                    dt=doc.publish_date.astimezone(LOCAL_TZ).date(),
                    filename=doc.filename,
                    source_page=product.page_url,
                    doc_id=doc.doc_id,
                    report_type_id=doc.report_type_id,
                    meta={
                        "market": market,
                        "friendly_name": doc.friendly_name,
                        "publish_date": doc.publish_date.isoformat(),
                        "listed_bytes": doc.content_size,
                        "dt_basis": "publish_date",
                    },
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
