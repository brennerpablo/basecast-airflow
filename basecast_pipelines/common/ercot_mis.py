"""ERCOT report listings without login.

Each data-product page (``/mp/data-products/data-product-details?id=<EMIL id>``) declares, in its inline
script, the report type id and the two services its browser code uses: ``reportListUrl`` (JSON document
list) and ``reportDownloadUrl`` (download by document id). We read all three from the page itself.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from basecast_pipelines.common.http import HttpClient

PRODUCT_PAGE = "https://www.ercot.com/mp/data-products/data-product-details?id={emil_id}"

_JS_VAR = r'var\s+{name}\s*=\s*"([^"]*)"'


class ErcotPageError(Exception):
    pass


@dataclass(frozen=True)
class ErcotDoc:
    doc_id: str
    report_type_id: str
    friendly_name: str
    constructed_name: str
    extension: str
    publish_date: datetime
    content_size: int

    @property
    def filename(self) -> str:
        return self.constructed_name or f"{self.friendly_name}.{self.extension}"


@dataclass(frozen=True)
class ErcotProduct:
    emil_id: str
    page_url: str
    report_type_id: str
    list_url: str
    download_url: str

    def doc_download_url(self, doc: ErcotDoc) -> str:
        return f"{self.download_url}{doc.doc_id}"


def parse_product_page(html: str, emil_id: str, page_url: str) -> ErcotProduct:
    values = {}
    for name in ("reportTypeID", "reportListUrl", "reportDownloadUrl"):
        match = re.search(_JS_VAR.format(name=name), html)
        if not match or not match.group(1):
            raise ErcotPageError(f"{name} not found on {page_url}; the page layout changed")
        values[name] = match.group(1)
    return ErcotProduct(
        emil_id=emil_id,
        page_url=page_url,
        report_type_id=values["reportTypeID"],
        list_url=values["reportListUrl"],
        download_url=values["reportDownloadUrl"],
    )


def discover_product(http: HttpClient, emil_id: str) -> ErcotProduct:
    page_url = PRODUCT_PAGE.format(emil_id=emil_id)
    return parse_product_page(http.get(page_url).text, emil_id, page_url)


def parse_doc_list(payload: dict) -> list[ErcotDoc]:
    docs = []
    for item in payload["ListDocsByRptTypeRes"].get("DocumentList") or []:
        d = item["Document"]
        docs.append(
            ErcotDoc(
                doc_id=d["DocID"],
                report_type_id=d["ReportTypeID"],
                friendly_name=d["FriendlyName"],
                constructed_name=d.get("ConstructedName", ""),
                extension=d.get("Extension", ""),
                publish_date=datetime.fromisoformat(d["PublishDate"]),
                content_size=int(d.get("ContentSize") or 0),
            )
        )
    return docs


def list_documents(http: HttpClient, product: ErcotProduct) -> list[ErcotDoc]:
    return parse_doc_list(http.get(f"{product.list_url}{product.report_type_id}").json())
