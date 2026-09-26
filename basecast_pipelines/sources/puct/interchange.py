"""PUCT Interchange filings for the large-load dockets: the filings index (Excel export) of each control
number and every document of every item (no login). Dockets:

- 58777: ERCOT reliability assessment / 2026 LTLF. Item 38 is ERCOT's preliminary 2026-2032 LTLF filing,
  whose Attachment A has large-load requests by TSP and by load type; item 41 is the staff memo with final
  assumptions.
- 59772: 2026 LTLF adjustment (Batch Zero basis). Items 6 and 10 are the TPPA and Texas Electric
  Cooperatives comments; item 18 is the order granting the adjustment.
- 58481: large load interconnection standards rulemaking (PURA 37.0561), with stakeholder comments.

Each docket's filings page lists all of its items on one page; item pages link the documents
(``/Documents/<control>_<item>_<docid>.<ext>``), which never change, so they are fetched once.
``--opt items=38,41`` restricts to some items. ``dt`` is the fetch date.
"""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "puct_filings"
FILINGS_PAGE = (
    "https://interchange.puc.texas.gov/search/filings/?UtilityType=A&ControlNumber={control}"
    "&ItemMatch=Equal&DocumentType=ALL&SortOrder=Descending"
)
DOCKETS = (58777, 59772, 58481)

_ITEM = re.compile(r"/search/documents/\?controlNumber=(\d+)&itemNumber=(\d+)$", re.IGNORECASE)


def discover(http: HttpClient, *, items: str | int | None = None) -> list[RemoteFile]:
    today = local_today()
    only = {int(i) for i in str(items).split(",")} if items not in (None, "", "all") else None
    files = []
    for control in DOCKETS:
        page = FILINGS_PAGE.format(control=control)
        links = fetch_links(http, page)
        export = next((link for link in links if "/search/exportfilings/" in link.url), None)
        if export is not None:
            files.append(RemoteFile(url=export.url, dt=today, filename=f"{control}_filings.xlsx", source_page=page,
                                    meta={"control_number": control, "kind": "filings_index"}))
        item_pages = {}
        for link in links:
            match = _ITEM.search(link.url)
            if match and int(match.group(1)) == control and (only is None or int(match.group(2)) in only):
                item_pages.setdefault(int(match.group(2)), link.url)
        for item, item_url in sorted(item_pages.items()):
            for doc in fetch_links(http, item_url):
                if "/Documents/" in doc.url:
                    files.append(RemoteFile(url=doc.url, dt=today, source_page=item_url, immutable=True,
                                            meta={"control_number": control, "item": item,
                                                  "kind": "document", "link_text": doc.text}))
    return files


run = source_runner(SOURCE_ID, discover)
