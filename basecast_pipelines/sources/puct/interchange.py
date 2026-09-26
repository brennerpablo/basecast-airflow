"""PUCT Interchange filings for the large-load dockets: the filings index (Excel export) of each control
number, and every document of selected items. No login. Defaults:

- 58777 (ERCOT reliability assessment / 2026 LTLF): item 38, ERCOT's preliminary 2026-2032 LTLF filing
  whose Attachment A has large-load requests by TSP and by load type; item 41, staff memo with final
  assumptions.
- 59772 (2026 LTLF adjustment): items 6 and 10 (TPPA and Texas Electric Cooperatives comments), item 18
  (order granting the adjustment).
- 58481 (large load interconnection standards rulemaking, PURA 37.0561): index only.

Links are read from the filings list and document pages. ``dt`` is the fetch date.
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
ITEMS: dict[int, tuple[int, ...]] = {58777: (38, 41), 59772: (6, 10, 18), 58481: ()}

_ITEM = re.compile(r"/search/documents/\?controlNumber=(\d+)&itemNumber=(\d+)$", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    files = []
    for control, items in ITEMS.items():
        page = FILINGS_PAGE.format(control=control)
        links = fetch_links(http, page)
        export = next((link for link in links if "/search/exportfilings/" in link.url), None)
        if export is not None:
            files.append(RemoteFile(url=export.url, dt=today, filename=f"{control}_filings.xlsx", source_page=page,
                                    meta={"control_number": control, "kind": "filings_index"}))
        for link in links:
            match = _ITEM.search(link.url)
            if not match or int(match.group(2)) not in items:
                continue
            for doc in fetch_links(http, link.url):
                if "/Documents/" in doc.url:
                    files.append(RemoteFile(url=doc.url, dt=today, source_page=link.url,
                                            meta={"control_number": control, "item": int(match.group(2)),
                                                  "kind": "document", "link_text": doc.text}))
    return files


run = source_runner(SOURCE_ID, discover)
