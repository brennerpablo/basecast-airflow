"""Large-load interconnection material from ERCOT committee meeting pages and the Helpful Resources page,
2024 onward. There is no data product: only decks, and ERCOT moved and renamed them three times.

- ``status_deck``: "Large Load Interconnection Status Update" by the Large Load Integration Team. Named
  "LLI Queue Status Update" on LFLTF pages (2024) and inside TAC agenda-item zips (Oct 2024 - Aug 2025),
  "<Month> TAC Report" (TAC zips Oct 2025 - Jan 2026, LLWG pages Feb - Mar 2026), then
  "<Month> <day> LLWG Report" on LLWG pages (Apr 2026 onward).
- ``board_update``: Board / R&M "Interconnection and Grid Analysis Update" and its predecessor
  "System Planning and Weatherization Update".
- ``ercot_monthly``: "ERCOT Monthly <Month> <Year>" recaps (headline MW figures).
- ``other_large_load``: other large-load / Batch Zero decks on Board and R&M pages, TAC zips and the
  Helpful Resources page (legislative and PUCT presentations).

Meeting pages are static HTML (``/calendar/MMDDYYYY-<slug>``); every meeting page also links its
neighbours, which finds meetings missing from the year lists (LFLTF 2025). TAC zips are downloaded only
when a member name matches, read from the zip's central directory with one byte-range request. The name
patterns are candidate filters: confirm deck content at parse time. ``dt`` is the meeting date (ERCOT
Monthly: first day of the issue month; key documents: posting date from the URL path).
"""

from __future__ import annotations

import re
import struct
from datetime import date
from urllib.parse import unquote, urlsplit

from basecast_pipelines.common.discovery import Link, fetch_links, parse_month_year
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ercot_large_load_decks"

# Committee root pages and the token their meeting URLs carry.
COMMITTEES: dict[str, tuple[str, str]] = {
    "TAC": ("https://www.ercot.com/committees/tac", "TAC"),
    "LLWG": ("https://www.ercot.com/committees/tac/llwg", "LLWG"),
    "LFLTF": ("https://www.ercot.com/committees/inactive/lfltf", "LFLTF"),
    "Board": ("https://www.ercot.com/committees/board", "Board-of-Directors"),
    "R&M": ("https://www.ercot.com/committees/inactive/reliabilitymarkets", "Reliability-and-Markets-Committee"),
}
PRESENTATIONS_PAGE = "https://www.ercot.com/news/presentations"
DOCS_PREFIX = "https://www.ercot.com/files/docs/"

_MEETING = re.compile(r"^https://www\.ercot\.com/calendar/(\d{2})(\d{2})(\d{4})-(.+)$")
_POSTED = re.compile(r"/files/docs/(\d{4})/(\d{2})/(\d{2})/")
_MONTH = r"(?:january|february|march|april|may|june|july|august|september|october|november|december)"
STATUS_DECK = re.compile(
    rf"\blli queue status update\b|^{_MONTH} tac report\b|^{_MONTH} \d{{1,2}} llwg report\b"
    r"|\blarge load interconnection status update\b",
    re.IGNORECASE,
)
NOT_STATUS = re.compile(r"\bllwg .* tac update\b|^tac llwg\b|\bleadership\b", re.IGNORECASE)
TAC_ZIP_CANDIDATE = re.compile(r"ercot reports?|llwg report|lfltf report|large load issues", re.IGNORECASE)
BOARD_UPDATE = re.compile(
    r"interconnection and grid analysis update|system planning and weatherization update", re.IGNORECASE
)
OTHER_LARGE_LOAD = re.compile(r"large load|batch zero|batch study|data centers?\b", re.IGNORECASE)
ERCOT_MONTHLY = re.compile(rf"^ERCOT Monthly {_MONTH} \d{{4}}$", re.IGNORECASE)


def normalize(name: str) -> str:
    """Decode, drop the extension, turn ``_`` and ``-`` into spaces: "LLI%20Queue_Status-Update.pdf" style."""
    name = unquote(name)
    name = re.sub(r"\.(pdf|pptx?|docx?|xlsx?|zip)$", "", name, flags=re.IGNORECASE)
    return " ".join(re.sub(r"[_\-]+", " ", name).split())


def meeting_date(url: str) -> date | None:
    match = _MEETING.match(url)
    if not match:
        return None
    month, day, year = (int(g) for g in match.groups()[:3])
    try:
        return date(year, month, day)
    except ValueError:
        return None


def posted_date(url: str) -> date | None:
    match = _POSTED.search(url)
    return date(*(int(g) for g in match.groups())) if match else None


def zip_member_names(http: HttpClient, url: str, tail_bytes: int = 65536) -> list[str] | None:
    """Member names from the zip's central directory, reading only the end of the file (HTTP Range)."""
    tail = http.get(url, headers={"Range": f"bytes=-{tail_bytes}"}).content
    eocd = tail.rfind(b"PK\x05\x06")
    if eocd < 0 or eocd + 22 > len(tail):
        return None
    cd_size = struct.unpack("<I", tail[eocd + 12 : eocd + 16])[0]
    if cd_size + 22 > len(tail) and tail_bytes < cd_size + 22 + 65536:
        return zip_member_names(http, url, tail_bytes=cd_size + 22 + 65536)
    names, i = [], tail.find(b"PK\x01\x02", max(0, eocd - cd_size))
    while 0 <= i < eocd:
        flags = struct.unpack("<H", tail[i + 8 : i + 10])[0]
        n, x, c = struct.unpack("<HHH", tail[i + 28 : i + 34])
        raw = tail[i + 46 : i + 46 + n]
        names.append(raw.decode("utf-8" if flags & 0x800 else "cp437", errors="replace"))
        i = tail.find(b"PK\x01\x02", i + 46 + n + x + c)
    return names


def _year_pages(root: str, links: list[Link], since_year: int) -> list[str]:
    root_path = urlsplit(root).path.rstrip("/")
    urls = []
    for link in links:
        match = re.fullmatch(re.escape(root_path) + r"/(\d{4})/?", urlsplit(link.url).path)
        if match and int(match.group(1)) >= since_year and link.url not in urls:
            urls.append(link.url)
    return urls


def _meeting_links(links: list[Link], token: str, start: date, end: date) -> list[tuple[str, date]]:
    found = []
    for link in links:
        match = _MEETING.match(link.url)
        when = meeting_date(link.url)
        slug = match.group(4).lower() if match else ""
        if when and start <= when <= end and re.search(rf"(?:^|-){re.escape(token.lower())}(?:-|$)", slug):
            found.append((link.url, when))
    return found


def discover(http: HttpClient, *, since_year: int = 2024) -> list[RemoteFile]:
    today = local_today()
    start = date(int(since_year), 1, 1)
    found: dict[str, RemoteFile] = {}

    def add(link: Link, kind: str, dt: date, page: str, group: str, **meta: object) -> None:
        if link.url in found:
            return
        found[link.url] = RemoteFile(
            url=link.url,
            dt=dt,
            source_page=page,
            meta={"type": kind, "group": group, "link_text": link.text,
                  "posted_date": str(posted_date(link.url)), **meta},
        )

    def collect(page: str, links: list[Link], group: str, when: date | None) -> None:
        for link in links:
            if not link.url.startswith(DOCS_PREFIX):
                continue
            dt = when or posted_date(link.url) or today
            name = normalize(link.text or link.filename)
            is_zip = link.filename.lower().endswith(".zip")
            if group == "TAC" and is_zip:
                if not TAC_ZIP_CANDIDATE.search(name):
                    continue
                members = zip_member_names(http, link.url)
                if members is None:
                    add(link, "status_deck_candidate", dt, page, group, zip_members=None)
                    continue
                status = [m for m in members if STATUS_DECK.search(normalize(m.rsplit("/", 1)[-1]))
                          and not NOT_STATUS.search(normalize(m.rsplit("/", 1)[-1]))]
                other = [m for m in members if OTHER_LARGE_LOAD.search(normalize(m.rsplit("/", 1)[-1]))]
                if status:
                    add(link, "status_deck", dt, page, group, zip_members=status, all_members=members)
                elif other:
                    add(link, "other_large_load", dt, page, group, zip_members=other, all_members=members)
            elif STATUS_DECK.search(name) and not NOT_STATUS.search(name):
                add(link, "status_deck", dt, page, group)
            elif BOARD_UPDATE.search(name):
                add(link, "board_update", dt, page, group)
            elif group in {"Board", "R&M"} and OTHER_LARGE_LOAD.search(name):
                add(link, "other_large_load", dt, page, group)

    for group, (root, token) in COMMITTEES.items():
        root_links = fetch_links(http, root)
        list_pages = [(root, root_links)] + [(u, fetch_links(http, u)) for u in _year_pages(root, root_links, start.year)]
        queue: dict[str, date] = {}
        for _, links in list_pages:
            queue.update(_meeting_links(links, token, start, today))
        visited: set[str] = set()
        while pending := sorted(set(queue) - visited):
            for url in pending:
                visited.add(url)
                links = fetch_links(http, url)
                queue.update(_meeting_links(links, token, start, today))
                collect(url, links, group, queue[url])
        for page, links in list_pages:
            collect(page, links, group, None)

    for link in fetch_links(http, PRESENTATIONS_PAGE):
        if not link.url.startswith(DOCS_PREFIX):
            continue
        if ERCOT_MONTHLY.match(link.text):
            add(link, "ercot_monthly", parse_month_year(link.text) or today, PRESENTATIONS_PAGE, "ERCOT Monthly")
        elif OTHER_LARGE_LOAD.search(link.text) and (posted_date(link.url) or today) >= start:
            add(link, "other_large_load", posted_date(link.url) or today, PRESENTATIONS_PAGE, "Presentations")
    return list(found.values())


run = source_runner(SOURCE_ID, discover)
