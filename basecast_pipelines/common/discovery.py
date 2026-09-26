"""Link discovery. File URLs are always taken from the pages the runbook points to, never constructed."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlsplit

from basecast_pipelines.common.http import HttpClient


@dataclass(frozen=True)
class Link:
    url: str
    text: str
    section: str = ""

    @property
    def filename(self) -> str:
        return unquote(urlsplit(self.url).path.rsplit("/", 1)[-1])


_HEADINGS = {"h1": 1, "h2": 2, "h3": 3, "h4": 4}


def _collapse_repeat(text: str) -> str:
    """ERCOT headings often render their text twice ("X X"); keep one copy."""
    words = text.split()
    half = len(words) // 2
    if len(words) % 2 == 0 and words and words[:half] == words[half:]:
        words = words[:half]
    return " ".join(words)


class _LinkParser(HTMLParser):
    """Collects links with their text and the heading path (h2 > h3 > h4) they sit under."""

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[Link] = []
        self._href: str | None = None
        self._text: list[str] = []
        self._heading_level: int | None = None
        self._heading_text: list[str] = []
        self._headings: dict[int, str] = {}

    @property
    def section(self) -> str:
        levels = [lvl for lvl in sorted(self._headings) if lvl >= 2] or sorted(self._headings)
        return " > ".join(self._headings[lvl] for lvl in levels)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "base":
            href = dict(attrs).get("href")
            if href:
                self.base_url = urljoin(self.base_url, href)
        if tag in _HEADINGS:
            self._heading_level = _HEADINGS[tag]
            self._heading_text = []
        if tag != "a":
            return
        self._flush()
        href = dict(attrs).get("href")
        if href and not href.startswith(("#", "javascript:", "mailto:")):
            self._href = urljoin(self.base_url, href.strip())
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)
        if self._heading_level is not None:
            self._heading_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._flush()
        if tag in _HEADINGS and self._heading_level == _HEADINGS[tag]:
            text = _collapse_repeat(" ".join("".join(self._heading_text).split()))
            if text:
                level = self._heading_level
                self._headings = {k: v for k, v in self._headings.items() if k < level}
                self._headings[level] = text
            self._heading_level = None

    def _flush(self) -> None:
        if self._href is not None:
            text = " ".join("".join(self._text).split())
            self.links.append(Link(self._href, text, self.section))
        self._href = None
        self._text = []


def extract_links(html: str, base_url: str) -> list[Link]:
    """All ``<a href>`` links (absolute URLs) with their visible text, in page order, deduplicated by URL."""
    parser = _LinkParser(base_url)
    parser.feed(html)
    parser.close()
    parser._flush()
    seen: set[str] = set()
    unique = []
    for link in parser.links:
        if link.url not in seen:
            seen.add(link.url)
            unique.append(link)
    return unique


def fetch_links(http: HttpClient, page_url: str) -> list[Link]:
    response = http.get(page_url)
    return extract_links(response.text, str(response.url))


def file_links(links: list[Link], extensions: tuple[str, ...]) -> list[Link]:
    exts = tuple(e.lower() for e in extensions)
    return [link for link in links if link.filename.lower().endswith(exts)]


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}  # fmt: skip
_MONTH_YEAR = re.compile(
    r"(?<![a-z])(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\s_.-]*((?:19|20)\d{2})(?!\d)",
    re.IGNORECASE,
)


def parse_month_year(text: str) -> date | None:
    """First "<Month> <Year>" found in a name (``GIS_Report_August2026``, ``MORA_June_2026``) as a date on day 1."""
    match = _MONTH_YEAR.search(text)
    if not match:
        return None
    return date(int(match.group(2)), _MONTHS[match.group(1).lower()], 1)
