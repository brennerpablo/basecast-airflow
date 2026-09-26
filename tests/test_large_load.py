from __future__ import annotations

import io
import zipfile
from datetime import date

import httpx

from basecast_pipelines.sources.ercot.large_load import (
    BOARD_UPDATE,
    NOT_STATUS,
    STATUS_DECK,
    meeting_date,
    normalize,
    zip_member_names,
)


def _is_status(name: str) -> bool:
    n = normalize(name)
    return bool(STATUS_DECK.search(n)) and not NOT_STATUS.search(n)


def test_status_deck_names_across_eras() -> None:
    # Real names seen on ERCOT pages and inside TAC zips, 2024-2026.
    assert _is_status("LLI%20Queue%20Status%20Update%20-%202024_5_2.pdf")
    assert _is_status("LLI Queue Status Update - 2024-10-30.pptx")
    assert _is_status("LLI%20Queue%20Update%20-%202022-10-24.pdf")
    assert _is_status("October TAC Report.pptx")
    assert _is_status("March TAC Report Updated_03262026")
    assert _is_status("April 24 LLWG Report_042426")
    assert not _is_status("LLWG Mar 2025 TAC Update - Revised.pptx")
    assert not _is_status("TAC LLWG July 2026.pptx")
    assert not _is_status("Batch_Zero_Update_TAC_08262026.pptx")
    assert BOARD_UPDATE.search(normalize("9 Interconnection and Grid Analysis Update"))


def test_meeting_date_from_calendar_url() -> None:
    assert meeting_date("https://www.ercot.com/calendar/03132026-LLWG-Meeting-_-Webex") == date(2026, 3, 13)
    assert meeting_date("https://www.ercot.com/files/docs/2026/03/12/March-TAC-Report.pdf") is None


def test_zip_members_from_tail_only(make_http) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("LLI Queue Status Update - 2025-1.pdf", b"x" * 200_000)
        zf.writestr("Other deck.pptx", b"y" * 1000)
    body = buffer.getvalue()
    ranges = []

    def handler(request: httpx.Request) -> httpx.Response:
        ranges.append(request.headers.get("Range"))
        n = int(request.headers["Range"].removeprefix("bytes=-"))
        return httpx.Response(206, content=body[-n:])

    names = zip_member_names(make_http(handler), "https://www.ercot.com/files/docs/2025/01/22/18-ercot-reports.zip")
    assert names == ["LLI Queue Status Update - 2025-1.pdf", "Other deck.pptx"]
    assert ranges == ["bytes=-65536"]
