from __future__ import annotations

from datetime import date

import httpx

from basecast_pipelines.common.raw import RemoteFile, run_raw
from basecast_pipelines.common.validate import audit_raw, signature_mismatch


def test_signatures() -> None:
    assert signature_mismatch(b"PK\x03\x04rest", "a.xlsx") is None
    assert signature_mismatch(b"\xd0\xcf\x11\xe0rest", "a.xls") is None
    assert signature_mismatch(b"%PDF-1.7", "a.pdf") is None
    assert signature_mismatch(b"<html>", "a.zip") is not None
    assert signature_mismatch(b"anything", "a.txt") is None


def test_audit_detects_tampering(storage, make_http) -> None:
    files = [RemoteFile(url="https://www.ercot.com/f.pdf", dt=date(2026, 9, 25))]
    run_raw("s", lambda _http: files, storage=storage, http=make_http(lambda r: httpx.Response(200, content=b"%PDF-1.7 x")))
    assert audit_raw(storage) == (1, [])
    path = storage.root / "raw/source=s/dt=2026-09-25/f.pdf"
    path.write_bytes(b"%PDF-1.7 y")
    checked, problems = audit_raw(storage)
    assert checked == 1 and [p.problem for p in problems] == ["sha256 differs from manifest"]
