from __future__ import annotations

import json
from datetime import date

import httpx
import pytest

from basecast_pipelines.common.etl_run import read_runs
from basecast_pipelines.common.raw import RemoteFile, resolve_filename, run_raw

SOURCE = "test_source"
DT = date(2026, 9, 25)


def _files() -> list[RemoteFile]:
    return [
        RemoteFile(url="https://www.ercot.com/files/docs/a.xlsx", dt=DT, source_page="https://www.ercot.com/p"),
        RemoteFile(url="https://www.ercot.com/misdownload?doclookupId=7", dt=date(2026, 8, 1), doc_id="7",
                   filename="RPT.GIS_Report_August2026.xlsx"),
    ]


def _run(storage, http, files=None):
    return run_raw(SOURCE, lambda _http: files or _files(), storage=storage, http=http)


class Server:
    def __init__(self) -> None:
        self.bodies = {"/files/docs/a.xlsx": b"version-1", "/misdownload": b"gis"}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, content=self.bodies[request.url.path], headers={"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"})


def test_first_run_writes_files_and_manifest(storage, make_http) -> None:
    _run(storage, make_http(Server()))
    keys = storage.list(f"raw/source={SOURCE}/")
    assert f"raw/source={SOURCE}/dt=2026-09-25/a.xlsx" in keys
    assert f"raw/source={SOURCE}/dt=2026-08-01/RPT.GIS_Report_August2026.xlsx" in keys
    manifest = json.loads(storage.read_bytes(f"raw/source={SOURCE}/dt=2026-09-25/_manifest.json"))
    entry = manifest["entries"][0]
    assert entry["url"] == "https://www.ercot.com/files/docs/a.xlsx"
    assert entry["bytes"] == 9 and entry["http_status"] == 200 and len(entry["sha256"]) == 64
    runs = read_runs(storage)
    assert runs["status"].to_list() == ["success"] and runs["files"].to_list() == [2]


def test_rerun_is_idempotent(storage, make_http) -> None:
    server = Server()
    _run(storage, make_http(server))
    first_keys = storage.list("raw/")
    server.requests.clear()
    _run(storage, make_http(server))
    assert storage.list("raw/") == first_keys
    # the doc id is skipped without a request; the web file is re-checked conditionally
    assert [r.url.path for r in server.requests] == ["/files/docs/a.xlsx"]
    assert server.requests[0].headers["If-Modified-Since"] == "Mon, 01 Jan 2024 00:00:00 GMT"
    assert read_runs(storage)["files_skipped"].to_list() == [0, 2]


def test_changed_content_lands_in_new_snapshot(storage, make_http) -> None:
    server = Server()
    _run(storage, make_http(server))
    server.bodies["/files/docs/a.xlsx"] = b"version-2"
    later = [RemoteFile(url="https://www.ercot.com/files/docs/a.xlsx", dt=date(2026, 10, 25))]
    _run(storage, make_http(server), files=later)
    assert storage.read_bytes(f"raw/source={SOURCE}/dt=2026-09-25/a.xlsx") == b"version-1"
    assert storage.read_bytes(f"raw/source={SOURCE}/dt=2026-10-25/a.xlsx") == b"version-2"


def test_same_day_change_never_overwrites(storage, make_http) -> None:
    server = Server()
    _run(storage, make_http(server))
    server.bodies["/files/docs/a.xlsx"] = b"version-2"
    _run(storage, make_http(server))
    keys = [k for k in storage.list(f"raw/source={SOURCE}/dt=2026-09-25/") if "a" in k.rsplit("/", 1)[-1]]
    assert len([k for k in keys if k.endswith(".xlsx")]) == 2
    assert storage.read_bytes(f"raw/source={SOURCE}/dt=2026-09-25/a.xlsx") == b"version-1"


def test_404_is_recorded_and_run_is_partial(storage, make_http) -> None:
    def handler(request):
        if request.url.path == "/files/docs/a.xlsx":
            return httpx.Response(404)
        return httpx.Response(200, content=b"gis")

    _run(storage, make_http(handler))
    runs = read_runs(storage)
    assert runs["status"].to_list() == ["partial"]
    events = json.loads(runs["events"][0])
    assert any(e["kind"] == "error" and "404" in e["error"] for e in events)


def test_server_failure_fails_the_run(storage, make_http) -> None:
    with pytest.raises(Exception):
        _run(storage, make_http(lambda request: httpx.Response(503), max_consecutive_failures=3))
    assert read_runs(storage)["status"].to_list() == ["failed"]
    assert storage.list("raw/") == []


def test_filename_resolution() -> None:
    rf = RemoteFile(url="https://x.org/files/docs/2024/Native%20Load.zip", dt=DT)
    assert resolve_filename(rf, None) == "Native Load.zip"
    assert resolve_filename(rf, 'attachment; filename="RPT.1.zip"') == "RPT.1.zip"
    rf.filename = "explicit.xlsx"
    assert resolve_filename(rf, 'attachment; filename="RPT.1.zip"') == "explicit.xlsx"


def test_error_page_with_200_is_rejected(storage, make_http) -> None:
    def handler(request):
        if request.url.path == "/files/docs/a.xlsx":
            return httpx.Response(200, text="<!DOCTYPE html><html>Service unavailable</html>")
        return httpx.Response(200, content=b"gis")

    _run(storage, make_http(handler))
    assert not storage.exists(f"raw/source={SOURCE}/dt=2026-09-25/a.xlsx")
    assert read_runs(storage)["status"].to_list() == ["partial"]


def test_arcgis_error_json_is_rejected(storage, make_http) -> None:
    files = [RemoteFile(url="https://services.arcgis.com/q", dt=DT, filename="features_00000.geojson")]
    http = make_http(lambda request: httpx.Response(200, json={"error": {"code": 400, "message": "bad"}}))
    _run(storage, http, files=files)
    assert storage.list("raw/") == []


def test_register_local_file(storage, tmp_path) -> None:
    from basecast_pipelines.common.raw import register_local_file

    src = tmp_path / "queued_up_2025.xlsx"
    src.write_bytes(b"PK\x03\x04 fake workbook")
    key = register_local_file(storage, "lbnl_queued_up", src, dt=DT, origin_url="https://emp.lbl.gov/queues")
    assert key == "raw/source=lbnl_queued_up/dt=2026-09-25/queued_up_2025.xlsx" and src.exists()
    entry = json.loads(storage.read_bytes("raw/source=lbnl_queued_up/dt=2026-09-25/_manifest.json"))["entries"][0]
    assert entry["meta"]["origin"] == "manual" and entry["url"] == "https://emp.lbl.gov/queues" and entry["http_status"] == 0
    assert register_local_file(storage, "lbnl_queued_up", src, dt=date(2026, 9, 30)) is None
    assert read_runs(storage)["files_skipped"].to_list() == [0, 1]
