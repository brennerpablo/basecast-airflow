"""Generic raw ingestion: download discovered files into ``raw/source=<id>/dt=<date>/`` with manifests.

Idempotency, in order of cost:
1. the document id (ERCOT listings) was already fetched → skip, no request;
2. the URL is marked immutable and was already fetched → skip, no request;
3. conditional GET with the last ETag / Last-Modified → a 304 means unchanged;
4. the downloaded bytes have a sha256 we already hold → discard.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit

from basecast_pipelines.common.etl_run import EtlRun
from basecast_pipelines.common.http import HttpClient, HttpError
from basecast_pipelines.common.manifest import Manifest, ManifestEntry, ManifestIndex, dt_prefix
from basecast_pipelines.common.storage import Storage
from basecast_pipelines.common.validate import check_download

log = logging.getLogger(__name__)


@dataclass
class RemoteFile:
    url: str
    dt: date
    filename: str | None = None
    source_page: str | None = None
    doc_id: str | None = None
    report_type_id: str | None = None
    immutable: bool = False
    params: dict | None = None
    meta: dict = field(default_factory=dict)


Discover = Callable[..., list[RemoteFile]]

_CD_FILENAME = re.compile(r"""filename\*?=(?:UTF-8'')?["']?([^"';]+)""", re.IGNORECASE)


def _safe_name(name: str) -> str:
    name = unquote(name).strip().replace("\\", "_").replace("/", "_").replace("\x00", "")
    return name.lstrip(".") or "download"


def resolve_filename(rf: RemoteFile, content_disposition: str | None) -> str:
    if rf.filename:
        return _safe_name(rf.filename)
    if content_disposition:
        match = _CD_FILENAME.search(content_disposition)
        if match:
            return _safe_name(match.group(1))
    return _safe_name(PurePosixPath(urlsplit(rf.url).path).name)


def _unique_key(storage: Storage, prefix: str, filename: str, sha256: str) -> str:
    key = prefix + filename
    if not storage.exists(key):
        return key
    stem, dot, ext = filename.rpartition(".")
    stem, ext = (stem, f".{ext}") if dot else (filename, "")
    return f"{prefix}{stem}__{sha256[:8]}{ext}"


def fetch_raw(
    files: list[RemoteFile], *, source_id: str, storage: Storage, http: HttpClient, run: EtlRun
) -> None:
    index = ManifestIndex.load(storage, source_id)
    manifests: dict[date, Manifest] = {}
    for i, rf in enumerate(files, 1):
        label = f"[{source_id} {i}/{len(files)}]"
        if rf.doc_id and index.has_doc_id(rf.doc_id):
            run.files_skipped += 1
            continue
        previous = index.latest_for_url(rf.url)
        if previous is not None and rf.immutable:
            run.files_skipped += 1
            continue
        headers = {}
        if previous is not None:
            if previous.etag:
                headers["If-None-Match"] = previous.etag
            if previous.last_modified:
                headers["If-Modified-Since"] = previous.last_modified
        tmp = storage.temp_path()
        try:
            download = http.download(rf.url, tmp, params=rf.params, headers=headers or None)
        except HttpError as exc:
            # Server trouble or the failure breaker: stop the run. A 4xx on one link: record it and go on.
            if exc.status is None or exc.status >= 500 or exc.status == 429:
                tmp.unlink(missing_ok=True)
                raise
            tmp.unlink(missing_ok=True)
            run.event("error", url=rf.url, error=str(exc))
            log.warning("%s failed: %s", label, exc)
            continue
        if download.path is None:
            run.files_skipped += 1
            run.event("not_modified", url=rf.url)
            continue
        assert download.sha256 is not None
        if index.has_sha256(download.sha256):
            tmp.unlink(missing_ok=True)
            run.files_skipped += 1
            run.event("unchanged", url=rf.url, sha256=download.sha256)
            continue
        prefix = dt_prefix(source_id, rf.dt)
        filename = resolve_filename(rf, download.headers.get("Content-Disposition"))
        problem = check_download(tmp, filename)
        if problem:
            tmp.unlink(missing_ok=True)
            run.event("error", url=rf.url, error=problem)
            log.warning("%s rejected: %s", label, problem)
            continue
        key = _unique_key(storage, prefix, filename, download.sha256)
        storage.put_file(key, tmp)
        entry = ManifestEntry(
            file=key.removeprefix(prefix),
            url=rf.url,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            sha256=download.sha256,
            bytes=download.bytes,
            http_status=download.status,
            source_page=rf.source_page,
            doc_id=rf.doc_id,
            report_type_id=rf.report_type_id,
            content_type=download.headers.get("Content-Type"),
            etag=download.headers.get("ETag"),
            last_modified=download.headers.get("Last-Modified"),
            meta={**rf.meta, **({"params": rf.params} if rf.params else {})},
        )
        if rf.dt not in manifests:
            manifests[rf.dt] = Manifest.load(storage, source_id, rf.dt)
        manifests[rf.dt].add(entry, storage)
        index.add(entry)
        run.files += 1
        run.bytes += download.bytes
        log.info("%s %s (%.1f MB)", label, key, download.bytes / 1e6)


def run_raw(
    source_id: str,
    discover: Discover,
    *,
    storage: Storage,
    http: HttpClient,
    since: date | None = None,
    until: date | None = None,
    dry_run: bool = False,
    **options: object,
) -> list[RemoteFile]:
    """Discover, filter by ``dt`` in [since, until], then download (unless ``dry_run``) inside an etl_run."""

    def select(files: list[RemoteFile]) -> list[RemoteFile]:
        return [
            f for f in files if (since is None or f.dt >= since) and (until is None or f.dt <= until)
        ]

    if dry_run:
        return select(discover(http, **options))
    params = {"since": since, "until": until, **options}
    with EtlRun(source_id, storage, params=params) as run:
        requests_before = http.stats.requests
        files = select(discover(http, **options))
        run.event("discovered", files=len(files))
        fetch_raw(files, source_id=source_id, storage=storage, http=http, run=run)
        run.event("http", requests=http.stats.requests - requests_before, retries=http.stats.retries)
    return files


def source_runner(source_id: str, discover: Discover) -> Callable[..., list[RemoteFile]]:
    """Build a source module's ``run(*, storage, http, since=None, until=None)`` entry point."""

    def run(
        *,
        storage: Storage,
        http: HttpClient,
        since: date | None = None,
        until: date | None = None,
        dry_run: bool = False,
        **options: object,
    ) -> list[RemoteFile]:
        return run_raw(
            source_id,
            discover,
            storage=storage,
            http=http,
            since=since,
            until=until,
            dry_run=dry_run,
            **options,
        )

    run.__doc__ = f"Download the raw files of ``{source_id}``."
    return run
