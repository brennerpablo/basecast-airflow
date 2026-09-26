"""Helpers for parser tests: put a trimmed fixture into a temporary lake as a raw snapshot."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from basecast_pipelines.common.manifest import Manifest, ManifestEntry, dt_prefix
from basecast_pipelines.common.storage import LocalStorage
from basecast_pipelines.processing.core import RawFile

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def raw_file(storage: LocalStorage):
    """``raw_file(source, "sub/fixture.xlsx", dt=date(...), meta={...})`` → a ``RawFile`` in the temp lake."""

    def make(source: str, fixture: str | Path, *, dt: date = date(2026, 9, 25), name: str | None = None,
             meta: dict | None = None, url: str | None = None) -> RawFile:
        path = FIXTURES / fixture
        data = path.read_bytes()
        prefix = dt_prefix(source, dt)
        filename = name or path.name
        storage.write_bytes(prefix + filename, data, overwrite=True)
        entry = ManifestEntry(
            file=filename,
            url=url or f"https://example.test/{filename}",
            fetched_at=datetime.now(timezone.utc).isoformat(),
            sha256=hashlib.sha256(data).hexdigest(),
            bytes=len(data),
            http_status=200,
            meta=meta or {},
        )
        Manifest.load(storage, source, dt).add(entry, storage)
        return RawFile(storage, source, dt, prefix + filename, entry)

    return make
