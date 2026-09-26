"""Download manifests: one ``_manifest.json`` per ``raw/source=<id>/dt=<date>/`` folder, listing every file
written there with its provenance (URL, document id, fetch time, sha256, size, HTTP status)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date

from basecast_pipelines.common.storage import Storage

MANIFEST_NAME = "_manifest.json"


def raw_prefix(source_id: str) -> str:
    return f"raw/source={source_id}/"


def dt_prefix(source_id: str, dt: date) -> str:
    return f"{raw_prefix(source_id)}dt={dt.isoformat()}/"


@dataclass
class ManifestEntry:
    file: str
    url: str
    fetched_at: str
    sha256: str
    bytes: int
    http_status: int
    source_page: str | None = None
    doc_id: str | None = None
    report_type_id: str | None = None
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    meta: dict = field(default_factory=dict)


@dataclass
class Manifest:
    source: str
    dt: date
    entries: list[ManifestEntry] = field(default_factory=list)

    @property
    def key(self) -> str:
        return dt_prefix(self.source, self.dt) + MANIFEST_NAME

    @classmethod
    def load(cls, storage: Storage, source: str, dt: date) -> Manifest:
        manifest = cls(source=source, dt=dt)
        if storage.exists(manifest.key):
            payload = json.loads(storage.read_bytes(manifest.key))
            manifest.entries = [ManifestEntry(**e) for e in payload["entries"]]
        return manifest

    def add(self, entry: ManifestEntry, storage: Storage) -> None:
        """Append an entry and persist right away, so an interrupted run never loses provenance."""
        self.entries.append(entry)
        payload = {
            "source": self.source,
            "dt": self.dt.isoformat(),
            "entries": [asdict(e) for e in self.entries],
        }
        storage.write_bytes(self.key, json.dumps(payload, indent=2).encode(), overwrite=True)


class ManifestIndex:
    """Everything already fetched for a source, across all ``dt=`` folders (for idempotency checks)."""

    def __init__(self, entries: list[ManifestEntry]) -> None:
        self.entries: list[ManifestEntry] = []
        self._doc_ids: set[str] = set()
        self._sha256: set[str] = set()
        self._by_url: dict[str, ManifestEntry] = {}
        for entry in entries:
            self.add(entry)

    @classmethod
    def load(cls, storage: Storage, source: str) -> ManifestIndex:
        entries: list[ManifestEntry] = []
        for key in storage.list(raw_prefix(source)):
            if key.endswith("/" + MANIFEST_NAME):
                payload = json.loads(storage.read_bytes(key))
                entries.extend(ManifestEntry(**e) for e in payload["entries"])
        return cls(entries)

    def add(self, entry: ManifestEntry) -> None:
        self.entries.append(entry)
        if entry.doc_id:
            self._doc_ids.add(entry.doc_id)
        self._sha256.add(entry.sha256)
        previous = self._by_url.get(entry.url)
        if previous is None or entry.fetched_at >= previous.fetched_at:
            self._by_url[entry.url] = entry

    def has_doc_id(self, doc_id: str) -> bool:
        return doc_id in self._doc_ids

    def has_sha256(self, sha256: str) -> bool:
        return sha256 in self._sha256

    def latest_for_url(self, url: str) -> ManifestEntry | None:
        return self._by_url.get(url)
