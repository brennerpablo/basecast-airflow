"""The processing contract: a parser module declares ``DATASETS``, each turning raw files into one typed table.

A ``Dataset`` either parses one raw file at a time (``parse``) or builds the table from a list of files
(``build``). The runner adds ``source_file`` (the raw key) and ``ingested_at`` (UTC) and writes the table
to its target:

- ``postgres``: the ``basecast`` database, read by the API;
- ``bigquery``: typed Parquet in the lake (``parquet/<dataset>/dt=<date>/``) plus a BigQuery copy, for the
  large datasets only.

Write modes (idempotency):

- ``replace``: the table is rebuilt from every selected file (reference data, "latest snapshot wins");
- ``by_file``: rows belong to one raw file; reprocessing a file replaces its rows (report vintages);
- ``by_key``: new rows replace the rows with the same ``key`` (time series whose files overlap).

A module may also declare ``SQL_DATASETS``: Postgres tables derived from other tables by one ``SELECT``
(joins, PostGIS overlays, events across snapshots), rebuilt after its datasets on every run.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Literal

import polars as pl

from basecast_pipelines.common.manifest import MANIFEST_NAME, ManifestEntry, raw_prefix
from basecast_pipelines.common.storage import Storage

Target = Literal["postgres", "bigquery"]
Mode = Literal["replace", "by_file", "by_key"]


@dataclass(frozen=True)
class RawFile:
    """One file in ``raw/source=<id>/dt=<date>/`` with its manifest entry."""

    storage: Storage = field(repr=False, compare=False)
    source: str
    dt: date
    key: str
    entry: ManifestEntry = field(repr=False, compare=False)

    @property
    def name(self) -> str:
        return PurePosixPath(self.key).name

    @property
    def suffix(self) -> str:
        return PurePosixPath(self.key).suffix.lower()

    @property
    def sha256(self) -> str:
        return self.entry.sha256

    @property
    def url(self) -> str:
        return self.entry.url

    @property
    def meta(self) -> dict:
        return self.entry.meta or {}

    def read_bytes(self) -> bytes:
        return self.storage.read_bytes(self.key)

    def local_path(self) -> AbstractContextManager[Path]:
        """A filesystem path to the file (downloaded to a temp file when the lake is GCS)."""
        return self.storage.local_copy(self.key)


ParseFn = Callable[[RawFile], "pl.DataFrame | None"]
BuildFn = Callable[[list[RawFile]], "pl.DataFrame | None"]


@dataclass(frozen=True)
class Dataset:
    name: str
    target: Target
    mode: Mode
    description: str
    parse: ParseFn | None = None
    build: BuildFn | None = None
    inputs: Callable[[RawFile], bool] = lambda f: True
    select: Callable[[list[RawFile]], list[RawFile]] | None = None
    finalize: Callable[[pl.DataFrame], pl.DataFrame] | None = None
    # Called once with the files about to be parsed (by_file/by_key), e.g. to fetch slow external answers
    # for all of them in parallel before the sequential parse.
    prepare: Callable[[list["RawFile"]], None] | None = None
    key: tuple[str, ...] = ()
    version: int = 1
    geometry: Mapping[str, int] = field(default_factory=dict)
    partition: tuple[str, str] | None = None
    cluster: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (self.parse is None) == (self.build is None):
            raise ValueError(f"{self.name}: give exactly one of parse or build")
        if self.mode == "by_key" and not self.key:
            raise ValueError(f"{self.name}: by_key needs key columns")
        if self.build is not None and self.mode != "replace":
            raise ValueError(f"{self.name}: build works on many files at once, so its mode must be replace")
        if self.geometry and self.target != "postgres":
            raise ValueError(f"{self.name}: geometry columns are only supported in Postgres")
        if self.partition and self.partition[1] not in {"DAY", "MONTH", "YEAR"}:
            raise ValueError(f"{self.name}: partition granularity must be DAY, MONTH or YEAR")

    def files(self, raw: Sequence[RawFile]) -> list[RawFile]:
        chosen = [f for f in raw if self.inputs(f)]
        return self.select(chosen) if self.select else chosen


@dataclass(frozen=True)
class SqlDataset:
    """A Postgres table rebuilt from ``sql`` (a SELECT over other tables) and swapped in atomically."""

    name: str
    sql: str
    description: str
    indexes: tuple[tuple[str, ...], ...] = ()


def list_raw_files(storage: Storage, source: str) -> list[RawFile]:
    """Every file recorded in the source's manifests, oldest snapshot first."""
    files: list[RawFile] = []
    for key in storage.list(raw_prefix(source)):
        if not key.endswith("/" + MANIFEST_NAME):
            continue
        payload = json.loads(storage.read_bytes(key))
        prefix = key.removesuffix(MANIFEST_NAME)
        dt = date.fromisoformat(payload["dt"])
        for e in payload["entries"]:
            files.append(RawFile(storage, source, dt, prefix + e["file"], ManifestEntry(**e)))
    files.sort(key=lambda f: (f.dt, f.entry.fetched_at, f.key))
    return files


# --- selection helpers for Dataset.select ------------------------------------------------------------


def latest_dt(files: list[RawFile]) -> list[RawFile]:
    """Only the files of the newest ``dt=`` snapshot."""
    if not files:
        return []
    newest = max(f.dt for f in files)
    return [f for f in files if f.dt == newest]


def latest_by(group: Callable[[RawFile], object]) -> Callable[[list[RawFile]], list[RawFile]]:
    """The newest file of each group (e.g. by URL, for files the source overwrites in place)."""

    def select(files: list[RawFile]) -> list[RawFile]:
        newest: dict[object, RawFile] = {}
        for f in files:  # files come oldest first
            newest[group(f)] = f
        return sorted(newest.values(), key=lambda f: (f.dt, f.entry.fetched_at, f.key))

    return select


latest_by_url = latest_by(lambda f: f.url)


def iter_frames(dataset: Dataset, files: list[RawFile]) -> Iterator[tuple[RawFile, pl.DataFrame]]:
    """Parse files one by one (``parse`` datasets), skipping files that yield nothing."""
    assert dataset.parse is not None
    for f in files:
        df = dataset.parse(f)
        if df is not None and df.height:
            yield f, df
