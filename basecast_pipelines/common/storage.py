"""Lake storage. Keys are POSIX paths relative to the lake root (e.g. ``raw/source=x/dt=2026-09-25/f.xlsx``),
identical locally and in the future GCS bucket."""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from basecast_pipelines.config import PROJECT_ROOT


class StorageError(Exception):
    pass


class ObjectExistsError(StorageError):
    """Raised when a write would overwrite an existing object without ``overwrite=True``."""


class Storage(Protocol):
    def uri(self, key: str) -> str: ...
    def exists(self, key: str) -> bool: ...
    def read_bytes(self, key: str) -> bytes: ...
    def write_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> None: ...
    def put_file(self, key: str, src: Path, *, overwrite: bool = False) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def size(self, key: str) -> int: ...
    def temp_path(self, suffix: str = "") -> Path: ...
    def locked(self, key: str) -> AbstractContextManager[None]: ...


class LocalStorage:
    """Filesystem-backed lake. New objects appear atomically and are never overwritten unless asked."""

    TMP_DIR = "_tmp"

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = (self.root / key.lstrip("/")).resolve()
        if path != self.root and self.root not in path.parents:
            raise StorageError(f"key escapes the lake root: {key!r}")
        return path

    def uri(self, key: str) -> str:
        return self._path(key).as_uri()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def size(self, key: str) -> int:
        return self._path(key).stat().st_size

    def write_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> None:
        tmp = self.temp_path()
        tmp.write_bytes(data)
        self.put_file(key, tmp, overwrite=overwrite)

    def put_file(self, key: str, src: Path, *, overwrite: bool = False) -> None:
        """Move ``src`` into place. Without ``overwrite`` this fails if the key exists (hard link, no race)."""
        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if overwrite:
            os.replace(src, dest)
            return
        try:
            os.link(src, dest)
        except FileExistsError as exc:
            raise ObjectExistsError(key) from exc
        src.unlink()

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if base.is_file():
            return [prefix]
        if not base.exists():
            return []
        return sorted(
            p.relative_to(self.root).as_posix()
            for p in base.rglob("*")
            if p.is_file() and not p.name.startswith(".")
        )

    def temp_path(self, suffix: str = "") -> Path:
        tmp_dir = self.root / self.TMP_DIR
        tmp_dir.mkdir(parents=True, exist_ok=True)
        return tmp_dir / f"{uuid4().hex}{suffix}"

    @contextmanager
    def locked(self, key: str) -> Iterator[None]:
        """Exclusive advisory lock for read-modify-write objects shared across processes."""
        lock_path = self._path(key).with_name(f".{self._path(key).name}.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "w") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)


def storage_from_uri(uri: str) -> Storage:
    if uri.startswith("file://"):
        path = Path(uri.removeprefix("file://"))
        return LocalStorage(path if path.is_absolute() else PROJECT_ROOT / path)
    if uri.startswith("gs://"):
        raise NotImplementedError("GCS storage arrives with the move to GCP")
    raise StorageError(f"unsupported storage URI: {uri!r}")
