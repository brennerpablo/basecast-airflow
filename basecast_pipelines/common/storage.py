"""Lake storage. Keys are POSIX paths relative to the lake root (e.g. ``raw/source=x/dt=2026-09-25/f.xlsx``),
identical locally (``file://``) and in the GCS bucket (``gs://``)."""

from __future__ import annotations

import fcntl
import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePosixPath
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
    def local_copy(self, key: str) -> AbstractContextManager[Path]: ...


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
    def local_copy(self, key: str) -> Iterator[Path]:
        """A filesystem path to read the object from (the object itself, for the local lake)."""
        yield self._path(key)

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


class GcsStorage:
    """GCS-backed lake with the same guarantees as ``LocalStorage``: a write without ``overwrite`` is a
    create-only precondition (``if_generation_match=0``), so an existing object is never replaced."""

    LOCK_STALE_S = 3600

    def __init__(self, bucket: str, prefix: str = "", *, client=None, project: str | None = None) -> None:
        if client is None:
            from basecast_pipelines.common.gcp import storage_client

            client = storage_client(project)
        self.client = client
        self.bucket = client.bucket(bucket)
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""

    def _name(self, key: str) -> str:
        key = key.lstrip("/")
        if ".." in PurePosixPath(key).parts:
            raise StorageError(f"key escapes the lake root: {key!r}")
        return self.prefix + key

    def uri(self, key: str) -> str:
        return f"gs://{self.bucket.name}/{self._name(key)}"

    def exists(self, key: str) -> bool:
        return self.bucket.blob(self._name(key)).exists()

    def read_bytes(self, key: str) -> bytes:
        return self.bucket.blob(self._name(key)).download_as_bytes()

    def size(self, key: str) -> int:
        blob = self.bucket.get_blob(self._name(key))
        if blob is None:
            raise FileNotFoundError(key)
        return blob.size

    def write_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> None:
        from google.api_core.exceptions import PreconditionFailed

        try:
            self.bucket.blob(self._name(key)).upload_from_string(
                data, if_generation_match=None if overwrite else 0
            )
        except PreconditionFailed as exc:
            raise ObjectExistsError(key) from exc

    def put_file(self, key: str, src: Path, *, overwrite: bool = False) -> None:
        """Upload ``src`` and remove it. Without ``overwrite`` this fails if the key exists."""
        from google.api_core.exceptions import PreconditionFailed

        try:
            self.bucket.blob(self._name(key)).upload_from_filename(
                str(src), if_generation_match=None if overwrite else 0
            )
        except PreconditionFailed as exc:
            raise ObjectExistsError(key) from exc
        src.unlink()

    def list(self, prefix: str) -> list[str]:
        name = self._name(prefix)
        if name and not name.endswith("/"):
            if self.bucket.blob(name).exists():
                return [prefix]
            name += "/"
        return sorted(
            blob.name.removeprefix(self.prefix)
            for blob in self.client.list_blobs(self.bucket, prefix=name)
            if not PurePosixPath(blob.name).name.startswith(".") and not blob.name.endswith("/")
        )

    def temp_path(self, suffix: str = "") -> Path:
        tmp_dir = Path(os.environ.get("BASECAST_TMP_DIR", tempfile.gettempdir())) / "basecast"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        return tmp_dir / f"{uuid4().hex}{suffix}"

    @contextmanager
    def local_copy(self, key: str) -> Iterator[Path]:
        """Download the object to a temporary file, removed on exit."""
        path = self.temp_path(PurePosixPath(key).suffix)
        self.bucket.blob(self._name(key)).download_to_filename(str(path))
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)

    @contextmanager
    def locked(self, key: str) -> Iterator[None]:
        """Exclusive lock through a create-only lock object; a lock older than an hour is taken as stale."""
        from google.api_core.exceptions import NotFound, PreconditionFailed

        name = self._name(key)
        lock = self.bucket.blob(str(PurePosixPath(name).with_name(f".{PurePosixPath(name).name}.lock")))
        delay = 0.2
        while True:
            try:
                lock.upload_from_string(str(time.time()), if_generation_match=0)
                break
            except PreconditionFailed:
                try:
                    lock.reload()
                    if lock.time_created and time.time() - lock.time_created.timestamp() > self.LOCK_STALE_S:
                        lock.delete(if_generation_match=lock.generation)
                        continue
                except (NotFound, PreconditionFailed):
                    continue
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
        try:
            yield
        finally:
            try:
                lock.delete()
            except NotFound:
                pass


def storage_from_uri(uri: str) -> Storage:
    if uri.startswith("file://"):
        path = Path(uri.removeprefix("file://"))
        return LocalStorage(path if path.is_absolute() else PROJECT_ROOT / path)
    if uri.startswith("gs://"):
        bucket, _, prefix = uri.removeprefix("gs://").partition("/")
        return GcsStorage(bucket, prefix)
    raise StorageError(f"unsupported storage URI: {uri!r}")
