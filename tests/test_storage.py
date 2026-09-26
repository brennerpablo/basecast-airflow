from __future__ import annotations

import pytest

from basecast_pipelines.common.storage import LocalStorage, ObjectExistsError, StorageError


def test_write_refuses_overwrite(storage: LocalStorage) -> None:
    storage.write_bytes("raw/source=x/dt=2026-01-01/a.txt", b"one")
    with pytest.raises(ObjectExistsError):
        storage.write_bytes("raw/source=x/dt=2026-01-01/a.txt", b"two")
    assert storage.read_bytes("raw/source=x/dt=2026-01-01/a.txt") == b"one"


def test_overwrite_when_asked(storage: LocalStorage) -> None:
    storage.write_bytes("_runs/etl_run.parquet", b"one")
    storage.write_bytes("_runs/etl_run.parquet", b"two", overwrite=True)
    assert storage.read_bytes("_runs/etl_run.parquet") == b"two"


def test_put_file_moves_temp_into_place(storage: LocalStorage) -> None:
    tmp = storage.temp_path()
    tmp.write_bytes(b"payload")
    storage.put_file("raw/source=x/dt=2026-01-01/f.bin", tmp)
    assert not tmp.exists()
    assert storage.list("raw/") == ["raw/source=x/dt=2026-01-01/f.bin"]


def test_keys_cannot_escape_root(storage: LocalStorage) -> None:
    with pytest.raises(StorageError):
        storage.write_bytes("../outside.txt", b"x")
