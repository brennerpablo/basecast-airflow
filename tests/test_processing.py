from datetime import date

import polars as pl
import pytest

from basecast_pipelines.config import Settings
from basecast_pipelines.processing import runner
from basecast_pipelines.processing.core import Dataset, latest_by_url, latest_dt, list_raw_files
from basecast_pipelines.processing.tabular import excel_date, find_header_row, num, snake, with_header
from tests.parsers.conftest import raw_file  # noqa: F401  (fixture)


def test_dataset_needs_exactly_one_of_parse_or_build():
    with pytest.raises(ValueError):
        Dataset(name="x", target="postgres", mode="replace", description="")
    with pytest.raises(ValueError):
        Dataset(name="x", target="postgres", mode="by_key", description="", parse=lambda f: None)
    with pytest.raises(ValueError):
        Dataset(name="x", target="bigquery", mode="by_file", description="", build=lambda fs: None)


def test_list_raw_files_and_selection(raw_file, storage):
    raw_file("s", "ercot_ziptozone/appendix_d_trimmed.xlsx", dt=date(2026, 1, 1), url="https://x/a")
    raw_file("s", "ercot_ziptozone/appendix_d_trimmed.xlsx", dt=date(2026, 2, 1), name="b.xlsx", url="https://x/a")
    raw_file("s", "ercot_ziptozone/appendix_d_trimmed.xlsx", dt=date(2026, 1, 1), name="c.xlsx", url="https://x/c")
    files = list_raw_files(storage, "s")
    assert [f.dt for f in files] == [date(2026, 1, 1), date(2026, 1, 1), date(2026, 2, 1)]
    assert [f.name for f in latest_dt(files)] == ["b.xlsx"]
    assert sorted(f.name for f in latest_by_url(files)) == ["b.xlsx", "c.xlsx"]


def test_dry_run_adds_lineage_without_writing(raw_file, storage, monkeypatch):
    raw_file("s", "ercot_ziptozone/appendix_d_trimmed.xlsx")

    class Module:
        DATASETS = [
            Dataset(name="t", target="postgres", mode="by_file", description="",
                    parse=lambda f: pl.DataFrame({"x": [1, 2]})),
        ]

    monkeypatch.setattr(runner, "get_parser", lambda source: Module)
    [summary] = runner.run_process("s", storage=storage, settings=Settings("file://unused", "ua"), dry_run=True)
    assert summary.rows == 2 and summary.files == 1
    assert {"x", "source_file", "ingested_at"} <= set(summary.schema)
    assert summary.sample["source_file"][0].startswith("raw/source=s/dt=2026-09-25/")
    assert not storage.exists("_runs/etl_run.parquet")


def test_tabular_helpers():
    grid = pl.DataFrame({"c0": ["Title", None, "INR", "20INR0001"], "c1": [None, None, "Capacity (MW)", "1,200.5"]})
    assert find_header_row(grid, ["inr", r"capacity"]) == 2
    body = with_header(grid, 2)
    assert body.columns == ["inr", "capacity_mw"]
    assert body.select(num("capacity_mw")).item() == 1200.5
    assert snake("Projected COD (mm/dd/yyyy)") == "projected_cod_mm_dd_yyyy"
    dates = pl.DataFrame({"d": ["45000", "2024-03-01 00:00:00", "3/5/24", "03/05/2024", "x"]})
    assert dates.select(excel_date("d")).to_series().to_list() == [
        date(2023, 3, 15), date(2024, 3, 1), date(2024, 3, 5), date(2024, 3, 5), None
    ]
