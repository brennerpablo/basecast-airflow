from __future__ import annotations

import logging

import pytest

from basecast_pipelines.common import ops_log
from basecast_pipelines.common.etl_run import EtlRun


@pytest.fixture
def rows():
    written: list[dict] = []
    assert ops_log.install(insert=written.extend)
    yield written
    ops_log.uninstall()


def test_off_without_a_database():
    assert ops_log.install() is False
    ops_log.emit("info", "etl_run.start", "nothing happens")  # no writer, no error


def test_a_run_writes_start_events_and_end_with_its_run_id(storage, rows):
    with EtlRun("ercot_gis", storage, stage="raw") as run:
        run.event("discovered", files=3)
        run.event("unchanged", url="https://example.test/a.xlsx", sha256="0" * 64)
        run.event("error", url="https://example.test/b.xlsx", error="HTTPStatusError: 429 Too Many Requests")
    ops_log.flush()

    assert [r["event"] for r in rows] == ["etl_run.start", "etl.discovered", "etl.error", "etl_run.partial"]
    assert {r["run_id"] for r in rows} == {run.run_id}
    assert all(r["service"] == "airflow" and r["context"]["source"] == "ercot_gis" for r in rows)
    error = rows[2]
    assert (error["level"], error["error_class"]) == ("error", "HTTPStatusError")
    assert error["fingerprint"] == "airflow:HTTPStatusError:ercot_gis"
    end = rows[3]
    assert end["level"] == "warn"
    assert end["context"]["item_errors"] == 1
    assert isinstance(end["duration_ms"], int)


def test_a_failed_run_carries_the_exception(storage, rows):
    with pytest.raises(ValueError), EtlRun("eia_860m", storage, stage="process"):
        raise ValueError("expected column 'INR' not found")
    ops_log.flush()

    failed = rows[-1]
    assert failed["event"] == "etl_run.failed"
    assert failed["level"] == "error"
    assert failed["error_class"] == "ValueError"
    assert "expected column 'INR' not found" in failed["error_stack"]
    assert failed["fingerprint"] == "airflow:ValueError:eia_860m"


def test_warnings_from_pipeline_loggers_are_stored_with_the_run_but_info_is_not(storage, rows):
    logger = logging.getLogger("basecast_pipelines.sources.test")
    with EtlRun("open_meteo", storage) as run:
        logger.info("one line per file, stays in the task log")
        logger.warning("column renamed upstream")
    logging.getLogger("somewhere.else").warning("not ours")
    ops_log.flush()

    logged = [r for r in rows if r["event"] == "log"]
    assert [(r["level"], r["message"]) for r in logged] == [("warn", "column renamed upstream")]
    assert logged[0]["run_id"] == run.run_id
    assert logged[0]["context"]["logger"] == "basecast_pipelines.sources.test"


def test_a_failed_insert_never_reaches_the_pipeline(storage, capsys):
    def broken(_rows):
        raise RuntimeError("relation ops.log does not exist")

    ops_log.install(insert=broken)
    try:
        with EtlRun("census_acs", storage):
            pass
        ops_log.flush()
    finally:
        ops_log.uninstall()
    assert "ops.log: dropped" in capsys.readouterr().err


def test_secrets_are_redacted_from_the_context(storage, rows):
    with EtlRun("puct_filings", storage, params={"api_key": "abc", "since": "2026-09-01"}):
        pass
    ops_log.flush()
    assert rows[0]["context"]["params"] == {"api_key": "[redacted]", "since": "2026-09-01"}
