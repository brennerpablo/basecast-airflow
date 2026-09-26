"""Every DAG file imports cleanly and every source has a scheduled DAG. Needs Airflow, which is not a
project dependency (it lives in the Airflow image): runs where ``airflow`` is importable, skips elsewhere."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parent.parent / "dags"

airflow = pytest.importorskip("airflow")


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory):
    import os

    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow_home")))
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    sys.path.insert(0, str(DAGS))
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # older layout
        from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=str(DAGS), safe_mode=True)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_every_source_has_an_incremental_dag(dagbag):
    from basecast_pipelines.parsers import PARSER_ONLY
    from basecast_pipelines.sources import SOURCE_MODULES

    expected = {f"dag_{s}_incremental" for s in [*SOURCE_MODULES, *PARSER_ONLY]} | {"dag_source_full"}
    assert set(dagbag.dag_ids) == expected


def test_dag_defaults_follow_the_fundsys_conventions(dagbag):
    for dag_id in dagbag.dag_ids:
        dag = dagbag.dags[dag_id]
        assert dag.catchup is False and dag.max_active_runs == 1, dag_id
        assert str(dag.timezone.name) == "America/Chicago", dag_id
        assert dag.on_failure_callback is not None, dag_id


def test_incremental_dags_fetch_then_process(dagbag):
    dag = dagbag.dags["dag_ercot_ziptozone_incremental"]
    assert set(dag.task_ids) == {"fetch_raw", "process"}
    assert dag.get_task("fetch_raw").pool == "ercot_http"
    assert "process" in dag.get_task("fetch_raw").downstream_task_ids


def test_thin_dag_files_mention_airflow_and_dag():
    """Airflow 3's safe-mode discovery skips files that never mention both words."""
    for path in DAGS.glob("dag_*.py"):
        head = path.read_text().splitlines()[0]
        assert head.startswith('"""Airflow DAG:'), path.name
