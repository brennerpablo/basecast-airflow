"""DAG factories, in the fundsys ``pipeline_graph`` style: each thin ``dag_*.py`` only assigns a factory
result. Every source gets a scheduled incremental DAG (``fetch_raw`` then ``process``, each task one
``etl_run`` row per stage); ``dag_source_full`` is manual, takes the source, window and stages as
params, and serves backfills and reprocessing. The pipeline code itself never imports Airflow."""

from __future__ import annotations

import logging
from datetime import timedelta
from importlib.util import find_spec

from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from basecast_dags import runtime
from basecast_dags.config import (
    DEFAULT_ARGS,
    ERCOT_POOL,
    FULL_DAG_ID,
    HEAVY_POOL,
    PROCESS_ONLY_SCHEDULES,
    SCHEDULES,
    START_DATE,
)

log = logging.getLogger(__name__)


def dag_failure_callback(context) -> None:
    """Never raises. The failed run is also in etl_run (status failed, error), which the /data page shows."""
    try:
        run = context.get("dag_run")
        log.error("basecast DAG failed: %s run %s", getattr(run, "dag_id", "?"), getattr(run, "run_id", "?"))
    except Exception:  # noqa: BLE001
        pass


def _dag_kwargs() -> dict:
    return {
        "start_date": START_DATE,
        "catchup": False,
        "max_active_runs": 1,
        "default_args": DEFAULT_ARGS,
        "on_failure_callback": dag_failure_callback,
    }


def _has_parser(source_id: str) -> bool:
    """Checked without importing the parser (keeps DAG parsing fast)."""
    from basecast_pipelines.parsers import NoParserError, parser_path

    try:
        return find_spec(parser_path(source_id)) is not None
    except (NoParserError, ModuleNotFoundError):
        return False


def _tags(source_id: str, *extra: str) -> list[str]:
    return ["basecast", source_id.split("_", 1)[0], *extra]


def build_source_dag(source_id: str):
    cfg = SCHEDULES[source_id]
    with_parser = _has_parser(source_id)

    @dag(
        dag_id=f"dag_{source_id}_incremental",
        schedule=cfg.cron,
        description=f"{source_id}: fetch new raw files{' and process them' if with_parser else ''}.",
        tags=_tags(source_id, "incremental"),
        **_dag_kwargs(),
    )
    def _dag():
        @task(pool=cfg.fetch_pool, execution_timeout=cfg.fetch_timeout)
        def fetch_raw(**context):
            return runtime.fetch_raw(source_id, context, options=cfg.options(runtime.run_date(context)))

        fetched = fetch_raw()
        if with_parser:

            @task(pool=HEAVY_POOL if cfg.heavy else "default_pool", execution_timeout=cfg.process_timeout)
            def process(**context):
                return runtime.process(source_id, context)

            fetched >> process()

    return _dag()


def build_process_only_dag(source_id: str):
    cfg = PROCESS_ONLY_SCHEDULES[source_id]

    @dag(
        dag_id=f"dag_{source_id}_incremental",
        schedule=cfg.cron,
        description=f"{source_id}: load the files versioned in the repo.",
        tags=_tags(source_id, "incremental"),
        **_dag_kwargs(),
    )
    def _dag():
        @task(execution_timeout=cfg.process_timeout)
        def process(**context):
            return runtime.process(source_id, context)

        process()

    return _dag()


def build_full_dag():
    sources = sorted([*SCHEDULES, *PROCESS_ONLY_SCHEDULES])

    @dag(
        dag_id=FULL_DAG_ID,
        schedule=None,
        description="Backfill or reprocess one source: raw fetch over a window and/or processing.",
        tags=["basecast", "full"],
        params={
            "source": Param(sources[0], type="string", enum=sources),
            # Arrays also accept a string: the API returns 500 on list conf for a plain "string" type.
            "stages": Param(["raw", "process"], type=["array", "string"], items={"type": "string"}),
            "since": Param(None, type=["null", "string"], description="raw dt >= YYYY-MM-DD"),
            "until": Param(None, type=["null", "string"], description="raw dt <= YYYY-MM-DD"),
            "options": Param({}, type="object", description="source discovery options, e.g. {\"start_year\": 1980}"),
            "datasets": Param([], type=["array", "string"], items={"type": "string"}),
            "reprocess": Param(False, type="boolean"),
            "rebuild": Param(False, type="boolean"),
        },
        **_dag_kwargs(),
    )
    def _dag():
        def _values(value) -> list[str]:
            if isinstance(value, str):
                return [v.strip() for v in value.split(",") if v.strip()]
            return list(value or [])

        @task(pool=ERCOT_POOL, execution_timeout=timedelta(hours=8))
        def fetch_raw(**context):
            p = context["params"]
            if "raw" not in _values(p["stages"]) or p["source"] in PROCESS_ONLY_SCHEDULES:
                raise AirflowSkipException("raw stage not requested")
            return runtime.fetch_raw(p["source"], context, since=p["since"], until=p["until"], options=p["options"])

        @task(pool=HEAVY_POOL, execution_timeout=timedelta(hours=8), trigger_rule="none_failed")
        def process(**context):
            p = context["params"]
            if "process" not in _values(p["stages"]):
                raise AirflowSkipException("process stage not requested")
            if p["source"] not in PROCESS_ONLY_SCHEDULES and not _has_parser(p["source"]):
                raise AirflowSkipException(f"{p['source']} has no parser yet")
            return runtime.process(
                p["source"], context, since=p["since"], until=p["until"], datasets=_values(p["datasets"]),
                reprocess=p["reprocess"], rebuild=p["rebuild"],
            )

        fetch_raw() >> process()

    return _dag()
