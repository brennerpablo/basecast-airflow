"""Task bodies shared by the DAG factories: build storage/HTTP from the environment, turn on ops.log, tag
etl_run rows with the Airflow run, and call the Airflow-free pipeline entry points."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

log = logging.getLogger(__name__)


def _labels(context: dict[str, Any]) -> dict:
    ti = context["ti"]
    return {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "airflow_run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
    }


def _day(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def run_date(context: dict[str, Any]) -> date:
    """The run's date in America/Chicago (manual runs have no logical date in Airflow 3)."""
    from basecast_pipelines.config import LOCAL_TZ

    moment = context.get("logical_date") or context["dag_run"].run_after
    return moment.astimezone(LOCAL_TZ).date()


def fetch_raw(source_id: str, context: dict[str, Any], *, since=None, until=None, options: dict | None = None) -> dict:
    from basecast_pipelines.common import ops_log
    from basecast_pipelines.common.etl_run import airflow_labels
    from basecast_pipelines.common.http import HttpClient
    from basecast_pipelines.common.storage import storage_from_uri
    from basecast_pipelines.config import load_settings
    from basecast_pipelines.sources import get_source

    ops_log.install()
    settings = load_settings()
    storage = storage_from_uri(settings.storage_root)
    airflow_labels.set(_labels(context))
    options = dict(options or {})
    log.info("fetch %s since=%s until=%s options=%s", source_id, since, until, options)
    with HttpClient(user_agent=settings.user_agent) as http:
        files = get_source(source_id).run(
            storage=storage, http=http, since=_day(since), until=_day(until), **options
        )
    return {"source": source_id, "discovered": len(files)}


def process(source_id: str, context: dict[str, Any], *, since=None, until=None, datasets=None,
            reprocess: bool = False, rebuild: bool = False) -> dict:
    from basecast_pipelines.common import ops_log
    from basecast_pipelines.common.etl_run import airflow_labels
    from basecast_pipelines.common.storage import storage_from_uri
    from basecast_pipelines.config import load_settings
    from basecast_pipelines.processing.runner import run_process

    ops_log.install()
    settings = load_settings()
    storage = storage_from_uri(settings.storage_root)
    airflow_labels.set(_labels(context))
    summaries = run_process(
        source_id, storage=storage, settings=settings, datasets=datasets or None, since=_day(since),
        until=_day(until), reprocess=reprocess, rebuild=rebuild,
    )
    return {s.dataset: s.event() for s in summaries}
