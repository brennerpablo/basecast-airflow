"""Airflow DAG: puct_filings incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_puct_filings_incremental = build_source_dag("puct_filings")
