"""Airflow DAG: pudl incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_pudl_incremental = build_source_dag("pudl")
