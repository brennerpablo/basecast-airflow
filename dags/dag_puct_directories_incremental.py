"""Airflow DAG: puct_directories incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_puct_directories_incremental = build_source_dag("puct_directories")
