"""Airflow DAG: eia_860 incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_eia_860_incremental = build_source_dag("eia_860")
