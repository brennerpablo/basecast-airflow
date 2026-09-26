"""Airflow DAG: eia_861 incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_eia_861_incremental = build_source_dag("eia_861")
