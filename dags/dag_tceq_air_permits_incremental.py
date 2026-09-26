"""Airflow DAG: tceq_air_permits incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_tceq_air_permits_incremental = build_source_dag("tceq_air_permits")
