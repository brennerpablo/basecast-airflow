"""Airflow DAG: eia_860m incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_eia_860m_incremental = build_source_dag("eia_860m")
