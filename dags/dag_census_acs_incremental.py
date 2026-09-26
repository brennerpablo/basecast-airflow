"""Airflow DAG: census_acs incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_census_acs_incremental = build_source_dag("census_acs")
