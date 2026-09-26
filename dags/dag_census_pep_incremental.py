"""Airflow DAG: census_pep incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_census_pep_incremental = build_source_dag("census_pep")
