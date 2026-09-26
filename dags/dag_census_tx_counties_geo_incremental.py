"""Airflow DAG: census_tx_counties_geo incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_census_tx_counties_geo_incremental = build_source_dag("census_tx_counties_geo")
