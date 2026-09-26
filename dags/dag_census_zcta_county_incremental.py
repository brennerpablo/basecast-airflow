"""Airflow DAG: census_zcta_county incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_census_zcta_county_incremental = build_source_dag("census_zcta_county")
