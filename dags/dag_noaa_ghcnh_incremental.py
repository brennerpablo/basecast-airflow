"""Airflow DAG: noaa_ghcnh incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_noaa_ghcnh_incremental = build_source_dag("noaa_ghcnh")
