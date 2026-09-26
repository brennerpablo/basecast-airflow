"""Airflow DAG: ercot_load_wz_daily incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_ercot_load_wz_daily_incremental = build_source_dag("ercot_load_wz_daily")
