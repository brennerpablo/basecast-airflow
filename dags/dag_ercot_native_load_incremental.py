"""Airflow DAG: ercot_native_load incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_ercot_native_load_incremental = build_source_dag("ercot_native_load")
