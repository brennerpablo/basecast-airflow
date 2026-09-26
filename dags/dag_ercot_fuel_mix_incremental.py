"""Airflow DAG: ercot_fuel_mix incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_ercot_fuel_mix_incremental = build_source_dag("ercot_fuel_mix")
