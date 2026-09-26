"""Airflow DAG: ercot_settlement_points incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_ercot_settlement_points_incremental = build_source_dag("ercot_settlement_points")
