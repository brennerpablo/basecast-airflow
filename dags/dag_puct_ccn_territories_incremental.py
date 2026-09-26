"""Airflow DAG: puct_ccn_territories incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_puct_ccn_territories_incremental = build_source_dag("puct_ccn_territories")
