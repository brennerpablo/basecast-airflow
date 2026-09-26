"""Airflow DAG: tx_comptroller incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_tx_comptroller_incremental = build_source_dag("tx_comptroller")
