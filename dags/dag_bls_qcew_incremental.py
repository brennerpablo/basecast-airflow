"""Airflow DAG: bls_qcew incremental, scheduled raw fetch and processing (cadence in basecast_dags/config.py)."""

from basecast_dags.pipeline_graph import build_source_dag

dag_bls_qcew_incremental = build_source_dag("bls_qcew")
