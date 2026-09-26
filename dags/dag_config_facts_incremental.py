"""Airflow DAG: config_facts incremental, loads the files versioned in the repo (no raw source)."""

from basecast_dags.pipeline_graph import build_process_only_dag

dag_config_facts_incremental = build_process_only_dag("config_facts")
