"""Models behind the product (backtest, generation-queue survival, account score, load vs. weather).

Phase 0 (``docs/PHASE0_ANALYSIS.md``) grows the logic here and the ``analysis/`` scripts only call it. Their
extra libraries live in the ``analysis`` dependency group (``uv sync --group analysis``), not in the Airflow
image; a model that moves into a DAG promotes its libraries to ``[project].dependencies`` and
``deploy/airflow/requirements.txt`` together.
"""
