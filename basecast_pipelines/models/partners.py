"""Base Power's current utility partners, from ``base_public_facts``.

Validation lock (``docs/PHASE0_ANALYSIS.md`` §0): the account score is built without knowing where the
partners fall, its weights are committed first, and only then (task A2) this module reveals the partners'
positions. Nothing in ``analysis/`` imports this module or reads ``base_public_facts``;
``tests/test_partner_lock.py`` enforces it.
"""

from __future__ import annotations

import polars as pl

from basecast_pipelines.models.db import read_sql


def partner_facts() -> pl.DataFrame:
    """Facts of category ``utility_partner``: one row per fact, ``entity`` as the source writes it."""
    return read_sql(
        "SELECT fact_id, entity, fact, source_url, source_date, verified "
        "FROM base_public_facts WHERE category = 'utility_partner' ORDER BY entity, source_date"
    )
