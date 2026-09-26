"""The validation lock in the marts: the held-out accounts leave the universe by name, before any signal.

Same rule as ``analysis/q3_signals.py``: ``accounts.find_names`` over the PUCT names, and every one of the five
names in ``config/marts.yaml`` (``validation.held_out_names``) must be found. Nothing here says anything about
those accounts beyond dropping them.
"""

from __future__ import annotations

import polars as pl


def held_out_ids(accounts: pl.DataFrame, names: list[str]) -> list[str]:
    """``account_id`` of the accounts matching ``names`` (``accounts``: ``account_id``, ``name``); raises unless
    every name is found."""
    from basecast_pipelines.models import accounts as A

    found = A.find_names(accounts["name"], names)["found"].drop_nulls().to_list()
    ids = accounts.filter(pl.col("name").is_in(found))["account_id"].to_list()
    if len(found) != len(names) or len(ids) != len(names):
        raise ValueError(f"validation hold-out found {len(ids)} of {len(names)} accounts")
    return ids


def without_held_out(universe: pl.DataFrame, accounts: pl.DataFrame, names: list[str]) -> pl.DataFrame:
    """``universe`` minus the held-out accounts."""
    return universe.filter(~pl.col("account_id").is_in(held_out_ids(accounts, names)))
