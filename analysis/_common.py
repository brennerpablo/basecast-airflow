"""Shared helpers for the phase 0 scripts (``docs/PHASE0_ANALYSIS.md``).

Each script is one question, written as ``# %%`` cells, and runs from the repo root with
``uv run --group analysis python analysis/qN_<name>.py``. The scripts only read (``basecast_reader``
through the Cloud SQL proxy, see ``basecast_pipelines/models/db.py``); figures go to ``analysis/out/``
(gitignored) and the numbers to ``docs/analysis/``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from basecast_pipelines.models.db import read_sql

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "analysis" / "out"

pl.Config.set_tbl_rows(60)
pl.Config.set_tbl_cols(30)
pl.Config.set_fmt_str_lengths(80)
pl.Config.set_tbl_width_chars(220)

__all__ = ["OUT", "ROOT", "out_path", "read_sql"]


def out_path(name: str) -> Path:
    """Path for a figure or scratch table under ``analysis/out/``, creating the folder."""
    OUT.mkdir(parents=True, exist_ok=True)
    return OUT / name
