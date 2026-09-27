"""``basecast_pipelines/marts/insights.py``: the pairing behind the date-by-date scores (no database)."""

from __future__ import annotations

from datetime import date

import polars as pl

from basecast_pipelines.marts import insights as I


def test_mape_pairs_cells_on_as_of_and_target_year():
    d1, d2 = date(2024, 7, 31), date(2025, 5, 31)
    cells = pl.DataFrame({
        "as_of": [d1, d2, d1, d2, d2], "target_year": [2025, 2026, 2025, 2026, 2027],
        "source": ["basecast", "basecast", "LTLF", "CDR", "basecast"], "error_pct": [2.0, -4.0, 8.0, 6.0, 10.0],
    })
    assert I._mape(cells, "basecast") == (16 / 3, 3)
    assert I._mape(cells, "basecast", "LTLF") == (2.0, 1)
    assert I._mape(cells, "CDR", "basecast") == (6.0, 1)


def test_queue_cards_must_name_their_queue():
    ok = pl.DataFrame({"id": ["A1", "B3", "B1"], "queue": ["large_load", "generation", None]})
    bad = pl.DataFrame({"id": ["B3"], "queue": [None]})
    assert I._queue_named(ok) and not I._queue_named(bad)
