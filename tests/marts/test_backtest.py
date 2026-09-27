"""The backtest marts' pure parts (``basecast_pipelines/marts/backtest.py``): eras, cell rows, pairing, the queue
summary and the fan, on synthetic frames (no database)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.marts import backtest as B
from basecast_pipelines.marts import config as marts_config


def test_era_splits_at_the_first_as_of_after_ltlf_2024():
    assert B.era_of(date(2024, 5, 31)) == "before_tsp_loads"
    assert B.era_of(date(2024, 7, 31)) == "with_tsp_loads"
    assert {e["era"] for e in B.ERAS} == {B.era_of(d) for d in B.PEAK_AS_OF_DATES}


def test_last_summer_counts_from_september_and_months_roll_over():
    assert B.last_summer(date(2026, 9, 26)) == 2026
    assert B.last_summer(date(2026, 8, 31)) == 2025
    assert B.add_months(date(2024, 8, 1), 1) == date(2024, 9, 1)
    assert B.add_months(date(2024, 12, 1), 1) == date(2025, 1, 1)
    assert B.add_months(date(2022, 7, 1), 24) == date(2024, 7, 1)


def test_cell_row_fills_every_column_and_rejects_unknown_ones():
    row = B.cell_row(source="LTLF", p50_mw=1.0)
    assert set(row) == set(B.CELL_SCHEMA) and row["p10_mw"] is None and row["source"] == "LTLF"
    with pytest.raises(KeyError):
        B.cell_row(nope=1)


def _cells(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame([B.cell_row(as_of=a, target_year=y, source=s, error_pct=e) for a, y, s, e in rows],
                        schema=B.CELL_SCHEMA)


def test_paired_keeps_only_the_cells_both_sides_have():
    d1, d2 = date(2024, 5, 31), date(2024, 7, 31)
    cells = _cells([(d1, 2025, "basecast", -2.0), (d1, 2026, "basecast", 4.0), (d2, 2026, "basecast", -1.0),
                    (d1, 2025, "LTLF", 3.0), (d2, 2026, "LTLF", -5.0), (d1, 2026, "CDR", 6.0),
                    (d1, 2025, "basecast_organic_only", -9.0)])
    ltlf = B.paired(cells, "LTLF").sort("as_of")
    assert ltlf.height == 2
    assert B.mape(ltlf["error_pct"]) == pytest.approx(1.5)
    assert B.mape(ltlf["official_pct"]) == pytest.approx(4.0)
    assert B.paired(cells, "CDR")["error_pct"].to_list() == [4.0]


def test_official_rows_take_the_cells_horizon_and_score_the_p50():
    official = pl.DataFrame({"product": ["LTLF", "LTLF-prelim"], "vintage": ["LTLF 2025", "2026 preliminary LTLF"],
                             "vintage_date": [date(2025, 4, 8), date(2026, 4, 15)], "target_year": [2026, 2026],
                             "forecast_mw": [94_650.0, 112_000.0], "series": ["a", "b"]})
    rows = B.official_cell_rows(official, date(2026, 5, 31), {2026: 91_134.0}, {2026: False},
                                {("LTLF-prelim", "2026 preliminary LTLF", 2026): False})
    ltlf, prelim = rows
    assert ltlf["horizon"] == 1 and ltlf["vintage_horizon"] == 2  # the cell's horizon, and the vintage's own
    assert ltlf["source"] == ltlf["product"] == "LTLF" and ltlf["era"] == "with_tsp_loads"
    assert ltlf["p10_mw"] is None and ltlf["in_band"] is None and ltlf["actual_final"] is False
    assert ltlf["error_pct"] == pytest.approx(100 * (94_650 / 91_134 - 1))
    assert ltlf["verified"] is True and prelim["verified"] is False


def test_prelim_verified_reads_the_manual_figure_behind_the_base_scenario():
    raw = pl.DataFrame({"metric": ["peak_demand", "peak_demand", "peak_mw"], "season": ["summer"] * 3,
                        "scenario": ["base", "range_low", "base"], "vintage": ["P", "P", "LTLF 2025"],
                        "target_year": [2026] * 3, "row_label": ["fig_base", "fig_low", None]})
    assert B.prelim_verified(raw, {"fig_base": True}) == {("LTLF-prelim", "P", 2026): True}
    assert B.prelim_verified(raw, {}) == {("LTLF-prelim", "P", 2026): False}


def test_queue_rows_sum_statewide_and_by_contract_stratum():
    scored = pl.DataFrame({"stratum": ["solar", "solar", "gas+other"], "capacity_mw": [100.0, 50.0, 200.0],
                           "mw_w": [30.0, 10.0, 20.0], "actual": [50.0, 0.0, 0.0], "projected": [100.0, 50.0, 0.0]})
    out = B.queue_backtest_rows(scored, date(2022, 6, 1)).sort("stratum")
    assert out["stratum"].to_list() == ["all", "gas_other", "solar"]
    total = out.row(0, named=True)
    assert (total["projects"], total["raw_mw"], total["pred_mw"], total["actual_mw"],
            total["developer_projected_mw"]) == (3, 350.0, 60.0, 50.0, 150.0)
    assert total["error_pct"] == pytest.approx(20.0)
    assert out.filter(pl.col("stratum") == "solar")["error_pct"].item() == pytest.approx(-20.0)


def test_county_rho_ranks_each_ranking_against_what_was_built():
    scored = pl.DataFrame({"county_fips": ["001", "003", "005", "007"], "mw_w": [1.0, 2.0, 3.0, 4.0],
                           "actual": [10.0, 20.0, 30.0, 40.0], "capacity_mw": [4.0, 3.0, 2.0, 1.0],
                           "projected": [1.0, 3.0, 2.0, 4.0]})
    rho = B.county_rho(scored)
    assert rho["county_rho_adj"] == pytest.approx(1.0)
    assert rho["county_rho_raw"] == pytest.approx(-1.0)
    assert rho["county_rho_developer"] == pytest.approx(0.8)


def test_fan_rows_mark_every_kind_of_the_summer():
    raw = pl.DataFrame({
        "product": ["LTLF", "LTLF", "LTLF", "LTLF", "LTLF"],
        "metric": ["peak_demand", "peak_demand", "peak_demand", "peak_mw", "peak_demand"],
        "season": ["summer"] * 5, "region_type": ["system", "system", "system", "ercot", "system"],
        "scenario": ["base", "range_low", "range_high", "tsp_provided", "base"],
        "vintage": ["P", "P", "P", "LTLF 2025", "P"], "vintage_date": [date(2026, 4, 15)] * 3 + [date(2025, 4, 8)] * 2,
        "target_year": [2026, 2026, 2026, 2026, 2029], "value": [112_000.0, 90_500.0, 98_000.0, 109_031.0, 1.0],
        "row_label": ["f_base", "f_low", "f_high", None, "f_2029"],
    })
    base = pl.DataFrame({"product": ["LTLF", "CDR", "LTLF-prelim", "LTLF"],
                         "vintage": ["LTLF 2025", "CDR Dec 2025", "P", "LTLF 2025"],
                         "vintage_date": [date(2025, 4, 8), date(2025, 12, 1), date(2026, 4, 15), date(2025, 4, 8)],
                         "target_year": [2026, 2026, 2026, 2027], "forecast_mw": [94_650.0, 95_419.0, 112_000.0, 9.0],
                         "series": ["summer peak_mw ercot_adjusted", "cdr", "manual", "x"]})
    cells = pl.DataFrame([
        B.cell_row(as_of=date(2025, 12, 31), target_year=2026, source="basecast", p10_mw=1.0, p50_mw=2.0,
                   p90_mw=3.0, verified=False),
        B.cell_row(as_of=date(2025, 5, 31), target_year=2026, source="LTLF", p50_mw=4.0, verified=True),
        B.cell_row(as_of=date(2025, 5, 31), target_year=2025, source="basecast", p50_mw=5.0, verified=False),
    ], schema=B.CELL_SCHEMA)
    actual = {"hourly_peak_mw": 91_134.0, "final": False}
    fan = B.fan_rows(2026, raw, base, {"f_base": True, "f_low": True, "f_high": False}, actual, cells)
    assert fan["target_year"].unique().to_list() == [2026]
    assert fan.group_by("kind").len().sort("kind").rows() == [
        ("actual", 1), ("model", 1), ("official", 3), ("official_preliminary", 1), ("official_range", 1)]
    prelim = fan.filter(pl.col("kind") == "official_preliminary").row(0, named=True)
    assert (prelim["value_mw"], prelim["product"], prelim["method"], prelim["verified"]) == (
        112_000.0, "LTLF-prelim", "manual", True)
    rng = fan.filter(pl.col("kind") == "official_range").row(0, named=True)
    assert (rng["value_mw"], rng["low_mw"], rng["high_mw"], rng["verified"]) == (None, 90_500.0, 98_000.0, False)
    assert set(fan.filter(pl.col("kind") == "official")["label"]) == {
        "LTLF 2025, ERCOT-adjusted", "LTLF 2025, TSP-provided", "CDR Dec 2025"}
    act = fan.filter(pl.col("kind") == "actual").row(0, named=True)
    assert act["final"] is False and act["label"].endswith("(preliminary)")
    model = fan.filter(pl.col("kind") == "model").row(0, named=True)
    assert (model["vintage"], model["value_mw"], model["low_mw"], model["high_mw"], model["verified"]) == (
        "2025-12-31", 2.0, 1.0, 3.0, False)
    assert not fan.select("target_year", "kind", "label").is_duplicated().any()


def test_golden_checks_hold_only_at_the_docs_date_and_switches():
    config = marts_config.load()
    assert B._as_x7(config) and B._as_x2(config)
    golden = [c for c in B.PEAK_BACKTEST_CHECKS if c.as_of is not None]
    assert golden and all(c.as_of == B.GOLDEN_AS_OF and c.applies is B._as_x7 for c in golden)
    other = {**config, "forecast": {**config["forecast"], "organic_fit_end": {"value": 2021, "status": "reviewed"}}}
    assert not B._as_x7(other)


def test_marts_build_in_dependency_order():
    names = [m.name for m in B.MARTS]
    assert names[0] == "mart_actual_summer_peaks"
    for mart in B.MARTS:
        for dep in (i for i in mart.inputs if i.startswith("mart_")):
            assert names.index(dep) < names.index(mart.name), (mart.name, dep)
    assert B.PEAK_BACKTEST.meta(pl.DataFrame(), None) == {"eras": list(B.ERAS)}
