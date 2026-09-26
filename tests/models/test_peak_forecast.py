"""Tiny synthetic checks for ``basecast_pipelines.models.peak_forecast`` (no database)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import peak_forecast as pf


def test_month_index_and_interpolate():
    assert pf.month_index("2025-01") == 2025 * 12
    # X16 #3: month-end convention. dec_index was Dec 1 and the summer point Jul 16
    assert pf.month_index(date(2025, 12, 31)) < pf.dec_index(2025) == pf.month_index(date(2026, 1, 1))
    assert pf.month_end_index("2025-12") == pf.dec_index(2025)
    assert pf.summer_index(2025) == pf.month_index(date(2025, 8, 1))  # end of July
    pts = [(0.0, 10.0), (10.0, 20.0)]
    assert pf.interpolate(pts, -1) is None
    assert pf.interpolate(pts, 5) == pytest.approx(15)
    assert pf.interpolate(pts, 99) == 20  # carried flat after the last point


def _vintages() -> pl.DataFrame:
    return pl.DataFrame({
        "vintage": [date(2023, 5, 31), date(2023, 12, 10), date(2024, 1, 20), date(2025, 1, 20)],
        "report_date": [date(2023, 5, 31), date(2023, 12, 10), date(2024, 4, 1), date(2025, 1, 25)],
        "a2e_stock_mw": [2000.0, 3000.0, 4000.0, 6000.0],
    })


def test_realized_year_end_uses_the_first_deck_after_the_year_and_its_publication_date():
    r = pf.realized_year_end(_vintages())
    row23 = r.filter(pl.col("target_year") == 2023).row(0, named=True)
    assert row23["realized_a2e_mw"] == 4000.0  # the Jan 2024 deck, not the Dec 2023 one
    assert row23["known_from"] == date(2024, 4, 1)
    assert r.filter(pl.col("target_year") == 2024)["realized_a2e_mw"].item() == 6000.0


def _bars() -> pl.DataFrame:
    return pl.DataFrame({
        "vintage": [date(2023, 5, 31)] * 2 + [date(2023, 12, 10)],
        "report_date": [date(2023, 5, 31)] * 2 + [date(2023, 12, 10)],
        "year": [2023, 2024, 2023],
        "total_mw": [12000.0, 22000.0, 5000.0],
        "no_studies_submitted": [2000.0, 2000.0, 0.0],
        "base_a2e_mw": [2000.0, 2000.0, 3000.0],
    })


def test_incremental_ratios_respect_the_as_of_date_and_the_horizon():
    realized = pf.realized_year_end(_vintages())
    # before the Dec 2023 stock was published nothing is estimable
    assert pf.incremental_ratios(_bars(), realized, date(2024, 3, 31)).is_empty()
    r = pf.incremental_ratios(_bars(), realized, date(2024, 4, 1))
    # May 2023 -> 2023 (7 months): (4000 - 2000) / (12000 - 2000); Dec 2023 deck is < 6 months ahead: dropped
    assert r.height == 1
    assert r["ratio"].item() == pytest.approx(0.2)
    firm = pf.incremental_ratios(_bars(), realized, date(2024, 4, 1), firm=True)
    assert firm["ratio"].item() == pytest.approx((4000 - 2000) / (10000 - 2000))
    both = pf.incremental_ratios(_bars(), realized, date(2025, 2, 1))
    assert sorted(both["target_year"].to_list()) == [2023, 2024]


def test_promised_by_year_is_cumulative_past_the_last_bar():
    bars = pl.DataFrame({"year": [2026, 2027], "total_mw": [10.0, 30.0], "no_studies_submitted": [1.0, 5.0]})
    assert pf.promised_by_year(bars, [2026, 2027, 2029]) == {2026: 10.0, 2027: 30.0, 2029: 30.0}
    assert pf.promised_by_year(bars, [2027], firm=True) == {2027: 25.0}


def test_a2e_path_runs_from_the_vintage_stock_through_projected_decembers():
    promised = {2026: 20_000.0, 2027: 40_000.0}
    path = pf.a2e_path(10_000.0, date(2026, 6, 1), promised, 0.2, [2027])
    dec26 = 10_000 + 0.2 * 10_000
    dec27 = 10_000 + 0.2 * 30_000
    assert path[2027] == pytest.approx(dec26 + (dec27 - dec26) * 7 / 12)  # end of Dec 2026 -> end of Jul 2027
    # a promise below the current stock adds nothing
    assert pf.a2e_path(10_000.0, date(2026, 6, 1), {2026: 5_000.0, 2027: 5_000.0}, 0.5, [2027])[2027] == 10_000


def test_unattributed_subtracts_the_observed_large_load():
    points = pl.DataFrame({"month_idx": [pf.month_end_index("2023-01"), pf.month_end_index("2024-01")],
                           "a2e_mw": [1000.0, 3400.0]})
    u = pf.unattributed({2021: 500.0, 2023: 5000.0}, points, {2021: "2021-08", 2023: "2023-07"}, 0.5)
    assert u["year"].to_list() == [2023]  # 2021 is before the post-break window
    a2e = 1000 + 2400 * 6 / 12
    assert u["u_mw"].item() == pytest.approx(5000 - 0.5 * a2e)


def _panel(noise: bool = False):
    np = pytest.importorskip("numpy")
    years = list(range(2003, 2020))
    x = [30 + (y % 4) * 0.5 for y in years]
    rng = np.random.default_rng(0)
    peak = [60_000 + 900 * (y - 2014) + 1000 * t + (rng.normal(0, 300) if noise else 0)
            for y, t in zip(years, x, strict=True)]
    return pl.DataFrame({"year": years, "peak_mw": peak, "t": x})


def test_fit_organic_recovers_the_coefficients_and_draws_center_on_the_mean():
    np = pytest.importorskip("numpy")
    fit = pf.fit_organic(_panel(), "t")
    assert fit.beta == pytest.approx([60_000, 900, 1000], rel=1e-6)
    noisy = pf.fit_organic(_panel(noise=True), "t")
    draws = pf.organic_draws(noisy, [2027], 20_000, np.random.default_rng(1), resid_sd=500)
    assert np.median(draws) == pytest.approx(noisy.mean(2027, noisy.x_p50), rel=0.005)
    panel = _panel()
    errs = pf.rolling_origin_errors(panel, "t", 2012, 2019)
    # noiseless data: the only error left is the weather gap, 1000 MW/°C x (training median - that summer's value)
    for row in errs.iter_rows(named=True):
        train = panel.filter(pl.col("year") < row["year"])["t"].to_numpy()
        x_y = panel.filter(pl.col("year") == row["year"])["t"].item()
        assert row["error_mw"] == pytest.approx(1000 * (np.median(train) - x_y), abs=1e-6)


def test_simulate_adds_the_layers_and_summarize_and_score():
    np = pytest.importorskip("numpy")
    fit = pf.fit_organic(_panel(noise=True), "t")
    draws = pf.simulate(fit, [2026, 2027], base=10_000.0, vintage=date(2026, 3, 1),
                        promised={2026: 20_000.0, 2027: 40_000.0}, ratios=[0.1, 0.2], factor=0.5,
                        u_values=[1000.0, 2000.0], resid_sd=500, n=2_000)
    assert np.allclose(draws["total"], draws["organic"] + draws["large_load"] + draws["unattributed"])
    assert (draws["large_load"][:, 1] >= draws["large_load"][:, 0]).all()
    no_ll = pf.simulate(fit, [2026], base=None, vintage=None, promised=None, ratios=None, factor=None,
                        u_values=None, n=100)
    assert (no_ll["large_load"] == 0).all() and (no_ll["unattributed"] == 0).all()
    summ = pf.summarize(draws, [2026, 2027])
    assert set(summ["layer"]) == {"organic", "large_load", "unattributed", "total"}
    tot = summ.filter(pl.col("layer") == "total")
    scored = pf.score(tot, {2026: tot["p50_mw"][0]})
    assert scored.height == 1 and scored["error_pct"].item() == pytest.approx(0) and scored["in_p10_p90"].item()


def test_official_asof_takes_the_latest_vintage_per_product():
    base = pl.DataFrame({
        "product": ["LTLF", "LTLF", "CDR", "CDR"],
        "vintage": ["LTLF 2024", "LTLF 2025", "CDR May", "CDR Dec"],
        "vintage_date": [date(2024, 7, 18), date(2025, 4, 8), date(2025, 5, 1), date(2025, 12, 1)],
        "target_year": [2026] * 4,
        "forecast_mw": [106_000.0, 94_650.0, 95_400.0, 95_400.0],
    })
    got = pf.official_asof(base, date(2025, 6, 1))
    assert sorted(got["vintage"].to_list()) == ["CDR May", "LTLF 2025"]
    later = pf.official_asof(base, date(2025, 6, 1), after_days=200)
    assert later.filter(pl.col("after_as_of"))["vintage"].to_list() == ["CDR Dec"]
