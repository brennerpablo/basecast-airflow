"""County acquisition priority (``basecast_pipelines/models/acquisition_zones.py``), on tiny synthetic frames."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import acquisition_zones as Z


def _overlap() -> pl.DataFrame:
    rows = [
        # county A: 60% IOU in ERCOT, 40% one co-op -> retail_direct
        ("A", "iou1", "iou", True, 0.6),
        ("A", "c1", "coop", True, 0.4),
        # county B: co-op + muni in ERCOT, IOU outside ERCOT -> partnership, addressable 0.6
        ("B", "c1", "coop", True, 0.3),
        ("B", "m1", "muni", True, 0.3),
        ("B", "iou2", "iou", False, 0.4),
        # county C: dual certified, sums to 1.25 -> normalized 0.4 / 0.4 / 0.2 -> mixed
        ("C", "iou1", "iou", True, 0.5),
        ("C", "c2", "coop", True, 0.5),
        ("C", "c3", "coop", False, 0.25),
        ("C", "c4", "coop", True, 0.005),  # under PARTNER_MIN_SHARE: not an account of C
    ]
    return pl.DataFrame(
        rows, schema=["county_fips", "ccn_no", "utility_type", "in_ercot", "county_share"], orient="row"
    )


def test_channel_split_shares_labels_and_partners():
    out = {r["county_fips"]: r for r in Z.channel_split(_overlap()).iter_rows(named=True)}
    a, b, c = out["A"], out["B"], out["C"]
    assert a["channel"] == "retail_direct" and a["retail_share"] == pytest.approx(0.6)
    assert a["addressable_share"] == pytest.approx(1.0) and a["n_partners"] == 1
    assert b["channel"] == "partnership" and b["partner_share"] == pytest.approx(0.6)
    assert b["outside_share"] == pytest.approx(0.4) and b["addressable_share"] == pytest.approx(0.6)
    assert b["partner_type"] == "coop"  # tie goes to coop
    assert b["n_partners"] == 2 and b["top_partner_share"] == pytest.approx(0.5)
    total = 0.5 + 0.5 + 0.25 + 0.005
    assert c["channel"] == "mixed"
    assert c["retail_share"] == pytest.approx(0.5 / total) and c["outside_share"] == pytest.approx(0.25 / total)
    assert c["n_partners"] == 1 and c["top_partner_share"] == pytest.approx(1.0)
    for r in out.values():
        assert r["retail_share"] + r["coop_share"] + r["muni_share"] + r["outside_share"] == pytest.approx(1.0)


def test_county_accounts_lists_ercot_coops_and_munis_largest_first():
    out = Z.county_accounts(_overlap())
    assert out.filter(pl.col("county_fips") == "B")["account_id"].to_list() == ["c1", "m1"]
    assert out.filter(pl.col("county_fips") == "C")["account_id"].to_list() == ["c2"]  # c3 outside, c4 < 1%
    assert "iou1" not in out["account_id"].to_list()


def test_zone_values_reach_counties_by_zone_mix():
    czl = pl.DataFrame(
        {"county_fips": ["A", "B", "B", "C"], "weather_zone": ["COAST", "COAST", "EAST", "NORTH"],
         "share": [1.0, 0.25, 0.75, 1.0]}
    )
    out = dict(Z.zone_to_county({"COAST": 1.0, "EAST": 3.0}, czl, "v").iter_rows())
    assert out == {"A": pytest.approx(1.0), "B": pytest.approx(2.5)}  # C's zone has no value: no row
    wz = Z.weather_zone_values({"LZ_WEST": 100.0, "LZ_NORTH": 50.0})
    assert wz["NORTH"] == pytest.approx(0.72 * 100 + 0.28 * 50)
    assert wz["FWEST"] == 100.0 and "COAST" not in wz


def test_daily_spread_is_top_minus_bottom_two_hours():
    prices = pl.DataFrame(
        {"settlement_point": ["LZ_X"] * 16, "day": [date(2025, 7, 1)] * 16,
         "price_usd_mwh": [10.0] * 8 + [110.0] * 8}
    )
    out = Z.daily_spread(prices)
    assert out["spread_usd_mwh"].to_list() == [100.0]
    assert Z.daily_spread(prices.head(10)).height == 0  # fewer than 2 × 8 intervals
    assert Z.cagr(100.0, 121.0, 2) == pytest.approx(0.1) and Z.cagr(0.0, 1.0, 2) is None


def _signals() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "county_fips": ["A", "B", "C"],
            "m": [3.0, 1.0, 2.0],
            "g": [1.0, 3.0, 2.0],
            "addressable_share": [1.0, 1.0, 0.5],
            "retail_share": [1.0, 0.1, 0.3],
            "partner_share": [0.0, 0.9, 0.7],
        }
    )


def test_tilt_lets_grid_cut_but_not_lift_a_county_with_no_homes():
    s = Z.score_counties(_signals(), {"m": 1.0}, {"g": 1.0}, tilt=0.4)
    by = {r["county_fips"]: r for r in s.iter_rows(named=True)}
    # percentile ranks: m A=1, C=.5, B=0; g B=1, C=.5, A=0
    assert by["A"]["priority"] == pytest.approx(1.0 * 0.6)
    assert by["B"]["priority"] == pytest.approx(0.0)
    assert by["C"]["priority"] == pytest.approx(0.5 * 0.8)
    assert s["county_fips"].to_list() == ["A", "C", "B"] and s["rank"].to_list() == [1, 2, 3]
    add = {r["county_fips"]: r["priority"] for r in
           Z.score_counties(_signals(), {"m": 1.0}, {"g": 1.0}, tilt=0.4, combine="additive").iter_rows(named=True)}
    assert add["B"] == pytest.approx(0.4)  # the additive mean lifts the no-market county
    gated = {r["county_fips"]: r["priority"] for r in
             Z.score_counties(_signals(), {"m": 1.0}, {"g": 1.0}, gate="addressable_share").iter_rows(named=True)}
    assert gated["C"] == pytest.approx(0.5 * 0.8 * 0.5)
    with pytest.raises(ValueError):
        Z.score_counties(_signals(), {"m": 1.0}, {"g": 1.0}, combine="max")


def test_missing_signal_renormalizes_within_its_block():
    sig = _signals().with_columns(pl.Series("m2", [None, 2.0, 1.0]))
    s = Z.score_counties(sig, {"m": 0.5, "m2": 0.5}, {"g": 1.0}, tilt=0.0)
    by = {r["county_fips"]: r["market_score"] for r in s.iter_rows(named=True)}
    assert by["A"] == pytest.approx(1.0)  # only m, renormalized
    assert by["B"] == pytest.approx((0.0 + 1.0) / 2)


def test_channel_lists_by_eligibility_or_share():
    s = Z.score_counties(_signals(), {"m": 1.0}, {"g": 1.0}, tilt=0.4)
    el = {r["county_fips"]: r for r in Z.channel_priority(s).iter_rows(named=True)}
    assert el["A"]["retail_rank"] == 1 and el["C"]["retail_rank"] == 2 and el["B"]["retail_rank"] is None
    assert el["A"]["partner_rank"] is None and el["C"]["partner_rank"] == 1
    sh = {r["county_fips"]: r for r in Z.channel_priority(s, mode="share").iter_rows(named=True)}
    assert sh["C"]["retail_priority"] == pytest.approx(0.4 * 0.3) and sh["B"]["retail_rank"] == 3


def test_legend_classes_and_drivers():
    breaks, cls = Z.legend_classes(pl.Series([0.1, 0.2, 0.3, 0.4, 0.5]), n=5)
    assert len(breaks) == 4 and cls.to_list() == [1, 2, 3, 4, 5]
    s = Z.score_counties(_signals(), {"m": 1.0}, {"g": 1.0}, tilt=0.4)
    d = {r["county_fips"]: r for r in Z.drivers(s, {"m": 1.0, "g": 0.25}).iter_rows(named=True)}
    assert d["A"]["drivers"] == "m" and d["A"]["drags"] == "g"
    assert d["C"]["drivers"] is None  # both at the median


def test_rank_stability_covers_every_variant():
    sig = _signals().with_columns(pl.Series("m2", [2.0, 1.0, 3.0]), pl.Series("g2", [1.0, 2.0, 3.0]))
    market, grid = {"m": 0.6, "m2": 0.4}, {"g": 0.5, "g2": 0.5}
    tab = Z.rank_stability(sig, market=market, grid=grid, tilt=0.4, top=2)
    # 2 blocks × (2 signals × 2 signs + 2 leave-outs) + 4 tilt settings
    assert tab.height == 2 * (2 * 2 + 2) + 4
    assert {"spearman", "all_top2_kept", "retail_top2_kept", "partner_top2_kept", "max_move"} <= set(tab.columns)
    same = Z.compare_rankings(Z.ranks(sig, market=market, grid=grid), Z.ranks(sig, market=market, grid=grid), top=2)
    assert same["spearman"] == pytest.approx(1.0) and same["max_move"] == 0


def test_default_weights_sum_to_one_per_block():
    assert sum(Z.MARKET.values()) == pytest.approx(1.0)
    assert sum(Z.GRID.values()) == pytest.approx(1.0)
    assert 0 < Z.GRID_TILT < 1
