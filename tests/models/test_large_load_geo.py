"""Tiny synthetic checks for ``basecast_pipelines.models.large_load_geo`` (no database)."""

from __future__ import annotations

import polars as pl
import pytest

from basecast_pipelines.models import large_load_geo as geo

np = pytest.importorskip("numpy")


def test_zone_code_maps_deck_labels_and_drops_the_rest():
    assert geo.zone_code("Far West") == "FWEST"
    assert geo.zone_code("  North  Central ") == "NCENT"
    assert geo.zone_code("South Central") == "SCENT"
    assert geo.zone_code("north") == "NORTH"
    assert geo.zone_code("Not Specified") is None
    assert geo.zone_code("Total") is None
    assert geo.zone_code(None) is None


def test_shares_fill_missing_zones_and_clip_negatives():
    s = geo.shares({"FWEST": 30, "NCENT": 10, "SOUTH": -5})
    assert s["FWEST"] == pytest.approx(0.75)
    assert s["SOUTH"] == 0 and s["COAST"] == 0
    assert sum(s.values()) == pytest.approx(1)
    assert sum(geo.shares({}).values()) == 0


def test_chart_zone_mw_filters_by_label_and_skips_non_zones():
    cv = pl.DataFrame({
        "document": ["d"] * 5, "page": [7] * 5,
        "status_label": ["Base load", "Studied load", "Base load", "Base load", "Base load"],
        "category": ["Far West", "Far West", "Coast", "Not Specified", "Coast"],
        "value_mw": [100.0, 50.0, 20.0, 9.0, 5.0],
    })
    assert geo.chart_zone_mw(cv, document="d", page=7, label="^base load$") == {"FWEST": 100.0, "COAST": 25.0}
    assert geo.chart_zone_mw(cv, document="d", page=7, exclude_label="^base") == {"FWEST": 50.0}


def test_lz_west_north_fraction_solves_the_straddling_zone():
    zone = {"FWEST": 10.0, "WEST": 20.0, "NORTH": 40.0, "NCENT": 50.0}
    assert geo.lz_west_north_fraction(zone, lz_west_mw=40.0) == pytest.approx(0.25)
    assert geo.lz_west_north_fraction(zone, lz_west_mw=5.0) == 0.0  # clipped
    assert geo.lz_west_north_fraction({"FWEST": 1.0}, 3.0) is None


def test_rake_to_group_hits_the_target_and_keeps_proportions():
    share = geo.shares({"FWEST": 1, "WEST": 1, "NORTH": 2, "COAST": 4, "NCENT": 2})
    member = geo.lz_west_membership(0.5)
    out = geo.rake_to_group(share, member, group_share=0.6)
    # the group (FWEST + WEST + half of NORTH, each part at its side's factor) holds exactly 0.6
    a = 0.6 / sum(share[z] * member[z] for z in geo.ZONES)
    b = 0.4 / sum(share[z] * (1 - member[z]) for z in geo.ZONES)
    assert out["FWEST"] == pytest.approx(share["FWEST"] * a)
    assert out["COAST"] / out["NCENT"] == pytest.approx(2)
    assert out["NORTH"] == pytest.approx(share["NORTH"] * (0.5 * a + 0.5 * b))
    assert sum(out.values()) == pytest.approx(1)
    assert out["FWEST"] + out["WEST"] + share["NORTH"] * 0.5 * a == pytest.approx(0.6)
    with pytest.raises(ValueError):
        geo.rake_to_group({"COAST": 1.0}, member, 0.5)


def test_tsp_zone_shares_and_allocation():
    overlap = pl.DataFrame({
        "county_fips": ["1", "2", "3"],
        "utility_name": ["Oncor Electric Delivery Company LLC"] * 2 + ["CPS Energy"],
        "overlap_km2": [30.0, 10.0, 5.0],
    })
    czl = pl.DataFrame({"county_fips": ["1", "2", "2", "3"], "weather_zone": ["FWEST", "NCENT", "EAST", "SCENT"],
                        "share": [1.0, 0.5, 0.5, 1.0]})
    ts = geo.tsp_zone_shares(overlap, czl)
    oncor = dict(zip(*ts.filter(pl.col("tsp") == "Oncor").select("weather_zone", "share").to_dict(as_series=False).values(),
                     strict=True))
    assert oncor["FWEST"] == pytest.approx(0.75) and oncor["NCENT"] == pytest.approx(0.125)
    assert "AEP" not in ts["tsp"].to_list()  # no service area in the fixture
    req = pl.DataFrame({"name": ["Oncor", "CPS", "Brazos"], "year": [2030] * 3, "mw": [100.0, 10.0, 50.0]})
    a = geo.allocate_tsp_requests(req, ts)
    assert a["mw"].sum() == pytest.approx(110)  # Brazos dropped without a fallback
    b = geo.allocate_tsp_requests(req, ts, fallback={"COAST": 1.0})
    assert b.filter(~pl.col("mapped"))["mw"].sum() == pytest.approx(50)


def test_county_zone_long_unpivots_share_columns():
    czw = pl.DataFrame({"county_fips": ["1"], "share_fwest": [0.7], "share_west": [0.3], "share_outside": [0.0],
                        "share_coast": [None]})
    long = geo.county_zone_long(czw)
    assert dict(zip(long["weather_zone"].to_list(), long["share"].to_list(), strict=True)) == {"FWEST": 0.7, "WEST": 0.3}


def test_split_large_load_sends_the_increment_to_the_pipeline_split():
    draws = np.array([[80.0, 150.0], [100.0, 300.0]])
    out = geo.split_large_load(draws, stock_ll=100.0, stock_share={"FWEST": 1.0}, increment_share={"NCENT": 1.0})
    assert out["FWEST"].tolist() == [[80.0, 100.0], [100.0, 100.0]]
    assert out["NCENT"].tolist() == [[0.0, 50.0], [0.0, 200.0]]
    assert sum(out[z] for z in geo.ZONES) == pytest.approx(draws)


def test_residual_shares_and_compare():
    s, resid = geo.residual_shares({"FWEST": 300, "WEST": 50, "COAST": 100}, {"FWEST": 100, "WEST": 150})
    assert resid["WEST"] == -100
    assert s["FWEST"] == pytest.approx(200 / 300) and s["WEST"] == 0
    tab = geo.compare_with_excess({"FWEST": 100}, {"FWEST": 300})
    assert tab.filter(pl.col("weather_zone") == "FWEST")["excess_minus_allocated_mw"].item() == 200
    assert geo.spearman({"FWEST": 3, "WEST": 2, "COAST": 1}, {"FWEST": 30, "WEST": 20, "COAST": 10},
                        zones=("FWEST", "WEST", "COAST")) == pytest.approx(1)


def test_county_pressure_spreads_by_signals_within_zone():
    signals = pl.DataFrame({"county_fips": ["1", "2", "3"], "n_signals": [3, 1, 0]})
    czl = pl.DataFrame({"county_fips": ["1", "2", "3"], "weather_zone": ["FWEST", "FWEST", "COAST"], "share": [1.0] * 3})
    cp = geo.county_pressure({"stock_mw": {"FWEST": 400.0, "COAST": 50.0}}, signals, czl)
    got = dict(zip(cp["county_fips"].to_list(), cp["stock_mw_alloc"].to_list(), strict=True))
    assert got == {"1": pytest.approx(300.0), "2": pytest.approx(100.0)}  # COAST has no signal: left at zone level


def test_share_spread_reports_band():
    t = geo.share_spread({"a": {"FWEST": 0.6, "WEST": 0.4}, "b": {"FWEST": 0.2, "WEST": 0.8}}, "a")
    row = t.filter(pl.col("weather_zone") == "FWEST").row(0, named=True)
    assert (row["central"], row["low"], row["high"]) == (0.6, 0.2, 0.6)
