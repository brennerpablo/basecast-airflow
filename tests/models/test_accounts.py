"""Account universe and signals (``basecast_pipelines/models/accounts.py``), on tiny synthetic frames."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import accounts as A


def test_names_normalize_to_their_distinctive_words():
    assert A.core_name("Bartlett City of") == "bartlett"
    assert A.core_name("City of Bartlett - (TX)") == "bartlett"
    assert A.core_name("Bandera Electric Coop, Inc") == "bandera"
    assert A.core_name("Garland Power & Light System") == "garland"
    assert A.core_name("Heart of Texas Electric Cooperative, Inc.") == "heart texas"
    assert A.normalize_name("Mid-South Elec Coop Assn") == "mid south electric cooperative association"
    assert A.state_tag("Acme Electric Coop, Inc - (NM)") == "NM"
    assert A.state_tag("Acme Electric Coop, Inc (OK)") == "OK"
    assert A.state_tag("Acme Electric Coop, Inc") is None


def test_type_from_name():
    assert A.infer_type_from_name("City of Tulia") == "muni"
    assert A.infer_type_from_name("Acme Elec Coop Inc") == "coop"
    assert A.infer_type_from_name("Acme Retail Energy LLC") is None


def test_name_score_is_strict_about_extra_words():
    pytest.importorskip("rapidfuzz")
    assert A.name_score("Acme Electric Cooperative, Inc.", "Acme Elec Coop, Inc") == 100
    assert A.name_score("Southwest Texas Electric Cooperative", "Southwest Rural Elec Assn Inc") < 90
    assert A.name_score("Zeta City Government", "City of Zeta - (TX)") == 100


def test_county_overlap_is_the_share_of_territory_area():
    puct = pl.DataFrame(
        {"account_id": ["1", "1", "2"], "county_fips": ["a", "b", "b"], "overlap_km2": [30.0, 70.0, 5.0]}
    )
    eia = pl.DataFrame({"utility_id": ["u", "u", "v"], "county_fips": ["a", "a", "b"]})
    out = A.county_overlap(puct, eia).sort("account_id", "utility_id")
    assert out.rows() == [("1", "u", 0.3), ("1", "v", 0.7), ("2", "v", 1.0)]


def _crosswalk_inputs():
    accounts = pl.DataFrame(
        {
            "account_id": ["10", "11", "12"],
            "name": ["Acme Electric Cooperative, Inc.", "Zeta City of", "Brandname Energy"],
            "account_type": ["coop", "muni", "muni"],
        }
    )
    eia = pl.DataFrame(
        {
            "utility_id": ["1", "2", "3", "4"],
            "utility_name": [
                "Acme Electric Coop, Inc - (NM)",
                "Acme Electric Coop, Inc - (TX)",
                "City of Zeta - (TX)",
                "City of Omega - (TX)",
            ],
            "account_type": ["coop", "coop", "muni", "muni"],
        }
    )
    overlap = pl.DataFrame(
        {"account_id": ["10", "11", "12"], "utility_id": ["2", "3", "4"], "county_overlap": [1.0, 0.9, 1.0]}
    )
    return accounts, eia, overlap


def test_match_breaks_name_ties_by_county_overlap_and_flags_low_scores():
    pytest.importorskip("rapidfuzz")
    accounts, eia, overlap = _crosswalk_inputs()
    out = {r["account_id"]: r for r in A.match_utilities(accounts, eia, overlap).iter_rows(named=True)}
    assert out["10"]["eia_utility_id"] == "2" and out["10"]["status"] == "auto"
    assert out["10"]["runner_up_score"] == 100
    assert out["11"]["eia_utility_id"] == "3" and out["11"]["status"] == "auto"
    # A brand name that shares no word with the EIA filing: review, with the county candidate proposed.
    assert out["12"]["status"] == "review"
    assert out["12"]["geo_utility_id"] == "4" and out["12"]["geo_overlap"] == 1.0


def test_match_without_county_overlap_is_never_auto():
    pytest.importorskip("rapidfuzz")
    accounts, eia, _ = _crosswalk_inputs()
    out = A.match_utilities(accounts, eia)
    assert set(out["status"]) == {"review"}


def test_build_universe_keeps_auto_and_accepted_rows():
    accounts = pl.DataFrame({"account_id": ["1", "2", "3"], "account_type": ["coop", "muni", "muni"]})
    crosswalk = pl.DataFrame(
        {
            "account_id": ["1", "2", "3"],
            "eia_utility_id": ["a", "b", "c"],
            "status": ["auto", "review", "review"],
            "decision": [None, "accept", "reject"],
        }
    )
    assert A.build_universe(accounts, crosswalk)["account_id"].to_list() == ["1", "2"]
    everyone = A.build_universe(accounts, crosswalk, intersect=False)
    assert everyone.height == 3 and everyone.filter(pl.col("account_id") == "3")["eia_utility_id"].item() is None


def test_read_crosswalk_round_trip(tmp_path):
    path = tmp_path / "cw.yaml"
    path.write_text(
        "matches:\n"
        "- {ccn_no: '1', eia_utility_id: '9', status: auto}\n"
        "- {ccn_no: '2', eia_utility_id: '8', status: review, decision: reject}\n"
    )
    out = A.read_crosswalk(path)
    assert out.rows() == [("1", "9", "auto", None), ("2", "8", "review", "reject")]


LINKS = pl.DataFrame(
    {
        "account_id": ["1", "1", "2"],
        "county_fips": ["a", "b", "b"],
        "county_share": [0.5, 0.1, 1.0],
        "overlap_km2": [10.0, 30.0, 50.0],
    }
)


def test_apportion_weights_by_county_share_and_keeps_missing_as_null():
    values = pl.DataFrame({"county_fips": ["a", "b"], "x": [100.0, None]})
    out = dict(A.apportion(values, LINKS, ["x"]).select("account_id", "x").iter_rows())
    assert out == {"1": 50.0, "2": None}


def test_population_growth_and_permits_per_1k():
    pop = pl.DataFrame(
        {"county_fips": ["a", "a", "b", "b"], "year": [2020, 2025, 2020, 2025], "value": [100.0, 120.0, 1000.0, 1000.0]}
    )
    growth = A.population_growth(pop, LINKS, start=2020, end=2025)
    g = {r["account_id"]: r for r in growth.iter_rows(named=True)}
    assert g["1"]["population"] == pytest.approx(160.0)  # 0.5 * 120 + 0.1 * 1000
    assert g["1"]["pop_growth"] == pytest.approx(160 / 150 - 1)
    assert g["2"]["pop_growth"] == pytest.approx(0.0)
    permits = pl.DataFrame({"county_fips": ["a"], "units": [16.0]})
    per = {r["account_id"]: r for r in A.permits_per_1k(permits, growth, LINKS).iter_rows(named=True)}
    assert per["1"]["permits_per_1k"] == pytest.approx(8 / 160 * 1000)
    assert per["2"]["permits_per_1k"] is None  # its only county has no permit-issuing place


def test_owner_single_family_uses_lines_3_and_4():
    housing = pl.DataFrame(
        {"county_fips": ["a"] * 4 + ["b"] * 2, "line": [1, 3, 4, 5, 1, 3], "estimate": [100, 50, 10, 5, 200, 20]}
    )
    out = {r["account_id"]: r for r in A.owner_single_family(housing, LINKS).iter_rows(named=True)}
    assert out["2"]["owner_sf_homes"] == pytest.approx(20.0)
    assert out["2"]["owner_sf_share"] == pytest.approx(0.1)
    assert out["1"]["owner_sf_share"] == pytest.approx((30 + 2) / (50 + 20))


def test_data_center_sites_counts_only_new_sites_and_zero_fills():
    sites = pl.DataFrame(
        {"county_fips": ["a", "a", "b"], "first_affil_begin_dt": [date(2025, 3, 1), date(2019, 1, 1), None]}
    )
    out = dict(A.data_center_sites(sites, LINKS, since=date(2025, 1, 1)).iter_rows())
    assert out == {"1": 0.5, "2": 0.0}


def test_zone_peak_growth_is_area_weighted_cagr():
    peaks = pl.DataFrame(
        {
            "region_id": ["N", "N", "S", "S"],
            "target_year": [2025, 2027, 2025, 2027],
            "value": [100.0, 121.0, 100.0, 100.0],
        }
    )
    zones = pl.DataFrame({"county_fips": ["a", "b"], "weather_zone": ["N", "S"]})
    out = dict(A.zone_peak_growth(peaks, zones, LINKS, start=2025, end=2027).iter_rows())
    assert out["1"] == pytest.approx(0.1 * 10 / 40)
    assert out["2"] == pytest.approx(0.0)


def test_eia_signals_skip_early_release_and_part_c_customers():
    rows = []
    for year, early, cust in ((2019, False, 100), (2024, False, 121), (2025, True, 999)):
        rows.append((year, early, "u", "A", "total", 1000.0, cust))
        rows.append((year, early, "u", "A", "residential", 600.0, cust))
    rows.append((2024, False, "u", "C", "total", 0.0, 50))
    sales = pl.DataFrame(
        rows,
        schema=["data_year", "early_release", "utility_id", "part", "sector", "sales_mwh", "customers"],
        orient="row",
    )
    out = A.eia_account_signals(sales, year=2024, base_year=2019).row(0, named=True)
    assert out["eia_customers"] == 121
    assert out["eia_res_sales_share"] == pytest.approx(0.6)
    assert out["eia_customer_cagr"] == pytest.approx(1.21 ** (1 / 5) - 1)


def test_signal_summary_flags_coverage_and_near_constant():
    df = pl.DataFrame(
        {
            "full": [float(i) for i in range(10)],
            "sparse": [1.0, 2.0] + [None] * 8,
            "flat": [0.0] * 9 + [1.0],
        }
    )
    out = {r["signal"]: r for r in A.signal_summary(df, ["full", "sparse", "flat"]).iter_rows(named=True)}
    assert out["full"]["enters"] and out["full"]["coverage"] == 1.0
    assert not out["sparse"]["enters"] and out["sparse"]["coverage"] == 0.2
    assert out["flat"]["near_constant"] and not out["flat"]["enters"]
    assert out["flat"]["zero_share"] == 0.9


def test_yoy_jumps_only_between_consecutive_years():
    series = pl.DataFrame(
        {"utility_id": ["u"] * 4, "data_year": [2019, 2020, 2022, 2023], "value": [100.0, 160.0, 400.0, 390.0]}
    )
    out = A.yoy_jumps(series, "value")
    assert out.select("prev_year", "data_year").rows() == [(2019, 2020)]


def test_redundant_pairs_uses_rank_correlation():
    df = pl.DataFrame({"a": [1.0, 2.0, 3.0, 4.0], "b": [1.0, 10.0, 100.0, 1000.0], "c": [4.0, 1.0, 3.0, 2.0]})
    out = {(r["a"], r["b"]): r for r in A.redundant_pairs(df, ["a", "b", "c"]).iter_rows(named=True)}
    assert out[("a", "b")]["spearman"] == pytest.approx(1.0) and out[("a", "b")]["redundant"]
    assert not out[("a", "c")]["redundant"]


def test_score_bins_close_the_last_bin():
    out = A.score_bins(pl.Series([100.0, 95.0, 89.9, 10.0]))
    assert dict(out.iter_rows()) == {
        "[0, 50)": 1, "[50, 70)": 0, "[70, 80)": 0, "[80, 90)": 1, "[90, 95)": 0, "[95, 100]": 2
    }
