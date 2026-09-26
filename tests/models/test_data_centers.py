"""Q4 logic on tiny synthetic frames (no database): site selection, territory types, metro flags."""

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import data_centers as dc

SITES = pl.DataFrame(
    {
        "ref_num_txt": ["RN1", "RN2", "RN3", "RN4"],
        "reg_ent_name": ["OLD DC", "NEW DC", "UNDATED DC", "RURAL DC"],
        "county_name": ["DALLAS", "Hill", "BEXAR", "HASKELL"],
        "county_fips": ["48113", "48217", "48029", "48207"],
        "city": ["GARLAND", "HUBBARD", None, None],
        "first_affil_begin_dt": [date(2021, 12, 15), date(2025, 5, 29), None, date(2025, 1, 1)],
        "has_undated_affiliation": [True, False, True, True],
    }
)

# Hill: co-op 60 km² (share 0.6) + IOU 50 km² (0.5) overlapping → sum 1.1. Haskell: co-op only.
# Dallas: IOU 80, muni Garland 10, co-op 10. Travis: a co-op/IOU tie on area.
OVERLAP = pl.DataFrame(
    {
        "county_fips": ["48217", "48217", "48207", "48113", "48113", "48113", "48453", "48453"],
        "utility_name": ["A EC", "B IOU", "C EC", "B IOU", "Garland Power & Light", "D EC", "E EC", "F IOU"],
        "utility_type": ["coop", "iou", "coop", "iou", "muni", "coop", "coop", "iou"],
        "overlap_km2": [60.0, 50.0, 100.0, 80.0, 10.0, 10.0, 5.0, 5.0],
        "county_share": [0.6, 0.5, 1.0, 0.8, 0.1, 0.1, 0.5, 0.5],
    }
)


def test_select_new_sites_keeps_dated_sites_since_2025():
    new = dc.select_new_sites(SITES)
    assert new["ref_num_txt"].to_list() == ["RN4", "RN2"]  # 2025-01-01 is included; sorted by date
    assert dc.fully_undated_sites(SITES)["ref_num_txt"].to_list() == ["RN3"]


def test_county_territory_types_largest_area_and_normalized_weights():
    types = dc.county_territory_types(OVERLAP).to_dicts()
    by = {r["county_fips"]: r for r in types}
    hill = by["48217"]
    assert hill["largest_type"] == "coop"
    assert hill["coop_w"] == pytest.approx(0.6 / 1.1)
    assert hill["iou_w"] == pytest.approx(0.5 / 1.1)
    assert hill["muni_w"] == 0
    assert hill["share_sum"] == pytest.approx(1.1)
    assert hill["coop_raw"] == pytest.approx(0.6)
    assert by["48113"]["largest_type"] == "iou"
    assert by["48453"]["largest_type"] == "coop"  # tie on area: first in TYPES
    for r in types:
        assert r["coop_w"] + r["muni_w"] + r["iou_w"] == pytest.approx(1.0)


def test_type_summary_counts_and_weights_with_an_unclassified_county():
    sites = pl.DataFrame({"ref_num_txt": ["a", "b", "c"], "county_fips": ["48217", "48113", "48999"]})
    classified = dc.classify_sites(sites, dc.county_territory_types(OVERLAP))
    summary = {r["type"]: r for r in dc.type_summary(classified).to_dicts()}
    assert summary["coop"]["sites_largest_area"] == 1
    assert summary["iou"]["sites_largest_area"] == 1
    assert summary["unclassified"]["sites_largest_area"] == 1
    assert summary["coop"]["sites_weighted"] == pytest.approx(0.6 / 1.1 + 0.1)
    assert summary["coop"]["share_weighted"] == pytest.approx((0.6 / 1.1 + 0.1) / 3)
    assert summary["muni"]["sites_weighted"] == pytest.approx(0.1)


def test_flag_metro_legacy_list_and_density_threshold():
    fips = ["48113", "48217", "48207"]
    population = pl.DataFrame({"county_fips": fips, "population": [2_000_000.0, 40_000.0, 5_000.0]})
    counties = pl.DataFrame({"county_fips": fips, "aland_m2": [2_000e6, 2_500e6, 2_300e6]})
    density = dc.county_density(population, counties)
    assert density.filter(pl.col("county_fips") == "48113")["density_per_km2"][0] == pytest.approx(1000.0)
    flagged = dc.flag_metro(SITES, density, threshold=100.0)
    by = {r["ref_num_txt"]: r for r in flagged.to_dicts()}
    assert by["RN1"]["metro_legacy"] and by["RN1"]["metro_density"]
    assert not by["RN2"]["metro_legacy"] and not by["RN2"]["metro_density"]
    assert by["RN3"]["metro_legacy"] and by["RN3"]["metro_density"] is None  # no density row
    low = dc.flag_metro(SITES, density, threshold=10.0)
    assert low.filter(pl.col("ref_num_txt") == "RN2")["metro_density"][0] is True  # Hill: 16/km² ≥ 10


def test_city_muni_match_by_name_within_the_county():
    out = dc.city_muni_match(SITES, OVERLAP)
    by = {r["ref_num_txt"]: r["city_muni"] for r in out.to_dicts()}
    assert by == {"RN1": "Garland Power & Light", "RN2": None, "RN3": None, "RN4": None}
    assert out.height == SITES.height


def test_decide_needs_both_coop_share_and_a_non_metro_majority():
    assert dc.decide(0.60, 0.51) == "in video"
    assert dc.decide(0.59, 0.90) == "signal only"
    assert dc.decide(0.80, 0.50) == "signal only"
