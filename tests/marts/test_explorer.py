"""The Explorer marts' pure parts (``basecast_pipelines/marts/explorer.py``) on tiny synthetic frames (no database)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import explorer as E
from basecast_pipelines.models import acquisition_zones as Z

REPORT = date(2026, 8, 1)
HORIZONS = {"2027": 16.0, "2028": 28.0}
CAVEATS = {"machine_read_unverified", "preliminary_actuals", "weights_pending_review", "band_uncalibrated",
           "beyond_backtested_window", "allocated_statewide", "by_county_not_point", "by_area_not_homes",
           "requests_not_forecasts", "policy_pause_2026", "optimistic_weather", "simulated"}


def _scored() -> pl.DataFrame:
    """Four projects in two counties, in ``AdjustedQueue.scored``'s columns."""
    return pl.DataFrame(
        {
            "inr": ["P3", "P1", "P2", "P4"],
            "project_name": ["Gas big", "Solar", "Storage", "Gas small"],
            "county_fips": ["48001", "48001", "48003", "48003"],
            "county_name": ["Anderson", "Anderson", "Andrews", "Andrews"],
            "weather_zone": ["EAST", "EAST", "FWEST", "FWEST"],
            "cdr_reporting_zone": ["NORTH", "NORTH", "WEST", "SOUTH"],
            "fuel_type": ["Gas", "Solar", "Battery", "Gas"],
            "stratum": ["gas+other", "solar", "storage", "gas+other"],
            "stage": ["ia_signed", "entry", "entry", "ia_signed"],
            "elapsed": [3.0, 10.0, 20.0, 5.0],
            "capacity_mw": [600.0, 100.0, 200.0, 50.0],
            "projected_cod": [date(2027, 6, 1), None, date(2028, 1, 1), None],
            "curve": ["gas+other", "solar", "all", "gas+other"],
            "p_2027": [0.5, 0.1, 0.2, 0.4],
            "p_2028": [0.8, 0.3, 0.4, 0.6],
            "mw_2027": [300.0, 10.0, 40.0, 20.0],
            "mw_2028": [480.0, 30.0, 80.0, 30.0],
            "clamped_2027": [False, True, False, False],
            "clamped_2028": [False, True, False, False],
        }
    )


def _stage_dates() -> pl.DataFrame:
    return pl.DataFrame({"inr": ["P1", "P2", "P3", "P4"],
                         "stage_date": [date(2025, 11, 1), date(2025, 1, 1), date(2026, 6, 1), date(2026, 4, 1)]})


def _projects() -> pl.DataFrame:
    return E.project_rows(_scored(), _stage_dates(), REPORT, HORIZONS)


def test_project_rows_use_the_contract_names():
    p = _projects()
    by = {r["inr"]: r for r in p.iter_rows(named=True)}
    assert p["inr"].to_list() == ["P1", "P2", "P3", "P4"]
    assert set(p["as_of_month"]) == {REPORT}
    assert by["P3"]["stratum"] == "gas_other" and by["P3"]["stage"] == "ia" and by["P1"]["stage"] == "entry"
    assert by["P2"]["curve"] == "pooled" and by["P1"]["curve"] == "own" and by["P3"]["curve"] == "own"
    assert by["P1"]["p_cod_2028"] == 0.3 and by["P1"]["elapsed_months"] == 10.0 and by["P1"]["clamped_2027"]
    assert by["P2"]["stage_date"] == date(2025, 1, 1)
    assert [by[i]["large_gas"] for i in ("P1", "P2", "P3", "P4")] == [False, False, True, False]
    assert {"p_2027", "elapsed"}.isdisjoint(p.columns)


def test_project_rows_reject_a_stage_without_a_contract_name():
    with pytest.raises(ValueError, match="synchronized"):
        E.project_rows(_scored().with_columns(pl.lit("synchronized").alias("stage")), _stage_dates(), REPORT,
                       HORIZONS)


def test_county_rows_sum_strata_and_all():
    c = E.county_rows(_projects(), HORIZONS)
    by = {(r["county_fips"], r["stratum"]): r for r in c.iter_rows(named=True)}
    anderson = by[("48001", "all")]
    assert anderson["projects"] == 2 and anderson["raw_mw"] == 700.0 and anderson["adj_mw_2028"] == 510.0
    assert anderson["projects_ia"] == 1 and anderson["raw_mw_ia"] == 600.0
    assert anderson["ratio_2027"] == pytest.approx(310.0 / 700.0)
    assert anderson["large_gas_mw_2028"] == 480.0 and by[("48001", "gas_other")]["large_gas_mw_2028"] == 480.0
    assert by[("48001", "solar")]["large_gas_mw_2028"] is None
    assert by[("48003", "all")]["large_gas_mw_2028"] == 0.0  # gas, but under 500 MW
    assert by[("48001", "solar")]["projects_ia"] == 0 and by[("48001", "solar")]["raw_mw_ia"] == 0.0
    assert anderson["county_name"] == "Anderson" and anderson["weather_zone"] == "EAST"
    # modal CDR zone of the county, ties alphabetical, on every stratum row
    assert {by[("48003", s)]["cdr_reporting_zone"] for s in ("all", "storage", "gas_other")} == {"SOUTH"}
    assert c.height == 2 + 4  # two "all" rows, four county × stratum rows


def test_county_rows_rank_within_the_stratum():
    c = E.county_rows(_projects(), HORIZONS)
    alls = {r["county_fips"]: r for r in c.filter(pl.col("stratum") == "all").iter_rows(named=True)}
    assert (alls["48001"]["rank_raw"], alls["48003"]["rank_raw"]) == (1, 2)
    assert (alls["48001"]["rank_adj"], alls["48003"]["rank_adj"]) == (1, 2)
    gas = {r["county_fips"]: r["rank_raw"] for r in c.filter(pl.col("stratum") == "gas_other").iter_rows(named=True)}
    assert gas == {"48001": 1, "48003": 2}
    assert c.filter(pl.col("stratum") == "solar")["rank_raw"].to_list() == [1]
    with pytest.raises(ValueError, match="2028"):
        E.county_rows(_projects(), {"2027": 16.0})


def _flagged() -> pl.DataFrame:
    """Q4's frame after flag_metro and city_muni_match, three sites."""
    return pl.DataFrame(
        {
            "ref_num_txt": ["RN2", "RN1", "RN3"],
            "reg_ent_name": ["B CAMPUS", "A DATA CENTER", "C SITE"],
            "county_name": ["BELL", "DALLAS", "HARDIN"],
            "county_fips": ["48027", "48113", "48199"],
            "city": [None, "DALLAS", None],
            "city_muni": [None, None, None],
            "first_affil_begin_dt": [date(2026, 1, 15), date(2025, 11, 25), date(2025, 6, 20)],
            "matched_by_name": [True, False, False],
            "matched_by_naics": [False, True, True],
            "has_undated_affiliation": [False, False, True],
            "largest_type": ["iou", "iou", "iou"],
            "coop_w": [0.42, 0.05, 0.40],
            "muni_w": [0.0, 0.0, 0.0],
            "iou_w": [0.58, 0.95, 0.60],
            "dominant_iso_rto": ["ERCOT", "ERCOT,SPP", "MISO"],
            "density_per_km2": [147.0, 1177.0, 25.0],
            "metro_legacy": [False, True, False],
            "metro_density": [True, True, False],
        }
    )


def _counties() -> pl.DataFrame:
    return pl.DataFrame({"county_fips": ["48027", "48113"], "county": ["Bell", "Dallas"]})


def test_site_rows_rename_mark_ercot_and_follow_the_switches():
    s = E.site_rows(_flagged(), _counties(), metro_rule="county_list_13", include_naics_only=True)
    assert s["tceq_rn"].to_list() == ["RN3", "RN1", "RN2"]  # by first permit date
    by = {r["tceq_rn"]: r for r in s.iter_rows(named=True)}
    assert by["RN2"]["county_name"] == "Bell" and by["RN3"]["county_name"] == "HARDIN"  # tx_counties name, else TCEQ's
    assert [by[k]["matched_by"] for k in ("RN1", "RN2", "RN3")] == ["naics", "name", "naics"]
    assert [by[k]["iso_class"] for k in ("RN1", "RN2", "RN3")] == ["mixed", "ercot", "outside"]
    assert [by[k]["in_ercot"] for k in ("RN1", "RN2", "RN3")] == [True, True, False]
    assert [by[k]["metro"] for k in ("RN1", "RN2", "RN3")] == [True, False, False]
    assert by["RN2"]["coop_share_w"] == 0.42
    dens = E.site_rows(_flagged(), _counties(), metro_rule="density_100", include_naics_only=True)
    assert dict(zip(dens["tceq_rn"], dens["metro"], strict=True)) == {"RN1": True, "RN2": True, "RN3": False}
    named = E.site_rows(_flagged(), _counties(), metro_rule="county_list_13", include_naics_only=False)
    assert named["tceq_rn"].to_list() == ["RN2"]
    with pytest.raises(ValueError):
        E.site_rows(_flagged(), _counties(), metro_rule="msa", include_naics_only=True)


def test_iso_class_of_missing_county_is_null():
    out = pl.DataFrame({"d": ["ERCOT", "ERCOT,MISO,SPP", "WECC", None]}).select(E.iso_class(pl.col("d")))
    assert out.to_series().to_list() == ["ercot", "mixed", "outside", None]


def test_newest_by_year_keeps_the_last_file_of_each_year():
    files = [SimpleNamespace(name=n) for n in (
        "rpt.1.DAMLZHBSPP_2024.zip", "rpt.2.RTMLZHBSPP_2024.zip", "rpt.3.RTMLZHBSPP_2025.zip",
        "rpt.4.RTMLZHBSPP_2025.zip", "rpt.5.RTMLZHBSPP_2026.zip")]
    out = E.newest_by_year(files, [2024, 2025])
    assert {y: f.name for y, f in out.items()} == {2024: "rpt.2.RTMLZHBSPP_2024.zip", 2025: "rpt.4.RTMLZHBSPP_2025.zip"}
    with pytest.raises(FileNotFoundError, match="2023"):
        E.newest_by_year(files, [2023, 2024])


def test_zone_inputs_mean_excess_and_peak_cagr():
    excess = pl.DataFrame({"weather_zone": ["A", "A", "A", "B", "ERCOT"], "year": [2022, 2023, 2024, 2023, 2023],
                           "excess_pct": [100.0, 10.0, 20.0, 5.0, 1.0]})
    assert E.mean_excess(excess, [2023, 2024], ["A", "B"]) == {"A": 15.0, "B": 5.0}
    peaks = pl.DataFrame({"weather_zone": ["A", "A", "B", "B"], "year": [2021, 2026, 2021, 2026],
                          "p50": [100.0, 200.0, 50.0, 50.0]})
    cagr = E.peak_cagr(peaks, 2021, 2026, ["A", "B"])
    assert cagr["A"] == pytest.approx(2 ** (1 / 5) - 1) and cagr["B"] == pytest.approx(0.0)
    with pytest.raises(KeyError):
        E.peak_cagr(peaks, 2020, 2026, ["A"])
    with pytest.raises(ValueError, match="B"):
        E.peak_cagr(peaks.with_columns(pl.when(pl.col("weather_zone") == "B").then(0.0).otherwise("p50")
                                       .alias("p50")), 2021, 2026, ["A", "B"])


def _signals() -> pl.DataFrame:
    n = 6
    return pl.DataFrame(
        {
            "county_fips": [f"4800{i}" for i in range(n)],
            "addr_sf_homes": [100.0, 5000.0, 300.0, 2000.0, 50.0, 800.0],
            "pop_growth": [0.01, 0.20, 0.05, 0.10, 0.00, 0.03],
            "permits_per_1k": [1.0, 20.0, None, 8.0, 0.5, 3.0],
            "owner_sf_share": [0.6, 0.7, 0.5, 0.65, 0.55, 0.6],
            "zone_peak_cagr": [0.02] * 3 + [0.12] * 3,
            "ll_pressure": [5.0] * 3 + [150.0] * 3,
            "lz_spread": [130.0] * 3 + [170.0] * 3,
            "dc_sites": [0.0, 0.0, 1.0, 0.0, 0.0, 2.0],
            "retail_share": [1.0, 0.6, 0.0, 0.3, 0.1, 0.5],
            "partner_share": [0.0, 0.4, 1.0, 0.7, 0.9, 0.5],
        }
    )


def test_score_signals_gives_lists_classes_and_breaks():
    scored, breaks = E.score_signals(_signals())
    assert len(breaks) == 4 and breaks == sorted(breaks)
    assert scored.schema["drivers"] == pl.List(pl.Utf8) and scored.schema["drags"] == pl.List(pl.Utf8)
    assert scored["drivers"].null_count() == 0 and scored["drags"].null_count() == 0
    assert set(scored["priority_class"]) <= {1, 2, 3, 4, 5}
    top = scored.filter(pl.col("rank") == 1).row(0, named=True)
    assert top["county_fips"] == "48001" and top["priority_class"] == 5  # the most homes and growth
    assert top["drivers"] and set(top["drivers"]) <= set(E.SIGNALS)
    assert {"retail_rank", "partner_rank", "market_score", "grid_factor"} <= set(scored.columns)


def test_marts_are_declared_for_the_contract():
    names = [m.name for m in E.MARTS]
    assert names == ["mart_queue_project_scores", "mart_queue_adjusted_county", "mart_data_center_sites_new",
                     "mart_county_acquisition"]
    for m in E.MARTS:
        assert set(m.caveats) <= CAVEATS, m.name
        assert m.inputs and m.description
    assert set(E.SIGNALS) == set(Z.WEIGHTS)  # every weighted signal has a label, block and unit for mart_meta
    assert {s for s, (_, b, _) in E.SIGNALS.items() if b == "market"} == set(Z.MARKET)
    assert {s for s, (_, b, _) in E.SIGNALS.items() if b == "grid"} == set(Z.GRID)


def test_q4_golden_checks_hold_only_with_the_naics_matches():
    config = marts_config.load()
    assert E._naics_default(config)
    off = {**config, "dc_sites": {**config["dc_sites"], "include_naics_only": {"value": False, "status": "reviewed"}}}
    assert not E._naics_default(off)
    golden = [c for c in E.DC_CHECKS if c.as_of is not None]
    assert golden and all(c.applies is E._naics_default for c in golden)
    for checks in (E.PROJECT_CHECKS, E.COUNTY_CHECKS, E.ACQUISITION_CHECKS):
        assert all(c.as_of in (None, E.GOLDEN_AS_OF) for c in checks)
