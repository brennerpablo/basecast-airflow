"""The forecast marts' pure parts (``basecast_pipelines/marts/forecast.py``): years, the approvals pace, the LZ split,
the zone layers, the headline re-dating, the in-service stack, the official lines and the zone-sum checks."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import forecast as F
from basecast_pipelines.models import peak_forecast as pf

ZONES = F.ZONES


def test_forecast_years_keep_x7s_five_summers_and_stop_at_last_year():
    assert F.last_summer(date(2026, 9, 26)) == 2026
    assert F.last_summer(date(2026, 8, 31)) == 2025
    assert F.forecast_years(date(2026, 9, 26), 2030) == ([2027, 2028, 2029, 2030, 2031], [2027, 2028, 2029, 2030])
    assert F.forecast_years(date(2026, 3, 1), 2030) == ([2026, 2027, 2028, 2029, 2030], [2026, 2027, 2028, 2029, 2030])
    assert F.forecast_years(date(2026, 9, 26), 2033)[1][-1] == 2033


def test_pace_a2e_grows_linearly_from_the_start_month():
    start = pf.month_end_index("2026-06")
    out = F.pace_a2e(9_000.0, start, 1_200.0, [2027, 2028])
    # end of July 2027 is 13 months after the end of June 2026
    assert out[2027] == pytest.approx(9_000 + 1_200 * 13 / 12)
    assert out[2028] - out[2027] == pytest.approx(1_200)


def test_lz_split_prefers_headlines_and_adds_later_charts():
    headline = pl.DataFrame({"report_date": [date(2025, 1, 1), date(2025, 2, 1)], "lz_west": [600.0, 700.0],
                             "other": [400.0, 300.0], "src": ["headline"] * 2})
    chart = pl.DataFrame({"report_date": [date(2025, 1, 1), date(2025, 1, 15), date(2025, 3, 1)],
                          "lz_west": [1.0, 2.0, 500.0], "other": [1.0, 2.0, 500.0], "src": ["chart"] * 3})
    lzs = F.lz_split_table(headline, chart)
    assert lzs["report_date"].to_list() == [date(2025, 1, 1), date(2025, 2, 1), date(2025, 3, 1)]
    assert lzs["src"].to_list() == ["headline", "headline", "chart"]
    assert lzs["lz_west_share"].to_list() == pytest.approx([0.6, 0.7, 0.5])
    assert F.lz_share_at(lzs, date(2025, 2, 20)) == pytest.approx(0.7)
    assert F.lz_share_at(lzs, date(2024, 1, 1)) == pytest.approx(0.6)  # before the first deck: the first deck


def _even(value: float = 1.0) -> dict[str, float]:
    return {z: value / len(ZONES) for z in ZONES}


def test_zone_layers_split_the_statewide_draws_and_carry_the_allocation_range():
    np = pytest.importorskip("numpy")
    n, years = 2_000, [2027, 2028]
    rng = np.random.default_rng(0)
    org = {z: 1_000 + rng.normal(0, 10, size=(n, 2)) for z in ZONES}
    ll = np.full((n, 2), 800.0)  # stock_ll = 400: half the LL follows the stock split, half the pipeline split
    u = np.full((n, 2), 80.0)
    stock = {z: (1.0 if z == "NORTH" else 0.0) for z in ZONES}
    pipe = {z: (1.0 if z == "SOUTH" else 0.0) for z in ZONES}
    unatt = {z: (1.0 if z == "FWEST" else 0.0) for z in ZONES}
    rows = F.zone_layer_rows(org, ll, u, stock_ll=400.0, stock=stock, pipeline=pipe, unattributed=unatt,
                             variants=[(stock, pipe), (_even(), pipe), (stock, _even())], years=years, keep=[2027])
    frame = pl.DataFrame(rows)
    assert frame.height == len(ZONES) * 4 and set(frame["target_year"]) == {2027}

    def get(zone, layer):
        return frame.filter((pl.col("region_id") == zone) & (pl.col("layer") == layer)).row(0, named=True)

    assert get("NORTH", "large_load")["p50_mw"] == pytest.approx(400)
    assert get("SOUTH", "large_load")["p50_mw"] == pytest.approx(400)
    assert get("FWEST", "unattributed")["p50_mw"] == pytest.approx(80)
    # an even stock split leaves NORTH 400 / 8 = 50 MW; an even pipeline split gives it 400 + 400 / 8 = 450 MW
    assert get("NORTH", "large_load")["p10_mw"] == pytest.approx(50)
    assert get("NORTH", "large_load")["p90_mw"] == pytest.approx(450)
    assert get("SOUTH", "total")["p10_mw"] < get("SOUTH", "total")["p50_mw"] <= get("SOUTH", "total")["p90_mw"]
    # zone totals add up to the statewide LL + U (organic aside), and the shares of LL + U sum to 1
    extra = frame.filter(pl.col("layer").is_in(["large_load", "unattributed"]))["p50_mw"].sum()
    assert extra == pytest.approx(880)
    assert frame.filter(pl.col("layer") == "total")["share_of_ll_u"].sum() == pytest.approx(1.0)
    kinds = dict(zip(frame["layer"], frame["band_kind"], strict=False))
    assert kinds == {"organic": "p10_p90", "large_load": "allocation_range", "unattributed": "allocation_range",
                     "total": "allocation_range"}


def _headlines(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        rows, orient="row",
        schema={"report_date": pl.Date, "as_of_date": pl.Date, "metric": pl.String, "value": pl.Float64,
                "as_of_suspect": pl.Boolean, "peak_basis": pl.String, "member": pl.String, "file_name": pl.String},
    )


NONSIM, SIM = "observed_peak_non_simultaneous_mw", "observed_peak_simultaneous_mw"


def test_headlines_redate_the_known_tac_decks_and_drop_other_suspect_rows():
    hl = _headlines([
        (date(2025, 1, 22), date(2025, 1, 1), NONSIM, 3_211.0, False, "monthly", None, "a.pdf"),
        (date(2026, 1, 21), date(2025, 1, 1), NONSIM, 3_977.0, True, "monthly", "January TAC Report.pdf", "z.zip"),
        (date(2026, 1, 21), date(2025, 1, 1), SIM, 3_765.0, True, "monthly", "January TAC Report.pdf", "z.zip"),
        (date(2026, 3, 13), date(2025, 3, 1), NONSIM, 3_883.0, True, "monthly", None, "March-TAC-Report.pdf"),
        (date(2025, 3, 26), date(2025, 3, 1), NONSIM, 3_315.0, False, "monthly", None, "b.pdf"),
        (date(2024, 4, 1), None, NONSIM, 2_844.0, False, "all_time", None, "c.pdf"),
        (date(2025, 1, 22), date(2025, 1, 1), "approved_to_energize_mw", 6_306.0, False, None, None, "a.pdf"),
    ])
    fixed = F.redate_headlines(hl)
    assert fixed.height == 4  # the unconfirmed "March 2025", the undated row and the other metric are out
    assert fixed.filter(pl.col("redated"))["as_of_date"].unique().to_list() == [date(2026, 1, 1)]
    monthly = F.headline_observed_by_month(hl)
    by_month = {r["month"]: r for r in monthly.iter_rows(named=True)}
    assert by_month[date(2025, 1, 1)]["headline_nonsimultaneous_mw"] == 3_211
    assert by_month[date(2025, 1, 1)]["headline_simultaneous_mw"] is None
    assert by_month[date(2026, 1, 1)]["headline_nonsimultaneous_mw"] == 3_977
    assert by_month[date(2026, 1, 1)]["headline_redated"] is True
    assert by_month[date(2025, 3, 1)]["headline_nonsimultaneous_mw"] == 3_315


def test_month_spine_keeps_months_without_a_reading():
    spine = F.month_spine(date(2023, 10, 15), date(2024, 2, 1))
    assert spine["month"].to_list() == [date(2023, 10, 1), date(2023, 11, 1), date(2023, 12, 1), date(2024, 1, 1),
                                        date(2024, 2, 1)]


def test_in_service_long_maps_the_buckets_and_leaves_out_unprinted_segments():
    bars = pl.DataFrame({
        "vintage": [date(2026, 3, 13)] * 2, "report_date": [date(2026, 3, 13)] * 2, "document": ["d.pdf"] * 2,
        "page": [3, 3], "year": [2025, 2027], "total_mw": [6_714.0, 66_679.0], "a2e": [6_714.0, 8_720.0],
        "planning_studies_approved": [None, 9_164.0], "under_ercot_review": [None, 28_454.0],
        "no_studies_submitted": [None, 20_341.0],
    })
    long = F.in_service_long(bars)
    assert long.height == 5
    assert set(long["status"]) == {"approved_to_energize", "planning_studies_approved", "under_ercot_review",
                                   "no_studies_submitted"}
    y27 = long.filter(pl.col("in_service_year") == 2027)
    assert y27["mw"].sum() == pytest.approx(66_679) and not long["verified"].any()


def _official(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        rows, orient="row",
        schema={"product": pl.String, "vintage": pl.String, "vintage_date": pl.Date, "target_year": pl.Int64,
                "region_type": pl.String, "region_id": pl.String, "scenario": pl.String, "value": pl.Float64,
                "source_file": pl.String},
    )


def test_official_lines_take_the_latest_vintages_by_as_of():
    frame = _official([
        ("LTLF", "LTLF 2024", date(2024, 4, 1), 2027, "ercot", "ERCOT", "ercot_adjusted", 90.0, "f"),
        ("LTLF", "LTLF 2025", date(2025, 4, 8), 2027, "ercot", "ERCOT", "ercot_adjusted", 100.0, "f"),
        ("LTLF", "LTLF 2025", date(2025, 4, 8), 2027, "weather_zone", "FWEST", "ercot_adjusted", 10.0, "f"),
        ("LTLF", "LTLF 2025", date(2025, 4, 8), 2027, "ercot", "ERCOT", "tsp_provided", 130.0, "f"),
        ("LTLF", "LTLF 2025", date(2025, 4, 8), 2035, "ercot", "ERCOT", "tsp_provided", 999.0, "f"),
        ("CDR", "CDR May 2025", date(2025, 5, 1), 2027, "ercot", "ERCOT", None, 101.0, "f"),
        ("CDR", "CDR May 2025 Revised", date(2025, 5, 1), 2027, "ercot", "ERCOT", None, 102.0, "f"),
        ("CDR", "CDR Dec 2026", date(2026, 12, 1), 2027, "ercot", "ERCOT", None, 105.0, "f"),
    ])
    out = F.official_lines(frame, date(2026, 9, 26), [2027, 2028])
    got = {(r["series"], r["region_id"]): (r["vintage"], r["label"], r["mw"]) for r in out.iter_rows(named=True)}
    assert got == {
        ("ercot_adjusted", "ERCOT"): ("2025", "LTLF 2025 (ERCOT-adjusted)", 100.0),
        ("ercot_adjusted", "FWEST"): ("2025", "LTLF 2025 (ERCOT-adjusted)", 10.0),
        ("tsp_provided", "ERCOT"): ("2025", "LTLF 2025 (TSP-provided)", 130.0),
        ("cdr", "ERCOT"): ("May 2025 Revised", "CDR May 2025 Revised", 102.0),  # the revision wins; Dec 2026 is later
    }


def _peak_frame(ercot: float, zones: float, fixed: float) -> pl.DataFrame:
    rows = [{"region_type": "ercot", "region_id": "ERCOT", "variant": F.PRE, "layer": "total", "target_year": 2027,
             "p50_mw": ercot, "x7_fixed_p50_mw": None}]
    rows += [{"region_type": "weather_zone", "region_id": z, "variant": F.PRE, "layer": "total", "target_year": 2027,
              "p50_mw": zones / len(ZONES), "x7_fixed_p50_mw": fixed / len(ZONES)} for z in ZONES]
    return pl.DataFrame(rows)


def test_zone_sum_checks_compare_with_ercot_and_with_x7s_fixed_shares():
    frame = _peak_frame(92_819.0, 92_720.0, 92_730.0)  # X11 §4, 2027
    assert not F._gap_check("x11_mw", "ercot_mw", 0.1)(frame).passed  # 0.107%
    assert F._gap_check("x7_fixed_mw", "ercot_mw", 0.1)(frame).passed  # 0.096%
    assert F._gap_check("x11_mw", "x7_fixed_mw", 0.1)(frame).passed  # 0.011%
    assert F._zone_sum("x11_mw", 2027)(frame) == pytest.approx(92_720)


def test_switches_the_code_cannot_honour_raise():
    config = marts_config.load()
    F._check_switches(config)
    for key, bad in (("large_load.mode", "scenarios"), ("large_load.zone_allocation", "x7_fixed"),
                     ("forecast.default_variant", "approvals_pace")):
        group, name = key.split(".")
        other = {**config, group: {**config[group], name: {"value": bad, "status": "reviewed"}}}
        with pytest.raises(NotImplementedError):
            F._check_switches(other)


def test_golden_checks_hold_only_for_the_x7_default():
    config = marts_config.load()
    assert F._as_x7(config)
    switch = {"value": F.LATEST, "status": "reviewed"}
    latest = {**config, "forecast": {**config["forecast"], "default_variant": switch}}
    assert not F._as_x7(latest)
    names = [c.name for c in F.MARTS[0].checks]
    assert len(names) == len(set(names))
    assert [m.name for m in F.MARTS] == ["mart_peak_forecast", "mart_official_peak_lines",
                                         "mart_large_load_realization", "mart_large_load_in_service",
                                         "mart_large_load_monthly"]
