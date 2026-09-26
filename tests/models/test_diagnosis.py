"""Per-account diagnosis (``basecast_pipelines/models/diagnosis.py``), on tiny synthetic frames (no database)."""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import diagnosis as D
from basecast_pipelines.models import triggers as T

AS_OF = date(2026, 9, 26)
WEIGHTS = {"a": 0.5, "b": 0.3, "c": 0.2}
DIRS = {"a": "higher", "b": "higher", "c": "higher"}

LINKS = pl.DataFrame({
    "account_id": ["c1", "c1", "m1", "c2"],
    "county_fips": ["001", "002", "002", "003"],
    "overlap_km2": [900.0, 100.0, 20.0, 500.0],
    "county_share": [0.6, 0.1, 0.02, 0.5],
    "territory_share": [0.9, 0.1, 1.0, 1.0],
})
GEO = pl.DataFrame({"county_fips": ["001", "002", "003"], "county_name": ["Alpha", "Beta", "Gamma"]})
ZONE = pl.DataFrame({"county_fips": ["001", "002", "003"], "weather_zone": ["NCENT", "NORTH", "NCENT"]})


def _event(acct, trigger, d, county, ref):
    return {"account_id": acct, "trigger": trigger, "event_date": d, "county_fips": county, "title": f"t-{ref}",
            "detail": "d", "source": "s", "source_ref": ref, "exposure": 1.0}


EVENTS = pl.DataFrame([
    _event("c1", "dc_permit", date(2026, 9, 1), "001", "RN1"),
    _event("c1", "gen_storage_ia", date(2025, 12, 1), "001", "INR1"),
    _event("c1", "new_transmission", date(2026, 7, 13), "001", "P1"),
    _event("c1", "dc_permit", date(2024, 1, 1), "001", "RN0"),  # old
    _event("c1", "dev_agreement", date(2027, 1, 1), "001", "CH380"),  # future: never active
    _event("m1", "market_registration", date(2026, 5, 1), None, "LSE:M1"),
])


def _inputs() -> D.DiagnosisInputs:
    signals = pl.DataFrame({
        "account_id": ["c1", "m1", "c2"], "a": [3.0, 1.0, 2.0], "b": [None, 5.0, 1.0], "c": [1.0, 2.0, 3.0],
        "population": [1000.0, 50.0, 400.0], "pop_growth": [0.1, 0.02, 0.05], "owner_sf_homes": [500.0, 10.0, 200.0],
        "owner_sf_share": [0.6, 0.4, 0.5], "permits_per_1k": [5.0, 1.0, 2.0], "dc_sites": [0.6, 0.0, 0.0],
    })
    pct = T.percentile_ranks(signals, DIRS)
    scored = T.score_accounts(pct, WEIGHTS).with_columns(
        pl.col("rank").map_elements(lambda r: T.score_tier(r, 3), return_dtype=pl.Utf8).alias("tier")
    )
    eia = pl.DataFrame({
        "utility_id": ["10"] * 4 + ["20"] * 2,
        "data_year": [2019, 2023, 2024, 2025, 2019, 2024],
        "early_release": [False, False, False, True, False, False],
        "form": ["long", "long", "long", "long", "short", "short"],
        "customers": [100.0, 110.0, 10.0, 11.0, 50.0, 60.0],
        "delivery_customers": [0.0, 0.0, 105.0, 106.0, 0.0, 0.0],
        "sales_mwh": [1000.0, 1100.0, 200.0, 210.0, 400.0, 500.0],
        "revenue_thousand_usd": [100.0, 121.0, 30.0, 31.0, 40.0, 60.0],
        "price_usd_kwh": [0.10, 0.11, 0.15, 0.1476, 0.10, 0.12],
    })
    res = pl.DataFrame({"utility_id": ["10"], "data_year": [2024], "early_release": [False], "usd_per_kwh": [0.16]})
    return D.DiagnosisInputs(
        accounts=pl.DataFrame({"account_id": ["c1", "m1", "c2"], "name": ["Coop One", "City Muni", "Coop Two"],
                               "account_type": ["coop", "muni", "coop"], "eia_utility_id": ["10", "20", None],
                               "gt_cooperative": ["G&T X", None, None]}),
        links=LINKS, geo=GEO, county_zone=ZONE, signals=signals, scored=scored, weights=WEIGHTS,
        signal_meta={"a": ("Signal A", "share", "src_a")}, events=EVENTS, eia=eia, res_price=res,
        dc_sites=pl.DataFrame({"ref_num_txt": ["RN1", "RN9"], "reg_ent_name": ["DC ONE", "DC OLD"],
                               "county_fips": ["001", "002"], "first_affil_begin_dt": [date(2026, 9, 1), date(2024, 1, 1)],
                               "matched_by_name": [True, False]}),
        queue=pl.DataFrame({"county_fips": ["001", "001", "002"], "stratum": ["storage", "solar", "storage"],
                            "projects": [2, 1, 1], "raw_mw": [300.0, 100.0, 50.0], "adj_mw_2027": [30.0, 5.0, 10.0],
                            "adj_mw_2028": [60.0, 10.0, 20.0]}),
        permits_monthly=pl.DataFrame({
            "county_fips": ["001", "001", "002"],
            "period_start": [date(2025, 3, 1), date(2026, 3, 1), date(2026, 3, 1)],
            "units_total": [100.0, 150.0, 1000.0],
        }),
        permits_month=date(2026, 8, 1),
        zone_peaks=pl.DataFrame({"region_id": ["NCENT"] * 3, "target_year": [2025, 2026, 2031],
                                 "value": [30000.0, 31000.0, 40000.0]}),
        zone_cp=pl.DataFrame({"region_id": ["NCENT"] * 2 + ["NORTH"], "year": [2025, 2026, 2025],
                              "cp_avg_mw": [27000.0, 28000.0, 1500.0], "ncp_summer_mw": [28000.0, 31620.0, 1800.0],
                              "cf_summer": [0.964, 0.9, 0.83], "share_4cp": [0.33, 0.33, 0.02],
                              "share_energy": [0.30, 0.30, 0.025], "ncp_end_hour": [16.9, 17.0, 15.5],
                              "n_months": [4, 3, 4]}),
        sources={"src_a": D.Source("table_a", date(2025, 7, 1))},
        as_of=AS_OF,
        ltlf_years=(2025, 2031),
    )


def test_an_account_outside_the_scored_universe_is_never_assembled():
    with pytest.raises(KeyError):
        D.assemble("held_out", _inputs())


def test_contributions_sum_to_the_score_and_missing_signals_are_renormalized():
    inp = _inputs()
    d = D.assemble("c1", inp)
    sig = d["score"]["signals"]
    assert sig["contribution"].sum() == pytest.approx(d["score"]["score"])
    b = sig.filter(pl.col("signal") == "b").row(0, named=True)
    assert b["pct"] is None and b["weight_used"] == 0
    assert sig.filter(pl.col("signal") == "a")["weight_used"][0] == pytest.approx(0.5 / 0.7)
    assert sig.filter(pl.col("signal") == "a")["source"][0] == "table_a"


def test_context_counties_fall_back_to_the_home_county():
    c1, label1 = D.with_context(D.account_counties(LINKS, GEO, ZONE, "c1"))
    assert c1.filter(pl.col("context"))["county_fips"].to_list() == ["001"] and "1 exposed" in label1
    m1, label2 = D.with_context(D.account_counties(LINKS, GEO, ZONE, "m1"))
    assert m1.filter(pl.col("context"))["county_fips"].to_list() == ["002"] and "home county Beta" in label2
    assert not m1["exposed"].any()


def test_queue_sums_context_counties_and_apportions_all():
    counties, _ = D.with_context(D.account_counties(LINKS, GEO, ZONE, "c1"))
    q = D.queue_near(pl.DataFrame({"county_fips": ["001", "002"], "stratum": ["storage", "storage"], "projects": [1, 1],
                                   "raw_mw": [300.0, 50.0], "adj_mw_2027": [30.0, 10.0], "adj_mw_2028": [60.0, 20.0]}),
                     counties)
    total = q.filter(pl.col("stratum") == "total").row(0, named=True)
    assert total["raw_mw_context"] == 300.0
    assert total["raw_mw_apportioned"] == pytest.approx(300 * 0.6 + 50 * 0.1)


def test_timeline_marks_active_events_and_never_fires_future_ones():
    tl = D.trigger_timeline(EVENTS, "c1", as_of=AS_OF)
    active = dict(zip(tl["source_ref"], tl["active"], strict=True))
    assert active == {"RN1": True, "INR1": True, "P1": True, "RN0": False, "CH380": False}
    assert tl.filter(pl.col("source_ref") == "P1")["strength"][0] == "context"
    assert tl["event_date"].to_list() == sorted(tl["event_date"].to_list(), reverse=True)


def test_next_action_card_names_the_rule_and_the_freshest_strong_trigger():
    tl = D.trigger_timeline(EVENTS, "c1", as_of=AS_OF)
    card = D.next_action_card("B", 30, 107, tl)
    assert card["action"] == T.next_action("B", 2, 25) == "call_now"
    assert card["lead_trigger"]["source_ref"] == "RN1"
    assert card["offer"] == T.TRIGGERS["dc_permit"].offer
    assert "Tier B (rank 30 of 107) + 2 active strong triggers (freshest 25 days old)" in card["rule"]
    assert any(p.startswith(T.TRIGGERS["new_transmission"].label) for p in card["talking_points"])
    assert card["changes_on"] is None or card["changes_on"] > AS_OF
    quiet = D.next_action_card("C", 90, 107, tl.filter(pl.col("strength") == "context"))
    assert quiet["action"] == "hold" and quiet["lead_trigger"] is None


def test_eia_series_counts_delivery_only_meters_and_keeps_later_early_release():
    inp = _inputs()
    s = D.eia_series(inp.eia, inp.res_price, "10", years=(2013, 2024))
    assert s["data_year"].to_list() == [2019, 2023, 2024, 2025]
    assert D.series_value(s, 2024, "meters") == 115.0  # 10 bundled + 105 delivery-only: no false break
    assert D.yoy_breaks(s) == []
    assert D.yoy_breaks(s, col="customers")[0][0] == 2024
    assert D.series_value(s, 2024, "res_price_usd_kwh") == 0.16
    assert D.eia_series(inp.eia, inp.res_price, None, years=(2013, 2024)).height == 0


def test_permit_trend_uses_the_x5_windows():
    counties, _ = D.with_context(D.account_counties(LINKS, GEO, ZONE, "c1"))
    monthly = pl.DataFrame({"county_fips": ["001", "001", "001"],
                            "period_start": [date(2024, 9, 1), date(2025, 8, 1), date(2025, 9, 1)],
                            "units_total": [10.0, 100.0, 150.0]})
    t = D.permit_trend(monthly, counties, as_of_month=date(2026, 8, 1))
    assert t["recent"] == pytest.approx(150 * 0.6) and t["prior"] == pytest.approx(110 * 0.6)
    assert D.permit_trend(monthly.head(0), counties, as_of_month=date(2026, 8, 1))["change"] is None


def test_zone_outlook_reads_the_latest_full_summer_and_flags_early_peaks():
    inp = _inputs()
    o = D.zone_outlook(inp.zone_peaks, inp.zone_cp, "NCENT", ltlf_years=(2025, 2031), as_of=AS_OF)
    assert o["cp_year"] == 2025 and o["intensity"] == pytest.approx(0.33 / 0.30)
    assert o["now_vs_ltlf"] == pytest.approx(31620 / 31000 - 1)
    assert o["ltlf_cagr"] == pytest.approx((40000 / 30000) ** (1 / 6) - 1)
    assert o["peak_mismatch"] is None and "15:45–17:45" in o["line"]
    north = D.zone_outlook(inp.zone_peaks, inp.zone_cp, "NORTH", ltlf_years=(2025, 2031), as_of=AS_OF)
    assert north["peak_mismatch"] == "early" and north["ltlf_end"] is None and north["q7_holdout_mape"] == 47.0
    assert D.zone_outlook(inp.zone_peaks, inp.zone_cp, None, ltlf_years=(2025, 2031)) == {"zone": None}


def test_assembled_diagnosis_carries_sources_gaps_and_renders():
    inp = _inputs()
    d = D.assemble("m1", inp)
    assert d["simulated"] is False
    facts = {f["key"]: f for f in D.all_facts(d)}
    assert facts["customers"]["value"] == 60.0 and facts["customers"]["as_of"] == date(2024, 12, 31)
    assert facts["queue_raw_mw"]["value"] == 50.0  # home county Beta
    assert "home county Beta" in facts["queue_raw_mw"]["note"]
    assert facts["ercot_roles"]["value"] == "LSE"
    kinds = {(g["key"], g["kind"]) for g in D.data_gaps(d, as_of=AS_OF)}
    assert ("exposed_counties", "structural") in kinds
    assert ("res_price", "missing") in kinds and ("eia_series", "coverage") in kinds
    assert ("account_4cp_mw", "missing") in kinds
    assert not any(k == "zone_q7_mape" for k, _ in kinds)
    md = D.render_markdown(d)
    assert "### City Muni (muni, CCN m1)" in md and "market_registration" in md
    json.loads(D.dumps(d))  # dates and frames serialize


def test_coverage_is_computed_per_fact_and_type():
    inp = _inputs()
    cov = D.coverage([D.assemble(a, inp) for a in ("c1", "m1", "c2")])
    row = cov.filter(pl.col("key") == "customers").row(0, named=True)
    assert row["all"] == pytest.approx(2 / 3) and row["muni"] == 1.0 and row["coop"] == 0.5


def test_the_action_change_date_is_when_the_last_strong_event_ages_out():
    tl = D.trigger_timeline(pl.DataFrame([_event("x", "dc_permit", date(2025, 10, 22), "001", "RN")]), "x", as_of=AS_OF)
    assert D.action_on("A", tl, AS_OF) == "call_now"
    assert D.action_changes_on("A", tl, AS_OF) == (date(2026, 10, 22), "nurture")
    fresh = D.trigger_timeline(pl.DataFrame([_event("x", "dc_permit", date(2026, 9, 1), "001", "RN")]), "x", as_of=AS_OF)
    assert D.action_changes_on("B", fresh, AS_OF) == (date(2026, 12, 1), "nurture")  # 91 days: no longer fresh
    assert D.action_changes_on("C", tl.head(0), AS_OF) is None
