"""Account triggers and next action (``basecast_pipelines/models/triggers.py``), on tiny synthetic frames."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import triggers as T

LINKS = pl.DataFrame(
    {
        "account_id": ["c1", "c1", "c2", "m1"],
        "county_fips": ["001", "002", "002", "002"],
        "county_share": [0.6, 0.1, 0.5, 0.02],
    }
)


def _event(trigger: str, d: date, county: str | None, ref: str) -> dict:
    return {"trigger": trigger, "event_date": d, "county_fips": county, "title": "t", "detail": "d",
            "source": "s", "source_ref": ref}


def test_county_events_reach_accounts_above_the_share_threshold():
    ev = pl.DataFrame([_event("dc_permit", date(2026, 5, 1), "002", "RN1"), _event("dc_permit", date(2026, 5, 1), None, "RN2")])
    out = T.events_by_county(ev, LINKS, min_share=0.2)
    assert out.select("account_id", "exposure").rows() == [("c2", 0.5)]  # c1 has 10%, m1 2%; null county dropped


def test_a_line_through_two_counties_counts_once_per_account():
    ev = pl.DataFrame([_event("new_transmission", date(2026, 7, 1), c, "P1") for c in ("001", "002")])
    out = T.events_by_county(ev, LINKS, min_share=0.05).sort("account_id")
    assert out.select("account_id", "exposure").rows() == [("c1", 0.6), ("c2", 0.5)]


def test_active_window_excludes_old_and_future_events():
    ev = pl.DataFrame(
        {
            "account_id": ["a", "a", "a"],
            "trigger": ["dc_permit"] * 3,
            "event_date": [date(2025, 9, 26), date(2025, 9, 27), date(2026, 10, 1)],
            "source_ref": ["x", "y", "z"],
        }
    )
    act = T.active_events(ev, as_of=date(2026, 9, 26), window_days=365)
    assert act["source_ref"].to_list() == ["y"]
    assert act["age_days"].to_list() == [364]


def test_dc_permit_events_drop_undated_sites():
    sites = pl.DataFrame(
        {
            "ref_num_txt": ["RN1", "RN2"],
            "reg_ent_name": ["A DATA CENTER", "B"],
            "county_fips": ["001", "002"],
            "first_affil_begin_dt": [date(2026, 1, 5), None],
            "matched_by_name": [True, False],
        }
    )
    out = T.dc_permit_events(sites)
    assert out["source_ref"].to_list() == ["RN1"]
    assert out["detail"].to_list() == ["matched by name"]


def test_gen_storage_events_filter_fuel_and_size_and_map_county_names():
    projects = pl.DataFrame(
        {
            "inr": ["1", "2", "3", "4"],
            "project_name": ["S", "W", "small", "none"],
            "county": ["Fort Bend", "Fort Bend", "Fort Bend", "Fort Bend"],
            "fuel_type": ["storage", "wind", "storage", "gas"],
            "capacity_mw": [100.0, 300.0, 10.0, 200.0],
            "ia_signed": [date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 1), None],
            "last_status": ["active"] * 4,
        }
    )
    geo = pl.DataFrame({"county_fips": ["48157"], "county_name": ["Fort Bend County"]})
    out = T.gen_storage_events(projects, geo, min_mw=50, fuels=("storage", "gas"))
    assert out.select("source_ref", "county_fips").rows() == [("1", "48157")]


def test_dev_agreements_value_rule_and_mapping_split():
    ag = pl.DataFrame(
        {
            "program": ["ch312", "ch380", "ch380", "ch380", "ch380"],
            "agreement_id": ["a", "b", "c", "d", "e"],
            "status": ["Active", "Active", "Active", "Cancelled", "Active"],
            "local_government_type": [None, "City", "County", "City", "City"],
            "local_government_name": ["Zeta City", "Zeta", "Alpha", "Zeta", "Zeta"],
            "county_fips": ["001"] * 5,
            "recipient_name": ["R"] * 5,
            "executed_date": [date(2026, 3, 1), None, None, None, None],
            "effective_date": [date(2027, 1, 1), date(2026, 2, 1), date(2026, 2, 1), date(2026, 2, 1), date(2026, 2, 1)],
            "total_incentive_value": [0.0, 5e6, 2e6, 9e6, 1e5],
        }
    )
    ev = T.dev_agreement_events(ag, min_value=1e6)
    assert sorted(ev["source_ref"].to_list()) == ["ch312:a", "ch380:b", "ch380:c"]  # cancelled and small dropped
    assert ev.filter(pl.col("source_ref") == "ch312:a")["event_date"].item() == date(2026, 3, 1)  # executed date
    assert sorted(T.county_agreements(ev)["source_ref"].to_list()) == ["ch312:a", "ch380:c"]
    accounts = pl.DataFrame({"account_id": ["m1", "c1"], "name": ["City of Zeta", "Zeta Electric Coop"],
                             "account_type": ["muni", "coop"]})
    by_name = T.city_agreement_accounts(ev, accounts)
    assert by_name.select("account_id", "source_ref").rows() == [("m1", "ch380:b")]


def test_transmission_events_are_dated_by_first_listing():
    tpit = pl.DataFrame(
        {
            "snapshot_date": [date(2025, 6, 1), date(2026, 6, 1), date(2026, 6, 1), None],
            "project_id": ["P1", "P1", "P2", "P3"],
            "project_title": ["line", "line", "low", "undated"],
            "tsp": ["X"] * 4,
            "kv": [345.0, 345.0, 69.0, 345.0],
            "transmission_status": ["Planned"] * 4,
            "county_start_fips": ["001", "001", "001", "001"],
            "county_end_fips": ["002", "002", "001", "002"],
        }
    )
    out = T.transmission_events(tpit, min_kv=138).sort("county_fips")
    assert out.select("source_ref", "event_date", "county_fips").rows() == [
        ("P1", date(2025, 6, 1), "001"),
        ("P1", date(2025, 6, 1), "002"),
    ]


def test_registration_matches_by_core_name_within_type():
    pytest.importorskip("rapidfuzz")
    mp = pl.DataFrame(
        {
            "name": ["ZETA ELECTRIC CO OP INC (LSE)", "CITY OF ZETA (LSE)", "ZETA POWER LLC (QSE)"],
            "entity_name": ["ZETA ELECTRIC CO OP INC", "CITY OF ZETA", "ZETA POWER LLC"],
            "market_participant_type": ["LSE", "LSE", "QSE"],
            "sfa_effective_date": [date(2026, 5, 1), date(2026, 2, 1), date(2026, 1, 1)],
        }
    )
    accounts = pl.DataFrame({"account_id": ["c1", "m1"], "name": ["Zeta Electric Cooperative, Inc.", "Zeta City Government"],
                             "account_type": ["coop", "muni"]})
    out = T.registration_events(mp, accounts).sort("account_id", "source_ref")
    # the co-op matches its CO OP registration; the untyped "ZETA POWER LLC" matches both by core name
    assert out.select("account_id", "source_ref").rows() == [
        ("c1", "LSE:ZETA ELECTRIC CO OP INC (LSE)"),
        ("c1", "QSE:ZETA POWER LLC (QSE)"),
        ("m1", "LSE:CITY OF ZETA (LSE)"),
        ("m1", "QSE:ZETA POWER LLC (QSE)"),
    ]


def test_rate_increase_uses_residential_price_change():
    sales = pl.DataFrame(
        {
            "utility_id": ["1", "1", "2", "2"],
            "data_year": [2024, 2025, 2024, 2025],
            "part": ["A"] * 4,
            "sector": ["residential"] * 4,
            "revenue_thousand_usd": [100.0, 120.0, 100.0, 101.0],
            "sales_mwh": [1000.0, 1000.0, 1000.0, 1000.0],
            "early_release": [False, True, False, True],
        }
    )
    prices = T.residential_price(sales)
    universe = pl.DataFrame({"account_id": ["a1", "a2"], "eia_utility_id": ["1", "2"]})
    out = T.rate_increase_events(prices, universe, year=2025, base_year=2024, threshold=0.10)
    assert out["account_id"].to_list() == ["a1"]
    assert out["event_date"].to_list() == [date(2025, 12, 31)]


def test_permit_surge_compares_two_twelve_month_windows():
    months = [date(2024, m, 1) for m in range(9, 13)] + [date(2025, m, 1) for m in range(1, 13)] + [
        date(2026, m, 1) for m in range(1, 9)
    ]
    monthly = pl.DataFrame(
        {
            "county_fips": ["001"] * len(months),
            "period_start": months,
            "units_total": [10.0 if d <= date(2025, 8, 1) else 20.0 for d in months],
        }
    )
    links = pl.DataFrame({"account_id": ["c1"], "county_fips": ["001"], "county_share": [0.5]})
    out = T.permit_surge_events(monthly, links, as_of_month=date(2026, 8, 1), growth=0.25, min_units=50)
    assert out["account_id"].to_list() == ["c1"]
    assert out["title"].item() == "permits +100.0% in 12 months"  # 60 → 120 apportioned units


def test_tsp_large_load_maps_gt_to_rfi_tsp():
    gt = pl.DataFrame({"account_id": ["c1", "c2", "m1"],
                       "gt": ["Brazos Electric Power Coop", "Unknown G&T", "LCRA"]})
    rfi = pl.DataFrame(
        {
            "docket": ["58777"] * 4, "item": [38] * 4, "filed_date": [date(2026, 4, 15)] * 4,
            "name": ["Brazos", "Brazos", "LCRA", "LCRA"], "year": [2030, 2030, 2030, 2029],
            "mw": [26576.0, 26576.0, 500.0, 400.0],
        }
    )
    out = T.tsp_large_load_events(gt, rfi, target_year=2030, min_mw=1000)
    assert out["account_id"].to_list() == ["c1"]
    accounts = pl.DataFrame({"account_id": ["m9"], "name": ["CPS Energy"]})
    assert T.self_tsp_rows(accounts).rows() == [("m9", "CPS Energy")]


def test_next_action_rules():
    assert T.next_action("A", 1, 300) == "call_now"
    assert T.next_action("A", 0, None) == "nurture"
    assert T.next_action("B", 2, 300) == "call_now"
    assert T.next_action("B", 1, 30) == "call_now"
    assert T.next_action("B", 1, 200) == "nurture"
    assert T.next_action("B", 0, None) == "watch"
    assert T.next_action("C", 1, 10) == "watch"
    assert T.next_action("C", 0, None) == "hold"
    assert [T.score_tier(r, 8) for r in range(1, 9)] == ["A", "A", "B", "B", "C", "C", "C", "C"]


def test_trigger_summary_counts_strong_and_context():
    ev = pl.DataFrame(
        {
            "account_id": ["a", "a", "a", "b"],
            "trigger": ["dc_permit", "dc_permit", "new_transmission", "rate_increase"],
            "event_date": [date(2026, 9, 1), date(2026, 3, 1), date(2026, 7, 1), date(2025, 12, 31)],
            "source_ref": ["x", "y", "z", "w"],
        }
    )
    s = T.account_trigger_summary(ev, as_of=date(2026, 9, 26)).sort("account_id")
    assert s.select("account_id", "n_strong", "n_context", "strong_age_days").rows() == [
        ("a", 1, 1, 25),
        ("b", 0, 1, None),
    ]
    cov = T.trigger_coverage(ev, 10, as_of=date(2026, 9, 26))
    row = cov.filter(pl.col("trigger") == "dc_permit").row(0, named=True)
    assert (row["accounts_12m"], row["events_12m"], row["share_12m"]) == (1, 2, 0.1)


def test_score_is_weighted_percentile_rank_with_renormalization():
    sig = pl.DataFrame({"account_id": ["a", "b", "c"], "x": [1.0, 2.0, 3.0], "y": [3.0, None, 1.0]})
    pct = T.percentile_ranks(sig, {"x": "higher", "y": "lower"})
    assert pct["pct_x"].to_list() == [0.0, 0.5, 1.0]
    assert pct["pct_y"].to_list() == [0.0, None, 1.0]
    scored = T.score_accounts(pct, {"x": 0.5, "y": 0.5})
    assert scored.select("account_id", "score", "rank").rows() == [("c", 1.0, 1), ("b", 0.5, 2), ("a", 0.0, 3)]


def test_rank_stability_reports_every_variant():
    sig = pl.DataFrame({"account_id": list("abcde"), "x": [1.0, 2, 3, 4, 5], "y": [5.0, 3, 4, 1, 2]})
    pct = T.percentile_ranks(sig, {"x": "higher", "y": "higher"})
    stab = T.rank_stability(pct, {"x": 0.6, "y": 0.4}, delta=0.05, top=2)
    assert stab.height == 2 * 2 + 2
    assert stab.filter(pl.col("variant") == "x +0.05")["spearman"].item() == pytest.approx(1.0)
    assert stab.filter(pl.col("variant") == "without x")["spearman"].item() < 1.0


# --- X16 findings 5 and 6: NaN scores ----------------------------------------------------------------------------


def test_an_account_without_any_signal_gets_a_null_score_ranked_last():
    sig = pl.DataFrame({"account_id": ["a", "b", "c", "z"], "x": [1.0, 2.0, 3.0, None], "y": [3.0, 1.0, 2.0, None]})
    scored = T.score_accounts(T.percentile_ranks(sig, {"x": "higher", "y": "higher"}), {"x": 0.5, "y": 0.5})
    assert scored["account_id"].to_list() == ["c", "a", "b", "z"]
    assert scored.filter(pl.col("account_id") == "z").select("score", "rank").row(0) == (None, 4)
    assert not scored["score"].is_nan().any()


def test_a_signal_only_one_account_has_ranks_nobody():
    # X16 #6: x = [1.0, None] gave pct_x = [NaN, null], and the NaN poisoned the score
    sig = pl.DataFrame({"account_id": ["a", "b", "c"], "x": [1.0, None, None], "y": [1.0, 2.0, 3.0]})
    pct = T.percentile_ranks(sig, {"x": "lower", "y": "higher"})
    assert pct["pct_x"].to_list() == [None, None, None]
    assert pct["pct_y"].to_list() == [0.0, 0.5, 1.0]
    scored = T.score_accounts(pct, {"x": 0.5, "y": 0.5})
    assert scored.select("account_id", "score").rows() == [("c", 1.0), ("b", 0.5), ("a", 0.0)]  # on y alone
    empty = T.percentile_ranks(sig.with_columns(pl.lit(None, pl.Float64).alias("x")), {"x": "higher"})
    assert empty["pct_x"].to_list() == [None, None, None]
