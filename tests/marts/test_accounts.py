"""The account marts' pure parts (``basecast_pipelines/marts/accounts.py``): flags, glossary, golden-check switches."""

from __future__ import annotations

from datetime import date

import polars as pl

from basecast_pipelines.marts import accounts as M
from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts.core import MartContext
from basecast_pipelines.models import triggers as T


def _diag(*, exposed: list[bool], hpm: float | None, form: str | None, breaks: list) -> dict:
    return {
        "header": [{"key": "eia_form", "value": form}],
        "territory": {"facts": [{"key": "homes_per_meter", "value": hpm}],
                      "counties": pl.DataFrame({"exposed": exposed}, schema={"exposed": pl.Boolean})},
        "eia_breaks": breaks,
    }


def test_flags_follow_the_data_gap_rules():
    assert M.flags(_diag(exposed=[False, False], hpm=0.1, form="short", breaks=[(2020, 0.8)])) == [
        "no_exposed_county", "apportionment_under", "short_form", "eia_break"]
    assert M.flags(_diag(exposed=[True], hpm=1.4, form="long", breaks=[])) == ["apportionment_over"]
    assert M.flags(_diag(exposed=[], hpm=None, form=None, breaks=[])) == []


def test_glossary_covers_every_trigger_flag_and_action():
    g = M.build_glossary(MartContext(date(2026, 9, 26), {}, read_sql=lambda *a: None))
    triggers = g.filter(pl.col("kind") == "trigger")
    assert set(triggers["code"]) == set(T.TRIGGERS)
    assert dict(zip(triggers["code"], triggers["strength"])) == {k: v.strength for k, v in T.TRIGGERS.items()}
    assert set(g.filter(pl.col("kind") == "flag")["code"]) == set(M.FLAGS)
    assert set(g.filter(pl.col("kind") == "next_action")["code"]) == {"call_now", "nurture", "watch", "hold"}
    assert g.filter(pl.col("kind") != "trigger")["strength"].null_count() == g.height - triggers.height


def test_golden_checks_hold_only_for_x5_switches():
    config = marts_config.load()
    assert M._as_x5(config)
    x4 = {**config, "scoring": {"weights_set": {"value": "x4", "status": "reviewed"}}}
    assert not M._as_x5(x4)
    golden = [c for c in M.ACCOUNT_CHECKS if c.as_of is not None]
    assert golden and all(c.applies is M._as_x5 for c in golden)
    structural = [c.name for c in M.ACCOUNT_CHECKS if c.as_of is None]
    assert "no is_base_partner column" in structural and "107 accounts (5 held out)" in structural


def test_timeline_drops_undated_events_and_fills_missing_titles():
    tl = pl.DataFrame({"event_date": [date(2026, 5, 1), None, date(2025, 1, 2)], "title": ["A", "B", None],
                       "detail": ["a", "b", "BEPC 345.0 kV, Planned"]})
    out = M.timeline({"triggers": tl})
    assert out["title"].to_list() == ["A", "BEPC 345.0 kV, Planned"]


def test_lcra_supplied_munis_keep_the_fact_but_not_the_trigger():
    gt = pl.DataFrame({"account_id": ["m1", "m2", "m2", "c1"], "gt": ["LCRA", "LCRA", "Brazos", "LCRA"]})
    names = pl.DataFrame({"account_id": ["m1", "m2", "c1"], "account_type": ["muni", "muni", "coop"]})
    assert sorted(M.trigger_gt_links(gt, names).rows()) == [("c1", "LCRA"), ("m2", "Brazos")]
