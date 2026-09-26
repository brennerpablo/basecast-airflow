"""X13: the TSP RFI by entity type, G&T -> members, allocation and the per-account fact (synthetic, no DB)."""

import polars as pl
import pytest

from basecast_pipelines.models import gt_large_load as G

ENT = {
    "Wires": G.TspEntity("iou", r"^Wires Co", None),
    "Gen": G.TspEntity("gt_coop", r"^Gen Coop", "Gen Electric Coop"),
    "River": G.TspEntity("public_power", r"^River Authority", "River"),
    "City": G.TspEntity("muni", r"^City Power", None),
    G.AGGREGATE_NAME: G.TspEntity("unknown", None, None),
}


def _rfi() -> pl.DataFrame:
    rows = []
    for member in ("A.pptx", "A.pdf"):  # two copies with the same numbers
        for name, y26, y30 in [("Wires", 60, 600), ("Gen", 10, 300), ("River", 20, 50), (G.AGGREGATE_NAME, 10, 50)]:
            rows += [{"name": name, "is_total": False, "year": 2026, "mw": float(y26), "member": member},
                     {"name": name, "is_total": False, "year": 2030, "mw": float(y30), "member": member}]
        rows += [{"name": "Total", "is_total": True, "year": 2026, "mw": 100.0, "member": member},
                 {"name": "Total", "is_total": True, "year": 2030, "mw": 1001.0, "member": member}]
    return pl.DataFrame(rows)


def test_rfi_by_tsp_dedupes_copies_and_checks_total():
    tsp = G.rfi_by_tsp(_rfi())
    assert tsp.height == 10  # 5 names x 2 years, copies collapsed
    chk = G.check_total(tsp)
    assert chk.filter(pl.col("year") == 2026)["diff"].item() == 0
    assert chk.filter(pl.col("year") == 2030)["diff"].item() == -1  # rounding in the filed table


def test_classify_raises_on_unknown_tsp():
    tsp = G.rfi_by_tsp(_rfi().vstack(pl.DataFrame(
        [{"name": "Mystery", "is_total": False, "year": 2030, "mw": 1.0, "member": "A.pptx"}])))
    with pytest.raises(KeyError, match="Mystery"):
        G.classify(tsp, ENT)


def test_by_entity_type_shares_sum_to_one():
    out = G.by_entity_type(G.rfi_by_tsp(_rfi()), ENT, col="group")
    sums = out.group_by("year").agg(pl.col("share").sum())
    assert all(abs(s - 1) < 1e-12 for s in sums["share"])
    y30 = dict(out.filter(pl.col("year") == 2030).select("group", "mw").iter_rows())
    assert y30 == {"IOU": 600.0, "co-op": 300.0, "public power": 50.0, "unknown": 50.0}


def test_co_op_shares_three_readings():
    c = G.co_op_shares(G.rfi_by_tsp(_rfi()).with_columns(
        pl.col("name").replace({"River": "LCRA"})), {**ENT, "LCRA": ENT["River"]}).filter(pl.col("year") == 2030)
    assert c["coop"].item() == pytest.approx(300 / 1000)
    assert c["coop_with_lcra"].item() == pytest.approx(350 / 1000)
    assert c["coop_known"].item() == pytest.approx(300 / 950)


def test_check_segments_statuses():
    members = pl.DataFrame({
        "year": [2025, 2026, 2026, 2026],
        "segment": ["cooperative", "investor_owned_utility", "cooperative", "cooperative"],
        "member_name": ["Wires Co LLC", "Wires Co LLC", "Gen Coop Inc", "River Authority"],
    })
    s = dict(G.check_segments(members, ENT).select("tsp", "status").iter_rows())
    assert s == {"Wires": "agrees", "Gen": "agrees", "River": "override", "City": "not_found"}
    wrong = members.with_columns(pl.lit("municipal").alias("segment"))
    assert G.check_segments(wrong, ENT).filter(pl.col("tsp") == "Gen")["status"].item() == "differs"


def test_gt_members_split_self_and_unmapped():
    acc = pl.DataFrame({
        "account_id": ["1", "2", "3", "4"],
        "name": ["North EC", "Dual EC", "East EC", "CPS Energy"],
        "gt_cooperative": ["Gen Electric Coop", "Gen Electric Coop;River", "Other G&T", None],
    })
    m = G.gt_members(acc, ENT)
    dual = m.filter(pl.col("account_id") == "2")
    assert dual.height == 2 and set(dual["tsp"]) == {"Gen", "River"} and dual["n_gt"].unique().to_list() == [2]
    assert m.filter(pl.col("account_id") == "3")["tsp"].item() is None
    own = m.filter(pl.col("account_id") == "4")
    assert own["tsp"].item() == "CPS" and own["via"].item() == "self"


def test_allocate_splits_two_supplier_members_and_skips_missing_weights():
    gt_mw = pl.DataFrame({"tsp": ["Gen"], "mw": [900.0]})
    w = pl.DataFrame({
        "account_id": ["1", "2", "3", "4"], "tsp": ["Gen"] * 4, "n_gt": [1, 2, 1, 1],
        "weight": [100.0, 100.0, None, 0.0],
    })
    a = G.allocate(gt_mw, w)
    got = dict(a.select("account_id", "mw").iter_rows())
    assert got == pytest.approx({"1": 600.0, "2": 300.0})  # weights 100 and 100/2
    assert a["share"].sum() == pytest.approx(1.0)


def test_method_spread_ratio():
    df = pl.DataFrame({"a": [10.0, 0.0], "b": [40.0, 5.0]})
    out = G.method_spread(df, ["a", "b"])
    assert out["max_over_min"].to_list() == [4.0, None]


def test_exposure_facts_numbers_and_sentence():
    tsp = G.rfi_by_tsp(pl.DataFrame([
        {"name": n, "is_total": t, "year": y, "mw": mw, "member": "A.pptx"}
        for n, t, y, mw in [("Gen", False, 2026, 100.0), ("Gen", False, 2030, 20_000.0), ("Gen", False, 2032, 25_000.0),
                            ("CPS", False, 2026, 0.0), ("CPS", False, 2030, 1_000.0), ("CPS", False, 2032, 1_500.0),
                            ("Total", True, 2030, 100_000.0)]
    ]))
    members = pl.DataFrame({
        "account_id": ["1", "2", "9"], "gt": ["Gen Electric Coop", "Gen Electric Coop", "CPS Energy"],
        "n_gt": [1, 1, 1], "tsp": ["Gen", "Gen", "CPS"], "via": ["g&t", "g&t", "self"],
    })
    f = G.exposure_facts(members, tsp, near=2030, far=2032)
    one = f.filter(pl.col("account_id") == "1").row(0, named=True)
    assert one["share_near"] == pytest.approx(0.2) and one["n_accounts"] == 2
    assert one["fact"].startswith("Your wholesale supplier Gen Electric Coop reported 20.0 GW")
    assert "25.0 GW by 2032" in one["fact"] and "20.0% of the ERCOT-wide RFI" in one["fact"]
    assert f.filter(pl.col("account_id") == "9")["fact"].item().startswith("As a TSP, you reported 1.0 GW")


def test_growth_multiple():
    g = G.growth_multiple(G.rfi_by_tsp(_rfi()), start=2026, end=2030)
    assert dict(g.select("name", "multiple").iter_rows())["Gen"] == pytest.approx(30.0)


def test_sales_shares_drops_delivery_only_and_early_release():
    sales = pl.DataFrame({
        "data_year": [2024] * 5 + [2025],
        "early_release": [False] * 5 + [True],
        "part": ["A", "D", "C", "A", "A", "A"],
        "ownership": ["Cooperative", "Retail Power Marketer", "Investor Owned", "Municipal", None, "Cooperative"],
        "ba_code": ["ERCO"] * 6,
        "sector": ["total"] * 6,
        "sales_mwh": [10.0, 70.0, 500.0, 15.0, 5.0, 99.0],
    })
    s = dict(G.sales_shares(sales, year=2024).select("group", "share").iter_rows())
    assert s == pytest.approx({"co-op": 0.10, "IOU": 0.70, "public power": 0.15, "unknown": 0.05})
