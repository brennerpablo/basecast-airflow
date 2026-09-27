"""``mart_insights``: the video's numbers for /insights, one card per A or B line of ``docs/analysis/video-candidates.md``.

Every value, caption and caveat is assembled here from the marts already written to Postgres (so a card shows what
the API serves), never copied from the doc; the golden checks compare the values with the doc. A card whose source
mart is not built yet is left out. Queue cards always name their queue (``queue``): the generation queue and the
large-load queue both came to ~438 GW in 2026.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from typing import Any

import polars as pl

from basecast_pipelines.marts.core import Mart, MartContext, value_check

GOLDEN_AS_OF = date(2026, 9, 26)
DOC = "docs/analysis/video-candidates.md"


def _gw(mw: float) -> str:
    return f"{mw / 1000:.1f}"


def _pct(x: float, *, sign: bool = False) -> str:
    return f"{x:+.1f}%" if sign else f"{x:.1f}%"


def _read(ctx: MartContext, mart: str, where: str = "", params: dict | None = None) -> pl.DataFrame | None:
    """A mart's rows, or None when the mart is not built."""
    if ctx.read_sql("select to_regclass(%(t)s)::text as t", {"t": f"public.{mart}"})["t"][0] is None:
        return None
    return ctx.read_sql(f'select * from "{mart}" {where}', params)


def _mape(cells: pl.DataFrame, source: str, paired_with: str | None = None) -> tuple[float, int]:
    """Mean |error_pct| of ``source`` on the (as_of, target_year) cells ``paired_with`` also has."""
    own = cells.filter(pl.col("source") == source)
    if paired_with:
        keys = cells.filter(pl.col("source") == paired_with).select("as_of", "target_year")
        own = own.join(keys, on=["as_of", "target_year"])
    return float(own["error_pct"].abs().mean()), own.height


def _card(id_: str, grade: str, title: str, caption: str, value: float, unit: str, figures: list[dict],
          caveat: str, codes: list[str], queue: str | None, depends_on: list[str], source_doc: str,
          link: str) -> dict[str, Any]:
    return {"id": id_, "grade": grade, "title": title, "caption": caption, "value": value, "unit": unit,
            "figures": json.dumps(figures), "caveat": caveat, "caveat_codes": codes, "queue": queue,
            "verified": grade == "A", "depends_on": depends_on, "source_doc": source_doc, "link": link}


def _fig(label: str, value: float | None, unit: str) -> dict[str, Any]:
    return {"label": label, "value": value, "unit": unit}


# --- the cards (order = the doc's rows) ----------------------------------------------------------------------


def a1(ctx: MartContext) -> dict | None:
    r = _read(ctx, "mart_large_load_realization", "where target_year = 2025")
    if r is None:
        return None
    deck = r.filter(pl.col("deck_vintage").dt.year() == 2024, pl.col("deck_vintage").dt.month() == 10).row(0, named=True)
    promised, approved, energized = deck["promised_mw"], deck["realized_a2e_mw"], deck["realized_energized_mw"]
    return _card(
        "A1", "A", "Large loads promised vs approved",
        f"In {deck['deck_vintage']:%B %Y}, ERCOT's large-load queue showed {_gw(promised)} GW in service by the end "
        f"of 2025; by December 2025, {_gw(approved)} GW had approval to energize, {approved / promised:.0%} of it.",
        promised, "MW", [_fig("Approved to energize, Dec 2025", approved, "MW"),
                         _fig("Approved ÷ promised", approved / promised, "ratio")],
        "Stock-to-stock from ERCOT's chart labels, not project-level tracking. Approval to energize is not "
        f"energized: {energized / approved:.0%} of the approved stock was energized at that date. One deck only.",
        ["machine_read_unverified"], "large_load", ["R3"], "docs/analysis/q5_large_load.md §3",
        "/forecast?tab=large-loads",
    )


def a2(ctx: MartContext) -> dict | None:
    fan = _read(ctx, "mart_backtest_fan", "where target_year = 2026")
    peaks = _read(ctx, "mart_actual_summer_peaks", "where year = 2026")
    if fan is None or peaks is None:
        return None
    prelim = fan.filter(pl.col("kind") == "official_preliminary")["value_mw"].item()
    rng = fan.filter(pl.col("kind") == "official_range").row(0, named=True)
    actual = peaks["hourly_peak_mw"].item()
    q15 = peaks["peak_15min_mw"].item()
    miss = prelim - actual
    over20 = miss - 20_000
    return _card(
        "A2", "A", "ERCOT's preliminary forecast vs the 2026 peak",
        f"ERCOT's preliminary long-term forecast, built from utilities' large-load requests, put summer 2026 near "
        f"{prelim / 1000:.0f} GW; the grid peaked at {_gw(actual)} GW, about {miss / 1000:.0f} GW lower and inside the "
        f"{_gw(rng['low_mw'])}–{rng['high_mw'] / 1000:.0f} GW range ERCOT gave in the same filing.",
        miss, "MW", [_fig("Preliminary forecast", prelim, "MW"), _fig("Actual peak (hourly)", actual, "MW"),
                     _fig("Miss", 100 * miss / actual, "%"), _fig("ERCOT range low", rng["low_mw"], "MW"),
                     _fig("ERCOT range high", rng["high_mw"], "MW")],
        "Say \"preliminary long-term forecast\": ERCOT raised concerns about it in the same letter, and its own range "
        f"held the actual. Only {over20:,.0f} MW of the margin sits above \"20 GW\" ({prelim - q15 - 20_000:,.0f} MW on "
        f"the 15-min peak), and July is not final-settled: an upward revision of about {100 * over20 / actual:.2f}% "
        "breaks \"more than 20 GW\". Say \"about 21 GW\".",
        ["preliminary_actuals"], None, ["R6"], "docs/analysis/q1_backtest.md §4", "/backtest",
    )


def b1(ctx: MartContext) -> dict | None:
    cells = _read(ctx, "mart_peak_backtest")
    fan = _read(ctx, "mart_backtest_fan", "where target_year = 2026")
    if cells is None or fan is None:
        return None
    base = cells.filter(pl.col("source") == "basecast")
    c = base.filter(pl.col("target_year") == 2026).sort("as_of").row(-1, named=True)
    prelim = fan.filter(pl.col("kind") == "official_preliminary")["value_mw"].item()
    coverage = base["in_band"].mean()
    return _card(
        "B1", "B", "Our model, rebuilt as of May 31, 2026",
        f"Rebuilt with only what was public on {c['as_of']:%B %-d, %Y}, our model put this summer's peak at "
        f"{_gw(c['p50_mw'])} GW (range {_gw(c['p10_mw'])}–{_gw(c['p90_mw'])}); the grid peaked at "
        f"{_gw(c['actual_mw'])}, while ERCOT's preliminary forecast six weeks earlier said {prelim / 1000:.0f}.",
        c["p50_mw"], "MW", [_fig("P10", c["p10_mw"], "MW"), _fig("P90", c["p90_mw"], "MW"),
                            _fig("Actual", c["actual_mw"], "MW"), _fig("Error", c["error_pct"], "%"),
                            _fig("ERCOT preliminary", prelim, "MW")],
        "2026 actual is preliminary. The method was designed in Sep 2026 with 2023–2026 in view (X7's leak list). "
        f"The band covered {coverage:.0%} of the backtest cells, not 80%: don't call it an 80% interval.",
        ["preliminary_actuals", "band_uncalibrated", "machine_read_unverified"], None, ["R13", "R3", "R6"],
        "docs/analysis/x7_peak_forecast.md §3", "/backtest",
    )


def b3(ctx: MartContext) -> dict | None:
    q = _read(ctx, "mart_queue_adjusted_county")
    bt = _read(ctx, "mart_queue_backtest", "where stratum = 'all'")
    if q is None or bt is None:
        return None
    allq = q.filter(pl.col("stratum") == "all")
    raw, a27, a28, n = (allq[c].sum() for c in ("raw_mw", "adj_mw_2027", "adj_mw_2028", "projects"))
    gas27 = q.filter(pl.col("stratum") == "gas_other")["adj_mw_2027"].sum()
    errs = bt.sort("report_month")["error_pct"].to_list()
    return _card(
        "B3", "B", "The generation queue, adjusted",
        f"ERCOT's generation queue lists {raw / 1000:.0f} GW, but our model expects about {a27 / 1000:.0f} GW of it to "
        f"reach commercial operation by the end of 2027 and {a28 / 1000:.0f} GW by the end of 2028; run from past queue "
        f"snapshots, the same method landed within about {max(abs(e) for e in errs):.0f}% of what was built over the "
        "next two years.",
        a27, "MW", [_fig("Projects", n, "projects"), _fig("Raw generation queue", raw, "MW"),
                    _fig("Expected by Dec 2028", a28, "MW"),
                    *[_fig(f"Backtest error, {m:%b %Y} queue", e, "%")
                      for m, e in zip(bt.sort("report_month")["report_month"], errs, strict=True)]],
        "Always say \"generation queue\": ERCOT's large-load queue also came to ~438 GW in 2026. The backtest covers "
        f"{len(errs)} snapshots, which also picked the model, and its windows overlap. Dec 2028 is beyond the 24-month "
        f"window tested. {_gw(gas27)} GW of the Dec 2027 number is gas and other, mostly large projects beyond the "
        "fit history.",
        ["beyond_backtested_window"], "generation", ["R4", "R10"], "docs/analysis/x2_adjusted_queue.md §1–3",
        "/explorer?layer=queue",
    )


def b4(ctx: MartContext) -> dict | None:
    fan = _read(ctx, "mart_backtest_fan", "where target_year = 2026")
    if fan is None:
        return None
    actual = fan.filter(pl.col("kind") == "actual")["value_mw"].item()
    ltlf = fan.filter(pl.col("vintage") == "LTLF 2025")
    tsp = ltlf.filter(pl.col("label").str.contains("TSP"))["value_mw"].item()
    adj = ltlf.filter(pl.col("label").str.contains("ERCOT-adjusted"))["value_mw"].item()
    return _card(
        "B4", "B", "Utilities' filings vs ERCOT's adjustment",
        f"In April 2025, ERCOT's long-term forecast put summer 2026 at {tsp / 1000:.0f} GW with utilities' large-load "
        f"filings taken as submitted, and at {_gw(adj)} GW after ERCOT's own adjustment; the grid peaked at "
        f"{_gw(actual)} GW.",
        tsp, "MW", [_fig("TSP-provided vs actual", 100 * (tsp / actual - 1), "%"),
                    _fig("ERCOT-adjusted", adj, "MW"), _fig("ERCOT-adjusted vs actual", 100 * (adj / actual - 1), "%"),
                    _fig("Actual", actual, "MW")],
        "\"TSP-provided\" is a sensitivity ERCOT published, not its adopted forecast. Both are normal-weather numbers, "
        "and 2026 is preliminary.",
        ["preliminary_actuals"], None, ["R6"], "docs/analysis/q1_backtest.md §4", "/backtest",
    )


def b5(ctx: MartContext) -> dict | None:
    cells = _read(ctx, "mart_peak_backtest")
    if cells is None:
        return None
    new = cells.filter(pl.col("era") == "with_tsp_loads")
    old = cells.filter(pl.col("era") == "before_tsp_loads")
    ours_l, n_l = _mape(new, "basecast", "LTLF")
    ltlf, _ = _mape(new, "LTLF", "basecast")
    ours_c, n_c = _mape(new, "basecast", "CDR")
    cdr, _ = _mape(new, "CDR", "basecast")
    ours_o, n_o = _mape(old, "basecast", "LTLF")
    ltlf_o, _ = _mape(old, "LTLF", "basecast")
    ours_all, n_all = _mape(cells, "basecast", "LTLF")
    ltlf_all, _ = _mape(cells, "LTLF", "basecast")
    return _card(
        "B5", "B", "Scored date by date against ERCOT",
        f"Rebuilt date by date since mid-2024, our forecasts missed summer peaks by {_pct(ours_l)} on average, against "
        f"{_pct(ltlf)} for ERCOT's long-term forecast on the same cases; before ERCOT started taking the large-load "
        f"filings in, ERCOT was slightly better ({_pct(ltlf_o)} vs {_pct(ours_o)}).",
        ours_l, "%", [_fig(f"LTLF, same {n_l} cells", ltlf, "%"), _fig(f"Ours vs CDR, {n_c} cells", ours_c, "%"),
                      _fig(f"CDR, same {n_c} cells", cdr, "%"), _fig(f"Earlier: ours, {n_o} cells", ours_o, "%"),
                      _fig(f"Earlier: LTLF, {n_o} cells", ltlf_o, "%"), _fig(f"Overall ours, {n_all} cells", ours_all, "%"),
                      _fig(f"Overall LTLF, {n_all} cells", ltlf_all, "%")],
        f"Only {cells.filter(pl.col('source') == 'basecast')['target_year'].n_unique()} distinct summers are scored, "
        f"and {cells.filter(pl.col('source') == 'basecast', pl.col('target_year') == 2026).height} of the "
        f"{n_all} cells target 2026, which is preliminary. The cells overlap. The method was designed in 2026 (X7's "
        "leakage list). Keep the \"ERCOT was better before\" half: it is what makes the claim credible.",
        ["preliminary_actuals"], None, ["R13", "R3"], "docs/analysis/x7_peak_forecast.md §3", "/backtest",
    )


def b6(ctx: MartContext) -> dict | None:
    r = _read(ctx, "mart_large_load_realization", "where min_horizon_met and target_year in (2024, 2025)")
    if r is None:
        return None
    m = r.group_by("target_year").agg(pl.len().alias("n"), pl.col("incremental_a2e").median().alias("inc"),
                                      pl.col("gross_a2e").median().alias("gross")).sort("target_year")
    lo, hi = m.row(0, named=True), m.row(1, named=True)
    return _card(
        "B6", "B", "How much of the promised large load gets approved",
        f"Of the new large-load megawatts ERCOT's decks promised a year or more ahead, only {lo['inc']:.0%}–"
        f"{hi['inc']:.0%} had approval to energize by that year's end.",
        hi["inc"], "ratio", [_fig(f"Median incremental ratio {lo['target_year']} ({lo['n']} decks)", lo["inc"], "ratio"),
                             _fig(f"Gross ratio {lo['target_year']}", lo["gross"], "ratio"),
                             _fig(f"Gross ratio {hi['target_year']}", hi["gross"], "ratio")],
        "Stock-to-stock with no project ids, so new entrants push the incremental ratio up. Approval, not "
        "energization. Machine-read chart values.",
        ["machine_read_unverified"], "large_load", ["R3"], "docs/analysis/q5_large_load.md §3",
        "/forecast?tab=large-loads",
    )


def b7(ctx: MartContext) -> dict | None:
    i = _read(ctx, "mart_large_load_in_service", "where in_service_year = 2025")
    if i is None:
        return None
    by = i.group_by("deck_vintage").agg(pl.col("total_mw").max()).sort("deck_vintage")
    first = by.filter(pl.col("deck_vintage").dt.year() == 2024, pl.col("deck_vintage").dt.month() == 10).row(0, named=True)
    last = by.filter(pl.col("deck_vintage").dt.year() == 2025).row(-1, named=True)
    return _card(
        "B7", "B", "The large-load queue slides",
        f"ERCOT's large-load queue mostly slides rather than fails: the MW promised for 2025 fell from "
        f"{_gw(first['total_mw'])} GW in the {first['deck_vintage']:%B %Y} deck to {_gw(last['total_mw'])} GW in "
        f"{last['deck_vintage']:%B %Y}, while the later years' bars grew.",
        last["total_mw"], "MW", [_fig(f"Promised for 2025, {first['deck_vintage']:%b %Y} deck", first["total_mw"], "MW")],
        "This is why a by-year realization ratio understates eventual realization. Machine-read.",
        ["machine_read_unverified"], "large_load", ["R3"], "docs/analysis/q5_large_load.md §3",
        "/forecast?tab=large-loads",
    )


def b8(ctx: MartContext) -> dict | None:
    decks = _read(ctx, "mart_peak_forecast", "where region_id = 'ERCOT'")
    i = _read(ctx, "mart_large_load_in_service", "where in_service_year = 2027")
    r = _read(ctx, "mart_large_load_realization")
    if decks is None or i is None or r is None:
        return None
    pre = decks.filter(pl.col("variant") == "deck_pre_batch_zero")["deck_vintage"][0]
    post = decks.filter(pl.col("variant") == "deck_latest")["deck_vintage"][0]

    def bar(deck, status=None):
        rows = i.filter(pl.col("deck_vintage") == deck)
        return rows["total_mw"].max() if status is None else rows.filter(pl.col("status") == status)["mw"].sum()

    def stock(deck):
        return r.filter(pl.col("deck_vintage") == deck)["base_a2e_mw"].max()

    return _card(
        "B8", "B", "After ERCOT's April 2026 intake",
        f"After ERCOT's April 2026 intake, the large-load megawatts promised by 2027 went from {bar(pre) / 1000:.0f} GW "
        f"to {bar(post) / 1000:.0f} GW, while the stock approved to energize stayed near {stock(post) / 1000:.0f} GW.",
        bar(post), "MW", [_fig(f"Promised by 2027, {pre:%b %Y} deck", bar(pre), "MW"),
                          _fig(f"Approved, {pre:%b %Y}", stock(pre), "MW"), _fig(f"Approved, {post:%b %Y}", stock(post), "MW")],
        f"Machine-read. Most of the jump is \"No Studies Submitted\" ({_gw(bar(pre, 'no_studies_submitted'))} → "
        f"{_gw(bar(post, 'no_studies_submitted'))} GW), plus planning studies approved for 2027 going from "
        f"{bar(pre, 'planning_studies_approved') / 1000:.0f} to {bar(post, 'planning_studies_approved') / 1000:.0f} GW. "
        "ERCOT paused approvals for data centers and crypto ≥ 75 MW on 2026-08-03.",
        ["machine_read_unverified", "policy_pause_2026"], "large_load", ["R3", "R13"],
        "docs/analysis/x7_peak_forecast.md §1–2", "/forecast?tab=large-loads",
    )


def b13(ctx: MartContext) -> dict | None:
    cells = _read(ctx, "mart_peak_backtest")
    if cells is None:
        return None
    organic, n = _mape(cells, "basecast_organic_only")
    ours, _ = _mape(cells, "basecast")
    return _card(
        "B13", "B", "What the large-load layer adds",
        f"Without a large-load layer, our own model would have missed recent summer peaks by {_pct(organic)} on "
        f"average; with it, by {_pct(ours)}.",
        ours, "%", [_fig(f"Organic only, {n} cells", organic, "%")],
        "Same leakage caveats as the date-by-date score. \"Organic only\" is our own trend + weather model, not ERCOT's.",
        ["preliminary_actuals"], None, ["R13"], "docs/analysis/x7_peak_forecast.md §3", "/backtest",
    )


CARDS: tuple[Callable[[MartContext], dict | None], ...] = (a1, a2, b1, b3, b4, b5, b6, b7, b8, b13)
INPUTS = ("mart_large_load_realization", "mart_large_load_in_service", "mart_backtest_fan", "mart_actual_summer_peaks",
          "mart_peak_backtest", "mart_queue_adjusted_county", "mart_queue_backtest", "mart_peak_forecast")
SCHEMA = {"id": pl.Utf8, "grade": pl.Utf8, "rank": pl.Int32, "title": pl.Utf8, "caption": pl.Utf8,
          "value": pl.Float64, "unit": pl.Utf8, "figures": pl.Utf8, "caveat": pl.Utf8,
          "caveat_codes": pl.List(pl.Utf8), "queue": pl.Utf8, "verified": pl.Boolean,
          "depends_on": pl.List(pl.Utf8), "source_doc": pl.Utf8, "link": pl.Utf8}
ORDER = ["A1", "A2", *[f"B{i}" for i in range(1, 14)]]


def build_insights(ctx: MartContext) -> pl.DataFrame:
    cards = [c for f in CARDS if (c := f(ctx)) is not None]
    cards.sort(key=lambda c: ORDER.index(c["id"]))
    rows = [{**c, "rank": i + 1} for i, c in enumerate(cards)]
    return pl.DataFrame(rows, schema=SCHEMA)


def _v(id_: str):
    return lambda f: f.filter(pl.col("id") == id_)["value"].item()


def _golden(name, id_, expected, tol):
    return value_check(name, _v(id_), expected, as_of=GOLDEN_AS_OF, tol=tol)


def _queue_named(f: pl.DataFrame) -> bool:
    queue_cards = {"A1": "large_load", "B3": "generation", "B6": "large_load", "B7": "large_load",
                   "B8": "large_load", "B9": "generation"}
    got = dict(zip(f["id"], f["queue"], strict=True))
    return all(got[k] == v for k, v in queue_cards.items() if k in got)


INSIGHTS = Mart(
    name="mart_insights",
    build=build_insights,
    key=("id",),
    inputs=INPUTS,
    description="The video's numbers (video-candidates.md, grades A and B), each assembled from the marts with its "
    "mandatory caveat, for /insights.",
    checks=(
        value_check("every queue card names its queue", _queue_named, True),
        value_check("only grades A and B", lambda f: set(f["grade"]) <= {"A", "B"}, True),
        _golden("A1 promised for 2025, Oct 2024 deck", "A1", 26_836, 1),
        _golden("A2 miss of the preliminary forecast", "A2", 20_866, 1),
        _golden("B1 May 2026 rebuild P50", "B1", 89_037, 1),
        _golden("B3 generation queue expected by Dec 2027", "B3", 38_689, 1),
        _golden("B4 LTLF 2025 TSP-provided for 2026", "B4", 109_031, 1),
        _golden("B5 our MAPE since mid-2024 (LTLF cells)", "B5", 3.1, 0.05),
        _golden("B6 median incremental ratio 2025", "B6", 0.20, 0.005),
        _golden("B7 promised for 2025, Nov 2025 deck", "B7", 13_400, 1),
        _golden("B8 promised by 2027, Jun 2026 deck", "B8", 201_000, 1),
        _golden("B13 our MAPE, 18 cells", "B13", 3.3, 0.05),
    ),
    json_columns=("figures",),
)

MARTS = (INSIGHTS,)
