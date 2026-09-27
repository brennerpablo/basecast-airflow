"""The load marts' pure parts (``basecast_pipelines/marts/load.py``) on tiny synthetic frames (no database), plus
the annotations config against the repo docs it cites."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

np = pytest.importorskip("numpy")
pl = pytest.importorskip("polars")

from basecast_pipelines.config import PROJECT_ROOT  # noqa: E402
from basecast_pipelines.marts import core  # noqa: E402
from basecast_pipelines.marts import load as L  # noqa: E402
from basecast_pipelines.models import queue_adjusted as qa  # noqa: E402
from basecast_pipelines.models import survival as sv  # noqa: E402

COD, WD, CENS = sv.COD, sv.WITHDRAWN, sv.CENSORED
CAVEATS = {"machine_read_unverified", "preliminary_actuals", "weights_pending_review", "band_uncalibrated",
           "beyond_backtested_window", "allocated_statewide", "by_county_not_point", "by_area_not_homes",
           "requests_not_forecasts", "policy_pause_2026", "optimistic_weather", "simulated"}
# get-data's fixtures (basecast_get_data/data/fixtures/<mart>.json), without the provenance columns
CONTRACT = {
    "mart_load_normalized_monthly": {"weather_zone", "month", "days", "complete", "avg_mw", "avg_norm_mw", "energy_gwh",
                                     "energy_norm_gwh", "peak_mw", "peak_norm_mw", "t_mean_c", "yoy_norm_pct"},
    "mart_load_normalized_annual": {"weather_zone", "year", "energy_gwh", "energy_norm_gwh", "yoy_norm_pct",
                                    "summer_peak_mw", "summer_peak_norm_p10", "summer_peak_norm_p50",
                                    "summer_peak_norm_p90", "complete_through"},
    "mart_queue_stage_curves": {"as_of_month", "stage", "stratum", "weighting", "month", "at_risk", "cif_cod",
                                "cif_withdrawn", "survival", "supported"},
    "mart_annotations": {"date", "title", "detail", "source_url", "verified"},
}


# --- X12 -----------------------------------------------------------------------------------------------------


def _days(zone: str, start: date, end: date, level, ratio: float = 0.9, peak: float = 1.5) -> tuple:
    """Daily rows of one zone: ``dmean`` = ``level(date)``, ``normalized`` = ``ratio`` × it; the peak frame's
    ``dmax`` = ``peak`` × ``dmean`` with its own normalized value."""
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    dmean = [float(level(d)) for d in dates]
    energy = pl.DataFrame({"weather_zone": zone, "date": dates, "n_hours": 24, "t_mean": 20.0, "dmean": dmean,
                           "normalized": [v * ratio for v in dmean]})
    peaks = pl.DataFrame({"weather_zone": zone, "date": dates, "dmax": [v * peak for v in dmean],
                          "normalized": [v * peak * 0.95 for v in dmean]})
    return energy, peaks


def test_monthly_rows_contract_names_and_twelve_month_change():
    e, p = _days("NORTH", date(2020, 1, 1), date(2021, 2, 10), lambda d: 100.0 if d.year == 2020 else 110.0)
    m = L.monthly_rows(e, p)
    assert set(m.columns) >= CONTRACT["mart_load_normalized_monthly"]
    by = {r["month"]: r for r in m.iter_rows(named=True)}
    jan20, jan21, feb21 = by[date(2020, 1, 1)], by[date(2021, 1, 1)], by[date(2021, 2, 1)]
    assert jan20["energy_gwh"] == pytest.approx(100 * 24 * 31 / 1000)
    assert jan20["avg_norm_mw"] == pytest.approx(90.0) and jan20["peak_mw"] == pytest.approx(150.0)
    assert jan20["yoy_norm_pct"] is None and jan20["complete"] and jan20["days"] == 31
    assert jan21["yoy_norm_pct"] == pytest.approx(10.0) and jan21["yoy_pct"] == pytest.approx(10.0)
    assert not feb21["complete"] and feb21["days"] == 10  # the running month
    assert m.schema["days"] == pl.Int64 and m["t_mean_c"].to_list()[0] == pytest.approx(20.0)


def test_monthly_change_reads_the_same_month_a_year_before_not_a_shift():
    e, p = _days("NORTH", date(2020, 1, 1), date(2021, 3, 31), lambda d: 100.0 + d.month)
    gap = [date(2020, 2, 1) <= d < date(2020, 3, 1) for d in e["date"]]
    m = L.monthly_rows(e.filter(~pl.Series(gap)), p.filter(~pl.Series(gap)))
    by = {r["month"]: r for r in m.iter_rows(named=True)}
    assert by[date(2021, 2, 1)]["yoy_norm_pct"] is None  # Feb 2020 missing: no comparison
    assert by[date(2021, 3, 1)]["yoy_norm_pct"] == pytest.approx(0.0)


def _annual_inputs():
    growth = {2016: 1.0, 2017: 1.01, 2018: 1.03, 2019: 1.06, 2020: 1.12, 2021: 1.20}
    e, p = _days("ERCOT", date(2016, 1, 1), date(2021, 3, 31), lambda d: 100.0 * growth[d.year] + d.month / 10)
    m = L.monthly_rows(e, p)
    peaks = pl.DataFrame({"weather_zone": "ERCOT", "year": list(range(2016, 2021)),
                          "p10": [140.0 + y for y in range(5)], "p50": [150.0 + y for y in range(5)],
                          "p90": [160.0 + y for y in range(5)], "mean": 150.0, "weather_years": 20})
    return m, e, p, peaks


def test_annual_rows_yoy_z_ytd_and_summer_peak():
    m, e, p, peaks = _annual_inputs()
    a = L.annual_rows(m, e, p, peaks, ref_years=(2017, 2019))
    assert set(a.columns) >= CONTRACT["mart_load_normalized_annual"]
    by = {r["year"]: r for r in a.iter_rows(named=True)}
    energy = {y: float(e.filter(pl.col("date").dt.year() == y).select((pl.col("normalized") * 24).sum()).item()) / 1000
              for y in range(2016, 2021)}
    yoy = {y: 100 * (energy[y] / energy[y - 1] - 1) for y in range(2017, 2021)}
    assert by[2016]["yoy_norm_pct"] is None and by[2016]["yoy_basis"] is None
    for y in range(2017, 2021):
        assert by[y]["yoy_norm_pct"] == pytest.approx(yoy[y]) and by[y]["yoy_basis"] == L.CALENDAR
    ref = np.array([yoy[y] for y in (2017, 2018, 2019)])
    assert by[2020]["yoy_norm_z"] == pytest.approx((yoy[2020] - ref.mean()) / ref.std(ddof=1))
    # the running year: Jan 1 - Mar 31 2021 vs the same days of the year (X12's day-of-year rule), not a full year
    r = by[2021]
    assert not r["complete"] and r["months"] == 3 and r["complete_through"] == date(2021, 3, 31)
    assert r["yoy_basis"] == L.YTD and r["yoy_norm_z"] is None
    cut = date(2021, 3, 31).timetuple().tm_yday

    def ytd(y):
        return e.filter((pl.col("date").dt.year() == y) & (pl.col("date").dt.ordinal_day() <= cut))["normalized"].sum()

    assert r["yoy_norm_pct"] == pytest.approx(100 * (ytd(2021) / ytd(2020) - 1))
    # summer: the actual June-September daily max and the quantiles of that year
    summer19 = p.filter((pl.col("date").dt.year() == 2019) & pl.col("date").dt.month().is_in([6, 7, 8, 9]))
    assert by[2019]["summer_peak_mw"] == pytest.approx(summer19["dmax"].max())
    assert by[2019]["summer_peak_norm_p50"] == 153.0 and by[2019]["summer_weather_years"] == 20
    assert by[2021]["summer_peak_mw"] is None and by[2021]["summer_peak_norm_p50"] is None
    assert L.yoy_reference(a, (2017, 2019))["ERCOT"]["mean_pct"] == pytest.approx(ref.mean())
    bias = L.p50_bias(a, (2019, 2019))["ERCOT"]
    assert bias == pytest.approx(100 * (153.0 / summer19["dmax"].max() - 1))


# --- X2 stage curves -----------------------------------------------------------------------------------------


def test_composed_from_entry_is_composed_cod_at_zero_for_any_cause():
    pre = sv.fit_cif([1, 2, 3, 4], [qa.IA_EVENT, WD, qa.IA_EVENT, CENS])
    ia = sv.fit_cif([1, 2], [COD, CENS])
    months = [0.0, 0.5, 1.5, 2.5, 3.5, 6.0]
    ref = [qa.composed_cod(pre, ia, [0.0], h)[0][0] for h in months]
    np.testing.assert_allclose(L.composed_from_entry(pre, ia, months, COD), ref)
    # withdrawal by hand at 2.5 months: pre-IA withdrawal 1/4 (at 2), plus the IA at 1 (1/4) then withdrawing
    # one month later with 1/2
    ia_wd = sv.fit_cif([1, 2], [WD, CENS])
    np.testing.assert_allclose(L.composed_from_entry(pre, ia_wd, [2.5], WD), [0.25 + 0.25 * 0.5])


def _outcomes() -> pl.DataFrame:
    """48 projects entered 2019-01-01: solar (24) and storage (20) fit their own curves; wind (4) falls back to the
    pooled one. Every 3rd signs its IA after 3-14 months and half of those reach COD 6-17 months later; every 5th
    of the rest withdraws; the others are still queued on 2024-12-01. One storage project has no capacity."""
    rows = []
    for i in range(48):
        stratum = "solar" if i < 24 else "storage" if i < 44 else "wind"
        start = date(2019, 1, 1)
        ia = cod = None
        if i % 3 == 0:
            ia = start + timedelta(days=30 * (3 + i % 12))
            if i % 2 == 0:
                cod = ia + timedelta(days=30 * (6 + i % 12))
        event = COD if cod else WD if (ia is None and i % 5 == 0) else CENS
        when = cod if cod else start + timedelta(days=30 * (4 + i % 9)) if event == WD else date(2024, 12, 1)
        rows.append({"inr": f"P{i}", "first_seen_month": start, "entry_date": start, "ia_signed": ia,
                     "event": event, "event_date": when, "stratum": stratum,
                     "capacity_mw": None if i == 30 else 10.0 + i})
    return pl.DataFrame(rows, schema_overrides={"event": pl.Int8, "ia_signed": pl.Date})


KW = dict(min_n=5, min_events=2, min_at_risk=3)
PAUSE_URL = "https://www.ercot.com/files/docs/2026/08/25/10.-Large-Load-Issues.zip"  # findings.md, Notes


def test_stage_curve_rows_are_x2s_fits_in_the_contract_names():
    out = _outcomes()
    months = list(range(0, 73, 3))
    rows = L.stage_curve_rows(out, qa.fit_stages("entry_ia_sm"), months=months, **KW)
    assert set(rows.columns) >= CONTRACT["mart_queue_stage_curves"] - {"as_of_month"}
    assert set(rows["stage"].unique()) == {"entry", "ia"}
    assert set(rows["stratum"].unique()) == {"all", "solar", "storage", "wind", "gas_other"}
    assert set(rows["weighting"].unique()) == {"mw", "count"}
    assert rows.height == 2 * 5 * 2 * len(months)
    curve = {r[0]: r[1] for r in rows.filter(pl.col("month") == 0).select("stratum", "curve").unique().iter_rows()}
    assert curve["solar"] == "own" and curve["wind"] == "pooled" and curve["gas_other"] == "pooled"
    # the IA-stage count curve is the Aalen-Johansen fit of the IA landmark frame, untouched
    frame = qa.stage_frame(out, "ia_signed")
    fit = sv.fit_cif(frame["time"].to_numpy(), frame["event"].to_numpy(), frame["entry"].to_numpy())
    ia_all = rows.filter((pl.col("stage") == "ia") & (pl.col("stratum") == "all") & (pl.col("weighting") == "count"))
    np.testing.assert_allclose(ia_all["cif_cod"].to_numpy(), fit.at(months, COD))
    np.testing.assert_allclose(ia_all["at_risk"].to_numpy(), fit.n_at_risk(months))
    assert ia_all["cif_ia"].null_count() == len(months)
    # the entry stage is the semi-Markov composition of the MW-weighted fits the queue is scored with
    curves = qa.fit_stage_curves(out, qa.fit_stages("entry_ia_sm"), weight="capacity_mw", **KW)
    pre, ia = curves[(qa.PRE_IA, "all")], curves[("ia_signed", "all")]
    entry = rows.filter((pl.col("stage") == "entry") & (pl.col("stratum") == "all") & (pl.col("weighting") == "mw"))
    ref = [qa.composed_cod(pre.fit, ia.fit, [0.0], h)[0][0] for h in months]
    np.testing.assert_allclose(entry["cif_cod"].to_numpy(), ref)
    np.testing.assert_allclose(entry["cif_ia"].to_numpy(), pre.fit.at(months, qa.IA_EVENT))
    # at_risk is in projects; the MW fits leave out the project without capacity
    assert entry["n"][0] == 47 and rows.filter((pl.col("stage") == "entry") & (pl.col("weighting") == "count")
                                              & (pl.col("stratum") == "all"))["n"][0] == 48


def test_stage_curve_rows_pass_the_structural_checks_and_cut_support():
    rows = L.stage_curve_rows(_outcomes(), qa.fit_stages("entry_ia_sm"), months=list(range(0, 73)), **KW)
    frame = rows.select(pl.lit(date(2024, 11, 1)).alias("as_of_month"), pl.all())
    checks = core.run_checks(L.QUEUE_STAGE_CURVES, frame, date(2000, 1, 1))
    assert {c["status"] for c in checks if c["check"].startswith("IA signed")} == {"skipped"}
    assert [c for c in checks if c["status"] == "failed"] == []
    ia = rows.filter((pl.col("stage") == "ia") & (pl.col("stratum") == "all") & (pl.col("weighting") == "count"))
    last = ia.filter(pl.col("supported"))["month"].max()
    assert last <= ia["support_end_months"][0] and not ia.filter(pl.col("month") > last)["supported"].any()
    entry = rows.filter((pl.col("stage") == "entry") & (pl.col("stratum") == "all") & (pl.col("weighting") == "count"))
    assert entry.filter(pl.col("supported"))["month"].max() <= last  # entry also needs the IA curve's support


def test_stage_curve_rows_need_the_semi_markov_stage_set():
    with pytest.raises(NotImplementedError):
        L.stage_curve_rows(_outcomes(), ("entry", "ia_signed"), **KW)


# --- annotations ---------------------------------------------------------------------------------------------


def test_annotations_config_cites_urls_written_in_the_repo_docs():
    items = L.load_annotations()
    assert items, "config/annotations.yaml is empty"
    for a in items:
        doc = PROJECT_ROOT / a["doc_ref"]
        assert doc.is_file(), f"{a['id']}: doc_ref {a['doc_ref']} does not exist"
        if a["source_url"] is not None:
            assert a["source_url"] in doc.read_text(), f"{a['id']}: {a['source_url']} is not in {a['doc_ref']}"
        else:
            assert a["verified"] is False
    by = {a["id"]: a for a in items}
    assert by["large_load_pause"]["date"] == date(2026, 8, 3)
    assert by["large_load_pause"]["source_url"] == PAUSE_URL
    assert by["batch_zero_intake"]["source_url"] is None and by["batch_zero_intake"]["verified"] is False


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "annotations.yaml"
    path.write_text(body)
    return path


GOOD = """annotations:
  - {id: a, date: 2026-08-03, kind: large_load_policy, title: T, detail: D, source_url: https://x/y, doc_ref: d.md,
     verified: true}
"""


@pytest.mark.parametrize("body", [
    GOOD.replace("https://x/y", "null"),  # verified without a URL
    GOOD.replace("verified: true", "verified: true, extra: 1"),  # unknown field
    GOOD + GOOD.removeprefix("annotations:\n"),  # duplicate id
    GOOD.replace("2026-08-03", "Aug 2026"),  # not a date
    GOOD.replace("large_load_policy", "rumor"),  # unknown kind
])
def test_load_annotations_rejects_bad_entries(tmp_path, body):
    with pytest.raises(ValueError):
        L.load_annotations(_write(tmp_path, body))


def test_annotation_rows_stop_at_the_as_of_date(tmp_path):
    items = L.load_annotations(_write(tmp_path, GOOD + GOOD.removeprefix("annotations:\n").replace(
        "id: a", "id: b").replace("2026-08-03", "2021-02-15")))
    assert L.annotation_rows(items, date(2026, 8, 2))["id"].to_list() == ["b"]
    rows = L.annotation_rows(items, date(2026, 9, 26))
    assert rows["id"].to_list() == ["b", "a"] and rows["date_precision"].to_list() == ["day", "day"]
    assert set(rows.columns) >= CONTRACT["mart_annotations"]
    real = L.build_annotations(SimpleNamespace(as_of=date(2026, 9, 26)))
    assert [c for c in core.run_checks(L.ANNOTATIONS, real, date(2026, 9, 26)) if c["status"] != "passed"] == []


# --- the marts -----------------------------------------------------------------------------------------------


def test_mart_definitions():
    assert [m.name for m in L.MARTS] == list(CONTRACT)
    for m in L.MARTS:
        assert set(m.caveats) <= CAVEATS, m.name
        assert m.name.startswith("mart_") and m.key
    assert L.QUEUE_STAGE_CURVES.key == ("as_of_month", "stage", "stratum", "weighting", "month")
