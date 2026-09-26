"""Aalen–Johansen and the outcome rules of ``models/survival.py`` on tiny hand-computed fixtures (no DB)."""

from datetime import date

import pytest

np = pytest.importorskip("numpy")
pl = pytest.importorskip("polars")

from basecast_pipelines.models import survival as sv

COD, WD, CENS = sv.COD, sv.WITHDRAWN, sv.CENSORED


def test_aj_two_causes_with_censoring():
    # t=1: n=4, COD -> S=3/4, CIF1=1/4; t=2: n=3, WD -> CIF2=3/4*1/3=1/4, S=1/2;
    # t=3: n=2, COD -> CIF1=1/4+1/2*1/2=1/2, S=1/4; t=4 censored
    fit = sv.fit_cif([1, 2, 3, 4], [COD, WD, COD, CENS])
    assert fit.causes == (COD, WD)
    np.testing.assert_allclose(fit.times, [1, 2, 3])
    np.testing.assert_allclose(fit.at_risk, [4, 3, 2])
    np.testing.assert_allclose(fit.at([0.5, 1, 2.5, 3, 10], COD), [0, 0.25, 0.25, 0.5, 0.5])
    np.testing.assert_allclose(fit.at([0.5, 2, 10], WD), [0, 0.25, 0.25])
    np.testing.assert_allclose(fit.survival, [0.75, 0.5, 0.25])
    # the cumulative incidences and the event-free survival add up to one
    np.testing.assert_allclose(fit.cif[-1].sum() + fit.survival[-1], 1.0)


def test_aj_tied_events_and_censoring_at_the_same_time():
    # t=2: n=5 (the censoring at 2 is still at risk), one COD and one WD -> CIF1=CIF2=1/5, S=3/5;
    # t=3: n=2, COD -> CIF1 = 1/5 + 3/5 * 1/2 = 1/2
    fit = sv.fit_cif([2, 2, 2, 3, 5], [COD, WD, CENS, COD, CENS])
    np.testing.assert_allclose(fit.at_risk, [5, 2])
    np.testing.assert_allclose(fit.at([2, 3], COD), [0.2, 0.5])
    np.testing.assert_allclose(fit.at([3], WD), [0.2])


def test_aj_delayed_entry():
    # at risk on (entry, time]: t=4: n=2 (the third enters at 4.5) -> CIF1=1/2, S=1/2;
    # t=5: n=2 -> CIF1 = 1/2 + 1/2*1/2 = 3/4, S=1/4; t=6: n=1, WD -> CIF2 = 1/4
    fit = sv.fit_cif([4, 5, 6], [COD, COD, WD], entry=[0, 3, 4.5])
    np.testing.assert_allclose(fit.at_risk, [2, 2, 1])
    np.testing.assert_allclose(fit.at([6], COD), [0.75])
    np.testing.assert_allclose(fit.at([6], WD), [0.25])
    # ignoring the truncation gives a different answer: 1/3 + 2/3*1/2 = 2/3
    np.testing.assert_allclose(sv.fit_cif([4, 5, 6], [COD, COD, WD]).at([6], COD), [2 / 3])


def test_aj_weights_equal_repeated_rows():
    weighted = sv.fit_cif([1, 2, 3], [COD, WD, CENS], weights=[3, 1, 2])
    repeated = sv.fit_cif([1, 1, 1, 2, 3, 3], [COD, COD, COD, WD, CENS, CENS])
    np.testing.assert_allclose(weighted.at([1, 2, 3], COD), repeated.at([1, 2, 3], COD))
    np.testing.assert_allclose(weighted.at([1, 2, 3], WD), repeated.at([1, 2, 3], WD))
    np.testing.assert_allclose(weighted.at([1], COD), [0.5])  # 3 of 6


def test_aj_edge_cases():
    fit = sv.fit_cif([1, 2], [CENS, CENS])
    assert fit.times.size == 0
    np.testing.assert_allclose(fit.at([12, 36]), [0, 0])
    np.testing.assert_allclose(sv.fit_cif([1, 2], [WD, CENS]).at([5], COD), [0])  # cause never seen
    fit = sv.fit_cif([1, 2, 3], [COD, CENS, COD])
    np.testing.assert_allclose(fit.n_at_risk([0.5, 2, 3, 4]), [3, 1, 1, 0])
    with pytest.raises(ValueError, match="time > entry"):
        sv.fit_cif([1, 2], [COD, COD], entry=[1, 0])
    with pytest.raises(ValueError, match="same length"):
        sv.fit_cif([1, 2], [COD])
    with pytest.raises(ValueError, match=">= 0"):
        sv.fit_cif([1, 2], [COD, -1])


def _events() -> pl.DataFrame:
    latest = date(2026, 8, 1)
    rows = [
        # inr, exit_status, first_seen, last_seen, entry_date, ia_signed, cod, cancel, last_active
        ("A", "operational", date(2020, 1, 1), date(2022, 6, 1), date(2020, 1, 15), date(2021, 1, 1),
         date(2022, 6, 10), None, date(2022, 5, 1)),
        ("B", "cancelled", date(2020, 1, 1), date(2021, 3, 1), date(2019, 7, 1), None,
         None, date(2021, 3, 5), date(2021, 2, 1)),
        ("C", "dropped", date(2020, 1, 1), date(2020, 12, 1), date(2020, 1, 1), None,
         None, None, date(2020, 12, 1)),
        ("D", "inactive", date(2020, 1, 1), latest, date(2020, 1, 1), date(2019, 1, 1),
         None, None, date(2024, 1, 1)),
        ("E", "active", date(2020, 1, 1), latest, date(2020, 1, 1), None, None, None, latest),
        ("F", "inactive", date(2020, 1, 1), date(2023, 4, 1), date(2020, 1, 1), None,
         None, None, date(2022, 12, 1)),
    ]
    cols = ["inr", "exit_status", "first_seen_month", "last_seen_month", "entry_date", "ia_signed",
            "commercial_operation_date", "cancel_date", "last_active_month"]
    df = pl.DataFrame(rows, schema=cols, orient="row")
    return df.with_columns(
        latest_report_month=pl.lit(latest),
        fis_approved=pl.lit(None, dtype=pl.Date),
        cod_first_month=pl.col("commercial_operation_date").dt.truncate("1mo"),
        exit_month=pl.lit(None, dtype=pl.Date),
        fuel_group=pl.lit("solar"),
    )


def test_outcomes_event_codes_and_dates():
    out = sv.outcomes(_events()).sort("inr")
    assert out["event"].to_list() == [COD, WD, WD, CENS, CENS, WD]
    assert out["vanished"].to_list() == [False, False, True, False, False, True]
    assert out["event_date"].to_list() == [
        date(2022, 6, 10),  # COD date
        date(2021, 3, 5),  # cancel date
        date(2021, 1, 1),  # dropped: end of the last month listed
        date(2026, 9, 1),  # still listed: censored at the end of the latest report month
        date(2026, 9, 1),
        date(2023, 5, 1),  # inactive and gone before the latest report: like dropped
    ]
    censored = sv.outcomes(_events(), vanished="censored").sort("inr")
    assert censored["event"].to_list() == [COD, WD, CENS, CENS, CENS, CENS]
    with pytest.raises(ValueError):
        sv.outcomes(_events(), vanished="drop")


def test_landmark_frame_clock_and_delayed_entry():
    ia = sv.landmark_frame(_events(), "ia_signed").sort("inr")
    assert ia["inr"].to_list() == ["A", "D"]  # only projects that reached the landmark
    a, d = ia.row(0, named=True), ia.row(1, named=True)
    assert a["entry"] == 0.0  # listed before the IA
    assert a["time"] == pytest.approx((date(2022, 6, 10) - date(2021, 1, 1)).days / sv.DAYS_PER_MONTH)
    assert d["entry"] == pytest.approx((date(2020, 1, 1) - date(2019, 1, 1)).days / sv.DAYS_PER_MONTH)
    assert d["event"] == CENS
    entry = sv.landmark_frame(_events(), "entry").sort("inr")
    b = entry.row(1, named=True)
    assert b["entry"] == pytest.approx((date(2020, 1, 1) - date(2019, 7, 1)).days / sv.DAYS_PER_MONTH)
    assert not entry["clipped"].any()
    with pytest.raises(ValueError):
        sv.landmark_frame(_events(), "construction_start")


def test_cif_table_counts():
    table = sv.cif_table(sv.landmark_frame(_events(), "entry"), None, horizons=(120,))
    row = table.row(0, named=True)
    assert (row["n"], row["cod"], row["withdrawn"], row["censored"]) == (6, 1, 3, 2)
    assert 0 < row["cif_120"] < 1


def test_fuel_group():
    df = pl.DataFrame({"fuel_type": ["solar", "wind", "storage", "gas", "oil", "nuclear", None]})
    assert df.select(sv.fuel_group())["fuel_type"].to_list() == [
        "solar", "wind", "storage", "gas", "other", "other", "other"]


def test_merge_small_strata():
    counts = {"solar": (100, 50), "gas": (40, 12), "other": (20, 5), "wind": (35, 11)}
    assert sv.merge_small_strata(counts) == {
        "solar": "solar", "wind": "wind", "gas": "gas+other", "other": "gas+other"}
    # a merged stratum that still fails keeps folding (wind -> solar)
    counts = {"solar": (100, 50), "wind": (10, 2)}
    assert sv.merge_small_strata(counts) == {"solar": "solar+wind", "wind": "solar+wind"}
    # nothing to merge into: the stratum stays as it is
    assert sv.merge_small_strata({"storage": (5, 1)}) == {"storage": "storage"}
