"""Conditional CIF, as-of truncation and scoring of ``models/queue_adjusted.py`` on tiny fixtures (no DB)."""

from datetime import date

import pytest

np = pytest.importorskip("numpy")
pl = pytest.importorskip("polars")

from basecast_pipelines.models import queue_adjusted as qa  # noqa: E402
from basecast_pipelines.models import survival as sv  # noqa: E402

COD, WD, CENS = sv.COD, sv.WITHDRAWN, sv.CENSORED


@pytest.fixture
def fit():
    # q6's hand-computed fit: F_cod = 0, .25 (t>=1), .5 (t>=3); S = .75 (t>=1), .5 (t>=2), .25 (t>=3)
    return sv.fit_cif([1, 2, 3, 4], [COD, WD, COD, CENS])


def test_survival_at_is_a_right_continuous_step(fit):
    np.testing.assert_allclose(qa.survival_at(fit, [0, 0.99, 1, 2.5, 3, 10]), [1, 1, 0.75, 0.5, 0.25, 0.25])


def test_conditional_cod_from_zero_is_the_cif(fit):
    p, clamped = qa.conditional_cod(fit, [0, 0, 0], [0.5, 1, 3])
    np.testing.assert_allclose(p, fit.at([0.5, 1, 3]))
    assert not clamped.any()


def test_conditional_cod_matches_the_product_limit_by_hand(fit):
    # alive at 1.5: three left (WD at 2, COD at 3, censored at 4). P(COD by 3.5) = (1 - 1/3) * 1/2 = 1/3,
    # and (F(3.5) - F(1.5)) / S(1.5) = (.5 - .25) / .75 = 1/3
    p, _ = qa.conditional_cod(fit, [1.5], 2.0)
    np.testing.assert_allclose(p, [1 / 3])
    # a window with no COD step in it gives 0; a window that only covers the WD step gives 0 too
    p, _ = qa.conditional_cod(fit, [1.0, 1.5], [0.5, 1.0])
    np.testing.assert_allclose(p, [0.0, 0.0])
    # alive at 2.5: two left (COD at 3, censored at 4): P(COD by 3) = 1/2 = (.5 - .25) / .5
    p, _ = qa.conditional_cod(fit, [2.5], 0.5)
    np.testing.assert_allclose(p, [0.5])


def test_conditional_cod_vectorized_horizons_and_bounds(fit):
    p, _ = qa.conditional_cod(fit, [1.5, 1.5], [2.0, 100.0])
    np.testing.assert_allclose(p, [1 / 3, 1 / 3])  # nothing after t=3 moves F
    assert ((p >= 0) & (p <= 1)).all()
    with pytest.raises(ValueError):
        qa.conditional_cod(fit, [-1.0], 1.0)


def test_conditional_cod_clamps_past_the_support():
    # 20 subjects: COD at 1..10, censored at 11..20. At risk at t=k is 21-k; support (>=10 at risk) ends at 10.
    t = np.arange(1, 21, dtype=float)
    e = np.where(t <= 10, COD, CENS)
    fit = sv.fit_cif(t, e)
    end = qa.support_end(fit, 10)
    assert end == 10.0
    # e + h = 13 > 10: the clock moves back to 10 - 3 = 7, i.e. the window (7, 10]
    p, clamped = qa.conditional_cod(fit, [10.0], 3.0, end=end)
    ref, _ = qa.conditional_cod(fit, [7.0], 3.0)
    assert clamped.tolist() == [True]
    np.testing.assert_allclose(p, ref)
    # alive at 7: 13 left, 3 COD in (7, 10] -> 3/13
    np.testing.assert_allclose(ref, [3 / 13])
    # a horizon longer than the support starts the clock at 0
    p, clamped = qa.conditional_cod(fit, [4.0], 12.0, end=end)
    np.testing.assert_allclose(p, fit.at([12.0]))
    assert clamped.all()


def _events(**over):
    base = dict(
        inr=["A", "B", "C", "D"],
        exit_status=["operational", "cancelled", "active", "operational"],
        first_seen_month=[date(2020, 1, 1)] * 3 + [date(2023, 1, 1)],
        last_seen_month=[date(2022, 5, 1), date(2021, 5, 1), date(2024, 12, 1), date(2024, 1, 1)],
        latest_report_month=[date(2024, 12, 1)] * 4,
        last_active_month=[date(2022, 5, 1), date(2021, 3, 1), date(2024, 12, 1), date(2024, 1, 1)],
        commercial_operation_date=[date(2022, 5, 10), None, None, date(2024, 1, 5)],
        cod_first_month=[date(2022, 5, 1), None, None, date(2024, 1, 1)],
        exit_month=[date(2022, 5, 1), date(2021, 4, 1), None, date(2024, 1, 1)],
        cancel_date=[None, date(2021, 4, 2), None, None],
        entry_date=[date(2020, 1, 1)] * 3 + [date(2023, 1, 1)],
        fis_approved=[date(2020, 6, 1), None, date(2021, 12, 1), None],
        ia_signed=[date(2021, 1, 15), None, date(2021, 12, 20), date(2023, 3, 1)],
        ia_first_month=[date(2021, 2, 1), None, date(2022, 3, 1), date(2023, 3, 1)],
        approved_synchronization=[date(2022, 3, 1), None, None, date(2023, 11, 1)],
        synchronization_first_month=[date(2022, 3, 1), None, None, date(2023, 11, 1)],
        fuel_group=["solar", "storage", "gas", "other"],
    )
    base.update(over)
    return pl.DataFrame(base)


def test_truncate_at_censors_later_events_and_hides_unreported_milestones():
    out = qa.truncate_at(_events(), date(2022, 1, 1))
    by = {r["inr"]: r for r in out.iter_rows(named=True)}
    assert "D" not in by  # first listed after the as-of date
    assert by["A"]["event"] == CENS and by["A"]["event_date"] == date(2022, 1, 1)
    assert by["B"]["event"] == WD  # cancelled before the as-of date
    assert by["C"]["event"] == CENS
    # C's IA is dated before the as-of date but first reported in 2022-03: not known yet
    assert by["C"]["ia_signed"] is None and by["C"]["fis_approved"] == date(2021, 12, 1)
    # A's synchronization comes after the as-of date
    assert by["A"]["approved_synchronization"] is None and by["A"]["ia_signed"] == date(2021, 1, 15)
    later = qa.truncate_at(_events(), date(2025, 1, 1))
    assert later.filter(pl.col("inr") == "A")["event"].item() == COD


def test_current_stage_later_stage_wins_even_when_dated_earlier():
    df = pl.DataFrame({
        "entry_date": [date(2020, 1, 1)] * 4,
        "fis_approved": [None, date(2021, 1, 1), date(2022, 6, 1), date(2021, 1, 1)],
        "ia_signed": [None, None, date(2021, 6, 1), date(2021, 6, 1)],
        "approved_synchronization": [None, None, None, date(2023, 1, 1)],
    })
    stages = qa.STAGE_SETS["entry_fis_ia_sync"]
    out = df.with_columns(stage=qa.current_stage(stages)).with_columns(d=qa.stage_date(stages))
    assert out["stage"].to_list() == ["entry", "fis_approved", "ia_signed", "synchronized"]
    assert out["d"].to_list() == [date(2020, 1, 1), date(2021, 1, 1), date(2021, 6, 1), date(2023, 1, 1)]
    # with an as-of date, a later milestone does not count yet
    early = df.with_columns(stage=qa.current_stage(stages, date(2022, 1, 1)))
    assert early["stage"].to_list() == ["entry", "fis_approved", "ia_signed", "ia_signed"]
    # the two-landmark set sends FIS-only projects to entry
    two = df.with_columns(stage=qa.current_stage(qa.STAGE_SETS["entry_ia"]))
    assert two["stage"].to_list() == ["entry", "entry", "ia_signed", "ia_signed"]


def test_stage_frame_uses_delayed_entry():
    out = sv.outcomes(_events())
    frame = qa.stage_frame(out, "ia_signed").sort("inr")
    assert frame["inr"].to_list() == ["A", "C", "D"]
    d = frame.filter(pl.col("inr") == "D").row(0, named=True)
    assert d["entry"] == 0.0  # listed before its IA
    a = frame.filter(pl.col("inr") == "A").row(0, named=True)
    assert a["event"] == COD
    assert a["time"] == pytest.approx((date(2022, 5, 10) - date(2021, 1, 15)).days / sv.DAYS_PER_MONTH)


def test_score_uses_pooled_curve_when_stratum_missing_and_aggregates():
    t = np.arange(1, 41, dtype=float)
    ev = np.where(t % 2 == 0, COD, WD)
    pooled = sv.fit_cif(t, ev)
    curves = {("entry", qa.POOLED): qa.StageCurve("entry", qa.POOLED, pooled, 40.0, 40, 20)}
    queue = pl.DataFrame({
        "inr": ["x", "y"], "stage": ["entry", "entry"], "stratum": ["solar", "wind"],
        "elapsed": [0.0, 0.0], "capacity_mw": [100.0, 50.0], "county": ["A", "A"],
    })
    scored = qa.score(queue, curves, {"h10": 10.0})
    assert scored["curve"].to_list() == [qa.POOLED, qa.POOLED]
    p = float(pooled.at([10.0])[0])
    np.testing.assert_allclose(scored["mw_h10"].to_numpy(), [100 * p, 50 * p])
    agg = qa.aggregate(scored, "county", ["h10"])
    assert agg["raw_mw"].item() == 150.0
    assert agg["ratio_h10"].item() == pytest.approx(p)


def test_fit_stage_curves_skips_small_strata():
    n = 40
    df = pl.DataFrame({
        "inr": [f"P{i}" for i in range(n)],
        "exit_status": ["operational" if i % 3 == 0 else "active" for i in range(n)],
        "first_seen_month": [date(2019, 1, 1)] * n,
        "last_seen_month": [date(2024, 12, 1)] * n,
        "latest_report_month": [date(2024, 12, 1)] * n,
        "last_active_month": [date(2024, 12, 1)] * n,
        "commercial_operation_date": [date(2020, 1 + i % 12, 1) if i % 3 == 0 else None for i in range(n)],
        "cod_first_month": [None] * n, "exit_month": [None] * n, "cancel_date": [None] * n,
        "entry_date": [date(2019, 1, 1)] * n,
        "capacity_mw": [10.0 + i for i in range(n)],
        "stratum": ["solar"] * 35 + ["wind"] * 5,
    })
    curves = qa.fit_stage_curves(sv.outcomes(df), ["entry"], min_n=30, min_events=5)
    assert ("entry", qa.POOLED) in curves and ("entry", "solar") in curves
    assert ("entry", "wind") not in curves


def test_composed_cod_by_hand():
    # pre-IA sojourn: IA at 1 (n=4 -> dF_IA = 1/4, S = 3/4), WD at 2 (S = 1/2), IA at 3 (dF_IA = 1/2 * 1/2)
    pre = sv.fit_cif([1, 2, 3, 4], [qa.IA_EVENT, WD, qa.IA_EVENT, CENS])
    # IA stage: half reach COD one month after the IA
    ia = sv.fit_cif([1, 2], [COD, CENS])
    # from entry, 2.5 months: only the IA at 1 fits, with 1.5 months left -> 1/4 * 1/2
    p, _ = qa.composed_cod(pre, ia, [0.0], 2.5)
    np.testing.assert_allclose(p, [0.125])
    # alive before the IA at 1.5, 2 months: the IA at 3 leaves 0.5 months, too few for COD
    p, _ = qa.composed_cod(pre, ia, [1.5], 2.0)
    np.testing.assert_allclose(p, [0.0])
    # 3 months: P(IA at 3 | alive at 1.5) = 2/3 * 1/2 = 1/3, then COD with 1/2 -> 1/6
    p, _ = qa.composed_cod(pre, ia, [1.5], 3.0)
    np.testing.assert_allclose(p, [1 / 6])


def test_composed_cod_adds_direct_cod_without_ia():
    # no IA at all: the composed chance is the conditional CIF of COD from entry
    pre = sv.fit_cif([1, 2, 3, 4], [COD, WD, COD, CENS])
    ia = sv.fit_cif([1.0], [COD])
    p, _ = qa.composed_cod(pre, ia, [1.5], 2.0)
    ref, _ = qa.conditional_cod(pre, [1.5], 2.0)
    np.testing.assert_allclose(p, ref)


def test_pre_ia_frame_events_and_left_truncation():
    out = sv.outcomes(_events())
    frame = qa.pre_ia_frame(out).sort("inr")
    by = {r["inr"]: r for r in frame.iter_rows(named=True)}
    assert by["A"]["event"] == qa.IA_EVENT and by["B"]["event"] == WD and by["C"]["event"] == qa.IA_EVENT
    assert by["A"]["time"] == pytest.approx((date(2021, 1, 15) - date(2020, 1, 1)).days / sv.DAYS_PER_MONTH)
    # D was listed (2023-01) before its IA (2023-03): observed pre-IA for two months
    assert by["D"]["event"] == qa.IA_EVENT and by["D"]["time"] > by["D"]["entry"]
    late = _events(first_seen_month=[date(2020, 1, 1)] * 3 + [date(2023, 6, 1)])
    assert "D" not in qa.pre_ia_frame(sv.outcomes(late))["inr"].to_list()  # IA before its first listing


def test_fit_stages_semi_markov():
    assert qa.fit_stages("entry_ia") == ("entry", "ia_signed")
    assert qa.fit_stages("entry_ia_sm") == (qa.PRE_IA, "ia_signed")
