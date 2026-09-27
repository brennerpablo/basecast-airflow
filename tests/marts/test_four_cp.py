"""The 4CP marts' pure parts (``basecast_pipelines/marts/four_cp.py``) on tiny synthetic frames (no database)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import core
from basecast_pipelines.marts import four_cp as F
from basecast_pipelines.models import four_cp as FC

CHI = ZoneInfo(FC.LOCAL_TZ)
AS_OF = date(2026, 9, 26)
CAVEATS = {"machine_read_unverified", "preliminary_actuals", "weights_pending_review", "band_uncalibrated",
           "beyond_backtested_window", "allocated_statewide", "by_county_not_point", "by_area_not_homes",
           "requests_not_forecasts", "policy_pause_2026", "optimistic_weather", "simulated"}
RATES = [
    {"year": 2025, "usd_per_kw_year": 68.547, "docket": "57491", "status": "final"},
    {"year": 2026, "usd_per_kw_year": 75.527, "docket": "59080", "status": "pending"},
]


def _utc(local: datetime) -> datetime:
    return local.replace(tzinfo=CHI).astimezone(UTC)


def _monthly_system(rows: list[tuple[date, float, datetime, bool]]) -> pl.DataFrame:
    """``ercot_monthly_peaks``-shaped system 15-minute peaks: (month, MW, local interval end, final flag)."""
    return pl.DataFrame(
        {
            "month": [r[0] for r in rows],
            "region_type": "ercot",
            "region_id": "ERCOT",
            "metric": "peak_15min_mw",
            "value": [r[1] for r in rows],
            "interval_minutes": 15,
            "peak_local": [r[2] for r in rows],
            "peak_ts_utc": [_utc(r[2]) for r in rows],
            "final_settlement": [r[3] for r in rows],
        }
    ).with_columns(pl.col("peak_ts_utc").dt.convert_time_zone("UTC"))


def _hourly_load(days: dict[date, list[float]], zone: str = "ERCOT") -> pl.DataFrame:
    """Hour-ending rows for whole local days: ``ts_utc`` = interval end."""
    rows = []
    for day, values in days.items():
        start = _utc(datetime(day.year, day.month, day.day))
        for i, mw in enumerate(values):
            rows.append({"ts_utc": start + timedelta(hours=i + 1), "operating_date": day, "hour_ending": i + 1,
                         "weather_zone": zone, "mw": mw})
    return pl.DataFrame(rows).with_columns(pl.col("ts_utc").dt.convert_time_zone("UTC"))


# --- inputs ----------------------------------------------------------------------------------------------------


def test_cutoff_last_summer_and_workbook_year():
    assert F.cutoff_utc(date(2026, 7, 1)) == datetime(2026, 7, 2, 5, 0, tzinfo=UTC)  # CDT midnight
    assert F.cutoff_utc(date(2026, 1, 1)) == datetime(2026, 1, 2, 6, 0, tzinfo=UTC)  # CST midnight
    assert F.last_summer(date(2026, 5, 31)) == 2025 and F.last_summer(date(2026, 6, 1)) == 2026
    assert F.workbook_year("FuelMixReport_PreviousYears/IntGenbyFuel2011.xls") == 2011
    assert F.workbook_year("IntGenbyFuel2026-1-.xlsx") == 2026


# --- Q1: intervals ---------------------------------------------------------------------------------------------


def _calendar() -> pl.DataFrame:
    monthly = _monthly_system([
        (date(2010, 6, 1), 60_000.0, datetime(2010, 6, 21, 16, 45), False),  # old workbook: no flag
        (date(2026, 6, 1), 82_900.0, datetime(2026, 6, 18, 17, 0), True),
        (date(2026, 7, 1), 91_263.0, datetime(2026, 7, 22, 17, 30), False),
    ])
    days = {date(2026, 9, 12): [50.0] * 16 + [86_492.0] + [50.0] * 7, date(2026, 9, 13): [60.0] * 24}
    return FC.cp_calendar(FC.cp_intervals(monthly), FC.hourly_cps(_hourly_load(days)))


def test_interval_rows_use_the_contract_names_and_the_final_rule():
    per_cp = pl.DataFrame({"year": [2026], "month": [7], "price_rank": [158]},
                          schema={"year": pl.Int32, "month": pl.Int32, "price_rank": pl.Int32})
    rows = F.interval_rows(_calendar(), per_cp, as_of=AS_OF)
    assert rows.select("year", "month").rows() == [(2010, 6), (2026, 6), (2026, 7), (2026, 9)]
    assert rows["source"].to_list() == ["de_15min", "de_15min", "de_15min", "hourly_provisional"]
    # a past year's value counts as final; this year's follows the workbook's flag; hourly never is
    assert rows["final"].to_list() == [True, True, False, False]
    assert rows["interval_end_local"].to_list() == ["2010-06-21 16:45", "2026-06-18 17:00", "2026-07-22 17:30",
                                                   "2026-09-12 17:00"]
    assert rows["interval_start_local"].to_list()[2:] == ["2026-07-22 17:15", "2026-09-12 16:00"]
    assert rows["interval_end_utc"].to_list()[1] == datetime(2026, 6, 18, 22, 0, tzinfo=UTC)
    # 15:45–17:45 holds 16:30–16:45, 16:45–17:00, 17:15–17:30 and the hour 16:00–17:00
    assert rows["in_window"].to_list() == [True, True, True, True]
    assert rows["mw"].to_list()[-1] == 86_492.0
    assert rows["price_rank"].to_list() == [None, None, 158, None]
    late = F.interval_rows(_calendar(), None, as_of=AS_OF, window=(960, 60))  # 16:00–17:00
    assert late["in_window"].to_list() == [True, True, False, True]


def test_eval_years_end_at_the_last_summer_with_four_15_minute_cps():
    rows = [(date(y, m, 1), 1.0, datetime(y, m, 10, 17, 0), True) for y in (2024, 2025) for m in FC.CP_MONTHS]
    rows += [(date(2026, m, 1), 1.0, datetime(2026, m, 10, 17, 0), True) for m in (6, 7, 8)]
    cps = FC.cp_intervals(_monthly_system(rows))
    assert F.eval_years(cps, first=2024) == range(2024, 2026)
    with pytest.raises(ValueError):
        F.eval_years(cps.filter(pl.col("month") != 9))


# --- Q3: zones -------------------------------------------------------------------------------------------------


def _zone_monthly() -> pl.DataFrame:
    """Two weather zones and a load zone (plus a DC tie) over one summer each, and a load zone before 2011."""
    rows = []

    def add(region_type: str, region: str, year: int, cp: float, ncp: float, energy: float, months=FC.CP_MONTHS):
        for m in months:
            base = {"month": date(year, m, 1), "region_type": region_type, "region_id": region, "interval_minutes": 15,
                    "final_settlement": True, "peak_ts_utc": None}
            rows.append({**base, "metric": "coincident_peak_15min_mw", "value": cp,
                         "peak_local": datetime(year, m, 15, 17, 0)})
            rows.append({**base, "metric": "noncoincident_peak_15min_mw", "value": ncp,
                         "peak_local": datetime(year, m, 16, 16, 30)})
            rows.append({**base, "metric": "energy_mwh", "value": energy, "peak_local": None})

    add("weather_zone", "NCENT", 2012, 90.0, 100.0, 600.0)
    add("weather_zone", "FWEST", 2012, 10.0, 20.0, 400.0)
    add("weather_zone", "NCENT", 2013, 90.0, 100.0, 600.0, months=(6, 7))
    add("weather_zone", "FWEST", 2013, 10.0, 20.0, 400.0, months=(6, 7))
    add("load_zone", "LZ_WEST", 2011, 30.0, 40.0, 100.0)
    add("load_zone", "DC_E", 2011, 5.0, 5.0, 5.0)
    add("load_zone", "LZ_WEST", 2010, 30.0, 40.0, 100.0)
    return pl.DataFrame(rows).with_columns(pl.col("peak_ts_utc").cast(pl.Datetime("us", "UTC")))


def test_zone_rows_weather_and_load_zones_with_intensity():
    z = F.zone_rows(_zone_monthly())
    assert z.columns == list(F.ZONE_COLUMNS)
    assert z.select("region_type", "region_id", "year").rows() == [
        ("load_zone", "LZ_WEST", 2011),  # 2010 dropped (load zones from 2011), DC ties never in
        ("weather_zone", "FWEST", 2012), ("weather_zone", "FWEST", 2013),
        ("weather_zone", "NCENT", 2012), ("weather_zone", "NCENT", 2013),
    ]
    nc = z.filter((pl.col("region_id") == "NCENT") & (pl.col("year") == 2012)).row(0, named=True)
    assert nc["cf_summer"] == pytest.approx(0.9)
    assert nc["share_4cp"] == pytest.approx(0.9) and nc["share_energy"] == pytest.approx(0.6)
    assert nc["intensity"] == pytest.approx(1.5)
    assert nc["ncp_end_hour"] == pytest.approx(16.5)
    assert nc["complete"] and nc["n_months"] == 4
    assert z.filter(pl.col("year") == 2013)["complete"].to_list() == [False, False]
    lz = z.filter(pl.col("region_id") == "LZ_WEST").row(0, named=True)
    assert lz["share_4cp"] == pytest.approx(1.0)  # the DC tie is out of the denominators


# --- Q2: dispatch curve ----------------------------------------------------------------------------------------

PEAKS = [150.0, 100.0, 100.0, 100.0, 200.0, 100.0, 100.0, 100.0, 100.0, 100.0]  # the CP on day 5


def _dispatch_inputs() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    days, cps = [], []
    for y in (2020, 2021):
        for m in FC.CP_MONTHS:
            for d, p in enumerate(PEAKS, start=1):
                days.append({"operating_date": date(y, m, d), "peak_mw": p})
            end = 1050 if (y, m) == (2021, 9) else 1020  # one CP ends 17:30: the 1 h 16:00–17:00 misses it
            cps.append({"year": y, "month": m, "cp_date": date(y, m, 5), "end_min": end, "interval_minutes": 15,
                        "source": "15min"})
    daily = FC.with_reference(pl.DataFrame(days).with_columns(
        pl.col("operating_date").dt.year().cast(pl.Int32).alias("year"),
        pl.col("operating_date").dt.month().cast(pl.Int32).alias("month"),
    )).with_columns(pl.col("peak_mw").alias("forecast_wx"))
    sys_wx = daily.select(pl.col("operating_date").alias("date"), (pl.col("peak_mw") / 10).alias("t_max"),
                          (pl.col("peak_mw") / 12).alias("t_mean"))
    cps = pl.DataFrame(cps).with_columns(pl.col("year").cast(pl.Int32), pl.col("month").cast(pl.Int32))
    return daily, sys_wx, cps


WINDOWS2 = {F.MAIN: F.WINDOW, "1h 16:00–17:00 (HE 17)": (960, 60)}


def test_dispatch_grid_scores_every_rule_and_averages_the_draws():
    daily, sys_wx, cps = _dispatch_inputs()
    grid = F.dispatch_grid(daily, sys_wx, cps, range(2020, 2022), xs=(0.96, 1.0), sigmas=(0.0, 0.02), draws=2,
                           top_ns=(1, 2), windows=WINDOWS2)
    assert grid.height == 2 * 2 * 3 + 2 * 3 * 2  # (weather, σ 0, σ 2%) × X × window + top-N × 3 scores × window
    assert grid.select("forecast", "param", "window").is_duplicated().sum() == 0
    noisy = grid.filter(pl.col("forecast") == "actual × noise σ=2%")
    assert noisy["draws"].to_list() == [2] * 4 and set(grid["n_summers"].to_list()) == {2}
    main = grid.filter((pl.col("forecast") == F.WEATHER) & (pl.col("param") == 0.96) & (pl.col("window") == F.MAIN))
    # month start + the day-5 peak: 2 days a month, 8 a summer; every CP caught
    assert main.select("dispatch_days", "all4_rate", "month_rate").row(0) == (8.0, 1.0, 1.0)
    one_h = grid.filter((pl.col("forecast") == F.WEATHER) & (pl.col("param") == 0.96)
                        & (pl.col("window") == "1h 16:00–17:00 (HE 17)"))
    assert one_h.select("all4_rate", "month_rate").row(0) == (0.5, 7 / 8)
    top1 = grid.filter((pl.col("forecast") == "top-N by hottest t_max") & (pl.col("param") == 1.0)
                       & (pl.col("window") == F.MAIN))
    assert top1.select("dispatch_days", "all4_rate").row(0) == (4.0, 1.0)


def test_headline_is_the_grids_headline_row():
    daily, sys_wx, cps = _dispatch_inputs()
    years = range(2020, 2022)
    head = F.headline(daily, cps, years)
    grid = F.dispatch_grid(daily, sys_wx, cps, years, xs=(0.96,), sigmas=(), top_ns=(), windows={F.MAIN: F.WINDOW})
    row = grid.row(0, named=True)
    assert {k: head[k] for k in ("dispatch_days", "all4_rate", "month_rate", "day_rate")} == {
        k: row[k] for k in ("dispatch_days", "all4_rate", "month_rate", "day_rate")}
    with pytest.raises(ValueError):
        F.headline(daily, cps, years, rule=("top-N by hottest t_max", 2, F.MAIN))


def test_curve_rows_label_rules_windows_and_the_headline():
    daily, sys_wx, cps = _dispatch_inputs()
    years = range(2020, 2022)
    grid = F.dispatch_grid(daily, sys_wx, cps, years, xs=(0.96,), sigmas=(0.0,), top_ns=(2,), windows=WINDOWS2)
    rows = F.curve_rows(grid, years)
    by = {(r["forecast"], r["window"]): r for r in rows.iter_rows(named=True)}
    head = by[(F.WEATHER, F.MAIN)]
    assert head["headline"] and head["operable"] and head["optimistic"] and head["label"] == F.OPTIMISTIC
    assert (head["rule"], head["window_start_local"], head["window_end_local"], head["window_hours"]) == (
        "threshold", "15:45", "17:45", 2.0)
    assert head["summers"] == "2020–2021"
    perfect = by[("actual × noise σ=0%", F.MAIN)]
    assert not perfect["optimistic"] and perfect["label"] is None and perfect["operable"]
    top = by[("top-N by weather-model peak", "1h 16:00–17:00 (HE 17)")]
    assert top["rule"] == "top_n" and not top["operable"] and top["optimistic"]
    assert top["window_start_local"] == "16:00" and top["window_end_local"] == "17:00"
    assert not by[("top-N by actual peak (hindsight)", F.MAIN)]["optimistic"]
    assert int(rows["headline"].sum()) == 1


# --- Q4: scarcity ----------------------------------------------------------------------------------------------

DAYS = [date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)]


def _scarcity_inputs():
    """Three July days. Load 100.5 + 0.01 k by hour, 300 MW at July 2 HE 17 (the CP) and 250 MW at July 3 HE 21.
    Wind 1 MWh every 15 minutes; solar 50 MWh in hours ending 10–19. Prices 20 + 0.001 i, 30 at the CP interval
    and 25 intervals at the 5,000 cap from July 3 17:45 (tied: rank 1)."""
    values = {}
    k = 0
    for day in DAYS:
        values[day] = []
        for he in range(1, 25):
            mw = 100.5 + 0.01 * k
            k += 1
            if (day, he) == (DAYS[1], 17):
                mw = 300.0
            if (day, he) == (DAYS[2], 21):
                mw = 250.0
            values[day].append(mw)
    load = _hourly_load(values)
    fuel, price = [], []
    i = 0
    cap_from = datetime(2024, 7, 3, 17, 45)
    for day in DAYS:
        for q in range(96):
            local = datetime(day.year, day.month, day.day) + timedelta(minutes=15 * q)
            start = _utc(local)
            fuel.append({"interval_start_utc": start, "fuel": "wind", "generation_mwh": 1.0})
            fuel.append({"interval_start_utc": start, "fuel": "solar",
                         "generation_mwh": 50.0 if 9 <= local.hour <= 18 else 0.0})
            p = 20 + 0.001 * i
            if local == datetime(2024, 7, 2, 16, 45):
                p = 30.0
            if cap_from <= local < cap_from + timedelta(minutes=15 * 25):
                p = 5000.0
            price.append({"interval_start_utc": start, "price_usd_mwh": p})
            i += 1
    utc = pl.Datetime("us", "UTC")
    fuel = pl.DataFrame(fuel).with_columns(pl.col("interval_start_utc").cast(utc))
    price = pl.DataFrame(price).with_columns(pl.col("interval_start_utc").cast(utc))
    cps = FC.cp_intervals(_monthly_system([(date(2024, 7, 1), 310.0, datetime(2024, 7, 2, 17, 0), True)]))
    return cps, load, fuel, price


def test_scarcity_ranks_the_cp_and_counts_price_cap_ties():
    per_cp, per_year = F.scarcity(*_scarcity_inputs(), first=2024)
    cp = per_cp.row(0, named=True)
    assert (cp["year"], cp["month"]) == (2024, 7)
    assert cp["wind_solar_mw"] == pytest.approx(204.0)
    assert cp["cp_net_mw"] == pytest.approx(310.0 - 204.0)
    # net 96 at the CP hour: below the 42 hours without solar (96.5+) and July 3 HE 21 (246)
    assert cp["net_load_rank"] == 43
    assert (cp["price_usd_mwh"], cp["price_rank"], cp["price_month_max"]) == (30.0, 26, 5000.0)
    y = per_year.row(0, named=True)
    assert (y["year"], y["load_peak_mean_he"], y["net_load_peak_mean_he"], y["net_peak_same_day"]) == (2024, 17, 21, 0)
    assert (y["cp_net_load_rank_median"], y["cp_price_rank_median"], y["cp_in_top20_price_share"]) == (43, 26, 0.0)
    # rank("min"): the 25 tied cap intervals all rank 1, so 25 intervals count as the month's "top 20" (X16)
    assert y["top20_price_intervals"] == 25
    assert y["top20_price_after_18h_share"] == pytest.approx(24 / 25)  # 17:45–18:00 is hour ending 18
    assert y["top20_price_he17_18_share"] == pytest.approx(1 / 25)
    assert y["wind_solar_share"] == pytest.approx((3 * 24 * 4 + 3 * 10 * 200) / sum(_scarcity_inputs()[1]["mw"]))
    assert (y["n_cps"], y["months"]) == (1, 1)


# --- rates and the offer ----------------------------------------------------------------------------------------


def test_rate_rows_from_the_config():
    rates = F.rate_rows(RATES)
    assert rates.select("charges_for_year", "docket", "usd_per_mw_yr", "status", "billed_year",
                        "set_on_4cp_summer").rows() == [(2025, "57491", 68_547.0, "final", 2026, 2024),
                                                        (2026, "59080", 75_527.0, "pending", 2027, 2025)]
    assert rates["source_url"].to_list() == [F.RATE_SOURCES["57491"], F.RATE_SOURCES["59080"]]
    own = F.rate_rows([{**RATES[0], "source_url": "https://example.test/x"}])
    assert own["source_url"].item() == "https://example.test/x"
    with pytest.raises(ValueError):
        F.rate_rows([{**RATES[0], "status": "proposed"}])


def test_rate_checks_pass_on_the_repo_config():
    config = marts_config.load()
    frame = F.rate_rows(marts_config.value(config, "four_cp.rates"))
    checks = core.run_checks(F.FOUR_CP_RATES, frame, F.GOLDEN_AS_OF, config)
    assert [c["check"] for c in checks if c["status"] != "passed"] == []


def _zones() -> pl.DataFrame:
    """``zone_rows``-shaped weather-zone rows: NCENT complete in 2024–2025, partial in 2026."""
    return pl.DataFrame({
        "region_type": "weather_zone", "region_id": "NCENT", "year": [2024, 2025, 2026],
        "cp_avg_mw": [24_000.0, 25_000.0, 26_000.0], "ncp_summer_mw": [26_000.0, 27_000.0, 28_000.0],
        "cf_summer": [0.92, 0.9259, 0.93], "cf_month": [0.95, 0.95, 0.95], "share_4cp": [0.33, 0.33, 0.33],
        "share_energy": [0.30, 0.30, 0.30], "intensity": [1.1, 1.1, 1.1], "ncp_end_hour": [16.9, 17.0, 17.1],
        "energy_mwh": [1.0, 1.0, 1.0], "n_months": [4, 4, 3], "complete": [True, True, False],
    }).with_columns(pl.col("year").cast(pl.Int32), pl.col("n_months").cast(pl.Int32))


def test_zone_line_is_the_diagnosis_line_without_the_pre_x15_rate_clause():
    line = F.zone_line(_zones(), "NCENT", AS_OF)
    assert line.startswith("4CP (NCENT, 2025): zone load at the CPs 93% of its own peak, 4CP intensity 1.10; ")
    assert "15:45–17:45" in line and "$/kW-yr" not in line
    assert F.zone_line(_zones(), "COAST", AS_OF) is None


OFFER_KEYS = ["zone", "zone_line", "window_start_local", "window_end_local", "dispatch_days", "rates", "note",
              "account_4cp", "verified"]


def test_offer_has_the_agreed_shape():
    o = F.offer("NCENT", "4CP line", {"dispatch_days": 55.75, "all4_rate": 0.9375}, F.rate_rows(RATES))
    assert list(o) == OFFER_KEYS
    assert (o["window_start_local"], o["window_end_local"], o["dispatch_days"]) == ("15:45", "17:45", 55.8)
    assert o["rates"] == [
        {"charges_for_year": 2025, "docket": "57491", "usd_per_mw_yr": 68_547.0, "status": "final",
         "billed_year": 2026},
        {"charges_for_year": 2026, "docket": "59080", "usd_per_mw_yr": 75_527.0, "status": "pending",
         "billed_year": 2027},
    ]
    assert o["note"] == "avoided cost for the co-op, not Base revenue; fleet kW per home not verified"
    assert o["account_4cp"] == {
        "key": "account_4cp_mw", "label": "Account load at the 4CP", "value": None, "unit": "MW",
        "source": "UtilityDataSource", "as_of": None, "note": "not public: needs the co-op's own meter data",
        "simulated": False, "verified": False,
    }
    assert o["verified"] is False  # the 2026 rate is pending
    final = F.offer("NCENT", None, {"dispatch_days": None}, F.rate_rows([RATES[0]]))
    assert final["verified"] is True and final["dispatch_days"] is None


def _seeded_ctx() -> core.MartContext:
    def no_db(*a, **k):
        raise AssertionError("offer_block must not read the database here")

    ctx = core.MartContext(AS_OF, {}, no_db)
    ctx.cache.update({"four_cp.zones": _zones(), "four_cp.headline": {"dispatch_days": 55.75},
                      "four_cp.rates": F.rate_rows(RATES)})
    return ctx


def test_offer_block_is_cached_per_zone_and_returns_copies():
    ctx = _seeded_ctx()
    assert F.offer_block(ctx, None) is None
    a = F.offer_block(ctx, "NCENT")
    assert list(a) == OFFER_KEYS and a["zone"] == "NCENT" and a["dispatch_days"] == 55.8
    assert a["zone_line"].startswith("4CP (NCENT, 2025)")
    a["rates"].clear()
    a["account_4cp"]["value"] = 1.0
    b = F.offer_block(ctx, "NCENT")
    assert len(b["rates"]) == 2 and b["account_4cp"]["value"] is None
    assert "four_cp.offer.NCENT" in ctx.cache
    assert F.offer_block(ctx, "COAST")["zone_line"] is None


# --- declarations ----------------------------------------------------------------------------------------------


def test_marts_are_declared_for_the_contract():
    assert [m.name for m in F.MARTS] == ["mart_four_cp_intervals", "mart_four_cp_zone", "mart_four_cp_dispatch_curve",
                                         "mart_four_cp_scarcity", "mart_four_cp_rates"]
    for m in F.MARTS:
        assert set(m.caveats) <= CAVEATS, m.name
        assert m.description and m.key
        assert all(c.as_of in (None, F.GOLDEN_AS_OF) for c in m.checks), m.name
    assert F.FOUR_CP_DISPATCH_CURVE.caveats == ("optimistic_weather",)
    assert set(F.WINDOWS) >= {F.MAIN} and F.WINDOW == (945, 120)
    assert F.HEADLINE[2] in F.WINDOWS and F.HEADLINE[1] in F.XS


def test_printed_tolerance_is_half_the_last_digit_inclusive():
    assert F._printed(0) == pytest.approx(0.5) and F._printed(2) == pytest.approx(0.005)
    check = F._golden("x", lambda f: 0.275, 0.28, tol=F._printed(2))
    assert core.run_checks(core.Mart("mart_t", lambda c: None, ("k",), (), "t", checks=(check,)), pl.DataFrame(),
                           F.GOLDEN_AS_OF)[0]["status"] == "passed"
