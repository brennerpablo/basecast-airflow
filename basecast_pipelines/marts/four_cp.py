"""4CP marts (A-M5, P1; marts-proposal §3.7) and the 4CP offer of the account detail (A-M3, X3 + X15).

Port of ``analysis/x3_four_cp.py`` (``docs/analysis/x3_four_cp.md``), with the interval label and the rates that
X15 closed (``docs/analysis/x15_source_checks.md``; findings review item 11):

- ``mart_four_cp_intervals``: X3 Q1. ERCOT's 15-minute system peak of each June–September since 2008 from the D&E
  report (``four_cp.cp_intervals``; the time is ERCOT's "Interval ending", X15), the hourly table's highest hour
  for the months the report has not published yet (``hourly_cps``, provisional), whether the 15:45–17:45 window
  covers it, and Q4's scarcity at the interval from 2011 (its hour's net-load rank, the RT hub price and its rank
  in the month). ``final`` is the workbook's final-settlement flag; workbooks before 2016 carry no flag, so a past
  year's value counts as final.
- ``mart_four_cp_zone``: X3 Q3. ``four_cp.zone_coincidence`` for the weather zones (from 2008) and the load zones
  (from 2011, no DC ties), with ``intensity`` = ``share_4cp`` ÷ ``share_energy``.
- ``mart_four_cp_dispatch_curve``: X3 Q2's grid: six discharge windows × rule (a) (dispatch when the forecast peak
  ≥ X × the month-to-date max) on the weather model, on the actual peak with seeded noise (100 draws, seeds 0–99)
  and rule (b) (the top N days of each month), scored over the full summers since 2010 (2010–2025 at 2026-09-26).
  The weather model uses observed ERA5 weather, so it is optimistic (caveat ``optimistic_weather``); rule (b) needs
  the whole month up front (``operable = false``).
- ``mart_four_cp_scarcity``: X3 Q4 per summer since 2011: load vs net-load (load − wind − solar) peak hour, the CPs'
  net-load and price ranks, where the month's 20 highest-priced intervals fall, and the wind + solar share.
  X3 ranks prices with ``rank("min")``, so ties at the price cap can keep more than 20 intervals in a month
  (X16); the port keeps that and publishes the count (``top20_price_intervals``).
- ``mart_four_cp_rates``: the ERCOT postage-stamp rates of ``config/marts.yaml`` (``four_cp.rates``, X15 Q2), in
  $/kW-yr and $/MW-yr; ``billed_year`` = ``charges_for_year`` + 1 (16 TAC 25.192(d): a summer's 4CP bills the
  next year), as the contract reads it.
- :func:`offer_block`: the 4CP offer card of ``/accounts/[id]``, for ``marts/accounts.py``.

X3 reads the fuel mix and the RTM prices from caches under ``analysis/out/``; here both are parsed from the lake's
raw files with the repo's parsers: the newest ``RTMLZHBSPP_<year>`` workbook of each year (as the Explorer marts do,
``HB_HUBAVG`` only) and the newest fuel-mix file of each link (as the ``ercot_fuel_mix_15min`` dataset selects them,
wind and solar only). Every input stops at the end of the run's ``as_of`` day, and X3's scoring summers end at the
latest summer with all four 15-minute CPs published.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts.core import Mart, MartContext, value_check
from basecast_pipelines.models import four_cp as FC

GOLDEN_AS_OF = date(2026, 9, 26)  # X3 and X15's run date

FIRST_YEAR = 2008  # the D&E report's 15-minute peaks start in June 2008
EVAL_FIRST = 2010  # X3 scores the dispatch rules on the summers from 2010
RECENT_FROM = 2021  # X3's "2021–2026" (the recent 15-minute CPs)
LOAD_ZONE_FROM = 2011  # X3 reads the load zones from 2011 (nodal)
SCARCITY_FROM = 2011  # fuel mix and nodal RT prices
WEIGHT_YEARS = range(2003, 2023)  # Q7's zone weights for the system weather (X3)
HUB = "HB_HUBAVG"
FUEL_SOURCE = "ercot_fuel_mix"
FUELS = ("wind", "solar")
TOP_PRICES = 20  # the month's highest-priced intervals
LATE_HE = 19  # hour ending 19 or later = the interval starts at 18:00 or later
SUMMER = len(FC.CP_MONTHS)

# The discharge windows X3 reports: name -> (start, length), minutes after local midnight (four_cp.in_window).
WINDOWS: dict[str, tuple[int, int]] = {
    "1h 16:00–17:00 (HE 17)": (960, 60),
    "1h 16:15–17:15": (975, 60),
    "2h 15:45–17:45": (945, 120),
    "2h 16:00–18:00 (HE 17–18)": (960, 120),
    "4h 15:00–19:00 (HE 16–19)": (900, 240),
    "4h 16:00–20:00 (HE 17–20)": (960, 240),
}
MAIN = "2h 15:45–17:45"  # the offer's window (X3 decision; X15 kept it: the label is interval ending)
WINDOW = WINDOWS[MAIN]
XS = (0.90, 0.92, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00)
SIGMAS = (0.0, 0.02, 0.03, 0.05)
DRAWS = 100
TOP_NS = tuple(range(1, 11))
WEATHER = "weather (ERA5 observed)"
TOP_N = {  # rule (b) label -> the daily column it ranks
    "top-N by weather-model peak": "forecast_wx",
    "top-N by hottest t_max": "t_max",
    "top-N by actual peak (hindsight)": "peak_mw",
}
HEADLINE = (WEATHER, 0.96, MAIN)  # X3's alert rule: 15 of 16 summers at ~56 days
OPTIMISTIC = "optimistic: observed ERA5 weather"
SOURCE_NAMES = {"15min": "de_15min", "hourly": "hourly_provisional"}

BILLING_LAG_YEARS = 1  # 16 TAC 25.192(d)
RATE_STATUSES = {"final", "pending"}
RATE_SOURCES = {  # docket -> the filing X15 quotes the rate from (fetched 2026-09-26); config ``source_url`` wins
    "57491": "https://interchange.puc.texas.gov/Documents/57491_58_1505349.PDF",
    "59080": "https://interchange.puc.texas.gov/Documents/59080_50_1603607.ZIP",
}
OFFER_NOTE = "avoided cost for the co-op, not Base revenue; fleet kW per home not verified"
# diagnosis.zone_outlook still ends its 4CP line with this clause, written before X15 sourced the rates.
STALE_RATE_CLAUSE = "; no $/kW-yr rate in the repo (not verified)"


# --- inputs (read once per run) -----------------------------------------------------------------------------


def _hhmm(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def cutoff_utc(as_of: date) -> datetime:
    """The UTC instant of the local midnight that ends ``as_of``."""
    return datetime.combine(as_of + timedelta(days=1), time(0), tzinfo=ZoneInfo(FC.LOCAL_TZ)).astimezone(UTC)


def last_summer(as_of: date) -> int:
    """The latest summer with any 4CP month by ``as_of`` (the year's price workbook is needed only from June)."""
    return as_of.year if as_of.month >= min(FC.CP_MONTHS) else as_of.year - 1


def monthly_peaks(ctx: MartContext) -> pl.DataFrame:
    """``ercot_monthly_peaks`` June–September rows through the ``as_of`` month."""
    return ctx.cached("four_cp.monthly_peaks",
                      lambda: FC.load_monthly_peaks().filter(pl.col("month") <= ctx.as_of))


def hourly(ctx: MartContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    """``ercot_load_hourly_wz`` and ``weather_hourly_wz`` through ``as_of`` (the full tables are cached under the
    Explorer's keys, so one run reads them once)."""

    def build() -> tuple[pl.DataFrame, pl.DataFrame]:
        from basecast_pipelines.models import weather_load as wl

        load = ctx.cached("weather_load.hourly_load", wl.load_hourly_load)
        weather = ctx.cached("weather_load.hourly_weather", wl.load_hourly_weather)
        return (load.filter(pl.col("operating_date") <= ctx.as_of),
                weather.filter(pl.col("ts_utc") < cutoff_utc(ctx.as_of)))

    return ctx.cached("four_cp.hourly", build)


def _storage():
    from basecast_pipelines.common.storage import storage_from_uri
    from basecast_pipelines.config import load_settings

    return storage_from_uri(load_settings().storage_root)


def hub_prices(ctx: MartContext) -> pl.DataFrame:
    """RTM 15-minute ``HB_HUBAVG`` prices from 2011 through ``as_of`` (``interval_start_utc``,
    ``price_usd_mwh``), parsed from the newest ``RTMLZHBSPP_<year>`` workbook of each year."""

    def build() -> pl.DataFrame:
        from basecast_pipelines.marts.explorer import SPP_SOURCE, newest_by_year
        from basecast_pipelines.parsers.ercot.spp_hist import parse_rtm
        from basecast_pipelines.processing.core import list_raw_files

        frames = []
        years = range(SCARCITY_FROM, last_summer(ctx.as_of) + 1)
        for f in newest_by_year(list_raw_files(_storage(), SPP_SOURCE), years).values():
            df = parse_rtm(f)
            if df is None:
                raise ValueError(f"{f.key}: not an RTM workbook")
            frames.append(df.filter(pl.col("settlement_point") == HUB).select("interval_start_utc", "price_usd_mwh"))
        return (
            pl.concat(frames).unique("interval_start_utc", keep="last")
            .filter(pl.col("interval_start_utc") < cutoff_utc(ctx.as_of))
            .sort("interval_start_utc")
        )

    return ctx.cached("four_cp.hub_prices", build)


def workbook_year(name: str) -> int:
    """The year in a fuel-mix workbook's name (its first four digits, as X3 reads it)."""
    return int("".join(ch for ch in name if ch.isdigit())[:4])


def wind_solar(ctx: MartContext) -> pl.DataFrame:
    """Fuel-mix wind and solar MWh per 15-minute interval from 2011 through ``as_of`` (``interval_start_utc``,
    ``fuel``, ``generation_mwh``), from the newest file of each fuel-mix link."""

    def build() -> pl.DataFrame:
        from basecast_pipelines.parsers.ercot._market_common import workbooks
        from basecast_pipelines.parsers.ercot.fuel_mix import _workbook
        from basecast_pipelines.processing.core import latest_by, list_raw_files

        files = latest_by(lambda f: f.meta.get("link_text") or f.url)(list_raw_files(_storage(), FUEL_SOURCE))
        frames = []
        for f in files:
            if f.suffix not in {".zip", ".xlsx", ".xls"}:
                continue
            for name, data in workbooks(f):
                if workbook_year(name) < SCARCITY_FROM:
                    continue
                wb = _workbook(name, data, f.key)
                frames.append(wb.filter(pl.col("fuel").cast(pl.String).is_in(FUELS)).select(
                    "interval_start_utc", pl.col("fuel").cast(pl.String), "generation_mwh"))
        if not frames:
            raise FileNotFoundError(f"no {FUEL_SOURCE} workbook from {SCARCITY_FROM} in the lake")
        return (
            pl.concat(frames).unique(["interval_start_utc", "fuel"], keep="last")
            .filter(pl.col("interval_start_utc") < cutoff_utc(ctx.as_of))
            .sort("interval_start_utc")
        )

    return ctx.cached("four_cp.wind_solar", build)


# --- Q1: the 4CP intervals ----------------------------------------------------------------------------------


def calendar(ctx: MartContext) -> pl.DataFrame:
    """X3 Q1: the D&E 15-minute CPs, hourly CPs for the months the report lacks, from 2008."""

    def build() -> pl.DataFrame:
        load, _ = hourly(ctx)
        cp15 = FC.cp_intervals(monthly_peaks(ctx))
        return FC.cp_calendar(cp15, FC.hourly_cps(load)).filter(pl.col("year") >= FIRST_YEAR)

    return ctx.cached("four_cp.calendar", build)


def eval_years(cps: pl.DataFrame, first: int = EVAL_FIRST) -> range:
    """X3's scoring summers: ``first`` through the latest summer with all four 15-minute CPs (2010–2025 at
    2026-09-26)."""
    full = (
        cps.filter(pl.col("source") == "15min").group_by("year").len()
        .filter(pl.col("len") == SUMMER)["year"]
    )
    if full.is_empty():
        raise ValueError("no summer with four 15-minute CPs")
    return range(first, int(full.max()) + 1)


def _in_years(years: range) -> pl.Expr:
    return pl.col("year").is_between(years.start, years.stop - 1)


def interval_rows(cps: pl.DataFrame, per_cp: pl.DataFrame | None, *, as_of: date,
                  window: tuple[int, int] = WINDOW) -> pl.DataFrame:
    """The calendar in the contract's names: ``interval_end_local`` (``YYYY-MM-DD HH:MM``, CPT, interval ending),
    ``interval_end_utc``, ``mw``, ``source`` (``de_15min`` | ``hourly_provisional``) and ``final`` (the
    workbook's final-settlement flag, or a past year's 15-minute value; never an hourly value), plus
    ``in_window`` (the whole interval inside ``window``) and ``per_cp``'s scarcity columns by (year, month)."""
    fifteen = pl.col("source") == "15min"
    out = cps.select(
        "year",
        "month",
        pl.col("end_local").dt.strftime("%Y-%m-%d %H:%M").alias("interval_end_local"),
        pl.col("end_utc").alias("interval_end_utc"),
        pl.col("cp_mw").alias("mw"),
        pl.col("source").replace_strict(SOURCE_NAMES, return_dtype=pl.Utf8).alias("source"),
        (fifteen & (pl.col("final_settlement").fill_null(False) | (pl.col("year") < as_of.year))).alias("final"),
        "final_settlement",
        (pl.col("end_local") - pl.duration(minutes=pl.col("interval_minutes"))).dt.strftime("%Y-%m-%d %H:%M")
        .alias("interval_start_local"),
        pl.col("interval_minutes").cast(pl.Int32),
        pl.col("end_min").cast(pl.Int32),
        pl.col("hour_ending").cast(pl.Int32),
        "cp_date",
        pl.col("weekday").cast(pl.Int32),
        FC.in_window(*window).alias("in_window"),
    )
    if per_cp is not None:
        out = out.join(per_cp, on=["year", "month"], how="left")
    return out.sort("year", "month")


def build_intervals(ctx: MartContext) -> pl.DataFrame:
    return interval_rows(calendar(ctx), scarcity_tables(ctx)[0], as_of=ctx.as_of)


def _intervals_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    s, n = WINDOW
    years = eval_years(calendar(ctx))
    past = frame.filter(_in_years(years))
    recent = frame.filter((pl.col("year") >= RECENT_FROM) & (pl.col("source") == "de_15min"))
    return {
        "window_start_local": _hhmm(s),
        "window_end_local": _hhmm(s + n),
        "window_label": f"{FC.window_label(s, n)} CPT",
        "window_coverage": {
            f"{years.start}–{years.stop - 1}": [int(past["in_window"].sum()), past.height],
            f"{RECENT_FROM}–{frame['year'].max()}": [int(recent["in_window"].sum()), recent.height],
        },
        "window_chosen": "in-sample (X3): picked on the same summers it is scored on",
        "interval_label": "interval ending: ERCOT's D&E workbook labels the 15-minute peak time 'Interval ending' "
                          "(X15), so 17:00 is 16:45–17:00 CPT",
        "settlement_match": "X15: ERCOT's settlement 4CP (NP9-83-M) matches the D&E 15-minute peak in 66 of 68 "
                            "months 2008–2025 (all of 2022–2025); NP9-83-M is not in the lake, so not checked here",
        "price_point": HUB,
    }


# --- Q3: zones at the 4CP intervals -------------------------------------------------------------------------

ZONE_COLUMNS = ("region_type", "region_id", "year", "cp_avg_mw", "ncp_summer_mw", "cf_summer", "cf_month",
                "share_4cp", "share_energy", "intensity", "ncp_end_hour", "energy_mwh", "n_months", "complete")


def zone_rows(monthly: pl.DataFrame) -> pl.DataFrame:
    """X3 Q3: ``zone_coincidence`` per weather zone (from 2008) and load zone (from 2011, no ``DC_`` ties), with
    ``intensity`` = ``share_4cp`` ÷ ``share_energy`` and ``complete`` = all four CP months."""
    wz = FC.zone_coincidence(monthly, "weather_zone").filter(pl.col("year") >= FIRST_YEAR)
    lz = FC.zone_coincidence(monthly, "load_zone").filter(
        (pl.col("year") >= LOAD_ZONE_FROM) & ~pl.col("region_id").str.starts_with("DC_")
    )
    return (
        pl.concat([wz.with_columns(pl.lit("weather_zone").alias("region_type")),
                   lz.with_columns(pl.lit("load_zone").alias("region_type"))], how="vertical_relaxed")
        .with_columns(
            pl.when(pl.col("share_energy") > 0).then(pl.col("share_4cp") / pl.col("share_energy")).alias("intensity"),
            pl.col("n_months").cast(pl.Int32),
            (pl.col("n_months") == SUMMER).alias("complete"),
        )
        .select(ZONE_COLUMNS)
        .sort("region_type", "region_id", "year")
    )


def zone_table(ctx: MartContext) -> pl.DataFrame:
    return ctx.cached("four_cp.zones", lambda: zone_rows(monthly_peaks(ctx)))


def build_zone(ctx: MartContext) -> pl.DataFrame:
    return zone_table(ctx)


def _zone_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    return {
        "cf_summer": "mean zone load at the four ERCOT 15-minute CPs ÷ the zone's own highest 15-minute load of "
                     "the summer",
        "intensity": "the zone's share of ERCOT's 4CP load ÷ its share of summer energy (above 1: more "
                     "transmission per MWh)",
        "ncp_end_hour": "mean local clock hour at which the zone's own monthly peaks end (decimal)",
        "complete": "all four CP months published; the current summer is partial until September's D&E report",
    }


# --- Q2: the dispatch curve ---------------------------------------------------------------------------------


def daily_forecast(ctx: MartContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    """X3 Q2: the daily peaks with the month-to-date reference and the weather model's day-ahead forecast
    (``forecast_wx``, fit on the three previous summers), and the system daily weather (``date``, ``t_max``,
    ``t_mean``)."""

    def build() -> tuple[pl.DataFrame, pl.DataFrame]:
        from basecast_pipelines.models import weather_load as wl

        load, weather = hourly(ctx)
        weights = wl.zone_weights(load, WEIGHT_YEARS)
        sys_wx = wl.system_weather_daily(wl.weather_daily(weather), weights).select("date", "t_max", "t_mean")
        daily = FC.weather_forecast(FC.with_reference(FC.daily_peaks(load)), sys_wx,
                                    range(FIRST_YEAR, ctx.as_of.year + 1))
        return daily, sys_wx

    return ctx.cached("four_cp.daily", build)


def _summaries(dispatch: pl.DataFrame, cps: pl.DataFrame, years: range,
               windows: Mapping[str, tuple[int, int]]) -> list[dict[str, Any]]:
    disp = dispatch.filter(_in_years(years))
    return [{"window": w, **FC.summarize_dispatch(FC.evaluate_dispatch(disp, cps, s, n))}
            for w, (s, n) in windows.items()]


def dispatch_grid(daily: pl.DataFrame, sys_wx: pl.DataFrame, cps: pl.DataFrame, years: range, *,
                  xs: Sequence[float] = XS, sigmas: Sequence[float] = SIGMAS, draws: int = DRAWS,
                  top_ns: Iterable[int] = TOP_NS,
                  windows: Mapping[str, tuple[int, int]] = WINDOWS) -> pl.DataFrame:
    """X3 Q2: every rule × parameter × window, scored per summer over ``years`` and averaged (over the noise
    draws too). ``daily``: :func:`daily_forecast`'s frame; ``cps``: the calendar."""
    cp_eval = cps.filter(_in_years(years))
    rows: list[dict[str, Any]] = []

    def add(dispatch: pl.DataFrame, label: str, param: float) -> None:
        rows.extend({"forecast": label, "param": float(param), **r}
                    for r in _summaries(dispatch, cp_eval, years, windows))

    for x in xs:
        add(FC.threshold_dispatch(daily, "forecast_wx", x), WEATHER, x)
    for sigma in sigmas:
        for draw in range(draws if sigma > 0 else 1):
            noisy = FC.noisy_forecast(daily, sigma, seed=draw)
            for x in xs:
                add(FC.threshold_dispatch(noisy, "forecast", x), f"actual × noise σ={sigma:.0%}", x)
    daily_t = daily.join(sys_wx.select(pl.col("date").alias("operating_date"), "t_max"), on="operating_date",
                         how="left")
    for n in top_ns:
        for label, col in TOP_N.items():
            add(FC.top_n_dispatch(daily_t, col, n), label, n)
    return (
        pl.DataFrame(rows)
        .group_by("forecast", "param", "window", maintain_order=True)
        .agg(
            pl.col("dispatch_days").mean(), pl.col("all4_rate").mean(), pl.col("month_rate").mean(),
            pl.col("day_rate").mean(), pl.len().cast(pl.Int32).alias("draws"),
            pl.col("years").max().cast(pl.Int32).alias("n_summers"),
        )
    )


def headline(daily: pl.DataFrame, cps: pl.DataFrame, years: range, rule: tuple[str, float, str] = HEADLINE) -> dict:
    """The headline row of :func:`dispatch_grid` alone (the weather model, X = 96%, 15:45–17:45)."""
    label, x, window = rule
    if label != WEATHER:
        raise ValueError(f"the headline rule is on the weather model, not {label!r}")
    (row,) = _summaries(FC.threshold_dispatch(daily, "forecast_wx", x), cps.filter(_in_years(years)), years,
                        {window: WINDOWS[window]})
    return row


def curve_rows(grid: pl.DataFrame, years: range) -> pl.DataFrame:
    """The grid in the mart's columns: the rule (``threshold`` | ``top_n``), the window's local start and end,
    whether a desk can run the rule day by day (``operable``), the ``optimistic`` label on the ERA5-based rows and
    the ``headline`` row."""
    start = pl.col("window").replace_strict({w: _hhmm(s) for w, (s, _) in WINDOWS.items()}, return_dtype=pl.Utf8)
    end = pl.col("window").replace_strict({w: _hhmm(s + n) for w, (s, n) in WINDOWS.items()}, return_dtype=pl.Utf8)
    hours = pl.col("window").replace_strict({w: n / 60 for w, (_, n) in WINDOWS.items()}, return_dtype=pl.Float64)
    top_n = pl.col("forecast").is_in(list(TOP_N))
    era5 = pl.col("forecast").is_in([WEATHER, "top-N by weather-model peak", "top-N by hottest t_max"])
    label, x, window = HEADLINE
    return grid.select(
        "forecast", "param", "window", "dispatch_days", "all4_rate", "month_rate", "day_rate",
        pl.when(top_n).then(pl.lit("top_n")).otherwise(pl.lit("threshold")).alias("rule"),
        start.alias("window_start_local"),
        end.alias("window_end_local"),
        hours.alias("window_hours"),
        (~top_n).alias("operable"),
        era5.alias("optimistic"),
        pl.when(era5).then(pl.lit(OPTIMISTIC)).otherwise(pl.lit(None, pl.Utf8)).alias("label"),
        ((pl.col("forecast") == label) & ((pl.col("param") - x).abs() < 1e-9) & (pl.col("window") == window))
        .alias("headline"),
        "draws",
        "n_summers",
        pl.lit(f"{years.start}–{years.stop - 1}").alias("summers"),
    ).sort("forecast", "window", "param")


def build_dispatch_curve(ctx: MartContext) -> pl.DataFrame:
    daily, sys_wx = daily_forecast(ctx)
    cps = calendar(ctx)
    years = eval_years(cps)
    return curve_rows(dispatch_grid(daily, sys_wx, cps, years), years)


def headline_dispatch(ctx: MartContext) -> dict:
    """The headline rule's scores (``dispatch_days``, ``all4_rate``...), cheap without the whole grid."""

    def build() -> dict:
        daily, _ = daily_forecast(ctx)
        cps = calendar(ctx)
        return headline(daily, cps, eval_years(cps))

    return ctx.cached("four_cp.headline", build)


def _dispatch_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    head = frame.filter(pl.col("headline")).row(0, named=True)
    label, x, window = HEADLINE
    return {
        "headline": {"forecast": label, "param": x, "window": window, "dispatch_days": head["dispatch_days"],
                     "all4_rate": head["all4_rate"], "month_rate": head["month_rate"]},
        "label": OPTIMISTIC,
        "summers": head["summers"],
        "noise": {"sigmas": list(SIGMAS), "draws": DRAWS, "seeds": f"0–{DRAWS - 1}"},
        "weather_model": "log(peak) ~ year effects + t_max + t_max² + t_mean(d−1) + weekend on the ERCOT-weighted "
                         "ERA5 weather, fit on the three previous summers, level from the previous 7 days' residuals",
        "rule_a": "dispatch day d when the forecast peak ≥ X × the month-to-date max through d−1; the first day of "
                  "each month always dispatches",
        "rule_b": "the top N days of each month by a score: needs the whole month up front (a benchmark)",
        "window_chosen": "in-sample (X3)",
        "fleet_limits": "Base's battery duration, reserve and cycling limits are not verified (X3 review item 3)",
    }


# --- Q4: net load and scarcity ------------------------------------------------------------------------------


def scarcity(cps: pl.DataFrame, load: pl.DataFrame, fuel: pl.DataFrame, price: pl.DataFrame, *,
             first: int = SCARCITY_FROM) -> tuple[pl.DataFrame, pl.DataFrame]:
    """X3 Q4. Returns ``(per_cp, per_year)``:

    - ``per_cp`` (the 15-minute CPs from ``first``, by year and month): ``cp_net_mw`` (the CP MW minus wind and
      solar in the interval), ``net_load_rank`` (the CP hour's rank by net load in its month, 1 = highest),
      ``price_usd_mwh``, ``price_rank`` (the interval's RT price rank in its month, ``rank("min")``) and
      ``price_month_max``;
    - ``per_year``: the mean hour ending of the monthly load and net-load peaks, how many months both fall on the
      same day, the CPs' median ranks and shares in the top 10 net-load hours / top 20 prices, where the month's
      top-20 priced intervals fall (median HE, share in HE 17–18, share after 18:00, and how many there are:
      price-cap ties can make more than 20 a month), and the June–September wind + solar share of load.

    ``fuel``: ``interval_start_utc``, ``fuel`` (wind | solar), ``generation_mwh``; ``price``:
    ``interval_start_utc``, ``price_usd_mwh``."""
    months = FC.CP_MONTHS
    net = FC.net_load_hourly(load, fuel)
    peaks = (
        FC.monthly_argmax(net, "load_mw").join(FC.monthly_argmax(net, "net_mw"), on=["year", "month"])
        .filter(pl.col("year") >= first)
    )
    by_year = peaks.group_by("year").agg(
        pl.col("load_mw_he").mean().alias("load_peak_mean_he"),
        pl.col("net_mw_he").mean().alias("net_load_peak_mean_he"),
        (pl.col("net_mw_date") == pl.col("load_mw_date")).sum().cast(pl.Int32).alias("net_peak_same_day"),
        pl.len().cast(pl.Int32).alias("months"),
    )
    ren = net.filter(pl.col("operating_date").dt.month().is_in(months)).group_by(
        pl.col("operating_date").dt.year().cast(pl.Int32).alias("year")
    ).agg(
        ((pl.col("wind_mw") + pl.col("solar_mw")).sum() / pl.col("load_mw").sum()).alias("wind_solar_share"),
        pl.col("solar_mw").max().alias("solar_max_mw"),
    )

    fuel15 = fuel.pivot(on="fuel", index="interval_start_utc", values="generation_mwh")
    for f in FUELS:
        if f not in fuel15.columns:
            fuel15 = fuel15.with_columns(pl.lit(None, pl.Float64).alias(f))
    cp = (
        cps.filter((pl.col("source") == "15min") & (pl.col("year") >= first))
        .with_columns((pl.col("end_utc") - pl.duration(minutes=15)).alias("interval_start_utc"))
        .join(fuel15.select("interval_start_utc", *FUELS), on="interval_start_utc", how="left")
        .join(price, on="interval_start_utc", how="left")
        .with_columns((pl.col("cp_mw") - 4 * (pl.col("wind") + pl.col("solar"))).alias("cp_net_mw"))
    )
    net_rank = FC.rank_within_month(net, "net_mw").select("ts_utc", "net_mw_rank")
    cp = cp.with_columns(
        (pl.col("interval_start_utc").dt.truncate("1h") + pl.duration(hours=1)).alias("ts_utc")
    ).join(net_rank, on="ts_utc", how="left")
    local = pl.col("_l")
    price_m = price.with_columns(
        pl.col("interval_start_utc").dt.convert_time_zone(FC.LOCAL_TZ).alias("_l")
    ).filter(local.dt.month().is_in(months)).with_columns(
        pl.col("price_usd_mwh").rank("min", descending=True).over(local.dt.year(), local.dt.month())
        .alias("price_rank"),
        pl.col("price_usd_mwh").max().over(local.dt.year(), local.dt.month()).alias("price_month_max"),
    )
    cp = cp.join(price_m.select("interval_start_utc", "price_rank", "price_month_max"), on="interval_start_utc",
                 how="left")
    per_cp = cp.select(
        "year", "month",
        (4 * (pl.col("wind") + pl.col("solar"))).alias("wind_solar_mw"),
        "cp_net_mw",
        pl.col("net_mw_rank").cast(pl.Int32).alias("net_load_rank"),
        "price_usd_mwh",
        pl.col("price_rank").cast(pl.Int32),
        "price_month_max",
    )
    cp_year = cp.group_by("year").agg(
        pl.col("net_mw_rank").median().alias("cp_net_load_rank_median"),
        (pl.col("net_mw_rank") <= 10).mean().alias("cp_net_load_top10_share"),
        pl.col("price_rank").median().alias("cp_price_rank_median"),
        (pl.col("price_rank") <= TOP_PRICES).mean().alias("cp_in_top20_price_share"),
        pl.len().cast(pl.Int32).alias("n_cps"),
    )
    top = price_m.filter(pl.col("price_rank") <= TOP_PRICES).with_columns(
        ((local.dt.hour().cast(pl.Int32) * 60 + local.dt.minute().cast(pl.Int32) + 15 + 59) // 60).alias("he")
    )
    top_year = top.group_by(local.dt.year().cast(pl.Int32).alias("year")).agg(
        pl.col("he").median().alias("top20_price_median_he"),
        pl.col("he").is_between(17, 18).mean().alias("top20_price_he17_18_share"),
        (pl.col("he") >= LATE_HE).mean().alias("top20_price_after_18h_share"),
        pl.len().cast(pl.Int32).alias("top20_price_intervals"),
    )
    per_year = (
        by_year.join(ren, on="year", how="full", coalesce=True)
        .join(cp_year, on="year", how="full", coalesce=True)
        .join(top_year, on="year", how="full", coalesce=True)
        .filter(pl.col("year") >= first)
        .select(
            "year", "load_peak_mean_he", "net_load_peak_mean_he", "cp_net_load_rank_median", "cp_price_rank_median",
            "cp_in_top20_price_share", "top20_price_after_18h_share", "wind_solar_share", "cp_net_load_top10_share",
            "top20_price_median_he", "top20_price_he17_18_share", "top20_price_intervals", "n_cps", "months",
            "net_peak_same_day", "solar_max_mw",
        )
        .sort("year")
    )
    return per_cp.sort("year", "month"), per_year


def scarcity_tables(ctx: MartContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    def build() -> tuple[pl.DataFrame, pl.DataFrame]:
        load, _ = hourly(ctx)
        return scarcity(calendar(ctx), load, wind_solar(ctx), hub_prices(ctx))

    return ctx.cached("four_cp.scarcity", build)


def build_scarcity(ctx: MartContext) -> pl.DataFrame:
    return scarcity_tables(ctx)[1]


def _scarcity_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    return {
        "price_point": HUB,
        "price_market": "RTM 15-minute settlement point prices (energy only; ancillary services are out of the MVP)",
        "price_rank": "rank('min') within the local month: ties (price-cap intervals) share a rank, so a month can "
                      "have more than 20 intervals at rank ≤ 20 (top20_price_intervals)",
        "net_load": "ERCOT hourly load − wind − solar (fuel mix, MWh per 15 minutes summed to the hour)",
        "late": "top20_price_after_18h_share: intervals in hour ending 19 or later (starting 18:00 CPT or later)",
        "prices_through": hub_prices(ctx)["interval_start_utc"].max(),
        "fuel_mix_through": wind_solar(ctx)["interval_start_utc"].max(),
    }


# --- rates (X15) --------------------------------------------------------------------------------------------


def rate_rows(rates: Sequence[Mapping[str, Any]]) -> pl.DataFrame:
    """``four_cp.rates`` of the config as the mart's rows: ``charges_for_year`` (the PUCT docket's year),
    ``docket``, ``postage_stamp_usd_per_kw_yr``, ``usd_per_mw_yr`` (× 1000), ``status``, ``billed_year``
    (``charges_for_year`` + 1, the contract's reading of 16 TAC 25.192(d)), ``set_on_4cp_summer`` (the summer
    whose average 4CP the docket divides by, X15) and ``source_url``."""
    rows = []
    for r in rates:
        if r["status"] not in RATE_STATUSES:
            raise ValueError(f"four_cp.rates: status {r['status']!r} not in {sorted(RATE_STATUSES)}")
        year, kw = int(r["year"]), float(r["usd_per_kw_year"])
        rows.append({
            "charges_for_year": year,
            "docket": str(r["docket"]),
            "postage_stamp_usd_per_kw_yr": kw,
            "usd_per_mw_yr": round(kw * 1000, 3),
            "status": r["status"],
            "billed_year": year + BILLING_LAG_YEARS,
            "set_on_4cp_summer": year - 1,
            "source_url": r.get("source_url") or RATE_SOURCES.get(str(r["docket"])),
        })
    schema = {"charges_for_year": pl.Int32, "docket": pl.Utf8, "postage_stamp_usd_per_kw_yr": pl.Float64,
              "usd_per_mw_yr": pl.Float64, "status": pl.Utf8, "billed_year": pl.Int32, "set_on_4cp_summer": pl.Int32,
              "source_url": pl.Utf8}
    return pl.DataFrame(rows, schema=schema).sort("charges_for_year")


def rate_table(ctx: MartContext) -> pl.DataFrame:
    return ctx.cached("four_cp.rates", lambda: rate_rows(marts_config.value(ctx.config, "four_cp.rates")))


def build_rates(ctx: MartContext) -> pl.DataFrame:
    return rate_table(ctx)


def _rates_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    return {
        "rule": "16 TAC 25.192: each DSP pays the monthly rate × its previous year's average 4CP demand; ERCOT files "
                "the current year's 4CP by December 1 and it bills the next year",
        "rate": "ERCOT postage stamp (sum of the TSPs' TCOS ÷ ERCOT's average 4CP), quoted verbatim from the PUCT "
                "matrix (X15); TCOS ÷ average 4CP from the same table gives 2–10% less, not checked",
        "reading": OFFER_NOTE,
        "config_status": marts_config.status(ctx.config, "four_cp.rates"),
    }


# --- the offer block of /accounts/[id] (A-M3) ---------------------------------------------------------------

_NO_LTLF = pl.DataFrame(schema={"region_id": pl.Utf8, "target_year": pl.Int32, "value": pl.Float64})


def zone_line(zones: pl.DataFrame, zone: str, as_of: date) -> str | None:
    """The zone's 4CP talking point, as ``diagnosis.zone_outlook`` phrases it (X9 / X3), without the pre-X15
    "no $/kW-yr rate" clause: the offer carries the rates."""
    from basecast_pipelines.models import diagnosis as D

    outlook = D.zone_outlook(_NO_LTLF, zones, zone, ltlf_years=(as_of.year, as_of.year), as_of=as_of)
    line = outlook.get("line")
    return None if line is None else line.removesuffix(STALE_RATE_CLAUSE)


def offer(zone: str, line: str | None, head: Mapping[str, Any], rates: pl.DataFrame) -> dict:
    """The offer card from its parts (see :func:`offer_block`)."""
    from basecast_pipelines.models import diagnosis as D

    s, n = WINDOW
    days = head.get("dispatch_days")
    account = D.fact("account_4cp_mw", "Account load at the 4CP", None, "MW", D.Source("UtilityDataSource", None),
                     "not public: needs the co-op's own meter data")
    return {
        "zone": zone,
        "zone_line": line,
        "window_start_local": _hhmm(s),
        "window_end_local": _hhmm(s + n),
        "dispatch_days": None if days is None else round(float(days), 1),
        "rates": rates.select("charges_for_year", "docket", "usd_per_mw_yr", "status", "billed_year").to_dicts(),
        "note": OFFER_NOTE,
        "account_4cp": {**account, "simulated": False, "verified": False},
        "verified": bool(rates.height) and bool((rates["status"] == "final").all()),
    }


def offer_block(ctx: MartContext, zone: str | None) -> dict | None:
    """The 4CP offer of an account in weather zone ``zone`` (its primary zone), or None without a zone.

    ``{zone, zone_line, window_start_local, window_end_local, dispatch_days, rates[{charges_for_year, docket,
    usd_per_mw_yr, status, billed_year}], note, account_4cp, verified}``: the zone's 4CP line; the 15:45–17:45
    window; the dispatch days per summer of X3's alert rule (weather model, X = 96%; optimistic: observed ERA5
    weather); the postage-stamp rates of the config; the account's own 4CP load as a gap (private data, a
    ``UtilityDataSource`` fact); ``verified`` = every rate is final. Built once per zone and run (``ctx.cached``);
    each call returns its own copy."""
    if zone is None:
        return None

    def build() -> dict:
        zones = zone_table(ctx).filter(pl.col("region_type") == "weather_zone")
        return offer(zone, zone_line(zones, zone, ctx.as_of), headline_dispatch(ctx), rate_table(ctx))

    return copy.deepcopy(ctx.cached(f"four_cp.offer.{zone}", build))


# --- checks --------------------------------------------------------------------------------------------------


def _golden(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, **kw)


def _printed(decimals: int) -> float:
    """Half a unit of the doc's last printed digit (inclusive): 54 reads 53.5–54.5, 28% reads 27.5–28.5%."""
    return 0.5 * 10 ** -decimals + 1e-9


def _covered(frame: pl.DataFrame, lo: int, hi: int, *, fifteen_only: bool = False) -> tuple[int, int]:
    f = frame.filter(pl.col("year").is_between(lo, hi))
    if fifteen_only:
        f = f.filter(pl.col("source") == "de_15min")
    return int(f["in_window"].sum()), f.height


def _cps(frame: pl.DataFrame, lo: int, hi: int) -> pl.DataFrame:
    return frame.filter(pl.col("year").is_between(lo, hi) & (pl.col("source") == "de_15min"))


def _hourly_mw(frame: pl.DataFrame, year: int, month: int) -> float | None:
    hit = frame.filter((pl.col("year") == year) & (pl.col("month") == month)
                       & (pl.col("source") == "hourly_provisional"))
    return hit["mw"].item() if hit.height == 1 else None


INTERVAL_CHECKS = (
    value_check("unique year × month", lambda f: f.select("year", "month").is_duplicated().any(), False),
    value_check("June–September only", lambda f: set(f["month"].unique().to_list()) <= set(FC.CP_MONTHS), True),
    value_check("sources de_15min | hourly_provisional",
                lambda f: set(f["source"].unique().to_list()) <= set(SOURCE_NAMES.values()), True),
    value_check("no hourly value is final", lambda f: f.filter(pl.col("source") != "de_15min")["final"].any(), False),
    _golden("76 intervals, June 2008 → September 2026", lambda f: f.height, 76),
    _golden("75 from the D&E report (September 2026 hourly)",
            lambda f: f.filter(pl.col("source") == "de_15min").height, 75),
    _golden("September 2026 hourly CP 86,492 MW (2026-09-12 HE 17)", lambda f: _hourly_mw(f, 2026, 9), 86_492, tol=1),
    _golden("15:45–17:45 covers 63 of the 64 CPs of 2010–2025", lambda f: _covered(f, 2010, 2025), (63, 64)),
    _golden("15:45–17:45 covers 23 of the 23 15-minute CPs of 2021–2026",
            lambda f: _covered(f, 2021, 2026, fifteen_only=True), (23, 23)),
    _golden("36 of 75 15-minute CPs end at 17:00",
            lambda f: f.filter((pl.col("source") == "de_15min") & (pl.col("end_min") == 1020)).height, 36),
    _golden("CP hour's median net-load rank 3 (2011–2020)", lambda f: _cps(f, 2011, 2020)["net_load_rank"].median(), 3,
            tol=_printed(0)),
    _golden("CP hour's median net-load rank 109 (2021–2026)", lambda f: _cps(f, 2021, 2026)["net_load_rank"].median(),
            109, tol=_printed(0)),
    _golden("CP hour in the month's top 10 net-load hours: 77% (2011–2020)",
            lambda f: (_cps(f, 2011, 2020)["net_load_rank"] <= 10).mean(), 0.77, tol=_printed(2)),
    _golden("CP hour in the month's top 10 net-load hours: 13% (2021–2026)",
            lambda f: (_cps(f, 2021, 2026)["net_load_rank"] <= 10).mean(), 0.13, tol=_printed(2)),
    _golden("CP interval's median price rank 54 (2011–2020)", lambda f: _cps(f, 2011, 2020)["price_rank"].median(), 54,
            tol=_printed(0)),
    _golden("CP interval's median price rank 567 (2021–2026)", lambda f: _cps(f, 2021, 2026)["price_rank"].median(),
            567, tol=_printed(0)),
    _golden("CP among the month's top-20 priced intervals: 25% (2011–2020)",
            lambda f: (_cps(f, 2011, 2020)["price_rank"] <= TOP_PRICES).mean(), 0.25, tol=1e-9),
    _golden("CP among the month's top-20 priced intervals: 0% (2021–2026)",
            lambda f: (_cps(f, 2021, 2026)["price_rank"] <= TOP_PRICES).mean(), 0.0, tol=1e-9),
)


def _era(frame: pl.DataFrame, region: str, lo: int, hi: int) -> pl.DataFrame:
    return frame.filter((pl.col("region_id") == region) & pl.col("complete") & pl.col("year").is_between(lo, hi))


def _era_mean(region: str, col: str, lo: int = 2021, hi: int = 2025):
    return lambda f: _era(f, region, lo, hi)[col].mean()


def _era_intensity(region: str, lo: int = 2021, hi: int = 2025):
    """X3's era table: mean 4CP share ÷ mean energy share."""
    return lambda f: _era(f, region, lo, hi)["share_4cp"].mean() / _era(f, region, lo, hi)["share_energy"].mean()


def _zone_year(region: str, year: int, col: str):
    return lambda f: f.filter((pl.col("region_id") == region) & (pl.col("year") == year))[col].item()


ZONE_CHECKS = (
    value_check("unique region × year",
                lambda f: f.select("region_type", "region_id", "year").is_duplicated().any(), False),
    value_check("no DC tie rows", lambda f: f["region_id"].str.starts_with("DC_").any(), False),
    _golden("152 weather-zone rows (8 zones × 2008–2026)",
            lambda f: f.filter(pl.col("region_type") == "weather_zone").height, 152),
    _golden("FWEST CF 0.880 (2021–2025)", _era_mean("FWEST", "cf_summer"), 0.880, tol=0.0005),
    _golden("FWEST 4CP share 7.40% (2021–2025)", _era_mean("FWEST", "share_4cp"), 0.0740, tol=0.00005),
    _golden("FWEST energy share 9.93% (2021–2025)", _era_mean("FWEST", "share_energy"), 0.0993, tol=0.00005),
    _golden("FWEST 4CP intensity 0.75 (2021–2025)", _era_intensity("FWEST"), 0.75, tol=0.005),
    _golden("FWEST CF 0.954 (2010–2014)", _era_mean("FWEST", "cf_summer", 2010, 2014), 0.954, tol=0.0005),
    _golden("NCENT 4CP intensity 1.10 (2021–2025)", _era_intensity("NCENT"), 1.10, tol=0.005),
    _golden("NORTH CF 0.875 (2021–2025)", _era_mean("NORTH", "cf_summer"), 0.875, tol=0.0005),
    _golden("FWEST CF 0.78 in 2025", _zone_year("FWEST", 2025, "cf_summer"), 0.78, tol=0.005),
    _golden("LZ_RAYBN 4CP intensity 1.22 (2021–2025)", _era_intensity("LZ_RAYBN"), 1.22, tol=0.005),
    _golden("LZ_WEST CF 0.90 (2021–2025)", _era_mean("LZ_WEST", "cf_summer"), 0.90, tol=0.005),
)


def _curve(frame: pl.DataFrame, forecast: str, param: float, window: str = MAIN) -> dict:
    return frame.filter((pl.col("forecast") == forecast) & ((pl.col("param") - param).abs() < 1e-9)
                        & (pl.col("window") == window)).row(0, named=True)


def _curve_value(forecast: str, param: float, col: str):
    return lambda f: _curve(f, forecast, param)[col]


def _days_and_rate(forecast: str, param: float):
    """(dispatch days to the doc's one decimal, all-four rate)."""
    return lambda f: (round(_curve(f, forecast, param)["dispatch_days"], 1), _curve(f, forecast, param)["all4_rate"])


DISPATCH_CHECKS = (
    value_check("unique forecast × param × window",
                lambda f: f.select("forecast", "param", "window").is_duplicated().any(), False),
    value_check("450 rule × parameter × window rows (X3's grid)", lambda f: f.height, 450),
    value_check("one headline row", lambda f: int(f["headline"].sum()), 1),
    value_check("rates in [0, 1]", lambda f: bool(f.select(
        pl.all_horizontal(pl.col("all4_rate", "month_rate", "day_rate").is_between(0, 1)).all()).item()), True),
    _golden("scored on the 16 summers 2010–2025", lambda f: f["summers"].unique().to_list(), ["2010–2025"]),
    _golden("weather model X = 96%: 55.8 dispatch days a summer", _curve_value(WEATHER, 0.96, "dispatch_days"), 55.8,
            tol=_printed(1)),
    _golden("weather model X = 96%: all four caught in 15 of 16 summers", _curve_value(WEATHER, 0.96, "all4_rate"),
            15 / 16, tol=1e-9),
    _golden("weather model X = 96%: 98.4% of CP months", _curve_value(WEATHER, 0.96, "month_rate"), 0.984,
            tol=_printed(3)),
    _golden("weather model X = 97%: 49.6 days, 14 of 16", _days_and_rate(WEATHER, 0.97), (49.6, 14 / 16)),
    _golden("weather model X = 100%: 30.3 days, 9 of 16", _days_and_rate(WEATHER, 1.0), (30.3, 9 / 16)),
    _golden("perfect foresight X = 99%: 36.8 days, 15 of 16", _days_and_rate("actual × noise σ=0%", 0.99),
            (36.8, 15 / 16)),
    _golden("perfect foresight top 2 days a month: 8 days, 15 of 16",
            _days_and_rate("top-N by actual peak (hindsight)", 2), (8.0, 15 / 16)),
    _golden("noise σ = 2%, X = 96%: 58.2 days", _curve_value("actual × noise σ=2%", 0.96, "dispatch_days"), 58.2,
            tol=_printed(1)),
    _golden("noise σ = 2%, X = 96%: all four in 91% of summers", _curve_value("actual × noise σ=2%", 0.96, "all4_rate"),
            0.91, tol=_printed(2)),
)


def _year(year: int, col: str):
    return lambda f: f.filter(pl.col("year") == year)[col].item()


SCARCITY_CHECKS = (
    value_check("unique year", lambda f: f["year"].is_duplicated().any(), False),
    _golden("16 summers 2011–2026", lambda f: (f.height, f["year"].min(), f["year"].max()), (16, 2011, 2026)),
    _golden("top-20 priced intervals after 18:00: 1–10% in 2011–2020", lambda f: bool(
        f.filter(pl.col("year") <= 2020)["top20_price_after_18h_share"].is_between(0.005, 0.105).all()), True),
    _golden("top-20 priced intervals after 18:00: 15% in 2021", _year(2021, "top20_price_after_18h_share"), 0.15,
            tol=_printed(2)),
    _golden("top-20 priced intervals after 18:00: 28% in 2022", _year(2022, "top20_price_after_18h_share"), 0.28,
            tol=_printed(2)),
    _golden("top-20 priced intervals after 18:00: 64% in 2023", _year(2023, "top20_price_after_18h_share"), 0.64,
            tol=_printed(2)),
    _golden("top-20 priced intervals after 18:00: 90% in 2024", _year(2024, "top20_price_after_18h_share"), 0.90,
            tol=_printed(2)),
    _golden("top-20 priced intervals after 18:00: 95% in 2025", _year(2025, "top20_price_after_18h_share"), 0.95,
            tol=_printed(2)),
    _golden("top-20 priced intervals after 18:00: 98% in 2026", _year(2026, "top20_price_after_18h_share"), 0.98,
            tol=_printed(2)),
    _golden("net-load peak mean HE 20.0 in 2023", _year(2023, "net_load_peak_mean_he"), 20.0, tol=0.005),
    _golden("net-load peak mean HE 20.5 in 2024", _year(2024, "net_load_peak_mean_he"), 20.5, tol=0.005),
    _golden("net-load peak mean HE 20.75 in 2025", _year(2025, "net_load_peak_mean_he"), 20.75, tol=0.005),
    _golden("net-load peak mean HE 21.0 in 2026 (June–August)", _year(2026, "net_load_peak_mean_he"), 21.0, tol=0.005),
    _golden("load peak mean HE 17.7 in 2026", _year(2026, "load_peak_mean_he"), 17.7, tol=0.05),
    _golden("wind + solar 5% of June–September load in 2011", _year(2011, "wind_solar_share"), 0.05, tol=_printed(2)),
    _golden("wind + solar 33% in 2025", _year(2025, "wind_solar_share"), 0.33, tol=_printed(2)),
    _golden("wind + solar 38% in 2026 (June–August)", _year(2026, "wind_solar_share"), 0.38, tol=_printed(2)),
)


def _rate(year: int):
    return lambda f: f.filter(pl.col("charges_for_year") == year).select(
        "docket", "usd_per_mw_yr", "status", "billed_year").row(0)


RATES_CHECKS = (
    value_check("unique charges_for_year", lambda f: f["charges_for_year"].is_duplicated().any(), False),
    value_check("billed the year after", lambda f: bool((f["billed_year"] == f["charges_for_year"] + 1).all()), True),
    value_check("$/MW-yr = 1000 × $/kW-yr", lambda f: bool(
        ((f["usd_per_mw_yr"] - 1000 * f["postage_stamp_usd_per_kw_yr"]).abs() < 1e-6).all()), True),
    _golden("2025: Docket 57491, $68,547/MW-yr, final", _rate(2025), ("57491", 68_547.0, "final", 2026)),
    _golden("2026: Docket 59080, $75,527/MW-yr, pending", _rate(2026), ("59080", 75_527.0, "pending", 2027)),
)


# --- marts ---------------------------------------------------------------------------------------------------

FOUR_CP_INTERVALS = Mart(
    name="mart_four_cp_intervals",
    build=build_intervals,
    key=("year", "month"),
    inputs=("ercot_monthly_peaks", "ercot_load_hourly_wz", "ercot_fuel_mix_15min", "prices_rtm_hub_lz"),
    description="ERCOT's four summer coincident peaks by year since 2008: the 15-minute interval (D&E report, "
    "interval ending, CPT) or the hourly table's provisional max for months not yet published, whether the "
    "15:45–17:45 discharge covers it, and its net-load and RT price rank in the month (X3 Q1, Q4).",
    caveats=("preliminary_actuals",),
    checks=INTERVAL_CHECKS,
    meta=_intervals_meta,
)

FOUR_CP_ZONE = Mart(
    name="mart_four_cp_zone",
    build=build_zone,
    key=("region_type", "region_id", "year"),
    inputs=("ercot_monthly_peaks",),
    description="Each weather zone and load zone at ERCOT's 4CP by summer: mean load at the CPs, coincidence factor "
    "vs its own peak, 4CP and energy shares, 4CP intensity and its own peak hour (X3 Q3).",
    caveats=("preliminary_actuals",),
    checks=ZONE_CHECKS,
    meta=_zone_meta,
)

FOUR_CP_DISPATCH_CURVE = Mart(
    name="mart_four_cp_dispatch_curve",
    build=build_dispatch_curve,
    key=("forecast", "param", "window"),
    inputs=("ercot_monthly_peaks", "ercot_load_hourly_wz", "weather_hourly_wz"),
    description="Dispatch days per summer vs the share of summers with all four CPs caught, per day-ahead rule "
    "(threshold on the month-to-date max, top-N days), forecast and discharge window, 2010–2025 (X3 Q2). "
    "Optimistic: the weather model uses observed ERA5 weather.",
    caveats=("optimistic_weather",),
    checks=DISPATCH_CHECKS,
    meta=_dispatch_meta,
)

FOUR_CP_SCARCITY = Mart(
    name="mart_four_cp_scarcity",
    build=build_scarcity,
    key=("year",),
    inputs=("ercot_monthly_peaks", "ercot_load_hourly_wz", "ercot_fuel_mix_15min", "prices_rtm_hub_lz"),
    description="Per summer since 2011: load vs net-load peak hour, the 4CP intervals' net-load and RT price ranks, "
    "where the month's 20 highest-priced intervals fall and the wind + solar share (X3 Q4).",
    caveats=("preliminary_actuals",),
    checks=SCARCITY_CHECKS,
    meta=_scarcity_meta,
)

FOUR_CP_RATES = Mart(
    name="mart_four_cp_rates",
    build=build_rates,
    key=("charges_for_year",),
    inputs=(),
    description="ERCOT's wholesale transmission postage-stamp rate by charge year (PUCT docket, final or pending), "
    "in $/kW-yr and $/MW-yr, and the year a summer's 4CP is billed (config four_cp.rates, X15).",
    checks=RATES_CHECKS,
    meta=_rates_meta,
)

MARTS = (FOUR_CP_INTERVALS, FOUR_CP_ZONE, FOUR_CP_DISPATCH_CURVE, FOUR_CP_SCARCITY, FOUR_CP_RATES)
