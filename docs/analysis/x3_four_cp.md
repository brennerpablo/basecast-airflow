# X3 — 4CP targeting: the offer to co-ops

Exploration X3 after phase 0. Run on 2026-09-26 against production Postgres (`basecast_reader`, read-only):
`ercot_monthly_peaks` (Demand and Energy workbooks, snapshot 2026-09-26), `ercot_load_hourly_wz` (through hour ending
2026-09-26 00:00 CDT), `weather_hourly_wz` (ERA5, through 2026-09-20). Q4 also reads the local raw lake
(`ercot_fuel_mix` 2011 → 2026-08-31, `ercot_spp_hist` RTM 2011 → 2026-09-19) through the repo's own parsers.

- Script: `analysis/x3_four_cp.py` (`uv run --group analysis python analysis/x3_four_cp.py`, ~50 s; the first run
  also parses the fuel mix and RTM workbooks into two caches under `analysis/out/`)
- Model code: `basecast_pipelines/models/four_cp.py` (loader `load_monthly_peaks()`; pure `cp_intervals()`,
  `hourly_cps()`, `cp_calendar()`, `daily_peaks()`, `in_window()`, `window_coverage()`, `with_reference()`,
  `threshold_dispatch()`, `top_n_dispatch()`, `evaluate_dispatch()`, `summarize_dispatch()`, `noisy_forecast()`,
  `weather_forecast()`, `zone_coincidence()`, `hourly_from_15min()`, `net_load_hourly()`, `monthly_argmax()`,
  `rank_within_month()`); reuses `weather_load` (loaders, `weather_daily`, `zone_weights`, `system_weather_daily`)
  without changing it. Tests: `tests/models/test_four_cp.py` (synthetic, DST days included, no DB).
- Outputs (gitignored): `analysis/out/x3_cp_timing.png`, `x3_dispatch_tradeoff.png`, `x3_zone_coincidence.png`,
  `x3_net_load.png`, `x3_dispatch_rules.csv`, `x3_zone_coincidence.csv`, and the caches
  `x3_fuel_wind_solar.parquet`, `x3_rtm_hubavg.parquet`.

**Definitions.**

- **4CP interval** = ERCOT's 15-minute system peak of each of June–September (`ercot_monthly_peaks`,
  `metric = 'peak_15min_mw'`, region `ERCOT`). `peak_local` is the workbook's local **interval ending** (parser
  docstring), so "17:00" is 16:45–17:00 CPT. The zones' coincident values (`coincident_peak_15min_mw`) sum to that
  MW exactly in every summer month 2008–2026 (max relative difference 1e-5), so they are the loads at that interval.
  That these are the same intervals ERCOT uses to settle 4CP transmission charges is **not verified** (ERCOT's own 4CP
  list is not in the repo).
- **4CP rule** (PUCT Subst. R. 25.192, **not verified** here): a distribution provider's wholesale transmission
  charge is allocated by its average load at the four intervals. **No transmission rate ($/kW-yr) is in the repo**
  (`config/manual_official_figures.yaml` and `manual_official_figures` have none), so every result below is in kW
  and dispatch days; any dollar value is **not verified**.
- **Discharge window** = local wall clock (start, end]; an interval counts only if it lies wholly inside.
- **Dispatch day** = a day the fleet discharges in the window. **Hit** = the CP day was dispatched and the window
  covers the CP interval. **All four** = every CP month of the summer hit. Rates over the 16 summers 2010–2025.

## Q1 — The 4CP intervals by year

15-minute intervals from the D&E report for June 2008 → August 2026 (75 months). **September 2026 is the only
hourly value** (the D&E workbook stops at August): the hourly table's max so far, 86,492 MW on 2026-09-12 HE 17
(16:00–17:00), **provisional** (September runs through the 30th; July–August 2026 are not final-settled).

Local interval ending (CPT) and MW:

| Year | June | July | August | September |
|---|---|---|---|---|
| 2008 | 06-16 16:45 · 59,699 | 07-31 16:45 · 61,175 | 08-04 17:00 · 62,266 | 09-02 16:45 · 56,434 |
| 2009 | 06-25 16:15 · 62,393 | 07-13 17:00 · 63,518 | 08-05 16:00 · 62,241 | 09-03 16:00 · 55,383 |
| 2010 | 06-21 16:45 · 60,840 | 07-15 16:45 · 60,730 | 08-23 16:00 · 66,028 | 09-14 16:45 · 58,268 |
| 2011 | 06-15 17:00 · 63,220 | 07-27 16:30 · 65,501 | 08-03 17:00 · 68,416 | 09-02 16:30 · 63,214 |
| 2012 | 06-26 16:30 · 66,577 | 07-31 17:00 · 65,889 | 08-01 17:00 · 66,583 | 09-05 17:00 · 64,959 |
| 2013 | 06-27 17:00 · 64,533 | 07-31 17:00 · 65,040 | 08-07 16:45 · 67,328 | 09-03 16:45 · 63,479 |
| 2014 | 06-30 16:30 · 59,819 | 07-21 16:45 · 63,627 | 08-25 17:00 · 66,580 | 09-10 17:00 · 64,596 |
| 2015 | 06-10 16:45 · 61,779 | 07-30 16:45 · 67,700 | 08-10 17:00 · 69,942 | 09-08 16:30 · 64,513 |
| 2016 | 06-15 17:00 · 65,001 | 07-14 16:30 · 67,518 | 08-11 16:30 · 71,150 | 09-19 16:00 · 67,071 |
| 2017 | 06-23 16:45 · 67,695 | 07-28 17:00 · 69,629 | 08-16 17:00 · 68,028 | 09-20 16:45 · 63,740 |
| 2018 | 06-27 17:00 · 69,255 | 07-19 17:00 · 73,539 | 08-23 16:45 · 69,990 | 09-19 16:30 · 64,693 |
| 2019 | 06-19 17:00 · 68,271 | 07-30 16:30 · 71,162 | 08-12 17:00 · 74,897 | 09-06 16:45 · 69,187 |
| 2020 | 06-08 17:45 · 68,196 | 07-13 16:45 · 74,392 | 08-13 16:45 · 74,420 | 09-01 14:30 · 64,945 |
| 2021 | 06-23 17:00 · 70,394 | 07-26 17:00 · 73,305 | 08-24 17:00 · 73,822 | 09-01 17:00 · 72,441 |
| 2022 | 06-23 17:00 · 76,803 | 07-20 16:45 · 80,233 | 08-02 17:00 · 78,669 | 09-20 17:00 · 71,150 |
| 2023 | 06-27 17:00 · 81,018 | 07-31 17:00 · 83,032 | 08-10 17:00 · 85,637 | 09-08 16:45 · 84,540 |
| 2024 | 06-30 17:45 · 79,750 | 07-01 17:00 · 81,184 | 08-20 17:00 · 85,357 | 09-20 16:00 · 77,879 |
| 2025 | 06-19 17:00 · 77,486 | 07-30 17:00 · 81,908 | 08-18 17:00 · 83,968 | 09-04 17:30 · 80,076 |
| 2026 | 06-18 17:00 · 82,902 | 07-22 17:30 · 91,263 | 08-24 17:00 · 91,055 | *09-12 HE 17 · 86,492 (hourly, provisional)* |

**Cross-check against the hourly table** (75 months): the 15-minute CP falls on the day of the month's highest
hour in 72/75 months (the exceptions are 2010-07, 2018-09 and 2024-09, each one day apart) and in the same hour
ending in 63/75; the 15-minute MW is 0.02–0.48% above the hourly max. The CP day is the month's highest daily
(hourly) peak in 73 of 76 months and the second highest in the other 3.

**Has the hour moved later?** Slightly, and only recently. OLS of the interval end on the year: **+1.4 min/yr**
(SE 0.5, p = 0.011, n = 75).

| Era | CPs | Mean interval end | Ending ≤ 16:30 | 16:45 | 17:00 | After 17:00 | Weekend | Friday |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| 2008–2012 | 20 | 16:40 | 7 | 6 | 7 | 0 | 0 | 1 |
| 2013–2017 | 20 | 16:46 | 5 | 7 | 8 | 0 | 0 | 2 |
| 2018–2022 | 20 | 16:48 | 3 | 5 | 11 | 1 | 0 | 1 |
| 2023–2026 | 15 | 17:02 | 1 | 1 | 10 | 3 | 1 | 2 |

- The CP is a tight cluster: **36 of 75 intervals end at exactly 17:00** (16:45–17:00), 19 at 16:45. Only
  4 have ever ended after 17:00: 2020-06 (17:45), 2024-06 (17:45, the only weekend CP: Sunday 2024-06-30),
  2025-09 (17:30), 2026-07 (17:30).
- The **hourly** peak moved to HE 18 in 2023–2026 (phase 0 Q1), but the 15-minute CP did not follow: on 10 of the
  15 CP days since 2023 HE 18 averaged more than HE 17, yet the peak 15 minutes still ended at 17:00. The CP now
  sits on the HE 17/18 boundary. Whether the "17:00" cluster is interval ending (as labelled) or beginning changes
  which side of that boundary it is on: **review item 1**.
- Day of week: Mon 18, Tue 13, Wed 21, Thu 16, Fri 6, Sat 0, Sun 1. A weekday-only dispatch calendar would have
  missed 1 CP in 19 years.

## Q2 — Dispatch days needed to catch all four

**Windows first.** Coverage of the CP intervals by a fixed daily window (2010–2025: 64 CPs; 2021–2026: the 23
15-minute CPs):

| Window (CPT) | 2010–2025 | 2021–2026 |
|---|---:|---:|
| 1 h 16:00–17:00 (HE 17) | 57/64 | 19/23 |
| 1 h 16:30–17:30 | 49/64 | 21/23 |
| 2 h 16:00–18:00 (HE 17–18) | 60/64 | 22/23 |
| **2 h 15:45–17:45** | **63/64** | **23/23** |
| 4 h 15:00–19:00 (HE 16–19) | 63/64 | 23/23 |
| 4 h 16:00–20:00 (HE 17–20, the brief's guess) | 60/64 | 22/23 |
| 4 h 14:00–18:00 | 64/64 | 23/23 |

No 1-hour window catches more than 57/64, so a 1-hour product can never promise all four. A **2-hour discharge
15:45–17:45** does as well as any 4-hour window except 14:00–18:00; its one miss is 2020-09-01 (interval ending
14:30). HE 17–20 misses the 16:00-ending CPs. The window was picked on the same years it is scored on (in-sample).

**Day-ahead forecasts of the daily peak** (evaluated June–September, 2010–2025):

- *Perfect foresight*: the actual daily hourly peak (σ = 0).
- *Noise model*: actual × exp(σ·z), σ = 2%, 3%, 5% (100 seeded draws each).
- *Weather model*: `log(peak) ~ year effects + t_max + t_max² + t_mean(d−1) + weekend` on the ERCOT-weighted ERA5
  weather (Q7's zone weights), fit on the three previous summers, level set by the mean residual of the previous
  7 days. Day-ahead MAPE **1.9%** (log SD 2.5%). **ERA5 is observed weather**, so this assumes a perfect temperature
  forecast: an optimistic proxy for a real day-ahead forecast.

**Rule (a)**: dispatch day d when the forecast peak ≥ X × the month-to-date max through d−1 (known the evening
before); the first day of each month is always dispatched. **Rule (b)**: the top N days of each month by a score
(needs the whole month's forecast up front, so it is a benchmark, not an operable rule).

Results with the 2 h 15:45–17:45 window (all 16 summers; the ceiling is 15/16 because of 2020-09):

| Forecast | Rule | Dispatch days / summer | All four caught | CP months caught |
|---|---|---:|---:|---:|
| perfect foresight | (a) X = 100% (every new monthly record) | 23.9 | 13/16 (81%) | 95.3% |
| perfect foresight | (a) X = 99% | 36.8 | 15/16 (94%) | 98.4% |
| perfect foresight | (b) top 2 days/month | 8 | 15/16 (94%) | 98.4% |
| **weather model** | **(a) X = 96%** | **55.8** | **15/16 (94%)** | 98.4% |
| weather model | (a) X = 97% | 49.6 | 14/16 (88%) | 96.9% |
| weather model | (a) X = 98% | 43.4 | 13/16 (81%) | 95.3% |
| weather model | (a) X = 100% | 30.3 | 9/16 (56%) | 87.5% |
| weather model | (b) top 5 days/month | 20 | 12/16 (75%) | 93.8% |
| weather model | (b) top 10 days/month | 40 | 15/16 (94%) | 98.4% |
| hottest t_max | (b) top 10 days/month | 40 | 15/16 (94%) | 98.4% |
| noise σ = 2% | (a) X = 96% | 58.2 | 91% | 97.8% |
| noise σ = 3% | (a) X = 94% | 68.9 | 91% | 97.7% |
| noise σ = 5% | (a) X = 90% | 84.5 | 90% | 97.6% |

With either 1 h window (16:00–17:00, 16:15–17:15) the best any rule reaches is 11/16 summers (69%); with HE 17–18 or
HE 17–20, 12/16 (75%). Full grid (every X, N, σ and
window) in `analysis/out/x3_dispatch_rules.csv`; the trade-off curves in `x3_dispatch_tradeoff.png`.

**Has it got harder?** Yes. Weather model, 2 h window:

| Summers | X = 95% | X = 97% | X = 98% |
|---|---|---|---|
| 2010–2017 | 58.8 days, 8/8 | 46.1 days, 8/8 | 40.1 days, 8/8 |
| 2018–2025 | 64.8 days, 7/8 | 53.1 days, 6/8 | 46.8 days, 5/8 |

Days per summer at X = 95%: 46–66 in 2010–2021, then **72, 76, 68, 79, 87** in 2022–2026. The cause is flatter
summers: the number of days within 3% of their month's peak averaged 23 per summer in 2010–2017, **46 in 2023–2026**
(55 in 2025, 50 in 2026). More near-peak days means more days a rule has to call to be sure.

**For the offer**: with a weather-grade forecast, catching all four CPs in ~9 summers out of 10 costs **~50–56
dispatch days per summer** of **2 hours at 15:45–17:45**, about 13–14 days a month. Perfect foresight would need ~37;
a desk with a good monthly outlook (rule b) ~40. Battery duration, backup reserve and cycling limits of Base's fleet
are **not verified**, so whether ~55 two-hour cycles a summer is acceptable is review item 3.

## Q3 — Zones at the 4CP intervals

From the D&E zone sheets (`coincident_peak_15min_mw`, `noncoincident_peak_15min_mw`, `energy_mwh`), June–September.
**CF** = mean zone load at the 4CP ÷ the zone's own highest 15-minute load of the summer. **4CP intensity** = the
zone's share of ERCOT's 4CP load ÷ its share of summer energy (above 1: the zone pays more transmission per MWh).
Means of 2010–2014 → 2021–2025:

| Zone | CF | 4CP share | Energy share | 4CP intensity | Own peak ends (mean) |
|---|---|---|---|---|---|
| COAST | 0.950 → 0.954 | 27.7% → 27.4% | 28.6% → 27.5% | 0.97 → 0.99 | 16:26 → 16:10 |
| EAST | 0.893 → 0.932 | 3.55% → 3.60% | 3.62% → 3.42% | 0.98 → 1.05 | 15:38 → 16:03 |
| **FWEST** | **0.954 → 0.880** | **3.27% → 7.40%** | **3.71% → 9.93%** | **0.88 → 0.75** | 16:39 → 16:22 |
| NCENT | 0.942 → 0.942 | 36.0% → 32.6% | 34.3% → 29.8% | 1.05 → 1.10 | 16:47 → 16:59 |
| **NORTH** | **0.920 → 0.875** | 2.21% → 2.31% | 2.25% → 2.45% | 0.98 → 0.94 | 16:23 → 15:57 |
| SCENT | 0.938 → 0.923 | 16.9% → 16.6% | 16.7% → 16.5% | 1.01 → 1.01 | 17:09 → 18:13 |
| SOUTH | 0.950 → 0.917 | 7.67% → 7.56% | 8.06% → 7.92% | 0.95 → 0.95 | 16:08 → 16:20 |
| WEST | 0.946 → 0.932 | 2.69% → 2.51% | 2.70% → 2.51% | 1.00 → 1.00 | 16:50 → 16:57 |

Single years: FWEST CF 0.78 in 2025 and 0.84 in 2026 (June–August only); NORTH 0.70 in 2025 and 0.83 in 2026.

Load zones (2011–2014 → 2021–2025): LZ_WEST CF 0.97 → 0.90, 4CP share 6.7% → 10.8% vs energy 7.3% → 13.6%
(intensity 0.92 → 0.79); LZ_RAYBN (the Rayburn load zone; its owner's identity not verified here) 4CP share 1.06% → 1.54%, intensity 1.14 → 1.22;
LZ_LCRA intensity 1.04 → 1.13; LZ_SOUTH CF 0.96 → 0.90.

**What this says, against the brief's hypothesis.** The brief expected flatter large-load zones to carry a rising
4CP share. FWEST's 4CP share did more than double (3.3% → 7.4%), but **less than its energy share** (3.7% → 9.9%),
and its load at the 4CP fell from 95% to 88% of its own summer peak (78% in 2025). NORTH shows the same drop. A flat
load would push CF towards 1, not away from it, so the new load in FWEST and NORTH is *less* present at ERCOT's
4CP than its energy suggests: consistent with large flexible loads that already curtail in likely 4CP intervals
(a known practice; **not verified** from this data, and no single customer can be seen here). Meanwhile NCENT,
the residential/commercial DFW zone, has the highest 4CP intensity (1.10, rising).

For co-ops:

- **FWEST / NORTH co-ops**: their big new loads may already be dodging 4CP, so the co-op's remaining 4CP kW is
  mostly its residential and small-commercial load, the part a home-battery fleet touches. Their own peak also
  ends earlier (16:22 and 15:57 on average in 2021–2025, about 15:25–15:30 in June–August 2026) than ERCOT's 17:00 CP: a battery aimed at the
  co-op's own peak (G&T demand charges, local capacity) and one aimed at 4CP are **different discharges**, so the
  offer has to say which one it serves. How much of these co-ops' load is 4CP-responsive is a question for the
  co-op's own data (`UtilityDataSource`).
- **NCENT and LCRA / Rayburn-type co-ops** (intensity 1.10–1.22 and rising): their load is peakiest at the 4CP, so
  every kW shaved at 17:00 is a larger share of their transmission bill: the strongest 4CP pitch.

## Q4 — Net load and scarcity at the 4CP

Net load = ERCOT hourly load − wind − solar (fuel mix, MWh per 15 minutes summed to the hour). Wind + solar rose
from 5% of June–September load in 2011 to 33% in 2025 (38% in June–August 2026; solar max 35.1 GW in 2026).

| Summer | Load peak, mean HE | Net-load peak, mean HE |
|---|---:|---:|
| 2011–2014 | 17.0 | 17.0 |
| 2015–2021 | 16.75–17.25 | 15.75–16.5 |
| 2022 | 17.0 | 17.25 |
| 2023 | 17.5 | **20.0** |
| 2024 | 17.75 | **20.5** |
| 2025 | 17.75 | **20.75** |
| 2026 (Jun–Aug) | 17.7 | **21.0** |

At the 4CP interval itself:

| | 2011–2020 (40 CPs) | 2021–2026 (23 CPs) |
|---|---:|---:|
| CP hour's rank by net load within its month (median, of ~720 h) | **3** | **109** |
| CP hour in the month's top 10 net-load hours | 77% | 13% |
| CP interval's rank by RT price (HB_HUBAVG) within its month (median, of ~2,900) | 54 | 567 |
| CP interval among the month's 20 highest-priced intervals | 25% | **0%** |

The month's 20 highest RT price intervals fell after 18:00 (HE ≥ 19) in 1–10% of cases in 2011–2020, 15% in 2021,
28% in 2022, **64% in 2023, 90% in 2024, 95% in 2025, 98% in 2026**. The 2026 CP intervals cleared at $42–50/MWh
while their months peaked at $211–781/MWh.

So **yes**: since 2023 the net-load peak comes 3–4 hours after the load peak, and ERCOT's 4CP (measured on load)
no longer lines up with the scarcity hours. For a battery fleet this is a real conflict: a 15:45–17:45 4CP discharge
and an evening (HE 19–21) energy discharge are two different cycles, and on a 4CP-alert day the fleet has to be
recharged in between or split its energy. Ancillary service prices are out of the MVP, so this uses energy prices
only.

## Headline for the video

"ERCOT's 4CP still lands at 5 pm: a 2-hour discharge from 3:45 to 5:45 would have covered 63 of the 64 peak
intervals since 2010. But flatter summers mean catching all four now takes about 55 battery days a summer with a
weather-based call, and since 2023 those 5 pm intervals are no longer when power is scarce: the costliest intervals
have moved after 6 pm." *Caveats: window chosen in-sample; weather model uses observed ERA5 weather (optimistic);
2026 July–August not final-settled and September 2026 provisional; the D&E "interval ending" label and its match to
ERCOT's settlement 4CP are not verified; no $/kW-yr in the repo.*

## Proposed decisions.md lines

- 2026-09-26 — The 4CP intervals come from `ercot_monthly_peaks.peak_15min_mw` (D&E report, local interval ending), with the hourly table only for months the report has not published yet. — The zone coincident loads sum to it exactly in every summer month 2008–2026; ERCOT's settlement 4CP list is not in the repo (not verified).
- 2026-09-26 — The default 4CP discharge window is 2 hours, 15:45–17:45 CPT. — It covered 63/64 CPs in 2010–2025 and 23/23 since 2021; no 1-hour window covered more than 57/64 and HE 17–20 covered 60/64 (chosen in-sample).
- 2026-09-26 — The 4CP offer is expressed in kW at the 4CP and dispatch days per summer; no dollar value until a transmission rate is sourced. — No $/kW-yr figure exists in the repo.
- 2026-09-26 — The 4CP alert rule shown in the product is "dispatch when the forecast peak ≥ 96% of the month-to-date max", labelled optimistic. — Best simple causal rule tested (15/16 summers at ~56 days); its forecast uses observed ERA5 weather.
- 2026-09-26 — "4CP intensity" (zone 4CP share ÷ energy share) and the zone coincidence factor become account signals by weather/load zone. — They separate zones whose load is peakiest at the 4CP (NCENT 1.10, LZ_RAYBN 1.22) from zones whose new load avoids it (FWEST 0.75).

## Review items for Pablo

1. **Interval label.** The D&E workbook labels the 15-minute peak time as interval ending (parser), so "17:00" =
   16:45–17:00. Since 2023 the hourly peak is HE 18 while the 15-minute peak still reads 17:00, which would also fit
   an interval-*beginning* label. Check one year against ERCOT's published 4CP intervals. If it is beginning, the
   recommended window shifts 15 minutes later (16:00–18:00 then covers the same set).
2. **Transmission rate.** Source the ERCOT wholesale transmission (postage-stamp / TCOS) rate in $/kW-yr and the
   billing lag (4CP summer → following year's charges) before any dollar number reaches the pitch.
3. **Fleet limits.** Base's battery kW, kWh and backup-reserve policy are not verified here; ~50–56 two-hour cycles
   a summer (plus any evening energy dispatch, Q4) must fit them.
4. **FWEST / NORTH reading.** Falling CF read as 4CP-avoiding large loads: plausible, not verified. OK to say it in
   the video only as "their load is less present at the 4CP than its size suggests"?
5. **Q4 in the video?** The "4CP and scarcity split in 2023" chart is strong, but it uses RT hub prices only.
6. **Assumptions to confirm**: first day of each month always dispatched; weekend days allowed; 2010–2025 as the
   scoring window.

## Useful for the core features

- **/accounts next action and offer**: per co-op (through its weather zone and load zone), show the zone's 4CP
  intensity, CF and own-peak time next to the ERCOT CP time. Offer template: "N MW dispatched 15:45–17:45 on ~55
  alert days lowers your 4CP load by N MW (rate not verified)"; flag co-ops whose own peak ends before 16:00
  ("choose: own peak or 4CP"). Triggers: "4CP season starts" (June 1) and "4CP alert day" (rule a, X = 96%),
  exportable by webhook.
- **Forecast screen**: a 4CP card with the interval calendar (Q1 table), the timing trend, the dispatch trade-off
  curve (days vs. hit rate) and the load vs net-load peak hour chart. A daily "4CP probability" can reuse
  `weather_forecast()` once a real weather forecast replaces ERA5.
- **Marts to propose to get-data** (contract owned there; not done here): `four_cp_intervals` (year, month,
  interval end local/UTC, MW, source, final flag), `four_cp_zone` (zone, year, CF, 4CP share, energy share, own-peak
  hour) and `four_cp_dispatch_curve` (forecast, rule, parameter, window, days, hit rates).
