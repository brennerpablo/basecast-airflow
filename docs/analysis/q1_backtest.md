# Q1 — How far off are ERCOT's official summer peak forecasts?

Phase 0, question 1 (`docs/PHASE0_ANALYSIS.md` §2). Run on 2026-09-26 against the production database
(`basecast_reader`, read-only).

- Script: `analysis/q1_backtest.py` (`uv run --group analysis python analysis/q1_backtest.py`).
- Logic: `basecast_pipelines/models/backtest.py` (`summer_peaks`, `actual_summer_peaks`, `base_series`,
  `match_forecasts`, `horizon_summary`, `sign_verdict`). Tests: `tests/models/test_backtest.py`.
- Outputs (gitignored): `analysis/out/q1_metric_catalog.csv`, `q1_actual_summer_peaks.csv`, `q1_errors.csv`
  (every vintage × target year), `q1_horizon_summary.csv`, `q1_backtest.png`.

Conventions: **error = forecast − actual**, in % of the actual, so **positive = the official forecast was too
high**. **Horizon = summers ahead**: 1 is the first summer (June 1 on) that starts after the vintage's date; a
vintage dated June or later sits inside that summer, which is horizon 0 (partly observed). The official
forecasts are normal-weather (P50) values, so every error mixes forecast bias with that summer's weather.

## 1. Metric catalog

`official_forecasts` has 16,572 rows in 159 distinct (product, metric, scenario, region_type). Collapsing the
weather-year scenarios (`*_weather_year_YYYY`) and the labelled CDR sensitivities gives the table below (full
list in `analysis/out/q1_metric_catalog.csv`).

| product | metric | scenario | region_type | rows | vintages | targets |
|---|---|---|---|---:|---:|---|
| CDR | peak_mw | (none) | ercot | 738 | 46 | 2000–2034 |
| CDR | peak_mw | named sensitivity (2 labels, mostly 2025 CDRs) | ercot | 74 | 4 | 2021–2030 |
| CDR | peak_mw | (none) | utility | 831 | 3 | 2000–2007 |
| CDR | firm_peak_mw | (none) | ercot | 738 | 46 | 2000–2034 |
| CDR | firm_peak_mw | named sensitivity (17 labels, mostly 2025 CDRs) | ercot | 249 | 4 | 2021–2030 |
| CDR | firm_peak_mw | (none) | utility | 831 | 3 | 2000–2007 |
| CDR | peak_before_ee_mw | (none) / 1 label | ercot | 324 | 16 | 2016–2034 |
| LTLF | peak_mw | base | ercot | 1,321 | 11 | 2013–2034 |
| LTLF | peak_mw | base_p50 / base_p90 | ercot | 81 / 344 | 8 / 10 | 2014–2032 |
| LTLF | peak_mw | base_weather_year_YYYY (21) | ercot | 1,302 | 9 | 2014–2033 |
| LTLF | peak_mw | ercot_adjusted (+ _p75, _p90, _p75_no_large_loads, weather years) | ercot | 254 (+ 121) | 1 | 2025–2044 |
| LTLF | peak_mw | tsp_provided (+ _p90, weather years) | ercot | 254 (+ 119) | 1 | 2025–2044 |
| LTLF | peak_mw | ercot_adjusted_p75_no_large_loads | transmission_operator | 21 | 1 | 2025 |
| LTLF | peak_mw | base / base_p90 / ercot_adjusted / tsp_provided | weather_zone | 776 / 2,104 / 112 / 112 | 9 / 2 / 1 / 1 | 2013–2033 |
| LTLF | ncp_zone_sum_mw | base / base_p90 / ercot_adjusted / tsp_provided | ercot | 94 / 96 / 14 / 14 | 9 / 9 / 1 / 1 | 2014–2033 |
| LTLF | ncp_mw | base / base_p90 / ercot_adjusted / tsp_provided | weather_zone | 752 / 768 / 112 / 112 | 9 / 9 / 1 / 1 | 2014–2033 |
| LTLF | gross_peak_mw, gross_ncp_mw, gross_ncp_zone_sum_mw | base, base_p50, base_p90, weather years | ercot / weather_zone | 1,320 | 3 | 2021–2032 |
| LTLF | energy_mwh | base / ercot_adjusted / tsp_provided | ercot / weather_zone | 1,794 | 11 | 2013–2044 |
| LTLF | large_load_{contracts,officer_letters,total,additions}_mw | base / ercot_adjusted_p75 | ercot | 31 | 1 | 2024–2033 |
| LTLF | rooftop_pv_mw, rooftop_pv_ncp_mw, rooftop_pv_ncp_zone_sum_mw | base, base_p50, base_p90, weather years | ercot / weather_zone | 754 | 2 | 2022–2032 |
| LTLF (manual) | peak_demand | base / range_low / range_high | system | 3 / 1 / 1 | 1 | 2026–2032 |

**Base series chosen** (one value per product × vintage × target year, `base_series`):

| Product | Series | Why |
|---|---|---|
| LTLF | ERCOT-level `peak_mw`, summer: `ercot_adjusted` (2025 only) → `base` → `base_p50` → max of the Jun–Sep monthly `peak_mw` (`ercot_adjusted`/`base`) → `annual` `base` (2013 only) | The coincident hourly system peak at normal weather, ERCOT's own adopted number. Zone sums (`ncp_zone_sum_mw`) are non-coincident and `gross_*` adds rooftop PV back, so neither matches metered system load. Where both exist, `base` = `base_p50` = max of the monthly peaks (LTLF 2022: P50 77,733 vs monthly 77,579, 0.2%). |
| CDR | ERCOT-level summer `peak_mw` with no scenario ("Summer Peak Demand, normal weather"); a same-month revision replaces the original (`May 2025 Revised`) | The report's headline load line. `firm_peak_mw` nets out load resources and `peak_before_ee_mw` adds efficiency back. 139 of the 213 CDR values for targets 2014+ are an LTLF number re-published, so CDR is not independent evidence. |
| LTLF-prelim | Manual `peak_demand`, summer 2026, `base` = 112,000 MW (range 90,500–98,000 MW kept apart) | The 2026 preliminary LTLF has no tabular file (`config/manual_official_figures.yaml`, verified). It's kept out of the LTLF horizon summary: it's one point, preliminary, and ERCOT didn't adopt it. |

Sensitivity kept outside the base series: LTLF 2025 `tsp_provided` (the TSP-reported large loads without
ERCOT's adjustment).

## 2. Actual summer peak by year

**Definition:** the ERCOT system **hourly** peak (`ercot_monthly_peaks.metric = 'peak_hourly_mw'`, "net system
maximum hourly demand", hour ending), the max over June–September. The system peak is coincident by definition.
The LTLF and CDR forecast this same quantity, and ERCOT's own Demand and Energy workbook prints its LTLF
forecast next to it: `forecast_peak_hourly_mw` for Aug 2025 = 90,472 = LTLF 2024 summer 2025, and for Aug 2026
= 94,650 = LTLF 2025 ERCOT-adjusted summer 2026. The 15-minute peak runs 35–262 MW (≤ 0.4%) above the hourly
peak and changes no conclusion.

| Year | Hourly peak MW | Date, hour ending (CPT) | 15-min peak MW | 15-min interval ending | Note |
|---|---:|---|---:|---|---|
| 2008 | 62,174 | 2008-08-04 HE 17 | 62,266 | 08-04 17:00 | no final flag before 2015 |
| 2009 | 63,400 | 2009-07-13 HE 17 | 63,518 | 07-13 17:00 | |
| 2010 | 65,776 | 2010-08-23 HE 17 | 66,028 | 08-23 16:00 | |
| 2011 | 68,379 | 2011-08-03 HE 17 | 68,416 | 08-03 17:00 | |
| 2012 | 66,548 | 2012-06-26 HE 17 | 66,583 | 08-01 17:00 | |
| 2013 | 67,245 | 2013-08-07 HE 17 | 67,328 | 08-07 16:45 | |
| 2014 | 66,454 | 2014-08-25 HE 17 | 66,580 | 08-25 17:00 | |
| 2015 | 69,877 | 2015-08-10 HE 17 | 69,942 | 08-10 17:00 | |
| 2016 | 71,110 | 2016-08-11 HE 17 | 71,150 | 08-11 16:30 | |
| 2017 | 69,512 | 2017-07-28 HE 17 | 69,629 | 07-28 17:00 | |
| 2018 | 73,473 | 2018-07-19 HE 17 | 73,539 | 07-19 17:00 | |
| 2019 | 74,820 | 2019-08-12 HE 17 | 74,897 | 08-12 17:00 | |
| 2020 | 74,376 | 2020-08-14 HE 17 | 74,420 | 08-13 16:45 | |
| 2021 | 73,687 | 2021-08-24 HE 17 | 73,822 | 08-24 17:00 | |
| 2022 | 80,148 | 2022-07-20 HE 17 | 80,233 | 07-20 16:45 | |
| 2023 | 85,508 | 2023-08-10 HE 18 | 85,637 | 08-10 17:00 | the record until 2026 |
| 2024 | 85,245 | 2024-08-20 HE 18 | 85,357 | 08-20 17:00 | |
| 2025 | 83,707 | 2025-08-18 HE 18 | 83,968 | 08-18 17:00 | |
| **2026** | **91,134** | **2026-07-22 HE 18** | **91,263** | **07-22 17:30** | Jun–Aug published; Jul–Aug not final-settled |

**2026 detail.** The summer 2026 peak is **91,133.7 MW** in the hour ending **18:00 CDT on 2026-07-22**
(17:00–18:00 CDT, `peak_ts_utc` 2026-07-22 23:00 UTC). The 15-minute peak is 91,262.7 MW (interval ending
17:30 CDT). **data_as_of:**
- `DemandandEnergy2026-for-Corp-Comms.xlsx`, lake `raw/source=ercot_demand_energy/dt=2026-09-26/`, ingested
  2026-09-26 14:59 UTC. It covers January–August 2026. July and August don't carry the final-settlement flag.
- Cross-check against `ercot_load_hourly_wz` (the `ERCOT` row): the same 91,133.7 MW in the same hour. Data
  runs through hour ending 2026-09-26 00:00 CDT, and September comes from the preliminary `ercot_load_wz_daily`.
  The September max so far is 86,492 MW (2026-09-12 HE 17), 4.6 GW under the July peak, so summer 2026 is
  settled except for final-settlement revisions.

91,134 MW is a new all-time hourly record in this data (the previous one was 85,508 MW on 2023-08-10, the
"current all-time peak 85,508 MW" in ERCOT's April 2026 letter). It's preliminary until final settlement.

## 3. Error by vintage × target year

LTLF (plus the 2026 preliminary), error % (horizon):

| Vintage (date) | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| LTLF 2013 (2014-05-02)* | +1.1 (h0) | +5.0 (h1) | +3.1 (h2) | +4.3 (h3) | +8.5 (h4) | +3.7 (h5) | +2.8 (h6) | +4.3 (h7) | +6.4 (h8) | -1.4 (h9) |  |  |  |  |
| LTLF 2014 (2014-09-16)* |  | +2.5 (h0) | -1.2 (h1) | -1.5 (h2) | +2.0 (h3) | -2.3 (h4) | -2.6 (h5) | -0.8 (h6) | +1.4 (h7) | -5.6 (h8) | -10.5 (h9) | -9.1 (h10) |  |  |
| LTLF 2016 (2016-01-04) |  |  |  | -0.7 (h1) | +2.7 (h2) | -1.6 (h3) | -1.5 (h4) | -0.1 (h5) | +1.7 (h6) | -5.6 (h7) | -10.7 (h8) | -9.6 (h9) | -7.1 (h10) |  |
| LTLF 2017 (2016-12-12) |  |  |  |  | +4.9 (h1) | +0.9 (h2) | +1.0 (h3) | +2.9 (h4) | +5.1 (h5) | -2.2 (h6) | -7.2 (h7) | -5.8 (h8) | -2.9 (h9) | -9.7 (h10) |
| LTLF 2018 (2017-12-13) |  |  |  |  |  | -0.7 (h1) | -0.2 (h2) | +2.0 (h3) | +4.7 (h4) | -2.0 (h5) | -6.5 (h6) | -4.7 (h7) | -1.6 (h8) | -8.2 (h9) |
| LTLF 2019 (2018-12-11) |  |  |  |  |  |  | +0.0 (h1) | +3.3 (h2) | +7.0 (h3) | +0.4 (h4) | -4.0 (h5) | -1.8 (h6) | +1.9 (h7) | -4.6 (h8) |
| LTLF 2020 (2019-12-10) |  |  |  |  |  |  |  | +3.1 (h1) | +6.3 (h2) | -0.1 (h3) | -4.6 (h4) | -2.7 (h5) | +0.6 (h6) | -6.3 (h7) |
| LTLF 2021 (2020-12-28) |  |  |  |  |  |  |  |  | +4.8 (h1) | -1.6 (h2) | -6.1 (h3) | -4.7 (h4) | -2.0 (h5) | -9.1 (h6) |
| LTLF 2022 (2022-02-24) |  |  |  |  |  |  |  |  |  | -3.0 (h1) | -7.2 (h2) | -5.5 (h3) | -2.5 (h4) | -9.4 (h5) |
| LTLF 2023 (2023-01-18) |  |  |  |  |  |  |  |  |  |  | -3.7 (h1) | -1.1 (h2) | +2.4 (h3) | -4.4 (h4) |
| LTLF 2024 (2024-07-18) |  |  |  |  |  |  |  |  |  |  |  | +0.9 (h0) | +8.1 (h1) | **+16.8 (h2)** |
| LTLF 2025 (2025-04-08), ERCOT-adjusted |  |  |  |  |  |  |  |  |  |  |  |  | +2.5 (h1) | +3.9 (h2) |
| *LTLF 2025, TSP-provided (sensitivity)* |  |  |  |  |  |  |  |  |  |  |  |  | *+11.9 (h1)* | ***+19.6 (h2)*** |
| 2026 preliminary LTLF (2026-04-15) |  |  |  |  |  |  |  |  |  |  |  |  |  | **+22.9 (h1)** |

\* The dates look like upload dates (see review items). LTLF 2015 is not in the table (no vintage stored).

CDR, vintages from Dec 2019 (the full 45-vintage set, 2008–2026 targets, is in `q1_errors.csv`):

| Vintage (date) | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---:|---:|---:|---:|---:|---:|---:|
| CDR Dec 2019 | +3.1 (h1) | +6.3 (h2) | -0.1 (h3) | -4.6 (h4) | -2.7 (h5) | +0.6 (h6) | -6.3 (h7) |
| CDR May 2020 |  | +6.3 (h2) | -0.1 (h3) | -4.6 (h4) | -2.7 (h5) | +0.6 (h6) | -6.3 (h7) |
| CDR Dec 2020 |  | +4.8 (h1) | -1.6 (h2) | -6.1 (h3) | -4.7 (h4) | -2.0 (h5) | -9.1 (h6) |
| CDR May 2021 |  |  | -1.6 (h2) | -6.1 (h3) | -4.7 (h4) | -2.0 (h5) | -9.1 (h6) |
| CDR Dec 2021 |  |  | -2.6 (h1) | -6.6 (h2) | -4.7 (h3) | -1.6 (h4) | -8.3 (h5) |
| CDR May 2022 |  |  |  | -6.6 (h2) | -4.7 (h3) | -1.6 (h4) | -8.3 (h5) |
| CDR Nov 2022 |  |  |  | -3.2 (h1) | -0.3 (h2) | +3.6 (h3) | -3.0 (h4) |
| CDR May 2023 Revised |  |  |  |  | -0.3 (h2) | +3.6 (h3) | -3.0 (h4) |
| CDR Dec 2023 |  |  |  |  | -1.7 (h1) | +1.2 (h2) | -6.0 (h3) |
| CDR May 2024 Revised |  |  |  |  |  | +1.3 (h2) | -5.5 (h3) |
| CDR Dec 2024 Revised |  |  |  |  |  | +9.8 (h1) | **+18.9 (h2)** |
| CDR May 2025 Revised |  |  |  |  |  |  | +4.7 (h2) |
| CDR Dec 2025 |  |  |  |  |  |  | +4.7 (h1) |

**Summary by horizon** (error %, horizon-0 rows excluded from the decision, LTLF-prelim excluded):

| Product | Horizon | n | Mean % | Median % | Min % | Max % | Share over |
|---|---:|---:|---:|---:|---:|---:|---:|
| LTLF | 1 | 12 | +1.6 | +1.2 | -3.7 | +8.1 | 58% |
| LTLF | 2 | 12 | +2.1 | +1.8 | -7.2 | +16.8 | 58% |
| LTLF | 3 | 10 | +0.5 | +1.5 | -6.1 | +7.0 | 60% |
| LTLF | 4 | 10 | -0.4 | -1.9 | -4.7 | +8.5 | 40% |
| LTLF | 5 | 9 | -1.5 | -2.0 | -9.4 | +5.1 | 22% |
| LTLF | 6 | 8 | -1.9 | -1.3 | -9.1 | +2.8 | 38% |
| LTLF | 7 | 7 | -2.3 | -4.7 | -7.2 | +4.3 | 43% |
| LTLF | 8 | 6 | -3.7 | -5.1 | -10.7 | +6.4 | 17% |
| LTLF | 9 | 5 | -6.5 | -8.2 | -10.5 | -1.4 | 0% |
| LTLF | 10 | 3 | -8.7 | -9.1 | -9.7 | -7.1 | 0% |
| CDR | 1 | 23 | +0.7 | +0.0 | -6.6 | +9.8 | 52% |
| CDR | 2 | 38 | +1.2 | +0.5 | -6.6 | +18.9 | 53% |
| CDR | 3 | 37 | +0.7 | +1.3 | -6.1 | +7.0 | 62% |
| CDR | 4 | 36 | +1.2 | +1.8 | -4.7 | +8.6 | 67% |
| CDR | 5 | 34 | +1.2 | +0.8 | -8.3 | +9.1 | 53% |
| CDR | 6 | 30 | +1.3 | +0.7 | -9.1 | +11.6 | 60% |
| CDR | 7–11 | 70 | -0.7 to -4.4 | -0.8 to -7.1 | -13.8 | +12.3 | 23–47% |

Pooled horizons 1–3: LTLF mean +1.5%, MAPE 3.5% (n = 34); CDR mean +0.9%, MAPE 3.1% (n = 98). Horizons 4–6:
LTLF mean −1.2%, 33% over (n = 27).

**What drives the sign:** the target summer, more than the horizon. Every LTLF vintage over-forecast 2017 and 2021 (a
low-peak summer), and every vintage at horizon 1 or more under-forecast 2023 (−3.7 to −10.7%, the record peak)
and 2024. The pre-2022
vintages missed the 2022–2026 growth, which explains −5 to −10% at 7–10 summers. The large-load era broke that
pattern for 2026: the vintages built on TSP-reported large loads over-shot by 17–23% (below), while the
ERCOT-adjusted ones were +4–5%.

## 4. The 2026 preliminary: ~112 GW vs the 90.5–98 GW range vs the actual

All numbers come from the same ERCOT filing (PUCT Project 58777 item 38, cover letter dated 2026-04-15,
verified in `config/manual_official_figures.yaml`), against the actual hourly peak of 91,134 MW:

| 2026 summer figure | MW | vs actual MW | vs actual % |
|---|---:|---:|---:|
| Preliminary LTLF, "approximately 112,000 MW for summer 2026" | 112,000 | **+20,866** | **+22.9%** |
| ERCOT's own projection, low end ("90,500 MW to 98,000 MW") | 90,500 | −634 | −0.7% |
| ERCOT's own projection, high end | 98,000 | +6,866 | +7.5% |
| LTLF 2024 (2024-07-18), base | 106,405 | +15,271 | +16.8% |
| CDR Dec 2024 Revised | 108,391 | +17,257 | +18.9% |
| LTLF 2025 (2025-04-08), TSP-provided | 109,031 | +17,898 | +19.6% |
| LTLF 2025 (2025-04-08), ERCOT-adjusted | 94,650 | +3,517 | +3.9% |
| CDR May 2025 Revised / Dec 2025 | 95,419 | +4,286 | +4.7% |

Against the 15-minute peak (91,263 MW) the preliminary's miss is 20,737 MW.

**Pitch claim check.** "ERCOT's official preliminary 2026 forecast (~112 GW) missed that same year's peak by
more than 20 GW": **confirmed**, 20.9 GW (+22.9%), on the peak definition the forecast uses. Two caveats should
travel with it:
1. In the same letter, ERCOT itself said it expected summer 2026 at 90.5–98 GW. The actual landed inside that
   range, 0.7% above its low end. The 112 GW is the preliminary LTLF built on TSP large-load submissions (and is
   under the PUCT 59772 adjustment); ERCOT's own operational projection didn't carry it.
2. The margin over "20 GW" is 866 MW on the hourly peak (737 MW on the 15-minute peak), and July is not yet
   final-settled. Only an upward revision of about 0.95% would break the claim. How large final-settlement
   revisions usually are is not verified.

Suggested wording: "ERCOT's preliminary long-term forecast put summer 2026 at about 112 GW. The grid peaked at
91.1 GW on July 22, 21 GW lower. ERCOT's own adjusted view (90.5–98 GW) got it; the forecasts built on raw
TSP large-load filings missed by 15–21 GW."

## Decision

Rule (handoff): the same sign in most vintages at each horizon → "the official forecast is X% too high/low at Y
years"; a mixed sign → the narrative rests on the 2026 preliminary and the spread between vintages.

- **LTLF: mixed.** The share of over-forecasts is 58%, 58% and 60% at horizons 1–3, then 40% and 22% at 4–5:
  the direction flips with the horizon. That holds at either threshold (simple majority or ≥ 2/3).
- **CDR: mixed at ≥ 2/3** (52–67% over at 1–5). At a bare majority it reads "consistent over", but four of
  those five horizons sit at 52–62%, one vintage away from flipping, and 139 of 213 values are re-published LTLF
  numbers.
- **Verdict: mixed sign.** The narrative rests on the 2026 preliminary (112 GW vs 91.1 GW, +22.9%) and on the
  dispersion between vintages: TSP-based vintages were +17 to +23% for 2026, ERCOT-adjusted +4 to +5%, and in
  normal years the normal-weather LTLF is within about ±3.5% at 1–3 summers, with the sign set by the summer's
  weather. **A1 proceeds.**

## Proposed findings row

| Q1 | 2026 summer peak = 91,134 MW hourly (HE 18 CDT, 2026-07-22; 15-min 91,263 MW at 17:30) (data_as_of 2026-09-26: D&E workbook through Aug, Jul–Aug not final-settled; hourly load through 2026-09-26 00:00 CDT, Sept max 86.5 GW); preliminary 112 GW error = +20.9 GW / +22.9% (ERCOT's own 90.5–98 GW range contained the actual); mean error by horizon, LTLF: 1y +1.6%, 3y +0.5%, 5y −1.5% (share over 58% / 60% / 22%); 2026 by vintage: TSP-based +16.8 to +22.9%, ERCOT-adjusted +3.9 to +4.7% | narrative: mixed sign → 2026 preliminary + vintage dispersion (raw TSP-based vs ERCOT-adjusted), not "X% at Y years" | A1 | file |

## Review items for Pablo

1. **Pitch wording.** The "> 20 GW" claim holds (20.9 GW), but ERCOT's own April 2026 range (90.5–98 GW)
   contained the actual. Say "preliminary long-term forecast" and don't imply ERCOT operations expected 112 GW
   (suggested wording in §4).
2. **"Demand record of ~87–91 GW" (Jan 2026) in the product context doesn't match the data.** The record in Jan
   2026 was 85,508 MW (2023-08-10, hourly), the same number ERCOT's April 2026 letter gives. The 91,134 MW record
   came later (2026-07-22, preliminary). The line should become "record 85.5 GW (2023), broken in July 2026 at
   91.1 GW".
3. **"Consistent sign" threshold.** Claude used ≥ 2/3 of vintages at every horizon 1–5 with n ≥ 3. The handoff
   says "maioria". At 50% the CDR reads "consistent over" (52–67%) and the LTLF stays mixed. The decision
   doesn't change, because the LTLF, the independent series, is mixed at both thresholds.
4. **Vintage dates of LTLF 2013 and 2014 look like file upload dates** (2014-05-02 and 2014-09-16). Their values
   match CDR Dec 2012 and CDR Feb 2014, so their real horizons are probably one summer longer. Not verified. The
   impact is small: two vintages and 3 of the 12 h1 points.
5. **2026 is preliminary.** July–August are not final-settled, and September comes from the daily weather-zone
   load. Re-run once the December workbook is out.

## Proposed decisions.md lines

- 2026-09-26 — The backtest's actual peak is the ERCOT system hourly peak (`ercot_monthly_peaks.peak_hourly_mw`, hour ending), max over June–September; the base forecast series are LTLF ERCOT-level summer `peak_mw` (ERCOT-adjusted in 2025, else base, else P50, else max of Jun–Sep monthly), CDR summer `peak_mw` without scenario (same-month revision wins), and the manual 2026 preliminary kept apart. — LTLF and CDR forecast the coincident hourly system peak, and ERCOT's Demand and Energy workbook prints its LTLF next to that same number; zone sums, firm and gross peaks measure something else.
- 2026-09-26 — Backtest horizon = summers ahead (1 = first summer starting after the vintage date; a vintage dated June or later sits in horizon 0), error = (forecast − actual) / actual. — LTLF and CDR editions come out in Dec, Jan, Apr, May and Jul, so calendar-year differences mislabel May vs December editions.
- 2026-09-26 — Q1: the official forecasts' error sign is mixed by horizon (LTLF over-forecasts in 58–60% of vintages at 1–3 summers, in 22–40% at 4–5), so the video uses the 2026 preliminary (112 GW vs 91.1 GW actual, +22.9%) and the TSP-based vs ERCOT-adjusted spread (+17–23% vs +4–5% for 2026), not "ERCOT errs X% at Y years". — Rule in PHASE0_ANALYSIS Q1; the sign follows the target summer's weather more than the horizon.
- 2026-09-26 — "Consistent sign" in Q1 means ≥ 2/3 of vintages with the same sign at every horizon 1–5 with n ≥ 3. — A bare majority flips on a single vintage (CDR 52–53% at horizons 1, 2 and 5).

## Useful for the core features

- **/backtest screen:** `match_forecasts()` already has the mart's shape (product, vintage, vintage_date,
  target_year, horizon, forecast_mw, actual_mw, error_mw, error_pct). The two views that carry the story are
  the vintage fan (every LTLF vintage vs the actual line, with the 112 GW star and the 90.5–98 GW bar;
  `analysis/out/q1_backtest.png`) and the vintage × target error matrix of §3.
- **The bar basecast has to beat:** at 1–3 summers the normal-weather LTLF misses by 3.5% on average (MAPE),
  and ERCOT-adjusted 2025 was +3.9% for 2026. basecast's P50 should be scored against those numbers with the
  same `horizon_summary`, and its P10/P90 band should cover the ±5–7% weather swing (2023: −7%; 2021: +7%).
- **Explorer/Forecast framing:** most of the 2026 miss is the large-load assumption, not the weather. Within
  one vintage (LTLF 2025, same weather basis), the TSP-provided series was +19.6% and the ERCOT-adjusted one
  +3.9%. That's the "raw vs adjusted queue" toggle on one number: 109.0 vs 94.7 vs 91.1 GW actual.
- **Video line (numbers from this file):** "ERCOT's preliminary forecast said 112 GW for summer 2026. The grid
  peaked at 91.1 GW on July 22. A year earlier, the same forecast said 109 GW with the large-load filings as
  submitted and 94.7 GW after ERCOT's haircut. Knowing how much of the queue gets built is the whole game."
