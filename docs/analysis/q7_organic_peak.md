# Q7 — Do weather and trend explain the summer peak by zone?

Phase 0, `docs/PHASE0_ANALYSIS.md` §2 Q7. Run on 2026-09-26 against production Postgres (`basecast_reader`):
`ercot_load_hourly_wz` through 2026-09-25 (operating day) and `weather_hourly_wz` (Open-Meteo ERA5) through
2026-09-20 23:00 UTC.

- Script: `analysis/q7_organic_peak.py` (`uv run --group analysis python analysis/q7_organic_peak.py`, ~10 s)
- Model code: `basecast_pipelines/models/weather_load.py` (loaders `load_hourly_load()`, `load_hourly_weather()`,
  `load_official_hourly_peaks()`; pure `load_alignment_checks()`, `weather_daily()`, `summer_peaks()`,
  `summer_weather_features()`, `zone_weights()`, `system_weather_daily()`, `fit_peak()`, `holdout()`, `mape()`,
  `rolling_origin()`, `sup_chow()`); tests in `tests/models/test_weather_load.py` (synthetic, DST days included)
- Output (gitignored): `analysis/out/q7_holdout_by_zone.png` (actual vs. fit ≤ 2022 vs. hold-out, per zone)

Definitions used throughout:

- **Summer peak** = highest hourly MW over the operating days of June–September. Zones: each zone's own
  (non-coincident) peak. ERCOT: the system peak (the table's `ERCOT` series).
- **Weather variable (default)** = `t_mean_3d`: the maximum, over June–September, of the trailing 3-day mean of
  the zone's daily mean temperature (America/Chicago days). Also tested: the same on daily max temperature
  (`t_max_3d`), daily mean dew point (`td_mean_3d`) and daily max NWS heat index (`hi_max_3d`).
- **ERCOT weather** = the zones' daily values weighted by each zone's share of June–September energy over
  2003–2022 (training years only): COAST 0.286, NCENT 0.333, SCENT 0.167, SOUTH 0.079, FWEST 0.048,
  EAST 0.036, WEST 0.027, NORTH 0.024.
- **Model** = OLS `peak_mw ~ 1 + (year − 2014) + weather`, one row per summer, per zone.

## 1. Time alignment checks

| Check | Result |
|---|---|
| Which load table | `ercot_load_hourly_wz` is already the union: `_archive` covers 2003-01-01 → 2026-09-01 05:00 UTC (1,867,086 rows) and wins; `_np6345` (2026-08-24 → 2026-09-26, 7,128 rows) only fills September 2026. **2003–2025 is 100% archive.** |
| `ts_utc` = end of the hour-ending interval | `ts_utc − 1 h` in America/Chicago falls on `operating_date` for **all 1,872,486 rows** (0 mismatches). Days are therefore aggregated by `operating_date`; HE 24 stays in the day it closes. |
| Hours per operating day | 8,623 days × 24 h, 24 days × 23 h (every spring-forward 2003–2026), 22 days × 25 h. Fall-back 2016-11-06 has 24 h: its HE 24 is missing, **the only gap in 23.7 years** (outside summer). |
| ERCOT total present? zones sum to it? | `ERCOT` is its own series. Sum of the 8 zones − ERCOT: mean \|diff\| 0.3 MW; 48 hours differ by > 1 MW (max 2,360 MW, 2021-05-17/18), **none in June–September** (summer max 0.03 MW). |
| Zone names | Identical in both tables: COAST, EAST, FWEST, NCENT, NORTH, SCENT, SOUTH, WEST (load adds ERCOT). |
| Weather coverage | 207,936 hours per zone (2003-01-01 00:00 → 2026-09-20 23:00 UTC), no null temperature or dew point, `n_points` constant (1, 2 or 3 per zone as in `config/weather_points.yaml`). |
| Weather in GMT → Chicago days | DST days hold 23 and 25 instants (192 = 24 × 8 and 184 = 23 × 8 zone-days). Edge days are partial (6 h on 2002-12-31, 19 h on 2026-09-20) and outside the summers used. |
| Diurnal sanity (July means) | Temperature peaks at 16:00 local in 7 zones (15:00 SOUTH); load peaks at HE 17 in 7 zones (HE 18 SCENT), i.e. the hour 16:00–17:00. A UTC/local slip would show as a 5–6 h shift. |
| Cross-check vs. `ercot_monthly_peaks` (`peak_hourly_mw`, 2008–2025) | The June–September system peak from the hourly table is within **−0.37% … +0.02%** of ERCOT's published hourly peak every year (hourly table slightly lower in most years). |
| Summer completeness | 2003–2025: 2,928 hours and 122 weather days in every summer. 2026: 2,808 load hours (through Sep 25) and 112 weather days (through Sep 20): **partial**. |

## 2. Summer peak series (MW, June–September)

Zones are non-coincident peaks; ERCOT is the system peak. 2026 is partial (see above).

| Year | COAST | EAST | FWEST | NCENT | NORTH | SCENT | SOUTH | WEST | ERCOT |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2003 | 15,822 | 2,320 | 1,805 | 22,445 | 2,108 | 10,064 | 4,052 | 1,675 | 60,030 |
| 2004 | 16,642 | 2,335 | 1,735 | 20,759 | 2,070 | 9,835 | 4,178 | 1,605 | 58,484 |
| 2005 | 16,484 | 2,405 | 1,760 | 21,975 | 2,109 | 10,535 | 4,346 | 1,597 | 60,213 |
| 2006 | 16,746 | 2,481 | 1,869 | 22,746 | 2,306 | 10,906 | 4,278 | 1,741 | 62,203 |
| 2007 | 18,227 | 2,337 | 1,767 | 22,229 | 2,156 | 10,454 | 3,993 | 1,497 | 62,115 |
| 2008 | 17,623 | 2,373 | 1,886 | 22,595 | 2,258 | 11,299 | 4,281 | 1,631 | 62,103 |
| 2009 | 18,268 | 2,444 | 1,739 | 23,405 | 1,498 | 11,089 | 4,851 | 1,768 | 63,407 |
| 2010 | 18,064 | 2,429 | 1,867 | 24,533 | 1,512 | 11,324 | 4,807 | 1,851 | 65,713 |
| 2011 | 19,321 | 2,570 | 2,103 | 25,626 | 1,564 | 11,735 | 5,093 | 1,854 | 68,318 |
| 2012 | 18,411 | 2,421 | 2,172 | 24,748 | 1,559 | 11,641 | 5,241 | 1,849 | 66,558 |
| 2013 | 18,770 | 2,379 | 2,279 | 24,421 | 1,483 | 11,433 | 5,207 | 1,862 | 67,253 |
| 2014 | 18,578 | 2,325 | 2,688 | 23,446 | 1,408 | 11,452 | 5,352 | 1,853 | 66,464 |
| 2015 | 19,929 | 2,464 | 2,812 | 24,581 | 1,452 | 12,033 | 5,455 | 1,884 | 69,620 |
| 2016 | 19,826 | 2,494 | 2,909 | 25,282 | 1,440 | 12,345 | 5,787 | 1,899 | 71,093 |
| 2017 | 20,101 | 2,416 | 3,164 | 24,313 | 1,394 | 11,970 | 5,845 | 1,902 | 69,496 |
| 2018 | 20,270 | 2,566 | 3,655 | 26,499 | 1,522 | 12,887 | 5,791 | 2,084 | 73,308 |
| 2019 | 21,256 | 2,554 | 4,308 | 25,494 | 1,476 | 12,785 | 6,040 | 2,117 | 74,666 |
| 2020 | 20,905 | 2,871 | 4,439 | 25,510 | 1,420 | 13,011 | 5,850 | 2,058 | 74,328 |
| 2021 | 21,158 | 2,842 | 5,128 | 24,674 | 1,396 | 12,321 | 5,996 | 1,925 | 73,651 |
| 2022 | 22,014 | 3,004 | 5,475 | 27,200 | 1,997 | 14,143 | 6,270 | 2,075 | 80,038 |
| 2023 | 23,963 | 3,272 | 6,640 | 28,313 | 2,105 | 15,174 | 6,608 | 2,195 | 85,464 |
| 2024 | 23,180 | 3,004 | 7,560 | 27,803 | 2,362 | 15,665 | 6,761 | 2,230 | 85,199 |
| 2025 | 23,007 | 2,989 | 8,773 | 27,532 | 2,732 | 14,669 | 6,853 | 2,113 | 83,679 |
| 2026 (partial) | 24,221 | 3,322 | 8,725 | 29,755 | 2,772 | 15,934 | 7,418 | 2,912 | 91,134 |

Things the table shows before any model:

- **FWEST** grows ~4.9× (1.8 → 8.8 GW), accelerating since ~2010. Cause (Permian oil & gas, large loads) not
  verified from this data.
- **NORTH** drops a level in 2009 (2,258 → 1,498 MW) and jumps from 2022 (1,396 → 1,997 → 2,732 MW in 2025).
  Neither is weather; causes (zone composition change in 2009? large flexible loads after 2021?) not verified.
- The **ERCOT peak hour** moved from HE 17 (every summer 2008–2022) to HE 18 (2023–2026). Cause not verified.
- System peak / sum of the zones' own peaks = **0.979** on average (0.944–0.996), 2003–2025.

## 3. Fit `peak ~ year + t_mean_3d`, 2003–2025 (n = 23 per zone)

| Zone | Mean peak (MW) | Trend MW/yr (SE) | Weather MW/°C (SE) | R² | Residual SD MW (% of mean) |
|---|---:|---:|---:|---:|---:|
| COAST | 19,503 | 318 (16) | 305 (115) | 0.953 | 508 (2.6%) |
| EAST | 2,578 | 32.2 (4.9) | 39.9 (23.8) | 0.710 | 155 (6.0%) |
| FWEST | 3,414 | 284 (36) | −118 (144) | 0.800 | 959 (28.1%) |
| NCENT | 24,614 | 247 (15) | 524 (71) | 0.949 | 477 (1.9%) |
| NORTH | 1,797 | 0.2 (14.6) | −45 (70) | 0.024 | 420 (23.4%) |
| SCENT | 12,120 | 201 (18) | 287 (117) | 0.892 | 541 (4.5%) |
| SOUTH | 5,345 | 121.9 (4.1) | 248 (52) | 0.983 | 123 (2.3%) |
| WEST | 1,881 | 23.3 (2.1) | 40.8 (9.7) | 0.920 | 60 (3.2%) |
| **ERCOT** | 69,713 | 1,107 (68) | 1,201 (476) | **0.937** | 2,120 (3.0%) |

R² by weather variable (same model, 2003–2025):

| Zone | t_mean_3d | t_max_3d | td_mean_3d | hi_max_3d |
|---|---:|---:|---:|---:|
| COAST | 0.953 | 0.952 | 0.937 | 0.948 |
| EAST | 0.710 | 0.716 | 0.670 | 0.671 |
| FWEST | 0.800 | 0.805 | 0.801 | 0.819 |
| NCENT | 0.949 | 0.950 | 0.821 | 0.834 |
| NORTH | 0.024 | 0.031 | 0.040 | 0.114 |
| SCENT | 0.892 | 0.893 | 0.865 | 0.889 |
| SOUTH | 0.983 | 0.983 | 0.965 | 0.966 |
| WEST | 0.920 | 0.910 | 0.876 | 0.885 |
| ERCOT | 0.937 | 0.939 | 0.917 | 0.924 |

- The high R² is mostly **trend**. The weather feature varies little between summers (SD ~1 °C for ERCOT,
  0.5–1.7 °C by zone), so one SD of weather moves the ERCOT peak by ~1.2 GW.
- Daily mean vs. daily max temperature: same R² (±0.01). Dew point and heat index do not help, except
  heat index for NORTH (still 0.11). The default stays `t_mean_3d`.
- FWEST and NORTH have **negative, insignificant** weather coefficients: their peaks are not weather-driven.

## 4. Hold-out: fit ≤ 2022, predict 2023–2025 with actual weather

Per-year errors, default model (predicted − actual, % of actual):

| Zone | 2023 | 2024 | 2025 | **MAPE** |
|---|---:|---:|---:|---:|
| COAST | −6.7 | −4.6 | −4.0 | 5.1 |
| EAST | −14.1 | −8.4 | −7.3 | 9.9 |
| FWEST | −28.8 | −35.1 | −42.0 | **35.3** |
| NCENT | −1.7 | −4.5 | −4.2 | 3.5 |
| NORTH | −37.9 | −46.6 | −56.4 | **47.0** |
| SCENT | −10.0 | −12.1 | −8.3 | **10.1** |
| SOUTH | +1.0 | −2.9 | −4.0 | 2.6 |
| WEST | −1.5 | −1.7 | −3.0 | 2.1 |
| **ERCOT** | −6.9 | −8.5 | −7.4 | **7.6** |

Every miss but one is an **under-prediction**: 2023–2025 sit above the 2003–2022 trend. ERCOT predicted
79.5 / 78.0 / 77.5 GW against actual 85.5 / 85.2 / 83.7 GW.

Variants (hold-out MAPE %, same split). "trend only" drops the weather term; log = `log(peak)`; piecewise = a
second trend slope after a knot chosen by least squares on 2003–2022 only (ERCOT knot: 2012):

| Zone | default | trend only | log | piecewise | t_max_3d linear | best of 12 variants |
|---|---:|---:|---:|---:|---:|---:|
| COAST | 5.1 | 4.9 | 4.1 | 4.1 | 4.8 | 3.5 (t_max, piecewise) |
| EAST | 9.9 | 9.8 | 10.1 | 6.5 | 9.7 | 6.5 (t_mean, piecewise) |
| FWEST | 35.3 | 35.3 | 30.5 | 17.0 | 35.3 | 16.6 (t_max, piecewise) |
| NCENT | 3.5 | 3.7 | 2.9 | 2.9 | 2.9 | 2.3 (t_max, log) |
| NORTH | 47.0 | 46.4 | 45.1 | 28.5 | 46.5 | 25.6 (hi_max, piecewise) |
| SCENT | 10.1 | 10.2 | 9.3 | 7.2 | 9.7 | 6.4 (t_max, piecewise) |
| SOUTH | 2.6 | 2.9 | 2.1 | 4.3 | 1.2 | 1.2 (t_max, linear) |
| WEST | 2.1 | 3.5 | 1.2 | 6.4 | 1.1 | 0.9 (t_max, log) |
| **ERCOT** | **7.6** | 7.5 | 6.9 | 5.6 | 7.2 | 4.0 (hi_max, piecewise) |

- **Weather adds nothing out of sample for the total:** trend only 7.5% vs. 7.6% with temperature.
- No linear or log variant brings ERCOT under 5%. Only piecewise variants with a non-default weather variable
  do (4.0–4.8%), and they were picked after seeing the test years (12 variants × 1 split), so they are not
  evidence for plan A.

One-year-ahead rolling origin (fit on years < Y, predict Y, Y = 2013–2025), default model:

| Zone | MAPE | Worst year (APE) |
|---|---:|---|
| COAST | 2.1 | 2023 (6.7) |
| EAST | 5.3 | 2023 (14.1) |
| FWEST | 21.1 | 2025 (34.7) |
| NCENT | 1.7 | 2024 (4.3) |
| NORTH | 19.9 | 2025 (43.6) |
| SCENT | 4.0 | 2024 (10.1) |
| SOUTH | 2.0 | 2020 (5.7) |
| WEST | 2.6 | 2022 (5.2) |
| **ERCOT** | **2.8** | 2024 (7.5) |

The model works one year ahead (ERCOT 2.8%). It fails three years ahead across the 2022–2023 step.

## 5. Structural breaks

Two tests on the full-sample (2003–2025) default model:

- **sup-Chow:** Quandt's sup-F over break years, where all three coefficients change and each side keeps
  ≥ 5 years (candidates 2008–2021). The p-value is a parametric bootstrap with 2,000 draws that accounts for
  the search; its floor is 0.0005.
- **OLS-CUSUM:** Ploberger–Krämer, `statsmodels.breaks_cusumolsresid`.

| Zone | sup-Chow break (first year of new regime) | sup-F | p (bootstrap) | CUSUM p |
|---|---|---:|---:|---:|
| FWEST | 2017 | 66.3 | < 0.001 | 0.27 |
| EAST | 2020 | 34.6 | < 0.001 | 0.15 |
| ERCOT | 2020 | 31.6 | < 0.001 | 0.19 |
| SCENT | 2020 | 31.6 | < 0.001 | 0.20 |
| NORTH | 2020 | 25.7 | < 0.001 | 0.19 |
| SOUTH | 2020 | 7.8 | 0.017 | 0.91 |
| COAST | 2012 | 5.2 | 0.067 | 0.46 |
| NCENT | 2020 | 4.4 | 0.12 | 0.68 |
| WEST | 2009 | 3.1 | 0.32 | 0.94 |

- **Significant breaks:** ERCOT, FWEST, NORTH, SCENT and EAST (p < 0.001), and SOUTH (p = 0.017).
  COAST, NCENT and WEST show none.
- **2020 is close to the last admissible candidate (2021).** The real break is probably 2022–2023, and five
  years per side cannot place it more precisely.
- **CUSUM detects nothing (p ≥ 0.15).** With 23 points and a break at the end of the sample, it has little
  power.
- **FWEST breaks earlier (2017)**, as the handoff expected: the linear trend is wrong from the start.
- **ERCOT residual pattern** (full-sample fit, MW): +1.0 to +2.5 GW in 2003–2007, mostly −0.6 to −2.7 GW in
  2008–2021, then +3.4, +4.4 and +3.2 GW in 2023–2025. That is a convex, accelerating path that a straight trend
  cannot follow.

## 6. 2026 so far vs. the weather-normalized expectation

The model is fit on 2003–2025 and fed 2026's actual weather through Sep 20. The weather band is the fitted peak
at the P10/P50/P90 of 2003–2025's weather feature, for year 2026. The residual band is ±1.2816 × residual SD.

| Zone | 2026 peak (MW) | Peak hour (CDT, hour ending) | Expected, 2026 weather | Gap | Weather P10 / P50 / P90 | ± residual band |
|---|---:|---|---:|---:|---|---:|
| COAST | 24,221 | Aug 26, 17:00 | 23,310 | +3.9% | 22,989 / 23,303 / 23,667 | 652 |
| EAST | 3,322 | Aug 31, 17:00 | 3,015 | +10.2% | 2,906 / 2,965 / 3,041 | 198 |
| FWEST | 8,725 | Jul 24, 16:00 | 6,775 | +28.8% | 7,035 / 6,852 / 6,580 (neg. coef) | 1,229 |
| NCENT | 29,755 | Aug 24, 17:00 | 28,313 | +5.1% | 26,832 / 27,503 / 28,669 | 611 |
| NORTH | 2,772 | Sep 4, 17:00 | 1,719 | +61.2% | 1,880 / 1,800 / 1,715 (neg. coef) | 539 |
| SCENT | 15,934 | Aug 27, 16:00 | 14,645 | +8.8% | 14,148 / 14,533 / 14,854 | 693 |
| SOUTH | 7,418 | Sep 3, 17:00 | 6,953 | +6.7% | 6,689 / 6,790 / 6,973 | 157 |
| WEST | 2,912 | Sep 12, 16:00 | 2,185 | +33.3% | 2,076 / 2,152 / 2,248 | 76 |
| **ERCOT** | **91,134** | **Jul 22, 18:00** | **84,169** | **+8.3%** | 81,597 / 83,014 / 84,479 | 2,717 |

- **ERCOT:** 2026's summer peak (91,134 MW, equal to `ercot_monthly_peaks`) is **~7.0 GW above** what
  weather + trend expect for 2026's actual weather. It is above both the weather P90 (84.5 GW) and
  P50 + residual band (86.9 GW).
- **Caveat:** the same fit already under-shoots 2023–2025 by 3.2–4.4 GW, so part of the 7 GW is the
  model's missed step, not 2026 alone.
- **Weather spread is narrow:** for ERCOT, weather alone spans P10–P90 = 2.9 GW, less than the residual band
  (±2.7 GW).

## Decision

Rule (`docs/PHASE0_ANALYSIS.md` §2 Q7, default thresholds): ERCOT hold-out MAPE ≤ 5% → A3 as planned; zone
MAPE > 10% → zone flagged; ERCOT > 5% → plan B.

- **ERCOT total hold-out MAPE = 7.6% > 5% → plan B for A3.** The organic peak band comes from the residual
  SD, with no weather simulation.
- **Flagged zones (MAPE > 10%):** NORTH (47.0%), FWEST (35.3%), SCENT (10.1%, marginal). EAST (9.9%) sits just
  under the line.
- **Plan B fits the diagnosis.** The miss is a trend break around 2020–2023, not weather. Weather adds nothing
  out of sample (trend only: 7.5%). Weather's year-to-year spread (±1.4 GW for ERCOT) is smaller than the
  residual SD (2.1 GW). A weather simulation would refine the smallest term in the error.

## Proposed findings row

| Q7 | R² 2003–2025 (peak ~ year + max 3-day mean temp): SOUTH 0.98, COAST 0.95, NCENT 0.95, ERCOT 0.94, WEST 0.92, SCENT 0.89, FWEST 0.80, EAST 0.71, NORTH 0.02; residual SD ERCOT 2.1 GW (3.0%); hold-out (fit ≤ 2022 → 2023–25) MAPE ERCOT = 7.6% (all under-predicted; trend-only 7.5%); zones > 10%: NORTH 47.0%, FWEST 35.3%, SCENT 10.1% (EAST 9.9%); trend break ~2020 (sup-Chow p < 0.001: ERCOT, FWEST, NORTH, SCENT, EAST); 1-yr-ahead MAPE ERCOT 2.8%; 2026 peak 91.1 GW = +8.3% vs. 84.2 GW expected | plan B | A3 | — |

## Review items for Pablo

1. **Confirm plan B.** It holds under every linear and log variant (ERCOT 6.4–7.8%). Only post-hoc piecewise
   variants get under 5% (best 4.0%, heat index + knot). To revisit plan A, the piecewise model would first
   need a pre-registered hold-out, e.g. fit ≤ 2019 → test 2020–2022.
2. **Which residual SD sets the band.** The in-sample SD comes from a model that misses the recent step:
   ERCOT ±2.7 GW at P10/P90, and 2026 landed 7 GW out. Proposed default: the SD of the one-year-ahead
   rolling-origin errors (ERCOT MAPE 2.8%, worst 7.5%), which is honest about the break. The alternative is the
   in-sample SD with a note.
3. **What "organic" means.** The 2003–2025 trend already absorbs large loads that arrived (FWEST since ~2017,
   NORTH since ~2022, WEST in 2026). Stacking the large-load queue on top of it double-counts some load. Proposed
   default: organic = fit on ≤ 2019 (pre-break) for the flagged zones and ERCOT, with the post-2020 excess shown
   as "realized large load". To decide in A3.
4. **FWEST and NORTH.** Proposed: no organic model on screen, only actuals plus the large-load layer, flagged
   "trend not explained by weather". NORTH's 2009 drop and 2022+ jump need a source (zone redefinition?
   crypto?). Not verified.
5. **WEST 2026** (+33% vs. expected, 2,912 MW on Sep 12 vs. 2,113 MW in 2025) looks like a new load, not
   weather. Worth a look before it goes in the video. Not verified.
6. **Thresholds** (5% total, 10% zone) adopted as in the handoff. Pablo can adjust.

## Proposed decisions.md lines

- 2026-09-26 — A3 takes plan B: the weather-normalized (organic) peak band comes from the model's residual spread (peak ~ year + weather per zone), with no weather simulation. — Q7: ERCOT hold-out MAPE (fit ≤ 2022 → 2023–2025) is 7.6%, above the 5% threshold; weather adds nothing out of sample (trend only 7.5%) and its year-to-year spread (±1.4 GW) is smaller than the residual SD (2.1 GW).
- 2026-09-26 — Forecast flags NORTH, FWEST and SCENT (hold-out MAPE 47.0%, 35.3%, 10.1% > 10%); NORTH and FWEST get no organic model, only actuals and the large-load layer. — Q7 rule; their peaks are not weather-driven (negative, insignificant temperature coefficients) and break around 2017–2020.
- 2026-09-26 — Weather feature for the peak model: the June–September maximum of the trailing 3-day mean of the zone's daily mean temperature (America/Chicago days, ERA5); ERCOT uses the zones weighted by 2003–2022 summer energy share. — Daily max temperature gave the same R² (±0.01) and a similar hold-out (ERCOT 7.2% vs. 7.6%); dew point and heat index fit no better (NCENT R² 0.82–0.83 vs. 0.95).
- 2026-09-26 — Q7 thresholds adopted as in the phase 0 handoff: ERCOT hold-out MAPE ≤ 5% → plan A; zone MAPE > 10% → flagged. — Default rule, pending Pablo's review.

## Useful for the core features

- **Forecast screen:**
  - Organic P50 per zone and year = trend + median weather.
  - P10/P90 band = P50 ∓ 1.28 × residual SD (ERCOT ±2.7 GW in-sample; see review item 2).
  - The weather-only spread can be a thinner inner band (ERCOT 2026: 81.6 / 83.0 / 84.5 GW).
  - FWEST and NORTH are flagged; for the total, the zones' own peaks sum to ~1.02× the system peak
    (system / sum = 0.979).
- **2026 vs. the weather-normalized expectation:**
  - ERCOT peaked at **91,134 MW on 2026-07-22, HE 18 CDT**, against **84.2 GW** expected for 2026's actual
    weather: **+7.0 GW (+8.3%)**, above P90 by either band.
  - Largest zone gaps: FWEST +1.95 GW (+29%), NCENT +1.44 GW, SCENT +1.29 GW, NORTH +1.05 GW (+61%),
    WEST +0.73 GW (+33%).
  - This is the "load that is not organic" the queues are about.
- **Backtest screen:** one-year-ahead errors (ERCOT 2.8% MAPE, 2013–2025) against the 3-year hold-out (7.6%).
  Our own naive model degrades with horizon at the 2022–2023 step. That is an honest companion to Q1's official
  forecast errors.
- **Video line (after Pablo's review):** "Weather and 20 years of trend put Texas's 2026 peak at about 84 GW;
  it hit 91. That 7 GW gap is new load, and it lands in FWEST, NORTH and WEST first." Say that 2023–2025 were
  already 3–4 GW above the same fit.
- **For Base's battery story:** the ERCOT peak hour moved from HE 17 (2008–2022) to HE 18 (2023–2026). The
  cause is not verified; check it before using it.
