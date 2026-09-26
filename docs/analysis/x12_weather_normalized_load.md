# X12 — A weather-normalized load time series by weather zone

Exploration for the Forecast module ("weather-normalized load time series"). Run on 2026-09-26 against production
Postgres (`basecast_reader`, read-only): `ercot_load_hourly_wz` through operating day 2026-09-25 and
`weather_hourly_wz` (Open-Meteo ERA5) through 2026-09-20 19:00 UTC. The last day with complete weather is
**2026-09-19**, so every series below stops there.

- Script: `analysis/x12_weather_normalized_load.py` (`uv run --group analysis python analysis/x12_weather_normalized_load.py`, ~13 s)
- Model code: `basecast_pipelines/models/weather_normalized.py` (pure functions; loaders reuse `weather_load` and
  `peak_excess.daily_load_stats`, both unchanged); tests: `tests/models/test_weather_normalized.py` (synthetic, a
  spring-forward day, no DB)
- Outputs (gitignored): `analysis/out/x12_daily.parquet` (every zone-day: actual, fitted, weather terms, normalized),
  `x12_monthly.csv`, `x12_annual.csv`, `x12_summer_peak.csv`, `x12_growth.csv`, `x12_slopes.csv`,
  `x12_holdout_mape.csv`, `x12_holdout_days.csv`, `x12_residual_acf.csv`, `x12_variants.csv`, figures
  `x12_normalized_by_zone.png` (trailing-12-month average load, actual vs normalized, per zone) and
  `x12_ercot_summer_peak.png` (actual vs normalized P10/P50/P90 vs Q7)

**Numbers refreshed on 2026-09-26 after the X16 #1 fix** (the Winter Storm Uri days are now normalized from the
fitted day, not the shed load). Only 2021's normalized energy moves (ERCOT adjustment +1.0% → +1.6%), so the 2021–2022
YoY, the 2010–21 reference and the z-scores change: ERCOT 2010–21 mean 2.1 → 2.2%/yr; 2022 +5.0% (z 6.0) → +4.4%
(z 3.9); 2022–2025 now at 3.9–5.7 SD (was 5–7). The fits, the CAGRs and the summer peaks are unchanged, and the ~2022
step still stands.

## Method

**Targets, per zone and operating day (America/Chicago):** the daily mean MW (`dmean` = energy ÷ hours, so the
23- and 25-hour DST days compare; energy is rebuilt as `dmean × hours`) and the daily peak MW (`dmax`). The eight
weather zones and `ERCOT` are each modeled separately. ERCOT's weather is the zones' daily weather weighted by
their all-month energy share over 2003–2022.

**Daily model (OLS, one per zone, target and window):**

`y(d) = level(d) + season(d) + day type(d) + f(weather(d)) + e(d)`

| Term | Form |
|---|---|
| level | continuous piecewise-linear in time, knots every Jan 1 and Jul 1 (chosen on validation, below) |
| season | 2 Fourier harmonics of the day of year (daylight, school and farm calendars) |
| day type | day of week (Sunday baseline); NERC holidays (Sunday → Monday rule); bridge days (Friday after Thanksgiving, Dec 24, 26, 31); year-end week (Dec 27–30) |
| f(weather) | linear spline on the daily mean temperature, knots 5, 10, 15, 20, 24, 27, 30 °C (heating left, cooling right); yesterday's mean temperature above 22 °C and below 15 °C (building inertia); daily mean dew point above 16 °C |
| excluded | Winter Storm Uri, 2021-02-14 → 02-20 (load was shed, so load ≠ demand; dates from memory, not verified) |
| trimming | one refit without days whose residual exceeds 4 robust SDs (e.g. Harvey, 2017-08-27 → 29): 0.1–0.8% of zone-days |

**Rolling windows:** each year is normalized with the model fit on the three years around it (2003 uses
2003–2005, 2026 uses 2024–2026). The weather response can therefore drift over 24 years, which §3 measures.

**Normal weather = 2003–2022**, the first 20 years of the ERA5 series in the DB (it starts in 2003, so 1991–2020 is
not available). For each day, the weather of the same calendar day in each of the 20 normal years is run through
that day's model, keeping the day's own residual, day type and level:

`normalized(d) = actual(d) − f(weather actual) + mean over 20 years of f(weather of that calendar day)`

This is the expectation under the climate distribution, not the load at an averaged temperature (which would
understate the convex heat response). **Normalized summer peak:** for each of the 20 weather years, swap that
year's June–September weather into the year's days and take the season's maximum. P50 is the median of the 20,
and P10 and P90 bound it.

**Variant choice, on a validation year before looking at 2024–2025** (train 2020–2022, test 2023, mean MAPE of the
nine series, level re-anchored on the test year):

| Variant | Daily energy | Daily peak |
|---|---:|---:|
| temperature spline only | 3.56 | 4.65 |
| + lag | 3.36 | 4.45 |
| + lag + dew point | 3.33 | 4.47 |
| + lag + dew point + daily max > 35 °C | 3.34 | 4.46 |
| **+ lag + dew point, level knots every 6 months** | **3.28** | **4.23** |
| + lag + dew point, 3 Fourier harmonics | 3.32 | 4.44 |
| no weather at all (same design) | 8.29 | 9.88 |

- Chosen: **lag + dew point + 6-month level knots** for both targets.
- Dew point and the daily maximum add little. Daily mean temperature and yesterday's temperature do most of the
  work.
- The other variants (annual knots, an extra 32 °C knot, no trimming) move the ERCOT normalized peak by ≤ 0.5 GW
  (checked 2021–2026) and the annual energy adjustment by ≤ 0.15 pp.

## 1. Fit and validation

In-sample fit (rolling windows): daily-energy R² 0.92–0.99 by zone and window (ERCOT median 0.988), daily-peak R²
0.91–0.97 (ERCOT 0.972).

### Hold-out: train 2021–2023, predict every day of 2024–2025 (731 days) with actual weather

- **flat:** the level is frozen at its end-2023 value, a true forecast.
- **re-leveled:** each test year's mean error is removed. The level is taken as known, so this tests the weather
  response and the day types, which is what normalization relies on.
- **no weather:** the same re-leveled test, from a model without the weather terms.

| Zone | Energy MAPE flat | re-leveled | no weather | bias flat | Peak MAPE flat | re-leveled | no weather |
|---|---:|---:|---:|---:|---:|---:|---:|
| COAST | 5.43 | 4.46 | 9.94 | +4.0% | 6.80 | 6.13 | 15.18 |
| EAST | 3.77 | 3.14 | 9.15 | −2.4% | 5.47 | 5.24 | 11.87 |
| FWEST | 10.84 | 3.25 | 3.48 | −11.1% | 10.71 | 3.53 | 4.03 |
| NCENT | 3.33 | 2.86 | 9.31 | −1.5% | 5.16 | 4.94 | 11.85 |
| NORTH | 11.92 | 7.85 | 9.88 | −11.9% | 13.00 | 7.55 | 10.41 |
| SCENT | 3.60 | 2.40 | 9.47 | −2.8% | 4.98 | 4.23 | 12.37 |
| SOUTH | 5.37 | 3.47 | 9.09 | −4.5% | 5.78 | 4.87 | 11.66 |
| WEST | 12.74 | 6.63 | 9.41 | +11.9% | 14.42 | 7.81 | 11.30 |
| **ERCOT** | **4.01** | **2.94** | 7.97 | −1.9% | **4.62** | **3.98** | 10.71 |

- **Weather is most of the daily signal.** With the level known, the model cuts ERCOT's daily MAPE from 8.0% to
  2.9% for energy and from 10.7% to 4.0% for the peak. That is the opposite of Q7, where weather added nothing to
  the *annual* peak: at annual grain, weather varies little from year to year, while at daily grain it drives
  most of the variance.
- **The flat forecast misses where the level moves:** FWEST and NORTH were under-predicted by ~11–12% and WEST
  over-predicted by 12%. Those are the large-load zones, so this model is a normalizer, not a forecaster. With
  annual knots the flat forecast did better on validation (4.77% vs. 5.15%), so a forecast use should switch knots.
- **Worst days are outages, not weather:** COAST 2024-07-08 → 12 (Hurricane Beryl; actual 9.3 GW against a
  predicted 16.0 GW on the 8th) and 2024-05-17 (the Houston derecho, date consistent; not verified from the DB).
  NORTH's worst days are in 2025, a zone whose flexible load moves on its own.
- **FWEST:** weather barely helps (3.25% vs. 3.48%). Its load no longer follows temperature (§3).

### Residual autocorrelation (in-sample, rolling fits)

| Target | Lag-1 ACF by zone (ERCOT) | Lag-7 | Lag-365 | Ljung–Box (14) | Hold-out lag-1 |
|---|---|---|---|---|---|
| daily energy | 0.43–0.74 (0.50) | 0.10–0.31 | −0.10 to +0.05 | p ≈ 0 in every zone | 0.58–0.92 (0.85) |
| daily peak | 0.31–0.54 (0.37) | 0.06–0.25 | −0.06 to +0.06 | p ≈ 0 | 0.48–0.84 (0.69) |

- Residuals persist for a few days (multi-day heat and cold, outages, level drift) but not across years.
- Durbin–Watson is 0.5–1.4. The coefficient SEs in §3 are Newey–West (7 lags) for this reason.
- For forecasting, an AR(1) error term would be the next step (not done).
- The hold-out's higher lag-1 is level drift inside the test year, which the per-year re-leveling cannot remove.

## 2. The normalized series and its growth

### Weather adjustment (normalized ÷ actual − 1, annual energy, %)

| Year | COAST | EAST | FWEST | NCENT | NORTH | SCENT | SOUTH | WEST | **ERCOT** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2011 | −3.7 | −3.9 | −2.6 | −5.0 | −4.6 | −4.5 | −2.5 | −4.1 | **−4.3** |
| 2018 | −0.5 | −1.8 | −0.7 | −2.3 | −1.7 | −1.7 | −1.0 | −1.3 | −1.4 |
| 2019 | −1.2 | −1.2 | −0.3 | −1.2 | −0.8 | −2.2 | −1.6 | −1.5 | −1.1 |
| 2020 | +0.3 | +2.5 | −0.4 | +2.3 | +0.3 | +0.3 | +0.4 | +0.1 | +1.0 |
| 2021 | +1.4 | +0.5 | +1.2 | +2.3 | +1.6 | +1.6 | +0.4 | +1.5 | +1.6 |
| 2022 | −2.2 | −3.1 | −0.8 | −3.9 | −3.3 | −4.7 | −2.3 | −3.4 | **−3.2** |
| 2023 | −2.6 | −0.7 | −0.4 | −2.1 | −0.7 | −3.2 | −2.5 | −1.2 | −2.2 |
| 2024 | −0.8 | +0.7 | −0.1 | −0.1 | −0.4 | −1.5 | −1.5 | −0.6 | −0.7 |
| 2025 | −1.2 | −1.1 | −0.1 | −0.8 | −0.1 | −2.4 | −2.6 | −0.2 | −1.1 |

- Over the normal period (2003–2022) the ERCOT adjustment averages +0.06%, as it should by construction.
- The adjustment is largest in 2011 (−4.3%) and 2022 (−3.2%), both extreme summers.

### Normalized annual energy (TWh) and growth

| Zone | Raw 2019 | Raw 2025 | Norm. 2019 | Norm. 2025 | **CAGR 2019→25 raw** | **normalized** | Norm. CAGR 2010→19 | Norm. CAGR 2022→25 (raw) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| COAST | 108.2 | 129.2 | 107.0 | 127.6 | 2.99% | 2.98% | 1.99% | 3.42% (3.11%) |
| EAST | 13.0 | 15.9 | 12.9 | 15.8 | 3.43% | 3.44% | 0.95% | 1.77% (1.08%) |
| FWEST | 30.1 | 65.5 | 30.0 | 65.4 | 13.84% | 13.87% | 11.69% | 16.76% (16.51%) |
| NCENT | 121.1 | 134.1 | 119.6 | 133.0 | 1.72% | 1.79% | 1.19% | 2.45% (1.37%) |
| NORTH | 7.5 | 14.8 | 7.4 | 14.8 | 12.04% | 12.17% | −0.15% | 15.82% (14.58%) |
| SCENT | 61.1 | 77.8 | 59.8 | 75.9 | 4.11% | 4.07% | 1.46% | 3.99% (3.17%) |
| SOUTH | 31.6 | 38.9 | 31.1 | 37.8 | 3.50% | 3.32% | 2.34% | 3.94% (4.06%) |
| WEST | 11.3 | 12.1 | 11.2 | 12.1 | 1.16% | 1.38% | 2.36% | 3.38% (2.24%) |
| **ERCOT** | 383.8 | 488.2 | 379.5 | 482.8 | **4.09%** | **4.10%** | **2.12%** | **5.07% (4.33%)** |

- **2019 → 2025 is weather-neutral.** Both years ran ~1.1% above normal, so raw and normalized CAGR agree within
  0.2 pp in every zone. The answer depends on the base year: **from 2022 (a hot year), raw growth understates the
  underlying rate** (ERCOT 4.33% raw vs. 5.07% normalized; NCENT 1.37% vs. 2.45%).
- **Normalized ERCOT energy doubled its pace:** 2.12%/yr over 2010–2019, then +4.4 / +4.4 / +5.4 / +5.3% in
  2022–2025. Against the 2010–2021 distribution of normalized YoY (mean 2.16%, SD 0.57 pp), those are
  **3.9–5.7 SD** each year.
- **Weather noise removed:** the SD of annual YoY change over 2004–2021 falls from 1.98 to 1.04 pp for ERCOT,
  2.81 → 0.56 NCENT, 2.62 → 1.51 SOUTH, 2.92 → 2.16 SCENT and 3.52 → 2.70 WEST. COAST (1.66 → 1.73), FWEST and
  NORTH are unchanged: hurricanes and structural growth dominate their variance, not weather.
- **ERCOT modeled directly vs. the sum of the zones' normalized energy:** within ±0.17% every year.
- **Normal-period sensitivity:** with 2006–2025 as normal, normalized energy is +0.04% to +0.68% higher (ERCOT
  +0.45% in 2019, +0.39% in 2025). The recent climate is warmer; growth rates barely move.
- **2026 so far** (Jan 1 – Sep 19 vs. the same days of 2025):

  | Zone | ERCOT | NCENT | SOUTH | SCENT | WEST | FWEST | NORTH |
  |---|---:|---:|---:|---:|---:|---:|---:|
  | Normalized | **+3.8%** | +4.8% | +7.6% | +5.0% | **+25.3%** | +2.9% | −0.7% |
  | Raw | +5.1% | +7.7% | | | | | |

  About a quarter of 2026's raw ERCOT growth is weather. The normalized pace slowed from ~5.3% because FWEST
  stalled and NORTH fell (§4).

### Summer peak under normal weather (ERCOT, MW; June–September daily max; 2026 through Sep 19)

| Year | Actual | Normal P10 | **P50** | P90 | P50 vs. actual | Q7 at actual weather | Q7 trend + median weather |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2011 | 68,318 | 63,531 | 64,787 | 66,308 | −5.2% | 68,957 | 66,415 |
| 2019 | 74,666 | 71,551 | 74,828 | 77,365 | +0.2% | 75,537 | 75,268 |
| 2020 | 74,328 | 72,643 | 75,598 | 78,322 | +1.7% | 76,622 | 76,374 |
| 2021 | 73,651 | 74,954 | 77,551 | 79,436 | +5.3% | 75,584 | 77,481 |
| 2022 | 80,038 | 76,222 | 79,187 | 80,690 | −1.1% | 80,052 | 78,588 |
| 2023 | 85,464 | 78,931 | 82,344 | 84,781 | −3.7% | 82,022 | 79,694 |
| 2024 | 85,199 | 82,750 | 84,847 | 86,892 | −0.4% | 80,756 | 80,801 |
| 2025 | 83,679 | 85,421 | **88,003** | 90,272 | **+5.2%** | 80,490 | 81,907 |
| 2026 | 91,134 | 88,372 | **91,110** | 93,641 | −0.0% | 84,169 | 83,014 |

- **The normalized peak rose every year from 2012 to 2026** (64.8 → 91.1 GW). The raw peak fell in 7 of those 15
  years (2012, 2014, 2017, 2020, 2021, 2024, 2025).
- **2025's "dip" (83.7 GW) was a mild summer.** Under normal weather it would have been **88.0 GW**
  [85.4–90.3]. 2023's 85.5 GW record was weather-assisted (82.3 GW normal).
- **Pace:** +1.3 GW/yr in 2012–2019 (65.6 → 74.8), then **+2.7 GW/yr in 2021–2026** (77.6 → 91.1).
- **2026 was weather-neutral at the peak** (P50 = actual). Its 91.1 GW is the underlying level, not a heat
  anomaly.
- **Against Q7:** Q7's trend + median weather gives 83.0 GW for 2026, and the normalized peak sits **8.1 GW
  above** it. That is Q7's 7 GW gap, with 2026's slightly-hotter-than-median weather taken out. Against X1's
  pre-break model (80.0 GW) the gap is 11.1 GW.
- **Bias check:** over 2003–2022 the P50 averages **+0.85% above the actual peak** (SD 2.4 pp; zones +0.5% to
  +2.2%). It should be ~0. The cause is that ERCOT's daily peak on each summer's 5 hottest days sits **315 MW
  below** the weather response (−43 MW on days ranked 6–20). The swap spreads that shortfall away from the hottest
  weather, so the swapped maximum comes out higher. Likely causes are 4CP curtailment by large customers,
  conservation appeals and AC saturation (none verified). The P50 is reported uncorrected.

Zone summer peaks, normalized P50 (MW):

| Zone | 2019 | 2022 | 2023 | 2024 | 2025 | 2026 | Raw CAGR 2019→25 | Normalized |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| COAST | 21,269 | 22,120 | 23,687 | 23,580 | 24,970 | 24,722 | 1.3% | 2.7% |
| EAST | 2,527 | 3,021 | 3,112 | 3,148 | 3,194 | 3,246 | 2.7% | 4.0% |
| FWEST | 4,204 | 5,475 | 6,556 | 7,558 | 8,908 | 8,710 | 12.6% | 13.3% |
| NCENT | 25,925 | 26,115 | 26,934 | 27,573 | 28,019 | 29,147 | 1.3% | 1.3% |
| NORTH | 1,448 | 1,908 | 2,048 | 2,317 | 2,734 | 2,585 | 10.8% | 11.2% |
| SCENT | 12,793 | 14,059 | 14,780 | 15,113 | 15,486 | 16,046 | 2.3% | 3.2% |
| SOUTH | 6,012 | 6,305 | 6,481 | 6,711 | 6,894 | 7,199 | 2.1% | 2.3% |
| WEST | 2,082 | 2,017 | 2,116 | 2,159 | 2,265 | 2,850 | −0.0% | 1.4% |
| **ERCOT** | 74,828 | 79,187 | 82,344 | 84,847 | 88,003 | 91,110 | **1.9%** | **2.7%** |

**For peaks, unlike energy, 2019 → 2025 is not weather-neutral.** Summer 2025 was mild at peak (COAST +8.5%,
SCENT +5.6%, ERCOT +5.2%). Raw peak growth understates the underlying rate by 0.8 pp/yr for ERCOT and by
1.4 pp/yr for COAST.

## 3. Has the temperature response changed?

The same model is fit on non-overlapping three-year blocks. The slope is the MW change of daily mean load per
°C when today's and yesterday's temperature rise together. Cooling is read at the zone's median July–August day
(ERCOT 29.3 °C), heating at its December–February P10 (ERCOT 4.5 °C). SEs are Newey–West.

| Block | ERCOT cooling (GW/°C ± SE) | Cooling, % of summer mean load | ERCOT heating (GW/°C ± SE) | Daily-peak cooling (GW/°C) | Summer mean load (GW) |
|---|---:|---:|---:|---:|---:|
| 2003–05 | 1.69 ± 0.05 | 4.12 | −1.11 ± 0.08 | 2.33 | 41.0 |
| 2006–08 | 1.71 ± 0.06 | 3.94 | −1.36 ± 0.12 | 2.21 | 43.3 |
| 2009–11 | 1.86 ± 0.08 | 3.96 | −1.42 ± 0.04 | 2.51 | 47.0 |
| 2012–14 | 1.73 ± 0.05 | 3.70 | −1.57 ± 0.05 | 2.23 | 46.7 |
| 2015–17 | 2.12 ± 0.07 | 4.23 | −1.66 ± 0.08 | 2.81 | 50.0 |
| 2018–20 | 2.07 ± 0.06 | 3.83 | −1.62 ± 0.05 | 2.73 | 54.0 |
| 2021–23 | 2.13 ± 0.07 | 3.56 | −1.97 ± 0.13 | 2.63 | 59.9 |
| 2024–26 | **2.44 ± 0.08** | **3.67** | −1.82 ± 0.07 | 2.90 | 66.5 |

Cooling slope as % of summer mean load, by zone:

| Zone | 2003–05 | 2009–11 | 2015–17 | 2018–20 | 2021–23 | 2024–26 |
|---|---:|---:|---:|---:|---:|---:|
| COAST | 3.63 | 4.00 | 4.78 | 4.03 | 3.62 | 4.50 |
| EAST | 3.23 | 2.94 | 4.06 | 3.57 | 3.27 | 3.59 |
| FWEST | 2.36 | 2.25 | 1.41 | 1.20 | 0.95 | **0.27** |
| NCENT | 3.82 | 4.13 | 4.20 | 4.00 | 3.97 | 4.19 |
| NORTH | 3.32 | 3.29 | 3.30 | 3.20 | 2.47 | 2.63 |
| SCENT | 4.57 | 3.66 | 4.21 | 4.00 | 3.89 | 4.18 |
| SOUTH | 4.54 | 4.74 | 5.02 | 5.01 | 4.66 | 4.78 |
| WEST | 3.01 | 2.63 | 2.65 | 2.58 | 2.72 | 2.32 |

- **Yes, in MW; roughly no, per unit of load.**
  - ERCOT's cooling slope rose 44% (1.69 → 2.44 GW/°C) while summer load rose 62%. As a share of load it drifted
    down, 4.1% → 3.7% per °C.
  - If the 2003–05 share still held, 2024–26 would respond 2.74 GW/°C, not 2.44. The ~11% dilution is consistent
    with roughly 7 GW of weather-insensitive load, the same order as X1's flat excess (7–9 GW in 2023–2025). This
    is an inference from two ratios, not a measurement.
- **The weather-driven zones kept their share:** COAST, NCENT, SCENT and SOUTH stay at 3.6–5.0% per °C with no
  trend. More AC arrived with more homes, in proportion.
- **FWEST lost its weather response.**
  - Cooling fell from 2.4% to 0.27% per °C (20 ± 12 MW/°C on 7.5 GW).
  - Its heating slope turned **positive** in 2024–26 (+45 ± 11 MW/°C): load *falls* on cold days. That fits
    flexible loads curtailing in winter price spikes, or oil-field freeze-offs (not verified).
- **NORTH and WEST** shares fell modestly (3.3 → 2.6%, 3.0 → 2.3%), as a zone with added flat load would show.
- **Heating grew faster than cooling:** ERCOT −1.11 → −1.8 to −2.0 GW/°C (+65–78%) against +44% for cooling.
  Winter electrification (heat pumps, resistance heat in new homes) is the plausible reason (not verified). It
  matters for the winter peak, which this doc does not model.

## 4. Does the normalized series show the ~2022 step (X1)?

Normalized annual energy YoY (%), and its z-score against 2010–2021:

| Year | COAST | EAST | FWEST | NCENT | NORTH | SCENT | SOUTH | WEST | ERCOT |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2021 | +4.4 | +2.8 | +10.1 | +0.8 | +3.6 | +4.7 | +2.5 | −0.4 | +3.4 |
| 2022 | +0.1 | +3.2 | +14.7 (z 0.7) | +2.9 (z 2.7) | **+30.2 (z 12.2)** | **+6.7 (z 4.7)** | +5.8 | +2.9 | **+4.4 (z 3.9)** |
| 2023 | +2.1 | +1.9 | +21.4 (z 1.9) | +1.1 | +9.7 (z 3.9) | +4.7 | +2.6 | +3.6 | +4.4 (z 4.0) |
| 2024 | +3.7 | +0.5 | +15.7 | +3.5 | +24.3 (z 9.8) | +3.8 | +4.5 | +3.7 | +5.4 (z 5.7) |
| 2025 | +4.4 | +2.9 | +13.3 | +2.8 | +14.0 (z 5.6) | +3.5 | +4.7 | +2.8 | +5.3 (z 5.6) |
| 2010–21 mean (SD) | 2.2 (1.3) | 1.8 (2.8) | 10.8 (5.5) | 1.1 (0.7) | 0.0 (2.5) | 1.6 (1.1) | 2.3 (1.6) | 1.6 (2.8) | 2.2 (0.6) |

- **NORTH: yes, sharply.**
  - Normalized monthly YoY (average MW) goes from +3–4% (Oct–Nov 2021) to **+24% (Dec 2021)**, then +28% to +39%
    every month Jan–Nov 2022. That is X1's Dec 2021 / Jan 2022 timing, now with weather removed.
  - A second leg follows in 2024 (+21 to +31% monthly).
  - In 2026 NORTH turns negative: −1% (May), −8 / −11 / −10% (Jun–Aug), as X1 found.
- **FWEST: no 2022 step in energy.** FWEST has grown 10–21% a year since 2014 (normalized: 2018 +18%, 2019 +21%),
  so 2022 is ordinary for it (z 0.7).
  - X1's FWEST "excess" is measured against a pre-2020 *linear* peak trend. The energy series shows a sustained
    exponential ramp from ~2014, not a 2022 step. That matches Q7's FWEST break in 2017.
  - The 2026 **stall** is clear: normalized YoY +13–17% through Sep 2025, then +2 to +8% (Jan–May 2026) and
    +3 / +1 / −1 / −2% (Jun–Sep 2026).
- **ERCOT and SCENT: yes.** ERCOT's normalized growth jumps from 2.2% to 4.4% in 2022 and stays at 4.4–5.4% (3.9–5.7 SD
  every year 2022–2025). SCENT 2022 is 4.7 SD.
- **WEST 2026 (X1): confirmed with weather removed.** Normalized YoY +8% (Oct 2025), +13% (Nov), +17% to +23%
  (Feb–Jun 2026), **+32% (Jul)**, **+39% (Aug)**, +48% (Sep 1–19).
- **EAST** has an unflagged step in 2020 (+9.6%, z 2.8), visible in `x12_normalized_by_zone.png`. Cause not
  verified.

## 5. What the API should carry

Grain: **zone × month** for the chart (9 series × 285 months ≈ 2,600 rows), plus **zone × year** for the summer peak
band. Daily is optional (78k rows in all, ~4 MB Parquet) for a zoom into the last 24 months. All fit in memory in
get-data (Polars).

`load_normalized_monthly` (one row per `weather_zone` × `month`):

| Column | Type | Meaning |
|---|---|---|
| `weather_zone` | text | COAST … WEST, `ERCOT` |
| `month` | date | first day of the month (America/Chicago operating days) |
| `days`, `complete` | int, bool | days with load and complete weather; `false` for the running month |
| `avg_mw`, `avg_norm_mw` | float | average load actual / at normal weather (the chart's two lines; comparable across partial months) |
| `energy_gwh`, `energy_norm_gwh` | float | the same as energy |
| `peak_mw`, `peak_norm_mw` | float | highest daily peak / highest weather-adjusted daily peak (label: "weather-adjusted", not an expected peak) |
| `t_mean_c` | float | the month's mean daily temperature (ERA5, zone) |
| `yoy_norm_pct` | float | 12-month change of `avg_norm_mw` |

`load_normalized_annual` (one row per `weather_zone` × `year`):

| Column | Type | Meaning |
|---|---|---|
| `energy_gwh`, `energy_norm_gwh`, `yoy_norm_pct` | float | annual energy and its normalized growth |
| `summer_peak_mw` | float | actual Jun–Sep daily max |
| `summer_peak_norm_p10`, `_p50`, `_p90` | float | peak under the 20 normal weather years |
| `complete_through` | date | last day included (2026: 2026-09-19) |

Metadata on both: `normal_period = "2003-2022"`, `weather_source = "ERA5 (Open-Meteo)"`, `model = "x12-v1"`,
`as_of`. Optional `load_weather_response` (zone × three-year block: cooling and heating MW/°C, SE, % of load) for a
small "weather sensitivity" panel. These are proposals: the contract lives in basecast-get-data
(`docs/data-contract.md`), not changed here.

## Headline for the video (candidate)

"Raw summer peaks fell in 7 of the last 15 years. Take the weather out and Texas's peak rose in every one of them,
from 65 to 91 GW, and twice as fast since 2021. 2025's 'dip' was a mild summer: at normal weather it would have been
88 GW."

Caveats to say or show:

- "Normal weather" means 2003–2022 ERA5 (our model), not ERCOT's weather normalization.
- The P50 runs ~0.9% high on average over 2003–2022 (§2 bias check).
- 2026 covers June 1 – September 19.

## Proposed decisions.md lines

- 2026-09-26 — The Forecast's weather-normalized load series is a daily model per zone (piecewise-linear level with 6-month knots, day-of-year Fourier, day type, temperature spline + lag + dew point), fit in rolling 3-year windows, normalized as actual − f(actual weather) + mean f(same calendar day of 2003–2022). — X12: re-leveled hold-out MAPE 2024–2025 ERCOT 2.9% (energy) / 4.0% (peak) vs 8.0% / 10.7% without weather; the variant was chosen on a 2023 validation year before the test.
- 2026-09-26 — Normal weather = ERA5 2003–2022 (20 years, the first available in the DB), stated on the chart. — The DB's ERA5 series starts in 2003, so 1991–2020 is unavailable; a 2006–2025 normal moves normalized energy by ≤ 0.7% and leaves growth rates unchanged.
- 2026-09-26 — The normalized summer peak is the P10/P50/P90 over the 20 normal weather years of the season maximum with each day's residual kept, not the maximum of normalized daily peaks. — X12: the max of adjusted days has no expectation meaning; the weather-year swap gives a band (ERCOT 2026: 88.4 / 91.1 / 93.6 GW).
- 2026-09-26 — Winter Storm Uri days (2021-02-14 → 02-20) are excluded from every load-weather fit. — Load was shed, so metered load was not demand (dates not verified against an ERCOT source).
- 2026-09-26 — The API carries the normalized series at zone × month (plus zone × year for the summer peak band); daily only for the last 24 months if the zoom needs it. — ~2,600 rows cover 2003–2026 for 9 series; the chart's story (growth without weather noise) reads at monthly grain.

## Review items for Pablo

1. **Normal period.** 2003–2022 is the only 20-year ERA5 window in the DB. Do we want a longer normal (backfill
   1991–2002 from Open-Meteo, a pipeline change) or a recent one (2006–2025, +0.4% level)?
2. **Uri window and NERC holiday rule** are from memory (Feb 14–20, 2021; Sunday → Monday). Not verified.
3. **Peak bias (+0.85%).** ERCOT's daily peak falls 315 MW short of the weather response on each summer's 5
   hottest days. Show the P50 as is, or calibrate by −0.9%? The cause (4CP curtailment, conservation appeals, AC
   saturation) is not verified; X3's 4CP work is the natural place to test it.
4. **Normalizer, not forecaster.** The flat-level hold-out misses the large-load zones by ~12%. The Forecast's
   projections should stay on X1/X7's layers. X12 feeds the history chart and the backtest target (item 6).
5. **FWEST's weather response vanished** (cooling 0.27%/°C, heating positive in 2024–26). Worth one sentence on
   screen: "FWEST load no longer follows the weather". Cause not verified.
6. **Backtests against normalized actuals?** ERCOT's long-term forecasts are presumably normal-weather forecasts
   (not verified here). If so, judging them against the raw 2025 peak (83.7 GW) instead of the normalized one
   (88.0 GW) flatters or penalizes them by ~4 GW. Q1 / X7 could add the normalized column.
7. **Monthly `peak_norm_mw`** is a weather-adjusted daily maximum, not an expected monthly peak. Keep it in the
   API only with that label, or drop it and show peaks only at the summer grain.
8. **COAST outages** (Harvey 2017, Beryl 2024, the May 2024 derecho) stay in the normalized series because
   residuals are kept. Flag those days on the chart, or fill them with the fitted value?

## Useful for the core features

- **Forecast screen, history chart:**
  - Two lines per zone (actual, normal weather) at monthly or trailing-12-month grain, as in
    `analysis/out/x12_normalized_by_zone.png`, and a summer-peak panel with the P10–P90 band
    (`x12_ercot_summer_peak.png`).
  - The normalized ERCOT peak (74.8 → 91.1 GW, 2019–2026) is the honest starting point for the three-layer
    forecast. Its 2026 value equals the actual, so X7's layers start from a weather-neutral year.
- **Backtest screen:** normalized actuals are the fair target for normal-weather forecasts (review item 6). 2025
  is the year where it matters most (+4.3 GW).
- **Explorer map:** a "weather sensitivity" layer (cooling % per °C by zone: FWEST 0.3% vs. SOUTH 4.8%) shows
  where load has decoupled from weather, which is where flat large loads landed.
- **Commercial intelligence triggers:** the zone's normalized monthly YoY is a cleaner version of X1's "rising
  daily minimum" trigger, because weather cannot fire it. WEST: +13% (Nov 2025) → +39% (Aug 2026). NORTH turned
  negative in May 2026.
- **Video:** the "fell in 7 of 15 years / rose in all 15" contrast (headline above) and the 2025 mild-summer
  point.
