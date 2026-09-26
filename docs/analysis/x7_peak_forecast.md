# X7 — Peak forecast prototype (P10/P50/P90) and a pseudo-out-of-sample backtest

Exploration after phase 0. Run on 2026-09-26 against production Postgres (`basecast_reader`, read-only):
`ercot_load_hourly_wz` through operating day 2026-09-25, `weather_hourly_wz` through 2026-09-20 23:00 UTC,
`large_load_chart_values` and `large_load_headlines` (Gemini-read, `verified=false`), `ercot_monthly_peaks` and
`official_forecasts`.

- Script: `analysis/x7_peak_forecast.py` (`uv run --group analysis python analysis/x7_peak_forecast.py`, ~15 s)
- Model code: `basecast_pipelines/models/peak_forecast.py`. It holds pure functions on top of Q7's `weather_load.py`,
  Q5's `large_load.py`, X1's `peak_excess.py` and Q1's `backtest.py`, all four unchanged.
- Tests: `tests/models/test_peak_forecast.py` (synthetic, no DB; 11 tests, pass with `tests/test_partner_lock.py`)
- Outputs (gitignored), all in `analysis/out/`:
  - Figures: `x7_forecast_vs_official.png`, `x7_backtest.png`
  - Forecast tables: `x7_forecast_ercot.csv`, `x7_forecast_zones.csv`, `x7_official_2027_2031.csv`
  - Backtest tables: `x7_backtest.csv`, `x7_backtest_paired.csv`, `x7_backtest_official.csv`, `x7_backtest_inputs.csv`

**Every large-load number here comes from deck values Gemini read off chart images. They stay "not verified"
until the Q5 spot-check is done.** The 2026 actual (91,134 MW) is preliminary: July–August are not final-settled.

**Numbers refreshed on 2026-09-26 after the X16 #3 fix** (month-end convention: December stocks at Dec 31, the summer
point at the end of July, monthly readings at month end). Headline 2027 P50 93.0 → 92.8 GW and 2030 111.6 → 111.3 GW;
Jun 2026 deck 102.4 → 101.8 and 132.9 → 132.8 GW; the 2026-05-31 backtest cell 88.9 → 89.0 GW; P10–P90 coverage
61% → 56% (the 2025-12-31 cell leaves the band). The backtest MAPE of 3.3% holds.

## 1. Method

`peak(Y) = organic(Y) + LL(Y) + U`, for the ERCOT system (coincident, hourly) summer peak.

| Layer | Definition | Uncertainty (draws) |
|---|---|---|
| **organic** | Q7's `peak ~ 1 + (year − 2014) + t_mean_3d`, fit on summers 2003–2019 (the pre-break fit X1 proposes), at the median of the 2003–2019 weather feature. Coefficients: trend **856 MW/yr** (SE 44), weather 1,154 MW/°C, in-sample residual SD 852 MW. | Coefficients ~ MVN(β, OLS cov), so the trend's extrapolation error grows with the year. Residual ~ N(0, **1,814 MW**), the RMSE of the one-year-ahead rolling-origin errors at median weather over 2011–2019 (below). |
| **LL** (large load at the peak) | `factor × A2E(end of July, Y)`. A2E is the approved-to-energize stock. Its path is a line through the deck's own stock at the deck date and projected December stocks `A2E(Dec Y) = base + r × max(promised(Y) − base, 0)`. `promised(Y)` is the deck's in-service bar for Y (cumulative, all statuses), `base` the deck's own A2E stock, and `r` Q5's **incremental** realization ratio. | `r` is resampled from every (deck, target year) ratio known at the as-of date with a horizon ≥ 6 months. One `r` per draw applies to every year. `factor` is a point estimate: the mean of the observed-simultaneous-peak ÷ A2E ratios at past peak months (X1). |
| **U** (unattributed flat load) | The excess of past summers over the organic model (actual weather) minus `factor × A2E` at that summer's peak month, summers 2022+. Held flat. | Resampled from the past summers' values (discrete). |

- **Independent draws.** The three layers are drawn independently (10,000 draws), which is a prototype
  simplification. In reality a hot summer or a boom year would move them together.
- **Normal weather.** Each P50 is a normal-weather forecast (median weather), like ERCOT's LTLF and CDR, so the
  comparison is like for like.
- **Why this organic band and not Q7's.** Q7 proposed the SD of the one-year-ahead rolling-origin errors of the
  full-sample model. Those errors contain the post-2020 step, which the LL and U layers already carry, so the step
  would be counted twice. X7 uses the same rolling-origin idea, restricted to target years 2011–2019 (pre-break) and
  at median weather:
  - MAPE 2.3%, bias −1.7%, RMSE 1,814 MW.
  - The in-sample SD with resampled weather gives a band that held **5 of 9** of those years inside P10–P90
    (half-width 2.5%). The RMSE-based band holds **8 of 9** (half-width 3.5%). That 8/9 is in-sample by
    construction, because the RMSE comes from those same years.
- **Zones.**
  - **The decks give no regional split of the in-service promise**, so LL is **statewide only**. They split only
    the approved stock (LZ_WEST vs. Other, X1).
  - Zones are shown as **coincident contributions** to the ERCOT peak. Each zone's MW at the ERCOT peak hour is fit
    on year plus ERCOT weather (≤ 2019), so the fits are additive.
  - LL + U go to zones by X1's coincident-excess shares (mean 2023–2026, negatives clipped to 0): FWEST 30.3%,
    SCENT 25.3%, COAST 17.0%, NORTH 12.9%, NCENT 8.5%, EAST 6.0%, SOUTH 0, WEST 0.
  - The zone P50s add up to the ERCOT P50 within 0.1%.
  - Zone organic bands use the in-sample SD plus weather draws, not a rolling RMSE, so they are narrower than they
    should be.
  - Zones' own (non-coincident) peaks are higher: system ÷ sum of zone peaks = 0.979 (Q7).

Inputs as of today (2026-09-26):

| Input | Value | Source |
|---|---|---|
| Factor (observed simultaneous ÷ A2E, peak month) | 0.525 (Aug 2023), 0.514 (Aug 2024), 0.498 (Aug 2025); **mean 0.512** | decks; 2026 has no observation yet (latest deck month Mar 2026) |
| U by summer (MW) | 2022: 2,540 · 2023: 5,015 · 2024: 5,298 · 2025: 3,211 · 2026: 6,601; **median 5,015** | excess over organic − 0.512 × A2E |
| Incremental ratio `r` | 38 (deck, year) pairs, targets 2023–2025: **p10 0.131, p50 0.187, p90 0.289** | Q5 definition, realized = stock in the first deck after Dec Y |
| Latest deck | 2026-06-19 (post Batch Zero): A2E 8,900 MW; promised by 2026 / 2027 / 2028 / 2030: 60.8 / 201.0 / 352.2 / 451.0 GW | `large_load_chart_values` |
| Last deck before Batch Zero | 2026-03-13: A2E 9,020 MW; promised 21.8 / 66.7 / 124.8 / 238.6 GW | same |

## 2. Forecast 2027–2031 (ERCOT, GW)

**Headline variant: the last deck before Batch Zero (Mar 2026).** The ratio `r` was estimated on decks whose promise
for the next years was 15–40 GW. The Apr–Jun 2026 decks doubled it (the Jun 2026 deck promises 201 GW by 2027),
mostly as "No Studies Submitted" plus a jump in planning studies approved (9 → 37 GW for 2027). Meanwhile the
approved stock did not move (8.8 GW in Jan 2026, 8.9 GW in Jun 2026). Applying a pre–Batch Zero ratio to the post–Batch
Zero queue is out of sample, so the Jun 2026 deck is shown as the high sensitivity.

| Year | Organic P50 | LL P50 [P10–P90] | U P50 | **Total P50 [P10–P90]** — Mar 2026 deck | Total P50 [P10–P90] — Jun 2026 deck | Approvals at 2023–25 pace (P50 only) |
|---|---:|---:|---:|---:|---:|---:|
| 2027 | 79.7 | 8.4 [7.2–11.0] | 5.0 | **92.8 [89.3–96.4]** | 101.8 [96.5–109.7] | 90.5 |
| 2028 | 80.6 | 13.4 [10.7–19.6] | 5.0 | **98.8 [94.2–104.2]** | 116.7 [107.8–133.8] | 92.5 |
| 2029 | 81.4 | 18.9 [14.6–28.8] | 5.0 | **105.0 [99.1–113.6]** | 127.4 [115.9–151.3] | 94.4 |
| 2030 | 82.3 | 24.4 [18.4–38.1] | 5.0 | **111.3 [104.0–123.3]** | 132.8 [120.0–159.8] | 96.3 |
| 2031 | 83.1 | 26.7 [20.0–42.1] | 5.0 | **114.5 [106.7–128.0]** | 135.5 [122.3–164.0] | 98.3 |

- **The large-load layer is the whole story, and it rests on one assumption.**
  - The LL P50 of the Mar 2026 deck implies an approved stock at the summer peak of **16.3 GW in 2027 and 47.6 GW in
    2030**. It is 8.9 GW today.
  - The Jun 2026 deck implies 34.0 and 89.5 GW.
  - Approvals grew **+2.1 GW/yr** from Dec 2022 to Dec 2025 (2.4 → 4.5 → 6.3 → 8.8 GW) and ~+0.1 GW in H1 2026.
  - At that pace (the last column, a sanity check, not a forecast) the stock reaches 11.2 GW in summer 2027 and
    19.8 GW in 2031.
  - "Queue × a constant ratio" assumes approvals scale with the queue. The 2023–2026 decks say they have not.
- **Organic** grows ~0.85 GW/yr. **U** is flat at ~5 GW (band 2.5–6.6 GW).
- 2031 is past the Mar 2026 deck's last bar (2030), so its promise is the whole queue (238.6 GW). The 2030–2031 LL
  uses a ratio estimated at 6–34-month horizons on a 57–69-month horizon. **Not validated past ~3 years.**

### Next to ERCOT's latest official forecasts (GW)

| Series | 2027 | 2028 | 2029 | 2030 | 2031 |
|---|---:|---:|---:|---:|---:|
| basecast P50, Mar 2026 deck | **92.8** | **98.8** | **105.0** | **111.3** | **114.5** |
| basecast P50, Jun 2026 deck | 101.8 | 116.7 | 127.4 | 132.8 | 135.5 |
| LTLF 2025, ERCOT-adjusted (2025-04-08) | 104.3 | 121.5 | 128.9 | 138.9 | 144.5 |
| LTLF 2025, TSP-provided (sensitivity) | 138.2 | 173.2 | 196.7 | 208.0 | 218.4 |
| CDR Dec 2025 (= May 2025 Revised) | 105.4 | 123.1 | 130.8 | 141.7 | — |
| 2026 preliminary LTLF (2026-04-15) | — | — | 278.0* | — | — |

\* From `config/manual_official_figures.yaml`, `peak_demand`, season not stated (the file also has 367.8 GW for
2032). **`official_forecasts` has no final LTLF 2026**, only the preliminary's three manual figures. The
comparison here is with LTLF 2025 and CDR Dec 2025.

- **Mar 2026 deck vs. ERCOT's adjusted LTLF 2025:** −11.5 GW (2027), −22.7 (2028), −23.9 (2029), −27.6 (2030),
  −30.0 (2031). ERCOT's P50 sits above our P90 from 2027 on.
- **Jun 2026 deck:** lands 1–9 GW under ERCOT-adjusted. The post–Batch Zero queue times the historical ratio
  roughly reproduces ERCOT's own adjusted view.
- **TSP-provided:** 45–104 GW above the headline.

### Zones (coincident contribution to the ERCOT peak, Mar 2026 deck, MW)

| Zone | Share of LL + U | 2027 organic P50 | 2027 P50 [P10–P90] | 2031 P50 [P10–P90] |
|---|---:|---:|---:|---:|
| COAST | 17.0% | 22,202 | 24,417 [23,565–25,294] | 28,581 [27,011–30,802] |
| NCENT | 8.5% | 27,189 | 28,299 [27,141–29,358] | 30,734 [29,372–32,250] |
| SCENT | 25.3% | 13,164 | 16,473 [15,693–17,268] | 21,584 [19,732–25,100] |
| FWEST | 30.3% | 4,521 | 8,494 [7,504–9,505] | 14,580 [12,285–18,742] |
| SOUTH | 0% | 6,930 | 6,930 [6,539–7,315] | 7,447 [7,019–7,875] |
| NORTH | 12.9% | 881 | 2,576 [2,099–3,064] | 4,726 [3,721–6,476] |
| EAST | 6.0% | 2,529 | 3,319 [3,127–3,513] | 4,456 [4,006–5,286] |
| WEST | 0% | 2,224 | 2,224 [2,083–2,367] | 2,339 [2,188–2,495] |

- WEST gets no LL + U because its coincident excess averaged −32 MW over 2023–2026. Its 2026 step (+0.7 GW, X1)
  is not in the share yet.
- NORTH's organic (fit 2003–2019, with the 2009 drop inside) is not meaningful. Q7 flags it.
- The zone split is a **fixed allocation** of a statewide layer, not a zone forecast of large loads.

## 3. Pseudo-out-of-sample backtest

**Setup.**
- **As-of dates:** the month end after each official publication since 2023 (LTLF 2023 + CDR May 2023, CDR Dec
  2023, CDR May 2024, LTLF 2024, CDR Dec 2024, LTLF 2025 + CDR May 2025, CDR Dec 2025, 2026 preliminary).
- **Data at each date:** everything is rebuilt from what was published by then.
  - Decks are filtered by `report_date`.
  - A realized December stock counts only once the January deck reporting it was out.
  - The monthly A2E series is the latest reading among decks published by then.
  - Factor and U use only past summers.
- **Targets:** every summer from horizon 1 (Q1's convention: the first summer that starts after the date) through
  2026.
- **Official comparison:** each product's latest vintage published on or before the same date (Q1's `base_series`).
- **Actuals:** hourly system peak (Q1).

### Paired table (error = forecast − actual, % of actual)

| As of | Target | h | Actual GW | basecast P50 [P10–P90] | basecast % | In band | LTLF % | CDR % | Prelim % |
|---|---|---:|---:|---|---:|:---:|---:|---:|---:|
| 2023-05-31 | 2023 | 1 | 85.5 | 80.4 [77.9–82.8] | −6.0 | no | −3.7 | — | — |
| 2023-05-31 | 2024 | 2 | 85.2 | 82.3 [79.8–84.7] | −3.5 | no | −1.1 | −0.3 | — |
| 2023-05-31 | 2025 | 3 | 83.7 | 83.4 [80.9–85.9] | −0.4 | yes | +2.4 | +3.6 | — |
| 2023-05-31 | 2026 | 4 | 91.1 | 84.3 [81.8–86.8] | −7.5 | no | −4.4 | −3.0 | — |
| 2023-12-31 | 2024 | 1 | 85.2 | 83.8 [80.7–86.9] | −1.7 | yes | −1.1 | −1.7 | — |
| 2023-12-31 | 2025 | 2 | 83.7 | 85.3 [82.1–88.3] | +1.9 | yes | +2.4 | +1.2 | — |
| 2023-12-31 | 2026 | 3 | 91.1 | 86.5 [83.3–89.6] | −5.1 | no | −4.4 | −6.0 | — |
| 2024-05-31 | 2024 | 1 | 85.2 | 83.6 [80.7–86.5] | −1.9 | yes | −1.1 | — | — |
| 2024-05-31 | 2025 | 2 | 83.7 | 85.6 [82.6–88.4] | +2.3 | yes | +2.4 | +1.3 | — |
| 2024-05-31 | 2026 | 3 | 91.1 | 86.9 [83.9–89.9] | −4.6 | no | −4.4 | −5.5 | — |
| 2024-07-31 | 2025 | 1 | 83.7 | 86.0 [83.0–89.0] | +2.7 | yes | **+8.1** | +1.3 | — |
| 2024-07-31 | 2026 | 2 | 91.1 | 87.5 [84.5–90.4] | −4.0 | no | **+16.8** | −5.5 | — |
| 2024-12-31 | 2025 | 1 | 83.7 | 86.8 [83.8–89.6] | +3.7 | no | +8.1 | **+9.8** | — |
| 2024-12-31 | 2026 | 2 | 91.1 | 88.6 [85.5–91.4] | −2.8 | yes | +16.8 | **+18.9** | — |
| 2025-05-31 | 2025 | 1 | 83.7 | 86.1 [83.2–89.0] | +2.9 | yes | +2.5 | — | — |
| 2025-05-31 | 2026 | 2 | 91.1 | 88.6 [85.5–91.5] | −2.8 | yes | +3.9 | +4.7 | — |
| 2025-12-31 | 2026 | 1 | 91.1 | 88.2 [85.2–91.1] | −3.2 | no | +3.9 | +4.7 | — |
| 2026-05-31 | 2026 | 1 | 91.1 | **89.0 [86.0–91.9]** | −2.3 | yes | +3.9 | +4.7 | **+22.9** |

The official vintages in the table are:

- **LTLF:** LTLF 2023 through 2024-05-31, LTLF 2024 at 2024-07-31 and 2024-12-31, then LTLF 2025.
- **CDR:** the May or Dec edition of that date. At 2024-07-31 it is CDR May 2024.

At 2026-05-31 the method used the May 21, 2026 deck (post Batch Zero). The Mar 2026 deck gives 88.1 GW (−3.3%; the
actual sits just above its P90).

### Scores (same cells)

| Comparison | Cells | basecast MAPE | Official MAPE (bias) |
|---|---:|---:|---:|
| vs LTLF, all | 18 | **3.3%** | 5.1% (+2.8%) |
| vs CDR, all | 15 | **3.2%** | 4.8% (+1.9%) |
| vs LTLF, as-of before LTLF 2024 (Jan 2023 vintage) | 10 | 3.5% | **2.7%** (−1.3%) |
| vs CDR, same period | 8 | 3.4% | **2.8%** (−1.3%) |
| vs LTLF, from LTLF 2024 on | 8 | **3.1%** | 8.0% (+8.0%) |
| vs CDR, from Jul 2024 on | 7 | **3.1%** | 7.1% (+5.5%) |
| vs 2026 preliminary | 1 | **2.3%** | 22.9% |

- **basecast overall:** 18 cells, MAPE 3.3%, bias −1.8%. **Coverage of P10–P90: 10/18 = 56%** (nominal 80%).
- **By horizon:**

  | h | Cells | MAPE | Bias | Coverage |
  |---:|---:|---:|---:|---:|
  | 1 | 8 | 3.1% | −0.7% | 63% |
  | 2 | 6 | 2.9% | −1.5% | 67% |
  | 3 | 3 | 3.4% | −3.4% | 33% |
  | 4 | 1 | 7.5% | −7.5% | — |

- **Ablations** (same 18 cells):
  - Organic only: MAPE 10.5% (bias −10.5%).
  - No hindsight on the break (organic fit on every summer known at the date) plus LL, without U: MAPE 3.9%
    (bias −3.3%, coverage 39%).
  - The LL and U layers are what bring the error from ~10% to ~3%.

**Honest reading.**
- **Before the large-load era,** ERCOT's normal-weather forecasts were better than this method (2.7–2.8% vs.
  3.4–3.5%).
- **From mid-2024,** once ERCOT's forecasts took TSP large-load submissions in, basecast was 2.3–2.6× more accurate
  and its sign stayed negative: it under-forecasts slightly, while ERCOT over-forecast by 5–8%.
- **The band is too narrow.** 56% coverage against 80% nominal, all misses on the low side except 2025 from
  Dec 2024. Widen it before the screen claims "80%".
- **The cells are not independent:** 4 distinct summers, 8 of the 18 cells are summer 2026, and the ratios
  overlap across dates.

**Which layer misses** (P50 vs. what the layer turned out to be; realized LL = 0.512 × A2E at the peak month, today's
reading):

| Target | Realized LL | LL forecast by as-of date (GW) | Realized U* | U forecast |
|---|---:|---|---:|---:|
| 2024 | 2.81 | 2.64 (May 23), 2.73 (Dec 23), 2.74 (May 24) | 5.3 | 2.4–2.5 |
| 2025 | 3.84 | 2.90, 3.35, 3.84, 4.23 (Jul 24), 4.44 (Dec 24), 3.87 (May 25) | 1.9 | 2.4–5.0 |
| 2026 | 4.57 | 3.04, 3.67, 4.32, 4.80, 5.32, 5.53, 5.37, 6.09 (May 26) | 7.7 | 2.4–5.0 |

\* Realized U = actual − organic P50 − realized LL, so it includes the summer's weather.

- **LL:** close at 1–2 summers while the queue was stable (2023–2024 dates).
- **LL over-forecast 2026 from every date after mid-2024** by +0.2 to +1.5 GW. Promises for 2026 grew from
  30 GW to 55 GW while approvals did not keep up. That is the same failure the 2027–2031 forecast is exposed to.
- **U is the noisiest layer:** 1.9–7.7 GW. It swings with the summer's weather, and the 2026 excess (7.7 GW) is
  above every past value.

### Leakage list (information from after the as-of date)

1. **The pre-break window (≤ 2019) and the post-break start (2022)** were chosen in Q7/X1 with data through 2026.
   No backtest date could have known the break. The no-hindsight ablation (3.9% vs. 3.3%) measures the cost.
2. **The model form and weather feature** (linear, `t_mean_3d`, ERCOT weights) are Q7's default, fixed before its
   hold-out but reviewed against 2023–2025. Small.
3. **The method's design** (a factor on the approved stock, a separate U layer, the incremental ratio) comes from Q5
   and X1, written in Sep 2026 after seeing 2023–2026. This is the largest and least measurable leak: the method's
   structure was shaped by the years it is scored on.
4. **As of 2023-05-31 and 2023-12-31, neither the factor nor any ratio was estimable.**
   - The first observed-peak series and the first realized December stock (Dec 2023) come from decks with
     `report_date` 2024-04-01.
   - Handling: both dates use those first estimates (factor 0.525, ratios 0.15–0.17), flagged in
     `x7_backtest_inputs.csv`, `leaks` column.
   - U for 2023 at 2023-12-31 also uses that factor.
5. **Publication dates.** Availability uses `report_date`. For the Jan 2024 deck it is 2024-04-01, later than its
   data date, which is conservative. Whether `report_date` equals the public posting date is **not verified**. If a
   deck was posted after its `report_date`, it enters the backtest early.
6. **Deck reading rules designed later.** Two rules were written in 2026 after seeing later decks:
   - Gemini's `as_of` plausibility window (Q5).
   - The misdated-axis drop, built around the May 2026 deck. It only affects as-of 2026-05-31.
7. **`pick_vintages` runs on all decks and is filtered afterwards.** The one-deck-per-month choice and the reprint
   filter only look backwards, so the effect is negligible.
8. **Historical load** comes from the ERCOT archive, which may include settlement revisions made after the
   as-of date. Small. Weather is ERA5, available within days: no leak.
9. **Scoring actuals:** 2026 is preliminary.
10. **No leak** in the organic band (2011–2019 errors), the ERCOT weather weights (2003–2019) or the official
    comparators (strictly published by the date).

## 4. What the app needs from the API

**Mart `peak_forecast`**, one row per `(run_id, as_of, region_type, region_id, target_year, variant, layer)`:

| Column | Type | Note |
|---|---|---|
| `run_id`, `as_of` | text, date | the as-of date is also what the backtest slider moves |
| `region_type`, `region_id` | text | `ercot`/`ERCOT`, or `weather_zone`/`FWEST`… (coincident contribution) |
| `target_year` | int | |
| `variant` | text | `deck_pre_batch_zero` (default), `deck_latest`, `approvals_pace` (sanity) |
| `layer` | text | `organic`, `large_load`, `unattributed`, `total` |
| `p10_mw`, `p50_mw`, `p90_mw` | float | |
| `deck_vintage`, `factor`, `ratio_p10`, `ratio_p50`, `ratio_p90` | date, float | the inputs, for the "how we got here" panel |
| `verified` | bool | false while the deck values are machine-read |

**Mart `peak_backtest`**, one row per `(as_of, target_year, source)`, where `source` is `basecast` or an official
vintage:

| Column | Type | Note |
|---|---|---|
| `as_of`, `target_year`, `horizon` | date, int, int | horizon = Q1's summers ahead |
| `source`, `product`, `vintage`, `vintage_date` | text, date | `basecast` or `LTLF`/`CDR`/`LTLF-prelim` |
| `p10_mw`, `p50_mw`, `p90_mw` | float | official rows only fill `p50_mw` |
| `actual_mw`, `actual_final` | float, bool | 2026 `actual_final = false` |
| `error_pct`, `in_band` | float, bool | |
| `leak_note` | text | e.g. "factor and ratio from decks published 2024-04-01" |

**Mart `large_load_realization`**, one row per `(deck_vintage, target_year)`: `promised_mw`, `base_a2e_mw`,
`realized_a2e_mw`, `known_from`, `ratio`. This is the "promised vs approved" chart and the source of the ratio
band.

Small tables, so they can live in memory (Polars), with the sliders as filters. Recomputing the simulation live is
not needed.

## Headline for the video (candidate)

"Rebuilt with only what was public on May 31, 2026, our model put this summer's peak at 89.0 GW (range 86.0–91.9).
The grid peaked at 91.1. ERCOT's preliminary forecast from six weeks earlier said 112."

Caveats to say or show:

- The 2026 actual is preliminary.
- The large-load inputs are machine-read decks (spot-check pending).
- The method's structure was designed in 2026.
- ERCOT's own April 2026 range (90.5–98 GW) also held the actual (Q1).
- Before 2024 ERCOT's normal forecasts were slightly more accurate than ours (2.7% vs. 3.5%). The gap opened when
  its forecasts started taking the large-load queue at face value (2024–2026: 8.0% vs. 3.1%).

## Proposed decisions.md lines

- 2026-09-26 — The peak forecast is organic (pre-break fit ≤ 2019, median weather) + large load (0.51 × projected approved-to-energize stock at end of July; December stocks = deck base + incremental realization ratio × new MW promised) + a flat unattributed layer resampled from 2022+ summers, combined by independent simulation (P10/P50/P90). — X7: on 18 pseudo-out-of-sample cells (as-of dates 2023-05 to 2026-05) it scored MAPE 3.3% vs 5.1% (LTLF) and 4.8% (CDR) on the same cells; organic alone scored 10.5%.
- 2026-09-26 — The organic band uses the RMSE of one-year-ahead rolling-origin errors at median weather over pre-break summers (2011–2019, 1,814 MW) plus coefficient uncertainty, not the in-sample SD or Q7's full-sample rolling errors. — The in-sample band held 5/9 pre-break years; Q7's full-sample errors contain the post-2020 step that the large-load layers already carry.
- 2026-09-26 — The default forecast uses the last large-load deck before Batch Zero (2026-03-13); the latest deck is a high sensitivity until a ratio can be estimated on post–Batch Zero decks. — The Apr–Jun 2026 decks doubled the MW promised for 2027 (66.7 → 201 GW) while the approved stock stayed at 8.8–8.9 GW; the ratio was estimated on 15–40 GW promises.
- 2026-09-26 — Large loads in the peak forecast are statewide; zones get a fixed share of LL + U (coincident excess shares 2023–2026). — The decks split the in-service promise by year only; the only regional split (LZ_WEST vs Other) is of the approved stock.
- 2026-09-26 — The backtest rebuilds every input as of the month end after each official publication (decks by report_date, realized stocks from the first deck after the year), compares with each product's latest vintage by the same date, and lists leaks per row. — A like-for-like date is what makes the /backtest number credible; as-of dates before 2024-04 have no estimable ratio or factor and are flagged.

## Review items for Pablo

1. **Pre- or post–Batch Zero deck as the default.** The two give 111.3 vs. 132.8 GW for 2030. Neither is validated
   at 4–5 years, and the backtest shows LL over-forecasting whenever promises inflate. The approvals-pace check
   (96.3 GW in 2030) is a third, lower view. Options: pick one, or show all three on screen as scenarios.
2. **The band is too narrow** (56% coverage). Options:
   - Widen U, e.g. a continuous distribution over its min–max instead of resampling 4–5 values.
   - Correlate the layers.
   - Add the rolling-RMSE residual at every horizon, not just one.
   Proposed: calibrate on these 18 cells and say so.
3. **The factor 0.51 as the contribution at the peak.** "Observed simultaneous" may be among the loads, not at the
   system peak (X1, not verified).
4. **U at 2026's 6.6 GW** is above every earlier value. Is it a trend (keep it flat, as now) or 2026 weather?
5. **The 2031 bar** is the whole queue (the Mar 2026 deck stops at 2030). Maybe stop the forecast at 2030.
6. **Spot-check** (Q5): everything in the LL layer depends on it.
7. **A final LTLF 2026** is not in `official_forecasts`. If ERCOT published one, add it before the video.

## Useful for the core features

- **Forecast screen:**
  - A stacked chart of the three layers per year (organic / large load / unattributed), with the P10–P90 band on
    the total.
  - ERCOT's LTLF 2025 (adjusted and TSP-provided) and CDR Dec 2025 as lines.
  - A toggle "deck: before / after Batch Zero / approvals pace" that moves only the LL layer.
  - The inputs panel: deck date, factor 0.512, ratio 0.13–0.29, approved stock 8.9 GW, all labeled "machine-read,
    not verified".
  - Zones as coincident contributions, flagged "statewide large load allocated by 2023–2026 shares".
- **Backtest screen:**
  - A date slider over the 8 as-of dates, showing our P50 and band vs. the official vintages of that date vs. the
    actual (`analysis/out/x7_backtest.png`).
  - The scores table split by era: before and after ERCOT started counting TSP large loads.
  - The leakage notes per row.
  - The ablation, organic only 10.5% → full method 3.3%, is the "why large loads matter" number.
- **API:** the three marts in §4. `x7_backtest_paired.csv` already has `peak_backtest`'s shape for the basecast
  rows.
- **Video:** `analysis/out/x7_forecast_vs_official.png` (fan chart vs. ERCOT's lines) and the 2026-05-31 cell of the
  backtest.
