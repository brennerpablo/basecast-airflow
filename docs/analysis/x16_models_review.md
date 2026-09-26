# X16 — correctness review of `basecast_pipelines/models/` (2026-09-26)

Scope: the model modules behind `docs/analysis/findings.md`. `peak_forecast.py` and `triggers.py` were reviewed at
`HEAD` (`git show HEAD:…`), because other agents are editing their working copies. Where a demonstration needed one of
them, the `HEAD` source was loaded as the module (the X7 re-runs below use `HEAD` `peak_forecast.py` and `HEAD`
`analysis/x7_peak_forecast.py`).

Method: read each module. Every suspect got a small check: synthetic input, a read-only query through `read_sql()`,
or a scratch re-run of the analysis script with its output sent to a scratch folder, never `analysis/out/`. This run
started from the unverified notes of an interrupted earlier run. Every item below was demonstrated again in this run;
leads that could not be demonstrated are listed separately and not counted.

No module, test, script, config or other doc was changed. The validation lock held: no `base_public_facts`, no
`models.partners`, no `config/base_public_facts.yaml`.

**Reproduced before hunting** (so the deltas below are against the published numbers):

| Doc | Published | Reproduced |
|---|---|---|
| X2 today | 38.7 GW | 38,689 MW |
| X2 backtest | −12.5% / +9.4% / +1.8% (actuals 15,940 / 18,428 / 28,460 MW) | same |
| X7 March deck | 93.0 [89.4–96.7] GW in 2027, 111.6 GW in 2030 | 92,998 [89,447–96,663] MW; 111,562 MW |
| X7 MAPE | 3.31% vs LTLF 5.06% / CDR 4.81% | same |
| X12 | ERCOT 2010–21 YoY 2.1 (SD 0.5); 2022 +5.0 (z 6.0) | 2.11 (0.47); +5.0 (z 6.0) |

## Confirmed findings

Severity:
- **medium**: a number printed in findings.md or an xN doc changes.
- **low**: the change is under a rounding step or small, or the bug only fires on inputs that do not occur today.

| # | Sev. | module:line | Bug | Demonstration | findings.md numbers affected, and size | Suggested fix |
|---|---|---|---|---|---|---|
| 1 | medium | `weather_normalized.py:383` (`normalize_year`); flag set at `:153`, fits skip it at `:310` | The Uri days (`excluded`, 2021-02-14 → 02-20) are left out of every fit but still normalized as `actual − w_actual + w_normal`. On those days the actual is shed load and `w_actual` is the heating response at −8 to −10 °C, so the shed MW stays in the normalized series as a large negative residual. `monthly()` / `annual()` then sum these days into 2021. | Real panel, ERCOT `dmean`, chosen spec `Spec(level_knots_per_year=2)`. Normalized MW: 02-13 (kept) 42,311, then **02-15: 15,940; 02-16: 14,283**; 02-18: 33,261; 02-21 (kept) 34,178. With `fitted − w_actual + w_normal` on the excluded days, 02-15 is 42,522 and 02-16 is 42,677. ERCOT's 2021 normalized energy is 2,228 GWh (0.56%) too low (COAST 703, NCENT 585, SCENT 363, FWEST 280, SOUTH 142 GWh). | **X12 row:** "grew 2.1%/yr in 2010–21" becomes **2.2%** (mean 2.11 → 2.16). "4.4–5.4%/yr in 2022–25" holds (2022: 5.0 → 4.4). **x12 doc §4, ERCOT:** 2021 YoY +2.8 → +3.4; 2022 +5.0 (z 6.0) → **+4.4 (z 3.9)**; 2023 z 5.0 → 4.0; 2024 z 7.0 → 5.7; 2025 z 6.9 → 5.6; reference SD 0.5 → 0.6. So "5–7 SD every year 2022–25" becomes **3.9–5.7 SD**. **2022 zone z:** SCENT 6.1 → 4.7, NORTH 12.9 → 12.2, NCENT 3.4 → about 2.7. The ~2022 step still stands. CAGRs outside 2021–22 and the summer-peak bands do not change (Feb is outside the summer). | In `normalize_year`, where `excluded`, set `normalized = fitted − w_actual + w_normal` (the model's day at normal weather). Or null those days and impute and flag them in `monthly()`. |
| 2 | low | `queue_adjusted.py:190–192` (`truncate_at`), with `survival.py:46–53, 65` (`EVENT_COLUMNS`, `load_events`) | `truncate_at` promises to drop a milestone "first reported after the month before `as_of`" through `ia_first_month` / `synchronization_first_month`. It only does so `if first in out.columns`. `sv.load_events()`, which X2 uses, does not select those columns, though `gis_project_events` has them. So the reporting-lag guard is silently off, and backdated IAs leak into the as-of training fits. | Same X2 backtest (`entry_ia_sm`, MW-weighted), with events plus the two `*_first_month` columns joined. IAs known at the three as-of dates drop 280 → 262, 352 → 335, 653 → 617. Statewide error changes from −12.53 / +9.42 / +1.76% to **−13.22 / +9.29 / +1.35%**. Today's queue is unchanged (38,689 / 70,394 MW), since every milestone is known by the latest report. | **X2 row / Start here:** "−12.5%, +9.4%, +1.8%" becomes −13.2%, +9.3%, +1.4%. "Lands within 13%" becomes **within about 13.2%**. The x2 doc's mean absolute error goes 7.9% → 8.0%. The 38.7 / 70.4 GW headline holds. | Add `ia_first_month`, `synchronization_first_month` (and `fis_first_month`, `cod_first_month`, `cancelled_month`) to `EVENT_COLUMNS`. Make `truncate_at` raise when a guard column is missing instead of skipping it. Not demonstrated here: extending the guard to FIS approvals and to COD/cancel events that were reported late. |
| 3 | low | `peak_forecast.py:38, 53–55` at `HEAD` (`SUMMER_POINT`, `dec_index`) | `month_index` puts month M at its first day. So `dec_index(Y) = Y*12+11` is **Dec 1**, not "end of December" as its docstring says. `SUMMER_POINT = 6.5` is **Jul 16**, not "end of July" as the comment says. The projected December stocks in `a2e_path` and the in-service-bar points in `a2e_points` sit a month early, while the vintage knot carries its exact day. | `dec_index(2026) = 24323 = month_index(date(2026,12,1))`; `month_index(date(2026,12,31)) = 24323.97`. X7 re-run from `HEAD` with `SUMMER_POINT = 7.0` and `dec_index = Y*12+12`: March deck 2027 P50 92,998 → **92,820** MW (P10–P90 89,447–96,663 → 89,292–96,420); 2030 111,562 → **111,328** [104,041–123,287]. June deck 2027 102,354 → 101,805 MW; 2030 132,923 → 132,800. Backtest MAPE: LTLF cells 3.31 → 3.27%, CDR cells 3.25 → 3.21%. | **X7 row:** "93.0 [89.4–96.7] GW in 2027" becomes **92.8 [89.3–96.4]**; "111.6 [104.2–123.7] in 2030" becomes **111.3 [104.0–123.3]**. MAPE 3.3% holds. Every change is under 0.6 GW. | Pick one convention. Month-end values: `dec_index = Y*12+12`, status months at `month_index + 1`, `SUMMER_POINT = 7.0`. Or, if mid-July is intended, fix the comments. |
| 4 | low | `four_cp.py:348–349` (`zone_coincidence`), called from `analysis/x3_four_cp.py` | `share_4cp` and `share_energy` are normalized over every `region_id` of the region type, including the `DC_*` tie rows. X3 drops `DC_` only after the shares are computed. | `ercot_monthly_peaks`: only `load_zone` 2011 has `DC_` rows (60 rows). LZ_WEST 2011 `share_4cp` is 0.063457 with them vs 0.063558 without. The largest change in any row is 0.063 pp (4CP share) and 0.10 pp (energy share). | X3's 2011–2014 load-zone era means move by < 0.03 pp. No printed value changes. | Drop `DC_` before `zone_coincidence`, or normalize over `LZ_*` only. |
| 5 | low (latent) | `triggers.py:589–592` at `HEAD` (`score_accounts`) | An account with every weighted signal null gets `num/den = 0/0 = NaN`. Polars sorts NaN above every number, so with `descending=True` it ranks **#1**. | Synthetic a, b, c with signals, and z with all signals null: z gets score NaN and **rank 1**, then c 0.75, a 0.50, b 0.25. | None today: all 107 accounts have county links, so `dc_sites` is never null. It would bite in A2 if an unlinked account reaches the ranking. | `pl.when(den > 0).then(num / den)`, and sort with `nulls_last=True`. |
| 6 | low (latent) | `triggers.py:580–581` at `HEAD` (`percentile_ranks`) | A signal with exactly one non-null value gives `(1−1)/(1−1)` = NaN, which then poisons `score` as in #5. | `x = [1.0, None]` → `pct_x = [NaN, null]`. | None today: every weighted signal has n ≥ 106. | Return 1.0 (or null) when n ≤ 1. |

## Checked and fine

**Re-run in this session**
- **Aalen–Johansen `fit_cif`** matches an independent brute-force product-limit recursion to 2.5e-12. The test used 200 random data sets with heavy ties, delayed entry and MW weights.
  - The risk set is (entry, time]: tied events share it, and censorings at t stay at risk.
  - `at` / `n_at_risk` use right-continuous steps.
- **Hour ending and time zone (`weather_load`, `peak_excess`, `weather_normalized`):**
  - 0 load rows where the local date of (ts_utc − 1 h) differs from `operating_date`.
  - Hours per day: 24 days with 23 h and 22 days with 25 h. The only gap is 2016-11-06, which has 24 h (one missing fall-back hour).
  - All daily and summer grouping is on the local operating date. Weather days are the America/Chicago date of the instant: 23 h / 25 h on DST days; the partial edge days are 2002-12-31 and 2026-09-20.
- **Zone sum vs the ERCOT total:**
  - June–September: within 0.03 MW.
  - All year: 48 hours off by more than 1 MW, all on 2021-05-17/18 (zones sum 680–2,360 MW below the total). This is in the source data, not in a module.
- **Key uniqueness:**
  - `county_weather_zone`: 254 rows, 254 counties.
  - `county_utility_overlap_puct` (ERCOT co-ops and munis): 528 rows, unique per (ccn_no, county) and per (territory, county). So the `zone_peak_growth` and apportion joins do not duplicate rows.
- **EIA-861 part C:** `eia861_sales` has no part C rows (only A and D), so the part-C rules in `accounts.eia_totals`, `eia861_short_form.long_form_totals` and `triggers.residential_price` never fire.

**Read and checked against the formulas (no issue found)**
- **`backtest.py`:**
  - The `summers_ahead` / `horizon_expr` June 1 edge agrees.
  - `summer_peaks` takes Jun–Sep and the max per `report_year`.
  - The `_pick_ltlf` preference order and the `_pick_cdr` revised-copy dedupe are right.
  - `sign_verdict` is right.
- **`weather_load.py`:**
  - The NWS heat index (Rothfusz, both adjustments, Steadman below 80 °F) is right.
  - The 3-day window is t−2..t with `min_samples = 3`.
  - `system_weather_daily` renormalizes weights over the non-null zones.
  - The `chow_scan` segment bounds and F degrees of freedom are right.
  - `fit_peak` centering and the df-corrected SD are right.
- **`peak_excess.py`:**
  - The `flat_block` algebra is right.
  - `step_scan` compares the 28 days before and the 28 from t on.
  - `coincident_zone_loads` takes the zones at the ERCOT peak hour.
- **`four_cp.py`:**
  - `in_window` gives (start, start+len]; 15:45–17:45 covers intervals ending 16:00–17:45.
  - `hour_ending` = ceil(end_min/60) (17:15 → HE18).
  - `with_reference` is the month's cum-max shifted one day.
  - `hourly_from_15min` keys on the UTC hour end.
  - `weather_forecast` fits only on earlier summers.
- **`queue_adjusted.py`:**
  - `conditional_cod` = (F(e+h) − F(e)) / S(e), with the clamp.
  - `composed_cod` is the IA-time increments × F_IA(e+h−u) plus the direct COD, over S_pre(e).
  - `pre_ia_frame` drops IAs at or before the delayed entry.
  - Stage clocks match the fitted curves' origins.
- **`triggers.py` at `HEAD`:**
  - The active window is (as_of − 365 d, as_of].
  - The permit-surge windows are 12 + 12 months.
  - `score_tier` gives 27 / 27 / 53 for n = 107.
  - `events_by_county` keeps the largest exposure per (account, trigger, event).
- **`accounts.py`:**
  - `apportion` keeps null when no county has data.
  - The `zone_peak_growth` area weights are right.
  - The CAGR exponents are right.
- **`eia861_short_form.py`:** thousand USD / MWh = USD/kWh; `combine_forms` keeps the long form when a utility is in both.
- **`large_load_geo.py`:**
  - `lz_west_north_fraction` is right.
  - `rake_to_group` keeps each side's total.
  - `split_large_load` puts the stock first, then the increment.
- **`large_load.py`:** `realization_ratios` counts only vintages before the realized date and before the year end.

**Not reviewed in depth in this run (timebox)**
- `acquisition_zones.py`: `channel_split` and `score_counties` were skimmed; nothing found.
- `gt_large_load.py`: skimmed; the co-op share arithmetic is a plain sum ÷ total.
- `diagnosis.py`, `data_centers.py`, `muni_places.py`: not reviewed.

## Leads not demonstrated (for A0, not counted above)

- **TPIT, 2026-07-13 snapshot:**
  - It is loaded from two source files: 4,254 rows for 2,122 project ids.
  - 444 projects are "first listed" on that date, 256 of them Conceptual, against 53–315 at earlier snapshots.
  - If that workbook carries a category earlier ones lacked, `transmission_events` marks old projects as new and inflates the `new_transmission` context trigger.
  - Context triggers do not change the next action.
- **`analysis/x3_four_cp.py` (a script, outside `models/`):** the "top-20 priced intervals" count uses `rank("min")`, so ties at the price cap can keep more than 20 in a month. The earlier run saw 34 in Aug 2011; not re-verified here.
- **`weather_normalized`:**
  - Other outage days (Beryl, COAST July 2024) keep their residual the same way as #1, but they are not in `EXCLUDED_DAYS`.
  - The normalized summer P50 sits about 0.85% above the actual peak over 2003–2022 (earlier run's figure).
- **`peak_forecast` zone bands:** the zone split uses in-sample sigma plus weather draws, while the ERCOT total uses the rolling RMSE, so the two bands are not on the same basis.
- **Sort ties:** `large_load.pick_vintages`, `a2e_by_month`, `peak_excess.energized_by_month` and `approved_by_load_zone` break ties without a tiebreaker. Today the ties give identical readings.
- **`survival.outcomes`:** censoring still-listed projects at `last_seen_month + 1 month` assumes a report covers its whole month. The data-date semantics were not checked.
- **`triggers.residential_price`:** it groups by (utility_id, data_year) without `early_release`, so a year loaded both final and early would blend. That does not happen today.
- **EIA-861 parts A and D:** they are summed for 17 utility-years that carry both. Whether D (service by power marketers) overlaps A for the same customers was not verified.
