# Marts proposal per app screen (X8, 2026-09-26)

A synthesis of phase 0 (Q1–Q7) and the explorations X1–X7 and X9 (X10 was still running and is not included). It
answers one question for each app page (`/explorer`, `/forecast`, `/backtest`, `/accounts`, `/accounts/[id]`,
`/data`): which small tables should `basecast-get-data` keep in memory, and which `basecast_pipelines/models/`
function builds each one. **This is a proposal only.** The contract belongs to `basecast-get-data`
(`docs/data-contract.md`). Nothing there was changed. Where this doc and the contract disagree, the contract stays in
force until the owner updates it, together with the Pydantic models and `openapi.json`.

Conventions (from the contract): `county_fips` is a 5-digit string, `weather_zone` is one of COAST, EAST, FWEST, NCENT,
NORTH, SCENT, SOUTH, WEST (plus `ERCOT` for the system), MW are floats, years are ints, and dates are ISO 8601
(timestamps in UTC). Every response uses the `meta` + `data` envelope.

Labels used below:

- **Status.** *supported*: an analysis built this table and its prototype sits in `analysis/out/` (gitignored; row
  counts from the prototype files on 2026-09-26). *partial*: the numbers exist in a doc, but the table layout is new
  here. *speculative*: no analysis tested it.
- **Rows.** A number marked *prototype* was counted in the prototype file. A number marked *derived* is this doc's
  arithmetic on the grain.
- **Contract today.** *implemented*: served by get-data now (only §6, the /data endpoints). *draft §N*: described in
  the contract's draft but not implemented. *new*: not in the contract.
- **R*n*** = item *n* of the [review queue in findings.md](findings.md#review-queue-for-pablo). Review items that
  stayed in a single analysis doc are cited by doc and item number.
- **Refresh.** The models are not scheduled yet (models are out of scope in `CLAUDE.md` for now). The cadence
  proposed here is "after the input source lands". The source crons come from `dags/basecast_dags/config.py`
  (America/Chicago).

## 0. Summary

| Screen | Mart | Grain | Rows | Contract today | Blocked by |
|---|---|---|---:|---|---|
| /explorer | `queue_adjusted_county` | report month × county × stratum | ~970 / month (derived) | draft §1 (2 of its 5 metrics) | R4, R10 |
| /explorer | `queue_project_scores` | report month × project (`inr`) | 1,810 (prototype) | new | R4, R10 |
| /explorer | `data_center_sites_new` | TCEQ site (RN) | 38 (prototype) | new | R7 |
| /explorer, /forecast | `zone_peak_excess` | zone × summer × basis | 864 (derived) | new (a §2 metric) | R5, R9 |
| /forecast | `peak_forecast` | run × as-of × region × year × variant × layer | 540 / as-of (derived) | draft §3 (shape differs) | R13, R3, R5, R9 |
| /forecast | `large_load_realization` | deck vintage × target year | 63 (prototype) | new | R3 |
| /forecast | `large_load_in_service` | deck vintage × in-service year × status | not counted | new | R3 |
| /forecast | `large_load_monthly` | month | 37 (prototype) | new (nearest: §2 `large_load_raw_mw`) | R3 |
| /forecast | `organic_peak_history` | region × summer | 216 (derived) | new | R5, R8 |
| /forecast | `queue_stage_curves` | as-of × stage × stratum × weighting × month | 3,413 for Q6's curves (prototype) | new | R4 |
| /forecast, /accounts | `four_cp_intervals` | summer month | 76 (derived) | new | R11 |
| /forecast, /accounts | `four_cp_zone` | zone × year | 152 (prototype) | new | R11 |
| /forecast | `four_cp_dispatch_curve` | forecast × rule parameter × window | 450 (prototype) | new | R11 |
| /forecast | `four_cp_scarcity` | summer | 16 (derived) | new | R11 |
| /backtest | `peak_backtest` | as-of × target year × source | ~52 (derived) | draft §4 (one year only) | R13, R3, R6 |
| /backtest | `official_forecast_errors` | product × vintage × target year | 354 (prototype) | draft §4 (partial) | R6, R8 |
| /backtest | `actual_summer_peaks` | summer | 19 (prototype) | draft §4 (`actual` label) | R6 |
| /backtest | `queue_backtest` | report month × stratum | 15 (derived) | new | R4, R10 |
| /accounts | `accounts` | account (`ccn_no`) | 107 now, 112 after A2 | draft §5 (key and fields differ) | R1, R2, R12, R14 |
| /accounts/[id] | `account_events` | account × event | 6,014 (prototype) | draft §5 `triggers[]` | R12, R7 |
| /accounts/[id] | `account_facts` | account × fact key | ~4.6k (X9) | draft §5 `profile`, `coverage` | R1, R2, R14 |
| /accounts/[id] | `account_counties` | account × county | not counted | draft §5 `profile.counties[]` | R14 |
| /accounts/[id] | `account_eia_series` | account × data year × release | ~1.4k (X9) | new | R2, 861S parser |
| /accounts/[id], /forecast | `zone_outlook` | weather zone | 8 | new | R11, R5 |
| /accounts/[id] | `account_private_facts` | account × fact key | — | `meta.simulated`, `coverage` | adapters not built |
| /data | (registration of the marts above) | — | — | implemented §6 | — |

Every mart is small enough for Polars in memory, as the architecture asks. The largest is `account_events` (~6k rows).
Recomputing any simulation live is not needed (X7 §4).

## 1. Rules that apply to every mart

- **`model_version` and `as_of` on every row.** They go to `meta.model_run_id` and `meta.data_as_of` in the envelope.
- **`verified`.** Every value read from chart images of the large-load decks carries `verified = false` until the
  Q5 human spot-check is ticked (`docs/large-load-spot-check.md`, R3). X6 re-read 10 of those values by eye, and all
  10 agree with Gemini. That read does not replace the spot-check.
- **Validation lock.** The account marts are built from the scored universe only: 107 accounts until A2 reveals the
  held-out five. `diagnosis.assemble()` raises `KeyError` for an account outside that universe, and the API must keep
  that behaviour. `is_base_partner` stays out of every payload until A2 (X9 §4, R14).
- **One name per mart.** X2 calls the county table `queue_adjusted_county` and X9 calls it `gen_queue_adjusted`. X5
  calls the event table `account_triggers` and X9 calls it `account_events`. This doc keeps `queue_adjusted_county`
  (the county grain is the map's grain) and `account_events` (it also holds context events, not only triggers).
- **Preliminary actuals.** July–August 2026 are not final-settled, and September 2026 comes from the daily
  weather-zone load (Q1). Rows scored against 2026 carry `actual_final = false`.
- **Simulated data** follows the rule in §7.

## 2. /explorer

The map needs the generation queue, raw and adjusted, at county grain. Large loads have no county detail: the decks
split only the approved stock, into LZ_WEST and "Other" (X1), and they give no regional split of the in-service
promise (X7). So a county-level "adjusted large-load queue" is **not supported**.

### 2.1 `queue_adjusted_county` (X2)

- **Key:** (`as_of_month`, `county_fips`, `stratum`), where `stratum` is one of `all`, `solar`, `storage`, `wind`,
  `gas_other`.
- **Built by:** `queue_adjusted.truncate_at` → `fit_stage_curves(..., fit_stages("entry_ia_sm"))` → `build_queue` →
  `score` → `aggregate`. This is X2's primary variant: semi-Markov entry stage, MW-weighted.
- **Refresh:** when a new GIS report month lands. The `ercot_gis` DAG checks every Monday at 07:00; the report itself
  is monthly.
- **Rows:** 194 counties with an active project × 5 strata ≈ 970 per report month (derived). The prototype,
  `x2_county_adjusted.csv`, has 194 rows, wide by fuel for Dec 2028 only.
- **Status:** supported.

| Column | Type | Unit | Note |
|---|---|---|---|
| `county_fips`, `county_name` | string | — | county → FIPS matches 100% (Q6 §7) |
| `weather_zone` | string \| null | — | null for the 54 projects (25.6 GW) in counties outside `county_weather_zone` (X2 review item 4) |
| `cdr_reporting_zone` | string | — | modal zone of the county's projects |
| `projects`, `projects_ia` | int | count | active projects; of which IA-stage (IA signed or synchronized) |
| `raw_mw`, `raw_mw_ia` | float | MW | listed capacity of active projects |
| `adj_mw_2027`, `adj_mw_2028` | float | MW | expected MW reaching COD by Dec 2027 / Dec 2028 |
| `ratio_2027`, `ratio_2028` | float | fraction | adjusted ÷ raw |
| `rank_raw`, `rank_adj` | int | rank | the map's "change" metric is `rank_raw − rank_adj` |
| `large_gas_mw_2028` | float | MW | *speculative*: adjusted MW from gas+other projects of ≥ 500 MW, for the UI flag R10 proposes |
| `model_version`, `as_of_month` | string, date | — | |

- **Horizons:** only Dec 2027 and Dec 2028 were computed, and only 24-month windows were backtested. Dec 2028 is
  already past the tested window. Adding columns for 2029–2030 is speculative and would need the "beyond the
  backtested window" label (X2 decisions).
- **Contract:** draft §1 `GET /geo/counties/metrics` has `gen_queue_raw_mw` and `gen_queue_adjusted_mw`, with
  `value`, `p10`, `p90`. **p10/p90 are not supported yet**: the Bernoulli band (±1.7–2.5 GW statewide) is too narrow,
  and a band built from the backtest errors (±13%) or a bootstrap is still to do (R10). New: `stratum`, the ratio,
  the ranks, and a horizon (Dec YYYY) in place of `year`. The draft's other three metrics:
  - `organic_peak_growth_mw` is not supported at county grain. Q7 and X7 fit zones and ERCOT only.
  - `permits_units` needs no model. It is `census_permits_county` as is, with Q3's caveats: BPS counts permit-issuing
    places only, and 31 of 254 counties have no report.
  - `acquisition_score` is an account score (X5), not a county one. Putting it on a county needs an aggregation rule
    that no analysis tested, so it is speculative. "Priority acquisition zones" can be drawn today from the
    `accounts` mart over the PUCT territory polygons.
- **Blocked by:** R4 (semi-Markov entry stage in place of Q6's landmark curves) and R10 (large new gas, band).

### 2.2 `queue_project_scores` (X2)

- **Key:** (`as_of_month`, `inr`).
- **Built by:** `queue_adjusted.score`, in the same pipeline as 2.1.
- **Refresh:** same as 2.1.
- **Rows:** 1,810 (prototype `x2_project_scores.csv`).
- **Status:** supported.
- **Why it exists:** with it in memory, the API can re-aggregate by fuel, stage or zone in milliseconds (X2), and the
  map can drill down to projects.

| Column | Type | Unit | Note |
|---|---|---|---|
| `inr`, `project_name` | string | — | the contract's `inr` key convention |
| `county_fips`, `county_name`, `weather_zone`, `cdr_reporting_zone` | string | — | |
| `fuel_type`, `stratum` | string | — | strata: solar, storage, wind, gas_other |
| `stage` | string | — | `entry` (including FIS approved without an IA) or `ia` (IA signed, or synchronized without COD) |
| `stage_date`, `elapsed_months` | date, float | months | time spent at the stage |
| `capacity_mw` | float | MW | latest listed capacity |
| `projected_cod` | date \| null | — | the developer's date, shown as the baseline X2 beats |
| `curve` | string | — | own stratum or pooled |
| `p_cod_2027`, `p_cod_2028` | float | probability | P(COD by Dec YYYY \| stage, elapsed time) |
| `mw_2027`, `mw_2028` | float | MW | `capacity_mw × p` |
| `clamped_2027`, `clamped_2028` | bool | — | clock clamped past the curve's support (fewer than 10 at risk) |

- **Contract:** new. The contract has the `inr` key convention but no project endpoint.
- **Blocked by:** R4 and R10.

### 2.3 `data_center_sites_new` (Q4)

- **Key:** `tceq_rn`.
- **Built by:** `data_centers.select_new_sites` → `classify_sites` (over `county_territory_types`) → `flag_metro`
  (over `county_density`).
- **Refresh:** weekly, after `tceq_air_permits` (Wednesdays at 05:00).
- **Rows:** 38 (prototype `q4_sites.csv`): sites whose first permit is on or after 2025-01-01, in 27 counties.
- **Status:** supported.

| Column | Type | Unit | Note |
|---|---|---|---|
| `tceq_rn`, `site_name`, `county_fips`, `county_name` | string | — | |
| `city` | string \| null | — | 10 of 38 have one |
| `first_permit_date` | date | — | earliest `affil_begin_dt`; what that date means is not verified |
| `matched_by` | string | — | `name` or `naics` (23 of 38 match on NAICS 518210 only) |
| `has_undated_affiliation` | bool | — | |
| `largest_type` | string | — | coop \| muni \| iou (by county, not by point) |
| `coop_share_w` | float | fraction | normalized co-op `county_share` |
| `iso_class` | string | — | 7 sites sit in counties that are not mainly ERCOT |
| `density_per_km2` | float | people/km² | |
| `metro_legacy`, `metro_density` | bool | — | the 13-county list; density ≥ 100/km² |

- **Map use:** a county choropleth of the site count, with a toggle that hides the NAICS-only matches. Label it "by
  county, not by point" (Q4 "Useful").
- **Contract:** new (for example, a `dc_sites_new` metric in §1). `tceq_data_center_sites` has no snapshot date
  (X9 §3.1), so `data_as_of` falls back to the latest permit date until the parser adds one.
- **Blocked by:** R7 (metro rule, NAICS-only matches, non-ERCOT sites).

### 2.4 `zone_peak_excess` (X1). Also used by /forecast.

- **Key:** (`region_id`, `year`, `basis`). `region_id` is one of the 8 weather zones or `ERCOT`. `basis` is
  `noncoincident`, `coincident`, `dmax` or `dmin`.
- **Built by:** `peak_excess.excess_vs_prebreak`, `coincident_zone_loads` and `summer_shape` / `flat_block_table`.
- **Refresh:** yearly, after the summer, once the D&E report carries September (on the 10th). The current summer's row
  is flagged partial.
- **Rows:** 9 regions × 24 summers (2003–2026) × 4 bases = 864 (derived). The prototype `x1_excess_peak.csv` has 216
  rows for one basis.
- **Status:** supported.

| Column | Type | Unit | Note |
|---|---|---|---|
| `actual_mw`, `predicted_mw` | float | MW | prediction from the pre-break fit (summers ≤ 2019; NORTH 2010–2019 is the proposal) |
| `excess_mw`, `excess_pct` | float | MW, % | |
| `train_resid_sd_mw`, `trend_mw_yr` | float | MW, MW/yr | |
| `share_coincident` | float \| null | fraction | mean 2023–2026 share of the coincident excess (FWEST 30.7%, SCENT 25.5%, COAST 17.2%, NORTH 13.1%) |
| `min_max_ratio` | float | ratio | average summer day's min/max load: the "flat load arrived" metric (WEST 0.63 → 0.72, 2019 → 2026) |
| `partial` | bool | — | the current summer |

- **Contract:** new. It would be a new metric in draft §2 `/geo/zones/metrics`. The draft's `large_load_raw_mw` by
  load zone is supported only as two buckets (LZ_WEST and Other, approved stock). By weather zone it is not supported,
  and `large_load_adjusted_mw` by zone is not supported either (X7: the large-load layer is statewide).
- **Blocked by:** R5 (what "organic" means), R9 (flatness vs behind-the-meter solar; don't name WEST's 2026 block).

## 3. /forecast

The Forecast module has three parts: the peak forecast by layer, the large-load flow (promised vs approved), and the
generation-queue survival curves. The 4CP card from X3 also lives here, and the account page reuses its tables.

### 3.1 `peak_forecast` (X7 §4)

- **Key:** (`run_id`, `as_of`, `region_type`, `region_id`, `target_year`, `variant`, `layer`).
- **Built by:** `peak_forecast.fit_organic` → `organic_draws`; `incremental_ratios`, `observed_factor`, `unattributed`
  and `a2e_path` → `large_load_draws`; `simulate` → `summarize`.
- **Refresh:** after each new large-load deck (the deck DAG runs every Monday at 08:00; the decks come out about
  monthly), and yearly after the summer (organic refit, U).
- **Rows:** 9 regions × 5 years × 3 variants × 4 layers = 540 per as-of (derived). The prototypes have 40 ERCOT rows
  (`x7_forecast_ercot.csv`: 2 deck variants × 4 layers × 5 years) and 40 zone rows.
- **Status:** supported. The band is not calibrated.

| Column | Type | Unit | Note |
|---|---|---|---|
| `run_id`, `as_of` | string, date | — | `as_of` is also what the /backtest slider moves |
| `region_type`, `region_id` | string | — | `ercot`/`ERCOT` or `weather_zone`/`FWEST`…; zones are **coincident contributions** to the ERCOT peak |
| `target_year` | int | — | 2027–2031 today; R13 proposes stopping at 2030 |
| `variant` | string | — | `deck_pre_batch_zero` (default), `deck_latest`, `approvals_pace` (a sanity check, P50 only) |
| `layer` | string | — | `organic`, `large_load`, `unattributed`, `total` |
| `p10_mw`, `p50_mw`, `p90_mw` | float | MW | |
| `deck_vintage` | date | — | the deck behind the large-load layer |
| `factor` | float | ratio | observed simultaneous peak ÷ approved stock (0.512) |
| `ratio_p10`, `ratio_p50`, `ratio_p90` | float | ratio | incremental realization ratio (0.131 / 0.187 / 0.289) |
| `share_of_ll_u` | float \| null | fraction | zone rows: the fixed share of LL + U (from `zone_peak_excess`) |
| `verified` | bool | — | false while the deck values are machine-read |

- **Contract:** draft §3 `GET /forecasts?region_type=weather_zone|county|utility` returns `series[]` (year, p10, p50,
  p90) and `components[]` (organic_mw, large_load_mw, generation_added_mw). The differences:
  - Add `unattributed_mw`. It is the third layer (X1, X7).
  - `generation_added_mw` has no source in these analyses. This is a demand forecast; X2's adjusted queue is supply.
    Drop it, or serve it as a separate supply series (open question for get-data).
  - `region_type=county` is not supported. No analysis fits a county peak.
  - `region_type=utility` needs the account's own load (UtilityDataSource), so it would be **simulated** (§7).
  - Zones are a fixed allocation of a statewide layer, not zone forecasts of large load. Label them that way (X7).
  - Add `variant` and an inputs block (deck date, factor, ratio band, approved stock), all labelled
    "machine-read, not verified".
- **Blocked by:** R13 (default deck, a band that covers 61% of cells against a nominal 80%, stop at 2030), R3
  (spot-check), R5, R9, and X1 review item 3 / X7 review item 3 (whether "simultaneous" means at the system peak,
  which decides if 0.51 is the peak contribution or an upper bound).

### 3.2 `large_load_realization` (Q5, X7)

- **Key:** (`deck_vintage`, `target_year`).
- **Built by:** `large_load.realization_ratios`, with `summarize_ratios` for the band. The as-of subsets used by the
  backtest come from `peak_forecast.incremental_ratios`.
- **Refresh:** with each deck.
- **Rows:** 63 (prototype `q5_ratios.csv`).
- **Status:** supported.

| Column | Type | Unit | Note |
|---|---|---|---|
| `deck_vintage`, `report_date` | date | — | one vintage per calendar month |
| `target_year`, `horizon_months` | int | —, months | the band uses horizons ≥ 6 months |
| `promised_mw`, `promised_firm_mw` | float | MW | the deck's in-service bar for the year (firm = minus "No Studies Submitted") |
| `base_a2e_mw` | float | MW | the deck's own approved-to-energize stock |
| `realized_a2e_mw`, `realized_energized_mw` | float | MW | December stock; observed energized |
| `known_from` | date | — | first deck that reports the December stock (X7) |
| `gross_a2e`, `gross_a2e_firm`, `incremental_a2e`, `incremental_a2e_firm`, `gross_energized` | float | ratio | |
| `document`, `page` | string, int | — | for a link to the slide |
| `verified` | bool | — | |

- **Contract:** new. It feeds the "promised vs approved" chart and the ratio band.
- **Blocked by:** R3 (spot-check, headline ratio definition, approved-to-energize vs energized).

### 3.3 `large_load_in_service` (Q5)

- **Key:** (`deck_vintage`, `in_service_year`, `status`), where `status` is one of `approved_to_energize`,
  `planning_studies_approved`, `under_ercot_review`, `no_studies_submitted`. The later decks' sub-splits are summed
  back into these four (Q5 §1).
- **Built by:** `large_load.in_service_wide` + `pick_vintages`.
- **Refresh:** with each deck.
- **Rows:** not counted: 28 vintages × the bars each deck prints × 4 statuses.
- **Status:** partial. The prototype is a figure (`q5_promised_vs_realized.png`), not a table.
- **Columns:** `mw` (float, MW, cumulative: MW in service by the end of the year), `document`, `page`, `verified`.
- **Contract:** new. It is the "promised" layer of the large-load flow chart.
- **Blocked by:** R3.

### 3.4 `large_load_monthly` (Q5, X1)

- **Key:** `month`.
- **Built by:** `large_load.a2e_by_month` (misdated axes dropped; X1's `peak_excess.a2e_by_month_checked` gives the
  same answer), `peak_excess.energized_by_month` and `peak_excess.approved_by_load_zone`.
- **Refresh:** with each deck.
- **Rows:** 37 approved-stock months (prototype `q5_a2e_by_month.csv`) and 32 observed-peak months
  (`x1_energized_by_month.csv`).
- **Status:** supported.

| Column | Type | Unit | Note |
|---|---|---|---|
| `month` | date | — | first of month |
| `a2e_mw` | float \| null | MW | approved-to-energize stock; null for 2023-11, 2024-02 and 2024-04, which have no reading (Q5: gap or carry forward) |
| `observed_simultaneous_mw`, `observed_nonsimultaneous_mw` | float \| null | MW | "Loads Approved to Energize – Observations" |
| `a2e_lz_west_mw`, `a2e_other_mw` | float \| null | MW | the only regional split the decks give |
| `read_from_vintage`, `document`, `page` | date, string, int | — | latest reading wins |
| `verified` | bool | — | |

- **Contract:** new. The nearest item is draft §2 `large_load_raw_mw` by load zone.
- **Blocked by:** R3. Also X6 note 7: the Jan and Feb 2026 decks print "January 2025" for the 3,977 MW observed peak,
  and `large_load_headlines` stores it a year early. Anything that dates observed peaks from the headlines needs a
  parser fix first.

### 3.5 `organic_peak_history` (Q7): the weather-normalized series

- **Key:** (`region_id`, `year`), where `region_id` is one of the 8 weather zones or `ERCOT`.
- **Built by:** `weather_load.summer_peaks`, `summer_weather_features`, `fit_peak`, `holdout` and `rolling_origin`.
- **Refresh:** yearly after the summer, and weekly while the summer is in progress (daily load at 06:15; Open-Meteo on
  Wednesdays).
- **Rows:** 9 × 24 summers (2003–2026) = 216 (derived). A 9-row model card (R², trend, weather coefficient, residual
  SD, hold-out MAPE, sup-Chow break year and p) goes with it.
- **Status:** partial. Q7's tables hold the numbers, but the prototype is a figure only.

| Column | Type | Unit | Note |
|---|---|---|---|
| `actual_peak_mw`, `peak_ts_utc` | float, timestamp | MW | zones: own (non-coincident) peak; ERCOT: system peak |
| `weather_feature_c` | float | °C | `t_mean_3d` |
| `fitted_fullsample_mw`, `fitted_prebreak_mw` | float | MW | 2003–2025 fit and ≤ 2019 fit |
| `expected_at_actual_weather_mw` | float | MW | 2026: ERCOT 84,169 MW (full sample) |
| `weather_p10_mw`, `weather_p50_mw`, `weather_p90_mw` | float | MW | weather-only band |
| `one_year_ahead_error_pct` | float \| null | % | rolling origin |
| `flagged` | bool | — | NORTH, FWEST, SCENT (hold-out MAPE > 10%) |
| `partial` | bool | — | 2026 |

- **Contract:** new. The Forecast module asks for a "weather-normalized load time series", and the draft has no
  endpoint for it.
- **Blocked by:** R5 (the band and the definition of organic) and R8 (the thresholds behind `flagged`).

### 3.6 `queue_stage_curves` (Q6, X2)

- **Key:** (`as_of_month`, `stage`, `stratum`, `weighting`, `month`). `stage` is `entry_to_ia` (the semi-Markov step)
  or `ia`. `weighting` is `mw` or `count`.
- **Built by:** `queue_adjusted.fit_stage_curves` (`StageCurve`), on top of `survival.fit_cif`. Q6's landmark curves
  come from `survival.cif_table`.
- **Refresh:** with each GIS report month.
- **Rows:** 3,413 for Q6's landmark × stratum curves (prototype `q6_cif_curves.csv`, count-weighted). X2's stage
  curves were not exported, so they are not counted.
- **Status:** supported (Q6); partial (X2's curves).
- **Columns:** `month` (int), `at_risk` (int), `cif_cod`, `cif_withdrawn`, `survival` (floats, probability) and
  `supported` (bool: at least 10 at risk; past that point the app shows no value, as Q6 review item 5 proposes).
- **Screen:** "COD by 12/24/36/48 months" plus the withdrawal curve for each stratum. A single project's conditional
  chance comes from `queue_project_scores`.
- **Contract:** new.
- **Blocked by:** R4 (Aalen–Johansen with the semi-Markov entry stage, COD as the event, the at-risk cut).

### 3.7 The 4CP tables (X3)

All four are **new** to the contract. X3 proposed the first three names. All four are blocked by **R11**: the D&E
interval-ending label is not verified against ERCOT's settlement 4CP list, and there is no $/kW-yr rate in the repo,
so no dollar column.

| Mart | Key | Built by | Rows | Columns | Refresh |
|---|---|---|---:|---|---|
| `four_cp_intervals` | (`year`, `month`), June–September | `four_cp.cp_intervals` (D&E 15-min); `four_cp.hourly_cps` for months the D&E has not published | 76 (derived: 2008–2026 × 4) | `interval_end_local` (string, CPT), `interval_end_utc` (timestamp), `mw` (float, MW), `source` (`de_15min` \| `hourly_provisional`), `final` (bool) | monthly after D&E (the 10th, 07:30); daily and provisional in June–September |
| `four_cp_zone` | (`region_id`, `year`), weather zones and load zones | `four_cp.zone_coincidence` | 152 weather-zone rows (prototype `x3_zone_coincidence.csv`: 8 zones × 19 years); load-zone rows not counted | `cp_avg_mw`, `ncp_summer_mw` (MW), `cf_summer`, `share_4cp`, `share_energy` (fractions), `intensity` (= share_4cp ÷ share_energy), `ncp_end_hour` (local), `energy_mwh` | yearly after the summer |
| `four_cp_dispatch_curve` | (`forecast`, `param`, `window`) | `four_cp.threshold_dispatch` / `top_n_dispatch` → `evaluate_dispatch` → `summarize_dispatch` | 450 (prototype `x3_dispatch_rules.csv`) | `dispatch_days` (per summer), `all4_rate`, `month_rate`, `day_rate` | static, re-run once a year |
| `four_cp_scarcity` | `year` (2011–2026) | `four_cp.net_load_hourly`, `monthly_argmax`, `rank_within_month` | 16 (derived) | `load_peak_mean_he`, `net_load_peak_mean_he`, `cp_net_load_rank_median`, `cp_price_rank_median`, `cp_in_top20_price_share`, `top20_price_after_18h_share`, `wind_solar_share` | yearly |

- `four_cp_dispatch_curve` carries the label "optimistic: the forecast uses observed ERA5 weather". Whether ~55
  two-hour cycles a summer fit Base's fleet is X3 review item 3, which is a FleetDataSource question (§7).
- `four_cp_scarcity` uses real-time hub prices only (ancillary services are out of the MVP), so it also depends on X3
  review item 5. Its numbers are in X3's tables, but there is no CSV prototype, only the parsed price and fuel-mix
  caches.

## 4. /backtest

### 4.1 `peak_backtest` (X7 §4)

- **Key:** (`as_of`, `target_year`, `source`, `vintage`).
- **Built by:** `peak_forecast.simulate` → `summarize` → `score`. The official rows come from
  `peak_forecast.official_asof`, over `backtest.base_series`.
- **Refresh:** when a new official vintage (LTLF or CDR; checked monthly on the 2nd) or deck lands, and again once
  summer 2026 is final-settled.
- **Rows:** ~52 (derived): 18 basecast cells (prototype `x7_backtest_paired.csv`) plus the official rows scored on
  the same cells (18 LTLF, 15 CDR, 1 preliminary). `x7_backtest.csv` has 55 rows across both deck variants.
- **Status:** supported.

| Column | Type | Unit | Note |
|---|---|---|---|
| `as_of`, `target_year`, `horizon` | date, int, int | — | horizon = summers ahead (Q1 convention) |
| `source`, `product`, `vintage`, `vintage_date` | string, date | — | `basecast`, or `LTLF` / `CDR` / `LTLF-prelim` |
| `variant` | string \| null | — | basecast rows only |
| `p10_mw`, `p50_mw`, `p90_mw` | float | MW | official rows fill `p50_mw` only |
| `actual_mw`, `actual_final` | float, bool | MW | 2026: `actual_final = false` |
| `error_pct`, `in_band` | float, bool | % | |
| `organic_p50`, `ll_p50`, `u_p50`, `ll_realized`, `u_realized` | float | MW | "which layer misses" (X7 §3) |
| `leak_note` | string \| null | — | e.g. "factor and ratio from decks published 2024-04-01" |

- **Optional rows** (partial): `source = basecast_organic_only` (the ablation: MAPE 10.5% vs 3.3%) and Q7's
  one-year-ahead errors, as the honest companion that X7 and Q7 suggest.
- **Contract:** draft §4 `GET /backtest?target=summer_peak&year=` returns one year's `series[]`, with the labels
  `official_preliminary`, `official_adjusted`, `actual` and `model`. That covers the 2026 fan chart only (112 GW, the
  90.5–98 GW range, the actual, the model). New: `as_of`, `horizon`, `error_pct`, `in_band`, `leak_note`, and the
  score split by era (before and after ERCOT's forecasts took TSP large loads in).
- **Blocked by:** R13, R3, and R6 (how the preliminary is labelled and worded).

### 4.2 `official_forecast_errors` (Q1)

- **Key:** (`product`, `vintage`, `target_year`).
- **Built by:** `backtest.base_series` → `match_forecasts`, with `horizon_summary` for the summary table
  (`sign_verdict` for the decision).
- **Refresh:** after the monthly LTLF/CDR check (on the 2nd) and the D&E report (on the 10th).
- **Rows:** 354 (prototype `q1_errors.csv`), plus 22 summary rows (`q1_horizon_summary.csv`).
- **Status:** supported.
- **Columns:** `product`, `vintage`, `vintage_date`, `target_year`, `horizon`, `series` (which base series the value
  came from), `forecast_mw`, `actual_mw`, `actual_complete` (bool), `actual_peak_local`, `error_mw`, `error_pct`.
- **Screen:** the vintage fan (every LTLF vintage against the actual line, with the 112 GW star and the 90.5–98 GW
  bar) and the vintage × target error matrix (Q1 §3).
- **Contract:** draft §4 has only one year's series. The matrix is new.
- **Blocked by:** R6 and R8 (the "consistent sign" threshold). Also Q1 review item 4: the LTLF 2013 and 2014 dates look
  like upload dates.

### 4.3 `actual_summer_peaks` (Q1)

- **Key:** `year` (2008–2026).
- **Built by:** `backtest.actual_summer_peaks` (and `backtest.summer_peaks`).
- **Refresh:** monthly after D&E (the 10th), and provisional in season from the hourly table.
- **Rows:** 19 (prototype `q1_actual_summer_peaks.csv`).
- **Status:** supported.
- **Columns:** `hourly_peak_mw`, `peak_ts_utc`, `hour_ending_local`, `peak_15min_mw`, `interval_end_local_15min`,
  `final` (bool), `data_as_of`.
- **Contract:** the `actual` label in draft §4.
- **Blocked by:** R6. The record in January 2026 was 85,508 MW (2023); 91,134 MW (2026-07-22) is preliminary.

### 4.4 `queue_backtest` (X2 §3)

- **Key:** (`report_month`, `stratum`), where `stratum` is `all` or one of the four fuel groups.
- **Built by:** `queue_adjusted.truncate_at` + `build_queue` + `score` + `aggregate` on each past snapshot, with
  `actual_cod_mw` for the outcome.
- **Refresh:** static. Re-run when another 24-month window closes.
- **Rows:** 3 dates × 5 = 15 (derived). The project-level `x2_backtest_projects.csv` (35,610 rows) stays out of the
  API.
- **Status:** supported.
- **Columns:** `raw_mw`, `pred_mw`, `actual_mw`, `developer_projected_mw` (MW); `error_pct`; `window_months` (24);
  `model_variant`. Per date: `county_rho_adj`, `county_rho_raw`, `county_rho_developer` (Spearman: 0.64–0.66 adjusted,
  0.43–0.45 raw).
- **Contract:** new. It could be a second target in draft §4 (for example `target=gen_queue_cod`).
- **Blocked by:** R4 and R10.

## 5. /accounts and /accounts/[id]

Every source here is public (X9: "nothing simulated"). The one fact that no public source carries is the account's
own load. §7 covers it.

### 5.1 `accounts` (X5, X9)

- **Key:** `account_id` = PUCT `ccn_no` (string).
- **Built by:** `accounts.build_universe` → `build_signals`; `triggers.score_accounts` → `score_tier` →
  `next_action`; `diagnosis.action_changes_on` for the lapse date; `eia861_short_form.utility_signals` for meters.
- **Refresh:** daily. Event ages and lapse dates move with the calendar, and `ercot_mp_list` is daily. GIS, TCEQ and
  the Comptroller are weekly; BPS and TPIT monthly; EIA, ACS and PEP yearly.
- **Rows:** 107 while the validation lock holds, 112 after A2.
- **Status:** supported. Weights are pending.

| Column | Type | Unit | Note |
|---|---|---|---|
| `account_id`, `name` | string | — | |
| `account_type` | string | — | `coop` \| `muni` |
| `eia_utility_id` | string \| null | — | from the crosswalk (5 rows pending, R2) |
| `gt` | string \| null | — | G&T / wholesale supplier; 81.3% coverage (X9 §3.2) |
| `primary_weather_zone` | string | — | largest overlap area |
| `meters` | float \| null | count | EIA bundled + delivery-only, latest final year |
| `score`, `rank`, `tier` | float, int, string | — | tier A (top quartile), B (second), C (bottom half) |
| `pct_<signal>`, `raw_<signal>` | float \| null | — | the six weighted signals today; the list payload nests them as `signals` |
| `next_action` | string | — | `call_now` \| `nurture` \| `watch` \| `hold` |
| `action_changes_on` | date \| null | — | when the action lapses without a new event (5 of 25 call-nows lapse within 90 days) |
| `n_strong`, `n_context` | int | — | active events (12 months) |
| `latest_event_date` | date \| null | — | |
| `top_trigger` | object \| null | — | `{trigger, title, event_date, age_days}` |
| `active_triggers`, `flags` | string[] | — | flags: `no_exposed_county`, `apportionment_under`, `apportionment_over`, `short_form`, `eia_break` |
| `simulated` | bool | — | false: every input is public |

- **Contract:** draft §5 `GET /accounts` is keyed by `utility_id` and lists `priority_score`, `first_deficit_year`,
  `deficit_mw_p50_next_3y`, `is_base_partner`, `parent_utility_id`, `in_ercot` and `top_trigger`. The differences:
  - The key becomes `ccn_no` (R14, X9 §4).
  - `priority_score` becomes `score`, `rank` and `tier`.
  - `first_deficit_year` and `deficit_mw_p50_next_3y` are **not supported**. There is no account-level forecast until
    A3, and a deficit needs the account's own load, which would be simulated (§7). X9 proposes leaving them out.
  - `is_base_partner` is held until A2.
  - `parent_utility_id` becomes the G&T's name. The analyses carry no EIA id for G&Ts.
  - `in_ercot` is implicit: the universe is ERCOT only. The 13 mixed-ISO co-ops could carry a badge (Q2).
  - CSV export (`/accounts/export.csv` in X9, `/accounts/{id}/export.csv` in the draft) is the same rows. Both are
    supported.
- **Blocked by:** R1 (weights: Q3 or X4), R2 (crosswalk), R12 (strong vs context triggers), R14 (key, partners,
  munis) and R7 (the data-center signal).

### 5.2 `account_events` (X5; X9)

- **Key:** (`account_id`, `trigger`, `source_ref`, `county_fips`).
- **Built by:** `triggers.dc_permit_events`, `gen_storage_events`, `dev_agreement_events`, `registration_events`,
  `permit_surge_events`, `transmission_events`, `rate_increase_events` and `tsp_large_load_events`, then
  `active_events`.
- **Refresh:** daily. Alternatively, `age_days` and `active` can be computed at request time from the envelope's
  `data_as_of`.
- **Rows:** 6,014 (prototype `x5_trigger_events.csv`).
- **Status:** supported.
- **Columns:** `account_id`, `trigger`, `strength` (`strong` \| `context`), `event_date`, `age_days`, `active`,
  `title`, `detail`, `county_fips` and `county_name` (null for name matches), `exposure` (county_share, or 1 for a
  name match), `mapping` (`county` \| `name`), `source` (table), `source_ref` (RN, INR, agreement id, TPIT id,
  docket:item) and `offer_angle`.
- **Endpoints (X9):** the detail page inlines the active events plus one summary line per context trigger. The full
  history is paginated at `/accounts/{id}/events`, because Big Country EC alone has 178 events, which would make the
  detail JSON 141 KB inline.
- **Contract:** draft §5 `triggers[]` (`date`, `kind`, `description`, `evidence[]`). The richer event shape is new.
- **Blocked by:** R12 and R7, plus X9 review item 6 (choose the dev-agreement angle by NAICS, so wind-farm abatements
  stop reading as load).

### 5.3 `account_facts` (X9)

- **Key:** (`account_id`, `key`).
- **Built by:** `diagnosis.assemble` → `all_facts`, with `data_gaps` and `coverage` for the gap list.
- **Refresh:** daily, with `accounts`.
- **Rows:** ~4.6k (X9 §4).
- **Status:** supported.
- **Columns:** `block` (`header` \| `territory`), `key`, `label`, `value` (number \| string \| null; null is a gap),
  `unit`, `source`, `as_of` (date \| null), `note` and `simulated` (bool; §7).
- **Contract:** draft §5 `profile` (`name`, `type`, `counties[]`, `load_zones[]`) and `coverage`. The Fact shape is
  new. `load_zones[]` is not supported: no account has a load zone yet. The fix is `ercot_noie_load_map`, a mapping aid
  that has not been built into a table (X9 §3.1).
- **Blocked by:** R1, R2 and R14 (munis: 55 of 59 never reach the 20% county rule; the home-county facts are a
  labelled stopgap).

### 5.4 `account_counties`, `account_eia_series`, `zone_outlook` (X9)

| Mart | Key | Built by | Rows | Columns | Contract | Blocked by |
|---|---|---|---:|---|---|---|
| `account_counties` | (`account_id`, `county_fips`) | `diagnosis.account_counties` + `with_context`, over `county_utility_overlap_puct` | not counted | `county_name`, `overlap_km2`, `county_share`, `territory_share`, `weather_zone`, `exposed` (≥ 20%), `context` (home county for munis) | draft §5 `profile.counties[]` (with the area fraction): extend it | R14 |
| `account_eia_series` | (`account_id`, `data_year`, `early_release`) | `diagnosis.eia_series`, over `eia861_short_form.combine_forms` + `add_delivery` + `with_price` | ~1.4k | `form` (long \| short), `customers`, `delivery_customers`, `meters`, `sales_mwh`, `revenue_kusd`, `price_usd_kwh`, `res_price_usd_kwh` (long form only, 57% coverage) | new | R2, and the 861S + Delivery_Companies parser change (X4 spec; findings "Notes"). Until then the series is read from the raw zips, which the API cannot serve |
| `zone_outlook` | `weather_zone` | `diagnosis.zone_outlook`, over `four_cp.zone_coincidence` and the LTLF | 8 | `ltlf_start`, `ltlf_end`, `ltlf_cagr`, `ncp_now_mw`, `ncp_now_months`, `now_vs_ltlf`, `cp_year`, `cp_avg_mw`, `cf_summer`, `share_4cp`, `share_energy`, `intensity`, `ncp_end_hour`, `peak_mismatch` (early \| late \| null), `q7_holdout_mape` | new | R11, R5 |

The account page's queue block reads `queue_adjusted_county` (§2.1) through `account_counties`, so it needs no mart
of its own.

## 6. /data

The /data page is already served by the implemented part of the contract (§6: `/lake/*`, `/tables/*`,
`/pipeline/runs`). No analysis asks for a new mart here. Three proposals:

- **Register the marts.** Put every mart above in `dataset_registry`, so `/tables` lists it with its rows and
  `/data/flow` can draw the model step. The app's flow graph links a derived table to its inputs only when get-data
  sends `inputs` on the table, which it does not do yet (basecast-app `CLAUDE.md`). Each mart's "Built by" line above
  lists those inputs. *Speculative*: the `inputs` field does not exist yet.
- **A run record for mart builds.** Write one row per build with `mart`, `model_version`, `as_of`, the inputs'
  `data_as_of`, `rows`, `status` and `duration`, either as a new table or as `etl_run` rows with a `model` stage.
  *Speculative*: no analysis tested it.
- **Data-quality notes the page can show** (each one is documented):
  - `large_load_chart_values.verified = false`: the spot-check is pending (R3).
  - `tceq_data_center_sites` has no snapshot date, no MW and no coordinates (X9 §3.1).
  - 861S is not a table yet (X4).
  - `large_load_headlines` dates the Jan/Feb 2026 observed peaks to 2025 (X6 note 7).
  - July–August 2026 are not final-settled (Q1).

## 7. The "simulated" label for private-adapter data

The rule comes from KICKOFF §7 ("Label as simulated everything that comes from the private-data adapters and from
fixtures") and from the contract (`meta.simulated: true` whenever a response uses fixtures or the simulated
adapters). `basecast_pipelines/adapters/` holds only `__init__.py` today: `FleetDataSource` and `UtilityDataSource` are
not built yet.

**Where a screen needs private data** (every other value on the six pages is public):

| Screen | Value | Why no public source has it | Adapter |
|---|---|---|---|
| /accounts/[id] | the account's load at the 4CP intervals (kW) | 0 of 107 accounts have it (X9 §3.1): the 4CP offer can be stated at zone level only | `UtilityDataSource` |
| /accounts/[id] | large-load requests the co-op itself received | public data stops at the G&T's total in the PUCT 58777 RFI (the `tsp_large_load` context trigger, X5) | `UtilityDataSource` |
| /accounts/[id] | how much of the co-op's load responds to 4CP; its own-peak vs 4CP choice in kW | X3: a question for the co-op's own interval data | `UtilityDataSource` |
| /accounts/[id] | offer sizing: fleet kW and kWh in the territory, backup reserve, cycle limits (can ~55 two-hour cycles a summer fit?) | not verified anywhere in the repo (X3 review item 3) | `FleetDataSource` |
| /accounts (list) | the contract's `first_deficit_year` and `deficit_mw_p50_next_3y` | a deficit needs the account's load and an account forecast (A3) | `UtilityDataSource` |
| /forecast | `region_type=utility` | no public account-level load | `UtilityDataSource` |

/explorer and /backtest need no private data.

**Proposed rule:**

1. **Row level.** Every value that comes from an adapter carries `simulated: true`, and its `source` names the adapter
   (`UtilityDataSource (simulated)`, `FleetDataSource (simulated)`). In the account page these are
   `account_private_facts` rows, with the same Fact shape as `account_facts`. *Speculative*: the adapters don't
   exist yet.
2. **Envelope.** `meta.simulated` is true when any row in the response is simulated, as the contract already says.
   The list's per-account `simulated` flag (X9) turns true when that account shows any adapter value.
3. **Next to the number.** The app shows the "simulated" label beside each simulated value, not only in a page
   banner. The account's `coverage` block (draft §5) says `utility_private_data` / `fleet_data`, and `resolution`
   moves from `zone` to `territory` only through simulated data, labelled "territory (simulated)".
4. **No mixing** (proposal). Simulated values never feed the public marts: score, rank, tier, triggers, next action,
   forecast, backtest. They sit on top as an overlay. Otherwise the ranking the partnerships team sees would depend on
   invented numbers, and X5/X9 state that everything in them is public.
5. **Validation lock.** No simulated overlay is rendered for an account outside the scored universe. The same
   `KeyError` from `diagnosis.assemble` applies.

## 8. Decisions → marts

| Review item | Decision | Marts that wait for it |
|---|---|---|
| [R1](findings.md#review-queue-for-pablo) | score weights: Q3 or X4 | `accounts`, `account_facts` |
| R2 | the 5 crosswalk rows below 90 | `accounts`, `account_eia_series` |
| R3 | Q5 spot-check; ratio definition; approved vs energized | `large_load_realization`, `large_load_in_service`, `large_load_monthly`, `peak_forecast`, `peak_backtest` |
| R4 | AJ with a semi-Markov entry stage; COD as the event; the < 10 at-risk cut | `queue_adjusted_county`, `queue_project_scores`, `queue_stage_curves`, `queue_backtest` |
| R5 | the band's spread; what "organic" means | `organic_peak_history`, `zone_peak_excess`, `peak_forecast`, `zone_outlook` |
| R6 | pitch wording; the record in the product context | `actual_summer_peaks`, `peak_backtest`, `official_forecast_errors` |
| R7 | metro rule; NAICS-only sites; non-ERCOT sites | `data_center_sites_new`, `accounts` (`dc_sites`), `account_events` (`dc_permit`) |
| R8 | thresholds (5% / 10%, 2/3, ±5%) | `organic_peak_history` (`flagged`), `official_forecast_errors` |
| R9 | the unattributed layer; flatness vs behind-the-meter solar | `peak_forecast`, `zone_peak_excess` |
| R10 | large new gas; the adjusted-queue band | `queue_adjusted_county`, `queue_project_scores`, `queue_backtest` |
| R11 | 4CP interval label; $/kW-yr | `four_cp_*`, `zone_outlook`, the offer text in `account_facts` |
| R12 | `gen_storage_ia` strong or context; `dc_sites` counted twice | `account_events`, `accounts` (`next_action`) |
| R13 | which deck drives the forecast; band calibration; stop at 2030 | `peak_forecast`, `peak_backtest` |
| R14 | accounts keyed by `ccn_no`; `is_base_partner` held; munis | `accounts`, `account_facts`, `account_counties` |

Not in the findings queue but blocking a column: the X1/X7 "simultaneous" definition (`peak_forecast.factor`), X2
review item 4 (counties outside the zone map), X3 review items 3 and 5 (fleet limits; prices only), X9 review item 6
(the dev-agreement angle), and the 861S parser change (`account_eia_series`).
