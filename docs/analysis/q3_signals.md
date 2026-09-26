# Q3 — Are the account signals any good? (2026-09-26)

Spec: `docs/PHASE0_ANALYSIS.md` §2 Q3. Script: `analysis/q3_signals.py`
(`uv run --group analysis python analysis/q3_signals.py`, after `q2_universe.py`). Logic:
`basecast_pipelines/models/accounts.py` (`build_signals()` and helpers), tested in `tests/models/test_accounts.py`.
Output: the proposed `config/account_score.yaml`, plus `analysis/out/q3_signals.csv` and `q3_signal_pairs.csv`
(gitignored). No figure: the pair table decides the redundancy.

**Validation lock.** The universe is Q2's 112 accounts. The five partner accounts were matched by name and removed
**before** any signal was computed. Every number below covers **107 accounts** (48 co-ops, 59 munis) and includes
nothing about the partners.

## Signals and how they reach an account

County numbers are apportioned by `county_share` from `county_utility_overlap_puct` (the share of the county's area
inside the territory). This assumes people and homes are spread evenly over the county's area. Rates and shares
come out fine. Counts come out biased low for dense city territories: the smallest muni gets 13 residents. PUCT
territories overlap (dual certification), so the same county population can count for a co-op and a muni.

| Signal | Source | Definition |
|---|---|---|
| `eia_customers`, `eia_sales_mwh` | `eia861_sales` 2024 final | Total customers and retail MWh, summed over parts and BAs |
| `eia_res_sales_share` | same | Residential MWh / total MWh |
| `eia_customer_cagr` | same, 2019 → 2024 | Customer CAGR |
| `population`, `pop_growth` | `census_population_county` PEP V2025 | Territory population 2025; growth 2020 → 2025 |
| `permit_units`, `permits_per_1k` | `census_permits_county` annual 2023–2025, imputed `units_total` | Units permitted; per 1,000 residents (2025) |
| `owner_sf_homes`, `owner_sf_share` | `census_housing_county` ACS 2024 5-year B25032 | Owner-occupied 1-unit homes (lines 3+4); share of occupied units |
| `dc_sites` | `tceq_data_center_sites`, first permit ≥ 2025-01-01 | Sites per county × `county_share` (expected sites in the territory) |
| `zone_peak_cagr` | `ltlf_forecasts` LTLF 2025, `ercot_adjusted`, summer `peak_mw`, weather zone | CAGR 2025 → 2031, weighted by overlap area; county zone from `county_weather_zone` |

## 1. Coverage and distribution (107 accounts)

| Signal | Coverage | Min | p10 | Median | p90 | Max | Zeros | Mode share | Passes |
|---|---|---|---|---|---|---|---|---|---|
| eia_customers | **57.0%** (61) | 5,614 | 9,970 | 23,920 | 95,519 | 962,272 | 0% | 1.6% | no: coverage |
| eia_sales_mwh | **57.0%** | 209,203 | 273,210 | 635,975 | 2,562,612 | 25,254,756 | 0% | 1.6% | no: coverage |
| eia_res_sales_share | **57.0%** | 0.044 | 0.174 | 0.488 | 0.731 | 0.881 | 0% | 1.6% | no: coverage |
| eia_customer_cagr | **57.0%** | −44.5% | 0.9% | 2.2% | 4.6% | 10.6% | 0% | 1.6% | no: coverage |
| population | 100% | 13 | 110 | 21,472 | 296,314 | 2,200,599 | 0% | 0.9% | yes |
| pop_growth | 100% | −3.5% | 0.5% | 6.4% | 20.2% | 26.4% | 0% | 2.8% | yes |
| permit_units | 99.1% (106) | 0 | 0.3 | 150 | 5,782 | 50,874 | 1.9% | 1.9% | yes |
| permits_per_1k | 99.1% | 0 | 1.7 | 11.0 | 36.2 | 54.4 | 1.9% | 2.8% | yes |
| owner_sf_homes | 100% | 3.5 | 23 | 5,340 | 55,706 | 430,409 | 0% | 0.9% | yes |
| owner_sf_share | 100% | 0.414 | 0.540 | 0.618 | 0.682 | 0.733 | 0% | 2.8% | yes |
| dc_sites | 100% | 0 | 0 | 0 | 0.75 | 2.83 | **69.2%** | 69.2% | yes |
| zone_peak_cagr | 100% | 2.4% | 4.2% | 10.7% | 23.1% | 26.3% | 0% | 26.2% | yes |

- **Near-constant** means the most common value covers ≥ 90% of the accounts that have one (Claude's threshold; the
  spec does not set one). `dc_sites` is 69% zeros, so it passes, but it acts as a bonus, not a ranking.
  `zone_peak_cagr` has only 8 distinct zone values.
- **By type**, the EIA signals cover 93.8% of co-ops but **27.1% of munis**. The public signals cover 98–100% of both.
- **Projection with the short form.** Filling EIA totals from the raw `Short_Form_2024.xlsx` (not in the database)
  lifts `eia_customers` and `eia_sales_mwh` to **99.1%** coverage (106/107; City of Waelder's 861S row is blank).
  The 861S has totals only, so `eia_res_sales_share` cannot pass from EIA-861. `eia_customer_cagr` with the short
  form was not computed (not verified).
- Co-ops vs munis, medians: pop_growth 4.7% vs 7.0%; owner_sf_share 0.61 vs 0.63; accounts with a new data-center
  site in one of their counties: **52% of co-ops vs 14% of munis**.

## 2. EIA-861 history (universe utilities, final releases)

| Final years with customers (2013–2024) | Utilities |
|---|---|
| 12 | 60 |
| 8 | 1 |
| 7 | 7 |
| 1 (2019 only) | 35 |
| none | 4 |

- The 35 single-year utilities are short-form munis. 2019 is the only year their rows sit in `Sales_Ult_Cust`.
- **Early release:** the 2025 rows (`early_release = true`, 54 universe utilities) are left out of every signal.
- **Year-over-year jumps beyond ±50%** (3 utilities, 4 flags):

| Utility | Measure | Years | Before → after | Change | Read |
|---|---|---|---|---|---|
| Lubbock Power & Light | customers | 2023 → 2024 | 110,848 → 5,614 | −94.9% | Fits Lubbock's move to retail choice in 2024 (not verified). Its wires customers would then be reported as delivery-only service, which sits in `Delivery_Companies_<year>.xlsx` (in the zip, not parsed). Its 2019→2024 CAGR (the −44.5% minimum) is unusable. |
| Lubbock Power & Light | sales | 2023 → 2024 | 2.53 → 0.59 TWh | −76.7% | same |
| Lighthouse Electric Cooperative | sales | 2022 → 2023 | 261 → 440 GWh | +68.9% | Customers did not move beyond ±50%; cause not verified |
| Jackson Electric Cooperative | sales | 2013 → 2014 | 305 → 561 GWh | +84.0% | Outside the CAGR window |

Only Lubbock falls inside the 2019 → 2024 CAGR window.

## 3. Building permits

- **No report:** 31 of 254 Texas counties have no BPS row in 2023–2025 (no permit-issuing place). Of the universe's
  219 counties, 28 have none. One account (a muni whose only county has no permit-issuing place) gets null.
- **Imputed vs reported (`_rep`), Texas 2023–2025:** 50,507 of 668,346 units are imputed (**7.6%**). Per county,
  the imputed share has median 3.1%, p90 64.6% and max 100%; 6 counties are fully imputed. Per account (apportioned),
  the median is 6.9%, p90 54.2% and max 100%. The signal uses the imputed series, so it covers places that did not
  report.
- **Caveat:** BPS counts permits from permit-issuing places, mostly cities. As far as we know, Texas counties issue
  few residential permits in unincorporated areas (not verified). The rural growth of a co-op territory is
  therefore under-read, and `pop_growth` carries more of it.

## 4. Redundancy (Spearman, on the accounts that have both)

| Pair | ρ | n | Action |
|---|---|---|---|
| population × owner_sf_homes | **0.998** | 107 | keep owner_sf_homes |
| population × permit_units | **0.959** | 106 | drop both population and permit_units |
| permit_units × owner_sf_homes | **0.958** | 106 | (same size cluster) |
| eia_customers × eia_sales_mwh | **0.881** | 61 | keep eia_customers (when it enters) |
| eia_customer_cagr × permit_units | 0.769 | 61 | — |
| pop_growth × permits_per_1k | 0.731 | 106 | below 0.8: both stay |
| eia_customer_cagr × pop_growth | 0.684 | 61 | — |

- With the short form projected, eia_customers × owner_sf_homes = 0.877. That makes them the same size signal, so
  eia_customers should replace owner_sf_homes, not add to it.
- Among the six weighted signals, the highest |ρ| is 0.73 (pop_growth × permits_per_1k), then 0.52
  (owner_sf_homes × dc_sites).
- Spearman is used because the size signals are heavily skewed.

## Decision

- **Enter the score** (coverage ≥ 80%, not near-constant, one per redundant cluster): `pop_growth`,
  `permits_per_1k`, `owner_sf_homes`, `owner_sf_share`, `dc_sites`, `zone_peak_cagr`.
- **Out:** all four EIA-861 signals (coverage 57%). Also `population` and `permit_units`, which pass the rule but are
  redundant with `owner_sf_homes`.
- **Conditional:** once `eia861_sales` loads the 861S short form, `eia_customers` (projected 99%) replaces
  `owner_sf_homes` as the size signal, and `eia_customer_cagr` is re-checked.
- **Weights (proposed, pending Pablo)** in `config/account_score.yaml`: a weighted mean of percentile ranks within
  the universe.

| Signal | Weight | Why (one line) |
|---|---|---|
| pop_growth | 0.25 | Residents added are the load and peak growth the account must buy capacity for, and new homes for Base |
| owner_sf_homes | 0.20 | Addressable market for a home battery; the one size signal kept |
| permits_per_1k | 0.15 | Forward pipeline of new homes; adds to pop_growth (ρ 0.73), does not repeat it |
| dc_sites | 0.15 | Large-load pressure near the territory (a lower bound, no MW), a bonus for exposed accounts |
| zone_peak_cagr | 0.15 | ERCOT's own zonal peak growth forecast; a placeholder until A3, coarse (8 values) |
| owner_sf_share | 0.10 | How much of the territory fits the product (single-family owners vs. apartments) |

The weights were fixed without seeing where the partners fall. The script checks the YAML: every weighted signal
passes, no weighted pair is redundant, and the weights sum to 1.

## Proposed findings row

| Q3 | 107 accounts analysed (5 partners held out); kept: pop_growth .25, owner_sf_homes .20, permits_per_1k .15, dc_sites .15, zone_peak_cagr .15, owner_sf_share .10; dropped: 4 EIA-861 signals (coverage 57% < 80%; 99% for customers once the 861S short form is parsed), population and permit_units (ρ ≥ 0.96 with owner_sf_homes); EIA jumps > ±50%: 3 utilities (Lubbock 2024 −95% customers); permits 7.6% imputed, 31/254 counties without report | score on 6 public signals, weights proposed (pending Pablo); eia_customers replaces owner_sf_homes after the 861S fix | A2 | — |

## Review items for Pablo

1. **[PERGUNTAR] Approve the weights** in `config/account_score.yaml` (table above). Once approved, commit the file
   **before A2** reveals the partners.
2. **Near-constant threshold** = mode ≥ 90% of values, chosen by Claude. With it, `dc_sites` (69% zeros) enters. A
   stricter 60% threshold would take it out.
3. **Size bias against munis.** Area apportionment gives city territories too few homes, so munis rank low on
   `owner_sf_homes`. Options: (a) accept it until `eia_customers` lands (default); (b) rank within type (co-op vs.
   muni); (c) apportion by ZCTA or block population later.
4. **Parser follow-ups (outside this task).** Load `Short_Form_<year>.xlsx` (861S) into `eia861_sales`; this is what
   flips the EIA size signal to 99%. Optionally load `Delivery_Companies_<year>.xlsx` for utilities under retail
   choice (Lubbock).
5. **Lubbock P&L:** keep it in the universe, but flag its EIA series as broken from 2024 in the diagnosis.
6. **Hold-out:** Q3 statistics are on 107 accounts, without the partners. Confirm that this is how you want the
   validation lock read.

## Proposed decisions.md lines

- 2026-09-26 — Account score (proposed, pending Pablo's approval): weighted mean of within-universe percentile ranks
  of pop_growth 0.25, owner_sf_homes 0.20, permits_per_1k 0.15, dc_sites 0.15, zone_peak_cagr 0.15, owner_sf_share
  0.10 (`config/account_score.yaml`). — Q3: the only signals with ≥ 80% coverage that are not near-constant, one per
  redundant cluster (population, permit_units and owner_sf_homes have ρ ≥ 0.96); fixed with the five partners held
  out.
- 2026-09-26 — EIA-861 signals stay out of the account score until `eia861_sales` carries the 861S short form; then
  eia_customers replaces owner_sf_homes. — They cover 57% of the universe (27% of munis) today and 99% with the short
  form; eia_customers × owner_sf_homes ρ = 0.88.
- 2026-09-26 — Account signals take county data through `county_share` (area) apportionment, EIA-861 final releases
  only (the 2025 early release is flagged and left out), BPS imputed units, PEP vintage 2025 and ACS 2024 5-year.
  — Reproducible and covers every account; the bias against dense city territories is documented in
  `docs/analysis/q3_signals.md`.

## Useful for the core features

- **Diagnosis chips:** each weighted signal is a sentence in the per-account diagnosis, with its percentile. For
  example: "population +X% since 2020 (top decile)", "N new data-center permits in its counties since 2025", "zone
  peak forecast +Y%/yr".
- **Triggers:** `dc_sites > 0` (a new TCEQ data-center permit in a territory county) and a top-quintile `pop_growth`
  or `permits_per_1k` are natural rule-based triggers for the "next action". 52% of co-ops vs 14% of munis have the
  data-center trigger.
- **Ranking:** `config/account_score.yaml` is the single source for the /accounts order. The UI can show "how the
  score is built" straight from it, and later let Pablo move the weights.
- **Data-quality badges:** EIA year-over-year jumps (Lubbock) and imputed permit shares above 50% can show as small
  warnings on the account page, which supports the "robust pipeline" story.
