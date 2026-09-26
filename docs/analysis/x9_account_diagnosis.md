# X9 — Per-account diagnosis (`/accounts/[id]`) prototype (2026-09-26)

Script: `analysis/x9_account_diagnosis.py` (`uv run --group analysis python analysis/x9_account_diagnosis.py`, after
`x2_adjusted_queue.py` and `x3_four_cp.py`, whose outputs it reads). Logic: `basecast_pipelines/models/diagnosis.py`
(pure functions, no loaders), tested in `tests/models/test_diagnosis.py` (tiny synthetic frames, no DB). Outputs
(gitignored): `analysis/out/x9_diagnoses.md` (the three renders below), `x9_diagnosis_<ccn>.json` (the same three as
JSON, the shape proposed in §4), `x9_coverage.csv` (fact × type), `x9_gaps.csv` (account × gap), `x9_gap_summary.csv`.
As of **2026-09-26**. Every source is public: nothing here is simulated.

**Validation lock.** Same hold-out as Q3 / X5: the five partner accounts are matched by name and removed from the
universe **before** anything is scored or mapped. `diagnosis.assemble()` raises `KeyError` for an account outside the
scored universe, and the script checks that all five raise. Every number below covers the **107** other accounts (48
co-ops, 59 munis). No partner module or partner file is read.

**Reuse, not re-derivation.** Universe, signals, score and tiers come from `accounts.py` / `triggers.py` exactly as X5
runs them (0 of 107 ranks differ from `x5_ranking.csv`; next actions 25 call now / 12 nurture / 34 watch / 36 hold, as
in X5). EIA customers, sales, revenue and price come from `eia861_short_form.py` (long form + 861S from the lake +
delivery-only customers, as X4). The adjusted queue is X2's project scores rolled up to county × stratum (reproduces
`x2_county_adjusted.csv` exactly). The zone 4CP numbers are `four_cp.zone_coincidence()` (X3).

## 1. What the diagnosis holds

| Block | Content | Main sources |
|---|---|---|
| Header ("who they are") | type, CCN, G&T, EIA id, counties, territory km², EIA form, meters (bundled + delivery-only), meter growth 2019→2024, sales, revenue, average price and its trend, residential price, 2025 early-release price, ERCOT roles ever registered | `puct_ccn_territories`, crosswalk, `county_utility_overlap_puct`, `eia861_sales` + 861S, `ercot_market_participants` |
| Score ("why they rank here") | per signal: raw value, percentile, configured and used weight (renormalized when a signal is missing), contribution; contributions sum to the score | `config/account_score.yaml` (pending review) over census, TCEQ, LTLF |
| Triggers ("why now") | every event of the account, newest first: date, age, active flag, strength, title/detail, county and exposure or "by name", source table and reference, offer angle | the eight X5 triggers |
| Territory | population and growth, permits (2023–25 per 1k; last 12 months vs the 12 before), owner single-family homes and share, homes per meter, data-center sites, raw vs adjusted queue (all fuels and storage), weather zone mix, LTLF zone outlook, zone 2026 actual vs LTLF, Q7 model flag, zone 4CP load / CF / intensity / own-peak hour, account 4CP load (always empty: private) | census, TCEQ, X2, LTLF, D&E |
| EIA series | one row per year 2013–2024 + the 2025 early release: form, customers, delivery-only, meters, MWh, revenue, average and residential price | `eia861_sales` + 861S |
| Next action | action, the rule that fired in words, lead trigger (freshest active strong event), offer, talking points (context triggers + the zone's 4CP line), and **the date the action lapses** if nothing new happens | `triggers.next_action` |

Every scalar is a *fact*: `{key, label, value, unit, source, as_of, note}`; `value = null` is a gap. Four choices go
beyond X5:

- **Context counties.** County triggers need the account to cover ≥ 20% of the county. 55 of 59 munis cover no county
  that much, so for them the territory facts (sites, queue) use the **home county** (the one holding most of the
  territory) and say so in the note. Triggers keep X5's rule unchanged.
- **Meters include delivery-only customers** (EIA part C). Lubbock's −95% break disappears (X4's finding); sales,
  revenue and price stay bundled-only.
- **Action lapse date.** `action_changes_on()` replays the rule on the future dates where an event leaves the 12-month
  window or stops being fresh. Of the 25 call-now accounts, **1 lapses within 30 days and 5 within 90** without a new
  event. The #1 account is that one: New Braunfels Utilities' only strong trigger, a data-center permit from
  2025-10-22, ages out on **2026-10-22**, and NBU drops to nurture.
- **Context events collapse** in the render: one line per context trigger with its count (Big Country EC has 42 active
  transmission listings; 178 events ever).

## 2. Three diagnoses

Picked from the X5 dry run (non-partners only): **#1 overall** (New Braunfels Utilities, a muni), the **median
co-op** (Big Country EC, rank 54 of 107), and a **muni on the 861S short form** with a strong trigger (Boerne
Utilities, rank 22), so the three cover a long-form muni, a co-op and a short-form muni.

What each one tells the partnerships team in 30 seconds:

- **New Braunfels Utilities (call now, but only until 2026-10-22).** Rank 1 comes from growth (population +26.4%
  2020→2025, percentile 1.00; permits 39 per 1k). The call-now rests on one data-center permit (Cloudburst, Comal
  County) that expires in 26 days. Meters grew 44,382 → 60,463 (2019→2024) while the average price went from 7.31 to
  11.56 ¢/kWh (+9.6% a year). Comal's queue is 867 MW of storage, of which X2 expects 11 MW by Dec 2028. SCENT's own
  peak ends at 18:13 on average, after the 15:45–17:45 4CP window, so the offer must pick own peak or 4CP.
- **Big Country EC (median score, call now until 2027-05-26).** It ranks in the middle because the territory is not
  growing (population −0.4%, permits 1 per 1k: percentiles 0.04–0.05). The large-load and generation side is
  another story: 2.83 expected data-center sites (percentile 1.00), a new permit 8 days ago (CEMCO-348 CBP Sweetwater
  2, Fisher County, 93% of the county), four storage IAs in Fisher (240 MW). Its exposed counties hold 22.4 GW of
  raw queue; X2 expects 4.0 GW by Dec 2028. The WEST zone's 2026 summer peak (June–August) already runs 16.2% above
  LTLF 2025's 2026 value. The pitch is capacity, not rooftops.
- **Boerne Utilities (call now until 2027-02-12).** The lead trigger is a city-signed Ch. 380 with Buc-ee's (US$14.5M,
  2026-02-12), mapped by name. The data is thin in exactly the ways the gap table measures. It files the 861S since
  2020, so there is no residential price. Its all-class price rose 11.93 → 14.11 ¢/kWh in 2024 (+18%), then eased to
  12.90 in the 2025 early release. Its territory is 22.9 km², 1% of Kendall County, so area apportionment gives it
  710 people and 173 homes for 6,542 meters: its size percentile (0.36) is an artifact. Kendall's permits are up 43%
  in 12 months, but the permit-surge trigger needs ≥ 50 apportioned units and Boerne gets 7.6.

<!-- #1 of the X5 ranking (muni) -->
### New Braunfels Utilities (muni, CCN 30123)

**Call now** · score 0.83, rank 1 of 107, tier A · as of 2026-09-26 · all sources public (nothing simulated)

> **Rule:** Tier A (rank 1 of 107) + 1 active strong trigger (freshest 339 days old) → Call now. **Offer:** capacity: large-load pressure on the co-op's peak and 4CP; offer VPP capacity.
> **Holds until** 2026-10-22, then Nurture (if no new strong event).
> **Lead trigger:** CLOUDBURST DATA CENTERS (2025-10-22, `tceq_data_center_sites` RN112306667).
> - New transmission project listed in ERCOT TPIT touching the territory: grid constraint in the area; storage as a non-wires alternative
> - The account's G&T / TSP reported large-load requests (PUCT 58777 RFI): wholesale supplier faces large-load growth; capacity costs likely to rise
> - 4CP (SCENT, 2025): zone load at the CPs 92% of its own peak, 4CP intensity 1.02; a 15:45–17:45 CPT discharge covers the 4CP (X3: 63 of 64 CPs, 2010–2025), but the zone's own peak ends after 18:00: own-peak and 4CP discharges differ, the offer must say which it serves; no $/kW-yr rate in the repo (not verified)

**Who they are**

| Field | Value | Source | As of | Note |
|---|---|---|---|---|
| G&T / wholesale supplier | LCRA | `puct_ccn_territories` | 2026-06-29 | PUCT CCN layer date |
| EIA-861 utility id | 13418 | `config/utility_crosswalk.yaml` | 2026-09-26 | Q2 crosswalk; 5 rows pending review |
| Counties (largest overlap first) | Comal, Guadalupe | `county_utility_overlap_puct` | 2026-09-26 | 2 counties |
| Territory area in ERCOT counties | 420 km² | `county_utility_overlap_puct` | 2026-09-26 |  |
| EIA-861 form (latest year) | long | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Meters (all classes, incl. delivery-only) | 60,463 customers | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Meter growth 2019→2024 | +6.4% | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Retail sales | 1,745,413 MWh | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Retail revenue | US$ 201.7M | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Average price (all classes) | 11.56 ¢/kWh | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Price trend 2019→2024 | +9.6% | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Residential price | 13.18 ¢/kWh | `eia861_sales + 861S (lake)` | 2024-12-31 | long form only |
| Average price, early release | 10.69 ¢/kWh | `eia861_sales + 861S (lake)` | 2025-12-31 | early release, not final |
| ERCOT market roles registered (ever) | CRRAH, LSE, QSE, TDSP | `ercot_market_participants` | 2026-09-26 |  |

**Why they rank here** (contributions sum to the score)

| Signal | Raw | Percentile | Weight | Contribution | Source (as of) |
|---|---|---|---|---|---|
| Population growth 2020→2025 | 26.4% | 1.00 | 0.25 | 0.250 | `census_population_county` (2025-07-01) |
| Permits 2023–2025 per 1,000 residents | 38.96 units/1k | 0.92 | 0.15 | 0.139 | `census_permits_county` (2025-12-31) |
| Owner-occupied single-family homes | 13,567 homes | 0.65 | 0.20 | 0.130 | `census_housing_county` (2024-12-31) |
| New data-center sites since 2025 (expected) | 0.24 sites | 0.83 | 0.15 | 0.125 | `tceq_data_center_sites` (not recorded) |
| Owner-occupied single-family share | 70.0% | 0.96 | 0.10 | 0.096 | `census_housing_county` (2024-12-31) |
| Zone summer peak CAGR 2025→2031 (LTLF) | +10.8% | 0.59 | 0.15 | 0.088 | `ltlf_forecasts` (2025-10-06) |

**Why now** (10 active events in the last 12 months, 54 ever; newest first)

| Date | Age (d) | Trigger | Strength | Event | Where | Source |
|---|---|---|---|---|---|---|
| 2025-10-22 | 339 | dc_permit | strong | CLOUDBURST DATA CENTERS — matched by name | Comal (24%) | `tceq_data_center_sites` RN112306667 |
| 2026-07-13 | 75 | new_transmission | context | 8 events; latest: Highway 46 Bus Substation Upgrade — LCRATSC 138.0 kV, Conceptual | Comal | `tpit_projects` |
| 2026-04-15 | 164 | tsp_large_load | context | 1 event; latest: LCRA reported 4422.0 MW of large-load requests for 2030 — via G&T LCRA | by name | `puct_tsp_large_load_requests` |

**Territory**

| Field | Value | Source | As of | Note |
|---|---|---|---|---|
| Population (apportioned) | 57,092 people | `census_population_county` | 2025-07-01 | PEP vintage 2025 |
| Population growth 2020→2025 | 26.4% | `census_population_county` | 2025-07-01 | PEP vintage 2025 |
| Residential permits 2023–2025 per 1,000 residents | 38.96 units/1k | `census_permits_county` | 2025-12-31 | annual BPS 2023–2025, imputed |
| Permit units, last 12 months (apportioned) | 621 units | `census_permits_county` | 2026-08-01 |  |
| Permit units vs the 12 months before | -2.0% | `census_permits_county` | 2026-08-01 |  |
| Owner-occupied single-family homes (apportioned) | 13,567 homes | `census_housing_county` | 2024-12-31 | ACS 2024 5-year B25032 |
| Owner-occupied single-family share | 70.0% | `census_housing_county` | 2024-12-31 | ACS 2024 5-year B25032 |
| Apportioned homes per EIA meter | 0.22 | `census_housing_county` | 2024-12-31 | > 1 = area apportionment over-counts the territory (X5) |
| New data-center sites since 2025 (expected, by county share) | 0.24 sites | `tceq_data_center_sites` | not recorded | no snapshot date in the table; latest permit 2026-09-18 |
| New data-center sites since 2025 in the context counties | 1.00 sites | `tceq_data_center_sites` | not recorded | 1 exposed counties (share ≥ 20%) |
| Generation queue, raw (context counties) | 867 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | 1 exposed counties (share ≥ 20%) |
| Adjusted: expected COD by Dec 2027 (context counties) | 0.93 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Adjusted: expected COD by Dec 2028 (context counties) | 11.11 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Storage in the queue, raw (context counties) | 867 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Storage expected by Dec 2028 (context counties) | 11.11 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Primary weather zone (by area) | SCENT | `county_weather_zone` | 2026-09-26 |  |
| Zone summer peak outlook, LTLF 2025 CAGR 2025→2031 | +10.8% | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone summer peak 2031 (LTLF 2025, ERCOT-adjusted) | 28,109 MW | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone summer peak 2026 (15-min, actual) | 15,972 MW | `ercot_monthly_peaks` | 2026-08-01 | 3 of 4 months; not final-settled |
| Zone 2026 actual vs LTLF 2025 for 2026 | -6.4% | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone peak model hold-out MAPE (Q7, flagged zones only) | 10.1% | `docs/analysis/q7_organic_peak.md` | 2026-09-26 |  |
| Zone load at the 4CP (2025) | 13,478 MW | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Zone load at the 4CP ÷ its own summer peak | 0.92 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| 4CP intensity (4CP share ÷ energy share) | 1.02 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Zone's own monthly peaks end (2021–2025 mean, local) | 18:13 | `ercot_monthly_peaks` | 2026-08-01 | outside 16:00–18:00: own-peak and 4CP discharges differ (X3) |
| Account load at the 4CP | — | `UtilityDataSource` | not recorded | not public: needs the co-op's own meter data |

Counties: Comal 24% of county / 86% of territory (exposed); Guadalupe 3% of county / 14% of territory

Data-center sites since 2025 in its counties: CLOUDBURST DATA CENTERS (COMAL, 2025-10-22, name, exposed)

**EIA-861 series** (`eia861_sales` long form + 861S short form from the lake; final releases, early release marked)

| Year | Form | Meters | Sales (GWh) | Revenue (US$M) | Avg price (¢/kWh) | Residential (¢/kWh) |
|---|---|---|---|---|---|---|
| 2013 | long | 32,269 | 1,339.6 | 86.1 | 6.43 | 7.65 |
| 2014 | long | 33,775 | 1,480.0 | 103.7 | 7.01 | 8.39 |
| 2015 | long | 35,187 | 1,473.9 | 101.1 | 6.86 | 8.08 |
| 2016 | long | 36,761 | 1,489.6 | 100.1 | 6.72 | 8.28 |
| 2017 | long | 40,035 | 1,543.1 | 101.7 | 6.59 | 8.20 |
| 2018 | long | 42,535 | 1,631.1 | 118.7 | 7.28 | 8.92 |
| 2019 | long | 44,382 | 1,679.3 | 122.8 | 7.31 | 8.83 |
| 2020 | long | 46,924 | 1,709.6 | 138.3 | 8.09 | 9.75 |
| 2021 | long | 48,910 | 1,650.9 | 155.2 | 9.40 | 10.89 |
| 2022 | long | 52,503 | 1,771.1 | 208.9 | 11.80 | 13.55 |
| 2023 | long | 54,124 | 1,769.2 | 201.3 | 11.38 | 12.95 |
| 2024 | long | 60,463 | 1,745.4 | 201.7 | 11.56 | 13.18 |
| 2025 ER | long | 63,847 | 1,843.0 | 197.0 | 10.69 | 12.39 |

<!-- mid-ranked co-op (rank 54 of 107, the median) -->
### Big Country Electric Cooperative, Inc. (coop, CCN 30120)

**Call now** · score 0.50, rank 54 of 107, tier B · as of 2026-09-26 · all sources public (nothing simulated)

> **Rule:** Tier B (rank 54 of 107) + 3 active strong triggers (freshest 8 days old) → Call now. **Offer:** capacity: large-load pressure on the co-op's peak and 4CP; offer VPP capacity.
> **Holds until** 2027-05-26, then Nurture (if no new strong event).
> **Lead trigger:** CEMCO-348 CBP SWEETWATER 2 DATA CENTER (2026-09-18, `tceq_data_center_sites` RN112532585).
> - New transmission project listed in ERCOT TPIT touching the territory: grid constraint in the area; storage as a non-wires alternative
> - The account's G&T / TSP reported large-load requests (PUCT 58777 RFI): wholesale supplier faces large-load growth; capacity costs likely to rise
> - 4CP (WEST, 2025): zone load at the CPs 90% of its own peak, 4CP intensity 1.00; a 15:45–17:45 CPT discharge covers the 4CP (X3: 63 of 64 CPs, 2010–2025); no $/kW-yr rate in the repo (not verified)

**Who they are**

| Field | Value | Source | As of | Note |
|---|---|---|---|---|
| G&T / wholesale supplier | Golden Spread Electric Coop | `puct_ccn_territories` | 2026-06-29 | PUCT CCN layer date |
| EIA-861 utility id | 1591 | `config/utility_crosswalk.yaml` | 2026-09-26 | Q2 crosswalk; 5 rows pending review |
| Counties (largest overlap first) | Fisher, Scurry, Jones, Haskell | `county_utility_overlap_puct` | 2026-09-26 | 12 counties |
| Territory area in ERCOT counties | 11,745 km² | `county_utility_overlap_puct` | 2026-09-26 |  |
| EIA-861 form (latest year) | long | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Meters (all classes, incl. delivery-only) | 13,596 customers | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Meter growth 2019→2024 | +0.8% | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Retail sales | 354,958 MWh | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Retail revenue | US$ 38.1M | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Average price (all classes) | 10.74 ¢/kWh | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Price trend 2019→2024 | +1.4% | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Residential price | 13.72 ¢/kWh | `eia861_sales + 861S (lake)` | 2024-12-31 | long form only |
| Average price, early release | 10.49 ¢/kWh | `eia861_sales + 861S (lake)` | 2025-12-31 | early release, not final |
| ERCOT market roles registered (ever) | LSE, TDSP | `ercot_market_participants` | 2026-09-26 |  |

**Why they rank here** (contributions sum to the score)

| Signal | Raw | Percentile | Weight | Contribution | Source (as of) |
|---|---|---|---|---|---|
| New data-center sites since 2025 (expected) | 2.83 sites | 1.00 | 0.15 | 0.150 | `tceq_data_center_sites` (not recorded) |
| Zone summer peak CAGR 2025→2031 (LTLF) | +22.0% | 0.80 | 0.15 | 0.120 | `ltlf_forecasts` (2025-10-06) |
| Owner-occupied single-family homes | 9,845 homes | 0.58 | 0.20 | 0.117 | `census_housing_county` (2024-12-31) |
| Owner-occupied single-family share | 69.5% | 0.93 | 0.10 | 0.093 | `census_housing_county` (2024-12-31) |
| Population growth 2020→2025 | -0.4% | 0.04 | 0.25 | 0.009 | `census_population_county` (2025-07-01) |
| Permits 2023–2025 per 1,000 residents | 0.97 units/1k | 0.05 | 0.15 | 0.007 | `census_permits_county` (2025-12-31) |

**Why now** (52 active events in the last 12 months, 178 ever; newest first)

| Date | Age (d) | Trigger | Strength | Event | Where | Source |
|---|---|---|---|---|---|---|
| 2026-09-18 | 8 | dc_permit | strong | CEMCO-348 CBP SWEETWATER 2 DATA CENTER — matched by name | Fisher (93%) | `tceq_data_center_sites` RN112532585 |
| 2026-05-26 | 123 | dev_agreement | strong | Mesquite Creek Wind LLC — ch312 Active by Borden County, incentives $0.0M | Borden (33%) | `cpa_local_dev_agreements` ch312:000022696 |
| 2026-03-27 | 183 | gen_storage_ia | strong | Indigo Storage — storage 60.0 MW, status active | Fisher (93%) | `gis_project_events` 24INR0496 |
| 2026-03-27 | 183 | gen_storage_ia | strong | Indigo Storage 4 — storage 60.0 MW, status active | Fisher (93%) | `gis_project_events` 25INR0530 |
| 2026-03-27 | 183 | gen_storage_ia | strong | Indigo Storage 3 — storage 60.0 MW, status active | Fisher (93%) | `gis_project_events` 25INR0529 |
| 2026-03-27 | 183 | gen_storage_ia | strong | Indigo Storage 2 — storage 60.0 MW, status active | Fisher (93%) | `gis_project_events` 25INR0528 |
| 2026-02-05 | 233 | dc_permit | strong | JOURNEY SITE — matched by NAICS 518210 only | Haskell (74%) | `tceq_data_center_sites` RN112372271 |
| 2026-01-06 | 263 | dc_permit | strong | VANTAGE DATA CENTERS TX3 — matched by name | Shackelford (39%) | `tceq_data_center_sites` RN112347265 |
| 2025-12-01 | 299 | gen_storage_ia | strong | Crowded Star I BESS — storage 203.0 MW, status active | Jones (77%) | `gis_project_events` 25INR0473 |
| 2026-07-13 | 75 | new_transmission | context | 42 events; latest: Rebuild 22.8-mile 345 kV line from Farmland to Long Draw — ONCOR 345.0 kV, Conceptual | Borden, Fisher, Haskell, Jones, Scurry, Shackelford, Stonewall | `tpit_projects` |
| 2026-04-15 | 164 | tsp_large_load | context | 1 event; latest: Golden Spread reported 16580.0 MW of large-load requests for 2030 — via G&T Golden Spread Electric Coop | by name | `puct_tsp_large_load_requests` |

**Territory**

| Field | Value | Source | As of | Note |
|---|---|---|---|---|
| Population (apportioned) | 41,517 people | `census_population_county` | 2025-07-01 | PEP vintage 2025 |
| Population growth 2020→2025 | -0.4% | `census_population_county` | 2025-07-01 | PEP vintage 2025 |
| Residential permits 2023–2025 per 1,000 residents | 0.97 units/1k | `census_permits_county` | 2025-12-31 | annual BPS 2023–2025, imputed |
| Permit units, last 12 months (apportioned) | 6.12 units | `census_permits_county` | 2026-08-01 |  |
| Permit units vs the 12 months before | -53.8% | `census_permits_county` | 2026-08-01 |  |
| Owner-occupied single-family homes (apportioned) | 9,845 homes | `census_housing_county` | 2024-12-31 | ACS 2024 5-year B25032 |
| Owner-occupied single-family share | 69.5% | `census_housing_county` | 2024-12-31 | ACS 2024 5-year B25032 |
| Apportioned homes per EIA meter | 0.72 | `census_housing_county` | 2024-12-31 | > 1 = area apportionment over-counts the territory (X5) |
| New data-center sites since 2025 (expected, by county share) | 2.83 sites | `tceq_data_center_sites` | not recorded | no snapshot date in the table; latest permit 2026-09-18 |
| New data-center sites since 2025 in the context counties | 4.00 sites | `tceq_data_center_sites` | not recorded | 8 exposed counties (share ≥ 20%) |
| Generation queue, raw (context counties) | 22,372 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | 8 exposed counties (share ≥ 20%) |
| Adjusted: expected COD by Dec 2027 (context counties) | 2,160 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Adjusted: expected COD by Dec 2028 (context counties) | 4,036 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Storage in the queue, raw (context counties) | 6,256 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Storage expected by Dec 2028 (context counties) | 1,287 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Primary weather zone (by area) | WEST | `county_weather_zone` | 2026-09-26 | WEST 52%, NORTH 32%, NCENT 10%, FWEST 7% |
| Zone summer peak outlook, LTLF 2025 CAGR 2025→2031 | +23.1% | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone summer peak 2031 (LTLF 2025, ERCOT-adjusted) | 7,154 MW | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone summer peak 2026 (15-min, actual) | 2,893 MW | `ercot_monthly_peaks` | 2026-08-01 | 3 of 4 months; not final-settled |
| Zone 2026 actual vs LTLF 2025 for 2026 | 16.2% | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone peak model hold-out MAPE (Q7, flagged zones only) | — | `docs/analysis/q7_organic_peak.md` | 2026-09-26 |  |
| Zone load at the 4CP (2025) | 1,932 MW | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Zone load at the 4CP ÷ its own summer peak | 0.90 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| 4CP intensity (4CP share ÷ energy share) | 1.00 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Zone's own monthly peaks end (2021–2025 mean, local) | 16:57 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Account load at the 4CP | — | `UtilityDataSource` | not recorded | not public: needs the co-op's own meter data |

Counties: Fisher 93% of county / 19% of territory (exposed); Scurry 85% of county / 17% of territory (exposed); Jones 77% of county / 16% of territory (exposed); Haskell 74% of county / 15% of territory (exposed); Shackelford 39% of county / 8% of territory (exposed); Garza 38% of county / 7% of territory (exposed); +6 more

Data-center sites since 2025 in its counties: CEMCO-348 CBP SWEETWATER 2 DATA CENTER (FISHER, 2026-09-18, name, exposed); WOODROW DATA CENTER & POWER PLANT (NOLAN, 2026-05-18, name); JOURNEY SITE (HASKELL, 2026-02-05, NAICS 518210 only, exposed); VANTAGE DATA CENTERS TX3 (SHACKELFORD, 2026-01-06, name, exposed); BARBER LAKE DATA CENTER PROJECT (MITCHELL, 2025-08-27, name); THELMA SITE (HASKELL, 2025-08-15, NAICS 518210 only, exposed)

**EIA-861 series** (`eia861_sales` long form + 861S short form from the lake; final releases, early release marked)

| Year | Form | Meters | Sales (GWh) | Revenue (US$M) | Avg price (¢/kWh) | Residential (¢/kWh) |
|---|---|---|---|---|---|---|
| 2013 | long | 12,766 | 254.8 | 27.7 | 10.86 | 12.13 |
| 2014 | long | 12,856 | 260.9 | 30.6 | 11.74 | 13.10 |
| 2015 | long | 12,912 | 247.3 | 27.1 | 10.96 | 12.35 |
| 2016 | long | 12,810 | 234.3 | 25.4 | 10.85 | 12.60 |
| 2017 | long | 12,863 | 237.4 | 26.8 | 11.29 | 13.11 |
| 2018 | long | 12,909 | 251.2 | 26.6 | 10.59 | 12.17 |
| 2019 | long | 13,049 | 265.8 | 26.7 | 10.04 | 11.80 |
| 2020 | long | 13,128 | 255.5 | 29.2 | 11.43 | 12.79 |
| 2021 | long | 13,204 | 295.5 | 37.4 | 12.65 | 14.64 |
| 2022 | long | 13,484 | 363.3 | 47.8 | 13.16 | 15.87 |
| 2023 | long | 13,518 | 349.6 | 42.3 | 12.11 | 14.94 |
| 2024 | long | 13,596 | 355.0 | 38.1 | 10.74 | 13.72 |
| 2025 ER | long | 13,628 | 362.5 | 38.0 | 10.49 | 13.33 |

<!-- muni on the EIA-861 short form -->
### Boerne Utilities (muni, CCN 30012)

**Call now** · score 0.63, rank 22 of 107, tier A · as of 2026-09-26 · all sources public (nothing simulated)

> **Rule:** Tier A (rank 22 of 107) + 1 active strong trigger (freshest 226 days old) → Call now. **Offer:** new commercial/industrial load coming; offer peak shaving before it lands.
> **Holds until** 2027-02-12, then Nurture (if no new strong event).
> **Lead trigger:** Buc-ee's Ltd. (2026-02-12, `cpa_local_dev_agreements` ch380:0014685).
> - The account's G&T / TSP reported large-load requests (PUCT 58777 RFI): wholesale supplier faces large-load growth; capacity costs likely to rise
> - 4CP (SCENT, 2025): zone load at the CPs 92% of its own peak, 4CP intensity 1.02; a 15:45–17:45 CPT discharge covers the 4CP (X3: 63 of 64 CPs, 2010–2025), but the zone's own peak ends after 18:00: own-peak and 4CP discharges differ, the offer must say which it serves; no $/kW-yr rate in the repo (not verified)

**Who they are**

| Field | Value | Source | As of | Note |
|---|---|---|---|---|
| G&T / wholesale supplier | LCRA | `puct_ccn_territories` | 2026-06-29 | PUCT CCN layer date |
| EIA-861 utility id | 1913 | `config/utility_crosswalk.yaml` | 2026-09-26 | Q2 crosswalk; 5 rows pending review |
| Counties (largest overlap first) | Kendall | `county_utility_overlap_puct` | 2026-09-26 | 1 counties |
| Territory area in ERCOT counties | 22.87 km² | `county_utility_overlap_puct` | 2026-09-26 |  |
| EIA-861 form (latest year) | short | `eia861_sales + 861S (lake)` | 2024-12-31 | short form: totals only, no sector split |
| Meters (all classes, incl. delivery-only) | 6,542 customers | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Meter growth 2019→2024 | +3.0% | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Retail sales | 156,312 MWh | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Retail revenue | US$ 22.1M | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Average price (all classes) | 14.11 ¢/kWh | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Price trend 2019→2024 | +5.3% | `eia861_sales + 861S (lake)` | 2024-12-31 | 861S read from the raw zips, not a table yet (X4) |
| Residential price | — | `eia861_sales + 861S (lake)` | 2024-12-31 | long form only |
| Average price, early release | 12.90 ¢/kWh | `eia861_sales + 861S (lake)` | 2025-12-31 | early release, not final |
| ERCOT market roles registered (ever) | CRRAH, LSE, TDSP | `ercot_market_participants` | 2026-09-26 |  |

**Why they rank here** (contributions sum to the score)

| Signal | Raw | Percentile | Weight | Contribution | Source (as of) |
|---|---|---|---|---|---|
| Population growth 2020→2025 | 19.6% | 0.89 | 0.25 | 0.222 | `census_population_county` (2025-07-01) |
| Permits 2023–2025 per 1,000 residents | 22.91 units/1k | 0.74 | 0.15 | 0.111 | `census_permits_county` (2025-12-31) |
| Owner-occupied single-family share | 73.3% | 1.00 | 0.10 | 0.100 | `census_housing_county` (2024-12-31) |
| Owner-occupied single-family homes | 173 homes | 0.36 | 0.20 | 0.072 | `census_housing_county` (2024-12-31) |
| Zone summer peak CAGR 2025→2031 (LTLF) | +10.8% | 0.47 | 0.15 | 0.071 | `ltlf_forecasts` (2025-10-06) |
| New data-center sites since 2025 (expected) | 0.00 sites | 0.34 | 0.15 | 0.052 | `tceq_data_center_sites` (not recorded) |

**Why now** (2 active events in the last 12 months, 5 ever; newest first)

| Date | Age (d) | Trigger | Strength | Event | Where | Source |
|---|---|---|---|---|---|---|
| 2026-02-12 | 226 | dev_agreement | strong | Buc-ee's Ltd. — ch380 Active by Boerne, incentives $14.5M | Kendall (100%) | `cpa_local_dev_agreements` ch380:0014685 |
| 2026-04-15 | 164 | tsp_large_load | context | 1 event; latest: LCRA reported 4422.0 MW of large-load requests for 2030 — via G&T LCRA | by name | `puct_tsp_large_load_requests` |

**Territory**

| Field | Value | Source | As of | Note |
|---|---|---|---|---|
| Population (apportioned) | 710 people | `census_population_county` | 2025-07-01 | PEP vintage 2025 |
| Population growth 2020→2025 | 19.6% | `census_population_county` | 2025-07-01 | PEP vintage 2025 |
| Residential permits 2023–2025 per 1,000 residents | 22.91 units/1k | `census_permits_county` | 2025-12-31 | annual BPS 2023–2025, imputed |
| Permit units, last 12 months (apportioned) | 7.62 units | `census_permits_county` | 2026-08-01 |  |
| Permit units vs the 12 months before | 43.4% | `census_permits_county` | 2026-08-01 |  |
| Owner-occupied single-family homes (apportioned) | 173 homes | `census_housing_county` | 2024-12-31 | ACS 2024 5-year B25032 |
| Owner-occupied single-family share | 73.3% | `census_housing_county` | 2024-12-31 | ACS 2024 5-year B25032 |
| Apportioned homes per EIA meter | 0.03 | `census_housing_county` | 2024-12-31 | > 1 = area apportionment over-counts the territory (X5) |
| New data-center sites since 2025 (expected, by county share) | 0.00 sites | `tceq_data_center_sites` | not recorded | no snapshot date in the table; latest permit 2026-09-18 |
| New data-center sites since 2025 in the context counties | 0.00 sites | `tceq_data_center_sites` | not recorded | home county Kendall (the account covers 1% of it; no county ≥ 20%) |
| Generation queue, raw (context counties) | 101 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | home county Kendall (the account covers 1% of it; no county ≥ 20%) |
| Adjusted: expected COD by Dec 2027 (context counties) | 0.08 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Adjusted: expected COD by Dec 2028 (context counties) | 0.46 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Storage in the queue, raw (context counties) | 101 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Storage expected by Dec 2028 (context counties) | 0.46 MW | `gis_project_events → queue_adjusted (X2)` | 2026-08-01 | GIS report month; adjusted = expected MW reaching COD (model) |
| Primary weather zone (by area) | SCENT | `county_weather_zone` | 2026-09-26 |  |
| Zone summer peak outlook, LTLF 2025 CAGR 2025→2031 | +10.8% | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone summer peak 2031 (LTLF 2025, ERCOT-adjusted) | 28,109 MW | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone summer peak 2026 (15-min, actual) | 15,972 MW | `ercot_monthly_peaks` | 2026-08-01 | 3 of 4 months; not final-settled |
| Zone 2026 actual vs LTLF 2025 for 2026 | -6.4% | `ltlf_forecasts` | 2025-10-06 | LTLF 2025, ERCOT-adjusted, summer |
| Zone peak model hold-out MAPE (Q7, flagged zones only) | 10.1% | `docs/analysis/q7_organic_peak.md` | 2026-09-26 |  |
| Zone load at the 4CP (2025) | 13,478 MW | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Zone load at the 4CP ÷ its own summer peak | 0.92 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| 4CP intensity (4CP share ÷ energy share) | 1.02 | `ercot_monthly_peaks` | 2026-08-01 | D&E report; recent months not final-settled |
| Zone's own monthly peaks end (2021–2025 mean, local) | 18:13 | `ercot_monthly_peaks` | 2026-08-01 | outside 16:00–18:00: own-peak and 4CP discharges differ (X3) |
| Account load at the 4CP | — | `UtilityDataSource` | not recorded | not public: needs the co-op's own meter data |

Counties: Kendall 1% of county / 100% of territory

**EIA-861 series** (`eia861_sales` long form + 861S short form from the lake; final releases, early release marked)

| Year | Form | Meters | Sales (GWh) | Revenue (US$M) | Avg price (¢/kWh) | Residential (¢/kWh) |
|---|---|---|---|---|---|---|
| 2013 | long | 4,938 | 126.4 | 13.4 | 10.61 | 9.89 |
| 2014 | long | 5,056 | 132.1 | 15.0 | 11.33 | 10.52 |
| 2015 | long | 5,213 | 133.5 | 16.3 | 12.20 | 11.42 |
| 2016 | long | 5,305 | 133.5 | 14.9 | 11.19 | 10.45 |
| 2017 | long | 5,341 | 132.5 | 15.7 | 11.83 | 11.07 |
| 2018 | long | 5,435 | 142.1 | 16.9 | 11.90 | 11.12 |
| 2019 | long | 5,638 | 139.2 | 15.2 | 10.88 | 10.02 |
| 2020 | short | 5,957 | 141.3 | 14.3 | 10.11 | — |
| 2021 | short | 6,161 | 145.0 | 15.6 | 10.75 | — |
| 2022 | short | 6,381 | 156.6 | 17.6 | 11.24 | — |
| 2023 | short | 6,545 | 156.1 | 18.6 | 11.93 | — |
| 2024 | short | 6,542 | 156.3 | 22.1 | 14.11 | — |
| 2025 ER | short | 6,555 | 158.0 | 20.4 | 12.90 | — |

## 3. Data gaps

### 3.1 Gaps across the 107 non-partner accounts

"Accounts" = how many of the 107 have the gap (co-ops / munis). Stale = as-of older than 730 days (none: the oldest
as-of dates are EIA 2024, 634 days, and ACS 2024).

| Gap | Accounts (coop / muni) | What it breaks | Fix |
|---|---|---|---|
| **Account load at the 4CP** is not public | 107 (48 / 59) | the 4CP offer can only be stated at zone level ("the zone's load at the CPs is 92% of its peak"), not in the account's kW | `UtilityDataSource` (the co-op's own interval data); label **simulated** when it comes from the adapter |
| **No county ≥ 20%** (the X5 exposure rule) | 55 (0 / 55) | data-center, IA, county Ch. 312/380, permit-surge and transmission triggers can never fire; 55 of 59 munis only get name-mapped triggers | territory polygons × site points (TCEQ has no coordinates; GIS projects have county only), or a muni rule "the city is in the county" for context (done here for display only) |
| **Area apportionment misreads size** (homes per EIA meter < 0.25 or > 1) | 69 (16 / 53): 51 munis under-counted, 16 co-ops + 2 munis over-counted | population, homes and permit counts (and the `owner_sf_homes` signal) are wrong for city territories; permit-surge needs ≥ 50 apportioned units, so small-share munis never fire it (Boerne: Kendall +43%, 7.6 units) | X4's proposal: EIA meters as the size signal; ZCTA/block apportionment for the rest |
| **861S short form**: totals only | 47 (4 / 43) | no residential price or residential share; `rate_increase` sees only the 54 accounts with a long-form residential price in 2024 and 2025. An all-class rule would add 5 munis at ≥ +10% (Smithville +25.7%, Hempstead +24.0%, Robstown +13.8%, Lexington +12.5%, Weimar +10.4%; 2024 → 2025 early release) | load `Short_Form_<year>` in the `eia_861` parser (X4 spec); an all-class price trigger for short-form accounts |
| 861S is **read from the raw zips**, not a table | all | the API cannot serve the EIA block until the parser loads it | same parser change |
| `tceq_data_center_sites` has **no snapshot date** (and no MW, no coordinates) | 107 | site facts cannot carry an as-of; "latest permit 2026-09-18" is the best proxy | add `snapshot_date` (or `source_file` + `ingested_at`) to the parsed table |
| **No G&T** in the PUCT layer | 20 (1 / 19) | the wholesale-supplier line and the `tsp_large_load` context trigger are empty (CPS, Denton, Garland, Lubbock, Brownsville, College Station…) | who supplies each muni is not in the repo (not verified); ERCOT NOIE / load-zone map could give the load zone at least |
| **No load zone per account** | 107 | X3's load-zone 4CP intensity (LZ_LCRA 1.13, LZ_RAYBN 1.22) cannot be shown; the diagnosis uses the weather zone (8 values, coarse) | map accounts to load zones through `ercot_noie_load_map` (mapping aid, X5) |
| Missing from the **2025 early release** | 11 (4 / 7) | no 2025 price | wait for the final 2025 release |
| EIA **meter / price growth** missing | 4 munis (Bartlett, Hempstead, Robstown, Waelder) | no 2019 base or no 2024 row | none (source gap); Waelder has no EIA totals at all |
| **Permits** missing | 1–3 munis (Goldthwaite, Seymour) | counties with no permit-issuing place in BPS | none (source gap; Q3) |
| **Adjusted queue** is an analysis output | all | `x2_project_scores.csv`, not a table or mart | mart `gen_queue_adjusted` (county × stratum × horizon) from `queue_adjusted` |
| Zone peak 2026 is **June–August, not final-settled** | all | the "2026 actual vs LTLF" line moves when September lands | refresh with the D&E report; show "3 of 4 months" (done) |
| LTLF 2025 dated **2025-10-06** | all | the zone outlook is a year old; LTLF 2026 is not in `ltlf_forecasts` (not verified whether published) | load the next vintage; A3's forecast replaces it |
| EIA **as-of is the data year's end**, not the release date | all | "as of 2024-12-31" understates how fresh the file is | carry the release / download date from the lake manifest |
| **Dev-agreement offer angle** is fixed | Big Country: a Ch. 312 abatement for a wind farm (Mesquite Creek Wind) says "new commercial/industrial load coming" | the angle is wrong when the recipient is generation | pick the angle from the NAICS code (2211 = generation) |

### 3.2 Field coverage (share of the 107 with a value; fields at 100% for both types omitted)

| Field | All | Co-ops | Munis |
|---|---|---|---|
| G&T / wholesale supplier | 81.3% | 97.9% | 67.8% |
| Meters, sales, revenue, average price (2024) | 99.1% | 100% | 98.3% |
| Meter growth and price trend 2019→2024 | 96.3% | 100% | 93.2% |
| Residential price | **57.0%** | 93.8% | **27.1%** |
| Price, 2025 early release | 89.7% | 91.7% | 88.1% |
| Permits per 1k (2023–25) | 99.1% | 100% | 98.3% |
| Permit units, last 12 months / change | 98.1% / 97.2% | 100% | 96.6% / 94.9% |
| Homes per meter | 99.1% | 100% | 98.3% |
| Zone Q7 flag (by design: flagged zones only) | 47.7% | 33.3% | 59.3% |
| Account load at the 4CP | **0%** | 0% | 0% |

Everything else (population, growth, homes, data-center sites, queue raw and adjusted, weather zone, LTLF outlook, zone
2026 peak, zone 4CP numbers, ERCOT roles, counties) is present for all 107, but at county or zone resolution. The
real gap is **resolution, not coverage**: for most munis the territory block describes the county, not the city.

## 4. Proposed API shape (proposal only; the contract belongs to basecast-get-data)

Differences from the draft contract §5 (`docs/data-contract.md` in basecast-get-data):

- **Key:** `account_id` = PUCT `ccn_no` (string), not the EIA `utility_id`. The universe is PUCT's (112 ERCOT co-ops
  and munis). The EIA id is an attribute that comes from a crosswalk with 5 rows pending review, and plan B keeps
  accounts without one.
- **`forecast` and `deficit[]`** stay out until A3 exists. Today's diagnosis has zone-level facts only.
- **`is_base_partner`** should not be in the list until A2 reveals the partners (validation mode).

Same envelope as the contract (`meta.generated_at`, `meta.data_as_of`, `meta.simulated`, `meta.sources`, `data`). Types:
strings for ids and FIPS, floats for MW / shares / USD, ints for years and ranks, ISO dates.

### `GET /accounts` (list): one item per account (~107–112 rows, in memory)

| Field | Type | Note |
|---|---|---|
| `account_id` | string | PUCT `ccn_no` |
| `name`, `account_type` | string, `coop`\|`muni` | |
| `eia_utility_id` | string \| null | crosswalk |
| `gt` | string \| null | PUCT `gt_cooperative` |
| `primary_weather_zone` | string | largest overlap area |
| `meters` | float \| null | EIA latest final year, bundled + delivery-only |
| `score`, `rank`, `tier` | float, int, `A`\|`B`\|`C` | `rank` 1 = best |
| `signals` | object `{signal: {raw: float\|null, pct: float\|null}}` | six today |
| `next_action` | `call_now`\|`nurture`\|`watch`\|`hold` | |
| `action_changes_on` | date \| null | lapse date without new events |
| `n_strong`, `n_context` | int | active (12 months) |
| `latest_event_date` | date \| null | |
| `top_trigger` | object \| null `{trigger, title, event_date, age_days}` | freshest active strong event |
| `active_triggers` | string[] | chips |
| `flags` | string[] | `no_exposed_county`, `apportionment_under`, `apportionment_over`, `short_form`, `eia_break` |
| `simulated` | bool | false for everything public |

Query: `type`, `tier`, `next_action`, `trigger`, `zone`, `sort` (`rank` default), `q` (name). CSV export =
the same rows (`GET /accounts/export.csv`).

### `GET /accounts/{account_id}` (detail)

```text
data:
  account_id, as_of, simulated
  header: Fact[]                     # the header block of §1
  score: {score, rank, n_accounts, tier, method, weights_status: "pending_review",
          signals: [{signal, label, raw, unit, pct, weight, weight_used, contribution, source, as_of}]}
  next_action: {action, action_label, rule, lead_trigger: Event|null, offer, talking_points: string[],
                changes_on: date|null, changes_to: string|null}
  triggers: {active: Event[],                     # every active strong event + context events
             context_summary: [{trigger, count, latest_date, counties: string[]}],
             history_count: int}                  # full history paginated below
  territory: {facts: Fact[],
              counties: [{county_fips, county_name, overlap_km2, county_share, territory_share, weather_zone,
                          exposed: bool, context: bool}],
              context_rule: "exposed" | "home_county", context_label: string,
              zones: [{weather_zone, area_share}],
              data_centers: [{first_permit_date, name, tceq_rn, county_name, county_share, exposed, matched_by}],
              queue: [{stratum, projects_context, raw_mw_context, adj_mw_2027_context, adj_mw_2028_context,
                       raw_mw_apportioned, adj_mw_2027_apportioned, adj_mw_2028_apportioned}],
              zone_outlook: {zone, ltlf_start, ltlf_end, ltlf_cagr, ncp_now_mw, ncp_now_months, now_vs_ltlf,
                             cp_year, cp_avg_mw, cf_summer, share_4cp, share_energy, intensity, ncp_end_hour,
                             peak_mismatch: "early"|"late"|null, q7_holdout_mape}}
  eia_series: [{data_year, early_release, form, customers, delivery_customers, meters, sales_mwh, revenue_kusd,
                price_usd_kwh, res_price_usd_kwh}]
  gaps: [{key, kind: missing|stale|no_as_of|structural|quality|coverage, detail}]

Fact  = {key, label, value: number|string|null, unit|null, source, as_of: date|null, note|null}
Event = {event_date, age_days, active, trigger, label, strength, title, detail, county_fips|null, county_name|null,
         exposure, mapping: county|name, source, source_ref, offer}
```

`GET /accounts/{account_id}/events?since=&trigger=&page=` gives the full event history. Big Country has 178 events,
so the detail's inline JSON is 141 KB without this split. Samples: `analysis/out/x9_diagnosis_<ccn>.json` (inline
history, before the split).

**Marts behind it** (grain → rows): `accounts` (account, 107–112), `account_facts` (account × key, ~4.6k),
`account_events` (account × event, ~6k, X5), `account_counties` (account × county), `account_eia_series` (account ×
year × release, ~1.4k), `zone_outlook` (zone, 8), `gen_queue_adjusted` (county × stratum, < 800). All fit in memory
(Polars), as the architecture asks.

## Proposed decisions.md lines

- 2026-09-26 — The account diagnosis is assembled from the scored universe only; an account outside it (the held-out
  validation accounts) raises instead of rendering. — The validation lock must hold in the product code path too,
  not only in the analysis scripts.
- 2026-09-26 — Every displayed value in `/accounts/{id}` is a fact `{value, unit, source, as_of, note}`; `null` is a
  gap and the response lists its gaps. — The partnerships team must see where each number comes from and how old it
  is. The API can then report coverage (e.g. residential price 57%, munis 27%) instead of hiding it.
- 2026-09-26 — Accounts are keyed by PUCT `ccn_no` (`account_id`); the EIA `utility_id` is an attribute. — The
  universe is PUCT's; the EIA id comes from a crosswalk with rows pending review, and the draft contract's
  `utility_id` key cannot hold an account without one.
- 2026-09-26 — Territory facts for an account with no county ≥ 20% (55 of 59 munis) use its home county, labelled as
  such; triggers keep the ≥ 20% rule. — Otherwise the queue and site facts read 0 for almost every muni. The label
  keeps the county-level reading honest.
- 2026-09-26 — Meters = EIA bundled + delivery-only customers; sales, revenue and price stay bundled. — Keeps a
  utility that opens to retail choice (Lubbock) from showing a −95% break.
- 2026-09-26 — The next action carries the date it lapses without new events. — 5 of the 25 call-now accounts lapse
  within 90 days, including the #1 (2026-10-22); the list must show it.
- 2026-09-26 — Context triggers collapse to one line per trigger in the detail, and the full history moves to a
  paginated `/accounts/{id}/events`. — Transmission listings drown the strong events (Big Country: 42 active, 178
  ever).

## Review items for Pablo

1. **API key and contract.** Switch the draft contract's accounts key from `utility_id` to `ccn_no`, drop
   `forecast` / `deficit[]` until A3, and hold `is_base_partner` until A2? These are basecast-get-data changes. Nothing
   in that repo was touched.
2. **Muni resolution.** 55 of 59 munis never reach the 20% county rule and 51 are under-counted by area
   apportionment. The home-county context (done here, display only) is a stopgap. Options: (a) accept that munis get
   name-mapped triggers only, (b) a muni rule "the event is in the city's county" for data centers / IAs, or (c)
   ZCTA/point geometry. Default: (a) plus the labelled home-county facts.
3. **Short-form price trigger.** Add an all-class `rate_increase` for the 43 short-form munis? It would fire for 5 now.
   Default: yes, as context, once the 861S parser lands.
4. **Lapse date in the list.** NBU (#1) lapses to nurture on 2026-10-22. Show "call now until Oct 22" in `/accounts`
   and in the video, or does that undercut the #1? Default: show it.
5. **Own-peak vs 4CP.** The diagnosis flags zones whose own peak (2021–2025 mean) ends outside 16:00–18:00. SCENT is
   late (18:13), which covers every account whose primary zone is SCENT (NBU and Boerne among them). X3 only flagged
   early ones (FWEST, NORTH). Keep both directions? Default: yes.
6. **Dev-agreement angle.** Ch. 312 abatements for wind or solar farms fire `dev_agreement` with a load angle. Split by
   NAICS 2211 (generation → the `gen_storage_ia` angle)? Default: yes, a small `triggers.py` change, not made here.
7. **Numbers still pending elsewhere** show up in every diagnosis: the score weights (`account_score.yaml`, Q3 vs X4),
   the 5 crosswalk rows, the TCEQ `affil_begin_dt` meaning, the 4CP interval label and the missing $/kW-yr rate. The
   4CP line says "not verified".
