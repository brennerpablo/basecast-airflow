# X5 — Account triggers ("why now") and a dry run of the account ranking (2026-09-26)

Script: `analysis/x5_triggers.py` (`uv run --group analysis python analysis/x5_triggers.py`, after `q2_universe.py`).
Logic: `basecast_pipelines/models/triggers.py`, tested in `tests/models/test_triggers.py`. Outputs (gitignored):
`analysis/out/x5_trigger_events.csv` (one row per account × event), `x5_trigger_coverage.csv`, `x5_ranking.csv`,
`x5_sensitivity.csv`, `x5_apportionment_check.csv`. As of **2026-09-26**; "active" = event dated in the last 365 days.

**Validation lock.** Same hold-out as Q3: the five partner accounts are matched by name and removed from the
universe **before** any event is mapped or any score computed. Every number, ranking and example below covers the
**107** other accounts (48 co-ops, 59 munis). The ranking never shows where the partners would fall.

## 1. Trigger inventory

Mapping **by county** = the event's county reaches accounts whose territory covers ≥ 20% of that county's land
(`county_share` in `county_utility_overlap_puct`; by county, not by point, as in Q3/Q4). Mapping **by name** = the
event names the account itself. Coverage = accounts of the 107 with ≥ 1 event (ever, i.e. dated ≤ as-of / active).

| Source | Event → date | Mapping | Accounts ever / active (co-op + muni) | Recency | Noise | Verdict |
|---|---|---|---|---|---|---|
| `tceq_data_center_sites` | new data-center air permit → first `affil_begin_dt` | county | 23 / **14** (13 + 1) | latest 2026-09-18; median age 143 d | lower bound; 23 of Q4's 38 sites matched only by NAICS 518210 | **trigger** (strong) |
| `gis_project_events` | storage or gas project ≥ 50 MW signs its IA → `ia_signed` | county | 48 / **31** (30 + 1) | latest 2026-08-18; median 270 d | all fuels would give 36 active; 59 active IAs | **trigger** (strong) |
| `cpa_local_dev_agreements` | Ch. 312 abatement (`executed_date`) or Ch. 380 ≥ US$1M (`effective_date`) | county (Ch. 312, county-signed 380); name (city-signed 380 → that city's muni) | 60 / **18** (13 + 5) | latest 2026-08-12; median 218 d | retail/hotel deals pass the US$1M bar; Ch. 312 values mostly reported as 0; Ch. 380 effective dates can be future (not fired) | **trigger** (strong) |
| `ercot_market_participants` | account registers an ERCOT role (LSE/QSE/TDSP/RE…) → `sfa_effective_date` | name (core-name match ≥ 95, within type) | 99 / **2** (1 + 1) | 2026-05-22, 2026-02-04 | 99 accounts have *some* old registration; only new ones fire | **trigger** (strong, rare) |
| `census_permits_county` monthly | residential permit units, last 12 months vs the 12 before ≥ +25% (≥ 50 units) → latest month (2026-08) | county, apportioned by `county_share` | 4 / **4** (2 + 2) | 2026-08-01 | BPS covers permit-issuing places only (rural growth under-read, Q3) | **trigger** (strong) |
| `tpit_projects` | transmission project ≥ 138 kV first listed in a TPIT snapshot → that snapshot date | county (either endpoint) | 50 / **48** (44 + 4) | latest 2026-07-13; median 117 d | 1,022 projects first listed statewide in 12 months (302 in the 2025-10 snapshot alone; a format change is not ruled out, not verified) | **context** |
| `eia861_sales` | residential average price (revenue / MWh) up ≥ 10%, 2024 final → 2025 early release → period end 2025-12-31 | name (EIA id from the crosswalk) | 7 / **7** (6 + 1) | 2025 data year | long form only (57% of accounts; 861S handled by X4); release date not in the table | **context** |
| `puct_tsp_large_load_requests` | the account's G&T (or the account as TSP) reported ≥ 1 GW of large-load requests for 2030 → filed 2026-04-15 | name (`gt_cooperative` → RFI TSP; CPS Energy as its own TSP) | 73 / **73** (32 + 41) | one filing | same event for every member of Brazos (26.6 GW), Golden Spread (16.6 GW), LCRA (4.4 GW), Rayburn (1.8 GW), CPS (1.1 GW); STEC (0.8 GW) below the bar | **context** |
| `tceq_air_permits` (all AIRNSR) | any new permitted site → first `affil_begin_dt` | county | not mapped | 2,086 new sites in 2025 | 12-month mix: rail 984 (NAICS null), oil & gas 655, ready-mix 108, quarries 101; electric generation (2211) 54 | later: an "on-site generation" trigger from NAICS 2211 |
| `cpa_data_centers` | qualifying data-center sales-tax exemption → `effective_date` | none | — | 76 of 167 effective in the last 12 months | only 7 of 167 have a county (name hint) | not now: match names to TCEQ sites first (e.g. "Thelma Data Center" ↔ TCEQ "THELMA SITE", not verified) |
| `cpa_jeti` | JETI application | none | — | no date column | 21 rows, no county (school district only) | not now: needs a district → county crosswalk |
| `puct_filing_documents` | the account files in large-load dockets 58481 / 58777 / 59772 → `filed_date` | name (`filing_party`) | 1 (CPS Energy) | 2025-07-31 → 2026-09-18 | TEC and TPPA file for all co-ops / munis (22 docs, last 2026-06-01): no discrimination | not a trigger; a diagnosis line for CPS |
| `ercot_members` | yearly membership by segment | name | — | yearly | 57 co-op + muni segment members in 2026; changes are rare | not a trigger |
| `ercot_noie_load_map` | none (no dates) | — | — | — | — | mapping aid only (NOIE load zone per physical load) |
| `bls_qcew_county` | NAICS 518210 employment change | county | — | annual, 2025 latest | 118 counties with a row in 2025, only 28 disclosed | not a trigger |
| `eaglei_*` | outages | — | — | — | only coverage / MCC side files are loaded (no outage events); the national files (11.6 GB) wait for Pablo; outages are out of the MVP | candidate only |

Threshold sensitivity of the county mapping (active accounts at `county_share` ≥ 5% / 10% / **20%** / 30% / 50%):
data centers 19 / 17 / **14** / 11 / 9; storage+gas IAs 37 / 35 / **31** / 29 / 23; development agreements (county part)
18 / 15 / **13** / 12 / 5; transmission 57 / 54 / **48** / 47 / 44.

**Overall:** 96 of 107 accounts have ≥ 1 active trigger of any kind; **46 have ≥ 1 active strong trigger**.

## 2. Proposed triggers (rules)

Strong triggers drive the next action; context triggers only feed the talking points.

| Id | Rule | Strength | Offer angle |
|---|---|---|---|
| `dc_permit` | A TCEQ data-center site (`tceq_data_center_sites`) whose first permit is in the last 12 months, in a county where the account covers ≥ 20% of the land | strong | Large-load pressure on the account's peak and 4CP → VPP capacity |
| `gen_storage_ia` | A storage or gas project ≥ 50 MW in the GIS queue signs its interconnection agreement in the last 12 months, same county rule | strong | Capacity is being built around the account by merchants → member-owned distributed storage |
| `dev_agreement` | A Ch. 312 abatement executed, or a county-signed Ch. 380 ≥ US$1M effective, in the last 12 months (same county rule); or a city-signed Ch. 380 ≥ US$1M of the muni's own city | strong | New commercial/industrial load → peak shaving before it lands |
| `market_registration` | The account registers a new ERCOT market role (LSE, QSE, TDSP, RE…) in the last 12 months (core-name match ≥ 95) | strong | Its wholesale set-up is changing → QSE-ready VPP |
| `permit_surge` | Apportioned residential permit units in the last 12 months ≥ 1.25 × the 12 months before, and ≥ 50 units | strong | New homes → batteries at construction, builder deals |
| `new_transmission` | A ≥ 138 kV project first listed in TPIT in the last 12 months with an endpoint in a ≥ 20% county | context | Grid constraint → storage as a non-wires alternative |
| `rate_increase` | Residential average price up ≥ 10% year over year (EIA-861, latest vs prior year) | context | Bill pressure → demand-charge / 4CP savings |
| `tsp_large_load` | The account's G&T (or the account as TSP) reported ≥ 1 GW of large-load requests for 2030 in the PUCT 58777 RFI | context | Wholesale supplier faces large-load growth → capacity costs likely rise |

Active examples (non-partner): Big Country EC ← CEMCO-348 CBP Sweetwater 2 Data Center (2026-09-18); Medina EC ←
SAT14 (2026-08-12); CPS Energy ← Riverwalk BESS 120 MW IA (2026-08-18); Navarro County EC ← Owens Corning Ch. 312
(2026-08-12); Sam Houston EC registered as LSE and TDSP (2026-05-22); City of Caldwell registered as LSE (2026-02-04);
Taylor EC permits +120%, BTU and College Station +76%, Navasota Valley EC +51% (12 months to 2026-08); Tri-County EC
residential price +19.1% (2024 → 2025).

## 3. Next action

Tier from the score rank: **A** = top quartile (27), **B** = second quartile (27), **C** = bottom half (53).

| Tier | ≥ 2 strong, or 1 strong ≤ 90 days old | 1 strong, older | No strong trigger |
|---|---|---|---|
| A | call now | call now | nurture |
| B | call now | nurture | watch |
| C | watch (alert) | watch (alert) | hold |

Result on the 107: **call now 25** (A 19, B 6), nurture 12 (A 8, B 4), watch 34 (B 17, C 17), hold 36. Demoting
`gen_storage_ia` to context gives 17 call-now; demoting it and `dev_agreement` gives 11.

## 4. Ranking dry run (107 accounts, proposed weights)

Score = weighted mean of within-universe percentile ranks ((average rank − 1) / (n − 1)), weights renormalized when a
signal is missing (`config/account_score.yaml`, unchanged). Columns are percentile ranks.

| # | Account | Type | Score | pop gr. | homes | permits/1k | dc | zone | sf share | Active triggers | Action |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | New Braunfels Utilities | muni | 0.83 | 1.00 | 0.65 | 0.92 | 0.83 | 0.59 | 0.96 | dc_permit, new_transmission, tsp_large_load | call now |
| 2 | Fannin County EC | coop | 0.82 | 0.83 | 0.82 | 0.89 | 0.73 | 0.92 | 0.73 | new_transmission, tsp_large_load | nurture |
| 3 | Pedernales EC | coop | 0.81 | 0.79 | 0.99 | 0.91 | 0.95 | 0.75 | 0.17 | dc_permit, dev_agreement, gen_storage_ia, new_transmission, tsp_large_load | call now |
| 4 | Grayson-Collin EC | coop | 0.77 | 0.86 | 0.97 | 0.95 | 0.34 | 0.78 | 0.54 | gen_storage_ia, new_transmission, tsp_large_load | call now |
| 5 | Tri-County EC | coop | 0.75 | 0.64 | 0.98 | 0.68 | 0.89 | 0.75 | 0.45 | dc_permit, gen_storage_ia, new_transmission, rate_increase, tsp_large_load | call now |
| 6 | Georgetown Utility Systems | muni | 0.73 | 0.96 | 0.54 | 0.81 | 0.75 | 0.47 | 0.75 | dev_agreement, tsp_large_load | call now |
| 7 | Navarro County EC | coop | 0.70 | 0.82 | 0.81 | 0.85 | 0.79 | 0.12 | 0.68 | dev_agreement, gen_storage_ia, new_transmission, tsp_large_load | call now |
| 8 | Bartlett EC | coop | 0.70 | 0.73 | 0.88 | 0.78 | 0.99 | 0.32 | 0.28 | dc_permit, dev_agreement, gen_storage_ia, new_transmission, tsp_large_load | call now |
| 9 | United EC Services | coop | 0.69 | 0.74 | 0.93 | 0.71 | 0.90 | 0.21 | 0.49 | gen_storage_ia, new_transmission, tsp_large_load | call now |
| 10 | Sam Houston EC | coop | 0.68 | 0.75 | 0.91 | 0.90 | 0.86 | 0.10 | 0.31 | market_registration, rate_increase | call now |
| 11 | Bluebonnet EC | coop | 0.68 | 0.71 | 0.95 | 0.88 | 0.78 | 0.35 | 0.09 | dev_agreement, gen_storage_ia, new_transmission, tsp_large_load | call now |
| 12 | South Plains EC | coop | 0.67 | 0.41 | 0.92 | 0.72 | 0.87 | 0.94 | 0.08 | dc_permit, dev_agreement, gen_storage_ia, new_transmission, tsp_large_load | call now |
| 13 | Trinity Valley EC | coop | 0.67 | 0.91 | 0.94 | 0.55 | 0.76 | 0.08 | 0.46 | gen_storage_ia, new_transmission, tsp_large_load | call now |
| 14 | Wise EC | coop | 0.67 | 0.85 | 0.72 | 0.59 | 0.34 | 0.64 | 0.74 | tsp_large_load | nurture |
| 15 | CPS Energy | muni | 0.66 | 0.57 | 1.00 | 0.52 | 0.84 | 0.62 | 0.16 | gen_storage_ia, new_transmission, tsp_large_load | call now |
| … | | | | | | | | | | | |
| 103 | Brady Water & Light Works | muni | 0.26 | 0.03 | 0.14 | 0.06 | 0.34 | 0.84 | 0.43 | tsp_large_load | hold |
| 104 | Jasper-Newton EC | coop | 0.26 | 0.08 | 0.61 | 0.26 | 0.34 | 0.11 | 0.13 | — | hold |
| 105 | Deep East Texas EC | coop | 0.25 | 0.15 | 0.69 | 0.04 | 0.34 | 0.06 | 0.05 | gen_storage_ia, new_transmission | watch |
| 106 | Gonzales Electric System | muni | 0.25 | 0.23 | 0.09 | 0.17 | 0.34 | 0.59 | 0.03 | tsp_large_load | hold |
| 107 | Waelder Electric Department | muni | 0.20 | 0.23 | 0.04 | 0.16 | 0.34 | 0.37 | 0.04 | tsp_large_load | hold |

(`dc` = 0.34 is the tied rank of the 69% zeros.)

### Face validity

- **The top makes sense.** 12 co-ops and 3 munis. By apportioned population, 14 of the top 15 sit on the growth rings
  of Austin (Pedernales: Travis/Williamson; Georgetown; Bluebonnet: Bastrop/Travis; Bartlett: Bell/Williamson),
  San Antonio–New Braunfels (NBU: Comal/Guadalupe; CPS: Bexar/Comal), DFW (Fannin County, Grayson-Collin, Tri-County:
  Parker/Tarrant, Navarro: Ellis, United: Hood/Johnson, Trinity Valley: Kaufman, Wise: Denton/Wise) and Houston (Sam
  Houston: Montgomery/Liberty). South Plains EC (Lubbock/Hale) is the one outside, lifted by its weather zone's forecast
  peak growth (NORTH zone for Lubbock; zone rank 0.94) and a data-center site. The bottom 5 are small, slow-growing territories (Deep East Texas, Jasper-Newton,
  Gonzales, Brady, Waelder).
- **No tiny-muni artifact at the top.** The smallest top-15 accounts are Georgetown (6.1k apportioned homes; 33.1k
  EIA meters) and NBU (13.6k; 60.5k meters): area apportionment *under*-counts munis, as Q3 said (59 munis: 9 in A,
  16 in B, 34 in C).
- **The artifact runs the other way: co-ops on the edge of a metro county.** Apportioned owner-occupied single-family
  homes exceed the account's EIA-861 2024 meters (all classes) for 17 of the 61 accounts with meters (median ratio
  0.72). In the top 15: **Fannin County EC 2.63** (12,172 meters; 74% of its apportioned population comes from Collin
  County), **Bartlett EC 3.33** (15,253 meters), Navarro 1.54, Grayson-Collin 1.28, Tri-County 1.08. Capping homes at
  meters barely moves the ranking (Spearman 0.999, same top 15; Bartlett 8 → 14), because the same apportionment also
  lifts their growth rates, which the cap does not touch. Fannin County EC at #2 is the clearest case to review.

## 5. Sensitivity

| Variant | Spearman vs base | Top 15 kept | Largest rank move | Next actions changed (of 107) |
|---|---|---|---|---|
| each weight ± 0.05 (12 runs, renormalized) | **0.993–0.997** | 14–15 | 7–11 | 4–10 |
| without pop_growth | 0.848 | 11 | 41 | 29 |
| without owner_sf_homes | 0.920 | 9 | 24 | 23 |
| without zone_peak_cagr | 0.952 | 12 | 23 | 22 |
| without permits_per_1k | 0.956 | 13 | 24 | 11 |
| without dc_sites | 0.965 | **8** | 19 | 15 |
| without owner_sf_share | 0.976 | 14 | 22 | 13 |

The ranking is robust to the exact weights; it depends on *which* signals are in. `dc_sites` moves the top most (only
8 of the top 15 stay without it) although it is 69% zeros: it is the tie-breaker among the fast-growing co-ops.

## Proposed decisions.md lines

- 2026-09-26 — Account triggers = dated public events mapped to an account by county (the account covers ≥ 20% of the
  county's land) or by name, active for 12 months: five strong (`dc_permit`, `gen_storage_ia` storage/gas ≥ 50 MW IA,
  `dev_agreement` Ch. 312 / Ch. 380 ≥ US$1M, `market_registration`, `permit_surge` ≥ +25%) and three context
  (`new_transmission` TPIT ≥ 138 kV, `rate_increase` ≥ +10%, `tsp_large_load` G&T ≥ 1 GW in the 58777 RFI). — X5: each
  has a date, a source reference and a reproducible rule; 46 of 107 accounts have an active strong trigger, while
  transmission (48 accounts, 1,022 new projects a year) and the RFI (one filing for 73 accounts) are too broad to drive
  the action.
- 2026-09-26 — Next action = score tier (A top quartile, B second, C bottom half) × active strong triggers: A + any →
  call now; B + (≥ 2 or one ≤ 90 days) → call now; B + one older → nurture; A without → nurture; B without → watch;
  C + any → watch; C without → hold. — X5: 25 call-now accounts out of 107, stable under ± 0.05 weight changes (4–10
  actions change).
- 2026-09-26 — City-signed Ch. 380 agreements map only to the city's own muni; Ch. 312 and county-signed Ch. 380 map by
  county. — A city deal sits inside the city (IOU or muni wires); mapping it by county handed Arlington's deals to a
  co-op.
- 2026-09-26 — CPA data centers, JETI, PUCT filings, ERCOT members, QCEW and EAGLE-I are not triggers for now. — No
  county (7/167, 0/21), no discrimination (TEC/TPPA file for everyone; 1 account files itself), yearly or suppressed
  (QCEW 28/118 counties disclosed), or out of the MVP (outages).

## Review items for Pablo

1. **Strong vs context.** `gen_storage_ia` fires for 31 accounts and makes 8 of the 25 call-nows; demoting it to
   context leaves 17. Claude's default keeps it strong (the kickoff names "a new generation or storage project").
2. **`dc_sites` counted twice.** The score's `dc_sites` (sites since 2025) and the `dc_permit` trigger (last 12 months)
   read the same TCEQ sites. Default: keep both (exposure vs timing); the alternative is to drop `dc_sites` from the
   score and let the trigger carry it (top 15 would keep only 8 names).
3. **Fannin County EC at #2** (and Bartlett EC at #8) come from area apportionment of Collin / Williamson–Bell: 2.6 and
   3.3 apportioned homes per EIA meter. Fixes: EIA customers as the size signal once the 861S short form lands (X4),
   or ZCTA/block apportionment.
4. **Thresholds are Claude's:** 20% county share, 12-month window, 90-day freshness, ≥ 50 MW, US$1M for Ch. 380,
   +25% permits, +10% price, ≥ 1 GW for the RFI, ≥ 138 kV. Sensitivity for the county share is in §1.
5. **Name matches to check:** ERCOT registrations use core-name equality; an entity whose name does not say its type
   (e.g. "X POWER LLC") can match an account whose core name is "X". The two active matches (Sam Houston EC, City of
   Caldwell) are exact and typed; the 99 "ever" matches were not reviewed one by one.
6. **Dates not verified:** the meaning of TCEQ `affil_begin_dt` (Q4), Ch. 380 `effective_date` (incentive start vs
   signature), the 2025-10 TPIT spike (302 first listings), and EIA part D in the residential price.

## Useful for the core features

- **Mart `account_triggers`** (grain: account × event): `account_id`, `trigger`, `strength`, `event_date`,
  `age_days`, `active`, `title`, `detail`, `county_fips`, `exposure` (county_share or 1 for name matches), `mapping`
  (county / name), `source` (table), `source_ref` (RN, INR, agreement id, TPIT id, docket:item), `offer_angle`. This is
  `x5_trigger_events.csv` plus the spec columns; ~6k rows, fine in memory.
- **Mart `accounts`** (grain: account): `account_id`, `name`, `account_type`, `score`, `rank`, `tier`, the six
  `pct_*` columns and raw signals, `n_strong`, `n_context`, `latest_event`, `triggers` (list), `next_action`,
  `top_offer_angle`, plus data-quality flags (`homes_per_meter` > 1, imputed permits > 50%, Lubbock's EIA break).
- **/accounts:** sortable by rank, filter by tier / next action / trigger type / type; a "why now" chip per active
  trigger with its age ("data-center permit · 38 days"); CSV export straight from the mart.
- **/accounts/[id]:** a dated timeline of the account's events (ever, with the 12-month window shaded), the score
  breakdown by signal (percentile bars), the next-action card with the rule that fired ("Tier A + data-center permit
  → call now") and the offer angle, and the counties behind each county-mapped event with their `county_share`.
- **Validation (A2):** the partners' triggers and actions are computed by the same rules once the weights are
  approved; nothing here was computed for them.
