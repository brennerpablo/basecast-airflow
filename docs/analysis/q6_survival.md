# Q6 — Can the generation queue support a survival model?

Phase 0, `docs/PHASE0_ANALYSIS.md` §2 Q6. Run on 2026-09-26 against production Postgres (`basecast_reader`),
`gis_project_events` built from the GIS reports through **2026-08** (latest report month).

- Script: `analysis/q6_survival.py` (`uv run --group analysis python analysis/q6_survival.py`)
- Model code: `basecast_pipelines/models/survival.py` (`load_events()`, `outcomes()`, `landmark_frame()`,
  `fit_cif()`, `cif_table()`, `merge_small_strata()`); tests in `tests/models/test_survival.py`
- Outputs (gitignored): `analysis/out/q6_cif_curves.csv` (step curves per landmark × stratum, with at-risk
  counts), `analysis/out/q6_cif_by_landmark.png`

## 1. Cohort composition

Cohort: `first_seen_month >= 2018-08` (first report of the GINR layout), **n = 3,117** INRs (of 3,764).
Fuel group from `fuel_type`: solar, wind, storage, gas; everything else is "other" (other 33, oil 13,
hydro 2, nuclear 2, coal 2, biomass 1). No cohort row has a null `fuel_type`.

| Fuel group | active | operational | cancelled | inactive | dropped | total |
|---|---:|---:|---:|---:|---:|---:|
| storage | 889 | 229 | 360 | 75 | 7 | 1,560 |
| solar | 603 | 91 | 303 | 58 | 8 | 1,063 |
| gas | 136 | 34 | 47 | 5 | 1 | 223 |
| wind | 129 | 29 | 38 | 22 | 0 | 218 |
| other | 10 | 17 | 25 | 1 | 0 | 53 |
| **total** | 1,767 | 400 | 773 | 161 | 16 | 3,117 |

- **dropped:** 16 (0.5%). Plus 9 projects last listed as `inactive` that vanished before the latest
  report; the model treats them like `dropped` ("vanished", 25 in all).
- **exit_inferred:** 17 (0.5%): the 16 dropped plus 1 operational project inferred from a synchronization
  approval (no COD record).
- **months_missing:** 0 for 3,088 (99.1%), 1 for 13, 2–10 for 16 (max 10). Gaps by status: active 17,
  operational 7, cancelled 5, inactive 0, dropped 0. The monthly series is almost gap-free in this cohort.
- Follow-up is short for most of the cohort: 1,777 of 3,117 were first listed in 2023–2026, and 1,919 (62%) are
  still listed in the queue (active or inactive).

## 2. Event definitions

- **Event of interest (COD):** ERCOT's commercial operation approval, `commercial_operation_date` (399 of
  400 operational; the one inferred from synchronization uses its `exit_month`). Why COD and not
  synchronization or energization: it is what the product promises ("reaches COD"), and the earlier
  milestones are near-certain precursors, not alternatives. Of 491 synchronized projects, 386 are operational,
  102 are still active, 3 inactive and **none was cancelled**; synchronization precedes COD by a median
  4.7 months (p90 14.3), energization by 5.8 (p90 15.1). Using synchronization would only shift the curves
  about five months earlier.
- **Competing event (withdrawn):** cancellation, dated `cancel_date` (never null for cancelled projects;
  a project cancelled, relisted and cancelled again counts from its last active month).
- **Vanished** (`dropped`, or inactive and gone before the latest report): withdrawn at the end of the last
  listed month (primary), or censored there (sensitivity, §6).
- **Censored:** projects still listed in the 2026-08 report, active **or inactive**, at the end of that
  month. Inactive is not an exit: of 413 cohort projects that were ever inactive, 120 are active again and
  54 reached COD.
- **Clock:** months (days / 30.4375) from the landmark date. **Landmarks:** entry (`entry_date` =
  `screening_study_started`, else the first listed month), `fis_approved`, `ia_signed`; only projects that
  reached the landmark are included.
- **Delayed entry (left truncation):** a project is observed from its first listed month, so it enters the
  risk set there when it was listed after its landmark date: 908 projects at entry (29%, median 6.2
  months late), 28 at FIS approved (3%), 194 at IA signed (18%, median 9.7 months). Ignoring it barely
  moves the entry curve (pooled CIF36 8.0% vs 8.3%) but understates the IA curve (41.1% vs 44.0%).
- **Estimator:** Aalen–Johansen written in numpy (`fit_cif`: exact handling of ties, delayed entry,
  optional weights). Cross-check against lifelines `AalenJohansenFitter` (same entry, jittered ties, seed 0):
  max absolute difference **7.7e-4** at 12/24/36/48 months over all 15 landmark × stratum fits.

## 3. Strata merges

Rule: a stratum with n < 30 or fewer than 10 COD events is folded into another, applied so that the
groups are the same in all three landmarks.

| Fuel group | n entry / FIS / IA | COD entry / FIS / IA |
|---|---|---|
| gas | 223 / 75 / 82 | 34 / 31 / 32 |
| other | 53 / 12 / 13 | 17 / 10 / 10 |
| solar | 1,063 / 367 / 373 | 91 / 90 / 91 |
| storage | 1,560 / 461 / 502 | 229 / 126 / 225 |
| wind | 218 / 65 / 83 | 29 / 27 / 26 |

**Merge:** `other` fails n ≥ 30 at FIS and IA → folded into gas as **gas+other**. Final strata: solar,
storage, wind, gas+other. All pass the rule in every landmark.

## 4. CIF of COD by landmark × fuel group

Primary definition (vanished = withdrawn). Counts: n, COD events, withdrawals; CIF in %; `wd36` = CIF of
withdrawal at 36 months; at risk = projects still at risk at 36 / 48 months. Cells marked † have fewer
than 10 projects at risk at that horizon: the value is the last observed step, not an estimate to show.

| Landmark | Stratum | n | COD | withdrawn | CIF12 | CIF24 | CIF36 | CIF48 | wd36 | at risk 36 / 48 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| entry | solar | 1,063 | 91 | 317 | 0.1 | 0.9 | 1.8 | 4.0 | 25.6 | 518 / 362 |
| entry | storage | 1,560 | 229 | 368 | 4.0 | 7.7 | 10.6 | 15.7 | 21.3 | 606 / 328 |
| entry | wind | 218 | 29 | 39 | 1.2 | 2.4 | 6.9 | 13.4 | 19.8 | 83 / 42 |
| entry | gas+other | 276 | 51 | 74 | 12.7 | 16.5 | 24.6 | 31.3 | 34.5 | 44 / 27 |
| entry | **all** | 3,117 | 400 | 798 | 3.2 | 5.7 | 8.3 | 12.4 | 23.6 | 1,251 / 759 |
| FIS approved | solar | 367 | 90 | 55 | 0.0 | 7.2 | 18.6 | 34.4 | 13.4 | 139 / 74 |
| FIS approved | storage | 461 | 126 | 65 | 2.0 | 17.5 | 34.7 | 49.3 | 16.8 | 79 / 29 |
| FIS approved | wind | 65 | 27 | 3 | 4.0 | 23.1 | 48.5 | 70.6† | 5.6 | 15 / 0 |
| FIS approved | gas+other | 87 | 41 | 16 | 19.3 | 42.9 | 59.8 | 59.8† | 19.0 | 11 / 8 |
| FIS approved | **all** | 980 | 284 | 139 | 2.8 | 16.1 | 31.3 | 45.3 | 14.9 | 244 / 118 |
| IA signed | solar | 373 | 91 | 31 | 0.3 | 4.8 | 22.2 | 35.4 | 7.7 | 126 / 75 |
| IA signed | storage | 502 | 225 | 32 | 5.4 | 31.3 | 59.1 | 73.6 | 7.2 | 69 / 24 |
| IA signed | wind | 83 | 26 | 2 | 0.0 | 9.8 | 31.5 | 58.9 | 4.0 | 19 / 11 |
| IA signed | gas+other | 95 | 42 | 9 | 15.6 | 52.2 | 67.5 | 69.9† | 8.6 | 10 / 3 |
| IA signed | **all** | 1,053 | 384 | 74 | 3.9 | 21.8 | 44.0 | 57.4 | 7.4 | 226 / 117 |

**MW-weighted** CIF36 / CIF48 (weight `capacity_mw`, the latest listed capacity; 126 cohort rows without it
are left out): entry all **2.8% / 6.4%** (solar 1.3 / 3.4, storage 3.2 / 7.3, wind 1.3 / 7.7, gas+other
11.9 / 20.7); FIS approved all 24.9 / 38.8; IA signed all **30.4% / 46.6%** (solar 20.1 / 35.0, storage
43.6 / 63.4, wind 24.7 / 53.0, gas+other 65.7 / 68.6). Large projects reach COD less often and later
than small ones, so the MW share is well below the project share.

## 5. Ordering sanity check (CIF36: IA signed > FIS approved > entry)

| Stratum | entry | FIS approved | IA signed | ordered? |
|---|---:|---:|---:|---|
| solar | 1.8 | 18.6 | 22.2 | yes |
| storage | 10.6 | 34.7 | 59.1 | yes |
| wind | 6.9 | 48.5 | 31.5 | **no** |
| gas+other | 24.6 | 59.8 | 67.5 | yes |
| all | 8.3 | 31.3 | 44.0 | yes |

Wind fails, and the reason is structural, not a data error: **the two landmarks are not nested.** Among
cohort projects with both milestones, the IA was signed before FIS approval in 67% of wind projects (median
FIS approval 7.9 months after the IA), and in 35–45% of the other groups (median gap under 2 months). For
wind, "IA signed" is the earlier milestone. The wind estimates are also thin (15 and 19 at risk at 36
months) and their 95% intervals overlap (lifelines): FIS approved [32.1, 63.1] vs IA signed [16.9, 47.2].
Entry < IA holds in every stratum, and entry < FIS holds in every stratum.

## 6. Dropped sensitivity (vanished = competing vs censored)

CIF36 difference, censored minus competing, in percentage points:

| Landmark | gas+other | solar | storage | wind | all |
|---|---:|---:|---:|---:|---:|
| entry | +0.19 | +0.03 | +0.05 | +0.04 | +0.06 |
| FIS approved | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| IA signed | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

Max **0.19 pp**, far below the 10 pp threshold: the app shows one version (vanished = withdrawn) with a
footnote. Only 25 cohort projects vanished, none after reaching FIS approval or IA.

## 7. County → FIPS

`county` (name) against `tx_counties.county_name`, normalized (upper case, letters only, "County"
removed): **100%** match, 3,117 / 3,117 in the cohort and 3,764 / 3,764 over all INRs; no null county.
Unmatched list: none. Target (≥ 98%) met.

## 8. Plan B, ready: cohort rates

Current outcome of each first-listed-year cohort (all fuels):

| Cohort | n | COD | withdrawn | still queued |
|---|---:|---:|---:|---:|
| 2018 (Aug–Dec) | 83 | 27.7% | 53.0% | 19.3% |
| 2019 | 243 | 25.5% | 46.5% | 28.0% |
| 2020 | 240 | 28.8% | 37.1% | 34.2% |
| 2021 | 310 | 20.6% | 38.4% | 41.0% |
| 2022 | 464 | 12.3% | 32.1% | 55.6% |
| 2023 | 542 | 10.1% | 24.2% | 65.7% |
| 2024 | 530 | 7.0% | 17.4% | 75.7% |
| 2025 | 503 | 6.6% | 10.5% | 82.9% |
| 2026 | 202 | 0.0% | 4.0% | 96.0% |

Cohort rates cannot say anything about the 2022+ cohorts (56–96% still queued): that is what AJ with
censoring is for.

## Decision

- **Handoff rule, applied literally: cohort rates for A4.** Strata sizes pass (after gas+other), the dropped
  sensitivity is 0.19 pp (< 10 pp, one version), but the ordering check fails in one fuel (wind: FIS
  approved 48.5% > IA signed 31.5% at 36 months), and the rule asks for coherent ordering in every fuel.
- **Proposed override, pending Pablo:** Aalen–Johansen in A4 with two nested landmarks, **entry and IA
  signed**, for the four strata (ordering entry < IA holds everywhere), and FIS approved dropped as a
  landmark (or shown pooled only). The check failed because its premise (FIS approval comes before the IA)
  does not hold in ERCOT's queue, not because the data or the estimator are broken: sizes pass, the numpy
  estimator matches lifelines to 7.7e-4, and cohort rates would be blind for the 2022+ cohorts that make
  up most of today's queue.

## Proposed findings row

| Q | Result (numbers) | Decision | FINAL_SPRINT task affected | Verified |
|---|---|---|---|---|
| Q6 | cohort n = 3,117 (first listed ≥ 2018-08); dropped = 0.5% (exit_inferred 0.5%); CIF36 IA-signed solar = 22.2% (storage 59.1%, all 44.0%; from entry all 8.3%, MW-weighted 2.8%); ordering ok? no, wind only (FIS 48.5% > IA 31.5%: IA precedes FIS approval in 67% of wind projects); strata: other → gas+other; dropped sensitivity ≤ 0.19 pp; county match = 100% | cohort rates by the rule; AJ override (entry + IA landmarks) proposed, pending Pablo | A4 | — |

## Review items for Pablo

1. **Override the rule?** Approve AJ with entry + IA signed landmarks (FIS approved dropped or pooled only)
   instead of cohort rates. Claude's default: yes, for the reasons in the Decision.
2. **Event = ERCOT commercial operation approval**, not synchronization (≈ 5 months earlier, no synchronized
   project ever cancelled). Confirm COD is the milestone the app names.
3. **Inactive projects still listed are censored**, not withdrawn (174 of 413 ever-inactive came back or
   reached COD). The alternative would lower the curves slightly.
4. **Cohort boundary:** `first_seen_month >= 2018-08` excludes projects first listed in the old layout that
   reached FIS/IA after 2018-08. A landmark-date-based cohort (landmark ≥ 2018-08, first listed ≥ 2017-04,
   when cancellations start to be recorded) would add history to the FIS/IA curves. Not done (timebox).
5. **What the app shows past the data:** curves are cut where fewer than 10 projects remain at risk; CIF48
   is not available for wind at FIS approved and gas+other at FIS/IA (†). Confirm the cut-off of 10.
6. **MW-weighted vs project-count CIF:** the Explorer's "adjusted MW" should use the MW-weighted curves
   (2.8% vs 8.3% at 36 months from entry). `capacity_mw` is the latest listed capacity (not verified how
   often it changes over a project's life).

## Proposed decisions.md lines

- 2026-09-26 — Queue survival event of interest = ERCOT commercial operation approval (`commercial_operation_date`); competing event = cancellation; projects that vanish without a recorded reason (dropped, or inactive and gone) count as withdrawn at the end of their last listed month. — Synchronization precedes COD by a median 4.7 months and no synchronized project in the cohort was cancelled; treating vanished projects as censored moves CIF36 by at most 0.19 pp.
- 2026-09-26 — Inactive projects still listed in the latest GIS report are censored, not withdrawn. — 174 of 413 ever-inactive projects in the cohort came back to active (120) or reached COD (54).
- 2026-09-26 — Queue survival strata: solar, storage, wind, gas+other (fuel "other" folded into gas by the n ≥ 30 / ≥ 10 COD rule); delayed entry at the first listed month in every landmark. — "Other" has 12–13 projects at the FIS/IA landmarks; 29% of the cohort was listed after its screening start and 18% after its IA.
- 2026-09-26 — [pending Pablo] A4 uses Aalen–Johansen with the entry and IA signed landmarks; FIS approved is not a landmark (or pooled only). — The phase 0 ordering check failed only for wind because the IA precedes FIS approval in 67% of wind projects; cohort rates cannot see the 2022+ cohorts (56–96% still queued). If Pablo keeps the rule instead: A4 uses cohort rates.

## Useful for the core features

- **Forecast survival screen:** the curve for a project is its stratum's CIF from the latest landmark it has
  reached; for a project already *t* months past that landmark, the conditional chance of COD by *t + h* is
  (CIF(t + h) − CIF(t)) / S(t), all of which `fit_cif` returns (`cif`, `survival`). The screen can show
  "COD by 12/24/36/48 months" plus the withdrawal curve, cut where fewer than 10 are at risk.
- **Explorer adjusted queue:** adjusted MW per county = Σ project MW × conditional MW-weighted CIF at the
  chosen horizon. County → FIPS matches 100%, so no manual crosswalk is needed.
- **Video (verify before use, phase 0 numbers):** of the MW that enters ERCOT's queue, about **3% is
  operating 3 years later and 6% after 4 years** (MW-weighted AJ, cohort first listed since Aug 2018); a
  signed IA is the real signal: **44% of IA-signed projects reach COD within 3 years**, 59% for storage vs
  22% for solar.
- Non-obvious for the pitch: after an IA, **storage converts almost three times as fast as solar** (59% vs
  22% at 36 months), and from entry, solar almost never reaches COD within 3 years (1.8%).
