# Q5 — Do the large-load series support a realization ratio?

Phase 0, question Q5 (`docs/PHASE0_ANALYSIS.md` §2). Script: `analysis/q5_large_load.py`; logic:
`basecast_pipelines/models/large_load.py`; tests: `tests/models/test_large_load_models.py`. Figure and tables in
`analysis/out/q5_*` (gitignored). Run on 2026-09-26 against Cloud SQL as `basecast_reader`.

**Every chart value here was read by Gemini (`verified=false`). The human spot-check is PENDING**
(`docs/large-load-spot-check.md`). The numbers below are "not verified" until Pablo ticks it.

## 1. Inventory

Inputs: `large_load_chart_values` (6,470 values, 65 documents, 2022-08 → 2026-09), `large_load_headlines`
(247 sentences), `large_load_status` (103 native PPTX values, Oct 2024 deck and the Jul 2026 Batch Zero deck).
Series found (one row per document × page × chart in `analysis/out/q5_inventory.csv`):

| Series kind | Charts | Documents | What it is |
|---|---:|---:|---|
| `in_service_year` | 59 | 57 | "Current Large Load Interconnection Queue" / "Actual and Projected Large Load Growth 20xx-20yy": MW by projected in-service year, stacked by status. **Cumulative**: bar Y = MW with in-service ≤ end of Y |
| `status_by_month` | 24 | 24 | "ERCOT Approvals – Past 12 Months" / "Approved Large Load – Growth in the Past Year": approved-to-energize and planning-studies-approved **stock** by month |
| `energized_by_month` | 48 | 26 | "Loads Approved to Energize – Observations": observed (non-)simultaneous peak of approved loads by month |
| `queue_by_month` | 35 | 35 | total queue by month (standalone / co-located) |
| `by_load_zone` / `by_project_type` / `by_tsp` | 44 / 55 / 10 | 26 / 38 / 10 | approved or queued MW by zone, type, TSP |
| other | 83 | 41 | size bins, submittal quarter, Batch Zero, RPG, etc. |

Since May 2023 (58 documents: status decks, board updates, ERCOT Monthly, hearings):

- **Status decks: 30. All 30 have the in-service series; 24 have the status-by-month series** (the chart first
  appears in Dec 2023; the 5 decks of May–Oct 2023 carry only the approved-to-energize sentence, and the Oct 2024
  PPTX has it as a native chart in `large_load_status`, not read by Gemini).
- All documents: 54 of 58 have the in-service series, 24 the status-by-month series.
- **Vintages** (one snapshot per calendar month, board-update reprints of the previous deck collapsed, status deck
  preferred): **28**, all from status decks, 2023-05-31 → 2026-06-19.
- Status labels are stable from May 2023 (Approved to Energize, Planning Studies Approved, Under ERCOT Review, No
  Studies Submitted). From Jan 2025 "Approved to Energize" splits into Observed Energized + Approved but Not
  Operational (summed back here). From Apr 2026 the decks print GW with one decimal and split planning studies
  into "Section 9.4/9.5 met" and "9.4 only" (summed).

## 2. Consistency (tolerance ±5%)

| Check | Result |
|---|---|
| Bars sum vs chart total (every in-service chart × year with a stack) | **428 / 428** within ±5%; worst 2.1% (May 2026, GW rounding) |
| Chart total (last bar) vs same-deck "ERCOT is tracking … GW" sentence | **17 / 17**; worst 0.7% |
| Chart approved stock (last bar) vs same-deck "approved to energize … MW" sentence | **36 / 37**; the miss is Mar 2023 (2,570 vs 2,370 MW, +8.4%), before the phase 0 window; since May 2023 all within 0.7% |
| Cumulative totals rise with the year (a misread bar would break it) | 59 / 59 charts |
| **Vintages since May 2023 passing all checks** | **28 / 28**, each with at least one same-deck headline checked |

Cross-checks Claude ran without Pablo (independent reads, but not a substitute for the spot-check):

- Gemini status series (last month) vs the regex-read sentence of the same deck: **22 / 22** within 1% (the May
  2026 deck's chart is left out, see the misdated axes below).
- Native PPTX values (Oct 2024 deck) vs Gemini's reading of the same months in the next deck (Nov 2024):
  **63 / 63** within 1% (approvals, queue and observed-peak series).
- Same vintage read twice (revised copies and reprints): **252 / 252** bar values within 1%. The March 2026 PDF
  (data of 13 Mar, queue 239 GW) and its "Updated_03262026" PPTX (26 Mar, 410.6 GW) disagree because they are two
  different snapshots that Gemini dated alike; that is a real revision, not a misread.
- Gemini's `as_of` was wrong on three 2026 documents (read "2024-05-31" off the May 2026 deck, "2024-08-20" and
  "2024-07-28" off Aug 2026 decks). The vintage uses `as_of` only within −31/+90 days of the report date,
  otherwise the report date.
- **Misdated monthly axes (fixed 2026-09-26, found by exploration X1).** Gemini also read the May 2026 deck's
  four monthly charts (queue, approvals, two observed-peak charts) two years early: 2023-07..2024-05 for
  2025-07..2026-05. Because the approved stock by month keeps the latest deck's reading, those values overwrote
  11 months of the approved-to-energize series (Dec 2023 showed 8,786 MW instead of 4,479 MW).
  `large_load.misdated_month_axes` now flags a monthly chart whose *last* month is more than 3 months before its
  deck (or more than 1 after); every other chart ends 0–1 months before its deck. `drop_misdated_months` drops
  the flagged charts' monthly values, and `a2e_by_month` and both cross-checks apply it. The rule looks at the last
  month, not each value's age, because good charts open with old anchor bars (April 2022 in the 2023 queue
  charts, October 2022 in the approvals charts). Dropped rather than re-dated: shifting the May 2026 axes by +24
  months matches the June 2026 deck's reading of the same months 10/10, so nothing is lost, and a re-date would
  be a guess where no later deck confirms it.

## 3. Preliminary realization ratio

### Definitions (both series are stocks)

- **Promised(v, Y)**: in vintage (deck) `v`, the in-service bar for year `Y` = MW the queue says will be in
  service by 31 Dec of `Y`, all statuses. *Firm* variant: minus "No Studies Submitted".
- **Base(v)**: the approved-to-energize stock in vintage `v` (last bar's approved segments), i.e. already approved
  when the deck was made. It comes from the vintage's own in-service chart, not the monthly series, so the
  incremental ratio never used the misdated months.
- **Realized A2E(Y)**: approved-to-energize stock in Dec of `Y` from the status series (latest deck's reading):
  2024 = **6,297 MW**, 2025 = **8,786 MW**, 2026 (partial, Jun) = **8,926 MW**. These months were not among the
  misdated ones, so the axis fix leaves them, and every ratio below, unchanged. The fix changes only
  2023-07..2024-05 of the monthly series (table below). "Latest reading" means restated history wins: the Apr
  2024 deck restates Jul–Dec 2023 upward (Dec 2023: 4,479 MW, vs 3,188 MW in the Dec 2023 deck, which matches
  that deck's own sentence as of 11 Dec).
- **Realized energized(Y)**: observed-energized bar for `Y` in the first deck after `Y` ends: 2024 = 4,256 MW
  (Jan 2025 deck), 2025 = 5,728 MW (Jan 2026), 2026 partial = 5,700 MW (Jun 2026). This is observed peak
  consumption, lower than approved capacity by construction.
- **Gross** = Realized A2E(Y) / Promised(v, Y). **Incremental** = (Realized A2E(Y) − Base(v)) / (Promised(v, Y) −
  Base(v)): of the MW the deck said would *newly* come online by `Y`, the share that got approved.
- Only vintages made before the end of `Y` (and before the realized date) count; the band uses horizons ≥ 6
  months, because a deck made weeks before year end promises almost nothing new for that year.
- **Caveat:** the decks have no project ids, so this is stock-to-stock, not cohort survival: realized MW include
  loads that entered the queue after the vintage. That biases the incremental ratio up.

### Results (median across vintages [p10–p90], horizon ≥ 6 months)

| Target year | Vintages | Gross A2E | Gross A2E, firm | Incremental A2E | Incremental, firm | Energized ÷ promised |
|---|---:|---|---|---|---|---|
| 2024 | 10 | **0.32** [0.28–0.37] | 0.35 [0.31–0.41] | **0.15** [0.07–0.19] | 0.17 [0.09–0.21] | 0.22 [0.19–0.25] |
| 2025 | 18 | **0.35** [0.33–0.50] | 0.37 [0.35–0.61] | **0.20** [0.15–0.26] | 0.23 [0.17–0.33] | 0.23 [0.22–0.33] |
| 2026 (partial, Jun) | 28 | 0.28 [0.22–0.35] | 0.33 [0.30–0.45] | 0.09 [0.00–0.23] | 0.11 [0.00–0.25] | 0.18 [0.14–0.23] |

Per-vintage rows: `analysis/out/q5_ratios.csv`; figure: `analysis/out/q5_promised_vs_realized.png`. After the
axis fix, all 63 rows of `q5_ratios.csv` have the same values as before; only the column order changed.

Approved-to-energize stock by month, where the axis fix changed it (`analysis/out/q5_a2e_by_month.csv`):

| Month | Before (misdated May 2026 deck) | After | Read from |
|---|---:|---:|---|
| 2023-07 | 7,150 | 3,084 | May 2024 deck |
| 2023-08 | 7,502 | 3,744 | Jul 2024 deck |
| 2023-09 | 7,502 | 3,926 | Jul 2024 deck |
| 2023-10 | 7,502 | 3,926 | Sep 2024 deck |
| 2023-11 | 7,712 | no reading | — |
| 2023-12 | 8,786 | 4,479 | Nov 2024 deck |
| 2024-01 | 8,786 | 4,479 | Jan 2025 deck |
| 2024-02 | 9,042 | no reading | — |
| 2024-03 | 9,042 | 4,479 | Feb 2025 deck |
| 2024-04 | 9,012 | no reading | — |
| 2024-05 | 9,062 | 4,479 | Mar 2025 deck |

What the figure shows:

- **Promises run about 3× what gets approved.** For 2025 the decks promised 22–27 GW through Dec 2024 and 8.8 GW
  was approved by Dec 2025. Of the *new* MW promised a year or more ahead, 15–20% got approved on time.
- **The promise slides right instead of failing.** The 2025 bar fell from 26.8 GW (Oct 2024 deck) to 13.4 GW (Nov
  2025) as projects moved their dates, while the 2026+ bars grew. So the queue's in-service dates are optimistic
  rather than the projects being dropped. A realization *by year* therefore understates eventual realization.
- **2026 is moving.** From Apr 2026 the 2026 bar jumps from 21.8 GW (Mar) to 43–61 GW (Apr–Jun) as the queue
  goes past 400 GW (Batch Zero). Its ratio is partial (half a year of approvals) and is a lower bound.

## Decision

Rule: ≥ 6 decks with the in-service series and consistent totals (≤ 5%) → realization estimated, with a band (A3 as
planned); else plan B (three scenarios).

**28 vintages have the in-service series and all 28 are consistent (worst 2.1%) → realization is estimated, with a
band. A3 goes ahead as planned.** Central value for A3: incremental A2E ratio ≈ 0.20 at a ~12-month horizon
(band 0.15–0.26, from 2025); gross ≈ 0.35 (0.33–0.50). The screen labels it "estimated from ERCOT decks (chart
values machine-read; human spot-check: pending)" until Pablo's check. **Condition:** if the spot-check finds more
than 1 of 10 values wrong, Q5 falls back to plan B (three scenarios, "scenario, not estimated").

## Proposed findings row

| Q | Result (numbers) | Decision | FINAL_SPRINT task affected | Verified |
|---|---|---|---|---|
| Q5 | decks with in-service series = 30/30 status decks since May 2023 (28 monthly vintages; 24 also have the status-by-month series); consistency = 428/428 bar stacks ≤5% (worst 2.1%), totals 17/17 and approved stock 36/37 vs same-deck headlines (all ≤0.7% since May 2023); spot-check: 0/10 checked (pending), auto cross-checks 22/22, 63/63, 252/252; the May 2026 deck's misdated monthly axes are now dropped at the source (the ratios did not change); realization (A2E at year end ÷ promised, median [p10–p90], horizon ≥6 mo) 2024 = 0.32 [0.28–0.37], 2025 = 0.35 [0.33–0.50]; incremental 2024 = 0.15, 2025 = 0.20; 2026 partial = 0.28 | estimated (with band), conditional on the spot-check | A3 | no (spot-check pending) |

## Review items for Pablo

- **[PERGUNTAR] Spot-check** `docs/large-load-spot-check.md`: 10 values (5 in-service bars, 5 approved-stock
  months) with the public ERCOT link and page. Tick OK or write the slide's number. More than 1 miss → plan B.
- Confirm the **ratio definition** for A3: incremental A2E (new approvals ÷ new promised) as the headline, gross
  and energized as context. Alternative: gross A2E.
- Confirm that the **approved-to-energize stock** (not observed energized) is the "realized" side. Energized
  (observed peak) is about 65% of approved (5.7 of 8.8 GW in Jan 2026), which matters for the peak forecast.

## Proposed decisions.md lines

- 2026-09-26 — Q5: the large-load realization ratio is estimated with a band (A3 as planned), not scenarios. — 28 monthly vintages since May 2023 carry the in-service chart and all pass the ±5% checks (bars vs total, totals and approved stock vs same-deck headlines).
- 2026-09-26 — Realization ratio = approved-to-energize stock in Dec of Y ÷ the vintage's in-service bar for Y; the incremental variant subtracts the vintage's own approved stock; band = p10–p90 across vintages with horizon ≥ 6 months. — Both series are stocks and the decks have no project ids, so the ratio is stock-to-stock, not cohort survival.
- 2026-09-26 — Deck vintages: one per calendar month, board-update reprints collapsed, and Gemini's chart `as_of` used only within −31/+90 days of the report date. — Gemini dated the May and Aug 2026 decks to 2024, and board updates reprint last month's deck.
- 2026-09-26 — A Gemini monthly-axis chart whose last month is more than 3 months before its deck (or more than 1 after) is treated as misdated and its monthly values are dropped (`large_load.drop_misdated_months`, also used by X1 with its 16-month tolerance). — Gemini read the May 2026 deck's axes two years early, which overwrote 2023-07..2024-05 of the approved stock; a per-value age limit would also drop real anchor bars (Apr 2022, Oct 2022).
- 2026-09-26 — If the human spot-check of the Gemini large-load values finds more than 1 of 10 wrong, the realization ratio drops to plan B (three scenarios). — The automatic checks compare Gemini against the same decks' text, not against a human reading.

## Useful for the core features

- **Forecast screen, large-load flow:** the per-vintage in-service stack (`in_service_wide` + `pick_vintages`) is
  the "promised" layer, and the approved stock by month (`a2e_by_month`, misdated axes dropped) is the "realized"
  line. It has no reading for 2023-11, 2024-02 and 2024-04, so carry the previous month forward or leave a gap. Same shapes as the
  figure; `realization_ratios` is the mart behind a "promised vs approved" chart with the band.
- **Video line (after the spot-check):** "ERCOT's own decks promised about 3× the large load that was approved by
  year end; only 15–20% of the new MW promised a year ahead got approved on time." Source: 18 vintages, 2025.
- **Non-obvious insight:** the queue does not mostly fail, it slides. The 2025 promise halved in one year while
  the 2026+ bars grew. The forecast should model delay (dates moving right) as well as attrition.
- **Peak forecast input:** approved ≠ load on peak. Observed energized peak is ~65% of the approved stock, so the
  MW that reach the summer peak need a second factor after realization.
- **Guardrail:** these ratios are for 1–3 year horizons. Do not apply them to the 2030 bar (232.5 GW in Jan 2026)
  without a horizon-specific estimate; the Apr 2026 jump past 400 GW also changes the queue's makeup (Batch Zero).
