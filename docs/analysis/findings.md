# Phase 0 findings — 2026-09-26, 12:40–12:59 CDT (setup f750659 to findings d7be30e)

Partner mode: **validation** (Pablo delegated the choice to Claude at 12:35 CDT; Claude chose validation because it
can still turn into look-alike later, while look-alike can never go back to validation). Q3 held the five partner
accounts out of every statistic; `tests/test_partner_lock.py` keeps `analysis/` away from `base_public_facts`.

Plan and rules: `docs/PHASE0_ANALYSIS.md`. Per-question detail (tables, definitions, caveats):
`docs/analysis/qN_*.md`. Scripts: `analysis/qN_*.py` (`uv run --group analysis python analysis/<script>`), logic in
`basecast_pipelines/models/`. `docs/FINAL_SPRINT.md` was not available, so the task column names the tasks as the
phase 0 handoff does. Every decision below follows the handoff's rule; where Claude proposes to deviate, the
row says so and the item waits in the review queue.

| Q | Result (numbers) | Decision | FINAL_SPRINT task affected | Verified |
|---|---|---|---|---|
| Q1 | 2026 summer peak = **91,134 MW** hourly, HE 18 CDT on 2026-07-22 (15-min 91,263 MW; data_as_of 2026-09-26, Jul–Aug not final-settled); preliminary 112 GW error = **+20.9 GW / +22.9%** (ERCOT's own 90.5–98 GW range contained the actual); LTLF mean error by horizon: 1y +1.6%, 3y +0.5%, 5y −1.5% (share over-forecasting 58% / 60% / 22%); 2026 by vintage: TSP-based +16.8 to +22.9%, ERCOT-adjusted +3.9 to +4.7% | mixed sign → narrative on the 2026 preliminary and on the TSP-based vs ERCOT-adjusted spread, not "X% at Y years" | A1 | file (ERCOT D&E workbook, LTLF/CDR files) |
| Q2 | PUCT universe = **112** (52 co-ops + 60 munis in ERCOT); EIA-861 2024 ERCOT co-ops/munis = 106 (60 long form + 46 short form only, 861S not parsed); matched ≥ 90 with a shared county = **107/112 (95.5%)**; after review 112/112 (5 brand names, proposed accept); all 5 partners exist by name; EIA sales loaded for 66/112 | **go** (PUCT ∩ EIA) | A2 | crosswalk pending review |
| Q3 | 107 accounts (partners held out); kept: pop_growth .25, owner_sf_homes .20, permits_per_1k .15, dc_sites .15, zone_peak_cagr .15, owner_sf_share .10; dropped: 4 EIA-861 signals (coverage 57% < 80%), population and permit_units (ρ ≥ 0.96 with owner_sf_homes); EIA jumps > ±50%: 3 utilities (Lubbock 2024 −95% customers); permits 7.6% imputed, 31/254 counties unreported | score on 6 public signals; weights proposed in `config/account_score.yaml` (pending Pablo), committed at 8c40761 | A2 | — |
| Q4 | 38 new TCEQ data-center sites (first permit since 2025) in 27 counties; co-op territory **46.6%** weighted by county_share (19/38 = 50% by largest area; upper bound 57.8%); outside the metros **35/38** (13-county list, which reproduces the old "34 of 37") or 28/38 (density ≥ 100/km²); by county, not by point | **signal only** (< 60% co-op) | A2, video | — |
| Q5 | in-service series in 30/30 status decks since May 2023 (28 monthly vintages); consistency 428/428 bar stacks ≤ 5%, totals 17/17 and approved stock 36/37 vs same-deck headlines; spot-check 0/10 (pending), automatic cross-checks 23/23, 63/63, 252/252; realization (approved-to-energize at year end ÷ promised, median [p10–p90]) 2024 = **0.32 [0.28–0.37]**, 2025 = **0.35 [0.33–0.50]**, 2026 partial 0.28; incremental 0.15 / 0.20 | **estimated with a band** (A3 as planned), falls to scenarios if the spot-check finds > 1 of 10 wrong | A3 | no (Gemini values; human spot-check pending) |
| Q6 | cohort n = **3,117** (first listed ≥ 2018-08); dropped 0.5%, exit_inferred 0.5%; CIF of COD at 36 months: from entry 8.3% (MW-weighted 2.8%), FIS approved 31.3%, IA signed **44.0%** (solar 22.2%, storage 59.1%, wind 31.5%, gas+other 67.5%); ordering fails for wind only (FIS 48.5% > IA 31.5%: 67% of wind projects sign the IA before FIS approval); dropped sensitivity ≤ 0.19 pp; county match 100%; numpy AJ = lifelines within 7.7e-4 | rule → cohort rates; **Claude proposes AJ on the entry + IA landmarks** (pending Pablo) | A4 | — |
| Q7 | peak ~ year + hottest 3-day mean temp, 2003–2025: R² ERCOT 0.94 (SOUTH .98, COAST .95, NCENT .95, WEST .92, SCENT .89, FWEST .80, EAST .71, NORTH .02); residual SD ERCOT 2.1 GW (3.0%); hold-out (fit ≤ 2022 → 2023–25) MAPE ERCOT = **7.6%**, every year under-predicted, trend-only 7.5%; zones > 10%: NORTH 47.0%, FWEST 35.3%, SCENT 10.1%; trend break ~2020 (p < 0.001); 1-year-ahead MAPE 2.8%; 2026 = 91.1 GW vs 84.2 GW expected (+8.3%) | **plan B** for A3 (residual band, no weather simulation); NORTH, FWEST, SCENT flagged | A3 | — |

## Review queue for Pablo

Every [PERGUNTAR] of the handoff, plus the calls Claude made alone. Claude's default is already in use; nothing
below blocks the next tasks except item 1 (A2 needs the weights first).

1. **Q3 / X4 — pick and approve the score weights.** Two proposals, both fixed without the partners: Q3's
   (`config/account_score.yaml`, on area-apportioned homes) and X4's (EIA customers from the 861S short form replace
   homes, customer growth 2019→2024 enters; ρ 0.94 with Q3, 23 of the top 25 kept; table in
   `docs/analysis/x4_eia861_short_form.md` §3). Claude's recommendation: X4, because it removes the apportionment
   artifact X5 found (Fannin County EC at #2 with 2.6 apportioned homes per meter). Once approved, A2 reveals the
   partners. Related: the near-constant threshold (mode ≥ 90%) is Claude's; at 60% `dc_sites` would drop out; X4
   shows munis rank low because they are small (median 3,012 meters), so ranking within type is the fix if Base
   wants munis on their own terms.
2. **Q2 — the 5 crosswalk rows under 90** (CPS Energy, CoServ, PenTex, BTU, Comanche): all proposed accept, each
   pinned by ≥ 99.8% county overlap. Table in `docs/analysis/q2_universe.md`.
3. **Q5 — spot-check** `docs/large-load-spot-check.md` (10 values with the ERCOT link and page). More than 1 miss →
   plan B (three scenarios). Also confirm the ratio definition (incremental as headline?) and that the
   "realized" side is approved-to-energize, not energized (energized ≈ 65% of approved in Jan 2026).
4. **Q6 / X2 — override the rule?** Claude proposes Aalen–Johansen instead of cohort rates: the ordering check
   assumed FIS approval comes before the IA, which is false for most wind projects, and cohort rates are blind to
   the 2022+ cohorts. X2 refined it: entry-stage projects need a semi-Markov step (time to IA, then the IA curve),
   because the marginal entry curve counts COD of projects that already signed and overpredicts entry-stage MW
   5–11× in the backtest. Also confirm COD (not synchronization) as the event and the cut where fewer than 10
   projects remain at risk.
5. **Q7 — which spread sets the A3 band** (default: one-year-ahead rolling errors, not the in-sample SD) and what
   "organic" means (default: fit ≤ 2019 for the flagged zones and ERCOT, post-2020 excess shown as realized large
   load, so the large-load layer is not counted twice). NORTH's 2009 drop and 2022+ jump and WEST's +33% in 2026 are
   characterized by X1 (causes not verified).
6. **Q1 — pitch wording and product context.** Say "preliminary long-term forecast"; ERCOT's own April 2026 range
   held the actual. The "demand record of ~87–91 GW" in Jan 2026 (CLAUDE.md, KICKOFF §1) is wrong: the record then
   was 85,508 MW (2023-08-10); it was broken on 2026-07-22 at 91.1 GW (preliminary).
7. **Q4 — metro definition** (13-county list vs density ≥ 100/km²), whether to keep the 23 sites matched only by
   NAICS 518210 (some names are not data centers), dropping the 7 non-ERCOT sites from the ERCOT-facing signal, and
   whether "35 of 38 outside the big metros" can go in the video even though Q4 is "signal only".
8. **Thresholds** adopted as in the handoff: Q7 5% / 10%, Q1 "consistent sign" = ≥ 2/3 of vintages, Q5 ±5%.

9. **X1 — the unattributed layer.** Accept a flat "unattributed" layer (~4.5 GW in 2026) next to 0.51 × the
   approved stock, or fold it into large load (factor ~1.0)? Check the flatness against ERCOT's distributed-solar
   estimate before the video says "runs day and night". Don't name WEST's 2026 block (~0.7 GW, flat) in the video.
10. **X2 — large new gas.** 9.1 GW of the Dec 2027 adjusted queue is gas+other, much of it ≥ 500 MW, where only 2 of
   16 such projects with an IA reached COD. Default: flag it in the UI now, add a size split if time allows. The
   Bernoulli band is too narrow to show; use the backtest errors (±13%) or a bootstrap.
11. **X3 — interval label and dollars.** Confirm the D&E 15-min peak time is interval *ending* against one year of
   ERCOT's published 4CP (if it is beginning, the window moves 15 minutes later). No $/kW-yr transmission rate is
   in the repo: source it before any dollar figure reaches the pitch.
12. **X5 — triggers.** Keep "storage or gas ≥ 50 MW signs its IA" as a strong trigger (25 call-now) or demote it to
   context (17)? Data centers count in both the score and a trigger (default: keep both). Every trigger threshold
   is Claude's (list in `docs/analysis/x5_triggers.md`).

## Beyond phase 0 — explorations X1–X5

Run after the seven questions closed, each aimed at a core feature. Detail in `docs/analysis/xN_*.md`; every number
is a candidate, not yet in `docs/video-numbers.md`.

| X | Question | Headline | Feeds |
|---|---|---|---|
| X1 | Is the peak excess over weather the large loads? | Against a pre-2020 model, the summer peak ran 6.9 / 8.1 / 7.1 / 11.2 GW high in 2023–2026; the overnight minimum rose as much as the peak (flat load); the decks' observed large loads cover a third to half. Split for the Forecast: pre-break organic + 0.51 × approved stock + a flat unattributed layer (2026: 89.1 vs 91.1 GW, −2.3%). Found and fixed a misdated deck in Q5 (ratios unchanged). | Forecast (A3), video |
| X2 | Raw vs adjusted generation queue by county | 438 GW active (1,810 projects) → ~38.7 GW reaching COD by Dec 2027 (8.8%) and 70.4 GW by Dec 2028; 24-month backtests from 2022, 2023 and 2024 queues miss by −12.5%, +9.4%, +1.8%, while developers' COD dates overstate 4–7×; county ranking beats the raw queue (Spearman 0.64–0.66 vs 0.43–0.45). | Explorer map, A4, video |
| X3 | 4CP timing and the offer to co-ops | A 2-hour discharge 15:45–17:45 covered 63 of 64 CPs (2010–2025); catching all four with a weather-based call takes ~56 days a summer and near-peak days doubled since 2023; since 2021 no CP fell in its month's top-20 priced intervals and the net-load peak moved to HE 20–21. | Accounts next action / offer, video |
| X4 | EIA-861 short form | Reading 861S from the lake takes EIA customers from 57% to 99.1% of accounts (munis 27% → 98%); customers replace apportioned homes; Lubbock's −95% is retail choice (105,080 delivery-only customers). Parser spec for later in the doc. | Account score (A2), diagnosis |
| X5 | Triggers and a ranking dry run | 8 triggers (5 strong); 46 of 107 non-partner accounts have one active; next action: 25 call now, 12 nurture, 34 watch, 36 hold; 14 of the top 15 sit on the metro growth rings; stable to ±0.05 weight shifts (ρ ≥ 0.993). | /accounts, /accounts/[id] |

## Notes

- **One story across Q1, Q5, Q7 and X1.** The 2026 peak (91.1 GW) was 20.9 GW under the 112 GW preliminary, yet
  7 GW over what trend and weather predict (11 GW over a pre-2020 fit). X1 shows the excess is flat load, and ERCOT's
  large-load reports explain a third to half of it: the official forecast overshoots by taking requests at face
  value, while the load that does arrive is real and under-reported.
- **Follow-up that unblocks the best account signal:** `eia_861` should also parse `Short_Form_<year>.xlsx` (861S);
  X4 has the spec and shows the gain (EIA customers 57% → 99% of accounts). It is a pipeline change, so it waits
  until Airflow work resumes.
- **Queue scale for the Explorer:** from queue entry, only 8.3% of projects (2.8% of MW) reach COD within 36 months;
  X2 turns it into the county map and backtests it.
- Every figure lives in `analysis/out/` (gitignored); re-run the scripts to regenerate them.
