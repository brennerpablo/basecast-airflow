# Phase 0 findings — 2026-09-26, 12:40–13:45 CDT

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

1. **Q3 — approve the score weights** (`config/account_score.yaml`). Once approved, A2 reveals the partners.
   Related: the near-constant threshold (mode ≥ 90%) is Claude's; at 60% `dc_sites` would drop out. Munis rank
   low on size because county data is apportioned by area (default: accept until EIA customers land).
2. **Q2 — the 5 crosswalk rows under 90** (CPS Energy, CoServ, PenTex, BTU, Comanche): all proposed accept, each
   pinned by ≥ 99.8% county overlap. Table in `docs/analysis/q2_universe.md`.
3. **Q5 — spot-check** `docs/large-load-spot-check.md` (10 values with the ERCOT link and page). More than 1 miss →
   plan B (three scenarios). Also confirm the ratio definition (incremental as headline?) and that the
   "realized" side is approved-to-energize, not energized (energized ≈ 65% of approved in Jan 2026).
4. **Q6 — override the rule?** Claude proposes Aalen–Johansen with the entry and IA-signed landmarks instead of
   cohort rates: the ordering check assumed FIS approval comes before the IA, which is false for most wind
   projects, and cohort rates are blind to the 2022+ cohorts. Also confirm COD (not synchronization) as the event
   and the cut where fewer than 10 projects remain at risk.
5. **Q7 — which spread sets the A3 band** (default: one-year-ahead rolling errors, not the in-sample SD) and what
   "organic" means (default: fit ≤ 2019 for the flagged zones and ERCOT, post-2020 excess shown as realized large
   load, so the large-load layer is not counted twice). NORTH's 2009 drop and 2022+ jump and WEST's +33% in 2026 are
   not explained yet (exploration X1).
6. **Q1 — pitch wording and product context.** Say "preliminary long-term forecast"; ERCOT's own April 2026 range
   held the actual. The "demand record of ~87–91 GW" in Jan 2026 (CLAUDE.md, KICKOFF §1) is wrong: the record then
   was 85,508 MW (2023-08-10); it was broken on 2026-07-22 at 91.1 GW (preliminary).
7. **Q4 — metro definition** (13-county list vs density ≥ 100/km²), whether to keep the 23 sites matched only by
   NAICS 518210 (some names are not data centers), dropping the 7 non-ERCOT sites from the ERCOT-facing signal, and
   whether "35 of 38 outside the big metros" can go in the video even though Q4 is "signal only".
8. **Thresholds** adopted as in the handoff: Q7 5% / 10%, Q1 "consistent sign" = ≥ 2/3 of vintages, Q5 ±5%.

## Notes

- **One story across Q1, Q5 and Q7.** The 2026 peak (91.1 GW) was 20.9 GW under the 112 GW preliminary, yet 7 GW over
  what trend and weather predict. Every hold-out year since 2023 ran above the model, with a break around 2020.
  Exploration X1 tests whether that excess is the large loads the ERCOT decks report as energized (≈ 5.7 GW in Jan
  2026).
- **Follow-up that unblocks the best account signal:** `eia_861` should also parse `Short_Form_<year>.xlsx` (861S).
  46 of 106 ERCOT co-ops and munis file only that form; with it, EIA customers cover 99% of the universe and replace
  the area-apportioned homes signal.
- **Queue scale for the Explorer:** from queue entry, only 8.3% of projects (2.8% of MW) reach COD within 36 months.
  Exploration X2 turns this into the raw vs adjusted MW map by county, with a backtest.
- Every figure lives in `analysis/out/` (gitignored); re-run the scripts to regenerate them.
