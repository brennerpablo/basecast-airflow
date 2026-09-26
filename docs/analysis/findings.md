# Phase 0 findings — 2026-09-26, 12:40–12:59 CDT (setup f750659 to findings d7be30e)

Partner mode: **validation** (Pablo delegated the choice to Claude at 12:35 CDT; Claude chose validation because it
can still turn into look-alike later, while look-alike can never go back to validation). Q3 held the five partner
accounts out of every statistic; `tests/test_partner_lock.py` keeps `analysis/` away from `base_public_facts`.

Plan and rules: `docs/PHASE0_ANALYSIS.md`. Per-question detail (tables, definitions, caveats):
`docs/analysis/qN_*.md`. Scripts: `analysis/qN_*.py` (`uv run --group analysis python analysis/<script>`), logic in
`basecast_pipelines/models/`. `docs/FINAL_SPRINT.md` was not available, so the task column names the tasks as the
phase 0 handoff does. Every decision below follows the handoff's rule; where Claude proposes to deviate, the
row says so and the item waits in the review queue.

## Start here

**Decide first** (each unblocks a task; the full queue below has 20 items):

1. **Score weights, Q3 or X4** (item 1). A2 cannot reveal the partners until one set is approved.
2. **Spot-check the 10 deck values** (item 3, `docs/large-load-spot-check.md`, ~10 minutes). Q5, X1, X7 and X11 all
   rest on the machine-read large-load values.
3. **Queue model for A4** (item 4): Aalen–Johansen with X2's semi-Markov entry stage, instead of cohort rates.
4. **Forecast scenarios for A3** (items 5 and 13): the organic baseline (fit ≤ 2019) and which large-load deck
   (March vs June 2026) drives 2027–2030.
5. **Pitch wording** (item 6 and `docs/analysis/video-candidates.md`): the record was 85.5 GW until July 2026, and
   the "> 20 GW" miss has an 866 MW margin.

**Strongest findings so far:**

- The 2026 peak (91.1 GW) was 20.9 GW under ERCOT's 112 GW preliminary but 7–11 GW over trend and weather. The excess
  is flat, always-on load, and ERCOT's own large-load reports explain only a third to half of it (Q1, Q7, X1, X12).
- A three-layer forecast (organic + realized large load + flat layer), rebuilt as of 8 past dates, beat LTLF and CDR
  on the same cells: MAPE 3.3% vs 5.1% and 4.8% (X7, with its leaks listed).
- The generation queue's 438 GW becomes ~39 GW by the end of 2027; a 24-month backtest from three past queues lands
  within 13%, while developers' dates add up to 5–8× what got built (X2).
- Since 2021 the 4CP interval is no longer when power is scarce: a battery aimed at 4CP (15:45–17:45) misses the
  evening price spikes (X3).

**Git state:** pushes to `basecast-airflow` paused from `214b906` while the long `puct_filings process` run was
alive (each push redeploys the Airflow VM and restarts the scheduler, which killed that run 9 times during this session, 10 times today); it
finished at 19:27 UTC and the waiting commits went up in one push. **Reproducible:** all 19 scripts in `analysis/`
re-ran from scratch at 14:30 CDT without errors (~3 minutes in total) and left every tracked file unchanged.

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
11. **X3 — interval label and dollars.** *Closed by X15:* ERCOT's official 4CP intervals (NP9-83-M) match the D&E
   15-min peaks in 66 of 68 months 2008–2025 (all of 2022–2025), and the workbook labels the time "Interval
   ending": the 15:45–17:45 window stands. The ERCOT postage-stamp rate is $68.547/kW-yr for 2025 (Docket 57491,
   final) and $75.527/kW-yr for 2026 (Docket 59080, order on remand pending); summer Y's 4CP sets year Y+1's bills
   (16 TAC 25.192). Caveat: the rate is quoted verbatim from the PUCT matrix, but TCOS ÷ average 4CP from the
   same table gives 2–10% less (67.21 for 2025); the matrix's denominator was not checked. Use the quoted rate.
12. **X5 — triggers.** Keep "storage or gas ≥ 50 MW signs its IA" as a strong trigger (25 call-now) or demote it to
   context (17)? Data centers count in both the score and a trigger (default: keep both). Every trigger threshold
   is Claude's (list in `docs/analysis/x5_triggers.md`).
13. **X7 — which large-load deck drives the forecast.** The March 2026 deck (before ERCOT's April "Batch Zero"
   intake) gives 111.6 GW for 2030, the June 2026 deck 132.9 GW, and the 2023–25 approval pace 96.4 GW. Default: show
   the three as scenarios, stop at 2030. The band covers 61% of cells in a nominal 80%: calibrate it on the 18
   backtest cells and say so. *X15:* ERCOT has not published a final LTLF 2026 (only the April preliminary; final
   Batch Zero classifications are due in December), so there is nothing to add.
14. **X9 — accounts API and munis.** Key accounts by `ccn_no`, not the EIA `utility_id`; hold `is_base_partner` until
   A2 (these are basecast-get-data changes, not made). Munis: 55 of 59 never reach the 20% county rule (X10 tests
   city-level Census data as the fix). Show "call now until <date>" when a trigger lapses (the #1 lapses on
   2026-10-22).
15. **X10 — munis.** Add Census place files narrowly (muni diagnosis facts and a muni permit trigger, score
   unchanged) and rank munis within type in `/accounts` (defaults). Confirm BTU (CCN "Rural Electric Division")
   and the EIA id describe the same system (68,685 meters suggest all of BTU).
16. **X11 — large-load zones.** Accept the Batch Zero base load as the zone shape of the approved stock and the
   f = 0.72 NORTH share of LZ_WEST (from two machine-read charts; f from 0 to 1 moves NORTH between 1.1 and 2.5 GW)?
   The G&T co-ops in the TSP filing are sized in X13 (45.7 GW for 2030, 22.9%; see item 19). Weight TCEQ sites by recency (a 1987 permit counts as much as
   a 2026 one today).
17. **X8 — marts and video lines.** `marts-proposal.md` is the input for A0 and for basecast-get-data's contract
   (nothing changed there); `video-candidates.md` feeds `docs/video-numbers.md` once the items above are settled.
18. **X12 — the normalized series.** Normal weather = ERA5 2003–2022 (the only 20 years in the DB). *X15 confirmed*
   the Uri exclusion (ERCOT's rotating outages ran Feb 15 01:20 – Feb 18 00:42, 2021; X12 drops Feb 14–20) and the
   NERC holiday list (the weekend rule's text was not verified). Open: on each summer's 5 hottest days the actual
   peak runs ~315 MW under what the weather predicts (4CP curtailment? not verified); "the normalized peak rose every
   year" holds at P50, but some steps sit inside the P10–P90 band (2019 → 2020: +0.8 GW).
19. **X13 — G&T large-load exposure.** Keep it a context fact (81 of 107 accounts) and let LCRA-supplied munis keep
   the fact but not the trigger (73 → 33; a `triggers.py` change, not made)? Count LCRA as public power (22.9%
   co-op) or with the co-ops as ERCOT does (25.1%)?
20. **X14 — acquisition zones.** The kickoff never defined "priority acquisition zones"; X14 reads them as a county
   priority with two channels (retail-direct in competitive IOU territory, partnership in co-op/muni territory).
   Confirm the reading, the multiplicative form (market × grid) and that channel shares go by land (homes would be
   better); the Lubbock and Nueces retail opt-ins are not verified.

## Beyond phase 0 — explorations X1–X15

Run after the seven questions closed, each aimed at a core feature. Detail in `docs/analysis/xN_*.md`; every number
is a candidate, not yet in `docs/video-numbers.md`.

| X | Question | Headline | Feeds |
|---|---|---|---|
| X1 | Is the peak excess over weather the large loads? | Against a pre-2020 model, the summer peak ran 6.9 / 8.1 / 7.1 / 11.2 GW high in 2023–2026; the overnight minimum rose as much as the peak (flat load); the decks' observed large loads cover a third to half. Split for the Forecast: pre-break organic + 0.51 × approved stock + a flat unattributed layer (2026: 89.1 vs 91.1 GW, −2.3%). Found and fixed a misdated deck in Q5 (ratios unchanged). | Forecast (A3), video |
| X2 | Raw vs adjusted generation queue by county | 438 GW active (1,810 projects) → ~38.7 GW reaching COD by Dec 2027 (8.8%) and 70.4 GW by Dec 2028; 24-month backtests from 2022, 2023 and 2024 queues miss by −12.5%, +9.4%, +1.8%, while developers' projected COD dates add up to 5–8× what got built; county ranking beats the raw queue (Spearman 0.64–0.66 vs 0.43–0.45). | Explorer map, A4, video |
| X3 | 4CP timing and the offer to co-ops | A 2-hour discharge 15:45–17:45 covered 63 of 64 CPs (2010–2025); catching all four with a weather-based call takes ~56 days a summer and near-peak days doubled since 2023; since 2021 no CP fell in its month's top-20 priced intervals and the net-load peak moved to HE 20–21. | Accounts next action / offer, video |
| X4 | EIA-861 short form | Reading 861S from the lake takes EIA customers from 57% to 99.1% of accounts (munis 27% → 98%); customers replace apportioned homes; Lubbock's −95% is retail choice (105,080 delivery-only customers). Parser spec for later in the doc. | Account score (A2), diagnosis |
| X5 | Triggers and a ranking dry run | 8 triggers (5 strong); 46 of 107 non-partner accounts have one active; next action: 25 call now, 12 nurture, 34 watch, 36 hold; 14 of the top 15 sit on the metro growth rings; stable to ±0.05 weight shifts (ρ ≥ 0.993). | /accounts, /accounts/[id] |
| X6 | Do the video numbers hold from the raw files? | All six re-derived without the models or parsers: 2026 peak 91,133.7 MW (HE 18, 2026-07-22), old record 85,508.5 MW, 112 GW and 90.5–98 GW (PUCT 58777 item 38), 232,500 MW tracked and 3.78% approved (Jan 2026), 1,810 projects / 438,262 MW (Aug 2026 GIS), Q5 deck inputs. Careful: the "> 20 GW" margin is 866 MW; the large-load queue reached ~438 GW by May 2026, the same figure as the generation queue. | video |
| X7 | Does a three-layer peak forecast beat ERCOT? | Rebuilt as of 8 dates (18 cells): MAPE 3.3% vs LTLF 5.1% and CDR 4.8% (the design itself was chosen in 2026, a leak the doc lists). From the March 2026 deck: 93.0 [89.4–96.7] GW in 2027 and 111.6 [104.2–123.7] in 2030, vs LTLF 2025 104.3 and 138.9. After Batch Zero the MW promised by 2027 went from 67 to 201 GW while approved stayed at 8.8–8.9 GW. | Forecast, /backtest (A3), video |
| X8 | What the screens need, and which lines can go in the video | `marts-proposal.md`: marts per screen (grain, key, columns, the `models/` function, blocking review item). `video-candidates.md`: lines ranked by strength with source, X6 status and caveat; top: 26.8 GW promised by end-2025 vs 8.8 GW approved; 112 vs 91.1 GW; the May 2026 backtest (88.9 GW vs 91.1). Its re-check fixed an X2 cell (+670%, 5–8×). | A0, get-data contract, video |
| X9 | What the /accounts/[id] page can show | `assemble(ccn_no)` renders score breakdown, triggers with lapse dates, territory, EIA series and next action as facts with source and as-of. Gaps: no public source for an account's own 4CP load (the UtilityDataSource case), munis miss county triggers, apportionment misreads size for 69 accounts. | /accounts/[id], API shape |
| X10 | Do Census place data fix the munis? | All 59 non-partner munis match a place. Size: homes per meter 0.03 → 0.42, ρ with meters 0.77 → 0.95. Growth: no (ρ 0.53 vs 0.54), cities grow onto land their muni does not serve. Strong triggers for munis 10 → 13 of 59. | /accounts diagnosis, muni ranking |
| X11 | Where are the large loads? | Weather-zone charts exist (Batch Zero Update Sep 2026, QSA Q4 2026, LLIS May 2026); LZ_WEST = FWEST + WEST + 0.72 × NORTH. Approved stock leans NORTH 2.5, FWEST 1.7, NCENT 1.4, WEST 1.2 GW; the pipeline leans NCENT, NORTH, SOUTH. By zone, ~10 GW moves between zones in the 2030 forecast (total ±0.1%). | Forecast by zone, Explorer |
| X12 | A weather-normalized load series for the Forecast | Daily model per zone (rolling 3-year fits, normal = ERA5 2003–2022): ERCOT daily MAPE 2.9% energy / 4.0% peak with the level known (8.0% / 10.7% without weather). Normalized energy grew 2.1%/yr in 2010–21, then 4.4–5.4%/yr in 2022–25. Normalized summer peak rose every year 2012–2026 (64.8 → 91.1 GW) while the raw peak fell in 7; 2025's 83.7 GW was a mild summer (88.0 normal); 2026 was weather-neutral. | Forecast series, video |
| X13 | How much of the large-load wave lands with co-ops? | Of 199.5 GW requested for 2030 in the TSP filing, co-op G&Ts hold 45.7 GW (22.9%; Brazos 26.6, Golden Spread 16.6), ~1.9× the co-ops' 12.3% of ERCOT retail MWh, up from 8.8% in 2026. No location below the TSP, so no allocation to members: a context fact for 81 of 107 accounts. Q4 stays signal only. | /accounts context, video |
| X14 | What are the Explorer's "priority acquisition zones"? | A county priority from territory signals only: market (homes, growth, permits, owner share) scaled by grid value (normalized zone peak growth, flat large-load pressure, load-zone price spread, new data centers), split by channel: 161 partnership, 39 retail-direct, 4 mixed counties. 17 of the top 20 on the metro growth rings; rural NORTH/WEST counties rise on grid value; stable to ±0.05 weights (ρ ≥ 0.996). | Explorer map |
| X15 | Close the factual review items with primary sources | 4CP label confirmed (66/68 months vs ERCOT NP9-83-M); postage-stamp rate $68.547/kW-yr (2025) and $75.527/kW-yr (2026, pending); no final LTLF 2026 exists; Uri dates and NERC holidays confirmed. Every URL fetched on 2026-09-26. | X3 offer in dollars, X7, X12 |

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
- **ERCOT paused approvals to energize large data centers and crypto on 2026-08-03.** Source: ERCOT "Large Load
  Issues" (https://www.ercot.com/files/docs/2026/08/25/10.-Large-Load-Issues.zip, p. 2): "August 3, 2026 - Issued a
  Market Notice pausing the Batch Zero study process and delaying final classification of all large loads. Paused
  approvals to energize data centers or virtual currency mining facilities (crypto facilities) that are 75 MW or
  greater", in response to Governor Abbott's August 3 directive; the PUCT presentation of 2026-08-20 (p. 7) lists 17
  large loads (6,608 MW) affected through the end of 2026. It explains why the approved stock sat at 8.8–8.9 GW all of
  2026 (X7) and biases Q5's partial 2026 ratio (0.28) low: read 2026 as a policy pause, not as realization.
- **The 4CP offer in dollars (X3 + X15, arithmetic only).** Each MW a fleet takes off a co-op's load in all four 4CP
  intervals lowers that co-op's wholesale transmission charges by about $68,547 a year at the 2025 postage-stamp rate
  ($75,527 at the pending 2026 rate), billed the year after the summer. Caveats: it is the co-op's avoided cost, not
  Base's revenue; it assumes every interval is hit (a 15:45–17:45 discharge hit 63 of 64 in 2010–2025, ~56 dispatch
  days a summer with a weather-based call); the fleet's kW per home is not verified.
- Every figure lives in `analysis/out/` (gitignored); re-run the scripts to regenerate them.
- **X16 review (2026-09-26, `docs/analysis/x16_models_review.md`):** 6 confirmed bugs. Fixed: Uri days now normalize the fitted day in `weather_normalized.py` (X12's ERCOT 2021 energy was 2.2 TWh low; "2.1%/yr in 2010–21" becomes 2.2%, 2022 YoY +5.0% (z 6.0) becomes +4.4% (z 3.9), "5–7 SD" becomes 3.9–5.7 SD; the X12 doc still shows the old numbers) and `DC_` ties left out of `four_cp.zone_coincidence` (< 0.1 pp). Open: `survival.load_events` skips `ia_first_month` (X2 backtest −13.2/+9.3/+1.4% after the fix), `peak_forecast` month-end dates (X7 2027 ≈ 92.8 GW, 2030 ≈ 111.3 GW), two latent NaN cases in `triggers.py`.
- **Paused to save credits:** X17–X23 stopped mid-run; their partial files stay uncommitted in the working tree.
