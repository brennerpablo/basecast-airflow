# X2: the adjusted generation queue by county

Exploration X2, after phase 0. Run on 2026-09-26 against production Postgres (`basecast_reader`), GIS through the
**2026-08** report (as-of date = end of that month, 2026-09-01).

- Script: `analysis/x2_adjusted_queue.py` (`uv run --group analysis python analysis/x2_adjusted_queue.py`)
- Model code: `basecast_pipelines/models/queue_adjusted.py`, on top of `models/survival.py` (imported, unchanged);
  tests in `tests/models/test_queue_adjusted.py` (conditional-CIF and semi-Markov math by hand, as-of truncation,
  stage assignment, scoring; no DB)
- Outputs (gitignored): `analysis/out/x2_county_adjusted.csv` (county grain), `x2_project_scores.csv` (project
  grain), `x2_zone_fuel_adjusted.csv`, `x2_backtest_projects.csv`, `x2_eia860m_match.csv`, `x2_backtest.png`,
  `x2_county_raw_vs_adjusted.png`

**Backtest refreshed on 2026-09-26 after the X16 #2 fix.** The reporting-lag guard now applies (`survival.load_events()`
did not load `ia_first_month` / `synchronization_first_month`, so backdated IAs leaked into the as-of fits). Statewide
backtest error moved from −12.5% / +9.4% / +1.8% to −13.2% / +9.3% / +1.4% (mean absolute 7.9% → 8.0%). Today's queue
numbers (§1, §2) are unchanged.

## Method in one paragraph

Each active project gets P(COD by the horizon | its stage, months already spent there, no exit yet). **IA-stage**
projects: the q6 Aalen–Johansen CIF from the IA landmark, conditioned at the elapsed time,
`(F(e+h) − F(e)) / S(e)`. **Entry-stage** projects (no IA yet, including FIS approved without an IA) are scored
**semi-Markov**. From entry, the time to IA is fit as a competing risk against COD and withdrawal, with delayed
entry. The chance of signing the IA in `(e, e+h]` is then convolved with the IA-stage CIF from 0 months. Curves are
MW-weighted, with strata solar / storage / wind / gas+other (the pooled curve is used when a stratum fails
n ≥ 30 / ≥ 10 events). The clock is clamped where fewer than 10 projects remain at risk: past that point, an older
project gets the last supported `h`-month conditional rate (12.9% of projects at the Dec 2027 horizon, 23% at
Dec 2028). Expected MW = Σ capacity × p. The backtest selected this variant (§3); the first-pass variants are kept
for comparison.

## 1. The active queue today (Aug 2026 report)

**1,810 active projects, 438,262 MW** (plus 168 inactive, 36,669 MW, not in the raw queue). Capacity is present
for all; county → FIPS matches 100%. 54 projects (25.6 GW) are in counties without an ERCOT weather zone in
`county_weather_zone` (e.g. Deaf Smith, Chambers, Gray).

Stage = the latest milestone reached (a later stage wins even when its date is earlier).

| Stage | solar | storage | wind | gas+other | total n | total MW | median months at stage |
|---|---:|---:|---:|---:|---:|---:|---:|
| entry (no FIS approval, no IA) | 332 / 82,175 | 585 / 111,405 | 73 / 28,666 | 102 / 62,057 | 1,092 | 284,303 | 23.9 |
| FIS approved, no IA | 28 / 4,692 | 67 / 10,900 | 2 / 246 | 3 / 736 | 100 | 16,575 | 10.1 |
| IA signed, not synchronized | 199 / 50,929 | 207 / 34,584 | 46 / 14,416 | 35 / 16,918 | 487 | 116,848 | 17.9 |
| synchronized, no COD yet | 57 / 12,515 | 30 / 3,145 | 37 / 4,803 | 7 / 74 | 131 | 20,536 | 18.8 |
| **total** | 616 / 150,312 | 889 / 160,034 | 158 / 48,131 | 147 / 79,785 | **1,810** | **438,262** | |

(cells: projects / MW). The primary model uses two stages: **entry** = the first two rows (1,198 projects, 301 GW),
**IA** = the last two (612 projects, 137 GW).

Top 20 counties by raw MW (194 counties have an active project):

| # | County | n | raw MW | | # | County | n | raw MW |
|---|---|---:|---:|---|---|---|---:|---:|
| 1 | Pecos | 56 | 20,717 | | 11 | Harris | 31 | 6,732 |
| 2 | Brazoria | 57 | 11,697 | | 12 | Deaf Smith | 11 | 6,594 |
| 3 | Wilbarger | 21 | 8,506 | | 13 | Matagorda | 28 | 6,355 |
| 4 | Wharton | 41 | 8,503 | | 14 | Zapata | 27 | 6,291 |
| 5 | Reeves | 24 | 8,301 | | 15 | Lamar | 24 | 6,054 |
| 6 | Milam | 29 | 8,286 | | 16 | Cameron | 28 | 5,979 |
| 7 | Jack | 17 | 7,979 | | 17 | Ellis | 26 | 5,867 |
| 8 | Hill | 23 | 7,874 | | 18 | Upton | 18 | 5,649 |
| 9 | San Patricio | 27 | 6,856 | | 19 | Freestone | 22 | 5,357 |
| 10 | Mitchell | 15 | 6,792 | | 20 | Borden | 17 | 5,257 |

## 2. Adjusted queue: expected MW reaching COD by Dec 2027 (16 months) and Dec 2028 (28 months)

**Statewide: 438,262 MW raw → 38,689 MW by Dec 2027 (8.8%) → 70,394 MW by Dec 2028 (16.1%).**

| Group | raw MW | adj. Dec 2027 | ratio | adj. Dec 2028 | ratio |
|---|---:|---:|---:|---:|---:|
| stage entry | 301,059 | 923 | 0.3% | 7,624 | 2.5% |
| stage IA signed | 137,202 | 37,766 | 27.5% | 62,770 | 45.8% |
| storage | 160,034 | 13,428 | 8.4% | 24,395 | 15.2% |
| solar | 150,312 | 10,773 | 7.2% | 20,659 | 13.7% |
| gas+other | 79,785 | 9,068 | 11.4% | 16,627 | 20.8% |
| wind | 48,131 | 5,421 | 11.3% | 8,714 | 18.1% |
| CDR WEST | 141,330 | 10,442 | 7.4% | 19,353 | 13.7% |
| CDR NORTH | 111,113 | 8,768 | 7.9% | 17,373 | 15.6% |
| CDR SOUTH | 103,558 | 9,521 | 9.2% | 16,882 | 16.3% |
| CDR COASTAL | 38,435 | 4,834 | 12.6% | 8,240 | 21.4% |
| CDR PANHANDLE | 28,582 | 2,712 | 9.5% | 4,878 | 17.1% |
| CDR HOUSTON | 15,243 | 2,413 | 15.8% | 3,669 | 24.1% |
| WZ FWEST | 65,326 | 4,542 | 7.0% | 8,959 | 13.7% |
| WZ NCENT | 63,986 | 4,951 | 7.7% | 8,996 | 14.1% |
| WZ SOUTH | 59,884 | 4,187 | 7.0% | 7,988 | 13.3% |
| WZ COAST | 48,879 | 7,188 | 14.7% | 11,904 | 24.4% |
| WZ WEST | 46,984 | 2,578 | 5.5% | 4,848 | 10.3% |
| WZ NORTH | 46,236 | 5,854 | 12.7% | 9,684 | 20.9% |
| WZ SCENT | 42,509 | 4,574 | 10.8% | 7,629 | 17.9% |
| WZ EAST | 38,841 | 2,708 | 7.0% | 6,409 | 16.5% |
| WZ none (county outside the zone map) | 25,616 | 2,107 | 8.2% | 3,978 | 15.5% |

By stage × fuel (Dec 2028 ratio): IA-stage gas+other 66.6%, storage 60.5%, wind 43.9%, solar 31.9%; entry-stage
gas+other 8.5%, storage 1.3%, wind 1.0%, solar 0.5%. Almost all adjusted MW (89% by Dec 2028) comes from projects that
already have an IA.

**Where the map changes.** Raw vs adjusted (Dec 2028) Spearman over 194 counties is 0.78, and only **13 of the top
20** counties stay in the top 20. The adjusted map is *more* concentrated: the top 10 counties hold 28.5% of adjusted
MW vs 21.8% of raw MW.

- Big raw queue, little adjusted MW (raw ≥ 4 GW): **Mitchell** 6,792 → 116 MW (1.7%; rank 10 → 117), **Ellis**
  5,867 → 429 (7.3%; 17 → 56), **Zapata** 6,291 → 487 (7.7%; 14 → 51), **Nolan** 4,441 → 368 (8.3%; 28 → 64),
  **Pecos** 20,717 → 1,767 (8.5%; still #5, down from #1), **Jack** 7,979 → 753 (9.4%; 7 → 27), **San Patricio**
  6,856 → 851 (9 → 23), **Cameron** 5,979 → 645 (16 → 40).
- Adjusted MW well above the county's raw rank: **Bastrop** 5,098 → 2,562 (50%; rank 23 → 2), **Ward** 3,325 →
  1,565 (47%; 46 → 7), **Haskell** 3,976 → 1,280 (32%; 36 → 10), **Refugio** 2,034 → 942 (46%; 76 → 15),
  **Wheeler** 1,300 → 862 (one project; 104 → 22), **Chambers** 1,069 → 754 (114 → 26), **Fort Bend** 2,008 → 747
  (77 → 29), **Galveston** 2,943 → 881 (52 → 21).
- Top 5 adjusted (Dec 2028): Brazoria 3,694, Bastrop 2,562, Wharton 2,538, Wilbarger 2,295, Pecos 1,767 MW.
- **Caveat on the gainers:** several are driven by a few large IA-stage gas projects (Ward: BasRanch 1,350 MW;
  Bastrop: three gas projects of 880–1,104 MW; Wheeler: Moon Hammer 1,300 MW; Wilbarger: Limitless Energy Hub I/II),
  each scored 0.66 by Dec 2028. See review item 2.

## 3. Backtest: the queue as it stood, 24 months ahead

For each past report month the queue is the `gis_snapshots` rows with status active (capacity, county and milestones
as printed then). The curves are fit on the cohort truncated at the as-of date: events after it are censored, and
IA / synchronization milestones first reported after it are hidden. "Actual" = the as-of listed MW of the projects
whose ERCOT COD falls within the next 24 months. Baselines: the raw queue, and the developers' own `projected_cod`
within the window.

| Queue of | projects | raw MW | **predicted** | **actual** | **error** | developer projected COD | raw |
|---|---:|---:|---:|---:|---:|---:|---:|
| Jun 2022 | 881 | 174,846 | 13,833 | 15,940 | **−13.2%** | 121,064 (+660%) | +997% |
| Jan 2023 | 1,066 | 215,786 | 20,140 | 18,428 | **+9.3%** | 141,880 (+670%) | +1,071% |
| Aug 2024 | 1,614 | 330,882 | 28,845 | 28,460 | **+1.4%** | 145,631 (+412%) | +1,063% |

**Statewide mean absolute error 8.0% (mean −0.9%, worst 13.2%).** The actual falls within ±1.2 Bernoulli standard
deviations (z = +1.2, −0.8, −0.2). That band is sampling noise only; it leaves out model error.

By fuel (predicted vs actual):

| Fuel | Jun 2022 | Jan 2023 | Aug 2024 | pooled error | mean abs error |
|---|---|---|---|---:|---:|
| solar | 4,932 / 6,173 (−20%) | 7,867 / 7,949 (−1%) | 11,433 / 12,541 (−9%) | −9.1% | 10.0% |
| storage | 4,159 / 3,581 (+16%) | 6,282 / 5,531 (+14%) | 11,252 / 11,544 (−3%) | +5.0% | 10.8% |
| wind | 4,110 / 4,409 (−7%) | 3,572 / 3,385 (+6%) | 3,834 / 2,373 (+62%) | +13.3% | 24.6% |
| gas+other | 632 / 1,778 (−64%) | 2,419 / 1,563 (+55%) | 2,326 / 2,002 (+16%) | +0.6% | 45.1% |

By stage: IA-stage −20.6%, −1.1%, −4.9%; entry-stage +56%, +151%, +108%, on small MW (2.4–3.3 GW predicted vs
1.3–1.6 GW actual).

**Counties (the map test).** The Spearman correlation of predicted vs actual county MW is 0.66 / 0.64 / 0.64. The raw
queue gets 0.43 / 0.44 / 0.45, and the developer projected COD 0.48 / 0.51 / 0.56. County-level WAPE is 83% / 86%
/ 72% (57–84 counties had any COD in the window). The adjusted map ranks counties clearly better than the raw one, but
a single county's number is noisy.

**Variants tried** (mean absolute statewide error over the three dates):

| Variant | error |
|---|---:|
| **entry + IA, semi-Markov entry, MW-weighted (primary)** | **8.0%** |
| + synchronized stage, semi-Markov, MW | 35.6% |
| entry + IA semi-Markov, count-weighted | 37.8% |
| entry + IA landmarks, marginal entry curve, MW (the q6 proposal as is) | 48.2% |
| entry + FIS + IA, MW | 57.0% |
| entry + FIS + IA + sync, MW | 93.4% |
| count-weighted versions of the above | 70.6–133% |

- **Why the q6 landmark model fails at entry:** the marginal entry curve counts the COD of projects that had already
  signed an IA. An entry-stage project has not, so the entry stage came out 5–11× too high (+512%, +830%, +1,130%).
  The semi-Markov step fixes that.
- **MW beats count weighting** because big projects convert less.
- **A "synchronized" stage overpredicts** (+94%, +130%, +3%). Many synchronized projects stay active for more than
  24 months without COD (21 / 27 / 42 of them at the three dates). Not verified why: phased CODs are a candidate.

Caveat: the primary variant was chosen on these same three dates, out of 10 variants. The semi-Markov fix came from a
stage-level diagnostic, not a search, but there is no held-out date. The windows of the first two dates overlap by 17
months, and **Dec 2028 (28 months) is beyond the 24-month window tested**.

## 4. External check: GIS COD vs EIA-860M operating generators

This checks the COD event against EIA-860M: 421 GIS projects with an ERCOT COD in 2019–2025 (55,226 MW), against the
2026-08 EIA-860M TX operating generators (2,486), aggregated by plant × fuel. Candidates share the county FIPS and
fuel group. **Strong** = a shared distinctive name token and capacity within ×0.67–1.5; **weak** = a shared token
only, or capacity within ±10% only.

| Fuel | n | strong | strong or weak |
|---|---:|---:|---:|
| storage | 177 | 72.7% | 92.7% |
| solar | 113 | 70.3% | 89.4% |
| wind | 78 | 47.4% | 85.9% |
| gas | 37 | 11.1% | 78.4% |
| other | 16 | 0.0% | 18.8% |
| **all** | 421 | **58.7%** (247) | **86.5%** (364) |

Timing (strong matches): EIA first operating month minus ERCOT COD month has a median of **0 months**, with 68% within
±3 months (p10 −12, p90 +1). The early tail is mostly later phases matched to the first phase's EIA plant (e.g. Texas
Solar Nova 2 → Nova 1). Renewables and storage agree well. Gas and "other" match poorly by name: EIA names the host
plant, GIS names the unit or the project. **Not verified** beyond this name/capacity heuristic: the weak matches can
be coincidences, and the 57 unmatched (e.g. BlueBell Solar, High Lonesome Wind, Tavener Solar) were not checked by hand.

## Headline for the video (candidate)

> "ERCOT's generation queue lists 438 GW. Our model expects about **39 GW to reach commercial operation by the end of
> 2027 and 70 GW by the end of 2028**, roughly 1 MW in 6. Run from past queue snapshots, the same method landed
> **within 13.2% of what actually got built over the next two years** (three snapshots, 2022–2024). Developers' own COD
> dates came to 5–8× what got built."

Caveats to keep with it: the backtest covers 24-month windows on three snapshots, which also picked the model. Dec 2028
lies beyond the tested window. 8.3 GW of the Dec 2027 number is IA-stage gas, much of it large projects (≥ 880 MW) that are outside
the history the curves were fit on.

## Proposed decisions.md lines

- 2026-09-26 — The Explorer's adjusted queue = Σ capacity × P(COD by the horizon | stage, months at the stage): IA-stage projects use the MW-weighted Aalen–Johansen CIF from the IA, conditioned at the elapsed time ((F(e+h) − F(e)) / S(e)); entry-stage projects (no IA, including FIS approved) are scored semi-Markov (time to IA from entry, convolved with the IA curve). — Backtest from the 2022-06, 2023-01 and 2024-08 queues, 24 months ahead: statewide error −13.2% / +9.3% / +1.4%, vs +48% mean for the plain landmark curves and 5–8× for developer COD dates.
- 2026-09-26 — Adjusted-queue curves are MW-weighted, and FIS approval and synchronization are not stages. — Count weighting overpredicts (mean error 38% vs 8%); adding FIS approval or synchronization as a stage made the backtest worse (57% and 36%).
- 2026-09-26 — Past the curve's support (fewer than 10 projects at risk) the clock is clamped so the horizon window ends at the last supported time. — The step CIF is flat past its last event, which would give old projects a probability of 0.
- 2026-09-26 — [pending Pablo] The Explorer shows Dec 2027 and Dec 2028 (end-of-year horizons from the latest GIS report), with the backtest error as the credibility note; horizons beyond 24 months are labelled "beyond the backtested window". — Only 24-month windows were backtested.

## Review items for Pablo

1. **Accept the semi-Markov entry stage** as the A4 / Explorer model, replacing the q6 proposal of entry + IA
   landmarks with marginal curves. The q6 proposal overpredicts entry-stage MW 5–11× in the backtest.
2. **Large new gas.** 9.1 GW of the Dec 2027 number and 16.6 GW of the Dec 2028 number are gas+other. The IA-stage
   median is 217 MW today vs 102 MW in the training data, and only 2 of 16 gas+other projects of ≥ 500 MW with an IA in
   the cohort reached COD (40 of 79 smaller ones did). Most of the big ones are recent and censored. Options: a size
   split (≥ 500 MW) for gas, a cap tied to turbine lead times, or leaving it with a footnote. This moves the map most
   in Bastrop, Ward, Wheeler and Wilbarger. Claude's default: flag it in the UI now and add the size split if time allows.
3. **Near-term rate.** The model implies ~29 GW/yr of COD over the next 16 months vs 14.2 GW/yr realized over
   Aug 2024 – Aug 2026. This is consistent with the IA-stage queue growing from 51.6 GW (Jun 2022) to 60.8 (Jan 2023),
   87.3 (Aug 2024) and 137.2 GW today (realized COD has run at ~15–16% of the IA-stage MW per year). Whether today's
   bigger IA queue can convert at historical rates (supply chain, interconnection work) is **not verified**.
4. **Counties outside the weather-zone map.** 54 active projects (25.6 GW) sit in counties with no ERCOT weather
   zone (Deaf Smith, Chambers, Gray…). Should the map show them, and under which zone?
5. **Actual MW = as-of listed capacity**, not as-built capacity. Training weights use the latest listed capacity (a
   small look-ahead); the backtest queue uses the snapshot's.
6. **Uncertainty band.** The Bernoulli band (±1.7–2.5 GW) is too narrow to show as P10/P90. A band from the backtest
   errors (±13%) or a bootstrap over projects and curves is still to do.
7. **Wind at the latest date** (+62%) and **gas+other** in every date (mean abs 45%) are the weak strata.

## Useful for the core features

**Explorer map (choropleth by FIPS).** Proposed mart `queue_adjusted_county`: one row per `county_fips` ×
`as_of_report_month`. Columns:

- keys and labels: `county_fips`, `county_name`, `weather_zone`, `cdr_reporting_zone` (the modal one);
- counts: `projects`, `projects_ia`;
- raw: `raw_mw`, `raw_mw_ia` (IA-stage), `raw_mw_<fuel>` for solar / storage / wind / gas_other;
- adjusted: `adj_mw_<yyyy>` for year-end horizons (2026–2030), `adj_mw_<yyyy>_<fuel>`, `ratio_<yyyy>`;
- `rank_raw`, `rank_adj`, `model_version`.

~200 rows per month. The metric toggle on the map is raw / adjusted / ratio, and "change" is `rank_raw − rank_adj`.
`analysis/out/x2_county_adjusted.csv` is a prototype with Dec 2027 / Dec 2028 and the per-fuel Dec 2028 split.

**Project grain (drill-down and filters).** Proposed mart `queue_project_scores`: one row per active `inr`. Columns:
`project_name`, `county_fips`, `stratum`, `stage`, `stage_date`, `elapsed_months`, `capacity_mw`, `projected_cod`,
`p_cod_<yyyy>`, `mw_<yyyy>`, `clamped`, `curve` (own stratum or pooled). Prototype:
`analysis/out/x2_project_scores.csv` (1,810 rows). With that table in memory, the API can re-aggregate by any
filter (fuel, stage, zone) in Polars in milliseconds.

**Credibility panel.** The backtest table of §3 (statewide and by fuel) and the county Spearman (0.64–0.66 adjusted vs
0.43–0.45 raw), as a small static mart `queue_backtest` with `report_month`, `stratum`, `pred_mw`, `actual_mw`,
`developer_projected_mw` and `raw_mw`.

**Pipeline.** Monthly after the GIS parser: `truncate_at(cohort, as_of)` → `fit_stage_curves(..., fit_stages("entry_ia_sm"))`
→ `build_queue(snapshot, events, as_of, ...)` → `score` → `aggregate`. Seconds for the full queue (numpy only, no
lifelines).
