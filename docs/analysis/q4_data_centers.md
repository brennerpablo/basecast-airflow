# Q4 — Do the new data centers land in co-op territory? (2026-09-26)

Spec: `docs/PHASE0_ANALYSIS.md` §2 Q4. Script: `analysis/q4_data_centers.py`
(`uv run --group analysis python analysis/q4_data_centers.py`). Logic: `basecast_pipelines/models/data_centers.py`,
tested in `tests/models/test_data_centers.py`. Site table: `analysis/out/q4_sites.csv` (gitignored). No figure:
none would change the decision.

**The attribution is by county, not by point.** `tceq_data_center_sites` has no coordinates (the parser drops
street addresses), so each site takes the territory mix of its whole county from `county_utility_overlap_puct`.
A site inside a city or next to a substation can sit in a different utility than its county's largest one.

## 1. Site selection

Filter from the decisions log: TCEQ AIRNSR rows whose site name matches `DATA ?CENTER|DATACENTER|DATA CTR` or
whose NAICS is 518210, one row per site (RN), dated by the earliest known `affil_begin_dt`.

| Check | Result |
|---|---|
| Sites in `tceq_data_center_sites` | 88 |
| First known permit on or after 2025-01-01 | **38 sites in 27 counties** (matches the decisions log) |
| Of those, with an undated row too (`has_undated_affiliation`) | 1: Project Llano, Armstrong County (RN112229901, first dated row 2025-06-11). It is the 38th site: the first count read the 1800-01-01 placeholder as its earliest date |
| Sites with no dated row at all (left out, cannot be placed) | 2: ALE62 (Bexar), Temple Data Center RN111448668 (Bell) |
| Matched by name / by NAICS 518210 only | 15 / 23 |

The meaning of `affil_begin_dt` (application date or affiliation start) is still not verified.

## 2. Territory type (co-op / muni / IOU)

Two ways, per the spec:

- **Largest area:** the type with the most summed `overlap_km2` in the county.
- **county_share-weighted:** each type's summed `county_share` divided by the county's total. PUCT CCN
  territories overlap (dual certification), so the raw shares of the 38 sites' counties sum to 1.00–1.74
  (median 1.23). Without the normalization, the co-op share clipped to 1 gives an upper bound.

| Type | Sites, largest area | Share | Sites, weighted | Share |
|---|---|---|---|---|
| Co-op | 19 | 50.0% | 17.7 | **46.6%** |
| Muni | 0 | 0.0% | 0.8 | 2.1% |
| IOU | 19 | 50.0% | 19.5 | 51.4% |
| Unclassified | 0 | — | 0 | — |
| **Total** | 38 | | 38 | |

Cuts:

| Cut | Sites | Co-op, largest area | Co-op, weighted |
|---|---|---|---|
| All new sites | 38 | 50.0% | 46.6% |
| Upper bound (raw co-op `county_share` clipped to 1) | 38 | — | 57.8% |
| Counties whose dominant ISO class includes ERCOT (`county_iso_share`) | 31 in 22 counties | 51.6% | 47.5% |
| Name-matched sites only (drops the NAICS-only matches) | 15 | — | 42.4% |
| Non-metro sites (density rule below) | 28 | — | 51.8% |

Seven sites are in counties that are not mainly ERCOT: Hardin (MISO), El Paso ×2 (WECC), Gray, Deaf Smith and
Wilbarger ×2 (SPP). The split is regional: the co-op sites cluster in the Rolling Plains, the Panhandle edge and
the Central Texas ring (Haskell, Fisher, Hardeman, Floyd, Nolan, Shackelford, Bosque, Hill, Milam, Medina, Tom
Green); the IOU sites cluster in the Permian (Ward, Midland, Pecos, Howard, Mitchell), Bell, El Paso and Dallas,
plus Hardin, Gray and Hunt.

Point-level hint: 10 of 38 sites have a city. Only one city names a muni of its county: Cloudburst Data Centers
in New Braunfels (Comal), counted as co-op by county but probably in New Braunfels Utilities (not verified).

## 3. Metro vs non-metro

**The "34 outside the big metros" is reproducible.** It is not written anywhere in the repo: it came from a
one-off query in the Claude Code session of 2026-09-26 (06:35 UTC), which counted sites whose TCEQ county name
was not in a hard-coded list of 13 counties: Dallas, Tarrant, Collin, Denton, Ellis, Harris, Bexar, Travis,
Williamson, Fort Bend, Rockwall, Kaufman, Hays. The list is now `LEGACY_METRO_COUNTIES` in the model. It is an
ad hoc list, not an OMB/MSA definition: it leaves out e.g. Bell (Killeen–Temple), Lubbock, El Paso, Montgomery
and Galveston.

- With the old selection (undated rows read as 1800-01-01): **37 sites in 26 counties, 34 outside**, the same
  as the decision.
- With the current selection: **35 of 38 outside** (92.1%). The 3 metro sites are in Dallas (2) and
  Williamson (1).

Cross-check by density, because the list is ad hoc: Census PEP vintage 2025 population (2025) ÷ land area
(`tx_counties.aland_m2`). Explicit threshold: **≥ 100 people per km² of land (~259/sq mi) is metro**, which
takes the 29 densest Texas counties.

| Metro rule | Metro counties (TX) | New sites outside metro |
|---|---|---|
| 13-county list (the decision's) | 13 | 35 / 38 (92.1%) |
| Density ≥ 100/km² (fallback, explicit) | 29 | 28 / 38 (73.7%) |
| Density ≥ 50/km² (sensitivity) | 42 | 25 / 38 |
| Density ≥ 250/km² (sensitivity) | 13 | 33 / 38 |

The density rule adds Bell (147/km², 4 sites), El Paso (334/km², 2) and Comal (144/km², 1). An MSA-based rule
is not possible from the database: there is no county→CBSA crosswalk table (not verified otherwise).

Sites that are both co-op (largest area) and non-metro: 17 of 38 (density rule), 18 of 38 (13-county list).

## 4. Site list

Type = largest-area type of the county. Co-op w = normalized co-op `county_share`. Metro L = 13-county list,
metro D = density ≥ 100/km².

| First permit | Site (TCEQ name) | County | Type | Co-op w | ISO class | Pop/km² | Metro L | Metro D | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 2025-02-04 | TEMPLE 5MW DATA CENTER | Bell | iou | 0.42 | ERCOT | 147 | no | yes | |
| 2025-05-29 | NEXUS DATA CENTER HUBBARD | Hill | coop | 0.62 | ERCOT | 16 | no | no | |
| 2025-06-11 | PROJECT LLANO | Armstrong | coop | 0.81 | ERCOT,SPP | 1 | no | no | undated row; NAICS only |
| 2025-06-20 | QUANTUM EXPEDITIONS SILSBEE FACILITY | Hardin | iou | 0.40 | MISO | 25 | no | no | NAICS only |
| 2025-07-01 | DFW10 DFW11 DFW17A DFW17B DFW17C | Bosque | coop | 0.72 | ERCOT | 7 | no | no | NAICS only |
| 2025-08-15 | THELMA SITE | Haskell | coop | 0.74 | ERCOT,SPP | 2 | no | no | NAICS only |
| 2025-08-27 | BARBER LAKE DATA CENTER PROJECT | Mitchell | iou | 0.05 | ERCOT | 4 | no | no | |
| 2025-10-02 | EL PASO DATACENTER - EGUS | El Paso | iou | 0.16 | WECC | 334 | no | yes | |
| 2025-10-10 | CIRCE PERMIAN DATA CENTER | Ward | iou | 0.00 | ERCOT | 5 | no | no | |
| 2025-10-22 | CLOUDBURST DATA CENTERS | Comal | coop | 0.59 | ERCOT | 144 | no | yes | city New Braunfels (muni) |
| 2025-11-25 | STREAM DFWC1 | Dallas | iou | 0.05 | ERCOT | 1177 | yes | yes | NAICS only |
| 2025-12-04 | CINCO SITE | Medina | coop | 0.89 | ERCOT | 17 | no | no | NAICS only |
| 2025-12-22 | COLOVORE AUSTIN 1 | Williamson | coop | 0.50 | ERCOT | 260 | yes | yes | NAICS only |
| 2026-01-06 | VANTAGE DATA CENTERS TX3 | Shackelford | coop | 0.65 | ERCOT | 1 | no | no | |
| 2026-01-15 | TEMPLE DATA CENTER | Bell | iou | 0.42 | ERCOT | 147 | no | yes | |
| 2026-02-05 | JOURNEY SITE | Haskell | coop | 0.74 | ERCOT,SPP | 2 | no | no | NAICS only |
| 2026-02-20 | WURLDWIDE | El Paso | iou | 0.16 | WECC | 334 | no | yes | NAICS only |
| 2026-03-05 | HARVEY DFM | Midland | iou | 0.00 | ERCOT | 81 | no | no | NAICS only |
| 2026-03-09 | CIRCE ENERGY DATA CENTERS E MONAHANS CAMPUS | Ward | iou | 0.00 | ERCOT | 5 | no | no | |
| 2026-03-09 | POOLSIDE DATA CENTER PECOS COUNTY II | Pecos | iou | 0.50 | ERCOT | 1 | no | no | |
| 2026-05-01 | MEITNER ENERGY CENTER | Gray | iou | 0.18 | SPP | 9 | no | no | NAICS only |
| 2026-05-05 | PLATON SITE | Wilbarger | coop | 0.59 | SPP | 5 | no | no | NAICS only |
| 2026-05-06 | MILAM COUNTY DC | Milam | coop | 0.65 | ERCOT | 10 | no | no | NAICS only |
| 2026-05-13 | JAKE FACILITY | Midland | iou | 0.00 | ERCOT | 81 | no | no | NAICS only |
| 2026-05-18 | WOODROW DATA CENTER & POWER PLANT | Nolan | coop | 0.58 | ERCOT | 6 | no | no | |
| 2026-06-10 | LBB120 HORIZON JUNCTION TECHNOLOGY PARK | Floyd | coop | 0.82 | ERCOT,SPP | 2 | no | no | NAICS only |
| 2026-06-12 | WESTLINE 2335 | Tom Green | coop | 0.72 | ERCOT | 31 | no | no | NAICS only |
| 2026-06-15 | ROMAN ENERGY CENTER | Deaf Smith | coop | 0.90 | SPP | 5 | no | no | NAICS only |
| 2026-06-15 | STAMPEDE DATA CENTER | Bell | iou | 0.42 | ERCOT | 147 | no | yes | |
| 2026-06-19 | SCHLACHTER REALTY | Dallas | iou | 0.05 | ERCOT | 1177 | yes | yes | NAICS only |
| 2026-07-10 | ROYSE CITY DATA CENTER | Hunt | iou | 0.41 | ERCOT | 57 | no | no | |
| 2026-07-10 | CEMCO-403 & 365 CBP CLOCKTOWER DATA CENTER | Hardeman | coop | 0.75 | ERCOT,SPP | 2 | no | no | |
| 2026-07-23 | KMT AZURITE ENERGY | Pecos | iou | 0.50 | ERCOT | 1 | no | no | NAICS only |
| 2026-08-12 | SAT14 | Medina | coop | 0.89 | ERCOT | 17 | no | no | NAICS only |
| 2026-08-12 | BLUESTEM TECHNOLOGY PARK | Bell | iou | 0.42 | ERCOT | 147 | no | yes | NAICS only |
| 2026-09-14 | VKU SITE | Wilbarger | coop | 0.59 | SPP | 5 | no | no | NAICS only |
| 2026-09-16 | MIDAS COMPUTE - BIG SPRING FACILITY | Howard | iou | 0.00 | ERCOT | 13 | no | no | NAICS only |
| 2026-09-18 | CEMCO-348 CBP SWEETWATER 2 DATA CENTER | Fisher | coop | 0.80 | ERCOT,SPP | 2 | no | no | |

## Decision

Rule: ≥ 60% of sites (weighted) in co-op territory **and** most outside the metros → in the video; otherwise
signal only.

- Co-op, weighted: **46.6%** (17.7 of 38). Below 60% under every cut: largest area 50.0%, ERCOT counties
  47.5%, name-matched only 42.4%, and even the un-normalized upper bound, 57.8%.
- Outside the metros: yes, 35/38 (13-county list) or 28/38 (density ≥ 100/km²).

**→ Signal only.** The data centers do go outside the big metros, but they split about evenly between co-op and
IOU counties (the Permian sites are IOU), so "they land in co-op territory" does not hold at county level. The
finding stays as a county signal in the account score and stays out of the video script.

## Proposed findings row

| Q | Result (numbers) | Decision | FINAL_SPRINT task affected | Verified |
|---|---|---|---|---|
| Q4 | 17.7 of 38 new sites (first permit since 2025, 27 counties) in co-op territory weighted by county_share = 46.6% (largest area 19/38 = 50%; ERCOT counties 47.5%; upper bound 57.8%); outside metro: 35/38 (the decision's 13-county list, reproduced: 34/37) or 28/38 (PEP density ≥ 100/km²); by county, not by point | signal only | A2, video | — |

## Review items for Pablo

1. **Metro definition.** The "34" used an ad hoc 13-county list. Keep it, or switch to density ≥ 100/km²? The
   density rule moves Bell (4 sites, Killeen–Temple), El Paso (2) and Comal (1) into metro.
2. **NAICS-only matches.** 23 of 38 sites match only by NAICS 518210, and some names do not read as data
   centers (MEITNER ENERGY CENTER, ROMAN ENERGY CENTER, KMT AZURITE ENERGY, SCHLACHTER REALTY, QUANTUM
   EXPEDITIONS). An older row carries 518210 on LUFKIN PAPER MILL, so the code is not clean. Keep them in the
   signal, or keep only name matches (15 sites)?
3. **Non-ERCOT sites.** 7 sites sit in counties that are mainly SPP, MISO or WECC. Proposal: drop them from the
   ERCOT-facing account signal.
4. **A weaker line for the video?** "35 of 38 new data-center air permits since 2025 are outside the big
   metros" holds, but the handoff rule sends Q4 to "signal only". Using that line anyway is your call.
5. **County vs point.** Only 10 of 38 sites have a city, and one of them (Cloudburst, New Braunfels) is likely
   in a muni that the county attribution counts as co-op. Whether the raw TCEQ Central Registry carries
   coordinates is not verified; if it does, a point-in-CCN test would replace the county attribution.

## Proposed decisions.md lines

- 2026-09-26 — Q4: new data-center sites stay a county signal in the account score and out of the video script. — 46.6% of the 38 sites since 2025 are in co-op territory weighted by county_share (50% by largest area, 57.8% upper bound), below the 60% bar; they split about evenly between co-op and IOU counties.
- 2026-09-26 — The "big metros" of the TCEQ data-center count are the 13 counties of the earlier query (Dallas, Tarrant, Collin, Denton, Ellis, Harris, Bexar, Travis, Williamson, Fort Bend, Rockwall, Kaufman, Hays), now `LEGACY_METRO_COUNTIES` in `models/data_centers.py`, with PEP density ≥ 100 people/km² of land reported next to it. — The list reproduces "34 of 37" but is ad hoc; the density rule adds Bell, El Paso and Comal.
- 2026-09-26 — Data-center sites are attributed to utility types by county, not by point, with `county_share` normalized by the county's own sum. — `tceq_data_center_sites` has no coordinates, and PUCT CCN territories overlap (shares sum to up to 1.74).

## Useful for the core features

- **Explorer map layer:** a county choropleth "new data-center air permits since 2025" (count per county) from
  `select_new_sites`, with a toggle for NAICS-only matches. It shows the move away from the metros (35/38)
  without claiming the utility.
- **Account trigger:** "new TCEQ data-center air permit in a county you serve", dated by the first permit and
  weighted by the account's `county_share` in that county. It is event-shaped (a date, a site name), which suits
  the next-action rules better than a static score term. Label it "by county, not by point".
- **Score signal (Q3/A2):** new sites per account, weighted by territory overlap; it covers only 27 counties,
  so check it against Q3's ≥ 80% coverage rule before it enters the score (it will likely be mostly zeros).
- **Video:** drop "co-op territory". If Pablo wants a line, the one that holds is "outside the big metros",
  with the Permian (IOU) vs Rolling Plains (co-op) split as the honest nuance.
