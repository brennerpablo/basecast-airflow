# X10 — City-level (Census place) signals for the municipal utilities (2026-09-26)

Script: `analysis/x10_muni_places.py` (`uv run --group analysis python analysis/x10_muni_places.py`). Logic:
`basecast_pipelines/models/muni_places.py` (parsers, crosswalk, place signals, city trigger mappings; pure functions),
tested in `tests/models/test_muni_places.py` (tiny synthetic inputs, no network, no DB). Outputs (gitignored):
`analysis/out/x10_sources.csv` (URL, fetch time, bytes, sha256), `x10_crosswalk.csv`, `x10_signals.csv`,
`x10_muni_trigger_events.csv`, `x10_score_ranks.csv`, `x10_run.log`; Census files cached in
`analysis/out/x10_cache/`. As of **2026-09-26**. Analysis only: no source module, parser, DAG, `data/` write or DB
write. The DB was read through `read_sql()`; the overlap was computed server-side by PostGIS in read-only queries.

**Validation lock.** Same hold-out as Q3 / X5 / X9: the five partner accounts are matched by name and removed
**before** anything is computed. Every number below covers the **107** other accounts; the muni numbers cover **59**
munis (Austin Energy is held out). No partner module or partner file is read.

**Headline.** Every muni maps to its city (59/59, each confirmed by the polygon). Place data fixes the *size*
reading (owner-occupied single-family homes per EIA meter: 0.03 apportioned → 0.42 by place; ρ with meters 0.77 →
0.95), but it does **not** fix the *growth* reading: cities grow past their muni's wires (Georgetown city +56% in
2020→2025, its meters +4.3% a year), so place population growth tracks meter growth no better than the county
(ρ 0.53 vs 0.54 on the same 2020→2024 window). A place-level permit trigger lifts munis with an active strong
trigger from **10 to 13** of 59. Recommendation: add the place files narrowly (diagnosis facts + muni permit
trigger), keep the score on X4's signals, and show munis ranked within type.

## Sources (URLs confirmed by fetching them; fetch times UTC)

| File | URL | Fetched | Use |
|---|---|---|---|
| `sub-est2025_48.csv` (280 KB) | https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025_48.csv | 2026-09-26 18:35 | PEP V2025 city/town population, July 1 2020–2025 |
| `SUB-EST2025.pdf` | https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2025/SUB-EST2025.pdf | 2026-09-26 18:38 | layout: SUMLEV 162 = incorporated place, 157 = county place part; boundaries as of 2025-01-01 for every year |
| `so2022a.txt` … `so2025a.txt` (~0.69 MB each) | https://www2.census.gov/econ/bps/Place/South%20Region/so2025a.txt (same folder, `so<yyyy>a.txt`) | 2026-09-26 18:36 | BPS annual permits by place (South region; Texas rows kept), imputed and reported units |
| `so2508y.txt`, `so2608y.txt` | https://www2.census.gov/econ/bps/Place/South%20Region/so2608y.txt (and `so2508y.txt`) | 2026-09-26 18:36 | BPS year to date Jan–Aug 2026 and Jan–Aug 2025 by place |
| `cb_2024_48_place_500k.zip` (1.7 MB) | https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_48_place_500k.zip | 2026-09-26 18:36 | place polygons for the crosswalk check |
| `acsdt5y2024-b25032.dat` (already in the lake, `census_acs` dt=2026-09-25) | https://www2.census.gov/programs-surveys/acs/summary_file/2024/table-based-SF/data/5YRData/acsdt5y2024-b25032.dat | 2026-09-26 04:31 (lake manifest) | ACS 2024 5-year B25032 by place: the file already holds 1,863 Texas places (GEO_ID `1600000US48…`); the parser keeps counties only |

- **The ACS API needs a key now.** `https://api.census.gov/data/2024/acs/acs5?get=NAME,B01003_001E&for=place:*&in=state:48`
  answered 302 to `https://api.census.gov/data/missing_key.html` with header `X-DataWebAPI-KeyError: 1` (2026-09-26
  18:36 UTC). `CENSUS_API_KEY` is not set. The summary file above needs no key and was already downloaded, so ACS
  B01003 was not needed (PEP gives population). The API's variable metadata
  (`https://api.census.gov/data/2024/acs/acs5/groups/B25032.json`) did answer without a key; it confirms lines 1–4
  (total; owner-occupied; owner 1-unit detached; owner 1-unit attached).
- BPS place files are regional. Texas is in `South Region/`. Each file has two header rows, and the grouped labels
  (`1-unit`, …) sit above the *Units* column; the parser reads them from the file. A county's unincorporated area
  has FIPS place `99990` (Texas counties do report permits there: 53 such rows in 2025).

## 1. Crosswalk munis → places

Rule: the core name of the PUCT name (`accounts.core_name`, the Q2 rule), else the EIA utility name from the Q2
crosswalk, equals the core of an incorporated place name (`Boerne city` → `boerne`); a core shared by two places
never matches. Then the check: PostGIS intersects each CCN territory (`puct_ccn_territories.geom`, SRID 4326) with
every incorporated place polygon whose bounding box touches a muni (173 candidates; NAD83 read as WGS84, a sub-metre
difference for areas).

| Result | Munis |
|---|---|
| Name match | **59 / 59** (57 by the PUCT name; 2 by the EIA name: CPS Energy → San Antonio city, BTU Rural Electric Division → Bryan city) |
| Name match = the place with the largest overlap | **59 / 59** |
| Confirmed (≥ 50% of the place in the territory, or ≥ 50% of the territory in the place) | **59 / 59** (no partial, no rejected, no miss) |

How well the city describes the territory (`fit`, shares by area):

| Fit | Munis | Meaning | Examples |
|---|---|---|---|
| `same` (both shares ≥ 50%) | 31 | city ≈ territory | Garland (63% / 99%), College Station, Lubbock, Denton, Bastrop |
| `territory_larger` (territory in place < 50%) | 21 | the muni serves well past the city limits: place data under-counts it | CPS (33% of its territory is San Antonio city), Kerrville (14%), Floresville (2%), BTU Rural (9%), NBU (25%), San Saba (8%) |
| `city_larger` (place in territory < 50%) | 7 | the territory sits inside a bigger city, part of which other utilities serve: place data over-counts it | Granbury (23% of the city is in the CCN), Seguin (28%), Brownsville (34%), San Marcos (36%), Boerne (48%), Sanger, Robstown |

Median share of a muni's territory inside its city: 0.65 (p10 0.24, p90 0.96). Area shares are not population
shares: the city limits hold most of the people even where the territory runs into rural land (not measured here;
it would need block population).

## 2. Place signals vs apportioned signals vs EIA meters

Place signals use Q3's definitions on the city: `pop_growth` PEP 2020→2025 (July 1), `permits_per_1k` BPS 2023–2025
imputed units per 1,000 residents of 2025, `owner_sf_homes` / `owner_sf_share` ACS 2024 5-year B25032 lines 3+4 over
line 1. Meters are X4's wires customers (long + 861S + delivery-only), 2024. Coverage on the 59 munis: population,
growth and housing 59/59; permits 55/59 (Goldsmith, Goldthwaite, Moulton, Waelder have no BPS row 2023–2025).

**Size: place fixes it.** Ratios per EIA meter, median [p10–p90]:

| Ratio per meter | Munis, county-apportioned | **Munis, place** | Co-ops, county-apportioned |
|---|---|---|---|
| owner-occupied single-family homes | 0.03 [0.01–0.26] | **0.42 [0.29–0.73]** | 0.80 [0.52–1.59] |
| occupied housing units | 0.05 [0.01–0.50] | **0.75 [0.53–1.08]** | 1.32 [0.86–2.80] |
| residents | 0.16 [0.04–1.43] | **2.02 [1.34–3.49]** | 3.55 [2.32–8.23] |

(n = 58 munis with meters; Waelder has none.) Occupied units per meter by fit: `same` 0.79 [0.63–1.03] (n 30),
`city_larger` 1.15 [0.98–1.41] (7), `territory_larger` 0.61 [0.49–0.78] (21), exactly the direction the polygon
predicts. 42 of 58 munis land between 0.6 and 1.2 occupied units per meter (meters also count businesses, so
slightly under 1 is expected). Boerne: 173 apportioned owner-SF homes (0.03 per meter) → **4,785** in Boerne city
(0.73 per meter, 6,542 meters). Georgetown 6,114 → 24,162 (33,066 meters); Seguin 759 → 7,402 (9,970 meters).

Spearman among the munis:

| Pair | County | Place | n |
|---|---|---|---|
| meters × owner-SF homes | 0.765 | **0.951** | 58 |
| meters × population | 0.767 | **0.952** | 58 |
| meter CAGR 2019→24 × pop growth 2020→25 | 0.420 | 0.486 | 55 |
| meter CAGR 2020→24 × pop CAGR 2020→24 (same window) | 0.538 | 0.532 | 57 |
| meter CAGR 2019→24 × permits per 1k | 0.379 | 0.482 | 52–54 |
| county value × place value (pop_growth; permits_per_1k; owner_sf_share) | 0.845; 0.809; **0.500** | | 55–59 |

**Growth: place does not fix it.** On the same window, place population growth tracks meter growth no better than
county growth (0.53 vs 0.54), and it is further from it in level: median |pop CAGR − meter CAGR| 1.06 pp for the
place vs 0.95 pp for the county; the place is closer for 22 of 57. The reason is the non-obvious part: **fast cities
grow past their muni's wires.** Georgetown city +55.6% (2020→2025) vs meters +4.3% a year (≈ +23% over five years);
Farmersville city +48.4% vs meters −0.3% a year; Seguin +39.2% vs +3.1% a year; Bastrop +36.7% vs +1.3% a year. PEP
holds boundaries fixed (2025-01-01) for every year, so this is not annexation; the new homes sit inside the city but
on land another utility serves (dual certification; which utility is not verified). Meter growth (X4) remains the
growth signal for munis.

- Medians, munis: meter CAGR 2019→24 1.1% a year; pop growth 2020→25 7% (county) and 7% (place, p90 32% vs 22%);
  permits per 1k 11.7 (county) vs 15.2 (place); owner-SF share 0.63 (county) vs 0.59 (place: cities have more
  apartments than their counties, ρ only 0.50 between the two readings; which one describes the territory better is
  not verified).

## 3. Triggers for munis

X5's five strong triggers, recomputed with X5's code (as of 2026-09-26, active = last 365 days): **10 of 59 munis**
have one active (NBU dc_permit; CPS gen_storage_ia; Boerne, Brownsville, Garland, Georgetown, Granbury
dev_agreement via city Ch. 380; Caldwell market_registration; BTU and College Station county permit_surge).

| City mapping tested | Events ever (munis) | Active munis | New munis with an active strong trigger |
|---|---|---|---|
| TCEQ data-center site `city` = the muni's city | 13 (4: CPS 9 sites, Garland 2, Georgetown 1, NBU 1) | NBU (Cloudburst, 2025-10-22) | **0**: 10 of the 13 are already in X5's county events, and only 10 of the 38 new sites since 2025 carry a city |
| Ch. 312 abatement whose city taxing unit is the muni's city (`taxing_units`, else `local_government_name`) | 58 (9 munis: Brenham, Greenville, Seguin, Lubbock, Granbury, …) | none (latest 2024-05-02) | **0** now |
| `permit_surge` on the city's own BPS series: Jan–Aug 2026 vs Jan–Aug 2025, ≥ +25% and ≥ 50 units | 9 | BTU, Bridgeport, Burnet, Caldwell, Castroville, College Station, Garland, NBU, Sanger | 4 (Bridgeport, Burnet, Castroville, Sanger) |
| the same, monthly reporters only (8 of 8 months in both years) | 6 | Bridgeport, Burnet, Caldwell, Castroville, Garland, NBU | **3** (Sanger is not in the monthly sample: its YTD is Census imputation; BTU and College Station reported 7 of 8 months in one year) |

- **Munis with an active strong trigger: 10 → 13** of 59 with the reporters-only place surge (14 without the
  reporter filter, 16 at ≥ 20 units). Examples: Bridgeport 17 → 134 units, Burnet 49 → 131, Castroville 48 → 61,
  Garland 149 → 440, NBU 669 → 1,206, College Station 630 → 1,578.
- Statewide, 70 of 712 Texas places with a prior-year count surge on this rule.
- **The rest of X5's county triggers stay out of reach for munis**: generation IAs (GIS projects carry a county
  only), TPIT lines (county endpoints) and county Ch. 312/380 need coordinates, which the sources do not have.
- Ch. 380 by city is already X5's rule; the Ch. 312 city rule adds history (9 munis) but nothing active today.

## 4. Score effect and recommendation

**Score dry run** (X4's proposed weights; munis take the place value of `pop_growth`, `permits_per_1k` and
`owner_sf_share`, co-ops keep the county ones; size and meter growth are X4's EIA signals for everyone):

- Spearman with the county-rate score over the 107: **0.949**.
- Munis in the top 25: 8 → 7; median muni rank 67 → 71; munis in the top half 24 → 24.
- Largest moves are small munis: Castroville 64 → 28, Caldwell 78 → 56, Weimar 96 → 75 up; Llano 47 → 84,
  Whitesboro 31 → 67, Bartlett 42 → 76 down.
- Ranked among munis only, the top 10 share 8 names under both variants (NBU, Georgetown, CPS, Greenville, Boerne,
  Seguin, Denton, Lockhart).

So place data does not rescue munis in a mixed ranking: X4 already showed they rank low because they are small, and
place growth over-reads cities that outgrow their wires.

**Recommendation: add the place files, narrowly; do not change the score.**

1. **Diagnosis facts for munis** from the city, labelled "City of X (Census place)", not "territory": population and
   growth, owner-occupied single-family homes and share, permits per 1,000. They replace today's apportioned numbers
   (173 homes for Boerne), which are wrong by ~25×. Show the fit badge (`same` / `city_larger` / `territory_larger`)
   next to them.
2. **`permit_surge` for munis from the city's BPS series** (same months year to date vs the prior year, ≥ +25%,
   ≥ 50 units, monthly reporters only), in addition to the county rule. +3 munis with an active strong trigger now.
3. **Ch. 312 by the muni's city** as a rule change (no new source): history for 9 munis, fires when a new one lands.
4. **Score:** keep X4's signals (eia_customers for size, eia_customer_cagr for growth). Treat munis by **ranking
   within type** in `/accounts` (a type filter with its own rank), not by swapping their rates.
5. **Not worth it:** TCEQ city mapping (no gain), the ACS API (needs a key; the summary file is enough).

**Spec, if approved** (extend the existing census sources; no new source id needed):

| Piece | Source file (discover from the folder, never construct) | Grain | Columns | Refresh |
|---|---|---|---|---|
| `census_pep` + place totals | `popest/datasets/<period>/cities/totals/sub-est<vintage>_48.csv` | place × year (SUMLEV 162; keep 157 for county parts) | `place_fips`, `place_name`, `funcstat`, `series`, `year`, `measure` = population, `value` | yearly (V2025 released May 2026 per the layout PDF) |
| `census_bps` + places | `econ/bps/Place/South Region/so<yyyy>a.txt` (annual), `so<yymm>y.txt` (year to date) | permit office × period (Texas rows) | `place_fips` (null for 99990), `bps_id`, `county_code`, `period_type` annual/ytd, `year`, `month`, `months_rep`, `units_total`, `units_1`, `units_total_rep` | monthly (YTD), yearly (annual); release lag not verified |
| `census_acs` parser | the same `acsdt5y<year>-b25032.dat` already fetched | place × line | `place_fips`, `vintage`, `table_id`, `line`, `estimate`, `moe` (same shape as `census_housing_county`) | yearly |
| `census_tx_counties_geo` + places | `geo/tiger/GENZ<year>/shp/cb_<year>_48_place_500k.zip` from the cartographic-boundary page | place | `place_fips`, `name`, `lsad`, `geom` (PostGIS) | yearly |
| crosswalk | computed (`muni_places.match_places` + `classify_overlap`), reviewed into `config/` like `utility_crosswalk.yaml` | muni | `ccn_no`, `place_fips`, `matched_on`, `place_in_territory`, `territory_in_place`, `fit` | when PUCT or the places change |

Size: ~0.7 MB per BPS regional file (4 annual + 2 YTD needed now), 0.3 MB PEP, 1.7 MB polygons.

## Proposed findings row

| X10 | 59/59 munis match their city by name (57 PUCT, 2 EIA), each the best spatial place and confirmed by the CCN polygon (fit: 31 same, 21 territory larger, 7 city larger). Place fixes size: owner-SF homes per meter 0.03 → 0.42 (occupied units 0.05 → 0.75), ρ with meters 0.77 → 0.95. Place does not fix growth: ρ with meter CAGR 0.53 vs 0.54 (same window); cities outgrow their muni's wires (Georgetown city +56% vs meters +4.3%/yr). City-mapped triggers: TCEQ city adds 0, Ch. 312 by city 0 active (9 munis ever), place permit surge +3 → munis with an active strong trigger 10 → 13 of 59. Score with place rates: ρ 0.949, munis in top 25 8 → 7 | add place files for the diagnosis and the muni permit trigger; score unchanged; munis ranked within type | A2 | — |

## Proposed decisions.md lines

- 2026-09-26 — Munis are crosswalked to Census incorporated places by core name (PUCT name, else EIA name) and checked
  against the CCN polygon (≥ 50% of the place in the territory or of the territory in the place). — X10: 59 of 59
  match, each is the place with the largest overlap; CPS Energy and BTU need the EIA name.
- 2026-09-26 — The muni diagnosis shows city facts (PEP population and growth, ACS owner-occupied single-family
  homes and share, BPS permits) labelled as the city with a fit badge, instead of area-apportioned county numbers. —
  X10: apportionment gives 0.03 owner-SF homes per meter, the city 0.42 (ρ with meters 0.77 → 0.95; Boerne 173 →
  4,785 homes).
- 2026-09-26 — The account score keeps X4's signals for munis (EIA meters and meter growth); place rates do not
  replace the county ones, and `/accounts` ranks munis within type. — X10: place growth tracks meter growth no
  better (ρ 0.53 vs 0.54) because cities grow past their muni's wires, and swapping the rates moves the order little
  (ρ 0.949, munis in top 25 8 → 7).
- 2026-09-26 — `permit_surge` also fires for a muni from its city's BPS place series (Jan–month year to date vs the
  same months a year earlier, ≥ +25%, ≥ 50 units, monthly reporters only). — X10: +3 munis with an active strong
  trigger (10 → 13 of 59); non-reporters' YTD is imputation only.
- 2026-09-26 — Ch. 312 abatements whose city taxing unit is the muni's city map to that muni by name; TCEQ site cities
  are not used. — X10: Ch. 312 by city gives 9 munis a history (none active); only 10 of 38 new TCEQ sites carry a
  city and the one muni hit was already mapped by county.

## Review items for Pablo

1. **Approve the narrow scope**: place files for the diagnosis and the muni permit trigger, score unchanged
   (default). Alternative: munis on place `permits_per_1k` and `owner_sf_share` in the score (not `pop_growth`),
   which lifts a few small munis (Castroville 64 → 28) without changing the top.
2. **Ranking within type** for munis in `/accounts` (default yes). It is a presentation choice; X4 and X10 both say
   the mixed ranking puts munis low because they are small and grow slowly by meters.
3. **Thresholds are Claude's**: the 50% overlap rule and the fit classes, the monthly-reporter filter, and reusing
   X5's +25% / 50 units on a year-to-date window (8 months instead of 12). At ≥ 20 units two more munis fire
   (Bowie, Smithville).
4. **City larger than the territory** (7 munis: Granbury, Seguin, Brownsville, San Marcos, Boerne, Sanger, Robstown):
   city facts over-count them (1.15 occupied units per meter). Show the badge (default) or scale by meters?
5. **Territory larger than the city** (21 munis, e.g. Floresville 2% of its territory in the city, Kerrville 14%):
   the city is a floor. BTU Rural Electric Division maps to Bryan city by its EIA name; confirm that the CCN and the
   EIA id describe the same system (the 68,685 meters suggest the whole BTU, not the rural division only; not
   verified).
6. **Census API key**: the ACS API now refuses keyless calls. Not needed for this plan; get a free key only if
   another table is wanted by API.
7. **Unverified**: which utility serves the growth in fast cities outside their muni's wires (Georgetown,
   Farmersville, Seguin, Bastrop); whether the TCEQ `city` is the jurisdiction or the mailing city; BPS monthly
   release lag.
