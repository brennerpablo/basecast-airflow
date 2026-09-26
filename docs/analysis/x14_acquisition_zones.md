# X14 — Priority acquisition zones by county (Explorer)

Exploration after X13. Run on 2026-09-26 against production Postgres (`basecast_reader`, read-only):
`tx_counties`, `county_weather_zone`, `county_utility_overlap_puct`, `census_population_county`, `census_permits_county`,
`census_housing_county`, `tceq_data_center_sites`, `ltlf_forecasts`. Also the RTM load-zone prices from the local lake
(`ercot_spp_hist`, parsed with the repo's parser) and the outputs of X1, X2, X11 and X12 in `analysis/out/`.

- Script: `analysis/x14_acquisition_zones.py` (`uv run --group analysis python analysis/x14_acquisition_zones.py`,
  ~10 s; run X1, X2, X11 and X12 first)
- Model code: `basecast_pipelines/models/acquisition_zones.py` (pure functions + thin loaders; `accounts.py`,
  `triggers.py`, `data_centers.py` and `large_load_geo.py` imported unchanged)
- Tests: `tests/models/test_acquisition_zones.py` (synthetic, no DB; 10 tests, pass with `tests/test_partner_lock.py`)
- Outputs (gitignored) in `analysis/out/`: `x14_county_priority.csv` (the map prototype, 204 rows),
  `x14_top_retail.csv`, `x14_top_partner.csv`, `x14_county_accounts.csv` (431 rows), `x14_sensitivity.csv`,
  `x14_signal_summary.csv`, `x14_signal_pairs.csv`, `x14_rtm_lz.parquet` (price cache), `x14_run.log`

**Validation lock.** The county score uses territory signals only. It never reads an account score, an account rank,
Base's public-facts table, the partners module or Base's current service areas. The competitive area comes from the
public PUCT territory types. The county → accounts list names every ERCOT co-op and muni in a county, with no
partner flag.

**The weights are a proposal.** There is no outcome to fit them to. §4 shows how much the ranking depends on them.

## 1. Definition

**Zone = county.** A county is the map's grain, and it is the grain at which the homeowner data exists. The
universe is the **204 ERCOT counties** (`county_weather_zone.in_ercot`). The other 50 Texas counties are drawn on the
map without a priority.

**Channel.** Each county's PUCT territory rows (`county_utility_overlap_puct`) are split by type. `county_share` is
summed per channel and divided by the county's own sum, because dual-certified territories overlap. This is the same
rule as `data_centers.county_territory_types`.

| Channel | PUCT rows | Meaning for Base |
|---|---|---|
| `retail` | `utility_type = iou` and `in_ercot` (Oncor, CenterPoint, AEP Texas, TNMP) | competitive retail area: a homeowner can pick Base as retailer |
| `coop`, `muni` | co-op or muni with `in_ercot` | no retail choice: Base needs the utility (the /accounts list) |
| `outside` | any territory not in ERCOT (Entergy, SWEPCO, SPS, El Paso Electric, SPP-only co-ops, …) | neither channel |

`channel` = `retail_direct` when the retail share is ≥ 0.5, `partnership` when co-op + muni is ≥ 0.5, else `mixed`.
`addressable_share` = 1 − outside.

**Signals.** Each signal becomes its percentile rank among the 204 counties (`triggers.percentile_ranks`, ties
averaged). The county builders of `accounts.py` run with one "account" per county (`county_links`: `county_share` =
1), so the definitions match Q3.

| Block | Signal | Weight in block | Source and definition | Grain |
|---|---|---:|---|---|
| Market | `addr_sf_homes` | 0.35 | ACS 2024 B25032 lines 3+4 (owner-occupied single-family) × `addressable_share` | county |
| Market | `pop_growth` | 0.30 | Census PEP V2025, 2020 → 2025 | county |
| Market | `permits_per_1k` | 0.15 | BPS annual 2023–2025, imputed `units_total`, per 1,000 residents in 2025 | county |
| Market | `owner_sf_share` | 0.20 | owner-occupied single-family ÷ occupied units (same ACS table) | county |
| Grid | `zone_peak_cagr` | 0.30 | X12 weather-normalized summer peak P50, CAGR 2021 → 2026 | weather zone |
| Grid | `ll_pressure` | 0.20 | X1 summer-peak excess over the pre-2020 fit, mean `excess_pct` 2023–2026 (the flat large-load layer) | weather zone |
| Grid | `lz_spread` | 0.25 | RTM 15-min load-zone SPP 2023–2025: mean daily (top 2 h − bottom 2 h), a 2-hour battery's spread before losses | load zone via weather zone |
| Grid | `dc_sites` | 0.25 | TCEQ data-center sites with a first permit ≥ 2025-01-01 (Q4) | county |

A zone value reaches a county as the mean over the county's weather-zone mix (`county_weather_zone.share_*`). Load
zones reach weather zones through a proxy: COAST → LZ_HOUSTON; EAST, NCENT → LZ_NORTH; NORTH → 0.72 LZ_WEST + 0.28
LZ_NORTH (X11's f); FWEST, WEST → LZ_WEST; SCENT, SOUTH → LZ_SOUTH. The NOIE zones (LZ_AEN, LZ_CPS, LZ_LCRA,
LZ_RAYBN) are left out. This mapping is **not verified**.

**Score.**

```
market_score = weighted mean of the market ranks (renormalized over the signals a county has)
grid_score   = weighted mean of the grid ranks
grid_factor  = 1 − 0.4 + 0.4 × grid_score          (GRID_TILT = 0.4; range 0.70–0.96 in the data)
priority     = market_score × grid_factor           (0–1; observed 0.003–0.763)
```

Grid value can cut a county's market score by up to 40%, but it cannot lift a county that has no homes. The channel
enters two ways: through the size signal (homes × addressable share), and by deciding which channel lists a county
joins. A county joins a channel's list when that channel covers **≥ 25% of its land**, and the list is ranked by the
same priority.

**Context, not weighted.** X2's adjusted generation queue (`gen_adj_mw_2028`, `gen_adj_storage_mw_2028`) is carried
on every row. Its sign for a battery fleet is ambiguous: local storage competes for the same spread, while solar
widens the evening ramp. It would also add a fourth West-leaning signal.

## 2. Inputs as they came out

| Weather zone | Peak CAGR 2021–26 (X12) | Excess over pre-2020 fit 2023–26 (X1) | Approved stock ÷ 2026 peak (X11, alt.) |
|---|---:|---:|---:|
| NORTH | 12.0% | 157.9% | 0.95 |
| FWEST | 11.0% | 88.1% | 0.20 |
| WEST | 7.6% | 7.8% | 0.43 |
| SCENT | 3.7% | 13.2% | 0.04 |
| SOUTH | 3.6% | 2.0% | 0.06 |
| EAST | 2.6% | 22.8% | 0.03 |
| NCENT | 2.4% | 3.3% | 0.05 |
| COAST | 1.9% | 5.8% | 0.04 |

| Load zone | Mean daily 2-h spread 2023–25, $/MWh | 2023 | 2024 | 2025 | Median day |
|---|---:|---:|---:|---:|---:|
| LZ_WEST | 170.2 | 290.8 | 123.1 | 96.8 | 80.8 |
| LZ_NORTH | 134.2 | 237.0 | 89.7 | 75.9 | 48.6 |
| LZ_HOUSTON | 130.5 | 237.9 | 86.0 | 67.7 | 48.1 |
| LZ_SOUTH | 123.5 | 199.9 | 101.0 | 69.8 | 49.4 |

- Coverage is 204/204 for every signal except `permits_per_1k`, which covers 176 (86.3%). The 28 counties with no
  BPS report have their market weights renormalized.
- `dc_sites` is 89.7% zeros: 31 sites in 21 ERCOT counties. It passes Q3's near-constant rule (mode < 90%) by a hair,
  and it works as a bonus, not a ranking.
- **Redundancy.** No weighted pair has |ρ| > 0.8. The highest are `addr_sf_homes` × `permits_per_1k` 0.69,
  `zone_peak_cagr` × `ll_pressure` 0.68, `addr_sf_homes` × `pop_growth` 0.65, and `zone_peak_cagr` × `lz_spread` 0.58.
  The alternatives are the redundant ones: LTLF zone CAGR × X11 stock pressure 0.92, and X12 peak CAGR × LTLF CAGR
  0.88.
- Every grid signal points West. NORTH and FWEST top both growth signals, and LZ_WEST has the widest spread in every
  year. The grid block therefore separates the West from the rest more than it separates counties within a zone.

**Channel split (204 counties).** 161 are `partnership` (159 led by co-ops, 2 by munis: Bexar and Brazos), 39 are
`retail_direct` and 4 are `mixed` (Foard, Hardeman, Montgomery, Wilbarger). By land the mean shares are 30.5% retail,
63.9% co-op, 2.1% muni and 3.4% outside. 24 counties have less than 90% addressable land. 103 counties are eligible
for the retail list, 180 for the partnership list, and 79 for both. In the partnership counties, one co-op or muni
covers the partnership land in 45 counties, two in 71, three in 28, four in 13 and five in 4.

## 3. Design choices the data forced

1. **Tilt, not an additive mean.** First draft: a Q3-style weighted mean, market 0.55 and grid 0.45. The zone-level
   grid ranks then carried whole FWEST/WEST blocks into the retail list: 5 FWEST/WEST counties, and 2 counties with
   fewer than 5,000 owner-occupied single-family homes (Archer 2,635, Clay 3,280). With the tilt, 2 FWEST/WEST
   counties (Midland, Ector) and 1 small county (Archer) remain. The additive ranking correlates 0.88 (grid 0.40)
   and 0.79 (grid 0.45) with the tilt, and keeps 11–16 of the tilt's top 20. Percentile ranks turn LZ_SOUTH vs
   LZ_HOUSTON ($123.5 vs $130.5) into a rank gap as wide as a 1,000× gap in homes. The tilt caps that effect.
2. **Channel lists by eligibility, not priority × share.** Area shares under-read the channel that serves the dense
   part of a county. Oncor covers 49% of Collin's land but most of its homes (not verified). Ranked by priority ×
   retail share, the retail list admits Ward (2,883 homes), Winkler, Andrews and Howard, and pushes Collin to #21 and
   Denton to #45. That ranking keeps only 10 of 20 retail and 8 of 20 partnership counties. The share now only
   decides membership (≥ 25%).
3. **Addressable share in the size signal, not on the whole priority.** Multiplying the priority by the addressable
   share drops Montgomery County from #17 to #125, because 49% of its land is Entergy Texas (MISO). The Woodlands is
   CenterPoint (ERCOT) and holds much of its housing (not verified). That is the same area bias, in the other
   direction.

## 4. Results

### Top 20 per channel

**Retail-direct** (≥ 25% of land in ERCOT IOU territory, 103 eligible), ranked by priority:

| # | County | Zone | Priority | Overall | Retail share | Market | Grid | Owner SF homes | Pop growth 20–25 | Drivers |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | Williamson | SCENT | 0.73 | 2 | 0.46 | 0.89 | 0.54 | 162,707 | +22% | homes, growth |
| 2 | Midland | FWEST | 0.72 | 3 | 1.00 | 0.76 | 0.88 | 40,373 | +10% | homes, growth |
| 3 | Grayson | NORTH | 0.70 | 4 | 0.44 | 0.78 | 0.72 | 32,956 | +13% | homes, growth |
| 4 | Rockwall | NCENT | 0.67 | 6 | 0.59 | 0.93 | 0.29 | 32,508 | +29% | growth, homes |
| 5 | Fort Bend | COAST | 0.66 | 8 | 1.00 | 0.94 | 0.25 | 218,869 | +17% | homes, growth |
| 6 | Ellis | NCENT | 0.66 | 9 | 0.53 | 0.92 | 0.29 | 49,795 | +24% | growth, homes |
| 7 | Collin | NCENT | 0.66 | 10 | 0.49 | 0.88 | 0.38 | 261,209 | +21% | homes, growth |
| 8 | Kaufman | NCENT | 0.66 | 11 | 0.34 | 0.89 | 0.35 | 35,085 | +42% | growth, homes |
| 9 | Denton | NCENT | 0.65 | 12 | 0.31 | 0.87 | 0.36 | 227,562 | +17% | homes, growth |
| 10 | Hunt | NCENT | 0.65 | 13 | 0.51 | 0.78 | 0.59 | 22,482 | +23% | growth, homes |
| 11 | Fannin | NORTH | 0.65 | 14 | 0.27 | 0.73 | 0.71 | 8,798 | +9% | growth, owner share |
| 12 | Wise | NCENT | 0.62 | 18 | 0.36 | 0.79 | 0.46 | 15,903 | +22% | growth, homes |
| 13 | Brazoria | COAST | 0.62 | 19 | 0.95 | 0.88 | 0.25 | 91,055 | +12% | homes, growth |
| 14 | Parker | NCENT | 0.62 | 20 | 0.32 | 0.86 | 0.29 | 39,391 | +24% | growth, homes |
| 15 | Johnson | NCENT | 0.59 | 24 | 0.34 | 0.83 | 0.29 | 40,763 | +20% | homes, growth |
| 16 | Smith | EAST | 0.57 | 29 | 0.51 | 0.73 | 0.45 | 50,577 | +8% | homes, growth |
| 17 | Van Zandt | EAST | 0.56 | 30 | 0.33 | 0.73 | 0.42 | 14,877 | +11% | growth, homes |
| 18 | Archer | NORTH | 0.56 | 31 | 0.61 | 0.63 | 0.71 | 2,635 | +7% | owner share, growth |
| 19 | Bell | NCENT | 0.55 | 33 | 0.58 | 0.71 | 0.44 | 75,156 | +8% | homes, growth |
| 20 | Ector | FWEST | 0.55 | 36 | 1.00 | 0.61 | 0.74 | 33,139 | +5% | homes, permits |

**Partnership** (≥ 25% of land in ERCOT co-op or muni territory, 180 eligible), ranked by priority. All 20 are led
by co-ops:

| # | County | Zone | Priority | Overall | Partner share | Utilities ≥ 1% | Largest one's share | Market | Grid | People/km² | Drivers |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | Comal | SCENT | 0.76 | 1 | 1.00 | 3 | 0.59 | 0.94 | 0.54 | 144 | growth, homes |
| 2 | Williamson | SCENT | 0.73 | 2 | 0.54 | 5 | 0.60 | 0.89 | 0.54 | 260 | homes, growth |
| 3 | Grayson | NORTH | 0.70 | 4 | 0.56 | 4 | 0.92 | 0.78 | 0.72 | 64 | homes, growth |
| 4 | Kendall | SCENT | 0.68 | 5 | 1.00 | 4 | 0.36 | 0.86 | 0.46 | 31 | growth, homes |
| 5 | Rockwall | NCENT | 0.67 | 6 | 0.41 | 1 | 1.00 | 0.93 | 0.29 | 427 | growth, homes |
| 6 | Guadalupe | SCENT | 0.67 | 7 | 0.94 | 4 | 0.84 | 0.87 | 0.42 | 109 | homes, growth |
| 7 | Ellis | NCENT | 0.66 | 9 | 0.47 | 3 | 0.55 | 0.92 | 0.29 | 99 | growth, homes |
| 8 | Collin | NCENT | 0.66 | 10 | 0.51 | 5 | 0.51 | 0.88 | 0.38 | 595 | homes, growth |
| 9 | Kaufman | NCENT | 0.66 | 11 | 0.66 | 2 | 0.89 | 0.89 | 0.35 | 103 | growth, homes |
| 10 | Denton | NCENT | 0.65 | 12 | 0.69 | 4 | 0.85 | 0.87 | 0.36 | 470 | homes, growth |
| 11 | Hunt | NCENT | 0.65 | 13 | 0.49 | 3 | 0.80 | 0.78 | 0.59 | 57 | growth, homes |
| 12 | Fannin | NORTH | 0.65 | 14 | 0.73 | 2 | 0.99 | 0.73 | 0.71 | 17 | growth, owner share |
| 13 | Burnet | SCENT | 0.65 | 15 | 0.99 | 1 | 1.00 | 0.79 | 0.55 | 22 | growth, homes |
| 14 | Hays | SCENT | 0.64 | 16 | 1.00 | 3 | 0.95 | 0.84 | 0.42 | 174 | homes, growth |
| 15 | Montgomery | COAST | 0.63 | 17 | 0.34 | 3 | 0.62 | 0.89 | 0.28 | 289 | homes, growth |
| 16 | Wise | NCENT | 0.62 | 18 | 0.64 | 5 | 0.85 | 0.79 | 0.46 | 36 | growth, homes |
| 17 | Parker | NCENT | 0.62 | 20 | 0.68 | 2 | 0.94 | 0.86 | 0.29 | 79 | growth, homes |
| 18 | Montague | NORTH | 0.60 | 21 | 0.80 | 2 | 0.53 | 0.67 | 0.72 | 9 | growth, owner share |
| 19 | Lubbock | NORTH | 0.60 | 22 | 0.89 | 2 | 0.88 | 0.66 | 0.76 | 142 | homes, permits |
| 20 | Llano | WEST | 0.60 | 23 | 1.00 | 2 | 0.96 | 0.71 | 0.59 | 10 | growth, permits |

"Drivers" are the two signals with the largest weight × (rank − 0.5). "Homes" means `addr_sf_homes`. 11 counties
appear in both lists, because they are split between an IOU and co-ops. The map shows them as one county with two
shares.

### Face validity

- **Metro growth rings.** The rings are the counties next to the five cores (Claude's list, not an OMB MSA
  definition). Overall top 20: 17 ring counties (DFW 8, Houston 3, Austin 3, San Antonio 3), no core, and 3 others
  (Midland, Grayson, Fannin). Retail top 20: 12 ring (DFW 9, Houston 2, Austin 1), plus Midland, Grayson, Fannin,
  Smith, Van Zandt, Archer, Bell and Ector. Partnership top 20: 15 ring (DFW 8, Austin 3, San Antonio 3, Houston 1),
  plus Grayson, Fannin, Montague, Lubbock and Llano. **Yes: both lists sit mostly on the rings.** The ring counties
  outside the top 20 are Johnson 24, Bastrop 26, Medina 27, Hood 28, Wilson 32, Galveston 38, Caldwell 42,
  Waller 54, Atascosa 79 and Bandera 90.
- **Cores rank mid-table:** Travis 35, Bexar 37, Tarrant 43, Harris 56, Dallas 71. Harris has the most owner-occupied
  single-family homes (902k), but its growth, permits per 1,000 and owner share are rates, and on owner share it
  sits at the 10th percentile. This is intended for a growth map, and it is review item 2.
- **Rural partnership counties lifted by grid value.** Comparing the full rank with the market-only rank, for
  counties with ≥ 50% partnership land and < 100 people/km²: Tom Green 79 → 48 (+31, one new data-center site), Clay
  73 → 45, Lamar 74 → 46, Val Verde 91 → 66, Montague 43 → 21, Taylor 61 → 41, Cooke 44 → 25, Gillespie 58 → 39,
  Haskell 144 → 125 (two new sites). All of them are in NORTH or WEST, so **the lift is zone-wide**: nothing places the
  large loads within a zone (X11). 10 of the partnership top 20 are rural (< 100/km²).
- **Midland and Grayson** are the grid story at the top. Midland sits in the fastest-growing, highest-priced zone
  (FWEST, LZ_WEST) and has a real home market. Grayson has NORTH's peak growth plus Sherman/Denison's growth. Both are
  plausible, and both lean on zone-level numbers.

## 5. Sensitivity

`x14_sensitivity.csv`, 36 variants against the base ranking (204 counties; "kept" = of the base top 20):

| Variant | Spearman ρ | Top 20 kept (all / retail / partner) | Largest move |
|---|---:|---|---:|
| ± 0.05 on one weight inside a block (16 variants) | ≥ 0.996 | ≥ 19 / 18 / 18 | 18 |
| Leave one market signal out | 0.849 (homes) – 0.985 (permits) | ≥ 15 / 15 / 16 | 85 |
| Leave one grid signal out | ≥ 0.996 | ≥ 17 / 18 / 18 | 17 |
| Tilt 0.3 / 0.5 | 0.998 / 0.997 | 20 and 17 / 19 and 20 / 17 and 19 | 15 |
| Tilt 0 (market only) | 0.979 | 18 / 17 / 17 | 31 |
| Tilt 1 (grid fully multiplicative) | 0.732 | 8 / 11 / 11 | 88 |
| Zone growth = LTLF 2025 CAGR 2025–31 (Q3's signal) | 0.998 | 20 / 19 / 19 | 11 |
| Large-load pressure = X11 approved stock ÷ peak | 0.995 | 20 / 18 / 19 | 16 |
| Spread on the median day | 0.997 | 18 / 20 / 18 | 14 |
| Size = homes without the ERCOT share | 0.998 | 20 / 20 / 20 | 28 |
| Addressable share also multiplies the priority | 0.974 | 19 / 19 / 18 | 108 (Montgomery) |
| Channel lists by priority × share | 1.0 | 20 / 10 / 8 | 0 |
| Additive mean, grid 0.40 / 0.45 | 0.878 / 0.794 | 16 and 11 / 16 and 14 / 18 and 15 | 83 and 102 |

**Read.** Small weight shifts do not change the lists. What matters is the structure: whether a size signal is in
(dropping homes costs 4 of the overall top 20), how grid and market combine, and how the channel share is used. Among the
grid inputs, the alternatives (LTLF vs X12 growth, X1 vs X11 pressure, mean vs median spread) barely move the
ranking, because they all order the zones West-first.

## 6. What the map needs from the API

**Mart `county_acquisition`** (proposal; the contract belongs to basecast-get-data and nothing there was changed).

- **Key:** (`as_of`, `county_fips`). 204 rows per build. Refresh after ACS, PEP, BPS, TCEQ, X1/X12 (after the summer)
  or a new price year lands. Built by `acquisition_zones.channel_split` → the `accounts.py` builders over
  `county_links` → `score_counties` → `channel_priority` → `drivers` → `legend_classes`.
- **Columns:**

| Column | Type | Note |
|---|---|---|
| `county_fips`, `county_name`, `weather_zone` | string | 5-digit FIPS; main zone |
| `priority`, `rank`, `priority_class` | float, int, int | 0–1; 1 = best; class 1–5 = quintile (5 = top) |
| `market_score`, `grid_score`, `grid_factor` | float | the tooltip reads "market 0.89 × grid 0.82" |
| `channel`, `partner_type` | string | `retail_direct` \| `partnership` \| `mixed`; `coop` \| `muni` |
| `retail_share`, `coop_share`, `muni_share`, `outside_share`, `partner_share`, `addressable_share` | float | by land, normalized; label "by area" |
| `retail_rank`, `partner_rank` | int \| null | null = not in that channel's list (share < 25%) |
| `n_partners`, `top_partner_share` | int, float | how many co-ops/munis a partnership county needs |
| `drivers`, `drags` | string[] | top 2 signals up / down |
| the 8 signals and their `pct_*` | float | for the breakdown panel |
| `gen_raw_mw`, `gen_adj_mw_2028` | float | context from `queue_adjusted_county` (join it; don't copy it) |
| `model_version`, `as_of` | string, date | weights and tilt go in `meta` |

- **Legend.** Quintile classes (breaks 0.258 / 0.341 / 0.429 / 0.530 in this run; send them in `meta` because they
  move with each build). Color: channel as the hue (retail-direct, partnership, mixed), priority class as the
  lightness, and non-ERCOT counties hatched grey. A toggle chooses "all", "retail-direct list" or "partnership list";
  counties outside the chosen list are greyed. This fits the existing choropleth by FIPS through feature-state.
- **Link to /accounts.** A click on a county with `partner_share` > 0 opens `/accounts?county=<fips>`. That list is
  the reverse of X9's `account_counties` (`x14_county_accounts.csv`: `county_fips`, `account_id` = `ccn_no`,
  `utility_type`, `county_share` ≥ 0.01, largest first). No new mart is needed: `GET /accounts` gains a `county`
  filter over `account_counties`. The list shows every co-op and muni with its own account score. The county score
  never reads the account score, and the account list never reads the county score, so the lock holds both ways.
- **Contract impact.** Draft §1 lists `acquisition_score` as a county metric. `marts-proposal.md` §2.1 called it an
  account score with no tested county rule. X14 is that rule: a county-native score, not an aggregate of accounts.

## 7. Caveats

- **Land, not homes.** Channel shares come from area. A muni or an IOU that serves a county's dense core is
  under-read (Travis shows 40% muni by land; Montgomery shows 18% retail). A population-weighted split (Census blocks ×
  PUCT polygons in PostGIS) would fix it; that is a pipeline change.
- **PUCT types cannot see retail-choice opt-ins.** Lubbock (LP&L joined ERCOT and retail competition, 2023–24, not
  verified) is labelled by land: 78% co-op, 11% muni. Nueces EC's opt-in is not verified either. No special-casing
  was done, by the lock's rule of public PUCT types only.
- **Zone-level grid signals** give every county of a zone the same value. NORTH's 12%/yr and 158% come from a small
  base (2.6 GW summer peak).
- **Prices.** 2023's scarcity dominates the mean spread. The load-zone mapping is a proxy. The spread is the energy
  value only (no ancillary services, which are out of the MVP).
- **Upstream flags carry over:** X1's flat layer (R9), X11's machine-read deck values (R3, R16), Q4's TCEQ dates and
  NAICS-only matches (R7), and BPS non-reporting (Q3).

## Proposed decisions.md lines

- 2026-09-26 — The Explorer's "priority acquisition zone" is a county score from territory signals only: market
  score (owner-occupied single-family homes × ERCOT share .35, population growth 2020–25 .30, permits per 1,000 .15,
  owner single-family share .20) × grid factor (1 − 0.4 + 0.4 × grid score; zone normalized peak growth .30, flat
  large-load pressure .20, load-zone 2-hour spread .25, new data-center sites .25); the weights are a proposal. — X14:
  an additive mean let zone-level grid ranks lift near-empty Permian counties; the tilt keeps homes first; ±0.05 shifts
  keep ρ ≥ 0.996.
- 2026-09-26 — Acquisition channel from public PUCT territory types only: retail-direct = ERCOT IOU land, partnership
  = ERCOT co-op/muni land, outside = non-ERCOT; a county joins a channel's list at ≥ 25% of its land, ranked by the
  same priority. — Area shares under-read the channel serving a county's dense part (priority × share pushed Collin to
  #21 and Denton to #45 on the retail list).
- 2026-09-26 — The addressable (ERCOT) share enters only through the size signal, not as a multiplier on the priority.
  — As a multiplier it drops Montgomery County from #17 to #125 (49% of its land is Entergy Texas).
- 2026-09-26 — A partnership county's click-through lists the ERCOT co-ops and munis covering ≥ 1% of its land (the
  reverse of `account_counties`), with no partner flag. — No new mart; keeps the validation lock.
- 2026-09-26 — X2's adjusted generation queue is map context, not a priority input. — Its sign for a home-battery
  fleet is ambiguous, and it would be a fourth West-leaning signal.

## Review items for Pablo

1. **Weights and tilt** (all Claude's): market .35/.30/.15/.20, grid .30/.20/.25/.25, tilt 0.4, list threshold 25%.
   They are stable to ±0.05, but the structure matters (§3, §5).
2. **Growth map or size map?** The cores rank 35–71 (Harris 56 with 902k homes). If the map should show where the
   most batteries can go, raise `addr_sf_homes` or add a size-only layer.
3. **Zone-level grid signals.** NORTH and WEST rural counties rise as a block. Keep them, or damp them (MW instead of
   %, or zone signals at a lower weight than the county ones)?
4. **Weather zone → load zone proxy**, with the NOIE zones left out (not verified). Would the mean spread (2023-heavy)
   or the median day be better? The ranking is the same either way (ρ 0.997).
5. **Area bias in the channel shares** (Montgomery, Travis, Collin): approve a population-weighted split as a pipeline
   task, or live with it and label shares "by area"?
6. **Opt-in areas** (Lubbock, Nueces EC): special-case them into retail-direct? That needs a public source for the
   opt-in list (PUCT; not in the repo).
7. **Contract:** replace draft §1's `acquisition_score` with the county-native `county_acquisition` mart (§6), plus a
   `county` filter on `/accounts`. basecast-get-data owns the change.
8. **Build dependency:** the mart must call the X1, X11 and X12 model functions, not read `analysis/out/`, so R5, R9
   and R16 gate `ll_pressure` and `zone_peak_cagr`.
