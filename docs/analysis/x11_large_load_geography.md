# X11 — Where the large loads are: a zone allocation for the peak forecast and the Explorer

Exploration after X7. Run on 2026-09-26 against production Postgres (`basecast_reader`, read-only):
`large_load_chart_values`, `large_load_headlines`, `large_load_status` (Gemini/regex-read decks, `verified=false`),
`puct_tsp_large_load_requests`, `county_weather_zone`, `county_utility_overlap_puct`, `tceq_data_center_sites`,
`cpa_data_centers`, `cpa_local_dev_agreements`, `census_population_county`, plus X7's load and weather inputs.

- Script: `analysis/x11_large_load_geography.py` (`uv run --group analysis python analysis/x11_large_load_geography.py`, ~15 s)
- Model code: `basecast_pipelines/models/large_load_geo.py` (pure functions + thin loaders; `large_load.py`,
  `peak_excess.py`, `peak_forecast.py`, `data_centers.py`, `weather_load.py` imported unchanged)
- Tests: `tests/models/test_large_load_geo.py` (synthetic, no DB; 11 tests, pass with `tests/test_partner_lock.py`)
- Outputs (gitignored) in `analysis/out/`: `x11_a2e_by_load_zone.csv`, `x11_stock_allocation.csv`,
  `x11_pipeline_shares.csv`, `x11_tsp_rfi_zone_by_year.csv`, `x11_promised_allocation.csv`, `x11_x1_check.csv`,
  `x11_zone_forecast_compare.csv`, `x11_zone_forecast_variants.csv`, `x11_zone_allocation.csv`,
  `x11_county_pressure.csv`, `x11_named_county_check.csv`

**Every MW by zone or county below is allocated, not observed.** The deck values are machine-read and not
verified (Q5 spot-check pending). The Batch Zero page 7 values were checked by eye against the rendered slide.

## 1. Inventory: large load by geography

| Source (table · chart) | Geography | Dates | Definition | Size |
|---|---|---|---|---|
| `large_load_chart_values` · "Approved to Energize by Load Zone" (+ `large_load_headlines` `dimension=load_zone`, regex) | LZ_WEST vs Other | 22 decks, 2024-04 → 2026-06 | approved-to-energize stock = observed non-simultaneous peak + remaining approved | 4,479 MW (Apr 2024) → 8,926 MW (Jun 2026); LZ_WEST share 63% → 48–50% (2025) → **56.6%** |
| same · "Approved to Energize Loads by Load Zone 2023" (Feb 2024) | ERCOT load zones Houston/North/South/West | 2023 | MW approved during 2023 | 1,956 MW (West 1,596, South 360) |
| `large_load_status` (native PPTX) | LZ_WEST vs Other | Oct 2024 only | observed + remaining approved | 5,697 MW |
| `large_load_chart_values` · "Large Load Project Distribution by Load Zone" | LZ_NORTH / LZ_WEST / Other | 8 decks, 2025-10 → 2026-06 (status split from May 2026) | tracked queue with studies (planning approved + under review) | 80.9 GW → 200.0 GW |
| same · "LLIS Projects by Weather Zone (GWs)" | 8 weather zones + Not Specified | May 2026 | whole LLIS queue with Batch Zero eligibility paths; "(6) not included unless studies submitted" split out | 458.9 GW (126.0 GW with studies) |
| same · "Batch Zero Tracking by Weather Zone (GWs)" | 8 weather zones + Not Specified | Jun 2026 | whole queue at Batch Zero intake | 481.4 GW |
| same · "Base & studied load by region, MW" (Batch Zero Update, Board 2026-09-14, p. 7) | 8 weather zones | conditional classification of 2026-09-03, MW in **2032** | base = energized before/after 2022-03-25 (22.3 GW), QSA (6.6), Permian plan (3.4), advancing (28.8), committed (4.2), net metering (1.2); studied = firm, PCLR, WLPUN | base **66.4 GW**, studied **125.4 GW**; 302.2 GW excluded |
| same · "Top base / base + studied load counties" (same page) | 5 + 5 counties | same | as above | Dallas 5,713, Harris 5,672, Haskell 2,689, Brazoria 2,357, Ellis 2,330 (base); Cameron 12,235, Dallas 7,637, Jones 7,000, Harris 6,899, Childress 6,750 (base + studied) |
| same · "Q4 2026 QSA Incoming Generation & Load Capacity – Weather Zone" (Board 2026-06) | 8 weather zones | Q4 2026 QSA | large loads entering the stability assessment, 5–8 months before energization | 3,898 MW (NORTH 1,889, NCENT 841, SCENT 518, FWEST 350, WEST 300) |
| same · "Large Load Project Distribution – TSP", "Requests by TSP" | 14 TSPs | 2025-10 → 2026-05 | **project counts only**, no MW | e.g. Oncor 259, LCRA TSC 60, AEP 46 (Apr 2026) |
| `puct_tsp_large_load_requests` (58777 item 38, filed 2026-04-15) | 13 TSPs × year | 2026–2032 | MW **requested** (RFI submissions), cumulative | 11,443 → 242,999 MW; Oncor 109.6 GW, AEP 42.3, Brazos 32.2, Golden Spread 17.4, TNMP 13.4 |
| `tceq_data_center_sites` | county | permits 1987 → 2026-09 | air-permit sites matched as data centers (no MW) | 88 sites, 38 since 2025 |
| `cpa_data_centers` | county (7 of 167) | 2013 → 2026-09 | sales-tax exemption registrations (no MW) | 167 |
| `cpa_local_dev_agreements` | county | 2006 → 2026-08 | ch312/ch380 agreements naming a data center (no MW) | 15 |
| `ercot_settlement_point_map`, `ercot_noie_load_map` | bus → load zone | — | no county or coordinates, so they cannot map load zones to weather zones | — |

Nothing in the tables gives large load by county with MW beyond the ten Batch Zero callouts, and no chart gives
the in-service-year promise by region (X7's premise holds for the in-service bars). The raw decks were not
re-parsed. A text search of the Interconnection-and-Grid and Batch Zero decks (Feb–Sep 2026) found no geographic
chart the tables missed; other decks were not re-checked.

## 2. Method and allocation

**Load zone → weather zone.** LZ_WEST = FWEST + WEST + f × NORTH (the NORTH weather zone straddles AEP Texas
North and Oncor/co-op areas). The May 2026 deck shows the same population both ways (LZ distribution 121.8 GW vs
LLIS-with-studies by weather zone 126.0 GW), so `f = (LZ_WEST − FWEST − WEST) / NORTH = (41.0 − 21.3) / 27.3 =
0.72`. The Jun 2026 pair cannot be used (200 GW vs 481 GW: different populations). Not verified.

**(a) Approved stock.** Zone shape from the Batch Zero base load by weather zone (the only weather-zone split of
energized-or-near loads), raked so FWEST + WEST + 0.72 × NORTH hold the deck's LZ_WEST share of the approved stock
(56.6% in Jun 2026; the unraked base split gives 37.3%). Band = min/max over f ∈ {0, 0.72, 1} and the unraked split.

| Zone | Share | Approved stock, MW (Jun 2026, 8,926 MW) [band] |
|---|---:|---:|
| NORTH | 27.6% | 2,463 [1,063–2,502] |
| FWEST | 19.4% | 1,734 [1,142–2,962] |
| NCENT | 15.2% | 1,356 [1,088–1,958] |
| WEST | 13.7% | 1,222 [805–2,088] |
| COAST | 11.5% | 1,022 [820–1,476] |
| SCENT | 7.0% | 628 [504–908] |
| SOUTH | 4.5% | 397 [319–574] |
| EAST | 1.2% | 103 [83–149] |

**(b) Promised MW.** Central split: Batch Zero base + studied load in 2032 (191.8 GW, after 302 GW excluded). The
alternatives set the band. The TSP RFI goes to zones through each wires TSP's PUCT CCN area × county zone shares;
only Oncor, AEP, TNMP, CenterPoint and CPS have a CCN area (69–70% of the 2030–2032 MW). The G&T co-ops (Brazos,
Golden Spread, Rayburn, STEC), LCRA TSC and WETT/Lone Star stay unmapped.

| Zone | **Central** (BZ base+studied) | BZ studied | BZ tracking Jun 26 | LLIS with studies May 26 | TSP RFI 2030, area | TSP RFI 2030, population | QSA Q4 2026 |
|---|---:|---:|---:|---:|---:|---:|---:|
| NCENT | **21.1%** | 20.6 | 30.5 | 24.4 | 22.4 | 56.6 | 21.6 |
| NORTH | **20.1%** | 19.4 | 22.5 | 21.7 | 8.1 | 2.6 | 48.5 |
| SOUTH | **12.9%** | 16.3 | 6.3 | 9.0 | 10.0 | 18.3 | 0 |
| WEST | **11.7%** | 13.1 | 11.1 | 11.9 | 11.0 | 2.9 | 7.7 |
| SCENT | **11.2%** | 11.8 | 10.8 | 14.0 | 3.6 | 5.8 | 13.3 |
| FWEST | **9.9%** | 8.4 | 6.6 | 5.0 | 32.7 | 3.8 | 9.0 |
| COAST | **9.1%** | 5.2 | 6.1 | 7.5 | 4.4 | 6.8 | 0 |
| EAST | **4.0%** | 5.3 | 6.0 | 6.6 | 7.9 | 3.2 | 0 |

- The four deck-based splits agree within a few points. The TSP path swings with the weighting (Oncor: 36% FWEST
  by area, 80% NCENT by population), so it is a check, not the central estimate. Its shares barely move by year
  (2026 → 2032), so a single split for every in-service year loses little.
- Applied to the Mar 2026 deck's promise (cumulative, all statuses): 2030 = 238.6 GW → NCENT 50.2, NORTH 48.0, SOUTH
  30.8, WEST 27.9, SCENT 26.8, FWEST 23.7, COAST 21.7, EAST 9.6 GW (`x11_promised_allocation.csv`, with the band
  and the Jun 2026 deck). These are promises, not forecasts: X7's ratio (~0.19) applies.
- **The pipeline is not where the approved stock is.** The queue leans to NCENT/NORTH/SOUTH; the approved stock to
  NORTH/FWEST/WEST (LZ_WEST).

## 3. Check against X1: do the zones with approved large load show the excess?

Allocated LL at the peak = 0.512 × A2E at the peak month × that month's raked stock split, vs X1's coincident
excess (MW):

| Zone | 2025 LL | 2025 excess | 2026 LL | 2026 excess | 2026 dmin (flat) excess |
|---|---:|---:|---:|---:|---:|
| FWEST | 714 | 2,681 | 888 | 3,068 | 4,063 |
| SCENT | 286 | 1,874 | 322 | 2,363 | 2,115 |
| COAST | 465 | 653 | 524 | 2,068 | 2,131 |
| NCENT | 617 | 864 | 695 | 1,400 | 1,866 |
| NORTH | 1,031 | 890 | 1,262 | 1,313 | 902 |
| EAST | 47 | 454 | 53 | 630 | 393 |
| WEST | 503 | −263 | 626 | 230 | 539 |
| SOUTH | 181 | −100 | 204 | 102 | 787 |

- **Only partly.** Rank correlation of allocated stock vs excess is 0.50 (2025) and 0.38 (2026); vs the flat
  (dmin) excess 0.40 / 0.45.
- **NORTH and WEST:** the allocation puts the large load there, and the excess roughly equals it (NORTH) or falls
  short (WEST before 2026). The 2026 WEST step (+0.5–0.7 GW flat) is the first sign of it.
- **FWEST, SCENT and COAST:** they carry 1.4–7× more excess than their allocated large load. X1's unattributed flat
  load sits there: FWEST (Permian electrification is the obvious candidate, not verified), SCENT and COAST.
- **U split** (mean 2023–26 excess minus allocated LL, clipped): FWEST 33.6%, SCENT 33.0%, COAST 18.3%, EAST 8.2%,
  NCENT 3.6%, NORTH 3.3%, SOUTH 0, WEST 0. X7's single share mixed the two layers.

## 4. X7 zone re-run: fixed share vs X11 (Mar 2026 deck, coincident contribution, P50 MW)

X11: LL up to 0.512 × the deck's approved stock follows the stock split; LL above it (new approvals) the pipeline
split; U the residual split. Organic and draws are X7's own. Band = P50 range when one input split is swapped for
each alternative in §2 (allocation band only, not P10–P90).

| Zone | 2027 X7 fixed | 2027 X11 [band] | 2030 X7 fixed | **2030 X11** [band] | 2030 Δ |
|---|---:|---:|---:|---:|---:|
| NCENT | 28,314 | 28,909 [28,770–30,316] | 30,334 | **32,933** [32,794–40,041] | +2,600 |
| COAST | 24,448 | 23,929 [23,550–24,164] | 27,950 | **26,217** [24,339–26,452] | −1,734 |
| SCENT | 16,519 | 15,474 [15,162–15,619] | 20,940 | **17,670** [16,130–18,210] | −3,271 |
| SOUTH | 6,930 | 7,661 [7,135–7,873] | 7,315 | **10,124** [7,521–11,197] | +2,808 |
| FWEST | 8,551 | 7,363 [7,057–8,292] | 13,822 | **9,419** [8,172–13,936] | −4,403 |
| NORTH | 2,601 | 3,120 [2,395–4,231] | 4,523 | **6,172** [2,690–11,828] | +1,649 |
| WEST | 2,224 | 3,324 [2,977–3,772] | 2,312 | **5,281** [3,537–5,729] | +2,969 |
| EAST | 3,330 | 3,125 [2,960–3,282] | 4,318 | **3,802** [2,985–4,573] | −516 |
| Sum | 92,916 | 92,906 | 111,514 | 111,616 | |

- The ERCOT total does not move (±0.1%). The zones do: by 2030 X11 moves ~10 GW between zones.
- FWEST's and SCENT's 2030 values drop 4.4 and 3.3 GW: X7 extrapolated the flat excess as if new large loads
  would keep landing there. The queue points to NCENT, NORTH, SOUTH and WEST instead.
- WEST gets large load for the first time (X7 gave it 0% because its 2023–26 excess was negative).
- NORTH's band is the widest (2.7–11.8 GW): the Q4 2026 QSA puts 48% of incoming load there, the TSP path 3–8%.

## 5. County pressure for the Explorer (allocated, not observed)

Each zone's MW is spread over its counties by their share of the zone's data-center signals: TCEQ sites + CPA
data-center agreements + CPA registrations with a county (35 counties). Every zone has at least one signal, so all
8,926 MW of approved stock land in a county. Top counties by allocated approved stock: Wilbarger 1,112 MW, Harris 825,
Pecos 601, Dickens 556, Haskell 516, Ward 451, Bexar 366, Taylor 364, Medina 364, Dallas 360.

Validation against the counties the Batch Zero deck names (MW, observed vs allocated):

| County | Signals | Base load (deck) | Base allocated | Base + studied (deck) | Base + studied allocated |
|---|---:|---:|---:|---:|---:|
| Harris | 4 | 5,672 | 8,860 | 6,899 | 14,104 |
| Dallas | 10 | 5,713 | 3,862 | 7,637 | 10,706 |
| Haskell | 2 | 2,689 | 2,953 | — | 8,156 |
| Ellis | 5 | 2,330 | 1,931 | — | 5,353 |
| Brazoria | 0 | 2,357 | 0 | — | 0 |
| Cameron | 0 | — | 0 | 12,235 | 0 |
| Jones | 0 | — | 0 | 7,000 | 0 |
| Childress | 0 | — | 0 | 6,750 | 0 |

- For base load in counties with permits, the spread lands within −32% to +56%.
- **The biggest pipeline counties have no permit signal at all:** Cameron, Jones and Childress, 26 GW together.
  Air permits and tax agreements lag the interconnection queue. The county layer is fine for the approved stock and
  misleading for the pipeline. The Explorer should show the pipeline at zone level plus the deck's named counties,
  not spread it by permits.

## Proposed decisions.md lines

- 2026-09-26 — Large load in the peak forecast is split by zone in three parts: 0.51 × the current approved stock follows the stock split (Batch Zero base load by weather zone, raked to the deck's LZ_WEST share); new approvals follow the Batch Zero base + studied split; the unattributed layer follows the zone excess left after the allocated large load. — X11: the approved stock (LZ_WEST 57%) and the queue (NCENT/NORTH/SOUTH) sit in different zones, and the flat excess not explained by large loads sits in FWEST/SCENT/COAST (rank correlation of allocated stock vs excess 0.38–0.50); a single share moved up to 4.4 GW per zone by 2030.
- 2026-09-26 — LZ_WEST = FWEST + WEST + 0.72 × NORTH for allocation purposes (not verified). — Solved from the May 2026 deck, which shows the same ~122–126 GW with studies both by load zone and by weather zone.
- 2026-09-26 — The TSP RFI (58777 item 38) is a check on the zone split, not an input. — Only 69–70% of its MW maps to a CCN area (G&T co-ops, LCRA TSC, WETT and Lone Star have none), and Oncor's split swings from 36% FWEST by area to 80% NCENT by population.
- 2026-09-26 — The Explorer spreads only the approved stock to counties (by data-center permit and agreement signals); the pipeline stays at zone level plus the counties ERCOT names. — Cameron, Jones and Childress hold 26 GW of Batch Zero base + studied load with no permit signal.

## Review items for Pablo

1. **Batch Zero base load as the stock's zone shape.** It is 34% energized and 43% "advancing" loads, in 2032 MW,
   so it tilts toward zones with big future projects (NORTH 21%). No weather-zone split of the energized stock
   alone exists in the decks. Accept, or keep only the LZ_WEST/Other split with an even spread inside each side?
2. **f = 0.72 for NORTH in LZ_WEST.** It comes from two Gemini-read charts. The band (f from 0 to 1) moves the NORTH
   stock between 1.1 and 2.5 GW. A bus → county map (not in the DB) would settle it.
3. **The Sep 2026 conditional classification can change in December** (Governor's directive, eligibility audit), and
   ERCOT paused energization of ≥ 75 MW data centers and crypto on 2026-08-03. The pipeline split should be re-read
   from the December filing.
4. **The TSP RFI's G&T co-ops** (Brazos 32 GW, Golden Spread 17 GW) are unmapped. Mapping them needs their member
   co-op lists (not in the DB; not verified). It matters for the co-op story: that MW is in co-op territory.
5. **County signals weight a 1987 TCEQ permit the same as a 2026 one.** Weight by recency, or only count sites
   since 2022?
6. **Spot-check** (Q5) now also covers the Batch Zero p. 7 and QSA p. 7 values. I checked p. 7 by eye against the
   rendered slide; the QSA slide was checked against its PDF text layer.

## Useful for the core features

- **Forecast screen:** zone contributions from `x11_zone_forecast_compare.csv`, labelled "large load allocated:
  approved stock by the Batch Zero base split, new approvals by the base + studied split", with the allocation band
  as a second, lighter range. Replace X7's fixed-share note.
- **Explorer map:** two zone layers, "approved large load (allocated)" and "large-load pipeline 2032 (Batch Zero,
  observed by zone)". A county layer holds the allocated approved stock (`x11_county_pressure.csv`, `alloc_label`)
  plus ERCOT's ten named counties as observed points. Don't spread the pipeline by permits.
- **Accounts / diagnosis:** a co-op in NORTH, WEST, SOUTH or NCENT sits where the queue is heading. One in FWEST,
  SCENT or COAST sits where flat load already arrived without matching approved large loads, which is the case for
  the UtilityDataSource question ("what did you receive?").
- **Non-obvious insight for the video (candidate):** "The large loads ERCOT has approved sit in the West; the flat
  load that actually showed up is in the Permian, Central Texas and Houston; and the next wave is queued around
  Dallas, the Panhandle and the Valley. Cameron, Jones and Childress counties hold 26 GW of queued load and no
  data-center permit yet." (Queue by zone and the named counties are ERCOT's own, Sep 2026, conditional.)
- **API:** mart `large_load_zone_allocation` (`as_of`, `weather_zone`, `measure` in {`a2e_stock`, `pipeline_2032`,
  `u_share`}, `central`, `low`, `high`, `method`, `verified=false`), and `county_large_load_pressure` from
  `x11_county_pressure.csv`.
