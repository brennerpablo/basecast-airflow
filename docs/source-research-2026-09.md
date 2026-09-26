# Source research — 2026-09-26

Two research passes run overnight after the first raw backfill, looking for public datasets we had not
mapped: supply side, ERCOT and PUCT; and demand side, utilities and socioeconomic data. Every URL below was
requested (status, size, Last-Modified). Samples were read to confirm headers where noted. Entries are also
in `catalog.yaml`, in the section "Added 2026-09-26". Anything not confirmed is marked **not verified**.

## Outcome

| Dataset | Verdict | Source id (raw) | Why it matters |
|---|---|---|---|
| PUCT electric CCN service areas (ArcGIS) | downloaded | `puct_ccn_territories` | Regulator's own co-op/muni/IOU polygons with CCN number, ISO and G&T; fresher than HIFLD |
| ERCOT TPIT (transmission projects) | downloaded | `ercot_tpit` | County-level projects with projected/actual in-service dates; archive 2009+ gives slippage rates |
| PUCT 58777 item 38 (2026 LTLF, TSP large-load table) | downloaded | `puct_filings` | Only structured large-load forecast by TSP (e.g. Brazos 32,150 MW in 2032) and by load type |
| ERCOT Regional Transmission Plan (PG7-048-M) | downloaded | `ercot_rtp` | TSP-submitted vs ERCOT-forecast vs plan load by weather zone; planned generators by INR |
| EIA-860M (latest + Decembers) | downloaded | `eia_860m` | Planned/operating generators with status; cross-check for GIS |
| EIA-860 2025 | downloaded | `eia_860` | Plant's transmission/distribution owner links plants to co-ops/munis |
| ERCOT NP4-160-SG settlement points | downloaded | `ercot_settlement_points` | NOIE mapping (LZ_AEN/CPS/LCRA/RAYBN → physical loads); 31-day window |
| ERCOT NP12-215-ER market participants | downloaded | `ercot_mp_list` | TDSP registrations (co-ops, munis) |
| ERCOT members by segment | downloaded | `ercot_members` | Co-ops/munis active in ERCOT governance, 2013–2026 |
| PUCT directories (coop, muni, pgc_facility…) | downloaded | `puct_directories` | Account list; `PrimaryIDNo` = CCN number; generation facilities by host territory |
| ERCOT Demand and Energy report | downloaded | `ercot_demand_energy` | Monthly peaks with hour, by load zone (incl. NOIE zones) and weather zone, 2008–2026 |
| ERCOT Fuel Mix Report | downloaded | `ercot_fuel_mix` | 15-min generation by fuel 2007–2026 (net-load features) |
| TCEQ Central Registry, air permits (AIRNSR) | downloaded | `tceq_air_permits` | Data-center backup-generator permits by county, often pre-construction |
| Comptroller Ch. 312/380 agreements, data-center registry, JETI | downloaded | `tx_comptroller` | Large-project incentives by county and date |
| NOAA GHCNh (12 airport stations) | downloaded | `noaa_ghcnh` | License-free hourly weather (ISD is frozen since Aug 2025) |
| PUDL tables (FERC 714 forecasts, EIA-860M changelog…) | downloaded | `pudl` | ERCOT's official 10-year peak forecasts 2006–2025 (backtest vintages) |
| Census PEP Vintage 2025 | downloaded | `census_pep` | County population and migration 2020–2025 |
| BLS QCEW NAICS 518210 | downloaded | `bls_qcew` | Data-center employment by county (many suppressed) |
| PUC Interchange filing lists | partly (index + chosen items) | `puct_filings` | Dockets 58481, 58777, 59772 |
| LBNL Queued Up | blocked (Cloudflare 403) | — | Needs a manual browser download; queue outcomes across RTOs (CC BY 4.0, per search results; not verified) |
| Census CBP 2023, Ch. 313 list, TWDB water-demand projections, Texas Demographic Center projections, sales-tax permits, RRC drilling permits | later | — | Lower value or manual/session-only downloads |
| SSWG cases, NSO notices, PUCT gen_tables.xls, EIA-930 subregions, FERC 714 hourly, ISD, EIA-861M | skip | — | Confidential, PDF-only, stale, redundant with what we have, or frozen |

Personal data: TPIT contacts, NP12-215-ER representatives, PUCT directory contacts, Ch. 380 officers and
RTP appendix staff emails are in the raw files as published. **Drop them at parse**; never carry them
into Parquet or the API.

## Supply, territories and ERCOT/PUCT

### PUCT electric CCN service areas
- PUCT's public ArcGIS item `366445b63acd4dbda6d60f9244e89c23`, "Electric Service Territory/Outage Map
  Locator Viewer". Its Experience Builder config lists
  `services6.arcgis.com/N6Lzvtb46cpxThhu/.../{COOP_DIST/FeatureServer/310, MUNI/FeatureServer/320, IOU/FeatureServer/300}`.
- 68 co-op, 72 muni and 8 IOU polygons, last edited 2026-06-29.
- Fields: COMPANY_NAME, COMPANY_TYPE, CCN_NO (sometimes a list, e.g. "30028,30170"),
  INDEPENDENT_SYSTEM_OPERATOR_REGIONAL_TRANSMISSION_ORGANIZATION, GENERATION_TRANSMISSION_COOPERATIVE,
  DATA_SOURCE(_DATE), phones, websites.
- The layers call themselves "UNOFFICIAL"; the official maps are county mylars at PUCT Central Records.
  Areas overlap. No license is stated.

### ERCOT TPIT
- Transmission Planning page, links "Transmission Project and Information Tracking" (current xlsx) and
  "Archived …" (zip, 74 workbooks, 2009–2026).
- Current file: July 13, 2026 ad hoc update, 2,127 projects (Future 1,429, Planned 358, Completed 262,
  Cancelled 78). Header on row 2.
- Columns include ERCOT Project Number, Transmission Status, Transmission Owner, Projected/Actual
  In-Service Date, county columns, RPG Number, SSWG bus numbers.
- ERCOT reuses old `/files/docs/<date>/` paths for new content.

### PUCT Project 58777, item 38
- ERCOT's "Preliminary Long-Term Load Forecast for Years 2026–2032" (2026-04-15).
- The zip's `Attachment A.pptx` slide 5 has requested large load by TSP, 2026 → 2032:
  - Oncor 5,018 → 109,554 MW; AEP 2,410 → 42,260; Brazos 100 → 32,150; Golden Spread 905 → 17,405;
    TNMP 1,753 → 13,427; LCRA 400 → 5,697; Lone Star 300 → 4,700; WETT 4,450; CenterPoint 4,026;
    Rayburn 1,961; CPS 1,600; STEC 1,078;
  - total 11,443 → 242,999 MW.
- Slide 4 splits the total by type: data centers 228,420 MW in 2032.
- These are raw, preliminary TSP submissions. The Batch Zero adjustment was approved in 59772 item 18
  (2026-06-18).
- Docket titles: 58481 is "Rulemaking to implement large load interconnection standards under PURA
  37.0561"; 58777 is the ERCOT reliability assessment / LTLF docket.

### ERCOT Regional Transmission Plan (PG7-048-M, RTID 24735)
- The listing holds the 2024 and 2025 packages. The planning page links the 2014–2023 archive zip (26 MB)
  and the 2022 addendum.
- Appendix B "Reliability Case-Load Forecast" by weather zone:
  - TSP-submitted load: 127,724 MW in 2027;
  - ERCOT 90th-percentile forecast: 86,577 MW;
  - the plan's load: 125,862 MW in 2027 and 164,508 MW in 2031.
- The same appendix lists generator additions by GINR, county and COD. The sheets are presentation-style.

### EIA-860M and EIA-860
- EIA-860M: 134 monthly files, Jul 2015 → Aug 2026. Future months sit inside HTML comments.
  - Sheets: Operating, Planned, Retired, Canceled or Postponed. Header on row 3.
  - There is no INR, so matching to GIS is fuzzy (name, county, MW, fuel).
  - Default download: latest + Decembers. The full history (~1 GB, not measured) is available with
    `--opt all_months=true`. PUDL's `core_eia860m__changelog_generators` already tracks every monthly
    change.
- EIA-860 2025 final (23.6 MB): the plant table has "Transmission or Distribution System Owner".

### ERCOT account lists and NOIE mapping
- NP4-160-SG (RTID 10008): weekly zips with 5 CSVs. NOIE_Mapping (817 rows) is the only table tying the
  NOIE zones to physical loads. PSSE_BUS_NUMBER joins TPIT's SSWG buses.
- NP12-215-ER (RTID 21129): MPList xlsx with a TDSP sheet (177 registrations).
- ERCOT members by segment 2013–2026: voluntary, a subset. "Base Texas REP, LLC" is a member.
- PUCT `/bulkcopy/*.csv`: 76 co-ops and 73 munis statewide; `pgc_facility.csv` has 1,560 ERCOT
  facilities with county and host territory.

### ERCOT Demand and Energy, Fuel Mix
- Demand and Energy: link text "Demand and Energy" on Helpful Resources and its year pages, 2008–2026.
  - Sheets: Demand (monthly peak with date and hour ending; 2026: 91,133.7 MW on 07/22, HE 18), Load
    Zones, Weather Zones.
  - The current-year file is overwritten monthly.
- Fuel Mix: Generation page, "Fuel Mix Report: <year>" plus the 2007–2024 zip (51 MB). No EMIL id.
  - Headers vary even inside a workbook ("Fuel" vs "Fuel Type", "Settlement Type " with a trailing
    space).

## Demand side, utilities and socioeconomic

### TCEQ Central Registry (data.texas.gov)
- Five regional datasets: msah-s2rv Central, 5eqq-7nad North, t34q-qzi3 DFW, tzyg-j7q4 Coastal & East,
  9iad-hrn8 Border & Permian. They are refreshed daily and found with the Socrata catalog API.
- `program_code='AIRNSR'` gives about 690k rows. Columns include reg_ent_name, re_phys_loc_addr_county,
  additional_id_status (PENDING/ACTIVE), indus_typ_cd, affil_begin_dt.
- Data-center hits by county: DFW, Central Texas (Bexar 11, Travis 5), rural North and West Texas
  (Wilbarger, Haskell, Fisher, Deaf Smith, Nolan, Mitchell…), mostly dated 2025–2026.
- NAICS is unreliable, so match names. There is no MW. The exact meaning of `affil_begin_dt` is not
  verified.
- Reproducible filter, run 2026-09-26: name matches `DATA ?CENTER|DATACENTER|DATA CTR` or `indus_typ_cd`
  = 518210, deduplicated by site (`ref_num_txt`), dated by the earliest `affil_begin_dt`.
  - It finds 88 sites. The yearly count of permit rows rises: 12 (2024), 19 (2025), 30 (2026 to date).
  - 37 sites have their first permit since 2025, spread over 26 counties; 34 of them are outside the big
    metros.
  - Treat it as a lower bound, and it has no MW. The per-county claim above came from the research
    agent's sampling.

### Texas Comptroller
- Ch. 312 abatements and Ch. 380/381 agreements: daily CSVs. The API (`api.comptroller.texas.gov/open-data/v1/tables/...`,
  declared in the SB 1340 `results.php` script) returns them in `downloadLink`. It needs a DataTables
  query with at least one column spec.
  - ch312-abatement-detail: 7,599 rows × 63 columns. NAICS is 94% empty; dates are MM/DD/YYYY text.
  - Data-center rows appear in Taylor (Lancium Abilene), Ellis, Wilbarger, Mitchell and Dickens.
- Qualifying data centers (64) and large data-center projects (103): HTML tables, no county.
- JETI current agreements: 21 rows, including 9 chip fabs in Grimes County. The API's CSV link is dead.
- Ch. 313 (legacy, 734 rows through 2022): later. The ISD-number → county FIPS rule (2n−1) fits every
  case checked, but it is **not verified** officially.

### Weather: NOAA GHCNh
- ISD / Global Hourly / ISD-Lite stopped updating in Aug–Oct 2025. GHCNh replaces them.
- Station-year Parquet files come from NCEI's bucket (`hourly/access/by-year/<Y>/parquet/GHCNh_<ID>_<Y>.parquet`),
  about 1 MB each.
- The 12 station ids are in `config/weather_points.yaml`.
- About 329 columns. Keep FM15 reports with a temperature. Timestamps appear to be UTC (**not verified**
  against the docs).
- U.S. federal data, so no license limit. This is the fallback to Open-Meteo's non-commercial terms.

### PUDL (Catalyst Cooperative)
- Release v2026.9.0, CC-BY-4.0 (attribution required, commercial use allowed). Public S3 bucket.
- `core_ferc714__yearly_planning_area_demand_forecast` holds ERCOT's 10-year summer/winter peak forecasts
  for report years 2006–2025.
- ERCOT co-ops and munis (Austin Energy, CPS, LCRA, Pedernales, Brazos) are **not** FERC 714 filers, so
  there is no utility-level hourly load for them.

### Census PEP, BLS QCEW
- PEP Vintage 2025: `co-est2025-alldata.csv`, 2 MB, Latin-1.
- QCEW annual county CSV by NAICS through the documented API. For 518210, 82 of 120 Texas county rows are
  suppressed; the leading counties are Travis, Dallas and Collin.

### Later / skipped
- Census CBP 2023 (12.9 MB zip).
- Texas Demographic Center projections: form download only.
- TWDB `HistoricalUseAndProjections.xlsx`: industrial and steam-electric water demand by county to 2080;
  a cheap proxy worth adding later.
- Sales-tax permits (noisy).
- RRC drilling permits (needs a web session).
- EIA-930 subregions: the 8 weather zones we already have.
- FERC 714 hourly (no ERCOT co-op/muni filers).
- EIA-861M: only CPS and Nueces EC among Texas co-ops/munis.
