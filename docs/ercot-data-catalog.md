# ERCOT Public Data Catalog for basecast (Base Power 48h Hackathon)

ERCOT publishes everything the MVP needs except one piece: generator queue data is public per project, with a stable INR ID, but large-load (≥75 MW) queue data is public only as monthly aggregate PDF decks. The large-load survival model therefore has to be built on aggregate stage counts and flows, not on per-project records. Every other P0 input has a machine-readable, downloadable source:
- GIS monthly xlsx;
- hourly weather-zone load back to 2003;
- CDR xlsx back to 2000;
- LTLF hourly xlsb/xlsx by weather zone;
- a ZIP-to-weather-zone table.

The one clear gap is an official county-to-zone table: ERCOT has none, so we build it ourselves.

## TL;DR
- **Generation queue: model per project. Large-load queue: aggregates only.** The GIS report (EMIL `PG7-200-ER`, Report Type ID 15933, monthly xlsx) has one row per project, keyed by INR. It carries county and milestone dates, which gives us survival events. The large-load queue exists publicly only as the monthly "Large Load Interconnection Status Update" slide decks, which report MW by status, load zone, type, TSP and year. There is no public project list, and ERCOT's internal LLI-# IDs are not published.
- **All three hints are confirmed:**
  - `pg7-200-er` / 15933 is correct.
  - ERCOT's Developer Portal "API limitations" page says the API "allows up to 30 requests per minute" (HTTP 429 above that), that "downloads of historic files via the API and WebUI are limited to 1,000 files at a time", and that "regions outside the United States of America are restricted".
  - `ercot.com/gridinfo/resource` is the Resource Adequacy page.
- **Recent changes that will break naive DAGs:**
  - **RTC+B (Dec 5, 2025)** removed ORDC energy adders, moved AS procurement into real time, and renamed or replaced several price-adder reports.
  - **May 2026 CDR:** not published. ERCOT posted a "Generation Resource Capacity Forecast" xlsx in its place.
  - **2026 LTLF:** still being adjusted via PUCT (Batch Zero basis).
  - **GIS series:** gained a companion Transmission Interconnection Costs report (`PG7-201-ER`) in March 2026.

## Key Findings

1. **GIS (generation queue).** The EMIL page lists: first run 2-1-2017, Chron-Monthly, xlsx, Display Duration 2555 days (~7 years), channels "Public, EWS". Independent analysis (Modo Energy) says historical GIS reports date back to the beginning of 2019. Small-generation (<10 MW) project details only appear from January 2022. The INR number (e.g., `20INR0290`) is the stable per-project key across snapshots. Projects can appear on more than one sheet in the same file (active sheets plus the commissioning update), so dedupe on INR plus snapshot date.
2. **Large loads.** ERCOT's public outputs are:
   - monthly TAC/LLWG status decks (PDF);
   - board "Interconnection and Grid Analysis Update" decks;
   - ERCOT Monthly recaps;
   - Batch Zero process documents.

   The March 13, 2026 deck defines five status buckets and reports 9,042 MW approved to energize, with an observed non-simultaneous peak of 4,004 MW in March 2026 (updated deck `March-TAC-Report-Updated_03262026.pptx`, slide 6). Its TSP breakdown folds groups with fewer than five customers into "Other" to protect customer data. This is an explicit confidentiality policy, so do not expect per-project releases.
3. **Hourly load by weather zone.** Available as annual files from 1995. The eight-weather-zone series starts in April 2003; before that, 11 control areas were reported. Formats: 2002–2014 xls, 2015 xls, 2016+ zip. The current-year file is updated monthly around the 9th.
4. **CDR.** Every edition we checked ships as PDF plus spreadsheet (xls through 2014, xlsx later). The earliest is "Capacity, Demand and Reserves Report – 2000" (xls only). The December 2025 CDR xlsx is 13 MB. The May 2026 CDR was not produced.
5. **LTLF.** The 2025 vintage has an hourly ERCOT and weather-zone forecast (xlsb, ~46 MB), monthly peak/energy (xlsx), and weather-year scenario files by weather zone (xlsx, 66–78 MB each). Archive year pages exist for 2013–2025. The 2026 preliminary LTLF (278,003 MW in 2029; 367,790 MW in 2032) is a PUCT filing and is under adjustment.
6. **Geography.** The only official ERCOT geographic crosswalk is ZIP code → weather zone: the "ZipToZone" sheet in Load Profiling Guide Appendix D. Load zones are defined electrically (buses), not by county. GIS carries County, so the county join happens on our side.

## Summary Table

| # | Dataset | EMIL ID / RTID | Format | Freq | Grain (space / time) | History | Access | Priority | Verified |
|---|---|---|---|---|---|---|---|---|---|
| 1 | GIS Report | PG7-200-ER / 15933 | xlsx | Monthly | Project (INR), county / snapshot | 2019+ (files); EMIL first run 2017 | No login (ercot.com); API archive | P0 | Verified at source |
| 1b | GIS Transmission Interconnection Costs | PG7-201-ER / 27292 | xlsx | Monthly (if TSP costs reported) | Project | Mar 2026+ | No login | P1 | Verified at source |
| 2 | Large Load Interconnection Status Update | none (committee/board docs) | PDF | Monthly | System; load zone; type; TSP; year | ~2024–present (decks) | No login | P0 | Verified (content); history depth not verified |
| 3 | Hourly Load Data Archives (Native Load) | none (web page) | zip/xls | Monthly (~9th) | 8 weather zones + ERCOT / hourly | 1995 (WZ since Apr 2003) | No login | P0 | Verified at source (columns not verified) |
| 3b | Actual System Load by Weather Zone | NP6-345-CD / 13101 | csv/zip | Daily | 8 WZ + total / hourly | API data from Dec 2023; archive ~7y | No login / API | P0 (incremental) | Partially verified |
| 4 | CDR | none (Resource Adequacy page) | pdf + xlsx | Semiannual (May/Dec) | System, CDR zones / seasonal-annual | 2000+ | No login | P0 | Verified at source |
| 5 | Long-Term Load Forecast | none (Load Forecast page) | xlsx/xlsb/docx | Annual | ERCOT + weather zone / hourly | Year pages 2013–2025 | No login | P0 | Verified at source |
| 6a | Load Profiling Guide Appendix D (ZipToZone) | none | xlsx | Ad hoc | ZIP → weather zone | Current (Apr 30, 2024 version) | No login | P0 | Verified at source |
| 6b | Settlement Points List & Electrical Buses Mapping | NP4-160-SG / 10008 | zip/csv | Model updates | Bus → settlement point / load zone | Rolling | No login / API | P1 | IDs from gridstatus (secondary) |
| 7a | RT Settlement Point Prices | NP6-905-CD / 12301 | csv/zip | Every 15 min | Node/hub/LZ / 15-min | API from Dec 2023; archive ~7y | No login / API | P1 | Partially verified |
| 7b | DAM Settlement Point Prices | NP4-190-CD / 12331 | csv/zip | Daily | Node/hub/LZ / hourly | same | No login / API | P1 | Partially verified |
| 7c | Historical RTM / DAM LZ & Hub Prices | NP6-785-ER / 13061; NP4-180-ER / 13060 | xlsx (zip) | Annual (updated) | Hub/LZ / 15-min, hourly | 2011+ | No login | P1 | Partially verified |
| 8a | DAM Clearing Prices for Capacity | NP4-188-CD / 12329; hist NP4-181-ER / 13091 | csv/zip; xlsx | Daily; annual | System / hourly | Hist by year | No login / API | P1 | Verified at source |
| 8b | RT Clearing Prices for Capacity (15-min, SCED) | NP6-331-CD; SCED-interval ID not verified; hist NP6-796-ER, NP6-795-ER | csv/zip | Per interval; weekly hist | System / 15-min, 5-min | Starts Dec 5, 2025 (RTC+B) | No login / API | P1 | Partially verified |
| 8c | RT Price Adders (post-RTC+B) | NP6-323-CD, NP6-324-CD; hist NP6-792-ER, NP6-793-ER | csv/zip | Per interval; weekly | System | Series break Dec 5, 2025 | No login / API | P2 | Verified at source |
| 9a | Fuel Mix (dashboard + Fuel Mix Report) | EMIL ID not verified | json / xlsx | 5-min / monthly | System by fuel | Multi-year (not verified) | No login | P1 | Not verified (IDs) |
| 9b | Solar/Wind actual + forecast by region | NP4-745-CD, NP4-746-CD, NP4-742-CD, NP4-743-CD | csv/zip | Hourly / 5-min | Geographic region | API | No login / API | P1 | IDs verified (release notes) |
| 10 | Seven-Day Load Forecast by Weather Zone | NP3-561-CD (RTID not verified); by Forecast Zone NP3-560-CD / 12311 | csv/zip | Hourly | WZ / hourly | API from Dec 2023 | No login / API | P1 | Partially verified |
| 11 | MORA | none (Resource Adequacy page) | pdf + xlsx | Monthly (1st Friday, 2 months ahead) | System | Dec 2023 edition onward | No login | P1 | Verified at source |
| 12 | 60-Day SCED / DAM Disclosures | Not verified (commonly NP3-965-ER / NP3-966-ER) | zip/csv | Daily (60-day lag) | Resource | API archive | API | P2 | Not verified |
| 13 | Unplanned Resource Outages / Hourly Resource Outage Capacity | RTID 22912; NP3-233-CD | xlsx; csv | Daily; hourly | Resource; zone | API | No login / API | P2 | IDs from gridstatus (secondary) |
| 14 | Demand and Energy report (peak records) | Not verified | xlsx | Monthly | System | Not verified | No login | P2 | Not verified |
| X | Electric Retail Service Territories (HIFLD) | n/a | shapefile/GeoJSON | Irregular | Utility polygon | Last HIFLD Open snapshot Sep 30, 2024 | No login (archives/EIA Atlas) | P1 | Partially verified |

## Dataset Cards

### 1. GIS Report: Generator Interconnection Status (P0)
- **Where:** `https://www.ercot.com/mp/data-products/data-product-details?id=pg7-200-er`. EMIL `pg7-200-er`, Report Type ID **15933**. Status Active, first run 2-1-2017, EMIL last updated 2-9-2026. Channel "Public, EWS", Display Duration 2555 days, rule PG7.1(2). Legacy MIS URL pattern (used by older gridstatus): `mis.ercot.com/misapp/GetReports.do?reportTypeId=15933`. Public API archive: `GET https://api.ercot.com/api/public-reports/archive/PG7-200-ER` (pattern verified for other products; not tested for this one).
- **Format / cadence:** one xlsx per month, published early the following month (e.g., the September 2024 report posted in October 2024).
- **Grain:** one row per project (INR), with county. Known sheets: "Project Details - Large Gen", "Project Details - Small Gen", "Inactive Projects", "Cancellation Update", plus a commissioning update (gridstatus PR #810).
- **Columns (verified from gridstatus parser):** `INR` (header marker row), `Screening Study Started` (gridstatus maps it to Queue Date), `County`, `Capacity (MW)`. ERCOT study tables derived from GIS show GINR, Project Name, County, Capacity (MW), Fuel, Projected COD. FIS and IA milestone date columns (e.g., FIS Approved, IA Signed) exist per Modo/ercotqueue analyses; exact header text is **not verified**, so read headers dynamically.
- **History:** 2019+ monthly files per Modo Energy. The API archive advertises ≥7 years. Small-gen details only from Jan 2022.
- **gridstatus:** `Ercot().get_interconnection_queue()` parses the current file. The newer version parses all four sheets; confirm the version you pin.
- **Used by:** generation survival model, supply-side overlay, county map. **P0.**
- **Gotchas:**
  - Header row position varies (scan for "INR").
  - Duplicate INRs across sheets.
  - The cancelled sheet has sparse columns.
  - Since March 2026 the monthly series also includes the `PG7-201-ER` cost file (Report Type ID 27292, first run 3-1-2026). Don't ingest it into the queue table.
- **Confidence:** IDs and cadence verified at source; column list partially verified.

### 2. Large Load Interconnection Queue ≥75 MW (P0)
- **Where (all PDF, no EMIL product found):**
  - Monthly "Large Load Interconnection Status Update" decks from the Large Load Integration Team to TAC/LLWG, e.g., `https://www.ercot.com/files/docs/2026/03/12/March-TAC-Report.pdf` (March 13, 2026).
  - Board "Interconnection and Grid Analysis Update" decks (Apr 2026 Item 9, May 2026 Item 8).
  - ERCOT Monthly recaps (e.g., the January 2026 issue).
  - Process page `https://www.ercot.com/services/rq/large-load-integration` (Batch Zero, PGRR145 forms; last updated Sep 22, 2026).
  - Legislative decks (Senate B&C, April 2026).
  - NPRR1267 ("Large Load Interconnection Status Report", Jan 8, 2025 docx) formalizes such a report. Its approval status is **not verified**.
- **Content (verified, March 2026 deck):**
  - Queue over the past 12 months.
  - Status buckets: Observed Energized; Approved to Energize but Not Operational; Planning Studies Approved; Under ERCOT Review; No Studies Submitted.
  - Approvals by zone and project type (LZ_NORTH, LZ_SOUTH, LZ_HOUSTON aggregated as "Other").
  - Distribution by size, TSP, submittal date, in-service date, type and load zone.
  - Observed non-simultaneous (4,004 MW) and simultaneous (3,522 MW) peaks of approved loads in March 2026: `https://www.ercot.com/files/docs/2026/03/27/March-TAC-Report-Updated_03262026.pptx`, slides 6 and 7 (checked 2026-09-26). The first version of the deck (`March-TAC-Report.pdf`, 2026-03-12, pp. 6-7) printed 3,883 and 3,801 MW labeled "March 2025"; ERCOT replaced them.
- **Headline figures:**
  - Jan 21, 2026: the ERCOT Monthly January 2026 recap (published Feb 13, 2026) says tracked large-load projects "total approximately 232,500 megawatts" and "8,786 MW of large load demand has received approval to energize".
  - March 2026: 9,042 MW approved. The ERCOT deck also reports 137 new LLI submissions of ~140,000 MW still being processed. Zero-Emission Grid's summary of that deck (a secondary source) says these requests would lift the total queue "from approximately ~238,000 MW to nearly ~380,000 MW".
- **Grain:** aggregate only (system, load zone, type, TSP, year). **No per-project fields.** ERCOT assigns each request an LLI-# internally (per the Dec 2025 Large Load Q&A), but it is not published.
- **Access:** no login. Values sit in charts, so we extract them manually or with PDF/table tooling.
- **Used by:** large-load realization model (aggregate stage-transition rates), LTLF comparison. **P0.**
- **Gotchas:**
  - Category definitions changed with PGRR115/Batch Zero (PGRR145).
  - Unapproved MW are reclassified to "No Studies Submitted".
  - From 2027, only loads with executed interconnection agreements (PUC Project 58481) enter the large-load forecast.
  - Old decks live on committee meeting pages; discover them by crawling meeting pages.
- **Confidence:** content verified; completeness of the monthly archive **not verified**.

### 3. Hourly Load Data Archives: Native Load by Weather Zone (P0)
- **Where:** `https://www.ercot.com/gridinfo/load/load_hist`. Examples: `/files/docs/2026/02/10/Native_Load_2026.zip` (updated Sep 11, 2026), `/files/docs/2025/02/11/Native_Load_2025.zip`, `/files/docs/2015/10/22/2014_ercot_hourly_load_data.xls`.
- **Format:** 1995–1997 txt/zip (FERC 714); 1998–2000 zip; 2002–2014 xls; 2015 xls; 2016+ zip. There is no 2001 file.
- **Grain:** hourly (hour ending), 8 weather zones (Coast, East, Far West, North, North Central, South, South Central, West) plus ERCOT total, since April 2003. Earlier years use 11 control areas.
- **Latency:** previous month available ~9th of the month (after settlement).
- **Columns:** expected `Hour Ending, COAST, EAST, FWEST, NORTH, NCENT, SOUTH, SCENT, WEST, ERCOT`. **Not verified** (zip not opened); detect headers at parse time.
- **gridstatus:** `get_load_by_weather_zone(date)` for recent days via NP6-345-CD. Archive helpers: method name not verified.
- **Used by:** weather-normalized load model, peak-by-region backtests. **P0.**
- **Gotchas:**
  - DST: hour ending "24:00" and duplicated hour-2 rows on fall-back (sometimes flagged with a DST column).
  - The current-year zip is overwritten monthly, so snapshot it immutably.
  - The page text still says "1995–2016" even though files go to 2026.

### 3b. Actual System Load by Weather Zone (NP6-345-CD) (P0 incremental)
- `https://www.ercot.com/mp/data-products/data-product-details?id=NP6-345-CD`, RTID 13101 (gridstatus constant). Daily file for the previous operating day. Actual hourly demand by forecast zone is `NP6-346-CD` (RTID 14836).
- Public API data from mid-Dec 2023; older days via archive files.
- Use it to fill the gap between the last monthly archive and today.

### 4. CDR: Capacity, Demand and Reserves (P0)
- **Where:** `https://www.ercot.com/gridinfo/resource` plus year pages `/gridinfo/resource/2000` … `/2025`. Pre-2012 items are also in `/news/presentations/2011`.
- **Editions (verified):**
  - Dec 2025: pdf 8.5 MB + xlsx 13 MB (`/files/docs/2025/12/19/CapacityDemandandReservesReport_December2025.xlsx`).
  - May 2025 and May 2025 Revised (xlsx ~962 KB).
  - Dec 2024 Revised (Feb 13, 2025).
  - May 2024 Revised.
  - 2014: Feb, May and Dec editions as xls + pdf, plus May 2013.
  - Earliest: "Capacity, Demand and Reserves Report – 2000", xls only (`/files/docs/2017/11/03/CDR_Summer_02082000.xls`).
- **Machine-readable?** Yes, a spreadsheet for every edition checked. Tab layout changes across vintages, so write a per-vintage parser map. The Dec 2025 CDR uses hourly data from the April 2025 ERCOT Adjusted Forecast in its Load tab.
- **Recent change:** **the May 2026 CDR was not produced.** ERCOT requested a PUCT good-cause exception (Project 55999) because the 2026 LTLF is being adjusted. It posted "Generation Resource Capacity Forecast" (`/files/docs/2026/05/18/Generation_Resource_Forecast_May2026.xlsx`, 352 KB) instead. An Aug 2026 ERCOT presentation warns that Batch Zero delays "could delay" the December 2026 CDR; that is a possibility, not a confirmed delay. The signed PUCT order is **not verified**.
- **Used by:** "official forecast vs. realized" backtest, planned-capacity overlay. **P0.**
- **Gotchas:** methodology breaks (ELCC for storage, the 2025 load forecast adopting TSP large loads, "Protocol Prescribed" resources), revised re-issues, and the season definitions.

### 5. Long-Term Load Forecast (P0)
- **Where:** `https://www.ercot.com/gridinfo/load/forecast`, year pages `/forecast/2013` … `/2025` (no 2015 link shown).
- **2025 files:**
  - `2025-ERCOT-Monthly-Peak-Demand-and-Energy-Forecast.xlsx`.
  - `2025_LTLF_Report.docx`.
  - `TSP-Provided-Hourly-Forecast.xlsb` (46.5 MB) and `ErcotAdjustedForecast.xlsb` (46.2 MB): ERCOT and weather-zone hourly.
  - `ERCOT-Peak-Demand-Scenarios.xlsx`.
  - `Summer-and-Winter-Peaks.xlsx` (weather-zone coincident and non-coincident peaks, 2025–2031).
  - Weather-year scenario files per weather zone (`Coast.xlsx` … `West.xlsx`, 66–78 MB, weather years 1980–2024 for forecast years 2025–2035).
- **Method:** hourly 10-year forecast using economic drivers and 2008–2022 weather.
- **2026 vintage:**
  - Preliminary LTLF filed Apr 15, 2026 in PUCT Project 58777: 278,003 MW in 2029, 367,790 MW in 2032.
  - In its letter in PUCT Project 58777 (Item 38), ERCOT said it "currently projects that summer 2026 peak load will fall within a range of approximately 90,500 MW to 98,000 MW, compared with the preliminary LTLF's forecasted peak demand of approximately 112,000 MW".
  - Adjustment request filed May 18, 2026 (Project 59772). ERCOT recommends the Batch Zero option; PUC Staff supported it. ERCOT Market Notice M-B051826-01 says the Batch Zero–adjusted forecast "would not be finalized until mid-August 2026".
  - 2026 files were **not yet on the Load Forecast page** when we fetched it. Final status is **not verified**.
- **Used by:** official-forecast benchmark; large-load realization calibration. **P0.**
- **Gotchas:** xlsb requires `pyxlsb`; files are very large; TSP-provided and ERCOT-adjusted variants differ; vintage definitions change with 16 TAC §25.370 (effective March 1, 2026).

### 6. Geographic mappings (P0)
- **Official ERCOT source:** Load Profiling Guide Appendix D, "Profile Decision Tree" xlsx (`/files/docs/2024/04/30/Appendix_D_Profile_Decision_Tree_050124.xlsx`, Apr 30, 2024). Its **ZipToZone** sheet maps ZIP codes to weather zones. It is listed on `https://www.ercot.com/mktrules/guides/loadprofiling/current`.
- **County → weather zone:** no official table on ercot.com. The media-kit weather-zone map warns it "does not necessarily include all counties". ERCOT's LTLF says county data was "mapped into the weather zones", but no table is published.
- **Load zones:** defined by electrical buses. The mapping comes from `NP4-160-SG` Settlement Points List and Electrical Buses Mapping (RTID 10008, per gridstatus).
- **Datasets that carry county:** GIS (County, plus a CDR reporting zone). The large-load decks do **not** carry county.
- **Recommendation:** derive county ↔ weather zone from ZipToZone plus a Census ZIP (ZCTA)-to-county crosswalk (majority-area rule), and store it with provenance. Use hub/load zone only for prices.

### 7. Settlement Point Prices (P1)
- **RT:** `NP6-905-CD` (RTID 12301), 15-min SPPs for resource nodes, hubs and load zones. API endpoint `np6-905-cd/spp_node_zone_hub` is **not verified**.
- **DAM:** `NP4-190-CD` (RTID 12331). API endpoint `https://api.ercot.com/api/public-reports/np4-190-cd/dam_stlmnt_pnt_prices`, seen in the api-specs discussion.
- **History:** `NP6-785-ER` (13061) and `NP4-180-ER` (13060), annual hub/LZ files from 2011.
- **gridstatus:** `get_spp(date, market=..., location_type=...)`; `get_rtm_spp(year)` / `get_dam_spp(year)` for annual history (older name `get_historical_rtm_spp`).
- **Gotchas:**
  - RTC+B changed price formation from Dec 5, 2025: the ORDC adder is gone, and there is an RT system-wide offer cap of $2,000/MWh vs a DA cap of $5,000/MWh (Yes Energy; secondary).
  - Price corrections (`NP4-196-M`, `NP4-197-M`).
  - DST repeated intervals flag.

### 8. Ancillary Service Prices (P1). Watch the RTC+B break
- **DAM MCPC:** `NP4-188-CD` (RTID 12329), API `np4-188-cd/dam_clear_price_for_cap`. Historical by year: `NP4-181-ER` (13091). gridstatus: `get_mcpc_dam()` (long format per AS type).
- **RT MCPC (new with RTC+B, from Dec 5, 2025):**
  - `NP6-331-CD` "Real-Time Clearing Prices for Capacity by 15-Minute Settlement Interval" (time-weighted RT MCPC plus RT Reliability Deployment Price Adders).
  - "Real-Time Clearing Prices for Capacity by SCED Interval" (5-min); EMIL ID **not verified**.
  - `NP6-329-CD` "RTD Indicative Real-Time MCPC".
  - Weekly history: `NP6-796-ER` (15-min) and `NP6-795-ER` (SCED interval), both added to the Public API.
  - Products: Reg-Up, Reg-Down, RRS, Non-Spin, ECRS.
- **Adders:**
  - `NP6-323-CD`, renamed "Real-Time Price Adders by SCED Interval". ORDC adders are removed; RTRDPA remains.
  - `NP6-324-CD` 15-min, formerly "Real-Time ORDC and Reliability Deployment Prices…".
  - Weekly history: `NP6-792-ER`, `NP6-793-ER`.
  - `NP4-212-CD`: AS Demand Curves.
  - ERCOT's RTM page says several pre-RTC+B products were "Replaced by NP6-653-M" and remain linked only "until December 2026".
- **Gotchas:**
  - There are no pre-Dec-2025 RT MCPCs. Before RTC+B, the RT AS value lived in ORDC adders on energy.
  - SASM is gone.
  - Separate "RTC Market Trials" (`*-rtcmt`) products exist; exclude them.

### 9. Fuel mix, solar and wind (P1)
- **Dashboard:** `https://www.ercot.com/gridmktinfo/dashboards/fuelmix` (JSON behind it; gridstatus `get_fuel_mix(date)`, 5-min).
- **Fuel Mix Report:** "actual generation by fuel type for each 15-minute settlement interval, plus monthly and YTD totals", posted as annual workbooks under Helpful Resources. EMIL ID **not verified**.
- **Solar / wind:** `NP4-745-CD` (solar hourly actual+forecast by geographic region), `NP4-746-CD` (solar 5-min actual), `NP4-742-CD` (wind hourly actual+forecast), `NP4-743-CD` (wind 5-min).
- **Gotchas:** dashboard capacity totals differ from MORA because of co-located large flexible loads.

### 10. Seven-Day Load Forecast by Weather Zone (P1)
- `NP3-561-CD` (hourly by weather zone, current day plus 6 days). By forecast zone: `NP3-560-CD` (RTID 12311). gridstatus: `get_load_forecast()`. The system-wide multi-model MTLF is a separate product.
- MTLF accuracy (day-ahead backcast xlsx) is published monthly on the Load Forecast page, e.g., the Aug 2026 file posted Sep 2, 2026.

### 11. MORA (P1)
- On the Resource Adequacy page. Released the first Friday of each month, two months ahead; the first edition covered December 2023. PDF plus xlsx pair (`MORA_[Month][Year].xlsx`), e.g., November 2026 posted Sep 3, 2026 (xlsx 244.8 KB). Revised versions may be posted.
- Use: near-term risk and capacity context for pricing. Deterministic load is P50.

### 12–14. P2
- **60-day SCED/DAM disclosures:** IDs **not verified** in this pass (commonly NP3-965-ER / NP3-966-ER). gridstatus has 60-day parsers (`ercot_60d_utils`). The RTC+B release added 2-/3-day disclosures: `NP3-906-EX`, `NP3-907-EX`, `NP3-908-ER`, `NP3-909-ER`, `NP3-914-EX`, `NP3-915-EX`.
- **Outages:** Unplanned Resource Outages Report (RTID 22912) and Hourly Resource Outage Capacity `NP3-233-CD` (gridstatus constants). **Not verified at source.**
- **Demand and Energy report:** not located. Peak records are **not verified** at source.

### X. Outside ERCOT: utility territories and co-ops/munis (P1)
- **HIFLD "Electric Retail Service Territories"** (ORNL/LANL/INL; polygons with fields including ZIP, TELEPHONE, VAL_METHOD, VAL_DATE, NAICS_CODE, SOURCEDATE). **HIFLD Open was deactivated Aug 26, 2025.** Use the archived copy (DataLumos project 239091, snapshot 9/30/2024) or the EIA U.S. Energy Atlas layer.
- **PUCT service-area maps and an authoritative ERCOT co-op/municipal list:** **not verified** in this pass. The NOIE load zones LZ_AEN, LZ_CPS, LZ_LCRA and LZ_RAYBN identify the largest municipal/co-op areas. Build the full list from EIA-861 utility data filtered to ERCOT (not verified here).

## Answers to the 7 Questions

1. **Large loads: per project or aggregate?** Aggregate only:
   - **Fields:** MW by status bucket (5 categories), load zone, project type (e.g., data center, crypto, industrial, oil & gas), size bin, TSP (small groups folded into "Other"), submittal year, in-service year, and observed peaks of energized load.
   - **Since:** monthly decks appear throughout 2025–2026. The earliest edition is **not verified**.
   - **Old snapshots:** they stay posted as meeting materials (dated `/files/docs/YYYY/MM/DD/` URLs), so snapshot them yourself. No LLI-# or project names are public.
2. **GIS:**
   - **Monthly reports since 2019:** yes, via the EMIL page (7-year display duration) and the Public API archive (`/archive/PG7-200-ER`, bulk POST download of up to 1,000 docIds). The tail end may age out; start the backfill now.
   - **Stable ID:** yes, INR (format `YYINRnnnn`).
   - **Survival events:** entry = `Screening Study Started`; study stage = FIS started/approved dates; agreement = IA signed date; success = approval to synchronize / commercial operation (projects move to the commissioning update, then drop off); failure = appearance on "Cancellation Update" or "Inactive Projects" (inactive per Planning Guide 5.2.5). Exact milestone header names are **not verified**.
3. **County:** GIS carries County. No ERCOT load, price or large-load dataset does. There is no official county ↔ weather zone ↔ load zone table. The official crosswalk is ZIP → weather zone (LPG Appendix D ZipToZone). Load zones map via settlement points and buses (`NP4-160-SG`).
4. **Hourly weather-zone load:** from April 2003 (8 zones), with system/control-area data back to 1995. xls through 2015, zip annually since 2016.
5. **CDR:** editions from 2000 (xls), semiannual. Each checked edition has an xls/xlsx beside the PDF. May 2026 was replaced by the Generation Resource Capacity Forecast xlsx.
6. **Post-RTC+B RT AS prices:**
   - `NP6-331-CD` (15-min settlement MCPC);
   - the SCED-interval MCPC product (ID not verified);
   - `NP6-329-CD` (RTD indicative);
   - history `NP6-796-ER` / `NP6-795-ER`;
   - adders `NP6-323-CD` / `NP6-324-CD` (renamed);
   - DAM remains `NP4-188-CD`.
7. **Retention and backfill:**
   - **Public API:** ERCOT's API Explorer Support page says row-level search "only goes back as far as November 30, 2023", and older data must be downloaded as file packages. The file archive covers at least 7 years.
   - **Limits:** per ERCOT's Developer Portal "API limitations" page, "up to 30 requests per minute", historic downloads "limited to 1,000 files at a time", and "regions outside the United States of America are restricted".
   - **Auth:** free registration, Azure B2C ID token plus `Ocp-Apim-Subscription-Key`. Tokens need periodic refresh; gridstatus assumes a one-hour lifetime (`TOKEN_EXPIRATION_SECONDS = 3600`), which ERCOT has not documented. In ercot/api-specs Discussion #113, ERCOT suspended the key of a user sending "about 4 requests per hour", citing "high failure rate and long query duration", so throttle and retry politely.
   - **ercot.com EMIL pages** show a rolling window (GIS: 2555 days).
   - **Backfill path:** annual/historical `-ER` products and web archives (load 1995+, CDR 2000+, LTLF 2013+, SPP 2011+), then the API file archive for daily products, then API row queries for 2023+. For older gaps, submit a Public Portal data request.

## MVP Recommendation (48h) and Ingestion Order
1. `geo_ziptozone` plus a Census ZCTA-county crosswalk, building the county↔WZ table (a join key everything else needs).
2. `gis_report`: backfill all monthly files 2019→now via the API archive; parse sheets; key on INR+snapshot. This is the survival model's training set.
3. `hourly_load_archive` 2003→now, plus `actual_load_weather_zone` for the current month. This drives weather-normalized peaks by WZ.
4. `ltlf` (2025 ERCOT Adjusted hourly + Summer-and-Winter-Peaks) and `cdr` (Dec 2025 xlsx, plus 2–3 older vintages for backtest).
5. `large_load_status`: hand-extract the last ~12 monthly decks into a small CSV (date × status × zone MW) and apply aggregate realization rates.
6. Stretch: `spp_hist` (hub/LZ annual) and `mora` for pricing context.

Skip AS prices, 60-day data and outages for the MVP. Post-RTC+B AS history is under a year old and not needed for peak MW.

## Caveats
- **Not verified:** actual column headers inside the Native_Load zips and the GIS milestone columns; several EMIL IDs (SCED-interval RT MCPC, Fuel Mix Report, 60-day, Demand & Energy); gridstatus method names beyond `get_interconnection_queue`, `get_load_by_weather_zone`, `get_mcpc_dam`, `get_spp`, `get_fuel_mix`. No code was executed and no file sizes were measured beyond those listed on ercot.com.
- The GIS history start is sourced from Modo Energy (secondary). ERCOT's EMIL lists a first run of 2017.
- LTLF 2026 and the December 2026 CDR are in flux. Re-check `/gridinfo/load/forecast` and `/gridinfo/resource` weekly.

---

## `data/catalog.yaml`

```yaml
- id: gis_report
  name: "GIS Report (Generator Interconnection Status)"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=pg7-200-er"
  api_endpoint: "https://api.ercot.com/api/public-reports/archive/PG7-200-ER  # report_type_id=15933; not tested"
  format: xlsx
  frequency: monthly
  spatial_grain: project (INR), county
  temporal_grain: monthly snapshot
  history_start: "2019-01 (files, per Modo Energy); EMIL first run 2017-02-01"
  access: "no login on ercot.com; Public API (registration, 30 req/min, 1000 files/download, US-only)"
  gridstatus_method: "Ercot().get_interconnection_queue()"
  priority: P0
  used_by: [generation_survival_model, supply_overlay, county_map]
  caveats: "header row varies (scan for 'INR'); INR duplicated across sheets; small-gen detail only since 2022-01; PG7-201-ER cost file in same series since 2026-03; milestone header names not verified"
  verified: "verified at source (IDs/cadence); columns partially verified"

- id: gis_interconnection_costs
  name: "GIS Report for Transmission Interconnection Costs"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=pg7-201-er"
  api_endpoint: "report_type_id=27292; API path not verified"
  format: xlsx
  frequency: monthly (only if TSPs report costs)
  spatial_grain: project
  temporal_grain: monthly
  history_start: "2026-03"
  access: no login
  gridstatus_method: none
  priority: P1
  used_by: [generation_survival_model_features]
  caveats: "new series; months with no TSP costs have no file"
  verified: verified at source

- id: large_load_status
  name: "Large Load Interconnection Status Update (TAC/LLWG/Board decks)"
  source_url: "https://www.ercot.com/files/docs/2026/03/12/March-TAC-Report.pdf"
  api_endpoint: none
  format: pdf
  frequency: monthly
  spatial_grain: system, load zone, project type, TSP (aggregated)
  temporal_grain: monthly snapshot; submittal/in-service year bins
  history_start: "not verified (decks seen 2025-2026)"
  access: no login
  gridstatus_method: none
  priority: P0
  used_by: [large_load_realization_model, ltlf_benchmark]
  caveats: "aggregate only, no per-project IDs (LLI-# internal); charts need manual/PDF extraction; categories changed with PGRR115/PGRR145 Batch Zero; TSPs with <5 customers folded into Other"
  verified: "verified at source (content); archive depth not verified"

- id: hourly_load_archive
  name: "Hourly Load Data Archives (Native Load by Weather Zone)"
  source_url: "https://www.ercot.com/gridinfo/load/load_hist"
  api_endpoint: none
  format: "zip (2016+), xls (2002-2015), txt/zip (1995-2000)"
  frequency: monthly (~9th, current-year file overwritten)
  spatial_grain: 8 weather zones + ERCOT (control areas before 2003-04)
  temporal_grain: hourly (hour ending)
  history_start: "1995 (weather zones since 2003-04; no 2001)"
  access: no login
  gridstatus_method: "not verified"
  priority: P0
  used_by: [weather_normalized_load_model, peak_backtest]
  caveats: "columns not verified (expected Hour Ending, COAST, EAST, FWEST, NORTH, NCENT, SOUTH, SCENT, WEST, ERCOT); DST duplicate/missing hour; snapshot current-year file immutably"
  verified: "verified at source (files); columns not verified"

- id: actual_load_weather_zone
  name: "Actual System Load by Weather Zone"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP6-345-CD"
  api_endpoint: "report_type_id=13101 (gridstatus); API path not verified"
  format: csv (zip)
  frequency: daily
  spatial_grain: 8 weather zones + total
  temporal_grain: hourly
  history_start: "API rows ~2023-12; archive files ~7 years"
  access: "no login / Public API"
  gridstatus_method: "Ercot().get_load_by_weather_zone(date)"
  priority: P0
  used_by: [weather_normalized_load_model]
  caveats: "use to bridge the latest month not yet in archive; DST"
  verified: partially verified

- id: cdr
  name: "Capacity, Demand and Reserves Report (CDR)"
  source_url: "https://www.ercot.com/gridinfo/resource"
  api_endpoint: none
  format: "pdf + xlsx (xls through 2014; 2000 xls only)"
  frequency: semiannual (May, December)
  spatial_grain: system, CDR zones
  temporal_grain: seasonal/annual, 5-year horizon (10-year in older editions)
  history_start: "2000"
  access: no login
  gridstatus_method: none
  priority: P0
  used_by: [forecast_vs_actual_backtest, planned_capacity_overlay]
  caveats: "May 2026 CDR not produced (replaced by Generation_Resource_Forecast_May2026.xlsx); Dec 2026 may be delayed; tab layouts differ by vintage; revised re-issues"
  verified: verified at source

- id: ltlf
  name: "ERCOT Long-Term Load Forecast (LTLF)"
  source_url: "https://www.ercot.com/gridinfo/load/forecast"
  api_endpoint: none
  format: "xlsx, xlsb (~46 MB), docx"
  frequency: annual (plus PUCT-driven adjustments)
  spatial_grain: ERCOT + weather zone
  temporal_grain: hourly, 10-year horizon; monthly peak/energy
  history_start: "2013 (year pages)"
  access: no login
  gridstatus_method: none
  priority: P0
  used_by: [official_forecast_benchmark, large_load_realization_calibration]
  caveats: "2026 preliminary LTLF (PUCT 58777) under adjustment (PUCT 59772, Batch Zero basis), files not on page when checked; xlsb needs pyxlsb; TSP-provided vs ERCOT-adjusted variants"
  verified: verified at source

- id: geo_ziptozone
  name: "Load Profiling Guide Appendix D - Profile Decision Tree (ZipToZone)"
  source_url: "https://www.ercot.com/files/docs/2024/04/30/Appendix_D_Profile_Decision_Tree_050124.xlsx"
  api_endpoint: none
  format: xlsx
  frequency: ad hoc
  spatial_grain: ZIP code -> weather zone
  temporal_grain: static
  history_start: "2024-04-30 version"
  access: no login
  gridstatus_method: none
  priority: P0
  used_by: [geo_join_county_weatherzone]
  caveats: "no official county table; derive county via Census ZCTA crosswalk; load zones are bus-based"
  verified: verified at source

- id: settlement_points_mapping
  name: "Settlement Points List and Electrical Buses Mapping"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP4-160-SG"
  api_endpoint: "report_type_id=10008 (gridstatus)"
  format: zip/csv
  frequency: with network model updates
  spatial_grain: bus -> settlement point / load zone / hub
  temporal_grain: static per model
  history_start: rolling
  access: no login
  gridstatus_method: "not verified"
  priority: P1
  used_by: [price_module, geo_join]
  caveats: "includes NOIE zones (LZ_AEN, LZ_CPS, LZ_LCRA, LZ_RAYBN)"
  verified: "not verified at source (IDs from gridstatus)"

- id: spp_rt
  name: "Settlement Point Prices at Resource Nodes, Hubs and Load Zones (RT)"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD"
  api_endpoint: "report_type_id=12301; path not verified"
  format: csv (zip)
  frequency: every 15 minutes
  spatial_grain: node, hub, load zone
  temporal_grain: 15-minute
  history_start: "API rows ~2023-12; archive ~7 years; annual hub/LZ via NP6-785-ER since 2011"
  access: "no login / Public API"
  gridstatus_method: "Ercot().get_spp(date, market='REAL_TIME_15_MIN'); get_rtm_spp(year)"
  priority: P1
  used_by: [pricing_module]
  caveats: "RTC+B (2025-12-05) removed ORDC adder, RTSWCAP $2,000; price corrections NP4-197-M; DST flag"
  verified: partially verified

- id: spp_dam
  name: "DAM Settlement Point Prices"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP4-190-CD"
  api_endpoint: "https://api.ercot.com/api/public-reports/np4-190-cd/dam_stlmnt_pnt_prices"
  format: csv (zip)
  frequency: daily
  spatial_grain: node, hub, load zone
  temporal_grain: hourly
  history_start: "API rows ~2023-12; annual hub/LZ via NP4-180-ER since 2011"
  access: "no login / Public API"
  gridstatus_method: "Ercot().get_spp(date, market='DAY_AHEAD_HOURLY'); get_dam_spp(year)"
  priority: P1
  used_by: [pricing_module]
  caveats: "price corrections NP4-196-M; DST"
  verified: partially verified

- id: spp_hist_hub_lz
  name: "Historical RTM and DAM Load Zone and Hub Prices"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP6-785-ER"
  api_endpoint: "RTM report_type_id=13061; DAM NP4-180-ER report_type_id=13060"
  format: xlsx (zip)
  frequency: annual file, updated
  spatial_grain: hub, load zone
  temporal_grain: 15-minute (RTM), hourly (DAM)
  history_start: "2011"
  access: no login
  gridstatus_method: "get_rtm_spp(year) / get_dam_spp(year)"
  priority: P1
  used_by: [pricing_module]
  caveats: "one sheet per month in workbook (not verified)"
  verified: partially verified

- id: as_dam_mcpc
  name: "DAM Clearing Prices for Capacity (+ Historical NP4-181-ER)"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP4-188-CD"
  api_endpoint: "np4-188-cd/dam_clear_price_for_cap (report_type_id=12329); hist NP4-181-ER (13091)"
  format: csv (zip); xlsx (historical)
  frequency: daily; annual
  spatial_grain: system
  temporal_grain: hourly
  history_start: "historical by year (start not verified)"
  access: "no login / Public API"
  gridstatus_method: "Ercot().get_mcpc_dam(date)"
  priority: P1
  used_by: [pricing_module]
  caveats: "product set Reg-Up, Reg-Down, RRS, Non-Spin, ECRS; exclude RTC Market Trials (-rtcmt) products"
  verified: verified at source

- id: as_rt_mcpc
  name: "Real-Time Clearing Prices for Capacity (15-min and SCED interval)"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP6-331-CD"
  api_endpoint: "NP6-331-CD; SCED-interval EMIL ID not verified; hist NP6-796-ER, NP6-795-ER (added to API)"
  format: csv (zip)
  frequency: per interval; weekly historical
  spatial_grain: system
  temporal_grain: 15-minute; 5-minute (SCED)
  history_start: "2025-12-05 (RTC+B go-live)"
  access: "no login / Public API"
  gridstatus_method: "not verified"
  priority: P1
  used_by: [pricing_module]
  caveats: "no pre-RTC+B equivalent; includes RT Reliability Deployment Price Adders; RTD indicative MCPC in NP6-329-CD"
  verified: partially verified

- id: rt_price_adders
  name: "Real-Time Price Adders by SCED Interval / 15-min (post-RTC+B)"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=NP6-324-CD"
  api_endpoint: "NP6-323-CD, NP6-324-CD; hist NP6-792-ER, NP6-793-ER"
  format: csv (zip)
  frequency: per interval; weekly
  spatial_grain: system
  temporal_grain: 5-minute; 15-minute
  history_start: "series break 2025-12-05"
  access: "no login / Public API"
  gridstatus_method: "not verified"
  priority: P2
  used_by: [pricing_module]
  caveats: "renamed; ORDC adders removed, RTRDPA remains; several legacy products replaced by NP6-653-M, links kept until 2026-12"
  verified: verified at source

- id: fuel_mix
  name: "Fuel Mix (dashboard) and Fuel Mix Report"
  source_url: "https://www.ercot.com/gridmktinfo/dashboards/fuelmix"
  api_endpoint: "not verified"
  format: json (dashboard); xlsx (report)
  frequency: 5-minute; monthly/annual workbook
  spatial_grain: system by fuel
  temporal_grain: 5-minute; 15-minute settlement interval
  history_start: "not verified"
  access: no login
  gridstatus_method: "Ercot().get_fuel_mix(date)"
  priority: P1
  used_by: [supply_context]
  caveats: "EMIL ID of Fuel Mix Report not verified; dashboard capacity differs from MORA (co-located flexible loads)"
  verified: not verified

- id: solar_wind_actual_forecast
  name: "Solar/Wind Power Production - Actual and Forecast by Geographical Region"
  source_url: "https://developer.ercot.com/applications/pubapi/relnotes/"
  api_endpoint: "np4-745-cd/spp_hrly_actual_fcast_geo (solar hourly); NP4-746-CD (solar 5-min); NP4-742-CD (wind hourly); NP4-743-CD (wind 5-min)"
  format: csv (zip)
  frequency: hourly; 5-minute
  spatial_grain: geographic region
  temporal_grain: hourly; 5-minute
  history_start: "API rows ~2023-12; archive ~7 years"
  access: Public API / no login
  gridstatus_method: "not verified"
  priority: P1
  used_by: [net_load_features]
  caveats: "region definitions differ from weather zones"
  verified: "IDs verified (release notes / api-specs discussion)"

- id: load_forecast_7day
  name: "Seven-Day Load Forecast by Weather Zone"
  source_url: "https://www.ercot.com/mp/data-products/data-product-details?id=np3-561-cd"
  api_endpoint: "NP3-561-CD (RTID not verified); forecast-zone NP3-560-CD RTID 12311"
  format: csv (zip)
  frequency: hourly
  spatial_grain: weather zone
  temporal_grain: hourly, 7-day horizon
  history_start: "API rows ~2023-12"
  access: "no login / Public API"
  gridstatus_method: "Ercot().get_load_forecast(date)"
  priority: P1
  used_by: [short_term_benchmark]
  caveats: "multiple vintages per day - keep publish time"
  verified: partially verified

- id: mora
  name: "Monthly Outlook for Resource Adequacy (MORA)"
  source_url: "https://www.ercot.com/gridinfo/resource"
  api_endpoint: none
  format: pdf + xlsx
  frequency: monthly (first Friday, 2 months ahead)
  spatial_grain: system
  temporal_grain: hourly for target month
  history_start: "December 2023 edition (published 2023-10)"
  access: no login
  gridstatus_method: none
  priority: P1
  used_by: [pricing_context]
  caveats: "revised versions possible; replaced SARA"
  verified: verified at source

- id: disclosure_60d
  name: "60-Day SCED and DAM Disclosure Reports"
  source_url: "https://www.ercot.com/mp/data-products"
  api_endpoint: "not verified (commonly NP3-965-ER / NP3-966-ER)"
  format: zip/csv
  frequency: daily, 60-day lag
  spatial_grain: resource
  temporal_grain: 5-minute / hourly
  history_start: "archive ~7 years (not verified)"
  access: Public API archive
  gridstatus_method: "60-day parsers in gridstatus (method names not verified)"
  priority: P2
  used_by: [optional_supply_behaviour]
  caveats: "large files; RTC+B added new AS offer columns and 2-/3-day disclosures (NP3-906-EX etc.)"
  verified: not verified

- id: outages
  name: "Unplanned Resource Outages Report / Hourly Resource Outage Capacity"
  source_url: "https://www.ercot.com/gridinfo/generation"
  api_endpoint: "RTID 22912 (unplanned); np3-233-cd/hourly_res_outage_cap"
  format: xlsx; csv
  frequency: daily; hourly
  spatial_grain: resource; zone
  temporal_grain: event; hourly
  history_start: "not verified"
  access: "no login / Public API"
  gridstatus_method: "not verified"
  priority: P2
  used_by: [optional]
  caveats: "IDs from gridstatus constants"
  verified: not verified

- id: demand_energy_report
  name: "Demand and Energy Report (peak records)"
  source_url: "not verified"
  api_endpoint: none
  format: xlsx (not verified)
  frequency: monthly (not verified)
  spatial_grain: system
  temporal_grain: monthly
  history_start: not verified
  access: no login
  gridstatus_method: none
  priority: P2
  used_by: [peak_records_ui]
  caveats: "not located in this pass"
  verified: not verified

- id: utility_territories
  name: "Electric Retail Service Territories (HIFLD / EIA Energy Atlas)"
  source_url: "https://atlas.eia.gov/maps/geoplatform::electric-retail-service-territories-2/about"
  api_endpoint: "ArcGIS FeatureServer (GeoJSON/JSON/PBF)"
  format: shapefile / GeoJSON
  frequency: irregular
  spatial_grain: utility service polygon
  temporal_grain: static
  history_start: "last HIFLD Open snapshot 2024-09-30"
  access: no login
  gridstatus_method: none
  priority: P1
  used_by: [coop_muni_targeting, geo_join]
  caveats: "HIFLD Open deactivated 2025-08-26 - use EIA Atlas or DataLumos archive (project 239091); PUCT maps and ERCOT co-op/muni list not verified"
  verified: partially verified
```