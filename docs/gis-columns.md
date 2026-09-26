# GIS report: sheets, header rows and milestone columns by vintage

Answers the open question of KICKOFF §8 / task A2: *the real names of the GIS milestone columns and how
they vary between snapshots*. Built on 2026-09-26 from every monthly GIS report in the lake (EMIL
PG7-200-ER, Report Type 15933): 148 report months, May 2014 → Aug 2026, one workbook per month (the
newest correction wins, see §6). The mapping to stable names lives in `HEADER_MAP` in
`basecast_pipelines/parsers/ercot/gis.py`; every header seen in these 148 files is in it, so a full
parse logs no "unmapped header" warning.

Conventions: **Excel row** is the 1-based row of the header cell `INR` / `GINR Reference Number`;
**wraps** is how many rows the header spans (the parser joins them into one label, e.g. `Approval Date
for` + `Submission of Proof of` + `Site Control`). Month ranges are inclusive (`2019-08..2022-01`).

## 1. Two layouts

| Vintage | Workbook | Project tables (one row per INR) | Monthly event tables |
|---|---|---|---|
| 2014-05..2018-07 ("old") | xls until 2016-02 (2014-10..2015-02 zipped xlsx), then xlsx | `IA Table` (signed IA), `Full Study Table` (in full interconnection study, no IA), `Wind Chart` / `Solar Chart` (chart data, wind since 2014-05, solar since 2015-05), `QSA Units` (2018-05..07) | `New Units` (2014-05..2017-07; `New and Cancelled Units` in 2017-04), `Newly Operational & Cancelled` (2017-08..2018-07): sub-tables of new IAs (2014-05..06), synchronization approvals, commercial operation approvals and, since 2017-04, cancellations |
| 2018-08..today ("GINR"/"GIM") | xlsx | `Project Details` (2018-08..2022-01) → `Project Details - Large Gen` (2022-02..); `Project Details - Small Gen` (2022-01..) | `Project Commissioning Update` / `Project Cancellation Update` (2018-08/09..2019-07) → `Commissioning Update` / `Cancellation Update` (2019-08..); `Inactive Projects` (2019-08.., cumulative "as of end of month") |

Other sheets (kept only in `gis_cells`): `PROJECTS`/`Projects` (cover), `Summary of GIR`, `ProjectMatrix`,
zone sheets (`NORTH`…`HOUSTON`, 2016-04 only) in the old layout; `Contents`, `Disclaimer and References`,
`Acronyms`, `Summary`, `GINR Trends` → `GIM Trends` (2022-02) with `data_*` sheets, and `CORRECTIONS`
(in corrected files: 2019-05, 2020-06, 2020-07) in the new one.

## 2. Where the header is

| Sheet | Vintage | Excel row | Wraps |
|---|---|---|---|
| IA Table | 2014-05..2017-07 | 6 | 1 |
| IA Table | 2017-08..2018-07 | 5 | 1 |
| Full Study Table | 2014-05..2014-12 | 2–3 | 1 |
| Full Study Table | 2015-01..2018-07 | 3 | 1 |
| Wind / Solar Chart | 2014-05..2018-02 | 2–6 (5 leading empty rows in the xls files) | 1 |
| Wind / Solar Chart | 2018-03..2018-07 | 45–46 | 1 |
| QSA Units | 2018-05..2018-07 | 4 | 1 |
| New Units and successors | 2014-05..2018-07 | several tables per sheet: row 4, then 10–35 | 1 |
| Project Details | 2018-08..2018-12 | 22–25 | 4 |
| Project Details | 2019-01 | 23 | 1 (text in one cell) |
| Project Details | 2019-02..2020-06 | 23–25 | 5 |
| Project Details | 2020-07..2022-01 | 28–31 | 5 |
| Project Details - Large Gen | 2022-02..2026-08 | 31 | 5 |
| Project Details - Small Gen | 2022-01 / 2022-02..2026-08 | 16 / 15 | 4 / 3 |
| Commissioning, Cancellation, Inactive updates | 2018-08..2026-08 | 8 | 1 |

Above the header of the project tables there are notes and a group row (`Project Attributes` |
`Changes from Last Report` | `GINR/GIM Project Milestone Dates`); the group row is not part of the label.

## 3. Milestone columns (the answer)

Stable column in `gis_snapshots` ← header text as printed, per layout. "—" means the layout has no
such column.

| Stable column | Old layout 2014-05..2018-07 | New layout 2018-08..2026-08 |
|---|---|---|
| `screening_study_started` | — (screening projects are confidential and not listed) | `Screening Study Started` |
| `screening_study_complete` | — | `Screening Study Complete` |
| `fis_requested` | — (being in `Full Study Table` means FIS underway) | `FIS Requested` (missing in 2020-09) |
| `fis_approved` | — | `FIS Approved` |
| `fis_status` (Complete / Incomplete) | IA Table `FIS completion` (2015-04..2018-07; `FIS Completion` 2017-07); Full Study `Status` (2017-02..08), `FIS Report Status` (2017-09..2018-07), `FIS Status` (2018-01) | — |
| `economic_study_required` (Yes/No) | — | `Economic Study Required` (2020-06..) |
| `site_control_approved` | — | `Approval Date for Submission of Proof of Site Control` (2020-07..) |
| `ia_signed` | Wind/Solar Chart `IA Signed`; New Units `Signed IA Date` (2014-05..06); being in `IA Table` = IA executed | `IA Signed` (Large and Small Gen) |
| `financial_security_provided` (Yes/No) | IA Table `Sufficient Financial Security Received by TSP` (`…/Notice to Proceed Given` in 2016-03); charts `FS Posted` (true/false, 2017-03..) | `Financial Security and Notice to Proceed Provided` |
| `financial_security_required_dist` (Yes/No) | — | Small Gen `Financial Security Required to fund Dist. Upgrades` |
| `air_permit` / `air_permit_date` | IA Table `Air Permit` (2014-12..; `Air Permit(s)` 2017-08): Yes / No / N/A | `Air Permit`: a date, `Not Required` or blank (blank = required, not yet obtained) |
| `ghg_permit` / `ghg_permit_date` | — | `GHG Permit` (same convention) |
| `water_availability` / `water_availability_date` | IA Table `Water Rights` (2014-12..; `Proof of Adequate Water Supplies` 2017-08): Yes / No / N/A | `Water Availability` (same convention) |
| `meets_planning_6_9_met` (Yes/No) | IA Table `Meets Requirements of PG Section 6.9` (2014-05..11), `Meets All Requirements` (2014-12..2016-02) | — |
| `meets_planning_6_9_1bd_met` (Yes/No) | IA Table `Meets Section 6.9 Requirements (1)(b) through (1)(d)` (2016-03..2018-07) | — |
| `meets_planning_6_9_1` | — | `Meets Planning Guide Section 6.9(1) Requirements for Inclusion in Planning Models` |
| `meets_all_planning_6_9` | — | `Meets Planning Guide Section 6.9 Requirements for Inclusion in Planning Models` (2018-08..12), `Meets Planning Guide QSA (Section 6.9) Prerequisites` (2018-09..10, mislabelled: same position, **not verified**), `Meets All Planning Guide Section 6.9 Requirements for Inclusion in Planning Models` (2019-01..) |
| `meets_qsa_prerequisites` | being in `QSA Units` (2018-05..07) | `Meets Planning Guide QSA (Section 5.9) Prerequisites` |
| `construction_start` / `construction_end` | — | `Construction Start` / `Construction End` ("Not available this month" in 2018-08) |
| `approved_energization` | — | `Approved for Energization`; Commissioning Update rows `Energization Approved by ERCOT` + `Approval Date *` |
| `approved_synchronization` | `Part2 Synch Apprv` (`Part 2 Synch Approval` 2018-07) in "New Resources Approved / Available for Synchronization" | `Approved for Synchronization`; Commissioning Update `Synchronization Approved by ERCOT` |
| `commercial_operation_date` | `Part3 Commercial Apprv` (2014-05..2016-04), `Resource Commissioning Date *` (2016-01..2018-07) in "New Resources Approved for Commercial Operation(s)" | Commissioning Update `Commercial Operation Approved by ERCOT*` (`Commercial Operations Approved …` twice) |
| `cancel_date` | `Cancellation Date` / `Cancellation Date*` in "Projects Cancelled by the Developer*" (2017-04..2018-07) | `Cancel Date` (Project Cancellation Update, then Cancellation Update) |
| `inactive_date` | — | `Inactive Date` (Inactive Projects, 2019-08..) |
| `model_ready_date` | — | Small Gen `Model Ready Date` (2022-01..) |
| `projected_cod` | `Projected COD` (IA 2014-05..12), `Projected Date` (IA 2015-01..; charts 2015-01..), `COD Projection` (Wind Chart 2014), Full Study `Projected COD / Projected Date / Projected Month/Year / Projected COD Month/Year (as specified by the resource developer)`; `m/YYYY` text since 2017 | `Projected COD` |

Other columns: `study_phase` ← `GINR Study Phase` (2018-08..2022-01) / `GIM Study Phase` (2022-02..),
values such as `SS Completed, FIS Started, No IA` (9 combinations of SS / FIS / IA); `change_indicators`
← `Changes From Last Report` (IA Table) and five variants of `Change indicators: Proj Name, MW Size, COD,
SFS/NtP, FIS Request[, Status Change INA-to-PLN | SUS-to-PLN]`; `capacity_mw` ← `MW For Grid`,
`Capacity to Grid (MW)`, `MWForGrid`, `MW`, `MW **`, `Capacity (MW)`; `commissioning_categories` ←
`Commissioning Category` (`Energization Approved by ERCOT`, `Synchronization Approved by ERCOT`,
`Commercial Operation Approved by ERCOT*`, `Synchronization Approval Revoked by ERCOT`).

## 4. Values

- **Missing dates:** `1-1-1900` ("an actual date … is not available", report note; data migrated from
  the legacy GINR database), `Date Not Available` (2018-11..), `Not available this month` (2018-08),
  `Not Available` → null. Nothing else in a date column fails to parse (checked on all 148 files).
- **Clock times:** cancel, inactive and some milestone dates carry times (`2026-08-04 09:57:59.999`,
  Excel float noise); they are rounded to the second, then truncated to the date.
- **Fuel** is printed as codes in the project tables (`WIN`, `SOL`, `GAS`, `OTH` + technology `BA` for
  batteries, `NUC`, `COA`, `HYD`, `BIO`, `OIL`), as words in the update tables and the old layout
  (`Wind`, `WIND`, `Battery Storage`, `Storage`, `Gas/CE`, `Fuel Oil`, …). `fuel_type` (derived) folds
  them into storage / wind / solar / gas / coal / nuclear / hydro / biomass / oil / other.
- **INR:** `NNINRNNNN` plus suffixes (`14INR0012a`, `14INR0030a_2`, `15INR0070_1b`, `…B`, `…_ES`); one
  printed with footnote stars (`22INR0373***`, 2020-08) is cleaned. Confidential projects in the old
  `Full Study Table` have only INR, county, fuel and MW.
- **Personal data:** no sheet in any vintage has contact names, emails or phones; `Interconnecting
  Entity` is a company.

## 5. Coverage and continuity (full parse, 2026-09-26)

- `gis_snapshots`: 125,470 rows, 3,764 INRs; 190 INRs in May 2014 → 2,001 in Aug 2026 (1,810 active,
  438 GW of active capacity). No INR is duplicated within a month.
- Exits are only partly reported before 2019: cancellations since 2017-04, inactive projects since
  2019-08. Month-to-month continuity (an active INR missing from the next report without a recorded
  cancellation, inactivity or approval): 116 cases in 2014–2016, 14 in 2017–2018, 7 in 2019, none in
  2020–2023, 29 in 2024–2026 (mostly projects that come back after a few months).
- `gis_project_events` exit status: active 1,810, cancelled 1,029, operational 602 (1 inferred from a
  synchronization approval), inactive 187, dropped 136 (disappeared without a recorded reason, mostly
  2014–2016 cohorts). The 190 INRs of the first report (May 2014) are left-truncated: they entered
  the queue earlier.
- Small generator detail exists only since 2022-01; the old layout lists only projects in the full
  study or with an IA.

## 6. Files per month

Eight months have more than one workbook; the newest wins (listing publish date, else a
`Revised`/`corrected` name): 2016-08 (`…_corrected_wind_chart`), 2017-03 (`…_Revised`), 2020-04
(`…_revised`, published 2020-07-01), 2020-06 (`…_Correction`), 2020-07 (`…_CORRECTION2` over
`…_CORRECTION` and the original), 2022-04 (`…_Corrected`), 2023-06 (`…_Corrected`). The superseded
workbooks parse too. Corrections are complete reports; their `CORRECTIONS` sheet lists what changed
(e.g. a 1,140 → 80 MW fix, a technology code fix).

## 7. Co-located battery identification report (2020-10 → 2026-08, 71 files)

Sheets `Co-located with Solar` / `with Wind` / `with Thermal` / `Stand-Alone` list the batteries **and**
the generation projects they are co-located with, one row per INR (header row 14 until 2021-08, then 15):
`INR, Project Name, Project Status, Interconnecting Entity, POI Location, County, CDR Reporting Zone,
Projected COD, Fuel, Technology, Capacity (MW)`, plus since 2021-03 `IA Signed`, `Financial Security and
Notice to Proceed Provided` (wrapped over 3 rows), `Approved for Energization`, `Approved for
Synchronization`, and `Comment` (2021-11.., by sheet). Operational units are in `Co-located Commercial
Approved` (2021-08..2022-06) → `Co-located Operational` (2022-07..), keyed by `Unit Name` / `Unit Code`
with `In Service` (a year) and `Capacity (MW)*` (unit rating). `is_battery` is derived from technology
`BA`/`EN` or fuel `BAT`. One unit code appears twice in 2026-07/08 (`HRMS_SLR_UNIT1`), as in the source.
