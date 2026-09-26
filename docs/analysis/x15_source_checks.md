# X15 — Source checks for the review queue (4CP, transmission rate, LTLF 2026, Uri, NERC holidays)

Source research only: no model, no code, no DB writes. Every URL below was fetched on **2026-09-26** (WebFetch or
`curl`). Files were read in a scratch directory and are not in the lake. Postgres was read only once, to line up
the official 4CP intervals with `ercot_monthly_peaks` (`metric = 'peak_15min_mw'`, region `ERCOT`).

| # | Question | Answer in one line |
|---|---|---|
| 1 | 4CP intervals, and whether the D&E time is ending or beginning | The official 4CP intervals match the D&E 15-minute peaks in **66 of 68 months (2008–2025)**, and in all 16 for 2022–2025, where the MW agree to ≤ 0.13 MW. ERCOT's own D&E workbook labels the time **"Interval ending"**. |
| 2 | Transmission rate | ERCOT postage stamp **$68.547301/kW-yr for 2025** (Docket 57491, final order). For 2026 it is **$75.527270/kW-yr** (Docket 59080, final matrices filed; the order was remanded and is still pending). Lag: year Y's 4CP bills year Y+1. |
| 3 | LTLF 2026 final | **None published as of 2026-09-26.** The PUCT approved an adjustment on a Batch Zero basis (June 2026), but no MW values have been filed. Only the April preliminary exists (~112 GW for summer 2026). |
| 4 | Uri dates and NERC holidays | ERCOT ordered rotating outages from **2021-02-15 01:20** to **2021-02-18 00:42**, and the grid was back to normal at **2021-02-19 10:35**. The NERC holiday rule matches the code exactly. The Saturday/Sunday off-peak rule is NAESB's and was **not verified** at NAESB. |

---

## Q1 — The official 4CP intervals vs. the D&E workbook

**Answer.** ERCOT posts the settlement 4CP calculation every year as MIS report NP9-83-M (report type 13037,
"ERCOT Four Coincident Peak Calculations"). Each posting is one workbook that puts the four intervals in the
column headers (`June 6/19/2025 17:00`, …) with each DSP's MW. The 1996–2020 files come as one zip.

Official intervals as printed by ERCOT (local time; the workbook does not say ending or beginning), with the
"Total" MW:

| Year | June | July | August | September | Average 4CP (MW) | Same as D&E? |
|---|---|---|---|---|---|---|
| 2022 | 06-23 17:00 · 76,803.5 | 07-20 16:45 · 80,233.2 | 08-02 17:00 · 78,669.1 | 09-20 17:00 · 71,149.6 | 76,713.9 | 4/4, MW ≤ 0.13 off |
| 2023 | 06-27 17:00 · 81,017.6 | 07-31 17:00 · 83,032.6 | 08-10 17:00 · 85,636.9 | 09-08 16:45 · 84,540.3 | 83,556.8 | 4/4 |
| 2024 | 06-30 17:45 · 79,749.8 | 07-01 17:00 · 81,184.4 | 08-20 17:00 · 85,357.6 | 09-20 16:00 · 77,878.8 | 81,042.7 | 4/4 |
| 2025 | 06-19 17:00 · 77,485.8 | 07-30 17:00 · 81,908.3 | 08-18 17:00 · 83,968.5 | 09-04 17:30 · 80,076.5 | 80,859.8 | 4/4 |
| 2026 | not posted (25.192(d) sets December 1 as the deadline; the latest MIS posting is `4CP20251124`) | | | | | — |

For 2008–2020 (archive zip; the "revised" or "amendment" file where one exists), **2 of 52 months differ**:

- **2010-07.** Official 07-16 16:30 · 60,514.8 MW; D&E 07-15 16:45 · 60,730.0 MW. A different day.
- **2016-09.** Official 09-19 16:15 · 67,096.6 MW; D&E 09-19 16:00 · 67,070.9 MW. Same day, 15 minutes apart.
- **2016-07.** The official file notes: *"Per the request of PUCT staff, ERCOT has replaced the protocol compliant
  July 14 peak interval of 16:00 with 16:30"*. The D&E also shows 16:30, so this month counts as a match.
- **2021** is in neither the MIS listing (which starts at the 2022 posting) nor the archive, so it is **not verified**.

Before 2022 the MW differ by tens of MW (mean +5 MW, D&E − official). From 2022 they are identical, so since 2022
the D&E 15-minute peak *is* the settlement 4CP interval and MW.

**Interval ending or beginning.** The 4CP settlement workbook never states the convention. ERCOT's Demand and Energy
workbook does. In `DemandandEnergy2025-for-Corp-Comms.xlsx`, tab "Demand", the block "Net System Maximum Demand
based on 15-Minute Intervals" has a row labelled **"Interval ending"**, reading `17:00 | 17:00 | 17:00 | 17:30` for
June–September 2025. The MW in the row above (77,485.834748 · 81,908.321364 · 83,968.463328 · 80,076.46512) are
the 4CP workbook's totals to within 0.00002 MW. So ERCOT publishes the same interval under the same timestamp and
calls it interval ending. The parser's convention holds: "17:00" is 16:45–17:00 CPT.

The one thing not done is an independent check against a raw 15-minute load series. ERCOT's label is the only
source.

| URL | What it is | Quote | Fetched |
|---|---|---|---|
| https://www.ercot.com/mktinfo/data_agg/4cp | ERCOT 4CP page | "The most recent ERCOT Four Coincident Peak (4CP) program calculations for each Distribution Service Provider, for the months of June, July, August and September can be found here" (links NP9-83-M and the 1996–2020 zip) | 2026-09-26 |
| https://www.ercot.com/mp/data-products/data-product-details?id=NP9-83-M | Data product page | Report Type ID 13037, "Annually", Rule Reference "NP9.17.1(1)" | 2026-09-26 |
| https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13037 | MIS document list | four postings: `4CP20251124`, `4CP20241125`, `4CP20231127`, `4CP20221128` | 2026-09-26 |
| https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=1164129560 (2025), `…=1055377059` (2024), `…=961277079` (2023), `…=879528147` (2022) | 4CP workbooks | header "2025 Four Coincident Peak Load Calculation (MW)", columns `June 6/19/2025 17:00` … `September 9/04/2025 17:30`; note "* Note: DC Tie exports, Block Load Transfer exports, and Wholesale Storage Loads are excluded from this report." | 2026-09-26 |
| https://www.ercot.com/files/docs/2022/01/13/1996-2020_FourCoincidentPeakCalculations.zip | 1996–2020 archive | 2016: "NOTE 2: Per the request of PUCT staff, ERCOT has replaced the protocol compliant July 14 peak interval of 16:00 with 16:30" | 2026-09-26 |
| https://www.ercot.com/files/docs/2025/02/07/DemandandEnergy2025-for-Corp-Comms.xlsx | D&E 2025 (current file) | "Net System Maximum Demand based on 15-Minute Intervals" … "Interval ending \| 07:45 \| 07:30 \| 17:30 \| 18:00 \| 16:45 \| 17:00 \| 17:00 \| 17:00 \| 17:30 …" | 2026-09-26 |
| https://www.ercot.com/services/comm/mkt_notices/M-B080124-01 | Market notice, 2024-08-01 | "Real-Time operational Load includes Wholesale Storage Load (WSL) while it is excluded from the Settlement 4-CP peak determination." | 2026-09-26 |

**Effect on the review item.**

- **Closes item 11's first half.** The interval label is *interval ending* (ERCOT's own label). The 4CP window does
  not move 15 minutes later.
- The X3 caveat "that these are the same intervals ERCOT uses to settle 4CP … is **not verified**" can go. They are
  the same in every month from 2022 to 2025 (MW equal) and in 50 of 52 months from 2008 to 2020.
- X3's back-test (2010–2025) scores two months against a D&E interval that is not the settlement one: 2010-07 (a
  different day) and 2016-09 (15 minutes earlier). With the 2 h 15:45–17:45 window both official intervals are still
  inside the window. Whether each official *day* was a dispatch day in X3's rules was not re-run: **not verified**.
- The "HE 18 vs 17:00" puzzle in X3 stays real. On most CP days since 2023 the settlement interval is 16:45–17:00
  while the hourly peak is HE 18 (17:00–18:00).
- A note for the product: since mid-2024 the settlement 4CP excludes Wholesale Storage Load. Real-time load
  dashboards include it, so live 4CP tracking can pick a different interval (ERCOT M-B080124-01).

---

## Q2 — The ERCOT wholesale transmission ("postage stamp") rate and the billing lag

**Answer.** Every year PUCT staff open a docket, "Commission Staff's Petition to Set <year> Wholesale Transmission
Service Charges for ERCOT". It adds every TSP's approved TCOS (its annual access fee) and divides by ERCOT's average
4CP load from the previous summer. The final matrix prints the sum as "Total ERCOT Postage Stamp Rate $/KW", an
**annual** $/kW rate.

| Charges for | Docket | TCOS total | Average 4CP used | Postage stamp | Status |
|---|---|---|---|---|---|
| 2024 | 56050 | $5,060,233,203 | 2023: 83,685,241.4 kW | **$66.756998/kW-yr** | final matrix filed 2024-03-13 |
| 2025 | 57491 | $5,446,864,794.70 | 2024: 81,042,656.556 kW | **$68.547301/kW-yr** | order approving the matrix, 2025-06-05 |
| 2026 | 59080 | $6,055,595,814.67 | 2025: 80,859,771.132 kW (matrix A) / 80,874,021.132 kW (matrix B) | **$75.527270/kW-yr** | final matrices filed 2026-03-16. The first order was remanded (2026-06-04, Entergy Texas intervention); a **proposed order on remand** was filed 2026-09-25. **Not final.** |

Matrix B adds ~14 MW to each month's 4CP. Its only visible change is the load; the reason is plausibly the City of
Caldwell filing (item 47), which was not read. The rate is the same in both matrices. The 2024 average 4CP in the
PUCT matrix (83,685 MW) is 128 MW above ERCOT's 2023 workbook total (83,557 MW). The reason was not checked.

**Billing lag (stated in the rule).**

- 25.192(d): ERCOT files the **current year's** average 4CP by December 1, and "This demand shall be used to bill
  transmission service for the next year."
- 25.192(b)(1): the annual rate is converted to a monthly rate, and each DSP pays the monthly rate × "the DSP's
  previous year's average of the 4CP demand".

So summer Y's 4CP sets calendar Y+1's charges. Until the new matrix is approved, DSPs keep paying on the old one on
an interim basis, with a later true-up (57491 order, ordering paragraph 3).

| URL | What it says | Quote | Fetched |
|---|---|---|---|
| https://ftp.puc.texas.gov/public/puct-info/agency/rulesnlaws/subrules/electric/25.192/25.192.pdf | PUCT Subst. R. 25.192 | "The TSP's annual rate shall be converted to a monthly rate. The monthly transmission service charge to be paid by each DSP is the product of each TSP's monthly rate as specified in its tariff and the DSP's previous year's average of the 4CP demand that is coincident with the ERCOT 4CP." · "No later than December 1 of each year, ERCOT shall determine and file with the commission the current year's average 4CP demand for each DSP … This demand shall be used to bill transmission service for the next year." · "…for the four intervals coincident with ERCOT system peak for the months of June, July, August, and September, divided by four." | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/56050_54_1374934.PDF | Docket 56050, final matrix 2024 (the attachment's header still reads "Draft Matrix") | "TOTAL $5,060,233,203 … $66.756998 … 83,685,241.4" · "Total ERCOT Postage Stamp Rate $/KW $66.756998" · column "From ERCOT Filing 2023 Average 4CP (KW)" | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/57491_51_1481445.ZIP (`Final Staff Matrix GH DL.xlsx`) | Docket 57491, final matrix 2025 (2025-03-20) | "Total ERCOT Postage Stamp Rate $/KW \| 68.547300544"; TOTAL 5,446,864,794.70 · 81,042,656.556 kW | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/57491_53_1486508.PDF | Docket 57491, joint proposed order (2025-04-04) | "The wholesale transmission charges for each TSP in ERCOT are required to be updated annually based on the prior year's average of the four-coincident-peak (4CP) demand …" · "In the event that the Commission does not establish the transmission charges for 2026 before January 1, 2026, DSPs must continue paying the monthly billing amounts based on the 2025 matrix …" | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/57491_58_1505349.PDF | Docket 57491, signed order (filed 2025-06-05) | "Based on ERCOT's report, the average 4CP load is 81,042.6566 [MW]"; the attached matrix reads "Total ERCOT Postage Stamp Rate $/KW" 68.5473(01) (scanned text) | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/59080_50_1603607.ZIP (Attachments C and D xlsx) | Docket 59080, final matrices A and B (2026-03-16) | "Total ERCOT Postage Stamp Rate $/KW \| 75.52726963"; TOTAL 6,055,595,814.67; 4CP headers `6/19/2025 17:00 … 9/04/2025 17:30` | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/59080_86_1687248.PDF | Docket 59080, proposed order on remand (2026-09-25) | "The Commission approves the final transmission-charge matrix included as attachment A … (matrix A), effective for billing beginning January 1, 2026 through March 11, 2026 … matrix B, to replace matrix A and be effective for billing beginning March 12, 2026." · "the average 4CP load is 80,859.7711 megawatts (MW)." | 2026-09-26 |
| https://interchange.puc.texas.gov/search/filings/?UtilityType=A&ControlNumber=59080&ItemMatch=Equal&DocumentType=ALL&SortOrder=Descending | Docket 59080 filings list | item 62 (2026-06-04) "ORDER GRANTING INTERVENTION AND REMANDING PROCEEDING"; item 86 (2026-09-25) "PROPOSED ORDER ON REMAND WITH MEMORANDUM"; no final order after it | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/59080_5_1565104.PDF | ERCOT's 2025 4CP report to the PUCT | "The 2025 4CP dates were June 19, July 30, August 18, and September 4." | 2026-09-26 |

**Effect on the review item.**

- **Closes item 11's second half** (and X3's "No transmission rate ($/kW-yr) is in the repo"). The pitch can say
  ERCOT's 2026 postage stamp rate is about **$75.5 per kW-year**, about **$6.29/kW-month**. It is set on the 2025
  4CP; the matrices are filed and the final order is pending. The 2025 rate is $68.55, and the rate is up 13% in two
  years. For scale only: 1 MW off the average of the four 2026 intervals is worth about **$75.5k in 2027**, if the
  2027 rate equals the 2026 one (not verified).
- Caveats to keep in the pitch:
  - This is the ERCOT-wide sum of TSP access fees, as it reaches a DSP. What a co-op passes to its members, and
    whether Base captures any of it, is **not verified**.
  - The 2027 rate is not set.
  - PUCT Projects 58484/58000 (SB 6 follow-up on transmission cost allocation, which may change 4CP) showed up in
    search results only. They were **not read** here.
- Add the three rates, their dockets and the lag to `config/manual_official_figures.yaml` in a later task (not done
  here, since only this file was to be created).

---

## Q3 — Is there a final 2026 Long-Term Load Forecast?

**Answer: no, as of 2026-09-26.**

- **April 15, 2026.** ERCOT filed the *preliminary* 2026–2032 LTLF (Project 58777, item 38). Its "Forecast + Large
  Loads + Medium Loads" bars are 112,371 (2026), 152,882 (2027), 221,379 (2028), 278,003 (2029), 319,650 (2030),
  346,715 (2031) and 367,790 MW (2032). These are chart labels from the PDF text layer. The order by year is
  inferred from the rising values; the letter itself confirms 2029 and 2032. The same letter gives ERCOT's own
  summer 2026 range of **90,500–98,000 MW** against the preliminary's ~112,000 MW.
- **June 2026.** In Project 59772 the PUCT approved ERCOT's request to adjust the LTLF with **Option B (Batch Zero
  base load, PGRR 145)**. It ordered ERCOT to file the MW values "once those MW values are known". The 59772 filings
  list ends at that order (item 18, 2026-06-18); no MW values have been filed.
- **July 2, 2026.** A PUCT staff memo set "Batch Zero Base load forecast ready for model input: **August 21,
  2026**".
- **August 3, 2026.** ERCOT paused the Batch Zero process after the Governor's directive (market notice M-A080326-01).
- **September 14–15, 2026.** ERCOT's board update says classifications are *conditional* (base load 66.4 GW, 204
  projects; studied load 127.9 GW, 158 projects). "Final project inclusion / exclusion determination will be based
  on the results of the eligibility verification and audit and provided in December."
- **ERCOT's Load Forecast page** still lists only the 2025 LTLF reports.

| URL | What it says | Quote | Fetched |
|---|---|---|---|
| https://interchange.puc.texas.gov/Documents/58777_38_1622647.PDF | ERCOT preliminary LTLF 2026–2032 (2026-04-15) | "the forecast projects approximately 278,003 megawatts (MW) of total demand in the ERCOT Region by 2029 and 367,790 MW by 2032. For comparison, the ERCOT Region's current all-time peak demand is 85,508 MW." (footnote: "This peak demand record occurred on August 10, 2023.") · "ERCOT currently projects that summer 2026 peak load will fall within a range of approximately 90,500 MW to 98,000 MW, compared with the preliminary LTLF's forecasted peak demand of approximately 112,000 MW for summer 2026" | 2026-09-26 |
| https://www.ercot.com/news/release/04152026-ercot-releases-preliminary | ERCOT news release (2026-04-15) | calls the filing "a preliminary snapshot"; "approximately 367,790 MW of demand in the ERCOT Region by 2032" | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/59772_18_1658216.PDF | PUCT order granting the adjustment, Project 59772 | "the Commission approves ERCOT's request to adjust the LTLF using the Option B methodology." · "The Commission orders ERCOT to provide notice in this Project to the Commission of the load forecast MW values based on the above adjustments, once those MW values are known." | 2026-09-26 |
| https://interchange.puc.texas.gov/search/filings/?UtilityType=A&ControlNumber=59772&ItemMatch=Equal&DocumentType=ALL&SortOrder=Descending | Project 59772 filings list | last item: 18, 2026-06-18, "ORDER GRANTING ADJUSTMENT REQUEST" | 2026-09-26 |
| https://interchange.puc.texas.gov/Documents/58777_41_1662454.PDF | PUCT staff memo, 2026-07-02 | "Batch Zero Base load August 21, 2026 forecast ready for model input" · "ERCOT will use the Batch Zero LTLF for 2026 and 2029 as approved by the PUCT at their June 18, open meeting" | 2026-09-26 |
| https://www.ercot.com/services/comm/mkt_notices/M-A080326-01 | ERCOT market notice, 2026-08-03 | "ERCOT will not notify each Interconnecting Distribution Service Provider and Transmission Service Provider of how any Large Load is classified in the Batch Zero Interconnection Study by August 7, 2026." | 2026-09-26 |
| https://www.ercot.com/files/docs/2026/09/11/14-Batch-Zero-Update.pdf | ERCOT board item 14, 2026-09-14/15 | "The verification and audit process will have a TBD impact on the Batch Zero process and timeline." · "204 projects (66.4 GW) met the qualifications to be conditionally included as base load" · "Final project inclusion / exclusion determination will be based on the results of the eligibility verification and audit and provided in December" | 2026-09-26 |
| https://www.ercot.com/gridinfo/load/forecast | ERCOT Load Forecast page | LTLF section lists "2025 Long-Term Load Forecast Reports" (files dated 2025-04-08); no 2026 LTLF file | 2026-09-26 |

**Effect on the review item.**

- **Closes item 13's last sentence and X7 open item 7.** There is no final LTLF 2026 to add, so X7 keeps LTLF 2025 +
  CDR Dec 2025 as the official lines. The preliminary's 2026 value is ~112 GW, against ERCOT's own 90.5–98 GW range;
  the 2026 actual in the repo is 91.1 GW, preliminary.
- X7's footnote on the 278.0 GW figure: the filing says "total demand … by 2029" and the chart is "LTLF Peak
  Demand", so it is the 2029 peak; its season is not stated.
- The Batch Zero-adjusted forecast is a moving target (conditional classifications, final in December). If the video
  mentions it, say "adjusted forecast pending".
- **Corroborates item 6.** The record before 2026 was 85,508 MW on 2023-08-10, per ERCOT's own filing.

---

## Q4 — Winter Storm Uri dates and the NERC holiday rule

**Answer (Uri).** ERCOT's own review, presented to the board on 2021-02-24, gives:

- EEA 1 at **00:15** on Mon 2021-02-15, EEA 2 at 01:07, and EEA 3 with rotating outages ("10,800 MW Load Dropped")
  at **01:20**.
- "12:42 a.m. — Canceled last controlled outage orders" on **Thu 2021-02-18**. Some outages remained after that
  because of ice-storm damage and industrial restarts.
- Back to normal operations at **10:35** on Fri 2021-02-19.
- A load-shed request lasting **70.5 hours**, peaking at **20,000 MW**.

So firm load was shed on local days **Feb 15–18**, and the emergency ran Feb 15 00:15 → Feb 19 10:35. X12's exclusion
of **2021-02-14 → 02-20** covers all of it, plus Feb 14 (conservation appeals) and Feb 20 (restoration). The window
is supported by the source; its edges are a modeling choice.

**Answer (NERC holidays).**

- A NERC document ("Additional Off-peak Days (aka 'Holidays')", July 2014) lists six holidays: New Year's Day,
  Memorial Day, Independence Day, Labor Day, Thanksgiving and Christmas. A fixed-date holiday on a Sunday is
  observed the Monday after; one on a Saturday stays on Saturday.
- It is not on nerc.com. The copy fetched is hosted on OATI's OASIS (NV Energy). PJM's 2026 calendar corroborates it
  under "NERC 2026 Off-Peak Days": July 4 (Saturday) is kept on Saturday.
- `basecast_pipelines/models/weather_normalized.py::nerc_holidays()` implements exactly this rule.
- **Saturday/Sunday as off-peak** is *not* a NERC rule. The same document says the on/off-peak hour definitions
  "were moved over to the North American Energy Standards Board (NAESB)". NAESB's text was not fetched, so that part
  is **not verified**. It does not matter to X12, which models day of week as its own factor.

| URL | What it says | Quote | Fetched |
|---|---|---|---|
| https://www.ercot.com/files/docs/2021/02/24/2.2_REVISED_ERCOT_Presentation.pdf | ERCOT, "Review of February 2021 Extreme Cold Weather Event", urgent board meeting 2021-02-24 | "1:20 am EMERGENCY OPERATIONS LEVEL 3: Rotating Outages: 10,800 MW Load Dropped" · "Thursday, February 18 … 12:42 a.m. - Canceled last controlled outage orders - some outages remained due to ice storm damage; need for manual restoration and return of large industrial facilities." · "Friday, February 19 … 10:35 a.m. – Returned to normal operations" · "Duration load shed request (hours) … 70.5"; "Maximum load shed requested (MW) … 20,000" | 2026-09-26 |
| https://www.oasis.oati.com/NEVP/NEVPdocs/2014-2025_additional_off-peak_days.pdf | NERC "Additional Off-peak Days (aka 'Holidays')", July 2014, hosted by OATI/NV Energy (not nerc.com) | "New Year's Day, Independence Day, and Christmas Day, by definition, are predetermined dates each year. However, in the event that they fall on a Sunday, The 'NERC Additional Off-Peak Holiday' is celebrated the Monday immediately following that Sunday. If these days fall on a Saturday, the 'NERC Additional Off-Peak Holiday' remains on that Saturday." · "The on-peak/off-peak hours used to be defined in NERC's Policy 1, Appendix 1F … they were moved over to the North American Energy Standards Board (NAESB)." | 2026-09-26 |
| https://www.pjm.com/-/media/DotCom/markets-ops/settlements/2026-business-calendar.pdf | PJM 2026 calendar | "The following holidays are defined by NERC as Off-Peak days for the Eastern Interconnection during 2026: January 1 … May 25 … July 4 Independence Day Saturday … September 7 … November 26 … December 25" | 2026-09-26 |

**Effect on the review item.**

- **Closes item 18's "came from memory (not verified)".** The Uri window now has an ERCOT source.
  `weather_normalized.py` line 49 and the X12 decision line can cite ERCOT's 2021-02-24 review instead of "not
  verified" (a later edit, not made here).
- The holiday rule is verified against a NERC document and PJM's calendar. The code needs no change.
- Only the weekend-as-off-peak wording stays unverified, and X12 does not depend on it.

---

## Not done / still open

- 2021 official 4CP intervals: not in ERCOT's current MIS listing or the 1996–2020 archive.
- The 2026 official 4CP: ERCOT posts it by 2026-12-01.
- The 2026 transmission charges: the final PUCT order in Docket 59080 is still pending.
- An independent check of the interval convention against a raw 15-minute load series.
- NAESB's weekend off-peak definition.
- PUCT Projects 58484/58000 (possible change to 4CP allocation under SB 6).
