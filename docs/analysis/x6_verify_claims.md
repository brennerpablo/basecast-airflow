# X6 — Independent verification of the numbers for the video

Run on 2026-09-26. Script: `analysis/x6_verify_claims.py` (`uv run --group analysis python analysis/x6_verify_claims.py`,
~5 s). Output table: `analysis/out/x6_verify_claims.csv`; rendered chart pages for claim 5: `analysis/out/x6_*.png`
(gitignored).

**Method.** Each number is re-derived from the raw files in the local lake (`data/raw/source=<id>/dt=<date>/`), with its
own readers: Excel cells found by their labels, PDF text through pdfplumber, DOCX/PPTX XML read straight from the zip.
Nothing imports `basecast_pipelines.models` or the parsers. Postgres (`read_sql`, read-only) is used only to show what
the original analysis read. Claim 5's chart values are raster images (PNG inside the PDF/PPTX, no chart XML), so the
script renders the pages and Claude read the bar labels by eye. That read is independent of Gemini, but it is still a
visual read, not OCR.

## Results

| # | Claim | Original value | Recomputed value | Source (file, sheet/page, URL from the manifest) | Verdict |
|---|---|---|---|---|---|
| 1a | 2026 summer peak, hourly | 91,134 MW, HE 18 CDT, 2026-07-22 | **91,133.7 MW, 07/22/2026, hour ending 18:00**. `Native_Load_2026.xlsx` gives the same value in the same hour, and it is the summer max there too | `DemandandEnergy2026-for-Corp-Comms.xlsx`, sheet `Demand`, row "2026 Demand, MW", column Jul ([URL](https://www.ercot.com/files/docs/2026/02/09/DemandandEnergy2026-for-Corp-Comms.xlsx)); `Native_Load_2026.xlsx` ([URL](https://www.ercot.com/files/docs/2026/02/10/Native_Load_2026.zip)) | **confirmed** (preliminary: Jul and Aug carry no final-settlement `*`) |
| 1b | 2026 peak, 15-min | 91,263 MW | **91,262.7 MW**, 07/22/2026, interval ending 17:30 | same workbook, section "Net System Maximum Demand based on 15-Minute Intervals" | **confirmed** |
| 1c | Previous record | 85,508 MW on 2023-08-10 | **85,508.5 MW, 08/10/2023 HE 18:00**, the highest annual max in the D&E workbooks for 2017–2025. The 2025 workbook's "Max All Time, MW" row agrees ("08/10/2023 at 18:00"), and ERCOT's 2026-04-15 letter says "current all-time peak demand is 85,508 MW" | `DemandandEnergy2023.xlsx`, sheet `Demand` ([URL](https://www.ercot.com/files/docs/2023/02/07/DemandandEnergy2023.xlsx)); `DemandandEnergy2025-for-Corp-Comms.xlsx` ([URL](https://www.ercot.com/files/docs/2025/02/07/DemandandEnergy2025-for-Corp-Comms.xlsx)); PUCT 58777 item 38 letter | **confirmed** |
| 2a | Preliminary 2026 LTLF, summer 2026 | ~112 GW; missed by 20.9 GW (+22.9%) | Quote: "the preliminary LTLF's forecasted peak demand of approximately **112,000 MW** for summer 2026". 112,000 − 91,134 = **20,866 MW (+22.9%)**. Slide 7: "The TSP RFI included ~15GW of incremental medium and Large Loads for 2026, which would increase the peak to 112 GW." | PUCT Project 58777 item 38, cover letter `2026-04-15 (58777) Preliminary Long-Term Load Forecast.docx`, dated April 15, 2026, last paragraph; `Attachment A.pptx` slide 7 ([URL](https://interchange.puc.texas.gov/Documents/58777_38_1622646.ZIP)) | **confirmed** (framing caveat below) |
| 2b | ERCOT's own range, same filing | 90.5–98 GW | Quote: "ERCOT currently projects that summer 2026 peak load will fall within a range of approximately **90,500 MW to 98,000 MW**". Slide 7 bar labels: 90,486 MW and 98,086 MW. The actual is +634 MW over the low end | same letter and slide | **confirmed** |
| 3 | "In Jan 2026 ERCOT was tracking ~232.5 GW of large loads, only 3.8% of them approved to energize" | 232.5 GW; 3.8% | Quote (p6): "As of January 21, 2026 … proposed large load projects tracked by ERCOT total approximately **232,500 megawatts**" and "To date, **8,786 MW** of large load demand has received approval to energize". 8,786 / 232,500 = **3.78%**. The Jan 21 TAC deck (p6) also says 8,786 MW. `large_load_headlines` has the same two values | `ERCOT-Monthly-January-2026-FINAL.pdf` p6 ([URL](https://www.ercot.com/files/docs/2026/02/13/ERCOT-Monthly-January-2026-FINAL.pdf)); `January TAC Report.pdf` p6 in `16.-Large-Load-Issues.zip` ([URL](https://www.ercot.com/files/docs/2026/01/20/16.-Large-Load-Issues.zip)) | **confirmed** (the 3.8% is approval, not consumption) |
| 4 | Generation queue, Aug 2026 GIS | 1,810 active projects, 438,262 MW | **1,810 projects, 438,261.62 MW**: 1,778 rows in `Project Details - Large Gen` + 32 in `Project Details - Small Gen`, no duplicate INR, none also listed in `Inactive Projects` (168, 36,669 MW) or `Cancellation Update` (19, 4,581 MW). ERCOT's own `Summary` sheet: "Currently tracking **1890** generation interconnection or change requests as of Aug 31, 2026", Total Capacity Under Study **454,471.12 MW** | `RPT.00015933…GIS_Report_August2026.xlsx` ([URL](https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=1269363208)) | **confirmed** under the definition below |
| 5a | Q5 approved-to-energize stock, Dec 2024 | 6,297 MW | **6,297 MW**: the 2024-12 bar of "ERCOT Approvals – Past 12 Months" in the Jan 22, 2025 deck (p5), read by eye. The text brackets it: Nov 20, 2024 deck p5 "Of the total 6,297 MW Approved to Energize"; Jan 22, 2025 deck p6 "6,306 MW" | `LLI Queue Status Update - 2025-1.pdf` p5 in `18-ercot-reports.zip` ([URL](https://www.ercot.com/files/docs/2025/01/22/18-ercot-reports.zip)); `LLI Queue Status Update - 2024-11-20.pdf` p5 ([URL](https://www.ercot.com/files/docs/2024/11/20/12-ercot-reports.zip)) | **confirmed** |
| 5b | Q5 approved-to-energize stock, Dec 2025 | 8,786 MW | **8,786 MW**: the 2025-12 bar of the Jan 21, 2026 deck (p4), read by eye. The Nov 2025 bar is 7,712 MW, so December added 1,074 MW. Text: Nov 19, 2025 deck p6 "Of the 7502 MW"; Jan 21, 2026 deck p6 "Of the 8786 MW" | `January TAC Report.pdf` p4 in `16.-Large-Load-Issues.zip` (URL above); `November TAC Report.pdf` p6 ([URL](https://www.ercot.com/files/docs/2025/11/18/11-LLWG-Report.zip)) | **confirmed** |
| 5c | MW promised in service by end-2024 / end-2025, Oct 2024 vintage | 16,803 / 26,836 MW | **16,803 / 26,836 MW**, bar-total labels of "Actual and Projected Large Load Growth 2022-2028" (slide 3, PNG image4), read by eye. Gross ratio 6,297/16,803 = 0.37 (2-month horizon, outside Q5's band); 8,786/26,836 = **0.33** | `LLI Queue Status Update - 2024-10-30.pptx` slide 3 in `15-ercot-reports.zip` ([URL](https://www.ercot.com/files/docs/2024/10/29/15-ercot-reports.zip)) | **confirmed** |
| 5d | Same, May 2023 vintage (earliest) | 20,715 / 21,986 MW | **20,715 / 21,986 MW**, from the bar labels and the slide's own table. Segments match Q5 too (Approved to Energize 2,620; Planning Studies Approved 5,853 / 5,999; Under ERCOT Review 10,527 / 11,252; No Studies Submitted 1,715 / 2,115). Gross ratio 0.30 / 0.40 | `LLI Queue Status Update - 2023-05-31.pdf` p3 ([URL](https://www.ercot.com/files/docs/2023/05/31/LLI%20Queue%20Status%20Update%20-%202023-05-31.pdf)) | **confirmed** |
| 6 | Input of Q7/X1's "~7 GW above weather + trend": ERCOT MW at the 2026 peak hour | 91.1 GW (table `ercot_load_hourly_wz`) | Table **91,133.7 MW** = `Native_Load_2026` 91,133.7 = D&E workbook 91,133.7; the eight zones in the table add up to 91,133.7. The model fit (84.2 GW expected) was **not re-run** | `ercot_load_hourly_wz` (source `ercot_native_load`); raw files as in 1a | **input confirmed; the excess itself is not verified** |

Gemini vs the eye read (claim 5): 10 of 10 values agree exactly: four in-service totals, plus the 2024-11/12, 2025-01,
2025-11/12 and 2026-01 approved stocks (`large_load_chart_values`, `verified=false`). Q5's medians (0.32 / 0.35) were not
re-derived. That would need all 28 vintages. The two vintages checked here give 0.33 and 0.40 for 2025.

## Notes: disagreements and caveats

No number differs. What follows are the places where the wording around a number can mislead, plus two findings about
the data.

1. **CLAUDE.md's "against a demand record of ~87–91 GW" (Jan 2026) is wrong.** The record in January 2026 was 85,508 MW
   (2023-08-10), and ERCOT's own April 2026 letter says so. 91.1 GW became the record only on 2026-07-22. Q1 already
   flagged this. Don't put it in the video.
2. **"ERCOT's official preliminary forecast missed by > 20 GW" is true, but incomplete.** The filing calls it the
   "preliminary LTLF", built from TSP large-load submissions. ERCOT says in the same letter that it "has concerns with
   using the preliminary load forecast values", and slide 7 says "It seems unlikely that … the peak [will] exceed 98GW".
   The actual fell inside ERCOT's own 90.5–98 GW range. Only 866 MW of the "20 GW" margin is safe (737 MW on the 15-min
   peak), and July is not final-settled. The claim breaks only with an upward revision of about 0.95%. For scale, the one
   day with both a preliminary and a settlement-based value in the lake, 2026-08-24 HE 18, moved 64 MW (0.07%): NP6-345
   90,753 MW vs D&E 90,817 MW.
3. **232.5 GW is dated.** It was the count as of 2026-01-21. By the May 2026 board update, ERCOT was "tracking ~438 GW
   of Large Load Interconnection requests and ~452 GW of … Generation Interconnection requests"
   (`8-Interconnection-and-Grid-Analysis-Update.pdf` p1, [URL](https://www.ercot.com/files/docs/2026/05/24/8-Interconnection-and-Grid-Analysis-Update.pdf)).
   The 3.8% counts approval to energize, not consumption. ERCOT observed a non-simultaneous peak of 3,977 MW of those
   loads in January 2026, 1.7% of the tracked total.
4. **What "active" means for the 1,810 / 438,262.** It counts the rows of ERCOT's two public detail sheets, excluding
   inactive and cancelled projects. The Large Gen sheet "only [includes] those projects for which a Full Interconnection
   Study has been requested". Small generators appear only once they are public. ERCOT's own headline for the same
   report is broader: 1,890 requests and 454,471 MW "under study". The 438,262 MW also includes projects already
   synchronized and waiting for COD, and repowers counted at their net change. **Beware of "438 GW" twice:** our
   generation count happens to equal ERCOT's large-load count of May 2026 (~438 GW). Always say which queue.
5. **Claim 5 reads chart images.** No status deck from May 2023 on carries the in-service or approvals charts as native
   chart XML. The Oct 2024 PPTX has native XML only for the approvals, zone and observation charts, whose last approved
   value is 5,697 MW for Oct 2024. The monthly A2E series is a stock, and it moves in steps: +1,074 MW in December 2025
   alone. So the 2025 ratio depends on which month the approvals landed in.
6. **Two ERCOT products, two values for 2023.** The hourly load archive (`Native_Load_2023`) peaks at 85,464 MW and the
   D&E workbook at 85,508 MW, in the same hour. For 2026 both still show the same preliminary value. Quote the D&E
   numbers (Q1 does).
7. **Small data bug, outside these claims.** The Jan and Feb 2026 TAC decks say "3977 MW in January 2025" and "3998 MW
   in February 2025": ERCOT's typo for 2026. `large_load_headlines` stores `as_of_date` 2025-01-01 and 2025-02-01 for
   those observed-peak rows. Anything that dates observed peaks from the headlines would put them a year early. That
   needs a parser fix or a note (not done here; X6 touches no pipeline code).
8. **Claim 6: only the input was checked.** The table's own source is `Native_Load_2026`, so matching that file checks
   the parser only. The independent product is the D&E workbook, and it agrees to the kW. The daily NP6-345 weather-zone
   files in the lake start at operating day 2026-08-24, so they don't cover July 22. The size of the excess depends on
   the model (per the X1 doc, not re-derived here): +6.9 GW with Q7's 2003–2025 fit (84.2 GW expected), +8.8 GW with a
   2010–2019 fit, +11.2 GW with the 2003–2019 pre-break fit.

## Suggested wording for the video

1a/1b/1c: "On July 22, 2026, Texas set a new all-time demand record: about 91.1 gigawatts, up from 85.5 GW in 2023."
Caveat to carry: *preliminary ERCOT data, before final settlement; hourly value (91.3 GW on the 15-minute peak).*

2a/2b: "In April, the preliminary long-term forecast ERCOT filed with the PUC, built from utilities' large-load
requests, put this summer's peak near 112 GW. The actual was 91 GW, about 21 GW lower." Caveat to carry: *ERCOT itself
flagged that number and projected 90.5–98 GW in the same filing, so this is a miss by the TSP-driven forecast, not by
ERCOT's operational outlook.*

3: "In January 2026, ERCOT was tracking about 232 GW of proposed large loads. Only 3.8% of that had approval to
energize." Caveat to carry: *dated January 21, 2026 (the queue was ~438 GW by May 2026); "approved to energize" is not
"consuming" (about 4 GW was actually drawing power).*

4: "ERCOT's August 2026 interconnection report lists 1,810 active generation projects with public details, 438 GW in
total." Caveat to carry: *ERCOT's headline for the same report is 1,890 requests and 454 GW under study; don't mix it up
with the ~438 GW large-load queue.*

5: "In October 2024, ERCOT's queue showed 26.8 GW of large loads in service by the end of 2025. By December 2025, 8.8 GW
had approval to energize: about a third." Caveat to carry: *stock-to-stock from ERCOT's chart labels, not project-level
tracking; approval to energize, not energized.*

6: "Even so, this summer's peak ran several gigawatts above what weather and the long-run trend explain." Caveat to
carry: *only the 91.1 GW input is verified; the excess is model-dependent (about 7 GW in the full-sample model, up to 11
GW against the pre-2020 trend), so give a range or say "several GW", not a single point.*
