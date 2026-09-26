"""X6 — independent verification of the numbers that may go into the 5-minute video.

Adversarial re-derivation: every number is recomputed from the raw files in the local lake
(``data/raw/source=<id>/dt=<date>/``), by a code path that does not import ``basecast_pipelines.models`` or the
parsers. Postgres is read only to compare against what the original analysis used (``read_sql``, read-only).
Results and verdicts: ``docs/analysis/x6_verify_claims.md``.

Run: ``uv run --group analysis python analysis/x6_verify_claims.py`` (~1 min; needs the Cloud SQL proxy on
127.0.0.1:5439 for the table comparisons only).

Claim 5's chart values come from chart *images* (PNG inside the PDF/PPTX). They were read by eye (Claude, X6,
2026-09-26) from the pages this script renders to ``analysis/out/x6_*.png``; the script re-renders the pages and
checks the same-deck text sentences, it cannot OCR the bars.
"""

# %% setup
from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import date
from pathlib import Path

import pdfplumber
import polars as pl
from _common import ROOT, out_path, read_sql

RAW = ROOT / "data" / "raw"
RESULTS: list[dict] = []


def manifest_entry(folder: Path, file: str) -> dict:
    """The ``_manifest.json`` entry for ``file`` (URL, fetched_at, sha256)."""
    entries = json.loads((folder / "_manifest.json").read_text())["entries"]
    return next(e for e in entries if e["file"] == file)


def record(claim: str, original: str, recomputed: str, source: str, verdict: str) -> None:
    RESULTS.append(
        {"claim": claim, "original": original, "recomputed": recomputed, "source": source, "verdict": verdict}
    )
    print(f"[{verdict}] {claim}: original {original} | recomputed {recomputed}")


def sheet_rows(src, sheet: str) -> list[tuple]:
    return list(pl.read_excel(src, sheet_name=sheet, has_header=False, infer_schema_length=0).iter_rows())


def to_float(x) -> float | None:
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def de_peaks(path: Path) -> dict[str, dict]:
    """Demand and Energy workbook, sheet 'Demand': this year's hourly and 15-min monthly maxima.

    Rows are found by their labels, never by position: the section title, then the first row whose label is
    '<year> Demand, MW', then 'Date' and 'Hour ending' / 'Interval ending'.
    """
    rows = sheet_rows(path, "Demand")
    out = {}
    for title, key in (
        ("Net System Maximum Hourly Demand", "hourly"),
        ("Net System Maximum Demand based on 15-Minute Intervals", "min15"),
    ):
        start = next(i for i, r in enumerate(rows) if (r[0] or "").strip() == title)
        header = next(r for r in rows[start:] if (r[0] or "").strip() == "Description")
        vi = next(i for i in range(start, len(rows)) if re.fullmatch(r"\d{4} Demand, MW", (rows[i][0] or "").strip()))
        vals, dates, hours = rows[vi], rows[vi + 1], rows[vi + 2]
        assert (dates[0] or "").strip() == "Date" and "ending" in (hours[0] or "").lower(), (
            path.name,
            dates[0],
            hours[0],
        )
        months = [(h or "").rstrip("*") for h in header[1:13]]
        out[key] = {
            m: (to_float(v), d, h) for m, v, d, h in zip(months, vals[1:13], dates[1:13], hours[1:13]) if to_float(v)
        }
        out[key]["Annual"] = to_float(vals[13])
    return out


DE = RAW / "source=ercot_demand_energy" / "dt=2026-09-26"
NATIVE = RAW / "source=ercot_native_load" / "dt=2026-09-25"

_item38 = zipfile.ZipFile(RAW / "source=puct_filings" / "dt=2026-09-26" / "58777_38_1622646.ZIP")
_docx = zipfile.ZipFile(io.BytesIO(_item38.read(next(n for n in _item38.namelist() if n.lower().endswith(".docx")))))
q_record_text = re.search(
    r"current all-time peak demand is ([\d,]+) MW",
    re.sub(r"<[^>]+>", "", _docx.read("word/document.xml").decode("utf8")),
).group(1)

# %% Claim 1 — the 2026 summer peak (hourly and 15-min) and the previous record
de26 = de_peaks(DE / "DemandandEnergy2026-for-Corp-Comms.xlsx")
hourly = {m: v for m, v in de26["hourly"].items() if m != "Annual"}
peak_m = max(hourly, key=lambda m: hourly[m][0])
peak_mw, peak_date, peak_he = hourly[peak_m]
m15 = {m: v for m, v in de26["min15"].items() if m != "Annual"}
peak15_m = max(m15, key=lambda m: m15[m][0])
print("D&E 2026 hourly by month:", {m: round(v[0]) for m, v in hourly.items()})
print(
    f"D&E 2026 hourly peak {peak_mw:,.3f} MW on {peak_date} HE {peak_he}; annual cell {de26['hourly']['Annual']:,.3f}"
)
print(f"D&E 2026 15-min peak {m15[peak15_m][0]:,.3f} MW on {m15[peak15_m][1]} interval ending {m15[peak15_m][2]}")


def native_load(zip_path: Path) -> pl.DataFrame:
    """ERCOT hourly load archive (Native_Load_<year>): hour-ending string + ERCOT column, header read dynamically."""
    z = zipfile.ZipFile(zip_path)
    member = next(n for n in z.namelist() if n.lower().endswith((".xlsx", ".xls")))
    df = pl.read_excel(io.BytesIO(z.read(member)), sheet_id=1, infer_schema_length=0)
    hcol = next(c for c in df.columns if "hour" in c.lower())
    ecol = next(c for c in df.columns if c.strip().upper() == "ERCOT")
    return df.select(he=pl.col(hcol).cast(pl.Utf8), ercot=pl.col(ecol).cast(pl.Float64, strict=False)).drop_nulls(
        "ercot"
    )


nl26 = native_load(NATIVE / "Native_Load_2026.zip")
summer = nl26.filter(pl.col("he").str.slice(0, 2).is_in(["06", "07", "08", "09"]))
nl_peak = summer.sort("ercot", descending=True).row(0, named=True)
print(
    f"Native_Load_2026: rows {nl26.height}, last hour {nl26['he'][-1]}; summer max {nl_peak['ercot']:,.3f} at {nl_peak['he']}"
)

# Daily weather-zone actuals (NP6-345, preliminary): one CSV per operating day, in the lake from op day 2026-08-24.
daily = []
for d in sorted((RAW / "source=ercot_load_wz_daily").glob("dt=*")):
    for zp in d.glob("*.zip"):
        z = zipfile.ZipFile(zp)
        for m in z.namelist():
            daily.append(pl.read_csv(io.BytesIO(z.read(m)), infer_schema_length=0))
daily = (
    pl.concat(daily, how="diagonal")
    .select(
        day=pl.col("OperDay").str.to_date("%m/%d/%Y"),
        he=pl.col("HourEnding"),
        dst=pl.col("DSTFlag"),
        total=pl.col("TOTAL").cast(pl.Float64),
    )
    .unique(["day", "he", "dst"], keep="last")
    .sort("day", "he")
)
aug24 = daily.filter(pl.col("day") == date(2026, 8, 24)).sort("total", descending=True).row(0, named=True)
sep = daily.filter(pl.col("day").dt.month() == 9).sort("total", descending=True).row(0, named=True)
print(
    f"daily WZ files: op days {daily['day'].min()} .. {daily['day'].max()}; 2026-08-24 max {aug24['total']:,.2f} HE {aug24['he']}; "
    f"Sep max {sep['total']:,.2f} on {sep['day']} HE {sep['he']}"
)
print(f"  D&E Aug 2026 hourly: {hourly['Aug'][0]:,.3f} on {hourly['Aug'][1]} HE {hourly['Aug'][2]}")

# Previous record: every D&E workbook's annual hourly max (2017-2025, xlsx) and the Native_Load archives 2019-2025.
prev = []
for f in sorted(DE.glob("*.xlsx")):
    if "2026" in f.name:
        continue
    try:
        p = de_peaks(f)
    except (StopIteration, AssertionError, IndexError, ValueError) as e:  # older layouts: report, don't guess
        print("  D&E layout not read:", f.name, type(e).__name__)
        continue
    h = {m: v for m, v in p["hourly"].items() if m != "Annual"}
    m = max(h, key=lambda k: h[k][0])
    prev.append({"file": f.name, "mw": h[m][0], "date": h[m][1], "he": h[m][2]})
prev_de = pl.DataFrame(prev).sort("mw", descending=True)
print(prev_de)
native_prev = []
for y in range(2019, 2026):
    zp = NATIVE / f"Native_Load_{y}.zip"
    if zp.exists():
        r = native_load(zp).sort("ercot", descending=True).row(0, named=True)
        native_prev.append({"year": y, "mw": r["ercot"], "he": r["he"]})
native_prev = pl.DataFrame(native_prev).sort("mw", descending=True)
print(native_prev)
rec = prev_de.row(0, named=True)
# ERCOT's own all-time statement: the 'Max All Time, MW' row of the 2025 workbook (hourly section), max over months.
rows25 = sheet_rows(DE / "DemandandEnergy2025-for-Corp-Comms.xlsx", "Demand")
mat = next(i for i, r in enumerate(rows25) if (r[0] or "").strip() == "Max All Time, MW")
alltime = max(zip(rows25[mat][1:13], rows25[mat + 1][1:13]), key=lambda t: to_float(t[0]) or 0)
print(f"2025 workbook 'Max All Time, MW' (hourly), max over months: {to_float(alltime[0]):,.3f} on {alltime[1]}")
src_de = manifest_entry(DE, "DemandandEnergy2026-for-Corp-Comms.xlsx")["url"]

record(
    "1a 2026 summer peak, hourly",
    "91,134 MW, HE 18 CDT, 2026-07-22",
    f"{peak_mw:,.1f} MW on {peak_date} HE {peak_he} (D&E); Native_Load_2026 {nl_peak['ercot']:,.1f} at {nl_peak['he']}",
    f"DemandandEnergy2026-for-Corp-Comms.xlsx, sheet Demand ({src_de}); Native_Load_2026.xlsx",
    "confirmed" if round(peak_mw) == 91134 and peak_date == "07/22/2026" and peak_he == "18:00" else "differs",
)
record(
    "1b 2026 peak, 15-min",
    "91,263 MW",
    f"{m15[peak15_m][0]:,.1f} MW on {m15[peak15_m][1]} interval ending {m15[peak15_m][2]}",
    "same workbook, 'Net System Maximum Demand based on 15-Minute Intervals'",
    "confirmed" if round(m15[peak15_m][0]) == 91263 else "differs",
)
record(
    "1c previous record",
    "85,508 MW on 2023-08-10",
    f"{rec['mw']:,.1f} MW on {rec['date']} HE {rec['he']} ({rec['file']}); Native_Load max 2019-2025 "
    f"{native_prev['mw'][0]:,.1f} at {native_prev['he'][0]}; 2025 workbook 'Max All Time' {to_float(alltime[0]):,.1f} "
    f"on {alltime[1]}; ERCOT letter 2026-04-15: 'current all-time peak demand is {q_record_text} MW'",
    "D&E workbooks 2017-2025, sheet Demand; Native_Load_2019..2025",
    "confirmed" if round(rec["mw"]) == 85508 and rec["date"] == "08/10/2023" else "differs",
)

# %% Claim 2 — the preliminary 2026 LTLF (~112 GW) and ERCOT's 90.5-98 GW range, PUCT 58777 item 38
PUCT = RAW / "source=puct_filings" / "dt=2026-09-26"
item38 = zipfile.ZipFile(PUCT / "58777_38_1622646.ZIP")
docx_name = next(n for n in item38.namelist() if n.lower().endswith(".docx"))
docx = zipfile.ZipFile(io.BytesIO(item38.read(docx_name))).read("word/document.xml").decode("utf8")
paras = [re.sub(r"<[^>]+>", "", p) for p in re.findall(r"<w:p[ >].*?</w:p>", docx, flags=re.DOTALL)]
letter = " ".join(p.strip() for p in paras if p.strip())
q_range = re.search(
    r"ERCOT currently projects that summer 2026 peak load will fall within a range of approximately "
    r"([\d,]+) MW to ([\d,]+) MW, compared with the preliminary LTLF.s forecasted peak demand of "
    r"approximately ([\d,]+) MW for summer 2026",
    letter,
)
q_record = re.search(r"current all-time peak demand is ([\d,]+) MW", letter)
q_date = re.search(
    r"(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, 20\d\d", letter
)
lo, hi, prelim = (int(g.replace(",", "")) for g in q_range.groups())
print("letter date:", q_date.group(0), "| quote:", q_range.group(0))
print("letter all-time peak:", q_record.group(1))
pptx = zipfile.ZipFile(io.BytesIO(item38.read(next(n for n in item38.namelist() if n.lower().endswith(".pptx")))))
slide7 = " | ".join(re.findall(r"<a:t>([^<]*)</a:t>", pptx.read("ppt/slides/slide7.xml").decode("utf8")))
print("Attachment A slide 7:", slide7)
bars = [int(b.replace(",", "")) for b in re.findall(r"([\d,]{5,}) MW", slide7)]
url38 = manifest_entry(PUCT, "58777_38_1622646.ZIP")["url"]
actual = round(peak_mw)
record(
    "2a preliminary LTLF, summer 2026",
    "~112,000 MW (+20.9 GW / +22.9% vs actual)",
    f"{prelim:,} MW quoted; minus actual {actual:,} = {prelim - actual:,} MW ({(prelim - actual) / actual:+.1%})",
    f"PUCT 58777 item 38, cover letter docx ({url38}), dated {q_date.group(0)}; Attachment A slide 7",
    "confirmed" if prelim == 112000 and prelim - actual > 20000 else "differs",
)
record(
    "2b ERCOT's own range, same filing",
    "90,500-98,000 MW",
    f"{lo:,}-{hi:,} MW quoted (slide 7 bars {', '.join(f'{b:,}' for b in bars)} MW); actual is {actual - lo:+,} MW vs low end",
    "same letter, last paragraph; Attachment A slide 7",
    "confirmed" if (lo, hi) == (90500, 98000) else "differs",
)

# %% Claim 3 — "~232.5 GW of large loads tracked in Jan 2026, only 3.8% approved to energize"
LL = RAW / "source=ercot_large_load_decks"
mon = LL / "dt=2026-01-01" / "ERCOT-Monthly-January-2026-FINAL.pdf"
with pdfplumber.open(mon) as pdf:
    mon_pages = {i: " ".join((pg.extract_text() or "").split()) for i, pg in enumerate(pdf.pages, 1)}
m_tracked = next(
    (i, m)
    for i, t in mon_pages.items()
    if (m := re.search(r"As of (\w+ \d+, \d{4}).*?tracked by ERCOT total approximately ([\d,]+) megawatts", t))
)
m_a2e = next(
    (i, m)
    for i, t in mon_pages.items()
    if (m := re.search(r"To date, ([\d,]+) MW of large load demand has received approval to energize", t))
)
m_obs = next(
    (i, m) for i, t in mon_pages.items() if (m := re.search(r"non-simultaneous peak demand of ([\d,]+) MW", t))
)
tracked = int(m_tracked[1].group(2).replace(",", ""))
a2e_jan = int(m_a2e[1].group(1).replace(",", ""))
obs_jan = int(m_obs[1].group(1).replace(",", ""))
print(
    f"ERCOT Monthly Jan 2026 p{m_tracked[0]}: as of {m_tracked[1].group(1)}, tracked {tracked:,} MW; p{m_a2e[0]}: A2E {a2e_jan:,} MW; "
    f"observed non-simultaneous {obs_jan:,} MW"
)
tac = zipfile.ZipFile(LL / "dt=2026-01-21" / "16.-Large-Load-Issues.zip")
with pdfplumber.open(io.BytesIO(tac.read("January TAC Report.pdf"))) as pdf:
    tac_text = {i: " ".join((pg.extract_text() or "").split()) for i, pg in enumerate(pdf.pages, 1)}
tac_a2e = next(
    (i, int(m.group(1)))
    for i, t in tac_text.items()
    if (m := re.search(r"Of the (\d+) MW that have received Approval to Energize", t))
)
print(f"January TAC Report (2026-01-21) p{tac_a2e[0]}: A2E {tac_a2e[1]:,} MW")
hl = read_sql(
    "SELECT report_date, file_name, page, metric, value FROM large_load_headlines "
    "WHERE report_date IN ('2026-01-01', '2026-01-21') ORDER BY report_date, metric"
)
print(hl)
hl_tracked = hl.filter(pl.col("metric") == "total_mw_tracked")["value"].to_list()
hl_a2e = hl.filter(pl.col("metric") == "approved_to_energize_mw")["value"].to_list()
share = a2e_jan / tracked
url_mon = manifest_entry(LL / "dt=2026-01-01", mon.name)["url"]
record(
    "3 large loads tracked / approved, Jan 2026",
    "~232.5 GW; 3.8% approved to energize",
    f"{tracked:,} MW; {a2e_jan:,} MW approved = {share:.2%} (observed consuming {obs_jan:,} MW = {obs_jan / tracked:.1%}); "
    f"TAC deck {tac_a2e[1]:,} MW; headlines table {hl_tracked} / {hl_a2e}",
    f"ERCOT Monthly Jan 2026 p{m_tracked[0]} ({url_mon}); January TAC Report p{tac_a2e[0]}",
    "confirmed" if tracked == 232500 and round(share * 100, 1) == 3.8 else "differs",
)

# Context, not a claim: the same count by mid-2026 (board update, June 2026).
jun = LL / "dt=2026-06-01"
for f in sorted(jun.glob("*.pdf")):
    with pdfplumber.open(f) as pdf:
        for i, pg in enumerate(pdf.pages, 1):
            t = " ".join((pg.extract_text() or "").split())
            if m := re.search(r"ERCOT is tracking ~?[\d,.]+ GW of Large Load[^.]*\.", t):
                print(f"context, {f.name} p{i}: {m.group(0)}")

# %% Claim 4 — the generation queue in the Aug 2026 GIS report
GIS = RAW / "source=ercot_gis" / "dt=2026-08-01"
gis_file = next(GIS.glob("*GIS_Report_August2026.xlsx"))


def detail_rows(sheet: str) -> pl.DataFrame:
    rows = sheet_rows(gis_file, sheet)
    hi_ = next(i for i, r in enumerate(rows) if (r[0] or "").strip() == "INR")
    head = [(c or "").strip() for c in rows[hi_]]
    cap = next(j for j, c in enumerate(head) if c.lower().startswith("capacity"))
    data = [
        (r[0].strip(), to_float(r[cap]))
        for r in rows[hi_ + 1 :]
        if r[0] and re.fullmatch(r"\d{2}INR\d{4}\w*", r[0].strip())
    ]
    return pl.DataFrame(data, schema=["inr", "mw"], orient="row").with_columns(sheet=pl.lit(sheet))


def listed_inrs(sheet: str) -> pl.DataFrame:
    rows = sheet_rows(gis_file, sheet)
    hi_ = next(i for i, r in enumerate(rows) if "INR" in [(c or "").strip() for c in r])
    head = [(c or "").strip() for c in rows[hi_]]
    j, k = head.index("INR"), next(j for j, c in enumerate(head) if c.startswith("MW"))
    return pl.DataFrame(
        [(r[j].strip(), to_float(r[k])) for r in rows[hi_ + 1 :] if r[j] and "INR" in r[j]],
        schema=["inr", "mw"],
        orient="row",
    )


large, small = detail_rows("Project Details - Large Gen"), detail_rows("Project Details - Small Gen")
details = pl.concat([large, small])
inactive, cancelled = listed_inrs("Inactive Projects"), listed_inrs("Cancellation Update")
dupes = details.filter(pl.col("inr").is_duplicated())
gone = inactive["inr"].to_list() + cancelled["inr"].to_list()
overlap = details.filter(pl.col("inr").is_in(gone))
active = details.unique("inr").filter(~pl.col("inr").is_in(gone))
summary = sheet_rows(gis_file, "Summary")
tracking = next(
    re.search(r"tracking (\d+) generation", c).group(1) for r in summary for c in r if c and "Currently tracking" in c
)
under_study = next(to_float(r[1]) for r in summary if (r[0] or "").strip() == "Total Capacity Under Study")
print(
    f"Large Gen {large.height} rows / {large['mw'].sum():,.2f} MW; Small Gen {small.height} / {small['mw'].sum():,.2f} MW; "
    f"duplicated INRs {dupes.height}; in inactive/cancelled sheets {overlap.height}"
)
print(f"active (detail sheets, minus inactive/cancelled): {active.height} projects, {active['mw'].sum():,.2f} MW")
print(
    f"inactive sheet: {inactive.height} projects, {inactive['mw'].sum():,.2f} MW; cancelled this month {cancelled.height}, "
    f"{cancelled['mw'].sum():,.2f} MW"
)
print(f"ERCOT Summary sheet: tracking {tracking} requests, total capacity under study {under_study:,.2f} MW")
snap = read_sql(
    "SELECT status, count(*) AS n, sum(capacity_mw) AS mw FROM gis_snapshots "
    "WHERE report_month = '2026-08-01' GROUP BY status ORDER BY status"
)
print(snap)
url_gis = manifest_entry(GIS, gis_file.name)["url"]
record(
    "4 generation queue, Aug 2026 GIS",
    "1,810 active projects, 438,262 MW",
    f"{active.height:,} projects, {active['mw'].sum():,.0f} MW (Large Gen {large.height} + Small Gen {small.height} detail rows); "
    f"ERCOT's Summary: {int(tracking):,} requests, {under_study:,.0f} MW under study",
    f"{gis_file.name}, sheets 'Project Details - Large Gen' + '- Small Gen' ({url_gis})",
    "confirmed" if active.height == 1810 and round(active["mw"].sum()) == 438262 else "differs",
)

# %% Claim 5 — Q5 realization inputs: approved-to-energize stock (Dec 2024, Dec 2025) and one vintage's promises
# Chart values are images; the pages are rendered here and were read by eye (X6). The numbers below are that read.
EYE = {
    # (document, page): {label: MW}
    ("dt=2025-01-22 LLI Queue Status Update - 2025-1.pdf", 5): {
        "A2E 2024-12": 6297,
        "A2E 2025-01": 6306,
        "A2E 2024-11": 6297,
    },
    ("dt=2026-01-21 January TAC Report.pdf", 4): {"A2E 2025-12": 8786, "A2E 2025-11": 7712, "A2E 2026-01": 8786},
    ("dt=2024-10-30 LLI Queue Status Update - 2024-10-30.pptx", 3): {"promised 2024": 16803, "promised 2025": 26836},
    ("dt=2023-05-31 LLI Queue Status Update - 2023-05-31.pdf", 3): {
        "promised 2024": 20715,
        "promised 2025": 21986,
        "A2E base": 2620,
    },
}
renders = {
    "x6_jan2025_deck_p5.png": (
        zipfile.ZipFile(LL / "dt=2025-01-22" / "18-ercot-reports.zip").read("LLI Queue Status Update - 2025-1.pdf"),
        5,
    ),
    "x6_jan2026_deck_p4.png": (tac.read("January TAC Report.pdf"), 4),
    "x6_may2023_deck_p3.png": ((LL / "dt=2023-05-31" / "LLI Queue Status Update - 2023-05-31.pdf").read_bytes(), 3),
}
for name, (b, page) in renders.items():
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        pdf.pages[page - 1].to_image(resolution=110).save(out_path(name))
oct24 = zipfile.ZipFile(
    io.BytesIO(
        zipfile.ZipFile(LL / "dt=2024-10-30" / "15-ercot-reports.zip").read("LLI Queue Status Update - 2024-10-30.pptx")
    )
)
rels = oct24.read("ppt/slides/_rels/slide3.xml.rels").decode()
imgs = re.findall(r'Target="\.\./media/(image\d+\.png)"', rels)
for img in imgs:
    out_path(f"x6_oct2024_slide3_{img}").write_bytes(oct24.read(f"ppt/media/{img}"))
print("rendered for the eye read:", list(renders), [f"x6_oct2024_slide3_{i}" for i in imgs])

# Text sentences that bracket the two year-end stocks (independent of any chart).
sentences = []
for d in sorted(LL.glob("dt=*")):
    if not ("2024-11" <= d.name[3:] <= "2025-01-31" or "2025-11" <= d.name[3:] <= "2026-01-31"):
        continue
    blobs = [(f.name, f.read_bytes()) for f in d.glob("*.pdf")]
    for zp in d.glob("*.zip"):
        z = zipfile.ZipFile(zp)
        blobs += [(m, z.read(m)) for m in z.namelist() if m.lower().endswith(".pdf")]
    for name, b in blobs:
        with pdfplumber.open(io.BytesIO(b)) as pdf:
            for i, pg in enumerate(pdf.pages, 1):
                t = " ".join((pg.extract_text() or "").split())
                for m in re.finditer(
                    r"(?:Of the (?:total )?|To date, |➢ )([\d,]{4,}) MWs? (?:that )?(?:have |of large load demand has )?(?:received )?[Aa]ppro",
                    t,
                ):
                    mw = int(m.group(1).replace(",", ""))
                    sentences.append({"dt": d.name[3:], "doc": name, "page": i, "a2e_mw": mw})
sent = pl.DataFrame(sentences).unique(["dt", "doc", "a2e_mw"]).sort("dt")
print(sent)
# Oct 2024 deck: the approvals chart IS native (chart XML), use it for the vintage's own A2E base.
chart2 = oct24.read("ppt/charts/chart2.xml").decode()
ser = re.findall(r"<c:ser>.*?</c:ser>", chart2, flags=re.DOTALL)
a2e_ser = next(s for s in ser if "Approved to Energize" in s)
vals = [float(v) for v in re.findall(r"<c:v>([\d.]+)</c:v>", a2e_ser.split("<c:val>")[1])]
print("Oct 2024 PPTX native chart2 'Approved to Energize' last 3 values:", vals[-3:])

# What the original analysis used: Gemini's reading of the same bars (large_load_chart_values, verified=false).
gemini = read_sql(
    "SELECT report_date, page, status_bucket, category, value_mw FROM large_load_chart_values "
    "WHERE (page = 3 AND report_date IN ('2023-05-31', '2024-10-30') AND category IN ('2024', '2025') "
    "       AND status_bucket = 'total') "
    "   OR (report_date = '2025-01-22' AND page = 5 AND category IN ('2024-11', '2024-12', '2025-01') "
    "       AND status_bucket = 'approved_to_energize') "
    "   OR (report_date = '2026-01-21' AND page = 4 AND category IN ('2025-11', '2025-12', '2026-01') "
    "       AND status_bucket = 'approved_to_energize') "
    "ORDER BY report_date, category"
)
print(gemini)

a24, a25 = (
    EYE[("dt=2025-01-22 LLI Queue Status Update - 2025-1.pdf", 5)]["A2E 2024-12"],
    EYE[("dt=2026-01-21 January TAC Report.pdf", 4)]["A2E 2025-12"],
)
p24, p25 = (
    EYE[("dt=2024-10-30 LLI Queue Status Update - 2024-10-30.pptx", 3)][k] for k in ("promised 2024", "promised 2025")
)
q24, q25 = (
    EYE[("dt=2023-05-31 LLI Queue Status Update - 2023-05-31.pdf", 3)][k] for k in ("promised 2024", "promised 2025")
)
print(
    f"gross A2E ratio, Oct 2024 vintage: 2024 {a24 / p24:.3f}, 2025 {a25 / p25:.3f}; May 2023 vintage: 2024 {a24 / q24:.3f}, 2025 {a25 / q25:.3f}"
)
record(
    "5a A2E stock Dec 2024",
    "6,297 MW",
    f"{a24:,} MW (Jan 2025 deck p5 chart, eye read); text: Nov 20 2024 deck 6,297, Jan 22 2025 deck 6,306",
    "LLI Queue Status Update - 2025-1.pdf p5 (in 18-ercot-reports.zip, dt=2025-01-22)",
    "confirmed",
)
record(
    "5b A2E stock Dec 2025",
    "8,786 MW",
    f"{a25:,} MW (Jan 2026 deck p4 chart, eye read; Nov 2025 bar 7,712); text: Nov 19 2025 deck 7,502, "
    f"Jan 21 2026 deck {tac_a2e[1]:,}",
    "January TAC Report.pdf p4 (16.-Large-Load-Issues.zip, dt=2026-01-21)",
    "confirmed",
)
record(
    "5c promised by 2024 / 2025, Oct 2024 vintage",
    "16,803 / 26,836 MW (q5_ratios.csv)",
    f"{p24:,} / {p25:,} MW (eye read); gross ratio {a24 / p24:.2f} / {a25 / p25:.2f}",
    "LLI Queue Status Update - 2024-10-30.pptx slide 3 (PNG, not native chart)",
    "confirmed",
)
record(
    "5d promised by 2024 / 2025, May 2023 vintage",
    "20,715 / 21,986 MW (q5_ratios.csv)",
    f"{q24:,} / {q25:,} MW (eye read, chart labels and the slide's own table); gross ratio {a24 / q24:.2f} / {a25 / q25:.2f}",
    "LLI Queue Status Update - 2023-05-31.pdf p3",
    "confirmed",
)

# %% Claim 6 — the input of Q7/X1's "+7 GW over weather+trend": ERCOT MW at the 2026 peak hour
nl_row = nl26.filter(pl.col("he").str.starts_with("07/22/2026 18:00"))
zone = read_sql(
    "SELECT weather_zone, mw, source FROM ercot_load_hourly_wz "
    "WHERE operating_date = '2026-07-22' AND hour_ending = 18 ORDER BY weather_zone"
)
tbl = zone.filter(pl.col("weather_zone") == "ERCOT")["mw"].item()
zsum = zone.filter(pl.col("weather_zone") != "ERCOT")["mw"].sum()
tbl_max = read_sql(
    "SELECT operating_date, hour_ending, mw FROM ercot_load_hourly_wz WHERE weather_zone = 'ERCOT' "
    "AND operating_date >= '2026-06-01' ORDER BY mw DESC LIMIT 1"
).row(0, named=True)
print(
    f"Native_Load_2026 07/22 HE18 ERCOT {nl_row['ercot'].item():,.3f}; table ERCOT {tbl:,.3f} (source {zone['source'][0]}); "
    f"sum of 8 zones {zsum:,.3f}; table summer max {tbl_max}"
)
has_jul = daily.filter(pl.col("day") == date(2026, 7, 22)).height
print(f"daily WZ (NP6-345) rows for 2026-07-22 in the lake: {has_jul}")
record(
    "6 ERCOT MW at the 2026 peak hour (Q7/X1 input)",
    "91.1 GW actual (table ercot_load_hourly_wz)",
    f"table {tbl:,.1f} = Native_Load_2026 {nl_row['ercot'].item():,.1f} = D&E {peak_mw:,.1f}; zones sum {zsum:,.1f}; "
    f"model fit not re-run",
    "Native_Load_2026.xlsx (the table's own source) and the D&E workbook (independent product); "
    "the daily NP6-345 files start at op day 2026-08-24",
    "confirmed (input only)",
)

# %% summary table
res = pl.DataFrame(RESULTS)
print(res.select("claim", "verdict"))
res.write_csv(out_path("x6_verify_claims.csv"))
