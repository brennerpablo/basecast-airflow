"""Rebuild the trimmed ercot_gis fixtures from real raw files (a one-off tool, not a test).

Each fixture keeps the real sheets' layout (title rows, notes, wrapped header rows, blank rows, the
position of the used range) and only a few of their data rows, chosen by INR. Values are written back
with their Excel type (numbers, datetimes, text), so the parser sees what it sees in the real files.

    uv run python tests/fixtures/ercot_gis/make_fixtures.py /path/to/data/raw/source=ercot_gis
"""

from __future__ import annotations

import io
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import xlsxwriter

from basecast_pipelines.parsers.ercot.gis import _INR_CODE, find_blocks, read_sheets

HERE = Path(__file__).parent

# fixture name -> (raw file relative to the source folder, {sheet: INRs to keep | number of rows | None = all})
FIXTURES = {
    "gis_report_2017_08_trimmed.xlsx": (
        "dt=2017-08-01/GIS_REPORT__August_2017_Revised.xlsx",
        {
            "Projects": None,
            "Newly Operational & Cancelled": {"15INR0070_1b", "16INR0065b", "16INR0122", "13INR0006", "19INR0024"},
            "Full Study Table": {"12INR0055", "13INR0006", "13INR0025"},
            "IA Table": {"13INR0049", "14INR0027", "15INR0070_1b"},
            "Wind Chart": 2,
            "Solar Chart": {"15INR0070_1b"},
        },
    ),
    "gis_report_2026_08_trimmed.xlsx": (
        "dt=2026-08-01/RPT.00015933.0000000000000000.20260901.143805843.GIS_Report_August2026.xlsx",
        {
            "Summary": None,
            "Project Details - Large Gen": {"15INR0064b", "16INR0049", "22INR0467", "23INR0249"},
            "Project Details - Small Gen": {"22INR0596", "26INR0574"},
            "Commissioning Update": {"22INR0467", "23INR0249", "24INR0294", "26INR0574"},
            "Inactive Projects": {"13INR0010a", "17INR0022"},
            "Cancellation Update": {"21INR0280", "22INR0337"},
        },
    ),
    "colocated_battery_2026_08_trimmed.xlsx": (
        "dt=2026-08-01/RPT.00015933.0000000000000000.20260909.102333702."
        "Co-located_Battery_Identification_Report_August_2026.xlsx",
        {
            "Summary": None,
            "Co-located with Solar": 3,
            "Stand-Alone": 2,
            "Co-located Operational": 3,
        },
    ),
}

_STAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$")
_NUMBER = re.compile(r"^-?\d+(\.\d+)?(e-?\d+)?$")


def _write(ws, row: int, col: int, value: str, fmt) -> None:
    if _STAMP.match(value):
        ws.write_datetime(row, col, datetime.fromisoformat(value), fmt)
    elif _NUMBER.match(value):
        ws.write_number(row, col, float(value))
    else:
        ws.write_string(row, col, value)


def trim(src: Path, dst: Path, keep: dict) -> None:
    data = src.read_bytes()
    workbook = xlsxwriter.Workbook(dst)
    fmt = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})
    for sheet in read_sheets(data):
        if sheet.name not in keep:
            continue
        rule = keep[sheet.name]
        drop: set[int] = set()
        for b in find_blocks(sheet.rows):
            data_rows = [i for i in range(b.data_start, b.data_end)
                         if sheet.rows[i][b.key_col] and (_INR_CODE.match(sheet.rows[i][b.key_col].strip())
                                                          or "Unit" in b.labels[b.key_col])]
            if rule is None:
                continue
            if isinstance(rule, int):
                listed = [i for i in data_rows if not str(sheet.rows[i][b.key_col]).startswith("*")]
                drop |= set(listed[rule:])
            else:
                drop |= {i for i in data_rows if sheet.rows[i][b.key_col].strip() not in rule}
        ws = workbook.add_worksheet(sheet.name)
        out = sheet.row0
        for i, row in enumerate(sheet.rows):
            if i in drop:
                continue
            for j, value in enumerate(row):
                if value is not None and value != "":
                    _write(ws, out, sheet.cols[j], value, fmt)
            out += 1
    workbook.close()


def main(raw_root: str) -> None:
    root = Path(raw_root)
    for name, (rel, keep) in FIXTURES.items():
        trim(root / rel, HERE / name, keep)
        print(name, (HERE / name).stat().st_size, "bytes")
    # the Oct 2014 - Feb 2015 reports are zips holding one workbook
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(HERE / "gis_report_2017_08_trimmed.xlsx", "GIS_REPORT__August_2017_FINAL.xlsx")
    (HERE / "gis_report_2017_08_trimmed.zip").write_bytes(buffer.getvalue())


if __name__ == "__main__":
    main(sys.argv[1])
