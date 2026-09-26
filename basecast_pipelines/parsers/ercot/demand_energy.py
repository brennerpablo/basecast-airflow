"""ERCOT Demand and Energy report → ``ercot_monthly_peaks``: monthly peaks (with the local date and time of the
peak) and energy for the ERCOT system, by load zone and by weather zone, one tidy row per month × region ×
metric.

One workbook per year (2008 → current; xls until 2015, xlsx after). The sheets used, by layout:

- ``Demand``: net system maximum hourly demand (value, date, hour ending) and its forecast, the maximum based
  on 15-minute intervals (value, date, interval ending) and, from 2010, the minimum demand (15-minute);
- ``Energy``: net energy for load and its forecast;
- ``Load Zones`` / ``LoadZones`` (nodal market, 2011+; 2011 also lists the DC-tie zones ``DC_*``) and
  ``CongestionZones`` (zonal market, 2008-2010): zone demand coincident with the ERCOT 15-minute peak,
  non-coincident zone peak (``day @ HH:MM``) and zone energy;
- ``Weather Zones`` / ``WeatherZones``: the same three, by weather zone (names mapped to the contract codes;
  "Coastal", "Far West", "North Central"... in the older files).

Rows are read by their labels, never by position: the label column, the date formats (day of month,
``MM/DD/YYYY``, Excel dates) and the time formats (``0800``, ``8:00``, ``08:00``) all vary between vintages.
Only the report year's own rows are kept; the prior-year comparison rows, the all-time maxima, the
differences, YTD totals and load factors are derived and skipped.

Metrics: ``peak_hourly_mw``, ``forecast_peak_hourly_mw``, ``peak_15min_mw``, ``min_15min_mw``, ``energy_mwh``,
``forecast_energy_mwh``, ``coincident_peak_15min_mw`` and ``noncoincident_peak_15min_mw``. ``peak_local`` is the
published local wall-clock **end** of the peak interval (hour ending or 15-minute interval ending) and
``peak_ts_utc`` the same instant in UTC; coincident zone rows take the ERCOT 15-minute peak's time.
``final_settlement`` is true when the workbook flags the month (``Jan*``) as updated with final settlements
(the flag's meaning is read from the sheet's footnotes; the 2016 workbook uses ``*``, ``**`` and ``***``).

The current-year file is overwritten monthly; for years with more than one workbook (2009 and 2011 "through
November", two 2013 uploads) only the newest upload of each report year is read (``select``). Written
``by_key`` on (``month``, ``region_type``, ``region_id``, ``metric``).

Known quirks of the published workbooks, kept as published: the 2019 workbook's October coincident zone
demand is about twice the system peak in both zone sheets (the other 223 months sum to the ERCOT 15-minute
peak); congestion zones stop in November 2010 (nodal market from December); the November 2014 hourly peak
reads hour ending ``7:15``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile
from basecast_pipelines.processing.tabular import clean_label, read_grids

SOURCE_ID = "ercot_demand_energy"
LOCAL_TZ = ZoneInfo("America/Chicago")

_MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
_MONTH_CELL = re.compile(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s*(\**)$", re.IGNORECASE)
_YEAR_TITLE = re.compile(r"\bfor\s+((?:19|20)\d{2})\b", re.IGNORECASE)

_WEATHER_ZONES = {
    "COAST": "COAST", "COASTAL": "COAST", "EAST": "EAST", "FAR_WEST": "FWEST", "FWEST": "FWEST",
    "NORTH": "NORTH", "NORTH_CENTRAL": "NCENT", "NCENT": "NCENT", "NORTH_C": "NCENT", "SOUTH": "SOUTH",
    "SOUTHERN": "SOUTH", "SOUTH_CENTRAL": "SCENT", "SCENT": "SCENT", "SOUTH_C": "SCENT", "WEST": "WEST",
}
_CONGESTION_ZONES = {"NORTH", "SOUTH", "HOUSTON", "WEST", "NORTHEAST"}

SCHEMA = {
    "report_year": pl.Int16,
    "month": pl.Date,
    "region_type": pl.String,
    "region_id": pl.String,
    "metric": pl.String,
    "value": pl.Float64,
    "interval_minutes": pl.Int16,
    "peak_local": pl.Datetime("us"),
    "peak_ts_utc": pl.Datetime("us", "UTC"),
    "final_settlement": pl.Boolean,
}
KEY = ("month", "region_type", "region_id", "metric")


@dataclass
class _Header:
    label_col: int
    months: dict[int, int]  # column → month number
    stars: dict[int, int] = field(default_factory=dict)  # month → number of '*' after its name


@dataclass
class _Record:
    report_year: int
    month: int
    region_type: str
    region_id: str
    metric: str
    value: float
    interval_minutes: int | None = None
    peak_local: datetime | None = None
    final_settlement: bool = False


def _cells(row: tuple) -> list[str]:
    return [clean_label(c) for c in row if c is not None and clean_label(c)]


def _header(row: tuple) -> _Header | None:
    months, stars = {}, {}
    for j, cell in enumerate(row):
        m = _MONTH_CELL.match(clean_label(cell)) if cell is not None else None
        if m:
            month = _MONTHS.index(m.group(1).lower()[:3]) + 1
            months[j] = month
            stars[month] = len(m.group(2))
    if len(months) < 6:
        return None
    return _Header(label_col=min(months) - 1, months=months, stars=stars)


def _number(cell: object) -> float | None:
    text = clean_label(cell).replace(",", "")
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _final_stars(rows: list[tuple], year: int) -> set[int]:
    """Star counts whose footnote says the month was updated with final settlements for the report year."""
    out = set()
    for row in rows:
        for text in _cells(row):
            m = re.match(r"^(\*+)\s*(.*)$", text)
            if not m or "final settlement" not in m.group(2).lower():
                continue
            years = {int(y) for y in re.findall(r"(?:19|20)\d{2}", m.group(2))}
            if not years or year in years:
                out.add(len(m.group(1)))
    return out


def _sheet_year(rows: list[tuple]) -> int | None:
    for row in rows[:6]:
        for text in _cells(row):
            m = _YEAR_TITLE.search(text)
            if m:
                return int(m.group(1))
    return None


_EXCEL_EPOCH = date(1899, 12, 30)


def _day(cell: object, year: int, month: int) -> date | None:
    """The peak's date: day of month (``29``), ``MM/DD/YYYY`` or an Excel date (``2016-01-11 00:00:00``)."""
    text = clean_label(cell)
    if re.fullmatch(r"\d{1,2}(\.0+)?", text):
        return date(year, month, int(float(text)))
    if m := re.match(r"^(\d{4})-(\d{2})-(\d{2})", text):
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    if m := re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text):
        y = int(m.group(3))
        return date(y + 2000 if y < 100 else y, int(m.group(1)), int(m.group(2)))
    if re.fullmatch(r"\d{5}(\.0+)?", text):
        return _EXCEL_EPOCH + timedelta(days=int(float(text)))
    return None


def _clock(cell: object) -> int | None:
    """Minutes after midnight of an hour / interval ending: ``0800``, ``8:00``, ``08:00``, ``19`` (hour) or an
    Excel time (``1899-12-31 08:00:00`` or a day fraction)."""
    text = clean_label(cell)
    if m := re.match(r"^\d{4}-\d{2}-\d{2}[ T](\d{2}):(\d{2})", text):
        return int(m.group(1)) * 60 + int(m.group(2))
    if m := re.fullmatch(r"(\d{1,2}):(\d{2})(:\d{2})?", text):
        return int(m.group(1)) * 60 + int(m.group(2))
    if m := re.fullmatch(r"(\d{1,2})(\d{2})", text):
        return int(m.group(1)) * 60 + int(m.group(2))
    if m := re.fullmatch(r"(\d{1,2})(\.0+)?", text):
        return int(m.group(1)) * 60
    value = _number(text)
    if value is not None and 0 < value < 1:
        return round(value * 1440)
    return None


def _at(day: date, minutes: int) -> datetime:
    return datetime(day.year, day.month, day.day) + timedelta(minutes=minutes)


def _day_at(cell: object, year: int, month: int) -> datetime | None:
    """Zone peak time ``"22 @ 18:45"`` (day of month @ interval ending)."""
    m = re.fullmatch(r"(\d{1,2})\s*@\s*(\d{1,2}):?(\d{2})", clean_label(cell))
    if not m:
        return None
    return _at(date(year, month, int(m.group(1))), int(m.group(2)) * 60 + int(m.group(3)))


def _to_utc(local: datetime | None) -> datetime | None:
    if local is None:
        return None
    return local.replace(tzinfo=LOCAL_TZ).astimezone(timezone.utc)


class _SheetParser:
    """Walks one sheet row by row: section titles set the metric, month rows set the columns, labeled rows
    carry the values, and ``Date`` / ``Hour ending`` / ``Date and time`` rows add the peak's time."""

    def __init__(self, f: RawFile, sheet: str, rows: list[tuple], year: int) -> None:
        self.f, self.sheet, self.rows, self.year = f, sheet, rows, year
        self.final = _final_stars(rows, year)
        self.out: list[_Record] = []

    def fail(self, i: int, what: str) -> ValueError:
        return ValueError(f"{self.f.key} [{self.sheet} row {i}]: {what}: {_cells(self.rows[i])[:6]}")

    def values(self, header: _Header, row: tuple, *, region_type: str, region_id: str, metric: str,
               interval: int | None) -> list[_Record]:
        records = []
        for col, month in header.months.items():
            value = _number(row[col]) if col < len(row) else None
            if value is None:
                continue
            records.append(_Record(
                self.year, month, region_type, region_id, metric, value, interval,
                final_settlement=header.stars.get(month, 0) in self.final,
            ))
        self.out.extend(records)
        return records

    def peaks(self, i: int, records: list[_Record], header: _Header, *, dates: tuple | None = None,
              clocks: tuple | None = None, day_at: tuple | None = None) -> None:
        by_month = {r.month: r for r in records}
        for col, month in header.months.items():
            r = by_month.get(month)
            if r is None:
                continue
            if day_at is not None:
                local = _day_at(day_at[col], self.year, month) if day_at[col] is not None else None
            else:
                day = _day(dates[col], self.year, month) if dates and dates[col] is not None else None
                minutes = _clock(clocks[col]) if clocks and clocks[col] is not None else None
                local = _at(day, minutes) if day is not None and minutes is not None else None
            if local is None:
                if r.value == 0:  # e.g. the 2011 DC-tie zones with no load: "@ 00:00", no day
                    continue
                raise self.fail(i, f"unreadable peak time for {_MONTHS[month - 1]}")
            end = local - timedelta(minutes=1)  # 24:00 ends the peak day
            if (end.year, end.month) != (self.year, month):
                raise self.fail(i, f"peak time {local} outside {self.year}-{month:02d}")
            r.peak_local = local

    def system(self, sections: dict[str, tuple[str, int | None]], value_row: str, forecast: dict[str, str]) -> None:
        """``Demand`` and ``Energy`` sheets (region ERCOT)."""
        section = interval = header = None
        current: list[_Record] = []
        dates = None
        for i, row in enumerate(self.rows):
            cells = _cells(row)
            if not cells:
                continue
            if h := _header(row):
                header = h
                continue
            if len(cells) == 1:
                title = cells[0].lower()
                matched = [v for k, v in sections.items() if re.search(k, title)]
                if matched:  # a new section (None: one we skip, e.g. load factors)
                    section, interval = matched[0] or (None, None)
                    header, current = None, []
                    continue
            if header is None or section is None:
                continue
            label = clean_label(row[header.label_col]) if header.label_col >= 0 else ""
            low = label.lower()
            if re.match(rf"^{self.year}\s+{value_row}\b", low):
                current = self.values(header, row, region_type="ercot", region_id="ERCOT", metric=section,
                                      interval=interval)
                dates = None
            elif section in forecast and re.match(r"^forecast(ed)?\s", low):
                self.values(header, row, region_type="ercot", region_id="ERCOT", metric=forecast[section],
                            interval=None)
                current = []
            elif current and re.match(r"^date\b", low):
                dates = row
            elif current and dates is not None and re.match(r"^(hour|interval)\s*ending", low):
                self.peaks(i, current, header, dates=dates, clocks=row)
                current, dates = [], None
            else:
                current, dates = [], None

    def zones(self, region_type: str) -> None:
        section = header = None
        last: list[_Record] = []  # the records of the zone row a "Date and time" row belongs to
        for i, row in enumerate(self.rows):
            cells = _cells(row)
            if not cells:
                continue
            if h := _header(row):
                header, last = h, []
                continue
            if len(cells) == 1 and not cells[0].startswith("*"):
                title = cells[0].lower()
                if re.search(r"coincident\s+with", title):
                    section, header = "coincident_peak_15min_mw", None
                elif re.search(r"non-?\s*coincident|maximum\s+demand", title):
                    section, header = "noncoincident_peak_15min_mw", None
                elif re.search(r"energy", title) and not re.search(r"demand\s+and\s+energy", title):
                    section, header = "energy_mwh", None
                continue
            if header is None or section is None:
                continue
            label = clean_label(row[header.label_col]) if header.label_col >= 0 else ""
            if re.match(r"^date\b", label.lower()):
                if section == "noncoincident_peak_15min_mw" and last:
                    self.peaks(i, last, header, day_at=row)
                continue
            region = _region(region_type, label)
            has_values = any(_number(row[c]) is not None for c in header.months if c < len(row))
            if region is None:
                if has_values:
                    raise self.fail(i, f"unknown {region_type} {label!r}")
                continue
            interval = None if section == "energy_mwh" else 15
            last = self.values(header, row, region_type=region_type, region_id=region, metric=section,
                               interval=interval)


def _region(region_type: str, label: str) -> str | None:
    code = re.sub(r"[^A-Z0-9]+", "_", label.upper()).strip("_")
    if region_type == "weather_zone":
        return _WEATHER_ZONES.get(code)
    if region_type == "load_zone":
        return code if re.fullmatch(r"(LZ|DC)_[A-Z0-9]+", code) else None
    return code if code in _CONGESTION_ZONES else None


_DEMAND_SECTIONS = {
    r"maximum\s+hourly\s+demand": ("peak_hourly_mw", 60),
    r"maximum\s+demand\s+based\s+on\s+15": ("peak_15min_mw", 15),
    r"minimum\s+demand": ("min_15min_mw", 15),
    r"load\s+factor": None,
}
_ENERGY_SECTIONS = {r"net\s+energy\s+for\s+load": ("energy_mwh", None), r"load\s+factor": None}


def _sheet_kind(name: str) -> str | None:
    flat = re.sub(r"[^a-z]", "", name.lower())
    return {
        "demand": "demand", "energy": "energy", "loadzones": "load_zone", "weatherzones": "weather_zone",
        "congestionzones": "congestion_zone",
    }.get(flat)


def parse_demand_energy(f: RawFile) -> pl.DataFrame:
    with f.local_path() as path:
        grids = read_grids(path)
    records: list[_Record] = []
    years = set()
    kinds = set()
    for sheet, grid in grids.items():
        kind = _sheet_kind(sheet)
        if kind is None:
            continue  # contents, disclaimer, fuel mix, charts and comparison sheets
        rows = grid.rows()
        year = _sheet_year(rows)
        if year is None:
            raise ValueError(f"{f.key} [{sheet}]: no report year in the sheet title")
        years.add(year)
        kinds.add(kind)
        p = _SheetParser(f, sheet, rows, year)
        if kind == "demand":
            p.system(_DEMAND_SECTIONS, "demand", {"peak_hourly_mw": "forecast_peak_hourly_mw"})
        elif kind == "energy":
            p.system(_ENERGY_SECTIONS, "energy", {"energy_mwh": "forecast_energy_mwh"})
        else:
            p.zones(kind)
        records.extend(p.out)
    if len(years) != 1 or not {"demand", "energy", "weather_zone"} <= kinds:
        raise ValueError(f"{f.key}: expected Demand, Energy and Weather Zones sheets of one year, got "
                         f"{sorted(kinds)} for {sorted(years)}")
    if not any(r.metric == "peak_hourly_mw" for r in records):
        raise ValueError(f"{f.key}: no hourly peak found")
    return _frame(records)


def _frame(records: list[_Record]) -> pl.DataFrame:
    # coincident zone demand is measured at the ERCOT 15-minute peak
    system_peak = {r.month: r.peak_local for r in records if r.metric == "peak_15min_mw"}
    for r in records:
        if r.metric == "coincident_peak_15min_mw":
            r.peak_local = system_peak.get(r.month)
    df = pl.DataFrame(
        {
            "report_year": [r.report_year for r in records],
            "month": [date(r.report_year, r.month, 1) for r in records],
            "region_type": [r.region_type for r in records],
            "region_id": [r.region_id for r in records],
            "metric": [r.metric for r in records],
            "value": [r.value for r in records],
            "interval_minutes": [r.interval_minutes for r in records],
            "peak_local": [r.peak_local for r in records],
            "peak_ts_utc": [_to_utc(r.peak_local) for r in records],
            "final_settlement": [r.final_settlement for r in records],
        },
        schema=SCHEMA,
    )
    dupes = df.filter(pl.struct(*KEY).is_duplicated())
    if dupes.height:
        raise ValueError(f"{dupes.height} rows share {KEY}, e.g. {dupes.select(KEY).head(3).rows()}")
    return df


def _report_year(f: RawFile) -> int | None:
    for text in (f.meta.get("link_text") or "", f.name):
        m = re.search(r"(?<!\d)(20\d{2})(?!\d)", text)
        if m:
            return int(m.group(1))
    return None


def _uploaded(f: RawFile) -> date:
    """Upload date from the ercot.com path (``/files/docs/YYYY/MM/DD/``), else the snapshot date."""
    m = re.search(r"/files/docs/(\d{4})/(\d{2})/(\d{2})/", f.url)
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else f.dt


def newest_per_year(files: list[RawFile]) -> list[RawFile]:
    """The newest upload of each report year (drops the "through November" and superseded workbooks); the
    current-year file, overwritten at the same URL, resolves to its latest snapshot."""
    newest: dict[object, RawFile] = {}
    for f in sorted(files, key=lambda f: (_uploaded(f), f.dt, f.entry.fetched_at, f.key)):
        newest[_report_year(f) or f.url] = f
    return sorted(newest.values(), key=lambda f: (f.dt, f.entry.fetched_at, f.key))


DATASETS = [
    Dataset(
        name="ercot_monthly_peaks",
        target="postgres",
        mode="by_key",
        key=KEY,
        description=(
            "ERCOT Demand and Energy report, 2008+: monthly system peaks (hourly and 15-minute, with local and "
            "UTC time), minimum demand, energy and their forecasts, plus coincident / non-coincident peaks and "
            "energy by load zone (congestion zone before 2011) and weather zone; one row per month, region and "
            "metric."
        ),
        parse=parse_demand_energy,
        inputs=lambda f: f.suffix in {".xls", ".xlsx"},
        select=newest_per_year,
    ),
]
