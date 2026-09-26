"""Peak and energy tables of the LTLF workbooks, read into the common forecast shape.

ERCOT has published these small workbooks since 2013 with a new layout almost every year, so nothing here
assumes a position. Each sheet is read as a grid of strings and scanned for four kinds of table:

- **monthly** (header ``Year | Month | Peak | Energy``; 2025 puts the ERCOT Adjusted and TSP Provided tables
  side by side, and the TSP table's ``year | month`` labels sit one row below its value header, so labels and
  values are paired by position);
- **weekly P90 peaks** (``Begin_Date | End_Date | Peak_Date | Peak_Hour | <zones> | ERCOT``);
- **winter RS peak** (``Forecasted Peak Date | Forecasted Peak Hour | ERCOT ... | <transmission operators>``);
- **year tables**: a column of consecutive target years (``2025`` or ``2025-2026`` for winters) with zones,
  weather years or percentiles across. The table's title (the text lines above its header) says what it
  holds: coincident or non-coincident peak, gross or net, rooftop PV impact, energy, 90th percentile,
  TSP Provided or ERCOT Adjusted. Tables titled ``Historical`` (actual peaks) are skipped.

Conventions of the output (documented once here, used by ``ltlf.py``):

- ``metric``: ``peak_mw`` (coincident peak: the system peak for ERCOT, the zone's load at the system peak
  for a weather zone), ``ncp_mw`` (a zone's own, non-coincident peak), ``ncp_zone_sum_mw`` (the ERCOT or
  "Total NCP" column of a non-coincident table: the sum of the zones' own peaks, not a system peak),
  ``rooftop_pv_mw`` / ``rooftop_pv_ncp_mw`` / ``rooftop_pv_ncp_zone_sum_mw`` (behind-the-meter PV impact,
  negative), ``energy_mwh``, ``large_load_{contracts,officer_letters,total,additions}_mw``. A ``gross_``
  prefix marks peaks before the rooftop PV adjustment; unprefixed peaks are net (or unspecified, before
  2021).
- ``scenario``: the forecast variant (``base`` for the single forecast of a vintage, ``ercot_adjusted`` or
  ``tsp_provided`` in 2025), plus ``_p50`` / ``_p90`` / ``_p75`` / ``_weather_year_<YYYY>`` when the table
  or column is a weather scenario, plus ``_no_large_loads`` for loads without the large-load additions.
- ``season``: ``summer``, ``winter``, ``annual`` (no season in the title), ``month`` (``target_month`` set)
  or ``week`` (weekly P90 peaks, keyed by ``row_label``). Winters are labelled by the year they start in
  (``2025-2026`` → ``target_year`` 2025).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime

WEATHER_ZONES = ("COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST")

_REGION_ALIASES = {
    "coast": "COAST",
    "east": "EAST",
    "fwest": "FWEST",
    "far west": "FWEST",
    "ncent": "NCENT",
    "north central": "NCENT",
    "north": "NORTH",
    "scent": "SCENT",
    "south central": "SCENT",
    "south": "SOUTH",
    "west": "WEST",
    "ercot": "ERCOT",
}
_ZONE_SUM_LABELS = {"total ncp", "total pv", "total"}
_P50_LABELS = {"p50", "forecast", "official forecast 50/50"}
_P90_LABELS = {"p90", "90th percentile", "ercot 90th"}
_LARGE_LOAD_LABELS = {
    "contracts": "large_load_contracts_mw",
    "officer letters": "large_load_officer_letters_mw",
    "total large loads": "large_load_total_mw",
}

_YEAR = re.compile(r"^(\d{4})(?:\.0+)?$")
_YEAR_RANGE = re.compile(r"^(\d{4})\s*[-–/]\s*(?:\d{2}|\d{4})$")
_NUMBER = re.compile(r"^\(?-?[\d,]*\.?\d+(?:[eE][-+]?\d+)?\)?$")


@dataclass
class TableRow:
    target_year: int
    target_month: int | None
    season: str
    region_type: str
    region_id: str
    metric: str
    scenario: str
    value: float
    unit: str
    sheet: str
    row_label: str
    column_label: str


@dataclass
class SheetContext:
    """What the file says about itself: the variant named in its link text or section, if any."""

    sheet: str
    variant: str | None = None
    weather_hint: str | None = None  # e.g. "p90" from "Weekly P90 Peaks"
    workbook_text: str = ""
    rows: list[TableRow] = field(default_factory=list)


# --- small helpers ------------------------------------------------------------------------------------------


def norm(label: object) -> str:
    return re.sub(r"[\s_]+", " ", str(label or "")).strip().lower()


def to_float(text: str | None) -> float | None:
    if text is None:
        return None
    s = text.strip().replace(",", "")
    if not s or not _NUMBER.match(s):
        return None
    negative = s.startswith("(") and s.endswith(")")
    value = float(s.strip("()"))
    return -value if negative else value


def is_number(text: str | None) -> bool:
    return to_float(text) is not None


def year_key(text: str | None) -> int | None:
    """``2025`` → 2025 and ``2025-2026`` (a winter) → 2025; anything else → None."""
    if text is None:
        return None
    m = _YEAR.match(text.strip()) or _YEAR_RANGE.match(text.strip())
    if m and 1990 <= int(m.group(1)) <= 2100:
        return int(m.group(1))
    return None


def variant_of(text: str) -> str | None:
    t = norm(text)
    if "tsp provided" in t or "tsp-provided" in t:
        return "tsp_provided"
    if "ercot adjusted" in t:
        return "ercot_adjusted"
    return None


def region_of(label: str) -> str | None:
    return _REGION_ALIASES.get(norm(label))


def scenario(variant: str | None, *parts: str | None) -> str:
    return "_".join([variant or "base", *(p for p in parts if p)])


def parse_date(text: str | None) -> date | None:
    """ISO (``2024-08-18 00:00:00``, what fastexcel gives for date cells), ``04May2025`` or ``8/18/2024``."""
    if not text:
        return None
    s = text.strip()
    for fmt, cut in (("%Y-%m-%d", 10), ("%d%b%Y", None), ("%m/%d/%Y", None)):
        try:
            return datetime.strptime(s[:cut] if cut else s, fmt).date()
        except ValueError:
            continue
    serial = to_float(s)
    if serial is not None and 1 <= serial <= 80000:
        from basecast_pipelines.processing.tabular import excel_serial_to_date

        return excel_serial_to_date(serial)
    return None


def _clean(rows: Iterable[Sequence[object]]) -> list[list[str | None]]:
    out = []
    for row in rows:
        out.append([(re.sub(r"\s+", " ", str(v)).strip() or None) if v is not None else None for v in row])
    return out


def _blank(row: Sequence[str | None]) -> bool:
    return all(v is None for v in row)


def _find_row(rows: list[list[str | None]], patterns: Sequence[str], max_scan: int = 20) -> int | None:
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    for i, row in enumerate(rows[:max_scan]):
        cells = [c for c in row if c]
        if all(any(p.search(c) for c in cells) for p in compiled):
            return i
    return None


# --- monthly tables -----------------------------------------------------------------------------------------

_PEAK_HEADER = re.compile(r"^(monthly )?peaks?\b", re.IGNORECASE)
_ENERGY_HEADER = re.compile(r"^(annual |monthly )?energy\b", re.IGNORECASE)


def _monthly(ctx: SheetContext, rows: list[list[str | None]]) -> bool:
    """Year | Month | Peak | Energy tables (one or several side by side). Returns False when absent."""
    scan = rows[:15]
    pairs = [
        (r, c)
        for r, row in enumerate(scan)
        for c in range(len(row) - 1)
        if norm(row[c]) == "year" and norm(row[c + 1]) == "month"
    ]
    values = [
        (r, c, cell)
        for r, row in enumerate(scan)
        for c, cell in enumerate(row)
        if cell and (_PEAK_HEADER.match(cell) or _ENERGY_HEADER.match(cell))
    ]
    if not pairs or not values:
        return False
    for vr, vc, label in values:
        left = [(r, c) for r, c in pairs if c < vc]
        if not left:
            continue
        yr, yc = max(left, key=lambda p: p[1])
        labels = []
        for row in rows[yr + 1:]:
            y, m = year_key(row[yc]), to_float(row[yc + 1])
            if y is None or m is None or not 1 <= m <= 12:
                break
            labels.append((y, int(m), f"{row[yc]}-{row[yc + 1]}"))
        title = _nearest_title(rows, above=min(vr, yr), col=vc)
        variant = variant_of(title or "") or ctx.variant
        is_energy = bool(_ENERGY_HEADER.match(label))
        for i, (y, m, row_label) in enumerate(labels):
            r = vr + 1 + i
            if r >= len(rows):
                break
            value = to_float(rows[r][vc])
            if value is None:
                continue
            ctx.rows.append(
                TableRow(
                    target_year=y, target_month=m, season="month", region_type="ercot", region_id="ERCOT",
                    metric="energy_mwh" if is_energy else "peak_mw", scenario=scenario(variant),
                    value=value, unit="MWh" if is_energy else "MW", sheet=ctx.sheet, row_label=row_label,
                    column_label=_join(title, label),
                )
            )
    return True


def _nearest_title(rows: list[list[str | None]], *, above: int, col: int) -> str | None:
    """The text cell nearest to the left of (or at) ``col`` in the rows above ``above``."""
    best = None
    for r in range(max(0, above - 4), above):
        for c, v in enumerate(rows[r]):
            if v and not is_number(v) and c <= col and (best is None or c >= best[0]):
                best = (c, v)
    return best[1] if best else None


def _join(title: str | None, label: str) -> str:
    return f"{title} | {label}" if title else label


# --- weekly P90 peaks ---------------------------------------------------------------------------------------


def _weekly(ctx: SheetContext, rows: list[list[str | None]]) -> bool:
    h = _find_row(rows, [r"^begin.?date$", r"^peak.?date$", r"^peak.?hour$"])
    if h is None:
        return False
    header = rows[h]
    col = {norm(v).replace(" ", "_"): i for i, v in enumerate(header) if v}
    begin, end, peak, hour = (col.get(k) for k in ("begin_date", "end_date", "peak_date", "peak_hour"))
    if begin is None or peak is None or hour is None:
        raise ValueError(f"{ctx.sheet}: weekly peak table without Begin_Date / Peak_Date / Peak_Hour")
    regions = [(i, region, v) for i, v in enumerate(header) if v and (region := region_of(v))]
    if not regions:
        raise ValueError(f"{ctx.sheet}: weekly peak table without zone columns")
    weather = ctx.weather_hint
    for row in rows[h + 1:]:
        if _blank(row):
            break
        d_begin, d_peak = parse_date(row[begin]), parse_date(row[peak])
        he = to_float(row[hour])
        if d_peak is None or he is None:
            continue
        d_end = parse_date(row[end]) if end is not None else None
        label = f"week {d_begin}..{d_end}, peak {d_peak} HE{int(he)}"
        for i, region, header_label in regions:
            value = to_float(row[i])
            if value is None:
                continue
            ctx.rows.append(
                TableRow(
                    target_year=d_peak.year, target_month=d_peak.month, season="week",
                    region_type="ercot" if region == "ERCOT" else "weather_zone", region_id=region,
                    metric="peak_mw", scenario=scenario(ctx.variant, weather), value=value, unit="MW",
                    sheet=ctx.sheet, row_label=label, column_label=header_label,
                )
            )
    return True


# --- winter reliability-standard peak (ERCOT and transmission operators) ------------------------------------


def _rs_peak(ctx: SheetContext, rows: list[list[str | None]]) -> bool:
    h = _find_row(rows, [r"forecasted peak date", r"forecasted peak hour"])
    if h is None:
        return False
    header = rows[h]
    date_col = next(i for i, v in enumerate(header) if v and re.search(r"peak date", v, re.I))
    hour_col = next(i for i, v in enumerate(header) if v and re.search(r"peak hour", v, re.I))
    weather = percentile_hint(ctx.workbook_text)
    for row in rows[h + 1:]:
        if _blank(row):
            break
        peak_day, he = parse_date(row[date_col]), to_float(row[hour_col])
        if peak_day is None or he is None:
            continue
        season, target_year = _season_of(peak_day)
        classified = classify_rs_columns(header, skip={date_col, hour_col})
        with_ll = next((i for i, kind, _ in classified if kind == "with_ll"), None)
        no_ll = next((i for i, kind, _ in classified if kind == "no_ll"), None)
        tos = [(i, name) for i, kind, name in classified if kind == "to"]
        to_suffix = to_share_basis(
            sum(to_float(row[i]) or 0.0 for i, _ in tos),
            to_float(row[with_ll]) if with_ll is not None else None,
            to_float(row[no_ll]) if no_ll is not None else None,
        )
        label = f"peak {peak_day} HE{int(he)}"
        for i, kind, name in classified:
            value = to_float(row[i])
            if value is None:
                continue
            if kind == "to":
                region_type, region_id, metric, suffix = "transmission_operator", name, "peak_mw", to_suffix
            elif kind == "additions":
                region_type, region_id, metric, suffix = "ercot", "ERCOT", "large_load_additions_mw", None
            else:
                region_type, region_id, metric = "ercot", "ERCOT", "peak_mw"
                suffix = "no_large_loads" if kind == "no_ll" else None
            ctx.rows.append(
                TableRow(
                    target_year=target_year, target_month=None, season=season, region_type=region_type,
                    region_id=region_id, metric=metric, scenario=scenario(ctx.variant, weather, suffix),
                    value=value, unit="MW", sheet=ctx.sheet, row_label=label, column_label=header[i] or "",
                )
            )
    return True


def classify_rs_columns(header: Sequence[str | None], *, skip: set[int]) -> list[tuple[int, str, str]]:
    """(column, kind, name) for the value columns of the winter RS sheets: ERCOT with or without large loads,
    large-load additions, or a transmission operator (named as in the header)."""
    out = []
    for i, v in enumerate(header):
        if i in skip or not v:
            continue
        t = norm(v)
        if t in {"date", "year", "month", "day", "hour"}:
            continue
        if "large load additions" in t:
            out.append((i, "additions", v))
        elif "ercot" in t and re.search(r"\bno (large loads|lls?)\b", t):
            out.append((i, "no_ll", v))
        elif "ercot" in t and re.search(r"\bwith (large loads|lls?)\b", t):
            out.append((i, "with_ll", v))
        else:
            out.append((i, "to", v.rstrip("*").strip()))
    return out


def to_share_basis(to_sum: float, with_ll: float | None, no_ll: float | None) -> str | None:
    """The operators' values are shares of one ERCOT total; say which (checked, not assumed)."""
    if no_ll and abs(to_sum - no_ll) <= 0.01 * no_ll:
        return "no_large_loads"
    if with_ll and abs(to_sum - with_ll) <= 0.01 * with_ll:
        return None
    raise ValueError(f"transmission operator values sum to {to_sum:,.0f} MW, matching no ERCOT total")


def percentile_hint(text: str) -> str | None:
    m = re.search(r"(\d{2})th percentile", text, re.IGNORECASE)
    return f"p{m.group(1)}" if m else None


def _season_of(day: date) -> tuple[str, int]:
    if day.month in (12, 1, 2):
        return "winter", day.year if day.month == 12 else day.year - 1
    if 6 <= day.month <= 9:
        return "summer", day.year
    return "annual", day.year


# --- year tables --------------------------------------------------------------------------------------------


@dataclass
class _Run:
    col: int
    rows: list[int]
    years: list[int]


def _runs(rows: list[list[str | None]]) -> list[_Run]:
    """Columns holding consecutive target years (blank rows allowed in between), at least two long."""
    width = max((len(r) for r in rows), default=0)
    found: list[_Run] = []
    for c in range(width):
        cur: _Run | None = None
        for r, row in enumerate(rows):
            cell = row[c] if c < len(row) else None
            y = year_key(cell)
            if y is not None and any(is_number(v) for v in row[c + 1:]):
                if cur and y == cur.years[-1] + 1:
                    cur.rows.append(r)
                    cur.years.append(y)
                    continue
                if cur and len(cur.rows) >= 2:
                    found.append(cur)
                cur = _Run(c, [r], [y])
            elif _blank(row):
                continue
            else:
                if cur and len(cur.rows) >= 2:
                    found.append(cur)
                cur = None
        if cur and len(cur.rows) >= 2:
            found.append(cur)
    return found


@dataclass
class _Kind:
    kind: str  # cp | ncp | pv | energy
    season: str
    gross: bool
    weather: str | None
    variant: str | None
    energy_factor: float
    pv_context: str | None = None


def _classify(title_lines: list[str], previous: _Kind | None) -> _Kind | None:
    t = norm(" ".join(title_lines))
    season = "winter" if "winter" in t else "summer" if "summer" in t else "annual"
    ncp = re.search(r"non[- ]?coincident", t) is not None
    if "energy" in t:
        kind = "energy"
    elif "rooftop pv" in t or re.search(r"\bpv\b", t):
        kind = "pv"
    elif ncp:
        kind = "ncp"
    elif "coincident" in t or "peak" in t:
        kind = "cp"
    else:
        return None
    pv_context = None
    if kind == "pv":
        pv_context = "ncp" if ncp else "cp" if "coincident" in t else (previous.kind if previous else "cp")
        pv_context = "ncp" if pv_context == "ncp" else "cp"
    factor = 1_000.0 if "(gwh)" in t else 1_000_000.0 if "(twh)" in t else 1.0
    return _Kind(
        kind=kind, season=season, gross="gross" in t, weather="p90" if "90th percentile" in t else None,
        variant=variant_of(t), energy_factor=factor, pv_context=pv_context,
    )


def _metric(k: _Kind, zone_sum: bool) -> str:
    if k.kind == "energy":
        return "energy_mwh"
    if k.kind == "pv":
        base = "rooftop_pv_ncp" if k.pv_context == "ncp" else "rooftop_pv"
        return f"{base}_zone_sum_mw" if zone_sum and k.pv_context == "ncp" else f"{base}_mw"
    base = ("ncp_zone_sum" if zone_sum else "ncp") if k.kind == "ncp" else "peak"
    return f"{'gross_' if k.gross else ''}{base}_mw"


def _column(label: str, k: _Kind) -> tuple[str, str, str | None, str | None]:
    """(region_type, region_id, weather, metric override) for one column header of a year table."""
    t = norm(label)
    region = region_of(label)
    if region is not None:
        zone_sum = region == "ERCOT" and (k.kind == "ncp" or k.pv_context == "ncp")
        return ("ercot" if region == "ERCOT" else "weather_zone"), region, None, (
            _metric(k, zone_sum=True) if zone_sum else None
        )
    if t in _ZONE_SUM_LABELS:
        return "ercot", "ERCOT", None, _metric(k, zone_sum=True)
    if re.fullmatch(r"\d{4}", t):
        return "ercot", "ERCOT", f"weather_year_{t}", None
    if t in _P50_LABELS:
        return "ercot", "ERCOT", "p50", None
    if t in _P90_LABELS:
        return "ercot", "ERCOT", "p90", None
    if t in _LARGE_LOAD_LABELS:
        return "ercot", "ERCOT", None, _LARGE_LOAD_LABELS[t]
    raise ValueError(f"unknown column {label!r} in a year table")


def _year_tables(ctx: SheetContext, rows: list[list[str | None]], vintage_year: int) -> None:
    runs = _runs(rows)
    previous: _Kind | None = None
    for run in sorted(runs, key=lambda x: (x.rows[0], x.col)):
        h = run.rows[0] - 1
        while h >= 0 and _blank(rows[h]) and run.rows[0] - h <= 3:
            h -= 1
        if h < 0:
            continue
        # value columns run to the next table's year column on the same rows (side-by-side tables)
        limit = min(
            [o.col for o in runs if o.col > run.col and set(o.rows) & set(run.rows)] or [len(rows[h])]
        )
        # header cells over at least one number (a stray "\" beside the 2024 winter table has none)
        columns = [
            (c, label) for c in range(run.col + 1, min(limit, len(rows[h])))
            if (label := rows[h][c]) and any(is_number(rows[r][c]) for r in run.rows)
        ]
        if not columns:
            continue
        title = _title_lines(rows, header=h, first=run.col, last=limit)
        if any(line.strip().lower() == "historical" for line in title):
            continue  # actual (historical) peaks, not a forecast
        kind = _classify(title, previous)
        if kind is None:
            raise ValueError(f"{ctx.sheet}: cannot tell what the table titled {title!r} holds")
        previous = kind if kind.kind != "pv" else previous
        variant = kind.variant or variant_of(ctx.sheet) or ctx.variant
        for c, label in columns:
            region_type, region_id, column_weather, metric = _column(label, kind)
            metric = metric or _metric(kind, zone_sum=False)
            weather = column_weather or kind.weather or ctx.weather_hint
            for r, y in zip(run.rows, run.years):
                if y < vintage_year:
                    continue  # history printed next to the forecast (e.g. the 2013 H/F table)
                value = to_float(rows[r][c]) if c < len(rows[r]) else None
                if value is None:
                    continue
                if metric == "energy_mwh":
                    value *= kind.energy_factor
                ctx.rows.append(
                    TableRow(
                        target_year=y, target_month=None,
                        season=kind.season if kind.kind != "energy" else "annual",
                        region_type=region_type, region_id=region_id, metric=metric,
                        scenario=scenario(variant, weather), value=value,
                        unit="MWh" if metric == "energy_mwh" else "MW", sheet=ctx.sheet,
                        row_label=rows[r][run.col] or str(y), column_label=_join(" | ".join(title), label),
                    )
                )


def _title_lines(rows: list[list[str | None]], *, header: int, first: int, last: int) -> list[str]:
    """Text lines above a header row, within the table's columns, up to the previous table's data."""
    lines: list[str] = []
    r = header - 1
    while r >= 0 and header - r <= 5:
        row = rows[r]
        cells = [v for v in row[first:last] if v]
        if sum(is_number(v) for v in row) >= 2:
            break
        text = [v for v in cells if not is_number(v)]
        if text:
            lines.insert(0, " ".join(text))
        r -= 1
    return lines


# --- entry point --------------------------------------------------------------------------------------------


def is_hourly_header(rows: list[list[str | None]]) -> bool:
    return _find_row(rows, [r"^year$", r"^month$", r"^day$", r"^hour$"], max_scan=10) is not None


def parse_sheet(ctx: SheetContext, raw_rows: Iterable[Sequence[object]], vintage_year: int) -> list[TableRow]:
    rows = _clean(raw_rows)
    width = max((len(r) for r in rows), default=0)
    rows = [r + [None] * (width - len(r)) for r in rows]
    if not rows or is_hourly_header(rows):
        return ctx.rows  # hourly sheets belong to ltlf_hourly
    if _rs_peak(ctx, rows) or _weekly(ctx, rows) or _monthly(ctx, rows):
        return ctx.rows
    _year_tables(ctx, rows, vintage_year)
    return ctx.rows
