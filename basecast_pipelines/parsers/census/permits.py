"""Census Building Permits Survey, county files → ``census_permits_county`` (Texas only).

One raw file per period: annual ``coYYYYa.txt`` (1990 onward) and monthly ``coYYMMc.txt`` (2000 onward).
Each is a national comma-separated file with two header lines (a group line: ``1-unit``, ``2-units``,
``3-4 units``, ``5+ units`` and the same four with ``rep``; then ``Bldgs``/``Units``/``Value`` under each
group) whose spelling changed in 1999 (``34unit``, ``5-unit``). The header is read by content, not position.

Each size group has two blocks: the first includes imputation for places that did not report, the ``rep``
block is what was reported (the first is always ≥ the second; wording from the BPS county layout, not
verified against a layout document in raw). Columns are ``{bldgs,units,value}_{size}`` for the imputed
estimate and the same with ``_rep`` for reported only; ``value`` is construction cost in dollars.

Monthly county files before 2022 hold only the counties whose permit offices are in the monthly sample
(~33–70 of Texas's counties); from 2022 they cover every county with permit-issuing places (~220). Counties
without permit-issuing places never appear, so Texas has ~221–229 counties per year, not 254.

Survey dates: ``9099`` for 1990 (``YY99``) up to 1998, ``YYYY`` for annual files since 1999, ``YYYYMM`` for
monthly files. The period comes from the manifest (``kind``/``year``/``month``) or the file name and is
checked against the survey date."""

from __future__ import annotations

import csv
import io
import re

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url

SOURCE_ID = "census_bps"
TEXAS = "48"

_ANNUAL = re.compile(r"^co(\d{4})a(?:__[0-9a-f]{8})?\.txt$", re.IGNORECASE)
_MONTHLY = re.compile(r"^co(\d{2})(\d{2})c(?:__[0-9a-f]{8})?\.txt$", re.IGNORECASE)
_SIZES = (("1_unit", r"^1"), ("2_units", r"^2"), ("3_4_units", r"^3"), ("5_plus_units", r"^5"))
_METRICS = ("bldgs", "units", "value")
_ID_COLUMNS = {  # header text (group line + second line, lower case, no spaces) → column
    "surveydate": "survey_date",
    "fipsstate": "state_fips",
    "fipscounty": "county_code",
    "regioncode": "region_code",
    "divisioncode": "division_code",
    "countyname": "county_name",
}


def _period(f: RawFile) -> tuple[str, int, int | None]:
    """(period_type, year, month) from the manifest, falling back to the file name."""
    kind = f.meta.get("kind")
    if kind == "annual" and f.meta.get("year"):
        return "annual", int(f.meta["year"]), None
    if kind == "monthly" and f.meta.get("year") and f.meta.get("month"):
        return "monthly", int(f.meta["year"]), int(f.meta["month"])
    if m := _ANNUAL.match(f.name):
        return "annual", int(m.group(1)), None
    if m := _MONTHLY.match(f.name):
        return "monthly", 2000 + int(m.group(1)), int(m.group(2))
    raise ValueError(f"{f.key}: cannot tell the period (no manifest kind, unknown file name)")


def _size(label: str) -> str | None:
    text = label.strip().lower().replace(" ", "")
    for name, pattern in _SIZES:
        if re.match(pattern, text):
            return name
    return None


def _columns(group: list[str], sub: list[str]) -> list[str | None]:
    """Column names from the two header lines. The group label sits over the ``Units`` cell of its
    ``Bldgs, Units, Value`` triple, so each triple takes the label found at any of its three positions."""
    width = max(len(group), len(sub))
    group = group + [""] * (width - len(group))
    sub = sub + [""] * (width - len(sub))
    names: list[str | None] = [None] * width
    for j in range(width):
        key = re.sub(r"\s+", "", f"{group[j]}{sub[j]}").lower()
        if key in _ID_COLUMNS:
            names[j] = _ID_COLUMNS[key]
    for j in range(width - 2):
        if [s.strip().lower() for s in sub[j:j + 3]] != ["bldgs", "units", "value"]:
            continue
        label = next((g for g in group[j:j + 3] if g.strip()), "")
        size = _size(label)
        if size is None:
            raise ValueError(f"unknown size group {label!r} in the header")
        suffix = "_rep" if "rep" in label.lower() else ""
        for k, metric in enumerate(_METRICS):
            names[j + k] = f"{metric}_{size}{suffix}"
    return names


def _has_triple(row: list[str]) -> bool:
    cells = [c.strip().lower() for c in row]
    return any(cells[j:j + 3] == list(_METRICS) for j in range(len(cells) - 2))


def _total(columns: list[str]) -> pl.Expr:
    """Sum of the size groups; null when every group is null (a blank reported block)."""
    return pl.when(pl.all_horizontal(pl.col(columns).is_null())).then(None).otherwise(pl.sum_horizontal(columns))


def parse_permits(f: RawFile) -> pl.DataFrame | None:
    period_type, year, month = _period(f)
    text = f.read_bytes().decode("latin-1")
    rows = list(csv.reader(io.StringIO(text)))
    header = next((i for i, r in enumerate(rows[:10]) if _has_triple(r)), None)
    if header is None or header == 0:
        raise ValueError(f"{f.key}: no Bldgs/Units/Value header line")
    names = _columns(rows[header - 1], rows[header])
    expected = {f"{m}_{s}{r}" for m in _METRICS for s, _ in _SIZES for r in ("", "_rep")} | set(_ID_COLUMNS.values())
    missing = expected - set(n for n in names if n)
    if missing:
        raise ValueError(f"{f.key}: header lacks {sorted(missing)}")

    named = [(j, n) for j, n in enumerate(names) if n]
    width = named[-1][0] + 1
    state = names.index("state_fips")
    records = []
    for r in rows[header + 1:]:
        if len(r) < width or r[state].strip().zfill(2) != TEXAS:
            continue
        records.append({n: r[j].strip() for j, n in named})
    if not records:
        raise ValueError(f"{f.key}: no Texas rows")
    raw = pl.DataFrame(records, schema={n: pl.String for n in names if n})

    survey = raw["survey_date"].unique().to_list()
    allowed = {f"{year}{month:02d}"} if month else {str(year), f"{year % 100:02d}99"}
    if not set(survey) <= allowed:
        raise ValueError(f"{f.key}: survey dates {survey} do not match {period_type} {year}-{month}")

    counts = [f"{m}_{s}{r}" for r in ("", "_rep") for s, _ in _SIZES for m in _METRICS]
    df = raw.select(
        pl.lit(period_type).alias("period_type"),
        pl.lit(year, pl.Int32).alias("year"),
        pl.lit(month, pl.Int32).alias("month"),
        pl.date(year, month or 1, 1).alias("period_start"),
        (pl.lit(TEXAS) + pl.col("county_code").str.zfill(3)).alias("county_fips"),
        pl.col("county_name").str.strip_chars(),
        pl.col("region_code").cast(pl.Int32, strict=False),
        pl.col("division_code").cast(pl.Int32, strict=False),
        # blank cells (the reported block of some 2000-2002 monthly rows) are null; anything else must be a number
        *[pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).cast(pl.Int64).alias(c) for c in counts],
    )
    sizes = [s for s, _ in _SIZES]
    return df.with_columns(
        _total([f"bldgs_{s}" for s in sizes]).alias("bldgs_total"),
        _total([f"units_{s}" for s in sizes]).alias("units_total"),
        _total([f"value_{s}" for s in sizes]).alias("value_total"),
        _total([f"units_{s}_rep" for s in sizes]).alias("units_total_rep"),
    )


DATASETS = [
    Dataset(
        name="census_permits_county",
        target="postgres",
        mode="by_file",
        description=(
            "Census Building Permits Survey by Texas county, annual (1990→) and monthly (2000→): buildings, "
            "units and construction value by structure size (1, 2, 3–4, 5+ units), imputed estimate and "
            "reported-only (_rep)."
        ),
        parse=parse_permits,
        inputs=lambda f: bool(_ANNUAL.match(f.name) or _MONTHLY.match(f.name)),
        select=latest_by_url,
    ),
]
