"""EIA-861 short form (861S) for the account universe (exploration X4, analysis only).

Since 2020 (and before 2019), EIA collects small utilities on the short form: one row per utility and state in
``Short_Form_<year>.xlsx`` inside the annual EIA-861 zip, with total revenue, sales and customers (no sector
split) and four program flags. The ``eia_861`` parser does not load it yet, so the account signals built on
``eia861_sales`` miss most munis. This module reads it straight from the raw zips and combines it with the long
form, so ``analysis/x4_eia861_short_form.py`` can measure what the parser change would bring.

- :func:`parse_short_form` reads one workbook. Headers are found by what they say (the layout moves: no
  ``Ownership`` before 2015, ``BA_CODE`` vs ``BA Code``, ``w_heater`` in 2022, a leading note column in the early
  release). Output: typed columns, one row per utility and state, shaped like a future ``eia861_short_form``
  table (see :data:`SHORT_FORM_SCHEMA`).
- :func:`long_form_totals` and :func:`combine_forms` build one row per utility-year (final or early release)
  from both forms; when a utility-year sits in both, the long form wins and the row is flagged.
- :func:`utility_signals` turns that into the size, growth and price signals per EIA utility.

The ``load_*`` functions only read (the raw lake and, through ``models.db``, the database).
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from basecast_pipelines.parsers.eia._common import clean_strings, id_text, type_columns, zip_member
from basecast_pipelines.processing.tabular import clean_label, find_header_row, read_grid, sheet_names

SOURCE_ID = "eia_861"
ZIP_NAME = re.compile(r"^f861(\d{4})(er)?\.zip$", re.IGNORECASE)
SHORT_FORM_MEMBER = r"^Short_Form_\d{4}\w*\.xlsx?$"
DELIVERY_MEMBER = r"^Delivery_Companies_\d{4}\w*\.xlsx?$"

# Target column → regex on the cleaned header label (first match wins; order matters for "sales" vs "customers").
_COLUMNS: dict[str, str] = {
    "data_year": r"^data year$",
    "utility_id": r"^utility number$",
    "utility_name": r"^utility name$",
    "ownership": r"^ownership$",
    "state": r"^state$",
    "ba_code": r"^ba[ _]?code$",
    "revenue_thousand_usd": r"revenue",
    "sales_mwh": r"sales",
    "customers": r"customers",
    "net_metering": r"^net metering$",
    "demand_side_management": r"^demand side management$",
    "time_based_programs": r"^time based programs$",
}
_REQUIRED = ("utility_id", "utility_name", "state", "revenue_thousand_usd", "sales_mwh", "customers")
_FLAGS = ("net_metering", "demand_side_management", "time_based_programs")

SHORT_FORM_SCHEMA: dict[str, pl.DataType] = {
    "data_year": pl.Int64(),
    "early_release": pl.Boolean(),
    "utility_id": pl.String(),
    "utility_name": pl.String(),
    "ownership": pl.String(),
    "state": pl.String(),
    "ba_code": pl.String(),
    "revenue_thousand_usd": pl.Float64(),
    "sales_mwh": pl.Float64(),
    "customers": pl.Int64(),
    "net_metering": pl.Boolean(),
    "demand_side_management": pl.Boolean(),
    "time_based_programs": pl.Boolean(),
}

TOTALS_SCHEMA: dict[str, pl.DataType] = {
    "utility_id": pl.String(),
    "data_year": pl.Int64(),
    "early_release": pl.Boolean(),
    "customers": pl.Float64(),
    "sales_mwh": pl.Float64(),
    "revenue_thousand_usd": pl.Float64(),
}


def release_of(zip_name: str) -> tuple[int, bool]:
    """(data year, early release) from an EIA-861 zip name (``f8612024.zip``, ``f8612025er.zip``)."""
    match = ZIP_NAME.match(zip_name)
    if not match:
        raise ValueError(f"{zip_name}: not an EIA-861 zip name")
    return int(match.group(1)), bool(match.group(2))


def _column_map(labels: list[str]) -> dict[str, int]:
    """Target column → index in the header row. Each index is used once."""
    found: dict[str, int] = {}
    for target, pattern in _COLUMNS.items():
        rx = re.compile(pattern, re.IGNORECASE)
        for i, label in enumerate(labels):
            if i not in found.values() and label and rx.search(label):
                found[target] = i
                break
    missing = [c for c in _REQUIRED if c not in found]
    if missing:
        raise ValueError(f"short form: missing columns {missing} in header {labels}")
    return found


def parse_short_form(
    book: bytes | Path, *, data_year: int, early_release: bool, states: tuple[str, ...] | None = ("TX",)
) -> pl.DataFrame:
    """One ``Short_Form_<year>`` workbook → typed rows (:data:`SHORT_FORM_SCHEMA`), one per utility and state.

    ``'.'`` and blanks become null (EIA's missing marker). ``states=None`` keeps every state. Raises when a
    numeric column holds text, so a layout change fails loudly.
    """
    frames = []
    for sheet in sheet_names(book):
        grid = read_grid(book, sheet)
        header = find_header_row(grid, [r"utility number", r"customers"])
        if header is None:
            continue
        cols = _column_map([clean_label(c) for c in grid.row(header)])
        body = grid.slice(header + 1).select(pl.nth(i).alias(t) for t, i in cols.items())
        body = clean_strings(body).filter(pl.col("utility_id").is_not_null())
        if states is not None:
            body = body.filter(pl.col("state").is_in(list(states)))
        body = type_columns(
            body,
            floats=["revenue_thousand_usd", "sales_mwh"],
            ints=["customers", "data_year"],
            flags=[f for f in _FLAGS if f in body.columns],
        )
        frames.append(
            body.select(
                (pl.col("data_year") if "data_year" in body.columns else pl.lit(None, pl.Int64))
                .fill_null(data_year)
                .alias("data_year"),
                pl.lit(early_release).alias("early_release"),
                id_text("utility_id").alias("utility_id"),
                *(
                    pl.col(c).cast(t) if c in body.columns else pl.lit(None, t).alias(c)
                    for c, t in SHORT_FORM_SCHEMA.items()
                    if c not in ("data_year", "early_release", "utility_id")
                ),
            )
        )
    if not frames:
        raise ValueError("no short-form sheet (header with 'Utility Number' and 'Customers')")
    out = pl.concat(frames, how="vertical")
    wrong_year = out.filter(pl.col("data_year") != data_year)
    if wrong_year.height:
        raise ValueError(f"short form rows for {wrong_year['data_year'].unique().to_list()}, expected {data_year}")
    return out


def read_short_form_zip(data: bytes, zip_name: str, **kwargs) -> pl.DataFrame | None:
    """The short form inside one EIA-861 zip, or ``None`` when the zip has none (2019: every utility filed
    the long form that year)."""
    member = zip_member(data, SHORT_FORM_MEMBER)
    if member is None:
        return None
    data_year, early = release_of(zip_name)
    return parse_short_form(member[1], data_year=data_year, early_release=early, **kwargs).with_columns(
        pl.lit(f"{zip_name}/{member[0]}").alias("source_file")
    )


def raw_zips(raw_root: Path) -> list[Path]:
    """The EIA-861 zips of the local lake (``raw/source=eia_861/dt=*/``), the latest snapshot per file name."""
    latest: dict[str, Path] = {}
    for path in sorted((Path(raw_root) / f"source={SOURCE_ID}").glob("dt=*/f861*.zip")):
        if ZIP_NAME.match(path.name):
            latest[path.name] = path  # sorted by dt, so the last one wins
    return [latest[k] for k in sorted(latest)]


def load_short_form(raw_root: Path, **kwargs) -> pl.DataFrame:
    """Every short form in the local lake, stacked (final and early releases)."""
    frames = [
        df for p in raw_zips(raw_root) if (df := read_short_form_zip(p.read_bytes(), p.name, **kwargs)) is not None
    ]
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=SHORT_FORM_SCHEMA)


# --- totals per utility-year ---------------------------------------------------------------------------


def short_form_totals(short: pl.DataFrame) -> pl.DataFrame:
    """Short-form rows → one row per utility-year and release (:data:`TOTALS_SCHEMA`). A measure is null when
    every row of the utility-year is null (blank is not zero)."""

    def total(c: str) -> pl.Expr:
        return pl.when(pl.col(c).is_not_null().any()).then(pl.col(c).sum()).cast(pl.Float64).alias(c)

    return short.group_by("utility_id", "data_year", "early_release").agg(
        total("customers"), total("sales_mwh"), total("revenue_thousand_usd")
    ).select(TOTALS_SCHEMA.keys())


def long_form_totals(sales: pl.DataFrame) -> pl.DataFrame:
    """``eia861_sales`` total-sector rows → one row per utility-year and release (:data:`TOTALS_SCHEMA`).

    Same rule as ``accounts.eia_totals``: every part and balancing authority is summed, except part C customers
    (delivery-only, counted by the energy supplier too). Unlike it, early releases are kept (flagged) and
    revenue is carried.
    """
    tot = sales.filter(pl.col("sector") == "total")
    cust = pl.when(pl.col("part") == "C").then(None).otherwise(pl.col("customers"))

    def total(e: pl.Expr, name: str) -> pl.Expr:
        return pl.when(e.is_not_null().any()).then(e.sum()).cast(pl.Float64).alias(name)

    return tot.group_by("utility_id", "data_year", "early_release").agg(
        total(cust, "customers"),
        total(pl.col("sales_mwh"), "sales_mwh"),
        total(pl.col("revenue_thousand_usd"), "revenue_thousand_usd"),
    ).select(TOTALS_SCHEMA.keys())


def combine_forms(long: pl.DataFrame, short: pl.DataFrame) -> pl.DataFrame:
    """Long- and short-form totals → one row per utility, year and release, with ``form`` (``long`` /
    ``short``) and ``in_both`` (the utility-year was in both forms; the long form's numbers are kept)."""
    keys = ["utility_id", "data_year", "early_release"]
    lf = long.select(TOTALS_SCHEMA.keys()).with_columns(pl.lit("long").alias("form"))
    sf = short.select(TOTALS_SCHEMA.keys()).with_columns(pl.lit("short").alias("form"))
    both = lf.join(sf.select(keys), on=keys, how="semi").select(keys)
    out = pl.concat([lf, sf.join(lf.select(keys), on=keys, how="anti")], how="vertical")
    return out.join(both.with_columns(pl.lit(True).alias("in_both")), on=keys, how="left").with_columns(
        pl.col("in_both").fill_null(False)
    ).sort(keys)


def with_price(totals: pl.DataFrame) -> pl.DataFrame:
    """Average revenue per kWh in USD: thousand USD / MWh = USD / kWh. Null unless both are positive."""
    return totals.with_columns(
        pl.when((pl.col("sales_mwh") > 0) & (pl.col("revenue_thousand_usd") > 0))
        .then(pl.col("revenue_thousand_usd") / pl.col("sales_mwh"))
        .alias("price_usd_kwh")
    )


def _cagr(now: pl.Expr, then: pl.Expr, span: int) -> pl.Expr:
    return pl.when((now > 0) & (then > 0)).then((now / then) ** (1 / span) - 1)


def add_delivery(combined: pl.DataFrame, delivery: pl.DataFrame, *, first_year: int) -> pl.DataFrame:
    """Add ``delivery_customers`` and ``wires_customers`` (bundled + delivery-only) to :func:`combine_forms` rows.

    ``delivery``: :func:`read_delivery_zip` rows (``utility_id``, ``data_year``, ``early_release``,
    ``customers``). Delivery-only customers (EIA part C, retail choice) are listed from ``first_year`` on (the
    first zip with ``Delivery_Companies``): from then, a utility missing from the file has 0. Before it they are
    0 too, unless the utility already had delivery customers in ``first_year``; then they are unknown (null).
    A utility-year with delivery rows but no bundled row is not added (``wires_customers`` needs both).
    """
    keys = ["utility_id", "data_year", "early_release"]
    d = delivery.select(*keys, pl.col("customers").cast(pl.Float64).alias("_d"))
    had_first = delivery.filter(pl.col("data_year") == first_year)["utility_id"].unique().to_list()
    out = combined.join(d, on=keys, how="left").with_columns(
        pl.when(pl.col("_d").is_not_null())
        .then(pl.col("_d"))
        .when((pl.col("data_year") < first_year) & pl.col("utility_id").is_in(had_first))
        .then(None)
        .otherwise(0.0)
        .alias("delivery_customers")
    )
    return out.drop("_d").with_columns((pl.col("customers") + pl.col("delivery_customers")).alias("wires_customers"))


def utility_signals(
    combined: pl.DataFrame, *, year: int, base_year: int, customers: str = "customers"
) -> pl.DataFrame:
    """Size, growth and price per EIA utility from :func:`combine_forms` (final releases only).

    ``customers`` picks the meter count: ``customers`` (bundled service, the long-form rule) or
    ``wires_customers`` (after :func:`add_delivery`). With ``wires_customers`` the CAGR falls back to the bundled
    one when the base year's wires count is unknown.

    Returns ``utility_id``, ``eia_form`` (form in ``year``), ``eia_customers``, ``eia_sales_mwh``,
    ``eia_revenue_kusd``, ``eia_price`` (USD/kWh, bundled revenue over bundled sales), ``eia_customer_cagr`` and
    ``eia_price_cagr`` (``base_year`` → ``year``, null when either end is missing or zero), ``eia_base_form``.
    """
    final = with_price(combined.filter(~pl.col("early_release")))
    now = final.filter(pl.col("data_year") == year)
    then = final.filter(pl.col("data_year") == base_year).select(
        "utility_id",
        pl.col(customers).alias("_c0"),
        pl.col("customers").alias("_b0"),
        pl.col("price_usd_kwh").alias("_p0"),
        pl.col("form").alias("eia_base_form"),
    )
    span = year - base_year
    return now.join(then, on="utility_id", how="left").select(
        "utility_id",
        pl.col("form").alias("eia_form"),
        pl.col(customers).alias("eia_customers"),
        pl.col("sales_mwh").alias("eia_sales_mwh"),
        pl.col("revenue_thousand_usd").alias("eia_revenue_kusd"),
        pl.col("price_usd_kwh").alias("eia_price"),
        pl.coalesce(
            _cagr(pl.col(customers), pl.col("_c0"), span), _cagr(pl.col("customers"), pl.col("_b0"), span)
        ).alias("eia_customer_cagr"),
        _cagr(pl.col("price_usd_kwh"), pl.col("_p0"), span).alias("eia_price_cagr"),
        "eia_base_form",
    )


def percentile_rank(values: pl.Expr) -> pl.Expr:
    """Percentile rank in (0, 1], ties averaged, nulls stay null (the ``account_score.yaml`` method)."""
    return values.rank("average") / values.count()


# --- loaders (read-only) -------------------------------------------------------------------------------


def load_long_sales() -> pl.DataFrame:
    """``eia861_sales`` total-sector rows with revenue (``accounts.load_eia_sales`` leaves revenue out)."""
    from basecast_pipelines.models.db import read_sql

    return read_sql(
        """
        select data_year, early_release, utility_id, part, ba_code, sector, revenue_thousand_usd, sales_mwh,
               customers
        from eia861_sales where sector = 'total'
        """
    )


def read_delivery_zip(data: bytes, zip_name: str) -> pl.DataFrame | None:
    """Texas rows of ``Delivery_Companies_<year>`` (delivery-only service, EIA part C: the wires utility's
    customers under retail choice), total sector, as :data:`TOTALS_SCHEMA` plus ``utility_name``. Reuses the
    ``eia_861`` parser's Sales_Ult_Cust sheet reader (same three-row header block). ``None`` before 2020."""
    from basecast_pipelines.parsers.territories import eia_861

    member = zip_member(data, DELIVERY_MEMBER)
    if member is None:
        return None
    data_year, early = release_of(zip_name)
    frames = [
        df
        for s in sheet_names(member[1])
        if (df := eia_861._sales_sheet(read_grid(member[1], s), data_year, early)) is not None
    ]
    if not frames:
        return None
    rows = pl.concat(frames).filter(pl.col("sector") == "total")
    return rows.group_by("utility_id", "data_year", "early_release").agg(
        pl.col("utility_name").first(),
        pl.col("customers").sum().cast(pl.Float64),
        pl.col("sales_mwh").sum(),
        pl.col("revenue_thousand_usd").sum(),
    )


def load_delivery(raw_root: Path) -> pl.DataFrame:
    """Every ``Delivery_Companies`` workbook in the local lake (Texas, total sector), stacked."""
    frames = [df for p in raw_zips(raw_root) if (df := read_delivery_zip(p.read_bytes(), p.name)) is not None]
    if not frames:
        return pl.DataFrame(schema={**TOTALS_SCHEMA, "utility_name": pl.String()})
    return pl.concat(frames, how="vertical")
