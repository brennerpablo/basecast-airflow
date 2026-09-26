"""``official_forecasts``: every official peak and energy forecast in one long table (LTLF, CDR and the
manual figures in ``config/``), the input of the backtest. Declared by the three parser modules, so any of
them rebuilds it; skipped until all three tables exist."""

from __future__ import annotations

from basecast_pipelines.processing.core import SqlDataset

_COLUMNS = {
    "vintage": "text",
    "vintage_date": "date",
    "publisher": "text",
    "product": "text",
    "target_year": "integer",
    "target_month": "integer",
    "season": "text",
    "region_type": "text",
    "region_id": "text",
    "metric": "text",
    "scenario": "text",
    "value": "double precision",
    "unit": "text",
    "sheet": "text",
    "row_label": "text",
    "column_label": "text",
    "source_file": "text",
}
_SOURCES = ("ltlf_forecasts", "cdr_forecasts", "manual_official_forecasts")


def _select(table: str) -> str:
    columns = ", ".join(
        f"upper({name}) AS {name}" if name == "product" else f"{name}::{typ} AS {name}" for name, typ in _COLUMNS.items()
    )
    return f"SELECT {columns} FROM {table}"


OFFICIAL_FORECASTS = SqlDataset(
    name="official_forecasts",
    sql="\nUNION ALL\n".join(_select(t) for t in _SOURCES),
    description=(
        "Official ERCOT forecasts in one long shape: LTLF and CDR vintages plus manual figures with their "
        "source (vintage, target year/month/season, region, metric, scenario, value, unit, provenance)."
    ),
    indexes=(("metric", "target_year"), ("product", "vintage_date")),
)
