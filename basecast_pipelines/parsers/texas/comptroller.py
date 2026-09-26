"""Texas Comptroller large-project signals → ``cpa_local_dev_agreements``, ``cpa_data_centers``, ``cpa_jeti``.

Every table is rebuilt from the newest snapshot (``dt``); the source republishes whole files daily.

``cpa_local_dev_agreements``: one row per agreement, ``program`` ``ch312`` (tax abatements, from
``ch312-abatement-detail.csv``, which repeats each agreement once per taxing unit and property account) or
``ch380`` (Ch. 380/381 agreements, ``ch380.csv``). County: Ch. 312 from the appraisal district (``<County>
CAD``; the two-county ``Potter Randall CAD`` falls back to a lead taxing unit named ``<County> County``),
Ch. 380 from "County affected by agreement" (first one in ``county_fips``, all in ``counties_fips``), or the
government's own county when a county signed it and named none; city agreements without a county stay null. Not
inputs: ``ch312-abatement.csv`` (the same 1,386 agreements with fewer columns), the reinvestment-zone files
(zone designations, not agreements) and the post-abatement value reports.

``cpa_data_centers``: the registered Qualifying Data Centers (``DC…`` registrations) and Qualifying Large
Data Center Projects (``LD…``) HTML tables; cells may list several owners/occupants/operators (JSON lists).
``registration_id`` is the owner's registration (occupant's or operator's when there is no owner); two rows
carry two different ids (``registration_ids``). ``06/172026`` on the page is read as 2026-06-17 (the text is
kept in ``effective_date_text``).
The page gives no county: ``county_fips`` is filled only when the name says ``<Texas county> County``
(``county_method = 'name_hint'``, not verified).

``cpa_jeti``: current JETI (Ch. 403) agreements HTML table; proposed investment parsed from text such as
``$1.7 billion``. The page names the school district only; ``county_fips`` comes from a ``<county> County``
district name when there is one, otherwise it stays null (no ISD → county table in raw).

Personal data dropped: Ch. 380 officers, form preparers, entity contacts (names, phones, emails,
addresses) and assumed names; Ch. 312 contact ids, taxpayer ids and authorized users. Recipient names
(Ch. 312 property owner, Ch. 380 entity) are kept only when they read as an organization (``_privacy.py``);
otherwise ``recipient_name`` is null and ``recipient_name_redacted`` is true."""

from __future__ import annotations

import io
import json
import re
from html.parser import HTMLParser

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by, latest_dt
from basecast_pipelines.parsers.texas._counties import TX_COUNTIES, county_fips, county_fips_expr
from basecast_pipelines.parsers.texas._privacy import is_org_name

SOURCE_ID = "tx_comptroller"

_SHA = r"(?:__[0-9a-f]{8})?"
_CH312 = re.compile(rf"^ch312-abatement-detail{_SHA}\.csv$", re.IGNORECASE)
_CH380 = re.compile(rf"^ch380{_SHA}\.csv$", re.IGNORECASE)
_DATA_CENTERS = re.compile(rf"^data-center-lists{_SHA}\.html$", re.IGNORECASE)
_JETI = re.compile(rf"^jeti-current-agreements{_SHA}\.html$", re.IGNORECASE)


def _logical_name(f: RawFile) -> str:
    return re.sub(r"__[0-9a-f]{8}(\.[a-z]+)$", r"\1", f.name.lower())


def _newest(files: list[RawFile]) -> list[RawFile]:
    """The newest snapshot, one file per logical name (a same-day change is stored with a ``__<sha8>`` suffix)."""
    return latest_by(_logical_name)(latest_dt(files))


def _clean(value: object) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text or None


def _read_csv(f: RawFile) -> pl.DataFrame:
    df = pl.read_csv(io.BytesIO(f.read_bytes()), infer_schema=False, encoding="utf8-lossy")
    df = df.rename({c: re.sub(r"\s+", " ", c).strip() for c in df.columns})  # two headers end with a space
    return df.with_columns([pl.col(c).str.strip_chars().replace("", None) for c in df.columns])


def _need(f: RawFile, df: pl.DataFrame, columns: set[str]) -> None:
    missing = columns - set(df.columns)
    if missing:
        raise ValueError(f"{f.key}: missing columns {sorted(missing)}")


def _real_date(d: pl.Expr) -> pl.Expr:
    """``01/01/1900`` is a placeholder in these files, not a date."""
    return pl.when(d.dt.year() <= 1900).then(None).otherwise(d)


def _mdy(col: str) -> pl.Expr:
    """``03/20/2018`` or ``1/1/2022 12:00:00 AM`` → Date."""
    return _real_date(pl.col(col).str.extract(r"^(\d{1,2}/\d{1,2}/\d{4})").str.to_date("%m/%d/%Y", strict=False))


def _page_date(col: str) -> pl.Expr:
    """``06/17/2026``; also ``06/172026`` (a typo on the data-center page)."""
    text = pl.col(col).str.strip_chars()
    return _real_date(pl.coalesce(text.str.to_date("%m/%d/%Y", strict=False),
                                  text.str.to_date("%m/%d%Y", strict=False)))


def _yes(col: str) -> pl.Expr:
    return pl.col(col).str.to_uppercase().str.strip_chars().replace_strict({"Y": True, "N": False}, default=None,
                                                                           return_dtype=pl.Boolean)


def _money(col: str) -> pl.Expr:
    return pl.col(col).str.replace_all(r"[$,\s]", "").cast(pl.Float64, strict=False)


def _recipient(col: str) -> list[pl.Expr]:
    org = is_org_name(col)
    return [
        pl.when(org).then(pl.col(col)).otherwise(None).alias("recipient_name"),
        (pl.col(col).is_not_null() & ~org).alias("recipient_name_redacted"),
    ]


# --- cpa_local_dev_agreements ----------------------------------------------------------------------------

# one schema for both programs (columns a program lacks are null)
_AGREEMENT_SCHEMA = {
    "program": pl.String, "agreement_id": pl.String, "previous_agreement_id": pl.String, "status": pl.String,
    "agreement_type": pl.String, "local_government_id": pl.String, "local_government_type": pl.String,
    "local_government_name": pl.String, "reporting_entity": pl.String, "county_name": pl.String,
    "county_fips": pl.String, "counties_fips": pl.String, "recipient_name": pl.String,
    "recipient_name_redacted": pl.Boolean, "reinvestment_zone_name": pl.String, "executed_date": pl.Date,
    "effective_date": pl.Date, "expiration_date": pl.Date, "executed_modification_date": pl.Date,
    "effective_modification_date": pl.Date, "reason_for_modification": pl.String,
    "executed_cancellation_date": pl.Date, "reason_for_cancellation": pl.String, "submission_date": pl.Date,
    "last_updated_date": pl.Date, "business_activity": pl.String, "business_type": pl.String,
    "business_size": pl.String, "physical_structure": pl.String, "naics_code": pl.String,
    "employment_created": pl.Boolean, "new_construction": pl.Boolean, "clawbacks": pl.Boolean,
    "base_value": pl.Float64, "total_incentive_value": pl.Float64, "new_fte_per_year": pl.Float64,
    "payroll_created": pl.Float64, "property_value_created": pl.Float64, "abatement_term_years": pl.Float64,
    "taxing_units": pl.String, "n_taxing_units": pl.Int32, "scope": pl.String, "scope_note": pl.String,
    "summary": pl.String, "expiration_terms": pl.String, "funding_source": pl.String, "file_name": pl.String,
}  # fmt: skip


def _conform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        [(pl.col(c) if c in df.columns else pl.lit(None)).cast(t).alias(c) for c, t in _AGREEMENT_SCHEMA.items()]
    )



_CH312_FIRST = {  # source column → output column, same value on every row of an agreement
    "New Record ID Linked to Older Record ID": "previous_agreement_id",
    "CAD Name (Reporting Entity)": "reporting_entity",
    "Lead Taxing Unit Name": "local_government_name",
    "Reinvestment Zone Name": "reinvestment_zone_name",
    "Abatement Status": "status",
    "Business Activity": "business_activity",
    "Business Type": "business_type",
    "Business Size": "business_size",
    "Physical Structure": "physical_structure",
    "Reason for Modification": "reason_for_modification",
    "Reason for Cancellation": "reason_for_cancellation",
}


def _ch312(f: RawFile) -> pl.DataFrame:
    df = _read_csv(f)
    _need(f, df, {
        "Record ID", "Property Owner Name", "Executed/Signed Date of Agreement", "Effective Date of Agreement",
        "Expiration Date of Agreement", "Executed Modification Date", "Effective Modification Date",
        "Executed Cancellation Date", "Submission Date", "Base Value of Abated Property", "Total Value of Incentives",
        "New FTE/Year", "Payroll Dollars Created", "Property Value Dollars Created", "Length of Abatement Term",
        "Employment Created", "New Construction", "Clawbacks", "Taxing Unit Name", *_CH312_FIRST,
    })  # fmt: skip
    optional = lambda c: pl.col(c) if c in df.columns else pl.lit(None, pl.String)  # noqa: E731
    rows = df.select(
        pl.col("Record ID").alias("agreement_id"),
        *[pl.col(src).alias(dst) for src, dst in _CH312_FIRST.items()],
        pl.col("Property Owner Name").alias("_recipient"),
        _mdy("Executed/Signed Date of Agreement").alias("executed_date"),
        _mdy("Effective Date of Agreement").alias("effective_date"),
        _mdy("Expiration Date of Agreement").alias("expiration_date"),
        _mdy("Executed Modification Date").alias("executed_modification_date"),
        _mdy("Effective Modification Date").alias("effective_modification_date"),
        _mdy("Executed Cancellation Date").alias("executed_cancellation_date"),
        _mdy("Submission Date").alias("submission_date"),
        _money("Base Value of Abated Property").alias("base_value"),
        _money("Total Value of Incentives").alias("total_incentive_value"),
        _money("New FTE/Year").alias("new_fte_per_year"),
        _money("Payroll Dollars Created").alias("payroll_created"),
        _money("Property Value Dollars Created").alias("property_value_created"),
        pl.col("Length of Abatement Term").cast(pl.Float64, strict=False).alias("abatement_term_years"),
        optional("NAICS Code").str.extract(r"^(\d{2,6})$").alias("naics_code"),
        _yes("Employment Created").alias("employment_created"),
        _yes("New Construction").alias("new_construction"),
        _yes("Clawbacks").alias("clawbacks"),
        pl.col("Taxing Unit Name"),
    )
    first = [c for c in rows.columns if c not in ("agreement_id", "Taxing Unit Name")]
    maxima = {"new_fte_per_year", "payroll_created", "property_value_created", "abatement_term_years"}
    agreements = rows.group_by("agreement_id", maintain_order=True).agg(
        *[pl.col(c).drop_nulls().max().alias(c) if c in maxima else pl.col(c).drop_nulls().first().alias(c)
          for c in first],
        pl.col("Taxing Unit Name").drop_nulls().unique(maintain_order=True).alias("_units"),
    )
    county = county_fips_expr("reporting_entity")
    lead_county = pl.when(pl.col("local_government_name").str.contains(r"(?i)\bcounty$")).then(
        county_fips_expr("local_government_name")
    )
    out = agreements.with_columns(
        pl.lit("ch312").alias("program"),
        pl.coalesce(county, lead_county).alias("county_fips"),
        pl.col("_units").map_elements(lambda s: json.dumps(list(s)), return_dtype=pl.String).alias("taxing_units"),
        pl.col("_units").list.len().alias("n_taxing_units"),
        *_recipient("_recipient"),
    )
    return _conform(out.with_columns(
        pl.col("county_fips").replace_strict(TX_COUNTIES, default=None, return_dtype=pl.String).alias("county_name"),
        pl.when(pl.col("county_fips").is_null()).then(pl.lit("[]"))
        .otherwise(pl.format('["{}"]', pl.col("county_fips"))).alias("counties_fips"),
    ))


_CH380_COUNTIES = re.compile(r"^County affected by agreement(?: \((\d+)\))?$")


def _ch380(f: RawFile) -> pl.DataFrame:
    df = _read_csv(f)
    _need(f, df, {"Agreement Number", "Local Government Name", "Entity Name", "Effective Date", "Expiration Date",
                  "Agreement status", "County affected by agreement"})
    counties = sorted((c for c in df.columns if _CH380_COUNTIES.match(c)),
                      key=lambda c: int(_CH380_COUNTIES.match(c).group(1) or 1))
    # a county government's own county when no "county affected" is given (596 of 864 such rows in 2026-09)
    own = pl.when(pl.col("Government Type").str.to_lowercase() == "county").then(pl.col("Local Government Name")) \
        if "Government Type" in df.columns else pl.lit(None, pl.String)
    fips = pl.concat_list([*[county_fips_expr(c) for c in counties], county_fips_expr(own)]).list.drop_nulls()
    optional = lambda c: pl.col(c) if c in df.columns else pl.lit(None, pl.String)  # noqa: E731
    out = df.select(
        pl.lit("ch380").alias("program"),
        pl.col("Agreement Number").alias("agreement_id"),
        optional("Previous agreement number").alias("previous_agreement_id"),
        optional("Local Government ID").alias("local_government_id"),
        optional("Agreement Type").alias("agreement_type"),
        optional("Government Type").alias("local_government_type"),
        pl.col("Local Government Name").alias("local_government_name"),
        pl.col("Agreement status").alias("status"),
        pl.col("Entity Name").alias("_recipient"),
        _real_date(pl.col("Effective Date").str.to_date("%Y-%m-%d", strict=False)).alias("effective_date"),
        _real_date(pl.col("Expiration Date").str.to_date("%Y-%m-%d", strict=False)).alias("expiration_date"),
        optional("lst_upd_ts").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("last_updated_date"),
        optional("Focus/Scope of agreement").alias("scope"),
        optional("scope_note").alias("scope_note"),
        optional("Summary of Agreement").alias("summary"),
        optional("term_expr_tx").alias("expiration_terms"),
        optional("srce_fund_tx").alias("funding_source"),
        _money("totl_agmt_am").alias("total_incentive_value") if "totl_agmt_am" in df.columns
        else pl.lit(None, pl.Float64).alias("total_incentive_value"),
        optional("File Name").alias("file_name"),
        fips.list.first().alias("county_fips"),
        fips.list.unique(maintain_order=True).alias("_fips"),
    )
    return _conform(out.with_columns(
        pl.col("_fips").map_elements(lambda s: json.dumps(list(s)), return_dtype=pl.String).alias("counties_fips"),
        pl.col("county_fips").replace_strict(TX_COUNTIES, default=None, return_dtype=pl.String).alias("county_name"),
        *_recipient("_recipient"),
    ))


def parse_agreements(f: RawFile) -> pl.DataFrame | None:
    if _CH312.match(f.name):
        return _ch312(f)
    if _CH380.match(f.name):
        return _ch380(f)
    return None  # not an agreement file (inputs keeps these out)


# --- HTML tables -------------------------------------------------------------------------------------------


class _Tables(HTMLParser):
    """Every ``<table>``: its label (the last ``<summary>``/``<caption>`` text before or inside it) and rows of
    cells; a cell is the list of its ``<li>`` texts, or its whole text when it has no list."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[dict] = []
        self.label = ""
        self._label_tag: str | None = None
        self._label_text: list[str] = []
        self._table: dict | None = None
        self._row: list[list[str]] | None = None
        self._cell: list[str] | None = None
        self._text: list[str] = []
        self._items: list[str] = []
        self._in_li = False
        self._sup = False

    def handle_starttag(self, tag, attrs):
        if tag in ("summary", "caption"):
            self._label_tag, self._label_text = tag, []
        elif tag == "table":
            self._table = {"label": self.label, "rows": []}
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell, self._text, self._items = [], [], []
        elif tag == "li" and self._cell is not None:
            self._in_li, self._li = True, []
        elif tag == "sup":
            self._sup = True

    def handle_endtag(self, tag):
        if tag == self._label_tag:
            self.label = _clean("".join(self._label_text)) or self.label
            if self._table is not None:
                self._table["label"] = self.label
            self._label_tag = None
        elif tag == "li" and self._in_li:
            self._in_li = False
            if text := _clean("".join(self._li)):
                self._items.append(text)
        elif tag in ("td", "th") and self._cell is not None and self._row is not None:
            whole = _clean("".join(self._text))
            self._row.append(self._items if self._items else ([whole] if whole else []))
            self._cell = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if self._row:
                self._table["rows"].append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None
        elif tag == "sup":
            self._sup = False

    def handle_data(self, data):
        if self._label_tag:
            self._label_text.append(data)
        if self._cell is not None:
            self._text.append("*" if self._sup and data.strip() == "*" else data)
            if self._in_li:
                self._li.append(data)


def _html_tables(f: RawFile) -> list[dict]:
    parser = _Tables()
    parser.feed(f.read_bytes().decode("utf-8", errors="replace"))
    parser.close()
    return parser.tables


def _header_index(f: RawFile, header: list[list[str]], wanted: dict[str, str]) -> dict[str, int]:
    """Output column → cell position, matching header texts by regex."""
    texts = [(" ".join(c)).lower() for c in header]
    index = {}
    for out, pattern in wanted.items():
        hit = next((i for i, t in enumerate(texts) if re.search(pattern, t)), None)
        if hit is None:
            raise ValueError(f"{f.key}: no column matching {pattern!r} in {texts}")
        index[out] = hit
    return index


def _name_hint_county(name: str | None) -> str | None:
    """``Borden County Data Center`` → 48033; only an explicit ``<name> County`` counts."""
    for m in re.finditer(r"([A-Za-z][A-Za-z .']*?)\s+County\b", name or ""):
        words = m.group(1).split()
        for k in range(min(3, len(words)), 0, -1):  # "Deaf Smith County", "Van Zandt County", "Borden County"
            if fips := county_fips(" ".join(words[-k:])):
                return fips
    return None


_DC_COLUMNS = {
    "data_center_name": r"^data center$",
    "effective_date": r"effective date",
    "owner_names": r"owner name",
    "owner_registration_numbers": r"owner registration",
    "occupant_names": r"occupant name",
    "occupant_registration_numbers": r"occupant registration",
    "operator_names": r"operator name",
    "operator_registration_numbers": r"operator registration",
    "exemption_end_date": r"exemption end",
}


def parse_data_centers(f: RawFile) -> pl.DataFrame | None:
    tables = [t for t in _html_tables(f) if t["rows"] and "data center" in " ".join(t["rows"][0][0]).lower()]
    if not tables:
        raise ValueError(f"{f.key}: no data-center tables")
    records = []
    for table in tables:
        label = table["label"].lower()
        if "large data center" in label:
            program = "qualifying_large_data_center_project"
        elif "data center" in label:
            program = "qualifying_data_center"
        else:
            raise ValueError(f"{f.key}: data-center table with unknown label {table['label']!r}")
        index = _header_index(f, table["rows"][0], _DC_COLUMNS)
        for row in table["rows"][1:]:
            cell = lambda k: row[index[k]] if index[k] < len(row) else []  # noqa: E731
            name = _clean(" ".join(cell("data_center_name")))
            if not name:
                continue
            prefixes = [m.group(1) for k in ("owner", "occupant", "operator") for r in cell(f"{k}_registration_numbers")
                        if (m := re.search(r"\b([A-Z]{2}\d{4,})-", r))]
            ids = list(dict.fromkeys(prefixes))  # owner's registration first
            fips = _name_hint_county(name)
            records.append({
                "program": program,
                "data_center_name": name,
                "registration_id": ids[0] if ids else None,
                "registration_ids": json.dumps(ids),
                "effective_date": _clean(" ".join(cell("effective_date"))),
                "exemption_end_date": _clean(" ".join(cell("exemption_end_date"))),
                **{k: json.dumps(cell(k)) if cell(k) else None for k in _LIST_COLUMNS},
                "county_fips": fips,
                "county_method": "name_hint" if fips else None,
            })
    df = pl.DataFrame(records, schema={k: pl.String for k in records[0]}) if records else None
    if df is None:
        raise ValueError(f"{f.key}: data-center tables have no rows")
    return df.with_columns(
        pl.col("effective_date").alias("effective_date_text"),
        _page_date("effective_date").alias("effective_date"),
        _page_date("exemption_end_date").alias("exemption_end_date"),
    )


_LIST_COLUMNS = [k for k in _DC_COLUMNS if k.endswith(("names", "numbers"))]
_JETI_COLUMNS = {
    "application_number": r"application",
    "applicant": r"applicant",
    "school_district": r"school district",
    "project_type": r"project type",
    "proposed_investment_text": r"proposed investment",
    "limitation_text": r"limitation",
    "minimum_required_jobs_text": r"minimum required jobs",
}
_SCALE = {"thousand": 1e3, "million": 1e6, "billion": 1e9, "trillion": 1e12}


def _usd(text: str | None) -> float | None:
    m = re.search(r"\$\s*([\d,]*\.?\d+)\s*(thousand|million|billion|trillion)?", text or "", re.IGNORECASE)
    if not m:
        return None
    return float(m.group(1).replace(",", "")) * _SCALE.get((m.group(2) or "").lower(), 1.0)


def parse_jeti(f: RawFile) -> pl.DataFrame | None:
    tables = [t for t in _html_tables(f) if t["rows"] and "applicant" in " ".join(sum(t["rows"][0], [])).lower()]
    if len(tables) != 1:
        raise ValueError(f"{f.key}: expected one JETI agreements table, found {len(tables)}")
    table = tables[0]
    index = _header_index(f, table["rows"][0], _JETI_COLUMNS)
    records = []
    for row in table["rows"][1:]:
        values = {k: _clean(" ".join(row[i])) if i < len(row) else None for k, i in index.items()}
        if not values["application_number"]:
            continue
        jobs = values["minimum_required_jobs_text"] or ""
        limitation = re.search(r"([\d.]+)\s*%", values["limitation_text"] or "")
        count = re.fullmatch(r"\s*([\d,]+)\s*\*?\s*", jobs)
        fips = _name_hint_county(values["school_district"])
        records.append({
            **{k: v for k, v in values.items() if k not in ("limitation_text",)},
            "proposed_investment_usd": _usd(values["proposed_investment_text"]),
            "limitation_pct": float(limitation.group(1)) if limitation else None,
            "minimum_required_jobs": int(count.group(1).replace(",", "")) if count else None,
            "jobs_requirement_waived": "*" in jobs,
            "county_fips": fips,
            "county_method": "name_hint" if fips else None,
        })
    if not records:
        raise ValueError(f"{f.key}: JETI table has no rows")
    return pl.DataFrame(records, schema_overrides={"county_fips": pl.String, "county_method": pl.String,
                                                   "proposed_investment_usd": pl.Float64, "limitation_pct": pl.Float64,
                                                   "minimum_required_jobs": pl.Int64})


DATASETS = [
    Dataset(
        name="cpa_local_dev_agreements",
        target="postgres",
        mode="replace",
        description=(
            "Texas Comptroller local development agreements, one row per agreement: Ch. 312 tax abatements and "
            "Ch. 380/381 agreements, with county_fips, recipient (organizations only), dates and values."
        ),
        parse=parse_agreements,
        inputs=lambda f: bool(_CH312.match(f.name) or _CH380.match(f.name)),
        select=_newest,
    ),
    Dataset(
        name="cpa_data_centers",
        target="postgres",
        mode="replace",
        description=(
            "Texas Comptroller registered Qualifying Data Centers and Qualifying Large Data Center Projects: "
            "effective and exemption end dates, owners, occupants and operators; no county on the page."
        ),
        parse=parse_data_centers,
        inputs=lambda f: bool(_DATA_CENTERS.match(f.name)),
        select=_newest,
    ),
    Dataset(
        name="cpa_jeti",
        target="postgres",
        mode="replace",
        description=(
            "Texas Comptroller current JETI (Ch. 403) agreements: applicant, school district, project type, "
            "proposed investment (USD), limitation and minimum jobs."
        ),
        parse=parse_jeti,
        inputs=lambda f: bool(_JETI.match(f.name)),
        select=_newest,
    ),
]
