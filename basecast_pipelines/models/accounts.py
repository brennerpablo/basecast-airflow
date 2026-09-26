"""Account universe and account signals for the commercial-intelligence score (phase 0, Q2 and Q3).

An **account** is a distribution co-op or a municipal utility in ERCOT, one PUCT CCN territory
(``puct_ccn_territories``, ``utility_type`` coop/muni with ERCOT in ``iso_rto``); ``account_id`` is its
``ccn_no``. ``docs/PHASE0_ANALYSIS.md`` §2 Q2 asks whether that universe closes against EIA-861 (the only
source of customers and sales per utility) and Q3 whether the signals built on it are usable.

- **Crosswalk PUCT ↔ EIA-861:** names are normalized (abbreviations expanded, legal suffixes and state tags
  dropped), reduced to their distinctive words (``core_name``) and compared with rapidfuzz
  ``token_sort_ratio``, within the same type only (co-op ↔ Cooperative, muni ↔ Municipal / Political
  Subdivision). The EIA-861 ``Service_Territory`` counties confirm a pair: ``county_overlap`` is the share
  of the PUCT territory's area in counties the EIA utility lists. ``auto`` = score ≥ 90 and overlap > 0;
  anything else is ``review``.
- **Signals:** county numbers reach an account through ``county_utility_overlap_puct``. Counts (people,
  homes, permits, sites) are apportioned by ``county_share`` (the share of the county's area inside the
  territory), which assumes they spread evenly over the county's area: fine for growth rates and shares,
  biased low for dense city territories (munis). Zone growth is weighted by the overlap area.

The logic is pure functions on DataFrames (tested in ``tests/models/test_accounts.py``); the ``load_*``
functions are thin read-only queries through ``models.db``. rapidfuzz lives in the ``analysis`` group, so
it is imported where it is used.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from pathlib import Path

import polars as pl
import yaml

from basecast_pipelines.models.db import read_sql

ACCOUNT_TYPES = ("coop", "muni")
AUTO_SCORE = 90.0
EIA_TYPE = {"Cooperative": "coop", "Municipal": "muni", "Political Subdivision": "muni"}

# Abbreviations EIA-861 uses in utility names, expanded before comparing.
_ABBREVIATIONS = {
    "coop": "cooperative",
    "co op": "cooperative",
    "elec": "electric",
    "assn": "association",
    "assoc": "association",
    "util": "utility",
    "utils": "utilities",
    "dept": "department",
    "pub": "public",
    "sys": "system",
    "svc": "services",
    "svcs": "services",
    "cnty": "county",
    "mun": "municipal",
}
_LEGAL = {"inc", "incorporated", "llc", "ltd", "corp", "corporation", "co", "company"}
# Words every utility name carries; what is left is the distinctive part ("bandera", "san marcos").
_GENERIC = _LEGAL | {
    "electric",
    "electrical",
    "cooperative",
    "association",
    "city",
    "of",
    "town",
    "the",
    "utilities",
    "utility",
    "system",
    "systems",
    "municipal",
    "light",
    "power",
    "and",
    "department",
    "public",
    "board",
    "service",
    "services",
    "energy",
    "government",
    "works",
    "water",
    "division",
    "rural",
    "member",
}
_STATE_TAG = re.compile(r"\s*-?\s*\(([A-Za-z]{2})\)\s*$")


# --- names ---------------------------------------------------------------------------------------------


def state_tag(name: str | None) -> str | None:
    """``'Farmers Electric Coop, Inc - (NM)'`` → ``'NM'`` (EIA's disambiguation suffix), else None."""
    match = _STATE_TAG.search(name or "")
    return match.group(1).upper() if match else None


def normalize_name(name: str | None) -> str:
    """Lower-case ASCII words, abbreviations expanded, legal suffixes and the state tag dropped."""
    text = _STATE_TAG.sub("", name or "")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    text = text.replace("&", " and ").replace("co-op", "coop")
    words = re.sub(r"[^0-9a-z]+", " ", text).split()
    expanded = " ".join(_ABBREVIATIONS.get(w, w) for w in words)
    return " ".join(w for w in expanded.split() if w not in _LEGAL)


def core_name(name: str | None) -> str:
    """The distinctive words of a utility name: ``'Bartlett City of'`` → ``'bartlett'``."""
    return " ".join(w for w in normalize_name(name).split() if w not in _GENERIC)


def name_score(a: str | None, b: str | None) -> float:
    """0–100 similarity of two utility names (rapidfuzz ``token_sort_ratio`` on the core names).

    ``token_sort_ratio`` rather than ``token_set_ratio``: the set version scores 100 whenever one core is
    contained in the other ("southwest" vs "southwest texas").
    """
    from rapidfuzz import fuzz

    ca, cb = core_name(a), core_name(b)
    if not ca or not cb:
        ca, cb = normalize_name(a), normalize_name(b)
    return float(fuzz.token_sort_ratio(ca, cb))


def infer_type_from_name(name: str | None) -> str | None:
    """Co-op or muni from the name, for EIA utilities with no ownership in the parsed tables."""
    norm = normalize_name(name)
    if re.search(r"\b(city|town|village) of\b", norm) or "municipal" in norm.split():
        return "muni"
    if "cooperative" in norm.split() or re.search(r"\belectric (membership|association)\b", norm):
        return "coop"
    return None


# --- crosswalk -----------------------------------------------------------------------------------------


def county_overlap(puct_counties: pl.DataFrame, eia_counties: pl.DataFrame) -> pl.DataFrame:
    """Share of each PUCT territory's area in counties each EIA utility lists, for every pair that shares
    a county.

    ``puct_counties``: ``account_id``, ``county_fips``, ``overlap_km2``. ``eia_counties``: ``utility_id``,
    ``county_fips``. Returns ``account_id``, ``utility_id``, ``county_overlap`` (0–1].
    """
    area = puct_counties.group_by("account_id").agg(pl.col("overlap_km2").sum().alias("_total"))
    return (
        puct_counties.join(eia_counties.unique(["utility_id", "county_fips"]), on="county_fips")
        .group_by("account_id", "utility_id")
        .agg(pl.col("overlap_km2").sum().alias("_shared"))
        .join(area, on="account_id")
        .select("account_id", "utility_id", (pl.col("_shared") / pl.col("_total")).alias("county_overlap"))
    )


def match_utilities(
    accounts: pl.DataFrame,
    eia: pl.DataFrame,
    overlap: pl.DataFrame | None = None,
    *,
    auto_score: float = AUTO_SCORE,
) -> pl.DataFrame:
    """Best EIA-861 utility for each account, within the same type.

    ``accounts``: ``account_id``, ``name``, ``account_type``. ``eia``: ``utility_id``, ``utility_name``,
    ``account_type``. ``overlap``: output of :func:`county_overlap` (optional). Ties on the name score go to
    the larger county overlap, then to names without a foreign state tag.

    Returns one row per account: the best candidate by name (``eia_utility_id``, ``eia_utility_name``,
    ``score``, ``county_overlap``), the runner-up by name, the candidate with the largest county overlap
    (``geo_utility_id``, ``geo_utility_name``, ``geo_score``, ``geo_overlap``) and ``status``
    (``auto`` | ``review``).
    """
    pairs = accounts.select("account_id", "name", "account_type").join(
        eia.select("utility_id", "utility_name", "account_type"), on="account_type"
    )
    pairs = pairs.with_columns(
        pl.struct("name", "utility_name")
        .map_elements(lambda r: name_score(r["name"], r["utility_name"]), return_dtype=pl.Float64)
        .alias("score"),
        pl.col("utility_name")
        .map_elements(lambda n: state_tag(n) not in (None, "TX"), return_dtype=pl.Boolean)
        .alias("_foreign"),
    )
    if overlap is not None and overlap.height:
        pairs = pairs.join(overlap, on=["account_id", "utility_id"], how="left")
    else:
        pairs = pairs.with_columns(pl.lit(None, pl.Float64).alias("county_overlap"))
    pairs = pairs.with_columns(pl.col("county_overlap").fill_null(0.0))

    by_name = pairs.sort(
        ["account_id", "score", "county_overlap", "_foreign", "utility_id"],
        descending=[False, True, True, False, False],
    )
    best = by_name.group_by("account_id", maintain_order=True).first()
    second = by_name.group_by("account_id", maintain_order=True).agg(
        pl.col("utility_name").slice(1, 1).first().alias("runner_up_name"),
        pl.col("score").slice(1, 1).first().alias("runner_up_score"),
    )
    geo = (
        pairs.filter(pl.col("county_overlap") > 0)
        .sort(["account_id", "county_overlap", "score"], descending=[False, True, True])
        .group_by("account_id", maintain_order=True)
        .first()
        .select(
            "account_id",
            pl.col("utility_id").alias("geo_utility_id"),
            pl.col("utility_name").alias("geo_utility_name"),
            pl.col("score").alias("geo_score"),
            pl.col("county_overlap").alias("geo_overlap"),
        )
    )
    out = (
        accounts.select("account_id", "name", "account_type")
        .join(
            best.select(
                "account_id",
                pl.col("utility_id").alias("eia_utility_id"),
                pl.col("utility_name").alias("eia_utility_name"),
                "score",
                "county_overlap",
            ),
            on="account_id",
            how="left",
        )
        .join(second, on="account_id", how="left")
        .join(geo, on="account_id", how="left")
    )
    auto = (pl.col("score") >= auto_score) & (pl.col("county_overlap") > 0)
    return out.with_columns(pl.when(auto).then(pl.lit("auto")).otherwise(pl.lit("review")).alias("status"))


def score_bins(scores: pl.Series, edges: tuple[int, ...] = (0, 50, 70, 80, 90, 95, 100)) -> pl.DataFrame:
    """Counts of match scores per bin (``[lo, hi)``, the last bin closed), for the distribution table."""
    rows = []
    for lo, hi in zip(edges[:-1], edges[1:], strict=True):
        last = hi == edges[-1]
        mask = (scores >= lo) & ((scores <= hi) if last else (scores < hi))
        rows.append({"bin": f"[{lo}, {hi}]" if last else f"[{lo}, {hi})", "n": int(mask.sum())})
    return pl.DataFrame(rows)


def build_universe(accounts: pl.DataFrame, crosswalk: pl.DataFrame, *, intersect: bool = True) -> pl.DataFrame:
    """The account universe: ERCOT co-ops and munis with their accepted EIA-861 utility id.

    ``crosswalk`` rows are accepted when ``status`` is ``auto`` or their ``decision`` is ``accept``
    (reviewed rows, which may point to another utility through ``eia_utility_id``). ``intersect=True``
    keeps only matched accounts (PUCT ∩ EIA, the plan); ``False`` keeps every PUCT account (plan B).
    """
    decision = pl.col("decision") if "decision" in crosswalk.columns else pl.lit(None, pl.Utf8)
    accepted = crosswalk.filter((pl.col("status") == "auto") | (decision == "accept")).select(
        "account_id", "eia_utility_id"
    )
    universe = accounts.join(accepted, on="account_id", how="left")
    if intersect:
        universe = universe.filter(pl.col("eia_utility_id").is_not_null())
    return universe.sort("account_type", "account_id")


def find_names(names: pl.Series, wanted: list[str], *, min_score: float = AUTO_SCORE) -> pl.DataFrame:
    """Which ``wanted`` names exist in ``names`` (best name score ≥ ``min_score``). Returns ``wanted``,
    ``found`` (the matched name) and ``score``; nothing else about the match."""
    pool = names.drop_nulls().unique().to_list()
    rows = []
    for w in wanted:
        scored = sorted(((name_score(w, n), n) for n in pool), reverse=True)
        score, found = scored[0] if scored else (0.0, None)
        rows.append({"wanted": w, "found": found if score >= min_score else None, "score": score})
    return pl.DataFrame(rows)


# --- signals -------------------------------------------------------------------------------------------


def apportion(county_values: pl.DataFrame, links: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Sum county values into accounts, each county weighted by ``county_share``.

    ``links``: ``account_id``, ``county_fips``, ``county_share``. A column is null for an account when
    none of its counties has a value (no data is not zero).
    """
    joined = links.join(county_values, on="county_fips", how="left")
    return joined.group_by("account_id").agg(
        pl.when(pl.col(c).is_not_null().any()).then((pl.col(c) * pl.col("county_share")).sum()).alias(c)
        for c in columns
    )


def eia_totals(sales: pl.DataFrame) -> pl.DataFrame:
    """Customers, sales and residential sales per EIA utility and year from ``eia861_sales`` (final
    releases only).

    Sums every part and balancing authority of a utility, except part C customers (delivery-only, counted by
    the energy supplier too; EIA's own state-total rule). Returns ``utility_id``, ``data_year``,
    ``customers``, ``sales_mwh``, ``res_mwh``.
    """
    final = sales.filter(~pl.col("early_release").fill_null(False))
    cust = pl.when(pl.col("part") == "C").then(0).otherwise(pl.col("customers")).fill_null(0)
    tot = final.filter(pl.col("sector") == "total").group_by("utility_id", "data_year").agg(
        cust.sum().cast(pl.Float64).alias("customers"), pl.col("sales_mwh").fill_null(0).sum().alias("sales_mwh")
    )
    res = final.filter(pl.col("sector") == "residential").group_by("utility_id", "data_year").agg(
        pl.col("sales_mwh").fill_null(0).sum().alias("res_mwh")
    )
    return tot.join(res, on=["utility_id", "data_year"], how="left").sort("utility_id", "data_year")


def eia_account_signals(sales: pl.DataFrame, *, year: int, base_year: int) -> pl.DataFrame:
    """Size, mix and growth per EIA utility (see :func:`eia_totals`).

    Returns ``utility_id``, ``eia_customers``, ``eia_sales_mwh``, ``eia_res_sales_share`` (``year``) and
    ``eia_customer_cagr`` (``base_year`` → ``year``, null when either year is missing or zero).
    """
    totals = eia_totals(sales)
    now = totals.filter(pl.col("data_year") == year)
    then = totals.filter(pl.col("data_year") == base_year).select("utility_id", pl.col("customers").alias("_c0"))
    span = year - base_year
    return now.join(then, on="utility_id", how="left").select(
        "utility_id",
        pl.col("customers").alias("eia_customers"),
        pl.col("sales_mwh").alias("eia_sales_mwh"),
        pl.when(pl.col("sales_mwh") > 0).then(pl.col("res_mwh") / pl.col("sales_mwh")).alias("eia_res_sales_share"),
        pl.when((pl.col("_c0") > 0) & (pl.col("customers") > 0))
        .then((pl.col("customers") / pl.col("_c0")) ** (1 / span) - 1)
        .alias("eia_customer_cagr"),
    )


def population_growth(population: pl.DataFrame, links: pl.DataFrame, *, start: int, end: int) -> pl.DataFrame:
    """Territory population growth ``start`` → ``end`` (county populations apportioned by area).

    ``population``: ``county_fips``, ``year``, ``value`` (one series/vintage). Returns ``account_id``,
    ``population`` (``end``) and ``pop_growth``.
    """
    wide = population.filter(pl.col("year").is_in([start, end])).pivot(
        on="year", index="county_fips", values="value", aggregate_function="first"
    )
    wide = wide.rename({str(start): "_p0", str(end): "_p1"})
    per = apportion(wide, links, ["_p0", "_p1"])
    return per.select(
        "account_id",
        pl.col("_p1").alias("population"),
        pl.when(pl.col("_p0") > 0).then(pl.col("_p1") / pl.col("_p0") - 1).alias("pop_growth"),
    )


def permits_per_1k(permits: pl.DataFrame, population: pl.DataFrame, links: pl.DataFrame) -> pl.DataFrame:
    """Residential units permitted per 1,000 residents in the territory.

    ``permits``: ``county_fips``, ``units`` (already summed over the chosen years). ``population``:
    ``account_id``, ``population`` (from :func:`population_growth`). Counties without permit-issuing places
    have no row, so an account whose counties all lack one gets null.
    """
    units = apportion(permits.select("county_fips", "units"), links, ["units"])
    return units.join(population.select("account_id", "population"), on="account_id", how="left").select(
        "account_id",
        pl.col("units").alias("permit_units"),
        pl.when(pl.col("population") > 0).then(pl.col("units") / pl.col("population") * 1000).alias("permits_per_1k"),
    )


def owner_single_family(housing: pl.DataFrame, links: pl.DataFrame) -> pl.DataFrame:
    """Owner-occupied single-family homes (ACS B25032 lines 3 and 4) and their share of occupied units.

    ``housing``: ``county_fips``, ``line``, ``estimate`` for one vintage. Returns ``account_id``,
    ``owner_sf_homes`` and ``owner_sf_share``.
    """
    per_county = housing.group_by("county_fips").agg(
        pl.col("estimate").filter(pl.col("line").is_in([3, 4])).sum().alias("_sf"),
        pl.col("estimate").filter(pl.col("line") == 1).sum().alias("_occ"),
    )
    per = apportion(per_county, links, ["_sf", "_occ"])
    return per.select(
        "account_id",
        pl.col("_sf").alias("owner_sf_homes"),
        pl.when(pl.col("_occ") > 0).then(pl.col("_sf") / pl.col("_occ")).alias("owner_sf_share"),
    )


def data_center_sites(sites: pl.DataFrame, links: pl.DataFrame, *, since: date) -> pl.DataFrame:
    """Expected number of new data-center sites in the territory (sites per county × ``county_share``).

    ``sites``: ``county_fips``, ``first_affil_begin_dt``. Counties without a site count as zero (the
    registry covers the whole state), so this is never null for an account with counties.
    """
    per_county = (
        sites.filter(pl.col("first_affil_begin_dt") >= since).group_by("county_fips").agg(pl.len().alias("_n"))
    )
    joined = links.join(per_county, on="county_fips", how="left").with_columns(pl.col("_n").fill_null(0))
    return joined.group_by("account_id").agg((pl.col("_n") * pl.col("county_share")).sum().alias("dc_sites"))


def zone_peak_growth(
    zone_peaks: pl.DataFrame, county_zone: pl.DataFrame, links: pl.DataFrame, *, start: int, end: int
) -> pl.DataFrame:
    """Forecast summer peak CAGR of the territory's weather zones, weighted by overlap area.

    ``zone_peaks``: ``region_id``, ``target_year``, ``value``. ``county_zone``: ``county_fips``,
    ``weather_zone``. ``links`` needs ``overlap_km2``. Counties without a zone are left out.
    """
    span = end - start
    cagr = (
        zone_peaks.filter(pl.col("target_year").is_in([start, end]))
        .pivot(on="target_year", index="region_id", values="value", aggregate_function="first")
        .select(
            pl.col("region_id").alias("weather_zone"),
            ((pl.col(str(end)) / pl.col(str(start))) ** (1 / span) - 1).alias("_g"),
        )
    )
    joined = links.join(county_zone, on="county_fips", how="left").join(cagr, on="weather_zone", how="left")
    return (
        joined.filter(pl.col("_g").is_not_null())
        .group_by("account_id")
        .agg(((pl.col("_g") * pl.col("overlap_km2")).sum() / pl.col("overlap_km2").sum()).alias("zone_peak_cagr"))
    )


def build_signals(
    universe: pl.DataFrame,
    links: pl.DataFrame,
    *,
    eia: pl.DataFrame | None = None,
    population: pl.DataFrame | None = None,
    permits: pl.DataFrame | None = None,
    housing: pl.DataFrame | None = None,
    dc: pl.DataFrame | None = None,
    zone: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """One row per universe account with every signal the inputs allow.

    ``universe``: ``account_id`` (+ ``eia_utility_id`` for the EIA signals). ``links``: the account ×
    county rows. Each keyword is the output of the matching function above (``eia`` keyed by
    ``utility_id``, the others by ``account_id``); a missing input just leaves its columns out.
    """
    out = universe
    if eia is not None:
        out = out.join(eia.rename({"utility_id": "eia_utility_id"}), on="eia_utility_id", how="left")
    for part in (population, permits, housing, dc, zone):
        if part is not None:
            out = out.join(part, on="account_id", how="left")
    ids = set(links["account_id"].unique())
    if dc is not None:
        has_counties = pl.col("account_id").is_in(list(ids))
        out = out.with_columns(pl.when(has_counties).then(pl.col("dc_sites").fill_null(0.0)).alias("dc_sites"))
    return out


# --- signal quality ------------------------------------------------------------------------------------


def signal_summary(
    signals: pl.DataFrame, columns: list[str], *, min_coverage: float = 0.8, max_mode_share: float = 0.9
) -> pl.DataFrame:
    """Coverage and distribution of each signal, and whether it may enter the score.

    ``near_constant``: the most common value (rounded to 6 digits) covers ≥ ``max_mode_share`` of the
    accounts with a value, or there is a single distinct value. ``enters`` = coverage ≥ ``min_coverage``
    and not near constant (``docs/PHASE0_ANALYSIS.md`` §2 Q3).
    """
    n = signals.height
    rows = []
    for c in columns:
        s = signals[c].cast(pl.Float64).drop_nulls()
        k = s.len()
        mode_share = (
            s.round(6).value_counts(sort=True).get_column("count")[0] / k if k else 0.0
        )
        near_constant = k == 0 or s.n_unique() <= 1 or mode_share >= max_mode_share
        coverage = k / n if n else 0.0
        rows.append(
            {
                "signal": c,
                "n": k,
                "coverage": coverage,
                "min": s.min() if k else None,
                "p10": s.quantile(0.1, "linear") if k else None,
                "median": s.median() if k else None,
                "p90": s.quantile(0.9, "linear") if k else None,
                "max": s.max() if k else None,
                "zero_share": float((s == 0).sum()) / k if k else None,
                "mode_share": mode_share,
                "near_constant": near_constant,
                "enters": coverage >= min_coverage and not near_constant,
            }
        )
    return pl.DataFrame(rows)


def yoy_jumps(series: pl.DataFrame, value: str, *, threshold: float = 0.5) -> pl.DataFrame:
    """Year-over-year changes beyond ±``threshold`` per utility (merger, id change or error).

    ``series``: ``utility_id``, ``data_year``, ``value`` (one row per utility-year). Consecutive years only.
    """
    return (
        series.sort("utility_id", "data_year")
        .with_columns(
            pl.col("data_year").shift(1).over("utility_id").alias("prev_year"),
            pl.col(value).shift(1).over("utility_id").alias("prev"),
        )
        .filter((pl.col("data_year") - pl.col("prev_year") == 1) & (pl.col("prev") > 0))
        .with_columns((pl.col(value) / pl.col("prev") - 1).alias("change"))
        .filter(pl.col("change").abs() > threshold)
    )


def redundant_pairs(signals: pl.DataFrame, columns: list[str], *, threshold: float = 0.8) -> pl.DataFrame:
    """Spearman correlation of every signal pair on the accounts that have both; ``redundant`` above
    ``threshold`` in absolute value. Rank correlation because the size signals are heavily skewed."""
    rows = []
    for i, a in enumerate(columns):
        for b in columns[i + 1 :]:
            both = signals.select(a, b).drop_nulls()
            r = both.select(pl.corr(a, b, method="spearman")).item() if both.height >= 3 else None
            rows.append({"a": a, "b": b, "n": both.height, "spearman": r})
    out = pl.DataFrame(rows, schema={"a": pl.Utf8, "b": pl.Utf8, "n": pl.Int64, "spearman": pl.Float64})
    return out.with_columns((pl.col("spearman").abs() > threshold).fill_null(False).alias("redundant"))


def permits_reporting(permits: pl.DataFrame) -> pl.DataFrame:
    """Imputed vs reported permit units per county (``units_total`` vs ``units_total_rep``) over the rows
    given. ``imputed_share`` = 1 − reported / total."""
    return (
        permits.group_by("county_fips")
        .agg(pl.col("units_total").sum().alias("units"), pl.col("units_total_rep").sum().alias("units_rep"))
        .with_columns(
            pl.when(pl.col("units") > 0).then(1 - pl.col("units_rep") / pl.col("units")).alias("imputed_share")
        )
    )


# --- loaders (read-only) -------------------------------------------------------------------------------


def load_accounts() -> pl.DataFrame:
    """ERCOT co-ops and munis from ``puct_ccn_territories`` (account_id = ccn_no)."""
    return read_sql(
        """
        select ccn_no as account_id, territory_id, company_name as name, utility_type as account_type,
               iso_rto, ercot_only, gt_cooperative
        from puct_ccn_territories
        where utility_type in ('coop', 'muni') and in_ercot
        order by utility_type, ccn_no
        """
    )


def load_links() -> pl.DataFrame:
    """Account × county rows of the ERCOT co-op and muni territories (``county_utility_overlap_puct``)."""
    return read_sql(
        """
        select ccn_no as account_id, territory_key as territory_id, county_fips, overlap_km2, county_share,
               territory_share
        from county_utility_overlap_puct
        where utility_type in ('coop', 'muni') and in_ercot
        """
    )


def load_eia_utilities() -> pl.DataFrame:
    """Every Texas EIA-861 utility id with its latest name, type (ownership, else from the name), the years
    it appears in and whether it ever reported sales under ERCOT's balancing authority."""
    ids = read_sql(
        """
        with named as (
            select utility_id, utility_name, data_year, early_release from eia861_utility
            union all select utility_id, utility_name, data_year, early_release from eia861_service_territory
            union all select distinct utility_id, utility_name, data_year, early_release from eia861_sales
        ),
        latest as (
            select distinct on (utility_id) utility_id, utility_name
            from named where utility_id is not null
            order by utility_id, data_year desc, early_release asc
        ),
        owner as (
            select distinct on (utility_id) utility_id, ownership_type
            from (
                select utility_id, ownership_type, data_year, early_release from eia861_utility
                union all select utility_id, ownership, data_year, early_release from eia861_sales
            ) o
            where ownership_type is not null
            order by utility_id, data_year desc, early_release asc
        )
        select l.utility_id, l.utility_name, o.ownership_type,
               (select min(data_year) from named n where n.utility_id = l.utility_id) as first_year,
               (select max(data_year) from named n where n.utility_id = l.utility_id) as last_year,
               exists (select 1 from eia861_sales s where s.utility_id = l.utility_id and s.ba_code = 'ERCO')
                   as ever_erco
        from latest l left join owner o using (utility_id)
        """
    )
    return ids.with_columns(
        pl.coalesce(
            pl.col("ownership_type").replace_strict(EIA_TYPE, default=None, return_dtype=pl.Utf8),
            pl.when(pl.col("ownership_type").is_null()).then(
                pl.col("utility_name").map_elements(infer_type_from_name, return_dtype=pl.Utf8)
            ),
        ).alias("account_type"),
        pl.when(pl.col("ownership_type").is_not_null())
        .then(pl.lit("eia"))
        .otherwise(pl.lit("name"))
        .alias("type_source"),
    )


def load_eia_counties() -> pl.DataFrame:
    """Counties each EIA utility lists in ``Service_Territory``, from its latest year there."""
    return read_sql(
        """
        with last as (
            select utility_id, max(data_year) as data_year from eia861_service_territory group by utility_id
        )
        select distinct s.utility_id, s.county_fips
        from eia861_service_territory s join last using (utility_id, data_year)
        where s.county_fips is not null
        """
    )


def load_eia_sales() -> pl.DataFrame:
    return read_sql(
        """
        select data_year, early_release, utility_id, utility_name, part, ownership, ba_code, sector,
               sales_mwh, customers
        from eia861_sales
        """
    )


def load_population(series: str = "postcensal_v2025") -> pl.DataFrame:
    return read_sql(
        """
        select county_fips, year, value from census_population_county
        where series = %(series)s and measure = 'population'
        """,
        {"series": series},
    )


def load_permits_annual(years: list[int]) -> pl.DataFrame:
    return read_sql(
        """
        select county_fips, year, units_total, units_total_rep from census_permits_county
        where period_type = 'annual' and year = any(%(years)s)
        """,
        {"years": years},
    )


def load_housing(vintage: int = 2024) -> pl.DataFrame:
    return read_sql(
        "select county_fips, line, estimate from census_housing_county where vintage = %(v)s and table_id = 'B25032'",
        {"v": vintage},
    )


def load_dc_sites() -> pl.DataFrame:
    return read_sql("select county_fips, first_affil_begin_dt from tceq_data_center_sites")


def load_zone_peaks(vintage: str = "LTLF 2025", scenario: str = "ercot_adjusted") -> pl.DataFrame:
    return read_sql(
        """
        select region_id, target_year, value from ltlf_forecasts
        where vintage = %(vintage)s and region_type = 'weather_zone' and metric = 'peak_mw'
          and scenario = %(scenario)s and season = 'summer'
        """,
        {"vintage": vintage, "scenario": scenario},
    )


def load_county_zone() -> pl.DataFrame:
    """Weather zone per county: the ERCOT one, else ZipToZone's (counties outside ERCOT that a mixed
    territory still covers)."""
    return read_sql(
        "select county_fips, coalesce(weather_zone, ziptozone_zone) as weather_zone from county_weather_zone"
    )


def read_crosswalk(path: Path) -> pl.DataFrame:
    """``config/utility_crosswalk.yaml`` as a DataFrame (``account_id`` = ``ccn_no``), ready for
    :func:`build_universe`."""
    rows = yaml.safe_load(Path(path).read_text(encoding="utf-8"))["matches"]
    return pl.DataFrame(
        [
            {
                "account_id": str(r["ccn_no"]),
                "eia_utility_id": None if r.get("eia_utility_id") is None else str(r["eia_utility_id"]),
                "status": r["status"],
                "decision": r.get("decision"),
            }
            for r in rows
        ],
        schema={"account_id": pl.Utf8, "eia_utility_id": pl.Utf8, "status": pl.Utf8, "decision": pl.Utf8},
    )
