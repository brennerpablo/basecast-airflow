# %% [markdown]
# # Q2 — Does the account universe close? (`docs/PHASE0_ANALYSIS.md` §2 Q2)
#
# Universe = ERCOT co-ops and munis in the PUCT CCN layers; crosswalk to EIA-861 by normalized name +
# rapidfuzz, confirmed by the EIA `Service_Territory` counties. Writes the draft
# `config/utility_crosswalk.yaml`. Numbers go to `docs/analysis/q2_universe.md`.
#
# Validation lock: the partner check only confirms five names exist; nothing else is computed for them.
#
# Run: `uv run --group analysis python analysis/q2_universe.py`

# %%
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import polars as pl
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, out_path, read_sql  # noqa: E402
from basecast_pipelines.models import accounts as A  # noqa: E402

CROSSWALK_YAML = ROOT / "config" / "utility_crosswalk.yaml"
SALES_YEAR = 2024  # latest final EIA-861 release in the table (2025 is an early release)

# %% [markdown]
# ## 1. Universe counts

# %%
accounts = A.load_accounts()
assert accounts["account_id"].n_unique() == accounts.height, "ccn_no is not unique in the universe"
print("PUCT ERCOT co-ops and munis:")
print(accounts.group_by("account_type").agg(pl.len().alias("n"), pl.col("ercot_only").sum().alias("ercot_only")))
print("all PUCT territories by type and ERCOT:")
print(
    read_sql(
        "select utility_type, in_ercot, count(*) as n from puct_ccn_territories group by 1, 2 order by 1, 2"
    )
)

# %%
eia_counts = read_sql(
    """
    select data_year, early_release, ownership_type, count(*) as n,
           count(*) filter (where ba_codes like '%%ERCO%%') as ba_erco,
           count(*) filter (where rto_ercot) as rto_ercot,
           count(*) filter (where short_form) as short_form_rows
    from eia861_utility
    where ownership_type in ('Cooperative', 'Municipal', 'Political Subdivision')
    group by 1, 2, 3 order by 1, 2, 3
    """
)
print("EIA-861 Utility_Data, Texas co-ops / munis (BA ERCO = ERCO among the BAs of the utility's TX sales):")
print(eia_counts.filter(pl.col("data_year") >= 2018))

# %% [markdown]
# Since 2020 EIA moved small utilities to the short form (`Short_Form_<year>.xlsx`, form 861S), which the
# `eia_861` parser does not read: they appear in `Service_Territory` (ids and counties) but not in
# `eia861_utility` or `eia861_sales`. This cell counts them in the raw zip, if the local lake has it.

# %%
def short_form_tx(year: int) -> pl.DataFrame | None:
    """Texas rows of the raw 861S sheet (projection only; the parser does not load it)."""
    from basecast_pipelines.processing.tabular import read_grid

    hits = sorted((ROOT / "data" / "raw" / "source=eia_861").glob(f"dt=*/f861{year}.zip"))
    if not hits:
        return None
    with zipfile.ZipFile(hits[-1]) as z:
        name = next((n for n in z.namelist() if n.startswith(f"Short_Form_{year}")), None)
        if name is None:
            return None
        grid = read_grid(z.read(name), 0)
    header = [str(v) for v in grid.row(0)]
    body = grid.slice(1).rename(dict(zip(grid.columns, header, strict=True)))
    return body.filter(pl.col("State") == "TX").select(
        pl.col("Utility Number").alias("utility_id"),
        pl.col("Utility Name").alias("utility_name"),
        pl.col("Ownership").alias("ownership"),
        pl.col("BA Code").alias("ba_code"),
        pl.col("Total Sales (MWh)").cast(pl.Float64, strict=False).alias("sales_mwh"),
        pl.col("Total Customers").cast(pl.Float64, strict=False).alias("customers"),
    )


short_form = short_form_tx(SALES_YEAR)
if short_form is None:
    print("Short_Form: raw zip not in the local lake, skipped")
else:
    print(f"Short_Form_{SALES_YEAR} Texas rows by ownership and BA:")
    print(short_form.group_by("ownership", "ba_code").len().sort("ownership", "ba_code"))
    short_form.write_csv(out_path(f"q2_short_form_{SALES_YEAR}_tx.csv"))

# %% [markdown]
# ## 2. Crosswalk PUCT ↔ EIA-861

# %%
links = A.load_links()
eia = A.load_eia_utilities()
eia_counties = A.load_eia_counties()
candidates = eia.filter(pl.col("account_type").is_in(list(A.ACCOUNT_TYPES)))
print("EIA-861 Texas ids by inferred type:", candidates.group_by("account_type", "type_source").len().rows())

overlap = A.county_overlap(links.select("account_id", "county_fips", "overlap_km2"), eia_counties)
crosswalk = A.match_utilities(accounts, candidates, overlap)
print(crosswalk["status"].value_counts())
print("score distribution (best candidate per account):")
print(A.score_bins(crosswalk["score"]))
print("auto rows with a runner-up ≥ 90 (ties broken by county overlap / state tag):")
print(
    crosswalk.filter(pl.col("runner_up_score") >= A.AUTO_SCORE).select(
        "account_id", "eia_utility_name", "county_overlap", "runner_up_name"
    )
)
print("min county_overlap among auto rows:", crosswalk.filter(pl.col("status") == "auto")["county_overlap"].min())

# %%
review = crosswalk.filter(pl.col("status") == "review")
print("[PERGUNTAR] review rows (score < 90 or no shared county):")
print(
    review.select(
        "account_id", "name", "eia_utility_name", "score", "county_overlap", "geo_utility_id",
        "geo_utility_name", "geo_overlap",
    )
)

# %% [markdown]
# Claude's default for each review row (pending Pablo). Accept = take the utility with the largest county
# overlap (`geo_*`) when it covers ≥ 95% of the territory's area and the name difference has a known reason.

# %%
PROPOSALS: dict[str, tuple[str, str]] = {
    "30036": ("accept", "EIA spells it 'Comanche County Elec Coop Assn'; same counties (overlap 1.00)."),
    "30040": (
        "accept",
        "PenTex Energy is the new name of Cooke County Electric Cooperative Association (rename not verified "
        "at the source); EIA still files it as 'Cooke County Elec Coop Assn'; overlap 1.00.",
    ),
    "30047": (
        "accept",
        "CoServ is the trade name of Denton County Electric Cooperative, the name EIA files under; "
        "overlap 1.00.",
    ),
    "30022": (
        "accept",
        "BTU (Bryan Texas Utilities) is the City of Bryan's utility; EIA files one id, 'City of Bryan', that "
        "presumably also covers the city system the PUCT muni layer does not list (not verified); overlap 1.00.",
    ),
    "30031": (
        "accept",
        "CPS Energy is owned by the City of San Antonio, the name EIA files under; overlap 1.00.",
    ),
}
proposed = review.select(
    "account_id",
    pl.col("account_id").replace_strict({k: v[0] for k, v in PROPOSALS.items()}, default="reject").alias("decision"),
    pl.col("account_id").replace_strict({k: v[1] for k, v in PROPOSALS.items()}, default="no proposal").alias("reason"),
    pl.col("geo_utility_id").alias("proposed_utility_id"),
    pl.col("geo_utility_name").alias("proposed_utility_name"),
)
unexpected = set(review["account_id"]) - set(PROPOSALS)
assert not unexpected, f"review rows without a proposal: {sorted(unexpected)}"

# The accepted review rows take the proposed (county-overlap) utility.
crosswalk = (
    crosswalk.join(proposed, on="account_id", how="left")
    .with_columns(
        pl.when(pl.col("decision") == "accept")
        .then(pl.col("proposed_utility_id"))
        .otherwise(pl.col("eia_utility_id"))
        .alias("eia_utility_id"),
        pl.when(pl.col("decision") == "accept")
        .then(pl.col("proposed_utility_name"))
        .otherwise(pl.col("eia_utility_name"))
        .alias("eia_utility_name"),
        pl.when(pl.col("decision") == "accept")
        .then(pl.col("geo_overlap"))
        .otherwise(pl.col("county_overlap"))
        .alias("county_overlap"),
        pl.when(pl.col("decision") == "accept").then(pl.col("geo_score")).otherwise(pl.col("score")).alias("score"),
    )
)

# %% [markdown]
# ## Decision

# %%
n = accounts.height
auto = int((crosswalk["status"] == "auto").sum())
accepted = int(((crosswalk["status"] == "auto") | (crosswalk["decision"] == "accept")).sum())
by_type = crosswalk.group_by("account_type").agg(
    pl.len().alias("n"),
    (pl.col("status") == "auto").sum().alias("auto"),
    ((pl.col("status") == "auto") | (pl.col("decision") == "accept")).sum().alias("matched"),
)
print(by_type.sort("account_type"))
print(f"matched ≥ 90 (auto): {auto}/{n} = {auto / n:.1%}; "
      f"after the proposed review: {accepted}/{n} = {accepted / n:.1%}")
GO = accepted / n >= 0.8
print("DECISION:", "go — universe PUCT ∩ EIA" if GO else "plan B — PUCT-only universe, no EIA-861 signals")

# %% [markdown]
# What the matched ids carry: long-form sales rows in `eia861_sales` (final `SALES_YEAR`), or only the
# short form (not parsed).

# %%
universe = A.build_universe(accounts, crosswalk, intersect=GO)
with_sales = read_sql(
    """
    select distinct utility_id from eia861_sales
    where data_year = %(y)s and not early_release and sector = 'total'
    """,
    {"y": SALES_YEAR},
)["utility_id"]
universe = universe.with_columns(pl.col("eia_utility_id").is_in(with_sales.to_list()).alias("has_sales"))
if short_form is not None:
    universe = universe.with_columns(
        pl.col("eia_utility_id").is_in(short_form["utility_id"].to_list()).alias("in_short_form")
    )
cols = ["has_sales"] + (["in_short_form"] if short_form is not None else [])
print(universe.group_by("account_type").agg(pl.len().alias("n"), *[pl.col(c).sum() for c in cols]).sort("account_type"))
print(
    "universe with long-form EIA sales:",
    f"{universe['has_sales'].sum()}/{universe.height} = {universe['has_sales'].mean():.1%}",
)
if short_form is not None:
    either = (universe["has_sales"] | universe["in_short_form"]).mean()
    print(f"with sales if the parser also read Short_Form_{SALES_YEAR}: {either:.1%}")

# %% [markdown]
# EIA-861 ERCOT co-ops / munis that no PUCT account took (reverse check).

# %%
erco_ids = read_sql(
    """
    select distinct utility_id, utility_name, ownership_type from eia861_utility
    where data_year = %(y)s and not early_release and ba_codes like '%%ERCO%%'
      and ownership_type in ('Cooperative', 'Municipal', 'Political Subdivision')
    """,
    {"y": SALES_YEAR},
)
taken = set(crosswalk.filter((pl.col("status") == "auto") | (pl.col("decision") == "accept"))["eia_utility_id"])
print(f"EIA {SALES_YEAR} ERCO co-ops/munis: {erco_ids.height}; not in the crosswalk:")
print(erco_ids.filter(~pl.col("utility_id").is_in(list(taken))).sort("ownership_type", "utility_name"))

# %% [markdown]
# ## 3. Partner presence (names only)

# %%
PARTNER_NAMES = [
    "Bandera Electric Cooperative",
    "Guadalupe Valley Electric Cooperative (GVEC)",
    "CoServ",
    "Farmers Electric Cooperative",
    "Austin Energy",
]
present = A.find_names(accounts["name"], [w.replace(" (GVEC)", "") for w in PARTNER_NAMES])
in_universe = present.with_columns(
    pl.col("found").is_in(accounts.join(universe.select("account_id"), on="account_id")["name"].to_list()).alias(
        "in_universe"
    )
).select("wanted", pl.col("found").is_not_null().alias("in_puct"), "in_universe")
print(in_universe)
assert in_universe["in_puct"].all(), "a partner name is missing from the PUCT universe"

# %% [markdown]
# ## Draft crosswalk YAML

# %%
HEADER = """\
# DRAFT — pending Pablo's review. PUCT CCN territory ↔ EIA-861 utility id crosswalk for the account
# universe (ERCOT co-ops and munis), generated by analysis/q2_universe.py on 2026-09-26.
# score: rapidfuzz token_sort_ratio (0-100) of the normalized core names (basecast_pipelines/models/accounts.py).
# county_overlap: share of the PUCT territory's area in counties the EIA utility lists in Service_Territory.
# status: auto = score >= 90 and county_overlap > 0; review = anything else, with Claude's proposed
#         decision (accept/reject) and reason. Pablo confirms or edits the review rows.
"""


def _row(r: dict) -> dict:
    row = {
        "ccn_no": r["account_id"],
        "puct_name": r["name"],
        "utility_type": r["account_type"],
        "eia_utility_id": r["eia_utility_id"],
        "eia_utility_name": r["eia_utility_name"],
        "score": round(float(r["score"]), 1),
        "county_overlap": round(float(r["county_overlap"]), 3),
        "status": r["status"],
    }
    if r["status"] == "review":
        row["decision"] = r["decision"]
        row["reason"] = r["reason"]
    return row


doc = {"matches": [_row(r) for r in crosswalk.sort("account_type", "account_id").iter_rows(named=True)]}
CROSSWALK_YAML.write_text(HEADER + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120))
print(f"wrote {CROSSWALK_YAML.relative_to(ROOT)} ({len(doc['matches'])} rows)")
crosswalk.write_csv(out_path("q2_crosswalk.csv"))
