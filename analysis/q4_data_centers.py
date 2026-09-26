"""Q4: do the new data centers land in co-op territory, outside the metros? (docs/PHASE0_ANALYSIS.md §2)

Run from the repo root: ``uv run --group analysis python analysis/q4_data_centers.py``. Read-only. The
attribution is by county, not by point: a site takes its county's territory mix. Results go to
``docs/analysis/q4_data_centers.md``; the site table to ``analysis/out/q4_sites.csv``.
"""

# %% Load
from __future__ import annotations

import polars as pl

from analysis._common import out_path
from basecast_pipelines.models import data_centers as dc

sites_all = dc.load_sites()
overlap = dc.load_overlap()
counties = dc.load_counties()
density = dc.county_density(dc.load_population(2025), counties)
iso = dc.load_iso_share()
print("sites in tceq_data_center_sites:", sites_all.height)

# %% 1. Site selection: first known permit since 2025-01-01
new = dc.select_new_sites(sites_all)
print(f"since {dc.SINCE}: {new.height} sites in {new['county_fips'].n_unique()} counties (reference: 38 in 27)")
print("selected sites that also have an undated row (has_undated_affiliation):")
print(new.filter(pl.col("has_undated_affiliation")).select("ref_num_txt", "reg_ent_name", "county_name",
                                                          "first_affil_begin_dt"))
print("sites with no dated row at all (cannot be placed; left out):")
print(dc.fully_undated_sites(sites_all).select("ref_num_txt", "reg_ent_name", "county_name"))
print("match source among the new sites:")
print(new.group_by("matched_by_name", "matched_by_naics").len().sort("len", descending=True))

# %% 2. Territory type per county (largest area and county_share weights), by county, not by point
types = dc.county_territory_types(overlap)
classified = dc.classify_sites(new, types)
summary = dc.type_summary(classified)
print(summary)
print("raw county_share sums of the site counties (overlapping CCNs push them above 1):")
print(classified.select(pl.col("share_sum").min().alias("min"), pl.col("share_sum").median().alias("median"),
                        pl.col("share_sum").max().alias("max")))
# Upper bound without the normalization: the co-op raw share clipped to 1 (co-op/co-op overlaps double count).
coop_upper = float(classified["coop_raw"].fill_null(0).clip(upper_bound=1.0).mean())
print(f"co-op share, raw county_share clipped to 1 (upper bound): {coop_upper:.1%}")

# ERCOT subset: the commercial module only ranks ERCOT co-ops and munis. county_iso_share's dominant class.
classified = classified.join(iso, on="county_fips", how="left")
ercot = classified.filter(pl.col("dominant_iso_rto").str.contains("ERCOT"))
ercot_summary = dc.type_summary(ercot)
print(f"sites in counties whose dominant ISO class includes ERCOT: {ercot.height} in"
      f" {ercot['county_fips'].n_unique()} counties")
print(ercot_summary)
print("outside ERCOT:", classified.filter(~pl.col("dominant_iso_rto").str.contains("ERCOT"))
      .select("reg_ent_name", "county_name", "dominant_iso_rto", "largest_type").rows())

# Point-level hint the county attribution misses: a site whose city names a muni of its county.
hint = dc.city_muni_match(classified, overlap)
print("sites whose city matches a muni name in the county:",
      hint.filter(pl.col("city_muni").is_not_null()).select("reg_ent_name", "city", "city_muni").rows())
print("sites with a city:", classified.filter(pl.col("city").is_not_null()).height, "of", classified.height)

# %% 3. Metro vs non-metro: the decision's 13-county list (reproduced) and the density fallback
flagged = dc.flag_metro(classified, density)
# The decision counted 1800-01-01 as a site's earliest date, so the sites with an undated row fell before 2025.
legacy_prior = dc.flag_metro(new.filter(~pl.col("has_undated_affiliation")), density)
print(f"legacy selection (undated rows as 1800-01-01): {legacy_prior.height} sites in"
      f" {legacy_prior['county_fips'].n_unique()} counties, {legacy_prior.filter(~pl.col('metro_legacy')).height}"
      " outside the 13-county list (decision: 37 in 26, 34 outside)")
for label, col in (("legacy 13-county list", "metro_legacy"),
                   (f"density >= {dc.DENSITY_THRESHOLD_PER_KM2:.0f}/km2", "metro_density")):
    outside = flagged.filter(~pl.col(col)).height
    print(f"{label}: outside metro {outside}/{flagged.height} = {outside / flagged.height:.1%}")
for t in (50.0, 250.0):
    alt = dc.flag_metro(classified, density, threshold=t)
    print(f"  sensitivity density >= {t:.0f}/km2: outside {alt.filter(~pl.col('metro_density')).height}")
print("density rule marks metro, legacy list does not (or the reverse):")
print(flagged.filter(pl.col("metro_legacy") != pl.col("metro_density"))
      .select("county_name", "density_per_km2", "metro_legacy", "metro_density").unique().sort("county_name"))

# %% 4. Co-op and outside metro together; decision
coop_w = float(summary.filter(pl.col("type") == "coop")["share_weighted"][0])
coop_area = float(summary.filter(pl.col("type") == "coop")["share_largest_area"][0])
outside_legacy = flagged.filter(~pl.col("metro_legacy")).height / flagged.height
outside_density = flagged.filter(~pl.col("metro_density")).height / flagged.height
both = flagged.filter((pl.col("largest_type") == "coop") & ~pl.col("metro_density")).height
both_legacy = flagged.filter((pl.col("largest_type") == "coop") & ~pl.col("metro_legacy")).height
print(f"co-op weighted {coop_w:.1%}, co-op largest-area {coop_area:.1%};"
      f" outside metro legacy {outside_legacy:.1%}, density {outside_density:.1%};"
      f" co-op largest-area AND non-metro: density {both}, legacy {both_legacy}")
print("co-op weighted among non-metro sites (density):",
      f"{float(flagged.filter(~pl.col('metro_density'))['coop_w'].mean()):.1%}")
e = flagged.filter(pl.col("dominant_iso_rto").str.contains("ERCOT"))
print(f"ERCOT counties: co-op weighted {float(e['coop_w'].mean()):.1%},"
      f" outside metro (density) {e.filter(~pl.col('metro_density')).height}/{e.height}")
print("decision (legacy metro):", dc.decide(coop_w, outside_legacy))
print("decision (density metro):", dc.decide(coop_w, outside_density))
print("decision (upper-bound co-op share, legacy metro):", dc.decide(coop_upper, outside_legacy))
# NAICS-only matches (518210) include names that may not be data centers (e.g. "... ENERGY CENTER").
named = flagged.filter(pl.col("matched_by_name"))
print(f"name-matched only: {named.height} sites, co-op weighted {float(named['coop_w'].mean()):.1%},"
      f" outside metro (density) {named.filter(~pl.col('metro_density')).height}")

# %% 5. Site list for the doc
site_list = dc.city_muni_match(flagged, overlap).select(
    "first_affil_begin_dt", "ref_num_txt", "reg_ent_name", "county_name", "county_fips", "city", "city_muni",
    "largest_type", pl.col("coop_w").round(2), pl.col("muni_w").round(2), pl.col("iou_w").round(2),
    "dominant_iso_rto", pl.col("density_per_km2").round(0), "metro_legacy", "metro_density",
    "has_undated_affiliation", "matched_by_name", "matched_by_naics", "statuses",
).sort("first_affil_begin_dt", "ref_num_txt")
site_list.write_csv(out_path("q4_sites.csv"))
with pl.Config(tbl_rows=60, fmt_str_lengths=40):
    print(site_list.drop("ref_num_txt", "county_fips", "statuses"))
