# %% [markdown]
# # X10 — City-level (Census place) signals for the municipal utilities
#
# 60 of the 112 accounts are munis. Every territory signal today is county data apportioned by area, which
# misreads cities. This script crosswalks munis to Census incorporated places (name, checked against the CCN
# polygon), builds place-level population, permits and housing, compares them with the apportioned values and
# EIA meters (X4), and tests city-mapped triggers. Numbers go to `docs/analysis/x10_muni_places.md`; logic in
# `basecast_pipelines/models/muni_places.py`.
#
# Census files are fetched once into `analysis/out/x10_cache/` (gitignored); `x10_sources.csv` records each URL
# with its fetch time. Nothing is written to `data/` or to the database (read-only queries only).
#
# Validation lock: the five partner accounts are held out **before** anything is computed, as in
# `q3_signals.py`. Every statistic, rank and example covers the other 107 accounts (59 munis).
#
# Run: `uv run --group analysis python analysis/x10_muni_places.py`.

# %%
from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import polars as pl
import shapefile  # pyshp
from scipy.stats import spearmanr

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, out_path, read_sql  # noqa: E402
from basecast_pipelines.models import accounts as A  # noqa: E402
from basecast_pipelines.models import data_centers as DC  # noqa: E402
from basecast_pipelines.models import eia861_short_form as S  # noqa: E402
from basecast_pipelines.models import muni_places as M  # noqa: E402
from basecast_pipelines.models import triggers as T  # noqa: E402

CACHE = ROOT / "analysis" / "out" / "x10_cache"
RAW = ROOT / "data" / "raw"
AS_OF = T.AS_OF
POP_START, POP_END = 2020, 2025
PERMIT_YEARS = [2023, 2024, 2025]
EIA_YEAR, EIA_BASE = 2024, 2019
ZONE_START, ZONE_END = 2025, 2031
DC_SINCE = date(2025, 1, 1)
GEN_FUELS = ("storage", "gas")

BPS = "https://www2.census.gov/econ/bps/Place/South%20Region/"
SOURCES = {
    "sub-est2025_48.csv": "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025_48.csv",
    "SUB-EST2025.pdf": "https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2025/SUB-EST2025.pdf",
    "cb_2024_48_place_500k.zip": "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_48_place_500k.zip",
    **{f"so{y}a.txt": f"{BPS}so{y}a.txt" for y in (2022, 2023, 2024, 2025)},
    **{f"so{m}y.txt": f"{BPS}so{m}y.txt" for m in ("2508", "2608")},
}
# Already in the lake (census_acs, dt=2026-09-25): the ACS summary file carries every geography, places included.
ACS_FILE = RAW / "source=census_acs" / "dt=2026-09-25" / "acsdt5y2024-b25032.dat"
ACS_URL = "https://www2.census.gov/programs-surveys/acs/summary_file/2024/table-based-SF/data/5YRData/acsdt5y2024-b25032.dat"

# %% [markdown]
# ## Census files (fetched once, cached, logged)

# %%
CACHE.mkdir(parents=True, exist_ok=True)
log_path = CACHE / "fetch_log.json"
log = json.loads(log_path.read_text()) if log_path.exists() else {}
with httpx.Client(headers={"User-Agent": "basecast-analysis/0.1 (X10)"}, timeout=120, follow_redirects=False) as http:
    for name, url in SOURCES.items():
        path = CACHE / name
        if not path.exists():
            r = http.get(url)
            r.raise_for_status()
            path.write_bytes(r.content)
            log[name] = {"url": url, "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"), "status": r.status_code}
        log.setdefault(name, {"url": url, "fetched_at": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(timespec="seconds"), "status": 200})
log_path.write_text(json.dumps(log, indent=2))
acs_manifest = json.loads((ACS_FILE.parent / "_manifest.json").read_text())
acs_entry = next(e for e in acs_manifest["entries"] if e["file"] == ACS_FILE.name)
sources = pl.DataFrame(
    [
        {"file": n, "url": v["url"], "fetched_at": v["fetched_at"], "bytes": (CACHE / n).stat().st_size,
         "sha256": hashlib.sha256((CACHE / n).read_bytes()).hexdigest()}
        for n, v in log.items()
    ]
    + [{"file": ACS_FILE.name, "url": acs_entry["url"], "fetched_at": acs_entry["fetched_at"],
        "bytes": acs_entry["bytes"], "sha256": acs_entry["sha256"]}]
)
sources.write_csv(out_path("x10_sources.csv"))
print(sources.select("file", "fetched_at", "bytes"))

# %% [markdown]
# ## Universe (Q2) minus the held-out partner accounts (same approach as Q3)

# %%
accounts = A.load_accounts()
crosswalk = A.read_crosswalk(ROOT / "config" / "utility_crosswalk.yaml")
universe_all = A.build_universe(accounts, crosswalk, intersect=True)
PARTNER_NAMES = [
    "Bandera Electric Cooperative",
    "Guadalupe Valley Electric Cooperative",
    "CoServ",
    "Farmers Electric Cooperative",
    "Austin Energy",
]
held_names = A.find_names(accounts["name"], PARTNER_NAMES)["found"].drop_nulls().to_list()
held_ids = accounts.filter(pl.col("name").is_in(held_names))["account_id"].to_list()
assert len(held_ids) == len(PARTNER_NAMES), "partner hold-out did not find every name"
universe = universe_all.filter(~pl.col("account_id").is_in(held_ids))
ids = universe["account_id"].to_list()
N = universe.height
names = universe.join(accounts.select("account_id", "name"), on="account_id").select(
    "account_id", "name", "account_type", "eia_utility_id"
)
munis = names.filter(pl.col("account_type") == "muni")
muni_ids = munis["account_id"].to_list()
print(f"universe {universe_all.height}; held out {len(held_ids)}; analysed {N}; munis {munis.height}")
links = A.load_links().filter(pl.col("account_id").is_in(ids))

# %% [markdown]
# ## Place tables (PEP, BPS, ACS)

# %%
pep = M.parse_pep_places((CACHE / "sub-est2025_48.csv").read_text(encoding="latin-1"))
places = pep.select("place_fips", "place_name", "funcstat").unique()
print("incorporated places (PEP 2025):", places.height, "| functional status:", places["funcstat"].value_counts().rows())


def bps(name: str) -> pl.DataFrame:
    return M.parse_bps_places((CACHE / name).read_text(encoding="latin-1"))


bps_annual = pl.concat(
    [bps(f"so{y}a.txt").with_columns(pl.lit(y).alias("year")) for y in (2022, 2023, 2024, 2025)]
)
with ACS_FILE.open(encoding="latin-1") as f:
    acs = M.parse_acs_places(f)
print("BPS TX rows per year:", bps_annual.group_by("year").len().sort("year").rows(),
      "| ACS TX places (incl. CDPs):", acs.height)
psig = M.place_signals(pep, bps_annual, acs, start=POP_START, end=POP_END, permit_years=PERMIT_YEARS)

# %% [markdown]
# ## 1. Crosswalk munis → places (name, then the CCN polygon)

# %%
eia_names = A.load_eia_utilities().select(pl.col("utility_id").cast(pl.Utf8).alias("eia_utility_id"), "utility_name")
cand = pl.concat(
    [
        munis.select("account_id", "name", pl.lit("puct").alias("matched_on"), pl.lit(0).alias("priority")),
        munis.join(eia_names, on="eia_utility_id", how="inner").select(
            "account_id", pl.col("utility_name").alias("name"), pl.lit("eia").alias("matched_on"), pl.lit(1).alias("priority")
        ),
    ]
)
xw = M.match_places(cand, places).join(munis.select("account_id", "name", "eia_utility_id"), on="account_id")
print("name matches:", xw["place_fips"].is_not_null().sum(), "of", xw.height,
      "| by source:", xw["matched_on"].value_counts().rows())
print("no name match:", xw.filter(pl.col("place_fips").is_null()).join(
    cand.filter(pl.col("matched_on") == "eia"), on="account_id", how="left").select("name", "name_right").rows())

# Place polygons (cartographic boundary 2024, NAD83 ≈ WGS84 for areas) of every incorporated place whose bbox
# touches a muni territory; overlap is computed server-side in PostGIS, read-only.
ext = read_sql(
    """
    select ccn_no as account_id, st_xmin(e) x0, st_ymin(e) y0, st_xmax(e) x1, st_ymax(e) y1
    from (select ccn_no, st_extent(geom)::geometry e from puct_ccn_territories
          where ccn_no = any(%(ids)s) group by ccn_no) s
    """,
    {"ids": muni_ids},
)
boxes = ext.rows(named=True)
with zipfile.ZipFile(CACHE / "cb_2024_48_place_500k.zip") as z:
    stem = "cb_2024_48_place_500k"
    sf = shapefile.Reader(shp=z.open(f"{stem}.shp"), shx=z.open(f"{stem}.shx"), dbf=z.open(f"{stem}.dbf"))
    fields = [f[0] for f in sf.fields[1:]]
    geo = []
    for sr in sf.iterShapeRecords():
        rec = dict(zip(fields, sr.record, strict=True))
        x0, y0, x1, y1 = sr.shape.bbox
        if any(x0 <= b["x1"] and x1 >= b["x0"] and y0 <= b["y1"] and y1 >= b["y0"] for b in boxes):
            geo.append({"place_fips": rec["GEOID"], "lsad": rec["LSAD"], "gj": json.dumps(sr.shape.__geo_interface__)})
print("candidate place polygons near muni territories:", len(geo))
inc = set(places["place_fips"])
geo_inc = [g for g in geo if g["place_fips"] in inc]  # incorporated only (CDPs are not cities)
ov = read_sql(
    """
    with p as (
        select place_fips, st_makevalid(st_setsrid(st_geomfromgeojson(gj), 4326)) g
        from jsonb_to_recordset(%(p)s::jsonb) as x(place_fips text, gj text)
    ), t as (
        select ccn_no, st_makevalid(st_union(geom)) g from puct_ccn_territories
        where ccn_no = any(%(ids)s) group by ccn_no
    )
    select t.ccn_no as account_id, p.place_fips,
           st_area(t.g::geography) / 1e6 as territory_km2,
           st_area(p.g::geography) / 1e6 as place_km2,
           st_area(st_intersection(t.g, p.g)::geography) / 1e6 as inter_km2
    from t join p on st_intersects(t.g, p.g)
    """,
    {"p": json.dumps([{"place_fips": g["place_fips"], "gj": g["gj"]} for g in geo_inc]), "ids": muni_ids},
)
terr_km2 = read_sql(
    "select ccn_no as account_id, st_area(st_union(geom)::geography) / 1e6 as territory_km2 "
    "from puct_ccn_territories where ccn_no = any(%(ids)s) group by ccn_no",
    {"ids": muni_ids},
)
pk = ov.select("place_fips", "place_km2").unique()
checked = M.classify_overlap(
    xw.join(terr_km2, on="account_id", how="left")
    .join(pk, on="place_fips", how="left")
    .join(ov.select("account_id", "place_fips", "inter_km2"), on=["account_id", "place_fips"], how="left")
    .with_columns(pl.when(pl.col("place_fips").is_not_null()).then(pl.col("inter_km2").fill_null(0.0)).alias("inter_km2"))
)
# The best spatial place (largest share of the place inside the territory, among places ≥ 1 km² overlap)
best = (
    M.classify_overlap(ov)
    .filter(pl.col("inter_km2") >= 0.5)
    .sort("inter_km2", descending=True)
    .unique(subset=["account_id"], keep="first")
    .join(places.select("place_fips", "place_name"), on="place_fips")
    .select("account_id", pl.col("place_fips").alias("best_fips"), pl.col("place_name").alias("best_place"),
            pl.col("place_in_territory").alias("best_place_in_territory"))
)
checked = checked.join(best, on="account_id", how="left").with_columns(
    (pl.col("best_fips") == pl.col("place_fips")).alias("name_is_best_spatial")
)
checked.write_csv(out_path("x10_crosswalk.csv"))
print(checked["overlap_status"].value_counts().sort("overlap_status"))
print(checked["fit"].value_counts().sort("fit"))
print("name match = best spatial place:", checked["name_is_best_spatial"].sum(), "of", checked.height)
with pl.Config(float_precision=3, tbl_rows=70):
    print(checked.select("name", "place_name", "matched_on", "overlap_status", "fit", "place_in_territory",
                         "territory_in_place", "territory_km2", "place_km2", "best_place").sort("place_in_territory"))
q = checked["territory_in_place"].drop_nulls()
print("territory inside its city: p10/median/p90",
      [round(q.quantile(x, "linear"), 3) for x in (0.1, 0.5, 0.9)])
# Accounts whose place is confirmed carry place signals; the rest keep the county ones.
confirmed = checked.filter(pl.col("overlap_status") == "confirmed").select("account_id", "place_fips", "place_name")
print("confirmed:", confirmed.height, "of", munis.height)

# %% [markdown]
# ## 2. Place signals vs area-apportioned signals vs EIA meters

# %%
pop_growth = A.population_growth(A.load_population(), links, start=POP_START, end=POP_END)
permits = A.load_permits_annual(PERMIT_YEARS)
permit_units = permits.group_by("county_fips").agg(pl.col("units_total").sum().alias("units"))
county = (
    pop_growth.join(A.permits_per_1k(permit_units, pop_growth, links), on="account_id", how="left")
    .join(A.owner_single_family(A.load_housing(), links), on="account_id", how="left")
)
# Occupied units apportioned the same way (for units per meter)
occ_c = A.apportion(
    A.load_housing().filter(pl.col("line") == 1).select("county_fips", pl.col("estimate").alias("occupied_units")),
    links, ["occupied_units"],
)
county = county.join(occ_c, on="account_id", how="left")

long_tot = S.long_form_totals(S.load_long_sales())
short_tot = S.short_form_totals(S.load_short_form(RAW))
delivery = S.load_delivery(RAW)
eia = S.add_delivery(
    S.with_price(S.combine_forms(long_tot, short_tot)), delivery, first_year=int(delivery["data_year"].min())
)
eia_sig = S.utility_signals(eia, year=EIA_YEAR, base_year=EIA_BASE, customers="wires_customers").select(
    pl.col("utility_id").cast(pl.Utf8).alias("eia_utility_id"), "eia_customers", "eia_customer_cagr", "eia_form"
)
eia_2020 = S.utility_signals(eia, year=EIA_YEAR, base_year=2020, customers="wires_customers").select(
    pl.col("utility_id").cast(pl.Utf8).alias("eia_utility_id"), pl.col("eia_customer_cagr").alias("meter_cagr_2020_24")
)
PLACE_COLS = ["population", "pop_growth", "permit_units", "permits_per_1k", "occupied_units", "owner_sf_homes",
              "owner_sf_share"]
pop_2024 = pep.filter(pl.col("year").is_in([2020, 2024])).pivot(on="year", index="place_fips", values="population")
psig2 = psig.join(
    pop_2024.select("place_fips", ((pl.col("2024") / pl.col("2020")) ** (1 / 4) - 1).alias("plc_pop_cagr_2020_24")),
    on="place_fips", how="left",
)
cmp = (
    names.join(county.rename({c: f"cty_{c}" for c in PLACE_COLS}), on="account_id", how="left")
    .join(confirmed.select("account_id", "place_fips", "place_name"), on="account_id", how="left")
    .join(checked.select("account_id", "fit"), on="account_id", how="left")
    .join(psig2.rename({c: f"plc_{c}" for c in PLACE_COLS}), on="place_fips", how="left")
    .join(eia_sig, on="eia_utility_id", how="left")
    .join(eia_2020, on="eia_utility_id", how="left")
    .join(pop_growth.select("account_id"), on="account_id", how="left")
)
# county pop CAGR 2020→2024 for the same-window check
cp = A.population_growth(A.load_population(), links, start=2020, end=2024).select(
    "account_id", ((1 + pl.col("pop_growth")) ** (1 / 4) - 1).alias("cty_pop_cagr_2020_24")
)
cmp = cmp.join(cp, on="account_id", how="left")
for src in ("cty", "plc"):
    cmp = cmp.with_columns(
        (pl.col(f"{src}_owner_sf_homes") / pl.col("eia_customers")).alias(f"{src}_sf_per_meter"),
        (pl.col(f"{src}_occupied_units") / pl.col("eia_customers")).alias(f"{src}_occ_per_meter"),
        (pl.col(f"{src}_population") / pl.col("eia_customers")).alias(f"{src}_pop_per_meter"),
    )
cmp.write_csv(out_path("x10_signals.csv"))


def dist(s: pl.Series) -> str:
    s = s.drop_nulls()
    if s.is_empty():
        return "—"
    return f"{s.median():.2f} [{s.quantile(0.1, 'linear'):.2f}–{s.quantile(0.9, 'linear'):.2f}] n={s.len()}"


m = cmp.filter(pl.col("account_type") == "muni", pl.col("place_fips").is_not_null())
c = cmp.filter(pl.col("account_type") == "coop")
print("Ratios per EIA meter (2024 wires customers), median [p10–p90]:")
for k in ("sf_per_meter", "occ_per_meter", "pop_per_meter"):
    print(f"  {k:14s} munis county: {dist(m[f'cty_{k}'])} | munis place: {dist(m[f'plc_{k}'])} | co-ops county: {dist(c[f'cty_{k}'])}")
for fit in ("same", "city_larger", "territory_larger"):
    f = m.filter(pl.col("fit") == fit)
    print(f"  fit={fit}: occupied units per meter place {dist(f['plc_occ_per_meter'])} | county {dist(f['cty_occ_per_meter'])}")
within = m.filter(pl.col("plc_occ_per_meter").is_between(0.6, 1.2))
print("munis with place occupied units per meter in 0.6–1.2:", within.height, "of", m["plc_occ_per_meter"].drop_nulls().len())


def rho(df: pl.DataFrame, a: str, b: str) -> tuple[float, int]:
    d = df.select(a, b).drop_nulls()
    return (round(float(spearmanr(d[a], d[b]).statistic), 3), d.height) if d.height > 2 else (float("nan"), d.height)


print("Spearman among the munis with a confirmed place:")
for a, b in [
    ("eia_customers", "cty_owner_sf_homes"), ("eia_customers", "plc_owner_sf_homes"),
    ("eia_customers", "cty_population"), ("eia_customers", "plc_population"),
    ("eia_customer_cagr", "cty_pop_growth"), ("eia_customer_cagr", "plc_pop_growth"),
    ("meter_cagr_2020_24", "cty_pop_cagr_2020_24"), ("meter_cagr_2020_24", "plc_pop_cagr_2020_24"),
    ("cty_pop_growth", "plc_pop_growth"), ("cty_permits_per_1k", "plc_permits_per_1k"),
    ("cty_owner_sf_share", "plc_owner_sf_share"), ("eia_customer_cagr", "plc_permits_per_1k"),
    ("eia_customer_cagr", "cty_permits_per_1k"),
]:
    print(f"  {a} × {b}: {rho(m, a, b)}")
print("growth medians, munis: meters 2019→24 CAGR", dist(m["eia_customer_cagr"]),
      "| county pop 2020→25", dist(m["cty_pop_growth"]), "| place pop 2020→25", dist(m["plc_pop_growth"]))
print("permits per 1k, munis: county", dist(m["cty_permits_per_1k"]), "| place", dist(m["plc_permits_per_1k"]))
print("owner SF share, munis: county", dist(m["cty_owner_sf_share"]), "| place", dist(m["plc_owner_sf_share"]))
print("coverage (munis with confirmed place):", {k: m[f"plc_{k}"].is_not_null().sum() for k in PLACE_COLS})
abs_err = m.with_columns(
    (pl.col("cty_pop_cagr_2020_24") - pl.col("meter_cagr_2020_24")).abs().alias("e_cty"),
    (pl.col("plc_pop_cagr_2020_24") - pl.col("meter_cagr_2020_24")).abs().alias("e_plc"),
)
print("|pop CAGR − meter CAGR| 2020→24, median pp: county",
      round(abs_err["e_cty"].drop_nulls().median() * 100, 2), "| place", round(abs_err["e_plc"].drop_nulls().median() * 100, 2),
      "| place closer for", (abs_err["e_plc"] < abs_err["e_cty"]).sum(), "of", abs_err.select("e_cty", "e_plc").drop_nulls().height)
examples = ["Boerne Utilities", "New Braunfels Utilities", "Georgetown Utility Systems", "Seguin Electric System",
            "Kerrville Public Utility Board", "Lubbock Power & Light System"]
with pl.Config(float_precision=3):
    print(cmp.filter(pl.col("name").is_in(examples)).select(
        "name", "eia_customers", "cty_owner_sf_homes", "plc_owner_sf_homes", "cty_sf_per_meter", "plc_sf_per_meter",
        "cty_pop_growth", "plc_pop_growth", "eia_customer_cagr", "cty_permits_per_1k", "plc_permits_per_1k"))

# %% [markdown]
# ## 3. Triggers for munis: X5's strong triggers, then the city mappings

# %%
agreements_raw = read_sql(
    """
    select program, agreement_id, status, local_government_type, local_government_name, county_fips,
           recipient_name, executed_date, effective_date, total_incentive_value, naics_code, taxing_units
    from cpa_local_dev_agreements
    """
)
agreements = T.dev_agreement_events(agreements_raw.drop("taxing_units"), min_value=1_000_000.0)
geo_c = T.load_geography()
latest_permit_month = read_sql("select max(period_start) m from census_permits_county where period_type = 'monthly'")["m"][0]
sites = DC.load_sites()
dc_events = T.dc_permit_events(sites)
strong_parts = [
    T.events_by_county(dc_events, links),
    T.events_by_county(T.gen_storage_events(T.load_gis_projects(), geo_c, min_mw=50.0, fuels=GEN_FUELS), links),
    T.events_by_county(T.county_agreements(agreements), links),
    T.city_agreement_accounts(agreements, names.select("account_id", "name", "account_type")),
    T.registration_events(T.load_market_participants(), names.select("account_id", "name", "account_type")),
    T.permit_surge_events(T.load_permits_monthly(latest_permit_month.year - 3), links, as_of_month=latest_permit_month),
]
cols = ["account_id", *T.EVENT_COLUMNS, "exposure"]


def stack(parts: list[pl.DataFrame]) -> pl.DataFrame:
    return (
        pl.concat([p.select(cols) for p in parts], how="vertical_relaxed")
        .filter(pl.col("account_id").is_in(ids))  # validation lock
        .unique(subset=["account_id", "trigger", "source_ref"], keep="first")
    )


base = stack(strong_parts)
# City mappings (munis with a confirmed place only)
mp = confirmed.select("account_id", "place_fips", "place_name")
dc_city = M.dc_city_events(dc_events, sites.select("ref_num_txt", "city"), mp)
ch312_city = M.ch312_city_events(agreements, agreements_raw, mp)
ytd_now, ytd_prev = bps("so2608y.txt"), bps("so2508y.txt")
changes = M.permit_change(ytd_now, ytd_prev)
surge_place = M.place_permit_surge_events(changes, mp, as_of_month=date(2026, 8, 1), window="Jan–Aug 2026 vs Jan–Aug 2025")
changes_lo = M.permit_change(ytd_now, ytd_prev, min_units=20.0)
surge_place_lo = M.place_permit_surge_events(changes_lo, mp, as_of_month=date(2026, 8, 1), window="≥ 20 units")
new_parts = {"dc_city": dc_city, "ch312_city": ch312_city, "permit_surge_place": surge_place}
events = stack([base, *new_parts.values()])
events.join(names.select("account_id", "name", "account_type"), on="account_id").write_csv(out_path("x10_muni_trigger_events.csv"))


def active_accounts(ev: pl.DataFrame, only: list[str]) -> set[str]:
    return set(T.active_events(ev.filter(pl.col("account_id").is_in(only)))["account_id"].to_list())


print("munis with ≥ 1 active strong trigger (X5 rules):", len(active_accounts(base, muni_ids)), "of", len(muni_ids))
for k, v in new_parts.items():
    ever = v.filter(pl.col("account_id").is_in(muni_ids))
    print(f"  {k}: events ever {ever.height} ({ever['account_id'].n_unique()} munis); active munis:",
          sorted(names.filter(pl.col("account_id").is_in(list(active_accounts(v, muni_ids))))["name"].to_list()))
print("  permit_surge_place at ≥ 20 units: active munis:",
      sorted(names.filter(pl.col("account_id").is_in(list(active_accounts(surge_place_lo, muni_ids))))["name"].to_list()))
after = active_accounts(events, muni_ids)
print("munis with ≥ 1 active strong trigger after the city mappings:", len(after), "of", len(muni_ids),
      "| new:", sorted(names.filter(pl.col("account_id").is_in(list(after - active_accounts(base, muni_ids))))["name"].to_list()))
print("the same with permits at ≥ 20 units:", len(active_accounts(stack([events, surge_place_lo]), muni_ids)))
# Data-center sites located in a muni's city (ever / since 2025) and how many TCEQ sites carry a city at all
dc_all = sites.filter(pl.col("first_affil_begin_dt") >= DC_SINCE)
print("TCEQ new data-center sites since 2025:", dc_all.height, "| with a city:", dc_all["city"].is_not_null().sum())
print("  in a confirmed muni city:", dc_city.filter(pl.col("event_date") >= DC_SINCE).height,
      dc_city.filter(pl.col("event_date") >= DC_SINCE).select("account_id", "title", "event_date").rows())
tx_changes = changes.filter(pl.col("prior") > 0)
print("place YTD permit change (all TX places with prior > 0):", tx_changes.height, "| surge:", tx_changes["surge"].sum())
print("muni place YTD permit change:")
with pl.Config(float_precision=2, tbl_rows=70):
    print(changes.join(mp, on="place_fips").join(names.select("account_id", "name"), on="account_id")
          .select("name", "prior", "recent", "change", "surge").sort("change", descending=True, nulls_last=True).head(12))
ym = pl.concat([ytd_now.select("place_fips", "months_rep").with_columns(pl.lit("2026").alias("y")),
                ytd_prev.select("place_fips", "months_rep").with_columns(pl.lit("2025").alias("y"))]).join(mp, on="place_fips")
print("muni places by months reported in the YTD files:", ym.group_by("y", pl.col("months_rep") > 0).len().sort("y").rows())

# %% [markdown]
# ## 4. Score effect: X4's proposed weights, munis on place rates vs county rates

# %%
X4 = {"pop_growth": 0.15, "eia_customers": 0.20, "eia_customer_cagr": 0.10, "permits_per_1k": 0.15,
      "dc_sites": 0.15, "zone_peak_cagr": 0.15, "owner_sf_share": 0.10}
base_sig = (
    names.select("account_id", "account_type", "eia_utility_id")
    .join(pop_growth, on="account_id", how="left")
    .join(A.permits_per_1k(permit_units, pop_growth, links), on="account_id", how="left")
    .join(A.owner_single_family(A.load_housing(), links), on="account_id", how="left")
    .join(A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE), on="account_id", how="left")
    .join(A.zone_peak_growth(A.load_zone_peaks(), A.load_county_zone(), links, start=ZONE_START, end=ZONE_END),
          on="account_id", how="left")
    .join(eia_sig, on="eia_utility_id", how="left")
)
plc = cmp.select("account_id", "plc_pop_growth", "plc_permits_per_1k", "plc_owner_sf_share")
place_sig = base_sig.join(plc, on="account_id", how="left").with_columns(
    pl.coalesce("plc_pop_growth", "pop_growth").alias("pop_growth"),
    pl.coalesce("plc_permits_per_1k", "permits_per_1k").alias("permits_per_1k"),
    pl.coalesce("plc_owner_sf_share", "owner_sf_share").alias("owner_sf_share"),
)
dirs = {k: "higher" for k in X4}


def ranked(sig: pl.DataFrame) -> pl.DataFrame:
    return T.score_accounts(T.percentile_ranks(sig, dirs), X4).select("account_id", "score", "rank")


r0, r1 = ranked(base_sig), ranked(place_sig)
rr = r0.join(r1, on="account_id", suffix="_place").join(names.select("account_id", "name", "account_type"), on="account_id")
rr.write_csv(out_path("x10_score_ranks.csv"))
mm = rr.filter(pl.col("account_type") == "muni")
print("Spearman, county vs place rates (107):", round(float(spearmanr(rr["score"], rr["score_place"]).statistic), 3))
print("munis in top 25: county", (mm["rank"] <= 25).sum(), "→ place", (mm["rank_place"] <= 25).sum(),
      "| median muni rank:", mm["rank"].median(), "→", mm["rank_place"].median(),
      "| munis in top half (≤ 53):", (mm["rank"] <= 53).sum(), "→", (mm["rank_place"] <= 53).sum())
print("largest muni moves:")
print(mm.with_columns((pl.col("rank") - pl.col("rank_place")).alias("up")).sort("up", descending=True)
      .select("name", "rank", "rank_place", "up").head(8))
print(mm.with_columns((pl.col("rank") - pl.col("rank_place")).alias("up")).sort("up")
      .select("name", "rank", "rank_place", "up").head(5))
# Within-type ranking (option b of Q3): the munis' own order under each variant
for label, sig in (("county", base_sig), ("place", place_sig)):
    mu = ranked(sig.filter(pl.col("account_type") == "muni")).join(names.select("account_id", "name"), on="account_id")
    print(f"top 10 munis ranked among munis ({label} rates):", mu.sort("rank").head(10)["name"].to_list())

# %% [markdown]
# ## Checks behind the numbers above

# %%
# Monthly BPS covers a sample of permit offices; a place with 0 months reported in a YTD file carries Census
# imputation only, so a "surge" there says nothing. Recompute the place surge on monthly reporters only.
rep = (
    ytd_now.filter(pl.col("place_fips").is_not_null()).group_by("place_fips").agg(pl.col("months_rep").max().alias("m_now"))
    .join(ytd_prev.filter(pl.col("place_fips").is_not_null()).group_by("place_fips").agg(pl.col("months_rep").max().alias("m_prev")),
          on="place_fips", how="full", coalesce=True)
)
sp = changes.join(rep, on="place_fips", how="left").join(mp, on="place_fips").join(names.select("account_id", "name"), on="account_id")
print("muni place surges with the months reported (Jan–Aug):")
print(sp.filter(pl.col("surge")).select("name", "prior", "recent", "change", "m_prev", "m_now").sort("name"))
reported = changes.join(rep, on="place_fips").filter(pl.col("m_now") >= 8, pl.col("m_prev") >= 8)
surge_rep = M.place_permit_surge_events(reported, mp, as_of_month=date(2026, 8, 1), window="reporters only")
print("active muni surges, 8 of 8 months reported both years:",
      sorted(names.filter(pl.col("account_id").is_in(list(active_accounts(surge_rep, muni_ids))))["name"].to_list()))
after_rep = active_accounts(stack([base, dc_city, ch312_city, surge_rep]), muni_ids)
print("munis with ≥ 1 active strong trigger, reporters-only surge:", len(after_rep), "of", len(muni_ids))
print("munis whose place has no BPS row 2023–2025:",
      cmp.filter(pl.col("account_type") == "muni", pl.col("plc_permit_units").is_null())["name"].to_list())
print("Ch. 312 city events for munis (latest 8):")
print(ch312_city.join(names.select("account_id", "name"), on="account_id").sort("event_date", descending=True)
      .select("name", "event_date", "title", "detail").head(8))
print("data-center sites in a muni's city (all dates):")
print(dc_city.join(names.select("account_id", "name"), on="account_id").group_by("name")
      .agg(pl.len().alias("sites"), pl.col("event_date").max().alias("latest")).sort("name"))
# X5's county path already gave the same NBU site?
print("dc_city events already in the X5 county events:",
      dc_city.join(base, on=["account_id", "source_ref"], how="semi").height, "of", dc_city.height)
