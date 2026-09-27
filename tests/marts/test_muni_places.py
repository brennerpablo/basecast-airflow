"""The X10 muni extras of the account marts (``basecast_pipelines/marts/muni_places.py``), on synthetic inputs: no
database, no lake."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from pathlib import Path

import polars as pl
import pytest
import shapefile

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import muni_places as MP
from basecast_pipelines.marts.core import Mart, MartContext, run_checks
from basecast_pipelines.models import muni_places as M
from basecast_pipelines.models import triggers as T

AS_OF = date(2026, 9, 26)

PEP = """SUMLEV,STATE,COUNTY,PLACE,COUSUB,CONCIT,PRIMGEO_FLAG,FUNCSTAT,NAME,STNAME,ESTIMATESBASE2020,POPESTIMATE2020,POPESTIMATE2025
162,48,000,00100,00000,00000,0,A,Alpha city,Texas,1000,1000,1250
162,48,000,00200,00000,00000,0,A,Beta town,Texas,2000,2000,2100
162,48,000,00300,00000,00000,0,A,Gamma city,Texas,500,500,500
"""

ACS = [
    "GEO_ID|B25032_E001|B25032_M001|B25032_E002|B25032_M002|B25032_E003|B25032_M003|B25032_E004|B25032_M004\n",
    "1600000US4800100|400|1|300|1|250|1|10|1\n",
    "1600000US4800200|800|1|500|1|400|1|0|1\n",
]

_TOP = ("Survey,State,6-Digit,County,Census Place,FIPS Place,FIPS MCD,Pop,CSA,CBSA,Footnote,Central,Zip,Region,"
        "Division,Number of,Place,,1-unit,,,2-units,,,3-4 units,,,5+ units,,,1-unit rep,,,2-units rep,,,"
        "3-4 units rep,,,5+ units rep\n")
_SUB = ("Date,Code,ID,Code,Code,Code,Code,  ,Code,Code,Code,City,Code,Code,Code,Months Rep,Name,"
        + ",".join(["Bldgs,Units,Value"] * 8) + "\n \n")


def bps(survey: str, rows: list[tuple[str, int, float]]) -> str:
    """A BPS place file (two header rows) with one permit office per ``(FIPS place, months reported, units)``."""
    lines = [
        f"{survey},48,{i:06d},001,0001,{place} ,00000 ,1000 ,1,1, , ,1      ,3,7,{months},P{i},"
        f"1,{units},1,0,0,0,0,0,0,0,0,0,1,{units},1,0,0,0,0,0,0,0,0,0"
        for i, (place, months, units) in enumerate(rows, start=1)
    ]
    return _TOP + _SUB + "\n".join(lines) + "\n"


def place_zip(places: list[tuple[str, tuple[float, float, float, float]]]) -> bytes:
    """A cartographic-boundary-like zip: one square per ``(GEOID, (x0, y0, x1, y1))``."""
    shp, shx, dbf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    w = shapefile.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapefile.POLYGON)
    w.field("GEOID", "C", size=7)
    w.field("LSAD", "C", size=2)
    for geoid, (x0, y0, x1, y1) in places:
        w.poly([[[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]])
        w.record(geoid, "25")
    w.close()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for ext, b in ((".shp", shp), (".shx", shx), (".dbf", dbf)):
            z.writestr(f"cb_2024_48_place_500k{ext}", b.getvalue())
    return buf.getvalue()


def _write(path: Path, content: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content if isinstance(content, bytes) else content.encode("latin-1"))


# --- the lake files ------------------------------------------------------------------------------------------


def test_lake_files_keep_the_newest_snapshot_per_name(tmp_path):
    for dt, name in (("2026-09-01", "so2025a.txt"), ("2026-09-20", "so2025a.txt"), ("2026-09-01", "so2024a.txt"),
                     ("2026-09-20", "co2025a.txt")):
        _write(tmp_path / "source=census_bps" / f"dt={dt}" / name, dt)
    files = MP.lake_files(tmp_path, *MP.BPS_ANNUAL)
    assert sorted(files) == ["so2024a.txt", "so2025a.txt"]
    assert files["so2025a.txt"].read_text() == "2026-09-20"


def test_ytd_pair_takes_the_newest_started_month_with_its_prior_year():
    names = ["so2508y.txt", "so2608y.txt", "so2607y.txt", "so2609y.txt", "so2509y.txt", "so2605y.txt"]
    assert MP.ytd_pair(names, as_of=AS_OF) == ("so2609y.txt", "so2509y.txt", date(2026, 9, 1))
    assert MP.ytd_pair(names, as_of=date(2026, 8, 31)) == ("so2608y.txt", "so2508y.txt", date(2026, 8, 1))
    # neither July nor May 2026 has its 2025 month, and 2025's months have no 2024 file: no pair up to July
    with pytest.raises(FileNotFoundError):
        MP.ytd_pair(names, as_of=date(2026, 7, 31))


def test_ytd_window_names_the_same_months():
    assert MP.ytd_window(date(2026, 8, 1)) == "Jan–Aug 2026 vs Jan–Aug 2025"
    assert MP.ytd_window(date(2026, 1, 1)) == "Jan 2026 vs Jan 2025"


def test_load_tables_names_the_missing_files(tmp_path):
    with pytest.raises(FileNotFoundError) as err:
        MP.load_tables(tmp_path, as_of=AS_OF)
    for piece in ("so2023a.txt", "so2025a.txt", "census_pep", "census_acs", "census_tx_counties_geo"):
        assert piece in str(err.value)


def _lake(root: Path) -> Path:
    dt = "dt=2026-09-26"
    _write(root / "source=census_pep" / dt / "sub-est2025_48.csv", PEP)
    for y in MP.PERMIT_YEARS:
        _write(root / "source=census_bps" / dt / f"so{y}a.txt", bps(str(y), [("00100", 12, 10.0)]))
    _write(root / "source=census_bps" / dt / "so2508y.txt", bps("202508", [("00100", 8, 40.0), ("00200", 8, 10.0)]))
    _write(root / "source=census_bps" / dt / "so2608y.txt", bps("202608", [("00100", 8, 60.0), ("00200", 8, 90.0)]))
    _write(root / "source=census_bps" / dt / "so2610y.txt", bps("202610", []))  # a month after as_of: ignored
    _write(root / "source=census_acs" / "dt=2026-09-25" / "acsdt5y2024-b25032.dat", "".join(ACS))
    _write(root / "source=census_tx_counties_geo" / dt / "cb_2024_48_place_500k.zip",
           place_zip([("4800100", (0, 0, 1, 1)), ("4800200", (5, 5, 6, 6))]))
    return root


def test_load_tables_parses_the_lake(tmp_path):
    t = MP.load_tables(_lake(tmp_path), as_of=AS_OF)
    assert t.ytd_month == date(2026, 8, 1) and t.acs_year == 2024
    assert sorted(t.pep["year"].unique().to_list()) == [2020, 2025]
    assert t.bps_annual.group_by("year").len().sort("year").rows() == [(2023, 1), (2024, 1), (2025, 1)]
    assert t.ytd_now["units_total"].sum() == 150.0 and t.ytd_prev["units_total"].sum() == 50.0
    assert t.acs.height == 2 and t.geo_zip.name == "cb_2024_48_place_500k.zip"


# --- the crosswalk -------------------------------------------------------------------------------------------


MUNIS = pl.DataFrame({
    "account_id": ["m1", "m2", "m3", "c1"],
    "name": ["Alpha Utilities", "Northside Power Board", "Gamma Electric", "Alpha Electric Cooperative"],
    "account_type": ["muni", "muni", "muni", "coop"],
    "eia_utility_id": ["1", "2", "3", "4"],
})
EIA_NAMES = pl.DataFrame({"eia_utility_id": ["1", "2", "4"],
                          "utility_name": ["City of Alpha", "Beta Town of", "Alpha Electric Coop"]})


def test_name_candidates_try_the_puct_name_then_the_eia_name():
    cand = MP.name_candidates(MP.munis_of(MUNIS), EIA_NAMES)
    assert set(cand["account_id"]) == {"m1", "m2", "m3"}  # co-ops never get a city
    assert cand.filter(pl.col("account_id") == "m2").sort("priority").select("name", "matched_on").rows() == [
        ("Northside Power Board", "puct"), ("Beta Town of", "eia")]
    places = M.parse_pep_places(PEP).select("place_fips", "place_name", "funcstat").unique()
    hits = M.match_places(cand, places)
    assert dict(zip(hits["account_id"], hits["matched_on"])) == {"m1": "puct", "m2": "eia", "m3": "puct"}


def _overlap(rows: list[tuple[str, str, float, float, float]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["account_id", "place_fips", "territory_km2", "place_km2", "inter_km2"],
                        orient="row")


def test_check_crosswalk_classifies_the_fit_and_finds_the_best_place():
    places = M.parse_pep_places(PEP).select("place_fips", "place_name", "funcstat").unique()
    names = pl.DataFrame({"account_id": ["m1", "m2", "m3"], "place_fips": ["4800100", "4800200", None],
                          "place_name": ["Alpha city", "Beta town", None], "matched_on": ["puct", "eia", None]})
    ov = _overlap([
        ("m1", "4800100", 100.0, 40.0, 38.0),  # 95% of the city in a territory 38% covered: territory larger
        ("m2", "4800200", 10.0, 30.0, 9.0),  # 90% of the territory in a city 30% covered: city larger
        ("m2", "4800300", 10.0, 1.0, 1.0),  # a smaller neighbour
    ])
    terr = pl.DataFrame({"account_id": ["m1", "m2", "m3"], "territory_km2": [100.0, 10.0, 5.0]})
    out = MP.check_crosswalk(names, ov, terr, places).sort("account_id")
    assert out["overlap_status"].to_list() == ["confirmed", "confirmed", "no_place"]
    assert out["fit"].to_list() == ["territory_larger", "city_larger", None]
    assert out["best_fips"].to_list() == ["4800100", "4800200", None]
    assert out["name_is_best_spatial"].to_list() == [True, True, None]
    assert out.filter(pl.col("account_id") == "m1")["territory_in_place"].item() == pytest.approx(0.38)


def test_check_crosswalk_survives_no_overlap_rows():
    places = M.parse_pep_places(PEP).select("place_fips", "place_name", "funcstat").unique()
    names = pl.DataFrame({"account_id": ["m1"], "place_fips": ["4800100"], "place_name": ["Alpha city"],
                          "matched_on": ["puct"]})
    empty = pl.DataFrame({c: [] for c in ("account_id", "place_fips", "territory_km2", "place_km2", "inter_km2")})
    out = MP.check_crosswalk(names, empty, pl.DataFrame({"account_id": ["m1"], "territory_km2": [3.0]}), places)
    assert out["overlap_status"].to_list() == ["no_place"]


def test_place_polygons_keep_incorporated_places_near_a_territory():
    z = place_zip([("4800100", (0, 0, 1, 1)), ("4800200", (5, 5, 6, 6)), ("4899999", (0, 0, 1, 1))])
    boxes = [{"x0": 0.5, "y0": 0.5, "x1": 2.0, "y1": 2.0}]
    out = MP.place_polygons(z, boxes, {"4800100", "4800200"})
    assert [p["place_fips"] for p in out] == ["4800100"]  # 4800200 is far, 4899999 is not incorporated
    assert json.loads(out[0]["gj"])["type"] == "Polygon"


# --- the city block ------------------------------------------------------------------------------------------


ROW = {"account_id": "m1", "place_fips": "4800100", "place_name": "Alpha city", "fit": "territory_larger",
       "place_in_territory": 0.95, "territory_in_place": 0.38, "population": 1250.0, "pop_growth": 0.25,
       "permit_units": 30.0, "permits_per_1k": 24.0, "occupied_units": 400.0, "owner_sf_homes": 260.0,
       "owner_sf_share": 0.65, "eia_customers": 500.0}


def test_city_block_facts_say_city_not_territory():
    b = MP.city_block(ROW, acs_year=2024)
    json.dumps(b)  # JSON-ready
    assert {k: b[k] for k in ("place_name", "place_fips", "fit")} == {
        "place_name": "Alpha city", "place_fips": "4800100", "fit": "territory_larger"}
    facts = {f["key"]: f for f in b["facts"]}
    assert list(facts) == ["city_population", "city_pop_growth", "city_owner_sf_homes", "city_owner_sf_share",
                           "city_permit_units", "city_permits_per_1k", "city_homes_per_meter"]
    for f in b["facts"]:
        assert set(f) == {"key", "label", "value", "unit", "source", "as_of", "note", "simulated", "verified"}
        assert f["note"].startswith("city, not territory")
        assert f["simulated"] is False and f["verified"] is True
    assert facts["city_homes_per_meter"]["value"] == pytest.approx(0.52)
    assert facts["city_population"]["as_of"] == "2025-07-01"
    assert facts["city_permits_per_1k"]["source"] == "census_bps_place"
    # the fit's reading goes on the counts, not on the rates
    floor = MP.FIT_NOTE["territory_larger"]
    assert floor in facts["city_owner_sf_homes"]["note"] and floor in facts["city_homes_per_meter"]["note"]
    assert floor not in facts["city_pop_growth"]["note"] and floor not in facts["city_owner_sf_share"]["note"]


def test_city_block_gaps_stay_null_and_a_weak_fit_is_refused():
    b = MP.city_block({**ROW, "fit": "same", "permit_units": None, "permits_per_1k": None, "eia_customers": None},
                      acs_year=2024)
    facts = {f["key"]: f for f in b["facts"]}
    assert facts["city_permit_units"]["value"] is None and "no BPS row" in facts["city_permit_units"]["note"]
    assert facts["city_homes_per_meter"]["value"] is None
    assert facts["city_population"]["note"] == "city, not territory; PEP vintage 2025, July 1"
    with pytest.raises(ValueError):
        MP.city_block({**ROW, "fit": "weak"}, acs_year=2024)


def test_meters_are_the_wires_customers_of_the_eia_year():
    eia = pl.DataFrame({
        "utility_id": [1, 1, 2], "data_year": [2019, 2024, 2024], "early_release": [False] * 3,
        "form": ["long"] * 3, "customers": [90.0, 100.0, 50.0], "wires_customers": [90.0, 120.0, 50.0],
        "sales_mwh": [1.0] * 3, "revenue_thousand_usd": [1.0] * 3,
    })
    out = MP.meters(eia, MP.munis_of(MUNIS)).sort("account_id")
    assert out.rows() == [("m1", 120.0), ("m2", 50.0), ("m3", None)]


# --- the city permit surge -----------------------------------------------------------------------------------


def _ytd(rows: list[tuple[str, int, float]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={"place_fips": pl.Utf8, "months_rep": pl.Int32, "units_total": pl.Float64},
                        orient="row")


def test_reported_changes_keep_monthly_reporters_only():
    now = _ytd([("4800100", 8, 60.0), ("4800200", 0, 90.0), ("4800300", 8, 70.0), (None, 8, 500.0)])
    prev = _ytd([("4800100", 8, 40.0), ("4800200", 0, 10.0), ("4800300", 7, 10.0)])
    months = MP.reporting_months(now, prev).sort("place_fips")
    assert months.rows() == [("4800100", 8, 8), ("4800200", 0, 0), ("4800300", 8, 7)]
    out = MP.reported_changes(now, prev, months=8)
    assert out["place_fips"].to_list() == ["4800100"] and out["surge"].to_list() == [True]
    # without the filter the imputed-only place and the 7-month reporter would fire too
    assert M.permit_change(now, prev)["surge"].sum() == 3


def test_surge_events_have_the_account_event_shape_and_stay_in_the_universe():
    changes = pl.DataFrame({"place_fips": ["4800100", "4800200"], "recent": [60.0, 90.0], "prior": [40.0, 10.0],
                            "change": [0.5, 8.0], "surge": [True, True]})
    mp = pl.DataFrame({"account_id": ["m1", "x9"], "place_fips": ["4800100", "4800200"],
                       "place_name": ["Alpha city", "Beta town"]})
    ev = MP.surge_events(changes, mp, ["m1"], month=date(2026, 8, 1))
    assert ev.schema == pl.Schema(MP.EVENT_SCHEMA)
    assert ev.columns == ["account_id", *T.EVENT_COLUMNS, "exposure"]
    row = ev.row(0, named=True)
    assert ev.height == 1 and row["account_id"] == "m1" and row["trigger"] == "permit_surge"
    assert row["trigger"] in T.TRIGGERS and T.TRIGGERS[row["trigger"]].strength == "strong"
    assert row["event_date"] == date(2026, 8, 1) and row["county_fips"] is None and row["exposure"] == 1.0
    assert row["title"] == "Alpha city permits +50.0% (Jan–Aug 2026 vs Jan–Aug 2025)"
    assert row["source"] == "census_bps_place" and row["source_ref"] == "4800100:2026-08-01"


# --- the chain, with a fake database -------------------------------------------------------------------------


class FakeDb:
    """Answers the three PostGIS queries with fixed frames and records the account ids asked for."""

    def __init__(self) -> None:
        self.ids: list[list[str]] = []

    def __call__(self, query: str, params: dict | None = None) -> pl.DataFrame:
        self.ids.append(sorted(params["ids"]))
        if query == MP.EXTENT_SQL:
            return pl.DataFrame({"account_id": ["m1", "m2", "m3"], "x0": [0.0, 5.0, 9.0], "y0": [0.0, 5.0, 9.0],
                                 "x1": [1.0, 6.0, 9.5], "y1": [1.0, 6.0, 9.5]})
        if query == MP.OVERLAP_SQL:
            assert {p["place_fips"] for p in json.loads(params["p"])} == {"4800100", "4800200"}
            return _overlap([("m1", "4800100", 100.0, 40.0, 38.0), ("m2", "4800200", 10.0, 30.0, 9.0)])
        if query == MP.TERRITORY_SQL:
            return pl.DataFrame({"account_id": ["m1", "m2", "m3"], "territory_km2": [100.0, 10.0, 5.0]})
        raise AssertionError(f"unexpected query: {query[:60]}")


def test_the_chain_builds_blocks_and_events_for_the_given_munis_only(tmp_path, monkeypatch):
    monkeypatch.setattr(MP, "RAW", _lake(tmp_path))
    db = FakeDb()
    ctx = MartContext(AS_OF, {}, db)
    ctx.cache["muni_places.eia_names"] = EIA_NAMES
    meters = pl.DataFrame({"account_id": ["m1", "m2", "m3"], "eia_customers": [500.0, 200.0, 10.0]})
    monkeypatch.setattr(MP, "meters", lambda eia, munis: meters)
    accounts = MUNIS.filter(pl.col("account_id") != "m3")  # m3 is outside the universe
    blocks = MP.city_blocks(ctx, accounts, eia=pl.DataFrame())
    assert sorted(blocks) == ["m1", "m2"]
    assert blocks["m1"]["fit"] == "territory_larger" and blocks["m2"]["fit"] == "city_larger"
    homes = {f["key"]: f["value"] for f in blocks["m1"]["facts"]}["city_owner_sf_homes"]
    assert homes == 260.0
    ev = MP.place_permit_surge_events(ctx, accounts)
    # Alpha 40 → 60 (+50%, 60 units) and Beta 10 → 90 (+800%, 90 units), both 8 of 8 months reported
    assert sorted(ev["account_id"].to_list()) == ["m1", "m2"]
    assert db.ids and all(ids == ["m1", "m2"] for ids in db.ids)  # the database only sees the universe's munis
    assert len(db.ids) == 3  # the crosswalk is built once per run


def test_no_muni_needs_no_file_and_no_query(tmp_path, monkeypatch):
    monkeypatch.setattr(MP, "RAW", tmp_path)  # an empty lake
    ctx = MartContext(AS_OF, {}, lambda *a: pytest.fail("no query expected"))
    coops = MUNIS.filter(pl.col("account_type") == "coop")
    assert MP.city_blocks(ctx, coops) == {}
    assert MP.place_permit_surge_events(ctx, coops).schema == pl.Schema(MP.EVENT_SCHEMA)


# --- golden checks -------------------------------------------------------------------------------------------


def _config(**values) -> dict:
    config = marts_config.load()
    for key, value in values.items():
        group, name = key.split("__")
        config = {**config, group: {**config[group], name: {"value": value, "status": "reviewed"}}}
    return config


def test_golden_checks_apply_only_with_the_x10_switches():
    blocks = [{"fit": "same"}] * 31 + [{"fit": "city_larger"}] * 7 + [{"fit": "territory_larger"}] * 21
    detail = pl.DataFrame({"payload": [json.dumps({"city": b}) for b in blocks] + [json.dumps({"city": None})] * 48})
    on = _config(munis__place_facts=True, munis__place_permit_trigger=True)
    assert [c["status"] for c in run_checks(_mart(MP.DETAIL_CHECKS), detail, AS_OF, on)] == ["passed"] * 4
    off = _config(munis__place_facts=False, munis__place_permit_trigger=False)
    assert {c["status"] for c in run_checks(_mart(MP.DETAIL_CHECKS), detail, AS_OF, off)} == {"skipped"}
    accounts = pl.DataFrame({"account_type": ["muni"] * 59 + ["coop"] * 48,
                             "n_strong": [1] * 13 + [0] * 46 + [1] * 48})
    assert [c["status"] for c in run_checks(_mart(MP.ACCOUNT_CHECKS), accounts, AS_OF, on)] == ["passed"]
    assert [c["status"] for c in run_checks(_mart(MP.ACCOUNT_CHECKS), accounts, AS_OF, off)] == ["skipped"]


def _mart(checks) -> Mart:
    return Mart(name="mart_test", build=lambda ctx: pl.DataFrame(), key=(), inputs=(), description="",
                checks=checks)
