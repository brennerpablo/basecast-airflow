"""Census place signals for munis (``basecast_pipelines/models/muni_places.py``), on tiny synthetic inputs."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import muni_places as M
from basecast_pipelines.models import triggers as T

PEP = """SUMLEV,STATE,COUNTY,PLACE,COUSUB,CONCIT,PRIMGEO_FLAG,FUNCSTAT,NAME,STNAME,ESTIMATESBASE2020,POPESTIMATE2020,POPESTIMATE2025
040,48,000,00000,00000,00000,0,A,Texas,Texas,100,101,120
162,48,000,00100,00000,00000,0,A,Alpha city,Texas,1000,1000,1250
157,48,001,00100,00000,00000,0,A,Alpha city (pt.),Texas,500,500,600
162,48,000,00200,00000,00000,0,A,La Beta town,Texas,200,0,50
162,05,000,00300,00000,00000,0,A,Other city,Arkansas,9,9,9
"""

# Two header rows as the Census writes them: the group label sits over "Units" and the top row is one short.
BPS = (
    "Survey,State,6-Digit,County,Census Place,FIPS Place,FIPS MCD,Pop,CSA,CBSA,Footnote,Central,Zip,Region,"
    "Division,Number of,Place,,1-unit,,,2-units,,,3-4 units,,,5+ units,,,1-unit rep,,,2-units rep,,,"
    "3-4 units rep,,,5+ units rep\n"
    "Date,Code,ID,Code,Code,Code,Code,  ,Code,Code,Code,City,Code,Code,Code,Months Rep,Name,"
    + ",".join(["Bldgs,Units,Value"] * 8)
    + "\n \n"
    "2025,48,000100,001,0001,00100 ,00000 ,1000 ,1,1, , ,1      ,3,7,12,Alpha,10,10,1,1,2,1,0,0,0,1,40,1,"
    "8,8,1,1,2,1,0,0,0,1,40,1\n"
    "2025,48,000900,001,    ,99990 ,00000 ,50 ,1,1, , ,1      ,3,7,0,Alpha County Unincorporated Area,5,5,1,"
    "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0\n"
    "2025,05,000100,001,0001,00300 ,00000 ,9 ,1,1, , ,1      ,3,7,12,Other,1,1,1,0,0,0,0,0,0,0,0,0,"
    "1,1,1,0,0,0,0,0,0,0,0,0\n"
)

ACS = [
    "GEO_ID|B25032_E001|B25032_M001|B25032_E002|B25032_M002|B25032_E003|B25032_M003|B25032_E004|B25032_M004\n",
    "0500000US48001|900|1|600|1|500|1|20|1\n",
    "1600000US4800100|400|1|300|1|250|1|10|1\n",
    "1600000US0500300|4|1|3|1|2|1|0|1\n",
]


def test_place_and_city_cores_match_utility_cores():
    assert M.place_core("Boerne city") == "boerne"
    assert M.place_core("La Grange city") == "la grange"
    assert M.place_core("Lexington town") == "lexington"
    assert M.city_core("SAN ANTONIO") == "san antonio"
    assert M.city_core("Waxahachie City") == "waxahachie"


def test_pep_keeps_incorporated_places_of_the_state_long_by_year():
    out = M.parse_pep_places(PEP)
    assert out["place_fips"].unique().sort().to_list() == ["4800100", "4800200"]
    alpha = out.filter(pl.col("place_fips") == "4800100").sort("year")
    assert alpha.select("year", "population").rows() == [(2020, 1000.0), (2025, 1250.0)]


def test_bps_reads_headers_and_sums_structure_types():
    out = M.parse_bps_places(BPS)
    assert out.height == 2  # Arkansas dropped
    alpha = out.filter(pl.col("place_name") == "Alpha").row(0, named=True)
    assert alpha["place_fips"] == "4800100"
    assert alpha["units_total"] == 10 + 2 + 0 + 40
    assert alpha["units_1"] == 10
    assert alpha["units_total_rep"] == 8 + 2 + 0 + 40
    assert alpha["months_rep"] == 12
    unincorporated = out.filter(pl.col("place_name").str.contains("Unincorporated")).row(0, named=True)
    assert unincorporated["place_fips"] is None


def test_bps_fails_loudly_when_a_column_is_missing():
    with pytest.raises(ValueError, match="columns not found"):
        M.parse_bps_places("a,b\nc,d\n")


def test_acs_keeps_the_states_places_only():
    out = M.parse_acs_places(ACS)
    assert out.rows() == [("4800100", 400.0, 300.0, 260.0)]


def test_match_tries_names_in_order_and_skips_ambiguous_cores():
    places = pl.DataFrame(
        {"place_fips": ["4800100", "4800200", "4800300", "4800400"],
         "place_name": ["Alpha city", "Bryan city", "Twin city", "Twin town"]}
    )
    cand = pl.DataFrame(
        {
            "account_id": ["a", "b", "b", "t"],
            "name": ["Alpha Utilities", "BTU Rural Electric Division", "City of Bryan", "Twin Light & Power"],
            "matched_on": ["puct", "puct", "eia", "puct"],
            "priority": [0, 0, 1, 0],
        }
    )
    out = M.match_places(cand, places)
    assert out.select("account_id", "place_fips", "matched_on").rows() == [
        ("a", "4800100", "puct"),
        ("b", "4800200", "eia"),
        ("t", None, None),  # "twin" names two places
    ]


def test_overlap_status_and_fit():
    df = pl.DataFrame(
        {
            "place_km2": [10.0, 40.0, 10.0, 100.0, None],
            "territory_km2": [12.0, 10.0, 100.0, 100.0, 5.0],
            "inter_km2": [9.0, 9.0, 9.5, 5.0, None],
        }
    )
    out = M.classify_overlap(df)
    assert out["overlap_status"].to_list() == ["confirmed", "confirmed", "confirmed", "rejected", "no_place"]
    assert out["fit"].to_list() == ["same", "city_larger", "territory_larger", "weak", None]


def test_place_signals_follow_q3_definitions():
    pop = M.parse_pep_places(PEP)
    permits = M.parse_bps_places(BPS).with_columns(pl.lit(2025).alias("year"))
    housing = M.parse_acs_places(ACS)
    out = M.place_signals(pop, permits, housing, start=2020, end=2025, permit_years=[2025]).sort("place_fips")
    alpha, beta = out.row(0, named=True), out.row(1, named=True)
    assert alpha["pop_growth"] == pytest.approx(0.25)
    assert alpha["permits_per_1k"] == pytest.approx(52 / 1250 * 1000)
    assert alpha["owner_sf_share"] == pytest.approx(260 / 400)
    assert beta["pop_growth"] is None  # 2020 population 0
    assert beta["permit_units"] is None  # no BPS row: no data, not zero


def test_permit_change_uses_the_x5_thresholds():
    recent = pl.DataFrame({"place_fips": ["p1", "p2", "p3", None], "units_total": [60.0, 40.0, 10.0, 99.0]})
    prior = pl.DataFrame({"place_fips": ["p1", "p2", "p4"], "units_total": [40.0, 20.0, 5.0]})
    out = M.permit_change(recent, prior).sort("place_fips")
    assert out.select("place_fips", "surge").rows() == [("p1", True), ("p2", False), ("p3", False), ("p4", False)]
    assert out.filter(pl.col("place_fips") == "p3")["change"][0] is None  # no prior year
    assert M.permit_change(recent, prior, min_units=20).filter(pl.col("surge"))["place_fips"].sort().to_list() == [
        "p1", "p2"
    ]


MUNI_PLACES = pl.DataFrame(
    {"account_id": ["m1", "m2"], "place_fips": ["4800100", None], "place_name": ["Alpha city", None]}
)


def test_data_center_sites_map_to_the_muni_of_their_city():
    sites = pl.DataFrame(
        {
            "ref_num_txt": ["RN1", "RN2", "RN3"],
            "reg_ent_name": ["DC one", "DC two", "DC three"],
            "county_fips": ["48001", "48001", "48001"],
            "city": ["ALPHA", "OMEGA", None],
            "first_affil_begin_dt": [date(2026, 3, 1)] * 3,
            "matched_by_name": [True] * 3,
        }
    )
    out = M.dc_city_events(T.dc_permit_events(sites), sites.select("ref_num_txt", "city"), MUNI_PLACES)
    assert out.select("account_id", "source_ref", "exposure").rows() == [("m1", "RN1", 1.0)]


def test_city_units_read_the_json_list_and_fall_back_to_the_government_name():
    assert M.city_units('["Alpha City", "Alpha County", "Alpha ISD"]', "Alpha CAD") == ["Alpha City"]
    assert M.city_units(None, "Alpha City") == ["Alpha City"]
    assert M.city_units("not json", "Alpha County") == []


def test_ch312_abatements_of_the_muni_city_map_by_name():
    raw = pl.DataFrame(
        {
            "program": ["ch312", "ch312", "ch380"],
            "agreement_id": ["1", "2", "3"],
            "status": ["Active"] * 3,
            "local_government_type": [None, None, "City"],
            "local_government_name": ["Alpha City", "Alpha County", "Alpha"],
            "county_fips": ["48001"] * 3,
            "recipient_name": ["Plant", "Farm", "Shop"],
            "executed_date": [date(2026, 5, 1), date(2026, 5, 1), None],
            "effective_date": [date(2026, 5, 1), date(2026, 5, 1), date(2026, 5, 1)],
            "total_incentive_value": [0.0, 0.0, 2e6],
            "taxing_units": ['["Alpha City"]', '["Alpha County"]', None],
        }
    )
    events = T.dev_agreement_events(raw.drop("taxing_units"), min_value=1e6)
    out = M.ch312_city_events(events, raw, MUNI_PLACES)
    assert out.select("account_id", "source_ref").rows() == [("m1", "ch312:1")]


def test_place_permit_surge_events_have_the_event_shape():
    changes = pl.DataFrame(
        {"place_fips": ["4800100"], "recent": [80.0], "prior": [40.0], "change": [1.0], "surge": [True]}
    )
    out = M.place_permit_surge_events(changes, MUNI_PLACES, as_of_month=date(2026, 8, 1), window="YTD")
    assert out.columns == ["account_id", *T.EVENT_COLUMNS, "exposure"]
    assert out.row(0, named=True)["trigger"] == "permit_surge"
    assert out.row(0, named=True)["account_id"] == "m1"
