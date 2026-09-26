"""TPIT parser. Fixture: the four status sheets of the July 2026 workbook, a few rows each, copied cell for
cell; the TSP/Company Contact column holds simulated values (the real file has names, emails, phones)."""

from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot._tx_counties import county_fips
from basecast_pipelines.parsers.ercot.tpit import SQL_DATASETS, parse_projects, snapshot_of


def test_projects_and_personal_data(raw_file):
    df = parse_projects(raw_file("ercot_tpit", "ercot_tpit/ERCOT-July-Ad-Hoc-TPIT-No-Cost-071326-UPDATE_trimmed.xlsx"))
    assert df.height == 14
    assert set(df["sheet_status"]) == {"future", "planned", "completed", "cancelled"}
    assert df["snapshot_date"].unique().to_list() == [date(2026, 7, 13)]
    assert not any("contact" in c for c in df.columns)
    text = str(df.rows())
    assert "@" not in text and "Simulated Person" not in text
    first = df.filter(pl.col("project_id") == "81590")
    assert first["tsp"].item() == "STEC"
    assert first["kv"].item() == 138.0
    assert first["projected_in_service_date"].item() == date(2027, 1, 1)
    assert first["county_start"].item() == "Medina" and first["county_start_fips"].item() == "48325"
    done = df.filter(pl.col("project_id") == "73368")
    assert done["actual_in_service_date"].item() == date(2025, 1, 1)
    assert df.schema["projected_in_service_date"] == pl.Date and df.schema["kv"] == pl.Float64
    assert SQL_DATASETS[0].name == "tpit_project_history"


def test_snapshot_dates_and_counties():
    assert snapshot_of("FutureTPIT071326NoCost") == date(2026, 7, 13)
    assert snapshot_of("TPITFutureNoCost11012009") == date(2009, 11, 1)
    assert snapshot_of("ERCOTTPIT1999_-_2007NoCost.xls") is None
    assert county_fips("MIDLAND") == "48329"
    assert county_fips("De Witt") == county_fips("DEWITT") == "48123"
    assert county_fips("Brooks County, Tx") == "48047"
    assert county_fips("Grimes, Hunt") is None
