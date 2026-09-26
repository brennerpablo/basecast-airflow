"""tceq_air_permits: two trimmed export pages. Individuals' names and street addresses in the fixtures were
replaced with placeholders (DOE, JANE / 1 EXAMPLE RD) before committing; the last row of the North Texas page
repeats one row, as the real export does."""

import os
from datetime import date

import polars as pl
import pytest

from basecast_pipelines.parsers.texas.tceq_air_permits import DATA_CENTER_SQL, DATASETS, SQL_DATASETS, build_air_permits

NORTH = "tceq_air_permits/5eqq-7nad_north_texas_airnsr_0050000.csv"
DFW = "tceq_air_permits/t34q-qzi3_dallas_fort_worth_airnsr_0000000.csv"


def _pages(raw_file, dt=date(2026, 9, 26)):
    north = raw_file("tceq_air_permits", NORTH, dt=dt, meta={
        "dataset_id": "5eqq-7nad", "region": "North Texas", "program": "AIRNSR", "row_count": 10, "offset": 50000})
    dfw = raw_file("tceq_air_permits", DFW, dt=dt, meta={
        "dataset_id": "t34q-qzi3", "region": "Dallas / Fort Worth", "program": "AIRNSR", "row_count": 3, "offset": 0})
    return north, dfw


def test_pages_cleaned_deduplicated_and_anonymized(raw_file):
    north, dfw = _pages(raw_file)
    df = build_air_permits([north, dfw])
    assert df.height == 12  # 13 rows, one exact duplicate
    assert df["county_fips"].null_count() == 0 and set(df["county_fips"].cast(pl.String)) >= {"48011", "48121"}
    assert df.schema["affil_begin_dt"] == pl.Date and df.schema["tceq_region_number"] == pl.Int32
    llano = df.filter(pl.col("ref_num_txt") == "RN112229901")
    assert llano["affil_begin_dt"].null_count() == 1  # 1800-01-01 placeholder
    assert df["affil_end_dt"].drop_nulls().max() < date(2999, 1, 1)  # 3000-12-31 = open-ended → null
    people = df.filter(pl.col("princ_type").cast(pl.String).is_in(["INDIVIDUAL OWNER TYPE", "OTHER"]))
    assert people.height == 3
    assert people["princ_name"].null_count() == 3 and people["re_phys_loc_addr_line_1"].null_count() == 3
    assert "DOE, JANE" not in set(df["princ_name"].drop_nulls())
    companies = df.filter(pl.col("princ_type").cast(pl.String) == "CORPORATION")
    assert companies["princ_name"].null_count() == 0
    assert "re_phys_loc_desc" not in df.columns and "princ_ref_num_txt" in df.columns
    assert set(df["registry_region"].cast(pl.String)) == {"North Texas", "Dallas / Fort Worth"}
    assert df["source_file"].cast(pl.String).is_in([north.key, dfw.key]).all()


def test_only_newest_snapshot_and_missing_pages(raw_file):
    old_north, _ = _pages(raw_file, dt=date(2026, 9, 20))
    north, dfw = _pages(raw_file)
    assert DATASETS[0].files([old_north, north, dfw]) == [north, dfw]
    short = raw_file("tceq_air_permits", DFW, dt=date(2026, 9, 27), meta={
        "dataset_id": "t34q-qzi3", "region": "Dallas / Fort Worth", "row_count": 95605, "offset": 0})
    with pytest.raises(ValueError, match="page is missing"):
        build_air_permits([short])


def test_data_center_sql_matches_the_decision_filter():
    assert SQL_DATASETS[0].name == "tceq_data_center_sites"
    assert "DATA ?CENTER|DATACENTER|DATA CTR" in DATA_CENTER_SQL and "'518210'" in DATA_CENTER_SQL
    assert "GROUP BY ref_num_txt" in DATA_CENTER_SQL


def test_data_center_sql_on_postgres(raw_file):
    """Runs the SQL against a TEMP table (never the shared ``tceq_air_permits``) when BASECAST_TEST_DB_URL is set."""
    url = os.environ.get("BASECAST_TEST_DB_URL")
    if not url:
        pytest.skip("BASECAST_TEST_DB_URL not set")
    from psycopg import sql

    from basecast_pipelines.common.db import column_types, connect, copy_frame

    df = build_air_permits(list(_pages(raw_file)))
    with connect(url) as conn, conn.transaction():
        cols = sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
                                  for c, t in column_types(df).items())
        conn.execute(sql.SQL("CREATE TEMP TABLE tceq_air_permits ({}) ON COMMIT DROP").format(cols))
        copy_frame(conn, "tceq_air_permits", df)
        sites = {r[0]: r for r in conn.execute(
            f"SELECT ref_num_txt, county_fips, first_affil_begin_dt, has_undated_affiliation, has_pending "
            f"FROM ({DATA_CENTER_SQL}) s").fetchall()}
    assert set(sites) == {"RN112229901", "RN112532585", "RN105464747"}
    assert sites["RN112229901"][2:4] == (date(2025, 6, 11), True)  # the decision dated it 1800-01-01
    assert sites["RN112532585"][1] == "48151" and sites["RN112532585"][4] is True
