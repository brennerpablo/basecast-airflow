"""census_pep: trimmed rows of the four data files (Latin-1; a New Mexico county with "ñ" is dropped)."""

from datetime import date

import polars as pl

from basecast_pipelines.parsers.census.population import DATASETS, parse_population


def _value(df, fips, measure, ref):
    return df.filter((pl.col("county_fips") == fips) & (pl.col("measure") == measure) & (pl.col("ref_date") == ref))[
        "value"
    ].item()


def test_postcensal_latest_with_components(raw_file):
    f = raw_file("census_pep", "census_pep/co-est2025-alldata.csv")  # no manifest meta, like the real file
    df = parse_population(f)
    assert set(df["series"]) == {"postcensal_v2025"} and set(df["vintage"]) == {"Vintage 2025"}
    assert sorted(df["county_fips"].unique()) == ["48001", "48301", "48453"]  # state row and NM dropped
    assert _value(df, "48453", "estimates_base", date(2020, 4, 1)) == 1290157
    assert _value(df, "48453", "population", date(2020, 7, 1)) == 1296572
    assert {"births", "deaths", "natural_change", "domestic_mig", "net_mig", "r_net_mig"} <= set(df["measure"])
    assert DATASETS[0].inputs(f)


def test_vintage_2020_normalizes_measure_names(raw_file):
    df = parse_population(raw_file("census_pep", "census_pep/co-est2020-alldata.csv",
                                   meta={"series": "postcensal_v2020", "vintage": "Vintage 2020"}))
    assert set(df["series"]) == {"postcensal_v2020"}
    assert "natural_change" in set(df["measure"]) and "npopchg" in set(df["measure"])  # NATURALINC, NPOPCHG_2010
    assert _value(df, "48453", "census_population", date(2010, 4, 1)) > 1_000_000


def test_intercensal_2000_2010_unpadded_county_codes(raw_file):
    df = parse_population(raw_file("census_pep", "census_pep/co-est00int-tot.csv"))
    assert set(df["series"]) == {"intercensal_2000_2010"}
    assert sorted(df["county_fips"].unique()) == ["48001", "48301", "48453"]
    assert _value(df, "48301", "population", date(2000, 7, 1)) == 65
    assert df.filter(pl.col("county_fips") == "48453").height == 13  # base, 11 estimates, census 2010


def test_intercensal_2010_2020_year_codes(raw_file):
    df = parse_population(raw_file("census_pep", "census_pep/cc-est2020int-agesex-48.csv"))
    assert set(df["series"]) == {"intercensal_2010_2020"}
    anderson = df.filter(pl.col("county_fips") == "48001").sort("ref_date")
    assert anderson["ref_date"].to_list()[0] == date(2010, 4, 1) and anderson["measure"][0] == "estimates_base"
    assert anderson["ref_date"].to_list()[-1] == date(2020, 4, 1) and anderson["measure"][-1] == "census_population"
    assert anderson.filter(pl.col("measure") == "population")["year"].to_list() == list(range(2010, 2020))
    assert _value(df, "48001", "estimates_base", date(2010, 4, 1)) == 58457
