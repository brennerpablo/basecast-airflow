"""bls_qcew: header plus an Alabama row and Texas state, unknown-county, disclosed and suppressed rows."""

from basecast_pipelines.parsers.bls.qcew import DATASETS, parse_qcew


def test_texas_rows_areas_and_suppression(raw_file):
    f = raw_file("bls_qcew", "bls_qcew/qcew_2025_annual_518210.csv", meta={"year": 2025, "naics": "518210"})
    df = parse_qcew(f)
    rows = {r["area_fips"]: r for r in df.iter_rows(named=True)}
    assert set(rows) == {"48000", "48001", "48005", "48453", "48999"}
    assert rows["48000"]["area_type"] == "state" and rows["48000"]["county_fips"] is None
    assert rows["48999"]["area_type"] == "county_unknown" and rows["48999"]["county_fips"] is None
    travis = rows["48453"]
    assert travis["county_fips"] == "48453" and travis["disclosed"] and travis["employment"] == 11266
    assert travis["establishments"] == 474 and travis["industry_code"] == "518210" and travis["year"] == 2025
    suppressed = rows["48005"]
    assert not suppressed["disclosed"] and suppressed["disclosure_code"] == "N"
    assert suppressed["establishments"] == 1  # still published
    assert suppressed["employment"] is None and suppressed["total_wages"] is None
    assert suppressed["avg_annual_pay"] is None and suppressed["lq_annual_avg_emplvl"] is None
    assert suppressed["lq_annual_avg_estabs"] == 0.11
    assert DATASETS[0].inputs(f)
