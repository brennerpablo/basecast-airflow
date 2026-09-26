"""NP12-215-ER fixture: the MPList zip trimmed to the QSE and TDSP sheets with four registrations each;
the representatives' names and emails were replaced with placeholders."""

from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot.mp_list import DATASETS, parse_participants


def test_participants_without_representatives(raw_file):
    f = raw_file(
        "ercot_mp_list", "ercot_mp_list/MPList.zip",
        name="rpt.00021129.0000000000000000.20260925.094033582.MPList.zip",
        meta={"friendly_name": "MPList", "publish_date": "2026-09-25T09:40:33-05:00"},
    )  # fmt: skip
    assert DATASETS[0].inputs(f)
    df = parse_participants(f)
    assert df.columns == ["sheet", "name", "entity_name", "short_name", "duns_number", "market_participant_type",
                          "sfa_effective_date", "snapshot_date"]  # fmt: skip
    text = " ".join(str(v) for row in df.iter_rows() for v in row)
    assert "Person" not in text and "example.test" not in text
    assert df.height == 8  # the date footer of each sheet is dropped
    assert set(df["sheet"]) == {"QSE", "TDSP"}
    aep = df.filter(pl.col("short_name") == "TAEPTC").row(0, named=True)
    assert aep["entity_name"] == "AEP TEXAS CENTRAL COMPANY"
    assert aep["duns_number"] == "007924772"  # leading zeros kept
    assert aep["sfa_effective_date"] == date(2015, 4, 1)
    assert df["snapshot_date"].unique().to_list() == [date(2026, 9, 25)]
