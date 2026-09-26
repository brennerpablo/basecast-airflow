from datetime import date
from pathlib import Path

import polars as pl

from basecast_pipelines.parsers.config_facts import (
    DATASETS,
    build_facts,
    build_figures,
    build_forecasts,
    build_points,
)


def test_the_versioned_config_files_load_with_types_and_flags():
    figures = build_figures([])
    assert figures.height == 10 and figures["verified"].all()
    assert figures.schema["mw"] == pl.Float64 and figures.schema["source_date"] == pl.Date
    march = figures.filter(pl.col("figure_id") == "large_load_observed_peak_non_simultaneous_2026_03").row(0, named=True)
    assert (march["mw"], march["as_of_text"], march["as_of_date"], march["page"]) == (4004, "2026-03", date(2026, 3, 1), "slide 6")
    assert set(figures["source_file"]) == {"config/manual_official_figures.yaml"}

    facts = build_facts([])
    assert facts.height == 70 and not facts["verified"].any()
    assert facts["fact_id"].n_unique() == 70 and facts["source_date"].null_count() < 70

    points = build_points([])
    assert points.height == 12
    assert set(points["weather_zone"]) == {"COAST", "EAST", "FWEST", "NORTH", "NCENT", "SOUTH", "SCENT", "WEST"}
    assert points.group_by("weather_zone").agg(pl.col("weight").sum())["weight"].round(3).to_list() == [1.0] * 8


def test_forecast_figures_in_the_common_shape():
    df = build_forecasts([])
    assert df.columns == [
        "vintage", "vintage_date", "publisher", "product", "target_year", "target_month", "season", "region_type",
        "region_id", "metric", "scenario", "value", "unit", "sheet", "row_label", "column_label", "source_file",
    ]
    rows = {r["row_label"]: r for r in df.iter_rows(named=True)}
    assert rows["ltlf_2026_preliminary_peak_2032"]["target_year"] == 2032
    assert rows["ltlf_2026_preliminary_peak_2032"]["value"] == 367_790
    summer = rows["ercot_summer_2026_peak_range_low"]
    assert (summer["target_year"], summer["season"], summer["scenario"], summer["value"]) == (2026, "summer", "range_low", 90_500)
    assert set(df["vintage_date"]) == {date(2026, 4, 15)} and set(df["region_id"]) == {"ERCOT"}


def test_a_small_config_dir(tmp_path: Path):
    (tmp_path / "manual_official_figures.yaml").write_text(
        "figures:\n"
        "  - {id: x, category: large_load, metric: tracked_mw, target: null, as_of: 2026-01-21, mw: 232500,\n"
        "     approx: true, source: s, source_date: 2026-02-13, source_url: u, page: 6, verified: true, notes: n}\n"
    )
    df = build_figures([], config_dir=tmp_path)
    row = df.row(0, named=True)
    assert (row["as_of_date"], row["page"], row["approx"], row["target"]) == (date(2026, 1, 21), "6", True, None)
    assert build_forecasts([], config_dir=tmp_path).height == 0  # not a load-forecast entry


def test_datasets_are_declared():
    assert {d.name: d.mode for d in DATASETS} == {
        "manual_official_figures": "replace", "manual_official_forecasts": "replace",
        "base_public_facts": "replace", "weather_points": "replace",
    }
