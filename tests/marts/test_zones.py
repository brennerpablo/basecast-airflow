"""The zone layer marts' pure parts (``basecast_pipelines/marts/zones.py``) on tiny synthetic frames (no database)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest

from basecast_pipelines.marts import core
from basecast_pipelines.marts import zones as ZN

ZONES = ZN.ZONES
CAVEATS = {"machine_read_unverified", "preliminary_actuals", "weights_pending_review", "band_uncalibrated",
           "beyond_backtested_window", "allocated_statewide", "by_county_not_point", "by_area_not_homes",
           "requests_not_forecasts", "policy_pause_2026", "optimistic_weather", "simulated"}
CONTRACT_MEASURES = {"excess_share", "min_max_ratio_2019", "min_max_ratio_2026", "a2e_stock", "pipeline_2032",
                     "u_share"}
FIXTURE_ZONE_COLUMNS = {"weather_zone", "measure", "label", "unit", "method", "central", "low", "high", "verified"}
FIXTURE_COUNTY_COLUMNS = {"county_fips", "county_name", "weather_zone", "allocated_a2e_mw", "named_by_ercot",
                          "observed_base_mw", "observed_base_studied_mw", "verified"}


def _even(value: float = 1.0) -> dict[str, float]:
    return {z: value / len(ZONES) for z in ZONES}


def _excess() -> pl.DataFrame:
    """Coincident excess by zone and year, plus the ERCOT row (which the shares leave out)."""
    rows = []
    for i, z in enumerate(ZONES):
        for y in (2022, 2023, 2024):
            rows.append({"weather_zone": z, "year": y, "excess_mw": float(100 * (i + 1) + (1_000 if y == 2022 else 0))})
    rows[-1]["excess_mw"] = -500.0  # WEST 2024
    rows.append({"weather_zone": "ERCOT", "year": 2023, "excess_mw": 99_999.0})
    return pl.DataFrame(rows)


def test_excess_shares_average_the_years_and_keep_negative_zones():
    mw, share = ZN.excess_shares(_excess(), (2023, 2024))
    assert mw["COAST"] == pytest.approx(100.0)  # 2022 left out
    assert mw["WEST"] == pytest.approx((800 - 500) / 2)
    assert sum(share.values()) == pytest.approx(1.0)
    assert share["COAST"] == pytest.approx(100 / sum(mw.values()))
    neg = _excess().with_columns(pl.when(pl.col("weather_zone") == "EAST").then(-300.0)
                                 .otherwise(pl.col("excess_mw")).alias("excess_mw"))
    assert ZN.excess_shares(neg, (2023, 2024))[1]["EAST"] < 0  # not clipped, unlike X11's shares


def test_excess_shares_raise_without_a_zone():
    with pytest.raises(ValueError, match="WEST"):
        ZN.excess_shares(_excess().filter(pl.col("weather_zone") != "WEST"), (2023,))


def test_min_max_ratios_pick_the_year_and_raise_on_a_gap():
    shape = pl.DataFrame({"weather_zone": [*ZONES, *ZONES], "year": [2019] * 8 + [2026] * 8,
                          "min_max_ratio": [0.5 + i / 100 for i in range(16)]})
    assert ZN.min_max_ratios(shape, 2026)["COAST"] == pytest.approx(0.58)
    with pytest.raises(ValueError, match="2020"):
        ZN.min_max_ratios(shape, 2020)


def test_measure_rows_carry_the_contract_fields_and_null_bands():
    rows = ZN.measure_rows("a2e_stock", _even(800.0), low=_even(400.0), high=_even(1_600.0), verified=False,
                           extra={"lz_west_fraction": {"NORTH": 0.72}})
    rows += ZN.measure_rows("excess_share", _even(), verified=True)
    f = ZN.zone_frame(rows)
    assert FIXTURE_ZONE_COLUMNS <= set(f.columns)
    stock = f.filter(pl.col("measure") == "a2e_stock")
    assert stock.height == 8 and stock["unit"].unique().to_list() == ["MW"]
    assert stock["method"].unique().to_list() == ["allocated"] and not stock["verified"].any()
    assert stock.filter(pl.col("weather_zone") == "NORTH")["lz_west_fraction"].item() == 0.72
    assert stock.filter(pl.col("weather_zone") == "COAST")["lz_west_fraction"].item() is None
    share = f.filter(pl.col("measure") == "excess_share")
    assert share["low"].null_count() == 8 and share["high"].null_count() == 8 and share["verified"].all()
    assert share["method"].unique().to_list() == ["observed"]


def test_approved_stock_is_the_latest_deck():
    lzs = pl.DataFrame({"report_date": [date(2026, 5, 21), date(2026, 6, 19)], "lz_west": [4_900.0, 5_050.0],
                        "other": [3_800.0, 3_876.0], "src": ["chart", "chart"]}).with_columns(
        (pl.col("lz_west") / (pl.col("lz_west") + pl.col("other"))).alias("lz_west_share"))
    s = ZN.approved_stock(lzs)
    assert s["report_date"] == date(2026, 6, 19) and s["mw"] == 8_926.0
    assert s["lz_west_share"] == pytest.approx(5_050 / 8_926)
    with pytest.raises(ValueError):
        ZN.approved_stock(lzs.head(0))


def _signals() -> pl.DataFrame:
    sites = pl.DataFrame({"county_fips": ["48001", "48001", "48003"],
                          "first_affil_begin_dt": [date(2020, 1, 1), date(2025, 3, 1), None]})
    agreements = pl.DataFrame({"county_fips": ["48003"]})
    registrations = pl.DataFrame({"county_fips": ["48005", None]})
    return ZN.county_signals(sites, agreements, registrations)


def test_county_signals_count_each_source_once():
    s = {r["county_fips"]: r for r in _signals().iter_rows(named=True)}
    assert set(s) == {"48001", "48003", "48005"}  # the registration without a county is dropped
    assert (s["48001"]["tceq_sites"], s["48001"]["tceq_sites_since_2025"], s["48001"]["n_signals"]) == (2, 1, 2)
    assert (s["48003"]["tceq_sites"], s["48003"]["cpa_dc_agreements"], s["48003"]["n_signals"]) == (1, 1, 2)
    assert s["48003"]["tceq_sites_since_2025"] == 0  # an undated site is not recent
    assert (s["48005"]["cpa_dc_registrations"], s["48005"]["n_signals"]) == (1, 1)


def _cv() -> pl.DataFrame:
    return pl.DataFrame({
        "document": [ZN.F.BZ_DOC] * 5 + ["other.pdf"],
        "chart_title": ["Top base load counties", "Top base load counties", "Top base + studied load counties",
                        "Top base + studied load counties", "Base & studied load by region, MW",
                        "Top base load counties"],
        "category": ["Anderson", "Andrews ", "Andrews", "Angelina", "North", "Anderson"],
        "status_label": ["Base load", "Base load", "Base + studied load", "Base + studied load", "Base load",
                         "Base load"],
        "value_mw": [1_000.0, 500.0, 900.0, 7_000.0, 14_222.0, 1.0],
    })


def test_named_counties_pivot_the_two_lists():
    n = {r["county_name"]: r for r in ZN.named_counties(_cv()).iter_rows(named=True)}
    assert set(n) == {"Anderson", "Andrews", "Angelina"}  # the zone chart and other decks are left out
    assert (n["Anderson"]["observed_base_mw"], n["Anderson"]["observed_base_studied_mw"]) == (1_000.0, None)
    assert (n["Andrews"]["observed_base_mw"], n["Andrews"]["observed_base_studied_mw"]) == (500.0, 900.0)
    assert (n["Angelina"]["observed_base_mw"], n["Angelina"]["observed_base_studied_mw"]) == (None, 7_000.0)
    with pytest.raises(ValueError, match="no named counties"):
        ZN.named_counties(_cv().filter(pl.col("document") == "other.pdf"))


def _counties() -> pl.DataFrame:
    return pl.DataFrame({"county_fips": ["48001", "48003", "48005", "48007"],
                         "county_name": ["Anderson", "Andrews", "Angelina", "Aransas"],
                         "weather_zone": ["EAST", "FWEST", "EAST", "COAST"],
                         "share_east": [1.0, 0.0, 1.0, 0.0], "share_fwest": [0.0, 1.0, 0.0, 0.0],
                         "share_coast": [0.0, 0.0, 0.0, 1.0]})


def test_county_rows_fill_zero_mark_named_and_keep_observed_mw():
    from basecast_pipelines.models import large_load_geo as geo

    czl = geo.county_zone_long(_counties())
    allocated = geo.county_pressure({"a2e_stock_mw": {"EAST": 300.0, "FWEST": 100.0, "COAST": 50.0}}, _signals(),
                                    czl)
    c = ZN.county_rows(_counties(), _signals(), allocated, ZN.named_counties(_cv()))
    assert tuple(c.columns) == ZN.COUNTY_COLUMNS and FIXTURE_COUNTY_COLUMNS <= set(c.columns)
    by = {r["county_name"]: r for r in c.iter_rows(named=True)}
    # EAST's 300 MW split by signals: Anderson 2, Angelina 1
    assert by["Anderson"]["allocated_a2e_mw"] == pytest.approx(200.0)
    assert by["Angelina"]["allocated_a2e_mw"] == pytest.approx(100.0)
    assert by["Andrews"]["allocated_a2e_mw"] == pytest.approx(100.0)
    assert by["Aransas"]["allocated_a2e_mw"] == 0.0 and by["Aransas"]["n_signals"] == 0  # no signal: 0, COAST unplaced
    assert [by[n]["named_by_ercot"] for n in ("Anderson", "Andrews", "Angelina", "Aransas")] == [True, True, True,
                                                                                                 False]
    assert by["Aransas"]["observed_base_mw"] is None and by["Angelina"]["observed_base_studied_mw"] == 7_000.0
    assert not c["verified"].any()
    with pytest.raises(ValueError, match="Zavala"):
        ZN.county_rows(_counties(), _signals(), allocated,
                       pl.DataFrame({"county_name": ["Zavala"], "observed_base_mw": [1.0],
                                     "observed_base_studied_mw": [None]},
                                    schema={"county_name": pl.String, "observed_base_mw": pl.Float64,
                                            "observed_base_studied_mw": pl.Float64}))


# --- the build wiring, on synthetic inputs -------------------------------------------------------------------


def _inputs() -> ZN.ZoneInputs:
    stock = {z: (0.3 if z == "NORTH" else 0.1) for z in ZONES}
    unraked = {z: 1 / 8 for z in ZONES}
    return ZN.ZoneInputs(
        excess_mw=_even(8_000.0), excess_share=_even(), ercot_excess={2026: 8_000.0},
        ratios={2019: {**_even(4.8), "ERCOT": 0.61}, 2026: {**_even(5.6), "ERCOT": 0.68}},
        load_through=date(2026, 9, 25),
        split={"stock": stock, "stock_candidates": {ZN.STOCK_CENTRAL: stock, "bz_base_unraked": unraked},
               "unattributed": _even(), "f_north": 0.72},
        stock={"report_date": date(2026, 6, 19), "mw": 1_000.0, "lz_west_mw": 560.0, "other_mw": 440.0,
               "lz_west_share": 0.56, "src": "chart"},
        bz_base=_even(8_000.0), bz_studied={"NCENT": 2_000.0}, bz_report_date=date(2026, 9, 14),
    )


def _ctx(as_of: date = date(2026, 9, 26)) -> core.MartContext:
    return core.MartContext(as_of, {}, lambda *a, **k: None, cache={"zones.inputs": _inputs()})


def test_build_zone_layers_gives_six_measures_and_passes_the_structural_checks():
    ctx = _ctx(date(2026, 1, 1))  # golden checks skip at another as_of
    f = ZN.build_zone_layers(ctx)
    assert f.height == 48 and set(f["measure"]) == CONTRACT_MEASURES
    stock = f.filter(pl.col("measure") == "a2e_stock")
    north = stock.filter(pl.col("weather_zone") == "NORTH").row(0, named=True)
    assert (north["central"], north["low"], north["high"]) == pytest.approx((300.0, 125.0, 300.0))
    assert north["lz_west_fraction"] == 0.72
    assert stock["central"].sum() == pytest.approx(1_000.0)
    pipe = f.filter((pl.col("measure") == "pipeline_2032") & (pl.col("weather_zone") == "NCENT"))
    assert pipe["central"].item() == pytest.approx(3_000.0) and pipe["low"].item() is None
    stamped = core.stamp(f, ZN.ZONE_LAYERS, code="t", as_of=ctx.as_of, built_at=datetime.now(timezone.utc))
    results = core.run_checks(ZN.ZONE_LAYERS, stamped, ctx.as_of, ctx.config)
    assert [r for r in results if r["status"] == "failed"] == []
    assert {r["status"] for r in results} == {"passed", "skipped"}
    meta = ZN._zone_meta(f, ctx)
    assert meta["ercot_min_max_ratio"] == {2019: 0.61, 2026: 0.68} and meta["f_north"] == 0.72


def test_build_county_large_load_spreads_the_stock_and_marks_named(monkeypatch):
    from basecast_pipelines.models import data_centers as dc
    from basecast_pipelines.models import large_load_geo as geo

    monkeypatch.setattr(geo, "load_county_weather_zone", _counties)
    monkeypatch.setattr(dc, "load_sites", lambda: pl.DataFrame(
        {"county_fips": ["48001", "48003"], "first_affil_begin_dt": [date(2025, 2, 1), date(2019, 1, 1)]}))
    monkeypatch.setattr(geo, "load_dc_agreements", lambda: pl.DataFrame({"county_fips": ["48005"]}))
    monkeypatch.setattr(geo, "load_cpa_data_centers", lambda: pl.DataFrame({"county_fips": [None]},
                                                                            schema={"county_fips": pl.String}))
    monkeypatch.setattr(ZN.F, "deck_inputs", lambda ctx: SimpleNamespace(cv=_cv()))
    c = ZN.build_county_large_load(_ctx())
    by = {r["county_name"]: r for r in c.iter_rows(named=True)}
    # the stock split puts 100 MW in EAST (Anderson, Angelina: one signal each) and 100 MW in FWEST (Andrews)
    assert by["Anderson"]["allocated_a2e_mw"] == pytest.approx(50.0)
    assert by["Andrews"]["allocated_a2e_mw"] == pytest.approx(100.0)
    assert by["Aransas"]["allocated_a2e_mw"] == 0.0
    assert by["Angelina"]["named_by_ercot"] and by["Angelina"]["observed_base_studied_mw"] == 7_000.0
    results = core.run_checks(ZN.COUNTY_LARGE_LOAD, c, date(2026, 1, 1))
    assert [r for r in results if r["status"] == "failed"] == []


# --- checks and declarations ---------------------------------------------------------------------------------


def test_band_check_catches_a_central_outside_its_band():
    rows = ZN.measure_rows("a2e_stock", _even(800.0), low=_even(900.0), high=_even(1_600.0), verified=False)
    r = ZN._band_holds_central(ZN.zone_frame(rows))
    assert not r.passed and len(r.actual) == 8
    ok = ZN.measure_rows("u_share", _even(), verified=False)  # no band: nothing to hold
    assert ZN._band_holds_central(ZN.zone_frame(ok)).passed


def test_golden_checks_run_only_at_the_golden_as_of():
    for mart in ZN.MARTS:
        golden = [c for c in mart.checks if c.as_of is not None]
        assert golden and all(c.as_of == ZN.GOLDEN_AS_OF for c in golden)
    names = [c.name for m in ZN.MARTS for c in m.checks]
    assert len(names) == len(set(names))


def test_marts_are_declared_for_the_contract():
    assert [m.name for m in ZN.MARTS] == ["mart_zone_layers", "mart_county_large_load"]
    assert set(ZN.MEASURES) == CONTRACT_MEASURES
    assert {m for m, (_, _, method, _) in ZN.MEASURES.items() if method == "allocated"} == {"a2e_stock", "u_share"}
    assert ZN.MEASURES["pipeline_2032"][2] == "observed by zone"
    assert ZN.ZONE_LAYERS.key == ("weather_zone", "measure") and ZN.COUNTY_LARGE_LOAD.key == ("county_fips",)
    assert set(ZN.ZONE_LAYERS.caveats) == {"allocated_statewide", "machine_read_unverified"}
    for m in ZN.MARTS:
        assert set(m.caveats) <= CAVEATS, m.name
        assert m.inputs and m.description
