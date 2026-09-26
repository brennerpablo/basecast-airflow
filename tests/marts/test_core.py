"""The mart layer (``basecast_pipelines/marts/core.py``, ``config.py``, ``catalog.py``) on toy marts."""

from __future__ import annotations

import ast
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from basecast_pipelines.common.db import _array_literal
from basecast_pipelines.marts import catalog, core
from basecast_pipelines.marts import config as marts_config

ROOT = Path(__file__).resolve().parents[2]
AS_OF = date(2026, 9, 26)


def _toy(rows: list[int], *, expected: int = 3, name: str = "mart_toy", version: int = 1) -> core.Mart:
    return core.Mart(
        name=name,
        build=lambda ctx: pl.DataFrame({"k": rows, "tags": [["a", "b"]] * len(rows)}),
        key=("k",),
        inputs=("some_table",),
        description="toy",
        version=version,
        caveats=("band_uncalibrated",),
        checks=(
            core.value_check("rows", lambda f: f.height, expected, as_of=AS_OF),
            core.value_check("unique k", lambda f: f["k"].is_duplicated().any(), False),
        ),
        meta=lambda frame, ctx: {"legend_breaks": [0.1, 0.2]},
    )


def _ctx(as_of: date = AS_OF) -> core.MartContext:
    return core.MartContext(as_of, {}, read_sql=lambda *a, **k: pl.DataFrame())


def test_marts_are_named_mart_star():
    with pytest.raises(ValueError, match="mart_"):
        core.Mart("toy", lambda ctx: pl.DataFrame(), ("k",), (), "")
    assert _toy([1]).meta_name == "toy"


def test_stamp_adds_provenance_and_keeps_a_marts_own_as_of():
    built = datetime(2026, 9, 26, 23, 0, tzinfo=timezone.utc)
    out = core.stamp(pl.DataFrame({"k": [1]}), _toy([1], version=2), code="abc1234", as_of=AS_OF, built_at=built)
    assert out.row(0, named=True) == {"k": 1, "model_version": "abc1234.v2", "as_of": AS_OF, "built_at": built}
    own = core.stamp(pl.DataFrame({"as_of": [date(2024, 7, 31)]}), _toy([1]), code="x", as_of=AS_OF, built_at=built)
    assert own["as_of"].to_list() == [date(2024, 7, 31)]  # a backtest cell's date is not overwritten


def test_golden_checks_run_only_at_their_as_of():
    mart = _toy([1, 2, 3])
    frame = mart.build(_ctx())
    assert [c["status"] for c in core.run_checks(mart, frame, AS_OF)] == ["passed", "passed"]
    other = core.run_checks(mart, frame, date(2026, 9, 27))
    assert [c["status"] for c in other] == ["skipped", "passed"]
    failed = core.run_checks(_toy([1, 2]), _toy([1, 2]).build(_ctx()), AS_OF)
    assert failed[0] | {} == {"mart": "mart_toy", "check": "rows", "status": "failed", "expected": 3, "actual": 2}
    broken = core.Mart("mart_b", lambda c: pl.DataFrame(), (), (), "", checks=(
        core.Check("boom", lambda f: 1 / 0),))
    assert core.run_checks(broken, pl.DataFrame(), AS_OF)[0]["status"] == "failed"


def test_meta_rows_carry_as_of_caveats_inputs_and_the_marts_values():
    built = datetime(2026, 9, 26, tzinfo=timezone.utc)
    mart = _toy([1])
    meta = core.meta_rows(mart, mart.build(_ctx()), _ctx(), model_version="abc.v1", built_at=built)
    values = {r["key"]: json.loads(r["value"]) for r in meta.iter_rows(named=True)}
    assert values == {"as_of": "2026-09-26", "caveats": ["band_uncalibrated"], "inputs": ["some_table"],
                      "legend_breaks": [0.1, 0.2]}
    assert set(meta["mart"]) == {"toy"}


def test_dry_run_writes_parquet_and_skips_a_mart_that_fails(tmp_path):
    outcomes = core.build_marts([_toy([1, 2, 3]), _toy([1], name="mart_bad")], _ctx(), code="abc", dry_dir=tmp_path)
    assert [(o.name, o.written) for o in outcomes] == [("mart_toy", True), ("mart_bad", False)]
    assert "golden check failed: rows" in outcomes[1].reason
    written = pl.read_parquet(tmp_path / "mart_toy.parquet")
    assert written.columns == ["k", "tags", "model_version", "as_of", "built_at"]
    assert (tmp_path / "mart_meta__toy.parquet").exists() and not (tmp_path / "mart_bad.parquet").exists()
    with pytest.raises(ValueError, match="exactly one"):
        core.build_marts([], _ctx(), code="abc")


def test_code_version_prefers_the_deploy_sha(monkeypatch):
    monkeypatch.setenv("BASECAST_GIT_SHA", "0123456789abcdef")
    assert core.code_version(ROOT) == "0123456789ab" and core.is_clean("0123456789ab")
    assert not core.is_clean("abc-dirty") and not core.is_clean("unknown")


def test_array_literal_quotes_every_element():
    assert _array_literal(["a", 'b"c', None, "d\\e"]) == '{"a","b\\"c",NULL,"d\\\\e"}'
    assert _array_literal([]) == "{}" and _array_literal(None) is None


def test_the_marts_config_loads_and_rejects_bad_values(tmp_path):
    config = marts_config.load()
    assert marts_config.value(config, "forecast.default_variant") == "deck_pre_batch_zero"
    assert marts_config.status(config, "scoring.weights_set") == "default_pending_review"
    bad = tmp_path / "marts.yaml"
    bad.write_text(marts_config.PATH.read_text().replace("value: q3,", "value: q9,"))
    with pytest.raises(ValueError, match="scoring.weights_set"):
        marts_config.load(bad)
    revealed = tmp_path / "revealed.yaml"
    revealed.write_text(marts_config.PATH.read_text().replace("revealed: false", "revealed: true"))
    with pytest.raises(ValueError, match="revealed"):
        marts_config.load(revealed)


def test_held_out_names_match_the_analysis_scripts():
    """The five names moved to config/marts.yaml from q3_signals.py (exact); the other reference scripts keep their
    own five (q2 spells one with its acronym, which the fuzzy match takes)."""
    names = marts_config.held_out_names(marts_config.load())
    lists = {}
    for path in sorted((ROOT / "analysis").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "PARTNER_NAMES" for t in node.targets):
                lists[path.name] = ast.literal_eval(node.value)
    assert lists["q3_signals.py"] == names
    assert all(len(v) == len(names) for v in lists.values()), lists


def test_catalog_selects_in_build_order():
    assert catalog.select(None) == list(catalog.MARTS.values())
    with pytest.raises(KeyError, match="unknown"):
        catalog.select(["mart_nope"])


def test_build_writes_postgres_registry_and_keeps_the_old_version_on_a_failed_check(monkeypatch):
    """Needs a Postgres; runs only when BASECAST_TEST_DB_URL is set (e.g. the local PostGIS container)."""
    url = os.environ.get("BASECAST_TEST_DB_URL")
    if not url:
        pytest.skip("BASECAST_TEST_DB_URL not set")
    from basecast_pipelines.common.db import connect

    good, bad = _toy([1, 2, 3]), _toy([1, 2])
    monkeypatch.setattr(catalog, "MARTS", {good.name: good})
    with connect(url) as conn:
        for t in ("mart_toy", "mart_meta", "dataset_registry"):
            conn.execute(f"DROP TABLE IF EXISTS {t}")
    core.build_marts([good], _ctx(), code="abc", db_url=url)
    outcome = core.build_marts([bad], _ctx(), code="def", db_url=url)[0]
    assert not outcome.written
    with connect(url) as conn:
        rows = conn.execute("SELECT k, tags, model_version FROM mart_toy ORDER BY k").fetchall()
        meta = dict(conn.execute("SELECT key, value FROM mart_meta WHERE mart = 'toy'").fetchall())
        registry = conn.execute(
            "SELECT source_id, dataset, mode, key_columns, inputs FROM dataset_registry").fetchall()
    assert rows == [(1, ["a", "b"], "abc.v1"), (2, ["a", "b"], "abc.v1"), (3, ["a", "b"], "abc.v1")]
    assert meta["legend_breaks"] == [0.1, 0.2] and meta["as_of"] == "2026-09-26"
    assert registry == [("marts", "mart_toy", "replace", ["k"], ["some_table"])]
