"""``config/marts.yaml``: the build switches (BUILD_00 §3) and the validation lock, checked on load."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from basecast_pipelines.config import PROJECT_ROOT

PATH = PROJECT_ROOT / "config" / "marts.yaml"
STATUSES = {"default_pending_review", "reviewed"}
# Allowed values per switch; a switch not listed here takes any value of the right shape.
ALLOWED: dict[str, set[Any]] = {
    "scoring.weights_set": {"q3", "x4"},
    "large_load.mode": {"estimated", "scenarios"},
    "large_load.zone_allocation": {"x11", "x7_fixed"},
    "queue.model": {"entry_ia_sm", "cohort"},
    "forecast.unattributed_layer": {"separate"},
    "forecast.default_variant": {"deck_pre_batch_zero", "deck_latest", "approvals_pace"},
    "dc_sites.metro_rule": {"county_list_13", "density_100"},
    "dc_sites.include_naics_only": {True, False},
    "triggers.gen_storage_ia": {"strong", "context"},
    "munis.place_facts": {True, False},
    "munis.place_permit_trigger": {True, False},
}
N_HELD_OUT = 5


def load(path: Path = PATH) -> dict[str, Any]:
    """The raw YAML, validated: every switch is ``{value, status}`` with an allowed value and status, and the
    validation block holds the five held-out names while ``revealed`` is false."""
    config = yaml.safe_load(path.read_text())
    problems = []
    for group, switches in config.items():
        if group == "validation":
            continue
        for name, switch in switches.items():
            key = f"{group}.{name}"
            if not isinstance(switch, Mapping) or set(switch) != {"value", "status"}:
                problems.append(f"{key}: expected {{value, status}}")
                continue
            if switch["status"] not in STATUSES:
                problems.append(f"{key}: status {switch['status']!r} not in {sorted(STATUSES)}")
            if key in ALLOWED and switch["value"] not in ALLOWED[key]:
                problems.append(f"{key}: {switch['value']!r} not in {sorted(map(str, ALLOWED[key]))}")
    missing = sorted(set(ALLOWED) - {f"{g}.{n}" for g, s in config.items() if g != "validation" for n in s})
    if missing:
        problems.append(f"missing switches: {missing}")
    validation = config.get("validation") or {}
    if validation.get("revealed") is not False:
        problems.append("validation.revealed must stay false until A-M8")
    names = validation.get("held_out_names") or []
    if len(names) != N_HELD_OUT or len(set(names)) != N_HELD_OUT:
        problems.append(f"validation.held_out_names needs {N_HELD_OUT} distinct names")
    if problems:
        raise ValueError(f"{path.name}: " + "; ".join(problems))
    return config


def value(config: Mapping[str, Any], key: str) -> Any:
    """A switch's value by dotted key (``"forecast.default_variant"``)."""
    group, name = key.split(".", 1)
    return config[group][name]["value"]


def status(config: Mapping[str, Any], key: str) -> str:
    group, name = key.split(".", 1)
    return config[group][name]["status"]


def held_out_names(config: Mapping[str, Any]) -> list[str]:
    return list(config["validation"]["held_out_names"])
