"""The Airflow image installs the pipeline libraries at the same versions as pyproject.toml."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _pins(lines: list[str]) -> dict[str, str]:
    pins = {}
    for line in lines:
        match = re.match(r"^([A-Za-z0-9_.-]+)(?:\[[^\]]*\])?==([^\s;]+)", line.strip())
        if match:
            pins[match.group(1).lower()] = match.group(2)
    return pins


def test_image_requirements_match_pyproject():
    project = _pins(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"])
    image = _pins((ROOT / "deploy/airflow/requirements.txt").read_text().splitlines())
    assert image, "no pins in deploy/airflow/requirements.txt"
    for name, version in image.items():
        assert project.get(name) == version, f"{name}: image {version}, pyproject {project.get(name)}"
