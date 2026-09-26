"""Validation lock of phase 0: the analysis scripts never see where Base's partners fall.

``docs/PHASE0_ANALYSIS.md`` §0: the account score's weights are fixed without that information, so no
script in ``analysis/`` may import ``basecast_pipelines.models.partners`` or read ``base_public_facts``.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = ("models.partners", "models import partners", "base_public_facts")


def test_analysis_scripts_do_not_read_partners():
    scripts = sorted((ROOT / "analysis").rglob("*.py"))
    offenders = [
        f"{path.relative_to(ROOT)}: {token}"
        for path in scripts
        for token in FORBIDDEN
        if token in path.read_text()
    ]
    assert not offenders, offenders


def test_marts_do_not_read_partners():
    # the only future exception is marts/validation.py, after Pablo's go (A-M8)
    marts = ROOT / "basecast_pipelines" / "marts"
    offenders = [
        f"{path.name}: {token}"
        for path in sorted(marts.rglob("*.py"))
        for token in FORBIDDEN
        if token in path.read_text()
    ]
    assert not offenders, offenders


def test_models_other_than_partners_do_not_read_partners():
    models = ROOT / "basecast_pipelines" / "models"
    offenders = [
        f"{path.name}: {token}"
        for path in sorted(models.glob("*.py"))
        if path.name != "partners.py"
        for token in FORBIDDEN
        if token in path.read_text()
    ]
    assert not offenders, offenders
