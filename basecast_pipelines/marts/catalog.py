"""Every mart, in build order (a mart that reads another comes after it)."""

from __future__ import annotations

from basecast_pipelines.marts import accounts, backtest
from basecast_pipelines.marts.core import Mart

MARTS: dict[str, Mart] = {m.name: m for m in (*accounts.MARTS, *backtest.MARTS)}


def select(names: list[str] | None) -> list[Mart]:
    """The marts named (all when None), in build order; an unknown name raises."""
    if not names:
        return list(MARTS.values())
    unknown = sorted(set(names) - set(MARTS))
    if unknown:
        raise KeyError(f"unknown marts: {unknown} (see `basecast marts list`)")
    return [m for m in MARTS.values() if m.name in names]
