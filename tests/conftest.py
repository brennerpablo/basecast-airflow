from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.storage import LocalStorage

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(tmp_path / "lake")


class FakeClock:
    """Deterministic time for the rate limiter and backoff: sleeping just advances the clock."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def make_http(clock: FakeClock):
    def factory(handler, **kwargs) -> HttpClient:
        return HttpClient(
            user_agent="basecast-tests",
            transport=httpx.MockTransport(handler),
            sleep=clock.sleep,
            clock=clock,
            **kwargs,
        )

    return factory
