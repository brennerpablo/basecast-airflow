from __future__ import annotations

import hashlib

import httpx
import pytest

from basecast_pipelines.common.http import HttpError, RateLimiter, TooManyFailuresError


def test_rate_limiter_spaces_requests(clock) -> None:
    limiter = RateLimiter(20, clock=clock, sleep=clock.sleep)
    for _ in range(3):
        limiter.acquire()
    assert clock.sleeps == [3.0, 3.0]


def test_retries_5xx_then_succeeds(make_http, clock) -> None:
    responses = iter([httpx.Response(503), httpx.Response(200, text="ok")])
    http = make_http(lambda request: next(responses))
    assert http.get("https://www.ercot.com/x").text == "ok"
    assert http.stats.retries == 1


def test_honors_retry_after(make_http, clock) -> None:
    responses = iter([httpx.Response(429, headers={"Retry-After": "42"}), httpx.Response(200)])
    http = make_http(lambda request: next(responses), rate_limits={}, default_rpm=6000)
    http.get("https://example.org/x")
    assert 42 in clock.sleeps


def test_4xx_is_not_retried(make_http) -> None:
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(404)

    http = make_http(handler)
    with pytest.raises(HttpError) as info:
        http.get("https://www.ercot.com/missing")
    assert info.value.status == 404
    assert len(calls) == 1


def test_breaker_stops_after_consecutive_failures(make_http) -> None:
    http = make_http(lambda request: httpx.Response(500), max_retries=10, max_consecutive_failures=3)
    with pytest.raises(TooManyFailuresError):
        http.get("https://www.ercot.com/down")
    assert http.stats.requests == 3


def test_success_resets_failure_count(make_http) -> None:
    responses = iter([httpx.Response(500), httpx.Response(200), httpx.Response(500), httpx.Response(200)])
    http = make_http(lambda request: next(responses), max_consecutive_failures=2)
    http.get("https://www.ercot.com/a")
    http.get("https://www.ercot.com/b")


def test_download_streams_and_hashes(make_http, tmp_path) -> None:
    body = b"x" * 5000
    http = make_http(lambda request: httpx.Response(200, content=body))
    result = http.download("https://www.ercot.com/f.zip", tmp_path / "f.zip")
    assert result.bytes == 5000
    assert result.sha256 == hashlib.sha256(body).hexdigest()
    assert (tmp_path / "f.zip").read_bytes() == body


def test_user_agent_is_sent(make_http) -> None:
    seen = {}

    def handler(request):
        seen["ua"] = request.headers["User-Agent"]
        return httpx.Response(200)

    make_http(handler).get("https://www.ercot.com/")
    assert seen["ua"] == "basecast-tests"
