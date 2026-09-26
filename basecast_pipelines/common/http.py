"""One shared, polite HTTP client: per-host rate limits, retries with jittered exponential backoff,
``Retry-After`` support and a breaker that stops the run after repeated failures (ERCOT has suspended
API keys for high failure rates, so we stop and tell a human instead of hammering)."""

from __future__ import annotations

import hashlib
import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

# Requests per minute by host suffix. The ERCOT API allows 30/min; we use at most 20 on every ercot.com host.
DEFAULT_RATE_LIMITS: dict[str, float] = {
    "ercot.com": 20,
    "census.gov": 30,
    "eia.gov": 30,
    "arcgis.com": 30,
    # Open-Meteo's free tier allows 5,000 calls/hour and 10,000/day, and a one-year hourly request counts as
    # ~26 calls (one per two weeks): 3 requests/min keeps a backfill under the hourly cap.
    "open-meteo.com": 3,
    "puc.texas.gov": 30,
}
DEFAULT_RPM = 20.0
CHUNK_SIZE = 1 << 20


class HttpError(Exception):
    def __init__(self, url: str, message: str, status: int | None = None) -> None:
        super().__init__(f"{message} ({url})")
        self.url = url
        self.status = status


class TooManyFailuresError(HttpError):
    """Consecutive failures reached the limit: the run must stop and a human must look."""


class RateLimiter:
    """Evenly spaced request starts: at most ``rpm`` per minute, no bursts."""

    def __init__(
        self,
        rpm: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.interval = 60.0 / rpm
        self._clock = clock
        self._sleep = sleep
        self._next = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            wait = self._next - now
            self._next = max(now, self._next) + self.interval
        if wait > 0:
            self._sleep(wait)


@dataclass
class Download:
    url: str
    status: int
    headers: httpx.Headers
    path: Path | None
    sha256: str | None = None
    bytes: int = 0


@dataclass
class HttpStats:
    requests: int = 0
    retries: int = 0
    failures: int = 0
    bytes: int = 0
    failure_log: list[str] = field(default_factory=list)


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


class HttpClient:
    def __init__(
        self,
        *,
        user_agent: str,
        rate_limits: dict[str, float] | None = None,
        default_rpm: float = DEFAULT_RPM,
        max_retries: int = 4,
        backoff_base: float = 2.0,
        backoff_cap: float = 120.0,
        max_consecutive_failures: int = 6,
        timeout: httpx.Timeout | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = httpx.Client(
            headers={"User-Agent": user_agent},
            follow_redirects=True,
            timeout=timeout or httpx.Timeout(60.0, connect=20.0),
            transport=transport,
        )
        self.rate_limits = DEFAULT_RATE_LIMITS if rate_limits is None else rate_limits
        self.default_rpm = default_rpm
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.max_consecutive_failures = max_consecutive_failures
        self._sleep = sleep
        self._clock = clock
        self._limiters: dict[str, RateLimiter] = {}
        self._consecutive_failures = 0
        self.stats = HttpStats()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _limiter(self, url: str) -> RateLimiter:
        host = httpx.URL(url).host
        rpm = self.default_rpm
        for suffix, limit in self.rate_limits.items():
            if host == suffix or host.endswith("." + suffix):
                rpm = limit
                break
        if host not in self._limiters:
            self._limiters[host] = RateLimiter(rpm, clock=self._clock, sleep=self._sleep)
        return self._limiters[host]

    def _record_failure(self, url: str, reason: str) -> None:
        self._consecutive_failures += 1
        self.stats.failures += 1
        self.stats.failure_log.append(f"{reason}: {url}")
        log.warning("request failed (%s in a row): %s %s", self._consecutive_failures, reason, url)
        if self._consecutive_failures >= self.max_consecutive_failures:
            raise TooManyFailuresError(
                url, f"{self._consecutive_failures} consecutive failures, last: {reason}; stopping"
            )

    def _backoff(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, self.backoff_cap * 2.5)
        ceiling = min(self.backoff_cap, self.backoff_base * 2**attempt)
        return ceiling / 2 + random.uniform(0, ceiling / 2)

    def get(self, url: str, *, params: dict | None = None, headers: dict | None = None) -> httpx.Response:
        result = self._request(url, params=params, headers=headers, dest=None)
        assert isinstance(result, httpx.Response)
        return result

    def download(self, url: str, dest: Path, *, params: dict | None = None, headers: dict | None = None) -> Download:
        """Stream ``url`` into ``dest``. A 304 (conditional request) returns ``path=None``."""
        result = self._request(url, params=params, headers=headers, dest=dest)
        assert isinstance(result, Download)
        return result

    def _request(
        self, url: str, *, params: dict | None, headers: dict | None, dest: Path | None
    ) -> httpx.Response | Download:
        attempt = 0
        while True:
            self._limiter(url).acquire()
            self.stats.requests += 1
            retry_after = None
            try:
                if dest is None:
                    response = self._client.get(url, params=params, headers=headers)
                    status = response.status_code
                    if status < 400:
                        self.stats.bytes += len(response.content)
                        self._consecutive_failures = 0
                        return response
                else:
                    with self._client.stream("GET", url, params=params, headers=headers) as response:
                        status = response.status_code
                        if status < 400:
                            download = self._write_stream(response, dest)
                            self._consecutive_failures = 0
                            return download
                        response.read()
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                if status not in RETRYABLE_STATUS:
                    self._record_failure(url, f"HTTP {status}")
                    raise HttpError(url, f"HTTP {status}", status)
                self._record_failure(url, f"HTTP {status}")
                reason, last_status = f"HTTP {status}", status
            except (httpx.TransportError, _IncompleteBody) as exc:
                self._record_failure(url, type(exc).__name__)
                reason, last_status = f"{type(exc).__name__}: {exc}", None
            attempt += 1
            if attempt > self.max_retries:
                raise HttpError(url, f"giving up after {attempt} attempts, last: {reason}", last_status)
            delay = self._backoff(attempt, retry_after)
            self.stats.retries += 1
            log.info("retrying in %.1fs (%s): %s", delay, reason, url)
            self._sleep(delay)

    def _write_stream(self, response: httpx.Response, dest: Path) -> Download:
        if response.status_code == 304:
            return Download(url=str(response.url), status=304, headers=response.headers, path=None)
        digest = hashlib.sha256()
        size = 0
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as fh:
            for chunk in response.iter_bytes(CHUNK_SIZE):
                fh.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        expected = response.headers.get("Content-Length")
        if expected is not None and "Content-Encoding" not in response.headers and int(expected) != size:
            dest.unlink(missing_ok=True)
            raise _IncompleteBody(f"got {size} of {expected} bytes")
        self.stats.bytes += size
        return Download(
            url=str(response.url),
            status=response.status_code,
            headers=response.headers,
            path=dest,
            sha256=digest.hexdigest(),
            bytes=size,
        )


class _IncompleteBody(Exception):
    pass
