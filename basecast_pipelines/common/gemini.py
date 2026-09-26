"""Gemini on Vertex AI for reading numbers off chart images and image-only pages.

Every response is cached in the lake at
``derived/gemini/<source_id>/<raw sha256>__<task>__<prompt_version>__<model>.json``, so reruns never bill
again; a cached response is used even when Gemini is switched off. Settings (environment):

- ``GCP_PROJECT``: the Vertex AI project (unset → no new calls);
- ``BASECAST_GEMINI=off``: no new calls;
- ``GEMINI_MODEL``: default ``gemini-3.8-flash`` (Gemini 3.x answers only on location ``global``);
- ``GEMINI_MAX_CALLS``: new calls allowed per process (default 200);
- ``GEMINI_WORKERS``: concurrent calls for callers that fan out (default 4).

``google-genai`` and ``google-auth`` are imported only when a call is made, so parsing and tests work
without them. Numbers read by Gemini are stored with ``extraction_method = "gemini"`` and
``verified = false``.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from basecast_pipelines.common.storage import Storage

log = logging.getLogger(__name__)

LOCATION = "global"
DEFAULT_MODEL = "gemini-3.8-flash"
CACHE_PREFIX = "derived/gemini"
RETRIES = 3


@dataclass(frozen=True)
class Blob:
    """One input part: document bytes with their MIME type, or a text label."""

    data: bytes | str
    mime_type: str = "text/plain"


def model_name() -> str:
    return os.environ.get("GEMINI_MODEL") or DEFAULT_MODEL


def workers() -> int:
    return max(1, int(os.environ.get("GEMINI_WORKERS") or 4))


def cache_key(source_id: str, sha256: str, task: str, prompt_version: str, model: str) -> str:
    safe_model = re.sub(r"[^A-Za-z0-9._-]+", "_", model)
    return f"{CACHE_PREFIX}/{source_id}/{sha256}__{task}__{prompt_version}__{safe_model}.json"


@dataclass
class Stats:
    calls: int = 0
    cache_hits: int = 0
    skipped: int = 0
    failures: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    seconds: float = 0.0

    def line(self) -> str:
        return (f"gemini: {self.calls} calls, {self.cache_hits} cached, {self.skipped} skipped, {self.failures} failed, "
                f"{self.input_tokens:,} input / {self.output_tokens:,} output tokens, {self.seconds:.0f} s")


@dataclass
class Gemini:
    """Thread-safe caller with a lake cache and a call budget. One instance per process (``runner()``)."""

    project: str | None = None
    model: str = DEFAULT_MODEL
    enabled: bool = False
    max_calls: int = 200
    stats: Stats = field(default_factory=Stats)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _client: object = field(default=None, repr=False)
    _warned: set = field(default_factory=set, repr=False)

    @classmethod
    def from_env(cls) -> Gemini:
        project = os.environ.get("GCP_PROJECT") or None
        switch = (os.environ.get("BASECAST_GEMINI") or "on").strip().lower()
        return cls(
            project=project,
            model=model_name(),
            enabled=bool(project) and switch not in {"off", "0", "false", "no"},
            max_calls=int(os.environ.get("GEMINI_MAX_CALLS") or 200),
        )

    def _warn_once(self, key: str, message: str) -> None:
        with self._lock:
            if key in self._warned:
                return
            self._warned.add(key)
        log.warning(message)

    def extract_json(self, storage: Storage, *, source_id: str, sha256: str, task: str, prompt_version: str,
                     parts: list[Blob], prompt: str) -> dict | None:
        """The parsed JSON answer for ``parts`` + ``prompt``: from the lake cache, else from Gemini (when
        enabled and within budget). ``None`` when there is no answer; the cache entry also records usage."""
        key = cache_key(source_id, sha256, task, prompt_version, self.model)
        if storage.exists(key):
            with self._lock:
                self.stats.cache_hits += 1
            return json.loads(storage.read_bytes(key))
        if not self.enabled:
            with self._lock:
                self.stats.skipped += 1
            self._warn_once("disabled", "gemini: GCP_PROJECT unset or BASECAST_GEMINI=off; chart reading skipped "
                                        "(cached answers are still used)")
            return None
        with self._lock:
            if self.stats.calls >= self.max_calls:
                self.stats.skipped += 1
                over = True
            else:
                self.stats.calls += 1
                over = False
        if over:
            self._warn_once("budget", f"gemini: GEMINI_MAX_CALLS={self.max_calls} reached; remaining calls skipped")
            return None
        started = time.monotonic()
        try:
            text, usage = self._call_with_retries(parts, prompt)
        except Exception as exc:  # noqa: BLE001 - one failed document must not stop the run
            with self._lock:
                self.stats.failures += 1
            log.warning("gemini: %s %s failed: %s", task, sha256[:12], exc)
            return None
        elapsed = time.monotonic() - started
        try:
            answer = json.loads(_strip_fences(text))
        except json.JSONDecodeError:
            with self._lock:
                self.stats.failures += 1
            log.warning("gemini: %s %s returned non-JSON text", task, sha256[:12])
            return None
        record = {
            "model": self.model,
            "task": task,
            "prompt_version": prompt_version,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "latency_s": round(elapsed, 2),
            "usage": usage,
            "answer": answer,
        }
        storage.write_bytes(key, json.dumps(record, indent=1).encode(), overwrite=True)
        with self._lock:
            self.stats.input_tokens += usage.get("input_tokens") or 0
            self.stats.output_tokens += usage.get("output_tokens") or 0
            self.stats.seconds += elapsed
        return record

    def _call_with_retries(self, parts: list[Blob], prompt: str) -> tuple[str, dict]:
        for attempt in range(RETRIES):
            try:
                return self._generate(parts, prompt)
            except Exception as exc:  # noqa: BLE001
                code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
                retryable = code in {429, 500, 502, 503, 504} or code is None and "timeout" in str(exc).lower()
                if not retryable or attempt == RETRIES - 1:
                    raise
                delay = 2 ** (attempt + 1) + random.random() * 2
                log.info("gemini: %s, retrying in %.1f s", code or exc, delay)
                time.sleep(delay)
        raise RuntimeError("unreachable")

    def _generate(self, parts: list[Blob], prompt: str) -> tuple[str, dict]:
        """One Vertex AI call: (response text, token usage)."""
        from google.genai import types

        client = self._get_client()
        contents = [
            types.Part.from_bytes(data=p.data, mime_type=p.mime_type) if isinstance(p.data, bytes) else p.data
            for p in parts
        ]
        response = client.models.generate_content(
            model=self.model,
            contents=[*contents, prompt],
            config=types.GenerateContentConfig(response_mime_type="application/json", temperature=0),
        )
        meta = getattr(response, "usage_metadata", None)
        usage = {
            "input_tokens": getattr(meta, "prompt_token_count", None),
            "output_tokens": getattr(meta, "candidates_token_count", None),
        }
        return response.text or "", usage

    def _get_client(self):
        with self._lock:
            if self._client is None:
                import google.auth
                from google import genai

                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"], quota_project_id=self.project
                )
                self._client = genai.Client(vertexai=True, project=self.project, location=LOCATION, credentials=creds)
            return self._client


def _strip_fences(text: str) -> str:
    text = text.strip()
    match = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    return match.group(1) if match else text


_RUNNER: Gemini | None = None
_RUNNER_LOCK = threading.Lock()


def runner() -> Gemini:
    """The process-wide caller (settings read once from the environment)."""
    global _RUNNER
    with _RUNNER_LOCK:
        if _RUNNER is None:
            _RUNNER = Gemini.from_env()
        return _RUNNER


def reset() -> None:
    """Forget the process-wide caller (tests, or after changing the environment)."""
    global _RUNNER
    with _RUNNER_LOCK:
        _RUNNER = None
