"""The Gemini helper answers from the lake cache, and never calls out when switched off or over budget."""

from __future__ import annotations

import json

from basecast_pipelines.common.gemini import Blob, Gemini, cache_key


def test_cached_answer_is_used_even_when_disabled(storage):
    key = cache_key("s", "abc", "task", "v1", "m")
    storage.write_bytes(key, json.dumps({"answer": {"items": [1]}}).encode())
    g = Gemini(project=None, model="m", enabled=False)
    record = g.extract_json(storage, source_id="s", sha256="abc", task="task", prompt_version="v1",
                            parts=[Blob("x")], prompt="p")
    assert record["answer"] == {"items": [1]} and g.stats.cache_hits == 1


def test_disabled_or_over_budget_makes_no_call(storage, monkeypatch):
    g = Gemini(project="p", model="m", enabled=False)
    assert g.extract_json(storage, source_id="s", sha256="x", task="t", prompt_version="v1", parts=[], prompt="p") is None
    g = Gemini(project="p", model="m", enabled=True, max_calls=0)
    monkeypatch.setattr(g, "_generate", lambda parts, prompt: (_ for _ in ()).throw(AssertionError("called")))
    assert g.extract_json(storage, source_id="s", sha256="x", task="t", prompt_version="v1", parts=[], prompt="p") is None
    assert g.stats.skipped == 1


def test_a_call_is_cached(storage, monkeypatch):
    g = Gemini(project="p", model="m", enabled=True)
    monkeypatch.setattr(g, "_generate", lambda parts, prompt: ('{"items": []}', {"input_tokens": 5, "output_tokens": 2}))
    first = g.extract_json(storage, source_id="s", sha256="y", task="t", prompt_version="v1", parts=[], prompt="p")
    again = g.extract_json(storage, source_id="s", sha256="y", task="t", prompt_version="v1", parts=[], prompt="p")
    assert first["answer"] == again["answer"] == {"items": []}
    assert g.stats.calls == 1 and g.stats.cache_hits == 1 and g.stats.input_tokens == 5
