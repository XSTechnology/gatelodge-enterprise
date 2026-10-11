"""Stream spend-accounting tests (v2.0 fix) — streamed requests used to
log usage {} and NEVER charge any spend scope (key/session/team/backend
all blind on streams). Now: real usage from the wire when present
(openai stream_options.include_usage / SSE chunks carrying usage), a
chars//4 estimate otherwise; the key scope charges and the request-log
line carries usage. Strong form: pre-seed the ledger to 0 first — the
budget-gate-queries-early failure mode.
"""
import json
import types

from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway.storage import reset_storage

from test_gateway_stage3 import (gguf, wired, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config, _with_fake_llama)


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _month_log():
    return gw_app._month_log_path(gw_app._month_key())


def _stream(client, token, model="groq:cloud-fast", **over):
    with client.stream("POST", "/v1/chat/completions",
                       headers=_auth(token),
                       json={"model": model, "stream": True,
                             "messages": [{"role": "user",
                                           "content": "stream charge probe"}],
                             **over}) as r:
        assert r.status_code == 200
        for _ in r.iter_lines():
            pass                      # consume to completion


def test_embedded_stream_charges_key_scope(wired, gguf):  # noqa: F811
    """FakeLlama streams carry no usage → the chars//4 ESTIMATE charges
    the key ledger (strong form: ledger pre-seeded to 0)."""
    wired(_secured_config(gguf, api_keys=[
        {"id": "sk", "key": "vk-stream"}]))
    try:
        gw_app._scope_spend("key:sk")          # pre-seed 0 — the exact
        # failure mode where a budget gate cached the scope first

        def fn(records):
            with TestClient(gw_app.app) as client:
                _stream(client, "vk-stream", model="local:fast")
        _with_fake_llama(fn, stream_script=["Hello", " big", " world"])
        spend = gw_app._scope_spend("key:sk")
        assert spend["tokens"] > 0, "stream must charge the key scope"
        lines = [json.loads(l) for l in
                 _month_log().read_text(encoding="utf-8").splitlines()]
        assert lines[-1]["stream"] is True
        assert lines[-1]["usage"]["total_tokens"] > 0   # no more usage {}
    finally:
        reset_storage()


def test_cloud_stream_real_usage_charges_exact(wired, gguf, monkeypatch):  # noqa: F811
    """An openai-type stream whose final chunk carries usage (the
    include_usage contract) charges the EXACT wire usage — and the
    gateway requested include_usage."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "sk", "key": "vk-stream"}]))
    usage = {"prompt_tokens": 11, "completion_tokens": 7,
             "total_tokens": 18}
    fake = _FakeCloud()

    async def _create(**kw):
        fake.calls.append(kw)

        async def _gen():
            yield types.SimpleNamespace(
                choices=[types.SimpleNamespace(
                    delta=types.SimpleNamespace(content="hi there"))],
                usage=None,
                model_dump_json=lambda: json.dumps(
                    {"choices": [{"delta": {"content": "hi there"}}]}))
            yield types.SimpleNamespace(
                choices=[],
                usage=usage,
                model_dump_json=lambda: json.dumps(
                    {"choices": [], "usage": usage}))
        return _gen()

    fake.chat.completions.create = _create      # type: ignore[method-assign]
    _patch_cloud(monkeypatch, fake)
    try:
        gw_app._scope_spend("key:sk")          # pre-seed 0
        with TestClient(gw_app.app) as client:
            _stream(client, "vk-stream")
        assert fake.calls and fake.calls[-1].get("stream_options") == \
            {"include_usage": True}
        assert gw_app._scope_spend("key:sk")["tokens"] == 18   # EXACT
        lines = [json.loads(l) for l in
                 _month_log().read_text(encoding="utf-8").splitlines()]
        assert lines[-1]["usage"]["total_tokens"] == 18
    finally:
        reset_storage()


def test_stream_failure_does_not_charge(wired, gguf, monkeypatch):  # noqa: F811
    """A mid-flight failure produces an SSE error event, a 502 log line,
    and NO charge (mirrors the non-stream path: errors never charge)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "sk", "key": "vk-stream"}]))
    fake = _FakeCloud()

    async def _create(**kw):
        fake.calls.append(kw)

        async def _gen():
            raise RuntimeError("provider died mid-stream")
            yield                                      # pragma: no cover
        return _gen()

    fake.chat.completions.create = _create      # type: ignore[method-assign]
    _patch_cloud(monkeypatch, fake)
    try:
        gw_app._scope_spend("key:sk")
        with TestClient(gw_app.app) as client:
            with client.stream("POST", "/v1/chat/completions",
                               headers=_auth("vk-stream"),
                               json={"model": "groq:cloud-fast",
                                     "stream": True,
                                     "messages": [{"role": "user",
                                                   "content": "x"}]}) as r:
                for _ in r.iter_lines():
                    pass
        assert gw_app._scope_spend("key:sk")["tokens"] == 0
        lines = [json.loads(l) for l in
                 _month_log().read_text(encoding="utf-8").splitlines()]
        assert lines[-1]["status"] == 502
    finally:
        reset_storage()
