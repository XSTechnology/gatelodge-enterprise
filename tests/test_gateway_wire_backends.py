"""Wire-translator backend canaries (v2.3) — anthropic + gemini
upstream backend types:

    translation: OpenAI chat → provider wire format → OpenAI chat
    (system extraction, tool_use/tool_result blocks, input_schema /
    function tools, stop_reason mapping, usage both directions,
    gemini's steps timeline);

    integration: a configured anthropic/gemini backend serves
    /v1/chat/completions end-to-end (request routed, provider
    response translated, budgets/charging/logging ride the standard
    path) with the upstream HTTP mocked at gateway.wire._post_json;

    streaming: stream=true completes fully then emits ONE
    OpenAI-shaped chunk + [DONE];

    licensing: the provider gate covers the new types (unlicensed
    anthropic backend = don't-display, byte-identical unknown-model).
"""
import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
from gateway import licensing, wire
from gateway.storage import reset_storage

from test_gateway_stage3 import _secured_config, gguf, wired  # noqa: F401

# ── translation unit tests ─────────────────────────────────────────────────

def test_anthropic_request_translation():
    system, msgs = wire._oai_messages_to_anthropic([
        {"role": "system", "content": "Be terse."},
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello!",
         "tool_calls": [{"id": "t1", "type": "function",
                         "function": {"name": "get_weather",
                                      "arguments": '{"city": "Boston"}'}}]},
        {"role": "tool", "tool_call_id": "t1", "name": "get_weather",
         "content": "52F"},
        {"role": "user", "content": "Thanks"}])
    assert system == "Be terse."
    assert [m["role"] for m in msgs] == ["user", "assistant", "user",
                                         "user"]
    assert msgs[1]["content"][1]["type"] == "tool_use"
    assert msgs[1]["content"][1]["input"] == {"city": "Boston"}
    assert msgs[2]["content"][0]["type"] == "tool_result"
    assert msgs[2]["content"][0]["tool_use_id"] == "t1"
    tools = wire._oai_tools_to_anthropic([
        {"type": "function", "function": {
            "name": "get_weather", "description": "w",
            "parameters": {"type": "object"}}}])
    assert tools[0]["input_schema"] == {"type": "object"}


def test_anthropic_response_translation():
    d = wire._anthropic_to_oai({
        "id": "msg_1", "stop_reason": "tool_use",
        "content": [{"type": "text", "text": "Let me check."},
                    {"type": "tool_use", "id": "t1", "name": "get_w",
                     "input": {"city": "Boston"}}],
        "usage": {"input_tokens": 10, "output_tokens": 5}}, "claude-x")
    msg = d["choices"][0]["message"]
    assert d["choices"][0]["finish_reason"] == "tool_calls"
    assert msg["content"] == "Let me check."
    assert msg["tool_calls"][0]["function"]["name"] == "get_w"
    assert json.loads(msg["tool_calls"][0]["function"]["arguments"]) \
        == {"city": "Boston"}
    assert d["usage"] == {"prompt_tokens": 10, "completion_tokens": 5,
                          "total_tokens": 15}
    # end_turn → stop
    d2 = wire._anthropic_to_oai({"content": [{"type": "text", "text": "ok"}],
                                 "stop_reason": "end_turn",
                                 "usage": {}}, "claude-x")
    assert d2["choices"][0]["finish_reason"] == "stop"


def test_gemini_request_translation():
    system, turns = wire._oai_messages_to_gemini([
        {"role": "system", "content": "Be terse."},
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello!",
         "tool_calls": [{"id": "c1", "type": "function",
                         "function": {"name": "get_w",
                                      "arguments": '{"c": "B"}'}}]},
        {"role": "tool", "tool_call_id": "c1", "name": "get_w",
         "content": "52F"}])
    assert system == "Be terse."
    assert turns[0] == {"role": "user",
                        "content": [{"type": "text", "text": "Hi"}]}
    assert turns[1]["role"] == "model"
    fcs = [b for b in turns[1]["content"]
           if b.get("type") == "function_call"]
    assert fcs and fcs[0]["name"] == "get_w"
    assert turns[2]["type"] == "function_result"
    assert turns[2]["call_id"] == "c1"


def test_gemini_response_translation():
    d = wire._gemini_to_oai({
        "id": "int_1",
        "steps": [
            {"type": "user_input", "content": [{"type": "text",
                                                "text": "q"}]},
            {"type": "model_output",
             "content": [{"type": "text", "text": "It is 52F"}]},
            {"type": "function_call", "call_id": "fc_1",
             "name": "get_w", "arguments": {"c": "B"}}],
        "usage": {"prompt_tokens": 4, "completion_tokens": 2,
                  "total_tokens": 6}}, "gemini-x")
    msg = d["choices"][0]["message"]
    assert d["choices"][0]["finish_reason"] == "tool_calls"
    assert msg["content"] == "It is 52F"
    assert msg["tool_calls"][0]["id"] == "fc_1"
    assert d["usage"]["total_tokens"] == 6
    # usageMetadata (legacy shape) maps too
    d2 = wire._gemini_to_oai({
        "steps": [{"type": "model_output",
                   "content": [{"type": "text", "text": "ok"}]}],
        "usageMetadata": {"promptTokenCount": 3,
                          "candidatesTokenCount": 2,
                          "totalTokenCount": 5}}, "gemini-x")
    assert d2["usage"]["total_tokens"] == 5
    assert d2["choices"][0]["finish_reason"] == "stop"


# ── integration (mocked upstream) ─────────────────────────────────────────

def _wire_config(gguf, backend_type, api_key_env):
    cfgd = _secured_config(gguf)
    cfgd["backends"]["claude"] = {
        "type": backend_type,
        "api_key_env": api_key_env,
        "models": {"sonnet": "claude-sonnet-4-5"
                   if backend_type == "anthropic" else "gemini-3.8-flash"}}
    return cfgd


async def _fake_anthropic(url, headers, payload, timeout, provider):
    assert payload["model"] == "claude-sonnet-4-5"
    assert payload["max_tokens"] == 64
    assert payload.get("system") in (None, "Be terse.")
    assert headers["x-api-key"] == "test-key"
    return {"id": "msg_9", "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "TRANSLATED"}],
            "usage": {"input_tokens": 7, "output_tokens": 3}}


async def _fake_gemini(url, headers, payload, timeout, provider):
    assert payload["model"] == "gemini-3.8-flash"
    assert payload["store"] is False
    assert headers["x-goog-api-key"] == "test-key"
    return {"id": "int_9", "steps": [
        {"type": "model_output",
         "content": [{"type": "text", "text": "GEM TRANSLATED"}]}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 2,
                  "total_tokens": 7}}


@pytest.fixture()
def _mock_upstream(monkeypatch):
    def install(fn):
        async def _post(url, headers, payload, timeout, provider):
            return await fn(url, headers, payload, timeout, provider)
        monkeypatch.setattr(wire, "_post_json", _post)
    return install


def _entitle(monkeypatch, backend_name):
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"providers": [backend_name]}, "source": "file",
        "detail": "", "trial": None})


def test_anthropic_backend_serves(wired, gguf, _mock_upstream,
                                   monkeypatch):
    wired(_wire_config(gguf, "anthropic", "ANTHROPIC_TEST_KEY"))
    monkeypatch.setenv("ANTHROPIC_TEST_KEY", "test-key")
    _entitle(monkeypatch, "claude")
    _mock_upstream(_fake_anthropic)
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers={"Authorization":
                                     "Bearer admin-secret"},
                            json={"model": "claude:sonnet",
                                  "max_tokens": 64,
                                  "messages": [
                                      {"role": "system",
                                       "content": "Be terse."},
                                      {"role": "user",
                                       "content": "Hi"}]})
            assert r.status_code == 200, r.text
            d = r.json()
            assert d["choices"][0]["message"]["content"] == "TRANSLATED"
            assert d["usage"]["total_tokens"] == 10
            # the request log/charging rode the standard path
            assert gw_app._scope_spend("backend:claude")["tokens"] == 10
    finally:
        reset_storage()
        licensing.reset_licensing()


def test_gemini_backend_serves(wired, gguf, _mock_upstream, monkeypatch):
    wired(_wire_config(gguf, "gemini", "GEMINI_TEST_KEY"))
    monkeypatch.setenv("GEMINI_TEST_KEY", "test-key")
    _entitle(monkeypatch, "claude")
    _mock_upstream(_fake_gemini)
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers={"Authorization":
                                     "Bearer admin-secret"},
                            json={"model": "claude:sonnet",
                                  "max_tokens": 64,
                                  "messages": [
                                      {"role": "user",
                                       "content": "Hi"}]})
            assert r.status_code == 200, r.text
            assert r.json()["choices"][0]["message"]["content"] \
                == "GEM TRANSLATED"
            assert r.json()["usage"]["total_tokens"] == 7
    finally:
        reset_storage()
        licensing.reset_licensing()


def test_wire_backend_streams_single_chunk(wired, gguf, _mock_upstream,
                                           monkeypatch):
    """stream=true on a translator backend: full completion, ONE
    OpenAI-shaped chunk + [DONE] — valid streaming shape."""
    wired(_wire_config(gguf, "anthropic", "ANTHROPIC_TEST_KEY"))
    monkeypatch.setenv("ANTHROPIC_TEST_KEY", "test-key")
    _entitle(monkeypatch, "claude")
    _mock_upstream(_fake_anthropic)
    try:
        with TestClient(gw_app.app) as client:
            with client.stream(
                    "POST", "/v1/chat/completions",
                    headers={"Authorization": "Bearer admin-secret"},
                    json={"model": "claude:sonnet", "max_tokens": 64,
                          "stream": True,
                          "messages": [{"role": "user",
                                        "content": "Hi"}]}) as r:
                assert r.status_code == 200
                body = "\n".join(r.iter_lines())
            events = [json.loads(l[6:]) for l in body.splitlines()
                      if l.startswith("data: {")]
            done = [l for l in body.splitlines() if l == "data: [DONE]"]
            assert done, "stream must terminate with [DONE]"
            final = events[-1]
            assert final["choices"][0]["message"]["content"] \
                == "TRANSLATED"
            assert final["usage"]["total_tokens"] == 10
    finally:
        reset_storage()
        licensing.reset_licensing()


def test_wire_backend_licensing_dont_display(wired, gguf, monkeypatch):
    """The provider gate covers the new types: an unlicensed
    anthropic backend is invisible + byte-identical unknown-model."""
    wired(_wire_config(gguf, "anthropic", "ANTHROPIC_TEST_KEY"))
    monkeypatch.setenv("ANTHROPIC_TEST_KEY", "test-key")
    _entitle(monkeypatch, "someother")   # claude NOT entitled
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/v1/models",
                           headers={"Authorization":
                                    "Bearer admin-secret"})
            ids = [m["id"] for m in r.json()["data"]]
            assert "sonnet" not in ids
            r2 = client.post("/v1/chat/completions",
                             headers={"Authorization":
                                      "Bearer admin-secret"},
                             json={"model": "claude:sonnet",
                                   "max_tokens": 8,
                                   "messages": [{"role": "user",
                                                 "content": "x"}]})
            assert r2.status_code == 400
            err = r2.json()["error"]
            assert err["code"] == "model_not_found"
            assert "licens" not in err["message"].lower()
    finally:
        licensing.reset_licensing()
