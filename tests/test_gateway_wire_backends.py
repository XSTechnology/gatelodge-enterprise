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
        async def _post(url, headers, payload, timeout, provider,
                        auth=None):
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
            # licensed wire backends LIST their models (v1/models)
            r2 = client.get("/v1/models",
                            headers={"Authorization":
                                     "Bearer admin-secret"})
            ids = [m["id"] for m in r2.json()["data"]]
            assert "sonnet" in ids
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


# ── responses (the OpenAI forward-path API; Bedrock mantle speaks it) ─────

def _responses_config(gguf):
    cfgd = _secured_config(gguf)
    cfgd["backends"]["oairect"] = {
        "type": "responses",
        "api_key_env": "OPENAI_TEST_KEY",
        "models": {"gpt": "gpt-6-astra"}}
    return cfgd


async def _fake_responses(url, headers, payload, timeout, provider):
    assert payload["model"] == "gpt-6-astra"
    assert payload["store"] is False
    assert payload.get("instructions") in (None, "Be terse.")
    assert headers["Authorization"] == "Bearer test-key"
    assert url.endswith("/responses")
    # tool flow when tools are offered; text otherwise
    if payload.get("tools"):
        return {"id": "resp_1", "status": "completed", "output": [
            {"type": "message", "role": "assistant",
             "content": [{"type": "output_text", "text": "Checking."}]},
            {"type": "function_call", "call_id": "fc_1",
             "name": payload["tools"][0]["name"],
             "arguments": "{\"city\": \"Boston\"}"}],
            "usage": {"input_tokens": 9, "output_tokens": 4,
                      "total_tokens": 13}}
    return {"id": "resp_2", "status": "completed", "output": [
        {"type": "reasoning", "summary": []},      # skipped
        {"type": "message", "role": "assistant",
         "content": [{"type": "output_text", "text": "RESP TRANSLATED"}]}],
        "usage": {"input_tokens": 5, "output_tokens": 3,
                  "total_tokens": 8}}


def test_responses_translation_units():
    instructions, items = wire._oai_messages_to_responses([
        {"role": "system", "content": "Be terse."},
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello!",
         "tool_calls": [{"id": "c1", "type": "function",
                         "function": {"name": "get_w",
                                      "arguments": '{"c": "B"}'}}]},
        {"role": "tool", "tool_call_id": "c1", "content": "52F"}])
    assert instructions == "Be terse."
    assert items[0] == {"type": "message", "role": "user",
                        "content": "Hi"}
    fcs = [i for i in items if i["type"] == "function_call"]
    assert fcs[0]["call_id"] == "c1" and fcs[0]["name"] == "get_w"
    outs = [i for i in items if i["type"] == "function_call_output"]
    assert outs[0]["call_id"] == "c1" and outs[0]["output"] == "52F"
    tools = wire._oai_tools_to_responses([
        {"type": "function", "function": {
            "name": "get_w", "description": "w",
            "parameters": {"type": "object"}}}])
    assert tools == [{"type": "function", "name": "get_w",
                      "description": "w",
                      "parameters": {"type": "object"}}]


def test_responses_backend_serves(wired, gguf, _mock_upstream,
                                   monkeypatch):
    wired(_responses_config(gguf))
    monkeypatch.setenv("OPENAI_TEST_KEY", "test-key")
    _entitle(monkeypatch, "oairect")
    _mock_upstream(_fake_responses)
    try:
        with TestClient(gw_app.app) as client:
            # plain text (with a reasoning Item in the output — skipped)
            r = client.post("/v1/chat/completions",
                            headers={"Authorization":
                                     "Bearer admin-secret"},
                            json={"model": "oairect:gpt",
                                  "max_tokens": 64,
                                  "messages": [
                                      {"role": "system",
                                       "content": "Be terse."},
                                      {"role": "user", "content": "Hi"}]})
            assert r.status_code == 200, r.text
            d = r.json()
            assert d["choices"][0]["message"]["content"] \
                == "RESP TRANSLATED"
            assert d["usage"]["total_tokens"] == 8
            # tool call round-trip
            r2 = client.post("/v1/chat/completions",
                             headers={"Authorization":
                                      "Bearer admin-secret"},
                             json={"model": "oairect:gpt",
                                   "max_tokens": 64,
                                   "messages": [{"role": "user",
                                                 "content": "weather?"}],
                                   "tools": [{
                                       "type": "function",
                                       "function": {
                                           "name": "get_weather",
                                           "description": "w",
                                           "parameters": {
                                               "type": "object"}}}]})
            assert r2.status_code == 200, r2.text
            m = r2.json()["choices"][0]["message"]
            assert r2.json()["choices"][0]["finish_reason"] == "tool_calls"
            assert m["tool_calls"][0]["function"]["name"] == "get_weather"
            assert json.loads(
                m["tool_calls"][0]["function"]["arguments"]) \
                == {"city": "Boston"}
            # model lists
            r3 = client.get("/v1/models",
                            headers={"Authorization":
                                     "Bearer admin-secret"})
            assert "gpt" in [m["id"] for m in r3.json()["data"]]
    finally:
        reset_storage()
        licensing.reset_licensing()


def test_bedrock_mantle_sigv4_service_override():
    """The Bedrock-mantle spike (v2.3): the OpenAI-compatible endpoint
    signs SigV4 with service 'bedrock-mantle' — the backend config's
    `service` field reaches the signer."""
    import gateway.cloud as cloud_mod
    from gateway.credchains import SigV4Auth
    backend = {"base_url":
               "https://bedrock-mantle.us-east-1.api.aws/v1",
               "auth": "sigv4", "region": "us-east-1",
               "service": "bedrock-mantle"}
    client = cloud_mod.get_cloud_client("bedrock-test", backend)
    # the httpx auth carries the configured service
    auth = client._client.auth
    assert isinstance(auth, SigV4Auth)
    assert auth.service == "bedrock-mantle"
    assert auth.region == "us-east-1"
    cloud_mod.reset_clients()


# ── azure (classic deployment-path API) + vertex (GCP Interactions) ──────

def test_azure_client_construction(monkeypatch):
    """Azure per-model client: deployment in the URL path, the resource
    key on the api-key HEADER (Azure ignores the SDK Bearer), the
    required api-version query param, per-deployment caching."""
    import gateway.cloud as cloud_mod
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "az-key-123")
    backend = {"resource": "myres", "api_key_env": "AZURE_OPENAI_API_KEY"}
    client = cloud_mod.get_azure_client("az-t1", backend, "gpt4o-deploy")
    base = str(client.base_url).rstrip("/")
    assert base == ("https://myres.openai.azure.com/openai/deployments/"
                    "gpt4o-deploy")
    assert client.api_key == "azure"          # the header carries the real key
    # default_headers/query land on the SDK's _custom_* maps (merged
    # into each request) — not the raw httpx client headers
    assert client._custom_headers.get("api-key") == "az-key-123"
    assert dict(client._custom_query or {}).get("api-version") \
        == cloud_mod.AZURE_API_VERSION_DEFAULT
    # per-deployment cache: a second deployment gets its own client
    c2 = cloud_mod.get_azure_client("az-t2", backend, "other-deploy")
    assert str(c2.base_url).rstrip("/").endswith("other-deploy")
    cloud_mod.reset_clients()


def test_azure_backend_serves(wired, gguf, monkeypatch):
    """End-to-end: an azure backend routes chat through the shared
    openai execution path with the per-model deployment client."""
    cfgd = _secured_config(gguf)
    cfgd["backends"]["az"] = {
        "type": "azure", "resource": "myres",
        "api_key_env": "AZURE_OPENAI_API_KEY",
        "models": {"gpt4o": "gpt4o-deploy"}}
    wired(cfgd)
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "az-key-123")
    _entitle(monkeypatch, "az")

    # mock the openai SDK client the azure branch constructs
    class _FakeCompletions:
        async def create(self, **kwargs):
            assert kwargs["model"] == "gpt4o-deploy"
            return {"id": "chatcmpl-az", "object": "chat.completion",
                    "model": kwargs["model"],
                    "choices": [{"index": 0,
                                 "message": {"role": "assistant",
                                             "content": "AZURE OK"},
                                 "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}
    import gateway.cloud as cloud_mod
    class _FakeClient:
        chat = type("C", (), {"completions": _FakeCompletions()})()
    monkeypatch.setattr(cloud_mod, "get_azure_client",
                        lambda n, b, d, timeout=120: _FakeClient())
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers={"Authorization":
                                     "Bearer admin-secret"},
                            json={"model": "az:gpt4o", "max_tokens": 32,
                                  "messages": [{"role": "user",
                                                "content": "hi"}]})
            assert r.status_code == 200, r.text
            assert r.json()["choices"][0]["message"]["content"] == "AZURE OK"
            assert r.json()["usage"]["total_tokens"] == 5
            # models listed (deployment-mapped)
            r2 = client.get("/v1/models",
                            headers={"Authorization":
                                     "Bearer admin-secret"})
            assert "gpt4o" in [m["id"] for m in r2.json()["data"]]
    finally:
        reset_storage()
        licensing.reset_licensing()
        cloud_mod.reset_clients()


def _vertex_config(gguf):
    cfgd = _secured_config(gguf)
    cfgd["backends"]["vg"] = {
        "type": "vertex", "project": "acme-proj",
        "api_key_env": "VERTEX_EXPRESS_KEY",
        "models": {"flash": "gemini-3.8-flash"}}
    return cfgd


async def _fake_vertex(url, headers, payload, timeout, provider):
    assert url == ("https://aiplatform.googleapis.com/v1beta1/projects/"
                   "acme-proj/locations/global/interactions")
    assert payload["model"] == "gemini-3.8-flash"
    assert payload["store"] is False
    assert headers["x-goog-api-key"] == "vx-key"   # express key path
    return {"id": "int_v", "steps": [
        {"type": "model_output",
         "content": [{"type": "text", "text": "VERTEX OK"}]}],
        "usage": {"prompt_tokens": 6, "completion_tokens": 2,
                  "total_tokens": 8}}


def test_vertex_backend_serves(wired, gguf, _mock_upstream, monkeypatch):
    """Vertex Interactions: same payload as the gemini type, GCP URL +
    project, express-key (x-goog-api-key) or ADC Bearer auth."""
    wired(_vertex_config(gguf))
    monkeypatch.setenv("VERTEX_EXPRESS_KEY", "vx-key")
    _entitle(monkeypatch, "vg")
    _mock_upstream(_fake_vertex)
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers={"Authorization":
                                     "Bearer admin-secret"},
                            json={"model": "vg:flash", "max_tokens": 32,
                                  "messages": [{"role": "user",
                                                "content": "hi"}]})
            assert r.status_code == 200, r.text
            assert r.json()["choices"][0]["message"]["content"] \
                == "VERTEX OK"
            assert r.json()["usage"]["total_tokens"] == 8
    finally:
        reset_storage()
        licensing.reset_licensing()


# ── bedrock Converse (native, v2.3) ────────────────────────────────────────

def test_bedrock_translation():
    """OpenAI → Converse → OpenAI: system list, toolUse/toolResult
    blocks, toolSpec.inputSchema.json, stopReason mapping, usage."""
    system, msgs = wire._oai_messages_to_bedrock([
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
    assert msgs[1]["content"][0] == {"text": "Hello!"}
    assert msgs[1]["content"][1] == {"toolUse": {
        "toolUseId": "t1", "name": "get_weather",
        "input": {"city": "Boston"}}}
    assert msgs[2]["content"][0]["toolResult"]["toolUseId"] == "t1"
    tools = wire._oai_tools_to_bedrock([
        {"type": "function", "function": {
            "name": "get_weather", "description": "w",
            "parameters": {"type": "object"}}}])
    assert tools[0]["toolSpec"]["name"] == "get_weather"
    assert tools[0]["toolSpec"]["inputSchema"]["json"] == {"type": "object"}
    out = wire._bedrock_to_oai({
        "output": {"message": {"role": "assistant", "content": [
            {"text": "Hello "},
            {"toolUse": {"toolUseId": "tu1", "name": "get_weather",
                         "input": {"city": "Boston"}}}]}},
        "stopReason": "tool_use",
        "usage": {"inputTokens": 4, "outputTokens": 2,
                  "totalTokens": 6}}, "m")
    assert out["choices"][0]["finish_reason"] == "tool_calls"
    tc = out["choices"][0]["message"]["tool_calls"][0]
    assert tc["id"] == "tu1"
    assert tc["function"]["arguments"] == '{"city": "Boston"}'
    assert out["usage"] == {"prompt_tokens": 4, "completion_tokens": 2,
                            "total_tokens": 6}


def _bedrock_config(gguf):
    cfgd = _secured_config(gguf)
    cfgd["backends"]["bdr"] = {
        "type": "bedrock", "region": "eu-west-1",
        "api_key_env": "BEDROCK_TEST_KEY",
        "models": {"sonnet": "anthropic.claude-sonnet-4-5"}}
    return cfgd


async def _fake_bedrock(url, headers, payload, timeout, provider):
    assert url == ("https://bedrock-runtime.eu-west-1.amazonaws.com/"
                   "model/anthropic.claude-sonnet-4-5/converse")
    assert headers["x-api-key"] == "bd-key"
    assert payload["system"] == [{"text": "Be terse."}]
    assert payload["inferenceConfig"]["maxTokens"] == 32
    spec = payload["toolConfig"]["tools"][0]["toolSpec"]
    assert spec["name"] == "get_weather"
    assert spec["inputSchema"]["json"] == {"type": "object"}
    return {"output": {"message": {"role": "assistant", "content": [
        {"text": "BDR OK"},
        {"toolUse": {"toolUseId": "tu9", "name": "get_weather",
                     "input": {"city": "Boston"}}}]}},
        "stopReason": "tool_use",
        "usage": {"inputTokens": 4, "outputTokens": 2, "totalTokens": 6}}


def test_bedrock_backend_serves(wired, gguf, _mock_upstream,
                                monkeypatch):
    """Native bedrock Converse: regional bedrock-runtime URL, modelId
    in the path, Converse payload (system list, inferenceConfig,
    toolConfig), x-api-key when a key is configured."""
    wired(_bedrock_config(gguf))
    monkeypatch.setenv("BEDROCK_TEST_KEY", "bd-key")
    _entitle(monkeypatch, "bdr")
    _mock_upstream(_fake_bedrock)
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers={"Authorization":
                                     "Bearer admin-secret"},
                            json={"model": "bdr:sonnet", "max_tokens": 32,
                                  "messages": [
                                      {"role": "system",
                                       "content": "Be terse."},
                                      {"role": "user", "content": "hi"}],
                                  "tools": [{"type": "function",
                                             "function": {
                                                 "name": "get_weather",
                                                 "description": "w",
                                                 "parameters": {
                                                     "type": "object"}}}]})
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["choices"][0]["message"]["content"] == "BDR OK"
            assert body["choices"][0]["finish_reason"] == "tool_calls"
            assert body["usage"]["total_tokens"] == 6
            # models listing includes the bedrock model
            r2 = client.get("/v1/models",
                            headers={"Authorization":
                                     "Bearer admin-secret"})
            assert "sonnet" in [m["id"] for m in r2.json()["data"]]
    finally:
        reset_storage()
        licensing.reset_licensing()


def test_bedrock_auth_selection(monkeypatch):
    """Key path → x-api-key header, no signer; no key → the SigV4
    per-request signer (service 'bedrock', env-var chain)."""
    import asyncio

    import gateway.credchains as cc
    captured: dict = {}

    async def fake_post(url, headers, payload, timeout, provider,
                        auth=None):
        captured.clear()
        captured.update(url=url, headers=dict(headers), auth=auth)
        return {"output": {"message": {"role": "assistant",
                                       "content": [{"text": "ok"}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": 1, "outputTokens": 1,
                          "totalTokens": 2}}

    monkeypatch.setattr(wire, "_post_json", fake_post)
    msgs = [{"role": "user", "content": "hi"}]

    # 1) configured key → x-api-key, no signer
    monkeypatch.setattr(wire.cloud, "resolve_backend_key",
                        lambda b: "k-1")
    asyncio.run(wire.bedrock_converse(
        {"region": "us-east-1", "api_key_env": "K"}, "m1",
        msgs, None, None, None, 5.0))
    assert captured["headers"].get("x-api-key") == "k-1"
    assert captured["auth"] is None
    assert captured["url"] == ("https://bedrock-runtime.us-east-1."
                               "amazonaws.com/model/m1/converse")

    # 2) no key → SigV4 (credentials present in the env chain)
    monkeypatch.setattr(wire.cloud, "resolve_backend_key",
                        lambda b: "")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "shhh")
    asyncio.run(wire.bedrock_converse(
        {"region": "us-east-1"}, "m1", msgs, None, None, None, 5.0))
    assert isinstance(captured["auth"], cc.SigV4Auth)
    assert captured["auth"].service == "bedrock"
    assert "x-api-key" not in captured["headers"]
