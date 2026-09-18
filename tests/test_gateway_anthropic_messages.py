"""LLM Gateway — Anthropic Messages API (/v1/messages) shim.

Workshop (and other Anthropic-style clients) probe /v1/messages.
The shim translates Anthropic requests to the gateway's OpenAI surface.
"""
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    def _wire(config: dict):
        cfgfile = tmp_path / "gateway_config.json"
        cfgfile.write_text(json.dumps(config), encoding="utf-8")
        cfg = gw_config.GatewayConfig(cfgfile)
        monkeypatch.setattr(gw_config, "_CONFIG", cfg)
        engine = gw_models.EmbeddedEngine(cfg)
        monkeypatch.setattr(gw_models, "_ENGINE", engine)
        monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
        monkeypatch.setattr(lsrv, "PROJECT_ROOT", tmp_path)
        lsrv.reset_managers()
        rp.reset_managers()
        return cfgfile, cfg, engine

    yield _wire
    lsrv.reset_managers()
    rp.reset_managers()


def _cfg(tmp_path):
    return {
        "port": 9911, "bind": "127.0.0.1",
        "models_dir": str(tmp_path),
        "backends": {"local": {"type": "embedded",
                               "models": {"m": {"gguf": "x.gguf"}}}},
    }


def _fake_chat(text="hello", out_tok=7, in_tok=3):
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content=text))]
    resp.usage.prompt_tokens = in_tok
    resp.usage.completion_tokens = out_tok
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(return_value=resp)
    return client


# ── validation ─────────────────────────────────────────────────────────────

def test_messages_missing_model(wired, tmp_path):
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={"messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400
    assert r.json()["type"] == "error"
    assert "model" in r.json()["error"]["message"]


def test_messages_missing_messages(wired, tmp_path):
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={"model": "m"})
    assert r.status_code == 400
    assert "messages" in r.json()["error"]["message"]


def test_messages_invalid_json(wired, tmp_path):
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", content=b"not json",
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 400


# ── translation + non-streaming ────────────────────────────────────────────

def test_messages_non_streaming(wired, tmp_path, monkeypatch):
    client = _fake_chat("world", out_tok=5, in_tok=2)
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    d = r.json()
    assert d["type"] == "message"
    assert d["role"] == "assistant"
    assert d["content"][0]["type"] == "text"
    assert d["content"][0]["text"] == "world"
    assert d["usage"] == {"input_tokens": 2, "output_tokens": 5}
    assert d["stop_reason"] == "end_turn"
    assert d["id"].startswith("msg_")


def test_messages_system_and_blocks_translated(wired, tmp_path, monkeypatch):
    client = _fake_chat("ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m",
            "system": [{"type": "text", "text": "be brief"}],
            "messages": [
                {"role": "user",
                 "content": [{"type": "text", "text": "part one"},
                             {"type": "text", "text": "part two"}]},
                {"role": "assistant", "content": "noted"},
                {"role": "user", "content": "plain string"},
            ],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    msgs = sent["messages"]
    assert msgs[0] == {"role": "system", "content": "be brief"}
    assert msgs[1]["content"] == "part one\npart two"
    assert msgs[2] == {"role": "assistant", "content": "noted"}
    assert msgs[3] == {"role": "user", "content": "plain string"}


def test_messages_upstream_error(wired, tmp_path, monkeypatch):
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert r.json()["error"]["type"] == "api_error"
    assert "boom" in r.json()["error"]["message"]


# ── streaming ──────────────────────────────────────────────────────────────

def _fake_stream_client(chunks=("a", "b")):
    async def stream():
        for t in chunks:
            ch = MagicMock()
            ch.choices = [MagicMock(delta=MagicMock(content=t))]
            yield ch
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(return_value=stream())
    return client


def test_messages_streaming(wired, tmp_path, monkeypatch):
    client = _fake_stream_client(("a", "b"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    body = r.text
    for ev in ("message_start", "content_block_start", "content_block_delta",
               "content_block_stop", "message_delta", "message_stop"):
        assert f"event: {ev}" in body
    assert '"text_delta"' in body
    assert '"text": "a"' in body
    assert '"text": "b"' in body


def test_messages_streaming_upstream_error(wired, tmp_path, monkeypatch):
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(side_effect=RuntimeError("nope"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200  # SSE stream already started
    assert "event: error" in r.text
    assert "nope" in r.text


def test_messages_temperature_passthrough(wired, tmp_path, monkeypatch):
    client = _fake_chat("ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "temperature": 0.2,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    assert sent["temperature"] == 0.2


def test_messages_unknown_model_falls_back(wired, tmp_path, monkeypatch):
    """Workshop sends claude-* style names — shim must map to a known model."""
    client = _fake_chat("ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "claude-sonnet-4-5",
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    # the configured model "m" is the only known one in this fixture
    assert sent["model"] == "m"
    # response reports the model it actually used
    assert r.json()["model"] == "m"


def test_messages_fallback_prefers_largest_ctx(wired, tmp_path, monkeypatch):
    """Regression: the fallback used to take loaded models in insertion
    order — a small-ctx instance loaded FIRST won for unknown model
    names, and Workshop's ~11.6k-token session-handoff prompt overflowed
    it. The largest-context loaded model must win instead."""
    client = _fake_chat("ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    _, cfg, engine = wired(_cfg(tmp_path))
    # insertion order mimics the live bug: raw-name 27B@4096 loaded
    # before pool@32768
    monkeypatch.setattr(engine, "loaded_models", lambda: [
        {"alias": "Qwen3.8-27B-Uncensored-Q4_K_M", "n_ctx": 4096},
        {"alias": "pool", "n_ctx": 32768},
    ])
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "claude-sonnet-4-5",
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    assert sent["model"] == "pool"
    assert r.json()["model"] == "pool"


# ── self-call hygiene (regression: unclosed streams leaked the inner
#    request's worker checkout — a vanished client wedged the model) ──────

def _closeable_client(chunks=("a", "b"), text="ok"):
    """Fake AsyncOpenAI whose stream AND client record their closes —
    for both the streaming and non-streaming request shapes."""
    closed = {"stream": 0, "client": 0}

    class FakeStream:
        def __init__(self):
            self._it = iter(chunks)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                t = next(self._it)
            except StopIteration:
                raise StopAsyncIteration from None
            ch = MagicMock()
            ch.choices = [MagicMock(delta=MagicMock(content=t))]
            return ch

        async def close(self):
            closed["stream"] += 1

    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content=text))]
    resp.usage.prompt_tokens = 3
    resp.usage.completion_tokens = 2

    async def _create(**kwargs):
        if kwargs.get("stream"):
            return FakeStream()
        return resp

    async def _close():
        closed["client"] += 1

    client = AsyncMock()
    client.chat.completions.create = _create
    client.close = _close
    return client, closed


def test_messages_stream_closes_selfcall(wired, tmp_path, monkeypatch):
    """The shim's self-call stream + client are closed when the response
    completes — an unclosed stream hides the client disconnect from the
    gateway and leaks the inner worker checkout."""
    client, closed = _closeable_client(("a", "b"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    assert "message_stop" in r.text
    assert closed["stream"] == 1  # self-call stream closed
    assert closed["client"] == 1  # client closed


def test_messages_stream_closes_selfcall_on_error(wired, tmp_path, monkeypatch):
    """Upstream failure mid-stream still closes the self-call."""
    async def _create(**kwargs):
        raise RuntimeError("nope")

    async def _close():
        pass

    client = AsyncMock()
    client.chat.completions.create = _create
    client.close = _close
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200  # SSE already started
    assert "event: error" in r.text


def test_messages_nonstream_closes_client(wired, tmp_path, monkeypatch):
    """Non-streaming self-call closes its client too (connection reuse
    hygiene — per-request clients otherwise leak sockets)."""
    client, closed = _closeable_client(text="ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    assert r.json()["content"][0]["text"] == "ok"
    assert closed["client"] == 1


# ── tool calling (regression: the shim used to DROP the tools field —
#    the model never saw tool definitions and answered "I don't have
#    access to external tools" in plain text) ─────────────────────────────

def _fake_chat_tool(name="get_weather", args='{"city": "Paris"}', text=None):
    resp = MagicMock()
    tc = MagicMock()
    tc.id = "call_1"
    tc.function = MagicMock()
    tc.function.name = name
    tc.function.arguments = args
    msg = MagicMock()
    msg.content = text
    msg.tool_calls = [tc]
    resp.choices = [MagicMock(message=msg)]
    resp.usage.prompt_tokens = 10
    resp.usage.completion_tokens = 5
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(return_value=resp)
    return client


def _tools_body(**extra):
    body = {
        "model": "m", "max_tokens": 64,
        "tools": [{
            "name": "get_weather",
            "description": "Get the current weather for a city",
            "input_schema": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        }],
        "messages": [{"role": "user",
                      "content": "What's the weather in Paris?"}],
    }
    body.update(extra)
    return body


def test_messages_tools_forwarded_and_tool_use_returned(wired, tmp_path, monkeypatch):
    """Anthropic tools → OpenAI tools in the self-call; OpenAI
    tool_calls → Anthropic tool_use blocks + stop_reason tool_use."""
    client = _fake_chat_tool()
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json=_tools_body())
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    assert sent["tools"] == [{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the current weather for a city",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    }]
    d = r.json()
    assert d["stop_reason"] == "tool_use"
    assert d["content"][0]["type"] == "tool_use"
    assert d["content"][0]["id"] == "call_1"
    assert d["content"][0]["name"] == "get_weather"
    assert d["content"][0]["input"] == {"city": "Paris"}


def test_messages_no_tools_field_means_no_tools_kwarg(wired, tmp_path, monkeypatch):
    """Plain requests (no tools) must not send tools=None — the engine's
    Home-LLM template guard retries on that shape."""
    client = _fake_chat("hi there")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    assert "tools" not in sent


def test_messages_tool_history_translated(wired, tmp_path, monkeypatch):
    """Assistant tool_use → OpenAI tool_calls; user tool_result →
    role="tool" messages, so agentic follow-up turns keep working."""
    client = _fake_chat("It's sunny and 18C in Paris.")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m",
            "tools": [{"name": "get_weather", "description": "w",
                       "input_schema": {"type": "object"}}],
            "messages": [
                {"role": "user", "content": "weather in Paris?"},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "let me check"},
                    {"type": "tool_use", "id": "call_1",
                     "name": "get_weather", "input": {"city": "Paris"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call_1",
                     "content": "sunny 18C"},
                    {"type": "text", "text": "thanks"},
                ]},
            ],
        })
    assert r.status_code == 200
    msgs = client.chat.completions.create.call_args.kwargs["messages"]
    assert msgs[0] == {"role": "user", "content": "weather in Paris?"}
    a = msgs[1]
    assert a["role"] == "assistant"
    assert a["content"] == "let me check"
    assert a["tool_calls"][0]["id"] == "call_1"
    assert a["tool_calls"][0]["function"]["name"] == "get_weather"
    assert json.loads(a["tool_calls"][0]["function"]["arguments"]) == {"city": "Paris"}
    assert msgs[2] == {"role": "tool", "tool_call_id": "call_1",
                       "content": "sunny 18C"}
    assert msgs[3] == {"role": "user", "content": "thanks"}


def _tool_stream_client():
    """Self-call stream shaped like the embedded backend with tools:
    raw JSON-convention text first, then the parsed tool_calls chunk."""
    tc = MagicMock()
    tc.index = 0
    tc.id = "call_9"
    tc.function = MagicMock()
    tc.function.name = "get_weather"
    tc.function.arguments = '{"city": "Paris"}'

    def _chunk(text=None, tool_calls=None):
        ch = MagicMock()
        d = MagicMock()
        d.content = text
        d.tool_calls = tool_calls
        ch.choices = [MagicMock(delta=d)]
        return ch

    chunks = [
        _chunk(text='{"tool": "get_weather", "arguments": {"city": "Paris"}}'),
        _chunk(tool_calls=[tc]),
    ]

    class FakeStream:
        def __init__(self):
            self._it = iter(chunks)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self._it)
            except StopIteration:
                raise StopAsyncIteration from None

        async def close(self):
            pass

    client = AsyncMock()
    recorded: dict = {}

    async def _create(**kwargs):
        recorded.update(kwargs)
        return FakeStream()

    client.chat.completions.create = _create
    client.close = AsyncMock()
    return client, recorded


def test_messages_streaming_tool_use(wired, tmp_path, monkeypatch):
    """Streaming: the JSON-convention text is suppressed and a proper
    tool_use block (content_block_start/input_json_delta/stop) is
    emitted with stop_reason tool_use."""
    client, recorded = _tool_stream_client()
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json=_tools_body(stream=True))
    assert r.status_code == 200
    # tools reached the self-call
    assert recorded["tools"][0]["function"]["name"] == "get_weather"
    # the raw JSON-convention text was suppressed
    assert '{"tool": "get_weather"' not in r.text
    # tool_use block events present, in order
    assert '"type": "tool_use"' in r.text
    assert '"name": "get_weather"' in r.text
    assert "input_json_delta" in r.text
    assert '\\"city\\": \\"Paris\\"' in r.text
    assert '"stop_reason": "tool_use"' in r.text
    assert "message_stop" in r.text


def test_messages_streaming_prose_not_suppressed(wired, tmp_path, monkeypatch):
    """Prose answers must keep streaming live — the hold-back only
    engages when the first content starts with '{'."""
    client, _closed = _closeable_client(("The weather", " is sunny."))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json=_tools_body(stream=True))
    assert r.status_code == 200
    assert "The weather" in r.text
    assert "is sunny" in r.text
    assert '"stop_reason": "end_turn"' in r.text
    assert "tool_use" not in r.text
