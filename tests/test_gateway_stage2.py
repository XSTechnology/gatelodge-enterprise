"""LLM Gateway Stage 2 — SSE streaming + /healthz.

SSE chunks must parse as OpenAI stream events (chat.completion.chunk,
role delta first, finish chunk, `data: [DONE]` terminator); the engine's
think-hold-back must never leak reasoning deltas (whole, and when the
`</think>` closer splits across chunks); non-thinking output flushes at
stream end; stream+tools on embedded buffers content then emits
delta.tool_calls at stream end; unknown models 400; /healthz reports
backends, loaded instances, GPUs with free VRAM, discovered count.
"""
import json
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402


# ── FakeLlama with streaming support ───────────────────────────────────────

def _with_fake_llama(fn, script=None, stream_script=None):
    """Run fn with a fake llama_cpp module. `stream_script` is the list of
    content pieces yielded as stream chunks (default ["Hello", " world"])."""
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.script = list(script or [])
            self.n_ctx = kwargs.get("n_ctx", 4096)

        def create_chat_completion(self, **kwargs):
            records.append({"call": kwargs})
            if kwargs.get("stream"):
                pieces = (list(stream_script) if stream_script is not None
                          else ["Hello", " world"])

                def _gen():
                    for p in pieces:
                        yield {"choices": [{"delta": {"content": p}}]}

                return _gen()
            if self.script:
                return self.script.pop(0)
            return {"choices": [{"message": {"content": "ok"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        fn(records)
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)
    return records


# ── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture()
def gguf(tmp_path):
    p = tmp_path / "fake.gguf"
    p.write_bytes(b"x")
    return p


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    def _wire(config: dict):
        cfgfile = tmp_path / "gateway_config.json"
        cfgfile.write_text(json.dumps(config), encoding="utf-8")
        cfg = gw_config.GatewayConfig(cfgfile)
        monkeypatch.setattr(gw_config, "_CONFIG", cfg)
        engine = gw_models.EmbeddedEngine(cfg)
        monkeypatch.setattr(gw_models, "_ENGINE", engine)
        # JSONL request log → tmp (never touch the real data/gateway_log.jsonl)
        monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
        return cfgfile, cfg, engine
    return _wire


def _local_config(gguf):
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {"local": {"type": "embedded", "models": {
            "fast": {"gguf": str(gguf), "ctx": 1024, "placement": ""}}}},
        "aliases": {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
    }


def _stream_lines(client, payload) -> tuple[int, list[str]]:
    with client.stream("POST", "/v1/chat/completions", json=payload) as r:
        status = r.status_code
        # trailing \n\n of the last SSE event yields one empty line — drop it
        lines = [ln for ln in r.iter_lines() if ln.strip()]
    return status, lines


def _parse_sse(lines: list[str]) -> tuple[list[dict], bool]:
    events: list[dict] = []
    done = False
    for ln in lines:
        ln = ln.strip()
        if not ln.startswith("data: "):
            continue
        payload = ln[len("data: "):]
        if payload == "[DONE]":
            done = True
            break
        events.append(json.loads(payload))
    return events, done


def _content(events: list[dict]) -> str:
    return "".join(e["choices"][0]["delta"].get("content") or "" for e in events)


# ── streaming ──────────────────────────────────────────────────────────────

def test_stream_basic_openai_format(wired, gguf):
    wired(_local_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            status, lines = _stream_lines(client, {
                "model": "fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]})
        assert status == 200
        assert lines[-1] == "data: [DONE]"
        events, done = _parse_sse(lines)
        assert done
        assert all(e["object"] == "chat.completion.chunk" for e in events)
        # role chunk first, then content, then finish
        assert events[0]["choices"][0]["delta"]["role"] == "assistant"
        assert _content(events) == "Hello world"
        assert events[-1]["choices"][0]["delta"] == {}
        assert events[-1]["choices"][0]["finish_reason"] == "stop"
        # one conversation: shared id + model echo
        assert len({e["id"] for e in events}) == 1
        assert events[0]["model"] == "fast"
        assert events[0]["id"].startswith("chatcmpl-gw-")
        # llama was driven in streaming mode
        call = [r_["call"] for r_ in records if "call" in r_][0]
        assert call["stream"] is True

    _with_fake_llama(fn, stream_script=["Hello", " world"])


def test_stream_think_never_leaks(wired, gguf):
    wired(_local_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            status, lines = _stream_lines(client, {
                "model": "fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]})
        assert status == 200
        events, done = _parse_sse(lines)
        assert done
        content = _content(events)
        assert content == "The answer"
        assert "secret" not in content  # reasoning held back
        assert "</think>" not in content

    _with_fake_llama(fn, stream_script=[
        "<think>secret ", "reasoning</think>The ", "answer"])


def test_stream_think_closer_split_across_chunks(wired, gguf):
    wired(_local_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            status, lines = _stream_lines(client, {
                "model": "fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]})
        assert status == 200
        events, done = _parse_sse(lines)
        assert done
        assert _content(events) == "Split works"

    _with_fake_llama(fn, stream_script=["muse</thi", "nk>Split ", "works"])


def test_stream_non_thinking_flushes_at_end(wired, gguf):
    """No </think> closer at all — non-thinking output must not be lost."""
    wired(_local_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            status, lines = _stream_lines(client, {
                "model": "fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]})
        assert status == 200
        events, done = _parse_sse(lines)
        assert done
        assert _content(events) == "Just answer"

    _with_fake_llama(fn, stream_script=["Just ", "answer"])


def test_stream_tools_embedded(wired, gguf):
    """Streaming with tools on embedded: content deltas streamed live,
    tool call parsed at stream end → delta.tool_calls chunk."""
    wired(_local_config(gguf))
    tools = [{"type": "function", "function": {
        "name": "f", "description": "d",
        "parameters": {"type": "object", "properties": {"x": {"type": "integer"}}}}}]

    def fn(records):
        with TestClient(gw_app.app) as client:
            status, lines = _stream_lines(client, {
                "model": "fast", "stream": True, "tools": tools,
                "messages": [{"role": "user", "content": "call f"}]})
        assert status == 200
        events, done = _parse_sse(lines)
        assert done
        # Content deltas streamed live (the JSON tool call text)
        content_events = [e for e in events if e["choices"][0]["delta"].get("content")]
        assert len(content_events) >= 1
        # Tool-call delta at end
        tc_events = [e for e in events if e["choices"][0]["delta"].get("tool_calls")]
        assert len(tc_events) == 1
        tc_delta = tc_events[0]["choices"][0]["delta"]["tool_calls"][0]
        assert tc_delta["index"] == 0
        assert tc_delta["type"] == "function"
        assert tc_delta["function"]["name"] == "f"
        # Finish reason = tool_calls (not stop)
        finish_events = [e for e in events
                         if e["choices"][0].get("finish_reason") == "tool_calls"]
        assert len(finish_events) == 1

    _with_fake_llama(fn, stream_script=['{"tool": "f", "arguments": {"x": 1}}'])


def test_stream_tools_no_call_just_text(wired, gguf):
    """Tools offered but model returns plain text → normal stop finish."""
    wired(_local_config(gguf))
    tools = [{"type": "function", "function": {
        "name": "f", "description": "d", "parameters": {"type": "object"}}}]

    def fn(records):
        with TestClient(gw_app.app) as client:
            status, lines = _stream_lines(client, {
                "model": "fast", "stream": True, "tools": tools,
                "messages": [{"role": "user", "content": "hello"}]})
        assert status == 200
        events, done = _parse_sse(lines)
        assert done
        # No tool_calls delta — just content + stop
        tc_events = [e for e in events if e["choices"][0]["delta"].get("tool_calls")]
        assert len(tc_events) == 0
        assert _content(events) == "Hello world"
        finish_events = [e for e in events
                         if e["choices"][0].get("finish_reason") == "stop"]
        assert len(finish_events) == 1

    _with_fake_llama(fn, stream_script=["Hello", " world"])


def test_stream_unknown_model_400(wired, gguf):
    wired(_local_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "nope", "stream": True,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400
    assert "unknown model" in r.json()["error"]["message"]


def test_stream_engine_error_yields_error_event(wired, gguf):
    cfgd = _local_config(gguf)
    cfgd["backends"]["local"]["models"]["fast"]["gguf"] = "not-on-disk"
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        status, lines = _stream_lines(client, {
            "model": "fast", "stream": True,
            "messages": [{"role": "user", "content": "hi"}]})
    assert status == 200  # SSE started; the error arrives as an event
    events, done = _parse_sse(lines)
    assert done
    assert lines[-1] == "data: [DONE]"
    # exactly one event, and it is an OpenAI-shaped error object
    assert len(events) == 1
    err = events[0].get("error") or {}
    assert err.get("type") == "server_error"
    assert err.get("code") == "execution_error"
    assert "not found" in err.get("message", "")


# ── healthz ────────────────────────────────────────────────────────────────

def test_healthz(wired, gguf, monkeypatch):
    _, cfg, engine = wired(_local_config(gguf))
    monkeypatch.setattr(gw_app, "detect_gpus", lambda **kw: [
        {"index": 0, "name": "Fake 3090", "total_mb": 24564, "used_mb": 25000}])

    def fn(records):
        engine._get_instance("fast")  # one resident model
        with TestClient(gw_app.app) as client:
            r = client.get("/healthz")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["version"] == gw_app.app.version
        assert body["backends"]["local"]["type"] == "embedded"
        assert body["discovered_models"] == 0  # tmp models_dir is empty
        assert body["loaded_count"] == 1
        assert body["loaded"][0]["alias"] == "fast"
        assert body["loaded"][0]["role"] == "chat"
        assert body["loaded"][0]["n_ctx"] == 1024
        gpu = body["gpus"][0]
        assert gpu["free_mb"] == 0  # max(24564 - 25000, 0)

    _with_fake_llama(fn)


def test_healthz_free_vram_computed(wired, gguf, monkeypatch):
    wired(_local_config(gguf))
    monkeypatch.setattr(gw_app, "detect_gpus", lambda **kw: [
        {"index": 0, "name": "G1", "total_mb": 1000, "used_mb": 250}])
    with TestClient(gw_app.app) as client:
        r = client.get("/healthz")
    assert r.json()["gpus"][0]["free_mb"] == 750


def test_healthz_uses_short_gpu_ttl(wired, gguf, monkeypatch):
    """/healthz must NOT use the 30s placement cache — after a model
    loads, the cached pre-load snapshot showed 'used 0 MB' until the
    cache aged out (observed live)."""
    wired(_local_config(gguf))
    seen: list[float] = []

    def fake_detect(ttl_s=30.0):
        seen.append(ttl_s)
        return [{"index": 0, "name": "G1", "total_mb": 1000,
                 "used_mb": 100}]

    monkeypatch.setattr(gw_app, "detect_gpus", fake_detect)
    with TestClient(gw_app.app) as client:
        assert client.get("/healthz").status_code == 200
    assert seen and all(t < 30.0 for t in seen)
