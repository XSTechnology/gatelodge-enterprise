"""LLM Gateway Stage 3 — cloud backends, alias chains, hedge, JSONL log.

Chain plans (entries/hedge_s/local_only filtering); cloud execution through
a fake AsyncOpenAI (remote model ids, max_tokens cap 800); hedge race
(primary fast → fallback never starts; primary slow → fallback wins;
primary error → fallback answers); local_only never constructs a cloud
client; cloud SSE passthrough; streaming executes the primary only; one
JSONL line per request.
"""
import asyncio
import json
import sys
import time
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.cloud as gw_cloud  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402
from gateway.cloud import CloudBackendError  # noqa: E402


# ── FakeLlama (embedded) ───────────────────────────────────────────────────

def _with_fake_llama(fn, script=None, fail=False, delay=0.0, stream_script=None):
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.script = list(script or [])

        def create_chat_completion(self, **kwargs):
            if kwargs.get("stream"):
                pieces = (list(stream_script) if stream_script is not None
                          else ["Hello", " world"])

                def _gen():
                    for p in pieces:
                        yield {"choices": [{"delta": {"content": p}}]}

                return _gen()
            if delay:
                time.sleep(delay)
            if fail:
                raise RuntimeError("boom")
            records.append({"call": kwargs})
            if self.script:
                return self.script.pop(0)
            return {"choices": [{"message": {"content": "local answer"}}],
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


# ── Fake cloud client (AsyncOpenAI-shaped) ─────────────────────────────────

class _FakeResp:
    def __init__(self, d: dict):
        self._d = d

    def model_dump_json(self) -> str:
        return json.dumps(self._d)


def _cloud_nonstream(model):
    return {"id": "cc-1", "object": "chat.completion", "created": 1,
            "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant",
                                     "content": f"cloud[{model}] answer"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3,
                      "total_tokens": 8}}


def _cloud_stream_chunks(model):
    def _ch(delta, finish=None):
        return {"id": "cs-1", "object": "chat.completion.chunk", "created": 1,
                "model": model,
                "choices": [{"index": 0, "delta": delta,
                             "finish_reason": finish}]}
    return [_ch({"role": "assistant", "content": ""}),
            _ch({"content": "Cloud says hi"}),
            _ch({}, finish="stop")]


class _FakeCloud:
    """get_cloud_client replacement. delay: seconds before answering."""

    def __init__(self, delay: float = 0.0):
        self.calls: list[dict] = []
        self._delay = delay
        self.chat = types.SimpleNamespace(
            completions=types.SimpleNamespace(create=self._create))

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self._delay:
            await asyncio.sleep(self._delay)
        remote = kwargs.get("model", "remote-model")
        if kwargs.get("stream"):
            chunks = _cloud_stream_chunks(remote)

            async def _gen():
                for c in chunks:
                    yield _FakeResp(c)

            return _gen()
        return _FakeResp(_cloud_nonstream(remote))


# ── fixtures ───────────────────────────────────────────────────────────────

GROQ = {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}


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
        monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
        return cfgfile, cfg, engine
    return _wire


def _config(gguf, aliases=None, cloud_delay=0.0):
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {
            "local": {"type": "embedded", "models": {
                "fast": {"gguf": str(gguf), "ctx": 1024, "placement": ""}}},
            "groq": GROQ,
        },
        "aliases": aliases or {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
    }


def _patch_cloud(monkeypatch, fake: _FakeCloud):
    monkeypatch.setattr(gw_cloud, "get_cloud_client",
                        lambda name, backend, timeout=120.0: fake)
    return fake


# ── chain plan resolution ──────────────────────────────────────────────────

def test_chain_plan_alias_full(wired, gguf):
    _, cfg, _ = wired(_config(gguf, aliases={
        "smart": {"chain": ["local:fast", "groq:cloud-fast"], "hedge_s": 8}}))
    plan = cfg.resolve_chain("smart")
    assert plan["entries"] == [("local", "fast"), ("groq", "cloud-fast")]
    assert plan["hedge_s"] == 8.0
    assert plan["local_only"] is False


def test_chain_plan_direct_and_bare(wired, gguf):
    _, cfg, _ = wired(_config(gguf))
    assert cfg.resolve_chain("local:fast") == {
        "entries": [("local", "fast")], "hedge_s": 0.0, "local_only": False}
    assert cfg.resolve_chain("groq:cloud-fast")["entries"] == [("groq", "cloud-fast")]
    assert cfg.resolve_chain("fast")["entries"] == [("local", "fast")]


def test_chain_plan_local_only_filters_cloud(wired, gguf):
    _, cfg, _ = wired(_config(gguf, aliases={
        "private": {"chain": ["local:fast", "groq:cloud-fast"],
                    "local_only": True}}))
    plan = cfg.resolve_chain("private")
    assert plan["entries"] == [("local", "fast")]  # cloud stripped
    assert plan["local_only"] is True


def test_chain_plan_local_only_all_cloud_errors(wired, gguf):
    _, cfg, _ = wired(_config(gguf, aliases={
        "broken": {"chain": ["groq:cloud-fast"], "local_only": True}}))
    with pytest.raises(gw_config.GatewayConfigError):
        cfg.resolve_chain("broken")


# ── cloud execution ────────────────────────────────────────────────────────

def test_cloud_chat_executes(wired, gguf, monkeypatch):
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["choices"][0]["message"]["content"] == "cloud[llama-3.1-8b-instant] answer"
        assert body["model"] == "groq:cloud-fast"  # echoed, like embedded
        assert body["served_by"]["backend"] == "groq"
        assert body["served_by"]["model"] == "cloud-fast"
        assert body["usage"]["total_tokens"] == 8
        # the remote call used the REMOTE model id
        assert fake.calls[0]["model"] == "llama-3.1-8b-instant"
        assert fake.calls[0]["messages"] == [{"role": "user", "content": "hi"}]
        # no local model was ever loaded
        assert records == []

    _with_fake_llama(fn)


def test_cloud_max_tokens_capped(wired, gguf, monkeypatch):
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast", "max_tokens": 2000,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    assert fake.calls[0]["max_tokens"] == 800  # capped
    # omitted max_tokens stays omitted
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert "max_tokens" not in fake.calls[1]


def test_cloud_missing_key_502(wired, gguf, monkeypatch):
    # never point at a real provider here — use a guaranteed-unset env var
    monkeypatch.delenv("GATEWAY_TEST_KEY_DEFINITELY_UNSET", raising=False)
    cfgd = _config(gguf)
    cfgd["backends"]["groq"]["api_key_env"] = "GATEWAY_TEST_KEY_DEFINITELY_UNSET"
    wired(cfgd)
    # real get_cloud_client (not patched) — key env unset
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "GATEWAY_TEST_KEY_DEFINITELY_UNSET" in r.json()["error"]["message"]


def test_cloud_error_maps_502(wired, gguf, monkeypatch):
    wired(_config(gguf))
    fake = _FakeCloud()

    async def _boom(**kwargs):
        raise RuntimeError("connection refused")

    fake.chat.completions.create = _boom
    _patch_cloud(monkeypatch, fake)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "connection refused" in r.json()["error"]["message"]


# ── hedge ──────────────────────────────────────────────────────────────────

def test_hedge_primary_fast_local_wins(wired, gguf, monkeypatch):
    """Primary completes within hedge_s → the fallback never starts."""
    wired(_config(gguf, aliases={
        "smart": {"chain": ["local:fast", "groq:cloud-fast"], "hedge_s": 5}}))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200
        assert r.json()["choices"][0]["message"]["content"] == "local answer"
        assert r.json()["served_by"]["backend"] == "local"
        assert fake.calls == []  # fallback never started

    _with_fake_llama(fn)


def test_hedge_primary_slow_cloud_wins(wired, gguf, monkeypatch):
    """Primary slower than hedge_s → the fallback races and wins."""
    wired(_config(gguf, aliases={
        "smart": {"chain": ["local:fast", "groq:cloud-fast"], "hedge_s": 0.2}}))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            t0 = time.perf_counter()
            r = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "hi"}]})
            elapsed = time.perf_counter() - t0
            # (portal teardown after post() would wait out the zombie
            #  embedded task — the RESPONSE itself must beat the primary)
        assert r.status_code == 200, r.text
        assert r.json()["served_by"]["backend"] == "groq"
        assert r.json()["choices"][0]["message"]["content"].startswith("cloud[")
        assert elapsed < 0.9, f"hedge did not race ({elapsed:.2f}s)"

    # local primary sleeps 1.0s (inside the to_thread worker); hedge 0.2s
    _with_fake_llama(fn, delay=1.0)


def test_hedge_primary_error_fallback_answers(wired, gguf, monkeypatch):
    """Primary errors immediately → the fallback answers."""
    wired(_config(gguf, aliases={
        "smart": {"chain": ["local:fast", "groq:cloud-fast"], "hedge_s": 5}}))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200, r.text
        assert r.json()["served_by"]["backend"] == "groq"

    _with_fake_llama(fn, fail=True)


def test_local_only_never_constructs_cloud_client(wired, gguf, monkeypatch):
    """Privacy guarantee: a local_only alias never leaves the machine."""
    wired(_config(gguf, aliases={
        "private": {"chain": ["local:fast", "groq:cloud-fast"],
                    "local_only": True, "hedge_s": 0.1}}))

    def _forbidden(name, backend, timeout=120.0):
        raise AssertionError("cloud client constructed for local_only alias")

    monkeypatch.setattr(gw_cloud, "get_cloud_client", _forbidden)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "private",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200, r.text
        assert r.json()["served_by"]["backend"] == "local"

    _with_fake_llama(fn)


# ── streaming ──────────────────────────────────────────────────────────────

def test_cloud_stream_passthrough(wired, gguf, monkeypatch):
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
            "model": "groq:cloud-fast", "stream": True,
            "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/event-stream")
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    assert all(e["object"] == "chat.completion.chunk" for e in events)
    content = "".join(e["choices"][0]["delta"].get("content") or ""
                      for e in events)
    assert content == "Cloud says hi"
    assert events[-1]["choices"][0]["finish_reason"] == "stop"
    assert fake.calls[0]["stream"] is True
    assert fake.calls[0]["model"] == "llama-3.1-8b-instant"


def test_cloud_stream_with_tools(wired, gguf, monkeypatch):
    """Streaming with tools on cloud: tools forwarded as-is to the client."""
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    tools = [{"type": "function", "function": {
        "name": "get_weather", "description": "d",
        "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}}]
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
            "model": "groq:cloud-fast", "stream": True, "tools": tools,
            "messages": [{"role": "user", "content": "weather in sf"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    # Tools were forwarded to the cloud client
    assert fake.calls[0].get("tools") == tools
    assert fake.calls[0]["stream"] is True


def test_stream_executes_primary_only(wired, gguf, monkeypatch):
    """Streaming ignores chain fallbacks — primary entry only."""
    wired(_config(gguf, aliases={
        "smart": {"chain": ["local:fast", "groq:cloud-fast"], "hedge_s": 5}}))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            with client.stream("POST", "/v1/chat/completions", json={
                "model": "smart", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
                assert r.status_code == 200
                lines = [ln for ln in r.iter_lines() if ln.strip()]
        assert lines[-1] == "data: [DONE]"
        assert fake.calls == []  # cloud never touched

    _with_fake_llama(fn, stream_script=["Local ", "stream"])


# ── JSONL request log ──────────────────────────────────────────────────────

def test_jsonl_log_one_line_per_request(wired, gguf, monkeypatch):
    _, cfg, engine = wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r1 = client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "hi"}]})
            assert r1.status_code == 200
            r2 = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "hi"}]})
            assert r2.status_code == 200
            with client.stream("POST", "/v1/chat/completions", json={
                "model": "fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r3:
                assert r3.status_code == 200
                [ln for ln in r3.iter_lines()]
            r4 = client.post("/v1/chat/completions", json={
                "model": "nope",
                "messages": [{"role": "user", "content": "hi"}]})
            assert r4.status_code == 400
        lines = gw_app._LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 4
        entries = [json.loads(ln) for ln in lines]
        by_model = {e["model"]: e for e in entries}
        assert set(by_model) == {"fast", "groq:cloud-fast", "nope"}
        assert by_model["fast"]["backend"] == "local"
        assert by_model["groq:cloud-fast"]["backend"] == "groq"
        assert by_model["groq:cloud-fast"]["usage"]["total_tokens"] == 8
        assert by_model["nope"]["status"] == 400
        assert by_model["fast"]["status"] == 200
        # the stream request: one of the two fast lines has stream=true
        fast_lines = [e for e in entries if e["model"] == "fast"]
        assert any(e["stream"] for e in fast_lines)
        assert all(set(e) >= {"ts", "model", "backend", "stream", "ms",
                              "usage", "status"} for e in entries)

    _with_fake_llama(fn)
