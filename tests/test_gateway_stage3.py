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

import httpx
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
        # month-scoped log (the legacy path migrates on first write)
        lines = (gw_app._month_log_path(gw_app._month_key())
                 .read_text(encoding="utf-8").strip().splitlines())
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


# ── api key resolution: env → OS keyring ───────────────────────────────────

def test_key_env_beats_keyring(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "env-key")
    monkeypatch.setitem(sys.modules, "keyring", types.SimpleNamespace(
        get_password=lambda svc, user: "ring-key"))
    assert gw_cloud._env_or_keyring("GROQ_API_KEY") == "env-key"


def test_key_keyring_fallback_both_casings(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    seen: list[str] = []

    def _get(svc, user):
        seen.append(svc)
        return "ring-key" if svc == "workshop" and user == "GROQ_API_KEY" else None

    monkeypatch.setitem(sys.modules, "keyring",
                        types.SimpleNamespace(get_password=_get))
    assert gw_cloud._env_or_keyring("GROQ_API_KEY") == "ring-key"
    assert seen == ["Workshop", "workshop"]  # both casings tried in order


def test_cloud_client_builds_from_keyring(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "keyring", types.SimpleNamespace(
        get_password=lambda svc, user: "ring-key" if user == "GROQ_API_KEY" else None))
    # fresh backend dict — the module-level GROQ is mutated by earlier tests
    backend = {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
               "api_key_env": "GROQ_API_KEY",
               "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    gw_cloud.reset_clients()
    try:
        c1 = gw_cloud.get_cloud_client("krg-test", backend)
        c2 = gw_cloud.get_cloud_client("krg-test", backend)
        assert c1 is not None and c2 is c1  # cached per signature
    finally:
        gw_cloud.reset_clients()


def test_key_missing_everywhere_raises(monkeypatch):
    monkeypatch.delenv("GATEWAY_TEST_KEY_DEFINITELY_UNSET", raising=False)
    monkeypatch.setitem(sys.modules, "keyring",
                        types.SimpleNamespace(get_password=lambda s, u: None))
    gw_cloud.reset_clients()
    with pytest.raises(CloudBackendError, match="keyring"):
        gw_cloud.get_cloud_client("nokey", {
            "type": "openai", "base_url": "https://x.example/v1",
            "api_key_env": "GATEWAY_TEST_KEY_DEFINITELY_UNSET",
            "models": {}})


# ── per-backend max_tokens policy ──────────────────────────────────────────

def test_cap_max_tokens_policy():
    b = {"type": "openai"}
    assert gw_cloud.cap_max_tokens(b, 2000) == 800            # default cap
    assert gw_cloud.cap_max_tokens(b, 50) == 50               # under cap
    assert gw_cloud.cap_max_tokens({**b, "max_tokens_cap": 100}, 2000) == 100
    assert gw_cloud.cap_max_tokens({**b, "max_tokens_cap": None}, 2000) == 2000
    assert gw_cloud.cap_max_tokens({"type": "runpod"}, 2000) == 2000
    assert gw_cloud.cap_max_tokens({"type": "llama-server"}, 2000) == 2000
    assert gw_cloud.cap_max_tokens(b, None) is None           # omitted stays omitted


def test_backend_cap_override_end_to_end(wired, gguf, monkeypatch):
    """max_tokens_cap: null = uncapped (HF router / gpt-oss), int = custom,
    absent = default 800 — parsed by config, applied at the cap sites."""
    cfgd = _config(gguf)
    # fresh dicts — the module-level GROQ is shared (and mutated) by
    # other tests; never write through it
    cfgd["backends"]["groq"] = {
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY", "max_tokens_cap": 100,
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    cfgd["backends"]["hf"] = {
        "type": "openai", "base_url": "https://router.huggingface.co/v1",
        "api_key_env": "HF_TOKEN", "max_tokens_cap": None,
        "models": {"glm53": "zai-org/GLM-5.3-Flash"}}
    _, cfg, _ = wired(cfgd)
    assert cfg.backends["hf"]["max_tokens_cap"] is None
    assert cfg.backends["groq"]["max_tokens_cap"] == 100
    assert "max_tokens_cap" not in cfg.backends["local"]  # default applies
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "hf:glm53", "max_tokens": 2000,
                "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            assert fake.calls[0]["max_tokens"] == 2000   # null = uncapped
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast", "max_tokens": 2000,
                "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            assert fake.calls[1]["max_tokens"] == 100    # custom cap
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast", "max_tokens": 50,
                "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            assert fake.calls[2]["max_tokens"] == 50     # under cap unchanged

    _with_fake_llama(fn)


# ── chain cold breaker ─────────────────────────────────────────────────────

def _breaker_cfg(gguf, hedge=0.2):
    return _config(gguf, aliases={
        "smart": {"chain": ["groq:cloud-fast", "local:fast"],
                  "hedge_s": hedge}})


def test_breaker_opens_after_stall_then_skips_primary(wired, gguf, monkeypatch):
    """Primary stalls past hedge_s → fallback answers + breaker opens; the
    NEXT request starts at the fallback without calling the primary again
    (no hedge_s burned, no remote round-trip)."""
    wired(_breaker_cfg(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud(delay=2.0))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r1 = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "q1"}]})
            assert r1.status_code == 200, r1.text
            assert r1.json()["served_by"]["backend"] == "local"
            assert gw_app._breaker_open("groq:cloud-fast") is True
            r2 = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "q2"}]})
            assert r2.status_code == 200, r2.text
            assert r2.json()["served_by"]["backend"] == "local"
        assert len(fake.calls) == 1  # primary attempted once, then skipped

    gw_app.reset_breakers()
    try:
        _with_fake_llama(fn)
    finally:
        gw_app.reset_breakers()


def test_breaker_primary_error_also_opens(wired, gguf, monkeypatch):
    """A fast primary ERROR (not a stall) opens the breaker too — a dead
    backend must not be retried at full hedge cost every request."""
    wired(_breaker_cfg(gguf, hedge=5))
    calls: list[int] = []

    async def _boom_create(**kwargs):
        calls.append(1)
        raise RuntimeError("cloud down")

    fake = _FakeCloud()
    fake.chat.completions.create = _boom_create
    _patch_cloud(monkeypatch, fake)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r1 = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "q1"}]})
            assert r1.status_code == 200, r1.text
            assert r1.json()["served_by"]["backend"] == "local"
            assert gw_app._breaker_open("groq:cloud-fast") is True
            r2 = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "q2"}]})
            assert r2.status_code == 200, r2.text
            assert r2.json()["served_by"]["backend"] == "local"
        assert len(calls) == 1  # errored once, then skipped

    gw_app.reset_breakers()
    try:
        _with_fake_llama(fn)
    finally:
        gw_app.reset_breakers()


def test_breaker_expires_primary_retried(wired, gguf, monkeypatch):
    """After the cooldown the primary is attempted again."""
    wired(_breaker_cfg(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud(delay=2.0))

    def fn(records):
        with TestClient(gw_app.app) as client:
            client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "q1"}]})
            assert gw_app._breaker_open("groq:cloud-fast") is True
            # force-expire the cooldown window
            gw_app._COLD_UNTIL["groq:cloud-fast"] -= gw_app._COLD_BREAK_S + 1
            assert gw_app._breaker_open("groq:cloud-fast") is False
            client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "q2"}]})
        assert len(fake.calls) == 2  # primary retried after expiry

    gw_app.reset_breakers()
    try:
        _with_fake_llama(fn)
    finally:
        gw_app.reset_breakers()


def test_breaker_primary_fast_win_keeps_it_closed(wired, gguf, monkeypatch):
    """A primary that answers within hedge_s wins and never opens the
    breaker — slow backends are only skipped after they actually stall."""
    wired(_breaker_cfg(gguf, hedge=5))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "smart",
                "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            assert r.json()["served_by"]["backend"] == "groq"
            assert gw_app._breaker_open("groq:cloud-fast") is False
        assert len(fake.calls) == 1

    gw_app.reset_breakers()
    try:
        _with_fake_llama(fn)
    finally:
        gw_app.reset_breakers()


def test_breaker_not_opened_on_client_error(wired, gguf, monkeypatch):
    """A 4xx from the primary is the REQUEST's fault — the breaker stays
    closed and the primary is retried on the next request."""
    wired(_breaker_cfg(gguf, hedge=5))
    calls: list[int] = []

    async def _client_err(**kwargs):
        calls.append(1)
        e = RuntimeError("bad request")
        e.status_code = 400
        raise e

    fake = _FakeCloud()
    fake.chat.completions.create = _client_err
    _patch_cloud(monkeypatch, fake)

    def fn(records):
        with TestClient(gw_app.app) as client:
            for q in ("first question", "second question"):
                r = client.post("/v1/chat/completions", json={
                    "model": "smart",
                    "messages": [{"role": "user", "content": q}]})
                assert r.status_code == 200, r.text
                assert r.json()["served_by"]["backend"] == "local"
        assert len(calls) == 2  # primary retried — no breaker
        assert gw_app._breaker_open("groq:cloud-fast") is False

    gw_app.reset_breakers()
    try:
        _with_fake_llama(fn)
    finally:
        gw_app.reset_breakers()


# ── error classification ───────────────────────────────────────────────────

def test_classify_error_buckets():
    from openai import APIConnectionError, APITimeoutError

    assert gw_cloud.classify_error(APIConnectionError(request=None)) == "network"
    assert gw_cloud.classify_error(APITimeoutError(request=None)) == "timeout"

    def _err(status):
        e = RuntimeError("http error")
        e.status_code = status
        return e

    assert gw_cloud.classify_error(_err(429)) == "rate_limit"
    assert gw_cloud.classify_error(_err(500)) == "server"
    assert gw_cloud.classify_error(_err(502)) == "server"
    assert gw_cloud.classify_error(_err(401)) == "auth"
    assert gw_cloud.classify_error(_err(400)) == "client"
    assert gw_cloud.classify_error(RuntimeError("mystery")) == "unknown"
    # gateway call sites wrap the original — __cause__ chains are walked
    wrapped = CloudBackendError("cloud backend 'groq' call failed")
    wrapped.__cause__ = _err(503)
    assert gw_cloud.classify_error(wrapped) == "server"


def test_error_class_in_request_log(wired, gguf, monkeypatch):
    """502 responses log the classified failure kind (err field)."""
    wired(_config(gguf))

    async def _server_err(**kwargs):
        e = RuntimeError("upstream exploded")
        e.status_code = 502
        raise e

    fake = _FakeCloud()
    fake.chat.completions.create = _server_err
    _patch_cloud(monkeypatch, fake)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user",
                              "content": "trigger a classified failure"}]})
            assert r.status_code == 502
        lines = [json.loads(ln) for ln in
                 (gw_app._month_log_path(gw_app._month_key())
                  .read_text(encoding="utf-8").strip().splitlines())]
        assert lines[-1]["status"] == 502
        assert lines[-1]["err"] == "server"

    _with_fake_llama(fn)


# ── response cache ─────────────────────────────────────────────────────────

def test_cache_simple_prompt_served_from_memory(wired, gguf, monkeypatch):
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r1 = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "cache me once"}]})
            assert r1.status_code == 200
            r2 = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "cache me once"}]})
            assert r2.status_code == 200
            assert r2.json() == r1.json()
        assert len(fake.calls) == 1  # remote billed once
        lines = [json.loads(ln) for ln in
                 (gw_app._month_log_path(gw_app._month_key())
                  .read_text(encoding="utf-8").strip().splitlines())]
        assert lines[-1]["backend"] == "cache"
        assert gw_app.cache_stats() == {"entries": 1, "hits": 1}

    _with_fake_llama(fn)


def test_cache_skips_tools_multiturn_timeful_and_local_only(wired, gguf, monkeypatch):
    cfgd = _config(gguf, aliases={
        "private": {"chain": ["local:fast"], "local_only": True}})
    wired(cfgd)
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                # tools → never cached
                client.post("/v1/chat/completions", json={
                    "model": "groq:cloud-fast",
                    "tools": [{"type": "function",
                               "function": {"name": "f", "parameters": {}}}],
                    "messages": [{"role": "user",
                                  "content": "tool prompt never cached"}]})
                # multi-turn → never cached
                client.post("/v1/chat/completions", json={
                    "model": "groq:cloud-fast",
                    "messages": [{"role": "user", "content": "a"},
                                 {"role": "assistant", "content": "b"},
                                 {"role": "user", "content": "c"}]})
                # time-sensitive → never cached
                client.post("/v1/chat/completions", json={
                    "model": "groq:cloud-fast",
                    "messages": [{"role": "user",
                                  "content": "what time is it right now"}]})
        assert len(fake.calls) == 6
        assert gw_app.cache_stats()["entries"] == 0
        # local_only alias: privacy scope — executed twice, never cached
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                r = client.post("/v1/chat/completions", json={
                    "model": "private",
                    "messages": [{"role": "user",
                                  "content": "local only never cached"}]})
                assert r.status_code == 200, r.text
        assert len([r for r in records if "call" in r]) == 2
        assert gw_app.cache_stats()["entries"] == 0

    _with_fake_llama(fn)


def test_cache_key_covers_max_tokens(wired, gguf, monkeypatch):
    """Same prompt + different max_tokens = different answers — both
    execute (the key includes the generation params)."""
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            for mt in (100, 200):
                r = client.post("/v1/chat/completions", json={
                    "model": "groq:cloud-fast", "max_tokens": mt,
                    "messages": [{"role": "user", "content": "same prompt"}]})
                assert r.status_code == 200
        assert len(fake.calls) == 2

    _with_fake_llama(fn)


def test_cache_disabled_via_config(wired, gguf, monkeypatch):
    cfgd = _config(gguf)
    cfgd["response_cache"] = {"enabled": False}
    wired(cfgd)
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                r = client.post("/v1/chat/completions", json={
                    "model": "groq:cloud-fast",
                    "messages": [{"role": "user", "content": "cache is off"}]})
                assert r.status_code == 200
        assert len(fake.calls) == 2
        assert gw_app.cache_stats()["entries"] == 0

    _with_fake_llama(fn)


# ── /admin/verify ──────────────────────────────────────────────────────────

def test_admin_verify_probes_all_backends(wired, gguf, monkeypatch):
    cfgd = _config(gguf)
    cfgd["backends"]["rp"] = {
        "type": "runpod", "api_key_env": "RUNPOD_API_KEY",
        "models": {"podbig": {"gpu_type_ids": ["NVIDIA RTX A6000"],
                              "image": "ghcr.io/x", "alias": "podbig"}}}
    wired(cfgd)
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/verify", json={})
            assert r.status_code == 200, r.text
            data = r.json()
            rows = {f"{x['backend']}:{x['model']}": x for x in data["results"]}
            assert set(rows) == {"local:fast", "groq:cloud-fast", "rp:podbig"}
            assert rows["groq:cloud-fast"]["pass"] is True
            assert rows["groq:cloud-fast"]["latency_ms"] is not None
            assert rows["groq:cloud-fast"]["detail"].startswith("answered:")
            assert rows["local:fast"]["pass"] is True
            assert rows["rp:podbig"]["pass"] is False
            assert "skipped" in rows["rp:podbig"]["detail"]
            assert data["summary"] == {"total": 3, "pass": 2, "fail": 1}
        assert len(fake.calls) == 1  # one cloud round-trip

    _with_fake_llama(fn)


def test_admin_verify_reports_failures(wired, gguf, monkeypatch):
    wired(_config(gguf))

    async def _boom(**kwargs):
        raise RuntimeError("cloud down")

    fake = _FakeCloud()
    fake.chat.completions.create = _boom
    _patch_cloud(monkeypatch, fake)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/verify", json={"timeout_s": 5})
            assert r.status_code == 200, r.text
            data = r.json()
            rows = {f"{x['backend']}:{x['model']}": x for x in data["results"]}
            assert rows["groq:cloud-fast"]["pass"] is False
            assert rows["groq:cloud-fast"]["detail"].startswith("unknown:")
            assert rows["local:fast"]["pass"] is True
            assert data["summary"]["fail"] == 1

    _with_fake_llama(fn)


def test_admin_verify_backends_filter(wired, gguf, monkeypatch):
    """{"backends": [...]} probes a subset — e.g. only the cloud backend
    on a box whose local models don't fit in free VRAM."""
    wired(_config(gguf))
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/verify", json={"backends": ["groq"]})
            assert r.status_code == 200, r.text
            data = r.json()
            rows = [f"{x['backend']}:{x['model']}" for x in data["results"]]
            assert rows == ["groq:cloud-fast"]  # local backend untouched
            assert data["summary"]["total"] == 1
        assert len(fake.calls) == 1
        assert len(records) == 0  # no embedded model was ever loaded

    _with_fake_llama(fn)


# ── classify-aware error propagation (NO engine switching) ─────────────────

def _failing_cloud(monkeypatch, exc):
    fake = _FakeCloud()

    async def _boom(**kwargs):
        raise exc

    fake.chat.completions.create = _boom
    return _patch_cloud(monkeypatch, fake)


def test_upstream_rate_limit_maps_429_with_retry_after(wired, gguf, monkeypatch):
    """Upstream 429 → our 429 + Retry-After: OpenAI clients back off
    natively instead of hammering a rate-limited provider."""
    wired(_config(gguf))
    e = RuntimeError("Too many requests")
    e.status_code = 429
    _failing_cloud(monkeypatch, e)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "rate limited request"}]})
    assert r.status_code == 429
    assert r.headers["retry-after"] == "1"
    body = r.json()["error"]
    assert body["type"] == "rate_limit_error"
    assert body["code"] == "rate_limited"
    assert "Too many requests" in body["message"]


def test_upstream_timeout_maps_504(wired, gguf, monkeypatch):
    wired(_config(gguf))
    from openai import APITimeoutError
    _failing_cloud(monkeypatch, APITimeoutError(request=None))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user",
                          "content": "slow upstream should time out"}]})
    assert r.status_code == 504
    assert r.json()["error"]["code"] == "timeout"


def test_upstream_auth_maps_502_with_class(wired, gguf, monkeypatch):
    """A bad key is OUR config problem — 502 with code=auth tells the
    operator what to fix; the requestor can't do anything about it."""
    wired(_config(gguf))
    e = RuntimeError("invalid api key")
    e.status_code = 401
    _failing_cloud(monkeypatch, e)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "auth failing request"}]})
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "auth"


def test_upstream_unknown_stays_502(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _failing_cloud(monkeypatch, RuntimeError("connection refused"))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user",
                          "content": "mystery failure kind request"}]})
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "unknown"


def test_stream_error_event_carries_class(wired, gguf, monkeypatch):
    """Mid-flight stream errors: SSE has no status to change — the error
    EVENT carries the class so clients can react."""
    wired(_config(gguf))
    e = RuntimeError("upstream exploded")
    e.status_code = 500
    fake = _FakeCloud()

    async def _boom(**kwargs):
        raise e

    fake.chat.completions.create = _boom
    _patch_cloud(monkeypatch, fake)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "groq:cloud-fast", "stream": True,
                "messages": [{"role": "user",
                              "content": "stream failure probe"}]}) as r:
            assert r.status_code == 200  # SSE headers already sent
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    err = json.loads(lines[-2][len("data: "):])
    assert err["error"]["code"] == "server"
    assert lines[-1] == "data: [DONE]"


# ── console model-add: "name" key survives the normalizer ──────────────────

def test_config_accepts_name_key_for_cloud_models(wired, gguf):
    """The console's model form writes {"name": <remote_id>} for openai /
    llama-server-remote models — the normalizer must read it or those
    entries silently vanish on reload (observed bug)."""
    _, cfg, _ = wired(_config(gguf))
    raw = json.dumps({
        "port": 9911, "bind": "127.0.0.1", "models_dir": str(gguf.parent / "models"),
        "backends": {
            "groq": {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
                     "api_key_env": "GROQ_API_KEY",
                     "models": {"a": {"name": "org/model-a"},
                                "b": "org/model-b",
                                "c": {"id": "org/model-c"}}},
            "ws": {"type": "llama-server", "base_url": "http://127.0.0.1:1/v1",
                   "models": {"d": {"name": "remote-d"}}}},
        "aliases": {}})
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "gw.json"
        p.write_text(raw, encoding="utf-8")
        c2 = gw_config.GatewayConfig(p)
    assert c2.backends["groq"]["models"] == {
        "a": "org/model-a", "b": "org/model-b", "c": "org/model-c"}
    assert c2.backends["ws"]["models"] == {"d": "remote-d"}


def test_console_add_model_survives_reload(wired, gguf, monkeypatch):
    """POST /admin/models (the console form) for an openai backend → the
    entry persists through a config reload with the right wire id."""
    cfgfile, cfg, _ = wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/models", json={
            "name": "glm53", "backend": "groq", "role": "chat",
            "remote_id": "zai-org/GLM-5.3-Flash"})
        assert r.status_code == 200, r.text
    fresh = gw_config.GatewayConfig(cfgfile)
    assert fresh.backends["groq"]["models"]["glm53"] == "zai-org/GLM-5.3-Flash"


# ── /admin/backends/{name}/remote-models (cloud catalog proxy) ─────────────

def test_remote_models_proxies_catalog(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _config(gguf)
    # fresh dict — the module-level GROQ is mutated by earlier tests
    cfgd["backends"]["groq"] = {
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    wired(cfgd)

    async def _fake_fetch(base_url, api_key):
        assert base_url == "https://api.groq.com/openai/v1"
        return {"data": [{"id": "zai-org/GLM-5.3-Flash",
                          "pricing": {"groq": 0.1}},
                         {"id": "openai/gpt-oss-120b"}]}

    monkeypatch.setattr(gw_app, "_fetch_remote_catalog", _fake_fetch)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends/groq/remote-models")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["backend"] == "groq"
    assert d["count"] == 2
    # metadata passes through verbatim for the console picker
    assert d["models"][0]["pricing"] == {"groq": 0.1}


def test_remote_models_rejects_non_openai_backend(wired, gguf):
    wired(_config(gguf))  # "local" is embedded
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends/local/remote-models")
    assert r.status_code == 400
    r = client.get("/admin/backends/nope/remote-models")
    assert r.status_code == 404


def test_remote_models_missing_key_is_auth_error(wired, gguf, monkeypatch):
    monkeypatch.delenv("GATEWAY_TEST_KEY_DEFINITELY_UNSET", raising=False)
    monkeypatch.setitem(sys.modules, "keyring",
                        types.SimpleNamespace(get_password=lambda s, u: None))
    cfgd = _config(gguf)
    cfgd["backends"]["groq"] = {
        "type": "openai", "base_url": "https://x.example/v1",
        "api_key_env": "GATEWAY_TEST_KEY_DEFINITELY_UNSET",
        "models": {"m": "x"}}
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends/groq/remote-models")
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "auth"


def test_remote_models_upstream_failure_is_network(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _config(gguf)
    cfgd["backends"]["groq"] = {  # fresh dict — GROQ is mutated by earlier tests
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    wired(cfgd)

    async def _fetch_boom(base_url, api_key):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(gw_app, "_fetch_remote_catalog", _fetch_boom)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends/groq/remote-models")
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "network"


# ── monthly token budget (openai-type cloud backends) ──────────────────────

def _seed_budget_log(tokens_this_month, extra_lines=()):
    """Write ledger lines into the request log: this-month usage for the
    groq backend + decoys (old month, cache hits)."""
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    old = "2020-01-01T00:00:00"
    lines = [
        json.dumps({"ts": now, "model": "groq:cloud-fast", "backend": "groq",
                    "stream": False, "ms": 1,
                    "usage": {"total_tokens": tokens_this_month}, "status": 200}),
        # decoys: different month, cache hit, different backend
        json.dumps({"ts": old, "model": "groq:cloud-fast", "backend": "groq",
                    "stream": False, "ms": 1,
                    "usage": {"total_tokens": 9999}, "status": 200}),
        json.dumps({"ts": now, "model": "groq:cloud-fast", "backend": "cache",
                    "stream": False, "ms": 0,
                    "usage": {"total_tokens": 9999}, "status": 200}),
    ] + list(extra_lines)
    gw_app._LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_token_budget_blocks_and_charges(wired, gguf, monkeypatch):
    cfgd = _config(gguf)
    cfgd["backends"]["groq"] = {
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY", "monthly_token_budget": 100,
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    wired(cfgd)
    _seed_budget_log(90)  # 90/100 used this month (decoys excluded)
    fake = _patch_cloud(monkeypatch, _FakeCloud())  # usage total = 8

    with TestClient(gw_app.app) as client:
        # gate checks BEFORE the call: 90 < 100 → executes → ledger 98
        r1 = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "budget under"}]})
        assert r1.status_code == 200, r1.text
        assert gw_app._token_budget_used("groq") == 98
        # 98 < 100 → still allowed → ledger 106 (goes OVER mid-flight)
        r2 = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "budget edge"}]})
        assert r2.status_code == 200, r2.text
        # 106 ≥ 100 → refused without touching the provider
        r3 = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "budget over now"}]})
        assert r3.status_code == 503
        body = r3.json()["error"]
        assert body["code"] == "budget_exceeded"
        assert body["type"] == "overloaded_error"
        assert "106/100" in body["message"]
        assert len(fake.calls) == 2  # the third never reached the provider


def test_token_budget_zero_is_off(wired, gguf, monkeypatch):
    """No monthly_token_budget (default 0) → no enforcement ever."""
    wired(_config(gguf))
    _seed_budget_log(999999)
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "no budget configured"}]})
        assert r.status_code == 200, r.text
        assert len(fake.calls) == 1


def test_token_budget_in_stats(wired, gguf, monkeypatch):
    cfgd = _config(gguf)
    cfgd["backends"]["groq"] = {
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY", "monthly_token_budget": 500,
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    wired(cfgd)
    _seed_budget_log(120)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/stats")
        assert r.status_code == 200
        budgets = r.json()["budgets"]
        assert budgets == [{"backend": "groq", "budget": 500, "used": 120,
                            "month": time.strftime("%Y-%m")}]


# ── tier 1: virtual keys, pricing, log rotation ────────────────────────────

def _secured_config(gguf, api_keys=None, gateway_keys=("admin-secret",)):
    """Config with a fresh groq backend + auth enabled (any configured
    key turns bearer auth on; gateway_keys are the ADMIN keys)."""
    cfgd = _config(gguf)
    cfgd["backends"]["groq"] = {  # fresh dict — GROQ is mutated by earlier tests
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    if gateway_keys is not None:
        cfgd["gateway_keys"] = list(gateway_keys)
    if api_keys is not None:
        cfgd["api_keys"] = api_keys
    return cfgd


def _seed_key_log(key_id: str, tokens: int) -> None:
    """One current-month log line attributed to a virtual key."""
    gw_app._LOG_PATH.write_text(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "model": "groq:cloud-fast", "backend": "groq", "stream": False,
        "ms": 1, "usage": {"total_tokens": tokens}, "status": 200,
        "key": key_id}) + "\n", encoding="utf-8")


def test_virtual_key_auth_and_attribution(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "program-x", "key": "vk-test-1", "owner": "X"}]))
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        # any configured key enables auth: none / wrong → 401
        for headers in ({}, {"Authorization": "Bearer wrong"}):
            r = client.post("/v1/chat/completions", headers=headers, json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "auth check"}]})
            assert r.status_code == 401
        # virtual key works and is attributed in the log
        r2 = client.post("/v1/chat/completions", headers={
            "Authorization": "Bearer vk-test-1"}, json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "attributed request"}]})
        assert r2.status_code == 200, r2.text
        # virtual keys are CONSUMER keys — no admin surface
        r3 = client.get("/admin/stats", headers={
            "Authorization": "Bearer vk-test-1"})
        assert r3.status_code == 403
        # admin key reaches stats and sees the per-key usage
        r4 = client.get("/admin/stats", headers={
            "Authorization": "Bearer admin-secret"})
        assert r4.status_code == 200
        rows = r4.json()["per_key"]
        assert rows and rows[0]["key"] == "program-x"
        assert rows[0]["n"] == 1
        assert rows[0]["total_tokens"] == 8
    lines = [json.loads(ln) for ln in
             (gw_app._month_log_path(gw_app._month_key())
              .read_text(encoding="utf-8").strip().splitlines())]
    keyed = [e for e in lines if e.get("key")]
    assert keyed and keyed[-1]["key"] == "program-x"


def test_admin_keys_create_and_revoke(wired, gguf):
    cfgfile, _, _ = wired(_secured_config(gguf))
    with TestClient(gw_app.app) as client:
        h = {"Authorization": "Bearer admin-secret"}
        r = client.post("/admin/keys", headers=h, json={
            "id": "prog-a", "owner": "A", "monthly_token_budget": 1000})
        assert r.status_code == 200, r.text
        key = r.json()["key"]
        assert key.startswith("vk-")
        # listing shows it with budgets
        r2 = client.get("/admin/keys", headers=h)
        assert r2.status_code == 200
        entries = r2.json()["api_keys"]
        assert [e["id"] for e in entries] == ["prog-a"]
        assert entries[0]["monthly_token_budget"] == 1000
        assert entries[0]["key"] == key
        # the fresh key authenticates /v1 (healthz requires a key now)
        r3 = client.get("/healthz", headers={"Authorization": f"Bearer {key}"})
        assert r3.status_code == 200
        # revoke → immediate 401
        r4 = client.post("/admin/keys", headers=h,
                         json={"id": "prog-a", "delete": True})
        assert r4.status_code == 200
        r5 = client.get("/healthz", headers={"Authorization": f"Bearer {key}"})
        assert r5.status_code == 401
    # persisted through the config file
    fresh = gw_config.GatewayConfig(cfgfile)
    assert fresh.api_keys == {}


def test_admin_keys_requires_admin_key_first(wired, gguf):
    """No gateway_keys → creating a virtual key is refused: it would
    enable auth and lock the admin surface out."""
    wired(_secured_config(gguf, gateway_keys=[]))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/keys", json={"id": "x"})
    assert r.status_code == 400
    assert "admin" in r.json()["error"]["message"].lower()


def test_key_token_budget_429(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "prog-b", "key": "vk-b", "monthly_token_budget": 10}]))
    _seed_key_log("prog-b", 12)  # already over
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", headers={
            "Authorization": "Bearer vk-b"}, json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "key budget test"}]})
        assert r.status_code == 429
        body = r.json()["error"]
        assert body["code"] == "key_budget_exceeded"
        assert "12/10" in body["message"]
        assert len(fake.calls) == 0  # never reached the provider


# ── pricing ────────────────────────────────────────────────────────────────

def test_extract_price_cheapest_live_provider():
    model = {"id": "m", "providers": [
        {"provider": "a", "status": "live",
         "pricing": {"input": 0.42, "output": 3}},
        {"provider": "b", "status": "live",
         "pricing": {"input": 0.1, "output": 0.5}},
        {"provider": "dead", "status": "error",
         "pricing": {"input": 0.01, "output": 0.01}},
    ]}
    p = gw_app._extract_price(model)
    assert p is not None
    assert (p["in"], p["out"]) == (0.1, 0.5)  # cheapest LIVE provider
    assert set(p["providers"]) == {"a", "b"}   # per-provider map kept
    assert gw_app._extract_price({"id": "m"}) is None


def test_cost_usd_suffix_pinning():
    from types import SimpleNamespace
    cfg = SimpleNamespace(model_prices={
        "hf": {"org/m": {"in": 1.0, "out": 2.0,
                         "providers": {"groq": {"in": 0.1, "out": 0.2}}}}})
    usage = {"prompt_tokens": 1_000_000, "completion_tokens": 1_000_000}
    assert gw_app._cost_usd(cfg, "hf", "org/m", usage) == 3.0  # cheapest
    assert gw_app._cost_usd(cfg, "hf", "org/m:groq", usage) == 0.3  # pinned
    assert gw_app._cost_usd(cfg, "hf", "unknown", usage) is None


def test_cost_tracking_end_to_end(wired, gguf, monkeypatch):
    """Priced request → cost_usd in the response, the log, and stats."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _secured_config(gguf, api_keys=[
        {"id": "p", "key": "vk-p", "monthly_usd_budget": 1.0}])
    cfgd["model_prices"] = {"groq": {
        "llama-3.1-8b-instant": {"in": 0.5, "out": 2.0}}}
    wired(cfgd)
    _patch_cloud(monkeypatch, _FakeCloud())  # usage: 5 in / 3 out
    expected = round((5 * 0.5 + 3 * 2.0) / 1_000_000, 6)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", headers={
            "Authorization": "Bearer vk-p"}, json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "costed request"}]})
        assert r.status_code == 200
        assert r.json()["cost_usd"] == expected
        s = client.get("/admin/stats", headers={
            "Authorization": "Bearer admin-secret"}).json()
        row = next(x for x in s["per_model"]
                   if x["model"] == "groq:cloud-fast")
        assert row["cost_usd"] == expected
        krow = next(x for x in s["per_key"] if x["key"] == "p")
        assert krow["cost_usd"] == expected
    lines = [json.loads(ln) for ln in
             (gw_app._month_log_path(gw_app._month_key())
              .read_text(encoding="utf-8").strip().splitlines())]
    assert lines[-1]["cost_usd"] == expected


def test_price_capture_on_catalog_fetch(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgfile, _, _ = wired(_secured_config(gguf))

    async def _fake_fetch(base_url, api_key):
        return {"data": [
            {"id": "org/m", "providers": [
                {"provider": "a", "status": "live",
                 "pricing": {"input": 0.1, "output": 0.5}}]},
            {"id": "org/n"}]}  # no pricing → not captured

    monkeypatch.setattr(gw_app, "_fetch_remote_catalog", _fake_fetch)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends/groq/remote-models", headers={
            "Authorization": "Bearer admin-secret"})
        assert r.status_code == 200
    fresh = gw_config.GatewayConfig(cfgfile)
    assert fresh.model_prices["groq"]["org/m"] == {
        "in": 0.1, "out": 0.5, "providers": {"a": {"in": 0.1, "out": 0.5}}}
    assert "org/n" not in fresh.model_prices["groq"]


# ── log rotation + rollups ─────────────────────────────────────────────────

def test_log_migration_splits_month_and_archive(wired, gguf):
    wired(_config(gguf))
    m = time.strftime("%Y-%m")
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    lines = [
        json.dumps({"ts": now, "model": "a", "backend": "local",
                    "stream": False, "ms": 1, "usage": {}, "status": 200}),
        json.dumps({"ts": now, "model": "b", "backend": "local",
                    "stream": False, "ms": 1, "usage": {}, "status": 200}),
        json.dumps({"ts": "2020-01-01T00:00:00", "model": "old",
                    "backend": "local", "stream": False, "ms": 1,
                    "usage": {}, "status": 200}),
        "not json at all",
    ]
    gw_app._LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path = gw_app._ensure_month_log()
    # current-month lines imported; old + junk archived; legacy renamed
    kept = [json.loads(l) for l in
            path.read_text(encoding="utf-8").strip().splitlines()]
    assert [e["model"] for e in kept] == ["a", "b"]
    arch = (gw_app._LOG_PATH.parent / "gateway-log-archive.jsonl"
            ).read_text(encoding="utf-8").strip().splitlines()
    assert len(arch) == 2  # old month line + junk
    assert not gw_app._LOG_PATH.exists()
    assert (gw_app._LOG_PATH.parent
            / (gw_app._LOG_PATH.name + ".migrated")).exists()


def test_rollup_and_stats_history(wired, gguf):
    """A closed month's file rolls up on demand and /admin/stats merges
    it with the live month — full history without scanning archives."""
    wired(_config(gguf))
    closed = "2020-05"
    closed_lines = [
        json.dumps({"ts": closed + "-01T00:00:00", "model": "hist",
                    "backend": "groq", "stream": False, "ms": 10,
                    "usage": {"total_tokens": 7}, "status": 200,
                    "cost_usd": 0.001, "key": "old-key"}),
        json.dumps({"ts": closed + "-02T00:00:00", "model": "hist",
                    "backend": "groq", "stream": False, "ms": 20,
                    "usage": {"total_tokens": 3}, "status": 200,
                    "key": "old-key"}),
    ]
    (gw_app._LOG_PATH.parent / f"gateway-log-{closed}.jsonl").write_text(
        "\n".join(closed_lines) + "\n", encoding="utf-8")
    # one live current-month line
    gw_app._LOG_PATH.write_text(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "model": "live",
        "backend": "local", "stream": False, "ms": 5,
        "usage": {"total_tokens": 1}, "status": 200}) + "\n",
        encoding="utf-8")
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/stats")
        assert r.status_code == 200
        s = r.json()
    assert s["total"] == 3  # 2 rolled-up + 1 live
    hist = next(x for x in s["per_model"] if x["model"] == "hist")
    assert hist["n"] == 2 and hist["total_tokens"] == 10
    assert hist["cost_usd"] == 0.001
    krow = next(x for x in s["per_key"] if x["key"] == "old-key")
    assert krow["n"] == 2
    # the rollup file now exists (closed month aggregated)
    assert (gw_app._LOG_PATH.parent
            / f"gateway-rollup-{closed}.json").is_file()


# ── tier 2: governance, scheduled verify, images, audit, retry ─────────────

def test_key_model_allowlist(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _secured_config(gguf, api_keys=[
        {"id": "scoped", "key": "vk-scoped",
         "models": ["groq:cloud-fast"]},      # one model only
        {"id": "be", "key": "vk-be", "models": ["groq"]},  # whole backend
        {"id": "openkey", "key": "vk-open"}])  # no allowlist = all
    cfgd["backends"]["groq"]["models"]["cloud-slow"] = "llama-3.1-8b"
    wired(cfgd)
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        def _chat(key, model, prompt):
            return client.post("/v1/chat/completions", headers={
                "Authorization": f"Bearer {key}"}, json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}]})
        # scoped: allowed model OK, the other 403
        assert _chat("vk-scoped", "groq:cloud-fast", "allowlist allowed").status_code == 200
        r = _chat("vk-scoped", "groq:cloud-slow", "allowlist denied")
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "model_not_allowed"
        # whole-backend allowlist covers both models
        assert _chat("vk-be", "groq:cloud-slow", "backend allowlist").status_code == 200
        # no allowlist = everything; admin = everything
        assert _chat("vk-open", "groq:cloud-slow", "no allowlist").status_code == 200
        assert _chat("admin-secret", "groq:cloud-slow",
                     "admin unrestricted").status_code == 200
        # the DENIED request never reached the provider
        assert len(fake.calls) == 4


def test_key_rate_limit_rpm(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "bursty", "key": "vk-burst", "rate_limit_rpm": 2}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        def _hit(i):
            return client.post("/v1/chat/completions", headers={
                "Authorization": "Bearer vk-burst"}, json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": f"rpm test {i}"}]})
        assert _hit(1).status_code == 200
        assert _hit(2).status_code == 200
        r3 = _hit(3)
        assert r3.status_code == 429
        assert r3.json()["error"]["code"] == "rate_limited"
        assert "retry-after" in {k.lower() for k in r3.headers}
        # admin keys are not rate limited
        r4 = client.post("/v1/chat/completions", headers={
            "Authorization": "Bearer admin-secret"}, json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "admin no limit"}]})
        assert r4.status_code == 200
        # window slides: expire the recorded timestamps → allowed again
        win = gw_app._RATE_WINDOWS["bursty"]
        for i in range(len(win)):
            win[i] -= 61.0
        assert _hit(4).status_code == 200


def test_verify_scheduler_alerts_on_transitions(wired, gguf):
    from types import SimpleNamespace
    cfg = SimpleNamespace(verify_schedule={
        "failures_before_alert": 2, "webhook_url": ""})
    ring = gw_app._event_ring()
    ring.events.clear()
    fail = [{"backend": "groq", "model": "cloud-fast", "pass": False,
             "detail": "network: boom"}]
    ok = [{"backend": "groq", "model": "cloud-fast", "pass": True,
           "detail": "answered: 'x'"}]
    skipped = [{"backend": "rp", "model": "pod", "pass": False,
                "detail": "skipped (pod — deploy on demand)"}]
    # first failure: below threshold, no alert
    asyncio.run(gw_app._process_verify_results(cfg, fail))
    assert gw_app._VERIFY_STATE["groq:cloud-fast"]["fails"] == 1
    assert not any("failing" in str(e.get("message")) for e in ring.events)
    # second failure: transition → alert
    asyncio.run(gw_app._process_verify_results(cfg, fail))
    assert gw_app._VERIFY_STATE["groq:cloud-fast"] == {
        "fails": 2, "alerting": True}
    assert any("failing 2x" in str(e.get("message")) for e in ring.events)
    # third failure: no duplicate alert
    ring.events.clear()
    asyncio.run(gw_app._process_verify_results(cfg, fail))
    assert not any("failing" in str(e.get("message")) for e in ring.events)
    # recovery: transition back → recovered notice + reset
    asyncio.run(gw_app._process_verify_results(cfg, ok))
    assert gw_app._VERIFY_STATE["groq:cloud-fast"] == {
        "fails": 0, "alerting": False}
    assert any("RECOVERED" in str(e.get("message")) for e in ring.events)
    # skipped rows are not failures
    ring.events.clear()
    asyncio.run(gw_app._process_verify_results(cfg, skipped))
    assert "rp:pod" not in gw_app._VERIFY_STATE
    assert not ring.events


def test_verify_status_endpoint(wired, gguf):
    wired(_secured_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/verify", headers={
            "Authorization": "Bearer admin-secret"})
    assert r.status_code == 200
    d = r.json()
    assert d["schedule"]["enabled"] is False  # default off
    assert "state" in d and "last_run" in d


def test_images_passthrough(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _secured_config(gguf, api_keys=[
        {"id": "img", "key": "vk-img", "models": ["groq:cloud-fast"]}])
    cfgd["backends"]["groq"]["models"]["cloud-slow"] = "llama-3.1-8b"
    wired(cfgd)
    seen = {}

    async def _fake_img(base_url, api_key, payload):
        seen.update(payload)
        return {"data": [{"url": "https://x/img.png"}]}

    monkeypatch.setattr(gw_app, "_passthrough_images", _fake_img)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/images/generations", headers={
            "Authorization": "Bearer vk-img"}, json={
            "model": "groq:cloud-fast", "prompt": "a cat", "n": 1})
        assert r.status_code == 200, r.text
        assert r.json()["data"][0]["url"].endswith("img.png")
        assert seen["model"] == "llama-3.1-8b-instant"  # wire id mapping
        assert seen["prompt"] == "a cat"
        # non-openai backend → unsupported
        r2 = client.post("/v1/images/generations", headers={
            "Authorization": "Bearer vk-img"}, json={
            "model": "fast", "prompt": "a cat"})
        assert r2.status_code == 400
        assert r2.json()["error"]["code"] == "images_unsupported"
        # allowlist denies other models
        r3 = client.post("/v1/images/generations", headers={
            "Authorization": "Bearer vk-img"}, json={
            "model": "groq:cloud-slow", "prompt": "a cat"})
        assert r3.status_code == 403


def test_config_audit_trail(wired, gguf):
    wired(_secured_config(gguf))
    with TestClient(gw_app.app) as client:
        h = {"Authorization": "Bearer admin-secret"}
        client.post("/admin/keys", headers=h, json={"id": "audited"})
        client.post("/admin/models", headers=h, json={
            "name": "m1", "backend": "groq", "remote_id": "org/m1"})
        r = client.get("/admin/audit", headers=h)
        assert r.status_code == 200
        actions = [e["action"] for e in r.json()["audit"]]
        assert actions[0] == "model-upsert groq:m1"  # newest first
        assert actions[1] == "api-key-create audited"
        # every entry records WHO (the admin key)
        assert all(e["key"] == "admin" for e in r.json()["audit"])


def test_network_retry_once(wired, gguf, monkeypatch):
    from openai import APIConnectionError
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf))

    calls: list[int] = []

    async def _flaky(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise APIConnectionError(request=None)  # never reached provider
        return _FakeResp(_cloud_nonstream("retried"))

    fake = _FakeCloud()
    fake.chat.completions.create = _flaky
    _patch_cloud(monkeypatch, fake)
    _h = {"Authorization": "Bearer admin-secret"}
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", headers=_h, json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "retry once test"}]})
        assert r.status_code == 200, r.text
        assert r.json()["choices"][0]["message"]["content"] == \
            "cloud[retried] answer"
        assert len(calls) == 2  # exactly one retry

    # persistently dead: still exactly one retry, then a classified 502
    calls2: list[int] = []

    async def _dead(**kwargs):
        calls2.append(1)
        raise APIConnectionError(request=None)

    fake2 = _FakeCloud()
    fake2.chat.completions.create = _dead
    _patch_cloud(monkeypatch, fake2)
    with TestClient(gw_app.app) as client:
        r2 = client.post("/v1/chat/completions", headers=_h, json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "dead upstream"}]})
        assert r2.status_code == 502
        assert r2.json()["error"]["code"] == "network"
        assert len(calls2) == 2  # one retry, never more


def test_verify_schedule_config_parse():
    raw = {"verify_schedule": {
        "enabled": True, "interval_min": 15, "backends": ["hf"],
        "timeout_s": 45, "failures_before_alert": 3,
        "webhook_url": "https://hooks.example/gw"}}
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "gw.json"
        p.write_text(json.dumps(raw), encoding="utf-8")
        cfg = gw_config.GatewayConfig(p)
    assert cfg.verify_schedule == {
        "enabled": True, "interval_min": 15, "backends": ["hf"],
        "timeout_s": 45.0, "failures_before_alert": 3,
        "webhook_url": "https://hooks.example/gw"}
