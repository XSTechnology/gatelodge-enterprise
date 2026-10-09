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
                 gw_app._LOG_PATH.read_text(encoding="utf-8").strip().splitlines()]
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
                 gw_app._LOG_PATH.read_text(encoding="utf-8").strip().splitlines()]
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
