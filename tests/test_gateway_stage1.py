"""LLM Gateway Stage 1 — skeleton, config, /v1/models, non-streaming chat.

Covers: config defaults + file overrides + hot reload + broken-JSON
resilience; model resolution (alias primary, 'backend:model' direct form,
bare backend model, unknown → 400); auto-discovery (any GGUF in
models_dir servable by stem — config models shadow stems); the embedded
engine (instance registry keyed by gateway config, reload-on-config-change,
placement passthrough, GGUF resolution); the HTTP surface via TestClient
with the FakeLlama sys.modules trick.
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
from gateway.models import EmbeddedLLMError  # noqa: E402


# ── FakeLlama (sys.modules injection) ──────────────────────────────────────

def _with_fake_llama(fn, script=None, stream_script=None):
    """Run fn with a fake llama_cpp module; records Llama(**kwargs) loads and
    every create_chat_completion(**kwargs) call. `stream_script` = content
    pieces yielded as stream chunks (default ["Hello", " world"])."""
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


def _resp(content, pt=3, ct=2):
    return {"choices": [{"message": {"content": content}}],
            "usage": {"prompt_tokens": pt, "completion_tokens": ct,
                      "total_tokens": pt + ct}}


# ── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture()
def gguf(tmp_path):
    p = tmp_path / "fake.gguf"
    p.write_bytes(b"x")
    return p


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """Install a GatewayConfig + fresh engine singleton backed by a config
    file in tmp_path. Returns a setter for later hot-reloads."""
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


def _local_config(gguf, *, extra_backends=None, aliases=None, default="fast"):
    backends = {
        "local": {"type": "embedded", "models": {
            "fast": {"gguf": str(gguf), "ctx": 1024, "placement": ""},
            "smart": {"gguf": str(gguf), "ctx": 2048, "placement": ""},
        }},
    }
    if extra_backends:
        backends.update(extra_backends)
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": default,
        # Point models_dir at an empty/nonexistent tmp dir so tests never
        # see the REAL data/models GGUFs (auto-discovery would list them).
        "models_dir": str(gguf.parent / "models"),
        "backends": backends,
        "aliases": aliases or {
            "fast": {"chain": ["local:fast"], "hedge_s": 0},
            "smart": {"chain": ["local:smart"], "hedge_s": 0},
        },
    }


GROQ = {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}


# ── config ─────────────────────────────────────────────────────────────────

def test_config_defaults_when_file_missing(tmp_path):
    cfg = gw_config.GatewayConfig(tmp_path / "nope.json")
    assert cfg.port == 8010
    assert cfg.bind == "127.0.0.1"
    assert cfg.default_alias == ""
    assert cfg.local_models() == {}  # deliberately model-free defaults
    assert cfg.aliases == {}
    assert cfg.queue["max_waiting"] == 4
    assert cfg.gateway_keys == []
    # models_dir anchors at the project root
    assert Path(cfg.models_dir).is_absolute()
    assert cfg.models_dir.endswith("data\\models") or cfg.models_dir.endswith("data/models")


def test_config_file_overrides_top_level(tmp_path):
    f = tmp_path / "gateway_config.json"
    f.write_text(json.dumps({
        "port": 9912,
        "default_alias": "fast",
        "backends": {"local": {"type": "embedded", "models": {
            "only": {"gguf": "x", "ctx": 512}}}},
    }), encoding="utf-8")
    cfg = gw_config.GatewayConfig(f)
    assert cfg.port == 9912
    assert cfg.default_alias == "fast"
    # defining backends replaces the defaults wholesale
    assert set(cfg.local_models()) == {"only"}
    assert cfg.local_models()["only"]["ctx"] == 512
    # missing keys keep defaults
    assert cfg.bind == "127.0.0.1"


def test_config_models_dir_resolution(tmp_path):
    f = tmp_path / "gateway_config.json"
    f.write_text(json.dumps({"models_dir": str(tmp_path / "m")}), encoding="utf-8")
    cfg = gw_config.GatewayConfig(f)
    assert cfg.models_dir == str(tmp_path / "m")
    # relative anchors at the project root
    f.write_text(json.dumps({"models_dir": "ggufs"}), encoding="utf-8")
    cfg2 = gw_config.GatewayConfig(f)
    assert cfg2.models_dir == str(gw_config.PROJECT_ROOT / "ggufs")


def test_config_hot_reload_on_mtime_change(tmp_path):
    f = tmp_path / "gateway_config.json"
    f.write_text(json.dumps({"port": 1}), encoding="utf-8")
    cfg = gw_config.GatewayConfig(f)
    assert cfg.port == 1
    f.write_text(json.dumps({"port": 2}), encoding="utf-8")
    # defeat filesystem mtime granularity
    import os
    os.utime(f, ns=(0, int(cfg._mtime * 1e9) + 1_000_000_000))
    assert cfg.maybe_reload() is True
    assert cfg.port == 2
    assert cfg.maybe_reload() is False  # unchanged → no reload


def test_config_broken_json_keeps_previous(tmp_path):
    f = tmp_path / "gateway_config.json"
    f.write_text(json.dumps({"port": 5}), encoding="utf-8")
    cfg = gw_config.GatewayConfig(f)
    f.write_text("{ broken", encoding="utf-8")
    import os
    os.utime(f, ns=(0, int(cfg._mtime * 1e9) + 1_000_000_000))
    cfg.maybe_reload()
    assert cfg.port == 5  # previous values kept
    # fixing the file reloads cleanly
    f.write_text(json.dumps({"port": 6}), encoding="utf-8")
    os.utime(f, ns=(0, int(cfg._mtime * 1e9) + 1_000_000_000))
    assert cfg.maybe_reload() is True
    assert cfg.port == 6


def test_config_drops_malformed_entries(tmp_path):
    f = tmp_path / "gateway_config.json"
    f.write_text(json.dumps({
        "backends": {
            "local": {"type": "embedded", "models": {
                "good": {"gguf": "g"},
                "nologguf": {"ctx": 8},
            }},
            "bogus": {"type": "wat"},
        },
        "aliases": {"ok": {"chain": ["local:good"]}, "bad": 3},
    }), encoding="utf-8")
    cfg = gw_config.GatewayConfig(f)
    assert set(cfg.local_models()) == {"good"}
    assert "bogus" not in cfg.backends
    assert set(cfg.aliases) == {"ok"}


# ── resolution ─────────────────────────────────────────────────────────────

def test_resolve_alias_takes_primary_entry(wired, gguf):
    _, cfg, _ = wired(_local_config(
        gguf, extra_backends={"groq": GROQ},
        aliases={"smart": {"chain": ["local:smart", "groq:cloud-fast"],
                           "hedge_s": 8}}))
    assert cfg.resolve_model("smart") == ("local", "smart")


def test_resolve_direct_backend_model_form(wired, gguf):
    _, cfg, _ = wired(_local_config(gguf, extra_backends={"groq": GROQ}))
    assert cfg.resolve_model("local:fast") == ("local", "fast")
    assert cfg.resolve_model("groq:cloud-fast") == ("groq", "cloud-fast")


def test_resolve_unknown_model_errors(wired, gguf):
    _, cfg, _ = wired(_local_config(gguf, extra_backends={"groq": GROQ}))
    for bad in ("nope", "local:nope", "nobackend:x", "local:"):
        with pytest.raises(gw_config.GatewayConfigError):
            cfg.resolve_model(bad)


def test_resolve_alias_precedence_over_backend_model(wired, gguf):
    # alias 'smart' points elsewhere while a backend model 'smart' exists
    _, cfg, _ = wired(_local_config(
        gguf, aliases={"smart": {"chain": ["local:fast"]}}))
    assert cfg.resolve_model("smart") == ("local", "fast")


def test_resolve_empty_model_uses_default_alias(wired, gguf):
    _, cfg, _ = wired(_local_config(gguf, default="smart"))
    assert cfg.resolve_model("") == ("local", "smart")


def test_resolve_no_default_alias_configured(wired, gguf):
    cfgd = _local_config(gguf)
    cfgd["default_alias"] = ""
    _, cfg, _ = wired(cfgd)
    with pytest.raises(gw_config.GatewayConfigError):
        cfg.resolve_model("")


# ── engine ─────────────────────────────────────────────────────────────────

def test_engine_registry_and_reload_on_config_change(wired, gguf):
    cfgfile, cfg, engine = wired(_local_config(gguf))

    def fn(records):
        inst = engine._get_instance("fast")
        assert inst.name == "fast"
        load = [r["load"] for r in records if "load" in r]
        assert len(load) == 1
        assert load[0]["n_ctx"] == 1024
        assert load[0]["n_gpu_layers"] == -1

        # hot-reload ctx for 'fast' → instance reloads
        cfgd = _local_config(gguf)
        cfgd["backends"]["local"]["models"]["fast"]["ctx"] = 512
        cfgfile.write_text(json.dumps(cfgd), encoding="utf-8")
        import os
        os.utime(cfgfile, ns=(0, int(cfg._mtime * 1e9) + 1_000_000_000))
        cfg.maybe_reload()
        inst2 = engine._get_instance("fast")
        assert inst2 is not inst
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 2
        assert loads[1]["n_ctx"] == 512

    _with_fake_llama(fn)


def test_engine_unknown_local_model_raises(wired, gguf):
    _, cfg, engine = wired(_local_config(gguf))
    with pytest.raises(EmbeddedLLMError):
        engine._get_instance("nope")


def test_engine_unresolvable_gguf_raises(wired, gguf):
    cfgd = _local_config(gguf)
    cfgd["backends"]["local"]["models"]["fast"]["gguf"] = "missing-model"
    _, cfg, engine = wired(cfgd)
    with pytest.raises(EmbeddedLLMError):
        engine._get_instance("fast")


def test_engine_placement_passthrough(wired, gguf):
    cfgd = _local_config(gguf)
    cfgd["backends"]["local"]["models"]["fast"]["placement"] = "0"
    _, cfg, engine = wired(cfgd)

    def fn(records):
        engine._get_instance("fast")
        load = [r["load"] for r in records if "load" in r][0]
        assert load.get("main_gpu") == 0
        assert load.get("split_mode") == 0  # SPLIT_MODE_NONE

    _with_fake_llama(fn)


def test_engine_unload_and_loaded_models(wired, gguf):
    _, cfg, engine = wired(_local_config(gguf))

    def fn(records):
        engine._get_instance("fast")
        engine._get_instance("smart")
        assert len(engine.loaded_models()) == 2
        assert {m["alias"] for m in engine.loaded_models()} == {"fast", "smart"}
        assert engine.loaded_models()[0]["role"] == "chat"
        assert engine.unload("fast") == 1
        assert {m["alias"] for m in engine.loaded_models()} == {"smart"}
        assert engine.is_loaded()
        assert engine.unload("all") == 1
        assert not engine.is_loaded()

    _with_fake_llama(fn)


def test_resolve_gguf_from_models_dir(tmp_path):
    models = tmp_path / "models"
    models.mkdir()
    (models / "tiny-model-q4_k_m.gguf").write_bytes(b"x")
    # stem match
    p = gw_models.EmbeddedEngine.resolve_gguf("tiny-model-q4_k_m", models)
    assert p == str(models / "tiny-model-q4_k_m.gguf")
    # bare filename with extension
    p2 = gw_models.EmbeddedEngine.resolve_gguf("tiny-model-q4_k_m.gguf", models)
    assert p2 == p
    # nothing else matches
    assert gw_models.EmbeddedEngine.resolve_gguf("other", models) == ""
    assert gw_models.EmbeddedEngine.resolve_gguf("", models) == ""


def test_resolve_gguf_direct_path(gguf):
    assert gw_models.EmbeddedEngine.resolve_gguf(
        str(gguf), "Z:/nowhere") == str(gguf)


def test_engine_auto_discovery(tmp_path):
    """Any GGUF in models_dir is servable by stem with default ctx —
    no config entry needed."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "tiny-d.gguf").write_bytes(b"x")
    cfgfile = tmp_path / "gateway_config.json"
    cfgfile.write_text(json.dumps({"models_dir": str(models_dir)}),
                       encoding="utf-8")
    cfg = gw_config.GatewayConfig(cfgfile)
    engine = gw_models.EmbeddedEngine(cfg)

    def fn(records):
        inst = engine._get_instance("tiny-d")
        assert inst.name == "tiny-d"
        load = [r["load"] for r in records if "load" in r][0]
        assert load["n_ctx"] == 4096  # discovery default
        assert load["n_gpu_layers"] == -1

    _with_fake_llama(fn)


def test_engine_discovery_unknown_raises(tmp_path):
    cfgfile = tmp_path / "gateway_config.json"
    cfgfile.write_text(json.dumps(
        {"models_dir": str(tmp_path / "empty")}), encoding="utf-8")
    cfg = gw_config.GatewayConfig(cfgfile)
    engine = gw_models.EmbeddedEngine(cfg)
    with pytest.raises(EmbeddedLLMError):
        engine._get_instance("nope")


# ── HTTP surface ───────────────────────────────────────────────────────────

def test_models_endpoint_union(wired, gguf):
    wired(_local_config(gguf, extra_backends={"groq": GROQ},
                        aliases={"smart": {"chain": ["local:smart", "groq:cloud-fast"]}}))
    with TestClient(gw_app.app) as client:
        r = client.get("/v1/models")
    assert r.status_code == 200
    body = r.json()
    assert body["object"] == "list"
    ids = {m["id"] for m in body["data"]}
    assert ids == {"fast", "smart", "cloud-fast"}  # aliases + backend models
    assert all(m["object"] == "model" for m in body["data"])
    owned = {m["id"]: m["owned_by"] for m in body["data"]}
    assert owned["fast"] == "local"
    assert owned["cloud-fast"] == "groq"


def _discovery_config(gguf, *, models=None):
    cfgd = _local_config(gguf)
    cfgd["backends"]["local"]["models"] = models or {}
    cfgd["aliases"] = {}
    return cfgd


def test_models_endpoint_lists_discovered_ggufs(wired, gguf):
    models_dir = gguf.parent / "models"
    models_dir.mkdir()
    (models_dir / "tiny-a.gguf").write_bytes(b"x")
    cfgd = _discovery_config(gguf)
    cfgd["models_dir"] = str(models_dir)
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        r = client.get("/v1/models")
    assert r.status_code == 200
    body = r.json()
    ids = {m["id"] for m in body["data"]}
    assert ids == {"tiny-a"}  # discovered stem, nothing configured
    assert body["data"][0]["owned_by"] == "local"


def test_chat_auto_discovered_stem(wired, gguf):
    models_dir = gguf.parent / "models"
    models_dir.mkdir()
    (models_dir / "tiny-b.gguf").write_bytes(b"x")
    cfgd = _discovery_config(gguf)
    cfgd["models_dir"] = str(models_dir)
    wired(cfgd)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "tiny-b",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["model"] == "tiny-b"
        assert body["served_by"]["model"] == "tiny-b"
        load = [r_["load"] for r_ in records if "load" in r_][0]
        assert load["n_ctx"] == 4096  # discovery default

    _with_fake_llama(fn)


def test_config_model_beats_discovered_stem(wired, gguf):
    """A config model with the same name as a discovered stem wins —
    custom ctx/placement apply instead of the discovery defaults."""
    models_dir = gguf.parent / "models"
    models_dir.mkdir()
    (models_dir / "tiny-c.gguf").write_bytes(b"x")
    cfgd = _discovery_config(gguf, models={
        "tiny-c": {"gguf": str(gguf), "ctx": 777, "placement": ""}})
    cfgd["models_dir"] = str(models_dir)
    wired(cfgd)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "tiny-c",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200
        load = [r_["load"] for r_ in records if "load" in r_][0]
        assert load["n_ctx"] == 777  # config entry, not the discovery default

    _with_fake_llama(fn)


def test_chat_basic_roundtrip(wired, gguf):
    wired(_local_config(gguf))
    script = [_resp("Hello there.")]  # no '?', no echo

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "say hi"}],
                "temperature": 0.7, "max_tokens": 33})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["object"] == "chat.completion"
        assert body["model"] == "fast"
        assert body["id"].startswith("chatcmpl-gw-")
        assert body["choices"][0]["message"]["content"] == "Hello there."
        assert body["choices"][0]["finish_reason"] == "stop"
        assert body["usage"] == {"prompt_tokens": 3, "completion_tokens": 2,
                                 "total_tokens": 5}
        assert body["served_by"]["backend"] == "local"
        # kwargs reached llama.cpp
        call = [r_["call"] for r_ in records if "call" in r_][-1]
        assert call["temperature"] == 0.7
        assert call["max_tokens"] == 33

    _with_fake_llama(fn, script)


def test_chat_missing_model_uses_default_alias(wired, gguf):
    wired(_local_config(gguf, default="fast"))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200, r.text
        assert r.json()["model"] == "fast"

    _with_fake_llama(fn)


def test_chat_think_tags_stripped(wired, gguf):
    wired(_local_config(gguf))
    script = [_resp("<think>the user greeted me</think>Hello there.")]

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "say hi"}]})
        assert r.status_code == 200
        assert r.json()["choices"][0]["message"]["content"] == "Hello there."

    _with_fake_llama(fn, script)


def test_chat_tool_call_openai_shape(wired, gguf):
    wired(_local_config(gguf))
    script = [_resp('{"tool": "get_weather", "arguments": {"city": "Winnipeg"}}')]
    tools = [{"type": "function", "function": {
        "name": "get_weather", "description": "Get the weather",
        "parameters": {"type": "object",
                       "properties": {"city": {"type": "string"}}}}}]

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "weather in Winnipeg?"}],
                "tools": tools})
        assert r.status_code == 200, r.text
        body = r.json()
        msg = body["choices"][0]["message"]
        assert body["choices"][0]["finish_reason"] == "tool_calls"
        assert msg["content"] is None
        tc = msg["tool_calls"][0]
        assert tc["type"] == "function"
        assert tc["function"]["name"] == "get_weather"
        assert json.loads(tc["function"]["arguments"]) == {"city": "Winnipeg"}
        # tools were folded into a single LEADING system message for the model
        call = [r_["call"] for r_ in records if "call" in r_][0]
        msgs = call["messages"]
        assert msgs[0]["role"] == "system"
        assert "get_weather" in msgs[0]["content"]
        assert all(m["role"] != "system" for m in msgs[1:])

    _with_fake_llama(fn, script)


def test_chat_unknown_model_400_openai_shaped(wired, gguf):
    wired(_local_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "nope",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["type"] == "invalid_request_error"
    assert err["param"] == "model"
    assert "unknown model" in err["message"]


def test_chat_stream_now_implemented(wired, gguf):
    """Stage 2 wired SSE — stream:true now succeeds (see test_gateway_stage2
    for the full format assertions). Smoke check only."""
    wired(_local_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            with client.stream("POST", "/v1/chat/completions", json={
                "model": "fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
                assert r.status_code == 200
                assert r.headers["content-type"].startswith("text/event-stream")
                lines = [ln for ln in r.iter_lines() if ln.strip()]
        assert lines[-1] == "data: [DONE]"

    _with_fake_llama(fn, stream_script=["Hi", " there"])


def test_chat_cloud_missing_api_key_502(wired, gguf, monkeypatch):
    """Stage 3 executes cloud backends — a backend whose api_key_env points
    at an unset variable fails cleanly (no network attempt)."""
    import gateway.cloud as gw_cloud
    monkeypatch.setenv("GATEWAY_TEST_KEY_DEFINITELY_UNSET", "")  # ensure empty
    monkeypatch.delenv("GATEWAY_TEST_KEY_DEFINITELY_UNSET", raising=False)
    wired(_local_config(gguf, extra_backends={
        "groq": {"type": "openai", "base_url": "http://127.0.0.1:9/v1",
                 "api_key_env": "GATEWAY_TEST_KEY_DEFINITELY_UNSET",
                 "models": {"cloud-fast": "llama-3.1-8b-instant"}}}))
    # no cloud client may even be constructed with the real constructor
    monkeypatch.setattr(gw_cloud, "_CLIENTS", {})
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "GATEWAY_TEST_KEY_DEFINITELY_UNSET" in r.json()["error"]["message"]
    assert r.json()["error"]["type"] == "server_error"


def test_chat_empty_messages_400_openai_shaped(wired, gguf):
    wired(_local_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "fast", "messages": []})
    assert r.status_code == 400
    assert "error" in r.json()
    assert r.json()["error"]["type"] == "invalid_request_error"


def test_chat_embedded_error_502(wired, gguf):
    cfgd = _local_config(gguf)
    cfgd["backends"]["local"]["models"]["fast"]["gguf"] = "not-on-disk"
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert r.json()["error"]["type"] == "server_error"
