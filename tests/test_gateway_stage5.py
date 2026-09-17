"""LLM Gateway Stage 5 — console page, config editor, stats, events.

The console shell serves without a key (its API calls need one when
configured); /admin/config GET/PUT round-trips raw text with validation;
/admin/stats aggregates the JSONL request log per model; /admin/events
returns the in-memory ring (an engine load shows up there).
"""
import json
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402


@contextmanager
def _fake_llama():
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})

        def create_chat_completion(self, **kwargs):
            return {"choices": [{"message": {"content": "local answer"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        yield records
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)


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
        monkeypatch.setattr(gw_app, "_active_chats", 0)
        return cfgfile, cfg, engine
    return _wire


def _config(gguf, *, keys=None):
    cfg = {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {"local": {"type": "embedded", "models": {
            "fast": {"gguf": str(gguf), "ctx": 1024, "placement": ""}}}},
        "aliases": {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
    }
    if keys is not None:
        cfg["gateway_keys"] = keys
    return cfg


# ── console page ───────────────────────────────────────────────────────────

def test_console_served(wired, gguf):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    # the v2 admin console: 8 panels behind the category grid
    for panel in ("models", "services", "perf", "syslog",
                  "params", "keys", "health", "aliases"):
        assert f"showPanel('{panel}')" in r.text
    assert "admin/backends" in r.text and "admin/secrets" in r.text


def test_console_shell_loads_without_key(wired, gguf):
    """With keys configured the static shell still loads (it prompts for
    the key); the DATA endpoints stay protected."""
    wired(_config(gguf, keys=["sekrit"]))
    with TestClient(gw_app.app) as client:
        assert client.get("/").status_code == 200
        assert client.get("/healthz").status_code == 401
        assert client.get("/admin/stats").status_code == 401


# ── config editor ──────────────────────────────────────────────────────────

def test_admin_config_get_and_put_roundtrip(wired, gguf):
    cfgfile, cfg, _ = wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/config")
        assert r.status_code == 200
        body = r.json()
        assert body["path"].endswith("gateway_config.json")
        assert "fast" in body["text"]

        # edit: add an alias via the raw editor
        raw = json.loads(body["text"])
        raw["aliases"]["smol"] = {"chain": ["local:fast"], "hedge_s": 1}
        r2 = client.put("/admin/config",
                        json={"text": json.dumps(raw, indent=2)})
        assert r2.status_code == 200, r2.text
        assert r2.json()["ok"] is True
        assert "smol" in r2.json()["aliases"]
        # persisted + applied
        on_disk = json.loads(cfgfile.read_text(encoding="utf-8"))
        assert "smol" in on_disk["aliases"]
        assert "smol" in client.get("/admin/aliases").json()["aliases"]


def test_admin_config_put_invalid_json(wired, gguf):
    cfgfile, cfg, _ = wired(_config(gguf))
    before = cfgfile.read_text(encoding="utf-8")
    with TestClient(gw_app.app) as client:
        r = client.put("/admin/config", json={"text": "{ not json"})
        assert r.status_code == 400
        assert "invalid JSON" in r.json()["error"]["message"]
        r2 = client.put("/admin/config", json={"text": "[1, 2]"})
        assert r2.status_code == 400  # must be an object
        # file untouched
        assert cfgfile.read_text(encoding="utf-8") == before


# ── stats ──────────────────────────────────────────────────────────────────

def test_admin_stats_aggregates(wired, gguf):
    _, cfg, _ = wired(_config(gguf))
    lines = [
        {"ts": "t1", "model": "fast", "backend": "local", "stream": False,
         "ms": 100, "usage": {"total_tokens": 10}, "status": 200},
        {"ts": "t2", "model": "fast", "backend": "local", "stream": True,
         "ms": 300, "usage": {}, "status": 200},
        {"ts": "t3", "model": "fast", "backend": "local", "stream": False,
         "ms": 50, "usage": {"total_tokens": 5}, "status": 429},
        {"ts": "t4", "model": "groq:cloud", "backend": "groq", "stream": False,
         "ms": 700, "usage": {"total_tokens": 99}, "status": 200},
        "not json at all",
    ]
    gw_app._LOG_PATH.write_text("\n".join(json.dumps(l) for l in lines[:4]) +
                                "\n" + lines[4] + "\n", encoding="utf-8")
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/stats")
    assert r.status_code == 200
    s = r.json()
    assert s["total"] == 4  # junk line skipped
    fast = next(x for x in s["per_model"] if x["model"] == "fast")
    assert fast["n"] == 3
    assert fast["avg_ms"] == 150  # (100+300+50)/3
    assert fast["max_ms"] == 300
    assert fast["total_tokens"] == 15
    assert fast["errors"] == 1
    cloud = next(x for x in s["per_model"] if x["model"] == "groq:cloud")
    assert cloud["backend"] == "groq" and cloud["n"] == 1
    assert len(s["recent"]) == 4


def test_admin_stats_empty_log(wired, gguf):
    wired(_config(gguf))  # no log file at all
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/stats")
    assert r.status_code == 200
    assert r.json() == {"per_model": [], "recent": [], "total": 0}


# ── events ─────────────────────────────────────────────────────────────────

def test_admin_events_captures_engine_load(wired, gguf):
    _, cfg, _ = wired(_config(gguf))
    ring = gw_app._event_ring()
    ring.events.clear()
    with TestClient(gw_app.app) as client:
        with _fake_llama():
            r = client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200
            ev = client.get("/admin/events").json()["events"]
    assert any("Embedded LLM loaded" in e["message"] for e in ev)
    assert all(set(e) >= {"ts", "level", "logger", "message"} for e in ev)
