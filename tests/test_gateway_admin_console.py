"""LLM Gateway — admin console support endpoints (v2 admin rebuild).

/admin/backends: structured service definitions for every backend type
(embedded, llama-server subprocess + remote, runpod, openai cloud) with
secrets reduced to env-var NAMES, plus queue/bind/models_dir overview.
/admin/models: discovered GGUF inventory + loaded instances.
/admin/secrets: api_key_env presence (env / keyring / missing), never values.
/admin/llama-server/stop + /admin/runpod/stop: recovery actions against
managed children/pods. /admin/reload: force config reload.
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
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


# ── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture()
def gguf(tmp_path):
    p = tmp_path / "fake.gguf"
    p.write_bytes(b"x" * 1024)
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
        monkeypatch.setattr(lsrv, "PROJECT_ROOT", tmp_path)
        lsrv.reset_managers()
        rp.reset_managers()
        return cfgfile, cfg, engine

    yield _wire
    lsrv.reset_managers()
    rp.reset_managers()


def _full_config(gguf, models_dir=None):
    """Every backend type the console must be able to describe."""
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(models_dir or gguf.parent / "models"),
        "backends": {
            "local": {
                "type": "embedded",
                "models": {
                    "fast": {"gguf": str(gguf), "ctx": 1024, "placement": "",
                             "workers": 2},
                    "slow": {"gguf": str(gguf), "ctx": 512, "placement": ""},
                },
                "embedding_models": {
                    "embed": {"gguf": str(gguf), "ctx": 512, "placement": ""},
                },
            },
            "srv": {"type": "llama-server", "server_exe": "llama-server",
                    "port_base": 8120, "models": {
                        "batch": {"gguf": str(gguf), "ctx": 8192,
                                  "parallel": 4}}},
            "far": {"type": "llama-server",
                    "base_url": "http://10.0.0.5:8080", "api_key_env": "",
                    "models": {"big": "some-remote-model"}},
            "rp": {"type": "runpod", "api_key_env": "RUNPOD_API_KEY",
                   "models": {
                       "smollm": {"endpoint_id": "ep1", "remote_id": "smol",
                                  "transport": "runsync", "timeout_s": 420}}},
            "groq": {"type": "openai", "api_key_env": "GROQ_API_KEY",
                     "base_url": "https://api.groq.com/openai/v1",
                     "models": {"lj": "llama-3.1-8b-instant"}},
        },
        "aliases": {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
        "queue": {"max_waiting": 4, "timeout_s": 120},
    }


# ── /admin/backends: service definitions ───────────────────────────────────

def test_admin_backends_describes_every_type(wired, gguf):
    wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends")
        assert r.status_code == 200, r.text
        body = r.json()
        b = {e["name"]: e for e in body["backends"]}

        assert b["local"]["type"] == "embedded"
        assert set(b["local"]["models"]) == {"fast", "slow"}
        assert set(b["local"]["embedding_models"]) == {"embed"}
        assert b["local"]["models"]["fast"]["workers"] == 2

        assert b["srv"]["mode"] == "subprocess"
        assert b["srv"]["server_exe"] == "llama-server"
        assert b["srv"]["port_base"] == 8120
        assert "batch" in b["srv"]["models"]

        assert b["far"]["mode"] == "remote"
        assert b["far"]["base_url"] == "http://10.0.0.5:8080"
        assert b["far"]["models"]["big"] == "some-remote-model"

        assert b["rp"]["type"] == "runpod"
        assert b["rp"]["api_key_env"] == "RUNPOD_API_KEY"
        assert b["rp"]["models"]["smollm"]["transport"] == "runsync"

        assert b["groq"]["type"] == "openai"
        assert b["groq"]["base_url"] == "https://api.groq.com/openai/v1"
        assert b["groq"]["api_key_env"] == "GROQ_API_KEY"

        # overview keys
        assert body["queue"]["max_waiting"] == 4
        assert body["default_alias"] == "fast"
        assert body["port"] == 9911
        assert "models_dir" in body
        assert body["gateway_keys_configured"] is False


def test_admin_backends_never_leaks_secret_values(wired, gguf, monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "gsk-super-secret-value")
    wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        body = client.get("/admin/backends").json()
    raw = json.dumps(body)
    assert "gsk-super-secret-value" not in raw
    assert "GROQ_API_KEY" in raw  # env-var NAME is fine


# ── /admin/models: inventory ───────────────────────────────────────────────

def test_admin_models_lists_discovered_and_loaded(wired, gguf, tmp_path):
    mdir = tmp_path / "models"
    mdir.mkdir()
    (mdir / "tiny-Q4_K_M.gguf").write_bytes(b"x" * 2048)
    (mdir / "other.gguf").write_bytes(b"x" * 512)
    wired(_full_config(gguf, models_dir=mdir))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/models")
        assert r.status_code == 200, r.text
        body = r.json()
    names = {d["name"] for d in body["discovered"]}
    assert names == {"tiny-Q4_K_M", "other"}
    row = next(d for d in body["discovered"] if d["name"] == "tiny-Q4_K_M")
    assert row["size"] == 2048
    assert row["quantization"] == "Q4_K_M"
    assert row["path"].endswith("tiny-Q4_K_M.gguf")
    assert body["loaded"] == []  # nothing loaded in a fresh engine


def test_admin_models_empty_when_no_models_dir(wired, gguf, tmp_path):
    wired(_full_config(gguf, models_dir=tmp_path / "does-not-exist"))
    with TestClient(gw_app.app) as client:
        body = client.get("/admin/models").json()
    assert body["discovered"] == []


# ── /admin/secrets: key presence, never values ────────────────────────────

def _stub_keyring(monkeypatch, values):
    """Make keyring.get_password deterministic regardless of host state."""
    try:
        import keyring
    except ImportError:
        return  # endpoint degrades to missing; tests skip keyring source
    monkeypatch.setattr(keyring, "get_password",
                        lambda service, name: values.get(name))


def test_admin_secrets_sources(wired, gguf, monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    monkeypatch.setenv("GROQ_API_KEY", "x")  # env wins over keyring
    _stub_keyring(monkeypatch, {})
    wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/secrets")
        assert r.status_code == 200, r.text
        body = r.json()
    by_name = {k["name"]: k for k in body["api_keys"]}
    assert by_name["GROQ_API_KEY"]["source"] == "env"
    assert by_name["GROQ_API_KEY"]["backends"] == ["groq"]
    assert by_name["RUNPOD_API_KEY"]["source"] == "missing"
    assert by_name["RUNPOD_API_KEY"]["backends"] == ["rp"]
    assert body["gateway_keys_configured"] is False


def test_admin_secrets_keyring_source(wired, gguf, monkeypatch):
    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    _stub_keyring(monkeypatch, {"RUNPOD_API_KEY": "stored"})
    wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        body = client.get("/admin/secrets").json()
    by_name = {k["name"]: k for k in body["api_keys"]}
    assert by_name["RUNPOD_API_KEY"]["source"] == "keyring"


def test_admin_secrets_no_env_backends_no_rows(wired, gguf, monkeypatch):
    cfg = _full_config(gguf)
    cfg["backends"] = {"local": cfg["backends"]["local"]}  # only embedded
    wired(cfg)
    with TestClient(gw_app.app) as client:
        body = client.get("/admin/secrets").json()
    assert body["api_keys"] == []


# ── /admin/llama-server/stop: child recovery ───────────────────────────────

def test_admin_llama_server_stop_specific_child(wired, gguf):
    wired(_full_config(gguf))
    stopped = []
    lsrv._MANAGERS[("srv", "batch")] = types.SimpleNamespace(
        stop=lambda: stopped.append(("srv", "batch")))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/llama-server/stop",
                        json={"backend": "srv", "model": "batch"})
        assert r.status_code == 200, r.text
        assert r.json()["stopped"] == "srv:batch"
        # assert BEFORE the context exits — app shutdown also stops children
        assert stopped == [("srv", "batch")]


def test_admin_llama_server_stop_all(wired, gguf, monkeypatch):
    wired(_full_config(gguf))
    killed = []
    monkeypatch.setattr(lsrv, "kill_all", lambda: killed.append(True))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/llama-server/stop", json={"all": True})
        assert r.status_code == 200, r.text
        assert r.json()["stopped"] == "all"
        # assert BEFORE the context exits — app shutdown also calls kill_all
        assert killed == [True]


def test_admin_llama_server_stop_errors(wired, gguf):
    wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/llama-server/stop", json={})
        assert r.status_code == 400
        r = client.post("/admin/llama-server/stop",
                        json={"backend": "srv", "model": "nope"})
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "not_found"


# ── /admin/runpod/stop: pod recovery ───────────────────────────────────────

def test_admin_runpod_stop_managed_pod(wired, gguf):
    wired(_full_config(gguf))

    class _FakePod:
        def __init__(self):
            self.calls = []

        async def stop(self, terminate=False):
            self.calls.append(terminate)

    pod = _FakePod()
    rp._MANAGERS[("rp", "smollm")] = pod
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/stop",
                        json={"backend": "rp", "model": "smollm"})
        assert r.status_code == 200, r.text
        assert r.json() == {"stopped": "rp:smollm", "terminated": False}
        r = client.post("/admin/runpod/stop",
                        json={"backend": "rp", "model": "smollm",
                              "terminate": True})
        assert r.status_code == 200
        assert r.json()["terminated"] is True
    assert pod.calls == [False, True]


def test_admin_runpod_stop_errors(wired, gguf):
    wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/stop", json={})
        assert r.status_code == 400
        r = client.post("/admin/runpod/stop",
                        json={"backend": "rp", "model": "nope"})
        assert r.status_code == 404


# ── /admin/reload: forced config reload ────────────────────────────────────

def test_admin_reload_picks_up_disk_changes(wired, gguf, tmp_path):
    cfgfile, _, _ = wired(_full_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/reload")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ok"] is True
        assert body["backends"] == {"local": 2, "srv": 1, "far": 1,
                                    "rp": 1, "groq": 1}
        assert body["aliases"] == ["fast"]
        # add a backend on disk -> reload -> visible
        cfgd = json.loads(cfgfile.read_text(encoding="utf-8"))
        cfgd["backends"]["extra"] = {"type": "openai",
                                     "api_key_env": "X_KEY",
                                     "models": {"m1": "x"}}
        cfgfile.write_text(json.dumps(cfgd), encoding="utf-8")
        assert client.post("/admin/reload").status_code == 200
        names = {e["name"] for e in client.get("/admin/backends").json()
                 ["backends"]}
        assert "extra" in names


# ── auth coverage (middleware covers the new endpoints too) ────────────────

def test_admin_endpoints_require_key_when_configured(wired, gguf):
    cfg = _full_config(gguf)
    cfg["gateway_keys"] = ["sekrit"]
    wired(cfg)
    with TestClient(gw_app.app) as client:
        for path in ("/admin/backends", "/admin/models", "/admin/secrets"):
            assert client.get(path).status_code == 401, path
        assert client.post("/admin/reload").status_code == 401
        r = client.get("/admin/models",
                       headers={"Authorization": "Bearer sekrit"})
        assert r.status_code == 200
