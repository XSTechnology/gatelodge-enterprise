"""LLM Gateway v2 — llama-server backend.

SUBPROCESS mode: one managed child per model — lazy spawn on first
request, health-poll, reuse without respawn, argv flags (ctx / --parallel
/ GPU placement / extra_args), native OpenAI passthrough for chat +
streaming (tools forwarded, no JSON shim), 502 on exit-during-startup or
startup timeout, idle reaper, kill-all on gateway shutdown, /healthz
section, config-sig change replaces the child.

REMOTE mode (base_url — RunPod or any llama-server over HTTP): routed
through the cloud client path, model ids mapped, max_tokens NOT capped
(unlike openai-type cloud), no subprocess spawned. local_only keeps
subprocess entries and drops remote ones.
"""
import json
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.cloud as gw_cloud  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402


# ── Fakes ──────────────────────────────────────────────────────────────────

class _FakePopen:
    instances: list["_FakePopen"] = []

    def __init__(self, argv, **kwargs):
        self.argv = list(argv)
        self.pid = 4200 + len(_FakePopen.instances)
        self.terminated = False
        self._rc = None
        self.returncode = None
        _FakePopen.instances.append(self)

    def poll(self):
        return self._rc

    def terminate(self):
        self.terminated = True
        self._rc = self.returncode = 0

    def wait(self, timeout=None):
        return self._rc or 0

    def kill(self):
        self.terminated = True
        self._rc = self.returncode = 1


def _ls_reply(content="batched!", model="batch"):
    return {"id": "cs-1", "object": "chat.completion", "created": 1,
            "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": content},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 3,
                      "total_tokens": 7}}


def _patch_srv(monkeypatch, *, health=True, reply=None, stream=None,
               health_after=0):
    """Patch llamaserver boundaries; returns (calls, popens)."""
    calls: list[dict] = []
    real_popen = lsrv.Popen

    async def fake_health(base_url, timeout=5.0):
        calls.append({"health": base_url})
        return health

    async def fake_post(base_url, payload, timeout):
        calls.append({"post": (base_url, payload)})
        if isinstance(reply, Exception):
            raise reply
        return reply or _ls_reply()

    async def fake_stream(base_url, payload, timeout):
        calls.append({"stream": (base_url, payload)})
        for d in (stream or []):
            yield d

    monkeypatch.setattr(lsrv, "get_health", fake_health)
    monkeypatch.setattr(lsrv, "post_chat", fake_post)
    monkeypatch.setattr(lsrv, "stream_chat", fake_stream)
    monkeypatch.setattr(lsrv, "Popen", _FakePopen)
    return calls


class _FakeCloudResp:
    def __init__(self, d):
        self._d = d

    def model_dump_json(self):
        return json.dumps(self._d)


class _FakeCloud:
    def __init__(self):
        self.calls = []
        self.chat = types.SimpleNamespace(completions=types.SimpleNamespace(
            create=self._create))

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeCloudResp(_ls_reply(content="remote!", model=kwargs["model"]))


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
        monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
        # child logs → tmp, never the repo data/ dir
        monkeypatch.setattr(lsrv, "PROJECT_ROOT", tmp_path)
        _FakePopen.instances = []
        lsrv.reset_managers()
        return cfgfile, cfg, engine

    yield _wire
    lsrv.reset_managers()


def _config(gguf, remote=False, **model_over) -> dict:
    m = {"gguf": str(gguf), "ctx": 8192, "placement": "", "parallel": 2}
    m.update(model_over)
    cfg = {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "",
        "models_dir": str(gguf.parent / "models"),
        "backends": {
            "local": {"type": "embedded", "models": {}},
            "srv": {"type": "llama-server", "server_exe": "llama-server",
                    "port_base": 8120, "models": {"batch": m}},
        },
        "aliases": {},
    }
    if remote:
        cfg["backends"]["runpod"] = {
            "type": "llama-server",
            "base_url": "https://api.runpod.ai/v2/ep/openai/v1",
            "api_key_env": "RUNPOD_KEY_TEST",
            "models": {"big": "remote-model"}}
    return cfg


# ── subprocess mode: chat ──────────────────────────────────────────────────

def test_subprocess_chat(wired, gguf, monkeypatch):
    wired(_config(gguf))
    calls = _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch", "temperature": 0.3, "max_tokens": 77,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "batch"  # echoed request name
    assert body["served_by"]["backend"] == "srv"
    assert body["choices"][0]["message"]["content"] == "batched!"
    # child spawned with the right flags
    assert len(_FakePopen.instances) == 1
    argv = _FakePopen.instances[0].argv
    assert argv[0] == "llama-server"
    assert "-m" in argv and str(gguf) in argv
    assert "--parallel" in argv and argv[argv.index("--parallel") + 1] == "2"
    assert "--alias" in argv and argv[argv.index("--alias") + 1] == "batch"
    assert "--jinja" in argv and "--no-webui" in argv
    # request hit the child's HTTP API with the alias + knobs forwarded
    post = [c for c in calls if "post" in c][0]["post"]
    assert post[1]["model"] == "batch"
    assert post[1]["temperature"] == 0.3
    assert post[1]["max_tokens"] == 77  # local — NOT capped at 800


def test_subprocess_lazy_and_reuse(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        for _ in range(3):
            assert client.post("/v1/chat/completions", json={
                "model": "srv:batch",
                "messages": [{"role": "user", "content": "hi"}]}).status_code == 200
    assert len(_FakePopen.instances) == 1  # one child, reused


def test_subprocess_argv_gpu_and_extra(wired, gguf, monkeypatch):
    cfg = _config(gguf, placement="1", ctx=4096)
    cfg["backends"]["srv"]["extra_args"] = ["--mlock"]
    wired(cfg)
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
    argv = _FakePopen.instances[0].argv
    assert argv[argv.index("-c") + 1] == "4096"
    assert argv[argv.index("-mg") + 1] == "1"
    assert "--mlock" in argv


def test_subprocess_auto_placement(wired, gguf, monkeypatch):
    wired(_config(gguf, placement="auto"))
    _patch_srv(monkeypatch)
    monkeypatch.setattr(lsrv, "detect_gpus", lambda: [
        {"index": 0, "name": "a", "total_mb": 24564, "used_mb": 20000},
        {"index": 1, "name": "b", "total_mb": 24564, "used_mb": 4000},
    ])
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
    argv = _FakePopen.instances[0].argv
    ts = argv[argv.index("-ts") + 1]
    assert len(ts.split(",")) == 2  # split across both cards


def test_subprocess_exit_during_startup_502(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_srv(monkeypatch, health=False)
    # child "dies" right after spawn
    class _Dying(_FakePopen):
        def poll(self):
            return 3
    monkeypatch.setattr(lsrv, "Popen", _Dying)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "exited during startup" in r.json()["error"]["message"]


def test_subprocess_startup_timeout_502(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["srv"]["start_timeout_s"] = 0.01
    wired(cfg)
    _patch_srv(monkeypatch, health=False)  # proc alive, never healthy
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "not healthy" in r.json()["error"]["message"]


def test_subprocess_unknown_model_400(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "srv:nope",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400


def test_subprocess_missing_gguf_502(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["srv"]["models"]["batch"]["gguf"] = "not-on-disk"
    wired(cfg)
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "not found" in r.json()["error"]["message"]


# ── subprocess mode: streaming ─────────────────────────────────────────────

def _chunk_dict(delta, finish=None):
    return {"id": "cs-1", "object": "chat.completion.chunk",
            "created": 1, "model": "batch",
            "choices": [{"index": 0, "delta": delta,
                         "finish_reason": finish}]}


def _chunk_json(delta, finish=None):
    return json.dumps(_chunk_dict(delta, finish))


def test_subprocess_stream(wired, gguf, monkeypatch):
    wired(_config(gguf))
    calls = _patch_srv(monkeypatch, stream=[
        _chunk_json({"role": "assistant", "content": ""}),
        _chunk_json({"content": "hello"}),
        _chunk_json({}, finish="stop"),
        "[DONE]",
    ])
    tools = [{"type": "function", "function": {
        "name": "f", "description": "d",
        "parameters": {"type": "object"}}}]
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "batch", "stream": True, "tools": tools,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    # exactly one [DONE]; chunks passed through natively
    assert sum(1 for ln in lines if ln == "data: [DONE]") == 1
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    assert events[0]["object"] == "chat.completion.chunk"
    content = "".join(e["choices"][0]["delta"].get("content") or ""
                      for e in events)
    assert content == "hello"
    # tools forwarded natively — no JSON-convention shim
    sp = [c for c in calls if "stream" in c][0]["stream"]
    assert sp[1]["tools"] == tools
    assert sp[1]["stream"] is True


# ── remote mode (RunPod / any llama-server URL) ────────────────────────────

def test_remote_via_cloud_client(wired, gguf, monkeypatch):
    wired(_config(gguf, remote=True))
    fake = _FakeCloud()
    monkeypatch.setattr(gw_cloud, "get_cloud_client", lambda *a, **k: fake)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "runpod:big", "max_tokens": 5000,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    assert r.json()["served_by"]["backend"] == "runpod"
    # remote model id + max_tokens NOT capped (llama-server ≠ openai cloud)
    assert fake.calls[0]["model"] == "remote-model"
    assert fake.calls[0]["max_tokens"] == 5000
    assert len(_FakePopen.instances) == 0  # remote never spawns


def test_remote_stream_via_cloud_client(wired, gguf, monkeypatch):
    wired(_config(gguf, remote=True))
    chunks = [_chunk_dict({"content": "remote "}, None),
              _chunk_dict({"content": "rocks"}, None),
              _chunk_dict({}, "stop")]

    class _FakeStreamCloud:
        def __init__(self):
            self.calls = []
            self.chat = types.SimpleNamespace(completions=types.SimpleNamespace(
                create=self._create))

        async def _create(self, **kwargs):
            self.calls.append(kwargs)

            async def _gen():
                for c in chunks:
                    yield _FakeCloudResp(c)

            return _gen()

    fake = _FakeStreamCloud()
    monkeypatch.setattr(gw_cloud, "get_cloud_client", lambda *a, **k: fake)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "runpod:big", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    content = "".join(e["choices"][0]["delta"].get("content") or ""
                      for e in events)
    assert content == "remote rocks"
    assert len(_FakePopen.instances) == 0


def test_local_only_keeps_subprocess_drops_remote(wired, gguf, monkeypatch):
    cfg = _config(gguf, remote=True)
    cfg["aliases"]["priv"] = {"chain": ["srv:batch", "runpod:big"],
                              "local_only": True}
    wired(cfg)
    calls = _patch_srv(monkeypatch)
    fake = _FakeCloud()
    monkeypatch.setattr(gw_cloud, "get_cloud_client", lambda *a, **k: fake)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "priv", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    assert r.json()["served_by"]["backend"] == "srv"  # subprocess survived
    assert fake.calls == []  # remote (RunPod) dropped — leaves the machine


# ── registry lifecycle ─────────────────────────────────────────────────────

def test_idle_reaper(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["srv"]["idle_timeout_s"] = 60
    wired(cfg)
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "batch",
            "messages": [{"role": "user", "content": "hi"}]})
        mgr = lsrv._MANAGERS[("srv", "batch")]
        assert mgr.is_running()
        mgr.last_used = 1.0  # ancient → idle beyond idle_timeout_s
        n = lsrv._reaper_once()
        assert n == 1
        assert not mgr.is_running()
        assert _FakePopen.instances[0].terminated


def test_config_change_replaces_child(wired, gguf, monkeypatch):
    cfgfile, cfg, _ = wired(_config(gguf))
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
        # hot-reload: ctx change → new signature → old child replaced
        raw = json.loads(cfgfile.read_text(encoding="utf-8"))
        raw["backends"]["srv"]["models"]["batch"]["ctx"] = 2048
        cfgfile.write_text(json.dumps(raw), encoding="utf-8")
        cfg.reload_now()
        client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
    assert len(_FakePopen.instances) == 2  # respawned with new flags
    assert _FakePopen.instances[0].terminated  # old child stopped
    argv = _FakePopen.instances[1].argv
    assert argv[argv.index("-c") + 1] == "2048"


def test_kill_all_on_shutdown(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:  # context exit → shutdown event
        client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
        assert _FakePopen.instances[0].terminated is False
    assert _FakePopen.instances[0].terminated is True


def test_healthz_llama_server_section(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "batch", "messages": [{"role": "user", "content": "hi"}]})
        hz = client.get("/healthz").json()
    entry = hz["llama_server"][0]
    assert entry["backend"] == "srv"
    assert entry["model"] == "batch"
    assert entry["state"] == "running"
    assert entry["port"] >= 8120
    assert entry["parallel"] == 2


def test_v1_models_lists_llama_server_models(wired, gguf, monkeypatch):
    wired(_config(gguf, remote=True))
    _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        ids = [m["id"] for m in client.get("/v1/models").json()["data"]]
    assert "batch" in ids and "big" in ids
