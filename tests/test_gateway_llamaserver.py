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
               health_after=0, tokenize=None):
    """Patch llamaserver boundaries; returns (calls, popens).

    tokenize: None → deterministic fake counter (~5 chars/token);
    pass a callable(content)→list|None to override, or "down" to
    simulate a dead /tokenize endpoint (heuristic fallback)."""
    calls: list[dict] = []
    real_popen = lsrv.Popen

    def fake_tokenize(base_url, content, timeout=5.0):
        calls.append({"tokenize": (base_url, len(content))})
        if tokenize == "down":
            return None
        if tokenize is not None:
            return tokenize(content)
        return [0] * max(1, len(content) // 5)

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
    monkeypatch.setattr(lsrv, "post_tokenize_sync", fake_tokenize)
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
        # child-management tests repeat identical prompts around config
        # changes — the response cache would mask the new child entirely.
        "response_cache": {"enabled": False},
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


# ── trim + knobs (fit_chat / fit_prompt) ───────────────────────────────────

def _tok(monkeypatch):
    """Deterministic /tokenize fake (~5 chars/token) with call tracking."""
    seen: list[str] = []

    def fake(base_url, content, timeout=5.0):
        seen.append(content)
        return [0] * max(1, len(content) // 5)
    monkeypatch.setattr(lsrv, "post_tokenize_sync", fake)
    return fake, seen


def _big_history(n=6, chars=5000):
    """system + n user turns of ~chars/5 tokens each (fake counter)."""
    msgs = [{"role": "system", "content": "sys"}]
    for i in range(n):
        msgs.append({"role": "user", "content": f"m{i} " + "x" * chars})
    return msgs


def test_fit_chat_fast_path_untouched_single_tokenize(monkeypatch):
    _, seen = _tok(monkeypatch)
    msgs = [{"role": "system", "content": "be brief"},
            {"role": "user", "content": "hello " * 10},
            {"role": "assistant", "content": "hi"}]
    out, tools = lsrv.fit_chat("http://x", msgs, 512, None,
                               ctx=2048, parallel=1)
    assert out is msgs and tools is None  # identical list, untouched
    assert len(seen) == 1  # ONE joined call — no per-message counts


def test_fit_chat_trims_to_slot_ctx(monkeypatch):
    _tok(monkeypatch)
    msgs = _big_history()  # ~1000 tok/turn, ctx 2048 → budget 1728
    out, tools = lsrv.fit_chat("http://x", msgs, 256, None,
                               ctx=2048, parallel=1)
    assert len(out) == 2  # system + newest only
    assert out[0]["role"] == "system"
    assert "[context note:" in out[0]["content"]  # trim note on system
    assert out[1]["content"].startswith("m5")
    assert tools is None


def test_fit_chat_parallel_divides_context(monkeypatch):
    _tok(monkeypatch)
    msgs = _big_history()
    # parallel=4: slot = 8192//4 = 2048 → SAME trim as a 2048 window
    out, _ = lsrv.fit_chat("http://x", msgs, 256, None, ctx=8192, parallel=4)
    assert len(out) == 2
    # parallel=1: the full 8192 window holds all of it
    out, _ = lsrv.fit_chat("http://x", msgs, 256, None, ctx=8192, parallel=1)
    assert out is msgs


def test_fit_chat_native_tools_cost_starving_tools_dropped(monkeypatch):
    _tok(monkeypatch)
    tools = [{"type": "function", "function": {
        "name": f"t{i}", "description": "d" * 3000,
        "parameters": {"type": "object", "properties": {}}}} for i in range(4)]
    msgs = [{"role": "user", "content": "hi"}]
    out, tools_out = lsrv.fit_chat("http://x", msgs, 256, tools,
                                   ctx=2048, parallel=1)
    assert tools_out is None  # ~3k tokens of tools vs a 1728 budget
    assert out == msgs  # messages untouched once tools dropped


def test_fit_chat_native_tools_cost_budgeted(monkeypatch):
    _tok(monkeypatch)
    tools = [{"type": "function", "function": {
        "name": "t", "description": "d" * 500,
        "parameters": {"type": "object", "properties": {}}}}]
    msgs = _big_history(n=3)
    out, tools_out = lsrv.fit_chat("http://x", msgs, 256, tools,
                                   ctx=2048, parallel=1)
    assert tools_out is tools  # kept — but messages trimmed around them
    assert len(out) == 2


def test_fit_chat_prompt_cap_below_window(monkeypatch):
    _tok(monkeypatch)
    msgs = _big_history(n=3)  # ~3000 tokens — fits 8192, not the cap
    out, _ = lsrv.fit_chat("http://x", msgs, 256, None, ctx=8192,
                           parallel=1, prompt_cap=2048)
    assert len(out) == 2  # trimmed to the CAP, not the window


def test_fit_chat_tokenizer_down_heuristic_fallback(monkeypatch):
    monkeypatch.setattr(lsrv, "post_tokenize_sync",
                        lambda b, c, timeout=5.0: None)
    msgs = _big_history(chars=4000)  # heuristic: 1000 tok/turn
    out, _ = lsrv.fit_chat("http://x", msgs, 256, None, ctx=2048, parallel=1)
    assert len(out) == 2  # still trims, just with ~chars/4 counts


def test_tokenizer_bridge_caches(monkeypatch):
    _, seen = _tok(monkeypatch)
    br = lsrv._TokenizerBridge("http://x")
    br.n_ctx = 1024
    assert br.count("abcde") == 1
    assert br.count("abcde") == 1  # cache hit
    assert len(br.tokenize(b"abcde", add_bos=False)) == 1  # bytes path
    assert len(seen) == 1


def test_post_tokenize_sync_add_special_retry(monkeypatch):
    class _Resp:
        def __init__(self, status, payload):
            self.status_code = status
            self._p = payload

        def json(self):
            return self._p

    class _FakeSync:
        def __init__(self):
            self.sent = []

        def post(self, url, json=None, timeout=None):
            self.sent.append(json)
            if len(self.sent) == 1:
                return _Resp(400, {})  # old build rejects add_special
            return _Resp(200, {"tokens": [1, 2, 3]})

    fs = _FakeSync()
    monkeypatch.setattr(lsrv, "_sync_client", fs)
    assert lsrv.post_tokenize_sync("http://x", "hi") == [1, 2, 3]
    assert fs.sent[0]["add_special"] is False
    assert "add_special" not in fs.sent[1]  # retried bare


def test_post_tokenize_sync_failure_returns_none(monkeypatch):
    class _Boom:
        def post(self, *a, **k):
            raise RuntimeError("child gone")
    monkeypatch.setattr(lsrv, "_sync_client", _Boom())
    assert lsrv.post_tokenize_sync("http://x", "hi") is None


def test_fit_prompt_untouched_when_fits(monkeypatch):
    _tok(monkeypatch)
    p = "hi there"
    assert lsrv.fit_prompt("http://x", p, 256, ctx=2048,
                           parallel=1) is p


def test_fit_prompt_truncates_oversized(monkeypatch):
    _tok(monkeypatch)
    p = "y" * 30000  # 6000 tokens vs a 1728 budget
    out = lsrv.fit_prompt("http://x", p, 256, ctx=2048, parallel=1)
    assert "middle of this message was truncated" in out
    assert len(out) < len(p)
    assert len(out) // 5 <= 1728  # head+tail cut fits the budget


# ── trim + knobs through the API (subprocess chat) ─────────────────────────

def test_subprocess_chat_trims_oversized_history(wired, gguf, monkeypatch):
    wired(_config(gguf, ctx=2048, parallel=1))
    calls = _patch_srv(monkeypatch)
    msgs = _big_history()
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch", "messages": msgs, "max_tokens": 256})
    assert r.status_code == 200, r.text
    sent = [c for c in calls if "post" in c][0]["post"][1]["messages"]
    assert len(sent) < len(msgs)  # oldest dropped, system + newest kept
    assert "[context note:" in sent[0]["content"]
    assert sent[-1]["content"].startswith("m5")


def test_subprocess_chat_no_think_knob(wired, gguf, monkeypatch):
    wired(_config(gguf, no_think=True))
    calls = _patch_srv(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200, r.text
    sent = [c for c in calls if "post" in c][0]["post"][1]["messages"]
    assert sent[-1]["content"].rstrip().endswith("/no_think")


def test_subprocess_chat_prompt_cap_knob(wired, gguf, monkeypatch):
    # slot 4096 but capped at 2048 → trims to the cap, not the window
    wired(_config(gguf, max_prompt_tokens=2048))
    calls = _patch_srv(monkeypatch)
    msgs = _big_history(n=3)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "batch", "messages": msgs, "max_tokens": 256})
    assert r.status_code == 200, r.text
    sent = [c for c in calls if "post" in c][0]["post"][1]["messages"]
    assert len(sent) < len(msgs)


def test_subprocess_stream_trims_oversized(wired, gguf, monkeypatch):
    wired(_config(gguf, ctx=2048, parallel=1))
    calls = _patch_srv(monkeypatch, stream=[
        _chunk_json({"role": "assistant", "content": ""}),
        _chunk_json({"content": "ok"}),
        _chunk_json({}, finish="stop"),
        "[DONE]",
    ])
    msgs = _big_history()
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "batch", "stream": True, "messages": msgs,
                "max_tokens": 256}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    sent = [c for c in calls if "stream" in c][0]["stream"][1]["messages"]
    assert len(sent) < len(msgs)
    assert "[context note:" in sent[0]["content"]


def test_completions_llama_server_prompt_trim(wired, gguf, monkeypatch):
    wired(_config(gguf, ctx=2048, parallel=1))
    calls = _patch_srv(monkeypatch)

    async def fake_post_completions(base_url, payload, timeout):
        calls.append({"completions": (base_url, payload)})
        return {"id": "c-9", "object": "text.completion", "created": 1,
                "model": payload.get("model"),
                "choices": [{"index": 0, "text": "ok",
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                          "total_tokens": 2}}
    monkeypatch.setattr(lsrv, "post_completions", fake_post_completions)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/completions", json={
            "model": "batch", "prompt": "y" * 30000, "max_tokens": 256})
    assert r.status_code == 200, r.text
    sent = [c for c in calls if "completions" in c][0]["completions"][1]
    assert "middle of this message was truncated" in sent["prompt"]
    assert len(sent["prompt"]) < 30000


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
