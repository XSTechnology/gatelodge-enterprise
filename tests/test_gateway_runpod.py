"""LLM Gateway v2 — runpod backend (pods + serverless).

SERVERLESS models ({endpoint_id, remote_id}) route through the cloud
client with base_url https://api.runpod.ai/v2/<ep>/openai/v1 (per-endpoint
client cache, max_tokens NOT capped). POD models are lifecycle-managed:
cold deploy on first request (camelCase payload, raw `deploy` override),
find-by-name reuse while running, resume when stopped, health-poll via
the proxy URL, 502 on deploy failure / exit-during-startup / startup
timeout, idle reaper (stop tier then terminate tier), /healthz section,
/v1/models listing, local_only drops runpod entries.
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
import gateway.runpod as rp  # noqa: E402


# ── Fakes ──────────────────────────────────────────────────────────────────

POD_ID = "pod-ab12cd34"


class _FakeRunPod:
    """REST surface fake — pods list + call log."""

    def __init__(self):
        self.pods: list[dict] = []
        self.calls: list[tuple] = []

    async def deploy(self, payload, api_key_env):
        self.calls.append(("deploy", payload))
        pod = {"id": POD_ID, "name": payload.get("name"),
               "status": "RUNNING"}
        self.pods.append(pod)
        return pod

    async def list_pods(self, api_key_env):
        self.calls.append(("list",))
        return list(self.pods)

    async def start(self, pod_id, api_key_env):
        self.calls.append(("start", pod_id))
        for p in self.pods:
            if p["id"] == pod_id:
                p["status"] = "RUNNING"
        return {}

    async def stop(self, pod_id, api_key_env):
        self.calls.append(("stop", pod_id))
        for p in self.pods:
            if p["id"] == pod_id:
                p["status"] = "STOPPED"
        return {}

    async def terminate(self, pod_id, api_key_env):
        self.calls.append(("terminate", pod_id))
        self.pods = [p for p in self.pods if p["id"] != pod_id]
        return {}

    def kinds(self):
        return [c[0] for c in self.calls]


def _patch_pod_http(monkeypatch, *, health=True, reply=None, stream=None):
    """Patch the pod HTTP bridge (health + chat + stream)."""
    calls: list[dict] = []

    async def fake_health(base_url):
        calls.append({"health": base_url})
        return health

    async def fake_post(base_url, payload, timeout):
        calls.append({"post": (base_url, payload)})
        return reply or {"id": "cc-1", "object": "chat.completion",
                         "created": 1, "model": payload.get("model"),
                         "choices": [{"index": 0, "message": {
                             "role": "assistant", "content": "pod answer"},
                             "finish_reason": "stop"}],
                         "usage": {"prompt_tokens": 2, "completion_tokens": 2,
                                   "total_tokens": 4}}

    async def fake_stream(base_url, payload, timeout):
        calls.append({"stream": (base_url, payload)})
        for d in (stream or []):
            yield d

    monkeypatch.setattr(rp, "pod_healthy", fake_health)
    monkeypatch.setattr(lsrv, "post_chat", fake_post)
    monkeypatch.setattr(lsrv, "stream_chat", fake_stream)
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
        return _FakeCloudResp({
            "id": "cc-1", "object": "chat.completion", "created": 1,
            "model": kwargs["model"],
            "choices": [{"index": 0, "message": {
                "role": "assistant", "content": "serverless answer"},
                "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                      "total_tokens": 5}})


def _patch_cloud(monkeypatch):
    rec: dict = {}
    fake = _FakeCloud()

    def get_client(name, backend, timeout=120.0):
        rec["client_name"] = name
        rec["backend"] = backend
        return fake

    monkeypatch.setattr(gw_cloud, "get_cloud_client", get_client)
    return rec, fake


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
        rp.reset_managers()
        return cfgfile, cfg, engine

    yield _wire
    rp.reset_managers()


def _rp_config(gguf, **pod_over) -> dict:
    pod = {
        "gpu_type_ids": ["NVIDIA RTX A6000"],
        "image": "ghcr.io/ggml-org/llama.cpp:server-cuda",
        "docker_args": "-m /workspace/m.gguf --host 0.0.0.0 --port 8080",
        "ports": "8080/http", "spot": True, "alias": "bigpod",
        "start_timeout_s": 30,
    }
    pod.update(pod_over)
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "",
        "models_dir": str(gguf.parent / "models"),
        "backends": {
            "local": {"type": "embedded",
                      "models": {"loc": {"gguf": str(gguf), "ctx": 1024}}},
            "rp": {"type": "runpod", "api_key_env": "RUNPOD_API_KEY_TEST",
                   "models": {
                       "fast": {"endpoint_id": "ep-abc", "remote_id": "model-x"},
                       "big": pod}},
        },
        "aliases": {},
    }


def _patch_rest(monkeypatch) -> _FakeRunPod:
    fake = _FakeRunPod()
    monkeypatch.setattr(rp, "deploy_pod", fake.deploy)
    monkeypatch.setattr(rp, "list_pods", fake.list_pods)
    monkeypatch.setattr(rp, "start_pod", fake.start)
    monkeypatch.setattr(rp, "stop_pod", fake.stop)
    monkeypatch.setattr(rp, "terminate_pod", fake.terminate)
    return fake


# ── serverless mode ────────────────────────────────────────────────────────

def test_serverless_chat(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    rest = _patch_rest(monkeypatch)
    rec, fake = _patch_cloud(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "rp:fast", "max_tokens": 5000,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["served_by"]["backend"] == "rp"
    assert body["choices"][0]["message"]["content"] == "serverless answer"
    # OpenAI client pointed at the endpoint's URL, per-endpoint cache name
    assert rec["backend"]["base_url"] == \
        "https://api.runpod.ai/v2/ep-abc/openai/v1"
    assert rec["backend"]["api_key_env"] == "RUNPOD_API_KEY_TEST"
    assert rec["client_name"] == "runpod-rp-fast"
    # remote id + NO max_tokens cap (runpod bills per second)
    assert fake.calls[0]["model"] == "model-x"
    assert fake.calls[0]["max_tokens"] == 5000
    assert rest.kinds() == []  # serverless never touches pod lifecycle


def test_serverless_stream(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    _patch_rest(monkeypatch)
    _patch_cloud(monkeypatch)

    chunks = [{"id": "cs-1", "object": "chat.completion.chunk", "created": 1,
               "model": "model-x",
               "choices": [{"index": 0, "delta": {"content": "warm "},
                            "finish_reason": None}]},
              {"id": "cs-1", "object": "chat.completion.chunk", "created": 1,
               "model": "model-x",
               "choices": [{"index": 0, "delta": {"content": "serverless"},
                            "finish_reason": None}]},
              {"id": "cs-1", "object": "chat.completion.chunk", "created": 1,
               "model": "model-x",
               "choices": [{"index": 0, "delta": {},
                            "finish_reason": "stop"}]}]

    class _StreamCloud:
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

    fake = _StreamCloud()
    monkeypatch.setattr(gw_cloud, "get_cloud_client",
                        lambda name, backend, timeout=120.0: fake)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "rp:fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    content = "".join(e["choices"][0]["delta"].get("content") or ""
                      for e in events)
    assert content == "warm serverless"


# ── pod mode ───────────────────────────────────────────────────────────────

def test_pod_cold_deploy_and_chat(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    rest = _patch_rest(monkeypatch)
    http = _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "temperature": 0.2, "max_tokens": 42,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "big"
    assert body["served_by"]["backend"] == "rp"
    assert body["choices"][0]["message"]["content"] == "pod answer"
    # deployed once, never started (health-poll may re-list)
    kinds = rest.kinds()
    assert kinds.count("deploy") == 1
    assert "start" not in kinds
    payload = rest.calls[1][1]
    assert payload["name"] == "gw-rp-big"
    assert payload["imageName"] == "ghcr.io/ggml-org/llama.cpp:server-cuda"
    assert payload["gpuTypeIds"] == ["NVIDIA RTX A6000"]
    assert payload["interruptible"] is True  # spot → interruptible (live schema)
    assert payload["ports"] == ["8080/http"]  # array (live schema)
    assert payload["dockerStartCmd"][0] == "-m"  # docker args → string array
    assert "--port" in payload["dockerStartCmd"]
    # health-polled + proxied through the pod's proxy.runpod.net URL
    assert any(c["health"] == f"https://{POD_ID}-8080.proxy.runpod.net"
               for c in http)
    post = [c["post"] for c in http if "post" in c][0]
    assert post[0] == f"https://{POD_ID}-8080.proxy.runpod.net"
    assert post[1]["model"] == "bigpod"  # the pod model's alias
    assert post[1]["temperature"] == 0.2
    assert post[1]["max_tokens"] == 42


def test_pod_deploy_payload_mapping(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf, volume_gb=50, volume_mount="/data",
                     container_disk_gb=25, gpu_count=2, spot=False,
                     deploy={"imageName": "custom/img:latest", "zzz": 1})
    wired(cfg)
    rest = _patch_rest(monkeypatch)
    _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    payload = [c for c in rest.calls if c[0] == "deploy"][0][1]
    assert payload["volumeInGb"] == 50
    assert payload["volumeMountPath"] == "/data"
    assert payload["containerDiskInGb"] == 25
    assert payload["gpuCount"] == 2
    assert "interruptible" not in payload  # spot false → omitted
    assert payload["imageName"] == "custom/img:latest"  # raw deploy wins
    assert payload["zzz"] == 1  # unknown raw fields pass through


def test_pod_reuse_when_running(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    rest = _patch_rest(monkeypatch)
    _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        for _ in range(2):
            assert client.post("/v1/chat/completions", json={
                "model": "big",
                "messages": [{"role": "user", "content": "hi"}]}).status_code == 200
    # second request found the running pod — no second deploy, no start
    kinds = rest.kinds()
    assert kinds.count("deploy") == 1
    assert "start" not in kinds


def test_pod_resume_when_stopped(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    rest = _patch_rest(monkeypatch)
    _patch_pod_http(monkeypatch)
    rest.pods.append({"id": "pod-old", "name": "gw-rp-big",
                      "status": "EXITED"})
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    # found the stopped pod by name → started it, never deployed
    assert "deploy" not in rest.kinds()
    assert ("start", "pod-old") in rest.calls


def test_pod_deploy_failure_502(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    rest = _patch_rest(monkeypatch)

    async def boom(payload, api_key_env):
        raise rp.RunPodError("no gpu available")

    monkeypatch.setattr(rp, "deploy_pod", boom)
    _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "no gpu available" in r.json()["error"]["message"]


def test_pod_exited_during_startup_502(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf, start_timeout_s=3)
    wired(cfg)
    rest = _patch_rest(monkeypatch)

    async def deploy_then_die(payload, api_key_env):
        pod = await rest.deploy(payload, api_key_env)
        pod["status"] = "EXITED"  # dies right after deploy
        return pod

    async def start_noop(pod_id, api_key_env):
        rest.calls.append(("start", pod_id))
        return {}  # stays dead

    monkeypatch.setattr(rp, "deploy_pod", deploy_then_die)
    monkeypatch.setattr(rp, "start_pod", start_noop)
    _patch_pod_http(monkeypatch, health=False)  # never healthy
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "EXITED" in r.json()["error"]["message"]


def test_pod_startup_timeout_502(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf, start_timeout_s=0.01)
    wired(cfg)
    _patch_rest(monkeypatch)

    async def never_healthy(base_url):
        return False

    monkeypatch.setattr(rp, "pod_healthy", never_healthy)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "not healthy" in r.json()["error"]["message"]


def test_pod_pinned_pod_id(wired, gguf, monkeypatch):
    """BYO pod: pod_id pin skips find-by-name AND deploy; a stopped pinned
    pod is started; a missing pinned pod is a 502."""
    cfg = _rp_config(gguf, pod_id="pod-byo123", port=11434,
                     alias="qwen2.5:0.5b", image="", docker_args="")
    wired(cfg)
    rest = _patch_rest(monkeypatch)
    rest.pods.append({"id": "pod-byo123", "name": "disgusted_scarlet_sheep",
                      "status": "EXITED"})
    http = _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200, r.text
    kinds = rest.kinds()
    assert "deploy" not in kinds          # never deployed
    assert ("start", "pod-byo123") in rest.calls  # resumed instead
    post = [c["post"] for c in http if "post" in c][0]
    assert post[0] == "https://pod-byo123-11434.proxy.runpod.net"
    assert post[1]["model"] == "qwen2.5:0.5b"  # alias → remote model tag

    # pinned pod missing → 502
    rest.pods.clear()
    rp.reset_managers()
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "not found" in r.json()["error"]["message"]


def _stream_payload(model):
    return json.dumps({"id": "cs-1", "object": "chat.completion.chunk",
                       "created": 1, "model": model,
                       "choices": [{"index": 0, "delta": {"content": "gpu!"},
                                    "finish_reason": None}]})


def test_pod_stream(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    _patch_rest(monkeypatch)
    tools = [{"type": "function", "function": {
        "name": "f", "description": "d",
        "parameters": {"type": "object"}}}]
    http = _patch_pod_http(monkeypatch, stream=[
        _stream_payload("bigpod"), "[DONE]"])
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "big", "stream": True, "tools": tools,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    content = "".join(e["choices"][0]["delta"].get("content") or ""
                      for e in events)
    assert content == "gpu!"
    sp = [c["stream"] for c in http if "stream" in c][0]
    assert sp[1]["tools"] == tools  # native passthrough
    assert sp[1]["stream"] is True


# ── serverless RAW job protocol (transport "runsync") ─────────────────────

OLLAMA_SINGLE = {"created_at": "t", "done": True, "done_reason": "stop",
                 "eval_count": 7, "eval_duration": 1, "load_duration": 1,
                 "message": {"content": "one two three", "role": "assistant"},
                 "model": "smollm2:135m", "prompt_eval_count": 5,
                 "prompt_eval_duration": 1, "total_duration": 9}

OLLAMA_CHUNKS = [
    '{"model":"smollm2:135m","message":{"role":"assistant","content":"one "},"done":false}',
    '{"model":"smollm2:135m","message":{"role":"assistant","content":"two "},"done":false}',
    '{"model":"smollm2:135m","message":{"content":"","role":"assistant"},"done":true,'
    '"done_reason":"stop","eval_count":3,"prompt_eval_count":4}',
]


def _patch_runsync(monkeypatch, output=None, error=None, calls=None):
    async def fake_runsync(endpoint_id, body_input, api_key_env,
                            timeout=120.0, policy=None):
        if calls is not None:
            calls.append({"endpoint_id": endpoint_id, "input": body_input,
                          "policy": policy})
        if error:
            raise rp.RunPodError(error)
        return output
    monkeypatch.setattr(rp, "runsync_chat", fake_runsync)


def test_runsync_transport_chat(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["smollm"] = {
        "endpoint_id": "ep-xyz", "transport": "runsync",
        "remote_id": "smollm2:135m"}
    wired(cfg)
    calls: list[dict] = []
    _patch_runsync(monkeypatch, output=OLLAMA_SINGLE, calls=calls)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "smollm", "temperature": 0.2, "max_tokens": 44,
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "smollm"
    assert body["served_by"]["backend"] == "rp"
    assert body["choices"][0]["message"]["content"] == "one two three"
    assert body["choices"][0]["finish_reason"] == "stop"
    assert body["usage"] == {"prompt_tokens": 5, "completion_tokens": 7,
                             "total_tokens": 12}
    # raw protocol payload: model → remote_id, knobs forwarded
    assert calls[0]["input"]["model"] == "smollm2:135m"
    assert calls[0]["input"]["temperature"] == 0.2
    assert calls[0]["input"]["max_tokens"] == 44


def test_runsync_transport_chunked_output(wired, gguf, monkeypatch):
    """stream:true worker output (list of Ollama chunk strings) joins."""
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["smollm"] = {
        "endpoint_id": "ep-xyz", "transport": "runsync"}
    wired(cfg)
    _patch_runsync(monkeypatch, output=OLLAMA_CHUNKS)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "smollm",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    body = r.json()
    assert body["choices"][0]["message"]["content"] == "one two "
    assert body["usage"]["completion_tokens"] == 3
    assert body["usage"]["prompt_tokens"] == 4


def test_runsync_transport_job_failed_502(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["smollm"] = {
        "endpoint_id": "ep-xyz", "transport": "runsync"}
    wired(cfg)
    _patch_runsync(monkeypatch, error="job FAILED: oom")
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "smollm",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert "FAILED" in r.json()["error"]["message"]


def _patch_run_stream(monkeypatch, chunks=None, unavailable=False):
    """Fake run_job + stream_job for the streaming path."""
    async def fake_run(endpoint_id, body_input, api_key_env,
                       timeout=120.0, policy=None):
        return "job-123"

    async def fake_stream(endpoint_id, job_id, api_key_env, timeout=300.0):
        if unavailable:
            raise rp.RunPodStreamUnavailable("stream -> 404")
        for c in (chunks or []):
            yield c

    monkeypatch.setattr(rp, "run_job", fake_run)
    monkeypatch.setattr(rp, "stream_job", fake_stream)


def test_runsync_transport_stream_true(wired, gguf, monkeypatch):
    """Streaming: POST /run + GET /stream SSE chunks → incremental deltas."""
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["smollm"] = {
        "endpoint_id": "ep-xyz", "transport": "runsync"}
    wired(cfg)
    _patch_run_stream(monkeypatch, chunks=[
        '{"message":{"role":"assistant","content":"one "},"done":false}',
        {"message": {"role": "assistant", "content": "two"},
         "done": False},
        {"message": {"content": ""}, "done": True, "done_reason": "stop"},
    ])
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "smollm", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    deltas = [e["choices"][0]["delta"].get("content")
              for e in events if e["choices"][0]["delta"].get("content")]
    assert deltas == ["one ", "two"]  # incremental, not one blob
    assert events[-1]["choices"][0]["finish_reason"] == "stop"


def test_runsync_transport_stream_fallback(wired, gguf, monkeypatch):
    """No /stream support → collect-then-emit fallback via runsync."""
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["smollm"] = {
        "endpoint_id": "ep-xyz", "transport": "runsync"}
    wired(cfg)
    _patch_run_stream(monkeypatch, unavailable=True)
    calls: list[dict] = []
    _patch_runsync(monkeypatch, output=OLLAMA_SINGLE, calls=calls)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "smollm", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    content = "".join(e["choices"][0]["delta"].get("content") or ""
                      for e in events)
    assert content == "one two three"
    # fallback dropped stream:true from the collected job input
    assert calls[0]["input"].get("stream") is None


def test_runsync_default_transport_is_openai(wired, gguf, monkeypatch):
    """No transport field → openai shim path (cloud client), not runsync."""
    wired(_rp_config(gguf))
    _patch_rest(monkeypatch)
    rec, fake = _patch_cloud(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "rp:fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    assert rec["client_name"] == "runpod-rp-fast"  # cloud client used


def test_ollama_output_mapping_shapes():
    # single dict in a list (the worker's non-stream shape)
    d = rp.ollama_output_to_openai([OLLAMA_SINGLE], "echo")
    assert d["choices"][0]["message"]["content"] == "one two three"
    assert d["usage"]["total_tokens"] == 12
    assert d["model"] == "echo"
    # chunk-string list (the worker's stream:true shape)
    d2 = rp.ollama_output_to_openai(OLLAMA_CHUNKS, "echo")
    assert d2["choices"][0]["message"]["content"] == "one two "
    # garbage entries skipped
    d3 = rp.ollama_output_to_openai(["not json", 42, OLLAMA_SINGLE], "e")
    assert d3["choices"][0]["message"]["content"] == "one two three"
    # empty output → empty content, stop
    d4 = rp.ollama_output_to_openai(None, "e")
    assert d4["choices"][0]["message"]["content"] == ""
    assert d4["choices"][0]["finish_reason"] == "stop"


def test_ollama_chunk_content_shapes():
    # JSON string chunk
    c, done, reason = rp.ollama_chunk_content(
        '{"message":{"content":"hi"},"done":false}')
    assert (c, done, reason) == ("hi", False, "")
    # dict chunk with done
    c, done, reason = rp.ollama_chunk_content(
        {"message": {"content": ""}, "done": True, "done_reason": "length"})
    assert (c, done, reason) == ("", True, "length")
    # garbage tolerated
    assert rp.ollama_chunk_content("not json") == ("", False, "")
    assert rp.ollama_chunk_content(42) == ("", False, "")


# ── docs refinements: policy passthrough, 429 backoff, cancel-on-timeout ──

def test_runsync_policy_passthrough(wired, gguf, monkeypatch):
    """Config policy (executionTimeout/ttl) reaches the job submission."""
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["smollm"] = {
        "endpoint_id": "ep-xyz", "transport": "runsync",
        "policy": {"executionTimeout": 300000, "ttl": 600000}}
    wired(cfg)
    calls: list[dict] = []
    _patch_runsync(monkeypatch, output=OLLAMA_SINGLE, calls=calls)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "smollm",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    assert calls[0]["policy"] == {"executionTimeout": 300000,
                                  "ttl": 600000}


def test_serverless_policy_normalized(wired, gguf):
    """policy survives config normalization; non-dict values are dropped."""
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["a"] = {
        "endpoint_id": "e1", "policy": {"ttl": 60000}}
    cfg["backends"]["rp"]["models"]["b"] = {
        "endpoint_id": "e2", "policy": "garbage"}
    _, c, _ = wired(cfg)
    models = c.backends["rp"]["models"]
    assert models["a"]["policy"] == {"ttl": 60000}
    assert models["b"]["policy"] == {}


# ── runsync_chat internals (fake httpx) ────────────────────────────────────

class _FakeResp:
    def __init__(self, status_code, json_data=None):
        self.status_code = status_code
        self._json = json_data
        self.text = ""

    def json(self):
        return self._json


class _FakeAsyncClient:
    """Scripted responses; records every call. Class-level state (tests
    set .responses before awaiting the function under test)."""
    responses: list[_FakeResp] = []
    calls: list[tuple] = []

    def __init__(self, timeout=None):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None):
        _FakeAsyncClient.calls.append(("POST", url, json))
        return _FakeAsyncClient.responses.pop(0)

    async def get(self, url, headers=None):
        _FakeAsyncClient.calls.append(("GET", url, None))
        return _FakeAsyncClient.responses.pop(0)


@pytest.fixture()
def fake_httpx(monkeypatch):
    """Patch rp.httpx.AsyncClient with the scripted fake + fast backoff."""
    _FakeAsyncClient.responses = []
    _FakeAsyncClient.calls = []
    monkeypatch.setattr(rp.httpx, "AsyncClient", _FakeAsyncClient)
    monkeypatch.setattr(rp, "_RETRY_429_BASE_S", 0.01)
    monkeypatch.setenv("RUNPOD_API_KEY_TEST", "x")
    return _FakeAsyncClient


async def test_runsync_429_backoff(fake_httpx):
    """429 then 200 → the POST retried (docs: exponential backoff)."""
    fake_httpx.responses = [
        _FakeResp(429),
        _FakeResp(200, {"status": "COMPLETED", "id": "j1", "output": ["x"]}),
    ]
    out = await rp.runsync_chat("ep", {"prompt": "hi"},
                                "RUNPOD_API_KEY_TEST", timeout=10.0)
    assert out == ["x"]
    posts = [c for c in fake_httpx.calls if c[0] == "POST"]
    assert len(posts) == 2  # one retry
    assert "runsync?wait=" in posts[0][1]


async def test_runsync_policy_in_payload(fake_httpx):
    fake_httpx.responses = [
        _FakeResp(200, {"status": "COMPLETED", "id": "j1", "output": []}),
    ]
    await rp.runsync_chat("ep", {"prompt": "hi"}, "RUNPOD_API_KEY_TEST",
                          timeout=10.0, policy={"executionTimeout": 60000})
    body = fake_httpx.calls[0][2]
    assert body["policy"] == {"executionTimeout": 60000}
    assert body["input"] == {"prompt": "hi"}


async def test_runsync_cancel_on_timeout(fake_httpx):
    """Polling timeout → the queued job is CANCELLED before the error."""
    fake_httpx.responses = [
        _FakeResp(200, {"status": "IN_QUEUE", "id": "job-stuck"}),
        _FakeResp(200, {"status": "IN_QUEUE", "id": "job-stuck"}),  # poll
        _FakeResp(200, {"status": "cancelled"}),  # /cancel response
    ]
    with pytest.raises(rp.RunPodError) as ei:
        await rp.runsync_chat("ep", {"prompt": "hi"},
                              "RUNPOD_API_KEY_TEST", timeout=0.05)
    assert "cancelled" in str(ei.value) or "IN_QUEUE" in str(ei.value)
    kinds = [(m, url) for m, url, _ in fake_httpx.calls]
    assert ("POST", "https://api.runpod.ai/v2/ep/cancel/job-stuck") in kinds


async def test_run_job_policy_and_backoff(fake_httpx):
    fake_httpx.responses = [
        _FakeResp(429),
        _FakeResp(200, {"id": "job-9", "status": "IN_QUEUE"}),
    ]
    jid = await rp.run_job("ep", {"prompt": "hi"}, "RUNPOD_API_KEY_TEST",
                           timeout=10.0, policy={"ttl": 60000})
    assert jid == "job-9"
    posts = [c for c in fake_httpx.calls if c[0] == "POST"]
    assert len(posts) == 2  # 429 retried
    assert posts[0][2]["policy"] == {"ttl": 60000}


async def test_pod_idle_reaper_stop_then_terminate(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf, idle_stop_s=60, idle_terminate_s=600)
    wired(cfg)
    rest = _patch_rest(monkeypatch)
    _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
        mgr = rp._MANAGERS[("rp", "big")]
        assert mgr.is_ready()
        # idle past the STOP tier (not yet terminate) → stop, pod retained
        mgr.last_used -= 100
        assert await rp._reaper_once() == 1
        assert ("stop", POD_ID) in rest.calls
        assert any(p["id"] == POD_ID for p in rest.pods)
        # still "ready" bookkeeping cleared — re-deploy path next request
        assert not mgr.is_ready()
        # idle past the TERMINATE tier → gone
        mgr.ready = True  # simulate still-running bookkeeping
        mgr.last_used -= 700
        assert await rp._reaper_once() == 1
        assert ("terminate", POD_ID) in rest.calls
        assert rest.pods == []


def test_local_only_drops_runpod(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf)
    cfg["aliases"]["priv"] = {"chain": ["local:loc", "rp:big", "rp:fast"],
                              "local_only": True}
    wired(cfg)
    rest = _patch_rest(monkeypatch)
    rec, fake = _patch_cloud(monkeypatch)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "priv",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200
        assert r.json()["served_by"]["backend"] == "local"
        assert rest.kinds() == []   # no pod lifecycle
        assert fake.calls == []     # no serverless call

    _with_embedded(fn)


def _with_embedded(fn):
    """FakeLlama for the local backend (same trick as the other suites)."""
    import types as _types

    mod = _types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            self.n_ctx = kwargs.get("n_ctx", 4096)

        def create_chat_completion(self, **kwargs):
            return {"choices": [{"message": {"content": "local answer"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        fn(None)
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)


def test_healthz_runpod_section(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    _patch_rest(monkeypatch)
    _patch_pod_http(monkeypatch)
    with TestClient(gw_app.app) as client:
        client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
        hz = client.get("/healthz").json()
    entry = hz["runpod"][0]
    assert entry["backend"] == "rp"
    assert entry["model"] == "big"
    assert entry["mode"] == "pod"
    assert entry["state"] == "running"
    assert entry["pod_id"] == POD_ID
    assert entry["base_url"] == f"https://{POD_ID}-8080.proxy.runpod.net"


def test_v1_models_lists_runpod_models(wired, gguf, monkeypatch):
    wired(_rp_config(gguf))
    _patch_rest(monkeypatch)
    with TestClient(gw_app.app) as client:
        ids = [m["id"] for m in client.get("/v1/models").json()["data"]]
    assert "fast" in ids and "big" in ids


def test_pod_model_without_image_dropped(wired, gguf, monkeypatch):
    cfg = _rp_config(gguf)
    cfg["backends"]["rp"]["models"]["broken"] = {"gpu_type_ids": ["X"]}
    wired(cfg)
    _patch_rest(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "rp:broken",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400
