"""LLM Gateway v2 — /admin/runpod/test remote-GPU diagnostic endpoint.

Phased end-to-end test of a runpod POD model with structured data
reported back: pod (find/deploy/resume/reuse + pod info + timings) →
health (per-probe status/latency + served model ids) → chat (content +
usage) → container logs (best-effort tail) → optional stop/terminate
after. A failing pod is a SUCCESSFUL diagnostic: HTTP 200 with ok:false
and per-phase errors, so callers can act on the detail.
"""
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


# ── fixtures + fakes (mirrors test_gateway_runpod.py) ──────────────────────

POD_ID = "pod-ab12cd34"
POD_NAME = "gw-rp-big"


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


def _config(gguf, **pod_over) -> dict:
    pod = {
        "gpu_type_ids": ["NVIDIA RTX A6000"],
        "image": "ghcr.io/ggml-org/llama.cpp:server-cuda",
        "docker_args": "-m /workspace/m.gguf --host 0.0.0.0 --port 8080",
        "ports": "8080/http", "port": 8080,
        "alias": "bigpod", "start_timeout_s": 30,
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


class _FakeRunPod:
    def __init__(self):
        self.pods: list[dict] = []
        self.calls: list[tuple] = []

    async def deploy(self, payload, api_key_env):
        self.calls.append(("deploy", payload))
        pod = {"id": POD_ID, "name": payload.get("name"),
               "status": "RUNNING", "gpuTypeId": "NVIDIA RTX A6000"}
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


def _patch_rest(monkeypatch) -> _FakeRunPod:
    fake = _FakeRunPod()
    monkeypatch.setattr(rp, "deploy_pod", fake.deploy)
    monkeypatch.setattr(rp, "list_pods", fake.list_pods)
    monkeypatch.setattr(rp, "start_pod", fake.start)
    monkeypatch.setattr(rp, "stop_pod", fake.stop)
    monkeypatch.setattr(rp, "terminate_pod", fake.terminate)
    return fake


def _patch_diag(monkeypatch, *, healthy=True, chat_error=False,
                logs=None, logs_error=None):
    """Patch the diagnostic surface: pod_healthy (ensure_ready poll),
    pod_health_details, post_chat, fetch_pod_logs."""
    calls: list[dict] = []

    async def fake_healthy(base_url):
        calls.append({"poll": base_url})
        return healthy

    async def fake_details(base_url):
        calls.append({"details": base_url})
        return {"healthy": healthy,
                "health": {"status": 200 if healthy else 404, "ms": 5},
                "models": {"status": 200 if healthy else 404, "ms": 6,
                           "ids": ["m.gguf"] if healthy else []}}

    async def fake_post(base_url, payload, timeout):
        calls.append({"post": (base_url, payload, timeout)})
        if chat_error:
            raise lsrv.LlamaServerError("llama-server 500: boom")
        return {"id": "cc-1", "object": "chat.completion", "created": 1,
                "model": payload.get("model"),
                "choices": [{"index": 0, "message": {
                    "role": "assistant", "content": "pod answer"},
                    "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 2,
                          "total_tokens": 4}}

    async def fake_logs(pod_id, api_key_env, tail=100):
        calls.append({"logs": (pod_id, tail)})
        if logs_error:
            raise rp.RunPodError(logs_error)
        return logs if logs is not None else [
            {"ts": "2026-09-20T00:00:00Z", "source": "container",
             "line": "Model loaded."}]

    monkeypatch.setattr(rp, "pod_healthy", fake_healthy)
    monkeypatch.setattr(rp, "pod_health_details", fake_details)
    monkeypatch.setattr(rp, "fetch_pod_logs", fake_logs)
    monkeypatch.setattr(lsrv, "post_chat", fake_post)
    return calls


# ── happy paths ────────────────────────────────────────────────────────────

def test_diag_cold_deploy_ok(wired, gguf, monkeypatch):
    wired(_config(gguf))
    rest = _patch_rest(monkeypatch)
    calls = _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["error"] is None
    # pod phase: deployed, pod info whitelisted + id
    pod = body["phases"]["pod"]
    assert pod["ok"] is True
    assert pod["action"] == "deployed"
    assert pod["pod_id"] == POD_ID
    assert pod["pod"]["id"] == POD_ID
    assert pod["pod"]["gpuTypeId"] == "NVIDIA RTX A6000"
    assert "elapsed_s" in pod
    # health phase: per-probe detail + served ids
    health = body["phases"]["health"]
    assert health["ok"] is True
    assert health["base_url"] == f"https://{POD_ID}-8080.proxy.runpod.net"
    assert health["health"]["status"] == 200
    assert health["models"]["ids"] == ["m.gguf"]
    # chat phase: content + usage through the pod's proxy URL
    chat = body["phases"]["chat"]
    assert chat["ok"] is True
    assert chat["content"] == "pod answer"
    assert chat["usage"]["total_tokens"] == 4
    post = [c["post"] for c in calls if "post" in c][0]
    assert post[0] == f"https://{POD_ID}-8080.proxy.runpod.net"
    assert post[1]["model"] == "bigpod"  # the pod model's alias
    assert post[1]["max_tokens"] == 16   # payload default
    assert post[1]["temperature"] == 0
    # logs fetched for the pod
    assert body["logs"][0]["line"] == "Model loaded."
    assert [c["logs"] for c in calls if "logs" in c][0] == (POD_ID, 100)
    # default after=keep: no lifecycle action
    assert "stop" not in rest.kinds() and "terminate" not in rest.kinds()
    assert "after" not in body
    assert "elapsed_s" in body


def test_diag_reuse_second_call(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_rest(monkeypatch)
    _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        r1 = client.post("/admin/runpod/test",
                         json={"backend": "rp", "model": "big"})
        r2 = client.post("/admin/runpod/test",
                         json={"backend": "rp", "model": "big"})
    assert r1.json()["phases"]["pod"]["action"] == "deployed"
    assert r2.json()["phases"]["pod"]["action"] == "reused"
    assert r2.json()["ok"] is True


def test_diag_probe_only_existing_pod(wired, gguf, monkeypatch):
    wired(_config(gguf))
    rest = _patch_rest(monkeypatch)
    rest.pods.append({"id": "pod-77", "name": POD_NAME,
                      "status": "RUNNING", "gpuTypeId": "NVIDIA RTX A4000"})
    _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big",
                              "deploy": False})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["phases"]["pod"]["action"] == "probed"
    assert body["phases"]["pod"]["state"] == "RUNNING"
    assert body["phases"]["pod"]["pod_id"] == "pod-77"
    # probe-only NEVER deploys or starts
    kinds = rest.kinds()
    assert "deploy" not in kinds and "start" not in kinds


def test_diag_probe_only_no_pod(wired, gguf, monkeypatch):
    wired(_config(gguf))
    rest = _patch_rest(monkeypatch)
    _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big",
                              "deploy": False})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert "not deployed" in body["phases"]["pod"]["error"]
    assert body["phases"]["chat"]["skipped"] == "no base url"
    assert "deploy" not in rest.kinds()  # probe-only holds the line


def test_diag_probe_only_stopped_pod(wired, gguf, monkeypatch):
    wired(_config(gguf))
    rest = _patch_rest(monkeypatch)
    rest.pods.append({"id": "pod-88", "name": POD_NAME, "status": "STOPPED"})
    # healthy=False: a stopped pod's proxy is dead in reality
    _patch_diag(monkeypatch, healthy=False)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big",
                              "deploy": False})
    body = r.json()
    assert body["ok"] is False
    assert "STOPPED" in body["phases"]["pod"]["error"]
    # still probed for diagnostics, chat correctly skipped
    assert body["phases"]["health"]["ok"] is False
    assert body["phases"]["chat"]["skipped"] == "pod not healthy"
    assert "start" not in rest.kinds()  # probe-only never starts


# ── failure paths report diagnostics, not 5xx ──────────────────────────────

def test_diag_ready_timeout_reports_error_and_logs(wired, gguf, monkeypatch):
    wired(_config(gguf))  # spec start_timeout_s: 30 (big)
    rest = _patch_rest(monkeypatch)
    calls = _patch_diag(monkeypatch, healthy=False)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big",
                              "timeout_s": 0.1})  # override tightens it
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert "still booting after 0.1s" in body["phases"]["pod"]["error"]
    assert body["phases"]["chat"]["skipped"] == "pod not healthy"
    # pod WAS deployed (id known) — logs still fetched for the report
    assert body["phases"]["pod"]["pod_id"] == POD_ID
    assert body["logs"] and body["logs"][0]["line"] == "Model loaded."
    assert "deploy" in rest.kinds()


def test_diag_chat_failure(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_rest(monkeypatch)
    _patch_diag(monkeypatch, chat_error=True)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big"})
    body = r.json()
    assert body["ok"] is False
    assert "500" in body["phases"]["chat"]["error"]
    assert body["phases"]["pod"]["ok"] is True      # earlier phases intact
    assert body["phases"]["health"]["ok"] is True


def test_diag_logs_error_is_soft(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_rest(monkeypatch)
    _patch_diag(monkeypatch, logs_error="runpod pod logs 404: no logs")
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big"})
    body = r.json()
    assert body["ok"] is True                       # chat unaffected
    assert body["logs"] == []
    assert "404" in body["logs_error"]


# ── after-action ───────────────────────────────────────────────────────────

def test_diag_after_terminate(wired, gguf, monkeypatch):
    wired(_config(gguf))
    rest = _patch_rest(monkeypatch)
    _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big",
                              "after": "terminate"})
    body = r.json()
    assert body["ok"] is True
    assert body["after"] == "terminate"
    assert "terminate" in rest.kinds()
    assert rest.pods == []  # fake removes it


# ── request validation ─────────────────────────────────────────────────────

def test_diag_validation_errors(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_rest(monkeypatch)
    _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        for payload, needle in [
            ({}, "backend and model required"),
            ({"backend": "rp"}, "backend and model required"),
            ({"backend": "local", "model": "loc"}, "not a runpod backend"),
            ({"backend": "rp", "model": "nope"}, "unknown model"),
            ({"backend": "rp", "model": "fast"},
             "serverless endpoint"),  # endpoint_id model has no pod
            ({"backend": "rp", "model": "big", "after": "zap"},
             "after must be"),
        ]:
            r = client.post("/admin/runpod/test", json=payload)
            assert r.status_code == 400, (payload, r.text)
            assert needle in r.json()["error"]["message"], (payload, r.text)


def test_diag_custom_prompt_and_tail(wired, gguf, monkeypatch):
    wired(_config(gguf))
    _patch_rest(monkeypatch)
    calls = _patch_diag(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/test",
                        json={"backend": "rp", "model": "big",
                              "prompt": "ping", "max_tokens": 7,
                              "log_tail": 5})
    assert r.json()["ok"] is True
    post = [c["post"] for c in calls if "post" in c][0]
    assert post[1]["messages"][0]["content"] == "ping"
    assert post[1]["max_tokens"] == 7
    assert [c["logs"] for c in calls if "logs" in c][0] == (POD_ID, 5)
