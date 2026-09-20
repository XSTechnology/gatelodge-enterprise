"""LLM Gateway v2 — /admin/runpod/inventory enrichment.

The console's RunPod servers widget refreshes from this endpoint, so the
pods list must carry live detail (gpu, cost/hr, uptime, desired state),
not just id/name/status — with the status→desiredStatus fallback kept.
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


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


CONFIG = {
    "port": 9911, "bind": "127.0.0.1", "default_alias": "",
    "models_dir": "/nonexistent",
    "backends": {
        "rp": {"type": "runpod", "api_key_env": "RUNPOD_API_KEY_TEST",
               "models": {
                   "big": {"image": "x", "port": 8080},
                   "pinned": {"pod_id": "pod-live1", "port": 8080},
                   "gone": {"pod_id": "pod-dead9", "port": 8080},
                   "ep": {"endpoint_id": "ep-ok"},
               }},
    },
    "aliases": {},
}

PODS = [
    {"id": "pod-live1", "name": "gw-rp-big", "status": "RUNNING",
     "desiredStatus": "RUNNING", "gpuTypeId": "NVIDIA A100-SXM4-80GB",
     "costPerHour": 1.59, "uptimeSeconds": 3661,
     "imageName": "runpod/pytorch"},
    # pods-list schema: gpu object, string cost, startedAt, runtime, actions
    {"id": "pod-a2", "name": "slim_indigo_whippet", "status": "RUNNING",
     "gpu": {"count": 5, "id": "NVIDIA L40", "memory": 1251},
     "cost": "4.1", "image": "runpod/pytorch:1.0.2-cu1281",
     "startedAt": "2026-09-20T06:11:16.12Z",
     "actions": ["stop", "restart", "terminate"],
     "runtime": {"cpu": {"util": 0},
                 "gpus": [{"memoryUtil": 0, "util": 0},
                          {"memoryUtil": 12, "util": 40}]}},
    # no status → falls back to desiredStatus
    {"id": "pod-a3", "name": "royal_jade_urial", "desiredStatus": "STOPPED",
     "gpuTypeId": "NVIDIA A100-SXM4-80GB", "costPerHour": 1.59},
]
ENDPOINTS = [{"id": "ep-ok", "name": "smollm-ep", "workersMax": 2}]


def _patch(monkeypatch):
    async def fake_pods(api_key_env):
        return PODS

    async def fake_eps(api_key_env):
        return ENDPOINTS

    monkeypatch.setattr(rp, "list_pods", fake_pods)
    monkeypatch.setattr(rp, "list_endpoints", fake_eps)


def test_inventory_pods_enriched(wired, monkeypatch):
    wired(CONFIG)
    _patch(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/inventory")
    assert r.status_code == 200, r.text
    body = r.json()
    by_id = {p["id"]: p for p in body["pods"]}
    # live detail surfaced for the console widget
    live = by_id["pod-live1"]
    assert live["gpuTypeId"] == "NVIDIA A100-SXM4-80GB"
    assert live["costPerHour"] == 1.59
    assert live["uptimeSeconds"] == 3661
    assert live["imageName"] == "runpod/pytorch"
    assert live["status"] == "RUNNING"
    # status falls back to desiredStatus when absent
    assert by_id["pod-a3"]["status"] == "STOPPED"
    # pods-list schema normalized: gpu.id/count, string cost, startedAt,
    # runtime utilization, image → imageName
    a2 = by_id["pod-a2"]
    assert a2["gpuTypeId"] == "NVIDIA L40"
    assert a2["gpuCount"] == 5
    assert a2["costPerHour"] == 4.1
    assert isinstance(a2["uptimeSeconds"], int) and a2["uptimeSeconds"] >= 0
    assert a2["imageName"] == "runpod/pytorch:1.0.2-cu1281"
    assert a2["gpuUtil"] == 40
    assert a2["gpuMemUtil"] == 12
    assert a2["actions"] == ["stop", "restart", "terminate"]
    # stale detection intact: only pod-dead9 is gone (ep-ok is live)
    assert body["stale"] == [{"backend": "rp", "model": "gone",
                              "type": "pod", "id": "pod-dead9"}]
    assert body["endpoints"][0]["id"] == "ep-ok"
    assert body["endpoints"][0]["referenced_by"] == ["rp:ep"]


def test_action_endpoint_delete(wired, monkeypatch):
    wired(CONFIG)
    calls = []

    async def fake_delete(endpoint_id, api_key_env):
        calls.append((endpoint_id, api_key_env))
        return {}

    monkeypatch.setattr(rp, "delete_endpoint", fake_delete)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/action",
                        json={"endpoint_id": "ep-ok", "action": "delete"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["endpoint_id"] == "ep-ok"
    assert calls == [("ep-ok", "RUNPOD_API_KEY_TEST")]
    # validation: endpoints only support delete; ids are required
    with TestClient(gw_app.app) as client:
        for payload in (
                {"endpoint_id": "ep-ok", "action": "start"},
                {"endpoint_id": "ep-ok", "action": "stop"},
                {"endpoint_id": "ep-ok"},
                {"action": "delete"},
                {"pod_id": "", "action": "start"},
                {"pod_id": "pod-live1", "action": "reboot"}):
            r = client.post("/admin/runpod/action", json=payload)
            assert r.status_code == 400, (payload, r.text)


def test_inventory_error_path(wired, monkeypatch):
    wired(CONFIG)

    async def boom(api_key_env):
        raise rp.RunPodError("runpod api key env var 'X' is not set")

    monkeypatch.setattr(rp, "list_pods", boom)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/inventory")
    assert r.status_code == 200
    body = r.json()
    assert body["error"]
    assert body["pods"] == []
