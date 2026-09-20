"""LLM Gateway v2 — cold start, capacity fallback, provider panel.

Cold start: a chat request to a stopped pod auto-starts it (existing
ensure_ready); the NEW parts are visibility — non-streaming holds are
capped and end in a structured 503 warming_up + Retry-After, streaming
emits SSE comment keep-alives while booting, and manager status
surfaces a warming-up state for the console.

Capacity fallback: the deploy wizard tries the user's GPU combo, then
the recipe's other recommended combos (same DC — volumes are pinned).

Provider layer: registry + RunPod account (volumes with usage markers,
pod billing) + volume create/delete with an in-use guard.
"""
import asyncio
import contextlib
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.catalog as catalog  # noqa: E402
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
    rp.reset_status_cache()


CONFIG = {
    "port": 9911, "bind": "127.0.0.1", "default_alias": "",
    "models_dir": "/nonexistent",
    "backends": {
        "rp": {"type": "runpod", "api_key_env": "RUNPOD_API_KEY_TEST",
               "models": {
                   "big": {"image": "x", "port": 8080,
                           "pod_id": "pod-live1", "alias": "big",
                           "start_timeout_s": 30},
               }},
    },
    "aliases": {},
}

GPUS = [
    {"id": "NVIDIA A100-SXM4-80GB", "memory": 80,
     "price": {"community": 1.39, "secure": 1.59},
     "maxCount": {"community": 8, "secure": 8}},
    {"id": "NVIDIA H200", "memory": 141,
     "price": {"community": 3.59, "secure": 4.59},
     "maxCount": {"community": 8, "secure": 8}},
    {"id": "NVIDIA RTX 4090", "memory": 24,
     "price": {"community": 0.34, "secure": 0.74},
     "maxCount": {"community": 8, "secure": 8}},
]
DCS = [{"id": "EUR-IS-1", "region": "EUROPE",
        "networkVolumeTypes": ["STANDARD"]}]
VOLS = [{"id": "vol1", "name": "glm53-EUR-IS-1", "dataCenter": "EUR-IS-1",
         "size": 200, "type": "STANDARD"}]


def _patch_options(monkeypatch):
    async def fake_gpus(api_key_env):
        return GPUS

    async def fake_dcs(api_key_env):
        return DCS

    async def fake_vols(api_key_env):
        return VOLS

    monkeypatch.setattr(rp, "list_gpu_catalog", fake_gpus)
    monkeypatch.setattr(rp, "list_datacenter_catalog", fake_dcs)
    monkeypatch.setattr(rp, "list_volumes", fake_vols)


# ── Warming state ───────────────────────────────────────────────────────────

def test_manager_warming_flag_lifecycle(monkeypatch):
    mgr = rp._PodManager("rp", "big", "E",
                         {"port": 8080, "start_timeout_s": 30})
    monkeypatch.setattr(rp, "_MANAGERS", {("rp", "big"): mgr})
    assert mgr.warming is False
    mgr.warming = True
    mgr.warm_started_s = time.monotonic() - 12.0
    mgr.pod_id = "pod-x"
    st = rp.status()
    row = next(r for r in st if r["pod_id"] == "pod-x")
    assert row["state"] == "warming-up"
    assert row["warming"] is True
    assert row["warm_s"] >= 11.0
    mgr.warming = False
    mgr.ready = True
    row = next(r for r in rp.status() if r["pod_id"] == "pod-x")
    assert row["state"] == "running"
    assert row["warming"] is False


def test_warming_up_exception_is_runpod_error():
    assert issubclass(rp.RunPodWarmingUp, rp.RunPodError)


def test_sse_keepalive_during_cold_start(wired, monkeypatch):
    """Streaming chat while the pod boots: SSE comment keep-alives keep
    the connection visibly alive instead of hanging silently."""
    wired(CONFIG)
    monkeypatch.setattr(gw_app, "RUNPOD_SSE_KEEPALIVE_S", 0.05)

    class FakeMgr:
        spec = {"port": 8080, "alias": "big", "start_timeout_s": 30}
        pod_status = "STARTING"
        pod_id = "pod-live1"

        def is_ready(self):
            return False

        async def ensure_ready(self, timeout_s=None):
            await asyncio.sleep(0.25)
            return "https://pod-live1-8080.proxy.runpod.net"

        @contextlib.asynccontextmanager
        async def request(self):
            yield self

    monkeypatch.setattr(rp, "get_manager", lambda *a, **k: FakeMgr())

    async def fake_stream_chat(base, payload, timeout_s):
        yield json.dumps({"id": "x", "object": "chat.completion.chunk",
                          "choices": [{"index": 0, "delta": {"content": "hi"},
                                       "finish_reason": None}]})
        yield "[DONE]"

    monkeypatch.setattr(gw_app.llamaserver, "stream_chat", fake_stream_chat)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "big", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            body = "".join(line for line in r.iter_lines())
    assert "runpod pod warming up" in body
    assert "hi" in body


# ── Capacity fallback ───────────────────────────────────────────────────────

def test_candidate_combos_order_and_filtering():
    recipe = catalog.find_recipe("glm53-flash")
    # user picks H200x2 first; recipe's A100x3 comes second; the
    # user's other entries never duplicate
    combos = catalog.candidate_combos(
        recipe, {"gpu_id": "NVIDIA H200", "gpu_count": 2, "cloud": "secure"},
        GPUS)
    assert combos[0] == {"gpu_id": "NVIDIA H200", "gpu_count": 2,
                         "est_cost_per_hr": 9.18}
    assert combos[1] == {"gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
                         "est_cost_per_hr": 4.77}
    # same combo chosen directly: no duplicates
    combos = catalog.candidate_combos(
        recipe, {"gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
                 "cloud": "secure"}, GPUS)
    assert len(combos) == 2
    # undersized GPU choice is dropped; the recipe's own options remain
    combos = catalog.candidate_combos(
        recipe, {"gpu_id": "NVIDIA RTX 4090", "gpu_count": 8,
                 "cloud": "secure"}, GPUS)
    assert combos == [
        {"gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
         "est_cost_per_hr": 4.77},
        {"gpu_id": "NVIDIA H200", "gpu_count": 2,
         "est_cost_per_hr": 9.18}]


def test_deploy_fallback_second_combo_lands(wired, monkeypatch):
    cfgfile, _, _ = wired(CONFIG)
    _patch_options(monkeypatch)
    attempts = []

    async def fake_deploy(payload, api_key_env):
        attempts.append(payload["gpu"])
        if len(attempts) == 1:
            raise rp.RunPodError("runpod POST /pods -> 400: not enough "
                                 "free GPUs on the host machine")
        return {"id": "pod-fb2", "name": "gw-rp-glm53-flash"}

    monkeypatch.setattr(rp, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/catalog/deploy", json={
            "recipe_id": "glm53-flash",
            "gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
            "cloud": "secure", "data_center_id": "EUR-IS-1",
            "volume_mode": "existing", "volume_id": "vol1",
            "ctx_key": "64k", "container_disk_gb": 200,
        })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["pod_id"] == "pod-fb2"
    assert body["landed"]["gpu_id"] == "NVIDIA H200"
    assert body["landed"]["gpu_count"] == 2
    assert len(body["tried"]) == 1
    assert "not enough free GPUs" in body["tried"][0]["error"]
    # spec persisted with the LANDED combo, not the failed first choice
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    spec = raw["backends"]["rp"]["models"]["glm53-flash"]
    assert spec["gpu_type_ids"] == ["NVIDIA H200"]
    assert spec["gpu_count"] == 2


def test_deploy_fallback_all_fail_reports_tried(wired, monkeypatch):
    wired(CONFIG)
    _patch_options(monkeypatch)

    async def fake_deploy(payload, api_key_env):
        raise rp.RunPodError("runpod POST /pods -> 400: not enough "
                             "free GPUs on the host machine")

    monkeypatch.setattr(rp, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/catalog/deploy", json={
            "recipe_id": "glm53-flash",
            "gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
            "cloud": "secure", "data_center_id": "EUR-IS-1",
            "volume_mode": "existing", "volume_id": "vol1",
            "ctx_key": "64k", "container_disk_gb": 200,
        })
    assert r.status_code == 502, r.text
    body = r.json()
    assert body["error"]["code"] == "runpod_deploy_failed"
    assert len(body["tried"]) == 2
    assert all("not enough free GPUs" in t["error"] for t in body["tried"])


# ── ROCm recipe ─────────────────────────────────────────────────────────────

def test_mi300x_recipe_rocm_script():
    r = catalog.find_recipe("glm53-flash-mi300x")
    assert r is not None and r.get("experimental") is True
    assert r["gpu_options"] == [{"gpu": "AMD Instinct MI300X OAM", "count": 1}]
    s = catalog.render_script(r, r["ctx_options"][0])
    assert "-DGGML_HIP=ON" in s and "-DAMDGPU_TARGETS=gfx942" in s
    assert "/usr/local/cuda" not in s          # no CUDA toolchain on AMD
    assert "--parallel 2 --ctx-size 65536" in s  # 32k ctx (64k won't fit)
    # the cuda recipe still builds CUDA
    c = catalog.render_script(catalog.find_recipe("glm53-flash"),
                              catalog.find_recipe("glm53-flash")["ctx_options"][0])
    assert "-DGGML_CUDA=ON" in c and "/usr/local/cuda" in c


# ── Providers + account + volumes ───────────────────────────────────────────

def test_providers_registry(wired):
    wired(CONFIG)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/providers")
    assert r.status_code == 200
    provs = r.json()["providers"]
    assert [p["id"] for p in provs] == ["runpod"]
    assert "volumes" in provs[0]["features"]


def test_provider_runpod_account(wired, monkeypatch):
    wired(CONFIG)
    _patch_options(monkeypatch)

    async def fake_billing(env, days=7):
        return {"pods": [{"podId": "pod-live1", "cost": "1.59", "time": "t"}]}

    monkeypatch.setattr(rp, "list_pod_billing", fake_billing)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/providers/runpod/account")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["volumes"] == VOLS
    assert body["billing"]["pods"][0]["podId"] == "pod-live1"


def test_volume_create_and_delete(wired, monkeypatch):
    cfgfile, _, _ = wired(CONFIG)
    _patch_options(monkeypatch)
    made = []

    async def fake_create(name, dc, size, vtype, env):
        made.append((name, dc, size, vtype))
        return {"id": "vol-new9", "name": name}

    async def fake_delete(volume_id, env):
        made.append(("delete", volume_id))
        return {}

    monkeypatch.setattr(rp, "create_volume", fake_create)
    monkeypatch.setattr(rp, "delete_volume", fake_delete)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/volumes", json={
            "action": "create", "name": "test-vol", "data_center_id": "EUR-IS-1",
            "size_gb": 100, "volume_type": "STANDARD"})
        assert r.status_code == 200 and r.json()["volume"]["id"] == "vol-new9"
        r = client.post("/admin/runpod/volumes", json={
            "action": "delete", "volume_id": "vol-new9"})
        assert r.status_code == 200
    assert made == [("test-vol", "EUR-IS-1", 100, "STANDARD"),
                    ("delete", "vol-new9")]


def test_volume_delete_in_use_guard(wired, monkeypatch):
    cfg = json.loads(json.dumps(CONFIG))
    cfg["backends"]["rp"]["models"]["big"]["volume_id"] = "vol1"
    wired(cfg)
    _patch_options(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/volumes", json={
            "action": "delete", "volume_id": "vol1"})
    assert r.status_code == 409
    assert "rp:big" in r.json()["error"]["message"]


def test_runpod_logs_endpoint(wired, monkeypatch):
    cfg = json.loads(json.dumps(CONFIG))
    cfg["backends"]["rp"]["models"]["big"]["pod_id"] = "pod-live1"
    wired(cfg)
    seen = []

    async def fake_logs(pod_id, api_key_env, tail=100):
        seen.append((pod_id, tail))
        return [{"ts": "t1", "source": "container",
                 "line": "[boot] downloading model"}]

    monkeypatch.setattr(rp, "fetch_pod_logs", fake_logs)
    with TestClient(gw_app.app) as client:
        # by model (pod_id resolved from config spec)
        r = client.post("/admin/runpod/logs", json={"model": "big"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["pod_id"] == "pod-live1"
        assert body["lines"][0]["line"] == "[boot] downloading model"
        assert seen == [("pod-live1", 120)]
        # explicit pod_id + tail clamped
        r = client.post("/admin/runpod/logs", json={
            "pod_id": "pod-x", "tail": 9999})
        assert r.status_code == 200
        assert seen[-1] == ("pod-x", 400)
        # nothing to resolve → 400
        r = client.post("/admin/runpod/logs", json={"model": "nope"})
        assert r.status_code == 400


# ── Config-driven live status (boot-watch modal feeds on /healthz) ─────────

def test_refresh_status_materializes_config_models(wired, monkeypatch):
    """A freshly deployed model has NO manager until a request touches
    it — refresh_status must materialize it from config so the boot
    modal's state chip advances (the deploy-side gap: chip stuck on
    'starting', chat test never fired)."""
    cfg = json.loads(json.dumps(CONFIG))
    # strip the spec pod_id — resolution must go through find-by-NAME
    cfg["backends"]["rp"]["models"]["big"].pop("pod_id", None)
    wired(cfg)

    async def fake_pods(api_key_env):
        return [{"id": "pod-new77", "name": "gw-rp-big",
                 "status": "RUNNING"}]

    async def fake_healthy(base_url):
        return True

    monkeypatch.setattr(rp, "list_pods", fake_pods)
    monkeypatch.setattr(rp, "pod_healthy", fake_healthy)
    with TestClient(gw_app.app) as client:
        r = client.get("/healthz")
    assert r.status_code == 200
    row = next(x for x in r.json()["runpod"] if x["model"] == "big")
    assert row["pod_id"] == "pod-new77"   # adopted via pod NAME
    assert row["state"] == "running"      # RUNNING + healthy → ready
    assert row["base_url"]


def test_refresh_status_terminated_pod(wired, monkeypatch):
    cfg = json.loads(json.dumps(CONFIG))
    cfg["backends"]["rp"]["models"]["big"]["pod_id"] = "pod-gone9"
    wired(cfg)

    async def fake_pods(api_key_env):
        return []  # pod terminated — not in the list

    async def fake_healthy(base_url):
        raise AssertionError("must not probe a pod that is not RUNNING")

    monkeypatch.setattr(rp, "list_pods", fake_pods)
    monkeypatch.setattr(rp, "pod_healthy", fake_healthy)
    with TestClient(gw_app.app) as client:
        r = client.get("/healthz")
    row = next(x for x in r.json()["runpod"] if x["model"] == "big")
    assert row["state"] == "TERMINATED"
    assert row["base_url"] is None


def test_refresh_status_stopped_pod_not_probed(wired, monkeypatch):
    cfg = json.loads(json.dumps(CONFIG))
    cfg["backends"]["rp"]["models"]["big"]["pod_id"] = "pod-live1"
    wired(cfg)

    async def fake_pods(api_key_env):
        return [{"id": "pod-live1", "name": "gw-rp-big",
                 "status": "EXITED"}]

    async def fake_healthy(base_url):
        raise AssertionError("must not probe an EXITED pod")

    monkeypatch.setattr(rp, "list_pods", fake_pods)
    monkeypatch.setattr(rp, "pod_healthy", fake_healthy)
    with TestClient(gw_app.app) as client:
        r = client.get("/healthz")
    row = next(x for x in r.json()["runpod"] if x["model"] == "big")
    assert row["state"] == "EXITED"
    assert row["base_url"] is None
