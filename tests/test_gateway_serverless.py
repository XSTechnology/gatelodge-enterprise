"""Gateway v4.5 — serverless deployment.

API shapes verified live 2026-09-20 (create/patch/get/delete =
201/200/200/204): POST /v2/serverless {name, image, type: QUEUE,
gpu: {pools, count}, workers: {min, max, idleTimeout}, scaling:
{QUEUE_DELAY, queueDelay}, env, networkVolumes, dataCenterIds,
timeout}. Pool names come from the v2 GPU catalog per-GPU 'pool'
field (ADA_24 = 4090, AMPERE_80 = A100-80, HOPPER_141 = H200...).
"""
import asyncio
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.catalog as catalog  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402
import gateway.upload as up  # noqa: E402


SL_GPUS = [
    {"id": "NVIDIA GeForce RTX 4090", "memory": 24, "pool": "ADA_24",
     "price": {"community": 0.34, "secure": 0.74},
     "maxCount": {"community": 8, "secure": 8}},
    {"id": "NVIDIA L40S", "memory": 48, "pool": "ADA_48_PRO",
     "price": {"community": 0.69, "secure": 1.09},
     "maxCount": {"community": 8, "secure": 8}},
    {"id": "AMD Instinct MI300X OAM", "memory": 192, "pool": None,
     "price": {"community": 0.5, "secure": 2.39},
     "maxCount": {"community": 0, "secure": 8}},
]
SL_DCS = [{"id": "EUR-IS-1", "region": "EUROPE",
           "networkVolumeTypes": ["STANDARD"]}]
SL_VOLS = [{"id": "vol-sl", "name": "slvol", "dataCenter": "EUR-IS-1",
            "size": 50, "type": "STANDARD"}]


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    def _wire(models=None):
        cfgfile = tmp_path / "gateway_config.json"
        cfgfile.write_text(json.dumps({
            "port": 9911, "bind": "127.0.0.1", "default_alias": "",
            "models_dir": "/nonexistent",
            "backends": {"rp": {
                "type": "runpod", "api_key_env": "RUNPOD_API_KEY_TEST",
                "models": models or {}}}}), encoding="utf-8")
        cfg = gw_config.GatewayConfig(cfgfile)
        monkeypatch.setattr(gw_config, "_CONFIG", cfg)
        monkeypatch.setattr(gw_models, "_ENGINE",
                            gw_models.EmbeddedEngine(cfg))
        monkeypatch.setattr(gw_app, "_LOG_PATH",
                            tmp_path / "gateway_log.jsonl")
        return cfgfile, cfg

    yield _wire


def _patch_live(monkeypatch):
    async def fake_live(env):
        return {"gpus": SL_GPUS, "datacenters": SL_DCS,
                "volumes": SL_VOLS}

    monkeypatch.setattr(gw_app.catalog, "live_options", fake_live)


# ── payload builder + REST wrappers ────────────────────────────────────────

def test_serverless_endpoint_payload():
    p = rp.serverless_endpoint_payload(
        name="gw-sl-m", pool="ADA_24",
        env={"LLAMA_ARG_MODEL": "/workspace/uploads/m.gguf"},
        workers_max=3, idle_timeout=10, queue_delay=5,
        network_volume_id="vol-1", data_center_id="EUR-IS-1",
        disk_gb=46)
    assert p["type"] == "QUEUE"
    assert p["gpu"] == {"pools": ["ADA_24"], "count": 1,
                        "minCudaVersion": "12.8"}
    assert p["workers"] == {"min": 0, "max": 3, "idleTimeout": 10}
    assert p["scaling"] == {"type": "QUEUE_DELAY", "queueDelay": 5}
    assert p["networkVolumes"] == ["vol-1"]
    assert p["dataCenterIds"] == ["EUR-IS-1"]
    assert p["disk"] == 46
    assert p["flashboot"] == "FLASHBOOT"    # default: snapshot cold starts
    assert p["env"]["LLAMA_ARG_MODEL"] == "/workspace/uploads/m.gguf"
    # optional attachments omitted when not given; flashboot opt-out
    p2 = rp.serverless_endpoint_payload(name="n", pool="P", env={},
                                        flashboot=False)
    assert "networkVolumes" not in p2 and "dataCenterIds" not in p2
    assert "disk" not in p2
    assert p2["flashboot"] == "OFF"


async def test_worker_wrappers(monkeypatch):
    calls = []

    async def fake_rest(method, path, api_key_env, json_body=None):
        calls.append((method, path))
        if path.endswith("/workers"):
            return {"workers": [{"id": "w-1", "status": "RUNNING"}]}
        raise AssertionError("unexpected _rest call")

    monkeypatch.setattr(rp, "_rest", fake_rest)
    ws = await rp.list_endpoint_workers("ep-1", "E")
    assert ws == [{"id": "w-1", "status": "RUNNING"}]
    assert calls[0] == ("GET", "/serverless/ep-1/workers")
    # bare-list response shape tolerated
    async def fake_list(method, path, env, json_body=None):
        return [{"id": "w-2"}]
    monkeypatch.setattr(rp, "_rest", fake_list)
    assert await rp.list_endpoint_workers("ep-1", "E") == [{"id": "w-2"}]


async def test_get_worker_logs_bounded_stream(monkeypatch):
    """worker logs STREAM tail-style — _stream_collect must stop at
    max_lines (E2E: a plain GET hangs forever)."""
    seen = {}
    monkeypatch.setenv("E", "test-key")

    async def fake_stream(url, key, read_s=10.0, max_lines=200):
        seen["url"] = url
        seen["key"] = key
        return [f"line {i}" for i in range(max_lines + 50)]

    monkeypatch.setattr(rp, "_stream_collect", fake_stream)
    out = await rp.get_worker_logs("ep-1", "w-9", "E")
    assert seen["url"].endswith("/serverless/ep-1/workers/w-9/logs")
    # the seam is expected to bound itself; wrapper passes through
    assert len(out["lines"]) == 250


async def test_create_patch_get_endpoint_wrappers(monkeypatch):
    calls = []

    async def fake_rest(method, path, api_key_env, json_body=None):
        calls.append((method, path, api_key_env, json_body))
        if method == "POST":
            return {"id": "ep-1", "name": "n"}
        if method == "PATCH":
            return {"id": "ep-1", "patched": True}
        return {"id": "ep-1", "workers": {"max": 2}}

    monkeypatch.setattr(rp, "_rest", fake_rest)
    ep = await rp.create_endpoint({"name": "n"}, "E")
    assert ep["id"] == "ep-1" and calls[0] == ("POST", "/serverless",
                                               "E", {"name": "n"})
    out = await rp.patch_endpoint("ep-1", {"workers": {}}, "E")
    assert out["patched"] is True
    assert calls[1][1] == "/serverless/ep-1"
    got = await rp.get_endpoint("ep-1", "E")
    assert got["workers"]["max"] == 2
    assert calls[2][1] == "/serverless/ep-1"

    async def bad(method, path, env, json_body=None):
        return {"no": "id"}

    monkeypatch.setattr(rp, "_rest", bad)
    with pytest.raises(rp.RunPodError):
        await rp.create_endpoint({}, "E")


# ── upload helpers: safe names + bootstrap ─────────────────────────────────

def test_safe_filename():
    assert up.safe_filename("m-Q4_K_M.gguf") == "m-Q4_K_M.gguf"
    assert up.safe_filename("../evil/x.gguf") == "x.gguf"
    assert up.safe_filename("with space.gguf") is None
    assert up.safe_filename("quote'.gguf") is None
    assert up.safe_filename("") is None


def test_bootstrap_script():
    s = up.bootstrap_script("https://x/m.gguf", "m.gguf")
    assert "python3 /root/receiver.py > /root/receiver.log 2>&1 &" in s
    assert 'curl -fsSL --retry 3 --max-time 5400 ' \
           '-o "/workspace/uploads/m.gguf" \'https://x/m.gguf\'' in s
    assert "sleep infinity" in s
    inner = s.split("printf '%s' '", 1)[1].rsplit(
        "' > /root/receiver.py", 1)[0]
    compile(inner, "receiver.py", "exec")     # valid embedded python


def test_bootstrap_spec_mounts_volume():
    spec = up.bootstrap_spec("g", "https://x/m.gguf", "m.gguf", "vol-1",
                             "EUR-IS-1")
    assert spec["volume_id"] == "vol-1"
    assert spec["data_center_ids"] == ["EUR-IS-1"]   # volume DC-pinned
    assert spec["alias"] == up.BOOTSTRAP_MODEL
    assert spec["ports"] == "8081/http"


async def test_bootstrap_download_retries_gpu_tiers(monkeypatch, tmp_path):
    """The globally cheapest GPU often has no instances in the volume's
    DC — the bootstrap walks the price list until a deploy lands."""
    cfgfile = tmp_path / "gateway_config.json"
    cfgfile.write_text(json.dumps({
        "backends": {"rp": {"type": "runpod", "api_key_env": "E",
                            "models": {}}}}), encoding="utf-8")
    import gateway.config as gw_config

    cfg = gw_config.GatewayConfig(cfgfile)

    async def fake_vols(env):
        return [{"id": "vol-1", "dataCenter": "EUR-IS-1"}]

    async def fake_live(env):
        return {"gpus": [
            {"id": "cheap", "price": {"secure": 0.1}},
            {"id": "next", "price": {"secure": 0.3}}]}

    deploys = []

    async def fake_deploy(payload, env):
        deploys.append(payload)
        if len(deploys) == 1:
            raise rp.RunPodError("no instances in DC")
        return {"id": "pod-bs"}

    async def fake_list(pod_id):
        return [{"name": "m.gguf", "size": 10}]

    async def fake_term(cfg, backend, key):
        return {"ok": True}

    monkeypatch.setattr(rp, "list_volumes", fake_vols)
    monkeypatch.setattr(gw_app.catalog, "live_options", fake_live)
    monkeypatch.setattr(rp, "deploy_pod", fake_deploy)
    monkeypatch.setattr(up, "receiver_list", fake_list)
    monkeypatch.setattr(up, "_terminate_helper", fake_term)
    out = await up.bootstrap_download(cfg, "rp", "https://x/m.gguf",
                                      "m.gguf", 10, "vol-1")
    assert out["volume_id"] == "vol-1"
    assert len(deploys) == 2                       # cheap failed, next OK
    # the successful deploy is DC-pinned to the volume's DC
    assert deploys[1].get("dataCenterIds") == ["EUR-IS-1"] or \
        "EUR-IS-1" in str(deploys[1])


# ── POST /admin/models/deploy-serverless ───────────────────────────────────

def _patch_deploy(monkeypatch, volume_id="vol-new"):
    created = {}

    async def fake_create_volume(name, dc, size, vtype, env):
        created["volume"] = (name, dc, size, vtype)
        return {"id": volume_id}

    async def fake_create_endpoint(payload, env):
        created["endpoint"] = payload
        return {"id": "ep-sl1", "name": payload.get("name"),
                "requestUrls": {"run": "u"}}

    boot = []

    async def fake_bootstrap(cfg, backend, url, filename, size, vol):
        boot.append((url, filename, size, vol))
        return {"filename": filename, "size_bytes": size,
                "volume_id": vol}

    monkeypatch.setattr(gw_app.runpod, "create_volume", fake_create_volume)
    monkeypatch.setattr(gw_app.runpod, "create_endpoint",
                        fake_create_endpoint)
    monkeypatch.setattr(gw_app.upload, "bootstrap_download", fake_bootstrap)
    return created, boot


def test_deploy_serverless_hf_native_download(wired, monkeypatch):
    """source=hf: NO volume, NO bootstrap pod — the worker downloads
    from Hugging Face at cold start (no pod storage for serverless)."""
    cfgfile, _ = wired()
    _patch_live(monkeypatch)
    created, boot = _patch_deploy(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "hf", "repo_id": "Qwen/Qwen2.5-0.5B-Instruct-GGUF",
            "file_name": "qwen2.5-0.5b-instruct-q4_k_m.gguf",
            "size_gb": 0.49, "gpu_id": "NVIDIA GeForce RTX 4090",
            "alias": "slm", "workers_max": 2, "idle_timeout": 10})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["endpoint_id"] == "ep-sl1" and body["pool"] == "ADA_24"
    assert body["volume_id"] == ""             # no volume for hf
    assert boot == []                          # no bootstrap pod ran
    assert "volume" not in created             # no volume created
    ep = created["endpoint"]
    assert ep["gpu"]["pools"] == ["ADA_24"]
    assert ep["workers"] == {"min": 0, "max": 2, "idleTimeout": 10}
    # worker-native HF download env (llama-server --hf-repo/--hf-file)
    assert ep["env"]["LLAMA_ARG_HF_REPO"] == \
        "Qwen/Qwen2.5-0.5B-Instruct-GGUF"
    assert ep["env"]["LLAMA_ARG_HF_FILE"] == \
        "qwen2.5-0.5b-instruct-q4_k_m.gguf"
    assert "LLAMA_ARG_MODEL" not in ep["env"]
    assert "networkVolumes" not in ep           # no volume attached
    assert "dataCenterIds" not in ep            # no DC constraint
    assert ep["disk"] == 20                     # cold-start download fits
    # config persisted: routing works with zero new code
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    spec = raw["backends"]["rp"]["models"]["slm"]
    assert spec["endpoint_id"] == "ep-sl1"
    assert spec["remote_id"] == "slm" and spec["timeout_s"] == 600
    assert spec["serverless"]["pool"] == "ADA_24"
    assert spec["serverless"]["volume_id"] == ""


def test_deploy_serverless_upload_skips_bootstrap(wired, monkeypatch):
    cfgfile, _ = wired()
    _patch_live(monkeypatch)
    created, boot = _patch_deploy(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "upload",
            "file_name": "qwen2.5-0.5b-instruct-q4_k_m.gguf",
            "size_gb": 0.49, "volume_id": "vol-sl",
            "gpu_id": "NVIDIA GeForce RTX 4090"})
    assert r.status_code == 200, r.text
    assert boot == []                       # file already on the volume
    ep = created["endpoint"]
    assert ep["networkVolumes"] == ["vol-sl"]
    assert ep["dataCenterIds"] == ["EUR-IS-1"]   # from the volume's DC
    # serverless workers mount network volumes at /runpod-volume (NOT
    # the pod-view /workspace)
    assert ep["env"]["LLAMA_ARG_MODEL"] == \
        "/runpod-volume/uploads/qwen2.5-0.5b-instruct-q4_k_m.gguf"
    assert "LLAMA_ARG_HF_REPO" not in ep["env"]


def test_deploy_serverless_validation(wired, monkeypatch):
    wired()
    _patch_live(monkeypatch)
    _patch_deploy(monkeypatch)
    with TestClient(gw_app.app) as client:
        # GPU without a serverless pool -> 422
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "hf", "repo_id": "o/r",
            "file_name": "m.gguf", "size_gb": 5,
            "gpu_id": "AMD Instinct MI300X OAM"})
        assert r.status_code == 422
        # model too big for the single picked GPU -> 422
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "hf", "repo_id": "o/r",
            "file_name": "m.gguf", "size_gb": 50,
            "gpu_id": "NVIDIA GeForce RTX 4090"})
        assert r.status_code == 422
        # no gpu given + nothing single-GPU fits -> 422
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "hf", "repo_id": "o/r",
            "file_name": "m.gguf", "size_gb": 500})
        assert r.status_code == 422
        # unsafe filename -> 400
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "hf", "repo_id": "o/r",
            "file_name": "a b.gguf", "size_gb": 5})
        assert r.status_code == 400
        # modelscope/ollama: worker can only fetch from HF -> 400
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "modelscope", "file_name": "m.gguf",
            "size_gb": 5})
        assert r.status_code == 400
        # hf without a repo to download from -> 400
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "hf", "file_name": "m.gguf", "size_gb": 5})
        assert r.status_code == 400
        # upload source without a volume -> 400
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "upload", "file_name": "m.gguf", "size_gb": 5})
        assert r.status_code == 400
        # unknown volume -> 400
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "upload", "file_name": "m.gguf", "size_gb": 5,
            "volume_id": "vol-nope"})
        assert r.status_code == 400


def test_deploy_serverless_auto_gpu_cheapest_fitting(wired, monkeypatch):
    cfgfile, _ = wired()
    _patch_live(monkeypatch)
    created, boot = _patch_deploy(monkeypatch)
    with TestClient(gw_app.app) as client:
        # 40GB model: 4090 (24GB) too small -> auto-lands the L40S
        r = client.post("/admin/models/deploy-serverless", json={
            "source": "upload", "file_name": "big.gguf", "size_gb": 30,
            "volume_id": "vol-sl"})
    assert r.status_code == 200
    assert r.json()["pool"] == "ADA_48_PRO"
    assert created["endpoint"]["gpu"]["pools"] == ["ADA_48_PRO"]


# ── GET /admin/serverless + settings ───────────────────────────────────────

def test_admin_serverless_list_and_settings(wired, monkeypatch):
    wired(models={"slm": {
        "endpoint_id": "ep-1", "remote_id": "slm", "timeout_s": 600,
        "serverless": {"source": "hf", "pool": "ADA_24",
                       "workers_max": 1, "idle_timeout": 5}}})

    async def fake_list(env):
        return [{"id": "ep-1", "name": "gw-sl-slm",
                 "workers": {"min": 0, "max": 1, "idleTimeout": 5},
                 "scaling": {"type": "QUEUE_DELAY", "queueDelay": 4},
                 "gpu": {"pools": ["ADA_24"]}, "flashboot": "OFF"},
                {"id": "ep-other", "name": "byo",
                 "workers": {"max": 3}}]

    monkeypatch.setattr(gw_app.runpod, "list_endpoints", fake_list)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/serverless")
    assert r.status_code == 200
    body = r.json()
    ep = next(e for e in body["endpoints"] if e["model"] == "slm")
    assert ep["exists"] is True
    assert ep["live"]["workers"]["max"] == 1
    assert ep["pool"] == "ADA_24"
    assert body["untracked"][0]["endpoint_id"] == "ep-other"

    # settings: get-first-then-merge PATCH
    patched = {}

    async def fake_get(eid, env):
        return {"workers": {"min": 0, "max": 1, "idleTimeout": 5},
                "scaling": {"type": "QUEUE_DELAY", "queueDelay": 4}}

    async def fake_patch(eid, payload, env):
        patched.update(payload)
        return {"id": eid}

    monkeypatch.setattr(gw_app.runpod, "get_endpoint", fake_get)
    monkeypatch.setattr(gw_app.runpod, "patch_endpoint", fake_patch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/serverless/settings", json={
            "model": "slm", "workers_max": 3, "idle_timeout": 15})
    assert r.status_code == 200
    # whole objects sent, unspecified fields preserved
    assert patched["workers"] == {"min": 0, "max": 3, "idleTimeout": 15}
    assert patched["scaling"] == {"type": "QUEUE_DELAY", "queueDelay": 4}
    # settings persisted in the model metadata
    cfg = gw_config.get_gateway_config()
    assert cfg.backends["rp"]["models"]["slm"]["serverless"][
        "workers_max"] == 3


def test_admin_serverless_workers_and_logs(wired, monkeypatch):
    wired(models={"slm": {
        "endpoint_id": "ep-1", "remote_id": "slm", "timeout_s": 600,
        "serverless": {"source": "hf", "pool": "ADA_24"}}})
    got = []

    async def fake_list(eid, env):
        got.append(("list", eid))
        return [{"id": "w-1", "status": "RUNNING"}]

    async def fake_logs(eid, wid, env):
        got.append(("logs", eid, wid))
        return {"lines": [{"line": "start.sh: llama-server is up"}]}

    monkeypatch.setattr(gw_app.runpod, "list_endpoint_workers", fake_list)
    monkeypatch.setattr(gw_app.runpod, "get_worker_logs", fake_logs)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/serverless/workers?model=slm")
        assert r.status_code == 200
        assert r.json() == {"workers": [{"id": "w-1",
                                         "status": "RUNNING"}]}
        r = client.get("/admin/serverless/workers?model=slm"
                       "&worker_id=w-1")
        assert r.status_code == 200
        assert r.json()["lines"][0]["line"] == "start.sh: llama-server is up"
        # unknown model -> 400
        r = client.get("/admin/serverless/workers?model=nope")
        assert r.status_code == 400
    assert got[0] == ("list", "ep-1")
    assert got[1] == ("logs", "ep-1", "w-1")


def test_config_normalization_preserves_serverless_meta(wired):
    cfgfile, _ = wired(models={"slm": {
        "endpoint_id": "ep-1", "remote_id": "slm", "timeout_s": 600,
        "serverless": {"source": "upload", "pool": "ADA_24",
                       "volume_id": "v"}}})
    cfg = gw_config.get_gateway_config()
    m = cfg.backends["rp"]["models"]["slm"]
    assert m["endpoint_id"] == "ep-1"
    assert m["serverless"]["pool"] == "ADA_24"
    assert m["transport"] == "openai"
