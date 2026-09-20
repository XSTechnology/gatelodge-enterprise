"""LLM Gateway v2 — model catalogue + deploy wizard.

Recipes must be well-formed and grounded (VRAM ≥ model size, ctx
presets labeled with the build-correct parallel/ctx_total pair);
validate_selection enforces live-catalog reality (VRAM fit, maxCount,
cloud availability, DC volume support, volume DC-pinning); build_spec
reproduces the proven glm53 deployment shape (the recipe must never
drift from what was live-verified on 3× A100); the endpoints wire it
together with live options faked at the runpod layer.
"""
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


@pytest.fixture(autouse=True)
def _no_catalog_cache():
    catalog.clear_cache()
    yield
    catalog.clear_cache()


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
               "models": {}},
    },
    "aliases": {},
}

GPUS = [
    {"id": "NVIDIA GeForce RTX 4090", "memory": 24,
     "price": {"community": 0.34, "secure": 0.74},
     "maxCount": {"community": 8, "secure": 8}},
    {"id": "NVIDIA L40", "memory": 48,
     "price": {"community": 0.69, "secure": 0.82},
     "maxCount": {"community": 10, "secure": 8}},
    {"id": "NVIDIA A100-SXM4-80GB", "memory": 80,
     "price": {"community": 1.39, "secure": 1.59},
     "maxCount": {"community": 8, "secure": 8}},
    {"id": "NVIDIA H200", "memory": 141,
     "price": {"community": 3.59, "secure": 4.59},
     "maxCount": {"community": 8, "secure": 8}},
    # community cloud not offered (price 0 / maxCount 0)
    {"id": "NVIDIA B200", "memory": 180,
     "price": {"community": 0, "secure": 6.79},
     "maxCount": {"community": 0, "secure": 8}},
]
DCS = [
    {"id": "EUR-IS-1", "region": "EUROPE",
     "networkVolumeTypes": ["STANDARD"]},
    {"id": "CA-MTL-4", "region": "NORTH_AMERICA",
     "networkVolumeTypes": ["STANDARD", "HIGH_PERFORMANCE"]},
    {"id": "US-KS-2", "region": "NORTH_AMERICA",
     "networkVolumeTypes": []},
]
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


# ── Recipe well-formedness ──────────────────────────────────────────────────

def test_recipes_well_formed():
    assert len(catalog.CATALOG) >= 5
    for r in catalog.CATALOG:
        for key in ("id", "model", "quant", "build", "image", "size_gb",
                    "vram_min_gb", "gpu_options", "disk_gb_min",
                    "ctx_options", "ctx_default", "script",
                    "start_timeout_s"):
            assert r.get(key), f"{r.get('id')}: missing {key}"
        assert r["vram_min_gb"] >= r["size_gb"]
        assert r["disk_gb_min"] >= r["size_gb"]
        assert r["script"] in ("mainline", "glm5next_volume")
        if r["build"] == "glm5next":
            assert r["script"] == "glm5next_volume"
        keys = [o["key"] for o in r["ctx_options"]]
        assert len(keys) == len(set(keys)), "duplicate ctx keys"
        assert r["ctx_default"] in keys
        for o in r["ctx_options"]:
            assert o["parallel"] >= 1 and o["ctx_total"] >= 8192
        if r.get("volume"):
            assert r["volume"]["size_gb_min"] >= r["size_gb"]
        assert len(r["gpu_options"]) >= 1


def test_recipes_unique_ids():
    ids = [r["id"] for r in catalog.CATALOG]
    assert len(ids) == len(set(ids))


# ── validate_selection ──────────────────────────────────────────────────────

def _glm53():
    return catalog.find_recipe("glm53-flash")


GLM53_OK = {
    "gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
    "cloud": "secure", "data_center_id": "EUR-IS-1",
    "volume_mode": "existing", "volume_id": "vol1",
    "ctx_key": "64k", "container_disk_gb": 200,
}


def test_validate_ok():
    assert catalog.validate_selection(_glm53(), GLM53_OK, GPUS, DCS, VOLS) == []


def test_validate_vram_under_provisioned():
    c = dict(GLM53_OK, gpu_count=2)  # 2×80 = 160 < 210
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("VRAM" in e for e in errs)


def test_validate_community_unavailable():
    # B200 not offered on the community cloud
    c = dict(GLM53_OK, gpu_id="NVIDIA B200", gpu_count=2, cloud="community")
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("community" in e for e in errs)


def test_validate_dc_without_volume_support():
    c = dict(GLM53_OK, data_center_id="US-KS-2", volume_mode="create",
             volume_size_gb=200)
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("does not support" in e for e in errs)


def test_validate_volume_type_not_in_dc():
    c = dict(GLM53_OK, data_center_id="EUR-IS-1", volume_mode="create",
             volume_size_gb=200, volume_type="HIGH_PERFORMANCE")
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("HIGH_PERFORMANCE" in e for e in errs)


def test_validate_existing_volume_dc_pin():
    # volume vol1 lives in EUR-IS-1; pod requested in CA-MTL-4
    c = dict(GLM53_OK, data_center_id="CA-MTL-4")
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("DC-pinned" in e or "must run there" in e for e in errs)


def test_validate_volume_required():
    # glm53 needs a volume; 'none' mode rejected
    c = dict(GLM53_OK, volume_mode="none")
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("requires a network volume" in e for e in errs)


def test_validate_disk_and_ctx():
    c = dict(GLM53_OK, container_disk_gb=50)
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("disk" in e for e in errs)
    c = dict(GLM53_OK, ctx_key="999k")
    errs = catalog.validate_selection(_glm53(), c, GPUS, DCS, VOLS)
    assert any("context preset" in e for e in errs)


def test_validate_volume_unneeded_recipe():
    # small model + existing volume → rejected
    r = catalog.find_recipe("qwen3-8b")
    c = {"gpu_id": "NVIDIA GeForce RTX 4090", "gpu_count": 1,
         "cloud": "secure", "volume_mode": "existing", "volume_id": "vol1",
         "container_disk_gb": 20}
    errs = catalog.validate_selection(r, c, GPUS, DCS, VOLS)
    assert any("does not use a volume" in e for e in errs)


# ── build_spec ──────────────────────────────────────────────────────────────

def test_build_spec_glm53_matches_proven_recipe():
    spec = catalog.build_spec(_glm53(), GLM53_OK)
    assert spec["image"] == "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404"
    assert spec["docker_entrypoint"] == ["sh", "-c"]
    s = spec["docker_script"]
    # the three-phase boot: build-if-missing, download-if-missing,
    # copy-to-local, then serve with explicit parallel/ctx
    assert "glm5next/upstream" in s
    assert "snapshot_download" in s
    assert "/root/model/.copied" in s
    assert "--parallel 2 --ctx-size 131072" in s
    assert "--jinja" in s
    assert "--no-mmap" not in s  # glm5next build rejects it
    assert spec["gpu_type_ids"] == ["NVIDIA A100-SXM4-80GB"]
    assert spec["gpu_count"] == 3
    assert spec["container_disk_gb"] == 200
    assert spec["volume_id"] == "vol1"
    assert spec["data_center_ids"] == ["EUR-IS-1"]
    assert spec["start_timeout_s"] == 3600
    assert "spot" not in spec


def test_build_spec_mainline():
    r = catalog.find_recipe("qwen3-8b")
    spec = catalog.build_spec(r, {"gpu_id": "NVIDIA GeForce RTX 4090",
                                  "gpu_count": 1, "cloud": "secure",
                                  "container_disk_gb": 20})
    s = spec["docker_script"]
    assert "unsloth/Qwen3-8B-GGUF" in s
    assert "llama-server" in s
    # mainline semantics: ctx-size is TOTAL ÷ parallel → 2×32k
    assert "--parallel 2 --ctx-size 65536" in s
    assert "volume_id" not in spec
    assert "data_center_ids" not in spec
    assert spec["image"] == "ghcr.io/ggml-org/llama.cpp:server-cuda"
    assert "spot" not in spec


def test_build_spec_community_is_spot():
    r = catalog.find_recipe("qwen3-8b")
    spec = catalog.build_spec(r, {"gpu_id": "NVIDIA GeForce RTX 4090",
                                  "gpu_count": 1, "cloud": "community",
                                  "container_disk_gb": 20})
    assert spec["spot"] is True


def test_est_cost():
    assert catalog.est_cost_per_hr(GPUS, "NVIDIA A100-SXM4-80GB", 3,
                                   "secure") == 4.77
    assert catalog.est_cost_per_hr(GPUS, "NVIDIA A100-SXM4-80GB", 3,
                                   "community") == 4.17
    assert catalog.est_cost_per_hr(GPUS, "NVIDIA B200", 1, "community") is None
    assert catalog.est_cost_per_hr(GPUS, "nope", 1, "secure") is None


# ── Endpoints ───────────────────────────────────────────────────────────────

def test_endpoint_catalog(wired, monkeypatch):
    wired(CONFIG)
    _patch_options(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/catalog")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["recipes"]) >= 5
    assert body["backend"] == "rp"
    assert {g["id"] for g in body["gpus"]} == {g["id"] for g in GPUS}
    assert {d["id"] for d in body["datacenters"]} == {d["id"] for d in DCS}
    assert body["volumes"] == VOLS
    # template plumbing must not leak into the public shape
    assert "repo_url" not in body["recipes"][0]


def test_endpoint_deploy_rejects_invalid(wired, monkeypatch):
    wired(CONFIG)
    _patch_options(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/catalog/deploy", json={
            "recipe_id": "glm53-flash",
            "gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 2,
            "cloud": "secure", "data_center_id": "EUR-IS-1",
            "volume_mode": "existing", "volume_id": "vol1",
            "container_disk_gb": 200,
        })
    assert r.status_code == 422, r.text
    assert "errors" in r.json()["detail"]
    assert any("VRAM" in e for e in r.json()["detail"]["errors"])


def test_endpoint_deploy_happy_path(wired, monkeypatch):
    cfgfile, cfg, _ = wired(CONFIG)
    _patch_options(monkeypatch)
    deploys = []

    async def fake_deploy(payload, api_key_env):
        deploys.append(payload)
        return {"id": "pod-new1", "name": "gw-rp-glm53-flash"}

    monkeypatch.setattr(rp, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/catalog/deploy", json={
            "recipe_id": "glm53-flash",
            "gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 3,
            "cloud": "secure", "data_center_id": "EUR-IS-1",
            "volume_mode": "existing", "volume_id": "vol1",
            "ctx_key": "64k", "container_disk_gb": 200,
            "idle_terminate_s": 0,
        })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["pod_id"] == "pod-new1"
    assert body["model"] == "glm53-flash"
    assert body["est_cost_per_hr"] == 4.77
    # deploy payload carries the recipe spec
    assert deploys[0]["gpu"] == {"id": "NVIDIA A100-SXM4-80GB", "count": 3}
    # ports must be an ARRAY even though the spec stores "8080/http"
    # (string) — RunPod 422s otherwise (live-verified in the E2E)
    assert deploys[0]["ports"] == ["8080/http"]
    assert deploys[0]["mounts"]["network"][0]["volumeId"] == "vol1"
    assert deploys[0]["dataCenterIds"] == ["EUR-IS-1"]
    # spec persisted to config with pod_id for lifecycle management
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    spec = raw["backends"]["rp"]["models"]["glm53-flash"]
    assert spec["pod_id"] == "pod-new1"
    assert spec["volume_id"] == "vol1"
    assert spec["docker_script"].endswith("--parallel 2 --ctx-size 131072")


def test_endpoint_deploy_creates_volume(wired, monkeypatch):
    cfgfile, _, _ = wired(CONFIG)
    _patch_options(monkeypatch)
    created = []

    async def fake_create(name, dc, size, vtype, api_key_env):
        created.append((name, dc, size, vtype))
        return {"id": "vol-new1", "name": name}

    async def fake_deploy(payload, api_key_env):
        return {"id": "pod-new2", "name": "gw-rp-glm53-flash"}

    monkeypatch.setattr(rp, "create_volume", fake_create)
    monkeypatch.setattr(rp, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/catalog/deploy", json={
            "recipe_id": "glm53-flash",
            "gpu_id": "NVIDIA H200", "gpu_count": 2,
            "cloud": "secure", "data_center_id": "CA-MTL-4",
            "volume_mode": "create", "volume_size_gb": 250,
            "volume_type": "HIGH_PERFORMANCE",
            "container_disk_gb": 250,
        })
    assert r.status_code == 200, r.text
    assert created == [("gw-glm53-flash-ca-mtl-4", "CA-MTL-4", 250,
                        "HIGH_PERFORMANCE")]
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    spec = raw["backends"]["rp"]["models"]["glm53-flash"]
    assert spec["volume_id"] == "vol-new1"
    assert spec["gpu_type_ids"] == ["NVIDIA H200"]
    assert spec["data_center_ids"] == ["CA-MTL-4"]


def test_endpoint_deploy_unknown_recipe(wired, monkeypatch):
    wired(CONFIG)
    _patch_options(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/catalog/deploy", json={"recipe_id": "nope"})
    assert r.status_code == 400
