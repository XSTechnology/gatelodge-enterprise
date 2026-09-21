"""Gateway v4 model finder — capabilities, search, custom deploy.

mf-1: capability inference (curated family / name-variant / metadata
layers), task matching (business = chat AND deep_think), GGUF filename
parsing (quants, mmproj, -mtp noise), catalogue recipe capability tags.
"""
import asyncio
import json
import sys
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.capabilities as caps  # noqa: E402
import gateway.catalog as catalog  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.modelsearch as ms  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.upload as up  # noqa: E402


# ── capability inference (D1) ───────────────────────────────────────────────

def test_infer_curated_family_and_variant():
    r = caps.infer("unsloth/Qwen3-VL-8B-GGUF")
    assert "vision" in r["capabilities"]
    assert "chat" in r["capabilities"] and "deep_think" in r["capabilities"]
    assert r["evidence"]["vision"].startswith("curated:")
    assert r["evidence"]["vision"].endswith("vl")


def test_infer_coder_variant():
    r = caps.infer("Qwen2.5-Coder-7B-GGUF")
    assert "coding" in r["capabilities"]
    assert "vision" not in r["capabilities"]


def test_infer_name_layer_without_family():
    # unknown family — variant tokens still work, badged as name evidence
    r = caps.infer("MysteryLab/WhateverModel-VL-9B-GGUF")
    assert "vision" in r["capabilities"]
    assert r["evidence"]["vision"].startswith("name:")


def test_infer_metadata_tags():
    r = caps.infer("unknown/model-gguf", tags=["reasoning", "conversational"])
    assert "deep_think" in r["capabilities"]
    assert "chat" in r["capabilities"]
    assert r["evidence"]["deep_think"] == "tag:reasoning"


def test_infer_pipeline_tag_vision():
    r = caps.infer("ISTA-DASLab/Some-27B-GSQ-RCO-GGUF",
                   pipeline_tag="image-text-to-text")
    assert "vision" in r["capabilities"]
    assert r["evidence"]["vision"] == "tag:image-text-to-text"


def test_infer_default_chat():
    r = caps.infer("totally/unknown-thing")
    assert r["capabilities"] == ["chat"]
    assert r["evidence"]["chat"] == "default"


# ── task matching ──────────────────────────────────────────────────────────

def test_task_match_business_needs_chat_and_think():
    both = ["chat", "deep_think"]
    assert caps.task_match(both, "business") is True
    assert caps.task_match(["chat"], "business") is False
    assert caps.task_match(["chat", "vision"], "vision") is True
    assert caps.task_match(["chat"], "vision") is False
    # unknown task key filters nothing
    assert caps.task_match(["chat"], "nope") is True


def test_tasks_shape_for_console():
    keys = [t["key"] for t in caps.TASKS]
    assert keys == ["chat", "vision", "deep_think", "coding", "business"]
    assert all(t.get("label") for t in caps.TASKS)


# ── GGUF filename parsing ──────────────────────────────────────────────────

def test_parse_quant_variants():
    assert caps.parse_quant("Qwen3-8B-UD-Q4_K_XL.gguf") == "UD-Q4_K_XL"
    assert caps.parse_quant("Qwen3.8-27B-GSQ-RCO-IQ2_S-mtp.gguf") == \
        "GSQ-RCO-IQ2_S"
    assert caps.parse_quant("model.Q4_K_M.gguf") == "Q4_K_M"
    assert caps.parse_quant("whatever-IQ4_XS.gguf") == "IQ4_XS"
    assert caps.parse_quant("plain-Q8_0.gguf") == "Q8_0"
    assert caps.parse_quant("model-F16.gguf") == "F16"
    assert caps.parse_quant("Qwen3-14B.gguf") is None


def test_pickable_filters_noise():
    assert caps.is_pickable("Qwen3-8B-Q4_K_M.gguf") is True
    assert caps.is_pickable("Qwen3.8-27B-IQ2_S-mtp.gguf") is False
    assert caps.is_pickable("mmproj-Qwen3-VL.gguf") is False
    assert caps.is_pickable("Qwen3-8B-imatrix.gguf") is False
    assert caps.is_pickable("README.md") is False
    assert caps.is_mmproj("Qwen3-VL-mmproj-F16.gguf") is True
    assert caps.is_mmproj("model.gguf") is False


# ── catalogue recipes carry capabilities ────────────────────────────────────

def test_recipes_have_capability_tags():
    assert len(catalog.CATALOG) >= 7
    for r in catalog.CATALOG:
        assert r.get("capabilities"), r["id"]
        assert set(r["capabilities"]) <= set(caps.ALL_CAPS)
    ds = catalog.find_recipe("deepseek-v4-flash")
    assert "coding" in ds["capabilities"]
    # public shape keeps them (task chooser shows recipes first)
    pub = {p["id"]: p for p in catalog.public_recipes()}
    assert pub["qwen3-8b"]["capabilities"] == ["chat", "deep_think"]


# ── unified search + file listing (mf-2) ────────────────────────────────────

class _FakeResp:
    def __init__(self, status=200, json_data=None):
        self.status_code = status
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("err", request=None, response=None)


class _FakeClient:
    """httpx.AsyncClient stand-in routing by (method, url)."""
    responses: dict = {}

    def __init__(self, timeout=None):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, **kw):
        return _FakeClient.responses.get(("GET", url), _FakeResp(404))

    async def put(self, url, **kw):
        return _FakeClient.responses.get(("PUT", url), _FakeResp(404))


@pytest.fixture()
def fake_httpx(monkeypatch):
    _FakeClient.responses = {}
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    return _FakeClient


async def test_hf_search_and_files(monkeypatch):
    import huggingface_hub

    class _M:
        def __init__(self, mid, tags, pt, dl):
            self.id = mid
            self.tags = tags
            self.pipeline_tag = pt
            self.downloads = dl

    class _S:
        def __init__(self, name, size):
            self.rfilename = name
            self.size = size

    class _Info:
        def __init__(self, sib):
            self.siblings = sib

    class _Api:
        def list_models(self, filter=None, search=None, limit=None):
            return [_M("unsloth/ModelB-GGUF", ["conversational"], None, 10),
                    _M("a/Qwen3-VL-8B-GGUF", ["vision"],
                       "image-text-to-text", 99)]

        def model_info(self, repo_id, files_metadata=False):
            return _Info([_S("model-Q4_K_M.gguf", 5_000_000_000),
                          _S("model-Q4_K_M-mtp.gguf", 5_000_000_000),
                          _S("mmproj-F16.gguf", 800_000_000),
                          _S("README.md", 100)])

    monkeypatch.setattr(huggingface_hub, "HfApi", _Api)
    ms.reset_cache()
    results = await ms.search("x", source="hf")
    # sorted by downloads: the VL model first; capability layers applied
    assert results[0]["repo_id"] == "a/Qwen3-VL-8B-GGUF"
    assert "vision" in results[0]["capabilities"]
    assert results[1]["capabilities"] == ["chat"]
    out = await ms.list_files("hf", "unsloth/ModelB-GGUF")
    assert [f["name"] for f in out["files"]] == ["model-Q4_K_M.gguf"]
    assert out["files"][0]["quant"] == "Q4_K_M"
    assert "resolve/main" in out["files"][0]["url"]
    assert [f["name"] for f in out["mmproj"]] == ["mmproj-F16.gguf"]


async def test_modelscope_search_and_files(fake_httpx):
    ms.reset_cache()
    fake_httpx.responses[("PUT", ms._MS + "/dolphin/models")] = _FakeResp(
        200, {"Data": {"Model": [{"Path": "Qwen", "Name": "Qwen3-8B-GGUF",
                                  "Downloads": 5}]}})
    results = await ms.search("Qwen3", source="modelscope")
    assert len(results) == 1
    assert results[0]["repo_id"] == "Qwen/Qwen3-8B-GGUF"
    assert "deep_think" in results[0]["capabilities"]  # curated: qwen
    fake_httpx.responses[
        ("GET", ms._MS + "/models/Qwen/Qwen3-8B-GGUF/repo/files")] = \
        _FakeResp(200, {"Data": {"Files": [
            {"Name": "m", "Path": "Qwen3-8B-Q4_K_M.gguf",
             "Size": 5_027_783_488, "Type": "blob"},
            {"Path": "sub", "Type": "tree"},
            {"Path": "mmproj-F16.gguf", "Size": 800, "Type": "blob"}]}})
    out = await ms.list_files("modelscope", "Qwen/Qwen3-8B-GGUF")
    assert out["files"][0]["quant"] == "Q4_K_M"
    assert "FilePath=Qwen3-8B-Q4_K_M.gguf" in out["files"][0]["url"]
    assert len(out["mmproj"]) == 1


async def test_ollama_search_paste_and_files(fake_httpx):
    ms.reset_cache()
    mf = {"layers": [
        {"mediaType": "application/vnd.ollama.image.model",
         "size": 5_225_374_496, "digest": "sha256:abc"},
        {"mediaType": "application/vnd.ollama.image.template",
         "size": 1723, "digest": "sha256:t"}]}
    fake_httpx.responses[
        ("GET", ms._OL + "/qwen3/manifests/8b")] = _FakeResp(200, mf)
    # free-form paste model:tag
    results = await ms.search("qwen3:8b", source="ollama")
    assert len(results) == 1 and results[0]["name"] == "qwen3:8b"
    # curated library discovery
    results = await ms.search("qwen", source="ollama")
    assert {r["repo_id"] for r in results} == \
        {"qwen3", "qwen2.5", "qwen2.5-coder"}
    out = await ms.list_files("ollama", "qwen3")
    # only the 8b tag has a manifest in the fake; 0.6b etc. 404 → skipped
    assert [f["name"] for f in out["files"]] == ["qwen3:8b"]
    assert out["files"][0]["size_bytes"] == 5_225_374_496
    assert out["files"][0]["url"].endswith("/blobs/sha256:abc")


async def test_search_resilience_and_cache(monkeypatch):
    ms.reset_cache()

    async def boom(q, limit):
        raise RuntimeError("modelscope down")

    async def one(q, limit):
        return [{"source": "ollama", "repo_id": "x", "name": "x",
                 "capabilities": ["chat"], "evidence": {},
                 "downloads": 0, "url": "u"}]

    def hf_boom(q, limit):
        raise RuntimeError("hf down")

    monkeypatch.setattr(ms, "_hf_search_sync", hf_boom)
    monkeypatch.setattr(ms, "_modelscope_search", boom)
    monkeypatch.setattr(ms, "_ollama_search", one)
    # both sources failing elsewhere → ollama results still come back
    results = await ms.search("x", source="all")
    assert [r["repo_id"] for r in results] == ["x"]
    # cached: a CHANGED ollama impl must not re-run within the TTL
    async def changed(q, limit):
        return [{"repo_id": "should-not-appear"}]

    monkeypatch.setattr(ms, "_ollama_search", changed)
    results = await ms.search("x", source="all")
    assert [r["repo_id"] for r in results] == ["x"]


# ── endpoints (mf-2) ────────────────────────────────────────────────────────

@pytest.fixture()
def wired(tmp_path, monkeypatch):
    def _wire():
        cfgfile = tmp_path / "gateway_config.json"
        cfgfile.write_text(json.dumps({
            "port": 9911, "bind": "127.0.0.1", "default_alias": "",
            "models_dir": "/nonexistent",
            "backends": {"rp": {"type": "runpod",
                                "api_key_env": "RUNPOD_API_KEY_TEST",
                                "models": {}}}}), encoding="utf-8")
        cfg = gw_config.GatewayConfig(cfgfile)
        monkeypatch.setattr(gw_config, "_CONFIG", cfg)
        monkeypatch.setattr(gw_models, "_ENGINE",
                            gw_models.EmbeddedEngine(cfg))
        monkeypatch.setattr(gw_app, "_LOG_PATH",
                            tmp_path / "gateway_log.jsonl")
        return cfgfile

    yield _wire


def test_endpoint_search_task_and_files(wired, monkeypatch):
    wired()
    ms.reset_cache()

    async def fake_search(q, source="all", limit=10):
        return [{"source": "hf", "repo_id": "a/B-GGUF", "name": "B-GGUF",
                 "capabilities": ["chat", "coding"], "evidence": {},
                 "downloads": 3, "url": "u"}]

    async def fake_files(source, repo_id):
        return {"source": source, "repo_id": repo_id,
                "files": [{"name": "B-Q4.gguf", "size_bytes": 1,
                           "url": "http://x", "quant": "Q4"}],
                "mmproj": []}

    monkeypatch.setattr(ms, "search", fake_search)
    real_list_files = ms.list_files
    monkeypatch.setattr(ms, "list_files", fake_files)
    with TestClient(gw_app.app) as client:
        # coding task: only the coder recipe + the coding result
        r = client.get("/admin/models/search",
                       params={"q": "deepseek", "task": "coding"})
        assert r.status_code == 200
        body = r.json()
        assert [x["id"] for x in body["recipes"]] == ["deepseek-v4-flash"]
        assert body["results"][0]["repo_id"] == "a/B-GGUF"
        assert [t["key"] for t in body["tasks"]][:2] == ["chat", "vision"]
        # no query → recipes only
        r = client.get("/admin/models/search", params={"task": "vision"})
        body = r.json()
        assert body["results"] == [] and body["recipes"] == []
        # business task: every current recipe has chat+deep_think
        r = client.get("/admin/models/search", params={"task": "business"})
        assert len(r.json()["recipes"]) == len(catalog.CATALOG)
        # files endpoint
        r = client.get("/admin/models/files",
                       params={"source": "hf", "repo": "a/B-GGUF"})
        assert r.json()["files"][0]["quant"] == "Q4"
        r = client.get("/admin/models/files", params={"source": "hf"})
        assert r.status_code == 400
        # error paths use the REAL list_files (unpatched here)
        monkeypatch.setattr(ms, "list_files", real_list_files)
        r = client.get("/admin/models/files",
                       params={"source": "nope", "repo": "x"})
        assert r.status_code == 400


# ── custom deploy: sizing, spec builder, endpoints (mf-3) ──────────────────

LIVE = {
    "gpus": [
        {"id": "NVIDIA RTX 4090", "memory": 24,
         "price": {"community": 0.34, "secure": 0.74},
         "maxCount": {"community": 8, "secure": 8}},
        {"id": "NVIDIA A100-SXM4-80GB", "memory": 80,
         "price": {"community": 1.39, "secure": 1.59},
         "maxCount": {"community": 8, "secure": 8}},
    ],
    "datacenters": [{"id": "EUR-IS-1", "region": "EUROPE",
                     "networkVolumeTypes": ["STANDARD"]}],
    "volumes": [],
}


def _patch_live(monkeypatch):
    async def fake_live(env):
        return LIVE

    monkeypatch.setattr(gw_app.catalog, "live_options", fake_live)


def test_kv_and_ctx_options():
    # calibrated on the proven glm53 recipe (157GB → ~45GB KV @128k)
    assert catalog.kv_est_gb(157, 131072) == pytest.approx(45.5, abs=0.5)
    presets = catalog.custom_ctx_options(6, 24)
    assert [p["key"] for p in presets] == \
        ["2x32k", "1x64k", "2x64k", "1x128k"]
    # KV pressure drops the big presets on a tight card
    tight = catalog.custom_ctx_options(16, 24)
    assert [p["key"] for p in tight] == ["2x32k", "1x64k"]
    # model bigger than the card → nothing fits
    assert catalog.custom_ctx_options(200, 80) == []


def test_build_custom_spec_shapes():
    spec = catalog.build_custom_spec(
        file_name="Qwen/Qwen3-VL-8B-GGUF/Qwen3-VL-8B-Q4_K_M.gguf",
        file_url=("https://huggingface.co/Qwen/Qwen3-VL-8B-GGUF/"
                  "resolve/main/Qwen3-VL-8B-Q4_K_M.gguf"),
        size_gb=6.0, mmproj_url="https://x/mmproj.gguf",
        mmproj_size_gb=0.8, gpu_id="NVIDIA RTX 4090", gpu_count=1,
        cloud="secure", ctx_key="2x64k", container_disk_gb=25,
        alias="my-vl")
    s = spec["docker_script"]
    assert "curl -fsSL" in s and "--mmproj /models/mmproj.gguf" in s
    assert "--parallel 2 --ctx-size 131072" in s
    assert spec["alias"] == "my-vl" and spec["custom"] is True
    assert spec["source_size_gb"] == 6.0
    # no mmproj → no flag; volume → persistent dir; community → spot
    spec2 = catalog.build_custom_spec(
        file_name="m.gguf", file_url="https://x/m.gguf", size_gb=5,
        gpu_id="g", gpu_count=2, cloud="community", volume_id="vol-1",
        ctx_key="1x64k")
    assert "--mmproj" not in spec2["docker_script"]
    assert "/workspace/models" in spec2["docker_script"]
    assert spec2["spot"] is True and spec2["gpu_count"] == 2
    # alias defaults to the filename stem (last path component)
    spec3 = catalog.build_custom_spec(
        file_name="a/b/Cool-Model-Q4.gguf", file_url="u", size_gb=5,
        gpu_id="g", gpu_count=1, cloud="secure")
    assert spec3["alias"] == "Cool-Model-Q4"


def test_plan_endpoint(wired, monkeypatch):
    wired()
    _patch_live(monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/models/plan", params={
            "size_gb": 6, "gpu_id": "NVIDIA RTX 4090", "gpu_count": 1})
    assert r.status_code == 200
    body = r.json()
    assert body["fits"] is True
    assert body["ctx_default"] == "1x128k"      # auto: largest fitting
    assert body["est_cost_per_hr"] == 0.74
    assert body["disk_min_gb"] == 20            # floor is 20 GB
    r = client.get("/admin/models/plan", params={"size_gb": 0})
    assert r.status_code == 400


def test_deploy_custom_success_persists(wired, monkeypatch):
    cfgfile = wired()
    _patch_live(monkeypatch)
    deploys = []

    async def fake_deploy(payload, env):
        deploys.append(payload)
        return {"id": "pod-custom1", "name": "gw-rp-myvl"}

    monkeypatch.setattr(gw_app.runpod, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/models/deploy-custom", json={
            "source": "upload", "repo_id": "Qwen/Qwen3-VL-8B-GGUF",
            "file_name": "Qwen3-VL-8B-Q4_K_M.gguf",
            "file_url": ("https://huggingface.co/Qwen/Qwen3-VL-8B-GGUF/"
                         "resolve/main/Qwen3-VL-8B-Q4_K_M.gguf"),
            "size_gb": 6.0, "mmproj_url": "https://x/mmproj.gguf",
            "mmproj_size_gb": 0.8,
            "gpu_id": "NVIDIA RTX 4090", "gpu_count": 1,
            "cloud": "secure", "ctx_key": "2x64k",
            "idle_stop_s": 900, "alias": "myvl"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["pod_id"] == "pod-custom1"
    assert body["landed"]["gpu_id"] == "NVIDIA RTX 4090"
    assert body["est_cost_per_hr"] == 0.74
    assert "gw-rp-myvl" in str(deploys[0])
    # spec persisted with custom provenance + v3 lifecycle fields
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    spec = raw["backends"]["rp"]["models"]["myvl"]
    assert spec["pod_id"] == "pod-custom1"
    assert spec["custom"] is True
    assert spec["est_cost_per_hr"] == 0.74
    assert spec["idle_stop_s"] == 900
    assert spec["mmproj_url"] == "https://x/mmproj.gguf"
    assert "--mmproj" in spec["docker_script"]


def test_deploy_custom_fallback_and_422(wired, monkeypatch):
    wired()
    _patch_live(monkeypatch)

    async def fake_deploy(payload, env):
        return {"id": "pod-fb", "name": "n"}

    monkeypatch.setattr(gw_app.runpod, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        # user picks a 4090 for a 50GB model → auto-lands the A100
        r = client.post("/admin/models/deploy-custom", json={
            "source": "hf", "file_name": "big-Q4.gguf",
            "file_url": "https://x/big.gguf", "size_gb": 50,
            "gpu_id": "NVIDIA RTX 4090", "gpu_count": 1,
            "cloud": "secure", "alias": "bigm"})
        assert r.status_code == 200
        body = r.json()
        assert body["landed"]["gpu_id"] == "NVIDIA A100-SXM4-80GB"
        assert any("does not fit" in (t.get("error") or "")
                   for t in body["tried"])
        # 50GB on an 80GB A100: weights 56.5 + KV 14.5 = 71 ≤ 73.6 —
        # the full 128k preset fits, so the auto default lands there
        assert body["ctx_key"] == "1x128k"
        # nothing fits anywhere → 422
        r = client.post("/admin/models/deploy-custom", json={
            "source": "hf", "file_name": "huge.gguf",
            "file_url": "https://x/huge.gguf", "size_gb": 500,
            "gpu_id": "NVIDIA A100-SXM4-80GB", "gpu_count": 1,
            "cloud": "secure"})
        assert r.status_code == 422
        assert "no live GPU fits" in r.text


def test_deploy_custom_volume_create(wired, monkeypatch):
    cfgfile = wired()
    _patch_live(monkeypatch)
    vols = []

    async def fake_vol(name, dc, size, vtype, env):
        vols.append((name, dc, size, vtype))
        return {"id": "vol-new"}

    async def fake_deploy(payload, env):
        return {"id": "p2", "name": "n"}

    monkeypatch.setattr(gw_app.runpod, "create_volume", fake_vol)
    monkeypatch.setattr(gw_app.runpod, "deploy_pod", fake_deploy)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/models/deploy-custom", json={
            "source": "hf", "file_name": "m.gguf",
            "file_url": "https://x/m.gguf", "size_gb": 5,
            "gpu_id": "NVIDIA RTX 4090", "gpu_count": 1,
            "cloud": "secure", "volume_mode": "create",
            "data_center_id": "EUR-IS-1", "volume_size_gb": 100,
            "alias": "volm"})
        assert r.status_code == 200
        assert vols[0][1] == "EUR-IS-1" and vols[0][2] == 100
        raw = json.loads(cfgfile.read_text(encoding="utf-8"))
        spec = raw["backends"]["rp"]["models"]["volm"]
        assert spec["volume_id"] == "vol-new"
        assert "/workspace/models" in spec["docker_script"]
        # volume without a DC → 400
        r = client.post("/admin/models/deploy-custom", json={
            "source": "hf", "file_name": "m.gguf",
            "file_url": "https://x/m.gguf", "size_gb": 5,
            "volume_mode": "create"})
        assert r.status_code == 400


# ── upload path (mf-4) ──────────────────────────────────────────────────────

def test_receiver_script_valid():
    s = up.receiver_script()
    assert "printf '%s' '" in s
    assert "exec python3 /root/receiver.py" in s
    inner = s.split("printf '%s' '", 1)[1].rsplit(
        "' > /root/receiver.py", 1)[0]
    assert "'" not in inner              # sh-safe single-quote embedding
    compile(inner, "receiver.py", "exec")  # and it is valid python
    for needle in ("/health", "/status/", "/file/", "ThreadingHTTPServer",
                   "os.replace", "Content-Length required"):
        assert needle in inner


def test_receiver_spec():
    spec = up.receiver_spec("NVIDIA RTX 4090", 10 * 1024 ** 3,
                            volume_id="vol-1")
    assert spec["ports"] == "8081/http" and spec["port"] == 8081
    assert spec["idle_stop_s"] == 1800
    assert spec["volume_id"] == "vol-1"
    assert spec["container_disk_gb"] == 22        # 10GB*1.2 + 10
    spec2 = up.receiver_spec("g", 0)
    assert "volume_id" not in spec2
    assert spec2["container_disk_gb"] == 20       # floor


async def test_init_receiver_reuse(wired, monkeypatch):
    cfgfile = wired()
    cfg = gw_config.get_gateway_config()
    cfgdata = json.loads(cfgfile.read_text(encoding="utf-8"))
    cfgdata["backends"]["rp"]["models"][up.RECEIVER_MODEL] = {
        "image": "x", "pod_id": "pod-live1"}
    cfgfile.write_text(json.dumps(cfgdata), encoding="utf-8")
    cfg.reload_now()

    async def healthy(pid):
        return pid == "pod-live1"

    async def boom(*a, **k):
        raise AssertionError("healthy receiver must be reused, not redeployed")

    monkeypatch.setattr(up, "probe_receiver", healthy)
    monkeypatch.setattr(gw_app.runpod, "deploy_pod", boom)
    info = await up.init_receiver(cfg, "rp", 1024)
    assert info == {"pod_id": "pod-live1", "reused": True}


async def test_init_receiver_deploy(wired, monkeypatch):
    cfgfile = wired()
    cfg = gw_config.get_gateway_config()
    _patch_live(monkeypatch)                       # cheapest GPU = 4090

    async def probe(pid):
        return pid == "pod-rx"

    deploys = []

    async def fake_deploy(payload, env):
        deploys.append(payload)
        return {"id": "pod-rx", "name": "gw-rp-upload-receiver"}

    monkeypatch.setattr(up, "probe_receiver", probe)
    monkeypatch.setattr(gw_app.runpod, "deploy_pod", fake_deploy)
    info = await up.init_receiver(cfg, "rp", 5 * 1024 ** 3,
                                  volume_id="vol-1")
    assert info == {"pod_id": "pod-rx", "reused": False}
    assert "gw-rp-upload-receiver" in str(deploys[0])
    # persisted as a config model → watchdog owns its lifecycle
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    spec = raw["backends"]["rp"]["models"][up.RECEIVER_MODEL]
    assert spec["pod_id"] == "pod-rx"
    assert spec["volume_id"] == "vol-1"
    assert spec["idle_stop_s"] == 1800


def test_upload_endpoints(wired, monkeypatch):
    wired()

    async def fake_init(cfg, backend, size, vol=""):
        return {"pod_id": "pod-rx", "reused": False}

    monkeypatch.setattr(gw_app.upload, "init_receiver", fake_init)
    with TestClient(gw_app.app) as client:
        # init: filename is basename'd; >20GB → 413
        r = client.post("/admin/models/upload-init", json={
            "filename": "../evil/../mymodel.gguf", "size_bytes": 1234})
        assert r.status_code == 200
        assert r.json()["filename"] == "mymodel.gguf"
        r = client.post("/admin/models/upload-init", json={
            "filename": "x.gguf", "size_bytes": 21 * 1024 ** 3})
        assert r.status_code == 413

        # relay: fake httpx captures the forwarded PUT
        captured = {}

        class _RelayResp:
            status_code = 200
            content = b"{}"
            text = "{}"

            def json(self):
                return {"ok": True, "bytes": 8}

        class _RelayClient:
            def __init__(self, timeout=None):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def put(self, url, content=None, headers=None):
                captured["url"] = url
                captured["headers"] = headers
                captured["body"] = b"".join([c async for c in content])
                return _RelayResp()

        monkeypatch.setattr(httpx, "AsyncClient", _RelayClient)
        r = client.put("/admin/models/upload", content=b"MODELDATA",
                       headers={"x-pod-id": "pod-rx",
                                "x-filename": "mymodel.gguf",
                                "x-file-size": "8"})
        assert r.status_code == 200, r.text
        assert captured["url"] == up.receiver_url("pod-rx", "mymodel.gguf")
        assert captured["headers"]["Content-Length"] == "8"
        assert captured["body"] == b"MODELDATA"
        r = client.put("/admin/models/upload", content=b"x")
        assert r.status_code == 400

        # status / finalize (real finalize, patched receiver status)
        async def fake_status(pid, fn):
            return {"name": fn, "size": 8}

        monkeypatch.setattr(gw_app.upload, "receiver_status", fake_status)
        r = client.get("/admin/models/upload-status",
                       params={"pod_id": "pod-rx", "filename": "m.gguf"})
        assert r.json()["size"] == 8
        r = client.post("/admin/models/upload-finalize", json={
            "pod_id": "pod-rx", "filename": "m.gguf", "size_bytes": 8})
        body = r.json()
        assert body["file_url"] == up.receiver_url("pod-rx", "file/m.gguf")
        assert body["source"] == "upload"
        assert body["size_gb"] == 0.0
        # size mismatch → 502
        r = client.post("/admin/models/upload-finalize", json={
            "pod_id": "pod-rx", "filename": "m.gguf", "size_bytes": 99})
        assert r.status_code == 502

        # cleanup (patched)
        async def fake_clean(cfg, backend):
            return {"ok": True, "terminated": True, "pod_id": "pod-rx"}

        monkeypatch.setattr(gw_app.upload, "cleanup_receiver", fake_clean)
        r = client.post("/admin/models/upload-cleanup", json={})
        assert r.json()["terminated"] is True
