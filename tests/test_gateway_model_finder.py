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
        return cfg

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
