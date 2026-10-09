"""LLM Gateway v2 — POST /v1/embeddings.

Embedded GGUFs configured under backends.local.embedding_models load as
SEPARATE pooling instances (Llama(embedding=True); chat instances never
embed, embedding models are never auto-discovered). Cloud backends pass
through (remote model ids, dimensions forwarded). encoding_format
float|base64; input str|list[str]; token-id arrays and empty inputs 400;
unknown / chat models 400. /v1/models lists embedding models; /admin/load
+ /admin/unload + /healthz cover the embed registry; JSONL request log
records embeddings.
"""
import base64
import json
import struct
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.cloud as gw_cloud  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402


# ── FakeLlama (chat + embeddings) ──────────────────────────────────────────

_VEC = [0.1, 0.2, 0.3]


def _with_fake_llama(fn):
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.n_ctx = kwargs.get("n_ctx", 4096)

        def create_chat_completion(self, **kwargs):
            records.append({"call": kwargs})
            return {"choices": [{"message": {"content": "ok"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

        def create_embedding(self, input=None, model=None):
            records.append({"embed": {"input": input, "model": model}})
            data = [{"object": "embedding", "index": i, "embedding": list(_VEC)}
                    for i in range(len(input))]
            return {"object": "list", "data": data, "model": model or "fake",
                    "usage": {"prompt_tokens": 5 * len(input),
                              "total_tokens": 5 * len(input)}}

    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        fn(records)
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)
    return records


# ── Fake cloud client (embeddings) ─────────────────────────────────────────

class _FakeEmbResp:
    def __init__(self, d: dict):
        self._d = d

    def model_dump_json(self) -> str:
        return json.dumps(self._d)


class _FakeCloudEmb:
    def __init__(self):
        self.calls: list[dict] = []
        self.embeddings = types.SimpleNamespace(create=self._create)

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        n = len(kwargs.get("input") or [])
        return _FakeEmbResp({
            "object": "list",
            "model": kwargs.get("model"),
            "data": [{"object": "embedding", "index": i,
                      "embedding": [0.7, 0.8]} for i in range(n)],
            "usage": {"prompt_tokens": 2 * n, "total_tokens": 2 * n},
        })


# ── fixtures / helpers ─────────────────────────────────────────────────────

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
        return cfgfile, cfg, engine
    return _wire


def _config(gguf, with_cloud: bool = False) -> dict:
    cfg = {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "",
        "models_dir": str(gguf.parent / "models"),
        "backends": {"local": {"type": "embedded",
                               "models": {"fast": {"gguf": str(gguf),
                                                   "ctx": 1024,
                                                   "placement": ""}},
                               "embedding_models": {"embed": {"gguf": str(gguf),
                                                              "ctx": 512,
                                                              "placement": ""}}}},
        "aliases": {},
    }
    if with_cloud:
        cfg["backends"]["openai"] = {
            "type": "openai", "base_url": "https://api.openai.invalid/v1",
            "api_key_env": "OPENAI_API_KEY_TEST",
            "models": {"text-embedding-3-small": "text-embedding-3-small"},
        }
    return cfg


# ── embedded path ──────────────────────────────────────────────────────────

def test_embed_bare_name_local(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/embeddings", json={
                "model": "embed", "input": "hello"})
        assert r.status_code == 200
        body = r.json()
        assert body["object"] == "list"
        assert body["model"] == "embed"  # echo of the requested name
        assert len(body["data"]) == 1
        assert body["data"][0]["object"] == "embedding"
        assert body["data"][0]["index"] == 0
        assert body["data"][0]["embedding"] == _VEC
        assert body["usage"]["total_tokens"] == 5
        # the instance loaded in EMBEDDING mode, input normalized to a list
        load = [x["load"] for x in records if "load" in x][0]
        assert load.get("embedding") is True
        emb = [x["embed"] for x in records if "embed" in x][0]
        assert emb["input"] == ["hello"]

    _with_fake_llama(fn)


def test_embed_list_input(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/embeddings", json={
                "model": "embed", "input": ["a", "b", "c"]})
        assert r.status_code == 200
        data = r.json()["data"]
        assert [d["index"] for d in data] == [0, 1, 2]
        assert all(d["embedding"] == _VEC for d in data)
        assert r.json()["usage"]["total_tokens"] == 15
        emb = [x["embed"] for x in records if "embed" in x][0]
        assert emb["input"] == ["a", "b", "c"]

    _with_fake_llama(fn)


def test_embed_direct_form(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/embeddings", json={
                "model": "local:embed", "input": "hello"})
        assert r.status_code == 200
        assert r.json()["data"][0]["embedding"] == _VEC

    _with_fake_llama(fn)


def test_embed_chat_model_rejected(wired, gguf):
    """A chat model cannot embed — its context has no pooling."""
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "local:fast", "input": "hello"})
    assert r.status_code == 400
    assert "embedding model" in r.json()["error"]["message"]


def test_embed_unknown_model_400(wired, gguf):
    """Bare unknown names never fall back to auto-discovery."""
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "nomic-embed-text-v1.5.Q8_0", "input": "hello"})
    assert r.status_code == 400
    assert "unknown embedding model" in r.json()["error"]["message"]


def test_embed_empty_input_400(wired, gguf):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        assert client.post("/v1/embeddings", json={
            "model": "embed", "input": ""}).status_code == 400
        assert client.post("/v1/embeddings", json={
            "model": "embed", "input": []}).status_code == 400


def test_embed_token_ids_rejected(wired, gguf):
    """OpenAI allows token-id arrays; this gateway only takes strings."""
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "embed", "input": [1, 2, 3]})
    assert r.status_code == 400


def test_embed_base64(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/embeddings", json={
                "model": "embed", "input": "hello",
                "encoding_format": "base64"})
        assert r.status_code == 200
        enc = r.json()["data"][0]["embedding"]
        assert isinstance(enc, str)
        floats = struct.unpack(f"<{len(_VEC)}f", base64.b64decode(enc))
        assert list(floats) == pytest.approx(_VEC)

    _with_fake_llama(fn)


def test_embed_bad_encoding_format_400(wired, gguf):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "embed", "input": "hello",
            "encoding_format": "hex"})
    assert r.status_code == 400
    assert "encoding_format" in r.json()["error"]["message"]


def test_embed_dimensions_ignored_locally(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/embeddings", json={
                "model": "embed", "input": "hello", "dimensions": 2})
        assert r.status_code == 200
        # llama.cpp cannot truncate — full-dimension vectors come back
        assert r.json()["data"][0]["embedding"] == _VEC

    _with_fake_llama(fn)


# ── cloud path ─────────────────────────────────────────────────────────────

def test_embed_cloud_bare_name(wired, gguf, monkeypatch):
    wired(_config(gguf, with_cloud=True))
    fake = _FakeCloudEmb()
    monkeypatch.setattr(gw_cloud, "get_cloud_client", lambda *a, **k: fake)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "text-embedding-3-small", "input": ["one", "two"]})
    assert r.status_code == 200
    body = r.json()
    assert body["model"] == "text-embedding-3-small"  # echoed request name
    assert len(body["data"]) == 2
    assert body["data"][0]["embedding"] == [0.7, 0.8]
    # remote model id + input forwarded
    assert fake.calls[0]["model"] == "text-embedding-3-small"
    assert fake.calls[0]["input"] == ["one", "two"]


def test_embed_cloud_dimensions_forwarded(wired, gguf, monkeypatch):
    wired(_config(gguf, with_cloud=True))
    fake = _FakeCloudEmb()
    monkeypatch.setattr(gw_cloud, "get_cloud_client", lambda *a, **k: fake)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "openai:text-embedding-3-small", "input": "hi",
            "dimensions": 256})
    assert r.status_code == 200
    assert fake.calls[0]["dimensions"] == 256


def test_embed_local_wins_over_cloud_for_same_name(wired, gguf, monkeypatch):
    """Bare-name resolution prefers local embedding_models."""
    cfg = _config(gguf, with_cloud=True)
    cfg["backends"]["openai"]["models"]["embed"] = "remote-embedder"
    wired(cfg)
    fake = _FakeCloudEmb()
    monkeypatch.setattr(gw_cloud, "get_cloud_client", lambda *a, **k: fake)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/embeddings", json={
                "model": "embed", "input": "hi"})
        assert r.status_code == 200
        assert fake.calls == []  # never left the machine
        assert any("embed" in x for x in records)

    _with_fake_llama(fn)


# ── registry / admin / listing ─────────────────────────────────────────────

def test_embed_instance_separate_from_chat(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "hi"}]}).status_code == 200
            assert client.post("/v1/embeddings", json={
                "model": "embed", "input": "hi"}).status_code == 200
        loads = [x["load"] for x in records if "load" in x]
        assert len(loads) == 2  # two distinct Llama objects
        assert any(l.get("embedding") is True for l in loads)
        assert any("embedding" not in l for l in loads)

    _with_fake_llama(fn)


def test_v1_models_lists_embedding_models(wired, gguf):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/v1/models")
    ids = [m["id"] for m in r.json()["data"]]
    assert "embed" in ids
    assert "fast" in ids


def test_admin_load_and_unload_embedding(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/load", json={"model": "embed"})
            assert r.status_code == 200
            loaded = r.json()["loaded"]
            assert any(m["alias"] == "embed" and m["role"] == "embed"
                       for m in loaded)
            # the preloaded instance is the EMBEDDING one
            load = [x["load"] for x in records if "load" in x][0]
            assert load.get("embedding") is True
            r = client.post("/admin/unload", json={"model": "embed"})
            assert r.status_code == 200
            assert r.json()["unloaded"] == 1
            assert r.json()["loaded"] == []

    _with_fake_llama(fn)


def test_healthz_lists_embed_instance(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert client.post("/v1/embeddings", json={
                "model": "embed", "input": "hi"}).status_code == 200
            hz = client.get("/healthz").json()
        entries = {m["alias"]: m for m in hz["loaded"]}
        assert entries["embed"]["role"] == "embed"
        assert hz["loaded_count"] == 1

    _with_fake_llama(fn)


def test_embed_jsonl_logged(wired, gguf):
    _, _, engine = wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert client.post("/v1/embeddings", json={
                "model": "embed", "input": "hi"}).status_code == 200
        # month-scoped log (the legacy path migrates on first write)
        p = gw_app._month_log_path(gw_app._month_key())
        line = p.read_text(encoding="utf-8").strip().splitlines()[-1]
        e = json.loads(line)
        assert e["model"] == "embed"
        assert e["backend"] == "local"
        assert e["stream"] is False
        assert e["status"] == 200
        assert e["usage"]["total_tokens"] == 5

    _with_fake_llama(fn)


def test_embed_missing_gguf_502(wired, gguf):
    cfg = _config(gguf)
    cfg["backends"]["local"]["embedding_models"]["embed"]["gguf"] = "not-on-disk"
    wired(cfg)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/embeddings", json={
            "model": "embed", "input": "hi"})
    assert r.status_code == 502
    assert "GGUF" in r.json()["error"]["message"]
