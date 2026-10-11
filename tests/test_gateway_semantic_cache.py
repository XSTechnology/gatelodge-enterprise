"""Semantic cache canary tests (v2.0 Gatehouse) — prove the paraphrase
layer is REAL, not decorative:

    cosine math + threshold admission,
    paraphrase HIT serves without a backend call (cloud call count),
    dissimilar prompts MISS and execute,
    scoping: never across keys, params must match,
    TTL expiry + cap eviction,
    embed failure = transparent skip (serving never depends on it),
    the exact cache still takes precedence on identical repeats.
"""
import asyncio
import time

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
from gateway import semantic_cache

from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)


@pytest.fixture(autouse=True)
def _reset():
    semantic_cache.reset_semantic_cache()
    cfg = gw_config.get_gateway_config()
    orig = getattr(cfg, "semantic_cache", None)
    yield
    # restore whatever config object is current (tests may have wired)
    if orig is not None:
        gw_config.get_gateway_config().semantic_cache = orig
    semantic_cache.reset_semantic_cache()


def _enable(**over):
    """Patch the parsed semantic_cache section (the module reads it per
    call — the hot-reload path)."""
    section = {"enabled": True, "embedding_model": "test-emb",
               "threshold": 0.92, "ttl_s": 3600, "max": 100}
    section.update(over)
    gw_config.get_gateway_config().semantic_cache = section


# deterministic embedder: vector chosen by text prefix
VECS = {
    "v1": [1.0, 0.0, 0.0],
    "v2": [0.99, 0.14, 0.0],      # cosine(v1, v2) ≈ 0.99 → HIT
    "v3": [0.0, 1.0, 0.0],        # orthogonal → MISS
}
_embeds = {"n": 0}


def _embed(text: str):
    _embeds["n"] += 1
    for prefix, vec in VECS.items():
        if text.startswith(prefix):
            return vec
    return VECS["v3"]


# ── unit: the math ─────────────────────────────────────────────────────────

def test_cosine():
    assert semantic_cache.cosine([1, 0], [1, 0]) == 1.0
    assert semantic_cache.cosine([1, 0], [0, 1]) == 0.0
    assert abs(semantic_cache.cosine([1, 0], [1, 1])
               - 0.7071067811865475) < 1e-9
    assert semantic_cache.cosine([0, 0], [1, 1]) == 0.0


def test_lookup_store_cycle():
    _enable()
    semantic_cache.set_embedder(_embed)
    hit, vec = asyncio.run(semantic_cache.lookup(
        "m", "v1 hello", "k1", (100, 0.1)))
    assert hit is None and vec == VECS["v1"]
    asyncio.run(semantic_cache.store("m", "k1", vec,
                                     {"choices": [{"x": 1}]}, (100, 0.1)))
    hit2, _ = asyncio.run(semantic_cache.lookup(
        "m", "v2 paraphrase", "k1", (100, 0.1)))
    assert hit2 is not None and hit2["choices"][0]["x"] == 1
    assert hit2["served_by"]["cache"] == "semantic"
    st = semantic_cache.stats()
    assert st["stores"] == 1 and st["hits"] == 1 and st["entries"] == 1


def test_scoping_model_key_params():
    _enable()
    semantic_cache.set_embedder(_embed)
    asyncio.run(semantic_cache.store(
        "m", "k1", VECS["v1"], {"a": 1}, (100, 0.1)))
    for model, key, params in (("other", "k1", (100, 0.1)),
                               ("m", "k2", (100, 0.1)),
                               ("m", "k1", (50, 0.1)),
                               ("m", "k1", (100, 0.7))):
        hit, _ = asyncio.run(semantic_cache.lookup(
            model, "v1 x", key, params))
        assert hit is None, (model, key, params)


def test_ttl_and_cap():
    _enable(ttl_s=0.05)
    semantic_cache.set_embedder(_embed)
    asyncio.run(semantic_cache.store("m", "k", VECS["v1"], {"a": 1}, (1, 1)))
    time.sleep(0.08)
    hit, _ = asyncio.run(semantic_cache.lookup("m", "v1 x", "k", (1, 1)))
    assert hit is None and semantic_cache.stats()["entries"] == 0  # purged

    _enable(max=2)   # cap: 3 stores → 1 eviction
    for i, v in enumerate((VECS["v1"], [0.9, 0.1, 0.0], [0.1, 0.9, 0.0])):
        asyncio.run(semantic_cache.store("m", "k", v, {"i": i}, (1, 1)))
    assert semantic_cache.stats()["entries"] == 2
    assert semantic_cache.stats()["evictions"] == 1


def test_embed_failure_is_transparent():
    _enable()

    def _boom(_text):
        raise RuntimeError("no embedder configured")
    semantic_cache.set_embedder(_boom)
    hit, vec = asyncio.run(semantic_cache.lookup("m", "v1 x", "k", (1, 1)))
    assert hit is None and vec is None            # never raises
    assert semantic_cache.stats()["errors"] == 1


def test_disabled_never_embeds():
    semantic_cache.set_embedder(_embed)
    n0 = _embeds["n"]
    hit, vec = asyncio.run(semantic_cache.lookup("m", "v1 x", "k", (1, 1)))
    assert (hit, vec) == (None, None)
    assert _embeds["n"] == n0


# ── app-level: the serving path ────────────────────────────────────────────

def _chat(client, content, model="groq:cloud-fast", headers=None, **kw):
    return client.post("/v1/chat/completions", json={
        "model": model, "messages": [{"role": "user", "content": content}],
        **kw}, headers=headers or {})


def test_paraphrase_hit_skips_backend(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _enable()
    semantic_cache.set_embedder(_embed)
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        r1 = _chat(client, "v1 what is the capital of france")
        assert r1.status_code == 200, r1.text
        assert len(fake.calls) == 1
        # PARAPHRASE (different text, near-identical vector) → no call
        r2 = _chat(client, "v2 what's the capital city of france?")
        assert r2.status_code == 200, r2.text
        assert len(fake.calls) == 1, "paraphrase must not hit the backend"
        assert r2.json().get("served_by", {}).get("cache") == "semantic"
        assert r2.json()["choices"] == r1.json()["choices"]
        # dissimilar → executes
        r3 = _chat(client, "v3 write a haiku about routers")
        assert r3.status_code == 200
        assert len(fake.calls) == 2
    st = semantic_cache.stats()
    assert st["hits"] == 1 and st["stores"] == 2


def test_exact_cache_takes_precedence(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _enable()
    semantic_cache.set_embedder(_embed)
    _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        _chat(client, "v1 identical prompt")
        r = _chat(client, "v1 identical prompt")   # EXACT repeat
        assert r.status_code == 200
    # exact hit → the semantic layer never ran for the repeat (only the
    # first request's pre-exec lookup is recorded)
    assert semantic_cache.stats()["lookups"] == 1


def test_key_scoping_in_serving(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "consumer-a", "key": "vk-a"},
        {"id": "consumer-b", "key": "vk-b"}]))
    _enable()
    semantic_cache.set_embedder(_embed)
    fake = _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        ra = _chat(client, "v1 secret question",
                   headers={"Authorization": "Bearer vk-a"})
        assert ra.status_code == 200, ra.text
        # paraphrase of A's prompt under B's key → must NOT serve A's
        # answer; it executes
        rb = _chat(client, "v2 secret question",
                   headers={"Authorization": "Bearer vk-b"})
        assert rb.status_code == 200
        assert len(fake.calls) == 2
    assert semantic_cache.stats()["hits"] == 0, \
        "responses must never cross keys"


def test_metrics_gauge(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _enable()
    semantic_cache.set_embedder(_embed)
    _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        _chat(client, "v1 metric probe")
        m = client.get("/metrics")
        assert "gateway_semantic_cache_entries 1" in m.text
