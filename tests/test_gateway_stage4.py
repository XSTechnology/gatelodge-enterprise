"""LLM Gateway Stage 4 — bearer auth, admin endpoints, queue admission.

Auth: 401 without/with a wrong key when gateway_keys configured (open when
empty); hot-reload of keys. Admin: alias upsert persists to the config
file and is immediately usable; delete removes; invalid chains 400 without
touching the file. Load/unload control the embedded instance registry.
Queue: over-capacity concurrent chats 429 with Retry-After.
"""
import asyncio
import json
import sys
import time
import types
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402


# ── FakeLlama as a context manager (needed for async tests) ────────────────

@contextmanager
def _fake_llama(script=None, delay=0.0):
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.script = list(script or [])

        def create_chat_completion(self, **kwargs):
            if kwargs.get("stream"):
                def _gen():
                    for p in ["Hello", " world"]:
                        yield {"choices": [{"delta": {"content": p}}]}
                return _gen()
            if delay:
                time.sleep(delay)
            records.append({"call": kwargs})
            if self.script:
                return self.script.pop(0)
            return {"choices": [{"message": {"content": "local answer"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        yield records
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)


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
        gw_app._chat_slots.clear()
        return cfgfile, cfg, engine
    return _wire


def _config(gguf, *, keys=None, max_waiting=4):
    cfg = {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {"local": {"type": "embedded", "models": {
            "fast": {"gguf": str(gguf), "ctx": 1024, "placement": ""}}}},
        "aliases": {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
        "queue": {"max_waiting": max_waiting, "timeout_s": 120},
    }
    if keys is not None:
        cfg["gateway_keys"] = keys
    return cfg


AUTH = {"Authorization": "Bearer sekrit"}


# ── auth ───────────────────────────────────────────────────────────────────

def test_auth_open_when_no_keys(wired, gguf):
    wired(_config(gguf))  # no gateway_keys key at all
    with TestClient(gw_app.app) as client:
        for path in ("/v1/models", "/healthz", "/admin/aliases"):
            assert client.get(path).status_code == 200, path


def test_auth_401_without_or_wrong_key(wired, gguf):
    wired(_config(gguf, keys=["sekrit"]))
    with TestClient(gw_app.app) as client:
        for path in ("/v1/models", "/healthz", "/admin/aliases"):
            r = client.get(path)
            assert r.status_code == 401, path
            err = r.json()["error"]
            assert err["code"] == "invalid_api_key"
        r = client.get("/v1/models", headers={"Authorization": "Bearer nope"})
        assert r.status_code == 401
        r = client.get("/v1/models", headers=AUTH)
        assert r.status_code == 200


def test_auth_covers_chat(wired, gguf):
    wired(_config(gguf, keys=["sekrit"]))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "fast", "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 401


def test_auth_hot_reload_of_keys(wired, gguf):
    cfgfile, cfg, _ = wired(_config(gguf, keys=["sekrit"]))
    with TestClient(gw_app.app) as client:
        assert client.get("/v1/models").status_code == 401
        # operator removes all keys on disk -> open again, no restart
        cfgd = json.loads(cfgfile.read_text(encoding="utf-8"))
        cfgd["gateway_keys"] = []
        cfgfile.write_text(json.dumps(cfgd), encoding="utf-8")
        cfg.reload_now()
        assert client.get("/v1/models").status_code == 200


# ── admin: alias CRUD ──────────────────────────────────────────────────────

def test_admin_alias_upsert_persists_and_serves(wired, gguf):
    cfgfile, cfg, _ = wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/aliases", json={
            "name": "smol", "chain": ["local:fast"], "hedge_s": 0})
        assert r.status_code == 200, r.text
        assert r.json()["upserted"] == "smol"
        # persisted to the config file
        on_disk = json.loads(cfgfile.read_text(encoding="utf-8"))
        assert on_disk["aliases"]["smol"] == {"chain": ["local:fast"],
                                              "hedge_s": 0.0}
        # listed
        assert "smol" in client.get("/admin/aliases").json()["aliases"]

        def fn(records):
            rr = client.post("/v1/chat/completions", json={
                "model": "smol",
                "messages": [{"role": "user", "content": "hi"}]})
            assert rr.status_code == 200, rr.text
            assert rr.json()["model"] == "smol"  # immediately servable

        with _fake_llama():
            fn(None)


def test_admin_alias_delete(wired, gguf):
    cfgfile, cfg, _ = wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        assert client.post("/admin/aliases", json={
            "name": "smol", "chain": ["local:fast"]}).status_code == 200
        r = client.post("/admin/aliases", json={"name": "smol", "delete": True})
        assert r.status_code == 200
        assert "smol" not in client.get("/admin/aliases").json()["aliases"]
        # gone from disk too
        on_disk = json.loads(cfgfile.read_text(encoding="utf-8"))
        assert "smol" not in on_disk["aliases"]
        # and no longer usable
        rr = client.post("/v1/chat/completions", json={
            "model": "smol", "messages": [{"role": "user", "content": "hi"}]})
        assert rr.status_code == 400


def test_admin_alias_invalid_chain_400(wired, gguf):
    cfgfile, cfg, _ = wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/aliases", json={
            "name": "bad", "chain": ["local:nope"]})
        assert r.status_code == 400
        assert "no model" in r.json()["error"]["message"]
        # file untouched
        on_disk = json.loads(cfgfile.read_text(encoding="utf-8"))
        assert "bad" not in on_disk.get("aliases", {})
        # delete of a missing alias
        r2 = client.post("/admin/aliases", json={"name": "ghost", "delete": True})
        assert r2.status_code == 400


# ── admin: load / unload ───────────────────────────────────────────────────

def test_admin_load_and_unload(wired, gguf):
    _, cfg, engine = wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        with _fake_llama() as records:
            r = client.post("/admin/load", json={"model": "fast"})
            assert r.status_code == 200, r.text
            loaded = r.json()["loaded"]
            assert [m["alias"] for m in loaded] == ["fast"]
            assert any("load" in rec for rec in records)

            # unload one
            r2 = client.post("/admin/unload", json={"model": "fast"})
            assert r2.status_code == 200
            assert r2.json()["unloaded"] == 1
            assert r2.json()["loaded"] == []

            # load again, then unload all
            client.post("/admin/load", json={"model": "fast"})
            r3 = client.post("/admin/unload", json={"all": True})
            assert r3.json()["unloaded"] == 1
            assert not engine.is_loaded()


def test_admin_load_unknown_model_400(wired, gguf):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/load", json={"model": "nope"})
        assert r.status_code == 400


# ── queue admission ────────────────────────────────────────────────────────

async def test_queue_429_when_over_capacity(wired, gguf):
    """max_waiting 0 → capacity 1: a second concurrent chat gets 429."""
    wired(_config(gguf, max_waiting=0))
    transport = httpx.ASGITransport(app=gw_app.app)
    with _fake_llama(delay=1.0):
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as client:
            first = asyncio.create_task(client.post(
                "/v1/chat/completions",
                json={"model": "fast",
                      "messages": [{"role": "user", "content": "hi"}]}))
            await asyncio.sleep(0.3)  # let the first one get in flight
            second = await client.post(
                "/v1/chat/completions",
                json={"model": "fast",
                      "messages": [{"role": "user", "content": "hi"}]})
            assert second.status_code == 429
            assert second.headers.get("retry-after") == "1"
            assert second.json()["error"]["code"] == "rate_limited"
            r1 = await first
            assert r1.status_code == 200
    assert not gw_app._chat_slots  # slot released


async def test_queue_admits_within_capacity(wired, gguf):
    """max_waiting 4 → a second concurrent chat waits and succeeds."""
    wired(_config(gguf, max_waiting=4))
    transport = httpx.ASGITransport(app=gw_app.app)
    with _fake_llama(delay=0.5):
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as client:
            first = asyncio.create_task(client.post(
                "/v1/chat/completions",
                json={"model": "fast",
                      "messages": [{"role": "user", "content": "hi"}]}))
            await asyncio.sleep(0.15)
            second = await client.post(
                "/v1/chat/completions",
                json={"model": "fast",
                      "messages": [{"role": "user", "content": "hi"}]})
            assert second.status_code == 200
            r1 = await first
            assert r1.status_code == 200
    assert not gw_app._chat_slots


# ── zombie slot reaping (regression: abandoned streams used to wedge
#    the admission counter into a permanent 429 wall) ──────────────────────

async def test_stale_slot_reaped_by_next_request(wired, gguf):
    """A slot held past the stale threshold (abandoned stream / hung
    backend) is evicted by the next admission check — the gateway
    recovers instead of 429ing forever."""
    wired(_config(gguf, max_waiting=0))  # capacity 1
    # Simulate an abandoned stream: acquired, never released, aged out.
    sid = gw_app._acquire_chat_slot()
    gw_app._chat_slots[sid] -= gw_app._slot_stale_s(None) + 60.0
    assert len(gw_app._chat_slots) == 1

    transport = httpx.ASGITransport(app=gw_app.app)
    with _fake_llama():
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as client:
            r = await client.post(
                "/v1/chat/completions",
                json={"model": "fast",
                      "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200  # zombie evicted, not 429
    assert not gw_app._chat_slots  # only the live request's slot, released


async def test_fresh_slot_not_reaped(wired, gguf):
    """A slot held for less than the stale threshold still counts —
    the reaper must not over-release in-flight requests."""
    wired(_config(gguf, max_waiting=0))  # capacity 1
    sid = gw_app._acquire_chat_slot()  # fresh, still "in flight"
    try:
        transport = httpx.ASGITransport(app=gw_app.app)
        with _fake_llama():
            async with httpx.AsyncClient(transport=transport,
                                         base_url="http://test") as client:
                r = await client.post(
                    "/v1/chat/completions",
                    json={"model": "fast",
                          "messages": [{"role": "user", "content": "hi"}]})
                assert r.status_code == 429  # capacity genuinely taken
    finally:
        gw_app._release_chat_slot(sid)
    assert not gw_app._chat_slots


async def test_stream_slot_released_on_backend_error(wired, gguf):
    """A streaming request whose backend raises mid-stream releases its
    slot in the generator finally (the 502 path observed live)."""
    wired(_config(gguf, max_waiting=0))

    @contextmanager
    def _boom_llama():
        mod = types.ModuleType("llama_cpp")

        class BoomLlama:
            def __init__(self, **kwargs):
                pass

            def create_chat_completion(self, **kwargs):
                raise RuntimeError("prompt tokens (999) exceed context "
                                   "window of 4096")

        mod.Llama = BoomLlama
        real = sys.modules.get("llama_cpp")
        sys.modules["llama_cpp"] = mod
        try:
            yield []
        finally:
            if real is not None:
                sys.modules["llama_cpp"] = real
            else:
                sys.modules.pop("llama_cpp", None)

    transport = httpx.ASGITransport(app=gw_app.app)
    with _boom_llama():
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as client:
            async with client.stream(
                    "POST", "/v1/chat/completions",
                    json={"model": "fast", "stream": True,
                          "messages": [{"role": "user", "content": "hi"}]}) as r:
                assert r.status_code == 200  # SSE headers already sent
                body = (await r.aread()).decode()
            assert "exceed context" in body  # error surfaced in-stream
    assert not gw_app._chat_slots  # slot released despite mid-stream error
