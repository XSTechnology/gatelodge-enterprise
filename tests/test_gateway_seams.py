"""Canary seam tests — prove the three enterprise-fork interfaces are
WIRED, not decorative. A seam that nothing consumes passes isolation
tests all day; these assert that swapping an implementation changes
SERVING behavior (each test names the v1.1.0 decorative-seam defect it
guards against)."""
import json
import os

from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.auth as gw_auth
import gateway.config as gw_config
from gateway.auth import AuthPipeline, set_auth_pipeline
from gateway.storage import InMemoryStorage, reset_storage, set_storage

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)
from test_gateway_rbac import auth  # noqa: F401


class RecordingStorage(InMemoryStorage):
    """Canary backend — records every mutating call, delegates to the
    in-memory implementation. If core state bypassed get_storage(),
    nothing would appear in .writes."""

    def __init__(self):
        super().__init__()
        self.writes = []  # (ns, key)

    def set(self, ns, key, value):
        self.writes.append((ns, key))
        return super().set(ns, key, value)

    def incr(self, ns, key, amount=1.0):
        self.writes.append((ns, key))
        return super().incr(ns, key, amount)

    def delete(self, ns, key):
        self.writes.append((ns, key))
        return super().delete(ns, key)

    def clear(self, ns="*"):
        self.writes.append((ns, "*"))
        return super().clear(ns)

    def namespaces_written(self):
        return {ns for ns, _ in self.writes}


# ── storage seam ───────────────────────────────────────────────────────────

def test_chat_state_flows_through_storage_backend(wired, gguf, monkeypatch):
    """A cacheable chat must WRITE through the swappable backend:
    response-cache and spend-ledger entries appear as backend writes.
    (Guards: module dicts bypassing get_storage() — v1.1.0 defect.)"""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    rec = RecordingStorage()
    set_storage(rec)
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "seam probe one"}]})
            assert r.status_code == 200, r.text
        nss = rec.namespaces_written()
        assert "response_cache" in nss
        assert "spend_ledger" in nss
    finally:
        reset_storage()


def test_rate_windows_ride_storage_backend(wired, gguf, monkeypatch):
    """The RPM limiter (an auth link) must read/write its window through
    the backend — a shared backend rate-limits cluster-wide."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "bursty", "key": "vk-burst", "rate_limit_rpm": 2}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    rec = RecordingStorage()
    set_storage(rec)
    try:
        with TestClient(gw_app.app) as client:
            for i in range(2):
                r = client.post("/v1/chat/completions", headers={
                    "Authorization": "Bearer vk-burst"}, json={
                    "model": "groq:cloud-fast",
                    "messages": [{"role": "user",
                                  "content": f"seam rpm {i}"}]})
                assert r.status_code == 200, r.text
            r3 = client.post("/v1/chat/completions", headers={
                "Authorization": "Bearer vk-burst"}, json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "seam rpm 3"}]})
            assert r3.status_code == 429
        assert ("rate_windows", "bursty") in rec.writes
    finally:
        reset_storage()


# ── auth pipeline seam ─────────────────────────────────────────────────────

def test_auth_pipeline_swap_short_circuits_requests(wired, gguf):
    """set_auth_pipeline() must take effect on the NEXT request. A
    canary deny-link 451s everything — if the middleware ignored the
    active pipeline (v1.1.0 defect), the request would pass."""
    wired(_config(gguf))

    def _deny_everything(cfg, request, key_id):
        return JSONResponse(status_code=451,
                            content={"error": {"message": "canary deny",
                                               "code": "canary"}})

    set_auth_pipeline(AuthPipeline([_deny_everything]))
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/healthz")
            assert r.status_code == 451
            assert r.json()["error"]["code"] == "canary"
    finally:
        set_auth_pipeline(AuthPipeline())  # restore the core default


def test_enterprise_link_order_runs_and_identity_flows(wired, gguf,
                                                       monkeypatch):
    """An enterprise-shaped pipeline (resolve → SSO-ish link → admin
    gate → rate limit) runs every link, and the resolved key id is
    visible to the INSERTED link via the shared ContextVar."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf, api_keys=[
        {"id": "scoped", "key": "vk-scoped"}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    seen = {}

    def _sso_link(cfg, request, key_id):
        seen["key_id"] = key_id
        seen["ctx"] = gw_auth._REQUEST_KEY_ID.get()
        return None

    set_auth_pipeline(AuthPipeline([gw_auth.resolve_key_link, _sso_link,
                                    gw_auth.admin_gate_link,
                                    gw_auth.rate_limit_link]))
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", headers={
                "Authorization": "Bearer vk-scoped"}, json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "sso probe"}]})
            assert r.status_code == 200, r.text
        assert seen["key_id"] == "scoped"
        assert seen["ctx"] == "scoped"
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_contextvars_single_definition():
    """app and auth must share ONE ContextVar object per name — two
    same-named ContextVars are distinct objects and silently stop
    sharing state (the second v1.1.0 defect)."""
    assert gw_app._REQUEST_KEY_ID is gw_auth._REQUEST_KEY_ID
    assert gw_app._REQUEST_ID is gw_auth._REQUEST_ID


# ── config store seam ──────────────────────────────────────────────────────

def test_config_store_injection(tmp_path):
    """set_config_store() is the sanctioned injection: the singleton
    rebuilds from the new store and hot-reload follows the ACTIVE
    store's version token."""
    p1 = tmp_path / "a.json"
    p2 = tmp_path / "b.json"
    p1.write_text(json.dumps({"port": 9911, "default_alias": "from-a"}),
                  encoding="utf-8")
    p2.write_text(json.dumps({"port": 9912, "default_alias": "from-b"}),
                  encoding="utf-8")
    original = gw_config._CONFIG
    try:
        cfg_a = gw_config.set_config_store(gw_config.FileConfigStore(p1))
        assert cfg_a.default_alias == "from-a"
        assert gw_config.get_gateway_config() is cfg_a
        cfg_b = gw_config.set_config_store(gw_config.FileConfigStore(p2))
        assert cfg_b.default_alias == "from-b"
        assert gw_config.get_gateway_config() is cfg_b
        # hot reload follows the ACTIVE store (bump its version token
        # deterministically — mtime equality is not guaranteed)
        p2.write_text(json.dumps({"port": 9912,
                                  "default_alias": "from-b-v2"}),
                      encoding="utf-8")
        os.utime(p2, (2000000000, 2000000000))
        cfg_b.maybe_reload()
        assert gw_config.get_gateway_config().default_alias == "from-b-v2"
    finally:
        gw_config._CONFIG = original


# ── extra-router seam (the fourth seam) ────────────────────────────────────

def _probe_router():
    from fastapi import APIRouter
    r = APIRouter()

    @r.get("/hello")
    def _hello():
        return {"surface": "probe"}

    return r


def test_extra_router_mounts_and_owns_auth(wired, gguf):
    """set_extra_router mounts an enterprise surface whose routes SERVE
    (through the same middleware — request ids echo) while SKIPPING the
    core auth pipeline: its bearer is not a gateway credential (the
    SCIM shape). With gateway keys configured, a bare request to the
    mounted route still succeeds — the ROUTER owns its auth."""
    wired(_secured_config(gguf))   # keys configured → pipeline active
    gw_app.set_extra_router(_probe_router(), prefix="/probe")
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/probe/hello")       # NO bearer at all
            assert r.status_code == 200, r.text
            assert r.json() == {"surface": "probe"}
            assert r.headers.get("x-request-id")  # middleware still wraps
            # outside the prefix, the core pipeline is untouched
            r2 = client.get("/admin/stats")
            assert r2.status_code == 401
    finally:
        gw_app.set_extra_router(None, prefix="/probe")


def test_extra_router_replace_semantics(wired, gguf):
    """A second call swaps the previous router: the new prefix serves,
    and the STALE prefix falls back under the core pipeline (401 with
    keys configured — the pipeline short-circuits before routing)."""
    from fastapi import APIRouter
    wired(_secured_config(gguf))
    gw_app.set_extra_router(_probe_router(), prefix="/probe")
    r2 = APIRouter()

    @r2.get("/other")
    def _other():
        return {"surface": "two"}

    gw_app.set_extra_router(r2, prefix="/second")
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/second/other").status_code == 200
            # the stale prefix no longer bypasses the pipeline — with
            # keys configured that means 401 (with or without a bogus
            # bearer), never the mounted route again
            assert client.get("/probe/hello").status_code == 401
            assert client.get("/probe/hello",
                              headers={"Authorization": "Bearer x"}
                              ).status_code == 401
    finally:
        gw_app.set_extra_router(None, prefix="/second")


def test_extra_router_additive_two_surfaces(wired, gguf):
    """v2.0 seam extension: add_extra_router mounts a SECOND surface
    alongside the first (SCIM + MCP coexist). Both prefixes serve and
    both skip the core pipeline; a repeated mount of the same prefix is
    a no-op; set_extra_router(None) still removes EVERYTHING."""
    from fastapi import APIRouter
    wired(_secured_config(gguf))
    gw_app.set_extra_router(_probe_router(), prefix="/probe")
    r2 = APIRouter()

    @r2.get("/mcp-hello")
    def _mcp_hello():
        return {"surface": "mcp"}

    gw_app.add_extra_router(r2, prefix="/mcp", auth="owned")
    gw_app.add_extra_router(r2, prefix="/mcp", auth="owned")  # idempotent
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/probe/hello").status_code == 200
            assert client.get("/mcp/mcp-hello").status_code == 200
            assert client.get("/mcp/mcp-hello").json() == {"surface": "mcp"}
            # outside both prefixes the core pipeline is untouched
            assert client.get("/admin/stats").status_code == 401
    finally:
        gw_app.set_extra_router(None, prefix="/probe")
        assert "/mcp" not in gw_app._EXTRA_ROUTER_PREFIXES
        assert "/probe" not in gw_app._EXTRA_ROUTER_PREFIXES


def test_extra_router_additive_alone(wired, gguf):
    """add_extra_router works without any prior set_extra_router — and
    the None cleanup still removes it (the mark is recorded on first
    additive mount too)."""
    wired(_secured_config(gguf))
    gw_app.add_extra_router(_probe_router(), prefix="/solo", auth="owned")
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/solo/hello").status_code == 200
    finally:
        gw_app.set_extra_router(None, prefix="/solo")
        assert "/solo" not in gw_app._EXTRA_ROUTER_PREFIXES


def test_extra_router_requires_auth_declaration(wired, gguf):
    """GL-R16: an undeclared auth is a startup FAILURE, not a silent
    exposure — and auth='admin' gets the CORE gate chain (resolve →
    RBAC → admin gate), byte-identical to /admin itself."""
    from fastapi import APIRouter
    wired(_secured_config(gguf))
    # undeclared → refuse to mount
    try:
        gw_app.add_extra_router(_probe_router(), prefix="/oops")
        assert False, "undeclared auth must raise"
    except RuntimeError as e:
        assert "auth must be declared" in str(e)
    # invalid value → refuse
    try:
        gw_app.add_extra_router(_probe_router(), prefix="/oops",
                                auth="maybe")
        assert False, "invalid auth must raise"
    except RuntimeError:
        pass
    # auth='admin': the CORE enforces the admin gate on the prefix
    r = APIRouter()

    @r.get("/stats")
    def _stats():
        return {"surface": "core-gated"}

    gw_app.add_extra_router(r, prefix="/ext-admin", auth="admin")
    try:
        with TestClient(gw_app.app) as client:
            # no bearer → 401 (the core admin gate's exact behavior)
            assert client.get("/ext-admin/stats").status_code == 401
            # a valid admin key passes — the router needed no guard
            assert client.get("/ext-admin/stats", headers=auth(
                "admin-secret")).status_code == 200
    finally:
        gw_app.set_extra_router(None, prefix="/ext-admin")
