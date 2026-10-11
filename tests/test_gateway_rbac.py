"""RBAC canary tests — role-scoped admin keys via the auth seam.

Same discipline as tests/test_gateway_seams.py: the REAL governed
pipeline is swapped in and SERVING behavior is asserted — never the
link in isolation. Every test restores the default pipeline in
finally.
"""
import hashlib
import json
import logging
import os
import types

from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway import rbac as gw_rbac
from gateway.auth import (AuthPipeline, admin_gate_link, rbac_link,
                          rate_limit_link, resolve_key_link,
                          set_auth_pipeline)
from gateway.storage import reset_storage

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

OPS = {"key": "ops-secret", "role": "ops_readonly"}
KM = {"key": "km-secret", "role": "key_manager"}


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _rbac_config(gguf, roles=None, api_keys=None,
                 gateway_keys=("admin-secret", "ops-secret", "km-secret")):
    """Secured config + optional admin_key_roles section."""
    cfgd = _secured_config(gguf, api_keys=api_keys, gateway_keys=gateway_keys)
    if roles is not None:
        cfgd["admin_key_roles"] = roles
    return cfgd


def _wire_enterprise():
    """The governed pipeline — the core default since governance moved
    into the standard distribution (still set explicitly: canary
    discipline)."""
    set_auth_pipeline(AuthPipeline([resolve_key_link, rbac_link,
                                    admin_gate_link, rate_limit_link]))


# ── backward compat + defaults ─────────────────────────────────────────────

def test_no_roles_section_is_full_admin(wired, gguf):
    """No admin_key_roles section → every admin key keeps full access
    (pre-C-lite behavior, byte for byte)."""
    wired(_rbac_config(gguf, roles=None))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/reload", headers=auth("admin-secret"))
            assert r.status_code == 200, r.text
            r2 = client.get("/admin/config", headers=auth("admin-secret"))
            assert r2.status_code == 200
    finally:
        set_auth_pipeline(AuthPipeline())


def test_unassigned_admin_key_is_full_admin(wired, gguf):
    """A key absent from admin_key_roles defaults to full admin."""
    wired(_rbac_config(gguf, roles=[OPS]))  # only ops-secret is scoped
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/reload", headers=auth("admin-secret"))
            assert r.status_code == 200, r.text
    finally:
        set_auth_pipeline(AuthPipeline())


# ── the roles ──────────────────────────────────────────────────────────────

def test_ops_readonly_role(wired, gguf):
    """ops_readonly: admin GETs pass, /admin/config (raw key material)
    and every mutation 403 with the rbac_forbidden code."""
    wired(_rbac_config(gguf, roles=[OPS]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            ok = client.get("/admin/stats", headers=auth("ops-secret"))
            assert ok.status_code == 200, ok.text
            ok2 = client.get("/admin/verify", headers=auth("ops-secret"))
            assert ok2.status_code == 200
            for r in (client.get("/admin/config", headers=auth("ops-secret")),
                      client.post("/admin/reload", headers=auth("ops-secret")),
                      client.post("/admin/runpod/inventory",
                                  headers=auth("ops-secret"))):
                assert r.status_code == 403, r.text
                assert r.json()["error"]["code"] == "rbac_forbidden"
                assert "ops_readonly" in r.json()["error"]["message"]
    finally:
        set_auth_pipeline(AuthPipeline())


def test_key_manager_role(wired, gguf):
    """key_manager: virtual-key CRUD + stats/secrets reads; the admin
    key surface itself (/admin/gateway-keys) and raw config 403."""
    wired(_rbac_config(gguf, roles=[KM]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            created = client.post("/admin/keys", headers=auth("km-secret"),
                                  json={"id": "km-made-key",
                                        "owner": "km"})
            assert created.status_code == 200, created.text
            assert created.json()["created"] == "km-made-key"
            listed = client.get("/admin/keys", headers=auth("km-secret"))
            assert listed.status_code == 200
            assert "km-made-key" in [k["id"] for k in listed.json()["api_keys"]]
            assert client.get("/admin/stats",
                              headers=auth("km-secret")).status_code == 200
            assert client.get("/admin/secrets",
                              headers=auth("km-secret")).status_code == 200
            for r in (client.post("/admin/gateway-keys",
                                  headers=auth("km-secret"),
                                  json={"key": "minted-by-km"}),
                      client.get("/admin/config",
                                 headers=auth("km-secret")),
                      client.post("/admin/reload",
                                  headers=auth("km-secret"))):
                assert r.status_code == 403, r.text
                assert r.json()["error"]["code"] == "rbac_forbidden"
    finally:
        set_auth_pipeline(AuthPipeline())


def test_hash_form_role_entry(wired, gguf):
    """Role entries accept the sha256 form (the at-rest recommendation)
    exactly like gateway_keys does."""
    hashed = hashlib.sha256(b"ops-secret").hexdigest()
    wired(_rbac_config(gguf, roles=[{"key": hashed, "role": "ops_readonly"}]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats",
                              headers=auth("ops-secret")).status_code == 200
            r = client.get("/admin/config", headers=auth("ops-secret"))
            assert r.status_code == 403
    finally:
        set_auth_pipeline(AuthPipeline())


# ── core semantics preserved ───────────────────────────────────────────────

def test_virtual_key_still_gated_by_core(wired, gguf):
    """Virtual keys stay CONSUMER keys: /admin 403 comes from the core
    admin gate (admin_key_required), not from rbac_link."""
    wired(_rbac_config(gguf, roles=[OPS],
                       api_keys=[{"id": "program-x", "key": "vk-test-1"}]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("vk-test-1"))
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "admin_key_required"
    finally:
        set_auth_pipeline(AuthPipeline())


def test_open_mode_rbac_inert(wired, gguf):
    """Open mode (no keys configured): the admin surface stays open —
    rbac_link must not close what the core leaves open."""
    wired(_config(gguf))  # no gateway_keys / api_keys
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats").status_code == 200
    finally:
        set_auth_pipeline(AuthPipeline())


def test_serving_surface_unaffected(wired, gguf, monkeypatch):
    """RBAC governs /admin only: an ops_readonly ADMIN key still serves
    /v1 traffic (core behavior — admin keys are unlimited consumers)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_rbac_config(gguf, roles=[OPS]))
    _patch_cloud(monkeypatch, _FakeCloud())
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", headers=auth("ops-secret"),
                            json={"model": "groq:cloud-fast",
                                  "messages": [{"role": "user",
                                                "content": "serving probe"}]})
            assert r.status_code == 200, r.text
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


# ── failure modes ──────────────────────────────────────────────────────────

def test_unknown_role_fails_closed(wired, gguf, caplog):
    """A typo'd role denies the whole admin surface (403 on a plain
    GET) and warns at parse time — never silently widen access."""
    wired(_rbac_config(gguf, roles=[{"key": "ops-secret",
                                     "role": "superuser"}]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client, \
                caplog.at_level(logging.WARNING):
            r = client.get("/admin/stats", headers=auth("ops-secret"))
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "rbac_forbidden"
        assert any("unknown role" in rec.message for rec in caplog.records)
    finally:
        set_auth_pipeline(AuthPipeline())


def test_role_hot_reload(wired, gguf):
    """Role assignments hot-reload: editing admin_key_roles in the
    config file flips enforcement on the NEXT request, no restart."""
    cfgfile, _, _ = wired(_rbac_config(gguf, roles=[OPS]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/config", headers=auth("ops-secret"))
            assert r.status_code == 403
            cfgd = json.loads(cfgfile.read_text(encoding="utf-8"))
            cfgd["admin_key_roles"] = [{"key": "ops-secret",
                                        "role": "full_admin"}]
            cfgfile.write_text(json.dumps(cfgd), encoding="utf-8")
            os.utime(cfgfile, (2000000000, 2000000000))  # deterministic bump
            r2 = client.get("/admin/config", headers=auth("ops-secret"))
            assert r2.status_code == 200, r2.text
    finally:
        set_auth_pipeline(AuthPipeline())


def test_denial_is_audited(wired, gguf):
    """Denials land in config-audit.jsonl (the /admin/audit trail) as
    admin:<role>:<hash8> — no plaintext key material."""
    wired(_rbac_config(gguf, roles=[OPS]))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/reload", headers=auth("ops-secret"))
            assert r.status_code == 403
        p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
        lines = [json.loads(ln) for ln in
                 p.read_text(encoding="utf-8").strip().splitlines()]
        denies = [l for l in lines
                  if str(l.get("action", "")).startswith("rbac-deny")]
        assert denies, f"no rbac-deny line in {lines}"
        d = denies[-1]
        assert d["action"] == "rbac-deny POST /admin/reload"
        want = ("admin:ops_readonly:"
                + hashlib.sha256(b"ops-secret").hexdigest()[:8])
        assert d["key"] == want
    finally:
        set_auth_pipeline(AuthPipeline())


# ── unit: policy table + parsing ───────────────────────────────────────────

def test_policy_matcher_table():
    assert gw_rbac.allowed("full_admin", "POST", "/admin/reload")
    assert gw_rbac.allowed("full_admin", "GET", "/admin/config")
    assert gw_rbac.allowed("ops_readonly", "GET", "/admin/stats")
    assert gw_rbac.allowed("ops_readonly", "GET", "/admin/models/search")
    assert not gw_rbac.allowed("ops_readonly", "GET", "/admin/config")
    assert not gw_rbac.allowed("ops_readonly", "POST", "/admin/reload")
    assert not gw_rbac.allowed("ops_readonly", "PUT", "/admin/params")
    assert gw_rbac.allowed("key_manager", "GET", "/admin/keys")
    assert gw_rbac.allowed("key_manager", "POST", "/admin/keys")
    assert not gw_rbac.allowed("key_manager", "POST", "/admin/gateway-keys")
    assert not gw_rbac.allowed("key_manager", "GET", "/admin/config")
    # unknown role → no rows → deny everything (fail closed)
    assert not gw_rbac.allowed("superuser", "GET", "/admin/stats")


def test_role_section_parsing():
    entries = gw_rbac._parse_role_entries({"admin_key_roles": [
        {"key": "a", "role": "ops_readonly"},
        {"key": "a", "role": "key_manager"},   # duplicate → first wins
        {"key": "", "role": "ops_readonly"},   # missing key → dropped
        {"key": "b"},                          # missing role → dropped
        "nonsense",                            # not an object → dropped
        {"key": "b", "role": "key_manager", "note": "x"},
    ]})
    assert entries == [
        {"key": "a", "role": "ops_readonly", "note": ""},
        {"key": "b", "role": "key_manager", "note": "x"},
    ]
    # non-list section → empty, no crash
    assert gw_rbac._parse_role_entries({"admin_key_roles": "junk"}) == []
    assert gw_rbac._parse_role_entries({}) == []


def test_dead_entry_lint(caplog):
    """A role entry whose key matches no gateway_keys value is dead
    (the token 401s before RBAC runs) — the linter says so."""
    cfg = types.SimpleNamespace(gateway_keys=["admin-secret"])
    with caplog.at_level(logging.WARNING):
        gw_rbac._lint_entries(
            [{"key": "nowhere-key", "role": "ops_readonly", "note": ""}],
            cfg)
    assert any("dead entry" in rec.message for rec in caplog.records)
