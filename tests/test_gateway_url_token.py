"""URL-token auth tests (v2.x) — keyless clients whose only
configurable field is an endpoint URL (the "no key field, just a URL"
AI products): auth.token_forms gates the forms, both OFF by default.

    PATH form  /<token>/v1/...   → middleware rewrites to canonical
    /v1/... and the token resolves through the SAME _resolve_bearer —
    budgets, RBAC, rate limits and spend attribution are identical to
    a bearer client (proved here by the key-ledger charge),

    QUERY form /v1/...?key=...   → separately gated (tokens in query
    strings surface in proxy logs even more eagerly),

    precedence: Authorization header > path token > query token,

    shape guard: ONLY /<token>/v1/... rewrites — reserved top-level
    segments (admin, mcp, scim, teams, ...) are never URL-token
    routes, so no admin/MCP surface is reachable via URL token,

    reset discipline: the token ContextVar resets on every request —
    no cross-request leakage.
"""
import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
from gateway.storage import reset_storage

from test_gateway_stage3 import (gguf, wired, _config,  # noqa: F401
                                 _secured_config, _with_fake_llama)


def _tok_config(gguf, forms, api_keys=None):
    cfgd = _secured_config(gguf, api_keys=api_keys)
    cfgd["auth"] = {"token_forms": forms}
    return cfgd


# ── config parsing ─────────────────────────────────────────────────────────

def test_token_forms_default_off(wired, gguf):  # noqa: F811
    """No auth section → no URL-token forms: bearer-only, byte-for-byte
    the behavior of every prior release. /<token>/v1/... is just an
    unknown route (404 — no existence signal)."""
    wired(_secured_config(gguf))
    cfg = gw_config.get_gateway_config()
    assert cfg.auth_token_forms == set()
    with TestClient(gw_app.app) as client:
        # valid bearer: auth passes, the route is unknown → 404. (An
        # unauthenticated request would 401 first — auth runs before
        # routing; that is the pre-existing posture, unchanged.)
        r = client.get("/admin-secret/v1/models",
                       headers={"Authorization": "Bearer admin-secret"})
        assert r.status_code == 404          # never rewritten
        r = client.get("/v1/models?key=admin-secret")
        assert r.status_code == 401          # query ignored


def test_token_forms_parsing(wired, gguf):  # noqa: F811
    """Unknown form names drop silently; casing/whitespace normalize."""
    wired(_tok_config(gguf, ["Path", " query ", "bogus"]))
    cfg = gw_config.get_gateway_config()
    assert cfg.auth_token_forms == {"path", "query"}


# ── path form ──────────────────────────────────────────────────────────────

def test_path_form_serves_gateway_key(wired, gguf):  # noqa: F811
    """/<gateway-key>/v1/... serves with NO Authorization header — the
    endpoint URL IS the credential."""
    wired(_tok_config(gguf, ["path"]))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin-secret/v1/models")
        assert r.status_code == 200, r.text
        # canonical path serves too (rewrite changes nothing for it)
        r = client.get("/v1/models",
                       headers={"Authorization": "Bearer admin-secret"})
        assert r.status_code == 200


def test_path_form_bad_token_401(wired, gguf):  # noqa: F811
    """A rewritten path with an unknown token is an AUTH failure (401)
    — not a 404: the route exists, the credential doesn't."""
    wired(_tok_config(gguf, ["path"]))
    with TestClient(gw_app.app) as client:
        r = client.get("/wrong-key/v1/models")
        assert r.status_code == 401
        assert r.json()["error"]["code"] == "invalid_api_key"


def test_path_form_charges_virtual_key_scope(wired, gguf):  # noqa: F811
    """The strong attribution test: a full chat completion through the
    PATH form charges the virtual key's ledger — the URL token flows
    through the same resolution + charge machinery as a bearer."""
    wired(_tok_config(gguf, ["path"], api_keys=[
        {"id": "program-x", "key": "vk-team"}]))
    try:
        gw_app._scope_spend("key:program-x")     # cache 0 first

        def fn(records):
            with TestClient(gw_app.app) as client:
                r = client.post("/vk-team/v1/chat/completions",
                                json={"model": "local:fast",
                                      "messages": [{"role": "user",
                                                    "content": "hi"}]})
                assert r.status_code == 200, r.text
                assert r.json()["usage"]["total_tokens"] == 5
        _with_fake_llama(fn)
        assert gw_app._scope_spend("key:program-x")["tokens"] == 5
    finally:
        reset_storage()


def test_path_form_never_reaches_control_plane(wired, gguf):  # noqa: F811
    """Only /<token>/v1/... rewrites: reserved top-level segments and
    any other shape stay unknown routes — the admin console, /mcp,
    /scim and /teams are NOT reachable via URL token."""
    wired(_tok_config(gguf, ["path"]))
    with TestClient(gw_app.app) as client:
        # valid bearer so auth passes — a 404 here can ONLY mean the
        # path was never rewritten (the router found no such route)
        hdrs = {"Authorization": "Bearer admin-secret"}
        for path in ("/admin-secret/admin/stats",   # token then admin
                     "/admin/v1/models",            # reserved segment
                     "/mcp-key/mcp", "/t/scim/v2/Users",
                     "/admin-secret/healthz",       # not the v1 shape
                     "/admin-secret/"):             # bare token, no v1
            r = client.get(path, headers=hdrs)
            assert r.status_code == 404, path


# ── query form ─────────────────────────────────────────────────────────────

def test_query_form_gated(wired, gguf):  # noqa: F811
    """?key= works ONLY when "query" is enabled (it leaks into proxy
    logs more eagerly than the path form — separate opt-in)."""
    wired(_tok_config(gguf, ["path"]))          # path only
    with TestClient(gw_app.app) as client:
        r = client.get("/v1/models?key=admin-secret")
        assert r.status_code == 401
    wired(_tok_config(gguf, ["query"]))
    with TestClient(gw_app.app) as client:
        r = client.get("/v1/models?key=admin-secret")
        assert r.status_code == 200


# ── precedence + reset discipline ──────────────────────────────────────────

def test_header_beats_path_and_query(wired, gguf):  # noqa: F811
    """Authorization header is authoritative: a BAD header wins over a
    good path token (explicit beats ambient), and a good header beats
    a bad query key."""
    wired(_tok_config(gguf, ["path", "query"]))
    with TestClient(gw_app.app) as client:
        # bad header + good path token → 401 (header used)
        r = client.get("/admin-secret/v1/models",
                       headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401
        # good header + bad query key → 200 (header wins)
        r = client.get("/v1/models?key=wrong",
                       headers={"Authorization": "Bearer admin-secret"})
        assert r.status_code == 200


def test_no_cross_request_token_leakage(wired, gguf):  # noqa: F811
    """The URL-token ContextVar resets on EVERY request — a request
    after a path-form request carries no residue (401, not 200)."""
    wired(_tok_config(gguf, ["path"]))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin-secret/v1/models")
        assert r.status_code == 200
        r = client.get("/v1/models")            # no header, no token
        assert r.status_code == 401


def test_open_mode_path_form_still_serves(wired, gguf):  # noqa: F811
    """Open mode (no keys configured): the rewrite happens but the
    token is moot — /anything/v1/... serves like /v1/...."""
    cfgd = _config(gguf)                        # no gateway_keys
    cfgd["auth"] = {"token_forms": ["path"]}
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        r = client.get("/whatever/v1/models")
        assert r.status_code == 200
