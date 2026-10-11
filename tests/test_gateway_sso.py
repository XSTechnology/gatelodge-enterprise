"""SSO/OIDC canary tests — admin auth via the auth seam.

Same discipline as tests/test_gateway_rbac.py: the REAL governed
pipeline (with resolve_link) is
swapped in and SERVING behavior asserted. The introspection transport
is monkeypatched (gateway.sso.introspect) and so is the JWKS fetch
(gateway.sso._jwks_fetch) — no real HTTP anywhere.
"""
import hashlib
import json
import logging
import time

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa as _rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm
import jwt as pyjwt

import gateway.app as gw_app
import gateway.config as gw_config
from gateway import sso as gw_sso
from gateway.auth import (AuthPipeline, admin_gate_link, rate_limit_link,
                          rbac_link, resolve_link, set_auth_pipeline)
from gateway.identity import get_identity, set_identity
from gateway.storage import reset_storage

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config,  # noqa: F401
                                 _secured_config)

ADMIN_CLAIMS = {"sub": "user-1", "email": "alice@corp.example",
                "groups": ["gatelodge-full-admins"]}
OPS_CLAIMS = {"sub": "user-2", "email": "bob@corp.example",
              "groups": ["everyone"]}


class FakeIdP:
    """introspect() replacement: counts calls, returns configured
    claims / inactive / transport failure."""

    def __init__(self, claims=None, active=True, unavailable=False):
        self.calls = 0
        self.claims = claims or {}
        self.active = active
        self.unavailable = unavailable

    async def __call__(self, token, settings):
        self.calls += 1
        if self.unavailable:
            raise gw_sso.SSOUnavailable("idp down")
        if not self.active:
            return {}
        return dict(self.claims)


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _sso_section(**over) -> dict:
    s = {
        "enabled": True,
        "introspection_url": "https://idp.test/introspect",
        "client_id": "gatelodge",
        "client_secret_env": "TEST_SSO_SECRET",
        "role_rules": [{"claim": "groups",
                        "value": "gatelodge-full-admins",
                        "role": "full_admin"}],
        "default_role": "ops_readonly",
    }
    s.update(over)
    return s


def _sso_config(gguf, sso=None, gateway_keys=("admin-secret",),
                api_keys=None):
    cfgd = _secured_config(gguf, api_keys=api_keys, gateway_keys=gateway_keys)
    if sso is not None:
        cfgd["sso"] = sso
    return cfgd


def _wire_enterprise():
    """The governed pipeline — the core default since governance moved
    into the standard distribution (still set explicitly: canary
    discipline)."""
    set_auth_pipeline(AuthPipeline([resolve_link, rbac_link,
                                    admin_gate_link, rate_limit_link]))


# ── delegation: no sso section / sso disabled ──────────────────────────────

def test_sso_absent_delegates_to_core(wired, gguf):
    """No sso section → resolve_link is byte-for-byte the core resolver:
    gateway keys work, unknown (even JWT-shaped) tokens 401."""
    wired(_sso_config(gguf, sso=None))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats",
                              headers=auth("admin-secret")).status_code == 200
            r = client.get("/admin/stats", headers=auth("aaa.bbb.ccc"))
            assert r.status_code == 401
            assert r.json()["error"]["code"] == "invalid_api_key"
    finally:
        set_auth_pipeline(AuthPipeline())


def test_sso_secret_missing_disables_sso(wired, gguf, monkeypatch, caplog):
    """sso.enabled but the client secret env is unset → SSO disabled
    with an ERROR log (fail closed): the JWT token then 401s via the
    core path; gateway keys unaffected."""
    monkeypatch.delenv("TEST_SSO_SECRET", raising=False)
    wired(_sso_config(gguf, sso=_sso_section()))
    fake = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr(gw_sso, "introspect", fake)
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client, \
                caplog.at_level(logging.ERROR):
            assert client.get("/admin/stats",
                              headers=auth("admin-secret")).status_code == 200
            r = client.get("/admin/stats", headers=auth("aaa.bbb.ccc"))
            assert r.status_code == 401
        assert any("SSO DISABLED" in rec.message for rec in caplog.records)
        assert fake.calls == 0
    finally:
        set_auth_pipeline(AuthPipeline())


# ── the SSO path ───────────────────────────────────────────────────────────

def test_active_token_full_admin_via_rule(wired, gguf, monkeypatch):
    """OIDC-shaped bearer + active introspection with a matching groups
    rule → full_admin: reads AND mutations pass (the canary assertion —
    a token the core resolver alone would 401 now serves)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(ADMIN_CLAIMS))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("aaa.bbb.ccc"))
            assert r.status_code == 200, r.text
            r2 = client.post("/admin/reload", headers=auth("aaa.bbb.ccc"))
            assert r2.status_code == 200, r2.text
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_no_matching_rule_defaults_readonly(wired, gguf, monkeypatch):
    """SSO principal without a matching rule → default_role
    (ops_readonly): reads pass, /admin/config and mutations 403, and
    the denial is audited under the SSO identity."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(OPS_CLAIMS))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats",
                              headers=auth("jjj.kkk.lll")).status_code == 200
            r = client.get("/admin/config", headers=auth("jjj.kkk.lll"))
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "rbac_forbidden"
            assert "ops_readonly" in r.json()["error"]["message"]
            r2 = client.post("/admin/reload", headers=auth("jjj.kkk.lll"))
            assert r2.status_code == 403
        p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
        denies = [json.loads(ln) for ln in
                  p.read_text(encoding="utf-8").strip().splitlines()
                  if "rbac-deny" in ln]
        assert denies, "no rbac-deny audit lines"
        cfg_denies = [d for d in denies
                      if d["action"] == "rbac-deny GET /admin/config"]
        assert cfg_denies
        assert cfg_denies[-1]["key"] == "sso:bob@corp.example"
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_inactive_token_401(wired, gguf, monkeypatch):
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(active=False))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("mmm.nnn.ooo"))
            assert r.status_code == 401
            assert r.json()["error"]["code"] == "invalid_api_key"
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_audience_mismatch_401(wired, gguf, monkeypatch):
    """Configured audience not among the token's aud claim(s) → 401
    (checked in authenticate, before caching)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section(audience="gatelodge-api")))
    monkeypatch.setattr(gw_sso, "introspect",
                        FakeIdP({**ADMIN_CLAIMS, "aud": "other-api"}))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("ppp.qqq.rrr"))
            assert r.status_code == 401
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


# ── failure modes: the never-open invariant ────────────────────────────────

def test_idp_down_fail_closed_503(wired, gguf, monkeypatch):
    """Transport error with fail_open: false (default) → 503
    sso_unavailable. /admin is NOT reachable with the unvalidated
    token."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(unavailable=True))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("sss.ttt.uuu"))
            assert r.status_code == 503
            assert r.json()["error"]["code"] == "sso_unavailable"
            assert "Retry-After" in r.headers
    finally:
        set_auth_pipeline(AuthPipeline())


def test_idp_down_fail_open_with_keys_401(wired, gguf, monkeypatch):
    """fail_open: true + keys configured → delegates to the core
    resolver → 401 (an unvalidated OIDC token is just an unknown
    token). /admin never opens."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section(fail_open=True)))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(unavailable=True))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("vvv.www.xxx"))
            assert r.status_code == 401
            assert r.json()["error"]["code"] == "invalid_api_key"
    finally:
        set_auth_pipeline(AuthPipeline())


def test_idp_down_fail_open_open_mode_passes(wired, gguf, monkeypatch):
    """fail_open: true + true open mode (no keys) → behaves exactly as
    core open mode: request passes unauthenticated."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section(fail_open=True),
                      gateway_keys=None))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(unavailable=True))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("yyy.zzz.AAA"))
            assert r.status_code == 200, r.text
    finally:
        set_auth_pipeline(AuthPipeline())


# ── dispatch correctness ───────────────────────────────────────────────────

def test_gateway_key_bypasses_sso(wired, gguf, monkeypatch):
    """A non-OIDC-shaped gateway key never touches the IdP (dispatch
    is shape-first) — works even while the IdP is down."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    fake = FakeIdP(unavailable=True)
    monkeypatch.setattr(gw_sso, "introspect", fake)
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats",
                              headers=auth("admin-secret")).status_code == 200
        assert fake.calls == 0
    finally:
        set_auth_pipeline(AuthPipeline())


def test_token_prefix_dispatch(wired, gguf, monkeypatch):
    """token_prefixes force the SSO path for non-JWT tokens."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section(token_prefixes=["oidc-"])))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(ADMIN_CLAIMS))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth("oidc-opaque-1"))
            assert r.status_code == 200, r.text
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwt_shaped_gateway_key_lint(wired, gguf, monkeypatch, caplog):
    """A JWT-shaped gateway_key while SSO is enabled misroutes to SSO —
    the parse-time lint says so."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section(),
                      gateway_keys=("admin-secret", "jjj.kkk.lll")))
    with caplog.at_level(logging.WARNING):
        gw_sso.get_sso_state().settings(gw_config.get_gateway_config())
    assert any("looks OIDC-shaped" in rec.message for rec in caplog.records)


# ── introspection cache ────────────────────────────────────────────────────

def test_cache_hits_within_ttl(wired, gguf, monkeypatch):
    """Two requests with the same token → ONE introspection call (the
    second is served from the storage-backed cache)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    fake = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr(gw_sso, "introspect", fake)
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                r = client.get("/admin/stats", headers=auth("BBB.CCC.DDD"))
                assert r.status_code == 200
        assert fake.calls == 1
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_cache_ttl_zero_reintrospects(wired, gguf, monkeypatch):
    """cache_ttl_s: 0 → effectively no caching: every request
    introspects."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section(cache_ttl_s=0)))
    fake = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr(gw_sso, "introspect", fake)
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                assert client.get("/admin/stats",
                                  headers=auth("EEE.FFF.GGG")).status_code == 200
        assert fake.calls == 2
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_negative_result_not_cached(wired, gguf, monkeypatch):
    """Inactive tokens are never cached: the IdP sees every retry (an
    outage must not extend a denial — and a revived token must work)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    fake = FakeIdP(active=False)
    monkeypatch.setattr(gw_sso, "introspect", fake)
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                assert client.get("/admin/stats",
                                  headers=auth("HHH.III.JJJ")).status_code == 401
        assert fake.calls == 2
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


# ── regression: core behaviors with the tier-C pipeline ────────────────────

def test_open_mode_unchanged(wired, gguf):
    """Open mode (no keys, no sso): the admin surface stays open."""
    wired(_config(gguf))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats").status_code == 200
    finally:
        set_auth_pipeline(AuthPipeline())


def test_key_rbac_still_works_alongside_sso(wired, gguf, monkeypatch):
    """Key-based C-lite RBAC is untouched by tier C: an ops_readonly
    KEY is still enforced and audited under admin:<role>:<hash8>."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    cfgd = _sso_config(gguf, sso=_sso_section(),
                       gateway_keys=("admin-secret", "ops-secret"))
    cfgd["admin_key_roles"] = [{"key": "ops-secret",
                                "role": "ops_readonly"}]
    wired(cfgd)
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(ADMIN_CLAIMS))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/config", headers=auth("ops-secret"))
            assert r.status_code == 403
        p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
        denies = [json.loads(ln) for ln in
                  p.read_text(encoding="utf-8").strip().splitlines()
                  if "rbac-deny" in ln]
        assert denies[-1]["key"] == (
            "admin:ops_readonly:"
            + hashlib.sha256(b"ops-secret").hexdigest()[:8])
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


# ── unit: dispatch heuristic, parsing, role rules, identity ────────────────

def test_looks_like_oidc():
    assert gw_sso.looks_like_oidc("aaa.bbb.ccc")
    assert gw_sso.looks_like_oidc("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.sig")
    assert gw_sso.looks_like_oidc("oidc-opaque-1", ["oidc-"])
    assert not gw_sso.looks_like_oidc("oidc-opaque-1")       # no prefix given
    assert not gw_sso.looks_like_oidc("admin-secret")
    assert not gw_sso.looks_like_oidc("vk-abc123")
    assert not gw_sso.looks_like_oidc("a.b")                 # 2 segments
    assert not gw_sso.looks_like_oidc("a.b.c.d")             # 4 segments
    assert not gw_sso.looks_like_oidc("a.b.c!")              # bad char
    assert not gw_sso.looks_like_oidc("")
    assert not gw_sso.looks_like_oidc("..")                  # empty header


def test_parse_sso_units(monkeypatch, caplog):
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    st = gw_sso._parse_sso({"sso": {
        "enabled": True, "introspection_url": "https://x/y",
        "client_id": "cid", "client_secret_env": "TEST_SSO_SECRET",
        "role_rules": [
            {"claim": "groups", "value": "g", "role": "full_admin"},
            {"claim": "groups", "value": "h"},               # no role
            "nonsense",                                      # not an object
        ],
        "default_role": "ops_readonly",
        "timeout_s": "bogus",                                # → default 5.0
    }})
    assert st.enabled
    assert st.timeout_s == 5.0
    assert st.role_rules == [{"claim": "groups", "value": "g",
                              "role": "full_admin"}]
    # disabled: no section / not a dict
    assert not gw_sso._parse_sso({}).enabled
    assert not gw_sso._parse_sso({"sso": "junk"}).enabled
    # unknown default_role: kept (fails closed) + warning
    with caplog.at_level(logging.WARNING):
        st2 = gw_sso._parse_sso({"sso": {
            "enabled": True, "introspection_url": "https://x/y",
            "client_id": "cid", "client_secret_env": "TEST_SSO_SECRET",
            "default_role": "superuser"}})
    assert st2.default_role == "superuser"
    assert any("not a known role" in rec.message for rec in caplog.records)


def test_resolve_sso_role_rules():
    st = gw_sso.SSOSettings(
        default_role="ops_readonly",
        role_rules=[{"claim": "groups", "value": "g1", "role": "full_admin"},
                    {"claim": "tenant", "value": "acme", "role": "key_manager"}])
    # list-claim membership
    assert gw_sso.resolve_sso_role({"groups": ["g1", "other"]}, st) == "full_admin"
    # scalar equality
    assert gw_sso.resolve_sso_role({"tenant": "acme"}, st) == "key_manager"
    # first match wins
    assert gw_sso.resolve_sso_role({"groups": ["g1"], "tenant": "acme"}, st) == "full_admin"
    # no match → default
    assert gw_sso.resolve_sso_role({"groups": ["nope"]}, st) == "ops_readonly"
    # absent claim → default
    assert gw_sso.resolve_sso_role({}, st) == "ops_readonly"


def test_identity_contextvar_roundtrip():
    set_identity(None)
    assert get_identity() is None
    set_identity({"sub": "u", "email": "u@x", "tenant": "", "role": "full_admin",
                  "via": "sso"})
    ident = get_identity()
    assert ident["role"] == "full_admin" and ident["via"] == "sso"
    set_identity(None)
    assert get_identity() is None


# ── local JWT validation (JWKS) ────────────────────────────────────────────

def _gen_rsa():
    priv = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return priv, priv.public_key()


_PRIV1, _PUB1 = _gen_rsa()   # keyset A (kid k1)
_PRIV2, _PUB2 = _gen_rsa()   # keyset B (kid k2 — the rotated key)


def _jwk(pub, kid: str) -> dict:
    k = RSAAlgorithm.to_jwk(pub, as_dict=True)
    k["kid"] = kid
    k["alg"] = "RS256"
    return k


JWKS_A = {"keys": [_jwk(_PUB1, "k1")]}
JWKS_B = {"keys": [_jwk(_PUB2, "k2")]}


def _mint_jwt(priv, kid="k1", claims_over=None, alg="RS256", key=None):
    claims = {"sub": "jwt-user", "email": "jwt@corp.example",
              "groups": ["gatelodge-full-admins"],
              "exp": int(time.time()) + 3600,
              "iss": "https://idp.test"}
    claims.update(claims_over or {})
    return pyjwt.encode(claims, key if key is not None else priv,
                        algorithm=alg, headers={"kid": kid})


class FakeJWKS:
    """gateway.sso._jwks_fetch replacement: serves a configurable
    keyset, counts fetches, can fail."""

    def __init__(self, keyset=None, fail=False):
        self.keyset = keyset or JWKS_A
        self.fail = fail
        self.calls = 0

    async def __call__(self, url, timeout_s):
        self.calls += 1
        if self.fail:
            raise RuntimeError("jwks endpoint down")
        return self.keyset


@pytest.fixture()
def jwks(monkeypatch):
    """Clean JWKS cache + a controllable keyset transport."""
    gw_sso._JWKS_CACHE.clear()
    fake = FakeJWKS()
    monkeypatch.setattr(gw_sso, "_jwks_fetch", fake)
    yield fake
    gw_sso._JWKS_CACHE.clear()


def _jwks_config(gguf, **sso_over):
    monkeypatch_section = _sso_section(jwks_url="https://idp.test/jwks.json",
                                       **sso_over)
    return _sso_config(gguf, sso=monkeypatch_section)


def test_jwks_fast_path_never_calls_idp(wired, gguf, monkeypatch, jwks):
    """A valid JWT validates FULLY LOCALLY: one JWKS fetch, ZERO
    introspections — and the second request rides the token cache
    (still zero IdP calls, no new keyset fetch)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf))
    idp = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok = _mint_jwt(_PRIV1)
        with TestClient(gw_app.app) as client:
            r1 = client.get("/admin/stats", headers=auth(tok))
            assert r1.status_code == 200, r1.text
            r2 = client.post("/admin/reload", headers=auth(tok))
            assert r2.status_code == 200, r2.text
        assert idp.calls == 0        # no introspection round-trip
        assert jwks.calls == 1       # keyset fetched once, then cached
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_expired_falls_back_to_introspection(wired, gguf, monkeypatch,
                                                  jwks):
    """Local validation fails on expiry → introspection decides
    (inactive here) → 401. The fallback is the source of truth."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf))
    idp = FakeIdP(active=False)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok = _mint_jwt(_PRIV1, claims_over={"exp": int(time.time()) - 30})
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth(tok))
            assert r.status_code == 401
        assert idp.calls == 1        # fell back, IdP said inactive
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_wrong_audience_falls_back(wired, gguf, monkeypatch, jwks):
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf, audience="gatelodge-api"))
    idp = FakeIdP(active=False)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok = _mint_jwt(_PRIV1, claims_over={"aud": "other-api"})
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth(tok))
            assert r.status_code == 401
        assert idp.calls == 1
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_unknown_kid_refresh_throttled(wired, gguf, monkeypatch, jwks):
    """An unknown kid triggers ONE keyset refresh; a second unknown-kid
    token inside the throttle window does NOT hammer the IdP's JWKS
    endpoint (kid-spray must not become a DoS vector). Both requests
    fall back to introspection."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf))
    idp = FakeIdP(active=False)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok = _mint_jwt(_PRIV1, kid="attacker-kid")
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                r = client.get("/admin/stats", headers=auth(tok))
                assert r.status_code == 401
        assert jwks.calls == 1       # refreshed once, then throttled
        assert idp.calls == 2        # both fell back to introspection
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_kid_rotation_refreshes(wired, gguf, monkeypatch, jwks):
    """Key rotation: a token with a NEW kid (unknown to the cached
    keyset, past the throttle window) triggers a refresh and then
    validates locally — still zero introspections."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf))
    idp = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok1 = _mint_jwt(_PRIV1, kid="k1")
        with TestClient(gw_app.app) as client:
            assert client.get("/admin/stats",
                              headers=auth(tok1)).status_code == 200
        # the IdP rotates: the keyset now carries only k2
        jwks.keyset = JWKS_B
        # age the cached keyset past the unknown-kid refresh throttle
        for ent in gw_sso._JWKS_CACHE.values():
            ent["fetched_at"] -= gw_sso._JWKS_MIN_REFRESH_S + 1.0
        tok2 = _mint_jwt(_PRIV2, kid="k2")
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth(tok2))
            assert r.status_code == 200, r.text
        assert idp.calls == 0        # rotation handled locally
        assert jwks.calls == 2       # initial fetch + rotation refresh
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_fetch_failure_uses_introspection(wired, gguf, monkeypatch,
                                               jwks):
    """JWKS endpoint down but introspection up → auth still works
    (introspection is the resilient path, not a single point of
    failure)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf))
    jwks.fail = True
    idp = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok = _mint_jwt(_PRIV1)
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth(tok))
            assert r.status_code == 200, r.text
        assert idp.calls == 1
        assert jwks.calls == 1       # it tried, failed, fell back
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_hmac_alg_confusion_rejected(wired, gguf, monkeypatch, jwks):
    """A token referencing a REAL kid but signed with a shared-secret
    HMAC (the classic alg-confusion attack) is never locally
    validated — it falls to introspection, which rejects it."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_jwks_config(gguf))
    idp = FakeIdP(active=False)
    monkeypatch.setattr(gw_sso, "introspect", idp)
    _wire_enterprise()
    try:
        tok = _mint_jwt(None, kid="k1", alg="HS256",
                        key="attacker-shared-secret")
        with TestClient(gw_app.app) as client:
            r = client.get("/admin/stats", headers=auth(tok))
            assert r.status_code == 401
        assert idp.calls == 1        # rejected locally, IdP agreed
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_jwks_config_parsing(monkeypatch):
    """jwks_url/jwks_cache_ttl_s parse; explicit 0 ttl is honored
    (the `value or default` falsy-swallow lesson)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    base = {"enabled": True, "introspection_url": "https://x/y",
            "client_id": "cid", "client_secret_env": "TEST_SSO_SECRET"}
    st = gw_sso._parse_sso({"sso": {**base, "jwks_url": "https://x/jwks",
                                     "jwks_cache_ttl_s": 0}})
    assert st.jwks_url == "https://x/jwks"
    assert st.jwks_cache_ttl_s == 0.0
    st2 = gw_sso._parse_sso({"sso": dict(base)})
    assert st2.jwks_url == ""                    # absent = introspect only
    assert st2.jwks_cache_ttl_s == 300.0         # default


def test_whoami_sso_identity(wired, gguf, monkeypatch):
    """whoami surfaces the SSO principal: via sso, the full identity
    (sub/email/tenant), the role from the claims, and the sso:<email>
    audit identity."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    monkeypatch.setattr(gw_sso, "introspect", FakeIdP(ADMIN_CLAIMS))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            w = client.get("/admin/whoami",
                           headers=auth("zzz.yyy.xxx")).json()
        assert w["via"] == "sso" and w["role"] == "full_admin"
        assert w["who"] == "sso:alice@corp.example"
        assert w["identity"] == {"sub": "user-1",
                                 "email": "alice@corp.example",
                                 "tenant": ""}
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()
