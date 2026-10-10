"""SSO/OIDC — token introspection, tenant→role resolution, caching
(tier C).

Admin authentication against an external IdP via RFC 7662
introspection: POST the bearer to the IdP's introspection endpoint
with HTTP Basic client credentials → {active, sub, email, groups…}.
No local JWT/JWKS crypto (revocation checks are inherent); the
transport is one function (``introspect``) so tests and future
backends swap it wholesale.

Config lives in the operator config's ``sso`` section (raw-read from
the active store, like admin_key_roles — survives core mutations,
hot-reloads on the store's version token):

    "sso": {
      "enabled": true,
      "introspection_url": "https://idp.corp.example/oauth2/introspect",
      "client_id": "gatelodge-enterprise",
      "client_secret_env": "GATELODGE_SSO_CLIENT_SECRET",
      "audience": "",                  // optional aud claim check
      "timeout_s": 5.0,
      "cache_ttl_s": 60,
      "fail_open": false,
      "token_prefixes": [],            // e.g. ["oidc-"] forces SSO path
      "default_role": "ops_readonly",
      "role_rules": [
        {"claim": "groups", "value": "gatelodge-full-admins",
         "role": "full_admin"}
      ]
    }

Security posture:
- SSO users with no matching rule get ``default_role`` (ops_readonly)
  — an IdP-driven surface must not grant full admin by default.
- Unknown role names are KEPT (warned at parse) and deny everything
  via rbac.allowed() — fail closed and visible.
- Missing client secret with sso.enabled → SSO disabled with an
  ERROR log (OIDC tokens then 401 via the core path). Never a silent
  no-auth boot.
- The introspection cache is positive-only (an IdP outage must not
  extend a denial) and rides the storage backend (namespace
  "sso_introspect", EPOCH stamps) — shared across replicas for free
  in tier D.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from gateway.storage import get_storage

from . import rbac

logger = logging.getLogger(__name__)

_CACHE_NS = "sso_introspect"
_B64URL = re.compile(r"^[A-Za-z0-9_-]*$")


class SSOUnavailable(Exception):
    """Introspection transport failure (network/timeout/non-2xx/bad
    JSON). The caller decides fail-open vs fail-closed."""


@dataclass
class SSOSettings:
    enabled: bool = False
    issuer: str = ""
    introspection_url: str = ""
    client_id: str = ""
    client_secret_env: str = ""
    client_secret: str = ""      # resolved from the env at parse time
    audience: str = ""
    timeout_s: float = 5.0
    cache_ttl_s: float = 60.0
    fail_open: bool = False
    token_prefixes: list[str] = field(default_factory=list)
    default_role: str = "ops_readonly"
    role_rules: list[dict[str, str]] = field(default_factory=list)


def _mask(key: str) -> str:
    """Log-safe display (never full values)."""
    return (key[:6] + "…") if key else "<empty>"


def _parse_sso(raw: dict[str, Any]) -> SSOSettings:
    """sso section → SSOSettings (house-style normalization + lint).
    Structurally-invalid entries are dropped with warnings; unknown
    role names are KEPT (fail closed via rbac.allowed) with warnings."""
    s = raw.get("sso")
    s = s if isinstance(s, dict) else {}
    st = SSOSettings(
        enabled=bool(s.get("enabled")),
        issuer=str(s.get("issuer") or "").strip(),
        introspection_url=str(s.get("introspection_url") or "").strip(),
        client_id=str(s.get("client_id") or "").strip(),
        client_secret_env=str(s.get("client_secret_env") or "").strip(),
        audience=str(s.get("audience") or "").strip(),
        fail_open=bool(s.get("fail_open", False)),
        token_prefixes=[str(p).strip() for p in (s.get("token_prefixes") or [])
                        if str(p).strip()],
    )
    for name, attr, default, lo, hi in (
            ("timeout_s", "timeout_s", 5.0, 0.1, 60.0),
            ("cache_ttl_s", "cache_ttl_s", 60.0, 0.0, 3600.0)):
        raw_v = s.get(name)
        try:
            # explicit None check: 0 is a VALID cache_ttl_s (no caching)
            v = min(hi, max(lo, float(default if raw_v is None else raw_v)))
        except (TypeError, ValueError):
            v = default
        setattr(st, attr, v)
    default_role = str(s.get("default_role") or "").strip()
    if default_role:
        if default_role not in rbac.ROLES:
            logger.warning("sso: default_role %r is not a known role — "
                           "SSO users without a matching rule FAIL CLOSED",
                           default_role)
        st.default_role = default_role
    rules: list[dict[str, str]] = []
    for r in (s.get("role_rules") or []):
        if not isinstance(r, dict):
            logger.warning("sso: role_rules entry must be an object — skipped")
            continue
        claim = str(r.get("claim") or "").strip()
        value = str(r.get("value") or "").strip()
        role = str(r.get("role") or "").strip()
        if not (claim and value and role):
            logger.warning("sso: role_rules entry needs claim + value + "
                           "role — skipped")
            continue
        if role not in rbac.ROLES:
            logger.warning("sso: role_rules role %r is not a known role — "
                           "rule kept but FAILS CLOSED", role)
        rules.append({"claim": claim, "value": value, "role": role})
    st.role_rules = rules
    # secret: resolved from the env AT PARSE TIME (enterprise_main's boot
    # bridge keyring-loads it once, mirroring the core's api_key_env
    # convention — changing client_secret_env needs a restart)
    if st.client_secret_env:
        st.client_secret = os.environ.get(st.client_secret_env, "")
    if st.enabled:
        problems = []
        if not st.introspection_url:
            problems.append("introspection_url")
        if not st.client_id:
            problems.append("client_id")
        if not st.client_secret_env or not st.client_secret:
            problems.append(f"client secret (env {st.client_secret_env!r})")
        if problems:
            logger.error("sso: enabled but missing %s — SSO DISABLED "
                         "(OIDC tokens will 401 via the core path)",
                         ", ".join(problems))
            st.enabled = False
    return st


def _lint_sso(settings: SSOSettings, cfg) -> None:
    """Warn about configuration the dispatch would misroute: a
    gateway_key that looks OIDC-shaped is dispatched to SSO, not key
    auth. Virtual keys are always minted vk-… so they never collide."""
    if not settings.enabled:
        return
    for k in (getattr(cfg, "gateway_keys", None) or []):
        if looks_like_oidc(str(k), settings.token_prefixes):
            logger.warning("sso: gateway_key %s looks OIDC-shaped — it "
                           "would dispatch to SSO, not key auth; rename "
                           "the key or set token_prefixes", _mask(k))


class SSOState:
    """Version-cached sso-section parser (same pattern as rbac's
    AdminRoleMap): re-parses only when the active store's version
    token moves; an unreadable store keeps the last good settings."""

    def __init__(self) -> None:
        self._store: Any = None
        self._version: Any = object()  # sentinel ≠ any real token
        self._settings = SSOSettings()

    def settings(self, cfg) -> SSOSettings:
        store = getattr(cfg, "store", None)
        if store is not self._store:  # singleton swapped (tests)
            self._store = store
            self._version = object()
        if store is None:
            return self._settings
        version = store.version()
        if version == self._version:
            return self._settings
        raw = store.load()
        if isinstance(raw, dict):
            self._settings = _parse_sso(raw)
            _lint_sso(self._settings, cfg)
        # unreadable (None) or non-dict: keep last good settings
        self._version = version
        return self._settings


_STATE = SSOState()


def get_sso_state() -> SSOState:
    return _STATE


# ── dispatch heuristic ─────────────────────────────────────────────────────

def looks_like_oidc(token: str, prefixes: tuple | list = ()) -> bool:
    """True when the bearer should take the SSO path: an explicit
    token_prefixes match, or a compact-JWS shape (three dot-separated
    base64url segments — header.payload.signature; the signature may
    be empty for alg:none tokens, which introspection rejects anyway).
    """
    if not token:
        return False
    for p in prefixes:
        if p and token.startswith(p):
            return True
    parts = token.split(".")
    if len(parts) == 3 and parts[0] and parts[1]:
        return all(_B64URL.fullmatch(p) for p in parts)
    return False


# ── role resolution ────────────────────────────────────────────────────────

def resolve_sso_role(claims: dict[str, Any], settings: SSOSettings) -> str:
    """Introspection claims → role. role_rules are evaluated
    first-match against the claims (string equality, or membership
    when the claim is a list); no match → default_role. Unknown role
    names pass through (they fail closed in rbac.allowed)."""
    for rule in settings.role_rules:
        val = claims.get(rule["claim"])
        if val is None:
            continue
        values = val if isinstance(val, list) else [val]
        if any(str(v) == rule["value"] for v in values):
            return rule["role"]
    return settings.default_role


# ── introspection (the single transport point — tests patch this) ─────────

async def introspect(token: str, settings: SSOSettings) -> dict[str, Any]:
    """Raw RFC 7662 introspection: POST the token (form-encoded) to
    settings.introspection_url with HTTP Basic client credentials.

    Returns the claims dict when the token is active, {} when
    inactive. Raises SSOUnavailable on any transport failure — the
    caller decides fail-open vs fail-closed. Audience checking and
    caching live in authenticate() so a patched transport stays pure."""
    try:
        async with httpx.AsyncClient(timeout=settings.timeout_s) as hc:
            r = await hc.post(settings.introspection_url,
                              data={"token": token},
                              auth=(settings.client_id,
                                    settings.client_secret))
            r.raise_for_status()
            body = r.json()
    except (httpx.HTTPError, ValueError) as e:
        raise SSOUnavailable(str(e)) from e
    if not isinstance(body, dict) or not body.get("active"):
        return {}
    return body


def _audience_ok(claims: dict[str, Any], audience: str) -> bool:
    """Optional aud claim check: matches when the configured audience
    equals the aud value or is in the aud list. No audience configured
    → always OK."""
    if not audience:
        return True
    aud = claims.get("aud")
    auds = aud if isinstance(aud, list) else [aud]
    return audience in {str(a) for a in auds}


def _cache_get(token: str, ttl_s: float) -> dict[str, Any] | None:
    """Cached claims for token, or None. A hit requires TTL freshness
    AND (when the IdP sent an exp claim) that the token not have
    expired — the entry dies at min(cached_at + ttl, exp)."""
    ent = get_storage().get(_CACHE_NS, hashlib.sha256(
        token.encode("utf-8")).hexdigest())
    if not isinstance(ent, dict):
        return None
    now = time.time()
    claims = ent.get("claims")
    if not isinstance(claims, dict):
        return None
    if now - float(ent.get("cached_at") or 0.0) > ttl_s:
        return None
    exp = claims.get("exp")
    try:
        if exp is not None and now >= float(exp):
            return None
    except (TypeError, ValueError):
        pass
    return claims


def _cache_put(token: str, claims: dict[str, Any]) -> None:
    get_storage().set(_CACHE_NS, hashlib.sha256(
        token.encode("utf-8")).hexdigest(),
        {"claims": claims, "cached_at": time.time()})


async def authenticate(token: str, settings: SSOSettings) -> dict[str, Any]:
    """Validated claims for token: cache → introspect → cache.
    Returns {} when the token is invalid/inactive/audience-mismatched.
    Raises SSOUnavailable on transport failure. Positive-only caching
    (negative results are never cached — an IdP outage must not
    extend a denial)."""
    cached = _cache_get(token, settings.cache_ttl_s)
    if cached is not None:
        return cached
    claims = await introspect(token, settings)
    if claims and not _audience_ok(claims, settings.audience):
        claims = {}
    if claims:
        _cache_put(token, claims)
    return claims
