"""Composable auth middleware — the enterprise-fork extension point.

The app's middleware delegates EVERY request to the ACTIVE pipeline
(get_auth_pipeline()). The core default pipeline is:

    resolve_link      — bearer → key_id: OIDC-shaped bearers dispatch to
                        SSO validation (gateway/sso.py — inert without
                        an ``sso`` config section); everything else is
                        the classic key resolution below, byte-for-byte
    admin_gate_link   — /admin/* requires an admin key (403 otherwise)
    rate_limit_link   — per-key RPM sliding window (429 on over-limit)

The governed links are INERT WITHOUT CONFIG: no ``sso`` section →
resolve_link delegates to resolve_key_link untouched; no
``admin_key_roles`` section → every admin key is full_admin and
rbac_link passes everything. Governance turns on by writing config,
not by changing code (the deliberate no-enterprise-tax posture:
role-scoped admin keys and SSO/OIDC are core, not a paid gate).

Each link is a standalone function that either passes (returns None) or
short-circuits (returns a JSONResponse). The middleware wraps short-
circuits with the request-id header and never calls the endpoint.

Link contract (sync OR async — async links are awaited):
    def link(cfg, request, key_id) -> JSONResponse | None | Awaitable[...]
Links may update the request identity via _REQUEST_KEY_ID (resolve
does); the pipeline re-reads it between links.

NOTE on ordering: an "sso link after resolve_key_link" cannot work —
the classic resolver 401s any token not in gateway_keys/api_keys
before a later link runs. SSO must own resolution for OIDC-shaped
tokens, which is exactly what resolve_link does (dispatch, then
delegate everything else untouched).

Custom pipelines — swap the whole chain, zero core changes:

    from gateway.auth import (AuthPipeline, set_auth_pipeline, ...)
    set_auth_pipeline(AuthPipeline([...your links...]))

State: per-key rate windows ride the swappable StorageBackend
(namespace "rate_windows") — a shared backend (RedisStorage) gives
multi-replica rate limiting for free. Timestamps are EPOCH seconds so
stamps from different processes are comparable.

Identity: the request ContextVars live HERE (single definition).
gateway/app.py imports _REQUEST_KEY_ID/_REQUEST_ID from this module —
do not redefine them elsewhere: contextvars with the same NAME are
still distinct objects, and two definitions silently stop sharing
state.
"""
from __future__ import annotations

import asyncio
import contextvars
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

from .identity import get_identity, set_identity
from .rbac import DEFAULT_ROLE, allowed, get_admin_role_map
from .sso import (SSOUnavailable, authenticate, get_sso_state,
                  looks_like_oidc, resolve_sso_role)
from .storage import get_storage

logger = logging.getLogger(__name__)

# Request identity ContextVars — set by the auth pipeline (key) and the
# middleware (request id); read by logging, budgets, metrics, cost.
# THE definition — app.py imports these (see module docstring).
_REQUEST_KEY_ID = contextvars.ContextVar("gw_request_key", default="")
_REQUEST_ID = contextvars.ContextVar("gw_request_id", default="")


def _sha256_hex(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── core auth links ────────────────────────────────────────────────────────

def resolve_key_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 1: resolve the bearer token to a key_id.

    Any configured key (gateway_keys = admin, api_keys = virtual)
    enables bearer auth; gateway_keys accept plaintext OR sha256-hex.
    Virtual keys are hash-compared (constant-time). When NO keys are
    configured (open mode) everything passes with key_id=''.

    Sets _REQUEST_KEY_ID; returns 401 when keys are configured and the
    token is missing/unknown."""
    if not (cfg.gateway_keys or cfg.api_keys):
        _REQUEST_KEY_ID.set("")  # open mode — no keys configured
        return None
    auth = request.headers.get("authorization") or ""
    token = auth[7:].strip() if auth[:7].lower() == "bearer " else ""
    resolved = _resolve_bearer(cfg, token)
    _REQUEST_KEY_ID.set(resolved)
    if not resolved:
        return JSONResponse(
            status_code=401,
            content={"error": {
                "message": "missing or invalid API key",
                "type": "invalid_request_error", "param": None,
                "code": "invalid_api_key"}})
    return None


def _resolve_bearer(cfg, token: str) -> str:
    """Bearer token → key_id: 'admin' for gateway_keys (plaintext OR
    sha256-hex form), the api_keys id for a virtual key (hash compared,
    constant-time). '' when unknown."""
    if not token:
        return ""
    token_hash = _sha256_hex(token)
    for k in (cfg.gateway_keys or []):
        k = str(k)
        if k == token:
            return "admin"
        if len(k) == 64 and hmac.compare_digest(k, token_hash):
            return "admin"
    for entry in (cfg.api_keys or {}).values():
        if hmac.compare_digest(str(entry.get("key_hash") or ""),
                                token_hash):
            return str(entry.get("id") or "")
    return ""


def admin_gate_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 2: /admin/* requires an admin key (gateway_keys). Virtual
    keys are CONSUMER keys — 403 on the admin surface. Open mode (no
    keys configured) keeps /admin open."""
    if key_id == "admin":
        return None
    if not request.url.path.startswith("/admin"):
        return None
    if not key_id:
        return None  # open mode — /admin stays open
    return JSONResponse(
        status_code=403,
        content={"error": {
            "message": "admin endpoints require a gateway admin key",
            "type": "invalid_request_error", "param": None,
            "code": "admin_key_required"}})


def _key_rate_limit(key_id: str, rpm: int) -> float:
    """Record a request for key_id against its rpm limit. Returns 0.0
    when allowed, else the seconds until the oldest admitted request
    leaves the 60s window (Retry-After). Denied requests are NOT
    recorded (the window counts admissions). Window state rides the
    storage backend (namespace "rate_windows"); EPOCH timestamps so a
    shared backend sees comparable stamps across replicas."""
    now = time.time()
    st = get_storage()
    win = [t for t in (st.get("rate_windows", key_id) or [])
           if now - t <= 60.0]
    if len(win) >= rpm:
        return max(0.0, 60.0 - (now - win[0]))
    win.append(now)
    st.set("rate_windows", key_id, win)
    return 0.0


def rate_limit_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 3: per-key RPM (requests/minute, sliding 60s window) on the
    serving surface. Admin keys and open mode are unlimited. Returns 429
    on over-limit."""
    if not key_id or key_id == "admin":
        return None
    if not request.url.path.startswith("/v1/"):
        return None
    entry = (cfg.api_keys or {}).get(key_id) or {}
    rpm = int(entry.get("rate_limit_rpm") or 0)
    if rpm <= 0:
        return None
    retry_after = _key_rate_limit(key_id, rpm)
    if retry_after > 0:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": (f"key {key_id!r}: rate limit "
                            f"({rpm} requests/minute)"),
                "type": "rate_limit_error", "param": None,
                "code": "rate_limited"}},
            headers={"Retry-After": str(int(retry_after) + 1)})
    return None


# ── governance links (inert without config — see module docstring) ─────────

def _bearer_token(request: Request) -> str:
    """The Authorization bearer token ('' when absent) — same parse as
    resolve_key_link."""
    auth = request.headers.get("authorization") or ""
    return auth[7:].strip() if auth[:7].lower() == "bearer " else ""


def _audit_deny(who: str, method: str, path: str) -> None:
    """Append the denial to the core config-audit trail (same file,
    same shape, so it surfaces in GET /admin/audit). Best effort —
    auditing must never break the 403. ``who`` is the caller-computed
    identity: "sso:<email>" for SSO principals, "admin:<role>:<hash8>"
    for key-based admins."""
    try:
        import gateway.app as gw_app  # lazy: app imports THIS module
        rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
               "key": who,
               "action": f"rbac-deny {method} {path}"[:200]}
        p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with gw_app._log_lock:
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")
    except Exception:  # noqa: BLE001
        logger.debug("rbac denial audit failed", exc_info=True)


async def resolve_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 1 (default): dispatch OIDC-shaped bearers to SSO validation
    (gateway/sso.py); everything else to the classic resolve_key_link
    untouched (gateway-key auth and open mode behave byte-for-byte).

    On successful SSO validation: sets the identity ContextVar
    (gateway/identity.py) and _REQUEST_KEY_ID = "admin" — the identity
    every core gate (admin gate, spend attribution, budget/model gates)
    already understands.

    INVARIANT: never return None from the SSO path with an empty
    _REQUEST_KEY_ID while keys are configured — admin_gate_link's
    empty-key fast-path would leave /admin open. On introspection
    failure with fail_open, DELEGATE to the classic resolver instead
    of passing through."""
    settings = get_sso_state().settings(cfg)
    if settings.enabled:
        token = _bearer_token(request)
        if looks_like_oidc(token, settings.token_prefixes):
            return await _sso_resolve(cfg, request, token, settings)
    return resolve_key_link(cfg, request, key_id)


async def _sso_resolve(cfg, request: Request, token: str,
                       settings) -> JSONResponse | None:
    """The SSO branch of resolve_link: validate the token, populate the
    identity, and map the principal onto the core's admin identity."""
    try:
        claims = await authenticate(token, settings)
    except SSOUnavailable as e:
        logger.warning("sso: introspection unavailable (%s)", e)
        if settings.fail_open:
            # Behave as if SSO did not exist for THIS request: the
            # classic resolver decides (401 when keys are configured;
            # true open mode passes). It never leaves /admin reachable
            # with an unvalidated token.
            return resolve_key_link(cfg, request, "")
        return JSONResponse(
            status_code=503,
            content={"error": {
                "message": "SSO identity provider unavailable",
                "type": "server_error", "param": None,
                "code": "sso_unavailable"}},
            headers={"Retry-After": "5"})
    if not claims:
        return JSONResponse(
            status_code=401,
            content={"error": {
                "message": "missing or invalid API key",
                "type": "invalid_request_error", "param": None,
                "code": "invalid_api_key"}})
    role = resolve_sso_role(claims, settings)
    set_identity({
        "sub": str(claims.get("sub") or ""),
        "email": str(claims.get("email")
                     or claims.get("preferred_username") or ""),
        "tenant": str(claims.get("tenant") or ""),
        "role": role,
        "via": "sso",
    })
    _REQUEST_KEY_ID.set("admin")  # the identity every core gate knows
    logger.info("sso: authenticated %s (role %s)",
                claims.get("email") or claims.get("sub") or "?", role)
    return None


def rbac_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 2 (default): enforce the role→admin-surface mapping
    (gateway/rbac.py, from the config's admin_key_roles section) on
    /admin/* for ADMIN keys. Everything else passes through:

    - non-/admin paths (serving surface, console shell) — not ours;
    - virtual keys and open mode — admin_gate_link keeps its exact
      classic semantics (403 / open);
    - _REQUEST_KEY_ID is never touched — spend attribution and the
      admin gate depend on it.

    Role resolution is IDENTITY-FIRST: an SSO principal's role comes
    from its identity (no fallback — an SSO user is never re-derived
    from admin_key_roles); key-based admins use the AdminRoleMap →
    DEFAULT_ROLE (full_admin — the pre-governance behavior when no
    admin_key_roles section exists). Denial audit identity follows:
    SSO principals are recorded as sso:<email or sub>.

    Runs BEFORE admin_gate_link, so an allowed admin request still
    passes the core gate (key_id == "admin")."""
    path = request.url.path
    if not path.startswith("/admin"):
        return None
    if key_id != "admin":
        return None
    identity = get_identity()
    if identity is not None:
        role = str(identity.get("role") or "")
        who = ("sso:" + str(identity.get("email")
                            or identity.get("sub") or "unknown"))
    else:
        token = _bearer_token(request)
        role = get_admin_role_map().role_for(cfg, token) or DEFAULT_ROLE
        who = (f"admin:{role}:"
               f"{hashlib.sha256(token.encode('utf-8')).hexdigest()[:8]}"
               if token else f"admin:{role}")
    if allowed(role, request.method, path):
        logger.debug("rbac allow %s %s (role %s)", request.method, path, role)
        return None
    logger.warning("rbac deny %s %s (role %s)", request.method, path, role)
    _audit_deny(who, request.method, path)
    return JSONResponse(
        status_code=403,
        content={"error": {
            "message": f"role {role!r} may not {request.method} {path}",
            "type": "invalid_request_error", "param": None,
            "code": "rbac_forbidden"}})


# ── pipeline assembly ──────────────────────────────────────────────────────

AuthLink = Callable[[Any, Request, str], Any]


class AuthPipeline:
    """Ordered chain of auth links. Each link receives (cfg, request,
    key_id) and either passes (None) or short-circuits (a JSONResponse,
    or an awaitable of one for async links). The first non-None response
    wins; the request never reaches the endpoint.

    Enterprise constructs a different pipeline with additional links —
    the core pipeline is the DEFAULT, not a hardcoded path."""

    def __init__(self, links: list[AuthLink] | None = None):
        # Default = the GOVERNED pipeline: SSO-aware resolve + RBAC
        # before the classic admin gate + rate limit. Both governance
        # links are inert without config (see module docstring), so
        # this is byte-for-byte the classic behavior when no sso /
        # admin_key_roles sections exist. (/admin and /v1 are disjoint
        # prefixes, so core-link order is behavior-neutral — but keep
        # it stable: rbac_link must see the resolved key_id.)
        self.links = links or [resolve_link, rbac_link,
                               admin_gate_link, rate_limit_link]

    async def run(self, request: Request, cfg) -> JSONResponse | None:
        """Run all links in order. Returns the first short-circuit
        response, or None when all links pass."""
        key_id = _REQUEST_KEY_ID.get()  # may be pre-set by an earlier link
        for link in self.links:
            result = link(cfg, request, key_id)
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                result = await result
            if result is not None:
                return result
            key_id = _REQUEST_KEY_ID.get()  # links may update it
        return None


# The core pipeline (module-level singleton the middleware runs)
_core_pipeline = AuthPipeline()


def get_auth_pipeline() -> AuthPipeline:
    """The active auth pipeline. Default: SSO-aware resolve → RBAC →
    admin gate → per-key rate limit. Custom: set_auth_pipeline(
    AuthPipeline([...]))."""
    return _core_pipeline


def set_auth_pipeline(pipeline: AuthPipeline) -> None:
    """Swap the auth pipeline (enterprise: add SSO/RBAC links). Takes
    effect on the NEXT request — the middleware runs whatever is
    active."""
    global _core_pipeline
    _core_pipeline = pipeline
    logger.info("auth pipeline swapped (%d links)", len(pipeline.links))
