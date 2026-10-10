"""Enterprise auth links — wired into the pipeline by
enterprise_main.py:

    AuthPipeline([resolve_link, rbac_link,
                  admin_gate_link, rate_limit_link])

Tier C-lite: rbac_link — role→admin-surface enforcement.
Tier C:      resolve_link — replaces the core resolve_key_link and
             dispatches OIDC-shaped bearers to SSO validation (see
             gateway_enterprise/sso.py).

NOTE on ordering: an "sso_link after resolve_key_link" CANNOT work —
the core resolve 401s any token not in gateway_keys/api_keys before
an sso link would ever run. SSO must own resolution for OIDC-shaped
tokens, which is exactly what resolve_link does (dispatch, then
delegate everything else to the core resolver untouched).
"""
from __future__ import annotations

import hashlib
import json
import logging
import time

from fastapi import Request
from fastapi.responses import JSONResponse

from gateway.auth import _REQUEST_KEY_ID, resolve_key_link

from .identity import get_identity, set_identity
from .rbac import DEFAULT_ROLE, allowed, get_admin_role_map
from .sso import (SSOUnavailable, authenticate, get_sso_state,
                  looks_like_oidc, resolve_sso_role)

logger = logging.getLogger(__name__)


def _bearer_token(request: Request) -> str:
    """The Authorization bearer token ('' when absent) — same parse as
    the core's resolve_key_link."""
    auth = request.headers.get("authorization") or ""
    return auth[7:].strip() if auth[:7].lower() == "bearer " else ""


def _audit_deny(who: str, method: str, path: str) -> None:
    """Append the denial to the core config-audit trail (same file,
    same shape, so it surfaces in GET /admin/audit). Best effort —
    auditing must never break the 403. ``who`` is the caller-computed
    identity: "sso:<email>" for SSO principals, "admin:<role>:<hash8>"
    for key-based admins."""
    try:
        import gateway.app as gw_app  # lazy: keeps the link import light
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


# ── tier C: SSO-aware resolution ───────────────────────────────────────────

async def resolve_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 1 (enterprise): dispatch OIDC-shaped bearers to SSO
    validation; everything else to the core resolve_key_link untouched
    (gateway-key auth and open mode behave byte-for-byte as today).

    On successful SSO validation: sets the enterprise identity
    ContextVar and _REQUEST_KEY_ID = "admin" (the identity every core
    gate — admin gate, spend attribution, budget/model gates — already
    understands).

    INVARIANT (see the tier-C plan): never return None from the SSO
    path with an empty _REQUEST_KEY_ID while keys are configured —
    admin_gate_link's empty-key fast-path would leave /admin open. On
    introspection failure with fail_open, DELEGATE to the core
    resolver instead of passing through."""
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
            # Behave as if SSO did not exist for THIS request: the core
            # resolver decides (401 when keys are configured; true open
            # mode passes). It never leaves /admin reachable with an
            # unvalidated token.
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


# ── tier C-lite: RBAC on the admin surface ─────────────────────────────────

def rbac_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Enforce the role→admin-surface mapping (gateway_enterprise.rbac)
    on /admin/* for ADMIN keys. Everything else passes through:

    - non-/admin paths (serving surface, console shell) — not ours;
    - virtual keys and open mode — admin_gate_link keeps its exact
      core semantics (403 / open);
    - _REQUEST_KEY_ID is never touched — spend attribution and the
      admin gate depend on it.

    Role resolution is IDENTITY-FIRST (tier C): an SSO principal's
    role comes from its identity (no fallback — an SSO user is never
    re-derived from admin_key_roles); key-based admins use the C-lite
    AdminRoleMap → DEFAULT_ROLE. Denial audit identity follows: SSO
    principals are recorded as sso:<email or sub>.

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
