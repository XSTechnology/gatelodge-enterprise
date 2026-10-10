"""Enterprise auth links — wired into the pipeline by
enterprise_main.py:

    AuthPipeline([resolve_key_link, rbac_link,
                  admin_gate_link, rate_limit_link])

Tier C-lite: rbac_link (below). Tier C: sso_link slots in between
resolve_key_link and rbac_link when it lands.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time

from fastapi import Request
from fastapi.responses import JSONResponse

from .rbac import DEFAULT_ROLE, allowed, get_admin_role_map

logger = logging.getLogger(__name__)


def _bearer_token(request: Request) -> str:
    """The Authorization bearer token ('' when absent) — same parse as
    the core's resolve_key_link."""
    auth = request.headers.get("authorization") or ""
    return auth[7:].strip() if auth[:7].lower() == "bearer " else ""


def _audit_deny(role: str, method: str, path: str, token: str) -> None:
    """Append the denial to the core config-audit trail (same file,
    same shape, so it surfaces in GET /admin/audit). Best effort —
    auditing must never break the 403."""
    try:
        import gateway.app as gw_app  # lazy: keeps the link import light
        who = (f"admin:{role}:{hashlib.sha256(token.encode('utf-8')).hexdigest()[:8]}"
               if token else f"admin:{role}")
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


def rbac_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Enforce the role→admin-surface mapping (gateway_enterprise.rbac)
    on /admin/* for ADMIN keys. Everything else passes through:

    - non-/admin paths (serving surface, console shell) — not ours;
    - virtual keys and open mode — admin_gate_link keeps its exact
      core semantics (403 / open);
    - _REQUEST_KEY_ID is never touched — spend attribution and the
      admin gate depend on it.

    Runs BEFORE admin_gate_link, so an allowed admin request still
    passes the core gate (key_id == "admin")."""
    path = request.url.path
    if not path.startswith("/admin"):
        return None
    if key_id != "admin":
        return None
    token = _bearer_token(request)
    role = get_admin_role_map().role_for(cfg, token) or DEFAULT_ROLE
    if allowed(role, request.method, path):
        logger.debug("rbac allow %s %s (role %s)", request.method, path, role)
        return None
    logger.warning("rbac deny %s %s (role %s)", request.method, path, role)
    _audit_deny(role, request.method, path, token)
    return JSONResponse(
        status_code=403,
        content={"error": {
            "message": f"role {role!r} may not {request.method} {path}",
            "type": "invalid_request_error", "param": None,
            "code": "rbac_forbidden"}})
