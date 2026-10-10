"""Composable auth middleware — the enterprise-fork extension point.

The core auth pipeline is: bearer resolution → rate limit → admin gate.
Each link is a standalone function that either passes (returns None) or
short-circuits (returns a JSONResponse). The middleware assembles them
in order. The enterprise fork inserts additional links (SSO/OIDC token
validation, RBAC permission checks, tenant resolution) between the
existing ones WITHOUT modifying the core.

Link contract:
    async def link(request, cfg, key_id) -> JSONResponse | None
        - Returns None to pass to the next link
        - Returns a JSONResponse to short-circuit (the caller wraps it
          with the request ID header)

The core pipeline:
    _resolve_key_link     — bearer token → key_id ('' when unknown)
    _rate_limit_link      — per-key RPM check (429 on over-limit)
    _admin_gate_link      — /admin/* requires an admin key (403 otherwise)

Enterprise insertion points:
    AFTER _resolve_key_link, BEFORE _rate_limit_link:
        SSO/OIDC validation, tenant resolution
    AFTER _rate_limit_link, BEFORE _admin_gate_link:
        RBAC permission checks (role-scoped admin keys)

Usage:
    from .auth import AuthPipeline, resolve_key_link, rate_limit_link, admin_gate_link

    # Core (default):
    pipeline = AuthPipeline([resolve_key_link, rate_limit_link, admin_gate_link])

    # Enterprise:
    from gateway_enterprise.auth import sso_link, rbac_link
    pipeline = AuthPipeline([
        resolve_key_link, sso_link, rbac_link,
        rate_limit_link, admin_gate_link,
    ])
"""
from __future__ import annotations

import asyncio
import collections
import contextvars
import hashlib
import hmac
import json
import logging
import time
import uuid
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

# ContextVars set by the pipeline, read by the rest of the gateway
_REQUEST_KEY_ID = contextvars.ContextVar("gw_request_key", default="")
_REQUEST_ID = contextvars.ContextVar("gw_request_id", default="")


def _sha256_hex(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── Core auth links ────────────────────────────────────────────────────────

def resolve_key_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 1: resolve the bearer token to a key_id.

    Any configured key (gateway_keys = admin, api_keys = virtual)
    enables bearer auth. gateway_keys accept plaintext OR sha256-hex.
    Virtual keys are hash-compared (constant-time).

    Sets _REQUEST_KEY_ID on success; returns 401 on failure.
    """
    auth = request.headers.get("authorization") or ""
    token = auth[7:].strip() if auth[:7].lower() == "bearer " else ""

    resolved = _resolve_bearer(cfg, token)
    if not resolved:
        _REQUEST_KEY_ID.set("")
        return JSONResponse(
            status_code=401,
            content={"error": {
                "message": "missing or invalid API key",
                "type": "invalid_request_error", "param": None,
                "code": "invalid_api_key"}})
    _REQUEST_KEY_ID.set(resolved)
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


def rate_limit_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 2: per-key RPM (requests/minute, sliding 60s window).
    Admin keys and open mode are unlimited. Returns 429 on over-limit."""
    if not key_id or key_id == "admin":
        return None
    if not request.url.path.startswith("/v1/"):
        return None
    entry = (cfg.api_keys or {}).get(key_id) or {}
    rpm = int(entry.get("rate_limit_rpm") or 0)
    if rpm <= 0:
        return None
    # NOTE: the rate window lives in app.py's _RATE_WINDOWS; this link
    # delegates to the module-level _key_rate_limit function there.
    # The indirection is deliberate: the storage backend is swappable,
    # and the link doesn't care where the window lives.
    from . import app as _app
    retry_after = _app._key_rate_limit(key_id, rpm)
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


def admin_gate_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Link 3: /admin/* requires an admin key (gateway_keys). Virtual
    keys are CONSUMER keys — 403 on the admin surface."""
    if key_id == "admin":
        return None
    if not request.url.path.startswith("/admin"):
        return None
    if not key_id:
        return None  # open mode (no keys configured) — /admin stays open
    return JSONResponse(
        status_code=403,
        content={"error": {
            "message": "admin endpoints require a gateway admin key",
            "type": "invalid_request_error", "param": None,
            "code": "admin_key_required"}})


# ── Pipeline assembly ──────────────────────────────────────────────────────

AuthLink = Callable[[Any, Request, str], JSONResponse | None]


class AuthPipeline:
    """Ordered chain of auth links. Each link receives (cfg, request,
    key_id) and either passes (None) or short-circuits (JSONResponse).
    The first non-None response wins; the request never reaches the
    endpoint.

    Enterprise constructs a different pipeline with additional links —
    the core pipeline is the DEFAULT, not a hardcoded path."""

    def __init__(self, links: list[AuthLink] | None = None):
        self.links = links or [resolve_key_link, rate_limit_link,
                               admin_gate_link]

    async def run(self, request: Request, cfg) -> JSONResponse | None:
        """Run all links in order. Returns the first short-circuit
        response, or None when all links pass."""
        key_id = _REQUEST_KEY_ID.get()  # may be pre-set by an earlier link
        for link in self.links:
            result = link(cfg, request, key_id)
            if result is not None:
                return result
            key_id = _REQUEST_KEY_ID.get()  # links may update it
        return None


# The core pipeline (module-level for import by the middleware)
_core_pipeline = AuthPipeline()


def get_auth_pipeline() -> AuthPipeline:
    """The active auth pipeline. Core: resolve → rate limit → admin gate.
    Enterprise: set_auth_pipeline(AuthPipeline([...extended links...]))."""
    return _core_pipeline


def set_auth_pipeline(pipeline: AuthPipeline) -> None:
    """Swap the auth pipeline (enterprise: add SSO/RBAC links)."""
    global _core_pipeline
    _core_pipeline = pipeline
    logger.info("auth pipeline swapped (%d links)", len(pipeline.links))
