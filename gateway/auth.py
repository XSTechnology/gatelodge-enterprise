"""Composable auth middleware — the enterprise-fork extension point.

The app's middleware delegates EVERY request to the ACTIVE pipeline
(get_auth_pipeline()). The core default pipeline is:

    resolve_key_link   — bearer → key_id; 401 when keys are configured
                         and the token doesn't match; open mode passes
    admin_gate_link    — /admin/* requires an admin key (403 otherwise)
    rate_limit_link    — per-key RPM sliding window (429 on over-limit)

Each link is a standalone function that either passes (returns None) or
short-circuits (returns a JSONResponse). The middleware wraps short-
circuits with the request-id header and never calls the endpoint.

Link contract (sync OR async — async links are awaited):
    def link(cfg, request, key_id) -> JSONResponse | None | Awaitable[...]
Links may update the request identity via _REQUEST_KEY_ID (resolve
does); the pipeline re-reads it between links.

Enterprise usage — insert SSO/OIDC and RBAC links, zero core changes:

    from gateway.auth import (AuthPipeline, resolve_key_link,
                              admin_gate_link, rate_limit_link,
                              set_auth_pipeline)
    from gateway_enterprise.auth import sso_link, rbac_link

    set_auth_pipeline(AuthPipeline([
        resolve_key_link, sso_link, rbac_link,
        admin_gate_link, rate_limit_link,
    ]))

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
import logging
import time
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

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
        # Default order matches the historical middleware exactly:
        # authenticate → admin gate → rate limit. (/admin and /v1 are
        # disjoint prefixes, so core-link order is behavior-neutral —
        # but keep it stable: enterprise links may care, e.g. an RBAC
        # link must see the resolved key_id.)
        self.links = links or [resolve_key_link, admin_gate_link,
                               rate_limit_link]

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
    """The active auth pipeline. Core default: resolve → admin gate →
    rate limit. Enterprise: set_auth_pipeline(AuthPipeline([...]))."""
    return _core_pipeline


def set_auth_pipeline(pipeline: AuthPipeline) -> None:
    """Swap the auth pipeline (enterprise: add SSO/RBAC links). Takes
    effect on the NEXT request — the middleware runs whatever is
    active."""
    global _core_pipeline
    _core_pipeline = pipeline
    logger.info("auth pipeline swapped (%d links)", len(pipeline.links))
