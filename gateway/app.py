"""Gatelodge v1.2.1 — OpenAI-compatible multi-backend LLM front door.

A single-process gateway that routes LLM requests across local GGUFs,
llama-server children, RunPod pods/serverless, and OpenAI-compatible
cloud providers (including the HF Inference Providers router), with
virtual keys, per-key governance, dollar cost tracking, guardrails,
smart routing, batch processing, and an eval harness.

Implemented API surface:
  GET  /                     — single-page console (Models / Services /
                              Cloud / Performance / Syslog / Parameters /
                              API Keys / Health / Aliases / Governance)
  GET  /v1/models            — aliases + smart routes + backend models +
                              auto-discovered GGUF stems from models_dir
  POST /v1/chat/completions  — non-streaming AND streaming (SSE). All
                              backend types. Alias chains with hedge
                              racing; smart routes (cheapest/fastest/
                              round_robin/weighted); guardrails (request
                              rules before the response cache, response
                              rules on non-stream); per-key model
                              allowlists + RPM rate limits + monthly
                              token/USD budgets; response cache for
                              simple repeated prompts; cost_usd in the
                              response when the model is priced.
  POST /v1/completions       — legacy text completions: raw prompt in,
                              text out. All backend types; primary chain
                              entry only.
  POST /v1/embeddings        — OpenAI embeddings. Embedded GGUFs under
                              backends.<local>.embedding_models; cloud
                              passthrough.
  POST /v1/images/generations — OpenAI images passthrough for openai-type
                              cloud backends (HF router is chat-only).
  POST /v1/batches           — async batch: {model, requests:[{custom_id,
                              messages}]} executes at concurrency 4
                              through the full pipeline. GET /v1/batches
                              (list), GET /v1/batches/{id} (status),
                              GET /v1/batches/{id}/results (JSONL).
  GET  /metrics              — Prometheus exposition: request counters
                              by backend/model/key/status, latency, tokens,
                              cost, guardrail blocks, cache gauges, uptime.
  GET  /healthz              — backends, loaded instances, GPUs.

Admin surface (requires a gateway admin key when any keys are configured):
  GET/POST /admin/aliases    — alias chains with hedge racing
  GET/PUT /admin/config      — raw config editor (validated JSON, hot-reload)
  GET/POST /admin/backends   — service definitions / backend CRUD
  GET/POST /admin/models     — model inventory / model CRUD
  GET/POST /admin/keys       — virtual key inventory + usage / create /
                              revoke (keys hashed at rest, shown once)
  GET  /admin/secrets        — API key status per backend (env/keyring)
  GET  /admin/stats          — per-model + per-key aggregates (rollups +
                              live month); budgets
  GET  /admin/events         — recent gateway log events (in-memory ring)
  GET  /admin/verify (GET)   — scheduled-verification state
  POST /admin/verify         — active round-trip self-test per backend
  GET  /admin/guardrails     — guardrail config + live violation counts
  GET  /admin/routes         — smart-route config + live latency stats
  GET  /admin/audit          — config mutation audit trail (newest first)
  GET  /admin/evals          — saved eval runs
  GET  /admin/evals/{name}   — latest eval report
  POST /admin/evals/run      — golden-prompt eval across models (drift
                              detection on re-run)
  POST /admin/load           — preload an embedded instance
  POST /admin/unload         — drop instances

Auth: gateway_keys = ADMIN keys; api_keys = per-consumer virtual keys
(hashed at rest). Any configured key enables bearer auth; virtual keys
are limited to /v1/* + /metrics. Empty both = open (localhost trust).
Every request is attributed to its key in the log, metrics, and budgets.
Request IDs: inbound x-request-id honored, echoed on every response.

Logging: month-scoped gateway-log-YYYY-MM.jsonl (legacy migrates once);
closed months roll up to gateway-rollup-*.json. The gateway NEVER logs
prompt/response content — metadata only. log_retention_months deletes
old files. Config mutations land in config-audit.jsonl + optional
webhook sink.

Error policy: the gateway NEVER substitutes a local model when an
external provider fails. Upstream 429 → our 429 + Retry-After; timeout
→ 504; others → 502 with code=<class>. Network-class failures (connect)
retry once on the same model. Streams carry error events with the class.

Every request appends one JSONL line to data/gateway_log.jsonl:
{ts, model, backend, stream, ms, usage, status}.

Deferred: PyInstaller exe (v2 remaining).

Error bodies are OpenAI-shaped: {"error": {"message", "type", "param",
"code"}}.
"""
from __future__ import annotations

import asyncio
import base64
import collections
import contextvars
import hashlib
import itertools
import json
import logging
import random
import re
import secrets
import struct
import threading
import time
import uuid

import httpx
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from openai import AsyncOpenAI

try:
    import huggingface_hub
except ImportError:
    huggingface_hub = None

from . import cloud
from . import catalog
from . import capabilities
from . import lifecycle
from . import licensing
from . import llamaserver
from . import semantic_cache
from . import tracing
from . import traffic
from . import wire
from . import modelsearch
from . import providers
from . import runpod
from . import upload
# Auth seam: the middleware runs the ACTIVE pipeline (default below);
# enterprise swaps it via set_auth_pipeline(). _REQUEST_KEY_ID /
# _REQUEST_ID are defined in auth.py — imported here so the whole app
# shares ONE ContextVar object per name.
from . import auth
from .auth import (AuthPipeline, get_auth_pipeline, set_auth_pipeline,
                   _REQUEST_KEY_ID, _REQUEST_ID, _REQUEST_SESSION_ID,
                   _REQUEST_TEAM, _resolve_bearer, _sha256_hex,
                   apply_url_token_forms, session_id_from_header)
from .config import (PROJECT_ROOT, GatewayConfigError,
                     ProviderNotLicensedError, default_config_raw,
                     get_gateway_config, set_backend_filter,
                     EMBEDDED_BACKEND, OPENAI_BACKEND, LLAMA_SERVER_BACKEND,
                     RUNPOD_BACKEND, RUNPOD_PUBLIC_BACKEND)
from .models import (EmbeddedEngine, EmbeddedLLMError, detect_gpus,
                     get_engine, installed_ggufs)
# Storage seam: module state globals below are NamespaceDict/NamespaceSet
# facades over the swappable backend — set_storage(RedisStorage(...))
# moves them to shared state with zero further core changes.
from .storage import NamespaceDict, NamespaceSet, get_storage, reset_storage

logger = logging.getLogger(__name__)

app = FastAPI(title="Gatelodge", version="2.5.1")

# ── extra-router seam — the fourth enterprise extension point ──────────────
# Mount an additional APIRouter (enterprise: SCIM, later the MCP
# gateway). Core provides ONLY the insertion point — it mounts nothing
# itself. The mounted routes join the same middleware stack (request
# ids, logging) but OWN their authentication: the core auth pipeline
# does not gate the registered prefix, because enterprise-surface
# bearers (an IdP's SCIM token) are not gateway credentials.
_EXTRA_ROUTER_MARK: int | None = None
_EXTRA_ROUTER_PREFIXES: list[str] = []


def set_extra_router(router, *, prefix: str) -> None:
    """Mount an enterprise APIRouter at ``prefix`` (leading/trailing
    slashes normalized). Requests under the prefix skip the core auth
    pipeline — the router's endpoints are responsible for their own
    authentication scheme (RFC 7644 §2 for SCIM, etc.).

    Replace semantics: a second call swaps the previous extra router
    out; ``set_extra_router(None, prefix=...)`` removes it (tests use
    this for cleanup). Call at startup, before serving."""
    global _EXTRA_ROUTER_MARK
    prefix = "/" + prefix.strip("/")
    if _EXTRA_ROUTER_MARK is not None:
        app.router.routes = app.router.routes[:_EXTRA_ROUTER_MARK]
        _EXTRA_ROUTER_PREFIXES.clear()
        _EXTRA_ROUTER_MARK = None
    if router is None:
        return
    _EXTRA_ROUTER_MARK = len(app.router.routes)
    _EXTRA_ROUTER_PREFIXES.append(prefix)
    app.include_router(router, prefix=prefix)


def add_extra_router(router, *, prefix: str, auth: str = "") -> None:
    """Mount an ADDITIONAL enterprise surface alongside whatever is
    already mounted (v2.0: SCIM at /scim and the MCP gateway at /mcp
    coexist). Idempotent per prefix (a repeated mount is a no-op).
    set_extra_router() remains the reset-and-mount primitive — calling
    it (or its None cleanup) removes EVERYTHING mounted, added routers
    included, because the truncation mark predates them all.

    v2.5.1 (GL-R16): ``auth`` must be DECLARED —
      auth="owned"  the router owns its own authentication (an IdP's
                    SCIM bearer, MCP session auth) — the core pipeline
                    does not gate the prefix
      auth="admin"  the CORE enforces the /admin gate chain (resolve →
                    RBAC → admin gate) on every route under the prefix
                    — the router needs no guard of its own, so a guard
                    can never be forgotten or drift from the core's
    An undeclared auth raises at wiring time — a startup failure, not
    a silent exposure."""
    global _EXTRA_ROUTER_MARK
    prefix = "/" + prefix.strip("/")
    if auth not in ("owned", "admin"):
        raise RuntimeError(
            f"add_extra_router({prefix!r}): auth must be declared — "
            "'owned' (router authenticates itself) or 'admin' (core "
            "enforces the admin gate). Refusing to mount undeclared.")
    if prefix in _EXTRA_ROUTER_PREFIXES:
        return                      # already mounted — boot-time no-op
    if _EXTRA_ROUTER_MARK is None:
        # First surface arrived via the additive path: record the mark
        # so set_extra_router(None) still truncates everything.
        _EXTRA_ROUTER_MARK = len(app.router.routes)
    _EXTRA_ROUTER_PREFIXES.append(prefix)
    if auth == "admin":
        app.include_router(router, prefix=prefix,
                           dependencies=[Depends(_extension_admin_guard)])
    else:
        app.include_router(router, prefix=prefix)


async def _extension_admin_guard(request: Request) -> None:
    """Core-enforced admin gate for auth='admin' extension routers
    (GL-R16): the SAME link chain the core /admin surface runs —
    resolve → RBAC → admin gate — so behavior (open mode, role-scoped
    keys, virtual-key 403s) is byte-identical and can never drift from
    a router-owned copy."""
    from .auth import (admin_gate_link, resolve_key_link, rbac_link)
    from .config import get_gateway_config
    cfg = get_gateway_config()

    async def _run(link, key_id):
        out = link(cfg, request, key_id)
        if hasattr(out, "__await__"):
            out = await out
        return out

    r = await _run(resolve_key_link, "")
    if r is not None:
        raise _PipelineBlocked(r)
    key_id = _REQUEST_KEY_ID.get()
    r = await _run(rbac_link, key_id)
    if r is not None:
        raise _PipelineBlocked(r)
    r = await _run(admin_gate_link, key_id)
    if r is not None:
        raise _PipelineBlocked(r)


class _PipelineBlocked(Exception):
    """A pipeline link produced an error response — re-raised so the
    extension router returns exactly the link's status + body."""

    def __init__(self, response):
        self.response = response
        super().__init__("pipeline link blocked request")


@app.exception_handler(_PipelineBlocked)
async def _pipeline_blocked_handler(request: Request,
                                     exc: _PipelineBlocked):
    return exc.response


# ── control-plane split (v2.1): port-scoped ASGI shims ─────────────────────
# When admin_port is configured, __main__ serves TWO listeners around the
# SAME app: the serving port (data plane — /v1, /healthz, /metrics, the
# enterprise surfaces) and the admin port (/admin/*, the console, /healthz)
# on admin_bind (localhost by default). Isolation is the NETWORK, not auth
# logic — the Kong/Envoy posture. Blocked paths 404 ("not hosted on this
# port") rather than 403 (no existence leak).

_ADMIN_PREFIXES = ("/admin", "/teams", "/scim")   # control surfaces stay
# OFF the public serving port when the split is active; /mcp is DATA
# plane (agent clients) and stays on the serving port.
_ADMIN_PATHS = ("/admin", "/teams", "/scim", "/")


async def _asgi_404(scope, receive, send) -> None:
    body = b'{"error":{"message":"not found on this port","type":'
    body += b'"invalid_request_error","param":null,"code":null}}'
    headers = [(b"content-type", b"application/json"),
               (b"content-length", str(len(body)).encode())]
    await send({"type": "http.response.start", "status": 404,
                "headers": headers})
    await send({"type": "http.response.body", "body": body})


def _path_blocked(path: str, prefixes: tuple[str, ...]) -> bool:
    for p in prefixes:
        if path == p or path.startswith(p + "/") or (p == "/" and path == "/"):
            return True
    return False


def serving_scope_app():
    """The app as served on the DATA port under the split: /admin/*,
    /teams, /scim and the console shell are not hosted here."""
    async def _shim(scope, receive, send):
        if scope["type"] == "http" and _path_blocked(
                scope.get("path") or "", _ADMIN_PATHS):
            await _asgi_404(scope, receive, send)
            return
        await app(scope, receive, send)
    return _shim


def admin_scope_app():
    """The app as served on the ADMIN port: ONLY the control plane —
    /admin/*, /teams, /scim, the console shell and /healthz — the data
    plane (/v1, /mcp) is not hosted here."""
    from .auth import path_scope

    async def _shim(scope, receive, send):
        if scope["type"] == "http":
            path = scope.get("path") or ""
            allowed = (path in ("/healthz", "/")
                       or path_scope("/admin", path)
                       or path_scope("/teams", path)
                       or path_scope("/scim", path))
            if not allowed:
                await _asgi_404(scope, receive, send)
                return
        await app(scope, receive, send)
    return _shim


_LOG_PATH = PROJECT_ROOT / "data" / "gateway_log.jsonl"
_log_lock = threading.Lock()

# Concurrent chat completions in flight (queue admission control).
# Keyed registry (slot id → admission time) instead of a bare counter:
# a stream whose client disconnects mid-flight can leave its response
# generator suspended at a yield without ever running its finally —
# with a plain int that slot leaked forever and the gateway wedged
# into a permanent 429 wall (observed live). Every admission check
# first evicts slots held longer than the stale threshold, so an
# abandoned stream can block new requests for at most that long.
_chat_slots: dict[int, float] = {}
_chat_slot_seq = itertools.count(1)


def _acquire_chat_slot() -> int:
    sid = next(_chat_slot_seq)
    _chat_slots[sid] = time.monotonic()
    return sid


def _release_chat_slot(sid: int) -> None:
    _chat_slots.pop(sid, None)  # idempotent — double release is a no-op


def _slot_stale_s(cfg: Any) -> float:
    """How long a slot may legitimately be held. Real requests finish
    within the queue timeout (streaming ones within a couple of them);
    anything held longer is a zombie from an abandoned/cancelled
    stream or a backend that hung."""
    try:
        q = float((cfg.queue or {}).get("timeout_s") or 120)
    except Exception:  # noqa: BLE001 — config oddities must not wedge admission
        q = 120.0
    return max(600.0, q * 2.0)


def _reap_stale_chat_slots(cfg: Any) -> int:
    """Evict zombie slots; returns how many were dropped."""
    now = time.monotonic()
    stale_s = _slot_stale_s(cfg)
    stale = [sid for sid, t0 in _chat_slots.items() if now - t0 > stale_s]
    for sid in stale:
        _chat_slots.pop(sid, None)
    if stale:
        logger.warning("admission: reaped %d stale chat slot(s) held >%.0fs "
                       "(abandoned streams or hung backends)", len(stale), stale_s)
    return len(stale)


# ── In-memory event ring (console Logs pane) ───────────────────────────────

class _RingHandler(logging.Handler):
    """Keeps the last ~300 gateway log events for the console."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.events: collections.deque[dict[str, str]] = collections.deque(maxlen=300)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.events.append({
                "ts": time.strftime("%H:%M:%S"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage()[:300],
            })
        except Exception:  # noqa: BLE001 — never let logging break a request
            pass

    def push(self, event: dict) -> None:
        """Push a manual event (non-logging) into the ring."""
        self.events.append(event)


_EVENTS: _RingHandler | None = None


def _event_ring() -> _RingHandler:
    global _EVENTS
    if _EVENTS is None:
        _EVENTS = _RingHandler()
        logging.getLogger().addHandler(_EVENTS)
        # our INFO events must survive even when nothing called basicConfig
        logging.getLogger("gateway").setLevel(logging.INFO)
    return _EVENTS


_event_ring()  # attach at import — events before the first /admin/events call


# ── benign client-disconnect noise ─────────────────────────────────────────

class _BenignDisconnectFilter(logging.Filter):
    """Drop post-stream client-disconnect log noise.

    Clients that close immediately after [DONE] (Workshop's SDK) or
    abort mid-stream leave the server writing into a dead socket. On
    Windows that surfaces as an uvicorn 'Exception in ASGI application'
    traceback ending in ConnectionResetError [WinError 10054] plus
    asyncio-proactor 'socket.send() raised exception.' lines — AFTER a
    request that already completed successfully on our side. The filter
    is attached ONLY to the uvicorn.error / asyncio loggers: gateway.* /
    httpx records keep their full tracebacks (a child-side reset still
    logs normally through our own handlers)."""

    _BENIGN_NAMES = ("ClientDisconnected", "BrokenResourceError",
                     "EndOfStream")

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if "socket.send() raised exception" in record.getMessage():
                return False
            exc = record.exc_info[1] if record.exc_info else None
            depth = 0
            while exc is not None and depth < 8:  # walk cause/context chain
                if isinstance(exc, (ConnectionResetError,
                                    ConnectionAbortedError)):
                    return False
                if type(exc).__name__ in self._BENIGN_NAMES:
                    return False
                exc = exc.__cause__ or exc.__context__
                depth += 1
        except Exception:  # noqa: BLE001 — never break logging
            pass
        return True


for _lg_name in ("uvicorn.error", "asyncio"):
    logging.getLogger(_lg_name).addFilter(_BenignDisconnectFilter())


@app.on_event("shutdown")
async def _kill_llama_servers() -> None:
    """Stop every managed llama-server child when the gateway exits."""
    llamaserver.kill_all()


@app.on_event("shutdown")
async def _flush_tracing() -> None:
    """Best-effort final OTLP flush — in-flight spans ride the last
    batch instead of dying with the process."""
    tracing.shutdown_flush()


@app.on_event("startup")
async def _configure_tracing() -> None:
    """Install the tracing state from the otel section (boot-time — a
    section change needs a restart, like scim)."""
    cfg = get_gateway_config()
    tracing.configure(cfg.otel, version=app.version,
                      edition=licensing.status().get("edition") or "")


@app.on_event("startup")
async def _install_provider_gate() -> None:
    """v2.2 provider entitlement: install licensing.provider_allowed
    as the config backend filter (cloud backends serve only under
    their per-backend license claim) and surface the unlicensed set
    in the log at boot. Idempotent; inert when unlicensed backends
    don't exist."""
    set_backend_filter(licensing.provider_allowed)
    cfg = get_gateway_config()
    _log_unlicensed_backends(cfg)
    # v2.2 GPU cap advisory: a licensed gpus cap below the detected
    # count warns loudly (visibility) — the instances cap is the hard
    # enforcement; this catches over-provisioned boxes at boot.
    gpus_cap = licensing.caps().get("gpus")
    if gpus_cap is not None:
        try:
            n_gpus = len(detect_gpus(ttl_s=30.0))
            if n_gpus > gpus_cap:
                msg = (f"license caps this deployment at {gpus_cap} GPU(s) "
                       f"— {n_gpus} detected; the instance cap "
                       "({}) is enforced at load time".format(
                           licensing.caps().get("instances", "n/a")))
                logger.warning("License: %s", msg)
                _event_ring().push({
                    "ts": datetime.now().isoformat(),
                    "event": "license_gpu_overcap",
                    "cap": gpus_cap, "detected": n_gpus,
                    "message": msg})
        except Exception:                                  # noqa: BLE001
            pass


def _unlicensed_backends(cfg) -> list[str]:
    """Cloud backends the current license does NOT entitle (sorted)."""
    try:
        out = [name for name, b in (cfg.backends or {}).items()
               if not licensing.provider_allowed(
                   name, b.get("type") or "")]
        return sorted(out)
    except Exception:                                      # noqa: BLE001
        return []


_UNLICENSED_SEEN: list[str] | None = None


def _log_unlicensed_backends(cfg) -> None:
    """Warn ONCE per set-change (boot + hot-reload) — not per request."""
    global _UNLICENSED_SEEN
    current = _unlicensed_backends(cfg)
    if current != _UNLICENSED_SEEN:
        _UNLICENSED_SEEN = current
        if current:
            logger.warning(
                "license: %d cloud backend(s) NOT entitled and dropped "
                "from serving — add their names to the license providers "
                "claim: %s (local backends embedded/llama-server are "
                "always allowed)", len(current), ", ".join(current))
    return


@app.on_event("startup")
async def _start_lifecycle_watchdog() -> None:
    """v3: the pod lifecycle watchdog runs from BOOT, not lazily from
    request paths — no traffic must never mean no protection (a
    forgotten pod bills forever)."""
    lifecycle.ensure_watchdog()


async def _budget_gate(cfg, backend_name: str, model_key: str) -> None:
    """v3 D4: refuse to START a runpod pod when a monthly budget cap
    is hit — a RUNNING pod keeps serving (its cost is committed).
    Raises lifecycle.LifecycleBudgetExceeded; chokepoints map it to
    503 budget_exceeded."""
    backend = cfg.backends.get(backend_name) or {}
    if backend.get("type") != "runpod":
        return
    if ((backend.get("models") or {}).get(model_key) or {}).get("endpoint_id"):
        return  # serverless endpoint — not a lifecycle pod
    mgr = runpod.get_manager(cfg, backend_name, model_key)
    if mgr.is_ready():
        return
    ok, why = await lifecycle.budget_allows(cfg, backend_name, model_key)
    if not ok:
        raise lifecycle.LifecycleBudgetExceeded(why)


# ── OpenAI-shaped errors ───────────────────────────────────────────────────

def _error(status: int, message: str, err_type: str = "invalid_request_error",
           param: str | None = None, code: str | None = None,
           headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": err_type,
                           "param": param, "code": code}},
        headers=headers,
    )


def _exec_error_response(e: BaseException, requested: str,
                         stream: bool, t0: float) -> JSONResponse:
    """Classify-aware failure propagation — NO engine switching.

    Policy: when an external provider fails, the requestor gets an
    OpenAI-shaped error describing WHAT failed; the gateway never
    substitutes a local model (the box may not have the VRAM for it, and
    the caller asked for a specific model). Upstream rate limits map to
    429 + Retry-After (OpenAI clients back off natively); timeouts map
    to 504; every other class is 502 with code=<class>."""
    kind = cloud.classify_error(e)
    ms = int((time.perf_counter() - t0) * 1000)
    logger.warning("gateway execution failed (%s, %s): %s", requested, kind, e)
    status, err_type, code = 502, "server_error", kind or "execution_error"
    headers: dict[str, str] | None = None
    if kind == "rate_limit":
        status, err_type, code = 429, "rate_limit_error", "rate_limited"
        headers = {"Retry-After": "1"}
    elif kind == "timeout":
        status = 504
    _log_request(requested, "", stream, ms, {}, status, err=kind)
    return _error(status, str(e), err_type=err_type, code=code,
                  headers=headers)


# ── Bearer auth (gateway_keys = ADMIN keys; api_keys = per-consumer
# virtual keys with usage attribution). Empty both = open localhost trust. ──

# Per-request key identity (_REQUEST_KEY_ID) and request IDs
# (_REQUEST_ID) are ContextVars imported from .auth — the single
# definition. The auth middleware itself now delegates to the ACTIVE
# AuthPipeline (auth.py); bearer resolution (_resolve_bearer) and key
# hashing (_sha256_hex) also live there and are imported above.


# ── request IDs + Prometheus metrics (tier A) ──────────────────────────────
# Every request gets an id (inbound x-request-id honored, else minted):
# echoed on the response and stamped into every log line. Metrics are
# in-memory counters incremented from _log_request (the single chokepoint
# every request passes through, whatever its status) and rendered in
# Prometheus text format at GET /metrics. Label cardinality is capped —
# unknown model names from bad requests collapse into an "other" bucket.

# _REQUEST_ID is imported from .auth — the single definition.
_START_TIME = time.time()

# Prometheus counters stay PER-PROCESS by design (each replica is
# scraped separately; aggregation belongs to the scraper). They do NOT
# ride the storage backend.
_METRIC_LABELS: dict[str, tuple[str, ...]] = {
    "gateway_requests_total": ("backend", "model", "key", "status"),
    "gateway_request_ms_sum": ("backend", "model"),
    "gateway_request_ms_count": ("backend", "model"),
    "gateway_tokens_total": ("backend", "model"),
    "gateway_cost_usd_total": ("backend",),
    "gateway_requests_by_key_total": ("key",),
    "gateway_guardrail_blocks_total": ("rule",),
    "gateway_semantic_cache_hits_total": ("model",),
    "gateway_mcp_tool_calls_total": ("server", "tool", "status"),
}
_METRICS: dict[str, dict[tuple, float]] = {n: {} for n in _METRIC_LABELS}
_METRIC_CARDINALITY_CAP = 1000


def reset_metrics() -> None:
    """Drop in-memory metric counters (tests)."""
    for d in _METRICS.values():
        d.clear()


def _metric_inc(name: str, labels: tuple, value: float = 1.0) -> None:
    try:
        d = _METRICS[name]
        if labels not in d and len(d) >= _METRIC_CARDINALITY_CAP:
            labels = ("other",) * len(_METRIC_LABELS[name])
        d[labels] = d.get(labels, 0.0) + float(value)
    except Exception:  # noqa: BLE001 — metrics must never break a request
        pass


def _metrics_text() -> str:
    """Prometheus text exposition (format version 0.0.4)."""
    helps = {
        "gateway_requests_total": "Requests processed, by backend/model/key/status.",
        "gateway_request_ms_sum": "Cumulative latency (ms) by backend/model.",
        "gateway_request_ms_count": "Latency observations by backend/model.",
        "gateway_tokens_total": "Total tokens by backend/model.",
        "gateway_cost_usd_total": "Estimated provider cost (USD) by backend.",
        "gateway_requests_by_key_total": "Requests by consuming key.",
        "gateway_guardrail_blocks_total": "Guardrail rule violations.",
        "gateway_semantic_cache_hits_total": "Semantic-cache (paraphrase) hits by model.",
        "gateway_mcp_tool_calls_total": "MCP tool calls by server/tool/status.",
    }
    types = {"gateway_requests_total": "counter",
             "gateway_request_ms_sum": "counter",
             "gateway_request_ms_count": "counter",
             "gateway_tokens_total": "counter",
             "gateway_cost_usd_total": "counter",
             "gateway_requests_by_key_total": "counter",
             "gateway_guardrail_blocks_total": "counter",
             "gateway_semantic_cache_hits_total": "counter",
             "gateway_mcp_tool_calls_total": "counter"}
    out: list[str] = []
    for name, label_names in _METRIC_LABELS.items():
        out.append(f"# HELP {name} {helps[name]}")
        out.append(f"# TYPE {name} {types[name]}")
        for labels, value in sorted(_METRICS[name].items()):
            tag = ",".join(f'{ln}="{lv}"'
                           for ln, lv in zip(label_names, labels))
            out.append(f"{name}{{{tag}}} {value:g}")
    # gauges
    cs = cache_stats()
    out.append("# HELP gateway_cache_entries Cached responses.")
    out.append("# TYPE gateway_cache_entries gauge")
    out.append(f"gateway_cache_entries {cs['entries']}")
    out.append("# HELP gateway_cache_hits_total Response-cache hits.")
    out.append("# TYPE gateway_cache_hits_total counter")
    out.append(f"gateway_cache_hits_total {cs['hits']}")
    out.append("# HELP gateway_semantic_cache_entries Semantically "
               "cached responses (paraphrase layer).")
    out.append("# TYPE gateway_semantic_cache_entries gauge")
    out.append(f"gateway_semantic_cache_entries {cs['semantic']['entries']}")
    out.append("# HELP gateway_verify_failing Backends currently failing "
               "scheduled verification.")
    out.append("# TYPE gateway_verify_failing gauge")
    out.append(f"gateway_verify_failing "
               f"{sum(1 for s in _VERIFY_STATE.values() if s.get('alerting'))}")
    out.append("# HELP gateway_uptime_seconds Process uptime.")
    out.append("# TYPE gateway_uptime_seconds gauge")
    out.append(f"gateway_uptime_seconds {time.time() - _START_TIME:g}")
    out.append("# HELP gateway_build_info Build version.")
    out.append("# TYPE gateway_build_info gauge")
    out.append(f'gateway_build_info{{version="{app.version}"}} 1')
    return "\n".join(out) + "\n"


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    """Prometheus exposition. Reachable by ANY valid key when auth is on
    (consumers included) — point your scraper at it with a bearer."""
    return Response(_metrics_text(),
                    media_type="text/plain; version=0.0.4; charset=utf-8")


# ── per-key rate limiting (requests/minute, sliding window) ────────────────
# Admin keys and open mode are unlimited. The window state rides the
# swappable storage backend (namespace "rate_windows"); the limiter
# itself lives in auth.py (rate_limit_link). A restart / backend swap
# clears the window (one minute of slack, acceptable for an RPM limiter).

_RATE_WINDOWS = NamespaceDict("rate_windows")
_RATE_TPM_WINDOWS = NamespaceDict("rate_tpm_windows")


def reset_rate_windows() -> None:
    """Drop rate windows (tests)."""
    _RATE_WINDOWS.clear()
    _RATE_TPM_WINDOWS.clear()


def _rid_response(resp: Response, rid: str) -> Response:
    resp.headers["x-request-id"] = rid
    return resp


_EXPIRY_LEVEL_SEEN: int | None = None
_REVOKED_SEEN: bool = False


def _license_watch() -> None:
    """Per-request license watch (cheap): hot-reload a renewed license
    file + the revocation list in place, then surface expiry/grace/
    revocation on level CHANGE — event ring + log once per crossing,
    not per request."""
    global _EXPIRY_LEVEL_SEEN, _REVOKED_SEEN
    if licensing.maybe_reload_license():
        # entitlements may have changed with the renewal — re-surface
        _log_unlicensed_backends(get_gateway_config())
    if licensing.maybe_reload_crl():
        st = licensing.status()
        if st.get("mode") == "revoked" and not _REVOKED_SEEN:
            _REVOKED_SEEN = True
            msg = ("license REVOKED by the vendor's revocation list "
                   f"(serial {st.get('serial')}) — serving disabled")
            logger.error("License: %s", msg)
            try:
                _event_ring().push({
                    "ts": datetime.now().isoformat(),
                    "event": "license_revoked",
                    "serial": st.get("serial"),
                    "message": msg})
            except Exception:                              # noqa: BLE001
                pass
        if st.get("mode") != "revoked":
            _REVOKED_SEEN = False
    warn = licensing.expiry_warning()
    level = (warn or {}).get("level")
    if level != _EXPIRY_LEVEL_SEEN:
        _EXPIRY_LEVEL_SEEN = level
        if warn is not None:
            if warn.get("grace"):
                msg = (f"license EXPIRED — grace window: "
                       f"{warn['hours_left']} hour(s) of serving left; "
                       "renew and drop the new data/license.json in "
                       "place (hot-reloads, no restart)")
            else:
                msg = (f"license expires in {warn['days_left']} day(s) "
                       f"(level {level}) — renew and drop the new "
                       "data/license.json in place (hot-reloads, no "
                       "restart needed)")
            logger.warning("License: %s", msg)
            try:
                _event_ring().push({"ts": datetime.now().isoformat(),
                                    "event": "license_expiry_warning",
                                    "level": level,
                                    "days_left": warn.get("days_left"),
                                    "hours_left": warn.get("hours_left"),
                                    "message": msg})
            except Exception:                              # noqa: BLE001
                pass


@app.middleware("http")
async def _bearer_auth(request: Request, call_next):
    cfg = get_gateway_config()
    if cfg.maybe_reload():
        # hot-reload: re-surface the unlicensed-backend set (the
        # resolve_chain filter reads the licensing state live — this
        # only refreshes the boot-time warning)
        _log_unlicensed_backends(cfg)
    _license_watch()
    # URL-token auth (v2.x): /<token>/v1/... → canonical /v1/... with
    # the token riding _REQUEST_URL_TOKEN into the auth pipeline
    # (query form ?key= is read inside resolve_key_link). Runs BEFORE
    # the first request.url read so the license gate, span, routing
    # and cache keys all see the canonical path a bearer client sends.
    apply_url_token_forms(request, cfg)
    # Request id: honor inbound x-request-id (≤64 chars), else mint one.
    # Echoed on EVERY response and stamped into every log line.
    rid = ((request.headers.get("x-request-id") or "").strip())[:64] \
        or uuid.uuid4().hex[:16]
    _REQUEST_ID.set(rid)
    # Licensing (v1.5 closed distribution): an expired trial/license
    # stops serving — every route except /healthz answers 403
    # license_expired so the operator can still see WHY. Unmanaged
    # (source runs / tests — pre-bootstrap) passes through; the
    # enterprise binary never serves unlicensed (its entry point exits
    # before uvicorn starts). Evaluated at most once per minute.
    lic_ok, lic_mode = licensing.request_allowed()
    if not lic_ok and request.url.path != "/healthz":
        return _rid_response(_error(
            403,
            f"Gatelodge license/trial has expired ({lic_mode}) — serving "
            "is disabled. Install a valid data/license.json (or set "
            "GATELODGE_LICENSE) and restart.",
            "license_expired"), rid)
    # Session budgets (v1.4): capture the validated x-session-id when
    # the feature is enabled — the charge hook and request log read it.
    if (cfg.session_budgets or {}).get("enabled"):
        _REQUEST_SESSION_ID.set(session_id_from_header(request))
    # Tracing (v2.0): W3C-correlated SERVER span around everything past
    # the license gate. A valid inbound traceparent continues the
    # caller's trace; the active trace id rides the request log and the
    # current span echoes back on the response (client correlation).
    # None (disabled/unsampled) = the entire path costs one dict lookup.
    span = tracing.request_span(request.method, request.url.path,
                                request.headers.get("traceparent"))
    resp: Response
    if request.method == "GET" and request.url.path == "/":
        _REQUEST_KEY_ID.set("")  # static console shell — its API
        # calls still require the key (the page prompts for it)
        resp = await call_next(request)
    # extra-router seam: mounted enterprise surfaces (SCIM, …) own
    # their authentication — the core pipeline does not gate their
    # registered prefix (their bearers are not gateway credentials).
    # Segment-aware (GL-R18): '/scim' never swallows '/scim-thing'.
    elif any(auth.path_scope(_p, request.url.path)
             for _p in _EXTRA_ROUTER_PREFIXES):
        resp = await call_next(request)
    else:
        # Auth runs through the ACTIVE pipeline (default: bearer resolve →
        # admin gate → per-key RPM, all in auth.py). Enterprise swaps the
        # pipeline via set_auth_pipeline() — SSO/OIDC + RBAC links — with
        # zero core changes. The pipeline sets _REQUEST_KEY_ID on success.
        blocked = await get_auth_pipeline().run(request, cfg)
        resp = blocked if blocked is not None else await call_next(request)
    if span is not None:
        # Correlation attrs (the span object is shared with the
        # downstream context): request id joins trace ↔ log; key/session
        # ids join trace ↔ spend scopes. The key id was set by the auth
        # pipeline — it runs in THIS task, before call_next forks it.
        span.set("gatelodge.request_id", rid)
        kid = _REQUEST_KEY_ID.get()
        if kid:
            span.set("gatelodge.key_id", kid)
        sid = _REQUEST_SESSION_ID.get()
        if sid:
            span.set("gatelodge.session_id", sid)
        tracing.finish_request_span(span, resp.status_code)
        resp.headers["traceparent"] = tracing.format_traceparent(
            span.trace_id, span.span_id)
    return _rid_response(resp, rid)


@app.exception_handler(RequestValidationError)
async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    # FastAPI's default 422 body isn't OpenAI-shaped — make it so.
    msg = "; ".join(f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
                    for e in exc.errors()) or "invalid request body"
    return _error(400, msg)


# ── JSONL request log ──────────────────────────────────────────────────────

def _log_request(model: str, backend: str, stream: bool, ms: int,
                 usage: dict[str, Any], status: int, err: str = "",
                 cost_usd: float | None = None) -> None:
    """One line per request — best effort, never fails the request.
    err (optional) carries the classified failure kind for status >= 400
    (timeout/network/rate_limit/server/auth/client — cloud.classify_error).
    cost_usd (optional) is the estimated provider cost. The consuming
    key's id rides in via the request ContextVar (absent for open mode)."""
    rec: dict[str, Any] = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                           "model": model, "backend": backend, "stream": stream,
                           "ms": ms, "usage": usage, "status": status}
    if err:
        rec["err"] = err
    kid = _REQUEST_KEY_ID.get()
    if kid:
        rec["key"] = kid
    sid = _REQUEST_SESSION_ID.get()
    if sid:
        rec["session"] = sid
    team = _REQUEST_TEAM.get()
    if team:
        rec["team"] = team
    if cost_usd is not None:
        rec["cost_usd"] = round(cost_usd, 6)
    rid = _REQUEST_ID.get()
    if rid:
        rec["req"] = rid
    # Trace correlation (v2.0): the active OTLP trace id links this
    # log line to the exported span — log ↔ trace join in one id.
    trace_id = tracing.current_trace_id()
    if trace_id:
        rec["trace"] = trace_id
    # INTERNAL child span per model call (same chokepoint as the log —
    # every completion, whatever its status; no-op when unsampled).
    tracing.add_llm_span(model, backend, ms, usage or {}, status,
                         cost_usd=cost_usd)
    line = json.dumps(rec)
    # Traffic capture seam (v2.4 Content Vault): the terminal flush for
    # the request that observed itself at entry. No collector (no
    # backend installed, or a non-chat endpoint) = pure no-op. Called
    # from EVERY terminal path — errors and early exits included: the
    # vault's forensic value is the request side of a failure too.
    traffic.flush({"model": model, "backend": backend, "stream": stream,
                   "ms": ms, "usage": usage or {}, "status": status,
                   "err": err, "cost_usd": cost_usd, "key": kid,
                   "session": sid, "team": team, "req": rid,
                   "trace": trace_id})
    # Prometheus counters (best-effort; single chokepoint for every
    # request whatever its status — errors included)
    _metric_inc("gateway_requests_total",
                (backend or "none", model or "none",
                 kid or "anonymous", str(status)))
    if backend:
        _metric_inc("gateway_request_ms_sum", (backend, model), ms)
        _metric_inc("gateway_request_ms_count", (backend, model))
        _metric_inc("gateway_tokens_total",
                    (backend, model),
                    int((usage or {}).get("total_tokens") or 0))
        if cost_usd is not None:
            _metric_inc("gateway_cost_usd_total", (backend,), cost_usd)
        if kid:
            _metric_inc("gateway_requests_by_key_total", (kid,))
    try:
        with _log_lock:
            global _LOG_MONTH_STATE, _REQ_CHAIN_HEAD
            if _LOG_MONTH_STATE != _month_key():
                _LOG_MONTH_STATE = _month_key()
                _rollup_closed_months()
            path = _ensure_month_log()
            # Request-ledger chaining (v2.5): under the lock so the
            # head advances atomically with the append. Plain path
            # (no 'evidence' claim) writes exactly today's line.
            if _req_chain_on():
                if _REQ_CHAIN_HEAD is None:
                    _REQ_CHAIN_HEAD = _req_chain_tail_hash()
                rec["prev"] = _REQ_CHAIN_HEAD
                rec["hash"] = _req_record_hash(rec)
                _REQ_CHAIN_HEAD = rec["hash"]
                line = json.dumps(rec)
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except OSError:
        logger.warning("gateway_log write failed", exc_info=True)


# ── monthly spend ledger + rotating request log ────────────────────────────# The request log IS the ledger. Files are month-scoped
# (gateway-log-YYYY-MM.jsonl next to the legacy path); the legacy single
# file migrates once (current-month lines imported, older lines archived
# to gateway-log-archive.jsonl, legacy renamed *.migrated). Closed months
# roll up into gateway-rollup-YYYY-MM.json so /admin/stats keeps full
# history without scanning archives. Spend scopes: "backend:<name>"
# (openai-type token budgets) and "key:<id>" (per-consumer token/USD
# budgets). Cache hits log backend="cache" and never bill upstream.


def _capture_content(resp: dict) -> str:
    """First-choice assistant text of an OpenAI-shaped response — the
    content the traffic-capture seam observes for non-streaming
    responses and cache hits. Defensive by design: any shape quirk is
    an empty string, never an exception."""
    try:
        return str((((resp.get("choices") or [{}])[0])
                    .get("message") or {}).get("content") or "")
    except Exception:  # noqa: BLE001 — observation must never throw
        return ""


# Spend state rides the storage backend (shared when the enterprise
# swaps in RedisStorage; dict/set semantics preserved via the facades).
_SPEND_LEDGER = NamespaceDict("spend_ledger")    # scope → {month, tokens, usd}
_SPEND_ALERTED = NamespaceSet("spend_alerted")   # "scope:month" 80% alerts
_LOG_MIGRATED: bool = False
_LOG_MONTH_STATE: str | None = None


def _month_key() -> str:
    return time.strftime("%Y-%m")


def _month_log_path(month: str) -> Path:
    return _LOG_PATH.parent / f"gateway-log-{month}.jsonl"


def _ensure_month_log(month: str | None = None) -> Path:
    """Current month's log path, lazily migrating the legacy single-file
    log (current-month lines imported; older lines archived; legacy
    renamed .migrated — idempotent, no data ever lost)."""
    global _LOG_MIGRATED
    month = month or _month_key()
    path = _month_log_path(month)
    if _LOG_MIGRATED:
        return path
    _LOG_MIGRATED = True
    try:
        if _LOG_PATH.is_file():
            keep: list[str] = []
            archive: list[str] = []
            for ln in _LOG_PATH.read_text(encoding="utf-8",
                                          errors="replace").splitlines():
                try:
                    e = json.loads(ln)
                except json.JSONDecodeError:
                    archive.append(ln)
                    continue
                (keep if str(e.get("ts") or "").startswith(month + "-")
                 else archive).append(ln)
            if keep:
                with open(path, "a", encoding="utf-8") as f:
                    f.write("\n".join(keep) + "\n")
            if archive:
                ap = _LOG_PATH.parent / "gateway-log-archive.jsonl"
                with open(ap, "a", encoding="utf-8") as f:
                    f.write("\n".join(archive) + "\n")
            _LOG_PATH.rename(str(_LOG_PATH) + ".migrated")
            logger.info("gateway log migrated to month files "
                        "(%d current-month lines, %d archived)",
                        len(keep), len(archive))
    except OSError:
        logger.warning("gateway log migration failed", exc_info=True)
    return path


# ── request-ledger chaining (v2.5 Evidence Center, 'evidence' claim) ──────
# Same pattern as config-audit (§hash-chained audit below): prev + hash,
# sha256 over canonical sorted-keys JSON. GATED LIVE by the license —
# requests under an evidence-carrying Enterprise license chain; anything
# else (community trial/key, unmanaged source) writes today's plain
# line. The gate is read per request, so license swaps mid-month yield
# mixed records in one file; verify_request_chain() skips plain records
# TRANSPARENTLY (prev preserved — the writer keeps its head across
# plain lines) and still detects downgrades: a chained record edited
# into a plain one orphans the next chained record's prev → break.

_REQ_CHAIN_HEAD: str | None = None
_REQ_GENESIS = "genesis"


def _req_record_hash(rec: dict) -> str:
    """Canonical record hash: sha256 over sorted-keys JSON (hash field
    excluded by the caller) — identical rule to _audit_hash."""
    return hashlib.sha256(
        json.dumps(rec, sort_keys=True).encode("utf-8")).hexdigest()


def _req_chain_on() -> bool:
    """The 'evidence' license claim, read live per request. Never
    raises — chaining is a property of logging, which must never
    break the request path."""
    try:
        return licensing.allows("evidence")
    except Exception:  # noqa: BLE001
        return False


def _req_month_files() -> list[Path]:
    """Month log files in chronological order (archive first — it holds
    the oldest, pre-migration history; the legacy gateway_log.jsonl
    name is NOT a month file and is excluded)."""
    out: list[Path] = []
    arch = _LOG_PATH.parent / "gateway-log-archive.jsonl"
    if arch.is_file():
        out.append(arch)
    if _LOG_PATH.parent.is_dir():
        out += sorted(p for p in _LOG_PATH.parent.iterdir()
                      if re.fullmatch(r"gateway-log-\d{4}-\d{2}\.jsonl",
                                      p.name))
    return out


def _req_chain_tail_hash() -> str:
    """Chain head across cold starts and month rotations: walk the
    month files newest-first and return the LAST chained record's hash
    (plain-only tails keep walking — the chain is global across files
    and across plain stretches). Genesis when no chained record exists.
    Reads only file tails; a partial first line from the windowed read
    is discarded."""
    try:
        for p in reversed(_req_month_files()):
            try:
                with open(p, "rb") as f:
                    f.seek(0, 2)
                    size = f.tell()
                    window = 8192
                    f.seek(max(0, size - window))
                    skipped = size > window
                    tail = f.read().decode("utf-8", errors="replace")
            except OSError:
                continue
            lines = [l for l in tail.strip().splitlines() if l.strip()]
            if skipped and lines:
                lines = lines[1:]   # may be cut mid-record
            for ln in reversed(lines):
                try:
                    rec = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                h = rec.get("hash")
                if h:
                    return str(h)
                # plain record — keep walking back
        return _REQ_GENESIS
    except Exception:  # noqa: BLE001 — unreadable → fresh chain
        return _REQ_GENESIS


def _iter_month_lines(month: str) -> list[dict[str, Any]]:
    """Parsed lines of a month's log (missing file → [])."""
    try:
        raw = _month_log_path(month).read_text(
            encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    out = []
    for ln in raw:
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out


def _rollup_closed_months() -> None:
    """Aggregate any CLOSED month file lacking a rollup into
    gateway-rollup-YYYY-MM.json (the current month is never rolled up —
    it is still being written)."""
    try:
        current = _month_key()
        for p in sorted(_LOG_PATH.parent.glob("gateway-log-*.jsonl")):
            name = p.name[len("gateway-log-"):-len(".jsonl")]
            if not name or name >= current:
                continue
            rp = _LOG_PATH.parent / f"gateway-rollup-{name}.json"
            if rp.exists():
                continue
            agg = _aggregate_lines(_iter_month_lines(name))
            agg["month"] = name
            rp.write_text(json.dumps(agg), encoding="utf-8")
            logger.info("gateway log rolled up: %s", name)
    except OSError:
        logger.warning("gateway log rollup failed", exc_info=True)


def _aggregate_lines(lines: list[dict[str, Any]]) -> dict[str, Any]:
    """Parsed log lines → {per_model, per_key, total} (the rollup file
    format AND the current-month half of /admin/stats)."""
    per: dict[tuple[str, str], dict[str, Any]] = {}
    keys: dict[str, dict[str, Any]] = {}
    total = 0
    for e in lines:
        total += 1
        mk = (str(e.get("model") or "?"), str(e.get("backend") or "?"))
        agg = per.setdefault(mk, {
            "model": mk[0], "backend": mk[1], "n": 0, "ms_sum": 0,
            "max_ms": 0, "total_tokens": 0, "cost_usd": 0.0, "errors": 0})
        ms = int(e.get("ms") or 0)
        agg["n"] += 1
        agg["ms_sum"] += ms
        agg["max_ms"] = max(agg["max_ms"], ms)
        agg["total_tokens"] += int((e.get("usage") or {})
                                   .get("total_tokens") or 0)
        agg["cost_usd"] += float(e.get("cost_usd") or 0.0)
        if int(e.get("status") or 200) >= 400:
            agg["errors"] += 1
        kid = str(e.get("key") or "")
        if kid:
            kagg = keys.setdefault(kid, {"key": kid, "n": 0,
                                         "total_tokens": 0, "cost_usd": 0.0,
                                         "errors": 0})
            kagg["n"] += 1
            kagg["total_tokens"] += int((e.get("usage") or {})
                                        .get("total_tokens") or 0)
            kagg["cost_usd"] += float(e.get("cost_usd") or 0.0)
            if int(e.get("status") or 200) >= 400:
                kagg["errors"] += 1
    return {"per_model": list(per.values()), "per_key": list(keys.values()),
            "total": total}


def _scope_spend(scope: str) -> dict[str, float]:
    """{tokens, usd} this month for a spend scope — derived from the
    current month's log, cached in memory; month rollover re-derives.

    SHARED backends (multi-replica, tier D) never seed from the local
    log: it holds only THIS replica's requests, so seeding would
    undercount every other replica — the scope starts at 0 (accepted
    cold-start drift; charges accumulate on the shared ledger)."""
    month = _month_key()
    led = _SPEND_LEDGER.get(scope)
    if led is not None and led.get("month") == month:
        return {"tokens": int(led.get("tokens") or 0),
                "usd": float(led.get("usd") or 0.0)}
    _ensure_month_log()
    tokens = 0
    usd = 0.0
    if not getattr(get_storage(), "shared", False):
        kind, _, marker = scope.partition(":")
        for e in _iter_month_lines(month):
            if kind == "backend":
                if e.get("backend") != marker:
                    continue
            elif kind == "session":
                if e.get("session") != marker:
                    continue
            elif kind == "team":
                if e.get("team") != marker:
                    continue
            elif e.get("key") != marker:
                continue
            tokens += int((e.get("usage") or {}).get("total_tokens") or 0)
            usd += float(e.get("cost_usd") or 0.0)
    _SPEND_LEDGER[scope] = {"month": month, "tokens": tokens, "usd": usd}
    return {"tokens": tokens, "usd": usd}


def _charge_scope(scope: str, tokens: int, usd: float | None) -> None:
    if tokens <= 0 and not usd:
        return
    led = _SPEND_LEDGER.get(scope)
    if led is None or led.get("month") != _month_key():
        _scope_spend(scope)  # initialize from the log
        led = _SPEND_LEDGER.get(scope) or {}
    # read-modify-write (never in-place mutation): a swapped backend
    # (Redis/DB) only sees explicit sets.
    _SPEND_LEDGER[scope] = {
        "month": led.get("month") or _month_key(),
        "tokens": int(led.get("tokens") or 0) + int(tokens),
        "usd": float(led.get("usd") or 0.0) + float(usd or 0.0),
    }


def _token_budget_used(backend_name: str) -> int:
    """This-month tokens for a backend (the backend budget gate's view)."""
    return _scope_spend(f"backend:{backend_name}")["tokens"]


def reset_token_ledger() -> None:
    """Drop in-memory spend counters + alerts (tests / admin)."""
    _SPEND_LEDGER.clear()
    _SPEND_ALERTED.clear()


def _charge_request(cfg, backend_name: str, usage: dict,
                    cost_usd: float | None) -> None:
    """Charge a successful request: the backend's token ledger
    (openai-type, costed) + the consuming key's ledger (tokens always,
    usd when priced) + the key's TPM window (when rate_limit_tpm is
    configured) + the 80%-of-budget alert."""
    tokens = int((usage or {}).get("total_tokens") or 0)
    backend = cfg.backends.get(backend_name) or {}
    if backend.get("type") == "openai":
        _charge_scope(f"backend:{backend_name}", tokens, cost_usd)
    kid = _REQUEST_KEY_ID.get()
    if kid and kid != "admin":
        entry = (cfg.api_keys or {}).get(kid) or {}
        _charge_scope(f"key:{kid}", tokens, cost_usd)
        if int(entry.get("rate_limit_tpm") or 0) > 0:
            auth._tpm_charge(kid, tokens)
        _maybe_spend_alert(kid, entry)
    # Session budgets (v1.4): charge the session scope alongside the
    # key — same ledger machinery, so the enterprise pre-gate (and any
    # multi-replica shared backend) sees it for free.
    sid = _REQUEST_SESSION_ID.get()
    if sid:
        _charge_scope(f"session:{sid}", tokens, cost_usd)
    # Teams (v2.0): charge the team scope when the enterprise team link
    # attributed this request to a team — rollups and the team budget
    # gate read the same ledger.
    tid = _REQUEST_TEAM.get()
    if tid:
        _charge_scope(f"team:{tid}", tokens, cost_usd)


def _maybe_spend_alert(key_id: str, entry: dict) -> None:
    """Event-ring warning when a key crosses 80% of either monthly budget
    (once per key+month)."""
    tok_b = int(entry.get("monthly_token_budget") or 0)
    usd_b = float(entry.get("monthly_usd_budget") or 0)
    if tok_b <= 0 and usd_b <= 0:
        return
    month = _month_key()
    marker = f"key:{key_id}:{month}"
    if marker in _SPEND_ALERTED:
        return
    spend = _scope_spend(f"key:{key_id}")
    if ((tok_b > 0 and spend["tokens"] >= 0.8 * tok_b)
            or (usd_b > 0 and spend["usd"] >= 0.8 * usd_b)):
        _SPEND_ALERTED.add(marker)
        msg = (f"key {key_id!r} at ≥80% of monthly budget "
               f"({spend['tokens']} tokens, ${spend['usd']:.2f})")
        logger.warning("gateway: %s", msg)
        _event_ring().push({"ts": time.strftime("%H:%M:%S"),
                            "level": "WARNING", "logger": "gateway.budget",
                            "message": msg})


async def _token_budget_gate(cfg, backend_name: str) -> None:
    """Raise LifecycleBudgetExceeded (→ 503 budget_exceeded) when the
    backend's monthly token budget is exhausted. No-op without a budget."""
    backend = cfg.backends.get(backend_name) or {}
    if backend.get("type") != "openai":
        return
    try:
        budget = int(backend.get("monthly_token_budget") or 0)
    except (TypeError, ValueError):
        budget = 0
    if budget <= 0:
        return
    used = _scope_spend(f"backend:{backend_name}")["tokens"]
    if used >= budget:
        raise lifecycle.LifecycleBudgetExceeded(
            f"backend {backend_name!r}: monthly token budget exhausted "
            f"({used}/{budget} tokens) — raise monthly_token_budget or "
            f"wait for the month to roll over")


def _key_model_allowed(entry: dict, requested: str,
                       entries: list[tuple[str, str]]) -> bool:
    """Allowlist matching: an allowlist entry matches when it equals the
    requested name, a resolved 'backend:model' pair, the bare model, or
    a whole backend name. Empty allowlist (or containing '*') = all."""
    allowed = {str(a) for a in (entry.get("models") or [])}
    if not allowed or "*" in allowed:
        return True
    names = {requested}
    for b, m in entries:
        names.update({f"{b}:{m}", m, b})
    return bool(names & allowed)


def _key_model_gate(cfg, requested: str,
                    entries: list[tuple[str, str]]) -> Response | None:
    """Per-key model allowlist: 403 model_not_allowed when the virtual
    key may not use the requested model. Admin keys / open mode pass."""
    kid = _REQUEST_KEY_ID.get()
    if not kid or kid == "admin":
        return None
    entry = (cfg.api_keys or {}).get(kid)
    if not entry or _key_model_allowed(entry, requested, entries):
        return None
    return _error(403, f"key {kid!r} is not allowed to use {requested!r}",
                  param="model", code="model_not_allowed")


def _key_budget_gate(cfg) -> Response | None:
    """Per-KEY monthly budgets (tokens + USD): returns a 429 response when
    the presented virtual key is over budget; None when OK. Admin keys
    and open mode are exempt. Runs AFTER the response cache — cache hits
    cost nothing and stay available to over-budget keys."""
    kid = _REQUEST_KEY_ID.get()
    if not kid or kid == "admin":
        return None
    entry = (cfg.api_keys or {}).get(kid)
    if not entry:
        return None
    tok_b = int(entry.get("monthly_token_budget") or 0)
    usd_b = float(entry.get("monthly_usd_budget") or 0)
    if tok_b <= 0 and usd_b <= 0:
        return None
    spend = _scope_spend(f"key:{kid}")
    if tok_b > 0 and spend["tokens"] >= tok_b:
        return _error(429, f"key {kid!r}: monthly token budget exhausted "
                           f"({spend['tokens']}/{tok_b} tokens)",
                      err_type="rate_limit_error",
                      code="key_budget_exceeded")
    if usd_b > 0 and spend["usd"] >= usd_b:
        return _error(429, f"key {kid!r}: monthly spend budget exhausted "
                           f"(${spend['usd']:.2f}/${usd_b:.2f})",
                      err_type="rate_limit_error",
                      code="key_budget_exceeded")
    return None


# ── provider pricing (cost estimation) ─────────────────────────────────────

def _extract_price(model: dict[str, Any]) -> dict[str, Any] | None:
    """Cheapest live provider's $/Mtok rates + the per-provider map from
    the HF router's providers metadata."""
    provs = model.get("providers")
    if not isinstance(provs, list):
        return None
    best: dict[str, Any] | None = None
    provider_map: dict[str, Any] = {}
    for pv in provs:
        if not isinstance(pv, dict) or pv.get("status") not in (None, "live"):
            continue
        pr = pv.get("pricing") or {}
        try:
            pin = float(pr.get("input") or 0)
            pout = float(pr.get("output") or 0)
        except (TypeError, ValueError):
            continue
        pname = str(pv.get("provider") or "").strip()
        if pname and (pin or pout):
            provider_map[pname] = {"in": pin, "out": pout}
            if best is None or (pin + pout) < (best["in"] + best["out"]):
                best = {"in": pin, "out": pout}
    if not best:
        return None
    out = dict(best)
    if provider_map:
        out["providers"] = provider_map
    return out


def _capture_prices(backend_name: str, models: list) -> None:
    """Persist pricing from a fetched catalog into config model_prices
    (best-effort — never fails the catalog response)."""
    updates: dict[str, Any] = {}
    for m in models:
        mid = str((m or {}).get("id") or "").strip()
        if not mid:
            continue
        p = _extract_price(m)
        if p:
            updates[mid] = p
    if not updates:
        return

    def _mut(raw: dict) -> None:
        prices = raw.setdefault("model_prices", {})
        prices.setdefault(backend_name, {}).update(updates)

    try:
        _mutate_config(_mut, f"price-capture {backend_name}")
        logger.info("captured pricing for %d %s models",
                    len(updates), backend_name)
    except Exception:
        logger.warning("price capture failed", exc_info=True)


def _cost_usd(cfg, backend_name: str, wire_model: str,
              usage: dict) -> float | None:
    """Estimated request cost from cached provider pricing ($ per MILLION
    tokens). Suffix-aware: ':<provider>' uses that provider's rate;
    otherwise the cheapest provider's (the router's default :fastest may
    land anywhere — the estimate is deliberately conservative-low).
    None when the model isn't priced (local, runpod, uncataloged)."""
    prices = (cfg.model_prices or {}).get(backend_name) or {}
    p = prices.get(wire_model)
    if p is None and ":" in wire_model:
        base, suffix = wire_model.rsplit(":", 1)
        p = prices.get(base)
        if p:
            pv = (p.get("providers") or {}).get(suffix)
            if pv:
                p = {"in": pv.get("in", p.get("in") or 0),
                     "out": pv.get("out", p.get("out") or 0)}
    if not p:
        return None
    u = usage or {}
    try:
        return (int(u.get("prompt_tokens") or 0) * float(p.get("in") or 0)
                + int(u.get("completion_tokens") or 0)
                * float(p.get("out") or 0)) / 1_000_000
    except (TypeError, ValueError):
        return None


# ── request model ──────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")  # tolerate client extras (n, user, ...)

    model: str = ""
    messages: list[dict[str, Any]] = Field(min_length=1)
    tools: list[dict[str, Any]] | None = None
    stream: bool = False
    temperature: float | None = None
    max_tokens: int | None = None


# ── GET /v1/models ─────────────────────────────────────────────────────────

@app.get("/v1/models")
async def list_models() -> dict[str, Any]:
    """Model inventory with routing metadata.

    Every entry carries enough information for a consuming program to make
    smart routing decisions: capabilities, backend type, live status,
    VRAM cost, context length, locality (privacy), and alias resolution.

    OpenAI-compatible shape — extra fields are ignored by the SDK but
    available for programmatic consumption.
    """
    cfg = get_gateway_config()
    cfg.maybe_reload()
    engine = get_engine()

    # ── gather live state ────────────────────────────────────────────
    loaded = {m["alias"]: m for m in engine.loaded_models()}
    lsrv_status = {f"{s['backend']}:{s['model']}": s
                   for s in llamaserver.status()}
    rp_status = {f"{s['backend']}:{s['model']}": s
                 for s in runpod.status()}

    # embedding model names (local)
    embed_names = set(cfg.local_embedding_models())

    data: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(entry: dict[str, Any]) -> None:
        if entry["id"] and entry["id"] not in seen:
            seen.add(entry["id"])
            data.append(entry)

    # ── aliases (user-facing names, listed first) ────────────────────
    for name, alias in cfg.aliases.items():
        chain = alias.get("chain") or []
        if not chain:
            continue
        # v2.2 don't-display: an alias lists when ANY chain entry is
        # licensed (mixed chains serve their licensed part); an
        # all-unlicensed alias is invisible — nothing to click.
        if not any(licensing.provider_allowed(
                c.partition(":")[0],
                ((cfg.backends.get(c.partition(":")[0]) or {})
                 .get("type") or "")) for c in chain):
            continue
        primary_backend, _, primary_model = chain[0].partition(":")
        backend_cfg = cfg.backends.get(primary_backend, {})
        btype = backend_cfg.get("type", "gateway")

        # Determine capabilities from chain entries
        caps = _resolve_capabilities(chain, cfg, embed_names)
        is_local = all(cfg._entry_is_local(c.partition(":")[0]) for c in chain)

        _add({
            "id": name,
            "object": "model",
            "created": 0,
            "owned_by": primary_backend,
            "capabilities": caps,
            "backend_type": _backend_label(btype, primary_backend, cfg),
            "status": "available",
            "local": is_local,
            "chain": chain,
            "hedge_s": float(alias.get("hedge_s") or 0),
            "local_only": bool(alias.get("local_only")),
        })

    # ── smart routes (tier E) ────────────────────────────────────────
    for name, route in (getattr(cfg, "smart_routes", None) or {}).items():
        candidates = route.get("candidates") or []
        if not candidates:
            continue
        first = candidates[0]
        _add({
            "id": name,
            "object": "model",
            "created": 0,
            "owned_by": "smart-route",
            "capabilities": ["chat", "completions", "smart_routing"],
            "backend_type": f"smart:{route.get('strategy') or 'round_robin'}",
            "status": "available",
            "local": False,
            "candidates": [f"{c.get('backend')}:{c.get('model')}"
                           for c in candidates if isinstance(c, dict)],
        })

    # ── backend models ───────────────────────────────────────────────
    for bname, backend in cfg.backends.items():
        btype = backend.get("type")
        # v2.2 don't-display: unlicensed cloud backends never list
        # their models — consumers can't see (or click) what they
        # can't serve. Local types are always licensed.
        if not licensing.provider_allowed(bname, btype or ""):
            continue
        models = backend.get("models") or {}
        label = _backend_label(btype, bname, cfg)

        for mname, mcfg in models.items():
            key = f"{bname}:{mname}"
            caps = ["chat", "completions"]

            if btype == EMBEDDED_BACKEND:
                gguf = mcfg.get("gguf", mname)
                ctx = mcfg.get("ctx", 4096)
                workers = mcfg.get("workers", 1)
                placement = mcfg.get("placement", "")

                # Check loaded state
                inst = loaded.get(mname)
                status = "loaded" if inst else "available"
                vram = inst.get("size_vram", 0) if inst else 0
                role = inst.get("role", "chat") if inst else "chat"

                _add({
                    "id": mname,
                    "object": "model",
                    "created": 0,
                    "owned_by": bname,
                    "capabilities": caps,
                    "backend_type": label,
                    "status": status,
                    "local": True,
                    "vram_mb": vram // (1024 * 1024) if vram else 0,
                    "context_length": ctx,
                    "workers": workers,
                    "placement": placement,
                    "gguf": gguf,
                    "role": role,
                })

            elif btype == LLAMA_SERVER_BACKEND:
                base_url = backend.get("base_url")
                if base_url:
                    # Remote mode — always available
                    remote_id = mcfg if isinstance(mcfg, str) else mcfg.get("name", mname)
                    _add({
                        "id": mname,
                        "object": "model",
                        "created": 0,
                        "owned_by": bname,
                        "capabilities": caps,
                        "backend_type": label,
                        "status": "available",
                        "local": False,
                        "remote_id": remote_id,
                    })
                else:
                    # Subprocess mode
                    ctx = mcfg.get("ctx", 4096)
                    parallel = mcfg.get("parallel", 1)
                    ls = lsrv_status.get(key)
                    status = "loaded" if (ls and ls.get("state") == "running") else "available"

                    _add({
                        "id": mname,
                        "object": "model",
                        "created": 0,
                        "owned_by": bname,
                        "capabilities": caps,
                        "backend_type": label,
                        "status": status,
                        "local": True,
                        "context_length": ctx,
                        "parallel": parallel,
                        "pid": ls.get("pid") if ls else None,
                    })

            elif btype == RUNPOD_BACKEND:
                # Determine if serverless or pod
                has_endpoint = "endpoint_id" in mcfg
                if has_endpoint:
                    # Serverless — always available (cold start possible)
                    transport = mcfg.get("transport", "openai")
                    timeout = mcfg.get("timeout_s", backend.get("timeout_s", 120))
                    _add({
                        "id": mname,
                        "object": "model",
                        "created": 0,
                        "owned_by": bname,
                        "capabilities": caps,
                        "backend_type": label,
                        "status": "available",
                        "local": False,
                        "endpoint_id": mcfg.get("endpoint_id"),
                        "transport": transport,
                        "timeout_s": timeout,
                    })
                else:
                    # Pod mode
                    rp = rp_status.get(key)
                    state = (rp.get("state") if rp else "not-deployed")
                    status = "loaded" if state == "running" else state

                    _add({
                        "id": mname,
                        "object": "model",
                        "created": 0,
                        "owned_by": bname,
                        "capabilities": caps,
                        "backend_type": label,
                        "status": status,
                        "local": False,
                        "pod_id": (rp.get("pod_id") if rp else mcfg.get("pod_id")),
                        "idle_stop_s": mcfg.get("idle_stop_s", 0),
                        "idle_terminate_s": mcfg.get("idle_terminate_s", 0),
                    })

            elif btype in (OPENAI_BACKEND, "anthropic", "gemini",
                           "responses", "azure", "vertex", "bedrock"):
                # Cloud backend (openai-compatible or the v2.3 wire
                # translators — same shape: {name: remote_id}; azure's
                # remote_id is the DEPLOYMENT name)
                remote_id = mcfg if isinstance(mcfg, str) else mcfg.get("name", mname)
                _add({
                    "id": mname,
                    "object": "model",
                    "created": 0,
                    "owned_by": bname,
                    "capabilities": caps,
                    "backend_type": label,
                    "status": "available",
                    "local": False,
                    "remote_id": remote_id,
                })

            elif btype == RUNPOD_PUBLIC_BACKEND:
                # Public Endpoints — always available, no lifecycle
                _add({
                    "id": mname,
                    "object": "model",
                    "created": 0,
                    "owned_by": bname,
                    "capabilities": caps,
                    "backend_type": label,
                    "status": "available",
                    "local": False,
                    "model_slug": mcfg.get("model_slug"),
                    "timeout_s": mcfg.get("timeout_s", 120),
                })

    # ── auto-discovered GGUFs ────────────────────────────────────────
    local = cfg.local_backend_name() or "local"
    for g in installed_ggufs(cfg.models_dir):
        stem = g["name"]
        if stem in seen:
            continue
        inst = loaded.get(stem)
        is_embed = stem in embed_names
        caps = ["embeddings"] if is_embed else ["chat", "completions"]

        _add({
            "id": stem,
            "object": "model",
            "created": 0,
            "owned_by": local,
            "capabilities": caps,
            "backend_type": "embedded",
            "status": "loaded" if inst else "available",
            "local": True,
            "vram_mb": (inst.get("size_vram", 0) // (1024 * 1024)) if inst else 0,
            "context_length": (inst.get("n_ctx", 4096) if inst else 4096),
            "role": "embed" if is_embed else "chat",
            "quantization": g.get("quantization", "—"),
            "size_on_disk": g.get("size", 0),
        })

    # ── embedding models not yet covered ─────────────────────────────
    for mname in embed_names:
        if mname not in seen:
            inst = loaded.get(mname)
            _add({
                "id": mname,
                "object": "model",
                "created": 0,
                "owned_by": local,
                "capabilities": ["embeddings"],
                "backend_type": "embedded",
                "status": "loaded" if inst else "available",
                "local": True,
                "vram_mb": (inst.get("size_vram", 0) // (1024 * 1024)) if inst else 0,
                "role": "embed",
            })

    return {"object": "list", "data": data}


def _backend_label(btype: str | None, bname: str, cfg) -> str:
    """Human-readable backend type label."""
    if btype == EMBEDDED_BACKEND:
        return "embedded"
    if btype == LLAMA_SERVER_BACKEND:
        backend = cfg.backends.get(bname, {})
        if backend.get("base_url"):
            return "llama-server-remote"
        return "llama-server-subprocess"
    if btype == RUNPOD_BACKEND:
        return "runpod"
    if btype == OPENAI_BACKEND:
        return "openai-cloud"
    return btype or "unknown"


def _resolve_capabilities(chain: list[str], cfg, embed_names: set) -> list[str]:
    """Determine capabilities from chain entries."""
    caps = set()
    for entry in chain:
        backend_name = entry.partition(":")[0]
        model_key = entry.partition(":")[2]
        backend = cfg.backends.get(backend_name, {})
        if backend.get("type") == EMBEDDED_BACKEND:
            models = backend.get("models", {})
            mcfg = models.get(model_key, {})
            gguf = mcfg.get("gguf", model_key)
            if gguf in embed_names or model_key in embed_names:
                caps.add("embeddings")
            else:
                caps.add("chat")
                caps.add("completions")
        else:
            caps.add("chat")
            caps.add("completions")
    return sorted(caps) if caps else ["chat", "completions"]


# ── chain execution ────────────────────────────────────────────────────────

async def _ls_fit_prompt(cfg, backend_name: str, model_key: str, mgr,
                         base: str, messages: list[dict[str, Any]],
                         max_tokens: int | None,
                         tools: list[dict[str, Any]] | None
                         ) -> tuple[list[dict[str, Any]], list[dict[str, Any]] | None]:
    """Trim a chat payload to a llama-server child's per-slot context
    (ctx // parallel) and apply the per-model knobs.

    llama-server 400s on overflow instead of truncating, so trimming
    happens here exactly like the embedded path — but with the child's
    own /tokenize for exact counts and the NATIVE tools JSON cost. The
    knobs (no_think, max_prompt_tokens) live in
    backends.<name>.models.<model>, are read per request (hot-reload),
    and are deliberately NOT part of the child signature — flipping
    one never respawns the process."""
    m = cfg.backends[backend_name]["models"][model_key]
    try:
        prompt_cap = int(m.get("max_prompt_tokens") or 0) or None
    except (TypeError, ValueError):
        prompt_cap = None
    messages, tools = await asyncio.to_thread(
        llamaserver.fit_chat, base, messages, max_tokens, tools,
        mgr.ctx, mgr.parallel, prompt_cap,
        f"{backend_name}/{model_key}")
    if m.get("no_think"):
        messages = EmbeddedEngine._apply_no_think(messages)
    return messages, tools


async def _exec_entry(cfg, entry: tuple[str, str], body: ChatRequest,
                      requested: str) -> tuple[dict[str, Any], str, str]:
    """Execute ONE chain entry → (openai_response_dict, backend, model_key).

    Embedded → the in-process engine (tools via the JSON convention).
    llama-server subprocess / runpod pod → managed lifecycle, native
    OpenAI wire format. Cloud / remote llama-server / runpod serverless
    → AsyncOpenAI (tools as-is; max_tokens capped 800 for cloud-type
    only)."""
    backend_name, model_key = entry
    backend = cfg.backends[backend_name]
    btype = backend.get("type")

    if btype == "embedded":
        engine = get_engine()
        comp = await engine.chat.completions.create(
            model=model_key,
            messages=body.messages,
            tools=body.tools,
            temperature=body.temperature,
            max_tokens=body.max_tokens,
        )
        choice = comp.choices[0] if comp.choices else None
        if choice is None:  # unreachable with the current engine
            raise EmbeddedLLMError("empty completion")
        msg = choice.message
        out_msg: dict[str, Any] = {"role": msg.role, "content": msg.content}
        if msg.tool_calls:
            # OpenAI convention: content is null when the model called a tool
            out_msg["content"] = None
            out_msg["tool_calls"] = [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name,
                              "arguments": tc.function.arguments}}
                for tc in msg.tool_calls
            ]
        usage = comp.usage or {}
        return ({
            "id": f"chatcmpl-gw-{uuid.uuid4().hex[:12]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": requested,  # echo what the client asked for, like OpenAI
            "choices": [{
                "index": 0,
                "message": out_msg,
                "finish_reason": ("tool_calls" if msg.tool_calls
                                  else choice.finish_reason),
            }],
            "usage": {
                "prompt_tokens": int(usage.get("prompt_tokens") or 0),
                "completion_tokens": int(usage.get("completion_tokens") or 0),
                "total_tokens": int(usage.get("total_tokens") or 0),
            },
        }, backend_name, model_key)

    if btype == "llama-server" and not backend.get("base_url"):
        # Managed subprocess: native OpenAI wire format incl. tools —
        # no JSON-convention shim, no max_tokens cap (it's local).
        llamaserver.ensure_reaper()
        mgr = llamaserver.get_manager(cfg, backend_name, model_key)
        base = await mgr.ensure_ready()
        # Trim to the slot context + apply knobs (no_think / max_prompt
        # _tokens) — llama-server 400s on overflow instead of trimming.
        messages, tools = await _ls_fit_prompt(
            cfg, backend_name, model_key, mgr, base,
            body.messages, body.max_tokens, body.tools)
        payload: dict[str, Any] = {
            "model": mgr.alias,  # the child's --alias (its model id)
            "messages": messages,
        }
        if tools:
            payload["tools"] = tools
        if body.temperature is not None:
            payload["temperature"] = body.temperature
        if body.max_tokens:
            payload["max_tokens"] = body.max_tokens
        resp = await llamaserver.post_chat(base, payload,
                                           cfg.queue["timeout_s"])
        resp["model"] = requested  # consistent echo; served_by carries truth
        return resp, backend_name, model_key

    if btype == "runpod" and not backend["models"][model_key].get("endpoint_id"):
        # Managed GPU POD: find-or-deploy + health-wait, then proxy in
        # native OpenAI wire format through the pod's runpod.net URL.
        mgr = runpod.get_manager(cfg, backend_name, model_key)
        await _budget_gate(cfg, backend_name, model_key)  # v3: 503 over cap
        # Cold start: hold the request while the pod boots, but cap the
        # non-streaming hold well under the spec's full start_timeout_s
        # (glm recipes: 3600s) — past the cap, a structured 503 tells
        # clients to retry rather than dying on a silent long poll.
        hold = min(float(mgr.spec.get("start_timeout_s") or 600),
                   runpod.WARMUP_MAX_HOLD_S)
        async with mgr.request():  # v3: inflight guard for the watchdog
            base = await mgr.ensure_ready(timeout_s=hold)
            payload = {
                "model": mgr.spec.get("alias", model_key),
                "messages": body.messages,
            }
            if body.tools:
                payload["tools"] = body.tools
            if body.temperature is not None:
                payload["temperature"] = body.temperature
            if body.max_tokens:
                payload["max_tokens"] = body.max_tokens
            resp = await llamaserver.post_chat(base, payload,
                                               cfg.queue["timeout_s"])
        resp["model"] = requested
        return resp, backend_name, model_key

    if btype == "runpod" and backend["models"][model_key].get("transport") == "runsync":
        # Serverless RAW job protocol (Ollama-style worker): runsync + poll,
        # output mapped Ollama → OpenAI. Not streamable incrementally (the
        # /stream SSE endpoint is not available on all endpoints) — streaming
        # requests collect then emit.
        m = backend["models"][model_key]
        body_input: dict[str, Any] = {
            "model": m["remote_id"],
            "messages": body.messages,
        }
        if body.temperature is not None:
            body_input["temperature"] = body.temperature
        if body.max_tokens:
            body_input["max_tokens"] = body.max_tokens
        out = await runpod.runsync_chat(
            m["endpoint_id"], body_input,
            backend.get("api_key_env") or "RUNPOD_API_KEY",
            m.get("timeout_s") or cfg.queue["timeout_s"],
            policy=m.get("policy"))
        resp = runpod.ollama_output_to_openai(out, requested)
        return resp, backend_name, model_key

    if btype == "runpod-public":
        # Public Endpoints: pre-deployed, always warm, runsync protocol
        # Model config: {"model_slug": "black-forest-labs-flux-1-schnell"}
        m = backend["models"][model_key]
        body_input: dict[str, Any] = {
            "model": m.get("remote_id", m["model_slug"]),
            "messages": body.messages,
        }
        if body.temperature is not None:
            body_input["temperature"] = body.temperature
        if body.max_tokens:
            body_input["max_tokens"] = body.max_tokens
        out = await runpod.runsync_public_chat(
            m["model_slug"], body_input,
            backend.get("api_key_env") or "RUNPOD_API_KEY",
            m.get("timeout_s") or cfg.queue["timeout_s"],
            policy=m.get("policy"))
        resp = runpod.ollama_output_to_openai(out, requested)
        return resp, backend_name, model_key

    # Cloud-shaped path: openai backends, REMOTE llama-server, and runpod
    # SERVERLESS all speak OpenAI over HTTP with a bearer key.
    remote_id = backend["models"][model_key]
    if btype in ("anthropic", "gemini", "responses", "vertex", "bedrock"):
        # v2.3 wire-translator backends: OpenAI chat → the provider's
        # native wire format → OpenAI chat (gateway/wire.py). Same
        # budget/cost/logging posture as the openai path.
        await _token_budget_gate(cfg, backend_name)
        if btype == "anthropic":
            d = await wire.anthropic_chat(
                backend, str(remote_id), body.messages,
                body.temperature, body.max_tokens, body.tools,
                cfg.queue["timeout_s"])
        elif btype == "gemini":
            d = await wire.gemini_chat(
                backend, str(remote_id), body.messages,
                body.temperature, body.max_tokens, body.tools,
                cfg.queue["timeout_s"])
        elif btype == "vertex":
            d = await wire.vertex_chat(
                backend, str(remote_id), body.messages,
                body.temperature, body.max_tokens, body.tools,
                cfg.queue["timeout_s"])
        elif btype == "bedrock":
            d = await wire.bedrock_converse(
                backend, str(remote_id), body.messages,
                body.temperature, body.max_tokens, body.tools,
                cfg.queue["timeout_s"])
        else:
            d = await wire.responses_chat(
                backend, str(remote_id), body.messages,
                body.temperature, body.max_tokens, body.tools,
                cfg.queue["timeout_s"])
        d["model"] = requested  # consistent echo
        usage = d.get("usage") or {}
        cost = _cost_usd(cfg, backend_name, str(remote_id), usage)
        if cost is not None:
            d["cost_usd"] = round(cost, 6)
        _charge_request(cfg, backend_name, usage, cost)
        return d, backend_name, model_key
    client_name = backend_name
    cloud_backend = backend
    if btype == "runpod":
        m = backend["models"][model_key]
        remote_id = m["remote_id"]
        # per-endpoint client cache (one backend may hold several endpoints)
        client_name = f"runpod-{backend_name}-{model_key}"
        cloud_backend = {
            "base_url": runpod.serverless_base_url(m["endpoint_id"]),
            "api_key_env": backend.get("api_key_env") or "RUNPOD_API_KEY",
        }
    # v4.5: per-model timeout covers serverless cold starts — the openai
    # SDK would otherwise cap at the queue timeout (×3 retries with its
    # default retry policy = 362s, E2E-measured)
    client_timeout = (float(m.get("timeout_s") or 0)
                      if btype == "runpod" else 0.0) \
        or cfg.queue["timeout_s"]
    if btype == "openai":
        # monthly token budget (503 budget_exceeded when exhausted)
        await _token_budget_gate(cfg, backend_name)
    if btype == "azure":
        # v2.3: Azure OpenAI — per-model client (the deployment lives
        # in the URL path); the shared openai execution path below
        # serves it (model in body = the deployment name).
        await _token_budget_gate(cfg, backend_name)
        client = cloud.get_azure_client(
            f"azure-{backend_name}-{model_key}", backend,
            str(remote_id), timeout=client_timeout)
    else:
        client = cloud.get_cloud_client(client_name, cloud_backend,
                                        timeout=client_timeout)
    kwargs: dict[str, Any] = {
        "model": remote_id,  # remote model id
        "messages": body.messages,
    }
    if body.temperature is not None:
        kwargs["temperature"] = body.temperature
    if body.max_tokens:
        # cap per the backend's policy (openai-type cloud = cost control;
        # max_tokens_cap null = uncapped; local llama-server and runpod
        # per-second billing pass through)
        kwargs["max_tokens"] = cloud.cap_max_tokens(backend, body.max_tokens)
    if body.tools:
        kwargs["tools"] = body.tools
    try:
        resp = await client.chat.completions.create(**kwargs)
    except cloud.APIConnectionError as e:
        # Connect-class failure: the request never reached the provider —
        # one immediate retry of the SAME model (policy: a retry is not
        # engine switching). Timeouts are NOT retried (ambiguous + slow).
        logger.info("gateway: connect failure on %s (%s) — one retry",
                    backend_name, str(e)[:120])
        try:
            resp = await client.chat.completions.create(**kwargs)
        except Exception as e2:
            raise cloud.CloudBackendError(
                f"cloud backend {backend_name!r} call failed: {e2}") from e2
    except Exception as e:
        raise cloud.CloudBackendError(
            f"cloud backend {backend_name!r} call failed: {e}") from e
    d = (json.loads(resp.model_dump_json())
         if hasattr(resp, "model_dump_json") else dict(resp))
    d["model"] = requested  # consistent echo; served_by carries the truth
    usage = d.get("usage") or {}
    if btype in ("openai", "azure"):
        cost = _cost_usd(cfg, backend_name, str(remote_id), usage)
        if cost is not None:
            d["cost_usd"] = round(cost, 6)  # estimate rides to the caller
        _charge_request(cfg, backend_name, usage, cost)
    else:
        _charge_request(cfg, backend_name, usage, None)
    return d, backend_name, model_key


async def _first_success(tasks: list[asyncio.Task],
                         cancel_ok: list[bool]) -> Any:
    """Race tasks; return the first successful result. Losers: cancelled
    where safe (cloud), left to finish where not (embedded — cancelling
    mid-generation would leave a zombie thread inside llama.cpp, so its
    lock must be held until the generation actually completes)."""
    pending = set(tasks)
    errs: list[BaseException] = []
    while pending:
        done, pending = await asyncio.wait(pending,
                                           return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if not t.cancelled() and t.exception() is None:
                for p, ok in zip(tasks, cancel_ok):
                    if p is not t and not p.done() and ok:
                        p.cancel()
                    elif p is not t and not p.done():
                        # can't cancel safely — retrieve the result silently
                        p.add_done_callback(
                            lambda ft: ft.exception() if not ft.cancelled() else None)
                return t.result()
            if not t.cancelled() and t.exception() is not None:
                errs.append(t.exception())
    raise errs[0] if errs else RuntimeError("race produced no result")


# ── chain cold breaker (ported from the assistant's RunPod hedge
# bookkeeping): a chain PRIMARY that stalls past hedge_s (or errors)
# gets skipped for the cooldown window instead of burning hedge_s on
# every request racing a bad backend. Keyed "backend:model". A primary
# win clears it; expiry is the retry. Single-entry chains execute
# regardless (no fallback to skip to). Fallback entries are not
# breaker-tracked — only primaries pay the per-request hedge cost. ──

_COLD_BREAK_S = 120.0
# Breaker state rides the storage backend ("cold_breakers"); EPOCH
# stamps so a shared backend compares correctly across replicas.
_COLD_UNTIL = NamespaceDict("cold_breakers")


def _breaker_open(key: str) -> bool:
    return time.time() < _COLD_UNTIL.get(key, 0.0)


def _breaker_mark(key: str) -> None:
    _COLD_UNTIL[key] = time.time() + _COLD_BREAK_S


def _breaker_clear(key: str) -> None:
    _COLD_UNTIL.pop(key, None)


def reset_breakers() -> None:
    """Drop breaker state (tests)."""
    _COLD_UNTIL.clear()


async def _exec_chain(cfg, entries: list[tuple[str, str]], hedge_s: float,
                      body: ChatRequest, requested: str):
    """Execute an alias chain with hedge semantics.

    Single entry → direct. Multi entry → run the primary; if it completes
    within hedge_s (or hedge_s is 0) it wins; otherwise (or on its error)
    the next entry races it — first completion wins, remaining entries
    follow the same rule. A recently-stalled primary is skipped outright
    (cold breaker above)."""
    if len(entries) == 1:
        return await _exec_entry(cfg, entries[0], body, requested)

    pkey = f"{entries[0][0]}:{entries[0][1]}"
    if _breaker_open(pkey):
        logger.info("chain primary %s cold (recent stall) — starting at the fallback",
                    pkey)
        primary = None
    else:
        primary = asyncio.create_task(_exec_entry(cfg, entries[0], body, requested))
    if primary is not None:
        if hedge_s and hedge_s > 0:
            try:
                result = await asyncio.wait_for(asyncio.shield(primary), hedge_s)
                _breaker_clear(pkey)
                return result
            except asyncio.TimeoutError:
                _breaker_mark(pkey)  # hedge fires — fall through to the race
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Client errors (4xx) are the REQUEST's fault — the backend
                # is fine and must not be breaker-skipped for 120s.
                if cloud.classify_error(e) != "client":
                    _breaker_mark(pkey)
                primary = None  # primary failed fast — fallbacks only
        else:
            # no hedge configured: wait for the primary; on error, fall back
            try:
                result = await primary
                _breaker_clear(pkey)
                return result
            except Exception as e:
                if cloud.classify_error(e) != "client":
                    _breaker_mark(pkey)
                primary = None

    def _cancel_ok(entry: tuple[str, str]) -> bool:
        return (cfg.backends.get(entry[0]) or {}).get("type") != "embedded"

    last_err: BaseException | None = None
    for entry in entries[1:]:
        fb = asyncio.create_task(_exec_entry(cfg, entry, body, requested))
        racers = ([primary, fb] if primary is not None else [fb])
        try:
            result = await _first_success(
                racers, [_cancel_ok(entries[0]), _cancel_ok(entry)][:len(racers)])
            if (primary is not None and primary.done()
                    and not primary.cancelled()
                    and primary.exception() is None):
                _breaker_clear(pkey)  # primary finished fine after all
            return result
        except Exception as e:
            last_err = e
            primary = None
    raise last_err or RuntimeError("no chain entry succeeded")


# ── response cache (ported from the assistant's LLMClient) ─────────────────
# Repeated SIMPLE prompts (no tools, single user message, nothing
# time-sensitive, short) answer from memory instead of re-billing cloud
# backends per token — health checks and retrying third-party programs
# are the target. local_only aliases NEVER cache (privacy guarantee
# scope); streaming never caches. Sizing/kill switch: config
# response_cache {enabled, ttl_s, max} — hot-reloadable.

# Cache entries ride the storage backend (namespace "response_cache")
# — a shared backend means replicas share cached answers. "ts" is an
# EPOCH stamp so TTLs are comparable across processes.
_RESPONSE_CACHE = NamespaceDict("response_cache")
_CACHE_TIME_KEYWORDS = ("time", "date", "today", "now", "current",
                        "weather", "forecast", "latest", "news")


def _cache_key(requested: str, body: ChatRequest) -> str:
    """'' when the request is not cacheable (tools/stream/multi-turn/
    long/time-sensitive prompts). Key covers model, messages AND the
    generation params that change the answer (max_tokens, temperature)."""
    if body.tools or body.stream:
        return ""
    msgs = body.messages
    if not msgs or msgs[-1].get("role") != "user" or len(msgs) > 2:
        return ""
    text = str(msgs[-1].get("content") or "").strip().lower()
    if not text or len(text) > 200:
        return ""
    if any(kw in text for kw in _CACHE_TIME_KEYWORDS):
        return ""
    payload = json.dumps([requested, body.messages, body.max_tokens,
                          body.temperature], sort_keys=True, default=str)
    return hashlib.md5(payload.encode()).hexdigest()


def cache_stats() -> dict[str, Any]:
    total_hits = sum(e.get("hits", 0) for e in _RESPONSE_CACHE.values())
    return {"entries": len(_RESPONSE_CACHE), "hits": total_hits,
            "semantic": semantic_cache.stats()}


def clear_response_cache() -> int:
    """Drop cached responses (tests / admin). Returns exact-cache
    entries cleared; the semantic layer clears alongside."""
    n = len(_RESPONSE_CACHE)
    _RESPONSE_CACHE.clear()
    semantic_cache.clear()
    return n


# ── POST /v1/chat/completions ──────────────────────────────────────────────

@app.post("/v1/chat/completions")
async def chat_completions(body: ChatRequest) -> Response:
    cfg = get_gateway_config()
    cfg.maybe_reload()
    t0 = time.perf_counter()

    # Queue admission: capacity = resident instances (min 1) + running
    # llama-server children (they batch concurrently) + max_waiting.
    # Over-limit requests get 429 + Retry-After instead of piling onto the
    # serialized per-instance locks. Stale (zombie) slots are evicted first.
    engine = get_engine()
    _reap_stale_chat_slots(cfg)
    capacity = (max(1, len(engine.loaded_models()))
                + llamaserver.running_count() + cfg.queue["max_waiting"])
    if len(_chat_slots) >= capacity:
        _log_request(body.model, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 429)
        return _error(429, "gateway busy — too many concurrent requests, "
                      "retry shortly", err_type="rate_limit_error",
                      code="rate_limited", headers={"Retry-After": "1"})

    requested = body.model.strip() or cfg.default_alias
    # Traffic capture seam (v2.4): entry observation — before any
    # execution, so early exits (429/400/403/guardrail) still flush
    # with their terminal status and the request side captured.
    traffic.observe_request(requested, body)
    try:
        plan = cfg.resolve_chain(requested)
    except ProviderNotLicensedError:
        # v2.2 don't-display doctrine: unlicensed backends never list
        # (see /v1/models) and a request for one is byte-for-byte the
        # unknown-model answer — no existence leak, no license signal
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 400,
                     err="license")
        return _error(400, f"unknown model: {requested!r} (not an alias "
                      "or a backend model)", param="model",
                      code="model_not_found")
    except GatewayConfigError as e:
        # Smart route? (aliases and backend models always win)
        plan = _smart_route_fallback(cfg, requested)
        if plan is None and requested and EmbeddedEngine.resolve_gguf(
                requested, cfg.models_dir):
            # Auto-discovery: any GGUF in models_dir is servable by its stem
            plan = {"entries": [(cfg.local_backend_name() or "local", requested)],
                    "hedge_s": 0.0, "local_only": False}
        if plan is None:
            _log_request(requested, "", body.stream,
                         int((time.perf_counter() - t0) * 1000), {}, 400)
            return _error(400, str(e), param="model", code="model_not_found")
    entries = plan["entries"]

    # Per-key model allowlist (before the cache — a cached disallowed
    # model is still disallowed)
    denied = _key_model_gate(cfg, requested, entries)
    if denied is not None:
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 403)
        return denied

    # Request guardrails (before the cache — a blocked prompt never
    # serves a cached answer; streams included)
    blocked = apply_request_guardrails(cfg, requested, body.messages)
    if blocked is not None:
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 400,
                     err="guardrail")
        return blocked

    # Response cache: simple repeated prompts answer from memory. Never
    # for local_only aliases (privacy guarantee) or when disabled in config.
    rc_cfg = getattr(cfg, "response_cache", None) or {}
    ckey = ""
    if rc_cfg.get("enabled", True) and not plan["local_only"]:
        ckey = _cache_key(requested, body)
    if ckey:
        rc_ttl = float(rc_cfg.get("ttl_s") or 3600)
        hit = _RESPONSE_CACHE.get(ckey)
        if hit and (time.time() - hit["ts"]) < rc_ttl:
            # read-modify-write the hit counter (backend-visible)
            _RESPONSE_CACHE[ckey] = {**hit,
                                     "hits": int(hit.get("hits", 0)) + 1}
            resp = dict(hit["resp"])
            # observe BEFORE the log call — the flush inside _log_request
            # drops the collector token
            traffic.observe_response(_capture_content(resp))
            _log_request(requested, "cache", body.stream, 0,
                         resp.get("usage") or {}, 200)
            return JSONResponse(resp)
        if hit:  # expired — drop before re-executing
            _RESPONSE_CACHE.pop(ckey, None)
        # Semantic cache (v2.0): paraphrase hit for the SAME eligible
        # class. Scoped to (model, key, params) — never cross-key; the
        # local embed call is milliseconds and never gates serving (any
        # embed failure = transparent skip). The vector rides back so a
        # miss stores without re-embedding.
        sem_resp, sem_vec = await semantic_cache.lookup(
            requested, str(body.messages[-1].get("content") or ""),
            _REQUEST_KEY_ID.get(),
            (body.max_tokens, body.temperature))
        if sem_resp is not None:
            _metric_inc("gateway_semantic_cache_hits_total", (requested,))
            traffic.observe_response(_capture_content(sem_resp))
            _log_request(requested, "semantic-cache", body.stream, 0,
                         sem_resp.get("usage") or {}, 200)
            return JSONResponse(sem_resp)

    # Per-key monthly budgets (AFTER the cache — hits are free). Admin
    # keys and open mode pass through.
    over = _key_budget_gate(cfg)
    if over is not None:
        ms = int((time.perf_counter() - t0) * 1000)
        _log_request(requested, "", body.stream, ms, {}, 429,
                     err="key_budget")
        return over

    if body.stream:
        # Streaming executes the PRIMARY entry only — hedge racing is a
        # non-streaming feature (first-completion-wins has no stream analog
        # short of first-token hedging, which is a follow-up).
        slot = _acquire_chat_slot()
        return _stream_chat(requested, entries[0], body, slot)

    slot = _acquire_chat_slot()
    try:
        try:
            resp, winner, model_key = await _exec_chain(
                cfg, entries, plan["hedge_s"], body, requested)
        except runpod.RunPodWarmingUp as e:
            # Cold start still booting past the non-streaming hold cap —
            # retryable state, not a failure of the model.
            ms = int((time.perf_counter() - t0) * 1000)
            logger.info("gateway chat warming up (%s): %s", requested, e)
            _log_request(requested, "", body.stream, ms, {}, 503)
            return _error(503, str(e), err_type="overloaded_error",
                          code="warming_up", headers={"Retry-After": "30"})
        except lifecycle.LifecycleBudgetExceeded as e:
            # v3: monthly cap hit — refuse to START the pod
            ms = int((time.perf_counter() - t0) * 1000)
            _log_request(requested, "", body.stream, ms, {}, 503)
            return _error(503, str(e), err_type="overloaded_error",
                          code="budget_exceeded")
        except Exception as e:
            return _exec_error_response(e, requested, body.stream, t0)
        ms = int((time.perf_counter() - t0) * 1000)
        # Charge NON-openai winners here (v2.0 fix): the cloud branch
        # charges inside _exec_entry, but embedded/llama-server/runpod
        # returned uncharged — key/session/team ledgers relied on the
        # lazy log-seed, which caches 0 forever when a budget gate
        # queries the scope before the first log line lands (exactly
        # what the team pre-gate does). Streaming still rides the
        # log-seed (pre-existing, noted).
        if (cfg.backends.get(winner) or {}).get("type") != "openai":
            _charge_request(cfg, winner, resp.get("usage") or {},
                            resp.get("cost_usd"))
        # smart-routing latency bookkeeping (feeds the 'fastest' strategy)
        _record_route_latency(winner, model_key, ms)
        # response guardrails (non-stream only — streams are not buffered)
        _content = str((((resp.get("choices") or [{}])[0])
                        .get("message") or {}).get("content") or "")
        _content, _blocked = apply_response_guardrails(cfg, _content)
        if _blocked is not None:
            _log_request(requested, winner, body.stream, ms, {}, 400,
                         err="guardrail")
            return _blocked
        if _content:
            resp["choices"][0]["message"]["content"] = _content
        traffic.observe_response(_content)
        resp.setdefault("served_by", {"backend": winner, "model": model_key,
                                      "ms": ms})
        if ckey:
            # Store under the cache (bounded; oldest evicted). A max of 0
            # disables storage while leaving lookups inert. Epoch ts —
            # comparable across replicas on a shared backend.
            rc_max = int(rc_cfg.get("max") or 100)
            if rc_max > 0:
                if len(_RESPONSE_CACHE) >= rc_max:
                    oldest = min(_RESPONSE_CACHE.items(),
                                 key=lambda kv: kv[1]["ts"])
                    _RESPONSE_CACHE.pop(oldest[0], None)
                _RESPONSE_CACHE[ckey] = {"resp": dict(resp),
                                         "ts": time.time(), "hits": 0}
            # Semantic layer stores under the prompt VECTOR (computed
            # pre-exec; None = layer off/failed → no-op).
            await semantic_cache.store(
                requested, _REQUEST_KEY_ID.get(), sem_vec, dict(resp),
                (body.max_tokens, body.temperature))
        _log_request(requested, winner, body.stream, ms,
                     resp.get("usage") or {}, 200,
                     cost_usd=resp.get("cost_usd"))
        return JSONResponse(resp)
    finally:
        _release_chat_slot(slot)


# ── POST /v1/chat/completions (stream) ─────────────────────────────────────

# SSE comment cadence while a cold runpod pod boots (keep-alive so
# clients don't idle-timeout on a silent stream). Constant so tests
# can tighten it.
RUNPOD_SSE_KEEPALIVE_S = 15.0


def _estimate_stream_usage(body: ChatRequest, text_chars: int) -> dict:
    """Fallback token estimate for streams whose wire format carries no
    usage (embedded engine, passthrough SSE without a usage chunk):
    chars//4 on both sides — the same heuristic as the TPM gate's
    prompt estimate. An ESTIMATE (comment wherever budgets read it);
    real usage wins whenever the wire provides it."""
    prompt = len(json.dumps(body.messages, default=str)) // 4
    comp = text_chars // 4
    return {"prompt_tokens": prompt, "completion_tokens": comp,
            "total_tokens": prompt + comp}


def _stream_chat(requested: str, entry: tuple[str, str],
                 body: ChatRequest, slot: int) -> StreamingResponse:
    """SSE bridge — embedded engine (think-hold-back), llama-server
    subprocess (native SSE passthrough), or cloud passthrough.

    OpenAI wire format: role chunk → content chunks → finish chunk →
    `data: [DONE]` (cloud + llama-server chunks pass through natively).
    Mid-flight errors become an OpenAI-shaped error object + [DONE].

    Streaming with tools:
      — cloud / llama-server: forwarded as-is (native tool-call deltas)
      — embedded: content deltas streamed live; at stream end the buffered
        text is parsed for a JSON tool call → emitted as a delta.tool_calls
        chunk (JSON-convention shim has no token-level tool boundaries)."""
    chat_id = f"chatcmpl-gw-{uuid.uuid4().hex[:12]}"
    created = int(time.time())
    backend_name, model_key = entry

    def _chunk(delta: dict[str, Any], finish: str | None = None) -> str:
        event = {"id": chat_id, "object": "chat.completion.chunk",
                 "created": created, "model": requested,
                 "choices": [{"index": 0, "delta": delta,
                              "finish_reason": finish}]}
        return f"data: {json.dumps(event)}\n\n"

    async def _gen() -> AsyncIterator[str]:
        cfg = get_gateway_config()
        backend = cfg.backends[backend_name]
        t0 = time.perf_counter()
        # Stream spend accounting (v2.0 fix): streams used to log usage
        # {} and never charge — invisible to every spend surface. Real
        # usage wins when the wire provides it (openai include_usage,
        # SSE chunks carrying usage); otherwise a chars//4 estimate.
        usage_out: dict[str, Any] = {}
        text_chars = 0

        def _charge_and_log(ms: int) -> None:
            usage = usage_out or _estimate_stream_usage(body, text_chars)
            cost = None
            if backend.get("type") in ("openai", "anthropic", "gemini",
                                       "responses", "azure", "vertex",
                                       "bedrock"):
                rid = str((backend.get("models") or {}).get(model_key) or "")
                cost = _cost_usd(cfg, backend_name, rid, usage)
            _charge_request(cfg, backend_name, usage, cost)
            _log_request(requested, backend_name, True, ms, usage, 200,
                         cost_usd=cost)

        try:
            if backend.get("type") in ("anthropic", "gemini", "responses",
                                       "vertex", "bedrock"):
                # v2.3 wire-translator backends: complete fully, then
                # emit ONE OpenAI-shaped chunk + [DONE] (valid streaming
                # shape — every OpenAI client accepts single-chunk
                # streams; the shared tail charges/logs).
                resp, _, _ = await _exec_entry(cfg, entry, body, requested)
                usage_out.update(resp.get("usage") or {})
                traffic.observe_response(_capture_content(resp))
                yield "data: " + json.dumps(resp) + "\n\n"
            elif backend.get("type") == "embedded":
                engine = get_engine()
                stream = await engine.chat.completions.create(
                    model=model_key,
                    messages=body.messages,
                    tools=body.tools,
                    temperature=body.temperature,
                    max_tokens=body.max_tokens,
                    stream=True,
                )
                yield _chunk({"role": "assistant", "content": ""})
                # Buffer content for post-stream tool-call parsing
                content_buf: list[str] = []
                async for ch in stream:
                    for c in ch.choices:
                        text = c.delta.get("content")
                        if text:
                            content_buf.append(text)
                            text_chars += len(text)
                            traffic.observe_response(text)
                            yield _chunk({"content": text})
                # If tools were offered, check buffered content for a tool call
                if body.tools and content_buf:
                    tc = engine._parse_tool_call("".join(content_buf), body.tools)
                    if tc is not None:
                        yield _chunk({
                            "tool_calls": [{
                                "index": 0,
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }]
                        })
                        yield _chunk({}, finish="tool_calls")
                        yield "data: [DONE]\n\n"
                        ms = int((time.perf_counter() - t0) * 1000)
                        logger.info("gateway stream ok (%s / %s) in %dms [tool]",
                                    backend_name, model_key, ms)
                        _charge_and_log(ms)
                        return
                yield _chunk({}, finish="stop")
            elif (backend.get("type") == "llama-server"
                    and not backend.get("base_url")):
                # Managed subprocess — native OpenAI SSE incl. tool-call
                # deltas (no JSON-convention shim needed); [DONE] arrives
                # from the child, the common tail re-adds it if not.
                llamaserver.ensure_reaper()
                mgr = llamaserver.get_manager(cfg, backend_name, model_key)
                base = await mgr.ensure_ready()
                # Same trim + knobs as the non-streaming path — mid-stream
                # SSE errors from an overflowed child are the worst mode.
                messages, tools = await _ls_fit_prompt(
                    cfg, backend_name, model_key, mgr, base,
                    body.messages, body.max_tokens, body.tools)
                payload: dict[str, Any] = {
                    "model": mgr.alias,
                    "messages": messages,
                    "stream": True,
                }
                if tools:
                    payload["tools"] = tools
                if body.temperature is not None:
                    payload["temperature"] = body.temperature
                if body.max_tokens:
                    payload["max_tokens"] = body.max_tokens
                async for data in llamaserver.stream_chat(
                        base, payload, cfg.queue["timeout_s"]):
                    if data == "[DONE]":
                        break
                    try:                    # usage/content capture
                        d = json.loads(data)
                        if isinstance(d, dict):
                            if d.get("usage"):
                                usage_out = d["usage"]
                            ch0 = ((d.get("choices") or [{}])[0]
                                   .get("delta") or {}).get("content")
                            if ch0:
                                text_chars += len(ch0)
                                traffic.observe_response(ch0)
                    except ValueError:
                        pass
                    yield f"data: {data}\n\n"
            elif (backend.get("type") == "runpod"
                    and not backend["models"][model_key].get("endpoint_id")):
                # Managed GPU POD — same native SSE passthrough as the
                # llama-server subprocess, through the pod's proxy URL.
                # Cold start: hold the stream open for the FULL boot
                # window (glm recipes take 6+ min) while emitting SSE
                # comment keep-alives — clients stay connected and can
                # see "warming up" instead of timing out on silence.
                mgr = runpod.get_manager(cfg, backend_name, model_key)
                try:
                    await _budget_gate(cfg, backend_name, model_key)
                except lifecycle.LifecycleBudgetExceeded as e:
                    yield "data: " + json.dumps({"error": {
                        "message": str(e), "type": "overloaded_error",
                        "code": "budget_exceeded"}}) + "\n\n"
                    yield "data: [DONE]\n\n"
                    return
                async with mgr.request():  # v3: inflight guard — the
                    # generator's finally runs on completion OR client
                    # disconnect, so the watchdog can't stop the pod
                    # under a live stream
                    warm = asyncio.ensure_future(mgr.ensure_ready())
                    while True:
                        try:
                            base = await asyncio.wait_for(
                                asyncio.shield(warm), RUNPOD_SSE_KEEPALIVE_S)
                            break
                        except asyncio.TimeoutError:
                            st = mgr.pod_status or "starting"
                            yield (": runpod pod warming up "
                                   f"({st}) — first request after a cold "
                                   "start; model weights are loading\n\n")
                    payload = {
                        "model": mgr.spec.get("alias", model_key),
                        "messages": body.messages,
                        "stream": True,
                    }
                    if body.tools:
                        payload["tools"] = body.tools
                    if body.temperature is not None:
                        payload["temperature"] = body.temperature
                    if body.max_tokens:
                        payload["max_tokens"] = body.max_tokens
                    async for data in llamaserver.stream_chat(
                            base, payload, cfg.queue["timeout_s"]):
                        if data == "[DONE]":
                            break
                        try:                # usage/content capture
                            d = json.loads(data)
                            if isinstance(d, dict):
                                if d.get("usage"):
                                    usage_out = d["usage"]
                                ch0 = ((d.get("choices") or [{}])[0]
                                       .get("delta") or {}).get("content")
                                if ch0:
                                    text_chars += len(ch0)
                                    traffic.observe_response(ch0)
                        except ValueError:
                            pass
                        yield f"data: {data}\n\n"
            elif (backend.get("type") == "runpod"
                    and backend["models"][model_key].get("endpoint_id")
                    and backend["models"][model_key].get("transport") == "runsync"):
                # Serverless RAW job protocol — TRUE streaming via
                # POST /run + GET /stream/{id} (SSE of worker chunks);
                # falls back to collect-then-emit (runsync) when the
                # endpoint has no stream support.
                m = backend["models"][model_key]
                api_env = backend.get("api_key_env") or "RUNPOD_API_KEY"
                tmo = m.get("timeout_s") or cfg.queue["timeout_s"]
                body_input = {
                    "model": m["remote_id"],
                    "messages": body.messages,
                    "stream": True,
                }
                if body.temperature is not None:
                    body_input["temperature"] = body.temperature
                if body.max_tokens:
                    body_input["max_tokens"] = body.max_tokens
                yield _chunk({"role": "assistant", "content": ""})
                finish = "stop"
                try:
                    job_id = await runpod.run_job(m["endpoint_id"],
                                                  body_input, api_env, tmo,
                                                  policy=m.get("policy"))
                    got_any = False
                    async for chunk in runpod.stream_job(
                            m["endpoint_id"], job_id, api_env, tmo):
                        content, done, reason = runpod.ollama_chunk_content(chunk)
                        if content:
                            got_any = True
                            traffic.observe_response(content)
                            yield _chunk({"content": content})
                        if done:
                            finish = reason or "stop"
                    if not got_any:
                        raise runpod.RunPodStreamUnavailable("empty stream")
                    yield _chunk({}, finish=finish)
                except runpod.RunPodStreamUnavailable:
                    # no usable incremental stream — collect-then-emit
                    out = await runpod.runsync_chat(
                        m["endpoint_id"],
                        {k: v for k, v in body_input.items() if k != "stream"},
                        api_env, tmo, policy=m.get("policy"))
                    resp = runpod.ollama_output_to_openai(out, requested)
                    usage_out = resp.get("usage") or usage_out
                    choice = resp["choices"][0]
                    if choice["message"].get("content"):
                        traffic.observe_response(
                            str(choice["message"].get("content") or ""))
                        yield _chunk({"content": choice["message"]["content"]})
                    yield _chunk({}, finish=choice.get("finish_reason") or "stop")
            elif backend.get("type") == "runpod-public":
                # Public Endpoints — collect-then-emit (no incremental
                # stream on public endpoints; always warm so latency low)
                m = backend["models"][model_key]
                api_env = backend.get("api_key_env") or "RUNPOD_API_KEY"
                tmo = m.get("timeout_s") or cfg.queue["timeout_s"]
                body_input = {
                    "model": m.get("remote_id", m["model_slug"]),
                    "messages": body.messages,
                }
                if body.temperature is not None:
                    body_input["temperature"] = body.temperature
                if body.max_tokens:
                    body_input["max_tokens"] = body.max_tokens
                yield _chunk({"role": "assistant", "content": ""})
                out = await runpod.runsync_public_chat(
                    m["model_slug"], body_input, api_env, tmo,
                    policy=m.get("policy"))
                resp = runpod.ollama_output_to_openai(out, requested)
                usage_out = resp.get("usage") or usage_out
                choice = resp["choices"][0]
                if choice["message"].get("content"):
                    traffic.observe_response(
                        str(choice["message"].get("content") or ""))
                    yield _chunk({"content": choice["message"]["content"]})
                yield _chunk({}, finish=choice.get("finish_reason") or "stop")
            else:
                # cloud passthrough — the remote stream is already
                # OpenAI-chunk-shaped (role/finish chunks included);
                # remote llama-server and runpod serverless (openai
                # transport) share this path
                remote_id = backend["models"][model_key]
                client_name = backend_name
                cloud_backend = backend
                stream_timeout = cfg.queue["timeout_s"]
                if backend.get("type") == "runpod":
                    m = backend["models"][model_key]
                    remote_id = m["remote_id"]
                    client_name = f"runpod-{backend_name}-{model_key}"
                    cloud_backend = {
                        "base_url": runpod.serverless_base_url(m["endpoint_id"]),
                        "api_key_env": backend.get("api_key_env") or "RUNPOD_API_KEY",
                    }
                    # per-model timeout covers serverless cold starts
                    stream_timeout = (float(m.get("timeout_s") or 0)
                                      or cfg.queue["timeout_s"])
                client = cloud.get_cloud_client(client_name, cloud_backend,
                                                timeout=stream_timeout)
                if backend.get("type") in ("openai", "azure"):
                    # monthly token budget — 503-shaped mid-stream event
                    await _token_budget_gate(cfg, backend_name)
                if backend.get("type") == "azure":
                    # per-model client (deployment in the URL path)
                    client = cloud.get_azure_client(
                        f"azure-{backend_name}-{model_key}", backend,
                        str(remote_id), timeout=stream_timeout)
                kwargs: dict[str, Any] = {
                    "model": remote_id,
                    "messages": body.messages, "stream": True,
                }
                if backend.get("type") == "openai":
                    # real usage for the charge/log tail (OpenAI spec:
                    # a final usage-only chunk). Other OpenAI-compatible
                    # providers may not accept the param — openai type
                    # only.
                    kwargs["stream_options"] = {"include_usage": True}
                if body.temperature is not None:
                    kwargs["temperature"] = body.temperature
                if body.max_tokens:
                    kwargs["max_tokens"] = cloud.cap_max_tokens(
                        backend, body.max_tokens)
                if body.tools:
                    kwargs["tools"] = body.tools
                try:
                    stream = await client.chat.completions.create(**kwargs)
                except cloud.APIConnectionError:
                    # connect-class failure — one immediate retry (the
                    # request never reached the provider)
                    logger.info("gateway: stream connect failure on %s — "
                                "one retry", backend_name)
                    stream = await client.chat.completions.create(**kwargs)
                async for chunk in stream:
                    u = getattr(chunk, "usage", None)
                    if u:
                        usage_out = (u if isinstance(u, dict)
                                     else (u.model_dump()
                                           if hasattr(u, "model_dump")
                                           else dict(u)))
                    try:
                        cs = getattr(chunk, "choices", None)
                        if cs and getattr(cs[0].delta, "content", None):
                            text_chars += len(cs[0].delta.content)
                            traffic.observe_response(cs[0].delta.content)
                    except Exception:      # noqa: BLE001 — passthrough
                        pass
                    data = (chunk.model_dump_json()
                            if hasattr(chunk, "model_dump_json")
                            else json.dumps(chunk))
                    yield f"data: {data}\n\n"
            yield "data: [DONE]\n\n"
            ms = int((time.perf_counter() - t0) * 1000)
            logger.info("gateway stream ok (%s / %s) in %dms",
                        backend_name, model_key, ms)
            _charge_and_log(ms)
        except lifecycle.LifecycleBudgetExceeded as e:
            ms = int((time.perf_counter() - t0) * 1000)
            yield "data: " + json.dumps({"error": {
                "message": str(e), "type": "overloaded_error",
                "code": "budget_exceeded"}}) + "\n\n"
            yield "data: [DONE]\n\n"
            _log_request(requested, "", True, ms, {}, 503)
        except Exception as e:
            ms = int((time.perf_counter() - t0) * 1000)
            kind = cloud.classify_error(e)
            logger.warning("gateway stream failed (%s / %s, %s): %s",
                           backend_name, model_key, kind, e)
            # Mid-flight errors: SSE has no status code to change — the
            # error event carries the class so clients can react.
            yield "data: " + json.dumps(
                {"error": {"message": str(e), "type": "server_error",
                           "code": kind or "execution_error"}}) + "\n\n"
            yield "data: [DONE]\n\n"
            _log_request(requested, backend_name, True, ms, {}, 502, err=kind)
        finally:
            _release_chat_slot(slot)

    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# ── POST /v1/embeddings ────────────────────────────────────────────────────

class EmbeddingRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    model: str = ""
    input: str | list[str] = Field(min_length=1)
    encoding_format: str = "float"
    dimensions: int | None = None


def _b64_embeddings(data: list[dict[str, Any]]) -> None:
    """Encode each embedding in-place to base64 little-endian float32
    (OpenAI's base64 encoding_format)."""
    for d in data:
        vec = d.get("embedding") or []
        d["embedding"] = base64.b64encode(
            struct.pack(f"<{len(vec)}f", *vec)).decode("ascii")


@app.post("/v1/embeddings")
async def embeddings(body: EmbeddingRequest) -> Response:
    """OpenAI embeddings — embedded GGUFs (config-defined, separate
    pooling instances) or cloud passthrough (remote decides the model)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    t0 = time.perf_counter()
    requested = body.model.strip()

    fmt = (body.encoding_format or "float").strip().lower()
    if fmt not in ("float", "base64"):
        return _error(400, "encoding_format must be 'float' or 'base64'",
                      param="encoding_format")

    try:
        backend_name, model_key = cfg.resolve_embedding(requested)
    except ProviderNotLicensedError:
        # v2.2 don't-display: indistinguishable from unknown model
        _log_request(requested, "", False,
                     int((time.perf_counter() - t0) * 1000), {}, 400,
                     err="license")
        return _error(400, f"unknown embedding model: {requested!r} "
                      "(define it under backends.<local>.embedding_models "
                      "or a cloud backend)", param="model",
                      code="model_not_found")
    except GatewayConfigError as e:
        _log_request(requested, "", False,
                     int((time.perf_counter() - t0) * 1000), {}, 400)
        return _error(400, str(e), param="model", code="model_not_found")

    inputs: list[str] = ([body.input] if isinstance(body.input, str)
                         else list(body.input))
    if not any(s.strip() for s in inputs):
        return _error(400, "input must not be empty", param="input")

    # Same admission gate as chat — embedding requests serialize on their
    # instance lock, so an unbounded pile-up would starve everything else.
    engine = get_engine()
    _reap_stale_chat_slots(cfg)
    capacity = (max(1, len(engine.loaded_models()))
                + llamaserver.running_count() + cfg.queue["max_waiting"])
    if len(_chat_slots) >= capacity:
        _log_request(requested, "", False,
                     int((time.perf_counter() - t0) * 1000), {}, 429)
        return _error(429, "gateway busy — too many concurrent requests, "
                      "retry shortly", err_type="rate_limit_error",
                      code="rate_limited", headers={"Retry-After": "1"})

    slot = _acquire_chat_slot()
    try:
        backend = cfg.backends[backend_name]
        if backend.get("type") == "embedded":
            if body.dimensions is not None:
                # llama.cpp has no MRL-style truncation — the vector
                # dimension is fixed by the model.
                logger.warning("dimensions=%s ignored for embedded "
                               "embedding model %r", body.dimensions, requested)
            resp = await engine.embed(model_key, inputs)
        else:
            client = cloud.get_cloud_client(backend_name, backend,
                                            timeout=cfg.queue["timeout_s"])
            kwargs: dict[str, Any] = {
                "model": backend["models"][model_key], "input": inputs}
            if body.dimensions is not None:
                kwargs["dimensions"] = body.dimensions
            try:
                r = await client.embeddings.create(**kwargs)
            except Exception as e:
                raise cloud.CloudBackendError(
                    f"cloud backend {backend_name!r} embedding failed: {e}") from e
            resp = (json.loads(r.model_dump_json())
                    if hasattr(r, "model_dump_json") else dict(r))
        data = resp.get("data") or []
        if fmt == "base64":
            _b64_embeddings(data)
        out = {
            "object": "list",
            "data": data,
            "model": requested,  # echo what the client asked for
            "usage": resp.get("usage") or {},
        }
        ms = int((time.perf_counter() - t0) * 1000)
        _log_request(requested, backend_name, False, ms,
                     out["usage"], 200)
        return JSONResponse(out)
    except Exception as e:
        ms = int((time.perf_counter() - t0) * 1000)
        logger.warning("gateway embedding failed (%s): %s", requested, e)
        _log_request(requested, "", False, ms, {}, 502)
        return _error(502, str(e), err_type="server_error",
                      code="execution_error")
    finally:
        _release_chat_slot(slot)


# ── POST /v1/completions (legacy text completions) ─────────────────────────

class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    model: str = ""
    prompt: str | list[str] = Field(min_length=1)
    stream: bool = False
    temperature: float | None = None
    max_tokens: int | None = None


@app.post("/v1/completions")
async def legacy_completions(body: CompletionRequest) -> Response:
    """Legacy text completions — raw prompt in, text out (no chat
    template, no tools, no think-stripping). Every backend type serves it:
    embedded via llama_cpp create_completion (worker pools), llama-server
    subprocess/pods via the child's native /v1/completions, runpod
    serverless runsync via the worker's generate mode ({"prompt": ...}),
    and cloud/remote via client.completions passthrough. Executes the
    PRIMARY chain entry only (no hedge)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    t0 = time.perf_counter()

    requested = body.model.strip() or cfg.default_alias
    try:
        plan = cfg.resolve_chain(requested)
    except ProviderNotLicensedError:
        # v2.2 don't-display: indistinguishable from unknown model
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 400,
                     err="license")
        return _error(400, f"unknown model: {requested!r} (not an alias "
                      "or a backend model)", param="model",
                      code="model_not_found")
    except GatewayConfigError as e:
        plan = _smart_route_fallback(cfg, requested)
        if (plan is None and requested
                and EmbeddedEngine.resolve_gguf(requested, cfg.models_dir)):
            plan = {"entries": [(cfg.local_backend_name() or "local", requested)],
                    "hedge_s": 0.0, "local_only": False}
        if plan is None:
            _log_request(requested, "", body.stream,
                         int((time.perf_counter() - t0) * 1000), {}, 400)
            return _error(400, str(e), param="model", code="model_not_found")
    backend_name, model_key = plan["entries"][0]
    backend = cfg.backends[backend_name]
    btype = backend.get("type")

    # Per-key model allowlist
    denied = _key_model_gate(cfg, requested, plan["entries"])
    if denied is not None:
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 403)
        return denied

    # Request guardrails (completions: prompt is raw text)
    blocked = apply_request_guardrails(
        cfg, requested, [{"role": "user", "content": body.prompt}
                         if isinstance(body.prompt, str)
                         else {"role": "user",
                               "content": " ".join(body.prompt)}])
    if blocked is not None:
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 400,
                     err="guardrail")
        return blocked

    # Per-key monthly budgets (tokens + USD)
    over = _key_budget_gate(cfg)
    if over is not None:
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 429,
                     err="key_budget")
        return over

    engine = get_engine()
    _reap_stale_chat_slots(cfg)
    capacity = (max(1, len(engine.loaded_models()))
                + llamaserver.running_count() + cfg.queue["max_waiting"])
    if len(_chat_slots) >= capacity:
        _log_request(requested, "", body.stream,
                     int((time.perf_counter() - t0) * 1000), {}, 429)
        return _error(429, "gateway busy — too many concurrent requests, "
                      "retry shortly", err_type="rate_limit_error",
                      code="rate_limited", headers={"Retry-After": "1"})

    prompts: list[str] = ([body.prompt] if isinstance(body.prompt, str)
                          else list(body.prompt))
    if not any(p.strip() for p in prompts):
        return _error(400, "prompt must not be empty", param="prompt")

    slot = _acquire_chat_slot()
    try:
        # embedded — llama_cpp raw completion (worker pool checkout)
        if btype == "embedded":
            if body.stream:
                async def _gen() -> AsyncIterator[str]:
                    chat_id = f"cmpl-gw-{uuid.uuid4().hex[:12]}"
                    created = int(time.time())

                    def _chunk(delta: dict[str, Any], finish: str | None = None) -> str:
                        return f"data: {json.dumps({'id': chat_id, 'object': 'text.completion.chunk', 'created': created, 'model': requested, 'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]})}\n\n"

                    try:
                        yield _chunk({"role": "assistant", "content": ""})
                        stream = await engine.complete(
                            model_key, prompts[0], stream=True,
                            temperature=body.temperature,
                            max_tokens=body.max_tokens)
                        async for ch in stream:
                            text = (ch.get("choices") or [{}])[0].get("text")
                            if text:
                                yield _chunk({"content": text})
                        yield _chunk({}, finish="stop")
                        yield "data: [DONE]\n\n"
                        ms = int((time.perf_counter() - t0) * 1000)
                        _log_request(requested, backend_name, True, ms, {}, 200)
                    except Exception as e:
                        yield "data: " + json.dumps(
                            {"error": {"message": str(e), "type": "server_error",
                                       "code": "execution_error"}}) + "\n\n"
                        yield "data: [DONE]\n\n"
                        _log_request(requested, backend_name, True,
                                     int((time.perf_counter() - t0) * 1000), {}, 502)
                    finally:
                        _release_chat_slot(slot)
                return StreamingResponse(_gen(), media_type="text/event-stream",
                                         headers={"Cache-Control": "no-cache",
                                                  "X-Accel-Buffering": "no"})
            resp = await engine.complete(
                model_key, prompts[0], temperature=body.temperature,
                max_tokens=body.max_tokens)
            d = dict(resp)
            d["model"] = requested
            ms = int((time.perf_counter() - t0) * 1000)
            _log_request(requested, backend_name, False, ms,
                         d.get("usage") or {}, 200)
            return JSONResponse(d)

        # llama-server subprocess / runpod pod — native /v1/completions
        if btype == "llama-server" and not backend.get("base_url"):
            llamaserver.ensure_reaper()
            mgr = llamaserver.get_manager(cfg, backend_name, model_key)
            base = await mgr.ensure_ready()
            # Trim the raw prompt to the slot context (head+tail when
            # oversized) — llama-server 400s on overflow.
            prompt_text = await asyncio.to_thread(
                llamaserver.fit_prompt, base, prompts[0], body.max_tokens,
                mgr.ctx, mgr.parallel, f"{backend_name}/{model_key}")
            payload = {"model": mgr.alias, "prompt": prompt_text}
            if body.temperature is not None:
                payload["temperature"] = body.temperature
            if body.max_tokens:
                payload["max_tokens"] = body.max_tokens
            if body.stream:
                async def _gen_ls() -> AsyncIterator[str]:
                    try:
                        payload_s = {**payload, "stream": True}
                        async for data in llamaserver.stream_completions(
                                base, payload_s, cfg.queue["timeout_s"]):
                            if data == "[DONE]":
                                break
                            yield f"data: {data}\n\n"
                        yield "data: [DONE]\n\n"
                        _log_request(requested, backend_name, True,
                                     int((time.perf_counter() - t0) * 1000), {}, 200)
                    except Exception as e:
                        yield "data: " + json.dumps(
                            {"error": {"message": str(e), "type": "server_error",
                                       "code": "execution_error"}}) + "\n\n"
                        yield "data: [DONE]\n\n"
                        _log_request(requested, backend_name, True,
                                     int((time.perf_counter() - t0) * 1000), {}, 502)
                    finally:
                        _release_chat_slot(slot)
                return StreamingResponse(_gen_ls(), media_type="text/event-stream",
                                         headers={"Cache-Control": "no-cache",
                                                  "X-Accel-Buffering": "no"})
            resp = await llamaserver.post_completions(
                base, payload, cfg.queue["timeout_s"])
            resp["model"] = requested
            ms = int((time.perf_counter() - t0) * 1000)
            _log_request(requested, backend_name, False, ms,
                         resp.get("usage") or {}, 200)
            return JSONResponse(resp)

        if btype == "runpod" and not backend["models"][model_key].get("endpoint_id"):
            mgr = runpod.get_manager(cfg, backend_name, model_key)
            try:
                await _budget_gate(cfg, backend_name, model_key)
            except lifecycle.LifecycleBudgetExceeded as e:
                return _error(503, str(e), err_type="overloaded_error",
                              code="budget_exceeded")
            async with mgr.request():  # v3: inflight guard
                base = await mgr.ensure_ready()
                payload = {"model": mgr.spec.get("alias", model_key),
                           "prompt": prompts[0]}
                if body.temperature is not None:
                    payload["temperature"] = body.temperature
                if body.max_tokens:
                    payload["max_tokens"] = body.max_tokens
                resp = await llamaserver.post_completions(
                    base, payload, cfg.queue["timeout_s"])
            resp["model"] = requested
            ms = int((time.perf_counter() - t0) * 1000)
            _log_request(requested, backend_name, False, ms,
                         resp.get("usage") or {}, 200)
            return JSONResponse(resp)

        # runpod serverless runsync — worker generate mode
        if btype == "runpod" and backend["models"][model_key].get("transport") == "runsync":
            m = backend["models"][model_key]
            body_input: dict[str, Any] = {"model": m["remote_id"],
                                         "prompt": prompts[0]}
            if body.temperature is not None:
                body_input["temperature"] = body.temperature
            if body.max_tokens:
                # Ollama generate caps via options.num_predict (the worker
                # ignores bare max_tokens — live-verified)
                body_input["options"] = {"num_predict": body.max_tokens}
            out = await runpod.runsync_chat(
                m["endpoint_id"], body_input,
                backend.get("api_key_env") or "RUNPOD_API_KEY",
                m.get("timeout_s") or cfg.queue["timeout_s"],
                policy=m.get("policy"))
            resp = runpod.ollama_generate_output_to_openai(out, requested)
            ms = int((time.perf_counter() - t0) * 1000)
            _log_request(requested, backend_name, False, ms,
                         resp.get("usage") or {}, 200)
            return JSONResponse(resp)

        # runpod public endpoints — runsync generate mode
        if btype == "runpod-public":
            m = backend["models"][model_key]
            body_input: dict[str, Any] = {
                "model": m.get("remote_id", m["model_slug"]),
                "prompt": prompts[0],
            }
            if body.temperature is not None:
                body_input["temperature"] = body.temperature
            if body.max_tokens:
                body_input["options"] = {"num_predict": body.max_tokens}
            out = await runpod.runsync_public_chat(
                m["model_slug"], body_input,
                backend.get("api_key_env") or "RUNPOD_API_KEY",
                m.get("timeout_s") or cfg.queue["timeout_s"],
                policy=m.get("policy"))
            resp = runpod.ollama_generate_output_to_openai(out, requested)
            ms = int((time.perf_counter() - t0) * 1000)
            _log_request(requested, backend_name, False, ms,
                         resp.get("usage") or {}, 200)
            return JSONResponse(resp)

        # cloud-shaped passthrough: openai backends, remote llama-server,
        # runpod serverless (openai transport)
        remote_id = backend["models"][model_key]
        client_name = backend_name
        cloud_backend = backend
        if btype == "runpod":
            m = backend["models"][model_key]
            remote_id = m["remote_id"]
            client_name = f"runpod-{backend_name}-{model_key}"
            cloud_backend = {
                "base_url": runpod.serverless_base_url(m["endpoint_id"]),
                "api_key_env": backend.get("api_key_env") or "RUNPOD_API_KEY",
            }
        # v4.5: per-model timeout covers serverless cold starts
        client_timeout = (float(m.get("timeout_s") or 0)
                          if btype == "runpod" else 0.0) \
            or cfg.queue["timeout_s"]
        if btype == "openai":
            await _token_budget_gate(cfg, backend_name)
        client = cloud.get_cloud_client(client_name, cloud_backend,
                                        timeout=client_timeout)
        kwargs: dict[str, Any] = {"model": remote_id, "prompt": body.prompt}
        if body.temperature is not None:
            kwargs["temperature"] = body.temperature
        if body.max_tokens:
            kwargs["max_tokens"] = cloud.cap_max_tokens(backend, body.max_tokens)
        try:
            resp = await client.completions.create(**kwargs)
        except cloud.APIConnectionError as e:
            # connect-class failure — one immediate retry (same model)
            logger.info("gateway: completion connect failure on %s (%s) — "
                        "one retry", backend_name, str(e)[:120])
            try:
                resp = await client.completions.create(**kwargs)
            except Exception as e2:
                raise cloud.CloudBackendError(
                    f"cloud backend {backend_name!r} completion failed: {e2}") from e2
        except Exception as e:
            raise cloud.CloudBackendError(
                f"cloud backend {backend_name!r} completion failed: {e}") from e
        d = (json.loads(resp.model_dump_json())
             if hasattr(resp, "model_dump_json") else dict(resp))
        d["model"] = requested
        usage = d.get("usage") or {}
        cost = None
        if btype == "openai":
            cost = _cost_usd(cfg, backend_name, str(remote_id), usage)
            if cost is not None:
                d["cost_usd"] = round(cost, 6)
            _charge_request(cfg, backend_name, usage, cost)
        else:
            _charge_request(cfg, backend_name, usage, None)
        ms = int((time.perf_counter() - t0) * 1000)
        _log_request(requested, backend_name, False, ms,
                     usage, 200, cost_usd=cost)
        return JSONResponse(d)
    except lifecycle.LifecycleBudgetExceeded as e:
        ms = int((time.perf_counter() - t0) * 1000)
        _log_request(requested, "", body.stream, ms, {}, 503)
        return _error(503, str(e), err_type="overloaded_error",
                      code="budget_exceeded")
    except Exception as e:
        return _exec_error_response(e, requested, body.stream, t0)
    finally:
        _release_chat_slot(slot)


# ── POST /v1/images/generations (openai-backend passthrough) ───────────────

async def _passthrough_images(base_url: str, api_key: str,
                              payload: dict) -> dict[str, Any]:
    """POST {base_url}/images/generations, verbatim JSON (a separate
    function so tests can monkeypatch it)."""
    async with httpx.AsyncClient(timeout=120.0) as hc:
        r = await hc.post(f"{base_url.rstrip('/')}/images/generations",
                          headers={"Authorization": f"Bearer {api_key}"},
                          json=payload)
        r.raise_for_status()
        return r.json()


@app.post("/v1/images/generations")
async def images_generations(request: Request) -> Response:
    """OpenAI images passthrough for openai-type cloud backends (the wire
    model id maps through backend models like chat). NOTE: the HF
    router's OpenAI-compatible surface is chat-only — point image models
    at a provider that serves /images/generations (OpenAI, Together,
    fal, ...)."""
    try:
        body = await request.json()
    except Exception:
        return _error(400, "invalid JSON body")
    requested = str(body.get("model") or "").strip()
    if not requested:
        return _error(400, "model is required", param="model")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    try:
        plan = cfg.resolve_chain(requested)
    except ProviderNotLicensedError:
        # v2.2 don't-display: indistinguishable from unknown model
        _log_request(requested, "", False, 0, {}, 400, err="license")
        return _error(400, f"unknown model: {requested!r} (not an alias "
                      "or a backend model)", param="model",
                      code="model_not_found")
    except GatewayConfigError as e:
        _log_request(requested, "", False, 0, {}, 400)
        return _error(400, str(e), param="model", code="model_not_found")
    backend_name, model_key = plan["entries"][0]
    backend = cfg.backends.get(backend_name) or {}
    if backend.get("type") != "openai":
        _log_request(requested, "", False, 0, {}, 400)
        return _error(400, f"backend {backend_name!r} does not serve images "
                           f"(openai-type cloud backends only)",
                      param="model", code="images_unsupported")
    denied = _key_model_gate(cfg, requested, plan["entries"])
    if denied is not None:
        return denied
    key_env = str(backend.get("api_key_env") or "").strip()
    api_key = cloud.resolve_backend_key(backend)
    if key_env and not api_key:
        return _error(502, f"api key env var {key_env!r} is not set and the "
                           f"OS keyring has no '{key_env}' entry",
                      code="auth")
    wire = str((backend.get("models") or {}).get(model_key) or model_key)
    payload = {k: v for k, v in body.items() if k != "model"}
    payload["model"] = wire
    t0 = time.perf_counter()
    try:
        data = await _passthrough_images(str(backend.get("base_url") or ""),
                                         api_key, payload)
    except httpx.HTTPStatusError as e:
        return _exec_error_response(e, requested, False, t0)
    except httpx.RequestError as e:
        # GL-R15: class only to the client; the URL-bearing detail goes
        # to the operator log
        logger.warning("images passthrough failure: %s", e)
        return _error(502, "images request failed", code="network")
    _log_request(requested, backend_name, False,
                 int((time.perf_counter() - t0) * 1000), {}, 200)
    return JSONResponse(data)


# ── tier E: guardrails ─────────────────────────────────────────────────────

# Violation counters ride the storage backend (shared across replicas).
_GUARDRAIL_VIOLATIONS = NamespaceDict("guardrail_violations")

# Built-in prompt-injection heuristics (v1: pattern-based; the webhook
# policy engine handles anything deeper)
_INJECTION_PATTERNS = [
    r"(?i)ignore\s+(all\s+)?(previous|prior|above|earlier)\s+"
    r"(instructions|prompts?|rules|messages)",
    r"(?i)disregard\s+(the\s+)?(above|previous|prior|earlier)",
    r"(?i)forget\s+(everything|all|your)\s+"
    r"(above|previous|prior|instructions|training)",
    r"(?i)you\s+are\s+now\s+(a|an)\s+",
    r"(?i)(reveal|show|print|repeat|output)\s+(your|the)\s+"
    r"(system\s+)?(prompt|instructions|rules)",
    r"(?i)(developer|DAN)\s+mode|jailbreak",
    r"<\|?(im_start|im_end|system)\|>",
]

_GUARDRAIL_COMPILED: dict[str, "re.Pattern"] = {}


def _guardrail_re(pattern: str):
    if pattern not in _GUARDRAIL_COMPILED:
        try:
            _GUARDRAIL_COMPILED[pattern] = re.compile(pattern)
        except re.error:
            return None
    return _GUARDRAIL_COMPILED[pattern]


def reset_guardrail_state() -> None:
    """Drop violation counters (tests)."""
    _GUARDRAIL_VIOLATIONS.clear()


def _rule_violated(rule: dict, text: str) -> str | None:
    """Check one rule against text; returns the violation reason or None."""
    rtype = rule.get("type")
    if rtype == "pattern":
        rx = _guardrail_re(rule.get("pattern") or "")
        if rx and rx.search(text):
            return rule.get("message") or f"matched pattern {rule['name']!r}"
    elif rtype == "prompt_injection":
        for pat in _INJECTION_PATTERNS:
            rx = _guardrail_re(pat)
            if rx and rx.search(text):
                return (rule.get("message")
                        or f"prompt-injection pattern matched: {pat[:50]}")
    elif rtype == "json":
        try:
            json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return rule.get("message") or "response is not valid JSON"
    return None


def _messages_text(messages: list) -> str:
    """Concatenate all text content of a message list (for scanning)."""
    parts: list[str] = []
    for m in messages or []:
        if isinstance(m, dict):
            c = m.get("content")
            if isinstance(c, str):
                parts.append(c)
            elif isinstance(c, list):
                for b in c:
                    if isinstance(b, dict) and isinstance(b.get("text"), str):
                        parts.append(b["text"])
    return "\n".join(parts)


def _guardrail_hit(rule_name: str) -> None:
    _GUARDRAIL_VIOLATIONS[rule_name] = _GUARDRAIL_VIOLATIONS.get(rule_name, 0) + 1
    _metric_inc("gateway_guardrail_blocks_total", (rule_name,))


def apply_request_guardrails(cfg, model: str,
                             messages: list) -> Response | None:
    """Pre-execution guardrails: request rules over the concatenated
    message text + the external policy webhook. Returns a 400 response
    when a block-action rule fires; None when the request may proceed.
    Runs BEFORE the response cache (a blocked prompt never serves a
    cached answer)."""
    g = cfg.guardrails or {}
    if not g.get("enabled"):
        return None
    text = _messages_text(messages)
    for rule in g.get("request_rules") or []:
        why = _rule_violated(rule, text)
        if why:
            _guardrail_hit(rule["name"])
            if rule.get("action") == "block":
                logger.warning("guardrail %s blocked request: %s",
                               rule["name"], why[:120])
                return _error(400, f"guardrail {rule['name']!r}: {why}",
                              err_type="guardrail_violation",
                              code=rule["name"])
            # log/redact actions on REQUESTS = observe only
    url = g.get("webhook_url")
    if url:
        payload = {"direction": "request", "model": model,
                   "key": _REQUEST_KEY_ID.get() or "(open)",
                   "text": text[:8000]}
        verdict = _guardrail_webhook(url, g, payload)
        if verdict is not None and not verdict[0]:
            _guardrail_hit("webhook")
            logger.warning("guardrail webhook blocked request: %s",
                           verdict[1][:120])
            return _error(400, f"guardrail webhook: {verdict[1]}",
                          err_type="guardrail_violation", code="webhook")
    return None


def apply_response_guardrails(cfg, content: str) -> tuple[str, Response | None]:
    """Post-generation guardrails: response rules over the reply text.
    Returns (possibly redacted content, error-response-or-None). Block
    rules return a 400 — the generation is discarded (that is the point).
    Only applied to NON-STREAMING responses (streams are not buffered)."""
    g = cfg.guardrails or {}
    if not g.get("enabled"):
        return content, None
    for rule in g.get("response_rules") or []:
        why = _rule_violated(rule, content)
        if why:
            _guardrail_hit(rule["name"])
            action = rule.get("action")
            if action == "redact" and rule.get("type") == "pattern":
                rx = _guardrail_re(rule.get("pattern") or "")
                if rx:
                    content = rx.sub(rule.get("replacement") or "[REDACTED]",
                                     content)
                    logger.info("guardrail %s redacted response content",
                                rule["name"])
            elif action == "block":
                logger.warning("guardrail %s blocked response: %s",
                               rule["name"], why[:120])
                return content, _error(
                    400, f"guardrail {rule['name']!r}: {why}",
                    err_type="guardrail_violation", code=rule["name"])
            # log action: observed only
    url = g.get("webhook_url")
    if url:
        payload = {"direction": "response",
                   "key": _REQUEST_KEY_ID.get() or "(open)",
                   "text": content[:8000]}
        verdict = _guardrail_webhook(url, g, payload)
        if verdict is not None and not verdict[0]:
            _guardrail_hit("webhook")
            return content, _error(
                400, f"guardrail webhook: {verdict[1]}",
                err_type="guardrail_violation", code="webhook")
    return content, None


def _guardrail_webhook(url: str, g: dict,
                       payload: dict) -> tuple[bool, str] | None:
    """Call the external policy engine. Returns (allow, reason) or None
    on failure (fail-open) / disabled. Sync httpx — guardrail calls sit
    in the request path and must be fast."""
    try:
        r = httpx.post(url, json=payload,
                       timeout=float(g.get("webhook_timeout_s") or 10))
        if r.status_code == 200:
            d = r.json()
            return (bool(d.get("allow", True)),
                    str(d.get("reason") or "denied by policy"))
        return None if g.get("webhook_fail_open", True) else (False, f"webhook HTTP {r.status_code}")
    except Exception:  # noqa: BLE001 — fail-open per config
        if g.get("webhook_fail_open", True):
            logger.warning("guardrail webhook unreachable — fail-open",
                           exc_info=True)
            return None
        return (False, "webhook unreachable (fail-closed)")


@app.get("/admin/guardrails")
async def admin_guardrails() -> Response:
    """Guardrail configuration + live violation counts."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    return {"guardrails": cfg.guardrails,
            "violations": dict(_GUARDRAIL_VIOLATIONS)}


# ── tier E: smart routing ──────────────────────────────────────────────────

# Routing trackers ride the storage backend (latency EWMA + selection
# counts are shared across replicas; round-robin counters become a
# shared turnstile).
_ROUTE_LATENCY = NamespaceDict("route_latency")    # "b:m" → {ewma_ms, n}
_ROUTE_SELECTIONS = NamespaceDict("route_selections")  # route → {b:m: count}
_ROUTE_RR = NamespaceDict("route_rr")              # round-robin counters


def reset_route_state() -> None:
    """Drop routing trackers (tests)."""
    _ROUTE_LATENCY.clear()
    _ROUTE_SELECTIONS.clear()
    _ROUTE_RR.clear()


def _record_route_latency(backend: str, model: str, ms: float) -> None:
    """EWMA latency per backend:model — feeds the 'fastest' strategy.
    Read-modify-write (backend-visible, never in-place mutation)."""
    key = f"{backend}:{model}"
    st = _ROUTE_LATENCY.get(key) or {"ewma_ms": ms, "n": 0}
    _ROUTE_LATENCY[key] = {
        "ewma_ms": float(st.get("ewma_ms") or 0.0) * 0.7 + ms * 0.3,
        "n": int(st.get("n") or 0) + 1,
    }


def _candidate_price(cfg, cand: dict) -> float:
    """Cheapest-strategy score: avg in+out $/Mtok from cached pricing."""
    b = cfg.backends.get(cand.get("backend")) or {}
    wire = str((b.get("models") or {}).get(cand.get("model"))
               or cand.get("model") or "")
    p = (cfg.model_prices or {}).get(cand.get("backend"), {}).get(wire)
    if not p:
        return float("inf")
    return (float(p.get("in") or 0) + float(p.get("out") or 0)) / 2


def _resolve_smart_route(cfg, name: str) -> dict | None:
    """Smart route name → chosen candidate {backend, model, weight}.
    Aliases and backend models always win (checked by the caller first).
    Returns None when the name is not a smart route."""
    route = (getattr(cfg, "smart_routes", None) or {}).get(name)
    if not route:
        return None
    candidates = route.get("candidates") or []
    if not candidates:
        return None
    strategy = route.get("strategy") or "round_robin"
    if strategy == "cheapest":
        priced = [c for c in candidates if _candidate_price(cfg, c) < float("inf")]
        pool = priced or candidates
        pick = min(pool, key=lambda c: _candidate_price(cfg, c))
    elif strategy == "fastest":
        if not any(f"{c['backend']}:{c['model']}" in _ROUTE_LATENCY
                   for c in candidates):
            pick = candidates[0]  # cold start — explore the first
        else:
            pick = min(candidates, key=lambda c: _ROUTE_LATENCY.get(
                f"{c['backend']}:{c['model']}", {}).get("ewma_ms", float("inf")))
    elif strategy in ("weighted", "canary"):
        weights = [max(0.0, float(c.get("weight") or 1.0))
                   for c in candidates]
        pick = random.choices(candidates, weights=weights, k=1)[0]
    else:  # round_robin
        i = _ROUTE_RR.get(name, 0)
        pick = candidates[i % len(candidates)]
        _ROUTE_RR[name] = i + 1
    key = f"{pick.get('backend')}:{pick.get('model')}"
    sel = _ROUTE_SELECTIONS.get(name) or {}
    sel[key] = int(sel.get(key) or 0) + 1
    _ROUTE_SELECTIONS[name] = sel
    return pick


def _smart_route_fallback(cfg, requested: str) -> dict | None:
    """Build a single-entry plan when `requested` names a smart route."""
    pick = _resolve_smart_route(cfg, requested)
    if pick is None:
        return None
    return {"entries": [(str(pick["backend"]), str(pick["model"]))],
            "hedge_s": 0.0, "local_only": False}


@app.get("/admin/routes")
async def admin_routes() -> Response:
    """Smart-route config + live stats (latency EWMA, selection counts)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    routes = []
    for name, r in (cfg.smart_routes or {}).items():
        cands = []
        for c in r.get("candidates") or []:
            key = f"{c['backend']}:{c['model']}"
            cands.append({
                "backend": c["backend"], "model": c["model"],
                "weight": c.get("weight", 1.0),
                "ewma_ms": round(_ROUTE_LATENCY.get(key, {}).get("ewma_ms", 0)),
                "selections": _ROUTE_SELECTIONS.get(name, {}).get(key, 0),
            })
        routes.append({"name": name, "strategy": r.get("strategy"),
                       "candidates": cands})
    return {"routes": routes}


# ── tier E: shared internal executor (batch + evals) ──────────────────────

async def _exec_chat_internal(cfg, model: str, messages: list,
                              timeout_s: float = 300.0) -> dict[str, Any]:
    """One chat completion through the full internal pipeline: smart
    routes, request guardrails, chain execution (with backend budgets,
    cost tracking, key attribution via the ambient ContextVar), response
    guardrails. Raises on failure — callers decide how to record it.
    Used by the Batch API and the eval harness; the HTTP endpoints have
    their own richer paths (cache, streaming, queue admission)."""
    plan = None
    try:
        plan = cfg.resolve_chain(model)
    except ProviderNotLicensedError:
        # v2.2 don't-display: re-raise as a plain unknown-model error
        # so callers' generic handling stays byte-identical (no
        # license signal, no existence leak)
        raise GatewayConfigError(f"unknown model: {model!r}") from None
    except GatewayConfigError:
        plan = _smart_route_fallback(cfg, model)
    if plan is None:
        raise GatewayConfigError(f"unknown model {model!r}")
    blocked = apply_request_guardrails(cfg, model, messages)
    if blocked is not None:
        raise _GuardrailBlocked(str(blocked.body, "utf-8", "replace")
                                if isinstance(blocked.body, bytes)
                                else str(blocked.body))
    body = ChatRequest(model=model, messages=messages)
    resp, winner, model_key = await asyncio.wait_for(
        _exec_chain(cfg, plan["entries"], plan["hedge_s"], body, model),
        timeout=timeout_s)
    content = str((((resp.get("choices") or [{}])[0])
                   .get("message") or {}).get("content") or "")
    content, blocked = apply_response_guardrails(cfg, content)
    if blocked is not None:
        raise _GuardrailBlocked("response guardrail")
    if content:
        resp["choices"][0]["message"]["content"] = content
    return resp


class _GuardrailBlocked(Exception):
    """Raised by _exec_chat_internal when a guardrule blocks (the JSON
    body of the 400 is the message)."""


# ── tier E: Batch API ──────────────────────────────────────────────────────
# POST /v1/batches {model, requests: [{custom_id, messages}]} → async
# execution at concurrency 4 through the full pipeline (auth context,
# budgets, cost tracking, guardrails, response cache all apply).
# Results: data/batch-<id>.jsonl; manifest in memory + on disk.

# Batch registry rides the storage backend (a shared backend shows the
# batch list cluster-wide; results JSONL + the manifest still persist
# under data/).
_BATCHES = NamespaceDict("batches")
_BATCH_CONCURRENCY = 4
_BATCHES_MANIFEST = None  # lazily data/batches.json


def reset_batch_state() -> None:
    """Drop in-memory batch registry (tests)."""
    _BATCHES.clear()


def _batches_manifest_path() -> Path:
    return _LOG_PATH.parent / "batches.json"


def _batch_save_manifest() -> None:
    try:
        p = _batches_manifest_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        keep = sorted(_BATCHES.values(),
                      key=lambda b: b.get("created", ""), reverse=True)[:50]
        p.write_text(json.dumps(keep, indent=1), encoding="utf-8")
    except OSError:
        logger.warning("batch manifest write failed", exc_info=True)


async def _run_batch(batch_id: str, model: str,
                     requests: list[dict]) -> None:
    sem = asyncio.Semaphore(_BATCH_CONCURRENCY)
    cfg = get_gateway_config()

    async def _one(req: dict, idx: int) -> dict:
        cid = str(req.get("custom_id") or f"req-{idx}")
        async with sem:
            try:
                resp = await _exec_chat_internal(
                    cfg, model, req.get("messages") or [])
                return {"custom_id": cid, "status": "completed",
                        "response": resp}
            except _GuardrailBlocked as e:
                return {"custom_id": cid, "status": "failed",
                        "error": {"message": f"guardrail: {e}",
                                  "code": "guardrail_violation"}}
            except Exception as e:  # noqa: BLE001 — one bad request
                kind = (cloud.classify_error(e)
                        if not isinstance(e, GatewayConfigError) else "client")
                # GL-R15: classification only in the batch result —
                # exception strings can carry internal endpoints
                return {"custom_id": cid, "status": "failed",
                        "error": {"message": f"upstream {kind}",
                                  "code": kind}}

    results = await asyncio.gather(*[_one(r, i)
                                     for i, r in enumerate(requests)])
    # results file
    try:
        p = _LOG_PATH.parent / f"batch-{batch_id}.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
    except OSError:
        logger.warning("batch results write failed", exc_info=True)
    m = _BATCHES.get(batch_id)
    if m:
        m = {**m,
             "status": "completed",
             "completed": sum(1 for r in results if r["status"] == "completed"),
             "failed": sum(1 for r in results if r["status"] == "failed"),
             "finished": time.strftime("%Y-%m-%dT%H:%M:%S")}
        _BATCHES[batch_id] = m
        _batch_save_manifest()
    logger.info("batch %s done: %d ok, %d failed", batch_id,
                m["completed"] if m else 0, m["failed"] if m else 0)


@app.post("/v1/batches")
async def create_batch(request: Request) -> Response:
    """Submit an async batch: {model, requests: [{custom_id, messages}]}.
    Sub-requests execute at concurrency 4 through the FULL pipeline
    (this key's identity, budgets, allowlists, guardrails, cost
    tracking). Poll GET /v1/batches/{id}; results at .../results."""
    try:
        body = await request.json()
    except Exception:
        return _error(400, "invalid JSON body")
    model = str(body.get("model") or "").strip()
    requests = body.get("requests")
    if not model:
        return _error(400, "model is required", param="model")
    if not isinstance(requests, list) or not requests:
        return _error(400, "requests must be a non-empty array",
                      param="requests")
    if len(requests) > 500:
        return _error(400, "max 500 requests per batch", param="requests")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    # resolve model (chain or smart route) + allowlist check up front
    plan = None
    try:
        plan = cfg.resolve_chain(model)
    except ProviderNotLicensedError:
        # v2.2 don't-display: indistinguishable from unknown model
        _log_request(model, "", False, 0, {}, 400, err="license")
        return _error(400, f"unknown model: {model!r} (not an alias or "
                      "a backend model)", param="model",
                      code="model_not_found")
    except GatewayConfigError:
        plan = _smart_route_fallback(cfg, model)
    if plan is None:
        _log_request(model, "", False, 0, {}, 400)
        return _error(400, f"unknown model {model!r}", param="model",
                      code="model_not_found")
    denied = _key_model_gate(cfg, model, plan["entries"])
    if denied is not None:
        return denied
    batch_id = f"batch-{uuid.uuid4().hex[:12]}"
    _BATCHES[batch_id] = {
        "id": batch_id, "model": model, "status": "in_progress",
        "total": len(requests), "completed": 0, "failed": 0,
        "created": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "creator": _REQUEST_KEY_ID.get() or "(open)",
    }
    _batch_save_manifest()
    asyncio.create_task(_run_batch(batch_id, model, requests))
    logger.info("batch %s submitted: %d requests for %s",
                batch_id, len(requests), model)
    return {"id": batch_id, "model": model, "status": "in_progress",
            "total": len(requests),
            "created": _BATCHES[batch_id]["created"]}


@app.get("/v1/batches")
async def list_batches() -> Response:
    """Recent batches (in-memory registry, newest first)."""
    rows = sorted(_BATCHES.values(),
                  key=lambda b: b.get("created", ""), reverse=True)[:50]
    return {"batches": rows}


@app.get("/admin/batches")
async def admin_list_batches() -> Response:
    """Recent batches — admin-console view of the same registry as
    GET /v1/batches (Governance pane; passes the admin gate when
    auth is enabled)."""
    return await list_batches()


@app.get("/v1/batches/{batch_id}")
async def get_batch(batch_id: str) -> Response:
    m = _BATCHES.get(batch_id)
    if not m:
        return _error(404, f"batch {batch_id!r} not found "
                           f"(registry is in-memory; results files persist "
                           f"in the data dir)", param="batch_id",
                      code="batch_not_found")
    return m


@app.get("/v1/batches/{batch_id}/results")
async def get_batch_results(batch_id: str) -> Response:
    m = _BATCHES.get(batch_id)
    if not m or m.get("status") != "completed":
        return _error(404, f"batch {batch_id!r} not found or not completed",
                      param="batch_id", code="batch_not_found")
    p = _LOG_PATH.parent / f"batch-{batch_id}.jsonl"
    try:
        text = p.read_text(encoding="utf-8")
    except OSError:
        return _error(404, "results file missing", code="batch_not_found")
    return Response(text, media_type="application/jsonl")


# ── tier E: eval harness ───────────────────────────────────────────────────
# POST /admin/evals/run {name, models: [...], prompts: [{id, messages,
# expect_contains?, expect_not_contains?, expect_json?}]} → executes each
# prompt × model through the internal pipeline, evaluates assertions,
# saves a report to data/evals/<name>-<ts>.json, and diffs against the
# previous run of the same name (drift: assertions that flipped).

def _evals_dir() -> Path:
    return _LOG_PATH.parent / "evals"


@app.post("/admin/evals/run")
async def admin_evals_run(request: Request) -> Response:
    """Run a golden-prompt eval across models. Each prompt × model goes
    through the full internal pipeline; assertions: expect_contains[],
    expect_not_contains[], expect_json. Reports persist under
    data/evals/; re-running the same name reports drift (assertions that
    flipped since the previous run)."""
    try:
        body = await request.json()
    except Exception:
        return _error(400, "invalid JSON body")
    name = str(body.get("name") or "").strip()
    models = [str(m).strip() for m in (body.get("models") or []) if str(m).strip()]
    prompts = body.get("prompts")
    if not name or not re.fullmatch(r"[A-Za-z0-9._-]{1,40}", name):
        return _error(400, "eval name: letters/digits/dots/dashes, max 40",
                      param="name")
    if not models:
        return _error(400, "models is required (non-empty array)",
                      param="models")
    if not isinstance(prompts, list) or not prompts:
        return _error(400, "prompts is required (non-empty array)",
                      param="prompts")
    cfg = get_gateway_config()
    cfg.maybe_reload()

    # previous run (for drift)
    prev = _eval_latest_results(name)

    results: list[dict[str, Any]] = []
    for model in models:
        for p in prompts:
            pid = str(p.get("id") or f"p{len(results)}")
            t0 = time.perf_counter()
            row: dict[str, Any] = {"model": model, "prompt_id": pid,
                                   "pass": False, "failed_assertions": [],
                                   "latency_ms": None,
                                   "content_excerpt": ""}
            try:
                resp = await _exec_chat_internal(cfg, model,
                                                 p.get("messages") or [])
                content = str((((resp.get("choices") or [{}])[0])
                               .get("message") or {}).get("content") or "")
                row["latency_ms"] = int((time.perf_counter() - t0) * 1000)
                row["content_excerpt"] = content[:120]
                failed = []
                for s in (p.get("expect_contains") or []):
                    if str(s) not in content:
                        failed.append(f"expect_contains:{s[:40]}")
                for s in (p.get("expect_not_contains") or []):
                    if str(s) in content:
                        failed.append(f"expect_not_contains:{s[:40]}")
                if p.get("expect_json"):
                    try:
                        json.loads(content)
                    except (json.JSONDecodeError, TypeError):
                        failed.append("expect_json")
                row["failed_assertions"] = failed
                row["pass"] = not failed
            except Exception as e:  # noqa: BLE001 — eval records failures
                row["latency_ms"] = int((time.perf_counter() - t0) * 1000)
                row["failed_assertions"] = [f"error:{type(e).__name__}"]
            results.append(row)

    # summary per model
    summary = {}
    for model in models:
        rows = [r for r in results if r["model"] == model]
        summary[model] = {
            "pass": sum(1 for r in rows if r["pass"]),
            "fail": sum(1 for r in rows if not r["pass"]),
            "avg_ms": round(sum(r["latency_ms"] or 0 for r in rows)
                            / len(rows)) if rows else 0,
        }
    # drift vs previous run (same name): (model, prompt_id) pass flips
    drift = []
    if prev:
        prev_map = {(r["model"], r["prompt_id"]): r["pass"] for r in prev}
        for r in results:
            key = (r["model"], r["prompt_id"])
            if key in prev_map and prev_map[key] != r["pass"]:
                drift.append({"model": r["model"], "prompt_id": r["prompt_id"],
                              "change": ("pass→fail" if not r["pass"]
                                         else "fail→pass")})
    report = {"name": name, "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
              "models": models, "results": results, "summary": summary,
              "drift": drift}
    try:
        d = _evals_dir()
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{name}-{int(time.time() * 1000)}.json").write_text(
            json.dumps(report, indent=1), encoding="utf-8")
    except OSError:
        logger.warning("eval report write failed", exc_info=True)
    return report


def _eval_latest_results(name: str) -> list | None:
    """Results array of the most recent saved report for `name`."""
    try:
        files = sorted(_evals_dir().glob(f"{name}-*.json"))
        if not files:
            return None
        return json.loads(files[-1].read_text(encoding="utf-8")).get("results")
    except (OSError, json.JSONDecodeError):
        return None


@app.get("/admin/evals")
async def admin_evals_list() -> Response:
    """Saved eval runs (name, timestamp, pass counts)."""
    out = []
    try:
        for p in sorted(_evals_dir().glob("*.json")):
            try:
                d = json.loads(p.read_text(encoding="utf-8"))
                out.append({"name": d.get("name"), "ts": d.get("ts"),
                            "models": d.get("models"),
                            "summary": d.get("summary")})
            except json.JSONDecodeError:
                continue
    except OSError:
        pass
    return {"evals": out[-50:]}


@app.get("/admin/evals/{name}")
async def admin_evals_get(name: str) -> Response:
    """The most recent report for an eval name."""
    files = sorted(_evals_dir().glob(f"{name}-*.json"))
    if not files:
        return _error(404, f"no eval runs named {name!r}", param="name",
                      code="eval_not_found")
    try:
        return json.loads(files[-1].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return _error(502, f"report unreadable: {e}")


# ── GET /healthz ───────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Backend status, VRAM, loaded instances (design doc)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    engine = get_engine()

    # Fresh-enough VRAM: the 30s placement cache would report the
    # pre-load snapshot right after a model loads/unloads (observed:
    # "used 0 MB" with the pool resident, until the cache aged out).
    # 2s TTL = at most one nvidia-smi spawn (~100ms) per 2s under the
    # console/poller load; placement callers keep the 30s default.
    gpus = detect_gpus(ttl_s=2.0)
    for g in gpus:
        g["free_mb"] = max(g["total_mb"] - g["used_mb"], 0)

    loaded = engine.loaded_models()
    discovered = installed_ggufs(cfg.models_dir)
    return {
        "status": "ok",
        "version": app.version,
        "license": licensing.healthz_license(),
        "tracing": tracing.healthz_status(),
        "bind": cfg.bind,
        "port": cfg.port,
        "admin": ({"bind": cfg.admin_bind, "port": cfg.admin_port}
                  if getattr(cfg, "admin_port", None) else None),
        "models_dir": cfg.models_dir,
        "backends": {name: {"type": b.get("type"),
                            "configured_models": len(b.get("models") or {})}
                     for name, b in cfg.backends.items()},
        "discovered_models": len(discovered),
        "loaded_count": len(loaded),
        "loaded": loaded,  # Ollama-ps shaped: alias/path/size_vram/n_ctx/load_s
        "llama_server": llamaserver.status(),  # managed children (subprocess)
        "runpod": await runpod.refresh_status(cfg),  # lifecycle-managed pods (serverless is stateless)
        "lifecycle": {"watchdog": lifecycle.watchdog_running(),
                      "budget_usd_month": getattr(cfg, "budget_usd_month", 0),
                      "month_to_date": lifecycle.budget_snapshot(),
                      "alerts": lifecycle.alerts(10)},
        "gpus": gpus,
    }


# ── Admin: alias CRUD + instance control (Stage 4) ─────────────────────────

# ── hash-chained audit (v1.4: tamper-evident config-audit.jsonl) ───────────
# Every audit record carries prev (the previous record's hash; "genesis"
# for the first) and hash (sha256 of the record incl. prev, canonical
# JSON). The chain head is per-process and re-read from the file tail on
# first write, so chains continue across restarts. Chains are PER-REPLICA
# by design (audit files are per-replica; the SIEM webhook remains the
# cross-replica aggregate). Legacy (pre-v1.4) records are counted by the
# verifier but not verified; the chain restarts at genesis after them.

_AUDIT_CHAIN_HEAD: str | None = None
_AUDIT_GENESIS = "genesis"


def _audit_hash(rec: dict) -> str:
    """Canonical record hash: sha256 over sorted-keys JSON."""
    return hashlib.sha256(
        json.dumps(rec, sort_keys=True).encode("utf-8")).hexdigest()


def _audit_tail_hash(p) -> str:
    """The hash of the file's last chained record — the chain continues
    across restarts. Genesis when absent/empty/ending on a legacy
    record. Reads only the file tail; a partial first line from the
    windowed read is discarded."""
    try:
        with open(p, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            window = 8192
            f.seek(max(0, size - window))
            skipped = size > window
            tail = f.read().decode("utf-8", errors="replace")
        lines = [l for l in tail.strip().splitlines() if l.strip()]
        if skipped and lines:
            lines = lines[1:]   # the first line may be cut mid-record
        if not lines:
            return _AUDIT_GENESIS
        rec = json.loads(lines[-1])
        return str(rec.get("hash") or _AUDIT_GENESIS)
    except Exception:  # noqa: BLE001 — unreadable tail → fresh chain
        return _AUDIT_GENESIS


def append_audit_record(rec: dict) -> dict | None:
    """THE audit chokepoint: append one tamper-evident record to
    config-audit.jsonl and return the chained record (None on failure —
    auditing must never break the caller). Both writers use this:
    config mutations (_audit_config) and RBAC denials
    (gateway.auth._audit_deny)."""
    global _AUDIT_CHAIN_HEAD
    try:
        p = _LOG_PATH.parent / "config-audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        prev = _AUDIT_CHAIN_HEAD
        if prev is None:
            prev = _audit_tail_hash(p)
        chained = dict(rec)
        chained["prev"] = prev
        chained["hash"] = _audit_hash(chained)
        with _log_lock:
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(chained) + "\n")
        _AUDIT_CHAIN_HEAD = chained["hash"]
        return chained
    except Exception:  # noqa: BLE001
        logger.warning("audit append failed", exc_info=True)
        return None


def verify_audit_chain() -> dict:
    """Walk config-audit.jsonl and verify the hash chain: each chained
    record's hash must match its content, and prev must equal the
    previous record's hash. Legacy (pre-v1.4) records are counted but
    not verified. Returns {lines, chained, legacy, breaks, valid} —
    surfaced by GET /admin/audit (SOC2 CC7.1 tamper evidence)."""
    p = _LOG_PATH.parent / "config-audit.jsonl"
    breaks: list[dict] = []
    chained = legacy = 0
    prev = _AUDIT_GENESIS
    try:
        lines = p.read_text(encoding="utf-8").strip().splitlines()
    except OSError:
        lines = []
    for i, ln in enumerate(lines, 1):
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            breaks.append({"line": i, "reason": "unparseable"})
            continue
        h = rec.get("hash")
        if not h:
            legacy += 1          # pre-v1.4 record — chain restarts after
            prev = _AUDIT_GENESIS
            continue
        body = {k: v for k, v in rec.items() if k != "hash"}
        if _audit_hash(body) != h:
            breaks.append({"line": i, "reason": "hash mismatch (tampered)"})
        elif rec.get("prev") != prev:
            breaks.append({"line": i,
                           "reason": "chain break (prev mismatch)"})
        prev = h
        chained += 1
    return {"lines": len(lines), "chained": chained, "legacy": legacy,
            "breaks": breaks, "valid": not breaks}


def _audit_config(action: str) -> None:
    """Append-only audit of config mutations (who / when / what) —
    hash-chained config-audit.jsonl next to the request log (see
    append_audit_record). Ships the CHAINED record to the configured
    audit webhook (SIEM) when set — the SIEM can verify the chain
    independently."""
    rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "key": _REQUEST_KEY_ID.get() or "(open)",
           "action": action[:200]}
    chained = append_audit_record(rec)
    # remote sink (best-effort, fire-and-forget)
    try:
        cfg = get_gateway_config()
        url = (cfg.audit or {}).get("webhook_url") if cfg.audit else ""
        if url and chained is not None:
            payload = dict(chained)
            payload["type"] = "config_audit"
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_verify_webhook(url, payload))
            except RuntimeError:
                pass  # no loop (sync context) — local log only
    except Exception:  # noqa: BLE001 — alerting must never break mutation
        pass


@app.get("/admin/audit")
async def admin_audit() -> Response:
    """Last config mutations (newest first, max 200) + the hash-chain
    verification status."""
    try:
        lines = (_LOG_PATH.parent / "config-audit.jsonl").read_text(
            encoding="utf-8").strip().splitlines()
    except OSError:
        lines = []
    out = []
    for ln in lines[-200:]:
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return {"audit": out[::-1], "chain": verify_audit_chain()}


def verify_request_chain() -> dict:
    """Walk the request ledger (archive + month files, chronological)
    and verify the chain over every CHAINED record. Plain records —
    pre-v2.5 history and lines written without the 'evidence' claim —
    are skipped TRANSPARENTLY (counted as plain, prev preserved: the
    writer keeps its head across plain lines, so a later chained
    record still references the last chained hash). A chained record
    edited into a plain one (downgrade) orphans the next chained
    record's prev → break. Returns {files, lines, chained, plain,
    breaks, valid} — surfaced by GET /admin/ledger and the Evidence
    Package (v2.5 Evidence Center)."""
    breaks: list[dict] = []
    files: list[dict] = []
    chained = plain = 0
    prev = _REQ_GENESIS
    for p in _req_month_files():
        f_lines = f_chained = f_plain = 0
        try:
            lines = p.read_text(encoding="utf-8",
                                errors="replace").strip().splitlines()
        except OSError:
            lines = []
        for i, ln in enumerate(lines, 1):
            if not ln.strip():
                continue
            f_lines += 1
            try:
                rec = json.loads(ln)
            except json.JSONDecodeError:
                breaks.append({"file": p.name, "line": i,
                               "reason": "unparseable"})
                continue
            h = rec.get("hash")
            if not h:
                plain += 1              # transparent — prev preserved
                f_plain += 1
                continue
            body = {k: v for k, v in rec.items() if k != "hash"}
            if _req_record_hash(body) != h:
                breaks.append({"file": p.name, "line": i,
                               "reason": "hash mismatch (tampered)"})
            elif rec.get("prev") != prev:
                breaks.append({"file": p.name, "line": i,
                               "reason": "chain break (prev mismatch)"})
            prev = h
            chained += 1
            f_chained += 1
        files.append({"file": p.name, "lines": f_lines,
                      "chained": f_chained, "plain": f_plain})
    return {"files": files,
            "lines": sum(f["lines"] for f in files),
            "chained": chained, "plain": plain, "breaks": breaks,
            "valid": not breaks}


@app.get("/admin/ledger")
async def admin_ledger() -> Response:
    """Request-ledger chain verification (v2.5 Evidence Center):
    per-file chained/plain counts + the tamper-evidence walk. Same
    admin gate as /admin/audit."""
    return {"ledger": verify_request_chain()}


def _mutate_config(mutator, action: str = "config-mutation") -> None:
    """Read-modify-write the ACTIVE config store (built-in defaults when
    it has no config yet), then force the process config to reload.
    Every mutation lands in the audit trail with the caller's action.

    The write is optimistic-concurrent (tier D): the store version
    observed at read time rides the save; on a lost race (another
    writer — admin console or replica — committed first) the whole
    read-modify-write retries ONCE against fresh state, then raises.
    Two writers must never silently clobber each other."""
    cfg = get_gateway_config()
    for attempt in (1, 2):
        version = cfg.store.version()
        raw = cfg.store.load()
        if not isinstance(raw, dict):
            raw = default_config_raw()
        mutator(raw)
        if cfg.store.save(raw, expected_version=version):
            break
        logger.warning("config write race (attempt %d/2) — re-reading "
                       "fresh state", attempt)
    else:
        raise RuntimeError("config mutation lost the write race twice — "
                           "reload the config and retry")
    cfg.reload_now()
    _audit_config(action)


class AliasPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    chain: list[str] | None = None
    hedge_s: float | None = None
    local_only: bool | None = None
    delete: bool = False


@app.get("/admin/aliases")
async def admin_list_aliases() -> dict[str, Any]:
    cfg = get_gateway_config()
    cfg.maybe_reload()
    return {"aliases": cfg.aliases}


@app.post("/admin/aliases")
async def admin_upsert_alias(payload: AliasPayload) -> Response:
    cfg = get_gateway_config()
    cfg.maybe_reload()
    name = payload.name.strip()
    if not name:
        return _error(400, "alias name must not be empty", param="name")

    if payload.delete:
        if name not in cfg.aliases:
            return _error(400, f"alias {name!r} not found", param="name",
                          code="alias_not_found")
        _mutate_config(lambda raw: raw.setdefault("aliases", {}).pop(name, None),
                       f"alias-delete {name}")
        return {"deleted": name,
                "aliases": get_gateway_config().aliases}

    chain = [str(c).strip() for c in (payload.chain or []) if str(c).strip()]
    if not chain:
        return _error(400, "alias chain must list at least one "
                      "'backend:model' entry", param="chain")
    for entry in chain:  # validate against the CURRENT backends
        try:
            cfg._resolve_entry(entry)
        except GatewayConfigError as e:
            return _error(400, str(e), param="chain")

    alias: dict[str, Any] = {"chain": chain}
    if payload.hedge_s is not None:
        alias["hedge_s"] = max(0.0, float(payload.hedge_s))
    if payload.local_only:
        alias["local_only"] = True

    def _apply(raw: dict[str, Any]) -> None:
        raw.setdefault("aliases", {})[name] = alias

    _mutate_config(_apply, f"alias-upsert {name}")
    return {"upserted": name, "alias": alias,
            "aliases": get_gateway_config().aliases}


class LoadPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    model: str = ""
    all: bool = False  # noqa: A003 — 'all' matches the engine's unload API


@app.post("/admin/load")
async def admin_load(payload: LoadPayload) -> Response:
    """Preload an embedded instance (config model or discovered stem)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    name = payload.model.strip()
    if not name:
        return _error(400, "model must not be empty", param="model")
    engine = get_engine()
    # v2.2 scale caps: loading beyond the licensed instance count
    # refuses (embedding instances count toward the same cap)
    cap_ok, cap_limit = licensing.cap_allows(
        "instances", len(engine.loaded_models()))
    if not cap_ok:
        return _error(403, f"license cap: instance limit ({cap_limit}) "
                      "reached — this deployment's license caps its "
                      "loaded-instance count", code="license_caps")
    try:
        # Embedding models FIRST — they're explicit config, and a name
        # equal to a GGUF stem would otherwise auto-discover as CHAT.
        if name in cfg.local_embedding_models():
            engine._get_embedding_instance(name)
        else:
            engine._get_instance(name)
    except EmbeddedLLMError as e:
        return _error(400, str(e), param="model", code="model_not_found")
    return {"loaded": engine.loaded_models()}


@app.post("/admin/unload")
async def admin_unload(payload: LoadPayload) -> Response:
    cfg = get_gateway_config()
    cfg.maybe_reload()
    engine = get_engine()
    which = "all" if payload.all else payload.model.strip()
    if not which:
        return _error(400, "model must not be empty (or send "
                      "{\"all\": true})", param="model")
    # Drain first: bounded wait for in-flight requests to finish so the
    # unload doesn't pull the model out from under them (their worker
    # checkout would be orphaned and its VRAM pinned until GC).
    stuck = await engine.drain(which, wait_s=cfg.queue["timeout_s"] * 0.1)
    if stuck:
        logger.warning("admin/unload: %s still has stuck checkout(s) after "
                       "drain — unloading anyway (VRAM stays pinned until "
                       "the holders are reaped)", stuck)
    n = engine.unload(which)
    return {"unloaded": n, "drained": not stuck, "stuck": stuck,
            "loaded": engine.loaded_models()}


# ── Anthropic Messages API (/v1/messages) ──────────────────────────────────

def _anthropic_error(status: int, message: str,
                     err_type: str = "invalid_request_error") -> Response:
    """OpenAI-shaped errors won't do here — Anthropic clients expect this."""
    return JSONResponse(status_code=status, content={
        "type": "error",
        "error": {"type": err_type, "message": message},
    })


def _blocks_to_text(content: Any) -> str:
    """Anthropic content (string or block list) → plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text":
                parts.append(b.get("text", ""))
        return "\n".join(p for p in parts if p)
    return ""


def _anthropic_tools_to_openai(tools: Any) -> list[dict[str, Any]]:
    """Anthropic tool definitions → OpenAI function tools."""
    out: list[dict[str, Any]] = []
    if not isinstance(tools, list):
        return out
    for t in tools:
        if not isinstance(t, dict) or not t.get("name"):
            continue
        out.append({
            "type": "function",
            "function": {
                "name": str(t["name"]),
                "description": str(t.get("description") or ""),
                "parameters": t.get("input_schema")
                or {"type": "object", "properties": {}},
            },
        })
    return out


def _anthropic_messages_to_openai(raw_msgs: list) -> list[dict[str, Any]]:
    """Anthropic messages (tool_use / tool_result blocks) → OpenAI.

    - assistant tool_use blocks → assistant tool_calls
    - user tool_result blocks → role="tool" messages (emitted before the
      turn's plain text — OpenAI convention: tool output follows the
      assistant call, the next user text comes after)
    - text blocks join into the message content
    """
    out: list[dict[str, Any]] = []
    for m in raw_msgs:
        role = m.get("role", "user")
        content = m.get("content", "")
        if not isinstance(content, list):
            out.append({"role": role, "content":
                        content if isinstance(content, str)
                        else _blocks_to_text(content)})
            continue
        texts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        tool_results: list[dict[str, Any]] = []
        for b in content:
            if not isinstance(b, dict):
                continue
            btype = b.get("type")
            if btype == "text":
                if b.get("text"):
                    texts.append(b["text"])
            elif btype == "tool_use":
                tool_calls.append({
                    "id": b.get("id") or f"call_{b.get('name', 'x')}",
                    "type": "function",
                    "function": {
                        "name": str(b.get("name") or ""),
                        "arguments": json.dumps(b.get("input") or {}),
                    },
                })
            elif btype == "tool_result":
                inner = b.get("content")
                text = inner if isinstance(inner, str) else _blocks_to_text(inner)
                tool_results.append({
                    "role": "tool",
                    "tool_call_id": str(b.get("tool_use_id") or ""),
                    "content": text or "",
                })
        if role == "assistant":
            msg: dict[str, Any] = {"role": "assistant"}
            joined = "\n".join(texts)
            if tool_calls:
                if joined:
                    msg["content"] = joined
                msg["tool_calls"] = tool_calls
            else:
                msg["content"] = joined
            out.append(msg)
        else:
            out.extend(tool_results)
            if texts:
                out.append({"role": role, "content": "\n".join(texts)})
    return out


def _sse_event(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _close_quietly(obj: Any) -> Any:
    """Close an SDK client/stream whether close() is sync or async.
    Returns an awaitable (or None). Never raises — cleanup only."""
    fn = getattr(obj, "close", None)
    if fn is None:
        return None
    try:
        res = fn()
    except Exception:  # noqa: BLE001 — cleanup must not raise
        return None
    return res if hasattr(res, "__await__") else None


@app.post("/v1/messages")
async def anthropic_messages(request: Request) -> Response:
    """Anthropic Messages API shim over the gateway's OpenAI surface.

    Lets Anthropic-style clients (Workshop, Claude Code, LangChain
    anthropic provider) talk to the gateway. Translates request →
    /v1/chat/completions (self-call) → Anthropic response shape.
    Text AND tool_use content: tools, tool_use blocks and tool_result
    history are translated both ways; system as string or block list.
    """
    try:
        body = await request.json()
    except Exception:
        return _anthropic_error(400, "Invalid JSON body")

    model = str(body.get("model") or "").strip()
    if not model:
        return _anthropic_error(400, "model is required")

    raw_msgs = body.get("messages") or []
    if not raw_msgs:
        return _anthropic_error(400, "messages is required")

    # Translate messages: Anthropic → OpenAI (tool_use / tool_result aware)
    oai_messages: list[dict[str, Any]] = []
    system = body.get("system") or ""
    sys_text = system if isinstance(system, str) else _blocks_to_text(system)
    if sys_text:
        oai_messages.append({"role": "system", "content": sys_text})
    oai_messages.extend(_anthropic_messages_to_openai(raw_msgs))
    tools = _anthropic_tools_to_openai(body.get("tools"))

    max_tokens = int(body.get("max_tokens") or 1024)
    stream = bool(body.get("stream"))
    temperature = body.get("temperature")

    cfg = get_gateway_config()
    cfg.maybe_reload()

    # Anthropic-style clients send their own model names (claude-*, etc.)
    # which don't exist here. Resolve to a known gateway model; prefer
    # loaded (resident) models — largest context first, so a long-prompt
    # client (Workshop's session handoff is ~11.6k tokens) never lands on
    # a small-ctx instance merely because it was loaded first — then
    # configured backend models, then discovered GGUF stems, then aliases.
    # Without this the self-call 400s (model_not_found) and the shim
    # surfaces a confusing 502.
    candidates: list[str] = []
    engine = get_engine()
    try:
        loaded = sorted(engine.loaded_models(),
                        key=lambda m: m.get("n_ctx") or 0, reverse=True)
        candidates += [m["alias"] for m in loaded]
    except Exception:  # noqa: BLE001 — resolution must never block the call
        pass
    for b in (cfg.backends or {}).values():
        if isinstance(b, dict):
            candidates += [str(k) for k in (b.get("models") or {}).keys()]
    try:
        candidates += [d["name"] for d in installed_ggufs(cfg.models_dir)]
    except Exception:  # noqa: BLE001
        pass
    candidates += [str(a) for a in (getattr(cfg, "aliases", {}) or {})]
    known = set(candidates)
    if model not in known:
        fallback = next((c for c in candidates if c), model)
        logger.info("anthropic shim: model %r unknown, using %r", model, fallback)
        model = fallback

    # v2.5.1: the shim self-calls /v1 with the per-process internal
    # bearer (consumer-grade principal) — it no longer depends on
    # gateway_keys, which are now hashed-at-rest (GL-R07).
    api_key = _auth_internal_bearer()
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"

    if stream:
        async def gen() -> AsyncIterator[bytes]:
            out_tok = 0
            # Own client + stream, ALWAYS closed in finally. An unclosed
            # AsyncStream leaks the self-call connection: the gateway then
            # never sees the client disconnect, the inner SSE generator
            # stays suspended at its yield, and the embedded worker
            # checkout is held forever (observed wedging the 27B pool).
            client = AsyncOpenAI(
                base_url=f"http://{cfg.bind}:{cfg.port}/v1",
                api_key=api_key,
            )
            s = None
            yield _sse_event("message_start", {
                "type": "message_start",
                "message": {"id": msg_id, "type": "message", "role": "assistant",
                            "model": model, "content": [],
                            "usage": {"input_tokens": 0, "output_tokens": 0}},
            })
            yield _sse_event("content_block_start", {
                "type": "content_block_start", "index": 0,
                "content_block": {"type": "text", "text": ""}})
            # Tail events live INSIDE the try/except so a GeneratorExit
            # (client vanished) skips them — yielding after GeneratorExit
            # would raise. The finally is the only code that runs on every
            # exit path; closing the stream there is what lets the inner
            # request cancel and release its worker checkout.
            try:
                kwargs: dict[str, Any] = dict(model=model, messages=oai_messages,
                                              max_tokens=max_tokens, stream=True)
                if temperature is not None:
                    kwargs["temperature"] = temperature
                if tools:
                    kwargs["tools"] = tools
                s = await client.chat.completions.create(**kwargs)
                # JSON-convention backends stream the raw tool JSON as
                # text BEFORE the parsed tool_calls chunk arrives. Hold
                # the very first text back until we know it isn't a
                # `{...}` tool call — prose keeps streaming live, tool
                # JSON gets suppressed and re-emitted as tool_use.
                mode: str | None = None   # None=undecided, "text", "tool"
                held = ""
                tc_acc: dict[int, dict] = {}
                async for chunk in s:
                    choices = getattr(chunk, "choices", None) or []
                    if not choices:
                        continue
                    delta = getattr(choices[0], "delta", None)
                    if delta is None:
                        continue
                    tcs = getattr(delta, "tool_calls", None)
                    if isinstance(tcs, list):
                        for tc in tcs:
                            idx = getattr(tc, "index", 0) or 0
                            slot = tc_acc.setdefault(
                                idx, {"id": "", "name": "", "args": ""})
                            tc_id = getattr(tc, "id", None)
                            if tc_id:
                                slot["id"] = tc_id
                            fn = getattr(tc, "function", None)
                            if fn is not None:
                                name = getattr(fn, "name", None)
                                if name:
                                    slot["name"] = name
                                slot["args"] += (getattr(fn, "arguments", None)
                                                 or "")
                    text = getattr(delta, "content", None)
                    if not text:
                        continue
                    if mode == "text":
                        out_tok += 1
                        yield _sse_event("content_block_delta", {
                            "type": "content_block_delta", "index": 0,
                            "delta": {"type": "text_delta", "text": text}})
                    elif mode is None:
                        held += text
                        stripped = held.lstrip()
                        if not stripped:
                            continue
                        if stripped[0] == "{":
                            mode = "tool"   # suppress; tool_use follows
                        else:
                            mode = "text"
                            out_tok += 1
                            yield _sse_event("content_block_delta", {
                                "type": "content_block_delta", "index": 0,
                                "delta": {"type": "text_delta", "text": held}})
                            held = ""
                    # mode == "tool": swallow the JSON-convention text
                # Flush anything still held: either undecided whitespace/
                # short prose, or JSON-looking text the engine never
                # parsed into a tool call — show it, don't eat it.
                if held and (mode != "tool" or not tc_acc):
                    out_tok += 1
                    yield _sse_event("content_block_delta", {
                        "type": "content_block_delta", "index": 0,
                        "delta": {"type": "text_delta", "text": held}})
                yield _sse_event("content_block_stop", {
                    "type": "content_block_stop", "index": 0})
                if tc_acc:
                    block = 1
                    for idx in sorted(tc_acc):
                        slot = tc_acc[idx]
                        try:
                            json.loads(slot["args"] or "{}")
                            partial = slot["args"] or "{}"
                        except (json.JSONDecodeError, ValueError):
                            partial = json.dumps({"_raw": slot["args"]})
                        yield _sse_event("content_block_start", {
                            "type": "content_block_start", "index": block,
                            "content_block": {"type": "tool_use",
                                              "id": slot["id"] or f"toolu_{idx}",
                                              "name": slot["name"],
                                              "input": {}}})
                        yield _sse_event("content_block_delta", {
                            "type": "content_block_delta", "index": block,
                            "delta": {"type": "input_json_delta",
                                      "partial_json": partial}})
                        yield _sse_event("content_block_stop", {
                            "type": "content_block_stop", "index": block})
                        block += 1
                    yield _sse_event("message_delta", {
                        "type": "message_delta", "delta": {"stop_reason": "tool_use"},
                        "usage": {"output_tokens": out_tok}})
                else:
                    yield _sse_event("message_delta", {
                        "type": "message_delta", "delta": {"stop_reason": "end_turn"},
                        "usage": {"output_tokens": out_tok}})
                yield _sse_event("message_stop", {"type": "message_stop"})
            except Exception as e:  # noqa: BLE001 — stream must not die silently
                yield _sse_event("error", {
                    "type": "error",
                    "error": {"type": "api_error", "message": str(e)}})
                yield _sse_event("content_block_stop", {
                    "type": "content_block_stop", "index": 0})
                yield _sse_event("message_delta", {
                    "type": "message_delta", "delta": {"stop_reason": "end_turn"},
                    "usage": {"output_tokens": out_tok}})
                yield _sse_event("message_stop", {"type": "message_stop"})
            finally:
                aw = _close_quietly(s)
                if aw is not None:
                    try:
                        await aw
                    except Exception:  # noqa: BLE001
                        pass
                aw = _close_quietly(client)
                if aw is not None:
                    try:
                        await aw
                    except Exception:  # noqa: BLE001
                        pass

        return StreamingResponse(gen(), media_type="text/event-stream")

    # Non-streaming
    client = AsyncOpenAI(
        base_url=f"http://{cfg.bind}:{cfg.port}/v1",
        api_key=api_key,
    )
    try:
        kwargs: dict[str, Any] = dict(model=model, messages=oai_messages,
                                      max_tokens=max_tokens)
        if temperature is not None:
            kwargs["temperature"] = temperature
        if tools:
            kwargs["tools"] = tools
        resp = await client.chat.completions.create(**kwargs)
    except Exception as e:  # noqa: BLE001 — surface upstream failure
        # classify (walks causes: the gateway's own 429/504 arrive as
        # openai APIStatusError) — the requestor gets the class, never a
        # substituted model. v2.5.1 (GL-R15): the CLASS only reaches the
        # client — exception strings can carry internal endpoints; the
        # detail goes to the operator log.
        kind = cloud.classify_error(e)
        logger.warning("anthropic shim upstream failure (%s): %s",
                       kind, e)
        if kind == "rate_limit":
            return _anthropic_error(429, "Upstream rate limited",
                                    "rate_limit_error")
        if kind == "timeout":
            return _anthropic_error(504, "Upstream timeout", "api_error")
        return _anthropic_error(502, "Upstream error", "api_error")
    finally:
        aw = _close_quietly(client)
        if aw is not None:
            try:
                await aw
            except Exception:  # noqa: BLE001
                pass

    text_out = ""
    usage = getattr(resp, "usage", None)
    in_tok = (getattr(usage, "prompt_tokens", 0) or 0) if usage else 0
    out_tok = (getattr(usage, "completion_tokens", 0) or 0) if usage else 0
    choices = getattr(resp, "choices", None) or []
    tool_blocks: list[dict[str, Any]] = []
    if choices:
        msg = getattr(choices[0], "message", None)
        if msg is not None:
            text_out = getattr(msg, "content", None) or ""
            tcs = getattr(msg, "tool_calls", None)
            if isinstance(tcs, list):
                for tc in tcs:
                    fn = getattr(tc, "function", None)
                    if fn is None:
                        continue
                    raw_args = getattr(fn, "arguments", None) or "{}"
                    try:
                        args = json.loads(raw_args)
                    except (json.JSONDecodeError, ValueError):
                        args = None
                    if not isinstance(args, dict):
                        args = {"_raw": raw_args}
                    tool_blocks.append({
                        "type": "tool_use",
                        "id": getattr(tc, "id", None)
                        or f"toolu_{len(tool_blocks)}",
                        "name": getattr(fn, "name", None) or "",
                        "input": args,
                    })

    content: list[dict[str, Any]] = []
    if text_out and text_out.strip():
        content.append({"type": "text", "text": text_out})
    content.extend(tool_blocks)
    if not content:
        content = [{"type": "text", "text": ""}]

    return JSONResponse({
        "id": msg_id,
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": "tool_use" if tool_blocks else "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": in_tok, "output_tokens": out_tok},
    })


# ── Console (Stage 5): page + config editor + stats + events ───────────────

@app.get("/", response_class=HTMLResponse)
async def console() -> Response:
    """Single-page console (config / models / performance / logs panes)."""
    html = (Path(__file__).resolve().parent / "console.html").read_text(
        encoding="utf-8")
    return HTMLResponse(html)


@app.get("/admin/whoami")
async def admin_whoami(request: Request) -> Response:
    """The request's governing principal — the console's role
    awareness. who: the audit identity (sso:<email> for SSO
    principals, admin:<role>:<hash8> for keys, "open" in open mode).
    SSO principals also carry their full identity. The server stays
    the source of truth for enforcement; this is what lets the UI
    stop offering actions the role cannot perform."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    key_id = _REQUEST_KEY_ID.get()
    role, who, identity = auth.current_principal(cfg, request, key_id)
    if identity is not None:
        via = "sso"
    elif key_id == "admin":
        via = "admin_key"
    else:
        via, who = "open", "open"
    out: dict[str, Any] = {"via": via, "role": role, "who": who}
    if identity is not None:
        out["identity"] = {"sub": str(identity.get("sub") or ""),
                           "email": str(identity.get("email") or ""),
                           "tenant": str(identity.get("tenant") or "")}
    return out


@app.get("/admin/license")
async def admin_license() -> dict[str, Any]:
    """Licensing status (v1.5 closed distribution): mode, license
    claims (edition/customer/vendor/support/features/expiry) or trial
    window. Read-only, behind the admin pipeline + RBAC like every
    /admin GET. Claims are visible here — never on /healthz (that
    surface is unauthenticated)."""
    return licensing.status()


@app.get("/admin/tracing")
async def admin_tracing() -> dict[str, Any]:
    """Tracing status (v2.0): OTLP endpoint, sampling, span counters
    (created/exported/dropped, export errors). Read-only, behind the
    admin pipeline + RBAC like every /admin GET. The endpoint string
    stays here — never on /healthz."""
    return tracing.status()


@app.get("/admin/config")
async def admin_get_config() -> Response:
    """Raw config text for the console editor (defaults when the store
    has none). Reads through the ACTIVE store — file or database —
    never around it via cfg.path."""
    cfg = get_gateway_config()
    raw = cfg.store.load()
    text = (json.dumps(raw, indent=2) if isinstance(raw, dict)
            else json.dumps(default_config_raw(), indent=2))
    return {"path": str(getattr(cfg.store, "path", "(config store)")),
            "text": text}


class ConfigPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    text: str


@app.put("/admin/config")
async def admin_put_config(payload: ConfigPayload) -> Response:
    """Save the raw config text (validated as a JSON object) through the
    ACTIVE store + reload now. Wholesale replace — last write wins (the
    read-modify-write mutations go through _mutate_config, which is
    optimistic-concurrent)."""
    try:
        data = json.loads(payload.text)
    except json.JSONDecodeError as e:
        return _error(400, f"invalid JSON: {e}", param="text")
    if not isinstance(data, dict):
        return _error(400, "config must be a JSON object", param="text")
    cfg = get_gateway_config()
    cfg.store.save(data)
    cfg.reload_now()
    _audit_config("raw-config-put")
    return {"ok": True,
            "backends": {n: len(b.get("models") or {})
                         for n, b in cfg.backends.items()},
            "aliases": sorted(cfg.aliases)}


@app.get("/admin/stats")
async def admin_stats() -> Response:
    """Per-model aggregates + per-key usage + recent requests.

    Full history without scanning archives: closed months load from
    gateway-rollup-*.json; the current month aggregates live from the
    month-scoped log. per_key rows carry their configured budgets."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    _rollup_closed_months()
    per: dict[tuple[str, str], dict[str, Any]] = {}
    keys: dict[str, dict[str, Any]] = {}
    total = 0

    def _merge(agg: dict[str, Any]) -> None:
        nonlocal total
        total += int(agg.get("total") or 0)
        for row in agg.get("per_model") or []:
            mk = (str(row.get("model")), str(row.get("backend")))
            dst = per.setdefault(mk, {"model": mk[0], "backend": mk[1],
                                      "n": 0, "ms_sum": 0, "max_ms": 0,
                                      "total_tokens": 0, "cost_usd": 0.0,
                                      "errors": 0})
            dst["n"] += int(row.get("n") or 0)
            dst["ms_sum"] += int(row.get("ms_sum") or 0)
            dst["max_ms"] = max(dst["max_ms"], int(row.get("max_ms") or 0))
            dst["total_tokens"] += int(row.get("total_tokens") or 0)
            dst["cost_usd"] += float(row.get("cost_usd") or 0.0)
            dst["errors"] += int(row.get("errors") or 0)
        for row in agg.get("per_key") or []:
            kid = str(row.get("key"))
            dst = keys.setdefault(kid, {"key": kid, "n": 0, "total_tokens": 0,
                                        "cost_usd": 0.0, "errors": 0})
            dst["n"] += int(row.get("n") or 0)
            dst["total_tokens"] += int(row.get("total_tokens") or 0)
            dst["cost_usd"] += float(row.get("cost_usd") or 0.0)
            dst["errors"] += int(row.get("errors") or 0)

    try:
        for rp in sorted(_LOG_PATH.parent.glob("gateway-rollup-*.json")):
            try:
                _merge(json.loads(rp.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
    except OSError:
        pass
    month = _month_key()
    _ensure_month_log()
    month_lines = _iter_month_lines(month)
    _merge(_aggregate_lines(month_lines))

    key_rows = []
    for kid, kagg in keys.items():
        entry = (cfg.api_keys or {}).get(kid) or {}
        kagg["monthly_token_budget"] = int(
            entry.get("monthly_token_budget") or 0)
        kagg["monthly_usd_budget"] = float(
            entry.get("monthly_usd_budget") or 0)
        key_rows.append(kagg)

    rows = []
    for agg in per.values():
        rows.append({"model": agg["model"], "backend": agg["backend"],
                     "n": agg["n"],
                     "avg_ms": round(agg["ms_sum"] / agg["n"]) if agg["n"] else 0,
                     "max_ms": agg["max_ms"],
                     "total_tokens": agg["total_tokens"],
                     "cost_usd": round(agg["cost_usd"], 6),
                     "errors": agg["errors"]})
    rows.sort(key=lambda r: -r["n"])
    key_rows.sort(key=lambda r: -r["n"])
    # Monthly token budgets (openai-type cloud backends): live used/budget
    # from the log-derived ledger — the console's HF panel shows this.
    budgets = []
    try:
        for bname, b in (cfg.backends or {}).items():
            if (b.get("type") == "openai"
                    and int(b.get("monthly_token_budget") or 0) > 0):
                budgets.append({
                    "backend": bname,
                    "budget": int(b["monthly_token_budget"]),
                    "used": _scope_spend(f"backend:{bname}")["tokens"],
                    "month": month,
                })
    except Exception:
        pass
    return {"per_model": rows, "per_key": key_rows,
            "recent": month_lines[-50:], "total": total,
            "budgets": budgets}


async def _run_verify(cfg, only: set | None, include_pods: bool,
                      timeout_s: float, nonce: int) -> list[dict[str, Any]]:
    """The verify probe loop, shared by POST /admin/verify and the
    scheduled synthetic verification. POD models are skipped unless
    include_pods (they deploy on demand — real money, minutes)."""
    results: list[dict[str, Any]] = []
    for bname, backend in cfg.backends.items():
        if only is not None and bname not in only:
            continue
        btype = backend.get("type")
        for mkey in (backend.get("models") or {}):
            label = f"{bname}:{mkey}"
            if (btype == "runpod" and not include_pods
                    and not (backend["models"][mkey] or {}).get("endpoint_id")):
                results.append({"backend": bname, "model": mkey,
                                "latency_ms": None, "pass": False,
                                "detail": "skipped (pod — deploy on demand; "
                                          "include_pods to verify)"})
                continue
            req = ChatRequest(model=label, messages=[{
                "role": "user",
                "content": f"Reply with exactly: gateway verify ok {nonce}"}])
            t0 = time.perf_counter()
            try:
                resp, _winner, _mk = await asyncio.wait_for(
                    _exec_entry(cfg, (bname, mkey), req, label),
                    timeout=timeout_s)
                ms = int((time.perf_counter() - t0) * 1000)
                text = ""
                try:
                    text = str((((resp.get("choices") or [{}])[0])
                                .get("message") or {}).get("content") or "")
                except Exception:
                    pass
                ok = bool(text.strip())
                results.append({
                    "backend": bname, "model": mkey, "latency_ms": ms,
                    "pass": ok,
                    "detail": (f"answered: {text[:60]!r}" if ok
                               else "responded with no content")})
            except asyncio.TimeoutError:
                results.append({"backend": bname, "model": mkey,
                                "latency_ms": int((time.perf_counter() - t0) * 1000),
                                "pass": False,
                                "detail": f"timeout after {timeout_s:g}s"})
            except Exception as e:
                results.append({"backend": bname, "model": mkey,
                                "latency_ms": int((time.perf_counter() - t0) * 1000),
                                "pass": False,
                                "detail": f"{cloud.classify_error(e)}: {str(e)[:120]}"})
    return results


@app.post("/admin/verify")
async def admin_verify(request: Request) -> Response:
    """Active round-trip self-test (ported from the assistant's verify
    stack): a nonce prompt through every configured backend×model,
    reporting latency/pass/detail per entry. POD models are skipped by
    default; pass {"include_pods": true} to verify those too. Body:
    {include_pods?, timeout_s? (default 30), backends?: [names]} — the
    backends filter probes a subset (e.g. ["hf"] on a box whose local
    models don't fit in free VRAM). Calls _exec_entry directly — the
    response cache is untouched."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    include_pods = bool(body.get("include_pods"))
    try:
        timeout_s = max(5.0, float(body.get("timeout_s") or 30.0))
    except (TypeError, ValueError):
        timeout_s = 30.0
    only = None
    if isinstance(body.get("backends"), list) and body.get("backends"):
        only = {str(b).strip() for b in body["backends"] if str(b).strip()}
    cfg = get_gateway_config()
    cfg.maybe_reload()
    results = await _run_verify(cfg, only, include_pods, timeout_s,
                                int(time.time() * 1000))
    npass = sum(1 for r in results if r["pass"])
    return {"results": results,
            "summary": {"total": len(results), "pass": npass,
                        "fail": len(results) - npass}}


# ── scheduled synthetic verification (tier 2) ──────────────────────────────
# config verify_schedule {enabled, interval_min, backends, timeout_s,
# failures_before_alert, webhook_url}: a background task re-runs the
# verify probes on a timer, tracks consecutive failures per backend×model,
# and alerts (event ring + optional webhook POST) on the OK→FAIL
# transition once the threshold is crossed — plus a recovery notice.

# Consecutive-failure tracking rides the storage backend (shared alert
# state across replicas — only one replica alerts on a transition).
_VERIFY_STATE = NamespaceDict("verify_state")  # "backend:model" → {fails, alerting}
_VERIFY_LAST_RUN: str = ""
_VERIFY_SCHED_TASK: asyncio.Task | None = None


def reset_verify_state() -> None:
    """Drop scheduler state (tests)."""
    _VERIFY_STATE.clear()
    global _VERIFY_LAST_RUN
    _VERIFY_LAST_RUN = ""


async def _verify_webhook(url: str, payload: dict) -> None:
    """POST a JSON payload to a webhook — best-effort, never raises.
    (Shared by the verify scheduler and the audit sink.)"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as hc:
            await hc.post(url, json=payload)
    except Exception:  # noqa: BLE001 — alerting must never crash the loop
        logger.warning("verify webhook failed", exc_info=True)


async def _process_verify_results(cfg, results: list[dict[str, Any]]) -> None:
    """Consecutive-failure tracking + transition alerts. 'skipped' rows
    (pods) reset nothing — they are not failures."""
    sched = cfg.verify_schedule or {}
    threshold = max(1, int(sched.get("failures_before_alert") or 2))
    webhook = str(sched.get("webhook_url") or "")

    def _alert(msg: str, payload: dict) -> None:
        logger.warning("gateway: %s", msg)
        _event_ring().push({"ts": time.strftime("%H:%M:%S"),
                            "level": "WARNING", "logger": "gateway.verify",
                            "message": msg})
        if webhook:
            asyncio.create_task(_verify_webhook(webhook, payload))

    for row in results:
        key = f"{row.get('backend')}:{row.get('model')}"
        detail = str(row.get("detail") or "")
        if "skipped" in detail:
            continue
        if row.get("pass"):
            st = _VERIFY_STATE.get(key)
            if st and st.get("alerting"):
                _alert(f"verify: {key} RECOVERED",
                       {"type": "verify_recovered", "target": key})
            _VERIFY_STATE[key] = {"fails": 0, "alerting": False}
            continue
        # read-modify-write (backend-visible, never in-place mutation)
        st = _VERIFY_STATE.get(key) or {"fails": 0, "alerting": False}
        fails = int(st.get("fails") or 0) + 1
        alerting = bool(st.get("alerting"))
        if fails >= threshold and not alerting:
            alerting = True
            _alert(f"verify: {key} failing {fails}x — {detail[:120]}",
                   {"type": "verify_failing", "target": key,
                    "consecutive_failures": fails,
                    "detail": detail[:200]})
        _VERIFY_STATE[key] = {"fails": fails, "alerting": alerting}


async def _verify_scheduler_loop() -> None:
    logger.info("verify scheduler started")
    while True:
        try:
            cfg = get_gateway_config()
            cfg.maybe_reload()
            sched = cfg.verify_schedule or {}
            if sched.get("enabled"):
                only = set(sched.get("backends") or []) or None
                results = await _run_verify(
                    cfg, only, False,
                    float(sched.get("timeout_s") or 30.0),
                    int(time.time() * 1000))
                global _VERIFY_LAST_RUN
                _VERIFY_LAST_RUN = time.strftime("%Y-%m-%dT%H:%M:%S")
                await _process_verify_results(cfg, results)
                npass = sum(1 for r in results if r.get("pass"))
                logger.info("scheduled verify: %d/%d pass", npass, len(results))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — the loop must survive anything
            logger.warning("scheduled verify failed", exc_info=True)
        try:
            interval = max(1, int((get_gateway_config().verify_schedule
                                   or {}).get("interval_min") or 30))
        except Exception:
            interval = 30
        await asyncio.sleep(interval * 60)


@app.on_event("startup")
async def _start_verify_scheduler() -> None:
    global _VERIFY_SCHED_TASK
    # tier B hardening at boot: hash-migrate any plaintext api_keys AND
    # gateway_keys in the config FILE (the in-memory normalizer already
    # hashes api_keys; _resolve_bearer hashes gateway_keys at compare),
    # then enforce log retention.
    _migrate_plaintext_keys()
    _enforce_log_retention()
    if _VERIFY_SCHED_TASK is None or _VERIFY_SCHED_TASK.done():
        _VERIFY_SCHED_TASK = asyncio.create_task(_verify_scheduler_loop())


def _auth_internal_bearer() -> str:
    """The per-process internal self-call bearer (anthropic shim →
    /v1). Consumer-grade principal; never admin."""
    from .auth import _INTERNAL_BEARER
    return _INTERNAL_BEARER


def _migrate_plaintext_keys() -> None:
    """One-time rewrite to hashed-at-rest (GL-R07, v2.5.1 + v2.6):
    - api_keys entries carrying a plaintext `key` → sha256 `key_hash`
    - gateway_keys strings that are not already sha256 hex → hashes
    - admin_key_roles[].key plaintext values → hashes (v2.6: the same
      credential must not survive in plaintext in the role map when
      gateway_keys itself is hashed; also fixes the RBAC lint's
      dead-entry false positives after gateway_keys migration)
    The plaintext leaves the stored config; auth is unaffected
    (presented tokens are hashed and compared). Idempotent."""
    try:
        cfg = get_gateway_config()
        raw = cfg.store.load()
        if not isinstance(raw, dict):
            return
        changed = 0
        entries = raw.get("api_keys")
        if isinstance(entries, list):
            for e in entries:
                if isinstance(e, dict) and str(e.get("key") or "").strip():
                    e["key_hash"] = _sha256_hex(str(e["key"]).strip())
                    e.pop("key", None)
                    changed += 1
        from .auth import _is_hex64
        gw = raw.get("gateway_keys")
        if isinstance(gw, list):
            new_gw = []
            for k in gw:
                k = str(k)
                if k and not _is_hex64(k):
                    new_gw.append(_sha256_hex(k))
                    changed += 1
                else:
                    new_gw.append(k)
            if changed:
                raw["gateway_keys"] = new_gw
        roles = raw.get("admin_key_roles")
        if isinstance(roles, list):
            new_roles = []
            for item in roles:
                if isinstance(item, dict):
                    k = str(item.get("key") or "").strip()
                    if k and not _is_hex64(k):
                        item = dict(item)
                        item["key"] = _sha256_hex(k)
                        changed += 1
                new_roles.append(item)
            if changed:
                raw["admin_key_roles"] = new_roles
        if not changed:
            return

        def _mut(r: dict) -> None:
            if isinstance(entries, list):
                r["api_keys"] = entries
            if isinstance(gw, list):
                r["gateway_keys"] = raw["gateway_keys"]
            if isinstance(roles, list):
                r["admin_key_roles"] = raw["admin_key_roles"]

        _mutate_config(_mut, f"key-hash-migration ({changed} keys)")
        logger.info("migrated %d key(s) to hashed-at-rest", changed)
    except Exception:  # noqa: BLE001 — boot hardening must not block
        logger.warning("key hash migration failed", exc_info=True)


def _enforce_log_retention() -> None:
    """Delete gateway-log-YYYY-MM.jsonl / gateway-rollup-YYYY-MM.json
    older than log_retention_months (0 = keep forever). Runs at startup
    and at month rollover."""
    try:
        cfg = get_gateway_config()
        months = int(getattr(cfg, "log_retention_months", 0) or 0)
        if months <= 0:
            return
        current = _month_key()
        cutoff = (int(current[:4]) * 12 + int(current[5:7])) - months
        import glob as _glob
        for pattern in ("gateway-log-*.jsonl", "gateway-rollup-*.json",
                        "gateway-log-*.jsonl.migrated"):
            for p in _glob.glob(str(_LOG_PATH.parent / pattern)):
                name = Path(p).name
                # extract YYYY-MM
                digits = "".join(ch for ch in name if ch.isdigit())
                if len(digits) < 6:
                    continue
                try:
                    y, m = int(digits[:4]), int(digits[4:6])
                except ValueError:
                    continue
                if y * 12 + m < cutoff:
                    Path(p).unlink(missing_ok=True)
                    logger.info("log retention: removed %s", name)
    except Exception:  # noqa: BLE001 — housekeeping must not block
        logger.warning("log retention failed", exc_info=True)


@app.get("/admin/verify")
async def admin_verify_status() -> Response:
    """Scheduler state: the configured schedule, per-target consecutive
    failures, and the last run time."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    return {"schedule": cfg.verify_schedule,
            "state": {k: dict(v) for k, v in _VERIFY_STATE.items()},
            "last_run": _VERIFY_LAST_RUN}


@app.get("/admin/events")
async def admin_events() -> Response:
    """Recent gateway log events (in-memory ring, newest last)."""
    return {"events": list(_event_ring().events)}


# ── Admin: service definitions, models, secrets (console v2) ───────────────

# GL-R02 follow-up (v2.6): model specs can carry receiver-session
# credentials (upload_token, env.GW_UPLOAD_TOKEN) — they must never
# leave the gateway through admin GETs. Redacted everywhere a model
# spec is returned.
_SENSITIVE_MODEL_KEYS = {"upload_token"}
_SENSITIVE_ENV_NAMES = {"GW_UPLOAD_TOKEN"}


def public_model_spec(spec: Any) -> Any:
    """A model spec safe for admin responses: session credentials
    redacted (presence preserved as '***')."""
    if not isinstance(spec, dict):
        return spec
    out = {}
    for k, v in spec.items():
        if k in _SENSITIVE_MODEL_KEYS:
            out[k] = "***redacted***" if v else ""
        elif k == "env" and isinstance(v, dict):
            out[k] = {name: ("***redacted***"
                             if name in _SENSITIVE_ENV_NAMES else val)
                      for name, val in v.items()}
        else:
            out[k] = v
    return out


@app.get("/admin/backends")
async def admin_backends() -> Response:
    """Structured SERVICE DEFINITIONS for the console — every backend with
    its type, connection parameters, and models. Secrets never appear:
    cloud/runpod backends carry only the env-var NAME (api_key_env);
    receiver-session tokens in pod specs are redacted (GL-R02)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    unlicensed = set(_unlicensed_backends(cfg))
    out = []
    for name, b in cfg.backends.items():
        entry: dict[str, Any] = {"name": name, "type": b.get("type")}
        # v2.2 provider entitlement — the console greys these out
        entry["licensed"] = name not in unlicensed
        if b.get("type") == "embedded":
            entry["models"] = {k: public_model_spec(v) for k, v in
                               (b.get("models") or {}).items()}
            entry["embedding_models"] = b.get("embedding_models") or {}
        elif b.get("type") == "llama-server":
            if b.get("base_url"):
                entry["mode"] = "remote"
                entry["base_url"] = b["base_url"]
                entry["api_key_env"] = b.get("api_key_env") or ""
                entry["models"] = {k: public_model_spec(v) for k, v in
                                   (b.get("models") or {}).items()}
            else:
                entry["mode"] = "subprocess"
                for k in ("server_exe", "host", "port_base", "start_timeout_s",
                          "idle_timeout_s", "extra_args"):
                    entry[k] = b.get(k)
                entry["models"] = {k: public_model_spec(v) for k, v in
                                   (b.get("models") or {}).items()}
        elif b.get("type") == "runpod":
            entry["api_key_env"] = b.get("api_key_env") or "RUNPOD_API_KEY"
            entry["models"] = {k: public_model_spec(v) for k, v in
                               (b.get("models") or {}).items()}
        else:  # openai cloud
            entry["base_url"] = b.get("base_url") or "(OpenAI default)"
            entry["api_key_env"] = b.get("api_key_env") or ""
            entry["models"] = {k: public_model_spec(v) for k, v in
                               (b.get("models") or {}).items()}
        out.append(entry)
    return {"backends": out, "queue": cfg.queue, "default_alias": cfg.default_alias,
            "bind": cfg.bind, "port": cfg.port, "models_dir": cfg.models_dir,
            "gateway_keys_configured": bool(cfg.gateway_keys)}


@app.get("/admin/models")
async def admin_models() -> Response:
    """Model inventory for the console: every GGUF on disk (auto-
    discovered, servable by stem) + currently loaded instances."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    engine = get_engine()
    return {"discovered": installed_ggufs(cfg.models_dir),
            "loaded": engine.loaded_models()}


class ModelConfigPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = ""
    gguf: str = ""
    ctx: int | None = None
    placement: str = ""  # '' = default, 'N' = pin card, 'auto' = split
    workers: int | None = None
    parallel: int | None = None  # for llama-server subprocess
    backend: str = "local"  # which backend to add to
    role: str = "chat"  # 'chat' or 'embed'
    delete: bool = False
    # Remote / cloud specific
    remote_id: str = ""  # model ID on remote server (llama-server-remote, openai)
    endpoint_id: str = ""  # runpod serverless endpoint ID
    transport: str = ""  # runpod: "openai" or "runsync"
    timeout_s: int | None = None  # runpod serverless timeout
    pod_id: str = ""  # runpod pod pin


@app.post("/admin/models")
async def admin_upsert_model(payload: ModelConfigPayload) -> Response:
    """Create or update a model configuration entry.

    For embedded backends: {gguf, ctx, placement, workers}
    For llama-server subprocess: {gguf, ctx, placement, parallel}
    Role 'embed' puts it in embedding_models, 'chat' in models.
    """
    cfg = get_gateway_config()
    cfg.maybe_reload()
    name = payload.name.strip()
    if not name:
        return _error(400, "model name must not be empty", param="name")

    if payload.delete:
        _mutate_config(lambda raw: _remove_model_from_config(
            raw, payload.backend, name, payload.role),
            f"model-delete {payload.backend}:{name}")
        return {"deleted": name, "backend": payload.backend}

    # Validate backend exists
    backend = cfg.backends.get(payload.backend)
    if not backend:
        return _error(400, f"backend {payload.backend!r} not found",
                      param="backend")

    # Build model config based on backend type
    model_cfg: dict[str, Any] = {}
    btype = backend.get("type", "")
    bmode = backend.get("mode", "")

    if payload.gguf:
        model_cfg["gguf"] = payload.gguf
    if payload.ctx is not None:
        model_cfg["ctx"] = payload.ctx
    if payload.placement:
        model_cfg["placement"] = payload.placement
    if payload.workers is not None:
        model_cfg["workers"] = payload.workers
    if payload.parallel is not None:
        model_cfg["parallel"] = payload.parallel

    # Remote / cloud specific fields
    if btype == "llama-server" and bmode == "remote":
        if payload.remote_id:
            model_cfg["name"] = payload.remote_id  # remote uses "name" key
    elif btype in (OPENAI_BACKEND, "anthropic", "gemini", "responses",
                   "azure", "vertex", "bedrock"):
        # wire/cloud model: the remote id defaults to OUR name (identity
        # mapping — e.g. an Azure deployment named after the model), so
        # a model saved without remote_id never writes {} (which the
        # config normalizer would drop)
        model_cfg["name"] = payload.remote_id.strip() or name
    elif btype == "runpod":
        if payload.endpoint_id:
            model_cfg["endpoint_id"] = payload.endpoint_id
        if payload.transport and payload.transport != "openai":
            model_cfg["transport"] = payload.transport
        if payload.timeout_s is not None:
            model_cfg["timeout_s"] = payload.timeout_s
        if payload.pod_id:
            model_cfg["pod_id"] = payload.pod_id

    # Determine target dict (models vs embedding_models)
    target_key = "embedding_models" if payload.role == "embed" else "models"

    _mutate_config(lambda raw: _upsert_model_in_config(
        raw, payload.backend, name, model_cfg, target_key),
        f"model-upsert {payload.backend}:{name}")
    return {"saved": name, "backend": payload.backend, "role": payload.role}


def _upsert_model_in_config(raw: dict, backend: str, name: str,
                            model_cfg: dict, target_key: str) -> None:
    """Add or update a model in the config dict."""
    backends = raw.setdefault("backends", {})
    backend_cfg = backends.setdefault(backend, {})
    models = backend_cfg.setdefault(target_key, {})
    models[name] = model_cfg


def _remove_model_from_config(raw: dict, backend: str, name: str,
                              role: str) -> None:
    """Remove a model from the config dict."""
    target_key = "embedding_models" if role == "embed" else "models"
    backends = raw.get("backends", {})
    backend_cfg = backends.get(backend, {})
    models = backend_cfg.get(target_key, {})
    models.pop(name, None)


# ── Admin: backend CRUD ────────────────────────────────────────────────────

class BackendConfigPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = ""
    backend_type: str = "embedded"  # embedded, llama-server, runpod, openai,
    #                                    anthropic, gemini, responses, azure,
    #                                    vertex, runpod-public
    # llama-server subprocess
    server_exe: str = ""
    host: str = ""
    port_base: int | None = None
    start_timeout_s: float | None = None
    idle_timeout_s: float | None = None
    extra_args: str = ""  # space-separated, stored as list
    # remote (llama-server remote, openai, runpod, wire types)
    base_url: str = ""
    api_key_env: str = ""
    # azure classic (resource + api_version) / vertex (project) /
    # bedrock (region)
    resource: str = ""
    api_version: str = ""
    project: str = ""
    region: str = ""
    # delete
    delete: bool = False


@app.post("/admin/backends")
async def admin_upsert_backend(payload: BackendConfigPayload) -> Response:
    """Create or update a backend configuration entry."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    name = payload.name.strip()
    if not name:
        return _error(400, "backend name must not be empty", param="name")

    if payload.delete:
        _mutate_config(lambda raw: raw.get("backends", {}).pop(name, None),
                       f"backend-delete {name}")
        return {"deleted": name}

    # Build backend config
    bcfg: dict[str, Any] = {"type": payload.backend_type}

    if payload.backend_type == LLAMA_SERVER_BACKEND:
        if payload.base_url:
            # Remote mode
            bcfg["base_url"] = payload.base_url.rstrip("/")
            if payload.api_key_env:
                bcfg["api_key_env"] = payload.api_key_env
        else:
            # Subprocess mode
            if payload.server_exe:
                bcfg["server_exe"] = payload.server_exe
            if payload.host:
                bcfg["host"] = payload.host
            if payload.port_base is not None:
                bcfg["port_base"] = payload.port_base
            if payload.start_timeout_s is not None:
                bcfg["start_timeout_s"] = payload.start_timeout_s
            if payload.idle_timeout_s is not None:
                bcfg["idle_timeout_s"] = payload.idle_timeout_s
            if payload.extra_args:
                bcfg["extra_args"] = payload.extra_args.split()
    elif payload.backend_type == RUNPOD_BACKEND:
        bcfg["api_key_env"] = payload.api_key_env or "RUNPOD_API_KEY"
    elif payload.backend_type in (OPENAI_BACKEND, "anthropic", "gemini",
                                  "responses"):
        # openai-SHAPE wire types: base_url + api_key_env. Type-specific
        # defaults (api.anthropic.com, generativelanguage, openai/v1)
        # are filled by the config normalizer when omitted.
        if payload.base_url:
            bcfg["base_url"] = payload.base_url.rstrip("/")
        if payload.api_key_env:
            bcfg["api_key_env"] = payload.api_key_env
    elif payload.backend_type == "azure":
        # Azure OpenAI classic: resource + api_version + api-key header;
        # models (added via /admin/models) map our name → the DEPLOYMENT
        if payload.resource:
            bcfg["resource"] = payload.resource
        if payload.api_version:
            bcfg["api_version"] = payload.api_version
        if payload.api_key_env:
            bcfg["api_key_env"] = payload.api_key_env
    elif payload.backend_type == "vertex":
        # Vertex AI Interactions: project + express key (or ADC)
        if payload.project:
            bcfg["project"] = payload.project
        if payload.api_key_env:
            bcfg["api_key_env"] = payload.api_key_env
    elif payload.backend_type == "bedrock":
        # AWS Bedrock Converse: region + SigV4 env chain (or x-api-key);
        # base_url defaults to the regional bedrock-runtime endpoint
        if payload.region:
            bcfg["region"] = payload.region
        if payload.base_url:
            bcfg["base_url"] = payload.base_url.rstrip("/")
        if payload.api_key_env:
            bcfg["api_key_env"] = payload.api_key_env

    _mutate_config(lambda raw: raw.setdefault("backends", {}).update({name: bcfg}),
                   f"backend-upsert {name}")
    return {"saved": name, "type": payload.backend_type}


# ── Admin: remote model catalog (openai-type cloud backends) ───────────────

async def _fetch_remote_catalog(base_url: str, api_key: str) -> dict[str, Any]:
    """GET {base_url}/models, verbatim JSON (a separate function so tests
    can monkeypatch it). The HF router includes per-provider pricing /
    latency / throughput metadata on each entry — passed through
    untouched for the console's model picker."""
    async with httpx.AsyncClient(timeout=30.0) as hc:
        r = await hc.get(f"{base_url.rstrip('/')}/models",
                         headers={"Authorization": f"Bearer {api_key}"})
        r.raise_for_status()
        return r.json()


@app.get("/admin/backends/{name}/remote-models")
async def admin_remote_models(name: str) -> Response:
    """Live model catalog for an openai-type cloud backend: proxies the
    backend's GET {base_url}/models SERVER-SIDE so the API key never
    reaches the browser (same secrecy rule as /admin/secrets). Works for
    any OpenAI-compatible provider (HF router, Groq, OpenRouter...)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = cfg.backends.get(name)
    if not backend:
        return _error(404, f"backend {name!r} not found", param="backend")
    # v2.2 don't-display: an unlicensed backend's catalog is not
    # offered — indistinguishable from a backend that doesn't exist
    if not licensing.provider_allowed(name, backend.get("type") or ""):
        return _error(404, f"backend {name!r} not found", param="backend")
    if backend.get("type") != "openai":
        return _error(400, f"backend {name!r} is not an openai-type "
                           f"cloud backend", param="backend")
    key_env = str(backend.get("api_key_env") or "").strip()
    api_key = cloud.resolve_backend_key(backend)
    if key_env and not api_key:
        return _error(502, f"api key env var {key_env!r} is not set and the "
                           f"OS keyring has no '{key_env}' entry",
                      code="auth")
    try:
        data = await _fetch_remote_catalog(
            str(backend.get("base_url") or ""), api_key)
    except httpx.HTTPStatusError as e:
        return _exec_error_response(e, f"{name}:models", False,
                                    time.perf_counter())
    except httpx.RequestError as e:
        return _error(502, f"catalog fetch failed: {e}", code="network")
    models = data.get("data") if isinstance(data, dict) else None
    if not isinstance(models, list):
        models = []
    # Opportunistic pricing capture — the HF router's providers metadata
    # carries per-provider $/Mtok rates; persisting them powers cost
    # estimation and per-key USD budgets. Best-effort.
    _capture_prices(name, models)
    return {"backend": name, "base_url": backend.get("base_url"),
            "count": len(models), "models": models}


# ── Admin: virtual (per-consumer) API keys ─────────────────────────────────

class ApiKeyPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = ""
    owner: str = ""
    note: str = ""
    monthly_token_budget: int | None = None
    monthly_usd_budget: float | None = None
    models: list[str] | None = None  # allowlist (empty = all)
    rate_limit_rpm: int | None = None
    rate_limit_tpm: int | None = None
    delete: bool = False


@app.get("/admin/keys")
async def admin_keys() -> Response:
    """Virtual key inventory + live usage (the consuming key's identity
    rides in every request log line; usage sums the current month).
    Keys are hashed at rest — only a 16-char hash prefix shows here; the
    full key is returned exactly once at creation."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    month = _month_key()
    rows = []
    for kid, e in sorted((cfg.api_keys or {}).items()):
        spend = _scope_spend(f"key:{kid}")
        rows.append({
            "id": kid, "owner": e.get("owner") or "",
            "note": e.get("note") or "",
            "key_hash": str(e.get("key_hash") or "")[:16],
            "monthly_token_budget": int(e.get("monthly_token_budget") or 0),
            "monthly_usd_budget": float(e.get("monthly_usd_budget") or 0),
            "models": list(e.get("models") or []),
            "rate_limit_rpm": int(e.get("rate_limit_rpm") or 0),
            "rate_limit_tpm": int(e.get("rate_limit_tpm") or 0),
            "used_tokens": spend["tokens"],
            "used_usd": round(spend["usd"], 6),
            "month": month,
        })
    return {"api_keys": rows,
            "admin_keys_configured": bool(cfg.gateway_keys)}


@app.post("/admin/keys")
async def admin_upsert_key(payload: ApiKeyPayload) -> Response:
    """Create or revoke a virtual key. Creating one ENABLES auth (any key
    configured → bearer required), so an admin key (gateway_keys) must
    exist first — otherwise the admin surface locks itself out."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    kid = payload.id.strip()
    if not kid or not re.fullmatch(r"[A-Za-z0-9._-]{1,40}", kid):
        return _error(400, "key id: letters/digits/dots/dashes/underscores, "
                           "max 40 chars", param="id")
    if payload.delete:
        def _del(raw: dict) -> None:
            raw["api_keys"] = [k for k in (raw.get("api_keys") or [])
                               if str((k or {}).get("id")) != kid]
        _mutate_config(_del, f"api-key-revoke {kid}")
        return {"revoked": kid}
    # v2.2 scale caps: creating beyond the licensed key count refuses
    cap_ok, cap_limit = licensing.cap_allows(
        "keys", len(cfg.api_keys or {}))
    if not cap_ok:
        return _error(403, f"license cap: virtual-key limit "
                      f"({cap_limit}) reached — this deployment's "
                      "license caps its key count", code="license_caps")
    if not cfg.gateway_keys:
        return _error(400, "configure a gateway ADMIN key first "
                           "(gateway_keys) — creating virtual keys enables "
                           "auth and would otherwise lock out the admin "
                           "surface", param="gateway_keys")
    entry: dict[str, Any] = {
        "id": kid,
        # hashed at rest; the plaintext returns exactly ONCE below
        "key_hash": _sha256_hex(f"vk-{secrets.token_urlsafe(24)}"),
        "owner": payload.owner.strip(),
        "note": payload.note.strip(),
        "monthly_token_budget": max(0, int(payload.monthly_token_budget or 0)),
        "monthly_usd_budget": max(0.0, float(payload.monthly_usd_budget or 0)),
        "models": [str(m).strip() for m in (payload.models or [])
                   if str(m).strip()],
        "rate_limit_rpm": max(0, int(payload.rate_limit_rpm or 0)),
        "rate_limit_tpm": max(0, int(payload.rate_limit_tpm or 0)),
    }
    # mint the plaintext key whose hash we stored
    plaintext = f"vk-{secrets.token_urlsafe(24)}"
    entry["key_hash"] = _sha256_hex(plaintext)

    def _upsert(raw: dict) -> None:
        lst = [k for k in (raw.get("api_keys") or [])
               if str((k or {}).get("id")) != kid]
        lst.append(entry)
        raw["api_keys"] = lst

    _mutate_config(_upsert, f"api-key-create {kid}")
    # The full key returns ONCE here; only its hash is stored.
    return {"created": kid, "key": plaintext,
            "monthly_token_budget": entry["monthly_token_budget"],
            "monthly_usd_budget": entry["monthly_usd_budget"]}


@app.get("/admin/secrets")
async def admin_secrets() -> Response:
    """API-key STATUS for the console — which env vars the configured
    backends need, and whether each is available (env, the console's
    managed store, OS keyring via the boot bridge, or missing). NEVER
    returns key values."""
    import os

    cfg = get_gateway_config()
    cfg.maybe_reload()
    needed: dict[str, list[str]] = {}
    for bname, b in cfg.backends.items():
        env = b.get("api_key_env")
        if env:
            needed.setdefault(env, []).append(bname)
    rows = []
    for env_name, backends in sorted(needed.items()):
        in_env = bool(os.environ.get(env_name))
        in_managed = (not in_env
                      and bool(cloud.managed_secret(env_name)))
        in_keyring = False
        if not in_env and not in_managed:
            try:
                import keyring
                in_keyring = bool(keyring.get_password("Workshop", env_name))
            except Exception:
                in_keyring = False
        rows.append({
            "name": env_name,
            "backends": backends,
            "source": ("env" if in_env else
                       "console" if in_managed else
                       "keyring" if in_keyring else "missing"),
        })
    return {"api_keys": rows,
            "gateway_keys_configured": bool(cfg.gateway_keys),
            "hint": ("missing keys 502 on first use — paste the "
                     "credential below (console-managed), or supply it "
                     "via the container environment (env var) / a "
                     "mounted secret file (api_key_file)")}


class SecretPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = ""
    value: str = ""
    delete: bool = False


@app.post("/admin/secrets")
async def admin_set_secret(payload: SecretPayload) -> Response:
    """Set (or remove) a provider credential in the console-managed
    store (data/secrets.json — never the config, never logged, never
    returned by GET). Resolution: env var first, then this store, then
    api_key_file/api_key_cmd/keyring — a pasted key works immediately
    (read per-request), no restart. Audit-logged by NAME only."""
    name = payload.name.strip()
    if not name or not name.replace("_", "").isalnum() \
            or name.upper() != name:
        return _error(400, "name must be an ENV_VAR style identifier "
                      "(A-Z, 0-9, _)", param="name")
    if payload.delete:
        existed = cloud.delete_managed_secret(name)
        _audit_config(f"secret-remove {name}")
        return {"removed": name, "existed": existed}
    if not payload.value.strip():
        return _error(400, "value must not be empty (use delete: true "
                      "to remove)", param="value")
    ok = cloud.set_managed_secret(name, payload.value.strip())
    if not ok:
        return _error(500, "secret store not writable", code="storage")
    _audit_config(f"secret-set {name}")   # NAME only — never the value
    return {"stored": name, "note": "resolves on the next request — "
            "no restart needed"}


# ── Admin: recovery actions ────────────────────────────────────────────────

class StopPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    backend: str = ""
    model: str = ""
    all: bool = False  # noqa: A003
    terminate: bool = False


@app.post("/admin/llama-server/stop")
async def admin_llama_server_stop(payload: StopPayload) -> Response:
    """Recovery: stop managed llama-server children ({backend, model} or
    {all: true}). The next request respawns lazily."""
    if payload.all:
        llamaserver.kill_all()
        return {"stopped": "all"}
    if not payload.backend or not payload.model:
        return _error(400, "backend and model required (or {\"all\": true})")
    mgr = llamaserver._MANAGERS.get((payload.backend, payload.model))
    if mgr is None:
        return _error(404, f"no managed llama-server child for "
                      f"{payload.backend}:{payload.model}", code="not_found")
    mgr.stop()
    return {"stopped": f"{payload.backend}:{payload.model}"}


@app.post("/admin/runpod/stop")
async def admin_runpod_stop(payload: StopPayload) -> Response:
    """Recovery: stop (or terminate) a managed runpod pod. terminate=true
    frees all storage — the next request redeploys from scratch."""
    if not payload.backend or not payload.model:
        return _error(400, "backend and model required")
    mgr = runpod._MANAGERS.get((payload.backend, payload.model))
    if mgr is None:
        return _error(404, f"no managed pod for "
                      f"{payload.backend}:{payload.model}", code="not_found")
    await mgr.stop(terminate=payload.terminate)
    return {"stopped": f"{payload.backend}:{payload.model}",
            "terminated": payload.terminate}


class RunPodActionPayload(BaseModel):
    """Payload for runpod lifecycle actions: pods (start/stop/terminate
    by pod_id) and serverless endpoints (delete by endpoint_id)."""
    model_config = ConfigDict(extra="ignore")
    pod_id: str = ""
    endpoint_id: str = ""
    action: str = ""  # pods: start, stop, terminate; endpoints: delete


@app.post("/admin/runpod/action")
async def admin_runpod_action(payload: RunPodActionPayload) -> Response:
    """Direct lifecycle action on any runpod entity (managed or BYO).
    Pods: start (resume stopped), stop (keep storage), terminate (delete).
    Serverless endpoints: delete — permanent; workers, queued jobs and
    the bound template go with it."""
    if not payload.pod_id and not payload.endpoint_id:
        return _error(400, "pod_id or endpoint_id required")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    rp_backends = {n: b for n, b in cfg.backends.items() if b.get("type") == "runpod"}
    if not rp_backends:
        return _error(400, "no runpod backends configured")
    api_key_env = list(rp_backends.values())[0].get("api_key_env", "RUNPOD_API_KEY")
    try:
        if payload.endpoint_id:
            if payload.action != "delete":
                return _error(400, "unknown action: endpoint actions are "
                                   "delete")
            await runpod.delete_endpoint(payload.endpoint_id, api_key_env)
            return {"ok": True, "endpoint_id": payload.endpoint_id,
                    "action": "delete"}
        if payload.action == "start":
            await runpod.start_pod(payload.pod_id, api_key_env)
        elif payload.action == "stop":
            await runpod.stop_pod(payload.pod_id, api_key_env)
        elif payload.action == "terminate":
            # our own termination must not read as an unexpected death
            lifecycle.mark_stopped(payload.pod_id)
            await runpod.terminate_pod(payload.pod_id, api_key_env)
        else:
            return _error(400, f"unknown action: {payload.action!r}. Use start/stop/terminate")
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    return {"ok": True, "pod_id": payload.pod_id, "action": payload.action}


# ── Admin: v3 pod lifecycle ─────────────────────────────────────────────────

@app.post("/admin/runpod/stop-all")
async def admin_runpod_stop_all() -> Response:
    """PANIC: stop every RUNNING runpod pod across all backends (stop,
    not terminate — pods stay resumable). In-flight requests break by
    design; volume/storage spend continues (it's storage, not compute).
    Covers the whole account — BYO pods included."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if not rp_backends:
        return _error(400, "no runpod backends configured")
    stopped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for name, b in rp_backends.items():
        env = b.get("api_key_env", "RUNPOD_API_KEY")
        try:
            pods = await runpod.list_pods(env)
        except runpod.RunPodError as e:
            errors.append({"backend": name, "error": str(e)})
            continue
        for pod in pods if isinstance(pods, list) else []:
            if runpod.pod_state(pod) not in ("RUNNING", "READY", "UNKNOWN"):
                continue
            pid = pod.get("id")
            try:
                await runpod.stop_pod(pid, env)
                stopped.append({"backend": name, "pod_id": pid,
                                "name": pod.get("name")})
            except runpod.RunPodError as e:
                errors.append({"backend": name, "pod_id": pid,
                               "error": str(e)})
    if stopped:
        lifecycle._emit("panic_stop", "",
                        f"panic button: stopped {len(stopped)} running "
                        f"pod(s)", severity="warn")
    return {"ok": not errors, "stopped": stopped, "errors": errors}


class LifecycleSettingsPayload(BaseModel):
    """Per-model lifecycle knobs + the global monthly cap. Model keys
    are "backend|model" (same shape as month_to_date.per_model)."""
    model_config = ConfigDict(extra="ignore")

    budget_usd_month: float | None = None          # 0 disables the cap
    model_budgets: dict[str, float] | None = None  # "b|m" -> USD, 0 off
    model_auto_recover: dict[str, str] | None = None  # "b|m" -> alert|redeploy
    clear_alerts: bool = False


@app.get("/admin/lifecycle")
async def admin_lifecycle() -> Response:
    """Lifecycle panel data: watchdog state, cached month-to-date,
    per-model rules, recent alerts. Cached snapshot only — this never
    triggers a billing/pods fetch (D8)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    rules: list[dict[str, Any]] = []
    for name, b in cfg.backends.items():
        if b.get("type") != "runpod":
            continue
        for mkey, mspec in (b.get("models") or {}).items():
            if isinstance(mspec, dict) and not mspec.get("endpoint_id"):
                rules.append({
                    "backend": name, "model": mkey,
                    "alias": mspec.get("alias", mkey),
                    "idle_stop_s": mspec.get("idle_stop_s", 0),
                    "idle_terminate_s": mspec.get("idle_terminate_s", 0),
                    "monthly_budget_usd": mspec.get("monthly_budget_usd", 0),
                    "auto_recover": mspec.get("auto_recover", "alert"),
                    "est_cost_per_hr": mspec.get("est_cost_per_hr", 0),
                })
    return {
        "watchdog": {"running": lifecycle.watchdog_running(),
                     "interval_s": lifecycle.WATCHDOG_INTERVAL_S},
        "budget_usd_month": getattr(cfg, "budget_usd_month", 0),
        "month_to_date": lifecycle.budget_snapshot(),
        "rules": rules,
        "alerts": lifecycle.alerts(50),
    }


@app.post("/admin/lifecycle/settings")
async def admin_lifecycle_settings(
        payload: LifecycleSettingsPayload) -> Response:
    """Set the global monthly cap + per-model budget/auto-recover
    (persisted to gateway_config.json, hot-reloaded). Returns the new
    panel state so the console can re-render in one round trip."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    if payload.clear_alerts:
        lifecycle.clear_alerts()

    def _mspec(raw: dict, key: str) -> dict | None:
        b, _, mkey = str(key).partition("|")
        spec = (raw.get("backends", {}).get(b, {})
                .get("models", {}).get(mkey))
        return spec if isinstance(spec, dict) else None
    def mutator(raw: dict):
        if payload.budget_usd_month is not None:
            raw["budget_usd_month"] = max(
                0.0, float(payload.budget_usd_month))
        if payload.model_budgets:
            for key, val in payload.model_budgets.items():
                spec = _mspec(raw, key)
                if spec is not None:
                    try:
                        spec["monthly_budget_usd"] = max(0.0, float(val or 0))
                    except (TypeError, ValueError):
                        pass
        if payload.model_auto_recover:
            for key, val in payload.model_auto_recover.items():
                spec = _mspec(raw, key)
                if spec is not None and val in ("alert", "redeploy"):
                    spec["auto_recover"] = val

    _mutate_config(mutator)
    # stale budget cache could mask the new caps for up to 60s — drop it
    lifecycle.reset_budget_cache()
    try:
        await lifecycle.month_to_date(cfg)   # freshen for the response
    except Exception:  # noqa: BLE001 — panel state is best-effort
        pass
    return await admin_lifecycle()


class RunPodTestPayload(BaseModel):
    """Full remote-GPU diagnostic: ensure the pod (deploy / resume /
    reuse), health-probe, one test chat, container logs — a structured
    per-phase report. A failed test is a SUCCESSFUL diagnostic (HTTP 200
    with ok:false + phase errors), so scripts can act on the detail."""
    model_config = ConfigDict(extra="ignore")
    backend: str = ""
    model: str = ""
    prompt: str = "Reply with exactly: OK"
    max_tokens: int = 16
    timeout_s: float | None = None   # overrides spec start_timeout_s
    after: str = "keep"              # keep | stop | terminate
    deploy: bool = True              # False = probe-only, never deploys/starts
    logs: bool = True
    log_tail: int = 100


@app.post("/admin/runpod/test")
async def admin_runpod_test(payload: RunPodTestPayload) -> Response:
    """Test a runpod POD model end-to-end with diagnostics reported back:
    pod (find/deploy/resume, timings, pod info) → health (per-probe
    status/latency + served model ids) → chat (content + usage) →
    container logs (best-effort, tail) → optional stop/terminate after."""
    if not payload.backend or not payload.model:
        return _error(400, "backend and model required")
    if payload.after not in ("keep", "stop", "terminate"):
        return _error(400, "after must be keep|stop|terminate")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = cfg.backends.get(payload.backend)
    if not backend or backend.get("type") != "runpod":
        return _error(400, f"backend {payload.backend!r} is not a runpod backend")
    mspec = (backend.get("models") or {}).get(payload.model)
    if mspec is None:
        return _error(400, f"unknown model {payload.model!r} on backend "
                           f"{payload.backend!r}")
    if mspec.get("endpoint_id"):
        return _error(400, f"model {payload.model!r} is a serverless endpoint "
                           "— no pod to test (send a chat request instead)")

    api_key_env = backend.get("api_key_env", "RUNPOD_API_KEY")
    mgr = runpod.get_manager(cfg, payload.backend, payload.model)
    if payload.deploy and not mgr.is_ready():
        try:
            await _budget_gate(cfg, payload.backend, payload.model)
        except lifecycle.LifecycleBudgetExceeded as e:
            return _error(503, str(e), err_type="overloaded_error",
                          code="budget_exceeded")
    t_total = time.monotonic()
    res: dict[str, Any] = {"backend": payload.backend, "model": payload.model,
                           "ok": False, "phases": {}, "error": None}

    def _fail(phase: str, err: str) -> None:
        res["phases"][phase] = {"ok": False, "error": err}
        res["error"] = f"{phase}: {err}"

    # ── phase: pod (find / deploy / resume / reuse) ──────────────────
    base: str | None = None
    t0 = time.monotonic()
    try:
        if payload.deploy:
            base = await mgr.ensure_ready(payload.timeout_s)
            res["phases"]["pod"] = {
                "ok": True, "action": mgr.last_action or "ready",
                "elapsed_s": round(time.monotonic() - t0, 2)}
        else:
            pod = await mgr.find_pod()
            if pod is None:
                _fail("pod", "not deployed — probe-only mode never deploys")
            else:
                mgr.pod_id = str(pod.get("id"))
                mgr.last_pod_info = runpod.pod_info(pod)
                mgr.last_action = "probed"
                state = runpod.pod_state(pod)
                mgr.pod_status = state
                res["phases"]["pod"] = {"ok": True, "action": "probed",
                                        "state": state,
                                        "elapsed_s": round(time.monotonic() - t0, 2)}
                if state in ("RUNNING", "READY", "UNKNOWN"):
                    base = mgr.base_url()
                else:
                    _fail("pod", f"pod is {state} — probe-only mode never "
                                 "starts it")
    except runpod.RunPodError as e:
        _fail("pod", str(e))
    if mgr.pod_id:
        pod_phase = res["phases"].setdefault(
            "pod", {"ok": False, "error": "unknown"})
        pod_phase["pod_id"] = mgr.pod_id
        if mgr.last_pod_info:
            pod_phase["pod"] = mgr.last_pod_info

    # ensure_ready failed or probe-only found a stopped pod — if a pod id
    # is known, STILL probe it: the pod may have come up just after the
    # timeout, and the report should say so rather than give up.
    if base is None and mgr.pod_id:
        base = mgr.base_url()

    # ── phase: health probe ─────────────────────────────────────────
    if base:
        t0 = time.monotonic()
        try:
            det = await runpod.pod_health_details(base)
            res["phases"]["health"] = {
                "ok": bool(det.get("healthy")),
                "base_url": base,
                "elapsed_s": round(time.monotonic() - t0, 2), **det}
        except Exception as e:  # noqa: BLE001 — diagnostics must not 500
            _fail("health", repr(e))
    else:
        res["phases"]["health"] = {"ok": False, "skipped": "no base url"}

    # ── phase: test chat ────────────────────────────────────────────
    if base and res["phases"].get("health", {}).get("ok"):
        t0 = time.monotonic()
        try:
            chat = await llamaserver.post_chat(base, {
                "model": mgr.spec.get("alias") or payload.model,
                "messages": [{"role": "user", "content": payload.prompt}],
                "max_tokens": payload.max_tokens, "temperature": 0,
            }, float(payload.timeout_s or cfg.queue["timeout_s"]))
            msg = ((chat.get("choices") or [{}])[0].get("message") or {})
            res["phases"]["chat"] = {
                "ok": True, "elapsed_s": round(time.monotonic() - t0, 2),
                "content": (msg.get("content") or "")[:200],
                "usage": chat.get("usage") or {}}
        except Exception as e:  # noqa: BLE001 — diagnostics must not 500
            _fail("chat", str(e))
    else:
        res["phases"]["chat"] = {"ok": False, "skipped":
                                 "pod not healthy" if base else "no base url"}

    res["ok"] = bool(res["phases"].get("chat", {}).get("ok"))

    # ── phase: container logs (best-effort, always attempted) ───────
    if payload.logs and mgr.pod_id:
        try:
            res["logs"] = await runpod.fetch_pod_logs(
                mgr.pod_id, api_key_env, payload.log_tail)
        except Exception as e:  # noqa: BLE001 — logs are a bonus
            res["logs"] = []
            res["logs_error"] = str(e)
    else:
        res["logs"] = []

    # ── after-action (keep | stop | terminate) ──────────────────────
    if payload.after in ("stop", "terminate") and mgr.pod_id:
        await mgr.stop(terminate=payload.after == "terminate")
        res["after"] = payload.after

    res["elapsed_s"] = round(time.monotonic() - t_total, 2)
    logger.info("runpod test [%s/%s]: ok=%s %.1fs (action=%s)",
                payload.backend, payload.model, res["ok"],
                res["elapsed_s"], mgr.last_action)
    return res


class RunPodDeployPayload(BaseModel):
    """Payload for deploying a new runpod pod."""
    model_config = ConfigDict(extra="ignore")
    backend: str = ""
    model: str = ""
    # Pod spec fields (snake_case, matches gateway config)
    gpu_type_ids: list[str] | None = None
    image: str = ""
    docker_cmd: str = ""
    docker_args: str = ""
    docker_entrypoint: list[str] | None = None
    ports: str = ""
    port: int = 8080
    container_disk_gb: int = 10
    volume_gb: int = 0
    volume_mount: str = "/workspace"
    spot: bool = False
    env: dict[str, str] | None = None
    start_timeout_s: float = 600
    idle_stop_s: float = 0
    idle_terminate_s: float = 0
    alias: str = ""


@app.post("/admin/runpod/deploy")
async def admin_runpod_deploy(payload: RunPodDeployPayload) -> Response:
    """Deploy a new runpod pod and add it to config.
    If model name already exists in the backend, it replaces the config entry."""
    if not payload.backend or not payload.model:
        return _error(400, "backend and model required")
    if not payload.image:
        return _error(400, "image required")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    rp_backends = {n: b for n, b in cfg.backends.items() if b.get("type") == "runpod"}
    if payload.backend not in rp_backends:
        return _error(400, f"backend {payload.backend!r} is not a runpod backend")
    api_key_env = rp_backends[payload.backend].get("api_key_env", "RUNPOD_API_KEY")
    # Build pod spec
    spec: dict[str, Any] = {
        "image": payload.image,
        "ports": payload.ports or f"{payload.port}/http",
        "port": payload.port,
        "container_disk_gb": payload.container_disk_gb,
        "start_timeout_s": payload.start_timeout_s,
        "idle_stop_s": payload.idle_stop_s,
        "idle_terminate_s": payload.idle_terminate_s,
    }
    if payload.gpu_type_ids:
        spec["gpu_type_ids"] = payload.gpu_type_ids
    if payload.docker_cmd:
        spec["docker_cmd"] = payload.docker_cmd
    elif payload.docker_args:
        spec["docker_args"] = payload.docker_args
    if payload.docker_entrypoint:
        spec["docker_entrypoint"] = payload.docker_entrypoint
    if payload.volume_gb:
        spec["volume_gb"] = payload.volume_gb
        spec["volume_mount"] = payload.volume_mount
    if payload.spot:
        spec["spot"] = True
    if payload.env:
        spec["env"] = payload.env
    if payload.alias:
        spec["alias"] = payload.alias
    # Build deploy payload and send to RunPod
    deploy_payload = runpod.deploy_payload(payload.backend, payload.model, spec)
    try:
        pod = await runpod.deploy_pod(deploy_payload, api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    pod_id = pod.get("id")
    # Add pod_id to spec so the gateway manages it
    spec["pod_id"] = pod_id
    # Update config
    def mutator(raw: dict):
        models = raw.setdefault("backends", {}).setdefault(payload.backend, {}).setdefault("models", {})
        models[payload.model] = spec
    _mutate_config(mutator)
    return {"ok": True, "pod_id": pod_id, "backend": payload.backend,
            "model": payload.model, "name": pod.get("name")}


# ── Model catalogue + deploy wizard ────────────────────────────────────────

class CatalogDeployPayload(BaseModel):
    """Deploy a catalogue recipe with dropdown-chosen options. Every
    option is validated against LIVE RunPod catalog data before
    anything is created."""
    model_config = ConfigDict(extra="ignore")
    backend: str = ""
    recipe_id: str = ""
    gpu_id: str = ""
    gpu_count: int = 0
    cloud: str = "secure"          # secure | community (community = spot)
    data_center_id: str = ""
    volume_mode: str = "none"      # none | existing | create
    volume_id: str = ""
    volume_size_gb: int = 0
    volume_type: str = "STANDARD"  # STANDARD | HIGH_PERFORMANCE
    ctx_key: str = ""
    container_disk_gb: int = 0
    idle_stop_s: float = 1800       # 0 disables auto-stop
    idle_terminate_s: float = 0
    alias: str = ""


def _runpod_api_key_env(cfg) -> tuple[str, str]:
    """First runpod backend name + its api_key_env (wizard default)."""
    rp = {n: b for n, b in cfg.backends.items() if b.get("type") == "runpod"}
    name = next(iter(rp)) if rp else ""
    env = rp.get(name, {}).get("api_key_env", "RUNPOD_API_KEY") if name else "RUNPOD_API_KEY"
    return name, env


@app.get("/admin/catalog")
async def admin_catalog() -> Response:
    """Curated recipes + live RunPod options (GPUs with prices,
    data centers with volume support, account volumes) — the deploy
    wizard's dropdown data. GPU/DC lists are cached 5 min."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend, api_key_env = _runpod_api_key_env(cfg)
    if not backend:
        return _error(400, "no runpod backend configured")
    try:
        opts = await catalog.live_options(api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    return {"recipes": catalog.public_recipes(),
            "backend": backend, **opts}


@app.post("/admin/catalog/deploy")
async def admin_catalog_deploy(payload: CatalogDeployPayload) -> Response:
    """Validate recipe + choices against live options, create the
    network volume if requested, deploy the pod, save the spec."""
    if not payload.recipe_id:
        return _error(400, "recipe_id required")
    recipe = catalog.find_recipe(payload.recipe_id)
    if recipe is None:
        return _error(400, f"unknown recipe {payload.recipe_id!r}")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if backend not in rp_backends:
        return _error(400, f"backend {backend!r} is not a runpod backend")
    api_key_env = rp_backends[backend].get("api_key_env", "RUNPOD_API_KEY")

    try:
        opts = await catalog.live_options(api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")

    choices: dict[str, Any] = {
        "gpu_id": payload.gpu_id,
        "gpu_count": payload.gpu_count,
        "cloud": payload.cloud,
        "data_center_id": payload.data_center_id,
        "volume_mode": payload.volume_mode,
        "volume_id": payload.volume_id,
        "volume_size_gb": payload.volume_size_gb,
        "volume_type": payload.volume_type,
        "ctx_key": payload.ctx_key,
        "container_disk_gb": payload.container_disk_gb,
        "idle_stop_s": payload.idle_stop_s,
        "alias": payload.alias,
    }
    # Existing volume: the pod MUST run in the volume's DC — derive it
    # when the client didn't send one (network volumes are DC-pinned).
    if payload.volume_mode == "existing" and payload.volume_id \
            and not payload.data_center_id:
        vol = next((v for v in opts["volumes"]
                    if v.get("id") == payload.volume_id), None)
        if vol:
            choices["data_center_id"] = vol.get("dataCenter") or ""

    errors = catalog.validate_selection(recipe, choices, opts["gpus"],
                                        opts["datacenters"],
                                        opts["volumes"])
    if errors:
        return JSONResponse(status_code=422, content={
            "detail": {"code": "invalid_selection", "errors": errors}})

    # Volume creation happens after validation (size/type/DC checked)
    if payload.volume_mode == "create":
        vol_name = f"gw-{recipe['id']}-{payload.data_center_id}".lower()[:60]
        try:
            vol = await runpod.create_volume(
                vol_name, payload.data_center_id,
                payload.volume_size_gb, payload.volume_type, api_key_env)
        except runpod.RunPodError as e:
            return _error(502, f"volume create failed: {e}",
                          code="runpod_error")
        if not isinstance(vol, dict) or not vol.get("id"):
            return _error(502, f"volume create returned no id: "
                               f"{str(vol)[:200]}", code="runpod_error")
        choices["volume_id"] = vol["id"]

    spec = catalog.build_spec(recipe, choices)
    model_key = spec.get("alias") or recipe["id"]
    # Capacity fallback: try the user's GPU combo, then the recipe's
    # other recommended combos (same DC — volumes are DC-pinned). All
    # failures are collected so the console can show what was tried.
    candidates = catalog.candidate_combos(recipe, choices, opts["gpus"])
    if not candidates:
        return _error(400, "no deployable GPU combination for this recipe "
                          "and cloud", code="no_candidates")
    tried: list[dict[str, Any]] = []
    pod: dict[str, Any] | None = None
    landed: dict[str, Any] | None = None
    for cand in candidates:
        spec["gpu_type_ids"] = [cand["gpu_id"]]
        spec["gpu_count"] = cand["gpu_count"]
        deploy_payload = runpod.deploy_payload(backend, model_key, spec)
        try:
            pod = await runpod.deploy_pod(deploy_payload, api_key_env)
            landed = cand
            break
        except runpod.RunPodError as e:
            tried.append({**cand, "error": str(e)[:250]})
    if pod is None:
        return JSONResponse(status_code=502, content={
            "error": {"message": "deploy failed for every GPU combination "
                                 f"({len(tried)} tried) — capacity or API "
                                 "error; see 'tried'",
                      "code": "runpod_deploy_failed"},
            "tried": tried})
    pod_id = pod.get("id")
    spec["pod_id"] = pod_id
    # v3: persist the LANDED price — live budget accrual reads it
    if (landed or {}).get("est_cost_per_hr"):
        spec["est_cost_per_hr"] = float(landed["est_cost_per_hr"])

    def mutator(raw: dict):
        models = raw.setdefault("backends", {}).setdefault(
            backend, {}).setdefault("models", {})
        models[model_key] = spec

    _mutate_config(mutator)
    return {"ok": True, "pod_id": pod_id, "backend": backend,
            "model": model_key, "name": pod.get("name"),
            "est_cost_per_hr": (landed or {}).get("est_cost_per_hr"),
            "landed": landed, "tried": tried}


# ── Model finder: search + file listing (v4) ────────────────────────────────

@app.get("/admin/models/search")
async def admin_models_search(q: str = "", source: str = "all",
                              task: str = "", limit: int = 10) -> Response:
    """Find-a-model: task-matched catalogue recipes (shown first) +
    repo search across HF / ModelScope / Ollama. Empty q → recipes
    only. task filters BOTH lists by capability (the console's task
    chooser — business = chat AND deep_think)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    if not q and not task:
        return _error(400, "q or task required")
    results: list[dict[str, Any]] = []
    if q:
        limit = 10 if not 1 <= limit <= 30 else limit
        try:
            results = await modelsearch.search(q, source=source,
                                               limit=limit)
        except Exception as e:  # noqa: BLE001 — one bad source ≠ failure
            logger.warning("model search failed: %s", e)
        if task:
            results = [r for r in results if capabilities.task_match(
                r.get("capabilities", []), task)]
    ql = q.lower()
    recipes = [r for r in catalog.public_recipes()
               if (not task or capabilities.task_match(
                   r.get("capabilities", []), task))
               and (not ql or ql in (str(r.get("model", ""))
                                     + r["id"]).lower())]
    return {"recipes": recipes, "results": results,
            "tasks": capabilities.TASKS}


@app.get("/admin/models/files")
async def admin_models_files(source: str = "",
                             repo: str = "") -> Response:
    """Pickable GGUF quants (+ mmproj files) for one search result —
    real sizes and direct download URLs for the custom-model deploy."""
    if not repo:
        return _error(400, "repo required")
    try:
        return await modelsearch.list_files(source, repo)
    except ValueError as e:
        return _error(400, str(e))
    except Exception as e:  # noqa: BLE001 — upstream errors → 502
        return _error(502, f"listing {source}:{repo} failed: {e}",
                      code="modelsource_error")


@app.get("/admin/models/plan")
async def admin_models_plan(size_gb: float = 0, mmproj_gb: float = 0,
                            gpu_id: str = "", gpu_count: int = 1,
                            cloud: str = "secure") -> Response:
    """Simple-mode brain: given the picked file(s) + GPU combo, the
    fitting ctx presets (KV math), the auto default, disk minimum and
    estimated price."""
    if size_gb <= 0:
        return _error(400, "size_gb required")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend, api_key_env = _runpod_api_key_env(cfg)
    if not backend:
        return _error(400, "no runpod backend configured")
    try:
        opts = await catalog.live_options(api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    g = next((g for g in opts["gpus"] if g.get("id") == gpu_id), None)
    if g is None:
        return _error(400, f"unknown GPU {gpu_id!r}")
    cloud_key = "community" if cloud == "community" else "secure"
    price = (g.get("price") or {}).get(cloud_key)
    count = max(1, gpu_count)
    vram = float(g.get("memory") or 0) * count
    total_gb = size_gb + mmproj_gb
    presets = catalog.custom_ctx_options(total_gb, vram)
    return {"gpu_id": gpu_id, "gpu_count": count,
            "vram_total_gb": vram,
            "weights_vram_gb": round(catalog.weights_vram_gb(total_gb), 1),
            "ctx_options": presets,
            "ctx_default": presets[-1]["key"] if presets else None,
            "disk_min_gb": max(20, int(total_gb * 1.2) + 10),
            "est_cost_per_hr": (round(float(price) * count, 2)
                                if price else None),
            "fits": bool(presets)}


class CustomDeployPayload(BaseModel):
    """Deploy an arbitrary searched model. file_url/mmproj_url come
    from GET /admin/models/files (trusted-admin surface: the pod curls
    them as sent — same trust level as docker_cmd)."""
    model_config = ConfigDict(extra="ignore")

    backend: str = ""
    source: str = ""                 # hf | modelscope | ollama
    repo_id: str = ""
    file_name: str = ""
    file_url: str = ""
    size_gb: float = 0
    mmproj_name: str = ""
    mmproj_url: str = ""
    mmproj_size_gb: float = 0
    sha256: str = ""               # GL-R25: verified at boot when set
    mmproj_sha256: str = ""
    gpu_id: str = ""
    gpu_count: int = 1
    cloud: str = "secure"
    data_center_id: str = ""
    volume_mode: str = "none"        # none | create
    volume_size_gb: int = 0
    volume_type: str = "STANDARD"
    ctx_key: str = ""
    container_disk_gb: int = 0
    idle_stop_s: float = 1800
    alias: str = ""


@app.post("/admin/models/deploy-custom")
async def admin_models_deploy_custom(
        payload: CustomDeployPayload) -> Response:
    """Deploy a searched model (v4 D3/D4): size-validated GPU choice
    with price-ascending capacity fallback (auto-lands the cheapest
    FITTING combo when the user's pick is dry or too small), generic
    download-and-serve script (mmproj-aware for vision), optional
    persistent volume, full v3 lifecycle (idle-stop, budget caps,
    auto-recover, boot-watch)."""
    if not (payload.file_name and payload.file_url and payload.size_gb > 0):
        return _error(400, "file_name, file_url and size_gb are required")
    if payload.source not in (*modelsearch.SOURCES, "upload"):
        return _error(400, f"source must be one of "
                           f"{modelsearch.SOURCES + ('upload',)}")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if backend not in rp_backends:
        return _error(400, f"backend {backend!r} is not a runpod backend")
    api_key_env = rp_backends[backend].get("api_key_env", "RUNPOD_API_KEY")
    try:
        opts = await catalog.live_options(api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")

    total_gb = payload.size_gb + payload.mmproj_size_gb
    cloud_key = "community" if payload.cloud == "community" else "secure"

    def _combo(gpu_id: str, count: int) -> dict | None:
        g = next((g for g in opts["gpus"] if g.get("id") == gpu_id), None)
        if g is None:
            return None
        price = (g.get("price") or {}).get(cloud_key)
        if not price:
            return None
        return {"gpu_id": gpu_id, "gpu_count": count,
                "est_cost_per_hr": round(float(price) * count, 2),
                "vram_total_gb": float(g.get("memory") or 0) * count,
                "max_count": int((g.get("maxCount") or {}).get(cloud_key)
                                 or 0)}

    # user combo first, then every FITTING combo price-ascending
    candidates: list[dict] = []
    if payload.gpu_id:
        user = _combo(payload.gpu_id, payload.gpu_count or 1)
        if user is None:
            return _error(400, f"unknown GPU {payload.gpu_id!r} for the "
                               f"{cloud_key} cloud")
        if user["gpu_count"] > user["max_count"]:
            return _error(400, f"{payload.gpu_id} allows at most "
                               f"{user['max_count']}× on {cloud_key}")
        candidates.append(user)
    pool = sorted((c for g in opts["gpus"] for c in (
        _combo(g.get("id", ""), n) for n in (1, 2, 3, 4))
        if c and c["gpu_count"] <= c["max_count"]
        and catalog.custom_ctx_options(total_gb, c["vram_total_gb"])),
        key=lambda c: c["est_cost_per_hr"])
    candidates += [c for c in pool if c not in candidates]
    if not any(catalog.custom_ctx_options(total_gb, c["vram_total_gb"])
               for c in candidates):
        return JSONResponse(status_code=422, content={
            "detail": {"code": "invalid_selection", "errors": [
                f"no live GPU fits this model (~"
                f"{catalog.weights_vram_gb(total_gb):.0f} GB VRAM for "
                "weights alone; try a smaller quant)"]}})

    # optional persistent volume (downloads survive pod stops)
    volume_id = ""
    if payload.volume_mode == "create":
        if not payload.data_center_id:
            return _error(400, "data_center_id required to create a volume")
        dc = next((d for d in opts["datacenters"]
                   if d.get("id") == payload.data_center_id), None)
        if not dc or not (dc.get("networkVolumeTypes") or []):
            return _error(400, f"{payload.data_center_id} does not "
                               "support network volumes")
        vol_gb = payload.volume_size_gb or max(
            20, int(total_gb * 1.2) + 10)
        try:
            vol = await runpod.create_volume(
                f"gw-custom-{payload.alias or 'model'}-"
                f"{payload.data_center_id}".lower()[:60],
                payload.data_center_id, vol_gb,
                payload.volume_type, api_key_env)
        except runpod.RunPodError as e:
            return _error(502, f"volume create failed: {e}",
                          code="runpod_error")
        if not isinstance(vol, dict) or not vol.get("id"):
            return _error(502, "volume create returned no id",
                          code="runpod_error")
        volume_id = str(vol["id"])

    disk = payload.container_disk_gb or (
        20 if volume_id else max(20, int(total_gb * 1.2) + 10))
    alias = (payload.alias or
             payload.file_name.split("/")[-1].removesuffix(".gguf"))[:40]
    tried: list[dict[str, Any]] = []
    pod: dict[str, Any] | None = None
    landed: dict[str, Any] | None = None
    used_ctx = ""
    for cand in candidates:
        presets = catalog.custom_ctx_options(total_gb, cand["vram_total_gb"])
        if not presets:      # user's combo too small — fall through
            tried.append({**cand, "error": "model does not fit this GPU"})
            continue
        ctx_key = payload.ctx_key if any(
            p["key"] == payload.ctx_key for p in presets
        ) else presets[-1]["key"]
        spec = catalog.build_custom_spec(
            file_name=payload.file_name, file_url=payload.file_url,
            size_gb=payload.size_gb, mmproj_url=payload.mmproj_url,
            mmproj_size_gb=payload.mmproj_size_gb,
            gpu_id=cand["gpu_id"], gpu_count=cand["gpu_count"],
            cloud=payload.cloud, data_center_id=payload.data_center_id,
            volume_id=volume_id, ctx_key=ctx_key,
            container_disk_gb=disk, idle_stop_s=payload.idle_stop_s,
            alias=alias,
            sha256=payload.sha256, mmproj_sha256=payload.mmproj_sha256)
        deploy_payload = runpod.deploy_payload(backend, alias, spec)
        try:
            pod = await runpod.deploy_pod(deploy_payload, api_key_env)
            landed, used_ctx = cand, ctx_key
            break
        except runpod.RunPodError as e:
            tried.append({**cand, "error": str(e)[:250]})
    if pod is None:
        return JSONResponse(status_code=502, content={
            "error": {"message": "deploy failed for every GPU combination "
                                 f"({len(tried)} tried); see 'tried'",
                      "code": "runpod_deploy_failed"},
            "tried": tried})
    spec["pod_id"] = pod.get("id")
    if landed and landed.get("est_cost_per_hr"):
        spec["est_cost_per_hr"] = float(landed["est_cost_per_hr"])

    def mutator(raw: dict):
        models = raw.setdefault("backends", {}).setdefault(
            backend, {}).setdefault("models", {})
        models[alias] = spec

    _mutate_config(mutator)
    return {"ok": True, "pod_id": pod.get("id"), "backend": backend,
            "model": alias, "name": pod.get("name"),
            "est_cost_per_hr": (landed or {}).get("est_cost_per_hr"),
            "ctx_key": used_ctx, "landed": landed, "tried": tried}


# ── Model upload: receiver pod + gateway relay (v4 D5) ──────────────────────

class UploadInitPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    backend: str = ""
    filename: str = ""
    size_bytes: int = 0
    volume_id: str = ""      # optional: uploads persist on the volume


@app.post("/admin/models/upload-init")
async def admin_upload_init(payload: UploadInitPayload) -> Response:
    """Reuse a HEALTHY receiver pod or deploy one on the cheapest GPU.
    The receiver is a config model (upload-receiver): idle-stop 1800 +
    the never-healthy reaper own its lifecycle, and it shows in the
    servers widget."""
    import os

    filename = os.path.basename((payload.filename or "").strip())
    if not filename or filename in (".", ".."):
        return _error(400, "filename required")
    if payload.size_bytes <= 0:
        return _error(400, "size_bytes required")
    if payload.size_bytes > 20 * 1024 ** 3:
        return _error(413, "uploads over ~20 GB are impractical over a "
                           "home uplink — host the file and paste its "
                           "URL into the custom deploy instead")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if backend not in rp_backends:
        return _error(400, f"backend {backend!r} is not a runpod backend")
    try:
        info = await upload.init_receiver(cfg, backend, payload.size_bytes,
                                          payload.volume_id)
    except upload.UploadError as e:
        return _error(502, str(e), code="upload_error")
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    return {"ok": True, **info, "filename": filename,
            "size_bytes": payload.size_bytes}


@app.put("/admin/models/upload")
async def admin_models_upload(request: Request) -> Response:
    """Gateway relay: stream the browser upload to the receiver pod's
    proxy URL. Content-Length is forwarded explicitly — the receiver
    rejects chunked bodies (411). v2.5.1 (GL-R02): the receiver session
    token rides the x-upload-token header (the console got it from
    upload-init), and the relay HASHES the stream as it passes —
    finalize verifies content, not just size."""
    import hashlib
    import os
    from urllib.parse import quote

    pod_id = (request.headers.get("x-pod-id") or "").strip()
    filename = os.path.basename(
        (request.headers.get("x-filename") or "").strip())
    size_raw = (request.headers.get("x-file-size") or "").strip()
    token = (request.headers.get("x-upload-token") or "").strip()
    if not (pod_id and filename and size_raw.isdigit() and token):
        return _error(400, "x-pod-id, x-filename, x-file-size and "
                           "x-upload-token headers are required")
    url = upload.receiver_url(pod_id, quote(filename))
    digest = hashlib.sha256()

    async def _tee():
        async for chunk in request.stream():
            digest.update(chunk)
            yield chunk

    try:
        async with httpx.AsyncClient(
                timeout=httpx.Timeout(600.0, connect=30.0)) as c:
            r = await c.put(url, content=_tee(),
                            headers={"Content-Length": size_raw,
                                     "Authorization": f"Bearer {token}"})
    except httpx.HTTPError as e:
        return _error(502, f"relay to receiver failed: {e}",
                      code="upload_error")
    if r.status_code != 200:
        return _error(502, f"receiver rejected upload: {r.text[:200]}",
                      code="upload_error")
    upload.remember_relay_hash(pod_id, filename, digest.hexdigest())
    try:
        detail = r.json()
    except ValueError:
        detail = {}
    return {"ok": True, "pod_id": pod_id, "filename": filename,
            "sha256": digest.hexdigest(),
            "detail": detail}


@app.get("/admin/models/upload-status")
async def admin_upload_status(pod_id: str = "",
                              filename: str = "",
                              token: str = "") -> Response:
    """Bytes the receiver has so far (the console polls this during the
    relay — browser-side progress is localhost-instant and useless)."""
    if not (pod_id and filename and token):
        return _error(400, "pod_id, filename and token required")
    try:
        return await upload.receiver_status(pod_id, filename, token)
    except Exception as e:  # noqa: BLE001 — receiver unreachable
        return _error(502, f"receiver status failed: {e}",
                      code="upload_error")


class UploadFinalizePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pod_id: str = ""
    filename: str = ""
    size_bytes: int = 0
    token: str = ""          # the receiver session token (from init)
    sha256: str = ""         # optional expected content hash


@app.post("/admin/models/upload-finalize")
async def admin_upload_finalize(payload: UploadFinalizePayload) -> Response:
    """Verify the uploaded size AND content (v2.5.1: an expected sha256
    is checked against the relay hash / receiver sidecar — same-size
    replacement poisoning fails); returns the receiver's PUBLIC file
    URL (token rides as ?t= — the URL is the capability the model pod
    curls)."""
    if not (payload.pod_id and payload.filename and payload.size_bytes > 0
            and payload.token):
        return _error(400, "pod_id, filename, size_bytes and token "
                           "required")
    try:
        return await upload.finalize(payload.pod_id, payload.filename,
                                     payload.size_bytes, payload.token,
                                     payload.sha256)
    except upload.UploadError as e:
        return _error(502, str(e), code="upload_error")
    except Exception as e:  # noqa: BLE001
        return _error(502, f"finalize failed: {e}", code="upload_error")


class UploadCleanupPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    backend: str = ""


@app.post("/admin/models/upload-cleanup")
async def admin_upload_cleanup(payload: UploadCleanupPayload) -> Response:
    """Terminate the receiver pod + drop it from config (call after the
    model pod finished downloading, or anytime to abort)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    return await upload.cleanup_receiver(cfg, backend)


# ── Serverless deployment (v4.5) ────────────────────────────────────────────

class ServerlessDeployPayload(BaseModel):
    """Deploy a model as a RunPod SERVERLESS endpoint: scale-to-zero,
    no pod lifecycle, no helper pods. source=hf: the worker downloads
    the GGUF from Hugging Face at cold start (llama-server
    --hf-repo/--hf-file, straight into container disk). source=upload:
    the file must already be on a network volume (serverless workers
    mount volumes at /runpod-volume — NOT the pod-view /workspace)."""
    model_config = ConfigDict(extra="ignore")

    backend: str = ""
    source: str = ""              # hf | modelscope | ollama | upload
    repo_id: str = ""
    file_name: str = ""
    file_url: str = ""            # required unless source=upload
    size_gb: float = 0
    gpu_id: str = ""              # catalog GPU with a serverless pool
    alias: str = ""
    workers_max: int = 1
    idle_timeout: int = 5         # minutes
    queue_delay: int = 4
    ctx_total: int = 32768
    volume_mode: str = "reuse"    # reuse | create
    volume_id: str = ""
    volume_size_gb: int = 0
    data_center_id: str = ""


@app.post("/admin/models/deploy-serverless")
async def admin_deploy_serverless(
        payload: ServerlessDeployPayload) -> Response:
    """Endpoint create (scale-to-zero) → config persist. HF sources:
    the worker downloads the GGUF from Hugging Face at cold start
    (LLAMA_ARG_HF_REPO/LLAMA_ARG_HF_FILE — no volume, no helper pod);
    upload sources: the file must already be on a network volume
    (mounted at /runpod-volume by serverless workers). Request routing
    needs NO new code — endpoint_id models already go through the
    OpenAI-compat URL. Single-GPU only in v1 (422 otherwise)."""
    filename = upload.safe_filename(payload.file_name or "")
    if not filename:
        return _error(400, "file_name required (letters/digits/dots/"
                           "dashes/underscores only)")
    if not filename.lower().endswith(".gguf"):
        return _error(400, "file_name must be a .gguf file")
    if payload.size_gb <= 0:
        return _error(400, "size_gb required")
    # No pod storage for serverless (user directive 2026-09-21): HF
    # sources cold-start-download straight from Hugging Face into the
    # worker's container disk (llama-server --hf-repo/--hf-file); the
    # only volume path left is source=upload — a local file has no
    # other way onto RunPod. ModelScope/Ollama URLs can't be fetched
    # by the worker → rejected.
    if payload.source not in ("hf", "upload"):
        return _error(400, "serverless deploy supports source=hf (the "
                           "worker downloads the GGUF from Hugging "
                           "Face when it cold-starts) or source=upload "
                           "(the file is already on one of your "
                           "volumes)")
    if payload.source == "hf" and not payload.repo_id:
        return _error(400, "repo_id required for source=hf (the "
                           "Hugging Face repo to download from)")
    if payload.source == "upload" and not payload.volume_id:
        return _error(400, "volume_id required for source=upload (the "
                           "file must already be on that volume)")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if backend not in rp_backends:
        return _error(400, f"backend {backend!r} is not a runpod backend")
    api_key_env = rp_backends[backend].get("api_key_env", "RUNPOD_API_KEY")
    try:
        opts = await catalog.live_options(api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")

    # GPU: must carry a serverless pool (v2 catalog 'pool' field);
    # single-GPU sizing only in v1
    gpus = opts.get("gpus") or []
    if payload.gpu_id:
        gpu = next((g for g in gpus if g.get("id") == payload.gpu_id), None)
        if gpu is None:
            return _error(400, f"unknown GPU {payload.gpu_id!r}")
        pool = gpu.get("pool")
        if not pool:
            return JSONResponse(status_code=422, content={
                "detail": {"code": "invalid_selection", "errors": [
                    f"{payload.gpu_id} has no serverless pool — pick a "
                    "pool-backed GPU (e.g. RTX 4090 / L40S / A100-80)"]}})
    else:
        fitting = [g for g in gpus if g.get("pool")
                   and catalog.custom_ctx_options(
                       payload.size_gb, float(g.get("memory") or 0))]
        if not fitting:
            return JSONResponse(status_code=422, content={
                "detail": {"code": "invalid_selection", "errors": [
                    "no single serverless GPU fits this model (~"
                    f"{catalog.weights_vram_gb(payload.size_gb):.0f} GB "
                    "VRAM for weights alone; multi-GPU serverless is "
                    "not supported yet)"]}})
        gpu = min(fitting, key=lambda g: float(
            (g.get("price") or {}).get("secure") or 0))
        pool = gpu["pool"]
    if not catalog.custom_ctx_options(payload.size_gb,
                                      float(gpu.get("memory") or 0)):
        return JSONResponse(status_code=422, content={
            "detail": {"code": "invalid_selection", "errors": [
                f"{gpu.get('id')} ({gpu.get('memory')}GB) cannot serve "
                "this model — multi-GPU serverless is not supported "
                "yet"]}})

    # Volume: ONLY source=upload — a local file must already sit on a
    # network volume (serverless workers mount it at /runpod-volume).
    # HF sources need no volume: the worker downloads at cold start.
    volume_id = ""
    dc = payload.data_center_id or ""
    if payload.source == "upload":
        vol = next((v for v in (opts.get("volumes") or [])
                    if v.get("id") == payload.volume_id), None)
        if vol is None:
            return _error(400, f"unknown volume {payload.volume_id!r}")
        volume_id = payload.volume_id
        dc = str(vol.get("dataCenter") or "")

    alias = (payload.alias or filename.removesuffix(".gguf"))[:40]
    if payload.source == "hf":
        # worker-native download: llama-server --hf-repo --hf-file
        env = {"LLAMA_ARG_HF_REPO": payload.repo_id,
               "LLAMA_ARG_HF_FILE": filename,
               "LLAMA_ARG_CTX_SIZE": str(int(payload.ctx_total or 32768))}
    else:
        env = {"LLAMA_ARG_MODEL": f"/runpod-volume/uploads/{filename}",
               "LLAMA_ARG_CTX_SIZE": str(int(payload.ctx_total or 32768))}
    # container disk must fit the cold-start HF download (+ runtime);
    # upload sources read from the volume but keep the same floor
    disk_gb = max(20, int(payload.size_gb * 1.2) + 10)
    ep_payload = runpod.serverless_endpoint_payload(
        name=f"gw-sl-{alias}".lower()[:60], pool=pool, env=env,
        workers_max=max(1, payload.workers_max),
        idle_timeout=max(1, payload.idle_timeout),
        queue_delay=max(1, payload.queue_delay),
        network_volume_id=volume_id, data_center_id=dc,
        disk_gb=disk_gb)
    try:
        ep = await runpod.create_endpoint(ep_payload, api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")

    def mutator(raw: dict):
        models = raw.setdefault("backends", {}).setdefault(
            backend, {}).setdefault("models", {})
        models[alias] = {
            "endpoint_id": ep["id"],
            "remote_id": alias,
            "alias": alias,
            # cold starts (worker boot + volume read + load) routinely
            # exceed the global queue timeout — per-model 600s
            "timeout_s": 600,
            "serverless": {
                "source": payload.source,
                "repo_id": payload.repo_id,
                "file_name": filename,
                "size_gb": payload.size_gb,
                "gpu_id": gpu.get("id"),
                "pool": pool,
                "volume_id": volume_id,
                "workers_max": max(1, payload.workers_max),
                "idle_timeout": max(1, payload.idle_timeout),
            },
        }

    _mutate_config(mutator)
    return {"ok": True, "endpoint_id": ep["id"], "model": alias,
            "backend": backend, "gpu_id": gpu.get("id"), "pool": pool,
            "volume_id": volume_id,
            "workers_max": max(1, payload.workers_max),
            "idle_timeout": max(1, payload.idle_timeout),
            "request_urls": ep.get("requestUrls") or {}}


@app.get("/admin/serverless")
async def admin_serverless() -> Response:
    """Serverless panel: configured endpoint models (with deploy
    provenance) merged with LIVE endpoint state (workers, scaling);
    account endpoints the gateway doesn't track are listed too."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    entries = []
    for name, b in cfg.backends.items():
        if b.get("type") != "runpod":
            continue
        for mkey, m in (b.get("models") or {}).items():
            if isinstance(m, dict) and m.get("endpoint_id"):
                entries.append({
                    "backend": name, "model": mkey,
                    "alias": m.get("alias", mkey),
                    "endpoint_id": m["endpoint_id"],
                    "timeout_s": m.get("timeout_s", 0),
                    **(m.get("serverless") or {})})
    live: dict[str, dict] = {}
    for name, b in cfg.backends.items():
        if b.get("type") == "runpod":
            env = b.get("api_key_env", "RUNPOD_API_KEY")
            try:
                for ep in await runpod.list_endpoints(env):
                    if isinstance(ep, dict) and ep.get("id"):
                        live[ep["id"]] = ep
            except runpod.RunPodError as e:
                return _error(502, str(e), code="runpod_error")
            break
    for e in entries:
        ep = live.get(e["endpoint_id"])
        e["exists"] = ep is not None
        e["live"] = ({k: ep.get(k) for k in
                      ("name", "workers", "scaling", "gpu", "flashboot")}
                     if ep else None)
    tracked = {e["endpoint_id"] for e in entries}
    others = [{"endpoint_id": ep.get("id"), "name": ep.get("name"),
               "workers": ep.get("workers")}
              for eid, ep in live.items() if eid not in tracked]
    return {"endpoints": entries, "untracked": others}


class ServerlessSettingsPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    backend: str = ""
    model: str = ""
    workers_max: int | None = None
    idle_timeout: int | None = None    # minutes
    queue_delay: int | None = None


@app.post("/admin/serverless/settings")
async def admin_serverless_settings(
        payload: ServerlessSettingsPayload) -> Response:
    """Edit scaler settings on a deployed endpoint: PATCH /v2/
    serverless/{id} with the CURRENT workers/scaling objects merged
    with the requested changes (PATCH replaces whole objects — the
    get-first-then-merge avoids clobbering unspecified fields)."""
    if not payload.model:
        return _error(400, "model required")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    b = cfg.backends.get(backend) or {}
    m = (b.get("models") or {}).get(payload.model) or {}
    ep_id = str(m.get("endpoint_id") or "").strip()
    if not ep_id:
        return _error(400, f"model {payload.model!r} is not a serverless "
                           "endpoint model")
    env = b.get("api_key_env", "RUNPOD_API_KEY")
    try:
        cur = await runpod.get_endpoint(ep_id, env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    workers = dict(cur.get("workers") or {})
    scaling = dict(cur.get("scaling") or {})
    if payload.workers_max is not None:
        workers["max"] = max(1, payload.workers_max)
    if payload.idle_timeout is not None:
        workers["idleTimeout"] = max(1, payload.idle_timeout)
    if payload.queue_delay is not None:
        scaling["queueDelay"] = max(1, payload.queue_delay)
    try:
        await runpod.patch_endpoint(ep_id, {"workers": workers,
                                            "scaling": scaling}, env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    sl = dict(m.get("serverless") or {})
    if payload.workers_max is not None:
        sl["workers_max"] = max(1, payload.workers_max)
    if payload.idle_timeout is not None:
        sl["idle_timeout"] = max(1, payload.idle_timeout)
    if sl:
        def mutator(raw: dict):
            mspec = (raw.get("backends", {}).get(backend, {})
                     .get("models", {}).get(payload.model))
            if isinstance(mspec, dict):
                mspec["serverless"] = sl
        _mutate_config(mutator)
    return {"ok": True, "endpoint_id": ep_id, "workers": workers,
            "scaling": scaling}


@app.get("/admin/serverless/workers")
async def admin_serverless_workers(model: str, worker_id: str = "",
                                   backend: str = "") -> Response:
    """Serverless worker inspection: list an endpoint's workers, or
    fetch one worker's container logs (?worker_id=...). The logs API
    is the only programmatic window into serverless worker output
    (v2 OpenAPI discovery 2026-09-21 — previously console-only) —
    THE debugging path for crash-looping workers."""
    if not model:
        return _error(400, "model required")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = backend or _runpod_api_key_env(cfg)[0]
    b = cfg.backends.get(backend) or {}
    m = (b.get("models") or {}).get(model) or {}
    ep_id = str(m.get("endpoint_id") or "").strip()
    if not ep_id:
        return _error(400, f"model {model!r} is not a serverless "
                           "endpoint model")
    env = b.get("api_key_env", "RUNPOD_API_KEY")
    try:
        if worker_id:
            return await runpod.get_worker_logs(ep_id, worker_id, env)
        return {"workers": await runpod.list_endpoint_workers(ep_id, env)}
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")


# ── Provider registry + RunPod account panel ───────────────────────────────

@app.get("/admin/providers")
async def admin_providers() -> Response:
    """Registered cloud providers — the console builds one panel per
    entry (RunPod today; the registry is how other vendors onboard)."""
    return {"providers": providers.list_providers()}


@app.get("/admin/providers/runpod/account")
async def admin_provider_runpod_account() -> Response:
    """RunPod panel data: account volumes + recent pod billing."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend, api_key_env = _runpod_api_key_env(cfg)
    if not backend:
        return _error(400, "no runpod backend configured")
    try:
        vol = await runpod.list_volumes(api_key_env)
        billing = await runpod.list_pod_billing(api_key_env)
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    # which volumes are referenced by configured models (delete guard
    # + console "in use" markers)
    used: dict[str, list[str]] = {}
    for bname, b in cfg.backends.items():
        if b.get("type") != "runpod":
            continue
        for mname, m in (b.get("models") or {}).items():
            vid = (m or {}).get("volume_id") if isinstance(m, dict) else ""
            if vid:
                used.setdefault(str(vid), []).append(f"{bname}:{mname}")
    return {"backend": backend, "volumes": vol, "billing": billing,
            "volume_usage": used}


class VolumeManagePayload(BaseModel):
    """Create or delete a network volume (RunPod panel)."""
    model_config = ConfigDict(extra="ignore")
    backend: str = ""
    action: str = ""        # create | delete
    name: str = ""
    data_center_id: str = ""
    size_gb: int = 0
    volume_type: str = "STANDARD"
    volume_id: str = ""


@app.post("/admin/runpod/volumes")
async def admin_runpod_volumes(payload: VolumeManagePayload) -> Response:
    """Volume manager: create (name/DC/size/type) or delete (id —
    refused while a configured model still references it)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if backend not in rp_backends:
        return _error(400, f"backend {backend!r} is not a runpod backend")
    api_key_env = rp_backends[backend].get("api_key_env", "RUNPOD_API_KEY")
    if payload.action == "create":
        if not (payload.name and payload.data_center_id and payload.size_gb > 0):
            return _error(400, "name, data_center_id and size_gb required")
        try:
            vol = await runpod.create_volume(
                payload.name, payload.data_center_id, payload.size_gb,
                payload.volume_type, api_key_env)
        except runpod.RunPodError as e:
            return _error(502, str(e), code="runpod_error")
        return {"ok": True, "volume": vol}
    if payload.action == "delete":
        if not payload.volume_id:
            return _error(400, "volume_id required")
        for bname, b in cfg.backends.items():
            if b.get("type") != "runpod":
                continue
            for mname, m in (b.get("models") or {}).items():
                if isinstance(m, dict) and m.get("volume_id") == payload.volume_id:
                    return _error(409, f"volume is configured on model "
                                       f"{bname}:{mname} — remove that "
                                       "model first", code="volume_in_use")
        try:
            out = await runpod.delete_volume(payload.volume_id, api_key_env)
        except runpod.RunPodError as e:
            return _error(502, str(e), code="runpod_error")
        return {"ok": True, "result": out}
    return _error(400, "action must be 'create' or 'delete'")


class PodLogsPayload(BaseModel):
    """Recent container log lines for one pod (boot-watch modal)."""
    model_config = ConfigDict(extra="ignore")
    backend: str = ""
    model: str = ""
    pod_id: str = ""
    tail: int = 120


@app.post("/admin/runpod/logs")
async def admin_runpod_logs(payload: PodLogsPayload) -> Response:
    """Tail of a pod's container logs — the deploy modal streams these
    while the pod boots (downloading weights / building / loading)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    backend = payload.backend or _runpod_api_key_env(cfg)[0]
    rp_backends = {n: b for n, b in cfg.backends.items()
                   if b.get("type") == "runpod"}
    if backend not in rp_backends:
        return _error(400, f"backend {backend!r} is not a runpod backend")
    api_key_env = rp_backends[backend].get("api_key_env", "RUNPOD_API_KEY")
    pod_id = payload.pod_id
    if not pod_id and payload.model:
        spec = (rp_backends[backend].get("models") or {}).get(payload.model)
        pod_id = (spec or {}).get("pod_id") or ""
        if not pod_id:
            try:
                pod = await runpod.get_manager(
                    cfg, backend, payload.model).find_pod()
                pod_id = (pod or {}).get("id") or ""
            except Exception:  # noqa: BLE001 — find is best-effort
                pod_id = ""
    if not pod_id:
        return _error(400, "pod_id or model required (model must be "
                          "deployed or have a pod_id in config)")
    try:
        lines = await runpod.fetch_pod_logs(
            pod_id, api_key_env, max(10, min(int(payload.tail or 120), 400)))
    except runpod.RunPodError as e:
        return _error(502, str(e), code="runpod_error")
    return {"pod_id": pod_id, "lines": lines}


class RunPodServerlessPayload(BaseModel):
    """Payload for managing runpod serverless endpoints."""
    model_config = ConfigDict(extra="ignore")
    backend: str = ""
    model: str = ""
    endpoint_id: str = ""
    remote_id: str = ""
    transport: str = "openai"  # openai or runsync
    timeout_s: float = 120.0
    policy: dict[str, Any] | None = None
    delete: bool = False


@app.post("/admin/runpod/serverless")
async def admin_runpod_serverless(payload: RunPodServerlessPayload) -> Response:
    """Add or remove a serverless endpoint model from config (manual
    registration — e.g. BYO endpoints; v4.5's deploy-serverless does
    this automatically when IT creates the endpoint)."""
    if not payload.backend or not payload.model:
        return _error(400, "backend and model required")
    cfg = get_gateway_config()
    cfg.maybe_reload()
    rp_backends = {n: b for n, b in cfg.backends.items() if b.get("type") == "runpod"}
    if payload.backend not in rp_backends:
        return _error(400, f"backend {payload.backend!r} is not a runpod backend")
    if payload.delete:
        def mutator(raw: dict):
            models = raw.get("backends", {}).get(payload.backend, {}).get("models", {})
            models.pop(payload.model, None)
        _mutate_config(mutator)
        return {"ok": True, "deleted": f"{payload.backend}:{payload.model}"}
    if not payload.endpoint_id:
        return _error(400, "endpoint_id required for new serverless model")
    spec: dict[str, Any] = {
        "endpoint_id": payload.endpoint_id,
        "remote_id": payload.remote_id,
        "transport": payload.transport,
        "timeout_s": payload.timeout_s,
    }
    if payload.policy:
        spec["policy"] = payload.policy
    def mutator(raw: dict):
        models = raw.setdefault("backends", {}).setdefault(payload.backend, {}).setdefault("models", {})
        models[payload.model] = spec
    _mutate_config(mutator)
    return {"ok": True, "backend": payload.backend, "model": payload.model,
            "endpoint_id": payload.endpoint_id}


class ParamsPayload(BaseModel):
    """Structured parameters for the console — avoids raw JSON editing."""
    model_config = ConfigDict(extra="ignore")
    bind: str | None = None
    port: int | None = None
    models_dir: str | None = None
    default_alias: str | None = None
    queue_max_waiting: int | None = None
    queue_timeout_s: float | None = None


@app.put("/admin/params")
async def admin_put_params(payload: ParamsPayload) -> Response:
    """Update top-level config parameters via structured fields."""
    def mutator(raw: dict):
        if payload.bind is not None:
            raw["bind"] = payload.bind
        if payload.port is not None:
            raw["port"] = payload.port
        if payload.models_dir is not None:
            raw["models_dir"] = payload.models_dir
        if payload.default_alias is not None:
            raw["default_alias"] = payload.default_alias
        # Queue settings
        q = raw.setdefault("queue", {})
        if payload.queue_max_waiting is not None:
            q["max_waiting"] = payload.queue_max_waiting
        if payload.queue_timeout_s is not None:
            q["timeout_s"] = payload.queue_timeout_s

    _mutate_config(mutator)
    return {"ok": True}


class GatewayKeyPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    key: str = ""
    delete: bool = False


@app.post("/admin/gateway-keys")
async def admin_gateway_keys(payload: GatewayKeyPayload) -> Response:
    """Add or remove a gateway bearer key."""
    def mutator(raw: dict):
        keys = raw.setdefault("gateway_keys", [])
        if payload.delete and payload.key:
            if payload.key in keys:
                keys.remove(payload.key)
        elif payload.key and payload.key not in keys:
            keys.append(payload.key)

    _mutate_config(mutator)
    cfg = get_gateway_config()
    return {"ok": True, "gateway_keys": cfg.gateway_keys}


@app.post("/admin/runpod/inventory")
async def admin_runpod_inventory() -> Response:
    """Check RunPod entities against config. Returns live pods/endpoints
    and flags configured models whose entities no longer exist."""
    cfg = get_gateway_config()
    cfg.maybe_reload()

    # Find runpod backends
    rp_backends = {n: b for n, b in cfg.backends.items() if b.get("type") == "runpod"}
    if not rp_backends:
        return {"pods": [], "endpoints": [], "stale": [], "message": "no runpod backends configured"}

    # Use first runpod backend's api_key_env
    api_key_env = list(rp_backends.values())[0].get("api_key_env", "RUNPOD_API_KEY")

    try:
        pods = await runpod.list_pods(api_key_env)
        endpoints = await runpod.list_endpoints(api_key_env)
    except Exception as e:
        return {"error": str(e), "pods": [], "endpoints": [], "stale": []}

    pod_ids = {p.get("id") for p in pods}
    ep_ids = {e.get("id") for e in endpoints}

    # Check configured models for stale references
    stale = []
    ep_refs: dict[str, list[str]] = {}  # endpoint_id -> ["backend:model", ...]
    for bname, bdef in rp_backends.items():
        for mname, mdef in (bdef.get("models") or {}).items():
            pid = mdef.get("pod_id")
            eid = mdef.get("endpoint_id")
            if pid and pid not in pod_ids:
                stale.append({"backend": bname, "model": mname, "type": "pod", "id": pid})
            elif eid and eid not in ep_ids:
                stale.append({"backend": bname, "model": mname, "type": "endpoint", "id": eid})
            if eid:
                ep_refs.setdefault(eid, []).append(f"{bname}:{mname}")

    return {
        # pod_info: whitelisted live fields (gpu, cost/hr, uptime, status…)
        "pods": [runpod.pod_info(p)
                 | {"status": p.get("status") or p.get("desiredStatus")}
                 for p in pods],
        "endpoints": [{"id": e.get("id"), "name": e.get("name"),
                       "workersMax": e.get("workersMax"),
                       "referenced_by": ep_refs.get(e.get("id"), [])}
                      for e in endpoints],
        "stale": stale,
    }


@app.post("/admin/reload")
async def admin_reload() -> Response:
    """Force a config reload now (same as the console's reload button)."""
    cfg = get_gateway_config()
    cfg.reload_now()
    return {"ok": True,
            "backends": {n: len(b.get("models") or {})
                         for n, b in cfg.backends.items()},
            "aliases": sorted(cfg.aliases)}


# ── Admin: model download from HuggingFace ─────────────────────────────────

class DownloadPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    repo_id: str = ""  # e.g. "bartowski/Meta-Llama-3.1-8B-Instruct-Q4_K_M"
    filename: str = ""  # specific file, or blank for auto-detect
    hf_token: str = ""  # optional, for gated repos


@app.post("/admin/models/download")
async def admin_download_model(payload: DownloadPayload) -> Response:
    """Download a GGUF file from HuggingFace to models_dir.

    Returns progress events via the event ring. The file is downloaded
    to models_dir and will be auto-discovered on next healthz/config reload.
    """
    if huggingface_hub is None:
        return _error(500, "huggingface_hub not installed. Run: uv add huggingface_hub")

    repo_id = payload.repo_id.strip()
    if not repo_id:
        return _error(400, "repo_id is required", param="repo_id")

    cfg = get_gateway_config()
    cfg.maybe_reload()
    models_dir = Path(cfg.models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)

    filename = payload.filename.strip()
    hf_token = payload.hf_token.strip() or None

    try:
        if filename:
            # Download specific file — to_thread: a multi-GB GGUF on the
            # event loop would freeze every concurrent request (healthz,
            # chats) for the whole download.
            dest = await asyncio.to_thread(
                huggingface_hub.hf_hub_download,
                repo_id=repo_id,
                filename=filename,
                local_dir=str(models_dir),
                token=hf_token,
            )
        else:
            # Auto-detect: find first .gguf file in repo
            api = huggingface_hub.HfApi(token=hf_token)

            def _list_files() -> list[str]:
                try:
                    return api.list_repo_files(repo_id=repo_id, repo_type="model")
                except Exception:
                    return api.list_repo_files(repo_id=repo_id)

            files = await asyncio.to_thread(_list_files)
            ggufs = [f for f in files if f.endswith(".gguf")]
            if not ggufs:
                return _error(404, f"No .gguf files found in {repo_id}",
                              code="no_gguf_found")
            # Pick the first one (usually the main quant)
            filename = ggufs[0]
            dest = await asyncio.to_thread(
                huggingface_hub.hf_hub_download,
                repo_id=repo_id,
                filename=filename,
                local_dir=str(models_dir),
                token=hf_token,
            )

        dest_path = Path(dest)
        size_mb = round(dest_path.stat().st_size / (1024 * 1024), 1)
        _event_ring().push({
            "ts": datetime.now().isoformat(),
            "event": "model_downloaded",
            "repo_id": repo_id,
            "filename": filename,
            "dest": str(dest_path.name),
            "size_mb": size_mb,
        })
        return {
            "downloaded": dest_path.name,
            "path": str(dest_path),
            "size_mb": size_mb,
            "repo_id": repo_id,
        }
    except Exception as e:
        return _error(500, f"Download failed: {e}", code="download_error")


# ── Admin: model benchmark ─────────────────────────────────────────────────

class BenchmarkPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    model: str = ""  # model alias/name to benchmark
    prompt: str = "Explain quantum computing in simple terms."
    n: int = 1  # number of iterations (warmup + measured)
    stream: bool = False  # use streaming to measure TTFT
    max_tokens: int = 256
    base_url: str = ""  # external OpenAI-compatible endpoint (empty = self)
    api_key: str = ""  # API key for external endpoint


@app.post("/admin/benchmark")
async def admin_benchmark(payload: BenchmarkPayload) -> Response:
    """Run a performance benchmark on a model.

    Measures: first-load time (if cold), TTFT (time to first token),
    tokens per second, total latency, and output token count.
    Returns structured results suitable for the Perf panel.

    If base_url is set, benchmarks an external OpenAI-compatible endpoint.
    Otherwise benchmarks the gateway itself.
    """
    cfg = get_gateway_config()
    cfg.maybe_reload()

    model = payload.model.strip()
    if not model:
        return _error(400, "model is required", param="model")

    # Determine target endpoint
    base_url = payload.base_url.strip().rstrip("/")
    api_key = payload.api_key.strip() or None
    is_external = bool(base_url)

    # Verify model exists only for local benchmarks
    if not is_external:
        engine = get_engine()
        loaded = engine.loaded_models()
        discovered = installed_ggufs(cfg.models_dir)
        all_names = [m["alias"] for m in loaded] + [d["name"] for d in discovered]
        # Also check aliases
        for alias_name in cfg.aliases:
            all_names.append(alias_name)

        if model not in all_names:
            return _error(400, f"Model {model!r} not found. Available: {all_names[:10]}",
                          param="model", code="model_not_found")

        base_url = f"http://{cfg.bind}:{cfg.port}/v1"
        api_key = api_key or None

    results = []
    for i in range(payload.n):
        t0 = time.perf_counter()
        first_token_time = None
        first_token_ts = None
        token_count = 0

        client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key or "benchmark",
        )

        if payload.stream:
            # Streaming benchmark — measures TTFT
            try:
                stream = await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": payload.prompt}],
                    max_tokens=payload.max_tokens,
                    stream=True,
                )
                async for chunk in stream:
                    choices = getattr(chunk, "choices", [])
                    if choices and getattr(choices[0], "delta", None):
                        delta = choices[0].delta
                        content = getattr(delta, "content", None)
                        if content:
                            token_count += 1
                            if first_token_ts is None:
                                first_token_ts = time.perf_counter()
            except Exception as e:
                total_ms = int((time.perf_counter() - t0) * 1000)
                results.append({
                    "iteration": i + 1,
                    "error": str(e),
                    "total_ms": total_ms,
                })
                continue
        else:
            # Non-streaming benchmark
            try:
                resp = await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": payload.prompt}],
                    max_tokens=payload.max_tokens,
                )
                usage = getattr(resp, "usage", None)
                if usage:
                    token_count = getattr(usage, "completion_tokens", 0) or 0
                first_token_ts = time.perf_counter()  # approximation
            except Exception as e:
                total_ms = int((time.perf_counter() - t0) * 1000)
                results.append({
                    "iteration": i + 1,
                    "error": str(e),
                    "total_ms": total_ms,
                })
                continue

        total_ms = int((time.perf_counter() - t0) * 1000)
        ttft_ms = int((first_token_ts - t0) * 1000) if first_token_ts else total_ms
        tok_per_s = round(token_count / (total_ms / 1000), 1) if total_ms > 0 else 0

        results.append({
            "iteration": i + 1,
            "total_ms": total_ms,
            "ttft_ms": ttft_ms,
            "tokens": token_count,
            "tok_per_s": tok_per_s,
            "stream": payload.stream,
        })

    # Compute aggregates
    successful = [r for r in results if "error" not in r]
    agg = {}
    if successful:
        agg = {
            "avg_total_ms": round(sum(r["total_ms"] for r in successful) / len(successful)),
            "avg_ttft_ms": round(sum(r["ttft_ms"] for r in successful) / len(successful)),
            "avg_tok_per_s": round(sum(r["tok_per_s"] for r in successful) / len(successful), 1),
            "min_total_ms": min(r["total_ms"] for r in successful),
            "max_total_ms": max(r["total_ms"] for r in successful),
        }

    _event_ring().push({
        "ts": datetime.now().isoformat(),
        "event": "benchmark",
        "model": model,
        "iterations": len(successful),
        "avg_tok_per_s": agg.get("avg_tok_per_s"),
    })

    return {
        "model": model,
        "prompt": payload.prompt[:80] + ("..." if len(payload.prompt) > 80 else ""),
        "iterations": len(results),
        "successful": len(successful),
        "results": results,
        "aggregate": agg if agg else None,
    }

