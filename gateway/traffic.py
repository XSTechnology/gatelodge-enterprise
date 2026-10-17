"""Traffic capture seam — the Content Vault extension point (v2.4).

ENTERPRISE EXTENSION POINT, DEFAULT NO-OP. The standard gateway NEVER
persists prompt or response content: with no capture backend installed
(the default, forever) every entry point below returns immediately —
zero writes, zero retention, no measurable cost.

An enterprise build installs a capture backend here (see
gateway_enterprise/vault/ in the commercial fork; gated by the
``vault`` license claim and the ``vault`` config section). The backend
then receives, per /v1/chat/completions request:

  on_request(meta, body)   — once at entry, with the parsed request
                             (ChatRequest); returns a per-request
                             collector token or None to skip
  on_response(token, text) — every response content fragment as it is
                             assembled (streams append; non-streaming
                             and cache hits call once with the whole
                             text)
  on_flush(token, meta)    — exactly once from the request-log
                             chokepoint (gateway/app.py:_log_request)
                             with the terminal metadata: model,
                             backend, stream, ms, usage, status, err,
                             cost_usd, key, session, team, req, trace.
                             Errors (4xx/5xx, guardrail blocks, budget
                             refusals) flush with their status — the
                             request side is still captured.

Design rules, non-negotiable:

  1. Capture must NEVER break serving. Every call into a backend is
     exception-guarded; a failing backend logs a warning and counts it
     (stats()) but the request proceeds untouched.
  2. The collector rides a ContextVar — the per-request lifecycle is
     the context's lifecycle. Streams flush from the same context that
     observed the request (Starlette iterates the response generator
     in the request task).
  3. Only chat completions wire the seam today. /v1/messages self-calls
     /v1/chat/completions, so Anthropic-style clients are captured as
     the inner request. /v1/completions (legacy) and /v1/embeddings do
     not observe — no collector exists, flush is a no-op.
"""
from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger("gateway.traffic")

# capture-backend failures surfaced for the console (never fatal)
_STATS = {"requests": 0, "responses": 0, "flushes": 0, "errors": 0}


@runtime_checkable
class CaptureBackend(Protocol):
    """The interface an enterprise vault installs via install()."""

    def on_request(self, meta: dict[str, Any], body: Any) -> Any | None:
        ...  # pragma: no cover — interface

    def on_response(self, token: Any, text: str) -> None:
        ...  # pragma: no cover — interface

    def on_flush(self, token: Any, meta: dict[str, Any]) -> None:
        ...  # pragma: no cover — interface


_backend: CaptureBackend | None = None
# The per-request collector token (opaque to this module — whatever the
# backend's on_request returned; None until observed).
_TOKEN: ContextVar[Any | None] = ContextVar(
    "gateway_traffic_capture", default=None)


def install(backend: CaptureBackend) -> None:
    """Install the capture backend (enterprise boot, once, before
    serving). Installing replaces any previous backend; uninstall()
    restores the default no-op."""
    global _backend
    _backend = backend
    logger.info("traffic capture backend installed: %s",
                type(backend).__name__)


def uninstall() -> None:
    """Remove the capture backend — the no-op default returns."""
    global _backend
    _backend = None


def installed() -> bool:
    """True when a capture backend is installed (console/license
    surface)."""
    return _backend is not None


def stats() -> dict[str, int]:
    """Capture counters for the console tile (never fatal)."""
    return dict(_STATS)


def assert_configured(cfg: Any) -> None:
    """Boot-time gate: vault.enabled in the config REQUIRES an
    installed capture backend. The standard binary ships none — a
    community build (or an enterprise build without the ``vault``
    license claim, which skips installation) fails fast here with a
    clear error instead of silently not capturing.
    """
    vault = getattr(cfg, "vault", None) or {}
    if vault.get("enabled") and not installed():
        raise RuntimeError(
            "vault.enabled is set but no capture backend is installed — "
            "the Content Vault is an Enterprise add-on (license claim "
            "'vault'); remove the vault section or install the "
            "enterprise build")


# ── the seam entry points (all no-ops without a backend) ──────────────────

def observe_request(model: str, body: Any) -> None:
    """Entry observation — called once per chat request, after model
    resolution, before any execution (early errors still flush)."""
    b = _backend
    if b is None:
        return
    try:
        token = b.on_request({"model": model}, body)
        if token is not None:
            _TOKEN.set(token)
            _STATS["requests"] += 1
    except Exception:  # noqa: BLE001 — capture never breaks serving
        _STATS["errors"] += 1
        logger.warning("traffic capture: on_request failed",
                       exc_info=True)


def observe_response(text: str | None) -> None:
    """Response content observation — appended per fragment on streams,
    once with the assembled text for non-streaming and cache hits."""
    b = _backend
    if b is None or not text:
        return
    token = _TOKEN.get()
    if token is None:
        return
    try:
        b.on_response(token, text)
        _STATS["responses"] += 1
    except Exception:  # noqa: BLE001 — capture never breaks serving
        _STATS["errors"] += 1
        logger.warning("traffic capture: on_response failed",
                       exc_info=True)


def flush(meta: dict[str, Any]) -> None:
    """Terminal observation — called exactly once from the request-log
    chokepoint on EVERY terminal path (success, error, cache hit,
    guardrail block, budget refusal). The collector is dropped after."""
    b = _backend
    if b is None:
        return
    token = _TOKEN.get()
    _TOKEN.set(None)
    if token is None:
        return
    try:
        b.on_flush(token, meta)
        _STATS["flushes"] += 1
    except Exception:  # noqa: BLE001 — capture never breaks serving
        _STATS["errors"] += 1
        logger.warning("traffic capture: on_flush failed",
                       exc_info=True)
