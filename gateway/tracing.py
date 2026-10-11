"""Gatelodge tracing — W3C trace correlation + OTLP/HTTP-JSON export.

v2.0 "The Gatehouse". Hand-rolled on purpose (zero new dependencies —
same doctrine as the hand-rolled JWKS/SCIM/SigV4 code): the OTLP
HTTP/JSON wire format is small and stable, and owning it keeps the
binary lean and auditable.

    correlation   inbound ``traceparent`` (W3C Trace Context) continues
                  the caller's trace; the ACTIVE trace id is stamped
                  into the JSONL request log (rec["trace"]) and the
                  current span id echoes back on the response — one id
                  chain links client → gateway logs → collector.

    spans         one SERVER span per HTTP request (method/path/status/
                  duration/key/session/request-id), one INTERNAL child
                  span per model call (model/backend/tokens/cost/cache)
                  from the request-log chokepoint.

    export        batches accumulate in a capped in-process buffer; a
                  daemon flusher POSTs OTLP JSON to ``otel.endpoint``
                  every ``batch_flush_s`` seconds. Telemetry, not
                  ledger: a dead collector DROPS spans (warn-throttled,
                  counted) — it can never block or OOM the gateway.

    config        the ``otel`` section (boot-time; changes need a
                  restart, like scim):
                    {"enabled": true, "endpoint": "http://host:4318/v1/traces",
                     "service_name": "gatelodge", "sample_ratio": 1.0,
                     "headers": {"x-api-key": "literal"},
                     "headers_env": {"x-api-key": "ENV_VAR_NAME"},
                     "batch_flush_s": 5, "batch_max": 64, "timeout_s": 5}

Seams (test doctrine): set_tracing()/reset_tracing() install state and
an injectable exporter — tests assert the OTLP wire JSON without a
collector, and an in-test HTTP receiver proves the real protocol.
"""
from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import secrets
import threading
import time
from collections import deque
from typing import Any, Callable

import httpx

logger = logging.getLogger(__name__)

# W3C span kinds (OTLP enum values): 1 INTERNAL, 2 SERVER, 3 CLIENT.
KIND_INTERNAL = 1
KIND_SERVER = 2
KIND_CLIENT = 3

DEFAULTS: dict[str, Any] = {
    "enabled": False, "endpoint": "", "service_name": "gatelodge",
    "sample_ratio": 1.0, "headers": {}, "headers_env": {},
    "batch_flush_s": 5.0, "batch_max": 64, "timeout_s": 5.0,
}
BUFFER_CAP = 8192          # spans retained between flushes (drop-oldest)
_EXPORTER: Callable[[list[dict]], bool] | None = None   # tests override


# ── W3C Trace Context ──────────────────────────────────────────────────────

def parse_traceparent(raw: str | None) -> tuple[str, str] | None:
    """Parse ``traceparent: 00-<32hex trace>-<16hex span>-<2hex flags>``.
    Returns (trace_id, parent_span_id) or None (absent/malformed → the
    gateway mints a fresh root trace). Tolerant per the spec: unknown
    future versions still carry usable ids."""
    if not raw:
        return None
    parts = raw.strip().split("-")
    if len(parts) != 4:
        return None
    _ver, trace_id, span_id, _flags = parts
    if len(trace_id) != 32 or len(span_id) != 16:
        return None
    if not all(c in "0123456789abcdef" for c in trace_id + span_id):
        return None
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None                      # spec: all-zero ids are invalid
    return trace_id, span_id


def format_traceparent(trace_id: str, span_id: str) -> str:
    return f"00-{trace_id}-{span_id}-01"


# ── span model ─────────────────────────────────────────────────────────────

class Span:
    __slots__ = ("trace_id", "span_id", "parent_span_id", "name", "kind",
                 "start_ns", "end_ns", "attrs", "status_code")

    def __init__(self, trace_id: str, span_id: str, parent_span_id: str | None,
                 name: str, kind: int):
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.name = name
        self.kind = kind
        self.start_ns = time.time_ns()
        self.end_ns = 0
        self.attrs: dict[str, Any] = {}
        self.status_code = 1             # 1 OK / 2 ERROR (OTLP Status)

    def end(self, status_code: int | None = None) -> None:
        if not self.end_ns:
            self.end_ns = time.time_ns()
        if status_code is not None:
            self.status_code = status_code

    def set(self, key: str, value: Any) -> None:
        if value is not None:
            self.attrs[key] = value


def _new_id(n_bytes: int) -> str:
    return secrets.token_hex(n_bytes)


def _sampled(trace_id: str, ratio: float) -> bool:
    """Deterministic per-trace sampling — the same trace id always gets
    the same decision (stable across replicas reading one header)."""
    if ratio >= 1.0:
        return True
    if ratio <= 0.0:
        return False
    score = int(hashlib.sha256(trace_id.encode()).hexdigest()[:8], 16)
    return (score / 0xFFFFFFFF) < ratio


# ── state + buffer + flusher ───────────────────────────────────────────────

_CURRENT_SPAN = contextvars.ContextVar("gw_current_span", default=None)

_config: dict[str, Any] = dict(DEFAULTS)
_resource: dict[str, str] = {}
_counters: dict[str, int] = {"spans_created": 0, "spans_exported": 0,
                             "spans_dropped_buffer": 0,
                             "spans_dropped_export": 0, "export_errors": 0}
_buffer: deque[Span] = deque()
_lock = threading.Lock()
_wake = threading.Event()
_flusher: threading.Thread | None = None
_last_warn = 0.0


def _warn_throttled(msg: str, *args: Any) -> None:
    global _last_warn
    now = time.monotonic()
    if now - _last_warn > 60.0:
        _last_warn = now
        logger.warning(msg, *args)
    else:
        logger.debug(msg, *args)


def _resolved_headers() -> dict[str, str]:
    """Literal headers + headers_env resolved ONCE at configure time —
    the api_key_env convention: env vars hold secrets, never the config
    file. Real env vars always win over config literals."""
    out = {str(k): str(v) for k, v in (_config.get("headers") or {}).items()}
    for k, env in (_config.get("headers_env") or {}).items():
        import os
        val = os.environ.get(str(env))
        if val:
            out[str(k)] = val
    return out


def _otlp_payload(spans: list[Span]) -> dict[str, Any]:
    """Hand-rolled OTLP/HTTP JSON (protobuf-JSON): int64 fields as
    strings, attribute values typed per the AnyValue shape."""
    def attr(key: str, value: Any) -> dict[str, Any]:
        if isinstance(value, bool):
            v = {"boolValue": value}
        elif isinstance(value, int):
            v = {"intValue": str(value)}
        else:
            v = {"stringValue": str(value)}
        return {"key": key, "value": v}

    def span_json(s: Span) -> dict[str, Any]:
        out: dict[str, Any] = {
            "traceId": s.trace_id, "spanId": s.span_id,
            "name": s.name, "kind": s.kind,
            "startTimeUnixNano": str(s.start_ns),
            "endTimeUnixNano": str(s.end_ns or s.start_ns),
            "attributes": [attr(k, v) for k, v in s.attrs.items()],
            "status": {"code": s.status_code},
        }
        if s.parent_span_id:
            out["parentSpanId"] = s.parent_span_id
        return out

    res_attrs = [attr(k, v) for k, v in _resource.items()]
    return {"resourceSpans": [{
        "resource": {"attributes": res_attrs},
        "scopeSpans": [{"scope": {"name": "gatelodge.tracing",
                                  "version": "1"},
                        "spans": [span_json(s) for s in spans]}]}]}


def _export(spans: list[Span]) -> bool:
    global _EXPORTER
    payload = _otlp_payload(spans)
    if _EXPORTER is not None:
        try:
            ok = bool(_EXPORTER(payload))
            if ok:
                _counters["spans_exported"] += len(spans)
            else:
                _counters["spans_dropped_export"] += len(spans)
            return ok
        except Exception:                                  # noqa: BLE001
            _counters["export_errors"] += 1
            _warn_throttled("OTLP test exporter failed: %s", "exception")
            return False
    endpoint = str(_config.get("endpoint") or "")
    if not endpoint:
        return False
    try:
        r = httpx.post(endpoint, json=payload,
                       headers={"Content-Type": "application/json",
                                **_resolved_headers()},
                       timeout=float(_config.get("timeout_s") or 5.0))
        if r.status_code < 300:
            _counters["spans_exported"] += len(spans)
            return True
        _counters["spans_dropped_export"] += len(spans)
        _warn_throttled("OTLP export rejected: HTTP %d", r.status_code)
        return False
    except Exception:                                      # noqa: BLE001
        _counters["export_errors"] += 1
        _counters["spans_dropped_export"] += len(spans)
        _warn_throttled("OTLP export failed (endpoint unreachable?) — "
                        "dropping spans")
        return False


def _flush_locked() -> None:
    """Drain the buffer under the lock (chunked by batch_max)."""
    batch_max = max(1, int(_config.get("batch_max") or 64))
    while _buffer:
        chunk: list[Span] = []
        while _buffer and len(chunk) < batch_max:
            chunk.append(_buffer.popleft())
        _export(chunk)


def _flusher_loop() -> None:
    while True:
        # interval re-read EVERY cycle — configure() may replace the
        # config under a long-lived thread (restart-free tuning)
        interval = max(0.5, float(_config.get("batch_flush_s") or 5.0))
        _wake.wait(timeout=interval)
        _wake.clear()
        with _lock:
            if _buffer:
                _flush_locked()


def configure(cfg: dict[str, Any] | None, *, version: str = "",
              edition: str = "") -> None:
    """Install tracing state from the normalized ``otel`` config section
    (boot-time; section changes need a restart — same as scim). Starts
    the daemon flusher; a second call replaces state (tests, reload)."""
    global _flusher, _resource, _EXPORTER
    with _lock:
        _flush_locked() if _buffer else None            # drain the old
    raw = cfg if isinstance(cfg, dict) else {}
    cfgd = dict(DEFAULTS)
    for k in ("enabled", "endpoint", "service_name", "headers",
              "headers_env", "batch_flush_s", "batch_max", "timeout_s"):
        if k in raw:
            cfgd[k] = raw[k]
    try:
        cfgd["sample_ratio"] = max(0.0, min(1.0, float(raw.get("sample_ratio",
                                                              1.0))))
    except (TypeError, ValueError):
        cfgd["sample_ratio"] = 1.0
    enabled = bool(cfgd["enabled"]) and bool(str(cfgd["endpoint"]).strip())
    if bool(cfgd["enabled"]) and not enabled:
        logger.warning("otel.enabled but no endpoint set — tracing stays "
                       "OFF")
    cfgd["enabled"] = enabled
    _config.clear()
    _config.update(cfgd)
    _resource = {"service.name": str(cfgd["service_name"] or "gatelodge")}
    if version:
        _resource["service.version"] = version
    if edition:
        _resource["gatelodge.edition"] = edition
    _EXPORTER = None                       # real httpx path unless overridden
    if enabled and (_flusher is None or not _flusher.is_alive()):
        _flusher = threading.Thread(target=_flusher_loop, daemon=True,
                                    name="gatelodge-otlp")
        _flusher.start()
    if enabled:
        logger.info("tracing: OTLP export to %s (service=%s sample=%.2f)",
                    cfgd["endpoint"], cfgd["service_name"],
                    cfgd["sample_ratio"])


def shutdown_flush() -> None:
    """Best-effort final flush (app shutdown hook)."""
    with _lock:
        if _buffer:
            _flush_locked()


# ── request-path API (middleware + request-log chokepoint) ─────────────────

def request_span(method: str, path: str, traceparent: str | None) -> Span | None:
    """Start the SERVER span for an HTTP request. Continues the inbound
    W3C trace when a valid traceparent arrived; samples per trace id;
    returns None when tracing is off / unsampled (the caller must then
    touch nothing — zero overhead path)."""
    if not _config.get("enabled"):
        return None
    if traceparent:
        parsed = parse_traceparent(traceparent)
        trace_id, parent = parsed if parsed else (_new_id(16), None)
    else:
        trace_id, parent = _new_id(16), None
    if not _sampled(trace_id, float(_config.get("sample_ratio", 1.0))):
        return None
    span = Span(trace_id, _new_id(8), parent,
                f"{method} {path}", KIND_SERVER)
    span.set("http.request.method", method)
    span.set("http.route", path)
    _CURRENT_SPAN.set(span)
    _counters["spans_created"] += 1
    return span


def finish_request_span(span: Span | None, status_code: int) -> None:
    """End + enqueue the server span (ERROR status for 5xx)."""
    if span is None:
        return
    span.end(status_code=2 if status_code >= 500 else 1)
    enqueue(span)


def enqueue(span: Span) -> None:
    with _lock:
        if len(_buffer) >= BUFFER_CAP:
            _buffer.popleft()
            _counters["spans_dropped_buffer"] += 1
        _buffer.append(span)
    _wake.set()


def current_span() -> Span | None:
    return _CURRENT_SPAN.get()


def current_trace_id() -> str | None:
    s = _CURRENT_SPAN.get()
    return s.trace_id if s else None


def add_span(name: str, kind: int = KIND_INTERNAL,
             attrs: dict[str, Any] | None = None,
             parent: Span | None = None) -> None:
    """Standalone span for non-request work (v2.0: enterprise MCP tool
    calls ride the ACTIVE request span when present). No-op when
    tracing is off; never raises."""
    if not _config.get("enabled"):
        return
    try:
        p = parent or _CURRENT_SPAN.get()
        span = Span(p.trace_id if p else _new_id(16), _new_id(8),
                    p.span_id if p else None, name, kind)
        for k, v in (attrs or {}).items():
            span.set(k, v)
        span.end()
        _counters["spans_created"] += 1
        enqueue(span)
    except Exception:                                  # noqa: BLE001
        pass


def add_llm_span(model: str, backend: str, ms: int, usage: dict[str, Any],
                 status: int, cache_hit: bool = False,
                 cost_usd: float | None = None) -> None:
    """INTERNAL child span for one model call — emitted from the request
    log chokepoint (every completion, whatever its status). No-op when
    the request was unsampled/tracing off."""
    parent = _CURRENT_SPAN.get()
    if parent is None or not _config.get("enabled"):
        return
    span = Span(parent.trace_id, _new_id(8), parent.span_id,
                f"llm {model}", KIND_INTERNAL)
    span.set("llm.model", model)
    span.set("llm.backend", backend)
    span.set("llm.duration_ms", int(ms))
    u = usage or {}
    span.set("llm.prompt_tokens", int(u.get("prompt_tokens") or 0))
    span.set("llm.completion_tokens", int(u.get("completion_tokens") or 0))
    span.set("llm.total_tokens", int(u.get("total_tokens") or 0))
    if cache_hit:
        span.set("llm.cache_hit", True)
    if cost_usd is not None:
        span.set("llm.cost_usd", round(cost_usd, 6))
    span.end(2 if status >= 500 else 1)
    _counters["spans_created"] += 1
    enqueue(span)


def status() -> dict[str, Any]:
    """Full tracing status (GET /admin/tracing — admin-gated; the
    endpoint string may name an internal collector)."""
    return {"enabled": bool(_config.get("enabled")),
            "endpoint": _config.get("endpoint") or "",
            "service_name": _config.get("service_name"),
            "sample_ratio": _config.get("sample_ratio"),
            "buffered": len(_buffer), **dict(_counters)}


def healthz_status() -> dict[str, Any]:
    """/healthz-safe subset — NO endpoint string (healthz is
    unauthenticated)."""
    return {"enabled": bool(_config.get("enabled")),
            "exported": _counters["spans_exported"],
            "dropped": (_counters["spans_dropped_buffer"]
                        + _counters["spans_dropped_export"])}


def flush() -> None:
    """Drain the buffer now (tests; also the shutdown path)."""
    with _lock:
        if _buffer:
            _flush_locked()


# ── test seams ─────────────────────────────────────────────────────────────

def set_tracing(cfg: dict[str, Any] | None,
                exporter: Callable[[dict], bool] | None = None) -> None:
    """Install tracing state + an in-process exporter (tests assert the
    OTLP payload without a collector). configure() resets to the real
    httpx path."""
    global _EXPORTER
    configure(cfg)
    _EXPORTER = exporter
    if exporter is not None:              # immediate-mode for determinism
        _config["batch_flush_s"] = 0.0


def reset_tracing() -> None:
    """Back to disabled defaults (test teardown)."""
    global _EXPORTER
    with _lock:
        if _buffer:
            _buffer.clear()
    _config.clear()
    _config.update(DEFAULTS)
    _resource.clear()
    for k in _counters:
        _counters[k] = 0
    _EXPORTER = None
    _wake.set()
