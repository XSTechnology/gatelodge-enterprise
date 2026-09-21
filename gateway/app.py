"""OpenAI-compatible HTTP surface — Stages 1-5 + streaming tool deltas: full gateway + console.

Implemented:
  GET  /                     — single-page console (config / models /
                              performance / logs panes); the static shell
                              loads without a key, its API calls need one
  GET  /v1/models            — aliases + backend models + auto-discovered
                              GGUF stems from models_dir
  POST /v1/chat/completions  — non-streaming AND streaming (SSE). Embedded
                              engine (any GGUF stem in models_dir, multiple
                              models concurrently) AND openai-type cloud
                              backends. Alias chains execute with a hedge
                              race; local_only aliases never construct a
                              cloud client. Cloud max_tokens capped at 800.
                              Over-capacity concurrent requests get 429 +
                              Retry-After (queue.max_waiting).
                              Streaming with tools: cloud forwarded as-is
                              (native tool-call deltas); embedded buffers
                              content deltas then emits a delta.tool_calls
                              chunk at stream end (JSON-convention shim).
  POST /v1/embeddings        — OpenAI embeddings. Embedded GGUFs configured
                              under backends.<local>.embedding_models load
                              as separate pooling instances (never
                              auto-discovered); cloud backends pass through
                              (dimensions forwarded, ignored for embedded).
                              encoding_format float|base64 supported.
  POST /v1/completions       — legacy text completions: raw prompt in,
                              text out (no chat template/tools/think-strip).
                              All backend types serve it (embedded pools,
                              llama-server children + pods natively, runpod
                              runsync generate mode, cloud passthrough);
                              executes the primary chain entry only.
  llama-server backends      — type "llama-server". SUBPROCESS mode (no
                              base_url): one managed child per model,
                              spawned lazily, continuous batching across
                              --parallel slots, native tools + streaming,
                              idle_timeout_s reaper, killed on shutdown.
                              REMOTE mode (base_url — RunPod or any
                              llama-server over HTTP): OpenAI passthrough
                              via the cloud client path (api_key_env).
  runpod backends            — type "runpod" (api_key_env). SERVERLESS
                              models ({endpoint_id, remote_id, transport}):
                              "openai" (default) passes through the
                              endpoint's /openai/v1 shim; "runsync" speaks
                              the raw job protocol (Ollama-style workers —
                              runsync + status polling, Ollama→OpenAI
                              output mapping; streaming collects then
                              emits). POD models ({gpu_type_ids, image,
                              docker_args, ...} or pod_id pin):
                              lifecycle-managed GPU pods — find-or-deploy
                              on first request, health-polled via the
                              pod's proxy.runpod.net URL, idle-stop then
                              idle-terminate.
  GET  /healthz              — backends, loaded instances (VRAM/ctx),
                              GPUs with free VRAM, discovered model count
  GET/POST /admin/aliases    — list / upsert / delete aliases (persisted to
                              data/gateway_config.json, applied immediately)
  GET/PUT /admin/config      — raw config editor (validated JSON object,
                              written + hot-reloaded)
  POST /admin/load           — preload an embedded instance (config model
                              or discovered stem)
  POST /admin/unload         — drop instances ({model} or {all: true})
  GET  /admin/stats          — per-model latency/token/error aggregates +
                              recent requests from gateway_log.jsonl
  GET  /admin/events         — recent gateway log events (in-memory ring)

Auth: bearer tokens from gateway_keys in the config (hot-reload). Empty
list = open (localhost trust). Clients pass the key as the OpenAI api_key.
All data endpoints (including /healthz) require it when configured.

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
import itertools
import json
import logging
import struct
import threading
import time
import uuid

import httpx
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
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
from . import llamaserver
from . import modelsearch
from . import providers
from . import runpod
from . import upload
from .config import (PROJECT_ROOT, GatewayConfigError, default_config_raw,
                     get_gateway_config,
                     EMBEDDED_BACKEND, OPENAI_BACKEND, LLAMA_SERVER_BACKEND,
                     RUNPOD_BACKEND, RUNPOD_PUBLIC_BACKEND)
from .models import (EmbeddedEngine, EmbeddedLLMError, detect_gpus,
                     get_engine, installed_ggufs)

logger = logging.getLogger(__name__)

app = FastAPI(title="LLM Gateway", version="0.6.1")

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


# ── Bearer auth (gateway_keys; empty list = open, localhost trust) ─────────

@app.middleware("http")
async def _bearer_auth(request: Request, call_next):
    cfg = get_gateway_config()
    cfg.maybe_reload()
    keys = cfg.gateway_keys
    if keys:
        if request.method == "GET" and request.url.path == "/":
            return await call_next(request)  # static console shell — its API
            # calls still require the key (the page prompts for it)
        auth = request.headers.get("authorization") or ""
        token = auth[7:].strip() if auth[:7].lower() == "bearer " else ""
        if token not in keys:
            return JSONResponse(
                status_code=401,
                content={"error": {
                    "message": "missing or invalid API key",
                    "type": "invalid_request_error", "param": None,
                    "code": "invalid_api_key"}})
    return await call_next(request)


@app.exception_handler(RequestValidationError)
async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    # FastAPI's default 422 body isn't OpenAI-shaped — make it so.
    msg = "; ".join(f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
                    for e in exc.errors()) or "invalid request body"
    return _error(400, msg)


# ── JSONL request log ──────────────────────────────────────────────────────

def _log_request(model: str, backend: str, stream: bool, ms: int,
                 usage: dict[str, Any], status: int) -> None:
    """One line per request — best effort, never fails the request."""
    line = json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                       "model": model, "backend": backend, "stream": stream,
                       "ms": ms, "usage": usage, "status": status})
    try:
        with _log_lock:
            _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except OSError:
        logger.warning("gateway_log write failed", exc_info=True)


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

    # ── backend models ───────────────────────────────────────────────
    for bname, backend in cfg.backends.items():
        btype = backend.get("type")
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

            elif btype == OPENAI_BACKEND:
                # Cloud backend
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
    client = cloud.get_cloud_client(client_name, cloud_backend,
                                    timeout=client_timeout)
    kwargs: dict[str, Any] = {
        "model": remote_id,  # remote model id
        "messages": body.messages,
    }
    if body.temperature is not None:
        kwargs["temperature"] = body.temperature
    if body.max_tokens:
        # cap for openai-type cloud (cost); local llama-server and runpod
        # (per-second billing) pass through
        kwargs["max_tokens"] = (min(body.max_tokens, cloud.CLOUD_MAX_TOKENS_CAP)
                                if btype == "openai" else body.max_tokens)
    if body.tools:
        kwargs["tools"] = body.tools
    try:
        resp = await client.chat.completions.create(**kwargs)
    except Exception as e:
        raise cloud.CloudBackendError(
            f"cloud backend {backend_name!r} call failed: {e}") from e
    d = (json.loads(resp.model_dump_json())
         if hasattr(resp, "model_dump_json") else dict(resp))
    d["model"] = requested  # consistent echo; served_by carries the truth
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


async def _exec_chain(cfg, entries: list[tuple[str, str]], hedge_s: float,
                      body: ChatRequest, requested: str):
    """Execute an alias chain with hedge semantics.

    Single entry → direct. Multi entry → run the primary; if it completes
    within hedge_s (or hedge_s is 0) it wins; otherwise (or on its error)
    the next entry races it — first completion wins, remaining entries
    follow the same rule."""
    if len(entries) == 1:
        return await _exec_entry(cfg, entries[0], body, requested)

    primary = asyncio.create_task(_exec_entry(cfg, entries[0], body, requested))
    if hedge_s and hedge_s > 0:
        try:
            return await asyncio.wait_for(asyncio.shield(primary), hedge_s)
        except asyncio.TimeoutError:
            pass  # hedge fires — fall through to the race below
        except asyncio.CancelledError:
            raise
        except Exception:
            primary = None  # primary failed fast — fallbacks only
    else:
        # no hedge configured: wait for the primary; on error, fall back
        try:
            return await primary
        except Exception:
            primary = None

    def _cancel_ok(entry: tuple[str, str]) -> bool:
        return (cfg.backends.get(entry[0]) or {}).get("type") != "embedded"

    last_err: BaseException | None = None
    for entry in entries[1:]:
        fb = asyncio.create_task(_exec_entry(cfg, entry, body, requested))
        racers = ([primary, fb] if primary is not None else [fb])
        try:
            return await _first_success(
                racers, [_cancel_ok(entries[0]), _cancel_ok(entry)][:len(racers)])
        except Exception as e:
            last_err = e
            primary = None
    raise last_err or RuntimeError("no chain entry succeeded")


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
    try:
        plan = cfg.resolve_chain(requested)
    except GatewayConfigError as e:
        # Auto-discovery: any GGUF in models_dir is servable by its stem
        # (aliases and config models already had their chance above).
        if requested and EmbeddedEngine.resolve_gguf(requested, cfg.models_dir):
            plan = {"entries": [(cfg.local_backend_name() or "local", requested)],
                    "hedge_s": 0.0, "local_only": False}
        else:
            _log_request(requested, "", body.stream,
                         int((time.perf_counter() - t0) * 1000), {}, 400)
            return _error(400, str(e), param="model", code="model_not_found")
    entries = plan["entries"]

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
            ms = int((time.perf_counter() - t0) * 1000)
            logger.warning("gateway chat failed (%s): %s", requested, e)
            _log_request(requested, "", body.stream, ms, {}, 502)
            return _error(502, str(e), err_type="server_error",
                          code="execution_error")
        ms = int((time.perf_counter() - t0) * 1000)
        resp.setdefault("served_by", {"backend": winner, "model": model_key,
                                      "ms": ms})
        _log_request(requested, winner, body.stream, ms,
                     resp.get("usage") or {}, 200)
        return JSONResponse(resp)
    finally:
        _release_chat_slot(slot)


# ── POST /v1/chat/completions (stream) ─────────────────────────────────────

# SSE comment cadence while a cold runpod pod boots (keep-alive so
# clients don't idle-timeout on a silent stream). Constant so tests
# can tighten it.
RUNPOD_SSE_KEEPALIVE_S = 15.0


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
        try:
            if backend.get("type") == "embedded":
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
                        _log_request(requested, backend_name, True, ms, {}, 200)
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
                    choice = resp["choices"][0]
                    if choice["message"].get("content"):
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
                choice = resp["choices"][0]
                if choice["message"].get("content"):
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
                kwargs: dict[str, Any] = {
                    "model": remote_id,
                    "messages": body.messages, "stream": True,
                }
                if body.temperature is not None:
                    kwargs["temperature"] = body.temperature
                if body.max_tokens:
                    kwargs["max_tokens"] = (min(body.max_tokens,
                                                cloud.CLOUD_MAX_TOKENS_CAP)
                                            if backend.get("type") == "openai"
                                            else body.max_tokens)
                if body.tools:
                    kwargs["tools"] = body.tools
                stream = await client.chat.completions.create(**kwargs)
                async for chunk in stream:
                    data = (chunk.model_dump_json()
                            if hasattr(chunk, "model_dump_json")
                            else json.dumps(chunk))
                    yield f"data: {data}\n\n"
            yield "data: [DONE]\n\n"
            ms = int((time.perf_counter() - t0) * 1000)
            logger.info("gateway stream ok (%s / %s) in %dms",
                        backend_name, model_key, ms)
            _log_request(requested, backend_name, True, ms, {}, 200)
        except Exception as e:
            ms = int((time.perf_counter() - t0) * 1000)
            logger.warning("gateway stream failed (%s / %s): %s",
                           backend_name, model_key, e)
            yield "data: " + json.dumps(
                {"error": {"message": str(e), "type": "server_error",
                           "code": "execution_error"}}) + "\n\n"
            yield "data: [DONE]\n\n"
            _log_request(requested, backend_name, True, ms, {}, 502)
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
    except GatewayConfigError as e:
        if requested and EmbeddedEngine.resolve_gguf(requested, cfg.models_dir):
            plan = {"entries": [(cfg.local_backend_name() or "local", requested)],
                    "hedge_s": 0.0, "local_only": False}
        else:
            _log_request(requested, "", body.stream,
                         int((time.perf_counter() - t0) * 1000), {}, 400)
            return _error(400, str(e), param="model", code="model_not_found")
    backend_name, model_key = plan["entries"][0]
    backend = cfg.backends[backend_name]
    btype = backend.get("type")

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
        client = cloud.get_cloud_client(client_name, cloud_backend,
                                        timeout=client_timeout)
        kwargs: dict[str, Any] = {"model": remote_id, "prompt": body.prompt}
        if body.temperature is not None:
            kwargs["temperature"] = body.temperature
        if body.max_tokens:
            kwargs["max_tokens"] = (min(body.max_tokens, cloud.CLOUD_MAX_TOKENS_CAP)
                                    if btype == "openai" else body.max_tokens)
        try:
            resp = await client.completions.create(**kwargs)
        except Exception as e:
            raise cloud.CloudBackendError(
                f"cloud backend {backend_name!r} completion failed: {e}") from e
        d = (json.loads(resp.model_dump_json())
             if hasattr(resp, "model_dump_json") else dict(resp))
        d["model"] = requested
        ms = int((time.perf_counter() - t0) * 1000)
        _log_request(requested, backend_name, False, ms,
                     d.get("usage") or {}, 200)
        return JSONResponse(d)
    except Exception as e:
        ms = int((time.perf_counter() - t0) * 1000)
        logger.warning("gateway completion failed (%s): %s", requested, e)
        _log_request(requested, "", body.stream, ms, {}, 502)
        return _error(502, str(e), err_type="server_error",
                      code="execution_error")
    finally:
        _release_chat_slot(slot)


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
        "bind": cfg.bind,
        "port": cfg.port,
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

def _mutate_config(mutator) -> None:
    """Read-modify-write data/gateway_config.json (created from the built-in
    defaults when absent), then force the process config to reload."""
    cfg = get_gateway_config()
    if cfg.path.is_file():
        try:
            raw = json.loads(cfg.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = default_config_raw()
        if not isinstance(raw, dict):
            raw = default_config_raw()
    else:
        raw = default_config_raw()
    mutator(raw)
    cfg.path.parent.mkdir(parents=True, exist_ok=True)
    cfg.path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    cfg.reload_now()


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
        _mutate_config(lambda raw: raw.setdefault("aliases", {}).pop(name, None))
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

    _mutate_config(_apply)
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

    keys = getattr(cfg, "gateway_keys", None) or []
    api_key = keys[0] if keys else "benchmark"
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
        return _anthropic_error(502, f"Upstream error: {e}", "api_error")
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


@app.get("/admin/config")
async def admin_get_config() -> Response:
    """Raw config text for the console editor (defaults when no file)."""
    cfg = get_gateway_config()
    text = None
    if cfg.path.is_file():
        try:
            text = cfg.path.read_text(encoding="utf-8")
        except OSError:
            text = None
    if text is None:
        text = json.dumps(default_config_raw(), indent=2)
    return {"path": str(cfg.path), "text": text}


class ConfigPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    text: str


@app.put("/admin/config")
async def admin_put_config(payload: ConfigPayload) -> Response:
    """Save the raw config text (validated as a JSON object) + reload now."""
    try:
        data = json.loads(payload.text)
    except json.JSONDecodeError as e:
        return _error(400, f"invalid JSON: {e}", param="text")
    if not isinstance(data, dict):
        return _error(400, "config must be a JSON object", param="text")
    cfg = get_gateway_config()
    cfg.path.parent.mkdir(parents=True, exist_ok=True)
    cfg.path.write_text(payload.text, encoding="utf-8")
    cfg.reload_now()
    return {"ok": True,
            "backends": {n: len(b.get("models") or {})
                         for n, b in cfg.backends.items()},
            "aliases": sorted(cfg.aliases)}


@app.get("/admin/stats")
async def admin_stats() -> Response:
    """Per-model aggregates + recent requests from gateway_log.jsonl."""
    per: dict[tuple[str, str], dict[str, Any]] = {}
    recent: list[dict[str, Any]] = []
    total = 0
    try:
        lines = _LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
    except OSError:
        lines = []
    for ln in lines[-20000:]:
        try:
            e = json.loads(ln)
        except json.JSONDecodeError:
            continue
        total += 1
        recent.append(e)
        key = (str(e.get("model") or "?"), str(e.get("backend") or "?"))
        agg = per.setdefault(key, {
            "model": key[0], "backend": key[1], "n": 0, "ms_sum": 0,
            "max_ms": 0, "total_tokens": 0, "errors": 0})
        ms = int(e.get("ms") or 0)
        agg["n"] += 1
        agg["ms_sum"] += ms
        agg["max_ms"] = max(agg["max_ms"], ms)
        agg["total_tokens"] += int((e.get("usage") or {}).get("total_tokens") or 0)
        if int(e.get("status") or 200) >= 400:
            agg["errors"] += 1
    rows = []
    for agg in per.values():
        rows.append({"model": agg["model"], "backend": agg["backend"],
                     "n": agg["n"],
                     "avg_ms": round(agg["ms_sum"] / agg["n"]) if agg["n"] else 0,
                     "max_ms": agg["max_ms"],
                     "total_tokens": agg["total_tokens"],
                     "errors": agg["errors"]})
    rows.sort(key=lambda r: -r["n"])
    return {"per_model": rows, "recent": recent[-50:], "total": total}


@app.get("/admin/events")
async def admin_events() -> Response:
    """Recent gateway log events (in-memory ring, newest last)."""
    return {"events": list(_event_ring().events)}


# ── Admin: service definitions, models, secrets (console v2) ───────────────

@app.get("/admin/backends")
async def admin_backends() -> Response:
    """Structured SERVICE DEFINITIONS for the console — every backend with
    its type, connection parameters, and models. Secrets never appear:
    cloud/runpod backends carry only the env-var NAME (api_key_env)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    out = []
    for name, b in cfg.backends.items():
        entry: dict[str, Any] = {"name": name, "type": b.get("type")}
        if b.get("type") == "embedded":
            entry["models"] = b.get("models") or {}
            entry["embedding_models"] = b.get("embedding_models") or {}
        elif b.get("type") == "llama-server":
            if b.get("base_url"):
                entry["mode"] = "remote"
                entry["base_url"] = b["base_url"]
                entry["api_key_env"] = b.get("api_key_env") or ""
                entry["models"] = b.get("models") or {}
            else:
                entry["mode"] = "subprocess"
                for k in ("server_exe", "host", "port_base", "start_timeout_s",
                          "idle_timeout_s", "extra_args"):
                    entry[k] = b.get(k)
                entry["models"] = b.get("models") or {}
        elif b.get("type") == "runpod":
            entry["api_key_env"] = b.get("api_key_env") or "RUNPOD_API_KEY"
            entry["models"] = b.get("models") or {}
        else:  # openai cloud
            entry["base_url"] = b.get("base_url") or "(OpenAI default)"
            entry["api_key_env"] = b.get("api_key_env") or ""
            entry["models"] = b.get("models") or {}
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
            raw, payload.backend, name, payload.role))
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
    elif btype == "openai":
        if payload.remote_id:
            model_cfg["name"] = payload.remote_id
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
        raw, payload.backend, name, model_cfg, target_key))
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
    backend_type: str = "embedded"  # embedded, llama-server, openai, runpod
    # llama-server subprocess
    server_exe: str = ""
    host: str = ""
    port_base: int | None = None
    start_timeout_s: float | None = None
    idle_timeout_s: float | None = None
    extra_args: str = ""  # space-separated, stored as list
    # remote (llama-server remote, openai, runpod)
    base_url: str = ""
    api_key_env: str = ""
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
        _mutate_config(lambda raw: raw.get("backends", {}).pop(name, None))
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
    elif payload.backend_type == OPENAI_BACKEND:
        if payload.base_url:
            bcfg["base_url"] = payload.base_url.rstrip("/")
        if payload.api_key_env:
            bcfg["api_key_env"] = payload.api_key_env

    _mutate_config(lambda raw: raw.setdefault("backends", {}).update({name: bcfg}))
    return {"saved": name, "type": payload.backend_type}


@app.get("/admin/secrets")
async def admin_secrets() -> Response:
    """API-key STATUS for the console — which env vars the configured
    backends need, and whether each is available (env, OS keyring via the
    boot bridge, or missing). NEVER returns key values."""
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
        in_keyring = False
        if not in_env:
            try:
                import keyring
                in_keyring = bool(keyring.get_password("Workshop", env_name))
            except Exception:
                in_keyring = False
        rows.append({
            "name": env_name,
            "backends": backends,
            "source": ("env" if in_env else
                       "keyring" if in_keyring else "missing"),
        })
    return {"api_keys": rows,
            "gateway_keys_configured": bool(cfg.gateway_keys),
            "hint": ("missing keys 502 on first use — set the env var or "
                      "store it in the OS keyring (service 'Workshop', "
                      "username = env var name) and restart the gateway")}


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
            alias=alias)
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
    rejects chunked bodies (411)."""
    import os
    from urllib.parse import quote

    pod_id = (request.headers.get("x-pod-id") or "").strip()
    filename = os.path.basename(
        (request.headers.get("x-filename") or "").strip())
    size_raw = (request.headers.get("x-file-size") or "").strip()
    if not (pod_id and filename and size_raw.isdigit()):
        return _error(400, "x-pod-id, x-filename and x-file-size headers "
                           "are required")
    url = upload.receiver_url(pod_id, quote(filename))
    try:
        async with httpx.AsyncClient(
                timeout=httpx.Timeout(600.0, connect=30.0)) as c:
            r = await c.put(url, content=request.stream(),
                            headers={"Content-Length": size_raw})
    except httpx.HTTPError as e:
        return _error(502, f"relay to receiver failed: {e}",
                      code="upload_error")
    if r.status_code != 200:
        return _error(502, f"receiver rejected upload: {r.text[:200]}",
                      code="upload_error")
    try:
        detail = r.json()
    except ValueError:
        detail = {}
    return {"ok": True, "pod_id": pod_id, "filename": filename,
            "detail": detail}


@app.get("/admin/models/upload-status")
async def admin_upload_status(pod_id: str = "",
                              filename: str = "") -> Response:
    """Bytes the receiver has so far (the console polls this during the
    relay — browser-side progress is localhost-instant and useless)."""
    if not (pod_id and filename):
        return _error(400, "pod_id and filename required")
    try:
        return await upload.receiver_status(pod_id, filename)
    except Exception as e:  # noqa: BLE001 — receiver unreachable
        return _error(502, f"receiver status failed: {e}",
                      code="upload_error")


class UploadFinalizePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pod_id: str = ""
    filename: str = ""
    size_bytes: int = 0


@app.post("/admin/models/upload-finalize")
async def admin_upload_finalize(payload: UploadFinalizePayload) -> Response:
    """Verify the uploaded size; returns the receiver's PUBLIC file URL
    — deploy-custom accepts it as file_url (the model pod curls it)."""
    if not (payload.pod_id and payload.filename and payload.size_bytes > 0):
        return _error(400, "pod_id, filename and size_bytes required")
    try:
        return await upload.finalize(payload.pod_id, payload.filename,
                                     payload.size_bytes)
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

