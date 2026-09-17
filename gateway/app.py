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

Deferred to v2: /v1/completions, worker pools.

Error bodies are OpenAI-shaped: {"error": {"message", "type", "param",
"code"}}.
"""
from __future__ import annotations

import asyncio
import base64
import collections
import json
import logging
import struct
import threading
import time
import uuid
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from . import cloud
from . import llamaserver
from . import runpod
from .config import (PROJECT_ROOT, GatewayConfigError, default_config_raw,
                     get_gateway_config)
from .models import (EmbeddedEngine, EmbeddedLLMError, detect_gpus,
                     get_engine, installed_ggufs)

logger = logging.getLogger(__name__)

app = FastAPI(title="LLM Gateway", version="0.6.0-v2")

_LOG_PATH = PROJECT_ROOT / "data" / "gateway_log.jsonl"
_log_lock = threading.Lock()

# Concurrent chat completions in flight (queue admission control — the
# event loop is single threaded, plain int arithmetic is safe)
_active_chats = 0


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


@app.on_event("shutdown")
async def _kill_llama_servers() -> None:
    """Stop every managed llama-server child when the gateway exits."""
    llamaserver.kill_all()


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
    cfg = get_gateway_config()
    cfg.maybe_reload()
    data: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(model_id: str, owned_by: str) -> None:
        if model_id and model_id not in seen:
            seen.add(model_id)
            data.append({"id": model_id, "object": "model",
                         "created": 0, "owned_by": owned_by})

    # Aliases are the user-facing surface — listed first and winning name
    # collisions (owned_by = the alias's primary backend).
    for name, alias in cfg.aliases.items():
        chain = alias.get("chain") or []
        backend = chain[0].partition(":")[0] if chain else "gateway"
        _add(name, backend if backend in cfg.backends else "gateway")
    for bname, b in cfg.backends.items():
        for mname in (b.get("models") or {}):
            _add(mname, bname)
    # Auto-discovered GGUFs — every stem in models_dir is servable without
    # any config (aliases and config models shadow same-named stems).
    local = cfg.local_backend_name() or "local"
    for g in installed_ggufs(cfg.models_dir):
        _add(g["name"], local)
    # Configured embedding models (explicit only — never auto-discovered)
    for mname in cfg.local_embedding_models():
        _add(mname, local)
    return {"object": "list", "data": data}


# ── chain execution ────────────────────────────────────────────────────────

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
        payload: dict[str, Any] = {
            "model": mgr.alias,  # the child's --alias (its model id)
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
        resp["model"] = requested  # consistent echo; served_by carries truth
        return resp, backend_name, model_key

    if btype == "runpod" and not backend["models"][model_key].get("endpoint_id"):
        # Managed GPU POD: find-or-deploy + health-wait, then proxy in
        # native OpenAI wire format through the pod's runpod.net URL.
        runpod.ensure_reaper()
        mgr = runpod.get_manager(cfg, backend_name, model_key)
        base = await mgr.ensure_ready()
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
            m.get("timeout_s") or cfg.queue["timeout_s"])
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
    client = cloud.get_cloud_client(client_name, cloud_backend,
                                    timeout=cfg.queue["timeout_s"])
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
    global _active_chats
    cfg = get_gateway_config()
    cfg.maybe_reload()
    t0 = time.perf_counter()

    # Queue admission: capacity = resident instances (min 1) + running
    # llama-server children (they batch concurrently) + max_waiting.
    # Over-limit requests get 429 + Retry-After instead of piling onto the
    # serialized per-instance locks.
    engine = get_engine()
    capacity = (max(1, len(engine.loaded_models()))
                + llamaserver.running_count() + cfg.queue["max_waiting"])
    if _active_chats >= capacity:
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
        _active_chats += 1
        return _stream_chat(requested, entries[0], body)

    _active_chats += 1
    try:
        try:
            resp, winner, model_key = await _exec_chain(
                cfg, entries, plan["hedge_s"], body, requested)
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
        _release_chat_slot()


def _release_chat_slot() -> None:
    global _active_chats
    _active_chats = max(0, _active_chats - 1)


# ── POST /v1/chat/completions (stream) ─────────────────────────────────────

def _stream_chat(requested: str, entry: tuple[str, str],
                 body: ChatRequest) -> StreamingResponse:
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
                        _release_chat_slot()
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
                payload: dict[str, Any] = {
                    "model": mgr.alias,
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
                    and not backend["models"][model_key].get("endpoint_id")):
                # Managed GPU POD — same native SSE passthrough as the
                # llama-server subprocess, through the pod's proxy URL.
                runpod.ensure_reaper()
                mgr = runpod.get_manager(cfg, backend_name, model_key)
                base = await mgr.ensure_ready()
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
                                                  body_input, api_env, tmo)
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
                        api_env, tmo)
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
                if backend.get("type") == "runpod":
                    m = backend["models"][model_key]
                    remote_id = m["remote_id"]
                    client_name = f"runpod-{backend_name}-{model_key}"
                    cloud_backend = {
                        "base_url": runpod.serverless_base_url(m["endpoint_id"]),
                        "api_key_env": backend.get("api_key_env") or "RUNPOD_API_KEY",
                    }
                client = cloud.get_cloud_client(client_name, cloud_backend,
                                                timeout=cfg.queue["timeout_s"])
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
            _release_chat_slot()

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
    global _active_chats
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
    capacity = (max(1, len(engine.loaded_models()))
                + llamaserver.running_count() + cfg.queue["max_waiting"])
    if _active_chats >= capacity:
        _log_request(requested, "", False,
                     int((time.perf_counter() - t0) * 1000), {}, 429)
        return _error(429, "gateway busy — too many concurrent requests, "
                      "retry shortly", err_type="rate_limit_error",
                      code="rate_limited", headers={"Retry-After": "1"})

    _active_chats += 1
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
        _release_chat_slot()


# ── GET /healthz ───────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Backend status, VRAM, loaded instances (design doc)."""
    cfg = get_gateway_config()
    cfg.maybe_reload()
    engine = get_engine()

    gpus = detect_gpus()
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
        "runpod": runpod.status(),  # lifecycle-managed pods (serverless is stateless)
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
    n = engine.unload(which)
    return {"unloaded": n, "loaded": engine.loaded_models()}


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

