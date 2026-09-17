"""OpenAI-compatible HTTP surface — Stages 1-3: local + cloud, chains + hedge.

Implemented:
  GET  /v1/models           — aliases + backend models + auto-discovered
                              GGUF stems from models_dir
  POST /v1/chat/completions — non-streaming AND streaming (SSE). Embedded
                              engine (any GGUF stem in models_dir, multiple
                              models concurrently) AND openai-type cloud
                              backends. Alias chains execute with a hedge
                              race: the primary runs; after hedge_s (or on
                              its error) the next entry races it — first
                              completion wins. local_only aliases never
                              construct a cloud client. Non-streaming only
                              for chains — streaming executes the primary
                              entry. Cloud requests get max_tokens capped
                              at 800.
  GET  /healthz             — backends, loaded instances (VRAM/ctx),
                              GPUs with free VRAM, discovered model count

Every request appends one JSONL line to data/gateway_log.jsonl:
{ts, model, backend, stream, ms, usage, status}.

Arriving in later stages (per the design doc's stage plan):
  bearer auth + admin endpoints (Stage 4), console page (Stage 5).
  /v1/completions and /v1/embeddings are deferred to v2, as is streaming
  tool-call deltas (stream + tools → 400 today).

Error bodies are OpenAI-shaped: {"error": {"message", "type", "param",
"code"}}. Requests are open (localhost trust) until Stage 4 adds keys.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from . import cloud
from .config import (PROJECT_ROOT, GatewayConfigError, get_gateway_config)
from .models import (EmbeddedEngine, EmbeddedLLMError, detect_gpus,
                     get_engine, installed_ggufs)

logger = logging.getLogger(__name__)

app = FastAPI(title="LLM Gateway", version="0.3.0-stage3")

_LOG_PATH = PROJECT_ROOT / "data" / "gateway_log.jsonl"
_log_lock = threading.Lock()


# ── OpenAI-shaped errors ───────────────────────────────────────────────────

def _error(status: int, message: str, err_type: str = "invalid_request_error",
           param: str | None = None, code: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": err_type,
                           "param": param, "code": code}},
    )


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
    return {"object": "list", "data": data}


# ── chain execution ────────────────────────────────────────────────────────

async def _exec_entry(cfg, entry: tuple[str, str], body: ChatRequest,
                      requested: str) -> tuple[dict[str, Any], str, str]:
    """Execute ONE chain entry → (openai_response_dict, backend, model_key).

    Embedded → the in-process engine (tools via the JSON convention).
    Cloud → AsyncOpenAI (tools forwarded as-is, max_tokens capped 800)."""
    backend_name, model_key = entry
    backend = cfg.backends[backend_name]

    if backend.get("type") == "embedded":
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

    # openai-type cloud backend
    client = cloud.get_cloud_client(backend_name, backend,
                                    timeout=cfg.queue["timeout_s"])
    kwargs: dict[str, Any] = {
        "model": backend["models"][model_key],  # remote model id
        "messages": body.messages,
    }
    if body.temperature is not None:
        kwargs["temperature"] = body.temperature
    if body.max_tokens:
        kwargs["max_tokens"] = min(body.max_tokens, cloud.CLOUD_MAX_TOKENS_CAP)
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
        if body.tools:
            # The engine's stream path yields content deltas only — a JSON
            # tool call would arrive as plain text, not delta.tool_calls.
            _log_request(requested, "", True,
                         int((time.perf_counter() - t0) * 1000), {}, 400)
            return _error(400, "streaming with tools is not supported yet — "
                          "tool-call deltas arrive in a later stage",
                          code="stream_tools_unsupported")
        # Streaming executes the PRIMARY entry only — hedge racing is a
        # non-streaming feature (first-completion-wins has no stream analog
        # short of first-token hedging, which is a follow-up).
        return _stream_chat(requested, entries[0], body)

    try:
        resp, winner, model_key = await _exec_chain(
            cfg, entries, plan["hedge_s"], body, requested)
    except Exception as e:
        ms = int((time.perf_counter() - t0) * 1000)
        logger.warning("gateway chat failed (%s): %s", requested, e)
        _log_request(requested, "", body.stream, ms, {}, 502)
        return _error(502, str(e), err_type="server_error", code="execution_error")

    ms = int((time.perf_counter() - t0) * 1000)
    resp.setdefault("served_by", {"backend": winner, "model": model_key, "ms": ms})
    _log_request(requested, winner, body.stream, ms,
                 resp.get("usage") or {}, 200)
    return JSONResponse(resp)


# ── POST /v1/chat/completions (stream) ─────────────────────────────────────

def _stream_chat(requested: str, entry: tuple[str, str],
                 body: ChatRequest) -> StreamingResponse:
    """SSE bridge — embedded engine (think-hold-back) or cloud passthrough.

    OpenAI wire format: role chunk → content chunks → finish chunk →
    `data: [DONE]` (cloud chunks pass through in their native shape).
    Mid-flight errors become an OpenAI-shaped error object + [DONE]."""
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
                    tools=None,
                    temperature=body.temperature,
                    max_tokens=body.max_tokens,
                    stream=True,
                )
                yield _chunk({"role": "assistant", "content": ""})
                async for ch in stream:
                    for c in ch.choices:
                        text = c.delta.get("content")
                        if text:
                            yield _chunk({"content": text})
                yield _chunk({}, finish="stop")
            else:
                # cloud passthrough — the remote stream is already
                # OpenAI-chunk-shaped (role/finish chunks included)
                client = cloud.get_cloud_client(backend_name, backend,
                                                timeout=cfg.queue["timeout_s"])
                kwargs: dict[str, Any] = {
                    "model": backend["models"][model_key],
                    "messages": body.messages, "stream": True,
                }
                if body.temperature is not None:
                    kwargs["temperature"] = body.temperature
                if body.max_tokens:
                    kwargs["max_tokens"] = min(body.max_tokens,
                                               cloud.CLOUD_MAX_TOKENS_CAP)
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

    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


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
        "gpus": gpus,
    }
