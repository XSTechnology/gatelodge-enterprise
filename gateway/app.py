"""OpenAI-compatible HTTP surface — Stages 1-2: local backend, chat + SSE.

Implemented:
  GET  /v1/models           — aliases + backend models + auto-discovered
                              GGUF stems from models_dir
  POST /v1/chat/completions — non-streaming AND streaming (SSE,
                              `stream:true`); embedded backend only. Any
                              GGUF stem in models_dir is servable without
                              config; multiple models load lazily and run
                              concurrently (per-instance locks). Streamed
                              reasoning is held back — think-tags never
                              leak into content deltas.
  GET  /healthz             — backends, loaded instances (VRAM/ctx),
                              GPUs with free VRAM, discovered model count

Arriving in later stages (per the design doc's stage plan):
  cloud backends + alias chains + hedge + JSONL request log (Stage 3),
  bearer auth + admin endpoints (Stage 4), console page (Stage 5).
  /v1/completions and /v1/embeddings are deferred to v2, as is streaming
  tool-call deltas (stream + tools → 400 today).

Error bodies are OpenAI-shaped: {"error": {"message", "type", "param",
"code"}}. Requests are open (localhost trust) until Stage 4 adds keys.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from .config import GatewayConfigError, get_gateway_config
from .models import (EmbeddedEngine, EmbeddedLLMError, detect_gpus,
                     get_engine, installed_ggufs)

logger = logging.getLogger(__name__)

app = FastAPI(title="LLM Gateway", version="0.2.0-stage2")


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


# ── POST /v1/chat/completions ──────────────────────────────────────────────

@app.post("/v1/chat/completions")
async def chat_completions(body: ChatRequest) -> Response:
    cfg = get_gateway_config()
    cfg.maybe_reload()

    requested = body.model.strip() or cfg.default_alias
    try:
        backend_name, model_key = cfg.resolve_model(requested)
    except GatewayConfigError as e:
        # Auto-discovery: any GGUF in models_dir is servable by its stem
        # (aliases and config models already had their chance above).
        if requested and EmbeddedEngine.resolve_gguf(requested, cfg.models_dir):
            backend_name = cfg.local_backend_name() or "local"
            model_key = requested
        else:
            return _error(400, str(e), param="model", code="model_not_found")

    backend = cfg.backends[backend_name]
    if backend.get("type") != "embedded":
        # Resolves fine, executes later — explicit until Stage 3 lands.
        return _error(400,
                      f"backend {backend_name!r} ({backend.get('type')}) is not "
                      "available yet — cloud backends arrive in Stage 3",
                      param="model", code="backend_unavailable")
    if body.stream:
        if body.tools:
            # The engine's stream path yields content deltas only — a JSON
            # tool call would arrive as plain text, not delta.tool_calls.
            return _error(400, "streaming with tools is not supported yet — "
                          "tool-call deltas arrive in a later stage",
                          code="stream_tools_unsupported")
        return _stream_chat(requested, backend_name, model_key, body)

    engine = get_engine()
    t0 = time.perf_counter()
    try:
        comp = await engine.chat.completions.create(
            model=model_key,
            messages=body.messages,
            tools=body.tools,
            temperature=body.temperature,
            max_tokens=body.max_tokens,
        )
    except EmbeddedLLMError as e:
        logger.warning("gateway chat failed (%s / %s): %s",
                       backend_name, model_key, e)
        return _error(502, str(e), err_type="server_error", code="embedded_error")

    ms = int((time.perf_counter() - t0) * 1000)
    choice = comp.choices[0] if comp.choices else None
    if choice is None:  # unreachable with the current engine; belt and braces
        return _error(502, "empty completion", err_type="server_error")

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
    return JSONResponse({
        "id": f"chatcmpl-gw-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": requested,  # echo what the client asked for, like OpenAI
        "choices": [{
            "index": 0,
            "message": out_msg,
            "finish_reason": "tool_calls" if msg.tool_calls else choice.finish_reason,
        }],
        "usage": {
            "prompt_tokens": int(usage.get("prompt_tokens") or 0),
            "completion_tokens": int(usage.get("completion_tokens") or 0),
            "total_tokens": int(usage.get("total_tokens") or 0),
        },
        # Gateway provenance (non-standard, harmless for OpenAI clients)
        "served_by": {"backend": backend_name, "model": model_key, "ms": ms},
    })


# ── POST /v1/chat/completions (stream) ─────────────────────────────────────

def _stream_chat(requested: str, backend_name: str, model_key: str,
                 body: ChatRequest) -> StreamingResponse:
    """SSE bridge over the engine's async `_stream` (think-hold-back).

    OpenAI wire format: role chunk → content chunks → finish chunk →
    `data: [DONE]`. Mid-flight engine errors become an OpenAI-shaped
    error object followed by [DONE]."""
    engine = get_engine()
    chat_id = f"chatcmpl-gw-{uuid.uuid4().hex[:12]}"
    created = int(time.time())

    def _chunk(delta: dict[str, Any], finish: str | None = None) -> str:
        event = {"id": chat_id, "object": "chat.completion.chunk",
                 "created": created, "model": requested,
                 "choices": [{"index": 0, "delta": delta,
                              "finish_reason": finish}]}
        return f"data: {json.dumps(event)}\n\n"

    async def _gen() -> AsyncIterator[str]:
        t0 = time.perf_counter()
        try:
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
            yield "data: [DONE]\n\n"
            logger.info("gateway stream ok (%s / %s) in %dms",
                        backend_name, model_key, int((time.perf_counter() - t0) * 1000))
        except EmbeddedLLMError as e:
            logger.warning("gateway stream failed (%s / %s): %s",
                           backend_name, model_key, e)
            yield "data: " + json.dumps(
                {"error": {"message": str(e), "type": "server_error",
                           "code": "embedded_error"}}) + "\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# ── GET /healthz ───────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Backend status, VRAM, loaded instances (design doc, Stage 2)."""
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
