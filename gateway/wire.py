"""Wire-format translator backends (v2.3) — non-OpenAI upstream APIs.

Upstream providers that speak their OWN wire formats; this module
translates OpenAI chat (the gateway's internal shape) to each and
back, so they ride the same backend model, budgets, licensing and
logging as every other backend:

    anthropic — the Anthropic Messages API (POST {base}/v1/messages,
    x-api-key + anthropic-version). The OUTBOUND half of the inbound
    /v1/messages shim in app.py: system extraction, tool_use /
    tool_result blocks, input_schema tools. max_tokens is REQUIRED by
    the API — defaults to 1024 when the client omits it.

    gemini — Google's Interactions API (GA June 2026; generateContent
    is legacy — Google steers all new work here). POST
    {base}/v1beta2/interactions with x-goog-api-key; input turns +
    system_instruction + function tools; the steps timeline
    (model_output / function_call) maps back to OpenAI shape.
    STATELESS (store=false): the gateway owns conversation history,
    like every other backend — no server-side state is created.

    responses — OpenAI's Responses API (POST {base}/responses): the
    forward path OpenAI steers toward. instructions + typed Items,
    function_call / function_call_output, store=false stateless,
    reasoning Items skipped.

    vertex — Vertex AI Interactions: the gemini payload against
    aiplatform project URLs (x-goog-api-key express or ADC Bearer).

    bedrock — AWS Bedrock Converse (POST
    {base}/model/{modelId}/converse): the native type behind the
    bedrock-mantle shortcut. SigV4 (env-var chain, service 'bedrock')
    or x-api-key; toolUse / toolResult blocks; stopReason mapping.

All resolve keys through cloud.resolve_backend_key (env → file →
cmd → keyring) and raise cloud.CloudBackendError on failure (→ 502).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any
from uuid import uuid4

import httpx

from . import cloud

logger = logging.getLogger(__name__)

ANTHROPIC_VERSION = "2023-06-01"
ANTHROPIC_DEFAULT_MAX_TOKENS = 1024
GEMINI_INTERACTIONS_PATH = "/v1beta2/interactions"


async def _post_json(url: str, headers: dict[str, str],
                     payload: dict[str, Any], timeout: float,
                     provider: str,
                     auth: Any = None) -> dict[str, Any]:
    """One POST, JSON both ways; CloudBackendError on any failure
    (classified downstream by the caller's error handling). ``auth`` is
    an optional httpx.Auth (the AWS SigV4 per-request signer for the
    native bedrock type)."""
    try:
        async with httpx.AsyncClient(timeout=timeout, auth=auth) as hc:
            r = await hc.post(url, headers=headers,
                              json=payload)
        if r.status_code >= 400:
            body = (r.text or "")[:300]
            raise cloud.CloudBackendError(
                f"{provider} backend call failed: HTTP {r.status_code} "
                f"{body}")
        return r.json()
    except cloud.CloudBackendError:
        raise
    except Exception as e:                       # noqa: BLE001
        raise cloud.CloudBackendError(
            f"{provider} backend call failed: {e}") from e


# ── anthropic: OpenAI → Messages → OpenAI ─────────────────────────────────

def _oai_messages_to_anthropic(
        messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """OpenAI messages → (system, Messages-API messages). System role
    extracts to the top-level system param; tool_calls become
    tool_use blocks on the assistant turn; tool results become
    tool_result blocks on a USER turn (the API's requirement)."""
    system_parts: list[str] = []
    out: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if role == "system":
            if isinstance(content, str) and content.strip():
                system_parts.append(content)
            continue
        if role == "tool":
            # tool result → user turn with a tool_result block
            out.append({"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": str(m.get("tool_call_id") or ""),
                "content": [{"type": "text",
                             "text": content if isinstance(content, str)
                             else json.dumps(content)}]}]})
            continue
        if role == "assistant":
            blocks: list[dict[str, Any]] = []
            if isinstance(content, str) and content.strip():
                blocks.append({"type": "text", "text": content})
            elif isinstance(content, list):
                blocks.extend(content)
            for tc in (m.get("tool_calls") or []):
                fn = (tc or {}).get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {"_raw": fn.get("arguments")}
                blocks.append({"type": "tool_use",
                               "id": str(tc.get("id") or
                                         f"toolu_{uuid4().hex[:12]}"),
                               "name": fn.get("name") or "",
                               "input": args})
            out.append({"role": "assistant",
                        "content": blocks or [{"type": "text",
                                               "text": ""}]})
            continue
        # user (content may be a string or OpenAI content parts)
        if isinstance(content, str):
            out.append({"role": "user", "content": content})
        else:
            out.append({"role": "user", "content": content or ""})
    return "\n\n".join(system_parts), out


def _oai_tools_to_anthropic(tools: list[dict[str, Any]]
                            ) -> list[dict[str, Any]]:
    out = []
    for t in tools or []:
        fn = (t or {}).get("function") or {}
        if not fn.get("name"):
            continue
        out.append({"name": fn["name"],
                    "description": fn.get("description") or "",
                    "input_schema": fn.get("parameters")
                    or {"type": "object"}})
    return out


def _anthropic_to_oai(resp: dict[str, Any], model: str) -> dict[str, Any]:
    """Messages-API response → OpenAI chat-completion shape."""
    text_parts: list[str] = []
    tool_calls = []
    for b in (resp.get("content") or []):
        if b.get("type") == "text":
            text_parts.append(str(b.get("text") or ""))
        elif b.get("type") == "tool_use":
            tool_calls.append({
                "id": str(b.get("id") or f"call_{uuid4().hex[:12]}"),
                "type": "function",
                "function": {"name": b.get("name") or "",
                             "arguments": json.dumps(
                                 b.get("input") or {})}})
    stop = resp.get("stop_reason")
    finish = ("tool_calls" if stop == "tool_use"
              else "length" if stop == "max_tokens" else "stop")
    msg: dict[str, Any] = {
        "role": "assistant",
        "content": ("\n".join(text_parts) or None) if tool_calls
        else ("\n".join(text_parts))}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    usage = resp.get("usage") or {}
    return {
        "id": str(resp.get("id") or f"chatcmpl-gw-{uuid4().hex[:12]}"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": msg,
                     "finish_reason": finish}],
        "usage": {
            "prompt_tokens": int(usage.get("input_tokens") or 0),
            "completion_tokens": int(usage.get("output_tokens") or 0),
            "total_tokens": int(usage.get("input_tokens") or 0)
            + int(usage.get("output_tokens") or 0)},
    }


async def anthropic_chat(backend: dict[str, Any], model: str,
                         messages: list[dict[str, Any]],
                         temperature: float | None,
                         max_tokens: int | None,
                         tools: list[dict[str, Any]] | None,
                         timeout: float) -> dict[str, Any]:
    """One Anthropic Messages round-trip → OpenAI-shaped response."""
    api_key = cloud.resolve_backend_key(backend)
    if not api_key:
        env = backend.get("api_key_env") or "ANTHROPIC_API_KEY"
        raise cloud.CloudBackendError(
            f"anthropic backend: no API key (set {env} or the keyring)")
    base = str(backend.get("base_url") or
               "https://api.anthropic.com").rstrip("/")
    system, msgs = _oai_messages_to_anthropic(messages)
    payload: dict[str, Any] = {
        "model": model,
        # the Messages API REQUIRES max_tokens — default when omitted
        "max_tokens": int(max_tokens or ANTHROPIC_DEFAULT_MAX_TOKENS),
        "messages": msgs}
    if system:
        payload["system"] = system
    if temperature is not None:
        payload["temperature"] = temperature
    a_tools = _oai_tools_to_anthropic(tools or [])
    if a_tools:
        payload["tools"] = a_tools
    resp = await _post_json(
        f"{base}/v1/messages",
        {"x-api-key": api_key,
         "anthropic-version": ANTHROPIC_VERSION,
         "content-type": "application/json"},
        payload, timeout, "anthropic")
    return _anthropic_to_oai(resp, model)


# ── gemini: OpenAI → Interactions API → OpenAI ────────────────────────────

def _oai_messages_to_gemini(
        messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """OpenAI messages → (system_instruction, input turns). Tool
    results ride the input array as function_result items (the
    Interactions shape); assistant tool calls echo as model-turn
    function_call content."""
    system_parts: list[str] = []
    out: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if role == "system":
            if isinstance(content, str) and content.strip():
                system_parts.append(content)
            continue
        if role == "tool":
            out.append({
                "type": "function_result",
                "call_id": str(m.get("tool_call_id") or ""),
                "name": str(m.get("name") or ""),
                "result": [{"type": "text",
                            "text": content if isinstance(content, str)
                            else json.dumps(content)}]})
            continue
        if role == "assistant" and m.get("tool_calls"):
            blocks = []
            if isinstance(content, str) and content.strip():
                blocks.append({"type": "text", "text": content})
            for tc in m["tool_calls"]:
                fn = (tc or {}).get("function") or {}
                blocks.append({"type": "function_call",
                               "call_id": str(tc.get("id") or ""),
                               "name": fn.get("name") or "",
                               "arguments": fn.get("arguments") or "{}"})
            out.append({"role": "model", "content": blocks})
            continue
        out.append({
            "role": "user" if role == "user" else "model",
            "content": [{"type": "text",
                         "text": content if isinstance(content, str)
                         else json.dumps(content)}]
            if content is not None else []})
    return "\n\n".join(system_parts), out


def _oai_tools_to_gemini(tools: list[dict[str, Any]]
                         ) -> list[dict[str, Any]]:
    out = []
    for t in tools or []:
        fn = (t or {}).get("function") or {}
        if not fn.get("name"):
            continue
        out.append({"type": "function",
                    "name": fn["name"],
                    "description": fn.get("description") or "",
                    "parameters": fn.get("parameters")
                    or {"type": "object"}})
    return out


def _gemini_to_oai(resp: dict[str, Any], model: str) -> dict[str, Any]:
    """Interactions response (steps timeline) → OpenAI shape. Text =
    model_output steps' text blocks; tool_calls = function_call
    steps; usage from usage / usageMetadata (both shapes seen)."""
    text_parts: list[str] = []
    tool_calls = []
    for step in (resp.get("steps") or []):
        stype = step.get("type")
        if stype == "model_output":
            for c in (step.get("content") or []):
                if c.get("type") == "text":
                    text_parts.append(str(c.get("text") or ""))
        elif stype == "function_call":
            tool_calls.append({
                "id": str(step.get("call_id")
                          or f"call_{uuid4().hex[:12]}"),
                "type": "function",
                "function": {"name": step.get("name") or "",
                             "arguments": json.dumps(
                                 step.get("arguments") or {})}})
    msg: dict[str, Any] = {
        "role": "assistant",
        "content": ("\n".join(text_parts) or None) if tool_calls
        else ("\n".join(text_parts))}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    usage = resp.get("usage") or {}
    if not usage and resp.get("usageMetadata"):
        um = resp["usageMetadata"]
        usage = {"prompt_tokens": um.get("promptTokenCount"),
                 "completion_tokens": um.get("candidatesTokenCount"),
                 "total_tokens": um.get("totalTokenCount")}
    return {
        "id": str(resp.get("id") or f"chatcmpl-gw-{uuid4().hex[:12]}"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": msg,
                     "finish_reason":
                         "tool_calls" if tool_calls else "stop"}],
        "usage": {
            "prompt_tokens": int(usage.get("prompt_tokens") or 0),
            "completion_tokens": int(usage.get("completion_tokens") or 0),
            "total_tokens": int(usage.get("total_tokens")
                                or (int(usage.get("prompt_tokens") or 0)
                                    + int(usage.get("completion_tokens")
                                          or 0)))},
    }


async def gemini_chat(backend: dict[str, Any], model: str,
                      messages: list[dict[str, Any]],
                      temperature: float | None,
                      max_tokens: int | None,
                      tools: list[dict[str, Any]] | None,
                      timeout: float) -> dict[str, Any]:
    """One Gemini Interactions round-trip → OpenAI-shaped response.
    STATELESS: store=false — the gateway owns history, no server-side
    interaction state is created."""
    api_key = cloud.resolve_backend_key(backend)
    if not api_key:
        env = backend.get("api_key_env") or "GEMINI_API_KEY"
        raise cloud.CloudBackendError(
            f"gemini backend: no API key (set {env} or the keyring)")
    base = str(backend.get("base_url") or
               "https://generativelanguage.googleapis.com").rstrip("/")
    system, turns = _oai_messages_to_gemini(messages)
    payload: dict[str, Any] = {
        "model": model,
        "input": turns,
        # stateless by design — the gateway manages history like every
        # other backend; no server-side interaction state
        "store": False}
    if system:
        payload["system_instruction"] = system
    g_tools = _oai_tools_to_gemini(tools or [])
    if g_tools:
        payload["tools"] = g_tools
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens:
        payload["max_output_tokens"] = int(max_tokens)
    resp = await _post_json(
        f"{base}{GEMINI_INTERACTIONS_PATH}",
        {"x-goog-api-key": api_key,
         "content-type": "application/json"},
        payload, timeout, "gemini")
    return _gemini_to_oai(resp, model)


# ── responses: OpenAI Chat → the Responses API → OpenAI Chat ──────────────
#
# The Responses API (/v1/responses) is OpenAI's forward path — new
# capabilities (Ultrafast mode, latest-model tooling) land there first
# and the Assistants API has been sunset. Bedrock's mantle endpoint
# speaks it too. STATELESS here (store: false): the gateway owns the
# transcript like every other backend — no previous_response_id, no
# server-side state.

def _oai_messages_to_responses(
        messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """OpenAI messages → (instructions, input Items). System extracts
    to the top-level instructions param; tool calls become
    function_call Items; tool results become function_call_output
    Items linked by call_id."""
    system_parts: list[str] = []
    items: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if role == "system":
            if isinstance(content, str) and content.strip():
                system_parts.append(content)
            continue
        if role == "tool":
            items.append({
                "type": "function_call_output",
                "call_id": str(m.get("tool_call_id") or ""),
                "output": content if isinstance(content, str)
                else json.dumps(content)})
            continue
        if role == "assistant" and m.get("tool_calls"):
            if isinstance(content, str) and content.strip():
                items.append({"type": "message", "role": "assistant",
                              "content": content})
            for tc in m["tool_calls"]:
                fn = (tc or {}).get("function") or {}
                items.append({
                    "type": "function_call",
                    "call_id": str(tc.get("id") or ""),
                    "name": fn.get("name") or "",
                    "arguments": fn.get("arguments") or "{}"})
            continue
        items.append({
            "type": "message",
            "role": "user" if role == "user" else "assistant",
            "content": content if isinstance(content, str)
            else json.dumps(content) if content is not None else ""})
    return "\n\n".join(system_parts), items


def _oai_tools_to_responses(tools: list[dict[str, Any]]
                            ) -> list[dict[str, Any]]:
    """OpenAI's nested {type: function, function: {...}} → the
    Responses flattened {type: function, name, description,
    parameters}."""
    out = []
    for t in tools or []:
        fn = (t or {}).get("function") or {}
        if not fn.get("name"):
            continue
        out.append({"type": "function",
                    "name": fn["name"],
                    "description": fn.get("description") or "",
                    "parameters": fn.get("parameters")
                    or {"type": "object"}})
    return out


def _responses_to_oai(resp: dict[str, Any], model: str) -> dict[str, Any]:
    """The typed output array → OpenAI chat-completion shape.
    message Items carry output_text content; function_call Items carry
    tool calls; reasoning Items are skipped (internal)."""
    text_parts: list[str] = []
    tool_calls = []
    for item in (resp.get("output") or []):
        itype = item.get("type")
        if itype == "message":
            for c in (item.get("content") or []):
                if c.get("type") in ("output_text", "text"):
                    text_parts.append(str(c.get("text") or ""))
                elif isinstance(c.get("text"), str) and not c.get("type"):
                    text_parts.append(c["text"])
        elif itype == "function_call":
            tool_calls.append({
                "id": str(item.get("call_id")
                          or f"call_{uuid4().hex[:12]}"),
                "type": "function",
                "function": {"name": item.get("name") or "",
                             "arguments": str(
                                 item.get("arguments") or "{}")}})
    msg: dict[str, Any] = {
        "role": "assistant",
        "content": ("\n".join(text_parts) or None) if tool_calls
        else ("\n".join(text_parts))}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    usage = resp.get("usage") or {}
    status = str(resp.get("status") or "")
    finish = ("tool_calls" if tool_calls
              else "length" if status == "incomplete" else "stop")
    return {
        "id": str(resp.get("id") or f"chatcmpl-gw-{uuid4().hex[:12]}"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": msg,
                     "finish_reason": finish}],
        "usage": {
            "prompt_tokens": int(usage.get("input_tokens") or 0),
            "completion_tokens": int(usage.get("output_tokens") or 0),
            "total_tokens": int(usage.get("total_tokens")
                                or (int(usage.get("input_tokens") or 0)
                                    + int(usage.get("output_tokens")
                                          or 0)))},
    }


async def responses_chat(backend: dict[str, Any], model: str,
                         messages: list[dict[str, Any]],
                         temperature: float | None,
                         max_tokens: int | None,
                         tools: list[dict[str, Any]] | None,
                         timeout: float) -> dict[str, Any]:
    """One Responses-API round-trip → OpenAI-shaped response.
    STATELESS: store=false — the gateway owns the transcript, no
    previous_response_id server state is created."""
    api_key = cloud.resolve_backend_key(backend)
    if not api_key:
        env = backend.get("api_key_env") or "OPENAI_API_KEY"
        raise cloud.CloudBackendError(
            f"responses backend: no API key (set {env}, paste it in "
            "the console, or mount api_key_file)")
    base = str(backend.get("base_url")
               or "https://api.openai.com/v1").rstrip("/")
    instructions, items = _oai_messages_to_responses(messages)
    payload: dict[str, Any] = {
        "model": model,
        "input": items,
        # stateless by design — the gateway manages history like
        # every other backend
        "store": False}
    if instructions:
        payload["instructions"] = instructions
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens:
        payload["max_output_tokens"] = int(max_tokens)
    r_tools = _oai_tools_to_responses(tools or [])
    if r_tools:
        payload["tools"] = r_tools
    resp = await _post_json(
        f"{base}/responses",
        {"Authorization": f"Bearer {api_key}",
         "content-type": "application/json"},
        payload, timeout, "responses")
    return _responses_to_oai(resp, model)


# ── vertex: the Gemini Interactions API on GCP (same payload, different
# URL + Bearer auth). Preview: global location only.
# https://aiplatform.googleapis.com/v1beta1/projects/{p}/locations/global/interactions

async def vertex_chat(backend: dict[str, Any], model: str,
                      messages: list[dict[str, Any]],
                      temperature: float | None,
                      max_tokens: int | None,
                      tools: list[dict[str, Any]] | None,
                      timeout: float) -> dict[str, Any]:
    """Vertex AI Interactions (GCP) — the SAME wire shape as the AI
    Studio gemini backend (payload differences are limited to the URL
    and auth), so this reuses the gemini translators wholesale:
    system_instruction + input turns + function tools, store=false,
    steps timeline → OpenAI chat. Auth: an express-mode API key
    (api_key_env) or the ADC/service-account Bearer token."""
    project = str(backend.get("project") or "").strip()
    if not project:
        raise cloud.CloudBackendError(
            "vertex backend: project is required (the GCP project id)")
    # express-mode API key wins; ADC (service account / workload
    # identity via credchains) fills the gap
    api_key = cloud.resolve_backend_key(backend)
    headers: dict[str, str] = {"content-type": "application/json"}
    if api_key:
        headers["x-goog-api-key"] = api_key
    else:
        try:
            from .credchains import _adc_fetch_token
            token, _ = _adc_fetch_token()
            headers["Authorization"] = f"Bearer {token}"
        except Exception as e:                       # noqa: BLE001
            raise cloud.CloudBackendError(
                f"vertex backend: no API key and ADC failed ({e}) "
                "— set the express key or GOOGLE_APPLICATION_"
                "CREDENTIALS") from e
    system, turns = _oai_messages_to_gemini(messages)
    payload: dict[str, Any] = {
        "model": model,
        "input": turns,
        "store": False}          # stateless — the gateway owns history
    if system:
        payload["system_instruction"] = system
    g_tools = _oai_tools_to_gemini(tools or [])
    if g_tools:
        payload["tools"] = g_tools
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens:
        payload["max_output_tokens"] = int(max_tokens)
    url = (f"https://aiplatform.googleapis.com/v1beta1/projects/"
           f"{project}/locations/global/interactions")
    resp = await _post_json(url, headers, payload, timeout, "vertex")
    return _gemini_to_oai(resp, model)


# ── bedrock: OpenAI → Converse API → OpenAI (v2.3 native) ─────────────────
#
# The MANTLE shortcut (an openai-type backend + service: bedrock-mantle
# sigv4 override) covers customers already on the mantle endpoints.
# The NATIVE type here talks bedrock-runtime Converse directly — one
# consistent request/response across every Bedrock model, no extra AWS
# service in the path. Auth: AWS SigV4 (env-var credential chain) or,
# when the backend has a key configured, the x-api-key header.

BEDROCK_DEFAULT_MAX_TOKENS = 1024

_BEDROCK_STOP_MAP = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "tool_use": "tool_calls",
    "max_tokens": "length",
    "content_filtered": "content_filter",
    "guardrail_intervened": "content_filter",
}


def _oai_messages_to_bedrock(
        messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """OpenAI messages → (system, Converse messages). System extracts
    to the top-level system list; tool_calls become toolUse blocks on
    the assistant turn; tool results become toolResult blocks on a
    USER turn (the API's requirement)."""
    system_parts: list[str] = []
    out: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if role == "system":
            if isinstance(content, str) and content.strip():
                system_parts.append(content)
            continue
        if role == "tool":
            out.append({"role": "user", "content": [{
                "toolResult": {
                    "toolUseId": str(m.get("tool_call_id") or ""),
                    "content": [{
                        "text": content if isinstance(content, str)
                        else json.dumps(content)}]}}]})
            continue
        if role == "assistant":
            blocks: list[dict[str, Any]] = []
            if isinstance(content, str) and content.strip():
                blocks.append({"text": content})
            elif isinstance(content, list):
                blocks.extend(content)
            for tc in (m.get("tool_calls") or []):
                fn = (tc or {}).get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {"_raw": fn.get("arguments") or ""}
                blocks.append({"toolUse": {
                    "toolUseId": str(tc.get("id") or
                                     f"toolu_{uuid4().hex[:12]}"),
                    "name": fn.get("name") or "",
                    "input": args}})
            out.append({"role": "assistant",
                        "content": blocks or [{"text": ""}]})
            continue
        # user (content may be a string or OpenAI content parts)
        if isinstance(content, str):
            out.append({"role": "user", "content": [{"text": content}]})
        elif isinstance(content, list):
            blocks = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    blocks.append({"text": str(part.get("text") or "")})
            out.append({"role": "user", "content": blocks or [{"text": ""}]})
        else:
            out.append({"role": "user",
                        "content": [{"text": content if isinstance(
                            content, str) else json.dumps(content)}]})
    return "\n\n".join(system_parts), out


def _oai_tools_to_bedrock(tools: list[dict[str, Any]]
                          ) -> list[dict[str, Any]]:
    out = []
    for t in tools or []:
        fn = (t or {}).get("function") or {}
        if not fn.get("name"):
            continue
        out.append({"toolSpec": {
            "name": fn["name"],
            "description": fn.get("description") or "",
            "inputSchema": {"json": fn.get("parameters")
                            or {"type": "object"}}}})
    return out


def _bedrock_to_oai(resp: dict[str, Any], model: str) -> dict[str, Any]:
    """Converse response → OpenAI chat-completion shape."""
    text_parts: list[str] = []
    tool_calls = []
    blocks = ((resp.get("output") or {}).get("message") or {}).get(
        "content") or []
    for b in blocks:
        if "text" in b:
            text_parts.append(str(b.get("text") or ""))
        elif "toolUse" in b:
            tu = b.get("toolUse") or {}
            tool_calls.append({
                "id": str(tu.get("toolUseId") or
                          f"call_{uuid4().hex[:12]}"),
                "type": "function",
                "function": {"name": tu.get("name") or "",
                             "arguments": json.dumps(
                                 tu.get("input") or {})}})
    finish = _BEDROCK_STOP_MAP.get(str(resp.get("stopReason") or ""),
                                   "stop")
    msg: dict[str, Any] = {
        "role": "assistant",
        "content": ("\n".join(text_parts) or None) if tool_calls
        else "\n".join(text_parts)}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    usage = resp.get("usage") or {}
    return {
        "id": str(resp.get("id") or f"chatcmpl-gw-{uuid4().hex[:12]}"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": msg,
                     "finish_reason": finish}],
        "usage": {
            "prompt_tokens": int(usage.get("inputTokens") or 0),
            "completion_tokens": int(usage.get("outputTokens") or 0),
            "total_tokens": int(usage.get("totalTokens") or 0)
            or (int(usage.get("inputTokens") or 0)
                + int(usage.get("outputTokens") or 0))},
    }


async def bedrock_converse(backend: dict[str, Any], model: str,
                           messages: list[dict[str, Any]],
                           temperature: float | None,
                           max_tokens: int | None,
                           tools: list[dict[str, Any]] | None,
                           timeout: float) -> dict[str, Any]:
    """One Bedrock Converse round-trip → OpenAI-shaped response.
    POST {base}/model/{modelId}/converse, SigV4-signed (service
    'bedrock') or x-api-key when the backend has a key configured."""
    from . import credchains
    region = str(backend.get("region")
                 or os.environ.get("AWS_REGION", "us-east-1"))
    base = str(backend.get("base_url")
               or f"https://bedrock-runtime.{region}.amazonaws.com"
               ).rstrip("/")
    system, msgs = _oai_messages_to_bedrock(messages)
    inference: dict[str, Any] = {
        "maxTokens": int(max_tokens or BEDROCK_DEFAULT_MAX_TOKENS)}
    if temperature is not None:
        inference["temperature"] = temperature
    payload: dict[str, Any] = {
        "messages": msgs,
        "inferenceConfig": inference}
    if system:
        payload["system"] = [{"text": system}]
    b_tools = _oai_tools_to_bedrock(tools or [])
    if b_tools:
        payload["toolConfig"] = {"tools": b_tools}
    headers = {"content-type": "application/json"}
    auth = None
    api_key = cloud.resolve_backend_key(backend)
    if api_key:
        headers["x-api-key"] = api_key
    else:
        try:
            auth = credchains.SigV4Auth(region, "bedrock")
            auth._creds()          # fail fast, classified below
        except credchains.CloudCredentialError as e:
            raise cloud.CloudBackendError(
                f"bedrock backend: {e}") from e
    resp = await _post_json(
        f"{base}/model/{model}/converse", headers, payload, timeout,
        "bedrock", auth=auth)
    return _bedrock_to_oai(resp, model)
