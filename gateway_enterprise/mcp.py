"""Gatelodge MCP gateway — GOVERNED tool calls (v2.0 "The Gatehouse").

Enterprise-claimed feature (the ``mcp`` license key): the gateway sits
between MCP clients and upstream MCP servers so that tool calls get the
same governance as model calls — identity, per-key allowlists,
tamper-evident audit, metrics, and traces.

    client (Claude Desktop / Cursor / agent)          upstream MCP
             │  streamable HTTP, JSON-RPC 2.0          servers
             ▼                                              ▲
      POST /mcp  ── bearer key ──▶ allowlist gate ──▶ tools/call ─┘
                                    │
                     audit chain + Prometheus + OTLP span

Wire shape (MCP "streamable HTTP" transport, 2025-03-26; batching was
removed from the spec — single messages only):

    POST /mcp        initialize → capabilities + serverInfo
                     notifications/initialized → 202 (no body)
                     ping → {}
                     tools/list  → merged, namespaced "<server>.<tool>"
                     tools/call  → routed to the owning upstream, name
                                   translated back to the bare tool

The gateway is a STATELESS MCP server (no client session required) and
a session-holding MCP CLIENT per upstream (lazy initialize handshake,
``Mcp-Session-Id`` captured and reused, tools cached with a TTL).

Config (raw section, hot-read per request like scim — the core parses
nothing MCP-specific):

    "mcp": {
      "enabled": true,
      "keys": ["<sha256 of the MCP client bearer>"],
      "servers": {
        "fs":  {"url": "http://127.0.0.1:8801/mcp",
                "headers": {"authorization": "literal"},
                "headers_env": {"authorization": "ENV_VAR"}}
      },
      "allowlists": {"<key sha256>": {"fs": ["read_file", "list_dir"]}}
    }

Posture (matching RBAC doctrine): a key with an allowlist entry gets
EXACTLY what it lists; a key with NO entry gets everything (the
DEFAULT_ROLE posture — single-operator installs stay simple, and the
operator scopes keys by writing allowlists). Server names must be
dot-free (the namespace separator is the first dot).

Governance per tools/call (allowed AND denied): a hash-chained audit
record (append_audit_record — tamper-evident, same file as config
mutations), a Prometheus counter, and an OTLP span parented to the
/mcp request span.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from gateway import tracing
from gateway.app import append_audit_record, _metric_inc
from gateway.auth import _REQUEST_ID

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = "2025-03-26"
_KNOWN_VERSIONS = ("2025-03-26", "2024-11-05")
_TOOLS_TTL_S = 30.0          # upstream tools/list cache
_UPSTREAM_TIMEOUT_S = 60.0

# module-level upstream registry (rebuilt when a server's url changes)
_upstreams: dict[str, "_UpstreamMCP"] = {}
_upstream_urls: dict[str, str] = {}


def _sha256(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _section() -> dict[str, Any]:
    """Raw mcp section (per request — hot-reload, like scim)."""
    from gateway import config as gw_config
    raw = gw_config.get_gateway_config().store.load()
    mcp = raw.get("mcp") if isinstance(raw, dict) else None
    return mcp if isinstance(mcp, dict) else {}


def _bearer(request: Request) -> str:
    return (request.headers.get("authorization") or "").removeprefix(
        "Bearer").strip()


def _key_id(request: Request, section: dict[str, Any]) -> str | None:
    """Bearer → key id (first 12 hex of its sha256) or None. Keys are
    sha256 hashes at rest, exactly like gateway_keys."""
    token = _bearer(request)
    if not token:
        return None
    h = _sha256(token)
    keys = section.get("keys") or []
    if isinstance(keys, list) and h in keys:
        return h[:12]
    return None


def _allowed(section: dict[str, Any], key_hash: str,
             server: str, tool: str) -> bool:
    """Allowlist gate. Key with an entry → exactly what it lists
    ("*" wildcards); key with no entry → everything (DEFAULT_ROLE
    posture). Fail-closed on malformed entries."""
    allow = section.get("allowlists") or {}
    entry = allow.get(key_hash) if isinstance(allow, dict) else None
    if not isinstance(entry, dict):
        return True
    tools = entry.get(server)
    if tools is None:
        return False                       # entry exists, server omitted
    if tools == "*":
        return True
    if isinstance(tools, list):
        return "*" in tools or tool in tools
    return False


# ── upstream MCP client (streamable HTTP) ─────────────────────────────────

def _new_async_client(url: str, headers: dict[str, str]) -> httpx.AsyncClient:
    """Client factory — module function so tests can inject a mock
    transport (the SCIM/OTLP receiver doctrine). No base_url: every
    call POSTs the server's absolute URL (empty-path joining is a
    trap)."""
    return httpx.AsyncClient(headers=headers, timeout=_UPSTREAM_TIMEOUT_S)


def _parse_upstream_response(r: httpx.Response) -> dict[str, Any] | None:
    """JSON body directly, or the first JSON-RPC message from an SSE
    body (both are legal streamable-HTTP responses)."""
    ctype = (r.headers.get("content-type") or "").lower()
    if "text/event-stream" in ctype:
        for line in r.text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                try:
                    msg = json.loads(line[5:].strip())
                except ValueError:
                    continue
                if isinstance(msg, dict):
                    return msg
        return None
    try:
        return r.json()
    except ValueError:
        return None


class _UpstreamMCP:
    """One upstream MCP server: lazy initialize handshake, session id
    reuse, tools cache. NOT thread-safe by design — one asyncio task at
    a time (matches the gateway's request model)."""

    def __init__(self, name: str, url: str, headers: dict[str, str]):
        self.name = name
        self.url = url
        self.headers = headers
        self.client = _new_async_client(url, headers)
        self.session_id: str | None = None
        self.protocol_version: str | None = None
        self._tools: list[dict[str, Any]] | None = None
        self._tools_ts = 0.0
        self._last_session_id: str | None = None
        self._rpc_seq = iter(range(1, 1 << 30))

    async def _rpc(self, method: str, params: dict | None = None,
                   *, reinit: bool = True) -> dict[str, Any]:
        msg: dict[str, Any] = {"jsonrpc": "2.0", "id": next(self._rpc_seq),
                               "method": method}
        if params is not None:
            msg["params"] = params
        headers = {"Accept": "application/json, text/event-stream"}
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        if self.protocol_version:
            headers["MCP-Protocol-Version"] = self.protocol_version
        r = await self.client.post(self.url, json=msg, headers=headers)
        # streamable HTTP: the server may issue a session id on ANY
        # response; capture it (the initialize handshake is the usual
        # source) so subsequent calls carry it.
        sid = r.headers.get("mcp-session-id")
        if sid:
            self._last_session_id = sid
        # a dead session (server restarted) → one re-initialize, retry
        if r.status_code == 404 and self.session_id and reinit:
            self.session_id = None
            await self.ensure()
            return await self._rpc(method, params, reinit=False)
        r.raise_for_status()
        out = _parse_upstream_response(r)
        if not isinstance(out, dict):
            raise RuntimeError(f"upstream {self.name!r}: no JSON-RPC reply")
        if "error" in out:
            e = out["error"] or {}
            raise RuntimeError(f"upstream {self.name!r}: "
                               f"{e.get('message') or e}")
        return out.get("result") or {}

    async def ensure(self) -> None:
        """initialize handshake (idempotent) — captures the session id
        + protocol version for subsequent calls."""
        if self.session_id:
            return
        res = await self._rpc("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "gatelodge-mcp", "version": "1"}},
            reinit=False)
        self.protocol_version = res.get("protocolVersion") or PROTOCOL_VERSION
        self.session_id = self._last_session_id
        try:
            await self.client.post(self.url, json={
                "jsonrpc": "2.0", "method": "notifications/initialized"},
                headers={"Accept": "application/json"})
        except httpx.HTTPError:
            pass                       # notification — best effort

    async def tools(self, refresh: bool = False) -> list[dict[str, Any]]:
        if (self._tools is not None and not refresh
                and time.monotonic() - self._tools_ts < _TOOLS_TTL_S):
            return self._tools
        await self.ensure()
        res = await self._rpc("tools/list")
        tools = res.get("tools") or []
        self._tools = tools if isinstance(tools, list) else []
        self._tools_ts = time.monotonic()
        return self._tools

    async def call(self, tool: str, args: dict) -> dict[str, Any]:
        await self.ensure()
        return await self._rpc("tools/call", {"name": tool,
                                              "arguments": args or {}})

    async def aclose(self) -> None:
        await self.client.aclose()


async def _registry(section: dict[str, Any]) -> dict[str, _UpstreamMCP]:
    """Build/refresh the upstream registry from the config section
    (url change → fresh client, like the embedding-instance pattern;
    retired clients are closed)."""
    servers = section.get("servers") or {}
    if not isinstance(servers, dict):
        return {}
    retired: list[_UpstreamMCP] = []
    for name, spec in list(_upstreams.items()):
        if name not in servers or (servers.get(name) or {}).get("url") != \
                _upstream_urls.get(name):
            retired.append(_upstreams.pop(name))
            _upstream_urls.pop(name, None)
    for name, spec in servers.items():
        if not isinstance(spec, dict) or not spec.get("url") or name in _upstreams:
            continue
        headers = {str(k): str(v)
                   for k, v in (spec.get("headers") or {}).items()}
        for k, env in (spec.get("headers_env") or {}).items():
            val = os.environ.get(str(env))
            if val:
                headers[str(k)] = val
        _upstreams[name] = _UpstreamMCP(name, str(spec["url"]), headers)
        _upstream_urls[name] = str(spec["url"])
    for old in retired:
        try:
            await old.aclose()
        except Exception:                                      # noqa: BLE001
            pass
    return dict(_upstreams)


# ── governance hooks ───────────────────────────────────────────────────────

def _govern(key_id: str, server: str, tool: str, status: str, ms: int,
            err: str = "") -> None:
    """One tool call → chained audit record + Prometheus counter + OTLP
    span. Never raises; never blocks the response."""
    try:
        rec: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "action": "mcp-tool-call", "key": key_id,
            "server": server, "tool": tool, "status": status, "ms": ms}
        if err:
            rec["err"] = err[:200]
        append_audit_record(rec)
    except Exception:                                          # noqa: BLE001
        logger.warning("mcp audit record failed", exc_info=True)
    try:
        _metric_inc("gateway_mcp_tool_calls_total",
                    (server, tool, status))
    except Exception:                                          # noqa: BLE001
        pass
    attrs = {"mcp.server": server, "mcp.tool": tool,
             "mcp.status": status, "mcp.duration_ms": int(ms)}
    rid = _REQUEST_ID.get()
    if rid:
        attrs["gatelodge.request_id"] = rid
    tracing.add_span(f"mcp {server}.{tool}", kind=tracing.KIND_CLIENT,
                     attrs=attrs)


# ── the router (mounted at /mcp via add_extra_router) ─────────────────────

def _rpc_ok(msg_id: Any, result: dict) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": msg_id, "result": result})


def _rpc_err(msg_id: Any, code: int, message: str,
             http_status: int = 200) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": msg_id,
                         "error": {"code": code, "message": message}},
                        status_code=http_status)


async def _handle(request: Request) -> Response:
    section = _section()
    key_id = _key_id(request, section)
    if key_id is None:
        return _rpc_err(None, -32001, "unauthorized: MCP key required "
                        "(sha256-hashed under mcp.keys)", http_status=401)
    try:
        msg = await request.json()
    except Exception:                                          # noqa: BLE001
        return _rpc_err(None, -32700, "parse error: JSON body required")
    if not isinstance(msg, dict) or "method" not in msg:
        return _rpc_err(None, -32600, "invalid request: single JSON-RPC "
                        "message with a method (batching was removed "
                        "from MCP)")
    method = str(msg.get("method") or "")
    msg_id = msg.get("id")            # absent → notification
    params = msg.get("params") or {}

    # notifications carry no id and get no response body
    if msg_id is None:
        return Response(status_code=202)

    if method == "initialize":
        ver = params.get("protocolVersion")
        ver = ver if ver in _KNOWN_VERSIONS else PROTOCOL_VERSION
        from gateway.app import app as gw_app_obj
        return _rpc_ok(msg_id, {
            "protocolVersion": ver,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "gatelodge", "version":
                           gw_app_obj.version}})

    if method == "ping":
        return _rpc_ok(msg_id, {})

    if method == "tools/list":
        tools: list[dict[str, Any]] = []
        key_hash = _sha256(_bearer(request))
        reg = await _registry(section)
        for name, up in reg.items():
            try:
                for t in await up.tools():
                    tname = str(t.get("name") or "")
                    if not tname or not _allowed(section, key_hash,
                                                 name, tname):
                        continue
                    entry = {"name": f"{name}.{tname}"}
                    if t.get("description"):
                        entry["description"] = t["description"]
                    entry["inputSchema"] = t.get("inputSchema") or {
                        "type": "object"}
                    tools.append(entry)
            except Exception as e:                            # noqa: BLE001
                logger.warning("mcp tools/list from %r failed: %s",
                               name, e)
        return _rpc_ok(msg_id, {"tools": tools})

    if method == "tools/call":
        t0 = time.perf_counter()
        full = str(params.get("name") or "")
        server, _, tool = full.partition(".")
        key_hash = _sha256(_bearer(request))
        reg = await _registry(section)
        if not server or not tool or server not in reg:
            return _rpc_err(msg_id, -32602, f"unknown tool {full!r}")
        if not _allowed(section, key_hash, server, tool):
            ms = int((time.perf_counter() - t0) * 1000)
            _govern(key_id, server, tool, "denied", ms,
                    err="not in key allowlist")
            return _rpc_err(msg_id, -32002, f"tool {full!r} is not "
                            "allowed for this key")
        try:
            result = await reg[server].call(
                tool, dict(params.get("arguments") or {}))
            ms = int((time.perf_counter() - t0) * 1000)
            _govern(key_id, server, tool, "ok", ms)
            return _rpc_ok(msg_id, result)
        except Exception as e:                                # noqa: BLE001
            ms = int((time.perf_counter() - t0) * 1000)
            _govern(key_id, server, tool, "error", ms, err=str(e))
            return _rpc_err(msg_id, -32003, f"upstream {server!r} call "
                            f"failed: {e}")

    return _rpc_err(msg_id, -32601, f"method {method!r} not supported "
                    "(v1: initialize, ping, tools/list, tools/call)")


def build_router() -> APIRouter:
    """The MCP surface — mounted at /mcp via add_extra_router (v2.0
    additive seam; SCIM keeps its own mount). The prefix owns its
    auth: MCP bearers are NOT gateway credentials."""
    r = APIRouter()

    @r.post("")
    @r.post("/")
    async def _mcp(request: Request) -> Response:
        return await _handle(request)

    return r


def reset_mcp_state() -> None:
    """Drop the upstream registry (tests / teardown). Mock-transport
    clients in tests need no close; production churn closes via
    _registry's retired list."""
    _upstreams.clear()
    _upstream_urls.clear()
