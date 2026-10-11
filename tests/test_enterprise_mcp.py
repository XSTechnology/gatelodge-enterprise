"""MCP gateway canary tests (v2.0 Gatehouse) — the governed tool-call
surface, proven live:

    auth (MCP keys are sha256-at-rest, NOT gateway credentials),
    initialize/ping handshake, namespaced tools/list with per-key
    allowlist filtering, tools/call round-trip (name translation +
    session header), DENIED calls, upstream failures,
    hash-chained audit records for every outcome, OTLP span parented
    to the /mcp request span, the license gate, and SCIM+MCP coexisting
    on the additive router seam.
"""
import hashlib
import json
import sys
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

import enterprise_main
import gateway.app as gw_app
from gateway import tracing
from gateway_enterprise import mcp as mcp_mod
from gateway_enterprise import mcp

from test_gateway_stage3 import gguf, wired, _config  # noqa: F401

KEY = "mcp-client-1"
KH = hashlib.sha256(KEY.encode()).hexdigest()          # at-rest form
SERVERS = {"stub": {"url": "http://stub.internal/mcp"}}


@pytest.fixture(autouse=True)
def _reset():
    mcp.reset_mcp_state()
    tracing.reset_tracing()
    yield
    mcp.reset_mcp_state()
    tracing.reset_tracing()
    gw_app.set_extra_router(None, prefix="/cleanup")


def _mcp_config(gguf, keys=(KH,), allowlists=None):
    cfg = _config(gguf)
    cfg["mcp"] = {"enabled": True, "keys": list(keys),
                  "servers": dict(SERVERS)}
    if allowlists is not None:
        cfg["mcp"]["allowlists"] = allowlists
    return cfg


def _stub_handler(seen=None, sse=False):
    """A tiny upstream MCP server (streamable HTTP): echo + add tools,
    session id sess-42, 'boom' explodes."""
    if seen is None:
        seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        msg = json.loads(request.content.decode("utf-8"))
        seen.append({"method": msg.get("method"),
                     "session": request.headers.get("mcp-session-id"),
                     "params": msg.get("params")})
        mid = msg.get("id")
        method = msg.get("method")
        if method == "initialize":
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": mid,
                "result": {"protocolVersion": "2025-03-26",
                           "capabilities": {"tools": {}},
                           "serverInfo": {"name": "stub", "version": "1"}}},
                headers={"mcp-session-id": "sess-42"})
        if method == "tools/list":
            body = {"jsonrpc": "2.0", "id": mid, "result": {"tools": [
                {"name": "echo", "description": "echo text",
                 "inputSchema": {"type": "object"}},
                {"name": "add", "inputSchema": {"type": "object"}}]}}
            if sse:
                return httpx.Response(200, headers={
                    "content-type": "text/event-stream"},
                    text="data: " + json.dumps(body) + "\n\n")
            return httpx.Response(200, json=body)
        if method == "tools/call":
            p = msg.get("params") or {}
            if p.get("name") == "boom":
                return httpx.Response(500, json={"error": "kaboom"})
            text = str((p.get("arguments") or {}).get("text", ""))
            return httpx.Response(200, json={
                "jsonrpc": "2.0", "id": mid,
                "result": {"content": [{"type": "text",
                                        "text": text or "ok"}],
                           "isError": False}})
        return httpx.Response(200, json={})    # notifications / fallback
    return handler


def _mount(wired, gguf, monkeypatch, handler=None, seen=None, **cfgkw):
    wired(_mcp_config(gguf, **cfgkw))
    monkeypatch.setattr(
        mcp, "_new_async_client",
        lambda url, headers: httpx.AsyncClient(
            transport=httpx.MockTransport(handler or _stub_handler(seen)),
            headers=headers, timeout=5.0))
    gw_app.add_extra_router(mcp.build_router(), prefix="/mcp")


def _rpc(client, method, params=None, msg_id=1, bearer=KEY):
    return client.post("/mcp", headers={
        "Authorization": f"Bearer {bearer}"},
        json={"jsonrpc": "2.0", "id": msg_id, "method": method,
              **({"params": params} if params is not None else {})})


# ── auth + handshake ───────────────────────────────────────────────────────

def test_auth_required(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        r = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1,
                                      "method": "ping"})
        assert r.status_code == 401
        assert r.json()["error"]["code"] == -32001
        r2 = _rpc(client, "ping", bearer="wrong-key")
        assert r2.status_code == 401
        r3 = _rpc(client, "ping")
        assert r3.status_code == 200 and r3.json()["result"] == {}


def test_initialize_handshake(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "t", "version": "0"}})
        assert r.status_code == 200
        res = r.json()["result"]
        assert res["protocolVersion"] == "2025-03-26"
        assert "tools" in res["capabilities"]
        assert res["serverInfo"]["name"] == "gatelodge"
        # unknown client version → we answer with OUR latest
        r2 = _rpc(client, "initialize", {
            "protocolVersion": "1999-01-01"}, msg_id=2)
        assert r2.json()["result"]["protocolVersion"] == "2025-03-26"
        # notifications get no body (202)
        r3 = client.post("/mcp", headers={
            "Authorization": f"Bearer {KEY}"},
            json={"jsonrpc": "2.0", "method": "notifications/initialized"})
        assert r3.status_code == 202


def test_unknown_method_and_batch(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "resources/list")
        assert r.json()["error"]["code"] == -32601
        batch = client.post("/mcp", headers={
            "Authorization": f"Bearer {KEY}"},
            json=[{"jsonrpc": "2.0", "id": 1, "method": "ping"}])
        assert batch.json()["error"]["code"] == -32600


# ── tools/list + allowlists ────────────────────────────────────────────────

def test_tools_list_namespaced(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "tools/list")
        names = [t["name"] for t in r.json()["result"]["tools"]]
        assert names == ["stub.echo", "stub.add"]


def test_allowlist_filtering(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch,
           allowlists={KH: {"stub": ["echo"]}})
    with TestClient(gw_app.app) as client:
        names = [t["name"] for t in
                 _rpc(client, "tools/list").json()["result"]["tools"]]
        assert names == ["stub.echo"]           # only the listed tool

    # entry WITHOUT the server → nothing from it
    _mount(wired, gguf, monkeypatch,
           allowlists={KH: {"other": ["*"]}})
    with TestClient(gw_app.app) as client:
        names = [t["name"] for t in
                 _rpc(client, "tools/list").json()["result"]["tools"]]
        assert names == []

    # "*" wildcard → everything
    _mount(wired, gguf, monkeypatch, allowlists={KH: {"stub": "*"}})
    with TestClient(gw_app.app) as client:
        names = [t["name"] for t in
                 _rpc(client, "tools/list").json()["result"]["tools"]]
        assert names == ["stub.echo", "stub.add"]


def test_sse_upstream_response(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch, handler=_stub_handler(sse=True))
    with TestClient(gw_app.app) as client:
        names = [t["name"] for t in
                 _rpc(client, "tools/list").json()["result"]["tools"]]
        assert names == ["stub.echo", "stub.add"]


# ── tools/call + governance ────────────────────────────────────────────────

def _audit_lines():
    p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
    if not p.exists():
        return []
    return [json.loads(l) for l in
            p.read_text(encoding="utf-8").strip().splitlines() if l.strip()]


def test_tools_call_roundtrip(wired, gguf, monkeypatch):  # noqa: F811
    seen = []
    _mount(wired, gguf, monkeypatch, seen=seen)
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "tools/call",
                 {"name": "stub.echo", "arguments": {"text": "governed"}})
        assert r.status_code == 200
        assert r.json()["result"]["content"][0]["text"] == "governed"
    # the upstream saw the BARE tool name, the args, and the session
    calls = [s for s in seen if s["method"] == "tools/call"]
    assert calls and calls[0]["params"]["name"] == "echo"
    assert calls[0]["params"]["arguments"] == {"text": "governed"}
    assert calls[0]["session"] == "sess-42"      # captured from initialize
    recs = [l for l in _audit_lines()
            if l.get("action") == "mcp-tool-call"]
    assert recs and recs[-1]["status"] == "ok"
    assert recs[-1]["server"] == "stub" and recs[-1]["tool"] == "echo"
    assert recs[-1]["hash"]                     # chained record


def test_denied_call_audited(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch,
           allowlists={KH: {"stub": ["echo"]}})
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "tools/call",
                 {"name": "stub.add", "arguments": {"a": 1, "b": 2}})
        assert r.json()["error"]["code"] == -32002
    recs = [l for l in _audit_lines()
            if l.get("action") == "mcp-tool-call"]
    assert recs[-1]["status"] == "denied"
    assert recs[-1]["err"] == "not in key allowlist"


def test_upstream_failure_audited(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "tools/call",
                 {"name": "stub.boom", "arguments": {}})
        assert r.json()["error"]["code"] == -32003
    recs = [l for l in _audit_lines()
            if l.get("action") == "mcp-tool-call"]
    assert recs[-1]["status"] == "error"


def test_unknown_tool(wired, gguf, monkeypatch):  # noqa: F811
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        r = _rpc(client, "tools/call", {"name": "nosuch.tool"})
        assert r.json()["error"]["code"] == -32602


def test_tool_call_span_parented(wired, gguf, monkeypatch):  # noqa: F811
    payloads = []
    _mount(wired, gguf, monkeypatch)
    with TestClient(gw_app.app) as client:
        # AFTER startup (the boot event installs cfg.otel → disabled)
        tracing.set_tracing({"enabled": True,
                             "endpoint": "http://127.0.0.1:9"},
                            exporter=payloads.append)
        _rpc(client, "tools/call",
             {"name": "stub.echo", "arguments": {"text": "span"}})
    tracing.flush()
    spans = [s for p in payloads
             for s in p["resourceSpans"][0]["scopeSpans"][0]["spans"]]
    server = next(s for s in spans if s["kind"] == 2
                  and s["name"].startswith("POST /mcp"))
    tool = next(s for s in spans if s["name"] == "mcp stub.echo")
    assert tool["traceId"] == server["traceId"]
    assert tool["parentSpanId"] == server["spanId"]
    tattrs = {a["key"]: next(iter(a["value"].values()))
              for a in tool["attributes"]}
    assert tattrs["mcp.server"] == "stub"
    assert tattrs["mcp.status"] == "ok"


# ── stdio transports (REAL child processes) ────────────────────────────────

STUB_STDIO = r'''
import json, os, sys

def send(o):
    sys.stdout.write(json.dumps(o) + "\n")
    sys.stdout.flush()

TOOLS = [
    {"name": "echo", "description": "Echo text back",
     "inputSchema": {"type": "object"}},
    {"name": "env", "description": "Read an env var",
     "inputSchema": {"type": "object"}},
]

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    m = json.loads(line)
    mid, method = m.get("id"), m.get("method")
    p = m.get("params") or {}
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": "2025-03-26",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "stdio-stub", "version": "1"}}})
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}})
    elif method == "tools/call" and p.get("name") == "echo":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "content": [{"type": "text",
                         "text": str((p.get("arguments") or {})
                                     .get("text", ""))}],
            "isError": False}})
    elif method == "tools/call" and p.get("name") == "env":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "content": [{"type": "text",
                         "text": os.environ.get(
                             str((p.get("arguments") or {})
                                 .get("var", "")), "")}],
            "isError": False}})
    elif method == "tools/call" and p.get("name") == "boom":
        sys.exit(1)                     # die mid-call (restart test)
    elif mid is not None:
        send({"jsonrpc": "2.0", "id": mid, "result": {}})
    # notifications (no id): nothing to answer
'''


def _stdio_config(gguf, stub_path, env=None):
    cfg = _config(gguf)
    server: dict = {"command": [sys.executable, str(stub_path)]}
    if env:
        server["env"] = env
    cfg["mcp"] = {"enabled": True, "keys": [KH],
                  "servers": {"local": server}}
    return cfg


def _write_stub(tmp_path) -> Path:
    p = tmp_path / "mcp_stdio_stub.py"
    p.write_text(STUB_STDIO, encoding="utf-8")
    return p


def test_stdio_tools_list_and_call(wired, gguf, tmp_path):  # noqa: F811
    """A REAL child process: the gateway spawns the stub, handshakes,
    lists tools (namespaced) and round-trips a call."""
    wired(_stdio_config(gguf, _write_stub(tmp_path)))
    gw_app.add_extra_router(mcp.build_router(), prefix="/mcp")
    try:
        with TestClient(gw_app.app) as client:
            names = [t["name"] for t in
                     _rpc(client, "tools/list").json()["result"]["tools"]]
            assert names == ["local.echo", "local.env"]
            r = _rpc(client, "tools/call", {
                "name": "local.echo",
                "arguments": {"text": "from a child process"}})
            assert r.json()["result"]["content"][0]["text"] == \
                "from a child process"
    finally:
        gw_app.set_extra_router(None, prefix="/mcp")


def test_stdio_env_passes_through(wired, gguf, tmp_path):  # noqa: F811
    """Server env entries ride atop the gateway environment."""
    wired(_stdio_config(gguf, _write_stub(tmp_path),
                        env={"GATELODGE_TEST_ENV": "injected-42"}))
    gw_app.add_extra_router(mcp.build_router(), prefix="/mcp")
    try:
        with TestClient(gw_app.app) as client:
            r = _rpc(client, "tools/call", {
                "name": "local.env",
                "arguments": {"var": "GATELODGE_TEST_ENV"}})
            assert r.json()["result"]["content"][0]["text"] == "injected-42"
    finally:
        gw_app.set_extra_router(None, prefix="/mcp")


def test_stdio_child_death_respawns(wired, gguf, tmp_path):  # noqa: F811
    """A child that DIES mid-call (boom → exit 1): the call fails with
    -32003, the NEXT call transparently respawns the child and works."""
    wired(_stdio_config(gguf, _write_stub(tmp_path)))
    gw_app.add_extra_router(mcp.build_router(), prefix="/mcp")
    try:
        with TestClient(gw_app.app) as client:
            boom = _rpc(client, "tools/call", {"name": "local.boom"})
            assert boom.json()["error"]["code"] == -32003
            # the respawn happens on the next call
            ok = _rpc(client, "tools/call", {
                "name": "local.echo", "arguments": {"text": "revived"}})
            assert ok.status_code == 200
            assert ok.json()["result"]["content"][0]["text"] == "revived"
    finally:
        gw_app.set_extra_router(None, prefix="/mcp")


def test_stdio_and_http_servers_coexist(wired, gguf, tmp_path, monkeypatch):  # noqa: F811
    """One registry, both transports: stdio (real child) + HTTP
    (mock transport) side by side, tools merged + namespaced."""
    seen = []
    monkeypatch.setattr(
        mcp, "_new_async_client",
        lambda url, headers: httpx.AsyncClient(
            transport=httpx.MockTransport(_stub_handler(seen)), headers=headers,
            timeout=5.0))
    cfg = _stdio_config(gguf, _write_stub(tmp_path))
    cfg["mcp"]["servers"]["remote"] = {"url": "http://stub.internal/mcp"}
    wired(cfg)
    gw_app.add_extra_router(mcp.build_router(), prefix="/mcp")
    try:
        with TestClient(gw_app.app) as client:
            names = sorted(t["name"] for t in
                           _rpc(client, "tools/list")
                           .json()["result"]["tools"])
            assert names == ["local.echo", "local.env",
                             "remote.add", "remote.echo"]
    finally:
        gw_app.set_extra_router(None, prefix="/mcp")


def test_stdio_governed_audit(wired, gguf, tmp_path):  # noqa: F811
    """stdio tool calls get the SAME governance: chained audit record."""
    wired(_stdio_config(gguf, _write_stub(tmp_path)))
    gw_app.add_extra_router(mcp.build_router(), prefix="/mcp")
    try:
        with TestClient(gw_app.app) as client:
            _rpc(client, "tools/call", {
                "name": "local.echo", "arguments": {"text": "audited"}})
        p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
        recs = [json.loads(l) for l in
                p.read_text(encoding="utf-8").strip().splitlines()
                if l.strip()]
        rec = [r for r in recs
               if r.get("action") == "mcp-tool-call"][-1]
        assert rec["server"] == "local" and rec["tool"] == "echo"
        assert rec["status"] == "ok" and rec["hash"]
    finally:
        gw_app.set_extra_router(None, prefix="/mcp")


# ── the license gate + coexistence ─────────────────────────────────────────

def test_wire_mcp_license_gate(wired, gguf, monkeypatch):  # noqa: F811
    import gateway.app as gwa
    wired(_mcp_config(gguf))
    mounted = []
    monkeypatch.setattr(gwa, "add_extra_router",
                        lambda r, prefix: mounted.append(prefix))
    enterprise_main._wire_mcp(enabled=False)
    assert mounted == []                      # claim absent → ignored
    enterprise_main._wire_mcp(enabled=True)
    assert mounted == ["/mcp"]
    # section absent → no mount even when licensed
    wired(_config(gguf))
    enterprise_main._wire_mcp(enabled=True)
    assert mounted == ["/mcp"]


def test_scim_and_mcp_coexist(wired, gguf, monkeypatch):  # noqa: F811
    cfg = _mcp_config(gguf)
    cfg["scim"] = {"enabled": True,
                   "token_hash": hashlib.sha256(b"scim-token").hexdigest()}
    wired(cfg)
    monkeypatch.setattr(
        mcp, "_new_async_client",
        lambda url, headers: httpx.AsyncClient(
            transport=httpx.MockTransport(_stub_handler()), headers=headers,
            timeout=5.0))
    enterprise_main._wire_scim(enabled=True)
    enterprise_main._wire_mcp(enabled=True)
    try:
        with TestClient(gw_app.app) as client:
            # both surfaces mounted at once — the v2.0 additive seam
            r = _rpc(client, "ping")
            assert r.status_code == 200
            s = client.get("/scim/v2/Users", headers={
                "Authorization": "Bearer scim-token"})
            assert s.status_code == 200
    finally:
        gw_app.set_extra_router(None, prefix="/mcp")
