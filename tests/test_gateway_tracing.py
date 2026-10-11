"""Tracing canary tests (v2.0 Gatehouse) — prove OTLP tracing is REAL,
not decorative:

    W3C traceparent parsing/continuation (inbound header → same trace
    id exported + parent linkage + response echo),
    the OTLP/HTTP-JSON wire shape (int64-as-string, typed attributes),
    sampling (deterministic, ratio 0 kills the whole path),
    buffer cap + drop accounting, export-failure tolerance,
    app-level: SERVER + INTERNAL llm spans per request, request-log
    trace correlation (log line ↔ exported span share the trace id),
    and an in-test HTTP receiver proving the REAL httpx export path.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway import tracing

from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

TID = "0af7651916cd43dd8448eb211c80319d"     # 32 hex
SID = "b7ad6b7169203331"                     # 16 hex


@pytest.fixture(autouse=True)
def _reset_tracing():
    tracing.reset_tracing()
    yield
    tracing.reset_tracing()


# ── W3C trace context ──────────────────────────────────────────────────────

def test_parse_traceparent_valid():
    assert tracing.parse_traceparent(f"00-{TID}-{SID}-01") == (TID, SID)


def test_parse_traceparent_rejects_garbage():
    for bad in (None, "", "00-x-y-01", f"00-{TID}-{SID}",
                f"00-{'0'*32}-{SID}-01",                    # all-zero trace
                f"00-{TID}-{'0'*16}-01",                    # all-zero span
                f"00-{TID[:-1]}z{'0'*31}-{SID}-01", "junk"):
        assert tracing.parse_traceparent(bad) is None, bad


def test_format_traceparent_roundtrip():
    tp = tracing.format_traceparent(TID, SID)
    assert tp == f"00-{TID}-{SID}-01"
    assert tracing.parse_traceparent(tp) == (TID, SID)


def test_sampling_deterministic():
    assert tracing._sampled(TID, 0.0) is False
    assert tracing._sampled(TID, 1.0) is True
    # same trace id → same decision at any mid ratio
    a = tracing._sampled(TID, 0.5)
    assert a == tracing._sampled(TID, 0.5)


# ── OTLP payload shape ─────────────────────────────────────────────────────

def _one_span_payload():
    tracing.set_tracing({"enabled": True, "endpoint": "http://127.0.0.1:9",
                         "service_name": "t"}, exporter=lambda p: True)
    span = tracing.request_span("POST", "/v1/chat/completions",
                                f"00-{TID}-{SID}-01")
    span.set("http.status_code", 200)
    span.set("n", 7)
    span.set("ok", True)
    tracing.finish_request_span(span, 200)
    payload = tracing._otlp_payload([span])
    tracing.flush()
    return payload


def test_otlp_payload_shape():
    p = _one_span_payload()
    rs = p["resourceSpans"][0]
    res = {a["key"]: a["value"] for a in rs["resource"]["attributes"]}
    assert res["service.name"]["stringValue"] == "t"
    sp = rs["scopeSpans"][0]["spans"][0]
    assert sp["traceId"] == TID
    assert sp["parentSpanId"] == SID            # continued inbound trace
    assert sp["kind"] == 2                      # SERVER
    assert isinstance(sp["startTimeUnixNano"], str)   # int64 as string
    attrs = {a["key"]: a["value"] for a in sp["attributes"]}
    assert attrs["http.status_code"]["intValue"] == "200"
    assert attrs["n"]["intValue"] == "7"
    assert attrs["ok"]["boolValue"] is True


def test_otlp_payload_root_span_has_no_parent():
    tracing.set_tracing({"enabled": True, "endpoint": "http://127.0.0.1:9"},
                        exporter=lambda p: True)
    span = tracing.request_span("GET", "/v1/models", None)
    tracing.finish_request_span(span, 200)
    sp = tracing._otlp_payload([span])["resourceSpans"][0][
        "scopeSpans"][0]["spans"][0]
    assert "parentSpanId" not in sp
    tracing.flush()


# ── buffer + failure tolerance ─────────────────────────────────────────────

def test_buffer_cap_drops_oldest(monkeypatch):
    # configure directly with a LONG flush interval AND suppress the
    # wake signal — a live flusher (possibly a zombie from an earlier
    # test at 0.5s) would otherwise drain the buffer mid-fill.
    tracing.configure({"enabled": True, "endpoint": "http://127.0.0.1:9",
                       "batch_flush_s": 9999.0})
    tracing._EXPORTER = lambda p: True
    monkeypatch.setattr(tracing._wake, "set", lambda: None)
    for _ in range(tracing.BUFFER_CAP + 10):
        tracing.enqueue(tracing.Span(TID, tracing._new_id(8), None,
                                     "fill", 1))
    assert tracing.status()["spans_dropped_buffer"] == 10
    tracing.flush()
    assert tracing.status()["buffered"] == 0


def test_export_failure_never_raises():
    tracing.set_tracing({"enabled": True, "endpoint": "http://127.0.0.1:9"},
                        exporter=lambda p: False)
    span = tracing.request_span("GET", "/x", None)
    tracing.finish_request_span(span, 200)
    tracing.flush()                                  # must not raise
    assert tracing.status()["spans_dropped_export"] == 1

    def _boom(_p):
        raise RuntimeError("collector down")
    tracing._EXPORTER = _boom
    span = tracing.request_span("GET", "/x", None)
    tracing.finish_request_span(span, 200)
    tracing.flush()                                  # must not raise
    assert tracing.status()["export_errors"] == 1


def test_configure_without_endpoint_stays_off():
    tracing.configure({"enabled": True, "endpoint": ""})
    assert tracing.status()["enabled"] is False


# ── app-level: spans, correlation, sampling ────────────────────────────────

def _enable(exporter, **over):
    cfg = {"enabled": True, "endpoint": "http://127.0.0.1:9"}
    cfg.update(over)
    tracing.set_tracing(cfg, exporter=exporter)


def test_chat_request_produces_server_and_llm_spans(wired, gguf,  # noqa: F811
                                                    monkeypatch):
    import os
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    payloads = []
    with TestClient(gw_app.app) as client:
        # AFTER startup: the boot event installs state from cfg.otel
        # (absent → disabled) — the seam overrides it, as designed.
        _enable(payloads.append)
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "trace me"}]})
        assert r.status_code == 200, r.text
        assert "traceparent" in r.headers
    tracing.flush()
    assert payloads, "no OTLP payload exported"
    spans = [s for p in payloads
             for s in p["resourceSpans"][0]["scopeSpans"][0]["spans"]]
    server = next(s for s in spans if s["kind"] == 2)
    llm = next(s for s in spans if s["kind"] == 1)
    assert server["name"] == "POST /v1/chat/completions"
    assert llm["name"].startswith("llm ")
    assert llm["traceId"] == server["traceId"]
    assert llm["parentSpanId"] == server["spanId"]
    sattrs = {a["key"]: a["value"] for a in server["attributes"]}
    assert sattrs["gatelodge.request_id"]["stringValue"] == r.headers[
        "x-request-id"]
    lattrs = {a["key"]: a["value"] for a in llm["attributes"]}
    assert int(lattrs["llm.total_tokens"]["intValue"]) > 0


def test_inbound_traceparent_continues_trace(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    payloads = []
    with TestClient(gw_app.app) as client:
        _enable(payloads.append)
        r = client.post("/v1/chat/completions",
                        headers={"traceparent": f"00-{TID}-{SID}-01"},
                        json={"model": "groq:cloud-fast",
                              "messages": [{"role": "user",
                                            "content": "continue me"}]})
        assert r.status_code == 200
        # response echoes the ACTIVE span under the SAME trace id
        echoed = tracing.parse_traceparent(r.headers["traceparent"])
        assert echoed[0] == TID
    tracing.flush()
    spans = [s for p in payloads
             for s in p["resourceSpans"][0]["scopeSpans"][0]["spans"]]
    server = next(s for s in spans if s["kind"] == 2)
    assert server["traceId"] == TID
    assert server["parentSpanId"] == SID


def test_request_log_carries_trace_id(wired, gguf, monkeypatch, tmp_path):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    payloads = []
    with TestClient(gw_app.app) as client:
        _enable(payloads.append)
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "log my trace"}]})
        assert r.status_code == 200
    tracing.flush()
    spans = [s for p in payloads
             for s in p["resourceSpans"][0]["scopeSpans"][0]["spans"]]
    trace_ids = {s["traceId"] for s in spans}
    lines = []
    for p in tmp_path.glob("gateway-log*.jsonl"):
        lines += [json.loads(l) for l in p.read_text().splitlines() if l]
    logged = [l["trace"] for l in lines if "trace" in l]
    assert logged and set(logged) <= trace_ids, (
        f"log traces {logged} not in exported {trace_ids}")


def test_sampling_zero_kills_whole_path(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    payloads = []
    _enable(payloads.append, sample_ratio=0.0)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "skip me"}]})
        assert r.status_code == 200
        assert "traceparent" not in r.headers
    tracing.flush()
    assert payloads == []
    assert tracing.status()["spans_created"] == 0


def test_admin_tracing_and_healthz_surfaces(wired, gguf):  # noqa: F811
    wired(_secured_config(gguf))
    payloads = []
    headers = {"Authorization": "Bearer admin-secret"}
    with TestClient(gw_app.app) as client:
        _enable(payloads.append)
        a = client.get("/admin/tracing", headers=headers)
        assert a.status_code == 200
        assert a.json()["enabled"] is True
        assert a.json()["endpoint"] == "http://127.0.0.1:9"
        h = client.get("/healthz", headers=headers).json()
        assert h["tracing"]["enabled"] is True
        assert "endpoint" not in h["tracing"]   # never on the public surface


# ── the REAL httpx export path (in-test OTLP receiver) ─────────────────────

class _Receiver(BaseHTTPRequestHandler):
    posts: list[dict] = []

    def do_POST(self):                          # noqa: N802 (http.server)
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        _Receiver.posts.append({"path": self.path,
                                "ctype": self.headers.get("Content-Type"),
                                "body": json.loads(body)})
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):               # silence the test output
        pass


def test_real_http_export_to_receiver(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    srv = HTTPServer(("127.0.0.1", 0), _Receiver)
    port = srv.server_port
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        with TestClient(gw_app.app) as client:
            # no test exporter — the real httpx POST path (installed
            # after startup so the boot event can't clobber it)
            tracing.set_tracing({"enabled": True,
                                 "endpoint": f"http://127.0.0.1:{port}/v1/traces",
                                 "batch_flush_s": 0.05})
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "real wire"}]})
            assert r.status_code == 200
        tracing.flush()
        assert _Receiver.posts, "receiver never got the export POST"
        post = _Receiver.posts[-1]
        assert post["path"] == "/v1/traces"
        assert post["ctype"] == "application/json"
        spans = post["body"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
        assert any(s["kind"] == 2 for s in spans)
        assert tracing.status()["spans_exported"] > 0
    finally:
        srv.shutdown()
        _Receiver.posts.clear()
