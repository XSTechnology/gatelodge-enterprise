"""Session budget charge-hook tests (v1.4: x-session-id → session:<id>
spend scope).

When session_budgets.enabled and a request carries a valid
x-session-id, _charge_request charges the session scope ALONGSIDE the
consuming key (same ledger machinery — multi-replica for free via the
storage seam), and the request log line carries the session id. The
enterprise pre-gate (fork) reads the same scope.
"""
import json
import time

from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway.storage import reset_storage

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

CHAT_BODY = {"model": "groq:cloud-fast",
             "messages": [{"role": "user", "content": "session probe"}]}


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _session_config(gguf, enabled=True, **over):
    cfgd = _secured_config(gguf, api_keys=[dict(
        {"id": "sess-key", "key": "vk-sess"}, **over)])
    cfgd["session_budgets"] = {"enabled": enabled}
    return cfgd


def test_session_scope_charged_alongside_key(wired, gguf, monkeypatch):
    """Enabled + valid header → BOTH the key scope and the session
    scope are charged (session rides the same ledger machinery)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_session_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-sess") | {"x-session-id": "agentic-run-1"},
                            json=CHAT_BODY)
            assert r.status_code == 200, r.text
        key_spend = gw_app._scope_spend("key:sess-key")
        sess_spend = gw_app._scope_spend("session:agentic-run-1")
        assert key_spend["tokens"] == 8
        assert sess_spend["tokens"] == 8          # charged alongside
    finally:
        reset_storage()


def test_disabled_header_ignored(wired, gguf, monkeypatch):
    """Default (disabled): the header is ignored — no session scope
    exists (an enabled-only feature, zero overhead when off)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_session_config(gguf, enabled=False))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-sess") | {"x-session-id": "s1"},
                            json=CHAT_BODY)
            assert r.status_code == 200, r.text
        assert gw_app._scope_spend("session:s1")["tokens"] == 0
        assert gw_app._scope_spend("key:sess-key")["tokens"] == 8
    finally:
        reset_storage()


def test_absent_or_invalid_header_no_charge(wired, gguf, monkeypatch):
    """No header → no session scope; garbage header values are IGNORED
    (not truncated) so they cannot fragment the ledger with unbounded
    unique markers."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_session_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            assert client.post("/v1/chat/completions",
                               headers=auth("vk-sess"),
                               json=CHAT_BODY).status_code == 200
            bad = {"x-session-id": "garbage !! with spaces #"}
            assert client.post("/v1/chat/completions",
                               headers=auth("vk-sess") | bad,
                               json=dict(CHAT_BODY,
                                         messages=[{"role": "user",
                                                    "content": "2"}])
                               ).status_code == 200
        assert gw_app._scope_spend("session:garbage !! with spaces #"
                                   )["tokens"] == 0
    finally:
        reset_storage()


def test_sessions_are_independent(wired, gguf, monkeypatch):
    """Two different session ids charge two independent scopes — the
    per-session granularity the enterprise gate enforces."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_session_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            for i, sid in enumerate(("s-a", "s-b", "s-a")):
                r = client.post("/v1/chat/completions",
                                headers=auth("vk-sess") | {"x-session-id": sid},
                                json=dict(CHAT_BODY,
                                          messages=[{"role": "user",
                                                     "content": f"probe {i}"}]))
                assert r.status_code == 200, r.text
        assert gw_app._scope_spend("session:s-a")["tokens"] == 16  # 2 × 8
        assert gw_app._scope_spend("session:s-b")["tokens"] == 8
    finally:
        reset_storage()


def test_log_line_carries_session(wired, gguf, monkeypatch):
    """The request log line gains the session id when the feature is
    on (and session spend re-seeds from it after a ledger reset)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_session_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            assert client.post("/v1/chat/completions",
                               headers=auth("vk-sess") | {"x-session-id": "log-probe"},
                               json=CHAT_BODY).status_code == 200
        lines = [json.loads(l) for l in
                 gw_app._month_log_path(gw_app._month_key())
                 .read_text(encoding="utf-8").strip().splitlines()]
        ours = [l for l in lines if l.get("session") == "log-probe"]
        assert ours and ours[0]["key"] == "sess-key"
        # session spend re-seeds from the log (single-process backend)
        gw_app.reset_token_ledger()
        assert gw_app._scope_spend("session:log-probe")["tokens"] == 8
    finally:
        reset_storage()
