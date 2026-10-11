"""Session budget canary tests (v1.4) — the enterprise pre-gate on
session spend (the core charge hook feeds session:<id> scopes).

Real wiring: the enterprise pipeline (core governance + the session
gate, as enterprise_main wires it). Serving behavior asserted through
TestClient; the two-client case proves multi-replica shared gates.
"""
import json

import fakeredis
from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway.auth import (AuthPipeline, admin_gate_link, rate_limit_link,
                          rbac_link, resolve_link, set_auth_pipeline)
from gateway.storage import reset_storage, set_storage
from gateway_enterprise.redis_storage import RedisStorage
from gateway_enterprise.session_budget import session_budget_link

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

CHAT_BODY = {"model": "groq:cloud-fast",
             "messages": [{"role": "user", "content": "sb probe"}]}


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _wire_enterprise():
    """The enterprise_main.py pipeline, verbatim (v1.4)."""
    set_auth_pipeline(AuthPipeline([
        resolve_link, rbac_link, session_budget_link,
        admin_gate_link, rate_limit_link]))


def _sb_config(gguf, default_tokens, per_tenant=None, api_keys=None):
    cfgd = _secured_config(gguf, api_keys=api_keys)
    cfgd["session_budgets"] = {"enabled": True,
                               "default_tokens": default_tokens}
    if per_tenant:
        cfgd["session_budgets"]["per_tenant"] = per_tenant
    return cfgd


def test_session_over_budget_429(wired, gguf, monkeypatch):
    """default_tokens = one request's usage (8): the first request
    charges it, the second (same session) finds spend >= budget → 429
    session_budget_exceeded."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_sb_config(gguf, default_tokens=8,
                     api_keys=[{"id": "sb-key", "key": "vk-sb",
                                "owner": "team-x"}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            r1 = client.post("/v1/chat/completions",
                             headers=auth("vk-sb") | {"x-session-id": "run-1"},
                             json=CHAT_BODY)
            assert r1.status_code == 200, r1.text
            r2 = client.post("/v1/chat/completions",
                             headers=auth("vk-sb") | {"x-session-id": "run-1"},
                             json=dict(CHAT_BODY, messages=[
                                 {"role": "user", "content": "second"}]))
            assert r2.status_code == 429, r2.text
            assert r2.json()["error"]["code"] == "session_budget_exceeded"
            assert "run-1" in r2.json()["error"]["message"]
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_sessions_independent_and_no_session_inert(wired, gguf, monkeypatch):
    """Different session ids have independent budgets; requests
    WITHOUT a session id are never gated by the session budget."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_sb_config(gguf, default_tokens=8,
                     api_keys=[{"id": "sb-key", "key": "vk-sb"}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            # session A: first request admitted (0 < 8), charges 8;
            # second finds 8 >= 8 → 429. Session B is independent.
            assert client.post("/v1/chat/completions",
                               headers=auth("vk-sb") | {"x-session-id": "sA"},
                               json=CHAT_BODY).status_code == 200
            assert client.post("/v1/chat/completions",
                               headers=auth("vk-sb") | {"x-session-id": "sB"},
                               json=dict(CHAT_BODY, messages=[
                                   {"role": "user", "content": "2"}])
                               ).status_code == 200
            assert client.post("/v1/chat/completions",
                               headers=auth("vk-sb") | {"x-session-id": "sA"},
                               json=dict(CHAT_BODY, messages=[
                                   {"role": "user", "content": "3"}])
                               ).status_code == 429
            # NO session id → the gate is inert (key budget is 0 = off)
            for i in range(3):
                assert client.post(
                    "/v1/chat/completions", headers=auth("vk-sb"),
                    json=dict(CHAT_BODY, messages=[
                        {"role": "user", "content": f"free {i}"}])
                ).status_code == 200
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_per_tenant_override(wired, gguf, monkeypatch):
    """per_tenant keyed by the consuming key's OWNER: team-acme gets a
    bigger budget (16 = two requests) than the default (8 = one)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_sb_config(gguf, default_tokens=8,
                     per_tenant={"team-acme": 16},
                     api_keys=[{"id": "acme-key", "key": "vk-acme",
                                "owner": "team-acme"},
                               {"id": "other-key", "key": "vk-other",
                                "owner": "team-other"}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            # acme: override 16 → two requests admitted (0<16, 8<16),
            # the third finds 16 >= 16 → 429
            for i in range(2):
                assert client.post(
                    "/v1/chat/completions",
                    headers=auth("vk-acme") | {"x-session-id": "acme-run"},
                    json=dict(CHAT_BODY, messages=[
                        {"role": "user", "content": f"acme {i}"}])
                ).status_code == 200
            assert client.post(
                "/v1/chat/completions",
                headers=auth("vk-acme") | {"x-session-id": "acme-run"},
                json=dict(CHAT_BODY, messages=[
                    {"role": "user", "content": "acme 3"}])
            ).status_code == 429
            # other team: default 8 → first admitted, second 429s
            assert client.post(
                "/v1/chat/completions",
                headers=auth("vk-other") | {"x-session-id": "other-run"},
                json=CHAT_BODY).status_code == 200
            assert client.post(
                "/v1/chat/completions",
                headers=auth("vk-other") | {"x-session-id": "other-run"},
                json=dict(CHAT_BODY, messages=[
                    {"role": "user", "content": "other 2"}])
            ).status_code == 429
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_disabled_or_absent_section_inert(wired, gguf, monkeypatch):
    """No session_budgets tokens (or section absent) → the gate never
    fires even with a session id."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_secured_config(gguf,
                          api_keys=[{"id": "sb-key", "key": "vk-sb"}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as client:
            for i in range(3):
                assert client.post(
                    "/v1/chat/completions",
                    headers=auth("vk-sb") | {"x-session-id": "s1"},
                    json=dict(CHAT_BODY, messages=[
                        {"role": "user", "content": f"n {i}"}])
                ).status_code == 200
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_shared_session_budget_two_clients(wired, gguf, monkeypatch):
    """The multi-replica story: session spend rides the SHARED storage
    backend — client 1's charges count against client 2's gate (two
    'replicas' enforcing one budget). Budget 16 = two requests."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_sb_config(gguf, default_tokens=16,
                     api_keys=[{"id": "sb-key", "key": "vk-sb"}]))
    _patch_cloud(monkeypatch, _FakeCloud())
    set_storage(RedisStorage(client=fakeredis.FakeRedis()))
    _wire_enterprise()
    try:
        # client 1 ("replica A"): two requests admitted (0<16, 8<16)
        # → the SHARED session is now at 16
        with TestClient(gw_app.app) as c1:
            for i in range(2):
                r = c1.post("/v1/chat/completions",
                            headers=auth("vk-sb") | {"x-session-id": "shared"},
                            json=dict(CHAT_BODY, messages=[
                                {"role": "user", "content": f"rA {i}"}]))
                assert r.status_code == 200, r.text
        # client 2 ("replica B"): the SHARED session is at budget → 429
        with TestClient(gw_app.app) as c2:
            r2 = c2.post("/v1/chat/completions",
                         headers=auth("vk-sb") | {"x-session-id": "shared"},
                         json=CHAT_BODY)
            assert r2.status_code == 429, r2.text
            assert r2.json()["error"]["code"] == "session_budget_exceeded"
    finally:
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_session_budget_settings_parsing(wired, gguf):
    """Raw-section parse: defaults, junk dropped, per_tenant validated."""
    cfgfile, cfg, _ = wired(_sb_config(gguf, default_tokens=123,
                                       per_tenant={"t1": 456, "bad": "junk",
                                                   "": 9}))
    st = __import__("gateway_enterprise.session_budget",
                    fromlist=["get_session_budget_state"]
                    ).get_session_budget_state()
    s = st.settings(cfg)
    assert s["default_tokens"] == 123
    assert s["per_tenant"] == {"t1": 456}
