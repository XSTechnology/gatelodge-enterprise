"""TPM (tokens/minute) rate-limit tests — the per-key token window.

Same canary discipline as the seams tests: real wiring (the default
pipeline carries rate_limit_link), serving behavior asserted via
TestClient. The cloud backend is faked; usage tokens are what
_FakeCloud reports (total_tokens=8 per call).
"""
import time

from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway import auth as gw_auth
from gateway.storage import get_storage, reset_storage

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

CHAT_BODY = {"model": "groq:cloud-fast",
             "messages": [{"role": "user", "content": "tpm probe"}]}


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _tpm_config(gguf, tpm, **key_over):
    cfgd = _secured_config(gguf, api_keys=[dict(
        {"id": "tpm-key", "key": "vk-tpm", "rate_limit_tpm": tpm},
        **key_over)])
    return cfgd


def _window(key_id="tpm-key"):
    return get_storage().get("rate_tpm_windows", key_id) or []


def test_tpm_gate_denies_when_window_full(wired, gguf, monkeypatch):
    """A window already charged past the limit 429s BEFORE reaching the
    backend — with a Retry-After and the token-limit message."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_tpm_config(gguf, tpm=100))
    fake = _FakeCloud()
    _patch_cloud(monkeypatch, fake)
    try:
        # seed the window: 90 tokens charged a second ago
        get_storage().set("rate_tpm_windows", "tpm-key",
                          [[time.time() - 1.0, 90.0]])
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-tpm"), json=CHAT_BODY)
            assert r.status_code == 429, r.text
            assert "token rate limit" in r.json()["error"]["message"]
            assert "Retry-After" in r.headers
        assert fake.calls == []   # never reached the backend
    finally:
        reset_storage()


def test_tpm_estimate_denies_oversized_request(wired, gguf, monkeypatch):
    """The pre-gate prompt ESTIMATE (body bytes // 4) alone can exceed
    the limit — a request too large for the key's budget is rejected
    before consuming anything, even on an empty window."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_tpm_config(gguf, tpm=100))
    fake = _FakeCloud()
    _patch_cloud(monkeypatch, fake)
    big_body = {"model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "x" * 4000}]}
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-tpm"), json=big_body)
            assert r.status_code == 429, r.text
        assert fake.calls == []
    finally:
        reset_storage()


def test_tpm_charge_after_success(wired, gguf, monkeypatch):
    """A successful request charges its ACTUAL usage (from the
    provider's usage block) into the window — the estimate is never
    recorded."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_tpm_config(gguf, tpm=100000))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-tpm"), json=CHAT_BODY)
            assert r.status_code == 200, r.text
        win = _window()
        assert len(win) == 1
        assert win[0][1] == 8.0        # _FakeCloud's total_tokens
    finally:
        reset_storage()


def test_tpm_window_expires(wired, gguf, monkeypatch):
    """Charges older than 60s drop out of the window on the next read."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_tpm_config(gguf, tpm=100))
    _patch_cloud(monkeypatch, _FakeCloud())
    get_storage().set("rate_tpm_windows", "tpm-key",
                      [[time.time() - 61.0, 500.0]])
    try:
        with TestClient(gw_app.app) as client:
            # the stale charge is gone → gate passes (estimate small)
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-tpm"), json=CHAT_BODY)
            assert r.status_code == 200, r.text
    finally:
        reset_storage()


def test_rpm_and_tpm_both_enforced(wired, gguf, monkeypatch):
    """A key can carry both limits; each is checked (RPM first)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_tpm_config(gguf, tpm=100000, rate_limit_rpm=1))
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            r1 = client.post("/v1/chat/completions",
                             headers=auth("vk-tpm"), json=CHAT_BODY)
            assert r1.status_code == 200
            r2 = client.post("/v1/chat/completions",
                             headers=auth("vk-tpm"), json=CHAT_BODY)
            assert r2.status_code == 429
            assert "requests/minute" in r2.json()["error"]["message"]
    finally:
        reset_storage()


def test_tpm_admin_key_and_unlimited_keys_exempt(wired, gguf, monkeypatch):
    """Admin keys are unlimited (core rule); a key without a TPM limit
    never touches the token window."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _secured_config(gguf, api_keys=[{"id": "free-key",
                                             "key": "vk-free"}])
    wired(cfgd)
    _patch_cloud(monkeypatch, _FakeCloud())
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions",
                            headers=auth("vk-free"), json=CHAT_BODY)
            assert r.status_code == 200, r.text
            r2 = client.post("/v1/chat/completions",
                             headers=auth("admin-secret"), json=CHAT_BODY)
            assert r2.status_code == 200, r2.text
        assert _window("free-key") == []
        assert _window("admin") == []
    finally:
        reset_storage()


def test_tpm_admin_roundtrip(wired, gguf):
    """rate_limit_tpm survives create → list (admin surface parity)."""
    wired(_secured_config(gguf))   # admin key, no api_keys
    with TestClient(gw_app.app) as client:
        created = client.post("/admin/keys", headers=auth("admin-secret"),
                              json={"id": "tpm-admin-made",
                                    "rate_limit_tpm": 5000})
        assert created.status_code == 200, created.text
        listed = client.get("/admin/keys", headers=auth("admin-secret"))
        rows = {k["id"]: k for k in listed.json()["api_keys"]}
        assert rows["tpm-admin-made"]["rate_limit_tpm"] == 5000
        # cleanup (the mutation is real — goes through the store)
        client.post("/admin/keys", headers=auth("admin-secret"),
                    json={"id": "tpm-admin-made", "delete": True})


def test_tpm_window_units():
    """Unit: the gate's arithmetic (window sum + estimate vs limit) and
    Retry-After (oldest charge's age)."""
    st = get_storage()
    try:
        st.set("rate_tpm_windows", "unit-key",
               [[time.time() - 10.0, 60.0], [time.time() - 2.0, 30.0]])
        # 90 used + 20 estimate > 100 → deny; oldest is 10s old → ~50s left
        retry = gw_auth._tpm_gate("unit-key", 100, 20)
        assert 49.0 < retry <= 50.0, retry
        # fits → 0.0
        assert gw_auth._tpm_gate("unit-key", 1000, 20) == 0.0
        # a huge estimate alone exceeds → deny even with headroom
        assert gw_auth._tpm_gate("unit-key", 100, 5000) > 0
    finally:
        reset_storage()
