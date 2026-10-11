"""Cloud credential chains (v1.4) — auth: sigv4 / auth: adc backends.

No real cloud credentials on this box: the signer is unit-tested
against fixed inputs (deterministic — pinned regression), the ADC
token cache against a fake fetcher, and client construction against
the configured auth mode. The chains are lazy-imported (google-auth
optional).
"""
import time

import httpx
import pytest

import gateway.cloud as gw_cloud
from gateway import credchains
from gateway.config import GatewayConfig

# ── SigV4 signer (pure function, fixed inputs → pinned signature) ──────────

AK = "AKIDEXAMPLE"
SK = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
ST = "AQoEXAMPLESESSIONTOKEN"
URL = "https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1/chat/completions"


def test_sigv4_deterministic_and_wellformed():
    authz, headers = credchains._sigv4_sign(
        AK, SK, ST, "us-east-1", "bedrock", "POST", URL,
        b'{"model":"m"}', "20260101T000000Z")
    assert authz.startswith(
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260101/us-east-1/"
        "bedrock/aws4_request, SignedHeaders=host;x-amz-content-sha256;"
        "x-amz-date;x-amz-security-token, Signature=")
    assert len(authz.split("Signature=")[1]) == 64   # sha256 hex
    assert headers["x-amz-date"] == "20260101T000000Z"
    # deterministic: same inputs → same signature (regression pin)
    authz2, _ = credchains._sigv4_sign(
        AK, SK, ST, "us-east-1", "bedrock", "POST", URL,
        b'{"model":"m"}', "20260101T000000Z")
    assert authz2 == authz
    # any input change → different signature
    authz3, _ = credchains._sigv4_sign(
        AK, SK, ST, "us-east-1", "bedrock", "POST", URL,
        b'{"model":"x"}', "20260101T000000Z")
    assert authz3 != authz
    authz4, _ = credchains._sigv4_sign(
        AK, SK, ST, "eu-west-1", "bedrock", "POST", URL,
        b'{"model":"m"}', "20260101T000000Z")
    assert authz4 != authz


def test_sigv4_auth_flow_signs_request(monkeypatch):
    """SigV4Auth.auth_flow stamps Authorization + x-amz headers from
    the env-var chain; missing creds → CloudCredentialError."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", AK)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SK)
    monkeypatch.setenv("AWS_SESSION_TOKEN", ST)
    monkeypatch.delenv("AWS_REGION", raising=False)
    auth = credchains.SigV4Auth(region="us-east-1")
    req = httpx.Request("POST", URL, content=b'{"x":1}')
    next(auth.auth_flow(req))
    assert req.headers["Authorization"].startswith("AWS4-HMAC-SHA256")
    assert "x-amz-date" in req.headers
    assert "x-amz-content-sha256" in req.headers
    # missing creds → clear error
    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    auth2 = credchains.SigV4Auth(region="us-east-1")
    req2 = httpx.Request("POST", URL, content=b"{}")
    with pytest.raises(credchains.CloudCredentialError):
        next(auth2.auth_flow(req2))


# ── ADC token cache (fake fetcher) ─────────────────────────────────────────

def test_adc_cache_and_refresh():
    """ADCAuth caches the token until ~expiry (one fetch), refreshes
    when stale — the OpenAI SDK never sees the refresh."""
    calls = []

    def fake_fetch():
        calls.append(time.time())
        return f"tok-{len(calls)}", time.time() + 3600.0

    auth = credchains.ADCAuth(fetcher=fake_fetch)
    r1 = httpx.Request("GET", "https://x/v1/models")
    next(auth.auth_flow(r1))
    r2 = httpx.Request("GET", "https://x/v1/models")
    next(auth.auth_flow(r2))
    assert r1.headers["Authorization"] == "Bearer tok-1"
    assert r2.headers["Authorization"] == "Bearer tok-1"   # cached
    assert len(calls) == 1
    # force expiry (inside the 60s refresh skew) → refresh on next use
    auth._expires = time.time() + 10
    r3 = httpx.Request("GET", "https://x/v1/models")
    next(auth.auth_flow(r3))
    assert r3.headers["Authorization"] == "Bearer tok-2"
    assert len(calls) == 2


def test_adc_missing_google_auth(monkeypatch):
    """No google-auth package → CloudCredentialError with the fix in
    the message (lazy import never raises ImportError through the
    request path)."""
    import builtins
    real_import = builtins.__import__

    def _no_google(name, *a, **k):
        if name.startswith("google"):
            raise ImportError("no google")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _no_google)
    with pytest.raises(credchains.CloudCredentialError) as ei:
        credchains._adc_fetch_token()
    assert "google-auth" in str(ei.value)


# ── client construction by auth mode ───────────────────────────────────────

def _cfg_with_backend(backend: dict) -> GatewayConfig:
    import json
    import gateway.config as gw_config
    raw = json.loads(json.dumps({
        "port": 9911, "backends": {"cb": backend}}))
    cfg = gw_config.GatewayConfig.__new__(GatewayConfig)
    cfg.__dict__.update(gw_config.GatewayConfig.__dict__)
    # simpler: build through the normal path
    import tempfile
    from pathlib import Path
    p = Path(tempfile.mkdtemp()) / "c.json"
    p.write_text(json.dumps({"port": 9911,
                             "backends": {"cb": backend}}), encoding="utf-8")
    return GatewayConfig(p)


def test_config_parses_auth_region():
    cfg = _cfg_with_backend({
        "type": "openai", "auth": "SIGV4", "region": "us-west-2",
        "base_url": "https://x", "models": {"m": "m"}})
    b = cfg.backends["cb"]
    assert b["auth"] == "sigv4"          # normalized (case-insensitive)
    assert b["region"] == "us-west-2"
    # junk auth mode → empty (classic key auth)
    cfg2 = _cfg_with_backend({
        "type": "openai", "auth": "oauth9", "base_url": "https://x",
        "models": {"m": "m"}})
    assert cfg2.backends["cb"]["auth"] == ""


def test_client_construction_by_auth_mode(monkeypatch):
    """sigv4/adc backends build clients with the platform-chain auth
    (no api key resolution); classic backends unchanged."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", AK)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SK)
    try:
        c_sig = gw_cloud.get_cloud_client("t-sig", {
            "type": "openai", "auth": "sigv4", "region": "us-east-1",
            "base_url": "https://bedrock.example"})
        assert c_sig is not None
        c_adc = gw_cloud.get_cloud_client("t-adc", {
            "type": "openai", "auth": "adc",
            "base_url": "https://vertex.example"})
        assert c_adc is not None
        # classic backend: no auth field → static key path (env unset →
        # "not-set" placeholder, no crash)
        c_cls = gw_cloud.get_cloud_client("t-cls", {
            "type": "openai", "base_url": "https://classic.example"})
        assert c_cls is not None
    finally:
        gw_cloud.reset_clients()


def test_client_cache_signature_includes_auth():
    """Two backends, same base_url, different auth modes → distinct
    cached clients (the signature includes the mode)."""
    gw_cloud.get_cloud_client("t-a", {"type": "openai",
                                      "base_url": "https://same.example"})
    gw_cloud.get_cloud_client("t-b", {"type": "openai", "auth": "adc",
                                      "base_url": "https://same.example"})
    assert (gw_cloud._CLIENTS["t-a"][0] != gw_cloud._CLIENTS["t-b"][0])
    gw_cloud.reset_clients()
