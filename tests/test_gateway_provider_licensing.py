"""Provider-licensing canary tests (v2.2) — the per-backend cloud
entitlement + the license claim shape:

    provider_allowed: embedded/llama-server are FREE; openai/runpod/
    runpod-public backends serve only when their NAME is in the
    license ``providers`` claim. Unmanaged (source/tests) and TRIAL
    grant everything (trial = the full product, evaluation posture);

    features: v2 {code: Y/N} map with v1 list back-compat;

    identity: business_name + contract_email surface via status();

    enforcement (defense in depth): resolve_chain DROPS unlicensed
    entries (mixed chains serve their licensed part), an all-
    unlicensed plan raises ProviderNotLicensedError → HTTP 403
    license_providers on every serving surface, and /admin/backends
    carries a licensed:false flag for the console.

License minting uses a THROWAWAY keypair (set_license_public_key) —
the vendor private key never touches a test run.
"""
import datetime as dt
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
from gateway import licensing
from gateway.config import ProviderNotLicensedError

from test_gateway_stage3 import _config, _secured_config, gguf, wired  # noqa: F401

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture(scope="module")
def keypair():
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = k.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()).decode("ascii")
    pub = k.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")
    return priv, pub


@pytest.fixture(autouse=True)
def _reset():
    licensing.reset_licensing()
    gw_config.reset_backend_filter()
    yield
    licensing.reset_licensing()
    gw_config.reset_backend_filter()


def _mint(priv: str, **overrides) -> str:
    now = int(datetime.now(timezone.utc).timestamp())
    claims = {"product": "gatelodge", "edition": "enterprise",
              "business_name": "Acme Corp",
              "contract_email": "ops@acme.com",
              "support": True,
              "features": {c: True for c in licensing.FEATURE_CODES},
              "iat": now, "nbf": now}
    claims.update(overrides)
    return jwt.encode(claims, priv, algorithm="RS256")


def _activate(priv: str, pub: str, **claims_over) -> None:
    """Mint + install a licensed state (env-inline license)."""
    import os
    licensing.set_license_public_key(pub)
    os.environ["GATELODGE_LICENSE"] = _mint(priv, **claims_over)
    try:
        licensing.bootstrap(edition="enterprise")
    finally:
        os.environ.pop("GATELODGE_LICENSE", None)


# ── provider_allowed semantics ─────────────────────────────────────────────

def test_unmanaged_allows_everything():
    """No licensing state (source/tests): no gate."""
    assert licensing.provider_allowed("anything", "openai")
    assert licensing.provider_allowed("anything", "runpod")


def test_trial_allows_everything():
    """Trial = the full product (evaluation posture)."""
    licensing.set_license_state({"edition": "community", "mode": "trial",
                                 "license": None, "trial":
                                 {"days_left": 20}})
    assert licensing.provider_allowed("hf", "openai")
    assert licensing.provider_allowed("rp", "runpod")


def test_licensed_local_only_default(keypair):
    """Local compute is FREE: no providers claim needed. Cloud types
    are locked without the claim (the default posture)."""
    priv, pub = keypair
    _activate(priv, pub)                      # no providers claim
    assert licensing.provider_allowed("local", "embedded")
    assert licensing.provider_allowed("srv", "llama-server")
    assert not licensing.provider_allowed("hf", "openai")
    assert not licensing.provider_allowed("rp", "runpod")
    assert not licensing.provider_allowed("rp", "runpod-public")


def test_licensed_named_providers(keypair):
    """Per-backend granularity: exactly the named backends serve."""
    priv, pub = keypair
    _activate(priv, pub, providers=["hf", "rp"])
    assert licensing.provider_allowed("hf", "openai")       # named
    assert licensing.provider_allowed("rp", "runpod")       # named
    assert not licensing.provider_allowed("groq", "openai") # not named
    assert licensing.provider_allowed("local", "embedded")  # free anyway


def test_expired_state_denies_all():
    """Expired/unlicensed: nothing (the license middleware stops
    serving anyway — this is defense in depth)."""
    licensing.set_license_state({"edition": "community", "mode": "expired",
                                 "license": None})
    assert not licensing.provider_allowed("local", "embedded")
    assert not licensing.provider_allowed("hf", "openai")


# ── features Y/N + identity claims ─────────────────────────────────────────

def test_features_yn_map_and_v1_list(keypair):
    priv, pub = keypair
    _activate(priv, pub,
              features={"tier_d": True, "scim": True, "mcp": False,
                        "session_budgets": False, "teams": True})
    assert licensing.allows("tier_d")
    assert licensing.allows("teams")
    assert not licensing.allows("mcp")
    assert not licensing.allows("session_budgets")

    _activate(priv, pub, features=["tier_d", "mcp"])        # v1 list
    assert licensing.allows("tier_d")
    assert licensing.allows("mcp")                           # present = Y
    assert not licensing.allows("teams")                     # absent = N


def test_status_identity_and_entitlements(keypair):
    priv, pub = keypair
    _activate(priv, pub, providers=["hf"],
              contract_email="contracts@acme.com")
    st = licensing.status()
    assert st["mode"] == "licensed"
    assert st["business_name"] == "Acme Corp"
    assert st["customer"] == "Acme Corp"          # legacy alias
    assert st["contract_email"] == "contracts@acme.com"
    assert st["providers"] == ["hf"]
    assert isinstance(st["features"], dict) and st["features"]["mcp"] is True
    assert set(st["feature_codes"]) == set(licensing.FEATURE_CODES)


# ── enforcement: resolve_chain drops + 403s ────────────────────────────────

def _cloud_config(gguf):
    """local:fast (embedded) + groq:cloud-fast (openai cloud)."""
    cfgd = _secured_config(gguf)
    return cfgd


def test_resolve_chain_drops_unlicensed_entries(wired, gguf, keypair):
    """A mixed chain keeps its licensed part; an all-unlicensed plan
    raises ProviderNotLicensedError (never silently 404s)."""
    priv, pub = keypair
    cfgd = _cloud_config(gguf)
    cfgd["aliases"] = {"mixed": {"chain": ["local:fast",
                                           "groq:cloud-fast"],
                                 "hedge_s": 0},
                       "purecloud": {"chain": ["groq:cloud-fast"],
                                     "hedge_s": 0}}
    wired(cfgd)
    cfg = gw_config.get_gateway_config()
    # install the gate exactly as the app startup does (this test
    # drives resolve_chain directly, no TestClient lifespan)
    gw_config.set_backend_filter(licensing.provider_allowed)
    # unmanaged: everything resolves (source/tests default)
    plan = cfg.resolve_chain("mixed")
    assert len(plan["entries"]) == 2
    # licensed WITHOUT the groq entitlement: mixed drops to local
    _activate(priv, pub)                              # no providers
    plan = cfg.resolve_chain("mixed")
    assert plan["entries"] == [("local", "fast")]
    with pytest.raises(ProviderNotLicensedError):
        cfg.resolve_chain("purecloud")
    # named entitlement restores the full chain
    _activate(priv, pub, providers=["groq"])
    assert len(cfg.resolve_chain("mixed")["entries"]) == 2


def test_serving_403_license_providers(wired, gguf, keypair):
    """HTTP 403 license_providers (not 400 model_not_found) on every
    serving surface — the model exists, the entitlement doesn't. No
    fallback: smart routes don't mask a licensing answer."""
    priv, pub = keypair
    wired(_cloud_config(gguf))
    _activate(priv, pub)                              # groq not entitled
    with TestClient(gw_app.app) as client:
        for path, body in (
                ("/v1/chat/completions",
                 {"model": "groq:cloud-fast",
                  "messages": [{"role": "user", "content": "x"}]}),
                ("/v1/embeddings", {"model": "groq:cloud-fast",
                                    "input": "x"}),
                ("/v1/images/generations",
                 {"model": "groq:cloud-fast", "prompt": "x"}),
                ("/v1/completions",
                 {"model": "groq:cloud-fast", "prompt": "x"})):
            r = client.post(path, json=body,
                            headers={"Authorization": "Bearer admin-secret"})
            assert r.status_code == 403, (path, r.status_code, r.text)
            assert r.json()["error"]["code"] == "license_providers", path
        # local serving is untouched by the gate (502 here = no
        # llama_cpp in the test env — NOT a license error)
        r = client.post("/v1/chat/completions",
                        json={"model": "local:fast",
                              "messages": [{"role": "user",
                                            "content": "x"}]},
                        headers={"Authorization": "Bearer admin-secret"})
        assert (r.json().get("error") or {}).get("code") \
            != "license_providers"


def test_admin_backends_licensed_flag(wired, gguf, keypair):
    """/admin/backends marks unlicensed cloud backends for the console
    (they stay VISIBLE — config truth — but flagged)."""
    priv, pub = keypair
    wired(_cloud_config(gguf))
    _activate(priv, pub)                              # groq not entitled
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends",
                       headers={"Authorization": "Bearer admin-secret"})
        assert r.status_code == 200
        by_name = {b["name"]: b for b in r.json()["backends"]}
        assert by_name["local"]["licensed"] is True
        assert by_name["groq"]["licensed"] is False
        # with the entitlement: flag flips (hot — reads live state)
        _activate(priv, pub, providers=["groq"])
        r = client.get("/admin/backends",
                       headers={"Authorization": "Bearer admin-secret"})
        by_name = {b["name"]: b for b in r.json()["backends"]}
        assert by_name["groq"]["licensed"] is True


# ── license_tool: durations, Y/N features, providers, identity ────────────

def _tool():
    import license_tool
    return license_tool


def test_tool_issue_claims_and_durations(keypair, tmp_path, monkeypatch):
    """issue → plaintext claims + RS256 signature; duration presets
    produce the right exp; --feature Y/N, --provider, identity."""
    tool = _tool()
    priv, pub = keypair
    # throwaway signing key for the tool
    keyfile = tmp_path / "signing.pem"
    keyfile.write_text(priv, encoding="ascii")
    monkeypatch.setattr(tool, "KEY_PATH", keyfile)
    out = tmp_path / "license.json"

    rc = tool.main(["issue", "--edition", "enterprise",
                    "--business-name", "Acme Corp",
                    "--contract-email", "ops@acme.com",
                    "--feature", "teams=N", "--feature", "mcp=Y",
                    "--provider", "hf", "--provider", "rp",
                    "--duration", "90d", "--out", str(out)])
    assert rc == 0
    token = json.loads(out.read_text())["license"]
    claims = jwt.decode(token, pub, algorithms=["RS256"])
    assert claims["business_name"] == "Acme Corp"
    assert claims["contract_email"] == "ops@acme.com"
    assert claims["features"] == {c: True for c in tool.ENTERPRISE_FEATURES
                                  if c != "teams"} | {"teams": False}
    assert claims["providers"] == ["hf", "rp"]
    exp = dt.datetime.fromtimestamp(claims["exp"], dt.timezone.utc)
    expect = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=90)
    assert abs((exp - expect).total_seconds()) < 60

    # perpetual: no exp at all
    out2 = tmp_path / "perpetual.json"
    rc = tool.main(["issue", "--edition", "community",
                    "--business-name", "Beta LLC",
                    "--duration", "perpetual", "--out", str(out2)])
    assert rc == 0
    claims2 = jwt.decode(json.loads(out2.read_text())["license"],
                         pub, algorithms=["RS256"])
    assert "exp" not in claims2


def test_tool_requires_business_name(keypair, tmp_path, monkeypatch):
    tool = _tool()
    priv, pub = keypair
    keyfile = tmp_path / "signing.pem"
    keyfile.write_text(priv, encoding="ascii")
    monkeypatch.setattr(tool, "KEY_PATH", keyfile)
    rc = tool.main(["issue", "--edition", "enterprise",
                    "--out", str(tmp_path / "x.json")])
    assert rc == 2                                   # identity required
