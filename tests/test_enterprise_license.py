"""Enterprise licensing canary tests (v1.5 closed distribution) — the
COMMERCIAL policy, proven live:

    the enterprise entry point REFUSES TO BOOT without a valid
    ENTERPRISE license (community licenses and garbage both exit 2
    before any wiring or serving);

    the license's feature claims gate the commercial modules — tier D,
    SCIM and the session-budget link stay OFF without their keys even
    when the operator's config/env asks for them.

Mechanism tests (signature, trial, middleware) live upstream in
tests/test_gateway_licensing.py; these prove the FORK's wiring.
"""
import datetime as dt
import json
from datetime import datetime, timedelta, timezone

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import enterprise_main
from gateway import licensing
from gateway.auth import get_auth_pipeline, set_auth_pipeline, AuthPipeline
from gateway_enterprise.session_budget import session_budget_link


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
def _reset_licensing():
    licensing.reset_licensing()
    yield
    licensing.reset_licensing()
    set_auth_pipeline(AuthPipeline())    # core default


def _mint(priv: str, **overrides) -> str:
    now = int(datetime.now(timezone.utc).timestamp())
    claims = {"product": "gatelodge", "edition": "enterprise",
              "customer": "Enterprise Co", "support": True,
              "features": ["tier_d", "scim", "session_budgets", "mcp"],
              "iat": now, "nbf": now}
    claims.update(overrides)
    return jwt.encode(claims, priv, algorithm="RS256")


# ── the boot gate ──────────────────────────────────────────────────────────

def test_main_refuses_to_boot_without_license(monkeypatch, tmp_path):
    """No license → SystemExit(2) BEFORE any wiring/serving (the inline
    env var keeps the test independent of the repo's own data dir)."""
    monkeypatch.setenv("GATELODGE_LICENSE",
                       str(tmp_path / "does-not-exist.jwt"))
    with pytest.raises(SystemExit) as ei:
        enterprise_main.main()
    assert ei.value.code == 2


def test_main_refuses_to_boot_on_garbage_license(monkeypatch):
    monkeypatch.setenv("GATELODGE_LICENSE", "not-a-real-jwt")
    with pytest.raises(SystemExit) as ei:
        enterprise_main.main()
    assert ei.value.code == 2


def test_main_refuses_to_boot_on_community_license(keypair, monkeypatch):
    """A paid COMMUNITY license does not unlock the enterprise binary."""
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    monkeypatch.setenv("GATELODGE_LICENSE",
                       _mint(priv, edition="community", features=[]))
    with pytest.raises(SystemExit) as ei:
        enterprise_main.main()
    assert ei.value.code == 2


def test_main_boots_and_wires_with_enterprise_license(keypair, monkeypatch):
    """A valid enterprise license → main() proceeds through ALL the
    wiring (with the licensed flags) into the core entry point."""
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    monkeypatch.setenv("GATELODGE_LICENSE", _mint(priv))
    calls = {}
    monkeypatch.setattr(enterprise_main, "_wire_tier_d",
                        lambda enabled: calls.__setitem__("tier_d", enabled))
    monkeypatch.setattr(enterprise_main, "_wire_scim",
                        lambda enabled: calls.__setitem__("scim", enabled))
    monkeypatch.setattr(enterprise_main, "_wire_mcp",
                        lambda enabled: calls.__setitem__("mcp", enabled))
    monkeypatch.setattr(enterprise_main, "_wire_auth_pipeline",
                        lambda session_budgets: calls.__setitem__(
                            "session_budgets", session_budgets))
    import gateway.__main__ as gw_main
    monkeypatch.setattr(gw_main, "main",
                        lambda: calls.__setitem__("core", True))
    enterprise_main.main()
    assert calls == {"tier_d": True, "scim": True, "mcp": True,
                     "session_budgets": True, "core": True}


def test_main_wires_only_licensed_features(keypair, monkeypatch):
    """Feature claims are the commercial dial: an enterprise license
    WITHOUT the module keys boots, but the wiring stays off."""
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    monkeypatch.setenv("GATELODGE_LICENSE",
                       _mint(priv, features=["tier_d"]))
    calls = {}
    monkeypatch.setattr(enterprise_main, "_wire_tier_d",
                        lambda enabled: calls.__setitem__("tier_d", enabled))
    monkeypatch.setattr(enterprise_main, "_wire_scim",
                        lambda enabled: calls.__setitem__("scim", enabled))
    monkeypatch.setattr(enterprise_main, "_wire_mcp",
                        lambda enabled: calls.__setitem__("mcp", enabled))
    monkeypatch.setattr(enterprise_main, "_wire_auth_pipeline",
                        lambda session_budgets: calls.__setitem__(
                            "session_budgets", session_budgets))
    import gateway.__main__ as gw_main
    monkeypatch.setattr(gw_main, "main",
                        lambda: calls.__setitem__("core", True))
    enterprise_main.main()
    assert calls == {"tier_d": True, "scim": False, "mcp": False,
                     "session_budgets": False, "core": True}


# ── the wiring itself obeys the claims ─────────────────────────────────────

def test_wire_tier_d_unlicensed_skips_swap(monkeypatch):
    """tier_d claim absent → env vars are IGNORED (no storage/config
    swap) — the license outranks the operator's environment."""
    import gateway.storage as gw_storage
    swapped = []
    monkeypatch.setattr(gw_storage, "set_storage",
                        lambda s: swapped.append(s))
    monkeypatch.setenv("GATELODGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    monkeypatch.setenv("GATELODGE_CONFIG_DSN", "postgresql://x/y")
    enterprise_main._wire_tier_d(enabled=False)
    assert swapped == []


def test_wire_tier_d_licensed_swaps(monkeypatch):
    import gateway.storage as gw_storage
    swapped = []
    real = gw_storage.set_storage
    monkeypatch.setattr(gw_storage, "set_storage",
                        lambda s: (swapped.append(s),
                                   real(gw_storage.InMemoryStorage()))[1])
    monkeypatch.setenv("GATELODGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    enterprise_main._wire_tier_d(enabled=True)
    assert len(swapped) == 1
    assert type(swapped[0]).__name__ == "RedisStorage"


def test_wire_scim_unlicensed_never_mounts(monkeypatch, tmp_path):
    import gateway.app as gw_app
    mounted = []
    monkeypatch.setattr(gw_app, "set_extra_router",
                        lambda r, prefix: mounted.append(prefix))
    # a config WITH scim.enabled — the claim must override it
    cfgfile = tmp_path / "gateway_config.json"
    cfgfile.write_text(json.dumps({"scim": {"enabled": True}}),
                       encoding="utf-8")
    import gateway.config as gw_config
    monkeypatch.setattr(gw_config, "_CONFIG",
                        gw_config.GatewayConfig(cfgfile))
    enterprise_main._wire_scim(enabled=False)
    assert mounted == []


def test_wire_auth_pipeline_links_follow_claim():
    enterprise_main._wire_auth_pipeline(session_budgets=True)
    links = get_auth_pipeline().links
    assert len(links) == 5
    assert links[2] is session_budget_link

    enterprise_main._wire_auth_pipeline(session_budgets=False)
    links = get_auth_pipeline().links
    assert len(links) == 4
    assert session_budget_link not in links
