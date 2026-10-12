"""Licensing canary tests (v1.5 closed distribution) — prove the
license/trial machinery is REAL, not decorative:

    signature verification against the ACTIVE public key,
    trial anchor lifecycle (fresh → aged → rollback),
    the middleware actually BLOCKS on expiry (403 license_expired,
    /healthz exempt), /admin/license surfaces claims, /healthz does
    NOT leak them, and the enterprise boot gate refuses to boot.

Tests mint licenses with a THROWAWAY keypair (set_license_public_key)
— the vendor private key never touches a test run.
"""
import datetime as dt
import json
import uuid
from datetime import datetime, timedelta, timezone

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway import licensing

from test_gateway_stage3 import _config, _secured_config, gguf, wired  # noqa: F401


# ── helpers ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def keypair():
    """Throwaway RSA keypair standing in for the vendor's."""
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


def _mint(priv: str, **overrides) -> str:
    now = int(datetime.now(timezone.utc).timestamp())
    claims = {"product": "gatelodge", "edition": "community",
              "customer": "Acme Corp", "support": False,
              "iat": now, "nbf": now}
    claims.update(overrides)
    return jwt.encode(claims, priv, algorithm="RS256")


def _write_license(path, token: str, *, bare: bool = False) -> None:
    if bare:
        path.write_text(token, encoding="utf-8")
    else:
        path.write_text(json.dumps({"license": token}), encoding="utf-8")


def _anchor(path, first: datetime, max_seen: datetime) -> None:
    path.write_text(json.dumps({
        "first_boot": first.isoformat(), "max_seen": max_seen.isoformat()}),
        encoding="utf-8")


NOW = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)


# ── license verification (bootstrap) ───────────────────────────────────────

def test_bootstrap_licensed_community(keypair, tmp_path):
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    tok = _mint(priv, edition="community", customer="Acme Corp",
                vendor="runpod", support=True, features=["sso"],
                exp=int((NOW + timedelta(days=90)).timestamp()))
    lic = tmp_path / "license.json"
    _write_license(lic, tok)
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json", now=NOW)
    assert st["mode"] == "licensed"
    assert st["license_edition"] == "community"
    assert st["customer"] == "Acme Corp"
    assert st["vendor"] == "runpod"
    assert st["support"] is True
    assert st["features"] == {"sso": True}
    assert st["expires_at"] is not None
    assert st["perpetual"] is False
    assert licensing.allows("sso") is True
    assert licensing.allows("tier_d") is False


def test_enterprise_clauses_license_community_binary(keypair, tmp_path):
    """An enterprise license on a community binary removes the trial
    too — they paid more, not less."""
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(priv, edition="enterprise",
                              features=["tier_d", "scim"]))
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json", now=NOW)
    assert st["mode"] == "licensed"
    assert st["license_edition"] == "enterprise"


def test_perpetual_license(keypair, tmp_path):
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(priv))  # no exp claim
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json", now=NOW)
    assert st["mode"] == "licensed"
    assert st["expires_at"] is None
    assert st["perpetual"] is True


def test_bare_jwt_file_form_accepted(keypair, tmp_path):
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(priv), bare=True)
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json", now=NOW)
    assert st["mode"] == "licensed"


def test_invalid_signature_falls_to_trial(keypair, tmp_path):
    """A license signed by ANYONE ELSE is ignored — trial applies."""
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = other.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()).decode("ascii")
    _, pub = keypair
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(forged))
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json",
                             trial_days=30, now=NOW)
    assert st["mode"] == "trial"
    assert st["trial_days_left"] == 30
    assert "invalid token" in (st["detail"] or "")
    assert licensing.allows("sso") is False


def test_expired_license_falls_to_trial(keypair, tmp_path):
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(priv, exp=int((NOW - timedelta(days=1))
                                             .timestamp())))
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json",
                             trial_days=30, now=NOW)
    assert st["mode"] == "trial"


def test_wrong_product_rejected(keypair, tmp_path):
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(priv, product="something-else"))
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json",
                             trial_days=30, now=NOW)
    assert st["mode"] == "trial"


def test_node_lock(keypair, tmp_path, monkeypatch):
    priv, pub = keypair
    licensing.set_license_public_key(pub)
    monkeypatch.setenv("GATELODGE_NODE_FINGERPRINT", "abc123def4567890")
    lic = tmp_path / "license.json"
    _write_license(lic, _mint(priv, node="ABC123DEF4567890"))  # case-insensitive
    st = licensing.bootstrap(edition="community", license_path=lic,
                             anchor_path=tmp_path / "anchor.json", now=NOW)
    assert st["mode"] == "licensed"

    lic2 = tmp_path / "license2.json"
    _write_license(lic2, _mint(priv, node="0000000000000000"))
    st2 = licensing.bootstrap(edition="community", license_path=lic2,
                              anchor_path=tmp_path / "anchor2.json", now=NOW)
    assert st2["mode"] == "trial"       # node-locked elsewhere → ignored


# ── trial anchor lifecycle ─────────────────────────────────────────────────

def test_trial_fresh_anchor_created(keypair, tmp_path):
    _, pub = keypair
    licensing.set_license_public_key(pub)
    anchor = tmp_path / "anchor.json"
    st = licensing.bootstrap(edition="community", anchor_path=anchor,
                             trial_days=30, now=NOW)
    assert st["mode"] == "trial"
    assert st["trial_days_left"] == 30
    data = json.loads(anchor.read_text())
    assert datetime.fromisoformat(data["first_boot"]) == NOW


def test_trial_aged_expired(keypair, tmp_path):
    _, pub = keypair
    licensing.set_license_public_key(pub)
    anchor = tmp_path / "anchor.json"
    _anchor(anchor, NOW - timedelta(days=31), NOW - timedelta(days=1))
    st = licensing.bootstrap(edition="community", anchor_path=anchor,
                             trial_days=30, now=NOW)
    assert st["mode"] == "expired"
    assert st["trial_days_left"] == 0


def test_trial_one_day_left(keypair, tmp_path):
    _, pub = keypair
    licensing.set_license_public_key(pub)
    anchor = tmp_path / "anchor.json"
    _anchor(anchor, NOW - timedelta(days=29), NOW - timedelta(hours=1))
    st = licensing.bootstrap(edition="community", anchor_path=anchor,
                             trial_days=30, now=NOW)
    assert st["mode"] == "trial"
    assert st["trial_days_left"] == 1


def test_clock_rollback_is_expiry(keypair, tmp_path):
    """max_seen 1 day in the future = the wall clock ran backwards —
    treat as tamper → expired."""
    _, pub = keypair
    licensing.set_license_public_key(pub)
    anchor = tmp_path / "anchor.json"
    _anchor(anchor, NOW - timedelta(days=5), NOW + timedelta(days=1))
    st = licensing.bootstrap(edition="community", anchor_path=anchor,
                             trial_days=30, now=NOW)
    assert st["mode"] == "expired"


# ── enterprise semantics ───────────────────────────────────────────────────

def test_enterprise_unlicensed_never_trials(keypair, tmp_path):
    _, pub = keypair
    licensing.set_license_public_key(pub)
    anchor = tmp_path / "anchor.json"
    st = licensing.bootstrap(edition="enterprise", anchor_path=anchor, now=NOW)
    assert st["mode"] == "unlicensed"
    assert not anchor.exists()          # no trial anchor for enterprise


def test_enterprise_boot_gate(keypair, tmp_path):
    priv, pub = keypair
    licensing.set_license_public_key(pub)

    # valid enterprise license → gate passes
    lic = tmp_path / "ent.json"
    _write_license(lic, _mint(priv, edition="enterprise"))
    st = licensing.bootstrap(edition="enterprise", license_path=lic,
                             anchor_path=tmp_path / "a.json", now=NOW)
    licensing.enterprise_boot_gate(st)   # no SystemExit

    # community license on the enterprise binary → refuse to boot
    lic2 = tmp_path / "com.json"
    _write_license(lic2, _mint(priv, edition="community"))
    st2 = licensing.bootstrap(edition="enterprise", license_path=lic2,
                              anchor_path=tmp_path / "b.json", now=NOW)
    with pytest.raises(SystemExit):
        licensing.enterprise_boot_gate(st2)

    # no license at all → refuse to boot
    st3 = licensing.bootstrap(edition="enterprise",
                              anchor_path=tmp_path / "c.json", now=NOW)
    with pytest.raises(SystemExit):
        licensing.enterprise_boot_gate(st3)


# ── runtime re-evaluation + middleware enforcement ─────────────────────────

def test_request_allowed_license_expires_midrun():
    # v2.2 grace: expired 5s ago is INSIDE the default 72h window —
    # serving continues under mode "grace"
    licensing.set_license_state({
        "mode": "licensed", "edition": "community",
        "license": {"exp": int((datetime.now(timezone.utc)
                                - timedelta(seconds=5)).timestamp())},
        "trial": None})
    ok, mode = licensing.request_allowed()
    assert ok is True and mode == "grace"
    # grace disabled → the hard stop at exp is back
    licensing.set_license_state({
        "mode": "licensed", "edition": "community",
        "license": {"exp": int((datetime.now(timezone.utc)
                                - timedelta(seconds=5)).timestamp()),
                    "grace_hours": 0},
        "trial": None})
    ok, mode = licensing.request_allowed()
    assert ok is False and mode == "expired"


def test_request_allowed_trial_state_recheck(tmp_path):
    anchor = tmp_path / "anchor.json"
    _anchor(anchor, datetime.now(timezone.utc) - timedelta(days=31),
            datetime.now(timezone.utc) - timedelta(days=1))
    licensing.set_license_state({
        "mode": "trial", "edition": "community", "license": None,
        "trial": {"trial_days": 30, "first_boot": "", "days_left": 1},
        "anchor_path": str(anchor)})
    ok, mode = licensing.request_allowed()
    assert ok is False and mode == "expired"


def test_request_allowed_fail_open_on_garbage():
    """A licensing BUG must not brick a deployment — malformed state
    fails open (well-defined failures still block)."""
    licensing.set_license_state({
        "mode": "licensed", "edition": "community",
        "license": {"exp": "not-a-number"}, "trial": None})
    ok, _mode = licensing.request_allowed()
    assert ok is True


def test_unmanaged_passes():
    assert licensing.request_allowed() == (True, "unmanaged")


def test_ensure_bootstrapped_does_not_clobber():
    """The entry-point chain (enterprise_main → core main) must keep
    the FIRST installed state — the core's community bootstrap never
    overwrites an enterprise one."""
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"product": "gatelodge", "edition": "enterprise",
                    "customer": "X", "features": ["tier_d"]},
        "trial": None})
    st = licensing.ensure_bootstrapped(edition="community")
    assert st["edition"] == "enterprise"
    assert st["license_edition"] == "enterprise"
    # and with no state it bootstraps normally
    licensing.set_license_state(None)
    st2 = licensing.ensure_bootstrapped(edition="community")
    assert st2["edition"] == "community"


def test_expired_blocks_v1_but_not_healthz(wired, gguf):  # noqa: F811
    wired(_config(gguf))
    licensing.set_license_state({
        "mode": "expired", "edition": "community", "license": None,
        "trial": {"trial_days": 30, "first_boot": "", "days_left": 0}})
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/v1/models")
            assert r.status_code == 403
            assert r.json()["error"]["type"] == "license_expired"
            h = client.get("/healthz")
            assert h.status_code == 200
            assert h.json()["license"]["mode"] == "expired"
            assert "customer" not in h.json()["license"]
    finally:
        licensing.set_license_state(None)


def test_licensed_serves_and_admin_license_surfaces_claims(wired, gguf):
    wired(_secured_config(gguf))
    licensing.set_license_state({
        "mode": "licensed", "edition": "community",
        "license": {"product": "gatelodge", "edition": "community",
                    "customer": "Acme Corp", "vendor": "runpod",
                    "support": True, "features": ["sso"]},
        "trial": None, "source": "file"})
    try:
        with TestClient(gw_app.app) as client:
            headers = {"Authorization": "Bearer admin-secret"}
            v = client.get("/v1/models", headers=headers)
            assert v.status_code == 200
            a = client.get("/admin/license", headers=headers)
            assert a.status_code == 200
            body = a.json()
            assert body["mode"] == "licensed"
            assert body["customer"] == "Acme Corp"
            assert body["vendor"] == "runpod"
            assert body["support"] is True
            assert body["features"] == {"sso": True}
            h = client.get("/healthz", headers=headers).json()
            assert h["license"]["mode"] == "licensed"
            assert "customer" not in h["license"]
    finally:
        licensing.set_license_state(None)


def test_healthz_unmanaged(wired, gguf):  # noqa: F811
    """Source runs / tests: healthz reports unmanaged, serving passes."""
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        h = client.get("/healthz")
        assert h.status_code == 200
        assert h.json()["license"] == {"mode": "unmanaged"}
        assert client.get("/v1/models").status_code == 200
