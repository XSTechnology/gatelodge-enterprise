"""License-ops canary tests (v2.2) — the operational licensing
capabilities:

    SERIAL + LEDGER: every issued license carries a unique serial
    (GL-…); license_tool appends each issuance to the vendor-side
    ledger (vendor_keys/issued_ledger.jsonl); the serial surfaces in
    /admin/license status;

    KEY ROTATION (kid): the gateway trusts a LIST of public keys; a
    token's kid header (sha16 of the signing public key) selects its
    key; kid-less tokens fall back newest-first; an unknown kid is a
    clear rejection — rotation without bricking deployed binaries;

    EXPIRY EARLY-WARNING: levels at 30/7/1 days out (None when
    further out, perpetual, or already expired);

    HOT-RELOAD: a renewed license file swaps in place (mtime watch,
    no restart). A broken or edition-downgrade file keeps the OLD
    state (a botched renewal must never make serving worse) and
    doesn't re-verify until the file changes again;

    SCALE CAPS: caps {keys, instances, teams, gpus} from the license
    — absent = unlimited, trial = unlimited; /admin/keys and
    /admin/load refuse creation beyond the cap (403 license_caps).
"""
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway import licensing
from gateway.licensing import key_id

from test_gateway_stage3 import (_config, _secured_config, gguf,  # noqa: F401
                                 wired, _with_fake_llama)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


def _keypair():
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
    yield
    licensing.reset_licensing()


def _mint(priv, pub, kid=True, **over):
    """A licensed-enterprise token; kid header by default (the tool
    always stamps it)."""
    now = int(time.time())
    claims = {"product": "gatelodge", "edition": "enterprise",
              "serial": "GL-" + uuid.uuid4().hex[:12].upper(),
              "business_name": "Acme Corp", "support": True,
              "features": {c: True for c in licensing.FEATURE_CODES},
              "iat": now, "nbf": now}
    claims.update(over)
    headers = {"kid": key_id(pub)} if kid else None
    return jwt.encode(claims, priv, algorithm="RS256", headers=headers)


def _activate_file(path, priv, pub, **over):
    """bootstrap from a license FILE (hot-reload-able path)."""
    licensing.set_license_public_key(pub)
    path.write_text(json.dumps({"license": _mint(priv, pub, **over)}),
                    encoding="utf-8")
    return licensing.bootstrap(edition="enterprise", license_path=path)


# ── serial + ledger (tool) ─────────────────────────────────────────────────

def test_tool_serial_and_ledger(tmp_path, monkeypatch):
    import license_tool as tool
    priv, pub = _keypair()
    keyfile = tmp_path / "signing.pem"
    keyfile.write_text(priv, encoding="ascii")
    monkeypatch.setattr(tool, "KEY_PATH", keyfile)
    monkeypatch.setattr(tool, "LEDGER_PATH", tmp_path / "ledger.jsonl")
    out = tmp_path / "license.json"
    rc = tool.main(["issue", "--edition", "enterprise",
                    "--business-name", "Acme Corp",
                    "--duration", "1y", "--out", str(out)])
    assert rc == 0
    token = json.loads(out.read_text())["license"]
    # kid header present + matches the signing key
    header = json.loads(jwt.utils.base64url_decode(
        token.split(".")[0].encode() + b"==").decode())
    assert header["kid"] == key_id(pub)
    claims = jwt.decode(token, pub, algorithms=["RS256"])
    assert claims["serial"].startswith("GL-") and len(claims["serial"]) == 15
    # ledger: one line, matching serial + identity
    lines = (tmp_path / "ledger.jsonl").read_text().strip().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["serial"] == claims["serial"]
    assert entry["business_name"] == "Acme Corp"
    assert entry["kid"] == key_id(pub)


def test_status_surfaces_serial_caps_days_left(tmp_path):
    priv, pub = _keypair()
    lic = tmp_path / "license.json"
    _activate_file(lic, priv, pub,
                   caps={"keys": 10, "instances": 2},
                   exp=int(time.time()) + 20 * 86400)     # 20 days out
    st = licensing.status()
    assert st["serial"].startswith("GL-")
    assert st["caps"] == {"keys": 10, "instances": 2}
    assert st["days_left"] is not None and 19 < st["days_left"] <= 20
    assert st["expiry_warning"] == 30                     # 30-day level


# ── key rotation (kid) ─────────────────────────────────────────────────────

def test_kid_rotation_two_trusted_keys():
    """Old + new keys verify side by side; unknown kid rejects; a
    kid-less token falls back to trying keys newest-first."""
    priv1, pub1 = _keypair()
    priv2, pub2 = _keypair()
    licensing.set_license_public_keys([pub2, pub1])       # newest first
    t1 = _mint(priv1, pub1)                               # kid = key1
    t2 = _mint(priv2, pub2)                               # kid = key2
    now = datetime.now(timezone.utc)
    c1, why1 = licensing._verify_token(t1, now)
    c2, why2 = licensing._verify_token(t2, now)
    assert c1 and c2, (why1, why2)

    # unknown kid: a token signed by a key that is NOT trusted
    priv3, pub3 = _keypair()
    t3 = _mint(priv3, pub3)
    c3, why3 = licensing._verify_token(t3, now)
    assert c3 is None and "unknown signing key" in why3

    # kid-less token (v1/early-v2 shape): tries keys in order
    t4 = _mint(priv1, pub1, kid=False)
    c4, why4 = licensing._verify_token(t4, now)
    assert c4, why4
    t5 = _mint(priv2, pub2, kid=False)
    c5, why5 = licensing._verify_token(t5, now)
    assert c5, why5


# ── expiry early-warning levels ────────────────────────────────────────────

def test_expiry_warning_levels():
    for days, level in ((40, None), (29, 30), (20, 30), (6, 7),
                        (0.5, 1), (-1, 0), (-4, None)):
        state = {"mode": "licensed", "edition": "community",
                 "license": {"exp": int(time.time() + days * 86400)},
                 "source": "file", "detail": "", "trial": None}
        licensing.set_license_state(state)
        w = licensing.expiry_warning()
        if level is None:
            assert w is None, days
        else:
            assert w and w["level"] == level, (days, w)
    # -1 day is INSIDE the default 72h grace → level 0 (grace: True)
    licensing.set_license_state({"mode": "licensed", "edition": "community",
                                 "license": {"exp": int(time.time() - 86400)},
                                 "source": "file", "detail": "",
                                 "trial": None})
    w = licensing.expiry_warning()
    assert w and w["level"] == 0 and w["grace"] is True
    assert 47 < w["hours_left"] <= 71
    # perpetual: no warning
    licensing.set_license_state({"mode": "licensed", "edition": "community",
                                 "license": {}, "source": "file",
                                 "detail": "", "trial": None})
    assert licensing.expiry_warning() is None


# ── hot-reload ─────────────────────────────────────────────────────────────

def test_hot_reload_renewal(tmp_path):
    priv, pub = _keypair()
    lic = tmp_path / "license.json"
    st = _activate_file(lic, priv, pub)
    assert st["mode"] == "licensed"
    first_exp = licensing.status()["expires_at"]
    assert licensing.maybe_reload_license() is False       # unchanged file

    # renew: same key, longer window
    time.sleep(0.02)
    lic.write_text(json.dumps({"license": _mint(
        priv, pub, exp=int(time.time()) + 400 * 86400)}), encoding="utf-8")
    assert licensing.maybe_reload_license() is True
    assert licensing.status()["expires_at"] != first_exp


def test_hot_reload_broken_file_keeps_old_state(tmp_path):
    priv, pub = _keypair()
    lic = tmp_path / "license.json"
    _activate_file(lic, priv, pub)
    before = licensing.status()
    time.sleep(0.02)
    lic.write_text(json.dumps({"license": "not-a-jwt"}), encoding="utf-8")
    assert licensing.maybe_reload_license() is False
    after = licensing.status()
    assert after["mode"] == "licensed"
    assert after["serial"] == before["serial"]


def test_hot_reload_rejects_edition_downgrade(tmp_path):
    """An enterprise binary must not hot-reload onto a community
    license — the boot gate's edition requirement survives renewal."""
    priv, pub = _keypair()
    lic = tmp_path / "license.json"
    _activate_file(lic, priv, pub)
    before = licensing.status()
    time.sleep(0.02)
    now = int(time.time())
    claims = {"product": "gatelodge", "edition": "community",
              "serial": "GL-" + uuid.uuid4().hex[:12].upper(),
              "business_name": "Downgrade Inc", "support": False,
              "iat": now, "nbf": now}
    lic.write_text(json.dumps({"license": jwt.encode(
        claims, priv, algorithm="RS256", headers={"kid": key_id(pub)})}),
        encoding="utf-8")
    assert licensing.maybe_reload_license() is False
    assert licensing.status()["business_name"] == before["business_name"]


# ── scale caps ─────────────────────────────────────────────────────────────

def test_cap_allows_semantics():
    # no state: unlimited
    assert licensing.cap_allows("keys", 999) == (True, None)
    # trial: unlimited (the full product)
    licensing.set_license_state({"mode": "trial", "edition": "community",
                                 "license": None, "trial": {"days_left": 9}})
    assert licensing.cap_allows("keys", 999) == (True, None)
    # licensed with caps
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"caps": {"keys": 2, "instances": 1}},
        "source": "file", "detail": "", "trial": None})
    assert licensing.cap_allows("keys", 1) == (True, 2)
    assert licensing.cap_allows("keys", 2) == (False, 2)
    assert licensing.cap_allows("instances", 0) == (True, 1)
    assert licensing.cap_allows("instances", 1) == (False, 1)
    assert licensing.cap_allows("teams", 50) == (True, None)   # uncapped
    # junk values are dropped (that cap = unlimited)
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"caps": {"keys": "many"}}, "source": "file",
        "detail": "", "trial": None})
    assert licensing.cap_allows("keys", 999) == (True, None)


def test_admin_keys_cap_enforced(wired, gguf):
    """Virtual-key creation refuses at the licensed cap (403
    license_caps); under the cap it still works."""
    priv, pub = _keypair()
    wired(_secured_config(gguf))               # has admin-secret gateway key
    licensing.set_license_public_key(pub)
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"caps": {"keys": 2}}, "source": "file",
        "detail": "", "trial": None})
    hdrs = {"Authorization": "Bearer admin-secret"}
    with TestClient(gw_app.app) as client:
        for i in (1, 2):
            r = client.post("/admin/keys", headers=hdrs,
                            json={"id": f"cap-key-{i}", "key": f"k{i}"})
            assert r.status_code == 200, r.text
        r = client.post("/admin/keys", headers=hdrs,
                        json={"id": "cap-key-3", "key": "k3"})
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "license_caps"


def test_admin_load_instances_cap(wired, gguf):
    """Instance loads refuse beyond the licensed cap (403
    license_caps); under it they load."""
    priv, pub = _keypair()
    cfgd = _secured_config(gguf)
    cfgd["backends"]["local"]["models"]["fast2"] = {
        "gguf": str(gguf), "ctx": 1024, "placement": ""}
    wired(cfgd)
    licensing.set_license_public_key(pub)
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"caps": {"instances": 1}}, "source": "file",
        "detail": "", "trial": None})
    hdrs = {"Authorization": "Bearer admin-secret"}

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/load", headers=hdrs,
                            json={"model": "fast"})
            assert r.status_code == 200, r.text
            r = client.post("/admin/load", headers=hdrs,
                            json={"model": "fast2"})
            assert r.status_code == 403, r.text
            assert r.json()["error"]["code"] == "license_caps"
    _with_fake_llama(fn)


# ── expiry grace window (#7) ───────────────────────────────────────────────

def _licensed_state(exp_ts):
    return {"mode": "licensed", "edition": "enterprise",
            "license": {"serial": "GL-GRACE0000001",
                        "exp": exp_ts},
            "source": "file", "detail": "", "trial": None}


def test_grace_window_serves_and_warns():
    """Expired INSIDE the default 72h window: serving continues (mode
    'grace'), the warning escalates to level 0, status/healthz carry
    the hours left."""
    exp = int(time.time()) - 600              # expired 10 minutes ago
    licensing.set_license_state(_licensed_state(exp))
    ok, mode = licensing.request_allowed()
    assert ok and mode == "grace"
    w = licensing.expiry_warning()
    assert w and w["level"] == 0 and w["grace"] is True
    assert 71 < w["hours_left"] <= 72
    st = licensing.status()
    assert st["grace"]["grace_hours"] == 72
    assert st["grace_hours"] == 72            # absent claim → default
    assert st["expiry_warning"] == 0
    hz = licensing.healthz_license()
    assert hz["grace_hours_left"] == w["hours_left"]


def test_grace_disabled_and_custom_window():
    """grace_hours=0 = hard stop at exp; a custom window bounds the
    soft period."""
    # disabled
    s = _licensed_state(int(time.time()) - 60)
    s["license"]["grace_hours"] = 0
    licensing.set_license_state(s)
    ok, mode = licensing.request_allowed()
    assert not ok and mode == "expired"
    assert licensing.expiry_warning() is None
    # custom 1h window: inside serves, beyond stops
    s2 = _licensed_state(int(time.time()) - 1800)     # 30 min ago
    s2["license"]["grace_hours"] = 1
    licensing.set_license_state(s2)
    ok, mode = licensing.request_allowed()
    assert ok and mode == "grace"
    s3 = _licensed_state(int(time.time()) - 7200)     # 2h ago
    s3["license"]["grace_hours"] = 1
    licensing.set_license_state(s3)
    ok, mode = licensing.request_allowed()
    assert not ok and mode == "expired"
    # pre-expiry is untouched by grace config
    s4 = _licensed_state(int(time.time()) + 5 * 86400)
    licensing.set_license_state(s4)
    ok, mode = licensing.request_allowed()
    assert ok and mode == "licensed"


# ── signed revocation list (#6) ────────────────────────────────────────────

def _mint_crl(priv, pub, revoked, version=1):
    return jwt.encode({"crl": True, "revoked": list(revoked),
                       "version": version, "iat": int(time.time())},
                      priv, algorithm="RS256",
                      headers={"kid": key_id(pub)})


def _write_crl(path, token):
    path.write_text(json.dumps({"crl": token}), encoding="utf-8")


def test_crl_revoked_at_bootstrap(tmp_path, monkeypatch):
    """A serial on the CRL never installs as licensed — community
    flips to revoked mode, enterprise boot gate refuses."""
    priv, pub = _keypair()
    crl = tmp_path / "revocations.json"
    _write_crl(crl, _mint_crl(priv, pub, ["GL-GRACE0000001"]))
    monkeypatch.setattr(licensing, "_crl_path", lambda: crl)
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    lic.write_text(json.dumps({"license": _mint(
        priv, pub, serial="GL-GRACE0000001")}), encoding="utf-8")
    st = licensing.bootstrap(edition="enterprise", license_path=lic)
    assert st["mode"] == "revoked"
    assert "revoked" in (st["detail"] or "")
    ok, mode = licensing.request_allowed()
    assert not ok and mode == "revoked"


def test_crl_runtime_flip(tmp_path, monkeypatch):
    """A CRL dropped in place revokes a RUNNING license (mtime watch
    → immediate flip, no restart)."""
    priv, pub = _keypair()
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _activate_file(lic, priv, pub)           # licensed, serial GL-…
    crl = tmp_path / "revocations.json"
    monkeypatch.setattr(licensing, "_crl_path", lambda: crl)
    assert licensing.maybe_reload_crl() is False    # no file yet
    ok, _ = licensing.request_allowed()
    assert ok
    serial = licensing.status()["serial"]
    _write_crl(crl, _mint_crl(priv, pub, [serial]))
    assert licensing.maybe_reload_crl() is True
    ok, mode = licensing.request_allowed()
    assert not ok and mode == "revoked"


def test_crl_non_revoked_and_broken_files(tmp_path, monkeypatch):
    """Other serials serve on; corrupt/unsigned CRLs are IGNORED
    (fail-open for the file — never brick serving on a bad CRL)."""
    priv, pub = _keypair()
    licensing.set_license_public_key(pub)
    lic = tmp_path / "license.json"
    _activate_file(lic, priv, pub)
    crl = tmp_path / "revocations.json"
    monkeypatch.setattr(licensing, "_crl_path", lambda: crl)

    # a DIFFERENT serial revoked — active license unaffected
    _write_crl(crl, _mint_crl(priv, pub, ["GL-SOMEONEELSE1"]))
    assert licensing.maybe_reload_crl() is True
    ok, mode = licensing.request_allowed()
    assert ok and mode == "licensed"

    # corrupt file → ignored, prior CRL stays (deleting can't un-revoke)
    crl.write_text(json.dumps({"crl": "not-a-jwt"}), encoding="utf-8")
    time.sleep(0.02)
    assert licensing.maybe_reload_crl() is False
    assert ok

    # UNSIGNED CRL (wrong key) → ignored
    priv2, pub2 = _keypair()
    _write_crl(crl, _mint_crl(priv2, pub2, [
        licensing.status()["serial"]]))
    time.sleep(0.02)
    assert licensing.maybe_reload_crl() is False
    ok, mode = licensing.request_allowed()
    assert ok and mode == "licensed"


def test_tool_revoke_and_grace_options(tmp_path, monkeypatch):
    """license_tool revoke appends + bumps the CRL version + ledgers
    it; issue --grace-hours stamps the claim."""
    import license_tool as tool
    priv, pub = _keypair()
    keyfile = tmp_path / "signing.pem"
    keyfile.write_text(priv, encoding="ascii")
    monkeypatch.setattr(tool, "KEY_PATH", keyfile)
    monkeypatch.setattr(tool, "LEDGER_PATH", tmp_path / "ledger.jsonl")
    crl = tmp_path / "revocations.json"

    rc = tool.main(["revoke", "--serial", "GL-AAA111111111",
                    "--out", str(crl)])
    assert rc == 0
    rc = tool.main(["revoke", "--serial", "GL-BBB222222222",
                    "--out", str(crl)])
    assert rc == 0
    token = json.loads(crl.read_text())["crl"]
    claims = jwt.decode(token, pub, algorithms=["RS256"])
    assert claims["revoked"] == ["GL-AAA111111111", "GL-BBB222222222"]
    assert claims["version"] == 2
    # idempotent re-revoke
    rc = tool.main(["revoke", "--serial", "GL-AAA111111111",
                    "--out", str(crl)])
    assert rc == 0
    assert jwt.decode(json.loads(crl.read_text())["crl"],
                      pub, algorithms=["RS256"])["version"] == 2

    # grace-hours stamping
    out = tmp_path / "lic.json"
    rc = tool.main(["issue", "--edition", "enterprise",
                    "--business-name", "X", "--duration", "1y",
                    "--grace-hours", "0", "--out", str(out)])
    assert rc == 0
    c1 = jwt.decode(json.loads(out.read_text())["license"],
                    pub, algorithms=["RS256"])
    assert c1["grace_hours"] == 0
    # ledger carries the revocations
    lines = (tmp_path / "ledger.jsonl").read_text().strip().splitlines()
    revoked_entries = [json.loads(l) for l in lines if "revoked" in l]
    assert len(revoked_entries) == 2
    assert revoked_entries[0]["revoked"] == "GL-AAA111111111"
