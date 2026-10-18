"""Admin-key hardening tests (v2.5.1, GL-R07).

- gateway_keys are hashed-at-rest: a 64-hex entry authenticates ONLY
  the sha256 of the presented token — presenting the stored hash
  itself (a file-reader's only move) does NOT authenticate.
- legacy plaintext entries compare constant-time and are migrated to
  hashes at startup, together with api_keys.
- the per-process internal bearer (anthropic shim self-call) resolves
  to a CONSUMER-grade principal — never admin.
"""
import hashlib
import json

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
from gateway import auth as gw_auth

# shared fixtures (same directory)
from test_gateway_stage3 import gguf, wired, _secured_config  # noqa: F401,E401


def test_stored_hash_does_not_authenticate(wired, gguf):
    """The core R07 property: reading the config file gives nothing."""
    cfgd = _secured_config(gguf)
    stored = gw_app._sha256_hex("admin-hashed-secret")
    cfgd["gateway_keys"] = [stored]
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        # the preimage authenticates
        r = client.get("/admin/stats", headers={
            "Authorization": "Bearer admin-hashed-secret"})
        assert r.status_code == 200
        # the stored hash itself does NOT
        r2 = client.get("/admin/stats", headers={
            "Authorization": "Bearer " + stored})
        assert r2.status_code == 401


def test_legacy_plaintext_still_authenticates(wired, gguf):
    """Pre-migration plaintext entries keep working (constant-time)."""
    cfgd = _secured_config(gguf)
    cfgd["gateway_keys"] = ["admin-legacy-plaintext"]
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/stats", headers={
            "Authorization": "Bearer admin-legacy-plaintext"})
        assert r.status_code == 200


def test_64_char_non_hex_plaintext_is_not_treated_as_hash(wired, gguf):
    """Only 64-char LOWERCASE-HEX entries are hashes; a 64-char
    plaintext of another shape still literal-matches."""
    cfgd = _secured_config(gguf)
    odd = "zz" * 31 + "!"           # 64 chars, not hex
    cfgd["gateway_keys"] = [odd]
    wired(cfgd)
    cfg = gw_config.GatewayConfig(None)  # not used; direct resolver check
    from gateway.auth import _resolve_bearer
    from gateway.config import get_gateway_config
    assert _resolve_bearer(get_gateway_config(), odd) == "admin"


def test_gateway_keys_migrated_to_hashes(wired, gguf):
    """Startup migration rewrites plaintext gateway_keys (and api_keys)
    in the config FILE to sha256; the original plaintext still
    authenticates; the file no longer carries it."""
    cfgd = _secured_config(gguf)
    cfgfile, _, _ = wired(cfgd)
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    raw["gateway_keys"] = ["admin-migrate-me"]
    raw["api_keys"] = [{"id": "old", "key": "vk-old-plain"}]
    cfgfile.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    gw_app._migrate_plaintext_keys()
    migrated = json.loads(cfgfile.read_text(encoding="utf-8"))
    assert migrated["gateway_keys"] == \
        [hashlib.sha256(b"admin-migrate-me").hexdigest()]
    assert "key" not in migrated["api_keys"][0]
    # idempotent
    gw_app._migrate_plaintext_keys()
    assert json.loads(cfgfile.read_text(encoding="utf-8")) == migrated
    # the ORIGINAL plaintext still authenticates post-migration
    from gateway.auth import _resolve_bearer
    from gateway.config import get_gateway_config
    cfg = get_gateway_config()
    assert _resolve_bearer(cfg, "admin-migrate-me") == "admin"
    assert _resolve_bearer(cfg, "vk-old-plain") == "old"


def test_internal_bearer_is_consumer_not_admin(wired, gguf):
    """The shim's per-process bearer resolves to 'internal' — serving
    surface yes, admin surface NEVER."""
    cfgd = _secured_config(gguf)
    cfgd["gateway_keys"] = ["admin-secret"]
    wired(cfgd)
    from gateway.auth import _INTERNAL_BEARER, _resolve_bearer
    from gateway.config import get_gateway_config
    cfg = get_gateway_config()
    assert _resolve_bearer(cfg, _INTERNAL_BEARER) == "internal"
    assert _resolve_bearer(cfg, _INTERNAL_BEARER) != "admin"
    # and the admin gate denies it over HTTP
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/stats", headers={
            "Authorization": "Bearer " + _INTERNAL_BEARER})
        assert r.status_code == 403


def test_internal_bearer_never_in_config_or_file(wired, gguf):
    """The internal bearer is random per process and never persisted."""
    cfgd = _secured_config(gguf)
    cfgfile, _, _ = wired(cfgd)
    assert gw_auth._INTERNAL_BEARER not in cfgfile.read_text(
        encoding="utf-8")
    assert len(gw_auth._INTERNAL_BEARER) == 64


def test_api_key_cmd_runs_argv_not_shell(monkeypatch, tmp_path):
    """GL-R04: a plain command executes as an ARGV list (no shell); a
    command with shell metacharacters falls back to shell=True with a
    warning (legacy pipe fetchers keep working)."""
    import gateway.cloud as cloud

    calls = []

    def fake_run(cmd, shell=False, **kw):
        calls.append((cmd, shell))
        class _R:
            stdout = "sk-via-cmd\n"
        return _R()

    import subprocess
    monkeypatch.setattr(subprocess, "run", fake_run)

    # plain command → argv (shell=False)
    out = cloud.resolve_backend_key({"api_key_env": "",
                                     "api_key_cmd": "vault kv get -f x"})
    assert out == "sk-via-cmd"
    assert calls[-1][1] is False
    assert isinstance(calls[-1][0], list)

    # metacharacters → legacy shell path
    out2 = cloud.resolve_backend_key({"api_key_env": "",
                                      "api_key_cmd": "pass show x | head -1"})
    assert out2 == "sk-via-cmd"
    assert calls[-1][1] is True
    assert isinstance(calls[-1][0], str)
