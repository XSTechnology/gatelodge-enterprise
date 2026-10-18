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
        "https://x/m.gguf", "", "/models", 1, 8192,
        sha256="ab" * 32)
    assert "sha256sum -c" in s
    assert ("ab" * 32) in s
    # without a digest: no verification step (nothing to check)
    s2 = catalog._custom_script("https://x/m.gguf", "", "/models",
                                1, 8192)
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
