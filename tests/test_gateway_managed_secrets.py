"""Console-managed provider credentials (v2.2.1) — the in-program
key store:

    POST /admin/secrets {name, value} — set (works on the next
    request, no restart; audit-logged by NAME only);
    POST /admin/secrets {name, delete: true} — remove;
    GET /admin/secrets — status per env var (env / console / keyring /
    missing) — NEVER the value;
    resolution: env var → console store → api_key_file → api_key_cmd
    → keyring (the store slots in right after env);
    RBAC: key_manager may set/remove provider credentials.
"""
import json

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.cloud as cloud
from gateway import licensing

from test_gateway_stage3 import _secured_config, gguf, wired  # noqa: F401


@pytest.fixture(autouse=True)
def _clean_store(tmp_path, monkeypatch):
    """Point the managed store at a temp file (isolation)."""
    monkeypatch.setattr(cloud, "_managed_path",
                        lambda: tmp_path / "secrets.json")
    yield
    licensing.reset_licensing()


def _cfg(gguf):
    cfgd = _secured_config(gguf)
    cfgd["backends"]["groq"] = {
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}
    return cfgd


FULL = {"Authorization": "Bearer admin-secret"}
KM = {"Authorization": "Bearer km-secret"}


def _km_config(gguf):
    cfgd = _secured_config(gguf, gateway_keys=("admin-secret", "km-secret"))
    cfgd["admin_key_roles"] = [
        {"key": "km-secret", "role": "key_manager", "note": "dev"}]
    return cfgd


def test_set_and_resolve(wired, gguf):
    """Paste a key via the API → the backend resolves it (no env, no
    restart) → GET shows source 'console' and NEVER the value."""
    wired(_cfg(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/secrets", headers=FULL,
                        json={"name": "GROQ_API_KEY", "value": "gsk-test-123"})
        assert r.status_code == 200, r.text
        assert r.json()["stored"] == "GROQ_API_KEY"

        # resolution: env unset, store hit
        backend = {"api_key_env": "GROQ_API_KEY"}
        assert cloud.resolve_backend_key(backend) == "gsk-test-123"

        # status shows 'console' — and no value anywhere
        r = client.get("/admin/secrets", headers=FULL)
        row = [x for x in r.json()["api_keys"]
               if x["name"] == "GROQ_API_KEY"][0]
        assert row["source"] == "console"
        assert "gsk-test-123" not in r.text


def test_env_beats_store(wired, gguf, monkeypatch):
    """Env var stays the deployment's authority — the store only fills
    the gap."""
    wired(_cfg(gguf))
    monkeypatch.setenv("GROQ_API_KEY", "from-env")
    with TestClient(gw_app.app) as client:
        client.post("/admin/secrets", headers=FULL,
                    json={"name": "GROQ_API_KEY", "value": "from-store"})
        r = client.get("/admin/secrets", headers=FULL)
        row = [x for x in r.json()["api_keys"]
               if x["name"] == "GROQ_API_KEY"][0]
        assert row["source"] == "env"
        assert cloud.resolve_backend_key(
            {"api_key_env": "GROQ_API_KEY"}) == "from-env"


def test_delete_and_validation(wired, gguf):
    wired(_cfg(gguf))
    with TestClient(gw_app.app) as client:
        client.post("/admin/secrets", headers=FULL,
                    json={"name": "GROQ_API_KEY", "value": "v1"})
        # overwrite
        client.post("/admin/secrets", headers=FULL,
                    json={"name": "GROQ_API_KEY", "value": "v2"})
        assert cloud.resolve_backend_key(
            {"api_key_env": "GROQ_API_KEY"}) == "v2"
        # remove
        r = client.post("/admin/secrets", headers=FULL,
                        json={"name": "GROQ_API_KEY", "delete": True})
        assert r.status_code == 200 and r.json()["existed"] is True
        assert cloud.resolve_backend_key(
            {"api_key_env": "GROQ_API_KEY"}) == ""
        # bad names refuse
        for bad in ("", "lowercase", "WITH SPACE", "INJECT;rm"):
            r = client.post("/admin/secrets", headers=FULL,
                            json={"name": bad, "value": "x"})
            assert r.status_code == 400, bad
        # empty value refuses (delete is the explicit path)
        r = client.post("/admin/secrets", headers=FULL,
                        json={"name": "GROQ_API_KEY", "value": "  "})
        assert r.status_code == 400


def test_audit_records_name_only(wired, gguf):
    """The hash-chained audit log records the mutation by NAME — the
    value never lands in the audit chain."""
    wired(_cfg(gguf))
    with TestClient(gw_app.app) as client:
        client.post("/admin/secrets", headers=FULL,
                    json={"name": "GROQ_API_KEY", "value": "gsk-secret-value"})
        r = client.get("/admin/audit", headers=FULL)
        actions = [a.get("action") for a in r.json().get("audit", [])]
        assert any("secret-set GROQ_API_KEY" == a for a in actions)
        assert "gsk-secret-value" not in r.text


def test_key_manager_role_can_set(wired, gguf):
    """RBAC: key_manager (the credential-steward role) may set/remove
    provider credentials; ops_readonly may not."""
    wired(_km_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/secrets", headers=KM,
                        json={"name": "GROQ_API_KEY", "value": "km-set"})
        assert r.status_code == 200, r.text
        r2 = client.get("/admin/secrets", headers=KM)
        assert r2.status_code == 200
