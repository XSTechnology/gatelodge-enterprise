"""Hash-chained audit tests (v1.4: tamper-evident config-audit.jsonl).

Both audit writers — config mutations (_audit_config) and RBAC denials
(auth._audit_deny) — route through gateway.app.append_audit_record:
one chain, per-file verification, SOC2 CC7.1 tamper evidence.
"""
import json

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway.auth import (AuthPipeline, admin_gate_link, rate_limit_link,
                          rbac_link, resolve_key_link, set_auth_pipeline)

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config,  # noqa: F401
                                 _secured_config)
from test_gateway_rbac import auth, OPS  # noqa: F401

AUDIT_FILE = "config-audit.jsonl"


@pytest.fixture()
def audit_env(wired, gguf, monkeypatch, tmp_path):
    """Wired config + fresh audit file + reset chain head. Yields the
    audit path."""
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
    gw_app._AUDIT_CHAIN_HEAD = None
    yield tmp_path / AUDIT_FILE
    gw_app._AUDIT_CHAIN_HEAD = None


def _lines(path):
    return [json.loads(l) for l in
            path.read_text(encoding="utf-8").strip().splitlines()]


def test_mutation_and_denial_share_one_chain(wired, gguf, audit_env):
    """A config mutation and an RBAC denial both append CHAINED records
    to the SAME file — one head-to-tail chain across both writers (the
    boot-time key-hash migration may also audit; every consecutive pair
    must link, which the verifier proves). Note: the denial must come
    from rbac_link (a role-scoped ADMIN key) — virtual keys are denied
    by the core admin gate, which doesn't audit."""
    cfgd = _secured_config(gguf, gateway_keys=("admin-secret", "ops-secret"))
    cfgd["admin_key_roles"] = [OPS]
    wired(cfgd)
    with TestClient(gw_app.app) as client:
        # mutation → chained record
        client.post("/admin/aliases", headers=auth("admin-secret"),
                    json={"name": "chain-probe", "chain": ["local:fast"]})
        # rbac denial → chained record on the SAME chain
        client.get("/admin/config", headers=auth("ops-secret"))  # 403
    recs = _lines(audit_env)
    actions = [r["action"] for r in recs]
    assert any(a.startswith("alias-upsert") for a in actions)
    assert any(a.startswith("rbac-deny") for a in actions)
    # every record is chained, and consecutive records link head-to-tail
    assert all(r.get("hash") for r in recs)
    for prev_rec, rec in zip(recs, recs[1:]):
        assert rec["prev"] == prev_rec["hash"]
    assert recs[0]["prev"] == gw_app._AUDIT_GENESIS
    assert gw_app.verify_audit_chain()["valid"] is True


def test_verifier_valid_on_fresh_chain(wired, gguf, audit_env):
    """verify_audit_chain reports valid on an untouched chain, and
    GET /admin/audit surfaces it."""
    wired(_secured_config(gguf))
    with TestClient(gw_app.app) as client:
        # an audited mutation (POST /admin/reload is NOT audited)
        client.post("/admin/aliases", headers=auth("admin-secret"),
                    json={"name": "verify-probe", "chain": ["local:fast"]})
        body = client.get("/admin/audit", headers=auth("admin-secret")).json()
    assert body["chain"]["valid"] is True
    assert body["chain"]["chained"] >= 1
    assert body["chain"]["breaks"] == []


def test_verifier_detects_tampering(wired, gguf, audit_env):
    """Modifying a record's content breaks its hash; deleting an
    interior record breaks prev linkage — both reported."""
    wired(_secured_config(gguf))
    for i in range(3):
        gw_app._audit_config(f"probe-{i}")
    recs = _lines(audit_env)
    assert len(recs) == 3

    # tamper 1: rewrite record 2's action in place
    raw = audit_env.read_text(encoding="utf-8").splitlines()
    tampered = json.loads(raw[1])
    tampered["action"] = "FALSIFIED"
    raw[1] = json.dumps(tampered)
    audit_env.write_text("\n".join(raw) + "\n", encoding="utf-8")
    v = gw_app.verify_audit_chain()
    assert v["valid"] is False
    reasons = {b["line"]: b["reason"] for b in v["breaks"]}
    assert reasons[2] == "hash mismatch (tampered)"

    # tamper 2: delete an interior line → prev mismatch downstream
    raw = _lines(audit_env)
    raw = [json.dumps(r) for r in raw[:1] + raw[2:]]   # drop line 2
    audit_env.write_text("\n".join(raw) + "\n", encoding="utf-8")
    v2 = gw_app.verify_audit_chain()
    assert v2["valid"] is False
    assert any("prev mismatch" in b["reason"] for b in v2["breaks"])


def test_verifier_legacy_prefix(audit_env, wired, gguf):
    """Pre-v1.4 (hash-less) records count as legacy; the chain restarts
    at genesis after them and the file verifies valid."""
    audit_env.write_text(
        json.dumps({"ts": "2026-01-01T00:00:00", "key": "admin",
                    "action": "legacy-record"}) + "\n",
        encoding="utf-8")
    wired(_secured_config(gguf))
    gw_app._audit_config("new-style")
    v = gw_app.verify_audit_chain()
    assert v["legacy"] == 1
    assert v["chained"] == 1
    assert v["valid"] is True


def test_chain_continues_across_restart(audit_env, wired, gguf):
    """A fresh process (chain head unknown) continues the chain from the
    file tail — prev links across the restart."""
    wired(_secured_config(gguf))
    gw_app._audit_config("before-restart")
    head_before = gw_app._AUDIT_CHAIN_HEAD
    gw_app._AUDIT_CHAIN_HEAD = None          # simulate a process restart
    gw_app._audit_config("after-restart")
    recs = _lines(audit_env)
    assert recs[1]["prev"] == recs[0]["hash"] == head_before
    assert gw_app.verify_audit_chain()["valid"] is True


def test_unparseable_line_reported(audit_env, wired, gguf):
    wired(_secured_config(gguf))
    gw_app._audit_config("ok")
    with open(audit_env, "a", encoding="utf-8") as f:
        f.write("{not json\n")
    v = gw_app.verify_audit_chain()
    assert v["valid"] is False
    assert any(b["reason"] == "unparseable" for b in v["breaks"])
