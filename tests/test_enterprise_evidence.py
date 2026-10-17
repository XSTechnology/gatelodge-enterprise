"""Evidence Center — the M3 fork tests (spec §M3).

Covers: package assembly golden path, manifest hashes, period month
selection, inventory redaction (no key material), posture facts vs a
driven fixture, controls language discipline, vault-off/-on package
shapes, the CLI verifier (clean pass + tamper fail), and the
unlicensed surface staying absent.
"""
import json
import sys
import time
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
from gateway import licensing  # noqa: E402
from gateway_enterprise.evidence import controls, package, wire  # noqa: E402
from gateway_enterprise.evidence import tools as etools  # noqa: E402

# shared fixtures (same directory)
from test_gateway_stage3 import gguf, wired, _config, _secured_config  # noqa: E401
from test_gateway_rbac import auth  # noqa: E401


def _evidence_state() -> dict:
    return {"edition": "enterprise", "mode": "licensed",
            "license": {"edition": "enterprise", "serial": "GL-EVIDTEST",
                        "business_name": "Acme Corp",
                        "features": {"evidence": True}},
            "trial": None}


@pytest.fixture()
def ev_env(wired, gguf, monkeypatch, tmp_path):
    """Wired config + fresh data dir + evidence-licensed state."""
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
    monkeypatch.setattr(gw_app, "_LOG_MIGRATED", True)
    monkeypatch.setattr(gw_app, "_LOG_MONTH_STATE", None)
    monkeypatch.setattr(gw_app, "PROJECT_ROOT", tmp_path)
    gw_app._REQ_CHAIN_HEAD = None
    licensing.set_license_state(_evidence_state())
    yield tmp_path
    gw_app._REQ_CHAIN_HEAD = None
    licensing.set_license_state(None)


def _drive(n=5):
    for i in range(n):
        gw_app._log_request(f"m{i}", "b", False, 5, {"total_tokens": 1},
                            200 if i % 4 else 400, cost_usd=0.01)


def _month() -> str:
    return time.strftime("%Y-%m")


# ── assembly + manifest ────────────────────────────────────────────────────

def test_package_golden_path(ev_env):
    _drive(6)
    m = package.build_package()
    assert m["package_id"].startswith("evidence-")
    assert m["license"]["serial"] == "GL-EVIDTEST"
    assert m["period"] == {"from": _month(), "to": _month()}
    zpath = Path(m["path"])
    assert zpath.is_file() and zpath.parent.name == "evidence"
    with zipfile.ZipFile(zpath) as z:
        names = z.namelist()
        for expected in ("README.md", "manifest.json", "controls.md",
                         "posture.md", "posture.json", "inventory.json",
                         "license/license.json",
                         "ledger/ledger-verify.json",
                         "config-audit/config-audit-verify.json",
                         f"ledger/gateway-log-{_month()}.jsonl"):
            assert expected in names, expected
        # manifest hashes match every file bit-for-bit
        man = json.loads(z.read("manifest.json"))
        import hashlib
        for name, meta in man["files"].items():
            assert hashlib.sha256(z.read(name)).hexdigest() == \
                meta["sha256"], name
        # chained + valid: the evidence license chained the driven lines
        lv = json.loads(z.read("ledger/ledger-verify.json"))
        assert lv["valid"] and lv["chained"] == 6


def test_period_month_selection(ev_env):
    """An older month inside the range is included; one outside is
    not."""
    old = gw_app._LOG_PATH.parent / "gateway-log-2020-01.jsonl"
    old.write_text('{"ts": "2020-01-01T00:00:00", "model": "x"}\n',
                   encoding="utf-8")
    (gw_app._LOG_PATH.parent /
     "gateway-rollup-2020-01.json").write_text("{}", encoding="utf-8")
    _drive(1)
    m = package.build_package(month_from="2020-01", month_to=_month())
    assert any("gateway-log-2020-01" in n for n in m["files"])
    m2 = package.build_package(month_from="2021-01", month_to="2021-02")
    assert not any("gateway-log-2020-01" in n for n in m2["files"])
    assert not any(_month() in n for n in m2["files"])


def test_bad_period_rejected(ev_env):
    with pytest.raises(RuntimeError):
        package.build_package(month_from="garbage", month_to="2026-13")


# ── inventory redaction ────────────────────────────────────────────────────

def test_inventory_no_key_material(ev_env, gguf, wired):
    cfgd = _config(gguf)
    cfgd["api_keys"] = [
        {"id": "alice", "key": "SECRET-PLAINTEXT-KEY",
         "monthly_usd_budget": 10},
    ]
    wired(cfgd)
    from gateway_enterprise.evidence.inventory import build_inventory
    inv = build_inventory(gw_app.get_gateway_config())
    raw = json.dumps(inv)
    assert "SECRET-PLAINTEXT-KEY" not in raw
    a = inv["api_keys"][0]
    assert a["id"] == "alice"
    assert a["hash8"].endswith("…") and len(a["hash8"]) == 9
    assert a["monthly_usd_budget"] == 10


# ── posture facts ──────────────────────────────────────────────────────────

def test_posture_matches_fixture(ev_env):
    _drive(6)          # 6 requests: 4× 200 (i%4 != 0), 2× 400
    m = package.build_package()
    with zipfile.ZipFile(m["path"]) as z:
        p = json.loads(z.read("posture.json"))
        per = p["period"]
        assert per["requests"] == 6
        assert per["ok"] == 4 and per["client_errors"] == 2
        assert per["models"] == 6
        assert abs(per["cost_usd"] - 0.06) < 1e-9
        assert p["request_ledger"]["chained_records"] == 6
        assert p["request_ledger"]["chain_valid"] is True
        assert p["evidence_chaining"] is True
        md = z.read("posture.md").decode("utf-8")
        assert "intact" in md and "6" in md


def test_posture_vault_off(ev_env):
    _drive(1)
    m = package.build_package()
    with zipfile.ZipFile(m["path"]) as z:
        p = json.loads(z.read("posture.json"))
        assert p["content_vault"]["enabled"] is False
        assert "metadata-only" in p["content_vault"]["note"]
        assert "vault/vault-status.json" not in z.namelist()


def test_posture_vault_on(ev_env, monkeypatch):
    class FakeWriter:
        vault_id = "t"
        def status(self):
            return {"vault_id": "t", "mode": "all", "records": 3,
                    "keys": [{"v": 1, "role": "primary"}]}
        def _parts(self):
            return []
    import gateway_enterprise.vault as vvault
    monkeypatch.setattr(vvault, "get_writer", lambda: FakeWriter())
    from gateway_enterprise.vault import index as vindex
    monkeypatch.setattr(vindex, "verify_chain",
                        lambda parts, vid: {"ok": True, "checked": 3})
    _drive(1)
    m = package.build_package()
    with zipfile.ZipFile(m["path"]) as z:
        p = json.loads(z.read("posture.json"))
        assert p["content_vault"]["enabled"] is True
        assert p["content_vault"]["chain_valid"] is True
        vs = json.loads(z.read("vault/vault-status.json"))
        assert vs["records"] == 3


# ── controls discipline ────────────────────────────────────────────────────

def test_controls_language(ev_env):
    _drive(1)
    m = package.build_package()
    with zipfile.ZipFile(m["path"]) as z:
        c = z.read("controls.md").decode("utf-8")
    assert "SOC 2 TSC" in c and "ISO 27001" in c \
        and "NIST AI RMF" in c and "EU AI Act" in c
    assert "NOT provided" in controls.CONTROLS_MD
    assert "not certify" in controls.CONTROLS_MD
    assert "No certification or attestation" in controls.CONTROLS_MD


# ── CLI verifier ───────────────────────────────────────────────────────────

def test_cli_verifies_clean_package(ev_env, capsys):
    _drive(4)
    m = package.build_package()
    rc = etools._verify(m["path"])
    out = capsys.readouterr().out
    assert rc == 0 and "OK" in out
    assert "4 chained" in out


def test_cli_fails_tampered_package(ev_env, tmp_path, monkeypatch, capsys):
    _drive(4)
    m = package.build_package()
    src = Path(m["path"])
    bad = tmp_path / "tampered.zip"
    with zipfile.ZipFile(src) as zin, \
            zipfile.ZipFile(bad, "w") as zout:
        for item in zin.namelist():
            data = zin.read(item)
            if item.startswith("ledger/gateway-log-"):
                # flip a byte inside the first chained record
                text = data.decode("utf-8")
                rec = json.loads(text.splitlines()[0])
                rec["model"] = "EVIL"
                lines = text.splitlines()
                lines[0] = json.dumps(rec)
                data = "\n".join(lines).encode("utf-8")
            zout.writestr(item, data)
    rc = etools._verify(str(bad))
    out = capsys.readouterr().out
    assert rc == 1
    assert "LEDGER HASH MISMATCH" in out


def test_cli_flags_undeclared_file(ev_env, tmp_path, capsys):
    _drive(1)
    m = package.build_package()
    bad = tmp_path / "smuggled.zip"
    with zipfile.ZipFile(m["path"]) as zin, \
            zipfile.ZipFile(bad, "w") as zout:
        for item in zin.namelist():
            zout.writestr(item, zin.read(item))
        zout.writestr("extra/undeclared.txt", b"boo")
    rc = etools._verify(str(bad))
    assert rc == 1
    assert "UNDECLARED FILE" in capsys.readouterr().out


# ── surface ────────────────────────────────────────────────────────────────

def test_surface_mounted_and_gated(ev_env, gguf, wired):
    cfgd = _secured_config(gguf, gateway_keys=("admin-secret",))
    wired(cfgd)
    assert wire(enabled=True)["installed"] is True
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/evidence/status",
                       headers=auth("admin-secret"))
        assert r.status_code == 200
        body = r.json()
        assert body["licensed"] is True
        assert "request_ledger" in body and "config_audit" in body
        # package endpoint
        r2 = client.post("/admin/evidence/package",
                         headers=auth("admin-secret"), json={})
        assert r2.status_code == 200
        assert r2.json()["package_id"].startswith("evidence-")
        # virtual keys are denied (mirror of the core admin gate)
        r3 = client.get("/admin/evidence/status")
        assert r3.status_code == 403
    # unwire: the router stays mounted for the process (add_extra_router
    # replaces on next wire) — flip to unlicensed for the next test


def test_unlicensed_surface_absent(ev_env, gguf):
    licensing.set_license_state(None)
    assert wire(enabled=False)["installed"] is False
