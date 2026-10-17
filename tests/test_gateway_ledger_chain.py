"""Request-ledger chaining tests (v2.5 Evidence Center, 'evidence').

Under the evidence license claim every request line chains (prev/hash,
canonical sha256 — the config-audit pattern); anything else writes
today's plain line. Mixed files are legal: the verifier skips plain
records transparently (prev preserved) and still detects tampering and
downgrades. Chain continues across restarts and month files.
"""
import json
import time

import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway import licensing

# shared fixtures (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import gguf, wired, _config  # noqa: F401
from test_gateway_rbac import auth  # noqa: F401


def _evidence_state(on: bool = True) -> dict:
    return {"edition": "enterprise", "mode": "licensed",
            "license": {"edition": "enterprise", "serial": "GL-TEST",
                        "features": {"evidence": on} if on else {}},
            "trial": None}


@pytest.fixture()
def ledger_env(wired, gguf, monkeypatch, tmp_path):
    """Wired config + fresh log dir + clean licensing/chain state."""
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
    monkeypatch.setattr(gw_app, "_LOG_MIGRATED", True)
    monkeypatch.setattr(gw_app, "_LOG_MONTH_STATE", None)
    gw_app._REQ_CHAIN_HEAD = None
    licensing.set_license_state(None)
    yield tmp_path
    gw_app._REQ_CHAIN_HEAD = None
    licensing.set_license_state(None)


def _write(n=1, **kw):
    for i in range(n):
        gw_app._log_request(kw.get("model", f"m{i}"),
                            kw.get("backend", "b"),
                            False, 5, {"total_tokens": 1},
                            kw.get("status", 200))


def _lines(path):
    return [json.loads(l) for l in
            path.read_text(encoding="utf-8").strip().splitlines()]


def _month_path(env=None):
    return gw_app._LOG_PATH.parent / f"gateway-log-{time.strftime('%Y-%m')}.jsonl"


# ── gating ────────────────────────────────────────────────────────────────

def test_unmanaged_writes_plain(ledger_env):
    """No licensing state (source/tests): today's line, unchanged."""
    _write(2)
    recs = _lines(_month_path(ledger_env))
    assert len(recs) == 2
    assert all("hash" not in r and "prev" not in r for r in recs)
    v = gw_app.verify_request_chain()
    assert v["valid"] and v["chained"] == 0 and v["plain"] == 2


def test_trial_and_community_write_plain(ledger_env):
    """Trial = full evaluation of COMMUNITY — allows() is licensed-only,
    so trial and a featureless community license both write plain."""
    licensing.set_license_state({"edition": "community", "mode": "trial",
                                 "license": None, "trial": {"days_left": 9}})
    _write(1)
    licensing.set_license_state({"edition": "community", "mode": "licensed",
                                 "license": {"edition": "community",
                                             "features": {}},
                                 "trial": None})
    _write(1)
    recs = _lines(_month_path(ledger_env))
    assert all("hash" not in r for r in recs)
    v = gw_app.verify_request_chain()
    assert v["valid"] and v["chained"] == 0 and v["plain"] == 2


def test_evidence_claim_chains(ledger_env):
    """Enterprise license carrying evidence → every line chains."""
    licensing.set_license_state(_evidence_state())
    _write(3)
    recs = _lines(_month_path(ledger_env))
    assert all(r.get("hash") for r in recs)
    # head-to-tail linkage
    assert recs[0]["prev"] == "genesis"
    for a, b in zip(recs, recs[1:]):
        assert b["prev"] == a["hash"]
    v = gw_app.verify_request_chain()
    assert v["valid"] and v["chained"] == 3 and v["plain"] == 0


def test_evidence_n_writes_plain(ledger_env):
    """Enterprise license with evidence=N: plain (claim granularity)."""
    licensing.set_license_state(_evidence_state(on=False))
    _write(1)
    recs = _lines(_month_path(ledger_env))
    assert "hash" not in recs[0]


# ── continuity ────────────────────────────────────────────────────────────

def test_chain_continues_across_restart(ledger_env):
    """_REQ_CHAIN_HEAD=None simulates a cold start: the tail-read
    resumes the chain — record 4 links to record 3."""
    licensing.set_license_state(_evidence_state())
    _write(3)
    gw_app._REQ_CHAIN_HEAD = None            # restart
    _write(2)
    recs = _lines(_month_path(ledger_env))
    assert len(recs) == 5
    for a, b in zip(recs, recs[1:]):
        assert b["prev"] == a["hash"]
    assert gw_app.verify_request_chain()["valid"]


def test_chain_continues_across_month_files(ledger_env):
    """A chained record in an OLDER month file + empty current month:
    the walk-back continues the chain into the new file."""
    import datetime as dt
    prev_month = (dt.date.today().replace(day=1) - dt.timedelta(days=1))
    old = gw_app._LOG_PATH.parent / f"gateway-log-{prev_month:%Y-%m}.jsonl"
    # fabricate one chained record via the machinery itself
    licensing.set_license_state(_evidence_state())
    gw_app._REQ_CHAIN_HEAD = "genesis"
    rec = {"ts": "2000-01-01T00:00:00", "model": "old", "backend": "b",
           "stream": False, "ms": 1, "usage": {}, "status": 200}
    rec["prev"] = "genesis"
    rec["hash"] = gw_app._req_record_hash(rec)
    old.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    # rollup guard: closed month gets a rollup so nothing re-processes
    (gw_app._LOG_PATH.parent /
     f"gateway-rollup-{prev_month:%Y-%m}.json").write_text("{}", encoding="utf-8")
    # cold start into the CURRENT month
    gw_app._REQ_CHAIN_HEAD = None
    _write(1)
    recs = _lines(_month_path(ledger_env))
    assert recs[0]["prev"] == rec["hash"]    # links across the boundary
    v = gw_app.verify_request_chain()
    assert v["valid"] and v["chained"] == 2
    assert [f["file"] for f in v["files"]] == [old.name, recs and
                                               _month_path(ledger_env).name]


def test_tail_walk_back_through_plain_stretch(ledger_env):
    """Newest month file all-plain (community swap) → the head resolves
    from the older month's last CHAINED record."""
    import datetime as dt
    prev_month = (dt.date.today().replace(day=1) - dt.timedelta(days=1))
    old = gw_app._LOG_PATH.parent / f"gateway-log-{prev_month:%Y-%m}.jsonl"
    licensing.set_license_state(_evidence_state())
    rec = {"ts": "2000-01-01T00:00:00", "model": "x", "backend": "b",
           "stream": False, "ms": 1, "usage": {}, "status": 200,
           "prev": "genesis"}
    rec["hash"] = gw_app._req_record_hash(rec)
    old.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    (gw_app._LOG_PATH.parent /
     f"gateway-rollup-{prev_month:%Y-%m}.json").write_text("{}", encoding="utf-8")
    # current month: plain only
    licensing.set_license_state(None)
    _write(1)
    assert gw_app._req_chain_tail_hash() == rec["hash"]


# ── mixed records + tamper evidence ───────────────────────────────────────

def test_interleaved_plain_and_chained(ledger_env):
    """License swap mid-month: chained → plain → chained in one file.
    The verifier skips plain transparently (prev preserved) and the
    result is VALID — the writer kept its head across the plain line."""
    licensing.set_license_state(_evidence_state())
    _write(2)
    licensing.set_license_state(None)        # evidence off
    _write(1)
    licensing.set_license_state(_evidence_state())
    _write(2)
    recs = _lines(_month_path(ledger_env))
    assert len(recs) == 5
    chained = [r for r in recs if r.get("hash")]
    plain = [r for r in recs if not r.get("hash")]
    assert len(chained) == 4 and len(plain) == 1
    # the plain record did NOT reset the head: record 4 links to 2
    assert chained[2]["prev"] == chained[1]["hash"]
    v = gw_app.verify_request_chain()
    assert v["valid"] and v["chained"] == 4 and v["plain"] == 1


def test_tampered_record_breaks(ledger_env):
    """Editing a chained record's content → hash mismatch."""
    licensing.set_license_state(_evidence_state())
    _write(3)
    p = _month_path(ledger_env)
    lines = p.read_text(encoding="utf-8").splitlines()
    rec = json.loads(lines[1])
    rec["model"] = "TAMPERED"
    lines[1] = json.dumps(rec)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    v = gw_app.verify_request_chain()
    assert not v["valid"]
    assert v["breaks"][0]["reason"].startswith("hash mismatch")


def test_downgrade_to_plain_breaks(ledger_env):
    """Stripping a chained record's hash (downgrade attack) → the NEXT
    chained record's prev references a hash the walk can't reach."""
    licensing.set_license_state(_evidence_state())
    _write(3)
    p = _month_path(ledger_env)
    lines = p.read_text(encoding="utf-8").splitlines()
    rec = json.loads(lines[1])
    rec.pop("hash")
    rec.pop("prev")
    lines[1] = json.dumps(rec)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    v = gw_app.verify_request_chain()
    assert not v["valid"]
    assert v["breaks"][0]["reason"].startswith("chain break")
    assert v["plain"] == 1                  # the downgraded record


def test_deletion_breaks(ledger_env):
    """Removing a chained record mid-file → prev mismatch downstream."""
    licensing.set_license_state(_evidence_state())
    _write(3)
    p = _month_path(ledger_env)
    lines = p.read_text(encoding="utf-8").splitlines()
    del lines[1]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    v = gw_app.verify_request_chain()
    assert not v["valid"]
    assert v["breaks"][0]["reason"].startswith("chain break")


# ── surface + cost ────────────────────────────────────────────────────────

def test_admin_ledger_endpoint(ledger_env, gguf, wired):
    """/admin/ledger returns the walk (admin-gated like /admin/audit)."""
    from test_gateway_stage3 import _secured_config
    cfgd = _secured_config(gguf, gateway_keys=("admin-secret",))
    wired(cfgd)
    licensing.set_license_state(_evidence_state())
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/ledger", headers=auth("admin-secret"))
        assert r.status_code == 200
        body = r.json()["ledger"]
        assert body["valid"] and "files" in body and "chained" in body


def test_chaining_cost_is_negligible(ledger_env):
    """Chaining adds one dict copy + one sha256 per request inside the
    existing lock — measured against the plain path on the same
    machine/file, not an absolute bound."""
    licensing.set_license_state(None)
    _write(200)                                # warm the file
    t0 = time.perf_counter()
    _write(2000)
    plain_dt = time.perf_counter() - t0
    licensing.set_license_state(_evidence_state())
    gw_app._REQ_CHAIN_HEAD = None
    t0 = time.perf_counter()
    _write(2000)
    chain_dt = time.perf_counter() - t0
    assert chain_dt < plain_dt * 1.5 + 0.05, \
        f"plain {plain_dt:.2f}s vs chained {chain_dt:.2f}s"
    v = gw_app.verify_request_chain()
    assert v["valid"] and v["chained"] == 2000 and v["plain"] >= 2200
