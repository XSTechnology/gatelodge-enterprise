"""Gateway v3 — lifecycle watchdog rules.

tick() over fabricated managers: idle tiers (inflight guard, config
pod_id clear on terminate), budget stop + grace + refuse-start gate,
death alerts (never on EXITED), never-healthy reaper, auto-redeploy
single attempt, month-to-date billing shapes + live accrual, alert
edge-triggering, endpoint 503s.
"""
import asyncio
import json
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.lifecycle as lc  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


CONFIG = {
    "port": 9911, "bind": "127.0.0.1", "default_alias": "",
    "models_dir": "/nonexistent",
    "backends": {
        "rp": {"type": "runpod", "api_key_env": "RUNPOD_API_KEY_TEST",
               "models": {
                   "big": {"image": "x", "port": 8080, "alias": "big",
                           "pod_id": "pod-live1", "start_timeout_s": 30},
               }},
    },
    "aliases": {},
}


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    def _wire(config: dict):
        cfgfile = tmp_path / "gateway_config.json"
        cfgfile.write_text(json.dumps(config), encoding="utf-8")
        cfg = gw_config.GatewayConfig(cfgfile)
        monkeypatch.setattr(gw_config, "_CONFIG", cfg)
        monkeypatch.setattr(gw_models, "_ENGINE",
                            gw_models.EmbeddedEngine(cfg))
        monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
        rp.reset_managers()
        lc.reset()
        return cfgfile, cfg

    yield _wire
    rp.reset_managers()
    rp.reset_status_cache()
    lc.reset()


def _mgr(spec: dict, **over) -> rp._PodManager:
    m = rp._PodManager("rp", "big", "RUNPOD_API_KEY_TEST", dict(spec))
    m.pod_id = "pod-x"
    m.pod_status = "RUNNING"
    m.ready = True
    m.first_seen_s = time.monotonic() - 10.0
    for k, v in over.items():
        setattr(m, k, v)
    return m


def _isolate(monkeypatch, mgrs, *, snap=None, cfg=None):
    """Point tick() at fabricated managers with no network anywhere."""
    async def _rs(c):
        return None

    async def _budget(c):
        return snap

    monkeypatch.setattr(rp, "refresh_status", _rs)
    monkeypatch.setattr(lc, "month_to_date", _budget)
    monkeypatch.setattr(rp, "managers", lambda: list(mgrs))
    return cfg if cfg is not None else types.SimpleNamespace(
        budget_usd_month=0.0)


def _record_stops(monkeypatch, m):
    stopped: list[tuple] = []

    async def stop(terminate=False):
        stopped.append(("terminate" if terminate else "stop", m.pod_id))
        m.ready = False
        if terminate:
            m.pod_id = None

    monkeypatch.setattr(m, "stop", stop)
    return stopped


# ── alert semantics (D7) ────────────────────────────────────────────────────

def test_alerts_level_vs_event_kinds():
    lc.reset()
    # level kinds: one per crossing, re-arm after resolve
    assert lc._emit("over_budget", "big", "x", backend="rp", pod_id="p")
    assert not lc._emit("over_budget", "big", "x", backend="rp", pod_id="p")
    lc._resolve("over_budget", "rp", "big", "p")
    assert lc._emit("over_budget", "big", "x", backend="rp", pod_id="p")
    # event kinds: actions, always append (naturally rate-limited)
    assert lc._emit("idle_stopped", "big", "a", backend="rp", pod_id="p")
    assert lc._emit("idle_stopped", "big", "a", backend="rp", pod_id="p")
    assert len([a for a in lc.alerts() if a["kind"] == "idle_stopped"]) == 2


# ── idle tiers (D2) ─────────────────────────────────────────────────────────

async def test_idle_stop_inflight_guard(monkeypatch):
    lc.reset()
    m = _mgr({"image": "x", "port": 8080, "alias": "big",
              "idle_stop_s": 60, "idle_terminate_s": 0})
    m.last_used = time.monotonic() - 999
    m.inflight = 1
    stopped = _record_stops(monkeypatch, m)
    cfg = _isolate(monkeypatch, [(("rp", "big"), m)])
    acted = await lc.tick(cfg)      # live request → never stop mid-flight
    assert acted["idle_stopped"] == 0 and stopped == []
    m.inflight = 0
    acted = await lc.tick(cfg)
    assert acted["idle_stopped"] == 1 and stopped == [("stop", "pod-x")]
    assert any(a["kind"] == "idle_stopped" for a in lc.alerts())


async def test_terminate_clears_config_pod_id(wired, monkeypatch):
    cfgfile, cfg = wired(CONFIG)
    m = rp.get_manager(cfg, "rp", "big")
    m.spec["idle_terminate_s"] = 60
    m.pod_id = "pod-live1"
    m.pod_status = "RUNNING"
    m.ready = True
    m.last_used = time.monotonic() - 999
    stopped = _record_stops(monkeypatch, m)
    _isolate(monkeypatch, [(("rp", "big"), m)], cfg=cfg)
    acted = await lc.tick(cfg)
    assert acted["idle_terminated"] == 1
    assert stopped == [("terminate", "pod-live1")]
    # the persisted pod_id must be GONE or a restart re-pins a dead pod
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    assert "pod_id" not in raw["backends"]["rp"]["models"]["big"]
    assert m.spec.get("pod_id") is None and m.pod_id is None


# ── budget (D1/D4) ──────────────────────────────────────────────────────────

async def test_budget_stop_grace_and_edge_trigger(monkeypatch):
    lc.reset()
    m = _mgr({"image": "x", "port": 8080, "alias": "big",
              "monthly_budget_usd": 5})
    stopped = _record_stops(monkeypatch, m)
    snap = {"total_usd": 7.5, "per_model": {"rp|big": 7.5},
            "other_usd": 0.0, "accrual_usd": 0.0}
    cfg = _isolate(monkeypatch, [(("rp", "big"), m)], snap=snap,
                   cfg=types.SimpleNamespace(budget_usd_month=0.0))
    m.inflight = 1
    acted = await lc.tick(cfg)     # over cap but busy → grace starts
    assert stopped == [] and acted["budget_stopped"] == 0
    lc._OVER_BUDGET_SINCE[("rp", "big")] = time.monotonic() - 601
    acted = await lc.tick(cfg)     # grace elapsed → force stop
    assert stopped == [("stop", "pod-x")] and acted["budget_stopped"] == 1
    kinds = [a["kind"] for a in lc.alerts()]
    assert kinds.count("over_budget") == 1     # edge-triggered, not 2×
    assert "budget_force_stop" in kinds
    # account-wide cap stops even models under their own caps
    lc.reset()
    m2 = _mgr({"image": "x", "port": 8080, "alias": "big",
               "monthly_budget_usd": 0})
    stopped2 = _record_stops(monkeypatch, m2)
    cfg = _isolate(monkeypatch, [(("rp", "big"), m2)], snap=snap,
                   cfg=types.SimpleNamespace(budget_usd_month=5.0))
    acted = await lc.tick(cfg)
    assert stopped2 == [("stop", "pod-x")] and acted["budget_stopped"] == 1


async def test_budget_allows(monkeypatch):
    lc.reset()

    async def _snap(c):
        return {"total_usd": 7.5, "per_model": {"rp|big": 7.5}}

    async def _none(c):
        return None

    monkeypatch.setattr(lc, "month_to_date", _snap)
    cfg = types.SimpleNamespace(
        budget_usd_month=10.0,
        backends={"rp": {"type": "runpod", "models": {
            "big": {"monthly_budget_usd": 5.0},
            "cheap": {"monthly_budget_usd": 0}}}})
    ok, _ = await lc.budget_allows(cfg, "rp", "cheap")
    assert ok
    ok, why = await lc.budget_allows(cfg, "rp", "big")
    assert not ok and "big" in why
    cfg.budget_usd_month = 5.0
    ok, why = await lc.budget_allows(cfg, "rp", "cheap")
    assert not ok and "account cap" in why
    # billing unavailable → fail-open (a hiccup must not brick requests)
    monkeypatch.setattr(lc, "month_to_date", _none)
    ok, _ = await lc.budget_allows(cfg, "rp", "big")
    assert ok


# ── death + auto-redeploy (D5) ──────────────────────────────────────────────

async def test_death_alert_not_on_exit(monkeypatch):
    lc.reset()
    spec = {"image": "x", "port": 8080, "alias": "big"}
    m = _mgr(spec)
    cfg = _isolate(monkeypatch, [(("rp", "big"), m)])
    await lc.tick(cfg)                          # RUNNING recorded
    m.pod_status = "TERMINATED"
    m.ready = False
    await lc.tick(cfg)
    assert [a["kind"] for a in lc.alerts()] == ["pod_died"]
    # EXITED (a stop) then TERMINATED is NOT a death
    lc.reset()
    m2 = _mgr(spec)
    _isolate(monkeypatch, [(("rp", "big"), m2)])
    await lc.tick(cfg)
    m2.pod_status = "EXITED"
    m2.ready = False
    await lc.tick(cfg)
    m2.pod_status = "TERMINATED"
    await lc.tick(cfg)
    assert lc.alerts() == []


async def test_auto_redeploy_single_attempt(monkeypatch):
    lc.reset()
    spec = {"image": "x", "port": 8080, "alias": "big",
            "auto_recover": "redeploy", "pod_id": "pod-x"}
    m = _mgr(spec)
    starts: list = []

    async def ensure_ready(timeout_s=None):
        starts.append(m.pod_id)
        raise rp.RunPodError("no capacity anywhere")

    monkeypatch.setattr(m, "ensure_ready", ensure_ready)
    cfg = _isolate(monkeypatch, [(("rp", "big"), m)])
    await lc.tick(cfg)
    m.pod_status = "TERMINATED"
    m.ready = False
    await lc.tick(cfg)
    await asyncio.sleep(0.05)      # let the background redeploy task run
    kinds = [a["kind"] for a in lc.alerts()]
    assert "pod_died" in kinds
    assert "redeploy_started" in kinds
    assert "redeploy_failed" in kinds
    assert starts == [None]        # pod_id cleared BEFORE the attempt
    assert m.pod_id is None
    await asyncio.sleep(0.05)      # no retry loop — still one attempt
    assert len(starts) == 1


# ── never-healthy reaper (D6) ───────────────────────────────────────────────

async def test_never_healthy_reaper(monkeypatch):
    lc.reset()
    spec = {"image": "x", "port": 8080, "alias": "big",
            "start_timeout_s": 60}
    m = _mgr(spec)
    m.ready = False
    m.first_seen_s = time.monotonic() - 2000    # past max(60, 1800)
    stopped = _record_stops(monkeypatch, m)
    cfg = _isolate(monkeypatch, [(("rp", "big"), m)])
    acted = await lc.tick(cfg)
    assert acted["never_healthy_stopped"] == 1 and stopped == [("stop", "pod-x")]
    assert any(a["kind"] == "never_healthy" for a in lc.alerts())
    # a warming pod (request waiting on boot) is left alone
    lc.reset()
    m2 = _mgr(spec)
    m2.ready = False
    m2.first_seen_s = time.monotonic() - 2000
    m2.warming = True
    _isolate(monkeypatch, [(("rp", "big"), m2)])
    acted = await lc.tick(cfg)
    assert acted["never_healthy_stopped"] == 0


# ── month-to-date (D1) ──────────────────────────────────────────────────────

async def test_month_to_date_shapes_and_accrual(monkeypatch):
    lc.reset()
    iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    data = {"pods": [
        {"podId": "pod-live1", "totalAmount": 1.25, "startTime": iso},
        {"podId": "pod-gone", "totalAmount": 2.0, "startTime": iso},
    ]}

    async def billing(env, days=7):
        return data

    monkeypatch.setattr(rp, "list_pod_billing", billing)
    m = _mgr({"image": "x", "port": 8080, "alias": "big",
              "est_cost_per_hr": 4.77})
    m.pod_id = "pod-live1"
    m.first_seen_s = time.monotonic() - 3600.0
    monkeypatch.setattr(rp, "managers", lambda: [(("rp", "big"), m)])
    cfg = types.SimpleNamespace(
        backends={"rp": {"type": "runpod", "api_key_env": "E",
                         "models": {}}})
    snap = await lc.month_to_date(cfg)
    # billed pods NEVER accrue on top of their records (D1)
    assert snap["total_usd"] == pytest.approx(3.25)
    assert snap["per_model"] == {"rp|big": 1.25}
    assert snap["other_usd"] == pytest.approx(2.0)
    # a running pod with NO record yet accrues from first_seen — a
    # SECOND manager (fresh deploy), so the billed pod keeps its mapping
    m2 = _mgr({"image": "x", "port": 8080, "alias": "small",
               "est_cost_per_hr": 4.77})
    m2.pod_id = "pod-fresh"
    m2.first_seen_s = time.monotonic() - 1800.0      # 0.5h × $4.77
    monkeypatch.setattr(rp, "managers",
                        lambda: [(("rp", "big"), m), (("rp", "small"), m2)])
    lc._BUDGET_STATE["snapshot"] = None
    lc._BUDGET_STATE["at"] = 0.0
    snap = await lc.month_to_date(cfg)
    assert snap["accrual_usd"] == pytest.approx(4.77 * 0.5, rel=0.01)
    assert snap["per_model"]["rp|big"] == pytest.approx(1.25)
    assert snap["per_model"]["rp|small"] == pytest.approx(4.77 * 0.5, rel=0.01)
    assert snap["total_usd"] == pytest.approx(3.25 + 4.77 * 0.5, rel=0.01)


async def test_billing_unknown_shape_and_ts(monkeypatch):
    lc.reset()

    async def billing(env, days=7):
        # alternate record-list key + unparseable timestamp + string amount
        return {"items": [{"podId": "p1", "totalAmount": "3.5"}]}

    monkeypatch.setattr(rp, "list_pod_billing", billing)
    monkeypatch.setattr(rp, "managers", lambda: [])
    cfg = types.SimpleNamespace(
        backends={"rp": {"type": "runpod", "api_key_env": "E"}})
    snap = await lc.month_to_date(cfg)
    assert snap["total_usd"] == pytest.approx(3.5)   # unknown ts COUNTS
    assert snap["other_usd"] == pytest.approx(3.5)


# ── endpoint gates (D4 at the chokepoints) ─────────────────────────────────

async def test_budget_gate_passes_when_ready(wired, monkeypatch):
    _, cfg = wired(CONFIG)
    m = rp.get_manager(cfg, "rp", "big")
    m.ready = True
    m.pod_id = "pod-live1"
    called = []

    async def no(c, backend, model):
        called.append(1)
        return False, "should not matter"

    monkeypatch.setattr(lc, "budget_allows", no)
    await gw_app._budget_gate(cfg, "rp", "big")   # running pod serves
    assert called == []


def test_budget_503_nonstream(wired, monkeypatch):
    wired(CONFIG)

    async def no(c, backend, model):
        return False, "monthly spend $9.00 reached the $5 account cap"

    monkeypatch.setattr(lc, "budget_allows", no)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "big", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 503
    body = r.json()
    assert body["error"]["code"] == "budget_exceeded"
    assert "account cap" in body["error"]["message"]


def test_budget_stream_error_event(wired, monkeypatch):
    wired(CONFIG)

    async def no(c, backend, model):
        return False, "cap hit"

    monkeypatch.setattr(lc, "budget_allows", no)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "big", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            body = "".join(line for line in r.iter_lines())
    assert "budget_exceeded" in body
    assert "[DONE]" in body


# ── admin endpoints (lc-4) ──────────────────────────────────────────────────

def test_lifecycle_endpoint(wired):
    cfgfile, _ = wired(CONFIG)
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/lifecycle")
    assert r.status_code == 200
    body = r.json()
    # watchdog off under GATEWAY_WATCHDOG=0 (conftest)
    assert body["watchdog"]["running"] is False
    assert body["budget_usd_month"] == 0
    rule = next(x for x in body["rules"] if x["model"] == "big")
    assert rule["backend"] == "rp"
    assert rule["auto_recover"] == "alert"
    assert rule["idle_stop_s"] == 0
    assert "alerts" in body and "month_to_date" in body


def test_lifecycle_settings_persist(wired):
    cfgfile, _ = wired(CONFIG)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/lifecycle/settings", json={
            "budget_usd_month": 25,
            "model_budgets": {"rp|big": 5.5},
            "model_auto_recover": {"rp|big": "redeploy"},
        })
    assert r.status_code == 200
    body = r.json()
    assert body["budget_usd_month"] == 25
    rule = next(x for x in body["rules"] if x["model"] == "big")
    assert rule["monthly_budget_usd"] == 5.5
    assert rule["auto_recover"] == "redeploy"
    # persisted to the config file (survives restart)
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    assert raw["budget_usd_month"] == 25
    spec = raw["backends"]["rp"]["models"]["big"]
    assert spec["monthly_budget_usd"] == 5.5
    assert spec["auto_recover"] == "redeploy"
    # invalid auto_recover values never reach the config
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/lifecycle/settings", json={
            "model_auto_recover": {"rp|big": "nuke"}})
    spec = json.loads(cfgfile.read_text(encoding="utf-8"))[
        "backends"]["rp"]["models"]["big"]
    assert spec["auto_recover"] == "redeploy"


def test_stop_all_panic(wired, monkeypatch):
    wired(CONFIG)
    pods = [
        {"id": "pod-run1", "name": "gw-rp-a", "status": "RUNNING"},
        {"id": "pod-run2", "name": "byo", "status": "READY"},
        {"id": "pod-stop", "name": "gw-rp-b", "status": "EXITED"},
    ]

    async def list_pods(env):
        return pods

    stopped: list = []

    async def stop_pod(pid, env):
        stopped.append(pid)

    monkeypatch.setattr(rp, "list_pods", list_pods)
    monkeypatch.setattr(rp, "stop_pod", stop_pod)
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/runpod/stop-all")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert sorted(x["pod_id"] for x in body["stopped"]) == \
        ["pod-run1", "pod-run2"]      # only running pods, BYO included
    assert stopped == ["pod-run1", "pod-run2"]
    assert any(a["kind"] == "panic_stop" for a in lc.alerts())


def test_healthz_lifecycle_section(wired, monkeypatch):
    wired(CONFIG)

    async def _rs(c):
        return []

    monkeypatch.setattr(rp, "refresh_status", _rs)
    with TestClient(gw_app.app) as client:
        r = client.get("/healthz")
    assert r.status_code == 200
    sec = r.json()["lifecycle"]
    assert sec["watchdog"] is False
    assert "budget_usd_month" in sec and "alerts" in sec


# ── our-own-termination must not read as death (v4 E2E finding) ────────────

async def test_mark_stopped_prevents_death_alert(monkeypatch):
    lc.reset()
    m = _mgr({"image": "x", "port": 8080, "alias": "big"})
    cfg = _isolate(monkeypatch, [(("rp", "big"), m)])
    await lc.tick(cfg)                       # RUNNING recorded
    lc.mark_stopped("pod-x")                 # our own terminate
    m.pod_status = "TERMINATED"              # what refresh_status sees
    m.ready = False
    await lc.tick(cfg)
    assert "pod_died" not in [a["kind"] for a in lc.alerts()]


def test_refresh_status_drops_removed_models(wired, monkeypatch):
    cfgfile, cfg = wired(CONFIG)            # big in config, pod-live1
    rp.get_manager(cfg, "rp", "big")        # manager materialized
    assert ("rp", "big") in rp._MANAGERS

    async def no_pods(env):
        return []

    monkeypatch.setattr(rp, "list_pods", no_pods)
    rp.reset_status_cache()
    # remove the model from config -> its manager must not linger
    raw = json.loads(cfgfile.read_text(encoding="utf-8"))
    raw["backends"]["rp"]["models"].pop("big", None)
    cfgfile.write_text(json.dumps(raw), encoding="utf-8")
    cfg.reload_now()
    rp.reset_status_cache()
    import asyncio
    asyncio.get_event_loop_policy()
    asyncio.run(rp.refresh_status(cfg))
    assert ("rp", "big") not in rp._MANAGERS
