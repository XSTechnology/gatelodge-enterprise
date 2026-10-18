"""Gateway v3 — pod lifecycle watchdog.

The gateway owns the pod lifecycle after the green banner: idle
auto-stop, monthly budget caps, unexpected-death detection, never-
healthy reaping, and a console-visible alert log. One 30s tick over
runpod.refresh_status — started at app startup, NOT lazily from
request paths (no traffic must never mean no protection: a forgotten
pod bills forever).

Design (plans/2026-09-20_17-18-45_gateway_v3_lifecycle):
- D1 spend: RunPod billing is authoritative month-to-date; records are
  DAY-granular, so pods with no record yet in the window accrue live
  from the spec's est_cost_per_hr (wizard-persisted; catalog-price
  fallback). Already-billed pods rely on their records — accrual on
  top would double-count (cadence verified in the v3 live smoke).
- D4 budget: enforced by the watchdog (stop over-cap pods; force after
  a 10-min grace past inflight==0) AND by app.py chokepoints refusing
  to START pods (503 budget_exceeded, running pods keep serving).
- D5 death: TERMINATED while believed running → alert; auto-redeploy
  is per-model opt-in, ONE attempt (capacity can be dry for hours).
- D7 alerts: LEVEL kinds (over_budget, billing_fetch_failed) are
  edge-triggered via a key set; EVENT kinds are actions that are
  naturally rate-limited and always append.
- D8 this module imports runpod one-way; runpod knows nothing of it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from . import catalog, runpod
from .config import get_gateway_config

logger = logging.getLogger("gateway.lifecycle")

WATCHDOG_INTERVAL_S = 30.0
BUDGET_CACHE_TTL_S = 60.0
BUDGET_STOP_GRACE_S = 600.0      # over cap + busy → force-stop window
NEVER_HEALTHY_MIN_S = 1800.0     # floor for the never-healthy budget


class LifecycleBudgetExceeded(RuntimeError):
    """Starting this pod would exceed a monthly budget cap — app.py
    chokepoints map this to 503 budget_exceeded (D4 refuse-start)."""


# ── Alerts (D7) ─────────────────────────────────────────────────────────────

_ALERTS: deque[dict[str, Any]] = deque(maxlen=200)
_ALERT_KEYS: set[tuple] = set()
# level conditions suppress repeats until _resolve; event kinds don't
_LEVEL_KINDS = {"over_budget", "billing_fetch_failed"}


def alerts(limit: int = 50) -> list[dict[str, Any]]:
    """Most recent alerts, oldest first."""
    return list(_ALERTS)[-limit:]


def clear_alerts() -> None:
    _ALERTS.clear()
    _ALERT_KEYS.clear()


def reset() -> None:
    """Test helper — alerts + rule memory + budget cache."""
    clear_alerts()
    _LAST_POD_STATE.clear()
    _OVER_BUDGET_SINCE.clear()
    _BUDGET_STATE["snapshot"] = None
    _BUDGET_STATE["at"] = 0.0


def _emit(kind: str, model: str, message: str, *,
          severity: str = "warn", pod_id: str | None = None,
          backend: str = "") -> bool:
    """Append an alert; False when a level-kind alert is suppressed."""
    if kind in _LEVEL_KINDS:
        key = (kind, backend, model, pod_id)
        if key in _ALERT_KEYS:
            return False
        _ALERT_KEYS.add(key)
    _ALERTS.append({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "kind": kind, "backend": backend, "model": model,
        "pod_id": pod_id, "severity": severity, "message": message,
    })
    logger.info("lifecycle [%s] %s/%s: %s", kind, backend, model, message)
    return True


def _resolve(kind: str, backend: str, model: str,
             pod_id: str | None) -> None:
    """Level condition left — a future crossing may re-alert."""
    _ALERT_KEYS.discard((kind, backend, model, pod_id))


# ── Budget (D1) ─────────────────────────────────────────────────────────────

_BUDGET_STATE: dict[str, Any] = {"at": 0.0, "snapshot": None}


def _month_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _parse_ts(v: Any) -> datetime | None:
    """RunPod startTime is ISO-ish or epoch — parse defensively. None
    means unknown; such records COUNT (a cap that skips unreadable
    spend is no cap)."""
    if isinstance(v, (int, float)):
        n = float(v)
        if n > 1e12:        # ms epoch
            n /= 1000.0
        try:
            return datetime.fromtimestamp(n, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(v, str) and v:
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:       # naive ISO → UTC
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None
    return None


def _billing_records(data: Any) -> list[dict[str, Any]]:
    """Every observed /billing/pods shape → record list."""
    if not isinstance(data, dict):
        return []
    for key in ("pods", "billing", "items", "records"):
        v = data.get(key)
        if isinstance(v, list):
            return [r for r in v if isinstance(r, dict)]
    return []


async def _fetch_billing(cfg) -> dict[str, Any] | None:
    backends = [b for b in cfg.backends.values()
                if b.get("type") == "runpod"]
    if not backends:
        return None
    env = backends[0].get("api_key_env") or "RUNPOD_API_KEY"
    days = (datetime.now(timezone.utc) - _month_start_utc()).days + 1
    try:
        return await runpod.list_pod_billing(env, days=days)
    except Exception:  # noqa: BLE001 — budget checks never raise
        logger.warning("lifecycle billing fetch failed", exc_info=True)
        return None


def _pod_model_map() -> dict[str, tuple[str, str]]:
    """podId → (backend, model) from materialized managers — includes
    TERMINATED pods the gateway still knows about."""
    out: dict[str, tuple[str, str]] = {}
    for (b, k), m in runpod.managers():
        if m.pod_id:
            out[str(m.pod_id)] = (b, k)
    return out


async def _est_rate(m) -> float:
    """$/hr for a running pod: the wizard-persisted est_cost_per_hr,
    else catalog price × gpu_count (legacy specs)."""
    rate = float(m.spec.get("est_cost_per_hr") or 0)
    if rate > 0:
        return rate
    ids = m.spec.get("gpu_type_ids") or []
    if not ids:
        return 0.0
    try:
        opts = await catalog.live_options(m.api_key_env)
        for g in opts.get("gpus", []):
            if g.get("id") == ids[0]:
                price = (g.get("price") or {}).get(
                    "community" if m.spec.get("spot") else "secure")
                if price:
                    return float(price) * max(
                        1, int(m.spec.get("gpu_count") or 1))
    except Exception:  # noqa: BLE001 — price is best-effort
        pass
    return 0.0


async def month_to_date(cfg) -> dict[str, Any] | None:
    """Month-to-date spend (UTC month), cached BUDGET_CACHE_TTL_S:
    {total_usd, per_model: {"backend|model": usd}, other_usd,
     accrual_usd, as_of}. None when billing was never fetched."""
    now = time.monotonic()
    snap = _BUDGET_STATE["snapshot"]
    if snap is not None and now - _BUDGET_STATE["at"] < BUDGET_CACHE_TTL_S:
        return snap
    data = await _fetch_billing(cfg)
    if data is None:
        _emit("billing_fetch_failed", "",
              "RunPod billing unavailable — budget checks fail open "
              "until it recovers", severity="warn")
        return snap  # stale beats nothing (None only before first fetch)
    _resolve("billing_fetch_failed", "", "", None)
    month_start = _month_start_utc()
    mapping = _pod_model_map()
    total = 0.0
    per_model: dict[str, float] = {}
    other = 0.0
    billed: set[str] = set()
    for r in _billing_records(data):
        ts = _parse_ts(r.get("startTime"))
        if ts is not None and ts < month_start:
            continue
        try:
            amt = float(r.get("totalAmount") or 0)
        except (TypeError, ValueError):
            amt = 0.0
        total += amt
        pid = str(r.get("podId") or "")
        if pid:
            billed.add(pid)
        if pid in mapping:
            mk = f"{mapping[pid][0]}|{mapping[pid][1]}"
            per_model[mk] = per_model.get(mk, 0.0) + amt
        else:
            other += amt
    # Live accrual (D1): day-granular records lag — accrue running pods
    # since first_seen ONLY when no record exists for them in the
    # window (fresh deploys, the runaway-spend risk). Pods with records
    # rely on billing; accruing on top would double-count.
    accrual = 0.0
    for (b, k), m in runpod.managers():
        if not m.is_ready() or not m.first_seen_s:
            continue
        if m.pod_id and str(m.pod_id) in billed:
            continue
        rate = await _est_rate(m)
        if rate > 0:
            hrs = max(0.0, (now - m.first_seen_s) / 3600.0)
            mk = f"{b}|{k}"
            per_model[mk] = per_model.get(mk, 0.0) + rate * hrs
            accrual += rate * hrs
            total += rate * hrs
    snap = {"total_usd": round(total, 4),
            "per_model": {k: round(v, 4) for k, v in per_model.items()},
            "other_usd": round(other, 4),
            "accrual_usd": round(accrual, 4),
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%S")}
    _BUDGET_STATE["snapshot"] = snap
    _BUDGET_STATE["at"] = now
    return snap


async def budget_allows(cfg, backend: str,
                        model: str) -> tuple[bool, str]:
    """D4 refuse-start gate. True when starting this pod is allowed.

    v2.6 (GL-R13): billing-outage policy for HARD budgets. Default
    fail-open stays (a billing hiccup must not brick every request —
    it's alerted separately). With budget.billing_stale_after_s > 0
    (default 900), a hard cap + a billing snapshot older than the
    threshold DENIES NEW pod starts — the operator's cap stays
    enforceable when the spend signal is lost. Existing pods continue
    (stopping them doesn't recover the check either); accrual from
    running pods still counts in the cached snapshot."""
    cap_global = float(getattr(cfg, "budget_usd_month", 0) or 0)
    backends = getattr(cfg, "backends", None) or {}
    spec = ((backends.get(backend) or {}).get("models") or {}).get(model) or {}
    cap_model = float(spec.get("monthly_budget_usd") or 0)
    if cap_global <= 0 and cap_model <= 0:
        return True, ""
    # billing-outage policy knob (GL-R13): default 900s; 0 = off
    try:
        _raw = cfg.store.load() if getattr(cfg, "store", None) else {}
        budget_cfg = (_raw.get("budget") if isinstance(_raw, dict)
                      else {}) or {}
        _raw_s = budget_cfg.get("billing_stale_after_s")
        stale_after = (900.0 if _raw_s is None
                       else max(0.0, float(_raw_s)))
    except Exception:                        # noqa: BLE001
        stale_after = 900.0
    snap = await month_to_date(cfg)   # attempts refresh; updates the
    # fetch clock on success — _BUDGET_STATE["at"] measures time since
    # the last SUCCESSFUL billing fetch
    if stale_after > 0:
        age = time.monotonic() - _BUDGET_STATE["at"]
        if age > stale_after:
            return False, (f"billing data is {age / 60:.0f} min stale "
                           f"(> {stale_after / 60:g} min) — new pod "
                           "starts are denied until the spend signal "
                           "recovers (budget.billing_stale_after_s)")
    if snap is None:
        return True, ""
    if cap_global > 0 and snap["total_usd"] >= cap_global:
        return False, (f"monthly spend ${snap['total_usd']:.2f} reached the "
                       f"${cap_global:g} account cap — raise it in the "
                       "console or wait for the month to roll over")
    if cap_model > 0:
        spent = float(snap["per_model"].get(f"{backend}|{model}", 0.0))
        if spent >= cap_model:
            return False, (f"model {model} spent ${spent:.2f} of its "
                           f"${cap_model:g} monthly cap")
    return True, ""


# ── Actions (D5/D6) ─────────────────────────────────────────────────────────

def _clear_pod_id(m) -> None:
    """Drop pod_id from config + the manager's spec. A persisted
    pod_id re-pins a dead pod (refresh_status re-seeds from the spec)
    — after OUR terminate/redeploy the model must go back to find-or-
    -deploy. m.pod_id itself is left alone: _act_stop's stop() still
    needs it (and nulls it on terminate); _redeploy resets it."""
    try:
        cfg = get_gateway_config()
        if cfg.path.is_file():
            raw = json.loads(cfg.path.read_text(encoding="utf-8"))
            spec = (raw.get("backends", {}).get(m.backend_name, {})
                    .get("models", {}).get(m.model_key))
            if isinstance(spec, dict) and spec.get("pod_id"):
                spec.pop("pod_id", None)
                from .config import atomic_write_json
                atomic_write_json(cfg.path, raw)
                cfg.reload_now()
                logger.info("lifecycle cleared pod_id for %s/%s",
                            m.backend_name, m.model_key)
    except Exception:  # noqa: BLE001 — never break a lifecycle action
        logger.warning("lifecycle pod_id clear failed", exc_info=True)
    finally:
        m.spec.pop("pod_id", None)


async def _act_stop(m, *, terminate: bool) -> None:
    """Watchdog-initiated stop — remembered so the next tick's death
    detector doesn't mistake our own action for a crash."""
    _LAST_POD_STATE[(m.backend_name, m.model_key)] = "EXITED"
    if terminate:
        _clear_pod_id(m)   # else the spec re-seeds the dead pod
    await m.stop(terminate=terminate)


async def _redeploy(cfg, b: str, k: str, m) -> None:
    """D5: ONE attempt, no retry loop — capacity can be dry for hours
    (glm53 arc: EUR-IS-1 3×A100 dry 80+ min). Volume survives in the
    spec, so the new pod skips the model download."""
    ok, why = await budget_allows(cfg, b, k)
    if not ok:
        _emit("redeploy_refused", k, f"redeploy refused — {why}",
              severity="critical", pod_id=m.pod_id, backend=b)
        return
    old_pod = m.pod_id
    _clear_pod_id(m)
    m.ready = False
    m.pod_id = None
    m.pod_status = "unknown"
    m.first_seen_s = None
    m.warming = False
    _emit("redeploy_started", k,
          f"pod {old_pod} died — redeploying from spec (volume survives; "
          "no re-download)", pod_id=old_pod, backend=b)

    async def _run() -> None:
        try:
            await m.ensure_ready()
            _emit("redeploy_ok", k, f"redeployed as pod {m.pod_id}",
                  pod_id=m.pod_id, backend=b)
        except Exception as e:  # noqa: BLE001 — reported as an alert
            _emit("redeploy_failed", k,
                  f"redeploy failed: {e} — staying down (no retry)",
                  severity="critical", pod_id=old_pod, backend=b)

    asyncio.get_running_loop().create_task(_run())


# ── Rules (one watchdog tick, D3) ───────────────────────────────────────────

_LAST_POD_STATE: dict[tuple[str, str], str] = {}
_OVER_BUDGET_SINCE: dict[tuple[str, str], float] = {}


async def tick(cfg) -> dict[str, int]:
    """Refresh state, then apply rules per manager: death → never-
    healthy → budget stop → idle tiers. Returns action counts."""
    await runpod.refresh_status(cfg)
    snap = await month_to_date(cfg)
    now = time.monotonic()
    acted = {"idle_stopped": 0, "idle_terminated": 0, "budget_stopped": 0,
             "never_healthy_stopped": 0, "redeployed": 0}
    for (b, k), m in runpod.managers():
        spec = m.spec
        prev = _LAST_POD_STATE.get((b, k))
        cur = m.pod_status if m.pod_id else "not-deployed"

        # death (D5): TERMINATED while believed running. EXITED (a
        # stop, ours or a user's) is NOT a death.
        if cur == "TERMINATED" and prev in ("RUNNING", "READY", "UNKNOWN"):
            _emit("pod_died", k,
                  f"pod {m.pod_id} terminated unexpectedly (was {prev})",
                  severity="critical", pod_id=m.pod_id, backend=b)
            if spec.get("auto_recover") == "redeploy" and spec.get("image"):
                await _redeploy(cfg, b, k, m)
                acted["redeployed"] += 1
        _LAST_POD_STATE[(b, k)] = cur

        if not m.is_ready():
            # never-healthy reaper (D6): RUNNING but never served, no
            # request waiting on it, past the boot budget → stop it
            # (a boot nobody watches bills forever).
            if (m.pod_id
                    and m.pod_status in ("RUNNING", "READY", "UNKNOWN")
                    and not m.warming and m.inflight == 0
                    and m.first_seen_s is not None):
                boot_budget = max(
                    float(spec.get("start_timeout_s") or 600),
                    NEVER_HEALTHY_MIN_S)
                if now - m.first_seen_s > boot_budget:
                    _emit("never_healthy", k,
                          f"pod {m.pod_id} never became healthy within "
                          f"{boot_budget:.0f}s — stopped (check its logs)",
                          pod_id=m.pod_id, backend=b)
                    await _act_stop(m, terminate=False)
                    acted["never_healthy_stopped"] += 1
            continue

        # budget stop (D4): over cap → stop when idle, force-stop after
        # a grace window when constantly busy.
        if snap is not None:
            cap_g = float(getattr(cfg, "budget_usd_month", 0) or 0)
            cap_m = float(spec.get("monthly_budget_usd") or 0)
            over = ""
            if cap_g > 0 and snap["total_usd"] >= cap_g:
                over = (f"account cap ${cap_g:g} reached "
                        f"(${snap['total_usd']:.2f} month-to-date)")
            else:
                mk = f"{b}|{k}"
                spent = float(snap["per_model"].get(mk, 0.0))
                if cap_m > 0 and spent >= cap_m:
                    over = (f"model cap ${cap_m:g} reached "
                            f"(${spent:.2f} month-to-date)")
            if over:
                _emit("over_budget", k, f"stopping pod: {over}",
                      severity="critical", pod_id=m.pod_id, backend=b)
                if m.inflight == 0:
                    await _act_stop(m, terminate=False)
                    acted["budget_stopped"] += 1
                else:
                    since = _OVER_BUDGET_SINCE.get((b, k))
                    if since is None:
                        _OVER_BUDGET_SINCE[(b, k)] = now
                    elif now - since > BUDGET_STOP_GRACE_S:
                        _emit("budget_force_stop", k,
                              f"still in flight {BUDGET_STOP_GRACE_S / 60:.0f}m "
                              "past the cap — force-stopping",
                              severity="critical", pod_id=m.pod_id, backend=b)
                        await _act_stop(m, terminate=False)
                        acted["budget_stopped"] += 1
            else:
                _OVER_BUDGET_SINCE.pop((b, k), None)
                _resolve("over_budget", b, k, m.pod_id)

        # idle tiers (D2): stop is resumable, terminate frees everything
        # (opt-in); inflight guard so a live stream is never cut.
        if m.is_ready():
            idle = now - m.last_used
            it = float(spec.get("idle_terminate_s") or 0)
            ist = float(spec.get("idle_stop_s") or 0)
            if it > 0 and idle > it:
                _emit("idle_terminated", k,
                      f"idle {idle / 60:.0f}m > {it / 60:.0f}m — terminated",
                      severity="info", pod_id=m.pod_id, backend=b)
                await _act_stop(m, terminate=True)
                acted["idle_terminated"] += 1
            elif ist > 0 and idle > ist and m.inflight == 0:
                _emit("idle_stopped", k,
                      f"idle {idle / 60:.0f}m > {ist / 60:.0f}m — stopped "
                      "(resumes on the next request)",
                      severity="info", pod_id=m.pod_id, backend=b)
                await _act_stop(m, terminate=False)
                acted["idle_stopped"] += 1
    return acted


# ── Watchdog (D3) ───────────────────────────────────────────────────────────

_WATCHDOG_TASK: asyncio.Task | None = None


async def _watchdog_loop() -> None:
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_S)
        try:
            cfg = get_gateway_config()
            cfg.maybe_reload()
            await tick(cfg)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — the watchdog must never die
            logger.warning("lifecycle watchdog error", exc_info=True)


def ensure_watchdog() -> None:
    """Start the watchdog on the running loop (idempotent). Disabled
    with GATEWAY_WATCHDOG=0 (tests — the loop would fight the fakes)."""
    global _WATCHDOG_TASK
    if os.environ.get("GATEWAY_WATCHDOG", "1").strip() == "0":
        return
    loop = asyncio.get_running_loop()
    if _WATCHDOG_TASK is None or _WATCHDOG_TASK.get_loop() is not loop:
        _WATCHDOG_TASK = loop.create_task(_watchdog_loop())
        logger.info("lifecycle watchdog started (%.0fs tick)",
                    WATCHDOG_INTERVAL_S)


def watchdog_running() -> bool:
    return bool(_WATCHDOG_TASK and not _WATCHDOG_TASK.done())


def budget_snapshot() -> dict[str, Any] | None:
    """Cached month-to-date (NO fetch) — healthz / GET /admin/lifecycle
    render this; only the watchdog tick refreshes it (D8)."""
    return _BUDGET_STATE["snapshot"]


def mark_stopped(pod_id: str) -> None:
    """Our OWN termination of a pod must not read as an unexpected
    death (admin terminate action, upload receiver cleanup): pre-mark
    the manager + rule memory so the next tick's death detector sees
    EXITED, not RUNNING -> TERMINATED. E2E-proven bug 2026-09-20."""
    pid = str(pod_id or "").strip()
    if not pid:
        return
    for (b, k), m in runpod.managers():
        if str(m.pod_id) == pid:
            m.pod_status = "EXITED"
            _LAST_POD_STATE[(b, k)] = "EXITED"
            logger.info("lifecycle marked %s (%s/%s) as intentionally "
                        "stopped", pid, b, k)
            return


def reset_budget_cache() -> None:
    """Drop the cached month-to-date — settings changes want immediate
    effect, not a 60s TTL wait."""
    _BUDGET_STATE["snapshot"] = None
    _BUDGET_STATE["at"] = 0.0
