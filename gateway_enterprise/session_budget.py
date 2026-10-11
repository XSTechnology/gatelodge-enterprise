"""Session budgets — the enterprise pre-gate (v1.4), completing the
session-budgets control the core's charge hook feeds.

The core (upstream 57f429e) captures a validated x-session-id when
``session_budgets.enabled`` and charges a ``session:<id>`` spend scope
ALONGSIDE the consuming key — the same monthly ledger machinery, riding
the storage backend (shared across replicas in tier D). THIS module is
the enterprise side: an auth-pipeline link that pre-gates /v1 requests
on session spend vs a budget.

Config (raw section — the core parses only ``enabled``):

    "session_budgets": {
      "enabled": true,
      "default_tokens": 1000000,       // every session's budget
      "per_tenant": {"acme-team": 5000000}   // by consuming key OWNER
    }

Semantics mirror the core's per-KEY monthly budget gate
(_key_budget_gate): admission checks CURRENT spend (>= budget → 429
``session_budget_exceeded``, rate_limit_error type); the tenant axis is
the consuming key's OWNER field (SCIM users carry displayName there;
corporate keys carry the team name — the natural tenant marker in the
key model). The link is INERT without the section or a session id
(zero overhead when off) — the enterprise pipeline is safe to always
wire (enterprise_main does).
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from gateway.auth import _REQUEST_SESSION_ID

logger = logging.getLogger(__name__)


# ── settings: raw section, version-cached (AdminRoleMap pattern) ───────────

class SessionBudgetState:
    """Re-parses the raw session_budgets section when the active
    store's version token moves; keeps last good on unreadable load."""

    def __init__(self) -> None:
        self._store: Any = None
        self._version: Any = object()   # sentinel ≠ any real token
        self._settings: dict = {}

    def settings(self, cfg) -> dict:
        store = getattr(cfg, "store", None)
        if store is not self._store:    # singleton swapped (tests)
            self._store = store
            self._version = object()
        if store is None:
            return self._settings
        version = store.version()
        if version == self._version:
            return self._settings
        raw = store.load()
        if isinstance(raw, dict):
            sec = raw.get("session_budgets")
            sec = sec if isinstance(sec, dict) else {}
            try:
                default_tokens = max(0, int(sec.get("default_tokens") or 0))
            except (TypeError, ValueError):
                default_tokens = 0
            per_tenant: dict[str, int] = {}
            pt = sec.get("per_tenant")
            if isinstance(pt, dict):
                for tenant, v in pt.items():
                    try:
                        tv = max(0, int(v))
                    except (TypeError, ValueError):
                        continue
                    if tv > 0 and str(tenant).strip():
                        per_tenant[str(tenant).strip()] = tv
            self._settings = {"default_tokens": default_tokens,
                              "per_tenant": per_tenant}
        self._version = version
        return self._settings


_STATE = SessionBudgetState()


def get_session_budget_state() -> SessionBudgetState:
    return _STATE


# ── the link ───────────────────────────────────────────────────────────────

def session_budget_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Enterprise link: pre-gate /v1 requests carrying a session id on
    the session's token spend (monthly ledger — for a per-run session
    id, that IS the run budget). Mirrors the core's per-key budget gate
    shape (429 session_budget_exceeded). Inert when the section is
    absent/disabled or the request has no session id."""
    if not request.url.path.startswith("/v1/"):
        return None
    sid = _REQUEST_SESSION_ID.get()
    if not sid:
        return None
    settings = _STATE.settings(cfg)
    budget = int(settings.get("default_tokens") or 0)
    if budget <= 0:
        return None
    # per-tenant override: the consuming key's OWNER is the tenant axis
    if key_id and key_id != "admin":
        owner = str(((cfg.api_keys or {}).get(key_id) or {})
                    .get("owner") or "").strip()
        if owner and owner in (settings.get("per_tenant") or {}):
            budget = int(settings["per_tenant"][owner])
    if budget <= 0:
        return None
    import gateway.app as gw_app  # lazy: app imports nothing from here
    spend = gw_app._scope_spend(f"session:{sid}")
    if spend["tokens"] >= budget:
        logger.warning("session budget: %r at %d/%d tokens (key %r)",
                       sid, spend["tokens"], budget, key_id or "(open)")
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": (f"session {sid!r}: token budget exhausted "
                            f"({spend['tokens']}/{budget} tokens)"),
                "type": "rate_limit_error", "param": None,
                "code": "session_budget_exceeded"}})
    return None
