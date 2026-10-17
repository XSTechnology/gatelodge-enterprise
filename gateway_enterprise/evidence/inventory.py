"""Evidence Center — AI asset inventory (derived from live config).

Redaction rules (hard): NO key material, NO secret values, NO env-var
contents. Key hashes surface only as their first 8 hex chars (enough to
distinguish entries, useless for auth — auth compares full hashes
constant-time). File paths surface as basenames only.
"""
from __future__ import annotations

from typing import Any


def _hash8(h: Any) -> str:
    s = str(h or "")
    return (s[:8] + "…") if s else "—"


def build_inventory(cfg) -> dict[str, Any]:
    """cfg = gateway.config.GatewayConfig (already loaded/normalized).

    Returns the AI asset inventory: models, keys, providers, teams,
    budgets — the 'what is running' half of an evidence package."""
    backends: list[dict[str, Any]] = []
    for name in sorted((cfg.backends or {}).keys()):
        b = dict(cfg.backends.get(name) or {})
        b.pop("api_key", None)              # never, even if present
        for redact in ("api_key_file", "client_secret", "password"):
            if b.get(redact):
                b[redact] = _basename(b[redact])
        b["name"] = name
        b["provider_entitled"] = _entitled(name, b.get("type"))
        backends.append(b)

    keys: list[dict[str, Any]] = []
    for kid, e in sorted((cfg.api_keys or {}).items()):
        keys.append({
            "id": kid,
            "note": e.get("note") or None,
            "hash8": _hash8(e.get("key_hash")),
            "monthly_token_budget": e.get("monthly_token_budget") or 0,
            "monthly_usd_budget": e.get("monthly_usd_budget") or 0,
            "rate_limit_rpm": e.get("rate_limit_rpm") or 0,
            "models": e.get("models") or None,
        })

    aliases: list[dict[str, Any]] = []
    for name in sorted((cfg.aliases or {}).keys()):
        a = dict(cfg.aliases.get(name) or {})
        aliases.append({"name": name, "chain": a.get("chain") or [],
                        "hedge_s": a.get("hedge_s") or 0,
                        "local_only": bool(a.get("local_only"))})

    teams = getattr(cfg, "teams", None) or {}
    team_rows = []
    for tid, t in sorted(teams.items()) if isinstance(teams, dict) else []:
        members = t.get("members") if isinstance(t, dict) else None
        team_rows.append({"id": tid,
                          "members": len(members) if isinstance(
                              members, (list, dict)) else 0})

    return {
        "backends": backends,
        "models": {"default_alias": cfg.default_alias,
                   "aliases": aliases},
        "api_keys": keys,
        "teams": team_rows,
        "budgets": {
            "keys_with_token_budget":
                sum(1 for k in keys if k["monthly_token_budget"]),
            "keys_with_usd_budget":
                sum(1 for k in keys if k["monthly_usd_budget"]),
            "monthly_budget_usd": getattr(cfg, "monthly_budget_usd", 0)
            or 0,
        },
        "gateway_keys": len(cfg.gateway_keys or []),
    }


def _entitled(name: str, ptype: str | None) -> bool | str:
    """Provider entitlement state, live (the same gate resolve_chain
    filters on). Local compute is always entitled; cloud types depend
    on the license's providers claim."""
    try:
        from gateway import licensing
        if licensing.provider_allowed(name, str(ptype or "")):
            return True
        return "not-entitled"
    except Exception:                        # noqa: BLE001
        return "unknown"


def _basename(p: Any) -> str:
    s = str(p or "").replace("\\", "/").rstrip("/")
    return s.rsplit("/", 1)[-1] if s else "—"
