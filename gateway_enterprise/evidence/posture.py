"""Evidence Center — governance posture: derived facts only.

Every number is a query over artifacts included in the same package
(the ledger walk, the config-audit walk, the config snapshot, the
licensing state). No scoring, no weighting, no risk numerology — a
checklist a CISO can read in 90 seconds, each line citing its source.
"""
from __future__ import annotations

from typing import Any


def build_posture(cfg, lic_status: dict[str, Any],
                  ledger_verify: dict[str, Any],
                  audit_verify: dict[str, Any],
                  period_stats: dict[str, Any],
                  vault_state: dict[str, Any] | None) -> dict[str, Any]:
    """Assemble the posture dict. period_stats comes from the package's
    own ledger extract (counts computed at assembly time)."""
    aliases = cfg.aliases or {}
    keys = cfg.api_keys or {}

    posture: dict[str, Any] = {
        "edition": lic_status.get("edition"),
        "license_mode": lic_status.get("mode"),
        "license_serial": lic_status.get("serial"),
        "license_expires_at": lic_status.get("expires_at"),
        "license_perpetual": lic_status.get("perpetual"),
        "evidence_chaining": bool(
            (lic_status.get("features") or {}).get("evidence")),
        "request_ledger": {
            "chain_valid": ledger_verify.get("valid"),
            "chained_records": ledger_verify.get("chained"),
            "plain_records": ledger_verify.get("plain"),
            "source": "ledger/ledger-verify.json",
        },
        "config_audit": {
            "chain_valid": audit_verify.get("valid"),
            "chained_records": audit_verify.get("chained"),
            "source": "config-audit/config-audit-verify.json",
        },
        "budgets": {
            "keys_with_hard_stops":
                sum(1 for e in keys.values()
                    if (e.get("monthly_token_budget") or 0)
                    or (e.get("monthly_usd_budget") or 0)),
            "api_keys_total": len(keys),
            "source": "inventory.json",
        },
        "routing": {
            "aliases_total": len(aliases),
            "local_only_aliases":
                sum(1 for a in aliases.values()
                    if a.get("local_only")),
            "hedge_aliases": sum(1 for a in aliases.values()
                                 if (a.get("hedge_s") or 0) > 0),
            "source": "inventory.json",
        },
        "period": period_stats,
    }
    if vault_state is not None:
        posture["content_vault"] = {
            "enabled": vault_state.get("running", False),
            "mode": vault_state.get("mode"),
            "key_versions": [k.get("v") for k in
                             (vault_state.get("keys") or [])],
            "records": vault_state.get("records"),
            "chain_valid": vault_state.get("chain_valid"),
            "source": "vault/vault-status.json + vault/vault-verify.json",
        }
    else:
        posture["content_vault"] = {
            "enabled": False,
            "note": "content capture disabled — the metadata-only "
                    "posture is the product's default posture",
            "source": "config snapshot (no vault section active)",
        }
    return posture


def posture_markdown(p: dict[str, Any]) -> str:
    """The human-readable checklist (posture.md in the package)."""
    led = p["request_ledger"]
    aud = p["config_audit"]
    per = p.get("period") or {}
    v = p.get("content_vault") or {}
    b = p.get("budgets") or {}
    r = p.get("routing") or {}
    lines = [
        "# Governance posture",
        "",
        f"_Generated with the evidence package. Every figure below is "
        f"computed from artifacts included in this package — nothing "
        f"hand-entered._",
        "",
        "## License",
        f"- Edition: **{p.get('edition')}**, mode: "
        f"**{p.get('license_mode')}**"
        + (f", serial `{p.get('license_serial')}`"
           if p.get("license_serial") else ""),
        f"- Request-ledger chaining under the `evidence` claim: "
        f"**{'ON' if p.get('evidence_chaining') else 'OFF'}**",
        "",
        "## Tamper evidence",
        f"- Request ledger: "
        f"**{'intact' if led.get('chain_valid') else 'BREAK DETECTED'}** "
        f"({led.get('chained_records', 0)} chained, "
        f"{led.get('plain_records', 0)} plain records) "
        f"— `ledger/ledger-verify.json`",
        f"- Config audit chain: "
        f"**{'intact' if aud.get('chain_valid') else 'BREAK DETECTED'}** "
        f"({aud.get('chained_records', 0)} chained records) "
        f"— `config-audit/config-audit-verify.json`",
        f"- Content Vault: "
        + ("**on**" if v.get("enabled") else "**off** (metadata-only "
           "default posture)")
        + (f", mode {v.get('mode')}, key versions "
           f"{v.get('key_versions')}, {v.get('records')} records, chain "
           f"{'intact' if v.get('chain_valid') else 'BREAK DETECTED'}"
           if v.get("enabled") else ""),
        "",
        "## Controls in force",
        f"- Hard budgets on **{b.get('keys_with_hard_stops', 0)}** of "
        f"{b.get('api_keys_total', 0)} API keys",
        f"- **{r.get('local_only_aliases', 0)}** local-only aliases "
        f"(never leave the machine) of {r.get('aliases_total', 0)}",
        f"- **{r.get('hedge_aliases', 0)}** hedge/failover aliases",
        "",
        "## Request period",
        f"- Requests in period: **{per.get('requests', 0)}** "
        f"({per.get('ok', 0)} ok · {per.get('client_errors', 0)} client "
        f"errors · {per.get('server_errors', 0)} upstream failures)",
        f"- Distinct models: {per.get('models', 0)} · distinct keys: "
        f"{per.get('keys', 0)}",
        f"- Estimated provider cost: "
        f"${per.get('cost_usd', 0.0):.2f}",
        "",
        "_This is a factual control inventory, not a certification. "
        "See controls.md for framework mappings and the explicit "
        "not-provided list._",
    ]
    return "\n".join(lines) + "\n"
