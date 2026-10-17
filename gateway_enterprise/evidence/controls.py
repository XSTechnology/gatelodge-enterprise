"""Evidence Center — the control→framework mapping (controls.md).

Static content, shipped in every package. "Supports" language only:
Gatelodge produces evidence artifacts the customer's auditor evaluates;
Gatelodge is not a certification body and does not attest compliance.
The NOT-PROVIDED list is as prominent as the mapping — that honesty is
the product's standing posture.
"""

CONTROLS_MD = """# Control mapping

How the artifacts in this package map to common control frameworks.
Each row cites the artifact an auditor can inspect. Gatelodge
**supports** these controls with verifiable evidence; Gatelodge does
**not certify** anything — the auditor's conclusion is the auditor's.

| # | Gatelodge control | Evidence artifact (in this package) | SOC 2 TSC | ISO 27001:2022 | NIST AI RMF | EU AI Act |
|---|---|---|---|---|---|---|
| 1 | Per-request logging of who / which model / when / cost / outcome | `ledger/gateway-log-*.jsonl` | CC7.1 | A.8.15 | MEASURE | Art. 12 |
| 2 | Tamper-evident request ledger (hash chain; `evidence` claim) | `ledger/ledger-verify.json` | CC7.1 | A.8.15 | MEASURE | Art. 12 |
| 3 | Tamper-evident config-mutation + RBAC-denial audit chain | `config-audit/config-audit-verify.json` | CC7.1, CC8.1 | A.8.15, A.8.32 | GOVERN | Art. 12 |
| 4 | Provider allowlist — unapproved models do not serve | `inventory.json` (backends + entitlements) | CC6.1 | A.8.9 | GOVERN, MAP | Art. 15 |
| 5 | Per-key identity on every request; admin/virtual key separation | `inventory.json` (api_keys), `ledger/…` (`key` field) | CC6.1 | A.5.15, A.8.2 | GOVERN | Art. 12 |
| 6 | Hard budgets per key (tokens + USD; refusals, not warnings) | `inventory.json` (budgets) | CC8.1 | A.8.9 | GOVERN | — |
| 7 | Local-only routing option (requests never leave the machine) | `inventory.json` (routing.local_only_aliases) | CC6.7 | A.8.12 | MAP | Art. 15 |
| 8 | Signed license + revocation list (deployment identity) | `license/license.json` | CC6.1 | A.5.15 | GOVERN | — |
| 9 | Optional encrypted full-traffic capture, customer-held keys, blind search, escrow (Content Vault add-on) | `vault/` (when enabled) | CC6.1, CC7.1 | A.8.15, A.8.24 | MEASURE | Art. 12, Art. 10 |
| 10 | Self-verification: chains and package hashes check without trusting the vendor | `README.md`, `manifest.json` | CC7.2 | A.8.16 | MEASURE | — |

## Explicitly NOT provided (stated plainly)

- **No certification or attestation** of any kind — SOC 2, ISO 27001,
  FedRAMP or otherwise. Gatelodge is not an auditor. Certification
  programs for the company itself are on the roadmap; until then, the
  honest statement is: self-hosted software with verifiable evidence.
- **No content inspection, DLP, or threat detection.** The gateway
  governs routing and logs metadata by default; it does not inspect
  prompt content. The Content Vault captures content **encrypted to
  keys only the customer holds** — even then the gateway cannot read it.
- **No SaaS-embedded AI coverage.** SaaS copilots (M365 Copilot et al.)
  do not route through the gateway. The control point is API-driven AI.
- **No risk scoring.** Posture facts are counts and states, each
  traceable to an included artifact. Nothing weighted, nothing invented.

## Re-verification (no trust in the vendor required)

```
# every file in the package:
sha256sum <file>            # compare against manifest.json
# ledger chain (canonical JSON: sorted keys, no spaces):
#   hash = sha256(record-with-"hash"-removed, sort_keys=True)
#   prev = previous chained record's hash ("genesis" for the first)
```

The full verifier ships with the product:
`python -m gateway_enterprise.evidence.tools verify <package.zip>`
"""
