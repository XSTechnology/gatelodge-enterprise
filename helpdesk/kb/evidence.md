# Evidence Center — auditor-ready packages

title: Evidence Center — tamper-evident ledger, audit packages, control mapping
description: The Enterprise evidence layer — chained request ledger, one-command evidence packages, SOC 2 / ISO 27001 / NIST AI RMF / EU AI Act mappings

## What it is

The Evidence Center (Enterprise, v2.5.0) turns the gateway's records
into auditor-ready artifacts:

- **Tamper-evident request ledger.** Under the `evidence` license
  claim, every request line in `gateway-log-YYYY-MM.jsonl` carries
  `prev`/`hash` (sha256 over canonical JSON — the same pattern as the
  config-mutation audit chain). The chain continues across restarts
  and month files. Editing, deleting, or stripping the hash from any
  chained record breaks the verification walk.
- **One-command evidence packages.** `POST /admin/evidence/package`
  assembles a self-describing zip: the ledger months with their chain
  proofs, the config-audit chain, license state, an AI asset
  inventory, a governance posture report, and the control mapping.
- **Self-verification.** `manifest.json` carries the sha256 of every
  file. `python -m gateway_enterprise.evidence.tools verify
  <package.zip>` re-checks every hash and re-walks the ledger chain
  from the package's own extract — an auditor with any sha256 tool
  can verify without our software.

## What's in a package

```
README.md               what it is, how to re-verify, what it is NOT
manifest.json           package id, period, license, per-file sha256
ledger/                 request-ledger months + chain-verify result
config-audit/           config-mutation chain + verify result
license/                public license claims + CRL state
inventory.json          models, keys (hash-abbreviated), providers, budgets
posture.md + .json      governance posture — every figure cites its artifact
controls.md             SOC 2 TSC · ISO 27001 · NIST AI RMF · EU AI Act
vault/                  when the Content Vault add-on is running
```

## Honest boundaries (stated in every package)

- We produce evidence **for** your audit. We do not certify, attest,
  or imply compliance achieved.
- No risk scoring. Posture facts are counts and states, each
  traceable to an included artifact.
- No content inspection, DLP, or threat detection — with or without
  the Content Vault.
- Community edition keeps plain (unchained) request logs; chaining
  and packaging are the Enterprise evidence layer.

## Commands

```
GET  /admin/ledger                 # ledger chain verification state
GET  /admin/evidence/status        # what a package would contain now
POST /admin/evidence/package       # {"from": "2026-09", "to": "2026-10"}
python -m gateway_enterprise.evidence.tools verify <package.zip>
```

Period granularity is the month (files are month-scoped; whole files
are included so the chain walks cleanly). Packages land in
`data/evidence/` on your machine — nothing is transmitted anywhere.
