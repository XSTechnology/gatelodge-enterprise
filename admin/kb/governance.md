# Governance — keys, roles, budgets, audit

title: Governance — gateway keys, RBAC, budgets, audit chain, don't-display
description: How Gatelodge governs access and spend: keys, roles, budgets, the tamper-evident audit chain

## Gateway keys

Applications authenticate to the gateway with **gateway keys** — not
provider keys. Create one per team or service in the Access tile;
revoke instantly. Token precedence: `Authorization` header > `x-api-key`
> URL path token > query parameter.

Provider credentials (OpenAI/Anthropic/… keys) are stored centrally —
resolved from environment variables, files, commands, or the
console-managed secrets store — and **never appear in config files,
logs, or API responses**.

## Roles (RBAC)

- **admin** — full control
- **read-only ops** — see everything, change nothing
- **key-manager** — manage gateway keys only

The console and API enforce the same roles. Enterprise adds SCIM
provisioning and SSO/OIDC login.

## Budgets

- **Monthly token budgets** per team and per gateway key, with **hard
  stops** — when the budget is spent, requests are refused, not warned
- Enterprise adds **per-session budgets**
- Every request carries cost accounting: per-request, per-team,
  per-backend
- The **semantic cache** serves repeated requests without billing

## The audit chain

Every governed exchange is written to an **append-only, hash-linked
ledger** (`data/gateway-log-*.jsonl`): each entry chains to the
previous hash. Tampering with any entry breaks the chain — the
Governance tile verifies and displays this. This is evidence-grade
logging, not just logs.

## Don't-display doctrine

Unapproved or unlicensed models never appear in any catalog or
completion listing, and requesting one returns an error byte-identical
to an unknown model. The gateway cannot leak what it will not name.

## Revocation

- Gateway keys: instant, from the console
- Licenses: a certificate revocation list (CRL) ships with updates;
  revoked serials stop validating (hot-reloaded, no restart needed)
