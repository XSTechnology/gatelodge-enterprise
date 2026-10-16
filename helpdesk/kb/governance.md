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

## Shadow AI and the approved path

Shadow AI — employees using unapproved AI services outside governance
controls — is one of the fastest-growing enterprise risks: data leakage,
compliance exposure, and spend nobody can see. You cannot audit what you
do not control.

Gatelodge creates the approved path: one governed endpoint with the
models your organization has vetted, behind your identity provider, with
budgets per team. When the approved path is easy, fast, and covers what
people actually need, shadow usage stops being the path of least
resistance. And because every request through the gateway is attributed
to a user, a team, and a model, "who used which AI system with what
data" becomes a query, not an investigation.

## Credential centralization

Without a gateway, every application and script carries its own provider
API keys — distributed across environments, committed to repos, pasted
into notebooks. Each copy is an exposure and a rotation chore.

With Gatelodge, applications never hold provider credentials. They
authenticate to the gateway with their own keys; the gateway holds the
provider credentials centrally and applies them per policy. One place to
rotate, one place to audit who could reach which provider, zero provider
keys in your codebase.

## The governance maturity model

Organizations evolve through recognizable stages on the way to governed
AI:

- **Level 1 — Unmanaged:** direct provider usage by whoever signs up. No
  oversight, no visibility.
- **Level 2 — Inventory:** someone starts tracking which teams use which
  AI services, usually in a spreadsheet.
- **Level 3 — Controlled:** access controls, budgets, and an approved
  provider list emerge — often unevenly enforced.
- **Level 4 — Governed:** a central enforcement point manages AI
  operations: identity, policy, budgets, and audit applied consistently
  to every request.
- **Level 5 — Optimized:** governance is measurable and continuously
  improved — cost per workload, policy coverage, audit readiness on
  demand.

Most organizations today sit between Levels 1 and 3. Gatelodge is built
to take an organization to Levels 4 and 5: the gateway is the
enforcement point where identity, budget, provider policy, and audit
intersect on every request — not as an afterthought bolted onto
adoption, but as the front door it came through.
