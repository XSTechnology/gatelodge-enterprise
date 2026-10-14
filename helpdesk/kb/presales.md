# Presales — what Gatelodge is

title: Presales — what Gatelodge is, editions, pricing, trial
description: Product overview, what it does, editions and pricing, trial terms, deployment model

## What Gatelodge is

Gatelodge is a **self-hosted AI gateway** — a single binary that sits in
front of every LLM your teams use and exposes **one OpenAI-compatible
endpoint**. Requests arrive at the gate, get routed to the right model
(cloud or on-prem), and every exchange is governed: keys, budgets, cost
accounting, and a tamper-evident audit trail.

Tagline: **Every model. One gateway. Fully governed.**

## The problems it solves

- API keys pasted into scripts and shared over chat → per-team gateway
  keys, central credential vault; provider keys never appear in config
  or logs
- Surprise inference invoices → monthly token budgets with hard stops,
  per-team/per-request cost accounting, semantic cache (cache hits never
  bill)
- "Who called which model with what data?" → per-request structured logs
  plus a hash-chained, append-only audit trail — tamper-evident by
  construction
- SDK lock-in → one endpoint in front of OpenAI, Anthropic, Gemini,
  Azure, Vertex, Bedrock, RunPod, and local models; swap per task with
  alias chains, zero application changes
- Shadow AI → self-hosted with allowlisted backends; unapproved models
  don't even display
- Provider outages → hedge routing and automatic failover chains

## Editions

- **Community** — free compiled binary with the FULL governance stack
  (observability, cost control, governance, local inference) as a
  30-day trial from first boot. After the trial: buy a perpetual key
  (US$99, one-time) or keep using local inference for free.
- **Enterprise** — paid binary (no license = no boot): adds SCIM
  provisioning, SSO/OIDC, session budgets, MCP gateway, team
  workspaces, tier-D replicas (shared state over Redis), support, and
  compliance posture. Priced per user per month (approximately
  US$39/user/mo; exact terms on request).

There is no open-source edition — Gatelodge ships as machine code only
(closed distribution).

## Deployment model

- Single Windows executable (64-bit, ~156 MB, no installer)
- Boots in one command; admin console at http://localhost:8010 by default
- Everything runs inside your perimeter — prompts never leave your
  estate (only the calls you explicitly route to cloud providers do)
- Self-hosted by design: your keys, your logs, your audit chain

## Who it's for

Teams building AI features who need governance before compliance comes
knocking: platform/infra teams, security-conscious engineering orgs,
and channel partners (inference providers) who co-brand trials for
their customers.
