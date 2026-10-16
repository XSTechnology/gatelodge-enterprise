# Competitive playbook — internal, never served to customers

title: Competitive playbook — internal reference for the team
description: Internal competitive positioning against LiteLLM, Portkey, OpenRouter, Kong, and Azure APIM; buy-versus-build; where we win and where we face friction. Staff only.
internal: true

## How to use this document

This is the team's competitive reference. The Gatekeeper never serves it
to customers: docs marked internal are excluded from retrieval and from
the intent engine. When a customer names a competitor, answer from the
public "Comparing Gatelodge" doc — generic categories, no competitor
names. The named analysis below is for your preparation.

## Positioning in one line

Gatelodge is the AI Governance Gateway. LiteLLM is a developer tool;
Portkey is AI operations; OpenRouter is a marketplace; Kong and Azure
APIM govern APIs, not AI. We govern AI.

## LiteLLM

**What they do well:** massive developer adoption, open source, broad
provider coverage, easy bottom-up adoption. The default answer for
"connect once to many models."

**Their buyer:** developers and platform engineers, chosen bottom-up.

**Our differentiation:** governance. RBAC, SSO/SCIM, team workspaces,
hard budget enforcement, tamper-evident audit chain, provider
allowlisting. LiteLLM optimizes how developers reach models; we govern
how the organization uses them.

**Positioning language:** "LiteLLM helps developers connect to models.
Gatelodge helps enterprises govern AI usage." Never say "we are a better
LiteLLM" — that makes them the reference point.

**Where we face friction:** open-source preference (code inspection),
zero-cost entry, grassroots adoption. Answer: closed distribution is a
deliberate choice for a commercial governance product; offer the
30-day full-feature trial as the evaluation path.

## Portkey

**What they do well:** strong observability, tracing, reliability — the
AI operations platform. More mature commercial posture than most.

**Their buyer:** platform and AI engineering teams.

**Our differentiation:** governance-first vs ops-first; self-hosted vs
SaaS in the request path. Portkey is a service you send traffic through;
Gatelodge runs inside your perimeter.

**Positioning language:** "Portkey focuses on AI operations. Gatelodge
focuses on AI governance — and runs inside your walls."

## OpenRouter

**What they do well:** model access marketplace, huge catalog, minimal
friction.

**Their buyer:** individual developers and small teams.

**Our differentiation:** not really a competitor — different problem.
They answer "how do I access more models"; we answer "how does the
organization govern AI." If a customer mentions them, they are usually
an individual developer, not our buyer.

## Kong AI Gateway

**What they do well:** enterprise credibility, proven scale, mature API
management, existing footprint in most large orgs.

**Their buyer:** platform engineering and enterprise architecture.

**Our differentiation:** AI-specific by design — token budgets, model
routing, semantic caching, per-model cost attribution. A general gateway
extended toward AI is a spreadsheet extended toward accounting.

**Where we face friction:** Kong is often already procured. Answer: AI
traffic is different traffic; the trial makes the difference concrete.

## Azure API Management

**What they do well:** Azure ecosystem alignment, procurement ease,
mature governance.

**Our differentiation:** provider neutrality (they are Azure-centered),
AI-specific controls and cost management.

**Where we face friction:** Microsoft shops default to Microsoft.
Answer: multi-provider neutrality is exactly what a gateway is for.

## Buy versus build

The objection: "we can build this ourselves." The reality: the first 20%
(a reverse proxy plus routing) is easy. The remaining 80% — SSO, RBAC,
audit chain, budget accounting, failover, caching, compliance reporting,
and maintaining all of it forever — is where internal projects stall.
Ask what their timeline is for the audit chain alone.

## Where we win

Financial services (regulatory oversight, audit), government and defense
(data sovereignty, self-hosted), healthcare (privacy, controlled
access), insurance (compliance, cost visibility), large multi-team
enterprises (governance at scale).

## Where we face friction (be honest internally)

Developer-led adoption cycles (open-source is easier to trial), small
teams without governance requirements, early startups, and
open-source-preference procurement. The trial + the self-hosted story
are the answers; do not fight the open-source battle on their terms.

## Messaging rules

- Lead with governance, auditability, cost control, compliance readiness
  — never with routing or model count (commodities).
- Never name competitors in public docs or customer answers.
- Never claim certifications we do not hold — say where we are, offer
  the team follow-up.
- The category we own: AI Governance Gateway.
