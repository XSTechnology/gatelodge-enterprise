# Security Architecture — CISO deck material (ground-truthed)

Source: Copilot draft, reviewed against the product. Every claim below
is backed by shipped code. Where the draft overclaimed, it was cut or
reworded — see the claims discipline at the end.

## SLIDE: Enterprise AI Security Architecture

Title: **Bringing AI under security governance**

```
        ┌─────────────────────┐   ┌─────────────────────┐
        │      Employees      │   │   Business Systems  │
        │   AI Assistants,    │   │  Applications, APIs │
        │      Agents         │   │                     │
        └──────────┬──────────┘   └──────────┬──────────┘
                   │        (consumers)      │
                   └───────────┬─────────────┘
                               ▼
╔═════════════════════════════════════════════════════════╗
║                    DATAGATELODGE                         ║
║           AI Governance & Security Layer                 ║
║                                                          ║
║   Identity & Access Control                              ║
║   Policy Enforcement                                     ║
║   Budget Controls                                        ║
║   Audit Trail & Forensics                                ║
║   Request Logging                                        ║
║   Provider Governance                                    ║
║   Routing & Failover                                     ║
║   Secrets Management                                     ║
║   Compliance Evidence                                    ║
╚═════════════════════════════════════════════════════════╝
                               │
         ┌──────────┬─────────┼──────────┬─────────┐
         ▼          ▼         ▼          ▼         ▼
     ┌───────┐ ┌─────────┐ ┌───────┐ ┌───────┐ ┌────────┐
     │OpenAI │ │Anthropic│ │Gemini │ │ Azure │ │ Vertex │
     └───────┘ └─────────┘ └───────┘ │ OpenAI│ │   AI   │
                                    └───────┘ └────────┘
         ┌─────────┐  ┌──────────────┐  ┌────────────────┐
         │ Bedrock │  │ Any OpenAI-  │  │ Local / private│
         │ Converse│  │ compatible   │  │ models — run   │
         └─────────┘  │ endpoint     │  │ in the binary  │
                      └──────────────┘  └────────────────┘
```

Scope note for the speaker: this governs every AI interaction your
**applications and agents make via API**. AI embedded inside SaaS
products (M365 Copilot etc.) does not route through you — say so
plainly if asked; it is the honest boundary and the reason the
problem statement resonates.

## SLIDE: Security concerns mapped to controls

| CISO concern | DataGateLodge control |
|---|---|
| Shadow AI | Provider allowlisting — unapproved models don't even display |
| Data leakage | Governed routing — traffic only to approved providers, every request logged |
| Credential exposure | Centralized secrets — console-managed; key values never logged |
| Unauthorized access | Per-request identity — gateway keys with roles, RBAC, SSO from your IdP |
| Audit requirements | Tamper-evident audit chain — hash-linked; verification is a command you run |
| Incident investigation | Per-request forensic record — who, which model, when, cost, outcome |
| Regulatory compliance | Exportable evidence — structured JSONL into your SIEM |
| Third-party risk | Provider abstraction — applications don't change when providers do |
| Provider outage | Automatic failover; hedge routing races a slow primary |
| Cost abuse | Hard budgets per team and key — refusals, not warnings |

## SLIDE: Where DataGateLodge fits

```
   Identity Security          Network Security         API Security
 (Microsoft Entra / Okta)   (Firewalls / ZTNA)        (Kong / Apigee)
           └────────────────────┴─────────────────────┘
                                ▼
            ═════════════════════════════════════
                   AI GOVERNANCE LAYER
                      DataGateLodge
            ═════════════════════════════════════
                                │
        ┌───────────┬───────────┼───────────┬────────────┐
        ▼           ▼           ▼           ▼            ▼
     OpenAI     Anthropic     Gemini    hyperscaler   local
                                       AI services   models
```

**Key message:** Twenty years ago organizations added firewalls.
Ten years ago they added API gateways. Today they need governance
controls for AI traffic.

## Board-level statement

> "AI is becoming strategic infrastructure. Strategic infrastructure
> requires governance. DataGateLodge provides a single security and
> governance control point for every AI interaction your applications
> and agents make across the enterprise."

## Talk track (3–5 minutes)

**Opening.** "When most organizations start using AI, they connect
directly to model providers. Developers wire applications to OpenAI.
Data science uses Anthropic. Another team adopts Gemini. Product
experiments with local models. Every initiative makes sense alone —
together, AI adoption becomes fragmented, and security inherits
multiple trust boundaries, multiple credential stores, and multiple
places where the audit trail isn't."

**The problem, precisely.** "The issue isn't that organizations use
AI. It's that AI traffic fragments: multiple providers, multiple API
keys scattered through code, multiple billing accounts, no consistent
visibility. So the questions security asks — which models are
approved, who is using them, what did it cost, can we enforce policy,
can we investigate, can we evidence — get scattered answers."

**The control point.** "DataGateLodge sits between your applications
and every AI provider. No application talks directly to a provider.
That one architectural decision is the product."

**Identity and access.** "Every request is associated with a known
identity — a user, a service account, an application, a team. Access
controls and provider restrictions are enforced centrally, not
re-implemented by every development team."

**Policy enforcement.** "Most organizations already have an AI policy.
The gap is enforcement. Here, policy is technical: approved providers
allowed, unapproved ones blocked — they don't even display. Model
allowlists. Hard budget caps per team and key — a refusal, not a
warning. The organization moves from policy documents to policy
enforcement."

**Credentials.** "Without a control point, provider API keys end up
inside applications, pipelines, and config files. DataGateLodge
centralizes credential management — the console holds the secrets,
applications hold nothing, and key values are never logged."

**Visibility.** "Because it sits in the request path, it becomes the
authoritative record of AI activity: which models, used by whom, for
which applications, at what cost, through which provider — one record
instead of five vendors' dashboards."

**Audit and forensics.** "When compliance, legal, or incident response
needs to reconstruct what happened, the evidence already exists: a
tamper-evident, hash-linked ledger where any edit anywhere in the
history breaks the chain — and verification is a command you run
yourself, not a promise from us. Per-request records export as
structured JSONL into your SIEM."

**Provider independence.** "Below the layer: OpenAI, Anthropic,
Gemini, Azure OpenAI, Vertex, Bedrock, any OpenAI-compatible
endpoint, and local models that run inside the binary. Governance
stays constant whichever model a team picks — and when a provider has
an outage or changes pricing, requests fail over before your users
notice."

**Positioning.** "DataGateLodge is not another AI application. It's
security infrastructure. Organizations added firewalls to govern
network traffic, API gateways to govern application traffic. This is
the governance control for AI traffic."

**Close.** "The question we ask is simple: if regulators, auditors,
the board, or an incident response team asked tomorrow for a complete
picture of AI activity across your organization — who used which
model, when, at what cost — could you produce it? If the answer is
no, you don't have an AI governance problem. You have an AI security
architecture problem. That's the gap this closes."

## Persona messaging — same product, four angles

Use these to tune the room (also mirrored on the site's capabilities
page):

- **CEO — the business case.** AI spend capped by policy, not hope.
  One governed perimeter instead of sprawl. A board-ready answer to
  "how do we govern AI?" without new headcount.
- **CIO — the architecture.** OpenAI-compatible drop-in: applications
  keep their SDKs. Swap providers, fail over, mix local and cloud —
  without touching code. Fits the existing stack: SSO from your IdP,
  telemetry into your observability.
- **CISO — the controls.** Approved providers only. Per-request
  identity. Centralized secrets. A tamper-evident audit chain you can
  verify yourself.
- **Audit / compliance — the evidence.** Hash-linked request ledger,
  verification as a command, JSONL export to your SIEM. Honest
  posture: no certification theater — we say exactly what we are.

## Claims discipline — never say these

Ground truth for whoever presents. Cutting these is what keeps the
deck survivable in a technical evaluation:

1. **No "threat detection."** We detect nothing. We govern, log, and
   evidence. Detection is a roadmap conversation, not a claim.
2. **No DLP claims.** We govern where data goes (approved providers
   only) and log the activity. We do not inspect content.
3. **"Tamper-evident," never "immutable."** Append-only and
   hash-linked: edits break the chain and are detectable. That is the
   honest, and stronger, statement.
4. **Metadata by default — content is never silently logged.** The
   default record is who / which model / when / cost / outcome / trace
   correlation. Prompt content is captured only if the customer
   explicitly enables the Content Vault (Enterprise add-on, shipped
   in v2.4.0): full-traffic records encrypted to keys only the
   customer holds, searchable by user without decrypting content,
   escrowed so no single lost key loses data. With the vault on, "we
   can show you the prompts" is a claim — the records exist, chain-
   verified, and open only to the customer's keys.
5. **API-driven AI only.** SaaS-embedded AI (M365 Copilot et al.)
   does not route through the gateway. Say it plainly — the honest
   boundary is why the control-point story is credible.
6. **Providers we actually ship:** OpenAI, Anthropic Messages,
   Gemini Interactions, OpenAI Responses, Azure OpenAI, Vertex AI,
   Bedrock Converse, RunPod, any OpenAI-compatible endpoint, embedded
   local GGUF. (Native translators in the product — the full row is
   real.)

## Shipped — say it as shipped (v2.4.0)

- **Content Vault (Enterprise add-on, shipped):** optional full-
  traffic capture — every request and response written to an
  encrypted, tamper-evident local vault, keyed to the customer's own
  keys so the gateway can write but never read. A separate search key
  lets auditors extract a specific user's records without decrypting
  content; escrow is built in (multi-wrap keys, canary checks) so no
  single lost key means lost data. Off by default — the metadata-only
  default posture is the product's standing posture. Chain
  verification needs no keys at all; keycheck proves possession
  offline without the private key ever touching the gateway. Full
  spec: specs/content-vault-spec.md.
