# FAQ — questions at the gate

title: FAQ — frequently asked questions about the trial, data privacy, self-hosting, providers, license keys, and support
description: Direct answers to the questions buyers and security teams ask most: what happens after the 30-day trial, whether data passes through DataGateLodge servers, self-hosting effort, supported providers, what a license key contains, trying before buying, and how to get support

## What happens when the 30-day trial ends?

Local inference keeps working — free, forever. Cloud serving pauses
until you enter a key: a one-time US$99 Community key, or an
enterprise license. The check runs locally; nothing phones home.

## Does our data pass through DataGateLodge servers?

No. Gatelodge is self-hosted — requests go from your gateway directly
to the providers you configured. There is no DataGateLodge SaaS;
nothing of yours transits our infrastructure.

## How much work is self-hosting, really?

One binary, one command, Windows or Linux. The admin console opens at
http://localhost:8010, provider presets add backends in a click, and
your apps point at the endpoint they already speak (OpenAI-compatible).

## Which models and providers does it support?

Ten backend types — embedded GGUF, llama-server, RunPod (pods and
serverless), OpenAI, Anthropic, Gemini, OpenAI Responses, Azure OpenAI,
Vertex AI, and Bedrock — all behind one OpenAI-compatible endpoint,
swappable per task with alias chains.

## What's actually in a license key?

A signed set of claims: edition, expiry, entitled providers, and
feature flags. Keys never display secrets, hot-reload without a
restart, and are revocable through a tamper-evident revocation list.

## Can we try it before talking to anyone?

Yes. The Community binary is the full product on a 30-day trial —
download it from the Download page at datagatelodge.com. Or ask this
agent: it runs through Gatelodge itself.

## How do we get support?

support@datagatelodge.com, the chat widget on datagatelodge.com, or
enterprise support with your license. We answer within a business day.

## What happens if a provider goes down?

Alias chains carry fallbacks. When the primary provider errors or times
out, requests automatically fail over to the backup model — before your
users notice an outage. Hedge routing can race two providers when
latency matters most. A provider outage becomes a routing event, not an
incident.

## What is an alias?

An alias is a logical model name your applications call — for example
"smart" — that maps to a real model (or a chain of fallbacks) behind the
scenes. Applications integrate once against stable alias names; you
re-route what those aliases mean in the console without touching
application code.

## What is semantic caching?

A cache that recognizes when a request is effectively the same as one
already answered — and returns the cached response without billing the
provider. For repetitive workloads (support answers, policy lookups,
internal knowledge queries), this cuts both cost and latency
substantially. Cache hits never bill.

## Can we export the audit log?

Yes. Per-request activity is written as structured JSONL you can ship to
your SIEM or analysis pipeline, and the tamper-evident chain integrity
can be verified on demand from the console. Your audit evidence belongs
to you.

## What happens when a budget runs out?

Budgets are hard limits. When a team's monthly token budget is
exhausted, further requests for that team are denied until the budget
window resets — no surprise overage, no silent throttling. Per-session
budgets cap individual long conversations the same way.

## Can we use local models and cloud models together?

Yes — and it is a common pattern. Route high-volume, simple, or
sensitive workloads to local models running inside the Gatelodge binary
itself (no GPU cluster required for small models), and reserve premium
cloud models for the workloads that need them. The routing is per alias,
so the split is a configuration choice, not an architecture change.

## Can we switch providers without changing application code?

Yes — that is the point of the alias layer. Applications call alias
names; you change what those aliases route to in the console. Provider
switching, model upgrades, and cost-driven re-routing are all
configuration, not code changes.

## Does Gatelodge support single sign-on?

Yes — SSO via OIDC against your existing identity provider, SCIM
provisioning for user lifecycle, and role-based access control with
distinct roles for administration, read-only operations, key management,
and auditing. Your identity system remains the source of truth.

## Is Gatelodge SOC 2 certified?

Not yet. Our security posture today is self-hosted deployment (your
perimeter, your data residency), the tamper-evident audit chain, signed
license and revocation infrastructure, and centralized credential
management. Formal certification programs are on the roadmap — if
certification is on your critical path, our team will give you a
straight answer on timing.
