# Support playbook — internal, never served to customers

title: Support playbook — agent-first support operations
description: How support actually works here: the Gatekeeper agent is tier 1, the owner is tier 2. Escalation criteria, communication standards, the Lodge Office workflow, the diagnostic ladder, and a quick-reference card. Staff only.
internal: true

## Our support model is agent-first

We do not run a traditional multi-tier support desk. The Gatekeeper
agent IS tier 1: it answers product and documentation questions
confidently from the governed knowledge base, makes deterministic
policy decisions (refunds, eligibility), and escalates anything it
cannot answer with the full transcript attached.

The owner (you) are tier 2: the Lodge Office shows every conversation,
every review queue item, every inbound email with the agent's triage
and the rule that fired.

## What the agent handles (tier 1)

- Product, pricing, licensing, governance, backends, console, EULA,
  and troubleshooting questions answered from the knowledge base —
  cited, with sources.
- Refund and eligibility decisions via the deterministic policy: it
  extracts the facts, asks for what is missing, and the rules engine
  decides. The agent phrases; the policy decides.
- Inbound email: triaged by policy, answered when confident, queued to
  the Inbox when not.

## What always escalates to the team (tier 2)

The agent never improvises on:

- **Certification and compliance status** — SOC 2, ISO, audit
  certifications. It collects the visitor's email; we answer with our
  actual posture.
- **Pricing negotiations and discounts** — the agent quotes published
  pricing only.
- **Legal interpretation** — EULA/contract language questions.
- **Security incidents** — any report of a security issue.
- **Anything the knowledge base does not confidently cover** — by
  design, low confidence means escalation, not a guess.

## The tier-2 workflow (Lodge Office)

- **Inbox** — inbound email the agent queued, with topic and the triage
  rule that fired. Reply from the Compose screen.
- **Conversations** — chat transcripts flagged for review; the agent's
  answer and confidence are shown.
- **Reviews** — escalation requests with visitor email attached.
- **Knowledge** — when the same gap appears twice, write the answer
  once here; the agent learns it on the next question.

## Communication standards

- **Own the problem.** From intake to resolution. Never "that's not my
  area" — "I'll get you an answer and stay with it."
- **Business impact first.** Who is affected, what is broken, how
  severe — before architecture.
- **Facts over assumptions.** Never speculate about root cause. "We
  have not identified the cause yet; current investigation includes X"
  beats a wrong guess.
- **Update cadence.** Silence creates uncertainty. Commit to a next
  update time and keep it.
- **Document everything.** Fixed an issue? Add it to the knowledge
  base in the same session. Every resolved ticket should leave the KB
  better than it found it.

## Ticket classification (adapt to real volume)

- Authentication/SSO — login, RBAC, identity integration
- Routing/provider — model selection, failover, connectivity
- Governance — budgets, teams, policies, permissions
- Audit/compliance — records, export, verification
- Performance — latency, timeouts
- Licensing — keys, activation, recovery

Priority: production outage or auth unavailable is critical (immediate);
degradation or audit issues high; user-specific issues normal;
documentation questions routine.

## The diagnostic ladder

Technical tickets are diagnosed outside-in, one layer at a time.
Isolate the layer before changing anything; never fix two layers at
once. The order matters because each layer hides the next:

1. **User** — exact reproduction: who, what they did, the error text
   verbatim, since when, and the blast radius (one user, one team,
   everyone). Half of all tickets end here — expired trial, file in
   the wrong folder, typo'd model name.
2. **Auth** — which credential failed? Console login (admin or
   gateway key), license (expired trial, model not named on it),
   portal key (paused or regenerated client). A provider 401 is our
   stored credential being missing or wrong — not theirs.
3. **Context** — what did the request run as? Team and key decide
   budgets, permissions, and policies. "It randomly refuses" usually
   means "the rule is on that team." Portal tickets: which client,
   and which ruleset fired.
4. **Alias** — resolve the alias chain before suspecting anything
   deeper (Services tile). "Model not in catalog" is usually the
   alias pointing at a model that is not configured or not named on
   the license.
5. **Policy** — is a governance rule refusing this on purpose?
   Provider blocks, model allowlists, tool gates, decision policies.
   Refusals are logged with the rule that fired — find it before
   blaming the network.
6. **Budget** — hard caps, not warnings. Check the team or key cap
   in the Governance tile. A budget error is the budget working.
7. **Routing** — did the request go where expected? With failover
   and hedges, "the provider is down" is often "the fallback chain
   needs another hop." Establish which backend actually served.
8. **Provider** — only now suspect the upstream: provider 5xx,
   capacity, model deprecations. Confirm the provider returned the
   error, rather than our router labeling it.
9. **Audit** — the append-only request log arbitrates every dispute.
   Customer says the gateway ate it; provider says they never saw
   it; the chain says what actually happened. End every diagnosis
   here — "confirmed by the log" is the difference between a guess
   and a diagnosis.

Customer-facing fixes for the common cases live in the public
troubleshooting doc (that is layer 1 rendered as answers). This
ladder is how we get there without shotgunning.

## Golden rules

1. The agent answers what it knows and escalates what it does not —
   never the reverse.
2. Lead with the customer's business problem, not our architecture.
3. Never speculate — on causes, on certifications, on roadmap dates.
4. Every escalation arrives with evidence: transcript, timestamps,
   what was already tried.
5. Every resolved gap becomes a knowledge-base entry the same day.
6. The product is governance — support should feel governed too:
   accountable, traceable, honest.

## Quick reference card

One glance, mid-incident:

- **Ladder** — User · Auth · Context · Alias · Policy · Budget ·
  Routing · Provider · Audit. Outside-in, one layer at a time, end
  at the log.
- **Priority** — production outage or auth unavailable: critical,
  immediate. Degradation or audit issues: high. User-specific:
  normal. Documentation: routine.
- **Always escalates to us** — certification/compliance status,
  pricing negotiations, legal interpretation, security incidents,
  anything the knowledge base does not confidently cover.
- **Where to look (Lodge Office)** — Inbox: email the agent queued,
  with the triage rule that fired. Conversations: chat transcripts
  flagged for review. Reviews: escalations with visitor email
  attached. Knowledge: write the answer once, the agent learns it.
  Tasks: agent runs, step by step.
- **Ground truth** — request log `data/gateway-log-*.jsonl`
  (append-only, hash-linked); service log
  `data/gateway-service.log`; `GET /healthz` (authenticated) for
  status, version, license mode, backend summary.
- **How we sound** — own the problem; business impact first; facts
  over assumptions; commit to the next update time; fix goes into
  the knowledge base the same day.
- **Support address** — support@datagatelodge.com. Inbound lands in
  the Inbox after agent triage; confident questions are answered
  before we ever see them.
