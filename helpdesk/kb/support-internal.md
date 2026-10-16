# Support playbook — internal, never served to customers

title: Support playbook — agent-first support operations
description: How support actually works here: the Gatekeeper agent is tier 1, the owner is tier 2. Escalation criteria, communication standards, and the Lodge Office workflow. Staff only.
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
