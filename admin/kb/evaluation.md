# Evaluating an AI gateway — the questions to ask

title: Evaluating an AI gateway — the due-diligence questions and our answers
description: The questions security, compliance and architecture teams should ask when evaluating any AI gateway — audit chain verification, budget enforcement, secret management, failover, identity, certifications — answered specifically for Gatelodge.

## The questions that matter

When your organization evaluates an AI gateway — ours or anyone's — these are the questions your security, compliance, and architecture teams should ask. We answer each one specifically, because "it depends" is not governance.

## How does the audit chain actually verify?

Every request that passes through Gatelodge appends a record to a local, append-only ledger. Each record carries the cryptographic hash of the record before it — so any edit, anywhere in the history, breaks the chain. The console shows chain integrity directly, and verification is a command you can run yourself, not a promise from the vendor. Per-request logs are also written as structured JSONL you can export into your own SIEM or analysis pipeline.

Licenses and key revocations are signed with public-key cryptography and checked against a tamper-evident revocation list — a revoked key stops working even if someone replays an old license file.

## How are budgets enforced?

Budgets are hard limits, not warnings. Teams get monthly token budgets; when a team's budget is exhausted, further requests are denied — not throttled, not "overage billed." Per-session budgets cap individual long conversations. The console shows spend against budget in real time, per team and per backend, so cost attribution answers "which workload, which team, which model."

## How are secrets managed?

Provider credentials resolve from environment variables, files, commands, or the console-managed store — your choice. Secrets are never written to logs and never committed. Applications never see provider credentials at all: they authenticate to Gatelodge with their own keys, and Gatelodge holds the provider credentials centrally. One place to rotate, one place to audit.

## What happens when a provider fails?

Alias chains route by task, and every alias can carry fallbacks — when the primary provider errors or times out, requests fail over to the backup automatically, before your users notice. Hedge routing races two providers when latency matters most. Provider outages become routing events, not incidents.

## Can we block providers we have not approved?

Yes. The backend filter enforces which providers may be configured at all. Unlisted models never render — a request for one returns the same unknown-model error as any typo, so users cannot even discover what is not approved.

## How does identity work?

Single sign-on via OIDC against your existing identity provider, SCIM provisioning for user lifecycle, and role-based access control with distinct roles for administration, read-only operations, key management, and auditing. Team workspaces mirror your organizational structure, each with their own models, budgets, and permissions.

## Is it certified (SOC 2, ISO)?

Not yet. Our posture today: self-hosted deployment (your perimeter, your data residency), the tamper-evident audit chain, and signed license/revocation infrastructure. Formal certification programs are on the roadmap. If certification is a hard requirement for your timeline, talk to our team and we will be straight with you about where we are.

## How does it scale?

A single binary boots in one command and serves most deployments. For higher availability, gateway replicas share state over Redis, so you can run multiple instances behind a load balancer with consistent budgets, keys, and audit.

## What should you ask any vendor?

The checklist: How is audit integrity verified — by whom, with what command? Are budgets hard limits or warnings? Where do provider credentials live, and who can read them? What happens on provider failure — automatically, or after a ticket? Can unapproved providers be blocked outright? Who holds the encryption keys? Where does the data physically flow? If a vendor cannot answer these specifically, keep evaluating.
