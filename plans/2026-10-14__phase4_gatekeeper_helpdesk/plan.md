# Phase 4 — The Gatekeeper: AI help-desk agent through the Gatelodge gateway

**Scoped 2026-10-14.** Owner decisions: site chat widget · hybrid autonomy
(confident answers send, uncertain collect for review) · knowledge base
covers pre-sales AND technical support.

## What we're building

A customer-facing AI assistant on datagatelodge.com whose every LLM call
routes through a Gatelodge instance — the product answering questions
about itself, under its own governance. Working name: **the Gatekeeper**.

## Architecture (v1)

```
datagatelodge.com chat widget (gatehouse-styled, SSE)
        │  https://chat.datagatelodge.com
        ▼
helpdesk service (FastAPI, Docker, VM)  ──►  Postgres (conversations,
  │                                         messages, review queue — f-7)
  │  OpenAI-compatible call, alias "helpdesk"
  ▼
Gatelodge closed-edition container (v2.3.0, VM, internal-only)
  │  alias chain: triage → groq small · answer → groq gpt-oss-20b
  │  helpdesk key with monthly budget hard-stop · allowlist: groq only
  │  audit chain logs every customer exchange
  ▼
groq
```

The gateway deployment doubles as the first production run of the closed
Docker artifact — dogfooding the exact binary we sell.

## Surfaces & flows

- **Widget**: fixed bottom-right brass gate button on datagatelodge.com;
  opens the chat panel; Turnstile on open; per-IP rate limits; SSE
  streaming answers.
- **Two-stage answer**: (1) triage call on the cheap model classifies the
  question, picks the relevant KB doc, decides answerable/not; (2) answer
  call on the bigger model with the selected doc. Both through the gateway.
- **Hybrid autonomy**: the answer call self-rates confidence (structured
  output). Confident → streams to the customer. Not confident → the agent
  asks for an email ("let me get this right for you"), the transcript is
  emailed to support@datagatelodge.com via Resend and parked in the review
  queue. Hard rule: EULA/legal interpretation questions ALWAYS escalate.
- **Human handoff**: a "talk to a human" affordance in the widget at any
  point — same collect-email-and-escalate path.
- **Review queue**: conversations in Postgres; simple keyed admin endpoint
  lists pending reviews. (Pretty UI is v2.)

## Knowledge base (helpdesk/kb/, curated markdown — no vector DB in v1)

| doc | contents |
|---|---|
| presales.md | what Gatelodge is, editions, pricing, trial terms (distilled site copy) |
| licensing.md | purchase flow, key install, trial→paid, lost-key recovery |
| eula-summary.md | key EULA points, plain language; legal interpretation → escalate |
| console.md | console tiles, quick-add presets, where things live |
| backends.md | the 10 backend types, config shapes, provider setup |
| governance.md | gateway keys, RBAC, budgets, audit chain, don't-display |
| troubleshooting.md | trial expiry, invalid key, backend 401s, local models, common errors |

Corpus is small (~tens of KB) — triage selects a doc, answer model reads
it whole. Embeddings/vector search deferred until the corpus outgrows
context windows.

## Anti-abuse & cost

Turnstile gate on widget open · per-IP rate limits (as licensing service)
· max conversation length · gateway budget hard-stop on the helpdesk key
(monthly cap) · allowlisted backends (groq only) · don't-display covers
everything else.

## Out of scope for v1 (explicit)

Inbound-email agent (auto-reply on support@) · voice · multi-language ·
vector RAG · analytics beyond the review list · SSO on admin · pretty
admin UI. All deferred, all compatible with this architecture.

## Build order

1. KB corpus (write from known material — site, EULA draft, repo docs)
2. helpdesk service (sessions, chat SSE, two-stage, confidence, Postgres)
3. VM gateway (closed image + enterprise license + groq aliases + budget key)
4. Widget (chat.js + styles, gatehouse aesthetic, config-gated)
5. Tests (in-process with a mock gateway + container rehearsal)
6. Deploy: chat.datagatelodge.com tunnel hostname, Postgres wiring,
   end-to-end live check + a review-queue dry run
