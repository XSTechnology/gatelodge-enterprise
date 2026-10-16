# Governance pass + intent hardening + portal polish + estate hygiene

Owner order: 1 (governance), 2 (intent), 3 (portal), 4 (hygiene). Each phase
ships live before the next begins.

## Phase 1 — Lodge Office governance pass (lodge-office:13 + gatekeeper:18)

1a. Security linting of KB docs + prompt templates
- Regex lint engine in admin/app.py: secrets (sk-/ghp_/AKIA/key=value),
  private nets (10.x, 192.168.x, localhost:port), non-public emails,
  phone numbers, internal-mention phrases in PUBLIC docs, TODO/FIXME.
- Gatekeeper: GET /api/kb/prompts (review-key) returns its prompt
  constants (name + text). Admin lints them with the same engine.
- Lodge Office Knowledge screen: per-doc lint badge + prompts section
  with findings; findings also shown on the doc edit page.

1b. KB provenance
- kb_history table (name, action save|create|reset|restore, old_content,
  new_content, created_at). Written on /kb/new, /kb/save, /kb/reset,
  restore.
- Edit page: history list with view + restore (restore writes an
  override + history row + triggers re-analysis).

1c. Rulesets CRUD screen
- /policies list (name, kind, version, enabled, rules count) from the
  shared rulesets table. /policy/{id} editor: name/description/enabled,
  column + rule grid (adapted from the portal editor), version bump on
  save, zen-engine test box (zen added to admin requirements).
- Danger framing: these policies gate live agent behavior.

1d. Inbox fired-rule column
- inbound_emails.rule already recorded — surface it in the Inbox rows.

Ship: suites extended (admin +, helpdesk + for the prompts endpoint),
deploy, live verify.

## Phase 2 — Intent engine hardening
- Escalation criteria as data: escalation_topics table (topic, guidance,
  enabled) rendered into RERANK_SYSTEM at request time; Lodge Office
  editing; seeded with certification/pricing-negotiation/incident/EULA.
- Routing thresholds as settings: agent_settings table (key, value) for
  the inline chat/email constants; Lodge Office Settings editing.
- Chat widget suggestions: gatekeeper includes 2-3 related questions
  from the routed doc's intent map in the SSE done event; chat.js v4
  renders clickable chips (asset bump).

## Phase 3 — Portal polish
- Policy editor upgrade (grid CRUD completeness; evaluate gorules
  jdm-editor embed vs improved grid — decide by fit, no npm build step
  in the portal if avoidable).
- get./chat. root redirects: licensing / → site buy path; gatekeeper /
  → datagatelodge.com (widget lives on the site).

## Phase 4 — Estate hygiene
- Staging refresh: sync site/ -> sites/datagatelodge (RESTORE Dockerfile
  after /MIR), commit, Gitea push, Coolify rebuild.
- xs.technology mail routing cleanup (inspect CF email routes).
- Repo tidy: decide untracked leftovers (tasks.json, tuning.docx, old
  plan dirs, stray "Gatelodge" file) — inspect, then commit/ignore.
