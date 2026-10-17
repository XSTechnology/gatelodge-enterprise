# Evidence Center — spec (M1, v2.5.0)

Status: DRAFT for owner Go · 2026-10-17
Owner decision: Evidence Center first (sequencing review, 2026-10-17)

## What this is

An auditor-ready evidence capability in two layers:

1. **M2 (standard repo, both editions): chain the request ledger.** The
   per-request metadata log (gateway-log-YYYY-MM.jsonl) becomes
   tamper-evident with the same proven pattern as config-audit.jsonl
   (prev/hash, sha256 canonical JSON, restart-continuing). This closes a
   claims gap: the site, exec-brief modal, and deck already say "hash-
   linked request ledger"; today only config-audit is chained.
2. **M3 (fork, Enterprise): the Evidence Package.** One command assembles
   a self-describing zip: ledger extracts + chain proofs, config-audit
   chain, license state, AI asset inventory, governance posture report,
   and a control mapping (SOC 2 TSC / ISO 27001 Annex A / NIST AI RMF /
   EU AI Act) — everything an auditor asks for, in one artifact.

Non-goals (claims discipline — hard lines):
- We produce evidence FOR the customer's audit. We do not certify,
  attest, or imply compliance achieved. No "SOC 2 certified" anything.
- No risk scoring beyond derived facts (counts and states). No invented
  numerology.
- No content inspection. The vault (if on) contributes ciphertext-chain
  verification only — never decrypted content, never keys.

## Ground truth (what exists today)

| artifact | chained? | notes |
|---|---|---|
| config-audit.jsonl | YES (v1.4) | prev/hash canonical sha256; verify_audit_chain(); /admin/audit; console verify-on-open; SIEM webhook ships chained records |
| gateway-log-YYYY-MM.jsonl | NO | month-scoped metadata lines: ts/model/backend/stream/ms/usage/status/err/key/session/team/cost_usd/req/trace |
| gateway-rollup-YYYY-MM.json | derived | aggregates for /admin/stats — NOT evidence, excluded |
| vault parts | YES (v2.4) | chain over ciphertext; verify needs no keys |
| license + CRL | signed | RS256 JWT, revocation list versioned + signed |
| spend ledger | in-memory + rollups | per-key/team/provider budgets |

## M2 — request-ledger chaining (gatelodge_standard)

Record format: every request line gains `prev` (previous chained
record's hash; "genesis" for the first) and `hash` (sha256 over the
record with hash excluded, canonical sorted-keys JSON — identical rule
to config-audit so one mental model covers both chains).

- **Chain head:** in-process global under _log_lock; on cold start (or
  month rotation into an empty file) resolved by tail-reading the newest
  non-empty month file, walking back through months if needed; genesis
  only when no history exists. The chain therefore continues across
  restarts AND across month files — "any edit anywhere breaks the chain"
  is literally true.
- **Rotation:** month files rotate as today; the first record of a new
  month carries the last hash of the previous month. Rollup files stay
  derived (excluded from the chain; posture reports them as aggregates).
- **Legacy records** (pre-v2.5 lines without `hash`): counted, not
  verified; the chain restarts at genesis after them — the exact rule
  config-audit's verifier already uses.
- **Hot path:** one dict copy + one sha256 per request inside the
  existing _log_lock critical section — the same cost class as
  config-audit appends. No new I/O.
- **Verifier:** verify_request_chain() walks archive (if present) then
  month files in chronological order; returns {files, lines, chained,
  legacy, breaks, valid}. Exposed at GET /admin/ledger (verify result +
  per-file counts) and folded into the console's audit panel next to
  the config-audit chain.
- **Tests:** chain continuation across restart (new writer over same
  file), across month rotation, tamper detection (edit a line → break),
  legacy records counted-not-verified, open-mode and keyed records,
  perf smoke (10k appends < 1s).

Community ships this too — the audit chain is core governance, and the
free binary stays "the full governance stack."

## M3 — the Evidence Package (gateway_enterprise/evidence/)

License claim: `evidence` (FEATURE_CODES; enterprise default-on).
Gating mirrors the vault: licensed + configured surface mounted; the
community binary never assembles packages.

### Package contents (v1)

```
evidence-YYYYMMDD-HHMMSS.zip
├── README.md            what this is, how to re-verify, what it is NOT
├── manifest.json        package id, generated_at, gateway version,
│                        license serial/edition, period, file list + sha256
├── ledger/
│   ├── gateway-log-<months>.jsonl    period extract (raw, chained)
│   └── ledger-verify.json            verify_request_chain() over the extract
├── config-audit/
│   ├── config-audit.jsonl            full chain (config mutations + RBAC denials)
│   └── config-audit-verify.json      verify_audit_chain() result
├── license/
│   └── license.json                   public claims (serial, edition,
│                                       features, exp) + CRL version/state
├── inventory.json                     AI asset inventory (below)
├── posture.json + posture.md          governance posture — derived facts only
├── controls.md                        control → framework mapping
└── vault/                             only when the vault is enabled
    ├── vault-status.json              mode, key versions, records, tombstones
    └── vault-verify.json              ciphertext chain verification (no keys)
```

### inventory.json — the AI asset inventory

Derived from live config state; NO key material (ids, roles, hashes'
first 8, budget values only):
- models: alias → backend/type/remote name, local_only flags, hedge
- api_keys: id, role, budgets (token/usd), rate limits
- teams + workspaces (enterprise), members counted
- providers: backend name, type, entitlement state (licensed/not),
  auth mode (api-key/sigv4/adc — never the secret)
- spend month-to-date per scope

### posture.md — facts, not scores

A checklist a CISO can read in 90 seconds, each line citing its source
artifact: edition + license mode + serial; vault on/off + mode + key
versions; budgets enforced (count of keys/teams with hard stops);
unapproved-model attempts blocked in period (count from ledger);
chain states (request ledger / config-audit / vault: valid + counts);
verify-scheduler failing backends; local_only aliases (privacy); spend
MTD vs budgets. Every number is a query over included artifacts —
nothing hand-waved.

### controls.md — the mapping

Static table, "supports" language only:
- control → what Gatelodge does → artifact in this package that
  evidences it
- mapped to SOC 2 TSC (CC6.1 logical access, CC7.1 monitoring,
  CC7.2 anomaly evaluation, CC8.1 change management), ISO 27001
  Annex A (A.8 logging, A.5 access, A.8.9 access control), NIST AI RMF
  (GOVERN/MAP/MEASURE functions), EU AI Act (Art. 12 record-keeping,
  Art. 15 accuracy/robustness logging context)
- explicit "not provided" section: certification, attestation, content
  inspection, threat detection — stated plainly

### Surface

- `GET  /admin/evidence/status` — what a package would contain now
  (chain states, counts, inventory size); no assembly
- `POST /admin/evidence/package` {from, to} — assembles to
  data/evidence/evidence-*.zip, returns the manifest; admin-gated
  (same mirror pattern as /admin/vault)
- Offline CLI (customer-side, no license needed): the package carries
  its own proofs — `python -m gateway_enterprise.evidence.tools verify
  <zip>` re-checks manifest sha256s + re-runs chain verification over
  the included extracts. An auditor without our binary can still
  verify hashes with any sha256 tool; the canonical-JSON rule is
  documented in README.md.

### Ops notes

- On-demand only; the bundler streams month files (no full in-memory
  copies); period capped at 13 months per package.
- Packages land in data/evidence/ (operator retrieves; we never
  transmit). Self-hosted always — there is nowhere to send it.

## M4 — ship (v2.5.0)

- Suites (standard + fork), tag, merge, re-tag, build both binaries.
- Verify ports + smoke ×3 (8015-8017 pattern), standing 8010/8011.
- Dogfood first: generate our OWN estate package and include the
  ledger-verify output as the site/deck artifact (the vault-terminal
  pattern — real output, no mockups).
- Messaging: editions li + compare row (Enterprise), evaluation.md
  present tense, deck "Shipped" section append, site evidence note —
  page-fit discipline says MODAL, not inline (vault-modal precedent).
- KB: new evidence.md doc (both contexts) + gatekeeper/lodge-office
  rebuilds + re-analyze.
- Community zip rebuild + licensing mount redeploy (release process).

## Test plan

- M2: 8 tests (standard) — chaining, restart continuation, month
  rotation, tamper, legacy, verifier, perf, rotation+restart combined.
- M3: 10 tests (fork) — package assembly golden-path, manifest hashes
  correct, period extraction, inventory redaction (no key material),
  posture facts match a driven fixture, controls.md present + language
  lint ("certified" must not appear), vault-off package shape,
  keycheck-free vault section, CLI re-verify of a package, unlicensed
  surface absent.
- E2E: enterprise binary on a verify port → drive traffic → assemble
  package → CLI re-verify → tamper a byte → re-verify FAILS.

## Decision points for the owner

1. Go / no-go on this spec (M2+M3+M4 as v2.5.0).
2. License claim name `evidence` (my recommendation) vs folding into
   an existing claim.
3. Community keeps chained ledgers (M2 ships to both editions) —
   confirm; my recommendation stands (the free tier is "full
   governance", packaging is the paid part).
