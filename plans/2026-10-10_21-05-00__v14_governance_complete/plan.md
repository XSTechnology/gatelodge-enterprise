# v1.4 "Governance Complete" — SCIM, chained audit, session budgets, credential chains

Closes every SOC2 gap in plans/2026-10-10_21-00-00__soc2_type1_scoping/.
Upstream-first where core changes; fork modules via the seams.

## Workstreams

### 1. `set_extra_router` — the FOURTH seam (upstream)
The insertion point for enterprise API surfaces (SCIM now, the MCP
gateway in v2.0). Core mounts nothing itself.

    from gateway.app import set_extra_router
    set_extra_router(scim_router)          # fork startup, before serving

Design: `set_extra_router(router: APIRouter) -> None` in gateway/app.py
— idempotent (second call replaces), calls app.include_router(router).
Canary: include a probe router, assert the route serves 401-through-
pipeline (NOT bypassing auth — mounted routes join the SAME middleware
stack; SCIM must go through its own bearer scheme per RFC 7644 §2).
**Auth question to resolve at implementation: SCIM endpoints sit
outside /admin (no admin-gate) and outside /v1 — the pipeline's
links all no-op on /scim/*; the SCIM router must enforce its own
bearer (the IdP's SCIM token) inside its endpoints.**

### 2. SCIM 2.0 provisioning (fork: gateway_enterprise/scim.py)
RFC 7644 minimal viable surface, IdP-driven:
- POST /scim/v2/Users (create) — maps to a virtual key mint
  (owner = displayName; plaintext key returned in the SCIM response
  per the once-only convention? NO — SCIM never carries the gateway
  key; the response carries the user record; the key material goes to
  the enterprise config/secret store. DECISION at implementation.)
- PATCH /scim/v2/Users/{id} (active:false) — revoke the key.
- GET /scim/v2/Users?filter=... — list for IdP sync.
- DELETE — revoke.
Backed by the config store (api_keys entries, CAS via the existing
contract). Bearer: config `scim.token_hash` (hashed at rest like every
other credential).

### 3. Hash-chained audit (upstream — audit is core behavior)
_audit_config (and the rbac-deny writer in auth.py) append
tamper-evident records: each line gains `prev` (sha256 of the previous
line) + `hash` (sha256 of itself incl. prev). New helper in app.py
(_audit_chain_head under the storage seam? NO — audit stays
per-replica by design; the chain head is per-process memory, chain
validates per file). Verification: a small admin/console check or
script that walks a chain and reports breaks. Docstring note: the
chain detects tampering with the local file; multi-replica chains are
per-replica (SIEM remains the aggregate).

### 4. Session budgets (upstream charge hook + fork gate)
- UPSTREAM: _charge_request gains an optional session scope — when a
  `session:<id>` marker is derivable (x-session-id header, config
  `session_budgets.enabled`), charge `session:{id}` alongside
  `key:{kid}` (same _charge_scope machinery; the log line gains the
  session id).
- FORK: a pre-gate link in gateway_enterprise (session_budget_link)
  reading session spend vs a `session_budgets` config section
  ({default_tpm, per_tenant overrides}) — same shape as the TPM gate,
  riding storage (multi-replica session governance for free).

### 5. Cloud credential chains (upstream)
Backends gain credential providers alongside static api_key_env:
- `auth: sigv4` — AWS SigV4 signing (Bedrock) from the standard
  credential chain (env vars / instance profile).
- `auth: adc` — Google Application Default Credentials (Vertex) →
  exchanged for an access token, cached until expiry.
Implementation: gateway/cloud.py provider hook; no static secrets at
rest for cloud backends — the SOC2 story strengthens.

## Sequencing (each = commit + live verify)

1. [v14-1] set_extra_router upstream + canary; merge to fork.
2. [v14-2] Hash-chained audit upstream (+ chain verifier + tests);
   merge; verify tamper detection live.
3. [v14-3] Session charge hook upstream (+ tests); merge.
4. [v14-4] SCIM router in the fork via the seam (+ stub IdP SCIM
   endpoints in scripts/stub_idp.py for live verify).
5. [v14-5] session_budget_link in the fork (storage-backed gate).
6. [v14-6] Credential chains upstream (sigv4 + adc) with mocked
   chains in tests; live verify optional (no cloud creds on box).
7. [v14-7] v1.4.0 tag both repos; SOC2 evidence period starts.

## Out of scope (v2.0 "The Gatehouse")
MCP gateway (needs the same seam + scope sign-off — pending), OTel
tracing, semantic caching, teams/workspaces.
