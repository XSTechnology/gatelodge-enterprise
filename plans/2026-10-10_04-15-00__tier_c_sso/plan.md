# Tier C — SSO/OIDC via the auth seam (sso_link)

Enterprise admin authentication against an external IdP (OIDC), with
token introspection (RFC 7662), tenant→role resolution, and real
identity in the enterprise audit trail. Zero edits under `gateway/` —
everything lands in `gateway_enterprise` + fork-owned root files.

## Grounding (what the core actually does today)

- **Pipeline** (`gateway/auth.py`): links are sync OR async
  (`asyncio.iscoroutine(result)` → awaited); the middleware awaits
  `get_auth_pipeline().run(request, cfg)`. Links may update
  `_REQUEST_KEY_ID`; the pipeline re-reads it between links.
- **The ordering trap**: the seam docstring's example order
  `[resolve_key_link, sso_link, ...]` CANNOT work — `resolve_key_link`
  returns 401 for any token not in `gateway_keys`/`api_keys`, so an
  OIDC bearer dies before sso_link runs. The working design: an
  enterprise resolve wrapper that dispatches OIDC-shaped tokens to SSO
  validation and everything else to the core `resolve_key_link`.
- **Identity downstream**: `_REQUEST_KEY_ID` is a plain string. Core
  reads it for: spend attribution (`_charge_request` skips
  `key_id == "admin"`), request log lines, config-audit lines
  (`"admin"`), model/budget gates (admin exempt). `admin_gate_link`
  requires `key_id == "admin"` on /admin. ⇒ an authenticated SSO
  principal must end up with `_REQUEST_KEY_ID == "admin"` to keep the
  core gates working; richer identity (sub/email/tenant/role) rides a
  NEW enterprise ContextVar.
- **httpx is already a dependency** (app.py imports it; async
  `httpx.AsyncClient` used throughout) — introspection needs no new
  packages.
- **Config normalization pattern**: `_normalize_guardrails` /
  `_normalize_smart_routes` show the house style for enterprise-ish
  sections (defaults, type coercion, drop-invalid-with-warning).
  Enterprise reads its OWN sections raw from `store.load()` (C-lite
  pattern) — no core changes needed.
- **C-lite rbac_link** (gateway_enterprise/links.py): derives role from
  the bearer token via `AdminRoleMap`; denials audited. Tier C extends
  this: SSO identity (when present) takes precedence for role + audit
  identity.
- **The empty-key fast-path hazard** (review finding): `admin_gate_link`
  contains `if not key_id: return None` — empty identity PASSES /admin.
  The core maintains the invariant that resolve only yields "" in true
  open mode; any enterprise link that returns None without setting an
  identity while keys are configured would leave /admin open. Tier C
  code must never do that (see fail-mode decision 6).
- **Console**: static shell at `/` prompts for a bearer "key" and sends
  it as `Authorization: Bearer ...` — an IdP-issued token pasted there
  flows through the same pipeline. No console changes needed.

## Design decisions

1. **Introspection over local JWT validation** (roadmap names it
   explicitly): POST the bearer to the IdP's introspection endpoint
   with client credentials → `{active, sub, email, ...claims}`. No new
   crypto/JWKS dependencies; works with opaque tokens; revocation
   checks are inherent. Local JWKS/JWT validation is a possible later
   add — the validator is a single function behind a small interface.
2. **Enterprise resolve wrapper replaces `resolve_key_link` in the
   pipeline** (not "sso after resolve"): `resolve_link` (async — the
   pipeline awaits coroutine results) checks the bearer — if it looks
   OIDC (configurable; default heuristic: compact-JWS shape `xxx.yyy.zzz`
   OR an `sso.token_prefixes` match) → validate via introspection, set
   identity + `_REQUEST_KEY_ID = "admin"`; otherwise delegate to the
   core `resolve_key_link` untouched. Gateway-key auth and open mode
   behave byte-for-byte as today. **Dispatch is shape-first**: a
   JWT-shaped `gateway_keys` entry would misroute to SSO — parse-time
   lint warns when any configured gateway_key looks OIDC while SSO is
   enabled (operator should rename the key or set token_prefixes).
   Virtual keys are always minted `vk-…` so they never collide.
3. **Identity ContextVar in enterprise code**
   (`gateway_enterprise/identity.py`): `_SSO_IDENTITY` ContextVar holding
   `{sub, email, tenant, role, via}` or None. Single definition, same
   discipline as the core's contextvars (one object, imported everywhere).
4. **Tenant/role resolution = data rules in the enterprise config**:
   `sso.role_rules: [{claim, value, role}]` evaluated first-match
   against the introspection claims; `sso.default_role` (default
   `ops_readonly` — fail LOW for unknown SSO users, unlike key-based
   RBAC where unassigned = full_admin: an IdP-driven surface must not
   grant full admin by default). Unknown role names fail closed
   (reuses rbac.ROLES validation).
5. **Introspection result caching** rides the storage backend
   (namespace `sso_introspect`, key = sha256(token), value =
   `{claims, cached_at}` with `cached_at` an EPOCH stamp; a read is a
   hit when `now - cached_at <= cache_ttl_s`, default 60, and the
   token's own `exp` claim (when present) caps the entry — entry dies
   at min(cached_at + ttl, exp)) — per-replica in-memory now, shared
   across replicas for free in tier D. EPOCH timestamps per the
   storage convention. Cache is positive-only (negative results are
   not cached — an IdP outage must not extend a denial).
6. **Fail mode**: introspection transport error → 503 `sso_unavailable`
   (+ Retry-After) by default — fail CLOSED (governance product).
   `sso.fail_open: true` must mean "behave as if SSO did not exist for
   this request": DELEGATE to core `resolve_key_link` (keys configured
   → its 401; true open mode → its pass). **Never return None from the
   SSO path with an empty `_REQUEST_KEY_ID` while keys are configured**
   — `admin_gate_link` has an empty-key fast-path (`if not key_id:
   return None`, an invariant the core maintains by only passing "" in
   open mode) and an enterprise-introduced "" would leave /admin OPEN.
   Invalid/inactive token → 401 `invalid_api_key` (same shape as core).
7. **rbac_link extension**: role resolution order = SSO identity role
   (when `_SSO_IDENTITY` is set) → C-lite token-based AdminRoleMap →
   DEFAULT_ROLE. Denial audit identity: SSO `email`/`sub` when present
   (`sso:<email>`), else the C-lite `admin:<role>:<hash8>`. This is the
   "identity in the audit trail" deliverable; core's own mutation lines
   still say `"admin"` (no audit seam in core — documented limitation,
   candidate for an upstream seam).
8. **Scope**: SSO governs the ADMIN surface. `/v1/*` consumers remain a
   virtual-key story (an SSO principal on /v1 is `admin` → exempt from
   key budgets/rate limits — acceptable; admins are trusted). Browser
   redirect flows (authorization-code login page) are OUT of scope —
   the console's key prompt accepts a pasted IdP token.
9. **Secrets**: `sso.client_secret_env` names an env var; enterprise_main
   loads it from the OS keyring (service 'Workshop', username = env var
   name) at startup when unset — same convention as the core's boot
   bridge. Never logged. **Missing secret with `sso.enabled` → SSO is
   treated as disabled with an ERROR-level startup log** (OIDC tokens
   then 401 via the core path) — fail closed for SSO, core auth and
   serving unaffected; never a silent no-auth boot.

## Config section spec (enterprise section, raw-read like admin_key_roles)

```json
"sso": {
  "enabled": true,
  "issuer": "https://idp.corp.example",
  "introspection_url": "https://idp.corp.example/oauth2/introspect",
  "client_id": "gatelodge-enterprise",
  "client_secret_env": "GATELODGE_SSO_CLIENT_SECRET",
  "audience": "",                      // optional aud claim check
  "timeout_s": 5.0,
  "cache_ttl_s": 60,
  "fail_open": false,
  "token_prefixes": [],                // e.g. ["oidc-"] forces SSO path
  "default_role": "ops_readonly",
  "role_rules": [
    {"claim": "groups", "value": "gatelodge-full-admins", "role": "full_admin"},
    {"claim": "groups", "value": "gatelodge-key-managers", "role": "key_manager"}
  ]
}
```

Rule matching: claim value string equality, or membership when the
claim is a list. First match wins; no match → `default_role`; role not
in `ROLES` → fail closed + warning at parse.

## New/changed files

**NEW `gateway_enterprise/identity.py`** — `_SSO_IDENTITY` ContextVar +
`get_identity()` / `set_identity()` helpers (single definition).

**NEW `gateway_enterprise/sso.py`** — `SSOConfig` parser (from raw
store.load() section, house-style normalization + lint warnings),
`looks_like_oidc(token, prefixes)` heuristic, `introspect(token, cfg)`
(async httpx POST form-encoded, Basic auth client credentials, timeout),
`resolve_sso_role(claims, cfg)` (role_rules → default_role), and a
small per-process introspection cache helper riding `get_storage()`
(namespace `sso_introspect`).

**EDIT `gateway_enterprise/links.py`** — add `resolve_link` (the
dispatch wrapper: OIDC-shaped → sso path [async], else core
`resolve_key_link`); on successful SSO validation set identity +
`_REQUEST_KEY_ID = "admin"`; on failure 401/503 per fail mode. Extend
`rbac_link`: identity-first role resolution + identity-first denial
audit key.

**EDIT `enterprise_main.py`** — wire
`AuthPipeline([resolve_link, rbac_link, admin_gate_link,
rate_limit_link])` (resolve_link replaces resolve_key_link; update the
docstring recipe — the old `[resolve, sso, rbac, ...]` example order is
wrong and the comment should say why); keyring-load
`client_secret_env` before serving.

**EDIT `tests/test_enterprise_rbac.py`** (or new
`tests/test_enterprise_sso.py` — prefer new file, same fixture imports)
— canary-style with a monkeypatched introspection transport (no real
HTTP): patch `gateway_enterprise.sso` HTTP call point.

**NEW `scripts/stub_idp.py`** (dev-only, for live verification) —
minimal localhost HTTP server exposing `/oauth2/introspect` returning
configurable active/inactive responses with groups claims; run
alongside 8011 during manual verification; NOT imported by tests.

**EDIT `smoke_test.py`** — no change needed for SSO itself (no IdP in
smoke); optionally a `--sso-url` flag pointing at the stub for manual
matrix runs. Decide during implementation; default: leave smoke alone,
verify SSO via curl against the stub.

## Test plan (pytest, ~14 cases)

1. `sso.enabled` absent/false → resolve_link delegates to core resolve
   byte-for-byte (gateway key works, OIDC-shaped token 401s).
2. OIDC-shaped bearer + active introspection → 200 on GET /admin/stats;
   `_REQUEST_KEY_ID == "admin"`; identity ContextVar populated.
3. Inactive token → 401 `invalid_api_key`.
4. Introspection transport error → 503 `sso_unavailable` (fail closed,
   keys configured); with `fail_open: true` + keys configured → 401
   (delegates to core resolve); with `fail_open: true` + true open mode
   (no keys) → request passes exactly as core open mode. Assert /admin
   is NEVER reachable with an unvalidated OIDC token in any mode.
5. role_rules: list-claim membership → full_admin; no match →
   default_role (ops_readonly); unknown role in rules → fail closed +
   parse warning.
6. SSO full_admin can POST /admin/reload; SSO ops_readonly 403s on
   /admin/config with audit line keyed `sso:<email>`.
7. Gateway-key path unaffected: `dev-full`-style key still resolves via
   core resolve inside the wrapper (dispatch correctness); JWT-shaped
   gateway_key + sso enabled → parse lint warning fires.
8. Introspection cache: second request with same token does NOT hit the
   transport (count calls); TTL expiry re-introspects (fake clock or
   ttl 0); entry capped by token exp.
9. audience mismatch (when configured) → 401.
10. Open mode + sso disabled → everything as today.
11. `looks_like_oidc` unit: JWT shape, prefix list, plain keys negative.
12. SSOConfig parsing unit: defaults, bad types dropped with warnings,
    client_secret_env missing when enabled → sso disabled + ERROR log
    (fail closed), JWT-shaped gateway_key collision lint fires.
13. rbac denial for SSO user audited with `sso:<email>` identity.
14. Canary: pipeline swap changes serving behavior (SSO-enabled config
    accepts a token the core would 401) — the anti-decorative-seam
    assertion.

## Verification (live, port 8011 + stub IdP)

1. `uv run pytest tests -q` — expect 484 + ~14 new, all green.
2. Start `scripts/stub_idp.py` (e.g. port 8099) + enterprise on 8011
   with an `sso` section pointing at the stub.
3. Curl matrix: minted-active token → /admin/stats 200 (role per
   groups claim); inactive → 401; groups=full-admins → POST /admin/
   reload 200; no-match groups → config GET 403 (default_role);
   gateway key still works; audit shows `sso:` identities.
4. Cache check: two rapid requests → one stub hit (stub logs hits).
5. Restart without sso section → behavior identical to 33757c6.
6. Commit: `Tier C: SSO/OIDC admin auth via the auth seam`. Update
   project context.

## Out of scope (explicit)

- Local JWT/JWKS signature validation (introspection only at tier C).
- Browser authorization-code flow / console login page / logout.
- /v1 consumer SSO (virtual keys remain the consumer story).
- SCIM/user provisioning, per-SSO-user spend budgets.
- Identity in CORE audit lines (needs an upstream audit seam; enterprise
  denials DO carry identity).
- Multi-IdP (single issuer per deployment at tier C).
