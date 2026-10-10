# C-lite — Role-Scoped Admin Keys (rbac_link via the auth seam)

Tier C-lite of the enterprise roadmap: extend the ADMIN key model with
roles (read-only ops, key manager, full admin) and enforce a role→admin-
surface mapping through an enterprise `rbac_link` in the auth pipeline.
First real consumer of a seam. **Zero edits under `gateway/`** —
everything lands in a new `gateway_enterprise` package + fork-owned
root files.

## Grounding (what the core actually does today)

- `gateway/auth.py` — link contract `(cfg, request, key_id) ->
  JSONResponse | None`, sync or async; pipeline re-reads
  `_REQUEST_KEY_ID` between links. `_resolve_bearer`: every
  `gateway_keys` entry (plaintext OR sha256-hex form) → the single
  identity `"admin"`; `api_keys` entries → their id. `admin_gate_link`:
  `/admin/*` requires `key_id == "admin"` (open mode stays open).
- `gateway/config.py` — `GatewayConfig.gateway_keys` is a list of
  STRINGS (line ~800); `_normalize_api_keys` DROPS unknown fields, so
  roles cannot ride either key structure. `GatewayConfig.store` is the
  active `ConfigStore` (`load()/save()/version()`); `maybe_reload()`
  re-reads when the version token moves.
- `gateway/app.py` — `_bearer_auth` middleware: per-request
  `cfg.maybe_reload()` then `get_auth_pipeline().run(request, cfg)`;
  short-circuits get the request-id wrapper. `_mutate_config`
  (line ~3397) read-modify-writes the RAW config dict → unknown
  top-level JSON sections survive every core mutation. `GET
  /admin/config` (line ~3969) returns the raw config TEXT (exposes all
  key material). Audit: `_audit_config` appends `{ts, key, action}` to
  `config-audit.jsonl` next to `_LOG_PATH`; `key` is always `"admin"`.
- Admin surface inventory (72 routes): ~27 GET endpoints (read-only)
  and ~45 POST/PUT (mutations). Canary test template:
  `tests/test_gateway_seams.py` (swap in REAL wiring via
  `set_auth_pipeline`, restore in `finally`).

## Design decisions

1. **Roles live in a new `admin_key_roles` section of the operator
   config** (data/gateway_config.json), NOT in a separate store.
   - Survives core mutations (raw round-trip, see grounding).
   - Hot-reload for free via the active store's version token.
   - Follows config to `DatabaseConfigStore` in tier D — no extra DB
     story for roles.
   - Backward compatible: no section → every gateway key is full admin
     (exactly today's behavior).
2. **Fixed three-role catalog** (roadmap names exactly these): 
   `full_admin`, `ops_readonly`, `key_manager`. Policy table is DATA in
   code (first-match-wins rule rows) so custom roles later are a table
   edit, not a redesign. No config-level role-definition override in
     C-lite.
3. **No new endpoints.** Role assignment is a config edit (file, or
   `PUT /admin/config` by a full admin — itself RBAC-gated to
   full_admin). Mounting FastAPI routes from enterprise code would be a
   FOURTH extension mechanism; the fork philosophy is three seams.
4. **Role entries carry the key value** (plaintext or sha256-hex —
   same two forms `gateway_keys` accepts) because gateway_keys are
   bare strings with no stable id to reference. Hash form recommended
   (zero plaintext at rest); entry key MUST also appear in
   `gateway_keys` or `resolve_key_link` 401s the token before RBAC
   ever runs (lint warns on dead entries).
5. **rbac_link runs BEFORE admin_gate_link** per the recipe
   (resolve → [sso: tier C] → rbac → admin_gate → rate_limit). It only
   adjudicates when `key_id == "admin"` AND path starts with `/admin`;
   virtual keys and open mode pass through untouched (admin_gate keeps
   its exact core semantics). It never mutates `_REQUEST_KEY_ID`
   (spend attribution + admin_gate depend on it).
6. **Fail closed**: unknown role name → deny-all-`/admin` + WARNING at
   parse. A typo must not silently widen access in a governance
   product.
7. **Denials are audited** by appending to the same
   `config-audit.jsonl` in the same shape, with
   `key = "admin:<role>:<sha256(token)[:8]>"` (deterministic, no
   plaintext leak — same display convention as /admin/keys). Core's
   own mutation lines still say `"admin"` (identity in core audit
   lines improves with tier C SSO — noted as a known limitation).
8. **Serving surface is out of scope**: RBAC governs `/admin/*` only.
   Admin keys on `/v1/*` keep core behavior (roadmap wording: "role→
   admin-surface mapping"). Consumers get virtual keys.

## Policy table (exact)

Rule row = `(method | "ANY", pattern)`; pattern matches exact path, or
prefix when it ends with `/*`. First matching row wins; no match →
deny. Roles not in the table → deny-all.

| role | rules |
|---|---|
| `full_admin` | `("ANY", "/admin/*")` |
| `ops_readonly` | deny `("GET", "/admin/config")` (raw text = key material) → then allow `("GET", "/admin/*")` |
| `key_manager` | allow `("GET", "/admin/keys")`, `("POST", "/admin/keys")`, `("GET", "/admin/stats")`, `("GET", "/admin/secrets")` |

- `ops_readonly` is GET-only BY DESIGN; POST-reads (`/admin/runpod/
  inventory`, `/admin/runpod/logs`) are deliberately excluded —
  strictness beats convenience at C-lite; one table row adds them
  later if wanted.
- `key_manager` scope = virtual-key CRUD + the reads the console keys
  pane needs to render (usage, provider-key status). NOT
  `/admin/gateway-keys` (minting admin keys = full_admin only), NOT
  `/admin/config` (key material).
- Keys absent from `admin_key_roles` default to `full_admin`
  (backward compat).

## Config section spec

```json
"admin_key_roles": [
  {"key": "<sha256-hex of bearer (recommended) or the plaintext>",
   "role": "ops_readonly",
   "note": "NOC on-call (optional)"}
]
```

Semantics mirror `gateway_keys`: an entry matches a presented token
when `entry == token` (constant-time) or `entry == sha256(token)`.
First matching entry wins; duplicates/unknown roles/unmatched entries
log WARNINGs at parse time.

## New/changed files

**NEW `gateway_enterprise/`** (package):
- `__init__.py` — package docstring only.
- `rbac.py` — role catalog + policy table (`ROLES: dict[str,
  list[tuple[str, str]]]`, `DEFAULT_ROLE = "full_admin"`), the rule
  matcher `allowed(role, method, path) -> bool`, and `AdminRoleMap`:
  version-cached parser of the `admin_key_roles` section
  (`role_for(cfg, token) -> str`, `""` when unassigned; re-parse only
  when `cfg.store.version()` moves; keep last good on unreadable
  load, mirroring core). Lint warnings on parse.
- `links.py` — `rbac_link(cfg, request, key_id) -> JSONResponse |
  None` (sync link; the tier-C `sso_link` joins this module later).
  Flow: not `/admin/*` → pass; `key_id != "admin"` → pass; token →
  role (unassigned → pass); `full_admin` → pass; `allowed()` → pass;
  else 403 `{"error": {"message": "role '<role>' may not <METHOD>
  <path>", "type": "invalid_request_error", "param": null, "code":
  "rbac_forbidden"}}` + best-effort audit append (lazy-import
  `gateway.app` for `_LOG_PATH`/`_log_lock`; never raises).

**EDIT `enterprise_main.py`** — wire the pipeline before serving
(sso slot reserved as a comment), update the docstring recipe to
show C-lite as LANDED:

```python
from gateway.auth import (AuthPipeline, admin_gate_link,
                          rate_limit_link, resolve_key_link,
                          set_auth_pipeline)
from gateway_enterprise.links import rbac_link
set_auth_pipeline(AuthPipeline([resolve_key_link, rbac_link,
                                admin_gate_link, rate_limit_link]))
```

**EDIT `smoke_test.py`** (fork-owned) — optional `--key BEARER` (or
`GATEWAY_SMOKE_KEY` env): attach the Authorization header to every
check; when additionally `--ro-key BEARER` is given, append RBAC
checks: ro GET /admin/stats → 200, ro GET /admin/config → 403, ro
POST /admin/reload → 403, full POST /admin/reload → 200. Default
invocation unchanged (open-mode smoke keeps passing).

**EDIT this repo's `data/gateway_config.json`** (gitignored operator
config) — merge into the EXISTING 8011 config (preserve backends/
aliases): three dev admin keys (`dev-full`, `dev-ops`, `dev-km`
values) + roles for two of them. Keys make the surface non-open; all
live verification then runs with `--key`.

**NEW `tests/test_enterprise_rbac.py`** — canary-style (real wiring
via `set_auth_pipeline`, restore `AuthPipeline()` + `reset_storage()`
in `finally`; reuse `wired`/`gguf`/`_secured_config` helpers from
`test_gateway_stage3`).

## Test plan (pytest, ~12 cases)

1. No `admin_key_roles` section → admin key reaches everything
   (backward compat; e.g. POST /admin/reload 200).
2. `ops_readonly`: GET /admin/stats 200; GET /admin/config 403;
   POST /admin/reload 403; GET /admin/verify 200.
3. `key_manager`: GET+POST /admin/keys 200 (create + list), POST
   /admin/gateway-keys 403, GET /admin/config 403, GET /admin/stats
   200.
4. Unassigned admin key → full admin (POST /admin/reload 200).
5. Virtual key on /admin → 403 (admin_gate semantics unchanged).
6. Open mode (no keys at all) → rbac inert, /admin/stats 200 bare.
7. Hash-form role entry (sha256 in section, plaintext in
   gateway_keys) enforced.
8. Unknown role name → deny-all /admin (403 on GET /admin/stats) +
   warning logged.
9. Hot-reload: flip a key's role in the config file (bump mtime
   deterministically, `os.utime` like the config-seam test) → next
   request enforces the new role.
10. Denial audited: `config-audit.jsonl` gains a line with
    `key == "admin:ops_readonly:<hash8>"` and action
    `rbac-deny POST /admin/reload`.
11. `/v1/*` traffic unaffected: admin key with ops_readonly role
    still serves /v1/chat/completions 200 (FakeCloud, no GGUF load).
12. Unit: matcher table — exact, `/*` prefix, first-match deny-over
    allow, unknown role denies, ANY method row.

## Verification (live, port 8011)

1. `uv run pytest tests -q` — expect 470 + ~12 new, all green.
2. `start_enterprise.bat` (background) → startup log shows the
   pipeline-swap line ("auth pipeline swapped (4 links)").
3. `python smoke_test.py http://127.0.0.1:8011 --key <dev-full>`
   → all checks incl. RBAC rows pass.
4. Curl matrix: dev-ops GET stats 200 / GET config 403 / POST reload
   403; dev-km POST keys 200 / POST gateway-keys 403; wrong key 401;
   virtual key /admin 403; bare request 401.
5. Hot-reload live: edit a role in data/gateway_config.json → next
   request flips without restart.
6. Single commit: `C-lite: role-scoped admin keys via the auth seam`.
   Then update the project context (tier state).

## Out of scope (explicit)

- `sso_link` (tier C) — links.py just reserves its home.
- Console role-awareness (read-only keys see 403 toasts on action
  buttons; acceptable for C-lite).
- Identity in CORE audit lines (still `"admin"`; tier C SSO story).
- Custom/role-definition overrides in config; per-role rate limits;
  serving-surface (`/v1/*`) restrictions for admin keys.
- Multi-replica role propagation beyond what the store version token
  already gives (tier D).
