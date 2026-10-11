# Tier D — Multi-Replica Shared State (v1.3 "Foundation")

First release of the v1.3 → v1.4 → v2.0 upgrade train (competitive
review, 2026-10-10). Tier D moves ALL serving state + config onto
shared backends so several Gatelodge Enterprise replicas serve as one
logical gateway: RedisStorage (storage seam) + DatabaseConfigStore
(config seam). Core changes land UPSTREAM (gatelodge_standard) first
— the fork never edits `gateway/*`.

## Release-train context (each later phase gets its own bundle)

- **v1.3 Foundation (THIS PLAN)**: D-0 upstream fixes, RedisStorage,
  DatabaseConfigStore, conditional wiring, two-replica verification.
  Follow-ons in the same release, separate bundles: TPM limiting
  (upstream, D-2), JWKS (fork, E-1), whoami + console RBAC (E-2),
  ship artifacts (Docker/compose/Grafana/quickstart).
- **v1.4 Governance Complete**: SCIM (router seam), hash-chained
  audit, session budgets, cloud credential chains, air-gap bundle.
- **v2.0 The Gatehouse**: `set_extra_router` seam, MCP gateway,
  trace correlation + OTLP, semantic caching, teams/workspaces.
- Pending product decisions (do NOT block tier D): MCP scope
  sign-off, license posture (ELv2-style for the fork recommended),
  public pricing.

## Grounding (verified in-code this session)

- **StorageBackend protocol** (gateway/storage.py): get /
  get_default / set / delete / incr / contains / keys / items / len /
  min_by / clear — namespace-keyed, JSON-serializable values.
  InMemoryStorage is the default; NamespaceDict/NamespaceSet facades
  delegate through get_storage(). Riding namespaces today:
  response_cache, spend_ledger, spend_alerted, rate_windows, batches,
  route_latency, route_selections, route_rr, cold_breakers,
  guardrail_violations, verify_state, sso_introspect (tier C).
- **ConfigStore** (gateway/config.py:702): load() / save(raw) /
  version(); FileConfigStore default; set_config_store() rebuilds the
  singleton; maybe_reload() polls store.version() per request.
- **TWO CORE BYPASSES that break tier D — both fixed in D-0:**
  1. `_mutate_config` (app.py:3397) and `admin_put_config`
     (app.py:3990) read-modify-write via `cfg.path.write_text(...)`
     directly — bypassing `cfg.store.save()`. With a DB store, admin
     mutations would write the local FILE while replicas read the DB
     → silent divergence.
  2. `_scope_spend` (app.py:708) seeds a missing/stale ledger entry
     from the LOCAL month log (`_iter_month_lines`). A shared ledger
     seeded by the first replica to touch a scope undercounts every
     other replica's history.
- **Audit stays per-replica BY DESIGN**: config-audit.jsonl is local
  + SIEM webhook per replica; GET /admin/audit shows the local
  replica only (documented limitation; SIEM is the aggregate).
- **Upstream relationship**: gatelodge_enterprise merges FROM
  gatelodge_standard (origin = local path to that repo). D-0 commits
  upstream, then merges into the fork before fork modules build on
  the new contracts.
- **Prometheus metrics stay per-process BY DESIGN** — deployment
  story is per-replica scraping; do not fight this.

## Design decisions

1. **D-0 first, upstream.** All core changes land in
   gatelodge_standard (suite there is 470 tests; fork 505), merge
   into enterprise, THEN fork modules.
2. **Optimistic CAS on config writes** (upstream contract change):
   `ConfigStore.save(raw, *, expected_version=None) -> bool`.
   FileConfigStore ignores expected_version (single-process) and
   returns True. `_mutate_config` / `admin_put_config` pass the
   version they loaded; on False they retry the whole
   load→mutate→save ONCE, then fail loudly. Two admins mutating on
   two replicas must never silently clobber.
3. **`shared` flag on storage backends** (class attribute, default
   False; InMemoryStorage False, RedisStorage True): when
   `get_storage().shared`, `_scope_spend` does NOT seed from local
   logs — the scope starts at 0. Documented cold-start drift
   (mitigate by pre-warm or accepting it; month rollover re-zeroes
   everyone anyway).
4. **RedisStorage** (gateway_enterprise/redis_storage.py): sync
   redis-py + ConnectionPool (core storage calls are sync; sub-ms ops
   — same event-loop trade the core already makes with file I/O).
   Redis key = `f"{ns}:{key}"`; values JSON-serialized;
   `keys`/`items` via SCAN (never KEYS); `incr` via INCRBYFLOAT;
   constructor takes `url=` OR `client=` (fakeredis injection for
   canaries). Mirror InMemoryStorage's exact semantics for every
   protocol method (check min_by's signature against the protocol
   during implementation).
5. **DatabaseConfigStore** (gateway_enterprise/db_config.py):
   DB-API-based — Postgres via psycopg3 when constructed with
   `dsn=`; accepts any DB-API connection via `conn=` (sqlite3 in
   tests — REAL class, REAL SQL, no mocks). Single row
   `config(id=1, raw TEXT json, version BIGINT, updated_at)`.
   `save()` = `UPDATE config SET raw=?, version=version+1,
   updated_at=now WHERE id=1 AND version=:expected` → rowcount 1 or
   CAS-False. `load()` reads raw; `version()` is one indexed SELECT.
   **Seed-on-first-boot**: when the table is empty, seed from the
   existing data/gateway_config.json (operator-friendly migration),
   log once.
6. **Conditional wiring** (enterprise_main.py): swap backends ONLY
   when env vars are set — `GATELODGE_REDIS_URL`, 
   `GATELODGE_CONFIG_DSN`. No-tier-D deployments stay byte-for-byte
   identical to f5cc2c8. Boot order: logging → SSO secret →
   **storage** → **config store** → auth pipeline → serve (storage
   before config so tier C's sso_introspect cache never writes to a
   backend that's about to be swapped).
7. **Two-replica verification on one box**: 8011 + 8012 sharing
   Redis/PG. The killer check: a config mutation via replica A's
   admin API propagates to replica B on B's next request (version
   token poll) — plus shared rate window, shared SSO introspection
   cache, cross-replica response-cache hit.
8. **JSON columns over JSONB**: TEXT + json.dumps works identically
   on Postgres and sqlite; single-row config gains nothing from
   JSONB indexing.

## Config / env spec

```
GATELODGE_REDIS_URL=redis://127.0.0.1:6379/0     # unset = InMemory
GATELODGE_CONFIG_DSN=postgresql://user:pw@host/db  # unset = file store
```

(Env-var bootstrap mirrors the api_key_env convention: the DSN must
be known before config loads, so it cannot live IN the config. A
declarative `tier_d` config section may follow later — env wins.)

## Files

**UPSTREAM (gatelodge_standard — D-0):**
- `gateway/config.py` — ConfigStore.save contract (expected_version,
  -> bool); FileConfigStore adjusted.
- `gateway/app.py` — `_mutate_config` + `admin_put_config` route
  through `cfg.store.save()` with the loaded version + one retry;
  `_scope_spend` local-log seeding gated on `not
  getattr(get_storage(), "shared", False)`.
- `gateway/storage.py` — `shared = False` class attribute documented
  on the protocol.
- Upstream canary tests for all three fixes.

**FORK (gatelodge_enterprise):**
- `gateway_enterprise/redis_storage.py` — NEW.
- `gateway_enterprise/db_config.py` — NEW.
- `enterprise_main.py` — conditional wiring block.
- `tests/test_enterprise_tier_d.py` — NEW canary suite.
- `pyproject.toml` — dev dep `fakeredis`; runtime deps `redis`,
  `psycopg[binary]` (uv add).

## Test plan (~14 cases, fakeredis + sqlite — no real services)

1. Upstream: `_mutate_config` writes via `store.save()` (Recording
   store pattern), never `cfg.path`.
2. Upstream: CAS — a version conflict returns False from save and
   `_mutate_config` retries once and succeeds on fresh read.
3. Upstream: `_scope_spend` does not seed from the local log when
   `store.shared` (starts at 0; seeds normally when not shared).
4. RedisStorage: behavioral parity vs InMemoryStorage across the
   whole protocol (same fixture operations, same results).
5. RedisStorage: `incr` float semantics; namespace isolation
   (`ns:a` never leaks into `ns:b` via keys/items/clear).
6. Canary: `set_storage(RedisStorage(client=fakeredis))` → one chat
   request writes response_cache + spend_ledger entries into redis.
7. Canary: shared rate window — two clients (two "replicas") over
   one fakeredis see the same RPM window.
8. DatabaseConfigStore (sqlite conn): load/save/version round-trip;
   CAS conflict → False; seed-from-file when the table is empty.
9. Canary: `set_config_store(DatabaseConfigStore(conn=sqlite))` →
   config serves from the DB; save bumps version.
10. Two-replica canary: mutate config via client A's admin API →
    client B (fresh GatewayConfig on the same store) reloads on its
    next `maybe_reload()`.
11. SSO cache shared: introspection transport hit ONCE across two
    clients sharing fakeredis (tier C machinery on a shared backend).
12. Cross-replica response-cache hit: identical request served by
    the second client without reaching the backend.
13. Wiring guard: with no env vars, enterprise_main's wiring block
    leaves the default backends in place (unit-checkable without
    booting the server).
14. Upstream suite green (470 + new) AND fork suite green
    (505 + upstream new + tier-d new) after the merge.

## Verification (live, port 8011 + 8012)

1. Check for Docker on the box → `redis` + `postgres` containers.
   With Docker: full-stack live verify. Without: live demo on
   fakeredis + sqlite through the SAME wiring code paths (weaker
   story, documented; Docker-based verify remains a TODO for a
   machine that has it).
2. Two replicas (8011, 8012) sharing backends: smoke test on BOTH
   (`--key dev-full-8011`); config mutation via A's `/admin/config`
   PUT → B serves the new value on its next request; shared rate
   window demonstrated; SSO stub-idp introspection hit once across
   both replicas (needs the sso section re-added for the check).
3. No-env regression: boot without the env vars → behavior identical
   to f5cc2c8 (single-replica defaults).
4. Commit sequence: upstream commit(s) → merge into fork → fork
   commit(s). Update project context (both repos).

## Out of scope (explicit)

- SCIM, session budgets, MCP gateway, JWKS, TPM, whoami/console RBAC
  — separate bundles in the v1.3 train.
- Audit aggregation beyond the SIEM webhook (DB audit sink is a
  later option).
- Redis Sentinel/Cluster, TLS to Redis/PG, connection retry storms
  (config knobs later if demanded).
- Migration of existing in-memory state on a backend swap
  (longstanding documented non-goal — swap before serving).
- Managed/cluster deployment tooling (Docker/compose/Grafana ship as
  the separate "ship artifacts" bundle).
