"""Gatelodge Enterprise (gatelodge_enterprise) entry point.

The core (the ``gateway`` package) is NEVER edited in this repo — all
enterprise behavior is injected through the three seams at startup.
The core's default pipeline already carries governance (RBAC +
SSO/OIDC — upstreamed 755432f: role-scoped admin keys and SSO are
FREE in the standard distribution, the no-enterprise-tax posture).
This entry point therefore wires only the COMMERCIAL tier:

    tier D — multi-replica shared state (storage + config seams)

    # storage seam (before config: tier C's sso_introspect cache must
    # never write to a backend that's about to be swapped)
    from gateway.storage import set_storage
    from gateway_enterprise.redis_storage import RedisStorage
    set_storage(RedisStorage(url="redis://..."))

    # config seam
    from gateway import config as gw_config
    from gateway_enterprise.db_config import DatabaseConfigStore
    gw_config.set_config_store(DatabaseConfigStore(dsn="..."))

Both swaps are conditional on env vars (GATELODGE_REDIS_URL /
GATELODGE_CONFIG_DSN) — no env vars = the core's single-process
defaults, byte-for-byte. See _wire_tier_d().

Canary seam tests live in tests/test_gateway_seams.py (core) and
tests/test_enterprise_tier_d.py (fork) — the templates for
verification: swap in the REAL implementation, assert serving
behavior changes (never trust an unwired seam).
"""
from __future__ import annotations

import os


def _wire_tier_d() -> None:
    """Swap the storage + config backends when the operator points at
    shared ones (tier D — multi-replica: N replicas, one Redis, one
    DB). NO env vars = the core's single-process defaults, byte-for-
    byte. Env-var bootstrap mirrors the api_key_env convention: the
    Redis URL / DSN must be known before the config loads, so it
    cannot live IN the config.

      GATELODGE_REDIS_URL   e.g. redis://127.0.0.1:6379/0
      GATELODGE_CONFIG_DSN  e.g. postgresql://user:pw@host/gatelodge

    Order matters: storage BEFORE config (the sso_introspect cache
    must never write to a backend that's about to be swapped). The DB
    store seeds from the legacy data/gateway_config.json once, when
    its table is empty — the operator's pre-tier-D config carries
    over. Call BEFORE serving; existing state does NOT migrate."""
    from gateway import config as gw_config

    redis_url = os.environ.get("GATELODGE_REDIS_URL", "").strip()
    if redis_url:
        from gateway.storage import set_storage
        from gateway_enterprise.redis_storage import RedisStorage
        set_storage(RedisStorage(url=redis_url))

    dsn = os.environ.get("GATELODGE_CONFIG_DSN", "").strip()
    if dsn:
        from gateway_enterprise.db_config import DatabaseConfigStore
        gw_config.set_config_store(DatabaseConfigStore(
            dsn=dsn, seed_file=gw_config.CONFIG_PATH))


def main() -> None:
    # ── enterprise seam wiring (before serving) ────────────────────
    # The core's main() configures logging, but the wiring below logs
    # BEFORE it runs — configure logging here first (same format; the
    # core's basicConfig call then becomes a no-op).
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # Tier D: multi-replica shared state (the commercial tier —
    # governance itself is core now). No env vars = no-op.
    _wire_tier_d()

    # The core's default pipeline (resolve_link → rbac_link →
    # admin_gate → rate_limit) carries governance; the core's boot
    # bridges provider keys AND sso.client_secret_env from the OS
    # keyring. Nothing enterprise remains to wire.
    from gateway.__main__ import main as core_main
    core_main()


if __name__ == "__main__":
    main()
