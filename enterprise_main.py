"""Gatelodge Enterprise (gatelodge_enterprise) entry point.

The core (the ``gateway`` package) is NEVER edited in this repo — all
enterprise behavior is injected through the three seams at startup.
Until the enterprise implementations land (tiers C-lite / C / D), this
boots the core defaults on the enterprise port from THIS repo's
data/gateway_config.json — proving the wiring path works.

Seam recipe (fill in as the tiers land):

    # tier D — multi-replica shared state (LANDED; wired in main() via
    # _wire_tier_d(), conditional on GATELODGE_REDIS_URL /
    # GATELODGE_CONFIG_DSN — no env vars = core defaults):
    from gateway.storage import set_storage
    from gateway_enterprise.redis_storage import RedisStorage
    set_storage(RedisStorage(url="redis://..."))

    # tier C — SSO/OIDC admin auth (LANDED; wired in main() below).
    # NOTE: SSO is NOT a link after the core resolve_key_link — the
    # core resolver 401s unknown tokens before any later link runs.
    # The enterprise resolve_link REPLACES it in the pipeline and
    # dispatches OIDC-shaped bearers to SSO validation.
    from gateway.auth import (AuthPipeline, set_auth_pipeline,
                              admin_gate_link, rate_limit_link)
    from gateway_enterprise.links import rbac_link, resolve_link
    set_auth_pipeline(AuthPipeline([resolve_link, rbac_link,
                                    admin_gate_link, rate_limit_link]))

    # tier D — shared config (LANDED; same conditional wiring)
    from gateway import config as gw_config
    from gateway_enterprise.db_config import DatabaseConfigStore
    gw_config.set_config_store(DatabaseConfigStore(dsn="..."))

Canary seam tests live in tests/test_gateway_seams.py — the template
for fork-side verification: swap in the REAL implementation, assert
serving behavior changes (never trust an unwired seam).
"""
from __future__ import annotations

import os


def _load_sso_secret() -> None:
    """sso.client_secret_env → OS keyring fallback (boot bridge, same
    convention as the core's api_key_env bridge in gateway/__main__).
    Runs once at startup — changing client_secret_env in the config
    needs a restart. Never logs values."""
    try:
        import keyring
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load()
        sso = raw.get("sso") if isinstance(raw, dict) else None
        sso = sso if isinstance(sso, dict) else {}
        name = str(sso.get("client_secret_env") or "").strip()
        if name and not os.environ.get(name):
            val = keyring.get_password("Workshop", name)
            if val:
                os.environ[name] = val
    except Exception:  # noqa: BLE001 — locked/missing keyring is fine
        pass


def _wire_tier_d() -> None:
    """Swap the storage + config backends when the operator points at
    shared ones (tier D — multi-replica: N replicas, one Redis, one
    DB). NO env vars = the core's single-process defaults, byte-for-
    byte. Env-var bootstrap mirrors the api_key_env convention: the
    Redis URL / DSN must be known before the config loads, so it
    cannot live IN the config.

      GATELODGE_REDIS_URL   e.g. redis://127.0.0.1:6379/0
      GATELODGE_CONFIG_DSN  e.g. postgresql://user:pw@host/gatelodge

    Order matters: storage BEFORE config (tier C's sso_introspect
    cache must never write to a backend that's about to be swapped).
    The DB store seeds from the legacy data/gateway_config.json once,
    when its table is empty — the operator's pre-tier-D config carries
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

    _load_sso_secret()
    _wire_tier_d()   # storage → config (before auth: the pipeline reads cfg)

    # C-lite LANDED: role-scoped admin keys (rbac_link — the
    # role→admin-surface mapping from admin_key_roles).
    # Tier C LANDED: SSO/OIDC admin auth (resolve_link — replaces the
    # core resolver; dispatches OIDC-shaped bearers to introspection,
    # delegates everything else untouched). See gateway_enterprise/.
    from gateway.auth import (AuthPipeline, admin_gate_link,
                              rate_limit_link, set_auth_pipeline)
    from gateway_enterprise.links import rbac_link, resolve_link
    set_auth_pipeline(AuthPipeline([resolve_link, rbac_link,
                                    admin_gate_link, rate_limit_link]))

    from gateway.__main__ import main as core_main
    core_main()


if __name__ == "__main__":
    main()
