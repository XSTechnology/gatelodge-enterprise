"""Gatelodge Enterprise (gatelodge_enterprise) entry point.

The core (the ``gateway`` package) is NEVER edited in this repo — all
enterprise behavior is injected through the three seams at startup.
Until the enterprise implementations land (tiers C-lite / C / D), this
boots the core defaults on the enterprise port from THIS repo's
data/gateway_config.json — proving the wiring path works.

Seam recipe (fill in as the tiers land):

    # tier D — multi-replica shared state
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

    # tier D — shared config
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
