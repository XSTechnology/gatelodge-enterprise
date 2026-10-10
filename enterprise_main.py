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

    # tier C — SSO/OIDC + RBAC in front of the admin gate
    from gateway.auth import (AuthPipeline, set_auth_pipeline,
                              resolve_key_link, admin_gate_link,
                              rate_limit_link)
    from gateway_enterprise.links import sso_link, rbac_link
    set_auth_pipeline(AuthPipeline([resolve_key_link, sso_link,
                                    rbac_link, admin_gate_link,
                                    rate_limit_link]))

    # tier D — shared config
    from gateway import config as gw_config
    from gateway_enterprise.db_config import DatabaseConfigStore
    gw_config.set_config_store(DatabaseConfigStore(dsn="..."))

Canary seam tests live in tests/test_gateway_seams.py — the template
for fork-side verification: swap in the REAL implementation, assert
serving behavior changes (never trust an unwired seam).
"""
from __future__ import annotations


def main() -> None:
    # ── enterprise seam wiring lands HERE (before serving) ──────────
    # Until the implementations above exist, the core defaults serve.

    from gateway.__main__ import main as core_main
    core_main()


if __name__ == "__main__":
    main()
