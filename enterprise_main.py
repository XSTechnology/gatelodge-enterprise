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


def _wire_tier_d(enabled: bool = True) -> None:
    """Swap the storage + config backends when the operator points at
    shared ones (tier D — multi-replica: N replicas, one Redis, one
    DB). NO env vars = the core's single-process defaults, byte-for-
    byte. Env-var bootstrap mirrors the api_key_env convention: the
    Redis URL / DSN must be known before the config loads, so it
    cannot live IN the config.

    v1.5: ``enabled`` is the license claim (tier_d) — an enterprise
    license without the key skips the swap even when the env vars are
    set.

      GATELODGE_REDIS_URL   e.g. redis://127.0.0.1:6379/0
      GATELODGE_CONFIG_DSN  e.g. postgresql://user:pw@host/gatelodge

    Order matters: storage BEFORE config (the sso_introspect cache
    must never write to a backend that's about to be swapped). The DB
    store seeds from the legacy data/gateway_config.json once, when
    its table is empty — the operator's pre-tier-D config carries
    over. Call BEFORE serving; existing state does NOT migrate."""
    from gateway import config as gw_config

    if not enabled:
        import logging
        logging.getLogger("gateway.enterprise").warning(
            "tier D NOT licensed — running single-process state "
            "even though GATELODGE_REDIS_URL/GATELODGE_CONFIG_DSN "
            "may be set")
        return

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


def _wire_scim(enabled: bool = True) -> None:
    """Mount the SCIM provisioning surface (gateway_enterprise/scim.py)
    via the router seam when scim.enabled — boot-time mounting (section
    changes need a restart; the token_hash hot-reloads per request).
    v1.5: the scim license claim gates the mount regardless of the
    config section."""
    from gateway import config as gw_config
    from gateway.app import set_extra_router
    if not enabled:
        import logging
        logging.getLogger("gateway.enterprise").warning(
            "SCIM NOT licensed — the scim config section is ignored")
        return
    raw = gw_config.get_gateway_config().store.load()
    scim = raw.get("scim") if isinstance(raw, dict) else None
    scim = scim if isinstance(scim, dict) else {}
    if not scim.get("enabled"):
        return
    from gateway_enterprise.scim import build_router
    set_extra_router(build_router(), prefix="/scim")


def _wire_mcp(enabled: bool = True) -> None:
    """Mount the MCP gateway surface (gateway_enterprise/mcp.py) via the
    ADDITIVE router seam (v2.0 — SCIM at /scim and MCP at /mcp coexist;
    wiring order matters: scim mounts first, mcp ADDS). License-claimed:
    the 'mcp' feature key — enterprise licenses carry it by default, and
    an unlicensed binary ignores the mcp section entirely."""
    from gateway import config as gw_config
    from gateway.app import add_extra_router
    if not enabled:
        import logging
        logging.getLogger("gateway.enterprise").warning(
            "MCP gateway NOT licensed — the mcp config section is "
            "ignored")
        return
    raw = gw_config.get_gateway_config().store.load()
    mcp = raw.get("mcp") if isinstance(raw, dict) else None
    mcp = mcp if isinstance(mcp, dict) else {}
    if not mcp.get("enabled"):
        return
    from gateway_enterprise.mcp import build_router
    add_extra_router(build_router(), prefix="/mcp")


def _wire_teams(enabled: bool = True) -> None:
    """Mount the teams surface (gateway_enterprise/teams.py) at /teams
    via the additive router seam (SCIM + MCP + teams coexist). The
    auth-pipeline links (attribution + budget gate) are wired in
    _wire_auth_pipeline — this mounts the ADMIN surface only.
    License-claimed: the 'teams' feature key."""
    from gateway import config as gw_config
    from gateway.app import add_extra_router
    if not enabled:
        import logging
        logging.getLogger("gateway.enterprise").warning(
            "teams NOT licensed — the teams config section is ignored")
        return
    raw = gw_config.get_gateway_config().store.load()
    teams = raw.get("teams") if isinstance(raw, dict) else None
    teams = teams if isinstance(teams, dict) else {}
    if not teams.get("enabled"):
        return
    from gateway_enterprise.teams import build_router
    add_extra_router(build_router(), prefix="/teams")


def _wire_auth_pipeline(session_budgets: bool, teams: bool = False) -> None:
    """Install the enterprise auth pipeline (core governance + the
    license-claimed enterprise links). Without their claims the binary
    runs the CORE pipeline — the license file decides what this binary
    executes, not the config."""
    import logging
    from gateway.auth import (AuthPipeline, admin_gate_link,
                              rate_limit_link, resolve_link, rbac_link,
                              set_auth_pipeline)
    links = [resolve_link, rbac_link]
    if teams:
        from gateway_enterprise.teams import team_budget_link, team_link
        links += [team_link, team_budget_link]
    else:
        logging.getLogger("gateway.enterprise").warning(
            "teams NOT licensed — running without team links")
    if session_budgets:
        from gateway_enterprise.session_budget import session_budget_link
        links.append(session_budget_link)
    else:
        logging.getLogger("gateway.enterprise").warning(
            "session budgets NOT licensed — running the core pipeline")
    links += [admin_gate_link, rate_limit_link]
    set_auth_pipeline(AuthPipeline(links))


def main() -> None:
    # ── enterprise seam wiring (before serving) ────────────────────
    # The core's main() configures logging, but the wiring below logs
    # BEFORE it runs — configure logging here first (same format; the
    # core's basicConfig call then becomes a no-op).
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # v1.5 closed distribution: the enterprise binary REFUSES TO BOOT
    # without a valid ENTERPRISE license — no trial mode (that is the
    # community binary's posture). The gate exits(2) with a clear log
    # line before any wiring or serving. Community-edition licenses do
    # not unlock this binary.
    from gateway import licensing
    lic = licensing.bootstrap(edition="enterprise")
    licensing.enterprise_boot_gate(lic)

    # Enterprise feature claims gate the commercial modules — the
    # license file decides what this binary runs (issue with
    # scripts/license_tool.py; enterprise licenses default to all
    # three keys).
    _wire_tier_d(enabled=licensing.allows("tier_d"))
    _wire_scim(enabled=licensing.allows("scim"))
    _wire_mcp(enabled=licensing.allows("mcp"))
    _wire_teams(enabled=licensing.allows("teams"))

    # v1.4: session budgets — the ENTERPRISE link (inert without the
    # session_budgets section; the core carries the charge hook). The
    # enterprise pipeline = core governance + the session gate; without
    # the session_budgets claim the binary runs the core pipeline.
    # v2.0: team links (attribution + budget gate) ride the same
    # pipeline, teams claim gated.
    _wire_auth_pipeline(session_budgets=licensing.allows("session_budgets"),
                        teams=licensing.allows("teams"))

    # The core's default pipeline (resolve_link → rbac_link →
    # admin_gate → rate_limit) carries governance; the core's boot
    # bridges provider keys AND sso.client_secret_env from the OS
    # keyring. Nothing enterprise remains to wire.
    from gateway.__main__ import main as core_main
    core_main()


if __name__ == "__main__":
    main()
