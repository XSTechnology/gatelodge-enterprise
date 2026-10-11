"""Gatelodge Enterprise — the COMMERCIAL fork modules, injected via the
core seams (gateway.auth / gateway.storage / gateway.config) at
startup by enterprise_main.py.

Governance (RBAC role-scoped admin keys + SSO/OIDC) is CORE since
upstream 755432f — gateway/rbac.py, gateway/identity.py,
gateway/sso.py, with resolve_link + rbac_link in the default
pipeline. This package carries only the paid tiers:

    tier D — multi-replica shared state:
        redis_storage.RedisStorage (storage seam)
        db_config.DatabaseConfigStore (config seam)

Future: v1.4 SCIM + session budgets + audit hardening; v2.0 the
Gatehouse (MCP gateway, OTel, semantic caching, teams).
"""
