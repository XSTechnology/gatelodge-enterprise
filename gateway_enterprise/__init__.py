"""Gatelodge Enterprise — fork-side behavior injected via the core
seams (gateway.auth / gateway.storage / gateway.config).

Nothing in this package edits the core; it is imported and wired at
startup by enterprise_main.py:

    set_auth_pipeline(AuthPipeline([resolve_key_link, rbac_link,
                                    admin_gate_link, rate_limit_link]))

Tier C-lite: role-scoped admin keys (rbac.py, links.py).
Tier C:     SSO/OIDC (links.sso_link — reserved).
Tier D:     RedisStorage + DatabaseConfigStore (future modules).
"""
