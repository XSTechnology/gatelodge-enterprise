"""Role-scoped admin keys (tier C-lite).

Roles are assigned per ADMIN key in an ``admin_key_roles`` section of
the operator config (data/gateway_config.json — the section survives
core config mutations because _mutate_config round-trips the raw dict,
and it hot-reloads via the active store's version token):

    "gateway_keys": ["<sha256-of-full-admin-key>"],
    "admin_key_roles": [
      {"key": "<sha256-hex of the bearer (recommended) or plaintext>",
       "role": "ops_readonly", "note": "NOC on-call (optional)"}
    ]

Semantics mirror gateway_keys: an entry matches a presented token when
the entry equals the token or equals its sha256 hex. The entry's key
MUST also appear in gateway_keys — otherwise resolve_key_link 401s the
token before RBAC ever runs (this module lints for dead entries).

Roles (fixed catalog at C-lite; the policy table is data, so widening
it later is a table edit):

    full_admin    everything (DEFAULT for keys not in the section —
                  exactly the pre-C-lite behavior)
    ops_readonly  every admin GET except /admin/config (its raw text
                  exposes all key material)
    key_manager   virtual-key CRUD + the reads the console keys pane
                  needs; NOT /admin/gateway-keys (minting admin keys
                  is full_admin only); NOT /admin/secrets (v2.6:
                  provider credentials are secret_manager's duty)
    secret_manager  provider-credential management (console-pasted
                  cloud keys) — v2.6 split from key_manager (GL-R14)

Unknown role names FAIL CLOSED (no /admin access) with a parse-time
warning. Serving surface (/v1/*) is not RBAC's business — consumers
get virtual keys.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Any

logger = logging.getLogger(__name__)

# A policy rule row: (decision, method, pattern). decision is "allow"
# or "deny"; method is an HTTP verb or "ANY"; pattern matches the path
# exactly, or by prefix when it ends with "/*" (same loose prefix the
# core admin gate uses). FIRST matching row wins; a role with no
# matching row (or an unknown role) denies.
Rule = tuple[str, str, str]

DEFAULT_ROLE = "full_admin"

ROLES: dict[str, list[Rule]] = {
    "full_admin": [
        ("allow", "ANY", "/admin/*"),
    ],
    "ops_readonly": [
        # deny row first — first match wins over the wide allow below
        ("deny", "GET", "/admin/config"),   # raw text = key material
        ("allow", "GET", "/admin/*"),
    ],
    # v2.6 (GL-R14): provider-credential management is its own duty —
    # key_manager keeps consumer-key CRUD only. A role that mints
    # consumer keys must not also rotate cloud credentials.
    "key_manager": [
        ("allow", "GET", "/admin/keys"),
        ("allow", "POST", "/admin/keys"),
        ("allow", "GET", "/admin/stats"),
    ],
    "secret_manager": [
        ("allow", "GET", "/admin/secrets"),
        ("allow", "POST", "/admin/secrets"),  # console-pasted provider credentials
    ],
}


def _path_match(pattern: str, path: str) -> bool:
    """Exact path, or prefix when the pattern ends with "/*" — via the
    shared SEGMENT-AWARE matcher (GL-R18): "/admin/*" matches
    "/admin/keys" but never "/administrator"."""
    from .auth import path_scope
    if pattern.endswith("/*"):
        return path_scope(pattern[:-2], path)
    return pattern == path


def allowed(role: str, method: str, path: str) -> bool:
    """True when the policy table permits (method, path) for role.
    First matching rule row decides; no match or unknown role → deny."""
    for decision, r_method, pattern in ROLES.get(role, ()):
        if r_method != "ANY" and r_method != method:
            continue
        if _path_match(pattern, path):
            return decision == "allow"
    return False


def _sha256_hex(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _mask(key: str) -> str:
    """Log-safe display of a role-entry key (never full plaintext)."""
    return (key[:6] + "…") if key else "<empty>"


def _parse_role_entries(raw: dict[str, Any]) -> list[dict[str, str]]:
    """admin_key_roles section → [{key, role, note}]. Malformed entries
    are skipped with a warning; duplicates keep the first."""
    out: list[dict[str, str]] = []
    section = raw.get("admin_key_roles")
    if section is None:
        return out
    if not isinstance(section, list):
        logger.warning("admin_key_roles: expected a list — section ignored")
        return out
    seen: set[str] = set()
    for item in section:
        if not isinstance(item, dict):
            logger.warning("admin_key_roles: entry must be an object — skipped")
            continue
        key = str(item.get("key") or "").strip()
        role = str(item.get("role") or "").strip()
        if not key or not role:
            logger.warning("admin_key_roles: entry needs key + role — skipped")
            continue
        if key in seen:
            logger.warning("admin_key_roles: duplicate key %s (first wins)",
                           _mask(key))
            continue
        seen.add(key)
        if role not in ROLES:
            logger.warning("admin_key_roles: unknown role %r for key %s — "
                           "FAILS CLOSED (no /admin access)", role, _mask(key))
        out.append({"key": key, "role": role,
                    "note": str(item.get("note") or "").strip()})
    return out


def _lint_entries(entries: list[dict[str, str]], cfg) -> None:
    """Warn when a role entry can never match: its key is not a
    configured gateway_keys value (plaintext or hash form)."""
    if not entries:
        return
    gw_keys = [str(k) for k in (getattr(cfg, "gateway_keys", None) or [])]
    if not gw_keys:
        logger.warning("admin_key_roles: entries present but gateway_keys "
                       "is empty — every entry is dead (tokens 401 before "
                       "RBAC runs)")
        return
    known_plain = {k for k in gw_keys if len(k) != 64}
    known_hash = {k for k in gw_keys if len(k) == 64}
    known_hash |= {_sha256_hex(p) for p in known_plain}
    for e in entries:
        k = e["key"]
        if k in known_plain or (len(k) == 64 and k in known_hash):
            continue
        logger.warning("admin_key_roles: key %s (role %r) matches no "
                       "gateway_keys value — dead entry", _mask(k), e["role"])


class AdminRoleMap:
    """token → role assignments from the config's admin_key_roles
    section, cached per process. Re-parses only when the active
    store's version token moves (the same trigger as the core config
    hot-reload); an unreadable store keeps the last good entries,
    mirroring GatewayConfig's philosophy."""

    def __init__(self) -> None:
        self._store: Any = None
        self._version: Any = object()  # sentinel ≠ any real token
        self._entries: list[dict[str, str]] = []

    def refresh(self, cfg) -> None:
        """Re-parse when the store changed (cheap version check first)."""
        store = getattr(cfg, "store", None)
        if store is not self._store:  # singleton swapped (tests)
            self._store = store
            self._version = object()
        if store is None:
            return
        version = store.version()
        if version == self._version:
            return
        raw = store.load()
        if isinstance(raw, dict):
            self._entries = _parse_role_entries(raw)
            _lint_entries(self._entries, cfg)
        # unreadable (None) or non-dict: keep last good entries
        self._version = version

    def role_for(self, cfg, token: str) -> str:
        """The role assigned to this bearer token; '' when unassigned
        (the caller applies DEFAULT_ROLE). Matching mirrors the core's
        gateway_keys resolution: plaintext equality, or constant-time
        hash comparison for 64-hex entries."""
        self.refresh(cfg)
        if not token or not self._entries:
            return ""
        token_hash = _sha256_hex(token)
        for e in self._entries:
            k = e["key"]
            if k == token:
                return e["role"]
            if len(k) == 64 and hmac.compare_digest(k, token_hash):
                return e["role"]
        return ""


# Process-wide map (roles are config: one cache per process; tier-D
# replicas each cache against the shared store — the same consistency
# the core config reload already gives).
_MAP = AdminRoleMap()


def get_admin_role_map() -> AdminRoleMap:
    return _MAP
