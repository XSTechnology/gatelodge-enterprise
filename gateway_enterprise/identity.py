"""SSO identity — the enterprise identity ContextVar (tier C).

Single definition (same discipline as the core's auth ContextVars in
gateway/auth.py): links.py SETS it during SSO resolution; rbac_link
READS it for role resolution and audit identity. Request-scoped —
Starlette runs each request in its own task context, so a value set
during one request never leaks into the next (the core's
_REQUEST_KEY_ID relies on the same property).

Shape: {"sub": str, "email": str, "tenant": str, "role": str,
        "via": "sso"} — or None when the request is not SSO-authenticated.
"""
from __future__ import annotations

import contextvars
from typing import Any

# THE definition — import from here, never redefine (two same-named
# ContextVars are distinct objects and silently stop sharing state).
_SSO_IDENTITY = contextvars.ContextVar("gw_sso_identity", default=None)


def set_identity(identity: dict[str, Any] | None) -> None:
    """Set (or clear) the request's SSO identity."""
    _SSO_IDENTITY.set(identity)


def get_identity() -> dict[str, Any] | None:
    """The request's SSO identity, or None when not SSO-authenticated."""
    return _SSO_IDENTITY.get()
