"""SCIM 2.0 provisioning (RFC 7644) — IdP-driven user provisioning,
mounted via the set_extra_router seam (the fourth seam).

The IdP (Okta, Entra, …) drives provisioning; each SCIM User maps to a
VIRTUAL GATEWAY KEY (an api_keys entry): create → mint, deactivate /
delete → revoke. Mutations go through the core's config-mutation
chokepoint (`_mutate_config`), so every provisioning event lands in the
hash-chained audit trail and rides the CAS contract (no cross-replica
clobber; replicas pick the change up via the store's version token).

Config section (raw-read like every enterprise section):

    "scim": {
      "enabled": true,
      "token_hash": "<sha256 of the IdP's SCIM bearer token>"
    }

The bearer is the IDP's token — not a gateway credential. It is hashed
at rest like every other secret, and every SCIM request is checked
against it (constant-time). The router is mounted at boot when enabled
(changing the section needs a restart); the token_hash is read from
the ACTIVE store per request, so rotating it hot-reloads.

Security invariants:
- ONLY entries whose api_keys id starts with "scim-" are manageable
  through this surface — an IdP compromise cannot revoke or inspect
  admin-managed keys.
- SCIM never carries the gateway ADMIN key material; the minted
  CONSUMER key returns exactly once at creation, in a custom schema
  extension (urn:ietf:params:scim:schemas:extension:gatelodge:1.0) for
  the IdP to deliver to the user.
- Provisioning events are audited (scim-user-create / -update / -delete)
  in the same hash-chained trail as every other config mutation.

Deactivation model: api_keys entries have no "disabled" flag (the core
normalizer keeps a fixed field set), so deactivate/delete REMOVE the
entry — true deprovisioning (SOC2 CC6.2): the key stops authenticating
immediately. Re-activation is re-provisioning (a new key is minted).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

SCIM_USER = "urn:ietf:params:scim:schemas:core:2.0:User"
SCIM_LIST = "urn:ietf:params:scim:schemas:core:2.0:ListResponse"
SCIM_ERROR = "urn:ietf:params:scim:schemas:core:2.0:Error"
GW_EXT = "urn:ietf:params:scim:schemas:extension:gatelodge:1.0"

# SCIM-managed api_keys ids: this prefix is the management boundary.
_ID_PREFIX = "scim-"
_USER_NAME_MAX = 128


def _sha256_hex(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── config + auth ──────────────────────────────────────────────────────────

def _scim_section() -> dict:
    """The raw scim section from the ACTIVE config store (per request —
    token rotation hot-reloads; mounting itself is boot-time)."""
    try:
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load()
        scim = raw.get("scim") if isinstance(raw, dict) else None
        return scim if isinstance(scim, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _authorized(request: Request) -> bool:
    """The IdP's SCIM bearer (constant-time against the stored hash)."""
    want = str(_scim_section().get("token_hash") or "").strip()
    if not want:
        return False
    auth = request.headers.get("authorization") or ""
    token = auth[7:].strip() if auth[:7].lower() == "bearer " else ""
    return bool(token) and hmac.compare_digest(
        want, _sha256_hex(token))


def _scim_error(status: int, detail: str,
                scim_type: str = "") -> JSONResponse:
    body: dict = {"schemas": [SCIM_ERROR],
                  "status": str(status), "detail": detail[:256]}
    if scim_type:
        body["scimType"] = scim_type
    return JSONResponse(status_code=status, content=body)


def _guard(request: Request) -> JSONResponse | None:
    """401 for every SCIM request without the IdP bearer."""
    if _authorized(request):
        return None
    return _scim_error(401, "invalid SCIM bearer token")


# ── api_keys mapping ───────────────────────────────────────────────────────

def _entry_from_raw(raw: dict, kid: str) -> dict | None:
    for e in (raw.get("api_keys") or []):
        if isinstance(e, dict) and str(e.get("id")) == kid:
            return e
    return None


def _scim_entries(raw: dict) -> list[dict]:
    return [e for e in (raw.get("api_keys") or [])
            if isinstance(e, dict)
            and str(e.get("id") or "").startswith(_ID_PREFIX)]


def _user_resource(entry: dict, include_ext: dict | None = None) -> dict:
    """An api_keys entry → a SCIM User resource. userName rides the
    entry's note field, displayName its owner field."""
    out = {
        "schemas": [SCIM_USER],
        "id": str(entry.get("id") or ""),
        "userName": str(entry.get("note") or ""),
        "displayName": str(entry.get("owner") or ""),
        "active": True,  # entries present = active; revoked = removed
        "meta": {"resourceType": "User",
                 "location": f"/scim/v2/Users/{entry.get('id')}"},
    }
    if include_ext:
        out["schemas"] = [SCIM_USER, GW_EXT]
        out[GW_EXT] = include_ext
    return out


def _mutate(mutator, action: str) -> None:
    """Provisioning mutation through the CORE chokepoint: CAS on the
    config store + hash-chained audit + hot reload — the same contract
    as every admin mutation."""
    import gateway.app as gw_app  # lazy: app imports nothing from here
    gw_app._mutate_config(mutator, action)


def _mint_key(raw: dict, user_name: str, display_name: str) -> tuple[dict, str]:
    """Create the api_keys entry (key hashed at rest) + return it with
    the plaintext (returned to the IdP exactly ONCE)."""
    kid = f"{_ID_PREFIX}{uuid.uuid4().hex[:12]}"
    plaintext = f"vk-{secrets.token_urlsafe(24)}"
    lst = [e for e in (raw.get("api_keys") or [])
           if not (isinstance(e, dict) and str(e.get("note") or "")
                   .lower() == user_name.lower()
                   and str(e.get("id") or "").startswith(_ID_PREFIX))]
    entry = {"id": kid, "key_hash": _sha256_hex(plaintext),
             "owner": display_name, "note": user_name,
             "monthly_token_budget": 0, "monthly_usd_budget": 0.0,
             "models": [], "rate_limit_rpm": 0, "rate_limit_tpm": 0}
    lst.append(entry)

    def _apply(r: dict) -> None:
        r["api_keys"] = lst

    _mutate(_apply, f"scim-user-create {user_name}")
    return entry, plaintext


# ── the router ─────────────────────────────────────────────────────────────

def build_router() -> APIRouter:
    """The SCIM v2 Users router — mounted by enterprise_main via
    set_extra_router(router, prefix="/scim") when scim.enabled."""
    r = APIRouter()

    @r.post("/v2/Users")
    async def create_user(request: Request):
        if (e := _guard(request)) is not None:
            return e
        try:
            body = json.loads(await request.body() or b"{}")
        except json.JSONDecodeError:
            return _scim_error(400, "invalid JSON body")
        user_name = str(body.get("userName") or "").strip()
        display_name = str(body.get("displayName")
                           or user_name).strip()[:_USER_NAME_MAX]
        if not user_name or len(user_name) > _USER_NAME_MAX:
            return _scim_error(400, "userName is required (max "
                               f"{_USER_NAME_MAX} chars)", "invalidValue")
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load() or {}
        if any(str(e.get("note") or "").lower() == user_name.lower()
               for e in _scim_entries(raw)):
            return _scim_error(409, f"user {user_name!r} already exists",
                               "uniqueness")
        entry, plaintext = _mint_key(raw, user_name, display_name)
        return JSONResponse(
            status_code=201,
            content=_user_resource(
                entry, include_ext={"gateway_key": plaintext}),
            headers={"Location": f"/scim/v2/Users/{entry['id']}"})

    @r.get("/v2/Users")
    async def list_users(request: Request):
        if (e := _guard(request)) is not None:
            return e
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load() or {}
        entries = _scim_entries(raw)
        # minimal filter: userName eq "..." (case-insensitive) — the
        # form IdPs actually send for sync
        flt = request.query_params.get("filter") or ""
        m = re.search(r'userName\s+eq\s+"([^"]*)"', flt, re.IGNORECASE)
        if m:
            want = m.group(1).lower()
            entries = [e for e in entries
                       if str(e.get("note") or "").lower() == want]
        elif flt.strip():
            return _scim_error(400, "unsupported filter (only "
                               'userName eq "…" is supported)')
        resources = [_user_resource(e) for e in entries]
        return {"schemas": [SCIM_LIST], "totalResults": len(resources),
                "startIndex": 1, "itemsPerPage": len(resources),
                "Resources": resources}

    @r.get("/v2/Users/{kid}")
    async def get_user(kid: str, request: Request):
        if (e := _guard(request)) is not None:
            return e
        if not kid.startswith(_ID_PREFIX):
            return _scim_error(404, "user not found (not SCIM-managed)")
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load() or {}
        entry = _entry_from_raw(raw, kid)
        if entry is None:
            return _scim_error(404, f"user {kid!r} not found")
        return _user_resource(entry)

    def _revoke(raw: dict, kid: str) -> bool:
        entry = _entry_from_raw(raw, kid)
        if entry is None:
            return False

        def _del(r: dict) -> None:
            r["api_keys"] = [e for e in (r.get("api_keys") or [])
                             if str((e or {}).get("id")) != kid]

        user_name = str(entry.get("note") or kid)
        _mutate(_del, f"scim-user-delete {user_name}")
        return True

    @r.delete("/v2/Users/{kid}")
    async def delete_user(kid: str, request: Request):
        if (e := _guard(request)) is not None:
            return e
        if not kid.startswith(_ID_PREFIX):
            return _scim_error(404, "user not found (not SCIM-managed)")
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load() or {}
        if not _revoke(raw, kid):
            return _scim_error(404, f"user {kid!r} not found")
        return JSONResponse(status_code=204, content=None)

    def _apply_patch(entry: dict, ops: list) -> tuple[bool, bool] | None:
        """RFC 7644 Operations → (deactivate?, owner_change?). Handles
        both path-style ({op, path, value}) and value-object-style
        ({op, value: {active: false}} — the Okta form)."""
        deactivate = False
        owner = None
        for op in (ops or []):
            if not isinstance(op, dict):
                continue
            o = str(op.get("op") or "").lower()
            if o not in ("replace", "add", "remove"):
                continue
            path = str(op.get("path") or "")
            value = op.get("value")
            attrs = value if isinstance(value, dict) else {}
            if path == "active" or "active" in attrs:
                active = (attrs.get("active")
                          if "active" in attrs else value)
                if active is False:
                    deactivate = True
            if path == "displayName" or "displayName" in attrs:
                nv = (attrs.get("displayName")
                      if "displayName" in attrs else value)
                if isinstance(nv, str) and nv.strip():
                    owner = nv.strip()[:_USER_NAME_MAX]
        return deactivate, owner

    @r.patch("/v2/Users/{kid}")
    async def patch_user(kid: str, request: Request):
        if (e := _guard(request)) is not None:
            return e
        if not kid.startswith(_ID_PREFIX):
            return _scim_error(404, "user not found (not SCIM-managed)")
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load() or {}
        entry = _entry_from_raw(raw, kid)
        if entry is None:
            return _scim_error(404, f"user {kid!r} not found")
        try:
            body = json.loads(await request.body() or b"{}")
        except json.JSONDecodeError:
            return _scim_error(400, "invalid JSON body")
        parsed = _apply_patch(entry, body.get("Operations") or [])
        if parsed is None:
            return _scim_error(400, "unsupported patch")
        deactivate, owner = parsed
        if deactivate:
            if not _revoke(raw, kid):
                return _scim_error(404, f"user {kid!r} not found")
            return _user_resource(entry)   # last known state, now revoked
        if owner:
            def _own(r: dict) -> None:
                for e in (r.get("api_keys") or []):
                    if str((e or {}).get("id")) == kid:
                        e["owner"] = owner
            _mutate(_own, f"scim-user-update {entry.get('note') or kid}")
            entry = dict(entry, owner=owner)
        return _user_resource(entry)

    @r.put("/v2/Users/{kid}")
    async def put_user(kid: str, request: Request):
        if (e := _guard(request)) is not None:
            return e
        if not kid.startswith(_ID_PREFIX):
            return _scim_error(404, "user not found (not SCIM-managed)")
        from gateway.config import get_gateway_config
        raw = get_gateway_config().store.load() or {}
        entry = _entry_from_raw(raw, kid)
        if entry is None:
            return _scim_error(404, f"user {kid!r} not found")
        try:
            body = json.loads(await request.body() or b"{}")
        except json.JSONDecodeError:
            return _scim_error(400, "invalid JSON body")
        if body.get("active") is False:
            if not _revoke(raw, kid):
                return _scim_error(404, f"user {kid!r} not found")
            return _user_resource(entry)
        display = str(body.get("displayName")
                      or entry.get("owner") or "").strip()
        if display and display != entry.get("owner"):
            def _own(r: dict) -> None:
                for e in (r.get("api_keys") or []):
                    if str((e or {}).get("id")) == kid:
                        e["owner"] = display[:_USER_NAME_MAX]
            _mutate(_own,
                    f"scim-user-update {entry.get('note') or kid}")
            entry = dict(entry, owner=display[:_USER_NAME_MAX])
        return _user_resource(entry)

    return r
