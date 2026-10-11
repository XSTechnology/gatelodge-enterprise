"""Gatelodge teams/workspaces — multi-tenant governance (v2.0 "The
Gatehouse", enterprise-claimed ``teams``).

Teams group virtual (consumer) keys into billed, budgeted, delegably-
administered units — the procurement table-stakes layer:

    attribution   the auth-pipeline team link resolves the consuming
                  key → team and sets the core's _REQUEST_TEAM; the
                  request log stamps rec["team"] and _charge_request
                  charges a team:<id> spend scope (same ledger machinery
                  as key/session/backend scopes — tier D replicas share
                  it for free).

    budgets       a team may carry budget_usd_month and/or
                  budget_tokens_month; the enterprise pre-gate 429s at
                  cap (the _key_budget_gate shape).

    rollups       GET /teams — members, month-to-date spend, budget
                  state, per-member breakdown (the admin surface).

    delegation    TEAM ADMINS: gateway-style keys (sha256 hashes under
                  the team's ``admins`` list) with authority scoped to
                  their team ONLY — they view their team's rollups and
                  mint/revoke their team's virtual keys on
                  /teams/{team}/keys. They are NOT gateway_keys: the
                  core pipeline fail-closes them out of /admin/*
                  (401). A team-admin hash that also appears in
                  gateway_keys is a FULL admin — surfaced as a warning
                  in GET /teams (the footgun is the operator's to fix).

Config (raw section; the core parses nothing teams-specific — it only
carries _REQUEST_TEAM once the link sets it):

    "teams": {
      "enabled": true,
      "definitions": {
        "eng": {
          "name": "Engineering",
          "keys": ["program-x", "ci-runner"],      // virtual key ids
          "budget_usd_month": 50.0,
          "budget_tokens_month": 1000000,
          "admins": ["<sha256 of a gateway-style key>"]
        }
      }
    }

Mutations (team CRUD, team key mint/revoke) ride _mutate_config —
optimistic-CAS + hash-chained audit, exactly like every other config
mutation. Key minting mirrors the core's /admin/keys: plaintext
returns ONCE, the hash is what's stored.
"""
from __future__ import annotations

import logging
import re
import secrets
import time
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from gateway import config as gw_config
from gateway.auth import _REQUEST_TEAM, _resolve_bearer, _sha256_hex

logger = logging.getLogger(__name__)

_KEY_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,40}")


def _error(status: int, message: str, code: str | None = None):
    return JSONResponse(status_code=status, content={"error": {
        "message": message, "type": "invalid_request_error",
        "param": None, "code": code}})


# ── settings: raw section, version-cached (SessionBudgetState pattern) ─────

class TeamsState:
    """Re-parses the raw teams section when the active store's version
    token moves; keeps last good on unreadable load."""

    def __init__(self) -> None:
        self._store: Any = None
        self._version: Any = object()
        self._definitions: dict[str, dict[str, Any]] = {}

    def definitions(self) -> dict[str, dict[str, Any]]:
        cfg = gw_config.get_gateway_config()
        store = getattr(cfg, "store", None)
        if store is not self._store:
            self._store = store
            self._version = object()
        if store is None:
            return self._definitions
        version = store.version()
        if version == self._version:
            return self._definitions
        raw = store.load()
        defs: dict[str, dict[str, Any]] = {}
        if isinstance(raw, dict):
            sec = raw.get("teams")
            sec = sec.get("definitions") if isinstance(sec, dict) else None
            if isinstance(sec, dict):
                for tid, spec in sec.items():
                    if not isinstance(spec, dict):
                        continue
                    tid = str(tid).strip()
                    if not tid or not _KEY_ID_RE.fullmatch(tid):
                        continue
                    keys = [str(k).strip() for k in (spec.get("keys") or [])
                            if str(k).strip()]
                    admins = {str(a).strip().lower()
                              for a in (spec.get("admins") or [])
                              if len(str(a)) == 64}
                    defs[tid] = {
                        "name": str(spec.get("name") or tid),
                        "keys": keys,
                        "admins": admins,
                        "budget_usd_month": max(
                            0.0, float(spec.get("budget_usd_month") or 0)),
                        "budget_tokens_month": max(
                            0, int(spec.get("budget_tokens_month") or 0)),
                    }
        self._definitions = defs
        self._version = version
        return self._definitions


_STATE = TeamsState()


def team_for_key(key_id: str) -> str | None:
    """The team a virtual key belongs to (first match; a key listed in
    two teams is the operator's config smell — warned at load)."""
    if not key_id or key_id == "admin":
        return None
    for tid, d in _STATE.definitions().items():
        if key_id in d["keys"]:
            return tid
    return None


def reset_teams_state() -> None:
    """Drop cached definitions (tests)."""
    global _STATE
    _STATE = TeamsState()


# ── auth-pipeline links ────────────────────────────────────────────────────

def team_link(cfg, request: Request, key_id: str) -> JSONResponse | None:
    """Enterprise link: attribute the consuming virtual key to its team
    (sets the core's _REQUEST_TEAM — the charge hook and request log
    read it). Inert without team membership; zero-cost otherwise."""
    tid = team_for_key(key_id)
    if tid:
        _REQUEST_TEAM.set(tid)
    return None


def team_budget_link(cfg, request: Request,
                     key_id: str) -> JSONResponse | None:
    """Enterprise link: pre-gate /v1 requests on the TEAM's monthly
    budgets (USD + tokens — the _key_budget_gate shape). Inert when the
    key's team carries no budgets. Cache hits stay available (the gate
    runs in the pipeline, before the response cache is consulted —
    same posture as session budgets)."""
    if not request.url.path.startswith("/v1/"):
        return None
    tid = _REQUEST_TEAM.get()
    if not tid:
        return None
    d = _STATE.definitions().get(tid) or {}
    usd_b = float(d.get("budget_usd_month") or 0)
    tok_b = int(d.get("budget_tokens_month") or 0)
    if usd_b <= 0 and tok_b <= 0:
        return None
    import gateway.app as gw_app  # lazy: app imports nothing from here
    spend = gw_app._scope_spend(f"team:{tid}")
    if tok_b > 0 and spend["tokens"] >= tok_b:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": (f"team {tid!r}: monthly token budget "
                            f"exhausted ({spend['tokens']}/{tok_b} "
                            "tokens)"),
                "type": "rate_limit_error", "param": None,
                "code": "team_budget_exceeded"}})
    if usd_b > 0 and spend["usd"] >= usd_b:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": (f"team {tid!r}: monthly spend budget "
                            f"exhausted (${spend['usd']:.2f}/"
                            f"${usd_b:.2f})"),
                "type": "rate_limit_error", "param": None,
                "code": "team_budget_exceeded"}})
    return None


# ── the /teams surface (add_extra_router mount) ────────────────────────────

def _bearer(request: Request) -> str:
    auth = request.headers.get("authorization") or ""
    return auth[7:].strip() if auth[:7].lower() == "bearer " else ""


def _principal(request: Request) -> tuple[str, str | None]:
    """('full', None) for gateway keys; ('team', tid) for a team-admin
    key; ('', None) otherwise. Team-admin hashes are matched per team —
    a hash in BOTH a team's admins and gateway_keys resolves FULL (the
    stronger grant wins; GET /teams warns about it)."""
    token = _bearer(request)
    if not token:
        return "", None
    if _resolve_bearer(gw_config.get_gateway_config(), token) == "admin":
        return "full", None
    h = _sha256_hex(token)
    for tid, d in _STATE.definitions().items():
        if h in d["admins"]:
            return "team", tid
    return "", None


def _team_rollup(tid: str, d: dict[str, Any],
                 cfg) -> dict[str, Any]:
    import gateway.app as gw_app
    spend = gw_app._scope_spend(f"team:{tid}")
    usd_b = float(d.get("budget_usd_month") or 0)
    tok_b = int(d.get("budget_tokens_month") or 0)
    members = []
    for kid in d["keys"]:
        kspend = gw_app._scope_spend(f"key:{kid}")
        entry = (cfg.api_keys or {}).get(kid) or {}
        members.append({"id": kid,
                        "owner": entry.get("owner") or "",
                        "tokens": kspend["tokens"],
                        "usd": round(kspend["usd"], 6)})
    out: dict[str, Any] = {
        "id": tid, "name": d.get("name") or tid,
        "members": members,
        "month_spend": {"tokens": spend["tokens"],
                        "usd": round(spend["usd"], 6)},
        "budgets": {"usd_month": usd_b, "tokens_month": tok_b},
        "over_budget": bool((usd_b > 0 and spend["usd"] >= usd_b)
                            or (tok_b > 0 and spend["tokens"] >= tok_b)),
        "admin_keys": len(d.get("admins") or []),
    }
    return out


class TeamKeyPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    owner: str = ""
    note: str = ""
    monthly_token_budget: int = 0
    monthly_usd_budget: float = 0.0
    models: list[str] | None = None
    rate_limit_rpm: int = 0
    rate_limit_tpm: int = 0
    delete: bool = False


class TeamPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = ""
    budget_usd_month: float = 0.0
    budget_tokens_month: int = 0
    admins: list[str] | None = None     # sha256 hashes
    delete: bool = False


def build_router() -> APIRouter:
    """The teams surface — mounted at /teams via add_extra_router. The
    prefix owns its auth: gateway keys (full) and team-admin keys
    (scoped to their team). NOT part of the core pipeline — a team
    admin without a gateway key gets 401 on /admin/* (fail-closed)."""
    import gateway.app as gw_app
    r = APIRouter()

    def _authorize(request: Request, tid: str | None = None):
        """401 response when the principal may not act (optionally on
        the given team); None when allowed. Returns (kind, team)."""
        kind, principal_team = _principal(request)
        if not kind:
            return _error(401, "missing or invalid key (gateway admin "
                          "or team-admin key required)",
                          code="invalid_api_key"), ("", None)
        if kind == "team" and tid is not None and principal_team != tid:
            return _error(403, f"team-admin authority is scoped to "
                          f"{principal_team!r}", code="team_scope"), \
                (kind, principal_team)
        return None, (kind, principal_team)

    @r.get("")
    @r.get("/")
    def list_teams(request: Request):
        denied, (kind, pteam) = _authorize(request)
        if denied:
            return denied
        cfg = gw_config.get_gateway_config()
        cfg.maybe_reload()
        defs = _STATE.definitions()
        visible = (defs if kind == "full"
                   else {pteam: defs[pteam]} if pteam in defs else {})
        teams = [_team_rollup(tid, d, cfg) for tid, d in
                 sorted(visible.items())]
        out: dict[str, Any] = {"teams": teams, "scope": kind}
        if kind == "full":
            # footgun lint: a team-admin hash that is ALSO a gateway
            # key is a full admin on every surface
            gw_hashes = {str(k).lower() for k in (cfg.gateway_keys or [])
                         if len(str(k)) == 64}
            clashes = sorted({h for d in defs.values()
                              for h in d["admins"] if h in gw_hashes})
            if clashes:
                out["warnings"] = [
                    f"{len(clashes)} team-admin key hash(es) are also in "
                    "gateway_keys — those keys are FULL admins, not "
                    "team-scoped; remove them from gateway_keys"]
        return out

    @r.get("/{tid}")
    def team_detail(tid: str, request: Request):
        denied, _ = _authorize(request, tid)
        if denied:
            return denied
        d = _STATE.definitions().get(tid)
        if d is None:
            return _error(404, f"team {tid!r} not found",
                          code="team_not_found")
        cfg = gw_config.get_gateway_config()
        cfg.maybe_reload()
        return _team_rollup(tid, d, cfg)

    @r.get("/{tid}/keys")
    def team_keys(tid: str, request: Request):
        denied, _ = _authorize(request, tid)
        if denied:
            return denied
        d = _STATE.definitions().get(tid)
        if d is None:
            return _error(404, f"team {tid!r} not found",
                          code="team_not_found")
        cfg = gw_config.get_gateway_config()
        cfg.maybe_reload()
        out = []
        for kid in d["keys"]:
            entry = (cfg.api_keys or {}).get(kid)
            out.append({
                "id": kid,
                "present": entry is not None,
                "owner": (entry or {}).get("owner") or "",
                "note": (entry or {}).get("note") or "",
                "monthly_token_budget": (entry or {})
                .get("monthly_token_budget") or 0,
                "monthly_usd_budget": (entry or {})
                .get("monthly_usd_budget") or 0,
                "rate_limit_rpm": (entry or {}).get("rate_limit_rpm") or 0,
                "rate_limit_tpm": (entry or {}).get("rate_limit_tpm") or 0,
            })
        return {"team": tid, "keys": out}

    @r.post("/{tid}/keys")
    def team_key_upsert(tid: str, payload: TeamKeyPayload,
                        request: Request):
        denied, _ = _authorize(request, tid)
        if denied:
            return denied
        d = _STATE.definitions().get(tid)
        if d is None:
            return _error(404, f"team {tid!r} not found",
                          code="team_not_found")
        kid = payload.id.strip()
        if not kid or not _KEY_ID_RE.fullmatch(kid):
            return _error(400, "key id: letters/digits/dots/dashes/"
                          "underscores, max 40 chars", param="id")
        cfg = gw_config.get_gateway_config()
        cfg.maybe_reload()
        if payload.delete:
            if kid not in d["keys"]:
                return _error(404, f"key {kid!r} is not a member of "
                              f"team {tid!r}", code="key_not_found")

            def _del(raw: dict) -> None:
                raw["api_keys"] = [k for k in (raw.get("api_keys") or [])
                                   if str((k or {}).get("id")) != kid]
                tdef = ((raw.get("teams") or {}).get("definitions")
                        or {}).get(tid)
                if isinstance(tdef, dict):
                    tdef["keys"] = [k for k in (tdef.get("keys") or [])
                                    if k != kid]
            gw_app._mutate_config(_del, f"team-key-revoke {tid}/{kid}")
            return {"revoked": kid, "team": tid}
        if not cfg.gateway_keys:
            return _error(400, "configure a gateway ADMIN key first "
                          "(gateway_keys) — creating virtual keys enables "
                          "auth and would otherwise lock out the admin "
                          "surface", param="gateway_keys")
        plaintext = f"vk-{secrets.token_urlsafe(24)}"
        entry = {
            "id": kid,
            "key_hash": _sha256_hex(plaintext),  # hashed at rest
            "owner": payload.owner.strip(),
            "note": payload.note.strip(),
            "monthly_token_budget": max(0, int(
                payload.monthly_token_budget or 0)),
            "monthly_usd_budget": max(0.0, float(
                payload.monthly_usd_budget or 0)),
            "models": [str(m).strip() for m in (payload.models or [])
                       if str(m).strip()],
            "rate_limit_rpm": max(0, int(payload.rate_limit_rpm or 0)),
            "rate_limit_tpm": max(0, int(payload.rate_limit_tpm or 0)),
        }

        def _upsert(raw: dict) -> None:
            # ONE atomic mutation: the key entry + team membership
            lst = [k for k in (raw.get("api_keys") or [])
                   if str((k or {}).get("id")) != kid]
            lst.append(entry)
            raw["api_keys"] = lst
            teams = raw.setdefault("teams", {}).setdefault(
                "definitions", {})
            tdef = teams.setdefault(tid, {"keys": []})
            if not isinstance(tdef, dict):
                tdef = teams[tid] = {"keys": []}
            keys = [k for k in (tdef.get("keys") or []) if k != kid]
            keys.append(kid)
            tdef["keys"] = keys

        gw_app._mutate_config(_upsert, f"team-key-create {tid}/{kid}")
        # plaintext returns ONCE (the hash is what's stored)
        return {"created": kid, "team": tid, "key": plaintext,
                "monthly_token_budget": entry["monthly_token_budget"],
                "monthly_usd_budget": entry["monthly_usd_budget"]}

    @r.post("/{tid}")
    def team_upsert(tid: str, payload: TeamPayload, request: Request):
        denied, (kind, _) = _authorize(request)
        if denied:
            return denied
        if kind != "full":
            # team CRUD is a GATEWAY-ADMIN operation — team admins are
            # scoped to their team's keys, not to the team registry
            return _error(403, "team CRUD requires a gateway admin key",
                          code="team_scope")
        if not tid or not _KEY_ID_RE.fullmatch(tid):
            return _error(400, "team id: letters/digits/dots/dashes/"
                          "underscores, max 40 chars", param="tid")

        def _delete(raw: dict) -> None:
            ((raw.get("teams") or {}).get("definitions") or {}).pop(
                tid, None)

        if payload.delete:
            gw_app._mutate_config(_delete, f"team-delete {tid}")
            return {"deleted": tid}
        admins = [a for a in (payload.admins or [])
                  if len(str(a)) == 64]
        tdef: dict[str, Any] = {"name": payload.name.strip() or tid,
                                "admins": admins,
                                "budget_usd_month": max(
                                    0.0, float(
                                        payload.budget_usd_month or 0)),
                                "budget_tokens_month": max(
                                    0, int(
                                        payload.budget_tokens_month or 0))}

        def _upsert(raw: dict) -> None:
            defs = raw.setdefault("teams", {}).setdefault(
                "definitions", {})
            old = defs.get(tid) or {}
            tdef["keys"] = (old.get("keys") or [])   # membership preserved
            defs[tid] = tdef

        gw_app._mutate_config(_upsert, f"team-upsert {tid}")
        return {"upserted": tid, "team": tdef}

    return r
