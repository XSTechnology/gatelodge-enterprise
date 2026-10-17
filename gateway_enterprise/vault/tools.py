"""Content Vault — the admin surface (/admin/vault) + offline tool CLI.

Admin router (mounted via add_extra_router — auth-owns-prefix): the
endpoints carry their own admin-gate mirroring the core's /admin
semantics (open mode stays open; otherwise a gateway ADMIN key is
required, virtual keys 403).

  GET  /admin/vault/status     — mode, parts, disk, records, dropped,
                                 chain head, key versions
  POST /admin/vault/verify     — chain integrity over ciphertext
                                 (no keys needed)
  POST /admin/vault/search     — {user|model|team, value} → matching
                                 ENCRYPTED records (Key S lives
                                 server-side; content never decrypts)
  POST /admin/vault/keycheck   — {key_v} → a challenge wrapped to that
                                 key version; the operator decrypts
                                 OFFLINE and posts the token back —
                                 possession proven, private key never
                                 uploaded
  POST /admin/vault/keycheck/verify — {challenge_id, token}

Offline CLI (customer-side reference tool — the private key never
touches the gateway):
  python -m gateway_enterprise.vault.tools decrypt  <parts…> --key priv.pem
  python -m gateway_enterprise.vault.tools search    <parts…> --search-key-hex … --user key-abc
  python -m gateway_enterprise.vault.tools verify    <parts…> --vault-id …
  python -m gateway_enterprise.vault.tools keycheck  <challenge.json> --key priv.pem
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from . import index as vindex
from . import keys as vkeys

# in-memory keycheck challenges: id → {expected_hash, key_v, expires}
_CHALLENGES: dict[str, dict] = {}


def build_router(get_writer) -> APIRouter:
    """get_writer() → the active VaultWriter (or None when the vault
    is licensed-but-not-running — endpoints report that state)."""
    r = APIRouter()

    def _admin_guard(request: Request) -> JSONResponse | None:
        """Mirror admin_gate semantics for this self-authed surface."""
        from gateway.auth import _resolve_bearer
        from gateway.config import get_gateway_config
        cfg = get_gateway_config()
        if not (cfg.gateway_keys or cfg.api_keys):
            return None                      # open mode — like /admin
        auth = request.headers.get("authorization") or ""
        token = auth[7:].strip() if auth[:7].lower() == "bearer " else ""
        if _resolve_bearer(cfg, token) == "admin":
            return None
        return JSONResponse(status_code=403, content={"error": {
            "message": "vault endpoints require a gateway admin key",
            "type": "invalid_request_error", "param": None,
            "code": "admin_key_required"}})

    @r.get("/status")
    async def status(request: Request):
        if (g := _admin_guard(request)):
            return g
        w = get_writer()
        if w is None:
            return {"running": False}
        out = w.status()
        out["running"] = True
        out["capture"] = _capture_stats()
        return out

    @r.post("/verify")
    async def verify(request: Request):
        if (g := _admin_guard(request)):
            return g
        w = get_writer()
        if w is None:
            return {"error": "vault not running"}
        return vindex.verify_chain(w._parts(), w.vault_id)

    @r.post("/search")
    async def search(request: Request):
        if (g := _admin_guard(request)):
            return g
        w = get_writer()
        if w is None:
            return {"error": "vault not running"}
        try:
            body = await request.json()
        except Exception:                    # noqa: BLE001
            return JSONResponse(status_code=400, content={
                "error": "JSON body required"})
        field = ("user" if body.get("user") else
                 "model" if body.get("model") else
                 "team" if body.get("team") else "")
        value = body.get(field)
        if not field or value is None:
            return JSONResponse(status_code=400, content={
                "error": "one of user|model|team is required"})
        hits = vindex.search(w._parts(), w.search_key, field, value)
        return {"field": field, "matches": len(hits),
                "records": [{k: h[k] for k in ("part", "line", "seq",
                                               "ts", "record")}
                            for h in hits]}

    @r.post("/keycheck")
    async def keycheck(request: Request):
        if (g := _admin_guard(request)):
            return g
        w = get_writer()
        if w is None:
            return {"error": "vault not running"}
        try:
            body = await request.json()
            key_v = int(body.get("key_v") or 0)
        except Exception:                    # noqa: BLE001
            return JSONResponse(status_code=400, content={
                "error": "key_v (int) required"})
        entry = next((e for e in w.registry if e["v"] == key_v), None)
        if entry is None:
            return JSONResponse(status_code=404, content={
                "error": f"no key version {key_v} in the registry"})
        # challenge: random token, wrapped to that key's public — the
        # operator decrypts OFFLINE and posts the token back
        token = secrets.token_hex(16)
        dek = os.urandom(32)
        nonce, ct = vkeys.seal(dek, token.encode())
        wraps = vkeys.wrap_dek(dek, w.registry)
        cid = secrets.token_hex(8)
        _CHALLENGES[cid] = {"expected": hashlib.sha256(
            token.encode()).hexdigest(), "key_v": key_v,
            "expires": time.time() + 300}
        # prune expired
        for k in [k for k, v in _CHALLENGES.items()
                  if v["expires"] < time.time()]:
            _CHALLENGES.pop(k, None)
        return {"challenge_id": cid, "key_v": key_v,
                "expires_in_s": 300,
                "challenge": {"dek": wraps, "nonce": nonce, "ct": ct}}

    @r.post("/keycheck/verify")
    async def keycheck_verify(request: Request):
        if (g := _admin_guard(request)):
            return g
        try:
            body = await request.json()
            cid = str(body.get("challenge_id") or "")
            token = str(body.get("token") or "")
        except Exception:                    # noqa: BLE001
            return JSONResponse(status_code=400, content={
                "error": "JSON body required"})
        ch = _CHALLENGES.pop(cid, None)
        if ch is None or ch["expires"] < time.time():
            return JSONResponse(status_code=404, content={
                "error": "unknown or expired challenge"})
        ok = hashlib.sha256(token.encode()).hexdigest() == ch["expected"]
        return {"key_v": ch["key_v"], "ok": ok}

    def _capture_stats() -> dict:
        try:
            from gateway import traffic
            return traffic.stats()
        except Exception:                    # noqa: BLE001
            return {}
    _capture_stats()  # referenced for lint parity

    return r


# ── offline CLI (customer-side reference tool) ─────────────────────────────

def _cli() -> int:
    import argparse
    import sys
    p = argparse.ArgumentParser(
        prog="vault-tools",
        description="Content Vault offline reference tool — the "
                    "private key never touches the gateway")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("decrypt", help="decrypt extracted records")
    d.add_argument("records", help="JSON file: the search response's "
                                   "records array, or one record per line")
    d.add_argument("--key", required=True, help="private key PEM")

    s = sub.add_parser("search", help="blind-index search (Key S)")
    s.add_argument("parts", nargs="+", help="vault part files")
    s.add_argument("--search-key-hex", required=True)
    s.add_argument("--user", dest="field_user", default=None)
    s.add_argument("--model", dest="field_model", default=None)
    s.add_argument("--team", dest="field_team", default=None)

    v = sub.add_parser("verify", help="chain integrity (no keys)")
    v.add_argument("parts", nargs="+")
    v.add_argument("--vault-id", required=True)

    k = sub.add_parser("keycheck", help="answer a keycheck challenge")
    k.add_argument("challenge", help="challenge JSON (from the admin "
                                     "endpoint)")
    k.add_argument("--key", required=True, help="private key PEM")

    args = p.parse_args()
    if args.cmd == "decrypt":
        raw = json.loads(Path(args.records).read_text(encoding="utf-8"))
        records = raw if isinstance(raw, list) else [raw]
        priv = Path(args.key).read_text(encoding="ascii")
        print(json.dumps(vindex.decrypt(records, priv), indent=2))
        return 0
    if args.cmd == "search":
        field = ("user" if args.field_user else "model" if args.field_model
                 else "team")
        value = (args.field_user or args.field_model or args.field_team)
        sk = bytes.fromhex(args.search_key_hex)
        parts = [Path(x) for x in args.parts]
        hits = vindex.search(parts, sk, field, value)
        print(json.dumps(hits, indent=2))
        return 0
    if args.cmd == "verify":
        parts = [Path(x) for x in args.parts]
        print(json.dumps(vindex.verify_chain(parts, args.vault_id),
                         indent=2))
        return 0
    if args.cmd == "keycheck":
        ch = json.loads(Path(args.challenge).read_text(encoding="utf-8"))
        priv = Path(args.key).read_text(encoding="ascii")
        dek = vkeys.unwrap_dek(ch["challenge"]["dek"], priv)
        token = vkeys.open_(dek, ch["challenge"]["nonce"],
                            ch["challenge"]["ct"]).decode()
        print(token)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(_cli())
