"""Evidence Center — the enterprise evidence surface (/admin/evidence).

Mounted via add_extra_router when the license carries the 'evidence'
claim (the same claim that chains the request ledger — M2 in the
standard repo). The surface carries its own admin-gate mirroring the
core's /admin semantics, exactly like the vault's.

  GET  /admin/evidence/status    — what a package would contain now
  POST /admin/evidence/package   — {from: YYYY-MM, to: YYYY-MM} →
                                   assembles data/evidence/*.zip,
                                   returns the manifest
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("gateway.enterprise.evidence")


def wire(enabled: bool) -> dict:
    """Install the evidence surface when licensed. The ledger chaining
    itself is CORE (gateway.app, gated live by the same claim); this
    mounts only the packaging surface."""
    if not enabled:
        logger.info("evidence NOT licensed — no packaging surface")
        return {"installed": False, "reason": "not licensed"}

    from gateway.app import add_extra_router
    add_extra_router(_build_router(), prefix="/admin/evidence")
    logger.info("evidence packaging surface ACTIVE (/admin/evidence)")
    return {"installed": True}


def _build_router() -> APIRouter:
    r = APIRouter()

    def _admin_guard(request: Request) -> JSONResponse | None:
        """Mirror admin_gate semantics (same as the vault surface)."""
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
            "message": "evidence endpoints require a gateway admin key",
            "type": "invalid_request_error", "param": None,
            "code": "admin_key_required"}})

    @r.get("/status")
    async def status(request: Request):
        if (g := _admin_guard(request)):
            return g
        from gateway import licensing
        from gateway.app import verify_audit_chain, verify_request_chain
        from gateway.config import get_gateway_config
        try:
            from .inventory import build_inventory
            inv = build_inventory(get_gateway_config())
        except Exception:                    # noqa: BLE001
            inv = {}
        vault = None
        try:
            from gateway_enterprise.vault import get_writer
            w = get_writer()
            vault = {"running": w is not None}
        except Exception:                    # noqa: BLE001
            vault = {"running": False}
        return {
            "licensed": licensing.allows("evidence"),
            "request_ledger": verify_request_chain(),
            "config_audit": verify_audit_chain(),
            "inventory_size": {
                "backends": len(inv.get("backends") or []),
                "api_keys": len(inv.get("api_keys") or []),
                "aliases": len((inv.get("models") or {})
                               .get("aliases") or []),
            },
            "vault": vault,
        }

    @r.post("/package")
    async def package(request: Request):
        if (g := _admin_guard(request)):
            return g
        from .package import build_package
        try:
            body = await request.json()
        except Exception:                    # noqa: BLE001
            body = {}
        try:
            manifest = build_package(
                month_from=body.get("from"),
                month_to=body.get("to"))
        except RuntimeError as e:
            return JSONResponse(status_code=400, content={
                "error": str(e)})
        except Exception as e:               # noqa: BLE001
            logger.warning("evidence package assembly failed",
                           exc_info=True)
            return JSONResponse(status_code=500, content={
                "error": f"package assembly failed: {e}"})
        return manifest

    return r
