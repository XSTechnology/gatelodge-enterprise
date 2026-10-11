"""python -m gateway — run the Gatelodge gateway.

Plain HTTP on the configured bind:port (default 127.0.0.1:8010) — the
OpenAI client ecosystem targets http://localhost:8010/v1. Config lives
in data/gateway_config.json (hot-reloadable; see gateway/config.py),
GGUFs in data/models (models_dir in the config).

Secrets: every ``api_key_env`` named by the config is loaded from the OS
keyring at boot (Workshop convention: service 'Workshop', username = the
env var name) when the env var isn't already set — real env vars always
win, and adding a NEW api_key_env to the config needs a restart (the
bridge runs once at startup).
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys


def _load_secret_env() -> None:
    """api_key_env vars + sso.client_secret_env → OS keyring fallback.
    Never logs values."""
    try:
        import keyring
    except ImportError:
        return
    from .config import get_gateway_config
    cfg = get_gateway_config()
    names = {b.get("api_key_env") for b in cfg.backends.values()}
    # sso.client_secret_env rides the same boot bridge (governance is
    # core; the IdP client secret follows the api_key_env convention)
    raw = cfg.store.load()
    sso = raw.get("sso") if isinstance(raw, dict) else None
    if isinstance(sso, dict):
        names.add(sso.get("client_secret_env"))
    for name in sorted(n for n in names if n):
        if os.environ.get(name):
            continue
        try:
            val = keyring.get_password("Workshop", name)
            if val:
                os.environ[name] = val
        except Exception:  # noqa: BLE001 — locked/missing keyring is fine
            pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # Piped Windows children default to cp1252 — our logs are UTF-8.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    _load_secret_env()

    # Licensing (v1.5 closed distribution): community binaries run a
    # 30-day trial without a license; an expired trial keeps /healthz
    # alive but the middleware 403s everything else. A valid license
    # file (community OR enterprise claims) removes the limit.
    # ensure_bootstrapped: the enterprise entry point installs ITS
    # state before delegating here — this must not clobber it.
    from . import licensing
    licensing.ensure_bootstrapped(edition="community")

    from .config import get_gateway_config
    cfg = get_gateway_config()
    cfg.maybe_reload()

    import uvicorn
    from .app import app

    # TLS (tier A): https when enabled with certfile+keyfile. Binding
    # beyond 127.0.0.1 without TLS is the operator's explicit choice —
    # the config comment says what it means.
    ssl_kwargs: dict = {}
    scheme = "http"
    if (cfg.tls.get("enabled") and cfg.tls.get("certfile")
            and cfg.tls.get("keyfile")):
        ssl_kwargs = {"ssl_certfile": cfg.tls["certfile"],
                      "ssl_keyfile": cfg.tls["keyfile"]}
        scheme = "https"

    logger = logging.getLogger("gateway")

    # Control-plane split (v2.1): admin_port configured → TWO
    # listeners around the SAME app — the serving port (data plane)
    # and the admin port (/admin/*, console, /healthz) on admin_bind.
    # Isolation is the network itself (Kong/Envoy posture); restart
    # to change (listeners bind at boot).
    if getattr(cfg, "admin_port", None) and cfg.admin_port != cfg.port:
        from .app import admin_scope_app, serving_scope_app

        async def _serve_both() -> None:
            serving = uvicorn.Server(uvicorn.Config(
                serving_scope_app(), host=cfg.bind, port=cfg.port,
                log_level="info", **ssl_kwargs))
            admin = uvicorn.Server(uvicorn.Config(
                admin_scope_app(), host=cfg.admin_bind,
                port=cfg.admin_port, log_level="info", **ssl_kwargs))
            logger.info(
                "Gatelodge starting on %s://%s:%d (serving) + "
                "%s://%s:%d (admin) (models_dir: %s; models: %s)",
                scheme, cfg.bind, cfg.port, scheme, cfg.admin_bind,
                cfg.admin_port, cfg.models_dir,
                ", ".join(cfg.local_models()) or "none")
            await asyncio.gather(serving.serve(), admin.serve())

        asyncio.run(_serve_both())
        return

    logger.info("Gatelodge starting on %s://%s:%d (models_dir: %s; models: %s)",
                scheme, cfg.bind, cfg.port, cfg.models_dir,
                ", ".join(cfg.local_models()) or "none")
    uvicorn.run(app, host=cfg.bind, port=cfg.port, log_level="info",
                **ssl_kwargs)


if __name__ == "__main__":
    main()
