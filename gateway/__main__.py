"""python -m gateway — run the LLM gateway.

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

import logging
import os
import sys


def _load_secret_env() -> None:
    """api_key_env vars → OS keyring fallback. Never logs values."""
    try:
        import keyring
    except ImportError:
        return
    from .config import get_gateway_config
    names = {b.get("api_key_env")
             for b in get_gateway_config().backends.values()}
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
    logger.info("LLM gateway starting on %s://%s:%d (models_dir: %s; models: %s)",
                scheme, cfg.bind, cfg.port, cfg.models_dir,
                ", ".join(cfg.local_models()) or "none")
    uvicorn.run(app, host=cfg.bind, port=cfg.port, log_level="info",
                **ssl_kwargs)


if __name__ == "__main__":
    main()
