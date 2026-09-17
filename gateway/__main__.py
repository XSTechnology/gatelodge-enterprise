"""python -m gateway — run the LLM gateway.

Plain HTTP on the configured bind:port (default 127.0.0.1:8010) — the
OpenAI client ecosystem targets http://localhost:8010/v1. Config lives
in data/gateway_config.json (hot-reloadable; see gateway/config.py),
GGUFs in data/models (models_dir in the config).
"""
from __future__ import annotations

import logging
import sys


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

    from .config import get_gateway_config
    cfg = get_gateway_config()
    cfg.maybe_reload()

    import uvicorn
    from .app import app

    logger = logging.getLogger("gateway")
    logger.info("LLM gateway starting on http://%s:%d (models_dir: %s; models: %s)",
                cfg.bind, cfg.port, cfg.models_dir,
                ", ".join(cfg.local_models()) or "none")
    uvicorn.run(app, host=cfg.bind, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()
