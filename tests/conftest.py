"""Shared test setup. GATEWAY_WATCHDOG=0 keeps the v3 lifecycle
watchdog out of every TestClient run — the loop would tick against
fakes/missing API keys; the watchdog is tested via lifecycle.tick()
directly."""
import os

os.environ.setdefault("GATEWAY_WATCHDOG", "0")
