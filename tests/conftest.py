"""Shared test setup. GATEWAY_WATCHDOG=0 keeps the v3 lifecycle
watchdog out of every TestClient run — the loop would tick against
fakes/missing API keys; the watchdog is tested via lifecycle.tick()
directly.

The autouse fixture resets module-level request state (response cache,
chain cold breakers) around EVERY test — otherwise a cached response or
an open breaker from one test silently changes execution in the next
(cache hit instead of a backend call; primary skipped instead of raced).
"""
import os

import pytest

os.environ.setdefault("GATEWAY_WATCHDOG", "0")


@pytest.fixture(autouse=True)
def _clean_gateway_request_state():
    import gateway.app as gw_app
    gw_app.clear_response_cache()
    gw_app.reset_breakers()
    gw_app.reset_token_ledger()
    yield
    gw_app.clear_response_cache()
    gw_app.reset_breakers()
    gw_app.reset_token_ledger()
