"""Cloud provider registry.

Each vendor gets a Provider implementation (providers/base.py protocol)
and a console panel; the deploy wizard's recipe catalogue is shared
across them. Hardware / region / volume / billing specifics live
behind the provider, so the console and deploy endpoints never hardcode
a vendor. Onboard a new vendor by implementing the Provider protocol
and registering it in PROVIDERS below.
"""

from __future__ import annotations

PROVIDERS: dict[str, dict] = {
    "runpod": {
        "id": "runpod",
        "name": "RunPod",
        "features": ["gpus", "regions", "volumes", "deploy",
                     "start-stop", "billing"],
    },
}


def list_providers() -> list[dict]:
    """Registry view for the console's provider panels."""
    return [dict(v) for v in PROVIDERS.values()]


def get_provider(provider_id: str) -> dict | None:
    return PROVIDERS.get(provider_id)
