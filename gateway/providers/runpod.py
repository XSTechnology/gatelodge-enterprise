"""RunPod provider — implements the Provider protocol over the
gateway's runpod REST layer (gateway/runpod.py). Thin by design: the
REST client stays the single source of API truth."""

from __future__ import annotations

from typing import Any

from .. import runpod


class RunPodProvider:
    id = "runpod"
    name = "RunPod"

    def __init__(self, api_key_env: str = "RUNPOD_API_KEY"):
        self.api_key_env = api_key_env

    async def gpus(self) -> list[dict[str, Any]]:
        return await runpod.list_gpu_catalog(self.api_key_env)

    async def regions(self) -> list[dict[str, Any]]:
        return await runpod.list_datacenter_catalog(self.api_key_env)

    async def volumes(self) -> list[dict[str, Any]]:
        return await runpod.list_volumes(self.api_key_env)

    async def billing(self, days: int = 7) -> dict[str, Any]:
        return await runpod.list_pod_billing(self.api_key_env, days)
