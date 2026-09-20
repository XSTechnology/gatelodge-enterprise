"""Provider protocol — the vendor-neutral surface the console and
deploy wizard code against. RunPod is the first implementation
(providers/runpod.py); onboarding another vendor means implementing
this and registering it in providers/__init__.py.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Provider(Protocol):
    """A GPU-cloud vendor. All methods are async and return plain
    dicts/lists ready for the console; errors raise the vendor's own
    exception type (mapped to 502 by the endpoints)."""

    id: str
    name: str

    async def gpus(self) -> list[dict[str, Any]]:
        """Live GPU types: id, memory (GB), price per cloud class,
        maxCount per class."""
        ...

    async def regions(self) -> list[dict[str, Any]]:
        """Deployable regions/data centers incl. volume support."""
        ...

    async def volumes(self) -> list[dict[str, Any]]:
        """The account's storage volumes."""
        ...

    async def billing(self, days: int = 7) -> dict[str, Any]:
        """Recent spend, vendor-shaped (console renders defensively)."""
        ...
