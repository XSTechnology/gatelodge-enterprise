"""Storage abstraction — the enterprise-fork extension point.

The core gateway is single-process: all mutable state lives in
InMemoryStorage (dict-backed namespaces). The enterprise fork swaps
this for RedisStorage / PostgresStorage to share state across
replicas — the core modules never change.

Interface contract: every method takes a namespace (a string that
groups related keys, e.g. "response_cache", "spend_ledger") and a
key (arbitrary hashable). Values are JSON-serializable. The
InMemoryStorage implementation uses nested dicts; a Redis
implementation would use "namespace:key" as the Redis key with
JSON serialization.

Usage in the core:
    from .storage import get_storage, set_storage
    get_storage().set("response_cache", key, value)
    value = get_storage().get("response_cache", key)

Enterprise swaps the backend:
    from gateway.storage import set_storage
    set_storage(RedisStorage(url="redis://..."))
"""
from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class StorageBackend(Protocol):
    """Namespace-keyed mutable state. Implementations MUST be safe for
    concurrent access from async code (single event loop in the core;
    multi-replica backends handle their own locking)."""

    def get(self, ns: str, key: str) -> Any | None:
        """Get a value by namespace+key. None when absent."""
        ...

    def get_default(self, ns: str, key: str, default: Any) -> Any:
        """Get with a default (like dict.get)."""
        ...

    def set(self, ns: str, key: str, value: Any) -> None:
        """Set a value (upsert)."""
        ...

    def delete(self, ns: str, key: str) -> None:
        """Delete a key. No-op when absent."""
        ...

    def incr(self, ns: str, key: str, amount: float = 1.0) -> float:
        """Increment a numeric value (upsert from 0). Returns the new value."""
        ...

    def contains(self, ns: str, key: str) -> bool:
        """True when the key exists in the namespace."""
        ...

    def keys(self, ns: str) -> list[str]:
        """All keys in a namespace."""
        ...

    def items(self, ns: str) -> list[tuple[str, Any]]:
        """All (key, value) pairs in a namespace."""
        ...

    def len(self, ns: str) -> int:
        """Number of keys in a namespace."""
        ...

    def min_by(self, ns: str, field: str) -> tuple[str, Any] | None:
        """The (key, value) pair with the minimum value of `field` in
        the value dict. None when empty. (Used by cache eviction.)"""
        ...

    def clear(self, ns: str = "*") -> int:
        """Clear a namespace (or all when "*"). Returns keys cleared."""
        ...


class InMemoryStorage:
    """Dict-backed storage — the core default. Each namespace is a dict.
    NOT multi-process safe; single-process only (the core gateway)."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}

    def _ns(self, ns: str) -> dict[str, Any]:
        return self._data.setdefault(ns, {})

    def get(self, ns: str, key: str) -> Any | None:
        return self._ns(ns).get(key)

    def get_default(self, ns: str, key: str, default: Any) -> Any:
        return self._ns(ns).get(key, default)

    def set(self, ns: str, key: str, value: Any) -> None:
        self._ns(ns)[key] = value

    def delete(self, ns: str, key: str) -> None:
        self._ns(ns).pop(key, None)

    def incr(self, ns: str, key: str, amount: float = 1.0) -> float:
        d = self._ns(ns)
        current = d.get(key, 0)
        d[key] = current + amount
        return d[key]

    def contains(self, ns: str, key: str) -> bool:
        return key in self._ns(ns)

    def keys(self, ns: str) -> list[str]:
        return list(self._ns(ns).keys())

    def items(self, ns: str) -> list[tuple[str, Any]]:
        return list(self._ns(ns).items())

    def len(self, ns: str) -> int:
        return len(self._ns(ns))

    def min_by(self, ns: str, field: str) -> tuple[str, Any] | None:
        d = self._ns(ns)
        if not d:
            return None
        return min(d.items(), key=lambda kv: kv[1].get(field, 0))

    def clear(self, ns: str = "*") -> int:
        if ns == "*":
            count = sum(len(d) for d in self._data.values())
            self._data.clear()
            return count
        d = self._data.get(ns, {})
        count = len(d)
        d.clear()
        return count


# ── module-level backend management ────────────────────────────────────────

_BACKEND: StorageBackend = InMemoryStorage()


def get_storage() -> StorageBackend:
    """The active storage backend. Core: InMemoryStorage (default)."""
    return _BACKEND


def set_storage(backend: StorageBackend) -> None:
    """Swap the storage backend (enterprise: RedisStorage, etc.).
    Call BEFORE the app starts; existing state does NOT migrate."""
    global _BACKEND
    _BACKEND = backend
    logger.info("storage backend swapped to %s", type(backend).__name__)


def reset_storage() -> None:
    """Reset to a fresh InMemoryStorage (tests)."""
    global _BACKEND
    _BACKEND = InMemoryStorage()
