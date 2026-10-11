"""RedisStorage — the multi-replica storage backend (tier D).

Implements the core's StorageBackend protocol (gateway/storage.py)
against Redis, so every riding namespace — response_cache,
spend_ledger, spend_alerted, rate_windows, batches, route_latency,
route_selections, route_rr, cold_breakers, guardrail_violations,
verify_state, sso_introspect — is shared by all replicas pointing at
the same Redis.

Wiring (enterprise_main.py, BEFORE serving — existing state does NOT
migrate on a swap):

    from gateway.storage import set_storage
    from gateway_enterprise.redis_storage import RedisStorage
    set_storage(RedisStorage(url="redis://127.0.0.1:6379/0"))

Semantics mirror gateway.storage.InMemoryStorage exactly (the protocol
is behavioral, not just structural — see the parity tests):

- namespace-keyed: Redis key = ``"ns:key"``. Keys may themselves
  contain colons (spend scopes like ``key:program-x``) — the namespace
  is stripped with removeprefix, never split on ":". The NAMESPACES
  themselves are code-defined colon-free constants, so "ns:key" is
  always unambiguous.
- values are JSON-serialized (the protocol's contract: values must be
  JSON-serializable).
- keys/items/len enumerate via SCAN (never KEYS — it blocks production
  Redis).
- incr is atomic on the server (INCRBYFLOAT).
- min_by/clear are client-side over scanned items — O(namespace).

Deployment isolation: give each gateway deployment its own Redis DB
index (redis://host:6379/0 vs /1) — clear("*") clears the ENTIRE DB
index it is pointed at, exactly like InMemoryStorage clears everything
it holds.

shared = True: the core skips single-process assumptions (it never
re-derives spend from this replica's local log — see
gateway/app.py:_scope_spend).

Tests inject fakeredis: ``RedisStorage(client=fakeredis.FakeRedis())``
— the REAL class, a fake server, no mocks.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class RedisStorage:
    shared = True  # multi-replica: single-process assumptions are OFF

    def __init__(self, url: str | None = None, *, client: Any = None):
        if client is not None:
            self._r = client
        elif url:
            import redis  # lazy: the module imports without redis installed
            # decode_responses=True → str in/out; the JSON layer does the rest
            self._r = redis.Redis.from_url(url, decode_responses=True)
        else:
            raise ValueError("RedisStorage needs url= or client=")

    # ── key helpers ────────────────────────────────────────────────────

    @staticmethod
    def _k(ns: str, key: str) -> str:
        return f"{ns}:{key}"

    @staticmethod
    def _s(v: Any) -> Any:
        """str in/out regardless of the client's decode_responses
        setting (injected test clients may not enable it; json.loads
        handles bytes for values, but key handling needs str)."""
        return v.decode("utf-8") if isinstance(v, bytes) else v

    def _scan(self, pattern: str) -> list[str]:
        # SCAN iterates in batches; KEYS would block the server
        return [self._s(k) for k in self._r.scan_iter(match=pattern)]

    # ── StorageBackend protocol ────────────────────────────────────────

    def get(self, ns: str, key: str) -> Any | None:
        v = self._r.get(self._k(ns, key))
        return json.loads(v) if v is not None else None

    def get_default(self, ns: str, key: str, default: Any) -> Any:
        v = self.get(ns, key)
        return default if v is None else v

    def set(self, ns: str, key: str, value: Any) -> None:
        self._r.set(self._k(ns, key), json.dumps(value))

    def delete(self, ns: str, key: str) -> None:
        self._r.delete(self._k(ns, key))

    def incr(self, ns: str, key: str, amount: float = 1.0) -> float:
        return float(self._s(self._r.incrbyfloat(self._k(ns, key), amount)))

    def contains(self, ns: str, key: str) -> bool:
        return bool(self._r.exists(self._k(ns, key)))

    def keys(self, ns: str) -> list[str]:
        prefix = f"{ns}:"
        return [k.removeprefix(prefix) for k in self._scan(f"{prefix}*")]

    def items(self, ns: str) -> list[tuple[str, Any]]:
        prefix = f"{ns}:"
        keys = self._scan(f"{prefix}*")
        if not keys:
            return []
        pipe = self._r.pipeline()
        for k in keys:
            pipe.get(k)
        return [(k.removeprefix(prefix), json.loads(v))
                for k, v in zip(keys, pipe.execute())]

    def len(self, ns: str) -> int:
        return len(self._scan(f"{ns}:*"))

    def min_by(self, ns: str, field: str) -> tuple[str, Any] | None:
        d = dict(self.items(ns))
        if not d:
            return None
        return min(d.items(), key=lambda kv: kv[1].get(field, 0))

    def clear(self, ns: str = "*") -> int:
        pattern = "*" if ns == "*" else f"{ns}:*"
        keys = self._scan(pattern)
        if keys:
            self._r.delete(*keys)
        return len(keys)
