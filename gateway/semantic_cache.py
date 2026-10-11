"""Gatelodge semantic response cache — paraphrase hits, not just repeats.

v2.0 "The Gatehouse". The exact-match cache (response_cache) answers
REPEATED prompts; this layer answers SIMILAR ones: embed the incoming
prompt with a LOCAL embedding model (nomic-embed class — config-defined,
same machinery as /v1/embeddings) and serve the nearest cached response
when cosine similarity ≥ threshold. Local inference only — the lookup
costs milliseconds and zero API spend.

Design invariants (matching the exact cache's doctrine):
    eligibility      the SAME prompt class as the exact cache (no tools,
                     no stream, single user message, short, no
                     time-sensitive keywords) — enforced by the caller
                     passing the exact-cache key path; local_only aliases
                     NEVER cache (privacy scope).
    scoping          entries are scoped to (model, key, generation
                     params) — a response is never served across keys
                     (multi-tenant governance) and params (max_tokens,
                     temperature) must match exactly; only the TEXT is
                     matched semantically.
    storage          the storage seam ("semantic_cache" namespace) — a
                     shared backend means replicas share entries. Epoch
                     "ts" stamps; TTL + cap with oldest-first eviction,
                     exactly like the exact cache.
    failure posture  telemetry-grade: ANY embedding error (model not
                     configured, GGUF missing, engine hiccup) disables
                     the lookup for that request (warn-throttled,
                     counted) — serving NEVER depends on this layer.

Config (hot-reloadable; requires response_cache.enabled as the
response-caching master switch):

    "semantic_cache": {"enabled": true,
                       "embedding_model": "nomic-embed",
                       "threshold": 0.92, "ttl_s": 3600, "max": 100}

Threshold honesty: 0.92 is a conservative default for Q&A-style
traffic; too low serves WRONG answers confidently — operators tune per
workload (the /admin cache stats expose hit/lookup ratios).

Test seam: set_embedder() injects the embed function — tests drive
similarity scenarios without a model; reset_semantic_cache() restores.
"""
from __future__ import annotations

import logging
import math
import time
import uuid
from typing import Any, Callable

from .config import get_gateway_config
from .storage import NamespaceDict

logger = logging.getLogger(__name__)

DEFAULTS: dict[str, Any] = {"enabled": False, "embedding_model": "",
                            "threshold": 0.92, "ttl_s": 3600.0, "max": 100}

_SEM = NamespaceDict("semantic_cache")

_counters: dict[str, int] = {"lookups": 0, "hits": 0, "stores": 0,
                             "evictions": 0, "errors": 0}
_embed_ms_total = 0.0
_last_warn = 0.0

# test seam: async fn (text) -> list[float] | None
_embedder: Callable[[str], Any] | None = None


def _warn_throttled(msg: str, *args: Any) -> None:
    global _last_warn
    now = time.monotonic()
    if now - _last_warn > 60.0:
        _last_warn = now
        logger.warning(msg, *args)
    else:
        logger.debug(msg, *args)


def _cfg() -> dict[str, Any]:
    """Normalized semantic_cache section (hot-reloadable, per call)."""
    raw = getattr(get_gateway_config(), "semantic_cache", None)
    raw = raw if isinstance(raw, dict) else {}
    out = dict(DEFAULTS)
    out.update(raw)
    return out


# ── vectors ────────────────────────────────────────────────────────────────

def cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity, pure python (768-dim × N entries is ~µs-scale
    next to the embed call itself)."""
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return sum(x * y for x, y in zip(a, b)) / (na * nb)


async def _embed(text: str) -> list[float] | None:
    """Embed via the injected test seam or the LOCAL engine. Never
    raises — errors disable the layer for this request (counted)."""
    global _embed_ms_total
    t0 = time.perf_counter()
    try:
        if _embedder is not None:
            vec = _embedder(text)
            if hasattr(vec, "__await__"):
                vec = await vec
        else:
            from .models import get_engine
            cfg = _cfg()
            resp = await get_engine().embed(cfg["embedding_model"], text)
            vec = ((resp.get("data") or [{}])[0]
                   .get("embedding"))
        if not isinstance(vec, (list, tuple)) or not vec:
            return None
        _embed_ms_total += (time.perf_counter() - t0) * 1000.0
        return [float(x) for x in vec]
    except Exception:                                  # noqa: BLE001
        _counters["errors"] += 1
        _warn_throttled("semantic cache embed failed — lookup skipped "
                        "(is embedding_model configured?)")
        return None


# ── lookup / store ─────────────────────────────────────────────────────────

async def lookup(model: str, prompt: str, key_id: str,
                 params: tuple) -> tuple[dict[str, Any] | None,
                                         list[float] | None]:
    """Nearest-entry admission for (model, key, params) scope. Returns
    (cached_resp | None, prompt_vector | None) — the vector rides back
    so a miss stores WITHOUT re-embedding."""
    cfg = _cfg()
    if not (cfg["enabled"] and cfg["embedding_model"] and prompt.strip()):
        return None, None
    vec = await _embed(prompt)
    if vec is None:
        return None, None
    _counters["lookups"] += 1
    threshold = float(cfg["threshold"])
    ttl = float(cfg["ttl_s"])
    now = time.time()
    best: tuple[float, dict[str, Any], str] | None = None
    expired: list[str] = []
    for k, e in list(_SEM.items()):
        if now - float(e.get("ts") or 0) >= ttl:
            expired.append(k)
            continue
        if (e.get("model") != model or e.get("key") != key_id
                or tuple(e.get("params") or ()) != tuple(params)):
            continue
        sim = cosine(vec, e.get("vec") or [])
        if sim >= threshold and (best is None or sim > best[0]):
            best = (sim, e, k)
    for k in expired:                       # opportunistic TTL purge
        _SEM.pop(k, None)
    if best is None:
        return None, vec
    sim, entry, k = best
    _SEM[k] = {**entry, "hits": int(entry.get("hits", 0)) + 1}
    _counters["hits"] += 1
    resp = dict(entry.get("resp") or {})
    # OVERWRITE (the stored served_by names the original execution — a
    # semantic hit must be distinguishable from a fresh backend call)
    resp["served_by"] = {"cache": "semantic", "similarity": round(sim, 4)}
    return resp, vec


async def store(model: str, key_id: str, vec: list[float] | None,
                resp: dict[str, Any], params: tuple) -> None:
    """Cache a fresh response under its prompt vector (no-op when the
    layer is off, the vector is absent, or the cap is 0)."""
    cfg = _cfg()
    if not (cfg["enabled"] and vec):
        return
    cap = int(cfg["max"])
    if cap <= 0:
        return
    if len(_SEM) >= cap:                    # oldest-first, like exact cache
        oldest = min(_SEM.items(), key=lambda kv: kv[1].get("ts") or 0)
        _SEM.pop(oldest[0], None)
        _counters["evictions"] += 1
    _SEM[f"{model}|{key_id}|{uuid.uuid4().hex[:12]}"] = {
        "model": model, "key": key_id, "params": list(params),
        "vec": vec, "resp": dict(resp), "ts": time.time(), "hits": 0}
    _counters["stores"] += 1


# ── admin surface + seams ──────────────────────────────────────────────────

def stats() -> dict[str, Any]:
    return {**dict(_counters), "entries": len(_SEM),
            "threshold": _cfg()["threshold"],
            "embedding_model": _cfg()["embedding_model"],
            "embed_ms_total": round(_embed_ms_total, 1)}


def clear() -> int:
    n = len(_SEM)
    _SEM.clear()
    return n


def set_embedder(fn: Callable[[str], Any] | None) -> None:
    """Inject the embed function (tests). None restores the engine."""
    global _embedder
    _embedder = fn


def reset_semantic_cache() -> None:
    """Clear state + counters + seam (test teardown)."""
    global _embedder, _embed_ms_total
    _SEM.clear()
    for k in _counters:
        _counters[k] = 0
    _embedder = None
    _embed_ms_total = 0.0
