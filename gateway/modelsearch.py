"""Unified model search across repositories (v4 model finder, D2).

Sources:
- hf          HuggingFace via the huggingface_hub SDK (already a
              dependency). list_models(filter="gguf", search=) — the
              installed hub 1.32.0 REJECTS library= (verified live);
              model_info(files_metadata=True) gives per-file sizes;
              /resolve/main/ links are anonymous. Sync SDK → wrapped
              in asyncio.to_thread.
- modelscope  REST via httpx (no SDK — heavy deps). Shapes verified
              live 2026-09-20: PUT /api/v1/dolphin/models
              {"Page","PageSize","Name"} → Data.Model[]; GET
              /api/v1/models/{id}/repo/files?Revision=master →
              Data.Files[{Name,Path,Size,Type:"blob"}]; direct file
              GET /api/v1/models/{id}/repo?Revision=master&FilePath=.
- ollama      registry.ollama.ai manifests are anonymous and give
              layer sizes + blob URLs (307 → CDN; curl -L works), but
              tags/list is DISABLED (404, verified) — so discovery is
              a curated library list plus free-form "model:tag" paste.

Results are cached SEARCH_TTL_S. A failing source is logged and
skipped — one repo being down must not kill the search.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any
from urllib.parse import quote

import httpx

from . import capabilities

logger = logging.getLogger("gateway.modelsearch")

SEARCH_TTL_S = 300.0
SOURCES = ("hf", "modelscope", "ollama")

_cache: dict[tuple, tuple[float, Any]] = {}


def _cache_get(key: tuple) -> Any:
    hit = _cache.get(key)
    if hit is not None and time.monotonic() - hit[0] < SEARCH_TTL_S:
        return hit[1]
    return None


def _cache_set(key: tuple, val: Any) -> None:
    _cache[key] = (time.monotonic(), val)


def reset_cache() -> None:
    """Test helper."""
    _cache.clear()


# ── HuggingFace ─────────────────────────────────────────────────────────────

def _hf_search_sync(q: str, limit: int) -> list[dict[str, Any]]:
    from huggingface_hub import HfApi
    models = list(HfApi().list_models(filter="gguf", search=q,
                                      limit=limit))
    out: list[dict[str, Any]] = []
    for m in models:
        inf = capabilities.infer(m.id, tags=(m.tags or []),
                                 pipeline_tag=m.pipeline_tag)
        out.append({
            "source": "hf", "repo_id": m.id,
            "name": m.id.split("/")[-1],
            "capabilities": inf["capabilities"],
            "evidence": inf["evidence"],
            "downloads": int(getattr(m, "downloads", 0) or 0),
            "url": f"https://huggingface.co/{m.id}",
        })
    out.sort(key=lambda r: -r["downloads"])
    return out


def _hf_files_sync(repo_id: str) -> tuple[list, list]:
    from huggingface_hub import HfApi
    info = HfApi().model_info(repo_id, files_metadata=True)
    files: list[dict[str, Any]] = []
    mmproj: list[dict[str, Any]] = []
    for s in info.siblings or []:
        n = s.rfilename
        if not n.lower().endswith(".gguf"):
            continue
        entry = {"name": n, "size_bytes": int(s.size or 0),
                 "url": f"https://huggingface.co/{repo_id}/resolve/main/{n}"}
        if capabilities.is_mmproj(n):
            mmproj.append(entry)
        elif capabilities.is_pickable(n):
            entry["quant"] = capabilities.parse_quant(n)
            files.append(entry)
    files.sort(key=lambda f: f["size_bytes"])
    return files, mmproj


# ── ModelScope (shapes verified live 2026-09-20) ────────────────────────────

_MS = "https://www.modelscope.cn/api/v1"


async def _modelscope_search(q: str, limit: int) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.put(f"{_MS}/dolphin/models",
                        json={"Page": 1, "PageSize": limit, "Name": q})
        r.raise_for_status()
        data = (r.json().get("Data") or {}).get("Model") or []
    out: list[dict[str, Any]] = []
    for m in data:
        if not isinstance(m, dict):
            continue
        rid = f"{m.get('Path', '')}/{m.get('Name', '')}".strip("/")
        if "/" not in rid:
            continue
        inf = capabilities.infer(rid)
        out.append({
            "source": "modelscope", "repo_id": rid,
            "name": str(m.get("Name") or rid),
            "capabilities": inf["capabilities"],
            "evidence": inf["evidence"],
            "downloads": int(m.get("Downloads") or 0),
            "url": f"https://www.modelscope.cn/models/{rid}",
        })
    return out


async def _modelscope_files(repo_id: str) -> tuple[list, list]:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(f"{_MS}/models/{repo_id}/repo/files",
                        params={"Revision": "master"})
        r.raise_for_status()
        files_raw = (r.json().get("Data") or {}).get("Files") or []
    files: list[dict[str, Any]] = []
    mmproj: list[dict[str, Any]] = []
    for f in files_raw:
        if not isinstance(f, dict) or f.get("Type") != "blob":
            continue
        n = str(f.get("Path") or f.get("Name") or "")
        if not n.lower().endswith(".gguf"):
            continue
        entry = {
            "name": n, "size_bytes": int(f.get("Size") or 0),
            "url": f"{_MS}/models/{repo_id}/repo?Revision=master"
                   f"&FilePath={quote(n)}",
        }
        if capabilities.is_mmproj(n):
            mmproj.append(entry)
        elif capabilities.is_pickable(n):
            entry["quant"] = capabilities.parse_quant(n)
            files.append(entry)
    files.sort(key=lambda f: f["size_bytes"])
    return files, mmproj


# ── Ollama registry ─────────────────────────────────────────────────────────

_OL = "https://registry.ollama.ai/v2/library"

# tags/list is DISABLED on the registry (verified 404) — discovery is
# this curated library (+ free-form "model:tag" paste in the search box)
OLLAMA_LIBRARY: dict[str, list[str]] = {
    "llama3.1": ["8b", "70b", "405b"],
    "llama3.2": ["1b", "3b"],
    "qwen3": ["0.6b", "1.7b", "4b", "8b", "14b", "32b"],
    "qwen2.5": ["0.5b", "1.5b", "3b", "7b", "14b", "32b", "72b"],
    "qwen2.5-coder": ["0.5b", "1.5b", "3b", "7b", "14b", "32b"],
    "deepseek-r1": ["1.5b", "7b", "8b", "14b", "32b", "70b"],
    "gemma3": ["1b", "4b", "12b", "27b"],
    "phi4": ["14b"],
    "mistral": ["7b"],
    "mistral-nemo": ["12b"],
}


async def _ollama_manifest(c: httpx.AsyncClient, model: str,
                           tag: str) -> dict | None:
    r = await c.get(f"{_OL}/{model}/manifests/{tag}")
    if r.status_code != 200:
        return None
    return r.json()


def _ollama_layer(manifest: dict, needle: str) -> dict | None:
    for layer in manifest.get("layers", []):
        if needle in str(layer.get("mediaType", "")):
            return layer
    return None


async def _ollama_file(c: httpx.AsyncClient, model: str,
                       tag: str) -> dict | None:
    """Resolve one model:tag to a downloadable file entry."""
    mf = await _ollama_manifest(c, model, tag)
    if mf is None:
        return None
    layer = _ollama_layer(mf, "model")
    if not layer or not layer.get("digest"):
        return None
    quant = capabilities.parse_quant(tag) or (
        tag.upper() if tag else None)
    return {
        "name": f"{model}:{tag}",
        "size_bytes": int(layer.get("size") or 0),
        "url": f"{_OL}/{model}/blobs/{layer['digest']}",
        "quant": quant,
    }


async def _ollama_search(q: str, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as c:
        if ":" in q:                     # free-form paste: model:tag
            model, _, tag = q.partition(":")
            model, tag = model.strip().removeprefix("library/"), tag.strip()
            inf = capabilities.infer(model)
            out.append({
                "source": "ollama", "repo_id": model,
                "name": f"{model}:{tag}",
                "capabilities": inf["capabilities"],
                "evidence": inf["evidence"], "downloads": 0,
                "url": f"https://ollama.com/library/{model}",
            })
            return out
        for m in [m for m in OLLAMA_LIBRARY if q.lower() in m][:limit]:
            inf = capabilities.infer(m)
            out.append({
                "source": "ollama", "repo_id": m, "name": m,
                "capabilities": inf["capabilities"],
                "evidence": inf["evidence"], "downloads": 0,
                "url": f"https://ollama.com/library/{m}",
            })
    return out


async def _ollama_files(repo_id: str) -> tuple[list, list]:
    """File list = one entry per curated tag (manifests are cheap)."""
    files: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as c:
        for tag in OLLAMA_LIBRARY.get(repo_id, [])[:6]:
            entry = await _ollama_file(c, repo_id, tag)
            if entry:
                files.append(entry)
    files.sort(key=lambda f: f["size_bytes"])
    return files, []


# ── public API ──────────────────────────────────────────────────────────────

async def search(q: str, source: str = "all",
                 limit: int = 10) -> list[dict[str, Any]]:
    """Search across sources. One source failing is logged and skipped."""
    if not q:
        return []
    key = ("search", source, q.lower(), limit)
    hit = _cache_get(key)
    if hit is not None:
        return hit
    jobs = []
    if source in ("all", "hf"):
        jobs.append(asyncio.to_thread(_hf_search_sync, q, limit))
    if source in ("all", "modelscope"):
        jobs.append(_modelscope_search(q, limit))
    if source in ("all", "ollama"):
        jobs.append(_ollama_search(q, limit))
    lists = await asyncio.gather(*jobs, return_exceptions=True)
    results: list[dict[str, Any]] = []
    for lst in lists:
        if isinstance(lst, BaseException):
            logger.warning("model search source failed: %s", lst)
        else:
            results.extend(lst)
    _cache_set(key, results)
    return results


async def list_files(source: str, repo_id: str) -> dict[str, Any]:
    """Pickable GGUF quants (+mmproj files) for one repo/model."""
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}")
    key = ("files", source, repo_id)
    hit = _cache_get(key)
    if hit is not None:
        return hit
    if source == "hf":
        files, mmproj = await asyncio.to_thread(_hf_files_sync, repo_id)
    elif source == "modelscope":
        files, mmproj = await _modelscope_files(repo_id)
    else:
        files, mmproj = await _ollama_files(repo_id)
    out = {"source": source, "repo_id": repo_id, "files": files,
           "mmproj": mmproj}
    _cache_set(key, out)
    return out
