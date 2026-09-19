"""Embedded model engine — llama-cpp-python in-process (standalone).

Lifted from the assistant project's agent/embedded_llm.py (the model +
GPU machinery), restructured for the gateway: instances are keyed by the
gateway's own config (models under the 'local' backend), not by agent
settings. Carried over verbatim:

- lazy per-model instances, one asyncio.Lock each (requests serialize
  per model; instances run concurrently)
- q8_0 KV cache with an F16 fallback retry for models whose attention
  head width doesn't divide 32 (StableLM-arch Home-3B) — Qwen keeps the
  q8 KV savings, incompatible archs fall back automatically
- GPU placement: '' = llama.cpp default, 'N' = whole model pinned to
  card N, 'auto' = one model split across all cards weighted by free
  VRAM (nvidia-smi, ~30s cache); placement changes reload the instance
- JSON-convention tool calling for ANY model (no GGUF template support
  needed): tools serialized into a LEADING system instruction, tool
  calls parsed out of the completion text
- GGUF chat-template robustness: non-leading system messages folded to
  the front (Qwen3/Llama3 templates reject trailing system messages),
  a tools=None guard retry (Home-LLM's template iterates tools
  unguarded and crashes on None), think-tag stripping (Qwen3 reasoning
  never leaks into content), echo/instruction-parroting retry
"""
from __future__ import annotations

import asyncio
import gc
import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any, AsyncIterator, Iterator

from pydantic import BaseModel

from .config import GatewayConfig, get_gateway_config

logger = logging.getLogger(__name__)

# llama.cpp llama_split_mode: 0 = NONE (whole model on main_gpu),
# 1 = LAYER (layers distributed across cards per tensor_split)
_SPLIT_MODE_NONE = 0
_SPLIT_MODE_LAYER = 1


class EmbeddedLLMError(Exception):
    """Raised when an embedded model fails to load or generate."""


# ─── Think-stripping ───────────────────────────────────────────────────────

# Qwen3-style thinking: the <think> opener is part of the chat template's
# assistant header (never in the completion) — only the closer shows up.
# Keep only text after the LAST closer; no closer at all means a
# non-thinking model (phi4-mini etc.) — pass the answer through as-is.
def _strip_think(text: str) -> str:
    if "</think>" in text:
        return text.rsplit("</think>", 1)[1].strip()
    return text.strip()


# ─── GPU placement ─────────────────────────────────────────────────────────

def _cfg_gpu(raw: Any) -> str:
    """Normalized placement setting ('' / 'N' / 'auto')."""
    return (str(raw or "")).strip().lower()


def _placement_kwargs(placement: str) -> dict[str, Any]:
    """Llama kwargs for a placement setting.

    ''     → {} (llama.cpp default placement — first card)
    'N'    → whole model pinned to card N (SPLIT_MODE_NONE + main_gpu)
    'auto' → one model split across ALL cards, weighted by each card's
             FREE VRAM at load time (SPLIT_MODE_LAYER + tensor_split).
             Supports any number of cards; with fewer than 2 there is
             nothing to split → default placement.
    """
    val = _cfg_gpu(placement)
    if val in ("auto", "auto-split", "split"):
        try:
            gpus = detect_gpus()
        except Exception:
            gpus = []
        if len(gpus) < 2:
            return {}
        free = [max(g["total_mb"] - g["used_mb"], 1) for g in gpus]
        total = sum(free)
        split = [round(f / total, 4) for f in free]
        return {"tensor_split": split, "split_mode": _SPLIT_MODE_LAYER}
    try:
        n = int(val)
        return {"main_gpu": n, "split_mode": _SPLIT_MODE_NONE}
    except ValueError:
        return {}


def _placement_desc(placement: str) -> str:
    val = _cfg_gpu(placement)
    if val in ("auto", "auto-split", "split"):
        return " (split across all GPUs by free VRAM)"
    try:
        return f" on GPU {int(val)}"
    except ValueError:
        return ""


def _parse_nvidia_smi(out: str) -> list[dict[str, Any]]:
    """Parse nvidia-smi CSV lines: index,name,memory.total,memory.used.

    One line per GPU — multi-card machines return several."""
    gpus: list[dict[str, Any]] = []
    for line in out.strip().splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 4:
            continue
        try:
            gpus.append({
                "index": int(parts[0]),
                "name": parts[1],
                "total_mb": int(parts[2]),
                "used_mb": int(parts[3]),
            })
        except ValueError:
            continue
    return gpus


_GPU_CACHE: tuple[float, list[dict[str, Any]]] | None = None


def detect_gpus(ttl_s: float = 30.0) -> list[dict[str, Any]]:
    """NVIDIA GPUs present (nvidia-smi, cached ~30s). [] when unavailable."""
    global _GPU_CACHE
    now = time.monotonic()
    if _GPU_CACHE is not None and now - _GPU_CACHE[0] < ttl_s:
        return _GPU_CACHE[1]
    gpus: list[dict[str, Any]] = []
    try:
        import subprocess

        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total,memory.used",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        ).stdout
        gpus = _parse_nvidia_smi(out)
    except Exception:
        pass
    _GPU_CACHE = (now, gpus)
    return gpus


# ─── Response shape (mirrors OpenAI/llama_cpp pydantic types) ──────────────

class _Fn(BaseModel):
    name: str = ""
    arguments: str = "{}"


class _ToolCall(BaseModel):
    id: str = "call_embedded_0"
    type: str = "function"
    function: _Fn = _Fn()


class _Message(BaseModel):
    role: str = "assistant"
    content: str | None = None
    tool_calls: list[_ToolCall] | None = None


class _Choice(BaseModel):
    index: int = 0
    message: _Message
    finish_reason: str = "stop"


class _ChoiceDelta(BaseModel):
    index: int = 0
    delta: dict[str, Any] = {}
    finish_reason: str | None = None


class _Completion(BaseModel):
    choices: list[_Choice]
    usage: dict[str, int] = {}


class _Chunk(BaseModel):
    choices: list[_ChoiceDelta] = []


# ─── Instances ─────────────────────────────────────────────────────────────

class _Instance:
    """One loaded GGUF + its lock.

    placement: '' = llama.cpp default, 'N' = whole model on card N,
    'auto' = one model split across all cards (free-VRAM weighted).
    embedding: True builds a pooling context (Llama(embedding=True)) —
    such an instance serves /v1/embeddings ONLY, and a chat instance
    can never embed (no pooling in its context)."""

    def __init__(self, name: str, path: str, n_ctx: int, placement: str = "",
                 embedding: bool = False):
        from llama_cpp import Llama  # heavy import — deferred until first use

        if not Path(path).exists():
            raise EmbeddedLLMError(f"GGUF not found: {path}")
        t0 = time.perf_counter()
        kwargs = _placement_kwargs(placement)
        common: dict[str, Any] = dict(
            model_path=path,
            n_gpu_layers=-1,
            n_ctx=n_ctx,
            flash_attn=True,
            verbose=False,
        )
        if embedding:
            common["embedding"] = True
        try:
            # q8_0 KV cache halves KV VRAM — but its 32-wide blocks must
            # divide the model's head width. Qwen (128-dim) is fine;
            # StableLM-arch models (Home-3B, 80-dim) are NOT and context
            # creation fails. Retry once with the default F16 KV.
            self.llama = Llama(**common, type_k=8, type_v=8, **kwargs)
        except ValueError as e:
            if "Failed to create llama_context" not in str(e):
                raise
            logger.info(
                "%s: q8_0 KV cache incompatible with this model's head "
                "width — retrying with default F16 KV", name)
            self.llama = Llama(**common, **kwargs)
        self.load_s = round(time.perf_counter() - t0, 2)
        self.path = path
        self.name = name
        self.placement = _cfg_gpu(placement)
        self.embedding = embedding
        self.lock = asyncio.Lock()
        logger.info("Embedded LLM loaded: %s (%s) in %.2fs%s%s",
                    name, path, self.load_s, _placement_desc(placement),
                    " [embedding]" if embedding else "")


# ─── Degenerate-reply detection ────────────────────────────────────────────

def _is_echo(content: str, messages: list[dict[str, Any]]) -> bool:
    """True when the reply is a restatement of the user's last message."""
    last_user = next(
        (m.get("content", "") for m in reversed(messages) if m.get("role") == "user"),
        "",
    ).strip().lower()
    reply = content.strip().lower()
    if not last_user or not reply or not reply.endswith("?"):
        return False
    user_tokens = set(re.findall(r"[a-z]{3,}", last_user))
    reply_tokens = set(re.findall(r"[a-z]{3,}", reply))
    if not user_tokens:
        return False
    overlap = len(user_tokens & reply_tokens) / len(user_tokens)
    return overlap > 0.6


def _is_instruction_regurgitation(content: str) -> bool:
    """True when the model parroted the tool-calling instruction instead of
    calling a tool or answering (small-model failure mode)."""
    c = content.strip()
    return c.startswith("You can call tools") or (
        '"tool"' in c and "arguments" in c and "json" in c.lower() and len(c) < 500
    )


# ─── GGUF registry ─────────────────────────────────────────────────────────

def _human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def installed_ggufs(models_dir: str | Path) -> list[dict[str, Any]]:
    """GGUF files under models_dir (name = file stem, no extension)."""
    d = Path(models_dir)
    out: list[dict[str, Any]] = []
    if not d.is_dir():
        return out
    for f in sorted(d.glob("*.gguf")):
        try:
            size = f.stat().st_size
        except OSError:
            continue
        qm = re.search(r"(Q\d+_[A-Z0-9_]+|IQ\d+_[A-Z0-9_]+|F16|BF16)",
                       f.stem, re.IGNORECASE)
        out.append({
            "name": f.stem,
            "path": str(f),
            "size": size,
            "size_human": _human_size(size),
            "quantization": qm.group(1).upper() if qm else "—",
        })
    return out


# ─── Pools (worker pools) ──────────────────────────────────────────────────

class _InstancePool:
    """k homogeneous instances of ONE model behind a free-list.

    acquire() returns a free instance, creating one (up to max_workers)
    when none is free — loads run in a worker thread so the event loop
    keeps serving other models while a new worker boots. Instances are
    EXCLUSIVE while checked out: the pool itself is the concurrency gate
    (the per-instance locks are no longer taken on the request path).

    VRAM cost is k × the model's footprint — size worker pools for small
    hot models, not the 27B."""

    def __init__(self, name: str, max_workers: int, make):
        self.name = name
        self.max_workers = max(1, int(max_workers))
        self._make = make
        self.instances: list[_Instance] = []
        self.sig: tuple | None = None  # set by the engine (config shape)
        self.timeout_s = 120.0  # bounded worker wait (engine keeps it fresh)
        self._free: asyncio.Queue = asyncio.Queue()
        self._create_lock = asyncio.Lock()

    def _new_instance(self) -> _Instance:
        inst = self._make()
        self.instances.append(inst)
        return inst

    async def acquire(self) -> _Instance:
        while True:
            try:
                return self._free.get_nowait()
            except asyncio.QueueEmpty:
                pass
            if len(self.instances) < self.max_workers:
                async with self._create_lock:
                    try:  # someone may have released while we waited
                        return self._free.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    if len(self.instances) < self.max_workers:
                        return await asyncio.to_thread(self._new_instance)
            else:
                # All workers checked out — wait, but never forever. An
                # abandoned stream can hold a checkout indefinitely
                # (client vanished mid-generation); parking here wedged
                # every later request to that model (observed live).
                try:
                    return await asyncio.wait_for(self._free.get(),
                                                  timeout=self.timeout_s)
                except asyncio.TimeoutError:
                    raise EmbeddedLLMError(
                        f"model {self.name!r} busy — no free worker for "
                        f"{self.timeout_s:.0f}s (long generation or stuck "
                        "stream; retry shortly or POST /admin/unload to "
                        "reload the model)") from None

    def release(self, inst: _Instance) -> None:
        self._free.put_nowait(inst)

    def checked_out(self) -> int:
        """Instances currently held by in-flight requests (approximation:
        a worker mid-CREATE counts as checked out)."""
        return max(0, len(self.instances) - self._free.qsize())

    def first_or_create(self) -> _Instance:
        """Synchronous preload/registry access: the first instance,
        created on the caller's thread when the pool is empty. The new
        instance is ALSO seeded into the free list — /admin/load exists
        to make the model immediately servable, and acquire() only hands
        out what's in _free."""
        if not self.instances:
            inst = self._new_instance()
            self._free.put_nowait(inst)
            return inst
        return self.instances[0]


# ─── Engine ────────────────────────────────────────────────────────────────

class EmbeddedEngine:
    """OpenAI-shaped async client backed by in-process llama_cpp models.

    Consumers treat it like an AsyncOpenAI client:
      await engine.chat.completions.create(model=..., messages=..., ...)

    Any number of models can be resident at once — each is a lazily
    loaded _Instance with its own asyncio lock, so different models
    generate concurrently (VRAM is the only limit). Resolution: config
    models under backends.local.models first, then auto-discovery of
    GGUF stems in models_dir (default ctx 4096 / default placement).
    Changing a model's gguf/ctx/placement hot-reloads it on next use.
    """

    def __init__(self, config: GatewayConfig | None = None):
        self.chat = _ChatEndpoint(self)
        self._config = config or get_gateway_config()
        # Chat models live in POOLS (worker pools: k homogeneous instances,
        # lazily created, exclusively checked out — concurrent requests to
        # one model run in parallel instead of serializing on a lock).
        self._pools: dict[str, _InstancePool] = {}
        # Embedding instances stay SINGLE (llama_cpp batches internally).
        self._embed_instances: dict[str, _Instance] = {}
        self._embed_instance_cfg: dict[str, tuple[str, int, str]] = {}

    # ── instance registry ──

    @staticmethod
    def resolve_gguf(spec: str, models_dir: str | Path) -> str:
        """Model spec (GGUF stem or path) → filesystem path ('' if
        unresolvable). Direct paths first, then the models_dir registry
        (name or path match), then bare filenames under models_dir."""
        spec = (spec or "").strip()
        if not spec:
            return ""
        p = Path(spec)
        if p.is_file():
            return str(p)
        for g in installed_ggufs(models_dir):
            if g["name"] == spec or g["path"] == spec:
                return g["path"]
        for cand in (Path(models_dir) / spec, Path(models_dir) / (spec + ".gguf")):
            if cand.is_file():
                return str(cand)
        return ""

    def _pool_for(self, model_name: str | None) -> _InstancePool:
        """Pool for a model, built from the CURRENT config entry. A config
        change (gguf/ctx/placement/workers) replaces the pool — in-flight
        requests keep their checked-out instances (old pool is GC'd)."""
        self._config.maybe_reload()
        name = (model_name or "").strip()
        entry = self._config.local_models().get(name)
        if entry is None:
            # Auto-discovery: any GGUF in models_dir is servable by its
            # stem with defaults (ctx 4096, one worker). Config models
            # always win — custom ctx/placement/workers, paths outside
            # models_dir, and they shadow same-named stems.
            if name and self.resolve_gguf(name, self._config.models_dir):
                entry = {"gguf": name, "ctx": 4096, "placement": ""}
            else:
                raise EmbeddedLLMError(
                    f"unknown model {model_name!r} — define it under "
                    "backends.local.models in the gateway config, or drop "
                    "its GGUF into models_dir")
        path = self.resolve_gguf(entry.get("gguf", ""), self._config.models_dir)
        if not path:
            raise EmbeddedLLMError(
                f"GGUF for {name!r} not found: {entry.get('gguf')!r} "
                f"(looked in {self._config.models_dir})")
        try:
            ctx = int(entry.get("ctx") or 4096)
        except (TypeError, ValueError):
            ctx = 4096
        placement = _cfg_gpu(entry.get("placement", ""))
        try:
            workers = max(1, int(entry.get("workers") or 1))
        except (TypeError, ValueError):
            workers = 1
        want = (path, ctx, placement, workers)
        pool = self._pools.get(name)
        if pool is not None and pool.sig != want:
            logger.info("gateway model %r config changed — reloading", name)
            self._pools.pop(name, None)
            pool = None
        if pool is None:
            pool = _InstancePool(name, workers,
                                 make=lambda: _Instance(name, path, ctx, placement))
            pool.sig = want
            self._pools[name] = pool
        try:
            pool.timeout_s = float((self._config.queue or {})
                                   .get("timeout_s") or 120)
        except (TypeError, ValueError):
            pool.timeout_s = 120.0
        return pool

    def _get_instance(self, model_name: str | None) -> _Instance:
        """Sync registry/preload access: first instance of the pool,
        created on the caller's thread when empty (tests, /admin/load)."""
        return self._pool_for(model_name).first_or_create()

    def _model_entry(self, model_name: str | None) -> dict[str, Any]:
        """Config entry for a chat model — {} for auto-discovered GGUFs
        (no knobs; the unknown-name error surfaces at _acquire). Per-
        request knobs (no_think, max_prompt_tokens) live here, NOT in
        the pool signature: flag flips hot-reload on the next request
        without rebuilding the loaded instance."""
        return self._config.local_models().get((model_name or "").strip()) or {}

    async def _acquire(self, model_name: str | None) -> _Instance:
        """Check out an exclusive instance for a request (worker pool)."""
        return await self._pool_for(model_name).acquire()

    def _release(self, model_name: str | None, inst: _Instance) -> None:
        pool = self._pools.get((model_name or "").strip())
        if pool is not None:
            pool.release(inst)
        # pool replaced mid-request (config change) — the old pool (and the
        # checked-out instance with it) is garbage collected; dropping the
        # release is correct.

    def _get_embedding_instance(self, model_name: str | None) -> _Instance:
        """Lazily load / return an EMBEDDING instance (config-defined only —
        no auto-discovery: a chat GGUF mistaken for an embedder would
        return garbage vectors, so embedding models must be explicit)."""
        self._config.maybe_reload()
        name = (model_name or "").strip()
        entry = self._config.local_embedding_models().get(name)
        if entry is None:
            raise EmbeddedLLMError(
                f"unknown embedding model {model_name!r} — define it under "
                "backends.local.embedding_models in the gateway config")
        path = self.resolve_gguf(entry.get("gguf", ""), self._config.models_dir)
        if not path:
            raise EmbeddedLLMError(
                f"GGUF for embedding model {name!r} not found: "
                f"{entry.get('gguf')!r} (looked in {self._config.models_dir})")
        try:
            ctx = int(entry.get("ctx") or 4096)
        except (TypeError, ValueError):
            ctx = 4096
        placement = _cfg_gpu(entry.get("placement", ""))
        want = (path, ctx, placement)
        inst = self._embed_instances.get(name)
        if inst is not None and self._embed_instance_cfg.get(name) != want:
            logger.info("gateway embedding model %r config changed — reloading", name)
            self._embed_instances.pop(name, None)
            inst = None
        if inst is None:
            inst = _Instance(name, path, ctx, placement, embedding=True)
            self._embed_instances[name] = inst
            self._embed_instance_cfg[name] = want
        return inst

    async def embed(self, model: str, input: str | list[str]) -> dict[str, Any]:
        """OpenAI-shaped embedding response from a local embedding GGUF.
        llama_cpp's create_embedding already returns the wire format:
        {object, data: [{object, embedding, index}], model, usage}."""
        inst = self._get_embedding_instance(model)
        inputs = [input] if isinstance(input, str) else list(input)
        async with inst.lock:
            try:
                return await asyncio.to_thread(
                    inst.llama.create_embedding, input=inputs)
            except Exception as e:
                raise EmbeddedLLMError(f"embedded embedding failed: {e}") from e

    # ── legacy text completions (POST /v1/completions) ──

    async def complete(self, model: str, prompt: str, *,
                       temperature: float | None = None,
                       max_tokens: int | None = None,
                       stream: bool = False):
        """Raw text completion (no chat template, no tools, no
        think-stripping — legacy /v1/completions semantics). Returns the
        llama_cpp wire shape (already OpenAI text.completion) or, when
        stream=True, an async iterator of text.completion.chunk chunks."""
        inst = await self._acquire(model)
        llm_kwargs: dict[str, Any] = {
            "temperature": temperature if temperature is not None else 0.8}
        if max_tokens:
            llm_kwargs["max_tokens"] = max_tokens
        if stream:
            return self._stream_completion(model, inst, prompt, llm_kwargs)
        try:
            return await asyncio.to_thread(
                inst.llama.create_completion, prompt, **llm_kwargs)
        except EmbeddedLLMError:
            raise
        except Exception as e:
            raise EmbeddedLLMError(f"embedded completion failed: {e}") from e
        finally:
            self._release(model, inst)

    async def _stream_completion(self, model_name: str | None,
                                 inst: _Instance, prompt: str,
                                 llm_kwargs: dict) -> AsyncIterator[dict]:
        """Bridge the sync llama_cpp completion stream into async — raw
        text deltas, no think-hold-back (legacy semantics)."""
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def _emit(text: str) -> None:
            loop.call_soon_threadsafe(
                q.put_nowait, ("chunk", {"choices": [{"text": text}]}))

        def _worker() -> None:
            try:
                sync_iter = inst.llama.create_completion(
                    prompt=prompt, stream=True, **llm_kwargs)
                for chunk in sync_iter:
                    text = (chunk.get("choices") or [{}])[0].get("text")
                    if text:
                        _emit(text)
                loop.call_soon_threadsafe(q.put_nowait, ("done", None))
            except Exception as e:
                loop.call_soon_threadsafe(q.put_nowait, ("error", e))

        try:
            thread = threading.Thread(target=_worker, daemon=True)
            thread.start()
            while True:
                kind, payload = await q.get()
                if kind == "done":
                    break
                if kind == "error":
                    raise EmbeddedLLMError(
                        f"embedded completion stream failed: {payload}")
                yield payload
        finally:
            self._release(model_name, inst)

    def _unload_names(self, which: str) -> list[str]:
        if which == "all":
            return list(self._pools) + list(self._embed_instances)
        return ([which] if which in self._pools
                or which in self._embed_instances else [])

    async def drain(self, which: str = "all", wait_s: float = 5.0) -> dict:
        """Bounded wait for in-flight checkouts to return before an
        unload. Returns per-model {name: stuck_count} — models whose
        workers are still held when the deadline hits (abandoned
        streams); their VRAM stays pinned until the holders are GC'd."""
        deadline = time.monotonic() + max(0.0, float(wait_s))
        stuck: dict[str, int] = {}
        while True:
            stuck = {}
            for nm in self._unload_names(which):
                pool = self._pools.get(nm)
                if pool is not None and pool.checked_out() > 0:
                    stuck[nm] = pool.checked_out()
            if not stuck or time.monotonic() >= deadline:
                return stuck
            await asyncio.sleep(0.05)

    def unload(self, which: str = "all") -> int:
        """Drop pools/instances to free VRAM (chat AND embedding).
        Returns number of instances unloaded. Callers wanting graceful
        shutdown should await drain() first — checked-out instances
        abandoned here keep their VRAM pinned until GC reaps them."""
        names = self._unload_names(which)
        n = 0
        for nm in names:
            pool = self._pools.pop(nm, None)
            if pool is not None:
                n += len(pool.instances)
            if self._embed_instances.pop(nm, None) is not None:
                self._embed_instance_cfg.pop(nm, None)
                n += 1
        gc.collect()
        return n

    def is_loaded(self) -> bool:
        return bool(self._pools) or bool(self._embed_instances)

    def loaded_models(self) -> list[dict[str, Any]]:
        """Currently loaded instances, Ollama-ps shaped (for /healthz in
        Stage 2 and the console in Stage 5). Pooled models with more than
        one worker carry a worker index; embedding instances carry
        role 'embed'. Chat pools report role 'chat' — matching what the
        console's configured-models table and /v1/models show, so the
        loaded-vs-configured tables agree."""

        def _entry(inst: _Instance, role: str = "chat",
                   worker: int | None = None) -> dict[str, Any]:
            try:
                size = Path(inst.path).stat().st_size
            except OSError:
                size = 0
            n_ctx_raw = getattr(getattr(inst, "llama", None), "n_ctx", 0)
            n_ctx = int(n_ctx_raw() if callable(n_ctx_raw) else (n_ctx_raw or 0))
            out = {
                "name": Path(inst.path).stem,
                "alias": inst.name,
                "description": f"{inst.name} · {inst.path}",
                "path": inst.path,
                "role": role,
                "size_vram": size,
                "size_vram_human": _human_size(size),
                "size": size,
                "n_ctx": n_ctx,
                "load_s": inst.load_s,
                "cpu": False,
                "gpu": True,
                "expires": "",
                "digest": "",
            }
            if worker is not None:
                out["worker"] = worker
            return out

        out: list[dict[str, Any]] = []
        for pool in self._pools.values():
            many = len(pool.instances) > 1
            for i, inst in enumerate(pool.instances):
                out.append(_entry(inst, worker=i if many else None))
        return out + [_entry(i, role="embed")
                      for i in self._embed_instances.values()]

    # ── tool-calling helpers ──

    @staticmethod
    def _tools_instruction(tools: list[dict]) -> str:
        """Serialize tools into a system instruction (JSON-call convention)."""
        lines = [
            "You can call tools. When you need a tool, reply with ONLY a JSON object:",
            '{"tool": "<name>", "arguments": { ... } }',
            "When a tool result is already present in the conversation, do NOT call",
            "the tool again — answer the user directly in plain text using the result.",
            "Available tools:",
        ]
        for t in tools:
            fn = t.get("function", {})
            name = fn.get("name", "")
            desc = fn.get("description", "")
            params = json.dumps(fn.get("parameters", {}))
            lines.append(f"- {name}: {desc} | args schema: {params}")
        return "\n".join(lines)

    @staticmethod
    def _parse_tool_call(content: str, tools: list[dict]) -> _ToolCall | None:
        """Extract {"tool": name, "arguments": {...}} from model output."""
        text = content.strip()
        # Direct JSON, or first {...} block embedded in prose/fences
        candidates = [text]
        if "```" in text:
            for part in text.split("```"):
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    candidates.append(part)
        else:
            i, j = text.find("{"), text.rfind("}")
            if 0 <= i < j:
                candidates.append(text[i : j + 1])
        names = {t.get("function", {}).get("name", "") for t in tools}
        for cand in candidates:
            try:
                obj = json.loads(cand)
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(obj, dict):
                continue
            name = obj.get("tool") or obj.get("name") or ""
            if name in names:
                args = obj.get("arguments", obj.get("args", {}))
                return _ToolCall(
                    id=f"call_{name}_{int(time.time() * 1000)}",
                    function=_Fn(name=name, arguments=json.dumps(args)),
                )
        return None

    @staticmethod
    def _normalize_messages(messages: list[dict[str, Any]], tools: list[dict] | None) -> list[dict]:
        """Flatten OpenAI tool-calling history into plain chat messages.

        - system tool instruction MERGED into a single leading system message
          (GGUF chat templates — Qwen3, Llama3, Mistral — raise
          "System message must be at the beginning" for any system message
          after the first; a trailing tools-instruction system message broke
          every embedded tool-calling request)
        - assistant tool_calls → assistant text describing the call
        - role="tool" → user message with the tool result
        - any non-leading system messages are folded into the leading one
        """
        out: list[dict[str, Any]] = []
        system_parts: list[str] = []
        for m in messages:
            role = m.get("role", "user")
            content = m.get("content")
            if role == "tool":
                out.append({"role": "user", "content": f"Tool result: {content}"})
                continue
            if role == "assistant" and m.get("tool_calls"):
                parts = [content or ""]
                for tc in m["tool_calls"]:
                    fn = tc.get("function", {})
                    parts.append(f"Called tool {fn.get('name')} with {fn.get('arguments')}")
                out.append({"role": "assistant", "content": "\n".join(parts).strip()})
                continue
            if role == "system":
                if content:
                    system_parts.append(content)
                continue
            if content:
                out.append({"role": role, "content": content})
        if tools:
            system_parts.append(EmbeddedEngine._tools_instruction(tools))
        if system_parts:
            out.insert(0, {"role": "system", "content": "\n\n".join(system_parts)})
        return out

    @staticmethod
    def _apply_no_think(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Append the Qwen3 /no_think soft switch to a normalized chat.

        The GGUF chat template has no no_think logic of its own (only an
        enable_thinking variable llama-cpp-python 0.3.35 cannot pass),
        but the MODEL is trained on the marker and complies — verified
        live on Qwen3-8B-Q4_K_M: 400-token reasoning run → 7 tokens
        (empty <think></think>, then the answer), 3.2s → 0.07s. Marker
        goes on the merged leading system message when present, else on
        the newest user turn (the documented fallback position)."""
        if messages and messages[0].get("role") == "system":
            first = dict(messages[0])
            first["content"] = ((first.get("content") or "").strip()
                                + "\n/no_think")
            return [first] + list(messages[1:])
        for i in range(len(messages) - 1, -1, -1):
            if (messages[i].get("role") == "user"
                    and isinstance(messages[i].get("content"), str)
                    and messages[i]["content"]):
                m = dict(messages[i])
                m["content"] = m["content"].rstrip() + "\n/no_think"
                return list(messages[:i]) + [m] + list(messages[i + 1:])
        return messages

    # ── context-window fitting ──────────────────────────────────────────
    # llama-cpp raises "Requested tokens (N) exceed context window" on
    # overflow — it does NOT truncate. Agent clients that resend full
    # history (Workshop sessions reach 150k+ tokens) die mid-stream
    # without this; trim oldest non-system messages to fit instead.

    _FIXED_OVERHEAD = 64    # chat-template fixed overhead, tokens
    _PER_MSG_OVERHEAD = 16  # per-message template wrappers, tokens
    _TRIM_LOW_WATERMARK = 0.7  # over-budget prompts trim to 70% of the
                               # budget — the kept prefix then stays
                               # stable for a 30% growth window, so
                               # llama.cpp's KV prefix cache survives
    _TRIM_NOTE = ("\n\n[context note: earlier conversation history was "
                  "truncated to fit the context window]")

    @staticmethod
    def _count_tokens(inst: "_Instance", text: str) -> int:
        """Exact count via the instance's tokenizer; ~chars/4 fallback.

        The fallback keeps this testable with fake Llamas (and safe if
        a llama-cpp build lacks tokenize) — it only needs to be close,
        the budget carries the safety margin.
        """
        tok = getattr(getattr(inst, "llama", None), "tokenize", None)
        if callable(tok):
            data = text.encode("utf-8", "ignore")
            try:
                return len(tok(data, add_bos=False))
            except TypeError:
                try:
                    return len(tok(data))
                except Exception:  # noqa: BLE001 — fall back below
                    pass
            except Exception:  # noqa: BLE001
                pass
        return max(1, len(text) // 4)

    @staticmethod
    def _truncate_text(inst: "_Instance", text: str, max_tokens: int) -> str:
        """Shorten text to <= max_tokens, keeping head AND tail.

        Agent prompts put the actual question at the END of a message
        while file/log dumps matter from the start — a middle cut keeps
        both. Verifies with the same counter _fit_messages uses; returns
        text unchanged when it already fits."""
        if not text:
            return text
        total = EmbeddedEngine._count_tokens(inst, text)
        if total <= max_tokens:
            return text
        marker = ("\n[... middle of this message was truncated to fit "
                  "the context window ...]\n")
        for shrink in (0.92, 0.75, 0.6, 0.45, 0.3, 0.2, 0.12):
            target = max(128, int(len(text) * (max_tokens / total) * shrink))
            half = max(32, (target - len(marker)) // 2)
            cand = text[:half] + marker + text[-half:]
            if EmbeddedEngine._count_tokens(inst, cand) <= max_tokens:
                return cand
        # pathological tokenizer behavior — hard cut as the last resort
        target = max(128, int(len(text) * (max_tokens / total) * 0.1))
        return text[:target] + "\n[... truncated ...]"

    @staticmethod
    def _fit_messages(inst: "_Instance", messages: list[dict[str, Any]],
                      max_tokens: int | None,
                      tools: list[dict] | None = None,
                      prompt_cap: int | None = None,
                      tools_cost: int | None = None,
                      ) -> tuple[list[dict[str, Any]], list[dict] | None]:
        """Fit the prompt into the context window — never errors.

        Drop oldest non-system messages until the prompt fits n_ctx —
        down to a LOW WATERMARK (70% of the budget), not to exactly the
        budget: the kept prefix then stays identical while the session
        grows the remaining 30%, so llama.cpp's automatic KV prefix
        reuse covers it and only the new suffix is decoded.
        Keeps every system message (instructions) and the newest turns.
        When the newest message ALONE (plus system) exceeds the window —
        huge tool_result/summary blocks from agent clients — its content
        is head+tail truncated instead of raising (a degraded answer
        beats a hard failure); the system prompt is only truncated as a
        last resort when it fills the window itself.

        The budget reserves room for the tool definitions: they are
        serialized into the system message by _normalize_messages AFTER
        trimming, so a message-only count never sees them — agent
        clients (Workshop) send tool schemas rendering to thousands of
        tokens (verified live: 24,490-token trim + ~8.8k of tools =
        33,288 rendered > 32,768). Returns (messages, tools); tools are
        returned unchanged unless they starve the window, in which case
        they are dropped (plain-chat degradation beats a hard failure).

        prompt_cap (backends.local.models.<name>.max_prompt_tokens)
        bounds the rendered prompt — messages + tools + overhead —
        BELOW the context window. Prefill time grows ~linearly with
        prompt size, and agent clients with hard client-side timeouts
        (Workshop sessions fail past ~16-50s) need the worst case
        bounded even when the window could hold more history. The cap
        must leave room above the tools floor or tools drop (same
        plain-chat degradation as an n_ctx-starved window).

        tools_cost overrides the tools budget (llama-server backends
        pass the exact cost of the NATIVE tools JSON their chat
        template renders — the embedded _tools_instruction estimate
        undercounts it and would overflow the child).
        """
        raw = getattr(getattr(inst, "llama", None), "n_ctx", 0)
        n_ctx = int(raw() if callable(raw) else (raw or 0)) or 4096
        reserve = min(int(max_tokens or 2048), max(256, n_ctx // 4))
        budget = n_ctx - reserve - EmbeddedEngine._FIXED_OVERHEAD
        if prompt_cap:
            budget = min(budget,
                         int(prompt_cap) - EmbeddedEngine._FIXED_OVERHEAD)
        if tools:
            if tools_cost is None:
                tools_cost = EmbeddedEngine._count_tokens(
                    inst, EmbeddedEngine._tools_instruction(tools))
            # floor: keep tools unless they'd starve messages to an
            # unusable sliver (proportional so small windows still work)
            floor = max(256, n_ctx // 16)
            if budget - tools_cost < floor:
                logger.warning("tool definitions (%d tokens) + reserve "
                               "leave < %d tokens for messages on %s "
                               "(n_ctx %d) — dropping tools", tools_cost,
                               floor, getattr(inst, "name", "?"), n_ctx)
                tools = None
            else:
                budget -= tools_cost

        def _msg_tokens(m: dict[str, Any]) -> int:
            n = EmbeddedEngine._PER_MSG_OVERHEAD
            c = m.get("content")
            if isinstance(c, list):
                c = json.dumps(c)
            if isinstance(c, str) and c:
                n += EmbeddedEngine._count_tokens(inst, c)
            for tc in m.get("tool_calls") or []:
                fn = (tc.get("function") or {}) if isinstance(tc, dict) else {}
                n += EmbeddedEngine._count_tokens(
                    inst, (fn.get("name") or "") + (fn.get("arguments") or ""))
            return n

        counts = [_msg_tokens(m) for m in messages]
        total = sum(counts)
        if total <= budget:
            return messages, tools

        sys_idx = [i for i, m in enumerate(messages) if m.get("role") == "system"]
        sys_cost = sum(counts[i] for i in sys_idx)
        nonsys = [i for i in range(len(messages)) if i not in set(sys_idx)]

        kept: list[int] = []
        acc = sys_cost
        # Stable-prefix (KV-cache) trim: an over-budget prompt drops to a
        # LOW WATERMARK, not to exactly-budget. Trimming to exactly the
        # budget moves the kept-boundary by one message on every
        # subsequent request, so llama.cpp's automatic KV prefix reuse
        # (Llama.generate: longest-common-prefix match against the
        # previously evaluated tokens) collapses to the system section
        # and every request re-prefills from scratch. Trimming to 70% of
        # the budget lets the session grow the remaining 30% before the
        # next trim — every request in that window reuses the previous
        # prompt as a cached prefix and decodes only the new suffix.
        low = max(int(budget * EmbeddedEngine._TRIM_LOW_WATERMARK), 64)
        kept: list[int] = []
        acc = sys_cost
        for i in reversed(nonsys):
            if acc + counts[i] > low:
                break
            kept.append(i)
            acc += counts[i]
        if not kept and nonsys and sys_cost + counts[nonsys[-1]] <= budget:
            # The newest message alone busts the watermark but FITS the
            # budget — keep it alone rather than truncating its content
            # (identical outcome to a no-watermark trim).
            kept.append(nonsys[-1])
            acc = sys_cost + counts[nonsys[-1]]
        if not kept:
            # The newest message alone (plus system) busts the budget.
            # Truncate its CONTENT instead of erroring — agent clients
            # (Workshop) resend full history and stuff huge tool results
            # / summaries into the newest message; a degraded answer
            # beats a hard 502. Head+tail cut via _truncate_text.
            out = [dict(messages[i]) for i in sys_idx]
            avail = budget - sys_cost - EmbeddedEngine._PER_MSG_OVERHEAD
            if avail < 64:
                # system alone eats the window — truncate it too
                sys_limit = max(64, (budget - EmbeddedEngine._PER_MSG_OVERHEAD
                                     - 64) // 2)
                if out and isinstance(out[0].get("content"), str):
                    out[0]["content"] = EmbeddedEngine._truncate_text(
                        inst, out[0]["content"], sys_limit)
                avail = (budget - sys_limit
                         - EmbeddedEngine._PER_MSG_OVERHEAD - 64)
            if nonsys:
                m = dict(messages[nonsys[-1]])
                c = m.get("content")
                if isinstance(c, list):
                    c = json.dumps(c)
                if isinstance(c, str):
                    m["content"] = EmbeddedEngine._truncate_text(
                        inst, c, max(64, avail - 64))
                tcs = m.get("tool_calls")
                if isinstance(tcs, list) and tcs:
                    new_tcs = []
                    for tc in tcs:
                        if (isinstance(tc, dict)
                                and isinstance(tc.get("function"), dict)):
                            tc = dict(tc)
                            fn = dict(tc["function"])
                            if fn.get("arguments"):
                                fn["arguments"] = EmbeddedEngine._truncate_text(
                                    inst, str(fn["arguments"]),
                                    max(32, avail // 2))
                            tc["function"] = fn
                        new_tcs.append(tc)
                    m["tool_calls"] = new_tcs
                out.append(m)
                logger.warning("prompt trimmed %d→<=%d tokens for %s (n_ctx "
                               "%d, reserve %d): newest message content "
                               "truncated, %d older messages dropped)",
                               total, budget, getattr(inst, "name", "?"),
                               n_ctx, reserve, len(nonsys) - 1)
            else:
                logger.warning("prompt trimmed %d→<=%d tokens for %s (n_ctx "
                               "%d, reserve %d): system content truncated)",
                               total, budget, getattr(inst, "name", "?"),
                               n_ctx, reserve)
            if out:
                out[0]["content"] = ((out[0].get("content") or "")
                                     + EmbeddedEngine._TRIM_NOTE)
            return out, tools
        kept.reverse()
        logger.warning("prompt trimmed %d→%d tokens for %s (n_ctx %d, "
                       "reserve %d, dropped %d oldest messages)",
                       total, acc, getattr(inst, "name", "?"), n_ctx,
                       reserve, len(nonsys) - len(kept))

        out = ([dict(messages[i]) for i in sys_idx]
               + [dict(messages[i]) for i in kept])
        if sys_idx:
            first = out[0]
            first["content"] = (first.get("content") or "") + EmbeddedEngine._TRIM_NOTE
        return out, tools

    def _completion_sync(self, inst: _Instance, messages: list[dict], kwargs: dict) -> tuple[_Message, dict[str, int]]:
        try:
            out = inst.llama.create_chat_completion(messages=messages, **kwargs)
        except TypeError as e:
            if "is not iterable" not in str(e):
                raise
            # Home-LLM's chat template iterates `tools` UNGUARDED
            # ("for tool in tools") — llama-cpp passes None when no tools
            # are supplied and jinja crashes before generation. Retry with
            # an empty tools list (the template then renders its
            # "No tools were provided" fallback, as under Ollama).
            out = inst.llama.create_chat_completion(
                messages=messages, tools=[], **kwargs)
        msg = out.get("choices", [{}])[0].get("message", {})
        usage = out.get("usage", {}) or {}
        content = _strip_think(msg.get("content") or "")
        return _Message(role="assistant", content=content), usage


# ─── chat.completions.create endpoint ──────────────────────────────────────

class _ChatEndpoint:
    def __init__(self, engine: EmbeddedEngine) -> None:
        self.completions = _CompletionsEndpoint(engine)


class _CompletionsEndpoint:
    def __init__(self, engine: EmbeddedEngine) -> None:
        self._engine = engine

    async def create(self, *, timeout: float = 60.0, **kwargs: Any):  # noqa: ARG002
        model_name = kwargs.get("model")
        messages = list(kwargs.get("messages") or [])
        tools = kwargs.get("tools") or None
        stream = bool(kwargs.get("stream"))
        max_tokens = kwargs.get("max_tokens")
        temperature = kwargs.get("temperature")

        inst = await self._engine._acquire(model_name)
        # Per-model config knobs (hot-reload; NOT in the pool signature):
        # - max_prompt_tokens: cap the rendered prompt below n_ctx —
        #   prefill ~linear in prompt size, agent clients with hard
        #   client-side timeouts need the worst case bounded
        # - no_think: Qwen3 soft switch — skips the reasoning phase
        #   (held back from streams, so it is pure added latency)
        entry = self._engine._model_entry(model_name)
        try:
            prompt_cap = int(entry.get("max_prompt_tokens") or 0) or None
        except (TypeError, ValueError):
            prompt_cap = None
        no_think = bool(entry.get("no_think"))
        # Fit to the context window BEFORE anything else: llama-cpp
        # errors on overflow instead of truncating, so a full-history
        # agent session would die mid-stream ("Requested tokens exceed
        # context window"). Tokenizing a huge prompt is not free — keep
        # it off the event loop. Pass tools: their serialized form joins
        # the system message after trimming and must be budgeted for.
        messages, tools = await asyncio.to_thread(
            self._engine._fit_messages, inst, messages, max_tokens, tools,
            prompt_cap)
        llm_kwargs: dict[str, Any] = {"temperature": temperature if temperature is not None else 0.1}
        if max_tokens:
            llm_kwargs["max_tokens"] = max_tokens
        if stream:
            norm = self._engine._normalize_messages(messages, tools)
            if no_think:
                norm = EmbeddedEngine._apply_no_think(norm)
            # the generator owns the checkout; it releases in its finally
            return self._stream(model_name, inst, norm, llm_kwargs)
        try:
            norm = self._engine._normalize_messages(messages, tools)
            if no_think:
                norm = EmbeddedEngine._apply_no_think(norm)
            try:
                msg, usage = await asyncio.to_thread(
                    self._engine._completion_sync, inst, norm, llm_kwargs
                )
            except EmbeddedLLMError:
                raise
            except Exception as e:
                raise EmbeddedLLMError(f"embedded generation failed: {e}") from e

            # Tool-calling: parse JSON tool call out of content when tools were offered
            if tools and msg.content:
                tc = self._engine._parse_tool_call(msg.content, tools)
                if tc is not None:
                    msg.tool_calls = [tc]
                    logger.info("Embedded tool call: %s(%s)", tc.function.name, tc.function.arguments[:80])
                elif _is_echo(msg.content, messages) or _is_instruction_regurgitation(msg.content):
                    # Model echoed the question or parroted the tool instruction —
                    # retry once as plain chat (no tool instruction). Same worker:
                    # we still hold the exclusive checkout.
                    logger.info("Embedded echo detected — retrying without tool instruction")
                    plain = [m for m in messages if m.get("role") in ("system", "user", "assistant")]
                    # Same template constraint: fold any non-leading system
                    # messages into one at the front.
                    sys_txt = "\n\n".join(
                        (m.get("content") or "") for m in plain if m.get("role") == "system"
                    ).strip()
                    plain = [m for m in plain if m.get("role") != "system"]
                    if sys_txt:
                        plain.insert(0, {"role": "system", "content": sys_txt})
                    if no_think:
                        plain = EmbeddedEngine._apply_no_think(plain)
                    retry_kwargs = dict(llm_kwargs)
                    try:
                        msg, usage = await asyncio.to_thread(
                            self._engine._completion_sync, inst, plain, retry_kwargs
                        )
                    except Exception as e:
                        raise EmbeddedLLMError(f"embedded retry failed: {e}") from e
            return _Completion(choices=[_Choice(message=msg)], usage=usage)
        finally:
            self._engine._release(model_name, inst)

    async def _stream(self, model_name: str | None, inst: _Instance,
                      messages: list[dict], llm_kwargs: dict) -> AsyncIterator[_Chunk]:
        """Bridge the sync llama_cpp chunk generator into an async iterator.

        Thinking models (Qwen3-style) emit reasoning before a </think>
        closer — hold those deltas back so consumers never speak the
        reasoning. Non-thinking models never close: their text is flushed
        once the stream ends (or past the 8K degenerate cap), so content
        is never lost."""
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()  # unbounded — deltas are small

        def _emit(text: str) -> None:
            loop.call_soon_threadsafe(
                q.put_nowait, ("chunk", {"choices": [{"delta": {"content": text}}]})
            )

        def _worker() -> None:
            seen_close = False
            buf = ""
            try:
                sync_iter: Iterator[dict] = inst.llama.create_chat_completion(
                    messages=messages, stream=True, **llm_kwargs
                )
                for chunk in sync_iter:
                    delta = (chunk.get("choices") or [{}])[0].get("delta", {})
                    text = delta.get("content")
                    if not text:
                        continue
                    if seen_close:
                        _emit(text)
                        continue
                    buf += text
                    if "</think>" in buf:
                        seen_close = True
                        tail = buf.rsplit("</think>", 1)[1]
                        if tail:
                            _emit(tail)
                        buf = ""
                    elif len(buf) >= 8192:
                        # degenerate: reasoning ran long with no closer —
                        # stop holding (matches flush-at-end semantics)
                        seen_close = True
                        _emit(buf)
                        buf = ""
                if buf and not seen_close:
                    # stream ended, no closer — non-thinking output
                    _emit(buf)
                loop.call_soon_threadsafe(q.put_nowait, ("done", None))
            except Exception as e:  # surface generation errors to the consumer
                loop.call_soon_threadsafe(q.put_nowait, ("error", e))

        try:
            thread = threading.Thread(target=_worker, daemon=True)
            thread.start()
            while True:
                kind, payload = await q.get()
                if kind == "done":
                    break
                if kind == "error":
                    raise EmbeddedLLMError(f"embedded stream failed: {payload}")
                delta = (payload.get("choices") or [{}])[0].get("delta", {})
                text = delta.get("content")
                if text:
                    yield _Chunk(choices=[_ChoiceDelta(delta={"content": text})])
        finally:
            self._engine._release(model_name, inst)


# ─── Process-wide singleton ────────────────────────────────────────────────

_ENGINE: EmbeddedEngine | None = None


def get_engine() -> EmbeddedEngine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = EmbeddedEngine()
    return _ENGINE
