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
    'auto' = one model split across all cards (free-VRAM weighted)."""

    def __init__(self, name: str, path: str, n_ctx: int, placement: str = ""):
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
        self.lock = asyncio.Lock()
        logger.info("Embedded LLM loaded: %s (%s) in %.2fs%s",
                    name, path, self.load_s, _placement_desc(placement))


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


# ─── Engine ────────────────────────────────────────────────────────────────

class EmbeddedEngine:
    """OpenAI-shaped async client backed by in-process llama_cpp models.

    Consumers treat it like an AsyncOpenAI client:
      await engine.chat.completions.create(model=..., messages=..., ...)

    Instances are keyed by gateway model name (the 'local' backend's
    models in the gateway config); changing gguf/ctx/placement hot-reloads
    the instance on its next use.
    """

    def __init__(self, config: GatewayConfig | None = None):
        self.chat = _ChatEndpoint(self)
        self._config = config or get_gateway_config()
        self._instances: dict[str, _Instance] = {}
        self._instance_cfg: dict[str, tuple[str, int, str]] = {}

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

    def _get_instance(self, model_name: str | None) -> _Instance:
        self._config.maybe_reload()
        name = (model_name or "").strip()
        entry = self._config.local_models().get(name)
        if entry is None:
            raise EmbeddedLLMError(
                f"unknown local model {model_name!r} — define it under "
                "backends.local.models in the gateway config")
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
        want = (path, ctx, placement)
        inst = self._instances.get(name)
        if inst is not None and self._instance_cfg.get(name) != want:
            logger.info("gateway model %r config changed — reloading", name)
            self._instances.pop(name, None)
            inst = None
        if inst is None:
            inst = _Instance(name, path, ctx, placement)
            self._instances[name] = inst
            self._instance_cfg[name] = want
        return inst

    def unload(self, which: str = "all") -> int:
        """Drop instances to free VRAM. Returns number unloaded."""
        names = (list(self._instances) if which == "all"
                 else [which] if which in self._instances else [])
        for n in names:
            self._instances.pop(n, None)
            self._instance_cfg.pop(n, None)
        gc.collect()
        return len(names)

    def is_loaded(self) -> bool:
        return bool(self._instances)

    def loaded_models(self) -> list[dict[str, Any]]:
        """Currently loaded instances, Ollama-ps shaped (for /healthz in
        Stage 2 and the console in Stage 5)."""

        def _entry(inst: _Instance) -> dict[str, Any]:
            try:
                size = Path(inst.path).stat().st_size
            except OSError:
                size = 0
            n_ctx_raw = getattr(getattr(inst, "llama", None), "n_ctx", 0)
            n_ctx = int(n_ctx_raw() if callable(n_ctx_raw) else (n_ctx_raw or 0))
            return {
                "name": Path(inst.path).stem,
                "alias": inst.name,
                "description": f"{inst.name} · {inst.path}",
                "path": inst.path,
                "role": "gateway",
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

        return [_entry(i) for i in self._instances.values()]

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

        inst = self._engine._get_instance(model_name)
        norm = self._engine._normalize_messages(messages, tools)

        llm_kwargs: dict[str, Any] = {"temperature": temperature if temperature is not None else 0.1}
        if max_tokens:
            llm_kwargs["max_tokens"] = max_tokens

        if stream:
            return self._stream(inst, norm, llm_kwargs)

        async with inst.lock:
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
                # retry once as plain chat (no tool instruction).
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
                retry_kwargs = dict(llm_kwargs)
                async with inst.lock:
                    try:
                        msg, usage = await asyncio.to_thread(
                            self._engine._completion_sync, inst, plain, retry_kwargs
                        )
                    except Exception as e:
                        raise EmbeddedLLMError(f"embedded retry failed: {e}") from e
        return _Completion(choices=[_Choice(message=msg)], usage=usage)

    async def _stream(self, inst: _Instance, messages: list[dict], llm_kwargs: dict) -> AsyncIterator[_Chunk]:
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

        async with inst.lock:
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


# ─── Process-wide singleton ────────────────────────────────────────────────

_ENGINE: EmbeddedEngine | None = None


def get_engine() -> EmbeddedEngine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = EmbeddedEngine()
    return _ENGINE
