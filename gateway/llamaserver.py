"""llama-server backend — subprocess mode (continuous batching / slots).

One llama-server child per (backend, model), spawned LAZILY on first
request, health-polled until ready, and stopped when idle (idle_timeout_s)
or at gateway shutdown. Unlike the embedded engine — whose instances
serialize on a per-instance asyncio.Lock — llama-server batches concurrent
requests across slots on ONE loaded model.

Why subprocess instead of in-process llama.cpp: process isolation (a crash
never takes the gateway down), llama.cpp's server-side continuous batching,
and native OpenAI wire format (chat completions, SSE streaming, tool-call
deltas) — the gateway just proxies.

Remote llama-server (base_url set — RunPod serverless, Workshop's own
llama-server, any box on the LAN) is NOT handled here: config normalizes
it to plain OpenAI-style model ids and the app routes it through the
cloud-client passthrough (gateway/cloud.py).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import subprocess
import time
import types
from pathlib import Path
from subprocess import Popen
from typing import Any, AsyncIterator

import httpx

from .config import PROJECT_ROOT, GatewayConfig
from .models import EmbeddedEngine, detect_gpus

logger = logging.getLogger(__name__)


class LlamaServerError(Exception):
    """llama-server unusable or its call failed — maps to HTTP 502."""


# ── HTTP bridge (module-level so tests can monkeypatch) ────────────────────

_client: httpx.AsyncClient | None = None


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient()  # timeouts passed per request
    return _client


async def get_health(base_url: str, timeout: float = 5.0) -> bool:
    """llama-server /health — 200 once the model is loaded and serving."""
    try:
        r = await _http().get(f"{base_url}/health", timeout=timeout)
        return r.status_code == 200
    except httpx.HTTPError:
        return False


async def post_chat(base_url: str, payload: dict[str, Any],
                    timeout: float) -> dict[str, Any]:
    """Non-streaming chat — llama-server already answers OpenAI-shaped."""
    r = await _http().post(f"{base_url}/v1/chat/completions", json=payload,
                           timeout=timeout)
    if r.status_code >= 400:
        raise LlamaServerError(
            f"llama-server {r.status_code}: {r.text[:300]}")
    return r.json()


async def stream_chat(base_url: str, payload: dict[str, Any],
                      timeout: float) -> AsyncIterator[str]:
    """Streaming chat — yields SSE data payloads (the text after 'data: ',
    including the final '[DONE]')."""
    async with _http().stream("POST", f"{base_url}/v1/chat/completions",
                              json=payload, timeout=timeout) as r:
        if r.status_code >= 400:
            body = (await r.aread()).decode("utf-8", "replace")
            raise LlamaServerError(
                f"llama-server {r.status_code}: {body[:300]}")
        async for line in r.aiter_lines():
            if line.startswith("data: "):
                yield line[6:]


async def post_completions(base_url: str, payload: dict[str, Any],
                           timeout: float) -> dict[str, Any]:
    """Legacy POST /v1/completions — llama-server (and any OpenAI-compatible
    server) answers the OpenAI text.completion shape natively."""
    r = await _http().post(f"{base_url}/v1/completions", json=payload,
                           timeout=timeout)
    if r.status_code >= 400:
        raise LlamaServerError(
            f"llama-server {r.status_code}: {r.text[:300]}")
    return r.json()


async def stream_completions(base_url: str, payload: dict[str, Any],
                             timeout: float) -> AsyncIterator[str]:
    """Streaming legacy completions — yields SSE data payloads."""
    async with _http().stream("POST", f"{base_url}/v1/completions",
                              json=payload, timeout=timeout) as r:
        if r.status_code >= 400:
            body = (await r.aread()).decode("utf-8", "replace")
            raise LlamaServerError(
                f"llama-server {r.status_code}: {body[:300]}")
        async for line in r.aiter_lines():
            if line.startswith("data: "):
                yield line[6:]


# ── Tokenizer bridge (/tokenize) ───────────────────────────────────────────

_sync_client: httpx.Client | None = None


def _sync_http() -> httpx.Client:
    global _sync_client
    if _sync_client is None:
        _sync_client = httpx.Client()
    return _sync_client


def post_tokenize_sync(base_url: str, content: str,
                       timeout: float = 5.0) -> list[int] | None:
    """Exact token count from the child's /tokenize endpoint.

    SYNC on purpose: the shared trim helper (EmbeddedEngine._fit_
    messages) is sync and runs inside asyncio.to_thread. Returns None
    on any failure — callers fall back to the heuristic."""
    try:
        r = _sync_http().post(f"{base_url}/tokenize",
                              json={"content": content,
                                    "add_special": False},
                              timeout=timeout)
        if r.status_code == 400:  # older builds reject add_special — retry bare
            r = _sync_http().post(f"{base_url}/tokenize",
                                  json={"content": content},
                                  timeout=timeout)
        if r.status_code == 200:
            toks = (r.json() or {}).get("tokens")
            if isinstance(toks, list):
                return toks
    except Exception:  # noqa: BLE001 — heuristic fallback is the contract
        pass
    return None


class _TokenizerBridge:
    """Adapter giving EmbeddedEngine._fit_messages an exact tokenizer.

    _fit_messages is a SYNC static helper that reads only
    inst.llama.n_ctx and inst.llama.tokenize() — this bridge supplies
    both from a live llama-server child: n_ctx is the per-SLOT context
    (ctx // parallel), tokenize() hits the child's /tokenize endpoint
    for exact model-matched counts. Counts are cached by text; any
    /tokenize failure falls back to ~chars/4 — same contract as the
    embedded engine's own fallback."""

    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url
        self.timeout = timeout
        self.n_ctx = 4096  # caller sets the real slot context
        self._cache: dict[str, int] = {}

    def tokenize(self, data, add_bos=False):  # noqa: ARG002 — llama-cpp signature
        """Returns a list whose LENGTH is the count (callers only len())."""
        text = (data.decode("utf-8", "ignore")
                if isinstance(data, (bytes, bytearray)) else str(data))
        return [0] * self.count(text)

    def count(self, text: str) -> int:
        if text in self._cache:
            return self._cache[text]
        toks = post_tokenize_sync(self.base_url, text, self.timeout)
        n = len(toks) if toks is not None else max(1, len(text) // 4)
        self._cache[text] = n
        return n


def _slot_budget(n_ctx: int, max_tokens: int | None,
                 prompt_cap: int | None) -> int:
    """Message budget inside one slot — mirrors _fit_messages' math."""
    reserve = min(int(max_tokens or 2048), max(256, n_ctx // 4))
    budget = n_ctx - reserve - EmbeddedEngine._FIXED_OVERHEAD
    if prompt_cap:
        budget = min(budget, int(prompt_cap) - EmbeddedEngine._FIXED_OVERHEAD)
    return budget


def _msg_texts(messages: list[dict[str, Any]]) -> list[str]:
    """Texts whose token counts approximate the rendered prompt (same
    fields _fit_messages' _msg_tokens weighs)."""
    texts: list[str] = []
    for m in messages:
        c = m.get("content")
        if isinstance(c, list):
            texts.append(json.dumps(c))
        elif isinstance(c, str) and c:
            texts.append(c)
        for tc in m.get("tool_calls") or []:
            if (isinstance(tc, dict)
                    and isinstance(tc.get("function"), dict)):
                fn = tc["function"]
                texts.append((fn.get("name") or "")
                             + (fn.get("arguments") or ""))
    return texts


def fit_chat(base_url: str, messages: list[dict[str, Any]],
             max_tokens: int | None, tools: list[dict] | None,
             ctx: int, parallel: int,
             prompt_cap: int | None = None, name: str = "llama-server",
             timeout: float = 5.0
             ) -> tuple[list[dict[str, Any]], list[dict] | None]:
    """Fit a chat payload into the child's per-SLOT context — never errors.

    llama-server 400s on overflow instead of truncating (a full-history
    agent session kills the request), so trim here: drop oldest
    non-system messages, truncate an oversized newest message, and
    budget the NATIVE tools JSON (the child's chat template renders it
    server-side — its exact cost beats the embedded _tools_instruction
    estimate by ~40%). EmbeddedEngine._fit_messages does the real work;
    this wraps it with the slot context and the tokenizer bridge.

    Fast path: ONE /tokenize round trip on the joined contents (plus
    one for the tools JSON) — when the prompt clearly fits, per-message
    counts are skipped. Joined separators make the fast-path total
    slightly LARGER than the slow path's, so it never skips a needed
    trim. Only near/over budget does the full trim run.

    Knobs (no_think, max_prompt_tokens as prompt_cap) live in the
    model's config entry, are read per request by the caller, and are
    deliberately NOT part of the child signature."""
    slot_ctx = max(512, int(ctx or 0) // max(1, int(parallel or 1)))
    bridge = _TokenizerBridge(base_url, timeout)
    bridge.n_ctx = slot_ctx
    tools_cost = None
    if tools:
        tools_cost = int(bridge.count(
            json.dumps(tools, separators=(",", ":"))) * 1.15) + 128
    budget = _slot_budget(slot_ctx, max_tokens, prompt_cap)
    joined = "\n".join(_msg_texts(messages))
    joined_n = bridge.count(joined) if joined else 0
    if (joined_n + (tools_cost or 0)
            + EmbeddedEngine._PER_MSG_OVERHEAD * len(messages) <= budget):
        return messages, tools
    shim = types.SimpleNamespace(llama=bridge, name=name)
    return EmbeddedEngine._fit_messages(shim, messages, max_tokens, tools,
                                        prompt_cap, tools_cost=tools_cost)


def fit_prompt(base_url: str, prompt: str, max_tokens: int | None,
               ctx: int, parallel: int, name: str = "llama-server",
               timeout: float = 5.0) -> str:
    """Fit a raw /v1/completions prompt into the per-SLOT context."""
    slot_ctx = max(512, int(ctx or 0) // max(1, int(parallel or 1)))
    bridge = _TokenizerBridge(base_url, timeout)
    bridge.n_ctx = slot_ctx
    shim = types.SimpleNamespace(llama=bridge, name=name)
    budget = _slot_budget(slot_ctx, max_tokens, None)
    if EmbeddedEngine._count_tokens(shim, prompt) <= budget:
        return prompt
    return EmbeddedEngine._truncate_text(shim, prompt, budget)


# ── GPU placement → llama-server CLI flags ─────────────────────────────────

def _gpu_args(placement: str) -> list[str]:
    """'' / 'N' / 'auto' → -ngl/--main-gpu/--tensor-split flags.

    Always offload every layer (-ngl 99) — the embedded engine does the
    same; CPU-offload llama-server children make no sense behind a GPU
    gateway. 'auto' splits layers across cards weighted by FREE VRAM
    (same weighting as the embedded engine's placement)."""
    val = (placement or "").strip().lower()
    args = ["-ngl", "99"]
    if val in ("auto", "auto-split", "split"):
        try:
            gpus = detect_gpus()
        except Exception:
            gpus = []
        if len(gpus) >= 2:
            free = [max(g["total_mb"] - g["used_mb"], 1) for g in gpus]
            total = sum(free)
            args += ["-ts", ",".join(f"{f / total:.4f}" for f in free)]
    else:
        try:
            args += ["-mg", str(int(val))]
        except ValueError:
            pass
    return args


# ── One managed llama-server child ─────────────────────────────────────────

def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:80]


class _ServerProcess:
    """Spawn / health-poll / idle-stop ONE llama-server child.

    `sig` carries every knob that shapes the command line — config
    hot-reloads produce a different sig and the old child is replaced."""

    def __init__(self, backend_name: str, model_key: str, sig: tuple):
        (self.server_exe, self.host, self.port_base, self.extra_args,
         self.gguf_path, self.ctx, self.placement, self.parallel,
         self.alias, self.start_timeout_s, self.idle_timeout_s) = sig
        self.sig = sig
        self.backend_name = backend_name
        self.model_key = model_key
        self.port = self.port_base + (
            int(hashlib.md5(f"{backend_name}/{model_key}".encode())
                .hexdigest()[:8], 16) % 1000)
        self.log_path = (PROJECT_ROOT / "data" /
                         f"llamasrv-{_safe_name(backend_name)}-"
                         f"{_safe_name(model_key)}.log")
        self.lock = asyncio.Lock()
        self.proc: subprocess.Popen | None = None
        self._ready = False
        self._log_fh = None
        self.last_used = time.monotonic()
        self.started_s: float | None = None

    # ── state ──

    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def state(self) -> str:
        if not self.proc:
            return "stopped"
        if self.proc.poll() is not None:
            return "exited"
        return "running" if self._ready else "starting"

    def touch(self) -> None:
        self.last_used = time.monotonic()

    # ── lifecycle ──

    def _argv(self) -> list[str]:
        # NOTE: no -fa here — llama.cpp b8xxx changed --flash-attn to take
        # a VALUE (on|off|auto, default auto); a bare '-fa' makes modern
        # builds consume the NEXT flag as its value and exit rc=1. The
        # default (auto) enables FA where supported anyway.
        argv = [
            self.server_exe,
            "-m", self.gguf_path,
            "--host", self.host,
            "--port", str(self.port),
            "-c", str(self.ctx),
            "--alias", self.alias,
            "--no-webui", "--jinja",
            *_gpu_args(self.placement),
            *self.extra_args,
        ]
        if self.parallel > 1:
            argv += ["--parallel", str(self.parallel)]
        return argv

    def _spawn(self) -> None:
        if not Path(self.gguf_path).is_file():
            raise LlamaServerError(
                f"gguf for llama-server model {self.model_key!r} not found: "
                f"{self.gguf_path}")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_fh = open(self.log_path, "ab")
        logger.info("llama-server spawn [%s/%s]: %s (log: %s)",
                    self.backend_name, self.model_key,
                    " ".join(self._argv()), self.log_path)
        self.proc = Popen(  # noqa: S603 — argv list, no shell (tests patch this name)
            self._argv(), stdout=subprocess.DEVNULL, stderr=self._log_fh)
        self._ready = False
        self.started_s = time.monotonic()

    def stop(self) -> None:
        proc, self.proc = self.proc, None
        self._ready = False
        if proc is not None and proc.poll() is None:
            logger.info("llama-server stop [%s/%s] pid %s",
                        self.backend_name, self.model_key, proc.pid)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if self._log_fh is not None:
            self._log_fh.close()
            self._log_fh = None

    async def ensure_ready(self) -> str:
        """Spawn when needed and wait for /health. Returns base_url."""
        self.touch()
        async with self.lock:
            if self.is_running() and self._ready:
                return self.base_url()
            self.stop()  # clear any dead/half-started child
            self._spawn()
            deadline = time.monotonic() + self.start_timeout_s
            while time.monotonic() < deadline:
                if self.proc is not None and self.proc.poll() is not None:
                    raise LlamaServerError(
                        f"llama-server [{self.backend_name}/{self.model_key}] "
                        f"exited during startup (rc={self.proc.returncode}) "
                        f"— see {self.log_path}")
                if await get_health(self.base_url()):
                    self._ready = True
                    logger.info("llama-server ready [%s/%s] at %s "
                                "(load %.1fs)", self.backend_name,
                                self.model_key, self.base_url(),
                                time.monotonic() - self.started_s)
                    return self.base_url()
                await asyncio.sleep(0.5)
            raise LlamaServerError(
                f"llama-server [{self.backend_name}/{self.model_key}] not "
                f"healthy after {self.start_timeout_s:.0f}s — see "
                f"{self.log_path}")


# ── Registry + reaper ──────────────────────────────────────────────────────

_MANAGERS: dict[tuple[str, str], _ServerProcess] = {}
_REAPER_TASK: asyncio.Task | None = None


def get_manager(cfg: GatewayConfig, backend_name: str,
                model_key: str) -> _ServerProcess:
    """Manager for (backend, model) — config signature changes (hot
    reload) replace the child on next use."""
    b = cfg.backends[backend_name]
    m = b["models"][model_key]
    gguf_path = EmbeddedEngine.resolve_gguf(m["gguf"], cfg.models_dir)
    if not gguf_path:
        raise LlamaServerError(
            f"gguf for llama-server model {model_key!r} not found: "
            f"{m['gguf']!r} (looked in {cfg.models_dir})")
    sig = (b["server_exe"], b["host"], b["port_base"], tuple(b["extra_args"]),
           gguf_path, m["ctx"], m["placement"], m.get("parallel", 1),
           m.get("alias", model_key), b["start_timeout_s"], b["idle_timeout_s"])
    key = (backend_name, model_key)
    cur = _MANAGERS.get(key)
    if cur is not None and cur.sig != sig:
        logger.info("llama-server config for %s/%s changed — replacing child",
                    backend_name, model_key)
        cur.stop()
        cur = None
    if cur is None:
        cur = _ServerProcess(backend_name, model_key, sig)
        _MANAGERS[key] = cur
    return cur


def running_count() -> int:
    """Running (healthy or starting) children — feeds the admission gate:
    a batched server adds capacity beyond the embedded instances."""
    return sum(1 for m in _MANAGERS.values() if m.is_running())


def status() -> list[dict[str, Any]]:
    """Manager states for /healthz."""
    out: list[dict[str, Any]] = []
    for (b, k), m in _MANAGERS.items():
        running = m.is_running()
        out.append({
            "backend": b, "model": k, "alias": m.alias,
            "port": m.port, "state": m.state(),
            "pid": m.proc.pid if running else None,
            "parallel": m.parallel,
            "idle_s": (round(time.monotonic() - m.last_used, 1)
                       if running else None),
            "base_url": m.base_url() if running else None,
        })
    return out


def _reaper_once() -> int:
    """Stop idle children (idle_timeout_s > 0). Returns count stopped."""
    now = time.monotonic()
    n = 0
    for m in list(_MANAGERS.values()):
        if (m.idle_timeout_s > 0 and m.is_running()
                and now - m.last_used > m.idle_timeout_s):
            logger.info("llama-server idle %.0fs [%s/%s] — stopping",
                        now - m.last_used, m.backend_name, m.model_key)
            m.stop()
            n += 1
    return n


async def _reaper_loop() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            _reaper_once()
        except Exception:  # noqa: BLE001 — the reaper must never die
            logger.warning("llama-server reaper error", exc_info=True)


def ensure_reaper() -> None:
    """Start the idle reaper once per event loop (call from request
    handlers — a loop is guaranteed to exist there; tests create several)."""
    global _REAPER_TASK
    loop = asyncio.get_running_loop()
    if _REAPER_TASK is None or _REAPER_TASK.get_loop() is not loop:
        _REAPER_TASK = loop.create_task(_reaper_loop())


def kill_all() -> None:
    """Stop every child (gateway shutdown)."""
    for m in _MANAGERS.values():
        m.stop()


def reset_managers() -> None:
    """Test helper — drop the registry without stopping (fakes own cleanup)."""
    _MANAGERS.clear()
