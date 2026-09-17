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
import logging
import re
import subprocess
import time
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
