"""Cloud backends — any OpenAI-compatible endpoint (Groq / OpenRouter /
OpenAI / RunPod pods / HF router).

Clients are AsyncOpenAI instances, cached per backend name and rebuilt when
the backend's (base_url, api_key_env) signature changes in the config —
hot-reload friendly. The api key is resolved at CLIENT CONSTRUCTION
(per the design doc: keys never live in the JSON): the env var named by
``api_key_env`` first, then the OS keyring ('Workshop'/KEY and
'workshop'/KEY — the same store the assistant's LLMClient uses), so a key
stored once in the keyring works with zero env setup. Changing the value
later still needs a config touch or restart.

The gateway forwards tools as-is (cloud models have native tool support),
unlike the embedded engine's JSON-convention shim.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from openai import (APIConnectionError, APITimeoutError, AsyncOpenAI)

logger = logging.getLogger(__name__)

# Cloud-side max_tokens cap (design decision — mirrors the assistant's
# LLMClient). Only caps when the client sent one; omitted stays omitted.
# Per-backend override: backend.max_tokens_cap (int = custom cap,
# null = UNCAPPED — reasoning models like gpt-oss spend the budget on
# reasoning_content and come back empty under a tight cap).
CLOUD_MAX_TOKENS_CAP = 800


class CloudBackendError(Exception):
    """Cloud backend unusable or its call failed — maps to HTTP 502."""


_CLIENTS: dict[str, tuple[str, AsyncOpenAI]] = {}


def _env_or_keyring(key_env: str) -> str:
    """api key resolution: env var → OS keyring. Never logged.

    The keyring service is the assistant's ('Workshop'; Windows Credential
    Manager targets are case-insensitive, other platforms are not — both
    casings are tried). keyring is an optional dependency; absence or any
    lookup failure just means "no key from here"."""
    if not key_env:
        return ""
    key = os.environ.get(key_env) or ""
    if key:
        return key
    try:
        import keyring  # optional — not a hard dependency
        for service in ("Workshop", "workshop"):
            key = keyring.get_password(service, key_env) or ""
            if key:
                return key
    except Exception:
        pass
    return ""


def resolve_backend_key(backend: dict[str, Any]) -> str:
    """Full key-resolution chain for a backend (tier B), in order:
    1. api_key_env environment variable (always wins)
    2. api_key_file — first line of the file (Docker/K8s mounted
       secrets, Vault Agent rendered files)
    3. api_key_cmd — first line of the command's stdout (vault CLI,
       aws secretsmanager get-secret-value, any fetcher; the config is
       operator-trusted, same trust level as base_url)
    4. OS keyring ('Workshop'/'workshop' casings)
    Never logs values. Empty string when nothing resolves."""
    key_env = str(backend.get("api_key_env") or "").strip()
    key = os.environ.get(key_env) if key_env else ""
    if key:
        return key
    path = str(backend.get("api_key_file") or "").strip()
    if path:
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                key = f.readline().strip()
            if key:
                return key
        except OSError:
            pass
    cmd = str(backend.get("api_key_cmd") or "").strip()
    if cmd:
        try:
            import subprocess
            out = subprocess.run(  # noqa: S603 — operator-configured command
                cmd, shell=True, capture_output=True, text=True, timeout=15)
            stdout = (out.stdout or "").strip()
            if stdout:
                return stdout.splitlines()[0].strip()
        except Exception:  # noqa: BLE001 — a failing fetcher = no key
            pass
    return _env_or_keyring(key_env) if key_env else ""


def get_cloud_client(name: str, backend: dict[str, Any],
                     timeout: float = 120.0) -> AsyncOpenAI:
    """Cached AsyncOpenAI client for a configured openai-type backend."""
    base_url = str(backend.get("base_url") or "").strip()
    key_env = str(backend.get("api_key_env") or "").strip()
    sig = f"{base_url}\x00{key_env}"
    cached = _CLIENTS.get(name)
    if cached is not None and cached[0] == sig:
        return cached[1]
    api_key = resolve_backend_key(backend)
    if key_env and not api_key:
        raise CloudBackendError(
            f"cloud backend {name!r}: api key env var {key_env!r} is not set "
            f"and the OS keyring has no '{key_env}' entry")
    try:
        client = AsyncOpenAI(base_url=base_url or None,
                             api_key=api_key or "not-set",
                             timeout=timeout)
    except Exception as e:  # bad base_url etc.
        raise CloudBackendError(
            f"cloud backend {name!r} client init failed: {e}") from e
    _CLIENTS[name] = (sig, client)
    logger.info("cloud client ready: %s -> %s", name, base_url or "(openai default)")
    return client


def reset_clients() -> None:
    """Drop cached clients (tests / forced re-read of env keys)."""
    _CLIENTS.clear()


def cap_max_tokens(backend: dict[str, Any], requested: int | None) -> int | None:
    """Apply the backend's max_tokens policy to a client-sent value.

    openai-type cloud backends cap at the backend's ``max_tokens_cap``
    (default CLOUD_MAX_TOKENS_CAP; explicitly null = UNCAPPED). All other
    backend types pass the client's value through (local llama-server;
    per-second-billed runpod)."""
    if not requested:
        return requested
    if str(backend.get("type") or "") != "openai":
        return requested
    cap = backend.get("max_tokens_cap", CLOUD_MAX_TOKENS_CAP)
    return min(requested, cap) if cap else requested


def classify_error(e: BaseException) -> str:
    """Bucket a backend failure: timeout | network | rate_limit | server |
    auth | client | unknown (ported from the assistant's LLMClient).

    Walks __cause__ chains — gateway call sites wrap the original backend
    exception in CloudBackendError(... from e). 'client' means the REQUEST
    was bad (4xx other than auth/rate-limit): retrying or breaker-skipping
    won't help, so callers (chain breaker) treat it as non-opening."""
    cur: BaseException | None = e
    for _ in range(5):
        if cur is None:
            break
        if isinstance(cur, APITimeoutError):
            return "timeout"
        if isinstance(cur, APIConnectionError):
            return "network"
        status = getattr(cur, "status_code", None)
        if status is None:
            status = getattr(getattr(cur, "response", None), "status_code", None)
        if isinstance(status, int):
            if status == 429:
                return "rate_limit"
            if 500 <= status < 600:
                return "server"
            if status in (401, 403):
                return "auth"
            if 400 <= status < 500:
                return "client"
        cur = cur.__cause__
    return "unknown"
