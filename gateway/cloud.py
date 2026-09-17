"""Cloud backends — any OpenAI-compatible endpoint (Groq / OpenRouter /
OpenAI / RunPod pods).

Clients are AsyncOpenAI instances, cached per backend name and rebuilt when
the backend's (base_url, api_key_env) signature changes in the config —
hot-reload friendly. The api key is read from the env var named by
``api_key_env`` AT CLIENT CONSTRUCTION (per the design doc: keys never
live in the JSON; changing the env value needs a config touch or restart).

The gateway forwards tools as-is (cloud models have native tool support),
unlike the embedded engine's JSON-convention shim.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

# Cloud-side max_tokens cap (design decision — mirrors the assistant's
# LLMClient). Only caps when the client sent one; omitted stays omitted.
CLOUD_MAX_TOKENS_CAP = 800


class CloudBackendError(Exception):
    """Cloud backend unusable or its call failed — maps to HTTP 502."""


_CLIENTS: dict[str, tuple[str, AsyncOpenAI]] = {}


def get_cloud_client(name: str, backend: dict[str, Any],
                     timeout: float = 120.0) -> AsyncOpenAI:
    """Cached AsyncOpenAI client for a configured openai-type backend."""
    base_url = str(backend.get("base_url") or "").strip()
    key_env = str(backend.get("api_key_env") or "").strip()
    sig = f"{base_url}\x00{key_env}"
    cached = _CLIENTS.get(name)
    if cached is not None and cached[0] == sig:
        return cached[1]
    api_key = os.environ.get(key_env) if key_env else ""
    if key_env and not api_key:
        raise CloudBackendError(
            f"cloud backend {name!r}: api key env var {key_env!r} is not set")
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
