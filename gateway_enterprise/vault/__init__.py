"""Content Vault — the enterprise capture backend (spec: 
gatelodge specs/content-vault-spec.md).

Installed into the core's traffic seam (gateway/traffic.py) when the
license carries the 'vault' claim AND the config's vault section is
enabled. Default OFF; the standard build never captures content.

The backend implements the CaptureBackend protocol:

  on_request(meta, body)     → a per-request collector {req, resp[]}
  on_response(token, text)   → append the fragment
  on_flush(token, meta)      → mode filter + sampling, then enqueue to
                               the bounded writer queue (the ONLY
                               request-path work; crypto and disk I/O
                               happen on the writer task)

Everything else — keys, multi-wrap escrow, chain, parts, retention,
canaries, admin endpoints — lives in keys/writer/index/tools.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from gateway import traffic

from . import keys as vkeys
from .tools import build_router
from .writer import VaultWriter

logger = logging.getLogger("gateway.enterprise.vault")


class VaultCapture:
    """The CaptureBackend the enterprise build installs."""

    def __init__(self, writer: VaultWriter, mode: str = "all",
                 sample_n: int = 1):
        self.writer = writer
        self.mode = mode              # all | prompts | responses
        self.sample_n = max(1, int(sample_n))
        self._sample_i = 0

    # ── the seam protocol ──────────────────────────────────────────

    def on_request(self, meta: dict, body: Any) -> dict | None:
        if self.mode == "sample" and self.sample_n > 1:
            self._sample_i += 1
            if (self._sample_i - 1) % self.sample_n:
                return None            # not this request's turn
        try:
            req = json.dumps(
                body.model_dump(exclude_none=True), default=str)
        except Exception:              # noqa: BLE001 — not a model? best effort
            req = json.dumps(body, default=str)
        return {"req": req, "resp": []}

    def on_response(self, token: dict, text: str) -> None:
        token["resp"].append(text)

    def on_flush(self, token: dict, meta: dict) -> None:
        self.writer.enqueue(token["req"], "".join(token["resp"]), meta)


# ── wiring (enterprise_main: _wire_vault) ──────────────────────────────────

_WRITER: VaultWriter | None = None
_CAPTURE: VaultCapture | None = None


def get_writer() -> VaultWriter | None:
    return _WRITER


def wire(enabled: bool) -> dict:
    """Install the vault when licensed + configured. Returns a status
    dict for the boot log. vault.enabled without a backend fails the
    CORE boot gate (traffic.assert_configured) — this function either
    installs a working vault or leaves nothing installed."""
    global _WRITER, _CAPTURE
    if not enabled:
        logger.info("vault NOT licensed — capture seam stays no-op")
        return {"installed": False, "reason": "not licensed"}

    from gateway.config import get_gateway_config
    cfg = get_gateway_config()
    raw = cfg.store.load()
    vault = raw.get("vault") if isinstance(raw, dict) else {}
    if not isinstance(vault, dict) or not vault.get("enabled"):
        logger.info("vault licensed but disabled in config — no-op")
        return {"installed": False, "reason": "disabled in config"}

    # key registry — the wrap set (primary + escrow), fail fast on bad
    registry = vkeys.normalize_registry(vault.get("keys") or [])
    mode = str(vault.get("mode") or "all")
    if mode.startswith("sample:"):
        sample_n = int(mode.split(":", 1)[1] or 1)
        mode = "sample"
    else:
        sample_n = 1
    if mode not in ("all", "prompts", "responses", "sample"):
        raise RuntimeError(f"vault.mode {mode!r} is not one of "
                           "all|prompts|responses|sample:1/N")

    vault_id = str(vault.get("vault_id") or "default")
    data_dir = Path(vault.get("dir") or
                    (Path("data") / "vault"))
    data_dir.mkdir(parents=True, exist_ok=True)

    # Key S: from the keyfile (existing vault), or generated + escrowed
    keyfile_path = data_dir / "vault.keys"
    if keyfile_path.exists():
        kf = vkeys.read_keyfile(keyfile_path)
        search_key = None
        # Key S itself is encrypted to the wrap set — recovered OFFLINE
        # by the customer. The RUNNING gateway needs the raw Key S to
        # index… so it must also come from config or a side file.
    else:
        search_key = vkeys.new_search_key()
        vkeys.write_keyfile(keyfile_path, vault_id, registry, search_key)

    if search_key is None:
        # restart path: Key S from the search_key_hex in config or the
        # side file vault.search-key (0600) — written once at init
        sk_hex = vault.get("search_key_hex") or ""
        sk_file = data_dir / "vault.search-key"
        if not sk_hex and sk_file.exists():
            sk_hex = sk_file.read_text(encoding="ascii").strip()
        if not sk_hex:
            raise RuntimeError(
                "vault restart needs the search key: set vault."
                "search_key_hex in config (it is in the keyfile's "
                "escrow — recover offline with any wrap-set private "
                "key) or keep vault.search-key beside the parts")
        search_key = bytes.fromhex(sk_hex)
    else:
        (data_dir / "vault.search-key").write_text(
            search_key.hex() + "\n", encoding="ascii")
        try:
            os.chmod(data_dir / "vault.search-key", 0o600)
        except OSError:
            pass                       # Windows — NTFS ACLs instead

    _WRITER = VaultWriter(
        vault_dir=data_dir, vault_id=vault_id, registry=registry,
        search_key=search_key, mode=mode,
        keep_days=int(vault.get("keep_days") or 0),
        max_gb=float(vault.get("max_gb") or 0),
        queue_cap=int(vault.get("queue_cap") or 10000))
    _CAPTURE = VaultCapture(_WRITER, mode=mode, sample_n=sample_n)
    traffic.install(_CAPTURE)

    # admin surface (/admin/vault) — self-authed, admin-gate mirrored
    from gateway.app import add_extra_router, app as gw_app
    add_extra_router(build_router(get_writer), prefix="/admin/vault")

    # writer lifecycle rides the app's events (the task needs the loop)
    gw_app.router.add_event_handler("startup", _start_writer)
    gw_app.router.add_event_handler("shutdown", _stop_writer)

    # canaries: one per key version, immediately — keycheck material
    for e in registry:
        _WRITER.canary(e["v"])

    logger.info("vault ACTIVE (mode=%s, keys=%s, dir=%s)", mode,
                [e["v"] for e in registry], data_dir)
    return {"installed": True, "mode": mode,
            "keys": [e["v"] for e in registry], "dir": str(data_dir)}


async def _start_writer() -> None:
    if _WRITER is not None:
        _WRITER.start()


async def _stop_writer() -> None:
    if _WRITER is not None:
        try:
            await _WRITER.stop()
        except Exception:              # noqa: BLE001 — shutdown best effort
            pass
