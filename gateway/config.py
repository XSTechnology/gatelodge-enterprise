"""Gateway configuration — data/gateway_config.json (hot-reloadable).

Fully standalone: this project has no relationship with the assistant
project — every knob lives in this file (or the built-in defaults).

Built-in defaults are DELIBERATELY model-free: no models or aliases are
predefined. Every GGUF in ``models_dir`` is auto-discovered and servable
by its file stem (e.g. ``Qwen3.8-27B-Uncensored-Q4_K_M``) with default
ctx 4096 and default GPU placement — multiple models load lazily and run
concurrently, as many as VRAM allows. Config entries override/extend:
friendly names, custom ctx/placement, aliases, cloud backends.

Full schema (Stage 3 adds cloud execution; the parser accepts it from day
one so configs don't churn between stages):

{
  "port": 8010,
  "bind": "127.0.0.1",
  "models_dir": "data/models",        # where GGUFs live (relative = project root)
  "gateway_keys": [],                 # bearer tokens; [] = open (localhost trust; Stage 4)
  "default_alias": "",                # model= defaults to this when omitted
  "backends": {
    "local": {"type": "embedded", "models": {
        "big":   {"gguf": "Qwen3.8-27B-Uncensored-Q4_K_M", "ctx": 8192, "placement": "auto"},
        "small": {"gguf": "qwen2.5-0.5b-instruct-q4_k_m", "ctx": 4096, "placement": ""}}},
    "groq":  {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
               "api_key_env": "GROQ_API_KEY",
               "models": {"cloud-fast": "llama-3.1-8b-instant"}}
  },
  "aliases": {
    "smart": {"chain": ["local:big", "groq:cloud-fast"], "hedge_s": 8},
    "private": {"chain": ["local:big"], "local_only": true}
  },
  "queue": {"max_waiting": 4, "timeout_s": 120}
}

Semantics:
- ``gguf`` is a GGUF stem or path, resolved against ``models_dir``.
- ``placement`` per model: '' = llama.cpp default, 'N' = pin to card N,
  'auto' = free-VRAM-weighted split across all cards.
- Alias chains list 'backend:model' entries; Stage 1 executes the FIRST
  entry only (hedge racing arrives in Stage 3). ``local_only`` aliases
  must never leave the machine (privacy guarantee once cloud exists).
- Cloud requests get max_tokens capped at 800.
- Top-level keys present in the file replace the built-in defaults
  wholesale (defining "backends" overrides the default backends).
- Secrets never live in the JSON — cloud backends name an env var
  (``api_key_env``) resolved at client-construction time (Stage 3).
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "data" / "gateway_config.json"  # CWD-independent
DEFAULT_MODELS_DIR = PROJECT_ROOT / "data" / "models"

EMBEDDED_BACKEND = "embedded"
OPENAI_BACKEND = "openai"
_BACKEND_TYPES = (EMBEDDED_BACKEND, OPENAI_BACKEND)

# Out-of-the-box config: deliberately model-free — no baked-in models or
# aliases. GGUFs in models_dir are auto-discovered (servable by stem);
# the operator adds named models / aliases / cloud backends in the config.
DEFAULT_CONFIG: dict[str, Any] = {
    "port": 8010,
    "bind": "127.0.0.1",
    "models_dir": str(DEFAULT_MODELS_DIR),
    "gateway_keys": [],
    "default_alias": "",
    "backends": {"local": {"type": "embedded", "models": {}}},
    "aliases": {},
    "queue": {"max_waiting": 4, "timeout_s": 120},
}


class GatewayConfigError(ValueError):
    """Unknown model / bad alias chain — maps to HTTP 400."""


def _default_raw() -> dict[str, Any]:
    """Deep copy of the built-in defaults (safe to update in place)."""
    return json.loads(json.dumps(DEFAULT_CONFIG))


def default_config_raw() -> dict[str, Any]:
    """Public deep copy of the built-in defaults (admin config mutations)."""
    return _default_raw()


def _resolve_dir(raw: Any) -> str:
    """models_dir value → absolute path (relative anchors at project root)."""
    p = Path(str(raw or "").strip() or DEFAULT_MODELS_DIR)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return str(p)


def _normalize_backends(raw: Any) -> dict[str, dict[str, Any]]:
    """Validate + normalize backends; drop malformed entries with a warning."""
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for name, b in raw.items():
        if not isinstance(b, dict):
            logger.warning("gateway config: backend %r is not an object — dropped", name)
            continue
        btype = str(b.get("type") or "").strip()
        if btype not in _BACKEND_TYPES:
            logger.warning("gateway config: backend %r has unknown type %r — dropped",
                           name, btype)
            continue
        entry: dict[str, Any] = {"type": btype}
        if btype == EMBEDDED_BACKEND:
            models: dict[str, dict[str, Any]] = {}
            for mname, m in (b.get("models") or {}).items():
                if isinstance(m, str):
                    m = {"gguf": m}
                if not isinstance(m, dict) or not (m.get("gguf") or "").strip():
                    logger.warning("gateway config: local model %r has no gguf — dropped", mname)
                    continue
                try:
                    ctx = int(m.get("ctx") or 4096)
                except (TypeError, ValueError):
                    ctx = 4096
                models[str(mname)] = {
                    "gguf": str(m["gguf"]).strip(),
                    "ctx": ctx,
                    "placement": str(m.get("placement") or "").strip(),
                }
            entry["models"] = models
        else:  # openai-type cloud backend (executed in Stage 3)
            entry["base_url"] = str(b.get("base_url") or "").strip()
            entry["api_key_env"] = str(b.get("api_key_env") or "").strip()
            cmodels: dict[str, str] = {}
            for mname, m in (b.get("models") or {}).items():
                if isinstance(m, dict):
                    m = m.get("id") or m.get("model") or ""
                if isinstance(m, str) and m.strip():
                    cmodels[str(mname)] = m.strip()
            entry["models"] = cmodels
        out[str(name)] = entry
    return out


def _normalize_aliases(raw: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for name, a in raw.items():
        if isinstance(a, list):
            a = {"chain": a}
        if not isinstance(a, dict):
            logger.warning("gateway config: alias %r is not an object — dropped", name)
            continue
        chain = [str(c).strip() for c in (a.get("chain") or []) if str(c).strip()]
        alias: dict[str, Any] = {"chain": chain}
        if a.get("hedge_s") is not None:
            try:
                alias["hedge_s"] = float(a["hedge_s"])
            except (TypeError, ValueError):
                pass
        if a.get("local_only"):
            alias["local_only"] = True
        out[str(name)] = alias
    return out


class GatewayConfig:
    """Parsed gateway_config.json; reloads when the file's mtime changes.

    A broken/unreadable file keeps the last good values (defaults on first
    load) — the gateway keeps serving instead of resetting to defaults.
    """

    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path is not None else CONFIG_PATH
        self._lock = threading.Lock()
        self._mtime: float | None = None
        self._apply(_default_raw())  # seed defaults before the first load
        self._load()

    # ── loading ──

    def _remember_mtime(self) -> None:
        try:
            self._mtime = self.path.stat().st_mtime if self.path.is_file() else None
        except OSError:
            pass

    def _load(self) -> None:
        if not self.path.is_file():
            self._apply(_default_raw())
            self._remember_mtime()
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.error("gateway config %s unreadable (%s) — previous values kept",
                         self.path, e)
            self._remember_mtime()
            return
        if not isinstance(data, dict):
            logger.error("gateway config %s: not a JSON object — previous values kept",
                         self.path)
            self._remember_mtime()
            return
        # Top-level keys in the file replace defaults wholesale.
        raw = _default_raw()
        raw.update(data)
        self._apply(raw)
        self._remember_mtime()

    def _apply(self, raw: dict[str, Any]) -> None:
        self.port = int(raw.get("port") or 8010)
        self.bind = str(raw.get("bind") or "127.0.0.1")
        self.models_dir = _resolve_dir(raw.get("models_dir"))
        self.gateway_keys = [str(k) for k in (raw.get("gateway_keys") or [])]
        self.default_alias = str(raw.get("default_alias") or "").strip()
        self.backends = _normalize_backends(raw.get("backends"))
        self.aliases = _normalize_aliases(raw.get("aliases"))
        queue = raw.get("queue") if isinstance(raw.get("queue"), dict) else {}
        try:
            max_waiting = int(queue["max_waiting"])  # 0 is valid — no 'or' default
        except (KeyError, TypeError, ValueError):
            max_waiting = 4
        try:
            timeout_s = float(queue["timeout_s"])
        except (KeyError, TypeError, ValueError):
            timeout_s = 120.0
        self.queue = {"max_waiting": max_waiting, "timeout_s": timeout_s}

    def maybe_reload(self) -> bool:
        """Re-read the file when its mtime changed. True when reloaded."""
        try:
            mtime = self.path.stat().st_mtime if self.path.is_file() else None
        except OSError:
            return False
        if mtime == self._mtime:
            return False
        with self._lock:
            if mtime == self._mtime:  # re-check under the lock
                return False
            self._load()
        return True

    def reload_now(self) -> None:
        """Unconditional reload (admin mutations want determinism, not
        mtime-granularity luck)."""
        with self._lock:
            self._load()

    # ── accessors ──

    def local_backend(self) -> dict[str, Any]:
        """The (first) embedded backend's config; {} when none configured."""
        for b in self.backends.values():
            if b.get("type") == EMBEDDED_BACKEND:
                return b
        return {}

    def local_models(self) -> dict[str, dict[str, Any]]:
        return dict(self.local_backend().get("models") or {})

    def local_backend_name(self) -> str:
        for name, b in self.backends.items():
            if b.get("type") == EMBEDDED_BACKEND:
                return name
        return ""

    # ── resolution ──

    def resolve_chain(self, name: str | None) -> dict[str, Any]:
        """Request ``model`` → execution plan (Stage 3).

        {"entries": [(backend_name, model_key), ...],  # chain order
         "hedge_s": float,                              # 0 = no hedge race
         "local_only": bool}                            # privacy flag

        Order: alias (full chain; hedge_s + local_only from the alias),
        then the direct 'backend:model' form, then a bare backend model
        name (embedded backend preferred) — both single-entry, no hedge.
        ``local_only`` plans keep ONLY embedded entries (they must never
        leave the machine, whatever the chain says).

        Raises GatewayConfigError for anything unknown (→ HTTP 400).
        """
        name = (name or "").strip()
        if not name:
            if not self.default_alias:
                raise GatewayConfigError("no model specified and no default_alias configured")
            name = self.default_alias
        alias = self.aliases.get(name)
        if alias is not None:
            chain = alias.get("chain") or []
            if not chain:
                raise GatewayConfigError(f"alias {name!r} has an empty chain")
            entries = [self._resolve_entry(c, alias=name) for c in chain]
            plan = {"entries": entries,
                    "hedge_s": float(alias.get("hedge_s") or 0),
                    "local_only": bool(alias.get("local_only"))}
        elif ":" in name:
            plan = {"entries": [self._resolve_entry(name)],
                    "hedge_s": 0.0, "local_only": False}
        else:
            local = self.local_backend_name()
            if local and name in (self.backends.get(local) or {}).get("models", {}):
                entries = [(local, name)]
            else:
                found = next(((bname, name) for bname, b in self.backends.items()
                              if name in (b.get("models") or {})), None)
                if found is None:
                    raise GatewayConfigError(
                        f"unknown model: {name!r} (not an alias or a backend model)")
                entries = [found]
            plan = {"entries": entries, "hedge_s": 0.0, "local_only": False}
        if plan["local_only"]:
            plan["entries"] = [
                (b, m) for b, m in plan["entries"]
                if (self.backends.get(b) or {}).get("type") == EMBEDDED_BACKEND]
            if not plan["entries"]:
                raise GatewayConfigError(
                    f"alias {name!r} is local_only but its chain has no "
                    "local backend entries")
        return plan

    def resolve_model(self, name: str | None) -> tuple[str, str]:
        """Primary chain entry of resolve_chain (Stage-1 compatibility)."""
        return self.resolve_chain(name)["entries"][0]

    def _resolve_entry(self, entry: str, alias: str | None = None) -> tuple[str, str]:
        backend_name, sep, model_key = entry.partition(":")
        what = f"alias {alias!r}" if alias else "model"
        if not sep or not backend_name or not model_key:
            raise GatewayConfigError(
                f"{what}: bad chain entry {entry!r} — expected 'backend:model'")
        backend = self.backends.get(backend_name)
        if backend is None:
            raise GatewayConfigError(f"{what}: unknown backend {backend_name!r}")
        if model_key not in (backend.get("models") or {}):
            raise GatewayConfigError(
                f"{what}: backend {backend_name!r} has no model {model_key!r}")
        return backend_name, model_key


# ── Process-wide singleton ─────────────────────────────────────────────────

_CONFIG: GatewayConfig | None = None


def get_gateway_config() -> GatewayConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = GatewayConfig()
    return _CONFIG
