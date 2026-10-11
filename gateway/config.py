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
               "models": {"cloud-fast": "llama-3.1-8b-instant"}},
    "srv":   {"type": "llama-server", "server_exe": "llama-server", "models": {
        "batch": {"gguf": "qwen2.5-0.5b-instruct-q4_k_m", "ctx": 16384,
                   "placement": "", "parallel": 4}}},
    "runpod": {"type": "llama-server", "base_url": "https://api.runpod.ai/v2/ENDPOINT/openai/v1",
               "api_key_env": "RUNPOD_API_KEY", "models": {"gpu-big": "model-id"}},
    "rp":    {"type": "runpod", "api_key_env": "RUNPOD_API_KEY", "models": {
        "fast": {"endpoint_id": "ep-abc", "remote_id": "model-name"},
        "big":  {"gpu_type_ids": ["NVIDIA RTX A6000"], "spot": true,
                  "image": "ghcr.io/ggml-org/llama.cpp:server-cuda",
                  "docker_args": "-m /workspace/model.gguf --host 0.0.0.0 --port 8080 -ngl 99 --jinja",
                  "ports": "8080/http", "port": 8080, "start_timeout_s": 600,
                  "idle_stop_s": 0, "idle_terminate_s": 0,
                  "env": {"HF_URL": "https://huggingface.co/.../model.gguf"},
                  "deploy": {}}}}
  },
  "aliases": {
    "smart": {"chain": ["local:big", "groq:cloud-fast"], "hedge_s": 8},
    "private": {"chain": ["local:big"], "local_only": true}
  },
  "queue": {"max_waiting": 4, "timeout_s": 120},
  "response_cache": {"enabled": true, "ttl_s": 3600, "max": 100}
}

Semantics:
- ``gguf`` is a GGUF stem or path, resolved against ``models_dir``.
- ``placement`` per model: '' = llama.cpp default, 'N' = pin to card N,
  'auto' = free-VRAM-weighted split across all cards.
- ``workers`` per embedded model (default 1): k lazily-created in-process
  instances behind a free-list — k concurrent requests to that model run
  in PARALLEL instead of serializing on one instance's lock. VRAM cost is
  k × the model's footprint; size pools for small hot models.
- Alias chains list 'backend:model' entries; Stage 1 executes the FIRST
  entry only (hedge racing arrives in Stage 3). ``local_only`` aliases
  must never leave the machine (privacy guarantee once cloud exists).
- Cloud requests get max_tokens capped at 800.
- ``embedding_models`` (embedded backends, same {gguf, ctx, placement}
  shape) are served by POST /v1/embeddings as SEPARATE instances —
  Llama(embedding=True) builds a pooling context; chat GGUFs cannot
  embed and embedding GGUFs are never auto-discovered.
- ``llama-server`` backends: SUBPROCESS mode (no base_url) spawns one
  llama-server child per model — continuous batching / slots (parallel
  concurrent requests on ONE loaded model, unlike the embedded engine's
  serialized lock); ctx is the TOTAL context divided among the slots;
  server_exe/host/port_base/start_timeout_s/idle_timeout_s (0=never
  idle-kill)/extra_args per backend; per-model parallel/alias knobs.
  REMOTE mode (base_url set — RunPod or any llama-server over HTTP) is
  a pure OpenAI passthrough: models map name → remote model id, optional
  api_key_env. local_only counts subprocess mode as local, remote as not.
- ``runpod`` backends (api_key_env, default RUNPOD_API_KEY): SERVERLESS
  models ({endpoint_id, remote_id?}) passthrough at
  https://api.runpod.ai/v2/<endpoint_id>/openai/v1 (Bearer). POD models
  ({gpu_type_ids, image, docker_args, ports, port, gpu_count,
  container_disk_gb, volume_gb, volume_mount, spot, env, alias,
  start_timeout_s, idle_stop_s, idle_terminate_s, deploy}) are
  lifecycle-managed: deployed on first request (find-by-name → resume a
  stopped pod → deploy new), health-polled via the pod's
  <pod-id>-<port>.proxy.runpod.net URL, proxied in OpenAI wire format,
  and idle-stopped (keeps storage) then idle-terminated per the two
  knobs (0 = never). ``deploy`` is a raw dict merged verbatim into the
  POST /v2/pods payload (escape hatch for REST fields the gateway
  doesn't model). Pods are NOT touched on gateway shutdown.
- ``runpod-public`` backends (api_key_env, default RUNPOD_API_KEY):
  RunPod Public Endpoints — pre-deployed, always-warm models accessed
  by slug (e.g. 'black-forest-labs-flux-1-schnell'). Models use
  {model_slug, remote_id?, timeout_s?, policy?}. Runs via runsync
  protocol (collect-then-emit for streaming). No lifecycle management
  needed — endpoints are managed by RunPod.
- Top-level keys present in the file replace the built-in defaults
  wholesale (defining "backends" overrides the default backends).
- Secrets never live in the JSON — cloud backends name an env var
  (``api_key_env``) resolved at client-construction time (Stage 3).
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Compiled binaries (Nuitka, v1.5 closed distribution): anchor the data
# tree to the EXECUTABLE's directory. In a onefile build __file__ points
# into the per-boot extraction temp dir — data/ (config, license, trial
# anchor, logs, models) would be orphaned between boots. Standalone
# builds land in the same place either way; source runs are unchanged.
if "__compiled__" in globals():
    PROJECT_ROOT = Path(sys.executable).resolve().parent
else:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "data" / "gateway_config.json"  # CWD-independent
DEFAULT_MODELS_DIR = PROJECT_ROOT / "data" / "models"

EMBEDDED_BACKEND = "embedded"
OPENAI_BACKEND = "openai"
LLAMA_SERVER_BACKEND = "llama-server"
RUNPOD_BACKEND = "runpod"
RUNPOD_PUBLIC_BACKEND = "runpod-public"
_BACKEND_TYPES = (EMBEDDED_BACKEND, OPENAI_BACKEND, LLAMA_SERVER_BACKEND,
                  RUNPOD_BACKEND, RUNPOD_PUBLIC_BACKEND)

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
    # Response cache (ported from the assistant's LLMClient): repeated
    # SIMPLE prompts (no tools, single user message, nothing
    # time-sensitive) answer from memory instead of re-billing cloud
    # backends per token. local_only aliases never cache (privacy).
    "response_cache": {"enabled": True, "ttl_s": 3600, "max": 100},
    # Virtual (per-consumer) API keys: each program gets its own bearer,
    # usage is attributed per key in the log/stats, and per-key monthly
    # budgets (tokens / USD) enforce spend. gateway_keys stay the ADMIN
    # keys — /admin/* requires one whenever ANY keys are configured.
    "api_keys": [],
    # Cached provider pricing ($ per MILLION tokens) captured from
    # backend catalogs (the HF router returns per-provider rates) —
    # powers per-request cost estimation and per-key USD budgets.
    "model_prices": {},
    # TLS (tier A): serve https instead of http. bind beyond 127.0.0.1
    # only makes sense with TLS enabled (or with a reverse proxy that
    # terminates TLS in front). certfile/keyfile are PEM paths.
    "tls": {"enabled": False, "certfile": "", "keyfile": ""},
    # Log retention (tier B): delete gateway-log-YYYY-MM.jsonl and
    # gateway-rollup-YYYY-MM.json files older than N months at startup
    # and at month rollover. 0 = keep forever. NOTE: the gateway never
    # logs prompt/response CONTENT — only metadata (model, backend, ms,
    # tokens, cost, status, key, request id) — by design.
    "log_retention_months": 0,
    # Audit shipping (tier B): POST every config-audit entry to this
    # webhook (SIEM/collector) in addition to config-audit.jsonl.
    "audit": {"webhook_url": ""},
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


def _normalize_local_models(raw: Any, what: str = "local model") -> dict[str, dict[str, Any]]:
    """Validate + normalize embedded model entries ({gguf, ctx, placement}).
    Unknown extra keys are preserved (llama-server models add parallel/alias)."""
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for mname, m in raw.items():
        if isinstance(m, str):
            m = {"gguf": m}
        if not isinstance(m, dict) or not (m.get("gguf") or "").strip():
            logger.warning("gateway config: %s %r has no gguf — dropped", what, mname)
            continue
        try:
            ctx = int(m.get("ctx") or 4096)
        except (TypeError, ValueError):
            ctx = 4096
        entry = dict(m)  # keep extras (parallel, alias, ...)
        entry.update({
            "gguf": str(m["gguf"]).strip(),
            "ctx": ctx,
            "placement": str(m.get("placement") or "").strip(),
        })
        out[str(mname)] = entry
    return out


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
            models = _normalize_local_models(b.get("models"))
            for m in models.values():
                try:
                    # worker pool size (k in-process instances for parallel
                    # serving; VRAM cost is k × model size)
                    m["workers"] = max(1, int(m.get("workers") or 1))
                except (TypeError, ValueError):
                    m["workers"] = 1
            entry["models"] = models
            # Embedding GGUFs are SEPARATE instances (Llama(embedding=True) —
            # pooling context); never auto-discovered (a chat GGUF mistaken
            # for an embedder would return garbage vectors).
            entry["embedding_models"] = _normalize_local_models(
                b.get("embedding_models"), what="local embedding model")
        elif btype == LLAMA_SERVER_BACKEND:
            base_url = str(b.get("base_url") or "").strip().rstrip("/")
            entry["base_url"] = base_url
            entry["api_key_env"] = str(b.get("api_key_env") or "").strip()
            if base_url:
                # REMOTE mode (RunPod / any llama-server over HTTP): models
                # map our name → the server's model id, cloud-style.
                # ("name" accepted for parity with the console's model form.)
                rmodels: dict[str, str] = {}
                for mname, m in (b.get("models") or {}).items():
                    if isinstance(m, dict):
                        m = m.get("id") or m.get("model") or m.get("name") or ""
                    if isinstance(m, str) and m.strip():
                        rmodels[str(mname)] = m.strip()
                entry["models"] = rmodels
            else:
                # SUBPROCESS mode: the gateway spawns and manages one
                # llama-server child per model (continuous batching /
                # slots — unlike the embedded engine's per-instance lock).
                entry["server_exe"] = str(b.get("server_exe") or "llama-server")
                entry["host"] = str(b.get("host") or "127.0.0.1")
                try:
                    entry["port_base"] = int(b.get("port_base") or 8120)
                except (TypeError, ValueError):
                    entry["port_base"] = 8120
                try:
                    entry["start_timeout_s"] = float(b.get("start_timeout_s") or 180)
                except (TypeError, ValueError):
                    entry["start_timeout_s"] = 180.0
                try:
                    entry["idle_timeout_s"] = float(b.get("idle_timeout_s") or 0)
                except (TypeError, ValueError):
                    entry["idle_timeout_s"] = 0.0
                entry["extra_args"] = [str(a) for a in (b.get("extra_args") or [])]
                lsmodels = _normalize_local_models(
                    b.get("models"), what="llama-server model")
                for mname, m in lsmodels.items():
                    try:
                        m["parallel"] = max(1, int(m.get("parallel") or 1))
                    except (TypeError, ValueError):
                        m["parallel"] = 1
                    m["alias"] = str(m.get("alias") or mname).strip() or mname
                entry["models"] = lsmodels
        elif btype == RUNPOD_BACKEND:
            # Two model shapes: SERVERLESS ({endpoint_id, remote_id?} →
            # OpenAI passthrough at api.runpod.ai) and POD ({gpu_type_ids,
            # image, docker_args, ...} → gateway-managed lifecycle: deploy
            # on demand, proxy via the pod's runpod.net URL, idle
            # stop/terminate). A raw `deploy` dict passes through verbatim.
            entry["api_key_env"] = str(b.get("api_key_env") or "RUNPOD_API_KEY")
            rmodels: dict[str, dict[str, Any]] = {}
            for mname, m in (b.get("models") or {}).items():
                if not isinstance(m, dict):
                    logger.warning("gateway config: runpod model %r is not "
                                   "an object — dropped", mname)
                    continue
                m = dict(m)
                if m.get("endpoint_id"):
                    # serverless — endpoint_id + remote_id + transport:
                    # "openai" (the /openai/v1 shim, default) or "runsync"
                    # (raw job protocol — Ollama-style workers whose OpenAI
                    # shim is absent/broken). timeout_s (0 = queue default)
                    # covers scale-to-zero cold starts, which routinely
                    # exceed the global queue timeout.
                    transport = str(m.get("transport") or "openai").strip().lower()
                    try:
                        tmo = float(m.get("timeout_s") or 0)
                    except (TypeError, ValueError):
                        tmo = 0.0
                    rmodels[str(mname)] = {
                        "endpoint_id": str(m["endpoint_id"]).strip(),
                        "remote_id": str(m.get("remote_id") or mname).strip(),
                        "transport": transport if transport in ("openai", "runsync") else "openai",
                        "timeout_s": tmo,
                        # docs: policy {executionTimeout, ttl, lowPriority}
                        # merged into every job submission for this model
                        "policy": dict(m["policy"]) if isinstance(m.get("policy"), dict) else {},
                        # v4.5: deploy provenance (console serverless panel)
                        "serverless": dict(m["serverless"]) if isinstance(m.get("serverless"), dict) else {},
                    }
                else:
                    # pod lifecycle — numbers coerced, deploy dict kept raw
                    def _num(key: str, default: float) -> float:
                        try:
                            return float(m.get(key) or default)
                        except (TypeError, ValueError):
                            return default
                    pod: dict[str, Any] = {
                        "gpu_type_ids": [str(g).strip() for g in
                                         (m.get("gpu_type_ids") or []) if str(g).strip()],
                        "image": str(m.get("image") or "").strip(),
                        "docker_args": str(m.get("docker_args") or "").strip(),
                        # FULL command incl. binary (overrides docker_args
                        # prefixing) + optional entrypoint passthrough
                        "docker_cmd": str(m.get("docker_cmd") or "").strip(),
                        # shell SCRIPT, verbatim (one argv element) — for
                        # docker_entrypoint like ["sh","-c"]; shlex-split
                        # docker_cmd would shred it (live-verified dash
                        # syntax error in the container)
                        "docker_script": str(m.get("docker_script") or "").strip(),
                        "docker_entrypoint": [str(a) for a in
                                              (m.get("docker_entrypoint") or [])
                                              if str(a).strip()],
                        # REST API wants an ARRAY ("8080/http" → ["8080/http"])
                        "ports": [p.strip() for p in
                                  str(m.get("ports") or "8080/http").split(",")
                                  if p.strip()] if not isinstance(m.get("ports"), list)
                        else [str(p).strip() for p in m["ports"] if str(p).strip()],
                        "port": max(1, int(_num("port", 8080))),
                        "gpu_count": max(1, int(_num("gpu_count", 1))),
                        "container_disk_gb": int(_num("container_disk_gb", 20)),
                        "volume_gb": int(_num("volume_gb", 0)),
                        "volume_mount": str(m.get("volume_mount") or "").strip(),
                        # network volume by id (survives pod termination;
                        # wins over volume_gb) + DC pinning (network
                        # volumes are data-center-bound)
                        "volume_id": str(m.get("volume_id") or "").strip(),
                        "data_center_ids": [str(d).strip() for d in
                                            (m.get("data_center_ids") or [])
                                            if str(d).strip()],
                        "spot": bool(m.get("spot")),
                        "env": {str(k): str(v) for k, v in (m.get("env") or {}).items()}
                               if isinstance(m.get("env") or {}, dict) else {},
                        "alias": str(m.get("alias") or mname).strip() or mname,
                        "pod_id": str(m.get("pod_id") or "").strip(),
                        "start_timeout_s": _num("start_timeout_s", 600),
                        "idle_stop_s": _num("idle_stop_s", 0),
                        "idle_terminate_s": _num("idle_terminate_s", 0),
                        # v3 lifecycle: monthly spend cap (0 = off),
                        # unexpected-death policy, and the LANDED price
                        # the deploy wizard persists for live accrual
                        "monthly_budget_usd": _num("monthly_budget_usd", 0),
                        "auto_recover": ("redeploy" if str(
                            m.get("auto_recover") or "").strip() == "redeploy"
                            else "alert"),
                        "est_cost_per_hr": _num("est_cost_per_hr", 0),
                        # v4 model finder: custom-model provenance
                        "custom": bool(m.get("custom")),
                        "source_file": str(m.get("source_file") or "").strip(),
                        "source_url": str(m.get("source_url") or "").strip(),
                        "source_size_gb": _num("source_size_gb", 0),
                        "mmproj_url": str(m.get("mmproj_url") or "").strip(),
                        "mmproj_size_gb": _num("mmproj_size_gb", 0),
                        "deploy": m.get("deploy") if isinstance(m.get("deploy"), dict) else {},
                    }
                    if not pod["image"] and not pod["pod_id"]:
                        # gateway-deployed pods need an image; a PINNED pod
                        # (pod_id) already exists and needs none
                        logger.warning("gateway config: runpod pod model %r "
                                       "has no image — dropped", mname)
                        continue
                    rmodels[str(mname)] = pod
            entry["models"] = rmodels
        elif btype == RUNPOD_PUBLIC_BACKEND:
            # Public Endpoints: {model_slug, remote_id?, timeout_s?, policy?}
            entry["api_key_env"] = str(b.get("api_key_env") or "RUNPOD_API_KEY")
            rmodels: dict[str, dict[str, Any]] = {}
            for mname, m in (b.get("models") or {}).items():
                if not isinstance(m, dict):
                    logger.warning("gateway config: runpod-public model %r "
                                   "is not an object — dropped", mname)
                    continue
                m = dict(m)
                slug = str(m.get("model_slug") or "").strip()
                if not slug:
                    logger.warning("gateway config: runpod-public model %r "
                                   "has no model_slug — dropped", mname)
                    continue
                try:
                    tmo = float(m.get("timeout_s") or 0)
                except (TypeError, ValueError):
                    tmo = 0.0
                rmodels[str(mname)] = {
                    "model_slug": slug,
                    "remote_id": str(m.get("remote_id") or slug).strip(),
                    "timeout_s": tmo,
                    "policy": dict(m["policy"]) if isinstance(m.get("policy"), dict) else {},
                }
            entry["models"] = rmodels
        else:  # openai-type cloud backend (executed in Stage 3)
            entry["base_url"] = str(b.get("base_url") or "").strip()
            entry["api_key_env"] = str(b.get("api_key_env") or "").strip()
            # Alternative key sources (tier B): read the key from a FILE
            # (Docker/K8s secrets, Vault Agent rendered files) or from a
            # COMMAND's stdout (vault CLI, aws secretsmanager, anything).
            # Resolution order in cloud.resolve_backend_key:
            # env var → api_key_file → api_key_cmd → OS keyring.
            entry["api_key_file"] = str(b.get("api_key_file") or "").strip()
            entry["api_key_cmd"] = str(b.get("api_key_cmd") or "").strip()
            # Platform credential chains (v1.4): auth: sigv4 (AWS env-var
            # chain, per-request signer) or auth: adc (Google Application
            # Default Credentials, cached token) — no static key at rest
            # (gateway/credchains.py). Empty = classic api-key auth.
            _auth = str(b.get("auth") or "").strip().lower()
            entry["auth"] = _auth if _auth in ("sigv4", "adc") else ""
            entry["region"] = str(b.get("region") or "").strip()
            # Monthly token budget (0 = off): the gateway counts usage from
            # its own request log (cache hits never bill — they log as
            # backend "cache") and refuses with 503 budget_exceeded once
            # the cap is hit. Streams are NOT charged (their usage isn't
            # extracted — same blind spot as /admin/stats).
            try:
                entry["monthly_token_budget"] = max(0, int(
                    b.get("monthly_token_budget") or 0))
            except (TypeError, ValueError):
                entry["monthly_token_budget"] = 0
            # Output cap applied to client-sent max_tokens: int ≥ 1, or
            # null = UNCAPPED (reasoning models — gpt-oss via the HF
            # router — spend the budget on reasoning_content and come
            # back empty under a tight cap). Absent = cloud default (800).
            if "max_tokens_cap" in b:
                raw_cap = b["max_tokens_cap"]
                if raw_cap is None:
                    entry["max_tokens_cap"] = None
                else:
                    try:
                        entry["max_tokens_cap"] = max(1, int(raw_cap))
                    except (TypeError, ValueError):
                        logger.warning("gateway config: backend %r "
                                       "max_tokens_cap must be an int or "
                                       "null — using the default", name)
            cmodels: dict[str, str] = {}
            for mname, m in (b.get("models") or {}).items():
                if isinstance(m, dict):
                    # "name" is what the console's model form writes —
                    # accept it or console-added cloud models silently
                    # vanish on reload (observed bug)
                    m = m.get("id") or m.get("model") or m.get("name") or ""
                if isinstance(m, str) and m.strip():
                    cmodels[str(mname)] = m.strip()
            entry["models"] = cmodels
        out[str(name)] = entry
    return out


def _hash_key(raw: str) -> str:
    """sha256 hex of a bearer key (for storage at rest)."""
    import hashlib
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _looks_hashed(raw: str) -> bool:
    """True when a stored key is already a sha256 hex digest (64 hex)."""
    return bool(raw) and len(raw) == 64 and all(
        c in "0123456789abcdef" for c in raw)


def _normalize_api_keys(raw: Any) -> dict[str, dict[str, Any]]:
    """api_keys: list of {id, key | key_hash, owner?, note?,
    monthly_token_budget?, monthly_usd_budget?, models?, rate_limit_rpm?,
    rate_limit_tpm?} → {id: entry}.

    KEYS ARE HASHED AT REST: a plaintext `key` is converted to
    `key_hash` (sha256) at load time and the plaintext is dropped from
    the normalized entry — auth compares hash(presented token). Existing
    plaintext in the CONFIG FILE is migrated once at startup
    (_migrate_plaintext_keys in app.py). Invalid entries dropped."""
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, list):
        return out
    for k in raw:
        if not isinstance(k, dict):
            continue
        kid = str(k.get("id") or "").strip()
        key_hash = str(k.get("key_hash") or "").strip()
        plaintext = str(k.get("key") or "").strip()
        if not kid or not (key_hash or plaintext):
            logger.warning("gateway config: api_keys entry needs id + "
                           "key/key_hash — dropped")
            continue
        if not key_hash:
            key_hash = _hash_key(plaintext)
        try:
            tok_b = max(0, int(k.get("monthly_token_budget") or 0))
        except (TypeError, ValueError):
            tok_b = 0
        try:
            usd_b = max(0.0, float(k.get("monthly_usd_budget") or 0))
        except (TypeError, ValueError):
            usd_b = 0.0
        try:
            rpm = max(0, int(k.get("rate_limit_rpm") or 0))
        except (TypeError, ValueError):
            rpm = 0
        try:
            tpm = max(0, int(k.get("rate_limit_tpm") or 0))
        except (TypeError, ValueError):
            tpm = 0
        models = [str(m).strip() for m in (k.get("models") or [])
                  if str(m).strip()]
        out[kid] = {
            "id": kid, "key_hash": key_hash,
            "owner": str(k.get("owner") or "").strip(),
            "note": str(k.get("note") or "").strip(),
            "monthly_token_budget": tok_b,
            "monthly_usd_budget": usd_b,
            "models": models,
            "rate_limit_rpm": rpm,
            "rate_limit_tpm": tpm,
        }
    return out


def _normalize_verify_schedule(raw: Any) -> dict[str, Any]:
    """verify_schedule: {enabled, interval_min, backends, timeout_s,
    failures_before_alert, webhook_url} — the scheduled synthetic
    verification (tier 2). Defaults: disabled, 30 min, all openai-type
    backends, 2 consecutive failures before alerting, no webhook."""
    rc = raw if isinstance(raw, dict) else {}
    try:
        interval = max(1, int(rc.get("interval_min") or 30))
    except (TypeError, ValueError):
        interval = 30
    try:
        timeout_s = max(5.0, float(rc.get("timeout_s") or 30.0))
    except (TypeError, ValueError):
        timeout_s = 30.0
    try:
        failures = max(1, int(rc.get("failures_before_alert") or 2))
    except (TypeError, ValueError):
        failures = 2
    backends = [str(b).strip() for b in (rc.get("backends") or [])
                if str(b).strip()]
    return {
        "enabled": bool(rc.get("enabled", False)),
        "interval_min": interval,
        "backends": backends,
        "timeout_s": timeout_s,
        "failures_before_alert": failures,
        "webhook_url": str(rc.get("webhook_url") or "").strip(),
    }


def _normalize_model_prices(raw: Any) -> dict[str, dict[str, Any]]:
    """model_prices: {backend: {model_id: {"in": $/Mtok, "out": $/Mtok,
    "providers": {name: {"in","out"}}?}}} — light validation, pass-through."""
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for backend, models in raw.items():
        if not isinstance(models, dict):
            continue
        bmap: dict[str, Any] = {}
        for mid, p in models.items():
            if not isinstance(p, dict):
                continue
            try:
                entry = {"in": max(0.0, float(p.get("in") or 0)),
                         "out": max(0.0, float(p.get("out") or 0))}
            except (TypeError, ValueError):
                continue
            provs = p.get("providers")
            if isinstance(provs, dict) and provs:
                pmap: dict[str, Any] = {}
                for pn, pv in provs.items():
                    if not isinstance(pv, dict):
                        continue
                    try:
                        pmap[str(pn)] = {
                            "in": max(0.0, float(pv.get("in") or 0)),
                            "out": max(0.0, float(pv.get("out") or 0))}
                    except (TypeError, ValueError):
                        continue
                if pmap:
                    entry["providers"] = pmap
            bmap[str(mid)] = entry
        if bmap:
            out[str(backend)] = bmap
    return out


def _normalize_guardrails(raw: Any) -> dict[str, Any]:
    """guardrails: {enabled, request_rules, response_rules, webhook_url,
    webhook_timeout_s, webhook_fail_open}. Rule types: pattern (regex),
    prompt_injection (built-in heuristics), json (response must parse).
    Actions: block (400 guardrail_violation), redact (response rules —
    replace matches), log (observe only). The optional webhook is an
    external policy engine: POST {direction, model, key, text} →
    {allow: bool, reason}; fail-open by default."""
    g = raw if isinstance(raw, dict) else {}

    def _rules(lst: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in (lst or []):
            if not isinstance(r, dict):
                continue
            rtype = str(r.get("type") or "").strip()
            if rtype not in ("pattern", "prompt_injection", "json"):
                continue
            out.append({
                "name": str(r.get("name") or rtype)[:60],
                "type": rtype,
                "pattern": str(r.get("pattern") or ""),
                "action": str(r.get("action") or "block").strip(),
                "replacement": str(r.get("replacement") or "[REDACTED]"),
                "message": str(r.get("message") or ""),
            })
        return out

    try:
        whto = max(1.0, float(g.get("webhook_timeout_s") or 10))
    except (TypeError, ValueError):
        whto = 10.0
    return {
        "enabled": bool(g.get("enabled", False)),
        "request_rules": _rules(g.get("request_rules")),
        "response_rules": _rules(g.get("response_rules")),
        "webhook_url": str(g.get("webhook_url") or "").strip(),
        "webhook_timeout_s": whto,
        "webhook_fail_open": bool(g.get("webhook_fail_open", True)),
    }


def _normalize_smart_routes(raw: Any) -> dict[str, dict[str, Any]]:
    """smart_routes: {name: {strategy, candidates}} — the tier E router.
    Strategies: cheapest (via model_prices), fastest (via live latency
    EWMA), round_robin, weighted/canary (weight fields). Candidates:
    [{backend, model, weight?}]. A smart route name resolves when the
    name matches NO alias or backend model (aliases win)."""
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for name, r in raw.items():
        if not isinstance(r, dict):
            continue
        cands = []
        for c in (r.get("candidates") or []):
            if not isinstance(c, dict):
                continue
            b = str(c.get("backend") or "").strip()
            m = str(c.get("model") or "").strip()
            if not b or not m:
                continue
            try:
                w = max(0.0, float(c.get("weight") or 1.0))
            except (TypeError, ValueError):
                w = 1.0
            cands.append({"backend": b, "model": m, "weight": w})
        if not cands:
            continue
        strategy = str(r.get("strategy") or "round_robin").strip()
        if strategy not in ("cheapest", "fastest", "round_robin",
                            "weighted", "canary"):
            strategy = "round_robin"
        out[str(name)] = {"strategy": strategy, "candidates": cands}
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


class ConfigStore:
    """Config persistence — the enterprise-fork extension point.

    Core: FileConfigStore (JSON file with mtime-based hot-reload).
    Enterprise: DatabaseConfigStore (shared DB for multi-replica).
    GatewayConfig delegates all I/O to the active store."""

    def load(self) -> dict[str, Any] | None:
        """Read the raw config. None when unreadable (keep previous)."""
        raise NotImplementedError

    def save(self, raw: dict[str, Any],
             *, expected_version: Any = None) -> bool:
        """Persist the raw config.

        Optimistic concurrency (tier D — multi-replica): when the caller
        passes the store version it observed before its read-modify-write
        (``expected_version``), a store that detects a newer committed
        write in between returns False — the caller should re-load,
        re-apply its mutation and retry. Stores that cannot detect races
        (the single-process file store) accept the argument and return
        True unconditionally."""
        raise NotImplementedError

    def version(self) -> Any:
        """A change-detection token (mtime for files, a revision number
        for DBs). None when the source doesn't exist."""
        raise NotImplementedError


class FileConfigStore(ConfigStore):
    """JSON-file-backed config storage — the core default."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict[str, Any] | None:
        if not self.path.is_file():
            return None
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.error("gateway config %s unreadable (%s)", self.path, e)
            return None

    def save(self, raw: dict[str, Any],
             *, expected_version: Any = None) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(raw, indent=2) + "\n",
                             encoding="utf-8")
        return True  # single-process file store — no race detection

    def version(self) -> Any:
        try:
            return self.path.stat().st_mtime if self.path.is_file() else None
        except OSError:
            return None


class GatewayConfig:
    """Parsed gateway config; reloads when the store's version changes.

    A broken/unreadable source keeps the last good values (defaults on
    first load) — the gateway keeps serving instead of resetting.

    The store is swappable: pass a ConfigStore instance to use a
    different backend (enterprise: DatabaseConfigStore). The default
    is FileConfigStore at CONFIG_PATH."""

    def __init__(self, path_or_store: Path | str | ConfigStore | None = None):
        if isinstance(path_or_store, ConfigStore):
            self.store = path_or_store
            self.path = getattr(path_or_store, "path", CONFIG_PATH)
        else:
            self.path = (Path(path_or_store) if path_or_store is not None
                         else CONFIG_PATH)
            self.store = FileConfigStore(self.path)
        self._lock = threading.Lock()
        self._version: Any = None
        self._apply(_default_raw())  # seed defaults before the first load
        self._load()

    # ── loading ──

    def _remember_mtime(self) -> None:
        self._version = self.store.version()

    def _load(self) -> None:
        data = self.store.load()
        if data is None:
            if not self.path.is_file() and not hasattr(self.store, "path"):
                # first boot with no config — apply defaults
                self._apply(_default_raw())
            self._remember_mtime()
            return
        if not isinstance(data, dict):
            logger.error("gateway config: not a JSON object — previous values kept")
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
        # Control-plane split (v2.1): admin_port set → /admin/* + the
        # console serve ONLY on that port (admin_bind, localhost by
        # default — the Kong/Envoy posture); the serving port loses
        # them entirely (network isolation, not just auth). None = the
        # single-port behavior of every prior release. Restart to
        # change (listeners bind at boot).
        ap = raw.get("admin_port")
        self.admin_port = int(ap) if ap else None
        self.admin_bind = str(raw.get("admin_bind") or "127.0.0.1")
        # Boot-context env overrides (container convention, like
        # GATELODGE_REDIS_URL): env WINS over the file and is re-applied
        # on every hot-reload — the operator can still edit everything
        # else in the file without the ports snapping back.
        try:
            env_port = os.environ.get("GATELODGE_PORT", "").strip()
            if env_port:
                self.port = int(env_port)
        except ValueError:
            pass
        env_bind = os.environ.get("GATELODGE_BIND", "").strip()
        if env_bind:
            self.bind = env_bind
        try:
            env_admin_port = os.environ.get("GATELODGE_ADMIN_PORT", "").strip()
            if env_admin_port:
                self.admin_port = int(env_admin_port)
        except ValueError:
            pass
        env_admin_bind = os.environ.get("GATELODGE_ADMIN_BIND", "").strip()
        if env_admin_bind:
            self.admin_bind = env_admin_bind
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
        # v3 lifecycle: account-wide monthly spend cap (0 = off)
        try:
            self.budget_usd_month = float(raw.get("budget_usd_month") or 0)
        except (TypeError, ValueError):
            self.budget_usd_month = 0.0
        # Response cache: kill switch + sizing (see DEFAULT_CONFIG).
        rc = raw.get("response_cache") if isinstance(raw.get("response_cache"), dict) else {}
        self.response_cache = {
            "enabled": bool(rc.get("enabled", True)),
            "ttl_s": max(0.0, float(rc.get("ttl_s") or 3600)),
            "max": max(0, int(rc.get("max") or 100)),
        }
        # Semantic cache (v2.0): paraphrase hits via LOCAL embeddings —
        # rides the response-cache eligibility rules and master switch;
        # gateway/semantic_cache.py owns the semantics.
        sc = raw.get("semantic_cache") if isinstance(raw.get("semantic_cache"), dict) else {}
        try:
            sc_threshold = max(0.0, min(1.0, float(sc.get("threshold") or 0.92)))
        except (TypeError, ValueError):
            sc_threshold = 0.92
        self.semantic_cache = {
            "enabled": bool(sc.get("enabled", False)),
            "embedding_model": str(sc.get("embedding_model") or "").strip(),
            "threshold": sc_threshold,
            "ttl_s": max(0.0, float(sc.get("ttl_s") or 3600)),
            "max": max(0, int(sc.get("max") or 100)),
        }
        # Virtual (per-consumer) keys + cached provider pricing
        self.api_keys = _normalize_api_keys(raw.get("api_keys"))
        self.model_prices = _normalize_model_prices(raw.get("model_prices"))
        # Scheduled synthetic verification (tier 2)
        self.verify_schedule = _normalize_verify_schedule(
            raw.get("verify_schedule"))
        # TLS: https when enabled with certfile+keyfile (tier A)
        tls = raw.get("tls") if isinstance(raw.get("tls"), dict) else {}
        self.tls = {
            "enabled": bool(tls.get("enabled", False)),
            "certfile": str(tls.get("certfile") or "").strip(),
            "keyfile": str(tls.get("keyfile") or "").strip(),
        }
        # Log retention (tier B): months to keep month files/rollups
        try:
            self.log_retention_months = max(0, int(
                raw.get("log_retention_months") or 0))
        except (TypeError, ValueError):
            self.log_retention_months = 0
        # Audit shipping (tier B)
        audit = raw.get("audit") if isinstance(raw.get("audit"), dict) else {}
        self.audit = {"webhook_url": str(audit.get("webhook_url") or "").strip()}
        # Tracing (v2.0): OTLP export — boot-time section (a change
        # needs a restart, like scim). gateway/tracing.py owns the
        # semantics; this is just the normalized surface.
        otel = raw.get("otel")
        otel = otel if isinstance(otel, dict) else {}
        self.otel = {
            "enabled": bool(otel.get("enabled")),
            "endpoint": str(otel.get("endpoint") or "").strip(),
            "service_name": str(otel.get("service_name") or "gatelodge"),
            "sample_ratio": otel.get("sample_ratio", 1.0),
            "headers": otel.get("headers")
            if isinstance(otel.get("headers"), dict) else {},
            "headers_env": otel.get("headers_env")
            if isinstance(otel.get("headers_env"), dict) else {},
            "batch_flush_s": otel.get("batch_flush_s", 5.0),
            "batch_max": otel.get("batch_max", 64),
            "timeout_s": otel.get("timeout_s", 5.0),
        }
        # Session budgets (v1.4): when enabled, an x-session-id header
        # on a request charges a session:<id> spend scope alongside the
        # consuming key (the enterprise pre-gate reads the raw section
        # for its own knobs — default_tpm, per-tenant overrides).
        sb = raw.get("session_budgets")
        sb = sb if isinstance(sb, dict) else {}
        self.session_budgets = {"enabled": bool(sb.get("enabled", False))}
        # Guardrails + smart routes (tier E)
        self.guardrails = _normalize_guardrails(raw.get("guardrails"))
        self.smart_routes = _normalize_smart_routes(raw.get("smart_routes"))

    def maybe_reload(self) -> bool:
        """Re-read when the store's version changed. True when reloaded."""
        version = self.store.version()
        if version == self._version:
            return False
        with self._lock:
            if self.store.version() == self._version:  # re-check under lock
                return False
            self._load()
        return True

    def reload_now(self) -> None:
        """Unconditional reload (admin mutations want determinism, not
        version-granularity luck)."""
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

    def local_embedding_models(self) -> dict[str, dict[str, Any]]:
        return dict(self.local_backend().get("embedding_models") or {})

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
                if self._entry_is_local(b)]
            if not plan["entries"]:
                raise GatewayConfigError(
                    f"alias {name!r} is local_only but its chain has no "
                    "local backend entries")
        return plan

    def _entry_is_local(self, backend_name: str) -> bool:
        """Embedded always; llama-server only in SUBPROCESS mode (remote
        base_url and runpod backends leave the machine like a cloud one)."""
        b = self.backends.get(backend_name) or {}
        if b.get("type") == EMBEDDED_BACKEND:
            return True
        if b.get("type") == LLAMA_SERVER_BACKEND:
            return not b.get("base_url")
        return False  # openai cloud, remote llama-server, runpod (all forms)

    def resolve_model(self, name: str | None) -> tuple[str, str]:
        """Primary chain entry of resolve_chain (Stage-1 compatibility)."""
        return self.resolve_chain(name)["entries"][0]

    def resolve_embedding(self, name: str | None) -> tuple[str, str]:
        """Embedding ``model`` → (backend_name, model_key).

        Direct 'backend:model': embedded backends resolve against
        embedding_models ONLY (chat instances cannot embed — their context
        is created without pooling); cloud backends resolve against models
        (the remote endpoint decides chat vs embedding). Bare names search
        local embedding_models first, then every cloud backend's models.
        """
        name = (name or "").strip()
        if not name:
            raise GatewayConfigError("embedding model must not be empty")
        if ":" in name:
            backend_name, _, model_key = name.partition(":")
            backend = self.backends.get(backend_name)
            if backend is None:
                raise GatewayConfigError(f"unknown backend in {name!r}")
            where = ("embedding_models" if backend.get("type") == EMBEDDED_BACKEND
                     else "models")
            if model_key not in (backend.get(where) or {}):
                raise GatewayConfigError(
                    f"backend {backend_name!r} has no embedding model {model_key!r}")
            return backend_name, model_key
        local = self.local_backend_name()
        if local and name in self.local_embedding_models():
            return local, name
        for bname, b in self.backends.items():
            if b.get("type") != EMBEDDED_BACKEND and name in (b.get("models") or {}):
                return bname, name
        raise GatewayConfigError(
            f"unknown embedding model: {name!r} (define it under "
            "backends.<local>.embedding_models or a cloud backend)")

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


def set_config_store(store: ConfigStore) -> GatewayConfig:
    """Swap the singleton's config store (the sanctioned injection —
    enterprise: DatabaseConfigStore for a shared-DB, multi-replica
    deployment). Rebuilds the config from the new store; hot-reload
    picks up the new store's version token from then on. Call at
    startup, BEFORE serving. Returns the new singleton."""
    global _CONFIG
    _CONFIG = GatewayConfig(store)
    logger.info("config store swapped to %s", type(store).__name__)
    return _CONFIG
