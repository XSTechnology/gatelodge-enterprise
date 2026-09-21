"""Model catalogue + deploy wizard core.

Curated RECIPES map a friendly model name to everything the gateway
needs to deploy it on RunPod: hardware requirements, llama.cpp build
flavor, boot script template, context presets, volume/disk minimums.
Everything hardware- or availability-related that can change (GPU
types + prices, data centers + volume support, account volumes) is
fetched LIVE from the RunPod catalog at deploy time — the recipe never
hardcodes it.

Hard-won knowledge baked into the recipes (glm53 deploy, 2026-09-20):
- glm5next arch needs unslothai/llama.cpp branch glm5next/upstream
  (mainline rejects its tensors), --jinja for the chat template, and
  REJECTS --no-mmap.
- NFS network-volume mmap is catastrophic (~34 MB/s, unkillable
  D-state) — big-model scripts copy weights to container disk first.
- ctx-size semantics DIFFER by build: mainline treats --ctx-size as
  the TOTAL divided by --parallel; the glm5next build defaulted to 4
  slots and treated it as PER-SLOT. Scripts therefore always pass BOTH
  --parallel and --ctx-size explicitly; each recipe's ctx_options
  carries the (parallel, ctx_total) pair that produces the labeled
  per-slot context for that recipe's build.
- Container disk is EPHEMERAL (wiped on stop) — size it ≥ model.
"""

from __future__ import annotations

import time
from typing import Any

from . import runpod

# ── Curated recipes ─────────────────────────────────────────────────────────

CATALOG: list[dict[str, Any]] = [
    {
        "id": "qwen3-8b",
        "model": "Qwen3-8B",
        "quant": "UD-Q4_K_XL",
        "tagline": "Starter — fast, cheap, surprisingly capable",
        "capabilities": ["chat", "deep_think"],
        "build": "mainline",
        "image": "ghcr.io/ggml-org/llama.cpp:server-cuda",
        "repo_url": ("https://huggingface.co/unsloth/Qwen3-8B-GGUF/"
                     "resolve/main/Qwen3-8B-UD-Q4_K_XL.gguf"),
        "size_gb": 6,
        "vram_min_gb": 12,
        "gpu_options": [
            {"gpu": "NVIDIA GeForce RTX 4090", "count": 1},
            {"gpu": "NVIDIA GeForce RTX 3090", "count": 1},
            {"gpu": "NVIDIA RTX A4000", "count": 1},
        ],
        "volume": None,
        "disk_gb_min": 20,
        "ctx_options": [
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k"},
            {"key": "64k", "parallel": 1, "ctx_total": 65536,
             "label": "1 slot × 64k"},
        ],
        "ctx_default": "32k",
        "thinking": True,
        "script": "mainline",
        "start_timeout_s": 1200,
    },
    {
        "id": "qwen3-30b-a3b",
        "model": "Qwen3-30B-A3B",
        "quant": "UD-Q4_K_XL",
        "tagline": "Fast MoE — 30B class, 3B active, quick responses",
        "capabilities": ["chat", "deep_think"],
        "build": "mainline",
        "image": "ghcr.io/ggml-org/llama.cpp:server-cuda",
        "repo_url": ("https://huggingface.co/unsloth/Qwen3-30B-A3B-GGUF/"
                     "resolve/main/Qwen3-30B-A3B-UD-Q4_K_XL.gguf"),
        "size_gb": 19,
        "vram_min_gb": 32,
        "gpu_options": [
            {"gpu": "NVIDIA L40", "count": 1},
            {"gpu": "NVIDIA L40S", "count": 1},
            {"gpu": "NVIDIA A40", "count": 1},
        ],
        "volume": None,
        "disk_gb_min": 30,
        "ctx_options": [
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k"},
            {"key": "64k", "parallel": 1, "ctx_total": 65536,
             "label": "1 slot × 64k"},
        ],
        "ctx_default": "32k",
        "thinking": True,
        "script": "mainline",
        "start_timeout_s": 1800,
    },
    {
        "id": "qwen3-32b",
        "model": "Qwen3-32B",
        "quant": "UD-Q4_K_XL",
        "tagline": "Dense 32B — strongest single-GPU generalist",
        "capabilities": ["chat", "deep_think"],
        "build": "mainline",
        "image": "ghcr.io/ggml-org/llama.cpp:server-cuda",
        "repo_url": ("https://huggingface.co/unsloth/Qwen3-32B-GGUF/"
                     "resolve/main/Qwen3-32B-UD-Q4_K_XL.gguf"),
        "size_gb": 21,
        "vram_min_gb": 34,
        "gpu_options": [
            {"gpu": "NVIDIA L40S", "count": 1},
            {"gpu": "NVIDIA L40", "count": 1},
            {"gpu": "NVIDIA A100 80GB PCIe", "count": 1},
        ],
        "volume": None,
        "disk_gb_min": 30,
        "ctx_options": [
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k"},
            {"key": "64k", "parallel": 1, "ctx_total": 65536,
             "label": "1 slot × 64k"},
        ],
        "ctx_default": "32k",
        "thinking": True,
        "script": "mainline",
        "start_timeout_s": 1800,
    },
    {
        "id": "glm53-flash",
        "model": "GLM-5.3-Flash",
        "quant": "UD-IQ4_XS",
        "quant_glob": "IQ4_XS",
        "tagline": ("Workhorse — 321B MoE (18B active), proven recipe "
                    "(3× A100 EUR-IS-1)"),
        "capabilities": ["chat", "deep_think"],
        "build": "glm5next",
        "image": "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404",
        "repo": "unsloth/GLM-5.3-Flash-GGUF",
        "allow_patterns": ["*UD-IQ4_XS*"],
        "size_gb": 157,
        "vram_min_gb": 210,
        "gpu_options": [
            {"gpu": "NVIDIA A100-SXM4-80GB", "count": 3},
            {"gpu": "NVIDIA H200", "count": 2},
        ],
        "volume": {"size_gb_min": 200},
        "disk_gb_min": 200,
        "ctx_options": [
            {"key": "64k", "parallel": 2, "ctx_total": 131072,
             "label": "2 slots × 64k (recommended)", "kv_est_gb": 45},
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k", "kv_est_gb": 23},
        ],
        "ctx_default": "64k",
        "thinking": True,
        "script": "glm5next_volume",
        "start_timeout_s": 3600,
    },
    {
        "id": "glm53-flash-q4xl",
        "model": "GLM-5.3-Flash",
        "quant": "Q4_K_XL",
        "quant_glob": "Q4_K_XL",
        "tagline": "Workhorse+ — higher-quality quant of GLM-5.3-Flash",
        "capabilities": ["chat", "deep_think"],
        "build": "glm5next",
        "image": "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404",
        "repo": "unsloth/GLM-5.3-Flash-GGUF",
        "allow_patterns": ["*Q4_K_XL*"],
        "size_gb": 200,
        "vram_min_gb": 240,
        "gpu_options": [
            {"gpu": "NVIDIA A100-SXM4-80GB", "count": 4},
            {"gpu": "NVIDIA H200", "count": 2},
        ],
        "volume": {"size_gb_min": 250},
        "disk_gb_min": 250,
        "ctx_options": [
            {"key": "64k", "parallel": 2, "ctx_total": 131072,
             "label": "2 slots × 64k (recommended)", "kv_est_gb": 45},
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k", "kv_est_gb": 23},
        ],
        "ctx_default": "64k",
        "thinking": True,
        "script": "glm5next_volume",
        "start_timeout_s": 3600,
    },
    {
        "id": "deepseek-v4-flash",
        "model": "DeepSeek-V4-Flash",
        "quant": "UD-IQ4_XS",
        "quant_glob": "IQ4_XS",
        "tagline": "Coder — DeepSeek V4 Flash MoE (sizes estimated)",
        "capabilities": ["chat", "deep_think", "coding"],
        "build": "glm5next",
        "image": "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404",
        "repo": "unsloth/DeepSeek-V4-Flash-GGUF",
        "allow_patterns": ["*UD-IQ4_XS*"],
        "size_gb": 115,
        "vram_min_gb": 145,
        "gpu_options": [
            {"gpu": "NVIDIA A100-SXM4-80GB", "count": 2},
            {"gpu": "NVIDIA B200", "count": 1},
        ],
        "volume": {"size_gb_min": 150},
        "disk_gb_min": 150,
        "ctx_options": [
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k", "kv_est_gb": 20},
        ],
        "ctx_default": "32k",
        "thinking": True,
        "script": "glm5next_volume",
        "start_timeout_s": 3600,
    },
    {
        "id": "glm53-flash-mi300x",
        "model": "GLM-5.3-Flash",
        "quant": "UD-IQ4_XS",
        "quant_glob": "IQ4_XS",
        "tagline": ("Experimental — GLM-5.3-Flash on ONE AMD MI300X "
                    "(192GB, ROCm build); 32k ctx fits, 64k does not"),
        "capabilities": ["chat", "deep_think"],
        "build": "glm5next",
        "image": "rocm/pytorch:latest",
        "repo": "unsloth/GLM-5.3-Flash-GGUF",
        "allow_patterns": ["*UD-IQ4_XS*"],
        "size_gb": 157,
        "vram_min_gb": 185,
        "gpu_options": [
            {"gpu": "AMD Instinct MI300X OAM", "count": 1},
        ],
        "volume": {"size_gb_min": 200},
        "disk_gb_min": 200,
        "ctx_options": [
            {"key": "32k", "parallel": 2, "ctx_total": 65536,
             "label": "2 slots × 32k (fits 192GB)", "kv_est_gb": 23},
        ],
        "ctx_default": "32k",
        "thinking": True,
        "script": "glm5next_volume_rocm",
        "start_timeout_s": 3600,
        "experimental": True,
    },
]

# ── Boot script templates ──────────────────────────────────────────────────

def _mainline_script(recipe: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Small/medium models: download the single curated GGUF to
    container disk and serve with the image's bundled llama-server.
    The image declares the server via ENTRYPOINT, but our pod spec
    overrides the entrypoint with sh -c, so resolve the binary
    explicitly (live-verified: prefixing the binary onto the appended
    CMD crash-looped)."""
    url = recipe["repo_url"]
    return (
        "set -e; mkdir -p /models; "
        "if [ ! -f /models/model.gguf ]; then "
        f"echo '[boot] downloading {recipe['model']} {recipe['quant']}'; "
        f"curl -fsSL --retry 3 --max-time 1800 -o /models/model.gguf '{url}'; "
        "echo '[boot] download done'; fi; "
        "df -h /models | tail -1; "
        "SRV=/app/llama-server; [ -x \"$SRV\" ] || "
        "SRV=$(command -v llama-server || echo /llama-server); "
        "echo '[boot] serving'; "
        "exec \"$SRV\" -m /models/model.gguf --host 0.0.0.0 --port 8080 "
        f"-ngl 99 --jinja --parallel {int(ctx['parallel'])} "
        f"--ctx-size {int(ctx['ctx_total'])}"
    )


def _glm5next_volume_script(recipe: dict[str, Any], ctx: dict[str, Any],
                            rocm: bool = False) -> str:
    """Big models on the glm5next arch: (1) build unsloth's llama.cpp
    fork if the volume doesn't have it cached, (2) download weights to
    the volume if missing, (3) copy weights to container disk (NFS
    mmap is ~34 MB/s and wedges the container in D-state), (4) serve
    with explicit --parallel/--ctx-size (build's ctx semantics are
    per-slot when --parallel is default — always pass both).
    rocm=True builds with GGML_HIP (AMD MI300X = gfx942) instead of
    CUDA — used by the MI300X recipe (experimental: ROCm build
    unverified live)."""
    repo = recipe["repo"]
    pats = ", ".join(f"'{p}'" for p in recipe["allow_patterns"])
    glob = recipe["quant_glob"]
    cmake_defines = ("-DGGML_HIP=ON -DAMDGPU_TARGETS=gfx942" if rocm
                     else "-DGGML_CUDA=ON")
    path_export = ("" if rocm
                   else "export PATH=/usr/local/cuda/bin:$PATH; ")
    return (
        "set -e; " + path_export +
        "echo '[boot] start'; "
        # 1) llama.cpp glm5next build — cached on the volume
        "if [ ! -x /workspace/llama.cpp/build/bin/llama-server ]; then "
        "echo '[build] installing tools'; "
        "(apt-get update -qq && apt-get install -y -qq git cmake "
        "build-essential ccache) > /workspace/apt.log 2>&1 || true; "
        "echo '[build] cloning unslothai/llama.cpp (glm5next/upstream)'; "
        "git clone --depth 1 --branch glm5next/upstream "
        "https://github.com/unslothai/llama.cpp /workspace/llama.cpp; "
        "echo '[build] cmake configure'; "
        "cmake -S /workspace/llama.cpp -B /workspace/llama.cpp/build "
        f"{cmake_defines} > /workspace/cmake.log 2>&1; "
        "echo '[build] compiling (once — cached on the volume)'; "
        "cmake --build /workspace/llama.cpp/build --config Release -j "
        "> /workspace/build.log 2>&1; "
        "fi; "
        "[ -x /workspace/llama.cpp/build/bin/llama-server ] || "
        "{ echo '[build] ERROR llama-server missing'; exit 1; }; "
        # 2) model weights — cached on the volume
        "if ! find /workspace/model -name '*.gguf' -type f | grep -q .; then "
        "echo '[model] downloading weights'; "
        "python3 -m pip install -q -U huggingface_hub "
        "> /workspace/pip.log 2>&1 || true; "
        "python3 -c \"from huggingface_hub import snapshot_download; "
        f"snapshot_download('{repo}', allow_patterns=[{pats}], "
        "local_dir='/workspace/model', max_workers=8)\"; "
        "echo '[model] download done'; fi; "
        # 3) copy to local disk (NFS mmap is fatal — see module docstring)
        "if [ ! -f /root/model/.copied ]; then "
        "echo '[model] copying to local disk'; mkdir -p /root/model; "
        f"cp $(find /workspace/model -name '*{glob}*.gguf' -type f) "
        "/root/model/ && touch /root/model/.copied; "
        "echo '[model] copy done'; fi; "
        "df -h /root | tail -1; "
        f"MODEL=$(ls /root/model/*{glob}*.gguf | sort | head -1); "
        "[ -n \"$MODEL\" ] || { echo '[model] ERROR no model file'; exit 1; }; "
        # 4) serve
        "echo \"[boot] serving $MODEL\"; "
        "exec /workspace/llama.cpp/build/bin/llama-server -m \"$MODEL\" "
        "--host 0.0.0.0 --port 8080 -ngl 99 --jinja "
        f"--parallel {int(ctx['parallel'])} --ctx-size {int(ctx['ctx_total'])}"
    )


def render_script(recipe: dict[str, Any], ctx: dict[str, Any]) -> str:
    kind = recipe.get("script")
    if kind == "mainline":
        return _mainline_script(recipe, ctx)
    if kind == "glm5next_volume":
        return _glm5next_volume_script(recipe, ctx)
    if kind == "glm5next_volume_rocm":
        return _glm5next_volume_script(recipe, ctx, rocm=True)
    raise ValueError(f"unknown script template {kind!r}")


# ── Recipe lookup / public shape ────────────────────────────────────────────

def find_recipe(recipe_id: str) -> dict[str, Any] | None:
    for r in CATALOG:
        if r["id"] == recipe_id:
            return r
    return None


def public_recipes() -> list[dict[str, Any]]:
    """Recipes minus template plumbing — for GET /admin/catalog."""
    out = []
    for r in CATALOG:
        slim = {k: v for k, v in r.items()
                if k not in ("allow_patterns", "repo_url")}
        out.append(slim)
    return out


# ── Live RunPod options (short cache) ───────────────────────────────────────

_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_TTL_S = 300.0


async def _cached(key: str, fetch):
    hit = _CACHE.get(key)
    if hit and (time.monotonic() - hit[0]) < _CACHE_TTL_S:
        return hit[1], True
    val = fetch()
    if hasattr(val, "__await__"):
        val = await val
    _CACHE[key] = (time.monotonic(), val)
    return val, False


def clear_cache() -> None:
    _CACHE.clear()


async def live_options(api_key_env: str) -> dict[str, Any]:
    """GPUs + data centers (5-min cache) and the account's volumes
    (uncached — cheap and user-mutable)."""
    gpus, gpus_cached = await _cached(
        "gpus", lambda: runpod.list_gpu_catalog(api_key_env))
    dcs, dcs_cached = await _cached(
        "dcs", lambda: runpod.list_datacenter_catalog(api_key_env))
    volumes = await runpod.list_volumes(api_key_env)
    return {
        "gpus": gpus,
        "datacenters": dcs,
        "volumes": volumes,
        "cached": bool(gpus_cached and dcs_cached),
        "cached_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


# ── Validation + spec building ──────────────────────────────────────────────

def _gpu_entry(gpus: list[dict[str, Any]], gpu_id: str) -> dict[str, Any] | None:
    for g in gpus:
        if g.get("id") == gpu_id:
            return g
    return None


def est_cost_per_hr(gpus: list[dict[str, Any]], gpu_id: str,
                    count: int, cloud: str) -> float | None:
    g = _gpu_entry(gpus, gpu_id)
    if not g:
        return None
    price = (g.get("price") or {}).get(cloud)
    if not isinstance(price, (int, float)) or price <= 0:
        return None
    return round(price * int(count), 2)


def candidate_combos(recipe: dict[str, Any], choices: dict[str, Any],
                     gpus: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ordered deploy candidates for capacity fallback: the user's
    choice first, then the recipe's other recommended gpu_options —
    each still checked against the live catalog (VRAM fit, maxCount,
    cloud availability). Same data center throughout: network volumes
    are DC-pinned, so the wizard only falls back across GPU combos,
    never across DCs. Live motivation: 3× A100-SXM4 EUR-IS-1 sat dry
    for 80+ minutes while 2× H200 in the same DC was bookable."""
    cloud = choices.get("cloud") or "secure"
    seen: set[tuple[str, int]] = set()
    out: list[dict[str, Any]] = []

    def add(gpu_id: Any, count: Any) -> None:
        try:
            n = int(count or 0)
        except (TypeError, ValueError):
            return
        if not gpu_id or n < 1:
            return
        g = _gpu_entry(gpus, str(gpu_id))
        if g is None:
            return
        maxc = (g.get("maxCount") or {}).get(cloud)
        price = (g.get("price") or {}).get(cloud)
        if not isinstance(maxc, (int, float)) or maxc < n:
            return
        if not isinstance(price, (int, float)) or price <= 0:
            return
        if float(g.get("memory") or 0) * n < recipe["vram_min_gb"]:
            return
        key = (str(gpu_id), n)
        if key in seen:
            return
        seen.add(key)
        out.append({"gpu_id": str(gpu_id), "gpu_count": n,
                    "est_cost_per_hr": round(price * n, 2)})

    add(choices.get("gpu_id"), choices.get("gpu_count"))
    for opt in recipe.get("gpu_options") or []:
        add(opt.get("gpu"), opt.get("count"))
    return out


def validate_selection(recipe: dict[str, Any], choices: dict[str, Any],
                       gpus: list[dict[str, Any]],
                       datacenters: list[dict[str, Any]],
                       volumes: list[dict[str, Any]]) -> list[str]:
    """Human-readable error list; empty = valid. choices keys: gpu_id,
    gpu_count, cloud, data_center_id, volume_mode (none|existing|create),
    volume_id, volume_size_gb, volume_type, ctx_key, container_disk_gb."""
    errors: list[str] = []
    cloud = choices.get("cloud") or "secure"
    if cloud not in ("secure", "community"):
        errors.append(f"cloud must be 'secure' or 'community', "
                      f"got {cloud!r}")
        cloud = "secure"

    gpu_id = choices.get("gpu_id") or ""
    count = int(choices.get("gpu_count") or 0)
    gpu = _gpu_entry(gpus, gpu_id)
    if gpu is None:
        errors.append(f"unknown GPU {gpu_id!r} (pick from the live catalog)")
    else:
        mem = gpu.get("memory") or 0
        total_vram = float(mem) * max(count, 1)
        if total_vram < recipe["vram_min_gb"]:
            errors.append(
                f"{gpu_id} × {count} gives {total_vram:.0f} GB VRAM — "
                f"{recipe['model']} {recipe['quant']} needs "
                f"{recipe['vram_min_gb']} GB")
        maxc = (gpu.get("maxCount") or {}).get(cloud)
        price = (gpu.get("price") or {}).get(cloud)
        if not isinstance(maxc, (int, float)) or maxc <= 0:
            errors.append(f"{gpu_id} is not available on the {cloud} cloud")
        elif count > maxc:
            errors.append(f"{gpu_id} allows at most {int(maxc)} GPUs on "
                          f"the {cloud} cloud (requested {count})")
        if not isinstance(price, (int, float)) or price <= 0:
            errors.append(f"{gpu_id} has no {cloud} price — unavailable")

    ctx_key = choices.get("ctx_key") or recipe["ctx_default"]
    if not any(o["key"] == ctx_key for o in recipe["ctx_options"]):
        errors.append(f"context preset {ctx_key!r} is not offered for "
                      f"{recipe['id']}")

    disk = int(choices.get("container_disk_gb") or 0)
    if disk < recipe["disk_gb_min"]:
        errors.append(f"container disk {disk} GB is too small — "
                      f"{recipe['id']} needs ≥ {recipe['disk_gb_min']} GB")

    dc_id = choices.get("data_center_id") or ""
    dc = next((d for d in datacenters if d.get("id") == dc_id), None)
    vol_req = recipe.get("volume")
    mode = choices.get("volume_mode") or "none"

    if vol_req:
        if mode not in ("existing", "create"):
            errors.append(f"{recipe['id']} requires a network volume "
                          f"(weights + build cache survive restarts) — "
                          f"pick an existing volume or create one")
        if mode == "existing":
            vol = next((v for v in volumes
                        if v.get("id") == choices.get("volume_id")), None)
            if vol is None:
                errors.append("volume_id must reference one of your "
                              "network volumes")
            elif dc_id and vol.get("dataCenter") != dc_id:
                errors.append(
                    f"volume {vol.get('id')} lives in "
                    f"{vol.get('dataCenter')} — the pod must run there "
                    f"(network volumes are DC-pinned)")
        if mode == "create":
            size = int(choices.get("volume_size_gb") or 0)
            if size < vol_req.get("size_gb_min", 1):
                errors.append(f"new volume too small — "
                              f"{recipe['id']} needs ≥ "
                              f"{vol_req.get('size_gb_min')} GB")
            if dc is not None:
                vol_types = dc.get("networkVolumeTypes") or []
                vtype = choices.get("volume_type") or "STANDARD"
                if not vol_types:
                    errors.append(f"data center {dc_id} does not support "
                                  f"network volumes")
                elif vtype not in vol_types:
                    errors.append(
                        f"data center {dc_id} offers volume types "
                        f"{vol_types} — {vtype} not available there")
            elif not dc_id:
                errors.append("a data center is required to create a volume")
    else:
        if mode == "existing":
            errors.append(f"{recipe['id']} does not use a volume — "
                          f"set volume_mode to 'none'")
    return errors


def build_spec(recipe: dict[str, Any], choices: dict[str, Any]) -> dict[str, Any]:
    """Validated choices → gateway pod spec (the exact shape the config
    allowlist preserves and deploy_payload consumes)."""
    ctx = next((o for o in recipe["ctx_options"]
                if o["key"] == (choices.get("ctx_key") or recipe["ctx_default"])),
               recipe["ctx_options"][0])
    spec: dict[str, Any] = {
        "image": recipe["image"],
        "gpu_type_ids": [choices.get("gpu_id") or
                         recipe["gpu_options"][0]["gpu"]],
        "gpu_count": int(choices.get("gpu_count") or
                         recipe["gpu_options"][0]["count"]),
        "docker_entrypoint": ["sh", "-c"],
        "docker_script": render_script(recipe, ctx),
        "ports": "8080/http",
        "port": 8080,
        "container_disk_gb": int(choices.get("container_disk_gb")
                                 or recipe["disk_gb_min"]),
        "start_timeout_s": recipe.get("start_timeout_s", 1800),
        # v3 cost-safety default: 30 min idle auto-stop (0 disables) —
        # volume recipes pay ~10 min per cold start, so don't go lower
        "idle_stop_s": float(choices.get("idle_stop_s", 1800)),
        "idle_terminate_s": float(choices.get("idle_terminate_s") or 0),
        "alias": choices.get("alias") or recipe["id"],
    }
    if choices.get("volume_id"):
        spec["volume_id"] = choices["volume_id"]
    dc = choices.get("data_center_id")
    if dc:
        spec["data_center_ids"] = [dc]
    if (choices.get("cloud") or "secure") == "community":
        spec["spot"] = True
    return spec
