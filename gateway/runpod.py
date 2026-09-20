"""RunPod backend — POD lifecycle management + SERVERLESS passthrough.

POD mode: one lifecycle-managed RunPod GPU pod per model. On first
request the gateway looks for an existing pod by name (tagged
gw-<backend>-<model>), resumes it when stopped, deploys a new one when
absent, then health-polls the pod's https://<pod-id>-<port>.proxy.runpod.net
URL (llama.cpp's /health) until the model is serving. Requests proxy in
native OpenAI wire format (reusing the llama-server HTTP bridge — pods
run llama-server or any OpenAI-compatible image). Idle reaper:
stop after idle_stop_s (keeps storage — cheap resume), terminate after
idle_terminate_s (frees everything; with download-on-boot storage
terminate is usually CHEAPER than stop). Pods are deliberately NOT
touched on gateway shutdown — they cost money and may serve others.

SERVERLESS mode: models with endpoint_id map to
https://api.runpod.ai/v2/<endpoint_id>/openai/v1 (Bearer api key) and
execute through the gateway's cloud-client path; the first request
after scale-to-zero blocks during worker boot (cold start) — set the
queue timeout accordingly.

REST API v2 (api.runpod.io/v2):
  POST   /v2/pods                    deploy (snake_case payload)
  GET    /v2/pods                    list (paginated: {"pods": [...], "pagination": {...}})
  POST   /v2/pods/{id}/action        lifecycle: {"action": "start"|"stop"|"restart"|"terminate"}
  GET    /v2/serverless              list endpoints (paginated)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import shlex
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import httpx

from . import llamaserver
from .config import GatewayConfig

logger = logging.getLogger(__name__)

REST_BASE = "https://api.runpod.io/v2"
SERVERLESS_URL = "https://api.runpod.ai/v2/{endpoint_id}/openai/v1"
SERVERLESS_API = "https://api.runpod.ai/v2/{endpoint_id}"
PUBLIC_API = "https://api.runpod.ai/v2/{model_slug}"


class RunPodError(Exception):
    """RunPod API failure or pod never became healthy — maps to 502."""


class RunPodWarmingUp(RunPodError):
    """Pod exists and is still BOOTING when the wait budget ran out
    (image pull / weights download / load). Distinct from a hard
    failure: the caller should surface 503 + Retry-After so clients
    retry instead of treating the model as broken."""


class RunPodStreamUnavailable(RunPodError):
    """GET /stream/{id} rejected — endpoint/worker without stream support;
    callers may fall back to collect-then-emit (runsync)."""


# Non-streaming requests hold at most this long while a cold pod boots
# (streaming holds up to the spec's start_timeout_s but emits SSE
# keep-alive comments, so it can afford the longer wait).
WARMUP_MAX_HOLD_S = 300.0


# 429 backoff (docs: "Implement retry logic with exponential backoff" —
# rate limits are dynamic, scaling with the endpoint's worker count)
_RETRY_429_ATTEMPTS = 3
_RETRY_429_BASE_S = 0.5


async def _request(method: str, url: str, hdr: dict[str, str],
                   timeout: float, json_body: dict | None = None) -> httpx.Response:
    """One HTTP call with 429 exponential-backoff retry."""
    delay = _RETRY_429_BASE_S
    r = None
    for attempt in range(_RETRY_429_ATTEMPTS + 1):
        async with httpx.AsyncClient(timeout=timeout) as ac:
            r = await (ac.post(url, json=json_body, headers=hdr)
                       if method == "POST" else ac.get(url, headers=hdr))
        if r.status_code != 429 or attempt == _RETRY_429_ATTEMPTS:
            return r
        await asyncio.sleep(delay)
        delay *= 2
    return r  # unreachable


async def _cancel_best_effort(base: str, job_id: str | None,
                              hdr: dict[str, str]) -> None:
    """POST /cancel/{id} — fire and forget (a timed-out queued job would
    otherwise keep consuming worker time)."""
    if not job_id:
        return
    try:
        await _request("POST", f"{base}/cancel/{job_id}", hdr, timeout=15.0)
    except Exception:  # noqa: BLE001 — cleanup must never mask the real error
        logger.warning("runpod cancel %s failed", job_id, exc_info=True)


# ── REST client (module-level functions so tests can monkeypatch) ──────────

def _api_key(api_key_env: str) -> str:
    import os

    key = os.environ.get(api_key_env, "")
    if not key:
        raise RunPodError(
            f"runpod api key env var {api_key_env!r} is not set")
    return key


async def _rest(method: str, path: str, api_key_env: str,
                json_body: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=60.0) as ac:
        r = await ac.request(method, f"{REST_BASE}{path}",
                             json=json_body,
                             headers={"Authorization": f"Bearer {_api_key(api_key_env)}"})
    if r.status_code >= 400:
        raise RunPodError(f"runpod {method} {path} -> {r.status_code}: "
                          f"{r.text[:800]}")
    return r.json() if r.content else {}


async def deploy_pod(payload: dict[str, Any], api_key_env: str) -> dict[str, Any]:
    """POST /v2/pods → the created pod object (id expected)."""
    out = await _rest("POST", "/pods", api_key_env, payload)
    pod = out if isinstance(out, dict) else {}
    if not (pod.get("id") or (isinstance(pod.get("pod"), dict) and pod["pod"].get("id"))):
        raise RunPodError(f"runpod deploy returned no pod id: {str(out)[:300]}")
    return pod.get("pod") if isinstance(pod.get("pod"), dict) else pod


async def list_pods(api_key_env: str) -> list[dict[str, Any]]:
    """GET /v2/pods → pod list (handles paginated response: {"pods": [...], "pagination": {...}})."""
    out = await _rest("GET", "/pods", api_key_env)
    if isinstance(out, list):
        return [p for p in out if isinstance(p, dict)]
    if isinstance(out, dict):
        for key in ("pods", "data", "items"):
            if isinstance(out.get(key), list):
                return [p for p in out[key] if isinstance(p, dict)]
    return []


async def list_endpoints(api_key_env: str) -> list[dict[str, Any]]:
    """GET /v2/serverless → endpoint list (handles paginated response)."""
    out = await _rest("GET", "/serverless", api_key_env)
    if isinstance(out, list):
        return [e for e in out if isinstance(e, dict)]
    if isinstance(out, dict):
        for key in ("endpoints", "data", "items", "serverless"):
            if isinstance(out.get(key), list):
                return [e for e in out[key] if isinstance(e, dict)]
    return []


async def start_pod(pod_id: str, api_key_env: str) -> Any:
    return await _rest("POST", f"/pods/{pod_id}/action", api_key_env, {"action": "start"})


async def stop_pod(pod_id: str, api_key_env: str) -> Any:
    return await _rest("POST", f"/pods/{pod_id}/action", api_key_env, {"action": "stop"})


async def terminate_pod(pod_id: str, api_key_env: str) -> Any:
    return await _rest("POST", f"/pods/{pod_id}/action", api_key_env, {"action": "terminate"})


async def delete_endpoint(endpoint_id: str, api_key_env: str) -> Any:
    """DELETE /v2/serverless/{id} — permanently deletes a serverless
    endpoint: terminates its workers, cancels queued/active jobs, and
    removes its bound template."""
    return await _rest("DELETE", f"/serverless/{endpoint_id}", api_key_env)


# ── Catalog / volume REST (live option surface for the deploy wizard) ──────

def _first_list(out: Any, *keys: str) -> list[dict[str, Any]]:
    """Unwrap a REST response that may be a bare list or {key: [...]}."""
    if isinstance(out, list):
        return [x for x in out if isinstance(x, dict)]
    if isinstance(out, dict):
        for key in keys:
            if isinstance(out.get(key), list):
                return [x for x in out[key] if isinstance(x, dict)]
    return []


async def list_gpu_catalog(api_key_env: str) -> list[dict[str, Any]]:
    """GET /v2/catalog/gpus — live GPU types: id, memory (GB), price
    {community, secure, serverless}, maxCount {community, secure}."""
    return _first_list(await _rest("GET", "/catalog/gpus", api_key_env),
                       "gpus", "data", "items")


async def list_datacenter_catalog(api_key_env: str) -> list[dict[str, Any]]:
    """GET /v2/catalog/datacenters — DCs with region, compliance and
    networkVolumeTypes ([] = volumes unsupported there)."""
    return _first_list(await _rest("GET", "/catalog/datacenters", api_key_env),
                       "dataCenters", "datacenters", "data", "items")


async def list_volumes(api_key_env: str) -> list[dict[str, Any]]:
    """GET /v2/network-volumes — the account's network volumes (id,
    name, dataCenter, size GB, type). Network volumes are DC-pinned."""
    return _first_list(await _rest("GET", "/network-volumes", api_key_env),
                       "networkVolumes", "volumes", "data", "items")


async def create_volume(name: str, data_center: str, size_gb: int,
                        vol_type: str, api_key_env: str) -> dict[str, Any]:
    """POST /v2/network-volumes {name, dataCenter, size, type} — type
    STANDARD or HIGH_PERFORMANCE (3x throughput; only some DCs offer
    it — check list_datacenter_catalog)."""
    return await _rest("POST", "/network-volumes", api_key_env, {
        "name": name, "dataCenter": data_center,
        "size": int(size_gb), "type": vol_type or "STANDARD",
    })


async def delete_volume(volume_id: str, api_key_env: str) -> Any:
    """DELETE /v2/network-volumes/{id} — the volume must not be mounted
    by a running pod (RunPod refuses; callers surface that error)."""
    return await _rest("DELETE", f"/network-volumes/{volume_id}",
                       api_key_env)


async def list_pod_billing(api_key_env: str, days: int = 7) -> dict[str, Any]:
    """GET /v2/billing/pods — per-pod billing history. Response shape
    varies by account state ({} / {pods: [...]} / {billing: [...]});
    returned as-is and rendered defensively by the console."""
    out = await _rest("GET", "/billing/pods", api_key_env)
    if isinstance(out, dict):
        return out
    return {"items": out}


def pod_state(pod: dict[str, Any]) -> str:
    """Best-effort pod state: status / desiredStatus fields vary by API
    version — upper-cased when present, else 'unknown'."""
    for key in ("status", "state", "desiredStatus"):
        val = pod.get(key)
        if val:
            return str(val).upper()
    return "UNKNOWN"


async def pod_healthy(base_url: str) -> bool:
    """A pod is serving when EITHER endpoint answers 200: llama.cpp's
    /health, or /v1/models (Ollama — no /health — and every other
    OpenAI-compatible image answers there)."""
    async with httpx.AsyncClient(timeout=10.0) as ac:
        for path in ("/health", "/v1/models"):
            try:
                r = await ac.get(f"{base_url}{path}")
                if r.status_code == 200:
                    return True
            except httpx.HTTPError:
                continue
    return False


async def pod_health_details(base_url: str) -> dict[str, Any]:
    """Diagnostic variant of pod_healthy: per-probe status + latency,
    plus the model ids the pod is serving. 'healthy' semantics are
    identical to pod_healthy (either probe 200)."""
    out: dict[str, Any] = {"healthy": False, "health": None, "models": None}
    async with httpx.AsyncClient(timeout=10.0) as ac:
        t0 = time.monotonic()
        try:
            r = await ac.get(f"{base_url}/health")
            out["health"] = {"status": r.status_code,
                             "ms": round((time.monotonic() - t0) * 1000)}
        except httpx.HTTPError as e:
            out["health"] = {"error": type(e).__name__}
        t0 = time.monotonic()
        try:
            r = await ac.get(f"{base_url}/v1/models")
            entry: dict[str, Any] = {
                "status": r.status_code,
                "ms": round((time.monotonic() - t0) * 1000)}
            if r.status_code == 200:
                ids: list[str] = []
                try:
                    for m in (r.json().get("data") or [])[:8]:
                        ids.append(str(m.get("id")))
                except Exception:  # noqa: BLE001 — body shape varies by image
                    pass
                entry["ids"] = ids
            out["models"] = entry
        except httpx.HTTPError as e:
            out["models"] = {"error": type(e).__name__}
    out["healthy"] = bool((out["health"] or {}).get("status") == 200
                          or (out["models"] or {}).get("status") == 200)
    return out


async def fetch_pod_logs(pod_id: str, api_key_env: str,
                         tail: int = 100) -> list[dict[str, Any]]:
    """GET /v2/pods/{id}/logs?tail=N — SSE (id:/data: lines; data is
    JSON with ts/source/line). The stream stays open for NEW lines, so
    the read is bounded: at most ``tail`` events or ~8s, whichever comes
    first. Raises RunPodError on HTTP >= 400; transport errors raise only
    when nothing was collected (partial logs still count)."""
    headers = {"Authorization": f"Bearer {_api_key(api_key_env)}"}
    url = f"{REST_BASE}/pods/{pod_id}/logs?tail={int(tail)}"
    events: list[dict[str, Any]] = []
    deadline = time.monotonic() + 8.0
    try:
        async with httpx.AsyncClient(timeout=10.0) as ac:
            async with ac.stream("GET", url, headers=headers) as r:
                if r.status_code >= 400:
                    body = (await r.aread()).decode("utf-8", "replace")
                    raise RunPodError(f"runpod pod logs {r.status_code}: "
                                      f"{body[:300]}")
                async for line in r.aiter_lines():
                    if len(events) >= tail or time.monotonic() > deadline:
                        break
                    if not line.startswith("data: "):
                        continue
                    try:
                        ev = json.loads(line[6:])
                        events.append({"ts": ev.get("ts"),
                                       "source": ev.get("source"),
                                       "line": str(ev.get("line"))[:400]})
                    except (ValueError, TypeError):
                        continue
    except httpx.HTTPError as e:
        if not events:
            raise RunPodError(f"runpod pod logs fetch failed: {e}") from e
    return events


# Whitelisted pod-object fields for diagnostics — the v2 schema varies,
# only what exists is reported. Additionally pod_info() normalizes the
# two schemas seen in the wild (deploy response vs pods-list object):
# gpuTypeId ← gpuTypeId | gpu.id, costPerHour ← costPerHour | cost,
# uptimeSeconds ← uptimeSeconds | startedAt, imageName ← image.
_POD_INFO_KEYS = ("id", "name", "status", "desiredStatus", "imageName",
                  "image", "gpuTypeId", "costPerHour", "containerDiskInGb",
                  "volumeInGb", "cloudType", "uptimeSeconds", "licenseFee",
                  "actions", "cloud", "dataCenterId")


def pod_info(pod: dict[str, Any] | None) -> dict[str, Any]:
    """Trimmed pod object for diagnostics (schema-tolerant, normalized)."""
    if not isinstance(pod, dict):
        return {}
    out = {k: pod[k] for k in _POD_INFO_KEYS if k in pod}
    gpu = pod.get("gpu")
    if isinstance(gpu, dict):
        out["gpu"] = gpu.get("id") or gpu.get("type") or gpu
        if isinstance(gpu.get("count"), int):
            out["gpuCount"] = gpu["count"]
    elif isinstance(gpu, str):
        out["gpu"] = gpu
    if "gpuTypeId" not in out and isinstance(out.get("gpu"), str):
        out["gpuTypeId"] = out["gpu"]
    if "costPerHour" not in out and pod.get("cost") is not None:
        try:
            out["costPerHour"] = float(pod["cost"])
        except (TypeError, ValueError):
            pass
    if "uptimeSeconds" not in out and pod.get("startedAt"):
        try:  # "2026-09-20T06:11:16.12Z" — fromisoformat needs +00:00
            started = datetime.fromisoformat(
                str(pod["startedAt"]).replace("Z", "+00:00"))
            out["uptimeSeconds"] = max(0, int(
                (datetime.now(timezone.utc) - started).total_seconds()))
        except ValueError:
            pass
    if "imageName" not in out and pod.get("image"):
        out["imageName"] = pod["image"]
    runtime = pod.get("runtime")
    if isinstance(runtime, dict) and isinstance(runtime.get("gpus"), list):
        utils = [g.get("util") for g in runtime["gpus"]
                 if isinstance(g, dict)
                 and isinstance(g.get("util"), (int, float))]
        if utils:
            out["gpuUtil"] = max(utils)
        mems = [g.get("memoryUtil") for g in runtime["gpus"]
                if isinstance(g, dict)
                and isinstance(g.get("memoryUtil"), (int, float))]
        if mems:
            out["gpuMemUtil"] = max(mems)
    return out


# ── Deploy payload builder ─────────────────────────────────────────────────

def _pod_name(backend_name: str, model_key: str) -> str:
    tag = re.sub(r"[^a-z0-9-]+", "-",
                 f"gw-{backend_name}-{model_key}".lower()).strip("-")
    return tag[:60]


def _normalize_ports(ports: Any) -> list[str]:
    """v2 wants an ARRAY. Config specs store "8080/http" (string) —
    the config loader normalizes on reload, but specs built at runtime
    (deploy wizard, /admin/runpod/deploy) bypass that path, so the
    string reached the API verbatim and RunPod 422'd ($.ports: got
    string, want array — live-verified via the catalog deploy E2E)."""
    if isinstance(ports, list):
        return [str(p).strip() for p in ports if str(p).strip()]
    return [p.strip() for p in str(ports or "").split(",") if p.strip()]


def deploy_payload(backend_name: str, model_key: str, spec: dict[str, Any],
                   gpu_id: str | None = None) -> dict[str, Any]:
    """Config pod spec → POST /v2/pods payload (snake_case, nested gpu).

    gpu_id overrides the FIRST gpu_type_ids entry — the deploy retry
    loop in _PodManager.ensure_ready walks the list when a type has no
    free hosts (RunPod 400 "no longer available with the requested
    specifications" — observed live with RTX A4000 in EUR-IS-1).

    v2 schema changes from v1:
    - imageName → image
    - gpuCount + gpuTypeIds → gpu: {id, count}
    - containerDiskInGb → disk
    - dockerStartCmd (array) → cmd (array) or args (string)
    - dockerEntrypoint → entrypoint
    - volumeInGb + volumeMountPath → mounts: {persistent: {size, path}}
    - env: array of {key, value} → object {key: value}
    - interruptible → cloud: "COMMUNITY" (spot) vs "SECURE" (default)

    docker_cmd (FULL command incl. the binary, e.g. "/llama-server -m ...")
    wins; docker_args passes through BARE (no binary). The official
    ghcr.io/ggml-org/llama.cpp:server-cuda image declares its server
    via ENTRYPOINT, so the pod's CMD is APPENDED — prefixing the
    binary produced "/llama-server /llama-server -hf ..." and a
    ~17s crash loop with "error: invalid argument: /llama-server"
    (live-verified from the container logs). docker_entrypoint (array)
    passes through for full control."""
    payload: dict[str, Any] = {
        "name": _pod_name(backend_name, model_key),
        "image": spec.get("image"),
        "ports": _normalize_ports(spec.get("ports")),
        "disk": spec.get("container_disk_gb"),
        "env": _normalize_env(spec.get("env")),
    }
    # GPU spec: nested object {id, count}
    gpu_type_ids = spec.get("gpu_type_ids")
    if gpu_id:
        payload["gpu"] = {
            "id": gpu_id,
            "count": spec.get("gpu_count") or 1,
        }
    elif gpu_type_ids:
        # v2 takes a single GPU id; use first from list (scheduler picks)
        gpu_id = gpu_type_ids[0] if isinstance(gpu_type_ids, list) else gpu_type_ids
        payload["gpu"] = {
            "id": gpu_id,
            "count": spec.get("gpu_count") or 1,
        }
    elif spec.get("gpu_count"):
        payload["gpu"] = {"count": spec["gpu_count"]}
    # Container command
    if spec.get("docker_cmd"):
        payload["cmd"] = shlex.split(spec["docker_cmd"])
    elif spec.get("docker_script"):
        # ONE argv element, VERBATIM — for shell entrypoints. shlex
        # splitting shreds a script: with entrypoint ["sh","-c"] the
        # container ran `sh -c "(curl" -fsSL ...` and dash died with
        # '-fsSL: 1: Syntax error: end of file unexpected' (live-
        # verified). The whole script must reach -c as one argument.
        payload["cmd"] = [spec["docker_script"]]
    elif spec.get("docker_args"):
        payload["cmd"] = shlex.split(spec["docker_args"])
    if spec.get("docker_entrypoint"):
        payload["entrypoint"] = list(spec["docker_entrypoint"])
    # Volume mounts: network volume by id (preferred — survives pod
    # termination) or a create-time persistent volume by size. Mutually
    # exclusive in the v2 API; volume_id wins.
    if spec.get("volume_id"):
        payload["mounts"] = {
            "network": [{"volumeId": spec["volume_id"],
                         "path": spec.get("volume_mount") or "/workspace"}]
        }
    elif spec.get("volume_gb"):
        payload["mounts"] = {
            "persistent": {
                "size": spec["volume_gb"],
                "path": spec.get("volume_mount") or "/workspace",
            }
        }
    # Spot → cloud: COMMUNITY
    if spec.get("spot"):
        payload["cloud"] = "COMMUNITY"
    # Preferred data centers — network volumes are DC-pinned, so the pod
    # must land in the volume's DC (scheduler otherwise picks freely).
    if spec.get("data_center_ids"):
        payload["dataCenterIds"] = list(spec["data_center_ids"])
    # Clean up None/empty values
    payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
    payload.update(spec.get("deploy") or {})  # raw escape hatch wins
    return payload


def _normalize_env(env) -> dict[str, str] | None:
    """Normalize env from v1 array format [{key, value}] to v2 object {key: value}."""
    if not env:
        return None
    if isinstance(env, dict):
        return env
    if isinstance(env, list):
        return {item.get("key", item.get("name")): item.get("value", "")
                for item in env if isinstance(item, dict)}
    return None


# ── Serverless URL builder ─────────────────────────────────────────────────

def serverless_base_url(endpoint_id: str) -> str:
    return SERVERLESS_URL.format(endpoint_id=endpoint_id)


# ── Serverless RAW job protocol (transport "runsync") ──────────────────────

async def runsync_chat(endpoint_id: str, body_input: dict[str, Any],
                       api_key_env: str, timeout: float = 120.0,
                       policy: dict[str, Any] | None = None) -> Any:
    """POST /v2/<ep>/runsync with {"input": {...}} (+ optional ``policy``
    — executionTimeout/ttl/lowPriority, per the docs); poll /status/{id}
    while the job queues or runs (scale-to-zero cold start). ?wait= keeps
    RunPod itself waiting up to 300s before bouncing IN_QUEUE. On OUR
    timeout the queued job is cancelled (best-effort /cancel). Returns
    the job's OUTPUT verbatim (worker-specific shape — map with
    ollama_output_to_openai or your own)."""
    base = SERVERLESS_API.format(endpoint_id=endpoint_id)
    hdr = {"Authorization": f"Bearer {_api_key(api_key_env)}"}
    # docs: ?wait is 1000-300000 ms — RunPod holds the sync window open
    wait_ms = max(1000, min(int(timeout * 1000), 300000))
    payload: dict[str, Any] = {"input": body_input}
    if isinstance(policy, dict) and policy:
        payload["policy"] = policy
    r = await _request("POST", f"{base}/runsync?wait={wait_ms}", hdr,
                       timeout, payload)
    if r.status_code >= 400:
        raise RunPodError(f"runpod serverless runsync -> {r.status_code}: "
                          f"{r.text[:300]}")
    job = r.json()
    deadline = time.monotonic() + timeout
    while job.get("status") in ("IN_QUEUE", "IN_PROGRESS"):
        if time.monotonic() >= deadline:
            await _cancel_best_effort(base, job.get("id"), hdr)
            raise RunPodError(f"runpod serverless job {job.get('id')} still "
                              f"{job.get('status')} after {timeout:.0f}s "
                              "(cancelled)")
        await asyncio.sleep(1.0)
        r = await _request("GET", f"{base}/status/{job.get('id')}", hdr,
                           timeout)
        if r.status_code >= 400:
            raise RunPodError(f"runpod serverless status -> {r.status_code}: "
                              f"{r.text[:300]}")
        job = r.json()
    if job.get("status") != "COMPLETED":
        raise RunPodError(f"runpod serverless job {job.get('status')}: "
                          f"{str(job.get('error'))[:300]}")
    return job.get("output")


async def runsync_public_chat(model_slug: str, body_input: dict[str, Any],
                               api_key_env: str, timeout: float = 120.0,
                               policy: dict[str, Any] | None = None) -> Any:
    """POST /v2/<model-slug>/runsync — Public Endpoints (pre-deployed,
    always warm). Same runsync protocol as user serverless but the URL
    uses a model slug (e.g. 'black-forest-labs-flux-1-schnell') instead
    of an endpoint id. No cold-start — no polling loop needed, single
    request round-trip."""
    base = PUBLIC_API.format(model_slug=model_slug)
    hdr = {"Authorization": f"Bearer {_api_key(api_key_env)}"}
    wait_ms = max(1000, min(int(timeout * 1000), 300000))
    payload: dict[str, Any] = {"input": body_input}
    if isinstance(policy, dict) and policy:
        payload["policy"] = policy
    r = await _request("POST", f"{base}/runsync?wait={wait_ms}", hdr,
                       timeout, payload)
    if r.status_code >= 400:
        raise RunPodError(f"runpod public endpoint {model_slug!r} -> "
                          f"{r.status_code}: {r.text[:300]}")
    job = r.json()
    # Public endpoints are always warm — should complete in one shot.
    # Poll briefly if still in progress (defensive).
    deadline = time.monotonic() + timeout
    while job.get("status") in ("IN_QUEUE", "IN_PROGRESS"):
        if time.monotonic() >= deadline:
            raise RunPodError(
                f"runpod public endpoint {model_slug!r} job "
                f"{job.get('id')} still {job.get('status')} after "
                f"{timeout:.0f}s")
        await asyncio.sleep(1.0)
        r = await _request("GET", f"{base}/status/{job.get('id')}", hdr,
                           timeout)
        if r.status_code >= 400:
            raise RunPodError(
                f"runpod public endpoint status -> {r.status_code}: "
                f"{r.text[:300]}")
        job = r.json()
    if job.get("status") != "COMPLETED":
        raise RunPodError(
            f"runpod public endpoint {model_slug!r} job "
            f"{job.get('status')}: {str(job.get('error'))[:300]}")
    return job.get("output")


async def run_job(endpoint_id: str, body_input: dict[str, Any],
                  api_key_env: str, timeout: float = 120.0,
                  policy: dict[str, Any] | None = None) -> str:
    """POST /v2/<ep>/run (async) → job id. Pair with stream_job for
    incremental results."""
    base = SERVERLESS_API.format(endpoint_id=endpoint_id)
    payload: dict[str, Any] = {"input": body_input}
    if isinstance(policy, dict) and policy:
        payload["policy"] = policy
    r = await _request("POST", f"{base}/run", hdr={
        "Authorization": f"Bearer {_api_key(api_key_env)}"},
        timeout=timeout, json_body=payload)
    if r.status_code >= 400:
        raise RunPodError(f"runpod serverless run -> {r.status_code}: "
                          f"{r.text[:300]}")
    job = r.json()
    if not job.get("id"):
        raise RunPodError(f"runpod serverless run returned no id: "
                          f"{str(job)[:200]}")
    return str(job["id"])


async def stream_job(endpoint_id: str, job_id: str, api_key_env: str,
                     timeout: float = 300.0) -> AsyncIterator[Any]:
    """GET /v2/<ep>/stream/{job_id} — SSE of incremental output chunks as
    the worker yields them (docs: single chunks up to 1 MB). Yields each
    chunk verbatim; raises when the job FAILED."""
    base = SERVERLESS_API.format(endpoint_id=endpoint_id)
    hdr = {"Authorization": f"Bearer {_api_key(api_key_env)}"}
    async with httpx.AsyncClient(timeout=timeout) as ac:
        async with ac.stream("GET", f"{base}/stream/{job_id}",
                             headers=hdr) as r:
            if r.status_code >= 400:
                body = (await r.aread()).decode("utf-8", "replace")
                raise RunPodStreamUnavailable(
                    f"runpod serverless stream -> {r.status_code}: "
                    f"{body[:300]}")
            async for line in r.aiter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if not payload:
                    continue
                try:
                    ev = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    continue
                if not isinstance(ev, dict):
                    continue
                status = ev.get("status")
                if status == "FAILED":
                    raise RunPodError(f"runpod serverless job FAILED: "
                                      f"{str(ev.get('error'))[:300]}")
                out = ev.get("output")
                if out is not None:
                    yield out
                if status == "COMPLETED":
                    return


def ollama_chunk_content(chunk: Any) -> tuple[str, bool, str]:
    """One worker output chunk (Ollama shape: dict or JSON string) →
    (content, done, done_reason). Tolerates anything else → ('', ...)."""
    if isinstance(chunk, str):
        try:
            chunk = json.loads(chunk)
        except (json.JSONDecodeError, ValueError):
            return "", False, ""
    if not isinstance(chunk, dict):
        return "", False, ""
    content = (chunk.get("message") or {}).get("content") or ""
    return content, bool(chunk.get("done")), str(chunk.get("done_reason") or "")


def ollama_generate_output_to_openai(output: Any, model_echo: str) -> dict[str, Any]:
    """RunPod Ollama-worker GENERATE output (input {"prompt": ...}) →
    OpenAI text.completion. Handles the single-dict shape ({response,
    eval_count, ...}) and a list of chunk dicts/JSON-strings; chunks carry
    incremental "response" pieces, the done chunk the stats."""
    parts: list[str] = []
    final: dict[str, Any] = {}
    for it in (output if isinstance(output, list) else [output]):
        if isinstance(it, str):
            try:
                it = json.loads(it)
            except (json.JSONDecodeError, ValueError):
                continue
        if not isinstance(it, dict):
            continue
        if it.get("response"):
            parts.append(it["response"])
        if it.get("done"):
            final = it
    pt = int(final.get("prompt_eval_count") or 0)
    ct = int(final.get("eval_count") or 0)
    return {
        "id": f"cmpl-rp-{time.time_ns():x}",
        "object": "text.completion",
        "created": int(time.time()),
        "model": model_echo,
        "choices": [{
            "index": 0,
            "text": "".join(parts),
            "finish_reason": final.get("done_reason") or "stop",
        }],
        "usage": {"prompt_tokens": pt, "completion_tokens": ct,
                  "total_tokens": pt + ct},
    }


def ollama_output_to_openai(output: Any, model_echo: str) -> dict[str, Any]:
    """RunPod Ollama-worker output → OpenAI chat.completion. Handles BOTH
    worker shapes: a single Ollama chat dict ({message, eval_count,
    prompt_eval_count, done_reason}) or a LIST of Ollama chunk JSON strings
    (stream:true collection — the last done chunk carries the stats)."""
    import uuid

    parts: list[str] = []
    final: dict[str, Any] = {}
    for it in (output if isinstance(output, list) else [output]):
        if isinstance(it, str):
            try:
                it = json.loads(it)
            except (json.JSONDecodeError, ValueError):
                continue
        if not isinstance(it, dict):
            continue
        msg = it.get("message") or {}
        if msg.get("content"):
            parts.append(msg["content"])
        if it.get("done"):
            final = it
    pt = int(final.get("prompt_eval_count") or 0)
    ct = int(final.get("eval_count") or 0)
    return {
        "id": f"chatcmpl-rp-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_echo,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "".join(parts)},
            "finish_reason": final.get("done_reason") or "stop",
        }],
        "usage": {"prompt_tokens": pt, "completion_tokens": ct,
                  "total_tokens": pt + ct},
    }


# ── Pod manager ────────────────────────────────────────────────────────────

class _PodManager:
    """Lifecycle state for ONE runpod pod-backed model."""

    def __init__(self, backend_name: str, model_key: str,
                 api_key_env: str, spec: dict[str, Any]):
        self.backend_name = backend_name
        self.model_key = model_key
        self.api_key_env = api_key_env
        self.spec = spec
        self.lock = asyncio.Lock()
        self.pod_id: str | None = None
        self.pod_status: str = "unknown"
        self.ready = False
        self.last_used = time.monotonic()
        self.deployed_s: float | None = None
        # warmup visibility: an ensure_ready is in flight (booting or
        # waiting on the lock behind one) — /admin/models + the console
        # surface this as "warming up"
        self.warming: bool = False
        self.warm_started_s: float | None = None
        # diagnostics (set by ensure_ready; read by /admin/runpod/test)
        self.last_action: str = ""      # reused|pinned|found|resumed|deployed
        self.last_pod_info: dict[str, Any] = {}
        # v3 lifecycle: in-flight request count — the idle rule never
        # fires while a request (incl. a long SSE stream) is live
        self.inflight: int = 0
        # monotonic first RUNNING sighting for THIS boot attempt — the
        # never-healthy reaper clock; reset when the pod isn't running
        self.first_seen_s: float | None = None

    def base_url(self) -> str:
        return f"https://{self.pod_id}-{self.spec['port']}.proxy.runpod.net"

    def touch(self) -> None:
        self.last_used = time.monotonic()

    @asynccontextmanager
    async def request(self):
        """One live request: touch + inflight guard. Wrap the WHOLE
        request — ensure_ready through the final proxied byte — so the
        lifecycle watchdog can't idle-stop the pod mid-flight. Inside
        an async generator the finally runs on completion OR client
        disconnect, which is exactly the semantics streams need."""
        self.touch()
        self.inflight += 1
        try:
            yield self
        finally:
            self.inflight -= 1

    def is_ready(self) -> bool:
        return self.ready and self.pod_id is not None

    async def _find_pod(self) -> dict[str, Any] | None:
        want = _pod_name(self.backend_name, self.model_key)
        for p in await list_pods(self.api_key_env):
            if p.get("name") == want or p.get("id") == self.pod_id:
                return p
        return None

    async def find_pod(self) -> dict[str, Any] | None:
        """Public find-by-name/pod-id — probe-only diagnostics use this
        WITHOUT the deploy/resume side effects of ensure_ready."""
        return await self._find_pod()

    async def ensure_ready(self, timeout_s: float | None = None) -> str:
        """Pinned pod (config pod_id) → use it directly; else find-by-name
        → resume stopped → deploy new. Then health-poll the proxy URL.
        Returns the proxy base URL. timeout_s overrides the spec's
        start_timeout_s for ONE call (diagnostics want tight bounds).
        Raises RunPodWarmingUp when the wait budget expires while the
        pod is still booting (alive, not failed) — callers map that to
        503 + Retry-After instead of 502."""
        self.touch()
        if not self.ready and not self.warming:
            self.warming = True
            self.warm_started_s = time.monotonic()
        try:
            return await self._ensure_ready_locked(timeout_s)
        finally:
            # Still-locked means another request is resuming/deploying
            # behind this one — keep the warming badge lit for it.
            self.warming = self.lock.locked()
            if not self.warming and self.ready:
                self.warm_started_s = None

    async def _ensure_ready_locked(self, timeout_s: float | None) -> str:
        async with self.lock:
            if self.is_ready():
                self.last_action = "reused"
                return self.base_url()
            pinned = str(self.spec.get("pod_id") or "").strip()
            if pinned:
                # BYO pod — never deployed or renamed by the gateway
                self.pod_id = pinned
                pod = await self._find_pod()
                self.last_pod_info = pod_info(pod)
                self.last_action = "pinned"
                if pod is None:
                    raise RunPodError(
                        f"runpod pod {pinned!r} (pod_id in config) not found")
                if pod_state(pod) not in ("RUNNING", "READY", "UNKNOWN"):
                    logger.info("runpod pod %s (pinned) is %s — starting",
                                pinned, pod_state(pod))
                    await start_pod(pinned, self.api_key_env)
                    self.last_action = "pinned-resumed"
            else:
                pod = await self._find_pod()
                self.last_pod_info = pod_info(pod)
                if pod is None:
                    # Deploy — walking gpu_type_ids on capacity errors:
                    # RunPod 400s ("no longer available with the requested
                    # specifications") when a type has no free hosts, and
                    # the list exists precisely to give the scheduler
                    # alternatives (observed live: A4000 dry in EUR-IS-1,
                    # RTX 3090 next in line).
                    last_err: Exception | None = None
                    types = self.spec.get("gpu_type_ids")
                    gpu_ids = ([t for t in types if isinstance(t, str)]
                               if isinstance(types, list) else [None])
                    for gid in gpu_ids:
                        payload = deploy_payload(
                            self.backend_name, self.model_key, self.spec,
                            gpu_id=gid)
                        logger.info("runpod deploy [%s/%s] (gpu %s): %s",
                                    self.backend_name, self.model_key,
                                    gid or "any", payload)
                        try:
                            pod = await deploy_pod(payload, self.api_key_env)
                            break
                        except RunPodError as e:
                            last_err = e
                            logger.warning("runpod deploy with gpu %s "
                                           "failed: %s", gid or "any", e)
                            continue
                    if pod is None:
                        raise last_err or RunPodError(
                            f"runpod deploy failed for {self.model_key!r}")
                    self.deployed_s = time.monotonic()
                    self.last_pod_info = pod_info(pod)
                    self.last_action = "deployed"
                else:
                    self.last_action = "found"
                self.pod_id = str(pod.get("id"))
                if pod_state(pod) not in ("RUNNING", "READY", "UNKNOWN"):
                    logger.info("runpod pod %s is %s — starting",
                                self.pod_id, pod_state(pod))
                    await start_pod(self.pod_id, self.api_key_env)
                    self.last_action = "resumed"
            # health-poll the proxy URL (llama.cpp /health, Ollama
            # /v1/models); pods take 30s–minutes (image pull + download)
            start_to = float(
                timeout_s if timeout_s is not None
                else (self.spec.get("start_timeout_s") or 600))
            deadline = time.monotonic() + start_to
            while time.monotonic() < deadline:
                pod_now = await self._find_pod()
                # Stale-pod guard: terminate is ASYNC — a terminating pod
                # lingers in the list long enough for find-by-name to
                # adopt it, then it vanishes and the poll keeps hitting
                # its dead proxy URL for the full timeout (live-observed
                # three times). Abort the moment our pod is gone or
                # replaced; the NEXT request deploys a fresh pod.
                if pod_now is None or str(pod_now.get("id")) != self.pod_id:
                    raise RunPodError(
                        f"runpod pod {self.pod_id} vanished during startup "
                        "(terminated while health-polling) — retry the "
                        "request to deploy a fresh pod")
                self.pod_status = pod_state(pod_now) if pod_now else "UNKNOWN"
                if self.pod_status in ("EXITED", "FAILED", "TERMINATED", "CRASHED"):
                    raise RunPodError(
                        f"runpod pod {self.pod_id} entered {self.pod_status} "
                        "during startup")
                if await pod_healthy(self.base_url()):
                    self.ready = True
                    logger.info("runpod pod ready [%s/%s] at %s",
                                self.backend_name, self.model_key,
                                self.base_url())
                    return self.base_url()
                await asyncio.sleep(5.0)
            raise RunPodWarmingUp(
                f"runpod pod [{self.backend_name}/{self.model_key}] still "
                f"booting after {start_to:.1f}s (pod {self.pod_id}, state "
                f"{self.pod_status}) — image pull / weights download / "
                "load in progress; retry shortly")

    async def stop(self, terminate: bool) -> None:
        pod_id, self.ready = self.pod_id, False
        if not pod_id:
            return
        try:
            if terminate:
                logger.info("runpod terminate pod %s [%s/%s]", pod_id,
                            self.backend_name, self.model_key)
                await terminate_pod(pod_id, self.api_key_env)
                self.pod_id = None
            else:
                logger.info("runpod stop pod %s [%s/%s]", pod_id,
                            self.backend_name, self.model_key)
                await stop_pod(pod_id, self.api_key_env)
                self.pod_status = "STOPPED"
        except Exception:  # noqa: BLE001 — lifecycle ops must never 500 a request
            logger.warning("runpod stop/terminate %s failed", pod_id, exc_info=True)


# ── Registry + reaper ──────────────────────────────────────────────────────

_MANAGERS: dict[tuple[str, str], _PodManager] = {}
_REAPER_TASK: asyncio.Task | None = None


def get_manager(cfg: GatewayConfig, backend_name: str,
                model_key: str) -> _PodManager:
    """Manager for (backend, model). Spec is re-read every call (hot
    reload-safe); pod identity is by NAME, so it survives gateway
    restarts."""
    backend = cfg.backends[backend_name]
    key = (backend_name, model_key)
    mgr = _MANAGERS.get(key)
    if mgr is None:
        mgr = _PodManager(backend_name, model_key,
                          backend.get("api_key_env") or "RUNPOD_API_KEY",
                          backend["models"][model_key])
        _MANAGERS[key] = mgr
    else:
        mgr.spec = backend["models"][model_key]
    return mgr


async def _reaper_once() -> int:
    """Idle-stop then idle-terminate pods. Returns count acted on."""
    now = time.monotonic()
    n = 0
    for m in list(_MANAGERS.values()):
        if not m.is_ready():
            continue
        idle = now - m.last_used
        if (m.spec.get("idle_terminate_s", 0) > 0
                and idle > m.spec["idle_terminate_s"]):
            await m.stop(terminate=True)
            n += 1
        elif (m.spec.get("idle_stop_s", 0) > 0
                and idle > m.spec["idle_stop_s"]):
            await m.stop(terminate=False)
            n += 1
    return n


async def _reaper_loop() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            await _reaper_once()
        except Exception:  # noqa: BLE001 — the reaper must never die
            logger.warning("runpod reaper error", exc_info=True)


def ensure_reaper() -> None:
    global _REAPER_TASK
    loop = asyncio.get_running_loop()
    if _REAPER_TASK is None or _REAPER_TASK.get_loop() is not loop:
        _REAPER_TASK = loop.create_task(_reaper_loop())


def status() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for (b, k), m in _MANAGERS.items():
        out.append({
            "backend": b, "model": k, "alias": m.spec.get("alias", k),
            "mode": "pod",
            "pod_id": m.pod_id,
            "state": ("warming-up" if (m.warming and not m.ready)
                      else "running" if m.is_ready()
                      else m.pod_status if m.pod_id else "not-deployed"),
            "warming": bool(m.warming and not m.ready),
            "warm_s": (round(time.monotonic() - m.warm_started_s, 1)
                       if (m.warming and not m.ready
                           and m.warm_started_s) else None),
            "base_url": m.base_url() if m.is_ready() else None,
            "idle_s": (round(time.monotonic() - m.last_used, 1)
                       if m.is_ready() else None),
            "inflight": m.inflight,
            "idle_stop_s": m.spec.get("idle_stop_s", 0),
            "idle_terminate_s": m.spec.get("idle_terminate_s", 0),
        })
    return out


# ── Config-driven live status (boot-watch modal polls /healthz) ────────────

_STATUS_STATE: dict[str, Any] = {"pods_at": 0.0, "pods": []}
_STATUS_TTL_S = 10.0


async def refresh_status(cfg) -> list[dict[str, Any]]:
    """Live status for EVERY configured runpod pod model — not just the
    ones a request has already touched. Materializes managers from
    config, seeds pod_id from the spec, refreshes pod state from ONE
    cached pods-list call (10s TTL), and health-probes running-but-not-
    -ready pods so the boot-watch modal sees the full lifecycle
    (not-deployed → starting/warming → running) without a chat request
    first. Deploy-side gap this closes: a freshly deployed model had no
    healthz entry until something called get_manager, so the modal's
    state chip never advanced to the chat-test phase."""
    backends = {n: b for n, b in cfg.backends.items()
                if b.get("type") == "runpod"}
    if not backends:
        return status()
    for name, b in backends.items():
        for mkey, m in (b.get("models") or {}).items():
            if isinstance(m, dict) and not m.get("endpoint_id"):
                get_manager(cfg, name, mkey)
    now = time.monotonic()
    if now - _STATUS_STATE["pods_at"] > _STATUS_TTL_S:
        for name, b in backends.items():
            try:
                _STATUS_STATE["pods"] = await list_pods(
                    b.get("api_key_env") or "RUNPOD_API_KEY")
            except Exception:  # noqa: BLE001 — status must never 500
                logger.warning("runpod refresh_status list failed",
                               exc_info=True)
            _STATUS_STATE["pods_at"] = now
            break
    pods = _STATUS_STATE["pods"]
    for m in list(_MANAGERS.values()):
        if not m.pod_id and m.spec.get("pod_id"):
            m.pod_id = str(m.spec["pod_id"])
        pod = next((p for p in pods
                    if (m.pod_id and p.get("id") == m.pod_id)
                    or p.get("name") == _pod_name(m.backend_name,
                                                  m.model_key)), None)
        if pod is None:
            if m.pod_id:
                m.pod_status = "TERMINATED"
                m.ready = False
                m.first_seen_s = None
            continue
        if not m.pod_id:
            m.pod_id = str(pod.get("id"))
        st = pod_state(pod)
        m.pod_status = st
        if st in ("RUNNING", "READY", "UNKNOWN"):
            if m.first_seen_s is None:
                m.first_seen_s = time.monotonic()
            if not m.ready:
                try:
                    if await pod_healthy(m.base_url()):
                        m.ready = True
                except Exception:  # noqa: BLE001 — probe is best-effort
                    pass
        else:
            m.ready = False
            m.first_seen_s = None
    return status()


def reset_status_cache() -> None:
    """Test helper — drop the cached pods list."""
    _STATUS_STATE["pods"] = []
    _STATUS_STATE["pods_at"] = 0.0


def reset_managers() -> None:
    """Test helper — drop the registry without lifecycle calls."""
    _MANAGERS.clear()
