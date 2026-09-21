"""Local-file upload to a RunPod receiver pod (v4 model finder, D5).

Flow:
  1. POST /admin/models/upload-init  -> reuse a HEALTHY receiver pod or
     deploy one on the cheapest GPU (runpod/pytorch image — python is
     guaranteed, image proven on the glm recipes); it runs a tiny
     stdlib HTTP receiver on port 8081 writing to /workspace/uploads
     (volume-mounted when one is given: uploads persist across stops)
  2. PUT  /admin/models/upload       -> the gateway RELAYS the browser
     stream to the receiver's proxy URL (no CORS issues; the localhost
     hop is free; the home uplink is the only slow leg)
  3. GET  /admin/models/upload-status -> receiver bytes-so-far (the
     browser sees localhost progress instantly — real progress is the
     relay, polled here)
  4. POST /admin/models/upload-finalize -> verify size, return the
     receiver's public /file/<name> URL -> deploy-custom accepts it as
     file_url (the model pod curls it like any repo URL)
  5. POST /admin/models/upload-cleanup -> terminate the receiver

The receiver is a config model ("upload-receiver") so the v3 watchdog
owns it: idle-stop 1800 reaps an abandoned one, the never-healthy
reaper cleans a broken boot, and it shows in the servers widget.
Single-shot uploads in v1 (a failure restarts from scratch — resumable
is parked; the receiver's .part + /status endpoints are the building
blocks).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx

from . import catalog, runpod
from .config import get_gateway_config

logger = logging.getLogger("gateway.upload")

RECEIVER_MODEL = "upload-receiver"
BOOTSTRAP_MODEL = "serverless-bootstrap"
RECEIVER_PORT = 8081
RECEIVER_IMAGE = "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404"
RECEIVER_READY_TIMEOUT_S = 150.0

# shell/URL-safe filenames only — they end up in sh -c scripts and paths
import re as _re

_SAFE_NAME_RE = _re.compile(r"^[A-Za-z0-9._-]+$")


def safe_filename(name: str) -> str | None:
    """A basename that is safe to embed in sh scripts and URLs."""
    import os

    base = os.path.basename((name or "").strip())
    if base and _SAFE_NAME_RE.match(base) and base not in (".", ".."):
        return base
    return None


class UploadError(RuntimeError):
    """Upload-path failure — mapped to 502 by the endpoints."""


def receiver_url(pod_id: str, path: str = "") -> str:
    return f"https://{pod_id}-{RECEIVER_PORT}.proxy.runpod.net/{path}"


# ── the receiver (stdlib-only python; NO single quotes — it is embedded
#    in a single-quoted sh printf) ───────────────────────────────────────────

_RECEIVER_PY = '''
import os, json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
D = "/workspace/uploads"
class H(BaseHTTPRequestHandler):
    def _send(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)
    def do_GET(self):
        try:
            if self.path == "/health":
                self._send(200, b"{\\"ok\\": true}")
            elif self.path == "/list":
                out = []
                if os.path.isdir(D):
                    for n in os.listdir(D):
                        out.append({"name": n,
                                    "size": os.path.getsize(os.path.join(D, n))})
                self._send(200, json.dumps({"files": out}).encode())
            elif self.path.startswith("/status/"):
                n = os.path.basename(self.path[8:])
                p = os.path.join(D, n)
                part = p + ".part"
                if os.path.exists(p):
                    sz = os.path.getsize(p)
                elif os.path.exists(part):
                    sz = os.path.getsize(part)
                else:
                    sz = -1
                self._send(200, json.dumps({"name": n, "size": sz}).encode())
            elif self.path.startswith("/file/"):
                n = os.path.basename(self.path[6:])
                p = os.path.join(D, n)
                if not os.path.exists(p):
                    self._send(404)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(os.path.getsize(p)))
                self.end_headers()
                with open(p, "rb") as f:
                    while True:
                        b = f.read(1 << 20)
                        if not b:
                            break
                        self.wfile.write(b)
            else:
                self._send(404)
        except Exception as e:
            self._send(500, str(e).encode())
    def do_PUT(self):
        try:
            n = os.path.basename(self.path.lstrip("/"))
            if not n or n in (".", ".."):
                self._send(400)
                return
            length = self.headers.get("Content-Length")
            if not length:
                self._send(411, b"{\\"error\\": \\"Content-Length required\\"}")
                return
            os.makedirs(D, exist_ok=True)
            p = os.path.join(D, n)
            length = int(length)
            got = 0
            with open(p + ".part", "wb") as f:
                while got < length:
                    chunk = self.rfile.read(min(1 << 20, length - got))
                    if not chunk:
                        break
                    f.write(chunk)
                    got += len(chunk)
            if got == length:
                os.replace(p + ".part", p)
                self._send(200, json.dumps({"ok": True,
                                            "bytes": got}).encode())
            else:
                self._send(502, json.dumps({"ok": False,
                                            "bytes": got}).encode())
        except Exception as e:
            self._send(500, str(e).encode())
    def log_message(self, *a):
        pass
ThreadingHTTPServer(("0.0.0.0", 8081), H).serve_forever()
'''


def receiver_script() -> str:
    """One-line sh: write the receiver to disk and run it foreground
    (the container lives exactly as long as the receiver serves)."""
    return ("set -e; mkdir -p /workspace/uploads; "
            "printf '%s' '" + _RECEIVER_PY + "' > /root/receiver.py; "
            "exec python3 /root/receiver.py")


def bootstrap_script(file_url: str, filename: str) -> str:
    """Receiver in the BACKGROUND + a foreground curl that downloads
    the model onto the volume. The receiver's /list is the 'done'
    signal (file at the expected size) — the gateway polls it, then
    terminates this pod. sleep infinity keeps the container alive
    until then."""
    return ("set -e; mkdir -p /workspace/uploads; "
            "printf '%s' '" + _RECEIVER_PY + "' > /root/receiver.py; "
            "python3 /root/receiver.py > /root/receiver.log 2>&1 & "
            f"echo '[boot] downloading {filename}'; "
            f"curl -fsSL --retry 3 --max-time 5400 "
            f"-o \"/workspace/uploads/{filename}\" '{file_url}'; "
            "echo '[boot] download done'; "
            "sleep infinity")


def receiver_spec(gpu_id: str, size_bytes: int,
                  volume_id: str = "") -> dict[str, Any]:
    disk = max(20, int(size_bytes / 1e9 * 1.2) + 10)
    spec: dict[str, Any] = {
        "image": RECEIVER_IMAGE,
        "gpu_type_ids": [gpu_id], "gpu_count": 1,
        "docker_entrypoint": ["sh", "-c"],
        "docker_script": receiver_script(),
        "ports": "8081/http", "port": RECEIVER_PORT,
        "container_disk_gb": disk, "start_timeout_s": 600,
        "idle_stop_s": 1800, "idle_terminate_s": 0,
        "alias": RECEIVER_MODEL, "custom": True,
        "source_file": "upload-receiver",
    }
    if volume_id:
        spec["volume_id"] = volume_id
    return spec


def bootstrap_spec(gpu_id: str, file_url: str, filename: str,
                   volume_id: str) -> dict[str, Any]:
    """Receiver + background-download pod writing onto the VOLUME (the
    file must persist after this pod dies — container disk would not)."""
    return {
        "image": RECEIVER_IMAGE,
        "gpu_type_ids": [gpu_id], "gpu_count": 1,
        "docker_entrypoint": ["sh", "-c"],
        "docker_script": bootstrap_script(file_url, filename),
        "ports": "8081/http", "port": RECEIVER_PORT,
        "container_disk_gb": 20, "start_timeout_s": 600,
        "idle_stop_s": 1800, "idle_terminate_s": 0,
        "alias": BOOTSTRAP_MODEL, "custom": True,
        "source_file": "serverless-bootstrap",
        "volume_id": volume_id,
    }


async def probe_receiver(pod_id: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(receiver_url(pod_id, "health"))
            return r.status_code == 200
    except Exception:  # noqa: BLE001 — probe is best-effort
        return False


def _persist_receiver_spec(cfg, backend: str, model_key: str,
                           spec: dict) -> None:
    """Read-modify-write the config (own tiny RMW — no app import, no
    cycles; same pattern as lifecycle._clear_pod_id)."""
    try:
        if cfg.path.is_file():
            raw = json.loads(cfg.path.read_text(encoding="utf-8"))
            models = (raw.setdefault("backends", {})
                      .setdefault(backend, {}).setdefault("models", {}))
            models[model_key] = spec
            cfg.path.write_text(json.dumps(raw, indent=2) + "\n",
                                encoding="utf-8")
            cfg.reload_now()
    except Exception:  # noqa: BLE001 — upload init must still return
        logger.warning("helper spec persist failed", exc_info=True)


def _cheapest_gpu(opts: dict) -> dict[str, Any]:
    priced = [(float((g.get("price") or {}).get("secure") or 0), g)
              for g in opts.get("gpus", [])]
    priced = [(p, g) for p, g in priced if p > 0]
    if not priced:
        raise UploadError("no GPU available for the helper pod")
    return min(priced, key=lambda t: t[0])[1]


async def _deploy_helper(cfg, backend: str, model_key: str,
                         spec: dict) -> str:
    """Deploy a helper pod (receiver / bootstrap) + persist its config
    entry so the v3 watchdog owns its lifecycle."""
    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    payload = runpod.deploy_payload(backend, model_key, spec)
    pod = await runpod.deploy_pod(payload, env)
    pod_id = str(pod.get("id") or "")
    if not pod_id:
        raise UploadError(f"helper deploy returned no id: {pod!r}")
    spec["pod_id"] = pod_id
    _persist_receiver_spec(cfg, backend, model_key, spec)
    return pod_id


async def init_receiver(cfg, backend: str, size_bytes: int,
                        volume_id: str = "") -> dict[str, Any]:
    """A healthy receiver pod_id — reused when possible, else deployed
    on the cheapest GPU. Raises UploadError when the receiver never
    becomes healthy (the config entry stays: the watchdog's never-
    healthy reaper stops a broken boot, idle-stop reaps an idle one)."""
    models = (cfg.backends.get(backend) or {}).get("models") or {}
    existing = models.get(RECEIVER_MODEL) or {}
    old_pod = str(existing.get("pod_id") or "").strip()
    if old_pod and await probe_receiver(old_pod):
        return {"pod_id": old_pod, "reused": True}

    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    opts = await catalog.live_options(env)
    gpu = _cheapest_gpu(opts)
    spec = receiver_spec(gpu["id"], size_bytes, volume_id)
    pod_id = await _deploy_helper(cfg, backend, RECEIVER_MODEL, spec)
    t0 = time.monotonic()
    while time.monotonic() - t0 < RECEIVER_READY_TIMEOUT_S:
        if await probe_receiver(pod_id):
            return {"pod_id": pod_id, "reused": False}
        await asyncio.sleep(5.0)
    raise UploadError(f"receiver pod {pod_id} not healthy after "
                      f"{RECEIVER_READY_TIMEOUT_S:.0f}s (it stays in the "
                      "servers widget; the watchdog reaps it)")


async def receiver_status(pod_id: str, filename: str) -> dict[str, Any]:
    """Bytes received for a file (-1 = nothing yet)."""
    from urllib.parse import quote
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(receiver_url(pod_id, "status/" + quote(filename)))
        r.raise_for_status()
        return r.json()


async def receiver_list(pod_id: str) -> list[dict[str, Any]]:
    """Files the receiver sees in /workspace/uploads (bootstrap's
    done-signal)."""
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(receiver_url(pod_id, "list"))
        r.raise_for_status()
        return (r.json() or {}).get("files") or []


async def finalize(pod_id: str, filename: str,
                   size_bytes: int) -> dict[str, Any]:
    """Verify the uploaded size and hand back the deployable URL."""
    st = await receiver_status(pod_id, filename)
    got = int(st.get("size") or -1)
    if got != size_bytes:
        raise UploadError(f"size mismatch: receiver has {got} bytes, "
                          f"expected {size_bytes}")
    from urllib.parse import quote
    return {"pod_id": pod_id, "filename": filename,
            "size_bytes": got,
            "size_gb": round(got / 1e9, 2),
            "file_url": receiver_url(pod_id, "file/" + quote(filename)),
            "source": "upload"}


async def _terminate_helper(cfg, backend: str, model_key: str) -> dict[str, Any]:
    """Terminate a helper pod (receiver / serverless bootstrap) + drop
    its config entry. mark_stopped first so the watchdog doesn't read
    our own termination as an unexpected death (E2E-proven bug)."""
    from . import lifecycle

    models = (cfg.backends.get(backend) or {}).get("models") or {}
    pod_id = str((models.get(model_key) or {}).get("pod_id") or "").strip()
    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    stopped = False
    if pod_id:
        try:
            lifecycle.mark_stopped(pod_id)
            await runpod.terminate_pod(pod_id, env)
            stopped = True
        except Exception:  # noqa: BLE001 — cleanup is best-effort
            logger.warning("helper terminate failed", exc_info=True)
    try:
        if cfg.path.is_file():
            raw = json.loads(cfg.path.read_text(encoding="utf-8"))
            (raw.get("backends", {}).get(backend, {})
             .get("models", {})).pop(model_key, None)
            cfg.path.write_text(json.dumps(raw, indent=2) + "\n",
                                encoding="utf-8")
            cfg.reload_now()
    except Exception:  # noqa: BLE001
        logger.warning("helper config cleanup failed", exc_info=True)
    return {"ok": True, "terminated": stopped, "pod_id": pod_id}


async def cleanup_receiver(cfg, backend: str) -> dict[str, Any]:
    """Terminate the upload receiver + drop its config entry."""
    return await _terminate_helper(cfg, backend, RECEIVER_MODEL)


async def bootstrap_download(cfg, backend: str, file_url: str,
                             filename: str, size_bytes: int,
                             volume_id: str,
                             timeout_s: float = 3600.0) -> dict[str, Any]:
    """Download a repo file ONTO THE VOLUME via a temporary bootstrap
    pod (receiver in the background + curl in the foreground). The
    receiver's /list is the done-signal; the pod is terminated on
    success AND failure. Returns {filename, size_bytes, volume_id}."""
    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    opts = await catalog.live_options(env)
    gpu = _cheapest_gpu(opts)
    spec = bootstrap_spec(gpu["id"], file_url, filename, volume_id)
    pod_id = await _deploy_helper(cfg, backend, BOOTSTRAP_MODEL, spec)
    try:
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout_s:
            try:
                for f in await receiver_list(pod_id):
                    if (f.get("name") == filename
                            and int(f.get("size") or 0) >= size_bytes):
                        return {"filename": filename,
                                "size_bytes": size_bytes,
                                "volume_id": volume_id}
            except Exception:  # noqa: BLE001 — poll is best-effort
                pass
            await asyncio.sleep(10.0)
        raise UploadError(f"bootstrap download of {filename} did not "
                          f"complete within {timeout_s:.0f}s")
    finally:
        await _terminate_helper(cfg, backend, BOOTSTRAP_MODEL)
