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
RECEIVER_PORT = 8081
RECEIVER_IMAGE = "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404"
RECEIVER_READY_TIMEOUT_S = 150.0


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


async def probe_receiver(pod_id: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(receiver_url(pod_id, "health"))
            return r.status_code == 200
    except Exception:  # noqa: BLE001 — probe is best-effort
        return False


def _persist_receiver_spec(cfg, backend: str, spec: dict) -> None:
    """Read-modify-write the config (own tiny RMW — no app import, no
    cycles; same pattern as lifecycle._clear_pod_id)."""
    try:
        if cfg.path.is_file():
            raw = json.loads(cfg.path.read_text(encoding="utf-8"))
            models = (raw.setdefault("backends", {})
                      .setdefault(backend, {}).setdefault("models", {}))
            models[RECEIVER_MODEL] = spec
            cfg.path.write_text(json.dumps(raw, indent=2) + "\n",
                                encoding="utf-8")
            cfg.reload_now()
    except Exception:  # noqa: BLE001 — upload init must still return
        logger.warning("upload receiver spec persist failed", exc_info=True)


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
    priced = [(g, float((g.get("price") or {}).get("secure") or 0))
              for g in opts.get("gpus", [])]
    priced = [(g, p) for g, p in priced if p > 0]
    if not priced:
        raise UploadError("no GPU available for the upload receiver")
    gpu, _ = min(priced, key=lambda t: t[1])
    spec = receiver_spec(gpu["id"], size_bytes, volume_id)
    payload = runpod.deploy_payload(backend, RECEIVER_MODEL, spec)
    pod = await runpod.deploy_pod(payload, env)
    pod_id = str(pod.get("id") or "")
    if not pod_id:
        raise UploadError(f"receiver deploy returned no id: {pod!r}")
    spec["pod_id"] = pod_id
    _persist_receiver_spec(cfg, backend, spec)
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


async def cleanup_receiver(cfg, backend: str) -> dict[str, Any]:
    """Terminate the receiver + drop its config entry."""
    models = (cfg.backends.get(backend) or {}).get("models") or {}
    spec = models.get(RECEIVER_MODEL) or {}
    pod_id = str(spec.get("pod_id") or "").strip()
    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    stopped = False
    if pod_id:
        try:
            await runpod.terminate_pod(pod_id, env)
            stopped = True
        except Exception:  # noqa: BLE001 — cleanup is best-effort
            logger.warning("receiver terminate failed", exc_info=True)
    try:
        if cfg.path.is_file():
            raw = json.loads(cfg.path.read_text(encoding="utf-8"))
            (raw.get("backends", {}).get(backend, {})
             .get("models", {})).pop(RECEIVER_MODEL, None)
            cfg.path.write_text(json.dumps(raw, indent=2) + "\n",
                                encoding="utf-8")
            cfg.reload_now()
    except Exception:  # noqa: BLE001
        logger.warning("receiver config cleanup failed", exc_info=True)
    return {"ok": True, "terminated": stopped, "pod_id": pod_id}
