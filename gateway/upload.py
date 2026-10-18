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
from .config import atomic_write_json, get_gateway_config

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
# v2.5.1 (GL-R02): every endpoint except /health requires the session
# token (Authorization: Bearer … or ?t=…) — the gateway generates it at
# receiver deploy and passes it via the pod env GW_UPLOAD_TOKEN. /list
# is GONE (the gateway polls /status/<name>); /status also reports the
# sha256 sidecar when the bootstrap wrote one, so finalize can verify
# content, not just size.

_RECEIVER_PY = '''
import hashlib, json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
D = "/workspace/uploads"
TOKEN = os.environ.get("GW_UPLOAD_TOKEN", "")
class H(BaseHTTPRequestHandler):
    def _send(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)
    def _authed(self):
        if not TOKEN:
            return False
        h = self.headers.get("Authorization") or ""
        if h == "Bearer " + TOKEN:
            return True
        q = self.path.split("?", 1)
        if len(q) == 2 and ("t=" + TOKEN) in q[1]:
            return True
        return False
    def do_GET(self):
        try:
            if self.path.split("?")[0] == "/health":
                self._send(200, b"{\\"ok\\": true}")
                return
            if not self._authed():
                self._send(403, b"{\\"error\\": \\"token required\\"}")
                return
            path = self.path.split("?")[0]
            if path.startswith("/status/"):
                n = os.path.basename(path[8:])
                p = os.path.join(D, n)
                part = p + ".part"
                if os.path.exists(p):
                    sz = os.path.getsize(p)
                elif os.path.exists(part):
                    sz = os.path.getsize(part)
                else:
                    sz = -1
                sha = ""
                sp = p + ".sha256"
                if os.path.exists(sp):
                    try:
                        sha = open(sp).read().strip()
                    except Exception:
                        sha = ""
                self._send(200, json.dumps(
                    {"name": n, "size": sz, "sha256": sha}).encode())
            elif path.startswith("/file/"):
                n = os.path.basename(path[6:])
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
            if not self._authed():
                self._send(403, b"{\\"error\\": \\"token required\\"}")
                return
            n = os.path.basename(self.path.lstrip("/").split("?")[0])
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


def _new_upload_token() -> str:
    """Per-receiver-session token (GL-R02): the gateway generates it,
    passes it to the pod via env, and shares it with the uploading
    console via the init response. Anyone who can reach the pod proxy
    URL but not the token gets 403 on everything but /health."""
    import secrets
    return secrets.token_hex(24)


def receiver_script() -> str:
    """One-line sh: write the receiver to disk and run it foreground
    (the container lives exactly as long as the receiver serves)."""
    return ("set -e; mkdir -p /workspace/uploads; "
            "printf '%s' '" + _RECEIVER_PY + "' > /root/receiver.py; "
            "exec python3 /root/receiver.py")


def bootstrap_script() -> str:
    """Receiver in the BACKGROUND + a foreground curl that downloads
    the model onto the volume. v2.5.1 (GL-R03): the URL and filename
    travel via POD ENV ($FILE_URL / $FILENAME) — nothing dynamic is
    interpolated into the sh text, so a hostile catalog entry cannot
    break out of quoting. After the download, the sha256 sidecar is
    written so /status (the gateway's done-signal) reports content,
    not just size. sleep infinity keeps the container alive until the
    gateway terminates the pod."""
    return ("set -e; mkdir -p /workspace/uploads; "
            "printf '%s' '" + _RECEIVER_PY + "' > /root/receiver.py; "
            "python3 /root/receiver.py > /root/receiver.log 2>&1 & "
            "echo '[boot] downloading ' \"$FILENAME\"; "
            "curl -fsSL --retry 3 --max-time 5400 "
            "-o \"/workspace/uploads/$FILENAME\" \"$FILE_URL\"; "
            "sha256sum \"/workspace/uploads/$FILENAME\" "
            "| cut -d ' ' -f 1 "
            "> \"/workspace/uploads/$FILENAME.sha256\"; "
            "echo '[boot] download done'; "
            "sleep infinity")


def receiver_spec(gpu_id: str, size_bytes: int,
                  volume_id: str = "", token: str = "") -> dict[str, Any]:
    disk = max(20, int(size_bytes / 1e9 * 1.2) + 10)
    spec: dict[str, Any] = {
        "image": RECEIVER_IMAGE,
        "gpu_type_ids": [gpu_id], "gpu_count": 1,
        "docker_entrypoint": ["sh", "-c"],
        "docker_script": receiver_script(),
        "ports": "8081/http", "port": RECEIVER_PORT,
        # GL-R02: the session token reaches the receiver via env —
        # never in the script text, never in a URL the proxy logs.
        "env": {"GW_UPLOAD_TOKEN": token},
        "container_disk_gb": disk, "start_timeout_s": 600,
        "idle_stop_s": 1800, "idle_terminate_s": 0,
        "alias": RECEIVER_MODEL, "custom": True,
        "source_file": "upload-receiver",
        "upload_token": token,
    }
    if volume_id:
        spec["volume_id"] = volume_id
    return spec


def bootstrap_spec(gpu_id: str, file_url: str, filename: str,
                   volume_id: str, data_center_id: str = "",
                   token: str = "") -> dict[str, Any]:
    """Receiver + background-download pod writing onto the VOLUME (the
    file must persist after this pod dies — container disk would not).
    DC-pinned to the volume's data center (network volumes are
    DC-bound; a helper pod in another DC cannot mount it). v2.5.1: the
    URL/filename/token travel via pod ENV (GL-R03/R02) — the sh text
    is a fixed string."""
    spec: dict[str, Any] = {
        "image": RECEIVER_IMAGE,
        "gpu_type_ids": [gpu_id], "gpu_count": 1,
        "docker_entrypoint": ["sh", "-c"],
        "docker_script": bootstrap_script(),
        "ports": "8081/http", "port": RECEIVER_PORT,
        "env": {"FILE_URL": file_url, "FILENAME": filename,
                "GW_UPLOAD_TOKEN": token},
        "container_disk_gb": 20, "start_timeout_s": 600,
        "idle_stop_s": 1800, "idle_terminate_s": 0,
        "alias": BOOTSTRAP_MODEL, "custom": True,
        "source_file": "serverless-bootstrap",
        "volume_id": volume_id,
    }
    if data_center_id:
        spec["data_center_ids"] = [data_center_id]
    return spec


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
            atomic_write_json(cfg.path, raw)
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
    """A healthy receiver pod_id + its session token — reused when
    possible, else deployed on the cheapest GPU. The token is
    generated per receiver deployment, passed to the pod via env, and
    persisted in the config entry (so a reused receiver keeps its
    token across gateway restarts). Raises UploadError when the
    receiver never becomes healthy (the config entry stays: the
    watchdog's never-healthy reaper stops a broken boot, idle-stop
    reaps an idle one)."""
    models = (cfg.backends.get(backend) or {}).get("models") or {}
    existing = models.get(RECEIVER_MODEL) or {}
    old_pod = str(existing.get("pod_id") or "").strip()
    old_token = str(existing.get("upload_token") or "").strip()
    if old_pod and old_token and await probe_receiver(old_pod):
        return {"pod_id": old_pod, "upload_token": old_token,
                "reused": True}

    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    opts = await catalog.live_options(env)
    gpu = _cheapest_gpu(opts)
    token = _new_upload_token()
    spec = receiver_spec(gpu["id"], size_bytes, volume_id, token=token)
    pod_id = await _deploy_helper(cfg, backend, RECEIVER_MODEL, spec)
    t0 = time.monotonic()
    while time.monotonic() - t0 < RECEIVER_READY_TIMEOUT_S:
        if await probe_receiver(pod_id):
            return {"pod_id": pod_id, "upload_token": token,
                    "reused": False}
        await asyncio.sleep(5.0)
    raise UploadError(f"receiver pod {pod_id} not healthy after "
                      f"{RECEIVER_READY_TIMEOUT_S:.0f}s (it stays in the "
                      "servers widget; the watchdog reaps it)")


async def receiver_status(pod_id: str, filename: str,
                          token: str = "") -> dict[str, Any]:
    """Bytes received for a file (-1 = nothing yet) + the sha256
    sidecar when the bootstrap wrote one. Token-authenticated."""
    from urllib.parse import quote
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(receiver_url(pod_id, "status/" + quote(filename)),
                        headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        return r.json()


# gateway-computed relay hashes (GL-R02): the PUT relay sees every
# byte, so it records the actual sha256 per (pod, filename) — finalize
# verifies content for browser uploads without a pod-side pass.
_RELAY_HASHES: dict[str, str] = {}


def remember_relay_hash(pod_id: str, filename: str, sha256: str) -> None:
    _RELAY_HASHES[f"{pod_id}/{filename}"] = sha256


def relay_hash(pod_id: str, filename: str) -> str:
    return _RELAY_HASHES.get(f"{pod_id}/{filename}", "")


async def finalize(pod_id: str, filename: str, size_bytes: int,
                   token: str = "", expect_sha256: str = "") -> dict[str, Any]:
    """Verify the uploaded size AND content (GL-R02): the actual
    sha256 comes from the receiver's sidecar (bootstrap downloads) or
    the gateway's relay hash (browser uploads). A provided
    expect_sha256 must match — same-size replacement poisoning fails
    here. Returns the deployable URL (the token rides as ?t= — the
    URL is the capability the model pod curls)."""
    st = await receiver_status(pod_id, filename, token)
    got = int(st.get("size") or -1)
    if got != size_bytes:
        raise UploadError(f"size mismatch: receiver has {got} bytes, "
                          f"expected {size_bytes}")
    actual = str(st.get("sha256") or "") or relay_hash(pod_id, filename)
    if expect_sha256:
        if not actual:
            raise UploadError("no sha256 available for the uploaded "
                              "file (relay hash missing) — cannot "
                              "verify content")
        if actual.lower() != expect_sha256.lower():
            raise UploadError(f"sha256 mismatch: got {actual[:16]}…, "
                              "expected "
                              f"{expect_sha256[:16]}… — the file may "
                              "have been replaced")
    from urllib.parse import quote
    from urllib.parse import urlencode
    file_url = receiver_url(pod_id, "file/" + quote(filename))
    if token:
        file_url += "?" + urlencode({"t": token})
    return {"pod_id": pod_id, "filename": filename,
            "size_bytes": got,
            "size_gb": round(got / 1e9, 2),
            "sha256": actual,
            "file_url": file_url,
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
            from .config import atomic_write_json
            atomic_write_json(cfg.path, raw)
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
                             sha256: str = "",
                             timeout_s: float = 3600.0) -> dict[str, Any]:
    """Download a repo file ONTO THE VOLUME via a temporary bootstrap
    pod (receiver in the background + curl in the foreground). The
    receiver's /status/<name> (size + sha256 sidecar) is the
    done-signal; the pod is terminated on success AND failure. An
    optional expected sha256 is verified before success is reported
    (GL-R02/R25) — a same-size replacement fails. Returns {filename,
    size_bytes, volume_id, sha256}."""
    env = ((cfg.backends.get(backend) or {}).get("api_key_env")
           or "RUNPOD_API_KEY")
    # the bootstrap receiver shares the upload-session token posture
    token = _new_upload_token()
    # volume's DC (network volumes are DC-bound)
    try:
        vols = await runpod.list_volumes(env)
    except Exception:  # noqa: BLE001 — fall back to unpinned placement
        vols = []
    vol_dc = ""
    for v in vols if isinstance(vols, list) else []:
        if isinstance(v, dict) and v.get("id") == volume_id:
            vol_dc = str(v.get("dataCenter") or "")
            break

    opts = await catalog.live_options(env)
    priced = sorted(
        (float((g.get("price") or {}).get("secure") or 0), g.get("id", ""))
        for g in opts.get("gpus", [])
        if (g.get("price") or {}).get("secure"))
    if not priced:
        raise UploadError("no GPU available for the bootstrap pod")
    pod_id = ""
    last_err = ""
    for _, gpu_id in priced:
        spec = bootstrap_spec(gpu_id, file_url, filename, volume_id,
                              vol_dc, token=token)
        try:
            pod_id = await _deploy_helper(cfg, backend, BOOTSTRAP_MODEL,
                                          spec)
            break
        except runpod.RunPodError as e:
            last_err = str(e)
            logger.info("bootstrap deploy with %s failed: %s", gpu_id,
                        last_err[:160])
            continue
    if not pod_id:
        raise UploadError(f"bootstrap pod deploy failed in every GPU "
                          f"tier (last: {last_err[:200]})")
    try:
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout_s:
            try:
                st = await receiver_status(pod_id, filename, token)
                if int(st.get("size") or -1) >= size_bytes:
                    actual = str(st.get("sha256") or "")
                    if sha256 and actual \
                            and actual.lower() != sha256.lower():
                        raise UploadError(
                            f"sha256 mismatch after download: got "
                            f"{actual[:16]}…, expected {sha256[:16]}…")
                    return {"filename": filename,
                            "size_bytes": size_bytes,
                            "volume_id": volume_id,
                            "sha256": actual or sha256}
            except UploadError:
                raise
            except Exception:  # noqa: BLE001 — poll is best-effort
                pass
            await asyncio.sleep(10.0)
        raise UploadError(f"bootstrap download of {filename} did not "
                          f"complete within {timeout_s:.0f}s")
    finally:
        await _terminate_helper(cfg, backend, BOOTSTRAP_MODEL)
