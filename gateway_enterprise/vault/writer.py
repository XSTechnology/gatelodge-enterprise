"""Content Vault — the writer: queue, records, chain, parts, retention.

Hot path contract (spec §4.1): enqueue() is the ONLY call that runs on
the request path — dict build + put_nowait, microseconds. A full queue
DROPS AND FLAGS (dropped counter, warn log) — the vault never blocks a
request. Encryption (HMAC indexes → DEK → multi-wrap → chain) and all
disk I/O happen on the writer task.

Record format (JSONL, one per line — spec §2.2):

  {"v":1,"seq":N,"ts":"…","trace":"…","sha_req":"…","sha_resp":"…",
   "idx":{"user":"hmacS(key-id)","model":"hmacS(model)","team":"hmacS(team)"},
   "prev":"sha256(prev record's ct bytes)",
   "dek":[{"v":2,"alg":"RSA-OAEP","wrap":"b64"}, …],
   "nonce":"b64","ct":"b64(AES-256-GCM(request+response+meta))"}

Part files vault-YYYYMMDD.jsonl under data/vault/; each part begins
with a header record continuing the chain from the previous part, so
verification walks parts in order. Retention (keep_days / max_gb)
drops whole oldest parts and records a tombstone line — the deleted
span is provably absent, never silently missing.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from . import keys as vkeys

logger = logging.getLogger("gateway.enterprise.vault")

GENESIS = "genesis"          # part 1's header continues from this marker


class VaultWriter:
    """The off-path writer. Owns the queue, the crypto, the parts."""

    def __init__(self, vault_dir: Path, vault_id: str,
                 registry: list[dict], search_key: bytes,
                 mode: str = "all", keep_days: int = 0,
                 max_gb: float = 0.0, queue_cap: int = 10000):
        self.dir = Path(vault_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.vault_id = vault_id
        self.registry = registry
        self.search_key = search_key
        self.mode = mode
        self.keep_days = max(0, int(keep_days))
        self.max_gb = max(0.0, float(max_gb))
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_cap)
        self.dropped = 0
        self.records = 0
        self.seq = 0
        self._task: asyncio.Task | None = None
        self._tombstones: list[dict] = []
        self._load_state()

    # ── startup / shutdown ─────────────────────────────────────────

    def _load_state(self) -> None:
        """Resume seq + chain head from the newest part's tail so a
        restart continues the chain (never rewrites history)."""
        for part in self._parts(reverse=True):
            for line in reversed(
                    part.read_text(encoding="utf-8").splitlines()):
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if rec.get("header"):
                    continue
                if "ct" in rec:
                    self.seq = int(rec.get("seq") or 0) + 1
                    self._chain_head = self._ct_hash(rec)
                    return
        self.seq = 0
        self._chain_head = hashlib.sha256(
            f"vault:{self.vault_id}".encode()).hexdigest()

    def start(self) -> None:
        """Start the writer task — requires a RUNNING event loop (the
        app's startup event, or tests inside asyncio.run)."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        """Sentinel + drain: the task exits after the queue empties.
        The await put() (not put_nowait) waits for space if a burst is
        mid-flight — shutdown is the one place waiting is correct."""
        if self._task is not None:
            await self.queue.put(None)
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            self._task = None

    # ── hot path ───────────────────────────────────────────────────

    def enqueue(self, request_dump: str | None, response_text: str,
                meta: dict) -> bool:
        """The ONLY request-path call. Returns False when the queue is
        full (dropped-and-flagged — never blocks, never raises)."""
        try:
            self.queue.put_nowait({"req": request_dump,
                                   "resp": response_text,
                                   "meta": dict(meta),
                                   "ts": time.time()})
            return True
        except asyncio.QueueFull:
            self.dropped += 1
            logger.warning("vault queue full — record DROPPED "
                           "(total dropped: %d)", self.dropped)
            return False

    # ── writer task ─────────────────────────────────────────────────

    async def _run(self) -> None:
        while True:
            item = await self.queue.get()
            if item is None:
                break
            try:
                line = self._build_record(item)
                self._append(line)
                self.records += 1
            except Exception:  # noqa: BLE001 — the vault never breaks
                logger.warning("vault write failed", exc_info=True)
            if self.queue.empty():
                self._enforce_retention()

    def _build_record(self, item: dict) -> str:
        req, resp, meta = item["req"], item["resp"], item["meta"]
        # capture modes (spec §5): all / prompts / responses / sample:1/N
        if self.mode == "prompts":
            resp = ""
        elif self.mode == "responses":
            req = None
        req_obj = json.loads(req) if req else None
        # canonical form for the integrity digests — the decrypt side
        # re-serializes identically (separators, default=str)
        req_canon = (json.dumps(req_obj, default=str,
                                separators=(",", ":"))
                     if req_obj is not None else None)
        payload = json.dumps({
            "ts": item["ts"], "meta": meta,
            "request": req_obj,
            "response": resp,
        }, default=str, separators=(",", ":")).encode("utf-8")
        dek = os.urandom(32)
        nonce, ct = vkeys.seal(dek, payload)
        rec = {
            "v": 1, "seq": self.seq, "ts": item["ts"],
            "trace": meta.get("trace") or "",
            "sha_req": (hashlib.sha256(req_canon.encode()).hexdigest()
                        if req_canon else ""),
            "sha_resp": (hashlib.sha256(resp.encode()).hexdigest()
                         if resp else ""),
            "idx": {
                "user": vkeys.blind_index(self.search_key,
                                          meta.get("key")),
                "model": vkeys.blind_index(self.search_key,
                                           meta.get("model")),
                "team": vkeys.blind_index(self.search_key,
                                          meta.get("team")),
            },
            "prev": self._chain_head,
            "dek": vkeys.wrap_dek(dek, self.registry),
            "nonce": nonce, "ct": ct,
        }
        self.seq += 1
        self._chain_head = self._ct_hash(rec)
        return json.dumps(rec, separators=(",", ":"))

    def _ct_hash(self, rec: dict) -> str:
        return hashlib.sha256(
            __import__("base64").b64decode(rec["ct"])).hexdigest()

    # ── parts, chain, retention ────────────────────────────────────

    def _parts(self, reverse: bool = False) -> list[Path]:
        parts = sorted(self.dir.glob("vault-*.jsonl"))
        return list(reversed(parts)) if reverse else parts

    def _part_for_today(self) -> Path:
        return self.dir / f"vault-{time.strftime('%Y%m%d')}.jsonl"

    def _append(self, line: str) -> None:
        part = self._part_for_today()
        new_part = not part.exists()
        with part.open("a", encoding="utf-8") as f:
            if new_part:
                # the header continues from the chain head BEFORE this
                # record — which is exactly the record's own prev (the
                # builder already advanced the head by now)
                header = {"header": True, "part": part.name,
                          "vault_id": self.vault_id,
                          "continues": json.loads(line)["prev"],
                          "ts": time.time()}
                f.write(json.dumps(header, separators=(",", ":")) + "\n")
            f.write(line + "\n")

    def _enforce_retention(self) -> None:
        """keep_days / max_gb: drop whole OLDEST parts; record the
        tombstone in the next part's header (provably absent)."""
        parts = self._parts()
        drop: list[Path] = []
        if self.keep_days and parts:
            cutoff = time.time() - self.keep_days * 86400
            for p in parts[:-1]:                      # never the active one
                day = p.stem.split("-")[-1]
                try:
                    if time.mktime(time.strptime(day, "%Y%m%d")) < cutoff:
                        drop.append(p)
                except ValueError:
                    continue
        if self.max_gb and parts:
            cap = self.max_gb * (1 << 30)
            while parts and sum(p.stat().st_size for p in parts) > cap:
                drop.append(parts.pop(0))
        for p in drop:
            p.unlink()
            t = {"deleted": p.name, "at": time.time()}
            # the tombstone is written IMMEDIATELY into the active part
            # (header-shaped line) — the deleted span is provably
            # absent the moment retention drops it, never deferred
            with self._part_for_today().open("a", encoding="utf-8") as f:
                f.write(json.dumps({"tombstone": t,
                                    "ts": time.time()},
                                   separators=(",", ":")) + "\n")
            self._tombstones.append(t)
            logger.info("vault retention dropped part %s", p.name)

    # ── status ─────────────────────────────────────────────────────

    def status(self) -> dict:
        parts = self._parts()
        size = sum(p.stat().st_size for p in parts)
        return {"vault_id": self.vault_id, "mode": self.mode,
                "parts": len(parts), "disk_mb": round(size / 1e6, 2),
                "records": self.records, "seq": self.seq,
                "dropped": self.dropped,
                "chain_head": self._chain_head,
                "keys": [{k: e[k] for k in ("v", "role", "alg")}
                         for e in self.registry]}

    def canary(self, key_version: int) -> dict:
        """Write a canary record (known plaintext) wrapped to the wrap
        set — keycheck decrypts these offline to prove key health."""
        line = self._build_record({
            "req": json.dumps({"canary": True, "key_v": key_version}),
            "resp": "", "meta": {"model": "vault", "key": f"canary:v"
                                                       f"{key_version}"},
            "ts": time.time()})
        self._append(line)
        self.records += 1
        rec = json.loads(line)
        return {"seq": rec["seq"], "dek": rec["dek"],
                "nonce": rec["nonce"], "ct": rec["ct"]}
