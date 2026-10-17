"""Content Vault — verification, search, and offline decryption.

These functions read the PART FILES directly (no queue, no locks —
append-only files). They back the admin endpoints AND the customer's
offline reference tool: the file format is documented (spec §2.2), so
any tooling can be built without us — these are the reference.

  verify_chain(parts)  — integrity with NO keys: each record's prev
                        must equal sha256(previous record's ct);
                        part headers must continue the chain. Tampering
                        anywhere breaks it and the break is locatable.

  search(parts, Key S, value) — blind-index equality scan; returns the
                        MATCHING ENCRYPTED records (extraction without
                        decryption).

  decrypt(records, private key) — open extracted records with any
                        wrap-set private key; verifies sha_req/sha_resp
                        against the plaintext.
"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

from . import keys as vkeys


def _iter_records(parts: list[Path]):
    """Yield (part_name, line_no, record) for every non-header, non-
    tombstone record, in chain order."""
    for part in parts:
        for i, line in enumerate(
                part.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                yield part.name, i, {"_unparseable": line}
                continue
            if rec.get("header") or rec.get("tombstone"):
                continue
            yield part.name, i, rec


def verify_chain(parts: list[Path], vault_id: str) -> dict:
    """Walk the chain over ciphertext. Returns
  {ok, checked, tombstones, break?} — break names part:line:reason.
    """
    genesis = hashlib.sha256(f"vault:{vault_id}".encode()).hexdigest()
    expected = genesis
    checked = 0
    tombstones: list[dict] = []
    for part in sorted(parts):
        for line in part.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                return {"ok": False, "checked": checked,
                        "tombstones": tombstones,
                        "break": f"{part.name}: unparseable line"}
            if rec.get("tombstone"):
                tombstones.append(rec["tombstone"])
                continue
            if rec.get("header"):
                if rec.get("continues") != expected:
                    return {"ok": False, "checked": checked,
                            "tombstones": tombstones,
                            "break": (f"{part.name}: header continues "
                                      f"{rec.get('continues')[:12]}… "
                                      f"but chain expects "
                                      f"{expected[:12]}…")}
                continue
            if rec.get("prev") != expected:
                return {"ok": False, "checked": checked,
                        "tombstones": tombstones,
                        "break": (f"{part.name}:{rec.get('seq')}: prev "
                                  f"mismatch — record altered or removed")}
            try:
                ct = base64.b64decode(rec["ct"])
            except Exception:               # noqa: BLE001
                return {"ok": False, "checked": checked,
                        "tombstones": tombstones,
                        "break": f"{part.name}:{rec.get('seq')}: bad ct"}
            expected = hashlib.sha256(ct).hexdigest()
            checked += 1
    return {"ok": True, "checked": checked, "tombstones": tombstones}


def search(parts: list[Path], search_key: bytes, field: str,
           value: Any, limit: int = 1000) -> list[dict]:
    """Blind-index scan: HMAC the search value once, compare against
    each record's idx.<field>. Returns the matching ENCRYPTED records
    with part/line references (extraction without decryption)."""
    want = vkeys.blind_index(search_key, value)
    out = []
    for part_name, line_no, rec in _iter_records(parts):
        if len(out) >= limit:
            break
        if rec.get("idx", {}).get(field) == want:
            out.append({"part": part_name, "line": line_no,
                        "seq": rec.get("seq"), "ts": rec.get("ts"),
                        "record": rec})
    return out


def decrypt(records: list[dict], private_pem: str) -> list[dict]:
    """Open extracted records with any wrap-set private key; verify
    sha_req / sha_resp against the decrypted plaintext (content
    integrity after decryption — spec §2.2). Both sides canonicalize
    the request identically (separators, default=str)."""
    out = []
    for item in records:
        rec = item["record"] if "record" in item else item
        dek = vkeys.unwrap_dek(rec["dek"], private_pem)
        payload = json.loads(vkeys.open_(dek, rec["nonce"], rec["ct"]))
        req = payload.get("request")
        resp = payload.get("response") or ""
        req_canon = (json.dumps(req, default=str, separators=(",", ":"))
                     if req is not None else None)
        sha_req = (hashlib.sha256(req_canon.encode()).hexdigest()
                   if req_canon else "")
        sha_resp = (hashlib.sha256(resp.encode()).hexdigest()
                    if resp else "")
        out.append({
            "seq": rec.get("seq"), "ts": payload.get("ts"),
            "meta": payload.get("meta"),
            "request": req, "response": resp,
            "integrity": {
                "sha_req_ok": sha_req == rec.get("sha_req"),
                "sha_resp_ok": sha_resp == rec.get("sha_resp"),
            }})
    return out
