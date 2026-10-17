"""Evidence Center — the package bundler.

Assembles a self-describing zip into data/evidence/: ledger months
(IN FULL — the chain walks cleanly over complete files; a line subset
would orphan prev links), chain verification results, config-audit
chain, license state, AI asset inventory, governance posture, and the
control mapping. manifest.json carries sha256 of every file — the
package verifies without trusting the vendor.

Period granularity is the MONTH (files are month-scoped). The walk in
ledger-verify.json covers exactly the files included, so an auditor
re-verifying gets the same result from the package alone.
"""
from __future__ import annotations

import hashlib
import json
import time
import zipfile
from pathlib import Path
from typing import Any


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _j(obj: Any) -> bytes:
    return (json.dumps(obj, indent=1, sort_keys=True)
            + "\n").encode("utf-8")


def _readme(pkg_id: str, months: list[str]) -> bytes:
    return f"""# Gatelodge evidence package `{pkg_id}`

**What this is:** auditor-ready evidence assembled by the customer's
own Gatelodge deployment — request ledger (months {', '.join(months)}),
chain-verification results, config-mutation audit chain, license state,
AI asset inventory, governance posture, and a control mapping
(SOC 2 TSC / ISO 27001 / NIST AI RMF / EU AI Act).

**What this is NOT:** a certification or attestation of any kind.
Gatelodge produces verifiable artifacts; the auditor concludes. The
not-provided list in controls.md is part of the product's honest
posture.

## Re-verify this package (no trust in the vendor required)

1. Every file's sha256 is in `manifest.json` — check them with any
   hashing tool.
2. The request-ledger chain: each chained record's `hash` is
   `sha256(canonical-JSON of the record with the "hash" field removed)`
   where canonical JSON = sorted keys, default separators; `prev` must
   equal the previous chained record's `hash` ("genesis" for the
   first). Records without a `hash` are plain (unchained) lines —
   skipped by the walk, counted in `ledger-verify.json` as `plain`.
3. The config-audit chain in `config-audit/` follows the same rule
   (its verifier restarts at genesis after plain records).

With the product installed:
`python -m gateway_enterprise.evidence.tools verify <this zip>`
""".encode("utf-8")


def _period_stats(lines: list[dict[str, Any]]) -> dict[str, Any]:
    ok = client_err = server_err = 0
    models: set[str] = set()
    keys: set[str] = set()
    cost = 0.0
    for e in lines:
        s = int(e.get("status") or 0)
        if s < 400:
            ok += 1
        elif s < 500:
            client_err += 1
        else:
            server_err += 1
        if e.get("model"):
            models.add(str(e["model"]))
        if e.get("key"):
            keys.add(str(e["key"]))
        cost += float(e.get("cost_usd") or 0)
    return {"requests": ok + client_err + server_err, "ok": ok,
            "client_errors": client_err, "server_errors": server_err,
            "models": len(models), "keys": len(keys),
            "cost_usd": round(cost, 4)}


def _month_files_for(month_from: str, month_to: str) -> list[Path]:
    """Month ledger files overlapping the period, chronological."""
    from gateway.app import _req_month_files
    out = []
    for p in _req_month_files():
        m = p.name[len("gateway-log-"):-len(".jsonl")]
        if month_from <= m <= month_to:
            out.append(p)
    return out


def build_package(month_from: str | None = None,
                  month_to: str | None = None) -> dict[str, Any]:
    """Assemble the package; returns the manifest (also written into
    the zip). Raises RuntimeError with a clear message on bad input."""
    from gateway import licensing
    from gateway.app import (PROJECT_ROOT, _LOG_PATH, verify_audit_chain,
                             verify_request_chain)
    from gateway.config import get_gateway_config

    month_to = month_to or time.strftime("%Y-%m")
    month_from = month_from or month_to
    if not (len(month_from) == 7 and len(month_to) == 7
            and month_from <= month_to):
        raise RuntimeError(
            "period must be YYYY-MM months, from <= to "
            f"(got {month_from!r}..{month_to!r})")

    months = _month_files_for(month_from, month_to)

    # ── ledger extract + period stats + verify over exactly these ──
    ledger_lines: list[dict[str, Any]] = []
    for p in months:
        for ln in p.read_text(encoding="utf-8",
                              errors="replace").splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                ledger_lines.append(json.loads(ln))
            except json.JSONDecodeError:
                ledger_lines.append({"unparseable": ln[:80]})

    ledger_verify = _verify_files(months)
    period_stats = _period_stats(ledger_lines)
    audit_verify = verify_audit_chain()

    cfg = get_gateway_config()
    lic_status = licensing.status()

    # ── vault section (when the add-on is running) ──
    vault_state: dict[str, Any] | None = None
    vault_files: dict[str, bytes] = {}
    try:
        from gateway_enterprise.vault import get_writer
        from gateway_enterprise.vault import index as vindex
        w = get_writer()
        if w is not None:
            st = w.status()
            st["running"] = True            # mirror /admin/vault/status
            vv = vindex.verify_chain(w._parts(), w.vault_id)
            vault_state = dict(st)
            vault_state["chain_valid"] = vv.get("ok")
            vault_state["verify"] = vv
    except Exception:                        # noqa: BLE001 — optional
        vault_state = None

    from .inventory import build_inventory
    from .posture import build_posture, posture_markdown
    inventory = build_inventory(cfg)
    posture = build_posture(cfg, lic_status, ledger_verify, audit_verify,
                            period_stats, vault_state)

    from gateway.app import app as gw_app
    version = getattr(gw_app, "version", "?")
    pkg_id = f"evidence-{time.strftime('%Y%m%d-%H%M%S')}"
    out_dir = PROJECT_ROOT / "data" / "evidence"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{pkg_id}.zip"

    files: dict[str, bytes] = {}
    files["README.md"] = _readme(pkg_id, [p.name[len("gateway-log-"):
                                            -len(".jsonl")] for p in months])
    for p in months:
        files[f"ledger/{p.name}"] = p.read_bytes()
    files["ledger/ledger-verify.json"] = _j(ledger_verify)
    ca = _LOG_PATH.parent / "config-audit.jsonl"
    files["config-audit/config-audit.jsonl"] = (
        ca.read_bytes() if ca.is_file() else b"")
    files["config-audit/config-audit-verify.json"] = _j(audit_verify)
    files["license/license.json"] = _j(lic_status)
    files["inventory.json"] = _j(inventory)
    files["posture.json"] = _j(posture)
    files["posture.md"] = posture_markdown(posture).encode("utf-8")
    from .controls import CONTROLS_MD
    files["controls.md"] = CONTROLS_MD.encode("utf-8")
    if vault_state is not None:
        files["vault/vault-status.json"] = _j(vault_state)

    manifest = {
        "package_id": pkg_id,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "gateway_version": version,
        "period": {"from": month_from, "to": month_to},
        "license": {"serial": lic_status.get("serial"),
                    "edition": lic_status.get("edition"),
                    "mode": lic_status.get("mode")},
        "files": {name: {"sha256": _sha256_bytes(data),
                         "bytes": len(data)}
                  for name, data in sorted(files.items())},
    }
    files["manifest.json"] = _j(manifest)

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED,
                         compresslevel=9) as z:
        for name, data in files.items():
            z.writestr(name, data)

    manifest["path"] = str(out_path)
    manifest["zip_bytes"] = out_path.stat().st_size
    return manifest


def _verify_files(paths: list[Path]) -> dict[str, Any]:
    """Chain walk over EXACTLY the given files (the package's own
    proof — same rules as gateway.app.verify_request_chain)."""
    from gateway.app import _REQ_GENESIS, _req_record_hash
    breaks: list[dict] = []
    files: list[dict] = []
    chained = plain = 0
    prev = _REQ_GENESIS
    for p in paths:
        f_lines = f_chained = f_plain = 0
        try:
            lines = p.read_text(encoding="utf-8",
                                errors="replace").strip().splitlines()
        except OSError:
            lines = []
        for i, ln in enumerate(lines, 1):
            if not ln.strip():
                continue
            f_lines += 1
            try:
                rec = json.loads(ln)
            except json.JSONDecodeError:
                breaks.append({"file": p.name, "line": i,
                               "reason": "unparseable"})
                continue
            h = rec.get("hash")
            if not h:
                plain += 1
                f_plain += 1
                continue
            body = {k: v for k, v in rec.items() if k != "hash"}
            if _req_record_hash(body) != h:
                breaks.append({"file": p.name, "line": i,
                               "reason": "hash mismatch (tampered)"})
            elif rec.get("prev") != prev:
                breaks.append({"file": p.name, "line": i,
                               "reason": "chain break (prev mismatch)"})
            prev = h
            chained += 1
            f_chained += 1
        files.append({"file": p.name, "lines": f_lines,
                      "chained": f_chained, "plain": f_plain})
    return {"files": files,
            "lines": sum(f["lines"] for f in files),
            "chained": chained, "plain": plain, "breaks": breaks,
            "valid": not breaks}
