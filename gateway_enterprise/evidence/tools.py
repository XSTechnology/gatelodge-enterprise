"""Evidence Center — offline CLI (customer/auditor side).

  python -m gateway_enterprise.evidence.tools verify <package.zip>

Re-checks every manifest sha256 and re-runs the ledger-chain walk over
the package's own extract — the verdict comes from the artifacts, not
from trusting the assembler. Exits 0 when everything verifies.
"""
from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from pathlib import Path


def _verify(path: str) -> int:
    p = Path(path)
    if not p.is_file():
        print(f"error: no such package: {p}")
        return 2
    problems: list[str] = []
    with zipfile.ZipFile(p) as z:
        names = set(z.namelist())
        try:
            manifest = json.loads(z.read("manifest.json"))
        except KeyError:
            print("error: no manifest.json in package")
            return 2
        except json.JSONDecodeError as e:
            print(f"error: manifest.json unparseable: {e}")
            return 2

        # 1. every manifest file present + sha256 exact
        files = manifest.get("files") or {}
        for name, meta in sorted(files.items()):
            if name not in names:
                problems.append(f"MISSING: {name}")
                continue
            digest = hashlib.sha256(z.read(name)).hexdigest()
            if digest != (meta or {}).get("sha256"):
                problems.append(f"HASH MISMATCH: {name}")

        # 2. no undeclared files (something smuggled in)
        for name in sorted(names - set(files) - {"manifest.json"}):
            problems.append(f"UNDECLARED FILE: {name}")

        # 3. ledger chain walk over the package's own extract
        chain, plain = 0, 0
        prev = "genesis"
        ledger_files = sorted(n for n in files
                              if n.startswith("ledger/gateway-log-"))
        for lf in ledger_files:
            for i, ln in enumerate(
                    z.read(lf).decode("utf-8", errors="replace")
                    .strip().splitlines(), 1):
                if not ln.strip():
                    continue
                try:
                    rec = json.loads(ln)
                except json.JSONDecodeError:
                    problems.append(f"UNPARSEABLE: {lf}:{i}")
                    continue
                h = rec.get("hash")
                if not h:
                    plain += 1
                    continue
                body = {k: v for k, v in rec.items() if k != "hash"}
                if hashlib.sha256(json.dumps(
                        body, sort_keys=True).encode(
                        "utf-8")).hexdigest() != h:
                    problems.append(f"LEDGER HASH MISMATCH: {lf}:{i}")
                elif rec.get("prev") != prev:
                    problems.append(f"LEDGER CHAIN BREAK: {lf}:{i}")
                prev = h
                chain += 1

    print(f"package: {manifest.get('package_id')} "
          f"(gateway {manifest.get('gateway_version')}, "
          f"period {manifest.get('period', {}).get('from')}"
          f"..{manifest.get('period', {}).get('to')})")
    print(f"files verified: {len(files)} | ledger: {chain} chained, "
          f"{plain} plain")
    if problems:
        print(f"\nFAILED — {len(problems)} problem(s):")
        for pr in problems[:20]:
            print(f"  - {pr}")
        return 1
    print("OK — every hash matches, the ledger chain walks clean")
    return 0


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(
        prog="evidence-tools",
        description="Evidence package verifier — trust the artifacts, "
                    "not the assembler")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify", help="verify a package zip")
    v.add_argument("package", help="the evidence-*.zip to verify")
    args = ap.parse_args()
    if args.cmd == "verify":
        return _verify(args.package)
    return 2


if __name__ == "__main__":
    raise SystemExit(_cli())
