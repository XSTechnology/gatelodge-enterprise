"""Gatelodge vendor license tooling — keypair generation + license issuing.

Runs inside the STANDARD repo venv (cryptography + pyjwt ride
pyjwt[crypto]). The signing key is the vendor crown jewel:

    scripts/license_tool.py keygen
        → vendor_keys/license_signing_key.pem   (gitignored; NEVER commit,
          never ship, eventually move to offline/HSM custody)

    scripts/license_tool.py issue --edition community --customer "Acme" \
        [--vendor runpod] [--support] [--features a,b] \
        [--days 365 | --perpetual] [--node <fingerprint>] \
        [--out data/license.json | --raw]

The license itself is an RS256 JWT (verified by gateway/licensing.py
against the PUBLIC key embedded in the binary). Claims:

    product   "gatelodge"                       (fixed)
    edition   "community" | "enterprise"        (license tier)
    customer  free-text identifier              (surfaces in /admin/license)
    vendor    optional channel partner          (e.g. a RunPod distribution)
    support   bool — vendor-support entitlement (channel tie-in)
    features  list — feature keys the license unlocks (community Pro
              extras; enterprise defaults to tier_d,scim,session_budgets)
    exp/nbf   standard JWT dates                (exp omitted = perpetual)
    node      optional node-lock fingerprint    (sha16 of host identity)

The public half of the generated keypair must be pasted into
gateway/licensing.py (_LICENSE_PUBLIC_KEY_PEM) — keygen prints it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEY_PATH = ROOT / "vendor_keys" / "license_signing_key.pem"

PRODUCT = "gatelodge"
ENTERPRISE_FEATURES = ["tier_d", "scim", "session_budgets"]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def cmd_keygen(args: argparse.Namespace) -> int:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    if KEY_PATH.exists() and not args.force:
        print(f"refusing to overwrite {KEY_PATH} (use --force to rotate)",
              file=sys.stderr)
        return 1
    KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_priv = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    KEY_PATH.write_bytes(pem_priv)
    pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo)
    print(f"private key: {KEY_PATH}  (gitignored — guard it)")
    print("public key (paste into gateway/licensing.py "
          "_LICENSE_PUBLIC_KEY_PEM):")
    print(pub.decode("ascii"))
    return 0


def cmd_issue(args: argparse.Namespace) -> int:
    import jwt

    priv = KEY_PATH.read_text(encoding="ascii")
    now = _now()
    claims: dict = {
        "product": PRODUCT,
        "edition": args.edition,
        "customer": args.customer,
        "support": bool(args.support),
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
    }
    if args.vendor:
        claims["vendor"] = args.vendor
    if args.features:
        claims["features"] = [f.strip() for f in args.features.split(",")
                              if f.strip()]
    elif args.edition == "enterprise":
        claims["features"] = list(ENTERPRISE_FEATURES)
    if args.node:
        claims["node"] = args.node
    if not args.perpetual:
        claims["exp"] = int((now + dt.timedelta(days=args.days)).timestamp())

    token = jwt.encode(claims, priv, algorithm="RS256")
    if args.raw:
        print(token)
        return 0
    out = Path(args.out) if args.out else ROOT / "data" / "license.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"license": token}, indent=2) + "\n",
                   encoding="utf-8")
    print(f"license written: {out}")
    print(f"  edition={claims['edition']} customer={claims['customer']!r} "
          f"features={claims.get('features', [])} "
          f"exp={'perpetual' if 'exp' not in claims else claims['exp']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    kg = sub.add_parser("keygen", help="generate the vendor signing keypair")
    kg.add_argument("--force", action="store_true",
                    help="overwrite an existing key (ROTATES all licenses)")
    kg.set_defaults(fn=cmd_keygen)

    isu = sub.add_parser("issue", help="issue a signed license (RS256 JWT)")
    isu.add_argument("--edition", required=True,
                     choices=["community", "enterprise"])
    isu.add_argument("--customer", required=True)
    isu.add_argument("--vendor", help="channel partner (e.g. runpod)")
    isu.add_argument("--support", action="store_true",
                     help="vendor-support entitlement")
    isu.add_argument("--features", help="comma-separated feature keys "
                     "(enterprise default: tier_d,scim,session_budgets)")
    isu.add_argument("--days", type=int, default=365,
                     help="validity window (default 365)")
    isu.add_argument("--perpetual", action="store_true",
                     help="no expiry (internal/dev licenses)")
    isu.add_argument("--node", help="node-lock fingerprint (omit = floating)")
    isu.add_argument("--out", help="output path (default data/license.json)")
    isu.add_argument("--raw", action="store_true",
                     help="print the JWT instead of writing a file")
    isu.set_defaults(fn=cmd_issue)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
