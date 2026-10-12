"""Gatelodge vendor license tooling — keypair generation + license issuing.

Runs inside the STANDARD repo venv (cryptography + pyjwt ride
pyjwt[crypto]). The signing key is the vendor crown jewel:

    scripts/license_tool.py keygen
        → vendor_keys/license_signing_key.pem   (gitignored; NEVER commit,
          never ship, eventually move to offline/HSM custody)

    scripts/license_tool.py issue --edition enterprise \
        --business-name "Acme Corp" --contract-email "ops@acme.com" \
        [--vendor runpod] [--support] \
        [--feature tier_d=Y --feature scim=N ...]   (default: all Y) \
        [--provider hf --provider rp ...]           (cloud backends) \
        [--duration 1y | 30d | 60d | 90d | 2y | 3y | perpetual] \
        [--node <fingerprint>] [--out data/license.json | --raw]

The token is a plain RS256 JWT: the claims are READABLE (operators
can see their entitlements); the SHA-256 hash of the contents is
signed with the vendor private key and gateway/licensing.py verifies
it against the PUBLIC key embedded in the binary.

Claims (v2):

    product         "gatelodge"                     (fixed)
    edition         "community" | "enterprise"      (license tier)
    business_name   the customer's business name
    contract_email  the contract contact address
    vendor          optional channel partner
    support         bool — vendor-support entitlement
    features        {code: Y/N} over gateway.licensing.FEATURE_CODES
    providers       [backend names] — per-backend cloud entitlement
                    (embedded/llama-server are free; openai/runpod/
                    runpod-public backends need their name here)
    exp/nbf         validity (duration presets; exp omitted = perpetual)
    node            optional node-lock fingerprint

The public half of the generated keypair must be pasted into
gateway/licensing.py (the keygen output) — keygen prints it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))          # import the registry from the core
from gateway.licensing import (  # noqa: E402
    CLOUD_PROVIDER_TYPES, FEATURE_CODES)

KEY_PATH = ROOT / "vendor_keys" / "license_signing_key.pem"

PRODUCT = "gatelodge"
ENTERPRISE_FEATURES = list(FEATURE_CODES)
DURATIONS = {"30d": 30, "60d": 60, "90d": 90, "1y": 365, "2y": 730,
             "3y": 1095, "perpetual": None}


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


def _parse_features(args) -> dict[str, bool]:
    """--feature CODE=Y|N (repeatable) over --features (v1 comma list).
    Default: every FEATURE_CODES entry Y (enterprise) / empty
    (community). Unknown codes warn but pass — forward compatibility
    with newer binaries."""
    out: dict[str, bool] = {}
    if args.features:                                   # v1 comma list
        out = {c.strip(): True for c in args.features.split(",")
               if c.strip()}
    elif args.edition == "enterprise":
        out = {c: True for c in FEATURE_CODES}
    for spec in args.feature or []:
        code, _, val = spec.partition("=")
        code = code.strip()
        if not code:
            continue
        out[code] = val.strip().lower() not in ("n", "no", "false", "0")
        if code not in FEATURE_CODES:
            print(f"warning: {code!r} is not in FEATURE_CODES "
                  f"{FEATURE_CODES} — issuing anyway (forward compat)",
                  file=sys.stderr)
    return out


def cmd_issue(args: argparse.Namespace) -> int:
    if not (args.business_name or args.customer):
        print("error: --business-name is required "
              "(or the legacy --customer alias)", file=sys.stderr)
        return 2
    priv = KEY_PATH.read_text(encoding="ascii")
    now = _now()

    duration = args.duration
    if args.perpetual:
        duration = "perpetual"                          # legacy flag
    days = DURATIONS[duration]

    claims: dict = {
        "product": PRODUCT,
        "edition": args.edition,
        "business_name": args.business_name or args.customer,
        "support": bool(args.support),
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
    }
    if args.contract_email:
        claims["contract_email"] = args.contract_email
    if args.customer and not args.business_name:
        claims["customer"] = args.customer              # v1 display alias
    if args.vendor:
        claims["vendor"] = args.vendor
    features = _parse_features(args)
    if features:
        claims["features"] = features
    if args.provider:
        claims["providers"] = [p.strip() for p in args.provider
                               if p.strip()]
    if args.node:
        claims["node"] = args.node
    if days is not None:
        claims["exp"] = int((now + dt.timedelta(days=days)).timestamp())

    # The license is PLAINTEXT claims; the SHA-256 hash of the
    # contents is signed with the vendor private key (RS256) and the
    # gateway verifies it with the embedded public key.
    import jwt
    token = jwt.encode(claims, priv, algorithm="RS256")

    if args.raw:
        print(token)
        return 0
    out = Path(args.out) if args.out else ROOT / "data" / "license.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"license": token}, indent=2) + "\n",
                   encoding="utf-8")
    on = {c: y for c, y in claims.get("features", {}).items() if y}
    print(f"license written: {out}")
    print(f"  edition={claims['edition']} "
          f"business={claims['business_name']!r} "
          f"email={claims.get('contract_email') or '-'}")
    print(f"  features(on)={sorted(on)} "
          f"providers={claims.get('providers', [])} "
          f"duration={duration}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Vendor-side sanity check: read a license file and verify it
    against the SAME keypair (the gateway does this at boot with the
    embedded public half)."""
    from gateway import licensing
    data = json.loads(Path(args.path).read_text(encoding="utf-8"))
    token = data.get("license") if isinstance(data, dict) else data
    claims, reason = licensing._verify_token(str(token), _now())
    if claims is None:
        print(f"INVALID: {reason}", file=sys.stderr)
        return 1
    print(json.dumps(claims, indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    kg = sub.add_parser("keygen", help="generate the vendor signing keypair")
    kg.add_argument("--force", action="store_true",
                    help="overwrite an existing key (ROTATES all licenses)")
    kg.set_defaults(fn=cmd_keygen)

    isu = sub.add_parser("issue", help="issue a signed license "
                         "(plaintext claims + RS256 signature)")
    isu.add_argument("--edition", required=True,
                     choices=["community", "enterprise"])
    isu.add_argument("--business-name", help="the customer's business "
                     "name (v2 identity claim)")
    isu.add_argument("--contract-email", help="contract contact address")
    isu.add_argument("--customer", help="(legacy v1 alias for the name)")
    isu.add_argument("--vendor", help="channel partner (e.g. runpod)")
    isu.add_argument("--support", action="store_true",
                     help="vendor-support entitlement")
    isu.add_argument("--feature", action="append", metavar="CODE=Y|N",
                     help=f"feature switch, repeatable — codes: "
                     f"{','.join(FEATURE_CODES)} (default: all Y)")
    isu.add_argument("--features", help="(legacy v1) comma-separated "
                     "feature keys")
    isu.add_argument("--provider", action="append", metavar="NAME",
                     help="cloud backend NAME entitled to serve "
                     f"({','.join(sorted(CLOUD_PROVIDER_TYPES))} types; "
                     "repeatable; local types are free)")
    isu.add_argument("--duration", default="1y", choices=list(DURATIONS),
                     help="validity preset: 30d/60d/90d evaluation, "
                     "1y/2y/3y term, perpetual (default 1y)")
    isu.add_argument("--days", type=int, help="(legacy) raw day count — "
                     "overrides --duration")
    isu.add_argument("--perpetual", action="store_true",
                     help="(legacy) no expiry")
    isu.add_argument("--node", help="node-lock fingerprint (omit = floating)")
    isu.add_argument("--out", help="output path (default data/license.json)")
    isu.add_argument("--raw", action="store_true",
                     help="print the token instead of writing a file")
    isu.set_defaults(fn=cmd_issue)

    ver = sub.add_parser("verify", help="verify + dump a license file's "
                         "claims (vendor-side sanity check)")
    ver.add_argument("path", help="license JSON file")
    ver.set_defaults(fn=cmd_verify)

    args = p.parse_args(argv)
    if getattr(args, "days", None) and getattr(args, "duration", None):
        DURATIONS[str(args.days) + "d"] = int(args.days)   # raw override
        args.duration = str(args.days) + "d"
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
