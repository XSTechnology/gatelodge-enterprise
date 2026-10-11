"""stub_idp.py — dev-only stub OIDC IdP (tier C introspection + JWKS).

A minimal RFC 7662 introspection endpoint AND a JWKS endpoint with
JWT minting, for live verification of the SSO path on this box — no
real IdP needed. NOT imported by anything; run it alongside the
gateway:

    python scripts/stub_idp.py [port]     (default 8099)

POST /oauth2/introspect (Basic auth accepted but NOT verified — dev
only). Opaque token conventions:

    stub-admin-<x>   active, groups=["gatelodge-full-admins"]
    stub-km-<x>      active, groups=["gatelodge-key-managers"]
    stub-ops-<x>     active, groups=["everyone"]        (→ default_role)
    anything else    {"active": false}

GET /jwks.json — the stub's RSA public keyset (kid stub-key-1), for
the gateway's LOCAL JWT validation (sso.jwks_url).

GET /token/<group> — mint a REAL RS256 JWT signed by the stub key
(groups claim per <group>: admin | km | ops; exp +1h). Use it as the
bearer: the gateway validates it locally against /jwks.json with ZERO
introspection round-trips.

Every introspection and JWKS fetch is logged (with running counts) so
the gateway's cache + local-validation behavior can be verified live:
JWT requests should show introspect #COUNT stuck at its old value.
"""
from __future__ import annotations

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

KID = "stub-key-1"
GROUPS = {
    "stub-admin": "gatelodge-full-admins",
    "stub-km": "gatelodge-key-managers",
    "stub-ops": "everyone",
}
MINT_GROUPS = {"admin": "gatelodge-full-admins",
               "km": "gatelodge-key-managers",
               "ops": "everyone"}

# one RSA keypair per stub run — the JWKS is derived from it
_PRIV = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_JWK = RSAAlgorithm.to_jwk(_PRIV.public_key(), as_dict=True)
_JWK["kid"] = KID
_JWK["alg"] = "RS256"
_JWKS = {"keys": [_JWK]}


class Handler(BaseHTTPRequestHandler):
    hits = 0          # introspections
    jwks_hits = 0     # keyset fetches
    mints = 0

    def do_GET(self) -> None:  # noqa: N802 — http.server naming
        path = self.path.split("?")[0]
        if path == "/jwks.json":
            Handler.jwks_hits += 1
            print(f"[stub-idp] jwks fetch #{Handler.jwks_hits}")
            self._json(200, _JWKS)
            return
        if path.startswith("/token/"):
            group = path[len("/token/"):].strip("/")
            if group not in MINT_GROUPS:
                self._json(404, {"error": "group must be admin|km|ops"})
                return
            Handler.mints += 1
            now = int(time.time())
            token = jwt.encode(
                {"sub": f"stub-jwt-{group}", "iss": "http://127.0.0.1:8099",
                 "aud": "gatelodge-dev",
                 "email": f"jwt-{group}@stub.example",
                 "groups": [MINT_GROUPS[group]],
                 "iat": now, "exp": now + 3600},
                _PRIV, algorithm="RS256", headers={"kid": KID})
            print(f"[stub-idp] mint #{Handler.mints}: JWT for {group!r}")
            self._json(200, {"token": token, "expires_in": 3600})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 — http.server naming
        if self.path.split("?")[0] != "/oauth2/introspect":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        body = parse_qs(self.rfile.read(length).decode("utf-8"))
        token = (body.get("token") or [""])[0]
        Handler.hits += 1
        print(f"[stub-idp] introspect #{Handler.hits}: token={token[:24]!r}…")
        for prefix, group in GROUPS.items():
            if token.startswith(prefix):
                self._json(200, {
                    "active": True,
                    "sub": token,
                    "email": f"{prefix.replace('stub-', '')}@stub.example",
                    "groups": [group],
                    "tenant": "dev",
                    "aud": "gatelodge-dev",
                    "exp": int(time.time()) + 3600,
                    "iss": "http://127.0.0.1:8099",
                })
                return
        self._json(200, {"active": False})

    def _json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):  # silence default stderr noise
        pass


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"[stub-idp] listening on http://127.0.0.1:{port}"
          f" — POST /oauth2/introspect | GET /jwks.json | GET /token/"
          "{{admin|km|ops}} (opaque tokens: stub-admin-* / stub-km-* / "
          f"stub-ops-* / else inactive; kid {KID})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[stub-idp] done ({Handler.hits} introspections, "
              f"{Handler.jwks_hits} jwks fetches, {Handler.mints} mints)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
