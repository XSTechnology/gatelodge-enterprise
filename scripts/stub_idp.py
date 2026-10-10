"""stub_idp.py — dev-only stub OIDC introspection endpoint (tier C).

A minimal RFC 7662 server for live verification of the enterprise SSO
path on this box — no real IdP needed. NOT imported by anything; run
it alongside the gateway:

    python scripts/stub_idp.py [port]     (default 8099)

POST /oauth2/introspect (Basic auth accepted but NOT verified — dev
only). Token conventions:

    stub-admin-<x>   active, groups=["gatelodge-full-admins"]
    stub-km-<x>      active, groups=["gatelodge-key-managers"]
    stub-ops-<x>     active, groups=["everyone"]        (→ default_role)
    anything else    {"active": false}

Every request is logged (with a running count) so the gateway's
introspection cache can be verified live: N gateway requests with the
same token within the TTL should show exactly ONE hit here.
"""
from __future__ import annotations

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

GROUPS = {
    "stub-admin": "gatelodge-full-admins",
    "stub-km": "gatelodge-key-managers",
    "stub-ops": "everyone",
}


class Handler(BaseHTTPRequestHandler):
    hits = 0

    def do_POST(self) -> None:  # noqa: N802 — http.server naming
        if self.path.split("?")[0] != "/oauth2/introspect":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        body = parse_qs(self.rfile.read(length).decode("utf-8"))
        token = (body.get("token") or [""])[0]
        Handler.hits += 1
        print(f"[stub-idp] introspect #{Handler.hits}: token={token!r} "
              f"auth={self.headers.get('Authorization', '')[:12]}…")
        for prefix, group in GROUPS.items():
            if token.startswith(prefix):
                self._json(200, {
                    "active": True,
                    "sub": token,
                    "email": f"{prefix.replace('stub-', '')}@stub.example",
                    "groups": [group],
                    "tenant": "dev",
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
          f"/oauth2/introspect (tokens: stub-admin-* / stub-km-* / "
          f"stub-ops-* / anything-else = inactive)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[stub-idp] done ({Handler.hits} introspections)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
