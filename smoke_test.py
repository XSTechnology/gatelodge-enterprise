"""smoke_test.py — Gatelodge release smoke test.

Curls every endpoint class and asserts expected status codes. Run this
after any deployment (start.bat, service install, exe) to confirm the
gateway is healthy end-to-end. Zero-cost: no LLM calls (except healthz).

Usage: python smoke_test.py [base_url] [--key BEARER] [--ro-key BEARER]
  default base_url: http://127.0.0.1:8010
  --key      bearer for every check (required when keys are configured)
  --ro-key   an ops_readonly admin key: adds C-lite RBAC checks
             (needs --key too — a full-admin key for the contrast row)
  env fallbacks: GATEWAY_SMOKE_KEY / GATEWAY_SMOKE_RO_KEY
"""
import os
import sys
import time

import httpx


def _arg(flag: str) -> str:
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return ""


BASE = (sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-")
        else "http://127.0.0.1:8010")
KEY = _arg("--key") or os.environ.get("GATEWAY_SMOKE_KEY", "")
RO_KEY = _arg("--ro-key") or os.environ.get("GATEWAY_SMOKE_RO_KEY", "")
HEADERS = {"Authorization": f"Bearer {KEY}"} if KEY else {}

checks = [
    # (name, method, path, expected_status, body[, headers])
    ("console shell", "GET", "/", 200, None),
    ("healthz", "GET", "/healthz", 200, None),
    ("v1/models", "GET", "/v1/models", 200, None),
    ("metrics", "GET", "/metrics", 200, None),
    ("admin/stats", "GET", "/admin/stats", 200, None),
    ("admin/events", "GET", "/admin/events", 200, None),
    ("admin/keys", "GET", "/admin/keys", 200, None),
    ("admin/secrets", "GET", "/admin/secrets", 200, None),
    ("admin/guardrails", "GET", "/admin/guardrails", 200, None),
    ("admin/routes", "GET", "/admin/routes", 200, None),
    ("admin/audit", "GET", "/admin/audit", 200, None),
    ("admin/evals", "GET", "/admin/evals", 200, None),
    ("admin/verify status", "GET", "/admin/verify", 200, None),
    ("admin/aliases", "GET", "/admin/aliases", 200, None),
    ("admin/backends", "GET", "/admin/backends", 200, None),
    ("admin/config", "GET", "/admin/config", 200, None),
    ("batches list", "GET", "/v1/batches", 200, None),
    ("admin batches (governance pane)", "GET", "/admin/batches", 200, None),
    ("bad model 400", "POST", "/v1/chat/completions", 400,
     {"model": "__nonexistent__", "messages": [{"role": "user", "content": "x"}]}),
]

if RO_KEY and KEY:
    # C-lite RBAC: the read-only key reads, cannot mutate or read raw
    # config; the full-admin key still can.
    ro = {"Authorization": f"Bearer {RO_KEY}"}
    checks += [
        ("rbac: ro GET stats", "GET", "/admin/stats", 200, None, ro),
        ("rbac: ro GET config", "GET", "/admin/config", 403, None, ro),
        ("rbac: ro POST reload", "POST", "/admin/reload", 403, None, ro),
        ("rbac: full POST reload", "POST", "/admin/reload", 200, None,
         HEADERS),
    ]


def main() -> int:
    passed = 0
    failed = 0
    t0 = time.perf_counter()
    print(f"Gatelodge smoke test — {BASE}"
          + (f" (bearer {'set' if KEY else 'not set'})" if KEY else ""))
    print("-" * 50)
    with httpx.Client(timeout=15) as c:
        for check in checks:
            name, method, path, expected, body = check[:5]
            headers = check[5] if len(check) > 5 else HEADERS
            try:
                if method == "GET":
                    r = c.get(f"{BASE}{path}", headers=headers)
                else:
                    r = c.post(f"{BASE}{path}", json=body, headers=headers)
                ok = r.status_code == expected
                mark = "✓" if ok else "✗"
                detail = "" if ok else f" (got {r.status_code}, want {expected})"
                print(f"  {mark} {name:25s} {r.status_code}{detail}")
                if ok:
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"  ✗ {name:25s} ERROR: {e}")
                failed += 1
    elapsed = time.perf_counter() - t0
    print("-" * 50)
    print(f"  {passed}/{passed + failed} passed in {elapsed:.1f}s")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
