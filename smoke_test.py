"""smoke_test.py — LLM Gateway release smoke test.

Curls every endpoint class and asserts expected status codes. Run this
after any deployment (start.bat, service install, exe) to confirm the
gateway is healthy end-to-end. Zero-cost: no LLM calls (except healthz).

Usage: python smoke_test.py [base_url]
  default base_url: http://127.0.0.1:8010
"""
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8010"

checks = [
    # (name, method, path, expected_status, body)
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
    ("bad model 400", "POST", "/v1/chat/completions", 400,
     {"model": "__nonexistent__", "messages": [{"role": "user", "content": "x"}]}),
]


def main() -> int:
    passed = 0
    failed = 0
    t0 = time.perf_counter()
    print(f"LLM Gateway smoke test — {BASE}")
    print("-" * 50)
    with httpx.Client(timeout=15) as c:
        for name, method, path, expected, body in checks:
            try:
                if method == "GET":
                    r = c.get(f"{BASE}{path}")
                else:
                    r = c.post(f"{BASE}{path}", json=body)
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
