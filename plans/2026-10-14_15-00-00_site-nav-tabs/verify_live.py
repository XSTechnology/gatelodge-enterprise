"""Live verification: datagatelodge.com serves the reworked nav + fresh assets.

Uses the project venv httpx (execute-tool SSL is unreliable for some hosts).
Browser UA — CF blocks python-urllib with error 1010.
"""
import re

import httpx

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
BASE = "https://datagatelodge.com"

ok = True
with httpx.Client(headers={"user-agent": UA}, follow_redirects=True,
                  timeout=30) as c:
    for path in ["/", "/gatelodge.html", "/services.html", "/eula.html"]:
        r = c.get(BASE + path)
        body = r.text
        checks = {
            "200": r.status_code == 200,
            "versioned css": "styles.css?v=20261014" in body,
            "dock": 'class="dock"' in body,
        }
        if path != "/eula.html":
            checks["eula modal"] = 'id="eula-modal"' in body
            checks["versioned site.js"] = "site.js?v=20261014" in body
        if path == "/gatelodge.html":
            checks["tabs"] = 'class="tabs-sticky"' in body
            checks["7 tab buttons"] = body.count('role="tab"') == 7
            checks["v2.3.0 console"] = "v2.3.0 · community" in body
        if path == "/services.html":
            checks["tabs"] = 'class="tabs-sticky"' in body
            checks["8 tab buttons"] = body.count('role="tab"') == 8
        bad = [k for k, v in checks.items() if not v]
        status = "OK  " if not bad else "FAIL"
        if bad:
            ok = False
        print(f"{status} {path:16} http={r.status_code} cf-cache={r.headers.get('cf-cache-status', '-')}"
              + (f"  MISSING: {bad}" if bad else ""))

    # the versioned asset itself must come back fresh from origin
    for asset in ["/styles.css?v=20261014", "/site.js?v=20261014",
                  "/chat.js?v=20261014"]:
        r = c.get(BASE + asset)
        fresh = r.headers.get("cf-cache-status", "-")
        has_dock = ".dock" in r.text if "css" in asset else "initTabs" in r.text
        print(f"{'OK  ' if r.status_code == 200 and has_dock else 'FAIL'} {asset:28} http={r.status_code} cf-cache={fresh} content={'new' if has_dock else 'STALE'}")
        if not (r.status_code == 200 and has_dock):
            ok = False

print("\nLIVE " + ("VERIFIED" if ok else "PROBLEMS FOUND"))
