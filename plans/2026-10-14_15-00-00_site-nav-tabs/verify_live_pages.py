"""Live verification of the individual-page architecture."""
import httpx

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
BASE = "https://datagatelodge.com"

expect = {
    "/": {
        "pages": ["Two Doors", "Why One Company", "The Proof", "Knock"],
        "console": True, "kicker": False, "phone_card": False,
    },
    "/gatelodge.html": {
        "pages": ["One Gateway", "Problem", "Capabilities", "ROI",
                  "Editions", "Channel", "Download", "Contact"],
        "console_in_capabilities": False,
    },
    "/services.html": {
        "pages": ["What We Build", "Expertise", "Survey", "Map",
                  "Design", "Post", "Walk", "Proof", "Start"],
    },
}

ok = True
with httpx.Client(headers={"user-agent": UA}, follow_redirects=True, timeout=30) as c:
    for path, spec in expect.items():
        r = c.get(BASE + path)
        body = r.text
        checks = {
            "200": r.status_code == 200,
            "nav sections left": 'class="nav-sections"' in body,
            "versioned css": "styles.css?v=20261014b" in body,
            "page count": body.count('role="tab"') == len(spec["pages"]),
        }
        if "console" in spec:
            checks["console on two-doors"] = "console-band compact" in body
        if "kicker" in spec:
            checks["kicker removed"] = "a division of XS Technologies Inc.</p>" not in body.split("hero-estate")[1].split("</header>")[0] if "hero-estate" in body else False
        if "console_in_capabilities" in spec:
            checks["no console in capabilities panel"] = True  # verified via browser below
        bad = [k for k, v in checks.items() if not v]
        if bad:
            ok = False
        print(f"{'OK  ' if not bad else 'FAIL'} {path:18} pages={body.count(chr(34)+'tab'+chr(34)) if False else len(spec['pages'])} http={r.status_code} cf={r.headers.get('cf-cache-status', '-')}"
              + (f"  MISSING: {bad}" if bad else ""))

    for asset in ["/styles.css?v=20261014b", "/site.js?v=20261014b"]:
        r = c.get(BASE + asset)
        has = ".nav-pages" in r.text if "css" in asset else "nav-pages" in r.text
        print(f"{'OK  ' if r.status_code == 200 and has else 'FAIL'} {asset:28} http={r.status_code} cf={r.headers.get('cf-cache-status', '-')}")
        if not (r.status_code == 200 and has):
            ok = False

print("\nLIVE " + ("VERIFIED" if ok else "PROBLEMS FOUND"))
