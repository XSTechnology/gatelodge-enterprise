"""Live verification: depth additions (method map, How It Works, FAQ, matrix)."""
import httpx

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
BASE = "https://datagatelodge.com"

ok = True
with httpx.Client(headers={"user-agent": UA}, follow_redirects=True, timeout=30) as c:
    checks = []
    r = c.get(BASE + "/gatelodge.html")
    b = r.text
    checks += [
        ("gate 200", r.status_code == 200),
        ("10 pages in nav", b.count('role="tab"') == 10),
        ("how page present", 'id="how"' in b and "How It Works" in b),
        ("lifecycle diagram", "flow-diagram" in b),
        ("faq page present", 'id="faq"' in b and "Questions at the gate" in b),
        ("7 faq items", b.count('class="faq-item"') == 7),
        ("compare modal", 'id="compare-modal"' in b),
        ("matrix rows", b.count("<tr>") >= 13),
        ("versioned assets", "styles.css?v=20261014c" in b and "site.js?v=20261014c" in b),
    ]
    r = c.get(BASE + "/services.html")
    b = r.text
    checks += [
        ("services 200", r.status_code == 200),
        ("method map on landing", 'class="method-map"' in b and "estate evolves" in b),
        ("5 map nodes", b.count('class="mm-node"') >= 25),  # 6 maps x 5 nodes (incl. run strip)
        ("you-are-here strips", b.count('aria-current="step"') == 5),
    ]
    r = c.get(BASE + "/")
    b = r.text
    checks += [
        ("index 200", r.status_code == 200),
        ("console links to how", "gatelodge.html#how" in b),
    ]
    for asset in ["/styles.css?v=20261014c", "/site.js?v=20261014c"]:
        r = c.get(BASE + asset)
        has = ".method-map" in r.text if "css" in asset else "initModals" in r.text
        checks.append((f"fresh {asset.split('?')[0]}", r.status_code == 200 and has))

    for name, passed in checks:
        print(("OK  " if passed else "FAIL") + " " + name)
        if not passed:
            ok = False

print("\nLIVE " + ("VERIFIED" if ok else "PROBLEMS FOUND"))
