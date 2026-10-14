"""Live browser check of the individual-page architecture."""
import os

from playwright.sync_api import sync_playwright

SHOTS = os.path.join(os.environ.get("TEMP", "."), "site_live_shots")
os.makedirs(SHOTS, exist_ok=True)

with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page(viewport={"width": 1440, "height": 900})
    errs = []
    page.on("pageerror", lambda e: errs.append(str(e)))

    for path in ["/", "/gatelodge.html", "/services.html"]:
        page.goto("https://datagatelodge.com" + path,
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)
        info = page.evaluate("""() => {
            const gk = document.querySelector('.gk-launch');
            const dock = document.querySelector('.dock');
            const landing = document.querySelector('.tab-panel:not([hidden])');
            return {
                pages: document.querySelectorAll('.nav-pages a').length,
                landing: landing ? landing.id : null,
                fits: document.documentElement.scrollHeight <= window.innerHeight,
                docH: document.documentElement.scrollHeight,
                gk: !!gk,
                noOverlap: gk && dock
                    ? gk.getBoundingClientRect().bottom <= dock.getBoundingClientRect().top
                    : null,
            };
        }""")
        # walk every page in the list, confirm each fits
        n = page.eval_on_selector_all(".nav-pages a", "els => els.length")
        all_fit = True
        heights = {}
        for i in range(n):
            page.eval_on_selector(f".nav-pages a:nth-of-type({i+1})", "el => el.click()")
            page.wait_for_timeout(250)
            h = page.evaluate("document.documentElement.scrollHeight")
            heights[page.evaluate("location.hash")] = h
            if h > page.evaluate("window.innerHeight"):
                all_fit = False
        print(path, info)
        print("   page heights:", heights, "ALL FIT" if all_fit else "SOME OVERFLOW")

    page.goto("https://datagatelodge.com/", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)
    page.screenshot(path=os.path.join(SHOTS, "live-two-doors.png"))
    b.close()
    print("page errors:", errs if errs else "none")
