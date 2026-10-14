"""Final live check through the public URL: dock + chat widget coexistence."""
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page(viewport={"width": 1440, "height": 900})
    errs = []
    page.on("pageerror", lambda e: errs.append(str(e)))

    for path in ["/gatelodge.html", "/services.html"]:
        page.goto("https://datagatelodge.com" + path,
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)  # let assets + chat config gate resolve
        info = page.evaluate("""() => {
            const gk = document.querySelector('.gk-launch');
            const dock = document.querySelector('.dock');
            const tabs = document.querySelectorAll('.tab').length;
            const gkBottom = gk ? gk.getBoundingClientRect().bottom : null;
            const dockTop = dock ? dock.getBoundingClientRect().top : null;
            return {tabs, gk: !!gk, gkBottom, dockTop,
                    overlap: gk && dock ? gkBottom > dockTop : null};
        }""")
        print(path, info)

    # open the EULA modal on the live product page
    page.goto("https://datagatelodge.com/gatelodge.html",
              wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2500)
    page.click(".dock-eula")
    page.wait_for_timeout(1200)
    loaded = page.eval_on_selector("#eula-body", "el => el.textContent.includes('Definitions')")
    print("live eula modal content loaded:", loaded)
    page.screenshot(path=r"%TEMP%\\live-eula.png".replace("%TEMP%", __import__("os").environ["TEMP"]))
    b.close()
    print("page errors:", errs if errs else "none")
