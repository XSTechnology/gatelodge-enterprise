"""Print actual rendered text of lines containing dash-like sequences."""
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page()
    page.goto("https://datagatelodge.com/gatelodge.html",
              wait_until="domcontentloaded")
    page.wait_for_timeout(1500)
    lede = page.eval_on_selector("#intro .lede", "el => el.textContent")
    print("HERO LEDE:", repr(lede[:200]))
    kicker = page.eval_on_selector("#intro .kicker",
                                   "el => el.textContent")
    print("KICKER:", repr(kicker))
    page.goto("https://datagatelodge.com/", wait_until="domcontentloaded")
    page.wait_for_timeout(1500)
    hero = page.eval_on_selector(".hero-copy .lede",
                                 "el => el.textContent")
    print("INDEX LEDE:", repr(hero[:200]))
    b.close()
