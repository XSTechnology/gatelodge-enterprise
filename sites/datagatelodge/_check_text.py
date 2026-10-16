"""Browser-verify the product page text renders clean (no mojibake)."""
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page()
    page.goto("https://datagatelodge.com/gatelodge.html",
              wait_until="domcontentloaded")
    page.wait_for_timeout(2500)
    page.click('.nav-pages a[href="#product"]')
    page.wait_for_timeout(400)
    tags = page.eval_on_selector_all(
        "#product .note-tag", "els => els.map(e => e.textContent.trim())")
    lede = page.eval_on_selector("#product .section-lede",
                                 "el => el.textContent")
    audit = page.eval_on_selector_all(
        "#product .product-note p",
        "els => els.map(e => e.textContent.slice(0, 90))")
    link = page.eval_on_selector("#product a[href='/services.html']",
                                 "el => el.textContent")
    print("note-tags:", tags)
    print("lede:", lede.strip()[:100])
    for a in audit:
        print("note:", a)
    print("link:", link)
    moji = page.evaluate(
        "document.getElementById('product').textContent"
        ".indexOf('\\u00c3\\u00a2') >= 0 || "
        "document.getElementById('product').textContent"
        ".indexOf('\\u00e2\\u20ac') >= 0")
    print("mojibake found:", moji)
    b.close()
