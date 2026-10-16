"""Check all site pages for mojibake in a real browser."""
from playwright.sync_api import sync_playwright

PAGES = ["https://datagatelodge.com/",
         "https://datagatelodge.com/gatelodge.html",
         "https://datagatelodge.com/services.html",
         "https://datagatelodge.com/eula.html"]

with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page()
    for url in PAGES:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        found = page.evaluate("""() => {
            const t = document.body.textContent;
            const bad = [];
            if (t.includes('\\u00c3\\u00a2')) bad.push('A-circumflex-cedilla');
            if (t.includes('\\u00e2\\u20ac\\u201c')) bad.push('euro-quote');
            if (t.includes('\\u00e2\\u20ac\\u201d')) bad.push('euro-dquote');
            return bad;
        }""")
        print(url.split('/')[-1] or 'index:', "CLEAN" if not found
              else f"MOJIBAKE {found}")
    b.close()
