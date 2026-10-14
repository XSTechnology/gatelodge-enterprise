"""Local verification of the tabbed navigation + dock + EULA modal rework."""
import json
import os
import sys

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8435"
SHOTS = os.path.join(os.environ.get("TEMP", "."), "site_nav_shots")
os.makedirs(SHOTS, exist_ok=True)

results = []
errors = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch()

        # ── desktop ────────────────────────────────────────────────────
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))

        # index
        page.goto(BASE + "/", wait_until="networkidle")
        check("index: brand links home", page.eval_on_selector(
            ".brand", "el => el.getAttribute('href')") == "/")
        check("index: 3 nav links", page.eval_on_selector_all(
            ".nav-links a", "els => els.length") == 3)
        check("index: overview current", page.eval_on_selector(
            '.nav-links a[aria-current="page"]', "el => el.textContent") == "Overview")
        dock = page.eval_on_selector(".dock", """el => {
            const cs = getComputedStyle(el);
            const r = el.getBoundingClientRect();
            return {pos: cs.position, top: r.top, h: r.height, w: r.width};
        }""")
        check("index: dock fixed at bottom",
              dock["pos"] == "fixed" and abs(dock["top"] + dock["h"] - 900) < 2,
              json.dumps(dock))
        one_line = page.eval_on_selector(".dock-inner", """el => {
            const parts = ['.dock-legal', '.dock-line', '.dock-eula']
              .map(s => el.querySelector(s))
              .filter(Boolean)
              .map(n => {
                const r = n.getBoundingClientRect();
                return {top: r.top, h: r.height, cy: r.top + r.height / 2};
              });
            const centersAligned = parts.every(p => Math.abs(p.cy - parts[0].cy) < 4);
            const noWrap = parts.every(p => p.h < 26);
            const oneRow = el.getBoundingClientRect().height <= 48;
            return centersAligned && noWrap && oneRow;
        }""")
        check("index: dock single line", one_line)
        check("index: eula button present", page.eval_on_selector(
            ".dock-eula", "el => el.textContent.trim()") == "EULA")
        check("index: body padded for dock", page.evaluate(
            "parseFloat(getComputedStyle(document.body).paddingBottom)") >= 40)
        page.screenshot(path=os.path.join(SHOTS, "01-index-top.png"))
        page.screenshot(path=os.path.join(SHOTS, "02-index-bottom.png"),
                        clip={"x": 0, "y": 460, "width": 1440, "height": 440})

        # EULA modal
        page.click(".dock-eula")
        page.wait_for_timeout(900)
        modal_open = page.eval_on_selector("#eula-modal", "el => el.open")
        check("index: eula modal opens", modal_open)
        body_txt = page.eval_on_selector("#eula-body", "el => el.textContent")
        check("index: eula content loads", "Definitions" in body_txt and "XS Technologies" in body_txt)
        scrollable = page.eval_on_selector(".eula-body", """el => {
            const cs = getComputedStyle(el);
            return cs.overflowY === 'auto' && el.scrollHeight > el.clientHeight;
        }""")
        check("index: eula body scrollable", scrollable,
              page.eval_on_selector(".eula-body", "el => el.scrollHeight + '/' + el.clientHeight"))
        check("index: eula draft banner kept", "DRAFT" in body_txt)
        page.screenshot(path=os.path.join(SHOTS, "03-index-eula-modal.png"))
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
        check("index: escape closes modal", not page.eval_on_selector(
            "#eula-modal", "el => el.open"))

        # gatelodge
        page.goto(BASE + "/gatelodge.html", wait_until="networkidle")
        tabs = page.eval_on_selector_all(".tab", "els => els.map(t => t.textContent.trim())")
        check("gate: 7 tabs", len(tabs) == 7, str(tabs))
        check("gate: problem default", page.eval_on_selector(
            "#problem", "el => !el.hidden"))
        check("gate: others hidden", page.eval_on_selector_all(
            ".tab-panel", "els => els.filter(e => e.id !== 'problem' && !e.hidden).length") == 0)
        check("gate: console in capabilities", page.eval_on_selector(
            "#capabilities .console-band", "el => !!el"))
        page.click('[aria-controls="capabilities"]')
        page.wait_for_timeout(400)
        check("gate: click activates capabilities", page.eval_on_selector(
            "#capabilities", "el => !el.hidden") and page.eval_on_selector(
            "#capabilities .console-band", "el => getComputedStyle(el).display !== 'none'"))
        page.screenshot(path=os.path.join(SHOTS, "04-gate-capabilities.png"))

        # hash deep link
        page.goto(BASE + "/gatelodge.html#download", wait_until="networkidle")
        check("gate: hash opens download", page.eval_on_selector(
            "#download", "el => !el.hidden"))
        check("gate: tab selected sync", page.eval_on_selector(
            '[aria-controls="download"]', "el => el.getAttribute('aria-selected')") == "true")

        # in-page anchor from hero
        page.goto(BASE + "/gatelodge.html", wait_until="networkidle")
        page.click('.hero-actions .btn-brass')
        page.wait_for_timeout(600)
        check("gate: hero download btn works", page.eval_on_selector(
            "#download", "el => !el.hidden"))
        check("gate: url hash updated", page.evaluate("location.hash") == "#download")

        # EULA link in download fine print opens modal too
        page.click("#download a[data-eula-open]")
        page.wait_for_timeout(900)
        check("gate: fine-print eula opens modal", page.eval_on_selector(
            "#eula-modal", "el => el.open"))
        page.keyboard.press("Escape")

        # sticky tabs: scroll down, bar stays under nav
        page.evaluate("window.scrollTo(0, 2200)")
        page.wait_for_timeout(300)
        sticky = page.eval_on_selector(".tabs-sticky", """el => {
            const r = el.getBoundingClientRect();
            return {top: r.top};
        }""")
        check("gate: tabs stick under nav", abs(sticky["top"] - 64) < 3, json.dumps(sticky))
        page.screenshot(path=os.path.join(SHOTS, "05-gate-sticky.png"))

        # chat widget above dock (config-gated: only renders when the
        # public chat service answers — skip if absent locally)
        has_gk = page.eval_on_selector_all(
            ".gk-launch", "els => els.length") > 0
        if has_gk:
            gk = page.eval_on_selector(".gk-launch", """el => {
                const r = el.getBoundingClientRect();
                return r.bottom;
            }""")
            check("gate: chat sits above dock", gk <= 900 - 46 + 1, str(gk))
        else:
            check("gate: chat widget config-gated (skipped)", True)

        # services
        page.goto(BASE + "/services.html", wait_until="networkidle")
        stabs = page.eval_on_selector_all(".tab", "els => els.map(t => t.textContent.trim())")
        check("services: 8 tabs", len(stabs) == 8, str(stabs))
        check("services: expertise default", page.eval_on_selector(
            "#expertise", "el => !el.hidden"))
        page.click('[aria-controls="design"]')
        page.wait_for_timeout(400)
        check("services: design panel shows", page.eval_on_selector(
            "#design", "el => !el.hidden"))
        check("services: design has step nav", page.eval_on_selector(
            "#design .step-nav a", "el => el.getAttribute('href')") == "#map")
        check("services: design verdict kept", "consultancies stop" in page.eval_on_selector(
            "#design", "el => el.textContent"))
        page.screenshot(path=os.path.join(SHOTS, "06-services-design.png"))

        # step-nav flow: click next
        page.click('#design .step-nav a[href="#post"]')
        page.wait_for_timeout(400)
        check("services: step-nav jumps to post", page.eval_on_selector(
            "#post", "el => !el.hidden") and page.evaluate("location.hash") == "#post")

        # keyboard nav
        page.focus('[aria-controls="post"]')
        page.keyboard.press("ArrowRight")
        page.wait_for_timeout(200)
        check("services: arrow key moves tab", page.eval_on_selector(
            '[aria-controls="walk"]', "el => el.getAttribute('aria-selected')") == "true")

        # hero button → survey
        page.goto(BASE + "/services.html", wait_until="networkidle")
        page.click('.hero-actions .btn-brass')
        page.wait_for_timeout(500)
        check("services: hero → survey", page.eval_on_selector(
            "#survey", "el => !el.hidden"))

        # method content preservation spot checks
        page.goto(BASE + "/services.html#walk", wait_until="networkidle")
        walk_txt = page.eval_on_selector("#walk", "el => el.textContent")
        for phrase in ["Walk the wall", "Confidence rate", "quarterly re-assessment",
                       "project and a capability"]:
            check(f"services: walk keeps '{phrase[:28]}'", phrase in walk_txt)

        ctx.close()

        # ── mobile ─────────────────────────────────────────────────────
        mctx = browser.new_context(viewport={"width": 390, "height": 844})
        mpage = mctx.new_page()
        mpage.on("pageerror", lambda e: errors.append("mobile: " + str(e)))

        mpage.goto(BASE + "/gatelodge.html", wait_until="networkidle")
        mpage.wait_for_timeout(400)
        check("mobile: no horizontal overflow", mpage.evaluate(
            "document.documentElement.scrollWidth <= 390"))
        dock_fits = mpage.eval_on_selector(".dock-inner", """el => {
            const r = el.getBoundingClientRect();
            const legal = el.querySelector('.dock-legal');
            return {w: r.width, overflow: el.scrollWidth > el.clientWidth + 2,
                    legalVisible: legal.getBoundingClientRect().width > 60};
        }""")
        check("mobile: dock fits", not dock_fits["overflow"] and dock_fits["legalVisible"],
              json.dumps(dock_fits))
        tabs_scroll = mpage.eval_on_selector(".tabs-bar", """el =>
            el.scrollWidth > el.clientWidth""")
        check("mobile: tab strip scrolls", tabs_scroll)
        mpage.screenshot(path=os.path.join(SHOTS, "07-mobile-gate.png"))
        mctx.close()
        browser.close()


run()

fails = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(("PASS " if ok else "FAIL ") + name + (("  — " + detail) if detail and not ok else ""))
print()
js_errors = [e for e in errors
             if "turnstile" not in e.lower()
             and "challenges.cloudflare" not in e.lower()
             and "get.datagatelodge.com" not in e      # local-only CORS
             and "chat.datagatelodge.com" not in e
             and "ERR_FAILED" not in e]                 # pair of the above
print(f"console errors (filtered): {len(js_errors)}")
for e in js_errors[:10]:
    print("  !", e[:200])
print(f"\n{len(results) - len(fails)}/{len(results)} checks passed")
sys.exit(1 if fails or js_errors else 0)
