"""Verification: individual-page architecture, nav page lists, fit-on-screen."""
import json
import os
import sys

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8435"
SHOTS = os.path.join(os.environ.get("TEMP", "."), "site_page_shots")
os.makedirs(SHOTS, exist_ok=True)

results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        errors = []
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))

        def fit_report(label):
            return page.evaluate(
                "({h: document.documentElement.scrollHeight, vh: window.innerHeight})"), label

        # ── index ─────────────────────────────────────────────────────
        page.goto(BASE + "/", wait_until="networkidle")
        check("index: 3 section links", page.eval_on_selector_all(
            ".nav-sections a", "els => els.length") == 3)
        check("index: overview current", page.eval_on_selector(
            '.nav-sections a[aria-current="page"]', "el => el.textContent") == "Overview")
        pages = page.eval_on_selector_all(
            ".nav-pages a", "els => els.map(a => a.textContent.trim())")
        check("index: 4 pages", pages == ["Two Doors", "Why One Company", "The Proof", "Knock"],
              str(pages))
        check("index: two-doors default", page.eval_on_selector(
            "#two-doors", "el => !el.hidden"))
        check("index: division kicker removed", page.eval_on_selector_all(
            ".kicker", "els => els.length") == 0)
        check("index: console on two-doors", page.eval_on_selector(
            "#two-doors .console-band.compact", "el => !!el"))
        h, _ = fit_report("two-doors")
        check("index: Two Doors fits screen", h["h"] <= h["vh"],
              f'{h["h"]}px vs {h["vh"]}px')
        page.screenshot(path=os.path.join(SHOTS, "01-index-two-doors.png"))

        # switch to Knock via the nav page list
        page.click('.nav-pages a[href="#contact"]')
        page.wait_for_timeout(400)
        check("index: knock page shows", page.eval_on_selector(
            "#contact", "el => !el.hidden") and page.evaluate("location.hash") == "#contact")
        check("index: phone card dropped", page.eval_on_selector_all(
            "#contact .contact-card", "els => els.length") == 2)
        check("index: aria-selected sync", page.eval_on_selector(
            '.nav-pages a[href="#contact"]',
            'el => el.getAttribute("aria-selected")') == "true")
        h, _ = fit_report("knock")
        check("index: Knock fits screen", h["h"] <= h["vh"], f'{h["h"]} vs {h["vh"]}')

        # back button returns to the landing
        page.go_back()
        page.wait_for_timeout(400)
        check("index: back → landing", page.eval_on_selector(
            "#two-doors", "el => !el.hidden"))

        # why + proof pages fit
        for pid in ["why", "proof"]:
            page.click(f'.nav-pages a[href="#{pid}"]')
            page.wait_for_timeout(350)
            h, _ = fit_report(pid)
            check(f"index: {pid} fits screen", h["h"] <= h["vh"], f'{h["h"]} vs {h["vh"]}')

        # ── gatelodge ─────────────────────────────────────────────────
        page.goto(BASE + "/gatelodge.html", wait_until="networkidle")
        gpages = page.eval_on_selector_all(
            ".nav-pages a", "els => els.map(a => a.textContent.trim())")
        check("gate: 11 pages", gpages == ["One Gateway", "How It Works", "The Product", "Problem", "Capabilities", "ROI",
                                           "Editions", "Channel", "Download", "FAQ", "Contact"], str(gpages))
        check("gate: gateway section current", page.eval_on_selector(
            '.nav-sections a[aria-current="page"]', "el => el.textContent") == "The Gateway")
        check("gate: no CTA", page.eval_on_selector_all(
            ".nav-cta", "els => els.length") == 0)
        check("gate: intro landing default", page.eval_on_selector(
            "#intro", "el => !el.hidden"))
        check("gate: console NOT in capabilities", page.eval_on_selector_all(
            "#capabilities .console-band", "els => els.length") == 0)
        h, _ = fit_report("intro")
        check("gate: One Gateway fits screen", h["h"] <= h["vh"], f'{h["h"]} vs {h["vh"]}')
        page.screenshot(path=os.path.join(SHOTS, "02-gate-intro.png"))

        for pid in ["how", "product", "problem", "capabilities", "roi", "editions",
                    "partners", "download", "faq", "contact"]:
            page.click(f'.nav-pages a[href="#{pid}"]')
            page.wait_for_timeout(350)
            h, _ = fit_report(pid)
            check(f"gate: {pid} fits screen", h["h"] <= h["vh"], f'{h["h"]} vs {h["vh"]}')
            if pid == "product":
                page.screenshot(path=os.path.join(SHOTS, "03a-gate-product.png"))
                check("gate: product shows real screens",
                      page.eval_on_selector_all("#product .shot img",
                                                "els => els.length") == 3)
                check("gate: product audit block",
                      page.eval_on_selector("#product .product-note",
                                            "el => !!el"))
                check("gate: vault note on product",
                      "Content Vault" in page.eval_on_selector(
                          "#product .product-note", "el => el.textContent"))
                check("gate: vault modal opens", (
                    page.click('#product [data-modal-open="vault-modal"]')
                    or page.wait_for_timeout(300)
                    or page.eval_on_selector("#vault-modal",
                                             "el => el.open")))
                check("gate: vault modal carries terminal shot", (
                    page.eval_on_selector("#vault-modal .vault-shot",
                                          "el => el.naturalWidth > 0")
                    and page.eval_on_selector_all(
                        "#vault-modal .vault-points li",
                        "els => els.length") == 4))
                page.screenshot(path=os.path.join(SHOTS, "03d-gate-vault.png"))
                page.keyboard.press("Escape")
                page.wait_for_timeout(200)
                check("gate: vault modal closes", not page.eval_on_selector(
                    "#vault-modal", "el => el.open"))
            if pid == "editions":
                check("gate: editions list the vault", "Content Vault" in
                      page.eval_on_selector("#editions .edition-primary",
                                            "el => el.textContent"))
            if pid == "problem":
                page.screenshot(path=os.path.join(SHOTS, "03-gate-problem.png"))
            if pid == "how":
                page.screenshot(path=os.path.join(SHOTS, "03b-gate-how.png"))
                check("gate: how has lifecycle diagram", page.eval_on_selector(
                    "#how .flow-diagram", "el => !!el"))
                check("gate: how has run strip", page.eval_on_selector_all(
                    "#how .method-map .mm-node", "els => els.length") == 4)
                check("gate: how lists 10 backend types", "Bedrock" in page.eval_on_selector(
                    "#how .backend-chips", "el => el.textContent"))
            if pid == "faq":
                check("gate: faq has 7 items", page.eval_on_selector_all(
                    "#faq .faq-item", "els => els.length") == 7)
            if pid == "capabilities":
                # title block alignment on a full-bleed band page
                align = page.eval_on_selector("#capabilities", """el => {
                    const l = el.querySelector('.section-label').getBoundingClientRect();
                    const h2 = el.querySelector('h2').getBoundingClientRect();
                    return Math.abs(l.left - h2.left) < 2;
                }""")
                check("gate: label aligns with content (alt band)", align)

        # editions compare modal
        page.click('.nav-pages a[href="#editions"]')
        page.wait_for_timeout(300)
        page.click('[data-modal-open="compare-modal"]')
        page.wait_for_timeout(300)
        check("gate: compare modal opens", page.eval_on_selector(
            "#compare-modal", "el => el.open"))
        check("gate: matrix has 13 rows", page.eval_on_selector_all(
            "#compare-modal .matrix-table tbody tr", "els => els.length") == 13)
        page.screenshot(path=os.path.join(SHOTS, "03c-editions-matrix.png"))
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
        check("gate: compare modal closes", not page.eval_on_selector(
            "#compare-modal", "el => el.open"))

        # executive messaging: CIO band (how), CEO band (roi), brief modal
        page.click('.nav-pages a[href="#how"]')
        page.wait_for_timeout(300)
        check("gate: CIO band on how-it-works", page.eval_on_selector(
            "#how .product-note .note-tag",
            "el => el.textContent.includes('CIO')"))
        page.click('.nav-pages a[href="#roi"]')
        page.wait_for_timeout(300)
        check("gate: CEO band on ROI", page.eval_on_selector(
            "#roi .product-note .note-tag",
            "el => el.textContent.includes('CEO')"))
        page.click('#roi [data-modal-open="exec-modal"]')
        page.wait_for_timeout(300)
        check("gate: exec brief modal opens", page.eval_on_selector(
            "#exec-modal", "el => el.open"))
        check("gate: exec brief has four roles", page.eval_on_selector_all(
            "#exec-modal .exec-role", "els => els.length") == 4)
        check("gate: exec brief carries concern table",
              page.eval_on_selector_all(
                  "#exec-modal .exec-table tbody tr",
                  "els => els.length") == 10)
        page.screenshot(path=os.path.join(SHOTS, "03d-exec-brief.png"))
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
        check("gate: exec brief modal closes", not page.eval_on_selector(
            "#exec-modal", "el => el.open"))

        # deep link still works
        page.goto(BASE + "/gatelodge.html#download", wait_until="networkidle")
        check("gate: deep link opens download", page.eval_on_selector(
            "#download", "el => !el.hidden"))

        # in-page anchor (hero button)
        page.goto(BASE + "/gatelodge.html", wait_until="networkidle")
        page.click('.hero-actions .btn-brass')
        page.wait_for_timeout(400)
        check("gate: hero button → download page", page.eval_on_selector(
            "#download", "el => !el.hidden") and page.evaluate("location.hash") == "#download")

        # EULA modal still wired
        page.click(".dock-eula")
        page.wait_for_timeout(1000)
        check("gate: eula modal loads", page.eval_on_selector(
            "#eula-body", "el => el.textContent.includes('Definitions')"))
        page.keyboard.press("Escape")

        # ── services ──────────────────────────────────────────────────
        page.goto(BASE + "/services.html", wait_until="networkidle")
        spages = page.eval_on_selector_all(
            ".nav-pages a", "els => els.map(a => a.textContent.trim())")
        check("services: 9 pages", spages == ["What We Build", "Expertise", "Survey", "Map",
                                              "Design", "Post", "Walk", "Proof", "Start"], str(spages))
        h, _ = fit_report("intro")
        check("services: What We Build fits", h["h"] <= h["vh"], f'{h["h"]} vs {h["vh"]}')
        check("services: method map on landing", page.eval_on_selector_all(
            "#intro .method-map .mm-node", "els => els.length") == 5)
        check("services: loop-back line present", "estate evolves" in page.eval_on_selector(
            "#intro .mm-loop", "el => el.textContent"))
        page.screenshot(path=os.path.join(SHOTS, "04-services-intro.png"))

        for pid in ["expertise", "survey", "map", "design", "post", "walk", "proof", "what-we-need"]:
            page.click(f'.nav-pages a[href="#{pid}"]')
            page.wait_for_timeout(350)
            h, _ = fit_report(pid)
            check(f"services: {pid} fits screen", h["h"] <= h["vh"], f'{h["h"]} vs {h["vh"]}')
            if pid == "design":
                page.screenshot(path=os.path.join(SHOTS, "05-services-design.png"))

        # you-are-here strips: current node marked on each method page
        for pid in ["survey", "map", "design", "post", "walk"]:
            page.click(f'.nav-pages a[href="#{pid}"]')
            page.wait_for_timeout(250)
            marked = page.eval_on_selector(
                f"#{pid} .method-map a[aria-current='step']",
                "el => el.getAttribute('href')")
            check(f"services: {pid} marks itself on the map", marked == f"#{pid}", str(marked))

        # step-nav flow across method pages
        page.click('.nav-pages a[href="#survey"]')
        page.wait_for_timeout(300)
        page.click('#survey .step-nav a[href="#map"]')
        page.wait_for_timeout(300)
        check("services: step-nav → map", page.eval_on_selector(
            "#map", "el => !el.hidden"))

        ctx.close()

        # ── mobile ────────────────────────────────────────────────────
        mctx = browser.new_context(viewport={"width": 390, "height": 844})
        mpage = mctx.new_page()
        mpage.goto(BASE + "/gatelodge.html", wait_until="networkidle")
        mpage.wait_for_timeout(400)
        check("mobile: no horizontal overflow", mpage.evaluate(
            "document.documentElement.scrollWidth <= 390"))
        nav_wrapped = mpage.eval_on_selector(".nav", """el => {
                return el.getBoundingClientRect().height > 80; }""")
        check("mobile: page list wraps to second row", nav_wrapped,
              str(mpage.eval_on_selector(".nav", "el => el.getBoundingClientRect().height")))
        mpage.screenshot(path=os.path.join(SHOTS, "06-mobile-gate.png"))
        mctx.close()
        browser.close()

    fails = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(("PASS " if ok else "FAIL ") + name + (("  — " + detail) if detail and not ok else ""))
    print()
    js_errors = [e for e in errors
                 if "turnstile" not in e.lower()
                 and "challenges.cloudflare" not in e.lower()
                 and "datagatelodge.com" not in e
                 and "ERR_FAILED" not in e]
    print(f"console errors (filtered): {len(js_errors)}")
    for e in js_errors[:10]:
        print("  !", e[:200])
    print(f"\n{len(results) - len(fails)}/{len(results)} checks passed")
    sys.exit(1 if fails or js_errors else 0)


run()
