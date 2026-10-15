/* Gatelodge site — ROI calculator + trial download + buy wiring. */
(function () {
  "use strict";

  var ENTERPRISE_PER_USER = 39;   // $/user/month

  /* ── licensing service wiring ──────────────────────────────────────── */
  // The licensing service (get.datagatelodge.com in production). Override
  // with window.TRIAL_API_BASE for local testing.
  var API = window.TRIAL_API_BASE || "https://get.datagatelodge.com";
  var turnstileWidgetId = null;

  // fetch config (payment link + turnstile sitekey) when the page loads
  function wireConfig() {
    fetch(API + "/api/config").then(function (r) { return r.json(); })
      .then(function (cfg) {
        if (cfg.payment_link) {
          var buy = document.getElementById("buy-key-btn");
          if (buy) {
            buy.href = cfg.payment_link;
            buy.hidden = false;
          }
          var note = document.getElementById("buy-note");
          if (note) note.textContent = "one-time · secure checkout · instant key";
        }
        if (cfg.turnstile_sitekey && window.turnstile) {
          turnstileWidgetId = window.turnstile.render(
            ".cf-turnstile",
            { sitekey: cfg.turnstile_sitekey, theme: "dark" });
        }
      }).catch(function () {
        // service unreachable — form still shows, submit will error clearly
      });
  }

  function wireTrialForm() {
    var form = document.getElementById("trial-form");
    if (!form) return;
    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var email = document.getElementById("trial-email").value.trim();
      var biz = document.getElementById("trial-biz").value.trim();
      var msg = document.getElementById("trial-msg");
      var btn = document.getElementById("trial-submit");
      var token = (turnstileWidgetId !== null &&
                   window.turnstile)
        ? window.turnstile.getResponse(turnstileWidgetId) : "";
      if (!email) return;
      btn.disabled = true;
      btn.textContent = "Checking…";
      msg.hidden = true;
      fetch(API + "/api/trial-request", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: email, business_name: biz, turnstile_token: token }),
      }).then(function (r) {
        if (r.status === 429) throw new Error(
          "Too many requests — please wait a while and try again.");
        if (!r.ok) return r.json().then(function (e) {
          throw new Error((e.detail && e.detail.message) ||
                          "request failed"); });
        return r.json();
      }).then(function (data) {
        var card = document.getElementById("trial-card");
        var link = document.getElementById("trial-download-link");
        link.href = API + (data.download || "/download/community");
        form.hidden = true;
        document.getElementById("trial-success").hidden = false;
        var mailed = document.getElementById("trial-email-note");
        if (mailed) mailed.hidden = !data.email_sent;
      }).catch(function (e) {
        msg.textContent = e.message || "something went wrong — try again";
        msg.hidden = false;
        btn.disabled = false;
        btn.textContent = "Get the download";
        if (window.turnstile && turnstileWidgetId !== null) {
          window.turnstile.reset(turnstileWidgetId);
        }
      });
    });
  }

  wireConfig();
  wireTrialForm();
  initTabs();
  initModals();

  /* ── pages ────────────────────────────────────────────────────────── */
  // The nav carries a contextual page list (.nav-pages). Links swap the
  // page panels in place — no reload. Panels keep their historic ids
  // (#problem, #download, #survey…), so old deep links keep working.
  // Clicks pushState (back/forward steps through pages); native hash
  // changes (in-page anchors) and popstate both resolve through the
  // same activation path.
  function initTabs() {
    var list = document.querySelector(".nav-pages");
    if (!list) return;
    var tabs = Array.prototype.slice.call(
      list.querySelectorAll('[role="tab"]'));

    function panelFor(tab) {
      var href = tab.getAttribute("href") || "";
      return document.getElementById(href.replace(/^#/, ""));
    }

    function activate(tab, focus) {
      tabs.forEach(function (t) {
        var on = t === tab;
        t.setAttribute("aria-selected", on ? "true" : "false");
        var p = panelFor(t);
        if (p) p.hidden = !on;
      });
      if (focus) tab.focus();
      // keep the chosen link visible inside a scrolling page strip
      if (list.scrollWidth > list.clientWidth) {
        var b = tab.offsetLeft, w = tab.offsetWidth;
        list.scrollTo({ left: b + w / 2 - list.clientWidth / 2,
                        behavior: focus ? "smooth" : "auto" });
      }
    }

    function goTo(tab) {
      activate(tab);
      var p = panelFor(tab);
      if (p && history.pushState) history.pushState(null, "", "#" + p.id);
      window.scrollTo(0, 0);   // a page switch shows the page from its top
    }

    tabs.forEach(function (tab, i) {
      tab.addEventListener("click", function (ev) {
        ev.preventDefault();
        goTo(tab);
      });
      tab.addEventListener("keydown", function (ev) {
        var d = ev.key === "ArrowRight" ? 1 :
                ev.key === "ArrowLeft" ? -1 : 0;
        if (!d) return;
        ev.preventDefault();
        goTo(tabs[(i + d + tabs.length) % tabs.length]);
      });
    });

    function resolve() {
      var h = (location.hash || "").replace(/^#/, "");
      var tab = h && list.querySelector('[href="#' + h + '"]');
      if (tab) { activate(tab); return true; }
      return false;
    }

    window.addEventListener("hashchange", function () {
      if (resolve()) window.scrollTo(0, 0);
    });
    window.addEventListener("popstate", function () {
      if (!resolve()) activate(tabs[0]);   // back to the section landing
      window.scrollTo(0, 0);
    });

    activate(tabs[0]);
    resolve();
  }

  /* ── pop-up windows (EULA, edition comparison, …) ──────────────────── */
  // Any [data-modal-open] opens the <dialog class="modal"> it names;
  // the ✕ button ([data-modal-close]) and a click on the dimmed area
  // close it. The EULA text itself is lifted from /eula.html on first
  // open, so there is exactly one source of truth for the agreement.
  function initModals() {
    Array.prototype.forEach.call(
      document.querySelectorAll("[data-modal-open]"),
      function (el) {
        el.addEventListener("click", function (ev) {
          ev.preventDefault();
          var d = document.getElementById(
            el.getAttribute("data-modal-open"));
          if (d && d.showModal) d.showModal();
        });
      });

    Array.prototype.forEach.call(
      document.querySelectorAll("dialog.modal"),
      function (d) {
        d.addEventListener("click", function (ev) {
          if (ev.target === d) d.close();
        });
        var c = d.querySelector("[data-modal-close]");
        if (c) c.addEventListener("click", function () { d.close(); });
      });

    var eula = document.getElementById("eula-modal");
    var body = document.getElementById("eula-body");
    var loaded = false;
    if (eula && body) {
      // <dialog> fires no "open" event of its own, so the lazy load
      // rides on the same clicks that open it
      Array.prototype.forEach.call(
        document.querySelectorAll('[data-modal-open="eula-modal"]'),
        function (b) {
          b.addEventListener("click", function () {
            if (loaded) return;
            loaded = true;
            fetch("eula.html").then(function (r) { return r.text(); })
              .then(function (html) {
                var doc = new DOMParser().parseFromString(
                  html, "text/html");
                var src = doc.querySelector(".legal-doc");
                if (src) body.innerHTML = src.innerHTML;
              })
              .catch(function () {
                body.innerHTML =
                  '<p>Could not load the agreement here — ' +
                  '<a href="eula.html">open it as a page</a> instead.</p>';
              });
          });
        });
    }
  }


  var $ = function (id) { return document.getElementById(id); };

  var els = {
    team: $("team"), spend: $("spend"), cache: $("cache"), route: $("route"),
    teamOut: $("team-out"), spendOut: $("spend-out"),
    cacheOut: $("cache-out"), routeOut: $("route-out"),
    gross: $("r-gross"), cost: $("r-cost"), net: $("r-net"), mult: $("r-mult")
  };

  function fmtUSD(n) {
    return "$" + Math.round(n).toLocaleString("en-US");
  }

  function paintFill(input) {
    var min = parseFloat(input.min), max = parseFloat(input.max);
    var pct = ((parseFloat(input.value) - min) / (max - min)) * 100;
    input.style.setProperty("--fill", pct + "%");
  }

  function recalc() {
    var team = parseFloat(els.team.value);
    var spend = parseFloat(els.spend.value);
    var cache = parseFloat(els.cache.value) / 100;
    var route = parseFloat(els.route.value) / 100;

    els.teamOut.textContent = String(team);
    els.spendOut.textContent = fmtUSD(spend);
    els.cacheOut.textContent = Math.round(cache * 100) + "%";
    els.routeOut.textContent = Math.round(route * 100) + "%";

    // combined rate: cache first, routing applies to what's left
    var combined = cache + route * (1 - cache);
    var gross = spend * combined;
    var cost = team * ENTERPRISE_PER_USER;
    var net = gross - cost;
    var mult = cost > 0 ? gross / cost : 0;

    els.gross.textContent = fmtUSD(gross) + "/mo";
    els.cost.textContent = fmtUSD(cost) + "/mo";
    els.net.textContent = (net >= 0 ? "" : "−") +
      fmtUSD(Math.abs(net)) + "/mo";
    els.mult.textContent = mult.toFixed(1) + "×";
    els.net.style.color = net >= 0 ? "" : "var(--danger)";
  }

  ["team", "spend", "cache", "route"].forEach(function (k) {
    var el = els[k];
    if (!el) return;
    paintFill(el);
    el.addEventListener("input", function () { paintFill(el); recalc(); });
  });
  if (els.team) recalc();

  // stagger the problem-ledger rows
  var rows = document.querySelectorAll(".ledger-row.reveal");
  for (var i = 0; i < rows.length; i++) {
    rows[i].style.setProperty("--i", String(i));
  }

  // scroll reveals
  if ("IntersectionObserver" in window &&
      !window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { e.target.classList.add("in"); io.unobserve(e.target); }
      });
    }, { threshold: 0.12, rootMargin: "0px 0px -40px 0px" });
    document.querySelectorAll(".reveal").forEach(function (el) { io.observe(el); });
  } else {
    document.querySelectorAll(".reveal").forEach(function (el) {
      el.classList.add("in");
    });
  }
})();
