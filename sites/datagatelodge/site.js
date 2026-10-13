/* Gatelodge site — ROI calculator + scroll reveals. No dependencies. */
(function () {
  "use strict";

  var ENTERPRISE_PER_USER = 39;   // $/user/month

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
