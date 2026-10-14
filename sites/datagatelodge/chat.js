/* Gatekeeper chat widget — Gatelodge support assistant.
   Zero dependencies. Renders ONLY when the chat service answers
   /api/config (config-gated: nothing appears until the backend is
   live). SSE streaming; escalation collects an email for the team. */
(function () {
  "use strict";

  var BASE = window.CHAT_API_BASE || "https://chat.datagatelodge.com";
  var state = {
    session: null, open: false, busy: false,
    turnstileId: null, cfg: null, escalated: false
  };

  function boot() {
    fetch(BASE + "/api/config", { mode: "cors" })
      .then(function (r) { return r.json(); })
      .then(function (cfg) {
        if (!cfg || !cfg.name) return;
        state.cfg = cfg;
        mount();
      })
      .catch(function () { /* service unreachable — no widget */ });
  }

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text) e.textContent = text;
    return e;
  }

  var dom = {};

  function mount() {
    var btn = el("button", "gk-btn");
    btn.type = "button";
    btn.setAttribute("aria-label", "Open the Gatekeeper chat");
    btn.innerHTML = '<svg viewBox="0 0 64 64" aria-hidden="true">'
      + '<path d="M14 50 V30 A18 18 0 0 1 50 30 V50" fill="none" '
      + 'stroke="currentColor" stroke-width="5"/>'
      + '<rect x="26" y="30" width="12" height="20" rx="1" '
      + 'fill="currentColor"/></svg>';
    btn.addEventListener("click", toggle);

    var panel = el("div", "gk-panel");
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-label", "Gatekeeper support chat");
    panel.hidden = true;
    panel.innerHTML =
      '<div class="gk-head">'
      + '<div><span class="gk-title">THE GATEKEEPER</span>'
      + '<span class="gk-sub mono">gatelodge support · answers from the lodge</span></div>'
      + '<button type="button" class="gk-close" aria-label="Close">&times;</button>'
      + "</div>"
      + '<div class="gk-msgs" aria-live="polite"></div>'
      + '<div class="gk-input-row">'
      + '<input type="text" class="gk-input" placeholder="Ask about pricing, setup, licenses…" '
      + 'maxlength="4000" autocomplete="off">'
      + '<button type="button" class="gk-send">Send</button>'
      + "</div>"
      + '<div class="gk-foot mono">'
      + '<a href="#" class="gk-human">talk to a human</a>'
      + '<span class="gk-brand">datagatelodge.solutions</span>'
      + "</div>";

    document.body.appendChild(btn);
    document.body.appendChild(panel);
    dom = { btn: btn, panel: panel,
            msgs: panel.querySelector(".gk-msgs"),
            input: panel.querySelector(".gk-input"),
            send: panel.querySelector(".gk-send"),
            close: panel.querySelector(".gk-close"),
            human: panel.querySelector(".gk-human") };

    dom.close.addEventListener("click", toggle);
    dom.send.addEventListener("click", send);
    dom.human.addEventListener("click", function (ev) {
      ev.preventDefault();
      askEmail("Of course — can I take your email so the team follows up?");
    });
    dom.input.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter" && !ev.shiftKey) { ev.preventDefault(); send(); }
    });
  }

  function toggle() {
    state.open = !state.open;
    dom.panel.hidden = !state.open;
    dom.btn.classList.toggle("gk-open", state.open);
    if (state.open) {
      if (!state.session) start();
      dom.input.focus();
    }
  }

  function addMsg(role, text) {
    var m = el("div", "gk-msg " + role);
    m.textContent = text;
    dom.msgs.appendChild(m);
    dom.msgs.scrollTop = dom.msgs.scrollHeight;
    return m;
  }

  function start() {
    addMsg("agent", "Welcome to the gatehouse. Ask me anything about "
      + "Gatelodge — pricing, setup, backends, licenses.");
    var proceed = function (token) {
      fetch(BASE + "/api/chat/session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ turnstile_token: token || "" })
      }).then(function (r) { return r.json(); })
        .then(function (d) {
          if (d && d.session_id) { state.session = d.session_id; return; }
          addMsg("agent", "The gate needs a moment — please try reopening "
            + "the chat. If it persists, email support@datagatelodge.com.");
        }).catch(function () {
          addMsg("agent", "The gate is temporarily unreachable — please "
            + "email support@datagatelodge.com.");
        });
    };
    if (state.cfg && state.cfg.turnstile_sitekey && window.turnstile) {
      var holder = el("div", "gk-turnstile");
      dom.msgs.appendChild(holder);
      state.turnstileId = window.turnstile.render(holder, {
        sitekey: state.cfg.turnstile_sitekey,
        theme: "dark",
        callback: function (token) {
          holder.remove();
          proceed(token);
        }
      });
    } else {
      proceed("");
    }
  }

  function send() {
    var text = dom.input.value.trim();
    if (!text || state.busy || !state.session) return;
    dom.input.value = "";
    addMsg("user", text);
    state.busy = true;
    dom.send.disabled = true;

    var bubble = addMsg("agent", "");
    var cur = "";

    fetch(BASE + "/api/chat/message", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: state.session, text: text })
    }).then(function (r) {
      var reader = r.body.getReader();
      var dec = new TextDecoder();
      var buf = "";
      function pump() {
        return reader.read().then(function (x) {
          buf += dec.decode(x.value || new Uint8Array(), { stream: !x.done });
          var i;
          while ((i = buf.indexOf("\n\n")) >= 0) {
            handleBlock(buf.slice(0, i), bubble, cur = "");
            buf = buf.slice(i + 2);
          }
          if (!x.done) return pump();
        });
      }
      return pump();
    }).catch(function () {
      bubble.textContent = cur ||
        "The gate dropped the connection — please email "
        + "support@datagatelodge.com.";
    }).finally(function () {
      state.busy = false;
      dom.send.disabled = false;
    });

    function handleBlock(block, bubble2) {
      var ev = "", data = "";
      block.split("\n").forEach(function (line) {
        if (line.indexOf("event: ") === 0) ev = line.slice(7);
        else if (line.indexOf("data: ") === 0) data += line.slice(6);
      });
      var d = {};
      try { d = JSON.parse(data || "{}"); } catch (e) {}
      if (ev === "token") {
        bubble2.textContent += (d.t || "");
        dom.msgs.scrollTop = dom.msgs.scrollHeight;
      } else if (ev === "done") {
        if (!bubble2.textContent) bubble2.textContent = "(no answer)";
      } else if (ev === "escalate") {
        bubble2.textContent = d.message ||
          "I want to get this exactly right — can I take your email?";
        askEmail();
      }
    }
  }

  function askEmail(prompt) {
    if (state.escalated) return;
    if (prompt) addMsg("agent", prompt);
    var row = el("div", "gk-email-row");
    var input = el("input", "gk-email-input");
    input.type = "email";
    input.placeholder = "you@company.com";
    var btn = el("button", "gk-email-send", "Send");
    btn.type = "button";
    row.appendChild(input);
    row.appendChild(btn);
    dom.msgs.appendChild(row);
    dom.msgs.scrollTop = dom.msgs.scrollHeight;
    input.focus();

    function submit() {
      var email = input.value.trim();
      if (!email) return;
      btn.disabled = true;
      fetch(BASE + "/api/chat/escalate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: state.session, email: email })
      }).then(function (r) { return r.json(); })
        .then(function () {
          state.escalated = true;
          row.remove();
          addMsg("agent", "Thank you — the team will follow up at "
            + email + ". Your transcript is with them.");
        }).catch(function () {
          btn.disabled = false;
          addMsg("agent", "That didn't send — try again, or email "
            + "support@datagatelodge.com directly.");
        });
    }
    btn.addEventListener("click", submit);
    input.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter") { ev.preventDefault(); submit(); }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
