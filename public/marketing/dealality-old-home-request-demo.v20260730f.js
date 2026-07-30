/**
 * Old Home — Request a Demo (v20260730f)
 * Path-gated to /old-home (and homepage aliases).
 * One-field email modal → POST /api/marketing/demo-request → platform-style toast.
 * Mailto fallback if API is unavailable.
 * Hero: keep Explore in original spot; small Request a Demo box immediately beside it.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home" && path !== "/" && path !== "/home") return;
    if (window.__ohRequestDemo >= 202607306) return;
    window.__ohRequestDemo = 202607306;

    var DEFAULT_API = "https://my-operators-backend-staging.up.railway.app";
    var FALLBACK_MAILTO = "joan@dealality.com";
    var SUCCESS_MSG = "Thanks — we'll reach out shortly";
    var EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    function apiBase() {
      var b = (window.DEALALITY_API_BASE || window.DEALALITY_API_BASE_URL || "").trim();
      return (b || DEFAULT_API).replace(/\/$/, "");
    }

    function injectCss() {
      var prev = document.getElementById("oh-demo-css");
      if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
      var css = document.createElement("style");
      css.id = "oh-demo-css";
      css.textContent = [
        "#oh-demo-overlay{position:fixed;inset:0;z-index:10050;display:none;align-items:center;justify-content:center;padding:20px;background:rgba(8,12,24,.62);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}",
        "#oh-demo-overlay.is-open{display:flex}",
        "#oh-demo-panel{width:min(420px,100%);background:linear-gradient(165deg,#0c1424 0%,#111b2e 55%,#0a101c 100%);border:1px solid rgba(255,255,255,.12);border-radius:16px;box-shadow:0 24px 64px rgba(0,0,0,.45);padding:28px 26px 24px;color:#e8eef8;font-family:inherit;position:relative}",
        "#oh-demo-close{position:absolute;top:12px;right:12px;width:36px;height:36px;border:0;border-radius:10px;background:transparent;color:rgba(232,238,248,.7);font-size:22px;line-height:1;cursor:pointer}",
        "#oh-demo-close:hover{background:rgba(255,255,255,.06);color:#fff}",
        "#oh-demo-kicker{margin:0 0 6px;font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#d69e2e;font-weight:600}",
        "#oh-demo-title{margin:0 0 8px;font-size:22px;line-height:1.25;font-weight:700;color:#fff}",
        "#oh-demo-lead{margin:0 0 18px;font-size:14px;line-height:1.45;color:rgba(232,238,248,.72)}",
        "#oh-demo-form{display:grid;gap:12px}",
        "#oh-demo-label{font-size:12px;font-weight:600;color:rgba(232,238,248,.78)}",
        "#oh-demo-email{width:100%;box-sizing:border-box;border-radius:10px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.04);color:#fff;padding:12px 14px;font-size:15px;outline:none}",
        "#oh-demo-email:focus{border-color:rgba(214,158,46,.7);box-shadow:0 0 0 3px rgba(214,158,46,.18)}",
        "#oh-demo-email::placeholder{color:rgba(232,238,248,.35)}",
        "#oh-demo-roles{display:flex;flex-wrap:wrap;gap:8px}",
        "#oh-demo-roles button{appearance:none;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:rgba(232,238,248,.85);border-radius:999px;padding:7px 12px;font-size:12px;font-weight:600;cursor:pointer}",
        "#oh-demo-roles button[aria-pressed=true]{border-color:rgba(214,158,46,.75);background:rgba(214,158,46,.16);color:#f4d03f}",
        "#oh-demo-submit{appearance:none;border:0;border-radius:10px;padding:12px 16px;font-size:14px;font-weight:700;cursor:pointer;color:#0b1220;background:linear-gradient(135deg,#f4d03f,#d69e2e);box-shadow:0 8px 20px rgba(214,158,46,.28)}",
        "#oh-demo-submit:disabled{opacity:.65;cursor:wait}",
        "#oh-demo-submit:hover:not(:disabled){filter:brightness(1.05)}",
        "#oh-demo-error{display:none;margin:0;font-size:12px;color:#fca5a5}",
        "#oh-demo-error.is-on{display:block}",
        "#oh-demo-note{margin:10px 0 0;font-size:11px;line-height:1.4;color:rgba(232,238,248,.45)}",
        ".oh-demo-toast{position:fixed;top:20px;right:20px;background:rgba(0,0,0,.9);border:1px solid rgba(255,255,255,.15);color:#e2e8f0;padding:12px 16px;border-radius:12px;font-size:13px;z-index:10060;box-shadow:0 8px 24px rgba(0,0,0,.4),0 0 0 1px rgba(255,255,255,.1);display:none;backdrop-filter:blur(12px);min-width:280px;max-width:min(360px,calc(100vw - 40px));transform:translateX(400px);opacity:0;transition:transform .3s ease-out,opacity .3s ease-out}",
        ".oh-demo-toast.show{display:block!important;transform:translateX(0);opacity:1}",
        ".oh-demo-toast .toast-content{display:flex;align-items:center;gap:12px}",
        ".oh-demo-toast .toast-wave-container{width:32px;height:32px;flex-shrink:0}",
        ".oh-demo-toast .wave-container{width:100%;height:100%;position:relative;background:#2a2a2a;border-radius:50%;border:2px solid #888;overflow:hidden;box-shadow:0 0 8px rgba(255,255,255,.1)}",
        ".oh-demo-toast .wave{position:absolute;bottom:0;left:0;right:0;height:100%}",
        ".oh-demo-toast .wave-1{height:40%;background:linear-gradient(45deg,#d69e2e,#f4d03f);animation:oh-demo-toast-wave-1 2s ease-in-out infinite}",
        ".oh-demo-toast .wave-2{height:60%;background:linear-gradient(45deg,#b8860b,#d69e2e);animation:oh-demo-toast-wave-2 2s ease-in-out infinite;animation-delay:.3s}",
        ".oh-demo-toast .wave-3{height:80%;background:linear-gradient(45deg,#f4d03f,#f7dc6f);animation:oh-demo-toast-wave-3 2s ease-in-out infinite;animation-delay:.6s}",
        ".oh-demo-toast .wave-particles{position:absolute;inset:0}",
        ".oh-demo-toast .particle{position:absolute;width:2px;height:2px;background:#d69e2e;border-radius:50%;animation:oh-demo-toast-particle 3s ease-in-out infinite;box-shadow:0 0 4px #d69e2e}",
        ".oh-demo-toast .particle:nth-child(1){left:20%}",
        ".oh-demo-toast .particle:nth-child(2){left:40%;animation-delay:.5s}",
        ".oh-demo-toast .particle:nth-child(3){left:60%;animation-delay:1s}",
        ".oh-demo-toast .particle:nth-child(4){left:80%;animation-delay:1.5s}",
        ".oh-demo-toast .toast-message{flex:1;font-weight:500}",
        "@keyframes oh-demo-toast-wave-1{0%,100%{transform:translateX(-100%) scaleY(1);opacity:.8}50%{transform:translateX(0) scaleY(1.2);opacity:1}}",
        "@keyframes oh-demo-toast-wave-2{0%,100%{transform:translateX(100%) scaleY(1);opacity:.6}50%{transform:translateX(0) scaleY(1.1);opacity:.9}}",
        "@keyframes oh-demo-toast-wave-3{0%,100%{transform:translateX(-100%) scaleY(1);opacity:.4}50%{transform:translateX(0) scaleY(1.3);opacity:.7}}",
        "@keyframes oh-demo-toast-particle{0%{transform:translateY(100%) scale(0);opacity:0}20%{transform:translateY(80%) scale(1);opacity:1}80%{transform:translateY(20%) scale(1);opacity:1}100%{transform:translateY(0) scale(0);opacity:0}}",
        /* Hero: keep Explore in original spot; small Request a Demo box immediately beside it */
        "#fsw-demo-wrap{display:none!important}",
        "#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{display:inline-flex!important;flex-direction:row!important;align-items:center!important;justify-content:flex-end!important;gap:8px!important;height:50px!important;min-height:50px!important;max-height:50px!important;width:auto!important;overflow:visible!important;position:absolute!important;inset:5px 5px 5px auto!important}",
        "#fsw-btn-wrap .oh-fsw-btn,#fsw-btn-wrap #fsw-btn{flex:0 0 auto!important;height:50px!important;min-height:50px!important;line-height:50px!important;padding:0 22px!important;margin:0!important}",
        "#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link{appearance:none!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;flex:0 0 auto!important;width:auto!important;height:40px!important;min-height:40px!important;max-height:40px!important;padding:0 12px!important;margin:0!important;box-sizing:border-box!important;border-radius:10px!important;border:1px solid rgba(255,255,255,.34)!important;background:rgba(255,255,255,.05)!important;color:#fff!important;font-size:12px!important;font-weight:600!important;letter-spacing:-.01em!important;line-height:1!important;white-space:nowrap!important;text-decoration:none!important;cursor:pointer!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.1)!important;transition:background .2s ease,border-color .2s ease,transform .15s ease!important}",
        "#fsw-demo-link.oh-fsw-demo-btn:hover,#fsw-demo-link:hover{background:rgba(183,162,252,.18)!important;border-color:rgba(183,162,252,.6)!important;color:#fff!important;transform:translateY(-1px)!important}",
        "#fsw-demo-link.oh-fsw-demo-btn:focus-visible{outline:2px solid rgba(183,162,252,.7)!important;outline-offset:2px!important}",
        /* Make room inside the email field for Explore + compact demo */
        "#fsw-email.oh-fsw-email,#fsw-email{padding-right:292px!important}",
        "#cta-band-demo-wrap{margin-top:14px}",
        "#cta-band-demo{appearance:none;background:none;border:0;padding:0;margin:0;font:inherit;cursor:pointer;text-decoration:underline;text-underline-offset:3px;color:rgba(255,255,255,.88);font-size:14px;font-weight:600}",
        "@media (max-width:640px){#fsw-email.oh-fsw-email,#fsw-email{padding-right:20px!important}#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{position:relative!important;inset:auto!important;width:100%!important;height:auto!important;min-height:0!important;max-height:none!important;margin-top:10px!important;justify-content:stretch!important;flex-wrap:wrap!important}#fsw-btn-wrap .oh-fsw-btn,#fsw-btn-wrap #fsw-btn{flex:1 1 auto!important;width:auto!important}#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link{flex:0 0 auto!important}.oh-demo-toast{right:12px;left:12px;min-width:0;transform:translateY(-24px)}.oh-demo-toast.show{transform:translateY(0)}}",
      ].join("");
      document.head.appendChild(css);
    }

    var toastTimer = null;
    function ensureToast() {
      var el = document.getElementById("ohDemoToast");
      if (el) return el;
      el = document.createElement("div");
      el.id = "ohDemoToast";
      el.className = "oh-demo-toast";
      el.setAttribute("role", "status");
      el.setAttribute("aria-live", "polite");
      el.innerHTML =
        '<div class="toast-content">' +
        '<div class="toast-wave-container"><div class="wave-container">' +
        '<div class="wave wave-1"></div><div class="wave wave-2"></div><div class="wave wave-3"></div>' +
        '<div class="wave-particles"><div class="particle"></div><div class="particle"></div><div class="particle"></div><div class="particle"></div></div>' +
        "</div></div>" +
        '<div class="toast-message"></div></div>';
      document.body.appendChild(el);
      return el;
    }

    function showToast(message) {
      var toast = ensureToast();
      var msg = toast.querySelector(".toast-message");
      if (msg) msg.textContent = message;
      toast.style.display = "block";
      if (toastTimer) clearTimeout(toastTimer);
      setTimeout(function () {
        toast.classList.add("show");
      }, 10);
      toastTimer = setTimeout(function () {
        toast.classList.remove("show");
        setTimeout(function () {
          toast.style.display = "none";
        }, 300);
      }, 2800);
    }

    var selectedRole = "";

    function ensureModal() {
      var overlay = document.getElementById("oh-demo-overlay");
      if (overlay) return overlay;
      overlay = document.createElement("div");
      overlay.id = "oh-demo-overlay";
      overlay.setAttribute("aria-hidden", "true");
      overlay.innerHTML =
        '<div id="oh-demo-panel" role="dialog" aria-modal="true" aria-labelledby="oh-demo-title">' +
        '<button type="button" id="oh-demo-close" aria-label="Close">×</button>' +
        '<p id="oh-demo-kicker">System demo</p>' +
        '<h2 id="oh-demo-title">Request a Demo</h2>' +
        '<p id="oh-demo-lead">Leave your email — we will reach out to schedule a short walkthrough.</p>' +
        '<form id="oh-demo-form" novalidate>' +
        '<label id="oh-demo-label" for="oh-demo-email">Work email</label>' +
        '<input id="oh-demo-email" name="email" type="email" autocomplete="email" required placeholder="you@company.com" />' +
        '<div id="oh-demo-roles" role="group" aria-label="Your role (optional)">' +
        '<button type="button" data-role="owner">Owner / Investor</button>' +
        '<button type="button" data-role="brand">Brand</button>' +
        '<button type="button" data-role="operator">Operator</button>' +
        '<button type="button" data-role="advisor">Advisor</button>' +
        "</div>" +
        '<p id="oh-demo-error" role="alert"></p>' +
        '<button type="submit" id="oh-demo-submit">Request Demo</button>' +
        "</form>" +
        '<p id="oh-demo-note">One step. No password. No calendar required.</p>' +
        "</div>";
      document.body.appendChild(overlay);

      overlay.addEventListener("click", function (e) {
        if (e.target === overlay) closeModal();
      });
      document.getElementById("oh-demo-close").addEventListener("click", closeModal);
      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape" && overlay.classList.contains("is-open")) closeModal();
      });

      var roles = document.getElementById("oh-demo-roles");
      roles.addEventListener("click", function (e) {
        var btn = e.target.closest("button[data-role]");
        if (!btn) return;
        var role = btn.getAttribute("data-role") || "";
        if (selectedRole === role) {
          selectedRole = "";
          btn.setAttribute("aria-pressed", "false");
          return;
        }
        selectedRole = role;
        var all = roles.querySelectorAll("button[data-role]");
        for (var i = 0; i < all.length; i++) {
          all[i].setAttribute("aria-pressed", all[i] === btn ? "true" : "false");
        }
      });

      document.getElementById("oh-demo-form").addEventListener("submit", onSubmit);
      return overlay;
    }

    function openModal(prefillEmail) {
      injectCss();
      var overlay = ensureModal();
      var input = document.getElementById("oh-demo-email");
      var err = document.getElementById("oh-demo-error");
      if (err) {
        err.textContent = "";
        err.classList.remove("is-on");
      }
      if (input) {
        var seed = (prefillEmail || "").trim();
        if (!seed) {
          var heroEmail = document.getElementById("fsw-email");
          seed = ((heroEmail && heroEmail.value) || "").trim();
        }
        input.value = seed;
      }
      overlay.classList.add("is-open");
      overlay.setAttribute("aria-hidden", "false");
      document.documentElement.style.overflow = "hidden";
      setTimeout(function () {
        if (input) input.focus();
      }, 30);
    }

    function closeModal() {
      var overlay = document.getElementById("oh-demo-overlay");
      if (!overlay) return;
      overlay.classList.remove("is-open");
      overlay.setAttribute("aria-hidden", "true");
      document.documentElement.style.overflow = "";
    }

    function setError(msg) {
      var err = document.getElementById("oh-demo-error");
      if (!err) return;
      err.textContent = msg || "";
      if (msg) err.classList.add("is-on");
      else err.classList.remove("is-on");
    }

    function mailtoFallback(email, role) {
      var subject = encodeURIComponent("Dealality demo request");
      var body = encodeURIComponent(
        "Please schedule a Dealality system demo.\n\nEmail: " +
          email +
          (role ? "\nRole: " + role : "") +
          "\nPage: " +
          location.href +
          "\n"
      );
      window.location.href = "mailto:" + FALLBACK_MAILTO + "?subject=" + subject + "&body=" + body;
    }

    async function postJson(url, payload) {
      var res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      return res;
    }

    async function submitDemo(email, role) {
      var payload = {
        email: email,
        role: role || undefined,
        pageUrl: location.href,
        referrer: document.referrer || undefined,
      };
      var base = apiBase();
      var demoRes = await postJson(base + "/api/marketing/demo-request", payload);
      if (demoRes.ok) return { ok: true, via: "demo-request" };

      // Interim fallback if demo-request is not deployed yet.
      var betaRes = await postJson(base + "/api/marketing/beta-notify", { email: email });
      if (betaRes.ok) return { ok: true, via: "beta-notify" };

      var err = new Error("Request failed");
      err.status = demoRes.status || betaRes.status || 502;
      throw err;
    }

    async function onSubmit(e) {
      e.preventDefault();
      var input = document.getElementById("oh-demo-email");
      var btn = document.getElementById("oh-demo-submit");
      var email = ((input && input.value) || "").trim().toLowerCase();
      setError("");
      if (!EMAIL_RE.test(email)) {
        setError("Enter a valid email address.");
        if (input) input.focus();
        return;
      }
      if (btn) {
        btn.disabled = true;
        btn.textContent = "Sending…";
      }
      try {
        await submitDemo(email, selectedRole);
        closeModal();
        showToast(SUCCESS_MSG);
      } catch (err) {
        // Last resort: open a prefilled email so the request still reaches ops.
        closeModal();
        showToast(SUCCESS_MSG);
        setTimeout(function () {
          mailtoFallback(email, selectedRole);
        }, 350);
      } finally {
        if (btn) {
          btn.disabled = false;
          btn.textContent = "Request Demo";
        }
      }
    }

    function wireTrigger(el, prefillFromHero) {
      if (!el || el.getAttribute("data-oh-demo-bound") === "1") return;
      el.setAttribute("data-oh-demo-bound", "1");
      el.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        openModal(prefillFromHero ? undefined : "");
      });
    }

    function enhanceCtas() {
      injectCss();

      // Nav primary CTA → Request a Demo
      var navCta = document.getElementById("nav-cta");
      if (navCta) {
        navCta.textContent = "Request a Demo";
        navCta.setAttribute("href", "#request-demo");
        navCta.setAttribute("aria-haspopup", "dialog");
        wireTrigger(navCta, true);
      }

      // Hero: restore Explore in original form spot; put small Request a Demo box right beside it
      var outer = document.getElementById("form-subscribe-wrap");
      var shell = document.getElementById("fsw-shell");
      var btnWrap = document.getElementById("fsw-btn-wrap");
      var fswBtn = document.getElementById("fsw-btn");
      var legacy = document.getElementById("fsw-demo-wrap");
      if (legacy && legacy.parentNode) legacy.parentNode.removeChild(legacy);

      // Undo prior wide CTA row that pulled Explore out of place
      var oldRow = document.getElementById("oh-hero-cta-row");
      if (oldRow && outer && shell) {
        var secondary = document.getElementById("fsw-secondary-wrap");
        if (shell.parentNode === oldRow) {
          if (secondary && secondary.parentNode === outer) outer.insertBefore(shell, secondary);
          else outer.insertBefore(shell, oldRow);
        }
        var stray = oldRow.querySelector("#fsw-demo-link");
        if (stray && btnWrap) btnWrap.appendChild(stray);
        if (oldRow.parentNode) oldRow.parentNode.removeChild(oldRow);
      }

      var demoBtn = document.getElementById("fsw-demo-link");
      if (btnWrap && fswBtn) {
        if (!demoBtn) {
          demoBtn = document.createElement("button");
          demoBtn.type = "button";
          demoBtn.id = "fsw-demo-link";
          demoBtn.className = "oh-fsw-demo-btn";
          demoBtn.textContent = "Request a Demo";
          demoBtn.setAttribute("aria-haspopup", "dialog");
        }
        // Explore first, demo immediately after (same absolute button cluster)
        if (fswBtn.parentNode !== btnWrap) btnWrap.appendChild(fswBtn);
        if (demoBtn.parentNode !== btnWrap) btnWrap.appendChild(demoBtn);
        if (btnWrap.firstElementChild !== fswBtn) btnWrap.insertBefore(fswBtn, btnWrap.firstElementChild);
        if (fswBtn.nextSibling !== demoBtn) btnWrap.insertBefore(demoBtn, fswBtn.nextSibling);
      }
      if (demoBtn) {
        demoBtn.className = "oh-fsw-demo-btn";
        demoBtn.textContent = "Request a Demo";
        demoBtn.setAttribute("aria-haspopup", "dialog");
        wireTrigger(demoBtn, true);
      }

      // CTA band secondary
      var ctaBtn = document.getElementById("cta-band-btn");
      if (ctaBtn && !document.getElementById("cta-band-demo-wrap")) {
        var wrap = document.createElement("p");
        wrap.id = "cta-band-demo-wrap";
        var link = document.createElement("button");
        link.type = "button";
        link.id = "cta-band-demo";
        link.textContent = "Or request a demo";
        wrap.appendChild(link);
        ctaBtn.insertAdjacentElement("afterend", wrap);
        wireTrigger(link, true);
      }

      // Pricing brand/operator access → demo modal (role preselect)
      ["pricing-brands-cta", "pricing-operators-cta"].forEach(function (id) {
        var a = document.getElementById(id);
        if (!a || a.getAttribute("data-oh-demo-bound") === "1") return;
        a.setAttribute("data-oh-demo-bound", "1");
        a.setAttribute("href", "#request-demo");
        a.addEventListener("click", function (e) {
          e.preventDefault();
          selectedRole = id.indexOf("brands") >= 0 ? "brand" : "operator";
          openModal();
          var roles = document.getElementById("oh-demo-roles");
          if (roles) {
            var all = roles.querySelectorAll("button[data-role]");
            for (var i = 0; i < all.length; i++) {
              var r = all[i].getAttribute("data-role");
              all[i].setAttribute("aria-pressed", r === selectedRole ? "true" : "false");
            }
          }
        });
      });

      // Explicit hooks for future Webflow buttons
      var hooks = document.querySelectorAll('[data-dealality-demo-open], [href="#request-demo"]');
      for (var i = 0; i < hooks.length; i++) wireTrigger(hooks[i], true);
    }

    window.ohOpenRequestDemo = function (email) {
      openModal(email || "");
    };

    function boot() {
      enhanceCtas();
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    setTimeout(boot, 0);
    setTimeout(boot, 400);
    setTimeout(boot, 1200);
    setTimeout(boot, 2500);
  } catch (err) {
    console.error("oh request demo failed", err);
  }
})();
