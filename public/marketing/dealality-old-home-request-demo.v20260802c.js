/**
 * Old Home — Request a Demo (v20260802c)
 * 02b: /es — "Or request a demo" → "O solicita una demo".
 * 02a: /es — Spanish modal + CTA labels; keep Solicitar acceso on nav.
 * 01d: ops mailto / fallback → hello@aohospitalityadvisors.com
 * 01b: no hover translateY bump on #fsw-demo-link (color/filter only).
 * Path-gated to /, /es, /old-home (and homepage aliases).
 * One-field email modal → POST /api/marketing/demo-request → platform-style toast.
 * Mailto fallback if API is unavailable.
 * Hero: Explore + Request a Demo under With/Without cards.
 * 01a: do NOT force Explore (#fsw-btn) to tall pill — that caused load size flash.
 *     Explore chrome is owned by FOUC / freeform-head / explore-cta (Connected Process).
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/old-home" && path !== "/" && path !== "/home" && !isEs) return;
    if (window.__ohRequestDemo >= 2026080203) return;
    window.__ohRequestDemo = 2026080203;

    var DEFAULT_API = "https://my-operators-backend-staging.up.railway.app";
    var FALLBACK_MAILTO = "hello@aohospitalityadvisors.com";
    var SUCCESS_MSG = isEs
      ? "Gracias — te contactaremos pronto"
      : "Thanks — we'll reach out shortly";
    var LABEL_DEMO = isEs ? "Solicitar Una Demo" : "Request a Demo";
    var LABEL_DEMO_SUBMIT = isEs ? "Solicitar Demo" : "Request Demo";
    var LABEL_ACCESS = isEs ? "Solicitar Acceso" : "Request Access";
    var EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    function apiBase() {
      var b = (window.DEALALITY_API_BASE || window.DEALALITY_API_BASE_URL || "").trim();
      return (b || DEFAULT_API).replace(/\/$/, "");
    }

    function injectCss() {
      // Idempotent — do not remove/re-append (that forced CTA reflow every enhance tick).
      if (document.getElementById("oh-demo-css")) return;
      var css = document.createElement("style");
      css.id = "oh-demo-css";
      css.textContent = [
        "#oh-demo-overlay{position:fixed;inset:0;z-index:10050;display:none;align-items:center;justify-content:center;padding:20px;background:rgba(8,12,24,.62);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}",
        "#oh-demo-overlay.is-open{display:flex}",
        "#oh-demo-panel{width:min(420px,100%);background:linear-gradient(165deg,#0c1424 0%,#111b2e 55%,#0a101c 100%);border:1px solid rgba(255,255,255,.12);border-radius:16px;box-shadow:0 24px 64px rgba(0,0,0,.45);padding:28px 26px 24px;color:#e8eef8;font-family:inherit;position:relative}",
        "#oh-demo-close{position:absolute;top:12px;right:12px;width:36px;height:36px;border:0;border-radius:10px;background:transparent;color:rgba(232,238,248,.7);font-size:22px;line-height:1;cursor:pointer}",
        "#oh-demo-close:hover{background:rgba(255,255,255,.06);color:#fff}",
        "#oh-demo-kicker{display:none}",
        "#oh-demo-title{margin:0 0 8px;font-size:22px;line-height:1.25;font-weight:700;color:#fff}",
        "#oh-demo-lead{margin:0 0 18px;font-size:14px;line-height:1.45;color:rgba(232,238,248,.72)}",
        "#oh-demo-form{display:grid;gap:12px}",
        "#oh-demo-label{font-size:12px;font-weight:600;color:rgba(232,238,248,.78)}",
        "#oh-demo-email{width:100%;box-sizing:border-box;border-radius:10px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.04);color:#fff;padding:12px 14px;font-size:15px;outline:none}",
        "#oh-demo-email:focus{border-color:rgba(215,142,44,.75);box-shadow:0 0 0 3px rgba(215,142,44,.18)}",
        "#oh-demo-email::placeholder{color:rgba(232,238,248,.35)}",
        "#oh-demo-roles{display:flex;flex-wrap:wrap;gap:8px}",
        "#oh-demo-roles button{appearance:none;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:rgba(232,238,248,.85);border-radius:999px;padding:7px 12px;font-size:12px;font-weight:600;cursor:pointer}",
        "#oh-demo-roles button[aria-pressed=true]{border-color:rgba(215,142,44,.8);background:rgba(215,142,44,.16);color:#f4d03f}",
        "#oh-demo-submit{appearance:none;border:0;border-radius:10px;padding:12px 16px;font-size:14px;font-weight:700;cursor:pointer;color:#0b1220;background:linear-gradient(135deg,#f4d03f,#d78e2c);box-shadow:0 8px 20px rgba(215,142,44,.28)}",
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
        /* Hero: CTA pair sits directly under With/Without cards */
        "#fsw-demo-wrap{display:none!important}",
        "#hero #form-subscribe-wrap,#form-subscribe-wrap.oh-fsw-wrap,#form-subscribe-wrap{width:100%!important;max-width:min(100%,26rem)!important;margin-left:0!important;margin-right:auto!important;align-items:flex-start!important;justify-content:flex-start!important}",
        "#hero #fsw-shell,#fsw-shell.oh-fsw-form,#fsw-shell,.oh-fsw-form-wrapper,#fsw-inner.oh-fsw-inner,#fsw-inner{width:auto!important;max-width:100%!important;margin:0!important;display:block!important}",
        /* Hide email chrome so Explore reads as the primary button under the cards */
        "#fsw-field-wrap,#fsw-email,#fsw-glow,#fsw-bg1,#fsw-bg2,#fsw-glow-rotate,#fsw-submit-hit{display:none!important}",
        "#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{position:relative!important;inset:auto!important;display:inline-flex!important;flex-direction:row!important;flex-wrap:nowrap!important;align-items:center!important;justify-content:flex-start!important;gap:8px!important;width:auto!important;height:auto!important;min-height:0!important;max-height:none!important;overflow:visible!important;margin:0!important}",
        /* Explore sizing owned elsewhere — only style Request a Demo here */
        "#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link{appearance:none!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;flex:0 0 auto!important;width:auto!important;height:2.55rem!important;min-height:2.55rem!important;max-height:2.55rem!important;padding:.62rem 1.1rem!important;margin:0!important;box-sizing:border-box!important;border-radius:999px!important;border:1px solid #d78e2c!important;background:#d78e2c!important;color:#0b1220!important;font-size:16px!important;font-weight:600!important;letter-spacing:-.01em!important;line-height:1.2!important;white-space:nowrap!important;text-decoration:none!important;cursor:pointer!important;box-shadow:0 8px 18px rgba(215,142,44,.28)!important;transform:none!important;transition:background .2s ease,border-color .2s ease,filter .15s ease!important}",
        "#fsw-demo-link.oh-fsw-demo-btn:hover,#fsw-demo-link:hover{background:#e09a3a!important;border-color:#e09a3a!important;color:#0b1220!important;filter:brightness(1.03)!important;transform:none!important}",
        "#fsw-demo-link.oh-fsw-demo-btn:focus-visible{outline:2px solid rgba(215,142,44,.85)!important;outline-offset:2px!important}",
        "#cta-band-demo-wrap{margin-top:14px}",
        "#cta-band-demo{appearance:none;background:none;border:0;padding:0;margin:0;font:inherit;cursor:pointer;text-decoration:underline;text-underline-offset:3px;color:rgba(255,255,255,.88);font-size:14px;font-weight:600}",
        "@media (max-width:640px){#hero #form-subscribe-wrap,#form-subscribe-wrap{max-width:100%!important}#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{flex-wrap:wrap!important}.oh-demo-toast{right:12px;left:12px;min-width:0;transform:translateY(-24px)}.oh-demo-toast.show{transform:translateY(0)}}",
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
      if (overlay && overlay.getAttribute("data-oh-demo-ver") === "30j") return overlay;
      if (overlay && overlay.parentNode) {
        try { overlay.parentNode.removeChild(overlay); } catch (_e) {}
      }
      overlay = document.createElement("div");
      overlay.id = "oh-demo-overlay";
      overlay.setAttribute("data-oh-demo-ver", "30j");
      overlay.setAttribute("aria-hidden", "true");
      overlay.innerHTML = isEs
        ? '<div id="oh-demo-panel" role="dialog" aria-modal="true" aria-labelledby="oh-demo-title">' +
          '<button type="button" id="oh-demo-close" aria-label="Cerrar">×</button>' +
          '<p id="oh-demo-kicker" hidden>Solicitud de demo</p>' +
          '<h2 id="oh-demo-title">Solicitud de demo</h2>' +
          '<p id="oh-demo-lead">Deja tu email — te contactaremos para agendar un breve walkthrough.</p>' +
          '<form id="oh-demo-form" novalidate>' +
          '<label id="oh-demo-label" for="oh-demo-email">Email de trabajo</label>' +
          '<input id="oh-demo-email" name="email" type="email" autocomplete="email" required placeholder="tu@empresa.com" />' +
          '<div id="oh-demo-roles" role="group" aria-label="Tu rol (opcional)">' +
          '<button type="button" data-role="owner">Propietario / Inversor</button>' +
          '<button type="button" data-role="brand">Marca</button>' +
          '<button type="button" data-role="operator">Operador</button>' +
          '<button type="button" data-role="advisor">Asesor</button>' +
          "</div>" +
          '<p id="oh-demo-error" role="alert"></p>' +
          '<button type="submit" id="oh-demo-submit">Solicitar demo</button>' +
          "</form>" +
          '<p id="oh-demo-note">Un paso. Sin contraseña. Sin calendario.</p>' +
          "</div>"
        : '<div id="oh-demo-panel" role="dialog" aria-modal="true" aria-labelledby="oh-demo-title">' +
          '<button type="button" id="oh-demo-close" aria-label="Close">×</button>' +
          '<p id="oh-demo-kicker" hidden>Platform Demo Request</p>' +
          '<h2 id="oh-demo-title">Platform Demo Request</h2>' +
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
        (isEs
          ? "Por favor agenda una demo de la plataforma Dealality.\n\nEmail: "
          : "Please schedule a Dealality platform demo.\n\nEmail: ") +
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
        setError(isEs ? "Ingresa un email válido." : "Enter a valid email address.");
        if (input) input.focus();
        return;
      }
      if (btn) {
        btn.disabled = true;
        btn.textContent = isEs ? "Enviando…" : "Sending…";
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
          btn.textContent = LABEL_DEMO_SUBMIT;
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

      // Nav primary CTA → Request Access → /signup (do not open demo modal)
      var navCta = document.getElementById("nav-cta");
      if (navCta) {
        // Drop any prior modal click listener from older script versions in this page load
        if (navCta.getAttribute("data-oh-demo-bound") === "1" && navCta.parentNode) {
          var freshNav = navCta.cloneNode(true);
          freshNav.removeAttribute("data-oh-demo-bound");
          navCta.parentNode.replaceChild(freshNav, navCta);
          navCta = freshNav;
        }
        navCta.textContent = LABEL_ACCESS;
        navCta.setAttribute("href", "https://www.dealality.com/signup");
        navCta.removeAttribute("aria-haspopup");
      }

      // Hero: place Explore + small Request a Demo directly under With/Without cards
      var signals = document.getElementById("hero-signals");
      var outer = document.getElementById("form-subscribe-wrap");
      var shell = document.getElementById("fsw-shell");
      var btnWrap = document.getElementById("fsw-btn-wrap");
      var fswBtn = document.getElementById("fsw-btn");
      var legacy = document.getElementById("fsw-demo-wrap");
      if (legacy && legacy.parentNode) legacy.parentNode.removeChild(legacy);

      // Undo any prior CTA row wrapper
      var oldRow = document.getElementById("oh-hero-cta-row");
      if (oldRow) {
        var stray = oldRow.querySelector("#fsw-demo-link");
        if (stray && btnWrap) btnWrap.appendChild(stray);
        if (shell && oldRow.contains(shell) && outer) {
          var secondary = document.getElementById("fsw-secondary-wrap");
          if (secondary && secondary.parentNode === outer) outer.insertBefore(shell, secondary);
          else outer.appendChild(shell);
        }
        if (oldRow.parentNode) oldRow.parentNode.removeChild(oldRow);
      }

      // Keep the CTA block immediately under the comparison cards
      if (signals && outer && signals.parentNode) {
        if (signals.nextElementSibling !== outer) {
          signals.parentNode.insertBefore(outer, signals.nextSibling);
        }
      }

      var demoBtn = document.getElementById("fsw-demo-link");
      if (btnWrap && fswBtn) {
        if (!demoBtn) {
          demoBtn = document.createElement("button");
          demoBtn.type = "button";
          demoBtn.id = "fsw-demo-link";
          demoBtn.className = "oh-fsw-demo-btn";
          demoBtn.textContent = LABEL_DEMO;
          demoBtn.setAttribute("aria-haspopup", "dialog");
        }
        if (fswBtn.parentNode !== btnWrap) btnWrap.appendChild(fswBtn);
        if (demoBtn.parentNode !== btnWrap) btnWrap.appendChild(demoBtn);
        if (btnWrap.firstElementChild !== fswBtn) btnWrap.insertBefore(fswBtn, btnWrap.firstElementChild);
        if (fswBtn.nextSibling !== demoBtn) btnWrap.insertBefore(demoBtn, fswBtn.nextSibling);
      }
      if (demoBtn) {
        demoBtn.className = "oh-fsw-demo-btn";
        demoBtn.textContent = LABEL_DEMO;
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
        link.textContent = isEs ? "O solicita una demo" : "Or request a demo";
        wrap.appendChild(link);
        ctaBtn.insertAdjacentElement("afterend", wrap);
        wireTrigger(link, true);
      }

      // Pricing brand/operator access → demo modal (role preselect)
      ["pricing-brands-cta", "pricing-operators-cta", "pricing-advisors-cta"].forEach(function (id) {
        var a = document.getElementById(id);
        if (!a || a.getAttribute("data-oh-demo-bound") === "1") return;
        a.setAttribute("data-oh-demo-bound", "1");
        a.setAttribute("href", "#request-demo");
        a.addEventListener("click", function (e) {
          e.preventDefault();
          selectedRole = id.indexOf("brands") >= 0 ? "brand" : id.indexOf("advisors") >= 0 ? "advisor" : "operator";
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

      // Explicit hooks for future Webflow buttons (never rebind #nav-cta)
      var hooks = document.querySelectorAll('[data-dealality-demo-open], [href="#request-demo"]');
      for (var i = 0; i < hooks.length; i++) {
        if (hooks[i].id === "nav-cta") continue;
        wireTrigger(hooks[i], true);
      }
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
