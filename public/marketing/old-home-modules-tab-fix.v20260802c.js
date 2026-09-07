/**
 * Old Home — Benefits outcomes-only + Spanish eyebrows (v20260802c)
 * Path-gated to /, /es, and /old-home (homepage + Spanish locale).
 * 02b: on /es, localize remaining English section eyebrows
 *      (Ecosystem + Insights badge-left).
 * 02a: unlock `/es` so Spanish home matches English single Benefits view.
 * Removes the “How Dealality Works” Benefits tab / panel / dots and
 * leaves What Owners Gain as a static Benefits eyebrow.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && path !== "/old-home" && !isEs) return;
    if (window.__ohModulesTabs >= 202608023) return;
    window.__ohModulesTabs = 202608023;

    document.querySelectorAll('link[href*="freeform-head.v20260729w16"]').forEach(function (l) {
      try {
        l.parentNode && l.parentNode.removeChild(l);
      } catch (e) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[oh-modules-tabs]", e);
        }
      }
    });

    var STYLE_ID = "oh-modules-tabs-02c";
    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "#modules-tab-platform,#modules-panel-platform,#modules-dots,",
        "#modules-dot-1,#modules-dot-2{display:none!important;}",
        "#modules-badge{display:inline-flex!important;align-items:center!important;",
        "overflow:hidden!important;border-radius:999px!important;",
        "border:1px solid rgba(255,255,255,.14)!important;",
        "background:rgba(8,15,37,.92)!important;padding:5px 15px 5px 5px!important;",
        "box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;}",
        "#modules-badge-left,#modules-tab-outcomes{",
        "display:inline-flex!important;align-items:center!important;",
        "padding:0 10px!important;height:32px!important;border:0!important;",
        "border-radius:10px!important;background:#343259!important;color:#fff!important;",
        "font-size:1rem!important;font-weight:500!important;line-height:1!important;",
        "cursor:default!important;pointer-events:none!important;}",
        "#modules-badge-right{display:inline-flex!important;align-items:center!important;",
        "margin-left:15px!important;color:#fff!important;font-size:1rem!important;",
        "font-weight:500!important;line-height:1!important;white-space:nowrap!important;}",
        "#modules-panel-outcomes{display:block!important;}",
        "#modules-panel-outcomes[hidden],#modules-panel-outcomes[aria-hidden='true']{",
        "display:block!important;}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    function hide(el) {
      if (!el) return;
      el.setAttribute("hidden", "");
      el.setAttribute("aria-hidden", "true");
      el.style.display = "none";
    }

    function show(el) {
      if (!el) return;
      el.removeAttribute("hidden");
      el.setAttribute("aria-hidden", "false");
      el.style.display = "";
    }

    function setText(el, value) {
      if (!el || !value) return;
      if (String(el.textContent || "").trim() === value) return;
      el.textContent = value;
    }

    function localizeEyebrows() {
      if (!isEs) return;
      setText(
        document.getElementById("eco-badge-left"),
        "Diseñado Para El Ecosistema Completo De Decisión"
      );
      setText(
        document.getElementById("eco-badge-right"),
        "Liderado Por El Propietario. Conectado A Participantes."
      );
      setText(document.getElementById("insights-badge-left"), "Perspectivas");
    }

    function apply() {
      var badge = document.getElementById("modules-badge");
      var t1 = document.getElementById("modules-tab-outcomes");
      var t2 = document.getElementById("modules-tab-platform");
      var p1 = document.getElementById("modules-panel-outcomes");
      var p2 = document.getElementById("modules-panel-platform");
      var dots = document.getElementById("modules-dots");
      var d1 = document.getElementById("modules-dot-1");
      var d2 = document.getElementById("modules-dot-2");

      hide(t2);
      hide(p2);
      hide(dots);
      hide(d1);
      hide(d2);
      show(p1);
      if (p1) {
        p1.removeAttribute("role");
        p1.removeAttribute("aria-labelledby");
      }

      if (badge && badge.getAttribute("data-oh-benefits") !== "02c") {
        badge.setAttribute("data-oh-benefits", "02c");
        badge.removeAttribute("role");
        badge.removeAttribute("aria-label");
        var left = isEs ? "Beneficios" : "Benefits";
        var right = isEs ? "Lo Que Ganan Los Propietarios" : "What Owners Gain";
        badge.innerHTML =
          '<span id="modules-badge-left">' + left + "</span>" +
          '<span id="modules-badge-right">' + right + "</span>";
      } else if (t1) {
        t1.setAttribute("aria-selected", "true");
        t1.setAttribute("tabindex", "-1");
        t1.textContent = isEs ? "Beneficios" : "Benefits";
      }

      localizeEyebrows();

      if (typeof console !== "undefined" && console.info) {
        console.info("[oh-modules-tabs]", "outcomes-only+eyebrows");
      }
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", apply);
    } else {
      apply();
    }
    window.addEventListener("load", apply);
    window.setTimeout(apply, 400);
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-modules-tabs]", err);
    }
  }
})();
