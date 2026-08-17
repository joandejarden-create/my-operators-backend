/**
 * Old Home — Modules tab wire (v20260730d)
 * Path-gated to /old-home. Keeps Outcomes/Platform panel switching.
 * Does NOT inject freeform-head w16 (that CSS forced 1-up testimonials and
 * overrode #oh-tt / w22 2-up tiles). Strips any stale w16 link if present.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;

    document.querySelectorAll('link[href*="freeform-head.v20260729w16"]').forEach(function (l) {
      try {
        l.parentNode && l.parentNode.removeChild(l);
      } catch (e) {}
    });

    function sp(el, on) {
      if (!el) return;
      if (on) {
        el.removeAttribute("hidden");
        el.setAttribute("aria-hidden", "false");
        el.style.display = "";
      } else {
        el.setAttribute("hidden", "");
        el.setAttribute("aria-hidden", "true");
        el.style.display = "none";
      }
    }

    function wire() {
      var d1 = document.getElementById("modules-dot-1");
      var d2 = document.getElementById("modules-dot-2");
      var t1 = document.getElementById("modules-tab-outcomes");
      var t2 = document.getElementById("modules-tab-platform");
      var p1 = document.getElementById("modules-panel-outcomes");
      var p2 = document.getElementById("modules-panel-platform");
      if (!p1 || !p2) return;

      function act(w) {
        var a = w === 1;
        if (d1) {
          d1.classList.toggle("is-active", a);
          d1.setAttribute("aria-selected", a ? "true" : "false");
        }
        if (d2) {
          d2.classList.toggle("is-active", !a);
          d2.setAttribute("aria-selected", a ? "false" : "true");
        }
        if (t1) t1.setAttribute("aria-selected", a ? "true" : "false");
        if (t2) t2.setAttribute("aria-selected", a ? "false" : "true");
        sp(p1, a);
        sp(p2, !a);
      }

      function b(el, w) {
        if (!el || el.dataset.ohMods) return;
        el.dataset.ohMods = "1";
        el.addEventListener(
          "click",
          function (e) {
            e.preventDefault();
            e.stopPropagation();
            act(w);
          },
          true
        );
      }

      b(d1, 1);
      b(d2, 2);
      b(t1, 1);
      b(t2, 2);
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wire);
    else wire();
    window.addEventListener("load", wire);
  } catch (e) {
    if (typeof console !== "undefined" && console.error) console.error("[oh-modules-tabs]", e);
  }
})();
/**
 * Old Home — modules tile SVG icons (v20260730f)
 * Path-gated to /old-home. Replaces empty <span> placeholders with SVG icons
 * for What Owners Gain + How Dealality Works cards.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohModulesIcons >= 202607306) return;
    window.__ohModulesIcons = 202607306;

    var STYLE_ID = "oh-modules-icons-30f";
    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "#mod-1-icon,#mod-2-icon,#mod-3-icon,#mod-4-icon,#mod-5-icon,#mod-6-icon,",
        "#modp-1-icon,#modp-2-icon,#modp-3-icon,#modp-4-icon,#modp-5-icon,#modp-6-icon,.mod-icon{",
        "width:72px!important;height:72px!important;min-width:72px!important;min-height:72px!important;",
        "display:flex!important;align-items:center!important;justify-content:center!important;",
        "border-radius:999px!important;margin:0 0 1.25rem!important;border:none!important;",
        "background:radial-gradient(circle at 50% 45%,rgba(140,146,255,.55) 0%,rgba(108,114,255,.22) 42%,rgba(108,114,255,.06) 68%,transparent 78%)!important;",
        "box-shadow:0 0 32px rgba(108,114,255,.32)!important;",
        "}",
        ".mod-icon svg,#mod-1-icon svg,#mod-2-icon svg,#mod-3-icon svg,#mod-4-icon svg,#mod-5-icon svg,#mod-6-icon svg,",
        "#modp-1-icon svg,#modp-2-icon svg,#modp-3-icon svg,#modp-4-icon svg,#modp-5-icon svg,#modp-6-icon svg{",
        "width:52px!important;height:52px!important;flex-shrink:0!important;display:block!important;",
        "}",
        ".mod-icon span,#mod-1-icon span,#mod-2-icon span,#mod-3-icon span,#mod-4-icon span,#mod-5-icon span,#mod-6-icon span,",
        "#modp-1-icon span,#modp-2-icon span,#modp-3-icon span,#modp-4-icon span,#modp-5-icon span,#modp-6-icon span{",
        "display:none!important;",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    var icons = {
      "mod-1-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="20" cy="20" r="16" stroke="#9B8AFB" stroke-width="1.5" stroke-dasharray="3 3"/><circle cx="20" cy="10" r="3" fill="#9B8AFB"/><circle cx="10" cy="26" r="3" fill="#9B8AFB" opacity=".6"/><circle cx="20" cy="26" r="3" fill="#9B8AFB" opacity=".6"/><circle cx="30" cy="26" r="3" fill="#9B8AFB" opacity=".6"/><path d="M20 13v5M14 24l4-4M26 24l-4-4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round"/></svg>',
      "mod-2-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="8" y="12" width="24" height="18" rx="3" stroke="#9B8AFB" stroke-width="1.5"/><path d="M8 18h24" stroke="#9B8AFB" stroke-width="1.2"/><rect x="12" y="22" width="7" height="4" rx="1" fill="#9B8AFB" opacity=".5"/><rect x="21" y="22" width="7" height="4" rx="1" fill="#9B8AFB" opacity=".5"/><path d="M16 9v4M24 9v4" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="15.5" cy="15" r="1" fill="#9B8AFB"/><circle cx="20" cy="15" r="1" fill="#9B8AFB"/><circle cx="24.5" cy="15" r="1" fill="#9B8AFB"/></svg>',
      "mod-3-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="20" cy="14" r="5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M12 30c0-4.4 3.6-8 8-8s8 3.6 8 8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="31" cy="14" r="3" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M28 24c1.7-1.3 3.2-1.5 5-1" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><circle cx="9" cy="14" r="3" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M12 24c-1.7-1.3-3.2-1.5-5-1" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/></svg>',
      "mod-4-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M10 28V14l10-5 10 5v14" stroke="#9B8AFB" stroke-width="1.5" stroke-linejoin="round"/><path d="M10 20h20M20 9v19" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="2 2" opacity=".4"/><circle cx="15" cy="17" r="2" fill="#9B8AFB" opacity=".6"/><circle cx="25" cy="17" r="2" fill="#9B8AFB" opacity=".6"/><circle cx="15" cy="24" r="2" fill="#9B8AFB" opacity=".4"/><circle cx="25" cy="24" r="2" fill="#9B8AFB" opacity=".4"/><path d="M7 28h26" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/></svg>',
      "mod-5-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="6" y="10" width="12" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="22" y="10" width="12" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M10 15h4M10 19h4M10 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><path d="M26 15h4M26 19h4M26 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><path d="M18 17l4 0M18 23l4 0" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="1.5 1.5" opacity=".35"/><circle cx="28" cy="27" r="1.5" fill="#9B8AFB"/><path d="M12 26l2 2 3-4" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
      "mod-6-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="20" cy="20" r="12" stroke="#9B8AFB" stroke-width="1.5"/><path d="M20 12v8l5.5 5.5" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M14 8l-3-3M26 8l3-3" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round"/><circle cx="20" cy="20" r="2" fill="#9B8AFB"/></svg>',
      "modp-1-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="9" y="6" width="22" height="28" rx="2.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M14 13h12M14 18h12M14 23h8" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><circle cx="28" cy="28" r="6" stroke="#9B8AFB" stroke-width="1.5"/><path d="M32 32l3.5 3.5" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/></svg>',
      "modp-2-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="8" cy="20" r="3" fill="#9B8AFB"/><path d="M11 20h5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="12" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><circle cx="20" cy="20" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><circle cx="20" cy="28" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><path d="M16 20l1-5.5M16 20l1 5.5" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M23 12h5M23 20h5M23 28h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" stroke-dasharray="2 2" opacity=".4"/><circle cx="32" cy="12" r="2.5" fill="#9B8AFB" opacity=".4"/><circle cx="32" cy="20" r="2.5" fill="#9B8AFB" opacity=".4"/><circle cx="32" cy="28" r="2.5" fill="#9B8AFB" opacity=".4"/></svg>',
      "modp-3-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="15" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="27" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M21 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M19 16h2" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-dasharray="1.5 1.5" opacity=".4"/><path d="M18 11l2-3M24 11l-2-3" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><circle cx="21" cy="7" r="1.5" fill="#9B8AFB" opacity=".5"/></svg>',
      "modp-4-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="6" y="12" width="20" height="16" rx="2.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M6 16l10 6 10-6" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><rect x="22" y="8" width="12" height="9" rx="2" stroke="#9B8AFB" stroke-width="1.2" opacity=".6"/><path d="M25 12h6M25 14h4" stroke="#9B8AFB" stroke-width="1" stroke-linecap="round" opacity=".4"/><circle cx="30" cy="26" r="5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M30 23v3l2 1.5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round"/></svg>',
      "modp-5-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="5" y="8" width="13" height="24" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="22" y="8" width="13" height="24" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 14h5M9 18h5M9 22h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><path d="M26 14h5M26 18h5M26 22h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><path d="M18 16l4 0M18 20l4 0" stroke="#9B8AFB" stroke-width="1.3" stroke-dasharray="1.5 1.5" opacity=".35"/><path d="M9 27l2 2 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><path d="M26 27l2 2 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
      "modp-6-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M8 22c0 0 4-8 12-8s12 8 12 8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><path d="M8 22c0 0 4 8 12 8s12-8 12-8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="20" cy="22" r="5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="22" r="2" fill="#9B8AFB"/><path d="M17 8h6M20 6v4" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" opacity=".5"/><path d="M31 15l2-2M9 15l-2-2" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/></svg>',
    };

    function apply() {
      var n = 0;
      Object.keys(icons).forEach(function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        if (el.querySelector("svg") && el.getAttribute("data-oh-svg") === "30f") return;
        el.classList.add("mod-icon");
        el.setAttribute("data-oh-svg", "30f");
        el.innerHTML = icons[id];
        n += 1;
      });
      return n;
    }

    function boot() {
      if (apply() >= 6) return;
      window.setTimeout(apply, 50);
      window.setTimeout(apply, 250);
      window.setTimeout(apply, 800);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    window.addEventListener("load", apply);
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-modules-icons]", err);
    }
  }
})();
