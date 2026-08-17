/**
 * Old Home Manual Process v1.1.20 boot — fetch approved HTML, bind demo CTA, then load draw JS.
 * Companion to old-home-manual-process.v20260801e.css + v20260801e.html + v20260801e.js
 * Path dots underlap Manual Process left edge (Features mf-node pattern).
 */
(function () {
  var HTML_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d4b33fd47713874400020_old-home-manual-process.v20260801e.html";
  var JS_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d4b340f6d5eb78c3c19f1_old-home-manual-process.v20260801e.js";
  var FONT_HREF =
    "https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,550;9..144,600&family=Inter+Tight:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@700;800&display=swap";

  function ensureFonts() {
    if (document.querySelector('link[data-dmp-fonts="1"]')) return;
    var link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = FONT_HREF;
    link.setAttribute("data-dmp-fonts", "1");
    document.head.appendChild(link);
  }

  function loadDrawJs() {
    if (document.querySelector('script[data-dmp-js="1"]')) return;
    var s = document.createElement("script");
    s.src = JS_URL;
    s.defer = true;
    s.setAttribute("data-dmp-js", "1");
    var root = document.getElementById("dealality-manual-process");
    (root || document.body).appendChild(s);
  }

  function openRequestDemo() {
    if (typeof window.ohOpenRequestDemo === "function") {
      window.ohOpenRequestDemo();
      return;
    }
    var demoBtn =
      document.querySelector("[data-dealality-demo-open]") ||
      document.querySelector('a[href="#request-demo"]') ||
      document.querySelector("#fsw-demo-link");
    if (demoBtn) demoBtn.click();
  }

  function bindDemoCta() {
    var root = document.getElementById("dealality-manual-process");
    if (!root) return;
    var btn = root.querySelector('[data-dmp-cta="demo"]');
    if (!btn || btn.getAttribute("data-dmp-bound") === "1") return;
    btn.setAttribute("data-dmp-bound", "1");
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      openRequestDemo();
    });
  }

  function boot() {
    var host = document.getElementById("dealality-manual-process-host");
    if (!host || host.getAttribute("data-dmp-boot") === "1") return;
    host.setAttribute("data-dmp-boot", "1");
    ensureFonts();
    fetch(HTML_URL, { credentials: "omit", cache: "no-store" })
      .then(function (res) {
        if (!res.ok) throw new Error("dmp-html-" + res.status);
        return res.text();
      })
      .then(function (html) {
        host.outerHTML = html;
        bindDemoCta();
        loadDrawJs();
      })
      .catch(function (err) {
        console.warn("[dealality-manual-process]", err);
        var el = document.getElementById("dealality-manual-process-host");
        if (!el) return;
        el.setAttribute("data-dmp-state", "error");
        el.removeAttribute("aria-busy");
        el.textContent = "Unable to load this section. Please refresh and try again.";
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
