/**
 * Old Home Manual Process v1.1.39 boot â€” section HTML is inlined / CDN-loaded.
 * fonts â†’ bind demo CTA â†’ load draw JS (f17: clamped bottom trails + no flash).
 */
(function () {
  var JS_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d92c203751affa3687fb6_old-home-manual-process.v20260801f17.js";
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
    var root = document.getElementById("dealality-manual-process");
    if (!root) {
      console.warn(
        "[dealality-manual-process] section markup missing from page embed"
      );
      return;
    }
    if (root.getAttribute("data-dmp-boot") === "1") return;
    root.setAttribute("data-dmp-boot", "1");
    ensureFonts();
    bindDemoCta();
    loadDrawJs();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
