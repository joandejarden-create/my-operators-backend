/**
 * Old Home Boot Guard (v20260730q)
 * Path-gated to /old-home. Same critical CSS/JS as 30b + testimonials v20260730n.
 * Does not inject freeform-head w16. Leaves How We Do It / modules-tab scripts alone.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohBootGuard && window.__ohBootGuard >= 202607317) return;
    window.__ohBootGuard = 202607317;

    var p = "ht" + "tps:";
    var b = p + "//cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var fl = document.createElement("link");
    fl.rel = "stylesheet";
    fl.href =
      p +
      "//fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap";
    document.head.appendChild(fl);
    [
      "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css",
      "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css",
      "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css",
      "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css",
      "6a6a7cb895b20766ec5595f9_dealality-old-home-freeform-head.v20260729w22.css",
      "6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css",
      "6a69e594ecaa34a1d14f852e_dealality-old-home-pricing.v20260729a.css",
      "6a6a7598332f85a5833260a6_dealality-old-home-hero-fit.v20260729e.css",
    ].forEach(function (f) {
      var l = document.createElement("link");
      l.rel = "stylesheet";
      l.href = b + f;
      document.head.appendChild(l);
    });
    [
      "6a6a5bdfff4fad2bc2596d50_dealality-old-home-hero-globe-bg.v202607307.js",
      "6a6a3c5a3eb739f6cf4f747e_dealality-old-home-platform-video-launcher.v20260729i.js",
      "6a6bbc932665516a2d413677_dealality-old-home-testimonials.v20260730n.js",
    ].forEach(function (f) {
      var s = document.createElement("script");
      s.defer = true;
      s.src = b + f;
      document.head.appendChild(s);
    });
  } catch (e) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-boot-guard]", e);
    }
  }
})();
