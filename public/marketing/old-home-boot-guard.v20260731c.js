/**
 * Old Home Boot Guard (v20260731c)
 * Path-gated to /old-home. Same as 31b + restores footer-oh iframe opener
 * (ohOpenOpportunityReview) for Explore / Opportunity Review CTAs.
 * Does not inject freeform-head w16. Leaves How We Do It / modules-tab scripts alone.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohBootGuard && window.__ohBootGuard >= 202607312) return;
    window.__ohBootGuard = 202607312;

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
      "6a6bd91e6302c929aa4de707_dealality-old-home-pricing.v20260730f.css",
      "6a6a7598332f85a5833260a6_dealality-old-home-hero-fit.v20260729e.css",
    ].forEach(function (f) {
      var l = document.createElement("link");
      l.rel = "stylesheet";
      l.href = b + f;
      document.head.appendChild(l);
    });
    [
      "6a6a5bdfff4fad2bc2596d50_dealality-old-home-hero-globe-bg.v202607307.js",
      "6a6c48e226a07d130ad339c3_dealality-old-home-platform-video-launcher.v20260731a.js",
      "6a6bc0f69cfdffe1f22fe71e_dealality-old-home-testimonials.v20260730s.js",
      "6a6bd91ed75786767cf1bec4_dealality-old-home-pricing.v20260730f.js",
      "6a6a1777165816fbdda9f484_old-home-footer-oh.v20260729f.js",
    ].forEach(function (f) {
      var s = document.createElement("script");
      s.defer = true;
      s.src = b + f;
      if (f.indexOf("platform-video-launcher") !== -1) {
        s.setAttribute("data-oh-pvl", "i");
      }
      document.head.appendChild(s);
    });
  } catch (e) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-boot-guard]", e);
    }
  }
})();
