(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
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
      "6a6a1d8ac5f82dea4aac9efb_dealality-old-home-freeform-head.v20260729w19.css",
      "6a69b8c44c1a006da8e12d60_dealality-old-home-platform-features.v20260729x1.css",
      "6a69e594ecaa34a1d14f852e_dealality-old-home-pricing.v20260729a.css",
    ].forEach(function (f) {
      var l = document.createElement("link");
      l.rel = "stylesheet";
      l.href = b + f;
      document.head.appendChild(l);
    });
    [
      "6a69d95a9c087b7141f6740a_dealality-old-home-hero-globe-bg.v202607304.js",
      "6a6942f351dd5a2340149b02_dealality-old-home-platform-video-launcher.v20260729h.js",
      "6a6a8739c83f9c69c9343dfe_dealality-old-home-testimonials.v20260730a.js",
      "6a69fe92f7d4f50fb6112eb9_old-home-footer-oh-20260729b.js",
    ].forEach(function (f) {
      var s = document.createElement("script");
      s.defer = true;
      s.src = b + f;
      document.head.appendChild(s);
    });
  } catch (e) {}
})();
