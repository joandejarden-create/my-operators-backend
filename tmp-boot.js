/**
 * Old Home Boot Guard (v20260801c)
 * Path-gated to /old-home.
 * 01c sprint:
 * - Section-type CSS loaded once (no 200–5200ms re-append loop)
 * - Single consolidated Google Fonts request + preconnect
 * - data-oh-fonts marker so Manual Process boot skips duplicate fonts
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohBootGuard && window.__ohBootGuard >= 202608013) return;
    window.__ohBootGuard = 202608013;

    // Critical: hide Watch Platform Overview until PVL sets data-oh-visible="1"
    // (only after the floating video launcher is dismissed/closed).
    if (!document.getElementById("oh-pvl-secondary-fouc")) {
      var st = document.createElement("style");
      st.id = "oh-pvl-secondary-fouc";
      st.textContent =
        "#fsw-secondary-wrap,#fsw-secondary-wrap.oh-fsw-secondary-wrap{" +
        "display:none!important;visibility:hidden!important;pointer-events:none!important}" +
        '#fsw-secondary-wrap[data-oh-visible="1"]{' +
        "display:block!important;visibility:visible!important;pointer-events:auto!important}";
      (document.head || document.documentElement).appendChild(st);
    }
    var wrapEarly = document.getElementById("fsw-secondary-wrap");
    if (wrapEarly) {
      var dismissed = false;
      try {
        dismissed =
          sessionStorage.getItem("dl_platform_video_launcher_dismissed_v3") ===
          "1";
      } catch (_e) {}
      if (dismissed) {
        wrapEarly.removeAttribute("hidden");
        wrapEarly.setAttribute("aria-hidden", "false");
        wrapEarly.setAttribute("data-oh-visible", "1");
      } else {
        wrapEarly.setAttribute("hidden", "");
        wrapEarly.setAttribute("aria-hidden", "true");
        wrapEarly.setAttribute("data-oh-visible", "0");
      }
    }

    var p = "ht" + "tps:";
    var b = p + "//cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var head = document.head || document.documentElement;

    function ensurePreconnect(href, crossOrigin) {
      var sel =
        'link[rel="preconnect"][href="' +
        href +
        '"]' +
        (crossOrigin ? '[data-oh-fonts-preconnect="1"]' : "");
      if (document.querySelector('link[rel="preconnect"][href="' + href + '"]'))
        return;
      var link = document.createElement("link");
      link.rel = "preconnect";
      link.href = href;
      link.setAttribute("data-oh-fonts-preconnect", "1");
      if (crossOrigin) link.crossOrigin = "anonymous";
      head.appendChild(link);
    }

    /* One font request for page + Manual Process (Fraunces). */
    if (!document.querySelector('link[data-oh-fonts="1"]')) {
      ensurePreconnect(p + "//fonts.googleapis.com", false);
      ensurePreconnect(p + "//fonts.gstatic.com", true);
      var fl = document.createElement("link");
      fl.rel = "stylesheet";
      fl.setAttribute("data-oh-fonts", "1");
      fl.href =
        p +
        "//fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,550;9..144,600&family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap";
      head.appendChild(fl);
    }

    [
      "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css",
      "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css",
      "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css",
      "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css",
      "6a6ca035bc58079a7c5af558_dealality-old-home-freeform-head.v20260729w26.css",
      "6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css",
      "6a6dc763d2843e12335fae98_dealality-old-home-pricing.v20260730g.css",
      "6a6c9e1362368e3a654cf326_dealality-old-home-hero-fit.v20260729h.css",
    ].forEach(function (f) {
      var l = document.createElement("link");
      l.rel = "stylesheet";
      l.href = b + f;
      head.appendChild(l);
    });

    // Section type lock — once, last. No timeout re-append loop (01c).
    var SECTION_TYPE_CSS =
      "6a6d4d297aebfdd31871d780_dealality-old-home-section-type.v20260801a.css";
    if (!document.getElementById("oh-section-type-lock")) {
      var sectionLink = document.createElement("link");
      sectionLink.id = "oh-section-type-lock";
      sectionLink.rel = "stylesheet";
      sectionLink.href = b + SECTION_TYPE_CSS;
      sectionLink.setAttribute("data-oh-section-type", "01a");
      head.appendChild(sectionLink);
    }

    [
      "6a6c9e1369c8fa1cf6e876ca_dealality-old-home-hero-globe-bg.v202607309.js",
      "6a6c73926d8f50645f9ae3fd_dealality-old-home-platform-video-launcher.v20260731b.js",
      "6a6dc762b0fe6b6f28efd740_dealality-old-home-testimonials.v20260731af.js",
      "6a6dc762de9a5a2f588d4d32_dealality-old-home-faqs.v20260801b.js",
      "6a6dc7638f9587090f907db0_dealality-old-home-pricing.v20260730g.js",
      "6a6a1777165816fbdda9f484_old-home-footer-oh.v20260729f.js",
      // Quiet hero tertiary: See How Dealality Works → #oh-how-we-do-it
      "6a6ca7ab8b96f30b1bf37099_dealality-old-home-hero-scroll-cue.v20260731g.js",
    ].forEach(function (f) {
      var s = document.createElement("script");
      s.defer = true;
      s.src = b + f;
      if (f.indexOf("platform-video-launcher") !== -1 || f.indexOf("PVL_31B") !== -1) {
        s.setAttribute("data-oh-pvl", "31b");
      }
      if (f.indexOf("hero-scroll-cue") !== -1) {
        s.setAttribute("data-oh-hero-scroll-cue", "31g");
      }
      head.appendChild(s);
    });
  } catch (e) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-boot-guard]", e);
    }
  }
})();
