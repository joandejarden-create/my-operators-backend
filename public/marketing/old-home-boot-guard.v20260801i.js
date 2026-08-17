/**
 * Old Home Boot Guard (v20260801i)
 * Path-gated to /old-home.
 * 01i: globe 01c hi-res texture preload + clearer map material.
 * 01h: testimonials 01a (scroll-triggered autoplay ×2).
 * 01g: restore 1k globe texture; inject critical CSS BEFORE early THREE/globe
 *      so hero type does not flash while THREE contends for bandwidth.
 * 01f: freeform-head Explore size lock + prioritize hero globe.
 * 01d: hero scroll-cue 01a (one-shot smooth scroll, no hero shake).
 * 01c: section-type CSS once + consolidated Google Fonts.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohBootGuard && window.__ohBootGuard >= 202608019) return;
    window.__ohBootGuard = 202608019;

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

    var THREE_JS = "6a6dd9f378d1ce1e6262fb6d_three.r125.min.js";
    var GLOBE_TEX = "6a691e4f3b0bf638b1052fc6_dealality-globe-texture.jpg";
    var GLOBE_JS =
      "6a6ded9c55659289261bd57c_dealality-old-home-hero-globe-bg.v20260801c.js";

    function ensurePreconnect(href, crossOrigin) {
      if (document.querySelector('link[rel="preconnect"][href="' + href + '"]'))
        return;
      var link = document.createElement("link");
      link.rel = "preconnect";
      link.href = href;
      link.setAttribute("data-oh-fonts-preconnect", "1");
      if (crossOrigin) link.crossOrigin = "anonymous";
      head.appendChild(link);
    }

    function preload(href, asType) {
      if (
        document.querySelector(
          'link[rel="preload"][href="' + href + '"][data-oh-globe-preload="1"]'
        )
      )
        return;
      var l = document.createElement("link");
      l.rel = "preload";
      l.as = asType;
      l.href = href;
      l.setAttribute("data-oh-globe-preload", "1");
      head.appendChild(l);
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

    // --- Critical CSS first (before THREE) so hero type settles without flash ---
    [
      "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css",
      "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css",
      "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css",
      "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css",
      "6a6dddcdb2b53bbf47c69fb3_dealality-old-home-freeform-head.v20260801a.css",
      "6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css",
      "6a6dc763d2843e12335fae98_dealality-old-home-pricing.v20260730g.css",
      "6a6c9e1362368e3a654cf326_dealality-old-home-hero-fit.v20260729h.css",
    ].forEach(function (f) {
      var l = document.createElement("link");
      l.rel = "stylesheet";
      l.href = b + f;
      head.appendChild(l);
    });

    // Section type lock — once, last among type CSS.
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

    // --- Hero globe priority path (after CSS kickoff) ---
    preload(b + THREE_JS, "script");
    preload(b + GLOBE_TEX, "image");
    if (GLOBE_JS.indexOf("PLACEHOLDER") === -1) {
      preload(b + GLOBE_JS, "script");
    }

    if (!document.querySelector('script[data-oh-three="r125"]') && !window.THREE) {
      var threeEarly = document.createElement("script");
      threeEarly.src = b + THREE_JS;
      threeEarly.async = true;
      threeEarly.setAttribute("data-oh-three", "r125");
      head.appendChild(threeEarly);
    }

    // Globe JS early (async) — still ahead of FAQs/pricing/testimonials.
    if (
      GLOBE_JS.indexOf("PLACEHOLDER") === -1 &&
      !document.querySelector('script[data-oh-globe="01c"]')
    ) {
      var globeEarly = document.createElement("script");
      globeEarly.src = b + GLOBE_JS;
      globeEarly.async = true;
      globeEarly.setAttribute("data-oh-globe", "01c");
      head.appendChild(globeEarly);
    }

    // Below-fold / secondary scripts — deferred, after globe kickoff.
    [
      "6a6c73926d8f50645f9ae3fd_dealality-old-home-platform-video-launcher.v20260731b.js",
      "6a6de8b4d2843e12336ad613_dealality-old-home-testimonials.v20260801a.js",
      "6a6dc762de9a5a2f588d4d32_dealality-old-home-faqs.v20260801b.js",
      "6a6dc7638f9587090f907db0_dealality-old-home-pricing.v20260730g.js",
      "6a6a1777165816fbdda9f484_old-home-footer-oh.v20260729f.js",
      // Quiet hero tertiary: See How Dealality Works → #oh-how-we-do-it
      "6a6dd78c78d1ce1e62613649_dealality-old-home-hero-scroll-cue.v20260801a.js",
    ].forEach(function (f) {
      var s = document.createElement("script");
      s.defer = true;
      s.src = b + f;
      if (f.indexOf("platform-video-launcher") !== -1 || f.indexOf("PVL_31B") !== -1) {
        s.setAttribute("data-oh-pvl", "31b");
      }
      if (f.indexOf("hero-scroll-cue") !== -1) {
        s.setAttribute("data-oh-hero-scroll-cue", "01a");
      }
      head.appendChild(s);
    });
  } catch (e) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-boot-guard]", e);
    }
  }
})();
