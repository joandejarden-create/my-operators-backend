/**
 * Old Home — laptop-tight hero fit + wide/tall breathing room (v20260729d).
 * Keeps 29c short-height compress; opens stack on wide + comfortable height.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroFitBoot && window.__ohHeroFitBoot >= 202607294) return;
    window.__ohHeroFitBoot = 202607294;

    var base = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var cssHref =
      base + "6a6a68e1fedb6e8369ce6830_dealality-old-home-hero-fit.v20260729d.css";
    var jsHref =
      base +
      "6a6a3c5a3eb739f6cf4f747e_dealality-old-home-platform-video-launcher.v20260729i.js";

    var existing = document.querySelector('link[data-oh-hero-fit="1"]');
    if (!existing) {
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = cssHref;
      link.setAttribute("data-oh-hero-fit", "1");
      document.head.appendChild(link);
    } else if (existing.href && existing.href.indexOf("v20260729d") === -1) {
      existing.href = cssHref;
    }

    if (!document.querySelector('script[data-oh-pvl="i"]')) {
      var s = document.createElement("script");
      s.defer = true;
      s.src = jsHref;
      s.setAttribute("data-oh-pvl", "i");
      document.head.appendChild(s);
    }

    var wrap = document.getElementById("fsw-secondary-wrap");
    if (wrap) {
      var dismissed = false;
      try {
        dismissed =
          sessionStorage.getItem("dl_platform_video_launcher_dismissed_v3") ===
          "1";
      } catch (_e) {}
      if (!dismissed) {
        wrap.setAttribute("hidden", "");
        wrap.setAttribute("aria-hidden", "true");
        wrap.setAttribute("data-oh-visible", "0");
      }
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-hero-fit]", err);
    }
  }
})();
