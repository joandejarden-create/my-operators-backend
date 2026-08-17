/**
 * Old Home — compress hero to first viewport + overview re-open link behavior.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroFitBoot) return;
    window.__ohHeroFitBoot = 1;

    var base = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var cssHref =
      base + "6a6a425eeb1d6d2a7d03519b_dealality-old-home-hero-fit.v20260729b.css";
    var jsHref =
      base +
      "6a6a3c5a3eb739f6cf4f747e_dealality-old-home-platform-video-launcher.v20260729i.js";

    if (!document.querySelector('link[data-oh-hero-fit="1"]')) {
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = cssHref;
      link.setAttribute("data-oh-hero-fit", "1");
      document.head.appendChild(link);
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
