/**
 * Old Home — laptop-tight hero fit + wide/tall breathing room (v20260731d).
 * Loads hero-fit CSS only. PVL is owned by boot-guard (singleton v20260731b).
 * v20260731d: pin hero-fit 29f (final quiet signal sizes; no 29e shrink jump).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroFitBoot && window.__ohHeroFitBoot >= 202607313) return;
    window.__ohHeroFitBoot = 202607313;

    var base = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var cssHref =
      base + "6a6c919f1183b11fc03668f6_dealality-old-home-hero-fit.v20260729f.css";

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

    var existing =
      document.querySelector('link[data-oh-hero-fit="1"]') ||
      document.querySelector('link[data-oh-fouc="herofit"]');
    if (!existing) {
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = cssHref;
      link.setAttribute("data-oh-hero-fit", "1");
      document.head.appendChild(link);
    } else {
      existing.setAttribute("data-oh-hero-fit", "1");
      if (existing.href && existing.href.indexOf("v20260729f") === -1) {
        existing.href = cssHref;
      }
    }

    // Do not inject PVL — boot-guard loads singleton launcher with data-oh-pvl="31b".

    var wrap = document.getElementById("fsw-secondary-wrap");
    if (wrap) {
      var dismissed = false;
      try {
        dismissed =
          sessionStorage.getItem("dl_platform_video_launcher_dismissed_v3") ===
          "1";
      } catch (_e) {}
      if (dismissed) {
        wrap.removeAttribute("hidden");
        wrap.setAttribute("aria-hidden", "false");
        wrap.setAttribute("data-oh-visible", "1");
      } else {
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
