/**
 * Old Home — laptop-tight hero fit + wide/tall fold-pin CTAs (v20260801b).
 * Loads hero-fit CSS only. PVL is owned by boot-guard.
 * 01b: hero-fit 01b — tall --hr-lh matches FOUC (no post-load crush).
 * 01a: hero-fit 01a — wide+tall CTAs near fold; eyebrow→lead spacing preserved.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home") return;
    if (window.__ohHeroFitBoot && window.__ohHeroFitBoot >= 202608012) return;
    window.__ohHeroFitBoot = 202608012;

    var base = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var cssHref =
      base + "6a6e0f42f5f1ad337d225404_dealality-old-home-hero-fit.v20260801b.css";

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
    if (existing) {
      if (existing.getAttribute("href") !== cssHref) existing.setAttribute("href", cssHref);
      return;
    }

    var link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = cssHref;
    link.setAttribute("data-oh-hero-fit", "1");
    link.setAttribute("data-oh-fouc", "herofit");
    (document.head || document.documentElement).appendChild(link);
  } catch (err) {
    try {
      console.warn("[oh-hero-fit]", err);
    } catch (_e) {}
  }
})();
