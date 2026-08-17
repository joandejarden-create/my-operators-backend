/**
 * Path-scoped loader: replace stale hero globe with v202607305
 * (pins behind rotating headline dim after reveal).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohGlobeBuild && window.__ohGlobeBuild >= 202607305) return;
    if (document.querySelector('script[data-oh-globe="305"]')) return;
    var s = document.createElement("script");
    s.defer = true;
    s.setAttribute("data-oh-globe", "305");
    s.src =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a385925234b7e3dcbeb04_dealality-old-home-hero-globe-bg.v202607305b.js";
    document.head.appendChild(s);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-globe-pin-dim]", err);
    }
  }
})();
