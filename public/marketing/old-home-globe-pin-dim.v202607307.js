/**
 * Path-scoped loader: replace stale hero globe with v202607307
 * (geography dim: Yucatán + Central America only).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohGlobeBuild && window.__ohGlobeBuild >= 202607307) return;
    if (document.querySelector('script[data-oh-globe="307"]')) return;
    var s = document.createElement("script");
    s.defer = true;
    s.setAttribute("data-oh-globe", "307");
    s.src =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a5bdfff4fad2bc2596d50_dealality-old-home-hero-globe-bg.v202607307.js";
    document.head.appendChild(s);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-globe-pin-dim]", err);
    }
  }
})();
