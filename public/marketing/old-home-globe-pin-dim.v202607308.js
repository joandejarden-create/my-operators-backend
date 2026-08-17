/**
 * Path-scoped loader: replace stale hero globe with v202607308
 * (P0 perf + Yucatán/CA pin dim).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohGlobeBuild && window.__ohGlobeBuild >= 202607308) return;
    if (document.querySelector('script[data-oh-globe="308"]')) return;
    var s = document.createElement("script");
    s.defer = true;
    s.setAttribute("data-oh-globe", "308");
    s.src =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/PLACEHOLDER_GLOBE_308.js";
    document.head.appendChild(s);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-globe-pin-dim]", err);
    }
  }
})();
