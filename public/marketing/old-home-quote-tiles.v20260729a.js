(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    var href =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a5d73722c376813c9a44d_dealality-old-home-quote-tiles.v20260729a.css";
    if (document.querySelector('link[data-oh-quote-tiles="1"]')) return;
    var link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = href;
    link.setAttribute("data-oh-quote-tiles", "1");
    (document.head || document.documentElement).appendChild(link);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-quote-tiles]", err);
    }
  }
})();
