/**
 * Dealality marketing nav width lock (v20260801a)
 * Match Home `#nav` 1120px content band on Symbol `.navbar-2` pages.
 * Path-gated: public marketing pages only (not platform apps).
 */
(function () {
  "use strict";

  try {
    var path =
      ((window.location && window.location.pathname) || "")
        .replace(/\/+$/, "")
        .toLowerCase() || "/";

    // Platform apps keep their own chrome.
    if (
      path.indexOf("/brand") === 0 ||
      path.indexOf("/hotel-owner") === 0 ||
      path.indexOf("/hotel-management-company") === 0 ||
      path.indexOf("/asset-manager") === 0 ||
      path.indexOf("/member") === 0 ||
      path.indexOf("/operator") === 0 ||
      path.indexOf("/third-party") === 0
    ) {
      return;
    }

    // Home already uses #nav / .oh-nav with freeform-head 1120 band.
    if (path === "/" || path === "/old-home") return;

    if (window.__dcMktNavWidth && window.__dcMktNavWidth >= 202608011) return;
    window.__dcMktNavWidth = 202608011;

    var STYLE_ID = "dc-mkt-nav-width-01a";
    // Same horizontal inset as Home freeform-head `#nav` / `.oh-nav`.
    var PAD =
      "calc((100% - 1120px) / 2 + clamp(1.5rem, 4vw, 3rem))";
    var PAD_NARROW = "clamp(1.25rem, 4vw, 2.5rem)";

    function inject() {
      if (document.getElementById(STYLE_ID)) return;
      var style = document.createElement("style");
      style.id = STYLE_ID;
      style.textContent = [
        "/* Marketing Symbol nav → Home 1120 content band */",
        ".navbar-2,",
        ".navbar-2.w-nav{",
        "width:100%!important;",
        "max-width:none!important;",
        "}",
        ".navbar-2 > .navbar_content{",
        "width:100%!important;",
        "max-width:none!important;",
        "margin-left:0!important;",
        "margin-right:0!important;",
        "box-sizing:border-box!important;",
        "padding-left:" + PAD + "!important;",
        "padding-right:" + PAD + "!important;",
        "}",
        /* Nested Symbol chrome keeps internal flex; do not double-pad. */
        ".navbar-2 > .navbar_content > .navbar_content{",
        "width:100%!important;",
        "max-width:none!important;",
        "box-sizing:border-box!important;",
        "padding-left:0!important;",
        "padding-right:0!important;",
        "margin:0!important;",
        "}",
        "@media (max-width:1200px){",
        ".navbar-2 > .navbar_content{",
        "padding-left:" + PAD_NARROW + "!important;",
        "padding-right:" + PAD_NARROW + "!important;",
        "}",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(style);
    }

    // Header-applied: inject immediately so Symbol nav matches Home before paint.
    inject();
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[dc_mkt_nav_width]", err);
    }
  }
})();
