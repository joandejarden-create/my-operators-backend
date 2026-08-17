/**
 * Dealality marketing nav width lock (v20260801c)
 * Match Home `#nav` 1120px content band on Symbol `.navbar-2` pages.
 * Path-gated: public marketing pages only (not platform apps).
 * 01c: also setProperty(...,'important') — Webflow inline/right pad was winning CSS sheet.
 */
(function () {
  "use strict";

  try {
    var path =
      ((window.location && window.location.pathname) || "")
        .replace(/\/+$/, "")
        .toLowerCase() || "/";

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

    if (window.__dcMktNavWidth && window.__dcMktNavWidth >= 202608013) return;
    window.__dcMktNavWidth = 202608013;

    var STYLE_ID = "dc-mkt-nav-width-01c";
    var PAD = "calc((100% - 1120px) / 2 + clamp(1.5rem, 4vw, 3rem))";
    var PAD_NARROW = "clamp(1.25rem, 4vw, 2.5rem)";

    function padValue() {
      try {
        return window.matchMedia("(max-width: 1200px)").matches
          ? PAD_NARROW
          : PAD;
      } catch (e) {
        return PAD;
      }
    }

    function injectCss() {
      if (document.getElementById(STYLE_ID)) return;
      var style = document.createElement("style");
      style.id = STYLE_ID;
      style.textContent = [
        "/* Marketing Symbol nav → Home 1120 content band */",
        ".navbar-2.w-nav,",
        ".navbar-2{",
        "width:100%!important;",
        "max-width:none!important;",
        "}",
        ".navbar-2.w-nav > .navbar_content,",
        ".navbar-2 > .navbar_content{",
        "width:100%!important;",
        "max-width:none!important;",
        "margin-left:0!important;",
        "margin-right:0!important;",
        "box-sizing:border-box!important;",
        "padding-left:" + PAD + "!important;",
        "padding-right:" + PAD + "!important;",
        "padding-inline:" + PAD + "!important;",
        "}",
        ".navbar-2 > .navbar_content > .navbar_content{",
        "width:100%!important;",
        "max-width:none!important;",
        "box-sizing:border-box!important;",
        "padding-left:0!important;",
        "padding-right:0!important;",
        "padding-inline:0!important;",
        "margin:0!important;",
        "}",
        "@media (max-width:1200px){",
        ".navbar-2.w-nav > .navbar_content,",
        ".navbar-2 > .navbar_content{",
        "padding-left:" + PAD_NARROW + "!important;",
        "padding-right:" + PAD_NARROW + "!important;",
        "padding-inline:" + PAD_NARROW + "!important;",
        "}",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(style);
    }

    function applyInline(root) {
      var doc = root || document;
      var nodes = doc.querySelectorAll(
        ".navbar-2 > .navbar_content, .navbar-2.w-nav > .navbar_content"
      );
      var pad = padValue();
      for (var i = 0; i < nodes.length; i++) {
        var el = nodes[i];
        // Only outermost direct child of .navbar-2
        if (!el.parentElement || !el.parentElement.classList.contains("navbar-2")) {
          continue;
        }
        el.style.setProperty("padding-left", pad, "important");
        el.style.setProperty("padding-right", pad, "important");
        el.style.setProperty("padding-inline", pad, "important");
        el.style.setProperty("box-sizing", "border-box", "important");
        el.style.setProperty("width", "100%", "important");
        el.style.setProperty("max-width", "none", "important");
      }
      var nested = doc.querySelectorAll(
        ".navbar-2 > .navbar_content > .navbar_content"
      );
      for (var j = 0; j < nested.length; j++) {
        nested[j].style.setProperty("padding-left", "0px", "important");
        nested[j].style.setProperty("padding-right", "0px", "important");
        nested[j].style.setProperty("padding-inline", "0px", "important");
      }
    }

    function run() {
      injectCss();
      applyInline(document);
    }

    run();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    }
    window.addEventListener("resize", function () {
      applyInline(document);
    });
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[dc_mkt_nav_width]", err);
    }
  }
})();
