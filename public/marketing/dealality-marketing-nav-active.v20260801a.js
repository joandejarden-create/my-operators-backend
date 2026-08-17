/**
 * Dealality marketing nav active-state (v20260801a)
 * Path-gated: public marketing pages only (not platform apps).
 * Disables Webflow w--current links in Dealality Navbar (visible, not clickable).
 */
(function () {
  try {
    var path = ((window.location && window.location.pathname) || "")
      .replace(/\/+$/, "")
      .toLowerCase() || "/";
    if (
      path.indexOf("/brand") === 0 ||
      path.indexOf("/hotel-owner") === 0 ||
      path.indexOf("/hotel-management-company") === 0 ||
      path.indexOf("/asset-manager") === 0 ||
      path.indexOf("/member") === 0
    ) {
      return;
    }
    if (window.__dcMktNavActive && window.__dcMktNavActive >= 202608011) return;
    window.__dcMktNavActive = 202608011;

    function injectStyles() {
      if (document.getElementById("dc-mkt-nav-active-01a")) return;
      var style = document.createElement("style");
      style.id = "dc-mkt-nav-active-01a";
      style.textContent = [
        ".w-nav a.w--current,",
        ".w-nav .w--current{",
        "pointer-events:none!important;",
        "cursor:default!important;",
        "opacity:.72!important;",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(style);
    }

    function disableCurrent(root) {
      if (!root) return;
      var nodes = root.querySelectorAll("a.w--current, .w--current");
      for (var i = 0; i < nodes.length; i++) {
        var el = nodes[i];
        var a = el.tagName === "A" ? el : el.closest ? el.closest("a") : null;
        if (!a) continue;
        a.setAttribute("aria-current", "page");
        a.setAttribute("aria-disabled", "true");
        a.tabIndex = -1;
        if (!a.__dcMktNavBound) {
          a.__dcMktNavBound = true;
          a.addEventListener(
            "click",
            function (e) {
              e.preventDefault();
              e.stopPropagation();
            },
            true
          );
        }
      }
    }

    function run() {
      injectStyles();
      var navs = document.querySelectorAll(".w-nav, .navbar, [data-animation]");
      if (!navs.length) {
        disableCurrent(document);
        return;
      }
      for (var i = 0; i < navs.length; i++) disableCurrent(navs[i]);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[dc_mkt_nav_active]", err);
    }
  }
})();
