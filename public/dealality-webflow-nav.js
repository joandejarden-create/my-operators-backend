/**
 * Shared navigation for Webflow "/hotel-owner/*" member URLs vs Dealality app shell ("/app#...").
 * Used by deal intake, summaries, franchise flow, and dashboard links to "My Deals" / "My Brand Deals".
 */
(function (global) {
  "use strict";

  function pathnameLooksHotelOwner(pathname) {
    return /^\/hotel-owner(\/|$)/i.test(pathname || "");
  }

  function pathnameLooksAppShell(pathname) {
    /* Shell is served as /app, /app/, and redirects may land on /app.html (see server.js). */
    return /^\/app(?:\.html)?(?:\/|$)/i.test(pathname || "");
  }

  /** True when the parent window is the Dealality app shell (same-origin), even if pathname is / or /index.html. */
  function hasAppShellEmbedParam() {
    try {
      return /(?:^|[?&])appShell=1(?:&|$)/.test(global.location.search || "");
    } catch (e) {
      return false;
    }
  }

  function hasEmbedParam() {
    try {
      return /(?:^|[?&])embed=1(?:&|$)/.test(global.location.search || "");
    } catch (e) {
      return false;
    }
  }

  /** Railway page in iframe on Webflow (dealality.com) — parent does not handle dealality-navigate. */
  function isRailwayEmbedInExternalParent() {
    if (global.top === global.self) return false;
    if (embeddedInDealalityAppShell(global.parent)) return false;
    return hasEmbedParam();
  }

  function getMyDealsEmbedIframeHref(queryString) {
    var parts = ["embed=1"];
    if (hasAppShellEmbedParam()) parts.push("appShell=1");
    if (queryString) parts.push(String(queryString).replace(/^\?/, ""));
    return "/my-deals.html?" + parts.join("&");
  }

  function navigateWithinEmbedIframeToMyDeals(queryString) {
    var path = getMyDealsEmbedIframeHref(queryString);
    if (
      global.DealalityMemberstackAuth &&
      typeof global.DealalityMemberstackAuth.navigateWithAuth === "function"
    ) {
      global.DealalityMemberstackAuth.navigateWithAuth(path);
      return;
    }
    global.location.href = path;
  }

  function embeddedInDealalityAppShell(parentWin) {
    if (hasAppShellEmbedParam() && global.top !== global.self) return true;
    if (!parentWin || parentWin === global) return false;
    try {
      if (pathnameLooksAppShell(parentWin.location.pathname || "")) return true;
    } catch (e) {
      return false;
    }
    try {
      return !!(parentWin.document && parentWin.document.getElementById("frameContainer"));
    } catch (e2) {
      return false;
    }
  }

  function postDealalityNavigateToParent(path) {
    if (!global.parent || global.parent === global) return;
    /* '*' is required for some dev/proxy setups; app.js still checks e.origin === window.location.origin. */
    global.parent.postMessage({ type: "dealality-navigate", path: path }, "*");
  }

  function getParentPathnameSafe() {
    try {
      if (global.parent && global.parent !== global) return global.parent.location.pathname || "";
    } catch (e) {}
    return "";
  }

  /** True if this window or (when embedded) same-origin parent is under /hotel-owner/. */
  function isHotelOwnerWebContext() {
    if (pathnameLooksHotelOwner(global.location.pathname || "")) return true;
    return pathnameLooksHotelOwner(getParentPathnameSafe());
  }

  function mergeHrefWithQuery(base, queryString) {
    if (!queryString) return base;
    return base + (base.indexOf("?") >= 0 ? "&" : "?") + queryString;
  }

  /** Path for dealality-navigate postMessage (parent is app shell or hotel-owner Webflow). Use canonical /my-deals so app.js ROUTES + hash match. */
  function getDealalityMyDealsNavigatePathForParentFrame() {
    try {
      if (global.parent && global.parent !== global) {
        var pp = global.parent.location.pathname || "";
        if (pathnameLooksHotelOwner(pp)) return "/hotel-owner/my-deals-new";
        var ph = String(global.parent.location.hash || "");
        if (pathnameLooksAppShell(pp) || embeddedInDealalityAppShell(global.parent) || ph.indexOf("#/") === 0) return "/my-deals";
      }
    } catch (e) {}
    return "/my-deals.html";
  }

  /** Full-document target for the My Deals list. */
  function getMyDealsTopLevelHref() {
    try {
      if (pathnameLooksHotelOwner(global.location.pathname || "")) return "/hotel-owner/my-deals-new";
    } catch (e) {}
    return "/my-deals.html";
  }

  /** Path for dealality-navigate postMessage when returning to My Brand Deals (app shell / hotel-owner). */
  function getDealalityBrandDealsNavigatePathForParentFrame() {
    try {
      if (global.parent && global.parent !== global) {
        var pp = global.parent.location.pathname || "";
        if (pathnameLooksHotelOwner(pp)) return "/brand-development-dashboard.html";
        var ph = String(global.parent.location.hash || "");
        if (pathnameLooksAppShell(pp) || embeddedInDealalityAppShell(global.parent) || ph.indexOf("#/") === 0) return "/brand-development-dashboard";
      }
    } catch (e) {}
    return "/brand-development-dashboard.html";
  }

  /** Full-document target for My Brand Deals (brand workspace). */
  function getBrandDealsTopLevelHref() {
    try {
      if (pathnameLooksHotelOwner(global.location.pathname || "")) return "/brand-development-dashboard.html";
    } catch (e) {}
    return "/brand-development-dashboard.html";
  }

  function mergeMyDealsHrefWithQuery(queryString) {
    return mergeHrefWithQuery(getMyDealsTopLevelHref(), queryString || "");
  }

  /**
   * @param {object} [options]
   * @param {function} [options.beforeNavigate]
   * @param {string} [options.queryString] optional query without leading "?"
   */
  function navigateToMyDealsList(options) {
    options = options || {};
    if (typeof options.beforeNavigate === "function") {
      try {
        options.beforeNavigate();
      } catch (e) {}
    }
    var qs = options.queryString || "";

    if (global.parent && global.parent !== global) {
      try {
        /* App shell: prefer dealalityAppShellNavigate — assigning the same # URL is a no-op and leaves a stale iframe (e.g. list → deal-summary). Fall back to full top navigation. */
        if (embeddedInDealalityAppShell(global.parent)) {
          if (!qs) {
            try {
              if (global.top !== global && typeof global.top.dealalityAppShellNavigate === "function") {
                global.top.dealalityAppShellNavigate("/my-deals");
                return;
              }
            } catch (eShellNav) {}
          }
          var qShell = qs ? "?" + qs : "";
          var shellMyDealsHref = "/app#/my-deals" + qShell;
          try {
            shellMyDealsHref = (global.top.location.origin || "") + "/app#/my-deals" + qShell;
          } catch (eOr) {}
          global.top.location.href = shellMyDealsHref;
          return;
        }
      } catch (eEmb) {}

      if (isRailwayEmbedInExternalParent()) {
        navigateWithinEmbedIframeToMyDeals(qs);
        return;
      }

      try {
        var navPath = mergeHrefWithQuery(getDealalityMyDealsNavigatePathForParentFrame(), qs);
        postDealalityNavigateToParent(navPath);
        return;
      } catch (e) {}
    }

    var inAppShell = pathnameLooksAppShell(global.location.pathname || "");
    if (inAppShell || (global.location.hash || "").indexOf("#/") === 0) {
      global.location.href = "/app#/my-deals";
      return;
    }
    global.location.href = mergeHrefWithQuery(getMyDealsTopLevelHref(), qs);
  }

  /**
   * @param {object} [options]
   * @param {function} [options.beforeNavigate]
   * @param {string} [options.queryString] optional query without leading "?"
   */
  function navigateToBrandDealsList(options) {
    options = options || {};
    if (typeof options.beforeNavigate === "function") {
      try {
        options.beforeNavigate();
      } catch (e) {}
    }
    var qs = options.queryString || "";

    if (global.parent && global.parent !== global) {
      try {
        if (embeddedInDealalityAppShell(global.parent)) {
          if (!qs) {
            try {
              if (global.top !== global && typeof global.top.dealalityAppShellNavigate === "function") {
                global.top.dealalityAppShellNavigate("/brand-development-dashboard");
                return;
              }
            } catch (eShellNavB) {}
          }
          var qShellB = qs ? "?" + qs : "";
          var shellBrandHref = "/app#/brand-development-dashboard" + qShellB;
          try {
            shellBrandHref = (global.top.location.origin || "") + "/app#/brand-development-dashboard" + qShellB;
          } catch (eOrB) {}
          global.top.location.href = shellBrandHref;
          return;
        }
      } catch (eEmbB) {}

      try {
        var navPathB = mergeHrefWithQuery(getDealalityBrandDealsNavigatePathForParentFrame(), qs);
        postDealalityNavigateToParent(navPathB);
        return;
      } catch (e) {}
    }

    var inAppShellB = pathnameLooksAppShell(global.location.pathname || "");
    if (inAppShellB || (global.location.hash || "").indexOf("#/") === 0) {
      global.location.href = "/app#/brand-development-dashboard";
      return;
    }
    global.location.href = mergeHrefWithQuery(getBrandDealsTopLevelHref(), qs);
  }

  /** Href for "My Deals" links in static HTML or innerHTML (no query). */
  function myDealsHref() {
    return getMyDealsTopLevelHref();
  }

  global.DealalityWebflowNav = {
    pathnameLooksHotelOwner: pathnameLooksHotelOwner,
    pathnameLooksAppShell: pathnameLooksAppShell,
    isHotelOwnerWebContext: isHotelOwnerWebContext,
    hasEmbedParam: hasEmbedParam,
    isRailwayEmbedInExternalParent: isRailwayEmbedInExternalParent,
    getMyDealsEmbedIframeHref: getMyDealsEmbedIframeHref,
    navigateWithinEmbedIframeToMyDeals: navigateWithinEmbedIframeToMyDeals,
    getDealalityMyDealsNavigatePathForParentFrame: getDealalityMyDealsNavigatePathForParentFrame,
    getMyDealsTopLevelHref: getMyDealsTopLevelHref,
    mergeMyDealsHrefWithQuery: mergeMyDealsHrefWithQuery,
    navigateToMyDealsList: navigateToMyDealsList,
    getDealalityBrandDealsNavigatePathForParentFrame: getDealalityBrandDealsNavigatePathForParentFrame,
    getBrandDealsTopLevelHref: getBrandDealsTopLevelHref,
    navigateToBrandDealsList: navigateToBrandDealsList,
    myDealsHref: myDealsHref,
  };
})(typeof window !== "undefined" ? window : this);
