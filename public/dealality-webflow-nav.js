/**
 * Shared navigation for Webflow "/hotel-owner/*" member URLs vs Dealality app shell ("/app#...").
 * Used by deal intake, summaries, franchise flow, and dashboard links to "My Deals".
 */
(function (global) {
  "use strict";

  function pathnameLooksHotelOwner(pathname) {
    return /^\/hotel-owner(\/|$)/i.test(pathname || "");
  }

  function pathnameLooksAppShell(pathname) {
    return /^\/app(\/|$)/i.test(pathname || "");
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

  /** Path for dealality-navigate postMessage (parent is app shell or hotel-owner Webflow). */
  function getDealalityMyDealsNavigatePathForParentFrame() {
    try {
      if (global.parent && global.parent !== global) {
        var pp = global.parent.location.pathname || "";
        if (pathnameLooksHotelOwner(pp)) return "/hotel-owner/my-deals-new";
        var ph = String(global.parent.location.hash || "");
        if (pathnameLooksAppShell(pp) || ph.indexOf("#/") === 0) return "/my-deals.html";
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
        var navPath = mergeHrefWithQuery(getDealalityMyDealsNavigatePathForParentFrame(), qs);
        global.parent.postMessage({ type: "dealality-navigate", path: navPath }, global.location.origin);
        return;
      } catch (e) {}
    }

    var inAppShell = global.location.pathname === "/app" || global.location.pathname === "/app/";
    if (inAppShell || (global.location.hash || "").indexOf("#/") === 0) {
      global.location.href = "/app#/my-deals.html";
      return;
    }
    global.location.href = mergeHrefWithQuery(getMyDealsTopLevelHref(), qs);
  }

  /** Href for "My Deals" links in static HTML or innerHTML (no query). */
  function myDealsHref() {
    return getMyDealsTopLevelHref();
  }

  global.DealalityWebflowNav = {
    pathnameLooksHotelOwner: pathnameLooksHotelOwner,
    pathnameLooksAppShell: pathnameLooksAppShell,
    isHotelOwnerWebContext: isHotelOwnerWebContext,
    getDealalityMyDealsNavigatePathForParentFrame: getDealalityMyDealsNavigatePathForParentFrame,
    getMyDealsTopLevelHref: getMyDealsTopLevelHref,
    mergeMyDealsHrefWithQuery: mergeMyDealsHrefWithQuery,
    navigateToMyDealsList: navigateToMyDealsList,
    myDealsHref: myDealsHref,
  };
})(typeof window !== "undefined" ? window : this);
