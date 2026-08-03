/**
 * Dealality sitewide public-page analytics.
 * Tracks page_land / scroll / engagement on marketing pages that do not
 * already have a specialized collector (home / old-home).
 *
 * Posts to POST {DEALALITY_API_BASE}/api/marketing/landing-events
 * Surfaces: insights | opportunity_review | site
 */
(function (global) {
  "use strict";

  if (global.__dealalityPublicAnalytics) return;
  global.__dealalityPublicAnalytics = 20260803;

  var SESSION_KEY = "dl_landing_sid_v1";
  var VISITOR_KEY = "dl_landing_vid_v1";
  var VISITOR_COOKIE = "dl_landing_vid_v1";
  var SCROLL_DEPTHS = [25, 50, 75, 100];
  var ENGAGEMENT_SECONDS = [30, 60, 120];
  var DEFAULT_API_BASE = "https://my-operators-backend-staging.up.railway.app";

  function normalizePath(pathname) {
    var p = String(pathname || "/").toLowerCase();
    if (p.length > 1 && p.charAt(p.length - 1) === "/") p = p.slice(0, -1);
    return p || "/";
  }

  function stripLocale(path) {
    if (path === "/es") return "/";
    if (path.indexOf("/es/") === 0) return path.slice(3) || "/";
    return path;
  }

  function isHomePath(path) {
    var rest = stripLocale(path);
    return (
      rest === "/" ||
      rest === "/old-home" ||
      rest === "/home-legacy"
    );
  }

  function isAuthPath(rest) {
    return (
      rest === "/signup" ||
      rest === "/signup-new" ||
      rest === "/log-in" ||
      rest === "/login" ||
      rest === "/join" ||
      rest === "/verify" ||
      rest === "/reset-password" ||
      rest === "/forget-password" ||
      rest === "/forgot-password" ||
      rest === "/password-set-success" ||
      rest === "/not-allowed" ||
      rest === "/401" ||
      rest.indexOf("/signup") === 0 ||
      rest.indexOf("/log-in") === 0 ||
      rest.indexOf("/login") === 0
    );
  }

  function isAppPath(rest) {
    if (rest === "/404" || rest === "/search") return false;
    return (
      /^\/(my-|brand-|operator-|deal-|company-|user-|profile|dashboard|add-|edit-|franchise|fee-|clause-|financial-|loi-|market-|partner-overview|partner-directory|outreach|valuation|third-party|new-deal|admin|api|static|assets|js\/|css\/|fonts\/|landing-analytics|health|deal-capture|the-radar|brand-education|password-set)/i.test(
        rest
      ) || /\-new$/.test(rest)
    );
  }

  function surfaceFor(path) {
    var rest = stripLocale(path);
    if (rest === "/insights" || rest.indexOf("/insights/") === 0) return "insights";
    if (rest === "/opportunity-review" || rest.indexOf("/opportunity-review/") === 0) {
      return "opportunity_review";
    }
    return "site";
  }

  function languageFor(path) {
    if (path === "/es" || path.indexOf("/es/") === 0) return "es";
    var docLang = (global.document && global.document.documentElement.lang) || "";
    return String(docLang).trim().slice(0, 2).toLowerCase() || "en";
  }

  var path = normalizePath(global.location && global.location.pathname);
  var rest = stripLocale(path);

  // Home has dealality-old-home-analytics / iframe landing collectors.
  if (isHomePath(path) || global.__ohLandingAnalytics) return;
  if (isAuthPath(rest) || isAppPath(rest)) return;
  // If a dedicated Insights collector already ran, do not double page_land.
  if (surfaceFor(path) === "insights" && global.__dealalityInsightsAnalytics) return;

  var SURFACE = surfaceFor(path);
  var LANGUAGE = languageFor(path);
  var state = {
    startedAt: Date.now(),
    firstScrollSent: false,
    scrollDepths: {},
    engagementSent: {},
  };

  function apiBase() {
    var b = String(
      global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || ""
    ).trim();
    return (b || DEFAULT_API_BASE).replace(/\/$/, "");
  }

  function readVisitorCookie() {
    try {
      var parts = String(global.document.cookie || "").split(";");
      for (var i = 0; i < parts.length; i++) {
        var piece = parts[i].trim();
        if (piece.indexOf(VISITOR_COOKIE + "=") === 0) {
          return decodeURIComponent(piece.slice(VISITOR_COOKIE.length + 1));
        }
      }
    } catch (_e) {}
    return null;
  }

  function writeVisitorCookie(id) {
    if (!id || !global.document) return;
    try {
      var secure = global.location.protocol === "https:" ? "; Secure" : "";
      global.document.cookie =
        VISITOR_COOKIE +
        "=" +
        encodeURIComponent(id) +
        "; path=/; max-age=" +
        60 * 60 * 24 * 400 +
        "; SameSite=Lax" +
        secure;
    } catch (_e2) {}
  }

  function visitorId() {
    try {
      var existing = localStorage.getItem(VISITOR_KEY);
      if (existing) {
        writeVisitorCookie(existing);
        return existing;
      }
    } catch (_e) {}
    var fromCookie = readVisitorCookie();
    if (fromCookie) {
      try {
        localStorage.setItem(VISITOR_KEY, fromCookie);
      } catch (_e2) {}
      return fromCookie;
    }
    var id =
      "dlv_" +
      Date.now().toString(36) +
      "_" +
      Math.random().toString(36).slice(2, 11);
    try {
      localStorage.setItem(VISITOR_KEY, id);
    } catch (_e3) {}
    writeVisitorCookie(id);
    return id;
  }

  function sessionId() {
    try {
      var existing = sessionStorage.getItem(SESSION_KEY);
      if (existing) return existing;
      var id =
        "dl_" +
        Date.now().toString(36) +
        "_" +
        Math.random().toString(36).slice(2, 11);
      sessionStorage.setItem(SESSION_KEY, id);
      return id;
    } catch (_e) {
      return "dl_" + Date.now();
    }
  }

  function params() {
    try {
      return new URLSearchParams(global.location.search);
    } catch (_e) {
      return new URLSearchParams();
    }
  }

  function deviceClass() {
    var w = global.innerWidth || 0;
    if (w < 768) return "mobile";
    if (w < 1024) return "tablet";
    return "desktop";
  }

  function context() {
    var p = params();
    return {
      sessionId: sessionId(),
      visitorId: visitorId(),
      embed: false,
      device: deviceClass(),
      landingVersion: "site",
      language: LANGUAGE,
      surface: SURFACE,
      path: (global.location.pathname || "") + (global.location.search || ""),
      referrer: (global.document && global.document.referrer) || "",
      utmSource: p.get("utm_source") || null,
      utmMedium: p.get("utm_medium") || null,
      utmCampaign: p.get("utm_campaign") || null,
    };
  }

  function send(event, extra) {
    var payload = Object.assign({}, context(), extra || {}, { event: event });
    var url = apiBase() + "/api/marketing/landing-events";
    try {
      global.dataLayer = global.dataLayer || [];
      global.dataLayer.push(
        Object.assign({ event: "dl_public", dl_event: event }, payload)
      );
    } catch (_e) {}
    try {
      var body = JSON.stringify(payload);
      if (navigator.sendBeacon) {
        navigator.sendBeacon(url, new Blob([body], { type: "application/json" }));
        return;
      }
    } catch (_e2) {}
    fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      credentials: "omit",
      keepalive: true,
    }).catch(function () {});
  }

  function track(event, extra) {
    send(event, extra || {});
  }

  function setupScroll() {
    function onScroll() {
      var doc = global.document.documentElement;
      var body = global.document.body;
      var scrollTop = global.pageYOffset || doc.scrollTop || 0;
      var height = Math.max(doc.scrollHeight, body ? body.scrollHeight : 0);
      var view = global.innerHeight || doc.clientHeight || 0;
      var maxScroll = Math.max(1, height - view);
      var pct = Math.min(100, Math.round((scrollTop / maxScroll) * 100));
      if (!state.firstScrollSent && pct > 2) {
        state.firstScrollSent = true;
        track("first_scroll", {
          seconds: Math.round((Date.now() - state.startedAt) / 1000),
        });
      }
      SCROLL_DEPTHS.forEach(function (d) {
        if (!state.scrollDepths[d] && pct >= d) {
          state.scrollDepths[d] = true;
          track("scroll_depth", { depth: d });
        }
      });
    }
    global.addEventListener("scroll", onScroll, { passive: true });
  }

  function setupEngagement() {
    ENGAGEMENT_SECONDS.forEach(function (sec) {
      global.setTimeout(function () {
        if (state.engagementSent[sec]) return;
        state.engagementSent[sec] = true;
        track("engagement_milestone", { seconds: sec });
      }, sec * 1000);
    });
  }

  function setupClicks() {
    global.document.addEventListener(
      "click",
      function (ev) {
        var t = ev.target;
        if (!t || !t.closest) return;
        var link = t.closest("a[href]");
        if (!link) return;
        var href = link.getAttribute("href") || "";
        if (!href || href === "#" || href.indexOf("javascript:") === 0) return;
        var label = String(link.textContent || href)
          .replace(/\s+/g, " ")
          .trim()
          .slice(0, 64);
        var loc = "unknown";
        if (link.closest("footer")) loc = "footer";
        else if (link.closest("nav, .w-nav, header, .navbar, #nav")) loc = "navbar";
        else if (link.closest(".mnav, [class*='mobile-nav']")) loc = "mobile_menu";
        track("cta_click", {
          label: label,
          location: loc,
          destination: link.href || href,
        });
      },
      true
    );
  }

  function init() {
    track("page_land", { section: SURFACE });
    setupScroll();
    setupEngagement();
    setupClicks();
  }

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(typeof window !== "undefined" ? window : globalThis);
