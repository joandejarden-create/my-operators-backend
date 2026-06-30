/**
 * Dealality Insights hub analytics (dealality.com/insights).
 * Posts to Railway /api/marketing/landing-events with surface=insights.
 *
 * Webflow: set window.DEALALITY_API_BASE to Railway, then load this script on /insights.
 */
(function (global) {
  "use strict";

  var SESSION_KEY = "dl_landing_sid_v1";
  var VISITOR_KEY = "dl_landing_vid_v1";
  var VISITOR_COOKIE = "dl_landing_vid_v1";
  var SURFACE = "insights";
  var SCROLL_DEPTHS = [25, 50, 75, 100];
  var ENGAGEMENT_SECONDS = [30, 60, 120];
  var DEFAULT_API_BASE = "https://my-operators-backend-staging.up.railway.app";

  var state = {
    startedAt: Date.now(),
    firstScrollSent: false,
    scrollDepths: {},
    engagementSent: {},
  };

  function apiBase() {
    var b = (global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || "").trim();
    return (b || DEFAULT_API_BASE).replace(/\/$/, "");
  }

  function onInsightsPage() {
    var path = (global.location.pathname || "").toLowerCase();
    return path === "/insights" || path.indexOf("/insights/") === 0;
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
      surface: SURFACE,
      path: (global.location.pathname || "") + (global.location.search || ""),
      referrer: (global.document && global.document.referrer) || "",
      utmSource: p.get("utm_source") || null,
      utmMedium: p.get("utm_medium") || null,
      utmCampaign: p.get("utm_campaign") || null,
    };
  }

  function slugify(text) {
    return String(text || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_|_$/g, "")
      .slice(0, 64);
  }

  function detectLanguage(el, label) {
    var langEl = el && el.closest("[lang]");
    if (langEl) {
      var lang = (langEl.getAttribute("lang") || "").trim().slice(0, 2);
      if (lang) return lang;
    }
    var text = String(label || "");
    if (/[áéíóúñ¿¡]/i.test(text)) return "es";
    var href = el && el.getAttribute ? el.getAttribute("href") || "" : "";
    if (/\/es(\/|$)/i.test(href)) return "es";
    var docLang = (global.document.documentElement.lang || "").trim().slice(0, 2);
    return docLang || "en";
  }

  function articleTitleFromLink(link) {
    if (!link) return "Article";
    var card =
      link.closest(
        ".blog-card, .collection-item, .insights-card, .w-dyn-item, article, [class*='insight'], [class*='blog']"
      ) || link;
    var heading = card.querySelector("h1, h2, h3, h4, .heading, [class*='title']");
    var text = heading ? heading.textContent : link.getAttribute("aria-label") || link.textContent;
    return String(text || link.pathname || "Article").replace(/\s+/g, " ").trim().slice(0, 96);
  }

  function isArticleLink(link) {
    if (!link || !link.getAttribute) return false;
    var href = link.getAttribute("href") || "";
    if (!href || href === "#" || href.indexOf("javascript:") === 0) return false;
    var path;
    try {
      path = new URL(href, global.location.href).pathname.toLowerCase();
    } catch (_e) {
      return false;
    }
    if (path === "/insights") return false;
    if (path.indexOf("/insights/") === 0 && path.length > "/insights/".length) return true;
    if (link.closest(".collection-list, .w-dyn-list, [class*='insights'], [class*='blog']")) {
      return path !== "/insights" && path.length > 1;
    }
    return false;
  }

  function isCtaLink(link) {
    if (!link) return false;
    var href = (link.getAttribute("href") || "").toLowerCase();
    var text = (link.textContent || "").toLowerCase();
    return (
      /signup|sign-up|early-access|request/.test(href) ||
      /request early access|get started|sign up/.test(text)
    );
  }

  function ctaLocation(link) {
    if (!link) return "insights_grid";
    if (link.closest("footer")) return "footer";
    if (link.closest(".w-nav, nav, header, .navbar")) return "navbar";
    if (link.closest(".mnav, [class*='mobile-nav']")) return "mobile_menu";
    return "insights_grid";
  }

  function send(event, extra) {
    var payload = Object.assign({}, context(), extra || {}, { event: event });
    var url = apiBase() + "/api/marketing/landing-events";

    try {
      if (global.dataLayer) {
        global.dataLayer.push(
          Object.assign({ event: "dl_insights", dl_event: event }, payload)
        );
      }
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

  function setupScrollDepth() {
    function onScroll() {
      if (!state.firstScrollSent) {
        state.firstScrollSent = true;
        track("first_scroll", {
          section: "insights",
          seconds: Math.round((Date.now() - state.startedAt) / 1000),
        });
      }
      var doc = global.document.documentElement;
      var scrollTop = global.pageYOffset || doc.scrollTop || 0;
      var height = Math.max(doc.scrollHeight - global.innerHeight, 1);
      var pct = Math.round((scrollTop / height) * 100);
      for (var i = 0; i < SCROLL_DEPTHS.length; i++) {
        var depth = SCROLL_DEPTHS[i];
        if (pct >= depth && !state.scrollDepths[depth]) {
          state.scrollDepths[depth] = true;
          track("scroll_depth", { section: "insights", depth: depth });
        }
      }
    }
    global.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  function setupEngagementMilestones() {
    ENGAGEMENT_SECONDS.forEach(function (seconds) {
      global.setTimeout(function () {
        if (state.engagementSent[seconds]) return;
        state.engagementSent[seconds] = true;
        track("engagement_milestone", { section: "insights", seconds: seconds });
      }, seconds * 1000);
    });
  }

  function setupClickTracking() {
    global.document.addEventListener(
      "click",
      function (ev) {
        var t = ev.target;
        if (!t || !t.closest) return;
        var link = t.closest("a[href]");
        if (!link) return;

        if (isArticleLink(link)) {
          var title = articleTitleFromLink(link);
          var dest = link.href || link.getAttribute("href") || "";
          track("insights_article_click", {
            section: "insights",
            label: title,
            destination: dest,
            language: detectLanguage(link, title),
            location: "insights_grid",
          });
          return;
        }

        if (isCtaLink(link)) {
          track("cta_click", {
            section: "insights",
            label: slugify(link.textContent),
            location: ctaLocation(link),
            destination: link.href || null,
          });
          return;
        }

        var nav = link.closest("nav, .w-nav, header, .navbar, .mnav");
        if (nav && !isArticleLink(link)) {
          track("nav_click", {
            section: "insights",
            label: slugify(link.textContent || link.getAttribute("href")),
            location: nav.closest(".mnav") ? "mobile_menu" : "navbar",
            destination: link.href || null,
          });
        }
      },
      true
    );
  }

  function init() {
    if (!onInsightsPage()) return;
    track("page_land", { section: "insights" });
    setupScrollDepth();
    setupEngagementMilestones();
    setupClickTracking();
  }

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(typeof window !== "undefined" ? window : globalThis);
