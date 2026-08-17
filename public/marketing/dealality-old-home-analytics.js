/**
 * Dealality old-home (new Webflow homepage) analytics collector.
 * Tags every event with landingVersion=old-home so the admin report can
 * separate previous iframe landing (v7–v9) from the new site without wiping history.
 *
 * Loads on / and /es. Posts to POST /api/marketing/landing-events on DEALALITY_API_BASE.
 */
(function (global) {
  "use strict";

  try {
    var path = (global.location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEsHome = path === "/es";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    // Only the homepage itself — not /es/insights, /es/who-its-for, etc.
    // Those are covered by dealality-public-analytics.js.
    var isHome =
      path === "/" ||
      path === "/old-home" ||
      path === "/home-legacy" ||
      isEsHome ||
      path === "/es/old-home" ||
      path === "/es/home-legacy";
    if (!isHome) return;
    if (global.__ohLandingAnalytics >= 2026080303) return;
    global.__ohLandingAnalytics = 2026080303;

    var SESSION_KEY = "dl_landing_sid_v1";
    var VISITOR_KEY = "dl_landing_vid_v1";
    var VISITOR_COOKIE = "dl_landing_vid_v1";
    var LANDING_VERSION = "old-home";
    var LANGUAGE = isEs ? "es" : "en";
    var API_BASE =
      (global.DEALALITY_API_BASE ||
        "https://my-operators-backend-staging.up.railway.app") +
      "/api/marketing/landing-events";

    var SECTION_IDS = [
      "hero",
      "oh-how-we-do-it",
      "modules",
      "pricing",
      "ecosystem",
      "testimonials",
      "faqs",
      "faq",
      "cta-band",
      "benefits",
      "platform",
    ];

    function sessionId() {
      try {
        var existing = global.sessionStorage.getItem(SESSION_KEY);
        if (existing) return existing;
        var id =
          "dl_" +
          Date.now().toString(36) +
          "_" +
          Math.random().toString(36).slice(2, 11);
        global.sessionStorage.setItem(SESSION_KEY, id);
        return id;
      } catch (_e) {
        return "dl_" + Date.now();
      }
    }

    function readCookie(name) {
      try {
        var match = ("; " + global.document.cookie).split("; " + name + "=");
        if (match.length === 2) return decodeURIComponent(match.pop().split(";").shift() || "");
      } catch (_e) {}
      return "";
    }

    function writeCookie(name, value) {
      try {
        global.document.cookie =
          name +
          "=" +
          encodeURIComponent(value) +
          "; path=/; max-age=31536000; SameSite=Lax";
      } catch (_e) {}
    }

    function visitorId() {
      try {
        var fromStore = global.localStorage.getItem(VISITOR_KEY);
        if (fromStore) return fromStore;
      } catch (_e) {}
      var fromCookie = readCookie(VISITOR_COOKIE);
      if (fromCookie) return fromCookie;
      var id =
        "dv_" +
        Date.now().toString(36) +
        "_" +
        Math.random().toString(36).slice(2, 11);
      try {
        global.localStorage.setItem(VISITOR_KEY, id);
      } catch (_e2) {}
      writeCookie(VISITOR_COOKIE, id);
      return id;
    }

    function deviceClass() {
      var w = global.innerWidth || 0;
      if (w < 768) return "mobile";
      if (w < 1024) return "tablet";
      return "desktop";
    }

    function params() {
      try {
        return new URLSearchParams(global.location.search);
      } catch (_e) {
        return new URLSearchParams();
      }
    }

    function context() {
      var p = params();
      return {
        sessionId: sessionId(),
        visitorId: visitorId(),
        embed: false,
        device: deviceClass(),
        landingVersion: LANDING_VERSION,
        language: LANGUAGE,
        surface: "landing",
        path: (global.location.pathname || "") + (global.location.search || ""),
        referrer: (global.document && global.document.referrer) || "",
        utmSource: p.get("utm_source") || null,
        utmMedium: p.get("utm_medium") || null,
        utmCampaign: p.get("utm_campaign") || null,
      };
    }

    function send(event, extra) {
      var payload = Object.assign({}, context(), extra || {}, { event: event });
      try {
        if (typeof global.clarity === "function") {
          global.clarity("event", event);
          if (payload.section) global.clarity("set", "dl_section", payload.section);
        }
      } catch (_e) {}
      try {
        global.dataLayer = global.dataLayer || [];
        global.dataLayer.push(
          Object.assign({ event: "dl_landing", dl_event: event }, payload)
        );
      } catch (_e2) {}
      try {
        var body = JSON.stringify(payload);
        if (navigator.sendBeacon) {
          navigator.sendBeacon(API_BASE, body);
          return;
        }
        fetch(API_BASE, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: body,
          keepalive: true,
          credentials: "omit",
        }).catch(function () {});
      } catch (_e3) {}
    }

    function track(event, extra) {
      send(event, extra);
    }

    // —— page land ——
    track("page_land", { section: "hero" });

    // —— scroll ——
    var scrolled = false;
    var depths = { 25: false, 50: false, 75: false, 100: false };
    var landAt = Date.now();
    function onScroll() {
      var doc = global.document.documentElement;
      var body = global.document.body;
      var scrollTop = global.pageYOffset || doc.scrollTop || 0;
      var height = Math.max(doc.scrollHeight, body ? body.scrollHeight : 0);
      var view = global.innerHeight || doc.clientHeight || 0;
      var maxScroll = Math.max(1, height - view);
      var pct = Math.min(100, Math.round((scrollTop / maxScroll) * 100));
      if (!scrolled && pct > 2) {
        scrolled = true;
        track("first_scroll", {
          seconds: Math.round((Date.now() - landAt) / 1000),
        });
      }
      [25, 50, 75, 100].forEach(function (d) {
        if (!depths[d] && pct >= d) {
          depths[d] = true;
          track("scroll_depth", { depth: d });
        }
      });
    }
    global.addEventListener("scroll", onScroll, { passive: true });

    // —— section views ——
    var seenSections = {};
    function observeSections() {
      if (!global.IntersectionObserver) return;
      var io = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (!entry.isIntersecting) return;
            var id = entry.target.id;
            if (!id || seenSections[id]) return;
            seenSections[id] = true;
            track("section_view", { section: id });
          });
        },
        { threshold: 0.35 }
      );
      SECTION_IDS.forEach(function (id) {
        var el = global.document.getElementById(id);
        if (el) io.observe(el);
      });
    }

    // —— CTAs ——
    function locationFor(el) {
      if (!el || !el.closest) return "unknown";
      if (el.id === "fsw-btn" || el.closest("#hero")) return "hero";
      if (el.id === "fsw-secondary" || el.id === "oh-pvl-open") return "hero_video";
      if (el.id === "pricing-owners-cta" || el.closest("#pricing")) return "pricing_owners";
      if (el.id === "nav-cta") return "nav_cta";
      if (el.id === "nav-signin") return "nav_signin";
      if (el.closest("#oh-how-we-do-it")) return "how_we_do_it";
      if (el.closest("footer")) return "footer";
      if (el.closest("#mnav") || el.closest(".mnav")) return "mobile_menu";
      if (el.closest("#nav")) return "navbar";
      if (el.closest("#cta-band")) return "cta_section";
      return "unknown";
    }

    function bindCtas() {
      var selectors = [
        "#fsw-btn",
        "#fsw-secondary",
        "#pricing-owners-cta",
        "#cta-band-btn",
        "#nav-cta",
        "#nav-signin",
        "#oh-pvl-open",
        '[data-dealality-process-cta="explore"]',
        '[data-dealality-process-cta="demo"]',
        '[data-dealality-process-cta="video"]',
        'a[href*="opportunity-review"]',
        'a[href*="signup"]',
        'a[href*="login"]',
        "#oh-demo-submit",
      ];
      selectors.forEach(function (sel) {
        try {
          global.document.querySelectorAll(sel).forEach(function (el) {
            if (el.getAttribute("data-oh-analytics-bound") === "1") return;
            el.setAttribute("data-oh-analytics-bound", "1");
            el.addEventListener(
              "click",
              function () {
                var loc = locationFor(el);
                var label = (el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 96);
                track("cta_click", {
                  location: loc,
                  label: label || el.id || sel,
                  element: el.id || null,
                  section: (el.closest("section[id]") && el.closest("section[id]").id) || null,
                });
              },
              true
            );
          });
        } catch (_e) {}
      });
    }

    // —— FAQ ——
    function bindFaqs() {
      try {
        global.document
          .querySelectorAll(
            '#faqs details, #faq details, [data-oh-faq], .oh-faq-item, button[aria-controls*="faq"]'
          )
          .forEach(function (el) {
            if (el.getAttribute("data-oh-faq-bound") === "1") return;
            el.setAttribute("data-oh-faq-bound", "1");
            el.addEventListener("click", function () {
              var q =
                (el.getAttribute("data-question-id") ||
                  el.id ||
                  (el.textContent || "").replace(/\s+/g, " ").trim()).slice(0, 64);
              track("faq_open", { questionId: q, section: "faqs" });
            });
          });
      } catch (_e) {}
    }

    // —— engagement milestones ——
    [30, 60, 120].forEach(function (sec) {
      global.setTimeout(function () {
        track("engagement_milestone", { seconds: sec });
      }, sec * 1000);
    });

    function boot() {
      observeSections();
      bindCtas();
      bindFaqs();
    }

    if (global.document.readyState === "loading") {
      global.document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    [800, 2500, 6000].forEach(function (ms) {
      global.setTimeout(function () {
        bindCtas();
        bindFaqs();
      }, ms);
    });
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-landing-analytics]", err);
    }
  }
})(typeof window !== "undefined" ? window : this);
