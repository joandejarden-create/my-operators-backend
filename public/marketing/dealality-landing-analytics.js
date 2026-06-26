/**
 * Dealality landing page analytics — top 20 resonance metrics.
 * Sends to Railway log, Microsoft Clarity (if present), and GTM dataLayer (if present).
 */
(function (global) {
  "use strict";

  var SESSION_KEY = "dl_landing_sid_v1";
  var VISITOR_KEY = "dl_landing_vid_v1";
  var VISITOR_COOKIE = "dl_landing_vid_v1";
  var SCROLL_DEPTHS = [25, 50, 75, 100];
  var ENGAGEMENT_SECONDS = [30, 60, 120];
  var VIDEO_PCTS = [25, 50, 75, 100];
  var SECTION_IDS = [
    "hero",
    "proofbar",
    "problem",
    "how",
    "audiences",
    "why",
    "faq",
    "cta",
  ];
  var STAGE_LABELS = [
    "understand",
    "prepare",
    "match",
    "shortlist",
    "compare",
    "loi",
  ];
  var SECTION_RANK = {
    hero: 1,
    proofbar: 2,
    problem: 3,
    how: 4,
    audiences: 5,
    why: 6,
    faq: 7,
    cta: 8,
  };

  var state = {
    startedAt: Date.now(),
    firstScrollSent: false,
    scrollDepths: {},
    sectionsSeen: {},
    engagementSent: {},
    videoPcts: {},
    stagesVisited: {},
    maxSectionRank: 0,
    maxSectionId: "hero",
    stageTourComplete: false,
    mobileNavOpen: false,
  };

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
      var embed = params().get("embed") === "1";
      var sameSite = embed ? "; SameSite=None" : "; SameSite=Lax";
      var partitioned = embed && secure ? "; Partitioned" : "";
      global.document.cookie =
        VISITOR_COOKIE +
        "=" +
        encodeURIComponent(id) +
        "; path=/; max-age=" +
        60 * 60 * 24 * 400 +
        sameSite +
        secure +
        partitioned;
    } catch (_e2) {}
  }

  function createVisitorId() {
    return (
      "dlv_" +
      Date.now().toString(36) +
      "_" +
      Math.random().toString(36).slice(2, 11)
    );
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

    var id = createVisitorId();
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

  function landingVersion() {
    var path = (global.location.pathname || "").toLowerCase();
    if (path.indexOf("v9") >= 0) return "v9";
    if (path.indexOf("v7") >= 0) return "v7";
    if (path.indexOf("v8") >= 0) return "v8";
    return "v9";
  }

  function context() {
    var p = params();
    return {
      sessionId: sessionId(),
      visitorId: visitorId(),
      embed: p.get("embed") === "1",
      device: deviceClass(),
      landingVersion: landingVersion(),
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

  function sectionFromElement(el) {
    if (!el || !el.closest) return null;
    var section = el.closest("section[id], #proofbar");
    if (!section) return null;
    return section.id || null;
  }

  function personaFromCta(el, label) {
    var text = (label || (el && el.textContent) || "").toLowerCase();
    var inOwners = el && el.closest && el.closest("#owners, #aud-panel-0");
    var inBrands = el && el.closest && el.closest("#brands, #aud-panel-1");
    var inPartners = el && el.closest && el.closest("#partners, #aud-panel-2");
    if (inOwners || /submit a project|get started|owner/.test(text)) return "owner";
    if (inPartners || /create your profile|advisor|partner/.test(text)) return "partner";
    if (inBrands || /join as a brand|brand or operator/.test(text)) return "brand";
    return null;
  }

  function locationFromCta(el) {
    if (!el) return "unknown";
    if (el.closest && el.closest("#hero")) return "hero";
    if (el.closest && el.closest("#cta")) return "cta_section";
    if (el.closest && el.closest("#owners")) return "audience_owners";
    if (el.closest && el.closest("#brands")) return "audience_brands";
    if (el.closest && el.closest("#partners")) return "audience_partners";
    if (el.closest && el.closest("footer")) return "footer";
    if (el.closest && el.closest(".mnav")) return "mobile_menu";
    if (el.closest && el.closest("#nav")) return "navbar";
    return sectionFromElement(el) || "unknown";
  }

  function mirrorExternal(event, payload) {
    try {
      if (typeof global.clarity === "function") {
        global.clarity("event", event);
        if (payload.section) global.clarity("set", "dl_section", payload.section);
        if (payload.persona) global.clarity("set", "dl_persona", payload.persona);
      }
    } catch (_e) {}

    try {
      global.dataLayer = global.dataLayer || [];
      global.dataLayer.push(
        Object.assign({ event: "dl_landing", dl_event: event }, payload)
      );
    } catch (_e2) {}
  }

  function send(event, extra) {
    var payload = Object.assign({}, context(), extra || {}, { event: event });
    mirrorExternal(event, payload);

    try {
      var body = JSON.stringify(payload);
      if (navigator.sendBeacon) {
        var blob = new Blob([body], { type: "application/json" });
        navigator.sendBeacon("/api/marketing/landing-events", blob);
        return;
      }
    } catch (_e) {}

    fetch("/api/marketing/landing-events", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      credentials: "omit",
      keepalive: true,
    }).catch(function () {});
  }

  function updateMaxSection(sectionId) {
    var rank = SECTION_RANK[sectionId] || 0;
    if (rank > state.maxSectionRank) {
      state.maxSectionRank = rank;
      state.maxSectionId = sectionId;
      send("max_section_depth", { section: sectionId, depth: rank });
    }
  }

  function track(event, extra) {
    send(event, extra);
  }

  function trackStageTab(idx, source) {
    var label = STAGE_LABELS[idx] || "stage_" + idx;
    state.stagesVisited[label] = true;
    track("stage_tab", { section: "how", element: label, label: label, source: source || "manual" });
    var visited = Object.keys(state.stagesVisited).length;
    if (!state.stageTourComplete && visited >= STAGE_LABELS.length) {
      state.stageTourComplete = true;
      track("stage_tour_complete", { section: "how", element: "all_stages" });
    }
  }

  function trackAudienceTab(idx) {
    var slugs = ["owners", "brands", "partners"];
    var slug = slugs[idx] || "audience_" + idx;
    track("audience_tab", { section: "audiences", element: slug, persona: slug });
  }

  function trackFaqOpen(button) {
    var text =
      button &&
      button.querySelector &&
      button.querySelector(".faq-q-t") &&
      button.querySelector(".faq-q-t").textContent;
    var qid = slugify(text);
    track("faq_open", { section: "faq", questionId: qid, label: text });
  }

  function trackVideoProgress(video) {
    if (!video || !video.duration || !isFinite(video.duration)) return;
    var pct = Math.floor((video.currentTime / video.duration) * 100);
    VIDEO_PCTS.forEach(function (mark) {
      if (pct >= mark && !state.videoPcts[mark]) {
        state.videoPcts[mark] = true;
        track("video_progress", { section: "hero", depth: mark });
        if (mark === 100) track("video_complete", { section: "hero" });
      }
    });
  }

  function setupScrollDepth() {
    function onScroll() {
      if (!state.firstScrollSent) {
        state.firstScrollSent = true;
        track("first_scroll", {
          seconds: Math.round((Date.now() - state.startedAt) / 1000),
        });
      }

      var doc = global.document.documentElement;
      var scrollTop = global.pageYOffset || doc.scrollTop || 0;
      var height = Math.max(doc.scrollHeight - global.innerHeight, 1);
      var pct = Math.min(100, Math.round((scrollTop / height) * 100));

      SCROLL_DEPTHS.forEach(function (mark) {
        if (pct >= mark && !state.scrollDepths[mark]) {
          state.scrollDepths[mark] = true;
          track("scroll_depth", { depth: mark });
        }
      });
    }

    global.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  function setupSectionViews() {
    if (!("IntersectionObserver" in global)) return;
    var targets = SECTION_IDS.map(function (id) {
      return global.document.getElementById(id);
    }).filter(Boolean);

    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var id = entry.target.id;
          if (state.sectionsSeen[id]) return;
          state.sectionsSeen[id] = true;
          track("section_view", { section: id });
          updateMaxSection(id);
        });
      },
      { threshold: 0.35 }
    );

    targets.forEach(function (el) {
      io.observe(el);
    });
  }

  function setupEngagementMilestones() {
    ENGAGEMENT_SECONDS.forEach(function (sec) {
      global.setTimeout(function () {
        if (state.engagementSent[sec]) return;
        state.engagementSent[sec] = true;
        track("engagement_milestone", { seconds: sec });
      }, sec * 1000);
    });
  }

  function setupClickTracking() {
    global.document.addEventListener(
      "click",
      function (ev) {
        var t = ev.target;
        if (!t || !t.closest) return;

        var cta = t.closest(
          "a.bp2, a.bfull, a.bout, a.nbn, a.bp2, button.bp2, a.bs2, button.bs2, .ec-link, a.nbg"
        );
        if (cta) {
          var label = slugify(cta.textContent);
          track("cta_click", {
            section: sectionFromElement(cta) || "unknown",
            location: locationFromCta(cta),
            label: label,
            persona: personaFromCta(cta, label),
            element: cta.id || cta.className.split(" ")[0] || "cta",
          });
        }

        var navLink = t.closest("#nav .nl a, .mnav-link");
        if (navLink) {
          track("nav_click", {
            section: "nav",
            label: slugify(navLink.textContent),
            element: (navLink.getAttribute("href") || "").replace("#", ""),
            location: navLink.closest(".mnav") ? "mobile_nav" : "desktop_nav",
          });
        }

        var outbound = t.closest("footer a[href], a[target='_top'][href^='http']");
        if (
          outbound &&
          outbound.href &&
          outbound.href.indexOf(global.location.host) === -1
        ) {
          track("outbound_click", {
            section: sectionFromElement(outbound) || "footer",
            destination: outbound.href,
            label: slugify(outbound.textContent),
          });
        }

        var faqBtn = t.closest(".faq-q");
        if (faqBtn && !faqBtn.parentElement.classList.contains("open")) {
          trackFaqOpen(faqBtn);
        }
      },
      true
    );
  }

  function setupMobileNav() {
    var nmenu = global.document.getElementById("nmenu");
    if (!nmenu) return;
    nmenu.addEventListener("click", function () {
      var opening = !global.document.getElementById("mnav").classList.contains("open");
      if (opening && !state.mobileNavOpen) {
        state.mobileNavOpen = true;
        track("mobile_nav_open", { section: "nav" });
      }
    });
  }

  function setupVideoHooks() {
    var video = global.document.getElementById("hero-overview-video");
    if (!video) return;
    video.addEventListener("timeupdate", function () {
      trackVideoProgress(video);
    });
  }

  function init() {
    track("page_land", { section: "hero" });
    setupScrollDepth();
    setupSectionViews();
    setupEngagementMilestones();
    setupClickTracking();
    setupMobileNav();
    setupVideoHooks();
  }

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  global.DealalityLandingAnalytics = {
    track: track,
    trackStageTab: trackStageTab,
    trackAudienceTab: trackAudienceTab,
    trackFaqOpen: trackFaqOpen,
    trackVideoOpen: function () {
      track("hero_video_open", { section: "hero" });
    },
    trackVideoClose: function (watchedSeconds) {
      track("hero_video_close", {
        section: "hero",
        seconds: watchedSeconds || 0,
      });
    },
    trackEmailCapture: function (outcome) {
      track("email_capture_submit", { section: "hero", outcome: outcome });
    },
    trackStageTourPause: function (resumed) {
      track("stage_tour_pause", {
        section: "how",
        source: resumed ? "resume" : "pause",
      });
    },
    trackStageAutoAdvance: function (idx) {
      trackStageTab(idx, "auto");
    },
  };
})(typeof window !== "undefined" ? window : globalThis);
