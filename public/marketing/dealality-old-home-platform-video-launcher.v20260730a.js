/**
 * Old Home — floating collapsible platform overview video launcher (v20260730a).
 * Self-injects CSS (styles were missing from published hero-fit). Lazy-loads <video> after open.
 * Session dismiss only (sessionStorage).
 */
(function () {
  "use strict";

  if (window.__ohPvlBuild && window.__ohPvlBuild >= 202607301) return;
  window.__ohPvlBuild = 202607301;

  var VIDEO_SRC =
    "https://res.cloudinary.com/dos2eqnzd/video/upload/q_auto,f_mp4/v1775832428/Video_Deck_Dealality_-_Short_Elevator_Pitch_Deck_Video_irnayw.mp4";
  var POSTER_SRC =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a693194497ac42db66ed6c6_platform-overview-poster-320.jpg";
  var POSTER_FALLBACK =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer-p-800.png";
  // Expanded player cover before play — solid black (not the platform UI frame).
  var VIDEO_POSTER_BLACK =
    "data:image/svg+xml," +
    encodeURIComponent(
      '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900"><rect width="100%" height="100%" fill="#000"/></svg>'
    );
  // Captions support playback accessibility without duplicating the panel marketing copy.
  var CAPTIONS_VTT =
    "WEBVTT\n\n" +
    "00:00:00.000 --> 00:00:18.000\n" +
    "Hotel opportunity decisions often span research, relationships, proposals, and negotiation.\n\n" +
    "00:00:18.000 --> 00:00:40.000\n" +
    "Dealality starts with opportunity assessment and credible strategic paths.\n\n" +
    "00:00:40.000 --> 00:01:05.000\n" +
    "Engage brands, operators, and advisors through structured outreach.\n\n" +
    "00:01:05.000 --> 00:01:19.000\n" +
    "Compare proposals and commercial trade-offs in one place.\n";
  var API_BASE =
    (window.DEALALITY_API_BASE ||
      "https://my-operators-backend-production.up.railway.app") +
    "/api/marketing/landing-events";
  var SESSION_KEY = "dl_landing_sid_v1";
  var DISMISS_KEY = "dl_platform_video_launcher_dismissed_v3";
  var MOBILE_MQ = "(max-width: 767px)";
  var SHOW_DELAY_MS = 7500;
  var SCROLL_TRIGGER = 0.15;
  var DISPLAY_DURATION = "1:19";

  if (isDismissed()) return;

  var reduceMotion =
    window.matchMedia &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var root = null;
  var launcherBtn = null;
  var panel = null;
  var modal = null;
  var video = null;
  var videoMounted = false;
  var shown = false;
  var expanded = false;
  var impressionSent = false;
  var milestones = {
    start: false,
    p25: false,
    p50: false,
    p75: false,
    complete: false,
  };
  var showTimer = null;
  var lastFocus = null;
  var captionsUrl = null;

  function deviceCategory() {
    var w = window.innerWidth || 0;
    if (w < 768) return "mobile";
    if (w < 1024) return "tablet";
    return "desktop";
  }

  function isMobile() {
    return window.matchMedia && window.matchMedia(MOBILE_MQ).matches;
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

  function isDismissed() {
    try {
      return sessionStorage.getItem(DISMISS_KEY) === "1";
    } catch (_e) {
      return false;
    }
  }

  function setDismissed() {
    try {
      sessionStorage.setItem(DISMISS_KEY, "1");
    } catch (_e) {}
  }

  function track(event, extra) {
    var payload = {
      event: event,
      sessionId: sessionId(),
      device: deviceCategory(),
      location: isMobile() ? "floating_launcher_mobile" : "floating_launcher",
      section: "platform_video_launcher",
      path: (window.location.pathname || "") + (window.location.search || ""),
      landingVersion: "old-home",
    };
    if (extra && typeof extra === "object") {
      Object.keys(extra).forEach(function (k) {
        payload[k] = extra[k];
      });
    }
    try {
      if (navigator.sendBeacon) {
        navigator.sendBeacon(
          API_BASE,
          new Blob([JSON.stringify(payload)], { type: "application/json" })
        );
        return;
      }
    } catch (_e) {}
    try {
      fetch(API_BASE, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        keepalive: true,
      }).catch(function () {});
    } catch (_e2) {}
  }

  function posterUrl() {
    if (POSTER_SRC.indexOf("PLACEHOLDER_") === -1) return POSTER_SRC;
    return POSTER_FALLBACK;
  }

  function a11yCandidates() {
    return Array.prototype.slice.call(
      document.querySelectorAll(
        [
          "#userwayAccessibilityIcon",
          ".userway_buttons_wrapper",
          ".uwy",
          "#accessibe_container",
          ".accessibe-widget",
          ".acsb-trigger",
          "#acsb-trigger",
          ".asw-container",
          ".asw-menu",
          "[class*='accessibe']",
          "[id*='accessibe']",
          "[class*='userway']",
          "[id*='userway']",
          "[aria-label*='Accessibility' i]",
          "[aria-label*='accessibility' i]",
          "[title*='Accessibility' i]",
          "button[aria-label*='Accessible' i]",
        ].join(",")
      )
    );
  }

  function positionAboveA11y() {
    if (!root) return;
    var gap = 20;
    var fallbackBottom = 24;
    var fallbackRight = 20;
    var placeLeft = false;
    var bottom = fallbackBottom;
    var right = fallbackRight;
    var left = fallbackRight;

    try {
      var nodes = a11yCandidates().filter(function (el) {
        if (!el || !el.getBoundingClientRect) return false;
        var r = el.getBoundingClientRect();
        var style = window.getComputedStyle(el);
        if (style.display === "none" || style.visibility === "hidden") return false;
        if (r.width < 8 || r.height < 8) return false;
        return r.bottom > window.innerHeight * 0.55 && r.right > window.innerWidth * 0.55;
      });

      if (nodes.length) {
        var topMost = Infinity;
        var rightMost = 0;
        nodes.forEach(function (el) {
          var r = el.getBoundingClientRect();
          topMost = Math.min(topMost, r.top);
          rightMost = Math.max(rightMost, window.innerWidth - r.left);
        });
        var fromBottom = Math.max(fallbackBottom, window.innerHeight - topMost + gap);
        if (fromBottom > window.innerHeight * 0.42) {
          placeLeft = true;
          bottom = fallbackBottom;
        } else {
          bottom = fromBottom;
          right = Math.max(fallbackRight, Math.min(28, rightMost - 8));
        }
      }
    } catch (_e) {}

    root.classList.toggle("is-left", placeLeft);
    root.style.setProperty("--oh-pvl-bottom", bottom + "px");
    root.style.setProperty("--oh-pvl-right", right + "px");
    root.style.setProperty("--oh-pvl-left", left + "px");
  }


  function injectCss() {
    var prev = document.getElementById("oh-pvl-css");
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    var css = document.createElement("style");
    css.id = "oh-pvl-css";
    css.textContent = '#oh-pvl{--oh-pvl-bottom:24px;--oh-pvl-right:20px;--oh-pvl-left:20px;position:fixed;z-index:10040;bottom:var(--oh-pvl-bottom);right:var(--oh-pvl-right);left:auto;width:min(372px,calc(100vw - 28px));font-family:inherit;color:#e8eef8;pointer-events:none;box-sizing:border-box}#oh-pvl *,#oh-pvl *::before,#oh-pvl *::after{box-sizing:border-box}#oh-pvl.is-left{right:auto;left:var(--oh-pvl-left)}#oh-pvl[hidden]{display:none!important}#oh-pvl .oh-pvl-open,#oh-pvl .oh-pvl-icon-btn,#oh-pvl .oh-pvl-panel,#oh-pvl .oh-pvl-modal{pointer-events:auto}#oh-pvl .oh-pvl-card{position:relative;display:flex;align-items:stretch;gap:0;padding:10px 10px 10px 10px;border-radius:16px;background:linear-gradient(165deg,rgba(14,22,40,.96) 0%,rgba(10,16,30,.98) 100%);border:1px solid rgba(255,255,255,.14);box-shadow:0 16px 40px rgba(0,0,0,.42),0 0 0 1px rgba(255,255,255,.04);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}#oh-pvl.is-visible .oh-pvl-card{animation:oh-pvl-in .38s cubic-bezier(.22,1,.36,1) both}#oh-pvl.is-anim .oh-pvl-panel{animation:oh-pvl-in .38s cubic-bezier(.22,1,.36,1) both}@media (prefers-reduced-motion:reduce){#oh-pvl.is-visible .oh-pvl-card,#oh-pvl.is-anim .oh-pvl-panel{animation:none}}@keyframes oh-pvl-in{from{opacity:0;transform:translateY(12px) scale(.98)}to{opacity:1;transform:none}}#oh-pvl .oh-pvl-open{appearance:none;border:0;background:transparent;color:inherit;display:flex;align-items:center;gap:12px;width:100%;padding:0 36px 0 0;margin:0;text-align:left;cursor:pointer;font:inherit}#oh-pvl .oh-pvl-thumb-wrap{position:relative;flex:0 0 auto;width:88px;height:58px;border-radius:10px;overflow:hidden;background:#0a0f1c;box-shadow:inset 0 0 0 1px rgba(255,255,255,.1)}#oh-pvl .oh-pvl-thumb{display:block;width:100%;height:100%;object-fit:cover}#oh-pvl .oh-pvl-play{position:absolute;inset:0;margin:auto;width:28px;height:28px;border-radius:999px;background:rgba(8,12,24,.72);border:1px solid rgba(255,255,255,.28);box-shadow:0 4px 14px rgba(0,0,0,.35)}#oh-pvl .oh-pvl-play::before{content:"";position:absolute;top:50%;left:52%;transform:translate(-40%,-50%);width:0;height:0;border-style:solid;border-width:6px 0 6px 10px;border-color:transparent transparent transparent #fff}#oh-pvl .oh-pvl-copy{display:flex;flex-direction:column;gap:2px;min-width:0;flex:1 1 auto}#oh-pvl .oh-pvl-title{font-size:14px;font-weight:700;letter-spacing:-.01em;color:#fff;line-height:1.25}#oh-pvl .oh-pvl-sub{font-size:12px;line-height:1.35;color:rgba(232,238,248,.72)}#oh-pvl .oh-pvl-meta{margin-top:2px;font-size:11px;font-weight:600;color:#8B90FF}#oh-pvl .oh-pvl-icon-btn,.oh-pvl-icon-btn{appearance:none;border:0;margin:0;padding:0;width:28px;height:28px;border-radius:8px;background:rgba(255,255,255,.06);color:rgba(232,238,248,.85);font-size:18px;line-height:1;cursor:pointer;display:inline-flex;align-items:center;justify-content:center}#oh-pvl #oh-pvl-dismiss{position:absolute;top:8px;right:8px}.oh-pvl-icon-btn:hover{background:rgba(255,255,255,.12)!important}.oh-pvl-icon-btn:focus-visible{outline:2px solid #8B90FF!important;outline-offset:2px}.oh-pvl-panel-actions .oh-pvl-icon-btn{margin:0!important}#oh-pvl .oh-pvl-panel{display:flex;flex-direction:column;gap:10px;padding:12px;border-radius:16px;background:linear-gradient(165deg,rgba(14,22,40,.98) 0%,rgba(10,16,30,.99) 100%);border:1px solid rgba(255,255,255,.14);box-shadow:0 20px 48px rgba(0,0,0,.5);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px)}#oh-pvl .oh-pvl-panel[hidden],#oh-pvl #oh-pvl-collapsed[hidden],#oh-pvl .oh-pvl-modal[hidden]{display:none!important}#oh-pvl .oh-pvl-panel-bar{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}#oh-pvl .oh-pvl-panel-heading{min-width:0;flex:1 1 auto}#oh-pvl .oh-pvl-panel-heading p,#oh-pvl .oh-pvl-panel-desc{margin:0}#oh-pvl #oh-pvl-panel-title,#oh-pvl #oh-pvl-modal-title{font-size:15px;font-weight:700;color:#fff;line-height:1.25;margin:0 0 4px}#oh-pvl #oh-pvl-panel-desc,#oh-pvl .oh-pvl-panel-desc{font-size:12px;line-height:1.4;color:rgba(232,238,248,.72)}#oh-pvl .oh-pvl-panel-actions{display:flex;gap:6px;flex:0 0 auto}#oh-pvl .oh-pvl-frame{position:relative;width:100%;aspect-ratio:16/9;border-radius:12px;overflow:hidden;background:#000;box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)}#oh-pvl .oh-pvl-frame video,#oh-pvl #oh-pvl-player{display:block;width:100%;height:100%;object-fit:contain;background:#000;border:0}#oh-pvl .oh-pvl-modal{position:fixed;inset:0;z-index:10045;display:none;align-items:center;justify-content:center;padding:18px;background:rgba(8,12,24,.72);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}#oh-pvl .oh-pvl-modal.is-open{display:flex}#oh-pvl #oh-pvl-modal-dialog{width:min(560px,100%);display:flex;flex-direction:column;gap:12px;padding:14px;border-radius:16px;background:linear-gradient(165deg,#0c1424 0%,#111b2e 55%,#0a101c 100%);border:1px solid rgba(255,255,255,.14);box-shadow:0 24px 64px rgba(0,0,0,.5);color:#e8eef8}#oh-pvl[data-state="modal"]{inset:0;width:auto;bottom:0;right:0;left:0;top:0;pointer-events:none}#oh-pvl[data-state="modal"] .oh-pvl-modal{pointer-events:auto}@media (max-width:767px){#oh-pvl{width:min(100vw - 20px,360px)}}';
    document.head.appendChild(css);
  }

  function ensureDom() {
    injectCss();
    if (root) return root;
    var stale = document.getElementById("oh-pvl");
    if (stale && stale.parentNode) {
      try {
        stale.parentNode.removeChild(stale);
      } catch (_e) {}
    }
    root = document.createElement("div");
    root.id = "oh-pvl";
    root.setAttribute("data-oh-pvl-ver", "30a");
    root.setAttribute("data-state", "hidden");
    root.innerHTML =
      '<div id="oh-pvl-collapsed" class="oh-pvl-card">' +
      '<button type="button" id="oh-pvl-open" class="oh-pvl-open" aria-expanded="false" aria-controls="oh-pvl-panel" aria-label="Open Why Dealality video">' +
      '<span class="oh-pvl-thumb-wrap" aria-hidden="true">' +
      '<img id="oh-pvl-thumb" class="oh-pvl-thumb" alt="" width="88" height="58" decoding="async" loading="lazy">' +
      '<span class="oh-pvl-play" aria-hidden="true"></span>' +
      "</span>" +
      '<span class="oh-pvl-copy">' +
      '<span class="oh-pvl-title">Why Dealality</span>' +
      '<span class="oh-pvl-sub">See the problem it solves—and how the platform works.</span>' +
      '<span class="oh-pvl-meta"><span class="oh-pvl-duration">' +
      DISPLAY_DURATION +
      "</span></span>" +
      "</span>" +
      "</button>" +
      '<button type="button" id="oh-pvl-dismiss" class="oh-pvl-icon-btn" aria-label="Dismiss Why Dealality launcher">×</button>' +
      "</div>" +
      '<div id="oh-pvl-panel" class="oh-pvl-panel" role="dialog" aria-modal="false" aria-labelledby="oh-pvl-panel-title" hidden>' +
      '<div class="oh-pvl-panel-bar">' +
      '<div class="oh-pvl-panel-heading">' +
      '<p id="oh-pvl-panel-title">Why Dealality</p>' +
      '<p id="oh-pvl-panel-desc">Hotel opportunity decisions are often fragmented across research, relationships, proposals, and negotiation. See how Dealality brings that work into one structured process.</p>' +
      "</div>" +
      '<div class="oh-pvl-panel-actions">' +
      '<button type="button" id="oh-pvl-minimize" class="oh-pvl-icon-btn" aria-label="Minimize Why Dealality video">–</button>' +
      '<button type="button" id="oh-pvl-close" class="oh-pvl-icon-btn" aria-label="Close Why Dealality video">×</button>' +
      "</div>" +
      "</div>" +
      '<div id="oh-pvl-frame" class="oh-pvl-frame"></div>' +
      "</div>" +
      '<div id="oh-pvl-modal" class="oh-pvl-modal" hidden aria-hidden="true">' +
      '<div id="oh-pvl-modal-dialog" role="dialog" aria-modal="true" aria-labelledby="oh-pvl-modal-title">' +
      '<div class="oh-pvl-panel-bar">' +
      '<div class="oh-pvl-panel-heading">' +
      '<p id="oh-pvl-modal-title">Why Dealality</p>' +
      '<p class="oh-pvl-panel-desc">Hotel opportunity decisions are often fragmented across research, relationships, proposals, and negotiation. See how Dealality brings that work into one structured process.</p>' +
      "</div>" +
      '<button type="button" id="oh-pvl-modal-close" class="oh-pvl-icon-btn" aria-label="Close Why Dealality video">×</button>' +
      "</div>" +
      '<div id="oh-pvl-modal-frame" class="oh-pvl-frame"></div>' +
      "</div>" +
      "</div>";

    document.body.appendChild(root);
    launcherBtn = document.getElementById("oh-pvl-open");
    panel = document.getElementById("oh-pvl-panel");
    modal = document.getElementById("oh-pvl-modal");

    var thumb = document.getElementById("oh-pvl-thumb");
    if (thumb) {
      thumb.src = posterUrl();
      thumb.onerror = function () {
        thumb.onerror = null;
        thumb.src = POSTER_FALLBACK;
      };
    }

    document.getElementById("oh-pvl-dismiss").addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      dismissLauncher();
    });
    launcherBtn.addEventListener("click", function (e) {
      e.preventDefault();
      openLauncher("launcher");
    });
    document.getElementById("oh-pvl-minimize").addEventListener("click", function (e) {
      e.preventDefault();
      minimizeLauncher();
    });
    document.getElementById("oh-pvl-close").addEventListener("click", function (e) {
      e.preventDefault();
      closeExpanded("close");
    });
    document.getElementById("oh-pvl-modal-close").addEventListener("click", function (e) {
      e.preventDefault();
      closeExpanded("close");
    });
    modal.addEventListener("click", function (e) {
      if (e.target === modal) closeExpanded("close");
    });

    document.addEventListener(
      "keydown",
      function (e) {
        if (e.key !== "Escape") return;
        if (!expanded) return;
        e.preventDefault();
        closeExpanded("close");
      },
      true
    );

    window.addEventListener(
      "resize",
      function () {
        positionAboveA11y();
        if (expanded && isMobile() && panel && !panel.hasAttribute("hidden")) {
          minimizeLauncher(true);
          openMobileModal(true);
        } else if (
          expanded &&
          !isMobile() &&
          modal &&
          modal.classList.contains("is-open")
        ) {
          minimizeLauncher(true);
          openDesktopPanel();
        }
      },
      { passive: true }
    );

    setTimeout(positionAboveA11y, 800);
    setTimeout(positionAboveA11y, 2500);
    setTimeout(positionAboveA11y, 5000);

    return root;
  }

  function pauseVideo() {
    if (!video) return;
    try {
      video.pause();
    } catch (_e) {}
  }

  function mountVideo(frameEl) {
    if (!frameEl) return null;
    if (videoMounted && video) {
      if (video.parentElement !== frameEl) {
        frameEl.appendChild(video);
      }
      return video;
    }
    video = document.createElement("video");
    video.id = "oh-pvl-player";
    video.setAttribute("controls", "");
    video.setAttribute("playsinline", "");
    video.setAttribute("webkit-playsinline", "");
    video.setAttribute("preload", "none");
    video.setAttribute("controlslist", "nodownload");
    video.setAttribute("crossorigin", "anonymous");
    video.poster = VIDEO_POSTER_BLACK;
    video.setAttribute("aria-label", "Why Dealality platform overview video");
    var source = document.createElement("source");
    source.src = VIDEO_SRC;
    source.type = "video/mp4";
    video.appendChild(source);
    var trackEl = document.createElement("track");
    trackEl.kind = "captions";
    trackEl.srclang = "en";
    trackEl.label = "English";
    trackEl.default = true;
    try {
      if (!captionsUrl) {
        captionsUrl = URL.createObjectURL(
          new Blob([CAPTIONS_VTT], { type: "text/vtt" })
        );
      }
      trackEl.src = captionsUrl;
    } catch (_e) {}
    video.appendChild(trackEl);
    frameEl.appendChild(video);
    videoMounted = true;

    video.addEventListener("play", function () {
      if (!milestones.start) {
        milestones.start = true;
        track("platform_video_start", { element: "platform_overview" });
      }
    });
    video.addEventListener("timeupdate", function () {
      if (!video.duration || !isFinite(video.duration) || video.duration <= 0) return;
      var pct = (video.currentTime / video.duration) * 100;
      if (pct >= 25 && !milestones.p25) {
        milestones.p25 = true;
        track("platform_video_25", {
          element: "platform_overview",
          seconds: Math.round(video.currentTime),
        });
      }
      if (pct >= 50 && !milestones.p50) {
        milestones.p50 = true;
        track("platform_video_50", {
          element: "platform_overview",
          seconds: Math.round(video.currentTime),
        });
      }
      if (pct >= 75 && !milestones.p75) {
        milestones.p75 = true;
        track("platform_video_75", {
          element: "platform_overview",
          seconds: Math.round(video.currentTime),
        });
      }
    });
    video.addEventListener("ended", function () {
      if (!milestones.complete) {
        milestones.complete = true;
        track("platform_video_complete", { element: "platform_overview" });
      }
    });
    return video;
  }

  function revealLauncher() {
    if (shown || isDismissed()) return;
    ensureDom();
    positionAboveA11y();
    shown = true;
    root.removeAttribute("hidden");
    root.setAttribute("data-state", "collapsed");
    root.classList.add("is-visible");
    if (!impressionSent) {
      impressionSent = true;
      track("platform_video_launcher_impression", {
        element: "platform_video_launcher",
      });
    }
  }

  function openDesktopPanel() {
    ensureDom();
    var frame = document.getElementById("oh-pvl-frame");
    mountVideo(frame);
    panel.removeAttribute("hidden");
    panel.setAttribute("aria-modal", "true");
    root.setAttribute("data-state", "expanded");
    launcherBtn.setAttribute("aria-expanded", "true");
    document.getElementById("oh-pvl-collapsed").setAttribute("hidden", "");
    expanded = true;
    if (!reduceMotion) {
      root.classList.add("is-anim");
    }
    var minBtn = document.getElementById("oh-pvl-minimize");
    if (minBtn) minBtn.focus();
  }

  function openMobileModal(skipTrack) {
    ensureDom();
    var frame = document.getElementById("oh-pvl-modal-frame");
    mountVideo(frame);
    document.getElementById("oh-pvl-collapsed").setAttribute("hidden", "");
    modal.removeAttribute("hidden");
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    document.documentElement.style.overflow = "hidden";
    document.body.style.overflow = "hidden";
    root.setAttribute("data-state", "modal");
    launcherBtn.setAttribute("aria-expanded", "true");
    expanded = true;
    var closeBtn = document.getElementById("oh-pvl-modal-close");
    if (closeBtn) closeBtn.focus();
    if (!skipTrack) {
      // tracked in openLauncher
    }
  }

  function openLauncher(source) {
    ensureDom();
    lastFocus = launcherBtn || document.activeElement;
    if (isMobile()) {
      openMobileModal(false);
    } else {
      openDesktopPanel();
    }
    track("platform_video_launcher_open", {
      element: "platform_video_launcher",
      source: source || "launcher",
    });
  }

  function minimizeLauncher(silent) {
    pauseVideo();
    if (panel) {
      panel.setAttribute("hidden", "");
      panel.setAttribute("aria-modal", "false");
    }
    if (modal) {
      modal.classList.remove("is-open");
      modal.setAttribute("hidden", "");
      modal.setAttribute("aria-hidden", "true");
      document.documentElement.style.overflow = "";
      document.body.style.overflow = "";
    }
    document.getElementById("oh-pvl-collapsed").removeAttribute("hidden");
    root.setAttribute("data-state", "collapsed");
    launcherBtn.setAttribute("aria-expanded", "false");
    expanded = false;
    if (!silent) {
      track("platform_video_minimize", { element: "platform_overview" });
    }
    if (launcherBtn) {
      try {
        launcherBtn.focus();
      } catch (_e) {}
    }
  }

  function closeExpanded(reason) {
    var watched =
      video && isFinite(video.currentTime) ? Math.round(video.currentTime) : 0;
    pauseVideo();
    if (panel) {
      panel.setAttribute("hidden", "");
      panel.setAttribute("aria-modal", "false");
    }
    if (modal) {
      modal.classList.remove("is-open");
      modal.setAttribute("hidden", "");
      modal.setAttribute("aria-hidden", "true");
      document.documentElement.style.overflow = "";
      document.body.style.overflow = "";
    }
    document.getElementById("oh-pvl-collapsed").removeAttribute("hidden");
    root.setAttribute("data-state", "collapsed");
    launcherBtn.setAttribute("aria-expanded", "false");
    expanded = false;
    track("platform_video_close", {
      element: "platform_overview",
      seconds: watched,
      reason: reason || "close",
    });
    var focusEl = lastFocus || launcherBtn;
    if (focusEl && typeof focusEl.focus === "function") {
      try {
        focusEl.focus();
      } catch (_e) {}
    }
  }

  function dismissLauncher() {
    pauseVideo();
    setDismissed();
    track("platform_video_dismiss", { element: "platform_video_launcher" });
    if (expanded) closeExpanded("dismiss");
    if (root) {
      root.setAttribute("data-state", "hidden");
      root.classList.remove("is-visible");
      root.setAttribute("hidden", "");
    }
    shown = false;
    syncSecondaryCta();
  }

  function placeSecondaryInHero() {
    var wrap = document.getElementById("fsw-secondary-wrap");
    var hero = document.getElementById("hero");
    if (!wrap || !hero) return wrap;
    if (wrap.parentNode !== hero) {
      try {
        hero.appendChild(wrap);
      } catch (_e) {}
    }
    return wrap;
  }

  function syncSecondaryCta() {
    var wrap = placeSecondaryInHero();
    if (!wrap) return;
    if (isDismissed()) {
      wrap.removeAttribute("hidden");
      wrap.setAttribute("aria-hidden", "false");
      wrap.setAttribute("data-oh-visible", "1");
    } else {
      wrap.setAttribute("hidden", "");
      wrap.setAttribute("aria-hidden", "true");
      wrap.setAttribute("data-oh-visible", "0");
    }
  }

  function bindSecondaryCta() {
    syncSecondaryCta();
    var cta = document.getElementById("fsw-secondary");
    if (!cta) return;
    if (!cta.getAttribute("data-oh-pvl-bound") && cta.parentNode) {
      var clone = cta.cloneNode(true);
      clone.setAttribute("data-oh-pvl-bound", "1");
      cta.parentNode.replaceChild(clone, cta);
      cta = clone;
    }
    cta.setAttribute("aria-controls", "oh-pvl-panel");
    cta.setAttribute("href", "#oh-pvl");
    cta.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      if (isDismissed()) {
        try {
          sessionStorage.removeItem(DISMISS_KEY);
        } catch (_e) {}
      }
      ensureDom();
      root.removeAttribute("hidden");
      syncSecondaryCta();
      if (!shown) revealLauncher();
      openLauncher("cta");
    });
  }

  function maybeShowFromScroll() {
    if (shown || isDismissed()) return;
    var doc = document.documentElement;
    var scrollTop = window.scrollY || doc.scrollTop || 0;
    var height = Math.max(1, doc.scrollHeight - window.innerHeight);
    if (scrollTop / height >= SCROLL_TRIGGER) {
      clearTimeout(showTimer);
      revealLauncher();
    }
  }

  function boot() {
    injectCss();
    try {
      document.documentElement.classList.add("oh-pvl-active");
    } catch (_e) {}

    bindSecondaryCta();
    ensureDom();
    root.setAttribute("hidden", "");
    positionAboveA11y();
    syncSecondaryCta();

    showTimer = setTimeout(function () {
      revealLauncher();
    }, SHOW_DELAY_MS);

    window.addEventListener("scroll", maybeShowFromScroll, { passive: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
