/**
 * Old Home hero — platform overview poster + lazy modal player.
 * Video/source load only after user click (poster or Watch CTA).
 */
(function () {
  "use strict";

  var VIDEO_SRC =
    "https://res.cloudinary.com/dos2eqnzd/video/upload/q_auto,f_mp4/v1775832428/Video_Deck_Dealality_-_Short_Elevator_Pitch_Deck_Video_irnayw.mp4";
  var CAPTIONS_VTT =
    "WEBVTT\n\n" +
    "00:00:00.000 --> 00:00:20.000\n" +
    "Dealality platform overview — a confidential process for hotel owners.\n\n" +
    "00:00:20.000 --> 00:00:45.000\n" +
    "Start with opportunity assessment and credible strategic paths.\n\n" +
    "00:00:45.000 --> 00:01:15.000\n" +
    "Engage brands, operators, and advisors with structured outreach.\n\n" +
    "00:01:15.000 --> 00:01:45.000\n" +
    "Compare proposals and commercial trade-offs in one place.\n\n" +
    "00:01:45.000 --> 00:02:30.000\n" +
    "Move forward with clearer options and stronger leverage.\n";
  var API_BASE =
    (window.DEALALITY_API_BASE ||
      "https://my-operators-backend-production.up.railway.app") +
    "/api/marketing/landing-events";
  var SESSION_KEY = "dl_landing_sid_v1";

  var posterBtn = document.getElementById("hero-video-poster");
  var ctaBtn = document.getElementById("fsw-secondary");
  if (!posterBtn && !ctaBtn) return;

  var reduceMotion =
    window.matchMedia &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  var modal = null;
  var dialog = null;
  var video = null;
  var closeBtn = null;
  var lastFocus = null;
  var videoMounted = false;
  var milestones = { start: false, p25: false, p50: false, p75: false, complete: false };
  var focusables = [];

  function deviceCategory() {
    var w = window.innerWidth || 0;
    if (w < 768) return "mobile";
    if (w < 1024) return "tablet";
    return "desktop";
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

  function track(event, extra) {
    var payload = {
      event: event,
      sessionId: sessionId(),
      device: deviceCategory(),
      location: "hero",
      section: "hero",
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
        navigator.sendBeacon(API_BASE, new Blob([JSON.stringify(payload)], { type: "application/json" }));
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

  function ensureModal() {
    if (modal) return modal;
    modal = document.createElement("div");
    modal.id = "oh-video-modal";
    modal.setAttribute("hidden", "");
    modal.setAttribute("aria-hidden", "true");
    modal.innerHTML =
      '<div id="oh-video-dialog" role="dialog" aria-modal="true" aria-labelledby="oh-video-title">' +
      '<div id="oh-video-bar">' +
      '<p id="oh-video-title">2-minute platform overview</p>' +
      '<button type="button" id="oh-video-close" aria-label="Close video">×</button>' +
      "</div>" +
      '<div id="oh-video-frame"></div>' +
      "</div>";
    document.body.appendChild(modal);
    dialog = document.getElementById("oh-video-dialog");
    closeBtn = document.getElementById("oh-video-close");
    closeBtn.addEventListener("click", function (e) {
      e.preventDefault();
      closeModal();
    });
    modal.addEventListener("click", function (e) {
      if (e.target === modal) closeModal();
    });
    return modal;
  }

  function mountVideo() {
    if (videoMounted) return video;
    var frame = document.getElementById("oh-video-frame");
    if (!frame) return null;
    video = document.createElement("video");
    video.id = "oh-video-player";
    video.setAttribute("controls", "");
    video.setAttribute("playsinline", "");
    video.setAttribute("webkit-playsinline", "");
    video.setAttribute("preload", "metadata");
    video.setAttribute("controlslist", "nodownload");
    video.setAttribute(
      "aria-label",
      "Dealality platform overview video with captions"
    );
    var source = document.createElement("source");
    source.src = VIDEO_SRC;
    source.type = "video/mp4";
    video.appendChild(source);
    var track = document.createElement("track");
    track.kind = "captions";
    track.srclang = "en";
    track.label = "English";
    track.default = true;
    try {
      track.src = URL.createObjectURL(
        new Blob([CAPTIONS_VTT], { type: "text/vtt" })
      );
    } catch (_e) {}
    video.appendChild(track);
    frame.appendChild(video);
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
        track("platform_video_25", { element: "platform_overview", seconds: Math.round(video.currentTime) });
      }
      if (pct >= 50 && !milestones.p50) {
        milestones.p50 = true;
        track("platform_video_50", { element: "platform_overview", seconds: Math.round(video.currentTime) });
      }
      if (pct >= 75 && !milestones.p75) {
        milestones.p75 = true;
        track("platform_video_75", { element: "platform_overview", seconds: Math.round(video.currentTime) });
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

  function getFocusables() {
    if (!dialog) return [];
    return Array.prototype.slice.call(
      dialog.querySelectorAll(
        'button, [href], video, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      )
    ).filter(function (el) {
      return !el.hasAttribute("disabled") && el.offsetParent !== null;
    });
  }

  function onKeydown(e) {
    if (!modal || !modal.classList.contains("is-open")) return;
    if (e.key === "Escape") {
      e.preventDefault();
      closeModal();
      return;
    }
    if (e.key !== "Tab") return;
    focusables = getFocusables();
    if (!focusables.length) return;
    var first = focusables[0];
    var last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
    }
  }

  function openModal(source) {
    ensureModal();
    mountVideo();
    lastFocus = document.activeElement;
    modal.removeAttribute("hidden");
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    document.documentElement.style.overflow = "hidden";
    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", onKeydown, true);
    if (closeBtn) closeBtn.focus();
    if (video) {
      try {
        video.currentTime = 0;
      } catch (_e) {}
      if (!reduceMotion) {
        var playPromise = video.play();
        if (playPromise && typeof playPromise.catch === "function") {
          playPromise.catch(function () {});
        }
      }
    }
    track(source === "cta" ? "hero_video_cta_click" : "hero_video_poster_click", {
      element: source === "cta" ? "watch_platform_overview" : "hero_video_poster",
      label: source === "cta" ? "Watch Platform Overview" : "Video poster",
    });
  }

  function closeModal() {
    if (!modal) return;
    var watched = video && isFinite(video.currentTime) ? Math.round(video.currentTime) : 0;
    if (video) {
      try {
        video.pause();
        video.currentTime = 0;
      } catch (_e) {}
    }
    modal.classList.remove("is-open");
    modal.setAttribute("hidden", "");
    modal.setAttribute("aria-hidden", "true");
    document.documentElement.style.overflow = "";
    document.body.style.overflow = "";
    document.removeEventListener("keydown", onKeydown, true);
    track("platform_video_close", {
      element: "platform_overview",
      seconds: watched,
    });
    if (lastFocus && typeof lastFocus.focus === "function") {
      try {
        lastFocus.focus();
      } catch (_e2) {}
    }
    lastFocus = null;
  }

  if (posterBtn) {
    posterBtn.addEventListener("click", function (e) {
      e.preventDefault();
      openModal("poster");
    });
  }
  if (ctaBtn) {
    ctaBtn.addEventListener("click", function (e) {
      e.preventDefault();
      openModal("cta");
    });
  }
})();
