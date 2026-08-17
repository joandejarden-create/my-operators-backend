/**
 * Dealality Old Home — Problem Deal Desk cinematic-v1 (Phase B)
 * Activates only when #about[data-oh-problem="deal-desk"] exists (not pathname-gated).
 * Controls: state progression, chapters, replay, visibility/tab, reduced-motion, PVL hide, analytics.
 * Does not replace embed HTML or native Webflow copy.
 */
(function () {
  "use strict";

  var SECTION_SEL = '#about[data-oh-problem="deal-desk"]';
  var DESK_SEL = "[data-dealality-problem-desk]";
  var INIT_ATTR = "data-deal-desk-initialized";
  var MOTION_ATTR = "data-deal-desk-motion";
  var COMPLETE_ATTR = "data-deal-desk-complete";
  var PVL_ABOUT_ATTR = "data-oh-pvl-about";

  var STATES = [
    "opportunity",
    "workstreams",
    "artifacts",
    "comparison",
    "momentum",
    "outcome",
  ];

  var CHAPTER_BY_STATE = {
    opportunity: "fragmented",
    workstreams: "fragmented",
    artifacts: "fragmented",
    comparison: "responses",
    momentum: "upside",
    outcome: "upside",
  };

  /* ~12s total desktop */
  var DESKTOP_AT = [0, 1500, 3500, 5500, 8000, 10000];
  var DESKTOP_TOTAL = 11500;
  /* Simplified / shorter mobile */
  var MOBILE_AT = [0, 900, 2000, 3300, 4800, 6200];
  var MOBILE_TOTAL = 7600;

  var MOBILE_MQ = "(max-width: 640px)";
  var VIEW_RATIO = 0.33;

  var section = null;
  var desk = null;
  var replayBtn = null;
  var timers = [];
  var observer = null;
  var pvlObserver = null;
  var started = false;
  var completed = false;
  var running = false;
  var pausedAt = null;
  var pausedStateIndex = 0;
  var reduced = false;
  var freezeState = "";

  function prefersReducedMotion() {
    try {
      return (
        window.matchMedia &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches
      );
    } catch (_e) {
      return false;
    }
  }

  function isMobile() {
    try {
      return window.matchMedia && window.matchMedia(MOBILE_MQ).matches;
    } catch (_e2) {
      return false;
    }
  }

  function track(name) {
    try {
      window.dataLayer = window.dataLayer || [];
      window.dataLayer.push({ event: name });
    } catch (_e) {}
    try {
      if (typeof window.gtag === "function") {
        window.gtag("event", name, { event_category: "old_home_problem" });
      }
    } catch (_e2) {}
  }

  function clearTimers() {
    timers.forEach(function (id) {
      clearTimeout(id);
    });
    timers = [];
  }

  function schedule(fn, ms) {
    timers.push(setTimeout(fn, ms));
  }

  function fromQuery() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var state = String(params.get("dealDeskState") || "")
        .trim()
        .toLowerCase();
      return STATES.indexOf(state) >= 0 ? state : "";
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[deal-desk] query parse failed", err);
      }
      return "";
    }
  }

  function setChapter(chapterKey) {
    if (!section) return;
    var nodes = section.querySelectorAll("[data-problem-chapter]");
    nodes.forEach(function (el) {
      var key = el.getAttribute("data-problem-chapter");
      var on = key === chapterKey;
      el.classList.toggle("is-active", on);
      if (on) {
        el.setAttribute("aria-current", "step");
      } else {
        el.removeAttribute("aria-current");
      }
    });
  }

  function setState(state) {
    if (!desk || STATES.indexOf(state) < 0) return;
    desk.setAttribute("data-story-state", state);
    setChapter(CHAPTER_BY_STATE[state] || "fragmented");

    desk.classList.toggle("is-reveal-fields", state === "opportunity");
    desk.classList.toggle(
      "is-reveal-paths",
      state === "workstreams" || state === "artifacts"
    );
    desk.classList.toggle(
      "is-reveal-lanes",
      state === "workstreams" || state === "artifacts"
    );
    desk.classList.toggle("is-reveal-artifacts", state === "artifacts");
    desk.classList.toggle("is-reveal-compare", state === "comparison");
    desk.classList.toggle("is-reveal-momentum", state === "momentum");
    desk.classList.toggle("is-reveal-outcome", state === "outcome");
  }

  function showReplay(show) {
    if (!replayBtn) return;
    if (reduced) {
      replayBtn.hidden = true;
      replayBtn.setAttribute("aria-hidden", "true");
      return;
    }
    replayBtn.hidden = !show;
    replayBtn.setAttribute("aria-hidden", show ? "false" : "true");
  }

  function markComplete() {
    completed = true;
    running = false;
    if (section) section.setAttribute(COMPLETE_ATTR, "true");
    setState("outcome");
    setChapter("upside");
    showReplay(true);
    track("problem_storyboard_complete");
  }

  function timelinePlan() {
    if (isMobile()) {
      return { at: MOBILE_AT, total: MOBILE_TOTAL };
    }
    return { at: DESKTOP_AT, total: DESKTOP_TOTAL };
  }

  function runTimeline(fromReplay, startIndex) {
    if (!desk || reduced) return;
    if (running && !fromReplay && startIndex == null) return;

    clearTimers();
    running = true;
    completed = false;
    pausedAt = null;
    if (section) section.removeAttribute(COMPLETE_ATTR);
    showReplay(false);

    var plan = timelinePlan();
    var startIdx = typeof startIndex === "number" ? startIndex : 0;
    if (startIdx < 0) startIdx = 0;
    if (startIdx >= STATES.length) startIdx = STATES.length - 1;

    var base = plan.at[startIdx] || 0;
    setState(STATES[startIdx]);

    if (fromReplay) track("problem_storyboard_replay");

    for (var i = startIdx + 1; i < STATES.length; i++) {
      (function (idx) {
        var delay = Math.max(0, (plan.at[idx] || 0) - base);
        schedule(function () {
          if (document.hidden) return;
          setState(STATES[idx]);
          pausedStateIndex = idx;
        }, delay);
      })(i);
    }

    var remain = Math.max(0, plan.total - base);
    schedule(function () {
      if (document.hidden) return;
      markComplete();
    }, remain);
  }

  function ensureReplayControl() {
    if (!section) return;
    var existing = section.querySelector("[data-deal-desk-replay]");
    if (existing) {
      replayBtn = existing;
      return;
    }
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "dpd-replay";
    btn.setAttribute("data-deal-desk-replay", "1");
    btn.textContent = "Replay the Process";
    btn.hidden = true;
    btn.setAttribute("aria-hidden", "true");
    btn.addEventListener("click", function () {
      clearTimers();
      running = false;
      completed = false;
      started = true;
      pausedStateIndex = 0;
      enableMotion();
      runTimeline(true, 0);
    });
    var stage = section.querySelector(".oh-problem-stage") || desk.parentNode;
    if (stage && stage.parentNode) {
      stage.parentNode.insertBefore(btn, stage.nextSibling);
    } else {
      section.appendChild(btn);
    }
    replayBtn = btn;
  }

  function dedupePvl() {
    var nodes = document.querySelectorAll("#oh-pvl");
    if (nodes.length <= 1) return nodes.length;
    for (var i = 1; i < nodes.length; i++) {
      if (nodes[i] && nodes[i].parentNode) {
        nodes[i].parentNode.removeChild(nodes[i]);
      }
    }
    return 1;
  }

  function setPvlAboutHidden(hidden) {
    try {
      if (hidden) {
        document.documentElement.setAttribute(PVL_ABOUT_ATTR, "1");
      } else {
        document.documentElement.removeAttribute(PVL_ABOUT_ATTR);
      }
    } catch (_e) {}
    dedupePvl();
  }

  function aboutVisibleRatio() {
    if (!section) return 0;
    var r = section.getBoundingClientRect();
    var vh = window.innerHeight || 1;
    var visible = Math.max(0, Math.min(r.bottom, vh) - Math.max(r.top, 0));
    return r.height > 0 ? visible / r.height : 0;
  }

  function syncPvlForAbout() {
    setPvlAboutHidden(aboutVisibleRatio() >= 0.35);
  }

  function onVisibilityChange() {
    if (!started || reduced || freezeState) return;
    if (document.hidden) {
      if (running) {
        clearTimers();
        running = false;
        pausedAt = Date.now();
      }
      return;
    }
    /* Resume: advance from current / next valid state, do not restart from opportunity */
    if (completed) return;
    if (pausedAt != null) {
      var next = Math.min(pausedStateIndex + 1, STATES.length - 1);
      pausedAt = null;
      runTimeline(false, next);
    }
  }

  function enableMotion() {
    if (!section || reduced) return;
    section.setAttribute(MOTION_ATTR, "1");
  }

  function startOnce() {
    if (started || freezeState) return;
    started = true;
    track("problem_storyboard_view");

    if (reduced) {
      if (section) section.setAttribute(MOTION_ATTR, "0");
      setState("outcome");
      setChapter("upside");
      if (section) section.setAttribute(COMPLETE_ATTR, "true");
      completed = true;
      showReplay(false);
      track("problem_storyboard_complete");
      return;
    }

    /* Enable motion CSS only when the sequence actually starts so the
       resting Opportunity card stays readable before the trigger fires. */
    enableMotion();
    runTimeline(false, 0);
  }

  function bindIntersection() {
    if (!section || freezeState) return;
    var target =
      section.querySelector(".dpd-stage") ||
      desk ||
      section;
    if (typeof IntersectionObserver === "undefined") {
      startOnce();
      return;
    }
    observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var ratio = entry.intersectionRatio;
          /* Trigger on the visual stage (~33% visible), not the tall #about
             copy+chapters wrapper — that made the desk look static/empty
             while motion CSS hid fields waiting for startOnce. */
          if (ratio >= VIEW_RATIO - 0.02) {
            startOnce();
            if (observer) {
              observer.disconnect();
              observer = null;
            }
          }
        });
      },
      { threshold: [0, 0.2, 0.3, 0.33, 0.35, 0.5, 0.75, 1] }
    );
    observer.observe(target);
  }

  function bindPvlObserver() {
    if (!section) return;
    syncPvlForAbout();
    if (typeof IntersectionObserver === "undefined") {
      window.addEventListener("scroll", syncPvlForAbout, { passive: true });
      return;
    }
    pvlObserver = new IntersectionObserver(
      function () {
        syncPvlForAbout();
      },
      { threshold: [0, 0.2, 0.35, 0.5, 0.75, 1] }
    );
    pvlObserver.observe(section);
    window.addEventListener("scroll", syncPvlForAbout, { passive: true });
  }

  function init() {
    section = document.querySelector(SECTION_SEL);
    if (!section) return;
    if (section.getAttribute(INIT_ATTR) === "true") return;
    section.setAttribute(INIT_ATTR, "true");

    desk = section.querySelector(DESK_SEL);
    if (!desk) return;

    reduced = prefersReducedMotion();
    freezeState = fromQuery();

    /* Keep motion off until startOnce so Opportunity fields stay visible
       while waiting for the stage IntersectionObserver trigger. */
    section.setAttribute(MOTION_ATTR, "0");
    ensureReplayControl();
    dedupePvl();
    bindPvlObserver();
    document.addEventListener("visibilitychange", onVisibilityChange);

    if (freezeState) {
      /* Deterministic QA: hold a single state; no timed sequence */
      setState(freezeState);
      showReplay(false);
      return;
    }

    if (reduced) {
      startOnce();
      return;
    }

    bindIntersection();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  window.DealalityDealDesk = {
    setState: function (state) {
      if (!desk) init();
      if (!desk) return false;
      clearTimers();
      running = false;
      setState(state);
      return true;
    },
    replay: function () {
      if (replayBtn) replayBtn.click();
    },
    dedupePvl: dedupePvl,
  };
})();
