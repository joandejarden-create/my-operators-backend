/**
 * ADP Guided Report Tour V1.1 — on-demand "How to Read This Report".
 *
 * Gates:
 *   GUIDED_TOUR_TARGET_VISIBLE_BEFORE_RENDER
 *   GUIDED_TOUR_PROGRAMMATIC_SCROLL_NOT_BLOCKED
 *   GUIDED_TOUR_CALLOUT_VIEWPORT_CONTAINMENT
 *   GUIDED_TOUR_NO_DEAD_END_STEP
 *   GUIDED_TOUR_DYNAMIC_STEP_COUNT_INTEGRITY
 *   ADP_GUIDED_TOUR_FILTER_ROW_ENTRY_POINT
 *   ADP_GUIDED_TOUR_BUTTON_PLATFORM_PARITY
 *   GUIDED_TOUR_POPOVER_PLATFORM_PARITY
 *   ADP_GUIDED_TOUR_REALITY_GAPS_SCROLL_REGRESSION
 *   GUIDED_TOUR_ALWAYS_REPLAYABLE
 *   GUIDED_TOUR_DURABLE_TARGET_ANCHORS
 *   GUIDED_TOUR_REPORT_STATE_PRESERVATION
 *   GUIDED_TOUR_ACCESSIBILITY
 *   GUIDED_TOUR_DOCUMENT_ORDER
 *   GUIDED_TOUR_FINISH_NO_SCROLL_JUMP
 *   GUIDED_TOUR_CLEAR_NOT_CLEVER
 *   GUIDED_TOUR_ANALYTICAL_TERMINOLOGY_INTEGRITY
 *   GUIDED_TOUR_CONCISE_BULLET_READABILITY
 *   GUIDED_TOUR_EXECUTIVE_INTERPRETATION_COMPLETE
 *   GUIDED_TOUR_NO_FAKE_GOOD_BAD_THRESHOLDS
 *
 * State restore policy:
 *   Exit early → restore captured scroll + filters
 *   Finish → keep final scroll position; restore filters/drawer only
 */
(function (global) {
  "use strict";

  var TOUR_VERSION = "ADP_GUIDED_REPORT_TOUR_V1_3_DOC_ORDER";
  var ANALYTICS_KEY = "adp_guided_tour_analytics_v1";
  var BUTTON_ID = "adpHowToReadReport";
  var SAFE_TOP = 100;
  var SAFE_BOTTOM = 160;
  var CALLOUT_W = 380;

  /**
   * Document top-to-bottom order (no mid-tour scroll jumps).
   * GUIDED_TOUR_DOCUMENT_ORDER
   * Hybrid step shape: guide line (demo voice) + Stronger/Weaker + Look next.
   * GUIDED_TOUR_CONCISE_BULLET_READABILITY
   */
  var STEP_DEFS = [
    {
      id: "executive-read",
      target: '[data-adp-tour-target="executive-read"]',
      title: "Start With the Executive Read",
      guide:
        "This is where you get the hotel's current AI demand position in one place — strongest signal, main constraint, and the management priority.",
      signal:
        "A stronger read shows broad, consistent visibility; a weaker read points to where attention is needed.",
      lookNext: "Note the two or three issues worth reviewing in more detail below.",
    },
    {
      id: "ai-consideration",
      target: '[data-adp-tour-target="ai-consideration"]',
      title: "AI Consideration",
      guide:
        "This is the broadest read on how often the hotel appears across the AI answers Dealality monitors.",
      signal:
        "Higher is generally better. Low or declining means the hotel is left out of more monitored answers.",
      lookNext: "Compare with Prior Run, then open Demand Territories to see where it comes from.",
    },
    {
      id: "scenario-presence",
      target: '[data-adp-tour-target="scenario-presence"]',
      title: "Scenario Presence",
      guide:
        "This shows how broadly the hotel shows up across the different traveler situations we test.",
      signal:
        "Higher coverage means relevance across more trip types; lower means some needs may not recognize the hotel at all.",
      lookNext: "Read this beside AI Consideration to separate broad visibility from uneven visibility.",
    },
    {
      id: "presence-index",
      target: '[data-adp-tour-target="presence-index"]',
      title: "Presence Index",
      guide:
        "This compares the hotel's AI presence with the average of the governed comparable hotels.",
      signal:
        "Around 100 is peer parity. Above 100 is stronger relative presence; below 100 is weaker.",
      lookNext: "Read the Index with the underlying hotel and peer presence rates.",
      note: "This is not market share or RevPAR Index.",
      optional: true,
    },
    {
      id: "demand-territories",
      target: '[data-adp-tour-target="demand-territories"]',
      title: "Demand Territories",
      guide:
        "This breaks visibility down by traveler need so you can see which trip types are driving the result.",
      signal:
        "Stronger territories show consistent recognition; weaker ones show gaps or competitor advantage.",
      lookNext: "Start with commercially important territories that have the largest gaps.",
    },
    {
      id: "trends",
      target: '[data-adp-tour-target="trends"]',
      title: "Trends",
      guide:
        "Trends shows how the hotel's AI demand position moves across comparable monitoring periods.",
      signal:
        "Improving results are generally positive; repeated declines deserve investigation. One movement alone is not a trend.",
      lookNext: "Connect changes back to Demand Territories, evidence, and open management actions.",
      note: "Movement after an action does not by itself prove causation.",
      optional: true,
    },
    {
      id: "provider-presence",
      target: '[data-adp-tour-target="provider-presence"]',
      title: "Provider Presence",
      guide:
        "This shows how consistently the hotel appears across the AI platforms Dealality monitors.",
      signal:
        "Balanced visibility across providers is generally stronger than depending heavily on one.",
      lookNext: "Focus on large or persistent provider gaps, not small one-period differences.",
      optional: true,
    },
    {
      id: "reality-gaps",
      target: '[data-adp-tour-target="reality-gaps"]',
      title: "Reality Gaps",
      guide:
        "This checks whether important property attributes are actually recognized in monitored AI answers.",
      signal:
        "High recognition is stronger. Low recognition may mean an attribute is missing, unclear, or not being surfaced.",
      lookNext: "Prioritize commercially important attributes with low recognition.",
    },
    {
      id: "competitive-displacement",
      target: '[data-adp-tour-target="competitive-displacement"]',
      title: "Competitive Displacement",
      guide:
        "When your hotel is absent, this shows which comparable hotels appear in the same monitored answers.",
      signal:
        "Repeated displacement by the same competitor matters more than a one-off appearance.",
      lookNext: "Open the evidence to see the exact traveler scenarios where it happens.",
    },
    {
      id: "evidence",
      target: '[data-adp-tour-target="evidence"]',
      title: "Evidence",
      guide:
        "Evidence is the actual monitored AI response behind a metric or finding — use it to verify the pattern.",
      signal:
        "Evidence is not scored good or bad; it confirms whether the observed pattern is real and understandable.",
      lookNext: "Check evidence before acting on surprising, material, or competitive findings.",
    },
  ];

  /**
   * Finish card — no page target / no scroll (GUIDED_TOUR_FINISH_NO_SCROLL_JUMP).
   * Stays at the last content scroll position (typically Evidence).
   */
  var FINISH_STEP = {
    id: "how-to-use",
    target: null,
    title: "How to Use the Report",
    finishCard: true,
    noScroll: true,
    bullets: [
      "Start with the Executive Read.",
      "Identify where visibility is strong or weak by traveler need.",
      "Verify the evidence, review competitors, and track whether the position changes over time.",
    ],
    note: "Use the Monthly Executive Review for recommended actions, accountable owners, and next-monitoring priorities.",
  };

  var state = {
    active: false,
    programmaticScrolling: false,
    stepIndex: 0,
    resolvedSteps: [],
    saved: null,
    root: null,
    onKeyDown: null,
    onWheel: null,
    onTouch: null,
    transitionToken: 0,
  };

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function prefersReducedMotion() {
    try {
      return window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (_) {
      return false;
    }
  }

  function loadAnalytics() {
    try {
      var raw = localStorage.getItem(ANALYTICS_KEY);
      return raw ? JSON.parse(raw) : { replayCount: 0, events: [] };
    } catch (_) {
      return { replayCount: 0, events: [] };
    }
  }

  function saveAnalytics(data) {
    try {
      localStorage.setItem(ANALYTICS_KEY, JSON.stringify(data));
    } catch (_) {}
  }

  function track(event, extra) {
    var data = loadAnalytics();
    if (event === "tourStarted") {
      data.replayCount = (data.replayCount || 0) + 1;
      data.tourStarted = true;
      data.lastStartedAt = new Date().toISOString();
    }
    if (event === "tourCompleted") data.tourCompleted = true;
    if (event === "tourExited") data.tourExited = true;
    if (extra && extra.lastStepReached != null) data.lastStepReached = extra.lastStepReached;
    data.events = (data.events || []).slice(-40);
    data.events.push({
      event: event,
      at: new Date().toISOString(),
      tourVersion: TOUR_VERSION,
      extra: extra || null,
    });
    saveAnalytics(data);
  }

  function warnSkip(stepId, reason) {
    try {
      console.warn("[ADP Guided Tour] skip step", stepId, reason);
    } catch (_) {}
  }

  function elementExistsInReport(el) {
    if (!el) return false;
    var success = document.getElementById("adpStateSuccess");
    if (!success || success.hidden) return false;
    if (!success.contains(el)) return false;
    if (el.hidden) return false;
    var style = window.getComputedStyle(el);
    if (style.display === "none" || style.visibility === "hidden") return false;
    return true;
  }

  function isTargetResolvable(selector) {
    var el = $(selector);
    if (!elementExistsInReport(el)) return false;
    var rect = el.getBoundingClientRect();
    // Zero-size before layout: still allow if offsetParent exists and has content box via scrollHeight
    if (rect.width < 2 && rect.height < 2 && !(el.offsetWidth || el.scrollHeight)) return false;
    return true;
  }

  function resolveSteps() {
    var out = [];
    STEP_DEFS.forEach(function (step) {
      if (!step.target || !isTargetResolvable(step.target)) {
        if (!step.optional) warnSkip(step.id, "target unavailable");
        return;
      }
      out.push(step);
    });
    // Finish is always appended when any content steps resolve — no page target / no scroll.
    if (out.length) out.push(FINISH_STEP);
    return out;
  }

  function captureReportState() {
    var prop = document.getElementById("adpProperty");
    var territory = document.getElementById("adpCompTerritorySelect");
    var drawer = document.getElementById("adpEvidenceDrawer");
    return {
      scrollX: window.scrollX || window.pageXOffset || 0,
      scrollY: window.scrollY || window.pageYOffset || 0,
      propertyId: prop ? prop.value : null,
      demandTerritory: territory ? territory.value : null,
      evidenceDrawerOpen: !!(drawer && drawer.open),
    };
  }

  function restoreReportState(saved, opts) {
    if (!saved) return;
    opts = opts || {};
    var territory = document.getElementById("adpCompTerritorySelect");
    if (territory && saved.demandTerritory != null && territory.value !== saved.demandTerritory) {
      territory.value = saved.demandTerritory;
      try {
        territory.dispatchEvent(new Event("change", { bubbles: true }));
      } catch (_) {}
    }
    var drawer = document.getElementById("adpEvidenceDrawer");
    if (drawer && saved.evidenceDrawerOpen === false && drawer.open) {
      try {
        drawer.close();
      } catch (_) {}
    }
    if (opts.restoreScroll !== false) {
      if (typeof saved.scrollX === "number" && typeof saved.scrollY === "number") {
        state.programmaticScrolling = true;
        window.scrollTo(saved.scrollX, saved.scrollY);
        state.programmaticScrolling = false;
      }
    }
  }

  function ensureButton() {
    var btn = document.getElementById(BUTTON_ID);
    if (!btn) return null;
    btn.hidden = false;
    btn.disabled = false;
    btn.removeAttribute("aria-disabled");
    return btn;
  }

  function keyContentRect(el) {
    var r = el.getBoundingClientRect();
    var keyH = Math.min(Math.max(r.height, 1), 140);
    return {
      top: r.top,
      left: r.left,
      width: r.width,
      height: r.height,
      keyBottom: r.top + keyH,
      full: r,
    };
  }

  function isInSafeViewport(el) {
    var k = keyContentRect(el);
    var vh = window.innerHeight;
    return k.top >= SAFE_TOP - 4 && k.keyBottom <= vh - SAFE_BOTTOM + 4;
  }

  function waitMs(ms) {
    return new Promise(function (resolve) {
      window.setTimeout(resolve, ms);
    });
  }

  function waitForStableBounds(el, maxMs) {
    maxMs = maxMs || 900;
    return new Promise(function (resolve) {
      var start = Date.now();
      var last = null;
      var stableTicks = 0;

      function tick() {
        if (!el || !el.isConnected) {
          resolve(false);
          return;
        }
        var r = el.getBoundingClientRect();
        var sig = [Math.round(r.top), Math.round(r.left), Math.round(r.width), Math.round(r.height)].join(",");
        if (sig === last) stableTicks += 1;
        else {
          stableTicks = 0;
          last = sig;
        }
        if (stableTicks >= 2 || Date.now() - start > maxMs) {
          resolve(isInSafeViewport(el) || true);
          return;
        }
        window.requestAnimationFrame(tick);
      }
      window.requestAnimationFrame(tick);
    });
  }

  /**
   * GUIDED_TOUR_PROGRAMMATIC_SCROLL_NOT_BLOCKED
   * Never use body overflow:hidden — it blocks window.scrollTo in Chromium.
   */
  function scrollTargetIntoSafeZone(el) {
    state.programmaticScrolling = true;
    var r = el.getBoundingClientRect();
    var vh = window.innerHeight;
    var safeH = Math.max(120, vh - SAFE_TOP - SAFE_BOTTOM);
    var targetY;
    if (r.height > safeH * 0.85) {
      targetY = window.scrollY + r.top - SAFE_TOP;
    } else {
      targetY = window.scrollY + r.top - SAFE_TOP - (safeH - Math.min(r.height, safeH)) / 2;
    }
    targetY = Math.max(0, Math.round(targetY));
    var behavior = prefersReducedMotion() ? "auto" : "smooth";
    try {
      window.scrollTo({ top: targetY, left: 0, behavior: behavior });
    } catch (_) {
      window.scrollTo(0, targetY);
    }
    var wait = prefersReducedMotion() ? 40 : 320;
    return waitMs(wait)
      .then(function () {
        return waitForStableBounds(el);
      })
      .then(function () {
        if (!isInSafeViewport(el)) {
          // Second pass — auto jump if smooth scroll under-shot
          r = el.getBoundingClientRect();
          if (r.height > safeH * 0.85) {
            targetY = window.scrollY + r.top - SAFE_TOP;
          } else {
            targetY = window.scrollY + r.top - SAFE_TOP - (safeH - Math.min(r.height, safeH)) / 2;
          }
          window.scrollTo(0, Math.max(0, Math.round(targetY)));
          return waitMs(60).then(function () {
            return waitForStableBounds(el, 400);
          });
        }
        return true;
      })
      .then(function (ok) {
        state.programmaticScrolling = false;
        return ok;
      })
      .catch(function () {
        state.programmaticScrolling = false;
        return false;
      });
  }

  function buildDom() {
    if (state.root) return state.root;
    var root = document.createElement("div");
    root.id = "adpGuidedTourRoot";
    root.className = "adp-gt";
    root.setAttribute("data-adp-guided-tour", "1");
    root.setAttribute("hidden", "");
    root.innerHTML =
      '<div class="adp-gt__highlight" data-adp-gt="highlight" aria-hidden="true"></div>' +
      '<div class="adp-gt__callout" role="dialog" aria-modal="true" aria-labelledby="adpGtTitle" aria-describedby="adpGtBody" hidden>' +
      '  <button type="button" class="adp-gt__close" data-adp-gt="exit" aria-label="Exit walkthrough">×</button>' +
      '  <div class="adp-gt__progress-wrap">' +
      '    <p class="adp-gt__progress" id="adpGtProgress"></p>' +
      '    <div class="adp-gt__progress-bar" aria-hidden="true"><span class="adp-gt__progress-fill" id="adpGtProgressFill"></span></div>' +
      "  </div>" +
      '  <h2 class="adp-gt__title" id="adpGtTitle"></h2>' +
      '  <p class="adp-gt__body" id="adpGtBody"></p>' +
      '  <ul class="adp-gt__finish-list" id="adpGtFinishList" hidden></ul>' +
      '  <div class="adp-gt__row" id="adpGtSignalRow">' +
      '    <p class="adp-gt__row-label">Stronger / Weaker</p>' +
      '    <p class="adp-gt__row-text" id="adpGtSignal"></p>' +
      "  </div>" +
      '  <div class="adp-gt__row" id="adpGtLookRow">' +
      '    <p class="adp-gt__row-label">Look Next</p>' +
      '    <p class="adp-gt__row-text" id="adpGtLook"></p>' +
      "  </div>" +
      '  <p class="adp-gt__note" id="adpGtNote" hidden></p>' +
      '  <div class="adp-gt__actions">' +
      '    <button type="button" class="adp-gt__btn adp-gt__btn--quiet" data-adp-gt="exit">Exit</button>' +
      '    <button type="button" class="adp-gt__btn adp-gt__btn--secondary" data-adp-gt="restart" hidden>Restart</button>' +
      '    <button type="button" class="adp-gt__btn adp-gt__btn--secondary" data-adp-gt="back">Back</button>' +
      '    <button type="button" class="adp-gt__btn adp-gt__btn--primary" data-adp-gt="next">Next</button>' +
      "  </div>" +
      "</div>";
    document.body.appendChild(root);

    root.addEventListener("click", function (e) {
      var t = e.target.closest("[data-adp-gt]");
      if (!t) return;
      var action = t.getAttribute("data-adp-gt");
      if (action === "exit") exitTour("exit");
      else if (action === "back") go(-1);
      else if (action === "next") go(1);
      else if (action === "restart") {
        state.stepIndex = 0;
        renderStep();
      }
    });

    state.root = root;
    return root;
  }

  function muteCallout() {
    if (!state.root) return;
    var callout = state.root.querySelector(".adp-gt__callout");
    var highlight = state.root.querySelector('[data-adp-gt="highlight"]');
    if (callout) {
      callout.setAttribute("hidden", "");
      callout.classList.add("adp-gt__callout--muted");
    }
    if (highlight) highlight.classList.add("adp-gt__highlight--muted");
  }

  function showCallout() {
    if (!state.root) return;
    var callout = state.root.querySelector(".adp-gt__callout");
    var highlight = state.root.querySelector('[data-adp-gt="highlight"]');
    if (callout) {
      callout.removeAttribute("hidden");
      callout.classList.remove("adp-gt__callout--muted");
    }
    if (highlight) highlight.classList.remove("adp-gt__highlight--muted");
  }

  function placeHighlight(el) {
    var highlight = state.root.querySelector('[data-adp-gt="highlight"]');
    if (!highlight || !el) return null;
    var pad = 8;
    var r = el.getBoundingClientRect();
    var maxH = Math.floor(window.innerHeight * 0.4);
    var hlH = Math.min(maxH, Math.max(44, r.height + pad * 2));
    var hlW = Math.min(window.innerWidth - 16, Math.max(44, r.width + pad * 2));
    var hlTop = Math.max(8, Math.min(r.top - pad, window.innerHeight - hlH - 8));
    var hlLeft = Math.max(8, Math.min(r.left - pad, window.innerWidth - hlW - 8));
    highlight.style.top = hlTop + "px";
    highlight.style.left = hlLeft + "px";
    highlight.style.width = hlW + "px";
    highlight.style.height = hlH + "px";
    return { top: hlTop, left: hlLeft, width: hlW, height: hlH };
  }

  /** GUIDED_TOUR_CALLOUT_VIEWPORT_CONTAINMENT — prefer right, left, bottom, top */
  function placeCallout(hl) {
    var callout = state.root.querySelector(".adp-gt__callout");
    if (!callout || !hl) return false;
    var gap = 14;
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var w = Math.min(CALLOUT_W, vw - 24);
    callout.style.width = w + "px";
    callout.style.maxHeight = Math.floor(vh * 0.55) + "px";
    // Temporarily show for measure
    callout.removeAttribute("hidden");
    var h = Math.min(callout.scrollHeight || 280, Math.floor(vh * 0.55));

    var candidates = [
      { name: "right", top: hl.top, left: hl.left + hl.width + gap },
      { name: "left", top: hl.top, left: hl.left - w - gap },
      { name: "bottom", top: hl.top + hl.height + gap, left: hl.left + hl.width / 2 - w / 2 },
      { name: "top", top: hl.top - h - gap, left: hl.left + hl.width / 2 - w / 2 },
    ];

    function fits(c) {
      return c.left >= 12 && c.left + w <= vw - 12 && c.top >= 12 && c.top + h <= vh - 12;
    }

    var chosen = null;
    for (var i = 0; i < candidates.length; i++) {
      if (fits(candidates[i])) {
        chosen = candidates[i];
        break;
      }
    }
    if (!chosen) {
      chosen = {
        name: "fallback",
        top: Math.max(12, vh - h - 16),
        left: Math.max(12, (vw - w) / 2),
      };
    }
    chosen.left = Math.max(12, Math.min(chosen.left, vw - w - 12));
    chosen.top = Math.max(12, Math.min(chosen.top, vh - h - 12));
    callout.style.top = Math.round(chosen.top) + "px";
    callout.style.left = Math.round(chosen.left) + "px";
    callout.setAttribute("data-adp-gt-placement", chosen.name);

    var box = callout.getBoundingClientRect();
    return box.top >= 0 && box.bottom <= vh + 1 && box.left >= 0 && box.right <= vw + 1;
  }

  function fillStepContent(step, index, total) {
    var progress = state.root.querySelector("#adpGtProgress");
    var fill = state.root.querySelector("#adpGtProgressFill");
    var title = state.root.querySelector("#adpGtTitle");
    var body = state.root.querySelector("#adpGtBody");
    var finishList = state.root.querySelector("#adpGtFinishList");
    var signalRow = state.root.querySelector("#adpGtSignalRow");
    var lookRow = state.root.querySelector("#adpGtLookRow");
    var signal = state.root.querySelector("#adpGtSignal");
    var look = state.root.querySelector("#adpGtLook");
    var note = state.root.querySelector("#adpGtNote");
    var nextBtn = state.root.querySelector('[data-adp-gt="next"]');
    var backBtn = state.root.querySelector('[data-adp-gt="back"]');
    var restartBtn = state.root.querySelector('[data-adp-gt="restart"]');
    var isLast = index >= total - 1;
    var isFinish = !!step.finishCard;

    if (progress) progress.textContent = index + 1 + " of " + total;
    if (fill) fill.style.width = Math.round(((index + 1) / total) * 100) + "%";
    if (title) title.textContent = step.title;

    if (isFinish) {
      if (body) {
        body.textContent = "";
        body.hidden = true;
      }
      if (finishList) {
        finishList.hidden = false;
        finishList.innerHTML = (step.bullets || [])
          .map(function (b) {
            return "<li>" + b + "</li>";
          })
          .join("");
      }
      if (signalRow) signalRow.hidden = true;
      if (lookRow) lookRow.hidden = true;
    } else {
      if (body) {
        body.hidden = false;
        body.textContent = step.guide || "";
      }
      if (finishList) {
        finishList.hidden = true;
        finishList.innerHTML = "";
      }
      if (signalRow) signalRow.hidden = !step.signal;
      if (lookRow) lookRow.hidden = !step.lookNext;
      if (signal) signal.textContent = step.signal || "";
      if (look) look.textContent = step.lookNext || "";
    }

    if (note) {
      note.textContent = step.note || "";
      note.hidden = !step.note;
    }
    if (backBtn) backBtn.disabled = index === 0;
    if (restartBtn) restartBtn.hidden = !isLast;
    if (nextBtn) nextBtn.textContent = isLast ? "Finish" : "Next";
  }

  function clearHighlight() {
    var highlight = state.root && state.root.querySelector('[data-adp-gt="highlight"]');
    if (!highlight) return;
    highlight.style.top = "0px";
    highlight.style.left = "0px";
    highlight.style.width = "0px";
    highlight.style.height = "0px";
    highlight.classList.add("adp-gt__highlight--muted");
  }

  /** Finish card: keep current scroll; place callout in viewport without re-anchoring. */
  function placeFinishCallout() {
    var callout = state.root.querySelector(".adp-gt__callout");
    if (!callout) return false;
    clearHighlight();
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var w = Math.min(CALLOUT_W, vw - 24);
    callout.style.width = w + "px";
    callout.removeAttribute("hidden");
    var h = Math.min(callout.scrollHeight || 320, Math.floor(vh * 0.7));
    var top = Math.max(16, Math.min(Math.round((vh - h) / 2), vh - h - 16));
    var left = Math.max(12, Math.round((vw - w) / 2));
    callout.style.top = top + "px";
    callout.style.left = left + "px";
    callout.setAttribute("data-adp-gt-placement", "finish-center");
    var box = callout.getBoundingClientRect();
    return box.top >= 0 && box.bottom <= vh + 1 && box.left >= 0 && box.right <= vw + 1;
  }

  function renderStep() {
    var token = ++state.transitionToken;
    var steps = state.resolvedSteps;
    if (!steps.length) {
      exitTour("exit");
      return;
    }

    muteCallout();

    function advancePastDead(fromIndex) {
      var i = fromIndex;
      while (i < steps.length) {
        var candidate = steps[i];
        if (candidate.finishCard || candidate.noScroll || !candidate.target) {
          state.stepIndex = i;
          return candidate;
        }
        var el = $(candidate.target);
        if (el && elementExistsInReport(el)) {
          state.stepIndex = i;
          return candidate;
        }
        warnSkip(candidate.id, "dead-end before render");
        i += 1;
      }
      return null;
    }

    var step = advancePastDead(state.stepIndex);
    if (!step) {
      exitTour("complete");
      return;
    }

    fillStepContent(step, state.stepIndex, steps.length);

    // GUIDED_TOUR_FINISH_NO_SCROLL_JUMP — do not re-target Executive Read / scroll to top.
    if (step.finishCard || step.noScroll || !step.target) {
      placeFinishCallout();
      showCallout();
      clearHighlight();
      track("stepViewed", { lastStepReached: step.id, index: state.stepIndex, total: steps.length });
      var finishNext = state.root.querySelector('[data-adp-gt="next"]');
      if (finishNext) finishNext.focus();
      return;
    }

    var el = $(step.target);
    scrollTargetIntoSafeZone(el).then(function (ok) {
      if (token !== state.transitionToken || !state.active) return;
      if (!ok || !elementExistsInReport(el)) {
        warnSkip(step.id, "could not bring target into safe viewport");
        if (state.stepIndex < steps.length - 1) {
          state.stepIndex += 1;
          renderStep();
        } else {
          exitTour("complete");
        }
        return;
      }
      var hl = placeHighlight(el);
      var contained = placeCallout(hl);
      if (!contained) {
        warnSkip(step.id, "callout could not be contained");
      }
      showCallout();
      track("stepViewed", { lastStepReached: step.id, index: state.stepIndex, total: steps.length });
      var nextBtn = state.root.querySelector('[data-adp-gt="next"]');
      if (nextBtn) nextBtn.focus();
    });
  }

  function go(delta) {
    var next = state.stepIndex + delta;
    if (delta > 0 && next >= state.resolvedSteps.length) {
      exitTour("complete");
      return;
    }
    if (next < 0) return;
    state.stepIndex = next;
    renderStep();
  }

  function onKeyDown(e) {
    if (!state.active) return;
    if (e.key === "Escape") {
      e.preventDefault();
      exitTour("exit");
    } else if (e.key === "ArrowRight") {
      e.preventDefault();
      go(1);
    } else if (e.key === "ArrowLeft") {
      e.preventDefault();
      go(-1);
    }
  }

  function blockUserScroll(e) {
    if (!state.active) return;
    if (state.programmaticScrolling) return;
    e.preventDefault();
  }

  function startTour() {
    ensureButton();
    var resolved = resolveSteps();
    if (!resolved.length) {
      window.alert("Load a property report first, then click How to Read This Report again.");
      return;
    }

    state.saved = captureReportState();
    state.resolvedSteps = resolved;
    state.stepIndex = 0;
    state.active = true;

    var root = buildDom();
    root.removeAttribute("hidden");
    document.documentElement.classList.add("adp-gt-active");
    document.body.classList.add("adp-gt-active");

    state.onKeyDown = onKeyDown;
    state.onWheel = blockUserScroll;
    state.onTouch = blockUserScroll;
    document.addEventListener("keydown", state.onKeyDown, true);
    document.addEventListener("wheel", state.onWheel, { passive: false, capture: true });
    document.addEventListener("touchmove", state.onTouch, { passive: false, capture: true });

    track("tourStarted", { stepCount: resolved.length });
    renderStep();
  }

  function exitTour(reason) {
    if (!state.active && !state.root) return;
    var last = state.resolvedSteps[state.stepIndex];
    if (reason === "complete") {
      track("tourCompleted", { lastStepReached: last && last.id });
    } else {
      track("tourExited", { lastStepReached: last && last.id });
    }

    state.active = false;
    state.transitionToken += 1;
    if (state.onKeyDown) {
      document.removeEventListener("keydown", state.onKeyDown, true);
      state.onKeyDown = null;
    }
    if (state.onWheel) {
      document.removeEventListener("wheel", state.onWheel, true);
      state.onWheel = null;
    }
    if (state.onTouch) {
      document.removeEventListener("touchmove", state.onTouch, true);
      state.onTouch = null;
    }
    document.documentElement.classList.remove("adp-gt-active");
    document.body.classList.remove("adp-gt-active");
    if (state.root) {
      muteCallout();
      state.root.setAttribute("hidden", "");
    }

    // Exit early → restore scroll. Finish → keep final report position.
    restoreReportState(state.saved, { restoreScroll: reason !== "complete" });
    state.saved = null;

    var btn = ensureButton();
    if (btn) btn.focus();
  }

  function bindHeaderButton() {
    var btn = ensureButton();
    if (!btn) return;
    btn.hidden = false;
    btn.disabled = false;
    btn.setAttribute("data-adp-guided-tour-entry", "1");
    if (!btn.getAttribute("data-adp-gt-bound")) {
      btn.setAttribute("data-adp-gt-bound", "1");
      btn.addEventListener("click", function () {
        startTour();
      });
    }
  }

  function init() {
    bindHeaderButton();
    document.addEventListener("adp:report-loaded", bindHeaderButton);
  }

  global.AdpGuidedReportTour = {
    version: TOUR_VERSION,
    init: init,
    start: startTour,
    exit: function () {
      exitTour("exit");
    },
    resolveSteps: resolveSteps,
    stepDefs: STEP_DEFS,
    finishStep: FINISH_STEP,
    buttonId: BUTTON_ID,
    analyticsKey: ANALYTICS_KEY,
    isActive: function () {
      return state.active;
    },
    isInSafeViewport: isInSafeViewport,
    SAFE_TOP: SAFE_TOP,
    SAFE_BOTTOM: SAFE_BOTTOM,
  };
})(typeof window !== "undefined" ? window : globalThis);
