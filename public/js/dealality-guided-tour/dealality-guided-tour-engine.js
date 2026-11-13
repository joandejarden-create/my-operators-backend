/**
 * Dealality Guided Tour Engine V1 — shared coachmark walkthrough.
 * Extracted from ADP Guided Report Tour patterns for reuse (GDI, ADP, etc.).
 *
 * Reuses ADP visual contract classes: .adp-gt, .adp-howto-read-btn
 */
(function (global) {
  "use strict";

  var DEFAULTS = {
    version: "DEALALITY_GUIDED_TOUR_ENGINE_V1",
    rootId: "dealalityGuidedTourRoot",
    analyticsKey: "dealality_guided_tour_analytics_v1",
    buttonId: null,
    entrySelector: "[data-dealality-guided-tour-entry]",
    activeClass: "adp-gt-active",
    safeTop: 100,
    safeBottom: 160,
    calloutWidth: 380,
    signalLabel: "Why it matters",
    lookNextLabel: "What to do next",
    emptyMessage:
      "Load opportunities for this property first, then open How to Read This Report again.",
    steps: [],
    finishStep: null,
    isTargetVisible: null,
    captureState: null,
    restoreState: null,
    beforeStart: null,
    beforeStep: null,
    afterExit: null,
    /** When true, tour root uses Popover API to share the dialog top layer. */
    useTopLayerPopover: false,
    /**
     * Optional repair when a modal dialog still wins hit-testing over the
     * popover coachmark (e.g. demote showModal → show). Called from
     * ensureTourPointerCapture.
     */
    repairPointerCapture: null,
  };

  function createGuidedTour(userConfig) {
    var config = {};
    var key;
    for (key in DEFAULTS) {
      if (Object.prototype.hasOwnProperty.call(DEFAULTS, key)) {
        config[key] = DEFAULTS[key];
      }
    }
    userConfig = userConfig || {};
    for (key in userConfig) {
      if (Object.prototype.hasOwnProperty.call(userConfig, key)) {
        config[key] = userConfig[key];
      }
    }

    var state = {
      active: false,
      programmaticScrolling: false,
      transitioning: false,
      stepIndex: 0,
      resolvedSteps: [],
      saved: null,
      root: null,
      onKeyDown: null,
      onWheel: null,
      onTouch: null,
      transitionToken: 0,
      boundEntry: false,
      pendingDelta: 0,
      _transitionStartedAt: 0,
    };

    function $(sel, root) {
      return (root || document).querySelector(sel);
    }

    function prefersReducedMotion() {
      try {
        return (
          window.matchMedia &&
          window.matchMedia("(prefers-reduced-motion: reduce)").matches
        );
      } catch (_) {
        return false;
      }
    }

    function loadAnalytics() {
      try {
        var raw = localStorage.getItem(config.analyticsKey);
        return raw ? JSON.parse(raw) : { replayCount: 0, events: [] };
      } catch (_) {
        return { replayCount: 0, events: [] };
      }
    }

    function saveAnalytics(data) {
      try {
        localStorage.setItem(config.analyticsKey, JSON.stringify(data));
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
      if (extra && extra.lastStepReached != null) {
        data.lastStepReached = extra.lastStepReached;
      }
      data.events = (data.events || []).slice(-40);
      data.events.push({
        event: event,
        at: new Date().toISOString(),
        tourVersion: config.version,
        extra: extra || null,
      });
      saveAnalytics(data);
    }

    function warnSkip(stepId, reason) {
      try {
        console.warn("[" + config.version + "] skip step", stepId, reason);
      } catch (_) {}
    }

    function defaultIsTargetVisible(el) {
      if (!el || !el.isConnected) return false;
      if (el.hidden) return false;
      var style = window.getComputedStyle(el);
      if (style.display === "none" || style.visibility === "hidden") return false;
      var rect = el.getBoundingClientRect();
      if (
        rect.width < 2 &&
        rect.height < 2 &&
        !(el.offsetWidth || el.scrollHeight)
      ) {
        return false;
      }
      return true;
    }

    function isVisible(el) {
      var fn = config.isTargetVisible || defaultIsTargetVisible;
      return fn(el);
    }

    function isTargetResolvable(selector) {
      if (!selector) return false;
      var el = $(selector);
      return isVisible(el);
    }

    function resolveSteps() {
      var out = [];
      (config.steps || []).forEach(function (step) {
        if (step.finishCard || step.noScroll || !step.target) {
          out.push(step);
          return;
        }
        // deferTargetCheck: include now; beforeStep may reveal the target.
        if (step.deferTargetCheck || step.forceInclude) {
          out.push(step);
          return;
        }
        if (!isTargetResolvable(step.target)) {
          if (!step.optional) warnSkip(step.id, "target unavailable");
          return;
        }
        out.push(step);
      });
      if (config.finishStep && out.length) {
        var hasFinish = out.some(function (s) {
          return s.finishCard;
        });
        if (!hasFinish) out.push(config.finishStep);
      }
      return out;
    }

    function waitMs(ms) {
      return new Promise(function (resolve) {
        window.setTimeout(resolve, ms);
      });
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
      };
    }

    function isInSafeViewport(el) {
      var k = keyContentRect(el);
      var vh = window.innerHeight;
      return (
        k.top >= config.safeTop - 4 &&
        k.keyBottom <= vh - config.safeBottom + 4
      );
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
          var sig = [
            Math.round(r.top),
            Math.round(r.left),
            Math.round(r.width),
            Math.round(r.height),
          ].join(",");
          if (sig === last) stableTicks += 1;
          else {
            stableTicks = 0;
            last = sig;
          }
          if (stableTicks >= 2 || Date.now() - start > maxMs) {
            resolve(true);
            return;
          }
          window.requestAnimationFrame(tick);
        }
        window.requestAnimationFrame(tick);
      });
    }

    function findScrollContainer(el) {
      if (!el || !el.closest) return null;
      var drawerBody = el.closest(".gdi-drawer-body");
      if (drawerBody) return drawerBody;
      var dialog = el.closest("dialog[open], dialog:modal");
      if (dialog) {
        return (
          dialog.querySelector(".gdi-drawer-body") ||
          dialog.querySelector("[data-drawer-scroll]") ||
          dialog
        );
      }
      return null;
    }

    function scrollTargetIntoSafeZone(el) {
      if (!el || !el.getBoundingClientRect) {
        return Promise.resolve(false);
      }
      state.programmaticScrolling = true;
      var behavior = prefersReducedMotion() ? "auto" : "smooth";
      var nested = findScrollContainer(el);

      // Targets inside a modal drawer: scroll the drawer, not the page.
      if (nested) {
        try {
          el.scrollIntoView({
            block: "nearest",
            inline: "nearest",
            behavior: behavior,
          });
        } catch (_) {
          try {
            el.scrollIntoView(true);
          } catch (__) {}
        }
        var nestedWait = prefersReducedMotion() ? 40 : 280;
        return waitMs(nestedWait)
          .then(function () {
            return waitForStableBounds(el, 500);
          })
          .then(function () {
            state.programmaticScrolling = false;
            return true;
          })
          .catch(function () {
            state.programmaticScrolling = false;
            return false;
          });
      }

      var r = el.getBoundingClientRect();
      var vh = window.innerHeight;
      var safeH = Math.max(120, vh - config.safeTop - config.safeBottom);
      var targetY;
      if (r.height > safeH * 0.85) {
        targetY = window.scrollY + r.top - config.safeTop;
      } else {
        targetY =
          window.scrollY +
          r.top -
          config.safeTop -
          (safeH - Math.min(r.height, safeH)) / 2;
      }
      targetY = Math.max(0, Math.round(targetY));
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
            r = el.getBoundingClientRect();
            if (r.height > safeH * 0.85) {
              targetY = window.scrollY + r.top - config.safeTop;
            } else {
              targetY =
                window.scrollY +
                r.top -
                config.safeTop -
                (safeH - Math.min(r.height, safeH)) / 2;
            }
            window.scrollTo(0, Math.max(0, Math.round(targetY)));
            return waitMs(60).then(function () {
              return waitForStableBounds(el, 400);
            });
          }
          return true;
        })
        .then(function () {
          state.programmaticScrolling = false;
          return true;
        })
        .catch(function () {
          state.programmaticScrolling = false;
          return false;
        });
    }

    function supportsPopover(el) {
      return !!(
        el &&
        typeof el.showPopover === "function" &&
        typeof el.hidePopover === "function"
      );
    }

    function raiseTourTopLayer() {
      var root = state.root;
      if (!root || !config.useTopLayerPopover || !supportsPopover(root)) {
        return;
      }
      try {
        if (root.matches && root.matches(":popover-open")) {
          root.hidePopover();
        }
      } catch (_) {}
      try {
        root.showPopover();
      } catch (_) {}
    }

    /**
     * Verify the Next control is actually hittable. Modal dialogs can paint
     * beneath a visible popover yet still win elementFromPoint — repair + re-raise.
     */
    function ensureTourPointerCapture() {
      if (!state.root || !config.useTopLayerPopover) return;
      raiseTourTopLayer();
      var nextBtn = state.root.querySelector('[data-dgt="next"]');
      if (!nextBtn || typeof document.elementFromPoint !== "function") return;
      var r = nextBtn.getBoundingClientRect();
      if (r.width < 2 || r.height < 2) return;
      var hit = document.elementFromPoint(
        r.left + r.width / 2,
        r.top + r.height / 2
      );
      if (hit && state.root.contains(hit)) return;
      if (typeof config.repairPointerCapture === "function") {
        try {
          config.repairPointerCapture({ hit: hit, button: nextBtn });
        } catch (_) {}
      }
      raiseTourTopLayer();
    }

    function releaseTransitionAndFlush() {
      state.transitioning = false;
      var pending = state.pendingDelta || 0;
      state.pendingDelta = 0;
      if (pending && state.active) {
        window.setTimeout(function () {
          if (!state.active || state.transitioning) return;
          go(pending > 0 ? 1 : -1);
        }, 0);
      }
    }

    function showTourLayer() {
      var root = state.root;
      if (!root) return;
      root.removeAttribute("hidden");
      if (config.useTopLayerPopover && supportsPopover(root)) {
        try {
          root.setAttribute("popover", "manual");
          root.showPopover();
        } catch (_) {}
      }
    }

    function hideTourLayer() {
      var root = state.root;
      if (!root) return;
      if (config.useTopLayerPopover && supportsPopover(root)) {
        try {
          if (root.matches && root.matches(":popover-open")) {
            root.hidePopover();
          }
        } catch (_) {}
      }
      root.setAttribute("hidden", "");
    }

    function buildDom() {
      if (state.root) return state.root;
      var root = document.createElement("div");
      root.id = config.rootId;
      root.className = "adp-gt";
      root.setAttribute("data-dealality-guided-tour", "1");
      root.setAttribute("hidden", "");
      if (config.useTopLayerPopover) {
        root.setAttribute("popover", "manual");
      }
      root.innerHTML =
        '<div class="adp-gt__highlight" data-dgt="highlight" aria-hidden="true"></div>' +
        '<div class="adp-gt__callout" role="dialog" aria-modal="true" aria-labelledby="dgtTitle" aria-describedby="dgtBody" hidden>' +
        '  <button type="button" class="adp-gt__close" data-dgt="exit" aria-label="Exit walkthrough">×</button>' +
        '  <div class="adp-gt__progress-wrap">' +
        '    <p class="adp-gt__progress" id="dgtProgress"></p>' +
        '    <div class="adp-gt__progress-bar" aria-hidden="true"><span class="adp-gt__progress-fill" id="dgtProgressFill"></span></div>' +
        "  </div>" +
        '  <h2 class="adp-gt__title" id="dgtTitle"></h2>' +
        '  <p class="adp-gt__body" id="dgtBody"></p>' +
        '  <ul class="adp-gt__finish-list" id="dgtFinishList" hidden></ul>' +
        '  <div class="adp-gt__row" id="dgtSignalRow">' +
        '    <p class="adp-gt__row-label" id="dgtSignalLabel"></p>' +
        '    <p class="adp-gt__row-text" id="dgtSignal"></p>' +
        "  </div>" +
        '  <div class="adp-gt__row" id="dgtLookRow">' +
        '    <p class="adp-gt__row-label" id="dgtLookLabel"></p>' +
        '    <p class="adp-gt__row-text" id="dgtLook"></p>' +
        "  </div>" +
        '  <p class="adp-gt__note" id="dgtNote" hidden></p>' +
        '  <div class="adp-gt__actions">' +
        '    <button type="button" class="adp-gt__btn adp-gt__btn--quiet" data-dgt="exit">Exit</button>' +
        '    <button type="button" class="adp-gt__btn adp-gt__btn--secondary" data-dgt="restart" hidden>Restart</button>' +
        '    <button type="button" class="adp-gt__btn adp-gt__btn--secondary" data-dgt="back">Back</button>' +
        '    <button type="button" class="adp-gt__btn adp-gt__btn--primary" data-dgt="next">Next</button>' +
        "  </div>" +
        "</div>";
    document.body.appendChild(root);

    function onTourAction(action) {
      if (action === "exit") exitTour("exit");
      else if (action === "back") go(-1);
      else if (action === "next") go(1);
      else if (action === "restart") {
        state.stepIndex = 0;
        renderStep();
      }
    }

    root.addEventListener("click", function (e) {
      var t = e.target.closest("[data-dgt]");
      if (!t) return;
      onTourAction(t.getAttribute("data-dgt"));
    });

    // Direct handlers — more reliable than bubbling alone under the overlay.
    root.querySelectorAll("[data-dgt]").forEach(function (el) {
      var action = el.getAttribute("data-dgt");
      if (!action || action === "highlight") return;
      el.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        onTourAction(action);
      });
    });

    state.root = root;
    return root;
  }

    function muteCallout() {
      if (!state.root) return;
      var callout = state.root.querySelector(".adp-gt__callout");
      var highlight = state.root.querySelector('[data-dgt="highlight"]');
      if (callout) {
        // Keep the callout in-layout (do not set [hidden]) so Next/Back retain
        // hit targets during async drawer open / scroll transitions.
        callout.classList.add("adp-gt__callout--muted");
        callout.removeAttribute("hidden");
      }
      if (highlight) highlight.classList.add("adp-gt__highlight--muted");
    }

    function showCallout() {
      if (!state.root) return;
      var callout = state.root.querySelector(".adp-gt__callout");
      var highlight = state.root.querySelector('[data-dgt="highlight"]');
      if (callout) {
        callout.removeAttribute("hidden");
        callout.classList.remove("adp-gt__callout--muted");
      }
      if (highlight) highlight.classList.remove("adp-gt__highlight--muted");
    }

    function placeHighlight(el) {
      var highlight = state.root.querySelector('[data-dgt="highlight"]');
      if (!highlight || !el) return null;
      var pad = 8;
      var r = el.getBoundingClientRect();
      // Opportunity Details drawers are viewport-tall. The default 40vh cap left
      // a small empty cutout while the rest of the page (and most of the drawer)
      // sat under the 9999px scrim — reading as a blank dark overlay.
      var isDrawerTarget = false;
      try {
        isDrawerTarget = !!(
          el.matches &&
          (el.matches("dialog.gdi-drawer") ||
            el.matches('[data-gdi-tour-target="drawer"]') ||
            el.classList.contains("gdi-drawer"))
        );
      } catch (_) {
        isDrawerTarget = false;
      }
      var maxH = Math.floor(
        window.innerHeight * (isDrawerTarget ? 0.94 : 0.4)
      );
      var hlH = Math.min(maxH, Math.max(44, r.height + pad * 2));
      var hlW = Math.min(
        window.innerWidth - 16,
        Math.max(44, r.width + pad * 2)
      );
      var hlTop = Math.max(
        8,
        Math.min(r.top - pad, window.innerHeight - hlH - 8)
      );
      var hlLeft = Math.max(
        8,
        Math.min(r.left - pad, window.innerWidth - hlW - 8)
      );
      if (isDrawerTarget) {
        // Pin to the visible right-rail drawer, not a mid-page fragment.
        hlTop = Math.max(0, Math.min(8, r.top));
        hlLeft = Math.max(0, Math.round(r.left));
        hlW = Math.min(window.innerWidth, Math.round(r.width));
        hlH = Math.min(window.innerHeight, Math.round(r.height));
      }
      highlight.style.top = hlTop + "px";
      highlight.style.left = hlLeft + "px";
      highlight.style.width = hlW + "px";
      highlight.style.height = hlH + "px";
      return { top: hlTop, left: hlLeft, width: hlW, height: hlH };
    }

    function placeCallout(hl) {
      var callout = state.root.querySelector(".adp-gt__callout");
      if (!callout || !hl) return false;
      var gap = 14;
      var vw = window.innerWidth;
      var vh = window.innerHeight;
      var w = Math.min(config.calloutWidth, vw - 24);
      callout.style.width = w + "px";
      callout.style.maxHeight = Math.floor(vh * 0.55) + "px";
      callout.removeAttribute("hidden");
      var h = Math.min(callout.scrollHeight || 280, Math.floor(vh * 0.55));
      // Prefer the open side opposite a right-hand drawer highlight so the
      // coachmark stays fully inside the viewport and remains hittable.
      var preferLeft = hl.left + hl.width / 2 > vw * 0.55;
      var isDrawerHighlight =
        preferLeft && hl.width >= Math.min(420, vw * 0.32) && hl.height >= vh * 0.55;
      // Tall right-rail drawer: keep the coachmark entirely in the left column —
      // never straddle the drawer content the walkthrough is explaining.
      if (isDrawerHighlight) {
        var drawerGap = Math.max(gap, 28);
        var maxW = Math.max(240, hl.left - drawerGap - 12);
        if (w > maxW) {
          w = maxW;
          callout.style.width = w + "px";
          // Re-measure after width shrink (height may change).
          var measured = callout.getBoundingClientRect();
          h = measured.height || h;
        }
        var leftCol = Math.max(12, hl.left - w - drawerGap);
        var topCol = Math.max(12, Math.min(hl.top + 24, vh - h - 12));
        callout.style.top = Math.round(topCol) + "px";
        callout.style.left = Math.round(leftCol) + "px";
        return true;
      }
      var candidates = preferLeft
        ? [
            { name: "left", top: hl.top, left: hl.left - w - gap },
            { name: "right", top: hl.top, left: hl.left + hl.width + gap },
            {
              name: "bottom",
              top: hl.top + hl.height + gap,
              left: Math.max(12, Math.min(hl.left, vw - w - 12)),
            },
            {
              name: "top",
              top: hl.top - h - gap,
              left: Math.max(12, Math.min(hl.left, vw - w - 12)),
            },
          ]
        : [
            { name: "right", top: hl.top, left: hl.left + hl.width + gap },
            { name: "left", top: hl.top, left: hl.left - w - gap },
            {
              name: "bottom",
              top: hl.top + hl.height + gap,
              left: hl.left + hl.width / 2 - w / 2,
            },
            {
              name: "top",
              top: hl.top - h - gap,
              left: hl.left + hl.width / 2 - w / 2,
            },
          ];
      function fits(c) {
        return (
          c.left >= 12 &&
          c.left + w <= vw - 12 &&
          c.top >= 12 &&
          c.top + h <= vh - 12
        );
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
      // Mobile / short viewports: keep Next/Back inside the visual viewport.
      try {
        var nextBtn = callout.querySelector('[data-dgt="next"]');
        if (nextBtn) {
          var nbr = nextBtn.getBoundingClientRect();
          if (nbr.bottom > vh - 4 || nbr.top < 4 || nbr.width < 2) {
            var compactH = Math.min(
              callout.scrollHeight || h,
              Math.floor(vh * 0.72)
            );
            callout.style.maxHeight = compactH + "px";
            callout.style.top =
              Math.max(8, Math.min(Math.round((vh - compactH) / 2), vh - compactH - 8)) +
              "px";
            callout.style.left = Math.max(8, Math.round((vw - w) / 2)) + "px";
          }
        }
      } catch (_) {}
      return true;
    }

    function clearHighlight() {
      var highlight =
        state.root && state.root.querySelector('[data-dgt="highlight"]');
      if (!highlight) return;
      highlight.style.top = "0px";
      highlight.style.left = "0px";
      highlight.style.width = "0px";
      highlight.style.height = "0px";
      highlight.classList.add("adp-gt__highlight--muted");
    }

    function placeFinishCallout() {
      var callout = state.root.querySelector(".adp-gt__callout");
      if (!callout) return false;
      clearHighlight();
      var vw = window.innerWidth;
      var vh = window.innerHeight;
      var w = Math.min(config.calloutWidth, vw - 24);
      callout.style.width = w + "px";
      callout.removeAttribute("hidden");
      var h = Math.min(callout.scrollHeight || 320, Math.floor(vh * 0.7));
      var top = Math.max(16, Math.min(Math.round((vh - h) / 2), vh - h - 16));
      var left = Math.max(12, Math.round((vw - w) / 2));
      callout.style.top = top + "px";
      callout.style.left = left + "px";
      return true;
    }

    function fillStepContent(step, index, total) {
      var progress = state.root.querySelector("#dgtProgress");
      var fill = state.root.querySelector("#dgtProgressFill");
      var title = state.root.querySelector("#dgtTitle");
      var body = state.root.querySelector("#dgtBody");
      var finishList = state.root.querySelector("#dgtFinishList");
      var signalRow = state.root.querySelector("#dgtSignalRow");
      var lookRow = state.root.querySelector("#dgtLookRow");
      var signalLabel = state.root.querySelector("#dgtSignalLabel");
      var lookLabel = state.root.querySelector("#dgtLookLabel");
      var signal = state.root.querySelector("#dgtSignal");
      var look = state.root.querySelector("#dgtLook");
      var note = state.root.querySelector("#dgtNote");
      var nextBtn = state.root.querySelector('[data-dgt="next"]');
      var backBtn = state.root.querySelector('[data-dgt="back"]');
      var restartBtn = state.root.querySelector('[data-dgt="restart"]');
      var isLast = index >= total - 1;
      var isFinish = !!step.finishCard;

      if (progress) progress.textContent = index + 1 + " of " + total;
      if (fill) fill.style.width = Math.round(((index + 1) / total) * 100) + "%";
      if (title) title.textContent = step.title;
      if (signalLabel) {
        signalLabel.textContent =
          step.signalLabel || config.signalLabel || "Why it matters";
      }
      if (lookLabel) {
        lookLabel.textContent =
          step.lookNextLabel || config.lookNextLabel || "What to do next";
      }

      if (isFinish) {
        if (body) {
          body.textContent = step.guide || "";
          body.hidden = !step.guide;
        }
        if (finishList) {
          finishList.hidden = !(step.bullets && step.bullets.length);
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
          // beforeStep may reveal deferred targets (e.g. open a modal drawer).
          if (candidate.deferTargetCheck || candidate.forceInclude || candidate._openDrawer) {
            state.stepIndex = i;
            return candidate;
          }
          var el = $(candidate.target);
          if (el && isVisible(el)) {
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

      function paint() {
        try {
          fillStepContent(step, state.stepIndex, steps.length);

          if (step.finishCard || step.noScroll || !step.target) {
            raiseTourTopLayer();
            placeFinishCallout();
            showCallout();
            clearHighlight();
            track("stepViewed", {
              lastStepReached: step.id,
              index: state.stepIndex,
              total: steps.length,
            });
            var finishNext = state.root.querySelector('[data-dgt="next"]');
            if (finishNext) finishNext.focus();
            ensureTourPointerCapture();
            releaseTransitionAndFlush();
            return;
          }

          var el = $(step.target);
          if (!el || !isVisible(el)) {
            warnSkip(step.id, "target missing at paint");
            if (state.stepIndex < steps.length - 1) {
              state.stepIndex += 1;
              renderStep();
            } else {
              releaseTransitionAndFlush();
              exitTour("complete");
            }
            return;
          }
          scrollTargetIntoSafeZone(el)
            .then(function (ok) {
              if (token !== state.transitionToken || !state.active) {
                releaseTransitionAndFlush();
                return;
              }
              // Re-query — DOM may refresh during async scroll/open.
              el = $(step.target) || el;
              if (!ok || !isVisible(el)) {
                warnSkip(step.id, "could not bring target into safe viewport");
                if (state.stepIndex < steps.length - 1) {
                  state.stepIndex += 1;
                  renderStep();
                } else {
                  releaseTransitionAndFlush();
                  exitTour("complete");
                }
                return;
              }
              raiseTourTopLayer();
              var hl = placeHighlight(el);
              placeCallout(hl);
              showCallout();
              ensureTourPointerCapture();
              track("stepViewed", {
                lastStepReached: step.id,
                index: state.stepIndex,
                total: steps.length,
              });
              var nextBtn = state.root.querySelector('[data-dgt="next"]');
              if (nextBtn) nextBtn.focus();
              releaseTransitionAndFlush();
            })
            .catch(function () {
              releaseTransitionAndFlush();
            });
        } catch (_) {
          releaseTransitionAndFlush();
        }
      }

      if (typeof config.beforeStep === "function") {
        state.transitioning = true;
        state._transitionStartedAt = Date.now();
        Promise.resolve(config.beforeStep(step, api))
          .then(function () {
            if (token !== state.transitionToken || !state.active) {
              releaseTransitionAndFlush();
              return;
            }
            raiseTourTopLayer();
            ensureTourPointerCapture();
            // Re-resolve target after beforeStep side effects (drawer open, etc.)
            if (step.target && !isTargetResolvable(step.target) && !step.optional) {
              warnSkip(step.id, "target unavailable after beforeStep");
              if (state.stepIndex < steps.length - 1) {
                state.stepIndex += 1;
                renderStep();
              } else {
                releaseTransitionAndFlush();
                exitTour("complete");
              }
              return;
            }
            if (step.target && !isTargetResolvable(step.target) && step.optional) {
              warnSkip(step.id, "optional target unavailable after beforeStep");
              if (state.stepIndex < steps.length - 1) {
                state.stepIndex += 1;
                renderStep();
              } else {
                releaseTransitionAndFlush();
                exitTour("complete");
              }
              return;
            }
            paint();
          })
          .catch(function () {
            raiseTourTopLayer();
            ensureTourPointerCapture();
            paint();
          });
      } else {
        state.transitioning = true;
        state._transitionStartedAt = Date.now();
        paint();
      }
    }

    function go(delta) {
      if (!state.active) return;
      if (state.transitioning) {
        // Queue one step instead of dropping the click while drawer/scroll settles.
        if (!state._transitionStartedAt) state._transitionStartedAt = Date.now();
        if (Date.now() - state._transitionStartedAt < 12000) {
          state.pendingDelta = delta > 0 ? 1 : -1;
          return;
        }
        state.transitioning = false;
        state.pendingDelta = 0;
      }
      state._transitionStartedAt = Date.now();
      state.pendingDelta = 0;
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
      var t = e.target;
      if (t && t.closest) {
        // Always allow scrolling the coachmark itself.
        if (t.closest(".adp-gt__callout")) return;
        // Drawer-body scroll while highlighting drawer content.
        var step = state.resolvedSteps[state.stepIndex];
        if (
          step &&
          (step.allowDrawerScroll || step.scrollInDrawer) &&
          t.closest(".gdi-drawer-body, #gdiDrawerBody, dialog.gdi-drawer")
        ) {
          return;
        }
      }
      e.preventDefault();
    }

    function startTour() {
      var startPromise = Promise.resolve();
      if (typeof config.beforeStart === "function") {
        startPromise = Promise.resolve(config.beforeStart(api));
      }
      return startPromise.then(function () {
        var resolved = resolveSteps();
        if (!resolved.length) {
          window.alert(config.emptyMessage);
          return;
        }

        state.saved =
          typeof config.captureState === "function"
            ? config.captureState()
            : {
                scrollX: window.scrollX || 0,
                scrollY: window.scrollY || 0,
              };
        state.resolvedSteps = resolved;
        state.stepIndex = 0;
        state.active = true;

        var root = buildDom();
        showTourLayer();
        document.documentElement.classList.add(config.activeClass);
        document.body.classList.add(config.activeClass);

        state.onKeyDown = onKeyDown;
        state.onWheel = blockUserScroll;
        state.onTouch = blockUserScroll;
        document.addEventListener("keydown", state.onKeyDown, true);
        document.addEventListener("wheel", state.onWheel, {
          passive: false,
          capture: true,
        });
        document.addEventListener("touchmove", state.onTouch, {
          passive: false,
          capture: true,
        });

        track("tourStarted", { stepCount: resolved.length });
        renderStep();
      });
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
      document.documentElement.classList.remove(config.activeClass);
      document.body.classList.remove(config.activeClass);
      if (state.root) {
        muteCallout();
        hideTourLayer();
      }

      if (typeof config.restoreState === "function") {
        config.restoreState(state.saved, {
          restoreScroll: reason !== "complete",
        });
      } else if (state.saved && reason !== "complete") {
        if (
          typeof state.saved.scrollX === "number" &&
          typeof state.saved.scrollY === "number"
        ) {
          window.scrollTo(state.saved.scrollX, state.saved.scrollY);
        }
      }
      state.saved = null;

      if (typeof config.afterExit === "function") {
        try {
          config.afterExit(reason);
        } catch (_) {}
      }

      var btn =
        (config.buttonId && document.getElementById(config.buttonId)) ||
        $(config.entrySelector);
      if (btn && typeof btn.focus === "function") {
        try {
          btn.focus();
        } catch (_) {}
      }
    }

    function bindEntry() {
      if (state.boundEntry) return;
      state.boundEntry = true;
      document.addEventListener(
        "click",
        function (e) {
          var btn = e.target.closest
            ? e.target.closest(config.entrySelector)
            : null;
          if (!btn) return;
          if (config.buttonId && btn.id && btn.id !== config.buttonId) {
            // still allow data-attribute entry
          }
          e.preventDefault();
          startTour();
        },
        false
      );
    }

    var api = {
      version: config.version,
      config: config,
      start: startTour,
      exit: function () {
        exitTour("exit");
      },
      resolveSteps: resolveSteps,
      isActive: function () {
        return state.active;
      },
      raiseTopLayer: raiseTourTopLayer,
      bindEntry: bindEntry,
      init: function () {
        bindEntry();
      },
    };

    return api;
  }

  global.DealalityGuidedTourEngine = {
    create: createGuidedTour,
    version: DEFAULTS.version,
  };
})(typeof window !== "undefined" ? window : globalThis);
