/**
 * GDI Guided Report Tour — "How to Read This Report"
 * Opportunities-page walkthrough (top → bottom). Opens Opportunity Details
 * drawer for evidence + feedback steps. Never switches to Commercial Priorities.
 * Reuses DealalityGuidedTourEngine + ADP coachmark CSS (.adp-gt / .adp-howto-read-btn).
 */
(function (global) {
  "use strict";

  var TOUR_VERSION = "GDI_GUIDED_REPORT_TOUR_V1_1e";
  var BUTTON_ID = "gdiHowToReadReport";

  var drawerSession = {
    openedByTour: false,
    wasOpenBeforeTour: false,
    priorOpportunityId: null,
    demoOpportunityId: null,
    cancelGuard: null,
    demotedFromModal: false,
    showModalPatch: null,
    demotePoll: null,
  };

  function waitMs(ms) {
    return new Promise(function (resolve) {
      window.setTimeout(resolve, ms);
    });
  }

  function drawerEl() {
    return (
      document.getElementById("gdiDrawer") ||
      document.getElementById("gdiShareDrawer") ||
      document.querySelector("dialog.gdi-drawer[data-gdi-tour-target='drawer']") ||
      document.querySelector("dialog.gdi-drawer")
    );
  }

  function drawerIsOpen() {
    var d = drawerEl();
    if (!d) return false;
    if (typeof d.open === "boolean") return d.open;
    return d.hasAttribute("open");
  }

  function closeDrawerNative() {
    var d = drawerEl();
    if (!d || !drawerIsOpen()) return;
    try {
      if (typeof d.close === "function") d.close();
      else d.removeAttribute("open");
    } catch (_) {}
    drawerSession.demotedFromModal = false;
  }

  /**
   * Modal <dialog showModal()> wins the browser top layer for hit-testing over
   * popover="manual" coachmarks — Next/Back appear clickable but clicks land on
   * the drawer. Demote the tour-owned drawer to non-modal show() so the popover
   * receives pointer events while remaining visually open.
   */
  function demoteModalDrawerForTour() {
    var d = drawerEl();
    if (!d || !drawerIsOpen()) return false;
    var isModal = false;
    try {
      isModal = !!(d.matches && d.matches(":modal"));
    } catch (_) {
      isModal = false;
    }
    if (!isModal) return false;
    // Suppress close-handler focus thrash while flipping modal → non-modal.
    var suppressClose = function (e) {
      try {
        e.stopImmediatePropagation();
      } catch (_) {}
    };
    d.addEventListener("close", suppressClose, true);
    try {
      d.close();
      if (typeof d.show === "function") d.show();
      else d.setAttribute("open", "");
      drawerSession.demotedFromModal = true;
      return true;
    } catch (_) {
      return false;
    } finally {
      d.removeEventListener("close", suppressClose, true);
    }
  }

  function patchDrawerShowModalForTour() {
    var d = drawerEl();
    if (!d || typeof d.showModal !== "function") return;
    if (drawerSession.showModalPatch) return;
    var original = d.showModal.bind(d);
    drawerSession.showModalPatch = original;
    d.showModal = function () {
      // While the tour owns the drawer, prefer non-modal show() so popover
      // coachmarks remain hittable in the browser top layer.
      if (tour && tour.isActive()) {
        try {
          if (typeof d.show === "function") d.show();
          else d.setAttribute("open", "");
          drawerSession.demotedFromModal = true;
          return;
        } catch (_) {}
      }
      return original();
    };
  }

  function restoreDrawerShowModal() {
    var d = drawerEl();
    if (d && drawerSession.showModalPatch) {
      d.showModal = drawerSession.showModalPatch;
    }
    drawerSession.showModalPatch = null;
    if (drawerSession.demotePoll) {
      try {
        clearInterval(drawerSession.demotePoll);
      } catch (_) {}
    }
    drawerSession.demotePoll = null;
  }

  function ensureTourDrawerPointerCapture(api) {
    patchDrawerShowModalForTour();
    demoteModalDrawerForTour();
    if (!drawerSession.demotePoll) {
      var tries = 0;
      drawerSession.demotePoll = setInterval(function () {
        tries += 1;
        demoteModalDrawerForTour();
        if (tries >= 12 || !(tour && tour.isActive())) {
          clearInterval(drawerSession.demotePoll);
          drawerSession.demotePoll = null;
        }
      }, 60);
    }
    if (api && typeof api.raiseTopLayer === "function") {
      api.raiseTopLayer();
    }
  }

  function ensureOpportunitiesTab() {
    var active = document.querySelector(
      '.section-nav-item[data-tab="opportunities"][aria-selected="true"]'
    );
    if (active) return Promise.resolve();
    var tab = document.querySelector(
      '.section-nav-item[data-tab="opportunities"]'
    );
    if (tab) {
      try {
        tab.click();
      } catch (_) {}
      return waitMs(220);
    }
    return Promise.resolve();
  }

  function drawerBodyEl() {
    return (
      document.getElementById("gdiDrawerBody") ||
      document.getElementById("gdiShareDrawerBody") ||
      (drawerEl() && drawerEl().querySelector(".gdi-drawer-body"))
    );
  }

  function currentOpenOpportunityId() {
    var d = drawerEl();
    if (!d || !drawerIsOpen()) return null;
    var marked = d.getAttribute("data-gdi-open-opportunity-id");
    if (marked) return marked;
    var body = drawerBodyEl();
    var fromBody =
      body &&
      body.querySelector("[data-id], [data-opportunity-id], [data-open]");
    if (fromBody) {
      return (
        fromBody.getAttribute("data-opportunity-id") ||
        fromBody.getAttribute("data-id") ||
        fromBody.getAttribute("data-open")
      );
    }
    return null;
  }

  function selectDemoCard() {
    var cards = document.querySelectorAll(
      'article.brand-card[data-open][data-gdi-tour-target="opportunity-card"]'
    );
    if (!cards.length) {
      cards = document.querySelectorAll("article.brand-card[data-open]");
    }
    var ranked = [];
    for (var i = 0; i < cards.length; i++) {
      var card = cards[i];
      var id = card.getAttribute("data-open");
      if (!id) continue;
      var score = 0;
      var contact = card.querySelector(
        ".brand-card__contact-name:not(.brand-card__contact-name--unidentified)"
      );
      var contactText = contact
        ? String(contact.textContent || "")
            .replace(/\s+/g, " ")
            .trim()
        : "";
      if (
        contactText &&
        contactText !== "Contact Research Required" &&
        contactText !== "Buyer Not Yet Identified"
      ) {
        score += 40;
      }
      if (
        card.querySelector(
          '.gdi-revenue-stream-icon--active[data-revenue-stream="guestrooms"], .gdi-revenue-stream-icon--active[data-revenue-stream="eventFnb"]'
        )
      ) {
        score += 20;
      }
      if (card.querySelector('[data-gdi-tour-target="view-details"]')) {
        score += 10;
      }
      var name = card.querySelector(".brand-card__name");
      var titleLen = name
        ? String(name.textContent || "").replace(/\s+/g, " ").trim().length
        : 0;
      if (titleLen >= 12) score += 8;
      if (card.querySelector(".brand-card__description")) score += 6;
      ranked.push({ card: card, id: id, score: score });
    }
    ranked.sort(function (a, b) {
      return b.score - a.score;
    });
    return ranked.length ? ranked[0] : null;
  }

  function openDemoDrawer() {
    var pick = selectDemoCard();
    if (!pick) return Promise.resolve(false);
    drawerSession.demoOpportunityId = pick.id;
    var btn =
      pick.card.querySelector('[data-gdi-tour-target="view-details"]') ||
      pick.card.querySelector("[data-open]");
    if (!btn) return Promise.resolve(false);
    try {
      btn.click();
    } catch (_) {
      return Promise.resolve(false);
    }
    drawerSession.openedByTour = true;
    var d = drawerEl();
    if (d) d.setAttribute("data-gdi-open-opportunity-id", pick.id);
    return waitForDrawerContent(14000).then(function (ok) {
      if (!ok) drawerSession.openedByTour = false;
      else demoteModalDrawerForTour();
      return ok;
    });
  }

  function waitForDrawerContent(maxMs) {
    maxMs = maxMs || 8000;
    var start = Date.now();
    return new Promise(function (resolve) {
      function tick() {
        var d = drawerEl();
        var body = drawerBodyEl();
        var ready =
          d &&
          drawerIsOpen() &&
          body &&
          (body.querySelector("[data-section-id]") ||
            body.querySelector(".gdi-section") ||
            body.querySelector(".gdi-detail-framework") ||
            body.querySelector("[data-gdi-tour-target='drawer-evidence']") ||
            body.querySelector("[data-gdi-tour-target='drawer-validation']")) &&
          !/Loading|Unable to load/i.test(body.textContent || "");
        if (ready) {
          resolve(true);
          return;
        }
        if (Date.now() - start > maxMs) {
          resolve(!!(d && drawerIsOpen()));
          return;
        }
        window.setTimeout(tick, 80);
      }
      tick();
    });
  }

  function installDrawerCancelGuard() {
    var d = drawerEl();
    if (!d || drawerSession.cancelGuard) return;
    drawerSession.cancelGuard = function (e) {
      // Tour owns Escape via capture listener — keep drawer open until tour closes it.
      if (tour && tour.isActive() && drawerSession.openedByTour) {
        e.preventDefault();
      }
    };
    d.addEventListener("cancel", drawerSession.cancelGuard);
  }

  function removeDrawerCancelGuard() {
    var d = drawerEl();
    if (d && drawerSession.cancelGuard) {
      d.removeEventListener("cancel", drawerSession.cancelGuard);
    }
    drawerSession.cancelGuard = null;
  }

  function restorePriorDrawer() {
    if (!drawerSession.wasOpenBeforeTour || !drawerSession.priorOpportunityId) {
      return Promise.resolve();
    }
    var priorId = drawerSession.priorOpportunityId;
    var btn = document.querySelector(
      'button[data-gdi-tour-target="view-details"][data-open="' +
        priorId +
        '"], article[data-open="' +
        priorId +
        '"]'
    );
    if (!btn) return Promise.resolve();
    try {
      btn.click();
    } catch (_) {}
    return waitMs(400);
  }

  var STEP_DEFS = [
    {
      id: "opportunities-overview",
      target: '[data-gdi-tour-target="opportunity-list"]',
      title: "Start with the Opportunities overview",
      guide:
        "This page is the hotel’s working view of Group & Demand Intelligence — researched accounts, events, and commercial demand worth reviewing for sales pursuit.",
      signal:
        "Use it as a living pipeline of potential business, not a static directory of every hotel in the market.",
      lookNext:
        "Begin with the filters above the cards so the list matches how you want to work today.",
    },
    {
      id: "top-filters",
      target: '[data-gdi-tour-target="filters"]',
      title: "Select your hotel and filters",
      guide:
        "Property selects the hotel. Weekly Change surfaces what is new or updated in the research inventory. Opportunity Type filters by kind of demand (for example associations, scaling accounts, or local catering).",
      signal:
        "Set these first so you are not scanning the wrong hotel or an outdated mix of demand types.",
      lookNext: "Next, set Market Scope to choose All, Hotel, or Competitive.",
    },
    {
      id: "market-scope",
      target: '[data-gdi-tour-target="market-scope"]',
      title: "Understand Market Scope",
      guide:
        "All shows every eligible record in the current view. Hotel focuses on hotel-specific pursuits for this property. Competitive includes competitor intelligence that is useful to monitor but is not automatically available business for this hotel.",
      signal:
        "Competitive is market intelligence — not a demand segment and not a booking guarantee.",
      lookNext:
        "Use Hotel for pursuit lists; use Competitive for overflow, future cycles, and competitive positioning.",
      note: "A record appears in both Hotel and Competitive only when both theses are independently supported.",
    },
    {
      id: "opportunity-cards",
      target: '[data-gdi-tour-target="opportunity-card"]',
      title: "Read the opportunity cards",
      guide:
        "Each card shows the commercial title, priority, recommended action, timing cues, a short demand summary, contact status, and Rooms / Local Catering icons.",
      signal:
        "Highlighted bed = guestroom demand. Highlighted fork and knife = local catering / meetings. Both can apply when evidence supports each stream.",
      lookNext:
        "Scan contact status and icons before opening details — they show readiness at a glance.",
    },
    {
      id: "opportunity-readiness",
      target: '[data-gdi-tour-target="card-signals"]',
      title: "Understand opportunity readiness",
      guide:
        "Pills and contact status show how ready an opportunity is: stronger pursue-now signals, developing accounts, competitive intelligence, or records that still need contact research.",
      signal:
        "“Contact Research Required” means keep the opportunity, but do not treat qualification as complete.",
      lookNext:
        "Open View Details when the headline, icons, and contact status justify a closer look.",
    },
    {
      id: "detail-drawer",
      target: '[data-gdi-tour-target="drawer"]',
      title: "Open Opportunity Details",
      guide:
        "The detail drawer holds the complete commercial reasoning: what is happening, potential business, why this hotel, why act now, facts and uncertainties, who to contact, and supporting evidence.",
      signal:
        "Rely on the drawer before deciding whether an opportunity is worth pursuing.",
      lookNext:
        "Scroll the drawer to review evidence and the recommended next step.",
      deferTargetCheck: true,
      forceInclude: true,
      optional: true,
      allowDrawerScroll: true,
      scrollInDrawer: true,
      _openDrawer: true,
    },
    {
      id: "drawer-evidence",
      target: '[data-gdi-tour-target="drawer-evidence"]',
      title: "Evidence and recommended action",
      guide:
        "Inside the drawer, separate confirmed facts from inferred demand, note uncertainties or competitor bookings, and read the recommended next step for the sales team.",
      signal:
        "Inferred demand is useful intelligence — it is not the same as a confirmed booking or open RFP.",
      lookNext:
        "Continue to Hotel Validation / Action / Outcome to record what your team decides.",
      deferTargetCheck: true,
      optional: true,
      allowDrawerScroll: true,
      scrollInDrawer: true,
      _requireDrawer: true,
    },
    {
      id: "hotel-validation",
      target: '[data-gdi-tour-target="drawer-validation"]',
      title: "Hotel Validation / Action / Outcome",
      guide:
        "Use these controls to record whether an opportunity is useful, already known, contacted, worth pursuing, or not a fit — without inventing new research in this walkthrough.",
      signal:
        "Sales feedback improves future GDI quality for the hotel. This demo does not save changes.",
      lookNext:
        "When you finish reviewing, close the drawer and return to the list workflow.",
      deferTargetCheck: true,
      optional: true,
      allowDrawerScroll: true,
      scrollInDrawer: true,
      _requireDrawer: true,
    },
    {
      id: "export",
      target: '[data-gdi-tour-target="export"]',
      title: "Export and continue working the list",
      guide:
        "Export CSV downloads the currently visible opportunity set with the fields your team needs for working review.",
      signal:
        "What you see and export depends on authorized inventory and the filters you applied.",
      lookNext:
        "Export after Market Scope and priority filters match your working list.",
      optional: true,
      _closeDrawer: true,
    },
  ];

  var FINISH_STEP = {
    id: "how-to-use",
    target: null,
    title: "How to use GDI",
    finishCard: true,
    noScroll: true,
    guide: "A practical reading order for sales leaders:",
    bullets: [
      "Review priority opportunities with filters and Market Scope set.",
      "Open details and verify the commercial reasoning before outreach.",
      "Identify the appropriate contact and next action.",
      "Distinguish active hotel pursuits from competitor intelligence.",
      "Record sales feedback and outcomes so research improves over time.",
    ],
    note: "Group & Demand Intelligence supports hotel sales judgment — it does not replace it.",
    _closeDrawer: true,
  };

  function isGdiTargetVisible(el) {
    if (!el || !el.isConnected) return false;
    if (el.hidden) return false;
    var style = window.getComputedStyle(el);
    if (style.display === "none" || style.visibility === "hidden") return false;
    var root =
      document.getElementById("gdiRoot") ||
      document.getElementById("gdiShareRoot");
    if (root && root.contains(el)) {
      var panel = el.closest(".gdi-tab-panel");
      if (panel && panel.hidden) return false;
    }
    // Drawer targets are valid when the dialog is open (including top layer).
    if (el.closest && el.closest("dialog.gdi-drawer")) {
      return drawerIsOpen();
    }
    if (el.id === "gdiDrawer" || el.getAttribute("data-gdi-tour-target") === "drawer") {
      return drawerIsOpen();
    }
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

  function buildTour() {
    if (!global.DealalityGuidedTourEngine) {
      console.warn("[GDI Guided Tour] DealalityGuidedTourEngine missing");
      return null;
    }

    return global.DealalityGuidedTourEngine.create({
      version: TOUR_VERSION,
      rootId: "gdiGuidedTourRoot",
      analyticsKey: "gdi_guided_tour_analytics_v1",
      buttonId: BUTTON_ID,
      entrySelector:
        "#gdiHowToReadReport, [data-gdi-guided-tour-entry], [data-dealality-guided-tour-entry]",
      signalLabel: "Why it matters",
      lookNextLabel: "What to do next",
      emptyMessage:
        "Load opportunities for this property first, then click How to Read This Report again.",
      useTopLayerPopover: true,
      steps: STEP_DEFS,
      finishStep: FINISH_STEP,
      isTargetVisible: isGdiTargetVisible,
      /**
       * Engine calls this when Next/Back coordinates hit a foreign top-layer
       * (modal dialog) instead of the coachmark — demote drawer and re-raise.
       */
      repairPointerCapture: function () {
        demoteModalDrawerForTour();
      },
      beforeStart: function () {
        return ensureOpportunitiesTab().then(function () {
          drawerSession.wasOpenBeforeTour = drawerIsOpen();
          drawerSession.priorOpportunityId = currentOpenOpportunityId();
          drawerSession.openedByTour = false;
          drawerSession.demoOpportunityId = null;
          drawerSession.demotedFromModal = false;
          patchDrawerShowModalForTour();
          installDrawerCancelGuard();
          // Start from a closed drawer so the walkthrough opens one deliberately.
          if (drawerSession.wasOpenBeforeTour) {
            closeDrawerNative();
            return waitMs(180);
          }
          return null;
        });
      },
      beforeStep: function (step, api) {
        if (!step) return null;

        var pageStepIds = {
          "opportunities-overview": 1,
          "top-filters": 1,
          "market-scope": 1,
          "opportunity-cards": 1,
          "opportunity-readiness": 1,
          export: 1,
          "how-to-use": 1,
        };

        if (
          (step._closeDrawer || pageStepIds[step.id]) &&
          drawerIsOpen() &&
          (drawerSession.openedByTour || step._closeDrawer)
        ) {
          closeDrawerNative();
          if (drawerSession.openedByTour) drawerSession.openedByTour = false;
          return waitMs(180).then(function () {
            return ensureOpportunitiesTab();
          });
        }

        if (step._openDrawer) {
          return ensureOpportunitiesTab().then(function () {
            if (drawerIsOpen() && drawerSession.demoOpportunityId) {
              ensureTourDrawerPointerCapture(api);
              return waitMs(80);
            }
            return openDemoDrawer().then(function (ok) {
              ensureTourDrawerPointerCapture(api);
              if (!ok) {
                console.warn(
                  "[GDI Guided Tour] No suitable opportunity drawer available — skipping drawer steps."
                );
              }
              return waitMs(120);
            });
          });
        }

        if (step._requireDrawer) {
          if (!drawerIsOpen()) {
            return openDemoDrawer().then(function () {
              ensureTourDrawerPointerCapture(api);
              return waitMs(100);
            });
          }
          ensureTourDrawerPointerCapture(api);
          return waitMs(60);
        }

        if (pageStepIds[step.id]) {
          return ensureOpportunitiesTab();
        }
        return null;
      },
      afterExit: function () {
        removeDrawerCancelGuard();
        restoreDrawerShowModal();
        var reopen = drawerSession.wasOpenBeforeTour
          ? drawerSession.priorOpportunityId
          : null;
        if (drawerSession.openedByTour || drawerIsOpen()) {
          closeDrawerNative();
        }
        drawerSession.openedByTour = false;
        drawerSession.demoOpportunityId = null;
        drawerSession.demotedFromModal = false;
        ensureOpportunitiesTab().then(function () {
          if (reopen) {
            drawerSession.wasOpenBeforeTour = false;
            drawerSession.priorOpportunityId = null;
            return restorePriorDrawer();
          }
          drawerSession.wasOpenBeforeTour = false;
          drawerSession.priorOpportunityId = null;
          return null;
        });
      },
      captureState: function () {
        var hotel = document.getElementById("gdiHotel");
        return {
          scrollX: window.scrollX || 0,
          scrollY: window.scrollY || 0,
          hotelId: hotel ? hotel.value : null,
          tab: "opportunities",
          drawerWasOpen: drawerSession.wasOpenBeforeTour,
          priorOpportunityId: drawerSession.priorOpportunityId,
        };
      },
      restoreState: function (saved, opts) {
        ensureOpportunitiesTab();
        if (opts && opts.restoreScroll !== false && saved) {
          if (
            typeof saved.scrollX === "number" &&
            typeof saved.scrollY === "number"
          ) {
            window.scrollTo(saved.scrollX, saved.scrollY);
          }
        }
      },
    });
  }

  var tour = null;

  function ensureTour() {
    if (!tour) tour = buildTour();
    return tour;
  }

  function init() {
    var t = ensureTour();
    if (t) t.init();
  }

  global.GdiGuidedReportTour = {
    version: TOUR_VERSION,
    init: init,
    start: function () {
      var t = ensureTour();
      if (t) return t.start();
    },
    exit: function () {
      if (tour) tour.exit();
    },
    stepDefs: STEP_DEFS,
    finishStep: FINISH_STEP,
    buttonId: BUTTON_ID,
    isActive: function () {
      return !!(tour && tour.isActive());
    },
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(typeof window !== "undefined" ? window : globalThis);
