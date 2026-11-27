/**
 * GDI Guided Report Tour — "How to Read This Report"
 * Tab-dynamic guides: Executive Summary | Opportunities | Commercial Priorities.
 * Portfolio Demand Intelligence keeps prior behavior (Opportunities guide).
 * Reuses DealalityGuidedTourEngine + ADP coachmark CSS (.adp-gt / .adp-howto-read-btn).
 */
(function (global) {
  "use strict";

  var TOUR_VERSION = "GDI_GUIDED_REPORT_TOUR_V1_2_CONTEXTUAL";
  var BUTTON_ID = "gdiHowToReadReport";
  var TOUR_NONMODAL_CLASS = "gdi-drawer--tour-nonmodal";

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

  var tour = null;
  var activeGuideKey = "opportunities";
  var tabSwitchBound = false;

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
    clearTourNonModalDrawer(d);
    drawerSession.demotedFromModal = false;
  }

  function pinTourNonModalDrawer(d) {
    if (!d) return;
    try {
      d.classList.add(TOUR_NONMODAL_CLASS);
    } catch (_) {}
  }

  function clearTourNonModalDrawer(d) {
    d = d || drawerEl();
    if (!d) return;
    try {
      d.classList.remove(TOUR_NONMODAL_CLASS);
    } catch (_) {}
  }

  function demoteModalDrawerForTour() {
    var d = drawerEl();
    if (!d || !drawerIsOpen()) return false;
    var isModal = false;
    try {
      isModal = !!(d.matches && d.matches(":modal"));
    } catch (_) {
      isModal = false;
    }
    if (!isModal) {
      pinTourNonModalDrawer(d);
      drawerSession.demotedFromModal = true;
      return false;
    }
    var suppressClose = function (e) {
      try {
        e.stopImmediatePropagation();
      } catch (_) {}
    };
    d.addEventListener("close", suppressClose, true);
    try {
      d.close();
      pinTourNonModalDrawer(d);
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
      if (tour && tour.isActive()) {
        try {
          pinTourNonModalDrawer(d);
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
    clearTourNonModalDrawer(d);
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

  function readActiveTab() {
    var active = document.querySelector(
      ".section-nav-item[data-tab][aria-selected='true']"
    );
    return active ? active.getAttribute("data-tab") || "opportunities" : "opportunities";
  }

  /**
   * Portfolio keeps prior Opportunities guide behavior (unchanged).
   */
  function resolveGuideKey(tab) {
    tab = tab || readActiveTab();
    if (tab === "executive-summary") return "executive-summary";
    if (tab === "priorities") return "priorities";
    return "opportunities";
  }

  function ensureTab(tabId) {
    var active = document.querySelector(
      '.section-nav-item[data-tab="' + tabId + '"][aria-selected="true"]'
    );
    if (active) return Promise.resolve();
    var tab = document.querySelector('.section-nav-item[data-tab="' + tabId + '"]');
    if (tab) {
      try {
        tab.click();
      } catch (_) {}
      return waitMs(260);
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
      body && body.querySelector("[data-id], [data-opportunity-id], [data-open]");
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
        ? String(contact.textContent || "").replace(/\s+/g, " ").trim()
        : "";
      if (
        contactText &&
        contactText !== "Contact Research Required" &&
        contactText !== "Buyer Not Yet Identified"
      ) {
        score += 40;
      }
      if (card.querySelector('[data-gdi-tour-target="view-details"]')) score += 10;
      ranked.push({ el: card, id: id, score: score });
    }
    ranked.sort(function (a, b) {
      return b.score - a.score;
    });
    if (ranked.length) return ranked[0];

    var listBtn = document.querySelector(
      ".gdi-list-view-btn[data-open], .gdi-list-opp-name[data-open], tr[data-open] .gdi-list-view-btn"
    );
    if (listBtn) {
      var lid =
        listBtn.getAttribute("data-open") ||
        (listBtn.closest("[data-open]") &&
          listBtn.closest("[data-open]").getAttribute("data-open"));
      if (lid) return { el: listBtn, id: lid, score: 1 };
    }
    return null;
  }

  function openDemoDrawer() {
    var pick = selectDemoCard();
    if (!pick) return Promise.resolve(false);
    drawerSession.demoOpportunityId = pick.id;
    var btn =
      (pick.el &&
        pick.el.querySelector &&
        (pick.el.querySelector('[data-gdi-tour-target="view-details"]') ||
          pick.el.querySelector("[data-open]"))) ||
      pick.el;
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
            body.querySelector("[data-gdi-tour-target='drawer-evidence']") ||
            body.querySelector("[data-gdi-tour-target='drawer-contact']")) &&
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
        '"], .gdi-list-view-btn[data-open="' +
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

  /* ——— Guide packs (customer-facing copy) ——— */

  var EXECUTIVE_SUMMARY_STEPS = [
    {
      id: "es-snapshot",
      target: '[data-gdi-tour-target="es-snapshot"]',
      title: "Commercial Snapshot",
      guide:
        "Start here for a concise view of this hotel’s qualified commercial demand. Each metric summarizes the inventory behind Group & Demand Intelligence for the selected property.",
      signal:
        "Use the Snapshot to brief leadership quickly before opening the detailed sections below.",
      lookNext: "Read what each commercial KPI means, then continue to the Assessment.",
    },
    {
      id: "es-assessment",
      target: '[data-gdi-tour-target="es-assessment"]',
      title: "Executive Commercial Assessment",
      guide:
        "This narrative explains the hotel’s commercial posture: where demand is strongest, what deserves near-term attention, and where judgment is still required.",
      signal:
        "Treat Assessment language as decision support for hotel leadership — not an automatic booking forecast.",
      lookNext: "Review Demand Composition to see how qualified opportunities break down by segment.",
    },
    {
      id: "es-composition",
      target: '[data-gdi-tour-target="es-composition"]',
      title: "Demand Composition",
      guide:
        "Demand Composition shows how qualified opportunities are distributed across demand segments for this hotel.",
      signal:
        "Segment mix helps prioritize which types of demand deserve sales coverage this cycle.",
      lookNext: "Check Commercial Demand Outlook for timing across the coming months.",
    },
    {
      id: "es-outlook",
      target: '[data-gdi-tour-target="es-outlook"]',
      title: "Commercial Demand Outlook",
      guide:
        "The Outlook places demand across months so leadership can see when commercial windows concentrate. Months with zero remain visible so gaps are honest.",
      signal:
        "Use Outlook for planning capacity and outreach timing — not as a guarantee of closed business.",
      lookNext: "Open the Dealality Research Receipt for research productivity context.",
    },
    {
      id: "es-receipt",
      target: '[data-gdi-tour-target="es-receipt"]',
      title: "Dealality Research Receipt",
      guide:
        "The Research Receipt summarizes the research work behind this hotel’s inventory, including estimated research hours and working days saved versus building the same picture manually.",
      signal:
        "Estimated productivity benefits describe research effort avoided. They are not the same as verified commercial outcomes or revenue.",
      lookNext:
        "Continue to Priority Commercial Opportunities for the sales-ready shortlist.",
      optional: true,
    },
    {
      id: "es-priority-opps",
      target: '[data-gdi-tour-target="es-priority-opps"]',
      title: "Priority Commercial Opportunities",
      guide:
        "These shared opportunity cards highlight the highest-priority pursuits for the hotel. Opportunity of the Week, when shown, marks the single most important near-term focus.",
      signal:
        "Open a card to review commercial reasoning, contact details, and recommended next steps.",
      lookNext: "Finish with Commercial Outcomes to separate verified results from estimates.",
      optional: true,
    },
    {
      id: "es-outcomes",
      target: '[data-gdi-tour-target="es-outcomes"]',
      title: "Commercial Outcomes",
      guide:
        "Commercial Outcomes capture verified hotel-reported results. Unavailable metrics appear honestly when evidence is not yet available — they are not hidden zeros.",
      signal:
        "Keep estimated research productivity and verified commercial outcomes distinct when briefing ownership or sales leadership.",
      lookNext:
        "Use Group & Demand Opportunities for the full working inventory and filters.",
      optional: true,
    },
  ];

  var EXECUTIVE_SUMMARY_FINISH = {
    id: "es-how-to-use",
    target: null,
    title: "How to Use the Executive Summary",
    finishCard: true,
    noScroll: true,
    guide: "A practical reading order for hotel leadership:",
    bullets: [
      "Scan the Commercial Snapshot KPIs.",
      "Read the Executive Commercial Assessment for posture and priorities.",
      "Review Demand Composition and Commercial Demand Outlook for mix and timing.",
      "Use the Research Receipt to understand research productivity — separately from outcomes.",
      "Open Priority Commercial Opportunities (and Opportunity of the Week) for pursuit focus.",
      "Check Commercial Outcomes for verified results only.",
    ],
    note: "When a metric is unavailable, treat it as incomplete evidence — not as zero demand.",
  };

  var OPPORTUNITIES_STEPS = [
    {
      id: "opportunities-overview",
      target: '[data-gdi-tour-target="opportunity-list"]',
      title: "Group & Demand Opportunities",
      guide:
        "This is the hotel’s working inventory of researched group and demand opportunities — accounts, events, and commercial demand worth reviewing for sales pursuit.",
      signal:
        "Use it as a living sales pipeline for this property, not a static market directory.",
      lookNext: "Set Property, Weekly Change, Opportunity Type, and Market Scope first.",
    },
    {
      id: "top-filters",
      target: '[data-gdi-tour-target="filters"]',
      title: "Property and Top Filters",
      guide:
        "Property selects the hotel. Weekly Change surfaces what is new or updated. Opportunity Type narrows by kind of demand. Need Dates context under the property bar shows how demand aligns to hotel need periods when configured.",
      signal:
        "Confirm the hotel and weekly lens before scanning the list so you are not reviewing the wrong mix.",
      lookNext: "Set Market Scope to All, Hotel, or Competitive.",
    },
    {
      id: "market-scope",
      target: '[data-gdi-tour-target="market-scope"]',
      title: "Market Scope",
      guide:
        "All shows every eligible record in the current view. Hotel focuses on hotel-specific pursuits for this property. Competitive includes competitor intelligence useful to monitor — it is not automatically available business for this hotel.",
      signal:
        "Competitive is market intelligence, not a booking guarantee.",
      lookNext: "Refine with Priority and Sales Opportunities filters.",
    },
    {
      id: "priority-filters",
      target: '[data-gdi-tour-target="priority-filters"]',
      title: "Priority Filters",
      guide:
        "Priority chips (All, High, Medium, Watch) focus the inventory by commercial priority for this hotel.",
      signal:
        "Start with High when you need a short pursuit list; use Watch for developing or longer-horizon demand.",
      lookNext: "Use Sales Opportunities chips for commercial demand themes.",
      optional: true,
    },
    {
      id: "sales-opportunity-filters",
      target: '[data-gdi-tour-target="sales-opportunity-filters"]',
      title: "Sales Opportunities Filters",
      guide:
        "Sales Opportunities chips highlight commercially attractive themes such as pursue-now demand, future groups, guest rooms, and catering — based on the live inventory for this hotel.",
      signal:
        "Combine Priority and Sales Opportunities to match how your team works today.",
      lookNext: "Switch List or Tiles and customize columns as needed.",
      optional: true,
    },
    {
      id: "view-toggle",
      target: '[data-gdi-tour-target="view-toggle"]',
      title: "List and Tiles Views",
      guide:
        "List is the default working table with sortable columns. Tiles present the same opportunities as cards. The Columns control (List only) shows or hides optional populated columns. Export CSV downloads the currently visible set.",
      signal:
        "What you see and export depends on authorized inventory and the filters you applied.",
      lookNext: "Scan Demand Window, Sales Action, Who to Contact, and Contact Details.",
      optional: true,
    },
    {
      id: "opportunity-rows",
      target: '[data-gdi-tour-target="opportunity-list"]',
      title: "Read Each Opportunity",
      guide:
        "Each row or card shows commercial title, Priority, Demand Type, Demand Window, Sales Action, Who to Contact, and Contact Details (email, telephone, LinkedIn when available). Guest rooms, catering, and meetings indicators show evidenced demand streams.",
      signal:
        "Use the eye icon to open details and the three-dot menu for contextual actions — without leaving the working list.",
      lookNext: "Open Opportunity Details to review contacts and commercial reasoning.",
    },
    {
      id: "detail-drawer",
      target: '[data-gdi-tour-target="drawer"]',
      title: "Opportunity Drawer",
      guide:
        "The drawer holds the full commercial brief: what is happening, potential business, why this hotel, why act now, facts and uncertainties, recommended approach, and supporting evidence.",
      signal:
        "Rely on the drawer before deciding whether an opportunity is worth pursuing.",
      lookNext: "Review Who to Contact for primary and alternative contacts.",
      deferTargetCheck: true,
      forceInclude: true,
      optional: true,
      allowDrawerScroll: true,
      scrollInDrawer: true,
      _openDrawer: true,
    },
    {
      id: "drawer-contact",
      target: '[data-gdi-tour-target="drawer-contact"]',
      title: "Who to Contact",
      guide:
        "Who to Contact presents the primary lodging or commercial decision-maker when verified, plus buyer influencers and alternative entry points when direct buying authority is not confirmed. Why We Are Showing This Contact explains relevance. Contact Details stay tied to the person shown — email, telephone, and LinkedIn are researched as separate channels.",
      signal:
        "On-demand contact enrichment (when available) is explicit and person-specific. It does not invent contacts or merge details across people.",
      lookNext: "Review evidence, then return to the list or export your working set.",
      deferTargetCheck: true,
      optional: true,
      allowDrawerScroll: true,
      scrollInDrawer: true,
      _requireDrawer: true,
    },
    {
      id: "drawer-evidence",
      target: '[data-gdi-tour-target="drawer-evidence"]',
      title: "Evidence and Next Step",
      guide:
        "Separate confirmed facts from inferred demand, note uncertainties, and read the recommended next step for the sales team.",
      signal:
        "Inferred demand is useful intelligence — it is not the same as a confirmed booking.",
      lookNext: "Close the drawer and continue working the filtered list.",
      deferTargetCheck: true,
      optional: true,
      allowDrawerScroll: true,
      scrollInDrawer: true,
      _requireDrawer: true,
    },
    {
      id: "export",
      target: '[data-gdi-tour-target="export"]',
      title: "Export CSV",
      guide:
        "Export CSV downloads the currently visible opportunity set for working review outside the page.",
      signal:
        "Export after filters and Market Scope match the list your team intends to work.",
      lookNext: "Return to Executive Summary for leadership briefing, or Commercial Priorities for need-date alignment.",
      optional: true,
      _closeDrawer: true,
    },
  ];

  var OPPORTUNITIES_FINISH = {
    id: "how-to-use",
    target: null,
    title: "How to Use Group & Demand Opportunities",
    finishCard: true,
    noScroll: true,
    guide: "A practical reading order for sales leaders:",
    bullets: [
      "Confirm Property, Weekly Change, Opportunity Type, and Market Scope.",
      "Refine with Priority and Sales Opportunities filters.",
      "Choose List or Tiles; use Columns and Export as needed.",
      "Review Demand Window, Sales Action, Who to Contact, and Contact Details.",
      "Open the drawer for commercial reasoning, primary and alternative contacts, and evidence.",
      "Distinguish hotel pursuits from Competitive market intelligence.",
    ],
    note: "Group & Demand Intelligence supports hotel sales judgment — it does not replace it.",
    _closeDrawer: true,
  };

  var PRIORITIES_STEPS = [
    {
      id: "hcp-summary",
      target: '[data-gdi-tour-target="hcp-summary"]',
      title: "Commercial Priorities Overview",
      guide:
        "Commercial Priorities capture how this hotel wants to win business: revenue priorities, active need months, preferred booking windows, and target demand segments.",
      signal:
        "Keep these preferences current so Group & Demand Opportunities can align to hotel commercial needs.",
      lookNext: "Review Revenue Priorities for the types of business you most want to attract.",
      optional: true,
    },
    {
      id: "hcp-revenue",
      target: '[data-gdi-tour-target="hcp-section-revenue"]',
      title: "Revenue Priorities",
      guide:
        "Select up to five types of business your hotel most wants to attract. These choices guide commercial prioritization across GDI.",
      signal:
        "Fewer, clearer priorities produce a sharper sales focus than selecting every option.",
      lookNext: "Set Need Dates for months when additional business is most valuable.",
      optional: true,
    },
    {
      id: "hcp-need-dates",
      target: '[data-gdi-tour-target="hcp-section-need-dates"]',
      title: "Need Dates",
      guide:
        "Need Dates mark months when additional demand is most valuable. Tap a month to add or review a need period. Months without a need period are neutral — not a signal of low demand.",
      signal:
        "Upcoming demand periods help sales focus outreach on windows that matter most to the hotel.",
      lookNext: "Confirm Preferred Business and Target Demand Segments.",
      optional: true,
    },
    {
      id: "hcp-preferred",
      target: '[data-gdi-tour-target="hcp-section-preferred"]',
      title: "Preferred Business",
      guide:
        "Describe group size, meeting needs, booking windows, and stay preferences so opportunity matching reflects how your hotel sells.",
      signal:
        "Preferred windows and meeting needs improve alignment between researched demand and hotel capacity.",
      lookNext: "Review Target Demand Segments and Local Market Intelligence.",
      optional: true,
    },
    {
      id: "hcp-customers",
      target: '[data-gdi-tour-target="hcp-section-customers"]',
      title: "Target Demand Segments",
      guide:
        "Choose preferred demand segments for this hotel so sales coverage stays intentional.",
      signal:
        "Segment preferences help identify which opportunities in Group & Demand Opportunities deserve attention first.",
      lookNext: "Use Local Market Intelligence for generators and known accounts.",
      optional: true,
    },
    {
      id: "hcp-local",
      target: '[data-gdi-tour-target="hcp-section-local-market"]',
      title: "Local Market Intelligence",
      guide:
        "Capture local demand generators and known accounts your hotel already works with. Suggestions remain unconfirmed until you select them.",
      signal:
        "Known relationships improve relevance when reviewing researched opportunities.",
      lookNext: "Save priorities, then return to Opportunities to open matching details.",
      optional: true,
    },
  ];

  var PRIORITIES_FINISH = {
    id: "hcp-how-to-use",
    target: null,
    title: "How to Use Commercial Priorities",
    finishCard: true,
    noScroll: true,
    guide: "A practical sequence for hotel commercial teams:",
    bullets: [
      "Set Revenue Priorities for the business you most want to attract.",
      "Mark Need Dates for months that need demand.",
      "Confirm Preferred Business and Target Demand Segments.",
      "Capture Local Market Intelligence and hotel fit considerations.",
      "Save changes, then use Group & Demand Opportunities to open matching opportunity details.",
    ],
    note: "Commercial Priorities guide alignment — they do not replace reviewing opportunity evidence in the drawer.",
  };

  function guidePack(key) {
    if (key === "executive-summary") {
      return { steps: EXECUTIVE_SUMMARY_STEPS, finish: EXECUTIVE_SUMMARY_FINISH, tab: "executive-summary" };
    }
    if (key === "priorities") {
      return { steps: PRIORITIES_STEPS, finish: PRIORITIES_FINISH, tab: "priorities" };
    }
    return { steps: OPPORTUNITIES_STEPS, finish: OPPORTUNITIES_FINISH, tab: "opportunities" };
  }

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

  function applyGuideToTour(key) {
    if (!tour || !tour.config) return;
    var pack = guidePack(key);
    activeGuideKey = key;
    tour.config.steps = pack.steps;
    tour.config.finishStep = pack.finish;
    tour.config.version = TOUR_VERSION + ":" + key;
    tour.config.emptyMessage =
      key === "executive-summary"
        ? "Load the Executive Summary for this property first, then click How to Read This Report again."
        : key === "priorities"
          ? "Open Commercial Priorities for this property first, then click How to Read This Report again."
          : "Load opportunities for this property first, then click How to Read This Report again.";
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
      steps: OPPORTUNITIES_STEPS,
      finishStep: OPPORTUNITIES_FINISH,
      isTargetVisible: isGdiTargetVisible,
      repairPointerCapture: function () {
        demoteModalDrawerForTour();
      },
      beforeStart: function () {
        var key = resolveGuideKey(readActiveTab());
        applyGuideToTour(key);
        var pack = guidePack(key);
        return ensureTab(pack.tab).then(function () {
          if (key !== "opportunities") {
            if (drawerIsOpen()) closeDrawerNative();
            return waitMs(120);
          }
          drawerSession.wasOpenBeforeTour = drawerIsOpen();
          drawerSession.priorOpportunityId = currentOpenOpportunityId();
          drawerSession.openedByTour = false;
          drawerSession.demoOpportunityId = null;
          drawerSession.demotedFromModal = false;
          patchDrawerShowModalForTour();
          installDrawerCancelGuard();
          if (drawerSession.wasOpenBeforeTour) {
            closeDrawerNative();
            return waitMs(180);
          }
          return null;
        });
      },
      beforeStep: function (step, api) {
        if (!step) return null;
        if (activeGuideKey !== "opportunities") {
          return ensureTab(guidePack(activeGuideKey).tab);
        }

        var pageStepIds = {
          "opportunities-overview": 1,
          "top-filters": 1,
          "market-scope": 1,
          "priority-filters": 1,
          "sales-opportunity-filters": 1,
          "view-toggle": 1,
          "opportunity-rows": 1,
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
            return ensureTab("opportunities");
          });
        }

        if (step._openDrawer) {
          return ensureTab("opportunities").then(function () {
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
          return ensureTab("opportunities");
        }
        return null;
      },
      afterExit: function () {
        removeDrawerCancelGuard();
        restoreDrawerShowModal();
        if (activeGuideKey !== "opportunities") {
          drawerSession.openedByTour = false;
          drawerSession.demoOpportunityId = null;
          drawerSession.demotedFromModal = false;
          drawerSession.wasOpenBeforeTour = false;
          drawerSession.priorOpportunityId = null;
          return;
        }
        var reopen = drawerSession.wasOpenBeforeTour
          ? drawerSession.priorOpportunityId
          : null;
        if (drawerSession.openedByTour || drawerIsOpen()) {
          closeDrawerNative();
        }
        drawerSession.openedByTour = false;
        drawerSession.demoOpportunityId = null;
        drawerSession.demotedFromModal = false;
        ensureTab("opportunities").then(function () {
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
          tab: readActiveTab(),
          guideKey: activeGuideKey,
          drawerWasOpen: drawerSession.wasOpenBeforeTour,
          priorOpportunityId: drawerSession.priorOpportunityId,
        };
      },
      restoreState: function (saved, opts) {
        var key = (saved && saved.guideKey) || resolveGuideKey();
        ensureTab(guidePack(key).tab);
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

  function ensureTour() {
    if (!tour) tour = buildTour();
    return tour;
  }

  function startForActiveTab() {
    var t = ensureTour();
    if (!t) return;
    applyGuideToTour(resolveGuideKey(readActiveTab()));
    return t.start();
  }

  function bindTabSwitchRefresh() {
    if (tabSwitchBound) return;
    tabSwitchBound = true;
    document.addEventListener(
      "click",
      function (e) {
        var tabBtn = e.target && e.target.closest
          ? e.target.closest(".section-nav-item[data-tab]")
          : null;
        if (!tabBtn || !tour || !tour.isActive()) return;
        var nextTab = tabBtn.getAttribute("data-tab");
        var nextKey = resolveGuideKey(nextTab);
        if (nextKey === activeGuideKey && nextTab !== "portfolio") return;
        // Exit stale guide, then restart with the guide for the newly selected tab.
        try {
          tour.exit();
        } catch (_) {}
        window.setTimeout(function () {
          applyGuideToTour(nextKey);
          if (tour) tour.start();
        }, 320);
      },
      true
    );
  }

  function init() {
    var t = ensureTour();
    if (t) t.init();
    bindTabSwitchRefresh();
  }

  global.GdiGuidedReportTour = {
    version: TOUR_VERSION,
    init: init,
    start: startForActiveTab,
    exit: function () {
      if (tour) tour.exit();
    },
    resolveGuideKey: resolveGuideKey,
    guidePack: guidePack,
    stepDefs: OPPORTUNITIES_STEPS,
    finishStep: OPPORTUNITIES_FINISH,
    executiveSummarySteps: EXECUTIVE_SUMMARY_STEPS,
    prioritiesSteps: PRIORITIES_STEPS,
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
