/**
 * Dealality GDI UI presentation — reusable product UI helpers.
 * Classification: REUSABLE_PRODUCT_UI
 * Hotel-specific facts must come from data/config, never hardcodes here.
 */
(function (root) {
  "use strict";

  var PRIORITY_DISPLAY = {
    HIGH_PRIORITY: { label: "HIGH", pillClass: "gdi-pill gdi-pill-high" },
    MEDIUM_PRIORITY: { label: "MEDIUM", pillClass: "gdi-pill gdi-pill-med" },
    WATCHLIST: { label: "WATCH", pillClass: "gdi-pill gdi-pill-watch" },
  };

  var ACTION_STATUS_DISPLAY = {
    CONTACT_NOW: "PURSUE NOW",
    QUALIFY_NOW: "QUALIFY",
    WATCH: "WATCH",
    TOO_EARLY: "TOO EARLY",
    CLOSED: "CLOSED",
    NOT_ACTIONABLE: "NOT ACTIONABLE",
  };

  var ACTION_PILL_DISPLAY = {
    CONTACT_NOW: {
      label: "PURSUE NOW",
      pillClass: "gdi-pill gdi-pill-action-pursue",
    },
    QUALIFY_NOW: {
      label: "QUALIFY",
      pillClass: "gdi-pill gdi-pill-action-qualify",
    },
    WATCH: { label: "WATCH", pillClass: "gdi-pill gdi-pill-action-watch" },
    TOO_EARLY: {
      label: "TOO EARLY",
      pillClass: "gdi-pill gdi-pill-action-too-early",
    },
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /** Allow only http(s) for external source links — reject javascript:/data:/file:. */
  function isSafeHttpUrl(url) {
    var s = String(url == null ? "" : url).trim();
    if (!s) return false;
    try {
      var u = new URL(s, "https://example.invalid");
      return u.protocol === "https:" || u.protocol === "http:";
    } catch (e) {
      return false;
    }
  }

  function fmtVal(v) {
    if (v == null || v === "" || v === "UNKNOWN") return "—";
    return v;
  }

  function trunc(s, n) {
    s = s == null ? "" : String(s);
    if (s.length <= n) return s;
    return s.slice(0, n) + "…";
  }

  var MONTH_SHORT = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];

  function parseIsoDateParts(iso) {
    if (!iso || typeof iso !== "string") return null;
    var m = iso.trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) return null;
    var year = Number(m[1]);
    var month = Number(m[2]);
    var day = Number(m[3]);
    if (!year || month < 1 || month > 12 || day < 1 || day > 31) return null;
    return { year: year, month: month, day: day };
  }

  /**
   * Tile/list event date: "Apr 20-23, 2027" (same month) / "Apr 20-May 2, 2027".
   * Same-day uses "Apr 20-20, 2027". Classification: REUSABLE_PRODUCT_UI
   */
  function eventDateRangeLabel(startIso, endIso, fallback) {
    var start = parseIsoDateParts(startIso);
    var end = parseIsoDateParts(endIso) || start;
    if (!start) {
      if (fallback && String(fallback).trim()) return String(fallback).trim();
      return "Dates TBD";
    }
    var startMon = MONTH_SHORT[start.month - 1];
    if (!end || (end.year === start.year && end.month === start.month && end.day === start.day)) {
      return startMon + " " + start.day + "-" + start.day + ", " + start.year;
    }
    if (end.year === start.year && end.month === start.month) {
      return startMon + " " + start.day + "-" + end.day + ", " + start.year;
    }
    var endMon = MONTH_SHORT[end.month - 1];
    if (end.year === start.year) {
      return (
        startMon +
        " " +
        start.day +
        "-" +
        endMon +
        " " +
        end.day +
        ", " +
        start.year
      );
    }
    return (
      startMon +
      " " +
      start.day +
      ", " +
      start.year +
      "-" +
      endMon +
      " " +
      end.day +
      ", " +
      end.year
    );
  }

  function priorityPill(priority) {
    var meta = PRIORITY_DISPLAY[priority] || PRIORITY_DISPLAY.WATCHLIST;
    return '<span class="' + meta.pillClass + '">' + esc(meta.label) + "</span>";
  }

  function tileEventDatePill(it, linked) {
    linked = linked || {};
    it = it || {};
    var label = eventDateRangeLabel(
      linked.eventStartDate || it.eventStartDate,
      linked.eventEndDate || it.eventEndDate,
      it.eventTiming || linked.eventTiming
    );
    return (
      '<span class="gdi-tile-pill gdi-tile-pill--date gdi-pill gdi-pill-date">' +
      esc(label || "Dates TBD") +
      "</span>"
    );
  }

  function tileActionPill(status, priority, activeFilters) {
    if (!status) return "";
    activeFilters = activeFilters || {};
    var isActive = activeFilters.booking === status;
    var meta = ACTION_PILL_DISPLAY[status];
    if (!meta) {
      return (
        '<button type="button" class="gdi-tile-pill gdi-tile-pill--action gdi-pill gdi-pill-action-watch' +
        (isActive ? " is-filter-active" : "") +
        '" data-gdi-tile-priority="' +
        esc(priority || "") +
        '" data-gdi-tile-booking="' +
        esc(status) +
        '" aria-pressed="' +
        isActive +
        '">' +
        esc(actionStatusLabel(status)) +
        "</button>"
      );
    }
    return (
      '<button type="button" class="gdi-tile-pill gdi-tile-pill--action ' +
      meta.pillClass +
      (isActive ? " is-filter-active" : "") +
      '" data-gdi-tile-priority="' +
      esc(priority || "") +
      '" data-gdi-tile-booking="' +
      esc(status) +
      '" aria-pressed="' +
      isActive +
      '">' +
      esc(meta.label) +
      "</button>"
    );
  }

  function actionStatusLabel(status) {
    if (!status) return "—";
    return ACTION_STATUS_DISPLAY[status] || String(status).replace(/_/g, " ");
  }

  function hotelLocationLine(hotel) {
    hotel = hotel || {};
    var parts = [];
    if (hotel.city) parts.push(hotel.city);
    if (hotel.state) parts.push(hotel.state);
    return parts.join(", ");
  }

  /**
   * Optional status badges — not structural chrome.
   * @param {{pilot?:boolean,readOnly?:boolean}} badges
   */
  function statusBadgesHtml(badges) {
    badges = badges || {};
    var bits = [];
    if (badges.pilot) bits.push('<span class="gdi-badge">PILOT</span>');
    if (badges.readOnly) bits.push('<span class="gdi-badge gdi-badge--readonly">Read-only</span>');
    if (!bits.length) return "";
    return '<div class="gdi-status-badges">' + bits.join("") + "</div>";
  }

  var GDI_PAGE_TITLE = "Group Demand Intelligence";
  var GDI_PAGE_SUBTITLE =
    "See which group opportunities your hotel may be positioned to pursue, why they matter now, and what sales action to take next.";
  var GDI_METHODOLOGY = [
    "Based on structured research into group and meeting demand the hotel may be positioned to pursue.",
    "Findings are research-assisted and observational. Sales teams should validate before outreach.",
    "Dealality qualifies opportunities by hotel fit, evidence confidence, and booking window. It does not guarantee bookings.",
  ];

  var MONTH_NAME_RE =
    "(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)";

  /**
   * Remove event dates/years from tile title/summary copy.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function scrubEventDatesFromText(raw) {
    var s = String(raw == null ? "" : raw);
    if (!s) return "";
    // Parenthetical ranges: (Jun 7-9), (Nov 13–15 & 20–22)
    s = s.replace(
      new RegExp(
        "\\s*\\(" +
          MONTH_NAME_RE +
          "\\.?\\s+\\d{1,2}(?:\\s*[–—\\-]\\s*\\d{1,2})?(?:\\s*&\\s*(?:" +
          MONTH_NAME_RE +
          "\\.?\\s+)?\\d{1,2}(?:\\s*[–—\\-]\\s*\\d{1,2})?)*\\)",
        "gi"
      ),
      ""
    );
    // "June 7–9, 2027" / ", June 7–9, 2027"
    s = s.replace(
      new RegExp(
        "(?:,\\s*)?" +
          MONTH_NAME_RE +
          "\\.?\\s+\\d{1,2}(?:\\s*[–—\\-]\\s*\\d{1,2})?,\\s*20\\d{2}",
        "gi"
      ),
      ""
    );
    // ISO ranges / singles
    s = s.replace(
      /\b20\d{2}-\d{2}-\d{2}(?:\s*[→\-–—]\s*20\d{2}-\d{2}-\d{2})?/g,
      ""
    );
    // Year before em dash: "Expo 2027 —"
    s = s.replace(/\s+20\d{2}(?=\s*[—\-–])/g, "");
    // "in 2027" / "for 2027" / bare trailing year
    s = s.replace(/\s+(?:in|for|of)\s+20\d{2}\b/gi, "");
    s = s.replace(/\s+20\d{2}(?=\s*[.;,]|$)/g, "");
    // Cleanup punctuation left behind
    s = s.replace(/\s*;\s*;/g, ";");
    s = s.replace(/\s*,\s*,/g, ",");
    s = s.replace(/\s*,\s*;/g, ";");
    s = s.replace(/;\s*$/g, "");
    s = s.replace(/,\s*$/g, "");
    s = s.replace(/\.\s*$/g, ".");
    s = s.replace(/\s*[—\-–]\s*$/g, "");
    s = s.replace(/\s{2,}/g, " ").trim();
    // Avoid orphan "with open venue selection." unfinished — keep as-is after year strip
    return s;
  }

  /**
   * ADP-identical product page header (title + subtitle + methodology box).
   * Classification: REUSABLE_PRODUCT_UI
   * Hotel identity lives in propertyBarHtml — not in this header.
   */
  function hotelShellHtml(opts) {
    opts = opts || {};
    var mode = opts.mode === "share" ? "share" : "auth";
    var title = opts.pageTitle || GDI_PAGE_TITLE;
    var subtitle = opts.pageSubtitle || GDI_PAGE_SUBTITLE;
    var methodology = opts.methodology || GDI_METHODOLOGY;
    var brand =
      mode === "share"
        ? '<div class="gdi-share-brand"><div class="gdi-share-brand__mark"><span class="gdi-share-brand__logo">D</span>Dealality</div>' +
          statusBadgesHtml(opts.badges) +
          "</div>"
        : "";

    var methodItems = (methodology || [])
      .map(function (line) {
        return '<li class="aiv-disclaimer-box__item">' + esc(line) + "</li>";
      })
      .join("");

    var badgeHtml = statusBadgesHtml(mode === "share" ? null : opts.badges);

    return (
      brand +
      '<header class="dashboard-header aiv-dashboard-header gdi-page-header">' +
      '<div class="dashboard-header-left">' +
      '<div class="dashboard-title-container">' +
      "<h1>" +
      esc(title) +
      "</h1>" +
      (badgeHtml ? '<div class="gdi-title-badges">' + badgeHtml + "</div>" : "") +
      "</div>" +
      '<p class="aiv-header-subtitle">' +
      esc(subtitle) +
      "</p>" +
      "</div>" +
      '<aside class="aiv-disclaimer-box" role="note">' +
      '<div class="aiv-disclaimer-box__title">Methodology</div>' +
      '<ul class="aiv-disclaimer-box__list">' +
      methodItems +
      "</ul></aside></header>"
    );
  }

  /**
   * ADP filters-section property bar (Property + research status + action).
   * Classification: REUSABLE_PRODUCT_UI
   */
  function propertyBarHtml(opts) {
    opts = opts || {};
    var hotel = opts.hotel || {};
    var name = hotel.hotelName || hotel.name || "Hotel";
    var location = hotelLocationLine(hotel);
    var propertyLabel = location ? name + " — " + location : name;
    var mode = opts.mode === "share" ? "share" : "auth";
    var hotelId = opts.hotelId || "";
    var hotels = Array.isArray(opts.hotels) ? opts.hotels : null;

    var propertyField;
    if (mode === "share" || opts.readOnlyProperty) {
      propertyField =
        '<div class="filter-group filter-group--grow"><label class="filter-label">Property</label>' +
        '<div class="filter-select gdi-filter-static">' +
        esc(propertyLabel) +
        "</div></div>";
    } else if (hotels && hotels.length) {
      var options = hotels
        .map(function (h) {
          var id = h.hotelId || "";
          var label =
            h.optionLabel ||
            (h.locationLine
              ? (h.hotelName || h.displayName || id) + " — " + h.locationLine
              : h.hotelName || h.displayName || id);
          return (
            '<option value="' +
            esc(id) +
            '"' +
            (id === hotelId ? " selected" : "") +
            ">" +
            esc(label) +
            "</option>"
          );
        })
        .join("");
      propertyField =
        '<div class="filter-group filter-group--grow"><label class="filter-label" for="gdiHotel">Property</label>' +
        '<select class="filter-select" id="gdiHotel" aria-label="Select GDI hotel">' +
        options +
        "</select></div>";
    } else {
      propertyField =
        '<div class="filter-group filter-group--grow"><label class="filter-label" for="gdiHotel">Property</label>' +
        '<select class="filter-select" id="gdiHotel"><option value="' +
        esc(hotelId) +
        '">' +
        esc(propertyLabel) +
        "</option></select></div>";
    }

    return (
      '<section class="filters-section aiv-filters-section gdi-property-bar" aria-label="Property selection">' +
      propertyField +
      weeklyThisWeekFilterHtml(opts) +
      '<div class="filter-group gdi-property-stat">' +
      '<span class="filter-label">Last Research</span>' +
      '<div class="gdi-property-stat__value">' +
      esc(opts.lastResearch != null && opts.lastResearch !== "" ? opts.lastResearch : "—") +
      "</div></div>" +
      '<div class="filter-group gdi-property-stat">' +
      '<span class="filter-label">Run Status</span>' +
      '<div class="gdi-property-stat__value">' +
      esc(opts.runStatus || "NEVER_RUN") +
      "</div></div>" +
      (opts.actionHtml
        ? '<div class="filter-group aiv-filter-actions">' + opts.actionHtml + "</div>"
        : "") +
      "</section>"
    );
  }

  /**
   * This Week delta filter — compact select in the property bar (left of Last Research).
   * Classification: REUSABLE_PRODUCT_UI
   */
  function weeklyThisWeekFilterHtml(opts) {
    opts = opts || {};
    if (opts.showWeeklyFilter === false) return "";
    var active = opts.activeWeekly != null ? opts.activeWeekly : "";
    var counts = opts.weeklyCounts || {};
    var options = [
      { value: "", label: "All", countKey: "all" },
      { value: "NEW", label: "New This Week", countKey: "NEW" },
      { value: "UPDATED", label: "Updated", countKey: "UPDATED" },
      { value: "REACTIVATED", label: "Reactivated", countKey: "REACTIVATED" },
      { value: "HIGH_PRIORITY", label: "High Priority", countKey: "HIGH_PRIORITY" },
    ];
    return (
      '<div class="filter-group gdi-weekly-filter">' +
      '<label class="filter-label" for="gdiWeeklyFilter">This Week</label>' +
      '<select class="filter-select" id="gdiWeeklyFilter" aria-label="Filter by weekly change">' +
      options
        .map(function (o) {
          var n = counts[o.countKey];
          var text =
            n == null || n === ""
              ? o.label
              : o.label + " (" + n + ")";
          return (
            '<option value="' +
            esc(o.value) +
            '"' +
            (active === o.value ? " selected" : "") +
            ">" +
            esc(text) +
            "</option>"
          );
        })
        .join("") +
      "</select></div>"
    );
  }

  /** Primary KPI strip — ADP .aiv-kpi treatment. Max ~5. */
  function primaryKpiHtml(items) {
    items = (items || []).slice(0, 5);
    if (!items.length) return "";
    return (
      '<div class="aiv-theme-group gdi-primary-kpis" aria-label="Primary metrics">' +
      '<div class="aiv-kpi-row aiv-theme-kpis gdi-aiv-kpi-row">' +
      items
        .map(function (it) {
          return (
            '<article class="aiv-kpi"><h3><span class="aiv-kpi-label">' +
            esc(it.label) +
            '</span></h3><div class="aiv-value">' +
            esc(it.value) +
            "</div>" +
            (it.meta
              ? '<div class="aiv-meta">' + esc(it.meta) + "</div>"
              : "") +
            "</article>"
          );
        })
        .join("") +
      "</div></div>"
    );
  }

  /** Secondary grouped summary — visually subordinate. */
  function secondarySummaryHtml(title, items) {
    items = items || [];
    if (!items.length) return "";
    return (
      '<section class="gdi-secondary-summary aiv-theme-group">' +
      '<h3 class="aiv-theme-label">' +
      esc(title) +
      "</h3>" +
      '<ul class="gdi-secondary-summary__list">' +
      items
        .map(function (it) {
          return (
            "<li><span class=\"gdi-secondary-summary__lab\">" +
            esc(it.label) +
            '</span><span class="gdi-secondary-summary__val">' +
            esc(it.value) +
            "</span></li>"
          );
        })
        .join("") +
      "</ul></section>"
    );
  }

  /**
   * Build primary + secondary KPI models from hotel summary + opportunities.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function buildMetricModel(opts) {
    opts = opts || {};
    var summary = opts.summary || {};
    var opportunities = opts.opportunities || [];
    var qualified =
      summary.qualifiedCount != null ? summary.qualifiedCount : opportunities.length;

    var primary = [
      { label: "Opportunities", value: qualified },
      {
        label: "High Priority",
        value: summary.highPriorityCount || 0,
      },
      {
        label: "Medium Priority",
        value: summary.mediumPriorityCount || 0,
      },
      { label: "Watch", value: summary.watchlistCount || 0 },
    ];
    if (summary.validated != null || summary.pendingValidation != null) {
      primary.push({
        label: "Validated",
        value: summary.validated || 0,
        meta:
          summary.pendingValidation != null
            ? summary.pendingValidation + " pending"
            : null,
      });
    } else if (opts.enteringWindow != null) {
      primary.push({
        label: "Entering Window",
        value: opts.enteringWindow,
      });
    }

    var validationItems = [];
    if (summary.validated != null || summary.confirmedNew != null) {
      validationItems = [
        { label: "Confirmed New", value: summary.confirmedNew || 0 },
        { label: "Already Known", value: summary.alreadyKnown || 0 },
        { label: "Worth Pursuing", value: summary.worthPursuingNow || 0 },
        { label: "Not Relevant", value: summary.notRelevant || 0 },
        {
          label: "Pending",
          value:
            summary.pendingValidation != null
              ? summary.pendingValidation
              : Math.max(0, qualified - (summary.validated || 0)),
        },
      ];
    }

    var territoryMap = {};
    opportunities.forEach(function (o) {
      var code = o.demandTerritoryFit;
      if (!code) return;
      if (!territoryMap[code]) {
        territoryMap[code] = {
          code: code,
          label: o.demandTerritoryFitLabel || code.replace(/_/g, " "),
          count: 0,
        };
      }
      territoryMap[code].count += 1;
    });
    var territoryItems = Object.keys(territoryMap)
      .map(function (k) {
        return territoryMap[k];
      })
      .sort(function (a, b) {
        return b.count - a.count;
      })
      .map(function (t) {
        return { label: t.label, value: t.count, code: t.code };
      });

    return { primary: primary, validation: validationItems, territories: territoryItems };
  }

  var GDI_MAIN_TAB = {
    id: "opportunities",
    label: "Group Demand\nIntelligence",
  };

  function formatTabLabel(label) {
    var raw = String(label == null ? "" : label);
    if (raw.indexOf("\n") >= 0) {
      return raw
        .split("\n")
        .map(function (line) {
          return esc(line);
        })
        .join("<br>");
    }
    return esc(raw).replace(/\s+/g, "<br>");
  }

  function contentTabsHtml(activeTab, tabs) {
    tabs = tabs || [GDI_MAIN_TAB];
    var icons = {
      opportunities:
        '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>',
      audit:
        '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 4h8a2 2 0 0 1 2 2v14l-6-3-6 3V6a2 2 0 0 1 2-2z"/></svg>',
    };
    if (!tabs.length) return "";
    return (
      '<nav class="bdd-section-nav aiv-section-nav gdi-section-nav" aria-label="Group Demand views" role="tablist">' +
      tabs
        .map(function (t) {
          var isActive = activeTab === t.id;
          var label = formatTabLabel(t.label || t.id);
          var icon = icons[t.id] || icons.opportunities;
          return (
            '<button type="button" class="section-nav-item' +
            (isActive ? " active" : "") +
            '" role="tab" aria-selected="' +
            isActive +
            '" data-tab="' +
            esc(t.id) +
            '">' +
            '<div class="section-nav-icon" title="' +
            esc(t.label) +
            '">' +
            icon +
            "</div>" +
            '<span class="section-nav-label">' +
            label +
            "</span></button>"
          );
        })
        .join("") +
      "</nav>"
    );
  }

  var PRIORITY_RANK = { HIGH_PRIORITY: 0, MEDIUM_PRIORITY: 1, WATCHLIST: 2 };
  /* Priority palette — green / amber / slate (distinct from Action Window). */
  var PRIORITY_SWATCH = {
    HIGH_PRIORITY: "#14ca74",
    MEDIUM_PRIORITY: "#fdb52a",
    WATCHLIST: "#7e89ac",
  };
  /* Action Window palette — violet / orange / cyan / pink (no overlap with Priority). */
  var ACTION_SWATCH = {
    CONTACT_NOW: "#a855f7",
    QUALIFY_NOW: "#f97316",
    WATCH: "#06b6d4",
    TOO_EARLY: "#ec4899",
  };

  function presetLabelWithCount(label, n) {
    if (n == null || n === "") return label;
    return label + " (" + n + ")";
  }
  var SORT_OPTIONS = [
    { value: "priority", label: "Priority" },
    { value: "hotelFitScore", label: "Hotel Fit" },
    { value: "evidenceConfidence", label: "Evidence" },
    { value: "eventStartDate", label: "Event date" },
    { value: "title", label: "Name (A-Z)" },
  ];

  function countByPriority(opportunities) {
    var counts = {
      "": 0,
      HIGH_PRIORITY: 0,
      MEDIUM_PRIORITY: 0,
      WATCHLIST: 0,
    };
    (opportunities || []).forEach(function (o) {
      counts[""] += 1;
      var p = o.priority || "WATCHLIST";
      if (counts[p] == null) counts[p] = 0;
      counts[p] += 1;
    });
    return counts;
  }

  function countByActionStatus(opportunities) {
    var counts = {
      "": 0,
      CONTACT_NOW: 0,
      QUALIFY_NOW: 0,
      WATCH: 0,
      TOO_EARLY: 0,
    };
    (opportunities || []).forEach(function (o) {
      counts[""] += 1;
      var s = o.bookingWindowStatus || "";
      if (counts[s] == null) counts[s] = 0;
      if (s) counts[s] += 1;
    });
    return counts;
  }

  /**
   * Facet counts for Priority presets given the other active browse filters.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function facetCountByPriority(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      booking: filters.booking || "",
      segment: filters.segment || "",
      territory: filters.territory || "",
    });
    return countByPriority(subset);
  }

  /**
   * Facet counts for Action Window presets given the other active browse filters.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function facetCountByActionStatus(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      priority: filters.priority || "",
      segment: filters.segment || "",
      territory: filters.territory || "",
    });
    return countByActionStatus(subset);
  }

  /**
   * Clear the companion filter when the pair would return zero rows.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function reconcileBrowseFilters(opportunities, filters, changedKey) {
    filters = Object.assign(
      {
        priority: "",
        booking: "",
        segment: "",
        territory: "",
        weekly: "",
      },
      filters || {}
    );
    if (!filterOpportunities(opportunities, filters).length) {
      if (changedKey === "priority") filters.booking = "";
      else if (changedKey === "booking") filters.priority = "";
      else if (changedKey === "weekly") {
        filters.priority = "";
        filters.booking = "";
      }
    }
    return filters;
  }

  function weeklyDeltaStateOf(opp) {
    opp = opp || {};
    if (opp.isNewThisWeek === true || opp.weeklyDeltaState === "NEW") return "NEW";
    return opp.weeklyDeltaState || "";
  }

  function weeklyDeltaPillHtml(opp) {
    var state = weeklyDeltaStateOf(opp);
    if (!state || state === "UNCHANGED" || state === "NOT_IN_CURRENT") return "";
    var map = {
      NEW: { label: "NEW", cls: "gdi-pill gdi-pill-delta gdi-pill-delta-new" },
      UPDATED: {
        label: "UPDATED",
        cls: "gdi-pill gdi-pill-delta gdi-pill-delta-updated",
      },
      REACTIVATED: {
        label: "REACTIVATED",
        cls: "gdi-pill gdi-pill-delta gdi-pill-delta-reactivated",
      },
    };
    var m = map[state];
    if (!m) return "";
    return (
      '<span class="' +
      m.cls +
      '" title="' +
      esc(m.label) +
      ' this week"><span class="gdi-pill-delta-dot" aria-hidden="true"></span>' +
      esc(m.label) +
      "</span>"
    );
  }

  function weeklyDeltaMetaLine(opp) {
    opp = opp || {};
    var state = weeklyDeltaStateOf(opp);
    var iso =
      state === "NEW"
        ? opp.firstSeenAt
        : opp.lastMaterialChangeAt || opp.lastSeenAt || null;
    if (!iso || (state !== "NEW" && state !== "UPDATED" && state !== "REACTIVATED")) {
      return "";
    }
    var label =
      state === "NEW" ? "Added" : state === "REACTIVATED" ? "Reactivated" : "Updated";
    var d = formatShortDate(iso);
    if (!d) return "";
    return (
      '<div class="gdi-tile-delta-meta">' + esc(label + " " + d) + "</div>"
    );
  }

  function weeklyDeltaHeaderSummaryHtml(counts) {
    counts = counts || {};
    var parts = [];
    if (counts.newThisWeek > 0) {
      parts.push(counts.newThisWeek + " new this week");
    }
    if (counts.updated > 0) {
      parts.push(counts.updated + " updated");
    }
    if (counts.reactivated > 0) {
      parts.push(counts.reactivated + " reactivated");
    }
    if (!parts.length) return "";
    return (
      '<div class="gdi-weekly-summary" role="status">' +
      esc(parts.join(" · ")) +
      "</div>"
    );
  }

  function weeklyDeltaPresetHtml(opts) {
    opts = opts || {};
    var active = opts.active != null ? opts.active : "";
    var counts = opts.counts || {};
    var presets = [
      { value: "", label: "All" },
      { value: "NEW", label: "New This Week" },
      { value: "UPDATED", label: "Updated" },
      { value: "REACTIVATED", label: "Reactivated" },
      { value: "HIGH_PRIORITY", label: "High Priority" },
    ];
    return (
      '<div class="chain-scale-legend gdi-browse-legend" role="group" aria-label="Weekly change filters">' +
      "<span>This Week</span>" +
      presets
        .map(function (p) {
          var n =
            p.value === ""
              ? counts.all != null
                ? counts.all
                : 0
              : counts[p.value] != null
                ? counts[p.value]
                : 0;
          var isActive = active === p.value;
          var noRecords = p.value !== "" && n === 0;
          return (
            '<button type="button" class="chain-scale-legend-item' +
            (isActive ? " active" : "") +
            (noRecords ? " no-records" : "") +
            '" data-gdi-weekly="' +
            esc(p.value) +
            '" aria-pressed="' +
            isActive +
            '"><span class="chain-scale-legend-label">' +
            esc(presetLabelWithCount(p.label, n)) +
            "</span></button>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function countByWeeklyDelta(rows) {
    var out = { NEW: 0, UPDATED: 0, REACTIVATED: 0, HIGH_PRIORITY: 0, all: 0 };
    (rows || []).forEach(function (o) {
      out.all += 1;
      var st = weeklyDeltaStateOf(o);
      if (st === "NEW") out.NEW += 1;
      if (st === "UPDATED") out.UPDATED += 1;
      if (st === "REACTIVATED") out.REACTIVATED += 1;
      if (o.priority === "HIGH_PRIORITY") out.HIGH_PRIORITY += 1;
    });
    return out;
  }

  function weeklyDeltaHeaderCounts(rows) {
    var c = countByWeeklyDelta(rows);
    return {
      newThisWeek: c.NEW,
      updated: c.UPDATED,
      reactivated: c.REACTIVATED,
    };
  }

  function priorityToneClass(priority) {
    if (priority === "HIGH_PRIORITY") return "gdi-priority-high";
    if (priority === "MEDIUM_PRIORITY") return "gdi-priority-med";
    return "gdi-priority-watch";
  }

  /**
   * Priority legend — swatches match tile border colors.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function priorityPresetHtml(opts) {
    opts = opts || {};
    var active = opts.active != null ? opts.active : "";
    var counts = opts.counts || {};
    var presets = [
      { value: "", label: "All", swatch: null },
      {
        value: "HIGH_PRIORITY",
        label: "High",
        swatch: PRIORITY_SWATCH.HIGH_PRIORITY,
      },
      {
        value: "MEDIUM_PRIORITY",
        label: "Medium",
        swatch: PRIORITY_SWATCH.MEDIUM_PRIORITY,
      },
      {
        value: "WATCHLIST",
        label: "Watch",
        swatch: PRIORITY_SWATCH.WATCHLIST,
      },
    ];
    return (
      '<div class="chain-scale-legend gdi-browse-legend" role="group" aria-label="Priority filters">' +
      "<span>Priority</span>" +
      presets
        .map(function (p) {
          var n = counts[p.value] != null ? counts[p.value] : 0;
          var isActive = active === p.value;
          var noRecords = p.value !== "" && n === 0;
          return (
            '<button type="button" class="chain-scale-legend-item' +
            (isActive ? " active" : "") +
            (noRecords ? " no-records" : "") +
            '" data-gdi-priority="' +
            esc(p.value) +
            '" aria-pressed="' +
            isActive +
            '">' +
            (p.swatch
              ? '<div class="chain-scale-legend-swatch" style="background:' +
                esc(p.swatch) +
                ';"></div>'
              : "") +
            '<span class="chain-scale-legend-label">' +
            esc(presetLabelWithCount(p.label, n)) +
            "</span></button>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  /**
   * Action Window legend — booking-window presets with opportunity counts.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function actionPresetHtml(opts) {
    opts = opts || {};
    var active = opts.active != null ? opts.active : "";
    var counts = opts.counts || {};
    var presets = [
      { value: "", label: "All", swatch: null },
      {
        value: "CONTACT_NOW",
        label: "Pursue Now",
        swatch: ACTION_SWATCH.CONTACT_NOW,
      },
      {
        value: "QUALIFY_NOW",
        label: "Qualify",
        swatch: ACTION_SWATCH.QUALIFY_NOW,
      },
      { value: "WATCH", label: "Watch", swatch: ACTION_SWATCH.WATCH },
      {
        value: "TOO_EARLY",
        label: "Watch - Too Early",
        swatch: ACTION_SWATCH.TOO_EARLY,
      },
    ];
    return (
      '<div class="chain-scale-legend gdi-browse-legend" role="group" aria-label="Action Window filters">' +
      "<span>Action Window</span>" +
      presets
        .map(function (p) {
          var n = counts[p.value] != null ? counts[p.value] : 0;
          var isActive = active === p.value;
          var noRecords = p.value !== "" && n === 0;
          return (
            '<button type="button" class="chain-scale-legend-item' +
            (isActive ? " active" : "") +
            (noRecords ? " no-records" : "") +
            '" data-gdi-booking="' +
            esc(p.value) +
            '" aria-pressed="' +
            isActive +
            '">' +
            (p.swatch
              ? '<div class="chain-scale-legend-swatch" style="background:' +
                esc(p.swatch) +
                ';"></div>'
              : "") +
            '<span class="chain-scale-legend-label">' +
            esc(presetLabelWithCount(p.label, n)) +
            "</span></button>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  /**
   * Brand Explorer results-toolbar markup + List/Tiles toggle.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function resultsToolbarHtml(opts) {
    opts = opts || {};
    var shown = opts.shown != null ? opts.shown : 0;
    var total = opts.total != null ? opts.total : 0;
    var sort = opts.sort || "priority";
    var viewMode = opts.viewMode === "tiles" ? "tiles" : "list";
    var noun = opts.noun || "Opportunities";
    var exportHref = opts.exportHref || "";
    return (
      '<div class="results-toolbar gdi-browse-toolbar">' +
      '<div class="results-count" aria-live="polite">Showing <strong>' +
      esc(shown) +
      "</strong> of <strong>" +
      esc(total) +
      "</strong> " +
      esc(noun) +
      "</div>" +
      '<div class="results-toolbar-sort">' +
      (exportHref
        ? '<a class="gdi-export-btn" id="gdiExportCsvBtn" href="' +
          esc(exportHref) +
          '" download>Export CSV</a>'
        : '<button type="button" class="gdi-export-btn" id="gdiExportCsvBtn">Export CSV</button>') +
      '<button type="button" class="sort-icon" id="gdiSortDirBtn" title="Toggle sort direction" aria-label="Toggle sort direction">⇅</button>' +
      '<div class="filter-group filter-group-sort">' +
      '<select class="filter-select sort-select" id="gdiSortSelect" aria-label="Sort opportunities">' +
      SORT_OPTIONS.map(function (o) {
        return (
          '<option value="' +
          esc(o.value) +
          '"' +
          (sort === o.value ? " selected" : "") +
          ">" +
          esc(o.label) +
          "</option>"
        );
      }).join("") +
      "</select></div>" +
      '<div class="gdi-view-toggle" role="group" aria-label="Layout">' +
      '<button type="button" class="gdi-view-toggle__btn' +
      (viewMode === "list" ? " is-active" : "") +
      '" data-gdi-view="list" aria-pressed="' +
      (viewMode === "list") +
      '">List</button>' +
      '<button type="button" class="gdi-view-toggle__btn' +
      (viewMode === "tiles" ? " is-active" : "") +
      '" data-gdi-view="tiles" aria-pressed="' +
      (viewMode === "tiles") +
      '">Tiles</button>' +
      "</div></div></div>"
    );
  }

  function filterOpportunities(rows, filters) {
    filters = filters || {};
    rows = (rows || []).slice();
    if (filters.priority) {
      rows = rows.filter(function (o) {
        return o.priority === filters.priority;
      });
    }
    if (filters.segment) {
      rows = rows.filter(function (o) {
        return o.segment === filters.segment;
      });
    }
    if (filters.booking) {
      rows = rows.filter(function (o) {
        return o.bookingWindowStatus === filters.booking;
      });
    }
    if (filters.territory) {
      rows = rows.filter(function (o) {
        return o.demandTerritoryFit === filters.territory;
      });
    }
    if (filters.weekly) {
      if (filters.weekly === "HIGH_PRIORITY") {
        rows = rows.filter(function (o) {
          return o.priority === "HIGH_PRIORITY";
        });
      } else {
        rows = rows.filter(function (o) {
          return weeklyDeltaStateOf(o) === filters.weekly;
        });
      }
    }
    return rows;
  }

  function sortOpportunities(rows, sortKey, sortDir) {
    var key = sortKey || "priority";
    var dir = sortDir == null ? 1 : sortDir;
    rows = (rows || []).slice();
    rows.sort(function (a, b) {
      var av = a[key];
      var bv = b[key];
      if (key === "priority") {
        av = PRIORITY_RANK[a.priority] != null ? PRIORITY_RANK[a.priority] : 9;
        bv = PRIORITY_RANK[b.priority] != null ? PRIORITY_RANK[b.priority] : 9;
        return (av - bv) * dir;
      }
      if (key === "hotelFitScore" || key === "evidenceConfidence") {
        av = typeof av === "number" ? av : -1;
        bv = typeof bv === "number" ? bv : -1;
        return (bv - av) * dir;
      }
      if (key === "eventStartDate") {
        av = a.eventStartDate || a.eventTiming || "";
        bv = b.eventStartDate || b.eventTiming || "";
        return String(av).localeCompare(String(bv)) * dir;
      }
      if (key === "title") {
        return String(a.title || "").localeCompare(String(b.title || "")) * dir;
      }
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av || "").localeCompare(String(bv || "")) * dir;
    });
    return rows;
  }

  function opportunityToBriefItem(o) {
    o = o || {};
    return {
      opportunityId: o.id,
      title: o.title,
      organizationName: o.organizationName,
      segment: o.segment,
      eventStartDate: o.eventStartDate || null,
      eventEndDate: o.eventEndDate || null,
      eventTiming: eventDateRangeLabel(
        o.eventStartDate,
        o.eventEndDate,
        o.eventTiming
      ),
      hotelFitScore: o.hotelFitScore,
      evidenceConfidence: o.evidenceConfidence,
      summaryWhat: o.summaryWhat,
      whyNow: o.whyNow,
      recommendedNextStep: o.recommendedAction || o.recommendedNextStep,
      primaryContact: o.primaryContact,
      opportunityQualificationLabel:
        o.opportunityQualificationLabel || o.opportunityQualification,
      priority: o.priority,
      bookingWindowStatus: o.bookingWindowStatus,
    };
  }

  /**
   * Brand Explorer brand-card shell for opportunity tiles.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function opportunityTileHtml(it, linked, activeFilters) {
    linked = linked || {};
    activeFilters = activeFilters || {};
    var priority = linked.priority || it.priority || "WATCHLIST";
    var tone = priorityToneClass(priority);
    var id = it.opportunityId || it.id;
    var title = scrubEventDatesFromText(it.title || "") || "—";
    var org = it.organizationName || linked.organizationName || "—";
    var segment = it.segment || linked.segment || "";
    var bookingStatus =
      linked.bookingWindowStatus || it.bookingWindowStatus || "";
    var summary = trunc(
      scrubEventDatesFromText(it.summaryWhat || linked.summaryWhat || ""),
      160
    );
    var contact = linked.primaryContact || it.primaryContact || null;
    var contactName =
      contact && contact.name && contact.name !== "UNKNOWN" ? contact.name : "";
    var contactEmail = contact && contact.email ? contact.email : "";
    var contactPhone = contact && contact.phone ? contact.phone : "";
    var contactBits = [];
    if (contactName) {
      contactBits.push(
        '<div class="brand-card__contact-name">' + esc(contactName) + "</div>"
      );
    }
    if (contactEmail) {
      contactBits.push(
        '<div class="brand-card__contact-line">' + esc(contactEmail) + "</div>"
      );
    }
    if (contactPhone) {
      contactBits.push(
        '<div class="brand-card__contact-line">' + esc(contactPhone) + "</div>"
      );
    }
    if (!contactBits.length) {
      contactBits.push(
        '<div class="brand-card__contact-line brand-card__contact-line--empty">No primary contact</div>'
      );
    }
    return (
      '<article class="brand-card brand-card--gdi-opp ' +
      tone +
      '" data-open="' +
      esc(id) +
      '" role="button" tabindex="0">' +
      '<div class="brand-card__header brand-card__header--no-logo">' +
      '<div class="brand-card__info">' +
      '<div class="brand-card__name">' +
      esc(title) +
      "</div>" +
      '<div class="brand-card__pills gdi-tile-pills">' +
      weeklyDeltaPillHtml(linked) +
      tileActionPill(bookingStatus, priority, activeFilters) +
      tileEventDatePill(it, linked) +
      commercialProgressionCompactPill(
        linked.commercialProgression || it.commercialProgression
      ) +
      "</div>" +
      weeklyDeltaMetaLine(linked) +
      '<div class="brand-card__meta">' +
      esc(org) +
      (segment ? " · " + esc(String(segment).toUpperCase()) : "") +
      "</div></div></div>" +
      (summary
        ? '<div class="brand-card__description">' + esc(summary) + "</div>"
        : "") +
      '<div class="brand-card__footer brand-card__footer--split">' +
      '<div class="brand-card__contact">' +
      contactBits.join("") +
      "</div>" +
      '<button type="button" class="brand-card__more-btn" data-open="' +
      esc(id) +
      '">View Details</button></div></article>'
    );
  }

  /**
   * List/Tiles grid of opportunity cards (same tile content; layout differs).
   * Classification: REUSABLE_PRODUCT_UI
   */
  function opportunityCardsGridHtml(rows, viewMode, activeFilters) {
    var mode = viewMode === "tiles" ? "tiles" : "list";
    var cards = (rows || [])
      .map(function (o) {
        return opportunityTileHtml(
          opportunityToBriefItem(o),
          o,
          activeFilters
        );
      })
      .join("");
    return (
      '<div class="results-grid gdi-results-grid' +
      (mode === "list" ? " gdi-results-grid--list" : "") +
      '" data-gdi-view-mode="' +
      mode +
      '">' +
      cards +
      "</div>"
    );
  }

  /**
   * Full browse chrome: Action Window + Priority presets + toolbar.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function opportunityBrowseChromeHtml(opts) {
    opts = opts || {};
    return (
      '<div class="gdi-browse">' +
      (opts.weeklySummaryHtml ||
        weeklyDeltaHeaderSummaryHtml(opts.weeklyHeaderCounts || {})) +
      '<div class="gdi-browse-presets">' +
      priorityPresetHtml({
        active: opts.activePriority != null ? opts.activePriority : "",
        counts: opts.priorityCounts || {},
      }) +
      actionPresetHtml({
        active: opts.activeBooking != null ? opts.activeBooking : "",
        counts: opts.actionCounts || {},
      }) +
      "</div>" +
      resultsToolbarHtml({
        shown: opts.shown,
        total: opts.total,
        sort: opts.sort,
        viewMode: opts.viewMode,
        noun: opts.noun,
        exportHref: opts.exportHref || "",
      }) +
      "</div>"
    );
  }

  function contactSummaryHtml(contact, opts) {
    opts = opts || {};
    if (!contact) {
      return '<div class="gdi-contact-summary gdi-contact-summary--empty">No primary contact resolved.</div>';
    }
    var lines = [];
    lines.push(
      '<div class="gdi-contact-summary__name">' + esc(contact.name || "—") + "</div>"
    );
    if (contact.role || contact.title) {
      lines.push(
        '<div class="gdi-contact-summary__role">' +
          esc(contact.role || contact.title) +
          "</div>"
      );
    }
    if (contact.email) {
      lines.push(
        '<div class="gdi-contact-summary__field">' + esc(contact.email) + "</div>"
      );
    }
    if (contact.phone) {
      lines.push(
        '<div class="gdi-contact-summary__field">' +
          esc(contact.phone) +
          (contact.phoneTypeLabel ? " · " + esc(contact.phoneTypeLabel) : "") +
          "</div>"
      );
    }
    return '<div class="gdi-contact-summary">' + lines.join("") + "</div>";
  }

  /** Action-first Weekly Brief card. Classification: REUSABLE_PRODUCT_UI */
  function weeklyBriefCardHtml(it, linked) {
    linked = linked || {};
    var priority = linked.priority || it.priority || "WATCHLIST";
    var tone =
      priority === "HIGH_PRIORITY"
        ? "high"
        : priority === "MEDIUM_PRIORITY"
          ? "med"
          : "watch";
    var contact = linked.primaryContact || it.primaryContact || null;
    var qual =
      linked.opportunityQualificationLabel ||
      linked.opportunityQualification ||
      it.opportunityQualificationLabel ||
      it.opportunityQualification ||
      "—";
    var openId = it.opportunityId || it.id;
    var eventDate = eventDateRangeLabel(
      linked.eventStartDate || it.eventStartDate,
      linked.eventEndDate || it.eventEndDate,
      it.eventTiming || linked.eventTiming
    );
    return (
      '<article class="gdi-brief-card gdi-brief-card--action gdi-brief-card--' +
      tone +
      '">' +
      '<div class="gdi-brief-card__top">' +
      priorityPill(priority) +
      commercialProgressionCompactPill(
        linked.commercialProgression || it.commercialProgression
      ) +
      '<div class="gdi-brief-card__heading">' +
      "<h3>" +
      esc(scrubEventDatesFromText(it.title) || it.title || "—") +
      "</h3>" +
      '<div class="gdi-brief-card__date">' +
      esc(eventDate) +
      "</div></div></div>" +
      '<div class="gdi-brief-card__secondary">' +
      "<span>" +
      esc(it.organizationName || "—") +
      "</span>" +
      (it.segment ? "<span>·</span><span>" + esc(it.segment) + "</span>" : "") +
      '<span class="gdi-score-chip">Hotel Fit <strong>' +
      esc(it.hotelFitScore != null ? it.hotelFitScore : "—") +
      "</strong></span>" +
      '<span class="gdi-score-chip gdi-score-chip--quiet">Qualification <strong>' +
      esc(qual) +
      "</strong></span>" +
      '<span class="gdi-score-chip gdi-score-chip--quiet">Evidence <strong>' +
      esc(it.evidenceConfidence != null ? it.evidenceConfidence : "—") +
      "</strong></span></div>" +
      '<div class="gdi-brief-card__body">' +
      '<div class="gdi-brief-block"><div class="gdi-meta-label">Why Now</div><p>' +
      esc(it.whyNow || "—") +
      '</p></div><div class="gdi-brief-block gdi-brief-block--action"><div class="gdi-meta-label">Recommended Action</div><p>' +
      esc(it.recommendedNextStep || "—") +
      "</p></div></div>" +
      '<div class="gdi-brief-card__footer">' +
      contactSummaryHtml(contact) +
      '<button type="button" class="gdi-btn gdi-btn-primary" data-open="' +
      esc(openId) +
      '">View Details</button></div></article>'
    );
  }

  function formatShortDate(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return "";
      return d.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    } catch (e) {
      return "";
    }
  }

  function fitLabel(key) {
    var map = {
      physicalFit: "Physical Fit",
      geographyFit: "Demand Territory Fit",
      timing: "Timing / Winnability",
      commercialValue: "Commercial Potential",
      historicalFit: "Historical Hotel / Brand Fit",
      competitiveAccessibility: "Competitive Accessibility",
      contactability: "Contactability",
    };
    return map[key] || key;
  }

  /**
   * Customer-facing commercial progression block (auth + share parity).
   * Never shows raw enums / event IDs / causal jargon.
   */
  function commercialProgressionHtml(progression) {
    if (!progression || !progression.currentStatusLabel) return "";
    var bits = [];
    bits.push(
      '<p class="gdi-progression-status">Current status: <strong>' +
        esc(progression.currentStatusLabel) +
        "</strong></p>"
    );
    var hist = [];
    if (progression.latestActionLabel) {
      var actionLine = "Latest activity: " + progression.latestActionLabel;
      var ad = formatShortDate(progression.latestActionDate);
      if (ad) actionLine += " · " + ad;
      hist.push(actionLine);
    }
    if (progression.latestOutcomeLabel) {
      var outLine = "Latest result: " + progression.latestOutcomeLabel;
      var od = formatShortDate(progression.latestOutcomeDate);
      if (od) outLine += " · " + od;
      hist.push(outLine);
    }
    if (hist.length) {
      bits.push(
        '<p class="gdi-progression-history">' +
          hist
            .map(function (line) {
              return esc(line);
            })
            .join("<br>") +
          "</p>"
      );
    }
    return (
      '<div class="gdi-commercial-progression" id="gdiCommercialProgression">' +
      bits.join("") +
      "</div>"
    );
  }

  function commercialProgressionCompactPill(progression) {
    if (!progression || !progression.compactStatusLabel) return "";
    return (
      '<span class="gdi-badge gdi-badge--progression">' +
      esc(progression.compactStatusLabel) +
      "</span>"
    );
  }

  function whoShouldSalesContactHtml(o, whoFn) {
    if (typeof whoFn === "function") return whoFn(o);
    var c = o.primaryContact;
    if (!c) return "<p>No usable contact resolved yet.</p>";
    var primaryKind =
      c.contactKind === "FUNCTIONAL"
        ? "FUNCTIONAL CONTACT"
        : c.functionalEntity
          ? "FUNCTIONAL CONTACT"
          : "PRIMARY CONTACT";
    var backups = (o.backupContacts || [])
      .map(function (b) {
        var kind =
          b.contactKind === "FUNCTIONAL_BACKUP" || b.functionalEntity
            ? "Functional"
            : "Backup";
        return (
          "<li><span class=\"gdi-contact-kind\">" +
          esc(kind) +
          "</span> · <strong>" +
          esc(b.name || "—") +
          "</strong> · " +
          esc(b.role || "") +
          " · " +
          esc(b.email || "") +
          (b.phone ? " · " + esc(b.phone) : "") +
          "</li>"
        );
      })
      .join("");
    return (
      '<p class="gdi-contact-kind-label"><strong>' +
      esc(primaryKind) +
      "</strong></p>" +
      contactSummaryHtml(c) +
      "<p><strong>Why this contact:</strong> " +
      esc(o.whoPrimaryReason || c.whyThisContact || o.whyThisContact || "—") +
      "</p>" +
      (o.contactGradeLabel || c.contactGradeLabel
        ? '<p class="gdi-contact-quality-quiet">Contact quality: ' +
          esc(o.contactGradeLabel || c.contactGradeLabel) +
          (o.contactConfidence != null || c.contactConfidence != null
            ? " · Confidence " + esc(o.contactConfidence ?? c.contactConfidence)
            : "") +
          "</p>"
        : "") +
      (backups
        ? "<h4>Backup / functional contacts</h4><ul>" + backups + "</ul>"
        : "")
    );
  }

  /**
   * Intelligence-to-action detail framework.
   * WHAT WE SEE / WHY IT MATTERS / WHY NOW / RECOMMENDED ACTION / CONTACT / EVIDENCE / VALIDATION
   */
  function intelligenceDetailHtml(o, audit, extras) {
    extras = extras || {};
    audit = audit || {};
    var comps = (audit.hotelFit && audit.hotelFit.components) || {};
    var labels = o.hotelFitComponentLabels || {};
    var sources = (o.sources || [])
      .map(function (s) {
        var link =
          s.url && isSafeHttpUrl(s.url)
            ? '<a href="' + esc(s.url) + '" target="_blank" rel="noopener noreferrer">' + esc(s.name) + "</a>"
            : esc(s.name || s.url || "");
        return (
          "<li>" + link + (s.supportsFact ? " — " + esc(s.supportsFact) : "") + "</li>"
        );
      })
      .join("");

    var thesis =
      o.hotelDemandThesis || o.hotelOpportunityThesis || o.bethesdaWinThesis || "Not publicly found";
    var dateLabel =
      o.eventDateDisplay ||
      eventDateRangeLabel(o.eventStartDate, o.eventEndDate, "Date not yet confirmed");
    var peakLabel = fmtVal(o.peakRooms ?? o.estimatedPeakRooms);
    var attLabel = fmtVal(o.attendance ?? o.estimatedAttendance);
    if (!peakLabel || peakLabel === "—") peakLabel = "Not publicly found";
    if (!attLabel || attLabel === "—") attLabel = "Not publicly found";
    var relatedN = o.relatedOpportunityCount || (o.relatedOpportunityIds || []).length || 0;
    var relatedHtml =
      relatedN > 0
        ? '<p class="gdi-related"><strong>Related opportunities:</strong> ' +
          esc(relatedN) +
          (o.eventSeriesId
            ? " · series <code>" + esc(o.eventSeriesId) + "</code>"
            : "") +
          "</p>"
        : "";
    var actionBasis = o.actionBasis
      ? "<p><strong>Why this action:</strong> " + esc(o.actionBasis) + "</p>"
      : "";
    var nextCycle =
      o.nextCycleDisplay
        ? "<p><strong>Cycle:</strong> " + esc(o.nextCycleDisplay) + "</p>"
        : "";

    var scoreGrid =
      '<div class="gdi-score-grid">' +
      '<div class="gdi-score-cell"><div class="n">' +
      esc(o.hotelFitScore) +
      '</div><div class="l">Hotel Fit</div></div>' +
      Object.keys(comps)
        .map(function (k) {
          return (
            '<div class="gdi-score-cell"><div class="n">' +
            esc(comps[k]) +
            '</div><div class="l">' +
            esc(labels[k] || fitLabel(k)) +
            "</div></div>"
          );
        })
        .join("") +
      "</div>";

    return (
      '<div class="gdi-detail-framework">' +
      commercialProgressionHtml(o.commercialProgression) +
      '<section class="gdi-section gdi-section--see"><h3>Event / Opportunity</h3>' +
      "<p>" +
      esc(o.summaryWhat || o.title || "—") +
      "</p>" +
      '<div class="gdi-detail-meta-row">' +
      "<span><strong>Date</strong> " +
      esc(dateLabel) +
      (o.eventDateStatus ? " · " + esc(o.eventDateStatus) : "") +
      "</span>" +
      "<span><strong>Type</strong> " +
      esc(o.opportunityTypeLabel || o.opportunityType || "—") +
      "</span>" +
      "<span><strong>Venue / sourcing</strong> " +
      esc(o.venueSourcingStatusLabel || o.venueSourcingStatus || "Unknown") +
      "</span>" +
      "<span><strong>Room demand</strong> " +
      esc(o.roomDemandLive || o.roomDemandStatusLabel || o.roomDemandStatus || "Unknown") +
      "</span></div>" +
      nextCycle +
      "<p>" +
      esc(o.venueSourcingRationale || "") +
      "</p>" +
      "<p>Attendance: " +
      esc(attLabel) +
      (o.attendanceStatus ? " (" + esc(o.attendanceStatus) + ")" : "") +
      " · Peak rooms: " +
      esc(peakLabel) +
      (o.peakRoomsStatus ? " (" + esc(o.peakRoomsStatus) + ")" : "") +
      "</p>" +
      "<p><strong>Room-demand thesis:</strong> <em>" +
      esc(thesis) +
      "</em></p>" +
      relatedHtml +
      "</section>" +
      '<section class="gdi-section gdi-section--matters"><h3>Why It Matters</h3><p>' +
      esc(o.summaryWhyMatters || "—") +
      "</p><p>" +
      esc(o.summaryWhyHotel || o.fitExplanation || "") +
      "</p>" +
      scoreGrid +
      "<p>" +
      esc((audit.hotelFit && audit.hotelFit.explanation) || o.fitExplanation || "") +
      "</p>" +
      '<div class="gdi-signal-row">' +
      "<div><span class=\"gdi-meta-label\">Hotel Fit</span><strong>" +
      esc(o.hotelFitScore) +
      "</strong></div>" +
      "<div><span class=\"gdi-meta-label\">Opportunity Qualification</span><strong>" +
      esc(o.opportunityQualificationLabel || o.opportunityQualification || "—") +
      "</strong></div>" +
      "<div><span class=\"gdi-meta-label\">Evidence Confidence</span><strong>" +
      esc(o.evidenceConfidence) +
      "</strong></div></div></section>" +
      '<section class="gdi-section gdi-section--now"><h3>Why Now</h3><p><strong>' +
      esc(actionStatusLabel(o.bookingWindowStatus) || o.bookingWindowLabel || "—") +
      "</strong></p><p>" +
      esc(o.whyNow || "—") +
      "</p><p>Location: " +
      esc(o.eventLocationStatusLabel || o.eventLocationStatus || "—") +
      " · " +
      esc(o.eventLocationSummary || o.destinationStatus || "—") +
      "</p><p>Demand Territory: " +
      esc(o.demandTerritoryFitLabel || "—") +
      "</p></section>" +
      '<section class="gdi-section gdi-section--action"><h3>Suggested Action</h3><p>' +
      esc(o.recommendedAction || "—") +
      "</p>" +
      actionBasis +
      "</section>" +
      '<section class="gdi-section gdi-section--contact"><h3>Contact</h3>' +
      whoShouldSalesContactHtml(o, extras.whoContactHtml) +
      (o.contactPathClass
        ? '<p class="gdi-contact-quality-quiet">Contact path: ' +
          esc(o.contactPathClass) +
          "</p>"
        : "") +
      "</section>" +
      '<section class="gdi-section gdi-section--evidence"><h3>Sources</h3>' +
      "<p>" +
      esc(o.evidenceConfidenceExplanation || audit.evidenceConfidenceExplanation || "") +
      "</p>" +
      "<p>Competitive context — STR: " +
      esc(o.likelyStrCompetitor || "—") +
      " · Group: " +
      esc(o.likelyGroupCompetitor || "—") +
      "</p><ul>" +
      (sources || "<li>Not publicly found</li>") +
      "</ul></section>" +
      (extras.validationHtml
        ? '<section class="gdi-section gdi-section--validation gdi-validation-panel"><h3>Validation / Action / Outcome</h3>' +
          extras.validationHtml +
          "</section>"
        : "") +
      "</div>"
    );
  }

  function assertNoHotelHardcode(sourceText) {
    // Markers split so this helper file itself does not trip the gate.
    var hotelMarkers = [
      "Bethesda" + " Marriott",
      "Bethesda" + " / Montgomery Core",
      "Why " + "Bethesda",
      "DMV demand" + " territory",
    ];
    var hits = [];
    hotelMarkers.forEach(function (m) {
      if (sourceText.indexOf(m) !== -1) hits.push(m);
    });
    return { ok: hits.length === 0, hits: hits };
  }

  var CUSTOMER_VALIDATION_ENUMS = {
    familiarityStatus: [
      "NEVER_SEEN_BEFORE",
      "ALREADY_KNOWN",
      "ACTIVELY_PURSUING",
      "PREVIOUSLY_PURSUED_LOST",
      "BOOKED_WON",
      "NOT_RELEVANT",
      "UNSURE",
    ],
    familiarityLabels: {
      NEVER_SEEN_BEFORE: "Never seen before",
      ALREADY_KNOWN: "Already known",
      ACTIVELY_PURSUING: "Actively pursuing",
      PREVIOUSLY_PURSUED_LOST: "Previously pursued / lost",
      BOOKED_WON: "Booked / won",
      NOT_RELEVANT: "Not relevant",
      UNSURE: "Unsure",
    },
    commercialValue: [
      "WORTH_PURSUING_NOW",
      "WORTH_WATCHING",
      "NOT_WORTH_PURSUING",
    ],
    commercialValueLabels: {
      WORTH_PURSUING_NOW: "Worth pursuing now",
      WORTH_WATCHING: "Worth watching",
      NOT_WORTH_PURSUING: "Not worth pursuing",
    },
    contactPerson: [
      "RIGHT_PERSON",
      "RELEVANT_NOT_DECISION_MAKER",
      "WRONG_PERSON",
      "UNSURE",
    ],
    contactPersonLabels: {
      RIGHT_PERSON: "Right person",
      RELEVANT_NOT_DECISION_MAKER: "Relevant but not decision maker",
      WRONG_PERSON: "Wrong person",
      UNSURE: "Unsure",
    },
    emailAssessment: ["USEFUL", "WRONG", "GENERIC", "NOT_TESTED"],
    emailAssessmentLabels: {
      USEFUL: "Useful",
      WRONG: "Wrong",
      GENERIC: "Generic",
      NOT_TESTED: "Not tested",
    },
    phoneAssessment: [
      "DIRECT_USABLE",
      "MAIN_SHARED_LINE",
      "WRONG",
      "NOT_TESTED",
    ],
    phoneAssessmentLabels: {
      DIRECT_USABLE: "Direct / usable",
      MAIN_SHARED_LINE: "Main / shared line",
      WRONG: "Wrong",
      NOT_TESTED: "Not tested",
    },
  };

  function enumSelectOptions(values, labels, selected) {
    var opts = '<option value="">Select…</option>';
    (values || []).forEach(function (val) {
      var lab = (labels && labels[val]) || val;
      opts +=
        '<option value="' +
        esc(val) +
        '"' +
        (selected === val ? " selected" : "") +
        ">" +
        esc(lab) +
        "</option>";
    });
    return opts;
  }

  function shareCustomerValidationFormHtml(o, enums, permissionContext) {
    if (!permissionContext || !permissionContext.canValidate) {
      return (
        '<p class="gdi-lede">Validation is not enabled on this access link. ' +
        "Request an updated Dealality link if your team needs to record hotel validation.</p>"
      );
    }
    var v = (o && o.shareValidation) || {};
    var e = enums || {};
    return (
      '<p class="gdi-lede">Fast review for sales. Stored for learning — does not overwrite research evidence.</p>' +
      '<div class="gdi-feedback gdi-hotel-validation-form" id="gdiShareValidation" data-gdi-validation-mode="SHARE_CUSTOMER">' +
      "<label>Familiarity / Status<select id=\"gdiSvFamiliarity\">" +
      enumSelectOptions(e.familiarityStatus, e.familiarityLabels, v.familiarityStatus) +
      "</select></label>" +
      "<label>Commercial Value<select id=\"gdiSvCommercial\">" +
      enumSelectOptions(e.commercialValue, e.commercialValueLabels, v.commercialValue) +
      "</select></label>" +
      "<label>Contact Person<select id=\"gdiSvPerson\">" +
      enumSelectOptions(e.contactPerson, e.contactPersonLabels, v.contactPersonAssessment) +
      "</select></label>" +
      "<label>Email Quality<select id=\"gdiSvEmail\">" +
      enumSelectOptions(e.emailAssessment, e.emailAssessmentLabels, v.emailAssessment) +
      "</select></label>" +
      "<label>Phone Quality<select id=\"gdiSvPhone\">" +
      enumSelectOptions(e.phoneAssessment, e.phoneAssessmentLabels, v.phoneAssessment) +
      "</select></label>" +
      '<textarea id="gdiSvNote" placeholder="Optional note" rows="2">' +
      esc(v.note || "") +
      "</textarea>" +
      '<div class="gdi-validation-actions">' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiSvSave" data-id="' +
      esc((o && o.id) || "") +
      '">Save</button>' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiSvSaveNext" data-id="' +
      esc((o && o.id) || "") +
      '">Save + Next</button>' +
      '<span class="gdi-validation-status" id="gdiSvStatus"></span></div></div>'
    );
  }

  /**
   * Shared customer feedback lifecycle — auth + authorized share parity.
   * permissionContext: { canValidate, canRecordAction, canRecordOutcome }
   */
  function customerFeedbackLifecycleHtml(o, enums, permissionContext, progression) {
    permissionContext = permissionContext || {};
    var bits = [];
    if (progression) {
      bits.push(commercialProgressionHtml(progression));
    }
    bits.push(
      '<div class="gdi-section gdi-section--validation gdi-validation-panel gdi-hotel-feedback" data-gdi-customer-lifecycle="1">' +
        "<h3>Hotel Validation</h3>"
    );
    bits.push(shareCustomerValidationFormHtml(o, enums, permissionContext));

    if (permissionContext.canRecordAction) {
      bits.push(
        '<div class="gdi-lifecycle-step" data-gdi-lifecycle-action="1">' +
          "<h4>Action</h4>" +
          '<p class="gdi-lifecycle-q">What did the team do?</p>' +
          '<div class="gdi-feedback">' +
          '<label>Action<select id="gdiDoAction"><option value="">Select action...</option>' +
          '<option value="NOT_CONTACTED">Not Contacted</option>' +
          '<option value="PLANNED_TO_CONTACT">Planned To Contact</option>' +
          '<option value="CONTACTED">Contacted</option>' +
          '<option value="FOLLOW_UP_REQUIRED">Follow Up Required</option>' +
          '<option value="RFP_REQUESTED">RFP Requested</option>' +
          '<option value="RFP_RECEIVED">RFP Received</option>' +
          '<option value="SITE_VISIT_REQUESTED">Site Visit Requested</option>' +
          '<option value="SITE_VISIT_COMPLETED">Site Visit Completed</option>' +
          '<option value="PROPOSAL_SUBMITTED">Proposal Submitted</option>' +
          '<option value="NEGOTIATING">Negotiating</option>' +
          '<option value="NO_ACTION">No Action</option></select></label>' +
          '<div class="gdi-validation-actions">' +
          '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiDoActionSave" data-id="' +
          esc((o && o.id) || "") +
          '">Save Action</button>' +
          '<span class="gdi-validation-status" id="gdiDoActionSaveStatus" hidden></span>' +
          "</div></div></div>"
      );
    }

    if (permissionContext.canRecordOutcome) {
      bits.push(
        '<div class="gdi-lifecycle-step" data-gdi-lifecycle-outcome="1">' +
          "<h4>Outcome</h4>" +
          '<p class="gdi-lifecycle-q">What happened?</p>' +
          '<div class="gdi-feedback">' +
          '<label>Outcome<select id="gdiDoOutcome"><option value="">Select outcome...</option>' +
          '<option value="WON">Won</option>' +
          '<option value="LOST">Lost</option>' +
          '<option value="BOOKED">Booked</option>' +
          '<option value="NO_RESPONSE">No Response</option>' +
          '<option value="NOT_QUALIFIED">Not Qualified</option>' +
          '<option value="OPPORTUNITY_CLOSED">Opportunity Closed</option>' +
          '<option value="DEFERRED">Deferred</option>' +
          '<option value="ADDED_TO_SOURCED_PROPERTIES">Added to sourced properties</option>' +
          '<option value="UNKNOWN">Unknown</option></select></label>' +
          '<label id="gdiDoLossWrap" hidden>Loss reason<select id="gdiDoLoss"><option value="">Select loss reason...</option>' +
          '<option value="RATE">Rate</option>' +
          '<option value="AVAILABILITY">Availability</option>' +
          '<option value="LOCATION">Location</option>' +
          '<option value="MEETING_SPACE">Meeting Space</option>' +
          '<option value="BRAND">Brand</option>' +
          '<option value="COMPETITOR">Competitor</option>' +
          '<option value="ROOM_BLOCK">Room Block</option>' +
          '<option value="DATES">Dates</option>' +
          '<option value="NO_RESPONSE">No Response</option>' +
          '<option value="OTHER">Other</option></select></label>' +
          '<div class="gdi-validation-actions">' +
          '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiDoOutcomeSave" data-id="' +
          esc((o && o.id) || "") +
          '">Save Outcome</button>' +
          '<span class="gdi-validation-status" id="gdiDoOutcomeSaveStatus" hidden></span>' +
          "</div></div></div>"
      );
    }

    bits.push("</div>");
    return bits.join("");
  }

  /** Customer-safe inactive-link copy (shared auth/share). */
  function inactiveShareLinkHtml(message) {
    var msg =
      message ||
      "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.";
    return (
      '<div class="gdi-error gdi-share-inactive" role="alert">' +
      "<h2>Access link unavailable</h2>" +
      "<p>" +
      esc(msg) +
      "</p></div>"
    );
  }

  root.DealalityGdiUi = {
    PRIORITY_DISPLAY: PRIORITY_DISPLAY,
    ACTION_STATUS_DISPLAY: ACTION_STATUS_DISPLAY,
    SORT_OPTIONS: SORT_OPTIONS,
    esc: esc,
    isSafeHttpUrl: isSafeHttpUrl,
    fmtVal: fmtVal,
    trunc: trunc,
    scrubEventDatesFromText: scrubEventDatesFromText,
    eventDateRangeLabel: eventDateRangeLabel,
    priorityPill: priorityPill,
    actionStatusLabel: actionStatusLabel,
    hotelLocationLine: hotelLocationLine,
    statusBadgesHtml: statusBadgesHtml,
    hotelShellHtml: hotelShellHtml,
    propertyBarHtml: propertyBarHtml,
    primaryKpiHtml: primaryKpiHtml,
    secondarySummaryHtml: secondarySummaryHtml,
    buildMetricModel: buildMetricModel,
    contentTabsHtml: contentTabsHtml,
    formatTabLabel: formatTabLabel,
    GDI_MAIN_TAB: GDI_MAIN_TAB,
    countByPriority: countByPriority,
    countByActionStatus: countByActionStatus,
    facetCountByPriority: facetCountByPriority,
    facetCountByActionStatus: facetCountByActionStatus,
    reconcileBrowseFilters: reconcileBrowseFilters,
    actionPresetHtml: actionPresetHtml,
    priorityPresetHtml: priorityPresetHtml,
    resultsToolbarHtml: resultsToolbarHtml,
    filterOpportunities: filterOpportunities,
    sortOpportunities: sortOpportunities,
    opportunityToBriefItem: opportunityToBriefItem,
    opportunityTileHtml: opportunityTileHtml,
    opportunityCardsGridHtml: opportunityCardsGridHtml,
    opportunityBrowseChromeHtml: opportunityBrowseChromeHtml,
    weeklyDeltaPillHtml: weeklyDeltaPillHtml,
    weeklyDeltaPresetHtml: weeklyDeltaPresetHtml,
    weeklyThisWeekFilterHtml: weeklyThisWeekFilterHtml,
    weeklyDeltaHeaderSummaryHtml: weeklyDeltaHeaderSummaryHtml,
    weeklyDeltaHeaderCounts: weeklyDeltaHeaderCounts,
    countByWeeklyDelta: countByWeeklyDelta,
    weeklyDeltaStateOf: weeklyDeltaStateOf,
    contactSummaryHtml: contactSummaryHtml,
    weeklyBriefCardHtml: weeklyBriefCardHtml,
    intelligenceDetailHtml: intelligenceDetailHtml,
    fitLabel: fitLabel,
    assertNoHotelHardcode: assertNoHotelHardcode,
    shareCustomerValidationFormHtml: shareCustomerValidationFormHtml,
    customerFeedbackLifecycleHtml: customerFeedbackLifecycleHtml,
    CUSTOMER_VALIDATION_ENUMS: CUSTOMER_VALIDATION_ENUMS,
    commercialProgressionHtml: commercialProgressionHtml,
    commercialProgressionCompactPill: commercialProgressionCompactPill,
    formatShortDate: formatShortDate,
    inactiveShareLinkHtml: inactiveShareLinkHtml,
    enumSelectOptions: enumSelectOptions,
  };
})(typeof window !== "undefined" ? window : globalThis);
