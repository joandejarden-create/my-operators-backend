/**
 * Dealality GDI UI presentation — reusable product UI helpers.
 * Classification: REUSABLE_PRODUCT_UI
 * Hotel-specific facts must come from data/config, never hardcodes here.
 */
(function (root) {
  "use strict";

  // Labels must match Priority / Action Timing filter legends exactly.
  var PRIORITY_DISPLAY = {
    HIGH_PRIORITY: { label: "High", pillClass: "gdi-pill gdi-pill-high" },
    MEDIUM_PRIORITY: { label: "Medium", pillClass: "gdi-pill gdi-pill-med" },
    // Distinct from Action Timing "Future Watch" — commercial browse priority only
    WATCHLIST: { label: "Watch", pillClass: "gdi-pill gdi-pill-watch" },
    UNCLASSIFIED: { label: "Unclassified", pillClass: "gdi-pill gdi-pill-watch" },
  };

  /**
   * Universal Sales Action Pill V1 — middle tile pill (exactly eight values).
   * Commercial Stage remains internal; never customer-facing middle pill.
   */
  var SALES_ACTION_PILL = {
    BUILD_ACCOUNT: {
      label: "Build Account",
      pillClass: "gdi-pill gdi-pill-sales-build-account",
    },
    CONTACT_BUYER: {
      label: "Contact Buyer",
      pillClass: "gdi-pill gdi-pill-sales-contact-buyer",
    },
    WIN_ROOM_BLOCK: {
      label: "Win Room Block",
      pillClass: "gdi-pill gdi-pill-sales-win-room-block",
    },
    BUILD_VENUE_PARTNERSHIP: {
      label: "Build Venue Partnership",
      pillClass: "gdi-pill gdi-pill-sales-build-venue",
    },
    ENGAGE_PLANNER: {
      label: "Engage Planner",
      pillClass: "gdi-pill gdi-pill-sales-engage-planner",
    },
    PURSUE_CATERING: {
      label: "Pursue Catering",
      pillClass: "gdi-pill gdi-pill-sales-pursue-catering",
    },
    QUALIFY_OPPORTUNITY: {
      label: "Qualify Opportunity",
      pillClass: "gdi-pill gdi-pill-sales-qualify",
    },
    MONITOR: {
      label: "Monitor",
      pillClass: "gdi-pill gdi-pill-sales-monitor",
    },
  };

  var CANONICAL_SALES_ACTION_KEYS = {
    BUILD_ACCOUNT: 1,
    CONTACT_BUYER: 1,
    WIN_ROOM_BLOCK: 1,
    BUILD_VENUE_PARTNERSHIP: 1,
    ENGAGE_PLANNER: 1,
    PURSUE_CATERING: 1,
    QUALIFY_OPPORTUNITY: 1,
    MONITOR: 1,
  };

  /**
   * Research-to-Drawer V2 — expandable Layer 2 under Sales Brief sections.
   * Expects opportunity.researchToDrawerV2 from merge overlay / API DTO.
   */
  function researchDetailExpandableHtml(opp, detailKey, summaryLabel, helpers) {
    var h = helpers || {};
    var escFn = typeof h.esc === "function" ? h.esc : function (s) { return String(s || ""); };
    var proseFn =
      typeof h.prose === "function"
        ? h.prose
        : function (v) {
            return v == null ? "" : String(v).replace(/\s+/g, " ").trim();
          };
    var cleanListFn =
      typeof h.cleanList === "function"
        ? h.cleanList
        : function (items) {
            return Array.isArray(items)
              ? items.map(proseFn).filter(Boolean)
              : [];
          };
    var listHtmlFn =
      typeof h.listHtml === "function"
        ? h.listHtml
        : function (items) {
            if (!items || !items.length) return "";
            return (
              "<ul>" +
              items
                .map(function (t) {
                  return "<li>" + escFn(t) + "</li>";
                })
                .join("") +
              "</ul>"
            );
          };
    var v2 = opp && opp.researchToDrawerV2;
    var detail = v2 && v2.researchDetail ? v2.researchDetail : null;
    if (!detail) return "";
    var body = proseFn(detail[detailKey]);
    var extraLists = [];
    if (detailKey === "potentialBusinessForThisHotel") {
      if (cleanListFn(detail.demandAssumptions).length) {
        extraLists.push(
          "<p><strong>Assumptions / gaps</strong></p>" +
            listHtmlFn(cleanListFn(detail.demandAssumptions))
        );
      }
      if (proseFn(detail.programmeDetails)) {
        extraLists.push(
          "<p><strong>Programme detail</strong></p><p>" +
            escFn(proseFn(detail.programmeDetails)) +
            "</p>"
        );
      }
    }
    if (detailKey === "whyThisHotel" && detail.competitiveIntelligence) {
      var ci = detail.competitiveIntelligence;
      var ciBits = [];
      if (proseFn(ci.sourcingLabel)) {
        ciBits.push(
          "<li><strong>Sourcing:</strong> " + escFn(proseFn(ci.sourcingLabel)) + "</li>"
        );
      }
      if (proseFn(ci.incumbentVenue)) {
        ciBits.push(
          "<li><strong>Incumbent / venue:</strong> " +
            escFn(proseFn(ci.incumbentVenue)) +
            "</li>"
        );
      }
      if (cleanListFn(ci.historicalVenues).length) {
        ciBits.push(
          "<li><strong>Historical venues:</strong> " +
            escFn(cleanListFn(ci.historicalVenues).join("; ")) +
            "</li>"
        );
      }
      if (proseFn(ci.note)) {
        ciBits.push("<li>" + escFn(proseFn(ci.note)) + "</li>");
      }
      if (ciBits.length) {
        extraLists.push(
          "<p><strong>Competitive / sourcing</strong></p><ul>" +
            ciBits.join("") +
            "</ul>"
        );
      }
    }
    if (
      detailKey === "buyerResearch" &&
      Array.isArray(detail.buyerResearch) &&
      detail.buyerResearch.length
    ) {
      extraLists.push(
        "<ul>" +
          detail.buyerResearch
            .map(function (c) {
              return (
                "<li><strong>" +
                escFn(proseFn(c.name) || "Contact") +
                "</strong>" +
                (proseFn(c.title) ? " — " + escFn(proseFn(c.title)) : "") +
                (proseFn(c.whyRelevant)
                  ? '<div class="gdi-muted">' + escFn(proseFn(c.whyRelevant)) + "</div>"
                  : "") +
                "</li>"
              );
            })
            .join("") +
          "</ul>"
      );
      body = body || "Verified contact research retained from source investigation.";
    }
    if (!body && !extraLists.length) return "";
    return (
      '<details class="gdi-research-detail-expand" data-research-detail="' +
      escFn(detailKey) +
      '"><summary>' +
      escFn(summaryLabel || "Additional research detail") +
      '</summary><div class="gdi-research-detail-body">' +
      (body ? "<p>" + escFn(body) + "</p>" : "") +
      extraLists.join("") +
      "</div></details>"
    );
  }

  /** Internal commercial-stage map (filters / research only — not middle pill). */
  var COMMERCIAL_STAGE_PILL = {
    PURSUE_NOW: {
      label: "Pursue Now",
      pillClass: "gdi-pill gdi-pill-stage-pursue",
    },
    FUTURE_CAPTURE: {
      label: "Future Capture",
      pillClass: "gdi-pill gdi-pill-stage-future",
    },
    RELATIONSHIP_DEVELOPMENT: {
      label: "Relationship Development",
      pillClass: "gdi-pill gdi-pill-stage-relationship",
    },
    QUALIFICATION_NEEDED: {
      label: "Qualification Needed",
      pillClass: "gdi-pill gdi-pill-stage-qualify",
    },
  };

  var ACTION_STATUS_DISPLAY = {
    CONTACT_NOW: "Pursue Now",
    QUALIFY_NOW: "Needs qualification",
    WATCH: "Future Watch",
    TOO_EARLY: "Too Early",
    UNCLASSIFIED: "Unclassified",
    CLOSED: "Closed",
    NOT_ACTIONABLE: "Not actionable",
  };

  var ACTION_PILL_DISPLAY = {
    CONTACT_NOW: {
      label: "Pursue Now",
      pillClass: "gdi-pill gdi-pill-action-pursue",
    },
    QUALIFY_NOW: {
      label: "Needs qualification",
      pillClass: "gdi-pill gdi-pill-action-qualify",
    },
    WATCH: {
      label: "Future Watch",
      pillClass: "gdi-pill gdi-pill-action-watch",
    },
    TOO_EARLY: {
      label: "Too Early",
      pillClass: "gdi-pill gdi-pill-action-too-early",
    },
    UNCLASSIFIED: {
      label: "Unclassified",
      pillClass: "gdi-pill gdi-pill-action-watch",
    },
  };

  /** Non-canonical timing codes — never show as Action Timing pills. */
  var NON_CANONICAL_ACTION_TIMING = {
    OPEN_ENDED: 1,
    FUTURE_WINDOW: 1,
    UNDETERMINED: 1,
    UNKNOWN: 1,
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

  /** Normalize bare www./domain values to https:// for href use. */
  function normalizeHttpUrlCandidate(raw) {
    var s = String(raw == null ? "" : raw).trim();
    if (!s) return "";
    if (/^https?:\/\//i.test(s)) return s;
    if (
      /^www\./i.test(s) ||
      /^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}([\/?#].*)?$/i.test(s)
    ) {
      return "https://" + s;
    }
    return s;
  }

  function looksLikeStandaloneWebsite(raw) {
    var s = String(raw == null ? "" : raw).trim();
    if (!s || /\s/.test(s)) return false;
    return (
      /^(https?:\/\/|www\.)/i.test(s) ||
      /^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}([\/?#].*)?$/i.test(s)
    );
  }

  /**
   * Drawer field values: safe http(s) websites become clickable links.
   * Escapes all non-URL text. Used for Venue/Planner intelligence and meta lists.
   */
  function formatDrawerFieldValueHtml(value) {
    var raw = String(value == null ? "" : value).trim();
    if (!raw) return "—";
    if (looksLikeStandaloneWebsite(raw)) {
      var href = normalizeHttpUrlCandidate(raw);
      if (isSafeHttpUrl(href)) {
        return (
          '<a href="' +
          esc(href) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(raw) +
          "</a>"
        );
      }
    }
    // Linkify embedded http(s) / www. URLs inside longer notes.
    var re = /(https?:\/\/[^\s<>"')\]]+|www\.[^\s<>"')\]]+)/gi;
    var out = "";
    var last = 0;
    var m;
    while ((m = re.exec(raw))) {
      out += esc(raw.slice(last, m.index));
      var matched = m[0];
      var trimmed = matched.replace(/[.,;:!?)]+$/, "");
      var trailing = matched.slice(trimmed.length);
      var linkHref = normalizeHttpUrlCandidate(trimmed);
      if (isSafeHttpUrl(linkHref)) {
        out +=
          '<a href="' +
          esc(linkHref) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(trimmed) +
          "</a>" +
          esc(trailing);
      } else {
        out += esc(matched);
      }
      last = m.index + matched.length;
    }
    out += esc(raw.slice(last));
    return out;
  }

  /** Label + value meta row; websites render as hyperlinks. */
  function drawerMetaFieldRowHtml(label, value) {
    if (value == null || value === "") return "";
    return (
      "<li><strong>" +
      esc(label) +
      ":</strong> " +
      formatDrawerFieldValueHtml(value) +
      "</li>"
    );
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
    // Single verified day — founder standard: "Apr 30, 2027" (not "Apr 30-30, 2027")
    if (!end || (end.year === start.year && end.month === start.month && end.day === start.day)) {
      return startMon + " " + start.day + ", " + start.year;
    }
    if (end.year === start.year && end.month === start.month) {
      return startMon + " " + start.day + "\u2013" + end.day + ", " + start.year;
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

  function isEventsOpportunityTile(it, linked) {
    linked = linked || {};
    it = it || {};
    if (
      linked.opportunityTypeFilterCategory === "ASSOCIATION_CONFERENCE_GROUP" ||
      it.opportunityTypeFilterCategory === "ASSOCIATION_CONFERENCE_GROUP"
    ) {
      return true;
    }
    if (
      linked.researchSegment === "associations_government" ||
      it.researchSegment === "associations_government"
    ) {
      return true;
    }
    return Boolean(
      (linked.eventsOpportunityOverview || it.eventsOpportunityOverview) &&
        linked.opportunityTypeFilterCategory !== "SCALING_COMPANY" &&
        it.opportunityTypeFilterCategory !== "SCALING_COMPANY"
    );
  }

  function tileEventDatePill(it, linked) {
    linked = linked || {};
    it = it || {};
    var plan = linked.tileBadgePlan || it.tileBadgePlan || {};
    var timing =
      linked.canonicalTiming || it.canonicalTiming || plan.canonicalTiming || {};
    // Prefer canonical display label (month-only, Multiple Dates, Ongoing, single day).
    var display =
      plan.datePillDisplayLabel ||
      linked.datePillDisplayLabel ||
      it.datePillDisplayLabel ||
      timing.displayLabel ||
      linked.eventDateDisplay ||
      it.eventDateDisplay ||
      "";
    display = String(display || "").trim();
    if (
      display &&
      !/^dates tbd$/i.test(display) &&
      !/^date not yet confirmed$/i.test(display) &&
      display !== "VERIFIED_DATE" &&
      !/^open[_\s-]?ended$/i.test(display)
    ) {
      var titleAttr = "";
      if (
        /multiple dates/i.test(display) &&
        Array.isArray(timing.occurrences) &&
        timing.occurrences.length
      ) {
        var occ = timing.occurrences
          .map(function (x) {
            return x && (x.startDate || x.date || "");
          })
          .filter(Boolean)
          .join(", ");
        if (occ) {
          titleAttr = ' title="' + esc(occ) + '" aria-label="Dates: ' + esc(occ) + '"';
        }
      }
      return (
        '<span class="gdi-tile-pill gdi-tile-pill--date gdi-pill gdi-pill-date" data-facet="event-date"' +
        titleAttr +
        ">" +
        esc(display) +
        "</span>"
      );
    }
    // Events: prefer DTO-resolved upcoming dates (never historical-as-future).
    var start =
      linked.tileEventStartDate ||
      it.tileEventStartDate ||
      plan.tileEventStartDate ||
      (isEventsOpportunityTile(it, linked)
        ? null
        : linked.eventStartDate || it.eventStartDate);
    var end =
      linked.tileEventEndDate ||
      it.tileEventEndDate ||
      plan.tileEventEndDate ||
      linked.eventEndDate ||
      it.eventEndDate;
    if (!start && !isEventsOpportunityTile(it, linked)) {
      start = linked.eventStartDate || it.eventStartDate;
    }
    // Pill 3 — verified demand/event date, else customer-facing "Dates TBD".
    // Never surface OPEN_ENDED or announcement-only dates as the demand window.
    if (parseIsoDateParts(start)) {
      var label = eventDateRangeLabel(start, end, null);
      if (label && !/dates tbd/i.test(label)) {
        return (
          '<span class="gdi-tile-pill gdi-tile-pill--date gdi-pill gdi-pill-date" data-facet="event-date">' +
          esc(label) +
          "</span>"
        );
      }
    }
    return (
      '<span class="gdi-tile-pill gdi-tile-pill--date gdi-pill gdi-pill-date" data-facet="event-date">Dates TBD</span>'
    );
  }

  function tilePriorityPill(priority, activeFilters) {
    var key = normalizePriorityFilterKey(priority);
    activeFilters = activeFilters || {};
    var isActive = activeFilters.priority === key;
    // Events customer contract: never render Unclassified — fall back to Watch.
    var meta =
      PRIORITY_DISPLAY[key] ||
      (key === "UNCLASSIFIED"
        ? PRIORITY_DISPLAY.WATCHLIST
        : PRIORITY_DISPLAY.WATCHLIST);
    var label =
      key === "UNCLASSIFIED" ? PRIORITY_DISPLAY.WATCHLIST.label : meta.label;
    var filterKey = key === "UNCLASSIFIED" ? "WATCHLIST" : key;
    return (
      '<button type="button" class="gdi-tile-pill gdi-tile-pill--priority ' +
      (filterKey === "WATCHLIST"
        ? PRIORITY_DISPLAY.WATCHLIST.pillClass
        : meta.pillClass) +
      (isActive || activeFilters.priority === filterKey
        ? " is-filter-active"
        : "") +
      '" data-gdi-tile-priority="' +
      esc(filterKey) +
      '" data-facet="priority" aria-pressed="' +
      (isActive || activeFilters.priority === filterKey) +
      '">' +
      esc(label) +
      "</button>"
    );
  }

  function normalizeSalesActionPillKey(raw) {
    var key = String(raw || "")
      .trim()
      .toUpperCase()
      .replace(/[\s-]+/g, "_");
    if (CANONICAL_SALES_ACTION_KEYS[key]) return key;
    return "QUALIFY_OPPORTUNITY";
  }

  /**
   * Pill 2 — Recommended Sales Action (eight approved labels). Always a colored badge.
   * Never Commercial Stage terminology.
   */
  function tileSalesActionPill(it, linked) {
    linked = linked || {};
    it = it || {};
    var plan = linked.tileBadgePlan || it.tileBadgePlan || {};
    var rawKey =
      linked.salesActionKey ||
      it.salesActionKey ||
      plan.salesActionKey ||
      "";
    var rawLabel =
      linked.salesActionPillLabel ||
      it.salesActionPillLabel ||
      plan.salesActionPillLabel ||
      "";
    var key = normalizeSalesActionPillKey(rawKey || rawLabel);
    var meta = SALES_ACTION_PILL[key] || SALES_ACTION_PILL.QUALIFY_OPPORTUNITY;
    var label = rawLabel && SALES_ACTION_PILL[normalizeSalesActionPillKey(rawLabel)]
      ? String(rawLabel).trim()
      : meta.label;
    // Reject commercial-stage / legacy classification labels if they leak through.
    if (
      /^(Pursue Now|Future Capture|Relationship Development|Qualification Needed|Account Development|Exploratory|Sales Opportunity|Future Opportunity|Research Needed)$/i.test(
        String(label),
      )
    ) {
      label = meta.label;
    }
    return (
      '<span class="gdi-tile-pill gdi-tile-pill--sales-action ' +
      meta.pillClass +
      '" data-facet="sales-action" data-sales-action="' +
      esc(key) +
      '">' +
      esc(label) +
      "</span>"
    );
  }

  function tileActionPill(status, priority, activeFilters) {
    if (!status) return "";
    var raw = String(status).trim().toUpperCase();
    // Qualification / open-ended CAR / legacy codes are not Action Timing pills.
    if (
      raw === "QUALIFY_NOW" ||
      raw === "RESEARCH_FURTHER" ||
      NON_CANONICAL_ACTION_TIMING[raw]
    ) {
      return "";
    }
    // Only canonical Action Timing filter values appear as pills.
    if (raw !== "CONTACT_NOW" && raw !== "WATCH" && raw !== "TOO_EARLY") {
      return "";
    }
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

  /** Customer-facing labels for internal enum / status codes. */
  var CUSTOMER_STATUS_LABELS = {
    FUTURE_CYCLE_EXPECTED: "Future cycle expected",
    FUTURE_CYCLE_CONFIRMED: "Future cycle confirmed",
    RECURRING_CYCLE_STRONG: "Recurring cycle (strong pattern)",
    NO_FUTURE_CYCLE_EVIDENCE: "No future-cycle evidence yet",
    LODGING_DEMAND_UNKNOWN: "Room demand not yet published",
    UNKNOWN: "Not yet established",
    PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE:
      "Primary Venue Selected / Overflow Possible",
    PRIMARY_VENUE_SELECTED: "Primary Venue Selected",
    HOTEL_SOURCING_UNCONFIRMED: "Hotel Sourcing Unconfirmed",
    CONTACT_NOW: "Pursue now",
    QUALIFY_NOW: "Needs qualification",
    WATCH: "Future watch",
    TOO_EARLY: "Too early",
    UNCLASSIFIED: "Unclassified",
    RESEARCH_CONTINUATION: "Research continuation",
    QUALIFIED: "Qualified",
  };

  function customerStatusLabel(raw, fallback) {
    if (raw == null || raw === "") return fallback || "—";
    var s = String(raw).trim();
    if (CUSTOMER_STATUS_LABELS[s]) return CUSTOMER_STATUS_LABELS[s];
    if (CUSTOMER_STATUS_LABELS[s.toUpperCase()]) {
      return CUSTOMER_STATUS_LABELS[s.toUpperCase()];
    }
    // Already human prose
    if (/\s/.test(s) && !/^[A-Z0-9_]+$/.test(s)) return s;
    if (/^[A-Z0-9_]+$/.test(s)) {
      return s
        .toLowerCase()
        .split("_")
        .map(function (w) {
          return w.charAt(0).toUpperCase() + w.slice(1);
        })
        .join(" ");
    }
    return s;
  }

  function stripEvidenceClaimPrefixes(raw) {
    var t = String(raw == null ? "" : raw).replace(/\s+/g, " ").trim();
    if (!t) return "";
    var prefixRe = /^(?:FACT(?:\/FIT-GATE)?|INFERENCE|MIXED|HYPOTHESIS|UNKNOWN)\s*:\s*/i;
    while (prefixRe.test(t)) {
      t = t.replace(prefixRe, "").trim();
    }
    t = t
      .replace(/\b(?:FACT(?:\/FIT-GATE)?|INFERENCE|MIXED|HYPOTHESIS)\s*:\s*/gi, "")
      .replace(/\s{2,}/g, " ")
      .trim();
    return t;
  }

  function distinctProse() {
    var seen = [];
    var out = [];
    for (var i = 0; i < arguments.length; i++) {
      var t = stripEvidenceClaimPrefixes(arguments[i]);
      if (!t || t === "—") continue;
      // Strip customer-facing implementation notes; keep any remaining prose.
      t = t
        .replace(
          /\s*No numeric score\s*[—–-]\s*reviewed package did not run canonical[^.]*\.?/gi,
          ""
        )
        .replace(
          /\s*reviewed package has no canonical[^.]*\.?/gi,
          ""
        )
        .replace(/\s*Qualitative fit\s*[—–-]\s*no numeric score on this prospect\.?/gi, "")
        .replace(/\s{2,}/g, " ")
        .trim();
      if (!t || t === "—") continue;
      if (
        /reviewed package did not run canonical|canonical (?:fit )?engine|canonical scoring|implementation note/i.test(
          t
        )
      ) {
        continue;
      }
      var key = t
        .toLowerCase()
        .replace(/[^\w\s]/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 96);
      if (!key) continue;
      var dup = false;
      for (var s = 0; s < seen.length; s++) {
        var prev = seen[s];
        if (
          prev === key ||
          (key.length >= 48 && prev.indexOf(key.slice(0, 48)) >= 0) ||
          (prev.length >= 48 && key.indexOf(prev.slice(0, 48)) >= 0)
        ) {
          dup = true;
          break;
        }
      }
      if (dup) continue;
      seen.push(key);
      out.push(t);
    }
    return out;
  }

  function formatPeakRoomsLabel(o) {
    var peak = o.peakRooms ?? o.estimatedPeakRooms ?? o.publishedPeakRooms;
    if (peak != null && peak !== "" && !Number.isNaN(Number(peak))) {
      return String(peak);
    }
    if (typeof peak === "string" && /\d/.test(peak) && !/housing|program|verified/i.test(peak)) {
      return peak;
    }
    return "Not yet published";
  }

  function formatHousingProgramLabel(o) {
    var status = String(o.housingProgramStatus || o.roomDemandStatus || "").toUpperCase();
    var label = o.housingProgramStatusLabel || o.roomDemandStatusLabel || "";
    if (
      status === "VERIFIED_HOUSING_PROGRAM" ||
      /verified housing program/i.test(String(label))
    ) {
      return label || "Verified housing program";
    }
    if (o.housingStatus) return customerStatusLabel(o.housingStatus, "");
    return "";
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

  var GDI_PAGE_TITLE = "Group & Demand Intelligence";
  var GDI_PAGE_SUBTITLE =
    "Dealality identifies future hotel demand, shows which opportunities matter most, and tells the sales team who to pursue and what to do next.";
  var GDI_METHODOLOGY = [
    "Based on structured research into group and meeting demand the hotel may be positioned to pursue.",
    "Findings are research-assisted and observational. Sales teams should validate before outreach.",
    "Dealality qualifies opportunities by hotel fit, evidence confidence, and booking window. It does not guarantee bookings.",
  ];

  var MONTH_NAME_RE =
    "(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)";

  /**
   * Acronyms preserved in tile secondary-headline Proper Case (presentation only).
   * Matched case-insensitively so enum leftovers / sentence-case labels stay readable.
   */
  var TILE_SECONDARY_HEADLINE_ACRONYMS = {
    plus: "PLUS",
    arias: "ARIAS",
    nih: "NIH",
    nyc: "NYC",
    dmc: "DMC",
    rfp: "RFP",
    tbd: "TBD",
    mba: "MBA",
    csi: "CSI",
    dc: "DC",
    anade: "ANADE",
    canaco: "CANACO",
    amexcap: "AMEXCAP",
    icuh: "ICUH",
    lasa: "LASA",
    andellac: "ANDELLAC",
    amrob: "AMRob",
    comrob: "COMRob",
    smne: "SMNE",
    cdmx: "CDMX",
    mpi: "MPI",
    pco: "PCO",
  };

  /**
   * Explicit customer-facing secondary-headline labels (classification + sourcing).
   * Keys are uppercase enums or lowercase normalized prose.
   * Classification: REUSABLE_PRODUCT_UI
   */
  var TILE_SECONDARY_HEADLINE_LABELS = {
    // Approved four Hotel Sourcing statuses (Events tile + drawer)
    HOTEL_SOURCING_OPEN: "Hotel Sourcing Open",
    "hotel sourcing open": "Hotel Sourcing Open",
    SOURCING_CONFIRMED_OPEN: "Hotel Sourcing Open",
    HOTEL_SOURCING_UNCONFIRMED: "Hotel Sourcing Unconfirmed",
    "hotel sourcing unconfirmed": "Hotel Sourcing Unconfirmed",
    SOURCING_UNCONFIRMED: "Hotel Sourcing Unconfirmed",
    VENUE_CONFIRMED: "Venue Confirmed",
    "venue confirmed": "Venue Confirmed",
    COMMITTED_ELSEWHERE: "Venue Confirmed",
    "committed elsewhere": "Venue Confirmed",
    HISTORICAL_VENUE_ONLY: "Historical Venue Only",
    "historical venue only": "Historical Venue Only",
    // Event type labels
    CORPORATE_INCENTIVE: "Corporate Incentive",
    "corporate incentive": "Corporate Incentive",
    ASSOCIATION_CONFERENCE: "Association Conference",
    "association conference": "Association Conference",
    ASSOCIATION_MEETING: "Association Meeting",
    "association meeting": "Association Meeting",
    CORPORATE_MEETING: "Corporate Meeting",
    "corporate meeting": "Corporate Meeting",
    LEADERSHIP_RETREAT: "Leadership Retreat",
    "leadership retreat": "Leadership Retreat",
    TRAINING_EVENT: "Training Event",
    "training event": "Training Event",
    INDUSTRY_CONFERENCE: "Industry Conference",
    "industry conference": "Industry Conference",
    TRADE_SHOW: "Trade Show",
    "trade show": "Trade Show",
    CORPORATE_EVENT: "Corporate Event",
    "corporate event": "Corporate Event",
    PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE: "Venue Confirmed",
    "primary venue selected / overflow possible": "Venue Confirmed",
    PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE: "Venue Confirmed",
    "primary venue selected / no overflow evidence": "Venue Confirmed",
    PRIMARY_VENUE_SELECTED: "Venue Confirmed",
    "primary venue selected": "Venue Confirmed",
    OPEN_UNRESOLVED: "Hotel Sourcing Unconfirmed",
    "open / unresolved": "Hotel Sourcing Unconfirmed",
    RFP_ACTIVE_SOURCING: "Hotel Sourcing Open",
    "rfp / active sourcing": "Hotel Sourcing Open",
    HOTEL_VENUE_TBD: "Hotel Sourcing Unconfirmed",
    "hotel / venue tbd": "Hotel Sourcing Unconfirmed",
    DESTINATION_TBD: "Hotel Sourcing Unconfirmed",
    "destination tbd": "Hotel Sourcing Unconfirmed",
    HOUSING_PENDING: "Hotel Sourcing Unconfirmed",
    "housing pending": "Hotel Sourcing Unconfirmed",
    REGISTRATION_OPEN_HOTEL_UNANNOUNCED: "Hotel Sourcing Unconfirmed",
    "registration open / hotel unannounced": "Hotel Sourcing Unconfirmed",
    PARTIALLY_PLACED: "Venue Confirmed",
    "partially placed": "Venue Confirmed",
    FULLY_PLACED: "Venue Confirmed",
    "fully placed": "Venue Confirmed",
    CURRENT_CYCLE_CLOSED: "Venue Confirmed",
    "current cycle closed": "Venue Confirmed",
    ASSOCIATION_OCCASION: "Association Conference",
    "association occasion": "Association Conference",
    PREFERRED_LODGING_PARTNERSHIP: "Preferred Lodging Partnership",
    "preferred lodging partnership": "Preferred Lodging Partnership",
    PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP:
      "Professional Event Planner Partnership",
    "professional event planner partnership":
      "Professional Event Planner Partnership",
    SCALING_COMPANY: "Scaling Company",
    "scaling company": "Scaling Company",
    LOCAL_MEETINGS_CATERING: "Local Meetings & Catering",
    "local meetings & catering": "Local Meetings & Catering",
    "local meetings and catering": "Local Meetings & Catering",
  };

  /**
   * Tile header Proper Case (presentation only).
   * Preserves short ALL-CAPS acronyms and known hospitality/event acronyms.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function toProperCaseDisplayTitle(raw) {
    var s = String(raw == null ? "" : raw).replace(/\s+/g, " ").trim();
    if (!s) return s;
    // Accent-safe: do not use \\b (breaks "Médicos" into "Mé"+"Dicos").
    if (/[a-záéíóúñ]/.test(s) && /[A-ZÁÉÍÓÚÑ]/.test(s) && s !== s.toUpperCase()) {
      return s
        .split(/(\s+|[-–—|:/,()]+)/)
        .map(function (tok) {
          var lower = tok.toLowerCase();
          if (TILE_SECONDARY_HEADLINE_ACRONYMS[lower]) {
            return TILE_SECONDARY_HEADLINE_ACRONYMS[lower];
          }
          return tok;
        })
        .join("");
    }
    return s
      .split(/(\s+|[-–—|:/,()]+)/)
      .map(function (tok) {
        if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(tok)) return tok;
        var lower = tok.toLowerCase();
        if (TILE_SECONDARY_HEADLINE_ACRONYMS[lower]) {
          return TILE_SECONDARY_HEADLINE_ACRONYMS[lower];
        }
        if (/^[A-Z]{2,8}$/.test(tok)) return tok;
        if (/^[A-Z0-9]{2,8}$/.test(tok) && /[0-9]/.test(tok)) return tok;
        return tok.charAt(0).toUpperCase() + tok.slice(1).toLowerCase();
      })
      .join("");
  }

  /**
   * Contact-role line under the person name on tiles — always Proper Case every word.
   * Unlike toProperCaseDisplayTitle, does not early-return on sentence-case strings
   * ("Event confirmation / routing contact" → "Event Confirmation / Routing Contact").
   * Classification: REUSABLE_PRODUCT_UI
   */
  function toProperCaseContactRoleLabel(raw) {
    var s = String(raw == null ? "" : raw).replace(/\s+/g, " ").trim();
    if (!s) return s;
    return s
      .split(/(\s+|[-–—|:/,()&]+)/)
      .map(function (tok) {
        if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(tok)) return tok;
        var lower = tok.toLowerCase();
        if (TILE_SECONDARY_HEADLINE_ACRONYMS[lower]) {
          return TILE_SECONDARY_HEADLINE_ACRONYMS[lower];
        }
        if (
          /^(ceo|cfo|cto|coo|vp|svp|evp|md|cmp|des|phd|mba|cmo|cro)$/i.test(
            tok.replace(/\.$/, "")
          )
        ) {
          return tok.replace(/\.$/, "").toUpperCase() + (/\.$/.test(tok) ? "." : "");
        }
        if (/^(dr|mr|mrs|ms|prof)\.?$/i.test(tok)) {
          return (
            tok.charAt(0).toUpperCase() +
            tok.slice(1, tok.length - (/\.$/.test(tok) ? 1 : 0)).toLowerCase() +
            (/\.$/.test(tok) ? "." : "")
          );
        }
        if (/^[A-Z]{2,8}$/.test(tok)) return tok;
        if (/^[A-Z0-9]{2,8}$/.test(tok) && /[0-9]/.test(tok)) return tok;
        return tok.charAt(0).toUpperCase() + tok.slice(1).toLowerCase();
      })
      .join("");
  }

  /**
   * Format classification / sourcing / short-org tokens for the tile meta line.
   * Prefer explicit taxonomy labels; fall back to Proper Case. Presentation only.
   */
  function formatTileSecondaryHeadlineLabel(raw) {
    var s = String(raw == null ? "" : raw).replace(/\s+/g, " ").trim();
    if (!s) return s;
    if (TILE_SECONDARY_HEADLINE_LABELS[s]) return TILE_SECONDARY_HEADLINE_LABELS[s];
    var upper = s.toUpperCase();
    if (TILE_SECONDARY_HEADLINE_LABELS[upper]) {
      return TILE_SECONDARY_HEADLINE_LABELS[upper];
    }
    var lower = s.toLowerCase();
    if (TILE_SECONDARY_HEADLINE_LABELS[lower]) {
      return TILE_SECONDARY_HEADLINE_LABELS[lower];
    }
    return toProperCaseDisplayTitle(s);
  }

  /**
   * Clean tile/summary copy without destroying concise occasion titles.
   * Keep years in titles like "AHIMA Advocacy Summit 2027".
   * Strip month-day ranges when a date pill already carries timing.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function scrubEventDatesFromText(raw, opts) {
    opts = opts || {};
    var s = String(raw == null ? "" : raw);
    if (!s) return "";

    // Concise account/occasion titles — preserve year; only strip research-note junk.
    var looksLikeCleanOccasion =
      s.length <= 90 &&
      !/verified via|verified:|hotel not named|hotel block tba|supported recurring|unconfirmed this pass/i.test(
        s
      ) &&
      !new RegExp(
        MONTH_NAME_RE + "\\.?\\s+\\d{1,2}(?:\\s*[–—\\-]\\s*\\d{1,2})?,\\s*20\\d{2}",
        "i"
      ).test(s);
    if (looksLikeCleanOccasion && opts.preserveOccasionYear !== false) {
      return s.replace(/\s{2,}/g, " ").trim();
    }

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
    // Research-note leftovers after date strip: "Verified via …: , Washington"
    s = s.replace(/\s*—\s*Verified[^—]*/gi, "");
    s = s.replace(/\s*Verified via[^.;]*/gi, "");
    s = s.replace(/\s*Verified:\s*/gi, "");
    s = s.replace(/:\s*,/g, ":");
    s = s.replace(/\s*;\s*;/g, ";");
    s = s.replace(/\s*,\s*,/g, ",");
    s = s.replace(/\s*,\s*;/g, ";");
    s = s.replace(/;\s*$/g, "");
    s = s.replace(/,\s*$/g, "");
    s = s.replace(/:\s*$/g, "");
    s = s.replace(/\.\s*$/g, ".");
    s = s.replace(/\s*[—\-–]\s*$/g, "");
    s = s.replace(/\s{2,}/g, " ").trim();
    return s;
  }

  /**
   * Meta line under title: useful segment/occasion class — not a full org repeat
   * when the title already carries the account/occasion name.
   */
  function tileClassificationMeta(it, linked, titleText) {
    linked = linked || {};
    it = it || {};
    var title = String(titleText || "");
    var org = String(
      it.displayCompanyName ||
        linked.displayCompanyName ||
        it.organizationName ||
        linked.organizationName ||
        ""
    ).trim();
    var typeLbl =
      linked.opportunityTypeLabel ||
      it.opportunityTypeLabel ||
      linked.segmentDisplayLabel ||
      it.segmentDisplayLabel ||
      linked.demandSignalTypeLabel ||
      it.demandSignalTypeLabel ||
      "";
    // Founder: Scaling tiles must keep "Corporate Account Relationship" visible —
    // never replace with the filter label "Scaling Company".
    if (/^scaling company$/i.test(String(typeLbl).trim())) {
      typeLbl = "Corporate Account Relationship";
    }
    var segment = it.segment || linked.segment || "";
    if (/child account|demand campaign|generator|mega-event|plausible_fit/i.test(String(segment))) {
      segment = "";
    }
    if (
      /^[a-z][a-z0-9]*(?:_[a-z0-9]+){1,8}$/.test(String(segment).trim()) &&
      !/\s/.test(String(segment))
    ) {
      segment = typeLbl || "Association";
    }
    var classBits = [];
    var typeRaw = String(
      linked.opportunityType || it.opportunityType || typeLbl || ""
    ).toUpperCase();
    var isScalingTile =
      linked.researchSegment === "scaling_companies" ||
      it.researchSegment === "scaling_companies" ||
      /SCALING_COMPANY/i.test(
        String(linked.demandFamily || it.demandFamily || linked.demandSignalType || it.demandSignalType || "")
      ) ||
      /scaling/i.test(String(segment));
    var isEventsTile =
      !isScalingTile &&
      (classifyOpportunityTypeFilter(linked) === "ASSOCIATION_CONFERENCE_GROUP" ||
        classifyOpportunityTypeFilter(it) === "ASSOCIATION_CONFERENCE_GROUP" ||
        linked.researchSegment === "associations_government" ||
        it.researchSegment === "associations_government" ||
        Boolean(
          (linked.eventsOpportunityOverview || it.eventsOpportunityOverview) &&
            !isScalingTile
        ));

    // Events tile meta: Organizer · Event Type · Hotel Sourcing Status
    if (isEventsTile) {
      var ov = linked.eventsOpportunityOverview || it.eventsOpportunityOverview || {};
      if (ov.tileClassificationLine) {
        return esc(ov.tileClassificationLine);
      }
      var orgShortEvents = org.replace(/\s*\([^)]*\)\s*$/, "").trim();
      orgShortEvents = orgShortEvents
        .replace(
          /,?\s*(S\.?\s*C\.?|S\.?\s*A\.?\s*(?:de\s*)?C\.?\s*V\.?|S\.?\s*de\s*R\.?\s*L\.?[^.]*|Inc\.?|LLC|Ltd\.?)\s*$/i,
          ""
        )
        .trim();
      var eventType =
        linked.eventTypeLabel ||
        it.eventTypeLabel ||
        ov.eventTypeLabel ||
        "";
      if (!eventType) {
        var sub = String(linked.eventSubtype || it.eventSubtype || "").toLowerCase();
        if (/incentive/.test(sub)) eventType = "Corporate Incentive";
        else if (/trade/.test(sub)) eventType = "Trade Show";
        else if (/retreat|leadership/.test(sub)) eventType = "Leadership Retreat";
        else if (/training/.test(sub)) eventType = "Training Event";
        else if (/corporate_meeting|meeting/.test(sub)) eventType = "Corporate Meeting";
        else if (/assoc|conference|medical/.test(sub)) eventType = "Association Conference";
        else if (/corporate_event|other/.test(sub)) eventType = "Corporate Event";
        else eventType = "Corporate Event";
      }
      var sourcing =
        ov.hotelSourcing ||
        linked.hotelSourcingStatusLabel ||
        it.hotelSourcingStatusLabel ||
        linked.venueSourcingStatusLabel ||
        it.venueSourcingStatusLabel ||
        "Hotel Sourcing Unconfirmed";
      sourcing = formatTileSecondaryHeadlineLabel(sourcing);
      eventType = formatTileSecondaryHeadlineLabel(eventType);
      var bits = [];
      if (orgShortEvents) bits.push(formatTileSecondaryHeadlineLabel(orgShortEvents));
      if (eventType) bits.push(eventType);
      if (sourcing) bits.push(sourcing);
      return esc(bits.join(" · "));
    }

    // Customer-facing classification — never dump FUTURE_CYCLE / enum codes.
    if (isScalingTile) {
      classBits.push("Corporate Account Relationship");
    } else if (typeRaw === "FUTURE_CYCLE" || /^future\s*cycle$/i.test(String(typeLbl))) {
      classBits.push("Association Conference");
    } else if (typeLbl && String(typeLbl).toUpperCase() !== "EVENT") {
      classBits.push(customerStatusLabel(typeLbl, String(typeLbl)));
    } else if (segment) {
      classBits.push(String(segment));
    } else {
      classBits.push("Association Conference");
    }
    var classLabel = formatTileSecondaryHeadlineLabel(classBits[0]);
    // Scaling tiles: classification only — never echo legal-entity organizationName
    // beside a clean displayCompanyName heading (founder tile contract).
    if (isScalingTile || /^corporate account relationship$/i.test(classLabel)) {
      return esc(classLabel);
    }
    // Only echo a short org token when title does not already include it.
    // Org echo uses Proper Case too; raw organizationName elsewhere stays as stored.
    var orgShort = org.replace(/\s*\([^)]*\)\s*$/, "").trim();
    // Strip legal suffixes before length check so "Roland Berger, S.C." does not leak.
    orgShort = orgShort
      .replace(
        /,?\s*(S\.?\s*C\.?|S\.?\s*A\.?\s*(?:de\s*)?C\.?\s*V\.?|S\.?\s*de\s*R\.?\s*L\.?[^.]*|Inc\.?|LLC|Ltd\.?)\s*$/i,
        ""
      )
      .trim();
    var titleHasOrg =
      org &&
      (title.toLowerCase().indexOf(org.toLowerCase()) >= 0 ||
        (orgShort.length >= 3 &&
          title.toLowerCase().indexOf(orgShort.toLowerCase()) >= 0) ||
        /\b(AHIMA|ACTS|ACC|AMWA|NDSS|NADO|NAR|AAD|NCHV|CMSS|HCAOA|USCM)\b/i.test(
          title
        ));
    if (org && !titleHasOrg && orgShort.length >= 3 && orgShort.length <= 48) {
      return (
        esc(formatTileSecondaryHeadlineLabel(orgShort)) +
        " · " +
        esc(classLabel)
      );
    }
    return esc(classLabel);
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

    var publishedValue =
      opts.lastPublishedUpdate != null && opts.lastPublishedUpdate !== ""
        ? opts.lastPublishedUpdate
        : opts.lastReviewedImport != null && opts.lastReviewedImport !== ""
          ? opts.lastReviewedImport
          : "";
    var lastResearchVal =
      opts.lastResearch != null && opts.lastResearch !== ""
        ? opts.lastResearch
        : "History unavailable";
    var runStatusVal =
      opts.runStatusLabel || opts.runStatus || "History unavailable";
    var publishedVal = publishedValue || "History unavailable";
    var sourceVerificationHtml =
      opts.lastSourceVerification != null && opts.lastSourceVerification !== ""
        ? '<span class="gdi-property-bar__history-sep" aria-hidden="true">·</span>' +
          '<span class="gdi-property-bar__history-item">' +
          '<span class="gdi-property-bar__history-label">Last source verification:</span> ' +
          '<span class="gdi-property-bar__history-value">' +
          esc(opts.lastSourceVerification) +
          "</span></span>"
        : "";
    // Always render hotel-level need-date status when a property context exists.
    // Missing DTO must not hide the control — default to NOT_ESTABLISHED.
    var needDateConfig = normalizeHotelNeedDateConfigV1(
      opts.hotelNeedDateConfigV1,
      opts.hotelId || (opts.hotel && opts.hotel.hotelId) || null
    );
    var showNeedDateConfig =
      opts.mode === "auth" ||
      opts.mode === "share" ||
      Boolean(opts.hotelId) ||
      Boolean(opts.hotel && (opts.hotel.hotelName || opts.hotel.name));
    // Same-row metadata: Need Dates shares hierarchy with research history fields.
    var needDateHistoryHtml = showNeedDateConfig
      ? '<span class="gdi-property-bar__history-item gdi-property-bar__history-item--need-dates" data-gdi-need-date-config-visible="1">' +
        '<span class="gdi-property-bar__history-label">Need Dates:</span> ' +
        hotelNeedDateConfigStatusHtml(needDateConfig) +
        needDateInformationLegendHtml({
          popoverId: opts.needDateLegendPopoverId || "gdiNeedDateLegendPopoverBar",
        }) +
        "</span>" +
        '<span class="gdi-property-bar__history-sep" aria-hidden="true">·</span>'
      : "";

    return (
      '<section class="filters-section aiv-filters-section gdi-property-bar" aria-label="Property selection">' +
      '<div class="gdi-property-bar__primary">' +
      propertyField +
      weeklyThisWeekFilterHtml(opts) +
      opportunityTypeFilterHtml(opts) +
      (opts.actionHtml
        ? '<div class="filter-group aiv-filter-actions gdi-property-bar__actions">' +
          opts.actionHtml +
          "</div>"
        : "") +
      "</div>" +
      '<footer class="gdi-property-bar__history" aria-label="Need Dates and research activity">' +
      needDateHistoryHtml +
      '<span class="gdi-property-bar__history-item">' +
      '<span class="gdi-property-bar__history-label">Last research:</span> ' +
      '<span class="gdi-property-bar__history-value">' +
      esc(lastResearchVal) +
      "</span></span>" +
      '<span class="gdi-property-bar__history-sep" aria-hidden="true">·</span>' +
      '<span class="gdi-property-bar__history-item">' +
      '<span class="gdi-property-bar__history-label">Run:</span> ' +
      '<span class="gdi-property-bar__history-value">' +
      esc(runStatusVal) +
      "</span></span>" +
      sourceVerificationHtml +
      '<span class="gdi-property-bar__history-sep" aria-hidden="true">·</span>' +
      '<span class="gdi-property-bar__history-item">' +
      '<span class="gdi-property-bar__history-label">Last published:</span> ' +
      '<span class="gdi-property-bar__history-value">' +
      esc(publishedVal) +
      "</span></span>" +
      "</footer>" +
      "</section>"
    );
  }

  /**
   * Weekly Change filter — research-freshness / inventory delta (NOT action timing).
   * Distinct from Action Timing (bookingWindowStatus). Classification: REUSABLE_PRODUCT_UI
   */
  function weeklyThisWeekFilterHtml(opts) {
    opts = opts || {};
    if (opts.showWeeklyFilter === false) return "";
    var active = opts.activeWeekly != null ? opts.activeWeekly : "";
    var counts = opts.weeklyCounts || {};
    var options = [
      { value: "", label: "All", countKey: "all" },
      { value: "NEW", label: "New this week", countKey: "NEW" },
      { value: "UPDATED", label: "Updated", countKey: "UPDATED" },
      { value: "REACTIVATED", label: "Reactivated", countKey: "REACTIVATED" },
      { value: "HIGH_PRIORITY", label: "High Priority", countKey: "HIGH_PRIORITY" },
    ];
    return (
      '<div class="filter-group gdi-weekly-filter">' +
      '<label class="filter-label" for="gdiWeeklyFilter">Weekly Change</label>' +
      '<select class="filter-select" id="gdiWeeklyFilter" aria-label="Filter by research freshness this week" title="Filters by when Dealality added or updated the opportunity — not when to call the buyer.">' +
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

  /**
   * Opportunity Type filter — next to This Week.
   * Counts come from other active filters (before applying the selected type).
   */
  function opportunityTypeFilterHtml(opts) {
    opts = opts || {};
    if (opts.showOpportunityTypeFilter === false) return "";
    var active = opts.activeOpportunityType != null ? opts.activeOpportunityType : "";
    var options = Array.isArray(opts.opportunityTypeOptions)
      ? opts.opportunityTypeOptions
      : [{ value: "", label: "All types", count: null }];
    // If active type vanished after hotel switch, fall back to All types
    var activeStillValid = options.some(function (o) {
      return o.value === active;
    });
    if (!activeStillValid) active = "";
    return (
      '<div class="filter-group gdi-opportunity-type-filter">' +
      '<label class="filter-label" for="gdiOpportunityTypeFilter">Opportunity Type</label>' +
      '<select class="filter-select" id="gdiOpportunityTypeFilter" aria-label="Filter by opportunity type">' +
      options
        .map(function (o) {
          var text =
            o.count == null || o.count === ""
              ? o.label
              : o.label + " (" + o.count + ")";
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

  /** Structured opportunity-type category (mirrors opportunity-type-filter-v2). */
  function classifyOpportunityTypeFilter(o) {
    o = o || {};
    if (o.opportunityTypeFilterCategory) {
      return String(o.opportunityTypeFilterCategory);
    }
    var subtypeEarly = String(
      o.demandOpportunitySubtype || o.opportunityType || ""
    ).toUpperCase();
    if (
      subtypeEarly === "PREFERRED_LODGING_PARTNERSHIP" ||
      o.preferredLodgingPartnershipV1 === true
    ) {
      return "PREFERRED_LODGING_PARTNERSHIP";
    }
    if (
      subtypeEarly === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
      o.professionalEventPlannerPartnershipV1 === true
    ) {
      return "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP";
    }
    var demandId = String(
      o.demandSegmentId || o.customerDemandSegmentId || ""
    )
      .trim()
      .toLowerCase();
    var SEGMENT_TO_FILTER = {
      corporate_accounts_scaling: "SCALING_COMPANY",
      associations_conferences_events: "ASSOCIATION_CONFERENCE_GROUP",
      corporate_meetings_incentives: "CORPORATE_MEETINGS_INCENTIVES",
      government_public_sector: "GOVERNMENT_PUBLIC_SECTOR",
      sports_entertainment: "SPORTS_ENTERTAINMENT",
      education_scientific: "EDUCATION_SCIENTIFIC",
      projects_infrastructure: "PROJECTS_INFRASTRUCTURE",
      social_special_events: "SOCIAL_SPECIAL_EVENTS",
      local_catering_day_meetings: "LOCAL_CATERING_DAY_MEETINGS",
      corporate_celebrations_private_events:
        "CORPORATE_CELEBRATIONS_PRIVATE_EVENTS",
      travel_trade_group_intermediaries: "TRAVEL_TRADE_GROUP_INTERMEDIARIES",
      relocation_extended_stay: "RELOCATION_EXTENDED_STAY",
    };
    if (demandId && SEGMENT_TO_FILTER[demandId]) {
      return SEGMENT_TO_FILTER[demandId];
    }
    var segment = String(o.researchSegment || o.segment || "")
      .trim()
      .toLowerCase();
    if (segment && SEGMENT_TO_FILTER[segment]) {
      return SEGMENT_TO_FILTER[segment];
    }
    var type = String(o.opportunityType || "").toUpperCase();
    var signal = String(o.demandSignalType || o.demandFamily || "")
      .trim()
      .toLowerCase();
    var family = String(o.demandFamily || "")
      .trim()
      .toLowerCase();
    if (
      segment === "scaling_companies" ||
      segment.indexOf("scaling") >= 0 ||
      signal.indexOf("scaling") >= 0
    ) {
      return "SCALING_COMPANY";
    }
    if (
      segment === "local_catering_day_meetings" ||
      segment.indexOf("local_catering") >= 0 ||
      segment === "local_meetings_catering" ||
      segment.indexOf("local_meetings") >= 0 ||
      signal.indexOf("local_meetings") >= 0 ||
      signal.indexOf("local_catering") >= 0
    ) {
      return "LOCAL_CATERING_DAY_MEETINGS";
    }
    if (
      segment === "government_public_sector" ||
      segment.indexOf("government") >= 0 ||
      signal.indexOf("government") >= 0
    ) {
      return "GOVERNMENT_PUBLIC_SECTOR";
    }
    if (
      segment === "relocation_extended_stay" ||
      segment.indexOf("relocation") >= 0 ||
      segment.indexOf("extended_stay") >= 0 ||
      signal.indexOf("relocation") >= 0
    ) {
      return "RELOCATION_EXTENDED_STAY";
    }
    if (
      type === "VENUE_PARTNERSHIP" ||
      segment === "private events" ||
      segment === "private_events" ||
      signal.indexOf("venue_partnership") >= 0 ||
      signal.indexOf("preferred_lodging") >= 0
    ) {
      return "PREFERRED_LODGING_PARTNERSHIP";
    }
    if (type === "SPECIFIC_PRIVATE_EVENT") return "SPECIFIC_PRIVATE_EVENT";
    if (
      segment.indexOf("sport") >= 0 ||
      signal.indexOf("sport") >= 0 ||
      family.indexOf("sport") >= 0
    ) {
      return "SPORTS_ENTERTAINMENT";
    }
    if (
      segment === "association" ||
      segment.indexOf("assoc") >= 0 ||
      type === "OVERFLOW_HOUSING" ||
      type === "PRIMARY_PURSUIT" ||
      type === "FIXED_VENUE_OPEN_HOUSING" ||
      signal.indexOf("association") >= 0 ||
      signal.indexOf("conference") >= 0
    ) {
      return "ASSOCIATION_CONFERENCE_GROUP";
    }
    if (
      type === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
      segment.indexOf("corporate") >= 0 ||
      signal.indexOf("corporate") >= 0
    ) {
      return "CORPORATE_MEETINGS_TRAINING";
    }
    return "OTHER_UNCLASSIFIED";
  }

  var OPPORTUNITY_TYPE_FILTER_LABELS = {
    SCALING_COMPANY: "Scaling Company",
    ASSOCIATION_CONFERENCE_GROUP: "Association / Conference / Events",
    GOVERNMENT_PUBLIC_SECTOR: "Government & Public Sector",
    LOCAL_CATERING_DAY_MEETINGS: "Local Catering & Day Meetings",
    RELOCATION_EXTENDED_STAY: "Relocation & Extended Stay",
    CORPORATE_MEETINGS_INCENTIVES: "Corporate Meetings & Incentives",
    SPORTS_ENTERTAINMENT: "Sports & Entertainment",
    EDUCATION_SCIENTIFIC: "Education & Scientific",
    PROJECTS_INFRASTRUCTURE: "Projects & Infrastructure",
    SOCIAL_SPECIAL_EVENTS: "Social & Special Events",
    CORPORATE_CELEBRATIONS_PRIVATE_EVENTS:
      "Corporate Celebrations & Private Events",
    TRAVEL_TRADE_GROUP_INTERMEDIARIES: "Travel Trade & Group Intermediaries",
    PREFERRED_LODGING_PARTNERSHIP: "Preferred Lodging Partnership",
    PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP:
      "Professional Event Planner Partnership",
    LOCAL_MEETINGS_CATERING: "Local Meetings & Catering",
    CORPORATE_MEETINGS_TRAINING: "Corporate Meetings / Training",
    SPORTS_GROUP: "Sports Group",
    SPECIFIC_PRIVATE_EVENT: "Specific Private Event",
    OTHER_UNCLASSIFIED: "Other / Unclassified",
  };

  function countByOpportunityTypeFilter(opportunities) {
    var counts = { all: 0 };
    (opportunities || []).forEach(function (o) {
      counts.all += 1;
      var c = classifyOpportunityTypeFilter(o);
      counts[c] = (counts[c] || 0) + 1;
    });
    return counts;
  }

  function opportunityTypeFilterOptionsFromCounts(counts) {
    counts = counts || {};
    var options = [{ value: "", label: "All types", count: counts.all || 0 }];
    Object.keys(OPPORTUNITY_TYPE_FILTER_LABELS).forEach(function (value) {
      var n = counts[value] || 0;
      if (n > 0) {
        options.push({
          value: value,
          label: OPPORTUNITY_TYPE_FILTER_LABELS[value],
          count: n,
        });
      }
    });
    return options;
  }

  /** Assessment-status prose must not masquerade as a sales opportunity summary. */
  function isAssessmentStatusProse(text) {
    var t = String(text || "");
    return (
      /dimension-level hotel-fit assessment/i.test(t) ||
      /overall numeric (?:score )?(provisional\/)?withheld/i.test(t) ||
      /weights renormalized/i.test(t) ||
      /applicable dimensions still lacking evidence/i.test(t) ||
      /^provisional hotel-fit score\b/i.test(t)
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
    label: "Group &\nDemand Intelligence",
  };

  /**
   * Customer/share property-bar status only.
   * Internal run IDs, costs, and campaign codes belong in AI Demand Admin.
   */
  function customerSafeRunStatusLabel(summary) {
    var s = summary || {};
    var code = String(s.runStatus || "").trim().toUpperCase();
    var label = String(s.runStatusLabel || "").trim();
    if (!code && !label) return "History unavailable";
    if (code === "NEVER_RUN" || code === "NOT_YET_RUN") return "Not run yet";
    if (code === "STATUS_UNAVAILABLE" || code === "HISTORY_UNAVAILABLE") {
      return "History unavailable";
    }
    if (
      code === "COMPLETED" ||
      code === "COMPLETE" ||
      code === "SUCCESS" ||
      /complet/i.test(label)
    ) {
      return "Research Completed";
    }
    if (/fail|error/i.test(code) || /fail|error/i.test(label)) {
      return "Research unsuccessful";
    }
    if (
      /run|progress|active|pending|started/i.test(code) ||
      /progress|running/i.test(label)
    ) {
      return "Research in progress";
    }
    if (/^gdi_run_/i.test(label) || /\$|webhound|campaign/i.test(label)) {
      return "Research Completed";
    }
    if (label && label.length <= 40 && !/[0-9a-f]{8,}/i.test(label)) {
      return label;
    }
    return "Research Completed";
  }

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
      priorities:
        '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="4" width="18" height="18" rx="2"/><path d="M16 2v4"/><path d="M8 2v4"/><path d="M3 10h18"/></svg>',
    };
    if (!tabs.length) return "";
    return (
      '<nav class="bdd-section-nav aiv-section-nav gdi-section-nav" aria-label="Group & Demand views" role="tablist">' +
      tabs
        .map(function (t) {
          var isActive = activeTab === t.id;
          var label = formatTabLabel(t.label || t.id);
          var icon = icons[t.id] || icons.opportunities;
          var plainLabel = String(t.label || t.id).replace(/\n/g, " ");
          var commonAttrs =
            ' class="section-nav-item' +
            (isActive ? " active" : "") +
            '" role="tab" aria-selected="' +
            isActive +
            '" data-tab="' +
            esc(t.id) +
            '"' +
            (t.href ? ' data-href="' + esc(t.href) + '"' : "");
          var inner =
            '<div class="section-nav-icon" title="' +
            esc(plainLabel) +
            '">' +
            icon +
            "</div>" +
            '<span class="section-nav-label">' +
            label +
            "</span>";
          // External destinations use an <a> so hotelId is preserved and styling matches peers.
          if (t.href) {
            return (
              '<a href="' +
              esc(t.href) +
              '"' +
              commonAttrs +
              ">" +
              inner +
              "</a>"
            );
          }
          return (
            '<button type="button"' +
            commonAttrs +
            ">" +
            inner +
            "</button>"
          );
        })
        .join("") +
      "</nav>"
    );
  }

  var PRIORITY_RANK = { HIGH_PRIORITY: 0, MEDIUM_PRIORITY: 1, WATCHLIST: 2 };
  /** Inventory Visibility V1 — sales → account development → future → research → excluded */
  var INVENTORY_LANE_RANK = {
    SALES_OPPORTUNITY: 0,
    ACCOUNT_DEVELOPMENT: 1,
    FUTURE_OPPORTUNITY: 2,
    RESEARCH_WATCHLIST: 3,
    EXCLUDED: 4,
  };
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
    { value: "salesRelevance", label: "Sales relevance" },
    { value: "hotelFitScore", label: "Hotel Fit" },
    { value: "evidenceConfidence", label: "Evidence" },
    { value: "eventStartDate", label: "Nearest future date" },
    { value: "title", label: "Name (A-Z)" },
  ];

  /** Sales Opportunities quick-filter chip keys (DTO: salesOpportunityQuickFilterKeys). */
  var SALES_OPPORTUNITY_FILTER = {
    ALL: "",
    HOTEL_SOURCING_NOT_CONFIRMED: "HOTEL_SOURCING_NOT_CONFIRMED",
    PURSUE_NOW: "PURSUE_NOW",
    FUTURE_GROUP_BUSINESS: "FUTURE_GROUP_BUSINESS",
    COMPETITOR_CAPTURE: "COMPETITOR_CAPTURE",
    PARTNERSHIP_OPPORTUNITIES: "PARTNERSHIP_OPPORTUNITIES",
    LOCAL_CATERING: "LOCAL_CATERING",
  };

  /** Compact chip labels — filter IDs/predicates unchanged. */
  var SALES_OPPORTUNITY_FILTER_LABEL = {
    "": "All",
    HOTEL_SOURCING_NOT_CONFIRMED: "Hotel Unconfirmed",
    PURSUE_NOW: "Pursue Now",
    FUTURE_GROUP_BUSINESS: "Future Groups",
    COMPETITOR_CAPTURE: "Competitor",
    PARTNERSHIP_OPPORTUNITIES: "Partnerships",
    LOCAL_CATERING: "Catering",
  };

  /** Full names for title / aria-label tooltips. */
  var SALES_OPPORTUNITY_FILTER_LABEL_FULL = {
    "": "All Opportunities",
    HOTEL_SOURCING_NOT_CONFIRMED: "Hotel Sourcing Not Confirmed",
    PURSUE_NOW: "Pursue Now",
    FUTURE_GROUP_BUSINESS: "Future Group Business",
    COMPETITOR_CAPTURE: "Competitor Capture",
    PARTNERSHIP_OPPORTUNITIES: "Partnership Opportunities",
    LOCAL_CATERING: "Local Catering",
  };

  var HOTEL_SOURCING_SUBGROUP = {
    BOTH: "BOTH",
    OPEN: "OPEN",
    UNCONFIRMED: "UNCONFIRMED",
  };

  var COMMERCIAL_STAGE_FILTER = {
    ALL: "",
    PURSUE_NOW: "PURSUE_NOW",
    FUTURE_CAPTURE: "FUTURE_CAPTURE",
    RELATIONSHIP_DEVELOPMENT: "RELATIONSHIP_DEVELOPMENT",
    QUALIFICATION_NEEDED: "QUALIFICATION_NEEDED",
  };

  var COMMERCIAL_STAGE_FILTER_LABEL = {
    PURSUE_NOW: "Pursue Now",
    FUTURE_CAPTURE: "Future Capture",
    RELATIONSHIP_DEVELOPMENT: "Relationship Development",
    QUALIFICATION_NEEDED: "Qualification Needed",
  };

  var COMMERCIAL_STAGE_SWATCH = {
    PURSUE_NOW: "#a855f7",
    FUTURE_CAPTURE: "#06b6d4",
    RELATIONSHIP_DEVELOPMENT: "#f97316",
    QUALIFICATION_NEEDED: "#7e89ac",
  };

  var SALES_OPPORTUNITY_SWATCH = {
    HOTEL_SOURCING_NOT_CONFIRMED: "#38bdf8",
    PURSUE_NOW: "#a855f7",
    FUTURE_GROUP_BUSINESS: "#34d399",
    COMPETITOR_CAPTURE: "#fb7185",
    PARTNERSHIP_OPPORTUNITIES: "#c084fc",
    LOCAL_CATERING: "#fbbf24",
  };

  /** Sales Focus — canonical salesActivationCategory lanes (not Priority / Commercial Stage). */
  var SALES_FOCUS_FILTER = {
    ALL: "",
    PURSUE_NOW: "PURSUE_NOW",
    SALES_QUALIFICATION: "SALES_QUALIFICATION",
  };

  var SALES_FOCUS_FILTER_LABEL = {
    "": "All",
    PURSUE_NOW: "Pursue Now",
    SALES_QUALIFICATION: "Sales Qualification",
  };

  var SALES_FOCUS_SWATCH = {
    PURSUE_NOW: "#a855f7",
    SALES_QUALIFICATION: "#38bdf8",
  };

  function normalizeSalesFocusKey(o) {
    var raw = String(
      (o && (o.salesActivationCategory || o.salesActivationKey)) || ""
    )
      .trim()
      .toUpperCase();
    if (raw === "PURSUE_NOW") return "PURSUE_NOW";
    if (raw === "SALES_QUALIFICATION" || raw === "QUALIFY_NOW") {
      return "SALES_QUALIFICATION";
    }
    return "";
  }

  function matchesSalesFocusFilter(o, filterKey) {
    if (!filterKey) return true;
    if (isExcludedFromSalesFilterInventory(o)) return false;
    return normalizeSalesFocusKey(o) === filterKey;
  }

  function isExcludedFromSalesFilterInventory(o) {
    if (!o) return true;
    if (o.archived === true || o.isArchived === true) return true;
    if (o.certificationFixtureOnly === true) return true;
    if (o.isTestData === true) return true;
    if (o.customerVisible === false) return true;
    if (o.prospectSuppressed === true) return true;
    if (o.localActiveInventory === false || o.localInventorySuperseded === true) {
      return true;
    }
    return false;
  }

  function normalizeCommercialStageFilterKey(o) {
    if (o && o.commercialStageFilterKey) {
      return String(o.commercialStageFilterKey).trim() || null;
    }
    var key = String(
      (o && (o.commercialStageKey || (o.tileBadgePlan && o.tileBadgePlan.commercialStageKey))) ||
        ""
    )
      .trim()
      .toUpperCase();
    if (key === "PURSUE_NOW" || key === "SALES_OPPORTUNITY") return "PURSUE_NOW";
    if (key === "FUTURE_CAPTURE" || key === "FUTURE_OPPORTUNITY") return "FUTURE_CAPTURE";
    if (key === "RELATIONSHIP_DEVELOPMENT" || key === "ACCOUNT_DEVELOPMENT") {
      return "RELATIONSHIP_DEVELOPMENT";
    }
    if (
      key === "RESEARCH_NEEDED" ||
      key === "RESEARCH_WATCHLIST" ||
      key === "QUALIFICATION_NEEDED"
    ) {
      return "QUALIFICATION_NEEDED";
    }
    var lane = String((o && o.inventoryLane) || "")
      .trim()
      .toUpperCase();
    if (lane === "SALES_OPPORTUNITY") return "PURSUE_NOW";
    if (lane === "FUTURE_OPPORTUNITY") return "FUTURE_CAPTURE";
    if (lane === "ACCOUNT_DEVELOPMENT") return "RELATIONSHIP_DEVELOPMENT";
    if (lane === "RESEARCH_WATCHLIST") return "QUALIFICATION_NEEDED";
    return null;
  }

  function normalizeHotelSourcingForFilter(o) {
    if (o && o.hotelSourcingStatusNormalized) {
      return String(o.hotelSourcingStatusNormalized).trim();
    }
    var raw = String(
      (o &&
        ((o.eventsOpportunityOverview && o.eventsOpportunityOverview.hotelSourcing) ||
          o.hotelSourcingStatusLabel ||
          o.hotelSourcingStatus)) ||
        ""
    ).trim();
    if (!raw) return null;
    if (/Hotel Sourcing Open/i.test(raw)) return "Hotel Sourcing Open";
    if (/Hotel Sourcing Unconfirmed/i.test(raw)) return "Hotel Sourcing Unconfirmed";
    if (/Venue Confirmed/i.test(raw)) return "Venue Confirmed";
    if (/Historical Venue Only/i.test(raw)) return "Historical Venue Only";
    return null;
  }

  function salesOpportunityKeysOf(o) {
    if (Array.isArray(o && o.salesOpportunityQuickFilterKeys)) {
      return o.salesOpportunityQuickFilterKeys;
    }
    return [];
  }

  function matchesHotelSourcingNotConfirmedFilter(o, subgroup) {
    var sourcing = normalizeHotelSourcingForFilter(o);
    // Locked published inventory may carry the Sales Opportunity key without
    // a separate sourcing label — honor the approved quick-filter membership.
    if (!sourcing) {
      var keys = salesOpportunityKeysOf(o);
      if (
        keys.indexOf(SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED) >= 0
      ) {
        // Treat key-only membership as Unconfirmed for BOTH/UNCONFIRMED chips.
        if (subgroup === HOTEL_SOURCING_SUBGROUP.OPEN) return false;
        return true;
      }
      return false;
    }
    var isOpen = sourcing === "Hotel Sourcing Open";
    var isUnconfirmed = sourcing === "Hotel Sourcing Unconfirmed";
    if (!isOpen && !isUnconfirmed) return false;
    if (subgroup === HOTEL_SOURCING_SUBGROUP.OPEN) return isOpen;
    if (subgroup === HOTEL_SOURCING_SUBGROUP.UNCONFIRMED) return isUnconfirmed;
    return true;
  }

  function matchesSalesOpportunityFilter(o, filterKey, sourcingSubgroup) {
    if (!filterKey) return !isExcludedFromSalesFilterInventory(o);
    if (isExcludedFromSalesFilterInventory(o)) return false;
    if (filterKey === SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED) {
      return matchesHotelSourcingNotConfirmedFilter(
        o,
        sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH
      );
    }
    var keys = salesOpportunityKeysOf(o);
    return keys.indexOf(filterKey) !== -1;
  }

  function salesRelevanceRank(o) {
    var priority = String(
      (o && (o.priorityFilterKey || o.priority)) || ""
    ).toUpperCase();
    var pRank =
      priority === "HIGH_PRIORITY"
        ? 0
        : priority === "MEDIUM_PRIORITY"
          ? 1
          : priority === "WATCHLIST" || priority === "WATCH"
            ? 2
            : 3;
    var stage = normalizeCommercialStageFilterKey(o);
    var sRank =
      stage === "PURSUE_NOW"
        ? 0
        : stage === "FUTURE_CAPTURE"
          ? 1
          : stage === "RELATIONSHIP_DEVELOPMENT"
            ? 2
            : 3;
    var sourcing = normalizeHotelSourcingForFilter(o);
    var srcRank =
      sourcing === "Hotel Sourcing Open"
        ? 0
        : sourcing === "Hotel Sourcing Unconfirmed"
          ? 1
          : sourcing === "Historical Venue Only"
            ? 2
            : 3;
    var fit =
      typeof (o && o.hotelFitScore) === "number"
        ? Math.max(0, 100 - o.hotelFitScore)
        : 50;
    var buyer =
      o &&
      (o.hasVerifiedNamedRelevantContact === true ||
        o.hasCredibleOutreachRoute === true)
        ? 0
        : 1;
    return pRank * 10000 + sRank * 1000 + srcRank * 100 + fit + buyer;
  }

  function normalizePriorityFilterKey(priority) {
    var p = String(priority || "")
      .trim()
      .toUpperCase();
    // Accept overlay aliases so Sales Ready High/Medium never render as Watch.
    if (p === "HIGH_PRIORITY" || p === "HIGH") return "HIGH_PRIORITY";
    if (p === "MEDIUM_PRIORITY" || p === "MEDIUM") return "MEDIUM_PRIORITY";
    if (p === "WATCHLIST" || p === "WATCH" || p === "LOW") return "WATCHLIST";
    if (p === "DISQUALIFIED") return "DISQUALIFIED";
    return "UNCLASSIFIED";
  }

  function countByPriority(opportunities) {
    var counts = {
      "": 0,
      HIGH_PRIORITY: 0,
      MEDIUM_PRIORITY: 0,
      WATCHLIST: 0,
      UNCLASSIFIED: 0,
    };
    (opportunities || []).forEach(function (o) {
      counts[""] += 1;
      // Prefer DTO filter key when present; never silently default missing → Watch.
      var p =
        o.priorityFilterKey ||
        normalizePriorityFilterKey(o.priority);
      if (counts[p] == null) counts[p] = 0;
      counts[p] += 1;
    });
    return counts;
  }

  function normalizeBookingWindowStatus(o) {
    if (o && o.actionTimingFilterKey) {
      return String(o.actionTimingFilterKey).trim() || "UNCLASSIFIED";
    }
    var s =
      o && o.bookingWindowStatus != null
        ? String(o.bookingWindowStatus).trim().toUpperCase()
        : "";
    if (!s || NON_CANONICAL_ACTION_TIMING[s]) return "UNCLASSIFIED";
    if (s === "CONTACT_NOW" || s === "WATCH" || s === "TOO_EARLY") return s;
    if (s === "LIKELY_TOO_LATE") return "TOO_EARLY";
    // QUALIFY_NOW and other postures are not Action Timing filter members.
    return "UNCLASSIFIED";
  }

  function countByActionStatus(opportunities) {
    var counts = {
      "": 0,
      CONTACT_NOW: 0,
      WATCH: 0,
      TOO_EARLY: 0,
      UNCLASSIFIED: 0,
    };
    (opportunities || []).forEach(function (o) {
      counts[""] += 1;
      var s = normalizeBookingWindowStatus(o);
      if (counts[s] == null) counts[s] = 0;
      counts[s] += 1;
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
      commercialStage: filters.commercialStage || "",
      salesOpportunity: filters.salesOpportunity || "",
      salesFocus: filters.salesFocus || "",
      sourcingSubgroup: filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH,
      segment: filters.segment || "",
      territory: filters.territory || "",
      weekly: filters.weekly || "",
      opportunityType: filters.opportunityType || "",
    });
    return countByPriority(subset);
  }

  function countBySalesFocus(opportunities) {
    var counts = {
      "": 0,
      PURSUE_NOW: 0,
      SALES_QUALIFICATION: 0,
    };
    (opportunities || []).forEach(function (o) {
      if (isExcludedFromSalesFilterInventory(o)) return;
      counts[""] += 1;
      var key = normalizeSalesFocusKey(o);
      if (key && counts[key] != null) counts[key] += 1;
    });
    return counts;
  }

  function facetCountBySalesFocus(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      priority: filters.priority || "",
      booking: filters.booking || "",
      commercialStage: filters.commercialStage || "",
      salesOpportunity: filters.salesOpportunity || "",
      sourcingSubgroup: filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH,
      segment: filters.segment || "",
      territory: filters.territory || "",
      weekly: filters.weekly || "",
      opportunityType: filters.opportunityType || "",
      // omit salesFocus so facet counts reflect other active filters
    });
    return countBySalesFocus(subset);
  }

  /**
   * Facet counts for Action Window presets given the other active browse filters.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function facetCountByActionStatus(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      priority: filters.priority || "",
      commercialStage: filters.commercialStage || "",
      salesOpportunity: filters.salesOpportunity || "",
      salesFocus: filters.salesFocus || "",
      sourcingSubgroup: filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH,
      segment: filters.segment || "",
      territory: filters.territory || "",
      weekly: filters.weekly || "",
      opportunityType: filters.opportunityType || "",
    });
    return countByActionStatus(subset);
  }

  function countByCommercialStage(opportunities) {
    var counts = {
      "": 0,
      PURSUE_NOW: 0,
      FUTURE_CAPTURE: 0,
      RELATIONSHIP_DEVELOPMENT: 0,
      QUALIFICATION_NEEDED: 0,
      UNCLASSIFIED: 0,
    };
    (opportunities || []).forEach(function (o) {
      if (isExcludedFromSalesFilterInventory(o)) return;
      counts[""] += 1;
      var stage = normalizeCommercialStageFilterKey(o);
      if (stage && counts[stage] != null) counts[stage] += 1;
      else counts.UNCLASSIFIED += 1;
    });
    return counts;
  }

  function facetCountByCommercialStage(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      priority: filters.priority || "",
      booking: filters.booking || "",
      salesOpportunity: filters.salesOpportunity || "",
      salesFocus: filters.salesFocus || "",
      sourcingSubgroup: filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH,
      segment: filters.segment || "",
      territory: filters.territory || "",
      weekly: filters.weekly || "",
      opportunityType: filters.opportunityType || "",
    });
    return countByCommercialStage(subset);
  }

  function countBySalesOpportunity(opportunities, sourcingSubgroup) {
    var counts = {
      "": 0,
      HOTEL_SOURCING_NOT_CONFIRMED: 0,
      PURSUE_NOW: 0,
      FUTURE_GROUP_BUSINESS: 0,
      COMPETITOR_CAPTURE: 0,
      PARTNERSHIP_OPPORTUNITIES: 0,
      LOCAL_CATERING: 0,
      sourcingOpen: 0,
      sourcingUnconfirmed: 0,
    };
    (opportunities || []).forEach(function (o) {
      if (isExcludedFromSalesFilterInventory(o)) return;
      counts[""] += 1;
      var keys = salesOpportunityKeysOf(o);
      keys.forEach(function (k) {
        if (counts[k] != null) counts[k] += 1;
      });
      // Hotel Sourcing Not Confirmed parent count respects subgroup when counting chip
      if (
        matchesHotelSourcingNotConfirmedFilter(
          o,
          sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH
        )
      ) {
        // already counted via keys when BOTH; for OPEN/UNCONFIRMED subgroup recount
      }
      var sourcing = normalizeHotelSourcingForFilter(o);
      if (sourcing === "Hotel Sourcing Open") counts.sourcingOpen += 1;
      if (sourcing === "Hotel Sourcing Unconfirmed") counts.sourcingUnconfirmed += 1;
    });
    // Recalculate HOTEL_SOURCING_NOT_CONFIRMED against active subgroup
    counts.HOTEL_SOURCING_NOT_CONFIRMED = 0;
    (opportunities || []).forEach(function (o) {
      if (isExcludedFromSalesFilterInventory(o)) return;
      if (
        matchesHotelSourcingNotConfirmedFilter(
          o,
          sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH
        )
      ) {
        counts.HOTEL_SOURCING_NOT_CONFIRMED += 1;
      }
    });
    return counts;
  }

  function facetCountBySalesOpportunity(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      priority: filters.priority || "",
      booking: filters.booking || "",
      commercialStage: filters.commercialStage || "",
      salesFocus: filters.salesFocus || "",
      segment: filters.segment || "",
      territory: filters.territory || "",
      weekly: filters.weekly || "",
      opportunityType: filters.opportunityType || "",
      // omit salesOpportunity + sourcingSubgroup for facet base
    });
    return countBySalesOpportunity(
      subset,
      filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH
    );
  }

  /**
   * Type facet counts from other active browse filters (before selected type).
   * Classification: REUSABLE_PRODUCT_UI
   */
  function opportunityTypeCountsForFilters(opportunities, filters) {
    filters = filters || {};
    var subset = filterOpportunities(opportunities, {
      priority: filters.priority || "",
      booking: filters.booking || "",
      commercialStage: filters.commercialStage || "",
      salesOpportunity: filters.salesOpportunity || "",
      salesFocus: filters.salesFocus || "",
      sourcingSubgroup: filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH,
      segment: filters.segment || "",
      territory: filters.territory || "",
      weekly: filters.weekly || "",
      // intentionally omit opportunityType so counts reflect other active filters
    });
    return countByOpportunityTypeFilter(subset);
  }

  /**
   * Clear companion filters when the pair would return zero rows.
   * Classification: REUSABLE_PRODUCT_UI
   */
  function reconcileBrowseFilters(opportunities, filters, changedKey) {
    filters = Object.assign(
      {
        priority: "",
        booking: "",
        commercialStage: "",
        salesOpportunity: "",
        salesFocus: "",
        sourcingSubgroup: HOTEL_SOURCING_SUBGROUP.BOTH,
        segment: "",
        territory: "",
        weekly: "",
        opportunityType: "",
      },
      filters || {}
    );
    if (!filters.sourcingSubgroup) {
      filters.sourcingSubgroup = HOTEL_SOURCING_SUBGROUP.BOTH;
    }
    if (!filterOpportunities(opportunities, filters).length) {
      if (changedKey === "priority") {
        filters.booking = "";
      } else if (changedKey === "booking") {
        filters.priority = "";
      } else if (changedKey === "commercialStage") {
        filters.salesOpportunity = "";
      } else if (changedKey === "salesOpportunity") {
        filters.commercialStage = "";
      } else if (changedKey === "weekly") {
        filters.priority = "";
        filters.booking = "";
      } else if (changedKey === "opportunityType") {
        filters.priority = "";
        filters.booking = "";
        filters.weekly = "";
      }
    }
    return filters;
  }

  function resetBrowseFilters() {
    return {
      priority: "",
      booking: "",
      commercialStage: "",
      salesOpportunity: "",
      sourcingSubgroup: HOTEL_SOURCING_SUBGROUP.BOTH,
      segment: "",
      territory: "",
      weekly: "",
      opportunityType: "",
    };
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
      "<span>Weekly Change</span>" +
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
    // Customer-facing Priority presets: All / High / Medium / Watch only.
    // Unclassified remains an internal data-quality key — never a preset chip.
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
      '<div class="gdi-browse-preset gdi-browse-preset--priority">' +
      '<div class="chain-scale-legend gdi-browse-legend gdi-browse-legend--priority gdi-preset-panel" role="group" aria-label="Priority filters">' +
      '<span class="gdi-preset-panel__title">Priority</span>' +
      '<div class="gdi-preset-panel__chips">' +
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
      "</div></div></div>"
    );
  }

  /**
   * Action Timing legend — when a salesperson should act (bookingWindowStatus).
   * Distinct from Weekly Change (research freshness) and Event Date.
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
        value: "WATCH",
        label: "Future Watch",
        swatch: ACTION_SWATCH.WATCH,
      },
      {
        value: "TOO_EARLY",
        label: "Too Early",
        swatch: ACTION_SWATCH.TOO_EARLY,
      },
      {
        value: "UNCLASSIFIED",
        label: "Unclassified",
        swatch: ACTION_SWATCH.WATCH,
      },
    ];
    return (
      '<div class="chain-scale-legend gdi-browse-legend" role="group" aria-label="Action Timing filters" title="When sales should investigate or contact — not the event date, and not research freshness.">' +
      "<span>Action Timing</span>" +
      presets
        .map(function (p) {
          var n =
            p.value === ""
              ? counts[""] != null
                ? counts[""]
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
        var key =
          o.priorityFilterKey || normalizePriorityFilterKey(o.priority);
        return key === filters.priority;
      });
    }
    if (filters.segment) {
      rows = rows.filter(function (o) {
        return o.segment === filters.segment;
      });
    }
    if (filters.booking) {
      rows = rows.filter(function (o) {
        return normalizeBookingWindowStatus(o) === filters.booking;
      });
    }
    if (filters.commercialStage) {
      rows = rows.filter(function (o) {
        if (isExcludedFromSalesFilterInventory(o)) return false;
        return (
          normalizeCommercialStageFilterKey(o) === filters.commercialStage
        );
      });
    }
    if (filters.salesOpportunity) {
      rows = rows.filter(function (o) {
        return matchesSalesOpportunityFilter(
          o,
          filters.salesOpportunity,
          filters.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH
        );
      });
    }
    if (filters.salesFocus) {
      rows = rows.filter(function (o) {
        return matchesSalesFocusFilter(o, filters.salesFocus);
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
    if (filters.opportunityType) {
      rows = rows.filter(function (o) {
        return classifyOpportunityTypeFilter(o) === filters.opportunityType;
      });
    }
    // Customer GDI: Priority + Sales Opportunities (+ Commercial Stage when set in state).
    // Ready vs Watch remains visible on cards; Action Timing remains available when set.
    return rows;
  }

  /** @deprecated Customer workflow chip row removed — always empty. */
  function normalizeCustomerWorkflowFilter() {
    return "";
  }

  /** @deprecated Customer workflow chip row removed — render nothing. */
  function workflowPresetHtml() {
    return "";
  }

  function sortOpportunities(rows, sortKey, sortDir) {
    var key = sortKey || "priority";
    var dir = sortDir == null ? 1 : sortDir;
    rows = (rows || []).slice();
    rows.sort(function (a, b) {
      var av = a[key];
      var bv = b[key];
      if (key === "salesRelevance") {
        var d = salesRelevanceRank(a) - salesRelevanceRank(b);
        if (d !== 0) return d * dir;
        av = a.eventStartDate || a.tileEventStartDate || "";
        bv = b.eventStartDate || b.tileEventStartDate || "";
        if (av && bv && av !== bv) return String(av).localeCompare(String(bv)) * dir;
        return String(a.displayTitle || a.title || "").localeCompare(
          String(b.displayTitle || b.title || "")
        );
      }
      if (key === "priority") {
        // Prefer High → Medium → Watch, then sales relevance, then date
        av = PRIORITY_RANK[a.priorityFilterKey || a.priority];
        bv = PRIORITY_RANK[b.priorityFilterKey || b.priority];
        av = av != null ? av : 9;
        bv = bv != null ? bv : 9;
        if (av !== bv) return (av - bv) * dir;
        var sr = salesRelevanceRank(a) - salesRelevanceRank(b);
        if (sr !== 0) return sr * dir;
        var aLane = INVENTORY_LANE_RANK[a.inventoryLane];
        var bLane = INVENTORY_LANE_RANK[b.inventoryLane];
        if (aLane != null || bLane != null) {
          av = aLane != null ? aLane : 9;
          bv = bLane != null ? bLane : 9;
          if (av !== bv) return (av - bv) * dir;
        }
        return 0;
      }
      if (key === "hotelFitScore" || key === "evidenceConfidence") {
        av = typeof av === "number" ? av : -1;
        bv = typeof bv === "number" ? bv : -1;
        return (bv - av) * dir;
      }
      if (key === "eventStartDate") {
        av = a.eventStartDate || a.tileEventStartDate || a.eventTiming || "";
        bv = b.eventStartDate || b.tileEventStartDate || b.eventTiming || "";
        // Empty dates sort after dated future events
        if (!av && bv) return 1 * dir;
        if (av && !bv) return -1 * dir;
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
      title: o.displayTitle || o.title,
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
      whyNow: o.watchCardWhenToAct || o.whyNow,
      recommendedNextStep:
        o.watchCardRecommendedNextStep ||
        o.recommendedAction ||
        o.recommendedNextStep,
      watchCardWhyMatters: o.watchCardWhyMatters || null,
      watchCardCurrentStatus: o.watchCardCurrentStatus || null,
      watchCardLodgingController: o.watchCardLodgingController || null,
      watchCardHotelSelectionStatus: o.watchCardHotelSelectionStatus || null,
      watchCardWhenToAct: o.watchCardWhenToAct || null,
      watchCardNextTrigger: o.watchCardNextTrigger || o.nextTriggerCondition || null,
      watchCardMonitoringStatus: o.watchCardMonitoringStatus || null,
      watchCardMonitoringLastChecked: o.watchCardMonitoringLastChecked || null,
      watchCardMonitoringNextExpected: o.watchCardMonitoringNextExpected || null,
      watchCardRecommendedNextStep: o.watchCardRecommendedNextStep || null,
      primaryContact: o.primaryContact,
      buyerEntity: o.buyerEntity || null,
      buyerRole: o.buyerRole || null,
      publicContactPath: o.publicContactPath || null,
      whoPathClass: o.whoPathClass || o.contactPathClass || null,
      readinessState: o.readinessState || null,
      customerFacingState: o.customerFacingState || null,
      opportunityQualificationLabel:
        o.opportunityQualificationLabel || o.opportunityQualification,
      priority: o.priority,
      bookingWindowStatus: o.bookingWindowStatus,
      pursuitId: o.pursuitId || null,
      pursuitStatus: o.pursuitStatus || null,
      canStartPursuit: o.canStartPursuit === true,
      outreachReadiness: o.outreachReadiness || null,
    };
  }

  function pursuitActionButtonHtml(it, linked) {
    linked = linked || {};
    var pursuitId = linked.pursuitId || it.pursuitId;
    var canStart = linked.canStartPursuit === true || it.canStartPursuit === true;
    var openId = it.opportunityId || it.id;
    if (pursuitId) {
      return (
        '<button type="button" class="gdi-btn gdi-btn-secondary" data-pursuit-view="' +
        esc(pursuitId) +
        '" data-open="' +
        esc(openId) +
        '">View Pursuit</button>'
      );
    }
    if (canStart) {
      return (
        '<button type="button" class="gdi-btn gdi-btn-secondary" data-pursuit-start="' +
        esc(openId) +
        '">Start Pursuit</button>'
      );
    }
    return "";
  }

  function pursuitPanelHtml(pursuit) {
    if (!pursuit) {
      return '<p class="gdi-muted">No active pursuit for this opportunity.</p>';
    }
    return (
      '<div class="gdi-pursuit-panel">' +
      "<ul>" +
      "<li><strong>Pursuit status:</strong> " +
      esc(pursuit.pursuitStatus || "—") +
      "</li>" +
      "<li><strong>GDI status:</strong> " +
      esc(pursuit.gdiFacingState || "—") +
      "</li>" +
      "<li><strong>Outreach readiness:</strong> " +
      esc(pursuit.outreachReadiness || "—") +
      "</li>" +
      "<li><strong>Hotel-selection status:</strong> " +
      esc(pursuit.hotelInclusionStatus || "—") +
      "</li>" +
      "<li><strong>Controller:</strong> " +
      esc(pursuit.contactOrganization || pursuit.controllerName || "—") +
      "</li>" +
      "<li><strong>Contact:</strong> " +
      esc(
        [pursuit.contactName, pursuit.contactRole, pursuit.contactEmail]
          .filter(Boolean)
          .join(" · ") || "—"
      ) +
      "</li>" +
      (pursuit.controllerOutreachEvidenceQuestion || pursuit.evidenceQuestion
        ? "<li><strong>Evidence question:</strong> " +
          esc(pursuit.controllerOutreachEvidenceQuestion || pursuit.evidenceQuestion) +
          "</li>"
        : "") +
      "<li><strong>Last outreach:</strong> " +
      esc(pursuit.lastContactDate || "—") +
      "</li>" +
      "<li><strong>Sent status:</strong> " +
      esc(
        pursuit.responseStatus === "SENT" || pursuit.lastContactDate
          ? "SENT"
          : pursuit.draftMessage
            ? "DRAFT_NOT_SENT"
            : "—"
      ) +
      "</li>" +
      "<li><strong>Next follow-up:</strong> " +
      esc(pursuit.nextFollowUpDate || "—") +
      "</li>" +
      "<li><strong>Next action:</strong> " +
      esc(pursuit.nextAction || "—") +
      "</li>" +
      "<li><strong>Next trigger:</strong> " +
      esc(pursuit.nextTrigger || "—") +
      "</li>" +
      (pursuit.controllerOutreachStructuredFacts || pursuit.structuredResponseFacts
        ? "<li><strong>Response facts:</strong> " +
          esc(
            typeof (pursuit.controllerOutreachStructuredFacts ||
              pursuit.structuredResponseFacts) === "string"
              ? pursuit.controllerOutreachStructuredFacts || pursuit.structuredResponseFacts
              : JSON.stringify(
                  pursuit.controllerOutreachStructuredFacts || pursuit.structuredResponseFacts
                )
          ) +
          "</li>"
        : "") +
      "<li><strong>Decision window:</strong> " +
      esc(
        (pursuit.decisionWindowStart || "—") +
          " → " +
          (pursuit.decisionWindowEnd || "—")
      ) +
      "</li>" +
      "<li><strong>Response:</strong> " +
      esc(pursuit.responseStatus || "—") +
      "</li>" +
      "<li><strong>Outcome:</strong> " +
      esc(pursuit.outcome || "—") +
      "</li>" +
      "<li><strong>Assigned:</strong> " +
      esc(pursuit.assignedTo || "UNASSIGNED") +
      "</li></ul>" +
      (pursuit.draftSubject
        ? '<div class="gdi-brief-block"><div class="gdi-meta-label">Draft subject</div><p>' +
          esc(pursuit.draftSubject) +
          "</p></div>"
        : "") +
      (pursuit.draftMessage
        ? '<div class="gdi-brief-block"><div class="gdi-meta-label">Draft message (' +
          esc(pursuit.draftLanguage || "es") +
          ")</div><pre class=\"gdi-outreach-draft\">" +
          esc(pursuit.draftMessage) +
          "</pre></div>"
        : "") +
      (pursuit.notes
        ? '<div class="gdi-brief-block"><div class="gdi-meta-label">Notes</div><p>' +
          esc(pursuit.notes) +
          "</p></div>"
        : "") +
      "</div>"
    );
  }

  /**
   * Commercial class (Priority / Exploratory) — separate from Action Window and WATCH stamp.
   */
  function commercialClassPillHtml(it, linked) {
    linked = linked || {};
    it = it || {};
    var section = String(
      linked.prospectReviewSection || it.prospectReviewSection || ""
    ).toUpperCase();
    // Prefer opportunity-specific commercial category over generic
    // "Corporate Account Relationship" repeated across cards.
    var specific =
      linked.commercialCategory ||
      it.commercialCategory ||
      linked.opportunityTypeLabel ||
      it.opportunityTypeLabel ||
      "";
    if (
      specific &&
      !/^corporate account relationship$/i.test(String(specific).trim())
    ) {
      return (
        '<span class="gdi-tile-pill gdi-pill-watch" data-facet="commercial-class">' +
        esc(specific) +
        "</span>"
      );
    }
    var label =
      linked.prospectCustomerLabel ||
      it.prospectCustomerLabel ||
      linked.prospectCommercialLabel ||
      it.prospectCommercialLabel ||
      specific ||
      "";
    var lbl = String(label || "");
    // Packaging class — must not read as the Priority High/Medium/Watch filter.
    if (section === "PRIORITY" || /prospect to qualify|core prospect/i.test(lbl)) {
      return (
        '<span class="gdi-tile-pill gdi-pill-high" data-facet="commercial-class">Core prospect</span>'
      );
    }
    if (section === "EXPLORATORY" || /exploratory/i.test(lbl)) {
      return (
        '<span class="gdi-tile-pill gdi-pill-med" data-facet="commercial-class">Potential opportunity</span>'
      );
    }
    if (lbl && !/^corporate account relationship$/i.test(lbl.trim())) {
      return (
        '<span class="gdi-tile-pill gdi-pill-watch" data-facet="commercial-class">' +
        esc(lbl) +
        "</span>"
      );
    }
    return "";
  }

  function readinessPillHtml(it, linked) {
    // Inventory Visibility V1 — honest lane label (does not claim Ready).
    var invLane = String(
      (linked && linked.inventoryLane) || (it && it.inventoryLane) || ""
    ).toUpperCase();
    var invLabel =
      (linked && linked.inventoryLaneLabel) ||
      (it && it.inventoryLaneLabel) ||
      "";
    if (invLane && invLabel) {
      var invCls =
        invLane === "SALES_OPPORTUNITY"
          ? "gdi-pill-high"
          : invLane === "ACCOUNT_DEVELOPMENT"
            ? "gdi-pill-med"
            : invLane === "EXCLUDED"
              ? "gdi-pill-watch"
              : "gdi-pill-action-watch";
      return (
        '<span class="gdi-tile-pill ' +
        invCls +
        '" data-facet="inventory-lane" title="' +
        esc(
          ((linked && linked.inventoryWhyNotSalesReady) ||
            (it && it.inventoryWhyNotSalesReady) ||
            [])
            .slice(0, 4)
            .join(", ")
        ) +
        '">' +
        esc(invLabel) +
        "</span>"
      );
    }
    // Maturity / readiness badges — QUALIFIED (pilot) vs ACTIONABLE (Ready) vs Future Watch.
    var maturity = String(
      (linked && linked.gdiMaturityState) || (it && it.gdiMaturityState) || ""
    ).toUpperCase();
    if (maturity === "QUALIFIED") {
      return (
        '<span class="gdi-tile-pill gdi-pill-action-qualify" data-facet="maturity">QUALIFIED</span>'
      );
    }
    if (maturity === "ACTIONABLE") {
      return (
        '<span class="gdi-tile-pill gdi-pill-action-pursue" data-facet="maturity">ACTIONABLE</span>'
      );
    }
    var facing = String(
      (linked && linked.customerFacingState) ||
        (it && it.customerFacingState) ||
        ""
    ).toUpperCase();
    var readiness = String(
      (linked && linked.readinessState) || (it && it.readinessState) || ""
    ).toUpperCase();
    if (
      facing === "FUTURE_WATCH" ||
      (/WATCH/.test(facing) && readiness !== "READY")
    ) {
      return (
        '<span class="gdi-tile-pill gdi-pill-action-watch" data-facet="readiness">Future Watch</span>'
      );
    }
    return "";
  }

  /**
   * Presentation-only: pick ONE compact contact block for the opportunity tile.
   * Full person + organization routes stay in the drawer / detail payloads.
   */
  function isGenericOrgEmailLocal(email) {
    var raw = String(email || "").trim();
    var e = raw.toLowerCase();
    if (!e || e.indexOf("@") < 0) return false;
    var localRaw = raw.split("@")[0] || "";
    var local = e.split("@")[0] || "";
    if (
      /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|enquiries|reception|front.?desk|general|mail|team|hr|press|media|pr|federal|help|customerservice|customer.?service|newbiz|events|meetings|talentandculture|talent.?culture)$/i.test(
        local
      )
    ) {
      return true;
    }
    // CamelCase or concatenated department inboxes (TalentandCulture, NewBiz) are org routes
    if (
      !/[._-]/.test(localRaw) &&
      (/[a-z][A-Z]/.test(localRaw) ||
        /(talent|culture|people|federal|press|media|events|meetings|support|sales|office|admin)/i.test(
          local
        ))
    ) {
      return true;
    }
    return false;
  }

  function shortContactRoleLabel(role) {
    var r = String(role || "")
      .replace(/\s+/g, " ")
      .trim();
    if (!r) return "";
    if (r.length > 56) {
      var cut = r.slice(0, 56);
      var sp = cut.lastIndexOf(" ");
      r = (sp > 28 ? cut.slice(0, sp) : cut).trim() + "…";
    }
    return r;
  }

  function isReferralContactRole(it, linked, role) {
    var blob = [
      role,
      linked.contactResponsibilityClass || it.contactResponsibilityClass || "",
      linked.contactResponsibility || it.contactResponsibility || "",
      linked.recommendedAction || it.recommendedAction || "",
    ].join(" ");
    return (
      linked.contactResponsibilityClass === "REFERRAL_ENTRY" ||
      it.contactResponsibilityClass === "REFERRAL_ENTRY" ||
      /intermediary|referral|not (a )?verified|not confirmed|lead to verify|not automatically the hotel buyer|not identified as the event buyer|sponsorship \/ exhibition|press inquiries/i.test(
        blob
      )
    );
  }

  function usableContactPath(url) {
    var u = String(url || "").trim();
    if (!u) return "";
    if (/^https?:\/\/[^/]+\/?$/i.test(u)) return "";
    if (/\/(en\/)?home\/?$/i.test(u)) return "";
    return u;
  }

  function selectCompactTileContact(it, linked) {
    linked = linked || {};
    it = it || {};
    var contact = linked.primaryContact || it.primaryContact || null;
    var contactName =
      contact && contact.name && contact.name !== "UNKNOWN" ? contact.name : "";
    var contactEmail = contact && contact.email ? String(contact.email).trim() : "";
    var contactPhone = contact && contact.phone ? String(contact.phone).trim() : "";
    var role = shortContactRoleLabel(
      (contact && (contact.role || contact.title)) ||
        linked.primaryContactRole ||
        it.primaryContactRole ||
        linked.buyerRole ||
        it.buyerRole ||
        linked.contactRole ||
        it.contactRole ||
        ""
    );
    var channelSep = linked.contactChannelSeparation || it.contactChannelSeparation || {};
    var personEmail =
      (channelSep.personSpecificEmail && String(channelSep.personSpecificEmail).trim()) ||
      contactEmail ||
      "";
    var personPhone =
      (channelSep.personSpecificPhone && String(channelSep.personSpecificPhone).trim()) ||
      contactPhone ||
      "";
    if (personEmail && isGenericOrgEmailLocal(personEmail)) personEmail = "";
    if (
      contact &&
      contact.emailOwnership === "organization_or_department" &&
      personEmail
    ) {
      personEmail = "";
    }

    var orgEmail =
      linked.organizationContactEmail ||
      it.organizationContactEmail ||
      linked.functionalContactEmail ||
      it.functionalContactEmail ||
      (channelSep.organizationRoute &&
      String(channelSep.organizationRoute).indexOf("@") >= 0
        ? channelSep.organizationRoute
        : "") ||
      "";
    if (
      !orgEmail &&
      linked.contactEmail &&
      isGenericOrgEmailLocal(linked.contactEmail)
    ) {
      orgEmail = linked.contactEmail;
    }
    if (
      !orgEmail &&
      it.contactEmail &&
      isGenericOrgEmailLocal(it.contactEmail)
    ) {
      orgEmail = it.contactEmail;
    }
    // Misfiled person_specific that is actually the org inbox
    if (
      !orgEmail &&
      contactEmail &&
      isGenericOrgEmailLocal(contactEmail)
    ) {
      orgEmail = contactEmail;
    }
    orgEmail = orgEmail ? String(orgEmail).trim() : "";
    if (
      personEmail &&
      orgEmail &&
      personEmail.toLowerCase() === orgEmail.toLowerCase()
    ) {
      if (isGenericOrgEmailLocal(personEmail)) {
        // Shared department inbox — do not attribute to the person on the tile
        personEmail = "";
      } else {
        // Person-looking email duplicated onto org fields — keep as personal channel
        orgEmail = "";
      }
    }
    if (
      !orgEmail &&
      channelSep.organizationRoute &&
      String(channelSep.organizationRoute).indexOf("@") >= 0 &&
      isGenericOrgEmailLocal(channelSep.organizationRoute)
    ) {
      orgEmail = String(channelSep.organizationRoute).trim();
    }

    var companyPhone =
      linked.companyPhone ||
      it.companyPhone ||
      (channelSep.companyPhone ? channelSep.companyPhone : "") ||
      "";
    if (!companyPhone && !personPhone) {
      companyPhone = linked.contactPhone || it.contactPhone || "";
    }
    companyPhone = companyPhone ? String(companyPhone).trim() : "";
    // Reject ISO dates / research timestamps mistaken for telephone numbers
    if (
      companyPhone &&
      (/^(?:19|20)\d{2}[-/.]\d{1,2}(?:[-/.]\d{1,2})?$/.test(companyPhone) ||
        companyPhone.replace(/\D/g, "").length < 8)
    ) {
      companyPhone = "";
    }
    if (
      personPhone &&
      (/^(?:19|20)\d{2}[-/.]\d{1,2}(?:[-/.]\d{1,2})?$/.test(String(personPhone).trim()) ||
        String(personPhone).replace(/\D/g, "").length < 8)
    ) {
      personPhone = "";
    }
    if (
      companyPhone &&
      personPhone &&
      companyPhone.replace(/\D/g, "") === personPhone.replace(/\D/g, "")
    ) {
      companyPhone = "";
    }

    var contactPath = usableContactPath(
      linked.contactUrl ||
        it.contactUrl ||
        linked.publicContactPath ||
        it.publicContactPath ||
        (channelSep.organizationRoute &&
        String(channelSep.organizationRoute).indexOf("@") < 0
          ? channelSep.organizationRoute
          : "") ||
        ""
    );
    var deptLabel = shortContactRoleLabel(
      linked.buyerEntity ||
        it.buyerEntity ||
        linked.functionalContactLabel ||
        it.functionalContactLabel ||
        ""
    );
    var isReferral = isReferralContactRole(it, linked, role);
    var orgName =
      linked.organizationName ||
      it.organizationName ||
      linked.buyerEntity ||
      it.buyerEntity ||
      "";
    var nameLooksLikeOrgEntity =
      contactName &&
      orgName &&
      String(contactName).toLowerCase() === String(orgName).toLowerCase();
    var hasPersonChannel = Boolean(personEmail || personPhone);
    var orgTileName =
      deptLabel ||
      (orgName ? shortContactRoleLabel(orgName) : "") ||
      "Organization contact";

    // Named person with a usable personal channel. Referral keeps the person
    // name and uses a short "Referral contact" role — never dump narratives.
    if (contactName && hasPersonChannel && !nameLooksLikeOrgEntity) {
      return {
        kind: isReferral ? "referral" : "person",
        name: contactName,
        role: isReferral ? "Referral contact" : role,
        channelKind: personEmail ? "email" : "phone",
        channelText: personEmail || personPhone,
        channelHref: "",
      };
    }
    // Named verified person without personal email/phone yet — still show
    // name + role on Scaling tiles (Surfe retrieve lives in the drawer).
    if (contactName && !nameLooksLikeOrgEntity && looksLikePersonNameLocal(contactName)) {
      return {
        kind: isReferral ? "referral" : "person",
        name: contactName,
        role: isReferral ? "Referral contact" : role || "Relevant contact",
        channelKind: "",
        channelText: "",
        channelHref: "",
      };
    }

    // Org-named "contact" rows often misfile the venue/department email as
    // person_specific — promote that email as the organization channel before
    // falling through to a form/page URL.
    if (nameLooksLikeOrgEntity && !orgEmail && personEmail) {
      orgEmail = personEmail;
      personEmail = "";
    }

    // Named org entity with a usable organization route
    if (nameLooksLikeOrgEntity && (personPhone || companyPhone || orgEmail || contactPath)) {
      if (orgEmail) {
        return {
          kind: "organization",
          name: orgTileName === "Organization contact" ? contactName || orgTileName : orgTileName,
          role: "",
          channelKind: "email",
          channelText: orgEmail,
          channelHref: "",
        };
      }
      if (personPhone || companyPhone) {
        return {
          kind: "organization",
          name: orgTileName === "Organization contact" ? contactName || orgTileName : orgTileName,
          role: "",
          channelKind: "phone",
          channelText: personPhone || companyPhone,
          channelHref: "",
        };
      }
    }

    if (orgEmail) {
      return {
        kind: "organization",
        name: orgTileName,
        role: "",
        channelKind: "email",
        channelText: orgEmail,
        channelHref: "",
      };
    }
    if (companyPhone) {
      return {
        kind: "organization",
        name: orgTileName,
        role: "",
        channelKind: "phone",
        channelText: companyPhone,
        channelHref: "",
      };
    }
    if (contactPath) {
      var formLike = /contact|form|inquiry|enquiry|reach|get.?in.?touch/i.test(
        contactPath
      );
      return {
        kind: "organization",
        name: orgTileName,
        role: "",
        channelKind: "form",
        channelText: formLike ? "Contact form" : "Contact page",
        channelHref: isSafeHttpUrl(contactPath) ? contactPath : "",
      };
    }

    return {
      kind: "empty",
      name: "",
      role: "",
      channelKind: "",
      channelText: "Office operations contact not yet identified",
      channelHref: "",
    };
  }

  /**
   * Universal tile contact footer V2 — ONE name + verified professional title
   * (else opportunity role, else Role Not Yet Verified). Or Buyer Not Yet Identified.
   * Never shows Contact Under Review / eligibility enums on the tile.
   */
  function resolveTileContactPreviewBlock(it, linked) {
    linked = linked || {};
    it = it || {};
    var preview =
      linked.tileContactPreview ||
      it.tileContactPreview ||
      null;
    var FORBIDDEN = {
      "Contact Under Review": true,
      "Relevant Contact": true,
      "Relevant contact": true,
      CONTACT_RELEVANT: true,
      INELIGIBLE_UNSPECIFIED: true,
      "Buyer Eligibility Pending": true,
      "Unverified Buyer": true,
      "Role Unverified": true,
      "Relevant Influencer": true,
      "Potential Buyer": true,
      "Decision Maker": true,
    };
    var OPPORTUNITY_ROLES = {
      "Event Organizer": true,
      "Meeting Planner": true,
      "Group Travel Coordinator": true,
      "Group Travel Contact": true,
      "Corporate Travel Contact": true,
      "Referral Contact": true,
    };
    function sanitizeDisplayRole(raw) {
      var r = shortContactRoleLabel(raw);
      if (!r || FORBIDDEN[r]) return "Role Not Yet Verified";
      if (/^relevant contact$/i.test(r)) return "Role Not Yet Verified";
      if (/referral/i.test(r)) return "Referral Contact";
      if (/event organizer/i.test(r)) return "Event Organizer";
      if (/meeting planner/i.test(r)) return "Meeting Planner";
      if (/corporate travel/i.test(r)) return "Corporate Travel Contact";
      if (/group travel|hotel sourc/i.test(r)) return "Group Travel Coordinator";
      if (/decision[-\s]?maker|potential buyer|contact under review|role unverified/i.test(r)) {
        return "Role Not Yet Verified";
      }
      return toProperCaseContactRoleLabel(r);
    }
    if (preview && preview.kind === "named" && preview.name) {
      var role = sanitizeDisplayRole(
        preview.displayRole || preview.roleLabel || ""
      );
      return {
        kind: "named",
        name: String(preview.name).replace(/\s+/g, " ").trim(),
        role: role,
      };
    }
    if (
      preview &&
      (preview.kind === "none" ||
        preview.displayName === "Buyer Not Yet Identified")
    ) {
      return { kind: "none", name: "Buyer Not Yet Identified", role: "" };
    }
    // Client fallback when DTO missing — title first, then opportunity role.
    function fallbackRoleFromPerson(person) {
      person = person || {};
      var title = shortContactRoleLabel(
        person.professionalTitle || person.jobTitle || person.title || ""
      );
      if (
        title &&
        !FORBIDDEN[title] &&
        !OPPORTUNITY_ROLES[title] &&
        !/^(decision[-\s]?maker|potential buyer)$/i.test(title)
      ) {
        return title;
      }
      var classLabel = shortContactRoleLabel(
        person.contactClassificationLabel || person.roleClass || person.role || ""
      );
      if (OPPORTUNITY_ROLES[classLabel]) return classLabel;
      if (/event organizer/i.test(classLabel)) return "Event Organizer";
      if (/meeting planner/i.test(classLabel)) return "Meeting Planner";
      if (/referral/i.test(classLabel)) return "Referral Contact";
      if (/corporate travel/i.test(classLabel)) return "Corporate Travel Contact";
      if (/group travel|hotel sourc/i.test(classLabel)) {
        return "Group Travel Coordinator";
      }
      return "Role Not Yet Verified";
    }
    var two = linked.scalingTwoContactPresentation || it.scalingTwoContactPresentation;
    var slot =
      (two &&
        two.primary &&
        two.primary.customerFacingNamed &&
        two.primary.name &&
        looksLikePersonNameLocal(two.primary.name) &&
        two.primary) ||
      (two &&
        two.secondary &&
        two.secondary.customerFacingNamed &&
        two.secondary.name &&
        looksLikePersonNameLocal(two.secondary.name) &&
        two.secondary) ||
      null;
    if (slot && slot.name) {
      return {
        kind: "named",
        name: String(slot.name).replace(/\s+/g, " ").trim(),
        role: toProperCaseContactRoleLabel(fallbackRoleFromPerson(slot)),
      };
    }
    var pc =
      (linked.primaryContact && linked.primaryContact.name && linked.primaryContact) ||
      (it.primaryContact && it.primaryContact.name && it.primaryContact) ||
      null;
    if (pc && looksLikePersonNameLocal(pc.name)) {
      return {
        kind: "named",
        name: String(pc.name).replace(/\s+/g, " ").trim(),
        role: toProperCaseContactRoleLabel(fallbackRoleFromPerson(pc)),
      };
    }
    var sc =
      (linked.secondaryContact && linked.secondaryContact.name && linked.secondaryContact) ||
      (it.secondaryContact && it.secondaryContact.name && it.secondaryContact) ||
      null;
    if (sc && looksLikePersonNameLocal(sc.name)) {
      return {
        kind: "named",
        name: String(sc.name).replace(/\s+/g, " ").trim(),
        role: toProperCaseContactRoleLabel(fallbackRoleFromPerson(sc)),
      };
    }
    return { kind: "none", name: "Buyer Not Yet Identified", role: "" };
  }

  function compactTileContactHtml(block) {
    block = block || {};
    if (block.kind === "none" || block.kind === "empty" || !block.name) {
      return (
        '<div class="brand-card__contact-name brand-card__contact-name--unidentified">' +
        esc(block.name || "Buyer Not Yet Identified") +
        "</div>"
      );
    }
    var bits = [];
    bits.push(
      '<div class="brand-card__contact-name">' + esc(block.name) + "</div>"
    );
    if (block.role) {
      bits.push(
        '<div class="brand-card__contact-line brand-card__contact-line--role">' +
          esc(toProperCaseContactRoleLabel(block.role)) +
          "</div>"
      );
    }
    return bits.join("");
  }

  /** Canonical revenue-stream classes — shared with multi-revenue qualification V1. */
  var HOTEL_REVENUE_STREAM_CLASS = {
    GUESTROOMS_ONLY: "GUESTROOMS_ONLY",
    EVENT_FNB_ONLY: "EVENT_FNB_ONLY",
    COMBINED_GUESTROOMS_EVENT_FNB: "COMBINED_GUESTROOMS_EVENT_FNB",
    REVENUE_STREAM_UNCONFIRMED: "REVENUE_STREAM_UNCONFIRMED",
  };

  var REVENUE_STREAM_ICON_LABEL = {
    guestrooms: "Guestrooms",
    eventFnb: "Meetings, Events & Catering",
  };

  /** Lucide-compatible bed-double path set (24×24, stroke currentColor). */
  function gdiSvgBedDouble() {
    return (
      '<svg class="gdi-revenue-stream-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M2 20v-8a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v8"/>' +
      '<path d="M4 10V6a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v4"/>' +
      '<path d="M12 4v6"/>' +
      '<path d="M2 18h20"/>' +
      "</svg>"
    );
  }

  /** Lucide-compatible utensils path set. */
  function gdiSvgUtensils() {
    return (
      '<svg class="gdi-revenue-stream-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M3 2v7c0 1.1.9 2 2 2h4a2 2 0 0 0 2-2V2"/>' +
      '<path d="M7 2v20"/>' +
      '<path d="M21 15V2a5 5 0 0 0-5 5v6c0 1.1.9 2 2 2h3Zm0 0v7"/>' +
      "</svg>"
    );
  }

  /** Lucide-compatible calendar-check (verified need-date match). */
  function gdiSvgCalendarCheck() {
    return (
      '<svg class="gdi-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M8 2v4"/>' +
      '<path d="M16 2v4"/>' +
      '<rect width="18" height="18" x="3" y="4" rx="2"/>' +
      '<path d="M3 10h18"/>' +
      '<path d="m9 16 2 2 4-4"/>' +
      "</svg>"
    );
  }

  /** Lucide-compatible calendar-clock (potentially flexible). */
  function gdiSvgCalendarClock() {
    return (
      '<svg class="gdi-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M16 14v2.2l1.6 1"/>' +
      '<path d="M16 2v4"/>' +
      '<path d="M21 7.5V6a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h7.5"/>' +
      '<path d="M3 10h5"/>' +
      '<path d="M8 2v4"/>' +
      '<circle cx="16" cy="16" r="6"/>' +
      "</svg>"
    );
  }

  /** Lucide-compatible calendar-search (timing research required). */
  function gdiSvgCalendarSearch() {
    return (
      '<svg class="gdi-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M16 2v4"/>' +
      '<path d="M21 14v-8a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h8"/>' +
      '<path d="M3 10h18"/>' +
      '<path d="M8 2v4"/>' +
      '<circle cx="18" cy="18" r="3"/>' +
      '<path d="m22 22-1.5-1.5"/>' +
      "</svg>"
    );
  }

  /** Hotel-level: dashed/estimated calendar (MARKET_ESTIMATED). Same 24×24 stroke style. */
  function gdiSvgCalendarDashed() {
    return (
      '<svg class="gdi-need-date-svg gdi-hotel-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="3 2" aria-hidden="true" focusable="false">' +
      '<path d="M8 2v4"/>' +
      '<path d="M16 2v4"/>' +
      '<rect width="18" height="18" x="3" y="4" rx="2"/>' +
      '<path d="M3 10h18"/>' +
      "</svg>"
    );
  }

  /** Hotel-level: calendar-minus (NOT_ESTABLISHED). Also opportunity HOTEL_NEED_DATES_NOT_ESTABLISHED. */
  function gdiSvgCalendarMinus() {
    return (
      '<svg class="gdi-need-date-svg gdi-hotel-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M8 2v4"/>' +
      '<path d="M16 2v4"/>' +
      '<rect width="18" height="18" x="3" y="4" rx="2"/>' +
      '<path d="M3 10h18"/>' +
      '<path d="M10 16h4"/>' +
      "</svg>"
    );
  }

  /** Lucide-compatible calendar-x (NO_CURRENT_MATCH). */
  function gdiSvgCalendarX() {
    return (
      '<svg class="gdi-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M8 2v4"/>' +
      '<path d="M16 2v4"/>' +
      '<rect width="18" height="18" x="3" y="4" rx="2"/>' +
      '<path d="M3 10h18"/>' +
      '<path d="m14 14-4 4"/>' +
      '<path d="m10 14 4 4"/>' +
      "</svg>"
    );
  }

  /** Lucide-compatible calendar-question (NOT_YET_ASSESSED). */
  function gdiSvgCalendarQuestion() {
    return (
      '<svg class="gdi-need-date-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<path d="M8 2v4"/>' +
      '<path d="M16 2v4"/>' +
      '<rect width="18" height="18" x="3" y="4" rx="2"/>' +
      '<path d="M3 10h18"/>' +
      '<path d="M10 16h.01"/>' +
      '<path d="M12 13a1.5 1.5 0 1 0-1.5-1.5"/>' +
      "</svg>"
    );
  }

  /** Small information (i) icon — clickable legend control (Radar-style). */
  function gdiSvgInfoCircle() {
    return (
      '<svg class="gdi-need-date-info-svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      '<circle cx="12" cy="12" r="10"/>' +
      '<path d="M12 16v-4"/>' +
      '<path d="M12 8h.01"/>' +
      "</svg>"
    );
  }

  /**
   * Normalize hotel need-date config DTO for presentation.
   * Never returns null — missing/invalid input becomes NOT_ESTABLISHED.
   */
  function normalizeHotelNeedDateConfigV1(statusDoc, hotelId) {
    statusDoc = statusDoc && typeof statusDoc === "object" ? statusDoc : {};
    var status = String(statusDoc.status || "NOT_ESTABLISHED").toUpperCase();
    if (
      status !== "HOTEL_CONFIRMED" &&
      status !== "MARKET_ESTIMATED" &&
      status !== "NOT_ESTABLISHED"
    ) {
      status = "NOT_ESTABLISHED";
    }
    // Prefer customer-facing hotel labels (Title Case); rewrite legacy API copy.
    var accessibleLabel =
      status === "HOTEL_CONFIRMED"
        ? "Hotel Confirmed"
        : status === "MARKET_ESTIMATED"
          ? "Market Estimated"
          : "Not Established";
    return {
      version: statusDoc.version || "GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1",
      hotelId: statusDoc.hotelId || hotelId || null,
      status: status,
      statusLabel: accessibleLabel,
      accessibleLabel: accessibleLabel,
      icon:
        statusDoc.icon ||
        (status === "HOTEL_CONFIRMED"
          ? "calendar-check"
          : status === "MARKET_ESTIMATED"
            ? "calendar-dashed"
            : "calendar-minus"),
      colorClass:
        statusDoc.colorClass ||
        (status === "HOTEL_CONFIRMED"
          ? "gdi-hotel-need-date-status--confirmed"
          : status === "MARKET_ESTIMATED"
            ? "gdi-hotel-need-date-status--estimated"
            : "gdi-hotel-need-date-status--not-established"),
      confirmedPeriodCount: Number(statusDoc.confirmedPeriodCount) || 0,
      estimatedPeriodCount: Number(statusDoc.estimatedPeriodCount) || 0,
      internalOnly: false,
    };
  }

  /**
   * Hotel-level need-date configuration status — beside Need Dates heading / hotel context.
   * Not an opportunity tile icon.
   */
  function hotelNeedDateConfigStatusHtml(statusDoc) {
    statusDoc = normalizeHotelNeedDateConfigV1(statusDoc);
    var status = statusDoc.status;
    var label = statusDoc.accessibleLabel;
    var colorClass = statusDoc.colorClass;
    var svg =
      status === "HOTEL_CONFIRMED"
        ? gdiSvgCalendarCheck()
        : status === "MARKET_ESTIMATED"
          ? gdiSvgCalendarDashed()
          : gdiSvgCalendarMinus();
    return (
      '<span class="gdi-hotel-need-date-status ' +
      colorClass +
      '" data-hotel-need-date-status="' +
      esc(status) +
      '" data-hotel-need-date-icon="' +
      esc(statusDoc.icon) +
      '" role="img" aria-label="' +
      esc(label) +
      '" title="' +
      esc(label) +
      '">' +
      svg +
      '<span class="gdi-hotel-need-date-status__text">' +
      esc(
        status === "HOTEL_CONFIRMED"
          ? "Hotel Confirmed"
          : status === "MARKET_ESTIMATED"
            ? "Market Estimated"
            : "Not Established"
      ) +
      "</span>" +
      '<span class="gdi-sr-only">' +
      esc(label) +
      "</span></span>"
    );
  }

  /** Compact legend row — reuses live tile SVG helpers + color classes. */
  function needDateLegendRowHtml(iconHtml, colorClass, label) {
    return (
      '<div class="gdi-need-date-legend-row">' +
      '<span class="gdi-need-date-legend-row__icon ' +
      esc(colorClass || "") +
      '" aria-hidden="true">' +
      iconHtml +
      "</span>" +
      '<span class="gdi-need-date-legend-row__label">' +
      esc(label) +
      "</span></div>"
    );
  }

  function needDateInformationLegendHtml(opts) {
    opts = opts || {};
    var popoverId = opts.popoverId || "gdiNeedDateLegendPopover";
    var btnId = opts.buttonId || popoverId + "Btn";
    var titleId = popoverId + "Title";
    var matchRows =
      needDateLegendRowHtml(
        gdiSvgCalendarCheck(),
        "gdi-need-date-icon--matches",
        "Matches a Confirmed Need Date"
      ) +
      needDateLegendRowHtml(
        gdiSvgCalendarClock(),
        "gdi-need-date-icon--potential",
        "Potential Match"
      ) +
      needDateLegendRowHtml(
        gdiSvgCalendarQuestion(),
        "gdi-need-date-icon--not-assessed",
        "Not Yet Assessed"
      ) +
      needDateLegendRowHtml(
        gdiSvgCalendarX(),
        "gdi-need-date-icon--no-match",
        "No Current Match"
      );
    var revenueRows =
      needDateLegendRowHtml(
        '<span class="gdi-revenue-stream-icon gdi-revenue-stream-icon--active">' +
          gdiSvgBedDouble() +
          "</span>",
        "",
        "Guestrooms"
      ) +
      needDateLegendRowHtml(
        '<span class="gdi-revenue-stream-icon gdi-revenue-stream-icon--active">' +
          gdiSvgUtensils() +
          "</span>",
        "",
        "Meetings & Catering"
      );
    return (
      '<div class="gdi-need-date-legend-wrap">' +
      '<button type="button" class="gdi-need-date-info-btn" id="' +
      esc(btnId) +
      '" aria-expanded="false" aria-controls="' +
      esc(popoverId) +
      '" aria-haspopup="dialog" title="Understanding the Icons">' +
      gdiSvgInfoCircle() +
      '<span class="gdi-sr-only">Open Need-Date icon legend</span>' +
      "</button>" +
      '<div class="gdi-need-date-legend-popover" id="' +
      esc(popoverId) +
      '" role="dialog" aria-modal="false" aria-labelledby="' +
      esc(titleId) +
      '" hidden>' +
      '<div class="gdi-need-date-legend-popover__header">' +
      '<h3 id="' +
      esc(titleId) +
      '">Understanding the Icons</h3>' +
      '<button type="button" class="gdi-need-date-legend-close" aria-label="Close legend">×</button>' +
      "</div>" +
      '<div class="gdi-need-date-legend-popover__body">' +
      '<div class="gdi-need-date-legend-section">' +
      '<div class="gdi-need-date-legend-section__title">Need-Date Match</div>' +
      matchRows +
      "</div>" +
      '<div class="gdi-need-date-legend-section">' +
      '<div class="gdi-need-date-legend-section__title">Potential Revenue</div>' +
      revenueRows +
      "</div>" +
      '<p class="gdi-need-date-legend-footer">Green: Confirmed · Gold: Potential · Gray: Other</p>' +
      "</div></div></div>"
    );
  }

  function needDatesHeadingWithLegendHtml(opts) {
    opts = opts || {};
    var title = opts.title || "Need Dates";
    var statusHtml = hotelNeedDateConfigStatusHtml(opts.hotelNeedDateConfigV1 || {});
    return (
      '<div class="gdi-need-dates-heading" data-section="need-dates-heading">' +
      "<h2>" +
      esc(title) +
      "</h2>" +
      statusHtml +
      needDateInformationLegendHtml(opts) +
      "</div>"
    );
  }

  function bindNeedDateLegendInteractions(root) {
    root = root || document;
    var wraps = root.querySelectorAll(".gdi-need-date-legend-wrap");
    wraps.forEach(function (wrap) {
      var btn = wrap.querySelector(".gdi-need-date-info-btn");
      var pop = wrap.querySelector(".gdi-need-date-legend-popover");
      if (!btn || !pop || btn.__gdiLegendBound) return;
      btn.__gdiLegendBound = true;
      function close() {
        pop.hidden = true;
        btn.setAttribute("aria-expanded", "false");
        pop.classList.remove("gdi-need-date-legend-popover--align-end");
        pop.classList.remove("gdi-need-date-legend-popover--flip-up");
        pop.classList.remove("gdi-need-date-legend-popover--fixed");
        pop.style.left = "";
        pop.style.right = "";
        pop.style.top = "";
        pop.style.bottom = "";
        pop.style.width = "";
        pop.style.position = "";
      }
      function placePopover() {
        pop.classList.remove("gdi-need-date-legend-popover--align-end");
        pop.classList.remove("gdi-need-date-legend-popover--flip-up");
        pop.classList.remove("gdi-need-date-legend-popover--fixed");
        pop.style.left = "";
        pop.style.right = "";
        pop.style.top = "";
        pop.style.bottom = "";
        pop.style.width = "";
        pop.style.position = "";
        var vv = window.visualViewport;
        var vw = Math.round(
          (vv && vv.width) || window.innerWidth || document.documentElement.clientWidth || 0
        );
        var vh = Math.round(
          (vv && vv.height) || window.innerHeight || document.documentElement.clientHeight || 0
        );
        var margin = 12;
        var maxW = Math.min(340, Math.max(200, vw - margin * 2));
        // Prefer fixed positioning so nested scroll/layout cannot clip the popover.
        pop.classList.add("gdi-need-date-legend-popover--fixed");
        pop.style.position = "fixed";
        pop.style.width = maxW + "px";
        pop.style.maxWidth = maxW + "px";
        var btnRect = btn.getBoundingClientRect();
        var left = btnRect.left;
        if (left + maxW > vw - margin) left = vw - maxW - margin;
        if (left < margin) left = margin;
        pop.style.left = Math.round(left) + "px";
        pop.style.right = "auto";
        pop.style.top = Math.round(btnRect.bottom + 8) + "px";
        pop.style.bottom = "auto";
        // Re-measure and clamp again (fonts/SVG can expand after first paint).
        var rect = pop.getBoundingClientRect();
        if (rect.right > vw - margin || rect.left < margin) {
          left = Math.min(Math.max(margin, left), Math.max(margin, vw - rect.width - margin));
          pop.style.left = Math.round(left) + "px";
          rect = pop.getBoundingClientRect();
        }
        if (rect.bottom > vh - margin) {
          var upTop = btnRect.top - rect.height - 8;
          if (upTop < margin) upTop = margin;
          pop.style.top = Math.round(upTop) + "px";
          pop.classList.add("gdi-need-date-legend-popover--flip-up");
        }
      }
      function open() {
        pop.hidden = false;
        btn.setAttribute("aria-expanded", "true");
        placePopover();
        var closer = pop.querySelector(".gdi-need-date-legend-close");
        if (closer) closer.focus();
      }
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        if (pop.hidden) open();
        else close();
      });
      btn.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          btn.click();
        }
      });
      pop.addEventListener("click", function (e) {
        if (e.target && e.target.classList.contains("gdi-need-date-legend-close")) {
          e.preventDefault();
          close();
          btn.focus();
        }
      });
      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape" && !pop.hidden) {
          close();
          btn.focus();
        }
      });
      document.addEventListener("click", function (e) {
        if (pop.hidden) return;
        if (wrap.contains(e.target)) return;
        close();
      });
    });
  }

  /**
   * Map any underlying / legacy indicator payload to V3 four-status customer display.
   * Safe default: NOT_YET_ASSESSED (calendar-question). Never purple.
   */
  function mapToCustomerNeedDateDisplayV3(rawState, hotelStatus) {
    var s = String(rawState || "").toUpperCase();
    var hotel = String(hotelStatus || "NOT_ESTABLISHED").toUpperCase();
    if (s === "MATCHES_NEED_DATE" || s === "VERIFIED_DATE_MATCH") {
      if (hotel === "MARKET_ESTIMATED") {
        return {
          state: "POTENTIAL_MATCH",
          icon: "calendar-clock",
          colorClass: "gdi-need-date-icon--potential",
          displayLabel: "Potential Match",
        };
      }
      return {
        state: "MATCHES_NEED_DATE",
        icon: "calendar-check",
        colorClass: "gdi-need-date-icon--matches",
        displayLabel: "Matches a Confirmed Need Date",
      };
    }
    if (s === "POTENTIAL_MATCH" || s === "POTENTIALLY_FLEXIBLE") {
      return {
        state: "POTENTIAL_MATCH",
        icon: "calendar-clock",
        colorClass: "gdi-need-date-icon--potential",
        displayLabel: "Potential Match",
      };
    }
    if (s === "NO_CURRENT_MATCH" || s === "NO_DATE_MATCH") {
      return {
        state: "NO_CURRENT_MATCH",
        icon: "calendar-x",
        colorClass: "gdi-need-date-icon--no-match",
        displayLabel: "No Current Match",
      };
    }
    // MARKET_NEED_DATES_ESTIMATED without thesis, research, not established, unknown
    return {
      state: "NOT_YET_ASSESSED",
      icon: "calendar-question",
      colorClass: "gdi-need-date-icon--not-assessed",
      displayLabel: "Not Yet Assessed",
    };
  }

  /**
   * Universal opportunity Need-Date indicator (V3 customer presentation).
   * Every rendered opportunity resolves exactly one of four calendar SVGs.
   */
  function resolveOpportunityNeedDateIndicatorForUi(it, linked) {
    linked = linked || {};
    it = it || {};
    var ind = linked.needDateIndicator || it.needDateIndicator || null;
    var preview = linked.tileNeedDatePreview || it.tileNeedDatePreview || null;
    var hotelCfg =
      linked.hotelNeedDateConfigV1 ||
      it.hotelNeedDateConfigV1 ||
      (typeof window !== "undefined" && window.__GDI_HOTEL_NEED_DATE_CONFIG__) ||
      null;
    var hotelStatus = String((hotelCfg && hotelCfg.status) || "NOT_ESTABLISHED");
    var state = "";
    var icon = "";
    var colorClass = "";
    var tip = "";
    var aria = "";
    var displayLabel = "";
    var tipTitle = "";
    var tipBody = "";

    var v3States = {
      MATCHES_NEED_DATE: 1,
      POTENTIAL_MATCH: 1,
      NOT_YET_ASSESSED: 1,
      NO_CURRENT_MATCH: 1,
    };

    if (ind && (ind.state || ind.indicatorState) && ind.icon && v3States[String(ind.state || ind.indicatorState)]) {
      state = String(ind.state || ind.indicatorState || "");
      icon = String(ind.icon || "");
      colorClass = String(ind.colorClass || "");
      tip = String(ind.tooltipText || "");
      tipTitle = String(ind.tooltipTitle || ind.displayLabel || "");
      tipBody = String(ind.tooltipExplanation || "");
      displayLabel = String(ind.displayLabel || ind.accessibleLabel || "");
      aria = String(ind.accessibleLabel || displayLabel || tip || "");
    } else {
      var raw =
        (ind && (ind.underlyingState || ind.state || ind.indicatorState)) ||
        (preview && (preview.underlyingState || preview.indicatorState || preview.state)) ||
        "";
      var mapped = mapToCustomerNeedDateDisplayV3(raw, hotelStatus);
      // If market-estimated underlying without thesis → not assessed (already default).
      if (
        String(raw).toUpperCase() === "MARKET_NEED_DATES_ESTIMATED" &&
        ind &&
        (ind.potentialMatchBasis ||
          /flexible|tentative|market_estimate/i.test(String(ind.reason || "")))
      ) {
        mapped = mapToCustomerNeedDateDisplayV3("POTENTIAL_MATCH", hotelStatus);
      }
      state = mapped.state;
      icon = mapped.icon;
      colorClass = mapped.colorClass;
      displayLabel = mapped.displayLabel;
      aria = mapped.displayLabel;
      if (state === "MATCHES_NEED_DATE") {
        tipTitle = "Matches a Confirmed Need Date";
        tipBody =
          "This opportunity overlaps with a hotel-confirmed need period.";
      } else if (state === "POTENTIAL_MATCH") {
        tipTitle = "Potential Match";
        tipBody = "The timing could work, but confirmation is needed.";
      } else if (state === "NO_CURRENT_MATCH") {
        tipTitle = "No Current Match";
        tipBody =
          "The known opportunity dates do not overlap with the hotel's confirmed need periods. This does not mean the opportunity is unsuitable.";
      } else {
        tipTitle = "Not Yet Assessed";
        tipBody =
          hotelStatus.toUpperCase() === "NOT_ESTABLISHED"
            ? "The hotel has not provided need dates. This opportunity may still be worth pursuing."
            : "Opportunity timing requires further research.";
      }
      tip = tipTitle + ". " + tipBody;
    }

    if (!colorClass) {
      colorClass =
        state === "MATCHES_NEED_DATE"
          ? "gdi-need-date-icon--matches"
          : state === "POTENTIAL_MATCH"
            ? "gdi-need-date-icon--potential"
            : state === "NO_CURRENT_MATCH"
              ? "gdi-need-date-icon--no-match"
              : "gdi-need-date-icon--not-assessed";
    }
    if (!tip) tip = aria || displayLabel || "Need-date intelligence";
    if (!aria) aria = displayLabel || tipTitle || tip;
    if (!displayLabel) displayLabel = aria;

    return {
      state: state,
      icon: icon,
      colorClass: colorClass,
      tooltipText: tip,
      tooltipTitle: tipTitle || displayLabel,
      tooltipExplanation: tipBody || tip,
      accessibleLabel: aria,
      displayLabel: displayLabel,
    };
  }

  function needDateIntelligenceIconHtml(it, linked) {
    var resolved = resolveOpportunityNeedDateIndicatorForUi(it, linked);
    var state = resolved.state;
    var icon = resolved.icon;
    var colorClass = resolved.colorClass;
    var tip = resolved.tooltipText;
    var aria = resolved.accessibleLabel;
    var svg =
      icon === "calendar-check"
        ? gdiSvgCalendarCheck()
        : icon === "calendar-clock"
          ? gdiSvgCalendarClock()
          : icon === "calendar-question"
            ? gdiSvgCalendarQuestion()
            : icon === "calendar-x"
              ? gdiSvgCalendarX()
              : gdiSvgCalendarQuestion();
    return (
      '<span class="gdi-need-date-icon ' +
      colorClass +
      '" data-need-date-state="' +
      esc(state) +
      '" data-need-date-icon="' +
      esc(icon) +
      '" data-need-date-display-label="' +
      esc(resolved.displayLabel || aria) +
      '" title="' +
      esc(tip) +
      '" role="img" tabindex="0" aria-label="' +
      esc(aria) +
      '">' +
      svg +
      '<span class="gdi-sr-only">' +
      esc(aria) +
      ". " +
      esc(resolved.tooltipExplanation || "") +
      "</span>" +
      "</span>"
    );
  }

  /** Conditional Hotel Need-Date Match drawer block (does not replace ten sections). */
  function needDateMatchDrawerSectionHtml(o) {
    o = o || {};
    var block = o.needDateMatchV1 || null;
    var preview = o.tileNeedDatePreview || null;
    if (!block && !(preview && preview.visible)) return "";
    var title =
      (block && block.title) ||
      (preview && preview.drawerTitle) ||
      "Hotel Need-Date Match";
    var months =
      (block && (block.needPeriodLabel || block.needPeriodRange)) ||
      (preview &&
        preview.displayMonthLabels &&
        preview.displayMonthLabels.join(", ")) ||
      "";
    var why =
      (block && block.whyThisMatches) ||
      (preview && preview.recommendedSalesApproach) ||
      "";
    var action =
      (block && block.suggestedSalesAction) ||
      (preview && preview.qualificationQuestion) ||
      "";
    var flex =
      (block && block.flexibilityClassification) ||
      (preview && preview.flexibilityClassification) ||
      "";
    var access =
      (block && block.commercialAccessibilityQualification) ||
      (preview && preview.commercialAccessibilityQualification) ||
      "";
    var timing =
      (block && (block.timingOverlapLabel || block.timingOverlapStatus)) ||
      (preview && preview.dateEvidenceStatus) ||
      "";
    var state =
      (block && block.tileIndicatorState) ||
      (preview && preview.indicatorState) ||
      "";
    if (!why && !months) return "";
    var oppTiming =
      (o.eventDateDisplay ||
        o.datePillDisplayLabel ||
        o.tileEventStartDate ||
        o.eventStartDate ||
        "") +
      (o.eventEndDate && o.eventStartDate && o.eventEndDate !== o.eventStartDate
        ? " – " + o.eventEndDate
        : "");
    var bits = "";
    if (months) {
      bits +=
        '<p><strong>Hotel need period(s):</strong> ' + esc(months) + "</p>";
    }
    if (oppTiming) {
      bits +=
        '<p><strong>Opportunity timing:</strong> ' + esc(oppTiming) + "</p>";
    }
    if (state || timing) {
      bits +=
        '<p><strong>Date-match classification:</strong> ' +
        esc(state || timing) +
        (timing && state ? " · " + esc(timing) : "") +
        "</p>";
    }
    if (flex) {
      bits +=
        '<p><strong>Flexibility status:</strong> ' +
        esc(flex) +
        (preview && preview.flexibilityMayPresentAsConfirmed
          ? " (buyer-confirmed)"
          : " — not buyer-confirmed unless evidenced") +
        "</p>";
    }
    if (access) {
      bits +=
        '<p><strong>Commercial accessibility:</strong> ' +
        esc(access) +
        "</p>";
    }
    if (why) bits += "<p>" + esc(why) + "</p>";
    if (state === "POTENTIALLY_FLEXIBLE") {
      bits +=
        "<p>Potential timing flexibility identified. Confirm event or travel dates with the organizer.</p>";
    }
    if (state === "TIMING_RESEARCH_REQUIRED") {
      bits +=
        "<p>Opportunity timing has not been confirmed. Further qualification is required before treating this as a Need-Date match.</p>";
    }
    if (action) {
      bits +=
        '<p><strong>What sales should verify next:</strong> ' +
        esc(action) +
        "</p>";
    }
    bits +=
      '<p class="gdi-muted">Date match does not prove hotel sourcing is open, rooms are available, a rate was offered, or business was secured.</p>';
    return (
      '<section class="gdi-section gdi-section--need-date-match" data-section-id="hotel_need_date_match"><h3>' +
      esc(title) +
      "</h3>" +
      bits +
      "</section>"
    );
  }

  function normalizeRevenueStreamClassUi(raw) {
    var c = String(raw || "")
      .trim()
      .toUpperCase()
      .replace(/\s+/g, "_");
    if (
      c === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY ||
      c === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY ||
      c === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB ||
      c === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED
    ) {
      return c;
    }
    return HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  }

  function thesisPresentUi(t) {
    var s = String(t || "").replace(/\s+/g, " ").trim();
    if (!s) return false;
    if (/^(unsupported|not supported|no evidence|none|n\/a|null|unverified)\b/i.test(s)) {
      return false;
    }
    return true;
  }

  /**
   * Resolve icon states from DTO (prefer API-attached revenueStreamIcons /
   * revenueStreamClass). Fallback derives only from thesis fields — never from
   * demand segment alone.
   */
  function resolveRevenueStreamIconState(it, linked) {
    linked = linked || {};
    it = it || {};
    var iconsDto = linked.revenueStreamIcons || it.revenueStreamIcons;
    var cls = normalizeRevenueStreamClassUi(
      linked.revenueStreamClass ||
        it.revenueStreamClass ||
        linked.applicableHotelRevenueStreams ||
        it.applicableHotelRevenueStreams
    );
    var guestThesis =
      linked.guestroomThesis ||
      it.guestroomThesis ||
      (linked.hotelDemandHypotheses && linked.hotelDemandHypotheses.guestroomHypothesis) ||
      (it.hotelDemandHypotheses && it.hotelDemandHypotheses.guestroomHypothesis) ||
      linked.potentialGuestroomDemand ||
      it.potentialGuestroomDemand ||
      "";
    var eventThesis =
      linked.eventFnbThesis ||
      it.eventFnbThesis ||
      (linked.hotelDemandHypotheses && linked.hotelDemandHypotheses.meetingsHypothesis) ||
      (linked.hotelDemandHypotheses && linked.hotelDemandHypotheses.cateringHypothesis) ||
      (it.hotelDemandHypotheses && it.hotelDemandHypotheses.meetingsHypothesis) ||
      (it.hotelDemandHypotheses && it.hotelDemandHypotheses.cateringHypothesis) ||
      linked.potentialMeetingDemand ||
      it.potentialMeetingDemand ||
      linked.potentialCateringDemand ||
      it.potentialCateringDemand ||
      "";

    var guestrooms = "muted";
    var eventFnb = "muted";

    if (iconsDto && (iconsDto.guestrooms || iconsDto.eventFnb)) {
      guestrooms = iconsDto.guestrooms === "active" ? "active" : "muted";
      eventFnb = iconsDto.eventFnb === "active" ? "active" : "muted";
    } else if (cls === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY) {
      guestrooms = "active";
    } else if (cls === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY) {
      eventFnb = "active";
    } else if (cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB) {
      guestrooms = "active";
      eventFnb = "active";
    } else {
      // Unconfirmed / missing class: only light icons when theses are present
      if (thesisPresentUi(guestThesis)) guestrooms = "active";
      if (thesisPresentUi(eventThesis)) eventFnb = "active";
    }

    // Drawer consistency — do not highlight without supporting thesis/evidence text
    // when the opposite stream is the only one with prose (combined demotion).
    if (guestrooms === "active" && !thesisPresentUi(guestThesis) && eventFnb === "active") {
      guestrooms = "muted";
    }
    if (eventFnb === "active" && !thesisPresentUi(eventThesis) && guestrooms === "active") {
      eventFnb = "muted";
    }
    // Single-stream classes from API may omit thesis prose on older rows — keep
    // class-driven highlight when only one side is active.
    if (
      cls === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY &&
      guestrooms === "muted" &&
      thesisPresentUi(guestThesis)
    ) {
      guestrooms = "active";
    }
    if (
      cls === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY &&
      eventFnb === "muted" &&
      thesisPresentUi(eventThesis)
    ) {
      eventFnb = "active";
    }

    // Muted streams: label only — never customer-visible "not indicated" copy.
    var guestAria =
      guestrooms === "active"
        ? REVENUE_STREAM_ICON_LABEL.guestrooms + " — applicable"
        : REVENUE_STREAM_ICON_LABEL.guestrooms;
    var eventAria =
      eventFnb === "active"
        ? REVENUE_STREAM_ICON_LABEL.eventFnb + " — applicable"
        : REVENUE_STREAM_ICON_LABEL.eventFnb;
    return {
      revenueStreamClass: cls,
      guestrooms: guestrooms,
      eventFnb: eventFnb,
      ariaGuestrooms: guestAria,
      ariaEventFnb: eventAria,
      ariaGroup: "Revenue streams: " + guestAria + "; " + eventAria,
    };
  }

  /**
   * Shared SVG revenue-stream indicators for all customer-facing GDI tiles.
   * Informational only — not clickable.
   */
  function revenueStreamIconsHtml(it, linked) {
    var state = resolveRevenueStreamIconState(it, linked);
    function iconSpan(kind, active, label, aria) {
      return (
        '<span class="gdi-revenue-stream-icon gdi-revenue-stream-icon--' +
        kind +
        (active ? " gdi-revenue-stream-icon--active" : " gdi-revenue-stream-icon--muted") +
        '" data-revenue-stream="' +
        esc(kind) +
        '" data-state="' +
        (active ? "active" : "muted") +
        '" title="' +
        esc(label) +
        '" role="img" aria-label="' +
        esc(aria) +
        '">' +
        (kind === "guestrooms" ? gdiSvgBedDouble() : gdiSvgUtensils()) +
        '<span class="gdi-sr-only">' +
        esc(aria) +
        "</span>" +
        "</span>"
      );
    }
    return (
      '<div class="gdi-revenue-stream-icons" role="group" aria-label="' +
      esc(state.ariaGroup) +
      '" data-revenue-stream-class="' +
      esc(state.revenueStreamClass) +
      '">' +
      iconSpan(
        "guestrooms",
        state.guestrooms === "active",
        REVENUE_STREAM_ICON_LABEL.guestrooms,
        state.ariaGuestrooms
      ) +
      iconSpan(
        "eventFnb",
        state.eventFnb === "active",
        REVENUE_STREAM_ICON_LABEL.eventFnb,
        state.ariaEventFnb
      ) +
      "</div>"
    );
  }

  function opportunityTileHtml(it, linked, activeFilters) {
    linked = linked || {};
    activeFilters = activeFilters || {};
    var priority = linked.priority || it.priority || "WATCHLIST";
    var tone = priorityToneClass(priority);
    var id = it.opportunityId || it.id;
    // Prefer DTO displayTitle (canonical customer-facing title). Drawer header
    // must match the same value — never prefer a stale independent header string.
    var titleRaw =
      linked.displayTitle ||
      it.displayTitle ||
      linked.drawerHeaderTitle ||
      it.drawerHeaderTitle ||
      (linked.eventsOpportunityOverview &&
        linked.eventsOpportunityOverview.drawerHeaderTitle) ||
      (it.eventsOpportunityOverview &&
        it.eventsOpportunityOverview.drawerHeaderTitle) ||
      linked.title ||
      it.title ||
      "";
    var title =
      toProperCaseDisplayTitle(
        scrubEventDatesFromText(titleRaw, { preserveOccasionYear: true })
      ) || "—";
    var bookingStatus =
      linked.bookingWindowStatus || it.bookingWindowStatus || "";
    // Prefer org-specific sales summary over hotel inventory thesis (room-count
    // boilerplate repeated across cards). Hotel fit stays in the drawer.
    // Do not use whyNow / recommendedAction as summary fallbacks — those belong
    // in the detail drawer only (compact-tile presentation contract).
    // Read whyNow/recommendedAction only to scrub them out of tile summaries —
    // never render as compact-tile "Why now:" / "Next:" lines (drawer-only).
    var whyNowRaw = linked.whyNow || it.whyNow || "";
    var nextActRaw =
      linked.recommendedAction ||
      it.recommendedAction ||
      linked.recommendedNextStep ||
      it.recommendedNextStep ||
      "";
    function isActionOrWhyNowContamination(text) {
      var t = String(text || "").trim();
      if (!t) return false;
      if (/^(why\s*now|next|timing)\s*:/i.test(t)) return true;
      var wn = String(whyNowRaw || "").trim();
      var na = String(nextActRaw || "").trim();
      if (wn && t === wn) return true;
      if (na && t === na) return true;
      return false;
    }
    // Tile body = "What is happening" executive summary (not Opportunity overview).
    var whatRaw =
      it.tileCustomerSummary ||
      linked.tileCustomerSummary ||
      it.whatIsHappening ||
      linked.whatIsHappening ||
      it.summaryWhat ||
      linked.summaryWhat ||
      "";
    // Scrub broken research lead-ins that survived older shadow rows
    whatRaw = String(whatRaw || "")
      .replace(/^On\s*,\s*/i, "")
      .replace(/^On\s+\d{4}-\d{2}-\d{2},\s*public materials described\s+/i, "")
      // Title/label debris only — do not strip intentional "scaling corporate account" prose.
      .replace(/\bScaling Corporate Account\b/g, "corporate account")
      .replace(/\b(local activity|local growth activity)\b/gi, "commercial expansion")
      .trim();
    if (isAssessmentStatusProse(whatRaw) || isActionOrWhyNowContamination(whatRaw)) {
      whatRaw = "";
    }
    // Never fall back to Opportunity overview / hotel-demand mechanism boilerplate on the tile.
    if (
      /\bplausible hotel-demand mechanism\b/i.test(whatRaw) ||
      /\bdocumented [\w-]*related commercial activity\b/i.test(whatRaw)
    ) {
      whatRaw = "";
    }
    var thesisRaw =
      linked.hotelOpportunityThesis ||
      it.hotelOpportunityThesis ||
      linked.summaryWhyHotel ||
      it.summaryWhyHotel ||
      linked.fitExplanation ||
      it.fitExplanation ||
      "";
    if (isAssessmentStatusProse(thesisRaw) || isActionOrWhyNowContamination(thesisRaw)) {
      thesisRaw = "";
    }
    // Drop inventory-only thesis when it lacks org/event specificity
    if (
      thesisRaw &&
      /Luxury lifestyle inventory|\d+\s*keys?|private dining\s*\/\s*boardroom stock/i.test(
        thesisRaw
      ) &&
      whatRaw
    ) {
      thesisRaw = "";
    }
    var summaryRaw =
      (whatRaw && String(whatRaw).length >= 40 ? whatRaw : "") ||
      whatRaw ||
      (thesisRaw && String(thesisRaw).length >= 40 ? thesisRaw : "") ||
      thesisRaw ||
      "";
    // Scaling tiles: keep verified activity dates in the body ("opened on April 8,
    // 2026"). Association tiles still scrub month-day ranges when the date pill
    // already carries timing. Never leave "on ." holes after a date strip.
    var isScalingTile =
      linked.opportunityTypeFilterCategory === "SCALING_COMPANY" ||
      it.opportunityTypeFilterCategory === "SCALING_COMPANY" ||
      linked.demandFamily === "SCALING_COMPANY" ||
      it.demandFamily === "SCALING_COMPANY" ||
      /Corporate Account Relationship/i.test(
        String(
          linked.opportunityTypeLabel ||
            it.opportunityTypeLabel ||
            linked.segmentDisplayLabel ||
            it.segmentDisplayLabel ||
            "",
        ),
      );
    var summaryBody = isScalingTile
      ? String(summaryRaw || "")
          .replace(/\bon\s*\.\s*/gi, " ")
          .replace(/\s{2,}/g, " ")
          .trim()
      : scrubEventDatesFromText(summaryRaw, { preserveOccasionYear: true });
    var summary = trunc(summaryBody, 320);
    var sourcingLbl =
      linked.hotelSourcingStatusLabel ||
      it.hotelSourcingStatusLabel ||
      linked.venueSourcingStatusLabel ||
      it.venueSourcingStatusLabel ||
      "";
    var classMeta = tileClassificationMeta(it, linked, title);
    // Events meta already includes Hotel Sourcing — do not append again.
    var eventsMetaIncludesSourcing =
      /Hotel Sourcing Open|Hotel Sourcing Unconfirmed|Venue Confirmed|Historical Venue Only/i.test(
        classMeta
      );
    // Universal Sales Action Pill V1: Priority · Recommended Sales Action · Timing.
    // Commercial Stage stays internal. Weekly delta is meta-line only — never a fourth pill.
    var priorityKey =
      linked.priorityFilterKey ||
      it.priorityFilterKey ||
      normalizePriorityFilterKey(priority);
    var contactBits = [
      compactTileContactHtml(resolveTileContactPreviewBlock(it, linked)),
    ];
    return (
      '<article class="brand-card brand-card--gdi-opp ' +
      tone +
      '" data-open="' +
      esc(id) +
      '" role="button" tabindex="0">' +
      '<div class="brand-card__header brand-card__header--no-logo">' +
      '<div class="brand-card__info">' +
      '<div class="brand-card__name-row">' +
      '<div class="brand-card__name">' +
      esc(title) +
      "</div>" +
      revenueStreamIconsHtml(it, linked) +
      needDateIntelligenceIconHtml(it, linked) +
      "</div>" +
      '<div class="brand-card__pills gdi-tile-pills">' +
      tilePriorityPill(priorityKey, activeFilters) +
      tileSalesActionPill(it, linked) +
      tileEventDatePill(it, linked) +
      "</div>" +
      weeklyDeltaMetaLine(linked) +
      '<div class="brand-card__meta">' +
      classMeta +
      (!eventsMetaIncludesSourcing && sourcingLbl
        ? " · " + esc(formatTileSecondaryHeadlineLabel(sourcingLbl))
        : "") +
      "</div>" +
      "</div></div>" +
      (summary
        ? '<div class="brand-card__description">' + esc(summary) + "</div>"
        : "") +
      (function () {
        var disc =
          linked.modeledDemandDisclaimer || it.modeledDemandDisclaimer || "";
        if (
          !disc &&
          linked.modeledRoomsMin != null &&
          linked.modeledRoomsMax != null
        ) {
          disc =
            "Estimated " +
            linked.modeledRoomsMin +
            "\u2013" +
            linked.modeledRoomsMax +
            " rooms. Modeled demand \u2014 lodging not verified.";
        }
        if (!disc) return "";
        return (
          '<div class="brand-card__meta brand-card__meta--modeled">' +
          esc(disc) +
          "</div>"
        );
      })() +
      // Intentionally omit research "Evidence: …" labels from customer cards.
      // Evidence classification remains internal; commercial stage uses pills above.
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

  function commercialStagePresetHtml(opts) {
    opts = opts || {};
    var active = opts.active != null ? opts.active : "";
    var counts = opts.counts || {};
    var presets = [
      { value: "", label: "All", swatch: null },
      {
        value: "PURSUE_NOW",
        label: "Pursue Now",
        swatch: COMMERCIAL_STAGE_SWATCH.PURSUE_NOW,
      },
      {
        value: "FUTURE_CAPTURE",
        label: "Future Capture",
        swatch: COMMERCIAL_STAGE_SWATCH.FUTURE_CAPTURE,
      },
      {
        value: "RELATIONSHIP_DEVELOPMENT",
        label: "Relationship Development",
        swatch: COMMERCIAL_STAGE_SWATCH.RELATIONSHIP_DEVELOPMENT,
      },
      {
        value: "QUALIFICATION_NEEDED",
        label: "Qualification Needed",
        swatch: COMMERCIAL_STAGE_SWATCH.QUALIFICATION_NEEDED,
      },
    ];
    return (
      '<div class="chain-scale-legend gdi-browse-legend" role="group" aria-label="Commercial Stage filters" title="Commercial opportunity stage — distinct from Priority and from Hotel Sourcing status.">' +
      "<span>Commercial Stage</span>" +
      presets
        .map(function (p) {
          var n = counts[p.value] != null ? counts[p.value] : 0;
          var isActive = active === p.value;
          var noRecords = p.value !== "" && n === 0;
          return (
            '<button type="button" class="chain-scale-legend-item' +
            (isActive ? " active" : "") +
            (noRecords ? " no-records" : "") +
            '" data-gdi-commercial-stage="' +
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

  function salesFocusPresetHtml(opts) {
    // Sales Focus preset group removed from customer GDI UI (2026-11).
    // salesActivationCategory / Pursue Now / Sales Qualification remain on DTOs.
    return "";
  }
  function salesOpportunityPresetHtml(opts) {
    opts = opts || {};
    var active = opts.active != null ? opts.active : "";
    var counts = opts.counts || {};
    var subgroup = opts.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH;
    var presets = [
      { value: "", label: SALES_OPPORTUNITY_FILTER_LABEL[""], swatch: null },
      {
        value: "HOTEL_SOURCING_NOT_CONFIRMED",
        label: SALES_OPPORTUNITY_FILTER_LABEL.HOTEL_SOURCING_NOT_CONFIRMED,
        swatch: SALES_OPPORTUNITY_SWATCH.HOTEL_SOURCING_NOT_CONFIRMED,
      },
      {
        value: "PURSUE_NOW",
        label: SALES_OPPORTUNITY_FILTER_LABEL.PURSUE_NOW,
        swatch: SALES_OPPORTUNITY_SWATCH.PURSUE_NOW,
      },
      {
        value: "FUTURE_GROUP_BUSINESS",
        label: SALES_OPPORTUNITY_FILTER_LABEL.FUTURE_GROUP_BUSINESS,
        swatch: SALES_OPPORTUNITY_SWATCH.FUTURE_GROUP_BUSINESS,
      },
      {
        value: "COMPETITOR_CAPTURE",
        label: SALES_OPPORTUNITY_FILTER_LABEL.COMPETITOR_CAPTURE,
        swatch: SALES_OPPORTUNITY_SWATCH.COMPETITOR_CAPTURE,
      },
      {
        value: "PARTNERSHIP_OPPORTUNITIES",
        label: SALES_OPPORTUNITY_FILTER_LABEL.PARTNERSHIP_OPPORTUNITIES,
        swatch: SALES_OPPORTUNITY_SWATCH.PARTNERSHIP_OPPORTUNITIES,
      },
      {
        value: "LOCAL_CATERING",
        label: SALES_OPPORTUNITY_FILTER_LABEL.LOCAL_CATERING,
        swatch: SALES_OPPORTUNITY_SWATCH.LOCAL_CATERING,
      },
    ];
    var chips = presets
      .map(function (p) {
        var n = counts[p.value] != null ? counts[p.value] : 0;
        var isActive = active === p.value;
        var noRecords = p.value !== "" && n === 0;
        var fullName =
          SALES_OPPORTUNITY_FILTER_LABEL_FULL[p.value] || p.label;
        var tip = presetLabelWithCount(fullName, n);
        return (
          '<button type="button" class="chain-scale-legend-item' +
          (isActive ? " active" : "") +
          (noRecords ? " no-records" : "") +
          '" data-gdi-sales-opportunity="' +
          esc(p.value) +
          '" aria-pressed="' +
          isActive +
          '" title="' +
          esc(tip) +
          '" aria-label="' +
          esc(tip) +
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
      .join("");

    var showSub =
      active === SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED;
    var subHtml = "";
    if (showSub) {
      var subs = [
        {
          value: HOTEL_SOURCING_SUBGROUP.BOTH,
          label: "Both",
          n:
            (counts.sourcingOpen || 0) + (counts.sourcingUnconfirmed || 0),
        },
        {
          value: HOTEL_SOURCING_SUBGROUP.OPEN,
          label: "Confirmed Open",
          n: counts.sourcingOpen || 0,
        },
        {
          value: HOTEL_SOURCING_SUBGROUP.UNCONFIRMED,
          label: "Unconfirmed",
          n: counts.sourcingUnconfirmed || 0,
        },
      ];
      subHtml =
        '<div class="gdi-sourcing-subgroup" role="group" aria-label="Hotel sourcing subgroup">' +
        '<span class="gdi-sourcing-subgroup__label">Sourcing focus</span>' +
        subs
          .map(function (s) {
            var isActive = subgroup === s.value;
            return (
              '<button type="button" class="chain-scale-legend-item gdi-sourcing-subgroup__btn' +
              (isActive ? " active" : "") +
              '" data-gdi-sourcing-subgroup="' +
              esc(s.value) +
              '" aria-pressed="' +
              isActive +
              '"><span class="chain-scale-legend-label">' +
              esc(presetLabelWithCount(s.label, s.n)) +
              "</span></button>"
            );
          })
          .join("") +
        "</div>";
    }

    return (
      '<div class="gdi-sales-opportunity-filters">' +
      '<div class="chain-scale-legend gdi-browse-legend gdi-browse-legend--sales gdi-preset-panel" role="group" aria-label="Sales Opportunities quick filters" title="Find commercially attractive demand — Hotel Sourcing Open is not inferred from a missing venue announcement.">' +
      '<span class="gdi-preset-panel__title">Sales Opportunities</span>' +
      '<div class="gdi-preset-panel__chips">' +
      chips +
      "</div></div>" +
      subHtml +
      "</div>"
    );
  }

  /**
   * Full browse chrome: Priority + Sales Opportunities + toolbar.
   * Sales Focus preset group removed (UI only) — salesActivationCategory remains
   * on DTOs / drawers. Commercial Stage remains on opportunity DTOs / tile pills.
   * Reset is the top-level Reset View control (not duplicated here).
   * Classification: REUSABLE_PRODUCT_UI
   */
  function inventoryViewToggleHtml(opts) {
    // Dual inventory UI removed (FOUNDER): one opportunity list only.
    // Research ledger remains backend-internal — never a second inventory surface.
    return "";
  }

  function opportunityBrowseChromeHtml(opts) {
    opts = opts || {};
    return (
      '<div class="gdi-browse">' +
      (opts.weeklySummaryHtml ||
        weeklyDeltaHeaderSummaryHtml(opts.weeklyHeaderCounts || {})) +
      inventoryViewToggleHtml(opts) +
      '<div class="gdi-browse-presets">' +
      priorityPresetHtml({
        active: opts.activePriority != null ? opts.activePriority : "",
        counts: opts.priorityCounts || {},
      }) +
      salesOpportunityPresetHtml({
        active:
          opts.activeSalesOpportunity != null
            ? opts.activeSalesOpportunity
            : "",
        counts: opts.salesOpportunityCounts || {},
        sourcingSubgroup:
          opts.sourcingSubgroup || HOTEL_SOURCING_SUBGROUP.BOTH,
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
      return '<div class="gdi-contact-summary gdi-contact-summary--empty">Office operations contact not yet identified</div>';
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
      (String(
        linked.customerFacingState || it.customerFacingState || ""
      ).toUpperCase() === "FUTURE_WATCH" &&
      (linked.watchCardWhyMatters || it.watchCardWhyMatters)
        ? '<div class="gdi-brief-block"><div class="gdi-meta-label">Why this matters</div><p>' +
          esc(linked.watchCardWhyMatters || it.watchCardWhyMatters || "—") +
          '</p></div><div class="gdi-brief-block"><div class="gdi-meta-label">Current status</div><p>' +
          esc(linked.watchCardCurrentStatus || it.watchCardCurrentStatus || "—") +
          '</p></div><div class="gdi-brief-block"><div class="gdi-meta-label">Who controls lodging</div><p>' +
          esc(
            linked.watchCardLodgingController ||
              it.watchCardLodgingController ||
              "—"
          ) +
          '</p></div><div class="gdi-brief-block"><div class="gdi-meta-label">Hotel-selection status</div><p>' +
          esc(
            linked.watchCardHotelSelectionStatus ||
              it.watchCardHotelSelectionStatus ||
              "—"
          ) +
          '</p></div><div class="gdi-brief-block"><div class="gdi-meta-label">When to act</div><p>' +
          esc(
            linked.watchCardWhenToAct || it.watchCardWhenToAct || it.whyNow || "—"
          ) +
          '</p></div><div class="gdi-brief-block"><div class="gdi-meta-label">Next trigger</div><p>' +
          esc(
            linked.watchCardNextTrigger ||
              it.watchCardNextTrigger ||
              linked.nextTriggerCondition ||
              "—"
          ) +
          "</p></div>" +
          (linked.watchCardMonitoringStatus || it.watchCardMonitoringStatus
            ? '<div class="gdi-brief-block"><div class="gdi-meta-label">Publication monitoring</div><p>' +
              esc(
                linked.watchCardMonitoringStatus ||
                  it.watchCardMonitoringStatus ||
                  "—"
              ) +
              (linked.watchCardMonitoringLastChecked ||
              it.watchCardMonitoringLastChecked
                ? "<br><span class=\"gdi-meta-quiet\">Last checked: " +
                  esc(
                    linked.watchCardMonitoringLastChecked ||
                      it.watchCardMonitoringLastChecked
                  ) +
                  "</span>"
                : "") +
              (linked.watchCardMonitoringNextExpected ||
              it.watchCardMonitoringNextExpected
                ? "<br><span class=\"gdi-meta-quiet\">" +
                  esc(
                    linked.watchCardMonitoringNextExpected ||
                      it.watchCardMonitoringNextExpected
                  ) +
                  "</span>"
                : "") +
              "</p></div>"
            : "") +
          '<div class="gdi-brief-block gdi-brief-block--action"><div class="gdi-meta-label">Recommended next step</div><p>' +
          esc(
            linked.watchCardRecommendedNextStep ||
              it.watchCardRecommendedNextStep ||
              it.recommendedNextStep ||
              "—"
          ) +
          "</p></div>"
        : '<div class="gdi-brief-block"><div class="gdi-meta-label">Why Now</div><p>' +
          esc(it.whyNow || "—") +
          '</p></div><div class="gdi-brief-block gdi-brief-block--action"><div class="gdi-meta-label">Recommended Action</div><p>' +
          esc(it.recommendedNextStep || "—") +
          "</p></div>") +
      "</div>" +
      '<div class="gdi-brief-card__footer">' +
      contactSummaryHtml(contact) +
      pursuitActionButtonHtml(it, linked) +
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
    var raw = String(progression.compactStatusLabel);
    // Do not show raw FUTURE_CYCLE_* codes on customer tiles.
    var label = customerStatusLabel(
      raw.replace(/\s+/g, "_").toUpperCase().replace(/_+/g, "_"),
      raw
    );
    if (/^FUTURE\s*CYCLE$/i.test(raw.trim())) {
      label = "Future cycle";
    }
    return (
      '<span class="gdi-badge gdi-badge--progression">' +
      esc(label) +
      "</span>"
    );
  }

  function secondaryHasUsableRoute(s) {
    if (!s || !s.name) return false;
    if (s.email) return true;
    if (s.phone) return true;
    if (s.organizationRoutingAlternative) return true;
    // Stage D: named CONTACT_RELEVANT secondary with role evidence / source is displayable
    // without fabricating email/phone (on-demand enrichment remains separate).
    if (s.whyThisContact || s.commercialRelevance || s.sourceUrl || s.profileUrl) {
      return true;
    }
    if (s.contactRelevanceLabel === "CONTACT_RELEVANT") return true;
    return false;
  }

  function secondaryNamedContactHtml(o) {
    var s = o && o.secondaryContact;
    if (!secondaryHasUsableRoute(s)) return "";
    var classLabel =
      s.contactClassificationLabel ||
      (s.commercialClass === "hotel_sourcing_decision_maker"
        ? "Hotel-sourcing decision-maker"
        : s.commercialClass === "operational_buyer_or_coordinator"
          ? "Operational buyer / coordinator"
          : s.commercialClass === "commercial_influencer_or_referral"
            ? "Commercial influencer / referral"
            : s.contactClassification === "VERIFIED_HOTEL_BUYER"
              ? "Verified hotel / meeting buyer"
              : s.contactClassification === "POTENTIAL_BUYER"
                ? "Potential buyer"
                : s.contactClassification === "INFLUENCER"
                  ? "Influencer"
                  : s.contactClassification === "LOCAL_OPERATIONS"
                    ? "Local operations / workplace contact"
                    : "Referral contact");
    var authorityNote =
      s.contractingAuthorityStatus === "CONTRACTING_AUTHORITY_VERIFIED"
        ? "Contracting authority verified"
        : s.contactRelevanceLabel === "CONTACT_RELEVANT"
          ? "Commercially relevant contact — contracting authority not verified"
          : "";
    var routeLabel =
      s.organizationRoutingLabel ||
      (s.organizationRoutingAlternative &&
      String(s.organizationRoutingAlternative).indexOf("@") >= 0
        ? "Company email — ask for " + s.name
        : "Company office — ask for " + s.name);
    var profileUrl = s.profileUrl || s.sourceUrl || null;
    var profile =
      profileUrl && isSafeHttpUrl(profileUrl)
        ? '<p><a href="' +
          esc(profileUrl) +
          '" target="_blank" rel="noopener noreferrer">Supporting profile / source</a></p>'
        : "";
    var evidence =
      Array.isArray(s.evidenceUrls) && s.evidenceUrls.length
        ? '<p class="gdi-muted"><a href="' +
          esc(s.evidenceUrls[0]) +
          '" target="_blank" rel="noopener noreferrer">Source</a></p>'
        : "";
    var why =
      s.whyThisContact ||
      s.commercialRelevance ||
      s.relationshipToOpportunity ||
      "";
    return (
      '<div class="gdi-secondary-contact">' +
      "<h4>Secondary contact</h4>" +
      "<p><strong>" +
      esc(s.name) +
      "</strong>" +
      (s.title || s.role ? " · " + esc(s.title || s.role) : "") +
      (s.organization ? " · " + esc(s.organization) : "") +
      "</p>" +
      "<p><strong>Why relevant:</strong> " +
      esc(classLabel) +
      (why ? " — " + esc(why) : "") +
      "</p>" +
      (authorityNote
        ? '<p class="gdi-muted">' + esc(authorityNote) + "</p>"
        : "") +
      (s.email
        ? "<p><strong>Direct email (verified):</strong> <a href=\"mailto:" +
          esc(s.email) +
          '">' +
          esc(s.email) +
          "</a></p>"
        : "") +
      (s.phone
        ? "<p><strong>Direct phone (verified):</strong> <a href=\"tel:" +
          esc(String(s.phone).replace(/[^\d+]/g, "")) +
          '">' +
          esc(s.phone) +
          "</a></p>"
        : "") +
      (s.organizationRoutingAlternative
        ? "<p><strong>" +
          esc(routeLabel) +
          ":</strong> " +
          (String(s.organizationRoutingAlternative).indexOf("@") >= 0
            ? '<a href="mailto:' +
              esc(s.organizationRoutingAlternative) +
              '">' +
              esc(s.organizationRoutingAlternative) +
              "</a>"
            : '<a href="tel:' +
              esc(String(s.organizationRoutingAlternative).replace(/[^\d+]/g, "")) +
              '">' +
              esc(s.organizationRoutingAlternative) +
              "</a>") +
          "</p>"
        : "") +
      (s.companyPhone &&
      (!s.organizationRoutingAlternative ||
        String(s.organizationRoutingAlternative).indexOf("@") >= 0)
        ? "<p><strong>Company office — ask for " +
          esc(s.name) +
          ":</strong> <a href=\"tel:" +
          esc(String(s.companyPhone).replace(/[^\d+]/g, "")) +
          '">' +
          esc(s.companyPhone) +
          "</a></p>"
        : "") +
      profile +
      evidence +
      "</div>"
    );
  }

  function looksLikePersonNameLocal(name) {
    var n = String(name || "").replace(/\s+/g, " ").trim();
    if (!n || n === "UNKNOWN" || n === "—") return false;
    if (
      /\b(association|conference|university|inc\.?|llc|corp\.?|group|desk|services|housing|registration|committee|department|team|staff|office)\b/i.test(
        n
      )
    ) {
      return false;
    }
    // Prefer Unicode letters (A'yesha Gaetán). ASCII-only regex falsely
    // rejected accented surnames and blanked verified named contacts.
    try {
      var particle =
        "(?:de|van|von|der|den|la|le|du|da|di|del|della|st\\.?)";
      var titleCase = new RegExp(
        "^[\\p{Lu}][\\p{L}'.-]+(?:\\s+(?:" +
          particle +
          "|[\\p{Lu}][\\p{L}'.-]+)){1,4}$",
        "u"
      );
      var allCaps =
        /^[\p{Lu}][\p{Lu}'.-]+(?:\s+[\p{Lu}][\p{Lu}'.-]+){1,3}$/u;
      if (titleCase.test(n) || allCaps.test(n)) return true;
    } catch (_e) {
      /* older engines without \p{} */
    }
    var ascii = n.normalize
      ? n.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
      : n;
    return /^[A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){1,3}$/.test(ascii);
  }

  function isPollutedPhoneDisplay(raw) {
    var t = String(raw || "").trim();
    if (!t) return true;
    if (/^(?:19|20)\d{2}[-/.]\d{1,2}(?:[-/.]\d{1,2})?$/.test(t)) return true;
    if (t.replace(/\D/g, "").length < 8) return true;
    return false;
  }

  function surfeCtaLabel(channel) {
    if (channel === "email") return "Find email";
    if (channel === "phone") return "Find phone";
    return "Find email & phone";
  }

  /** Strip internal eligibility enums from customer-facing Why relevant copy. */
  function scrubCustomerContactWhyProse(raw) {
    var s = String(raw || "").replace(/\s+/g, " ").trim();
    if (!s) return "";
    s = s
      .replace(/\bCONTACT_RELEVANT\b\.?\s*/gi, "")
      .replace(/\bINELIGIBLE_UNSPECIFIED\b\.?\s*/gi, "")
      .replace(/\bCONTRACTING_AUTHORITY_VERIFIED\b/gi, "contracting authority")
      .replace(/\bREFERRAL\b(?=\s*\.|$)/g, "referral route")
      .replace(/\s{2,}/g, " ")
      .replace(/^[\s.;:—-]+/, "")
      .trim();
    return s;
  }

  function twoContactSlotHtml(slot, opts) {
    if (!slot) return "";
    var oppId =
      (opts && opts.opportunityId) ||
      "";
    var slotCompanyPhone =
      slot.companyPhone && !isPollutedPhoneDisplay(slot.companyPhone)
        ? slot.companyPhone
        : "";
    var slotPersonPhone =
      slot.phone && !isPollutedPhoneDisplay(slot.phone) ? slot.phone : "";
    if (!slot.customerFacingNamed) {
      var hasOrgRoute = !!(slot.organizationEmail || slotCompanyPhone);
      // Secondary research-gap with no org route and no name: one quiet line only
      // when primary already shows the company path — never an empty named card.
      if (
        opts &&
        opts.omitEmptySecondaryGap &&
        slot.slot === "SECONDARY" &&
        !hasOrgRoute
      ) {
        return (
          '<p class="gdi-contact-quality-quiet">' +
          esc(slot.researchGapCopy || "Buyer Not Yet Identified.") +
          "</p>"
        );
      }
      return (
        '<div class="gdi-contact-slot gdi-contact-slot--gap">' +
        "<h4>" +
        esc(slot.displayLabel || "Contact") +
        "</h4>" +
        '<p class="gdi-contact-quality-quiet">' +
        esc(slot.researchGapCopy || "Buyer Not Yet Identified.") +
        "</p>" +
        (slot.organizationEmail
          ? "<p><strong>" +
            esc(slot.organizationEmailLabel || "Organization email") +
            ":</strong> " +
            esc(slot.organizationEmail) +
            "</p>"
          : "") +
        (slotCompanyPhone
          ? "<p><strong>" +
            esc(slot.companyPhoneLabel || "Organization phone") +
            ":</strong> " +
            esc(slotCompanyPhone) +
            "</p>"
          : "") +
        (slot.officialContactUrl && isSafeHttpUrl(slot.officialContactUrl)
          ? '<p><a href="' +
            esc(slot.officialContactUrl) +
            '" target="_blank" rel="noopener noreferrer">Official contact / program page</a></p>'
          : "") +
        "</div>"
      );
    }
    var linkedIn =
      (slot.linkedInUrl && isSafeHttpUrl(slot.linkedInUrl) && slot.linkedInUrl) ||
      (slot.profileUrl &&
      isSafeHttpUrl(slot.profileUrl) &&
      /linkedin\.com/i.test(slot.profileUrl)
        ? slot.profileUrl
        : "");
    var otherProfile =
      !linkedIn && slot.profileUrl && isSafeHttpUrl(slot.profileUrl)
        ? slot.profileUrl
        : "";
    var pathLabelRaw = String(slot.commercialContactPathLabel || "").trim();
    var pathLabelUseful =
      pathLabelRaw &&
      !/not yet established|research incomplete|no contact/i.test(pathLabelRaw);
    var pathLabel = pathLabelUseful
      ? '<p class="gdi-contact-kind-label"><strong>Contact path:</strong> ' +
        esc(pathLabelRaw) +
        "</p>"
      : slot.customerFacingNamed && !slot.email && !slot.phone
        ? '<p class="gdi-contact-kind-label"><strong>Contact path:</strong> Named person — profile / referral route</p>'
        : "";
    var surfeBtn = "";
    if (slot.surfeEnrichmentEligible && slot.surfeEnrichmentChannel && slot.name) {
      surfeBtn =
        '<div class="gdi-contact-reveal" data-gdi-contact-reveal-root="1">' +
        '<p class="gdi-contact-cta"><button type="button" class="gdi-btn gdi-btn--secondary" data-gdi-contact-cta="1" data-gdi-contact-channel="' +
        esc(slot.surfeEnrichmentChannel) +
        '" data-contact-name="' +
        esc(slot.name) +
        '" data-opportunity-id="' +
        esc(oppId) +
        '">' +
        esc(surfeCtaLabel(slot.surfeEnrichmentChannel)) +
        "</button></p>" +
        '<p class="gdi-muted gdi-contact-cta-note">Ready to retrieve. Results stay in this session only and are not saved to Dealality records.</p>' +
        '<p class="gdi-contact-reveal-status" data-gdi-contact-reveal-status hidden></p>' +
        "</div>";
    }
    return (
      '<div class="gdi-contact-slot" data-contact-slot="' +
      esc(slot.slot || "") +
      '">' +
      "<h4>" +
      esc(slot.displayLabel || "Contact") +
      "</h4>" +
      pathLabel +
      "<p><strong>Suggested contact:</strong> " +
      esc(slot.name) +
      "</p>" +
      (slot.title
        ? "<p><strong>Role:</strong> " + esc(slot.title) + "</p>"
        : "") +
      (slot.organization
        ? "<p><strong>Organization:</strong> " + esc(slot.organization) + "</p>"
        : "") +
      (slot.whyContact
        ? "<p><strong>Why relevant:</strong> " +
          esc(scrubCustomerContactWhyProse(slot.whyContact)) +
          "</p>"
        : "") +
      (slot.email
        ? "<p><strong>" +
          esc(slot.emailLabel || "Direct work email") +
          ":</strong> " +
          esc(slot.email) +
          "</p>"
        : "") +
      (slot.organizationEmail
        ? "<p><strong>" +
          esc(slot.organizationEmailLabel || "Organization email") +
          ":</strong> " +
          esc(slot.organizationEmail) +
          "</p>"
        : "") +
      (slotPersonPhone
        ? "<p><strong>" +
          esc(slot.phoneLabel || "Direct phone") +
          ":</strong> " +
          esc(slotPersonPhone) +
          "</p>"
        : "") +
      (slotCompanyPhone
        ? "<p><strong>" +
          esc(slot.companyPhoneLabel || "Organization phone") +
          ":</strong> " +
          esc(slotCompanyPhone) +
          "</p>"
        : "") +
      (slot.officialContactUrl && isSafeHttpUrl(slot.officialContactUrl)
        ? '<p><strong>Official contact / program page:</strong> <a href="' +
          esc(slot.officialContactUrl) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(slot.officialContactUrl) +
          "</a></p>"
        : "") +
      (linkedIn
        ? '<p><strong>LinkedIn:</strong> <a href="' +
          esc(linkedIn) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(linkedIn) +
          "</a></p>"
        : otherProfile
          ? '<p><a href="' +
            esc(otherProfile) +
            '" target="_blank" rel="noopener noreferrer">Professional profile</a></p>'
          : "") +
      surfeBtn +
      "</div>"
    );
  }

  function scalingTwoContactPresentationHtml(o) {
    var p = o && o.scalingTwoContactPresentation;
    if (!p || !p.primary) return "";
    var primaryNamed = !!p.primary.customerFacingNamed;
    var oppId = o.opportunityId || o.id || "";
    var routeLabel = String(
      p.primaryRouteTypeLabel ||
        p.primary.commercialContactPathLabel ||
        o.primaryRouteTypeLabel ||
        o.commercialContactPathLabel ||
        "",
    ).trim();
    var routeHtml =
      routeLabel &&
      !/not yet established|research incomplete|^NO_CONTACT$|^[A-Z0-9_]+$/i.test(
        routeLabel,
      )
        ? '<p class="gdi-muted gdi-primary-route-type"><strong>Primary route type:</strong> ' +
          esc(routeLabel) +
          "</p>"
        : "";
    return (
      '<div class="gdi-two-contact-standard" data-universal-who-to-contact="v2">' +
      routeHtml +
      twoContactSlotHtml(p.primary, { opportunityId: oppId }) +
      twoContactSlotHtml(p.secondary, {
        omitEmptySecondaryGap: primaryNamed,
        opportunityId: oppId,
      }) +
      "</div>"
    );
  }

  function whoShouldSalesContactHtml(o, whoFn) {
    // Unified Who to Contact: Scaling gold-standard two-contact slots win for
    // every segment (Events + Scaling). Segment-only whoFn is fallback only.
    var twoContactHtml = scalingTwoContactPresentationHtml(o);
    if (twoContactHtml) return twoContactHtml;
    // Never re-enter this function via whoFn — share-app historically passed
    // whoShouldSalesContactHtml as whoContactHtml, which double-appended the
    // secondary contact (Adrienne ×2) and skipped the named-primary path.
    if (
      typeof whoFn === "function" &&
      whoFn !== whoShouldSalesContactHtml
    ) {
      return whoFn(o) + secondaryNamedContactHtml(o);
    }

    var c = o.primaryContact;
    // Org-only primaryContact shells (phone/role without a person name) must not
    // render as named "Referral contact: —" / "Person phone".
    if (c && !looksLikePersonNameLocal(c.name || o.contactName)) {
      c = null;
    }
    var drawer = o.customerContactDrawer || null;
    function withSecondary(html) {
      return html + secondaryNamedContactHtml(o);
    }
    var rawPath = o.commercialContactPath || null;
    // Never display raw internal enums (NAMED_DIRECT, ORGANIZATION_PATH, …) as the contact name.
    var pathLabel =
      (drawer && drawer.functionalPath) ||
      o.commercialContactPathLabel ||
      (rawPath === "NAMED_DIRECT"
        ? "Named person — direct business email"
        : rawPath === "NAMED_PHONE"
          ? "Named person — phone route"
          : rawPath === "ORGANIZATION_PATH"
            ? "Company contact path"
            : rawPath === "FUNCTIONAL_CONTACT"
              ? "Functional / department email"
              : rawPath === "NO_CONTACT"
                ? "Contact route not yet established"
                : rawPath && !/^[A-Z0-9_]+$/.test(String(rawPath))
                  ? rawPath
                  : null);
    var tier = o.contactTier || (c && c.contactTier) || null;
    var tierLabel =
      o.contactTierLabel ||
      (drawer && drawer.tierLabel) ||
      (tier === "NAMED_DIRECT"
        ? "Named decision-maker with direct reachability"
        : tier === "NAMED_PARTIAL"
          ? "Named decision-maker — reachability incomplete"
          : tier === "FUNCTIONAL_CONTACT"
            ? "Functional events / meetings / housing contact"
            : tier === "ORGANIZATION_PATH"
              ? "Organization or official contact path"
              : tier === "GENERIC_ONLY"
                ? "Generic inbox only"
                : null);
    var briefRoutes =
      (o.salesBrief && o.salesBrief.whoToContact && o.salesBrief.whoToContact.routes) ||
      (o.commercialIntelligenceBrief &&
        o.commercialIntelligenceBrief.whoToContact &&
        o.commercialIntelligenceBrief.whoToContact.routes) ||
      null;
    if (!o.organizationContactEmail && briefRoutes && briefRoutes.email) {
      o.organizationContactEmail = briefRoutes.email;
    }
    if (!o.contactUrl && briefRoutes && briefRoutes.url) {
      o.contactUrl = briefRoutes.url;
    }
    var officialUrl =
      (drawer && drawer.source) ||
      o.contactOfficialUrl ||
      (c && (c.officialContactUrl || c.sourceUrl)) ||
      o.officialSource ||
      (briefRoutes && briefRoutes.url) ||
      null;
    var officialLink =
      officialUrl && isSafeHttpUrl(officialUrl)
        ? '<p><a href="' +
          esc(officialUrl) +
          '" target="_blank" rel="noopener noreferrer">Official contact / program page</a></p>'
        : "";
    var getDetailsCta =
      c &&
      c.name &&
      !c.email &&
      !c.phone &&
      (tier === "NAMED_PARTIAL" || tier === "NAMED_DIRECT" || !tier)
        ? '<p class="gdi-contact-cta"><button type="button" class="gdi-btn gdi-btn--secondary" data-gdi-contact-cta="1" data-gdi-contact-channel="both" data-opportunity-id="' +
          esc(o.opportunityId || o.id || "") +
          '">Get Contact Details</button></p><p class="gdi-contact-cta gdi-contact-cta--channels"><button type="button" class="gdi-btn gdi-btn--secondary" data-gdi-contact-cta="1" data-gdi-contact-channel="email" data-opportunity-id="' +
          esc(o.opportunityId || o.id || "") +
          '">Retrieve Email</button> <button type="button" class="gdi-btn gdi-btn--secondary" data-gdi-contact-cta="1" data-gdi-contact-channel="phone" data-opportunity-id="' +
          esc(o.opportunityId || o.id || "") +
          '">Retrieve Phone</button></p>'
        : "";
    var ceilingCopy =
      (drawer && drawer.publicCeilingCopy) ||
      (tier === "NO_CONTACT" || tier === "GENERIC_ONLY" || (!c && !pathLabel)
        ? "Named event contact not publicly identified yet"
        : null);
    var whyRelevant =
      (drawer && drawer.whyRelevant) ||
      o.whoPrimaryReason ||
      (c && c.whyThisContact) ||
      o.whyThisContact ||
      null;
    var actionHint =
      (drawer && drawer.recommendedAction) || o.recommendedContactAction || null;

    if (
      !c &&
      (pathLabel ||
        o.commercialContactPath ||
        tier === "FUNCTIONAL_CONTACT" ||
        tier === "ORGANIZATION_PATH" ||
        officialUrl)
    ) {
      return withSecondary(
        (tierLabel
          ? '<p class="gdi-contact-kind-label"><strong>' + esc(tierLabel) + "</strong></p>"
          : "") +
          "<p><strong>Primary Contact:</strong> " +
          esc(pathLabel || "Organization / official program contact path") +
          "</p>" +
          (drawer && drawer.role
            ? "<p><strong>Role:</strong> " + esc(drawer.role) + "</p>"
            : "") +
          (o.organizationName
            ? "<p><strong>Organization:</strong> " + esc(o.organizationName) + "</p>"
            : "") +
          (whyRelevant
            ? "<p><strong>Why Relevant:</strong> " + esc(whyRelevant) + "</p>"
            : "") +
          officialLink +
          '<p class="gdi-contact-quality-quiet">No named individual published — use the organization path.</p>' +
          (actionHint
            ? '<p class="gdi-contact-action">' + esc(actionHint) + "</p>"
            : "")
      );
    }

    if (!c) {
      var orgEmailFallback =
        o.organizationContactEmail ||
        o.functionalContactEmail ||
        o.contactEmail ||
        null;
      var companyPhoneFallback =
        o.companyPhone || o.contactPhone || null;
      if (
        companyPhoneFallback &&
        (/^(?:19|20)\d{2}[-/.]\d{1,2}(?:[-/.]\d{1,2})?$/.test(
          String(companyPhoneFallback).trim()
        ) ||
          String(companyPhoneFallback).replace(/\D/g, "").length < 8)
      ) {
        companyPhoneFallback = null;
      }
      var companyPhoneLbl =
        o.companyPhoneLabel || "Company / office phone";
      var askFallback =
        (o.contactChannelSeparation &&
          o.contactChannelSeparation.askToBeDirectedTo) ||
        "Ask for the person managing staff travel or internal meetings.";
      return withSecondary(
        '<p class="gdi-contact-quality-quiet">' +
          esc(
            ceilingCopy ||
              "Office operations contact not yet identified"
          ) +
          "</p>" +
          (orgEmailFallback
            ? "<p><strong>Organization email:</strong> " +
              esc(orgEmailFallback) +
              "</p>"
            : "") +
          (companyPhoneFallback
            ? "<p><strong>" +
              esc(companyPhoneLbl) +
              ":</strong> " +
              esc(companyPhoneFallback) +
              "</p>"
            : "") +
          (pathLabel
            ? "<p><strong>Functional Path:</strong> " + esc(pathLabel) + "</p>"
            : "") +
          officialLink +
          '<p class="gdi-contact-action">' +
          esc(actionHint || askFallback) +
          "</p>"
      );
    }

    var isReferralDrawer = isReferralContactRole(o, o, c.role || c.title || "");
    var primaryKind = isReferralDrawer
      ? "Referral contact"
      : c.contactKind === "FUNCTIONAL" || c.functionalEntity || tier === "FUNCTIONAL_CONTACT"
        ? "Functional / organization contact"
        : tierLabel || "Who sales should contact";
    var channelSep = o.contactChannelSeparation || null;
    var orgRoute =
      (channelSep && channelSep.organizationRoute) ||
      o.functionalContactEmail ||
      o.organizationContactEmail ||
      c.organizationContactRoute ||
      null;
    if (orgRoute && String(orgRoute).indexOf("@") < 0) orgRoute = null;
    var orgRouteIsNotPersonEmail =
      !c.email ||
      !orgRoute ||
      String(c.email).toLowerCase() !== String(orgRoute).toLowerCase();
    var companyPhoneDrawer =
      o.companyPhone ||
      (channelSep && channelSep.companyPhone) ||
      null;
    if (
      companyPhoneDrawer &&
      (/^(?:19|20)\d{2}[-/.]\d{1,2}(?:[-/.]\d{1,2})?$/.test(
        String(companyPhoneDrawer).trim()
      ) ||
        String(companyPhoneDrawer).replace(/\D/g, "").length < 8)
    ) {
      companyPhoneDrawer = null;
    }
    if (
      companyPhoneDrawer &&
      c.phone &&
      String(companyPhoneDrawer).replace(/\D/g, "") ===
        String(c.phone).replace(/\D/g, "")
    ) {
      companyPhoneDrawer = null;
    }
    var companyPhoneLbl =
      o.companyPhoneLabel || "Company / office phone";
    var suggestedLabel = isReferralDrawer
      ? "Referral contact: " +
        (c.name || "—") +
        (c.role || c.title ? " (" + (c.role || c.title) + ")" : "")
      : c.suggestedContactLabel ||
        (c.name
          ? "Suggested contact: " +
            c.name +
            (c.role || c.title ? " (" + (c.role || c.title) + ")" : "")
          : null);
    var askDirected =
      (channelSep && channelSep.askToBeDirectedTo) ||
      c.askToBeDirectedTo ||
      (c.name && orgRoute
        ? "Ask to be directed to " +
          (c.role || c.title || "the relevant function") +
          " (" +
          c.name +
          ")"
        : null);
    var seenChannels = {};
    function markChannel(v) {
      var key = String(v || "")
        .toLowerCase()
        .replace(/\s+/g, "");
      if (!key || seenChannels[key]) return false;
      seenChannels[key] = true;
      return true;
    }
    var personEmailIsOrg =
      c.email &&
      (isGenericOrgEmailLocal(c.email) ||
        c.emailOwnership === "organization_or_department" ||
        (orgRoute &&
          String(c.email).toLowerCase() === String(orgRoute).toLowerCase() &&
          isGenericOrgEmailLocal(c.email)));
    var personEmailHtml =
      c.email && !personEmailIsOrg && markChannel(c.email)
        ? "<p><strong>Person email:</strong> " + esc(c.email) + "</p>"
        : "";
    var orgEmailFromPerson =
      c.email && personEmailIsOrg && markChannel(c.email)
        ? "<p><strong>Organization email:</strong> " + esc(c.email) + "</p>"
        : "";
    var orgEmailHtml =
      orgEmailFromPerson ||
      (orgRoute && orgRouteIsNotPersonEmail && markChannel(orgRoute)
        ? "<p><strong>Organization email:</strong> " + esc(orgRoute) + "</p>"
        : !c.email && orgRoute && markChannel(orgRoute)
          ? "<p><strong>Organization email:</strong> " + esc(orgRoute) + "</p>"
          : "");
    var personPhoneHtml =
      c.phone && markChannel(c.phone)
        ? "<p><strong>Person phone:</strong> " +
          esc(c.phone) +
          (o.primaryContactPhoneTypeLabel || c.phoneTypeLabel || c.phoneType
            ? " (" +
              esc(
                o.primaryContactPhoneTypeLabel ||
                  c.phoneTypeLabel ||
                  c.phoneType
              ) +
              ")"
            : "") +
          "</p>"
        : "";
    var companyPhoneHtml =
      companyPhoneDrawer && markChannel(companyPhoneDrawer)
        ? "<p><strong>" +
          esc(companyPhoneLbl) +
          ":</strong> " +
          esc(companyPhoneDrawer) +
          "</p>"
        : "";
    var pathHtml =
      pathLabel && pathLabel !== orgRoute && markChannel(pathLabel)
        ? "<p><strong>Contact path:</strong> " + esc(pathLabel) + "</p>"
        : "";
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
          (b.email ? " · " + esc(b.email) : "") +
          (b.phone ? " · " + esc(b.phone) : "") +
          "</li>"
        );
      })
      .join("");
    return withSecondary(
      '<p class="gdi-contact-kind-label"><strong>' +
        esc(primaryKind) +
        "</strong></p>" +
        (suggestedLabel
          ? "<p><strong>" + esc(suggestedLabel) + "</strong></p>"
          : "<p><strong>Primary Contact:</strong> " + esc(c.name || "—") + "</p>") +
        "<p><strong>Role:</strong> " +
        esc((drawer && drawer.role) || c.role || c.title || "—") +
        "</p>" +
        "<p><strong>Organization:</strong> " +
        esc(c.organization || o.organizationName || "—") +
        "</p>" +
        personEmailHtml +
        orgEmailHtml +
        personPhoneHtml +
        companyPhoneHtml +
        (askDirected
          ? '<p class="gdi-contact-action">' + esc(askDirected) + "</p>"
          : "") +
        pathHtml +
        getDetailsCta +
        officialLink +
        "<p><strong>Why Relevant:</strong> " +
        esc(whyRelevant || "—") +
        "</p>" +
        (actionHint
          ? '<p class="gdi-contact-action">' + esc(actionHint) + "</p>"
          : "") +
        (o.contactGradeLabel || c.contactGradeLabel || (drawer && drawer.grade)
          ? '<p class="gdi-contact-quality-quiet">Contact quality: ' +
            esc(
              o.contactGradeLabel ||
                c.contactGradeLabel ||
                "Grade " + ((drawer && drawer.grade) || "")
            ) +
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

  function sourceListHtml(sources) {
    var list = (sources || [])
      .map(function (s) {
        var label = s.name || s.title || s.url || "";
        if (!label) return "";
        var link =
          s.url && isSafeHttpUrl(s.url)
            ? '<a href="' +
              esc(s.url) +
              '" target="_blank" rel="noopener noreferrer">' +
              esc(label) +
              "</a>"
            : esc(label);
        var support = s.supportsFact || s.whatItSupports || "";
        var meta = [
          s.sourceType ? esc(s.sourceType) : "",
          s.date ? esc(s.date) : "",
        ]
          .filter(Boolean)
          .join(" · ");
        return (
          "<li>" +
          link +
          (meta ? " · " + meta : "") +
          (support ? " — " + esc(support) : "") +
          "</li>"
        );
      })
      .filter(Boolean)
      .join("");
    return list || "<li>No source links available.</li>";
  }

  function peKnownListHtml(items, emptyMsg) {
    var rows = (items || [])
      .map(function (x) {
        return (
          "<li><strong>" +
          esc(x.field) +
          ":</strong> " +
          esc(x.value) +
          (x.status ? " <em>(" + esc(x.status) + ")</em>" : "") +
          "</li>"
        );
      })
      .join("");
    return rows || "<li>" + esc(emptyMsg || "None") + "</li>";
  }

  function venuePartnershipDetailHtml(o, audit, extras) {
    extras = extras || {};
    audit = audit || {};
    var ps = o.partnershipStatusDisplay || {};
    var address = [o.venueAddress, o.venueCity, o.venueRegion]
      .filter(Boolean)
      .join(", ");
    var fitRows = [
      ["Catchment", o.lodgingCatchmentFitLabel || o.lodgingCatchmentFit],
      ["Product fit", o.productFitLabel || o.productFit],
      ["Partnership potential", o.partnershipPotentialLabel || o.partnershipPotential],
      ["Lodging capture", o.lodgingCapturePotentialLabel || o.lodgingCapturePotential],
      [
        "Distance",
        o.distanceMiles != null
          ? o.distanceMiles +
            " mi" +
            (o.driveTimeMinutes != null ? " · ~" + o.driveTimeMinutes + " min" : "")
          : null,
      ],
      ["Venue lodging", o.onSiteLodgingStatusLabel || o.venueLodgingStatus || o.onSiteLodgingStatus],
    ]
      .filter(function (r) {
        return r[1];
      })
      .map(function (r) {
        return drawerMetaFieldRowHtml(r[0], r[1]);
      })
      .join("");

    return (
      '<div class="gdi-detail-framework gdi-detail-framework--venue-partnership">' +
      commercialProgressionHtml(o.commercialProgression) +
      '<section class="gdi-section"><h3>Opportunity Summary</h3><p>' +
      esc(o.summaryWhat || o.title || "—") +
      "</p>" +
      '<div class="gdi-detail-meta-row">' +
      "<span><strong>Type</strong> " +
      esc(
        o.opportunityTypeLabel ||
          customerStatusLabel(o.opportunityType, "Venue Partnership")
      ) +
      "</span>" +
      "<span><strong>Demand family</strong> " +
      esc(
        o.demandFamilyLabel ||
          customerStatusLabel(o.demandFamily, "Private Events")
      ) +
      "</span>" +
      "<span><strong>Timing</strong> " +
      esc(o.eventDateDisplay || "Ongoing partnership opportunity") +
      "</span>" +
      "<span><strong>Evidence</strong> " +
      esc(
        o.evidenceConfidence != null && o.evidenceConfidence !== ""
          ? (Number(o.evidenceConfidence) <= 1 && Number(o.evidenceConfidence) > 0
              ? Math.round(Number(o.evidenceConfidence) * 100)
              : Math.round(Number(o.evidenceConfidence))) + " / 100"
          : "—"
      ) +
      "</span></div></section>" +
      '<section class="gdi-section"><h3>Venue</h3>' +
      "<p><strong>" +
      esc(o.venueName || o.organizationName || "—") +
      "</strong>" +
      (o.venueType ? " · " + esc(o.venueType) : "") +
      "</p>" +
      (address ? "<p>" + esc(address) + "</p>" : "") +
      "<ul>" +
      (o.website || o.venueWebsite || o.organizationWebsite || o.contactUrl
        ? drawerMetaFieldRowHtml(
            "Website",
            o.website ||
              o.venueWebsite ||
              o.organizationWebsite ||
              o.contactUrl
          )
        : "") +
      (o.distanceMiles != null
        ? "<li><strong>Distance:</strong> " + esc(o.distanceMiles) + " mi from hotel</li>"
        : "") +
      (o.driveTimeMinutes != null
        ? "<li><strong>Drive time:</strong> ~" + esc(o.driveTimeMinutes) + " min</li>"
        : "") +
      (o.lodgingCatchmentFitLabel || o.lodgingCatchmentFit
        ? "<li><strong>Catchment:</strong> " +
          esc(o.lodgingCatchmentFitLabel || o.lodgingCatchmentFit) +
          "</li>"
        : "") +
      (o.venueCapacity != null || o.maxCapacity != null
        ? "<li><strong>Capacity:</strong> " +
          esc(o.venueCapacity ?? o.maxCapacity) +
          "</li>"
        : "") +
      (o.onSiteLodgingStatusLabel || o.onSiteLodgingStatus
        ? "<li><strong>On-site lodging:</strong> " +
          esc(o.onSiteLodgingStatusLabel || o.onSiteLodgingStatus) +
          "</li>"
        : "") +
      (o.eventActivityEvidenceStatusLabel || o.eventActivityEvidenceStatus
        ? "<li><strong>Event activity:</strong> " +
          esc(
            o.eventActivityEvidenceStatusLabel || o.eventActivityEvidenceStatus
          ) +
          "</li>"
        : "") +
      "</ul></section>" +
      '<section class="gdi-section"><h3>Why This Matters</h3><p>' +
      esc(
        o.summaryWhat ||
          o.summaryWhyMatters ||
          o.whyNow ||
          "—"
      ) +
      (o.evidenceStatusLabel
        ? '</p><p class="gdi-meta-value"><strong>Evidence status:</strong> ' +
          esc(o.evidenceStatusLabel) +
          (o.demandEvidenceLabel ? " (" + esc(o.demandEvidenceLabel) + ")" : "")
        : "") +
      "</p></section>" +
      '<section class="gdi-section"><h3>Recommended sales action</h3><p>' +
      esc(o.recommendedAction || o.recommendedNextStep || "—") +
      "</p></section>" +
      '<section class="gdi-section gdi-section--matters"><h3>Why this hotel</h3>' +
      hotelFitAssessmentSectionHtml(o, audit) +
      (fitRows
        ? '<ul class="gdi-pe-fit-legacy-meta">' + fitRows + "</ul>"
        : "") +
      "</section>" +
      '<section class="gdi-section"><h3>Partnership Status</h3><ul>' +
      "<li><strong>Partner status:</strong> " +
      esc(
        ps.partnerStatusLabel ||
          o.partnerStatusLabel ||
          o.partnerStatus ||
          "—"
      ) +
      "</li>" +
      "<li><strong>Partnership potential:</strong> " +
      esc(
        ps.partnershipPotentialLabel ||
          o.partnershipPotentialLabel ||
          o.partnershipPotential ||
          "—"
      ) +
      "</li>" +
      "<li><strong>Venue lodging:</strong> " +
      esc(
        ps.venueLodgingStatusLabel ||
          o.onSiteLodgingStatusLabel ||
          o.onSiteLodgingStatus ||
          "—"
      ) +
      "</li></ul></section>" +
      '<section class="gdi-section"><h3>Who Sales Should Contact</h3>' +
      whoShouldSalesContactHtml(o, extras.whoContactHtml) +
      "</section>" +
      '<section class="gdi-section"><h3>Evidence / Sources</h3>' +
      "<p>" +
      esc(o.evidenceConfidenceExplanation || audit.evidenceConfidenceExplanation || "") +
      "</p>" +
      "<ul>" +
      sourceListHtml(o.sources) +
      "</ul>" +
      "<h4>Verified</h4><ul>" +
      peKnownListHtml(
        o.knownVsEstimated && o.knownVsEstimated.verified,
        "No verified fields listed."
      ) +
      "</ul><h4>Estimated</h4><ul>" +
      peKnownListHtml(
        o.knownVsEstimated && o.knownVsEstimated.estimated,
        "No estimated fields."
      ) +
      "</ul><h4>Inferred</h4><ul>" +
      peKnownListHtml(
        o.knownVsEstimated && o.knownVsEstimated.inferred,
        "No inferred fields."
      ) +
      "</ul></section>" +
      '<section class="gdi-section"><h3>Recommended Action</h3><p>' +
      esc(o.recommendedAction || "—") +
      "</p></section>" +
      (extras.validationHtml
        ? '<section class="gdi-section gdi-section--validation gdi-validation-panel"><h3>Hotel Validation / Action / Outcome</h3>' +
          extras.validationHtml +
          "</section>"
        : "") +
      "</div>"
    );
  }

  function specificPrivateEventDetailHtml(o, audit, extras) {
    extras = extras || {};
    audit = audit || {};
    var dateLabel =
      o.eventDateDisplay ||
      eventDateRangeLabel(o.eventStartDate, o.eventEndDate, "Date not yet confirmed");
    return (
      '<div class="gdi-detail-framework gdi-detail-framework--specific-pe">' +
      commercialProgressionHtml(o.commercialProgression) +
      '<section class="gdi-section"><h3>Event</h3><p>' +
      esc(o.summaryWhat || o.title || "—") +
      "</p>" +
      '<div class="gdi-detail-meta-row">' +
      "<span><strong>Date</strong> " +
      esc(dateLabel) +
      "</span>" +
      "<span><strong>Type</strong> " +
      esc(
        o.opportunityTypeLabel ||
          customerStatusLabel(o.opportunityType, "Specific Private Event")
      ) +
      "</span>" +
      "<span><strong>Demand family</strong> " +
      esc(
        o.demandFamilyLabel ||
          customerStatusLabel(o.demandFamily, "Private Events")
      ) +
      "</span></div></section>" +
      '<section class="gdi-section"><h3>Venue</h3><p><strong>' +
      esc(o.venueName || o.organizationName || "—") +
      "</strong></p><p>" +
      esc(o.eventLocationSummary || [o.venueAddress, o.venueCity].filter(Boolean).join(", ") || "—") +
      "</p></section>" +
      '<section class="gdi-section"><h3>Demand</h3><p>Attendance: ' +
      esc(fmtVal(o.attendance ?? o.estimatedAttendance)) +
      " · Peak rooms: " +
      esc(fmtVal(o.peakRooms ?? o.estimatedPeakRooms)) +
      "</p><p>" +
      esc(
        (isAssessmentStatusProse(o.summaryWhyMatters)
          ? ""
          : o.summaryWhyMatters) ||
          (isAssessmentStatusProse(o.hotelOpportunityThesis)
            ? ""
            : o.hotelOpportunityThesis) ||
          o.summaryWhat ||
          "—"
      ) +
      "</p></section>" +
      '<section class="gdi-section"><h3>Who Sales Should Contact</h3>' +
      whoShouldSalesContactHtml(o, extras.whoContactHtml) +
      "</section>" +
      '<section class="gdi-section"><h3>Evidence / Sources</h3><p>' +
      esc(o.evidenceConfidenceExplanation || "") +
      "</p><ul>" +
      sourceListHtml(o.sources) +
      "</ul></section>" +
      '<section class="gdi-section"><h3>Recommended Action</h3><p>' +
      esc(o.recommendedAction || "—") +
      "</p></section>" +
      (extras.validationHtml
        ? '<section class="gdi-section gdi-section--validation gdi-validation-panel"><h3>Hotel Validation / Action / Outcome</h3>' +
          extras.validationHtml +
          "</section>"
        : "") +
      "</div>"
    );
  }

  /**
   * Scaling Companies — ONE canonical 10-section corporate drawer.
   * Renders o.scalingDrawerModel when present (API sanitize); otherwise builds a
   * minimal same-ID structure so layout never degrades by content presence.
   * Forbidden top-level: Commercial context / Why Dealality selected / Company background.
   */
  function corporateAccountRelationshipDetailHtml(o, audit, extras) {
    extras = extras || {};
    audit = audit || {};
    o = o || {};

    function prose(v) {
      return String(v || "")
        .replace(/\s+/g, " ")
        .trim();
    }
    function cleanList(items) {
      if (!Array.isArray(items)) return [];
      return items
        .map(prose)
        .filter(Boolean)
        .filter(function (t) {
          return !/^—+$/.test(t) && t !== "-" && t !== "•";
        });
    }
    function listHtml(items) {
      if (!items || !items.length) return "";
      return (
        "<ul>" +
        items
          .map(function (t) {
            return "<li>" + esc(t) + "</li>";
          })
          .join("") +
        "</ul>"
      );
    }
    function sourceListFromModel(sources, evidenceGroups, sourcesFull) {
      if ((!sources || !sources.length) && (!sourcesFull || !sourcesFull.length)) {
        return '<p class="gdi-muted">No source URLs attached yet.</p>';
      }
      function renderOne(s) {
        var url = prose(s && s.url);
        if (!url) return "";
        var title = prose(s && s.title) || url;
        var publisher = prose(s && s.publisher);
        var date = prose(s && (s.publicationDate || s.publishedDate));
        var establishes = prose(s && s.establishes);
        var bits =
          "<li class=\"gdi-evidence-item\">" +
          "<div class=\"gdi-evidence-title\"><strong>" +
          esc(title) +
          "</strong></div>";
        if (publisher) {
          bits +=
            "<div class=\"gdi-muted\">Publisher: " + esc(publisher) + "</div>";
        }
        if (date) {
          bits += "<div class=\"gdi-muted\">Date: " + esc(date) + "</div>";
        }
        if (establishes && establishes.length <= 120) {
          bits +=
            '<div class="gdi-muted">' + esc(establishes) + "</div>";
        }
        bits +=
          "<div><a href=\"" +
          esc(url) +
          "\" target=\"_blank\" rel=\"noopener noreferrer\">" +
          esc(url) +
          "</a></div></li>";
        return bits;
      }
      var defaultHtml = "";
      if (evidenceGroups && evidenceGroups.length) {
        defaultHtml = evidenceGroups
          .map(function (g) {
            var rows = (g.sources || []).map(renderOne).filter(Boolean);
            if (!rows.length) return "";
            return (
              "<div class=\"gdi-evidence-group\">" +
              "<p><strong>" +
              esc(prose(g.label) || "Evidence") +
              "</strong></p><ul>" +
              rows.join("") +
              "</ul></div>"
            );
          })
          .filter(Boolean)
          .join("");
      } else if (sources && sources.length) {
        defaultHtml =
          "<ul class=\"gdi-evidence-default\">" +
          sources.map(renderOne).filter(Boolean).join("") +
          "</ul>";
      }
      var full = Array.isArray(sourcesFull) ? sourcesFull : [];
      var defaultCount = (sources && sources.length) || 0;
      if (full.length > defaultCount && full.length > 0) {
        defaultHtml +=
          '<details class="gdi-evidence-expand">' +
          "<summary>View all supporting evidence (" +
          full.length +
          ")</summary>" +
          '<ul class="gdi-evidence-full">' +
          full.map(renderOne).filter(Boolean).join("") +
          "</ul></details>";
      }
      return defaultHtml || '<p class="gdi-muted">No source URLs attached yet.</p>';
    }

    var model = o.scalingDrawerModel || null;
    if (!model || !model.sections) {
      // Client-side structural fallback — same 10 IDs; content from available fields only.
      var brief =
        o.commercialIntelligenceBrief || o.salesBrief || {};
      var what =
        prose(o.tileCustomerSummary) ||
        prose(o.whatIsHappening) ||
        prose(o.summaryWhat) ||
        prose(brief.companyAndLocalActivity) ||
        "This company is tracked as a scaling corporate account; public expansion signals still need qualification before treating lodging demand as confirmed.";
      var pot =
        prose(brief.potentialBusinessForThisHotel) ||
        prose(o.potentialHotelBusiness) ||
        prose(o.summaryWhyMatters) ||
        "Hotel demand is not yet verified from public research.";
      var whyH =
        prose(o.whyThisHotel) ||
        prose(brief.whyThisHotel) ||
        "Property-specific fit is not yet established beyond geography.";
      var whyNow =
        prose(o.whyEstablishContactNow || o.whyNow || brief.whyActNow) ||
        "No time-critical trigger is established. Monitor until a travel/meetings owner is confirmed.";
      var next =
        prose(o.recommendedAction) ||
        "Identify who owns corporate travel and visiting-staff lodging for this account; introduce the hotel and confirm whether a preferred-hotel pattern exists.";
      if (
        /association staff\s*vs\s*PCO|host\/overflow|meetings\s*\/\s*congress\s*\/\s*operations route/i.test(
          next
        )
      ) {
        next =
          "Identify who owns corporate travel and visiting-staff lodging for this account; introduce the hotel and confirm whether a preferred-hotel pattern exists.";
      }
      model = {
        sectionIds: [
          "opportunity_overview",
          "what_is_happening",
          "potential_business",
          "why_this_hotel",
          "why_act_now",
          "facts_and_uncertainties",
          "who_to_contact",
          "recommended_next_step",
          "supporting_evidence",
          "hotel_validation",
        ],
        organizationName: prose(o.organizationName || o.company) || "Organization",
        opportunityTypeLabel:
          prose(o.opportunityTypeLabel) || "Corporate Account Relationship",
        sections: {
          opportunity_overview: {
            organizationName: prose(o.organizationName || o.company) || "Organization",
            typeLabel:
              prose(o.opportunityTypeLabel) || "Corporate Account Relationship",
            demandStage:
              prose(o.demandStageLabel) ||
              "Potential business opportunity; hotel needs not yet confirmed",
            timing: prose(o.eventDateDisplay || o.timing) || "Timing to be confirmed",
          },
          what_is_happening: { body: what, activityDate: null },
          potential_business: { body: pot, inferred: [] },
          why_this_hotel: (function () {
            var advF = cleanList(brief.whyThisHotelAdvantages);
            var limF = cleanList(brief.whyThisHotelLimitations);
            var bodyF = whyH;
            if (advF.length || limF.length) {
              var packF = advF.concat(limF).join(" ").toLowerCase();
              if (
                bodyF &&
                packF.indexOf(
                  bodyF.toLowerCase().slice(0, Math.min(80, bodyF.length))
                ) !== -1
              ) {
                bodyF = "";
              }
            }
            return { body: bodyF, advantages: advF, limitations: limF };
          })(),
          why_act_now: { body: whyNow },
          facts_and_uncertainties: {
            facts: cleanList(
              o.establishedFacts ||
                brief.establishedFacts ||
                (brief.establishedFactsAndUncertainties &&
                  brief.establishedFactsAndUncertainties.facts)
            ),
            uncertainties: cleanList(
              (brief.establishedFactsAndUncertainties &&
                brief.establishedFactsAndUncertainties.uncertainties) ||
                (brief.evidenceAndUncertainty &&
                  brief.evidenceAndUncertainty.uncertainties) ||
                o.uncertainties ||
                o.constraints
            ),
            factsEmptyNote:
              "No separately listed established facts beyond the activity narrative above.",
            uncEmptyNote:
              "Material unknowns remain (travel volume, budgets, buying authority) unless confirmed with the account.",
          },
          who_to_contact: {
            contactPathLabel: prose(o.commercialContactPathLabel) || null,
          },
          recommended_next_step: {
            body: next,
            qualificationQuestions: cleanList(
              brief.qualificationQuestions ||
                (brief.recommendedSalesApproach &&
                  brief.recommendedSalesApproach.qualificationQuestions) ||
                o.qualificationQuestions ||
                o.suggestedQualificationQuestions
            ),
          },
          supporting_evidence: {
            sources: (o.sources || [])
              .map(function (src) {
                return {
                  url: prose(src && (src.url || src)),
                  title: prose(src && src.title) || null,
                };
              })
              .filter(function (x) {
                return x.url;
              }),
            activityDate: null,
            researchVerificationDate: null,
          },
          hotel_validation: {},
        },
      };
    }

    var sec = model.sections;
    var ov = sec.opportunity_overview || {};
    var whatS = sec.what_is_happening || {};
    var potS = sec.potential_business || {};
    var whyS = sec.why_this_hotel || {};
    var nowS = sec.why_act_now || {};
    var factS = sec.facts_and_uncertainties || {};
    var whoS = sec.who_to_contact || {};
    var nextS = sec.recommended_next_step || {};
    var evS = sec.supporting_evidence || {};

    var contactNameSafe = o.contactName;
    if (
      contactNameSafe &&
      /^(Riviera Maya|Meetings Content Managers|Mexico City|Cancun|Cancún)$/i.test(
        String(contactNameSafe).trim()
      )
    ) {
      contactNameSafe = null;
    }
    var primaryContactSafe = o.primaryContact || null;
    if (
      primaryContactSafe &&
      primaryContactSafe.name &&
      /^(Riviera Maya|Meetings Content Managers|Mexico City|Cancun|Cancún)$/i.test(
        String(primaryContactSafe.name).trim()
      )
    ) {
      primaryContactSafe = Object.assign({}, primaryContactSafe, { name: null });
    }
    if (
      primaryContactSafe &&
      !looksLikePersonNameLocal(
        primaryContactSafe.name || contactNameSafe || o.contactName
      )
    ) {
      primaryContactSafe = Object.assign({}, primaryContactSafe, {
        name: null,
        phone: null,
      });
    }

    var factsHtml = "";
    var factsList = cleanList(factS.facts);
    var uncList = cleanList(factS.uncertainties);
    if (factsList.length) {
      factsHtml += "<p><strong>Established facts</strong></p>" + listHtml(factsList);
    } else if (factS.factsEmptyNote) {
      factsHtml += "<p class=\"gdi-muted\">" + esc(factS.factsEmptyNote) + "</p>";
    }
    if (uncList.length) {
      factsHtml += "<p><strong>Uncertainties</strong></p>" + listHtml(uncList);
    } else if (factS.uncEmptyNote) {
      factsHtml += "<p class=\"gdi-muted\">" + esc(factS.uncEmptyNote) + "</p>";
    }

    var whyBody = prose(whyS.body);
    var adv = cleanList(whyS.advantages);
    var lim = cleanList(whyS.limitations);
    // Prefer structured lists; never echo the same sentences as body + lists.
    if (adv.length || lim.length) {
      var packWhy = adv.concat(lim).join(" ").toLowerCase();
      if (
        whyBody &&
        packWhy.indexOf(
          whyBody.toLowerCase().slice(0, Math.min(80, whyBody.length))
        ) !== -1
      ) {
        whyBody = "";
      }
      if (whyBody) {
        adv = adv.filter(function (a) {
          return whyBody.toLowerCase().indexOf(a.toLowerCase().slice(0, 48)) === -1;
        });
        lim = lim.filter(function (a) {
          return whyBody.toLowerCase().indexOf(a.toLowerCase().slice(0, 48)) === -1;
        });
      }
    }
    var whyBits = whyBody ? "<p>" + esc(whyBody) + "</p>" : "";
    if (adv.length) {
      whyBits += "<p><strong>Advantages</strong></p>" + listHtml(adv);
    }
    if (lim.length) {
      whyBits += "<p><strong>Limitations / trade-offs</strong></p>" + listHtml(lim);
    }
    if (!whyBits) {
      whyBits = "<p>—</p>";
    }

    var qHtml = "";
    var qs = cleanList(nextS.qualificationQuestions);
    if (qs.length) {
      qHtml =
        "<p><strong>Qualification questions</strong></p>" + listHtml(qs);
    }

    function sharedSegmentOverviewHtml(opp) {
      var sov =
        (opp && opp.segmentOverviewModel) ||
        (opp &&
          opp.sharedTenSectionDrawer &&
          opp.sharedTenSectionDrawer.overviewModel) ||
        null;
      if (!sov || sov.structuredGrid !== true) return "";
      var fields = Array.isArray(sov.fields) ? sov.fields : [];
      var values = sov.values || sov;
      function row(label, value) {
        return (
          '<div class="gdi-events-overview-item"><span class="gdi-events-overview-label">' +
          esc(label) +
          '</span><span class="gdi-events-overview-value">' +
          esc(value || "Not yet verified") +
          "</span></div>"
        );
      }
      var grid = fields
        .map(function (f) {
          var key = f.key || f;
          var label = f.label || key;
          var val = (values && values[key]) || sov[key] || "Not yet verified";
          return row(label, val);
        })
        .join("");
      return (
        '<div class="gdi-events-opportunity-overview" data-overview-version="' +
        esc(sov.overviewMode || "segment-overview") +
        '">' +
        '<p class="gdi-events-overview-identity">' +
        esc(sov.identityLine || opp.organizationName || "—") +
        "</p>" +
        '<div class="gdi-events-overview-grid">' +
        grid +
        "</div></div>"
      );
    }
    var structuredOverviewHtml = sharedSegmentOverviewHtml(o);
    var overviewSectionInner = structuredOverviewHtml
      ? structuredOverviewHtml
      : "<p><strong>" +
        esc(
          o.displayCompanyName ||
            ov.organizationName ||
            model.organizationName ||
            "Organization"
        ) +
        "</strong>" +
        (function () {
          var typeLbl =
            ov.typeLabel ||
            model.opportunityTypeLabel ||
            o.opportunityTypeLabel ||
            model.segmentDisplayLabel ||
            o.segmentDisplayLabel ||
            "Corporate Account Relationship";
          if (/^scaling company$/i.test(String(typeLbl))) {
            typeLbl = "Corporate Account Relationship";
          }
          return " · " + esc(typeLbl);
        })() +
        "</p>" +
        (ov.body || model.overviewBody
          ? "<p>" + esc(prose(ov.body || model.overviewBody)) + "</p>"
          : "") +
        '<div class="gdi-detail-meta-row">' +
        "<span><strong>Demand stage</strong> " +
        esc(ov.demandStage || "—") +
        "</span>" +
        "<span><strong>Timing</strong> " +
        esc(ov.timing || "Timing to be confirmed") +
        "</span></div>";

    return (
      '<div class="gdi-detail-framework gdi-detail-framework--corporate-account gdi-detail-framework--scaling-canonical">' +
      commercialProgressionHtml(o.commercialProgression) +
      '<section class="gdi-section" data-section-id="opportunity_overview"><h3>Opportunity overview</h3>' +
      overviewSectionInner +
      "</section>" +
      needDateMatchDrawerSectionHtml(o) +
      (function () {
        if (
          !(
            o.venueIntelligence &&
            (o.preferredLodgingPartnershipV1 === true ||
              o.demandOpportunitySubtype === "PREFERRED_LODGING_PARTNERSHIP" ||
              o.opportunityType === "PREFERRED_LODGING_PARTNERSHIP")
          )
        ) {
          return "";
        }
        var viRows =
          Array.isArray(o.venueIntelligenceFieldRows) &&
          o.venueIntelligenceFieldRows.length
            ? o.venueIntelligenceFieldRows
            : [];
        var viLis = viRows
          .map(function (r) {
            return drawerMetaFieldRowHtml(r[0], r[1]);
          })
          .join("");
        return (
          '<section class="gdi-section" data-section-id="venue_intelligence"><h3>Venue Intelligence</h3>' +
          (viLis
            ? '<ul class="gdi-meta-list">' + viLis + "</ul>"
            : '<p class="gdi-muted">Venue intelligence fields pending verification.</p>') +
          "</section>"
        );
      })() +
      (function () {
        if (
          !(
            o.plannerIntelligence &&
            (o.professionalEventPlannerPartnershipV1 === true ||
              o.demandOpportunitySubtype ===
                "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
              o.opportunityType === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP")
          )
        ) {
          return "";
        }
        var piRows =
          Array.isArray(o.plannerIntelligenceFieldRows) &&
          o.plannerIntelligenceFieldRows.length
            ? o.plannerIntelligenceFieldRows
            : [];
        var piLis = piRows
          .map(function (r) {
            return drawerMetaFieldRowHtml(r[0], r[1]);
          })
          .join("");
        return (
          '<section class="gdi-section" data-section-id="planner_intelligence"><h3>Planner &amp; Intermediary Intelligence</h3>' +
          (piLis
            ? '<ul class="gdi-meta-list">' + piLis + "</ul>"
            : '<p class="gdi-muted">Planner intelligence fields pending verification.</p>') +
          "</section>"
        );
      })() +
      '<section class="gdi-section" data-section-id="what_is_happening"><h3>What is happening</h3><p>' +
      esc(prose(whatS.body) || "—") +
      "</p>" +
      (whatS.activityDate
        ? "<p><strong>Original activity date:</strong> " +
          esc(whatS.activityDate) +
          "</p>"
        : "") +
      "</section>" +
      '<section class="gdi-section" data-section-id="potential_business"><h3>Potential business for this hotel</h3><p>' +
      esc(prose(potS.body) || "—") +
      "</p>" +
      (cleanList(potS.inferred).length
        ? "<p><strong>Inferred (not verified volume)</strong></p>" +
          listHtml(cleanList(potS.inferred))
        : "") +
      researchDetailExpandableHtml(o, "potentialBusinessForThisHotel", "Additional demand research", {
        esc: esc,
        prose: prose,
        cleanList: cleanList,
        listHtml: listHtml,
      }) +
      "</section>" +
      '<section class="gdi-section gdi-section--matters" data-section-id="why_this_hotel"><h3>Why this hotel</h3>' +
      whyBits +
      researchDetailExpandableHtml(o, "whyThisHotel", "Additional hotel-fit research", {
        esc: esc,
        prose: prose,
        cleanList: cleanList,
        listHtml: listHtml,
      }) +
      "</section>" +
      '<section class="gdi-section" data-section-id="why_act_now"><h3>Why act now</h3><p>' +
      esc(prose(nowS.body) || "—") +
      "</p></section>" +
      '<section class="gdi-section" data-section-id="facts_and_uncertainties"><h3>Established facts and uncertainties</h3>' +
      factsHtml +
      "</section>" +
      '<section class="gdi-section gdi-section--contact" data-section-id="who_to_contact"><h3>Who to contact</h3>' +
      (whoS.contactPathLabel &&
      !/not yet established|research incomplete|^NO_CONTACT$/i.test(
        String(whoS.contactPathLabel)
      )
        ? '<p class="gdi-muted"><strong>Primary route type:</strong> ' +
          esc(whoS.contactPathLabel) +
          "</p>"
        : "") +
      whoShouldSalesContactHtml(
        Object.assign({}, o, {
          contactName: contactNameSafe,
          primaryContact: primaryContactSafe ||
            (contactNameSafe || o.contactEmail || o.contactPhone || o.contactUrl
              ? {
                  name: contactNameSafe || null,
                  role: o.contactResponsibility || o.contactRole || null,
                  email: o.contactEmail || null,
                  phone: o.contactPhone || null,
                  officialContactUrl: o.contactUrl || null,
                }
              : null),
          organizationContactEmail:
            o.organizationContactEmail || o.contactEmail || null,
          companyPhone: o.companyPhone || null,
          contactOfficialUrl: o.contactUrl || o.contactOfficialUrl || null,
          commercialContactPathLabel: whoS.contactPathLabel || null,
        }),
        extras.whoContactHtml
      ) +
      researchDetailExpandableHtml(o, "buyerResearch", "Buyer research detail", {
        esc: esc,
        prose: prose,
        cleanList: cleanList,
        listHtml: listHtml,
      }) +
      "</section>" +
      '<section class="gdi-section gdi-section--action" data-section-id="recommended_next_step"><h3>Recommended next step</h3><p>' +
      esc(prose(nextS.body) || "—") +
      "</p>" +
      qHtml +
      "</section>" +
      '<section class="gdi-section gdi-section--evidence" data-section-id="supporting_evidence"><h3>Supporting evidence</h3>' +
      sourceListFromModel(evS.sources, evS.evidenceGroups, evS.sourcesFull) +
      (evS.activityDate
        ? "<p><strong>Original activity date:</strong> " +
          esc(evS.activityDate) +
          "</p>"
        : "") +
      (evS.researchVerificationDate
        ? "<p><strong>Research verification date:</strong> " +
          esc(evS.researchVerificationDate) +
          " (not the activity date)</p>"
        : "") +
      "</section>" +
      '<section class="gdi-section gdi-section--validation gdi-validation-panel" data-section-id="hotel_validation"><h3>Hotel Validation / Action / Outcome</h3>' +
      (extras.validationHtml ||
        '<p class="gdi-muted">Validation controls load with the live drawer.</p>') +
      "</section>" +
      "</div>"
    );
  }

  /**
   * Opening "Why this hotel" copy — property × prospect fit only.
   * Does not reuse opportunity description / growth signal as a hotel thesis.
   */
  function dedupeRepeatedFacilityProse(raw) {
    var s = String(raw || "").replace(/\s+/g, " ").trim();
    if (!s) return "";
    // Collapse duplicated Chapultepec/facility boilerplate joined without punctuation.
    var chunks = s.split(/(?=Limitation to respect:)|(?=Chapultepec \/ Polanco)/i);
    var seen = {};
    var out = [];
    for (var i = 0; i < chunks.length; i++) {
      var c = String(chunks[i] || "").replace(/\s+/g, " ").trim();
      if (!c) continue;
      var k = c.toLowerCase();
      if (seen[k]) continue;
      seen[k] = true;
      out.push(c);
    }
    return out.join(" ");
  }

  /**
   * run-052 Issue 2 — authoritative sales action from segment gold brief.
   * Association: associationEventsGoldBrief
   * Scaling: commercialIntelligenceBrief (then salesBrief) — never AEGB first
   * Top-level recommendedAction is fallback only when no gold approach exists.
   */
  function resolveAuthoritativeSalesActionText(o) {
    o = o || {};
    function approachText(brief) {
      if (!brief || !brief.recommendedSalesApproach) return "";
      var a = brief.recommendedSalesApproach;
      if (typeof a === "string") return String(a).replace(/\s+/g, " ").trim();
      if (a && typeof a === "object") {
        return String(a.nextAction || a.openingApproach || "")
          .replace(/\s+/g, " ")
          .trim();
      }
      return "";
    }
    function isAssocProcurement(s) {
      return /association staff\s*vs\s*PCO|PCO vs housing|host\/overflow|meetings\s*\/\s*congress\s*\/\s*operations route|Organizer TBD/i.test(
        String(s || "")
      );
    }
    function preferCertified(candidate) {
      var certified = String(o.recommendedNextStep || "")
        .replace(/\s+/g, " ")
        .trim();
      var c = String(candidate || "")
        .replace(/\s+/g, " ")
        .trim();
      if (certified && (!c || isAssocProcurement(c))) return certified;
      if (c && !isAssocProcurement(c)) return c;
      return certified || c || "";
    }
    var isScaling =
      /scaling/i.test(String(o.researchSegment || o.segment || "")) ||
      String(o.opportunityType || o.detailLayout || "").toUpperCase() ===
        "CORPORATE_ACCOUNT_RELATIONSHIP";
    if (isScaling) {
      // Prefer API-sanitized recommendedAction when present (already scrubbed).
      var top = String(
        o.recommendedAction || o.recommendedNextStep || o.salesActionRecommended || ""
      )
        .replace(/\s+/g, " ")
        .trim();
      if (top && !isAssocProcurement(top)) return top;
      var scaling = approachText(o.commercialIntelligenceBrief);
      if (scaling && !isAssocProcurement(scaling)) return scaling;
      var salesSc = approachText(o.salesBrief);
      if (salesSc && !isAssocProcurement(salesSc)) return salesSc;
      return preferCertified(top || scaling || salesSc || "");
    }
    var assoc = approachText(o.associationEventsGoldBrief);
    if (assoc && !isAssocProcurement(assoc)) return assoc;
    var scalingFallback = approachText(o.commercialIntelligenceBrief);
    if (scalingFallback && !isAssocProcurement(scalingFallback)) {
      return scalingFallback;
    }
    var sales = approachText(o.salesBrief);
    if (sales && !isAssocProcurement(sales)) return sales;
    return preferCertified(
      o.recommendedAction || o.recommendedNextStep || o.salesActionRecommended || assoc || ""
    );
  }

  function whyThisHotelOpeningHtml(o) {
    o = o || {};
    // run-052 Issue 2: prefer segment gold / CIB whyThisHotel over legacy salesBrief facilities copy.
    // Scaling: never prefer associationEventsGoldBrief over CIB.
    var isScalingWhy =
      /scaling/i.test(String(o.researchSegment || o.segment || "")) ||
      String(o.opportunityType || o.detailLayout || "").toUpperCase() ===
        "CORPORATE_ACCOUNT_RELATIONSHIP";
    var briefWhy = isScalingWhy
      ? (o.commercialIntelligenceBrief &&
          o.commercialIntelligenceBrief.whyThisHotel) ||
        (o.salesBrief && o.salesBrief.whyThisHotel) ||
        o.whyThisHotel ||
        null
      : (o.associationEventsGoldBrief && o.associationEventsGoldBrief.whyThisHotel) ||
        (o.commercialIntelligenceBrief &&
          o.commercialIntelligenceBrief.whyThisHotel) ||
        (o.salesBrief && o.salesBrief.whyThisHotel) ||
        null;
    var hotelSpecific = [
      briefWhy,
      o.whyThisHotel,
      o.hotelOpportunityThesis,
      o.summaryWhyHotel,
      o.fitExplanation,
    ]
      .map(function (v) {
        return dedupeRepeatedFacilityProse(v);
      })
      .filter(function (v) {
        // Assessment-status prose belongs in fit caveats, not Why This Hotel.
        // Hotel-agnostic: never prefer a candidate because it names a specific property brand.
        return Boolean(v) && !isAssessmentStatusProse(v);
      });
    var opportunityDesc = String(o.summaryWhat || o.title || "")
      .replace(/\s+/g, " ")
      .trim();
    var pick = "";
    for (var i = 0; i < hotelSpecific.length; i++) {
      var candidate = hotelSpecific[i];
      if (
        opportunityDesc &&
        candidate.toLowerCase() === opportunityDesc.toLowerCase()
      ) {
        continue;
      }
      // Prefer paragraphs that mention property attributes / location / facilities
      // (generic cues only — no hotel-name hardcoding)
      if (
        /hotel|property|downtown|meeting|guestroom|location|lobby|suite|ballroom|venue|proximity|walkable|transit|airport|capacity|rooms?\b/i.test(
          candidate
        ) ||
        candidate.length > 40
      ) {
        pick = candidate;
        break;
      }
      if (!pick) pick = candidate;
    }
    if (!pick) {
      return (
        "<p class=\"gdi-muted\">Hotel-specific rationale not yet established for this prospect — see dimension assessments below. Opportunity description alone is not treated as hotel fit.</p>"
      );
    }
    // Scaling / corporate-account relationships are not venue-sourcing RFPs —
    // never echo internal enums like OPEN_UNRESOLVED as "Sourcing status".
    var isCorporateAccount =
      String(o.opportunityType || o.detailLayout || "").toUpperCase() ===
        "CORPORATE_ACCOUNT_RELATIONSHIP" ||
      /scaling/i.test(String(o.researchSegment || o.segment || ""));
    var sourcingNote = "";
    var rawSourcing =
      o.venueSourcingStatusLabel || o.venueSourcingStatus || o.sourcingStatus || "";
    if (
      !isCorporateAccount &&
      rawSourcing &&
      !/^OPEN_UNRESOLVED$/i.test(String(rawSourcing)) &&
      !/open\s*\/?\s*unresolved/i.test(String(rawSourcing))
    ) {
      sourcingNote =
        '<p class="gdi-muted"><strong>Sourcing status:</strong> ' +
        esc(
          formatTileSecondaryHeadlineLabel(
            customerStatusLabel(rawSourcing, "Hotel Sourcing Unconfirmed")
          )
        ) +
        " — distinct from why this property could serve the use case.</p>";
    }
    return "<p>" + esc(pick) + "</p>" + sourcingNote;
  }

  /**
   * Shared hotel-fit assessment renderer (all opportunity types).
   * Expandable dimension tiles: score/status, rationale, facts, sources, date.
   * Keyboard + touch: <details>/<summary>. No fixed heights that clip labels.
   */
  function hotelFitAssessmentSectionHtml(o, audit) {
    audit = audit || {};
    o = o || {};
    var comps = (audit.hotelFit && audit.hotelFit.components) || {};
    var labels = o.hotelFitComponentLabels || {};
    var basis = o.hotelFitCalculationBasis || {};
    var overallStatus = String(o.hotelFitOverallStatus || "").toLowerCase();
    var withheld =
      basis.numericScoreEmitted === false ||
      overallStatus === "insufficient_evidence" ||
      /overall numeric withheld/i.test(String(basis.reason || "")) ||
      /overall numeric withheld|withheld — insufficient/i.test(
        String(o.hotelFitRationale || o.hotelFitScoreNote || "")
      );
    var rawScore = o.hotelFitScore;
    var numericScore =
      !withheld &&
      rawScore != null &&
      rawScore !== "" &&
      !Number.isNaN(Number(rawScore))
        ? Number(rawScore)
        : null;
    var hasScore = numericScore != null;
    var provisional =
      hasScore &&
      (o.hotelFitProvisional === true ||
        overallStatus === "provisional" ||
        basis.provisional === true ||
        (o.hotelFitScoreBasis &&
          String(o.hotelFitScoreBasis).indexOf("provisional") >= 0));
    var dimAssess =
      o.hotelFitDimensionAssessments ||
      (audit.hotelFit && audit.hotelFit.dimensionAssessments) ||
      null;
    var qualComps = o.hotelFitQualitativeComponents || {};
    var dimKeys = [
      "physicalFit",
      "geographyFit",
      "timing",
      "commercialValue",
      "historicalFit",
      "competitiveAccessibility",
      "contactability",
    ];
    var fitTextRaw =
      o.fitExplanation ||
      o.fitRationale ||
      o.hotelFitRationale ||
      (audit.hotelFit && audit.hotelFit.explanation) ||
      "";
    // Assessment-status prose belongs in expandable Overall details caveats only —
    // never as a visible lead paragraph above the score grid.
    var fitText = isAssessmentStatusProse(fitTextRaw) ? "" : fitTextRaw;

    function dimTileHtml(k) {
      var compLabel = labels[k] || fitLabel(k);
      var d = (dimAssess && dimAssess[k]) || (qualComps && qualComps[k]) || null;
      var status = d && d.status ? d.status : null;
      var scoreVal =
        status === "not_applicable" || status === "insufficient_evidence"
          ? null
          : comps[k] != null &&
              comps[k] !== "" &&
              !Number.isNaN(Number(comps[k]))
            ? Number(comps[k])
            : d && d.score != null
              ? Number(d.score)
              : null;
      var displayN;
      var statusClass = "";
      if (status === "not_applicable") {
        displayN = "Not applicable";
        statusClass = " is-na";
      } else if (status === "insufficient_evidence") {
        displayN = "Needs verification";
        statusClass = " is-gap";
      } else if (scoreVal != null) {
        displayN = String(scoreVal);
      } else {
        displayN = "Not established";
        statusClass = " is-gap";
      }
      // Static tile: score/status + title only (no Details expansion).
      return (
        '<div class="gdi-score-cell' +
        statusClass +
        '">' +
        '<div class="n">' +
        esc(displayN) +
        "</div>" +
        '<div class="l">' +
        esc(compLabel) +
        "</div>" +
        "</div>"
      );
    }

    // run-052 Issue 6 (RC-07): never paint seven empty "Not established" tiles
    // merely because the renderer can emit them. Require real dimension evidence
    // or an overall score/withheld state from a real assessment pass.
    function hasUsableDimAssessment(src) {
      if (!src || typeof src !== "object") return false;
      var keys = Object.keys(src);
      if (!keys.length) return false;
      for (var i = 0; i < keys.length; i++) {
        var d = src[keys[i]];
        if (!d || typeof d !== "object") continue;
        var st = String(d.status || "").toLowerCase();
        if (
          st === "assessed" ||
          st === "insufficient_evidence" ||
          st === "not_applicable" ||
          st === "provisional"
        ) {
          return true;
        }
        if (d.score != null && !Number.isNaN(Number(d.score))) return true;
        if (d.rationale || d.summary || d.explanation) return true;
      }
      return false;
    }
    var usableDims =
      o.hotelFitShowDimensionGrid === true ||
      hasUsableDimAssessment(dimAssess) ||
      hasUsableDimAssessment(qualComps);
    // Explicit false from sanitize wins over empty shells.
    if (o.hotelFitShowDimensionGrid === false) usableDims = false;
    var showDimGrid = usableDims || hasScore || withheld;
    var dimTiles = showDimGrid ? dimKeys.map(dimTileHtml).join("") : "";
    var hasDimTiles = Boolean(dimTiles);

    var overallDisplay = hasScore
      ? String(numericScore)
      : withheld
        ? "Withheld"
        : showDimGrid
          ? "Not established"
          : "Not assessed";

    var signalRowHtml =
      '<div class="gdi-signal-row gdi-signal-row--fit">' +
      "<div><span class=\"gdi-meta-label\">Sales posture</span><strong>" +
      esc(
        customerStatusLabel(
          o.opportunityQualificationLabel || o.opportunityQualification,
          "—"
        )
      ) +
      '</strong><span class="gdi-muted gdi-posture-note"> · ' +
      esc(
        o.hotelFitPostureNote ||
          "Sales posture is derived from hotel fit and territory evidence — not a separate win-probability analysis."
      ) +
      "</span></div>" +
      "<div><span class=\"gdi-meta-label\">Evidence confidence</span><strong>" +
      esc(
        (function () {
          if (o.evidenceConfidence == null || o.evidenceConfidence === "") {
            return "Not scored";
          }
          var n = Number(o.evidenceConfidence);
          if (Number.isNaN(n)) return String(o.evidenceConfidence);
          if (n > 0 && n <= 1) n = Math.round(n * 100);
          return String(Math.round(n)) + " / 100";
        })()
      ) +
      "</strong>" +
      (o.evidenceConfidenceExplanation
        ? '<details class="gdi-inline-details"><summary>What this means</summary><p>' +
          esc(o.evidenceConfidenceExplanation) +
          "</p><p class=\"gdi-muted\">Evidence confidence is not hotel fit, commercial availability, or win probability.</p></details>"
        : '<p class="gdi-muted gdi-posture-note">Scale 0–100 · distinct from hotel fit</p>') +
      "</div>" +
      "<div><span class=\"gdi-meta-label\">Sales action</span><strong>" +
      esc(
        resolveAuthoritativeSalesActionText(o) ||
          customerStatusLabel(
            o.bookingWindowLabel || o.bookingWindowStatus,
            actionStatusLabel(o.bookingWindowStatus) || "—"
          )
      ) +
      "</strong></div>" +
      "<div><span class=\"gdi-meta-label\">Recommended timing</span><strong>" +
      esc(
        customerStatusLabel(
          o.bookingWindowLabel || o.bookingWindowStatus,
          actionStatusLabel(o.bookingWindowStatus) || "—"
        )
      ) +
      "</strong></div></div>";

    if (!hasScore && !hasDimTiles && !withheld) {
      return (
        '<div class="gdi-hotel-fit-block">' +
        '<div class="gdi-fit-qualitative">' +
        (fitText
          ? "<p>" + esc(fitText) + "</p>"
          : "<p>" +
            esc(
              o.hotelFitUnassessedCustomerNote ||
                "Hotel-fit dimensions have not been assessed for this prospect. Territory notes above are not a completed dimension scorecard."
            ) +
            "</p>") +
        "</div>" +
        signalRowHtml +
        "</div>"
      );
    }

    return (
      '<div class="gdi-hotel-fit-block">' +
      '<div class="gdi-score-grid">' +
      '<div class="gdi-score-cell gdi-score-cell--overall' +
      (provisional ? " is-provisional" : "") +
      (withheld && !hasScore ? " is-gap" : "") +
      '">' +
      '<div class="n">' +
      esc(overallDisplay) +
      "</div>" +
      '<div class="l">Overall Hotel Fit' +
      (provisional
        ? ' <span class="gdi-fit-status">(provisional)</span>'
        : withheld && !hasScore
          ? ' <span class="gdi-fit-status">(withheld)</span>'
          : "") +
      "</div>" +
      "</div>" +
      dimTiles +
      "</div>" +
      signalRowHtml +
      "</div>"
    );
  }

  /**
   * Intelligence-to-action detail framework.
   * WHAT WE SEE / WHY IT MATTERS / WHY NOW / RECOMMENDED ACTION / CONTACT / EVIDENCE / VALIDATION
   * Type-aware: VENUE_PARTNERSHIP / SPECIFIC_PRIVATE_EVENT / CORPORATE_ACCOUNT_RELATIONSHIP layouts.
   */
  function isAssociationConferenceEventsOpportunityUi(o) {
    o = o || {};
    if (classifyOpportunityTypeFilter(o) === "ASSOCIATION_CONFERENCE_GROUP") {
      return true;
    }
    var seg = String(o.researchSegment || o.segment || "").toLowerCase();
    if (seg.indexOf("assoc") >= 0 || seg.indexOf("conference") >= 0) return true;
    if (o.associationEventsGoldBrief && seg.indexOf("scaling") < 0) return true;
    if (
      o.segmentReclassificationPreview === true &&
      seg === "associations_government"
    ) {
      return true;
    }
    // Event-shaped rows that historically fell through to legacy Account/occasion
    // (MICE / incentive / closed programmes with event dates).
    if (/\bmice\b|\bincentive/.test(seg)) return true;
    if (o.eventsDrawerModel || o.eventsOpportunityOverview) return true;
    if (
      (o.eventStartDate || o.eventEndDate || o.eventDateDisplay) &&
      !/scaling/i.test(seg) &&
      (o.opportunityType === "CLOSED_DISQUALIFIED" ||
        o.opportunityType === "ASSOCIATION_CONFERENCE" ||
        /event|conference|congress|summit|incentive/i.test(
          String(o.opportunityTypeLabel || o.title || "")
        ))
    ) {
      return true;
    }
    return false;
  }

  /**
   * Shared structured Section 1 grid for demand segments 3–8 (Events CSS reuse).
   * One layout system — identity line + labeled field grid.
   */
  function structuredSegmentOverviewHtml(overview) {
    var ov = overview || {};
    var fields = Array.isArray(ov.fields) ? ov.fields : [];
    var values = ov.values || ov;
    function row(label, value) {
      return (
        '<div class="gdi-events-overview-item"><span class="gdi-events-overview-label">' +
        esc(label) +
        '</span><span class="gdi-events-overview-value">' +
        esc(value || "—") +
        "</span></div>"
      );
    }
    var grid = "";
    if (fields.length) {
      for (var i = 0; i < fields.length; i++) {
        var f = fields[i];
        var key = f && f.key ? f.key : "";
        var label = f && f.label ? f.label : key;
        grid += row(label, values[key]);
      }
    }
    return (
      '<div class="gdi-events-opportunity-overview" data-overview-version="' +
      esc(ov.overviewMode || "segment_structured_overview_v1") +
      '" data-segment-id="' +
      esc(ov.segmentId || "") +
      '">' +
      '<p class="gdi-events-overview-identity">' +
      esc(ov.identityLine || "—") +
      "</p>" +
      '<div class="gdi-events-overview-grid">' +
      grid +
      "</div></div>"
    );
  }

  function eventsOpportunityOverviewHtml(o) {
    var ov =
      (o && o.eventsOpportunityOverview) ||
      (o &&
        o.commercialIntelligenceBrief &&
        o.commercialIntelligenceBrief.eventsOpportunityOverview) ||
      (o &&
        o.associationEventsGoldBrief &&
        o.associationEventsGoldBrief.eventsOpportunityOverview) ||
      null;
    if (!ov) {
      // Client-side fallback when sanitize has not attached overview yet.
      // Never prefer series-identity slugs (normalizedEventName / underscore tokens).
      function isSlugToken(s) {
        var t = String(s || "").trim();
        if (!t) return true;
        if (/^[a-z0-9]+(_[a-z0-9]+)+$/.test(t)) return true;
        if (!/\s/.test(t) && !/[A-ZÁÉÍÓÚÑ]/.test(t) && /^[a-z0-9_.-]{2,64}$/.test(t)) {
          return true;
        }
        return false;
      }
      var org =
        String(o.displayCompanyName || o.organizationName || o.companyName || "").trim() ||
        "Organizer TBD";
      var rawCandidates = [
        o.eventDisplayName,
        o.eventName,
        o.title,
        o.displayTitle,
        o.canonicalEventName,
      ];
      var ev = "";
      for (var ci = 0; ci < rawCandidates.length; ci++) {
        var cand = String(rawCandidates[ci] || "").trim();
        if (!cand || isSlugToken(cand)) continue;
        if (cand.toLowerCase() === org.toLowerCase()) continue;
        ev = cand;
        break;
      }
      if (!ev) ev = "Programme TBD";
      var identity =
        org.toLowerCase() === ev.toLowerCase() ? org : org + " — " + ev;
      ov = {
        identityLine: identity,
        timing:
          eventDateRangeLabel(o.eventStartDate, o.eventEndDate, "Dates TBD") ||
          "Dates TBD",
        location:
          o.eventLocationSummary ||
          [o.eventLocationCity || o.eventCity, o.venueCity, o.city]
            .filter(Boolean)
            .join(", ") ||
          "Location TBD",
        hotelSourcing: "Sourcing unconfirmed",
        roomDemand: "Room demand not yet published",
        demandCycle: "Future cycle expected",
        attendance: "Not yet published",
        peakRooms: "Not yet published",
      };
    }
    function row(label, value) {
      return (
        '<div class="gdi-events-overview-item"><span class="gdi-events-overview-label">' +
        esc(label) +
        '</span><span class="gdi-events-overview-value">' +
        esc(value || "—") +
        "</span></div>"
      );
    }
    return (
      '<div class="gdi-events-opportunity-overview" data-overview-version="gdi-events-opportunity-overview-v1">' +
      '<p class="gdi-events-overview-identity">' +
      esc(ov.identityLine || "—") +
      "</p>" +
      '<div class="gdi-events-overview-grid">' +

      row("Timing", ov.timing) +
      row("Location", ov.location) +
      row("Hotel sourcing", ov.hotelSourcing) +
      row("Room demand", ov.roomDemand) +
      row("Demand cycle", ov.demandCycle) +
      row("Attendance", ov.attendance) +
      row("Peak rooms", ov.peakRooms) +
      "</div>" +
      (ov.venueConfirmationNote
        ? '<p class="gdi-events-overview-venue-note gdi-muted">' +
          esc(ov.venueConfirmationNote) +
          "</p>"
        : "") +
      "</div>"
    );
  }

  /**
   * Association / Conference / Events — ten-section drawer.
   * Section 1 = structured Events overview grid (NOT Scaling narrative).
   * Sections 2–10 = same hierarchy / subsections as Scaling Companies gold.
   */
  function associationConferenceEventsDetailHtml(o, audit, extras) {
    extras = extras || {};
    o = o || {};

    function prose(v) {
      return String(v || "")
        .replace(/\s+/g, " ")
        .trim();
    }
    function cleanList(items) {
      if (!Array.isArray(items)) return [];
      return items
        .map(prose)
        .filter(Boolean)
        .filter(function (t) {
          return !/^—+$/.test(t) && t !== "-" && t !== "•";
        });
    }
    function listHtml(items) {
      if (!items || !items.length) return "";
      return (
        "<ul>" +
        items
          .map(function (t) {
            return "<li>" + esc(t) + "</li>";
          })
          .join("") +
        "</ul>"
      );
    }
    function sourceListFromModel(sources, evidenceGroups, sourcesFull) {
      if ((!sources || !sources.length) && (!sourcesFull || !sourcesFull.length)) {
        return '<p class="gdi-muted">No source URLs attached yet.</p>';
      }
      function renderOne(s) {
        var url = prose(s && s.url);
        if (!url) return "";
        var title = prose(s && s.title) || url;
        var publisher = prose(s && s.publisher);
        var date = prose(s && (s.publicationDate || s.publishedDate));
        var establishes = prose(s && s.establishes);
        var bits =
          '<li class="gdi-evidence-item">' +
          '<div class="gdi-evidence-title"><strong>' +
          esc(title) +
          "</strong></div>";
        if (publisher) {
          bits +=
            '<div class="gdi-muted">Publisher: ' + esc(publisher) + "</div>";
        }
        if (date) {
          bits += '<div class="gdi-muted">Date: ' + esc(date) + "</div>";
        }
        if (establishes && establishes.length <= 120) {
          bits += '<div class="gdi-muted">' + esc(establishes) + "</div>";
        }
        bits +=
          '<div><a href="' +
          esc(url) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(url) +
          "</a></div></li>";
        return bits;
      }
      var defaultHtml = "";
      if (sources && sources.length) {
        defaultHtml =
          '<ul class="gdi-evidence-default">' +
          sources.map(renderOne).filter(Boolean).join("") +
          "</ul>";
      }
      var full = Array.isArray(sourcesFull) ? sourcesFull : [];
      var defaultCount = (sources && sources.length) || 0;
      if (full.length > defaultCount && full.length > 0) {
        defaultHtml +=
          '<details class="gdi-evidence-expand">' +
          "<summary>View all supporting evidence (" +
          full.length +
          ")</summary>" +
          '<ul class="gdi-evidence-full">' +
          full.map(renderOne).filter(Boolean).join("") +
          "</ul></details>";
      }
      return defaultHtml || '<p class="gdi-muted">No source URLs attached yet.</p>';
    }

    var model = o.eventsDrawerModel || null;
    if (!model || !model.sections) {
      var cib = o.commercialIntelligenceBrief || o.salesBrief || {};
      var aegb = o.associationEventsGoldBrief || {};
      function pick() {
        for (var i = 0; i < arguments.length; i++) {
          var v = arguments[i];
          if (v == null) continue;
          var s = String(v).replace(/\s+/g, " ").trim();
          if (s) return s;
        }
        return "";
      }
      model = {
        overviewMode: "events_structured_grid_v1",
        sections: {
          what_is_happening: {
            body: pick(
              cib.whatIsHappening,
              aegb.eventHistoryAndActivity,
              o.summaryWhat,
              o.whatIsHappening
            ),
            activityDate: null,
          },
          potential_business: {
            body: pick(
              cib.potentialBusinessForThisHotel,
              cib.potentialHotelBusiness,
              aegb.potentialBusinessForHotel,
              o.whyHotelDemand,
              o.proposedHotelUse
            ),
            inferred: [],
          },
          why_this_hotel: {
            body: pick(cib.whyThisHotel, aegb.whyThisHotel, o.whyThisHotel),
            advantages: cleanList(cib.whyThisHotelAdvantages),
            limitations: cleanList(cib.whyThisHotelLimitations),
          },
          why_act_now: {
            body: pick(cib.whyActNow, o.whyNow, o.summaryWhyNow),
          },
          facts_and_uncertainties: {
            facts: cleanList(
              cib.establishedFacts ||
                (cib.establishedFactsAndUncertainties &&
                  cib.establishedFactsAndUncertainties.facts) ||
                (cib.evidenceAndUncertainty &&
                  cib.evidenceAndUncertainty.facts) ||
                o.establishedFacts
            ),
            uncertainties: cleanList(
              (cib.establishedFactsAndUncertainties &&
                cib.establishedFactsAndUncertainties.uncertainties) ||
                (cib.evidenceAndUncertainty &&
                  cib.evidenceAndUncertainty.uncertainties) ||
                o.uncertainties
            ),
            factsEmptyNote:
              "No separately listed established facts beyond the activity narrative above.",
          },
          who_to_contact: {
            contactPathLabel: prose(o.commercialContactPathLabel) || null,
          },
          recommended_next_step: {
            body: pick(
              resolveAuthoritativeSalesActionText(o),
              cib.recommendedNextStep,
              o.recommendedAction
            ),
            qualificationQuestions: cleanList(
              cib.qualificationQuestions ||
                (cib.recommendedSalesApproach &&
                  cib.recommendedSalesApproach.qualificationQuestions) ||
                o.qualificationQuestions ||
                o.suggestedQualificationQuestions
            ),
          },
          supporting_evidence: {
            sources: (o.sources || [])
              .map(function (src) {
                return {
                  url: prose(src && (src.url || src)),
                  title: prose(src && src.title) || null,
                };
              })
              .filter(function (x) {
                return x.url;
              }),
            sourcesFull: [],
          },
        },
      };
    }

    var sec = model.sections;
    var whatS = sec.what_is_happening || {};
    var potS = sec.potential_business || {};
    var whyS = sec.why_this_hotel || {};
    var nowS = sec.why_act_now || {};
    var factS = sec.facts_and_uncertainties || {};
    var whoS = sec.who_to_contact || {};
    var nextS = sec.recommended_next_step || {};
    var evS = sec.supporting_evidence || {};

    var factsHtml = "";
    var factsList = cleanList(factS.facts);
    var uncList = cleanList(factS.uncertainties);
    if (factsList.length) {
      factsHtml +=
        "<p><strong>Established facts</strong></p>" + listHtml(factsList);
    } else if (factS.factsEmptyNote) {
      factsHtml +=
        '<p class="gdi-muted">' + esc(factS.factsEmptyNote) + "</p>";
    }
    if (uncList.length) {
      factsHtml +=
        "<p><strong>Uncertainties</strong></p>" + listHtml(uncList);
    }

    var whyBody = prose(whyS.body);
    var adv = cleanList(whyS.advantages);
    var lim = cleanList(whyS.limitations);
    if (adv.length || lim.length) {
      var packWhy = adv.concat(lim).join(" ").toLowerCase();
      if (
        whyBody &&
        packWhy.indexOf(
          whyBody.toLowerCase().slice(0, Math.min(80, whyBody.length))
        ) !== -1
      ) {
        whyBody = "";
      }
    }
    var whyBits = whyBody ? "<p>" + esc(whyBody) + "</p>" : "";
    if (adv.length) {
      whyBits += "<p><strong>Advantages</strong></p>" + listHtml(adv);
    }
    if (lim.length) {
      whyBits +=
        "<p><strong>Limitations / trade-offs</strong></p>" + listHtml(lim);
    }
    if (!whyBits) whyBits = "<p>—</p>";

    var qHtml = "";
    var qs = cleanList(nextS.qualificationQuestions);
    if (qs.length) {
      qHtml =
        "<p><strong>Qualification questions</strong></p>" + listHtml(qs);
    }

    return (
      '<div class="gdi-detail-framework gdi-detail-framework--events-canonical gdi-detail-framework--scaling-parity" data-events-overview-mode="structured_grid_v1">' +
      commercialProgressionHtml(o.commercialProgression) +
      '<section class="gdi-section" data-section-id="opportunity_overview"><h3>Opportunity overview</h3>' +
      eventsOpportunityOverviewHtml(o) +
      "</section>" +
      needDateMatchDrawerSectionHtml(o) +
      '<section class="gdi-section" data-section-id="what_is_happening"><h3>What is happening</h3><p>' +
      esc(prose(whatS.body) || "—") +
      "</p>" +
      (whatS.activityDate
        ? "<p><strong>Original activity date:</strong> " +
          esc(whatS.activityDate) +
          "</p>"
        : "") +
      "</section>" +
      '<section class="gdi-section" data-section-id="potential_business"><h3>Potential business for this hotel</h3><p>' +
      esc(prose(potS.body) || "—") +
      "</p>" +
      (cleanList(potS.inferred).length
        ? "<p><strong>Inferred (not verified volume)</strong></p>" +
          listHtml(cleanList(potS.inferred))
        : "") +
      researchDetailExpandableHtml(o, "potentialBusinessForThisHotel", "Additional demand research", {
        esc: esc,
        prose: prose,
        cleanList: cleanList,
        listHtml: listHtml,
      }) +
      "</section>" +
      '<section class="gdi-section gdi-section--matters" data-section-id="why_this_hotel"><h3>Why this hotel</h3>' +
      whyBits +
      researchDetailExpandableHtml(o, "whyThisHotel", "Additional hotel-fit research", {
        esc: esc,
        prose: prose,
        cleanList: cleanList,
        listHtml: listHtml,
      }) +
      "</section>" +
      '<section class="gdi-section" data-section-id="why_act_now"><h3>Why act now</h3><p>' +
      esc(prose(nowS.body) || "—") +
      "</p></section>" +
      '<section class="gdi-section" data-section-id="facts_and_uncertainties"><h3>Established facts and uncertainties</h3>' +
      factsHtml +
      "</section>" +
      '<section class="gdi-section gdi-section--contact" data-section-id="who_to_contact"><h3>Who to contact</h3>' +
      (whoS.contactPathLabel &&
      !/not yet established|research incomplete|^NO_CONTACT$/i.test(
        String(whoS.contactPathLabel)
      )
        ? '<p class="gdi-muted"><strong>Primary route type:</strong> ' +
          esc(whoS.contactPathLabel) +
          "</p>"
        : "") +
      whoShouldSalesContactHtml(o, extras.whoContactHtml) +
      researchDetailExpandableHtml(o, "buyerResearch", "Buyer research detail", {
        esc: esc,
        prose: prose,
        cleanList: cleanList,
        listHtml: listHtml,
      }) +
      "</section>" +
      '<section class="gdi-section gdi-section--action" data-section-id="recommended_next_step"><h3>Recommended next step</h3><p>' +
      esc(prose(nextS.body) || "—") +
      "</p>" +
      qHtml +
      "</section>" +
      '<section class="gdi-section gdi-section--evidence" data-section-id="supporting_evidence"><h3>Supporting evidence</h3>' +
      sourceListFromModel(evS.sources, evS.evidenceGroups, evS.sourcesFull) +
      (evS.activityDate
        ? "<p><strong>Original activity date:</strong> " +
          esc(evS.activityDate) +
          "</p>"
        : "") +
      (evS.researchVerificationDate
        ? "<p><strong>Research verification date:</strong> " +
          esc(evS.researchVerificationDate) +
          " (not the activity date)</p>"
        : "") +
      "</section>" +
      '<section class="gdi-section gdi-section--validation gdi-validation-panel" data-section-id="hotel_validation"><h3>Hotel Validation / Action / Outcome</h3>' +
      (extras.validationHtml ||
        '<p class="gdi-muted">Validation controls load with the live drawer.</p>') +
      "</section>" +
      "</div>"
    );
  }

  function intelligenceDetailHtml(o, audit, extras) {
    extras = extras || {};
    // Segment 8 subtypes use shared ten-section drawer + conditional intelligence
    // sections — not the legacy standalone venue layout.
    var isPreferredLodgingShared =
      o.preferredLodgingPartnershipV1 === true ||
      o.demandOpportunitySubtype === "PREFERRED_LODGING_PARTNERSHIP" ||
      o.opportunityType === "PREFERRED_LODGING_PARTNERSHIP" ||
      o.detailLayout === "PREFERRED_LODGING_PARTNERSHIP_SHARED_DRAWER" ||
      Boolean(o.venueIntelligence && o.sharedTenSectionDrawer);
    var isProfessionalEventPlannerShared =
      o.professionalEventPlannerPartnershipV1 === true ||
      o.demandOpportunitySubtype ===
        "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
      o.opportunityType === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
      o.detailLayout === "PROFESSIONAL_EVENT_PLANNER_SHARED_DRAWER" ||
      Boolean(o.plannerIntelligence && o.sharedTenSectionDrawer);
    if (
      !isPreferredLodgingShared &&
      !isProfessionalEventPlannerShared &&
      (o.detailLayout === "VENUE_PARTNERSHIP" ||
        o.opportunityType === "VENUE_PARTNERSHIP")
    ) {
      return venuePartnershipDetailHtml(o, audit, extras);
    }
    if (
      o.detailLayout === "SPECIFIC_PRIVATE_EVENT" ||
      o.opportunityType === "SPECIFIC_PRIVATE_EVENT"
    ) {
      return specificPrivateEventDetailHtml(o, audit, extras);
    }
    // Scaling Companies → corporate-account layout. Segment wins over missing
    // opportunityType/detailLayout and over a stray associationEventsGoldBrief.
    var segUi = String(o.researchSegment || o.segment || "").toLowerCase();
    var demandId = String(o.demandSegmentId || "").toLowerCase();
    var reclassEvents =
      o.segmentReclassificationPreview === true && segUi.indexOf("assoc") >= 0;
    var isScalingDrawer =
      !reclassEvents &&
      segUi !== "associations_government" &&
      (o.detailLayout === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
        o.opportunityType === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
        classifyOpportunityTypeFilter(o) === "SCALING_COMPANY" ||
        /scaling/i.test(segUi));
    // Shared twelve-segment drawers (Gov / Local Catering / Relocation / …):
    // same ten-section Scaling renderer + segment-specific Section 1 grid.
    // Must never fall through to legacy Account/occasion headings.
    var SHARED_SEGMENT_IDS = {
      government_public_sector: 1,
      local_catering_day_meetings: 1,
      relocation_extended_stay: 1,
      corporate_meetings_incentives: 1,
      sports_entertainment: 1,
      education_scientific: 1,
      projects_infrastructure: 1,
      social_special_events: 1,
      corporate_celebrations_private_events: 1,
      travel_trade_group_intermediaries: 1,
    };
    var isSharedSegmentDrawer =
      !reclassEvents &&
      (SHARED_SEGMENT_IDS[demandId] === 1 ||
        SHARED_SEGMENT_IDS[segUi] === 1 ||
        Boolean(o.sharedTenSectionDrawer) ||
        Boolean(o.segmentOverviewModel && o.segmentOverviewModel.structuredGrid) ||
        String(o.id || "").indexOf("gdi_s4912_") === 0);
    // Events must not use Scaling overview — route Association/Conference first
    // unless this row is genuinely Scaling Companies.
    if (!isScalingDrawer && isAssociationConferenceEventsOpportunityUi(o)) {
      return associationConferenceEventsDetailHtml(o, audit, extras);
    }
    if (isScalingDrawer || isSharedSegmentDrawer) {
      return corporateAccountRelationshipDetailHtml(o, audit, extras);
    }
    // Remaining Association-shaped rows still get Events ten-section
    if (isAssociationConferenceEventsOpportunityUi(o)) {
      return associationConferenceEventsDetailHtml(o, audit, extras);
    }

    // UNIVERSAL DRAWER MANDATE — never render legacy Account / occasion headings.
    // Normalize unrouted rows into Events (event-shaped) or Scaling ten-section.
    if (
      o.eventStartDate ||
      o.eventEndDate ||
      o.eventDateDisplay ||
      o.eventsDrawerModel ||
      /event|conference|congress|summit|incentive|mice|association/i.test(
        String(
          (o.researchSegment || "") +
            " " +
            (o.segment || "") +
            " " +
            (o.opportunityType || "") +
            " " +
            (o.title || "")
        )
      )
    ) {
      return associationConferenceEventsDetailHtml(o, audit, extras);
    }
    return corporateAccountRelationshipDetailHtml(o, audit, extras);

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
      "<label>Founder review (local)<select id=\"gdiSvFounderReview\">" +
      '<option value="">—</option>' +
      '<option value="ASSIGN_NOW"' +
      ((o && o.founderReviewDecision) === "ASSIGN_NOW" ? " selected" : "") +
      ">ASSIGN_NOW</option>" +
      '<option value="ASSIGN_FOR_DISCOVERY"' +
      ((o && o.founderReviewDecision) === "ASSIGN_FOR_DISCOVERY" ? " selected" : "") +
      ">ASSIGN_FOR_DISCOVERY</option>" +
      '<option value="RESEARCH_FIRST"' +
      ((o && o.founderReviewDecision) === "RESEARCH_FIRST" ? " selected" : "") +
      ">RESEARCH_FIRST</option>" +
      '<option value="DO_NOT_PURSUE"' +
      ((o && o.founderReviewDecision) === "DO_NOT_PURSUE" ? " selected" : "") +
      ">DO_NOT_PURSUE</option>" +
      "</select></label>" +
      '<textarea id="gdiSvNote" placeholder="Optional note" rows="2">' +
      esc(v.note || (o && o.founderReviewComment) || "") +
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

  /** Brand AI / ADP intelligence-product loading toast (deal-workspace-shell .bdd-toast). */
  var DEFAULT_PAGE_LOADING_MESSAGE = "Loading Group Demand Intelligence\u2026";
  var DETAIL_LOADING_MESSAGE = "Loading\u2026";

  /** Shared wave-orb markup (same primitives as page .bdd-toast.aiv-loading-toast). */
  function loadingWaveOrbHtml() {
    return (
      '<div class="toast-wave-container" aria-hidden="true">' +
      '<div class="wave-container">' +
      '<div class="wave wave-1"></div>' +
      '<div class="wave wave-2"></div>' +
      '<div class="wave wave-3"></div>' +
      '<div class="wave-particles">' +
      '<div class="particle"></div>' +
      '<div class="particle"></div>' +
      '<div class="particle"></div>' +
      '<div class="particle"></div>' +
      "</div></div></div>"
    );
  }

  /**
   * Inline drawer loader — same wave orb / typography as page toast, static in drawer body.
   * Do not use the full-page fixed toast inside the drawer.
   */
  function detailLoadingHtml() {
    return (
      '<div class="gdi-detail-loading bdd-toast aiv-loading-toast show" role="status" aria-live="polite" aria-busy="true">' +
      loadingWaveOrbHtml() +
      '<div class="toast-message">' +
      DETAIL_LOADING_MESSAGE +
      "</div></div>"
    );
  }

  /**
   * Canonical drawer error — replaces loader; optional Try again when opportunity id known.
   * @param {string} [message]
   * @param {{ retryOpportunityId?: string }} [opts]
   */
  function detailErrorHtml(message, opts) {
    opts = opts || {};
    var msg = message || "Unable to load opportunity detail.";
    var retryId = opts.retryOpportunityId ? String(opts.retryOpportunityId) : "";
    var retryBtn = retryId
      ? '<p class="gdi-detail-error__actions"><button type="button" class="gdi-btn gdi-btn-primary" data-gdi-detail-retry="' +
        esc(retryId) +
        '">Try again</button></p>'
      : "";
    return (
      '<div class="gdi-error gdi-detail-error" role="alert">' +
      "<p>" +
      esc(msg) +
      "</p>" +
      retryBtn +
      "</div>"
    );
  }

  /**
   * Show/hide canonical top-right loading toast (same controller pattern as Brand AI setLoading).
   * @param {HTMLElement|null} el toast root (.bdd-toast.aiv-loading-toast)
   * @param {boolean} on
   * @param {string} [message]
   */
  function setLoadingToast(el, on, message) {
    if (!el) return;
    var msgEl =
      (el.id ? document.getElementById(el.id + "Message") : null) ||
      el.querySelector(".toast-message");
    if (msgEl && message) msgEl.textContent = message;
    if (on) {
      if (msgEl && !message) msgEl.textContent = DEFAULT_PAGE_LOADING_MESSAGE;
      el.hidden = false;
      el.style.display = "flex";
      el.setAttribute("aria-busy", "true");
      el.classList.remove("show");
      void el.offsetHeight;
      requestAnimationFrame(function () {
        el.classList.add("show");
      });
    } else {
      el.classList.remove("show");
      el.setAttribute("aria-busy", "false");
      setTimeout(function () {
        if (!el.classList.contains("show")) {
          el.style.display = "none";
          el.hidden = true;
        }
      }, 300);
    }
  }

  root.DealalityGdiUi = {
    PRIORITY_DISPLAY: PRIORITY_DISPLAY,
    ACTION_STATUS_DISPLAY: ACTION_STATUS_DISPLAY,
    SORT_OPTIONS: SORT_OPTIONS,
    esc: esc,
    isSafeHttpUrl: isSafeHttpUrl,
    formatDrawerFieldValueHtml: formatDrawerFieldValueHtml,
    drawerMetaFieldRowHtml: drawerMetaFieldRowHtml,
    fmtVal: fmtVal,
    trunc: trunc,
    toProperCaseDisplayTitle: toProperCaseDisplayTitle,
    toProperCaseContactRoleLabel: toProperCaseContactRoleLabel,
    formatTileSecondaryHeadlineLabel: formatTileSecondaryHeadlineLabel,
    scrubEventDatesFromText: scrubEventDatesFromText,
    eventDateRangeLabel: eventDateRangeLabel,
    priorityPill: priorityPill,
    actionStatusLabel: actionStatusLabel,
    hotelLocationLine: hotelLocationLine,
    statusBadgesHtml: statusBadgesHtml,
    hotelShellHtml: hotelShellHtml,
    propertyBarHtml: propertyBarHtml,
    weeklyThisWeekFilterHtml: weeklyThisWeekFilterHtml,
    opportunityTypeFilterHtml: opportunityTypeFilterHtml,
    classifyOpportunityTypeFilter: classifyOpportunityTypeFilter,
    countByOpportunityTypeFilter: countByOpportunityTypeFilter,
    opportunityTypeFilterOptionsFromCounts: opportunityTypeFilterOptionsFromCounts,
    opportunityTypeCountsForFilters: opportunityTypeCountsForFilters,
    isAssessmentStatusProse: isAssessmentStatusProse,
    primaryKpiHtml: primaryKpiHtml,
    secondarySummaryHtml: secondarySummaryHtml,
    buildMetricModel: buildMetricModel,
    contentTabsHtml: contentTabsHtml,
    formatTabLabel: formatTabLabel,
    GDI_MAIN_TAB: GDI_MAIN_TAB,
    customerSafeRunStatusLabel: customerSafeRunStatusLabel,
    countByPriority: countByPriority,
    countByActionStatus: countByActionStatus,
    facetCountByPriority: facetCountByPriority,
    facetCountByActionStatus: facetCountByActionStatus,
    facetCountByCommercialStage: facetCountByCommercialStage,
    facetCountBySalesOpportunity: facetCountBySalesOpportunity,
    facetCountBySalesFocus: facetCountBySalesFocus,
    countByCommercialStage: countByCommercialStage,
    countBySalesOpportunity: countBySalesOpportunity,
    countBySalesFocus: countBySalesFocus,
    reconcileBrowseFilters: reconcileBrowseFilters,
    resetBrowseFilters: resetBrowseFilters,
    actionPresetHtml: actionPresetHtml,
    priorityPresetHtml: priorityPresetHtml,
    commercialStagePresetHtml: commercialStagePresetHtml,
    salesFocusPresetHtml: salesFocusPresetHtml,
    salesOpportunityPresetHtml: salesOpportunityPresetHtml,
    workflowPresetHtml: workflowPresetHtml,
    normalizeCustomerWorkflowFilter: normalizeCustomerWorkflowFilter,
    resultsToolbarHtml: resultsToolbarHtml,
    filterOpportunities: filterOpportunities,
    sortOpportunities: sortOpportunities,
    opportunityToBriefItem: opportunityToBriefItem,
    pursuitActionButtonHtml: pursuitActionButtonHtml,
    pursuitPanelHtml: pursuitPanelHtml,
    opportunityTileHtml: opportunityTileHtml,
    revenueStreamIconsHtml: revenueStreamIconsHtml,
    resolveRevenueStreamIconState: resolveRevenueStreamIconState,
    needDateIntelligenceIconHtml: needDateIntelligenceIconHtml,
    needDateMatchDrawerSectionHtml: needDateMatchDrawerSectionHtml,
    normalizeHotelNeedDateConfigV1: normalizeHotelNeedDateConfigV1,
    hotelNeedDateConfigStatusHtml: hotelNeedDateConfigStatusHtml,
    needDateInformationLegendHtml: needDateInformationLegendHtml,
    needDatesHeadingWithLegendHtml: needDatesHeadingWithLegendHtml,
    bindNeedDateLegendInteractions: bindNeedDateLegendInteractions,
    gdiSvgCalendarCheck: gdiSvgCalendarCheck,
    gdiSvgCalendarClock: gdiSvgCalendarClock,
    gdiSvgCalendarSearch: gdiSvgCalendarSearch,
    gdiSvgCalendarDashed: gdiSvgCalendarDashed,
    gdiSvgCalendarMinus: gdiSvgCalendarMinus,
    gdiSvgCalendarX: gdiSvgCalendarX,
    gdiSvgCalendarQuestion: gdiSvgCalendarQuestion,
    resolveOpportunityNeedDateIndicatorForUi: resolveOpportunityNeedDateIndicatorForUi,
    mapToCustomerNeedDateDisplayV3: mapToCustomerNeedDateDisplayV3,
    gdiSvgInfoCircle: gdiSvgInfoCircle,
    tilePriorityPill: tilePriorityPill,
    tileSalesActionPill: tileSalesActionPill,
    tileEventDatePill: tileEventDatePill,
    selectCompactTileContact: selectCompactTileContact,
    resolveTileContactPreviewBlock: resolveTileContactPreviewBlock,
    compactTileContactHtml: compactTileContactHtml,
    opportunityCardsGridHtml: opportunityCardsGridHtml,
    opportunityBrowseChromeHtml: opportunityBrowseChromeHtml,
    inventoryViewToggleHtml: inventoryViewToggleHtml,
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
    resolveAuthoritativeSalesActionText: resolveAuthoritativeSalesActionText,
    venuePartnershipDetailHtml: venuePartnershipDetailHtml,
    specificPrivateEventDetailHtml: specificPrivateEventDetailHtml,
    corporateAccountRelationshipDetailHtml: corporateAccountRelationshipDetailHtml,
    hotelFitAssessmentSectionHtml: hotelFitAssessmentSectionHtml,
    whyThisHotelOpeningHtml: whyThisHotelOpeningHtml,
    sourceListHtml: sourceListHtml,
    whoShouldSalesContactHtml: whoShouldSalesContactHtml,
    secondaryNamedContactHtml: secondaryNamedContactHtml,
    fitLabel: fitLabel,
    assertNoHotelHardcode: assertNoHotelHardcode,
    DEFAULT_PAGE_LOADING_MESSAGE: DEFAULT_PAGE_LOADING_MESSAGE,
    DETAIL_LOADING_MESSAGE: DETAIL_LOADING_MESSAGE,
    detailLoadingHtml: detailLoadingHtml,
    detailErrorHtml: detailErrorHtml,
    setLoadingToast: setLoadingToast,
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
