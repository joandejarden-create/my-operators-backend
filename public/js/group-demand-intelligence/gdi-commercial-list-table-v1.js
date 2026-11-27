/**
 * GDI Commercial Opportunity List View — sortable table (Deal Opportunities parity).
 * Additive presentation only. Tiles View remains unchanged.
 * Contract: gdi-commercial-list-table-v1
 */
(function (global) {
  "use strict";

  var CONTRACT = "gdi-commercial-list-table-v1";
  var STORAGE_COLUMNS = "gdi.listOptionalColumns.v1";

  var ICON_VIEW =
    '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>';
  var ICON_MORE =
    '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="5" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="12" cy="19" r="1"/></svg>';
  var ICON_EMAIL =
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>';
  var ICON_PHONE =
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.13.96.37 1.9.72 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.91.35 1.85.59 2.81.72A2 2 0 0 1 22 16.92z"/></svg>';
  var ICON_LINKEDIN =
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true"><path d="M4.98 3.5C4.98 4.88 3.86 6 2.5 6S0 4.88 0 3.5 1.12 1 2.5 1s2.48 1.12 2.48 2.5zM.5 8.5h4V23h-4V8.5zM8.5 8.5h3.8v2h.05c.53-1 1.82-2.05 3.75-2.05 4.01 0 4.75 2.64 4.75 6.07V23h-4v-5.9c0-1.41-.03-3.22-1.96-3.22-1.96 0-2.26 1.53-2.26 3.12V23h-4V8.5z"/></svg>';

  var SORT_INDICATOR =
    '<span class="sort-indicator" aria-hidden="true"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>';

  var DEFAULT_COLUMNS = [
    { id: "opportunity", label: "Opportunity", sortKey: "title", required: true },
    { id: "priority", label: "Priority", sortKey: "priority", required: true },
    { id: "demandType", label: "Demand Type", sortKey: "demandType", required: true },
    { id: "demandWindow", label: "Demand Window", sortKey: "eventStartDate", required: true },
    { id: "salesAction", label: "Sales Action", sortKey: "salesAction", required: true },
    { id: "whoToContact", label: "Who to Contact", sortKey: "whoToContact", required: true },
    { id: "contactDetails", label: "Contact Details", sortKey: "contactDetails", required: true },
    { id: "actions", label: "Actions", sortKey: null, required: true, noSort: true },
  ];

  // Permanently removed from List View (data retained for filters / ES / Admin):
  // - Contact Readiness
  // - Opportunity Status
  // Sales Opportunity Type removed earlier (Demand Type covers it).
  var RETIRED_OPTIONAL_COLUMN_IDS = [
    "contactReadiness",
    "opportunityStatus",
    "salesOpportunityType",
  ];

  var OPTIONAL_COLUMNS = [
    { id: "estimatedRoomNights", label: "Estimated Room Nights", sortKey: "estimatedRoomNights" },
    { id: "weeklyChange", label: "Weekly Change", sortKey: "weeklyChange" },
    { id: "marketScope", label: "Market Scope", sortKey: "marketScope" },
    { id: "needPeriodAlignment", label: "Need Period Alignment", sortKey: "needPeriodAlignment" },
    { id: "lastVerified", label: "Last Verified", sortKey: "lastVerified" },
  ];

  var OPTIONAL_COLUMN_SOURCES = {
    estimatedRoomNights:
      "estimatedRoomNights | peakRooms | roomNights | estimatedPeakRooms",
    weeklyChange: "weeklyDeltaState / isNewThisWeek (weeklyDeltaStateOf)",
    marketScope:
      "inventoryPopulations | hotelMarketScopeEligible | competitiveMarketScopeEligible | marketScope (Market Scope filter taxonomy)",
    needPeriodAlignment:
      "needDateIndicator.displayLabel | tileNeedDatePreview.displayLabel | needDateMatchV1.timingOverlapLabel",
    lastVerified: "lastVerifiedAt | lastVerified | evidenceAsOf",
  };

  var CONTACT_READINESS_RANK = {
    SALES_READY: 0,
    REACHABLE: 1,
    CONTACT_RESEARCH_REQUIRED: 2,
    CONTACT_IDENTIFICATION_REQUIRED: 3,
  };

  var CONTACT_READINESS_LABELS = {
    SALES_READY: "Sales-Ready Contact",
    REACHABLE: "Reachable Contact",
    CONTACT_RESEARCH_REQUIRED: "Contact Research Needed",
    CONTACT_IDENTIFICATION_REQUIRED: "Contact Not Identified",
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function text(v) {
    return String(v == null ? "" : v).trim();
  }

  function sanitizeOptionalColumnPrefs(map) {
    var next = {};
    var dirty = false;
    if (!map || typeof map !== "object") return { map: next, dirty: true };
    Object.keys(map).forEach(function (k) {
      if (RETIRED_OPTIONAL_COLUMN_IDS.indexOf(k) >= 0) {
        dirty = true;
        return;
      }
      if (OPTIONAL_COLUMNS.some(function (c) { return c.id === k; })) {
        next[k] = map[k] === true;
      } else {
        dirty = true;
      }
    });
    return { map: next, dirty: dirty };
  }

  function loadOptionalColumns() {
    try {
      var raw = sessionStorage.getItem(STORAGE_COLUMNS);
      if (!raw) return {};
      var parsed = JSON.parse(raw);
      var sanitized = sanitizeOptionalColumnPrefs(
        parsed && typeof parsed === "object" ? parsed : {},
      );
      if (sanitized.dirty) saveOptionalColumns(sanitized.map);
      return sanitized.map;
    } catch (e) {
      return {};
    }
  }

  function saveOptionalColumns(map) {
    try {
      var sanitized = sanitizeOptionalColumnPrefs(map || {});
      sessionStorage.setItem(
        STORAGE_COLUMNS,
        JSON.stringify(sanitized.map || {}),
      );
    } catch (e) {
      /* ignore */
    }
  }

  function visibleColumns(optionalMap, availableIds) {
    optionalMap = optionalMap || loadOptionalColumns();
    var cols = DEFAULT_COLUMNS.slice();
    var allow =
      availableIds && availableIds.length
        ? availableIds
        : OPTIONAL_COLUMNS.map(function (c) {
            return c.id;
          });
    OPTIONAL_COLUMNS.forEach(function (c) {
      if (optionalMap[c.id] === true && allow.indexOf(c.id) >= 0) {
        cols.splice(cols.length - 1, 0, c);
      }
    });
    return cols;
  }

  function monogramFromName(name) {
    var raw = text(name).replace(/[^A-Za-z0-9\s]/g, " ");
    var parts = raw.split(/\s+/).filter(Boolean);
    if (!parts.length) return "?";
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }

  function opportunityName(o) {
    return text(o.displayTitle || o.title || o.name) || "Untitled Opportunity";
  }

  function organizationContext(o) {
    return (
      text(o.organizationName) ||
      text(o.segment) ||
      text(o.segmentLabel) ||
      text(o.opportunityTypeLabel) ||
      ""
    );
  }

  function toPresentationProperCase(raw, UI) {
    var s = text(raw);
    if (!s) return s;
    if (UI && typeof UI.toProperCaseContactRoleLabel === "function") {
      try {
        return UI.toProperCaseContactRoleLabel(s) || s;
      } catch (e) {
        /* fall through */
      }
    }
    return s
      .split(/(\s+)/)
      .map(function (tok) {
        if (!/[A-Za-z]/.test(tok)) return tok;
        if (/^[A-Z]{2,8}$/.test(tok)) return tok;
        return tok.charAt(0).toUpperCase() + tok.slice(1).toLowerCase();
      })
      .join("");
  }

  function demandTypeLabel(o, UI) {
    var lab = "";
    // Prefer baked publication filter labels (same taxonomy as Opportunity Type filter).
    lab =
      text(o.opportunityTypeFilterLabel) ||
      text(o.segmentDisplayLabel) ||
      text(o.opportunityTypeLabel) ||
      "";
    if (!lab && UI && typeof UI.segmentDisplayLabel === "function") {
      try {
        lab = text(UI.segmentDisplayLabel(o));
      } catch (e) {
        lab = "";
      }
    }
    if (!lab && UI && typeof UI.classifyOpportunityTypeFilter === "function") {
      try {
        var cat = UI.classifyOpportunityTypeFilter(o);
        if (cat && UI.OPPORTUNITY_TYPE_FILTER_LABELS) {
          lab = text(UI.OPPORTUNITY_TYPE_FILTER_LABELS[cat]);
        }
      } catch (e2) {
        lab = "";
      }
    }
    if (!lab) {
      lab =
        text(o.segmentLabel) ||
        text(o.segment) ||
        text(o.demandSegment) ||
        "—";
    }
    if (lab === "—") return lab;
    return toPresentationProperCase(lab, UI);
  }

  function demandWindowDisplay(o, UI) {
    // Canonical hydrated timing first — keep List aligned with Tiles/Drawer.
    var baked = text(
      o.datePillDisplayLabel ||
        o.eventDateDisplay ||
        o.demandWindowLabel ||
        "",
    );
    if (baked) {
      if (/ongoing|continuous|recurring/i.test(baked)) {
        return toPresentationProperCase(baked, UI);
      }
      return baked;
    }
    if (UI && typeof UI.eventDateRangeLabel === "function") {
      try {
        var labeled = UI.eventDateRangeLabel(
          o.eventStartDate,
          o.eventEndDate,
          o.eventTiming,
        );
        if (labeled) return text(labeled);
      } catch (e) {
        /* fall through */
      }
    }
    var start = text(o.eventStartDate || o.tileEventStartDate);
    var end = text(o.eventEndDate || o.tileEventEndDate);
    if (start && end) return start + " → " + end;
    if (start) return start;
    var timing = text(o.eventTiming || o.demandTiming || o.demandWindowLabel);
    if (/ongoing|continuous|recurring/i.test(timing)) {
      return toPresentationProperCase(timing, UI);
    }
    if (timing) return timing;
    return "Dates TBD";
  }

  function demandWindowSortValue(o) {
    return text(o.eventStartDate || o.tileEventStartDate || "");
  }

  function salesActionMeta(o, UI) {
    var badge = o.tileBadgePlan || {};
    var canon = o.canonicalCustomerPresentation || null;
    var key = text(
      (canon && canon.commercialAction && canon.commercialAction.key) ||
        o.salesActionKey ||
        badge.salesActionKey ||
        "",
    );
    var label = text(
      (canon && canon.commercialAction && canon.commercialAction.pillLabel) ||
        o.salesActionPillLabel ||
        badge.salesActionPillLabel ||
        "",
    );
    if (!label && key) {
      label = key
        .replace(/_/g, " ")
        .toLowerCase()
        .replace(/\b\w/g, function (c) {
          return c.toUpperCase();
        });
    }
    if (!label) {
      var fallback =
        text(o.recommendedSalesAction) ||
        text(o.recommendedAction) ||
        "";
      // Never use long outreach prose as the Sales Action cell.
      if (fallback && fallback.length <= 40 && !/@/.test(fallback)) {
        label = fallback;
      }
    }
    if (!label) label = "—";
    else label = toPresentationProperCase(label, UI);
    return { key: key, label: label };
  }

  /**
   * Presentation-only: accept verified professional titles; reject department
   * labels, rationale blurbs, and evidence prose.
   */
  function resolveProfessionalTitle(raw, UI) {
    var s = text(raw);
    if (!s) return { title: "Title Not Verified", verified: false };

    s = s.replace(/^#+\s*/, "").replace(/\s+/g, " ").trim();
    var original = s;

    // Reject research rationale / unverified prose before clause cutting.
    if (
      /not stated|exact job title not|title not|role to confirm|start here to identify|ask to be directed|verified via|unconfirmed this pass|research rationale|sales approach|alternative entry|contact relevance/i.test(
        original,
      )
    ) {
      return { title: "Title Not Verified", verified: false };
    }
    if (
      /^published\b/i.test(original) &&
      /\bcontact\b/i.test(original) &&
      !/\b(director|manager|vp|president|officer|lead|specialist|coordinator|executive)\b/i.test(
        original,
      )
    ) {
      return { title: "Title Not Verified", verified: false };
    }
    // Strip markdown / LinkedIn headline noise prefixes
    if (/^judy\s+susser/i.test(s) || (s.indexOf(" - ") > 0 && s.length > 90)) {
      return { title: "Title Not Verified", verified: false };
    }

    // Prefer the first clause when evidence trails are appended.
    var cutters = [
      /\s+[—–-]\s+(?:authority|workplace|start here|ask to|hotel|lodging)/i,
      /\s*;\s*(?:subsequently|workplace|authority)/i,
      /,\s*per\s+/i,
      /\s+according to\b/i,
      /\s+professional profile\b/i,
      /\s+on\s+[^,]{2,60}'s\s+official\b/i,
      /\s+official (?:page|directory)\b/i,
      /\s+in the professional-contact record\b/i,
    ];
    for (var i = 0; i < cutters.length; i++) {
      var m = s.split(cutters[i]);
      if (m[0] && m[0].length >= 3 && m[0].length < s.length) {
        s = text(m[0]);
        break;
      }
    }

    // Drop trailing "; subsequently …" leftovers
    if (s.indexOf(";") > 0) {
      var first = text(s.split(";")[0]);
      if (first.length >= 3) s = first;
    }

    var lower = s.toLowerCase();
    var rejectExact = {
      events: 1,
      event: 1,
      gala: 1,
      membership: 1,
      sales: 1,
      marketing: 1,
      operations: 1,
      admin: 1,
      administration: 1,
      "membership events": 1,
      "gala / membership events": 1,
      "gala/membership events": 1,
      "referral contact": 1,
      "entry point": 1,
      "entry-point contact": 1,
      "alternative contact": 1,
      "commercial entry point": 1,
      "relevant commercial entry point": 1,
    };
    if (rejectExact[lower]) {
      return { title: "Title Not Verified", verified: false };
    }
    if (
      /\b(referral contact|entry[- ]point|alternative entry|commercial entry point)\b/i.test(
        s,
      )
    ) {
      return { title: "Title Not Verified", verified: false };
    }
    if (s.length > 90) {
      return { title: "Title Not Verified", verified: false };
    }
    // Require at least one title-like token (Director, Manager, VP, Lead, etc.)
    // OR a multi-word proper role that is not a single department noun.
    var titleLike =
      /\b(director|manager|vp|svp|evp|president|chief|officer|lead|head|specialist|coordinator|executive|consultant|engineer|analyst|supervisor|partner|principal|owner|founder|planner|agent)\b/i.test(
        s,
      );
    var multiWord = s.split(/\s+/).length >= 2;
    if (!titleLike && !multiWord) {
      return { title: "Title Not Verified", verified: false };
    }
    if (!titleLike && multiWord && /\/|events|gala|membership/i.test(s) && s.length < 40) {
      return { title: "Title Not Verified", verified: false };
    }

    return {
      title: toPresentationProperCase(s, UI),
      verified: true,
    };
  }

  function resolveListContact(o, UI) {
    var contact = null;
    if (UI && typeof UI.selectCompactTileContact === "function") {
      try {
        contact = UI.selectCompactTileContact(o, o) || null;
      } catch (e) {
        contact = null;
      }
    }
    if (!contact || !text(contact.name || contact.fullName)) {
      contact =
        (o.primaryContact && o.primaryContact.name && o.primaryContact) ||
        null;
    }
    return contact;
  }

  function whoContactMeta(o, UI) {
    var contact = resolveListContact(o, UI);
    var name = text(
      (contact && (contact.name || contact.fullName)) ||
        o.primaryContactName ||
        o.tileContactName ||
        o.contactName ||
        "",
    );
    var rawTitle =
      (contact && (contact.title || contact.role || contact.jobTitle)) ||
      o.primaryContactRole ||
      o.tileContactRoleLabel ||
      o.contactRole ||
      "";
    var titleInfo = resolveProfessionalTitle(rawTitle, UI);
    var buyerCode = text(
      o.buyerVsEntry ||
        (contact && contact.buyerVsEntry) ||
        (o.primaryContact && o.primaryContact.buyerVsEntry) ||
        "",
    );
    if (!name) {
      return {
        name: "Contact Not Yet Identified",
        role: "",
        title: "",
        sort: "",
        missing: true,
        buyerCode: buyerCode,
        entryPoint: buyerCode === "RELEVANT_COMMERCIAL_ENTRY_POINT",
      };
    }
    return {
      name: name,
      role: titleInfo.title,
      title: titleInfo.title,
      titleVerified: titleInfo.verified,
      sort: name.toLowerCase(),
      missing: false,
      buyerCode: buyerCode,
      entryPoint: buyerCode === "RELEVANT_COMMERCIAL_ENTRY_POINT",
      contact: contact,
    };
  }

  function isSwitchboardOrDepartmentPhone(contact, o) {
    var pc = (o && o.primaryContact) || {};
    var type = text(
      (contact &&
        (contact.phoneTypeLabel ||
          contact.phoneType ||
          contact.phoneClassification)) ||
        pc.phoneTypeLabel ||
        pc.phoneType ||
        pc.phoneClassification ||
        o.companyPhoneLabel ||
        "",
    ).toLowerCase();
    var status = text(
      (contact && contact.phoneVerificationStatus) ||
        pc.phoneVerificationStatus ||
        "",
    ).toUpperCase();
    if (
      /switchboard|department|main.?line|general.?office|reception|front.?desk|none/.test(
        type,
      )
    ) {
      return true;
    }
    if (status === "NO_PHONE") return true;
    if (
      type === "none" ||
      type === "general_switchboard" ||
      type === "department_phone" ||
      type === "company_phone" ||
      type === "main"
    ) {
      return true;
    }
    // Explicit companyPhone without person phone → treat as non-direct.
    if (!text(contact && contact.phone) && text(o.companyPhone)) return true;
    return false;
  }

  function resolveLinkedInUrl(contact, o) {
    var pc = (o && o.primaryContact) || {};
    var candidates = [
      contact && contact.linkedinUrl,
      contact && contact.linkedInUrl,
      contact && contact.sourceUrl,
      contact && contact.personSourceUrl,
      contact && contact.profileUrl,
      pc.linkedinUrl,
      pc.linkedInUrl,
      pc.sourceUrl,
      pc.personSourceUrl,
      pc.profileUrl,
      o && o.linkedinUrl,
    ];
    for (var i = 0; i < candidates.length; i += 1) {
      var u = text(candidates[i]);
      if (/^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\//i.test(u)) {
        return u.replace(/\/$/, "");
      }
    }
    return "";
  }

  function contactDetailsMeta(o, UI) {
    var who = whoContactMeta(o, UI);
    var contact = who.contact || resolveListContact(o, UI) || {};
    var email = text(
      contact.email ||
        o.contactEmail ||
        (who.missing ? "" : o.primaryContact && o.primaryContact.email) ||
        "",
    );
    var phoneRaw = text(
      contact.phone ||
        contact.directPhone ||
        o.contactPhone ||
        (who.missing ? "" : o.primaryContact && o.primaryContact.phone) ||
        "",
    );
    var phoneOk =
      phoneRaw && !isSwitchboardOrDepartmentPhone(contact, o) ? phoneRaw : "";
    var linkedinUrl = resolveLinkedInUrl(contact, o);
    var emailDisplay = email || "Email Not Available";
    var phoneDisplay = phoneOk || "Phone Not Available";
    var telHref = "";
    if (phoneOk) {
      var keepPlus = phoneOk.trim().charAt(0) === "+";
      var digits = phoneOk.replace(/[^\d]/g, "");
      telHref = "tel:" + (keepPlus ? "+" : "") + digits;
    }
    return {
      email: email,
      phone: phoneOk,
      linkedinUrl: linkedinUrl,
      emailDisplay: emailDisplay,
      phoneDisplay: phoneDisplay,
      emailHref: email ? "mailto:" + email : "",
      telHref: telHref,
      sort: (email || phoneOk || linkedinUrl || "").toLowerCase(),
      whoName: who.name,
    };
  }

  function contactReadinessMeta(o, UI) {
    var tier = text(
      o.contactActionabilityTier || o.contactActionabilityFilterKey || "",
    );
    var label =
      CONTACT_READINESS_LABELS[tier] ||
      text(o.contactActionabilityLabel) ||
      tier ||
      "—";
    if (label !== "—") label = toPresentationProperCase(label, UI);
    return {
      tier: tier,
      label: label,
      rank:
        CONTACT_READINESS_RANK[tier] != null
          ? CONTACT_READINESS_RANK[tier]
          : 9,
    };
  }

  function priorityMeta(o, UI) {
    var key = "";
    if (UI && typeof UI.normalizePriorityFilterKey === "function") {
      key = UI.normalizePriorityFilterKey(o.priorityFilterKey || o.priority);
    } else {
      key = text(o.priorityFilterKey || o.priority).toUpperCase();
      if (key === "HIGH") key = "HIGH_PRIORITY";
      if (key === "MEDIUM") key = "MEDIUM_PRIORITY";
      if (key === "WATCH") key = "WATCHLIST";
    }
    var label =
      key === "HIGH_PRIORITY"
        ? "High"
        : key === "MEDIUM_PRIORITY"
          ? "Medium"
          : key === "WATCHLIST"
            ? "Watch"
            : "Watch";
    return { key: key || "WATCHLIST", label: label };
  }

  function inventoryPopulationsOf(o, UI) {
    if (UI && typeof UI.opportunityInventoryPopulations === "function") {
      try {
        return UI.opportunityInventoryPopulations(o) || [];
      } catch (e) {
        /* fall through */
      }
    }
    o = o || {};
    if (Array.isArray(o.inventoryPopulations) && o.inventoryPopulations.length) {
      return o.inventoryPopulations.slice();
    }
    var pops = [];
    if (o.hotelMarketScopeEligible === true) pops.push("SALES_OPPORTUNITIES");
    if (
      o.competitiveMarketScopeEligible === true ||
      o.cmiVerified === true ||
      /^(VERIFIED|NEEDS_VERIFICATION|HISTORICAL)$/i.test(
        text(o.competitiveStatus),
      )
    ) {
      pops.push("COMPETITIVE_MARKET_INTELLIGENCE");
    }
    if (!pops.length && /competitive/i.test(text(o.marketScope))) {
      pops.push("COMPETITIVE_MARKET_INTELLIGENCE");
    }
    if (!pops.length) pops.push("SALES_OPPORTUNITIES");
    return pops;
  }

  function resolveMarketScopeDisplay(o, UI) {
    var pops = inventoryPopulationsOf(o, UI);
    var hotel =
      pops.indexOf("SALES_OPPORTUNITIES") >= 0 ||
      text(o.inventoryPopulationPrimary) === "SALES_OPPORTUNITIES" ||
      o.hotelMarketScopeEligible === true;
    var competitive =
      pops.indexOf("COMPETITIVE_MARKET_INTELLIGENCE") >= 0 ||
      text(o.inventoryPopulationPrimary) ===
        "COMPETITIVE_MARKET_INTELLIGENCE" ||
      o.competitiveMarketScopeEligible === true ||
      /^COMPETITIVE$/i.test(text(o.marketScope));
    if (hotel && competitive) return "Hotel · Competitive";
    if (competitive) return "Competitive";
    if (hotel) return "Hotel";
    var raw = text(o.marketScope || o.marketScopeLabel || o.population);
    if (!raw) return "";
    if (/^COMPETITIVE$/i.test(raw)) return "Competitive";
    if (/^HOTEL|SALES/i.test(raw)) return "Hotel";
    return toPresentationProperCase(raw.replace(/_/g, " "), UI);
  }

  function resolveWeeklyChangeDisplay(o, UI) {
    var state = "";
    if (UI && typeof UI.weeklyDeltaStateOf === "function") {
      try {
        state = text(UI.weeklyDeltaStateOf(o));
      } catch (e) {
        state = "";
      }
    }
    if (!state) {
      state =
        text(o.weeklyDeltaLabel) ||
        text(o.weeklyDeltaState) ||
        (o.isNewThisWeek === true ? "NEW" : "");
    }
    if (!state) return "";
    var map = {
      NEW: "New This Week",
      UPDATED: "Updated",
      REACTIVATED: "Reactivated",
      UNCHANGED: "Unchanged",
    };
    var key = state.toUpperCase().replace(/\s+/g, "_");
    if (map[key]) return map[key];
    return toPresentationProperCase(state.replace(/_/g, " "), UI);
  }

  function resolveNeedPeriodDisplay(o, UI) {
    var ind = o && o.needDateIndicator;
    var tile = o && o.tileNeedDatePreview;
    var match = o && o.needDateMatchV1;
    var label =
      text(ind && (ind.displayLabel || ind.accessibleLabel || ind.label)) ||
      text(tile && (tile.displayLabel || tile.accessibleLabel)) ||
      text(match && (match.timingOverlapLabel || match.classificationLine)) ||
      text(o && o.needDateRelevanceClass);
    if (!label) return "";
    return toPresentationProperCase(label, UI);
  }

  function resolveOpportunityStatusDisplay(o, UI) {
    var st = text(
      o.hotelSourcingStatusLabel ||
        o.opportunityStatusLabel ||
        o.commercialStagePillLabel ||
        o.statusLabel ||
        o.status ||
        "",
    );
    if (!st) return "";
    return toPresentationProperCase(st, UI);
  }

  function optionalCellValue(o, colId, UI) {
    switch (colId) {
      case "estimatedRoomNights": {
        var n =
          o.estimatedRoomNights ??
          o.peakRooms ??
          o.roomNights ??
          o.estimatedPeakRooms;
        if (n == null || n === "") return { display: "—", sort: null, populated: false };
        var num = Number(n);
        return {
          display: Number.isFinite(num) ? String(num) : text(n),
          sort: Number.isFinite(num) ? num : null,
          populated: true,
        };
      }
      case "weeklyChange": {
        var w = resolveWeeklyChangeDisplay(o, UI);
        return {
          display: w || "—",
          sort: w || "",
          populated: !!w,
        };
      }
      case "marketScope": {
        var m = resolveMarketScopeDisplay(o, UI);
        return {
          display: m || "—",
          sort: m.toLowerCase(),
          populated: !!m,
          isCompetitive: /competitive/i.test(m),
        };
      }
      case "needPeriodAlignment": {
        var nd = resolveNeedPeriodDisplay(o, UI);
        var aligned =
          o.needDateMatch === true ||
          o.needPeriodAligned === true ||
          (o.needDateMatchV1 && o.needDateMatchV1.confirmedNeedDateMatch === true) ||
          (o.needDateIndicator &&
            o.needDateIndicator.verifiedHotelNeedDateMatch === true);
        return {
          display: nd || "—",
          sort: aligned ? 0 : nd ? 1 : 2,
          populated: !!nd,
        };
      }
      case "opportunityStatus": {
        var st = resolveOpportunityStatusDisplay(o, UI);
        return {
          display: st || "—",
          sort: st.toLowerCase(),
          populated: !!st,
        };
      }
      case "lastVerified": {
        var lv = text(
          o.lastVerifiedAt || o.lastVerified || o.evidenceAsOf || "",
        );
        if (lv && lv.length > 10) lv = lv.slice(0, 10);
        return { display: lv || "—", sort: lv, populated: !!lv };
      }
      case "contactReadiness": {
        var cr = contactReadinessMeta(o, UI);
        return {
          display: cr.label || "—",
          sort: cr.rank,
          populated: !!(cr.tier || (cr.label && cr.label !== "—")),
        };
      }
      default:
        return { display: "—", sort: "", populated: false };
    }
  }

  function assessOptionalColumnCoverage(colId, inventoryRows, UI) {
    var rows = inventoryRows || [];
    var total = rows.length;
    var populated = 0;
    var competitive = 0;
    var distinct = Object.create(null);
    rows.forEach(function (o) {
      var cell = optionalCellValue(o, colId, UI);
      if (cell.populated) {
        populated += 1;
        distinct[String(cell.display || "").toLowerCase()] = 1;
        if (cell.isCompetitive) competitive += 1;
      }
    });
    var distinctCount = Object.keys(distinct).length;
    var coveragePct = total ? Math.round((populated / total) * 1000) / 10 : 0;
    var offer = populated > 0;
    var decision = "RETAIN";
    if (colId === "salesOpportunityType") {
      offer = false;
      decision = "CONSOLIDATED";
    } else if (!offer) {
      decision = "HIDDEN_NO_DATA";
    } else if (colId === "marketScope") {
      // Same canonical taxonomy as the Market Scope filter. Always offer when
      // rows resolve Hotel/Competitive so the optional column is never empty
      // while the filter classifies the opportunity.
      decision = competitive > 0 || distinctCount > 1
        ? "RETAIN_HYDRATED"
        : "RETAIN_HYDRATED_HOTEL_ONLY";
    } else if (coveragePct < 100) {
      decision = "RETAIN_HYDRATED";
    } else {
      decision = "RETAIN_HYDRATED";
    }
    return {
      id: colId,
      label: (OPTIONAL_COLUMNS.find(function (c) {
        return c.id === colId;
      }) || { label: colId }).label,
      canonicalSource: OPTIONAL_COLUMN_SOURCES[colId] || "",
      populated: populated,
      total: total,
      coveragePct: coveragePct,
      distinctCount: distinctCount,
      competitiveCount: competitive,
      offer: offer,
      decision: decision,
    };
  }

  function assessAllOptionalColumns(inventoryRows, UI) {
    return OPTIONAL_COLUMNS.map(function (c) {
      return assessOptionalColumnCoverage(c.id, inventoryRows, UI);
    });
  }

  function availableOptionalColumns(inventoryRows, UI) {
    return OPTIONAL_COLUMNS.filter(function (c) {
      return assessOptionalColumnCoverage(c.id, inventoryRows, UI).offer;
    });
  }

  function availableOptionalColumnIds(inventoryRows, UI) {
    return availableOptionalColumns(inventoryRows, UI).map(function (c) {
      return c.id;
    });
  }

  function verifiedLogoUrl(o) {
    if (o && o.organizationLogoVerified === true && text(o.organizationLogoUrl)) {
      return text(o.organizationLogoUrl);
    }
    return text(o && o.verifiedOrganizationLogoUrl);
  }

  function identityCellHtml(o) {
    var name = opportunityName(o);
    var secondary = organizationContext(o);
    var mono = monogramFromName(secondary || name);
    var logo = verifiedLogoUrl(o);
    var mark = logo
      ? '<div class="gdi-list-logo" aria-hidden="true"><img src="' +
        esc(logo) +
        '" alt="" loading="lazy" onerror="this.parentElement.classList.add(\'is-fallback\');this.remove();" /></div><div class="gdi-list-mono gdi-list-mono--fallback" aria-hidden="true">' +
        esc(mono) +
        "</div>"
      : '<div class="gdi-list-mono" aria-hidden="true">' + esc(mono) + "</div>";
    return (
      '<div class="gdi-list-identity">' +
      mark +
      '<div class="gdi-list-identity-copy">' +
      '<button type="button" class="gdi-list-opp-name" data-open="' +
      esc(o.id) +
      '" data-gdi-list-open="' +
      esc(o.id) +
      '">' +
      esc(name) +
      "</button>" +
      (secondary
        ? '<div class="gdi-list-opp-secondary">' + esc(secondary) + "</div>"
        : "") +
      "</div></div>"
    );
  }

  function whoCellHtml(o, UI) {
    var who = whoContactMeta(o, UI);
    return (
      '<div class="gdi-list-who' +
      (who.missing ? " is-missing" : "") +
      (who.entryPoint ? " is-entry-point" : "") +
      '"><div class="gdi-list-who-name">' +
      esc(who.name) +
      "</div>" +
      (who.role
        ? '<div class="gdi-list-who-role">' + esc(who.role) + "</div>"
        : "") +
      "</div>"
    );
  }

  function contactDetailsCellHtml(o, UI) {
    var d = contactDetailsMeta(o, UI);
    var emailLine = d.email
      ? '<a class="gdi-list-contact-link" href="' +
        esc(d.emailHref) +
        '">' +
        esc(d.email) +
        "</a>"
      : '<span class="gdi-list-contact-missing">' + esc(d.emailDisplay) + "</span>";
    var phoneLine = d.phone
      ? '<a class="gdi-list-contact-link" href="' +
        esc(d.telHref) +
        '">' +
        esc(d.phone) +
        "</a>"
      : '<span class="gdi-list-contact-missing">' + esc(d.phoneDisplay) + "</span>";
    var linkedinLine = d.linkedinUrl
      ? '<a class="gdi-list-contact-link gdi-list-linkedin" href="' +
        esc(d.linkedinUrl) +
        '" target="_blank" rel="noopener noreferrer" title="Open LinkedIn profile" aria-label="Open LinkedIn profile for ' +
        esc(d.whoName || "contact") +
        '">' +
        '<span class="gdi-list-contact-icon" aria-hidden="true">' +
        ICON_LINKEDIN +
        "</span>" +
        '<span class="gdi-list-linkedin-label">LinkedIn Profile</span></a>'
      : '<span class="gdi-list-contact-icon" aria-hidden="true">' +
        ICON_LINKEDIN +
        "</span>" +
        '<span class="gdi-list-contact-missing">LinkedIn Not Available</span>';
    return (
      '<div class="gdi-list-contact-details">' +
      '<div class="gdi-list-contact-line">' +
      '<span class="gdi-list-contact-icon" aria-hidden="true">' +
      ICON_EMAIL +
      "</span>" +
      emailLine +
      "</div>" +
      '<div class="gdi-list-contact-line">' +
      '<span class="gdi-list-contact-icon" aria-hidden="true">' +
      ICON_PHONE +
      "</span>" +
      phoneLine +
      "</div>" +
      '<div class="gdi-list-contact-line gdi-list-contact-linkedin">' +
      linkedinLine +
      "</div>" +
      "</div>"
    );
  }

  function listActionItems(o) {
    var items = [
      {
        act: "view",
        label: "View Details",
        title: "Open opportunity drawer",
      },
    ];
    if (o && o.pursuitId) {
      items.push({
        act: "pursuit-view",
        label: "View Pursuit",
        title: "Open existing pursuit",
        pursuitId: o.pursuitId,
      });
    } else if (o && o.canStartPursuit === true) {
      items.push({
        act: "pursuit-start",
        label: "Start Pursuit",
        title: "Start pursuit for this opportunity",
      });
    }
    return items;
  }

  function actionsCellHtml(o) {
    var items = listActionItems(o);
    var menuHtml = items
      .map(function (a) {
        return (
          '<button type="button" role="menuitem" class="gdi-list-more-menu__item" data-gdi-list-act="' +
          esc(a.act) +
          '" data-id="' +
          esc(o.id) +
          '"' +
          (a.pursuitId ? ' data-pursuit-id="' + esc(a.pursuitId) + '"' : "") +
          ' title="' +
          esc(a.title || a.label) +
          '">' +
          esc(a.label) +
          "</button>"
        );
      })
      .join("");
    return (
      '<div class="gdi-list-actions action-icons">' +
      '<button type="button" class="action-icon gdi-list-view-btn" data-open="' +
      esc(o.id) +
      '" data-gdi-list-open="' +
      esc(o.id) +
      '" title="View details" aria-label="View details for ' +
      esc(opportunityName(o)) +
      '">' +
      ICON_VIEW +
      "</button>" +
      '<button type="button" class="action-icon action-icon--more gdi-list-more-btn" data-gdi-list-more="' +
      esc(o.id) +
      '" title="More actions" aria-label="More actions for ' +
      esc(opportunityName(o)) +
      '" aria-haspopup="menu" aria-expanded="false">' +
      ICON_MORE +
      "</button>" +
      '<template class="gdi-list-more-template">' +
      menuHtml +
      "</template></div>"
    );
  }

  function headerCellHtml(col, sortKey, sortDir) {
    var colAttr = ' data-col="' + esc(col.id) + '"';
    if (col.noSort || !col.sortKey) {
      return (
        '<th class="no-sort" scope="col"' +
        colAttr +
        ">" +
        esc(col.label) +
        "</th>"
      );
    }
    var active = sortKey === col.sortKey;
    var cls =
      "gdi-list-th" +
      (active ? (sortDir === -1 ? " sort-desc" : " sort-asc") : "");
    return (
      '<th class="' +
      cls +
      '"' +
      colAttr +
      ' data-sort="' +
      esc(col.sortKey) +
      '" scope="col" tabindex="0" role="columnheader" aria-sort="' +
      (active ? (sortDir === -1 ? "descending" : "ascending") : "none") +
      '">' +
      esc(col.label) +
      SORT_INDICATOR +
      "</th>"
    );
  }

  function tdWrap(colId, inner) {
    return '<td data-col="' + esc(colId) + '">' + inner + "</td>";
  }

  function rowCellsHtml(o, cols, UI) {
    return cols
      .map(function (col) {
        switch (col.id) {
          case "opportunity":
            return tdWrap(col.id, identityCellHtml(o));
          case "priority": {
            var p = priorityMeta(o, UI);
            var pill =
              UI && typeof UI.priorityPill === "function"
                ? UI.priorityPill(p.key)
                : '<span class="gdi-pill">' + esc(p.label) + "</span>";
            return tdWrap(col.id, pill);
          }
          case "demandType":
            return tdWrap(col.id, esc(demandTypeLabel(o, UI)));
          case "demandWindow":
            return tdWrap(
              col.id,
              '<div class="gdi-list-date">' +
                esc(demandWindowDisplay(o, UI)) +
                "</div>",
            );
          case "salesAction": {
            var sa = salesActionMeta(o, UI);
            return tdWrap(
              col.id,
              '<span class="gdi-list-sales-action gdi-list-plain-text" data-sales-action="' +
                esc(sa.key) +
                '">' +
                esc(sa.label) +
                "</span>",
            );
          }
          case "whoToContact":
            return tdWrap(col.id, whoCellHtml(o, UI));
          case "contactDetails":
            return tdWrap(col.id, contactDetailsCellHtml(o, UI));
          case "actions":
            return tdWrap(col.id, actionsCellHtml(o));
          default: {
            var opt = optionalCellValue(o, col.id, UI);
            return tdWrap(col.id, esc(opt.display));
          }
        }
      })
      .join("");
  }

  var COLUMNS_ICON_SVG =
    '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="4" width="5" height="16" rx="1"/><rect x="10" y="4" width="5" height="16" rx="1"/><rect x="17" y="4" width="4" height="16" rx="1"/></svg>';

  function columnSettingsHtml(optionalMap, opts) {
    opts = opts || {};
    optionalMap = optionalMap || loadOptionalColumns();
    var UI = opts.UI || global.DealalityGdiUi;
    var inventoryRows = opts.inventoryRows;
    var available =
      inventoryRows && inventoryRows.length
        ? availableOptionalColumns(inventoryRows, UI)
        : OPTIONAL_COLUMNS.slice();
    var body =
      available.length > 0
        ? available
            .map(function (c) {
              var on = optionalMap[c.id] === true;
              return (
                '<label class="gdi-list-col-settings__row">' +
                '<input type="checkbox" data-gdi-list-column="' +
                esc(c.id) +
                '"' +
                (on ? " checked" : "") +
                " /> " +
                esc(c.label) +
                "</label>"
              );
            })
            .join("")
        : '<p class="gdi-list-col-settings__empty">No Optional Columns Have Populated Data For This Hotel.</p>';
    return (
      '<div class="gdi-list-col-settings" data-gdi-tour-target="columns-control">' +
      '<button type="button" class="gdi-list-col-settings__toggle sort-icon" id="gdiListColumnSettingsBtn" title="Customize Columns" aria-label="Customize Columns" aria-expanded="false" aria-controls="gdiListColumnSettingsPanel" aria-haspopup="true">' +
      COLUMNS_ICON_SVG +
      "</button>" +
      '<div class="gdi-list-col-settings__panel" id="gdiListColumnSettingsPanel" hidden role="menu">' +
      '<p class="gdi-list-col-settings__hint">Optional Columns (Populated For This Hotel)</p>' +
      body +
      "</div></div>"
    );
  }

  function tableHtml(rows, opts) {
    opts = opts || {};
    var UI = opts.UI || global.DealalityGdiUi;
    var sortKey = opts.sortKey || "priority";
    var sortDir = opts.sortDir == null ? 1 : opts.sortDir;
    var optionalMap = opts.optionalColumns || loadOptionalColumns();
    var inventoryRows =
      opts.inventoryRows ||
      opts.hotelInventoryRows ||
      rows ||
      [];
    var availableIds = availableOptionalColumnIds(inventoryRows, UI);
    var cols = visibleColumns(optionalMap, availableIds);
    var body =
      (rows || [])
        .map(function (o) {
          return (
            '<tr tabindex="0" data-id="' +
            esc(o.id) +
            '" data-open="' +
            esc(o.id) +
            '">' +
            rowCellsHtml(o, cols, UI) +
            "</tr>"
          );
        })
        .join("") ||
      '<tr><td colspan="' +
        cols.length +
        '"><div class="gdi-list-empty">No Opportunities Match These Filters.</div></td></tr>';

    return (
      '<div class="gdi-list-table-wrap" data-gdi-view-mode="list" data-gdi-tour-target="opportunity-list" data-gdi-contract="' +
      CONTRACT +
      '">' +
      '<div class="gdi-list-table-scroll">' +
      '<table class="gdi-commercial-table deals-table" id="gdiCommercialOppTable">' +
      "<thead><tr>" +
      cols
        .map(function (c) {
          return headerCellHtml(c, sortKey, sortDir);
        })
        .join("") +
      "</tr></thead>" +
      "<tbody>" +
      body +
      "</tbody></table></div></div>"
    );
  }

  function idTie(a, b) {
    return String((a && a.id) || "").localeCompare(String((b && b.id) || ""));
  }

  function compareForSortKey(a, b, key, dir, UI) {
    dir = dir == null ? 1 : dir;
    var cmp = null;
    if (key === "title") {
      cmp =
        opportunityName(a).localeCompare(opportunityName(b), undefined, {
          sensitivity: "base",
        }) * dir;
    } else if (key === "demandType") {
      cmp =
        demandTypeLabel(a, UI).localeCompare(demandTypeLabel(b, UI)) * dir;
    } else if (key === "salesAction") {
      cmp =
        salesActionMeta(a, UI).label.localeCompare(
          salesActionMeta(b, UI).label,
        ) * dir;
    } else if (key === "whoToContact") {
      var aw = whoContactMeta(a, UI);
      var bw = whoContactMeta(b, UI);
      if (aw.missing !== bw.missing) cmp = (aw.missing ? 1 : -1) * dir;
      else cmp = aw.sort.localeCompare(bw.sort) * dir;
    } else if (key === "contactDetails") {
      cmp =
        contactDetailsMeta(a, UI).sort.localeCompare(
          contactDetailsMeta(b, UI).sort,
        ) * dir;
    } else if (key === "contactReadiness") {
      var ar = contactReadinessMeta(a, UI).rank;
      var br = contactReadinessMeta(b, UI).rank;
      cmp = ar !== br ? (ar - br) * dir : 0;
    } else if (key === "estimatedRoomNights") {
      var an = optionalCellValue(a, "estimatedRoomNights").sort;
      var bn = optionalCellValue(b, "estimatedRoomNights").sort;
      if (an == null && bn == null) cmp = 0;
      else if (an == null) cmp = 1;
      else if (bn == null) cmp = -1;
      else cmp = (an - bn) * dir;
    } else if (
      key === "weeklyChange" ||
      key === "marketScope" ||
      key === "opportunityStatus" ||
      key === "lastVerified"
    ) {
      cmp =
        String(optionalCellValue(a, key).sort || "").localeCompare(
          String(optionalCellValue(b, key).sort || ""),
        ) * dir;
    } else if (key === "needPeriodAlignment") {
      var aa = optionalCellValue(a, key).sort;
      var bb = optionalCellValue(b, key).sort;
      cmp = (aa - bb) * dir;
    } else if (key === "eventStartDate") {
      var av = demandWindowSortValue(a);
      var bv = demandWindowSortValue(b);
      if (!av && !bv) cmp = 0;
      else if (!av) cmp = 1;
      else if (!bv) cmp = -1;
      else cmp = String(av).localeCompare(String(bv)) * dir;
    } else {
      return null;
    }
    if (cmp === 0) return idTie(a, b);
    return cmp;
  }

  var _moreMenuEl = null;
  var _moreMenuTrigger = null;

  function closeListMoreMenu() {
    if (_moreMenuEl && _moreMenuEl.parentNode) {
      _moreMenuEl.parentNode.removeChild(_moreMenuEl);
    }
    if (_moreMenuTrigger) {
      _moreMenuTrigger.setAttribute("aria-expanded", "false");
    }
    _moreMenuEl = null;
    _moreMenuTrigger = null;
  }

  function openListMoreMenu(trigger, handlers) {
    handlers = handlers || {};
    closeListMoreMenu();
    var tpl = trigger.parentNode && trigger.parentNode.querySelector(
      ".gdi-list-more-template",
    );
    if (!tpl) return;
    var menu = document.createElement("div");
    menu.className = "gdi-list-more-menu";
    menu.setAttribute("role", "menu");
    menu.innerHTML = tpl.innerHTML;
    document.body.appendChild(menu);
    _moreMenuEl = menu;
    _moreMenuTrigger = trigger;
    trigger.setAttribute("aria-expanded", "true");

    var rect = trigger.getBoundingClientRect();
    var mw = menu.offsetWidth || 200;
    var mh = menu.offsetHeight || 120;
    var left = Math.min(
      window.innerWidth - mw - 8,
      Math.max(8, rect.right - mw),
    );
    var top = rect.bottom + 6;
    if (top + mh > window.innerHeight - 8) {
      top = Math.max(8, rect.top - mh - 6);
    }
    menu.style.left = left + "px";
    menu.style.top = top + "px";

    menu.addEventListener("click", function (e) {
      var item = e.target.closest("[data-gdi-list-act]");
      if (!item) return;
      e.preventDefault();
      e.stopPropagation();
      var act = item.getAttribute("data-gdi-list-act");
      var id = item.getAttribute("data-id");
      var pursuitId = item.getAttribute("data-pursuit-id");
      closeListMoreMenu();
      if (act === "view" && typeof handlers.openDetail === "function") {
        handlers.openDetail(id);
      } else if (
        act === "pursuit-start" &&
        typeof handlers.startPursuit === "function"
      ) {
        handlers.startPursuit(id);
      } else if (
        act === "pursuit-view" &&
        typeof handlers.viewPursuit === "function"
      ) {
        handlers.viewPursuit(pursuitId, id);
      } else if (act === "view" && typeof handlers.openDetail !== "function") {
        // Fallback: reuse data-open path
        var fake = document.querySelector(
          '[data-gdi-list-open="' + id + '"]',
        );
        if (fake) fake.click();
      }
    });
  }

  function wireListTableInteractions(opts) {
    opts = opts || {};
    var root = opts.root;
    var state = opts.state;
    var render = opts.render;
    var persistBrowse = opts.persistBrowse;
    var openDetail = opts.openDetail;
    if (!root || !state || typeof render !== "function") return;

    root.querySelectorAll("#gdiCommercialOppTable th[data-sort]").forEach(
      function (th) {
        function activate() {
          var key = th.getAttribute("data-sort");
          if (!key) return;
          if (state.sortKey === key) state.sortDir = state.sortDir === 1 ? -1 : 1;
          else {
            state.sortKey = key;
            state.sortDir = 1;
          }
          var sortSel = document.getElementById("gdiSortSelect");
          if (sortSel) {
            var opt = Array.prototype.find.call(sortSel.options, function (o) {
              return o.value === key;
            });
            if (opt) sortSel.value = key;
          }
          if (typeof persistBrowse === "function") persistBrowse();
          render();
        }
        th.addEventListener("click", activate);
        th.addEventListener("keydown", function (e) {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            activate();
          }
        });
      },
    );

    root.querySelectorAll(".gdi-list-more-btn").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        if (_moreMenuTrigger === btn && _moreMenuEl) {
          closeListMoreMenu();
          return;
        }
        openListMoreMenu(btn, {
          openDetail: openDetail,
          startPursuit: function (id) {
            var startBtn = root.querySelector(
              '[data-pursuit-start="' + id + '"]',
            );
            if (startBtn) startBtn.click();
            else if (typeof opts.startPursuit === "function") {
              opts.startPursuit(id);
            }
          },
          viewPursuit: function (pursuitId, oppId) {
            if (typeof openDetail === "function" && oppId) openDetail(oppId);
            else if (typeof opts.viewPursuit === "function") {
              opts.viewPursuit(pursuitId, oppId);
            }
          },
        });
      });
    });

    if (!wireListTableInteractions._docCloseBound) {
      wireListTableInteractions._docCloseBound = true;
      document.addEventListener("click", function (e) {
        if (
          _moreMenuEl &&
          !e.target.closest(".gdi-list-more-menu") &&
          !e.target.closest(".gdi-list-more-btn")
        ) {
          closeListMoreMenu();
        }
      });
      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") closeListMoreMenu();
      });
      window.addEventListener("resize", closeListMoreMenu);
      window.addEventListener("scroll", closeListMoreMenu, true);
    }

    var toggle = document.getElementById("gdiListColumnSettingsBtn");
    var panel = document.getElementById("gdiListColumnSettingsPanel");
    if (toggle && panel) {
      toggle.addEventListener("click", function () {
        var open = panel.hasAttribute("hidden");
        if (open) panel.removeAttribute("hidden");
        else panel.setAttribute("hidden", "hidden");
        toggle.setAttribute("aria-expanded", open ? "true" : "false");
      });
      panel.querySelectorAll("[data-gdi-list-column]").forEach(function (input) {
        input.addEventListener("change", function () {
          var map = loadOptionalColumns();
          map[input.getAttribute("data-gdi-list-column")] = input.checked === true;
          saveOptionalColumns(map);
          render();
        });
      });
    }
  }

  function listTilesIdParity(rowsList, rowsTiles) {
    var a = (rowsList || []).map(function (o) {
      return String(o.id || "");
    });
    var b = (rowsTiles || []).map(function (o) {
      return String(o.id || "");
    });
    var missing = a.filter(function (id) {
      return b.indexOf(id) < 0;
    });
    var unexpected = b.filter(function (id) {
      return a.indexOf(id) < 0;
    });
    return {
      ok: missing.length === 0 && unexpected.length === 0 && a.length === b.length,
      listCount: a.length,
      tilesCount: b.length,
      missingFromTiles: missing,
      unexpectedInTiles: unexpected,
    };
  }

  global.DealalityGdiCommercialListTable = {
    CONTRACT: CONTRACT,
    DEFAULT_COLUMNS: DEFAULT_COLUMNS,
    OPTIONAL_COLUMNS: OPTIONAL_COLUMNS,
    OPTIONAL_COLUMN_SOURCES: OPTIONAL_COLUMN_SOURCES,
    RETIRED_OPTIONAL_COLUMN_IDS: RETIRED_OPTIONAL_COLUMN_IDS,
    sanitizeOptionalColumnPrefs: sanitizeOptionalColumnPrefs,
    tableHtml: tableHtml,
    columnSettingsHtml: columnSettingsHtml,
    visibleColumns: visibleColumns,
    loadOptionalColumns: loadOptionalColumns,
    saveOptionalColumns: saveOptionalColumns,
    compareForSortKey: compareForSortKey,
    wireListTableInteractions: wireListTableInteractions,
    listTilesIdParity: listTilesIdParity,
    CONTACT_READINESS_RANK: CONTACT_READINESS_RANK,
    resolveProfessionalTitle: resolveProfessionalTitle,
    whoContactMeta: whoContactMeta,
    contactDetailsMeta: contactDetailsMeta,
    contactDetailsCellHtml: contactDetailsCellHtml,
    salesActionMeta: salesActionMeta,
    optionalCellValue: optionalCellValue,
    assessOptionalColumnCoverage: assessOptionalColumnCoverage,
    assessAllOptionalColumns: assessAllOptionalColumns,
    availableOptionalColumns: availableOptionalColumns,
    availableOptionalColumnIds: availableOptionalColumnIds,
    resolveMarketScopeDisplay: resolveMarketScopeDisplay,
  };
})(typeof window !== "undefined" ? window : globalThis);
