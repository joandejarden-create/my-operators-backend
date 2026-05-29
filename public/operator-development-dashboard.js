/**
 * My Operator Deals — Phase 4 (workspace table, deal-meta, row actions).
 */
(function () {
  "use strict";

  var TAB_IDS = [
    "odd-new",
    "odd-active-review",
    "odd-awaiting-info",
    "odd-nda-room",
    "odd-terms-proposal",
    "odd-advanced",
    "odd-archived",
    "odd-deal-log",
  ];

  var TAB_COUNT_MAP = {
    "odd-new": "tabCountOddNew",
    "odd-active-review": "tabCountOddActiveReview",
    "odd-awaiting-info": "tabCountOddAwaitingInfo",
    "odd-nda-room": "tabCountOddNda",
    "odd-terms-proposal": "tabCountOddTerms",
    "odd-advanced": "tabCountOddAdvanced",
    "odd-archived": "tabCountOddArchived",
  };

  var BUCKET_BY_TAB = {
    "odd-new": "new",
    "odd-active-review": "active-review",
    "odd-awaiting-info": "awaiting-info",
    "odd-nda-room": "nda-room",
    "odd-terms-proposal": "terms-proposal",
    "odd-advanced": "advanced",
    "odd-archived": "archived",
  };

  var MAPPING_MESSAGES = {
    no_operator_link: "Your operator company is not connected yet.",
    names_unresolved: "Your operator company profile is linked but the operating company name could not be resolved.",
    lookup_error: "We could not load your operator company mapping. Try again or contact support.",
    no_user_record: "Your user account is not fully linked in Dealality.",
  };

  var DEMO_BYPASS_WARNING = "operator_deals_demo_bypass_role";

  var state = {
    currentTab: "odd-new",
    requests: [],
    activityEntries: [],
    dealMetaById: Object.create(null),
    meta: null,
    me: null,
    selectedCompany: "",
    showMultiCompany: false,
    filterStatus: "",
    filterAlignment: "",
    sortColumn: null,
    sortDirection: "asc",
    loading: true,
    error: null,
    modal: null,
    patching: false,
    moreMenuEl: null,
  };

  var STAGE_LABELS = {
    new: "Intake",
    "active-review": "Operator review",
    "awaiting-info": "Engaged",
    "nda-room": "NDA / shared workspace",
    "terms-proposal": "Terms review",
    advanced: "Advanced",
    archived: "Closed",
  };

  function $(id) {
    return document.getElementById(id);
  }

  function setHidden(el, hidden) {
    if (!el) return;
    if (hidden) el.setAttribute("hidden", "");
    else el.removeAttribute("hidden");
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function apiBase() {
    return window.location.origin || "";
  }

  async function authFetch(path, options) {
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(apiBase() + path, Object.assign({ maxWaitMs: 20000 }, options || {}));
    }
    return fetch(apiBase() + path, options || {});
  }

  function formatDateShort(iso) {
    if (!iso) return "—";
    var d = new Date(String(iso));
    if (Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
  }

  function formatFollowUpDisplay(row) {
    var d = row.nextFollowupDate;
    var h = row.nextFollowupHeader;
    if (!d && !h) return "—";
    var dateStr = d ? String(d).slice(0, 10) : "";
    return [dateStr, h].filter(Boolean).join(" · ") || "—";
  }

  function formatLastActivityDisplay(row) {
    var ms = row.lastActivitySort;
    if (ms == null) return "—";
    try {
      return new Date(ms).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
    } catch (_e) {
      return "—";
    }
  }

  function deriveStageLabel(row) {
    return STAGE_LABELS[row.workspaceBucket] || "—";
  }

  function getScoreClass(score) {
    var n = Number(score);
    if (Number.isNaN(n)) return "match-score-poor";
    if (n >= 80) return "match-score-high";
    if (n >= 50) return "match-score-medium";
    if (n >= 25) return "match-score-weak";
    return "match-score-poor";
  }

  function effectiveAlignmentScore(row) {
    if (row.alignmentScore == null || row.alignmentScore === "") return null;
    var n = Number(row.alignmentScore);
    return Number.isNaN(n) ? null : n;
  }

  function isStalledRow(row) {
    var P = window.DealWorkspacePipeline;
    if (P && P.isStalledRow) return P.isStalledRow(row);
    return false;
  }

  function derivePriorityBadges(row) {
    var out = [];
    var st = String(row._requestStatus || row.status || "").trim();
    var score = effectiveAlignmentScore(row);
    if (st === "New" || st === "Sent / Awaiting Response" || !st) out.push({ key: "new", label: "New" });
    if (["New", "Operator Viewed", "Viewed", "Sent / Awaiting Response"].includes(st)) {
      out.push({ key: "needs", label: "Needs Response" });
    }
    if (st === "More Info Requested") out.push({ key: "miss", label: "Info requested" });
    if (["Accepted", "Responded - Accepted"].includes(st) && row.workspaceBucket === "awaiting-info") {
      out.push({ key: "owner", label: "Owner Waiting" });
    }
    if (row.workspaceBucket === "awaiting-info" && st !== "More Info Requested") {
      out.push({ key: "miss", label: "Missing Info" });
    }
    if (isStalledRow(row)) out.push({ key: "overdue", label: "Overdue" });
    if (score != null && score >= 80) out.push({ key: "fit", label: "High Fit" });
    if (st === "Revisit Later") out.push({ key: "revisit", label: "Revisit Later" });
    var seen = new Set();
    return out.filter(function (b) {
      if (seen.has(b.label)) return false;
      seen.add(b.label);
      return true;
    });
  }

  function getWhySurfacedHeadline(row) {
    var notes = String(row.ownerNotes || "").trim();
    if (notes) return notes.length > 120 ? notes.slice(0, 120) + "…" : notes;
    var meta = state.dealMetaById[row.dealId] || {};
    if (meta.dealType) return meta.dealType;
    if (meta.locationLine) return meta.locationLine;
    return "Owner operating request — review alignment signals before responding.";
  }

  function displayStatusLabel(status) {
    var s = String(status || "New").trim();
    if (!s || s === "Sent / Awaiting Response") return "New";
    return s;
  }

  function statusBadgeSlug(status) {
    var label = displayStatusLabel(status);
    return String(label).toLowerCase().replace(/\s+/g, "-").replace(/[^\w-]/g, "");
  }

  function resolveRowDealMeta(row) {
    if (row && row.dealMeta && typeof row.dealMeta === "object") return row.dealMeta;
    return state.dealMetaById[row.dealId] || {};
  }

  function enrichRow(row) {
    var P = window.DealWorkspacePipeline;
    if (!P || !P.enrichWorkspaceRow) return row;
    var enriched = P.enrichWorkspaceRow(row);
    enriched._requestStatus = String(row.status || enriched._requestStatus || "").trim();
    enriched._nextStep = P.deriveOperatorNextAction ? P.deriveOperatorNextAction(enriched) : "—";
    var meta = resolveRowDealMeta(row);
    enriched.dealMeta = meta;
    enriched._dealTitle = meta.title || meta.projectName || null;
    enriched._market = meta.locationLine || meta.country || null;
    enriched._ownerCompany = meta.ownerCompany || null;
    enriched.propertyName =
      meta.projectName || meta.title || (row.dealId ? "Deal " + row.dealId.slice(0, 8) + "…" : "—");
    enriched.country = meta.country || null;
    enriched.rooms = meta.rooms != null && meta.rooms !== "" ? meta.rooms : null;
    enriched.stageLabel = deriveStageLabel(enriched);
    enriched.priorityBadges = derivePriorityBadges(enriched);
    enriched.lastActivityDisplay = formatLastActivityDisplay(enriched);
    enriched.followUpDisplay = formatFollowUpDisplay(enriched);
    return enriched;
  }

  function getEmptyStateHtml(tabId, hasAnyRequests, hasVisibleInTab) {
    if (tabId === "odd-deal-log") return "";
    var mappingMsg = state.meta && state.meta.message;
    if (mappingMsg) {
      return (
        "<h3>Operator company not connected</h3>" +
        "<p>" + esc(mappingMsg) + "</p>" +
        "<p>Ask your Dealality admin to link your user to <strong>Operator Setup - Master</strong> with an active submission status.</p>"
      );
    }
    if (hasAnyRequests && !hasVisibleInTab) {
      if (state.filterStatus || state.filterAlignment) {
        return "<h3>No rows match your filters</h3><p>Try clearing status or alignment signal filters.</p>";
      }
      return "<h3>No opportunities in this stage</h3><p>Try another pipeline tab.</p>";
    }

    var base =
      "<h3>No inbound operating opportunities yet</h3>" +
      "<p>Rows appear here when an owner contacts or selects your operating company through Dealality.</p>" +
      "<p>On the owner side, this typically starts in <strong>My Deals</strong> → <strong>Operator Strategy</strong>.</p>";

    var byTab = {
      "odd-new": base,
      "odd-active-review": "<h3>No deals in active operator review</h3><p>Mark an owner request as viewed to move it into review considerations.</p>",
      "odd-awaiting-info": "<h3>No deals waiting on owner information</h3><p>Use <strong>Request info</strong> when you need clarification from the owner.</p>",
      "odd-nda-room": "<h3>No deals in this stage</h3><p>Confidentiality and shared workspace steps are planned for a later phase.</p>",
      "odd-terms-proposal": "<h3>No deals in terms review</h3><p>Terms review workflow is planned for a later phase.</p>",
      "odd-advanced": "<h3>No finalist or advanced deals</h3><p>Advanced pipeline stages populate as requests progress.</p>",
      "odd-archived": "<h3>No declined or archived deals</h3><p>Declines and archives are kept for record-keeping.</p>",
    };
    return byTab[tabId] || "<h3>No opportunities in this stage</h3><p>Try another tab.</p>";
  }

  function alignmentBandMatches(row, filterVal) {
    if (!filterVal) return true;
    var band = String(row.alignmentBand || "").toLowerCase();
    return band.indexOf(String(filterVal).toLowerCase()) !== -1;
  }

  function getVisibleRows() {
    var bucket = BUCKET_BY_TAB[state.currentTab];
    if (!bucket) return [];
    var P = window.DealWorkspacePipeline;
    return state.requests
      .map(enrichRow)
      .filter(function (row) {
        if (!P || !P.enrichWorkspaceRow) return false;
        if (P.enrichWorkspaceRow(row).workspaceBucket !== bucket) return false;
        if (state.filterStatus && String(row.status || "") !== state.filterStatus) return false;
        if (!alignmentBandMatches(row, state.filterAlignment)) return false;
        if (state.showMultiCompany && state.selectedCompany) {
          if (String(row.operatingCompanyName || "").trim() !== state.selectedCompany) return false;
        }
        return true;
      });
  }

  function countRowsForTab(tabId) {
    var bucket = BUCKET_BY_TAB[tabId];
    if (!bucket || !state.requests.length) return 0;
    var P = window.DealWorkspacePipeline;
    if (!P || !P.enrichWorkspaceRow) return 0;
    return state.requests.filter(function (r) {
      return P.enrichWorkspaceRow(r).workspaceBucket === bucket;
    }).length;
  }

  function updateTabCounts() {
    Object.keys(TAB_COUNT_MAP).forEach(function (tabId) {
      var el = $(TAB_COUNT_MAP[tabId]);
      if (el) el.textContent = String(countRowsForTab(tabId));
    });
  }

  function getWorkspaceTableColspan() {
    return state.currentTab === "odd-new" ? 13 : 12;
  }

  function isNewLayoutTab() {
    return state.currentTab === "odd-new";
  }

  function sortTh(sortKey, label) {
    return (
      '<th data-odd-sort="' +
      sortKey +
      '"><span style="display:inline-flex;align-items:center;gap:4px;"><span>' +
      esc(label) +
      '</span><span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>'
    );
  }

  function ensureWorkspaceTableHeader() {
    var thead = $("bddWorkspaceThead");
    var cg = $("dealsTableColgroup");
    var table = $("dealsTable");
    if (!thead || !cg || !table) return;
    var isNew = isNewLayoutTab();
    table.classList.toggle("bdd-workspace-table--new-layout", isNew);
    if (isNew) {
      cg.innerHTML = "<col class=\"bdd-col-check\"><col><col><col><col><col><col><col><col><col><col><col><col>";
      thead.innerHTML =
        "<tr>" +
        '<th class="cell-checkbox no-sort"><input type="checkbox" id="oddSelectAllCheckbox" title="Select all"></th>' +
        '<th class="no-sort">Your decision</th>' +
        sortTh("headline", "Why this surfaced") +
        '<th class="no-sort">Alerts</th>' +
        sortTh("propertyName", "Opportunity") +
        sortTh("ownerCompany", "Owner company") +
        sortTh("country", "Country") +
        sortTh("rooms", "Rooms") +
        sortTh("stageLabel", "Stage") +
        sortTh("status", "Status") +
        sortTh("lastActivity", "Last activity") +
        sortTh("followUp", "Follow-up") +
        '<th class="no-sort cell-call-to-action"><span>More actions</span></th>' +
        "</tr>";
    } else {
      cg.innerHTML = "<col class=\"bdd-col-check\"><col><col><col><col><col><col><col><col><col><col><col>";
      thead.innerHTML =
        "<tr>" +
        '<th class="cell-checkbox no-sort"><input type="checkbox" id="oddSelectAllCheckbox" title="Select all"></th>' +
        '<th class="no-sort">Alerts</th>' +
        sortTh("propertyName", "Opportunity") +
        sortTh("ownerCompany", "Owner company") +
        sortTh("country", "Country") +
        sortTh("rooms", "Rooms") +
        sortTh("alignmentScore", "Alignment fit") +
        sortTh("stageLabel", "Stage") +
        sortTh("status", "Status") +
        sortTh("lastActivity", "Last activity") +
        sortTh("followUp", "Follow-up") +
        '<th class="no-sort cell-call-to-action"><span>More actions</span></th>' +
        "</tr>";
    }
    refreshSortHeaderClasses();
  }

  function refreshSortHeaderClasses() {
    if (!state.sortColumn) return;
    document.querySelectorAll("#dealsTable th[data-odd-sort]").forEach(function (th) {
      th.classList.remove("sort-asc", "sort-desc");
      if (th.getAttribute("data-odd-sort") === state.sortColumn) {
        th.classList.add(state.sortDirection === "asc" ? "sort-asc" : "sort-desc");
      }
    });
  }

  function compareSortValues(aVal, bVal) {
    if (aVal == null || aVal === "") aVal = "";
    if (bVal == null || bVal === "") bVal = "";
    if (typeof aVal === "number" && typeof bVal === "number") return aVal - bVal;
    return String(aVal).localeCompare(String(bVal), undefined, { numeric: true, sensitivity: "base" });
  }

  function sortVisibleRows(rows) {
    if (!state.sortColumn) return rows;
    var col = state.sortColumn;
    var dir = state.sortDirection === "desc" ? -1 : 1;
    return rows.slice().sort(function (a, b) {
      var aVal;
      var bVal;
      switch (col) {
        case "headline":
          aVal = getWhySurfacedHeadline(a).toLowerCase();
          bVal = getWhySurfacedHeadline(b).toLowerCase();
          break;
        case "propertyName":
          aVal = (a.propertyName || "").toLowerCase();
          bVal = (b.propertyName || "").toLowerCase();
          break;
        case "ownerCompany":
          aVal = (a._ownerCompany || "").toLowerCase();
          bVal = (b._ownerCompany || "").toLowerCase();
          break;
        case "country":
          aVal = (a.country || "").toLowerCase();
          bVal = (b.country || "").toLowerCase();
          break;
        case "rooms":
          aVal = Number(a.rooms);
          bVal = Number(b.rooms);
          if (Number.isNaN(aVal)) aVal = -1;
          if (Number.isNaN(bVal)) bVal = -1;
          break;
        case "alignmentScore":
          aVal = effectiveAlignmentScore(a);
          bVal = effectiveAlignmentScore(b);
          if (aVal == null) aVal = -1;
          if (bVal == null) bVal = -1;
          break;
        case "stageLabel":
          aVal = (a.stageLabel || "").toLowerCase();
          bVal = (b.stageLabel || "").toLowerCase();
          break;
        case "status":
          aVal = displayStatusLabel(a._requestStatus).toLowerCase();
          bVal = displayStatusLabel(b._requestStatus).toLowerCase();
          break;
        case "lastActivity":
          aVal = a.lastActivitySort != null ? a.lastActivitySort : 0;
          bVal = b.lastActivitySort != null ? b.lastActivitySort : 0;
          break;
        case "followUp":
          aVal = (a.nextFollowupDate || "").toString();
          bVal = (b.nextFollowupDate || "").toString();
          break;
        default:
          return 0;
      }
      return compareSortValues(aVal, bVal) * dir;
    });
  }

  function renderStatusCell(status) {
    var s = displayStatusLabel(status);
    if (s === "Operator Viewed" || s === "Viewed" || s === "Brand Viewed") {
      return '<span class="status-text-plain">' + esc(String(status || s)) + "</span>";
    }
    var slug = statusBadgeSlug(status);
    return '<span class="bdd-status-badge bdd-status-' + esc(slug) + '">' + esc(s) + "</span>";
  }

  function renderNewOpportunityDecisionCell(row) {
    var st = String(row._requestStatus || "").trim();
    if (["Declined", "Archived", "Responded - Declined"].includes(st)) {
      return '<div class="bdd-decision-stack"><span class="bdd-pill-muted" style="border:none;background:transparent;padding:0;">Closed</span></div>';
    }
    var requestId = esc(row.id || "");
    return (
      '<div class="bdd-decision-stack">' +
      '<button type="button" class="bdd-decision-btn bdd-decision-btn--primary" data-odd-action="interested" data-request-id="' +
      requestId +
      '">Interested</button>' +
      '<button type="button" class="bdd-decision-btn bdd-decision-btn--secondary" data-odd-action="requestInfo" data-request-id="' +
      requestId +
      '">Request Info</button>' +
      '<button type="button" class="bdd-decision-btn bdd-decision-btn--ghost" data-odd-action="decline" data-request-id="' +
      requestId +
      '">Decline</button>' +
      "</div>"
    );
  }

  function renderWhySurfacedCell(row) {
    var score = effectiveAlignmentScore(row);
    var hasScore = score != null && !Number.isNaN(score);
    var scoreClass = hasScore ? getScoreClass(score) : "match-score-empty";
    var headline = esc(getWhySurfacedHeadline(row));
    var detailsBtn = hasScore
      ? '<button type="button" class="match-score-new-details-btn" data-odd-alignment-details="' +
        esc(row.id || "") +
        '">Alignment Details</button>'
      : "";
    return (
      '<div class="bdd-why-cell-inner">' +
      '<p class="bdd-why-headline">' +
      headline +
      "</p>" +
      '<div class="bdd-why-fit-row">' +
      '<span class="match-score-badge ' +
      scoreClass +
      '">' +
      (hasScore ? score.toFixed(1) : "—") +
      "</span>" +
      detailsBtn +
      "</div></div>"
    );
  }

  function renderBadgesCell(row) {
    var badges = (row.priorityBadges || [])
      .map(function (b) {
        return '<span class="bdd-pill bdd-pill--' + esc(b.key) + '">' + esc(b.label) + "</span>";
      })
      .join(" ");
    return badges || '<span class="bdd-pill-muted">—</span>';
  }

  function renderAlignmentFitCell(row) {
    var score = effectiveAlignmentScore(row);
    var hasScore = score != null && !Number.isNaN(score);
    var scoreClass = hasScore ? getScoreClass(score) : "match-score-empty";
    var detailsBtn = hasScore
      ? '<button type="button" class="match-score-new-details-btn" data-odd-alignment-details="' +
        esc(row.id || "") +
        '">Alignment Details</button>'
      : "";
    return (
      '<div class="match-score-cell">' +
      '<span class="match-score-badge ' +
      scoreClass +
      '">' +
      (hasScore ? score.toFixed(1) : "—") +
      "</span>" +
      detailsBtn +
      "</div>"
    );
  }

  function renderWorkspaceCallToActionRow(row) {
    var requestId = esc(row.id || "");
    var st = String(row._requestStatus || "").trim();
    var svgEye =
      '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>';
    var svgCal =
      '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>';
    var svgDoc =
      '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>';
    var svgMail =
      '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg>';
    var svgMore =
      '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="12" cy="19" r="1"/></svg>';
    var termsActive = row.workspaceBucket === "terms-proposal" || st === "Pre-LOI" || st === "Pre-LOI / Term Comparison";
    var parts = [];
    parts.push(
      '<button type="button" class="action-icon" title="Opportunity workspace" aria-label="Opportunity workspace" data-odd-action="open" data-request-id="' +
        requestId +
        '">' +
        svgEye +
        "</button>"
    );
    parts.push(
      '<button type="button" class="action-icon" title="Owner request" aria-label="Owner request" data-action="communications" data-request-id="' +
        requestId +
        '">' +
        svgMail +
        "</button>"
    );
    parts.push(
      '<button type="button" class="action-icon" title="Schedule follow-up" aria-label="Schedule follow-up" data-action="schedule" data-request-id="' +
        requestId +
        '">' +
        svgCal +
        "</button>"
    );
    parts.push(
      '<button type="button" class="action-icon action-icon--proposal" title="' +
        (termsActive ? "Terms review notes" : "Terms review (available in Terms tab)") +
        '" aria-label="Terms review notes" data-odd-action="prepareTerms" data-request-id="' +
        requestId +
        '"' +
        (termsActive ? "" : " disabled") +
        ">" +
        svgDoc +
        "</button>"
    );
    parts.push(
      '<button type="button" class="action-icon" title="More actions" aria-label="More actions" data-action="odd-ws-more" data-request-id="' +
        requestId +
        '">' +
        svgMore +
        "</button>"
    );
    return '<div class="action-icons bdd-ws-cta-icons" title="Suggested actions update the operator request and activity log.">' + parts.join("") + "</div>";
  }

  function renderWorkspaceTableRow(row, isNewLayout) {
    var requestId = esc(row.id || "");
    var badgesHtml = renderBadgesCell(row);
    if (isNewLayout) {
      return (
        "<tr data-request-id=\"" +
        requestId +
        '">' +
        '<td class="cell-checkbox"><input type="checkbox" class="deal-row-checkbox" data-request-id="' +
        requestId +
        '" title="Select row"></td>' +
        '<td class="bdd-decision-cell">' +
        renderNewOpportunityDecisionCell(row) +
        "</td>" +
        '<td class="bdd-why-cell">' +
        renderWhySurfacedCell(row) +
        "</td>" +
        '<td class="bdd-badges-cell">' +
        badgesHtml +
        "</td>" +
        '<td><span class="property-name">' +
        esc(row.propertyName || "—") +
        "</span></td>" +
        "<td>" +
        esc(row._ownerCompany || "—") +
        "</td>" +
        "<td>" +
        esc(row.country || "—") +
        "</td>" +
        "<td>" +
        esc(String(row.rooms != null ? row.rooms : "—")) +
        "</td>" +
        "<td>" +
        esc(row.stageLabel || "—") +
        "</td>" +
        "<td>" +
        renderStatusCell(row._requestStatus) +
        "</td>" +
        '<td style="font-size:12px;color:var(--neutral--400);">' +
        esc(row.lastActivityDisplay || "—") +
        "</td>" +
        '<td style="font-size:12px;color:var(--neutral--400);max-width:120px;">' +
        esc(row.followUpDisplay || "—") +
        "</td>" +
        '<td class="cell-call-to-action bdd-ws-actions-col">' +
        renderWorkspaceCallToActionRow(row) +
        "</td>" +
        "</tr>"
      );
    }
    return (
      "<tr data-request-id=\"" +
      requestId +
      '">' +
      '<td class="cell-checkbox"><input type="checkbox" class="deal-row-checkbox" data-request-id="' +
      requestId +
      '" title="Select row"></td>' +
      '<td class="bdd-badges-cell">' +
      badgesHtml +
      "</td>" +
      '<td><span class="property-name">' +
      esc(row.propertyName || "—") +
      "</span></td>" +
      "<td>" +
      esc(row._ownerCompany || "—") +
      "</td>" +
      "<td>" +
      esc(row.country || "—") +
      "</td>" +
      "<td>" +
      esc(String(row.rooms != null ? row.rooms : "—")) +
      "</td>" +
      "<td>" +
      renderAlignmentFitCell(row) +
      "</td>" +
      "<td>" +
      esc(row.stageLabel || "—") +
      "</td>" +
      "<td>" +
      renderStatusCell(row._requestStatus) +
      "</td>" +
      '<td style="font-size:12px;color:var(--neutral--400);">' +
      esc(row.lastActivityDisplay || "—") +
      "</td>" +
      '<td style="font-size:12px;color:var(--neutral--400);max-width:120px;">' +
      esc(row.followUpDisplay || "—") +
      "</td>" +
      '<td class="cell-call-to-action bdd-ws-actions-col">' +
      renderWorkspaceCallToActionRow(row) +
      "</td>" +
      "</tr>"
    );
  }

  function renderWorkspaceTable() {
    var tbody = $("dealsTableBody");
    var table = $("dealsTable");
    var emptyEl = $("oddWorkspaceEmpty");
    var loadingEl = $("oddTableLoading");
    if (!tbody) return;

    if (loadingEl) loadingEl.style.display = state.loading ? "" : "none";
    if (state.loading) {
      if (table) table.style.display = "none";
      if (emptyEl) emptyEl.style.display = "none";
      return;
    }

    ensureWorkspaceTableHeader();
    var visible = sortVisibleRows(getVisibleRows());
    var hasAny = state.requests.length > 0;
    var isNewLayout = isNewLayoutTab();

    if (visible.length > 0) {
      if (table) table.style.display = "";
      if (emptyEl) emptyEl.style.display = "none";
      tbody.innerHTML = visible.map(function (row) {
        return renderWorkspaceTableRow(row, isNewLayout);
      }).join("");
    } else {
      if (table) table.style.display = "none";
      if (emptyEl) {
        emptyEl.style.display = "";
        emptyEl.className = "empty-state";
        emptyEl.innerHTML = getEmptyStateHtml(state.currentTab, hasAny, false);
      }
    }

    var countEl = $("oddResultsCount");
    if (countEl) {
      if (!hasAny) {
        countEl.textContent = "No operating opportunities connected yet.";
      } else {
        countEl.textContent =
          visible.length +
          " in this view · " +
          state.requests.length +
          " total operating opportunit" +
          (state.requests.length === 1 ? "y" : "ies");
      }
    }
  }

  function switchTab(tabId) {
    if (TAB_IDS.indexOf(tabId) === -1) tabId = "odd-new";
    state.currentTab = tabId;
    state.sortColumn = null;
    state.sortDirection = "asc";
    closeOddMoreMenu();

    document.querySelectorAll(".bdd-section-nav .section-nav-item").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("data-tab") === tabId);
    });

    var isLog = tabId === "odd-deal-log";
    $("sectionOddWorkspace").classList.toggle("active", !isLog);
    $("sectionOddDealLog").classList.toggle("active", isLog);

    if (!isLog) renderWorkspaceTable();

    if (window.location.hash !== "#" + tabId) {
      try {
        history.replaceState(null, "", "#" + tabId);
      } catch (_e) {
        window.location.hash = tabId;
      }
    }
  }

  function restoreTabFromHash() {
    var hash = (window.location.hash || "").replace(/^#/, "");
    if (TAB_IDS.indexOf(hash) !== -1) switchTab(hash);
  }

  function wireTabNav() {
    document.querySelectorAll(".bdd-section-nav .section-nav-item").forEach(function (btn) {
      btn.addEventListener("click", function () {
        switchTab(btn.getAttribute("data-tab") || "odd-new");
      });
    });
    window.addEventListener("hashchange", restoreTabFromHash);
  }

  function showPhaseBanner(meta, meMeta) {
    var banner = $("oddPhaseBanner");
    var text = $("oddPhaseBannerText");
    if (!banner || !text) return;

    var warnings = (meMeta && meMeta.warnings) || (meta && meta.warnings) || [];
    var mappingStatus = (meta && meta.mappingStatus) || (meMeta && meMeta.operatorMappingStatus);

    if (warnings.indexOf(DEMO_BYPASS_WARNING) !== -1) {
      text.textContent =
        "Demo preview: showing sample operator deals for your walkthrough. Role checks are temporarily relaxed in this environment.";
      setHidden(banner, false);
      return;
    }

    if (meta && meta.tableConfigured === false) {
      text.textContent = meta.message || "Operator Deal Requests table is not configured yet.";
      setHidden(banner, false);
      return;
    }

    if (mappingStatus && mappingStatus !== "ok" && mappingStatus !== "admin_unrestricted") {
      text.textContent = (meta && meta.message) || MAPPING_MESSAGES[mappingStatus] || MAPPING_MESSAGES.no_operator_link;
      setHidden(banner, false);
      return;
    }

    if (warnings.length) {
      text.textContent = "Operator mapping loaded with warnings: " + warnings.join(", ");
      setHidden(banner, false);
      return;
    }

    setHidden(banner, true);
  }

  function showError(message) {
    var el = $("oddErrorState");
    if (!el) return;
    if (!message) {
      setHidden(el, true);
      el.textContent = "";
      return;
    }
    el.textContent = message;
    setHidden(el, false);
  }

  function populateStatusFilter() {
    var sel = $("oddStatusFilter");
    if (!sel) return;
    var current = state.filterStatus;
    var statuses = [];
    state.requests.forEach(function (r) {
      var s = String(r.status || "New").trim();
      if (s && statuses.indexOf(s) === -1) statuses.push(s);
    });
    statuses.sort();
    sel.innerHTML = '<option value="">All statuses</option>';
    statuses.forEach(function (s) {
      var opt = document.createElement("option");
      opt.value = s;
      opt.textContent = s;
      sel.appendChild(opt);
    });
    sel.value = current;
  }

  function wireFilters() {
    var statusSel = $("oddStatusFilter");
    var alignSel = $("oddAlignmentFilter");
    var resetBtn = $("oddResetViewBtn");
    var companySel = $("oddCompanyFilter");

    if (statusSel) {
      statusSel.addEventListener("change", function () {
        state.filterStatus = statusSel.value || "";
        renderWorkspaceTable();
      });
    }
    if (alignSel) {
      alignSel.addEventListener("change", function () {
        state.filterAlignment = alignSel.value || "";
        renderWorkspaceTable();
      });
    }
    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        state.filterStatus = "";
        state.filterAlignment = "";
        if (statusSel) statusSel.value = "";
        if (alignSel) alignSel.value = "";
        if (companySel && state.showMultiCompany) {
          companySel.value = "";
          state.selectedCompany = "";
          loadOperatorDealRequests();
          return;
        }
        renderWorkspaceTable();
      });
    }
    if (companySel) {
      companySel.addEventListener("change", function () {
        state.selectedCompany = companySel.value || "";
        loadOperatorDealRequests();
      });
    }
  }

  function populateCompanyFilter(meData) {
    var group = $("oddCompanyFilterGroup");
    var sel = $("oddCompanyFilter");
    if (!group || !sel || !meData) return;

    var perms = meData.permissions || {};
    var names = Array.isArray(perms.allowedOperatingCompanyNames) ? perms.allowedOperatingCompanyNames.slice() : [];
    var isAdmin = meData.dealality && meData.dealality.isAdmin;

    sel.innerHTML = "";
    if (isAdmin) {
      setHidden(group, true);
      state.selectedCompany = "";
      state.showMultiCompany = false;
      return;
    }

    if (names.length <= 1) {
      setHidden(group, true);
      state.selectedCompany = names[0] || perms.primaryOperatingCompanyName || "";
      state.showMultiCompany = false;
      return;
    }

    var defaultOpt = document.createElement("option");
    defaultOpt.value = "";
    defaultOpt.textContent = "All linked companies";
    sel.appendChild(defaultOpt);

    names.forEach(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      sel.appendChild(opt);
    });

    state.selectedCompany = "";
    state.showMultiCompany = true;
    setHidden(group, false);
  }

  async function loadMeContext() {
    var res = await authFetch("/api/me");
    if (res.status === 401) throw new Error("Sign in to view My Operator Deals.");
    if (!res.ok) throw new Error("Could not load account permissions (" + res.status + ").");
    var data = await res.json();
    if (!data.success) throw new Error(data.message || "Account permissions unavailable.");
    state.me = data;
    populateCompanyFilter(data);
    return data;
  }

  function buildListQuery(meData) {
    var params = [];
    var isAdmin = meData && meData.dealality && meData.dealality.isAdmin;
    if (!isAdmin && state.selectedCompany) {
      params.push("operator=" + encodeURIComponent(state.selectedCompany));
    }
    return params.length ? "?" + params.join("&") : "";
  }

  function buildDealMetaQuery(dealIds) {
    var q = "ids=" + encodeURIComponent(dealIds.join(","));
    var isAdmin = state.me && state.me.dealality && state.me.dealality.isAdmin;
    if (!isAdmin && state.selectedCompany) {
      q += "&operator=" + encodeURIComponent(state.selectedCompany);
    }
    return q;
  }

  async function loadDealMeta() {
    var ids = [];
    state.requests.forEach(function (r) {
      if (r.dealId && ids.indexOf(r.dealId) === -1) ids.push(r.dealId);
    });
    ids = ids.slice(0, 40);
    if (!ids.length) return;

    var needsFetch = ids.some(function (id) {
      var row = state.requests.find(function (r) { return r.dealId === id; });
      var meta = row && row.dealMeta;
      return !meta || (!meta.country && !meta.ownerCompany && meta.rooms == null);
    });
    if (!needsFetch) {
      state.requests.forEach(function (r) {
        if (r.dealId && r.dealMeta) state.dealMetaById[r.dealId] = r.dealMeta;
      });
      return;
    }

    try {
      var res = await authFetch("/api/operator-deal-requests/deal-meta?" + buildDealMetaQuery(ids));
      if (!res.ok) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[operator-development-dashboard] deal-meta HTTP", res.status);
        }
        return;
      }
      var data = await res.json();
      (data.deals || []).forEach(function (d) {
        if (d.dealId) state.dealMetaById[d.dealId] = d;
      });
      state.requests.forEach(function (r, idx) {
        if (r.dealId && state.dealMetaById[r.dealId]) {
          state.requests[idx] = Object.assign({}, r, { dealMeta: state.dealMetaById[r.dealId] });
        }
      });
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[operator-development-dashboard] deal-meta failed:", err.message || err);
      }
    }
  }

  async function refreshKpiStrip() {
    if (!window.DealWorkspaceInsights || !window.DealWorkspacePipeline) return;
    await window.DealWorkspaceInsights.update({
      persona: "operator",
      rows: state.requests,
      filterParts: {},
      Pipeline: window.DealWorkspacePipeline,
      insightsElId: "oddKpiInsights",
      pipelineElId: "oddKpiPipeline",
      stuckPopoverId: "workspaceKpiStuckPopover",
      flowTitleElId: "workspaceInsightsFlowTitle",
      pipelineTitleElId: "workspaceInsightsPipelineTitle",
    });
  }

  async function loadActivityLog(meData) {
    if (!state.requests.length) {
      state.activityEntries = [];
      return;
    }
    var dealIds = state.requests.map(function (r) { return r.dealId; }).filter(Boolean);
    if (!dealIds.length) return;

    var q = "dealIds=" + encodeURIComponent(dealIds.slice(0, 40).join(","));
    var isAdmin = meData && meData.dealality && meData.dealality.isAdmin;
    if (!isAdmin && state.selectedCompany) {
      q += "&operator=" + encodeURIComponent(state.selectedCompany);
    }

    try {
      var res = await authFetch("/api/operator-deal-requests/activity?" + q);
      if (!res.ok) return;
      var data = await res.json();
      state.activityEntries = Array.isArray(data.entries) ? data.entries : [];
    } catch (_e) {
      /* non-blocking */
    }
  }

  function renderActivityPlaceholder() {
    var empty = $("oddActivityEmpty");
    var content = $("oddActivityContent");
    if (!empty) return;
    if (!state.activityEntries.length) {
      empty.style.display = "";
      empty.textContent = "No activity entries yet for your scoped operating companies.";
      if (content) {
        content.hidden = true;
        content.innerHTML = "";
      }
      return;
    }
    empty.style.display = "none";
    if (!content) return;
    content.hidden = false;
    var html =
      '<table class="bdd-activity-table"><thead><tr><th>When</th><th>Action</th><th>Deal</th><th>Details</th></tr></thead><tbody>';
    state.activityEntries.slice(0, 50).forEach(function (entry) {
      var dealLabel = entry.dealName || (entry.dealId ? entry.dealId.slice(0, 10) + "…" : "—");
      html +=
        "<tr><td>" + esc(formatDateShort(entry.createdAt)) + "</td>" +
        "<td>" + esc(entry.action || "—") + "</td>" +
        "<td>" + esc(dealLabel) + "</td>" +
        "<td>" + esc(entry.details || "") + "</td></tr>";
    });
    html += "</tbody></table>";
    content.innerHTML = html;
  }

  function findRequestById(id) {
    return state.requests.find(function (r) { return r.id === id; });
  }

  function getEnrichedRowByRequestId(requestId) {
    var row = findRequestById(requestId);
    return row ? enrichRow(row) : null;
  }

  function showOddToast(message, ok) {
    var el = $("oddToast");
    if (!el) {
      el = document.createElement("div");
      el.id = "oddToast";
      el.className = "bdd-toast";
      el.setAttribute("role", "status");
      document.body.appendChild(el);
    }
    el.textContent = message || "";
    el.style.display = "block";
    el.style.borderColor = ok === false ? "rgba(255,90,101,0.5)" : "rgba(255,255,255,0.15)";
    el.style.transform = "translateX(0)";
    el.style.opacity = "1";
    clearTimeout(showOddToast._t);
    showOddToast._t = setTimeout(function () {
      el.style.transform = "translateX(400px)";
      el.style.opacity = "0";
      setTimeout(function () { el.style.display = "none"; }, 300);
    }, 3200);
  }

  function closeOddMoreMenu() {
    if (state.moreMenuEl && state.moreMenuEl.parentNode) {
      state.moreMenuEl.parentNode.removeChild(state.moreMenuEl);
    }
    state.moreMenuEl = null;
  }

  function getAvailableOperatorWorkspaceActions(row) {
    var st = String(row._requestStatus || row.status || "").trim();
    var out = [];
    var seen = new Set();
    function push(id, label) {
      if (seen.has(id)) return;
      seen.add(id);
      out.push({ id: id, label: label });
    }

    push("open", "Opportunity workspace");
    if (["Declined", "Archived", "Responded - Declined"].includes(st)) {
      push("editNotes", "Notes & follow-up");
      return out;
    }
    if (["New", "Sent / Awaiting Response"].includes(st) || !st) {
      push("viewed", "Mark viewed");
    }
    if (!["Declined", "Archived", "Responded - Declined"].includes(st)) {
      push("interested", "Interested");
      push("requestInfo", "Request info");
      push("revisit", "Revisit later");
      push("decline", "Decline");
    }
    if (row.workspaceBucket === "terms-proposal" || st === "Pre-LOI" || st === "Pre-LOI / Term Comparison") {
      push("prepareTerms", "Terms review notes");
    }
    push("editNotes", "Notes & follow-up");
    push("ownerRequest", "Owner request");
    return out;
  }

  function showOddWorkspaceMoreMenu(anchorBtn) {
    closeOddMoreMenu();
    if (!anchorBtn) return;
    var requestId = anchorBtn.getAttribute("data-request-id");
    var row = getEnrichedRowByRequestId(requestId);
    if (!row || !requestId) return;

    var stripIds = new Set(["open", "ownerRequest", "editNotes", "prepareTerms"]);
    var menu = document.createElement("div");
    menu.className = "bdd-ws-more-menu";
    var rect = anchorBtn.getBoundingClientRect();
    menu.style.top = Math.min(window.innerHeight - 220, rect.bottom + 6) + "px";
    menu.style.left = Math.min(window.innerWidth - 220, Math.max(8, rect.left - 160)) + "px";

    getAvailableOperatorWorkspaceActions(row)
      .filter(function (a) { return !stripIds.has(a.id); })
      .forEach(function (a) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "bdd-ws-more-menu__item";
        btn.textContent = a.label;
        btn.addEventListener("click", function (e) {
          e.preventDefault();
          e.stopPropagation();
          closeOddMoreMenu();
          executeOperatorWorkspaceAction(a.id, requestId);
        });
        menu.appendChild(btn);
      });

    document.body.appendChild(menu);
    state.moreMenuEl = menu;
    setTimeout(function () {
      function dismiss(evt) {
        if (!state.moreMenuEl) return;
        if (state.moreMenuEl.contains(evt.target) || evt.target === anchorBtn) return;
        closeOddMoreMenu();
        document.removeEventListener("click", dismiss, true);
      }
      document.addEventListener("click", dismiss, true);
    }, 0);
  }

  async function executeOperatorWorkspaceAction(action, requestId) {
    if (!requestId) return;
    if (action === "open") {
      openOpportunityWorkspaceModal(requestId);
      return;
    }
    if (action === "ownerRequest") {
      openOwnerRequestModal(requestId);
      return;
    }
    if (action === "viewed") {
      await patchRequest(requestId, { status: "Operator Viewed" });
      showOddToast("Marked as viewed.", true);
      return;
    }
    if (action === "interested") {
      await patchRequest(requestId, { status: "Accepted" });
      showOddToast("Marked interested.", true);
      return;
    }
    if (action === "requestInfo") {
      openRequestInfoModal(requestId);
      return;
    }
    if (action === "decline") {
      openDeclineModal(requestId);
      return;
    }
    if (action === "revisit") {
      await patchRequest(requestId, { status: "Revisit Later" });
      showOddToast("Moved to revisit later.", true);
      return;
    }
    if (action === "editNotes") {
      openNotesModal(requestId);
      return;
    }
    if (action === "prepareTerms") {
      openNotesModal(requestId);
      showOddToast("Add terms notes in the follow-up panel.", true);
      return;
    }
  }

  function openOwnerRequestModal(requestId) {
    var row = getEnrichedRowByRequestId(requestId);
    if (!row) return;
    var notes = String(row.ownerNotes || "").trim();
    openModal({
      title: "Owner request",
      saveLabel: "Close",
      bodyHtml:
        "<p><strong>Owner company:</strong> " + esc(row._ownerCompany || "—") + "</p>" +
        "<p><strong>Opportunity:</strong> " + esc(row.propertyName || "—") + "</p>" +
        (notes
          ? "<p style=\"margin-top:12px;white-space:pre-wrap;\">" + esc(notes) + "</p>"
          : "<p style=\"margin-top:12px;color:var(--neutral--500);\">No owner message on file for this request.</p>"),
      onSave: function () {
        closeModal();
        return Promise.resolve();
      },
    });
  }

  function openOpportunityWorkspaceModal(requestId) {
    var row = getEnrichedRowByRequestId(requestId);
    if (!row) return;
    var score = effectiveAlignmentScore(row);
    var scoreStr = score != null ? score.toFixed(1) : "—";
    openModal({
      title: "Opportunity workspace",
      saveLabel: "Close",
      bodyHtml:
        "<p><strong>" + esc(row.propertyName || "Operating opportunity") + "</strong></p>" +
        "<p style=\"color:var(--neutral--400);font-size:13px;margin-top:4px;\">" +
        esc([row._ownerCompany, row.country, row.rooms != null ? row.rooms + " keys" : ""].filter(Boolean).join(" · ") || "—") +
        "</p>" +
        "<p style=\"margin-top:14px;\"><strong>Stage:</strong> " + esc(row.stageLabel || "—") +
        " · <strong>Status:</strong> " + esc(displayStatusLabel(row._requestStatus)) + "</p>" +
        "<p><strong>Alignment:</strong> " + esc(scoreStr) + " · " + esc(row.alignmentBand || "—") + "</p>" +
        "<p><strong>Next step:</strong> " + esc(row._nextStep || "—") + "</p>" +
        "<p style=\"margin-top:12px;font-size:13px;color:var(--neutral--500);\">Use row actions or the More menu to record review steps — each update is logged to activity.</p>",
      onSave: function () {
        closeModal();
        return Promise.resolve();
      },
    });
  }

  function replaceRequest(updated) {
    if (!updated || !updated.id) return;
    var idx = state.requests.findIndex(function (r) { return r.id === updated.id; });
    if (idx >= 0) {
      var prev = state.requests[idx];
      state.requests[idx] = Object.assign({}, updated, {
        dealMeta: updated.dealMeta || prev.dealMeta || null,
      });
    }
  }

  async function patchRequest(requestId, body) {
    if (state.patching) return null;
    state.patching = true;
    try {
      var res = await authFetch("/api/operator-deal-requests/" + encodeURIComponent(requestId), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok || !data.success) {
        showError(data.message || data.error || "Update failed (" + res.status + ").");
        return null;
      }
      replaceRequest(data.request);
      updateTabCounts();
      await refreshKpiStrip();
      await loadActivityLog(state.me);
      renderActivityPlaceholder();
      renderWorkspaceTable();
      showError(null);
      return data.request;
    } catch (err) {
      showError(err.message || "Could not update request.");
      return null;
    } finally {
      state.patching = false;
    }
  }

  function closeModal() {
    var backdrop = $("oddModalBackdrop");
    if (backdrop) {
      backdrop.classList.remove("active");
      backdrop.setAttribute("hidden", "");
    }
    state.modal = null;
  }

  function openModal(config) {
    state.modal = config;
    var backdrop = $("oddModalBackdrop");
    var title = $("oddModalTitle");
    var body = $("oddModalBody");
    var saveBtn = $("oddModalSaveBtn");
    if (!backdrop || !title || !body) return;
    title.textContent = config.title || "Notes";
    body.innerHTML = config.bodyHtml || "";
    if (saveBtn) saveBtn.textContent = config.saveLabel || "Save";
    backdrop.removeAttribute("hidden");
    backdrop.classList.add("active");
  }

  function wireModal() {
    var closeBtn = $("oddModalCloseBtn");
    var cancelBtn = $("oddModalCancelBtn");
    var saveBtn = $("oddModalSaveBtn");
    var backdrop = $("oddModalBackdrop");

    function onClose() { closeModal(); }
    if (closeBtn) closeBtn.addEventListener("click", onClose);
    if (cancelBtn) cancelBtn.addEventListener("click", onClose);
    if (backdrop) {
      backdrop.addEventListener("click", function (e) {
        if (e.target === backdrop) onClose();
      });
    }
    if (saveBtn) {
      saveBtn.addEventListener("click", async function () {
        if (!state.modal || typeof state.modal.onSave !== "function") return;
        saveBtn.disabled = true;
        try {
          await state.modal.onSave();
        } finally {
          saveBtn.disabled = false;
        }
      });
    }
  }

  function openRequestInfoModal(requestId) {
    openModal({
      title: "Request more info",
      bodyHtml:
        '<label>Message for the owner<textarea id="oddModalResponseNotes" placeholder="What review considerations or data gaps should the owner clarify?"></textarea></label>',
      onSave: async function () {
        var notes = ($("oddModalResponseNotes") && $("oddModalResponseNotes").value) || "";
        var ok = await patchRequest(requestId, {
          status: "More Info Requested",
          responseNotes: notes.trim(),
        });
        if (ok) closeModal();
      },
    });
  }

  function openDeclineModal(requestId) {
    openModal({
      title: "Decline opportunity",
      bodyHtml:
        '<label>Notes (optional)<textarea id="oddModalDeclineNotes" placeholder="Brief reason for your team records"></textarea></label>',
      onSave: async function () {
        var notes = ($("oddModalDeclineNotes") && $("oddModalDeclineNotes").value) || "";
        var ok = await patchRequest(requestId, {
          status: "Declined",
          responseNotes: notes.trim(),
        });
        if (ok) closeModal();
      },
    });
  }

  function openNotesModal(requestId) {
    var row = findRequestById(requestId);
    if (!row) return;
    openModal({
      title: "Notes & follow-up",
      bodyHtml:
        '<label>Internal notes<textarea id="oddModalInternalNotes">' + esc(row.operatorInternalNotes || "") + "</textarea></label>" +
        '<label>Follow-up date<input type="date" id="oddModalFollowupDate" value="' + esc((row.nextFollowupDate || "").slice(0, 10)) + '"></label>' +
        '<label>Follow-up header<input type="text" id="oddModalFollowupHeader" value="' + esc(row.nextFollowupHeader || "") + '"></label>' +
        '<label>Follow-up notes<textarea id="oddModalFollowupNotes">' + esc(row.nextFollowupNotes || "") + "</textarea></label>",
      onSave: async function () {
        var ok = await patchRequest(requestId, {
          operatorInternalNotes: ($("oddModalInternalNotes") && $("oddModalInternalNotes").value) || "",
          nextFollowupDate: ($("oddModalFollowupDate") && $("oddModalFollowupDate").value) || null,
          nextFollowupHeader: ($("oddModalFollowupHeader") && $("oddModalFollowupHeader").value) || "",
          nextFollowupNotes: ($("oddModalFollowupNotes") && $("oddModalFollowupNotes").value) || "",
          scheduledBy: "operator",
        });
        if (ok) closeModal();
      },
    });
  }

  function openAlignmentDetailsModal(requestId) {
    var row = findRequestById(requestId);
    if (!row) return;
    var score = effectiveAlignmentScore(row);
    var scoreStr = score != null ? score.toFixed(1) : "—";
    openModal({
      title: "Alignment signals",
      bodyHtml:
        "<p><strong>Alignment score:</strong> " +
        esc(scoreStr) +
        "</p>" +
        "<p><strong>Alignment signal:</strong> " +
        esc(row.alignmentBand || "—") +
        "</p>" +
        "<p><strong>Data confidence:</strong> " +
        esc(row.dataConfidence || "—") +
        "</p>" +
        (String(row.ownerNotes || "").trim()
          ? "<p><strong>Owner request:</strong> " + esc(row.ownerNotes) + "</p>"
          : "") +
        "<p style=\"margin-top:12px;font-size:13px;color:var(--neutral--500);\">Scores highlight fit signals and data gaps — they do not select an operator on the owner&apos;s behalf.</p>",
      onSave: function () {
        closeModal();
        return Promise.resolve();
      },
    });
    var saveBtn = $("oddModalSaveBtn");
    if (saveBtn) saveBtn.textContent = "Close";
  }

  function wireTableActions() {
    var table = $("dealsTable");
    if (table && !table._oddSortWired) {
      table._oddSortWired = true;
      table.addEventListener("click", function (e) {
        var th = e.target.closest("th[data-odd-sort]");
        if (!th) return;
        var col = th.getAttribute("data-odd-sort");
        if (!col) return;
        if (state.sortColumn === col) {
          state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
        } else {
          state.sortColumn = col;
          state.sortDirection = "asc";
        }
        renderWorkspaceTable();
      });
    }

    if (document._oddWorkspaceActionsWired) return;
    document._oddWorkspaceActionsWired = true;

    document.addEventListener("click", function (e) {
      if (!$("dealsTable") || !e.target.closest("#dealsTable")) return;

      var moreBtn = e.target.closest('[data-action="odd-ws-more"]');
      if (moreBtn) {
        e.preventDefault();
        e.stopPropagation();
        showOddWorkspaceMoreMenu(moreBtn);
        return;
      }

      var scheduleBtn = e.target.closest('.action-icon[data-action="schedule"]');
      if (scheduleBtn && !scheduleBtn.disabled) {
        e.preventDefault();
        e.stopPropagation();
        var schedId = scheduleBtn.getAttribute("data-request-id");
        if (schedId) openNotesModal(schedId);
        return;
      }

      var commBtn = e.target.closest('.action-icon[data-action="communications"]');
      if (commBtn && !commBtn.disabled) {
        e.preventDefault();
        e.stopPropagation();
        var commId = commBtn.getAttribute("data-request-id");
        if (commId) openOwnerRequestModal(commId);
        return;
      }

      var alignBtn = e.target.closest("[data-odd-alignment-details]");
      if (alignBtn) {
        e.preventDefault();
        e.stopPropagation();
        var alignId = alignBtn.getAttribute("data-odd-alignment-details");
        if (alignId) openAlignmentDetailsModal(alignId);
        return;
      }

      var btn = e.target.closest("[data-odd-action]");
      if (!btn || btn.disabled) return;
      e.preventDefault();
      e.stopPropagation();
      var action = btn.getAttribute("data-odd-action");
      var requestId = btn.getAttribute("data-request-id");
      if (!action || !requestId) return;
      executeOperatorWorkspaceAction(action, requestId);
    });
  }

  async function loadOperatorDealRequests() {
    state.loading = true;
    renderWorkspaceTable();
    showError(null);
    var meData = state.me;

    try {
      var res = await authFetch("/api/operator-deal-requests" + buildListQuery(meData));
      if (res.status === 403) {
        showError("My Operator Deals is not available for this account type.");
        state.requests = [];
        state.meta = null;
        return;
      }
      if (res.status === 503) {
        var errBody = await res.json().catch(function () { return {}; });
        state.requests = [];
        state.meta = { tableConfigured: false, message: errBody.message || "Operator Deal Requests is not configured." };
        showPhaseBanner(state.meta, meData && meData.meta);
        return;
      }
      if (!res.ok) throw new Error("API responded " + res.status);

      var data = await res.json();
      state.requests = Array.isArray(data.requests) ? data.requests : [];
      state.meta = data.meta || null;
      showPhaseBanner(state.meta, meData && meData.meta);

      await loadDealMeta();
      populateStatusFilter();
      await loadActivityLog(meData);
      renderActivityPlaceholder();

      var countEl = $("oddResultsCount");
      if (countEl) {
        if (state.meta && state.meta.mappingStatus && state.meta.mappingStatus !== "ok" && state.meta.mappingStatus !== "admin_unrestricted") {
          countEl.textContent = state.meta.message || MAPPING_MESSAGES[state.meta.mappingStatus] || "Operator company not connected.";
        } else if (state.requests.length === 0) {
          countEl.textContent = "No operating opportunities in your scope yet.";
        }
      }
    } catch (err) {
      console.error("[operator-development-dashboard] load failed:", err.message);
      showError(err.message || "We could not load the operator deals workspace. Try refreshing the page.");
      state.requests = [];
    } finally {
      state.loading = false;
      updateTabCounts();
      await refreshKpiStrip();
      switchTab(state.currentTab);
    }
  }

  async function init() {
    wireTabNav();
    wireFilters();
    wireModal();
    wireTableActions();
    restoreTabFromHash();
    state.loading = true;
    try {
      var meData = await loadMeContext();
      showPhaseBanner(null, meData.meta);
      await loadOperatorDealRequests();
    } catch (err) {
      showError(err.message || "Could not initialize My Operator Deals.");
      state.loading = false;
      renderWorkspaceTable();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
