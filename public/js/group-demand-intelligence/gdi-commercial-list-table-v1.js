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
    '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>';

  var SORT_INDICATOR =
    '<span class="sort-indicator" aria-hidden="true"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>';

  var DEFAULT_COLUMNS = [
    { id: "opportunity", label: "Opportunity", sortKey: "title", required: true },
    { id: "priority", label: "Priority", sortKey: "priority", required: true },
    { id: "demandType", label: "Demand Type", sortKey: "demandType", required: true },
    { id: "demandWindow", label: "Demand Window", sortKey: "eventStartDate", required: true },
    { id: "salesAction", label: "Sales Action", sortKey: "salesAction", required: true },
    { id: "whoToContact", label: "Who to Contact", sortKey: "whoToContact", required: true },
    { id: "contactReadiness", label: "Contact Readiness", sortKey: "contactReadiness", required: true },
    { id: "actions", label: "Actions", sortKey: null, required: true, noSort: true },
  ];

  var OPTIONAL_COLUMNS = [
    { id: "estimatedRoomNights", label: "Estimated Room Nights", sortKey: "estimatedRoomNights" },
    { id: "weeklyChange", label: "Weekly Change", sortKey: "weeklyChange" },
    { id: "marketScope", label: "Market Scope", sortKey: "marketScope" },
    { id: "salesOpportunityType", label: "Sales Opportunity Type", sortKey: "salesOpportunityType" },
    { id: "needPeriodAlignment", label: "Need Period Alignment", sortKey: "needPeriodAlignment" },
    { id: "opportunityStatus", label: "Opportunity Status", sortKey: "opportunityStatus" },
    { id: "lastVerified", label: "Last Verified", sortKey: "lastVerified" },
  ];

  var CONTACT_READINESS_RANK = {
    SALES_READY: 0,
    REACHABLE: 1,
    CONTACT_RESEARCH_REQUIRED: 2,
    CONTACT_IDENTIFICATION_REQUIRED: 3,
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

  function loadOptionalColumns() {
    try {
      var raw = sessionStorage.getItem(STORAGE_COLUMNS);
      if (!raw) return {};
      var parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (e) {
      return {};
    }
  }

  function saveOptionalColumns(map) {
    try {
      sessionStorage.setItem(STORAGE_COLUMNS, JSON.stringify(map || {}));
    } catch (e) {
      /* ignore */
    }
  }

  function visibleColumns(optionalMap) {
    optionalMap = optionalMap || loadOptionalColumns();
    var cols = DEFAULT_COLUMNS.slice();
    OPTIONAL_COLUMNS.forEach(function (c) {
      if (optionalMap[c.id] === true) cols.splice(cols.length - 1, 0, c);
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
    return text(o.displayTitle || o.title || o.name) || "Untitled opportunity";
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

  function demandTypeLabel(o, UI) {
    if (UI && typeof UI.segmentDisplayLabel === "function") {
      try {
        var lab = UI.segmentDisplayLabel(o);
        if (lab) return text(lab);
      } catch (e) {
        /* fall through */
      }
    }
    return (
      text(o.segmentLabel) ||
      text(o.segment) ||
      text(o.demandSegment) ||
      text(o.opportunityTypeLabel) ||
      "—"
    );
  }

  function demandWindowDisplay(o, UI) {
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
    if (/ongoing|continuous|recurring/i.test(timing)) return timing;
    if (timing) return timing;
    return "Timing unknown";
  }

  function demandWindowSortValue(o) {
    return text(o.eventStartDate || o.tileEventStartDate || "");
  }

  function salesActionMeta(o, UI) {
    var key = "";
    var label = "";
    if (UI && typeof UI.tileSalesActionPill === "function") {
      // Prefer DTO fields used by tileSalesActionPill internals
      var canon = o.canonicalCustomerPresentation || null;
      key =
        (canon && canon.commercialAction && canon.commercialAction.key) ||
        o.salesActionKey ||
        (o.tileBadgePlan && o.tileBadgePlan.salesActionKey) ||
        "";
      label =
        (canon && canon.commercialAction && canon.commercialAction.pillLabel) ||
        o.salesActionPillLabel ||
        (o.tileBadgePlan && o.tileBadgePlan.salesActionPillLabel) ||
        "";
    }
    key = text(key);
    label = text(label);
    if (!label) {
      label =
        text(o.recommendedSalesAction) ||
        text(o.recommendedAction) ||
        text(o.recommendedNextStep) ||
        "—";
    }
    return { key: key, label: label };
  }

  function whoContactMeta(o, UI) {
    var contact = null;
    if (UI && typeof UI.selectCompactTileContact === "function") {
      try {
        contact = UI.selectCompactTileContact(o, o) || null;
      } catch (e) {
        contact = null;
      }
    }
    if (!contact) {
      contact =
        (o.primaryContact && o.primaryContact.name && o.primaryContact) ||
        null;
    }
    var name = text(
      (contact && (contact.name || contact.fullName)) ||
        o.primaryContactName ||
        "",
    );
    var role = text(
      (contact && (contact.role || contact.title || contact.jobTitle)) ||
        o.primaryContactRole ||
        "",
    );
    var buyerCode =
      (o.buyerVsEntry ||
        (o.primaryContact && o.primaryContact.buyerVsEntry) ||
        "") + "";
    if (buyerCode === "RELEVANT_COMMERCIAL_ENTRY_POINT" && role) {
      // Do not imply decision-maker when entry point only
      role = role;
    }
    if (!name) {
      return {
        name: "Contact not yet identified",
        role: "",
        sort: "",
        missing: true,
      };
    }
    return { name: name, role: role, sort: name.toLowerCase(), missing: false };
  }

  function contactReadinessMeta(o) {
    var tier = text(
      o.contactActionabilityTier || o.contactActionabilityFilterKey || "",
    );
    var label =
      text(o.contactActionabilityLabel) ||
      (tier === "SALES_READY"
        ? "Sales-ready"
        : tier === "REACHABLE"
          ? "Reachable"
          : tier === "CONTACT_RESEARCH_REQUIRED"
            ? "Research needed"
            : tier === "CONTACT_IDENTIFICATION_REQUIRED"
              ? "Not identified"
              : tier || "—");
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

  function optionalCellValue(o, colId, UI) {
    switch (colId) {
      case "estimatedRoomNights": {
        var n =
          o.estimatedRoomNights ??
          o.peakRooms ??
          o.roomNights ??
          o.estimatedPeakRooms;
        if (n == null || n === "") return { display: "—", sort: null };
        var num = Number(n);
        return {
          display: Number.isFinite(num) ? String(num) : text(n),
          sort: Number.isFinite(num) ? num : null,
        };
      }
      case "weeklyChange": {
        var w =
          text(o.weeklyDeltaLabel) ||
          text(o.weeklyDeltaState) ||
          (UI && typeof UI.weeklyDeltaStateOf === "function"
            ? UI.weeklyDeltaStateOf(o)
            : "");
        return { display: w || "—", sort: w || "" };
      }
      case "marketScope": {
        var m = text(o.marketScope || o.marketScopeLabel || o.population);
        return { display: m || "—", sort: m.toLowerCase() };
      }
      case "salesOpportunityType": {
        var s = text(
          o.salesOpportunityTypeLabel ||
            o.salesOpportunityType ||
            o.opportunityTypeLabel ||
            "",
        );
        return { display: s || "—", sort: s.toLowerCase() };
      }
      case "needPeriodAlignment": {
        var aligned =
          o.needDateMatch === true ||
          o.needPeriodAligned === true ||
          (o.needDateMatchV1 && o.needDateMatchV1.aligned === true);
        var nd =
          text(o.needDateRelevanceClass) ||
          (aligned ? "Aligned" : text(o.needDateIndicator?.label) || "—");
        return { display: nd, sort: aligned ? 0 : 1 };
      }
      case "opportunityStatus": {
        var st = text(
          o.opportunityStatusLabel || o.statusLabel || o.status || "",
        );
        return { display: st || "—", sort: st.toLowerCase() };
      }
      case "lastVerified": {
        var lv = text(
          o.lastVerifiedAt || o.lastVerified || o.evidenceAsOf || "",
        );
        return { display: lv || "—", sort: lv };
      }
      default:
        return { display: "—", sort: "" };
    }
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

  function actionsCellHtml(o) {
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
      "</button></div>"
    );
  }

  function headerCellHtml(col, sortKey, sortDir) {
    if (col.noSort || !col.sortKey) {
      return (
        '<th class="no-sort" scope="col">' + esc(col.label) + "</th>"
      );
    }
    var active = sortKey === col.sortKey;
    var cls =
      "gdi-list-th" +
      (active ? (sortDir === -1 ? " sort-desc" : " sort-asc") : "");
    return (
      '<th class="' +
      cls +
      '" data-sort="' +
      esc(col.sortKey) +
      '" scope="col" tabindex="0" role="columnheader" aria-sort="' +
      (active ? (sortDir === -1 ? "descending" : "ascending") : "none") +
      '">' +
      esc(col.label) +
      SORT_INDICATOR +
      "</th>"
    );
  }

  function rowCellsHtml(o, cols, UI) {
    return cols
      .map(function (col) {
        switch (col.id) {
          case "opportunity":
            return "<td>" + identityCellHtml(o) + "</td>";
          case "priority": {
            var p = priorityMeta(o, UI);
            var pill =
              UI && typeof UI.priorityPill === "function"
                ? UI.priorityPill(p.key)
                : '<span class="gdi-pill">' + esc(p.label) + "</span>";
            return "<td>" + pill + "</td>";
          }
          case "demandType":
            return "<td>" + esc(demandTypeLabel(o, UI)) + "</td>";
          case "demandWindow":
            return (
              "<td><div class=\"gdi-list-date\">" +
              esc(demandWindowDisplay(o, UI)) +
              "</div></td>"
            );
          case "salesAction": {
            var sa = salesActionMeta(o, UI);
            return (
              "<td><span class=\"gdi-list-sales-action\" data-sales-action=\"" +
              esc(sa.key) +
              '">' +
              esc(sa.label) +
              "</span></td>"
            );
          }
          case "whoToContact": {
            var who = whoContactMeta(o, UI);
            return (
              "<td><div class=\"gdi-list-who" +
              (who.missing ? " is-missing" : "") +
              '"><div class="gdi-list-who-name">' +
              esc(who.name) +
              "</div>" +
              (who.role
                ? '<div class="gdi-list-who-role">' + esc(who.role) + "</div>"
                : "") +
              "</div></td>"
            );
          }
          case "contactReadiness": {
            var cr = contactReadinessMeta(o);
            return (
              '<td><span class="gdi-list-readiness" data-actionability-tier="' +
              esc(cr.tier) +
              '">' +
              esc(cr.label) +
              "</span></td>"
            );
          }
          case "actions":
            return "<td>" + actionsCellHtml(o) + "</td>";
          default: {
            var opt = optionalCellValue(o, col.id, UI);
            return "<td>" + esc(opt.display) + "</td>";
          }
        }
      })
      .join("");
  }

  var COLUMNS_ICON_SVG =
    '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="4" width="5" height="16" rx="1"/><rect x="10" y="4" width="5" height="16" rx="1"/><rect x="17" y="4" width="4" height="16" rx="1"/></svg>';

  function columnSettingsHtml(optionalMap) {
    optionalMap = optionalMap || loadOptionalColumns();
    return (
      '<div class="gdi-list-col-settings">' +
      '<button type="button" class="gdi-list-col-settings__toggle sort-icon" id="gdiListColumnSettingsBtn" title="Customize Columns" aria-label="Customize Columns" aria-expanded="false" aria-controls="gdiListColumnSettingsPanel" aria-haspopup="true">' +
      COLUMNS_ICON_SVG +
      "</button>" +
      '<div class="gdi-list-col-settings__panel" id="gdiListColumnSettingsPanel" hidden role="menu">' +
      '<p class="gdi-list-col-settings__hint">Optional columns (existing data only)</p>' +
      OPTIONAL_COLUMNS.map(function (c) {
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
      }).join("") +
      "</div></div>"
    );
  }

  function tableHtml(rows, opts) {
    opts = opts || {};
    var UI = opts.UI || global.DealalityGdiUi;
    var sortKey = opts.sortKey || "priority";
    var sortDir = opts.sortDir == null ? 1 : opts.sortDir;
    var optionalMap = opts.optionalColumns || loadOptionalColumns();
    var cols = visibleColumns(optionalMap);
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
        '"><div class="gdi-list-empty">No opportunities match these filters.</div></td></tr>';

    // Columns control lives in the shared results toolbar (single row).
    // Do not render a separate list-table toolbar row.
    return (
      '<div class="gdi-list-table-wrap" data-gdi-view-mode="list" data-gdi-contract="' +
      CONTRACT +
      '">' +
      '<div class="gdi-list-table-scroll">' +
      '<table class="gdi-commercial-table deals-table" id="gdiCommercialOppTable">' +
      "<thead><tr>" +
      cols.map(function (c) {
        return headerCellHtml(c, sortKey, sortDir);
      }).join("") +
      "</tr></thead>" +
      "<tbody>" +
      body +
      "</tbody></table></div></div>"
    );
  }

  /**
   * Extend UI.sortOpportunities for list-column keys without replacing tile sorts.
   */
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
    } else if (key === "contactReadiness") {
      var ar = contactReadinessMeta(a).rank;
      var br = contactReadinessMeta(b).rank;
      cmp = ar !== br ? (ar - br) * dir : 0;
    } else if (key === "estimatedRoomNights") {
      var an = optionalCellValue(a, "estimatedRoomNights").sort;
      var bn = optionalCellValue(b, "estimatedRoomNights").sort;
      // Missing numeric values always sort last (direction-independent).
      if (an == null && bn == null) cmp = 0;
      else if (an == null) cmp = 1;
      else if (bn == null) cmp = -1;
      else cmp = (an - bn) * dir;
    } else if (
      key === "weeklyChange" ||
      key === "marketScope" ||
      key === "salesOpportunityType" ||
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
      // Unknown / ongoing-without-date always after dated windows.
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

    // Row/name/view open uses existing [data-open] handlers in app.js / share-app.js.

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
    tableHtml: tableHtml,
    columnSettingsHtml: columnSettingsHtml,
    visibleColumns: visibleColumns,
    loadOptionalColumns: loadOptionalColumns,
    saveOptionalColumns: saveOptionalColumns,
    compareForSortKey: compareForSortKey,
    wireListTableInteractions: wireListTableInteractions,
    listTilesIdParity: listTilesIdParity,
    CONTACT_READINESS_RANK: CONTACT_READINESS_RANK,
  };
})(typeof window !== "undefined" ? window : globalThis);
