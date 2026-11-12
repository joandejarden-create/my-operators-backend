/**
 * GDI_UNIVERSAL_PRESENTATION_CONTROLLER_V1
 *
 * Shared browse / filter / sort / tile / drawer presentation for authenticated
 * GDI and external client-share. Adapters supply transport (api) and
 * capability-gated extras only — not independent field interpretation.
 *
 * Renderer: DealalityGdiUi (tiles, drawers, contacts, chrome).
 */
(function (global) {
  "use strict";

  var VERSION = "gdi-universal-presentation-controller-v1";

  function createDefaultFilters() {
    return {
      priority: "",
      segment: "",
      booking: "",
      commercialStage: "",
      salesOpportunity: "",
      sourcingSubgroup: "BOTH",
      territory: "",
      weekly: "",
      opportunityType: "",
      workflow: "",
    };
  }

  function resolveOpportunityTitle(o) {
    if (!o || typeof o !== "object") return "Opportunity";
    return (
      o.displayTitle ||
      o.drawerHeaderTitle ||
      o.title ||
      o.organizationName ||
      "Opportunity"
    );
  }

  function filteredSorted(state, UI) {
    return UI.sortOpportunities(
      UI.filterOpportunities(state.opportunities || [], state.filters || {}),
      state.sortKey || "priority",
      state.sortDir == null ? 1 : state.sortDir
    );
  }

  function applyTilePairFilter(state, priority, booking, e, render) {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    state.filters = state.filters || createDefaultFilters();
    state.filters.priority = priority || "";
    state.filters.booking = booking || "";
    if (typeof render === "function") render();
  }

  /**
   * Wire shared filter / sort / view-mode controls.
   * opts.onInventoryViewChange — auth-only inventory toggle hook
   * opts.onExport — CSV download handler
   */
  function wireBrowseFilterControls(opts) {
    opts = opts || {};
    var root = opts.root;
    var state = opts.state;
    var UI = opts.UI;
    var render = opts.render;
    var persistBrowse = opts.persistBrowse;
    if (!root || !state || !UI || typeof render !== "function") return;

    function reconcile(key) {
      if (typeof UI.reconcileBrowseFilters === "function") {
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          key
        );
      }
    }

    if (typeof opts.onInventoryViewChange === "function") {
      root.querySelectorAll("[data-gdi-inventory-view]").forEach(function (btn) {
        btn.addEventListener("click", function () {
          var next = btn.getAttribute("data-gdi-inventory-view") || "customer";
          if (next !== "research") next = "customer";
          opts.onInventoryViewChange(next);
        });
      });
    }

    root.querySelectorAll("[data-gdi-priority]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.priority = btn.getAttribute("data-gdi-priority") || "";
        reconcile("priority");
        render();
      });
    });
    root.querySelectorAll("[data-gdi-booking]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.booking = btn.getAttribute("data-gdi-booking") || "";
        reconcile("booking");
        render();
      });
    });
    root.querySelectorAll("[data-gdi-commercial-stage]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.commercialStage =
          btn.getAttribute("data-gdi-commercial-stage") || "";
        reconcile("commercialStage");
        render();
      });
    });
    root.querySelectorAll("[data-gdi-sales-opportunity]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.salesOpportunity =
          btn.getAttribute("data-gdi-sales-opportunity") || "";
        if (state.filters.salesOpportunity !== "HOTEL_SOURCING_NOT_CONFIRMED") {
          state.filters.sourcingSubgroup = "BOTH";
        }
        reconcile("salesOpportunity");
        render();
      });
    });
    root.querySelectorAll("[data-gdi-sourcing-subgroup]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.sourcingSubgroup =
          btn.getAttribute("data-gdi-sourcing-subgroup") || "BOTH";
        state.filters.salesOpportunity = "HOTEL_SOURCING_NOT_CONFIRMED";
        render();
      });
    });
    root
      .querySelectorAll("[data-gdi-tile-priority][data-gdi-tile-booking]")
      .forEach(function (btn) {
        btn.addEventListener("click", function (e) {
          applyTilePairFilter(
            state,
            btn.getAttribute("data-gdi-tile-priority"),
            btn.getAttribute("data-gdi-tile-booking"),
            e,
            render
          );
        });
      });

    var sortSel = document.getElementById("gdiSortSelect");
    if (sortSel) {
      sortSel.addEventListener("change", function () {
        state.sortKey = sortSel.value || "priority";
        state.sortDir = 1;
        if (typeof persistBrowse === "function") persistBrowse();
        render();
      });
    }
    var sortDirBtn = document.getElementById("gdiSortDirBtn");
    if (sortDirBtn) {
      sortDirBtn.addEventListener("click", function () {
        state.sortDir = state.sortDir === 1 ? -1 : 1;
        render();
      });
    }
    root.querySelectorAll("[data-gdi-view]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.viewMode =
          btn.getAttribute("data-gdi-view") === "tiles" ? "tiles" : "list";
        if (typeof persistBrowse === "function") persistBrowse();
        render();
      });
    });

    var exportBtn = document.getElementById("gdiExportCsvBtn");
    if (exportBtn && typeof opts.onExport === "function") {
      exportBtn.addEventListener("click", opts.onExport);
    }
  }

  /**
   * Shared opportunity detail open — same drawer renderer for auth + share.
   * opts.buildValidationHtml(opportunity) — capability-gated extras (share/auth)
   * opts.whoContactHtml — optional contact helper
   * opts.detailUrl(id) — absolute API path for detail fetch
   * opts.api(path) — returns Promise<{ opportunity, scoreAudit }>
   */
  function openOpportunityDetail(opts) {
    opts = opts || {};
    var UI = opts.UI;
    var state = opts.state;
    var drawer = opts.drawer;
    var drawerTitle = opts.drawerTitle;
    var drawerBody = opts.drawerBody;
    var drawerClose = opts.drawerClose;
    var id = opts.opportunityId;
    if (!UI || !state || !drawer || !id) return Promise.resolve();

    var listRow = (state.opportunities || []).find(function (o) {
      return o.id === id;
    });
    state.drawerReturnFocusEl =
      (document.activeElement &&
        document.activeElement.closest &&
        document.activeElement.closest("[data-open]")) ||
      document.activeElement ||
      null;

    drawerTitle.textContent = resolveOpportunityTitle(listRow);
    drawerBody.innerHTML = UI.detailLoadingHtml();
    if (typeof drawer.showModal === "function") drawer.showModal();
    else drawer.setAttribute("open", "open");
    if (drawerClose && typeof drawerClose.focus === "function") {
      try {
        drawerClose.focus();
      } catch (e) {
        /* ignore */
      }
    }

    var url =
      typeof opts.detailUrl === "function"
        ? opts.detailUrl(id)
        : opts.detailUrl;
    return opts
      .api(url)
      .then(function (data) {
        var o = data.opportunity || data;
        var audit = data.scoreAudit || {};
        if (typeof opts.normalizeOpportunity === "function") {
          o = opts.normalizeOpportunity(o, listRow, data) || o;
        }
        // Locked published: approvedCustomerFacingDetail is authoritative for
        // drawer body. Flatten onto the row without dropping the nested object.
        if (
          o &&
          o.approvedCustomerFacingDetail &&
          typeof o.approvedCustomerFacingDetail === "object"
        ) {
          o = Object.assign({}, o.approvedCustomerFacingDetail, o, {
            approvedCustomerFacingDetail: o.approvedCustomerFacingDetail,
            id: o.id || o.approvedCustomerFacingDetail.id,
            publishedPresentationLocked: true,
          });
        }
        drawerTitle.textContent =
          resolveOpportunityTitle(o) || resolveOpportunityTitle(listRow);
        var validationHtml =
          typeof opts.buildValidationHtml === "function"
            ? opts.buildValidationHtml(o)
            : "";
        drawerBody.innerHTML = UI.intelligenceDetailHtml(o, audit, {
          validationHtml: validationHtml,
          whoContactHtml: opts.whoContactHtml || null,
          mode: opts.mode || "auth",
          permissionContext: opts.permissionContext || null,
        });
        if (typeof opts.afterDetailRender === "function") {
          opts.afterDetailRender(o);
        }
        return o;
      })
      .catch(function (err) {
        var raw =
          (err && err.data && (err.data.message || err.data.error)) ||
          (err && err.message) ||
          "";
        var friendly = raw;
        if (
          !friendly ||
          /opportunity_not_found/i.test(String(friendly)) ||
          String(friendly).toLowerCase() === "request failed"
        ) {
          friendly =
            "This opportunity could not be opened. Please try again. If the problem continues, refresh the list and reopen the card.";
        }
        drawerBody.innerHTML = UI.detailErrorHtml(friendly, {
          retryOpportunityId: id,
        });
        throw err;
      });
  }

  function buildBrowseChromeOptions(state, UI, opts) {
    opts = opts || {};
    var rows = filteredSorted(state, UI);
    return {
      mode: opts.mode || "auth",
      inventoryView: state.inventoryView || "customer",
      customerInventoryCount: state.customerInventoryCount,
      researchInventoryCount: state.researchInventoryCount,
      showInventoryViewToggle: false,
      activePriority: state.filters.priority,
      priorityCounts: UI.facetCountByPriority(
        state.opportunities,
        state.filters
      ),
      activeBooking: state.filters.booking,
      actionCounts: UI.facetCountByActionStatus
        ? UI.facetCountByActionStatus(state.opportunities, state.filters)
        : {},
      activeCommercialStage: state.filters.commercialStage || "",
      commercialStageCounts: UI.facetCountByCommercialStage
        ? UI.facetCountByCommercialStage(state.opportunities, state.filters)
        : {},
      activeSalesOpportunity: state.filters.salesOpportunity || "",
      salesOpportunityCounts: UI.facetCountBySalesOpportunity
        ? UI.facetCountBySalesOpportunity(state.opportunities, state.filters)
        : {},
      sourcingSubgroup: state.filters.sourcingSubgroup || "BOTH",
      weeklyHeaderCounts: UI.weeklyDeltaHeaderCounts
        ? UI.weeklyDeltaHeaderCounts(state.opportunities)
        : {},
      shown: rows.length,
      total: (state.opportunities || []).length,
      sort: state.sortKey,
      viewMode: state.viewMode,
      noun: opts.noun || "Opportunities",
      exportHref: opts.exportHref || "",
    };
  }

  function renderOpportunityCardsHtml(state, UI, rows) {
    var list = rows || filteredSorted(state, UI);
    if (typeof UI.opportunityCardsGridHtml === "function") {
      return UI.opportunityCardsGridHtml(
        list,
        state.viewMode || "tiles",
        state.filters || {}
      );
    }
    return "";
  }

  global.DealalityGdiUniversalPresentationController = {
    VERSION: VERSION,
    createDefaultFilters: createDefaultFilters,
    resolveOpportunityTitle: resolveOpportunityTitle,
    filteredSorted: filteredSorted,
    applyTilePairFilter: applyTilePairFilter,
    wireBrowseFilterControls: wireBrowseFilterControls,
    openOpportunityDetail: openOpportunityDetail,
    buildBrowseChromeOptions: buildBrowseChromeOptions,
    renderOpportunityCardsHtml: renderOpportunityCardsHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
