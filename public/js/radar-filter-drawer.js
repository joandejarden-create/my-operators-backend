/**
 * Radar filter drawer — slide-over panel for Dealality Radar map filters.
 */
(function () {
  "use strict";

  var FILTER_PAIRS = [
    ["parentCompanyFilter", "parentCompanyFilterDrawer"],
    ["brandFilter", "brandFilterDrawer"],
    ["propertyTypeFilter", "propertyTypeFilterDrawer"],
    ["locationTypeFilter", "locationTypeFilterDrawer"],
  ];

  var DRAWER_ONLY_FILTERS = [
    "regionFilter",
    "countryFilter",
    "marketFilter",
    "hotelTypeFilter",
    "hotelServiceModelFilter",
    "managementCompanyFilter",
    "operationTypeFilter",
    "submarketFilter",
  ];

  var STATUS_VALUES = ["Open", "Pipeline", "Candidate"];
  var syncing = false;

  function el(id) {
    return document.getElementById(id);
  }

  function isDrawerOpen() {
    var drawer = el("radarFilterDrawer");
    return Boolean(drawer && drawer.classList.contains("is-open"));
  }

  function getActiveStatuses() {
    if (typeof window.getRadarStatusFilter === "function") {
      return window.getRadarStatusFilter() || [];
    }
    var select = el("statusFilter");
    return select && select.value ? [select.value] : [];
  }

  function countActiveMapLayers() {
    if (typeof window.countRadarMapLayerFilters === "function") {
      return window.countRadarMapLayerFilters();
    }
    return 0;
  }

  function countActiveFilters() {
    var count = 0;
    FILTER_PAIRS.forEach(function (pair) {
      var node = el(pair[0]);
      if (node && String(node.value || "").trim()) count += 1;
    });

    DRAWER_ONLY_FILTERS.forEach(function (id) {
      var node = el(id);
      if (node && String(node.value || "").trim()) count += 1;
    });

    ["roomsMinFilter", "roomsMaxFilter"].forEach(function (id) {
      var node = el(id);
      if (node && String(node.value || "").trim()) count += 1;
    });

    var search = el("locationSearch");
    if (search && String(search.value || "").trim()) count += 1;

    var statuses = getActiveStatuses();
    if (statuses.length > 0 && statuses.length < STATUS_VALUES.length) {
      count += 1;
    }

    count += countActiveMapLayers();

    return count;
  }

  function updateBadge() {
    var badge = el("radarFilterCountBadge");
    if (!badge) return;
    var count = countActiveFilters();
    if (count > 0) {
      badge.textContent = String(count);
      badge.style.display = "inline-flex";
    } else {
      badge.style.display = "none";
    }
  }

  function mirrorSelectOptions(sourceId, targetId) {
    var src = el(sourceId);
    var tgt = el(targetId);
    if (!src || !tgt) return;
    var current = tgt.value;
    tgt.innerHTML = src.innerHTML;
    if (current && Array.prototype.some.call(tgt.options, function (opt) { return opt.value === current; })) {
      tgt.value = current;
    } else {
      tgt.value = src.value;
    }
  }

  function mirrorAllOptions() {
    FILTER_PAIRS.forEach(function (pair) {
      mirrorSelectOptions(pair[0], pair[1]);
    });
  }

  function syncDrawerFromBar() {
    if (syncing) return;
    syncing = true;
    FILTER_PAIRS.forEach(function (pair) {
      var bar = el(pair[0]);
      var drawer = el(pair[1]);
      if (bar && drawer) drawer.value = bar.value;
    });
    syncing = false;
    updateBadge();
  }

  function syncBarFromDrawer() {
    if (syncing) return;
    syncing = true;
    FILTER_PAIRS.forEach(function (pair) {
      var bar = el(pair[0]);
      var drawer = el(pair[1]);
      if (!bar || !drawer || bar.value === drawer.value) return;
      bar.value = drawer.value;
      bar.dispatchEvent(new Event("change", { bubbles: true }));
    });
    syncing = false;
    updateBadge();
  }

  function bindFilterSync() {
    FILTER_PAIRS.forEach(function (pair) {
      var bar = el(pair[0]);
      var drawer = el(pair[1]);
      if (bar) {
        bar.addEventListener("change", function () {
          if (syncing) return;
          syncing = true;
          if (drawer) drawer.value = bar.value;
          syncing = false;
          updateBadge();
        });
      }
      if (drawer) {
        drawer.addEventListener("change", function () {
          syncBarFromDrawer();
        });
      }
    });

    var search = el("locationSearch");
    if (search) search.addEventListener("input", updateBadge);

    var statusSelect = el("statusFilter");
    if (statusSelect) {
      statusSelect.addEventListener("change", updateBadge);
    }

    DRAWER_ONLY_FILTERS.forEach(function (id) {
      var node = el(id);
      if (node) node.addEventListener("change", updateBadge);
    });

    ["roomsMinFilter", "roomsMaxFilter"].forEach(function (id) {
      var node = el(id);
      if (node) node.addEventListener("input", updateBadge);
    });
  }

  function openDrawer() {
    var overlay = el("radarFilterDrawerOverlay");
    var drawer = el("radarFilterDrawer");
    if (!overlay || !drawer) return;
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.enhance === "function") {
      window.RadarLayerDrawer.enhance();
    }
    if (typeof window.preloadRadarLayerFilterChips === "function") {
      window.preloadRadarLayerFilterChips();
    }
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === "function") {
      window.RadarLayerDrawer.syncControls();
    }
    mirrorAllOptions();
    syncDrawerFromBar();
    if (typeof window.syncTerritoryFilterOptions === "function") {
      window.syncTerritoryFilterOptions();
    } else if (typeof window.syncTerritoryBrandFilterOptions === "function") {
      window.syncTerritoryBrandFilterOptions();
    }
    overlay.classList.add("is-open");
    drawer.classList.add("is-open");
    overlay.setAttribute("aria-hidden", "false");
    drawer.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
    var closeBtn = el("radarFilterDrawerClose");
    if (closeBtn) closeBtn.focus();
  }

  function closeDrawer() {
    var overlay = el("radarFilterDrawerOverlay");
    var drawer = el("radarFilterDrawer");
    if (!overlay || !drawer) return;
    overlay.classList.remove("is-open");
    drawer.classList.remove("is-open");
    overlay.setAttribute("aria-hidden", "true");
    drawer.setAttribute("aria-hidden", "true");
    document.body.style.overflow = "";
    var openBtn = el("radarFilterOpenBtn");
    if (openBtn) openBtn.focus();
  }

  function init() {
    var openBtn = el("radarFilterOpenBtn");
    var closeBtn = el("radarFilterDrawerClose");
    var overlay = el("radarFilterDrawerOverlay");
    var doneBtn = el("radarFilterDoneBtn");
    var clearBtn = el("radarFilterResetViewBtn");

    if (openBtn) openBtn.addEventListener("click", openDrawer);
    if (closeBtn) closeBtn.addEventListener("click", closeDrawer);
    if (overlay) overlay.addEventListener("click", closeDrawer);
    if (doneBtn) doneBtn.addEventListener("click", closeDrawer);
    if (clearBtn) {
      clearBtn.addEventListener("click", function () {
        if (typeof window.resetView === "function") {
          window.resetView();
        }
        closeDrawer();
      });
    }

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && isDrawerOpen()) closeDrawer();
    });

    bindFilterSync();
    mirrorAllOptions();
    syncDrawerFromBar();
    updateBadge();
  }

  window.RadarFilterDrawer = {
    open: openDrawer,
    close: closeDrawer,
    updateBadge: updateBadge,
    countActive: countActiveFilters,
    mirrorOptions: mirrorAllOptions,
    syncFromBar: syncDrawerFromBar,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
