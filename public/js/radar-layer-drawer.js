/**
 * Expandable map layer cards with sub-filters in the Radar filter drawer.
 */
(function () {
  "use strict";

  var LAYER_CHIP_CONFIG = {
    hotelVisibilityToggle: {
      hint: "Choose which hotel statuses appear as map markers.",
      group: "hotelStatuses",
      chips: [
        { key: "Open", label: "Open" },
        { key: "Pipeline", label: "Pipeline" },
        { key: "Candidate", label: "Candidate / LOI" },
      ],
    },
    chainScaleToggle: {
      hint: "When Chain Scale View is on, limit which scales are shown.",
      group: "chainScales",
      chips: [
        { key: "Luxury", label: "Luxury" },
        { key: "Upper Upscale", label: "Upper Upscale" },
        { key: "Upscale", label: "Upscale" },
        { key: "Upper Midscale", label: "Upper Midscale" },
        { key: "Midscale", label: "Midscale" },
        { key: "Economy", label: "Economy" },
        { key: "Independent", label: "Independent" },
      ],
    },
    cityAggregationToggle: {
      hint: "Show city summary markers that meet a size or activity threshold.",
      type: "select",
      group: "cityAggFilter",
      options: [
        { type: "group", label: "Total hotels in city" },
        { value: "hotels:1", label: "All cities (1+ hotel)" },
        { value: "hotels:2", label: "2+ hotels (hide singletons)" },
        { value: "hotels:3", label: "3+ hotels" },
        { value: "hotels:5", label: "5+ hotels (small cluster)" },
        { value: "hotels:10", label: "10+ hotels (established city)" },
        { value: "hotels:15", label: "15+ hotels" },
        { value: "hotels:25", label: "25+ hotels (major market)" },
        { value: "hotels:50", label: "50+ hotels (large hub)" },
        { value: "hotels:100", label: "100+ hotels (mega hub)" },
        { type: "group", label: "Total keys (rooms) in city" },
        { value: "rooms:500", label: "500+ keys" },
        { value: "rooms:1000", label: "1,000+ keys" },
        { value: "rooms:2500", label: "2,500+ keys" },
        { value: "rooms:5000", label: "5,000+ keys" },
        { value: "rooms:10000", label: "10,000+ keys" },
        { type: "group", label: "Open hotels in city" },
        { value: "open:3", label: "3+ open hotels" },
        { value: "open:5", label: "5+ open hotels" },
        { value: "open:10", label: "10+ open hotels" },
        { value: "open:20", label: "20+ open hotels" },
        { value: "open:50", label: "50+ open hotels" },
        { type: "group", label: "Pipeline activity" },
        { value: "pipeline:1", label: "Has pipeline (1+)" },
        { value: "pipeline:3", label: "3+ pipeline hotels" },
        { value: "pipeline:5", label: "5+ pipeline hotels" },
        { value: "pipeline:10", label: "10+ pipeline hotels" },
      ],
    },
    whiteSpaceToggle: {
      hint: "Green / yellow hexagons only. Click a marker for Key Facts.",
      group: "whiteSpaceLevels",
      chips: [
        { key: "high", label: "Strong White Space (Green)" },
        { key: "medium", label: "Some White Space (Yellow)" },
      ],
    },
    brandTerritoryToggle: {
      hint: "Open vs conflict for your selected brand. Sister brands trigger Review at same chain scale (amber) or adjacent chain scale (yellow).",
      group: "territoryStatuses",
      chips: [
        { key: "open", label: "Open Territory (Green)" },
        { key: "review_same", label: "Review — Same Chain Scale (Amber)" },
        { key: "review_adjacent", label: "Review — Adjacent Chain Scale (Yellow)" },
      ],
      useExistingWrap: "territoryControlsWrap",
    },
    penetrationToggle: {
      hint: "Filter brand penetration squares by market intensity.",
      group: "penetrationLevels",
      chips: [
        { key: "high", label: "High (70%+ branded)" },
        { key: "medium", label: "Medium (40–69%)" },
        { key: "low", label: "Low (<40%)" },
      ],
    },
    pipelineToggle: {
      hint: "Filter pipeline pressure triangles by supply risk.",
      group: "pipelineLevels",
      chips: [
        { key: "high", label: "High pressure" },
        { key: "medium", label: "Medium pressure" },
        { key: "low", label: "Low pressure" },
      ],
    },
    infrastructureToggle: {
      hint: "Filter travel infrastructure points by type.",
      useExistingWrap: "infrastructureFilterWrap",
    },
    demandAnchorsToggle: {
      hint: "Filter demand anchor points by category.",
      useExistingWrap: "demandAnchorsFilterWrap",
    },
  };

  function layerHasSubfilters(config) {
    return !!(
      config &&
      (config.chips || config.type === "select" || config.useExistingWrap)
    );
  }

  function buildLayerResetButton(toggleId) {
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "radar-layer-reset-btn";
    btn.setAttribute("aria-label", "Reset sub-filters to show all");
    btn.setAttribute("title", "Reset sub-filters");
    btn.innerHTML =
      '<svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
      '<path fill="currentColor" d="M17.65 6.35A7.96 7.96 0 0 0 12 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08a5.99 5.99 0 0 1-5.65 4c-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.67 4.22 1.78L13 11h7V4l-2.35 2.35z"/>' +
      "</svg>";

    btn.addEventListener("mousedown", function (e) {
      e.preventDefault();
      e.stopPropagation();
    });
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      if (typeof window.resetMapLayerSubfilters === "function") {
        window.resetMapLayerSubfilters(toggleId);
      }
    });
    return btn;
  }

  function injectLayerResetButton(toggleLabel, toggleId, config) {
    if (!toggleLabel || !layerHasSubfilters(config)) return;
    if (toggleLabel.querySelector(".radar-layer-reset-btn")) return;

    var info = toggleLabel.querySelector(".info-tooltip");
    var btn = buildLayerResetButton(toggleId);
    if (info) {
      info.insertAdjacentElement("afterend", btn);
    } else {
      toggleLabel.appendChild(btn);
    }
  }

  function getFilters() {
    if (typeof window.getMapLayerFilters === "function") {
      return window.getMapLayerFilters();
    }
    return null;
  }

  function buildChipGroup(config) {
    var wrap = document.createElement("div");
    wrap.className = "radar-layer-chip-group";
    wrap.setAttribute("data-layer-group", config.group);

    config.chips.forEach(function (chip) {
      var label = document.createElement("label");
      label.className = "radar-layer-chip";
      var input = document.createElement("input");
      input.type = "checkbox";
      input.className = "radar-layer-chip__input";
      input.value = chip.key;
      input.setAttribute("data-layer-group", config.group);
      input.setAttribute("data-layer-key", chip.key);
      input.checked = true;
      var span = document.createElement("span");
      span.textContent = chip.label;
      label.appendChild(input);
      label.appendChild(span);
      wrap.appendChild(label);
    });

    return wrap;
  }

  function buildSelectControl(config) {
    var select = document.createElement("select");
    select.className = "control-select radar-layer-select";
    select.setAttribute("data-layer-group", config.group);
    var currentGroup = null;
    config.options.forEach(function (opt) {
      if (opt.type === "group") {
        currentGroup = document.createElement("optgroup");
        currentGroup.label = opt.label;
        select.appendChild(currentGroup);
        return;
      }
      var option = document.createElement("option");
      option.value = opt.value;
      option.textContent = opt.label;
      if (currentGroup) currentGroup.appendChild(option);
      else select.appendChild(option);
    });
    return select;
  }

  function showInfraWrapInDrawer(wrapEl) {
    if (!wrapEl) return;
    wrapEl.style.removeProperty("display");
    wrapEl.classList.add("is-visible");
    var chips = wrapEl.querySelector(".infra-filter-chips");
    if (chips) {
      chips.style.removeProperty("display");
      chips.classList.add("is-visible");
    }
  }

  function preloadInfraFilterChips() {
    if (typeof window.preloadRadarLayerFilterChips === "function") {
      window.preloadRadarLayerFilterChips();
      return;
    }
    if (typeof window.renderInfrastructureFilterChips === "function") {
      window.renderInfrastructureFilterChips();
    }
    if (typeof window.renderDemandAnchorsFilterChips === "function") {
      window.renderDemandAnchorsFilterChips();
    }
  }

  function setLayerCardSubfiltersEnabled(card, enabled) {
    var body = card && card.querySelector(".radar-layer-card__body");
    if (!body) return;

    body.querySelectorAll("input, select, button").forEach(function (el) {
      if (el.type === "checkbox" && el.closest(".radar-layer-card__head")) return;
      el.disabled = !enabled;
    });

    body.querySelectorAll(".infra-filter-chip").forEach(function (chip) {
      chip.disabled = !enabled;
      chip.classList.toggle("is-disabled", !enabled);
      if (!enabled) chip.setAttribute("aria-disabled", "true");
      else chip.removeAttribute("aria-disabled");
    });
  }

  function ensureLayerStatusEl(card) {
    var body = card && card.querySelector(".radar-layer-card__body");
    if (!body) return null;
    var status = body.querySelector(".radar-layer-card__status");
    if (!status) {
      status = document.createElement("p");
      status.className = "radar-layer-card__status";
      status.hidden = true;
      body.insertBefore(status, body.firstChild);
    }
    return status;
  }

  function setLayerLoading(toggleId, loading, message) {
    var card = document.querySelector('.radar-layer-card[data-layer-toggle="' + toggleId + '"]');
    if (!card) return;

    card.classList.toggle("is-loading", !!loading);
    var status = ensureLayerStatusEl(card);
    if (status) {
      var defaultMessage =
        toggleId === "brandTerritoryToggle"
          ? "Scanning Brand Territories…"
          : "Analyzing Opportunities…";
      status.textContent = message || defaultMessage;
      if (loading) {
        status.hidden = false;
        status.removeAttribute("hidden");
        status.style.display = "";
      } else {
        status.hidden = true;
        status.setAttribute("hidden", "");
        status.style.display = "none";
      }
    }

    syncLayerCard(card);
  }

  function syncLayerCard(card) {
    if (!card) return;
    var input = card.querySelector(".radar-layer-card__head input[type=\"checkbox\"]");
    var body = card.querySelector(".radar-layer-card__body");
    var on = !!(input && input.checked);
    card.classList.toggle("is-active", on);
    if (body) {
      body.hidden = false;
      body.classList.toggle("is-layer-off", !on);
    }
    setLayerCardSubfiltersEnabled(card, on);
  }

  function syncAllLayerCards() {
    document.querySelectorAll(".radar-layer-card").forEach(syncLayerCard);
  }

  function populateTerritoryRadiusSelect(select, selectedKm) {
    var RT = window.RadarTerritory;
    if (!select) return;

    var kmOptions =
      RT && RT.TERRITORY_CONFIG && RT.TERRITORY_CONFIG.radiusKmOptions
        ? RT.TERRITORY_CONFIG.radiusKmOptions
        : [3, 5, 10, 15, 25, 50];
    var defaultKm =
      RT && RT.TERRITORY_CONFIG && RT.TERRITORY_CONFIG.defaultRadiusKm
        ? RT.TERRITORY_CONFIG.defaultRadiusKm
        : 10;
    var formatLabel =
      RT && typeof RT.formatRadiusKmMiLabel === "function"
        ? RT.formatRadiusKmMiLabel.bind(RT)
        : function (km) {
            var kmVal = Math.max(1, Math.round(Number(km) || 1));
            var miVal = Math.round(kmVal * 0.621371);
            return kmVal + " km / " + miVal + " mi";
          };
    var selected = String(selectedKm || defaultKm);

    select.innerHTML = kmOptions
      .map(function (km) {
        var value = String(km);
        return (
          '<option value="' +
          value +
          '"' +
          (value === selected ? " selected" : "") +
          ">" +
          formatLabel(km) +
          "</option>"
        );
      })
      .join("");
  }

  function syncTerritoryControls() {
    var filters = getFilters();
    if (!filters) return;

    var unitSelect = document.getElementById("territoryUnitSelect");
    var radiusWrap = document.getElementById("territoryRadiusControls");
    var radiusSelect = document.getElementById("territoryRadiusSelect");
    var pinHint = document.getElementById("territoryPinHint");
    var unit = filters.territoryUnit || "submarket";

    if (unitSelect) unitSelect.value = unit;
    if (radiusSelect) {
      populateTerritoryRadiusSelect(radiusSelect, filters.territoryRadiusKm);
    }
    if (radiusWrap) {
      radiusWrap.style.display = unit === "radius" ? "block" : "none";
    }
    if (pinHint && filters.territoryPin && filters.territoryPin.lat != null) {
      pinHint.textContent =
        "Pin at " +
        Number(filters.territoryPin.lat).toFixed(4) +
        ", " +
        Number(filters.territoryPin.lng).toFixed(4) +
        ".";
    }
  }

  function bindTerritoryControls(root) {
    var territoryParent =
      (root && root.querySelector("#territoryParentCompanyFilter")) ||
      document.getElementById("territoryParentCompanyFilter");
    if (territoryParent && !territoryParent.dataset.bound) {
      territoryParent.dataset.bound = "1";
      territoryParent.addEventListener("change", function () {
        if (typeof window.setTerritoryParentCompanyFilterValue === "function") {
          window.setTerritoryParentCompanyFilterValue(territoryParent.value);
        }
        var parentFilter = document.getElementById("parentCompanyFilter");
        if (parentFilter) {
          parentFilter.dispatchEvent(new Event("change", { bubbles: true }));
        }
      });
    }

    var territoryBrand =
      (root && root.querySelector("#territoryBrandFilter")) ||
      document.getElementById("territoryBrandFilter");
    if (territoryBrand && !territoryBrand.dataset.bound) {
      territoryBrand.dataset.bound = "1";
      territoryBrand.addEventListener("change", function () {
        if (typeof window.setTerritoryBrandFilterValue === "function") {
          window.setTerritoryBrandFilterValue(territoryBrand.value);
        }
        var brandFilter = document.getElementById("brandFilter");
        if (brandFilter) {
          brandFilter.value = territoryBrand.value;
          brandFilter.dispatchEvent(new Event("change", { bubbles: true }));
        }
      });
    }

    if (typeof window.syncTerritoryFilterOptions === "function") {
      window.syncTerritoryFilterOptions();
    } else if (typeof window.syncTerritoryBrandFilterOptions === "function") {
      window.syncTerritoryBrandFilterOptions();
    }

    var unitSelect = (root && root.querySelector("#territoryUnitSelect")) || document.getElementById("territoryUnitSelect");
    if (unitSelect && !unitSelect.dataset.bound) {
      unitSelect.dataset.bound = "1";
      unitSelect.addEventListener("change", function () {
        if (typeof window.setMapLayerFilter !== "function") return;
        window.setMapLayerFilter("territoryUnit", unitSelect.value, true);
        syncTerritoryControls();
      });
    }

    var radiusSelect = (root && root.querySelector("#territoryRadiusSelect")) || document.getElementById("territoryRadiusSelect");
    if (radiusSelect && !radiusSelect.dataset.bound) {
      var filters = getFilters();
      populateTerritoryRadiusSelect(
        radiusSelect,
        filters && filters.territoryRadiusKm
      );
      radiusSelect.dataset.bound = "1";
      radiusSelect.addEventListener("change", function () {
        if (typeof window.setMapLayerFilter !== "function") return;
        window.setMapLayerFilter("territoryRadiusKm", radiusSelect.value, true);
      });
    }

    var dropBtn = (root && root.querySelector("#territoryDropPinBtn")) || document.getElementById("territoryDropPinBtn");
    if (dropBtn && !dropBtn.dataset.bound) {
      dropBtn.dataset.bound = "1";
      dropBtn.addEventListener("click", function () {
        if (typeof window.startTerritoryPinDrop === "function") {
          window.startTerritoryPinDrop();
        }
      });
    }

    var clearBtn = (root && root.querySelector("#territoryClearPinBtn")) || document.getElementById("territoryClearPinBtn");
    if (clearBtn && !clearBtn.dataset.bound) {
      clearBtn.dataset.bound = "1";
      clearBtn.addEventListener("click", function () {
        if (typeof window.clearTerritoryPin === "function") {
          window.clearTerritoryPin();
        }
      });
    }
  }

  function syncControls() {
    var filters = getFilters();
    if (!filters) return;

    document.querySelectorAll(".radar-layer-chip__input").forEach(function (input) {
      var group = input.getAttribute("data-layer-group");
      var key = input.getAttribute("data-layer-key");
      if (!group || !key || !filters[group]) return;
      input.checked = filters[group][key] !== false;
    });

    document.querySelectorAll(".radar-layer-select").forEach(function (select) {
      var group = select.getAttribute("data-layer-group");
      if (group === "cityAggFilter" && filters.cityAggFilter) {
        select.value = String(filters.cityAggFilter);
      } else if (group === "cityAggMinHotels") {
        select.value = "hotels:" + String(filters.cityAggMinHotels || 1);
      } else if (group === "territoryUnit" && filters.territoryUnit) {
        select.value = String(filters.territoryUnit);
      } else if (group === "territoryRadiusKm" && filters.territoryRadiusKm) {
        select.value = String(filters.territoryRadiusKm);
      }
    });

    syncTerritoryControls();

    document.querySelectorAll(".radar-layer-card").forEach(syncLayerCard);
  }

  function bindChipInputs(root) {
    root.querySelectorAll(".radar-layer-chip__input").forEach(function (input) {
      input.addEventListener("change", function () {
        if (typeof window.setMapLayerFilter !== "function") return;
        window.setMapLayerFilter(
          input.getAttribute("data-layer-group"),
          input.getAttribute("data-layer-key"),
          input.checked
        );
      });
    });

    root.querySelectorAll(".radar-layer-select").forEach(function (select) {
      select.addEventListener("change", function () {
        if (typeof window.setMapLayerFilter !== "function") return;
        window.setMapLayerFilter(
          select.getAttribute("data-layer-group"),
          select.value,
          true
        );
      });
    });
  }

  var LAYER_UI_VERSION = "24";

  function enhanceOverlayControls() {
    var container = document.getElementById("radarFilterDrawerTogglesMount");
    var mount = container && container.querySelector(".overlay-controls");
    if (!mount || mount.dataset.layerEnhanced === LAYER_UI_VERSION) return;
    if (!mount.querySelector(".toggle-switch")) return;

    var children = Array.from(mount.children);
    mount.innerHTML = "";

    for (var i = 0; i < children.length; i += 1) {
      var child = children[i];
      if (!child.classList || !child.classList.contains("toggle-switch")) continue;

      var toggleInput = child.querySelector('input[type="checkbox"]');
      var toggleId = toggleInput && toggleInput.id;
      var config = toggleId ? LAYER_CHIP_CONFIG[toggleId] : null;

      var card = document.createElement("div");
      card.className = "radar-layer-card";
      if (toggleId) card.setAttribute("data-layer-toggle", toggleId);

      var head = document.createElement("div");
      head.className = "radar-layer-card__head";
      head.appendChild(child);
      injectLayerResetButton(child, toggleId, config);
      card.appendChild(head);

      var body = document.createElement("div");
      body.className = "radar-layer-card__body";

      if (config && config.hint) {
        var hint = document.createElement("p");
        hint.className = "radar-layer-card__hint";
        hint.textContent = config.hint;
        body.appendChild(hint);
      }

      if (config && config.useExistingWrap) {
        var next = children[i + 1];
        if (next && next.id === config.useExistingWrap) {
          showInfraWrapInDrawer(next);
          body.appendChild(next);
          i += 1;
        }
      } else if (config && config.type === "select") {
        body.appendChild(buildSelectControl(config));
      } else if (config && config.chips) {
        body.appendChild(buildChipGroup(config));
      }

      card.appendChild(body);
      mount.appendChild(card);

      if (toggleInput) {
        (function (layerCard, layerToggleInput) {
          layerToggleInput.addEventListener("change", function () {
            syncLayerCard(layerCard);
          });
          syncLayerCard(layerCard);
        })(card, toggleInput);
      }
    }

    bindChipInputs(mount);
    bindTerritoryControls(mount);
    syncControls();
    mount.dataset.layerEnhanced = LAYER_UI_VERSION;
    preloadInfraFilterChips();
  }

  function init() {
    window.addEventListener("load", function () {
      setTimeout(enhanceOverlayControls, 0);
      setTimeout(enhanceOverlayControls, 100);
    });
  }

  window.RadarLayerDrawer = {
    enhance: enhanceOverlayControls,
    syncControls: syncControls,
    syncTerritoryControls: syncTerritoryControls,
    syncAllLayerCards: syncAllLayerCards,
    setLayerLoading: setLayerLoading,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
