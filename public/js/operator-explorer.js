/**
 * Operator Explorer page logic — generated from public/operator-explorer.html.
 * Regenerate: node webflow-operator-explorer-package/scripts/extract-from-public.js
 * then re-apply DEALITY_API_BASE wiring below.
 */
(function (global) {
  var DEFAULT_RAILWAY = "https://my-operators-backend-production.up.railway.app";
  var raw =
    global.DEALITY_API_BASE && String(global.DEALITY_API_BASE).replace(/\/$/, "").trim();
  if (!raw && global.location && global.location.hostname) {
    var h = global.location.hostname;
    var local = h === "localhost" || h === "127.0.0.1" || h === "[::1]";
    if (!local) raw = DEFAULT_RAILWAY;
  }
  var base = raw || "";
  global.__dealityApiUrl = function (path) {
    var p = path.charAt(0) === "/" ? path : "/" + path;
    return base ? base + p : p;
  };
})(typeof window !== "undefined" ? window : this);

document.addEventListener("DOMContentLoaded", function () {
  const resultsList = document.getElementById("resultsList");
  const emptyState = document.getElementById("emptyState");
  const resultsCount = document.getElementById("resultsCount");
  const sortSelect = document.getElementById("sortSelect");
  const loadingState = document.getElementById("loadingState");
  const operatorsTabCount = document.getElementById("operatorsTabCount");
  const favoritesTabCount = document.getElementById("favoritesTabCount");
  const listTabsNav = document.getElementById("operatorExplorerListTabs");
  const filterCountBadge = document.getElementById("filterCountBadge");

  let allOperators = [];
  let filteredOperators = [];
  let activeListTab = "operators";
  let goldMockPopupToken = "";
  let goldMockPopupTimeout = null;
  /** Legend-only chain scale filter (normalized, e.g. "luxury", "upper upscale"). */
  let selectedChainScaleNorm = "";

  /** Canonical Asset Type labels (case study hotel_type) — keep in sync with lib/operator-explorer/operator-case-study-hotel-type-normalize.js */
  var OPERATOR_ASSET_TYPE_OPTIONS = [
    "Full-Service",
    "Select-Service",
    "Resort",
    "Boutique",
    "Lifestyle",
  ];

  function mergeFilterOptionLists(catalog, observed) {
    var seen = {};
    (catalog || []).concat(observed || []).forEach(function (v) {
      var s = String(v || "").trim();
      if (s) seen[s] = true;
    });
    return Object.keys(seen).sort();
  }

  function normChainScaleLabel(s) {
    return String(s || "")
      .toLowerCase()
      .replace(/\s*chain\s*$/i, "")
      .trim();
  }

  function splitCsv(input) {
    if (!input) return [];
    if (Array.isArray(input)) {
      return input
        .map(function (s) {
          return String(s || "").trim();
        })
        .filter(Boolean);
    }
    return String(input)
      .split(",")
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  /** Prefer Platform chainScale; fall back / union with Profile chainScalesSupported for list stripe + legend. */
  function mergeChainScalesForDisplay(platformScales, supportedScales) {
    var out = [];
    var seen = {};
    function push(s) {
      var t = String(s || "").trim();
      if (!t) return;
      var key = normChainScaleLabel(t);
      if (!key || seen[key]) return;
      seen[key] = true;
      out.push(t);
    }
    (platformScales || []).forEach(push);
    (supportedScales || []).forEach(push);
    return out;
  }

  function normalizeOperator(row) {
    const companyName = row.companyName || row.operator_name || "Unknown Operator";
    const regions = splitCsv(row.regionsSupported || row.geography || "");
    const brands = splitCsv(row.brandsManaged || "");
    const serviceModels = splitCsv(row.primaryServiceModel || "");
    const serviceModelsSupported = splitCsv(row.serviceModelsSupported || "");
    const chainScalesSupported = splitCsv(row.chainScalesSupported || "");
    const scales = mergeChainScalesForDisplay(splitCsv(row.chainScale || ""), chainScalesSupported);
    const activeCountries = splitCsv(row.activeCountries || "");
    const brandedResidentialCapable = row.brandedResidentialCapable === true;
    const caseStudies = Array.isArray(row.caseStudiesDetail) ? row.caseStudiesDetail : [];
    const assetClasses = [
      ...new Set(
        caseStudies
          .map(function (cs) {
            return cs && cs.hotel_type ? String(cs.hotel_type).trim() : "";
          })
          .filter(Boolean)
      ),
    ];
    const situations = [
      ...new Set(
        caseStudies
          .map(function (cs) {
            return cs && cs.situation ? String(cs.situation).trim() : "";
          })
          .filter(Boolean)
      ),
    ];
    const capabilityTags = splitCsv(row.companyDescription || "").slice(0, 4);
    const brandedExperience = brands.length > 0;
    const independentExperience = brands.some(function (b) {
      return /independent/i.test(b);
    });
    // Independent is brand affiliation (brandsManaged), not an STR chain scale.
    // Surface it on the card legend/stripe when brands include Independent.
    if (
      independentExperience &&
      !scales.some(function (s) {
        return normChainScaleLabel(s) === "independent";
      })
    ) {
      scales.push("Independent");
    }
    return {
      id: row.id,
      operator_name: companyName,
      isOwnerOperator: !!row.isOwnerOperator,
      companyType: row.companyType || "",
      normalizedCompanyType: row.normalizedCompanyType || "",
      workspaceAccess: Array.isArray(row.workspaceAccess) ? row.workspaceAccess : [],
      operatorExplorerEligible: row.operatorExplorerEligible === true,
      thirdPartyManagementAvailability: row.thirdPartyManagementAvailability || "",
      thirdPartyManagementAvailabilityStatus: row.thirdPartyManagementAvailabilityStatus || "",
      reviewBeforeOutreach: row.reviewBeforeOutreach === true,
      eligibilitySource: row.eligibilitySource || "",
      companyDisplayBadges: Array.isArray(row.companyDisplayBadges) ? row.companyDisplayBadges : [],
      logo_url: row.logo || row.logo_url || "",
      website: row.website || row.Website || "",
      overview_short: row.companyDescription || row.overview_short || "",
      overview_long: row.companyDescription || row.overview_long || "",
      hotels_managed_count: Number(row.totalProperties || row.hotels_managed_count || 0) || 0,
      rooms_managed_count: Number(row.totalRooms || row.rooms_managed_count || 0) || 0,
      geography: regions,
      asset_classes: assetClasses,
      chain_scales: scales,
      branded_experience: brandedExperience,
      independent_experience: independentExperience,
      operating_situations: situations,
      service_models: serviceModels,
      service_models_supported: serviceModelsSupported,
      chain_scales_supported: chainScalesSupported,
      active_countries: activeCountries,
      branded_residential_capable: brandedResidentialCapable,
      capability_tags: capabilityTags,
      brands_managed: brands,
      parent_company: row.primaryServiceModel || row.parent_company || "",
    };
  }

  function showLoading(show) {
    loadingState.classList.toggle("hidden", !show);
    resultsList.classList.toggle("hidden", show);
    if (show) emptyState.classList.add("hidden");
  }

  async function fetchOperators() {
    try {
      showLoading(true);
      const response = await fetch(__dealityApiUrl("/api/third-party-operators?activeOnly=1"));
      if (!response.ok) throw new Error("Failed to fetch operators");
      const data = await response.json();
      function rowIsActiveExplorer(r) {
        return String((r && r.dealStatus) || "")
          .trim()
          .toLowerCase() === "active";
      }
      allOperators = (data.operators || []).filter(rowIsActiveExplorer).map(normalizeOperator);
      filteredOperators = [...allOperators];

      populateRegionFilter(
        [...new Set(allOperators.flatMap(function (o) { return o.geography || []; }))].sort()
      );
      populateAssetTypeFilter(
        mergeFilterOptionLists(
          OPERATOR_ASSET_TYPE_OPTIONS,
          [...new Set(allOperators.flatMap(function (o) { return o.asset_classes || []; }))]
        )
      );
      populateExperienceTypeFilter(
        [...new Set(allOperators.flatMap(function (o) { return o.operating_situations || []; }))].sort()
      );

      var fo = data.filterOptions || {};
      populateSelectFilter(
        "serviceModelSupportedFilter",
        fo.serviceModelsSupported && fo.serviceModelsSupported.length
          ? fo.serviceModelsSupported.slice()
          : [...new Set(allOperators.flatMap(function (o) { return o.service_models_supported || []; }))].sort(),
        "All Models"
      );
      populateSelectFilter(
        "chainScalesSupportedFilter",
        fo.chainScalesSupported && fo.chainScalesSupported.length
          ? fo.chainScalesSupported.slice()
          : [...new Set(allOperators.flatMap(function (o) { return o.chain_scales_supported || []; }))].sort(),
        "All Scales"
      );
      populateSelectFilter(
        "activeCountryFilter",
        fo.activeCountries && fo.activeCountries.length
          ? fo.activeCountries.slice()
          : [...new Set(allOperators.flatMap(function (o) { return o.active_countries || []; }))].sort(),
        "All Countries"
      );

      updateTabCounts();
      updateChainScaleQuickFilterStates();
      var favReady =
        window.OperatorExplorerFavorites && window.OperatorExplorerFavorites.ready
          ? window.OperatorExplorerFavorites.ready()
          : Promise.resolve();
      favReady
        .then(function () {
          updateTabCounts();
          filterOperators();
          showLoading(false);
        })
        .catch(function () {
          filterOperators();
          showLoading(false);
        });
    } catch (error) {
      console.error("Error fetching operators:", error);
      showLoading(false);
      resultsList.classList.add("hidden");
      emptyState.innerHTML = "<h3>Error loading operators</h3><p>" + escapeHtml(error.message) + "</p>";
      emptyState.classList.remove("hidden");
    }
  }

  function populateRegionFilter(options) {
    const select = document.getElementById("regionFilter");
    if (!select) return;
    select.innerHTML = '<option value="">All Regions</option>';
    (options || []).forEach(function (r) {
      const opt = document.createElement("option");
      opt.value = r;
      opt.textContent = r;
      select.appendChild(opt);
    });
  }

  function populateAssetTypeFilter(options) {
    const select = document.getElementById("assetTypeFilter");
    if (!select) return;
    select.innerHTML = '<option value="">All Types</option>';
    (options || []).forEach(function (a) {
      const opt = document.createElement("option");
      opt.value = a;
      opt.textContent = a;
      select.appendChild(opt);
    });
  }

  function populateExperienceTypeFilter(options) {
    populateSelectFilter("experienceTypeFilter", options || [], "All");
  }

  function populateSelectFilter(id, options, placeholder) {
    const select = document.getElementById(id);
    if (!select) return;
    select.innerHTML = '<option value="">' + escapeHtml(placeholder || "All") + "</option>";
    (options || []).forEach(function (value) {
      const opt = document.createElement("option");
      opt.value = value;
      opt.textContent = value;
      select.appendChild(opt);
    });
  }

  function getChainScaleClass(chainScale) {
    if (!chainScale) return "";
    const scale = chainScale.toLowerCase();
    if (scale.includes("luxury")) return "chain-scale-luxury";
    if (scale.includes("upper upscale")) return "chain-scale-upper-upscale";
    if (scale.includes("upscale") && !scale.includes("upper")) return "chain-scale-upscale";
    if (scale.includes("upper midscale")) return "chain-scale-upper-midscale";
    if (scale.includes("midscale")) return "chain-scale-midscale";
    if (scale.includes("economy")) return "chain-scale-economy";
    if (scale.includes("independent")) return "chain-scale-independent";
    return "";
  }

  function getChainScaleColor(chainScale) {
    if (!chainScale) return null;
    const scale = String(chainScale).toLowerCase();
    if (scale.includes("luxury")) return "#d4af37";
    if (scale.includes("upper upscale")) return "#9b59b6";
    if (scale.includes("upscale") && !scale.includes("upper")) return "#3498db";
    if (scale.includes("upper midscale")) return "#2ecc71";
    if (scale.includes("midscale")) return "#1abc9c";
    if (scale.includes("economy")) return "#e67e22";
    if (scale.includes("independent")) return "#94a3b8";
    return null;
  }

  function getChainScaleStripeBackground(chainScales) {
    const colors = [];
    (chainScales || []).forEach(function (scale) {
      const color = getChainScaleColor(scale);
      if (color && colors.indexOf(color) === -1) colors.push(color);
    });
    if (colors.length === 0) return "var(--accent--primary-1)";
    if (colors.length === 1) return colors[0];
    const step = 100 / colors.length;
    const stops = colors
      .map(function (color, index) {
        const start = (index * step).toFixed(3);
        const end = ((index + 1) * step).toFixed(3);
        return color + " " + start + "%, " + color + " " + end + "%";
      })
      .join(", ");
    return "linear-gradient(to bottom, " + stops + ")";
  }

  function normalizeWebsiteUrl(url) {
    if (!url) return "";
    const raw = String(url).trim();
    if (!raw) return "";
    if (/^https?:\/\//i.test(raw)) return raw;
    return "https://" + raw;
  }

  function websiteLabel(url) {
    if (!url) return "";
    return String(url)
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "")
      .replace(/\/$/, "");
  }

  function operatorTypeLabel(op) {
    var uiLabels = window.DEALALITY_UI_LABELS;
    if (uiLabels && typeof uiLabels.formatOperatorExplorerTypeLabel === "function") {
      return uiLabels.formatOperatorExplorerTypeLabel(op);
    }
    if (op.isOwnerOperator || op.normalizedCompanyType === "OWNER_OPERATOR") {
      return "Hotel Owner - Operator";
    }
    return "3rd Party Operator";
  }

  function buildOperatorBadgesHtml(op) {
    var uiLabels = window.DEALALITY_UI_LABELS;
    var badges =
      uiLabels && typeof uiLabels.buildOperatorExplorerCardBadges === "function"
        ? uiLabels.buildOperatorExplorerCardBadges(op)
        : [];
    if (!badges.length) {
      if (op.isOwnerOperator) badges.push("Hotel Owner - Operator");
      var status = String(op.thirdPartyManagementAvailabilityStatus || "").trim();
      if (status && status !== "Unknown / Legacy") {
        badges.push("Third-Party Management: " + status);
      } else if (String(op.thirdPartyManagementAvailability || "").toLowerCase() === "yes") {
        badges.push("Third-Party Management: Yes");
      }
      if (op.reviewBeforeOutreach) badges.push("Review Availability Before Outreach");
    }
    // Always render the badges row so tile height stays aligned with Brand Explorer cards
    // (empty slot still reserves one line via CSS min-height).
    var tooltip =
      uiLabels && typeof uiLabels.getOperatorExplorerCardBadgesTooltip === "function"
        ? uiLabels.getOperatorExplorerCardBadgesTooltip()
        : "Owner-Operator means this company owns or controls hotel assets and also operates hotels. Availability for third-party management may vary by market and deal type.";
    return (
      '<div class="operator-card__badges" title="' +
      escapeHtml(badges.length ? tooltip : "") +
      '">' +
      badges
        .slice(0, 3)
        .map(function (b) {
          return '<span class="operator-card__badge">' + escapeHtml(b) + "</span>";
        })
        .join("") +
      "</div>"
    );
  }

  function createOperatorCard(op) {
    const card = document.createElement("div");
    const chainScales = (op.chain_scales || [])
      .map(function (s) {
        return String(s || "").trim();
      })
      .filter(Boolean);
    const primaryScale = chainScales[0] || "";
    card.className = "brand-card " + getChainScaleClass(primaryScale);
    card.style.borderLeftColor = "transparent";
    card.onclick = function () {
      viewOperator(op.id);
    };

    const summaryRaw = op.overview_short || op.overview_long || "Learn more about this operator.";
    const summary = summaryRaw.length > 160 ? summaryRaw.substring(0, 160) + "..." : summaryRaw;
    const initial = (op.operator_name || "O").charAt(0).toUpperCase();
    const typeLabel = operatorTypeLabel(op);
    const badgesHtml = buildOperatorBadgesHtml(op);
    const stripeBackground = getChainScaleStripeBackground(chainScales);
    const websiteUrl = normalizeWebsiteUrl(op.website);
    const websiteText = websiteLabel(websiteUrl);

    let logoHtml = '<div class="brand-card__logo">';
    if (op.logo_url && String(op.logo_url).startsWith("http")) {
      logoHtml +=
        '<img src="' +
        escapeHtml(op.logo_url) +
        '" alt="' +
        escapeHtml(op.operator_name) +
        '" loading="lazy" referrerpolicy="no-referrer" onerror="this.style.display=\'none\'; var s=this.nextElementSibling; if(s) s.style.display=\'flex\';" onload="this.classList.add(\'loaded\');">';
      logoHtml += '<span class="brand-card__logo-initial">' + escapeHtml(initial) + "</span>";
    } else {
      logoHtml += '<span class="brand-card__logo-initial">' + escapeHtml(initial) + "</span>";
    }
    logoHtml += "</div>";

    const favStarHtml = op.id
      ? '<button type="button" class="favorite-star" data-oe-operator-id="' +
        escapeHtml(op.id) +
        '" aria-label="Add to favorites" aria-pressed="false" title="Save to favorites">' +
        '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg></button>'
      : "";

    card.innerHTML =
      favStarHtml +
      '<div class="brand-card__scale-stripe" style="background:' +
      stripeBackground +
      ';"></div>' +
      '<div class="brand-card__header">' +
      logoHtml +
      '<div class="brand-card__info">' +
      '<div class="brand-card__name">' +
      escapeHtml(op.operator_name) +
      "</div>" +
      '<div class="brand-card__type">' +
      escapeHtml(typeLabel) +
      "</div>" +
      badgesHtml +
      "</div></div>" +
      '<div class="brand-card__description">' +
      escapeHtml(summary) +
      "</div>" +
      '<div class="brand-card__footer">' +
      (websiteUrl
        ? '<a href="' +
          escapeHtml(websiteUrl) +
          '" class="brand-card__website" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation();">' +
          escapeHtml(websiteText) +
          "</a>"
        : "<span></span>") +
      '<button type="button" class="brand-card__more-btn" data-operator-id="' +
      escapeHtml(op.id) +
      '">View Operator</button>' +
      "</div>";
    card.querySelector(".brand-card__more-btn").addEventListener("click", function (e) {
      e.stopPropagation();
      openGoldMockPopup(op.id);
    });
    if (window.OperatorExplorerFavorites && window.OperatorExplorerFavorites.wireCardStars) {
      window.OperatorExplorerFavorites.wireCardStars(card);
    }
    return card;
  }

  function favoriteIdsSet() {
    const ids = window.OperatorExplorerFavorites ? window.OperatorExplorerFavorites.load() : [];
    const set = {};
    ids.forEach(function (id) {
      set[id] = true;
    });
    return set;
  }

  function updateTabCounts() {
    if (operatorsTabCount) operatorsTabCount.textContent = String(allOperators.length);
    if (favoritesTabCount) {
      const favCount = window.OperatorExplorerFavorites
        ? window.OperatorExplorerFavorites.load().length
        : 0;
      favoritesTabCount.textContent = String(favCount);
    }
  }

  function setActiveListTab(tab) {
    activeListTab = tab === "favorites" ? "favorites" : "operators";
    if (listTabsNav) {
      listTabsNav.querySelectorAll(".section-nav-item[data-tab]").forEach(function (btn) {
        const isActive = btn.getAttribute("data-tab") === activeListTab;
        btn.classList.toggle("active", isActive);
        btn.setAttribute("aria-selected", isActive ? "true" : "false");
      });
    }
    filterOperators();
  }

  function sortOperators() {
    const [field, dir] = sortSelect.value.split("-");
    filteredOperators.sort(function (a, b) {
      if (field === "hotels" || field === "rooms") {
        const aVal = field === "hotels" ? a.hotels_managed_count || 0 : a.rooms_managed_count || 0;
        const bVal = field === "hotels" ? b.hotels_managed_count || 0 : b.rooms_managed_count || 0;
        return dir === "asc" ? aVal - bVal : bVal - aVal;
      }
      const aVal = (a.operator_name || "").toLowerCase();
      const bVal = (b.operator_name || "").toLowerCase();
      if (aVal < bVal) return dir === "asc" ? -1 : 1;
      if (aVal > bVal) return dir === "asc" ? 1 : -1;
      return 0;
    });
  }

  function renderOperators() {
    resultsList.innerHTML = "";
    const poolTotal =
      activeListTab === "favorites"
        ? window.OperatorExplorerFavorites
          ? window.OperatorExplorerFavorites.load().length
          : 0
        : allOperators.length;
    if (filteredOperators.length === 0) {
      resultsList.classList.add("hidden");
      emptyState.classList.remove("hidden");
      if (activeListTab === "favorites") {
        emptyState.innerHTML =
          poolTotal === 0
            ? "<h3>No favorites yet</h3><p>Click the star on an operator card or <strong>Save</strong> on its profile to add it here.</p>"
            : "<h3>No operators found</h3><p>No saved operators match these filters. Clear filters or try a different search term.</p>";
      } else {
        emptyState.innerHTML =
          "<h3>No operators found</h3><p>No operators match these filters. Clear filters or try a different search term.</p>";
      }
      resultsCount.innerHTML =
        "Showing <strong>0</strong> of <strong>" +
        poolTotal +
        "</strong> " +
        (activeListTab === "favorites" ? "favorites" : "operators");
      return;
    }
    resultsList.classList.remove("hidden");
    emptyState.classList.add("hidden");
    filteredOperators.forEach(function (op) {
      resultsList.appendChild(createOperatorCard(op));
    });
    if (window.OperatorExplorerFavorites && window.OperatorExplorerFavorites.wireCardStars) {
      window.OperatorExplorerFavorites.wireCardStars(resultsList);
    }
    resultsCount.innerHTML =
      "Showing <strong>" +
      filteredOperators.length +
      "</strong> of <strong>" +
      poolTotal +
      "</strong> " +
      (activeListTab === "favorites" ? "favorites" : "operators");
  }

  function getActiveFilterCount() {
    let n = 0;
    if ((document.getElementById("searchInput").value || "").trim()) n++;
    if (document.getElementById("regionFilter").value) n++;
    if (document.getElementById("assetTypeFilter").value) n++;
    if (selectedChainScaleNorm) n++;
    if (document.getElementById("brandedIndependentFilter").value) n++;
    if (document.getElementById("experienceTypeFilter").value) n++;
    if (document.getElementById("serviceModelSupportedFilter").value) n++;
    if (document.getElementById("brandedResidentialFilter").value) n++;
    if (document.getElementById("chainScalesSupportedFilter").value) n++;
    if (document.getElementById("activeCountryFilter").value) n++;
    return n;
  }

  function updateFilterCountBadge() {
    const count = getActiveFilterCount();
    filterCountBadge.textContent = String(count);
    filterCountBadge.style.display = count > 0 ? "inline-flex" : "none";
    if (window.__operatorExplorerFilterDrawer && window.__operatorExplorerFilterDrawer.updateBadge) {
      window.__operatorExplorerFilterDrawer.updateBadge();
    }
  }

  function updateChainScaleQuickFilterActive() {
    const value = selectedChainScaleNorm;
    document.querySelectorAll(".chain-scale-legend-item").forEach(function (btn) {
      const scale = (btn.getAttribute("data-scale") || "").toLowerCase();
      const active = (scale === "all" && !value) || (scale && value && scale === value);
      btn.classList.toggle("active", !!active);
    });
  }

  function updateChainScaleQuickFilterStates() {
    document.querySelectorAll(".chain-scale-legend-item[data-scale]").forEach(function (btn) {
      const scale = (btn.getAttribute("data-scale") || "").toLowerCase();
      if (scale === "all") return;
      let count = 0;
      for (let i = 0; i < allOperators.length; i++) {
        const op = allOperators[i];
        if ((op.chain_scales || []).some(function (s) { return normChainScaleLabel(s) === scale; })) count++;
      }
      btn.classList.toggle("no-records", count === 0);
      if (count === 0) btn.setAttribute("aria-disabled", "true");
      else btn.removeAttribute("aria-disabled");
    });
    updateChainScaleQuickFilterActive();
  }

  function applyChainScaleQuickFilter(scale) {
    const norm = scale === "all" || !scale ? "" : String(scale).toLowerCase();
    const btn = document.querySelector('.chain-scale-legend-item[data-scale="' + (scale || "all") + '"]');
    if (btn && btn.classList.contains("no-records")) return;
    selectedChainScaleNorm = norm;
    filterOperators();
  }

  function filterOperators() {
    const searchText = (document.getElementById("searchInput").value || "").toLowerCase().trim();
    const region = document.getElementById("regionFilter").value;
    const assetType = document.getElementById("assetTypeFilter").value;
    const brandedIndependent = document.getElementById("brandedIndependentFilter").value;
    const experienceType = document.getElementById("experienceTypeFilter").value;
    const serviceModelSupported = document.getElementById("serviceModelSupportedFilter").value;
    const brandedResidential = document.getElementById("brandedResidentialFilter").value;
    const chainScalesSupported = document.getElementById("chainScalesSupportedFilter").value;
    const activeCountry = document.getElementById("activeCountryFilter").value;
    const favSet = activeListTab === "favorites" ? favoriteIdsSet() : null;

    filteredOperators = allOperators.filter(function (op) {
      if (favSet && !favSet[op.id]) return false;
      const searchable = [
        op.operator_name,
        (op.geography || []).join(" "),
        (op.brands_managed || []).join(" "),
        (op.chain_scales || []).join(" "),
        (op.active_countries || []).join(" "),
        (op.service_models_supported || []).join(" "),
        op.overview_short,
      ]
        .join(" ")
        .toLowerCase();
      if (searchText && !searchable.includes(searchText)) return false;
      if (
        region &&
        !(op.geography || []).some(function (g) {
          return String(g).toLowerCase().includes(region.toLowerCase());
        })
      )
        return false;
      if (assetType && !(op.asset_classes || []).includes(assetType)) return false;
      if (
        selectedChainScaleNorm &&
        !(op.chain_scales || []).some(function (s) {
          return normChainScaleLabel(s) === selectedChainScaleNorm;
        })
      )
        return false;
      if (brandedIndependent === "Branded" && !op.branded_experience) return false;
      if (brandedIndependent === "Independent" && !op.independent_experience) return false;
      if (
        experienceType &&
        !(op.operating_situations || []).some(function (s) {
          return String(s).toLowerCase() === experienceType.toLowerCase();
        })
      )
        return false;
      if (
        serviceModelSupported &&
        !(op.service_models_supported || []).some(function (s) {
          return String(s).toLowerCase() === serviceModelSupported.toLowerCase();
        })
      )
        return false;
      if (brandedResidential === "Yes" && !op.branded_residential_capable) return false;
      if (brandedResidential === "No" && op.branded_residential_capable) return false;
      if (
        chainScalesSupported &&
        !(op.chain_scales_supported || []).some(function (s) {
          return String(s).toLowerCase() === chainScalesSupported.toLowerCase();
        }) &&
        !(op.chain_scales || []).some(function (s) {
          return String(s).toLowerCase() === chainScalesSupported.toLowerCase();
        })
      )
        return false;
      if (
        activeCountry &&
        !(op.active_countries || []).some(function (c) {
          return String(c).toLowerCase() === activeCountry.toLowerCase();
        })
      )
        return false;
      return true;
    });

    sortOperators();
    renderOperators();
    updateFilterCountBadge();
    updateChainScaleQuickFilterActive();
  }

  function clearFilters() {
    document.getElementById("searchInput").value = "";
    document.getElementById("regionFilter").value = "";
    document.getElementById("assetTypeFilter").value = "";
    selectedChainScaleNorm = "";
    document.getElementById("brandedIndependentFilter").value = "";
    document.getElementById("experienceTypeFilter").value = "";
    document.getElementById("serviceModelSupportedFilter").value = "";
    document.getElementById("brandedResidentialFilter").value = "";
    document.getElementById("chainScalesSupportedFilter").value = "";
    document.getElementById("activeCountryFilter").value = "";
    sortSelect.value = "name-asc";
    filterOperators();
  }

  function toggleSortDirection() {
    const map = {
      "name-asc": "name-desc",
      "name-desc": "name-asc",
      "hotels-desc": "hotels-asc",
      "hotels-asc": "hotels-desc",
      "rooms-desc": "rooms-asc",
      "rooms-asc": "rooms-desc",
    };
    sortSelect.value = map[sortSelect.value] || "name-asc";
    filterOperators();
  }

  function escapeHtml(text) {
    if (!text) return "";
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
  }

  function closeGoldMockPopup() {
    var popup = document.getElementById("goldMockPopup");
    var frame = document.getElementById("goldMockPopupFrame");
    var loading = document.getElementById("goldMockPopupLoading");
    var loadingTitle = document.getElementById("goldMockPopupLoadingTitle");
    if (popup) {
      popup.style.display = "none";
      popup.setAttribute("aria-hidden", "true");
      document.body.style.overflow = "";
    }
    if (goldMockPopupTimeout) {
      clearTimeout(goldMockPopupTimeout);
      goldMockPopupTimeout = null;
    }
    goldMockPopupToken = "";
    if (loading) {
      loading.hidden = true;
      loading.classList.remove("is-error");
    }
    if (loadingTitle) loadingTitle.textContent = "Loading Operator Profile…";
    if (frame) {
      frame.classList.remove("is-ready");
      frame.src = "about:blank";
    }
  }

  function openGoldMockPopup(id) {
    if (!id || String(id).indexOf("rec") !== 0) return;
    var popup = document.getElementById("goldMockPopup");
    var frame = document.getElementById("goldMockPopupFrame");
    var loading = document.getElementById("goldMockPopupLoading");
    var loadingTitle = document.getElementById("goldMockPopupLoadingTitle");
    if (!popup || !frame) return;

    if (goldMockPopupTimeout) {
      clearTimeout(goldMockPopupTimeout);
      goldMockPopupTimeout = null;
    }

    goldMockPopupToken = Date.now().toString(36) + "-" + Math.random().toString(36).slice(2);
    frame.classList.remove("is-ready");
    if (loading) {
      loading.hidden = false;
      loading.classList.remove("is-error");
    }
    if (loadingTitle) loadingTitle.textContent = "Loading Operator Profile…";
    var dealIdParam = (new URLSearchParams(window.location.search || "")).get("dealId") || "";
    var profileUrl =
      "/operator-explorer-gold-mock.html?id=" +
      encodeURIComponent(id) +
      "&embed=1&popupToken=" +
      encodeURIComponent(goldMockPopupToken);
    if (dealIdParam) profileUrl += "&dealId=" + encodeURIComponent(dealIdParam);
    frame.src = profileUrl;

    popup.style.display = "flex";
    popup.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
  }

  function viewOperator(id) {
    openGoldMockPopup(id);
  }

  document.getElementById("goldMockPopupClose").addEventListener("click", closeGoldMockPopup);
  document.getElementById("goldMockPopupOverlay").addEventListener("click", closeGoldMockPopup);
  window.addEventListener("message", function (event) {
    if (!event || event.origin !== window.location.origin) return;
    var data = event.data || {};
    if (data.type !== "operator-gold-mock-ready") return;
    if (!goldMockPopupToken || data.popupToken !== goldMockPopupToken) return;
    var frame = document.getElementById("goldMockPopupFrame");
    var loading = document.getElementById("goldMockPopupLoading");
    var loadingTitle = document.getElementById("goldMockPopupLoadingTitle");
    if (!frame) return;

    if (goldMockPopupTimeout) {
      clearTimeout(goldMockPopupTimeout);
      goldMockPopupTimeout = null;
    }

    if (data.live === true) {
      frame.classList.add("is-ready");
      if (loading) loading.hidden = true;
      return;
    }

    frame.classList.remove("is-ready");
    if (loading) {
      loading.hidden = false;
      loading.classList.add("is-error");
    }
    if (loadingTitle) {
      loadingTitle.textContent =
        "Live profile unavailable. This operator did not return live profile data.";
    }
  });
  document.addEventListener("keydown", function (e) {
    if (e.key !== "Escape") return;
    var p = document.getElementById("goldMockPopup");
    if (p && p.style.display === "flex") closeGoldMockPopup();
  });

  document.getElementById("resetFiltersBtn").addEventListener("click", clearFilters);
  document.getElementById("sortIconBtn").addEventListener("click", toggleSortDirection);
  document.getElementById("searchInput").addEventListener("input", function () {
    clearTimeout(this._t);
    this._t = setTimeout(filterOperators, 200);
  });
  document.getElementById("regionFilter").addEventListener("change", filterOperators);
  [
    "assetTypeFilter",
    "brandedIndependentFilter",
    "experienceTypeFilter",
    "serviceModelSupportedFilter",
    "brandedResidentialFilter",
    "chainScalesSupportedFilter",
    "activeCountryFilter",
  ].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.addEventListener("change", filterOperators);
  });
  sortSelect.addEventListener("change", filterOperators);
  document.getElementById("chainScaleLegend").addEventListener("click", function (e) {
    const btn = e.target && e.target.closest && e.target.closest(".chain-scale-legend-item");
    if (!btn) return;
    applyChainScaleQuickFilter(btn.getAttribute("data-scale") || "all");
  });

  if (listTabsNav) {
    listTabsNav.addEventListener("click", function (e) {
      const btn = e.target && e.target.closest && e.target.closest(".section-nav-item[data-tab]");
      if (!btn || !listTabsNav.contains(btn)) return;
      const tab = btn.getAttribute("data-tab");
      if (tab === "operators" || tab === "favorites") setActiveListTab(tab);
    });
  }

  window.addEventListener("operator-explorer-favorites-changed", function () {
    updateTabCounts();
    if (window.OperatorExplorerFavorites && window.OperatorExplorerFavorites.wireCardStars && resultsList) {
      window.OperatorExplorerFavorites.wireCardStars(resultsList);
    }
    if (activeListTab === "favorites") filterOperators();
  });

  if (window.OperatorExplorerFavorites && window.OperatorExplorerFavorites.ready) {
    void window.OperatorExplorerFavorites.ready();
  }

  function initOperatorExplorerFilterDrawer() {
    if (window.__operatorExplorerFilterDrawer || !window.ExplorerFilterDrawer) return;
    window.__operatorExplorerFilterDrawer = window.ExplorerFilterDrawer.init({
      overlayId: "operatorExplorerFilterDrawerOverlay",
      drawerId: "operatorExplorerFilterDrawer",
      openBtnId: "operatorExplorerFilterOpenBtn",
      closeBtnId: "operatorExplorerFilterDrawerClose",
      doneBtnId: "operatorExplorerFilterDoneBtn",
      resetBtnId: "operatorExplorerFilterResetBtn",
      drawerBadgeId: "operatorExplorerDrawerFilterBadge",
      mainBadgeId: "filterCountBadge",
      filterIds: [
        "serviceModelSupportedFilter",
        "brandedResidentialFilter",
        "chainScalesSupportedFilter",
        "activeCountryFilter",
      ],
      onFilterChange: filterOperators,
      onReset: clearFilters,
      countActiveFilters: getActiveFilterCount,
    });
  }

  initOperatorExplorerFilterDrawer();
  window.addEventListener("load", initOperatorExplorerFilterDrawer);

  fetchOperators();
});
