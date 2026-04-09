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
  const filterCountBadge = document.getElementById("filterCountBadge");

  let allOperators = [];
  let filteredOperators = [];
  /** Legend-only chain scale filter (normalized, e.g. "luxury", "upper upscale"). */
  let selectedChainScaleNorm = "";

  function normChainScaleLabel(s) {
    return String(s || "")
      .toLowerCase()
      .replace(/\s*chain\s*$/i, "")
      .trim();
  }

  function splitCsv(input) {
    if (!input) return [];
    return String(input)
      .split(",")
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  function normalizeOperator(row) {
    const companyName = row.companyName || row.operator_name || "Unknown Operator";
    const regions = splitCsv(row.regionsSupported || row.geography || "");
    const scales = splitCsv(row.chainScale || "");
    const brands = splitCsv(row.brandsManaged || "");
    const serviceModels = splitCsv(row.primaryServiceModel || "");
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
    return {
      id: row.id,
      operator_name: companyName,
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
        [...new Set(allOperators.flatMap(function (o) { return o.asset_classes || []; }))].sort()
      );
      populateExperienceTypeFilter(
        [...new Set(allOperators.flatMap(function (o) { return o.operating_situations || []; }))].sort()
      );
      operatorsTabCount.textContent = String(allOperators.length);
      updateChainScaleQuickFilterStates();
      filterOperators();
      showLoading(false);
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
    const select = document.getElementById("experienceTypeFilter");
    if (!select) return;
    select.innerHTML = '<option value="">All</option>';
    (options || []).forEach(function (e) {
      const opt = document.createElement("option");
      opt.value = e;
      opt.textContent = e;
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
      .replace(/\/$/, "");
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
    const typeLabel = "3rd Party Operator";
    const regionText = (op.geography || []).slice(0, 2).join(", ").toUpperCase();
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

    card.innerHTML =
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
      '<div class="brand-card__meta">' +
      escapeHtml(regionText) +
      "</div>" +
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
    return card;
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
    if (filteredOperators.length === 0) {
      resultsList.classList.add("hidden");
      emptyState.classList.remove("hidden");
      resultsCount.innerHTML =
        "Showing <strong>0</strong> of <strong>" + allOperators.length + "</strong> operators";
      return;
    }
    resultsList.classList.remove("hidden");
    emptyState.classList.add("hidden");
    filteredOperators.forEach(function (op) {
      resultsList.appendChild(createOperatorCard(op));
    });
    resultsCount.innerHTML =
      "Showing <strong>" +
      filteredOperators.length +
      "</strong> of <strong>" +
      allOperators.length +
      "</strong> operators";
  }

  function getActiveFilterCount() {
    let n = 0;
    if ((document.getElementById("searchInput").value || "").trim()) n++;
    if (document.getElementById("regionFilter").value) n++;
    if (document.getElementById("assetTypeFilter").value) n++;
    if (selectedChainScaleNorm) n++;
    if (document.getElementById("brandedIndependentFilter").value) n++;
    if (document.getElementById("experienceTypeFilter").value) n++;
    return n;
  }

  function updateFilterCountBadge() {
    const count = getActiveFilterCount();
    filterCountBadge.textContent = String(count);
    filterCountBadge.style.display = count > 0 ? "inline-flex" : "none";
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

    filteredOperators = allOperators.filter(function (op) {
      const searchable = [
        op.operator_name,
        (op.geography || []).join(" "),
        (op.brands_managed || []).join(" "),
        (op.chain_scales || []).join(" "),
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
    var popupLoading = document.getElementById("goldMockPopupLoading");
    if (popup && document.activeElement && popup.contains(document.activeElement)) {
      document.activeElement.blur();
    }
    if (popup) {
      popup.style.display = "none";
      popup.setAttribute("aria-hidden", "true");
      document.body.style.overflow = "";
    }
    if (popupLoading) popupLoading.style.display = "none";
    if (frame) {
      frame.style.visibility = "hidden";
      frame.src = "about:blank";
    }
  }

  function ensureGoldMockPopupLoading() {
    var panel = document.querySelector("#goldMockPopup .gold-mock-popup-panel");
    var frame = document.getElementById("goldMockPopupFrame");
    if (!panel || !frame) return null;

    var existing = document.getElementById("goldMockPopupLoading");
    if (existing) return existing;

    var loading = document.createElement("div");
    loading.className = "gold-mock-popup-loading";
    loading.id = "goldMockPopupLoading";
    loading.setAttribute("aria-live", "polite");
    loading.innerHTML =
      '<div class="loading">' +
      '  <div class="loading-content">' +
      '    <div class="wave-container">' +
      '      <div class="wave wave-1"></div>' +
      '      <div class="wave wave-2"></div>' +
      '      <div class="wave wave-3"></div>' +
      "    </div>" +
      "    <div>" +
      '      <div class="loading-text-main">Loading operator...</div>' +
      '      <div class="loading-text-time">Estimated time: 2-6 seconds</div>' +
      "    </div>" +
      "  </div>" +
      '  <div class="loading-progress"><div class="loading-progress-bar"></div></div>' +
      "</div>";
    panel.insertBefore(loading, frame);
    return loading;
  }

  function openGoldMockPopup(id) {
    var popup = document.getElementById("goldMockPopup");
    var frame = document.getElementById("goldMockPopupFrame");
    var popupLoading = ensureGoldMockPopupLoading();
    if (!popup || !frame) return;
    if (popupLoading) popupLoading.style.display = "flex";
    frame.style.visibility = "hidden";
    frame.src =
      __dealityApiUrl("/operator-explorer-gold-mock.html") + "?id=" + encodeURIComponent(id);
    popup.style.display = "flex";
    popup.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
  }

  function viewOperator(id) {
    openGoldMockPopup(id);
  }

  document.getElementById("goldMockPopupClose").addEventListener("click", closeGoldMockPopup);
  document.getElementById("goldMockPopupOverlay").addEventListener("click", closeGoldMockPopup);
  document.getElementById("goldMockPopupFrame").addEventListener("load", function () {
    // Keep loader visible until inner page sends readiness message.
  });
  window.addEventListener("message", function (e) {
    var frame = document.getElementById("goldMockPopupFrame");
    var popup = document.getElementById("goldMockPopup");
    var popupLoading = document.getElementById("goldMockPopupLoading");
    if (!frame || !popup || !popupLoading) return;
    if (e.source !== frame.contentWindow) return;
    if (!e.data || e.data.type !== "operator-gold-mock-ready") return;
    if (popup.style.display !== "flex") return;
    popupLoading.style.display = "none";
    frame.style.visibility = "visible";
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
  document.getElementById("assetTypeFilter").addEventListener("change", filterOperators);
  document.getElementById("brandedIndependentFilter").addEventListener("change", filterOperators);
  document.getElementById("experienceTypeFilter").addEventListener("change", filterOperators);
  sortSelect.addEventListener("change", filterOperators);
  document.getElementById("chainScaleLegend").addEventListener("click", function (e) {
    const btn = e.target && e.target.closest && e.target.closest(".chain-scale-legend-item");
    if (!btn) return;
    applyChainScaleQuickFilter(btn.getAttribute("data-scale") || "all");
  });

  fetchOperators();
});
