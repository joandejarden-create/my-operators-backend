/**
 * Capital Provider Explorer — directory (Operator Explorer card/filter parity).
 */
(function (global) {
  "use strict";

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

(function () {
  "use strict";

  var allProviders = [];
  var filteredProviders = [];
  var selectedInstitutionType = "";
  var cpeProfilePopupToken = "";
  var activeListTab = "providers";
  var providersTabCount = document.getElementById("cpeProvidersTabCount");
  var favoritesTabCount = document.getElementById("cpeFavoritesTabCount");
  var listTabsNav = document.getElementById("cpeListTabs");

  var INSTITUTION_STRIPE = {
    bank: "#3498db",
    "regional bank": "#1abc9c",
    "national bank": "#2e86de",
    "development finance institution": "#16a085",
    "multilateral institution": "#8e44ad",
    "export credit / government finance": "#e67e22",
    "commercial bank": "#3498db",
    "debt fund": "#9b59b6",
    "cmbs lender": "#d4af37",
    "life company": "#2ecc71",
    "hud / agency lender": "#e67e22",
    "private credit": "#e74c3c",
    "mezzanine lender": "#a569bd",
    "investment bank": "#5dade2",
    "family office": "#48c9b0",
    "capital advisor": "#7f8c8d",
  };

  var INSTITUTION_SWATCH_CLASS = {
    bank: "bank",
    "regional bank": "regional-bank",
    "national bank": "national-bank",
    "development finance institution": "dfi",
    "multilateral institution": "multilateral",
    "export credit / government finance": "export-credit",
    "commercial bank": "commercial-bank",
    "debt fund": "debt-fund",
    "cmbs lender": "cmbs-lender",
    "life company": "life-company",
    "hud / agency lender": "hud-agency",
    "private credit": "private-credit",
    "mezzanine lender": "mezzanine",
    "investment bank": "investment-bank",
    "family office": "family-office",
    "capital advisor": "capital-advisor",
  };

  function escapeHtml(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function apiUrl(path) {
    if (typeof window.__dealityApiUrl === "function") {
      return window.__dealityApiUrl(path);
    }
    return path.charAt(0) === "/" ? path : "/" + path;
  }

  function normInstitution(s) {
    return String(s || "").trim().toLowerCase();
  }

  function institutionStripe(type) {
    return INSTITUTION_STRIPE[normInstitution(type)] || "#6c72ff";
  }

  function resolveLogoUrl(p) {
    var logoUrl = p.logoUrl && String(p.logoUrl).trim();
    if (logoUrl && /^https?:\/\//i.test(logoUrl)) return logoUrl;
    var websiteUrl = normalizeWebsiteUrl(p.website);
    if (!websiteUrl) return "";
    try {
      var host = new URL(websiteUrl).hostname.replace(/^www\./i, "");
      if (!host) return "";
      return (
        "https://www.google.com/s2/favicons?domain=" + encodeURIComponent(host) + "&sz=128"
      );
    } catch (e) {
      return "";
    }
  }

  function populateSelect(id, options, allLabel) {
    var el = document.getElementById(id);
    if (!el) return;
    var current = el.value;
    el.innerHTML = "<option value=\"\">" + escapeHtml(allLabel || "All") + "</option>";
    (options || []).forEach(function (opt) {
      var o = document.createElement("option");
      o.value = opt;
      o.textContent = opt;
      el.appendChild(o);
    });
    if (current && Array.from(el.options).some(function (o) { return o.value === current; })) {
      el.value = current;
    }
  }

  function buildInstitutionLegend(types) {
    var legend = document.getElementById("cpeInstitutionLegend");
    if (!legend) return;
    legend.querySelectorAll(".chain-scale-legend-item[data-institution]:not([data-institution='all'])").forEach(function (n) {
      n.remove();
    });
    (types || []).forEach(function (type) {
      var norm = normInstitution(type);
      var swatchKey = INSTITUTION_SWATCH_CLASS[norm] || "default";
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "chain-scale-legend-item";
      btn.setAttribute("data-institution", type);
      btn.innerHTML =
        '<div class="chain-scale-legend-swatch chain-scale-legend-swatch--' +
        swatchKey +
        '"></div><span class="chain-scale-legend-label">' +
        escapeHtml(type) +
        "</span>";
      legend.appendChild(btn);
    });
    legend.querySelectorAll(".chain-scale-legend-item[data-institution]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var val = btn.getAttribute("data-institution");
        selectedInstitutionType = val === "all" ? "" : val;
        legend.querySelectorAll(".chain-scale-legend-item").forEach(function (b) {
          b.classList.toggle("active", b === btn);
        });
        var typeSelect = document.getElementById("cpeInstitutionType");
        if (typeSelect) typeSelect.value = selectedInstitutionType;
        applyFilters();
      });
    });
  }

  function getFilterValues() {
    return {
      search: (document.getElementById("cpeSearch").value || "").trim().toLowerCase(),
      institutionType: selectedInstitutionType,
      geography: document.getElementById("cpeGeography").value,
      loanProduct: document.getElementById("cpeLoanProduct").value,
      loanSize: document.getElementById("cpeLoanSize").value,
      assetType: document.getElementById("cpeAssetType").value,
      projectStage: document.getElementById("cpeProjectStage").value,
    };
  }

  function countActiveFilters(f) {
    var n = 0;
    if (f.search) n++;
    if (f.institutionType) n++;
    if (f.geography) n++;
    if (f.loanProduct) n++;
    if (f.loanSize) n++;
    if (f.assetType) n++;
    if (f.projectStage) n++;
    return n;
  }

  function matchesList(arr, value) {
    if (!value) return true;
    return (arr || []).some(function (item) {
      return String(item).toLowerCase() === String(value).toLowerCase();
    });
  }

  function showLoading(show) {
    document.getElementById("cpeLoading").classList.toggle("hidden", !show);
    document.getElementById("cpeGrid").classList.toggle("hidden", show);
    if (show) {
      document.getElementById("cpeEmpty").classList.add("hidden");
      document.getElementById("cpeError").classList.add("hidden");
    }
  }

  function favoriteIdsSet() {
    var ids = window.CapitalExplorerFavorites ? window.CapitalExplorerFavorites.load() : [];
    var set = {};
    ids.forEach(function (id) {
      set[id] = true;
    });
    return set;
  }

  function updateTabCounts() {
    if (providersTabCount) providersTabCount.textContent = String(allProviders.length);
    if (favoritesTabCount) {
      var favCount = window.CapitalExplorerFavorites
        ? window.CapitalExplorerFavorites.load().length
        : 0;
      favoritesTabCount.textContent = String(favCount);
    }
  }

  function setActiveListTab(tab) {
    activeListTab = tab === "favorites" ? "favorites" : "providers";
    if (listTabsNav) {
      listTabsNav.querySelectorAll(".section-nav-item[data-tab]").forEach(function (btn) {
        var isActive = btn.getAttribute("data-tab") === activeListTab;
        btn.classList.toggle("active", isActive);
        btn.setAttribute("aria-selected", isActive ? "true" : "false");
      });
    }
    applyFilters();
  }

  function applyFilters() {
    var f = getFilterValues();
    var favSet = activeListTab === "favorites" ? favoriteIdsSet() : null;
    filteredProviders = allProviders.filter(function (p) {
      if (favSet && !favSet[p.id]) return false;
      if (f.institutionType && p.institutionType !== f.institutionType) return false;
      if (f.geography && !matchesList(p.geographicCoverage, f.geography)) return false;
      if (f.loanProduct && !matchesList(p.loanProducts, f.loanProduct)) return false;
      if (f.loanSize && p.loanSizeRange !== f.loanSize) return false;
      if (f.assetType && !matchesList(p.preferredAssetTypes, f.assetType)) return false;
      if (f.projectStage && !matchesList(p.projectStages, f.projectStage)) return false;
      if (f.search) {
        var hay = [
          p.name,
          p.institutionType,
          p.hotelLendingFocus,
          (p.geographicCoverage || []).join(" "),
          (p.loanProducts || []).join(" "),
          (p.preferredAssetTypes || []).join(" "),
          p.contactPathway,
          p.loanSizeLabel,
        ]
          .join(" ")
          .toLowerCase();
        if (hay.indexOf(f.search) === -1) return false;
      }
      return true;
    });

    var sort = document.getElementById("cpeSort").value || "name-asc";
    filteredProviders.sort(function (a, b) {
      var av = (a.name || "").toLowerCase();
      var bv = (b.name || "").toLowerCase();
      if (av < bv) return sort === "name-desc" ? 1 : -1;
      if (av > bv) return sort === "name-desc" ? -1 : 1;
      return 0;
    });

    var badge = document.getElementById("cpeFilterBadge");
    var n = countActiveFilters(f);
    if (badge) {
      badge.textContent = String(n);
      badge.style.display = n > 0 ? "inline-flex" : "none";
    }

    renderGrid();
  }

  function profilePopupUrl(id, popupToken) {
    return (
      "/capital-provider-explorer-detail?id=" +
      encodeURIComponent(id) +
      "&embed=1&popupToken=" +
      encodeURIComponent(popupToken)
    );
  }

  function closeProviderProfilePopup() {
    var popup = document.getElementById("cpeProfilePopup");
    var frame = document.getElementById("cpeProfilePopupFrame");
    var loading = document.getElementById("cpeProfilePopupLoading");
    var loadingTitle = document.getElementById("cpeProfilePopupLoadingTitle");
    if (popup) {
      popup.style.display = "none";
      popup.setAttribute("aria-hidden", "true");
      document.body.style.overflow = "";
    }
    cpeProfilePopupToken = "";
    if (loading) {
      loading.hidden = true;
      loading.classList.remove("is-error");
    }
    if (loadingTitle) loadingTitle.textContent = "Loading Capital Explorer profile…";
    if (frame) {
      frame.classList.remove("is-ready");
      frame.src = "about:blank";
    }
  }

  function openProviderProfilePopup(id) {
    if (!id) return;
    var popup = document.getElementById("cpeProfilePopup");
    var frame = document.getElementById("cpeProfilePopupFrame");
    var loading = document.getElementById("cpeProfilePopupLoading");
    var loadingTitle = document.getElementById("cpeProfilePopupLoadingTitle");
    if (!popup || !frame) return;

    cpeProfilePopupToken = Date.now().toString(36) + "-" + Math.random().toString(36).slice(2);
    frame.classList.remove("is-ready");
    if (loading) {
      loading.hidden = false;
      loading.classList.remove("is-error");
    }
    if (loadingTitle) loadingTitle.textContent = "Loading Capital Explorer profile…";
    frame.src = profilePopupUrl(id, cpeProfilePopupToken);

    popup.style.display = "flex";
    popup.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
  }

  function viewProvider(id) {
    openProviderProfilePopup(id);
  }

  function wireProfilePopup() {
    var closeBtn = document.getElementById("cpeProfilePopupClose");
    var overlay = document.getElementById("cpeProfilePopupOverlay");
    if (closeBtn) closeBtn.addEventListener("click", closeProviderProfilePopup);
    if (overlay) overlay.addEventListener("click", closeProviderProfilePopup);

    window.addEventListener("message", function (event) {
      if (!event || event.origin !== window.location.origin) return;
      var data = event.data || {};
      if (data.type !== "capital-provider-profile-ready") return;
      if (!cpeProfilePopupToken || data.popupToken !== cpeProfilePopupToken) return;

      var frame = document.getElementById("cpeProfilePopupFrame");
      var loading = document.getElementById("cpeProfilePopupLoading");
      var loadingTitle = document.getElementById("cpeProfilePopupLoadingTitle");
      if (!frame) return;

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
        loadingTitle.textContent = "Unable to load this capital provider profile.";
      }
    });

    document.addEventListener("keydown", function (e) {
      if (e.key !== "Escape") return;
      var popup = document.getElementById("cpeProfilePopup");
      if (popup && popup.style.display === "flex") closeProviderProfilePopup();
    });
  }

  function normalizeWebsiteUrl(url) {
    if (!url) return "";
    var raw = String(url).trim();
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

  function buildLogoHtml(p, initial) {
    var logoUrl = resolveLogoUrl(p);
    var html = '<div class="brand-card__logo">';
    if (logoUrl) {
      html +=
        '<img src="' +
        escapeHtml(logoUrl) +
        '" alt="' +
        escapeHtml(p.name) +
        '" loading="lazy" referrerpolicy="no-referrer" onerror="this.style.display=\'none\'; var s=this.nextElementSibling; if(s) s.style.display=\'flex\';" onload="this.classList.add(\'loaded\');">';
      html += '<span class="brand-card__logo-initial">' + escapeHtml(initial) + "</span>";
    } else {
      html += '<span class="brand-card__logo-initial">' + escapeHtml(initial) + "</span>";
    }
    html += "</div>";
    return html;
  }

  function createProviderCard(p) {
    var card = document.createElement("div");
    card.className = "brand-card";
    card.style.borderLeftColor = "transparent";
    card.onclick = function () {
      viewProvider(p.id);
    };

    var initial = (p.name || "C").charAt(0).toUpperCase();
    var summaryRaw =
      p.shortDescription || p.hotelLendingFocus || "Learn more about this capital provider.";
    var summary = summaryRaw.length > 160 ? summaryRaw.substring(0, 160) + "..." : summaryRaw;
    var regionText = (p.geographicCoverage || []).slice(0, 2).join(", ").toUpperCase();
    var stripe = institutionStripe(p.institutionType);
    var websiteUrl = normalizeWebsiteUrl(p.website);
    var websiteText = websiteLabel(websiteUrl);
    var typeLabel = String(p.institutionType || "").toUpperCase();
    var favStarHtml = p.id
      ? '<button type="button" class="favorite-star" data-cpe-provider-id="' +
        escapeHtml(p.id) +
        '" data-cpe-provider-name="' +
        escapeHtml(p.name || "") +
        '" aria-label="Add to favorites" aria-pressed="false" title="Save to favorites">' +
        '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg></button>'
      : "";

    card.innerHTML =
      favStarHtml +
      '<div class="brand-card__scale-stripe" style="background:' +
      escapeHtml(stripe) +
      ';"></div>' +
      '<div class="brand-card__header">' +
      buildLogoHtml(p, initial) +
      '<div class="brand-card__info">' +
      '<div class="brand-card__name">' +
      escapeHtml(p.name) +
      "</div>" +
      '<div class="brand-card__type">' +
      escapeHtml(typeLabel) +
      "</div>" +
      '<div class="brand-card__meta">' +
      escapeHtml(regionText || "—") +
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
      '<button type="button" class="brand-card__more-btn" data-provider-id="' +
      escapeHtml(p.id) +
      '">View Profile</button>' +
      "</div>";

    card.querySelector(".brand-card__more-btn").addEventListener("click", function (e) {
      e.stopPropagation();
      viewProvider(p.id);
    });

    if (window.CapitalExplorerFavorites && window.CapitalExplorerFavorites.wireCardStars) {
      window.CapitalExplorerFavorites.wireCardStars(card);
    }

    return card;
  }

  function renderGrid() {
    var grid = document.getElementById("cpeGrid");
    var countEl = document.getElementById("cpeResultsCount");
    var emptyEl = document.getElementById("cpeEmpty");
    var poolTotal =
      activeListTab === "favorites"
        ? window.CapitalExplorerFavorites
          ? window.CapitalExplorerFavorites.load().length
          : 0
        : allProviders.length;
    var poolLabel =
      activeListTab === "favorites" ? "favorites" : "Capital Providers";

    if (!filteredProviders.length) {
      grid.classList.add("hidden");
      emptyEl.classList.remove("hidden");
      if (activeListTab === "favorites") {
        emptyEl.innerHTML =
          poolTotal === 0
            ? "<h3>No favorites yet</h3><p>Click the star on a provider card or <strong>Save to Financing List</strong> on its profile to add it here.</p>"
            : "<h3>No capital providers found</h3><p>No saved providers match these filters. Clear filters or try a different search term.</p>";
      } else {
        emptyEl.innerHTML =
          "<h3>No capital providers found</h3><p>No providers match these filters. Clear filters or try a different search term.</p>";
      }
      if (countEl) {
        countEl.innerHTML =
          "Showing <strong>0</strong> of <strong>" + poolTotal + "</strong> " + poolLabel;
      }
      return;
    }

    grid.classList.remove("hidden");
    emptyEl.classList.add("hidden");
    grid.innerHTML = "";
    filteredProviders.forEach(function (p) {
      grid.appendChild(createProviderCard(p));
    });

    if (countEl) {
      countEl.innerHTML =
        "Showing <strong>" +
        filteredProviders.length +
        "</strong> of <strong>" +
        poolTotal +
        "</strong> " +
        poolLabel;
    }
  }

  async function authHeaders() {
    var auth = window.DealalityMemberstackAuth;
    if (!auth || typeof auth.getMemberstackJwtWhenReady !== "function") return {};
    try {
      var jwt = await auth.getMemberstackJwtWhenReady(8000);
      if (jwt) return { Authorization: "Bearer " + jwt };
    } catch (e) {
      console.warn("[CapitalProviderExplorer] auth headers skipped:", e);
    }
    return {};
  }

  async function loadProviders() {
    showLoading(true);
    try {
      var headers = await authHeaders();
      var res = await fetch(apiUrl("/api/capital-provider-explorer/providers"), { headers: headers });
      var data = await res.json();
      if (!res.ok || !data.success) {
        var msg = data.error || data.message || "Failed to load providers";
        if (res.status === 404 && msg === "API route not found") {
          msg =
            "API route not found — restart the backend (npm start) so /api/capital-provider-explorer is registered.";
        }
        throw new Error(msg);
      }

      allProviders = data.providers || [];
      var opts = data.filterOptions || {};

      populateSelect("cpeGeography", opts.geographies, "All Regions");
      populateSelect("cpeLoanProduct", opts.loanProducts, "All Types");
      populateSelect("cpeLoanSize", opts.loanSizeRanges, "All Sizes");
      populateSelect("cpeAssetType", opts.assetTypes, "All Types");
      populateSelect("cpeProjectStage", opts.projectStages, "All Stages");
      buildInstitutionLegend(opts.institutionTypes);

      filteredProviders = allProviders.slice();
      updateTabCounts();
      showLoading(false);
      var favReady =
        window.CapitalExplorerFavorites && window.CapitalExplorerFavorites.ready
          ? window.CapitalExplorerFavorites.ready()
          : Promise.resolve();
      favReady
        .then(function () {
          updateTabCounts();
          applyFilters();
        })
        .catch(function () {
          applyFilters();
        });
    } catch (err) {
      console.error("[CapitalProviderExplorer] load failed:", err);
      showLoading(false);
      document.getElementById("cpeError").classList.remove("hidden");
      document.getElementById("cpeErrorMessage").textContent = err.message || "Unknown error";
    }
  }

  function wireEvents() {
    var filterIds = [
      "cpeSearch",
      "cpeGeography",
      "cpeLoanProduct",
      "cpeLoanSize",
      "cpeAssetType",
      "cpeProjectStage",
      "cpeSort",
    ];
    filterIds.forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.addEventListener(id === "cpeSearch" ? "input" : "change", applyFilters);
    });

    document.getElementById("cpeResetFilters").addEventListener("click", function () {
      document.getElementById("cpeSearch").value = "";
      selectedInstitutionType = "";
      [
        "cpeGeography",
        "cpeLoanProduct",
        "cpeLoanSize",
        "cpeAssetType",
        "cpeProjectStage",
      ].forEach(function (id) {
        var el = document.getElementById(id);
        if (el) el.value = "";
      });
      document.getElementById("cpeSort").value = "name-asc";
      var legend = document.getElementById("cpeInstitutionLegend");
      if (legend) {
        legend.querySelectorAll(".chain-scale-legend-item").forEach(function (b) {
          b.classList.toggle("active", b.getAttribute("data-institution") === "all");
        });
      }
      applyFilters();
    });

    document.getElementById("cpeRetry").addEventListener("click", loadProviders);

    var sortToggle = document.getElementById("cpeSortToggle");
    var sortSelect = document.getElementById("cpeSort");
    if (sortToggle && sortSelect) {
      sortToggle.addEventListener("click", function () {
        sortSelect.value = sortSelect.value === "name-asc" ? "name-desc" : "name-asc";
        applyFilters();
      });
    }

    if (listTabsNav) {
      listTabsNav.addEventListener("click", function (e) {
        var btn = e.target.closest(".section-nav-item[data-tab]");
        if (!btn) return;
        var tab = btn.getAttribute("data-tab");
        if (tab === "providers" || tab === "favorites") setActiveListTab(tab);
      });
    }

    window.addEventListener("capital-explorer-favorites-changed", function () {
      updateTabCounts();
      if (activeListTab === "favorites") applyFilters();
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    wireProfilePopup();
    wireEvents();
    if (window.CapitalExplorerFavorites && window.CapitalExplorerFavorites.ready) {
      void window.CapitalExplorerFavorites.ready();
    }
    loadProviders();
  });
})();
