/**
 * Operator Setup — Brands Managed multi-select (search-driven, lazy DOM).
 * Keeps full catalog in memory; renders only selected + filtered matches (cap) in the <select>.
 */
(function (global) {
  "use strict";

  var MAX_VISIBLE_OPTIONS = 80;
  var catalog = [];
  var catalogLoaded = false;
  var catalogLoading = null;

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function flattenGrouped(data) {
    var out = [];
    (data && data.brandsByParentCompany ? data.brandsByParentCompany : []).forEach(function (group) {
      var parent = group.parentCompany || "Other";
      (group.brands || []).forEach(function (brand) {
        var name = nz(brand && brand.name ? brand.name : brand);
        if (name) out.push({ parent: parent, name: name });
      });
    });
    out.sort(function (a, b) {
      return a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
    });
    return out;
  }

  function selectedNamesFromSelect(select) {
    var out = new Set();
    if (!select) return out;
    Array.from(select.selectedOptions || []).forEach(function (opt) {
      var v = nz(opt.value);
      if (v && v !== "Independent" && v !== "Other") out.add(v);
    });
    return out;
  }

  function findCatalogEntry(name) {
    for (var i = 0; i < catalog.length; i++) {
      if (catalog[i].name === name) return catalog[i];
    }
    return null;
  }

  function appendExtraOptions(select) {
    var og = document.createElement("optgroup");
    og.label = "Also report";
    var ind = document.createElement("option");
    ind.value = "Independent";
    ind.textContent = "Independent";
    og.appendChild(ind);
    var oth = document.createElement("option");
    oth.value = "Other";
    oth.textContent = "Other (specify below)";
    og.appendChild(oth);
    select.appendChild(og);
  }

  /**
   * Render select options: pinned selected + search matches (capped).
   * @param {HTMLSelectElement} select
   * @param {string} [searchQuery]
   * @param {Set<string>} [pinnedSelected]
   */
  function renderBrandsManagedSelect(select, searchQuery, pinnedSelected) {
    if (!select) return;
    var q = nz(searchQuery).toLowerCase();
    var selected = pinnedSelected || selectedNamesFromSelect(select);
    var prevSpecial = {
      Independent: false,
      Other: false,
    };
    Array.from(select.selectedOptions || []).forEach(function (opt) {
      if (opt.value === "Independent") prevSpecial.Independent = true;
      if (opt.value === "Other") prevSpecial.Other = true;
    });

    select.innerHTML = "";
    var rendered = 0;

    selected.forEach(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      opt.selected = true;
      var entry = findCatalogEntry(name);
      if (entry) opt.title = entry.parent;
      select.appendChild(opt);
      rendered += 1;
    });

    if (catalog.length) {
      for (var i = 0; i < catalog.length; i++) {
        if (rendered >= MAX_VISIBLE_OPTIONS + selected.size) break;
        var item = catalog[i];
        if (selected.has(item.name)) continue;
        if (q && item.name.toLowerCase().indexOf(q) === -1) continue;
        var match = document.createElement("option");
        match.value = item.name;
        match.textContent = item.name;
        match.title = item.parent;
        select.appendChild(match);
        rendered += 1;
      }
    }

    if (select.options.length === 0) {
      var hint = document.createElement("option");
      hint.value = "";
      hint.disabled = true;
      hint.textContent = catalog.length
        ? q
          ? "No matches — refine your search"
          : "Type in the search box above to find brands"
        : "Loading brand catalog…";
      select.appendChild(hint);
    }

    appendExtraOptions(select);
    Array.from(select.options).forEach(function (opt) {
      if (opt.value === "Independent") opt.selected = prevSpecial.Independent;
      if (opt.value === "Other") opt.selected = prevSpecial.Other;
    });
  }

  function loadBrandsCatalog(apiUrl) {
    if (catalogLoaded) return Promise.resolve(catalog);
    if (catalogLoading) return catalogLoading;
    catalogLoading = fetch(apiUrl, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
      signal: AbortSignal.timeout(15000),
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      })
      .then(function (data) {
        if (!data || !data.success || !data.brandsByParentCompany) {
          throw new Error("Invalid brands response");
        }
        catalog = flattenGrouped(data);
        catalogLoaded = true;
        return catalog;
      })
      .finally(function () {
        catalogLoading = null;
      });
    return catalogLoading;
  }

  function applyBrandsPrefill(select, searchInput, brandNames) {
    if (!select) return;
    var pinned = new Set();
    (Array.isArray(brandNames) ? brandNames : String(brandNames || "").split(","))
      .map(nz)
      .filter(Boolean)
      .forEach(function (n) {
        pinned.add(n);
      });
    renderBrandsManagedSelect(select, searchInput && searchInput.value, pinned);
  }

  function filterBrandsManagedOptions(select, searchInput) {
    renderBrandsManagedSelect(select, searchInput ? searchInput.value : "", selectedNamesFromSelect(select));
  }

  function getParentCompanyForBrand(brandName) {
    var entry = findCatalogEntry(nz(brandName));
    return entry ? nz(entry.parent) : "";
  }

  global.OperatorSetupBrandsManaged = {
    loadBrandsCatalog: loadBrandsCatalog,
    renderBrandsManagedSelect: renderBrandsManagedSelect,
    applyBrandsPrefill: applyBrandsPrefill,
    filterBrandsManagedOptions: filterBrandsManagedOptions,
    selectedNamesFromSelect: selectedNamesFromSelect,
    getParentCompanyForBrand: getParentCompanyForBrand,
    getCatalogSize: function () {
      return catalog.length;
    },
  };
})(typeof window !== "undefined" ? window : global);
