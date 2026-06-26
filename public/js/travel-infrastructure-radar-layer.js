/**
 * Travel Infrastructure radar layer — shared fetch, filters, popup helpers.
 */
(function (global) {
  "use strict";

  var PRIMARY_FILTERS = [
    { id: "all", label: "All Travel Infrastructure", param: null },
    { id: "Airport", label: "Airport", param: "Airport" },
    { id: "Cruise Port", label: "Cruise Port", param: "Cruise Port" },
    { id: "Convention Center", label: "Convention Center", param: "Convention Center" },
  ];

  var OPTIONAL_FILTERS = [
    { id: "Train Station", label: "Train Station", param: "Train Station" },
    { id: "Highway Access", label: "Highway Access", param: "Highway Access" },
    { id: "Bus Terminal", label: "Bus Terminal", param: "Bus Terminal" },
    { id: "Ferry Terminal", label: "Ferry Terminal", param: "Ferry Terminal" },
    { id: "Port / Maritime", label: "Port / Maritime", param: "Port / Maritime" },
  ];

  var INFRA_COLORS = {
    Airport: "#9c27b0",
    "Cruise Port": "#e91e63",
    "Convention Center": "#00bcd4",
    Convention: "#00bcd4",
    Event: "#00bcd4",
    Train: "#3f51b5",
    Highway: "#795548",
    Bus: "#607d8b",
    Ferry: "#009688",
    Port: "#ff5722",
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function buildApiUrl(opts) {
    opts = opts || {};
    var params = [];
    if (opts.pointTypeFilter && opts.pointTypeFilter !== "all") {
      params.push("pointTypeFilter=" + encodeURIComponent(opts.pointTypeFilter));
    }
    if (opts.country) {
      params.push("country=" + encodeURIComponent(opts.country));
    }
    if (opts.region) {
      params.push("region=" + encodeURIComponent(opts.region));
    }
    var base = "/api/radar-map-points/travel-infrastructure";
    return params.length ? base + "?" + params.join("&") : base;
  }

  function parseItems(responseData) {
    if (!responseData) return [];
    if (Array.isArray(responseData)) return responseData;
    if (Array.isArray(responseData.points)) return responseData.points;
    if (Array.isArray(responseData.infrastructure)) {
      return responseData.infrastructure.filter(function (item) {
        return item.includeOnRadarMap !== false;
      });
    }
    if (Array.isArray(responseData.data)) return responseData.data;
    return [];
  }

  function extractTypeCounts(responseData) {
    if (!responseData) return {};
    var stats = responseData.statistics || {};
    if (stats.typeCounts && Object.keys(stats.typeCounts).length) {
      return stats.typeCounts;
    }
    var layer = responseData.layers && responseData.layers["Travel Infrastructure"];
    if (layer && layer.byPointType) return layer.byPointType;
    return {};
  }

  function getTotalCount(responseData, typeCounts) {
    if (responseData && responseData.totalCount != null) return responseData.totalCount;
    var stats = responseData && responseData.statistics;
    if (stats && stats.totalInfrastructure != null) return stats.totalInfrastructure;
    var tc = typeCounts || {};
    return Object.keys(tc).reduce(function (sum, k) {
      return sum + (Number(tc[k]) || 0);
    }, 0);
  }

  function getVisibleFilterDefs(typeCounts) {
    var counts = typeCounts || {};
    var visible = PRIMARY_FILTERS.slice();
    OPTIONAL_FILTERS.forEach(function (def) {
      if ((counts[def.id] || 0) > 0) visible.push(def);
    });
    return visible;
  }

  function countForFilter(def, typeCounts, totalCount) {
    if (def.id === "all") return totalCount != null ? totalCount : getTotalCount(null, typeCounts);
    return typeCounts[def.id] || 0;
  }

  function renderFilterChips(container, options) {
    if (!container) return;
    var selectedId = options.selectedId || "all";
    var typeCounts = options.typeCounts || {};
    var totalCount = options.totalCount;
    var onSelect = options.onSelect;
    var defs = getVisibleFilterDefs(typeCounts);

    container.innerHTML = "";
    defs.forEach(function (def) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "infra-filter-chip" + (selectedId === def.id ? " is-active" : "");
      btn.setAttribute("data-infra-filter", def.id);
      var count = countForFilter(def, typeCounts, totalCount);
      btn.innerHTML = esc(def.label) + '<span class="infra-count">(' + esc(count) + ")</span>";
      btn.addEventListener("click", function () {
        if (typeof onSelect === "function") onSelect(def.id);
      });
      container.appendChild(btn);
    });
  }

  function getInfraColor(item) {
    var iconKey = (item && (item.mapIconType || item.pointType || item.type)) || "Unknown";
    return INFRA_COLORS[iconKey] || INFRA_COLORS[item && item.type] || "#9c27b0";
  }

  function getColorForPointType(pointType) {
    return getInfraColor({ pointType: pointType, type: pointType });
  }

  function buildPopupHtml(item) {
    var color = getInfraColor(item);
    var displaySubtype = item.pointSubtype || item.pointType || item.type || "Travel Infrastructure";
    var pointType = item.pointType || item.type || "—";
    var city = item.city || "—";
    var country = item.country || "—";
    var rationale = item.hotelDemandRationale;
    if (rationale && rationale.length > 220) {
      rationale = rationale.slice(0, 217) + "…";
    }

    return (
      '<div style="min-width: 250px; font-family: Inter, sans-serif;">' +
      '<h3 style="margin:0 0 10px;color:#333;font-size:16px;border-bottom:2px solid ' +
      color +
      ';padding-bottom:5px;">' +
      esc(item.name || "Unknown") +
      "</h3>" +
      '<div style="background:#f8f9fa;padding:10px;border-radius:6px;margin-bottom:10px;">' +
      '<div style="font-size:16px;font-weight:bold;color:' +
      color +
      ';">' +
      esc(displaySubtype) +
      "</div>" +
      '<div style="font-size:11px;color:#666;text-transform:uppercase;">Travel Infrastructure</div>' +
      "</div>" +
      '<div style="font-size:12px;color:#666;line-height:1.5;">' +
      "<strong>Point Type:</strong> " +
      esc(pointType) +
      "<br>" +
      "<strong>Subtype:</strong> " +
      esc(item.pointSubtype || "—") +
      "<br>" +
      "<strong>Location:</strong> " +
      esc(city) +
      ", " +
      esc(country) +
      "<br>" +
      "<strong>Demand Relevance:</strong> " +
      esc(item.demandRelevance || "—") +
      "<br>" +
      "<strong>Data Confidence:</strong> " +
      esc(item.dataConfidence || "—") +
      (rationale
        ? "<br><strong>Hotel Demand:</strong> " + esc(rationale)
        : "") +
      "</div></div>"
    );
  }

  var fetchCache = {};
  var fetchInflight = {};

  function fetchCacheKey(opts) {
    return JSON.stringify(opts || {});
  }

  function fetchInfrastructure(opts) {
    opts = opts || {};
    var key = fetchCacheKey(opts);
    if (Object.prototype.hasOwnProperty.call(fetchCache, key)) {
      return Promise.resolve(fetchCache[key]);
    }
    if (fetchInflight[key]) {
      return fetchInflight[key];
    }
    var url = buildApiUrl(opts);
    fetchInflight[key] = fetch(url, {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (data) {
        fetchCache[key] = data;
        delete fetchInflight[key];
        return data;
      })
      .catch(function (err) {
        delete fetchInflight[key];
        throw err;
      });
    return fetchInflight[key];
  }

  global.TravelInfrastructureRadar = {
    PRIMARY_FILTERS: PRIMARY_FILTERS,
    OPTIONAL_FILTERS: OPTIONAL_FILTERS,
    buildApiUrl: buildApiUrl,
    parseItems: parseItems,
    extractTypeCounts: extractTypeCounts,
    getTotalCount: getTotalCount,
    getVisibleFilterDefs: getVisibleFilterDefs,
    renderFilterChips: renderFilterChips,
    getInfraColor: getInfraColor,
    getColorForPointType: getColorForPointType,
    buildPopupHtml: buildPopupHtml,
    fetchInfrastructure: fetchInfrastructure,
  };
})(typeof window !== "undefined" ? window : globalThis);
