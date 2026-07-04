/**
 * Demand Anchors radar layer — shared fetch, filters, popup helpers.
 */
(function (global) {
  "use strict";

  var PRIMARY_FILTERS = [
    { id: "all", label: "All Demand Anchors", param: null },
    { id: "Convention Center", label: "Convention Center", param: "Convention Center" },
    { id: "Medical Campus", label: "Medical Campus", param: "Medical Campus" },
    { id: "University / College", label: "University / College", param: "University / College" },
    { id: "Sports Venue", label: "Sports Venue", param: "Sports Venue" },
    { id: "Entertainment District", label: "Entertainment District", param: "Entertainment District" },
    { id: "Tourist Attraction", label: "Tourist Attraction", param: "Tourist Attraction" },
    { id: "Beach / Waterfront", label: "Beach / Waterfront", param: "Beach / Waterfront" },
    { id: "Business District", label: "Business District", param: "Business District" },
    { id: "Industrial / Logistics Zone", label: "Industrial / Logistics Zone", param: "Industrial / Logistics Zone" },
    { id: "Government / Civic Center", label: "Government / Civic Center", param: "Government / Civic Center" },
    { id: "Mixed-Use Development", label: "Mixed-Use Development", param: "Mixed-Use Development" },
    { id: "Future Growth Node", label: "Future Growth Node", param: "Future Growth Node" },
  ];

  var ANCHOR_COLORS = {
    Event: "#ff9800",
    Medical: "#e53935",
    Education: "#5c6bc0",
    Sports: "#43a047",
    Entertainment: "#ab47bc",
    Attraction: "#26a69a",
    Beach: "#00acc1",
    Business: "#1e88e5",
    Industrial: "#6d4c41",
    Government: "#546e7a",
    "Mixed-Use": "#8e24aa",
    "Growth Node": "#fdd835",
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
    if (opts.country) params.push("country=" + encodeURIComponent(opts.country));
    if (opts.region) params.push("region=" + encodeURIComponent(opts.region));
    if (opts.market) params.push("market=" + encodeURIComponent(opts.market));
    if (opts.dealRecordId) params.push("dealRecordId=" + encodeURIComponent(opts.dealRecordId));
    var base = "/api/radar-map-points/demand-anchors";
    return params.length ? base + "?" + params.join("&") : base;
  }

  function parseItems(responseData) {
    if (!responseData) return [];
    if (Array.isArray(responseData)) return responseData;
    if (Array.isArray(responseData.points)) return responseData.points;
    if (Array.isArray(responseData.anchors)) {
      return responseData.anchors.filter(function (item) {
        return item.includeOnRadarMap !== false;
      });
    }
    if (Array.isArray(responseData.data)) return responseData.data;
    return [];
  }

  function extractTypeCounts(responseData) {
    if (!responseData) return {};
    var stats = responseData.statistics || {};
    if (stats.typeCounts && Object.keys(stats.typeCounts).length) return stats.typeCounts;
    var layer = responseData.layers && responseData.layers["Demand Anchors"];
    if (layer && layer.byPointType) return layer.byPointType;
    return {};
  }

  function getTotalCount(responseData, typeCounts) {
    if (responseData && responseData.totalCount != null) return responseData.totalCount;
    var stats = responseData && responseData.statistics;
    if (stats && stats.totalDemandAnchors != null) return stats.totalDemandAnchors;
    var tc = typeCounts || {};
    return Object.keys(tc).reduce(function (sum, k) {
      return sum + (Number(tc[k]) || 0);
    }, 0);
  }

  function getVisibleFilterDefs(typeCounts) {
    var counts = typeCounts || {};
    return PRIMARY_FILTERS.filter(function (def) {
      if (def.id === "all") return true;
      return (counts[def.id] || 0) > 0 || PRIMARY_FILTERS.some(function (p) {
        return p.id === def.id;
      });
    });
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
    var defs = PRIMARY_FILTERS;

    container.innerHTML = "";
    defs.forEach(function (def) {
      if (def.id !== "all" && (typeCounts[def.id] || 0) === 0 && totalCount > 0) return;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "infra-filter-chip" + (selectedId === def.id ? " is-active" : "");
      btn.setAttribute("data-anchor-filter", def.id);
      var count = countForFilter(def, typeCounts, totalCount);
      btn.innerHTML = esc(def.label) + '<span class="infra-count">(' + esc(count) + ")</span>";
      btn.addEventListener("click", function () {
        if (typeof onSelect === "function") onSelect(def.id);
      });
      container.appendChild(btn);
    });
  }

  var POINT_TYPE_TO_ICON = {
    "Convention Center": "Event",
    "Medical Campus": "Medical",
    "University / College": "Education",
    "Sports Venue": "Sports",
    "Entertainment District": "Entertainment",
    "Tourist Attraction": "Attraction",
    "Beach / Waterfront": "Beach",
    "Business District": "Business",
    "Industrial / Logistics Zone": "Industrial",
    "Government / Civic Center": "Government",
    "Mixed-Use Development": "Mixed-Use",
    "Future Growth Node": "Growth Node",
  };

  function getColorForPointType(pointType) {
    var icon = POINT_TYPE_TO_ICON[pointType] || "Attraction";
    return ANCHOR_COLORS[icon] || "#ff9800";
  }

  function getAnchorColor(item) {
    var iconKey = (item && (item.mapIconType || item.pointType)) || "Attraction";
    return ANCHOR_COLORS[iconKey] || ANCHOR_COLORS[item && item.mapIconType] || "#ff9800";
  }

  function buildPopupHtml(item) {
    var color = getAnchorColor(item);
    var pointType = item.pointType || item.type || "—";
    var rationale = item.hotelDemandRationale;
    if (rationale && rationale.length > 220) rationale = rationale.slice(0, 217) + "…";

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
      esc(pointType) +
      "</div>" +
      '<div style="font-size:11px;color:#666;text-transform:uppercase;">Demand Anchor</div>' +
      "</div>" +
      '<div style="font-size:12px;color:#666;line-height:1.5;">' +
      "<strong>Point Type:</strong> " +
      esc(pointType) +
      "<br>" +
      "<strong>Demand Segment:</strong> " +
      esc(item.demandSegment || "—") +
      "<br>" +
      "<strong>Location:</strong> " +
      esc(item.city || "—") +
      ", " +
      esc(item.country || "—") +
      "<br>" +
      "<strong>Demand Relevance:</strong> " +
      esc(item.demandRelevance || "—") +
      "<br>" +
      "<strong>Data Confidence:</strong> " +
      esc(item.dataConfidence || "—") +
      (rationale ? "<br><strong>Hotel Demand:</strong> " + esc(rationale) : "") +
      "</div></div>"
    );
  }

  var fetchCache = {};
  var fetchInflight = {};

  function fetchCacheKey(opts) {
    return JSON.stringify(opts || {});
  }

  function fetchDemandAnchors(opts) {
    opts = opts || {};
    var key = fetchCacheKey(opts);
    if (Object.prototype.hasOwnProperty.call(fetchCache, key)) {
      return Promise.resolve(fetchCache[key]);
    }
    if (fetchInflight[key]) {
      return fetchInflight[key];
    }
    var url = buildApiUrl(opts);
    fetchInflight[key] = fetch(url, { headers: { "ngrok-skip-browser-warning": "true" } })
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

  global.DemandAnchorsRadar = {
    PRIMARY_FILTERS: PRIMARY_FILTERS,
    buildApiUrl: buildApiUrl,
    parseItems: parseItems,
    extractTypeCounts: extractTypeCounts,
    getTotalCount: getTotalCount,
    renderFilterChips: renderFilterChips,
    getAnchorColor: getAnchorColor,
    getColorForPointType: getColorForPointType,
    buildPopupHtml: buildPopupHtml,
    fetchDemandAnchors: fetchDemandAnchors,
  };
})(typeof window !== "undefined" ? window : globalThis);
