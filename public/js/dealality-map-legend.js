/**
 * Dealality shared map legend — Radar + Scout Market Map.
 * Hotel status, travel infrastructure, demand anchors, and Scout-only layers.
 */
(function (global) {
  "use strict";

  var PENTAGON_CLIP = "polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%)";
  var DIAMOND_CLIP = "polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%)";
  var HEX_CLIP = "polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%)";

  var HOTEL_COLORS = {
    open: "#2563eb",
    pipeline: "#dc2626",
    candidate: "#7c3aed",
  };

  var INFRA_COLORS = {
    Airport: "#9c27b0",
    "Cruise Port": "#e91e63",
    "Convention Center": "#00bcd4",
    Convention: "#00bcd4",
    "Train Station": "#3f51b5",
    Train: "#3f51b5",
    "Highway Access": "#795548",
    Highway: "#795548",
    "Bus Terminal": "#607d8b",
    Bus: "#607d8b",
    "Ferry Terminal": "#009688",
    Ferry: "#009688",
    "Port / Maritime": "#ff5722",
    Port: "#ff5722",
  };

  var INFRA_LEGEND = [
    { marker: "infrastructure-airport", label: "Airport", key: "Airport" },
    { marker: "infrastructure-cruise", label: "Cruise Port", key: "Cruise Port" },
    { marker: "infrastructure-convention", label: "Convention Center", key: "Convention Center" },
    { marker: "infrastructure-train", label: "Train Station", key: "Train Station" },
    { marker: "infrastructure-highway", label: "Highway Access", key: "Highway Access" },
    { marker: "infrastructure-bus", label: "Bus Terminal", key: "Bus Terminal" },
    { marker: "infrastructure-ferry", label: "Ferry Terminal", key: "Ferry Terminal" },
    { marker: "infrastructure-port", label: "Port / Maritime", key: "Port / Maritime" },
  ];

  var ANCHOR_COLORS = {
    Beach: "#00acc1",
    Attraction: "#26a69a",
    Entertainment: "#ab47bc",
    Medical: "#e53935",
    Education: "#e53935",
    Sports: "#43a047",
    Business: "#1e88e5",
    Government: "#546e7a",
    Industrial: "#6d4c41",
    "Mixed-Use": "#8e24aa",
    "Growth Node": "#fdd835",
    Event: "#ff9800",
  };

  var ANCHOR_LEGEND = [
    { marker: "demand-anchor-event", label: "Convention Center", keys: ["Convention Center", "Event"] },
    { marker: "demand-anchor-institutional", label: "Medical Campus", keys: ["Medical Campus", "Medical"] },
    { marker: "demand-anchor-education", label: "University / College", keys: ["University / College", "Education"] },
    { marker: "demand-anchor-sports", label: "Sports Venue", keys: ["Sports Venue", "Sports"] },
    { marker: "demand-anchor-entertainment", label: "Entertainment District", keys: ["Entertainment District", "Entertainment"] },
    { marker: "demand-anchor-attraction", label: "Tourist Attraction", keys: ["Tourist Attraction", "Attraction"] },
    { marker: "demand-anchor-beach", label: "Beach / Waterfront", keys: ["Beach / Waterfront", "Beach"] },
    { marker: "demand-anchor-business", label: "Business District", keys: ["Business District", "Business"] },
    { marker: "demand-anchor-industrial", label: "Industrial / Logistics Zone", keys: ["Industrial / Logistics Zone", "Industrial"] },
    { marker: "demand-anchor-government", label: "Government / Civic Center", keys: ["Government / Civic Center", "Government"] },
    { marker: "demand-anchor-mixed", label: "Mixed-Use Development", keys: ["Mixed-Use Development", "Mixed-Use"] },
    { marker: "demand-anchor-growth", label: "Future Growth Node", keys: ["Future Growth Node", "Growth Node"] },
  ];

  var SCOUT_LEGEND = [
    { marker: "scout-signal", label: "Generated Signals" },
    { marker: "scout-saved", label: "Saved Watchlist" },
    { marker: "scout-cluster", label: "Market Clusters" },
  ];

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;");
  }

  function sectionHtml(id, title, items) {
    var rows = items
      .map(function (item) {
        return (
          '<div class="dc-legend-item" data-legend-item="' +
          esc(item.id || item.marker) +
          '">' +
          '<div class="legend-marker ' +
          esc(item.marker) +
          '"></div>' +
          "<span>" +
          esc(item.label) +
          "</span></div>"
        );
      })
      .join("");
    return (
      '<div class="dc-legend-section" data-legend-section="' +
      esc(id) +
      '"><strong>' +
      esc(title) +
      "</strong></div>" +
      rows
    );
  }

  function render(container, opts) {
    opts = opts || {};
    if (!container) return;
    var html =
      sectionHtml("hotelStatus", "Hotel Status", [
        { id: "open", marker: "open", label: "Open Hotels" },
        { id: "pipeline", marker: "pipeline", label: "Pipeline" },
        { id: "candidate", marker: "candidate", label: "Candidate/LOI" },
      ]) +
      sectionHtml(
        "travelInfrastructure",
        "Travel Infrastructure",
        INFRA_LEGEND.map(function (i) {
          return { id: i.key, marker: i.marker, label: i.label };
        })
      ) +
      sectionHtml(
        "demandAnchors",
        "Demand Anchors",
        ANCHOR_LEGEND.map(function (i) {
          return { id: i.marker, marker: i.marker, label: i.label };
        })
      );
    if (opts.mode === "scout") {
      html += sectionHtml(
        "scoutLayers",
        "Scout Layers",
        SCOUT_LEGEND.map(function (i) {
          return { id: i.marker, marker: i.marker, label: i.label };
        })
      );
    }
    container.className = "dc-map-legend map-legend";
    container.innerHTML = html;
    updateVisibility(container, opts.visibility || {});
  }

  function setSectionDisplay(container, sectionId, show) {
    var section = container.querySelector('[data-legend-section="' + sectionId + '"]');
    if (!section) return;
    section.style.display = show ? "" : "none";
    var next = section.nextElementSibling;
    while (next && !next.hasAttribute("data-legend-section")) {
      next.style.display = show ? "" : "none";
      next = next.nextElementSibling;
    }
  }

  function updateVisibility(container, visibility) {
    if (!container) return;
    var v = visibility || {};
    setSectionDisplay(container, "hotelStatus", v.hotels !== false);
    setSectionDisplay(container, "travelInfrastructure", v.travelInfra !== false);
    setSectionDisplay(container, "demandAnchors", v.demandAnchors !== false);
    setSectionDisplay(container, "scoutLayers", v.scoutLayers !== false);
  }

  function normalizeHotelStatus(status) {
    var s = String(status || "").toLowerCase();
    if (s === "pipeline") return "pipeline";
    if (s.indexOf("candidate") !== -1 || s.indexOf("loi") !== -1) return "candidate";
    return "open";
  }

  function hotelStatusColor(status) {
    return HOTEL_COLORS[normalizeHotelStatus(status)] || HOTEL_COLORS.open;
  }

  function infraColor(category) {
    var c = String(category || "");
    return INFRA_COLORS[c] || INFRA_COLORS[c.split(" / ")[0]] || "#9c27b0";
  }

  function anchorColor(category) {
    var c = String(category || "");
    for (var i = 0; i < ANCHOR_LEGEND.length; i++) {
      var leg = ANCHOR_LEGEND[i];
      if (leg.keys.indexOf(c) !== -1) {
        var cls = leg.marker.replace("demand-anchor-", "");
        if (cls === "beach") return ANCHOR_COLORS.Beach;
        if (cls === "attraction") return ANCHOR_COLORS.Attraction;
        if (cls === "entertainment") return ANCHOR_COLORS.Entertainment;
        if (cls === "institutional") return ANCHOR_COLORS.Medical;
        if (cls === "sports") return ANCHOR_COLORS.Sports;
        if (cls === "business") return ANCHOR_COLORS.Business;
        if (cls === "industrial") return ANCHOR_COLORS.Industrial;
      }
    }
    return ANCHOR_COLORS.Attraction;
  }

  function shapeStyle(color, clip, size) {
    size = size || 14;
    return (
      "width:" +
      size +
      "px;height:" +
      size +
      "px;background:" +
      color +
      ";clip-path:" +
      clip +
      ";box-shadow:0 1px 3px rgba(0,0,0,.45);"
    );
  }

  function createCircleIcon(color, size) {
    if (!global.L) return null;
    size = size || 10;
    return global.L.divIcon({
      className: "dc-map-marker-icon-wrap",
      html:
        '<div style="width:' +
        size +
        "px;height:" +
        size +
        "px;background:" +
        color +
        ';border-radius:50%;box-shadow:0 1px 3px rgba(0,0,0,.45);"></div>',
      iconSize: [size, size],
      iconAnchor: [size / 2, size / 2],
    });
  }

  function createPentagonIcon(color, size) {
    if (!global.L) return null;
    size = size || 14;
    return global.L.divIcon({
      className: "dc-map-marker-icon-wrap",
      html: "<div style=\"" + shapeStyle(color, PENTAGON_CLIP, size) + '"></div>',
      iconSize: [size, size],
      iconAnchor: [size / 2, size / 2],
    });
  }

  function createDiamondIcon(color, size) {
    if (!global.L) return null;
    size = size || 14;
    return global.L.divIcon({
      className: "dc-map-marker-icon-wrap",
      html: "<div style=\"" + shapeStyle(color, DIAMOND_CLIP, size) + '"></div>',
      iconSize: [size, size],
      iconAnchor: [size / 2, size / 2],
    });
  }

  function createHexIcon(color, size) {
    if (!global.L) return null;
    size = size || 12;
    return global.L.divIcon({
      className: "dc-map-marker-icon-wrap",
      html: "<div style=\"" + shapeStyle(color, HEX_CLIP, size) + '"></div>',
      iconSize: [size, size],
      iconAnchor: [size / 2, size / 2],
    });
  }

  global.DealalityMapLegend = {
    render: render,
    updateVisibility: updateVisibility,
    hotelStatusColor: hotelStatusColor,
    normalizeHotelStatus: normalizeHotelStatus,
    infraColor: infraColor,
    anchorColor: anchorColor,
    createCircleIcon: createCircleIcon,
    createPentagonIcon: createPentagonIcon,
    createDiamondIcon: createDiamondIcon,
    createHexIcon: createHexIcon,
    HOTEL_COLORS: HOTEL_COLORS,
    INFRA_COLORS: INFRA_COLORS,
    ANCHOR_COLORS: ANCHOR_COLORS,
  };
})(typeof window !== "undefined" ? window : globalThis);
