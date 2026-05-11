(function () {
  "use strict";

  var state = {
    activeTab: "",
    data: null,
    filters: null,
    map: null,
    mapLayer: null,
    mapFallback: false,
    selectedMarkets: [],
    liveMapMode: false,
    liveHotels: [],
    liveInfrastructure: [],
    layerToggles: {
      opportunityMarkets: false,
      targetAssets: false,
      brandWhiteSpace: false,
      demandCenters: false,
      distressTransition: false,
      relationshipPaths: false,
      watchlist: false,
      sourceReview: false,
      outcomesLearning: false
    }
  };

  var FILTER_IDS = [
    "countryFilter",
    "marketFilter",
    "submarketFilter",
    "brandFilter",
    "parentCompanyFilter",
    "chainScaleFilter",
    "serviceModelFilter",
    "opportunityTypeFilter",
    "confidenceLevelFilter",
    "reviewStatusFilter",
    "relationshipPathStatusFilter"
  ];

  var LAYER_TOGGLE_CONFIG = [
    { id: "toggleLayerOpportunityMarkets", tab: "opportunityMarkets" },
    { id: "toggleLayerTargetAssets", tab: "targetAssets" },
    { id: "toggleLayerBrandWhiteSpace", tab: "brandWhiteSpace" },
    { id: "toggleLayerDemandCenters", tab: "demandCenters" },
    { id: "toggleLayerDistressTransition", tab: "distressTransition" },
    { id: "toggleLayerRelationshipPaths", tab: "relationshipPaths" },
    { id: "toggleLayerWatchlist", tab: "watchlist" },
    { id: "toggleLayerSourceReview", tab: "sourceReview" },
    { id: "toggleLayerOutcomesLearning", tab: "outcomesLearning" }
  ];

  function esc(value) {
    var div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
  }

  function confidenceBadge(score, level) {
    var cls = score >= 75 || level === "High" ? "success" : (score >= 60 || level === "Medium" ? "warning" : "danger");
    return '<span class="badge ' + cls + '">Confidence: ' + esc(level || score) + "</span>";
  }

  function reviewBadge(status) {
    var danger = status && status.toLowerCase().indexOf("pending") !== -1;
    var cls = danger ? "danger" : (status === "Validated" ? "success" : "warning");
    return '<span class="badge ' + cls + '">Review: ' + esc(status || "Unknown") + "</span>";
  }

  function populateSelect(id, options) {
    var select = document.getElementById(id);
    if (!select) return;
    var label = select.previousElementSibling ? select.previousElementSibling.textContent : "All";
    var html = '<option value="">All ' + esc(label) + "</option>";
    (options || []).forEach(function (opt) {
      html += '<option value="' + esc(opt) + '">' + esc(opt) + "</option>";
    });
    select.innerHTML = html;
  }

  function readFilters() {
    var output = {};
    FILTER_IDS.forEach(function (id) {
      var el = document.getElementById(id);
      output[id] = el ? el.value.trim() : "";
    });
    var searchInput = document.getElementById("searchInput");
    output.search = searchInput ? searchInput.value.trim().toLowerCase() : "";
    return output;
  }

  function matchesSearch(row, search) {
    if (!search) return true;
    var text = JSON.stringify(row).toLowerCase();
    return text.indexOf(search) !== -1;
  }

  function applyFilters(data, f) {
    function matchValue(current, expected) {
      if (!expected) return true;
      return String(current || "").toLowerCase() === String(expected).toLowerCase();
    }

    var markets = (data.markets || []).filter(function (row) {
      return matchValue(row.country, f.countryFilter) &&
        matchValue(row.marketName, f.marketFilter) &&
        matchValue(row.submarket, f.submarketFilter) &&
        matchValue(row.confidenceLevel, f.confidenceLevelFilter) &&
        matchesSearch(row, f.search);
    });

    var assets = (data.assets || []).filter(function (row) {
      return matchValue(row.country, f.countryFilter) &&
        matchValue(row.market, f.marketFilter) &&
        matchValue(row.submarket, f.submarketFilter) &&
        matchValue(row.brand || "Independent", f.brandFilter) &&
        matchValue(row.parentCompany, f.parentCompanyFilter) &&
        matchValue(row.chainScale, f.chainScaleFilter) &&
        matchValue(row.serviceModel, f.serviceModelFilter) &&
        matchValue(row.opportunityType, f.opportunityTypeFilter) &&
        matchValue(row.confidenceLevel, f.confidenceLevelFilter) &&
        matchValue(row.reviewStatus, f.reviewStatusFilter) &&
        matchValue(row.relationshipPathStatus, f.relationshipPathStatusFilter) &&
        matchesSearch(row, f.search);
    });

    var signals = (data.signals || []).filter(function (row) {
      return (!f.marketFilter || String(row.linkedEntity).toLowerCase().indexOf(String(f.marketFilter).toLowerCase()) !== -1) &&
        matchValue(row.confidenceLevel, f.confidenceLevelFilter) &&
        matchValue(row.reviewStatus, f.reviewStatusFilter) &&
        matchValue(row.humanValidationStatus, f.reviewStatusFilter) &&
        matchesSearch(row, f.search);
    });

    var demandCenters = (data.demandCenters || []).filter(function (row) {
      return matchValue(row.market, f.marketFilter) &&
        matchValue(row.country, f.countryFilter) &&
        matchesSearch(row, f.search);
    });

    var relationshipPaths = (data.relationshipPaths || []).filter(function (row) {
      return matchValue(row.relationshipPathStatus, f.relationshipPathStatusFilter) &&
        matchValue(row.confidenceLevel, f.confidenceLevelFilter) &&
        matchesSearch(row, f.search);
    });

    var targetOpportunities = (data.targetOpportunities || []).filter(function (row) {
      return matchValue(row.opportunityType, f.opportunityTypeFilter) &&
        matchValue(row.reviewStatus, f.reviewStatusFilter) &&
        matchValue(row.confidenceLevel, f.confidenceLevelFilter) &&
        matchesSearch(row, f.search);
    });

    var sources = (data.sources || []).filter(function (row) {
      return matchValue(row.reviewStatus, f.reviewStatusFilter) && matchesSearch(row, f.search);
    });

    var outcomes = (data.outcomes || []).filter(function (row) { return matchesSearch(row, f.search); });

    return {
      markets: markets,
      assets: assets,
      signals: signals,
      demandCenters: demandCenters,
      relationshipPaths: relationshipPaths,
      targetOpportunities: targetOpportunities,
      sources: sources,
      outcomes: outcomes
    };
  }

  function getCoordsByMarket(marketName) {
    var map = {
      "Punta Cana": [18.5601, -68.3725],
      "Santo Domingo": [18.4861, -69.9312],
      "Cartagena": [10.391, -75.4794],
      "Medellín": [6.2442, -75.5812],
      "San José": [9.9281, -84.0907],
      "Cancún / Riviera Maya": [21.1619, -86.8515]
    };
    return map[marketName] || [15.7, -74.2];
  }

  function hashString(value) {
    var s = String(value || "");
    var h = 0;
    for (var i = 0; i < s.length; i += 1) {
      h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    }
    return Math.abs(h);
  }

  function spreadPoint(base, key, latScale, lngScale) {
    var seed = hashString(key);
    var ring = (seed % 4) + 1;
    var angle = (seed % 360) * (Math.PI / 180);
    var latOffset = Math.sin(angle) * ring * latScale;
    var lngOffset = Math.cos(angle) * ring * lngScale;
    return [base[0] + latOffset, base[1] + lngOffset];
  }

  function chooseMarketByIndex(markets, idx) {
    var rows = markets || [];
    if (!rows.length) return null;
    return rows[idx % rows.length].marketName || null;
  }

  function signalPoint(signal, idx, filtered) {
    var linkedMarket = signal.linkedEntityType === "Market" ? signal.linkedEntity : "";
    var market = linkedMarket || chooseMarketByIndex(filtered.markets, idx) || "Punta Cana";
    var base = getCoordsByMarket(market);
    var key = [signal.signalType, signal.linkedEntity, signal.source, idx].join("|");
    return spreadPoint(base, key, 0.06, 0.065);
  }

  function sourcePoint(source, idx, filtered) {
    var inferred = String(source.linkedEntity || "").split(",")[0].trim();
    var market = inferred || chooseMarketByIndex(filtered.markets, idx) || "Punta Cana";
    var base = getCoordsByMarket(market);
    var key = [source.sourceName, source.sourceType, source.linkedEntity, idx].join("|");
    return spreadPoint(base, key, 0.06, 0.065);
  }

  function marketTerms(marketName) {
    var key = String(marketName || "").toLowerCase();
    var map = {
      "punta cana": ["punta cana", "bavaro", "uvero alto", "cap cana"],
      "santo domingo": ["santo domingo", "piantini", "colonial zone", "gazcue"],
      "cartagena": ["cartagena", "bocagrande", "walled city"],
      "medellín": ["medellin", "medellín", "el poblado", "laureles", "milla de oro"],
      "san josé": ["san jose", "san josé", "escazu", "escazú", "santa ana"],
      "cancún / riviera maya": ["cancun", "cancún", "riviera maya", "playa del carmen", "puerto morelos", "hotel zone"]
    };
    return map[key] || [];
  }

  function passesLiveGeoFilters(item, filters) {
    var country = String(item.country || "").toLowerCase();
    var city = String(item.city || "").toLowerCase();
    var name = String(item.name || "").toLowerCase();
    var search = String(filters.search || "").toLowerCase();

    if (filters.countryFilter && country !== String(filters.countryFilter).toLowerCase()) return false;
    if (filters.marketFilter) {
      var terms = marketTerms(filters.marketFilter);
      if (terms.length && !terms.some(function (t) { return city.indexOf(t) !== -1 || name.indexOf(t) !== -1; })) return false;
    }
    if (search) {
      var haystack = [name, city, country, String(item.brand || ""), String(item.type || "")].join(" ").toLowerCase();
      if (haystack.indexOf(search) === -1) return false;
    }
    return true;
  }

  /** Radar/Airtable census filters — brand-presence hotel rows. */
  function liveHotelPassesFilters(hotel, filters) {
    if (!passesLiveGeoFilters(hotel, filters)) return false;
    if (filters.brandFilter && String(hotel.brand || "").trim().toLowerCase() !== String(filters.brandFilter).trim().toLowerCase()) return false;
    if (filters.parentCompanyFilter && String(hotel.parentCompany || "").trim().toLowerCase() !== String(filters.parentCompanyFilter).trim().toLowerCase()) return false;
    if (filters.chainScaleFilter) {
      var cs = String(hotel.chainScale || hotel.propertyType || "").trim().toLowerCase();
      if (cs !== String(filters.chainScaleFilter).trim().toLowerCase()) return false;
    }
    return true;
  }

  function getRadarStatusFillHex(status) {
    var s = String(status || "").toLowerCase();
    if (s === "open") return "#2563eb";
    if (s === "pipeline") return "#dc2626";
    if (s === "candidate") return "#7c3aed";
    return "#8b5cf6";
  }

  function getRadarChainScaleFillHex(propertyType) {
    if (!propertyType) return "#6b7280";
    var colors = {
      Luxury: "#68B0AB",
      "Upper Upscale": "#FF785A",
      Upscale: "#8EF21F",
      "Upper Midscale": "#8e44ad",
      Midscale: "#daa520",
      Economy: "#694A38",
      "Extended Stay": "#e74c3c",
      "Select Service": "#1abc9c",
      Independent: "#34495e",
      LUXURY: "#68B0AB",
      "UPPER UPSCALE": "#FF785A",
      UPSCALE: "#8EF21F",
      "UPPER MIDSCALE": "#8e44ad",
      MIDSCALE: "#daa520",
      ECONOMY: "#694A38",
      "EXTENDED STAY": "#e74c3c",
      "SELECT SERVICE": "#1abc9c",
      INDEPENDENT: "#34495e"
    };
    if (colors[propertyType]) return colors[propertyType];
    var up = String(propertyType).toUpperCase();
    if (colors[up]) return colors[up];
    var low = String(propertyType).toLowerCase();
    if (low.indexOf("luxury") !== -1) return "#68B0AB";
    if (low.indexOf("upscale") !== -1) return "#FF785A";
    if (low.indexOf("midscale") !== -1) return "#daa520";
    if (low.indexOf("economy") !== -1) return "#694A38";
    if (low.indexOf("independent") !== -1) return "#34495e";
    return "#ef4444";
  }

  function getLiveHotelFillHex(hotel) {
    var chainScaleView = isToggleOn("toggleChainScaleView", false);
    if (!chainScaleView) return getRadarStatusFillHex(hotel.status);
    var brand = String(hotel.brand || "").toLowerCase();
    if (brand === "independent") return getRadarChainScaleFillHex("Independent");
    var pt = hotel.propertyType || hotel.chainScale;
    if (!pt || String(pt).trim() === "" || String(pt).toLowerCase() === "unknown") {
      return getRadarStatusFillHex(hotel.status);
    }
    return getRadarChainScaleFillHex(pt);
  }

  function buildLiveHotelPopup(hotel) {
    return radarPopupCard({
      title: hotel.name || "Hotel",
      statusValue: hotel.status || "—",
      statusLabel: "Status",
      rows: [
        { label: "Brand", value: hotel.brand || "—" },
        { label: "Parent Company", value: hotel.parentCompany || "—" },
        { label: "City", value: hotel.city || "—" },
        { label: "Country", value: hotel.country || "—" },
        { label: "Chain Scale", value: hotel.chainScale || hotel.propertyType || "—" },
        { label: "Rooms", value: hotel.rooms != null ? String(hotel.rooms) : "—" }
      ],
      extraLines: [{ label: "Source", value: "Hotel Census (Airtable)" }],
      location: (hotel.city || "—") + ", " + (hotel.country || "—"),
      lat: hotel.lat,
      lng: hotel.lng
    });
  }

  function statusClassFromOpportunityType(type) {
    var v = String(type || "").toLowerCase();
    if (v.indexOf("distressed") !== -1 || v.indexOf("transition") !== -1) return "candidate";
    if (v.indexOf("rebrand") !== -1) return "pipeline";
    return "open";
  }

  function classFromChainScale(chainScale) {
    var v = String(chainScale || "").toLowerCase();
    if (v.indexOf("luxury") !== -1 || v.indexOf("upper upscale") !== -1) return "chain-luxury";
    if (v.indexOf("upscale") !== -1 || v.indexOf("upper midscale") !== -1) return "chain-upscale";
    if (v.indexOf("midscale") !== -1 || v.indexOf("economy") !== -1) return "chain-midscale";
    return "chain-independent";
  }

  function isToggleOn(id, fallbackValue) {
    var el = document.getElementById(id);
    if (!el) return !!fallbackValue;
    return !!el.checked;
  }

  function getEnabledLayerTabs() {
    var enabled = {};
    Object.keys(state.layerToggles).forEach(function (tab) {
      if (state.layerToggles[tab]) enabled[tab] = true;
    });
    return enabled;
  }

  function getTabPinFlags() {
    var enabled = getEnabledLayerTabs();
    var showAssets = !!(enabled.targetAssets || enabled.relationshipPaths || enabled.watchlist || enabled.outcomesLearning);
    var showSignals = !!(enabled.brandWhiteSpace || enabled.distressTransition || enabled.sourceReview);
    return {
      enabled: enabled,
      showMarkets: !!enabled.opportunityMarkets,
      showAssets: showAssets,
      showSignals: showSignals,
      showDemandCenters: !!enabled.demandCenters
    };
  }

  function getWatchlistAssets(filtered) {
    var targetRows = (filtered.targetOpportunities || []).slice(0, 10);
    if (!targetRows.length) return [];
    var byName = {};
    (filtered.assets || []).forEach(function (asset) {
      byName[String(asset.assetName || "").toLowerCase()] = asset;
    });
    return targetRows.map(function (row) {
      var key = String(row.linkedAsset || "").toLowerCase();
      return byName[key];
    }).filter(Boolean);
  }

  /** Scout intelligence asset pins — layer tab toggles only (not Airtable census). */
  function getAssetRowsForMap(filtered, pinFlags) {
    var assets = filtered.assets || [];
    var fullLayers = !!(pinFlags.enabled.targetAssets || pinFlags.enabled.relationshipPaths || pinFlags.enabled.outcomesLearning);
    if (fullLayers) return assets;
    if (pinFlags.enabled.watchlist) return getWatchlistAssets(filtered);
    return [];
  }

  function getSourcePinRows(filtered) {
    return (filtered.sources || []).slice(0, 20);
  }

  function sourceCoords(source, idx) {
    return sourcePoint(source, idx, state.data || { markets: [] });
  }

  function buildAssetPopup(asset, filtered, lat, lng) {
    var tab = state.activeTab;
    var relationships = (filtered.relationshipPaths || []).filter(function (r) {
      return String(r.linkedAsset || "").toLowerCase() === String(asset.assetName || "").toLowerCase();
    });
    var relationship = relationships[0] || null;
    var targetOpp = (filtered.targetOpportunities || []).find(function (o) {
      return String(o.linkedAsset || "").toLowerCase() === String(asset.assetName || "").toLowerCase();
    });
    var outcome = (filtered.outcomes || []).find(function (o) {
      return String(o.linkedTargetOpportunity || "").toLowerCase() === String(asset.assetName || "").toLowerCase();
    });

    var extraLines = [
      { label: "Top Signals", value: (asset.topSignals || []).join(", ") },
      { label: "Why It Matters", value: asset.whyItMatters || "—" },
      { label: "Potential Brand Fit", value: (asset.potentialBrandFit || []).join(", ") },
      { label: "Potential Operator Fit", value: (asset.potentialOperatorFit || []).join(", ") },
      { label: "Relationship Path Status", value: asset.relationshipPathStatus || "—" },
      { label: "Recommended Next Action", value: asset.recommendedNextAction || "—" },
      { label: "Confidence", value: asset.confidenceScore || asset.confidenceLevel || "—" },
      { label: "Review Status", value: asset.reviewStatus || "—" }
    ];

    if (tab === "relationshipPaths" && relationship) {
      extraLines = extraLines.concat([
        { label: "Company", value: relationship.company || "—" },
        { label: "Contact Role", value: relationship.contactRole || "—" },
        { label: "Warm Intro Source", value: relationship.warmIntroSource || "—" },
        { label: "Public Contact Source", value: relationship.publicContactSource || "—" },
        { label: "Notes", value: relationship.notes || "—" },
        { label: "Last Validated", value: relationship.lastValidated || "—" }
      ]);
    }

    if (tab === "watchlist" && targetOpp) {
      extraLines = extraLines.concat([
        { label: "Target Priority Score", value: targetOpp.targetPriorityScore || "—" },
        { label: "Priority Label", value: targetOpp.priorityLabel || "—" },
        { label: "Watchlist Action", value: targetOpp.recommendedNextAction || "—" }
      ]);
    }

    if (tab === "outcomesLearning" && outcome) {
      extraLines = extraLines.concat([
        { label: "Outreach Path Used", value: outcome.outreachPathUsed || "—" },
        { label: "Response Received", value: outcome.responseReceived || "—" },
        { label: "Meeting Created", value: outcome.meetingCreated || "—" },
        { label: "Converted to Dealality Opportunity", value: outcome.convertedToDealalityOpportunity || "—" },
        { label: "Lessons Learned", value: outcome.lessonsLearned || "—" }
      ]);
    }

    return radarPopupCard({
      title: asset.assetName,
      statusValue: asset.opportunityType || asset.priorityLabel || "Target",
      statusLabel: "Opportunity Type",
      rows: [
        { label: "Current Brand / Independent", value: asset.currentBrandOrIndependent || asset.brand || "Independent" },
        { label: "Parent Company", value: asset.parentCompany || "Unknown" },
        { label: "Operator", value: asset.operator || "Unknown" },
        { label: "Owner", value: asset.owner || "Unknown" },
        { label: "Chain Scale", value: asset.chainScale || "Unknown" },
        { label: "Rooms", value: asset.roomCount || "Unknown" }
      ],
      extraLines: extraLines,
      location: (asset.submarket || asset.market || "Unknown") + ", " + (asset.country || "Unknown"),
      lat: lat,
      lng: lng
    });
  }

  function buildSignalPopup(signal, lat, lng) {
    return radarPopupCard({
      title: signal.signalType,
      statusValue: signal.signalStrength || signal.confidenceLevel || "Signal",
      statusLabel: "Signal Strength",
      rows: [
        { label: "Linked Entity", value: signal.linkedEntity || "—" },
        { label: "Entity Type", value: signal.linkedEntityType || "—" },
        { label: "Confidence", value: signal.confidenceScore || signal.confidenceLevel || "—" },
        { label: "Source", value: signal.source || "Mock Intelligence" },
        { label: "Validation", value: signal.humanValidationStatus || "Pending" },
        { label: "Review Status", value: signal.reviewStatus || "—" }
      ],
      extraLines: [
        { label: "AI Interpretation", value: signal.aiInterpretation || "—" },
        { label: "Recommended Action", value: signal.recommendedAction || "Review" }
      ],
      location: signal.linkedEntity || "Market-linked",
      lat: lat,
      lng: lng
    });
  }

  function buildSourcePopup(source, lat, lng) {
    return radarPopupCard({
      title: source.linkedEntity || "Source Review",
      statusValue: source.reviewStatus || "Source",
      statusLabel: "Review Status",
      rows: [
        { label: "Source Type", value: source.sourceType || "—" },
        { label: "Source Name", value: source.sourceName || "—" },
        { label: "Confidence", value: source.confidenceScore || "—" },
        { label: "Linked Entity", value: source.linkedEntity || "—" },
        { label: "Source URL", value: source.sourceUrl || "—" },
        { label: "Data Extracted", value: source.dataExtracted || "—" }
      ],
      extraLines: [
        { label: "Recommended Next Step", value: source.reviewStatus === "Validated" ? "Add to Opportunity" : "Request Validation" }
      ],
      location: source.linkedEntity || "Market-linked",
      lat: lat,
      lng: lng
    });
  }

  function buildDemandPopup(dc, lat, lng) {
    return radarPopupCard({
      title: dc.demandCenter,
      statusValue: dc.strength || "Demand",
      statusLabel: "Demand Strength",
      rows: [
        { label: "Market", value: dc.market || "—" },
        { label: "Country", value: dc.country || "—" },
        { label: "Demand Type", value: dc.demandType || "—" },
        { label: "Confidence", value: dc.confidenceScore || "—" },
        { label: "Review Status", value: dc.reviewStatus || "—" },
        { label: "Priority", value: dc.priorityLabel || "—" }
      ],
      extraLines: [
        { label: "Demand Note", value: dc.note || "—" }
      ],
      location: (dc.market || "Unknown") + ", " + (dc.country || "Unknown"),
      lat: lat,
      lng: lng
    });
  }

  function updateMapLegend(filtered) {
    var hotelsGroup = document.getElementById("legendHotelsGroup");
    var chainScaleGroup = document.getElementById("legendChainScaleGroup");
    var signalsGroup = document.getElementById("legendSignalsGroup");
    var brandPenetrationGroup = document.getElementById("legendBrandPenetrationGroup");
    var infraGroup = document.getElementById("legendInfrastructureGroup");
    var demandCentersGroup = document.getElementById("legendDemandCentersGroup");
    var pinFlags = getTabPinFlags();
    var showHotels = isToggleOn("toggleShowHotels", true);
    var chainScaleView = isToggleOn("toggleChainScaleView", false);
    var showBrandPenetration = isToggleOn("toggleBrandPenetration", false);
    var showInfrastructure = isToggleOn("toggleTravelInfrastructure", false);
    var filtersLeg = readFilters();
    var liveCount = (state.liveHotels || []).filter(function (h) {
      return liveHotelPassesFilters(h, filtersLeg);
    }).length;
    var censusPinsVisible = showHotels && liveCount > 0;
    if (hotelsGroup) hotelsGroup.style.display = censusPinsVisible && !chainScaleView ? "" : "none";
    if (chainScaleGroup) chainScaleGroup.style.display = censusPinsVisible && chainScaleView ? "" : "none";
    if (signalsGroup) signalsGroup.style.display = pinFlags.showSignals ? "" : "none";
    if (brandPenetrationGroup) brandPenetrationGroup.style.display = showBrandPenetration ? "" : "none";
    if (infraGroup) infraGroup.style.display = showInfrastructure ? "" : "none";
    if (demandCentersGroup) demandCentersGroup.style.display = pinFlags.showDemandCenters ? "" : "none";
  }

  function ensureMap() {
    if (state.map || state.mapFallback) return;
    if (typeof L === "undefined") {
      state.mapFallback = true;
      var el = document.getElementById("scoutMap");
      if (el) {
        el.classList.add("fallback-map");
        el.innerHTML = '<div class="fallback-map-watermark">Map fallback mode</div><div id="scoutMapFallbackLayer" class="fallback-layer"></div>';
      }
      return;
    }
    state.map = L.map("scoutMap", {
      center: [15.5, -74.5],
      zoom: 4,
      minZoom: 3,
      maxZoom: 10
    });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "© OpenStreetMap contributors"
    }).addTo(state.map);
    state.mapLayer = L.layerGroup().addTo(state.map);
  }

  function mapMarker(latlng, cssClass, popupHtml) {
    return L.marker(latlng, {
      icon: L.divIcon({
        className: "",
        html: '<div class="scout-map-marker ' + cssClass + '"></div>',
        iconSize: [14, 14],
        iconAnchor: [7, 7]
      })
    }).bindPopup(popupHtml, { maxWidth: 340 });
  }

  function formatCoord(v) {
    var n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return n.toFixed(4);
  }

  function popupField(label, value) {
    return '<div style="font-size:12px;color:#cbd5e1;"><strong>' + esc(label) + ':</strong><br><span style="color:#ffffff;">' + esc(value == null || value === "" ? "—" : value) + "</span></div>";
  }

  function radarPopupCard(opts) {
    var rows = opts.rows || [];
    var extraLines = opts.extraLines || [];
    var rowsHtml = rows.map(function (row) { return popupField(row.label, row.value); }).join("");
    var extraHtml = extraLines.map(function (row) {
      return '<div style="font-size:12px;color:#cbd5e1;margin-top:4px;"><strong>' + esc(row.label) + ":</strong> " + esc(row.value == null || row.value === "" ? "—" : row.value) + "</div>";
    }).join("");
    var statusValue = opts.statusValue == null || opts.statusValue === "" ? "—" : opts.statusValue;
    return '' +
      '<div style="min-width:300px;font-family:Inter,\'Segoe UI\',sans-serif;background:#1e293b;color:#ffffff;border:2px solid #ffffff;border-radius:8px;padding:15px;">' +
        '<h3 style="margin:0 0 10px 0;color:#ffffff;font-size:16px;border-bottom:2px solid #475569;padding-bottom:5px;">' + esc(opts.title || "Scout Pin") + "</h3>" +
        '<div style="background:#334155;padding:10px;border-radius:6px;margin-bottom:10px;">' +
          '<div style="font-size:18px;font-weight:bold;color:#60a5fa;">' + esc(statusValue) + "</div>" +
          '<div style="font-size:12px;color:#cbd5e1;text-transform:uppercase;">' + esc(opts.statusLabel || "Status") + "</div>" +
        "</div>" +
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;">' + rowsHtml + "</div>" +
        (extraHtml ? '<div style="border-top:1px solid #475569;padding-top:8px;margin-bottom:8px;">' + extraHtml + "</div>" : "") +
        '<div style="font-size:12px;color:#cbd5e1;"><strong>Location:</strong> ' + esc(opts.location || "—") + "<br>" +
        '<strong>Coordinates:</strong> ' + esc(formatCoord(opts.lat)) + ", " + esc(formatCoord(opts.lng)) + "</div>" +
      "</div>";
  }

  function toFallbackXY(lat, lng) {
    var minLat = -5;
    var maxLat = 33;
    var minLng = -120;
    var maxLng = -55;
    var x = ((lng - minLng) / (maxLng - minLng)) * 100;
    var y = ((maxLat - lat) / (maxLat - minLat)) * 100;
    return { x: Math.max(0, Math.min(100, x)), y: Math.max(0, Math.min(100, y)) };
  }

  function addFallbackPoint(layer, lat, lng, cls, tooltip) {
    var xy = toFallbackXY(lat, lng);
    var point = document.createElement("button");
    point.type = "button";
    point.className = "fallback-point " + cls;
    point.style.left = xy.x + "%";
    point.style.top = xy.y + "%";
    point.title = tooltip || "";
    layer.appendChild(point);
  }

  function addFallbackLiveHotelDot(layer, lat, lng, fillHex, tooltip) {
    var xy = toFallbackXY(lat, lng);
    var point = document.createElement("button");
    point.type = "button";
    point.className = "fallback-point fallback-live-airtable";
    point.style.background = fillHex;
    point.style.left = xy.x + "%";
    point.style.top = xy.y + "%";
    point.title = tooltip || "";
    layer.appendChild(point);
  }

  function appendCensusHotelsLeaflet(bounds) {
    if (!isToggleOn("toggleShowHotels", true) || typeof L === "undefined") return;
    if (!state.liveHotels || !state.liveHotels.length) return;
    var filters = readFilters();
    state.liveHotels.filter(function (h) {
      return liveHotelPassesFilters(h, filters);
    }).forEach(function (hotel) {
      var lat = Number(hotel.lat);
      var lng = Number(hotel.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
      var fill = getLiveHotelFillHex(hotel);
      var marker = L.circleMarker([lat, lng], {
        radius: 6,
        fillColor: fill,
        color: "#ffffff",
        weight: 2,
        opacity: 1,
        fillOpacity: 0.8
      });
      marker.bindPopup(buildLiveHotelPopup(hotel));
      marker.addTo(state.mapLayer);
      bounds.push([lat, lng]);
    });
  }

  function updateFallbackMap(filtered) {
    var layer = document.getElementById("scoutMapFallbackLayer");
    if (!layer) return;
    layer.innerHTML = "";

    var pinFlags = getTabPinFlags();
    var activeFilters = readFilters();
    var showCensusHotels = isToggleOn("toggleShowHotels", true);
    var showInfrastructure = isToggleOn("toggleTravelInfrastructure", false);

    if (showCensusHotels && state.liveHotels && state.liveHotels.length) {
      state.liveHotels.filter(function (h) {
        return liveHotelPassesFilters(h, activeFilters);
      }).forEach(function (hotel) {
        var lat = Number(hotel.lat);
        var lng = Number(hotel.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        addFallbackLiveHotelDot(layer, lat, lng, getLiveHotelFillHex(hotel), hotel.name || "Hotel");
      });
    }

    if (pinFlags.showMarkets) {
      (filtered.markets || []).forEach(function (market) {
        var coords = getCoordsByMarket(market.marketName);
        addFallbackPoint(layer, coords[0], coords[1], "open", market.marketName + " (" + market.country + ")");
      });
    }

    getAssetRowsForMap(filtered, pinFlags).forEach(function (asset) {
      var base = getCoordsByMarket(asset.market);
      var spread = spreadPoint(base, [asset.assetName, asset.market, asset.roomCount].join("|"), 0.045, 0.05);
      var jitterLat = spread[0];
      var jitterLng = spread[1];
      var cls = statusClassFromOpportunityType(asset.opportunityType);
      addFallbackPoint(layer, jitterLat, jitterLng, cls, asset.assetName + " (" + asset.market + ")");
    });

    if (showInfrastructure && state.liveInfrastructure && state.liveInfrastructure.length) {
      state.liveInfrastructure.filter(function (infra) {
        return passesLiveGeoFilters(infra, activeFilters);
      }).forEach(function (infra) {
        var t = String(infra.type || "").toLowerCase();
        var cls = t.indexOf("airport") !== -1 ? "signal" : "demand";
        addFallbackPoint(layer, infra.lat, infra.lng, cls, infra.name + " (" + infra.type + ")");
      });
    }

    if (pinFlags.showSignals) {
      if (pinFlags.enabled.sourceReview) {
        getSourcePinRows(filtered).forEach(function (source, idx) {
          var sourceLatLng = sourceCoords(source, idx);
          addFallbackPoint(layer, sourceLatLng[0], sourceLatLng[1], "signal", source.sourceName || source.linkedEntity);
        });
      }
      if (pinFlags.enabled.brandWhiteSpace || pinFlags.enabled.distressTransition) {
        filtered.signals.slice(0, 20).forEach(function (signal, idx) {
          var signalLatLng = signalPoint(signal, idx, filtered);
          addFallbackPoint(layer, signalLatLng[0], signalLatLng[1], "signal", signal.signalType);
        });
      }
    }

    if (pinFlags.showDemandCenters) {
      filtered.demandCenters.forEach(function (dc, idx) {
        var base = getCoordsByMarket(dc.market);
        var spread = spreadPoint(base, [dc.demandCenter, dc.market, idx].join("|"), 0.065, 0.07);
        addFallbackPoint(layer, spread[0], spread[1], "demand", dc.demandCenter + " (" + dc.market + ")");
      });
    }
  }

  function updateMap(filtered) {
    ensureMap();
    updateMapLegend(filtered);
    if (state.mapFallback) {
      updateFallbackMap(filtered);
      return;
    }
    if (!state.map || !state.mapLayer) return;
    state.mapLayer.clearLayers();

    var bounds = [];

    var pinFlags = getTabPinFlags();
    var activeFilters = readFilters();
    var showInfrastructure = isToggleOn("toggleTravelInfrastructure", false);

    appendCensusHotelsLeaflet(bounds);

    if (pinFlags.showMarkets) {
      (filtered.markets || []).forEach(function (market) {
        var coords = getCoordsByMarket(market.marketName);
        var marketMarker = mapMarker([coords[0], coords[1]], "open",
          radarPopupCard({
            title: market.marketName,
            statusValue: String(market.scoutOpportunityScore || "—"),
            statusLabel: "Market Scout Opportunity",
            rows: [
              { label: "Country", value: market.country || "—" },
              { label: "Submarket", value: market.submarket || "—" },
              { label: "Confidence", value: market.confidenceScore || market.confidenceLevel || "—" },
              { label: "Total Hotels", value: market.totalHotels || "—" },
              { label: "Branded Hotels", value: market.brandedHotels || "—" },
              { label: "Independent Hotels", value: market.independentHotels || "—" }
            ],
            extraLines: [
              { label: "Parent Companies", value: (market.parentCompaniesPresent || []).join(", ") },
              { label: "Brands Present", value: (market.brandsPresent || []).join(", ") },
              { label: "Brands Missing", value: (market.brandsMissing || []).join(", ") },
              { label: "Top Demand Drivers", value: (market.topDemandDrivers || []).join(", ") },
              { label: "Top White-Space Opportunities", value: (market.topWhiteSpaceOpportunities || []).join(", ") },
              { label: "Recommended Growth Plays", value: (market.recommendedGrowthPlays || []).join(", ") },
              { label: "Recommended Next Action", value: market.recommendedNextAction || "—" }
            ],
            location: (market.marketName || "Unknown") + ", " + (market.country || "Unknown"),
            lat: coords[0],
            lng: coords[1]
          })
        );
        marketMarker.addTo(state.mapLayer);
        bounds.push([coords[0], coords[1]]);
      });
    }

    getAssetRowsForMap(filtered, pinFlags).forEach(function (asset) {
      var base = getCoordsByMarket(asset.market);
      var spread = spreadPoint(base, [asset.assetName, asset.market, asset.roomCount].join("|"), 0.045, 0.05);
      var jitterLat = spread[0];
      var jitterLng = spread[1];
      var cls = statusClassFromOpportunityType(asset.opportunityType);
      var marker = mapMarker([jitterLat, jitterLng], cls, buildAssetPopup(asset, filtered, jitterLat, jitterLng));
      marker.addTo(state.mapLayer);
      bounds.push([jitterLat, jitterLng]);
    });

    if (showInfrastructure && state.liveInfrastructure && state.liveInfrastructure.length) {
      state.liveInfrastructure.filter(function (infra) {
        return passesLiveGeoFilters(infra, activeFilters);
      }).forEach(function (infra) {
        var type = String(infra.type || "").toLowerCase();
        var cls = type.indexOf("airport") !== -1 ? "infra-airport" :
          (type.indexOf("cruise") !== -1 ? "infra-cruise" : "infra-convention");
        var marker = mapMarker([infra.lat, infra.lng], cls,
          radarPopupCard({
            title: infra.name,
            statusValue: infra.type || "Infrastructure",
            statusLabel: "Infrastructure Type",
            rows: [
              { label: "City", value: infra.city || "Unknown" },
              { label: "Country", value: infra.country || "Unknown" },
              { label: "Layer", value: "Travel Infrastructure" },
              { label: "Source", value: "Radar Infrastructure Feed" }
            ],
            location: (infra.city || "Unknown") + ", " + (infra.country || "Unknown"),
            lat: infra.lat,
            lng: infra.lng
          })
        );
        marker.addTo(state.mapLayer);
        bounds.push([infra.lat, infra.lng]);
      });
    }

    if (pinFlags.showSignals) {
      if (pinFlags.enabled.sourceReview) {
        getSourcePinRows(filtered).forEach(function (source, idx) {
          var sourceLatLng = sourceCoords(source, idx);
          var sourceMarker = mapMarker([sourceLatLng[0], sourceLatLng[1]], "signal", buildSourcePopup(source, sourceLatLng[0], sourceLatLng[1]));
          sourceMarker.addTo(state.mapLayer);
          bounds.push([sourceLatLng[0], sourceLatLng[1]]);
        });
      }
      if (pinFlags.enabled.brandWhiteSpace || pinFlags.enabled.distressTransition) {
        filtered.signals.slice(0, 20).forEach(function (signal, idx) {
          var signalLatLng = signalPoint(signal, idx, filtered);
          var lat = signalLatLng[0];
          var lng = signalLatLng[1];
          var marker = mapMarker([lat, lng], "signal", buildSignalPopup(signal, lat, lng));
          marker.addTo(state.mapLayer);
          bounds.push([lat, lng]);
        });
      }
    }

    if (pinFlags.showDemandCenters) {
      filtered.demandCenters.forEach(function (dc, idx) {
        var base = getCoordsByMarket(dc.market);
        var spread = spreadPoint(base, [dc.demandCenter, dc.market, idx].join("|"), 0.065, 0.07);
        var lat = spread[0];
        var lng = spread[1];
        var marker = mapMarker([lat, lng], "demand",
          buildDemandPopup(dc, lat, lng)
        );
        marker.addTo(state.mapLayer);
        bounds.push([lat, lng]);
      });
    }

    if (bounds.length) state.map.fitBounds(bounds, { padding: [30, 30], maxZoom: 6 });
  }

  function updateKpis(filtered) {
    var needsValidation = filtered.assets.filter(function (a) {
      return a.reviewStatus === "Needs Validation" || a.reviewStatus === "Pending Analyst Review";
    }).length + filtered.signals.filter(function (s) {
      return s.humanValidationStatus === "Needs Validation" || s.humanValidationStatus === "Pending Analyst Review";
    }).length;

    document.getElementById("kpiOpportunityMarkets").textContent = filtered.markets.length;
    document.getElementById("kpiPriorityTargets").textContent = filtered.assets.filter(function (a) { return a.targetPriorityScore >= 85; }).length;
    document.getElementById("kpiWhiteSpaceSignals").textContent = filtered.signals.length;
    document.getElementById("kpiNeedsValidation").textContent = needsValidation;
    document.getElementById("kpiRelationshipPathsFound").textContent = filtered.relationshipPaths.length;
  }

  function renderLocationPills(filtered) {
    var pillsEl = document.getElementById("locationPills");
    var cardsEl = document.getElementById("locationCardsArea");
    if (!pillsEl || !cardsEl) return;

    var markets = filtered.markets || [];
    if (!markets.length) {
      pillsEl.innerHTML = '<div class="empty">No location pills for current filters.</div>';
      cardsEl.innerHTML = "";
      state.selectedMarkets = [];
      return;
    }

    // Keep only selected markets still visible after filtering.
    state.selectedMarkets = state.selectedMarkets.filter(function (name) {
      return markets.some(function (m) { return m.marketName === name; });
    });

    pillsEl.innerHTML = markets.map(function (m) {
      var isActive = state.selectedMarkets.indexOf(m.marketName) !== -1;
      return '<button type="button" class="location-pill' + (isActive ? " active" : "") + '" data-market-pill="' + esc(m.marketName) + '">' +
        esc(m.marketName) + ", " + esc(m.country) + "</button>";
    }).join("");

    pillsEl.querySelectorAll("[data-market-pill]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var market = btn.getAttribute("data-market-pill");
        var idx = state.selectedMarkets.indexOf(market);
        if (idx === -1) state.selectedMarkets.push(market);
        else state.selectedMarkets.splice(idx, 1);
        rerender();
      });
    });

    var selectedRows = markets.filter(function (m) {
      return state.selectedMarkets.indexOf(m.marketName) !== -1;
    });

    if (!selectedRows.length) {
      cardsEl.innerHTML = '<div class="empty">Click a location pill to show market cards.</div>';
      return;
    }

    cardsEl.innerHTML = selectedRows.map(function (m) {
      return '<article class="location-card">' +
        "<h4>" + esc(m.marketName) + ", " + esc(m.country) + "</h4>" +
        '<div class="mini"><strong>Scout Opportunity:</strong> ' + esc(m.scoutOpportunityScore) + "</div>" +
        '<div class="mini"><strong>Confidence:</strong> ' + esc(m.confidenceScore) + "</div>" +
        '<div class="mini"><strong>Total Hotels:</strong> ' + esc(m.totalHotels) + " · <strong>Branded:</strong> " + esc(m.brandedHotels) + " · <strong>Independent:</strong> " + esc(m.independentHotels) + "</div>" +
        '<div class="mini"><strong>Missing Brands:</strong> ' + esc(m.brandsMissing.join(", ")) + "</div>" +
        '<div class="mini"><strong>Next Action:</strong> ' + esc(m.recommendedNextAction) + "</div>" +
        "</article>";
    }).join("");
  }

  function renderEmpty() {
    return '<div class="empty">No results for current filters. Try Reset Filters.</div>';
  }

  function renderMarkets(markets) {
    if (!markets.length) return renderEmpty();
    return '<div class="cards-grid">' + markets.map(function (m) {
      return '<article class="intel-card">' +
        '<div class="card-head"><div><h3 class="card-title">' + esc(m.marketName) + '</h3><div class="meta">' + esc(m.country) + " · " + esc(m.submarket) + '</div></div>' +
        '<div class="badges"><span class="badge">Opportunity ' + esc(m.scoutOpportunityScore) + '</span>' + confidenceBadge(m.confidenceScore, m.confidenceLevel) + "</div></div>" +
        '<p class="metric-line"><strong>Total Hotels:</strong> ' + esc(m.totalHotels) + " · <strong>Branded:</strong> " + esc(m.brandedHotels) + " · <strong>Independent:</strong> " + esc(m.independentHotels) + "</p>" +
        '<p class="metric-line"><strong>Parent Companies:</strong> ' + esc(m.parentCompaniesPresent.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Brands Present:</strong> ' + esc(m.brandsPresent.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Brands Missing:</strong> ' + esc(m.brandsMissing.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Top Demand Drivers:</strong> ' + esc(m.topDemandDrivers.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Top White-Space Opportunities:</strong> ' + esc(m.topWhiteSpaceOpportunities.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Recommended Growth Plays:</strong> ' + esc(m.recommendedGrowthPlays.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Recommended Next Action:</strong> ' + esc(m.recommendedNextAction) + "</p>" +
        '<div class="cta-row"><button class="btn">View Sources</button><button class="btn">Add to Watchlist</button><button class="btn">Request Validation</button></div>' +
        "</article>";
    }).join("") + "</div>";
  }

  function renderAssets(assets) {
    if (!assets.length) return renderEmpty();
    return '<div class="cards-grid">' + assets.map(function (a) {
      return '<article class="intel-card">' +
        '<div class="card-head"><div><h3 class="card-title">' + esc(a.assetName) + '</h3><div class="meta">' + esc(a.submarket) + ", " + esc(a.market) + ", " + esc(a.country) + '</div></div>' +
        '<div class="badges"><span class="badge">' + esc(a.opportunityType) + "</span><span class=\"badge\">" + esc(a.priorityLabel) + "</span></div></div>" +
        '<div class="badges">' + confidenceBadge(a.confidenceScore, a.confidenceLevel) + reviewBadge(a.reviewStatus) + '<span class="badge">Relationship: ' + esc(a.relationshipPathStatus) + "</span></div>" +
        '<p class="metric-line"><strong>Current Brand/Independent:</strong> ' + esc(a.currentBrandOrIndependent) + "</p>" +
        '<p class="metric-line"><strong>Parent Company:</strong> ' + esc(a.parentCompany) + " · <strong>Operator:</strong> " + esc(a.operator) + "</p>" +
        '<p class="metric-line"><strong>Owner:</strong> ' + esc(a.owner) + " · <strong>Rooms:</strong> " + esc(a.roomCount) + "</p>" +
        '<p class="metric-line"><strong>Chain Scale:</strong> ' + esc(a.chainScale) + "</p>" +
        '<p class="metric-line"><strong>Target Priority Score:</strong> ' + esc(a.targetPriorityScore) + "</p>" +
        '<p class="metric-line"><strong>Top Signals:</strong> ' + esc(a.topSignals.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Why It Matters:</strong> ' + esc(a.whyItMatters) + "</p>" +
        '<p class="metric-line"><strong>Potential Brand Fit:</strong> ' + esc(a.potentialBrandFit.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Potential Operator Fit:</strong> ' + esc(a.potentialOperatorFit.join(", ")) + "</p>" +
        '<p class="metric-line"><strong>Recommended Next Action:</strong> ' + esc(a.recommendedNextAction) + "</p>" +
        '<div class="cta-row"><button class="btn">View Sources</button><button class="btn">View Images</button><button class="btn">Create Outreach Plan</button><button class="btn">Add to Dealality Opportunity</button><button class="btn">Monitor Target</button></div>' +
        "</article>";
    }).join("") + "</div>";
  }

  function renderSignals(signals, distressOnly) {
    var rows = distressOnly ? signals.filter(function (s) {
      return /distress|turnover|debt|succession|refinancing/i.test(s.signalType + " " + s.aiInterpretation);
    }) : signals;
    if (!rows.length) return renderEmpty();
    return '<div class="cards-grid">' + rows.map(function (s) {
      return '<article class="intel-card">' +
        '<div class="card-head"><div><h3 class="card-title">' + esc(s.signalType) + '</h3><div class="meta">' + esc(s.linkedEntityType) + ": " + esc(s.linkedEntity) + '</div></div>' +
        '<div class="badges"><span class="badge">Strength: ' + esc(s.signalStrength) + "</span>" + confidenceBadge(s.confidenceScore, s.confidenceLevel) + "</div></div>" +
        '<div class="badges">' + reviewBadge(s.humanValidationStatus) + "</div>" +
        '<p class="metric-line"><strong>Source:</strong> ' + esc(s.source) + "</p>" +
        '<p class="metric-line"><strong>AI Interpretation:</strong> ' + esc(s.aiInterpretation) + "</p>" +
        '<p class="metric-line"><strong>Recommended Action:</strong> ' + esc(s.recommendedAction) + "</p>" +
        '<div class="cta-row"><button class="btn">View Sources</button><button class="btn">Mark as Validated</button><button class="btn">Request Validation</button></div>' +
        "</article>";
    }).join("") + "</div>";
  }

  function renderDemandCenters(rows) {
    if (!rows.length) return renderEmpty();
    return '<div class="cards-grid">' + rows.map(function (d) {
      return '<article class="intel-card">' +
        '<div class="card-head"><div><h3 class="card-title">' + esc(d.demandCenter) + '</h3><div class="meta">' + esc(d.market) + " · " + esc(d.demandType) + '</div></div>' +
        '<div class="badges"><span class="badge">Strength: ' + esc(d.strength) + "</span>" + confidenceBadge(d.confidenceScore) + "</div></div>" +
        '<p class="metric-line"><strong>Demand Note:</strong> ' + esc(d.note) + "</p>" +
        '<div class="cta-row"><button class="btn">View Sources</button><button class="btn">Add to Watchlist</button></div>' +
        "</article>";
    }).join("") + "</div>";
  }

  function renderRelationshipPaths(rows) {
    if (!rows.length) return renderEmpty();
    return '<div class="cards-grid">' + rows.map(function (r) {
      return '<article class="intel-card">' +
        '<div class="card-head"><div><h3 class="card-title">' + esc(r.linkedAsset) + '</h3><div class="meta">' + esc(r.company) + " · " + esc(r.contactRole) + '</div></div>' +
        '<div class="badges"><span class="badge">' + esc(r.relationshipPathStatus) + "</span>" + confidenceBadge(r.confidenceScore, r.confidenceLevel) + "</div></div>" +
        '<p class="metric-line"><strong>Warm Intro Source:</strong> ' + esc(r.warmIntroSource) + "</p>" +
        '<p class="metric-line"><strong>Public Contact Source:</strong> ' + esc(r.publicContactSource) + "</p>" +
        '<p class="metric-line"><strong>Notes:</strong> ' + esc(r.notes) + "</p>" +
        '<p class="metric-line"><strong>Last Validated:</strong> ' + esc(r.lastValidated) + "</p>" +
        '<div class="cta-row"><button class="btn">Add Relationship Note</button><button class="btn">Create Outreach Plan</button><button class="btn">Request Validation</button></div>' +
        "</article>";
    }).join("") + "</div>";
  }

  function renderTargetOpps(rows) {
    if (!rows.length) return renderEmpty();
    return '<div class="cards-grid">' + rows.map(function (o) {
      return '<article class="intel-card"><h3 class="card-title">' + esc(o.linkedAsset) + '</h3>' +
        '<div class="badges"><span class="badge">' + esc(o.opportunityType) + "</span><span class=\"badge\">" + esc(o.priorityLabel) + "</span>" + confidenceBadge(o.confidenceScore) + reviewBadge(o.reviewStatus) + "</div>" +
        '<p class="metric-line"><strong>Target Priority Score:</strong> ' + esc(o.targetPriorityScore) + "</p>" +
        '<p class="metric-line"><strong>Recommended Next Action:</strong> ' + esc(o.recommendedNextAction) + "</p>" +
        '<div class="cta-row"><button class="btn">Add to Watchlist</button><button class="btn">Add to Dealality Opportunity</button></div></article>';
    }).join("") + "</div>";
  }

  function renderSources(rows) {
    if (!rows.length) return renderEmpty();
    return '<div class="cards-grid">' + rows.map(function (s) {
      return '<article class="intel-card"><h3 class="card-title">' + esc(s.linkedEntity) + '</h3>' +
        '<div class="badges"><span class="badge">' + esc(s.sourceType) + "</span>" + confidenceBadge(s.confidenceScore) + reviewBadge(s.reviewStatus) + "</div>" +
        '<p class="metric-line"><strong>Source Name:</strong> ' + esc(s.sourceName) + "</p>" +
        '<p class="metric-line"><strong>Source URL:</strong> <a href="' + esc(s.sourceUrl) + '" target="_blank" rel="noopener noreferrer">' + esc(s.sourceUrl) + "</a></p>" +
        '<p class="metric-line"><strong>Data Extracted:</strong> ' + esc(s.dataExtracted) + "</p>" +
        '<div class="cta-row"><button class="btn">View Sources</button><button class="btn">Mark as Validated</button></div></article>';
    }).join("") + "</div>";
  }

  function renderOutcomes(rows) {
    if (!rows.length) return renderEmpty();
    return '<div class="cards-grid">' + rows.map(function (o) {
      return '<article class="intel-card"><h3 class="card-title">' + esc(o.linkedTargetOpportunity) + '</h3>' +
        '<p class="metric-line"><strong>Outreach Path Used:</strong> ' + esc(o.outreachPathUsed) + "</p>" +
        '<p class="metric-line"><strong>Response Received:</strong> ' + esc(o.responseReceived) + " · <strong>Meeting Created:</strong> " + esc(o.meetingCreated) + "</p>" +
        '<p class="metric-line"><strong>Converted to Dealality Opportunity:</strong> ' + esc(o.convertedToDealalityOpportunity) + "</p>" +
        '<p class="metric-line"><strong>Lessons Learned:</strong> ' + esc(o.lessonsLearned) + "</p>" +
        '<div class="cta-row"><button class="btn">Create Outreach Plan</button><button class="btn">Add Relationship Note</button></div></article>';
    }).join("") + "</div>";
  }

  function renderActiveTab(filtered) {
    var contentArea = document.getElementById("contentArea");
    if (!state.activeTab) {
      contentArea.innerHTML = '<div class="empty">Enable a lower-row toggle to show pins and intelligence cards.</div>';
      return;
    }
    var html = "";
    if (state.activeTab === "opportunityMarkets") html = renderMarkets(filtered.markets);
    else if (state.activeTab === "targetAssets") html = renderAssets(filtered.assets);
    else if (state.activeTab === "brandWhiteSpace") html = renderSignals(filtered.signals, false);
    else if (state.activeTab === "demandCenters") html = renderDemandCenters(filtered.demandCenters);
    else if (state.activeTab === "distressTransition") html = renderSignals(filtered.signals, true);
    else if (state.activeTab === "relationshipPaths") html = renderRelationshipPaths(filtered.relationshipPaths);
    else if (state.activeTab === "watchlist") html = renderTargetOpps(filtered.targetOpportunities);
    else if (state.activeTab === "sourceReview") html = renderSources(filtered.sources);
    else if (state.activeTab === "outcomesLearning") html = renderOutcomes(filtered.outcomes);
    contentArea.innerHTML = html;
  }

  var statusStartTime = null;
  var statusProgressInterval = null;

  function showSystemStatus(message, timeEstimate) {
    var statusElement = document.getElementById("systemStatus");
    if (!statusElement) return;
    var statusText = statusElement.querySelector(".status-text");
    var statusTime = statusElement.querySelector(".status-time");
    var progressBar = statusElement.querySelector(".status-progress-bar");

    if (statusText && statusText.querySelector("div:first-child")) statusText.querySelector("div:first-child").textContent = message || "Processing...";
    if (statusTime) statusTime.textContent = "Estimated time: " + (timeEstimate || "2-3 seconds");
    if (progressBar) progressBar.style.width = "0%";

    statusElement.style.display = "block";
    statusStartTime = Date.now();

    startProgressAnimation();

    setTimeout(function () {
      statusElement.classList.add("show");
    }, 10);
  }

  function startProgressAnimation() {
    if (statusProgressInterval) clearInterval(statusProgressInterval);
    var statusEl = document.getElementById("systemStatus");
    if (!statusEl) return;
    var progressBar = statusEl.querySelector(".status-progress-bar");
    if (!progressBar) return;

    statusProgressInterval = setInterval(function () {
      if (!statusStartTime) return;
      var elapsed = Date.now() - statusStartTime;
      var estimatedTotal = 3000;
      var progress = Math.min((elapsed / estimatedTotal) * 100, 95);
      progressBar.style.width = progress + "%";
    }, 100);
  }

  function updateSystemStatus(message, timeEstimate) {
    var statusElement = document.getElementById("systemStatus");
    if (!statusElement || statusElement.style.display === "none") return;
    var statusText = statusElement.querySelector(".status-text");
    var statusTime = statusElement.querySelector(".status-time");
    if (statusText && statusText.querySelector("div:first-child")) {
      statusText.querySelector("div:first-child").textContent = message || "Processing...";
    }
    if (statusTime && timeEstimate) statusTime.textContent = "Estimated time: " + timeEstimate;
  }

  function hideSystemStatus() {
    var statusElement = document.getElementById("systemStatus");
    if (!statusElement) return;
    var progressBar = statusElement.querySelector(".status-progress-bar");
    if (progressBar) progressBar.style.width = "100%";
    if (statusProgressInterval) {
      clearInterval(statusProgressInterval);
      statusProgressInterval = null;
    }
    setTimeout(function () {
      statusElement.classList.remove("show");
      statusStartTime = null;
      setTimeout(function () {
        statusElement.style.display = "none";
      }, 300);
    }, 500);
  }

  function showRadarToast(message) {
    var toast = document.createElement("div");
    toast.className = "radar-toast";
    toast.textContent = message;
    document.body.appendChild(toast);
    setTimeout(function () {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, 2450);
  }

  function updateResetViewBadge() {
    var badge = document.getElementById("filterCountBadge");
    if (!badge) return;
    var values = readFilters();
    var count = 0;
    FILTER_IDS.forEach(function (id) {
      if (values[id]) count += 1;
    });
    if (values.search) count += 1;
    badge.textContent = String(count);
    badge.style.display = count > 0 ? "inline-flex" : "none";
  }

  function rerender() {
    if (!state.data) return;
    var filtered = applyFilters(state.data, readFilters());
    updateKpis(filtered);
    updateMap(filtered);
    renderActiveTab(filtered);
    updateResetViewBadge();
  }

  function bindEvents() {
    document.getElementById("resetFiltersBtn").addEventListener("click", function () {
      FILTER_IDS.forEach(function (id) {
        var el = document.getElementById(id);
        if (el) el.value = "";
      });
      var search = document.getElementById("searchInput");
      if (search) search.value = "";
      rerender();
      showRadarToast("Filters reset");
    });
    FILTER_IDS.concat(["searchInput"]).forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.addEventListener(id === "searchInput" ? "input" : "change", rerender);
    });

    ["toggleShowHotels", "toggleChainScaleView", "toggleTravelInfrastructure", "toggleBrandPenetration"].forEach(function (id) {
      var toggle = document.getElementById(id);
      if (!toggle) return;
      toggle.addEventListener("change", rerender);
    });

    LAYER_TOGGLE_CONFIG.forEach(function (cfg) {
      var toggle = document.getElementById(cfg.id);
      if (!toggle) return;
      state.layerToggles[cfg.tab] = !!toggle.checked;
      toggle.addEventListener("change", function () {
        state.layerToggles[cfg.tab] = !!toggle.checked;
        if (toggle.checked) {
          state.activeTab = cfg.tab;
        } else if (state.activeTab === cfg.tab) {
          var enabled = getEnabledLayerTabs();
          state.activeTab = Object.keys(enabled)[0] || "";
        }
        rerender();
      });
    });
    (function syncActiveTabFromInitialLayerToggles() {
      var enabled = getEnabledLayerTabs();
      var keys = Object.keys(enabled);
      if (!keys.length) state.activeTab = "";
      else if (!enabled[state.activeTab]) state.activeTab = keys[0];
    })();
  }

  async function loadData() {
    var contentArea = document.getElementById("contentArea");
    contentArea.innerHTML = '<div class="loading">Loading Scout data...</div>';
    showSystemStatus("Loading Scout intelligence...", "2-4 seconds");
    try {
      updateSystemStatus("Loading mock intelligence data...", "1-2 seconds");
      var responses = await Promise.all([
        fetch("/api/dealality-scout"),
        fetch("/api/dealality-scout/filters")
      ]);
      if (!responses[0].ok || !responses[1].ok) throw new Error("Could not load Scout API data");

      state.data = await responses[0].json();
      state.filters = await responses[1].json();

      // Live layers: Hotel Census (same source as Radar /api/brand-presence) + travel infrastructure.
      updateSystemStatus("Fetching map layers...", "1-2 seconds");
      state.liveMapMode = false;
      state.liveHotels = [];
      state.liveInfrastructure = [];
      try {
        var hotelsFetch = await fetch("/api/brand-presence?limit=100000");
        if (hotelsFetch.ok) {
          var hotelsJson = await hotelsFetch.json();
          if (hotelsJson.success && hotelsJson.hotels) {
            state.liveHotels = (hotelsJson.hotels || []).filter(function (h) {
              return Number.isFinite(Number(h.lat)) && Number.isFinite(Number(h.lng)) && (Number(h.lat) !== 0 || Number(h.lng) !== 0);
            }).map(function (h) {
              return {
                id: h.id,
                name: h.name,
                city: h.city,
                country: h.country,
                brand: h.brand,
                parentCompany: h.parentCompany,
                status: h.status,
                lat: Number(h.lat),
                lng: Number(h.lng),
                rooms: h.rooms,
                chainScale: h.chainScale,
                propertyType: h.propertyType,
                region: h.region,
                locationType: h.locationType
              };
            });
          }
        }
      } catch (_hotelErr) {
        state.liveHotels = [];
      }
      try {
        var infraFetch = await fetch("/api/travel-infrastructure");
        if (infraFetch.ok) {
          var infraJson = await infraFetch.json();
          state.liveInfrastructure = (infraJson.infrastructure || []).filter(function (i) {
            return Number.isFinite(Number(i.lat)) && Number.isFinite(Number(i.lng)) && (Number(i.lat) !== 0 || Number(i.lng) !== 0);
          }).map(function (i) {
            return {
              name: i.name,
              type: i.type,
              city: i.city,
              country: i.country,
              lat: Number(i.lat),
              lng: Number(i.lng)
            };
          });
        }
      } catch (_infraErr) {
        state.liveInfrastructure = [];
      }
      state.liveMapMode = state.liveHotels.length > 0 || state.liveInfrastructure.length > 0;

      populateSelect("countryFilter", state.filters.countries);
      populateSelect("marketFilter", state.filters.markets);
      populateSelect("submarketFilter", state.filters.submarkets);
      populateSelect("brandFilter", state.filters.brands);
      populateSelect("parentCompanyFilter", state.filters.parentCompanies);
      populateSelect("chainScaleFilter", state.filters.chainScales);
      populateSelect("serviceModelFilter", state.filters.serviceModels);
      populateSelect("opportunityTypeFilter", state.filters.opportunityTypes);
      populateSelect("confidenceLevelFilter", state.filters.confidenceLevels);
      populateSelect("reviewStatusFilter", state.filters.reviewStatuses);
      populateSelect("relationshipPathStatusFilter", state.filters.relationshipPathStatuses);

      rerender();
      updateMapLegend(state.data || { assets: [], targetOpportunities: [] });
      showRadarToast("Scout data loaded");
      if (state.liveMapMode) {
        showRadarToast("Hybrid mode: live map layers enabled");
      } else {
        showRadarToast("Mock mode: map layers using Scout dataset");
      }
    } catch (error) {
      contentArea.innerHTML = '<div class="error">Failed to load Scout data. ' + esc(error.message) + "</div>";
      showRadarToast("Failed to load Scout data");
    } finally {
      hideSystemStatus();
    }
  }

  bindEvents();
  loadData();
})();
