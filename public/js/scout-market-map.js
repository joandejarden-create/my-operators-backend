(function () {
  "use strict";

  var SCOUT_MAP_STORAGE_KEY = "scout_market_map_filters_v1";
  var SCOUT_MAP_OVERLAY_STORAGE_KEY = "scout_market_map_overlays_v1";
  var SIGNAL_TYPES = [
    "parent_company_market_gap",
    "brand_market_gap",
    "independent_conversion_cluster",
    "large_independent_asset",
    "pipeline_activity",
    "rebrand_candidate",
    "operator_opportunity_market",
  ];
  var REVIEW_STATUSES = [
    "New",
    "Watchlist",
    "Researching",
    "Ready for Outreach",
    "Dismissed",
    "Deal Created",
  ];

  /** Seed countries so filters work before the first census load. */
  var SCOUT_BOOTSTRAP_COUNTRIES = [
    "Mexico",
    "Colombia",
    "Costa Rica",
    "Dominican Republic",
    "Panama",
    "Peru",
    "Puerto Rico",
    "Jamaica",
    "Guatemala",
    "Honduras",
    "El Salvador",
    "Nicaragua",
    "Belize",
    "United States",
    "Canada",
    "Brazil",
    "Argentina",
    "Chile",
  ];

  var SCOUT_DEFAULT_COUNTRY = "Mexico";

  var state = {
    activeView: "supply",
    data: null,
    insights: null,
    signalById: {},
    map: null,
    demandAnchorsAvailable: true,
    layers: {
      hotels: null,
      signals: null,
      saved: null,
      clusters: null,
      travelInfra: null,
      demandAnchors: null,
    },
    overlays: {
      hotels: true,
      signals: true,
      saved: true,
      travelInfra: true,
      demandAnchors: true,
    },
  };

  function esc(v) {
    var d = document.createElement("div");
    d.textContent = v == null ? "" : String(v);
    return d.innerHTML;
  }

  function el(id) {
    return document.getElementById(id);
  }

  function populateSelect(id, options, allLabel) {
    var select = el(id);
    if (!select) return;
    var html = '<option value="">' + esc(allLabel || "All") + "</option>";
    (options || []).forEach(function (opt) {
      html += '<option value="' + esc(opt) + '">' + esc(opt) + "</option>";
    });
    select.innerHTML = html;
  }

  function mergeSelectOptions(id, options, allLabel) {
    var select = el(id);
    if (!select) return;
    var current = select.value;
    var merged = {};
    Array.from(select.options).forEach(function (opt) {
      if (opt.value) merged[opt.value] = 1;
    });
    (options || []).forEach(function (opt) {
      if (opt) merged[opt] = 1;
    });
    populateSelect(id, Object.keys(merged).sort(), allLabel);
    if (current && merged[current]) select.value = current;
  }

  function bootstrapCountrySelect() {
    populateSelect("scoutMapCountry", SCOUT_BOOTSTRAP_COUNTRIES, "Select country");
  }

  function ensureCountryForLoad(filters) {
    if (filters.country) return false;
    filters.country = SCOUT_DEFAULT_COUNTRY;
    if (el("scoutMapCountry")) el("scoutMapCountry").value = SCOUT_DEFAULT_COUNTRY;
    return true;
  }

  function readFilters() {
    return {
      country: el("scoutMapCountry")?.value || "",
      market: el("scoutMapMarket")?.value || "",
      submarket: el("scoutMapSubmarket")?.value || "",
      parentCompany: el("scoutMapParentCompany")?.value || "",
      brand: el("scoutMapBrand")?.value || "",
      chainScale: el("scoutMapChainScale")?.value || "",
      status: el("scoutMapStatus")?.value || "",
      signalType: el("scoutMapSignalType")?.value || "",
      reviewStatus: el("scoutMapReviewStatus")?.value || "",
      minPriorityScore: el("scoutMapMinPriority")?.value || "",
      overlayCategory: el("scoutMapOverlayCategory")?.value || "",
    };
  }

  function readOverlayToggles() {
    return {
      hotels: el("scoutMapShowHotels")?.checked !== false,
      signals: el("scoutMapShowSignals")?.checked !== false,
      saved: el("scoutMapShowSaved")?.checked !== false,
      travelInfra: el("scoutMapShowTravelInfra")?.checked !== false,
      demandAnchors: el("scoutMapShowDemandAnchors")?.checked !== false,
    };
  }

  function saveOverlayToggles(toggles) {
    try {
      localStorage.setItem(SCOUT_MAP_OVERLAY_STORAGE_KEY, JSON.stringify(toggles));
    } catch (_e) {}
  }

  function loadOverlayToggles() {
    try {
      var raw = localStorage.getItem(SCOUT_MAP_OVERLAY_STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (_e) {
      return null;
    }
  }

  function applyOverlayToggles(toggles) {
    if (!toggles) return;
    if (el("scoutMapShowHotels")) el("scoutMapShowHotels").checked = toggles.hotels !== false;
    if (el("scoutMapShowSignals")) el("scoutMapShowSignals").checked = toggles.signals !== false;
    if (el("scoutMapShowSaved")) el("scoutMapShowSaved").checked = toggles.saved !== false;
    if (el("scoutMapShowTravelInfra")) el("scoutMapShowTravelInfra").checked = toggles.travelInfra !== false;
    if (el("scoutMapShowDemandAnchors")) el("scoutMapShowDemandAnchors").checked = toggles.demandAnchors !== false;
    state.overlays = readOverlayToggles();
  }

  function saveFiltersToStorage(filters) {
    try {
      localStorage.setItem(SCOUT_MAP_STORAGE_KEY, JSON.stringify(filters));
    } catch (_e) {}
  }

  function loadFiltersFromStorage() {
    try {
      var raw = localStorage.getItem(SCOUT_MAP_STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (_e) {
      return null;
    }
  }

  function applyFiltersToForm(filters) {
    if (!filters) return;
    Object.keys(filters).forEach(function (key) {
      var map = {
        country: "scoutMapCountry",
        market: "scoutMapMarket",
        submarket: "scoutMapSubmarket",
        parentCompany: "scoutMapParentCompany",
        brand: "scoutMapBrand",
        chainScale: "scoutMapChainScale",
        status: "scoutMapStatus",
        signalType: "scoutMapSignalType",
        reviewStatus: "scoutMapReviewStatus",
        minPriorityScore: "scoutMapMinPriority",
        overlayCategory: "scoutMapOverlayCategory",
      };
      var node = el(map[key]);
      if (node) node.value = filters[key] || "";
    });
  }

  function buildInsightQuery(filters) {
    var q = new URLSearchParams();
    q.set("includeDemandOverlays", "1");
    q.set("includeSavedSignals", "1");
    q.set("includeInsightReview", "1");
    q.set("limit", "100");
    ["country", "city", "market", "submarket", "parentCompany", "brand", "chainScale", "locationType"].forEach(
      function (key) {
        if (filters[key]) q.set(key, filters[key]);
      }
    );
    return q.toString();
  }

  function loadInsights(callback) {
    var filters = readFilters();
    el("scoutMapLoadStatus").textContent = "Loading market insights…";
    fetch("/api/scout/market-insights?" + buildInsightQuery(filters))
      .then(function (r) {
        return r.json();
      })
      .then(function (json) {
        if (!json.success) throw new Error(json.error || "Insights load failed");
        state.insights = json;
        if (callback) callback();
        var qs = json.insightQualitySummary || {};
        el("scoutMapLoadStatus").textContent =
          "Loaded " +
          (json.summary?.insightsReturned || 0) +
          " insights · " +
          (qs.strong || 0) +
          " strong · read-only";
      })
      .catch(function (err) {
        setError(err.message);
        el("scoutMapLoadStatus").textContent = "Insight load failed";
      });
  }
  function readInsightCardFilters() {
    return {
      quality: el("scoutMapInsightQuality")?.value || "",
      priority: el("scoutMapInsightPriority")?.value || "",
      confidence: el("scoutMapInsightConfidence")?.value || "",
      insightType: el("scoutMapInsightType")?.value || "",
    };
  }

  function filterInsightsForDisplay(list) {
    var f = readInsightCardFilters();
    return (list || []).filter(function (ins) {
      if (f.quality && (ins.insightQuality || "") !== f.quality) return false;
      if (f.priority && (ins.priority || "") !== f.priority) return false;
      if (f.confidence && (ins.confidence || "") !== f.confidence) return false;
      if (f.insightType && ins.insightType !== f.insightType) return false;
      return true;
    });
  }

  function renderInsightSummary(data) {
    var wrap = el("scoutMapInsightSummary");
    if (!wrap || !data) return;
    var qs = data.insightQualitySummary || {};
    var gapCount =
      (data.summary && data.summary.dataGaps) ||
      (data.insightReviews || []).reduce(function (n, i) {
        return n + (i.dataGaps ? i.dataGaps.length : 0);
      }, 0);
    var items = [
      ["Strong Insights", qs.strong || 0],
      ["Directional Insights", qs.directional || 0],
      ["Weak Insights", qs.weak || 0],
      ["Data Gaps", gapCount],
    ];
    wrap.innerHTML = items
      .map(function (pair) {
        return (
          '<article class="scout-map-kpi"><div class="label">' +
          esc(pair[0]) +
          '</div><div class="value">' +
          esc(pair[1]) +
          "</div></article>"
        );
      })
      .join("");
  }

  function populateInsightTypeFilter(insights) {
    var sel = el("scoutMapInsightType");
    if (!sel) return;
    var current = sel.value;
    var types = {};
    (insights || []).forEach(function (i) {
      if (i.insightType) types[i.insightType] = 1;
    });
    var keys = Object.keys(types).sort();
    sel.innerHTML =
      '<option value="">All types</option>' +
      keys
        .map(function (t) {
          return '<option value="' + esc(t) + '">' + esc(t.replace(/_/g, " ")) + "</option>";
        })
        .join("");
    if (current && types[current]) sel.value = current;
  }

  function evidencePanelHtml(ins) {
    var items = ins.evidenceItems || [];
    var gaps = ins.dataGaps || [];
    var hotels = ins.relatedHotelExamples || [];
    var drivers = ins.relatedDemandDriverExamples || [];
    var signals = ins.relatedSignalExamples || [];
    var questions = ins.suggestedReviewQuestions || [];

    var evidenceList =
      items.length > 0
        ? '<ul class="scout-map-evidence-list">' +
          items
            .map(function (e) {
              return (
                "<li><strong>" +
                esc(e.label) +
                "</strong>: " +
                esc(e.value) +
                (e.confidence ? " <em>(" + esc(e.confidence) + ")</em>" : "") +
                "</li>"
              );
            })
            .join("") +
          "</ul>"
        : "<p>No structured evidence items.</p>";

    var gapsHtml =
      gaps.length > 0
        ? '<h5>Data Gaps</h5><ul class="scout-map-evidence-list scout-map-gap-list">' +
          gaps
            .map(function (g) {
              return "<li><strong>" + esc(g.label) + "</strong>: " + esc(g.detail) + "</li>";
            })
            .join("") +
          "</ul>"
        : "";

    var hotelsHtml =
      hotels.length > 0
        ? '<h5>Related Hotels</h5><ul class="scout-map-evidence-list">' +
          hotels
            .map(function (h) {
              return (
                "<li>" +
                esc(h.hotelName) +
                " · " +
                esc([h.affiliation, h.chainScale, h.rooms ? h.rooms + " rooms" : ""].filter(Boolean).join(" · ")) +
                "</li>"
              );
            })
            .join("") +
          "</ul>"
        : "";

    var driversHtml =
      drivers.length > 0
        ? '<h5>Related Demand Drivers</h5><ul class="scout-map-evidence-list">' +
          drivers
            .map(function (d) {
              return "<li>" + esc(d.name) + " · " + esc(d.category) + "</li>";
            })
            .join("") +
          "</ul>"
        : "";

    var signalsHtml =
      signals.length > 0
        ? '<h5>Related Signals</h5><ul class="scout-map-evidence-list">' +
          signals
            .map(function (s) {
              return "<li>" + esc(s.title) + "</li>";
            })
            .join("") +
          "</ul>"
        : "";

    var questionsHtml =
      questions.length > 0
        ? '<h5>Suggested Review Questions</h5><ul class="scout-map-evidence-list scout-map-review-questions">' +
          questions
            .map(function (q) {
              return "<li>" + esc(q) + "</li>";
            })
            .join("") +
          "</ul>"
        : "";

    return (
      '<div class="scout-map-evidence-panel" hidden>' +
      (ins.evidenceSummary ? "<p><strong>Evidence summary:</strong> " + esc(ins.evidenceSummary) + "</p>" : "") +
      (ins.confidenceExplanation
        ? "<p><strong>Confidence:</strong> " + esc(ins.confidenceExplanation) + "</p>"
        : "") +
      (ins.commercialInterpretation
        ? "<p><strong>Commercial read:</strong> " + esc(ins.commercialInterpretation) + "</p>"
        : "") +
      "<h5>Evidence Items</h5>" +
      evidenceList +
      gapsHtml +
      hotelsHtml +
      driversHtml +
      signalsHtml +
      questionsHtml +
      "</div>"
    );
  }

  function insightCardHtml(ins) {
    var priClass = "scout-map-insight-priority-" + (ins.priority || "medium").toLowerCase();
    var qual = (ins.insightQuality || "").toLowerCase();
    var qualClass = qual ? " scout-map-insight-quality-" + qual : "";
    var hasSignal = ins.relatedSignalIds && ins.relatedSignalIds.length > 0;
    var action = ins.suggestedReviewAction || (hasSignal ? "Save" : "Review");

    var badges =
      '<div class="meta">' +
      (ins.insightQuality
        ? '<span class="scout-map-badge quality-' + esc(qual) + '">' + esc(ins.insightQuality) + " quality</span>"
        : "") +
      '<span class="scout-map-badge">' + esc(ins.priority) + " priority</span>" +
      '<span class="scout-map-badge">' + esc(ins.confidence) + " confidence</span>" +
      (action ? '<span class="scout-map-badge">Suggested: ' + esc(action) + "</span>" : "") +
      "</div>";

    var actions =
      '<div class="scout-map-card-actions">' +
      (hasSignal && action === "Save"
        ? '<button type="button" class="scout-map-btn primary scout-map-insight-save-btn" data-signal-id="' +
          esc(ins.relatedSignalIds[0]) +
          '">Save Related Signal to Watchlist</button>'
        : "") +
      '<button type="button" class="scout-map-btn scout-map-insight-review-btn" data-insight-id="' +
      esc(ins.insightId) +
      '">Mark for Review</button>' +
      (action === "Watch"
        ? '<button type="button" class="scout-map-btn scout-map-insight-watch-btn" data-insight-id="' +
          esc(ins.insightId) +
          '">Add to Watch</button>'
        : "") +
      "</div>";

    return (
      '<article class="scout-map-card scout-map-insight-card ' +
      priClass +
      qualClass +
      '">' +
      '<div class="scout-map-insight-type">' +
      esc(ins.insightType.replace(/_/g, " ")) +
      "</div>" +
      "<h4>" +
      esc(ins.title) +
      "</h4>" +
      '<p class="scout-map-insight-text">' +
      esc(ins.insightText) +
      "</p>" +
      (ins.evidenceSummary && !ins.evidenceItems
        ? '<p class="scout-map-insight-why"><strong>Evidence:</strong> ' + esc(ins.evidenceSummary) + "</p>"
        : "") +
      (ins.dataGaps && ins.dataGaps.length && !ins.evidenceItems
        ? '<p class="scout-map-insight-why"><strong>Data gaps:</strong> ' +
          esc(ins.dataGaps.map(function (g) { return g.label; }).join("; ")) +
          "</p>"
        : "") +
      '<p class="scout-map-insight-why"><strong>Why it matters:</strong> ' +
      esc(ins.whyItMatters) +
      "</p>" +
      badges +
      '<p class="scout-map-next-step"><strong>Next step:</strong> ' +
      esc(ins.recommendedNextStep) +
      "</p>" +
      '<button type="button" class="scout-map-evidence-toggle" aria-expanded="false">Show evidence ▾</button>' +
      evidencePanelHtml(ins) +
      actions +
      "</article>"
    );
  }

  function bindEvidenceToggles() {
    document.querySelectorAll(".scout-map-evidence-toggle").forEach(function (btn) {
      btn.onclick = function () {
        var panel = btn.nextElementSibling;
        if (!panel) return;
        var open = panel.hidden;
        panel.hidden = !open;
        btn.setAttribute("aria-expanded", open ? "true" : "false");
        btn.textContent = open ? "Hide evidence ▴" : "Show evidence ▾";
      };
    });
  }

  function renderRankedOpportunities(list) {
    var panel = el("scoutMapRankedPanel");
    var wrap = el("scoutMapRankedList");
    if (!panel || !wrap) return;
    if (!list || !list.length) {
      panel.hidden = true;
      wrap.innerHTML = "";
      return;
    }
    panel.hidden = false;
    wrap.innerHTML = list
      .map(function (opp) {
        return (
          '<article class="scout-map-ranked-card">' +
          '<div class="scout-map-ranked-score">Score ' +
          esc(opp.score) +
          " · " +
          esc(opp.confidence) +
          "</div>" +
          "<strong>" +
          esc(opp.title) +
          "</strong>" +
          "<p>" +
          esc(opp.rationale) +
          "</p>" +
          "<p><em>" +
          esc(opp.suggestedAction) +
          "</em></p>" +
          "</article>"
        );
      })
      .join("");
  }

  function renderInsightView() {
    var panel = el("scoutMapResults");
    var title = el("scoutMapPanelTitle");
    var toolbar = el("scoutMapInsightToolbar");
    if (title) title.textContent = "Market insights";
    if (!panel) return;

    if (toolbar) toolbar.hidden = false;

    if (!state.insights) {
      panel.innerHTML = '<div class="scout-map-empty">Loading insights…</div>';
      loadInsights(function () {
        renderInsightView();
      });
      return;
    }

    var source = state.insights.insightReviews || state.insights.insights || [];
    populateInsightTypeFilter(source);
    renderInsightSummary(state.insights);

    var ins = filterInsightsForDisplay(source);
    panel.innerHTML =
      ins.length > 0
        ? ins.map(insightCardHtml).join("")
        : '<div class="scout-map-empty">No insights match current filters. Try broadening geography or clearing insight filters.</div>';
    renderRankedOpportunities(state.insights.rankedOpportunities || []);
    bindInsightActions();
    bindEvidenceToggles();
  }

  function bindInsightFilters() {
    ["scoutMapInsightQuality", "scoutMapInsightPriority", "scoutMapInsightConfidence", "scoutMapInsightType"].forEach(
      function (id) {
        var node = el(id);
        if (!node) return;
        node.addEventListener("change", function () {
          if (state.activeView === "insights") renderInsightView();
        });
      }
    );
  }

  function bindInsightActions() {
    document.querySelectorAll(".scout-map-insight-save-btn").forEach(function (btn) {
      btn.onclick = function () {
        saveSignal(btn.getAttribute("data-signal-id"), "Watchlist");
      };
    });
    document.querySelectorAll(".scout-map-insight-review-btn").forEach(function (btn) {
      btn.onclick = function () {
        setError("");
        el("scoutMapLoadStatus").textContent =
          "Marked for review (local only) — use Save Related Signal when a Scout signal is linked.";
      };
    });
    document.querySelectorAll(".scout-map-insight-watch-btn").forEach(function (btn) {
      btn.onclick = function () {
        setError("");
        el("scoutMapLoadStatus").textContent =
          "Watch noted (local only) — save a related Scout signal to persist on the watchlist.";
      };
    });
  }

  function buildQuery(filters) {
    var q = new URLSearchParams();
    q.set("includePipeline", "1");
    q.set("includeSignals", "1");
    q.set("includeSavedSignals", "1");
    q.set("includeDemandOverlays", "1");
    q.set("limit", "200");
    Object.keys(filters).forEach(function (key) {
      if (filters[key]) q.set(key, filters[key]);
    });
    if (filters.overlayCategory) q.set("category", filters.overlayCategory);
    return q.toString();
  }

  function overlayCategoryMatches(marker) {
    var cat = el("scoutMapOverlayCategory")?.value || "";
    if (!cat) return true;
    return marker.category === cat;
  }

  function setError(msg) {
    var box = el("scoutMapError");
    if (!box) return;
    if (msg) {
      box.textContent = msg;
      box.style.display = "block";
    } else {
      box.style.display = "none";
    }
  }

  function renderKpis(summary, demandOverlaySummary) {
    var wrap = el("scoutMapKpis");
    if (!wrap || !summary) return;
    var demandCount =
      (demandOverlaySummary && demandOverlaySummary.overlayMarkers) || 0;
    var items = [
      ["Open Hotels", summary.openHotels],
      ["Pipeline Hotels", summary.pipelineHotels],
      ["Branded Hotels", summary.brandedHotels],
      ["Independent Hotels", summary.independentHotels],
      ["Generated Signals", summary.signalMarkers],
      ["Saved Signals", summary.savedSignalMarkers],
      ["Demand Drivers", demandCount],
    ];
    wrap.innerHTML = items
      .map(function (pair) {
        return (
          '<article class="scout-map-kpi"><div class="label">' +
          esc(pair[0]) +
          '</div><div class="value">' +
          esc(pair[1]) +
          "</div></article>"
        );
      })
      .join("");
  }

  function mapLegend() {
    return window.DealalityMapLegend;
  }

  function renderLegend() {
    var leg = mapLegend();
    var mount = el("scoutMapLegendMount");
    if (!leg || !mount) return;
    if (!mount.dataset.mounted) {
      leg.render(mount, { mode: "scout" });
      mount.dataset.mounted = "1";
    }
    leg.updateVisibility(mount, {
      hotels: state.overlays.hotels,
      travelInfra: state.overlays.travelInfra,
      demandAnchors: state.overlays.demandAnchors && state.demandAnchorsAvailable,
      scoutLayers: state.overlays.signals || state.overlays.saved,
    });
  }

  function addMarker(layer, lat, lng, icon, popupHtml) {
    var marker = L.marker([lat, lng], { icon: icon });
    if (popupHtml) marker.bindPopup(popupHtml);
    marker.addTo(layer);
  }

  function initMap() {
    if (state.map) return;
    state.map = L.map("scoutMarketMap", { zoomControl: true }).setView([23.5, -102], 5);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap",
      maxZoom: 18,
    }).addTo(state.map);
    state.layers.hotels = L.layerGroup().addTo(state.map);
    state.layers.signals = L.layerGroup().addTo(state.map);
    state.layers.saved = L.layerGroup().addTo(state.map);
    state.layers.clusters = L.layerGroup().addTo(state.map);
    state.layers.travelInfra = L.layerGroup().addTo(state.map);
    state.layers.demandAnchors = L.layerGroup().addTo(state.map);
  }

  function clearLayers() {
    Object.keys(state.layers).forEach(function (k) {
      if (state.layers[k]) state.layers[k].clearLayers();
    });
  }

  function fitMapBounds() {
    var points = [];
    (state.data.hotelMarkers || []).forEach(function (m) {
      points.push([m.latitude, m.longitude]);
    });
    (state.data.signalMarkers || []).forEach(function (m) {
      points.push([m.latitude, m.longitude]);
    });
    (state.data.savedSignalMarkers || []).forEach(function (m) {
      if (m.latitude != null) points.push([m.latitude, m.longitude]);
    });
    (state.data.demandOverlayMarkers || []).forEach(function (m) {
      points.push([m.latitude, m.longitude]);
    });
    (state.data.marketClusters || []).forEach(function (c) {
      if (c.representativeLatitude != null) {
        points.push([c.representativeLatitude, c.representativeLongitude]);
      }
    });
    if (points.length) {
      state.map.fitBounds(points, { padding: [24, 24], maxZoom: 10 });
    }
  }

  function renderMapForView(view) {
    clearLayers();
    var d = state.data;
    if (!d || !state.map) return;

    var leg = mapLegend();
    var ov = state.overlays;
    var showHotels =
      ov.hotels && (view === "supply" || view === "pipeline" || view === "conversion");
    var showSignals = ov.signals && view !== "watchlist";
    var showSaved = ov.saved && (view === "watchlist" || view === "whitespace");
    var showClusters = view === "whitespace" || view === "conversion" || view === "pipeline";
    var showTravelInfra = ov.travelInfra;
    var showDemandAnchors = ov.demandAnchors && state.demandAnchorsAvailable;

    renderLegend();

    if (showHotels) {
      (d.hotelMarkers || []).forEach(function (m) {
        if (view === "pipeline" && m.status !== "Pipeline") return;
        if (view === "conversion" && !m.isIndependent) return;
        var color = leg ? leg.hotelStatusColor(m.status) : "#2563eb";
        var icon = leg ? leg.createCircleIcon(color, 10) : null;
        if (icon) {
          addMarker(
            state.layers.hotels,
            m.latitude,
            m.longitude,
            icon,
            "<strong>" + esc(m.hotelName) + "</strong><br>" + esc(m.popupSubtitle)
          );
        } else {
          L.circleMarker([m.latitude, m.longitude], {
            radius: 5,
            color: color,
            fillColor: color,
            fillOpacity: 0.75,
            weight: 1,
          })
            .bindPopup("<strong>" + esc(m.hotelName) + "</strong><br>" + esc(m.popupSubtitle))
            .addTo(state.layers.hotels);
        }
      });
    }

    if (showSignals) {
      var types =
        view === "whitespace"
          ? ["parent_company_market_gap", "brand_market_gap", "operator_opportunity_market"]
          : view === "conversion"
            ? ["independent_conversion_cluster", "large_independent_asset", "rebrand_candidate"]
            : view === "pipeline"
              ? ["pipeline_activity"]
              : null;

      (d.signalMarkers || []).forEach(function (m) {
        if (types && types.indexOf(m.signalType) === -1) return;
        var icon = leg ? leg.createCircleIcon("#fbbf24", 12) : null;
        if (icon) {
          addMarker(
            state.layers.signals,
            m.latitude,
            m.longitude,
            icon,
            "<strong>" + esc(m.title) + "</strong><br>" + esc(m.reason)
          );
        } else {
          L.circleMarker([m.latitude, m.longitude], {
            radius: 7,
            color: "#f59e0b",
            fillColor: "#fbbf24",
            fillOpacity: 0.9,
            weight: 2,
          })
            .bindPopup("<strong>" + esc(m.title) + "</strong><br>" + esc(m.reason))
            .addTo(state.layers.signals);
        }
      });
    }

    if (showSaved) {
      (d.savedSignalMarkers || []).forEach(function (m) {
        if (!m.latitude) return;
        var icon = leg ? leg.createCircleIcon("#34d399", 12) : null;
        if (icon) {
          addMarker(
            state.layers.saved,
            m.latitude,
            m.longitude,
            icon,
            "<strong>" + esc(m.signalTitle) + "</strong><br>" + esc(m.reviewStatus)
          );
        } else {
          L.circleMarker([m.latitude, m.longitude], {
            radius: 7,
            color: "#10b981",
            fillColor: "#34d399",
            fillOpacity: 0.9,
            weight: 2,
          })
            .bindPopup("<strong>" + esc(m.signalTitle) + "</strong><br>" + esc(m.reviewStatus))
            .addTo(state.layers.saved);
        }
      });
    }

    if (showClusters) {
      (d.marketClusters || []).forEach(function (c) {
        if (c.representativeLatitude == null) return;
        var icon = leg ? leg.createHexIcon("#a78bfa", 14) : null;
        if (icon) {
          addMarker(
            state.layers.clusters,
            c.representativeLatitude,
            c.representativeLongitude,
            icon,
            "<strong>" +
              esc(c.market || c.submarket || "Cluster") +
              "</strong><br>Signals: " +
              esc(c.signalCount) +
              " · Hotels: " +
              esc(c.openHotels)
          );
        } else {
          L.circleMarker([c.representativeLatitude, c.representativeLongitude], {
            radius: 10,
            color: "#8b5cf6",
            fillColor: "#a78bfa",
            fillOpacity: 0.35,
            weight: 2,
          })
            .bindPopup(
              "<strong>" +
                esc(c.market || c.submarket || "Cluster") +
                "</strong><br>Signals: " +
                esc(c.signalCount) +
                " · Hotels: " +
                esc(c.openHotels)
            )
            .addTo(state.layers.clusters);
        }
      });
    }

    if (showTravelInfra) {
      (d.demandOverlayMarkers || []).forEach(function (m) {
        if (m.overlayType !== "travel_infrastructure") return;
        if (!overlayCategoryMatches(m)) return;
        var color = leg ? leg.infraColor(m.category) : "#9c27b0";
        var icon = leg ? leg.createPentagonIcon(color, 14) : null;
        if (icon) {
          addMarker(
            state.layers.travelInfra,
            m.latitude,
            m.longitude,
            icon,
            "<strong>" + esc(m.popupTitle) + "</strong><br>" + esc(m.popupSubtitle) +
              (m.notes ? "<br><em>" + esc(m.notes) + "</em>" : "")
          );
        } else {
          L.circleMarker([m.latitude, m.longitude], {
            radius: 8,
            color: color,
            fillColor: color,
            fillOpacity: 0.85,
            weight: 2,
          })
            .bindPopup(
              "<strong>" + esc(m.popupTitle) + "</strong><br>" + esc(m.popupSubtitle) +
                (m.notes ? "<br><em>" + esc(m.notes) + "</em>" : "")
            )
            .addTo(state.layers.travelInfra);
        }
      });
    }

    if (showDemandAnchors) {
      (d.demandOverlayMarkers || []).forEach(function (m) {
        if (m.overlayType !== "demand_anchor") return;
        if (!overlayCategoryMatches(m)) return;
        var color = leg ? leg.anchorColor(m.category) : "#26a69a";
        var icon = leg ? leg.createDiamondIcon(color, 14) : null;
        if (icon) {
          addMarker(
            state.layers.demandAnchors,
            m.latitude,
            m.longitude,
            icon,
            "<strong>" + esc(m.popupTitle) + "</strong><br>" + esc(m.popupSubtitle) +
              (m.confidence ? "<br>Confidence: " + esc(m.confidence) : "")
          );
        } else {
          L.circleMarker([m.latitude, m.longitude], {
            radius: 8,
            color: color,
            fillColor: color,
            fillOpacity: 0.85,
            weight: 2,
          })
            .bindPopup(
              "<strong>" + esc(m.popupTitle) + "</strong><br>" + esc(m.popupSubtitle) +
                (m.confidence ? "<br>Confidence: " + esc(m.confidence) : "")
            )
            .addTo(state.layers.demandAnchors);
        }
      });
    }

    fitMapBounds();
  }

  function renderNoCoordsPanel() {
    var panel = el("scoutMapNoCoordsPanel");
    var list = el("scoutMapNoCoordsList");
    if (!panel || !list || !state.data) return;

    var items = (state.data.demandOverlayMarkersWithoutCoordinates || []).filter(function (m) {
      if (!overlayCategoryMatches(m)) return false;
      if (m.overlayType === "demand_anchor" && !state.demandAnchorsAvailable) return false;
      if (m.overlayType === "travel_infrastructure" && !state.overlays.travelInfra) return false;
      if (m.overlayType === "demand_anchor" && !state.overlays.demandAnchors) return false;
      return true;
    });

    if (!items.length) {
      panel.hidden = true;
      list.innerHTML = "";
      return;
    }

    panel.hidden = false;
    list.innerHTML = items
      .slice(0, 25)
      .map(function (m) {
        return (
          '<article class="scout-map-overlay-card">' +
          '<div class="scout-map-overlay-type">' + esc(m.overlayType.replace(/_/g, " ")) + "</div>" +
          "<strong>" + esc(m.name) + "</strong><br>" +
          esc(m.category) +
          " · " +
          esc([m.country, m.market, m.submarket].filter(Boolean).join(" / ")) +
          (m.confidence ? "<br>Confidence: " + esc(m.confidence) : "") +
          (m.sourceLabel ? "<br>Source: " + esc(m.sourceLabel) : "") +
          (m.notes ? "<p>" + esc(m.notes) + "</p>" : "") +
          "</article>"
        );
      })
      .join("");
  }

  function signalActionsHtml(signal) {
    if (!signal) return "";
    var saved = signal.saved === true;
    var html = '<div class="scout-map-card-actions">';
    if (!saved) {
      html +=
        '<button type="button" class="scout-map-btn primary scout-map-save-btn" data-signal-id="' +
        esc(signal.signalId) +
        '" data-status="Watchlist">Save to Watchlist</button>';
      html +=
        '<button type="button" class="scout-map-btn scout-map-save-btn" data-signal-id="' +
        esc(signal.signalId) +
        '" data-status="Dismissed">Dismiss</button>';
    } else {
      html += '<span class="scout-map-badge">Saved · ' + esc(signal.savedReviewStatus || "") + "</span>";
    }
    html += "</div>";
    return html;
  }

  function savedActionsHtml(saved) {
    return (
      '<div class="scout-map-card-actions">' +
      '<button type="button" class="scout-map-btn scout-map-patch-btn" data-signal-id="' +
      esc(saved.signalId) +
      '" data-status="Researching">Move to Researching</button>' +
      '<button type="button" class="scout-map-btn scout-map-patch-btn" data-signal-id="' +
      esc(saved.signalId) +
      '" data-status="Ready for Outreach">Ready for Outreach</button>' +
      '<button type="button" class="scout-map-btn scout-map-patch-btn" data-signal-id="' +
      esc(saved.signalId) +
      '" data-status="Dismissed">Dismiss</button>' +
      "</div>"
    );
  }

  function renderResults(view) {
    var panel = el("scoutMapResults");
    var title = el("scoutMapPanelTitle");
    if (!panel || !state.data) return;

    var titles = {
      supply: "Supply results",
      whitespace: "White space signals",
      conversion: "Conversion opportunities",
      pipeline: "Pipeline activity",
      watchlist: "Watchlist",
      insights: "Market insights",
    };
    if (title) title.textContent = titles[view] || "Results";

    if (view === "insights") {
      el("scoutMapRankedPanel")?.removeAttribute("hidden");
      if (el("scoutMapInsightToolbar")) el("scoutMapInsightToolbar").hidden = false;
      renderInsightView();
      return;
    }
    if (el("scoutMapInsightToolbar")) el("scoutMapInsightToolbar").hidden = true;
    el("scoutMapRankedPanel") && (el("scoutMapRankedPanel").hidden = true);

    var html = "";
    var d = state.data;

    if (view === "supply") {
      (d.hotelMarkers || []).slice(0, 40).forEach(function (m) {
        html +=
          '<article class="scout-map-card"><h4>' +
          esc(m.hotelName) +
          "</h4><p>" +
          esc(m.affiliation) +
          " · " +
          esc(m.market) +
          "</p><div class=\"meta\"><span class=\"scout-map-badge\">" +
          esc(m.status) +
          "</span><span class=\"scout-map-badge\">" +
          esc(m.rooms) +
          " rooms</span></div></article>";
      });
    } else if (view === "watchlist") {
      var savedAll = (d.savedSignalMarkers || []).concat([]);
      if (!savedAll.length && d.generatedSignals) {
        d.generatedSignals
          .filter(function (s) {
            return s.saved;
          })
          .forEach(function (s) {
            html +=
              '<article class="scout-map-card"><h4>' +
              esc(s.title) +
              "</h4><p>" +
              esc(s.reason) +
              '</p><div class="meta"><span class="scout-map-badge">' +
              esc(s.savedReviewStatus) +
              "</span></div>" +
              savedActionsHtml({ signalId: s.signalId }) +
              "</article>";
          });
      }
      (d.savedSignalMarkers || []).forEach(function (m) {
        html +=
          '<article class="scout-map-card"><h4>' +
          esc(m.signalTitle) +
          "</h4><p>" +
          esc(m.market) +
          " · " +
          esc(m.reviewStatus) +
          "</p>" +
          savedActionsHtml(m) +
          "</article>";
      });
    } else {
      var filterTypes = {
        whitespace: ["parent_company_market_gap", "brand_market_gap", "operator_opportunity_market"],
        conversion: ["independent_conversion_cluster", "large_independent_asset", "rebrand_candidate"],
        pipeline: ["pipeline_activity"],
      };
      var allowed = filterTypes[view] || [];
      (d.generatedSignals || [])
        .filter(function (s) {
          return allowed.indexOf(s.signalType) !== -1;
        })
        .slice(0, 40)
        .forEach(function (s) {
          html +=
            '<article class="scout-map-card"><h4>' +
            esc(s.title) +
            "</h4><p>" +
            esc(s.reason) +
            '</p><div class="meta"><span class="scout-map-badge">' +
            esc(s.signalType) +
            '</span><span class="scout-map-badge">P' +
            esc(s.priorityScore) +
            "</span></div>" +
            signalActionsHtml(s) +
            "</article>";
        });
      (d.marketClusters || [])
        .filter(function (c) {
          return c.signalCount > 0;
        })
        .slice(0, 15)
        .forEach(function (c) {
          html +=
            '<article class="scout-map-card"><h4>Cluster · ' +
            esc(c.market || c.submarket) +
            "</h4><p>" +
            esc(c.openHotels) +
            " open · " +
            esc(c.signalCount) +
            " signals</p></article>";
        });
    }

    panel.innerHTML = html || '<div class="scout-map-empty">No results for this view.</div>';
    bindCardActions();
  }

  function bindCardActions() {
    document.querySelectorAll(".scout-map-save-btn").forEach(function (btn) {
      btn.onclick = function () {
        var id = btn.getAttribute("data-signal-id");
        var status = btn.getAttribute("data-status");
        saveSignal(id, status);
      };
    });
    document.querySelectorAll(".scout-map-patch-btn").forEach(function (btn) {
      btn.onclick = function () {
        var id = btn.getAttribute("data-signal-id");
        var status = btn.getAttribute("data-status");
        patchSignal(id, { reviewStatus: status });
      };
    });
  }

  function saveSignal(signalId, reviewStatus) {
    var signal = state.signalById[signalId];
    if (!signal) return;
    el("scoutMapLoadStatus").textContent = "Saving…";
    fetch("/api/scout/opportunity-signals/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ signal: signal, reviewStatus: reviewStatus }),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (json) {
        if (!json.success) throw new Error(json.error || "Save failed");
        loadData();
      })
      .catch(function (err) {
        setError(err.message);
        el("scoutMapLoadStatus").textContent = "Save failed";
      });
  }

  function patchSignal(signalId, body) {
    el("scoutMapLoadStatus").textContent = "Updating…";
    fetch("/api/scout/opportunity-signals/" + encodeURIComponent(signalId), {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (json) {
        if (!json.success) throw new Error(json.error || "Update failed");
        loadData();
      })
      .catch(function (err) {
        setError(err.message);
        el("scoutMapLoadStatus").textContent = "Update failed";
      });
  }

  function hydrateOverlayCategories(data) {
    var cats = {};
    (data.demandOverlayMarkers || []).forEach(function (m) {
      if (m.category) cats[m.category] = 1;
    });
    (data.demandOverlayMarkersWithoutCoordinates || []).forEach(function (m) {
      if (m.category) cats[m.category] = 1;
    });
    populateSelect("scoutMapOverlayCategory", Object.keys(cats).sort(), "All categories");
  }

  function updateDemandAnchorsAvailability(data) {
    var hasDemand =
      (data.demandOverlayMarkers || []).some(function (m) {
        return m.overlayType === "demand_anchor";
      }) ||
      (data.demandOverlayMarkersWithoutCoordinates || []).some(function (m) {
        return m.overlayType === "demand_anchor";
      });
    var missingWarning = (data.warnings || []).some(function (w) {
      return /DEMAND_ANCHORS: table not found/i.test(w);
    });
    state.demandAnchorsAvailable = hasDemand || !missingWarning;
    var wrap = el("scoutMapDemandAnchorsToggleWrap");
    if (wrap) wrap.style.display = state.demandAnchorsAvailable ? "" : "none";
  }

  function hydrateFilterOptions(data, activeFilters) {
    var hotels = data.hotelMarkers || [];
    var countries = {};
    var markets = {};
    var submarkets = {};
    var parents = {};
    var brands = {};
    var scales = {};
    hotels.forEach(function (h) {
      if (h.country) countries[h.country] = 1;
      if (h.market) markets[h.market] = 1;
      if (h.submarket) submarkets[h.submarket] = 1;
      if (h.parentCompany) parents[h.parentCompany] = 1;
      if (h.affiliation) brands[h.affiliation] = 1;
      if (h.chainScale) scales[h.chainScale] = 1;
    });
    mergeSelectOptions("scoutMapCountry", Object.keys(countries), "Select country");
    mergeSelectOptions("scoutMapMarket", Object.keys(markets));
    mergeSelectOptions("scoutMapSubmarket", Object.keys(submarkets));
    mergeSelectOptions("scoutMapParentCompany", Object.keys(parents));
    mergeSelectOptions("scoutMapBrand", Object.keys(brands));
    mergeSelectOptions("scoutMapChainScale", Object.keys(scales));
    populateSelect("scoutMapSignalType", SIGNAL_TYPES, "All signal types");
    populateSelect("scoutMapReviewStatus", REVIEW_STATUSES, "All review statuses");
    hydrateOverlayCategories(data);
    if (activeFilters) applyFiltersToForm(activeFilters);
    else {
      var stored = loadFiltersFromStorage();
      if (stored) applyFiltersToForm(stored);
    }
  }

  function loadData() {
    setError("");
    var filters = readFilters();
    var usedDefaultCountry = ensureCountryForLoad(filters);
    saveFiltersToStorage(filters);

    el("scoutMapLoadStatus").textContent = usedDefaultCountry
      ? "Loading " + SCOUT_DEFAULT_COUNTRY + " by default — change Country and click Apply to switch…"
      : "Loading Hotel Census and map layers (first load may take 15–30s)…";
    el("scoutMapResults").innerHTML = '<div class="scout-map-empty">Loading…</div>';

    fetch("/api/scout/market-map?" + buildQuery(filters))
      .then(function (r) {
        return r.json();
      })
      .then(function (json) {
        if (!json.success) throw new Error(json.error || "Load failed");
        state.data = json;
        state.insights = null;
        state.signalById = {};
        (json.generatedSignals || []).forEach(function (s) {
          state.signalById[s.signalId] = s;
        });
        renderKpis(json.summary, json.demandOverlaySummary);
        updateDemandAnchorsAvailability(json);
        hydrateFilterOptions(json, filters);
        applyFiltersToForm(filters);
        initMap();
        renderMapForView(state.activeView);
        if (state.activeView === "insights") {
          loadInsights(function () {
            renderInsightView();
          });
        } else {
          renderResults(state.activeView);
        }
        renderNoCoordsPanel();
        el("scoutMapLoadStatus").textContent =
          "Loaded " +
          (json.summary?.hotelMarkers || 0) +
          " hotels · " +
          ((json.demandOverlaySummary && json.demandOverlaySummary.overlayMarkers) || 0) +
          " demand drivers · read-only";
      })
      .catch(function (err) {
        setError(err.message);
        el("scoutMapResults").innerHTML =
          '<div class="scout-map-empty">Failed to load map data.</div>';
        el("scoutMapLoadStatus").textContent = "Error loading data";
      });
  }

  function bindTabs() {
    document.querySelectorAll(".scout-map-tab").forEach(function (tab) {
      tab.addEventListener("click", function () {
        document.querySelectorAll(".scout-map-tab").forEach(function (t) {
          t.classList.remove("active");
        });
        tab.classList.add("active");
        state.activeView = tab.getAttribute("data-view");
        renderMapForView(state.activeView);
        if (state.activeView === "insights") {
          renderInsightView();
        } else {
          renderResults(state.activeView);
        }
      });
    });
  }

  function bindOverlayToggles() {
    [
      "scoutMapShowHotels",
      "scoutMapShowSignals",
      "scoutMapShowSaved",
      "scoutMapShowTravelInfra",
      "scoutMapShowDemandAnchors",
    ].forEach(function (id) {
      var node = el(id);
      if (!node) return;
      node.addEventListener("change", function () {
        state.overlays = readOverlayToggles();
        saveOverlayToggles(state.overlays);
        renderLegend();
        renderMapForView(state.activeView);
        renderNoCoordsPanel();
      });
    });
  }

  function init() {
    bootstrapCountrySelect();
    populateSelect("scoutMapSignalType", SIGNAL_TYPES, "All signal types");
    populateSelect("scoutMapReviewStatus", REVIEW_STATUSES, "All review statuses");
    populateSelect("scoutMapOverlayCategory", [], "All categories");
    var storedOverlays = loadOverlayToggles();
    if (storedOverlays) applyOverlayToggles(storedOverlays);
    else state.overlays = readOverlayToggles();
    bindOverlayToggles();
    bindTabs();
    bindInsightFilters();
    renderLegend();
    el("scoutMapApplyBtn")?.addEventListener("click", loadData);
    el("scoutMapResetBtn")?.addEventListener("click", function () {
      try {
        localStorage.removeItem(SCOUT_MAP_STORAGE_KEY);
      } catch (_e) {}
      document.querySelectorAll(".scout-map-filters select, .scout-map-filters input").forEach(function (node) {
        node.value = "";
      });
      loadData();
    });
    var stored = loadFiltersFromStorage();
    if (stored) applyFiltersToForm(stored);
    loadData();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
