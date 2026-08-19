/**
 * Brand AI Visibility page controller.
 * Tabs: Executive Summary (default) | Detailed View
 */
(function () {
  var STORAGE_BRAND_KEY = "aiv_brand_selected_brand";
  var STORAGE_GEO_KEY = "aiv_brand_geography";
  var STORAGE_PROVIDER_KEY = "aiv_brand_provider";
  var STORAGE_LANGUAGE_KEY = "aiv_brand_language";

  var marketTrendChart = null;
  var detailTrendChart = null;
  var MARKET_TREND_COLORS = [
    { border: "#9a91fb", fill: "rgba(154, 145, 251, 0.2)" },
    { border: "#57c3ff", fill: "rgba(87, 195, 255, 0.2)" },
    { border: "#f59e0b", fill: "rgba(245, 158, 11, 0.15)" },
    { border: "#14b8a6", fill: "rgba(20, 184, 166, 0.15)" },
    { border: "#f472b6", fill: "rgba(244, 114, 182, 0.15)" },
    { border: "#a3e635", fill: "rgba(163, 230, 53, 0.15)" },
  ];

  var state = {
    tab: "executive",
    geography: "CALA",
    provider: "openai",
    language: null,
    brandId: null,
    intent: "",
    questionFilter: "all",
    detailQuestions: [],
    portfolio: null,
    executive: null,
    overview: null,
    portfolioBrands: [],
    portfolioSortKey: null,
    portfolioSortDir: "asc",
    portfolioSortBound: false,
    portfolioInfoBound: false,
    brandNamesById: {},
    peerRows: [],
    competitorsMeta: null,
    competitorsMetaByProvider: null,
    peerSortKey: null,
    peerSortDir: "asc",
    demoBrandPortfolioKey: null,
    _execLanguageReconciled: false,
    _execCacheFp: null,
    _detailCacheFp: null,
    _detailSecondaryCache: null,
    requestGeneration: 0,
    loadAbort: null,
    loadFilterFp: null,
    staleResponsesDiscarded: 0,
  };

  function currentFilterSnapshot() {
    return {
      tab: state.tab,
      geography: state.geography || "",
      provider: state.provider || "",
      language: state.language || "",
      brandId: state.brandId || "",
      intent: state.intent || "",
      portfolioKey: state.demoBrandPortfolioKey || "",
    };
  }

  function filterSnapshotFp(snap) {
    var s = snap || currentFilterSnapshot();
    return [
      s.tab,
      s.geography,
      s.provider,
      s.language,
      s.brandId,
      s.intent,
      s.portfolioKey,
    ].join("|");
  }

  function execTabCacheFp() {
    return [
      state.geography || "",
      state.provider || "",
      state.language || "",
      state.demoBrandPortfolioKey || "",
    ].join("|");
  }

  function detailTabCacheFp() {
    return [
      state.geography || "",
      state.provider || "",
      state.language || "",
      state.brandId || "",
      state.intent || "",
      state.demoBrandPortfolioKey || "",
      watchlistState.mode || "missing",
    ].join("|");
  }

  function invalidateTabCaches() {
    state._execCacheFp = null;
    state._detailCacheFp = null;
    state._detailSecondaryCache = null;
    state.competitorsMeta = null;
    state.competitorsMetaByProvider = null;
  }

  function mergeCompetitorsRenderData(data) {
    var payload = data && typeof data === "object" ? Object.assign({}, data) : {};
    var providerKey = String(state.provider || "openai").toLowerCase();
    if (
      (Array.isArray(payload.ownerIntentBenchmarks) &&
        payload.ownerIntentBenchmarks.length > 0) ||
      payload.OWNER_INTENT_VISIBLE === true ||
      payload.SCENARIO_BENCHMARK_UI === "LIVE_CERTIFIED_VALUES_ONLY"
    ) {
      if (!state.competitorsMetaByProvider) state.competitorsMetaByProvider = {};
      state.competitorsMetaByProvider[providerKey] = {
        ownerIntentBenchmarks: payload.ownerIntentBenchmarks || [],
        ALL_PROVIDERS_DERIVED: payload.ALL_PROVIDERS_DERIVED === true,
        SCENARIO_BENCHMARK_UI: payload.SCENARIO_BENCHMARK_UI || null,
        CUSTOMER_INDEX_RENDERING: payload.CUSTOMER_INDEX_RENDERING || null,
        OWNER_INTENT_VISIBLE: payload.OWNER_INTENT_VISIBLE === true,
        peerPresentSubjectMissing: payload.peerPresentSubjectMissing || null,
      };
      state.competitorsMeta = state.competitorsMetaByProvider[providerKey];
    } else if (
      state.competitorsMetaByProvider &&
      state.competitorsMetaByProvider[providerKey]
    ) {
      payload = Object.assign({}, state.competitorsMetaByProvider[providerKey], payload);
      state.competitorsMeta = state.competitorsMetaByProvider[providerKey];
    }
    return payload;
  }

  function tryRenderFromTabCache() {
    if (state.tab === "executive") {
      if (
        state.executive &&
        state._execCacheFp &&
        state._execCacheFp === execTabCacheFp()
      ) {
        setActiveTab("executive");
        renderExecutive(state.executive);
        setError(null);
        setBanner(null);
        return true;
      }
      return false;
    }
    if (
      state.overview &&
      state._detailCacheFp &&
      state._detailCacheFp === detailTabCacheFp() &&
      state._detailSecondaryCache
    ) {
      setActiveTab("detail");
      if (state.portfolio) {
        fillBrandSelect(state.portfolio.brands);
      }
      syncFilterControlsFromState();
      paintDetailOverview(state.overview, state.brandId);
      var sec = state._detailSecondaryCache;
      renderDetailTrendChart(sec.trend || { ok: false, points: [] });
      renderCompetitors(
        sec.competitors || { ok: false, peers: [], competitors: [] }
      );
      renderSources(sec.sources || { ok: false, sources: [] });
      if (sec.watchlist) {
        applyWatchlistPayload(sec.watchlist);
      }
      setError(null);
      return true;
    }
    return false;
  }

  function beginLoadGeneration() {
    if (state.loadAbort && typeof state.loadAbort.abort === "function") {
      try {
        state.loadAbort.abort();
      } catch (_) {
        /* ignore */
      }
    }
    state.requestGeneration = (state.requestGeneration || 0) + 1;
    var generation = state.requestGeneration;
    var filterSnap = currentFilterSnapshot();
    state.loadFilterFp = filterSnapshotFp(filterSnap);
    var controller =
      typeof AbortController !== "undefined" ? new AbortController() : null;
    state.loadAbort = controller;
    return {
      generation: generation,
      filterSnap: filterSnap,
      filterFp: state.loadFilterFp,
      signal: controller ? controller.signal : null,
      isCurrent: function () {
        // Generation only. The same load mutates language / demo portfolio key
        // after the first executive-summary response (filter contract reconcile).
        // Comparing live currentFilterSnapshot() here treats that as stale, skips
        // renderExecutive(), and never calls setLoading(false).
        return generation === state.requestGeneration;
      },
    };
  }

  function shouldApplyLoadResult(loadToken, payload) {
    if (!loadToken || typeof loadToken.isCurrent !== "function" || !loadToken.isCurrent()) {
      state.staleResponsesDiscarded = (state.staleResponsesDiscarded || 0) + 1;
      return false;
    }
    if (payload && typeof payload === "object") {
      var pGeo =
        (payload.geography &&
          (payload.geography.key || payload.geography.commercialRegion)) ||
        payload.geographyKey ||
        null;
      var pProv = payload.provider != null ? String(payload.provider) : null;
      var pLang = payload.language != null ? String(payload.language) : null;
      var pBrand = payload.brandId != null ? String(payload.brandId) : null;
      var snap = loadToken.filterSnap || {};
      if (pGeo && snap.geography && String(pGeo) !== String(snap.geography)) {
        state.staleResponsesDiscarded = (state.staleResponsesDiscarded || 0) + 1;
        return false;
      }
      if (
        pProv &&
        snap.provider &&
        String(pProv).toLowerCase() !== String(snap.provider).toLowerCase()
      ) {
        state.staleResponsesDiscarded = (state.staleResponsesDiscarded || 0) + 1;
        return false;
      }
      if (
        pLang &&
        snap.language &&
        String(pLang).toLowerCase() !== String(snap.language).toLowerCase()
      ) {
        state.staleResponsesDiscarded = (state.staleResponsesDiscarded || 0) + 1;
        return false;
      }
      if (
        snap.tab === "detail" &&
        pBrand &&
        snap.brandId &&
        String(pBrand) !== String(snap.brandId)
      ) {
        state.staleResponsesDiscarded = (state.staleResponsesDiscarded || 0) + 1;
        return false;
      }
    }
    return true;
  }

  function $(id) {
    return document.getElementById(id);
  }

  function setLoading(on, message) {
    var el = $("aivLoading");
    if (!el) return;
    var msgEl = $("aivLoadingMessage") || el.querySelector(".toast-message");
    if (msgEl && message) msgEl.textContent = message;
    if (on) {
      if (msgEl && !message) {
        msgEl.textContent =
          state.tab === "detail"
            ? "Loading Detailed View…"
            : "Loading Executive Summary…";
      }
      el.hidden = false;
      el.style.display = "flex";
      el.setAttribute("aria-busy", "true");
      el.classList.remove("show");
      // Force reflow so the slide-in transition runs
      void el.offsetHeight;
      requestAnimationFrame(function () {
        el.classList.add("show");
      });
    } else {
      el.classList.remove("show");
      el.setAttribute("aria-busy", "false");
      setTimeout(function () {
        if (!el.classList.contains("show")) {
          el.style.display = "none";
          el.hidden = true;
        }
      }, 300);
    }
  }

  function setError(msg) {
    var el = $("aivError");
    if (!el) return;
    if (!msg) {
      el.hidden = true;
      el.textContent = "";
      return;
    }
    el.hidden = false;
    el.textContent = msg;
  }

  function setBanner(msg) {
    var el = $("aivStateBanner");
    if (!el) return;
    if (!msg) {
      el.hidden = true;
      el.textContent = "";
      return;
    }
    el.hidden = false;
    el.textContent = msg;
  }

  function setPartialBanner(freshness) {
    var el = $("aivPartialBanner");
    if (!el) return;
    if (!freshness || !freshness.PARTIAL_MONITORING) {
      el.hidden = true;
      el.innerHTML = "";
      return;
    }
    var completed = freshness.PROVIDERS_COMPLETED_DISPLAY || "";
    el.hidden = false;
    el.innerHTML =
      '<strong>Partial monitoring results</strong> — ' +
      AiVisibilityUi.escapeHtml(completed) +
      " providers completed." +
      '<span class="aiv-banner-secondary"> Metrics reflect completed providers only.</span>';
  }

  function renderFreshnessStrip(targetId, freshness) {
    var el = $(targetId);
    if (!el) return;
    if (!freshness) {
      el.hidden = true;
      el.innerHTML = "";
      return;
    }
    el.hidden = false;
    if (freshness.NOT_MONITORED) {
      el.innerHTML =
        '<div class="aiv-freshness-item">' +
        '<span class="aiv-freshness-label">Last monitored</span>' +
        '<span class="aiv-freshness-value">Not Monitored</span>' +
        "</div>" +
        '<div class="aiv-freshness-item">' +
        '<span class="aiv-freshness-label">Mode</span>' +
        '<span class="aiv-freshness-value">' +
        AiVisibilityUi.escapeHtml(freshness.MODE_LABEL || "Latest monitored results") +
        "</span></div>";
      return;
    }
    var partialChip = freshness.PARTIAL_MONITORING
      ? '<span class="aiv-freshness-chip aiv-freshness-chip--partial">Partial monitoring run</span>'
      : "";
    el.innerHTML =
      '<div class="aiv-freshness-item">' +
      '<span class="aiv-freshness-label">Last monitored</span>' +
      '<span class="aiv-freshness-value">' +
      AiVisibilityUi.escapeHtml(freshness.LAST_MONITORED_DISPLAY || "—") +
      "</span></div>" +
      '<div class="aiv-freshness-item">' +
      '<span class="aiv-freshness-label">Providers completed</span>' +
      '<span class="aiv-freshness-value">' +
      AiVisibilityUi.escapeHtml(freshness.PROVIDERS_COMPLETED_DISPLAY || "—") +
      "</span></div>" +
      partialChip +
      '<div class="aiv-freshness-item aiv-freshness-item--mode">' +
      '<span class="aiv-freshness-label">Mode</span>' +
      '<span class="aiv-freshness-value">' +
      AiVisibilityUi.escapeHtml(freshness.MODE_LABEL || "Latest monitored results") +
      "</span></div>";
  }

  function applyMonitoringFreshness(freshness) {
    renderFreshnessStrip("aivFreshnessStrip", freshness);
    setPartialBanner(freshness);
  }

  async function apiGet(path, opts) {
    var auth = window.DealalityMemberstackAuth;
    if (!auth || typeof auth.authFetch !== "function") {
      throw new Error("Sign in required to view Brand AI Visibility.");
    }
    var fetchOpts = {
      method: "GET",
      waitForLogin: true,
      maxWaitMs: 20000,
    };
    if (opts && opts.signal) {
      fetchOpts.signal = opts.signal;
    }
    var res;
    try {
      // Always wait for Memberstack / embed JWT so first paint can load without
      // requiring a manual Run Report click after auth becomes ready.
      res = await auth.authFetch(path, fetchOpts);
    } catch (netErr) {
      if (netErr && (netErr.name === "AbortError" || netErr.code === "ABORT_ERR")) {
        var aborted = new Error("Request aborted");
        aborted.kind = "aborted";
        aborted.aborted = true;
        throw aborted;
      }
      var net = new Error(
        (netErr && netErr.message) ||
          "Network error loading Brand AI Visibility. Check that the local server is running."
      );
      net.kind = "network";
      net.cause = netErr;
      throw net;
    }
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok || data.success === false) {
      var msg = data.message || data.error || "Request failed";
      var kind = "api_error";
      if (
        res.status === 404 ||
        data.error === "API route not found" ||
        msg === "API route not found"
      ) {
        kind = "api_route_missing";
        msg =
          "Brand AI Visibility API route not found on this server. Restart the Node process (npm start) so /api/ai-visibility routes register. This is not an empty-portfolio state.";
      } else if (res.status === 401 || res.status === 403) {
        kind = "auth";
      } else if (
        data.error === "provider_not_monitored" ||
        data.availability === "not_monitored" ||
        data.reasonCode === "PROVIDER_NOT_MONITORED"
      ) {
        kind = "provider_not_monitored";
        msg =
          data.message ||
          ((data.providerLabel || data.provider || "This provider") +
            " has not been monitored for this portfolio and geography yet.");
      } else if (msg === "Failed to fetch" || /failed to fetch/i.test(String(msg))) {
        kind = "network";
        msg =
          "Could not reach the Brand AI Visibility API. Confirm the Node server is running, then retry.";
      }
      var err = new Error(msg);
      err.status = res.status;
      err.payload = data;
      err.kind = kind;
      throw err;
    }
    return data;
  }

  function qs(extra) {
    var p = new URLSearchParams();
    p.set("geography", state.geography);
    p.set("provider", state.provider || "openai");
    if (state.language) {
      p.set("language", state.language);
    }
    // Intent applies on Detailed View (Owner Questions).
    if (state.tab === "detail" && state.intent) {
      p.set("intent", state.intent);
    }
    if (extra) {
      Object.keys(extra).forEach(function (k) {
        if (extra[k] != null && extra[k] !== "") p.set(k, extra[k]);
      });
    }
    return "?" + p.toString();
  }

  function persistBrand(brandId) {
    try {
      if (brandId) sessionStorage.setItem(STORAGE_BRAND_KEY, brandId);
    } catch (_) {
      /* ignore */
    }
  }

  function persistFilterPrefs() {
    try {
      if (state.geography) {
        sessionStorage.setItem(STORAGE_GEO_KEY, String(state.geography));
      }
      if (state.provider) {
        sessionStorage.setItem(STORAGE_PROVIDER_KEY, String(state.provider));
      }
      if (state.language) {
        sessionStorage.setItem(STORAGE_LANGUAGE_KEY, String(state.language));
      } else {
        sessionStorage.removeItem(STORAGE_LANGUAGE_KEY);
      }
    } catch (_) {
      /* ignore */
    }
  }

  function restoreFilterPrefs() {
    try {
      var geo = sessionStorage.getItem(STORAGE_GEO_KEY);
      var provider = sessionStorage.getItem(STORAGE_PROVIDER_KEY);
      var language = sessionStorage.getItem(STORAGE_LANGUAGE_KEY);
      if (geo) state.geography = geo;
      if (provider) state.provider = provider;
      if (language) state.language = language;
    } catch (_) {
      /* ignore */
    }
  }

  function syncFilterControlsFromState() {
    if ($("aivGeography") && state.geography) {
      $("aivGeography").value = state.geography;
    }
    if ($("aivProvider") && state.provider) {
      $("aivProvider").value = state.provider;
    }
    if ($("aivLanguage") && state.language) {
      $("aivLanguage").value = state.language;
    }
  }

  function restoreBrand(brands) {
    var ids = (brands || []).map(function (b) {
      return b.brandId;
    });
    var saved = null;
    try {
      saved = sessionStorage.getItem(STORAGE_BRAND_KEY);
    } catch (_) {
      saved = null;
    }
    if (saved && ids.indexOf(saved) >= 0) return saved;
    return ids[0] || null;
  }

  function syncFilterVisibility() {
    var brandGroup = $("aivBrandFilterGroup");
    var intentGroup = $("aivIntentFilterGroup");
    var intentSel = $("aivIntent");
    var brandSel = $("aivBrand");
    var isDetail = state.tab === "detail";
    if (brandGroup) brandGroup.hidden = !isDetail;
    // Intent filter applied to removed Owner Questions dump — watchlist has its own family filter.
    if (intentGroup) intentGroup.hidden = true;
    if (brandSel) brandSel.disabled = !isDetail;
    if (intentSel) {
      intentSel.disabled = true;
      if (!isDetail) {
        state.intent = "";
        intentSel.value = "";
      }
    }
  }

  function setActiveTab(tab) {
    // Phase 3A.4: only Executive Summary + Detailed View. Stale HDV → executive.
    if (tab === "detail") state.tab = "detail";
    else state.tab = "executive";
    try {
      sessionStorage.setItem("aiv_brand_tab", state.tab);
    } catch (_) {
      /* ignore */
    }

    var execBtn = $("aivTabExecutive");
    var detailBtn = $("aivTabDetail");
    if (execBtn) {
      execBtn.classList.toggle("active", state.tab === "executive");
      execBtn.setAttribute("aria-selected", state.tab === "executive" ? "true" : "false");
    }
    if (detailBtn) {
      detailBtn.classList.toggle("active", state.tab === "detail");
      detailBtn.setAttribute("aria-selected", state.tab === "detail" ? "true" : "false");
    }

    var execView = $("aivExecutiveView");
    var detailView = $("aivDetailView");
    if (execView) execView.hidden = state.tab !== "executive";
    if (detailView) detailView.hidden = state.tab !== "detail";
    syncFilterVisibility();
  }

  function fillBrandSelect(brands) {
    var sel = $("aivBrand");
    var list = brands || [];
    state.brandId = clampBrandToPortfolio(list);
    if (sel) {
      var html = "";
      list.forEach(function (b) {
        html +=
          '<option value="' +
          AiVisibilityUi.escapeHtml(b.brandId) +
          '">' +
          AiVisibilityUi.escapeHtml(b.brandName || b.brandId) +
          "</option>";
      });
      sel.innerHTML = html || '<option value="">No entitled brands</option>';
      if (state.brandId) sel.value = state.brandId;
    }
  }

  /** Keep Detailed View brand inside the active entitled portfolio (Choice ≠ Hilton). */
  function clampBrandToPortfolio(brands) {
    var ids = (brands || [])
      .map(function (b) {
        return b && b.brandId;
      })
      .filter(Boolean);
    if (state.brandId && ids.indexOf(state.brandId) >= 0) return state.brandId;
    return restoreBrand(brands);
  }

  function applyDemoPortfolioKeyFromPayload(data) {
    var nextKey =
      (data && (data.demoBrandPortfolioKey || data.demo_brand_portfolio_key)) || null;
    if (nextKey) nextKey = String(nextKey).trim().toLowerCase();
    else nextKey = null;
    if (
      state.demoBrandPortfolioKey &&
      nextKey &&
      state.demoBrandPortfolioKey !== nextKey
    ) {
      state.brandId = null;
      try {
        sessionStorage.removeItem(STORAGE_BRAND_KEY);
      } catch (_) {
        /* ignore */
      }
    }
    if (nextKey) state.demoBrandPortfolioKey = nextKey;
  }

  function fillProviderSelect(providers, selectorOptions) {
    var sel = $("aivProvider");
    if (!sel) return;
    var list =
      Array.isArray(selectorOptions) && selectorOptions.length
        ? selectorOptions.slice()
        : Array.isArray(providers)
          ? providers.slice()
          : [];
    if (!list.length) {
      list = [
        { id: "all", label: "All Providers" },
        { id: "openai", label: "OpenAI" },
        { id: "gemini", label: "Gemini" },
        { id: "perplexity", label: "Perplexity" },
        { id: "claude", label: "Claude" },
      ];
    }
    if (
      list.length &&
      !list.some(function (p) {
        return (p.id || p) === "all";
      })
    ) {
      list.unshift({
        id: "all",
        label: "All Providers",
        mode: "DERIVED",
        monitored: list.some(function (p) {
          return (p.id || p) !== "all" && p.monitored !== false;
        }),
      });
    }
    var want = state.provider || "openai";
    var ids = list.map(function (p) {
      return p.id || p;
    });
    if (ids.indexOf(want) < 0) want = ids.indexOf("openai") >= 0 ? "openai" : ids[0] || "openai";
    state.provider = want;
    sel.innerHTML = list
      .map(function (p) {
        var id = p.id || p;
        var label =
          p.label ||
          (id === "all"
            ? "All Providers"
            : id === "openai"
              ? "OpenAI"
              : id.charAt(0).toUpperCase() + id.slice(1));
        if (p.monitored === false && id !== "all") {
          label = label + " (Not Monitored)";
        }
        return (
          '<option value="' +
          AiVisibilityUi.escapeHtml(id) +
          '"' +
          (id === want ? " selected" : "") +
          ">" +
          AiVisibilityUi.escapeHtml(label) +
          "</option>"
        );
      })
      .join("");
  }

  function applyAvailableProviders(payload) {
    if (!payload) return;
    fillProviderSelect(
      payload.availableProviders,
      payload.providerSelectorOptions
    );
  }

  /** Preferred display order for Executive Summary tiles (selection/materiality unchanged). */
  var EXEC_INSIGHT_TILE_PRIORITY = [
    "MEANINGFUL_PRESENCE_CHANGE",
    "STRONGEST_PRESENCE_AREA",
    "LARGEST_COMPETITIVE_STRENGTH",
    "WEAKEST_PRESENCE_AREA",
    "LARGEST_COMPETITIVE_GAP",
    "QUESTIONS_MISSING_PATTERN",
    "PROVIDER_DISAGREEMENT",
    "CROSS_PROVIDER_STRENGTH",
  ];

  function sortExecutiveInsightTiles(boxes) {
    return (boxes || []).slice().sort(function (a, b) {
      var ia = EXEC_INSIGHT_TILE_PRIORITY.indexOf(a.type);
      var ib = EXEC_INSIGHT_TILE_PRIORITY.indexOf(b.type);
      if (ia < 0) ia = 99;
      if (ib < 0) ib = 99;
      return ia - ib;
    });
  }

  function buildInsightWatchLine(box) {
    var watch = String(box.whatToWatch || "").trim();
    var soWhat = String(box.soWhat || "").trim();
    if (watch) return watch;
    if (soWhat) return soWhat;
    return "";
  }

  function formatExecutiveDispositionLabel(raw) {
    if (!raw) return "";
    var key = String(raw).trim().toUpperCase();
    var map = {
      ACTION_REQUIRED: "Action Required",
      REVIEW_REQUIRED: "Review Required",
      MONITOR_ONLY: "Monitor",
      NO_ACTION_EXPECTED_POSITIONING: "No Action Indicated",
      INSUFFICIENT_EVIDENCE: "Review Required",
    };
    return map[key] || "";
  }

  /**
   * Executive tiles — category label + executive finding + evidence.
   */
  function renderExecutiveInsightTiles(insightPayload, opts) {
    opts = opts || {};
    var row = $(opts.rowId || "aivExecInsights");
    var section = $(opts.sectionId || "aivExecInsightSection");
    if (!row || !section) return 0;
    var boxes = sortExecutiveInsightTiles(
      insightPayload && Array.isArray(insightPayload.boxes)
        ? insightPayload.boxes
        : []
    ).slice(0, 5);
    if (!boxes.length) {
      section.hidden = true;
      row.innerHTML = "";
      row.removeAttribute("data-count");
      return 0;
    }
    section.hidden = false;
    row.setAttribute("data-count", String(boxes.length));
    row.innerHTML = boxes
      .map(function (box) {
        var kind = box.type || box.title || "";
        var title = box.title || kind || "Insight";
        if (
          kind === "PROVIDER_DISAGREEMENT" &&
          title === "Provider Disagreement"
        ) {
          title = "Provider Comparison";
        }
        title = String(title || "")
          .replace(/\s*[—-]\s*Review\s*$/i, "")
          .trim();
        var body =
          box.executiveFindingText || box.finding || box.takeaway || "";
        var isDetailRow = opts.rowId === "aivDetailInsights";
        var evidence = box.evidence || "";
        return (
          '<article class="aiv-insight-tile' +
          (isDetailRow ? " aiv-insight-tile--detail" : " aiv-insight-tile--exec") +
          '" data-insight-kind="' +
          AiVisibilityUi.escapeHtml(kind) +
          '" tabindex="0" role="button">' +
          "<h3>" +
          AiVisibilityUi.escapeHtml(title) +
          "</h3>" +
          (body
            ? '<p class="aiv-insight-body">' +
              AiVisibilityUi.escapeHtml(body) +
              "</p>"
            : "") +
          (evidence
            ? '<p class="aiv-insight-evidence"><span class="aiv-insight-evidence-label">Evidence:</span> ' +
              AiVisibilityUi.escapeHtml(evidence) +
              "</p>"
            : "") +
          "</article>"
        );
      })
      .join("");
    row.querySelectorAll(".aiv-insight-tile[data-insight-kind]").forEach(
      function (tile) {
        function go() {
          var nav = detailTargetForPriorityKind(
            tile.getAttribute("data-insight-kind")
          );
          if (!nav || !nav.targetId) return;
          gotoDetailSection({
            brandId: state.brandId,
            targetId: nav.targetId,
            watchlistMode: nav.watchlistMode || null,
          });
        }
        tile.addEventListener("click", go);
        tile.addEventListener("keydown", function (e) {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            go();
          }
        });
      }
    );
    return boxes.length;
  }

  function renderInsightBoxes(targetId, sectionId, insightPayload) {
    // Compact tiles on both Executive Summary and Detailed View insight rows.
    if (
      targetId === "aivExecInsights" ||
      sectionId === "aivExecInsightSection" ||
      targetId === "aivDetailInsights" ||
      sectionId === "aivDetailInsightSection"
    ) {
      renderExecutiveInsightTiles(insightPayload, {
        rowId: targetId,
        sectionId: sectionId,
      });
      return;
    }
    var row = $(targetId);
    var section = $(sectionId);
    if (!row || !section) return;
    var boxes =
      insightPayload && Array.isArray(insightPayload.boxes)
        ? insightPayload.boxes
        : [];
    if (!boxes.length) {
      section.hidden = true;
      row.innerHTML = "";
      return;
    }
    section.hidden = false;
    row.innerHTML = boxes
      .map(function (box) {
        var deepLinks = Array.isArray(box.evidenceDeepLinks)
          ? box.evidenceDeepLinks
          : [];
        var deepHtml = "";
        if (deepLinks.length) {
          deepHtml =
            '<p class="aiv-insight-meta aiv-insight-deep">' +
            deepLinks
              .slice(0, 3)
              .map(function (link) {
                var eid = link.evidenceId || "";
                var label =
                  link.label ||
                  [link.provider, link.promptId].filter(Boolean).join(" · ") ||
                  "View Evidence";
                if (!eid) {
                  return (
                    "<span>" + AiVisibilityUi.escapeHtml(label) + "</span>"
                  );
                }
                return (
                  '<button type="button" class="aiv-btn-text aiv-link" data-evidence="' +
                  AiVisibilityUi.escapeHtml(eid) +
                  '"' +
                  (link.responseId
                    ? ' data-response-id="' +
                      AiVisibilityUi.escapeHtml(link.responseId) +
                      '"'
                    : "") +
                  ">" +
                  AiVisibilityUi.escapeHtml(label) +
                  "</button>"
                );
              })
              .join(" · ") +
            "</p>";
        }
        var watch = buildInsightWatchLine(box);
        return (
          '<article class="aiv-insight-box">' +
          "<h3>" +
          AiVisibilityUi.escapeHtml(box.title || box.type || "Insight") +
          "</h3>" +
          '<p class="aiv-insight-finding">' +
          AiVisibilityUi.escapeHtml(box.finding || "") +
          "</p>" +
          (box.evidence
            ? '<p class="aiv-insight-meta">' +
              AiVisibilityUi.escapeHtml(box.evidence) +
              "</p>"
            : "") +
          deepHtml +
          (watch
            ? '<p class="aiv-insight-meta">' +
              AiVisibilityUi.escapeHtml(watch) +
              "</p>"
            : "") +
          "</article>"
        );
      })
      .join("");
    row.querySelectorAll("[data-evidence]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var brandForEvidence =
          btn.getAttribute("data-brand") || state.brandId;
        if (brandForEvidence) state.brandId = brandForEvidence;
        openEvidence(btn.getAttribute("data-evidence"));
      });
    });
  }

  /**
   * Populate Language select from API languageFilterContract.options.
   * Never adds an "All Languages" option.
   */
  function fillLanguageSelect(contract) {
    var sel = $("aivLanguage");
    if (!sel) return;
    var options =
      contract && Array.isArray(contract.options) ? contract.options : [];
    sel.innerHTML = options
      .map(function (opt) {
        var value = opt.value || opt;
        var label = opt.label || value;
        return (
          '<option value="' +
          AiVisibilityUi.escapeHtml(value) +
          '">' +
          AiVisibilityUi.escapeHtml(label) +
          "</option>"
        );
      })
      .join("");
  }

  /**
   * Show Language filter only when >1 completed language exists for the geography.
   * Sole language: hide control; keep state.language set for API qs.
   * No "All Languages" option ever.
   */
  function applyLanguageFilterContract(contract) {
    var group = $("aivLanguageFilterGroup");
    var sel = $("aivLanguage");
    var c = contract || {};
    var available = Array.isArray(c.availableLanguages)
      ? c.availableLanguages.slice()
      : [];
    if (
      !available.length &&
      Array.isArray(c.options) &&
      c.options.length
    ) {
      available = c.options.map(function (o) {
        return o.value || o;
      });
    }
    var visible =
      c.visible === true ||
      (available.length > 1);

    if (available.length === 1) {
      state.language = available[0];
      if (group) group.hidden = true;
      if (sel) {
        fillLanguageSelect({
          options: [
            {
              value: available[0],
              label:
                (c.options && c.options[0] && c.options[0].label) ||
                available[0],
            },
          ],
        });
        sel.value = available[0];
        sel.disabled = true;
      }
      return;
    }

    if (!visible || available.length < 2) {
      if (group) group.hidden = true;
      if (sel) sel.disabled = true;
      return;
    }

    fillLanguageSelect(c);
    if (
      !state.language ||
      available.indexOf(state.language) < 0
    ) {
      state.language =
        c.defaultSelection && available.indexOf(c.defaultSelection) >= 0
          ? c.defaultSelection
          : available.indexOf("en") >= 0
            ? "en"
            : available[0];
    }
    if (group) group.hidden = false;
    if (sel) {
      sel.disabled = false;
      sel.value = state.language;
    }
  }

  function emptyBlock(msg) {
    return '<div class="aiv-empty">' + AiVisibilityUi.escapeHtml(msg) + "</div>";
  }

  function listItems(items, mapFn) {
    if (!items || !items.length) return emptyBlock("None for this geography yet.");
    return (
      '<ul class="aiv-exec-list">' +
      items
        .map(function (item) {
          return "<li>" + mapFn(item) + "</li>";
        })
        .join("") +
      "</ul>"
    );
  }

  /** Explorer-style label | value rows (OE Best-Fit profile table grammar). */
  function kvRows(items, opts) {
    opts = opts || {};
    if (!items || !items.length) {
      return emptyBlock(opts.emptyMessage || "None for this geography yet.");
    }
    return (
      '<div class="aiv-kv-rows" role="table">' +
      items
        .map(function (item, idx) {
          var label = item.label || item.type || opts.fallbackLabel || "Item";
          var value = item.text || item.value || "";
          var last = idx === items.length - 1 ? " aiv-kv-row--last" : "";
          return (
            '<div class="aiv-kv-row' +
            last +
            '" role="row">' +
            '<div class="aiv-kv-label aiv-review-kind" role="rowheader">' +
            AiVisibilityUi.escapeHtml(label) +
            "</div>" +
            '<div class="aiv-kv-value" role="cell">' +
            AiVisibilityUi.escapeHtml(value) +
            "</div>" +
            "</div>"
          );
        })
        .join("") +
      "</div>"
    );
  }


  function territoryDisplay(t) {
    if (!t) return "—";
    if (typeof t === "string") return t;
    return t.intentTerritory || t.display || t.label || "—";
  }

  function pctLabel(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return "—";
    var pct = Math.round(v * 1000) / 10;
    return pct.toFixed(1) + "%";
  }

  function formatProviderDisplayName(idOrLabel) {
    if (idOrLabel == null || idOrLabel === "") return "—";
    var raw = String(idOrLabel).trim();
    if (!raw) return "—";
    var key = raw.toLowerCase();
    var labels = {
      openai: "OpenAI",
      gemini: "Gemini",
      perplexity: "Perplexity",
      claude: "Claude",
    };
    if (labels[key]) return labels[key];
    if (/^[A-Z]/.test(raw) && raw.indexOf(" ") === -1 && key !== raw) return raw;
    return raw.charAt(0).toUpperCase() + raw.slice(1);
  }

  function providerPresenceRate(row) {
    if (!row) return null;
    if (typeof row.presenceRate === "number") return row.presenceRate;
    if (typeof row.rate === "number") return row.rate;
    return null;
  }

  function renderCompactDiscoverability(el, payload) {
    if (!el) return;
    var d = payload || {};
    if (
      d.DISCOVERABILITY === "SOURCE_NOT_CONFIGURED" ||
      d.status === "CONNECTION_REQUIRED"
    ) {
      el.innerHTML =
        '<div class="aiv-empty">' +
        AiVisibilityUi.escapeHtml(
          d.message || "No official brand website has been configured."
        ) +
        "</div>";
      return;
    }
    if (d.DISCOVERABILITY === "CHECK_NOT_RUN" && !d.LIVE_BASELINE) {
      el.innerHTML =
        '<div class="aiv-empty">' +
        AiVisibilityUi.escapeHtml(
          d.message || "Discoverability baseline has not yet been measured."
        ) +
        "</div>";
      return;
    }
    function statusBadge(v, yesLabel, noLabel) {
      if (v === true) {
        return (
          '<span class="aiv-intel-status aiv-intel-status--yes">' +
          AiVisibilityUi.escapeHtml(yesLabel || "Yes") +
          "</span>"
        );
      }
      if (v === false) {
        return (
          '<span class="aiv-intel-status aiv-intel-status--no">' +
          AiVisibilityUi.escapeHtml(noLabel || "No") +
          "</span>"
        );
      }
      return '<span class="aiv-intel-status">—</span>';
    }
    var gaps = d.OWNER_INTENT_CONTENT_GAPS || [];
    var ownedCited =
      d.OWNED_SOURCES_CITED_IN_AI_RESPONSES == null
        ? '<span class="aiv-intel-status">See Citation Summary</span>'
        : typeof d.OWNED_SOURCES_CITED_IN_AI_RESPONSES === "number"
          ? '<span class="aiv-intel-status">' +
            AiVisibilityUi.escapeHtml(pctLabel(d.OWNED_SOURCES_CITED_IN_AI_RESPONSES)) +
            " · See Citation Summary</span>"
          : statusBadge(
              d.OWNED_SOURCES_CITED_IN_AI_RESPONSES,
              "Cited",
              "0% · See Citation Summary"
            );
    var rows = [
      ["Official Sources", statusBadge(d.OFFICIAL_SOURCES_CONFIGURED, "Configured ✓", "Not configured")],
      ["Public Access", statusBadge(d.PUBLIC_SOURCES_ACCESSIBLE, "Accessible ✓", "Not accessible")],
      [
        "Owner / Development Content",
        statusBadge(d.OWNER_DEVELOPMENT_CONTENT_FOUND, "Found ✓", "Not found"),
      ],
      [
        "Owner-Intent Content Gaps",
        '<span class="aiv-intel-status">' +
          AiVisibilityUi.escapeHtml(gaps.length ? String(gaps.length) : "None observed") +
          "</span>",
      ],
      ["Owned Sources Cited", ownedCited],
      [
        "Last checked",
        '<span class="aiv-intel-status">' +
          AiVisibilityUi.escapeHtml(
            d.LAST_CHECKED_AT ? String(d.LAST_CHECKED_AT).slice(0, 19) : "—"
          ) +
          "</span>",
      ],
    ];
    el.innerHTML =
      '<div class="aiv-intel-status-list" role="table">' +
      rows
        .map(function (row) {
          var meta =
            row[0] === "Last checked" ? " aiv-intel-status-row--meta" : "";
          return (
            '<div class="aiv-intel-status-row' +
            meta +
            '" role="row"><div class="aiv-intel-status-label" role="rowheader">' +
            AiVisibilityUi.escapeHtml(row[0]) +
            '</div><div class="aiv-intel-status-value" role="cell">' +
            row[1] +
            "</div></div>"
          );
        })
        .join("") +
      "</div>";
  }

  function renderPublicDiscoverability(el, payload) {
    if (!el) return;
    var d = payload || {};
    if (
      d.DISCOVERABILITY === "SOURCE_NOT_CONFIGURED" ||
      d.status === "CONNECTION_REQUIRED"
    ) {
      el.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">' +
        AiVisibilityUi.escapeHtml(
          d.message || "No official brand website has been configured."
        ) +
        "</div>";
      return;
    }
    if (d.DISCOVERABILITY === "CHECK_NOT_RUN" && !d.LIVE_BASELINE) {
      el.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">' +
        AiVisibilityUi.escapeHtml(
          d.message || "Discoverability baseline has not yet been measured."
        ) +
        "</div>";
      return;
    }
    function yn(v) {
      if (v === true) return "Yes";
      if (v === false) return "No";
      return "—";
    }
    function chip(v, yesLabel, noLabel) {
      if (v === true) {
        return (
          '<span class="aiv-intel-status aiv-intel-status--yes">' +
          AiVisibilityUi.escapeHtml(yesLabel || "Yes") +
          "</span>"
        );
      }
      if (v === false) {
        return (
          '<span class="aiv-intel-status aiv-intel-status--no">' +
          AiVisibilityUi.escapeHtml(noLabel || "No") +
          "</span>"
        );
      }
      return '<span class="aiv-intel-status">—</span>';
    }
    var baseline = d.baseline || {};
    var gaps = d.OWNER_INTENT_CONTENT_GAPS || [];
    var governedUrl =
      d.governedUrl ||
      (d.inventory && d.inventory.configured && d.inventory.configured[0] && d.inventory.configured[0].url) ||
      (d.brandRow && (d.brandRow.brandWebsite || d.brandRow.officialWebsite)) ||
      null;
    var finalUrl = d.finalUrl || d.FINAL_URL || baseline.FINAL_URL || governedUrl;
    var httpStatus =
      d.HTTP_STATUS != null
        ? d.HTTP_STATUS
        : d.httpStatus != null
          ? d.httpStatus
          : baseline.HTTP_STATUS != null
            ? baseline.HTTP_STATUS
            : null;
    var rows = [
      ["Governed URL", governedUrl || "—"],
      ["Final URL", finalUrl || "—"],
      ["HTTP status", httpStatus != null ? String(httpStatus) : "—"],
      ["Accessible", yn(d.PUBLIC_SOURCES_ACCESSIBLE)],
      [
        "Content retrievable",
        yn(
          baseline.CONTENT_RETRIEVABLE != null
            ? baseline.CONTENT_RETRIEVABLE
            : d.CONTENT_RETRIEVABLE
        ),
      ],
      [
        "Canonical present",
        yn(
          baseline.CANONICAL_PRESENT != null
            ? baseline.CANONICAL_PRESENT
            : d.CANONICAL_PRESENT
        ),
      ],
      [
        "Structured data present",
        yn(
          baseline.STRUCTURED_DATA_PRESENT != null
            ? baseline.STRUCTURED_DATA_PRESENT
            : d.STRUCTURED_DATA_PRESENT
        ),
      ],
      [
        "Indexability",
        baseline.INDEXABLE === true
          ? "Technically indexable"
          : baseline.INDEXABLE === false
            ? "Not technically indexable"
            : "—",
      ],
      ["Owner/development content found", yn(d.OWNER_DEVELOPMENT_CONTENT_FOUND)],
      [
        "Owner-intent content gaps",
        gaps.length ? String(gaps.length) + " observed" : "None observed",
      ],
      [
        "Owned sources cited in AI responses",
        d.OWNED_SOURCES_CITED_IN_AI_RESPONSES == null
          ? "See Citation & Source Intelligence"
          : yn(d.OWNED_SOURCES_CITED_IN_AI_RESPONSES),
      ],
    ];
    // Robots-allowed is not always exposed on the brand read payload — show only when present.
    var robots =
      d.ROBOTS_ALLOWED != null
        ? d.ROBOTS_ALLOWED
        : baseline.ROBOTS_ALLOWED != null
          ? baseline.ROBOTS_ALLOWED
          : null;
    if (robots != null) {
      rows.splice(7, 0, ["Robots allowed", yn(robots)]);
    }
    el.innerHTML =
      '<div class="aiv-disc-chips" aria-hidden="true">' +
      chip(d.PUBLIC_SOURCES_ACCESSIBLE, "Accessible", "Not accessible") +
      chip(d.OWNER_DEVELOPMENT_CONTENT_FOUND, "Dev content", "No dev content") +
      chip(
        baseline.CANONICAL_PRESENT != null
          ? baseline.CANONICAL_PRESENT
          : d.CANONICAL_PRESENT,
        "Canonical",
        "No canonical"
      ) +
      "</div>" +
      '<div class="aiv-kv" role="table">' +
      rows
        .map(function (row, idx, arr) {
          var last = idx === arr.length - 1 ? " aiv-kv-row--last" : "";
          return (
            '<div class="aiv-kv-row' +
            last +
            '" role="row"><div class="aiv-kv-label" role="rowheader">' +
            AiVisibilityUi.escapeHtml(row[0]) +
            '</div><div class="aiv-kv-value" role="cell">' +
            AiVisibilityUi.escapeHtml(row[1]) +
            "</div></div>"
          );
        })
        .join("") +
      "</div>" +
      (d.LAST_CHECKED_AT
        ? '<p class="help-text">Last checked ' +
          AiVisibilityUi.escapeHtml(String(d.LAST_CHECKED_AT).slice(0, 19)) +
          "</p>"
        : "") +
      '<p class="help-text">Public Discoverability is factual baseline only — not a composite score, and separate from AI Presence.</p>';
  }

  function renderProviderPresencePanel(panel) {
    var body = $("aivProviderPresenceBody");
    var note = $("aivProviderPresenceNote");
    if (!body) return;
    var rows = (panel && panel.rows) || [];
    if (!rows.length) {
      body.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">No provider monitoring rows for this brand yet.</div>';
      if (note) note.textContent = "";
      return;
    }
    var best = pickBestProviderRow(rows);
    body.innerHTML =
      '<div class="deals-table-container aiv-portfolio-table-wrap aiv-coverage-table-wrap">' +
      '<table class="deals-table aiv-portfolio-table aiv-coverage-table aiv-provider-presence-table">' +
      "<thead><tr>" +
      '<th class="no-sort">Provider</th>' +
      '<th class="no-sort">Status</th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">AI<br>Presence</span></span></th>' +
      '<th class="no-sort">Monitored</th>' +
      '<th class="no-sort">Missing</th>' +
      '<th class="no-sort">Citation</th>' +
      '<th class="no-sort">Owned</th>' +
      "</tr></thead><tbody>" +
      rows
        .map(function (r) {
          var ratePct =
            typeof r.PRESENCE_RATE === "number"
              ? Math.max(0, Math.min(100, r.PRESENCE_RATE * 100))
              : null;
          var status = r.MONITORING_STATUS_DISPLAY || r.MONITORING_STATUS || "—";
          var presentMon =
            r.PRESENT_N != null && r.MONITORED_N != null
              ? r.PRESENT_N + " / " + r.MONITORED_N
              : "—";
          var isBest =
            best &&
            r.PROVIDER === best.PROVIDER &&
            typeof r.PRESENCE_RATE === "number";
          var deltaRaw = r.DELTA_DISPLAY || "";
          var deltaNumeric =
            typeof r.DELTA_PP === "number" && Number.isFinite(r.DELTA_PP)
              ? r.DELTA_PP
              : null;
          var showDeltaBeside =
            deltaNumeric != null ||
            (/^[+\-−]?\d/.test(String(deltaRaw)) && /\bpp\b/i.test(String(deltaRaw)));
          var deltaDirClass =
            deltaNumeric == null
              ? "aiv-delta-flat"
              : deltaNumeric > 0
                ? "aiv-delta-up"
                : deltaNumeric < 0
                  ? "aiv-delta-down"
                  : "aiv-delta-flat";
          var presenceValue =
            ratePct == null
              ? AiVisibilityUi.escapeHtml(r.PRESENCE_RATE_DISPLAY || "—")
              : AiVisibilityUi.escapeHtml(r.PRESENCE_RATE_DISPLAY || "—");
          var presenceCell =
            ratePct == null
              ? presenceValue
              : '<span class="aiv-presence-cell">' +
                '<span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' +
                ratePct +
                '%"></span></span>' +
                '<span class="aiv-presence-cell__value">' +
                presenceValue +
                "</span>" +
                (showDeltaBeside
                  ? '<span class="aiv-presence-cell__delta ' +
                    deltaDirClass +
                    '" title="Change vs prior comparable run">' +
                    AiVisibilityUi.escapeHtml(deltaRaw) +
                    "</span>"
                  : "") +
                "</span>";
          return (
            '<tr class="aiv-provider-presence-row" data-provider="' +
            AiVisibilityUi.escapeHtml(r.PROVIDER || "") +
            '" tabindex="0" role="button">' +
            "<td><span class=\"project-name-text\">" +
            AiVisibilityUi.escapeHtml(r.PROVIDER_LABEL || r.PROVIDER || "—") +
            "</span>" +
            (isBest ? ' <span class="aiv-you-tag">Best</span>' : "") +
            "</td>" +
            "<td>" +
            AiVisibilityUi.escapeHtml(status) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            presenceCell +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(presentMon) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(
              r.QUESTIONS_MISSING_N != null ? String(r.QUESTIONS_MISSING_N) : "—"
            ) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(r.CITATION_RATE_DISPLAY || "—") +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(r.OWNED_SOURCE_CITATION_RATE_DISPLAY || "—") +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table></div>";
    if (note) {
      note.textContent =
        (panel && panel.CLIENT_COPY) ||
        "Select a provider row to reload Detailed View for that monitoring source.";
    }
    body.querySelectorAll(".aiv-provider-presence-row").forEach(function (row) {
      function activate() {
        var pid = row.getAttribute("data-provider");
        if (!pid) return;
        state.provider = pid;
        var sel = $("aivProvider");
        if (sel) sel.value = pid;
        persistFilterPrefs();
        loadDetail();
      }
      row.addEventListener("click", activate);
      row.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          activate();
        }
      });
    });
  }

  function pickBestProviderRow(rows) {
    var best = null;
    (rows || []).forEach(function (r) {
      if (typeof r.PRESENCE_RATE !== "number" || !Number.isFinite(r.PRESENCE_RATE)) {
        return;
      }
      if (!best || r.PRESENCE_RATE > best.PRESENCE_RATE) best = r;
    });
    return best;
  }

  function renderCoverageDiagnosticsSummary(panel, intentBlock, topTerritory) {
    var el = $("aivCoverageSummary");
    if (!el) return;
    var cards = [];
    var bestProv = pickBestProviderRow((panel && panel.rows) || []);
    if (bestProv) {
      cards.push({
        label: "Best Provider",
        value:
          (bestProv.PROVIDER_LABEL || bestProv.PROVIDER || "—") +
          (bestProv.PRESENCE_RATE_DISPLAY
            ? " · " + bestProv.PRESENCE_RATE_DISPLAY
            : ""),
      });
    }
    var rows = (intentBlock && intentBlock.rows) || [];
    var futureOrEmpty =
      !intentBlock ||
      intentBlock.availability === "future_ready" ||
      intentBlock.availability === "not_monitored" ||
      !rows.length;
    if (!futureOrEmpty) {
      if (topTerritory && (topTerritory.intentLabel || topTerritory.intentTerritory)) {
        cards.push({
          label: "Strongest Owner Intent",
          value:
            (topTerritory.intentLabel || topTerritory.intentTerritory) +
            (topTerritory.display ? " · " + topTerritory.display : ""),
        });
      }
      var weakest = pickWeakestIntentTerritory(rows);
      if (weakest && (weakest.intentLabel || weakest.intentTerritory)) {
        cards.push({
          label: "Weakest Owner Intent",
          value:
            (weakest.intentLabel || weakest.intentTerritory) +
            (weakest.display ? " · " + weakest.display : ""),
        });
      }
      var breadth = summarizeIntentCoverageBreadth(rows);
      if (breadth) {
        cards.push({
          label: "Coverage Breadth",
          value: breadth.display + " owner intents",
        });
      }
    }
    if (!cards.length) {
      el.innerHTML = "";
      el.hidden = true;
      return;
    }
    el.hidden = false;
    el.className = "aiv-kpi-row aiv-theme-kpis aiv-coverage-summary";
    el.innerHTML = cards
      .map(function (c) {
        return (
          '<article class="aiv-kpi"><h3>' +
          AiVisibilityUi.escapeHtml(c.label) +
          '</h3><div class="aiv-value">' +
          AiVisibilityUi.escapeHtml(c.value) +
          "</div></article>"
        );
      })
      .join("");
  }

  function renderLanguageComparison(lc, targetId) {
    var el = $(targetId || "aivExecLanguage");
    if (!el) return;
    if (!lc || !hasDualLanguageComparison(lc)) {
      el.innerHTML = "";
      return;
    }
    var enPresence = pctLabel(
      lc.EN_AI_PRESENCE != null ? lc.EN_AI_PRESENCE : lc.EN_PRESENCE
    );
    var esPresence = pctLabel(
      lc.ES_AI_PRESENCE != null ? lc.ES_AI_PRESENCE : lc.ES_PRESENCE
    );
    var enMissing =
      lc.EN_QUESTIONS_MISSING != null ? String(lc.EN_QUESTIONS_MISSING) : "—";
    var esMissing =
      lc.ES_QUESTIONS_MISSING != null ? String(lc.ES_QUESTIONS_MISSING) : "—";
    var showCitation =
      typeof lc.EN_CITATION_RATE === "number" ||
      typeof lc.ES_CITATION_RATE === "number";
    var showOwned =
      typeof lc.EN_OWNED_SOURCE_CITATION_RATE === "number" ||
      typeof lc.ES_OWNED_SOURCE_CITATION_RATE === "number";
    var rows =
      '<div class="aiv-lang-matrix" role="table">' +
      '<div class="aiv-lang-matrix__head" role="row">' +
      '<div class="aiv-lang-matrix__metric" role="columnheader"></div>' +
      '<div class="aiv-lang-matrix__col" role="columnheader">EN</div>' +
      '<div class="aiv-lang-matrix__col" role="columnheader">ES</div>' +
      "</div>" +
      '<div class="aiv-lang-matrix__row" role="row">' +
      '<div class="aiv-lang-matrix__metric" role="rowheader">AI Presence</div>' +
      '<div class="aiv-lang-matrix__col" role="cell">' +
      AiVisibilityUi.escapeHtml(enPresence) +
      '</div><div class="aiv-lang-matrix__col" role="cell">' +
      AiVisibilityUi.escapeHtml(esPresence) +
      "</div></div>" +
      '<div class="aiv-lang-matrix__row" role="row">' +
      '<div class="aiv-lang-matrix__metric" role="rowheader">Questions Missing</div>' +
      '<div class="aiv-lang-matrix__col" role="cell">' +
      AiVisibilityUi.escapeHtml(enMissing) +
      '</div><div class="aiv-lang-matrix__col" role="cell">' +
      AiVisibilityUi.escapeHtml(esMissing) +
      "</div></div>";
    if (showCitation) {
      rows +=
        '<div class="aiv-lang-matrix__row" role="row">' +
        '<div class="aiv-lang-matrix__metric" role="rowheader">Citation Coverage</div>' +
        '<div class="aiv-lang-matrix__col" role="cell">' +
        AiVisibilityUi.escapeHtml(pctLabel(lc.EN_CITATION_RATE)) +
        '</div><div class="aiv-lang-matrix__col" role="cell">' +
        AiVisibilityUi.escapeHtml(pctLabel(lc.ES_CITATION_RATE)) +
        "</div></div>";
    }
    if (showOwned) {
      rows +=
        '<div class="aiv-lang-matrix__row" role="row">' +
        '<div class="aiv-lang-matrix__metric" role="rowheader">Owned Source Coverage</div>' +
        '<div class="aiv-lang-matrix__col" role="cell">' +
        AiVisibilityUi.escapeHtml(pctLabel(lc.EN_OWNED_SOURCE_CITATION_RATE)) +
        '</div><div class="aiv-lang-matrix__col" role="cell">' +
        AiVisibilityUi.escapeHtml(pctLabel(lc.ES_OWNED_SOURCE_CITATION_RATE)) +
        "</div></div>";
    }
    if (lc.presenceNote || lc.ownedCitationNote) {
      rows +=
        '<div class="aiv-lang-matrix__note">' +
        AiVisibilityUi.escapeHtml(lc.presenceNote || lc.ownedCitationNote) +
        "</div>";
    }
    rows += "</div>";
    el.innerHTML = rows;
  }

  var PROMPT_DISCLOSURE_INFO = {
    OWNER_INTENT: {
      title: "Owner Intent",
      body:
        "Owner Intent represents the hotel owner or developer decision Dealality is testing, such as brand affiliation, conversion, flexibility or market entry. Dealality may use multiple governed question formulations to measure the same decision.",
    },
    DECISION_CONTEXT: {
      title: "Decision Context",
      body:
        "Decision Context describes the business situation behind the measurement. It helps explain what the AI was being asked to evaluate without exposing Dealality's exact production prompt.",
    },
    HOW_DEALALITY_MEASURES_AI: {
      title: "How Dealality Measures AI",
      body:
        "Dealality tests representative hotel owner and developer decision scenarios across monitored AI providers, markets and languages. Results are measured using governed scenarios and repeat observations. Exact production prompts and testing sequences are proprietary.",
    },
    QUESTIONS_MISSING: {
      title: "Questions Missing",
      body:
        "A question is considered missing when your brand is absent across every comparable monitored provider for that owner-decision observation. Missing does not mean zero demand or that another brand 'won.'",
    },
    BENCHMARK_STILL_DEVELOPING: {
      title: "Benchmark still developing",
      body:
        "Dealality shows a numeric benchmark as soon as this brand, Owner Intent and selected provider scope pass the required measurement-quality checks. Until then, the underlying Presence observations may still be shown without an uncertified score.",
    },
    YOUR_PRESENCE: {
      title: "Your Presence",
      body:
        "Your Presence shows how often your brand appeared across comparable monitored AI observations for this Owner Intent in the selected provider and geography view.",
    },
    MONITORED: {
      title: "Monitored",
      body:
        "Comparable observations measured for this Owner Intent in the selected provider and geography view. Shown as observations with presence / total comparable observations.",
    },
    MISSING: {
      title: "Missing",
      body:
        "Counts comparable monitored observations where your brand did not appear for this Owner Intent. Missing does not mean zero demand or that another brand won.",
    },
    PEER_PRESENT_GAPS: {
      title: "Peer-Present Gaps",
      body:
        "Counts monitored owner-decision observations where your brand was absent while one or more relevant peers were present. This highlights competitive visibility gaps; it does not mean another brand \"won\" the decision.",
    },
    AI_PRESENCE_INDEX: {
      title: "AI Presence Index",
      body:
        "Measures how often your brand appears in this owner-decision context relative to directly comparable brands in the selected provider scope. 100 represents competitive parity.",
    },
    CORE_PEERS: {
      title: "Core Peers",
      body:
        "Brands considered direct commercial alternatives for this specific owner decision. Dealality uses governed commercial characteristics to determine the relevant comparison group.",
    },
    OBSERVED_COMPETITORS: {
      title: "Observed Competitors",
      body:
        "Brands that actually appear as alternatives or peers across relevant Dealality AI observations. They may differ from a traditional competitive set.",
    },
    CHG_VS_PRIOR_RUN: {
      title: "Δ vs prior run",
      body:
        "Shows the change in this Owner Intent's certified AI Presence Index versus the most recent comparable prior measurement run. Change is shown in index points. If no comparable prior run exists, you will see Insufficient History. It is not a long-term trend.",
    },
  };

  function disclosureInfoIconHtml(key, labelOverride) {
    var info = PROMPT_DISCLOSURE_INFO[key];
    if (!info) return "";
    var label = labelOverride || info.title;
    return (
      '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About ' +
      AiVisibilityUi.escapeHtml(label) +
      '"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span>' +
      '<div class="tooltip-content" hidden><strong>' +
      AiVisibilityUi.escapeHtml(info.title) +
      "</strong><br>" +
      AiVisibilityUi.escapeHtml(info.body) +
      "</div></span>"
    );
  }

  function watchlistOwnerIntent(r) {
    return r.ownerIntent || r.intentLabel || r.PROMPT_FAMILY || "—";
  }

  function watchlistMissingLabel(r) {
    var missing = r.missingProviderCount;
    var comparable = r.comparableProviderCount;
    if (missing == null && Array.isArray(r.PROVIDERS_MISSING)) {
      missing = r.PROVIDERS_MISSING.length;
    }
    if (comparable == null && Array.isArray(r.PROVIDERS_MONITORED)) {
      comparable = r.PROVIDERS_MONITORED.length;
    }
    if (missing != null && comparable != null && comparable > 0) {
      return missing + " of " + comparable + " providers";
    }
    if (missing != null) return String(missing) + " providers";
    return r.SUBJECT_PRESENCE || "Missing";
  }

  var watchlistState = {
    mode: "missing",
    rows: [],
    raw: null,
    page: 0,
    pageSize: 25,
  };

  function fillWatchlistFilterOptions(rows) {
    function fill(selId, key) {
      var sel = $(selId);
      if (!sel) return;
      var cur = sel.value;
      var vals = {};
      (rows || []).forEach(function (r) {
        var v = r[key];
        if (v) vals[String(v)] = true;
      });
      sel.innerHTML =
        '<option value="">All</option>' +
        Object.keys(vals)
          .sort()
          .map(function (v) {
            return (
              '<option value="' +
              AiVisibilityUi.escapeHtml(v) +
              '">' +
              AiVisibilityUi.escapeHtml(v) +
              "</option>"
            );
          })
          .join("");
      if (cur) sel.value = cur;
    }
    // Provider / Region / Language follow page filters — only Owner Intent stays local.
    (function fillOwnerIntent(selId) {
      var sel = $(selId);
      if (!sel) return;
      var cur = sel.value;
      var vals = {};
      (rows || []).forEach(function (r) {
        var v = watchlistOwnerIntent(r);
        if (v && v !== "—") vals[String(v)] = true;
      });
      sel.innerHTML =
        '<option value="">All</option>' +
        Object.keys(vals)
          .sort()
          .map(function (v) {
            return (
              '<option value="' +
              AiVisibilityUi.escapeHtml(v) +
              '">' +
              AiVisibilityUi.escapeHtml(v) +
              "</option>"
            );
          })
          .join("");
      if (cur) sel.value = cur;
    })("aivWlFamily");
  }

  function renderWatchlistTable() {
    var body = $("aivWatchlistBody");
    var note = $("aivWatchlistNote");
    var paginationEl = $("aivWatchlistPagination");
    if (!body) return;
    var family = ($("aivWlFamily") && $("aivWlFamily").value) || "";
    // Provider / Geography / Language come from page filters via the questions API (qs).
    var filtered = (watchlistState.rows || []).filter(function (r) {
      if (family && String(watchlistOwnerIntent(r)) !== family) return false;
      return true;
    });
    var pageSize = watchlistState.pageSize || 25;
    var maxPage = Math.max(0, Math.ceil(filtered.length / pageSize) - 1);
    if (watchlistState.page > maxPage) watchlistState.page = maxPage;
    if (watchlistState.page < 0) watchlistState.page = 0;
    var start = watchlistState.page * pageSize;
    var rows = filtered.slice(start, start + pageSize);
    if (!filtered.length) {
      body.innerHTML =
        '<tr><td colspan="8"><div class="aiv-empty">No Questions Missing rows for the current filters.</div></td></tr>';
    } else {
      body.innerHTML = rows
        .map(function (r) {
          var peers = (r.PEERS_PRESENT || [])
            .map(function (p) {
              var name = p.entityName || p.name || "";
              // Never surface raw Airtable record IDs as "names".
              if (!name || /^rec[a-zA-Z0-9]{10,}$/.test(String(name))) {
                var id = p.entityId || "";
                if (id && state.brandNamesById && state.brandNamesById[id]) {
                  return state.brandNamesById[id];
                }
                return "";
              }
              return name;
            })
            .filter(Boolean)
            .join(", ");
          var corePeers = (r.corePeersPresent || []).join(" · ");
          var priority = r.priority || "";
          var ev = r.EVIDENCE || r.evidenceId || "";
          var ownerIntent = watchlistOwnerIntent(r);
          var decisionContext = r.decisionContext || "—";
          var geography = r.geography || r.geographyDisplay || r.REGION || "—";
          var language = r.languageDisplay || r.LANGUAGE || "—";
          return (
            "<tr>" +
            "<td><span class=\"aiv-wl-intent\">" +
            AiVisibilityUi.escapeHtml(ownerIntent) +
            "</span>" +
            promptOriginBadgeHtml(r) +
            "</td>" +
            "<td><span class=\"aiv-wl-decision-context\">" +
            AiVisibilityUi.escapeHtml(decisionContext) +
            "</span></td>" +
            "<td>" +
            AiVisibilityUi.escapeHtml(geography) +
            "</td>" +
            "<td>" +
            AiVisibilityUi.escapeHtml(language) +
            "</td>" +
            "<td>" +
            AiVisibilityUi.escapeHtml(watchlistMissingLabel(r)) +
            "</td>" +
            "<td>" +
            (corePeers
              ? '<span class="aiv-wl-core-peers">' + AiVisibilityUi.escapeHtml(corePeers) + "</span>"
              : AiVisibilityUi.escapeHtml(peers || "—")) +
            (r.observedCompetitor
              ? '<div class="aiv-wl-observed">Observed competitor: ' +
                AiVisibilityUi.escapeHtml(r.observedCompetitor) +
                "</div>"
              : "") +
            (r.competitiveContext
              ? '<div class="aiv-wl-context">' + AiVisibilityUi.escapeHtml(r.competitiveContext) + "</div>"
              : "") +
            "</td>" +
            "<td>" +
            (priority
              ? '<span class="aiv-priority-tag">' + AiVisibilityUi.escapeHtml(priority) + "</span>"
              : "—") +
            "</td>" +
            "<td>" +
            (ev
              ? '<button type="button" class="aiv-btn-text aiv-link" data-evidence="' +
                AiVisibilityUi.escapeHtml(ev) +
                '">Evidence</button>'
              : "—") +
            "</td></tr>"
          );
        })
        .join("");
      body.querySelectorAll("[data-evidence]").forEach(function (btn) {
        btn.addEventListener("click", function () {
          openEvidence(btn.getAttribute("data-evidence"));
        });
      });
    }
    if (paginationEl) {
      if (filtered.length > pageSize) {
        paginationEl.hidden = false;
        paginationEl.innerHTML =
          '<span class="aiv-pagination-meta">Showing ' +
          (start + 1) +
          "–" +
          Math.min(start + pageSize, filtered.length) +
          " of " +
          filtered.length +
          '</span> <button type="button" class="aiv-btn-text" data-wl-page="prev"' +
          (watchlistState.page <= 0 ? " disabled" : "") +
          ">Previous</button> <button type=\"button\" class=\"aiv-btn-text\" data-wl-page=\"next\"" +
          (watchlistState.page >= maxPage ? " disabled" : "") +
          ">Next</button>";
        paginationEl.querySelectorAll("[data-wl-page]").forEach(function (btn) {
          btn.addEventListener("click", function () {
            var dir = btn.getAttribute("data-wl-page");
            if (dir === "prev" && watchlistState.page > 0) watchlistState.page -= 1;
            if (dir === "next" && watchlistState.page < maxPage) {
              watchlistState.page += 1;
            }
            renderWatchlistTable();
          });
        });
      } else {
        paginationEl.hidden = true;
        paginationEl.innerHTML = "";
      }
    }
    if (note) {
      note.textContent =
        (watchlistState.raw &&
          cleanPriorityClientCopy(watchlistState.raw.CLIENT_COPY || "")) ||
        (watchlistState.mode === "peer_gaps"
          ? "Peers observed on these questions while this brand was not."
          : "This brand was not observed on these monitored questions.");
    }
  }

  function applyWatchlistPayload(payload) {
    var wl = (payload && payload.questionsMissingWatchlist) || {};
    watchlistState.raw = wl;
    watchlistState.rows = wl.rows || [];
    watchlistState.page = 0;
    (watchlistState.rows || []).forEach(function (r) {
      (r.PEERS_PRESENT || []).forEach(function (p) {
        if (p && p.entityId && p.entityName) {
          state.brandNamesById[p.entityId] = p.entityName;
        }
      });
    });
    var peerMissing = wl.peerPresentSubjectMissing || payload.peerPresentSubjectMissing;
    if (peerMissing && peerMissing.PEER_PRESENT_SUBJECT_MISSING_N != null) {
      state.peerGapCount = peerMissing.PEER_PRESENT_SUBJECT_MISSING_N;
    } else if (watchlistState.mode === "peer_gaps") {
      state.peerGapCount = watchlistState.rows.length;
    }
    fillWatchlistFilterOptions(watchlistState.rows);
    renderWatchlistTable();
    if (state.peerRows && state.peerRows.length && $("aivDetailPeerGapNote")) {
      renderCompetitors({ competitors: state.peerRows });
    }
  }

  async function loadWatchlist(mode, fetchOpts) {
    if (!state.brandId) return null;
    watchlistState.mode = mode || watchlistState.mode || "missing";
    var brandBase =
      "/api/ai-visibility/brand/" + encodeURIComponent(state.brandId);
    var modeParam =
      watchlistState.mode === "peer_gaps"
        ? "peers_present_subject_missing"
        : "missing";
    try {
      var data = await apiGet(
        brandBase +
          "/questions" +
          qs({
            filter: "missing",
            limit: "100",
            offset: "0",
            watchlistMode: modeParam,
          }),
        fetchOpts || {}
      );
      applyWatchlistPayload(data);
      return data;
    } catch (err) {
      var body = $("aivWatchlistBody");
      if (body) {
        body.innerHTML =
          '<tr><td colspan="8"><div class="aiv-empty">Unable to load Questions Missing watchlist.</div></td></tr>';
      }
      return null;
    }
  }

  function renderDiscoverabilityPlaceholder(el, payload, mode) {
    if (!el) return;
    if (
      payload &&
      (payload.OFFICIAL_SOURCES_CONFIGURED != null ||
        payload.DISCOVERABILITY ||
        payload.LIVE_BASELINE)
    ) {
      if (mode === "executive") {
        renderCompactDiscoverability(el, payload);
      } else {
        renderPublicDiscoverability(el, payload);
      }
      return;
    }
    var d = payload || {};
    var comingLater =
      d.comingLaterNote ||
      "Public crawl readiness can be measured without client connections. Referral and business impact require governed analytics or log connections.";

    function metricRows(metrics) {
      return (
        '<div class="aiv-kv" role="table">' +
        (metrics || [])
          .map(function (m, idx, arr) {
            var last = idx === arr.length - 1 ? " aiv-kv-row--last" : "";
            var value =
              m.display ||
              m.connectionCopy ||
              m.valueDisplay ||
              (m.value != null ? String(m.value) : "Data connection required");
            return (
              '<div class="aiv-kv-row' +
              last +
              '" role="row"><div class="aiv-kv-label" role="rowheader">' +
              AiVisibilityUi.escapeHtml(m.label || m.id || "") +
              '</div><div class="aiv-kv-value" role="cell">' +
              AiVisibilityUi.escapeHtml(value) +
              "</div></div>"
            );
          })
          .join("") +
        "</div>" +
        '<p class="aiv-discoverability-coming-later help-text">' +
        AiVisibilityUi.escapeHtml(comingLater) +
        "</p>"
      );
    }

    function metricsFromObject(section) {
      if (!section) return [];
      if (Array.isArray(section.metrics)) return section.metrics;
      var keys = [
        "priorityDevelopmentPages",
        "crawlerAccess",
        "developmentContentCrawlable",
        "aiReferralSessions",
        "qualifiedDevelopmentActions",
      ];
      var out = [];
      keys.forEach(function (k) {
        if (section[k] && typeof section[k] === "object") out.push(section[k]);
      });
      return out;
    }

    if (!payload) {
      el.innerHTML =
        '<div class="aiv-empty">' + AiVisibilityUi.escapeHtml(comingLater) + "</div>";
      return;
    }

    // Phase 3C.1 shape: discoverability / referral / businessImpact (+ optional modules)
    if (d.discoverability || d.referral || d.modules) {
      if (mode === "executive") {
        el.innerHTML =
          '<div class="aiv-signals-grid">' +
          '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
          AiVisibilityUi.escapeHtml((d.discoverability && d.discoverability.title) || "Discoverability") +
          "</h3>" +
          metricRows(metricsFromObject(d.discoverability)) +
          "</div>" +
          '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
          AiVisibilityUi.escapeHtml((d.referral && d.referral.title) || "Referral") +
          "</h3>" +
          metricRows(metricsFromObject(d.referral)) +
          "</div>" +
          '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
          AiVisibilityUi.escapeHtml((d.businessImpact && d.businessImpact.title) || "Business Impact") +
          "</h3>" +
          metricRows(metricsFromObject(d.businessImpact)) +
          "</div></div>";
        return;
      }
      var mods = d.modules || {};
      var crawl = mods.crawlReadiness || {};
      var referral = mods.referral || d.referral || {};
      var impact = mods.businessImpact || d.businessImpact || {};
      el.innerHTML =
        '<div class="aiv-signals-grid">' +
        '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
        AiVisibilityUi.escapeHtml(crawl.label || "Crawl Readiness") +
        "</h3>" +
        metricRows(crawl.metrics || metricsFromObject(d.discoverability)) +
        "</div>" +
        '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
        AiVisibilityUi.escapeHtml(referral.label || referral.title || "Referral") +
        "</h3>" +
        metricRows(referral.metrics || metricsFromObject(referral)) +
        "</div>" +
        '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
        AiVisibilityUi.escapeHtml(impact.label || impact.title || "Business Impact") +
        "</h3>" +
        metricRows(impact.metrics || metricsFromObject(impact)) +
        "</div></div>";
      return;
    }

    // Legacy OpenAI placeholder shape
    if (mode === "executive") {
      var tech = (d.technical && d.technical.metrics) || [];
      var biz = (d.businessImpact && d.businessImpact.metrics) || [];
      el.innerHTML =
        '<div class="aiv-signals-grid">' +
        '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
        AiVisibilityUi.escapeHtml((d.technical && d.technical.label) || "Technical Discoverability") +
        '</h3><p class="aiv-theme-block-help help-text">' +
        AiVisibilityUi.escapeHtml(d.helper || "") +
        "</p>" +
        metricRows(tech) +
        "</div>" +
        '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
        AiVisibilityUi.escapeHtml((d.businessImpact && d.businessImpact.label) || "Business Impact") +
        "</h3>" +
        metricRows(biz) +
        "</div></div>";
      return;
    }
    var crawlLegacy = d.technicalCrawlVisibility || {};
    var impactLegacy = d.aiOriginatedBusinessImpact || {};
    el.innerHTML =
      '<div class="aiv-signals-grid">' +
      '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
      AiVisibilityUi.escapeHtml(crawlLegacy.label || "Technical Crawl Visibility") +
      '</h3><p class="aiv-theme-block-help help-text">' +
      AiVisibilityUi.escapeHtml(crawlLegacy.helper || "") +
      "</p>" +
      metricRows(crawlLegacy.metrics) +
      "</div>" +
      '<div class="aiv-theme-card aiv-theme-card--future"><h3 class="aiv-theme-label">' +
      AiVisibilityUi.escapeHtml(impactLegacy.label || "AI-Originated Business Impact") +
      '</h3><p class="aiv-theme-block-help help-text">' +
      AiVisibilityUi.escapeHtml(impactLegacy.helper || "") +
      "</p>" +
      metricRows(impactLegacy.metrics) +
      "</div></div>";
  }

  function renderTerritoryKpiCard(el, opts) {
    if (!el) return;
    opts = opts || {};
    var label = opts.intentTerritory || "";
    var display = opts.display && opts.display !== "—" ? opts.display : "";
    var emptyMessage = opts.emptyMessage || "Not available for this brand yet.";
    if (!label && !display) {
      el.innerHTML =
        '<div class="aiv-empty">' + AiVisibilityUi.escapeHtml(emptyMessage) + "</div>";
      return;
    }
    el.innerHTML =
      '<div class="aiv-kpi"><div class="aiv-value">' +
      AiVisibilityUi.escapeHtml(label || display || "—") +
      '</div><div class="aiv-meta">' +
      AiVisibilityUi.escapeHtml(
        display && label ? display : opts.message || display || ""
      ) +
      "</div></div>";
  }

  function intentRowLabel(r) {
    return r.intentLabel || r.intentTerritory || "—";
  }

  function intentRowPresenceValue(r) {
    if (typeof r.subjectPresence === "number") return r.subjectPresence;
    if (typeof r.value === "number") return r.value;
    return null;
  }

  function pickWeakestIntentTerritory(rows) {
    var list = (rows || []).filter(function (r) {
      if (!r) return false;
      var val = intentRowPresenceValue(r);
      return (r.intentLabel || r.intentTerritory) && typeof val === "number" && Number.isFinite(val);
    });
    if (!list.length) return null;
    list = list.slice().sort(function (a, b) {
      var av = intentRowPresenceValue(a);
      var bv = intentRowPresenceValue(b);
      if (av !== bv) return av - bv;
      return String(intentRowLabel(a)).localeCompare(String(intentRowLabel(b)));
    });
    var weakest = list[0];
    var val = intentRowPresenceValue(weakest);
    return {
      intentTerritory: intentRowLabel(weakest),
      intentLabel: intentRowLabel(weakest),
      display:
        weakest.subjectPresenceDisplay ||
        weakest.display ||
        (val != null ? Math.round(val * 100) + "%" : "—"),
      value: val,
    };
  }

  function summarizeIntentCoverageBreadth(rows) {
    var list = (rows || []).filter(function (r) {
      return r && (r.intentLabel || r.intentTerritory);
    });
    if (!list.length) return null;
    var monitored = list.length;
    var present = list.filter(function (r) {
      var val = intentRowPresenceValue(r);
      return typeof val === "number" && Number.isFinite(val) && val > 0;
    }).length;
    return {
      present: present,
      monitored: monitored,
      display: present + " of " + monitored,
      meta:
        present === 0
          ? "No presence in monitored Owner Intents yet."
          : present === monitored
            ? "Present in every monitored Owner Intent."
            : "Present in " + present + " of " + monitored + " monitored Owner Intents.",
    };
  }

  function renderDecisionPatternExtras(intentBlock, topTerritory) {
    // Summary strip is rendered with providers in renderCoverageDiagnosticsSummary.
    void intentBlock;
    void topTerritory;
  }

  function formatOwnerIntentCustomerDate(iso) {
    if (!iso) return null;
    var s = String(iso);
    var d = s.length >= 10 ? s.slice(0, 10) : s;
    var parts = d.split("-");
    if (parts.length !== 3) return s;
    var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    var month = months[Number(parts[1]) - 1];
    var day = String(Number(parts[2]));
    if (!month) return s;
    return month + " " + day + ", " + parts[0];
  }

  function formatPresencePct(rate) {
    if (typeof rate !== "number" || !Number.isFinite(rate)) return null;
    return Math.round(rate * 100) + "%";
  }

  function renderChgVsPriorCell(row) {
    var display = row.chgVsPriorDisplay;
    if (display) {
      return AiVisibilityUi.formatDeltaCell({
        availability: "observed",
        display: display,
      });
    }
    return AiVisibilityUi.formatDeltaCell({
      availability: "insufficient_history",
      display: "Insufficient History",
    });
  }

  function renderHistoricalComparison(row) {
    var status = row.comparisonStatus || (row.chgVsPrior && row.chgVsPrior.comparisonStatus);
    var priorDate = row.priorPeriodDate || (row.chgVsPrior && row.chgVsPrior.priorPeriodDate);
    var currentDate = row.currentPeriodDate || (row.chgVsPrior && row.chgVsPrior.currentPeriodDate);
    var presenceHistory =
      row.presenceHistoryAvailable === true ||
      (row.chgVsPrior && row.chgVsPrior.presenceHistoryAvailable === true);
    var indexHistory =
      typeof row.indexChangePoints === "number" ||
      (row.chgVsPrior && typeof row.chgVsPrior.indexChangePoints === "number");
    if (!priorDate && status === "NO_PRIOR_PERIOD") {
      return (
        '<div class="aiv-intent-detail-block aiv-historical-comparison">' +
        '<div class="aiv-intent-detail-block__label">Historical Comparison</div>' +
        '<p class="aiv-historical-comparison__empty">No comparable prior run yet.</p>' +
        "</div>"
      );
    }
    if (!priorDate && !presenceHistory && !indexHistory) return "";
    var currentRun = formatOwnerIntentCustomerDate(currentDate);
    var priorRun = formatOwnerIntentCustomerDate(priorDate);
    var curPres = formatPresencePct(row.currentPresence != null ? row.currentPresence : row.subjectPresence);
    var priPres = formatPresencePct(row.priorPresence);
    var presChange = row.presenceChangePoints;
    var presenceLine = "";
    if (presenceHistory && curPres && priPres) {
      var presPts =
        typeof presChange === "number"
          ? presChange === 0
            ? "No change"
            : (presChange > 0 ? "+" : "") + String(presChange) + " pts"
          : "";
      presenceLine =
        '<div class="aiv-historical-comparison__line">Presence: ' +
        AiVisibilityUi.escapeHtml(curPres) +
        " vs " +
        AiVisibilityUi.escapeHtml(priPres) +
        " prior" +
        (presPts ? " " + AiVisibilityUi.escapeHtml(presPts) : "") +
        "</div>";
    }
    var indexLine = "";
    if (indexHistory && row.currentIndex != null && row.priorIndex != null) {
      var idxPts = row.chgVsPriorDisplay || "";
      indexLine =
        '<div class="aiv-historical-comparison__line">AI Presence Index: ' +
        AiVisibilityUi.escapeHtml(String(row.currentIndex)) +
        " vs " +
        AiVisibilityUi.escapeHtml(String(row.priorIndex)) +
        " prior" +
        (idxPts ? " " + AiVisibilityUi.escapeHtml(idxPts) : "") +
        "</div>";
    }
    var shortNote =
      row.shortInterval || (row.chgVsPrior && row.chgVsPrior.shortInterval)
        ? '<p class="aiv-historical-comparison__note">These measurements are from separate comparable runs and should be interpreted as period-over-period movement, not a long-term trend.</p>'
        : "";
    return (
      '<div class="aiv-intent-detail-block aiv-historical-comparison">' +
      '<div class="aiv-intent-detail-block__label">Historical Comparison</div>' +
      (currentRun
        ? '<div class="aiv-historical-comparison__line">Current run: ' +
          AiVisibilityUi.escapeHtml(currentRun) +
          "</div>"
        : "") +
      (priorRun
        ? '<div class="aiv-historical-comparison__line">Prior run: ' +
          AiVisibilityUi.escapeHtml(priorRun) +
          "</div>"
        : "") +
      presenceLine +
      indexLine +
      shortNote +
      "</div>"
    );
  }

  function renderOwnerIntentPeerChips(row) {
    var core = (row.selectedCorePeers || []).slice(0, 3);
    var observed = (row.selectedObservedCompetitors || []).slice(0, 3);
    if (!core.length && !observed.length) return "";
    var coreChips = core
      .map(function (name) {
        return (
          '<span class="aiv-peer-chip aiv-peer-chip--core">' +
          AiVisibilityUi.escapeHtml(name) +
          "</span>"
        );
      })
      .join("");
    var observedChips = observed
      .map(function (name) {
        return (
          '<span class="aiv-peer-chip aiv-peer-chip--observed">' +
          AiVisibilityUi.escapeHtml(name) +
          "</span>"
        );
      })
      .join("");
    return (
      '<div class="aiv-owner-intent-context aiv-owner-intent-context--expanded">' +
      (coreChips
        ? '<div class="aiv-intent-detail-block"><div class="aiv-intent-detail-block__label">Core Peers' +
          disclosureInfoIconHtml("CORE_PEERS") +
          '</div><div class="aiv-owner-intent-context__line">' +
          coreChips +
          "</div></div>"
        : "") +
      (observedChips
        ? '<div class="aiv-intent-detail-block"><div class="aiv-intent-detail-block__label">Observed Competitors' +
          disclosureInfoIconHtml("OBSERVED_COMPETITORS") +
          '</div><div class="aiv-owner-intent-context__line">' +
          observedChips +
          "</div></div>"
        : "") +
      "</div>"
    );
  }

  function bindUnifiedIntentExpansion(root) {
    if (!root) return;
    var buttons = root.querySelectorAll(".aiv-intent-expand");
    // ONE_ROW_AT_A_TIME — opening one Owner Intent closes the previous.
    function collapseAll() {
      buttons.forEach(function (other) {
        other.setAttribute("aria-expanded", "false");
        var otherLabel = other.getAttribute("data-intent-label") || "Owner Intent";
        other.setAttribute("aria-label", "Show details for " + otherLabel);
        var otherRow = other.closest("tr");
        if (otherRow) otherRow.classList.remove("is-expanded");
        var otherId = other.getAttribute("aria-controls");
        var otherDetail = otherId ? root.querySelector("#" + otherId) : null;
        if (otherDetail) otherDetail.hidden = true;
      });
    }
    buttons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var opening = btn.getAttribute("aria-expanded") !== "true";
        collapseAll();
        if (!opening) return;
        btn.setAttribute("aria-expanded", "true");
        var label = btn.getAttribute("data-intent-label") || "Owner Intent";
        btn.setAttribute("aria-label", "Hide details for " + label);
        var row = btn.closest("tr");
        if (row) row.classList.add("is-expanded");
        var detailId = btn.getAttribute("aria-controls");
        var detail = detailId ? root.querySelector("#" + detailId) : null;
        if (detail) detail.hidden = false;
      });
    });
  }

  function renderIntentCoverage(el, intentBlock) {
    if (!el) return;
    intentBlock = intentBlock || {};
    if (intentBlock.availability === "future_ready") {
      el.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">' +
        AiVisibilityUi.escapeHtml(
          intentBlock.message ||
            "Owner-intent coverage is not available for this brand yet."
        ) +
        "</div>";
      return;
    }
    if (!(intentBlock.rows || []).length) {
      el.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">No Owner Intent coverage for this geography yet.</div>';
      return;
    }

    var isUnified = intentBlock.unified === true;
    if (!isUnified) {
      var hasUnifiedShape = intentBlock.rows.some(function (r) {
        return r && r.intentLabel && r.scenarioId;
      });
      isUnified = hasUnifiedShape;
    }

    if (!isUnified) {
      el.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">' +
        AiVisibilityUi.escapeHtml(
          "Owner Intent coverage is loading with the latest diagnostics format. Refresh this page or restart the server if this message persists."
        ) +
        "</div>";
      if (typeof console !== "undefined" && console.warn) {
        console.warn(
          "[aiv-brand] legacy owner-intent coverage blocked — expected unified payload",
          intentBlock
        );
      }
      return;
    }

    el.innerHTML =
      '<div class="deals-table-container aiv-portfolio-table-wrap aiv-coverage-table-wrap aiv-unified-intent-table-wrap">' +
      '<table class="deals-table aiv-portfolio-table aiv-coverage-table aiv-intent-coverage-table aiv-unified-intent-table">' +
      "<thead><tr>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Owner<br>Intent</span>' +
      disclosureInfoIconHtml("OWNER_INTENT") +
      "</span></th>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Your<br>Presence</span>' +
      disclosureInfoIconHtml("YOUR_PRESENCE") +
      "</span></th>" +
      '<th class="no-sort aiv-chg-th"><span class="aiv-th-label"><span class="aiv-th-text">Δ vs<br>prior run</span>' +
      disclosureInfoIconHtml("CHG_VS_PRIOR_RUN") +
      "</span></th>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Monitored</span>' +
      disclosureInfoIconHtml("MONITORED") +
      "</span></th>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Missing</span>' +
      disclosureInfoIconHtml("MISSING") +
      "</span></th>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Peer-Present<br>Gaps</span>' +
      disclosureInfoIconHtml("PEER_PRESENT_GAPS") +
      "</span></th>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">AI Presence<br>Index</span>' +
      disclosureInfoIconHtml("AI_PRESENCE_INDEX") +
      "</span></th>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Position</span></span></th>' +
      "</tr></thead><tbody>" +
      intentBlock.rows
        .map(function (r, idx) {
          var presenceMain = r.subjectPresenceDisplay || "—";
          var pct =
            typeof r.subjectPresence === "number" && Number.isFinite(r.subjectPresence)
              ? Math.max(0, Math.min(100, Math.round(r.subjectPresence * 100)))
              : null;
          var presenceVisual =
            pct != null
              ? '<span class="aiv-presence-cell aiv-presence-cell--compact"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' +
                pct +
                '%"></span></span><span class="aiv-presence-cell__value">' +
                AiVisibilityUi.escapeHtml(presenceMain) +
                "</span></span>"
              : AiVisibilityUi.escapeHtml(presenceMain);
          var monitoredPresent =
            r.withPresenceCount != null
              ? r.withPresenceCount
              : r.presentN != null
                ? r.presentN
                : null;
          var monitoredTotal =
            r.comparableObservationCount != null
              ? r.comparableObservationCount
              : r.monitoredN != null
                ? r.monitoredN
                : null;
          var monitoredCell =
            monitoredPresent != null && monitoredTotal != null
              ? String(monitoredPresent) + "/" + String(monitoredTotal)
              : "—";
          var missing =
            r.missingCount != null
              ? String(r.missingCount)
              : "Not enough comparable evidence";
          var peerGaps =
            r.peerPresentGapCount != null
              ? String(r.peerPresentGapCount)
              : "Not enough comparable evidence";
          var certified = typeof r.indexValue === "number";
          var indexCell = certified
            ? '<span class="aiv-intent-index">' +
              AiVisibilityUi.escapeHtml(String(r.indexValue)) +
              "</span>"
            : '<span class="aiv-owner-intent-developing">' +
              AiVisibilityUi.escapeHtml(r.benchmarkStatus || "Benchmark still developing") +
              "</span>";
          var positionCell = r.position ? AiVisibilityUi.escapeHtml(r.position) : "";
          var chgCell = renderChgVsPriorCell(r);
          var historicalBlock = renderHistoricalComparison(r);
          var peerContext = renderOwnerIntentPeerChips(r);
          var decisionContextBlock = r.decisionContext
            ? '<div class="aiv-intent-detail-block"><div class="aiv-intent-detail-block__label">Decision Context</div><p class="aiv-intent-decision-context aiv-intent-decision-context--expanded">' +
              AiVisibilityUi.escapeHtml(r.decisionContext) +
              "</p></div>"
            : "";
          var hasDetail = !!(decisionContextBlock || peerContext || historicalBlock);
          var detailId = "aiv-intent-detail-" + idx;
          var intentLabel = r.intentLabel || "—";
          var expandControl = hasDetail
            ? '<button type="button" class="aiv-intent-expand" aria-expanded="false" aria-controls="' +
              detailId +
              '" data-intent-label="' +
              AiVisibilityUi.escapeHtml(intentLabel) +
              '" aria-label="Show details for ' +
              AiVisibilityUi.escapeHtml(intentLabel) +
              '"><span class="aiv-intent-chevron" aria-hidden="true"></span><span class="project-name-text">' +
              AiVisibilityUi.escapeHtml(intentLabel) +
              "</span></button>"
            : '<span class="project-name-text">' +
              AiVisibilityUi.escapeHtml(intentLabel) +
              "</span>";
          var detailRow = hasDetail
            ? '<tr class="aiv-unified-intent-detail" id="' +
              detailId +
              '" hidden><td colspan="8"><div class="aiv-intent-detail-panel">' +
              decisionContextBlock +
              (peerContext || "") +
              (historicalBlock || "") +
              "</div></td></tr>"
            : "";
          return (
            '<tr class="aiv-intent-row aiv-unified-intent-row">' +
            "<td>" +
            expandControl +
            "</td>" +
            '<td class="aiv-metric-cell aiv-presence-metric-cell">' +
            presenceVisual +
            "</td>" +
            '<td class="aiv-metric-cell aiv-delta-cell aiv-chg-metric-cell">' +
            chgCell +
            "</td>" +
            '<td class="aiv-metric-cell aiv-monitored-cell">' +
            AiVisibilityUi.escapeHtml(monitoredCell) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(missing) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(peerGaps) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            indexCell +
            "</td>" +
            '<td class="aiv-delta-cell">' +
            positionCell +
            "</td>" +
            "</tr>" +
            detailRow
          );
        })
        .join("") +
      "</tbody></table></div>";

    bindUnifiedIntentExpansion(el);
  }

  function renderLegacyIntentCoverage(el, intentBlock) {
    var weakest = pickWeakestIntentTerritory(intentBlock.rows);
    var strongestLabel =
      intentBlock.rows
        .slice()
        .filter(function (r) {
          return typeof r.value === "number" && Number.isFinite(r.value);
        })
        .sort(function (a, b) {
          return b.value - a.value;
        })[0] || null;

    el.innerHTML =
      '<div class="deals-table-container aiv-portfolio-table-wrap aiv-coverage-table-wrap">' +
      '<table class="deals-table aiv-portfolio-table aiv-coverage-table aiv-intent-coverage-table">' +
      "<thead><tr>" +
      '<th class="no-sort">Prompt Family</th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">AI<br>Presence</span></span></th>' +
      '<th class="no-sort">Monitored</th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">With<br>Presence</span></span></th>' +
      '<th class="no-sort">Missing</th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Peer-Present<br>Gaps</span></span></th>' +
      "</tr></thead><tbody>" +
      intentBlock.rows
        .map(function (r) {
          var pct =
            typeof r.value === "number"
              ? Math.max(0, Math.min(100, Math.round(r.value * 100)))
              : null;
          var monitored =
            r.monitoredN != null
              ? r.monitoredN
              : r.MONITORED_N != null
                ? r.MONITORED_N
                : r.denominator != null
                  ? r.denominator
                  : null;
          var present =
            r.presentN != null
              ? r.presentN
              : r.PRESENT_N != null
                ? r.PRESENT_N
                : r.numerator != null
                  ? r.numerator
                  : typeof r.value === "number" && monitored != null
                    ? Math.round(r.value * monitored)
                    : null;
          var missing =
            monitored != null && present != null
              ? Math.max(0, monitored - present)
              : null;
          var peerGaps =
            r.peerPresentSubjectMissingN != null
              ? r.peerPresentSubjectMissingN
              : r.PEER_PRESENT_SUBJECT_MISSING_N != null
                ? r.PEER_PRESENT_SUBJECT_MISSING_N
                : "0";
          var isStrong =
            strongestLabel &&
            r.intentTerritory === strongestLabel.intentTerritory;
          var isWeak =
            weakest && r.intentTerritory === weakest.intentTerritory;
          var badge = "";
          if (isStrong) badge = ' <span class="aiv-you-tag">Best</span>';
          else if (isWeak) badge = ' <span class="aiv-you-tag">Weakest</span>';
          var presenceCell =
            pct == null
              ? AiVisibilityUi.escapeHtml(r.display || "—")
              : '<span class="aiv-presence-cell"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' +
                pct +
                '%"></span></span><span class="aiv-presence-cell__value">' +
                AiVisibilityUi.escapeHtml(
                  r.display || (Math.round(r.value * 1000) / 10).toFixed(1) + "%"
                ) +
                "</span></span>";
          return (
            '<tr class="aiv-intent-row">' +
            "<td><span class=\"project-name-text\">" +
            AiVisibilityUi.escapeHtml(r.intentTerritory || "—") +
            "</span>" +
            badge +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            presenceCell +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(monitored != null ? String(monitored) : "—") +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(present != null ? String(present) : "—") +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(missing != null ? String(missing) : "—") +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(String(peerGaps)) +
            "</td>" +
            "</tr>"
          );
        })
        .join("") +
      "</tbody></table></div>";
  }

  function renderAiVsDealality(tbodyId, ctx) {
    var section = $("aivDetailAiVsSection");
    var ctxBody = $(tbodyId);
    if (!ctxBody) return;
    ctx = ctx || {};
    var ctxRows = ctx.rows || [];
    if (!ctxRows.length) {
      if (section) section.hidden = true;
      ctxBody.innerHTML = "";
      return;
    }
    if (section) section.hidden = false;
    ctxBody.innerHTML = ctxRows
      .map(function (r) {
        var ownerIntent = r.ownerIntent || r.intentLabel || "Owner Decision Scenario";
        var decisionContext = r.decisionContext || "—";
        var aiRep = r.aiRepresentation || r.aiPattern || "—";
        return (
          "<tr><td>" +
          AiVisibilityUi.escapeHtml(ownerIntent) +
          "</td><td class=\"aiv-wl-decision-context\">" +
          AiVisibilityUi.escapeHtml(decisionContext) +
          "</td><td>" +
          AiVisibilityUi.escapeHtml(aiRep) +
          "</td><td>" +
          AiVisibilityUi.escapeHtml(
            r.dealalityContext || "Dealality context not yet available"
          ) +
          '</td><td class="aiv-metric-cell">' +
          AiVisibilityUi.escapeHtml(r.reviewStatus || "—") +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderReviewItems(el, items) {
    var section = $("aivDetailReviewSection");
    if (!el) return;
    items = items || [];
    if (!items.length) {
      if (section) section.hidden = true;
      el.innerHTML = "";
      return;
    }
    if (section) section.hidden = false;
    el.innerHTML = items
      .map(function (item, idx) {
        var title = cleanPriorityClientCopy(item.title || item.type || "Review Item");
        var desc = cleanPriorityClientCopy(item.description || item.text || "");
        var disposition = formatExecutiveDispositionLabel(item.actionDisposition);
        return (
          '<article class="aiv-theme-card aiv-hdv-review-card"><div class="aiv-hdv-review-index">' +
          (idx + 1) +
          '</div><h3 class="aiv-review-kind">' +
          AiVisibilityUi.escapeHtml(title) +
          (disposition
            ? ' <span class="aiv-meta-tag">' + AiVisibilityUi.escapeHtml(disposition) + "</span>"
            : "") +
          '</h3><p class="aiv-hdv-review-desc">' +
          AiVisibilityUi.escapeHtml(desc) +
          '</p><div class="aiv-hdv-review-meta">' +
          AiVisibilityUi.escapeHtml(item.geography || "") +
          (item.providerLabel
            ? " · " + AiVisibilityUi.escapeHtml(item.providerLabel)
            : "") +
          (item.evidenceId
            ? ' · <button type="button" class="aiv-btn-text aiv-link" data-evidence="' +
              AiVisibilityUi.escapeHtml(item.evidenceId) +
              '" data-brand="' +
              AiVisibilityUi.escapeHtml(item.brandId || state.brandId || "") +
              '" data-evidence-provider="' +
              AiVisibilityUi.escapeHtml(item.provider || state.provider || "") +
              '" data-evidence-language="' +
              AiVisibilityUi.escapeHtml(
                item.language || state.language || ""
              ) +
              '">View Evidence</button>'
            : "") +
          "</div></article>"
        );
      })
      .join("");
    el.querySelectorAll("[data-evidence]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openEvidence(btn.getAttribute("data-evidence"), {
          brandId: btn.getAttribute("data-brand") || state.brandId,
          provider: btn.getAttribute("data-evidence-provider") || null,
          language: btn.getAttribute("data-evidence-language") || null,
        });
      });
    });
  }

  function formatBestCompetitivePosition(bestPos) {
    bestPos = bestPos || {};
    var name = String(bestPos.brandName || bestPos.brandId || "").trim();
    var display = String(bestPos.display || "").trim();
    if (!display || display === "—") return "Not Monitored";
    // All Providers has no single peer ladder — guide the user to pick a provider.
    if (/select a provider/i.test(display)) return display;
    // Never surface bogus "#0 of N" ranks (null-presence ranking bug residue).
    if (/#?0\s+of\s+\d+/i.test(display)) {
      return name || "Not Monitored";
    }
    // Strip repeated "Brand — " prefixes left by older API payloads.
    if (name) {
      var prefix = name + " — ";
      while (display.indexOf(prefix) === 0) {
        display = display.slice(prefix.length).trim();
      }
      if (display === name) return name;
      return name + " — " + display;
    }
    return display;
  }

  function renderAllProvidersPanel(panel, targetId) {
    var el = $(targetId || "aivExecAllProviders");
    if (!el) return;
    var p = panel || {};
    if (!p.READY && !p.PROVIDERS_MONITORED) {
      el.innerHTML =
        '<div class="aiv-empty">Cross-provider comparison appears when multiple providers have completed monitoring for this geography.</div>';
      return;
    }
    if (p.NOT_COMPARABLE) {
      el.innerHTML =
        '<div class="aiv-empty">Not Comparable — provider observations cannot validly be aggregated for this cohort.</div>';
      return;
    }
    var monitored = p.PROVIDERS_MONITORED || [];
    var appears = p.PROVIDERS_WHERE_BRAND_APPEARS || [];
    var strong = p.STRONGEST_PROVIDER || p.STRONGEST_PROVIDER_BY_PRESENCE;
    var weak = p.WEAKEST_PROVIDER || p.WEAKEST_PROVIDER_BY_PRESENCE;
    var breakdown = p.PRESENCE_BY_PROVIDER || p.PROVIDER_PRESENCE_BREAKDOWN || [];
    var ranked = (Array.isArray(breakdown) ? breakdown.slice() : [])
      .map(function (row) {
        return {
          provider: row.provider || row.id || "",
          label: formatProviderDisplayName(row.label || row.provider || row.id),
          rate: providerPresenceRate(row),
          availability: row.availability,
        };
      })
      .sort(function (a, b) {
        var ar = typeof a.rate === "number" ? a.rate : -1;
        var br = typeof b.rate === "number" ? b.rate : -1;
        return br - ar;
      });
    var maxRate = 0;
    ranked.forEach(function (row) {
      if (typeof row.rate === "number" && row.rate > maxRate) maxRate = row.rate;
    });
    if (maxRate <= 0) maxRate = 1;

    function providerLine(node) {
      if (!node) return "—";
      var name = formatProviderDisplayName(node.label || node.provider);
      var rate =
        typeof node.rate === "number"
          ? pctLabel(node.rate)
          : typeof node.presenceRate === "number"
            ? pctLabel(node.presenceRate)
            : null;
      return rate ? name + " · " + rate : name;
    }

    var listHtml = ranked
      .map(function (row) {
        var rateLabel =
          typeof row.rate === "number"
            ? pctLabel(row.rate)
            : row.availability === "not_monitored"
              ? "Not Monitored"
              : "—";
        var width =
          typeof row.rate === "number"
            ? Math.max(4, Math.round((row.rate / maxRate) * 100))
            : 0;
        return (
          '<div class="aiv-provider-rank-row">' +
          '<div class="aiv-provider-rank-label">' +
          AiVisibilityUi.escapeHtml(row.label) +
          "</div>" +
          '<div class="aiv-provider-rank-track" aria-hidden="true">' +
          '<div class="aiv-provider-rank-fill" style="width:' +
          width +
          '%"></div></div>' +
          '<div class="aiv-provider-rank-value">' +
          AiVisibilityUi.escapeHtml(rateLabel) +
          "</div></div>"
        );
      })
      .join("");

    el.innerHTML =
      '<div class="aiv-intel-kpi-pair">' +
      '<div class="aiv-intel-kpi"><div class="aiv-intel-kpi__value">' +
      AiVisibilityUi.escapeHtml(String(monitored.length || "—")) +
      '</div><div class="aiv-intel-kpi__label">monitored</div></div>' +
      '<div class="aiv-intel-kpi"><div class="aiv-intel-kpi__value">' +
      AiVisibilityUi.escapeHtml(String(appears.length || "—")) +
      '</div><div class="aiv-intel-kpi__label">with Presence</div></div>' +
      "</div>" +
      '<div class="aiv-intel-strong-weak">' +
      '<div><div class="aiv-intel-mini-label">Strongest</div><div class="aiv-intel-mini-value">' +
      AiVisibilityUi.escapeHtml(providerLine(strong)) +
      "</div></div>" +
      '<div><div class="aiv-intel-mini-label">Weakest</div><div class="aiv-intel-mini-value">' +
      AiVisibilityUi.escapeHtml(providerLine(weak)) +
      "</div></div>" +
      "</div>" +
      (listHtml
        ? '<div class="aiv-provider-rank-list" aria-label="Presence by provider">' +
          listHtml +
          "</div>"
        : "");
  }

  /** Top prompt-family rows for Gaps / Risks — does not restate Snapshot Questions Missing KPI. */
  function buildPromptFamilyGapItems(data, existingGaps) {
    var fam = data.promptFamilyMissing;
    var families = fam && Array.isArray(fam.families) ? fam.families : [];
    if (!families.length) return [];
    var existingBlob = (existingGaps || [])
      .map(function (g) {
        return String(g.label || "") + " " + String(g.text || g.value || "");
      })
      .join(" ")
      .toLowerCase();
    var out = [];
    families.slice(0, 4).forEach(function (f) {
      var name = f.promptFamily || "Unspecified";
      if (existingBlob.indexOf(String(name).toLowerCase()) !== -1) return;
      var monitored = f.MONITORED_QUESTIONS != null ? f.MONITORED_QUESTIONS : "—";
      var missing = f.QUESTIONS_MISSING != null ? f.QUESTIONS_MISSING : "—";
      out.push({
        label: name,
        text:
          String(missing) +
          " missing of " +
          String(monitored) +
          " monitored questions",
      });
    });
    return out;
  }

  function renderExecGapsMerged(data) {
    var gaps = Array.isArray(data.gaps) ? data.gaps.slice() : [];
    var familyItems = buildPromptFamilyGapItems(data, gaps);
    var combined = gaps.concat(familyItems);
    $("aivExecGaps").innerHTML = kvRows(combined, {
      emptyMessage: "No factual gaps recorded for this geography.",
      fallbackLabel: "Gap",
    });
  }

  function priorityReviewMentionsPeerPresent(priItems) {
    var blob = (priItems || [])
      .map(function (i) {
        return (
          String(i.title || "") +
          " " +
          String(i.description || "") +
          " " +
          String(i.text || "")
        );
      })
      .join(" ")
      .toLowerCase();
    return (
      blob.indexOf("peer") !== -1 &&
      (blob.indexOf("not observed") !== -1 ||
        blob.indexOf("subject brand") !== -1 ||
        blob.indexOf("where we are not") !== -1)
    );
  }

  function formatPresenceCompact(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return null;
    return pctLabel(v);
  }

  function escapeRegExp(s) {
    return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function stripReviewFilterNoise(text, data) {
    var t = String(text || "");
    if (!t) return "";
    var geo =
      (data && data.geography && (data.geography.key || data.geography.commercialRegion)) ||
      state.geography ||
      "";
    var providerLabel =
      (data && data.providerLabel) ||
      formatProviderDisplayName((data && data.provider) || state.provider) ||
      "";
    if (geo) {
      t = t.replace(
        new RegExp("\\bin\\s+" + escapeRegExp(geo) + "\\b", "gi"),
        ""
      );
      t = t.replace(
        new RegExp(escapeRegExp(geo) + "\\s*\\([^)]*monitoring[^)]*\\)", "gi"),
        ""
      );
      t = t.replace(new RegExp("^" + escapeRegExp(geo) + "\\s*[·•|,:—-]+\\s*", "i"), "");
      t = t.replace(new RegExp("\\s*[·•|,]\\s*" + escapeRegExp(geo) + "\\b", "gi"), "");
    }
    if (providerLabel && providerLabel !== "—") {
      t = t.replace(
        new RegExp(escapeRegExp(providerLabel) + "\\s*monitoring", "gi"),
        ""
      );
      t = t.replace(
        new RegExp("\\(" + escapeRegExp(providerLabel) + "[^)]*\\)", "gi"),
        ""
      );
    }
    t = t.replace(/\(\s*\)/g, "");
    t = t.replace(/\s{2,}/g, " ").replace(/\s+([.,;:])/g, "$1").trim();
    t = t.replace(/^[-–—·|,:\s]+/, "").replace(/[-–—·|,:\s]+$/, "");
    return t;
  }

  function buildFilterContextLine(data) {
    var geo =
      (data && data.geography && (data.geography.key || data.geography.commercialRegion)) ||
      state.geography ||
      "";
    var provider =
      (data && data.providerLabel) ||
      formatProviderDisplayName((data && data.provider) || state.provider);
    if (
      data &&
      (data.providerMode === "DERIVED" ||
        data.ALL_PROVIDERS_DERIVED === true ||
        String(data.provider || "").toLowerCase() === "all")
    ) {
      provider = "All Providers";
    }
    var langCode = (data && data.language) || state.language || "";
    var lang =
      langCode === "en"
        ? "English"
        : langCode === "es"
          ? "Spanish"
          : langCode
            ? String(langCode).toUpperCase()
            : "";
    return [geo, provider, lang].filter(Boolean).join(" · ");
  }

  function gotoDetailSection(opts) {
    opts = opts || {};
    var brandId = opts.brandId || state.brandId;
    if (brandId) state.brandId = brandId;
    if (opts.watchlistMode) {
      watchlistState.mode = opts.watchlistMode;
    }
    state._detailNav = {
      targetId: opts.targetId || "aivDetailToplineSection",
      watchlistMode: opts.watchlistMode || null,
    };
    try {
      if (opts.targetId) {
        history.replaceState(
          null,
          "",
          (window.location.pathname || "") +
            (window.location.search || "") +
            "#" +
            opts.targetId
        );
      }
    } catch (_) {}
    var p = loadDetail();
    if (p && typeof p.then === "function") {
      p.then(function () {
        applyDetailNavTarget(state._detailNav);
      }).catch(function (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[aiv] detail navigation failed", err);
        }
      });
    } else {
      setActiveTab("detail");
    }
  }

  function applyDetailNavTarget(nav) {
    nav = nav || state._detailNav || {};
    if (nav.watchlistMode) {
      var wlTabs = $("aivWatchlistTabs");
      if (wlTabs) {
        wlTabs.querySelectorAll("button[data-watchlist]").forEach(function (b) {
          b.classList.toggle(
            "is-active",
            b.getAttribute("data-watchlist") === nav.watchlistMode
          );
        });
      }
    }
    var target = $(nav.targetId || "");
    if (target && target.scrollIntoView) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    state._detailNav = null;
  }

  function detailTargetForPriorityKind(kind) {
    var k = String(kind || "").toLowerCase();
    if (k.indexOf("peer") !== -1 || k.indexOf("competitive") !== -1) {
      return {
        targetId: "aivDetailPeerSection",
        watchlistMode: k.indexOf("peer") !== -1 ? "peer_gaps" : null,
      };
    }
    if (k.indexOf("missing") !== -1 || k.indexOf("question") !== -1) {
      return { targetId: "aivDetailWatchlistSection", watchlistMode: "missing" };
    }
    if (k.indexOf("provider") !== -1 || k.indexOf("intent") !== -1) {
      return { targetId: "aivDetailCoverageSection" };
    }
    if (k.indexOf("citation") !== -1 || k.indexOf("source") !== -1) {
      return { targetId: "aivDetailCitationSection" };
    }
    if (k.indexOf("discover") !== -1) {
      return { targetId: "aivDetailDiscoverabilitySection" };
    }
    if (k.indexOf("language") !== -1) {
      return { targetId: "aivDetailLanguageSection" };
    }
    if (
      k.indexOf("visibility change") !== -1 ||
      k.indexOf("presence change") !== -1 ||
      k.indexOf("trend") !== -1 ||
      k.indexOf("movement") !== -1
    ) {
      return { targetId: "aivDetailTrendSection" };
    }
    return { targetId: "aivDetailReviewSection" };
  }

  function renderOriginalCompetitiveContext(data) {
    var el = $("aivExecCompetitive");
    if (!el) return;
    var cc = data.competitiveContext || {};
    var cp = data.currentPosition || {};
    var top = cp.topBrandByAiPresence || {};
    var bestPos = cp.bestCompetitivePosition || cp.topBrandByCompetitivePosition || {};
    var peerMissing = data.peerPresentSubjectMissing || {};
    var n = peerMissing.PEER_PRESENT_SUBJECT_MISSING_N || 0;

    var yourName =
      cc.subjectBrandName ||
      top.brandName ||
      top.brandId ||
      bestPos.brandName ||
      bestPos.brandId ||
      "";
    var yourPresenceNum =
      typeof top.presence === "number" && Number.isFinite(top.presence)
        ? top.presence
        : null;
    var yourPresence =
      formatPresenceCompact(yourPresenceNum) ||
      (top.display && top.display !== "—" ? String(top.display) : null);
    var yourRankDisplay = "";
    if (bestPos.display && String(bestPos.display).indexOf("#") !== -1) {
      yourRankDisplay = String(bestPos.display).replace(/^.*?(#\d.*)$/, "$1").trim();
      if (yourRankDisplay === String(bestPos.display) && bestPos.brandName) {
        yourRankDisplay = formatBestCompetitivePosition(bestPos).replace(
          new RegExp("^" + escapeRegExp(bestPos.brandName) + "\\s*[—-]\\s*", "i"),
          ""
        );
      }
    } else if (bestPos.rank != null) {
      yourRankDisplay =
        "#" +
        bestPos.rank +
        (bestPos.peerCount != null ? " of " + bestPos.peerCount : "");
    }

    var peerName = (cc.leadingPeer && cc.leadingPeer.name) || "";
    var peerPresenceNum =
      cc.leadingPeer &&
      typeof cc.leadingPeer.presence === "number" &&
      Number.isFinite(cc.leadingPeer.presence)
        ? cc.leadingPeer.presence
        : null;
    var peerPct = formatPresenceCompact(peerPresenceNum);
    var peerRank =
      cc.leadingPeer && cc.leadingPeer.rank != null
        ? "#" + cc.leadingPeer.rank
        : null;

    if (!peerName && !yourName) {
      el.innerHTML = emptyBlock(
        cc.message || "No competitive context for monitored entitled brands yet."
      );
      return;
    }

    var leadGapHtml = "";
    if (yourPresenceNum != null && peerPresenceNum != null) {
      var deltaPp = Math.round((yourPresenceNum - peerPresenceNum) * 1000) / 10;
      var absPp = Math.abs(deltaPp);
      var leadLabel = deltaPp >= 0 ? "Presence Lead" : "Presence Gap";
      var leadValue =
        (deltaPp > 0 ? "+" : deltaPp < 0 ? "−" : "") +
        String(absPp) +
        " pp";
      leadGapHtml =
        '<div class="aiv-snap-stat">' +
        '<div class="aiv-snap-stat__label">' +
        AiVisibilityUi.escapeHtml(leadLabel) +
        "</div>" +
        '<div class="aiv-snap-stat__value aiv-snap-stat__value--lead">' +
        AiVisibilityUi.escapeHtml(leadValue) +
        "</div>" +
        '<div class="aiv-snap-stat__sub">vs leading peer</div>' +
        "</div>";
    } else {
      leadGapHtml =
        '<div class="aiv-snap-stat">' +
        '<div class="aiv-snap-stat__label">Presence Lead</div>' +
        '<div class="aiv-snap-stat__value">—</div>' +
        '<div class="aiv-snap-stat__sub">Comparable peer Presence unavailable</div>' +
        "</div>";
    }

    var gapStatHtml =
      '<div class="aiv-snap-stat">' +
      '<div class="aiv-snap-stat__label">Peer-Present Gap</div>' +
      '<div class="aiv-snap-stat__value">' +
      AiVisibilityUi.escapeHtml(
        n > 0 ? String(n) + (n === 1 ? " question" : " questions") : "0"
      ) +
      "</div>" +
      '<div class="aiv-snap-stat__sub">' +
      AiVisibilityUi.escapeHtml(
        n > 0
          ? "Peer appeared while your brand was not observed."
          : "No peer-present gaps in this cohort."
      ) +
      "</div></div>";

    var html =
      '<div class="aiv-snap-scoreboard">' +
      '<div class="aiv-snap-stat">' +
      '<div class="aiv-snap-stat__label">Your Position</div>' +
      (yourName
        ? '<div class="aiv-snap-stat__name" title="' +
          AiVisibilityUi.escapeHtml(yourName) +
          '">' +
          AiVisibilityUi.escapeHtml(yourName) +
          "</div>"
        : "") +
      (yourRankDisplay
        ? '<div class="aiv-snap-stat__rank">' +
          AiVisibilityUi.escapeHtml(yourRankDisplay) +
          "</div>"
        : "") +
      (yourPresence
        ? '<div class="aiv-snap-stat__sub">' +
          AiVisibilityUi.escapeHtml(yourPresence + " AI Presence") +
          "</div>"
        : "") +
      "</div>" +
      '<div class="aiv-snap-stat">' +
      '<div class="aiv-snap-stat__label">Leading Peer</div>' +
      (peerName
        ? '<div class="aiv-snap-stat__name" title="' +
          AiVisibilityUi.escapeHtml(peerName) +
          '">' +
          AiVisibilityUi.escapeHtml(peerName) +
          "</div>"
        : '<div class="aiv-snap-stat__name">Not observed</div>') +
      (peerRank
        ? '<div class="aiv-snap-stat__rank">' +
          AiVisibilityUi.escapeHtml(peerRank) +
          "</div>"
        : "") +
      (peerPct
        ? '<div class="aiv-snap-stat__sub">' +
          AiVisibilityUi.escapeHtml(peerPct + " AI Presence") +
          "</div>"
        : "") +
      "</div>" +
      leadGapHtml +
      gapStatHtml +
      "</div>";

    if (n > 0) {
      var row = (peerMissing.rows || [])[0];
      if (row) {
        var peerNames = (row.PEERS_PRESENT || [])
          .map(function (p) {
            var name = p.entityName || p.name || "";
            if (!name || /^rec[a-zA-Z0-9]{10,}$/.test(String(name))) return "";
            return name;
          })
          .filter(Boolean)
          .slice(0, 2)
          .join(", ");
        var family = String(row.PROMPT_FAMILY || row.ownerIntent || row.intentLabel || "").trim();
        var q = String(row.decisionContext || row.ownerIntent || row.promptId || "").trim();
        if (q.length > 64) q = q.slice(0, 61) + "…";
        var exampleLine = "";
        if (peerNames && family) {
          exampleLine = peerNames + " · " + family;
        } else if (peerNames && q) {
          exampleLine = peerNames + " · " + q;
        } else if (peerNames) {
          exampleLine = peerNames;
        } else if (family || q) {
          exampleLine = family || q;
        }
        if (exampleLine) {
          html +=
            '<div class="aiv-snap-example-block">' +
            '<div class="aiv-snap-example-label">Example</div>' +
            '<p class="aiv-snap-example-text">' +
            AiVisibilityUi.escapeHtml(exampleLine) +
            "</p></div>";
        }
      }
      html +=
        '<button type="button" class="aiv-inline-link" data-aiv-goto="detail-peer-gap">View full peer-gap analysis →</button>';
    }

    el.innerHTML = html;
    el.querySelectorAll("[data-aiv-goto='detail-peer-gap']").forEach(function (btn) {
      btn.addEventListener("click", function () {
        gotoDetailSection({
          brandId: cc.subjectBrandId || top.brandId || state.brandId,
          targetId: "aivDetailWatchlistSection",
          watchlistMode: "peer_gaps",
        });
      });
    });
  }

  function priorityCategoryLabel(item) {
    var type = String((item && item.type) || "").toLowerCase();
    var title = String((item && item.title) || "").trim();
    var titleKey = title.toLowerCase();
    if (
      type.indexOf("peer") !== -1 ||
      titleKey.indexOf("competitive") !== -1 ||
      titleKey.indexOf("peer") !== -1
    ) {
      return "Competitive Gap";
    }
    if (
      type.indexOf("question") !== -1 ||
      type.indexOf("missing") !== -1 ||
      titleKey.indexOf("missing") !== -1 ||
      titleKey.indexOf("absent") !== -1
    ) {
      return "Questions Missing";
    }
    if (
      type.indexOf("provider") !== -1 ||
      titleKey.indexOf("provider") !== -1
    ) {
      return "Provider Difference";
    }
    if (
      type.indexOf("presence_change") !== -1 ||
      type.indexOf("comparable") !== -1 ||
      titleKey.indexOf("visibility change") !== -1 ||
      titleKey.indexOf("presence change") !== -1
    ) {
      return "Visibility Change";
    }
    if (type.indexOf("citation") !== -1 || type.indexOf("owned") !== -1) {
      return "Citation Gap";
    }
    if (type.indexOf("discoverability") !== -1) {
      return "Discoverability";
    }
    if (title && title.length <= 36 && !/deterministic|airtable|opportunit/i.test(title)) {
      return title;
    }
    return "Review";
  }

  function extractPriorityNumeric(text) {
    var raw = String(text || "");
    var m = raw.match(/([+-]?\d+(?:\.\d+)?)\s*(pp|%)/i);
    if (!m) return null;
    // Keep the full sentence intact — stripping the measure left broken copy
    // ("absent on of…", "trails … by AI Presence").
    return {
      value: m[1] + (m[2].toLowerCase() === "pp" ? " pp" : "%"),
      cleaned: raw.trim(),
    };
  }

  function cleanPriorityClientCopy(text) {
    var t = String(text || "");
    t = t.replace(/\bsubject brand\b/gi, "your brand");
    t = t.replace(/\bdeterministic rules?\b/gi, "consistent rules");
    t = t.replace(/\bnot Airtable Opportunities\b/gi, "");
    t = t.replace(/\bnot Opportunities\b/gi, "");
    t = t.replace(/\bAirtable\b/gi, "");
    t = t.replace(/\bscaffold\b/gi, "");
    t = t.replace(/\bclassifier\b/gi, "");
    t = t.replace(/\bfixture\b/gi, "");
    t = t.replace(/\bmetric contract\b/gi, "");
    t = t.replace(/\bprovider adapter\b/gi, "");
    t = t.replace(/\bevidenceId\b/gi, "");
    t = t.replace(/\bresponseId\b/gi, "");
    t = t.replace(/\(\s*\)/g, "");
    t = t.replace(/\s{2,}/g, " ").trim();
    t = t.replace(/^[-–—·|,:\s]+/, "").replace(/[-–—·|,:\s]+$/, "");
    return t;
  }

  function renderPriorityReviewCompact(data, entitledCount) {
    var el = $("aivExecPriority");
    if (!el) return;
    var pri = data.priorityReviewItems || {};
    if (entitledCount === 0) {
      el.innerHTML =
        '<div class="aiv-empty">Priority Review requires entitled brands in the active portfolio.</div>';
      return;
    }
    if (pri.status === "future_ready" || !(pri.items || []).length) {
      el.innerHTML =
        '<div class="aiv-empty">' +
        AiVisibilityUi.escapeHtml(
          pri.message || "No evidence-backed review items for this geography yet."
        ) +
        "</div>";
      return;
    }
    var context = buildFilterContextLine(data);
    var allItems = pri.items || [];
    var visible = allItems.slice(0, 4);
    var html = "";
    if (context) {
      html +=
        '<div class="aiv-priority-context">' +
        AiVisibilityUi.escapeHtml(context) +
        "</div>";
    }
    html +=
      '<ul class="aiv-priority-list">' +
      visible
        .map(function (i) {
          var kind = priorityCategoryLabel(i);
          var rawBody = stripReviewFilterNoise(
            i.description || i.text || "",
            data
          );
          rawBody = cleanPriorityClientCopy(rawBody);
          var extracted = extractPriorityNumeric(rawBody);
          var body = extracted && extracted.cleaned ? extracted.cleaned : rawBody;
          var value = extracted ? extracted.value : null;
          // Prefer title as primary fact when description is empty / same as category
          if (!body && i.title && priorityCategoryLabel(i) !== String(i.title)) {
            body = cleanPriorityClientCopy(
              stripReviewFilterNoise(String(i.title), data)
            );
          }
          return (
            '<li class="aiv-priority-row" data-aiv-priority-kind="' +
            AiVisibilityUi.escapeHtml(kind) +
            '" data-brand="' +
            AiVisibilityUi.escapeHtml(i.brandId || "") +
            '" tabindex="0" role="button">' +
            '<div class="aiv-priority-row__main">' +
            '<div class="aiv-priority-kind">' +
            AiVisibilityUi.escapeHtml(kind) +
            "</div>" +
            (body
              ? '<div class="aiv-priority-body">' +
                AiVisibilityUi.escapeHtml(body) +
                "</div>"
              : "") +
            "</div>" +
            (value
              ? '<div class="aiv-priority-value">' +
                AiVisibilityUi.escapeHtml(value) +
                "</div>"
              : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul>";
    if (allItems.length > 4) {
      html +=
        '<button type="button" class="aiv-inline-link" data-aiv-goto="detail-review">View all review items →</button>';
    }
    el.innerHTML = html;
    el.querySelectorAll("[data-aiv-priority-kind]").forEach(function (row) {
      function go() {
        var kind = row.getAttribute("data-aiv-priority-kind") || "";
        var nav = detailTargetForPriorityKind(kind);
        gotoDetailSection({
          brandId: row.getAttribute("data-brand") || state.brandId,
          targetId: nav.targetId,
          watchlistMode: nav.watchlistMode || undefined,
        });
      }
      row.addEventListener("click", go);
      row.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          go();
        }
      });
    });
    el.querySelectorAll("[data-aiv-goto='detail-review']").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var firstBrand =
          (visible[0] && visible[0].brandId) ||
          (data.competitiveContext && data.competitiveContext.subjectBrandId) ||
          (data.currentPosition &&
            data.currentPosition.topBrandByAiPresence &&
            data.currentPosition.topBrandByAiPresence.brandId) ||
          state.brandId;
        gotoDetailSection({
          brandId: firstBrand,
          targetId: "aivDetailReviewSection",
        });
      });
    });
  }

  function domainTypeLabel(domain, panel) {
    if (!domain || !panel) return null;
    var key = String(domain).toLowerCase();
    var freq = Array.isArray(panel.DOMAIN_FREQUENCY) ? panel.DOMAIN_FREQUENCY : [];
    for (var i = 0; i < freq.length; i++) {
      if (String(freq[i].domain || "").toLowerCase() === key && freq[i].sourceType) {
        var st = String(freq[i].sourceType).toUpperCase();
        if (st === "OWNED") return "Owned";
        if (st === "THIRD_PARTY" || st === "EXTERNAL") return "External";
      }
    }
    var citedVs = panel.CITED_VS_ASSOCIATED || {};
    var cited = Array.isArray(citedVs.CITED_DOMAINS) ? citedVs.CITED_DOMAINS : [];
    var assoc = Array.isArray(citedVs.ASSOCIATED_DOMAINS)
      ? citedVs.ASSOCIATED_DOMAINS
      : [];
    var inCited = cited.some(function (d) {
      return String(d).toLowerCase() === key;
    });
    var inAssoc = assoc.some(function (d) {
      return String(d).toLowerCase() === key;
    });
    // Only label when the two lists actually differ (do not fabricate).
    if (cited.length && assoc.length) {
      var same =
        cited.length === assoc.length &&
        cited.every(function (d, idx) {
          return String(d).toLowerCase() === String(assoc[idx] || "").toLowerCase();
        });
      if (!same) {
        if (inCited && !inAssoc) return "Cited";
        if (inAssoc && !inCited) return "Associated";
      }
    }
    return null;
  }

  function normalizeSourceTag(label) {
    if (!label) return null;
    var raw = String(label).trim();
    var key = raw.toUpperCase().replace(/[\s-]+/g, "_");
    if (key === "OWNED" || key === "OWNED_SOURCE") return "OWNED";
    if (
      key === "THIRD_PARTY" ||
      key === "THIRDPARTY" ||
      key === "EXTERNAL" ||
      key === "NON_OWNED"
    ) {
      return "EXTERNAL";
    }
    return raw;
  }

  /** Display labels for source ownership / mix — Proper Case, not ALL CAPS. */
  function formatSourceTypeDisplay(typeKey) {
    var key = normalizeSourceTag(typeKey);
    if (key === "OWNED") return "Owned";
    if (key === "EXTERNAL") return "External";
    if (!typeKey) return "—";
    return String(typeKey)
      .replace(/_/g, " ")
      .toLowerCase()
      .replace(/\b\w/g, function (ch) {
        return ch.toUpperCase();
      });
  }

  function formatSourceMixDisplay(mixKey) {
    var key = String(mixKey || "")
      .trim()
      .toUpperCase()
      .replace(/[\s-]+/g, "_");
    if (key === "OWNED_ONLY" || key === "OWNED") return "Owned Only";
    if (key === "MIXED_SOURCES" || key === "MIXED") return "Mixed Sources";
    if (key === "EXTERNAL_ONLY" || key === "EXTERNAL") return "External Only";
    if (key === "NO_CITATIONS" || key === "NONE") return "No Citations";
    return formatSourceTypeDisplay(mixKey);
  }

  function renderOriginalEvidenceSummary(data) {
    var el = $("aivExecEvidence");
    var note = $("aivExecEvidenceNote");
    var viewAll = $("aivExecSourcesViewAll");
    var topEl = $("aivExecTopSources");
    if (!el) return;
    var summary = data.evidenceSummary || {};
    var panel = data.sourceExecutivePanel || {};
    var domains =
      (Array.isArray(panel.DOMAIN_FREQUENCY) && panel.DOMAIN_FREQUENCY.length
        ? panel.DOMAIN_FREQUENCY
        : null) ||
      summary.recurringDomains ||
      [];
    if (note) {
      note.hidden = true;
      note.textContent = "";
    }
    // Top Owned / Top External now live in Citation Intelligence 2×2 KPIs.
    if (topEl) topEl.innerHTML = "";

    var DEFAULT_VISIBLE_SOURCES = 8;

    if (panel.CITATION_SUPPORT === "NOT_SUPPORTED") {
      el.innerHTML =
        '<p class="aiv-evidence-quiet">Citation frequency is not supported for this provider cohort.</p>';
      if (viewAll) {
        viewAll.hidden = true;
        viewAll.onclick = null;
      }
      return;
    }

    if (!domains.length) {
      el.innerHTML =
        '<p class="aiv-evidence-quiet">' +
        AiVisibilityUi.escapeHtml(
          summary.message ||
            "No recurring domains available for monitored entitled brands in this geography yet."
        ) +
        "</p>";
      if (viewAll) {
        viewAll.hidden = true;
        viewAll.onclick = null;
      }
      return;
    }

    function formatFreqOneDecimal(row) {
      if (row && typeof row.SOURCE_CITATION_FREQUENCY === "number") {
        return (row.SOURCE_CITATION_FREQUENCY * 100).toFixed(1) + "%";
      }
      var raw =
        row && row.SOURCE_CITATION_FREQUENCY_DISPLAY
          ? String(row.SOURCE_CITATION_FREQUENCY_DISPLAY).trim()
          : "";
      if (!raw) return "—";
      var m = raw.match(/^(-?\d+(?:\.\d+)?)\s*%?$/);
      if (m) return (Number(m[1])).toFixed(1) + "%";
      return raw;
    }

    function freqPct(row) {
      return formatFreqOneDecimal(row);
    }

    function freqWidth(row) {
      if (row && typeof row.SOURCE_CITATION_FREQUENCY === "number") {
        return Math.max(0, Math.min(100, row.SOURCE_CITATION_FREQUENCY * 100));
      }
      return 0;
    }

    function responsesCitingLabel(row) {
      if (!row || typeof row === "string") return "—";
      if (row.responsesCitingDisplay) return String(row.responsesCitingDisplay);
      if (
        row.RESPONSES_CITING_SOURCE != null &&
        row.COMPARABLE_RESPONSES != null
      ) {
        return (
          String(row.RESPONSES_CITING_SOURCE) +
          " of " +
          String(row.COMPARABLE_RESPONSES)
        );
      }
      if (row.responsesAppearingIn != null) return String(row.responsesAppearingIn);
      return "—";
    }

    el.innerHTML =
      '<div class="deals-table-container aiv-recurring-sources-wrap" data-truncated="' +
      (domains.length > DEFAULT_VISIBLE_SOURCES ? "true" : "false") +
      '" data-visible-rows="' +
      String(DEFAULT_VISIBLE_SOURCES) +
      '">' +
      '<table class="deals-table aiv-recurring-sources-table" aria-label="Recurring source citation frequency">' +
      "<thead><tr>" +
      '<th><span class="aiv-th-label"><span class="aiv-th-text">Source</span></span></th>' +
      '<th><span class="aiv-th-label"><span class="aiv-th-text">Responses<br>Citing</span></span></th>' +
      '<th><span class="aiv-th-label"><span class="aiv-th-text">Frequency</span></span></th>' +
      "</tr></thead><tbody>" +
      domains
        .map(function (d) {
          var row = typeof d === "string" ? { domain: d } : d || {};
          var domain = row.domain || row.url || "";
          var href = sourceDomainHref(domain);
          var isOwned =
            String(row.SOURCE_TYPE || row.sourceType || "").toUpperCase() ===
            "OWNED";
          var nameInner = href
            ? '<a class="aiv-source-link project-name-text" href="' +
              AiVisibilityUi.escapeHtml(href) +
              '" target="_blank" rel="noopener noreferrer">' +
              AiVisibilityUi.escapeHtml(String(domain)) +
              "</a>"
            : '<span class="project-name-text">' +
              AiVisibilityUi.escapeHtml(String(domain || "—")) +
              "</span>";
          var you = isOwned
            ? ' <span class="aiv-you-tag">You</span>'
            : "";
          // Do not use base class aiv-source-row on <tr> — that class is display:grid for legacy lists.
          var rowClass = isOwned ? ' class="aiv-source-row--you"' : "";
          var pct = freqPct(row);
          var width = freqWidth(row);
          return (
            "<tr" +
            rowClass +
            ">" +
            '<td><span class="aiv-sources-freq-domain">' +
            nameInner +
            you +
            "</span></td>" +
            '<td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(responsesCitingLabel(row)) +
            "</td>" +
            '<td class="aiv-metric-cell">' +
            '<span class="aiv-sources-freq-bar">' +
            '<span class="aiv-sources-freq-track" aria-hidden="true"><span class="aiv-sources-freq-fill" style="width:' +
            width +
            '%"></span></span>' +
            '<span class="aiv-sources-freq-pct">' +
            AiVisibilityUi.escapeHtml(pct) +
            "</span></span></td>" +
            "</tr>"
          );
        })
        .join("") +
      "</tbody></table></div>";

    if (viewAll) {
      if (domains.length > DEFAULT_VISIBLE_SOURCES) {
        viewAll.hidden = false;
        viewAll.textContent = "View all sources →";
        viewAll.onclick = function () {
          var list = el.querySelector(".aiv-recurring-sources-wrap");
          if (!list) return;
          var truncated = list.getAttribute("data-truncated") === "true";
          list.setAttribute("data-truncated", truncated ? "false" : "true");
          viewAll.textContent = truncated
            ? "Show fewer sources"
            : "View all sources →";
        };
      } else {
        viewAll.hidden = true;
        viewAll.onclick = null;
      }
    }
  }

  function citationKpiCard(label, value, helper) {
    return (
      '<article class="aiv-kpi">' +
      "<h3>" +
      AiVisibilityUi.escapeHtml(label) +
      '</h3><div class="aiv-value">' +
      AiVisibilityUi.escapeHtml(value == null || value === "" ? "—" : String(value)) +
      '</div><div class="aiv-meta">' +
      AiVisibilityUi.escapeHtml(helper || "") +
      "</div></article>"
    );
  }

  function citationTopSourceKpiCard(label, sourceObj) {
    var domain = sourceObj && sourceObj.domain ? String(sourceObj.domain) : null;
    var citing =
      sourceObj &&
      (sourceObj.responsesCitingDisplay ||
        (sourceObj.RESPONSES_CITING_SOURCE != null &&
        sourceObj.COMPARABLE_RESPONSES != null
          ? String(sourceObj.RESPONSES_CITING_SOURCE) +
            " of " +
            String(sourceObj.COMPARABLE_RESPONSES)
          : null));
    var freq =
      sourceObj && typeof sourceObj.SOURCE_CITATION_FREQUENCY === "number"
        ? (sourceObj.SOURCE_CITATION_FREQUENCY * 100).toFixed(1) + "%"
        : sourceObj && sourceObj.SOURCE_CITATION_FREQUENCY_DISPLAY
          ? (function () {
              var raw = String(sourceObj.SOURCE_CITATION_FREQUENCY_DISPLAY).trim();
              var m = raw.match(/^(-?\d+(?:\.\d+)?)\s*%?$/);
              return m ? Number(m[1]).toFixed(1) + "%" : raw;
            })()
          : null;
    var meta = "Not observed";
    if (domain) {
      if (citing && freq) meta = citing + " · " + freq;
      else if (citing) meta = citing;
      else if (freq) meta = freq;
      else meta = "Cited in this cohort";
    }
    return (
      '<article class="aiv-kpi aiv-kpi--source">' +
      "<h3>" +
      AiVisibilityUi.escapeHtml(label) +
      '</h3><div class="aiv-value aiv-value--domain">' +
      AiVisibilityUi.escapeHtml(domain || "Not observed") +
      '</div><div class="aiv-meta">' +
      AiVisibilityUi.escapeHtml(meta) +
      "</div></article>"
    );
  }

  function buildCitationExecInsight(data, panel, cr, own, withCit, denom, ownedConfigured) {
    var allCited =
      withCit != null &&
      denom != null &&
      Number(withCit) === Number(denom) &&
      Number(denom) > 0;
    var ownedZero =
      ownedConfigured &&
      typeof own.value === "number" &&
      own.value === 0 &&
      own.denominator > 0;
    var providerLabel =
      (data && data.providerLabel) ||
      formatProviderDisplayName((data && data.provider) || state.provider) ||
      "this provider";

    if (allCited && ownedZero) {
      return (
        "All monitored " +
        providerLabel +
        " responses in this cohort included citations, but none matched the current governed owned-domain set."
      );
    }

    var rel = data.presenceCitationRelationship;
    if (rel && rel.statement) return String(rel.statement);
    return null;
  }

  function renderExecCitationSummary(data) {
    var card = $("aivExecCitationCard");
    var el = $("aivExecCitations");
    var note = $("aivExecCitationNote");
    var insightEl = $("aivExecCitationInsight");
    if (!el) return;
    if (card) card.hidden = true;

    var panel = data.sourceExecutivePanel;
    var ownedConfigured =
      panel &&
      (panel.OWNED_SOURCE_CITATION_RATE &&
        panel.OWNED_SOURCE_CITATION_RATE.OWNED_SOURCE_CLASSIFICATION_READY ===
          true);
    if (
      !ownedConfigured &&
      panel &&
      panel.ELIGIBLE_BRANDS_WITH_OWNED_DOMAIN > 0
    ) {
      ownedConfigured = true;
    }
    var ownedRate = panel && panel.OWNED_SOURCE_CITATION_RATE;
    if (
      ownedRate &&
      (ownedRate.OWNED_SOURCE_CLASSIFICATION_READY === false ||
        /not configured/i.test(String(ownedRate.display || "")))
    ) {
      ownedConfigured = false;
    }

    function hideNotes() {
      if (note) {
        note.hidden = true;
        note.textContent = "";
      }
      if (insightEl) {
        insightEl.hidden = true;
        insightEl.textContent = "";
      }
    }

    if (!panel || !panel.READY) {
      el.innerHTML =
        '<div class="aiv-citation-exec-empty">' +
        AiVisibilityUi.escapeHtml(
          "Citation metrics appear when comparable monitoring with citation capture is available for this geography."
        ) +
        "</div>";
      hideNotes();
      return;
    }

    var cr = panel.CITATION_RATE || {};
    var own = panel.OWNED_SOURCE_CITATION_RATE || {};
    var withCit =
      panel.RESPONSES_WITH_CITATIONS != null
        ? panel.RESPONSES_WITH_CITATIONS
        : cr.numerator;
    var denom =
      cr.denominator != null
        ? cr.denominator
        : own.denominator != null
          ? own.denominator
          : panel.COMPARABLE_RESPONSES != null
            ? panel.COMPARABLE_RESPONSES
            : null;

    var ownedDisplay = ownedConfigured
      ? own.display || (own.value === 0 ? "0%" : "—")
      : own.display || "Not configured";

    var coverageLine = "";
    if (withCit != null && denom != null) {
      if (typeof cr.value === "number" && cr.value < 1) {
        coverageLine =
          "Citation coverage: " +
          (cr.display || "—") +
          " · " +
          String(withCit) +
          " of " +
          String(denom) +
          " responses";
      } else {
        coverageLine =
          String(withCit) +
          " of " +
          String(denom) +
          " monitored responses included at least one citation.";
      }
    }

    var mix = panel.SOURCE_MIX || {};
    var mixReady = mix.READY === true && ownedConfigured;
    var mixSegments = [
      {
        key: "owned",
        label: "Owned Only",
        n: mix.OWNED_ONLY_N,
        rate: mix.OWNED_ONLY_RATE,
      },
      {
        key: "mixed",
        label: "Mixed Sources",
        n: mix.MIXED_SOURCES_N,
        rate: mix.MIXED_SOURCES_RATE,
      },
      {
        key: "external",
        label: "External Only",
        n: mix.EXTERNAL_ONLY_N,
        rate: mix.EXTERNAL_ONLY_RATE,
      },
      {
        key: "none",
        label: "No Citations",
        n: mix.NO_CITATIONS_N,
        rate: mix.NO_CITATIONS_RATE,
      },
    ];

    function mixPct(seg) {
      if (seg && seg.rate && typeof seg.rate.value === "number") {
        return Math.max(0, Math.min(100, seg.rate.value * 100));
      }
      return 0;
    }

    function mixDisplay(seg) {
      if (seg && seg.rate && seg.rate.display) return String(seg.rate.display);
      return "—";
    }

    var topOwnedObj = panel.TOP_OWNED_DOMAIN || null;
    var topExternalObj =
      panel.TOP_EXTERNAL_DOMAIN || panel.TOP_THIRD_PARTY_DOMAIN || null;
    var citationCoverageValue =
      withCit != null && denom != null
        ? String(withCit) + " of " + String(denom)
        : cr.display || "—";
    var citationCoverageHelper =
      typeof cr.value === "number" && cr.display
        ? cr.display + " of monitored responses included at least one citation."
        : coverageLine || "Responses with at least one citation.";

    var html =
      '<div class="aiv-citation-exec-top aiv-citation-exec-top--2x2">' +
      citationKpiCard(
        "Owned Source Coverage",
        ownedDisplay,
        ownedConfigured
          ? "Share of monitored responses citing at least one governed owned source."
          : "Requires governed official domains."
      ) +
      citationKpiCard(
        "Citation Coverage",
        citationCoverageValue,
        citationCoverageHelper
      ) +
      citationTopSourceKpiCard("Top Owned Source", topOwnedObj) +
      citationTopSourceKpiCard("Top External Source", topExternalObj) +
      "</div>";

    if (mixReady) {
      html +=
        '<div class="aiv-source-mix aiv-source-mix--compact">' +
        '<div class="aiv-source-mix__head">' +
        '<div class="aiv-source-mix__label">Source Mix</div>' +
        '<span class="info-tooltip aiv-col-info">' +
        '<span class="info-icon" role="button" tabindex="0" aria-label="About Source Mix">' +
        '<svg width="12" height="12" aria-hidden="true"><use href="#aiv-info-icon"></use></svg>' +
        "</span>" +
        '<div class="tooltip-content" hidden><strong>Source Mix</strong><br>' +
        formatInfoTooltipBody(
          (COLUMN_INFO.sourceMix && COLUMN_INFO.sourceMix.body) ||
            panel.SOURCE_MIX_DEFINITION ||
            ""
        ) +
        "</div></span></div>" +
        '<div class="aiv-source-mix__bar" role="img" aria-label="Source mix composition">';
      mixSegments.forEach(function (seg) {
        var w = mixPct(seg);
        if (w <= 0) return;
        html +=
          '<span class="aiv-source-mix__seg aiv-source-mix__seg--' +
          seg.key +
          '" style="width:' +
          w +
          '%" title="' +
          AiVisibilityUi.escapeHtml(seg.label + ": " + mixDisplay(seg)) +
          '"></span>';
      });
      html += "</div>" + '<div class="aiv-source-mix__legend aiv-source-mix__legend--pills">';
      mixSegments.forEach(function (seg) {
        html +=
          '<div class="aiv-source-mix__pill aiv-source-mix__pill--' +
          seg.key +
          '">' +
          '<span class="aiv-source-mix__pill-label">' +
          AiVisibilityUi.escapeHtml(seg.label) +
          "</span>" +
          '<span class="aiv-source-mix__pill-value">' +
          AiVisibilityUi.escapeHtml(mixDisplay(seg)) +
          "</span></div>";
      });
      html += "</div></div>";
    } else if (!ownedConfigured) {
      html +=
        '<p class="aiv-source-mix__unavailable">Source Mix appears when governed owned domains are configured.</p>';
    }

    el.innerHTML = html;
    hideNotes();

    var citationZero =
      (typeof cr.value === "number" && cr.value === 0 && denom > 0) ||
      (cr.display === "0%" && denom > 0);
    if (citationZero && note) {
      note.hidden = false;
      note.textContent =
        "No citations were observed in monitored answers for this geography. AI Presence can still be measured when brands appear without cited URLs.";
    }

    var mixInsight =
      panel.SOURCE_MIX_INTERPRETATION &&
      panel.SOURCE_MIX_INTERPRETATION.statement
        ? String(panel.SOURCE_MIX_INTERPRETATION.statement)
        : null;
    var insight =
      mixInsight ||
      buildCitationExecInsight(
        data,
        panel,
        cr,
        own,
        withCit,
        denom,
        ownedConfigured
      );
    if (insightEl && insight) {
      insightEl.hidden = false;
      insightEl.textContent = insight;
    }
  }

  function shouldShowCrossProviderSection(panel) {
    if (!panel) return false;
    if (panel.NOT_COMPARABLE === true) return false;
    var monitored = panel.PROVIDERS_MONITORED || [];
    if (!panel.READY) return false;
    return monitored.length > 1;
  }

  function shouldShowDiscoverabilitySection(payload) {
    if (!payload) return false;
    if (
      payload.DISCOVERABILITY === "SOURCE_NOT_CONFIGURED" ||
      payload.status === "CONNECTION_REQUIRED"
    ) {
      return true;
    }
    if (payload.DISCOVERABILITY === "CHECK_NOT_RUN" && !payload.LIVE_BASELINE) {
      return true;
    }
    if (payload.DISCOVERABILITY === "CHECK_FAILED") {
      return true;
    }
    if (
      payload.LIVE_BASELINE ||
      payload.OFFICIAL_SOURCES_CONFIGURED != null ||
      payload.DISCOVERABILITY
    ) {
      return true;
    }
    return false;
  }

  function hasDualLanguageComparison(lc) {
    if (!lc) return false;
    var en =
      lc.EN_AI_PRESENCE != null ? lc.EN_AI_PRESENCE : lc.EN_PRESENCE;
    var es =
      lc.ES_AI_PRESENCE != null ? lc.ES_AI_PRESENCE : lc.ES_PRESENCE;
    return typeof en === "number" && typeof es === "number";
  }

  function filterInsightsForExecDedupe(insightPayload, data) {
    // Keep material Executive Summary tiles (target 3–5). Only drop exact
    // duplicate competitive tiles when Priority Review repeats the same finding text.
    // Also strip any cross-region gap tiles when the selected geography is not Global
    // (defense in depth if an older payload slips through).
    var boxes =
      insightPayload && Array.isArray(insightPayload.boxes)
        ? insightPayload.boxes.slice()
        : [];
    if (!boxes.length) return { boxes: [] };
    var selectedGeo = String(
      (data && data.geography && (data.geography.key || data.geography.commercialRegion)) ||
        state.geography ||
        ""
    )
      .trim()
      .toLowerCase();
    var isGlobalSelected = selectedGeo === "global";
    if (!isGlobalSelected) {
      boxes = boxes.filter(function (box) {
        if (String(box.type || "") !== "WEAKEST_PRESENCE_AREA") return true;
        var title = String(box.title || "").toLowerCase();
        var blob = [
          box.title,
          box.finding,
          box.takeaway,
          box.soWhat,
          box.evidence,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        if (title.indexOf("regional gap") !== -1) return false;
        if (blob.indexOf("across regions") !== -1) return false;
        if (blob.indexOf("across global monitoring") !== -1) return false;
        // Cross-region phrasing like "Global trails Europe" while CALA is selected.
        if (
          /\b(global|europe|north america|asia pacific|mea)\b/.test(blob) &&
          selectedGeo &&
          blob.indexOf(selectedGeo) === -1
        ) {
          return false;
        }
        return true;
      });
    }
    var priItems =
      (data.priorityReviewItems && data.priorityReviewItems.items) || [];
    var priBlob = priItems
      .map(function (i) {
        return (
          String(i.title || "") +
          " " +
          String(i.description || "") +
          " " +
          String(i.text || "")
        );
      })
      .join(" ")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
    var filtered = boxes.filter(function (box) {
      if (String(box.type || "") !== "LARGEST_COMPETITIVE_GAP") return true;
      var finding = String(box.finding || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
      if (!finding || !priBlob) return true;
      // Exact/near-exact statement already shown in Priority Review — keep other tiles.
      return priBlob.indexOf(finding) === -1;
    });
    return Object.assign({}, insightPayload || {}, { boxes: filtered });
  }

  function renderIntegratedSecondarySections(data) {
    var panel = data.allProvidersPanel || data.crossProviderPresence;
    var discPayload =
      data.publicDiscoverability ||
      data.discoverabilityBusinessImpact ||
      data.openAiDiscoverability;
    var lc = data.languageComparison;

    var showProvider = shouldShowCrossProviderSection(panel);
    var showDisc = shouldShowDiscoverabilitySection(discPayload);
    var showLang = hasDualLanguageComparison(lc);

    var providerCard = $("aivExecProviderCard");
    var discCard = $("aivExecDiscoverabilityCard");
    var langCard = $("aivExecLanguageCard");
    var overview = $("aivExecIntelligenceOverview");
    var section = $("aivExecIntelligenceOverviewSection");

    if (providerCard) {
      providerCard.hidden = !showProvider;
      if (showProvider) {
        renderAllProvidersPanel(panel, "aivExecAllProviders");
      } else {
        var crossEl = $("aivExecAllProviders");
        if (crossEl) crossEl.innerHTML = "";
      }
    }

    if (discCard) {
      discCard.hidden = !showDisc;
      if (showDisc) {
        renderDiscoverabilityPlaceholder(
          $("aivExecDiscoverability"),
          discPayload,
          "executive"
        );
      } else {
        var discEl = $("aivExecDiscoverability");
        if (discEl) discEl.innerHTML = "";
      }
    }

    if (langCard) {
      langCard.hidden = !showLang;
      if (showLang) {
        renderLanguageComparison(lc);
      } else {
        var langEl = $("aivExecLanguage");
        if (langEl) langEl.innerHTML = "";
      }
    }

    var visibleCount =
      (showProvider ? 1 : 0) + (showDisc ? 1 : 0) + (showLang ? 1 : 0);
    if (overview) {
      overview.setAttribute("data-count", String(visibleCount));
    }
    if (section) {
      section.hidden = visibleCount === 0;
    }
  }

  function ensureDemoBrandPortfolioClientContext() {
    try {
      var ws =
        (window.localStorage &&
          window.localStorage.getItem("dealality_active_workspace")) ||
        "";
      var demo =
        (window.localStorage &&
          window.localStorage.getItem("dealality_demo_brand_portfolio")) ||
        "";
      demo = String(demo || "")
        .trim()
        .toLowerCase();
      var allowed =
        demo === "marriott" ||
        demo === "hilton" ||
        demo === "choice" ||
        demo === "ihg";
      // Brand-Side founder/demo QA: Marriott is the default governed showcase portfolio.
      if (ws === "Brand" && !allowed) {
        window.localStorage.setItem("dealality_demo_brand_portfolio", "marriott");
      }
    } catch (_) {}
  }

  function clearExecutiveDomForFreshPaint() {
    var ids = [
      "aivExecInsights",
      "aivExecPosition",
      "aivExecRegional",
      "aivExecStrengths",
      "aivExecGaps",
      "aivExecCompetitive",
      "aivExecPriority",
      "aivExecEvidence",
      "aivExecCitations",
      "aivExecTopSources",
      "aivExecAllProviders",
      "aivExecDiscoverability",
      "aivExecLanguage",
    ];
    ids.forEach(function (id) {
      var el = $(id);
      if (el) el.innerHTML = "";
    });
    [
      "aivExecCitationNote",
      "aivExecCitationInsight",
      "aivExecEvidenceNote",
      "aivExecSourcesViewAll",
    ].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.hidden = true;
      if (el.tagName === "BUTTON") {
        el.onclick = null;
        el.textContent = "View all sources →";
      } else {
        el.textContent = "";
      }
    });
    [
      "aivExecInsightSection",
      "aivExecIntelligenceOverviewSection",
    ].forEach(function (id) {
      var sec = $(id);
      if (sec) sec.hidden = true;
    });
    ["aivExecProviderCard", "aivExecDiscoverabilityCard", "aivExecLanguageCard"].forEach(
      function (id) {
        var card = $(id);
        if (card) card.hidden = true;
      }
    );
    var overview = $("aivExecIntelligenceOverview");
    if (overview) overview.setAttribute("data-count", "0");
    var notable = $("aivNotableMoves");
    if (notable) {
      notable.hidden = true;
      notable.innerHTML = "";
    }
    var mix = $("aivExecPromptMix");
    if (mix) {
      mix.hidden = true;
      mix.innerHTML = "";
    }
  }

  function renderExecutivePromptMix(summary) {
    var el = $("aivExecPromptMix");
    if (!el) return;
    var show = summary && summary.showPromptMix === true;
    if (!show) {
      el.hidden = true;
      el.innerHTML = "";
      return;
    }
    var label = (summary && summary.promptCoverageLabel) || "Prompt Intelligence";
    var compact = (summary && summary.promptCoverageCompact) || "Observed demand · Expert scenarios";
    var help =
      (summary && summary.EXECUTIVE_STORY) ||
      "We use both observed demand and expert scenario intelligence. Observed demand reflects externally measured query themes. Scenario intelligence tests commercially important owner and developer decisions that may not appear as literal high-volume searches.";
    el.hidden = false;
    el.innerHTML =
      '<span class="aiv-prompt-mix-label">' +
      AiVisibilityUi.escapeHtml(label) +
      "</span>" +
      '<span class="aiv-prompt-mix-compact">' +
      AiVisibilityUi.escapeHtml(compact) +
      "</span>" +
      '<span class="aiv-prompt-mix-help">' +
      AiVisibilityUi.escapeHtml(help) +
      "</span>";
  }

  function promptOriginBadgeHtml(row) {
    if (!row || row.showOriginBadge === false) return "";
    var origin = row.promptOrigin || row.PROMPT_ORIGIN;
    if (origin === "LEGACY_UNCLASSIFIED" || !origin) return "";
    var badge = row.originBadge || null;
    if (!badge) {
      if (origin === "OBSERVED") badge = "Observed";
      else if (origin === "DERIVED") badge = "Derived";
      else if (origin === "SCENARIO") badge = "Scenario";
    }
    if (!badge) return "";
    var cls = "aiv-origin-badge";
    if (origin === "OBSERVED") cls += " aiv-origin-badge--observed";
    var title = row.originDetail || "";
    if (origin === "SCENARIO" && !title) title = "Expert scenario";
    if (origin === "DERIVED" && !title) title = "Derived from observed demand";
    if (origin === "OBSERVED" && !title) {
      title = "Demand tier is relative to comparable observed queries within the source country and language cohort.";
    }
    return (
      '<span class="' +
      cls +
      '"' +
      (title
        ? ' title="' + AiVisibilityUi.escapeHtml(title) + '"'
        : "") +
      ">" +
      AiVisibilityUi.escapeHtml(badge) +
      "</span>"
    );
  }

  function renderExecutive(data) {
    setActiveTab("executive");
    clearExecutiveDomForFreshPaint();
    state.executive = data;
    applyMonitoringFreshness(data.monitoringFreshness || null);
    state.execLanguageComparison = data.languageComparison || null;
    var portfolioBrands =
      (data.portfolioOverview && data.portfolioOverview.brands) || [];
    var entitledCount = portfolioBrands.length;
    var cp = data.currentPosition || {};
    var pap = cp.portfolioAiPresence || {};
    var bestPos = cp.bestCompetitivePosition || cp.topBrandByCompetitivePosition || {};

    // 1. Executive Summary tiles — P0E intelligence primary; legacy insights fallback
    var p0eInsights =
      data.executiveIntelligenceInsights &&
      Array.isArray(data.executiveIntelligenceInsights.boxes) &&
      data.executiveIntelligenceInsights.boxes.length
        ? data.executiveIntelligenceInsights
        : null;
    var insightFiltered = filterInsightsForExecDedupe(
      p0eInsights || data.executiveInsights,
      data
    );
    state._execInsightFilterFp = [
      state.geography,
      state.provider,
      state.language || "",
    ].join("|");
    renderInsightBoxes(
      "aivExecInsights",
      "aivExecInsightSection",
      insightFiltered
    );

    // 2. Portfolio Snapshot — equal KPI tiles (unchanged)
    var cards = [
      [
        pap.label || "Portfolio AI Presence",
        pap.display || "—",
        pap.helper ||
          (String(state.provider || "").toLowerCase() === "all"
            ? "Mean of each linked brand’s cross-provider average Presence — not a single combined AI run."
            : "Share of monitored owner questions where at least one of your linked brands appeared."),
      ],
      [
        "Brands Monitored",
        (cp.brandsMonitored && cp.brandsMonitored.display) || "—",
        "How many of your linked brands are included in monitoring here.",
      ],
      [
        "Strongest Brand",
        (cp.topBrandByAiPresence &&
          (cp.topBrandByAiPresence.brandName || cp.topBrandByAiPresence.brandId)) ||
          "—",
        "Entitled brand with the highest Observed Presence in this geography.",
      ],
      [
        "Best Competitive Position",
        formatBestCompetitivePosition(bestPos),
        "Your brand with the best peer rank in this geography.",
      ],
      [
        "Questions Missing",
        (cp.questionsMissing && cp.questionsMissing.display) || "—",
        (cp.questionsMissing && cp.questionsMissing.helper) ||
          (String(state.provider || "").toLowerCase() === "all"
            ? "Comparable owner questions where none of your linked brands appeared on any monitored provider."
            : "Questions where none of your brands appeared in the answer."),
      ],
    ];
    $("aivExecPosition").innerHTML = cards
      .map(function (c) {
        return (
          '<article class="aiv-kpi"><h3>' +
          AiVisibilityUi.escapeHtml(c[0]) +
          '</h3><div class="aiv-value">' +
          AiVisibilityUi.escapeHtml(c[1]) +
          '</div><div class="aiv-meta">' +
          AiVisibilityUi.escapeHtml(c[2]) +
          "</div></article>"
        );
      })
      .join("");

    renderExecutivePromptMix(data.promptOriginSummary || null);

    var geos = data.geographySummary || [];
    var regionalBody = $("aivExecRegional");
    if (regionalBody) {
      if (!geos.length) {
        regionalBody.innerHTML =
          '<tr><td colspan="6"><div class="aiv-empty">No geography summary available.</div></td></tr>';
      } else {
        regionalBody.innerHTML = geos
          .map(function (g) {
            var notMonitored =
              g.availability === "not_monitored" || g.brandsMonitored === 0;
            var monitored = notMonitored
              ? "Not Monitored"
              : g.displayMonitored || "—";
            var leadName = g.topBrandByAiPresence
              ? g.topBrandByAiPresence.brandName ||
                g.topBrandByAiPresence.brandId ||
                "—"
              : "—";
            var leadPresence =
              g.topBrandByAiPresence && g.topBrandByAiPresence.display
                ? g.topBrandByAiPresence.display
                : "—";
            var rankName = g.bestCompetitivePosition
              ? g.bestCompetitivePosition.brandName ||
                g.bestCompetitivePosition.brandId ||
                "—"
              : "—";
            var rankDisplay = "—";
            if (
              g.bestCompetitivePosition &&
              typeof g.bestCompetitivePosition.rank === "number" &&
              g.bestCompetitivePosition.rank > 0
            ) {
              rankDisplay =
                g.bestCompetitivePosition.peerCount != null
                  ? "#" +
                    g.bestCompetitivePosition.rank +
                    " of " +
                    g.bestCompetitivePosition.peerCount
                  : g.bestCompetitivePosition.display ||
                    "#" + g.bestCompetitivePosition.rank;
            }
            return (
              "<tr>" +
              '<td><span class="project-name-text">' +
              AiVisibilityUi.escapeHtml(g.geography || "—") +
              "</span></td>" +
              '<td class="aiv-metric-cell">' +
              AiVisibilityUi.escapeHtml(monitored) +
              "</td>" +
              '<td class="aiv-metric-cell">' +
              AiVisibilityUi.escapeHtml(notMonitored ? "—" : leadName) +
              "</td>" +
              '<td class="aiv-metric-cell">' +
              AiVisibilityUi.escapeHtml(notMonitored ? "Not Monitored" : leadPresence) +
              "</td>" +
              '<td class="aiv-metric-cell">' +
              AiVisibilityUi.escapeHtml(rankName) +
              "</td>" +
              '<td class="aiv-metric-cell">' +
              AiVisibilityUi.escapeHtml(rankDisplay) +
              "</td>" +
              "</tr>"
            );
          })
          .join("");
      }
    }

    renderMarketMovementChart(data.marketMovement || null);

    var changes = (data.whatChanged && data.whatChanged.items) || [];
    var notable =
      (data.whatChanged && data.whatChanged.notableItems) ||
      changes.filter(function (c) {
        return c.absoluteDelta !== 0;
      });
    var notableEl = $("aivNotableMoves");
    if (notableEl) {
      if (notable.length) {
        notableEl.hidden = false;
        notableEl.innerHTML =
          '<div class="aiv-notable-moves-inner">' +
          '<span class="aiv-notable-label">Notable moves</span>' +
          '<ul class="aiv-notable-list">' +
          notable
            .map(function (c) {
              var label =
                (c.brandName || c.brandId || "") +
                " " +
                (c.compactDisplay ||
                  (typeof c.absoluteDeltaPp === "number"
                    ? (c.absoluteDeltaPp > 0 ? "+" : "−") +
                      Math.abs(c.absoluteDeltaPp) +
                      " pp"
                    : ""));
              return "<li>" + AiVisibilityUi.escapeHtml(label) + "</li>";
            })
            .join("") +
          "</ul></div>";
      } else {
        notableEl.hidden = true;
        notableEl.innerHTML = "";
      }
    }

    $("aivExecStrengths").innerHTML = kvRows(data.strengths || [], {
      emptyMessage: "No monitored strengths for your brands in this geography yet.",
      fallbackLabel: "Strength",
    });
    renderExecGapsMerged(data);

    renderPortfolioTable(portfolioBrands);
    renderOriginalCompetitiveContext(data);
    renderPriorityReviewCompact(data, entitledCount);

    renderOriginalEvidenceSummary(data);
    renderExecCitationSummary(data);

    if (entitledCount > 0) {
      renderIntegratedSecondarySections(data);
    } else {
      ["aivExecIntelligenceOverviewSection"].forEach(function (id) {
        var sec = $(id);
        if (sec) sec.hidden = true;
      });
      ["aivExecProviderCard", "aivExecDiscoverabilityCard", "aivExecLanguageCard"].forEach(
        function (id) {
          var card = $(id);
          if (card) card.hidden = true;
        }
      );
    }

    wireExecutiveDetailDeepLinks(data);
    setBanner(null);
  }

  function wireExecutiveDetailDeepLinks(data) {
    var subjectBrand =
      (data.competitiveContext && data.competitiveContext.subjectBrandId) ||
      (data.currentPosition &&
        data.currentPosition.topBrandByAiPresence &&
        data.currentPosition.topBrandByAiPresence.brandId) ||
      state.brandId;

    function bindCard(cardId, targetId, extra) {
      var card = $(cardId);
      if (!card || card.hidden) return;
      card.style.cursor = "pointer";
      card.setAttribute("tabindex", "0");
      card.setAttribute("role", "link");
      function go() {
        gotoDetailSection(
          Object.assign(
            { brandId: subjectBrand, targetId: targetId },
            extra || {}
          )
        );
      }
      card.onclick = go;
      card.onkeydown = function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          go();
        }
      };
    }

    bindCard("aivExecProviderCard", "aivDetailCoverageSection");
    bindCard("aivExecDiscoverabilityCard", "aivDetailDiscoverabilitySection");
    bindCard("aivExecLanguageCard", "aivDetailLanguageSection");

    var citationEl = $("aivExecCitations");
    if (citationEl) {
      citationEl.style.cursor = "pointer";
      citationEl.onclick = function () {
        gotoDetailSection({
          brandId: subjectBrand,
          targetId: "aivDetailCitationSection",
        });
      };
    }

    var trendCard = $("aivMarketTrendChart") || $("aivMarketTrendEmpty");
    var trendSection = trendCard && trendCard.closest(".aiv-theme-card");
    if (trendSection) {
      var trendFooter =
        trendSection.querySelector(".aiv-chart-meta") || trendSection;
      var trendLink = trendSection.querySelector("[data-aiv-goto='detail-trend']");
      if (!trendLink) {
        trendLink = document.createElement("button");
        trendLink.type = "button";
        trendLink.className = "aiv-inline-link aiv-chart-trend-link";
        trendLink.setAttribute("data-aiv-goto", "detail-trend");
        trendLink.textContent = "Open brand trends →";
        trendFooter.appendChild(trendLink);
      } else {
        trendLink.classList.add("aiv-chart-trend-link");
        if (trendLink.parentElement !== trendFooter) {
          trendFooter.appendChild(trendLink);
        }
      }
      trendLink.onclick = function () {
        gotoDetailSection({
          brandId: subjectBrand,
          targetId: "aivDetailTrendSection",
        });
      };
    }

    var missingKpi = null;
    var pos = $("aivExecPosition");
    if (pos) {
      pos.querySelectorAll(".aiv-kpi").forEach(function (kpi) {
        var h = kpi.querySelector("h3");
        if (h && /questions missing/i.test(h.textContent || "")) missingKpi = kpi;
      });
    }
    if (missingKpi) {
      missingKpi.style.cursor = "pointer";
      missingKpi.onclick = function () {
        gotoDetailSection({
          brandId: subjectBrand,
          targetId: "aivDetailWatchlistSection",
          watchlistMode: "missing",
        });
      };
    }
  }

  function sourceDomainHref(domain) {
    var d = String(domain || "")
      .trim()
      .replace(/^['"]+|['"]+$/g, "");
    if (!d) return null;
    if (/^https?:\/\//i.test(d)) {
      try {
        var u = new URL(d);
        if (u.protocol !== "http:" && u.protocol !== "https:") return null;
        return u.href;
      } catch (_) {
        return null;
      }
    }
    // Domain-only citations from monitoring — open as https
    if (!/^[a-z0-9.-]+\.[a-z]{2,}([/:?#].*)?$/i.test(d)) return null;
    if (/[<>\s]/.test(d)) return null;
    return "https://" + d.replace(/^\/+/, "");
  }

  function destroyMarketTrendChart() {
    if (marketTrendChart) {
      try {
        marketTrendChart.destroy();
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[aiv] market trend chart destroy failed", err);
        }
      }
      marketTrendChart = null;
    }
  }

  function toPresencePct(v) {
    if (typeof v !== "number" || !isFinite(v)) return null;
    return v <= 1 ? Math.round(v * 1000) / 10 : Math.round(v * 10) / 10;
  }

  function renderMarketMovementChart(mm) {
    var valueEl = $("aivMarketTrendValue");
    var metaEl = $("aivMarketTrendMeta");
    var emptyEl = $("aivMarketTrendEmpty");
    var legendEl = $("aivMarketTrendLegend");
    var canvas = $("aivMarketTrendChart");
    destroyMarketTrendChart();

    if (valueEl) {
      valueEl.textContent =
        mm && mm.headlineValue != null ? String(mm.headlineValue) : "—";
    }
    if (metaEl) {
      var metaParts = [];
      if (mm && mm.dateRangeLabel) metaParts.push(mm.dateRangeLabel);
      if (mm && mm.note) metaParts.push(mm.note);
      if (mm && mm.brandsOmittedFromChart > 0) {
        metaParts.push(
          "Showing top " +
            mm.brandSeriesCount +
            " of " +
            mm.brandsWithPoints +
            " monitored brands by latest AI Presence."
        );
      }
      metaEl.textContent = metaParts.join(" · ");
    }
    if (legendEl) {
      legendEl.hidden = true;
      legendEl.innerHTML = "";
    }

    var ready =
      mm &&
      mm.chartReady &&
      mm.labels &&
      mm.labels.length >= 2 &&
      mm.series &&
      mm.series.length;
    if (!ready) {
      if (canvas) canvas.style.display = "none";
      if (emptyEl) {
        emptyEl.hidden = false;
        var baseEmpty =
          (mm && mm.emptyMessage) ||
          "Trend will develop as additional monitoring periods are completed.";
        // Chart is geography-scoped; Global / NA often have only one period.
        var geoHint =
          state.geography && state.geography !== "CALA"
            ? " The chart follows the Geography filter — switch to CALA (or another region with more than one completed run) to see presence over time."
            : "";
        emptyEl.textContent = baseEmpty + geoHint;
      }
      return;
    }

    if (!canvas || typeof window.Chart !== "function") {
      if (emptyEl) {
        emptyEl.hidden = false;
        emptyEl.textContent =
          typeof window.Chart !== "function"
            ? "Chart library unavailable — refresh the page to load trend visuals."
            : (mm.emptyMessage || "Trend chart unavailable.");
      }
      if (canvas) canvas.style.display = "none";
      return;
    }

    if (emptyEl) {
      emptyEl.hidden = true;
      emptyEl.textContent = "";
    }
    canvas.style.display = "block";

    var datasets = mm.series.map(function (s, idx) {
      var colors = MARKET_TREND_COLORS[idx % MARKET_TREND_COLORS.length];
      return {
        label: s.brandName || s.brandId,
        data: (s.data || []).map(toPresencePct),
        borderColor: colors.border,
        // Solid swatch for LOI-style tooltip color boxes (no area fill under lines)
        backgroundColor: colors.border,
        fill: false,
        tension: 0.4,
        borderWidth: 2,
        pointRadius: 4,
        pointHoverRadius: 6,
        pointBackgroundColor: colors.border,
        pointBorderColor: "#fff",
        pointBorderWidth: 2,
        pointHoverBackgroundColor: colors.border,
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 2,
        spanGaps: false,
      };
    });

    if (legendEl) {
      legendEl.hidden = false;
      legendEl.innerHTML = datasets
        .map(function (ds) {
          return (
            '<span class="aiv-chart-legend-item"><span class="aiv-chart-legend-swatch" style="background:' +
            AiVisibilityUi.escapeHtml(ds.borderColor) +
            '"></span>' +
            AiVisibilityUi.escapeHtml(ds.label) +
            "</span>"
          );
        })
        .join("");
    }

    marketTrendChart = new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: mm.labels,
        datasets: datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: {
          padding: { top: 18, right: 8, bottom: 2, left: 2 },
        },
        // LOI Market Hub Deal Volume chart interaction + tooltip styling
        interaction: {
          mode: "index",
          intersect: false,
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: true,
            backgroundColor: "rgba(255, 255, 255, 0.95)",
            titleColor: "#080f25",
            bodyColor: "#37446b",
            borderColor: "#d9e1fa",
            borderWidth: 1,
            padding: 12,
            cornerRadius: 6,
            displayColors: true,
            boxPadding: 4,
            callbacks: {
              title: function (items) {
                if (!items || !items.length) return "";
                return String(items[0].label || "");
              },
              label: function (context) {
                var y = context.parsed && context.parsed.y;
                var name = context.dataset.label || "";
                if (y == null || !isFinite(y)) {
                  return " " + name + ": —";
                }
                return " " + name + ": " + y + "%";
              },
              filter: function (item) {
                var y = item.parsed && item.parsed.y;
                return y != null && isFinite(y);
              },
            },
          },
        },
        scales: {
          x: {
            grid: { display: false, drawBorder: false },
            ticks: { color: "#7e89ac", font: { size: 11 } },
          },
          y: {
            beginAtZero: true,
            max: 100,
            grid: {
              color: "rgba(126, 137, 172, 0.1)",
              drawBorder: false,
            },
            ticks: {
              color: "#7e89ac",
              font: { size: 11 },
              padding: 6,
              callback: function (value) {
                return value + "%";
              },
            },
          },
        },
      },
    });
  }

  function formatMonitoringStamp(raw) {
    if (!raw) return "—";
    var s = String(raw);
    // Prefer readable date/time without forcing fake local labels
    if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
      return s.slice(0, 10) + " · " + s.slice(11, 16) + " UTC";
    }
    return s.slice(0, 19);
  }

  function metricNumeric(metric, preferRank) {
    if (!metric) return null;
    if (preferRank && typeof metric.rank === "number" && isFinite(metric.rank)) {
      return metric.rank;
    }
    if (typeof metric.value === "number" && isFinite(metric.value)) return metric.value;
    if (typeof metric.absoluteDeltaPp === "number" && isFinite(metric.absoluteDeltaPp)) {
      return metric.absoluteDeltaPp;
    }
    if (typeof metric.absoluteDelta === "number" && isFinite(metric.absoluteDelta)) {
      return metric.absoluteDelta;
    }
    return null;
  }

  function portfolioSortValue(brand, key) {
    if (!brand) return null;
    switch (key) {
      case "brandName":
        return String(brand.brandName || brand.brandId || "").toLowerCase();
      case "aiPresence":
        return metricNumeric(brand.aiPresence, false);
      case "aiPresenceChange":
        return metricNumeric(brand.aiPresenceChange, false);
      case "competitivePosition":
        return metricNumeric(brand.competitivePosition, true);
      case "questionsMissing":
        return metricNumeric(brand.questionsMissing, false);
      case "topDecisionTerritory":
        return territoryDisplay(brand.topDecisionTerritory);
      case "latestMonitoring":
        return brand.latestMonitoring ? String(brand.latestMonitoring) : null;
      default:
        return null;
    }
  }

  function comparePortfolioValues(aVal, bVal, dir) {
    var aNull = aVal == null || aVal === "";
    var bNull = bVal == null || bVal === "";
    if (aNull && bNull) return 0;
    if (aNull) return 1;
    if (bNull) return -1;
    var cmp = 0;
    if (typeof aVal === "number" && typeof bVal === "number") {
      cmp = aVal - bVal;
    } else {
      cmp = String(aVal).localeCompare(String(bVal), undefined, {
        numeric: true,
        sensitivity: "base",
      });
    }
    return dir === "desc" ? -cmp : cmp;
  }

  function sortedPortfolioBrands(brands) {
    var rows = (brands || []).slice();
    if (!state.portfolioSortKey) return rows;
    var key = state.portfolioSortKey;
    var dir = state.portfolioSortDir || "asc";
    rows.sort(function (a, b) {
      return comparePortfolioValues(
        portfolioSortValue(a, key),
        portfolioSortValue(b, key),
        dir
      );
    });
    return rows;
  }

  function updatePortfolioSortHeaders() {
    var table = $("aivPortfolioTable");
    if (!table) return;
    table.querySelectorAll("th[data-sort]").forEach(function (th) {
      th.classList.remove("sort-asc", "sort-desc");
      if (state.portfolioSortKey && th.getAttribute("data-sort") === state.portfolioSortKey) {
        th.classList.add(state.portfolioSortDir === "desc" ? "sort-desc" : "sort-asc");
      }
    });
  }

  function bindPortfolioSort() {
    if (state.portfolioSortBound) return;
    var table = $("aivPortfolioTable");
    if (!table) return;
    state.portfolioSortBound = true;
    table.querySelectorAll("th[data-sort]").forEach(function (th) {
      th.addEventListener("click", function (e) {
        if (e.target.closest && e.target.closest(".info-tooltip, .info-icon, .tooltip-content")) {
          return;
        }
        e.preventDefault();
        e.stopPropagation();
        var key = th.getAttribute("data-sort");
        if (!key) return;
        if (state.portfolioSortKey === key) {
          state.portfolioSortDir = state.portfolioSortDir === "asc" ? "desc" : "asc";
        } else {
          state.portfolioSortKey = key;
          state.portfolioSortDir = "asc";
        }
        updatePortfolioSortHeaders();
        renderPortfolioTable(state.portfolioBrands);
      });
    });
  }

  function closePortfolioColumnInfo() {
    var container = $("aivTooltipContainer");
    if (container) container.innerHTML = "";
  }

  function openPortfolioColumnInfo(tooltipContent) {
    var container = $("aivTooltipContainer");
    if (!container || !tooltipContent) return;
    container.innerHTML = "";
    var cloned = tooltipContent.cloneNode(true);
    cloned.classList.add("aiv-tooltip-panel");
    cloned.style.display = "block";
    cloned.style.visibility = "visible";
    cloned.style.opacity = "1";
    cloned.style.width = "";
    cloned.style.maxWidth = "";
    cloned.removeAttribute("hidden");
    var closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "tooltip-close-btn";
    closeBtn.setAttribute("aria-label", "Close");
    closeBtn.innerHTML = "×";
    closeBtn.addEventListener("click", function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      closePortfolioColumnInfo();
    });
    cloned.appendChild(closeBtn);
    container.appendChild(cloned);
  }

  function bindPortfolioColumnInfo() {
    if (state.portfolioInfoBound) return;
    var container = $("aivTooltipContainer");
    if (!container) return;
    state.portfolioInfoBound = true;

    function openFromEvent(e) {
      var icon = e.target.closest && e.target.closest(".aiv-page .aiv-col-info .info-icon");
      if (!icon) return;
      e.preventDefault();
      e.stopPropagation();
      var tip = icon.closest(".info-tooltip");
      var content = tip && tip.querySelector(".tooltip-content");
      openPortfolioColumnInfo(content);
    }

    document.addEventListener("click", function (e) {
      if (e.target.closest && e.target.closest(".aiv-page .aiv-col-info .info-icon")) {
        openFromEvent(e);
        return;
      }
      if (
        e.target.closest &&
        !e.target.closest(".aiv-col-info") &&
        !e.target.closest("#aivTooltipContainer .tooltip-content")
      ) {
        closePortfolioColumnInfo();
      }
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        closePortfolioColumnInfo();
        return;
      }
      if (
        (e.key === "Enter" || e.key === " ") &&
        e.target.closest &&
        e.target.closest(".aiv-page .aiv-col-info .info-icon")
      ) {
        openFromEvent(e);
      }
    });
    container.addEventListener("click", function (e) {
      if (e.target === container) closePortfolioColumnInfo();
    });
  }

  function renderPortfolioTable(brands) {
    var table = $("aivPortfolioTable");
    var tbody = table.querySelector("tbody");
    var results = $("aivPortfolioResults");
    var countEl = $("aivPortfolioCount");
    state.portfolioBrands = Array.isArray(brands) ? brands.slice() : [];
    bindPortfolioSort();
    bindPortfolioColumnInfo();
    updatePortfolioSortHeaders();
    var rows = sortedPortfolioBrands(state.portfolioBrands);
    if (!rows.length) {
      if (results) results.hidden = true;
      tbody.innerHTML =
        '<tr><td colspan="6"><div class="aiv-empty">No entitled brands for this company yet. Link Brand Basics records on Company Profile to enable Brand AI Visibility.</div></td></tr>';
      setBanner(null);
      return;
    }
    setBanner(null);
    if (results) results.hidden = false;
    if (countEl) {
      countEl.innerHTML =
        "Showing <strong>" +
        rows.length +
        "</strong> of <strong>" +
        rows.length +
        "</strong> brands";
    }
    tbody.innerHTML = rows
      .map(function (b) {
        return (
          '<tr class="aiv-portfolio-row" data-brand="' +
          AiVisibilityUi.escapeHtml(b.brandId) +
          '" tabindex="0" role="button">' +
          '<td><span class="project-name-text">' +
          AiVisibilityUi.escapeHtml(b.brandName || b.brandId) +
          "</span></td>" +
          '<td class="aiv-metric-cell">' +
          AiVisibilityUi.formatMetricCell(b.aiPresence) +
          "</td>" +
          '<td class="aiv-metric-cell aiv-delta-cell">' +
          AiVisibilityUi.formatDeltaCell(b.aiPresenceChange) +
          "</td>" +
          '<td class="aiv-metric-cell">' +
          AiVisibilityUi.formatMetricCell(b.competitivePosition) +
          "</td>" +
          '<td class="aiv-metric-cell">' +
          AiVisibilityUi.formatMetricCell(b.questionsMissing) +
          "</td>" +
          '<td class="aiv-metric-cell aiv-col-secondary">' +
          AiVisibilityUi.escapeHtml(territoryDisplay(b.topDecisionTerritory)) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
    tbody.querySelectorAll("[data-brand]").forEach(function (row) {
      function go() {
        drillToDetail(row.getAttribute("data-brand"));
      }
      row.addEventListener("click", go);
      row.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          go();
        }
      });
    });
  }

  function drillToDetail(brandId) {
    if (!brandId) return;
    state.brandId = brandId;
    persistBrand(brandId);
    fillBrandSelect((state.portfolio && state.portfolio.brands) || []);
    var sel = $("aivBrand");
    if (sel) sel.value = brandId;
    gotoDetailSection({
      brandId: brandId,
      targetId: "aivDetailToplineSection",
    });
  }

  function buildDetailInsightsFromOverview(overview) {
    var boxes = [];
    var k = (overview && overview.kpis) || {};
    var presence = k.aiPresence;
    var missing = k.questionsMissing;
    var rank = k.competitivePosition;
    if (presence && presence.availability === "observed" && presence.display) {
      boxes.push({
        type: "STRONGEST_PRESENCE_AREA",
        finding: "Brand appears in monitored responses for this geography.",
        evidence: "Observed AI Presence " + presence.display + ".",
        soWhat: "Presence is the certified Brand AI Visibility v1 signal.",
        whatToWatch: "Comparable Presence change on the next monitoring period.",
      });
    }
    if (missing && missing.availability === "observed" && missing.value > 0) {
      boxes.push({
        type: "QUESTIONS_MISSING_PATTERN",
        finding: "Brand was not observed in some monitored owner questions.",
        evidence: missing.display || String(missing.value) + " questions missing.",
        soWhat: "Absence is an observed result — not automatically a strategic problem.",
        whatToWatch: "Group by prompt family, provider, region, and language.",
      });
    }
    if (rank && rank.availability === "observed" && rank.display) {
      boxes.push({
        type: "LARGEST_COMPETITIVE_GAP",
        finding: "Competitive Position is based on AI Presence Rate rank.",
        evidence: "Current rank " + rank.display + " in the comparable peer cohort.",
        soWhat: "No arbitrary weighting — Presence rank only.",
        whatToWatch: "Peer Presence rates on the next comparable window.",
      });
    }
    return { boxes: boxes, IMPLEMENTED: true };
  }

  function renderKpis(overview) {
    var k = overview.kpis || {};
    var presence = k.aiPresence || {};
    var delta = presence.delta || null;
    var presenceDeltaHtml = "";
    if (delta) {
      presenceDeltaHtml =
        '<div class="aiv-meta aiv-meta-delta" title="AI Presence change vs prior comparable monitoring run">' +
        AiVisibilityUi.formatDeltaCell(delta) +
        ' <span class="aiv-meta-delta-label">AI Presence vs prior</span>' +
        "</div>";
    }
    var cards = [
      {
        title: "AI Presence",
        metric: presence,
        desc: "Share of monitored questions where this brand appeared.",
        extra: presenceDeltaHtml,
      },
      {
        title: "Competitive Position",
        metric: k.competitivePosition,
        desc: "This brand’s rank by AI Presence among peers.",
        extra: "",
      },
      {
        title: "Questions Missing",
        metric: k.questionsMissing,
        desc: "Share and count of questions where this brand did not appear.",
        extra: "",
      },
    ];
    var row = $("aivKpiRow");
    if (!row) return;
    row.innerHTML = cards
      .map(function (c) {
        return (
          '<article class="aiv-kpi"><h3>' +
          AiVisibilityUi.escapeHtml(c.title) +
          '</h3><div class="aiv-value">' +
          AiVisibilityUi.formatMetricCell(c.metric) +
          "</div>" +
          (c.extra || "") +
          (c.desc
            ? '<div class="aiv-meta aiv-meta-desc">' +
              AiVisibilityUi.escapeHtml(c.desc) +
              "</div>"
            : "") +
          "</article>"
        );
      })
      .join("");
  }

  function renderRegional(overview) {
    var tbody = $("aivRegional");
    if (!tbody) return;
    var rows = overview.regionalPosition || [];
    if (!rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="3"><div class="aiv-empty">No regional position data for this brand yet.</div></td></tr>';
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        return (
          "<tr><td><span class=\"project-name-text\">" +
          AiVisibilityUi.escapeHtml(r.geography) +
          "</span></td><td class=\"aiv-metric-cell\">" +
          AiVisibilityUi.formatMetricCell(r.aiPresence) +
          "</td><td class=\"aiv-metric-cell\">" +
          AiVisibilityUi.formatMetricCell(r.competitivePosition) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function syncQuestionFilterTabs() {
    var tabs = $("aivQuestionTabs");
    if (!tabs) return;
    tabs.querySelectorAll("button[data-filter]").forEach(function (b) {
      b.classList.toggle(
        "is-active",
        b.getAttribute("data-filter") === state.questionFilter
      );
    });
  }

  function questionMatchesFilter(q, filter) {
    var status = String((q && (q.presenceLabel || q.brandStatus)) || "");
    var f = String(filter || "all").toLowerCase();
    var present =
      q && q.presenceObserved === true
        ? true
        : status === "Present" ||
          status === "Mentioned" ||
          status === "Recommended" ||
          status === "First Recommended";
    if (f === "present" || f === "won") return present;
    if (f === "missing") return status === "Missing" || q.presenceObserved === false;
    return true;
  }

  function renderQuestions(data) {
    var table = $("aivQuestionsTable");
    if (!table) return;
    var tbody = table.querySelector("tbody");
    if (data && Array.isArray(data.questions)) {
      state.detailQuestions = data.questions;
    }
    if (data && data.pagination) {
      state.questionsPagination = data.pagination;
    }
    syncQuestionFilterTabs();
    var allRows = state.detailQuestions || [];
    var rows = allRows.filter(function (q) {
      return questionMatchesFilter(q, state.questionFilter);
    });
    var paginationEl = $("aivQuestionsPagination");
    if (paginationEl && state.questionsPagination) {
      var pg = state.questionsPagination;
      paginationEl.hidden = !(pg.total > pg.limit);
      if (!paginationEl.hidden) {
        paginationEl.innerHTML =
          '<span class="aiv-pagination-meta">Showing ' +
          AiVisibilityUi.escapeHtml(String(rows.length)) +
          " of " +
          AiVisibilityUi.escapeHtml(String(pg.total)) +
          " questions" +
          (pg.hasMore ? " · more available via pagination" : "") +
          "</span>";
      }
    }
    if (!rows.length) {
      var emptyMsg;
      if (state.questionFilter === "present" || state.questionFilter === "won") {
        emptyMsg =
          "No Presence observed yet — this brand was not observed in monitored owner questions for this geography.";
      } else if (state.questionFilter === "missing") {
        emptyMsg =
          allRows.length > 0
            ? "No Missing questions in the current result set."
            : "No monitored questions for this geography yet.";
      } else if (state.intent) {
        emptyMsg =
          "No monitored questions for this geography with Intent “" +
          state.intent +
          "”. Try All intents or another Intent Territory.";
      } else {
        emptyMsg = "No monitored questions for this geography yet.";
      }
      tbody.innerHTML =
        '<tr><td colspan="6"><div class="aiv-empty">' +
        AiVisibilityUi.escapeHtml(emptyMsg) +
        "</div></td></tr>";
      return;
    }
    tbody.innerHTML = rows
      .map(function (q) {
        var presenceLabel = q.presenceLabel || q.brandStatus || "Missing";
        var stabilityTitle =
          q.observationSummary && q.observationSummary.presenceLabel
            ? [
                q.observationSummary.presenceLabel,
                q.observationSummary.recurrence || "",
                q.observationSummary.firstObserved
                  ? "First " + String(q.observationSummary.firstObserved).slice(0, 10)
                  : "",
                q.observationSummary.lastObserved
                  ? "Last " + String(q.observationSummary.lastObserved).slice(0, 10)
                  : "",
              ]
                .filter(Boolean)
                .join(" · ")
            : "";
        var ownerIntent =
          q.ownerIntent || q.intentLabel || q.intentTerritory || "Owner Decision Scenario";
        var decisionContext = q.decisionContext || "";
        return (
          "<tr><td><span class=\"aiv-wl-intent\">" +
          AiVisibilityUi.escapeHtml(ownerIntent) +
          "</span>" +
          (decisionContext
            ? '<div class="aiv-wl-decision-context">' +
              AiVisibilityUi.escapeHtml(decisionContext) +
              "</div>"
            : "") +
          promptOriginBadgeHtml(q) +
          "</td><td class=\"aiv-metric-cell\"" +
          (stabilityTitle
            ? ' title="' + AiVisibilityUi.escapeHtml(stabilityTitle) + '"'
            : "") +
          ">" +
          AiVisibilityUi.statusBadge(presenceLabel) +
          '</td><td class="aiv-metric-cell">' +
          AiVisibilityUi.escapeHtml(q.provider || "—") +
          '</td><td class="aiv-metric-cell">' +
          AiVisibilityUi.escapeHtml((q.topCompetitor && q.topCompetitor.entityName) || "—") +
          "</td><td class=\"aiv-metric-cell\">" +
          AiVisibilityUi.escapeHtml(q.intentTerritory || "—") +
          '</td><td class="aiv-metric-cell"><button type="button" class="aiv-btn-text aiv-link" data-evidence="' +
          AiVisibilityUi.escapeHtml(q.evidenceId || "") +
          '"' +
          (q.responseId
            ? ' data-response-id="' + AiVisibilityUi.escapeHtml(q.responseId) + '"'
            : "") +
          ">View Evidence</button></td></tr>"
        );
      })
      .join("");
    tbody.querySelectorAll("[data-evidence]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openEvidence(btn.getAttribute("data-evidence"));
      });
    });
  }

  function formatPeerRate(value) {
    if (typeof value !== "number" || !Number.isFinite(value)) return "—";
    if (value <= 1) return (Math.round(value * 1000) / 10).toFixed(1) + "%";
    return Number(value).toFixed(1) + "%";
  }

  var COLUMN_INFO = {
    brandName: {
      title: "Brand",
      body:
        "What it is: A hotel brand linked to your company in this portfolio.\n\nHow to read: Each row is one brand. Click a row to open Detailed View for that brand.\n\nWhy it matters: This is your starting point for comparing Presence, gaps, and competitive position brand by brand.",
    },
    aiPresence: {
      title: "AI Presence",
      body:
        "What it is: How often this brand appeared in AI answers to the monitored owner questions.\n\nHow to read: Higher % means the brand showed up in more monitored answers. 0% means it was monitored but not observed. “—” or Not Monitored means there is no comparable data yet — not a zero score.\n\nWhy it matters: Presence is the core Brand AI Visibility signal. It shows whether owners asking AI about development and positioning are seeing your brand at all.",
    },
    aiPresenceChange: {
      title: "Δ vs prior run",
      body:
        "What it is: Change in AI Presence versus the previous comparable monitoring run.\n\nHow to read: Shown in percentage points (pp). Positive means Presence improved; negative means it declined. If periods are not comparable, you will see Not Comparable or Insufficient History instead of a number.\n\nWhy it matters: It tells you whether visibility is moving — not just where you stand today.",
    },
    competitivePosition: {
      title: "Competitive Position",
      body:
        "What it is: This brand’s rank by AI Presence among the fixed peer set in the selected geography.\n\nHow to read: #1 means the highest Presence among peers. A higher number means more peers outrank this brand. Rank is based only on Observed Presence — not recommendations or a composite score.\n\nWhy it matters: It shows whether you are leading, keeping pace, or trailing peers on the questions owners ask AI.",
    },
    questionsMissing: {
      title: "Questions Missing",
      body:
        "What it is: Owner-decision observations where this brand did not appear in the AI answer.\n\nHow to read: Shown as % (count). Higher % or count means more monitored owner-decision scenarios went unanswered for your brand. Use Detailed View → Questions Missing Watchlist to see Owner Intent and Decision Context.\n\nWhy it matters: Missing observations are concrete gaps — places where owners asked AI and your brand was absent.",
    },
    topDecisionTerritory: {
      title: "Top Decision Territory",
      body:
        "What it is: The owner-intent question type (prompt family) where this brand appeared most often.\n\nHow to read: Shown only when the brand had Observed Presence (>0%) in at least one prompt family. If the brand was absent from every monitored question, this shows — (not a inferred territory). It is not a weighted score or priority ranking across families.\n\nWhy it matters: It shows which owner decision themes already surface your brand — a useful contrast to families where you are missing.",
    },
    citationFrequency: {
      title: "Citation Frequency",
      body:
        "What it is: How often this domain was cited in successful monitored AI responses in the current cohort.\n\nHow to read: A higher % means the domain appeared as a citation in more responses. Compare rows to see which sources show up most often. Frequency can differ by provider, question, geography, language, and run.\n\nWhy it matters: It shows which websites AI answers are pointing to when they cite sources. It does not measure influence, authority, or ranking power — only observed citation frequency.",
    },
    sourceMix: {
      title: "Source Mix",
      body:
        "What it is: How monitored responses break down by citation pattern: owned-only, mixed owned and external, external-only, or no citations.\n\nHow to read: Each segment is a share of comparable responses. Owned Only means only official domains were cited. External Only means only third-party domains. Mixed means both. No Citations means the answer did not cite a source.\n\nWhy it matters: It shows whether AI answers lean on your official sites, outside sites, both, or none — so you can see citation patterns at a glance without treating mix as a quality score.",
    },
  };

  function formatInfoTooltipBody(body) {
    return AiVisibilityUi.escapeHtml(String(body || "")).replace(/\n/g, "<br>");
  }

  function columnInfoHtml(key) {
    var info = COLUMN_INFO[key];
    if (!info) return "";
    return (
      '<span class="info-tooltip aiv-col-info">' +
      '<span class="info-icon" role="button" tabindex="0" aria-label="About ' +
      AiVisibilityUi.escapeHtml(info.title) +
      '"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span>' +
      '<div class="tooltip-content" hidden><strong>' +
      AiVisibilityUi.escapeHtml(info.title) +
      "</strong><br>" +
      formatInfoTooltipBody(info.body) +
      "</div></span>"
    );
  }

  function sortHeaderHtml(key, labelHtml) {
    return (
      '<th data-sort="' +
      AiVisibilityUi.escapeHtml(key) +
      '"><span class="aiv-th-label"><span class="aiv-th-text">' +
      labelHtml +
      '</span><span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>' +
      columnInfoHtml(key) +
      "</span></th>"
    );
  }

  function peerSortValue(peer, key) {
    if (!peer) return null;
    switch (key) {
      case "brandName":
        return String(peer.entityName || peer.entityId || "").toLowerCase();
      case "aiPresence":
        return typeof peer.aiPresenceRate === "number" ? peer.aiPresenceRate : null;
      case "aiPresenceChange":
        if (peer.aiPresenceChange && typeof peer.aiPresenceChange.value === "number") {
          return peer.aiPresenceChange.value;
        }
        if (peer.aiPresenceChange && typeof peer.aiPresenceChange.absolute === "number") {
          return peer.aiPresenceChange.absolute;
        }
        if (peer.aiPresenceChange && typeof peer.aiPresenceChange.absoluteDeltaPp === "number") {
          return peer.aiPresenceChange.absoluteDeltaPp;
        }
        return typeof peer.aiPresenceChange === "number" ? peer.aiPresenceChange : null;
      case "competitivePosition":
        return typeof peer.competitivePosition === "number" ? peer.competitivePosition : null;
      default:
        return null;
    }
  }

  function sortedPeerRows(rows) {
    var list = (rows || []).slice();
    if (!state.peerSortKey) return list;
    var key = state.peerSortKey;
    var dir = state.peerSortDir || "asc";
    list.sort(function (a, b) {
      return comparePortfolioValues(peerSortValue(a, key), peerSortValue(b, key), dir);
    });
    return list;
  }

  function updatePeerSortHeaders(table) {
    if (!table) return;
    table.querySelectorAll("th[data-sort]").forEach(function (th) {
      th.classList.remove("sort-asc", "sort-desc");
      if (state.peerSortKey && th.getAttribute("data-sort") === state.peerSortKey) {
        th.classList.add(state.peerSortDir === "desc" ? "sort-desc" : "sort-asc");
      }
    });
  }

  function bindPeerSort(table) {
    if (!table) return;
    table.querySelectorAll("th[data-sort]").forEach(function (th) {
      th.addEventListener("click", function (e) {
        if (e.target.closest && e.target.closest(".info-tooltip, .info-icon, .tooltip-content")) {
          return;
        }
        e.preventDefault();
        e.stopPropagation();
        var key = th.getAttribute("data-sort");
        if (!key) return;
        if (state.peerSortKey === key) {
          state.peerSortDir = state.peerSortDir === "asc" ? "desc" : "asc";
        } else {
          state.peerSortKey = key;
          state.peerSortDir = "asc";
        }
        renderCompetitors({ competitors: state.peerRows });
      });
    });
  }

  function formatPresenceIndexCopy(indexValue, relativeGapPct) {
    if (typeof indexValue !== "number") return "—";
    var gap = Math.round(Math.abs(relativeGapPct || 0));
    if (indexValue > 100) return String(indexValue) + " · " + gap + "% above benchmark";
    if (indexValue < 100) return String(indexValue) + " · " + gap + "% below benchmark";
    return String(indexValue) + " · at parity";
  }

  function formatOwnerIntentPresence(value) {
    if (typeof value !== "number" || !Number.isFinite(value)) return "";
    return formatPeerRate(value);
  }

  function formatOwnerIntentIndex(row) {
    if (typeof row.indexValue === "number") {
      return String(row.indexValue);
    }
    return "Benchmark still developing";
  }

  function formatOwnerIntentPosition(row) {
    if (typeof row.indexValue !== "number" || typeof row.relativeGapPct !== "number") {
      return "";
    }
    var gap = Math.round(Math.abs(row.relativeGapPct || 0));
    if (row.indexValue > 100) return gap + "% above benchmark";
    if (row.indexValue < 100) return gap + "% below benchmark";
    return "At competitive parity";
  }

  function renderOwnerIntentSummaryTable(rows) {
    var body = rows
      .map(function (row) {
        var certified = typeof row.indexValue === "number";
        var rowClass = certified ? " aiv-owner-intent-summary-row--certified" : "";
        var indexCell = formatOwnerIntentIndex(row);
        var positionCell = formatOwnerIntentPosition(row);
        return (
          "<tr class=\"aiv-owner-intent-summary-row" +
          rowClass +
          '">' +
          "<td>" +
          AiVisibilityUi.escapeHtml(row.intentLabel || "Owner intent") +
          "</td>" +
          '<td class="aiv-metric-cell">' +
          AiVisibilityUi.escapeHtml(formatOwnerIntentPresence(row.subjectPresence)) +
          "</td>" +
          '<td class="aiv-metric-cell">' +
          (certified
            ? AiVisibilityUi.escapeHtml(indexCell)
            : '<span class="aiv-owner-intent-developing">' +
              AiVisibilityUi.escapeHtml(indexCell) +
              disclosureInfoIconHtml("BENCHMARK_STILL_DEVELOPING", "Benchmark still developing") +
              "</span>") +
          "</td>" +
          '<td class="aiv-delta-cell">' +
          (positionCell ? AiVisibilityUi.escapeHtml(positionCell) : "") +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="deals-table-container aiv-portfolio-table-wrap aiv-owner-intent-summary-wrap">' +
      '<table class="deals-table aiv-portfolio-table aiv-owner-intent-summary-table">' +
      "<thead><tr>" +
      '<th class="no-sort">Owner intent</th>' +
      '<th class="no-sort">Your Presence</th>' +
      '<th class="no-sort">AI Presence Index</th>' +
      '<th class="no-sort">Position</th>' +
      "</tr></thead>" +
      "<tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function renderOwnerIntentPeerContext(row) {
    if (!row) return "";
    var coreChips = (row.selectedCorePeers || [])
      .map(function (name) {
        return (
          '<span class="aiv-peer-chip">' + AiVisibilityUi.escapeHtml(name) + "</span>"
        );
      })
      .join("");
    var observedChips = (row.selectedObservedCompetitors || [])
      .map(function (name) {
        return (
          '<span class="aiv-peer-chip aiv-peer-chip--observed">' +
          AiVisibilityUi.escapeHtml(name) +
          "</span>"
        );
      })
      .join("");
    var coreInfo =
      "Brands considered direct commercial alternatives for this specific owner decision. Dealality uses governed commercial characteristics to determine the relevant comparison group.";
    var observedInfo =
      "Brands that actually appear as alternatives or peers across relevant Dealality AI observations. They may differ from a traditional competitive set.";
    if (!coreChips && !observedChips) return "";
    return (
      '<div class="aiv-owner-intent-context">' +
      (coreChips
        ? '<div class="aiv-owner-intent-context__line"><span class="aiv-owner-intent-label">Core peers</span>' +
          coreChips +
          '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About Core Peers"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span>' +
          '<div class="tooltip-content" hidden><strong>Core Peers</strong><br>' +
          AiVisibilityUi.escapeHtml(coreInfo) +
          "</div></span></div>"
        : "") +
      (observedChips
        ? '<div class="aiv-owner-intent-context__line"><span class="aiv-owner-intent-label aiv-owner-intent-label--observed">Observed competitors</span>' +
          observedChips +
          '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About Observed Competitors"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span>' +
          '<div class="tooltip-content" hidden><strong>Observed Competitors</strong><br>' +
          AiVisibilityUi.escapeHtml(observedInfo) +
          "</div></span></div>"
        : "") +
      "</div>"
    );
  }

  function renderOwnerIntentBenchmarks(data, peerTableHtml) {
    var rows = data.ownerIntentBenchmarks || [];
    if (!rows.length) return peerTableHtml || "";
    var indexInfo =
      "Measures how often your brand appears in a specific owner-decision context relative to directly comparable brands measured in the selected AI provider scope. 100 represents competitive parity.";
    var primaryRow =
      rows.filter(function (row) {
        return typeof row.indexValue === "number";
      })[0] || rows[0];
    var html =
      '<div class="aiv-owner-intent-block">' +
      '<div class="aiv-owner-intent-head">' +
      "<h3 class=\"aiv-theme-label\">AI Presence by Owner Intent</h3>" +
      '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About AI Presence Index"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span>' +
      '<div class="tooltip-content" hidden><strong>AI Presence Index</strong><br>' +
      AiVisibilityUi.escapeHtml(indexInfo) +
      "</div></span></div>" +
      '<div class="aiv-owner-intent-columns">' +
      '<div class="aiv-owner-intent-col aiv-owner-intent-col--summary">' +
      renderOwnerIntentSummaryTable(rows) +
      renderOwnerIntentPeerContext(primaryRow) +
      "</div>" +
      '<div class="aiv-owner-intent-col aiv-owner-intent-col--peers">' +
      (peerTableHtml || "") +
      "</div>" +
      "</div></div>";
    return html;
  }

  function renderCompetitors(data) {
    data = mergeCompetitorsRenderData(data || {});
    var el = $("aivCompetitors");
    var gapNote = $("aivDetailPeerGapNote");
    if (!el) return;
    if (data && Array.isArray(data.competitors)) {
      state.peerRows = data.competitors;
      data.competitors.forEach(function (c) {
        if (c && c.entityId && c.entityName) {
          state.brandNamesById[c.entityId] = c.entityName;
        }
      });
    }
    var rows = (state.peerRows || []).slice().sort(function (a, b) {
      var ra =
        typeof a.competitivePosition === "number"
          ? a.competitivePosition
          : 9999;
      var rb =
        typeof b.competitivePosition === "number"
          ? b.competitivePosition
          : 9999;
      if (ra !== rb) return ra - rb;
      var pa = typeof a.aiPresenceRate === "number" ? a.aiPresenceRate : -1;
      var pb = typeof b.aiPresenceRate === "number" ? b.aiPresenceRate : -1;
      return pb - pa;
    });
    if (!rows.length) {
      el.innerHTML =
        '<div class="aiv-empty aiv-empty--compact">Not Monitored — no competitive context for this geography yet.</div>';
      if (gapNote) {
        gapNote.hidden = true;
        gapNote.textContent = "";
      }
      return;
    }
    var subject = rows.filter(function (c) {
      return c.isSubject;
    })[0];
    var subjectRate =
      subject && typeof subject.aiPresenceRate === "number"
        ? subject.aiPresenceRate
        : null;
    // Top 9 leaders; if the filtered subject brand is outside that window, append it
    // (e.g. ranks 1–9 + subject at #33). If already in top 9, show top 10.
    var PEER_LEADERBOARD_TOP = 9;
    var PEER_LEADERBOARD_MAX = 10;
    var topLeaders = rows.slice(0, PEER_LEADERBOARD_TOP);
    var subjectInTopLeaders =
      subject &&
      topLeaders.some(function (r) {
        return (
          r.isSubject ||
          (subject.entityId && r.entityId === subject.entityId)
        );
      });
    var displayRows =
      subject && !subjectInTopLeaders
        ? topLeaders.concat([subject])
        : rows.slice(0, PEER_LEADERBOARD_MAX);
    var subjectOutsideTop = !!(subject && !subjectInTopLeaders);

    function peerTableRow(c) {
      var name = c.entityName || c.entityId || "Peer";
      var isYou = !!c.isSubject;
      var rank =
        c.competitivePosition != null ? "#" + c.competitivePosition : "—";
      var rate = formatPeerRate(c.aiPresenceRate);
      var gapCell = "—";
      if (
        !isYou &&
        subjectRate != null &&
        typeof c.aiPresenceRate === "number"
      ) {
        var gapPp = Math.round((c.aiPresenceRate - subjectRate) * 1000) / 10;
        gapCell = (gapPp > 0 ? "+" : "") + gapPp + " pp";
      } else if (isYou) {
        gapCell = "—";
      }
      var rowClass = [
        isYou ? "aiv-peer-row--you" : "",
        isYou && subjectOutsideTop ? "aiv-peer-row--outside-top" : "",
      ]
        .filter(Boolean)
        .join(" ");
      return (
        '<tr class="' +
        rowClass +
        '">' +
        '<td class="aiv-metric-cell">' +
        AiVisibilityUi.escapeHtml(rank) +
        "</td>" +
        "<td><span class=\"project-name-text\">" +
        AiVisibilityUi.escapeHtml(name) +
        "</span>" +
        (isYou ? ' <span class="aiv-you-tag">You</span>' : "") +
        "</td>" +
        '<td class="aiv-metric-cell">' +
        AiVisibilityUi.escapeHtml(rate) +
        "</td>" +
        '<td class="aiv-delta-cell">' +
        AiVisibilityUi.escapeHtml(gapCell) +
        "</td>" +
        "</tr>"
      );
    }

    var peerTableHtml =
      '<div class="deals-table-container aiv-portfolio-table-wrap aiv-peers-table-wrap">' +
      '<table class="deals-table aiv-portfolio-table aiv-peers-table">' +
      "<thead><tr>" +
      '<th class="no-sort">Rank</th>' +
      '<th class="no-sort">Brand</th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">AI<br>Presence</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">vs<br>You</span></span></th>' +
      "</tr></thead>" +
      "<tbody>" +
      displayRows.map(peerTableRow).join("") +
      "</tbody></table></div>";

    el.innerHTML =
      peerTableHtml +
      (subjectOutsideTop && subject && subject.competitivePosition != null
        ? '<p class="aiv-theme-block-help help-text aiv-peers-outside-note">Showing top ' +
          PEER_LEADERBOARD_TOP +
          " peers plus your brand at #" +
          AiVisibilityUi.escapeHtml(String(subject.competitivePosition)) +
          ".</p>"
        : "");

    var gapN =
      state.peerGapCount ||
      (data &&
        data.peerPresentSubjectMissing &&
        data.peerPresentSubjectMissing.PEER_PRESENT_SUBJECT_MISSING_N) ||
      0;
    if (gapNote) {
      if (gapN > 0) {
        gapNote.hidden = false;
        gapNote.innerHTML =
          AiVisibilityUi.escapeHtml(String(gapN)) +
          " peer-present / subject-missing question" +
          (gapN === 1 ? "" : "s") +
          '. <button type="button" class="aiv-btn-text aiv-link" data-aiv-goto="detail-peer-gap">Open watchlist</button>';
        gapNote.querySelectorAll("[data-aiv-goto]").forEach(function (btn) {
          btn.addEventListener("click", function () {
            gotoDetailSection({
              brandId: state.brandId,
              targetId: "aivDetailWatchlistSection",
              watchlistMode: "peer_gaps",
            });
          });
        });
      } else {
        gapNote.hidden = true;
        gapNote.textContent = "";
      }
    }
  }

  function renderDetailCitationTopline(overview, sourcesData) {
    var el = $("aivDetailCitationTopline");
    if (!el) return;
    var s = (overview && overview.secondary) || {};
    var csi = (sourcesData && sourcesData.citedSourceIntelligence) || {};
    var rows = csi.topCitedSources || sourcesData.sources || [];
    var topOwned = null;
    var topExternal = null;
    rows.forEach(function (row) {
      var st = String(row.SOURCE_TYPE || row.sourceType || "").toUpperCase();
      if (st === "OWNED" && !topOwned) topOwned = row;
      if ((st === "THIRD_PARTY" || st === "EXTERNAL") && !topExternal) {
        topExternal = row;
      }
    });
    var ownedCount = 0;
    var externalCount = 0;
    rows.forEach(function (row) {
      var st = String(row.SOURCE_TYPE || row.sourceType || "").toUpperCase();
      if (st === "OWNED") ownedCount += 1;
      else externalCount += 1;
    });
    var mixLabel = "NO_CITATIONS";
    if (rows.length) {
      if (ownedCount && externalCount) mixLabel = "MIXED";
      else if (ownedCount) mixLabel = "OWNED_ONLY";
      else mixLabel = "EXTERNAL_ONLY";
    }
    el.innerHTML =
      citationKpiCard(
        "Citation Coverage",
        (s.citationRate && s.citationRate.display) || "—",
        "Share of comparable responses with at least one citation."
      ) +
      citationKpiCard(
        "Owned Source Coverage",
        (s.ownedSourceCitationRate && s.ownedSourceCitationRate.display) || "—",
        "Share citing a governed official domain."
      ) +
      citationKpiCard(
        "Source Mix",
        formatSourceMixDisplay(mixLabel),
        "Cited sources in this cohort."
      ) +
      citationTopSourceKpiCard("Top Owned Source", topOwned) +
      citationTopSourceKpiCard("Top External Source", topExternal);
  }

  function renderSources(data) {
    var el = $("aivSources");
    var noteEl = $("aivDetailSourcesNote");
    if (!el) return;

    renderDetailCitationTopline(state.overview, data);

    var csi = (data && data.citedSourceIntelligence) || null;
    var rows =
      (csi && csi.topCitedSources && csi.topCitedSources.length
        ? csi.topCitedSources
        : null) ||
      data.sources ||
      [];
    if (!rows.length) {
      if (noteEl) {
        noteEl.textContent =
          "No provider citations stored for this geography yet.";
      }
      el.innerHTML = "";
      return;
    }
    if (noteEl) {
      noteEl.textContent =
        "Cited = explicitly returned in the observed answer. Associated = broader response/search evidence. Frequency is not influence or authority.";
    }
    el.innerHTML =
      '<div class="deals-table-container aiv-portfolio-table-wrap aiv-detail-sources-wrap">' +
      '<table class="deals-table aiv-portfolio-table aiv-detail-sources-table" aria-label="Cited Sources">' +
      "<thead><tr>" +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Source</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Type</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Responses<br>Citing</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Frequency</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Occurrences</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Prompt<br>Families</span></span></th>' +
      '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">Providers /<br>Markets</span></span></th>' +
      "</tr></thead><tbody>" +
      rows
        .slice(0, 20)
        .map(function (s, idx) {
          var domain = String((s && s.domain) || "").trim();
          var href = sourceDomainHref(domain);
          var typeRaw = String((s && (s.SOURCE_TYPE || s.sourceType)) || "").toUpperCase();
          var typeKey =
            typeRaw === "OWNED"
              ? "OWNED"
              : typeRaw === "THIRD_PARTY" || typeRaw === "EXTERNAL"
                ? "EXTERNAL"
                : typeRaw || "";
          var typeLabel = typeKey ? formatSourceTypeDisplay(typeKey) : "—";
          var isOwned = typeKey === "OWNED";
          var you = isOwned ? ' <span class="aiv-you-tag">You</span>' : "";
          var rowClass = isOwned ? ' class="aiv-source-row--you"' : "";
          var nameCell = href
            ? '<a class="aiv-source-link project-name-text" href="' +
              AiVisibilityUi.escapeHtml(href) +
              '" target="_blank" rel="noopener noreferrer">' +
              AiVisibilityUi.escapeHtml(domain || "—") +
              "</a>"
            : '<span class="project-name-text">' +
              AiVisibilityUi.escapeHtml(domain || "—") +
              "</span>";
          var responses =
            s.RESPONSES_CITING_SOURCE != null
              ? s.RESPONSES_CITING_SOURCE
              : s.responsesAppearingIn != null
                ? s.responsesAppearingIn
                : s.occurrenceCount;
          var denom =
            s.COMPARABLE_RESPONSES != null ? s.COMPARABLE_RESPONSES : null;
          var citingLabel =
            responses != null && denom != null
              ? String(responses) + " of " + String(denom)
              : String(responses ?? "—");
          var freq =
            typeof s.SOURCE_CITATION_FREQUENCY === "number"
              ? (s.SOURCE_CITATION_FREQUENCY * 100).toFixed(1) + "%"
              : s.SOURCE_CITATION_FREQUENCY_DISPLAY
                ? (function () {
                    var raw = String(s.SOURCE_CITATION_FREQUENCY_DISPLAY).trim();
                    var m = raw.match(/^(-?\d+(?:\.\d+)?)\s*%?$/);
                    return m ? Number(m[1]).toFixed(1) + "%" : raw;
                  })()
                : "—";
          var refs =
            s.CITATION_OCCURRENCES != null
              ? s.CITATION_OCCURRENCES
              : s.citationCount != null
                ? s.citationCount
                : s.occurrenceCount;
          var decisions = Array.isArray(s.PROMPT_FAMILIES_CITING_SOURCE)
            ? s.PROMPT_FAMILIES_CITING_SOURCE.join(", ")
            : Array.isArray(s.ownerDecisions)
              ? s.ownerDecisions.join(", ")
              : "—";
          var markets = Array.isArray(s.markets)
            ? s.markets.join(", ")
            : Array.isArray(s.GEOGRAPHIES_CITING_SOURCE)
              ? s.GEOGRAPHIES_CITING_SOURCE.join(", ")
              : "—";
          var providers = Array.isArray(s.providers)
            ? s.providers.join(", ")
            : Array.isArray(s.PROVIDERS_CITING_SOURCE)
              ? s.PROVIDERS_CITING_SOURCE.join(", ")
              : "";
          var marketCell = [providers, markets].filter(Boolean).join(" · ") || "—";
          var evidenceId = s.evidenceId || s.sampleEvidenceId || "";
          return (
            "<tr" +
            rowClass +
            ' data-source-idx="' +
            idx +
            '"><td><span class="aiv-sources-freq-domain">' +
            nameCell +
            you +
            (evidenceId
              ? ' <button type="button" class="aiv-btn-text aiv-link" data-evidence="' +
                AiVisibilityUi.escapeHtml(evidenceId) +
                '">Evidence</button>'
              : "") +
            "</span></td><td class=\"aiv-metric-cell\">" +
            AiVisibilityUi.escapeHtml(typeLabel) +
            '</td><td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(citingLabel) +
            '</td><td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(String(freq)) +
            '</td><td class="aiv-metric-cell">' +
            AiVisibilityUi.escapeHtml(String(refs ?? "—")) +
            "</td><td>" +
            AiVisibilityUi.escapeHtml(decisions || "—") +
            "</td><td>" +
            AiVisibilityUi.escapeHtml(marketCell) +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table></div>";
    el.querySelectorAll("[data-evidence]").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        openEvidence(btn.getAttribute("data-evidence"));
      });
    });
  }

  function destroyDetailTrendChart() {
    if (detailTrendChart) {
      try {
        detailTrendChart.destroy();
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[aiv] detail trend chart destroy failed", err);
        }
      }
      detailTrendChart = null;
    }
  }

  function renderDetailTrendChart(trend) {
    var emptyEl = $("aivDetailTrendEmpty");
    var helpEl = $("aivTrendHelp");
    var canvas = $("aivDetailTrendChart");
    var summaryEl = $("aivDetailTrendSummary");
    var chartWrap = $("aivDetailTrendChartWrap");
    destroyDetailTrendChart();

    var points = (trend && trend.points) || [];
    var labels = points.map(function (p) {
      var s = String(p.date || "");
      if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10) + " · " + s.slice(11, 16);
      return s.slice(0, 10) || "—";
    });
    var data = points.map(function (p) {
      return toPresencePct(p.value);
    });
    var ready = labels.length >= 2;

    if (!ready) {
      if (chartWrap) chartWrap.hidden = true;
      if (canvas) canvas.style.display = "none";
      if (summaryEl) {
        summaryEl.hidden = true;
        summaryEl.innerHTML = "";
      }
      if (emptyEl) {
        emptyEl.hidden = false;
        emptyEl.textContent =
          "Additional comparable monitoring periods are required for trend analysis.";
      }
      if (helpEl) helpEl.textContent = "";
      return;
    }

    var current = data[data.length - 1];
    var prior = data[data.length - 2];
    var change =
      current != null && prior != null
        ? Math.round((current - prior) * 10) / 10
        : null;
    if (summaryEl) {
      summaryEl.hidden = false;
      summaryEl.innerHTML =
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Current</div><div class="aiv-detail-trend-stat__value">' +
        AiVisibilityUi.escapeHtml(current != null ? current + "%" : "—") +
        '</div></div><div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Prior</div><div class="aiv-detail-trend-stat__value">' +
        AiVisibilityUi.escapeHtml(prior != null ? prior + "%" : "—") +
        '</div></div><div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Change</div><div class="aiv-detail-trend-stat__value">' +
        AiVisibilityUi.escapeHtml(
          change == null
            ? "—"
            : (change > 0 ? "+" : "") + change + " pp"
        ) +
        '</div></div><div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Dates</div><div class="aiv-detail-trend-stat__value">' +
        AiVisibilityUi.escapeHtml(
          (labels[labels.length - 2] || "—") +
            " → " +
            (labels[labels.length - 1] || "—")
        ) +
        "</div></div>";
    }

    if (!canvas || typeof window.Chart !== "function") {
      if (chartWrap) chartWrap.hidden = true;
      if (emptyEl) {
        emptyEl.hidden = false;
        emptyEl.textContent =
          typeof window.Chart !== "function"
            ? "Chart library unavailable — refresh the page to load trend visuals."
            : "Trend chart unavailable.";
      }
      return;
    }

    if (emptyEl) {
      emptyEl.hidden = true;
      emptyEl.textContent = "";
    }
    if (chartWrap) chartWrap.hidden = false;
    canvas.style.display = "block";
    if (helpEl) {
      helpEl.textContent =
        labels[0] +
        " – " +
        labels[labels.length - 1] +
        " · Actual monitoring periods only.";
    }

    var brandLabel =
      (state.overview && state.overview.brandName) || "Selected brand";
    var color = MARKET_TREND_COLORS[0];

    detailTrendChart = new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: brandLabel,
            data: data,
            borderColor: color.border,
            backgroundColor: color.border,
            fill: false,
            tension: 0.4,
            borderWidth: 2,
            pointRadius: 4,
            pointHoverRadius: 6,
            pointBackgroundColor: color.border,
            pointBorderColor: "#fff",
            pointBorderWidth: 2,
            pointHoverBackgroundColor: color.border,
            pointHoverBorderColor: "#fff",
            pointHoverBorderWidth: 2,
            spanGaps: false,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { top: 18, right: 8, bottom: 2, left: 2 } },
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: true,
            backgroundColor: "rgba(255, 255, 255, 0.95)",
            titleColor: "#080f25",
            bodyColor: "#37446b",
            borderColor: "#d9e1fa",
            borderWidth: 1,
            padding: 12,
            cornerRadius: 6,
            displayColors: true,
            boxPadding: 4,
            callbacks: {
              title: function (items) {
                return items && items.length ? String(items[0].label || "") : "";
              },
              label: function (context) {
                var y = context.parsed && context.parsed.y;
                var name = context.dataset.label || "";
                if (y == null || !isFinite(y)) return " " + name + ": —";
                return " " + name + ": " + y + "%";
              },
            },
          },
        },
        scales: {
          x: {
            grid: { display: false, drawBorder: false },
            ticks: { color: "#7e89ac", font: { size: 11 } },
          },
          y: {
            beginAtZero: true,
            max: 100,
            grid: { color: "rgba(126, 137, 172, 0.1)", drawBorder: false },
            ticks: {
              color: "#7e89ac",
              font: { size: 11 },
              padding: 6,
              callback: function (value) {
                return value + "%";
              },
            },
          },
        },
      },
    });
  }

  function formatEvidenceExcerpt(raw) {
    var text = String(raw || "").trim();
    if (!text) return '<p class="aiv-excerpt-p">No excerpt available.</p>';
    var lines = text.split(/\r?\n/);
    var out = [];
    var i = 0;

    function stripMdBold(s) {
      return String(s || "").replace(/\*\*(.+?)\*\*/g, "$1").replace(/__(.+?)__/g, "$1");
    }

    function renderTable(tableLines) {
      var rows = tableLines.filter(function (line) {
        return !/^\|\s*:?-{3,}/.test(line);
      });
      if (!rows.length) return "";
      var html = '<div class="aiv-excerpt-table-wrap"><table class="aiv-excerpt-table">';
      rows.forEach(function (row, idx) {
        var cells = row
          .replace(/^\|/, "")
          .replace(/\|$/, "")
          .split("|")
          .map(function (c) {
            return AiVisibilityUi.escapeHtml(stripMdBold(c.trim()));
          });
        var tag = idx === 0 ? "th" : "td";
        if (idx === 0) html += "<thead>";
        if (idx === 1) html += "</thead><tbody>";
        html +=
          "<tr>" +
          cells
            .map(function (c) {
              return "<" + tag + ">" + c + "</" + tag + ">";
            })
            .join("") +
          "</tr>";
      });
      if (rows.length === 1) html += "</thead><tbody>";
      html += "</tbody></table></div>";
      return html;
    }

    while (i < lines.length) {
      var line = lines[i];
      if (/^\s*\|/.test(line)) {
        var tableLines = [];
        while (i < lines.length && /^\s*\|/.test(lines[i])) {
          tableLines.push(lines[i]);
          i += 1;
        }
        out.push(renderTable(tableLines));
        continue;
      }
      var trimmed = line.trim();
      if (!trimmed) {
        i += 1;
        continue;
      }
      if (/^#{1,3}\s+/.test(trimmed)) {
        out.push(
          '<h4 class="aiv-excerpt-h">' +
            AiVisibilityUi.escapeHtml(stripMdBold(trimmed.replace(/^#{1,3}\s+/, ""))) +
            "</h4>"
        );
      } else {
        out.push(
          '<p class="aiv-excerpt-p">' +
            AiVisibilityUi.escapeHtml(stripMdBold(trimmed)) +
            "</p>"
        );
      }
      i += 1;
    }
    return out.join("") || '<p class="aiv-excerpt-p">No excerpt available.</p>';
  }

  function evidenceEmptyMessage(data) {
    var reason = (data && (data.emptyReason || data.filterReason)) || "";
    if (reason === "ACCESS_DEPTH" || reason === "UNAUTHORIZED_EVIDENCE") {
      return "Evidence not available for this access depth.";
    }
    if (reason === "LANGUAGE_MISMATCH") {
      return "This evidence was recorded in a different language than the current filter. Switch Language to match the monitoring language, then open it again.";
    }
    if (reason === "PROVIDER_MISMATCH") {
      return "This evidence was recorded on a different provider than the current filter. Switch Provider to match, then open it again.";
    }
    if (reason === "EVIDENCE_NOT_FOUND") {
      return "Evidence record was not found for this monitoring run.";
    }
    if (data && data.allowed === false) {
      return "Evidence not available for this access depth.";
    }
    return "Evidence is not available for this link.";
  }

  async function openEvidence(evidenceId, opts) {
    opts = opts || {};
    var brandId = opts.brandId || state.brandId;
    if (!evidenceId || !brandId || brandId === "portfolio") return;
    var body = $("aivEvidenceBody");
    var drawer = $("aivEvidenceDrawer");
    if (body) {
      body.innerHTML = '<div class="aiv-empty">Loading evidence…</div>';
    }
    if (drawer && typeof drawer.showModal === "function" && !drawer.open) {
      drawer.showModal();
    }
    try {
      var query = { evidenceId: evidenceId };
      if (opts.provider) query.provider = opts.provider;
      if (opts.language) query.language = opts.language;
      var data = await apiGet(
        "/api/ai-visibility/brand/" +
          encodeURIComponent(brandId) +
          "/evidence" +
          qs(query)
      );
      var ev = (data.evidence || [])[0];
      if (!body) return;
      if (!ev) {
        body.innerHTML =
          '<div class="aiv-empty">' +
          AiVisibilityUi.escapeHtml(evidenceEmptyMessage(data)) +
          "</div>";
      } else {
        var providerLine =
          (ev.provider || "openai") + (ev.model ? " · " + ev.model : "");
        var descriptors = (ev.evidenceDescriptors || []).filter(Boolean);
        var citations = ev.citations || [];
        var metaBits = [];
        if (ev.commercialRegion || ev.geographyScope) {
          metaBits.push(ev.commercialRegion || ev.geographyScope);
        }
        if (ev.timestamp) {
          metaBits.push(String(ev.timestamp).slice(0, 19).replace("T", " "));
        }

        body.innerHTML =
          '<div class="aiv-evidence">' +
          '<section class="aiv-evidence-question">' +
          '<div class="aiv-evidence-label">Owner Intent</div>' +
          '<p class="aiv-evidence-question-text">' +
          AiVisibilityUi.escapeHtml(ev.ownerIntent || ev.measurementContextLabel || "—") +
          "</p>" +
          (ev.decisionContext
            ? '<div class="aiv-evidence-label aiv-evidence-label--sub">Decision Context</div>' +
              '<p class="aiv-evidence-decision-context">' +
              AiVisibilityUi.escapeHtml(ev.decisionContext) +
              "</p>"
            : "") +
          "</section>" +
          '<section class="aiv-evidence-meta" aria-label="Evidence details">' +
          '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Status</div>' +
          '<div class="aiv-evidence-value">' +
          AiVisibilityUi.statusBadge(ev.brandStatus) +
          "</div></div>" +
          '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Provider</div>' +
          '<div class="aiv-evidence-value">' +
          AiVisibilityUi.escapeHtml(providerLine) +
          "</div></div>" +
          '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Context</div>' +
          '<div class="aiv-evidence-value">' +
          AiVisibilityUi.escapeHtml(metaBits.join(" · ") || "—") +
          "</div></div>" +
          '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Entity</div>' +
          '<div class="aiv-evidence-value">' +
          AiVisibilityUi.escapeHtml(
            (ev.drilldownTrace && ev.drilldownTrace.canonicalEntityId) ||
              ev.entityId ||
              brandId ||
              "—"
          ) +
          (ev.presenceObserved ? " · observed" : " · not observed") +
          "</div></div>" +
          (ev.responseId || (ev.drilldownTrace && ev.drilldownTrace.responseId)
            ? '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Response</div>' +
              '<div class="aiv-evidence-value aiv-mono">' +
              AiVisibilityUi.escapeHtml(
                ev.responseId || ev.drilldownTrace.responseId
              ) +
              "</div></div>"
            : "") +
          "</section>" +
          (descriptors.length
            ? '<section class="aiv-evidence-chips" aria-label="Descriptors">' +
              descriptors
                .map(function (d) {
                  return (
                    '<span class="aiv-evidence-chip">' +
                    AiVisibilityUi.escapeHtml(d) +
                    "</span>"
                  );
                })
                .join("") +
              "</section>"
            : "") +
          '<section class="aiv-evidence-section">' +
          '<div class="aiv-evidence-label">AI response excerpt</div>' +
          '<div class="aiv-excerpt aiv-excerpt--rich">' +
          formatEvidenceExcerpt(ev.responseExcerpt) +
          "</div></section>" +
          (citations.length
            ? '<section class="aiv-evidence-section">' +
              '<div class="aiv-evidence-label">Cited sources</div>' +
              '<ul class="aiv-exec-list aiv-sources-list aiv-evidence-sources">' +
              citations
                .slice(0, 12)
                .map(function (c) {
                  var domain = String((c && (c.domain || c.url)) || "").trim();
                  var href = sourceDomainHref(domain) || (c && c.url) || null;
                  var label = c.domain || domain || c.url || "—";
                  if (!href) {
                    return (
                      "<li><span class=\"aiv-source-item\">" +
                      AiVisibilityUi.escapeHtml(label) +
                      "</span></li>"
                    );
                  }
                  return (
                    '<li><a class="aiv-source-link" href="' +
                    AiVisibilityUi.escapeHtml(href) +
                    '" target="_blank" rel="noopener noreferrer">' +
                    AiVisibilityUi.escapeHtml(label) +
                    "</a></li>"
                  );
                })
                .join("") +
              "</ul></section>"
            : "") +
          "</div>";
      }
      if (drawer && typeof drawer.showModal === "function" && !drawer.open) {
        drawer.showModal();
      }
    } catch (err) {
      if (body) {
        body.innerHTML =
          '<div class="aiv-empty">' +
          AiVisibilityUi.escapeHtml(err.message || "Failed to load evidence") +
          "</div>";
      }
      setError(err.message || "Failed to load evidence");
      if (drawer && typeof drawer.showModal === "function" && !drawer.open) {
        drawer.showModal();
      }
    }
  }

  async function loadExecutive(loadToken) {
    setActiveTab("executive");
    var token = loadToken || beginLoadGeneration();
    var fetchOpts = token.signal ? { signal: token.signal } : {};
    var data = await apiGet(
      "/api/ai-visibility/brand/executive-summary" + qs(),
      fetchOpts
    );
    if (!shouldApplyLoadResult(token, data)) return null;
    state.executive = data;
    var requestedLanguage = state.language;
    applyAvailableProviders(data);
    applyLanguageFilterContract(
      data.languageFilterContract || {
        availableLanguages: data.availableLanguages || [],
        options: (data.availableLanguages || []).map(function (code) {
          return {
            value: code,
            label: code === "es" ? "Spanish" : code === "en" ? "English" : code,
          };
        }),
        visible: (data.availableLanguages || []).length > 1,
        defaultSelection: (data.availableLanguages || []).indexOf("en") >= 0 ? "en" : (data.availableLanguages || [])[0] || null,
        ALL_LANGUAGES_OPTION: false,
      }
    );
    // First load often omits language; contract then picks EN/ES. Re-fetch only when
    // the resolved UI language differs from what the server already returned.
    if (
      !requestedLanguage &&
      state.language &&
      !state._execLanguageReconciled
    ) {
      state._execLanguageReconciled = true;
      var serverLang = String(data.language || "").toLowerCase();
      var uiLang = String(state.language || "").toLowerCase();
      if (serverLang !== uiLang) {
        if (!token.isCurrent()) return null;
        data = await apiGet(
          "/api/ai-visibility/brand/executive-summary" + qs(),
          fetchOpts
        );
        if (!shouldApplyLoadResult(token, data)) return null;
        state.executive = data;
        applyAvailableProviders(data);
        applyLanguageFilterContract(
          data.languageFilterContract || {
            availableLanguages: data.availableLanguages || [],
            visible: (data.availableLanguages || []).length > 1,
            defaultSelection:
              (data.availableLanguages || []).indexOf("en") >= 0
                ? "en"
                : (data.availableLanguages || [])[0] || null,
            ALL_LANGUAGES_OPTION: false,
          }
        );
      }
    }
    if (!token.isCurrent()) return null;
    applyDemoPortfolioKeyFromPayload(data);
    state.portfolio = {
      brands: (data.portfolioOverview && data.portfolioOverview.brands) || [],
      availableProviders: data.availableProviders || [],
      availableLanguages: data.availableLanguages || [],
      languageFilterContract: data.languageFilterContract || null,
    };
    state.brandId = clampBrandToPortfolio(state.portfolio.brands);
    fillBrandSelect(state.portfolio.brands);
    persistFilterPrefs();
    syncFilterControlsFromState();
    renderExecutive(data);
    state._execCacheFp = execTabCacheFp();
    return data;
  }

  async function paintDetailOverview(overview, brandId) {
    state.overview = overview;
    applyMonitoringFreshness(
      overview.monitoringFreshness ||
        (state.executive && state.executive.monitoringFreshness) ||
        null
    );
    applyLanguageFilterContract(
      overview.languageFilterContract || {
        availableLanguages: overview.availableLanguages || [],
        options: (overview.availableLanguages || []).map(function (code) {
          return {
            value: code,
            label: code === "es" ? "Spanish" : code === "en" ? "English" : code,
          };
        }),
        visible: (overview.availableLanguages || []).length > 1,
        defaultSelection:
          (overview.availableLanguages || []).indexOf("en") >= 0
            ? "en"
            : (overview.availableLanguages || [])[0] || null,
        ALL_LANGUAGES_OPTION: false,
      }
    );
    renderKpis(overview);
    await loadDetailExecutiveInsights(overview);
    var dp = overview.decisionPatterns || {};
    renderIntentCoverage($("aivDetailIntentCoverage"), dp.ownerIntentCoverage);
    renderDecisionPatternExtras(dp.ownerIntentCoverage, dp.topDecisionTerritory);
    renderDetailIntelligence(overview);
    renderDetailNarrativeSection(state.executive || {});
    if (
      !overview.detailIntelligence ||
      !overview.detailIntelligence.recommendedReviews ||
      !overview.detailIntelligence.recommendedReviews.length
    ) {
      renderReviewItems($("aivDetailReviewItems"), overview.reviewItems);
    }
    renderProviderPresencePanel(overview.providerPresencePanel);
    renderCoverageDiagnosticsSummary(
      overview.providerPresencePanel,
      dp.ownerIntentCoverage,
      dp.topDecisionTerritory
    );
    renderDiscoverabilityPlaceholder(
      $("aivDetailDiscoverability"),
      overview.publicDiscoverability ||
        overview.discoverabilityBusinessImpact ||
        overview.openAiDiscoverability,
      "detail"
    );
    var langSection = $("aivDetailLanguageSection");
    var lc =
      overview.languageComparison ||
      (state.execLanguageComparison &&
      (!state.execLanguageComparison.brandId ||
        state.execLanguageComparison.brandId === brandId)
        ? state.execLanguageComparison
        : null);
    if (langSection) {
      if (hasDualLanguageComparison(lc)) {
        langSection.hidden = false;
        renderLanguageComparison(lc, "aivDetailLanguage");
      } else {
        langSection.hidden = true;
        var langEl = $("aivDetailLanguage");
        if (langEl) langEl.innerHTML = "";
      }
    }
    if (
      overview.availabilityMessage ||
      (overview.kpis &&
        overview.kpis.aiPresence &&
        overview.kpis.aiPresence.availability === "not_monitored")
    ) {
      setBanner(
        overview.availabilityMessage ||
          "No monitoring data is available for this brand in " +
            state.geography +
            " yet."
      );
    } else {
      setBanner(null);
    }
  }

  function settledPanelValue(settled, index, token, fallbackMsg) {
    var settledItem = settled[index];
    if (settledItem.status === "fulfilled") {
      if (!shouldApplyLoadResult(token, settledItem.value)) {
        return {
          ok: false,
          message: "Stale response discarded",
          points: [],
          peers: [],
          competitors: [],
          sources: [],
        };
      }
      return settledItem.value;
    }
    if (
      settledItem.reason &&
      (settledItem.reason.aborted || settledItem.reason.kind === "aborted")
    ) {
      return {
        ok: false,
        message: "Request cancelled",
        points: [],
        peers: [],
        competitors: [],
        sources: [],
      };
    }
    console.error(
      "[ai-visibility-brand] secondary panel failed",
      fallbackMsg,
      settledItem.reason
    );
    return {
      ok: false,
      message: (settledItem.reason && settledItem.reason.message) || fallbackMsg,
      points: [],
      peers: [],
      competitors: [],
      sources: [],
    };
  }

  function renderDetailIntelligence(overview) {
    var intel = (overview && overview.detailIntelligence) || null;
    if (!intel || intel.ok !== true) {
      var assocSec = $("aivDetailAssociationsSection");
      if (assocSec) assocSec.hidden = true;
      return;
    }

    var assocEl = $("aivDetailAssociations");
    var assocSec = $("aivDetailAssociationsSection");
    var associations = intel.validatedAssociations || [];
    if (assocSec && assocEl) {
      if (!associations.length) {
        assocSec.hidden = true;
        assocEl.innerHTML = "";
      } else {
        assocSec.hidden = false;
        assocEl.innerHTML =
          '<div class="deals-table-container aiv-portfolio-table-wrap">' +
          '<table class="deals-table aiv-portfolio-table">' +
          "<thead><tr>" +
          '<th class="no-sort">Attribute</th>' +
          '<th class="no-sort">Evidence Strength</th>' +
          '<th class="no-sort">Observations</th>' +
          '<th class="no-sort">Providers</th>' +
          "</tr></thead><tbody>" +
          associations
            .map(function (a) {
              return (
                "<tr><td>" +
                AiVisibilityUi.escapeHtml(
                  String(a.attributeId || "—").replace(/_/g, " ")
                ) +
                "</td><td>" +
                AiVisibilityUi.escapeHtml(
                  String(a.descriptor || "—").replace(/_/g, " ")
                ) +
                "</td><td>" +
                AiVisibilityUi.escapeHtml(String(a.observationCount || "—")) +
                "</td><td>" +
                AiVisibilityUi.escapeHtml(String((a.providers || []).length || "—")) +
                "</td></tr>"
              );
            })
            .join("") +
          "</tbody></table></div>";
      }
    }

    var truth = intel.truthComparisons || {};
    var truthRows = (truth.executiveEligible || []).map(function (t) {
      return {
        ownerIntent: t.dimension ? String(t.dimension).replace(/_/g, " ") : "Brand fact",
        decisionContext: t.headline || "Governed brand fact comparison",
        aiRepresentation: t.headline || t.aiClaim || "—",
        dealalityContext: t.dealalityFact || "Governed Brand Basics fact",
        reviewStatus: "Potential AI Perception Gap",
      };
    });
    if (truthRows.length) {
      renderAiVsDealality("aivDetailContextBody", { rows: truthRows });
    } else if (overview && overview.aiVsDealalityContext) {
      renderAiVsDealality("aivDetailContextBody", overview.aiVsDealalityContext);
    } else {
      renderAiVsDealality("aivDetailContextBody", null);
    }

    var reviews = intel.recommendedReviews || [];
    if (reviews.length) {
      renderReviewItems(
        $("aivDetailReviewItems"),
        reviews.map(function (r, idx) {
          return {
            type: r.actionCategory || "review",
            title: r.scenarioName || "Recommended Review",
            description: r.text,
          };
        })
      );
    }
  }

  function renderDetailNarrativeSection(data) {
    var section = $("aivDetailNarrativeSection");
    var body = $("aivNarrativeBody");
    var empty = $("aivNarrativeEmpty");
    if (!section || !body) return;

    var summary = (data && data.narrativeSummary) || {};
    var narratives = summary.productionNarratives || [];
    if (!narratives.length) {
      section.hidden = true;
      body.innerHTML = "";
      if (empty) empty.hidden = true;
      return;
    }

    section.hidden = false;
    if (empty) empty.hidden = true;

    body.innerHTML = narratives.map(function (n) {
      var family = String(n.narrativeFamily || "").replace(/_/g, " ").toLowerCase();
      family = family.charAt(0).toUpperCase() + family.slice(1);
      var disp = formatExecutiveDispositionLabel(n.disposition);
      var dispClass = (n.disposition || "").indexOf("NO_ACTION") >= 0 || (n.disposition || "").indexOf("MONITOR") >= 0
        ? "aiv-disposition--calm"
        : "";
      return (
        "<tr>" +
        "<td>" + AiVisibilityUi.escapeHtml(n.headline || family) + "</td>" +
        "<td>" + AiVisibilityUi.escapeHtml(family) + "</td>" +
        "<td>" + AiVisibilityUi.escapeHtml(n.evidence || "—") + "</td>" +
        "<td>" + AiVisibilityUi.escapeHtml(String(n.providerCount || "—")) + "</td>" +
        "<td>" + AiVisibilityUi.escapeHtml(String(n.observationCount || "—")) + " obs</td>" +
        '<td><span class="aiv-insight-disposition ' + dispClass + '">' + AiVisibilityUi.escapeHtml(disp || "—") + "</span></td>" +
        "</tr>"
      );
    }).join("");
  }

  async function loadDetailExecutiveInsights(overview) {
    var section = $("aivDetailInsightSection");
    var row = $("aivDetailInsights");
    if (!section || !row) return;
    var brandId = (overview && overview.brandId) || state.brandId || "";
    var fp = [
      brandId,
      state.geography,
      state.provider,
      state.language || "",
    ].join("|");
    try {
      var insightPayload =
        overview &&
        overview.brandExecutiveIntelligenceInsights &&
        Array.isArray(overview.brandExecutiveIntelligenceInsights.boxes) &&
        overview.brandExecutiveIntelligenceInsights.boxes.length
          ? overview.brandExecutiveIntelligenceInsights
          : overview && overview.detailExecutiveInsights
            ? overview.detailExecutiveInsights
            : null;
      // Fallback: if overview omitted brand insights, do not reuse portfolio Exec tiles.
      if (!insightPayload || !Array.isArray(insightPayload.boxes)) {
        section.hidden = true;
        row.innerHTML = "";
        state._detailInsightFilterFp = fp;
        return;
      }
      state._detailInsightFilterFp = fp;
      renderInsightBoxes(
        "aivDetailInsights",
        "aivDetailInsightSection",
        insightPayload
      );
    } catch (err) {
      section.hidden = true;
      row.innerHTML = "";
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[aiv] detail executive insights skipped", err);
      }
    }
  }

  async function loadDetail(loadToken) {
    setActiveTab("detail");
    var token = loadToken || beginLoadGeneration();
    var fetchOpts = token.signal ? { signal: token.signal } : {};
    if (!state.portfolio) {
      var port = await apiGet(
        "/api/ai-visibility/brand/portfolio" + qs(),
        fetchOpts
      );
      if (!shouldApplyLoadResult(token, port)) return null;
      applyDemoPortfolioKeyFromPayload(port);
      state.portfolio = port;
      applyAvailableProviders(port);
      applyLanguageFilterContract(
        port.languageFilterContract || {
          availableLanguages: port.availableLanguages || [],
          visible: (port.availableLanguages || []).length > 1,
          defaultSelection:
            (port.availableLanguages || []).indexOf("en") >= 0
              ? "en"
              : (port.availableLanguages || [])[0] || null,
          ALL_LANGUAGES_OPTION: false,
        }
      );
    } else {
      applyDemoPortfolioKeyFromPayload(state.portfolio);
      applyAvailableProviders(state.portfolio);
      if (state.portfolio.languageFilterContract || state.portfolio.availableLanguages) {
        applyLanguageFilterContract(
          state.portfolio.languageFilterContract || {
            availableLanguages: state.portfolio.availableLanguages || [],
            visible: (state.portfolio.availableLanguages || []).length > 1,
            defaultSelection:
              (state.portfolio.availableLanguages || []).indexOf("en") >= 0
                ? "en"
                : (state.portfolio.availableLanguages || [])[0] || null,
            ALL_LANGUAGES_OPTION: false,
          }
        );
      }
    }
    if (!token.isCurrent()) return null;
    fillBrandSelect(state.portfolio.brands);
    state.brandId = clampBrandToPortfolio(state.portfolio.brands);
    if (!state.brandId) {
      $("aivKpiRow").innerHTML = emptyBlock("Select an entitled brand.");
      return null;
    }
    persistBrand(state.brandId);
    var brandId = state.brandId;
    var brandBase =
      "/api/ai-visibility/brand/" + encodeURIComponent(brandId);
    var navMode =
      (state._detailNav && state._detailNav.watchlistMode) ||
      watchlistState.mode ||
      "missing";
    var overviewP = apiGet(brandBase + "/overview" + qs(), fetchOpts);
    var trendP = apiGet(brandBase + "/trend" + qs(), fetchOpts);
    var competitorsP = apiGet(brandBase + "/competitors" + qs(), fetchOpts);
    var sourcesP = apiGet(brandBase + "/sources" + qs(), fetchOpts);
    var watchlistP = loadWatchlist(navMode, fetchOpts);

    var overview = await overviewP;
    if (!shouldApplyLoadResult(token, overview)) return null;
    await paintDetailOverview(overview, brandId);
    if (!token.isCurrent()) return null;

    var competitorsEl = $("aivCompetitors");
    var sourcesEl = $("aivSources");
    if (competitorsEl) competitorsEl.innerHTML = emptyBlock("Loading peer context…");
    if (sourcesEl) sourcesEl.innerHTML = emptyBlock("Loading sources…");
    setLoading(false);

    var settled = await Promise.allSettled([
      trendP,
      competitorsP,
      sourcesP,
      watchlistP,
    ]);
    if (!token.isCurrent()) return null;
    var trendData = settledPanelValue(settled, 0, token, "Could not load trend");
    var competitorsData = settledPanelValue(
      settled,
      1,
      token,
      "Could not load competitors"
    );
    var sourcesData = settledPanelValue(settled, 2, token, "Could not load sources");
    var watchlistData =
      settled[3].status === "fulfilled" ? settled[3].value : null;
    renderDetailTrendChart(trendData);
    renderCompetitors(competitorsData);
    renderSources(sourcesData);
    state._detailCacheFp = detailTabCacheFp();
    state._detailSecondaryCache = {
      trend: trendData,
      competitors: competitorsData,
      sources: sourcesData,
      watchlist: watchlistData,
    };
    return overview;
  }

  async function loadAll() {
    ensureDemoBrandPortfolioClientContext();
    if (tryRenderFromTabCache()) {
      setLoading(false);
      return;
    }
    setError(null);
    setLoading(true);
    var loadToken = beginLoadGeneration();
    try {
      if (state.tab === "detail") {
        await loadDetail(loadToken);
      } else {
        await loadExecutive(loadToken);
      }
    } catch (err) {
      if (err && (err.aborted || err.kind === "aborted")) {
        return;
      }
      if (!loadToken.isCurrent()) {
        return;
      }
      console.error("[ai-visibility-brand]", err);
      clearExecutiveDomForFreshPaint();
      var kind = err.kind || "api_error";
      if (kind === "api_route_missing") {
        setError(err.message);
        setBanner(null);
      } else if (kind === "auth") {
        setError(err.message || "Not authorized for Brand AI Visibility.");
      } else if (kind === "provider_not_monitored") {
        setError(null);
        setBanner(err.message || "This provider has not been monitored yet.");
        $("aivExecutiveView").hidden = state.tab !== "executive";
        $("aivDetailView").hidden = state.tab !== "detail";
        return;
      } else if (kind === "network") {
        setError(
          err.message ||
            "Could not reach the Brand AI Visibility API. Confirm the Node server is running."
        );
      } else {
        setError(err.message || "Failed to load Brand AI Visibility");
      }
      $("aivExecutiveView").hidden = true;
      $("aivDetailView").hidden = true;
    } finally {
      if (loadToken.isCurrent()) {
        setLoading(false);
      }
    }
  }

  function wire() {
    bindPortfolioColumnInfo();
    $("aivTabExecutive").addEventListener("click", function () {
      setActiveTab("executive");
      loadAll();
    });
    $("aivTabDetail").addEventListener("click", function () {
      setActiveTab("detail");
      loadAll();
    });
    $("aivApply").addEventListener("click", function () {
      state.geography = $("aivGeography").value;
      if ($("aivProvider") && !$("aivProvider").disabled) {
        state.provider = $("aivProvider").value || "openai";
      }
      if ($("aivLanguage") && !$("aivLanguage").disabled && $("aivLanguage").value) {
        state.language = $("aivLanguage").value;
      }
      if (state.tab === "detail") {
        state.brandId = $("aivBrand").value || state.brandId;
        persistBrand(state.brandId);
        state.intent = $("aivIntent").value || "";
      }
      state._execLanguageReconciled = false;
      invalidateTabCaches();
      persistFilterPrefs();
      loadAll();
    });
    $("aivReset").addEventListener("click", function () {
      state.geography = "CALA";
      state.provider = "openai";
      state.language = null;
      state.intent = "";
      state.questionFilter = "all";
      state._execLanguageReconciled = false;
      invalidateTabCaches();
      $("aivGeography").value = "CALA";
      if ($("aivProvider")) $("aivProvider").value = "openai";
      if ($("aivLanguage")) $("aivLanguage").value = "";
      if ($("aivIntent")) $("aivIntent").value = "";
      if (state.tab === "detail") {
        state.brandId = restoreBrand((state.portfolio && state.portfolio.brands) || []);
        if ($("aivBrand") && state.brandId) $("aivBrand").value = state.brandId;
      }
      persistFilterPrefs();
      loadAll();
    });
    if ($("aivLanguage")) {
      $("aivLanguage").addEventListener("change", function () {
        var next = $("aivLanguage").value || null;
        if (!next || next === state.language) return;
        state.language = next;
        persistFilterPrefs();
        loadAll();
      });
    }
    var qTabs = $("aivQuestionTabs");
    if (qTabs) {
      qTabs.addEventListener("click", function (e) {
        var btn = e.target.closest("button[data-filter]");
        if (!btn || state.tab !== "detail" || !state.brandId) return;
        state.questionFilter = btn.getAttribute("data-filter") || "all";
        qTabs.querySelectorAll("button").forEach(function (b) {
          b.classList.toggle("is-active", b === btn);
        });
        renderQuestions({ questions: state.detailQuestions || [] });
      });
    }
    var wlTabs = $("aivWatchlistTabs");
    if (wlTabs) {
      wlTabs.addEventListener("click", function (e) {
        var btn = e.target.closest("button[data-watchlist]");
        if (!btn || !state.brandId) return;
        wlTabs.querySelectorAll("button").forEach(function (b) {
          b.classList.toggle("is-active", b === btn);
        });
        loadWatchlist(btn.getAttribute("data-watchlist") || "missing");
      });
    }
    ["aivWlFamily"].forEach(function (id) {
      var el = $(id);
      if (el) {
        el.addEventListener("change", function () {
          watchlistState.page = 0;
          renderWatchlistTable();
        });
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    var initialTab = "executive";
    try {
      var params = new URLSearchParams(window.location.search || "");
      var qTab = params.get("tab");
      var hashTab = (window.location.hash || "").replace(/^#/, "");
      var stored = sessionStorage.getItem("aiv_brand_tab");
      var candidate = qTab || hashTab || stored || "executive";
      if (candidate === "detail") initialTab = "detail";
      else initialTab = "executive"; // includes stale "hdv"
    } catch (_) {
      initialTab = "executive";
    }
    restoreFilterPrefs();
    setActiveTab(initialTab);
    // Keep selects in sync with restored / default state before first auto-load.
    syncFilterControlsFromState();
    if ($("aivGeography") && !$("aivGeography").value) {
      $("aivGeography").value = state.geography || "CALA";
    }
    if ($("aivProvider") && !$("aivProvider").value) {
      $("aivProvider").value = state.provider || "openai";
    }
    ensureDemoBrandPortfolioClientContext();
    wire();
    loadAll();
  });
})();
