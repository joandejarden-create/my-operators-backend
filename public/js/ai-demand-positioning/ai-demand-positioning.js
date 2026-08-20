/**
 * AI Demand Positioning — Frontend Renderer.
 * Single-page owner report (Executive Summary / full scroll). Detailed View tab removed.
 */
(function () {
  "use strict";

  var API_BASE = "/api/ai-demand-positioning";
  var currentPayload = null;

  function esc(s) { var d = document.createElement("div"); d.textContent = s || ""; return d.innerHTML; }
  function toProperCase(s) { return (s || "").replace(/\b\w/g, function(c) { return c.toUpperCase(); }); }

  function adpGovernedIndexTip() {
    return "<strong>AI Presence Index</strong><br><br>" +
      "AI Presence Index compares how often your hotel appears in monitored AI responses with the average appearance rate of relevant CORE comparable hotels for the same demand territory.<br><br>" +
      "100 represents benchmark parity. A score above 100 means your hotel appears more often than the benchmark; a score below 100 means it appears less often.<br><br>" +
      "Example: if your hotel appears in 60% of monitored AI responses and the CORE benchmark is 50%, the AI Presence Index is 120 — meaning your hotel appears 20% more often than the benchmark.<br><br>" +
      "When the CORE Benchmark is not yet certified, the Index shows the same status. Individual CORE hotel AI Presence values in the Competitive Set can still be valid before certification.<br><br>" +
      "This measures relative AI presence, not bookings, market share, or recommendation share.";
  }

  function adpYourAiPresenceTip() {
    return "<strong>Your AI Presence</strong><br><br>How often this hotel appears across comparable monitored AI responses for this demand territory.";
  }

  function adpCoreBenchmarkTip() {
    return "<strong>CORE Benchmark</strong><br><br>" +
      "The average AI presence rate of governed comparable hotels relevant to this demand territory. " +
      "Only CORE comparable hotels are included in the benchmark.<br><br>" +
      "A CORE hotel with 0% observed AI presence remains a valid measured result. " +
      "Missing or unavailable provider/observation scope is not converted to zero.<br><br>" +
      "<strong>Why &ldquo;Benchmark not yet certified&rdquo;?</strong><br><br>" +
      "You may still see AI Presence values for individual CORE hotels in the Competitive Set. " +
      "Those measurements can be valid before the combined benchmark is certified. " +
      "Dealality only publishes the benchmark once the comparable set and the subject-vs-benchmark comparison " +
      "pass the required coverage and stability checks.";
  }

  function adpBenchmarkUncertifiedShortTip() {
    return "<strong>Benchmark not yet certified</strong><br><br>" +
      "Individual CORE hotel presence can be measured before the overall benchmark is certified. " +
      "The benchmark is published only after required coverage and stability checks pass. " +
      "You may therefore see CORE hotel AI Presence values in the Competitive Set even when this benchmark is not yet certified.";
  }

  function adpPropertyRealityCoverageTip() {
    return adpExecutiveMetricTip("Property Reality Coverage", {
      summary: "This is about representation quality — whether AI reflects what the hotel actually offers — not how often the hotel is named.",
      definition: "The share of monitored property facts and attributes that appear in AI answers for this hotel.",
      formula: "Monitored property attributes that AI currently reflects, divided by all governed attributes tracked for this property.",
      grain: "Each governed attribute counts once. Attributes that were not monitored for this property are left out — they are not treated as missing.",
      whyTrack: "Shows whether AI understands the hotel\u2019s real offer (rooms, location cues, amenities, positioning). Weak coverage can leave important selling points out of AI answers even when the hotel is mentioned.",
      important: "This is not a visibility score and not a booking measure. Low coverage means AI is incomplete about the property, not that travelers are absent."
    });
  }

  function adpTopObservedAlternativeTip() {
    return adpExecutiveMetricTip("Top Observed AI Alternative", {
      summary: "This is the hotel AI named most often alongside or instead of yours across monitored answers this period.",
      definition: "The single hotel that appeared most frequently as an alternative in the comparable AI answers we collected.",
      formula: "Among hotels named in monitored AI answers, the one with the highest appearance count in this period.",
      grain: "Counted from observed hotel names in AI answers. Ties are resolved by the highest observed count.",
      whyTrack: "Gives a practical read on which hotel AI surfaces most often in your competitive context — useful for understanding who travelers may see when AI answers stay questions.",
      important: "An observed AI alternative is not automatically a direct commercial competitor. This is an observation from AI answers, not a judgment of brand, rate, or market set membership."
    });
  }

  function adpTravelerNeedsAppearedTip() {
    return adpExecutiveMetricTip("Traveler Needs Where Hotel Appeared", {
      summary: "This is about breadth — for how many different traveler needs AI recognized the hotel at least once.",
      definition: "The share of monitored traveler needs where at least one AI answer mentioned your hotel.",
      formula: "Traveler needs where any monitored AI model named your hotel, divided by all traveler needs tested for this property.",
      grain: "Each traveler need counts once. If any one model names you for that need, it counts as appeared — even if other models do not.",
      whyTrack: "Shows how widely the hotel shows up across different stay occasions (business, leisure, couples, family, and so on).",
      important: "This matches governed AI Scenario Presence when the same traveler-need universe is used. It is the complement of Traveler Needs Where Hotel Was Missing. Missing data is not treated as absence."
    });
  }

  function adpTravelerNeedsMissingTip() {
    return adpExecutiveMetricTip("Traveler Needs Where Hotel Was Missing", {
      summary: "This is about gaps — traveler needs where AI never named the hotel in the answers we monitored.",
      definition: "The share of monitored traveler needs where no AI answer mentioned your hotel.",
      formula: "Traveler needs with no monitored AI mention of your hotel, divided by all traveler needs tested for this property.",
      grain: "Each traveler need counts once. A need is missing only when every monitored AI answer for that need left the hotel out.",
      whyTrack: "Highlights concrete traveler needs you did not win in AI answers — places to investigate content, positioning, or competitive pressure.",
      important: "This is the complement of Traveler Needs Where Hotel Appeared. Appeared count plus missing count equals total traveler needs tested. Missing data is not shown as 0% absence."
    });
  }

  function adpScenariosMonitoredTip() {
    return adpExecutiveMetricTip("Scenarios Monitored", {
      summary: "This is how large the monitoring set was — traveler scenarios tested across each AI provider.",
      definition: "Shown as traveler scenarios × providers (for example, 60 x 4 Providers).",
      formula: "Count of traveler scenarios monitored, shown together with how many AI providers were included in this period.",
      grain: "Each traveler scenario is asked once per included provider. This is not the same as how often the hotel appeared.",
      whyTrack: "Shows the monitoring hand for this period — how many traveler situations were tested, and on how many providers.",
      important: "Scenarios Monitored answers “how many situations × providers did we test?” Traveler Needs Where Hotel Appeared answers “in how many of those traveler scenarios did the hotel show up at least once?” They are separate cards."
    });
  }

  function adpCoreComparableTip() {
    return "<strong>CORE Comparable</strong><br><br>" +
      "A hotel included in the governed comparable set for this demand territory. " +
      "CORE hotels are used to calculate the CORE Benchmark. " +
      "A CORE hotel remains in the benchmark even if it was not surfaced by AI during the current period.";
  }

  var selectedCompTerritory = null;
  var ADP_MIN_RANK_SAMPLE = 5;
  var METRIC_STATE_INSUFFICIENT_DATA = "Insufficient data";
  var METRIC_STATE_INSUFFICIENT_RANKED = "Insufficient ranked responses";
  var METRIC_STATE_INSUFFICIENT_COMPARABLE = "Insufficient comparable data";
  var METRIC_STATE_INSUFFICIENT_PROPERTY = "Insufficient property data";

  function metricStateValue(text) {
    return text;
  }

  function metricStateOpts() {
    return { metricState: true };
  }

  function formatDemandTerritory(intent, payload) {
    var idx = payload && payload.intentPresenceIndex && payload.intentPresenceIndex[intent];
    if (idx && idx.territory) return idx.territory;
    return intent.replace(/_/g, " ").replace(/\b\w/g, function(c) { return c.toUpperCase(); });
  }

  function propertyRealityCoverageSnapshot(rg) {
    var total = rg && rg.totalAttributes ? rg.totalAttributes : 0;
    var recognized = rg && rg.recognizedCount != null ? rg.recognizedCount : ((rg && rg.recognized) ? rg.recognized.length : 0);
    if (total > 0) {
      return {
        label: "Property Reality Coverage",
        value: fmtPct((recognized / total) * 100),
        meta: recognized + " of " + total + " monitored property attributes represented by AI",
        tooltip: adpPropertyRealityCoverageTip(),
      };
    }
    return {
      label: "Property Reality Coverage",
      value: METRIC_STATE_INSUFFICIENT_PROPERTY,
      meta: "Governed property attribute coverage is not available for this property yet.",
      tooltip: adpPropertyRealityCoverageTip(),
      metricState: true,
    };
  }

  function isConsiderationCalculable(cr) {
    return cr && Number.isFinite(Number(cr.rate)) && Number(cr.comparableObservations) > 0;
  }

  function isScenarioPresenceCalculable(sp) {
    return sp && Number.isFinite(Number(sp.rate)) && Number(sp.eligibleScenarios) > 0;
  }

  function isRankMetricsCalculable(rm) {
    return rm && Number.isFinite(Number(rm.rankEligibleN)) && Number(rm.rankEligibleN) >= ADP_MIN_RANK_SAMPLE;
  }

  function hasComparableExecutiveEvidence(em, payload) {
    if (isConsiderationCalculable(em && em.considerationRate)) return true;
    if (isScenarioPresenceCalculable(em && em.scenarioPresence)) return true;
    return !!(payload && payload.period && Number(payload.period.scenarioCount) > 0);
  }

  var BENCHMARK_UNCERTIFIED_LABEL = "Benchmark not yet certified";
  var BENCHMARK_UNCERTIFIED_LINE1 = "Benchmark not";
  var BENCHMARK_UNCERTIFIED_LINE2 = "yet certified";

  function developingCell() {
    if (window.AiVisibilityUi && typeof AiVisibilityUi.formatTwoLineAvailabilityCell === "function") {
      return AiVisibilityUi.formatTwoLineAvailabilityCell(
        "insufficient_history",
        BENCHMARK_UNCERTIFIED_LINE1,
        BENCHMARK_UNCERTIFIED_LINE2
      );
    }
    return (
      '<span class="aiv-avail-insufficient_history aiv-delta-none aiv-table-availability-label" title="' +
      esc(BENCHMARK_UNCERTIFIED_LABEL) +
      '">' +
      '<span class="aiv-table-availability-label__line">' +
      esc(BENCHMARK_UNCERTIFIED_LINE1) +
      "</span>" +
      '<span class="aiv-table-availability-label__line">' +
      esc(BENCHMARK_UNCERTIFIED_LINE2) +
      "</span>" +
      "</span>"
    );
  }

  function insufficientHistoryCell() {
    if (window.AiVisibilityUi && typeof AiVisibilityUi.formatDeltaCell === "function") {
      return AiVisibilityUi.formatDeltaCell({ availability: "insufficient_history" });
    }
    return (
      '<span class="aiv-avail-insufficient_history aiv-delta-none aiv-table-availability-label">' +
      '<span class="aiv-table-availability-label__line">Insufficient</span>' +
      '<span class="aiv-table-availability-label__line">History</span>' +
      "</span>"
    );
  }
  
  function coreBenchmarkCell(idxData, intentKey) {
    if (!idxData || idxData.coreBenchmarkRatePct == null) return developingCell();
    var coreCount = Number(idxData.coreCount);
    var basedOn = Number.isFinite(coreCount) && coreCount > 0
      ? '<button type="button" class="aiv-cell-submeta aiv-cell-submeta--link" data-adp-core-scroll="' + esc(intentKey || "") + '">Based on ' + coreCount + ' CORE comparable hotels</button>'
      : "";
    return '<span class="aiv-intent-rate-value">' + fmtPct(idxData.coreBenchmarkRatePct) + '</span>' + basedOn;
  }
  
  function adpTipHtml(title, question, why) {
    return "<strong>" + title + "</strong><br><br><strong>Question:</strong> " + question +
      "<br><br><strong>Why track it:</strong> " + why;
  }

  /** Detailed tooltips for optional executive metrics only. */
  function adpExecutiveMetricTip(title, parts) {
    var html = "<strong>" + title + "</strong>";
    if (parts.summary) {
      html += "<br><br>" + parts.summary;
    }
    if (parts.definition) {
      html += "<br><br><strong>What this shows:</strong> " + parts.definition;
    }
    if (parts.formula) {
      html += "<br><br><strong>How it\u2019s calculated:</strong> " + parts.formula;
    }
    if (parts.grain) {
      html += "<br><br><strong>How we count:</strong> " + parts.grain;
    }
    if (parts.whyTrack) {
      html += "<br><br><strong>Why track it:</strong> " + parts.whyTrack;
    }
    if (parts.important) {
      html += "<br><br><strong>Important:</strong> " + parts.important;
    }
    return html;
  }
  function adpTipPlain(question, why) {
    return "Question: " + question + " Why track it: " + why;
  }
  function info(tooltip) { return '<span class="adp-info-icon" data-tooltip="' + esc(tooltip) + '">i</span>'; }

  /** Consistent ADP percent display: ##.#% */
  function fmtPct(value) {
    if (typeof value === "string") {
      var trimmed = value.trim();
      if (trimmed.endsWith("%")) {
        var parsed = parseFloat(trimmed.slice(0, -1));
        if (Number.isFinite(parsed)) value = parsed;
      }
    }
    var n = Number(value);
    if (!Number.isFinite(n)) return "0.0%";
    return (Math.round(n * 10) / 10).toFixed(1) + "%";
  }

  function fmtPctFromRatio(ratio) {
    return fmtPct(Number(ratio) * 100);
  }

  async function authFetch(url) {
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, { waitForLogin: true, maxWaitMs: 12000 });
    }
    return fetch(url, { headers: { Accept: "application/json" }, credentials: "include" });
  }

  function show(id) { var el = document.getElementById(id); if (el) el.hidden = false; }
  function hide(id) { var el = document.getElementById(id); if (el) el.hidden = true; }
  function hideAll() { ["adpStateLoading","adpStateError","adpStateNoData","adpStateSuccess"].forEach(hide); }

  // --- Tab management (Executive Summary only; Detailed View removed) ---
  function initTabs() {
    var tab = document.getElementById("adpTabExec");
    var panel = document.getElementById("adpPanelExecutive");
    if (tab) {
      tab.classList.add("active");
      tab.setAttribute("aria-selected", "true");
    }
    if (panel) {
      panel.hidden = false;
      panel.setAttribute("data-panel", "executive");
    }
  }

  // --- Load ---
  function getQueryPropertyId() {
    try {
      var params = new URLSearchParams(window.location.search);
      return params.get("property") || params.get("propertyId") || "";
    } catch (e) {
      return "";
    }
  }

  /** Active property for evidence API calls — never fall back to another hotel. */
  function getActivePropertyId() {
    if (currentPayload) {
      if (currentPayload.property && currentPayload.property.propertyId) {
        return currentPayload.property.propertyId;
      }
      if (currentPayload.propertyId) return currentPayload.propertyId;
    }
    var sel = document.getElementById("adpProperty");
    if (sel && sel.value) return sel.value;
    return getQueryPropertyId();
  }

  async function loadProperties() {
    try {
      var res = await authFetch(API_BASE + "/properties");
      var data = await res.json();
      var sel = document.getElementById("adpProperty");
      if (!sel || !data.ok) return;
      sel.innerHTML = "";
      (data.properties || []).forEach(function (p) {
        var opt = document.createElement("option");
        opt.value = p.propertyId;
        opt.textContent = p.name + " — " + p.city + ", " + p.state;
        sel.appendChild(opt);
      });
      var requested = getQueryPropertyId();
      if (requested && sel.querySelector('option[value="' + requested + '"]')) {
        sel.value = requested;
      }
      sel.addEventListener("change", loadReport);
      if (data.properties.length) loadReport();
    } catch (e) { console.error("[ADP] loadProperties error", e); }
  }

  async function loadReport() {
    var sel = document.getElementById("adpProperty");
    if (!sel || !sel.value) return;
    hideAll(); show("adpStateLoading");
    try {
      var res = await authFetch(API_BASE + "/property/" + encodeURIComponent(sel.value) + "/report");
      var data = await res.json();
      if (!data.ok) {
        if (data.error === "no_monitoring_data") { hideAll(); show("adpStateNoData"); return; }
        throw new Error(data.message || data.error);
      }
      currentPayload = data;
      selectedCompTerritory = null;
      hideAll(); show("adpStateSuccess");
      renderExecutive(data);
    } catch (err) {
      hideAll(); show("adpStateError");
      var errEl = document.getElementById("adpErrorMessage");
      if (errEl) errEl.textContent = err.message || "Unable to load report.";
    }
  }

  // =============================================
  // EXECUTIVE SUMMARY TAB
  // =============================================
  function renderExecutive(d) {
    renderExecKpis(d);
    renderExecutiveRead(d);
    try {
      renderExecutiveMetricEnhancements(d);
    } catch (err) {
      console.error("[ADP] optional executive metrics render failed", err);
    }
    renderExecIntentTable(d.demandCapture);
    renderTrends(d);
    renderProviderPresenceTable(d);
    renderRealityKpis(d.realityGap);
    renderExecStrengthsGaps(d.realityGap);
    renderExecCompTable(d);
    renderExecDisplacement(d.lostDemand);
    renderExecActions(d.actions);
    renderExecEvidence(d.evidence);
    // Legacy insight tiles: keep container for old-snapshot IDs; do not populate customer scroll.
    var legacyInsights = document.getElementById("adpExecInsights");
    if (legacyInsights) {
      legacyInsights.hidden = true;
      legacyInsights.innerHTML = "";
    }
  }

  function resolveExecutiveReadPresentation(er) {
    if (!er || er.available === false) return null;

    var ux = er.ux;
    var summary = er.summary;
    var writeup = er.writeup;
    var biggestStrength = (ux && ux.biggestStrength) || (summary && summary.biggestStrength) || null;
    var biggestConstraint = (ux && ux.biggestConstraint) || (summary && summary.biggestConstraint) || null;
    var changeSinceLastRun =
      (ux && ux.changeSinceLastRun) ||
      (summary && summary.changeSinceLastComparableRun) ||
      (summary && summary.changeSinceLastRun) ||
      null;
    var narrative =
      (writeup && writeup.body) ||
      (ux && ux.executiveSummary && ux.executiveSummary.narrative) ||
      (er.current && er.current.narrative) ||
      er.narrative ||
      null;
    var title =
      (writeup && writeup.title) ||
      (ux && ux.executiveSummary && ux.executiveSummary.title) ||
      "What The Data Says";

    if (!narrative) return null;

    function fallbackBox(sectionLabel, headline, body) {
      return { sectionLabel: sectionLabel, headline: headline, body: body };
    }

    return {
      biggestStrength: biggestStrength || fallbackBox(
        "BIGGEST STRENGTH",
        "Still developing",
        "A governed strength summary is not available for this view yet."
      ),
      biggestConstraint: biggestConstraint || fallbackBox(
        "BIGGEST CONSTRAINT",
        "Still developing",
        "A governed constraint summary is not available for this view yet."
      ),
      changeSinceLastRun: changeSinceLastRun || fallbackBox(
        "CHANGE SINCE LAST COMPARABLE RUN",
        "Change unavailable",
        "Comparable prior-period change is not available for this view yet."
      ),
      title: title,
      narrative: narrative,
    };
  }

  function renderExecutiveSummaryBox(box) {
    if (!box) return "";
    return (
      '<article class="adp-er-summary-box">' +
      '<p class="adp-er-summary-box__label">' + esc(box.sectionLabel || "") + "</p>" +
      '<p class="adp-er-summary-box__headline">' + esc(box.headline || "") + "</p>" +
      '<p class="adp-er-summary-box__body">' + esc(box.body || "") + "</p>" +
      "</article>"
    );
  }

  function renderExecutiveRead(d) {
    var gridEl = document.getElementById("adpExecutiveReadGrid");
    var summariesEl = document.getElementById("adpExecutiveReadSummaries");
    var titleEl = document.getElementById("adpExecutiveReadMainTitle");
    var narrativeEl = document.getElementById("adpExecutiveReadNarrative");
    var emptyEl = document.getElementById("adpExecutiveReadEmpty");
    var section = document.getElementById("adpExecutiveReadSection");
    if (!narrativeEl || !section) return;

    var er = d && d.executiveRead;
    var presentation = resolveExecutiveReadPresentation(er);

    if (!er || er.available === false || !presentation || !presentation.narrative) {
      if (gridEl) gridEl.hidden = true;
      narrativeEl.textContent = "";
      if (summariesEl) summariesEl.innerHTML = "";
      if (emptyEl) emptyEl.hidden = false;
      return;
    }

    if (emptyEl) emptyEl.hidden = true;
    if (gridEl) gridEl.hidden = false;

    if (summariesEl) {
      summariesEl.innerHTML =
        renderExecutiveSummaryBox(presentation.biggestStrength) +
        renderExecutiveSummaryBox(presentation.biggestConstraint) +
        renderExecutiveSummaryBox(presentation.changeSinceLastRun);
    }

    if (titleEl) titleEl.textContent = presentation.title || "What The Data Says";
    narrativeEl.textContent = presentation.narrative;
  }

  function renderExecInsights(d) {
    // Replaced by governed Executive Read (Part B). Retained as no-op for callers.
    return;
  }

  function execTile(title, body, evidence) {
    return '<article class="aiv-insight-tile aiv-insight-tile--exec">' +
      '<h3>' + esc(title) + '</h3>' +
      '<p class="aiv-insight-body">' + esc(body) + '</p>' +
      (evidence ? '<p class="aiv-insight-evidence"><span class="aiv-insight-evidence-label">Evidence:</span> ' + esc(evidence.replace(/^Evidence:\s*/i, '')) + '</p>' : '') +
      '</article>';
  }

  function renderExecKpis(d) {
    var el = document.getElementById("adpKpiRow");
    if (!el) return;
    var rg = d.realityGap;
    var cs = d.competitiveSet;

    // Exact Property Snapshot contract (5 cards). Strongest-demand territory belongs in
    // Executive Summary only — not shown here (PROPERTY_SNAPSHOT_STRONGEST_DEMAND_TERRITORY_VISIBLE=0).
    var realityCard = propertyRealityCoverageSnapshot(rg);
    var scenariosCard = scenariosMonitoredSnapshot(d);
    var travelerNeeds = travelerNeedsAppearanceSnapshot(d);
    var topAlternative = cs.observed && cs.observed.length ? cs.observed[0].name : "—";
    var topAlternativeMeta = cs.observed && cs.observed.length
      ? "Most frequently named alternative hotel across monitored AI responses this period."
      : "No observed AI alternatives are available for this period yet.";

    el.innerHTML =
      kpiCardWithInfo(
        realityCard.label,
        realityCard.value,
        realityCard.meta,
        realityCard.tooltip,
        "adp-executive-metric-tooltip",
        realityCard.metricState ? metricStateOpts() : undefined
      ) +
      kpiCardWithInfo(
        "Scenarios Monitored",
        scenariosCard.value,
        scenariosCard.meta,
        adpScenariosMonitoredTip(),
        "adp-executive-metric-tooltip",
        scenariosCard.available ? undefined : metricStateOpts()
      ) +
      kpiCardWithInfo(
        "Traveler Needs Where Hotel Appeared",
        travelerNeeds.appearedValue,
        travelerNeeds.appearedMeta,
        adpTravelerNeedsAppearedTip(),
        "adp-executive-metric-tooltip",
        travelerNeeds.available ? undefined : metricStateOpts()
      ) +
      kpiCardWithInfo(
        "Traveler Needs Where Hotel Was Missing",
        travelerNeeds.missingValue,
        travelerNeeds.missingMeta,
        adpTravelerNeedsMissingTip(),
        "adp-executive-metric-tooltip",
        travelerNeeds.available ? undefined : metricStateOpts()
      ) +
      kpiCardWithInfo(
        "Top Observed AI Alternative",
        topAlternative,
        topAlternativeMeta,
        adpTopObservedAlternativeTip(),
        "adp-executive-metric-tooltip"
      );
  }

  /**
   * Card 2 — Scenarios Monitored (distinct from Appeared).
   * Primary value = traveler scenarios × providers (monitoring hand volume).
   */
  function scenariosMonitoredSnapshot(d) {
    var travelerNeeds = travelerNeedsAppearanceSnapshot(d);
    var scenarioCount = travelerNeeds.available ? travelerNeeds.total : null;
    if (scenarioCount == null) {
      var dc = d.demandCapture || {};
      var sp = d.executiveMetrics && d.executiveMetrics.scenarioPresence;
      if (sp && Number.isFinite(Number(sp.eligibleScenarios)) && Number(sp.eligibleScenarios) > 0) {
        scenarioCount = Number(sp.eligibleScenarios);
      } else if (Number.isFinite(Number(dc.totalScenarios)) && Number(dc.totalScenarios) > 0) {
        scenarioCount = Number(dc.totalScenarios);
      } else if (d.period && Number.isFinite(Number(d.period.scenarioCount)) && Number(d.period.scenarioCount) > 0) {
        scenarioCount = Number(d.period.scenarioCount);
      }
    }

    var providerNames = collectMonitoredProviderLabels(d);
    var providerCount = providerNames.length;
    if (
      providerCount === 0 &&
      d.period &&
      Number.isFinite(Number(d.period.providerCount)) &&
      Number(d.period.providerCount) > 0
    ) {
      providerCount = Number(d.period.providerCount);
    }

    if (scenarioCount == null) {
      return {
        available: false,
        value: "—",
        meta: "Scenario count not available yet.",
      };
    }

    if (!providerCount) {
      return {
        available: true,
        value: String(scenarioCount),
        meta: scenarioCount + " traveler scenarios monitored this period.",
      };
    }

    var providerWord = providerCount === 1 ? "Provider" : "Providers";
    var meta = providerNames.length
      ? scenarioCount + " traveler scenarios × " + formatProviderList(providerNames) + "."
      : scenarioCount + " traveler scenarios × " + providerCount + " " + providerWord.toLowerCase() + ".";

    return {
      available: true,
      value: scenarioCount + " x " + providerCount + " " + providerWord,
      meta: meta,
    };
  }

  function formatSnapshotProviderLabel(raw) {
    return formatProviderDisplayName(raw);
  }

  /** Customer-facing provider labels — never "Openai". */
  function formatProviderDisplayName(raw) {
    if (raw == null || raw === "") return "";
    var key = String(raw).trim().toLowerCase();
    var labels = {
      openai: "OpenAI",
      chatgpt: "OpenAI",
      "gpt-4": "OpenAI",
      "gpt-4o": "OpenAI",
      gemini: "Gemini",
      perplexity: "Perplexity",
      claude: "Claude",
      anthropic: "Claude",
    };
    if (labels[key]) return labels[key];
    var s = String(raw).trim();
    return s.charAt(0).toUpperCase() + s.slice(1);
  }

  function collectMonitoredProviderLabels(d) {
    var preferredOrder = ["openai", "chatgpt", "gemini", "perplexity", "claude"];
    var found = Object.create(null);
    function note(raw) {
      if (raw == null || raw === "") return;
      var key = String(raw).trim().toLowerCase();
      if (!key || found[key]) return;
      found[key] = true;
    }
    var lists = [
      d.evidence && d.evidence.providers,
      d.providerVisibility && d.providerVisibility.providers,
      d.period && d.period.providers,
    ];
    lists.forEach(function (list) {
      if (!Array.isArray(list)) return;
      list.forEach(function (row) {
        if (typeof row === "string") note(row);
        else if (row && row.provider) note(row.provider);
      });
    });
    var labels = [];
    var seenLabel = Object.create(null);
    preferredOrder.forEach(function (key) {
      if (!found[key]) return;
      var label = formatSnapshotProviderLabel(key);
      if (!label || seenLabel[label]) return;
      seenLabel[label] = true;
      labels.push(label);
      delete found[key];
    });
    Object.keys(found).sort().forEach(function (key) {
      var label = formatSnapshotProviderLabel(key);
      if (!label || seenLabel[label]) return;
      seenLabel[label] = true;
      labels.push(label);
    });
    return labels;
  }

  function formatProviderList(names) {
    if (!names.length) return "";
    if (names.length === 1) return names[0];
    if (names.length === 2) return names[0] + " and " + names[1];
    return names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
  }

  /**
   * Property Snapshot traveler-needs complements.
   * Prefer governed AI Scenario Presence counts when available so Appeared reconciles
   * with Scenario Presence; fall back to demandCapture universe. Missing data ≠ absence.
   */
  function travelerNeedsAppearanceSnapshot(d) {
    var propertyName = (d.property && d.property.name) || "The hotel";
    var dc = d.demandCapture || {};
    var sp = d.executiveMetrics && d.executiveMetrics.scenarioPresence;
    var total = null;
    var appeared = null;
    var appearedPct = null;

    if (
      sp &&
      Number.isFinite(Number(sp.eligibleScenarios)) &&
      Number(sp.eligibleScenarios) > 0 &&
      Number.isFinite(Number(sp.capturedScenarios))
    ) {
      total = Number(sp.eligibleScenarios);
      appeared = Number(sp.capturedScenarios);
      appearedPct = Number.isFinite(Number(sp.rate)) ? Number(sp.rate) : null;
    } else if (
      Number.isFinite(Number(dc.totalScenarios)) &&
      Number(dc.totalScenarios) > 0 &&
      Number.isFinite(Number(dc.capturedScenarios))
    ) {
      total = Number(dc.totalScenarios);
      appeared = Number(dc.capturedScenarios);
      appearedPct = Number.isFinite(Number(dc.overallRate)) ? Number(dc.overallRate) : null;
    }

    if (total == null || appeared == null || appearedPct == null) {
      return {
        appearedValue: "—",
        appearedMeta: "Not enough monitored traveler needs to calculate appearance yet.",
        missingValue: "—",
        missingMeta: "Not enough monitored traveler needs to calculate missing needs yet.",
        available: false,
      };
    }

    var missing = total - appeared;
    var missingPct = Math.round((100 - appearedPct) * 10) / 10;

    return {
      available: true,
      total: total,
      appeared: appeared,
      missing: missing,
      appearedPct: appearedPct,
      missingPct: missingPct,
      appearedValue: fmtPct(appearedPct) + " (" + appeared + ")",
      missingValue: fmtPct(missingPct) + " (" + missing + ")",
      appearedMeta:
        propertyName +
        " appeared in at least one AI answer for " +
        appeared +
        " of the " +
        total +
        " traveler needs tested.",
      missingMeta:
        "The hotel did not appear in any monitored AI answer for " +
        missing +
        " of the " +
        total +
        " traveler needs tested.",
    };
  }

  function kpiCard(label, value, helpText) {
    return '<article class="aiv-kpi"><h3>' + esc(label) +
      '</h3><div class="aiv-value">' + esc(value) +
      '</div><div class="aiv-meta">' + esc(helpText) +
      '</div></article>';
  }

  function kpiCardWithInfo(label, value, metaText, tooltipHtml, tooltipClass, opts) {
    var tipClass = "tooltip-content" + (tooltipClass ? " " + tooltipClass : "");
    var valueClass = opts && opts.metricState ? " aiv-value--metric-state" : "";
    var infoHtml = tooltipHtml
      ? '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="Info about ' + esc(label) + '"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="' + tipClass + '" hidden>' + tooltipHtml + '</div></span>'
      : "";
    return '<article class="aiv-kpi"><h3><span class="aiv-kpi-label">' + esc(label) + '</span>' + infoHtml +
      '</h3><div class="aiv-value' + valueClass + '">' + esc(value) +
      '</div><div class="aiv-meta">' + esc(metaText) +
      '</div></article>';
  }

  function hideExecutiveMetricsSection() {
    var section = document.getElementById("adpExecutiveMetricsSection");
    if (section) section.hidden = true;
  }

  function formatPpDelta(delta) {
    if (delta == null || !Number.isFinite(Number(delta))) return null;
    var n = Number(delta);
    return (n > 0 ? "+" : "") + n.toFixed(1) + " pts vs last run";
  }

  /**
   * Fixed five-card AI Demand Positioning Metrics — always visible; missing data uses explicit states.
   */
  function renderExecutiveMetricEnhancements(payload) {
    var section = document.getElementById("adpExecutiveMetricsSection");
    var row = document.getElementById("adpExecutiveMetricsRow");
    if (!section || !row) return;

    var em = payload && payload.executiveMetrics;
    var cards = [];
    var cr = em && em.considerationRate;
    if (isConsiderationCalculable(cr)) {
      var crDelta = em.currentVsPrior && em.currentVsPrior.deltas
        ? formatPpDelta(em.currentVsPrior.deltas.considerationRate)
        : null;
      cards.push(kpiCardWithInfo(
        "AI Consideration Rate",
        fmtPct(cr.rate),
        (cr.presentObservations + " of " + cr.comparableObservations + " comparable AI responses") +
          (crDelta ? " · " + crDelta : ""),
        adpExecutiveMetricTip("AI Consideration Rate", {
          summary: "This is about frequency — across all provider responses, how often you actually made the consideration set.",
          definition: "How often your hotel is named in the AI answers we collected for traveler questions that apply to your property.",
          formula: "AI answers that mention your hotel, divided by all usable AI answers in this period.",
          grain: "Each AI answer counts on its own. If OpenAI mentions you and Gemini does not, that is one yes and one no \u2014 not a single combined result.",
          whyTrack: "This is the simplest read on whether AI is putting your hotel in front of travelers at all, across every answer we collected.",
          important: "Being named is not the same as being recommended, ranked first, or winning a booking. This does not measure actual demand captured."
        }),
        "adp-executive-metric-tooltip"
      ));
    } else {
      cards.push(kpiCardWithInfo(
        "AI Consideration Rate",
        metricStateValue(METRIC_STATE_INSUFFICIENT_DATA),
        "Not enough comparable AI responses to calculate this metric.",
        adpExecutiveMetricTip("AI Consideration Rate", {
          summary: "This is about frequency — across all provider responses, how often you actually made the consideration set.",
          definition: "How often your hotel is named in the AI answers we collected for traveler questions that apply to your property.",
          important: "This metric requires enough comparable AI responses. It is not shown as 0% when data is insufficient."
        }),
        "adp-executive-metric-tooltip",
        metricStateOpts()
      ));
    }

    var sp = em && em.scenarioPresence;
    if (isScenarioPresenceCalculable(sp)) {
      var spDelta = em.currentVsPrior && em.currentVsPrior.deltas
        ? formatPpDelta(em.currentVsPrior.deltas.scenarioPresence)
        : null;
      cards.push(kpiCardWithInfo(
        "AI Scenario Presence",
        fmtPct(sp.rate),
        ("Present in " + sp.capturedScenarios + " of " + sp.eligibleScenarios + " monitored demand scenarios") +
          (spDelta ? " · " + spDelta : ""),
        adpExecutiveMetricTip("AI Scenario Presence", {
          summary: "This is about breadth — how many different demand situations you participate in.",
          definition: "The share of traveler questions we monitor where at least one AI model mentioned your hotel.",
          formula: "Traveler questions where any monitored AI model named your hotel, divided by all traveler questions that apply to your property.",
          grain: "Each traveler question counts once. If any one model names you, that question counts as present \u2014 even if other models do not.",
          whyTrack: "This shows how widely you appear across different traveler needs (business, leisure, family, and so on), rather than how often you appear in every individual AI answer.",
          important: "This is not the same as AI Consideration Rate. That metric counts every AI answer separately. This one asks: for each traveler question, did any model mention you?"
        }),
        "adp-executive-metric-tooltip"
      ));
    } else {
      cards.push(kpiCardWithInfo(
        "AI Scenario Presence",
        metricStateValue(METRIC_STATE_INSUFFICIENT_DATA),
        "Not enough comparable scenario observations to calculate this metric.",
        adpExecutiveMetricTip("AI Scenario Presence", {
          summary: "This is about breadth — how many different demand situations you participate in.",
          definition: "The share of traveler questions we monitor where at least one AI model mentioned your hotel.",
          important: "This metric requires enough monitored scenario observations. It is not shown as 0% when data is insufficient."
        }),
        "adp-executive-metric-tooltip",
        metricStateOpts()
      ));
    }

    var rm = em && em.rankMetrics;
    if (isRankMetricsCalculable(rm) && Number.isFinite(Number(rm.numberOneAppearanceRate))) {
      var n1Delta = em.currentVsPrior && em.currentVsPrior.deltas
        ? formatPpDelta(em.currentVsPrior.deltas.numberOneAppearanceRate)
        : null;
      cards.push(kpiCardWithInfo(
        "#1 Appearance Rate",
        fmtPct(rm.numberOneAppearanceRate),
        (rm.numberOneCount + " of " + rm.rankEligibleN + " ranked AI responses") +
          (n1Delta ? " · " + n1Delta : ""),
        adpExecutiveMetricTip("#1 Appearance Rate", {
          definition: "When AI listed hotels in a clear order, how often was your hotel named first?",
          formula: "AI answers where your hotel is listed first, divided by AI answers that used a clear ranking (numbered list, ranked bullets, or a ranked table).",
          grain: "Only answers with a clear ranking are included. A hotel mentioned in ordinary paragraph text is not treated as #1 just because it appears first in the sentence.",
          whyTrack: "Tells you whether AI leads with your hotel when it actually ranks options. Top-3 Appearance Rate shows whether you still make the shortlist when you are not first.",
          important: "This is not a \u201ctop recommendation\u201d score. Unranked answers are left out. If too few ranked answers exist, this card shows Insufficient ranked responses rather than 0%."
        }),
        "adp-executive-metric-tooltip"
      ));
    } else {
      cards.push(kpiCardWithInfo(
        "#1 Appearance Rate",
        metricStateValue(METRIC_STATE_INSUFFICIENT_RANKED),
        "More rank-eligible AI responses are needed to calculate #1 Appearance Rate.",
        adpExecutiveMetricTip("#1 Appearance Rate", {
          definition: "When AI listed hotels in a clear order, how often was your hotel named first?",
          important: "Requires at least " + ADP_MIN_RANK_SAMPLE + " rank-eligible AI responses before a rate is shown."
        }),
        "adp-executive-metric-tooltip",
        metricStateOpts()
      ));
    }

    if (isRankMetricsCalculable(rm) && Number.isFinite(Number(rm.topThreeAppearanceRate))) {
      cards.push(kpiCardWithInfo(
        "Top-3 Appearance Rate",
        fmtPct(rm.topThreeAppearanceRate),
        rm.topThreeCount + " of " + rm.rankEligibleN + " ranked AI responses",
        adpExecutiveMetricTip("Top-3 Appearance Rate", {
          definition: "When AI listed hotels in a clear order, how often was your hotel in the first three names?",
          formula: "AI answers where your hotel is listed 1st, 2nd, or 3rd, divided by AI answers that used a clear ranking.",
          grain: "Uses the same ranked answers as #1 Appearance Rate. Unranked answers are not included.",
          whyTrack: "Shows whether you make AI\u2019s shortlist even when you are not the first hotel named.",
          important: "A higher list position is not automatically a competitive win. If too few ranked answers exist, this card shows Insufficient ranked responses rather than 0%."
        }),
        "adp-executive-metric-tooltip"
      ));
    } else {
      cards.push(kpiCardWithInfo(
        "Top-3 Appearance Rate",
        metricStateValue(METRIC_STATE_INSUFFICIENT_RANKED),
        "More rank-eligible AI responses are needed to calculate Top-3 Appearance Rate.",
        adpExecutiveMetricTip("Top-3 Appearance Rate", {
          definition: "When AI listed hotels in a clear order, how often was your hotel in the first three names?",
          important: "Requires at least " + ADP_MIN_RANK_SAMPLE + " rank-eligible AI responses before a rate is shown."
        }),
        "adp-executive-metric-tooltip",
        metricStateOpts()
      ));
    }

    if (hasComparableExecutiveEvidence(em, payload)) {
      var cpsCount = em && em.competitorPresentScenarios && Number.isFinite(Number(em.competitorPresentScenarios.scenarioCount))
        ? Number(em.competitorPresentScenarios.scenarioCount)
        : 0;
      cards.push(kpiCardWithInfo(
        "Competitor-Present Scenarios",
        String(cpsCount) + " scenarios",
        "Traveler questions where a comparable hotel appeared and yours did not",
        adpExecutiveMetricTip("Competitor-Present Scenarios", {
          definition: "How many traveler questions had at least one AI answer that named a comparable hotel but not yours.",
          formula: "Count of traveler questions where your hotel was missing in at least one AI answer and a comparable hotel was named in that answer.",
          grain: "Each traveler question counts once, even if more than one AI model showed a competitor without you.",
          whyTrack: "These are the questions where AI is sending attention to another hotel instead of yours. That is more useful than a raw count of individual missing answers.",
          important: "This is not lost bookings. A hotel AI named is not automatically your true competitor. Absence from an answer is not the same as losing a guest."
        }),
        "adp-executive-metric-tooltip"
      ));
    } else {
      cards.push(kpiCardWithInfo(
        "Competitor-Present Scenarios",
        metricStateValue(METRIC_STATE_INSUFFICIENT_COMPARABLE),
        "More comparable competitive observations are needed to calculate this metric.",
        adpExecutiveMetricTip("Competitor-Present Scenarios", {
          definition: "How many traveler questions had at least one AI answer that named a comparable hotel but not yours.",
          important: "This metric requires enough comparable competitive observations. It is not shown as 0 scenarios when data is insufficient."
        }),
        "adp-executive-metric-tooltip",
        metricStateOpts()
      ));
    }

    section.hidden = false;
    row.innerHTML = cards.join("");
  }

  function kpiStatus(rate) { return rate >= 50 ? "good" : rate >= 30 ? "warning" : "critical"; }

  var SORT_ARROWS = '<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>';

  function thCol(label, tooltip, thClass) {
    return '<th class="no-sort' + (thClass ? ' ' + thClass : '') + '"><span class="aiv-th-label"><span class="aiv-th-text">' + label + '</span>' + SORT_ARROWS + (tooltip ? '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="Info"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="tooltip-content adp-benchmark-tooltip" hidden>' + tooltip + '</div></span>' : '') + '</span></th>';
  }

  var adpTrendChart = null;

  function renderTrends(d) {
    var emptyEl = document.getElementById("adpTrendEmpty");
    var summaryEl = document.getElementById("adpTrendSummary");
    var chartWrap = document.getElementById("adpTrendChartWrap");
    var canvas = document.getElementById("adpTrendChart");
    if (!emptyEl) return;

    if (adpTrendChart) { try { adpTrendChart.destroy(); } catch (_) {} adpTrendChart = null; }

    var trends = d.trends;
    if (!trends || trends.length < 2) {
      emptyEl.hidden = false;
      emptyEl.innerHTML = '<p class="aiv-empty__message">Additional comparable monitoring periods are required for trend analysis.</p>';
      if (summaryEl) summaryEl.hidden = true;
      if (chartWrap) chartWrap.hidden = true;
      return;
    }

    var labels = trends.map(function(t) {
      var s = t.date || "";
      return s.slice(0, 10) + " \u00b7 " + (s.slice(11, 16) || "");
    });

    var realityData = trends.map(function(t) { return t.propertyRealityCoverage != null ? Math.round(t.propertyRealityCoverage * 10) / 10 : null; });
    var scenarioData = trends.map(function(t) { return t.scenarioPresenceRate != null ? Math.round(t.scenarioPresenceRate * 10) / 10 : null; });
    var considerationData = trends.map(function(t) { return t.considerationRate != null ? Math.round(t.considerationRate * 10) / 10 : null; });

    var last = trends[trends.length - 1];
    var prev = trends[trends.length - 2];
    function ppChange(cur, prior) {
      if (cur == null || prior == null) return null;
      return Math.round((cur - prior) * 10) / 10;
    }

    if (summaryEl) {
      summaryEl.hidden = false;
      var rcCh = ppChange(last.propertyRealityCoverage, prev.propertyRealityCoverage);
      var spCh = ppChange(last.scenarioPresenceRate, prev.scenarioPresenceRate);
      var crCh = ppChange(last.considerationRate, prev.considerationRate);
      summaryEl.innerHTML =
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Reality Coverage</div><div class="aiv-detail-trend-stat__value">' + (last.propertyRealityCoverage != null ? fmtPct(last.propertyRealityCoverage) : "\u2014") + '</div><div class="aiv-detail-trend-stat__delta">' + (rcCh == null ? "" : (rcCh > 0 ? "+" : "") + rcCh + " pp") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Scenario Presence</div><div class="aiv-detail-trend-stat__value">' + (last.scenarioPresenceRate != null ? fmtPct(last.scenarioPresenceRate) : "\u2014") + '</div><div class="aiv-detail-trend-stat__delta">' + (spCh == null ? "" : (spCh > 0 ? "+" : "") + spCh + " pp") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Consideration Rate</div><div class="aiv-detail-trend-stat__value">' + (last.considerationRate != null ? fmtPct(last.considerationRate) : "\u2014") + '</div><div class="aiv-detail-trend-stat__delta">' + (crCh == null ? "" : (crCh > 0 ? "+" : "") + crCh + " pp") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Periods</div><div class="aiv-detail-trend-stat__value">' + trends.length + '</div></div>';
    }

    if (!canvas || typeof window.Chart !== "function") {
      if (chartWrap) chartWrap.hidden = true;
      emptyEl.hidden = false;
      emptyEl.innerHTML = '<p class="aiv-empty__message">Chart library unavailable. Refresh the page.</p>';
      return;
    }

    emptyEl.hidden = true;
    if (chartWrap) chartWrap.hidden = false;
    canvas.style.display = "block";

    adpTrendChart = new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Reality Coverage",
            data: realityData,
            borderColor: "#57c3ff",
            backgroundColor: "#57c3ff",
            fill: false,
            tension: 0.35,
            borderWidth: 2,
            pointRadius: 4,
            spanGaps: false
          },
          {
            label: "Scenario Presence",
            data: scenarioData,
            borderColor: "#6c72ff",
            backgroundColor: "#6c72ff",
            fill: false,
            tension: 0.35,
            borderWidth: 2,
            pointRadius: 4,
            spanGaps: false
          },
          {
            label: "Consideration Rate",
            data: considerationData,
            borderColor: "#a78bfa",
            backgroundColor: "#a78bfa",
            fill: false,
            tension: 0.35,
            borderWidth: 2,
            pointRadius: 4,
            spanGaps: false
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { top: 18, right: 8, bottom: 2, left: 2 } },
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: {
            display: true,
            position: "bottom",
            labels: { color: "#7e89ac", boxWidth: 12, padding: 14, font: { size: 11 } }
          },
          tooltip: {
            enabled: true,
            backgroundColor: "rgba(8, 15, 37, 0.95)",
            titleColor: "#d9e1fa",
            bodyColor: "#aeb8d4",
            borderColor: "#37446b",
            borderWidth: 1,
            padding: 12,
            cornerRadius: 6,
            displayColors: true,
            boxPadding: 4,
            callbacks: {
              title: function(items) { return items && items.length ? String(items[0].label || "") : ""; },
              label: function(ctx) {
                var y = ctx.parsed && ctx.parsed.y;
                var name = ctx.dataset && ctx.dataset.label ? ctx.dataset.label : "Metric";
                if (y == null || !isFinite(y)) return " " + name + ": \u2014";
                return " " + name + ": " + fmtPct(y);
              }
            }
          }
        },
        scales: {
          x: {
            grid: { display: false, drawBorder: false },
            ticks: { color: "#7e89ac", font: { size: 11 } }
          },
          y: {
            beginAtZero: true,
            max: 100,
            grid: { color: "rgba(126,137,172,0.1)", drawBorder: false },
            ticks: {
              color: "#7e89ac",
              font: { size: 11 },
              padding: 6,
              callback: function(value) { return fmtPct(value); }
            }
          }
        }
      }
    });
  }

  function renderProviderPresenceTable(d) {
    var el = document.getElementById("adpProviderTableContainer");
    if (!el) return;
    var ev = d.evidence;
    if (!ev || !ev.providers || !ev.providers.length) {
      el.innerHTML = '<p class="aiv-empty-message">No provider data available.</p>';
      return;
    }

    var totalScenarios = d.demandCapture.totalScenarios;
    var html = '<table class="deals-table aiv-portfolio-table aiv-coverage-table">';
    html += '<thead><tr>';
    html += thCol('Provider', adpTipHtml('Provider', 'Which AI model was queried on its own for each demand scenario?', 'Models do not behave the same. You need provider-level visibility to fix gaps on each one.'));
    html += thCol('Status', adpTipHtml('Status', 'Was this provider included in the current monitoring period?', 'Confirms the row reflects live monitored responses, not an estimate or partial run.'));
    html += thCol('AI<br>Presence', adpTipHtml('AI Presence', 'When this provider alone answers a monitored demand question, how often does it mention your property?', 'Each model learns from different sources. A gap here means travelers using that tool may never see you.'));
    html += thCol('Monitored', adpTipHtml('Monitored', 'In how many demand scenarios did this provider mention your property, out of all scenarios asked on this provider?', 'Shows raw capture count for this model, separate from the combined rate across providers.'));
    html += thCol('Missing', adpTipHtml('Missing', 'In how many scenarios did this provider fail to mention your property?', 'These are provider-specific gaps you can target with content or authority work for that model.'));
    html += thCol('Citation', adpTipHtml('Citation', 'When this provider answers, how often does it include source links in the response?', 'Citation-backed answers show which websites AI is reading. Without citations, it is harder to trace and improve what the model uses.'));
    html += thCol('Owned', adpTipHtml('Owned', 'Of this provider\u2019s citations, what share point to your owned websites?', 'Owned sources give you direct control over how AI describes your property. Requires configured owned domains.'));
    html += '</tr></thead><tbody>';

    ev.providers.forEach(function(p) {
      var name = formatProviderDisplayName(p.provider);
      var pct = p.presence;
      var missing = p.total - p.mentioned;
      var presenceVisual = '<span class="aiv-presence-cell aiv-presence-cell--compact"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' + Math.round(pct) + '%"></span></span><span class="aiv-presence-cell__value">' + fmtPct(pct) + '</span></span>';

      // Citation rate for this provider
      var citRate = '0.0%';
      if (ev.providerCitations && ev.providerCitations[p.provider] != null) {
        citRate = fmtPct(ev.providerCitations[p.provider]);
      }

      html += '<tr>';
      html += '<td>' + esc(name) + '</td>';
      html += '<td>Monitored</td>';
      html += '<td class="aiv-metric-cell aiv-presence-metric-cell">' + presenceVisual + '</td>';
      html += '<td class="aiv-metric-cell">' + p.mentioned + ' / ' + p.total + '</td>';
      html += '<td class="aiv-metric-cell">' + missing + '</td>';
      html += '<td class="aiv-metric-cell">' + citRate + '</td>';
      html += '<td class="aiv-metric-cell">—</td>';
      html += '</tr>';
    });

    html += '</tbody></table>';
    el.innerHTML = html;
  }

  function renderExecIntentTable(dc) {
    var el = document.getElementById("adpIntentTableContainer");
    if (!el) return;

    var html = '<table class="deals-table aiv-portfolio-table aiv-coverage-table aiv-intent-coverage-table aiv-unified-intent-table">';
    html += '<thead><tr>';
    html += thCol('Demand<br>Territory', adpTipHtml('Demand Territory', 'Which type of traveler demand is being measured (business, leisure, couples, meetings, and so on)?', 'Capture varies by trip purpose. You need to know where AI recommends you and where demand leaks by intent.'));
    html += thCol('Your AI<br>Presence', adpYourAiPresenceTip());
    html += thCol('CORE<br>Benchmark', adpCoreBenchmarkTip());
    html += thCol('AI Presence<br>Index', adpGovernedIndexTip());
    html += thCol('\u0394 vs<br>Prior Run', adpTipHtml('\u0394 vs Prior Run', 'Is your presence in this intent improving or declining compared with the last comparable monitoring run?', 'A single period does not show direction. Tracking change tells you whether recent content or positioning work is working.'), 'aiv-intent-col-secondary');
    html += thCol('Monitored', adpTipHtml('Monitored', 'How many demand scenarios in this intent were tested, and how many captured your property?', 'Confirms sample size and shows captured versus total for this intent category.'), 'aiv-intent-col-secondary');
    html += thCol('Missing', adpTipHtml('Missing', 'In this intent, how many scenarios had no AI mention of your property?', 'These are concrete lost-demand moments for that traveler type.'), 'aiv-intent-col-secondary');
    html += thCol('Peer-Present<br>Gaps', adpTipHtml('Peer-Present Gaps', 'In this intent, how often do competitors appear in scenarios where you do not?', 'This is competitive displacement: rivals are winning the recommendation when you are absent.'), 'aiv-intent-col-secondary');
    html += thCol('Missing<br>Evidence', adpTipHtml('Missing Evidence', 'What did AI actually say in scenarios where you were missing from this intent?', 'Lets you read real responses and diagnose why competitors were chosen instead.'), 'aiv-intent-col-secondary');
    html += '</tr></thead><tbody>';

    var entries = Object.entries(dc.byIntent);
    // Compute peer-present gaps from lost demand data
    var ld = currentPayload && currentPayload.lostDemand;
    var peerGapsByIntent = {};
    if (ld && ld.scenarios) {
      ld.scenarios.forEach(function(s) {
        if (s.competitorsPresent && s.competitorsPresent.length > 0) {
          peerGapsByIntent[s.intent] = (peerGapsByIntent[s.intent] || 0) + 1;
        }
      });
    }

    entries.forEach(function (e) {
      var intent = e[0], data = e[1];
      var label = intent.replace(/_/g, " ").replace(/\b\w/g, function(c){return c.toUpperCase();});
      var pct = data.rate;
      var monitored = data.captured + "/" + data.total;
      var missing = data.total - data.captured;
      var peerGaps = peerGapsByIntent[intent] || 0;

      var idxData = currentPayload && currentPayload.intentPresenceIndex && currentPayload.intentPresenceIndex[intent];
      var subjectPct = idxData && idxData.subjectRatePct != null ? idxData.subjectRatePct : null;
      var presenceVisual = subjectPct != null
        ? '<span class="aiv-presence-cell aiv-presence-cell--compact"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' + Math.round(subjectPct) + '%"></span></span><span class="aiv-presence-cell__value">' + fmtPct(subjectPct) + '</span></span>'
        : developingCell();

      html += '<tr class="aiv-intent-row aiv-unified-intent-row">';
      html += '<td><span class="project-name-text">' + esc((idxData && idxData.territory) || label) + '</span></td>';
      html += '<td class="aiv-metric-cell aiv-presence-metric-cell aiv-intent-rate-cell">' + presenceVisual + '</td>';
      if (idxData && idxData.index != null && (idxData.subjectRatePct == null || idxData.coreBenchmarkRatePct == null)) {
        html += '<td class="aiv-metric-cell aiv-intent-rate-cell">' + developingCell() + '</td>';
        html += '<td class="aiv-metric-cell aiv-intent-index-cell">' + developingCell() + '</td>';
      } else {
        html += '<td class="aiv-metric-cell aiv-intent-rate-cell">' + coreBenchmarkCell(idxData, intent) + '</td>';
        if (idxData && idxData.index != null && idxData.subjectRatePct != null && idxData.coreBenchmarkRatePct != null) {
          html += '<td class="aiv-metric-cell aiv-intent-index-cell"><strong>' + idxData.index + '</strong></td>';
        } else {
          html += '<td class="aiv-metric-cell aiv-intent-index-cell">' + developingCell() + '</td>';
        }
      }
      html += '<td class="aiv-metric-cell aiv-delta-cell aiv-chg-metric-cell aiv-intent-col-secondary">' + insufficientHistoryCell() + '</td>';
      html += '<td class="aiv-metric-cell aiv-monitored-cell aiv-intent-col-secondary">' + monitored + '</td>';
      html += '<td class="aiv-metric-cell aiv-intent-col-secondary">' + missing + '</td>';
      html += '<td class="aiv-metric-cell aiv-intent-col-secondary">' + peerGaps + '</td>';
      // Missing Evidence link
      if (missing > 0) {
        html += '<td class="aiv-metric-cell aiv-intent-col-secondary"><button type="button" class="aiv-btn-text aiv-link" data-adp-evidence-intent="' + esc(intent) + '">' + missing + ' Missing</button></td>';
      } else {
        html += '<td class="aiv-metric-cell aiv-intent-col-secondary"><span style="color:var(--aiv-text-secondary)">—</span></td>';
      }
      html += '</tr>';
    });

    html += '</tbody></table>';
    el.innerHTML = html;

    // Wire evidence links
    el.querySelectorAll("[data-adp-evidence-intent]").forEach(function(btn) {
      btn.addEventListener("click", function() {
        openAdpEvidence(btn.getAttribute("data-adp-evidence-intent"));
      });
    });

    el.querySelectorAll("[data-adp-core-scroll]").forEach(function(btn) {
      btn.addEventListener("click", function() {
        var intent = btn.getAttribute("data-adp-core-scroll");
        if (intent) selectCompetitiveTerritory(intent, true);
      });
    });
  }

  function openAdpEvidence(intent) {
    var drawer = document.getElementById("adpEvidenceDrawer");
    var body = document.getElementById("adpEvidenceBody");
    if (!drawer || !body) return;
    body.innerHTML = '<div class="aiv-empty">Loading evidence\u2026</div>';
    if (typeof drawer.showModal === "function" && !drawer.open) drawer.showModal();

    var pid = getActivePropertyId();
    if (!pid) {
      body.innerHTML = '<div class="aiv-empty">Select a property to view evidence.</div>';
      return;
    }
    fetch("/api/ai-demand-positioning/property/" + encodeURIComponent(pid) + "/evidence?intent=" + encodeURIComponent(intent) + "&type=missing")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (!data.ok || !data.evidence || !data.evidence.length) {
          body.innerHTML = '<div class="aiv-empty">No missing evidence available for this intent.</div>';
          return;
        }
        var intentLabel = intent.replace(/_/g, " ").replace(/\b\w/g, function(c){return c.toUpperCase();});
        var html = '';
        data.evidence.forEach(function(ev) {
          var providerLine = formatProviderDisplayName(ev.provider || "unknown");
          var citations = ev.sourcesCited || ev.providerCitations || [];
          var competitors = ev.competitorsMentioned || [];
          var metaBits = [];
          if (ev.timestamp) metaBits.push(String(ev.timestamp).slice(0, 19).replace("T", " "));

          html += '<div class="aiv-evidence">' +
            '<section class="aiv-evidence-question">' +
            '<div class="aiv-evidence-label">Owner Intent</div>' +
            '<p class="aiv-evidence-question-text">' + esc(intentLabel) + '</p>' +
            '<div class="aiv-evidence-label aiv-evidence-label--sub">Decision Context</div>' +
            '<p class="aiv-evidence-decision-context">' + esc(ev.scenarioLabel || "") + '</p>' +
            '</section>' +
            '<section class="aiv-evidence-meta" aria-label="Evidence details">' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Status</div>' +
            '<div class="aiv-evidence-value"><span class="aiv-status-badge aiv-status-badge--missing">Missing</span></div></div>' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Provider</div>' +
            '<div class="aiv-evidence-value">' + esc(providerLine) + '</div></div>' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Context</div>' +
            '<div class="aiv-evidence-value">' + esc(metaBits.join(" \u00b7 ") || "\u2014") + '</div></div>' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Entity</div>' +
            '<div class="aiv-evidence-value">' + esc(pid) + ' \u00b7 not observed</div></div>' +
            '</section>' +
            (competitors.length ? '<section class="aiv-evidence-chips" aria-label="Competitors present">' +
              competitors.slice(0, 8).map(function(c){ return '<span class="aiv-evidence-chip">' + esc(c) + '</span>'; }).join("") +
              '</section>' : '') +
            '<section class="aiv-evidence-section">' +
            '<div class="aiv-evidence-label">AI response excerpt</div>' +
            '<div class="aiv-excerpt aiv-excerpt--rich">' + formatExcerpt(ev.responseExcerpt || "") + '</div>' +
            '</section>' +
            (citations.length ? '<section class="aiv-evidence-section">' +
              '<div class="aiv-evidence-label">Cited sources</div>' +
              '<ul class="aiv-exec-list aiv-sources-list aiv-evidence-sources">' +
              citations.slice(0, 8).map(function(c){ var url = typeof c === 'string' ? c : (c.url || c.domain || ""); return '<li><a href="' + esc(url) + '" target="_blank" rel="noopener">' + esc(url) + '</a></li>'; }).join("") +
              '</ul></section>' : '') +
            '</div><hr style="border-color:var(--neutral--700);margin:1.5rem 0;">';
        });
        html += '<p class="aiv-theme-help help-text" style="margin-top:0.5rem;">Showing ' + data.evidence.length + ' of ' + data.total + ' missing observations for ' + esc(intentLabel) + '.</p>';
        body.innerHTML = html;
      })
      .catch(function() {
        body.innerHTML = '<div class="aiv-empty">Error loading evidence.</div>';
      });
  }

  function formatExcerpt(text) {
    if (!text) return '<em>No response excerpt available.</em>';
    return '<div style="max-height:300px;overflow-y:auto;white-space:pre-wrap;font-size:12px;line-height:1.5;color:var(--neutral--200,#e2e8f0);">' + esc(text) + '</div>';
  }

  function renderRealityKpis(rg) {
    var el = document.getElementById("adpRealityKpis");
    if (!el) return;
    var recognized = rg.recognized ? rg.recognized.length : 0;
    var gaps = rg.gaps ? rg.gaps.length : 0;
    var total = rg.totalAttributes || (recognized + gaps);
    var gapPct = total > 0 ? (gaps / total) * 100 : 0;
    var recognitionPct = total > 0 ? (recognized / total) * 100 : 0;
    var avgRecognition = rg.recognized && rg.recognized.length
      ? rg.recognized.reduce(function(sum, r) { return sum + r.recognitionRate; }, 0) / rg.recognized.length
      : 0;

    el.innerHTML =
      '<div class="aiv-kpi-row aiv-citation-kpis">' +
      '<article class="aiv-kpi"><h3>Reality Gap</h3><div class="aiv-value">' + fmtPct(gapPct) + '</div><div class="aiv-meta">AI misses ' + gaps + ' of ' + total + ' tracked attributes</div></article>' +
      '<article class="aiv-kpi"><h3>Property Reality Coverage</h3><div class="aiv-value">' + fmtPct(recognitionPct) + '</div><div class="aiv-meta">' + recognized + ' of ' + total + ' monitored property attributes represented by AI</div></article>' +
      '<article class="aiv-kpi"><h3>Avg Strength</h3><div class="aiv-value">' + fmtPct(avgRecognition) + '</div><div class="aiv-meta">Average recognition rate across recognized attributes</div></article>' +
      '<article class="aiv-kpi"><h3>Tracked Attributes</h3><div class="aiv-value">' + total + '</div><div class="aiv-meta">Total property attributes being monitored</div></article>' +
      '</div>';
  }

  function renderExecStrengthsGaps(rg) {
    var strengthsEl = document.getElementById("adpExecStrengths");
    var gapsEl = document.getElementById("adpExecGaps");
    if (!strengthsEl || !gapsEl) return;

    if (rg.recognized && rg.recognized.length) {
      strengthsEl.innerHTML = '<ul class="aiv-signal-list">' +
        rg.recognized.map(function (r) {
          return '<li class="aiv-signal-item aiv-signal-item--strength"><span class="aiv-signal-icon">✓</span><span class="aiv-signal-text">' + esc(toProperCase(r.label)) + '</span><span class="aiv-signal-badge">' + fmtPct(r.recognitionRate) + '</span></li>';
        }).join("") + '</ul>';
    } else {
      strengthsEl.innerHTML = '<p class="aiv-empty-message">No attributes well-recognized yet.</p>';
    }

    if (rg.gaps && rg.gaps.length) {
      gapsEl.innerHTML = '<ul class="aiv-signal-list">' +
        rg.gaps.map(function (g) {
          return '<li class="aiv-signal-item aiv-signal-item--gap"><span class="aiv-signal-icon">✗</span><span class="aiv-signal-text">' + esc(toProperCase(g.label)) + '</span><span class="aiv-signal-badge aiv-signal-badge--' + g.severity.toLowerCase() + '">' + g.severity + '</span></li>';
        }).join("") + '</ul>';
    } else {
      gapsEl.innerHTML = '<p class="aiv-empty-message">All tracked attributes are well-represented.</p>';
    }
  }

  function defaultCompetitiveTerritory(d) {
    var block = d && d.competitiveRankingByTerritory;
    if (!block || !block.byTerritory) return null;
    if (block.defaultView && block.byTerritory[block.defaultView]) return block.defaultView;
    if (block.byTerritory.overall) return "overall";
    var ranking = block.byTerritory;
    var order = block.selectorOrder || Object.keys(ranking);
    for (var i = 0; i < order.length; i++) {
      if (ranking[order[i]]) return order[i];
    }
    return Object.keys(ranking)[0] || null;
  }

  function isOverallCompView(ranking) {
    return ranking && (ranking.viewType === "overall" || ranking.intent === "overall");
  }

  function selectCompetitiveTerritory(intent, scrollToTable) {
    selectedCompTerritory = intent;
    clearAdpEvidenceDrawer();
    if (currentPayload) renderExecCompTable(currentPayload);
    var sel = document.getElementById("adpCompTerritorySelect");
    if (sel && intent) sel.value = intent;
    if (scrollToTable) {
      var section = document.getElementById("adpCompTable");
      if (section && section.scrollIntoView) section.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  function clearAdpEvidenceDrawer() {
    var drawer = document.getElementById("adpEvidenceDrawer");
    var body = document.getElementById("adpEvidenceBody");
    if (body) body.innerHTML = "";
    if (drawer && typeof drawer.close === "function" && drawer.open) {
      try { drawer.close(); } catch (e) { /* ignore */ }
    }
  }

  function highlightCompCoreRows() {
    var tbody = document.getElementById("adpCompTableBody");
    if (!tbody) return;
    var coreRows = tbody.querySelectorAll("tr.adp-row--core, tr.adp-row--core-appended");
    if (!coreRows.length) return;
    coreRows.forEach(function(row) { row.classList.add("aiv-row--highlight"); });
    if (coreRows[0].scrollIntoView) {
      coreRows[0].scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
    window.setTimeout(function() {
      coreRows.forEach(function(row) { row.classList.remove("aiv-row--highlight"); });
    }, 2400);
  }

  function renderCompBenchmarkCount(countWrap, countEl, territoryRanking, isOverall) {
    if (!countWrap || !countEl) return;
    if (isOverall) {
      countWrap.hidden = true;
      countEl.textContent = "";
      countEl.innerHTML = "";
      return;
    }
    var rec = territoryRanking.reconciliation || {};
    var coreCount = Number(rec.CORE_COUNT || territoryRanking.coreCount);
    if (Number.isFinite(coreCount) && coreCount > 0) {
      countWrap.hidden = false;
      countEl.innerHTML =
        '<button type="button" class="aiv-cell-submeta aiv-cell-submeta--link" data-adp-comp-core-highlight="1">' +
        "Based on " + coreCount + " CORE comparable hotels</button>";
      var btn = countEl.querySelector("[data-adp-comp-core-highlight]");
      if (btn) {
        btn.addEventListener("click", function() {
          highlightCompCoreRows();
        });
      }
      return;
    }
    countWrap.hidden = true;
    countEl.textContent = "";
    countEl.innerHTML = "";
  }

  function renderCompTerritorySelector(d) {
    var wrap = document.getElementById("adpCompTerritoryWrap");
    if (!wrap) return;
    var block = d.competitiveRankingByTerritory;
    var ranking = block && block.byTerritory;
    if (!ranking || !Object.keys(ranking).length) {
      wrap.hidden = true;
      wrap.innerHTML = "";
      return;
    }
    if (!selectedCompTerritory || !ranking[selectedCompTerritory]) {
      selectedCompTerritory = defaultCompetitiveTerritory(d);
    }
    var order = (block && block.selectorOrder) || Object.keys(ranking);
    var options = order.filter(function(key) { return ranking[key]; }).map(function(key) {
      var label = ranking[key].territory || (key === "overall" ? "Overall" : key.replace(/_/g, " "));
      return '<option value="' + esc(key) + '"' + (key === selectedCompTerritory ? ' selected' : '') + '>' + esc(label) + '</option>';
    }).join("");
    wrap.hidden = false;
    wrap.innerHTML =
      '<label class="adp-comp-territory-label" for="adpCompTerritorySelect">Demand territory</label>' +
      '<select id="adpCompTerritorySelect" class="adp-comp-territory-select" aria-label="Select demand territory for competitive ranking">' +
      options + '</select>';
    var sel = document.getElementById("adpCompTerritorySelect");
    if (sel) {
      sel.addEventListener("change", function() {
        selectCompetitiveTerritory(sel.value, false);
      });
    }
  }

  function relationshipPill(row, isOverall) {
    if (row.isSubject) {
      return '<strong>' + esc(row.name) + '</strong> <span class="adp-pill adp-pill--you">You</span>';
    }
    var pills = '<strong>' + esc(row.name) + '</strong> ';
    if (!isOverall && row.isCore) {
      pills += '<span class="adp-pill adp-pill--core" title="Included in the governed comparable-hotel benchmark for this demand territory.">CORE</span>';
    } else if (!isOverall) {
      pills += '<span class="adp-pill adp-pill--extended">Observed</span>';
    }
    if (row.statusLabel) {
      pills += ' <span class="adp-pill adp-pill--muted">' + esc(row.statusLabel) + '</span>';
    }
    return pills;
  }

  function renderExecCompTable(d) {
    var el = document.getElementById("adpCompTableBody");
    var countWrap = document.getElementById("adpCompCount");
    var countEl = document.querySelector("#adpCompCount .results-count");
    var tableEl = document.getElementById("adpCompTable");
    if (!el) return;

    renderCompTerritorySelector(d);

    var rankingBlock = d.competitiveRankingByTerritory && d.competitiveRankingByTerritory.byTerritory;
    var territoryRanking = rankingBlock && selectedCompTerritory ? rankingBlock[selectedCompTerritory] : null;
    var isOverall = isOverallCompView(territoryRanking);

    if (tableEl) {
      tableEl.classList.remove("adp-comp-table--overall");
      if (isOverall) tableEl.classList.add("adp-comp-table--overall");
    }

    var territoryTh = document.querySelector('#adpCompTable th[data-sort="territory"] .aiv-th-text');
    if (territoryTh) {
      territoryTh.innerHTML = isOverall ? "Top Demand<br>Territory" : "Demand<br>Territory";
    }

    if (!territoryRanking || !territoryRanking.displayRows || !territoryRanking.displayRows.length) {
      renderCompBenchmarkCount(countWrap, countEl, territoryRanking, isOverall);
      el.innerHTML = '<tr><td colspan="7"><p class="aiv-empty-message">No competitive ranking data for the selected view.</p></td></tr>';
      return;
    }

    var rows = territoryRanking.displayRows;
    renderCompBenchmarkCount(countWrap, countEl, territoryRanking, isOverall);

    var subjectName =
      (d.property && d.property.name) ||
      (rows.find(function(r) { return r.isSubject; }) || {}).name ||
      "your property";
    var html = "";
    rows.forEach(function(row) {
      var rankDisplay = row.displayRank === "—" ? "—" : "#" + row.displayRank;
      var presence = row.aiPresencePct != null ? fmtPct(row.aiPresencePct) : "—";
      if (row.isZeroPresenceCore || row.isZeroPresenceSubject) presence = "0.0%";
      var dispCount =
        row.displacement && Number.isFinite(Number(row.displacement.count))
          ? Number(row.displacement.count)
          : 0;
      var competitorId = (row.displacement && row.displacement.competitorId) || row.entityId || "";
      var territory = isOverall
        ? (row.topDemandTerritory || "—")
        : (territoryRanking.territory || "—");
      var rowClass = row.isSubject ? ' class="adp-row--subject"' : (row.isCore ? ' class="adp-row--core"' : "");
      if (row.isAppendedCore || row.isAppendedSubject) rowClass = ' class="adp-row--core-appended"';

      var dispCell;
      if (row.isSubject) {
        dispCell = '<span style="color:var(--aiv-text-secondary)">—</span>';
      } else if (dispCount > 0) {
        dispCell =
          '<button type="button" class="aiv-btn-text aiv-link"' +
          ' data-adp-displacement="1"' +
          ' data-adp-competitor-id="' + esc(competitorId) + '"' +
          ' data-adp-competitor-name="' + esc(row.name) + '"' +
          ' data-adp-displacement-count="' + dispCount + '">' +
          dispCount + " Scenarios</button>";
      } else {
        dispCell = '<span style="color:var(--aiv-text-secondary)">—</span>';
      }

      html += '<tr' + rowClass + ' data-entity-id="' + esc(row.entityId || "") + '">' +
        '<td>' + rankDisplay + '</td>' +
        '<td>' + relationshipPill(row, isOverall) + '</td>' +
        '<td>' + presence + '</td>' +
        '<td class="aiv-metric-cell aiv-delta-cell aiv-chg-metric-cell"><span class="aiv-avail-insufficient_history aiv-delta-none">—</span></td>' +
        '<td>' + dispCell + '</td>' +
        '<td>' + (row.isSubject ? '—' : (row.appearances + " scenarios")) + '</td>' +
        '<td>' + esc(territory) + '</td>' +
        '</tr>';
    });
    el.innerHTML = html;

    el.querySelectorAll("[data-adp-displacement]").forEach(function(btn) {
      btn.addEventListener("click", function() {
        openAdpDisplacementEvidence({
          competitorId: btn.getAttribute("data-adp-competitor-id"),
          competitorName: btn.getAttribute("data-adp-competitor-name"),
          expectedCount: Number(btn.getAttribute("data-adp-displacement-count") || 0),
          subjectName: subjectName,
          scopeKey: selectedCompTerritory,
        });
      });
    });
  }

  function openAdpDisplacementEvidence(opts) {
    opts = opts || {};
    var competitorName = opts.competitorName || "";
    var competitorId = opts.competitorId || "";
    var subjectName = opts.subjectName || "your property";
    var scopeKey = opts.scopeKey || selectedCompTerritory || "overall";
    var drawer = document.getElementById("adpEvidenceDrawer");
    var body = document.getElementById("adpEvidenceBody");
    if (!drawer || !body) return;
    body.innerHTML = '<div class="aiv-empty">Loading displacement evidence\u2026</div>';
    if (typeof drawer.showModal === "function" && !drawer.open) drawer.showModal();

    var pid = getActivePropertyId();
    if (!pid) {
      body.innerHTML = '<div class="aiv-empty">Select a property to view evidence.</div>';
      return;
    }

    var isOverall = scopeKey === "overall";
    var qs =
      "type=displacement" +
      "&competitor=" + encodeURIComponent(competitorName) +
      (competitorId ? "&competitorId=" + encodeURIComponent(competitorId) : "") +
      (isOverall
        ? "&scope=overall"
        : "&scope=demand_territory&intent=" + encodeURIComponent(scopeKey));

    fetch("/api/ai-demand-positioning/property/" + encodeURIComponent(pid) + "/evidence?" + qs)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (!data.ok || !data.evidence || !data.evidence.length) {
          body.innerHTML = '<div class="aiv-empty">No displacement evidence available for ' + esc(competitorName) + '.</div>';
          return;
        }
        var html =
          '<p class="aiv-theme-help help-text" style="margin-bottom:1rem;">Scenarios where <strong>' +
          esc(competitorName) +
          "</strong> appeared and <strong>" +
          esc(subjectName) +
          "</strong> did not.</p>";
        data.evidence.forEach(function(ev) {
          var providerLine = formatProviderDisplayName(ev.provider || "unknown");
          var citations = ev.sourcesCited || ev.providerCitations || [];
          var competitors = ev.competitorsMentioned || [];
          var territoryLine = ev.territory || ev.intent || "";

          html += '<div class="aiv-evidence">' +
            (territoryLine
              ? '<section class="aiv-evidence-meta" aria-label="Demand territory" style="margin-bottom:0.5rem;">' +
                '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Demand Territory</div>' +
                '<div class="aiv-evidence-value">' + esc(String(territoryLine).replace(/_/g, " ")) + '</div></div></section>'
              : "") +
            '<section class="aiv-evidence-question">' +
            '<div class="aiv-evidence-label">Demand Scenario</div>' +
            '<p class="aiv-evidence-question-text">' + esc(ev.scenarioLabel || "") + '</p>' +
            '</section>' +
            '<section class="aiv-evidence-meta" aria-label="Evidence details">' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Status</div>' +
            '<div class="aiv-evidence-value"><span class="aiv-status-badge--missing">Displaced</span></div></div>' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Provider</div>' +
            '<div class="aiv-evidence-value">' + esc(providerLine) + '</div></div>' +
            '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Displacing Competitor</div>' +
            '<div class="aiv-evidence-value">' + esc(ev.displacingCompetitor || competitorName) + '</div></div>' +
            '</section>' +
            (competitors.length ? '<section class="aiv-evidence-chips" aria-label="All competitors mentioned">' +
              competitors.slice(0, 8).map(function(c){ return '<span class="aiv-evidence-chip">' + esc(c) + '</span>'; }).join("") +
              '</section>' : '') +
            '<section class="aiv-evidence-section">' +
            '<div class="aiv-evidence-label">AI response excerpt</div>' +
            '<div class="aiv-excerpt aiv-excerpt--rich">' + formatExcerpt(ev.responseExcerpt || "") + '</div>' +
            '</section>' +
            (citations.length ? '<section class="aiv-evidence-section">' +
              '<div class="aiv-evidence-label">Cited sources</div>' +
              '<ul class="aiv-exec-list aiv-sources-list aiv-evidence-sources">' +
              citations.slice(0, 8).map(function(c){ var url = typeof c === 'string' ? c : (c.url || c.domain || ""); return '<li><a href="' + esc(url) + '" target="_blank" rel="noopener">' + esc(url) + '</a></li>'; }).join("") +
              '</ul></section>' : '') +
            '</div><hr style="border-color:var(--neutral--700);margin:1.5rem 0;">';
        });
        html += '<p class="aiv-theme-help help-text" style="margin-top:0.5rem;">Showing ' + data.evidence.length + ' of ' + data.total + ' displacement scenarios for ' + esc(competitorName) + '.</p>';
        body.innerHTML = html;
      })
      .catch(function() {
        body.innerHTML = '<div class="aiv-empty">Error loading displacement evidence.</div>';
      });
  }

  function inferTopTerritory(competitorName) {
    if (!currentPayload) return "—";
    var observations = currentPayload._rawObsByComp;
    if (!observations) {
      // Build lookup from lost demand scenarios
      var ld = currentPayload.lostDemand;
      if (!ld || !ld.scenarios) return deriveFromName(competitorName);
      var intentCounts = {};
      ld.scenarios.forEach(function(s) {
        if (s.competitorsPresent && s.competitorsPresent.indexOf(competitorName) !== -1) {
          intentCounts[s.intent] = (intentCounts[s.intent] || 0) + 1;
        }
      });
      var best = Object.entries(intentCounts).sort(function(a,b){return b[1]-a[1];})[0];
      if (best) return best[0].replace(/_/g, " ").replace(/\b\w/g, function(c){return c.toUpperCase();});
      return deriveFromName(competitorName);
    }
    return "—";
  }

  function deriveFromName(name) {
    var n = name.toLowerCase();
    if (n.includes("waldorf") || n.includes("mandarin") || n.includes("ritz") || n.includes("four seasons")) return "Luxury / Premium";
    if (n.includes("marriott") || n.includes("hilton") || n.includes("hyatt")) return "Business / Loyalty";
    if (n.includes("boutique") || n.includes("collection") || n.includes("autograph")) return "Collection / Soft Brand";
    if (n.includes("resort") || n.includes("beach") || n.includes("club")) return "Leisure / Resort";
    if (n.includes("suites") || n.includes("residence")) return "Extended / Suites";
    return "General Demand";
  }

  function renderExecDisplacement(ld) {
    var el = document.getElementById("adpExecDisplacement");
    if (!el) return;
    if (!ld.displacement || !ld.displacement.length) {
      el.innerHTML = '<p class="aiv-empty-message">No displacement data.</p>';
      return;
    }
    var html = '<ul class="aiv-signal-list">';
    ld.displacement.forEach(function (d) {
      html += '<li class="aiv-signal-item aiv-signal-item--gap"><span class="aiv-signal-icon">⚡</span><span class="aiv-signal-text">' + esc(d.name) + '</span><span class="aiv-signal-badge aiv-signal-badge--high">' + d.displacementCount + ' scenarios</span></li>';
    });
    html += '</ul>';
    if (ld.topReasons && ld.topReasons.length) {
      html += '<div style="margin-top:0.75rem;padding:0.75rem;background:rgba(251,191,36,0.06);border-radius:6px;border:1px solid rgba(251,191,36,0.12)">';
      html += '<p style="font-size:0.75rem;font-weight:700;color:#fbbf24;margin:0 0 0.4rem;text-transform:uppercase">Top Reasons</p>';
      html += '<ul style="margin:0;padding:0 0 0 1rem;font-size:0.82rem;color:#fde68a;line-height:1.6">';
      ld.topReasons.forEach(function (r) { html += '<li>' + esc(r.reason) + '</li>'; });
      html += '</ul></div>';
    }
    el.innerHTML = html;
  }

  function renderExecActions(actions) {
    var el = document.getElementById("adpExecActions");
    if (!el) return;
    if (!actions || !actions.length) { el.innerHTML = '<p class="aiv-empty-message">No priority actions this period.</p>'; return; }
    var html = '<div class="aiv-hdv-review-grid">';
    actions.slice(0, 4).forEach(function (a, idx) {
      var title = toProperCase(a.title || "");
      html += '<article class="aiv-theme-card aiv-hdv-review-card">' +
        '<div class="aiv-hdv-review-index">' + (idx + 1) + '</div>' +
        '<h3 class="aiv-review-kind">' + esc(title) + '</h3>' +
        '<p class="aiv-hdv-review-desc">' + esc(a.description || "") + '</p>' +
        '</article>';
    });
    html += '</div>';
    el.innerHTML = html;
  }

  function renderExecWhiteSpace(ws) {
    var el = document.getElementById("adpExecWhiteSpace");
    if (!el) return;
    if (!ws.opportunities || !ws.opportunities.length) {
      el.innerHTML = '<div class="aiv-theme-card"><p class="aiv-empty-message">No clear white space opportunities this period.</p></div>';
      return;
    }
    var html = '';
    ws.opportunities.slice(0, 6).forEach(function (o) {
      var cls = o.opportunityScore === "HIGH" ? "" : " opp-medium";
      html += '<div class="adp-opp-card' + cls + '">';
      html += '<div class="adp-opp-header"><span class="adp-opp-intent">' + esc(o.intent.replace(/_/g, " ")) + '</span>';
      html += '<span class="adp-opp-badge badge-' + o.opportunityScore.toLowerCase() + '">' + esc(o.opportunityScore) + '</span></div>';
      html += '<p class="adp-opp-rationale">' + esc(o.rationale) + '</p>';
      html += '</div>';
    });
    el.innerHTML = html;
  }

  function renderExecEvidence(ev) {
    if (!ev) return;

    // Citation Intelligence — 2×2 KPI grid + Source Mix (same as Brand AI)
    var citEl = document.getElementById("adpExecCitations");
    if (citEl) {
      var topSource = ev.topSources && ev.topSources.length ? ev.topSources[0] : null;
      var secondSource = ev.topSources && ev.topSources.length > 1 ? ev.topSources[1] : null;

      var html = '<div class="aiv-citation-exec-top aiv-citation-exec-top--2x2">';
      html += '<article class="aiv-kpi"><h3>Citation Coverage</h3><div class="aiv-value">' + esc(ev.totalWithSources + ' of ' + ev.totalObservations) + '</div><div class="aiv-meta">' + esc(fmtPct(ev.citationRate) + ' of monitored responses included at least one citation.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>Avg Sources per Citation</h3><div class="aiv-value">' + esc(String(ev.avgSourcesPerCitation)) + '</div><div class="aiv-meta">Average number of sources cited per grounded response.</div></article>';
      html += '<article class="aiv-kpi"><h3>Top Source</h3><div class="aiv-value aiv-value--domain">' + esc(topSource ? topSource.domain : '—') + '</div><div class="aiv-meta">' + (topSource ? esc(topSource.count + ' of ' + ev.totalWithSources + ' · ' + fmtPct(topSource.frequency)) : 'No sources observed.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>Second Source</h3><div class="aiv-value aiv-value--domain">' + esc(secondSource ? secondSource.domain : '—') + '</div><div class="aiv-meta">' + (secondSource ? esc(secondSource.count + ' of ' + ev.totalWithSources + ' · ' + fmtPct(secondSource.frequency)) : 'No second source observed.') + '</div></article>';
      html += '</div>';

      // Source Mix — Owned / External from governed ownership registry when configured
      var noCit = ev.totalObservations - ev.totalWithSources;
      var citPct = ev.totalObservations > 0 ? fmtPctFromRatio(ev.totalWithSources / ev.totalObservations) : '0.0%';
      var noCitPct = ev.totalObservations > 0 ? fmtPctFromRatio(noCit / ev.totalObservations) : '0.0%';
      var ownedConfigured = ev.ownedDomainsConfigured === true || (ev.ownedSourceShare != null);
      var ownedPct = ownedConfigured && ev.ownedSourceShare != null ? fmtPct(ev.ownedSourceShare) : null;
      var extPct = ownedConfigured && ev.externalSourceShare != null
        ? fmtPct(ev.externalSourceShare)
        : (ev.totalWithSources > 0 ? '100.0%' : '0.0%');
      var ownedMeta = ownedConfigured
        ? (ev.ownedSourceShare > 0
          ? 'Share of cited responses that included at least one owned property or brand property page.'
          : 'Owned domains are configured, but no owned sources were cited in this period.')
        : 'No governed owned domains configured yet.';
      var extMeta = ownedConfigured
        ? 'Share of cited responses whose sources were external (not property-owned).'
        : 'All cited sources are treated as external until owned domains are configured.';
      html += '<div class="aiv-source-mix aiv-source-mix--compact">';
      html += '<div class="aiv-source-mix__head"><div class="aiv-source-mix__label">Source Mix</div></div>';
      html += '<div class="aiv-kpi-row aiv-theme-kpis" style="grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
      html += '<article class="aiv-kpi"><h3>Owned Sources</h3><div class="aiv-value">' + esc(ownedPct != null ? ownedPct : '0.0%') + '</div><div class="aiv-meta">' + esc(ownedMeta) + '</div></article>';
      html += '<article class="aiv-kpi"><h3>External Sources</h3><div class="aiv-value">' + esc(extPct) + '</div><div class="aiv-meta">' + esc(extMeta) + '</div></article>';
      html += '<article class="aiv-kpi"><h3>With Citations</h3><div class="aiv-value">' + citPct + '</div><div class="aiv-meta">' + esc(ev.totalWithSources + ' responses had sources.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>No Citations</h3><div class="aiv-value">' + noCitPct + '</div><div class="aiv-meta">' + esc(noCit + ' responses had no source URLs.') + '</div></article>';
      html += '</div></div>';

      citEl.innerHTML = html;
    }

    // Source Landscape — same markup as Brand AI recurring sources table
    var srcEl = document.getElementById("adpExecSources");
    if (srcEl && ev.topSources && ev.topSources.length) {
      var html = '<div class="deals-table-container aiv-recurring-sources-wrap">';
      html += '<table class="deals-table aiv-recurring-sources-table" aria-label="Recurring source citation frequency"><thead><tr>';
      html += '<th><span class="aiv-th-label"><span class="aiv-th-text">Source</span><span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About Source"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="tooltip-content" hidden>' + adpTipHtml('Source', 'Which website domain did AI cite when answering demand questions?', 'These domains shape how AI describes hotels in your market. Tracking them shows where to publish or improve content.') + '</div></span></span></th>';
      html += '<th><span class="aiv-th-label"><span class="aiv-th-text">Responses<br>Citing</span><span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About Responses Citing"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="tooltip-content" hidden>' + adpTipHtml('Responses Citing', 'In how many provider responses did AI cite this domain?', 'Higher counts mean this website appears often in grounded answers. It is a practical target for content or partnership work.') + '</div></span></span></th>';
      html += '<th><span class="aiv-th-label"><span class="aiv-th-text">Frequency</span><span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="About Frequency"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="tooltip-content" hidden>' + adpTipHtml('Frequency', 'What share of all cited responses included this domain?', 'Shows relative weight among sources AI used this period. This is observed citation share, not a quality or authority score.') + '</div></span></span></th>';
      html += '</tr></thead><tbody>';
      ev.topSources.forEach(function(s) {
        var barWidth = Math.min(s.frequency, 100);
        html += '<tr>';
        html += '<td><span class="aiv-sources-freq-domain"><a class="aiv-source-link project-name-text" href="https://' + esc(s.domain) + '" target="_blank" rel="noopener noreferrer">' + esc(s.domain) + '</a></span></td>';
        html += '<td class="aiv-metric-cell">' + s.count + ' of ' + ev.totalWithSources + '</td>';
        var freqDisplay = fmtPct(s.frequency);
        html += '<td class="aiv-metric-cell"><span class="aiv-sources-freq-bar"><span class="aiv-sources-freq-track" aria-hidden="true"><span class="aiv-sources-freq-fill" style="width:' + barWidth + '%"></span></span><span class="aiv-sources-freq-pct">' + freqDisplay + '</span></span></td>';
        html += '</tr>';
      });
      html += '</tbody></table></div>';
      srcEl.innerHTML = html;
    } else if (srcEl) {
      srcEl.innerHTML = '<p class="aiv-empty-message">No source citations observed this period. Citation data is primarily from Perplexity and grounded providers.</p>';
    }

  }

  // =============================================
  // DETAILED VIEW TAB
  // =============================================
  function renderDetail(d) {
    renderBrief(d.brief, d.property);
    renderDemandCaptureRetired();
    renderLostDemand(d.lostDemand);
    renderCompSet(d.competitiveSet);
    renderWhiteSpace(d.whiteSpace);
    renderRealityGap(d.realityGap);
    renderDetailActions(d.actions);
  }

  function renderBrief(brief, property) {
    var el = document.getElementById("adpBriefSection");
    if (!el || !brief) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Monthly Owner Brief' + info(adpTipPlain('What is the executive summary of this period\u2019s AI demand position?', 'Owners need a quick read of wins, risks, and opportunities before drilling into tables.')) + '</h3></div>';
    html += '<p style="font-size:0.85rem;color:var(--aiv-text-secondary);margin-bottom:1rem">' + esc(property.name) + ' &middot; ' + esc(property.affiliation) + ' &middot; ' + esc(property.city + ", " + property.state) + '</p>';
    html += '<ul class="adp-brief-list">';
    brief.items.forEach(function (item) {
      var iconCls = "icon-" + item.type;
      var icon = item.type === "risk" ? "⚠" : item.type === "opportunity" ? "★" : item.type === "gap" ? "◉" : item.type === "headline" ? "◆" : "ℹ";
      html += '<li class="adp-brief-item"><span class="adp-brief-icon ' + iconCls + '">' + icon + '</span><span>' + esc(item.text) + '</span></li>';
    });
    html += '</ul></div>';
    el.innerHTML = html;
  }

  function renderDemandCaptureRetired() {
    var el = document.getElementById("adpDemandSection");
    if (!el) return;
    el.innerHTML = "";
    el.hidden = true;
  }

  function renderLostDemand(ld) {
    var el = document.getElementById("adpLostSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Lost Demand: Competitive Displacement' + info(adpTipPlain('In which demand scenarios is your property absent, and which hotels appear instead?', 'These are direct leakage points where AI active demand went to competitors.')) + '</h3></div>';
    html += '<div class="adp-hero-kpi" style="margin-bottom:1.25rem">';
    html += '<div><div class="adp-hero-value">' + ld.totalLost + '</div><div class="adp-hero-label">Scenarios Lost</div></div>';
    html += '<div class="adp-hero-context"><strong>' + ld.highRelevanceLost + '</strong> are high-relevance scenarios.</div>';
    html += '</div>';
    if (ld.displacement && ld.displacement.length) {
      html += '<h4 style="font-size:0.8rem;font-weight:700;color:var(--aiv-text-secondary);text-transform:uppercase;margin-bottom:0.5rem">Lost to:</h4>';
      html += '<ul class="adp-displacement-list">';
      ld.displacement.forEach(function (d) {
        html += '<li class="adp-displacement-item"><span class="adp-displacement-name">' + esc(d.name) + '</span><span class="adp-displacement-badge">' + d.displacementCount + ' scenarios</span></li>';
      });
      html += '</ul>';
    }
    if (ld.topReasons && ld.topReasons.length) {
      html += '<div class="adp-reasons"><h4>Top Reasons</h4><ul>';
      ld.topReasons.forEach(function (r) { html += '<li>' + esc(r.reason) + ' (' + r.count + ')</li>'; });
      html += '</ul></div>';
    }
    html += '</div>';
    el.innerHTML = html;
  }

  function renderCompSet(cs) {
    var el = document.getElementById("adpCompSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Observed AI Competitive Set' + info(adpTipPlain('Which hotels does AI name alongside or instead of your property?', 'Shows who AI treats as your real competitive set, including rivals outside your declared comp list.')) + '</h3></div>';
    html += '<div class="adp-comp-stats">';
    html += '<div class="adp-comp-stat"><div class="adp-comp-stat-value">' + cs.declaredCount + '</div><div class="adp-comp-stat-label">Declared</div></div>';
    html += '<div class="adp-comp-stat"><div class="adp-comp-stat-value">' + cs.observedCount + '</div><div class="adp-comp-stat-label">AI Observed</div></div>';
    html += '<div class="adp-comp-stat"><div class="adp-comp-stat-value">' + fmtPct(cs.overlapRate) + '</div><div class="adp-comp-stat-label">Overlap</div></div>';
    html += '</div>';
    if (cs.surprises && cs.surprises.length) {
      html += '<h4 style="font-size:0.8rem;font-weight:700;color:var(--aiv-text-secondary);text-transform:uppercase;margin:1rem 0 0.5rem">Not in your declared set:</h4>';
      cs.surprises.forEach(function (s) {
        html += '<div class="adp-surprise"><span class="adp-surprise-tag">New</span><span>' + esc(s.name) + ', ' + s.scenarioCount + ' scenarios</span></div>';
      });
    }
    html += '</div>';
    el.innerHTML = html;
  }

  function renderWhiteSpace(ws) {
    var el = document.getElementById("adpWhiteSpaceSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Demand White Space' + info(adpTipPlain('Which demand scenarios have weak competitor ownership where your attributes fit?', 'These are opportunities to establish authority before a rival dominates the category.')) + '</h3></div>';
    if (!ws.opportunities || !ws.opportunities.length) {
      html += '<p class="aiv-empty-message">No white space opportunities this period.</p>';
    } else {
      html += '<p style="font-size:0.85rem;color:var(--aiv-text-secondary);margin-bottom:1rem"><strong>' + ws.totalOpportunities + '</strong> opportunities. <strong>' + ws.highOpportunities + '</strong> HIGH.</p>';
      ws.opportunities.forEach(function (o) {
        var cls = o.opportunityScore === "HIGH" ? "" : " opp-medium";
        html += '<div class="adp-opp-card' + cls + '"><div class="adp-opp-header"><span class="adp-opp-intent">' + esc(o.intent.replace(/_/g, " ")) + '</span>';
        html += '<span class="adp-opp-badge badge-' + o.opportunityScore.toLowerCase() + '">' + esc(o.opportunityScore) + '</span></div>';
        html += '<p class="adp-opp-rationale">' + esc(o.rationale) + '</p></div>';
      });
    }
    html += '</div>';
    el.innerHTML = html;
  }

  function renderRealityGap(rg) {
    var el = document.getElementById("adpRealitySection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>AI Reality Gap' + info(adpTipPlain('Does AI accurately recognize your property\u2019s key attributes?', 'If AI misses facts like location, amenities, or positioning, it will favor hotels that are better described.')) + '</h3></div>';
    html += '<div class="adp-hero-kpi" style="margin-bottom:1.25rem">';
    html += '<div><div class="adp-hero-value">' + esc(fmtPct(rg.display)) + '</div><div class="adp-hero-label">Reality Gap</div></div>';
    html += '<div class="adp-hero-context">AI misses <strong>' + rg.gapCount + '</strong> of <strong>' + rg.totalAttributes + '</strong> tracked attributes.</div>';
    html += '</div>';
    html += '<div class="adp-gap-grid">';
    html += '<div class="adp-gap-col col-recognized"><h4>AI Recognizes</h4>';
    (rg.recognized || []).forEach(function (r) {
      html += '<div class="adp-gap-item"><span class="adp-gap-icon icon-ok">✓</span><span>' + esc(r.label) + '</span><span class="adp-gap-pct">' + fmtPct(r.recognitionRate) + '</span></div>';
    });
    html += '</div><div class="adp-gap-col col-missed"><h4>AI Misses</h4>';
    (rg.gaps || []).forEach(function (g) {
      html += '<div class="adp-gap-item"><span class="adp-gap-icon icon-miss">✗</span><span>' + esc(g.label) + '</span><span class="adp-gap-pct">' + fmtPct(g.recognitionRate) + '</span></div>';
    });
    html += '</div></div></div>';
    el.innerHTML = html;
  }

  function renderDetailActions(actions) {
    var el = document.getElementById("adpActionsSection");
    if (!el) return;
    if (!actions || !actions.length) { el.innerHTML = ''; return; }
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Recommended Actions' + info(adpTipPlain('What should owners do next to improve AI demand capture?', 'Turns observed gaps into prioritized steps tied to evidence from this monitoring period.')) + '</h3></div>';
    actions.forEach(function (a) {
      html += '<div class="adp-action-card priority-' + (a.priority || '').toLowerCase() + '">';
      html += '<div class="adp-action-title">' + esc(a.title) + '</div>';
      html += '<div class="adp-action-desc">' + esc(a.description) + '</div>';
      if (a.expectedImpact) html += '<div class="adp-action-impact">Expected: ' + esc(a.expectedImpact) + '</div>';
      html += '</div>';
    });
    html += '</div>';
    el.innerHTML = html;
  }

  // --- Column info tooltips (Brand AI parity) ---
  var columnInfoBound = false;

  function closeColumnInfo() {
    var container = document.getElementById("aivTooltipContainer");
    if (container) container.innerHTML = "";
  }

  function openColumnInfo(tooltipContent) {
    var container = document.getElementById("aivTooltipContainer");
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
    closeBtn.innerHTML = "\u00d7";
    closeBtn.addEventListener("click", function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      closeColumnInfo();
    });
    cloned.appendChild(closeBtn);
    container.appendChild(cloned);
  }

  function bindColumnInfo() {
    if (columnInfoBound) return;
    var container = document.getElementById("aivTooltipContainer");
    if (!container) return;
    columnInfoBound = true;

    function openFromEvent(e) {
      var icon = e.target.closest && e.target.closest(".aiv-page .aiv-col-info .info-icon");
      if (!icon) return;
      e.preventDefault();
      e.stopPropagation();
      var tip = icon.closest(".info-tooltip");
      var content = tip && tip.querySelector(".tooltip-content");
      openColumnInfo(content);
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
        closeColumnInfo();
      }
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        closeColumnInfo();
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
      if (e.target === container) closeColumnInfo();
    });
  }

  // --- Init ---
  document.addEventListener("DOMContentLoaded", function () {
    bindColumnInfo();
    initTabs();
    loadProperties();
    var btn = document.getElementById("adpLoadReport");
    if (btn) btn.addEventListener("click", loadReport);
    var retry = document.getElementById("adpRetry");
    if (retry) retry.addEventListener("click", loadReport);
  });
})();
