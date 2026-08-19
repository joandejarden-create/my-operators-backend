/**
 * AI Demand Positioning — Frontend Renderer.
 * Executive Summary tab mirrors Brand AI Intelligence design.
 * Detailed View tab has full drill-down data.
 */
(function () {
  "use strict";

  var API_BASE = "/api/ai-demand-positioning";
  var currentPayload = null;

  function esc(s) { var d = document.createElement("div"); d.textContent = s || ""; return d.innerHTML; }
  function toProperCase(s) { return (s || "").replace(/\b\w/g, function(c) { return c.toUpperCase(); }); }
  function info(tooltip) { return '<span class="adp-info-icon" data-tooltip="' + esc(tooltip) + '">i</span>'; }

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

  // --- Tab management ---
  function initTabs() {
    var tabs = document.querySelectorAll(".aiv-section-nav [role='tab']");
    tabs.forEach(function (tab) {
      tab.addEventListener("click", function () {
        tabs.forEach(function (t) { t.classList.remove("active"); t.setAttribute("aria-selected", "false"); });
        tab.classList.add("active");
        tab.setAttribute("aria-selected", "true");
        var panel = tab.getAttribute("data-tab");
        document.querySelectorAll("[data-panel]").forEach(function (p) { p.hidden = true; });
        var target = document.querySelector('[data-panel="' + panel + '"]');
        if (target) target.hidden = false;
      });
    });
  }

  // --- Load ---
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
      hideAll(); show("adpStateSuccess");
      renderExecutive(data);
      renderDetail(data);
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
    renderExecInsights(d);
    renderExecKpis(d);
    renderTrends(d);
    renderProviderPresenceTable(d);
    renderExecIntentTable(d.demandCapture);
    renderRealityKpis(d.realityGap);
    renderExecStrengthsGaps(d.realityGap);
    renderExecCompTable(d.competitiveSet);
    renderExecDisplacement(d.lostDemand);
    renderExecActions(d.actions);
    renderExecEvidence(d.evidence);
  }

  function renderExecInsights(d) {
    var el = document.getElementById("adpExecInsights");
    if (!el) return;
    var tiles = [];

    // Tile 1 — AI Demand Capture
    tiles.push(execTile("AI Demand Capture",
      "Your property appears in " + d.demandCapture.capturedScenarios + " of " + d.demandCapture.totalScenarios + " relevant AI demand scenarios across " + d.period.providerCount + " providers. Overall capture rate: " + d.demandCapture.overallRate + "%.",
      "Evidence: " + d.demandCapture.capturedScenarios + "/" + d.demandCapture.totalScenarios + " scenarios captured across " + d.period.providerCount + " providers."));

    // Tile 2 — Largest Displacement
    if (d.lostDemand.displacement.length) {
      var top = d.lostDemand.displacement[0];
      tiles.push(execTile("Largest Competitive Displacement",
        top.name + " appears in " + top.displacementCount + " scenarios where your property is absent. This is your primary competitive threat in AI-driven demand.",
        "Evidence: " + top.name + " displaces in " + top.displacementCount + " of " + d.demandCapture.totalScenarios + " monitored scenarios."));
    }

    // Tile 3 — Demand Opportunities
    if (d.whiteSpace.highOpportunities > 0) {
      tiles.push(execTile("Demand White Space",
        d.whiteSpace.highOpportunities + " high-opportunity demand scenarios identified where no competitor dominates and your property attributes align. These represent your strongest growth targets.",
        "Evidence: " + d.whiteSpace.highOpportunities + " HIGH opportunity scenarios out of " + d.whiteSpace.opportunities.length + " total white-space scenarios."));
    }

    // Tile 4 — Reality Gap
    tiles.push(execTile("Reality Gap",
      "AI misses or underrepresents " + d.realityGap.gapCount + " of your " + d.realityGap.totalAttributes + " tracked property attributes. Closing this gap would increase demand capture across all providers.",
      "Evidence: " + d.realityGap.gapCount + "/" + d.realityGap.totalAttributes + " attributes unrecognized or underrepresented."));

    el.setAttribute("data-count", String(tiles.length));
    el.innerHTML = tiles.join("");
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
    var dc = d.demandCapture, ld = d.lostDemand, rg = d.realityGap, ws = d.whiteSpace, cs = d.competitiveSet;

    var bestIntent = null, bestRate = 0;
    Object.entries(dc.byIntent).forEach(function(e) { if (e[1].rate > bestRate) { bestRate = e[1].rate; bestIntent = e[0]; } });

    el.innerHTML =
      kpiCard("AI Demand Capture", dc.display, "Share of monitored demand scenarios where at least one AI provider mentioned your property.") +
      kpiCard("Scenarios Monitored", dc.totalScenarios + " × " + d.period.providerCount + " providers", "How many demand scenarios were tested across how many AI providers this period.") +
      kpiCard("Strongest Segment", bestIntent ? bestIntent.replace(/_/g, " ").replace(/\b\w/g, function(c){return c.toUpperCase();}) : "—", "The traveler intent category with the highest capture rate for your property.") +
      kpiCard("Top AI Competitor", cs.observed && cs.observed.length ? cs.observed[0].name : "—", "The hotel that appears most frequently across all AI demand scenarios — your strongest competitor in AI.") +
      kpiCard("Questions Missing", (100 - dc.overallRate).toFixed(1) + "% (" + dc.missedScenarios + ")", "Monitored demand scenarios where your property was not mentioned in the AI answer.");
  }

  function kpiCard(label, value, helpText) {
    return '<article class="aiv-kpi"><h3>' + esc(label) +
      '</h3><div class="aiv-value">' + esc(value) +
      '</div><div class="aiv-meta">' + esc(helpText) +
      '</div></article>';
  }

  function kpiStatus(rate) { return rate >= 50 ? "good" : rate >= 30 ? "warning" : "critical"; }

  var SORT_ARROWS = '<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>';

  function thCol(label, tooltip) {
    return '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">' + label + '</span>' + SORT_ARROWS + (tooltip ? '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="Info"><svg width="12" height="12" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="tooltip-content" hidden>' + tooltip + '</div></span>' : '') + '</span></th>';
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
    var data = trends.map(function(t) { return t.demandCaptureRate != null ? Math.round(t.demandCaptureRate * 10) / 10 : null; });

    var current = data[data.length - 1];
    var prior = data[data.length - 2];
    var change = (current != null && prior != null) ? Math.round((current - prior) * 10) / 10 : null;

    if (summaryEl) {
      summaryEl.hidden = false;
      summaryEl.innerHTML =
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Current</div><div class="aiv-detail-trend-stat__value">' + (current != null ? current + "%" : "\u2014") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Prior</div><div class="aiv-detail-trend-stat__value">' + (prior != null ? prior + "%" : "\u2014") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Change</div><div class="aiv-detail-trend-stat__value">' + (change == null ? "\u2014" : (change > 0 ? "+" : "") + change + " pp") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Periods</div><div class="aiv-detail-trend-stat__value">' + trends.length + '</div></div>';
    }

    if (!canvas || typeof window.Chart !== "function") {
      if (chartWrap) chartWrap.hidden = true;
      emptyEl.hidden = false;
      emptyEl.innerHTML = '<p class="aiv-empty__message">Chart library unavailable \u2014 refresh the page.</p>';
      return;
    }

    emptyEl.hidden = true;
    if (chartWrap) chartWrap.hidden = false;
    canvas.style.display = "block";

    adpTrendChart = new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [{
          label: "Demand Capture",
          data: data,
          borderColor: "#6c72ff",
          backgroundColor: "#6c72ff",
          fill: false,
          tension: 0.4,
          borderWidth: 2,
          pointRadius: 4,
          pointHoverRadius: 6,
          pointBackgroundColor: "#6c72ff",
          pointBorderColor: "#fff",
          pointBorderWidth: 2,
          pointHoverBackgroundColor: "#6c72ff",
          pointHoverBorderColor: "#fff",
          pointHoverBorderWidth: 2,
          spanGaps: false
        }]
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
            backgroundColor: "rgba(255,255,255,0.95)",
            titleColor: "#080f25",
            bodyColor: "#37446b",
            borderColor: "#d9e1fa",
            borderWidth: 1,
            padding: 12,
            cornerRadius: 6,
            displayColors: true,
            boxPadding: 4,
            callbacks: {
              title: function(items) { return items && items.length ? String(items[0].label || "") : ""; },
              label: function(ctx) {
                var y = ctx.parsed && ctx.parsed.y;
                if (y == null || !isFinite(y)) return " Demand Capture: \u2014";
                return " Demand Capture: " + y + "%";
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
              callback: function(value) { return value + "%"; }
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
    html += thCol('Provider', '');
    html += thCol('Status', '');
    html += thCol('AI<br>Presence', 'Share of monitored scenarios where this provider mentioned your property.');
    html += thCol('Monitored', 'Total scenarios successfully queried on this provider.');
    html += thCol('Missing', 'Scenarios where this provider did not mention your property.');
    html += thCol('Citation', 'Share of this provider\u2019s responses that included source citations.');
    html += thCol('Owned', 'Share of citations pointing to your governed owned domains. Requires owned domain configuration.');
    html += '</tr></thead><tbody>';

    ev.providers.forEach(function(p) {
      var name = p.provider.charAt(0).toUpperCase() + p.provider.slice(1);
      var pct = p.presence;
      var missing = p.total - p.mentioned;
      var presenceVisual = '<span class="aiv-presence-cell aiv-presence-cell--compact"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' + Math.round(pct) + '%"></span></span><span class="aiv-presence-cell__value">' + pct + '%</span></span>';

      // Citation rate for this provider
      var citRate = '0.0%';
      if (ev.providerCitations && ev.providerCitations[p.provider]) {
        citRate = ev.providerCitations[p.provider].toFixed(1) + '%';
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
    html += thCol('Intent<br>Category', 'The traveler demand category being measured \u2014 e.g. business, leisure, couples, meetings.');
    html += thCol('Your<br>Presence', 'Share of monitored scenarios in this intent where at least one AI provider mentioned your property.');
    html += thCol('\u0394 vs<br>Prior Run', 'Change in presence vs the previous monitoring period. Requires at least two comparable periods.');
    html += thCol('Monitored', 'How many scenario \u00d7 provider observations were monitored for this intent category.');
    html += thCol('Missing', 'Scenarios in this intent where your property was not mentioned by any provider.');
    html += thCol('Peer-Present<br>Gaps', 'Scenarios where at least one competitor appears but your property does not \u2014 the competitive displacement signal.');
    html += thCol('AI Presence<br>Index', 'Indexed position vs benchmark. 100 = at parity. Requires benchmark development.');
    html += thCol('Missing<br>Evidence', 'View AI response excerpts for scenarios where your property was not mentioned in this intent category.');
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

      // Presence bar visual
      var presenceVisual = '<span class="aiv-presence-cell aiv-presence-cell--compact"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' + Math.round(pct) + '%"></span></span><span class="aiv-presence-cell__value">' + pct + '%</span></span>';

      html += '<tr class="aiv-intent-row aiv-unified-intent-row">';
      html += '<td><span class="project-name-text">' + esc(label) + '</span></td>';
      html += '<td class="aiv-metric-cell aiv-presence-metric-cell">' + presenceVisual + '</td>';
      html += '<td class="aiv-metric-cell aiv-delta-cell aiv-chg-metric-cell">' + (window.AiVisibilityUi ? AiVisibilityUi.formatDeltaCell({ availability: "insufficient_history" }) : '<span class="aiv-avail-insufficient_history aiv-delta-none">Insufficient History</span>') + '</td>';
      html += '<td class="aiv-metric-cell aiv-monitored-cell">' + monitored + '</td>';
      html += '<td class="aiv-metric-cell">' + missing + '</td>';
      html += '<td class="aiv-metric-cell">' + peerGaps + '</td>';
      var idxData = currentPayload && currentPayload.intentPresenceIndex && currentPayload.intentPresenceIndex[intent];
      if (idxData && idxData.index !== null) {
        var idxVal = idxData.index;
        var idxClass = idxVal >= 100 ? 'aiv-delta-positive' : 'aiv-delta-negative';
        html += '<td class="aiv-metric-cell">' + idxVal + '</td>';
      } else {
        html += '<td class="aiv-metric-cell aiv-delta-cell"><span class="aiv-avail-insufficient_history aiv-delta-none">Benchmark still developing</span></td>';
      }
      // Missing Evidence link
      if (missing > 0) {
        html += '<td class="aiv-metric-cell"><button type="button" class="aiv-btn-text aiv-link" data-adp-evidence-intent="' + esc(intent) + '">' + missing + ' Missing</button></td>';
      } else {
        html += '<td class="aiv-metric-cell"><span style="color:var(--aiv-text-secondary)">—</span></td>';
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
  }

  function openAdpEvidence(intent) {
    var drawer = document.getElementById("adpEvidenceDrawer");
    var body = document.getElementById("adpEvidenceBody");
    if (!drawer || !body) return;
    body.innerHTML = '<div class="aiv-empty">Loading evidence\u2026</div>';
    if (typeof drawer.showModal === "function" && !drawer.open) drawer.showModal();

    var pid = currentPayload && currentPayload.property && currentPayload.property.propertyId || "adp_waterstone_boca_raton";
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
          var providerLine = (ev.provider || "unknown").charAt(0).toUpperCase() + (ev.provider || "unknown").slice(1);
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
    var gapPct = total > 0 ? Math.round((gaps / total) * 100) : 0;
    var recognitionPct = total > 0 ? Math.round((recognized / total) * 100) : 0;
    var avgRecognition = rg.recognized && rg.recognized.length
      ? Math.round(rg.recognized.reduce(function(sum, r) { return sum + r.recognitionRate; }, 0) / rg.recognized.length)
      : 0;

    el.innerHTML =
      '<div class="aiv-kpi-row aiv-citation-kpis">' +
      '<article class="aiv-kpi"><h3>Reality Gap</h3><div class="aiv-value">' + gapPct + '%</div><div class="aiv-meta">AI misses ' + gaps + ' of ' + total + ' tracked attributes</div></article>' +
      '<article class="aiv-kpi"><h3>Recognition Rate</h3><div class="aiv-value">' + recognitionPct + '%</div><div class="aiv-meta">' + recognized + ' of ' + total + ' attributes recognized</div></article>' +
      '<article class="aiv-kpi"><h3>Avg Strength</h3><div class="aiv-value">' + avgRecognition + '%</div><div class="aiv-meta">Average recognition rate across recognized attributes</div></article>' +
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
          return '<li class="aiv-signal-item aiv-signal-item--strength"><span class="aiv-signal-icon">✓</span><span class="aiv-signal-text">' + esc(toProperCase(r.label)) + '</span><span class="aiv-signal-badge">' + r.recognitionRate + '%</span></li>';
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

  function renderExecCompTable(cs) {
    var el = document.getElementById("adpCompTableBody");
    var countEl = document.querySelector("#adpCompCount .results-count");
    if (!el) return;

    var totalScenarios = currentPayload ? currentPayload.demandCapture.totalScenarios : 65;
    var propertyCapture = currentPayload ? currentPayload.demandCapture.capturedScenarios : 0;
    var observed = cs.observed || [];
    var displacement = (currentPayload && currentPayload.lostDemand && currentPayload.lostDemand.displacement) || [];

    if (countEl) countEl.textContent = "Showing " + Math.min(observed.length, 15) + " of " + observed.length + " competitors";

    // Insert subject property into the ranked list
    var propertyName = currentPayload && currentPayload.property ? currentPayload.property.name : "Your Property";
    var propertyPresence = currentPayload ? currentPayload.demandCapture.overallRate : 0;
    var propertyScenarios = currentPayload ? currentPayload.demandCapture.capturedScenarios : 0;

    var ranked = observed.slice();
    var subjectEntry = { name: propertyName, scenarioCount: propertyScenarios, isSubject: true, isCore: false };
    // Insert subject at correct rank position
    var subjectRank = 0;
    for (var i = 0; i < ranked.length; i++) {
      if (propertyScenarios >= ranked[i].scenarioCount) { subjectRank = i; break; }
      subjectRank = i + 1;
    }
    ranked.splice(subjectRank, 0, subjectEntry);

    // Show top 10, but always include subject property
    var displayList;
    if (subjectRank < 10) {
      displayList = ranked.slice(0, 10);
    } else {
      displayList = ranked.slice(0, 9);
      displayList.push(ranked[subjectRank]);
    }

    if (countEl) countEl.textContent = "Showing " + displayList.length + " of " + (observed.length + 1) + " competitors";

    var html = "";
    displayList.forEach(function (c) {
      var actualRank = ranked.indexOf(c) + 1;
      var presence = totalScenarios > 0 ? ((c.scenarioCount / totalScenarios) * 100).toFixed(1) + "%" : "—";
      var dispEntry = displacement.find(function(d) { return d.name === c.name; });
      var dispCount = dispEntry ? dispEntry.displacementCount : 0;
      var territory;
      if (c.isSubject) {
        var bestIntent = null, bestRate = 0;
        if (currentPayload && currentPayload.demandCapture && currentPayload.demandCapture.byIntent) {
          Object.entries(currentPayload.demandCapture.byIntent).forEach(function(e) { if (e[1].rate > bestRate) { bestRate = e[1].rate; bestIntent = e[0]; } });
        }
        territory = bestIntent ? bestIntent.replace(/_/g, " ").replace(/\b\w/g, function(ch){return ch.toUpperCase();}) : "—";
      } else {
        territory = inferTopTerritory(c.name);
      }
      var namePill = c.isSubject
        ? '<strong>' + esc(c.name) + '</strong> <span class="adp-pill adp-pill--you">You</span>'
        : '<strong>' + esc(c.name) + '</strong> ' + (c.isCore ? '<span class="adp-pill adp-pill--core">Core</span>' : '<span class="adp-pill adp-pill--extended">Extended</span>');

      var rowClass = c.isSubject ? ' class="adp-row--subject"' : '';
      html += '<tr' + rowClass + '>' +
        '<td>#' + actualRank + '</td>' +
        '<td>' + namePill + '</td>' +
        '<td>' + presence + '</td>' +
        '<td class="aiv-metric-cell aiv-delta-cell aiv-chg-metric-cell"><span class="aiv-avail-insufficient_history aiv-delta-none">—</span></td>' +
        '<td>' + (c.isSubject ? '<span style="color:var(--aiv-text-secondary)">—</span>' : (dispCount > 0 ? '<button type="button" class="aiv-btn-text aiv-link" data-adp-displacement="' + esc(c.name) + '">' + dispCount + ' Scenarios</button>' : '<span style="color:var(--aiv-text-secondary)">—</span>')) + '</td>' +
        '<td>' + (c.isSubject ? '—' : ((c.scenarioCount / totalScenarios) * 100).toFixed(1) + '% (' + c.scenarioCount + ')') + '</td>' +
        '<td>' + esc(territory) + '</td>' +
        '</tr>';
    });
    el.innerHTML = html;

    // Wire displacement evidence links
    el.querySelectorAll("[data-adp-displacement]").forEach(function(btn) {
      btn.addEventListener("click", function() {
        openAdpDisplacementEvidence(btn.getAttribute("data-adp-displacement"));
      });
    });
  }

  function openAdpDisplacementEvidence(competitorName) {
    var drawer = document.getElementById("adpEvidenceDrawer");
    var body = document.getElementById("adpEvidenceBody");
    if (!drawer || !body) return;
    body.innerHTML = '<div class="aiv-empty">Loading displacement evidence\u2026</div>';
    if (typeof drawer.showModal === "function" && !drawer.open) drawer.showModal();

    var pid = currentPayload && currentPayload.property && currentPayload.property.propertyId || "adp_waterstone_boca_raton";
    fetch("/api/ai-demand-positioning/property/" + encodeURIComponent(pid) + "/evidence?type=displacement&competitor=" + encodeURIComponent(competitorName))
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (!data.ok || !data.evidence || !data.evidence.length) {
          body.innerHTML = '<div class="aiv-empty">No displacement evidence available for ' + esc(competitorName) + '.</div>';
          return;
        }
        var html = '<p class="aiv-theme-help help-text" style="margin-bottom:1rem;">Scenarios where <strong>' + esc(competitorName) + '</strong> appears but your property does not.</p>';
        data.evidence.forEach(function(ev) {
          var providerLine = (ev.provider || "unknown").charAt(0).toUpperCase() + (ev.provider || "unknown").slice(1);
          var citations = ev.sourcesCited || ev.providerCitations || [];
          var competitors = ev.competitorsMentioned || [];

          html += '<div class="aiv-evidence">' +
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
            '<div class="aiv-evidence-value">' + esc(competitorName) + '</div></div>' +
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
      html += '<article class="aiv-kpi"><h3>Citation Coverage</h3><div class="aiv-value">' + esc(ev.totalWithSources + ' of ' + ev.totalObservations) + '</div><div class="aiv-meta">' + esc(ev.citationRate + '% of monitored responses included at least one citation.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>Avg Sources per Citation</h3><div class="aiv-value">' + esc(String(ev.avgSourcesPerCitation)) + '</div><div class="aiv-meta">Average number of sources cited per grounded response.</div></article>';
      html += '<article class="aiv-kpi"><h3>Top Source</h3><div class="aiv-value aiv-value--domain">' + esc(topSource ? topSource.domain : '—') + '</div><div class="aiv-meta">' + (topSource ? esc(topSource.count + ' of ' + ev.totalWithSources + ' · ' + topSource.frequency + '%') : 'No sources observed.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>Second Source</h3><div class="aiv-value aiv-value--domain">' + esc(secondSource ? secondSource.domain : '—') + '</div><div class="aiv-meta">' + (secondSource ? esc(secondSource.count + ' of ' + ev.totalWithSources + ' · ' + secondSource.frequency + '%') : 'No second source observed.') + '</div></article>';
      html += '</div>';

      // Source Mix — 4 cards: Owned, External, With Citations, No Citations
      var noCit = ev.totalObservations - ev.totalWithSources;
      var citPct = ev.totalObservations > 0 ? ((ev.totalWithSources / ev.totalObservations) * 100).toFixed(1) + '%' : '0.0%';
      var noCitPct = ev.totalObservations > 0 ? ((noCit / ev.totalObservations) * 100).toFixed(1) + '%' : '0.0%';
      var extPct = ev.totalWithSources > 0 ? '100.0%' : '0.0%';
      html += '<div class="aiv-source-mix aiv-source-mix--compact">';
      html += '<div class="aiv-source-mix__head"><div class="aiv-source-mix__label">Source Mix</div></div>';
      html += '<div class="aiv-kpi-row aiv-theme-kpis" style="grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
      html += '<article class="aiv-kpi"><h3>Owned Sources</h3><div class="aiv-value">0.0%</div><div class="aiv-meta">No governed owned domains configured yet.</div></article>';
      html += '<article class="aiv-kpi"><h3>External Sources</h3><div class="aiv-value">' + extPct + '</div><div class="aiv-meta">All cited sources are external.</div></article>';
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
      html += '<th><span class="aiv-th-label"><span class="aiv-th-text">Source</span></span></th>';
      html += '<th><span class="aiv-th-label"><span class="aiv-th-text">Responses<br>Citing</span></span></th>';
      html += '<th><span class="aiv-th-label"><span class="aiv-th-text">Frequency</span></span></th>';
      html += '</tr></thead><tbody>';
      ev.topSources.forEach(function(s) {
        var barWidth = Math.min(s.frequency, 100);
        html += '<tr>';
        html += '<td><span class="aiv-sources-freq-domain"><a class="aiv-source-link project-name-text" href="https://' + esc(s.domain) + '" target="_blank" rel="noopener noreferrer">' + esc(s.domain) + '</a></span></td>';
        html += '<td class="aiv-metric-cell">' + s.count + ' of ' + ev.totalWithSources + '</td>';
        var freqDisplay = Number(s.frequency).toFixed(1);
        html += '<td class="aiv-metric-cell"><span class="aiv-sources-freq-bar"><span class="aiv-sources-freq-track" aria-hidden="true"><span class="aiv-sources-freq-fill" style="width:' + barWidth + '%"></span></span><span class="aiv-sources-freq-pct">' + freqDisplay + '%</span></span></td>';
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
    renderDemandCapture(d.demandCapture);
    renderLostDemand(d.lostDemand);
    renderCompSet(d.competitiveSet);
    renderWhiteSpace(d.whiteSpace);
    renderRealityGap(d.realityGap);
    renderDetailActions(d.actions);
  }

  function renderBrief(brief, property) {
    var el = document.getElementById("adpBriefSection");
    if (!el || !brief) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Monthly Owner Brief' + info("Summary of your property's AI demand position this monitoring period.") + '</h3></div>';
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

  function renderDemandCapture(dc) {
    var el = document.getElementById("adpDemandSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>AI Demand Capture Index' + info("Percentage of tested demand scenarios where at least one AI provider mentions your property.") + '</h3></div>';
    html += '<div class="adp-hero-kpi">';
    html += '<div><div class="adp-hero-value">' + esc(dc.display) + '</div><div class="adp-hero-label">Demand Capture Rate</div></div>';
    html += '<div class="adp-hero-context"><strong>' + dc.capturedScenarios + '</strong> of <strong>' + dc.totalScenarios + '</strong> relevant scenarios include your property.<br><strong>' + dc.missedScenarios + '</strong> scenarios where you do not appear.</div>';
    html += '</div>';
    html += '<div class="adp-intent-grid">';
    Object.entries(dc.byIntent).forEach(function (e) {
      var status = kpiStatus(e[1].rate);
      html += '<div class="adp-intent-card"><div class="adp-intent-label">' + esc(e[0].replace(/_/g, " ")) + '</div>';
      html += '<div class="adp-intent-value ' + status + '">' + e[1].rate + '%</div>';
      html += '<div class="adp-intent-detail">' + e[1].captured + ' of ' + e[1].total + '</div></div>';
    });
    html += '</div></div>';
    el.innerHTML = html;
  }

  function renderLostDemand(ld) {
    var el = document.getElementById("adpLostSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Lost Demand — Competitive Displacement' + info("Scenarios where your property is absent. Shows who appears instead and likely reasons.") + '</h3></div>';
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
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Observed AI Competitive Set' + info("Hotels that AI mentions alongside or instead of your property.") + '</h3></div>';
    html += '<div class="adp-comp-stats">';
    html += '<div class="adp-comp-stat"><div class="adp-comp-stat-value">' + cs.declaredCount + '</div><div class="adp-comp-stat-label">Declared</div></div>';
    html += '<div class="adp-comp-stat"><div class="adp-comp-stat-value">' + cs.observedCount + '</div><div class="adp-comp-stat-label">AI Observed</div></div>';
    html += '<div class="adp-comp-stat"><div class="adp-comp-stat-value">' + cs.overlapRate + '%</div><div class="adp-comp-stat-label">Overlap</div></div>';
    html += '</div>';
    if (cs.surprises && cs.surprises.length) {
      html += '<h4 style="font-size:0.8rem;font-weight:700;color:var(--aiv-text-secondary);text-transform:uppercase;margin:1rem 0 0.5rem">Not in your declared set:</h4>';
      cs.surprises.forEach(function (s) {
        html += '<div class="adp-surprise"><span class="adp-surprise-tag">New</span><span>' + esc(s.name) + ' — ' + s.scenarioCount + ' scenarios</span></div>';
      });
    }
    html += '</div>';
    el.innerHTML = html;
  }

  function renderWhiteSpace(ws) {
    var el = document.getElementById("adpWhiteSpaceSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Demand White Space' + info("Demand scenarios with weak competitor ownership where your property attributes align.") + '</h3></div>';
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
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>AI Reality Gap' + info("Compares actual property attributes vs what AI recognizes.") + '</h3></div>';
    html += '<div class="adp-hero-kpi" style="margin-bottom:1.25rem">';
    html += '<div><div class="adp-hero-value">' + esc(rg.display) + '</div><div class="adp-hero-label">Reality Gap</div></div>';
    html += '<div class="adp-hero-context">AI misses <strong>' + rg.gapCount + '</strong> of <strong>' + rg.totalAttributes + '</strong> tracked attributes.</div>';
    html += '</div>';
    html += '<div class="adp-gap-grid">';
    html += '<div class="adp-gap-col col-recognized"><h4>AI Recognizes</h4>';
    (rg.recognized || []).forEach(function (r) {
      html += '<div class="adp-gap-item"><span class="adp-gap-icon icon-ok">✓</span><span>' + esc(r.label) + '</span><span class="adp-gap-pct">' + r.recognitionRate + '%</span></div>';
    });
    html += '</div><div class="adp-gap-col col-missed"><h4>AI Misses</h4>';
    (rg.gaps || []).forEach(function (g) {
      html += '<div class="adp-gap-item"><span class="adp-gap-icon icon-miss">✗</span><span>' + esc(g.label) + '</span><span class="adp-gap-pct">' + g.recognitionRate + '%</span></div>';
    });
    html += '</div></div></div>';
    el.innerHTML = html;
  }

  function renderDetailActions(actions) {
    var el = document.getElementById("adpActionsSection");
    if (!el) return;
    if (!actions || !actions.length) { el.innerHTML = ''; return; }
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>Recommended Actions' + info("Data-driven actions to improve AI demand position.") + '</h3></div>';
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

  // --- Init ---
  document.addEventListener("DOMContentLoaded", function () {
    initTabs();
    loadProperties();
    var btn = document.getElementById("adpLoadReport");
    if (btn) btn.addEventListener("click", loadReport);
    var retry = document.getElementById("adpRetry");
    if (retry) retry.addEventListener("click", loadReport);
  });
})();
