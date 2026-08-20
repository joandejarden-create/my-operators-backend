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

  /** Info tooltip copy: question the metric answers + why it matters. */
  function adpTipHtml(title, question, why) {
    return "<strong>" + title + "</strong><br><br><strong>Question:</strong> " + question +
      "<br><br><strong>Why track it:</strong> " + why;
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
    tiles.push(execTile(
      "AI Demand Capture",
      "Your property appears in " +
        d.demandCapture.capturedScenarios +
        " of " +
        d.demandCapture.totalScenarios +
        " demand scenarios (" +
        fmtPct(d.demandCapture.overallRate) +
        ") across " +
        d.period.providerCount +
        " providers. That is your AI consideration share this period. The gap is demand currently going to more visible competitors.",
      "Evidence: " + d.demandCapture.capturedScenarios + "/" + d.demandCapture.totalScenarios + " scenarios captured across " + d.period.providerCount + " providers."
    ));

    // Tile 2 — Largest Displacement
    if (d.lostDemand.displacement.length) {
      var top = d.lostDemand.displacement[0];
      tiles.push(execTile(
        "Largest Competitive Displacement",
        top.name +
          " displaces you in " +
          top.displacementCount +
          " scenarios where your property is absent. That is the largest single-competitor leakage pattern. Protecting those moments limits share loss to this rival.",
        "Evidence: " + top.name + " displaces in " + top.displacementCount + " of " + d.demandCapture.totalScenarios + " monitored scenarios."
      ));
    }

    // Tile 3 — Demand Opportunities
    if (d.whiteSpace.highOpportunities > 0) {
      tiles.push(execTile(
        "Demand White Space",
        d.whiteSpace.highOpportunities +
          " scenarios show high white-space with no dominant competitor. Your attributes already match this intent. Prioritizing content and sources here is a practical capture lift.",
        "Evidence: " + d.whiteSpace.highOpportunities + " HIGH opportunity scenarios out of " + d.whiteSpace.opportunities.length + " total white-space scenarios."
      ));
    }

    // Tile 4 — Reality Gap
    tiles.push(execTile(
      "Reality Gap",
      "AI underrecognizes " +
        d.realityGap.gapCount +
        " of " +
        d.realityGap.totalAttributes +
        " tracked attributes. Missing facts push recommendations toward better-described rivals. Closing these gaps strengthens selection in high-value checks.",
      "Evidence: " + d.realityGap.gapCount + "/" + d.realityGap.totalAttributes + " attributes unrecognized or underrepresented."
    ));

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
      kpiCard("AI Demand Capture", fmtPct(dc.display), "Question: When travelers ask AI for hotels in monitored scenarios, how often does your property appear? Why track it: This is your headline demand win rate.") +
      kpiCard("Scenarios Monitored", dc.totalScenarios + " × " + d.period.providerCount + " providers", "Question: How much demand did we test this period? Why track it: Larger, consistent monitoring gives more reliable capture and gap signals.") +
      kpiCard("Strongest Segment", bestIntent ? bestIntent.replace(/_/g, " ").replace(/\b\w/g, function(c){return c.toUpperCase();}) : "—", "Question: Which traveler intent does AI recommend you for most often? Why track it: Shows where you already win demand and where to defend share.") +
      kpiCard("Top AI Competitor", cs.observed && cs.observed.length ? cs.observed[0].name : "—", "Question: Which hotel appears most often across AI demand answers? Why track it: This is the rival most likely to capture demand you miss.") +
      kpiCard("Questions Missing", fmtPct(100 - dc.overallRate) + " (" + dc.missedScenarios + ")", "Question: In how many demand scenarios did AI fail to mention your property? Why track it: Each missing scenario is a traveler need you did not win.");
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
    return '<th class="no-sort"><span class="aiv-th-label"><span class="aiv-th-text">' + label + '</span>' + SORT_ARROWS + (tooltip ? '<span class="info-tooltip aiv-col-info"><span class="info-icon" role="button" tabindex="0" aria-label="Info"><svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg></span><div class="tooltip-content" hidden>' + tooltip + '</div></span>' : '') + '</span></th>';
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
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Current</div><div class="aiv-detail-trend-stat__value">' + (current != null ? fmtPct(current) : "\u2014") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Prior</div><div class="aiv-detail-trend-stat__value">' + (prior != null ? fmtPct(prior) : "\u2014") + '</div></div>' +
        '<div class="aiv-detail-trend-stat"><div class="aiv-detail-trend-stat__label">Change</div><div class="aiv-detail-trend-stat__value">' + (change == null ? "\u2014" : (change > 0 ? "+" : "") + change + " pp") + '</div></div>' +
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
                return " Demand Capture: " + fmtPct(y);
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
      var name = p.provider.charAt(0).toUpperCase() + p.provider.slice(1);
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
    html += thCol('Intent<br>Category', adpTipHtml('Intent Category', 'Which type of traveler demand is being measured (business, leisure, couples, meetings, and so on)?', 'Capture varies by trip purpose. You need to know where AI recommends you and where demand leaks by intent.'));
    html += thCol('Your<br>Presence', adpTipHtml('Your Presence', 'For this intent, in what share of demand scenarios did at least one AI provider mention your property?', 'This is your headline capture rate for that traveler need. Gaps here mean lost consideration for that trip type.'));
    html += thCol('\u0394 vs<br>Prior Run', adpTipHtml('\u0394 vs Prior Run', 'Is your presence in this intent improving or declining compared with the last monitoring run?', 'A single period does not show direction. Tracking change tells you whether recent content or positioning work is working.'));
    html += thCol('Monitored', adpTipHtml('Monitored', 'How many demand scenarios in this intent were tested, and how many captured your property?', 'Confirms sample size and shows captured versus total for this intent category.'));
    html += thCol('Missing', adpTipHtml('Missing', 'In this intent, how many scenarios had no AI mention of your property?', 'These are concrete lost-demand moments for that traveler type.'));
    html += thCol('Peer-Present<br>Gaps', adpTipHtml('Peer-Present Gaps', 'In this intent, how often do competitors appear in scenarios where you do not?', 'This is competitive displacement: rivals are winning the recommendation when you are absent.'));
    html += thCol('AI Presence<br>Index', adpTipHtml('AI Presence Index', 'How does your presence in this intent compare with your declared comp set?', 'Only shown when at least three declared comps appear often enough in this intent (30%+ average). Capped at 200. If comps rarely appear, the benchmark is too thin to compare fairly.'));
    html += thCol('Missing<br>Evidence', adpTipHtml('Missing Evidence', 'What did AI actually say in scenarios where you were missing from this intent?', 'Lets you read real responses and diagnose why competitors were chosen instead.'));
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
      var presenceVisual = '<span class="aiv-presence-cell aiv-presence-cell--compact"><span class="aiv-presence-cell__bar" aria-hidden="true"><span class="aiv-presence-cell__fill" style="width:' + Math.round(pct) + '%"></span></span><span class="aiv-presence-cell__value">' + fmtPct(pct) + '</span></span>';

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
    var gapPct = total > 0 ? (gaps / total) * 100 : 0;
    var recognitionPct = total > 0 ? (recognized / total) * 100 : 0;
    var avgRecognition = rg.recognized && rg.recognized.length
      ? rg.recognized.reduce(function(sum, r) { return sum + r.recognitionRate; }, 0) / rg.recognized.length
      : 0;

    el.innerHTML =
      '<div class="aiv-kpi-row aiv-citation-kpis">' +
      '<article class="aiv-kpi"><h3>Reality Gap</h3><div class="aiv-value">' + fmtPct(gapPct) + '</div><div class="aiv-meta">AI misses ' + gaps + ' of ' + total + ' tracked attributes</div></article>' +
      '<article class="aiv-kpi"><h3>Recognition Rate</h3><div class="aiv-value">' + fmtPct(recognitionPct) + '</div><div class="aiv-meta">' + recognized + ' of ' + total + ' attributes recognized</div></article>' +
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
      var presence = totalScenarios > 0 ? fmtPctFromRatio(c.scenarioCount / totalScenarios) : "—";
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
        '<td>' + (c.isSubject ? '—' : (fmtPctFromRatio(c.scenarioCount / totalScenarios) + ' (' + c.scenarioCount + ')')) + '</td>' +
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

    var pid = getActivePropertyId();
    if (!pid) {
      body.innerHTML = '<div class="aiv-empty">Select a property to view evidence.</div>';
      return;
    }
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
      html += '<article class="aiv-kpi"><h3>Citation Coverage</h3><div class="aiv-value">' + esc(ev.totalWithSources + ' of ' + ev.totalObservations) + '</div><div class="aiv-meta">' + esc(fmtPct(ev.citationRate) + ' of monitored responses included at least one citation.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>Avg Sources per Citation</h3><div class="aiv-value">' + esc(String(ev.avgSourcesPerCitation)) + '</div><div class="aiv-meta">Average number of sources cited per grounded response.</div></article>';
      html += '<article class="aiv-kpi"><h3>Top Source</h3><div class="aiv-value aiv-value--domain">' + esc(topSource ? topSource.domain : '—') + '</div><div class="aiv-meta">' + (topSource ? esc(topSource.count + ' of ' + ev.totalWithSources + ' · ' + fmtPct(topSource.frequency)) : 'No sources observed.') + '</div></article>';
      html += '<article class="aiv-kpi"><h3>Second Source</h3><div class="aiv-value aiv-value--domain">' + esc(secondSource ? secondSource.domain : '—') + '</div><div class="aiv-meta">' + (secondSource ? esc(secondSource.count + ' of ' + ev.totalWithSources + ' · ' + fmtPct(secondSource.frequency)) : 'No second source observed.') + '</div></article>';
      html += '</div>';

      // Source Mix — 4 cards: Owned, External, With Citations, No Citations
      var noCit = ev.totalObservations - ev.totalWithSources;
      var citPct = ev.totalObservations > 0 ? fmtPctFromRatio(ev.totalWithSources / ev.totalObservations) : '0.0%';
      var noCitPct = ev.totalObservations > 0 ? fmtPctFromRatio(noCit / ev.totalObservations) : '0.0%';
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

  function renderDemandCapture(dc) {
    var el = document.getElementById("adpDemandSection");
    if (!el) return;
    var html = '<div class="adp-card"><div class="adp-card-header"><h3>AI Demand Capture Index' + info(adpTipPlain('When travelers ask AI for hotel recommendations in monitored demand scenarios, how often does your property appear?', 'This is the headline demand win rate. Low capture means AI is sending travelers elsewhere.')) + '</h3></div>';
    html += '<div class="adp-hero-kpi">';
    html += '<div><div class="adp-hero-value">' + esc(fmtPct(dc.display)) + '</div><div class="adp-hero-label">Demand Capture Rate</div></div>';
    html += '<div class="adp-hero-context"><strong>' + dc.capturedScenarios + '</strong> of <strong>' + dc.totalScenarios + '</strong> relevant scenarios include your property.<br><strong>' + dc.missedScenarios + '</strong> scenarios where you do not appear.</div>';
    html += '</div>';
    html += '<div class="adp-intent-grid">';
    Object.entries(dc.byIntent).forEach(function (e) {
      var status = kpiStatus(e[1].rate);
      html += '<div class="adp-intent-card"><div class="adp-intent-label">' + esc(e[0].replace(/_/g, " ")) + '</div>';
      html += '<div class="adp-intent-value ' + status + '">' + fmtPct(e[1].rate) + '</div>';
      html += '<div class="adp-intent-detail">' + e[1].captured + ' of ' + e[1].total + '</div></div>';
    });
    html += '</div></div>';
    el.innerHTML = html;
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
