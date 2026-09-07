/**
 * Helena CMO Founder Console V2 — client
 * Default: Executive Assessment. Tactical approvals sit downstream.
 */
(function () {
  "use strict";

  var consoleVm = null;
  var activeDecisionId = null;

  function authFetch(url, opts) {
    var auth = window.DealalityMemberstackAuth;
    if (!auth || typeof auth.authFetch !== "function") {
      return Promise.reject(new Error("auth_unavailable"));
    }
    return auth.authFetch(url, opts || {});
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtVal(v) {
    if (v == null || v === "") return '<span class="value gap">UNKNOWN</span>';
    if (v === "DATA_GAP" || v === "UNKNOWN") return '<span class="value gap">' + esc(v) + "</span>";
    return '<span class="value">' + esc(v) + "</span>";
  }

  function listHtml(items) {
    if (!items || !items.length) return '<p class="helena-muted">None recorded.</p>';
    return '<ul class="helena-list">' + items.map(function (x) {
      return "<li>" + esc(typeof x === "string" ? x : x.title || x.issue || JSON.stringify(x)) + "</li>";
    }).join("") + "</ul>";
  }

  function setTab(name) {
    document.querySelectorAll(".helena-tab").forEach(function (t) {
      t.classList.toggle("active", t.getAttribute("data-tab") === name);
    });
    document.querySelectorAll(".helena-panel").forEach(function (p) {
      p.hidden = p.getAttribute("data-panel") !== name;
    });
  }

  function renderSafety(vm) {
    var el = document.getElementById("helenaSafety");
    var s = vm.safety || {};
    var state = (vm.meta && vm.meta.strategyState) || "STRATEGY_PENDING_FOUNDER_REVIEW";
    el.innerHTML =
      '<span class="helena-pill off">EXECUTE OFF</span>' +
      '<span class="helena-pill off">RECURRING OFF</span>' +
      (s.marketingOsDecisionWriteConnected
        ? '<span class="helena-pill">OS WRITE ON</span>'
        : '<span class="helena-pill warn">OS WRITE NOT CONNECTED</span>') +
      '<span class="helena-pill warn">' + esc(state) + "</span>";
  }

  function renderAssessment(vm) {
    var el = document.getElementById("tab-assessment");
    var ea = vm.executiveAssessment || {};
    var health = ea.overallHealth || vm.overallMarketingHealth || {};
    var pending = !!(vm.meta && vm.meta.pendingStrategyReview);
    var first = ea.whatShouldHappenFirst || [];
    var cov = vm.sourceCoverage || {};
    var sd = vm.strategyDecision || {};
    var thesis = ea.revisedStrategyThesis || ea.recommendedStrategyThesis || "";
    var opts = ea.strategicOptions || sd.alternatives || [];
    var gaps = (sd.blockingGaps && sd.blockingGaps.gaps) || [];
    var dataDec = ea.dataCompletenessDecision || sd.dataCompletenessDecision || {};

    el.innerHTML =
      '<div class="helena-section"><h2>Source coverage</h2>' +
      '<div class="helena-card helena-coverage">' +
      '<div class="helena-scoregrid">' +
      '<div class="helena-score"><div class="label">Reviewed</div><div class="value">' +
      esc(cov.reviewed != null ? cov.reviewed + " / " + cov.relevant : "—") +
      "</div></div>" +
      '<div class="helena-score"><div class="label">Coverage</div><div class="value">' +
      esc(cov.coveragePercent != null ? cov.coveragePercent + "%" : "—") +
      "</div></div>" +
      '<div class="helena-score"><div class="label">Fresh</div><div class="value">' +
      esc(cov.fresh != null ? cov.fresh : "—") +
      "</div></div>" +
      '<div class="helena-score"><div class="label">Stale</div><div class="value">' +
      esc(cov.stale != null ? cov.stale : "—") +
      "</div></div>" +
      '<div class="helena-score"><div class="label">Missing</div><div class="value gap">' +
      esc(cov.missing != null ? cov.missing : "—") +
      "</div></div>" +
      '<div class="helena-score"><div class="label">Confidence</div><div class="value">' +
      esc(cov.confidence || (vm.meta && vm.meta.baselineConfidence) || "MEDIUM") +
      "</div></div></div>" +
      '<p class="helena-muted">' +
      esc(cov.honesty || "Validation pack required for full coverage detail.") +
      '</p><button type="button" class="helena-btn ghost" data-tab-jump="baseline">Drill into coverage</button></div></div>' +
      '<div class="helena-section"><h2>Current state</h2><p class="helena-prose">' +
      esc(ea.whereWeStand) +
      '</p><div class="helena-scoregrid"><div class="helena-score"><div class="label">Health</div><div class="value">' +
      esc(health.score) +
      "/10</div><div class=\"helena-muted\">" +
      esc(health.label || "") +
      (health.priorScore != null ? " · prior " + esc(health.priorScore) : "") +
      "</div></div></div>" +
      (health.whyRevised ? '<p class="helena-muted">' + esc(health.whyRevised) + "</p>" : "") +
      "<h3>Strengths</h3>" +
      listHtml(ea.topStrengths) +
      "<h3>Not working</h3>" +
      listHtml(ea.whatIsNotWorking) +
      "</div>" +
      '<div class="helena-section"><h2>Strategic diagnosis <span class="helena-chip">' +
      esc(ea.centralProblemConfidence || "MEDIUM_CONFIDENCE") +
      "</span></h2><p class=\"helena-prose\">" +
      esc(ea.centralProblem) +
      "</p></div>" +
      '<div class="helena-section"><h2>Strategic options</h2>' +
      listHtml(
        opts.map(function (o) {
          return (
            (o.recommended ? "★ " : "") +
            (o.id || "") +
            " — " +
            (o.name || "") +
            (o.score != null ? " (" + o.score + "/10)" : "")
          );
        }),
      ) +
      "</div>" +
      '<div class="helena-section"><h2>Helena recommendation <span class="helena-chip">' +
      esc(ea.helenaRecommendationScore != null ? ea.helenaRecommendationScore + "/10" : ea.strategyChallengeDecision || "READY") +
      "</span></h2><p class=\"helena-prose\"><strong>" +
      esc(ea.helenaRecommendationName || "Pending 6E pack") +
      "</strong></p><p class=\"helena-prose\">" +
      esc(thesis) +
      "</p><h3>Will not prioritize</h3>" +
      listHtml(ea.willNotPrioritize || []) +
      '<p class="helena-muted">Primary CTA is <strong>DISCUSS STRATEGY</strong> — not approve/lock.</p></div>' +
      '<div class="helena-section"><h2>Uncertainties</h2>' +
      listHtml(ea.uncertainties || ea.unknowns || []) +
      "<h3>Data completeness</h3><p class=\"helena-prose\">" +
      esc((dataDec.answer || "—") + " — " + (dataDec.explanation || "")) +
      "</p><h3>Gap classes</h3>" +
      listHtml(
        gaps.map(function (g) {
          return g.gap + " → " + g.classification;
        }),
      ) +
      "</div>" +
      '<div class="helena-section"><h2>Founder discussion</h2>' +
      '<div class="helena-card"><p class="helena-prose">' +
      esc(ea.founderDiscussionPrompt || "Discuss the recommended strategic choice before any lock.") +
      '</p><button type="button" class="helena-btn primary" data-open-decision="STRATEGY-V1">DISCUSS STRATEGY</button> ' +
      '<button type="button" class="helena-btn" data-tab-jump="strategy">Open strategy detail</button></div></div>' +
      '<div class="helena-section"><h2>Top 3 things that matter first</h2>' +
      listHtml(first.slice(0, 3)) +
      "</div>" +
      (pending
        ? '<div class="helena-section"><h2>Proposed actions (not ready to approve)</h2><p class="helena-muted">Context only while strategy is in founder discussion.</p>' +
          listHtml((vm.joanNeedsToDecide || []).map(function (d) {
            return d.id + ": " + d.decision + " — " + (d.displayStatus || "PROPOSED — PENDING STRATEGY REVIEW");
          })) +
          "</div>"
        : "");
  }

  function renderBaseline(vm) {
    var el = document.getElementById("tab-baseline");
    var card = vm.scorecard || {};
    var keys = Object.keys(card);
    var cov = vm.sourceCoverage || {};
    var html =
      '<div class="helena-section"><h2>Baseline meta</h2><p class="helena-meta">Version ' +
      esc(vm.meta && vm.meta.baselineVersion) +
      " · Last deep review " +
      esc(vm.meta && vm.meta.lastDeepReviewDate) +
      " · Freshness " +
      esc(vm.meta && vm.meta.dataFreshness) +
      " · Validation " +
      esc(vm.meta && vm.meta.validationPack) +
      "</p><p class=\"helena-muted\">Deep baseline is not re-run every week. Weekly Helena updates deltas only.</p></div>";

    html +=
      '<div class="helena-section"><h2>Source coverage detail</h2><p class="helena-muted">' +
      esc(cov.honesty || "") +
      "</p>";
    (cov.sources || []).forEach(function (s) {
      html +=
        '<div class="helena-card"><div class="helena-meta">' +
        esc(s.group) +
        " · " +
        esc(s.freshness) +
        " · " +
        esc(s.confidence) +
        "</div><p class=\"helena-prose\">" +
        esc(s.source) +
        "</p><p class=\"helena-muted\">Reviewed: " +
        esc(String(s.reviewed)) +
        " · Available: " +
        esc(String(s.available)) +
        "</p></div>";
    });
    html += "</div>";

    keys.forEach(function (k) {
      var r = card[k];
      html +=
        '<div class="helena-card helena-baseline-card"><div class="helena-meta">' +
        esc(k) +
        '</div><div class="helena-scoregrid"><div class="helena-score"><div class="label">Score</div><div class="value">' +
        esc(r.score) +
        '/10</div></div></div><p class="helena-prose"><strong>Status:</strong> ' +
        esc(r.currentState) +
        "</p><p class=\"helena-prose\">" +
        esc(r.why) +
        '</p><p class="helena-muted">Gap: ' +
        esc(r.biggestGap) +
        " · Opportunity: " +
        esc(r.opportunity) +
        " · Confidence: " +
        esc(r.confidence) +
        "</p></div>";
    });
    el.innerHTML = html;
  }

  function renderFindings(vm) {
    var el = document.getElementById("tab-findings");
    var f = vm.findings || {};
    el.innerHTML =
      '<div class="helena-section"><h2>Strengths</h2>' +
      listHtml((f.strengths || []).map(function (s) { return s.title + " — " + s.evidence; })) +
      '</div><div class="helena-section"><h2>Weaknesses / improvements</h2>' +
      listHtml((f.improvements || []).map(function (i) {
        return "[" + i.urgency + "] " + i.issue + " → " + i.consequence;
      })) +
      '</div><div class="helena-section"><h2>Opportunities</h2>' +
      listHtml((vm.executiveAssessment && vm.executiveAssessment.topOpportunities) || []) +
      '</div><div class="helena-section"><h2>Risks / if we do nothing</h2>' +
      listHtml((vm.diagnosis && vm.diagnosis.ifDoNothing) || []) +
      '</div><div class="helena-section"><h2>Unknowns</h2>' +
      listHtml(f.unknowns || []) +
      "</div>";
  }

  function renderStrategy(vm) {
    var el = document.getElementById("tab-strategy");
    var st = vm.strategy || {};
    var sd = vm.strategyDecision || {};
    var pending = !!st.pendingStrategyReview;
    var thesis =
      (st.recommendation && st.recommendation.name) ||
      (sd.recommended && sd.recommended.name) ||
      st.thesis;
    var channels = sd.channelRoles || [];
    el.innerHTML =
      '<div class="helena-section"><h2>Strategy state</h2><span class="helena-chip">' +
      esc(st.state) +
      '</span><p class="helena-muted">Primary CTA: DISCUSS STRATEGY — approval/lock not requested in Phase 6E.</p></div>' +
      '<div class="helena-section"><h2>Helena recommendation</h2><p class="helena-prose"><strong>' +
      esc(thesis) +
      "</strong>" +
      (st.recommendation && st.recommendation.score != null
        ? " · " + esc(st.recommendation.score) + "/10"
        : "") +
      "</p><p class=\"helena-prose\">" +
      esc((st.choice && st.choice.competeWhere) || st.thesis || "") +
      "</p></div>" +
      '<div class="helena-section"><h2>Win first</h2><p class="helena-prose">' +
      esc((st.choice && st.choice.winFirst) || st.who || "") +
      "</p></div>" +
      '<div class="helena-section"><h2>Lead problem / entry offer</h2><p class="helena-prose">' +
      esc((st.choice && st.choice.leadProblem) || st.problem || "") +
      "</p><p class=\"helena-prose\">Entry: " +
      esc((st.choice && st.choice.entryOffer) || "") +
      "</p></div>" +
      '<div class="helena-section"><h2>Channel roles</h2>' +
      (channels.length
        ? listHtml(
            channels.map(function (c) {
              return c.channel + " [" + c.rank + "] — " + c.job;
            }),
          )
        : '<ul class="helena-list"><li>Website: ' +
          esc(st.websiteRole) +
          "</li><li>LinkedIn: " +
          esc(st.linkedinRole) +
          "</li></ul>") +
      '</div><div class="helena-section"><h2>What we will not do</h2>' +
      listHtml((st.choice && st.choice.willNotDo) || st.stopDoing || []) +
      '</div><div class="helena-section"><h2>Founder action</h2>' +
      (pending
        ? '<button type="button" class="helena-btn primary" data-open-decision="STRATEGY-V1">DISCUSS STRATEGY</button>'
        : '<p class="helena-muted">State is ' + esc(st.state) + ". Not auto-locked.</p>") +
      "</div>";
  }

  function renderPriorities(vm) {
    var el = document.getElementById("tab-priorities");
    var road = vm.roadmap || {};
    var html = "";
    ["NOW", "NEXT", "LATER", "PARK"].forEach(function (bucket) {
      html += '<div class="helena-section"><h2>' + bucket + "</h2>";
      (road[bucket] || []).forEach(function (i) {
        html +=
          '<div class="helena-card"><div class="helena-meta">' +
          esc(i.id) +
          " · pillar " +
          esc(i.strategicPillarId) +
          "</div><h3 class=\"helena-card-title\">" +
          esc(i.title) +
          '</h3><p class="helena-prose">Why: ' +
          esc(i.whyNow) +
          "</p><p class=\"helena-muted\">Value: " +
          esc(i.expectedValue) +
          " · Dependency: " +
          esc(i.dependency) +
          " · Success: " +
          esc(i.successLooksLike) +
          "</p></div>";
      });
      html += "</div>";
    });
    el.innerHTML = html;
  }

  function renderActions(vm) {
    var el = document.getElementById("tab-actions");
    var pending = !!(vm.meta && vm.meta.pendingStrategyReview);
    var html =
      '<div class="helena-section"><h2>Actions &amp; approvals</h2><p class="helena-prose">' +
      (pending
        ? "Strategy is in founder discussion. Tactical items are proposed only — Helena will not treat them as the primary ask."
        : "Strategy reviewed. Tactical PREPARE approvals may proceed (EXECUTE still OFF).") +
      "</p></div>";

    html += '<div class="helena-section"><h2>Strategy gate</h2><div class="helena-card">';
    html +=
      '<p class="helena-prose">' +
      esc((vm.founderStrategyDecision && vm.founderStrategyDecision.decision) || "Discuss strategy") +
      '</p><button type="button" class="helena-btn primary" data-open-decision="STRATEGY-V1">DISCUSS STRATEGY</button></div></div>';

    html += '<div class="helena-section"><h2>Joan needs to decide (tactical)</h2>';
    (vm.joanNeedsToDecide || []).forEach(function (d) {
      html +=
        '<div class="helena-card"><div class="helena-meta">' +
        esc(d.id) +
        " · " +
        esc(d.displayStatus || d.status) +
        (d.strategicParent ? " · " + esc(d.strategicParent) + " → " + esc(d.priorityParent) : "") +
        '</div><p class="helena-prose">' +
        esc(d.decision) +
        "</p>";
      if (!pending) {
        html +=
          '<button type="button" class="helena-btn" data-open-decision="' +
          esc(d.id) +
          '">Open decision</button>';
      } else {
        html += '<p class="helena-muted">PROPOSED — PENDING STRATEGY REVIEW</p>';
      }
      html += "</div>";
    });
    html += "</div>";

    html += '<div class="helena-section"><h2>PREPARE approvals</h2>';
    (vm.approvalsWaiting || []).forEach(function (a) {
      html +=
        '<div class="helena-card"><div class="helena-meta">' +
        esc(a.id) +
        " · " +
        esc(a.displayStatus || a.status) +
        '</div><p class="helena-prose">' +
        esc(a.title || a.name || a.id) +
        "</p>";
      if (!pending) {
        html +=
          '<div class="helena-btn-row">' +
          '<button type="button" class="helena-btn primary" data-approval-id="' +
          esc(a.id) +
          '" data-approval-action="APPROVE">Approve PREPARE</button>' +
          '<button type="button" class="helena-btn warn" data-approval-id="' +
          esc(a.id) +
          '" data-approval-action="HOLD">Hold</button></div>';
      } else {
        html += '<p class="helena-muted">PROPOSED — PENDING STRATEGY REVIEW</p>';
      }
      html += "</div>";
    });
    html += "</div>";
    el.innerHTML = html;
  }

  function renderPerformance(vm) {
    var el = document.getElementById("tab-performance");
    var brief = vm.weekBrief || {};
    var perf = brief.performance || {};
    var rows = perf.accountabilityRows || perf.accountability || [];
    var html =
      '<div class="helena-section"><h2>Weekly performance context</h2><p class="helena-muted">From Manual Week 1 pack — measurement still PARTIAL / attribution BROKEN.</p>';
    if (!rows.length) {
      html += '<p class="helena-muted">No accountability rows loaded.</p>';
    } else {
      html += '<table class="helena-table"><thead><tr><th>Initiative</th><th>Did</th><th>Expected</th><th>Happened</th><th>Learned</th></tr></thead><tbody>';
      rows.forEach(function (r) {
        html +=
          "<tr><td>" +
          esc(r.initiative) +
          "</td><td>" +
          esc(r.did) +
          "</td><td>" +
          esc(r.expected) +
          "</td><td>" +
          esc(r.happened) +
          "</td><td>" +
          esc(r.learned) +
          "</td></tr>";
      });
      html += "</tbody></table>";
    }
    html +=
      '</div><div class="helena-section"><h2>Measurement state</h2><p class="helena-prose">GA4: ' +
      fmtVal(perf.measurementState && perf.measurementState.ga4) +
      " · Attribution: " +
      fmtVal(perf.attributionState && (perf.attributionState.chain || perf.attributionState.status)) +
      "</p></div>";
    el.innerHTML = html;
  }

  function renderAdp(vm) {
    var el = document.getElementById("tab-adp");
    var brief = vm.weekBrief || {};
    var adp = brief.adpPilots || {};
    el.innerHTML =
      '<div class="helena-section"><h2>ADP pilots</h2><p class="helena-prose">' +
      esc(adp.note || adp.emptyState || "No live pilots recorded.") +
      '</p><p class="helena-muted">No hotel names fabricated.</p>' +
      listHtml(adp.candidates || []) +
      "</div>";
  }

  function renderHistory(vm) {
    var el = document.getElementById("tab-history");
    var brief = vm.weekBrief || {};
    var hist = brief.history || [];
    var html = '<div class="helena-section"><h2>History</h2>';
    hist.forEach(function (h) {
      html +=
        '<div class="helena-card"><div class="helena-meta">' +
        esc(h.week) +
        '</div>' +
        listHtml(h.topPriorities || []) +
        '<p class="helena-muted">' +
        esc(h.packPath || "") +
        "</p></div>";
    });
    html +=
      '<div class="helena-card"><div class="helena-meta">Baseline V1</div><p class="helena-prose">Deep CMO baseline ' +
      esc(vm.meta && vm.meta.lastDeepReviewDate) +
      " · strategy " +
      esc(vm.meta && vm.meta.strategyState) +
      "</p></div></div>";
    el.innerHTML = html;
  }

  function renderEvidence(vm) {
    var el = document.getElementById("tab-evidence");
    var brief = vm.weekBrief || {};
    var links = (brief.evidence && brief.evidence.links) || [];
    var html =
      '<div class="helena-section"><h2>Evidence</h2><ul class="helena-list">' +
      '<li>reports/helena-cmo-baseline-v1/00_EXECUTIVE_CMO_ASSESSMENT.md</li>' +
      '<li>reports/helena-cmo-baseline-v1/helena-cmo-baseline-v1.json</li>' +
      '<li>reports/helena-cmo-baseline-v1/helena-cmo-strategy-v1.json</li>';
    links.forEach(function (l) {
      html += "<li>" + esc(l.title) + " — <code>" + esc(l.path) + "</code></li>";
    });
    html += "</ul></div>";
    el.innerHTML = html;
  }

  function openDecision(id) {
    activeDecisionId = id;
    var body = document.getElementById("helenaDialogBody");
    var title = document.getElementById("helenaDialogTitle");
    document.getElementById("helenaDialogNote").value = "";
    if (id === "STRATEGY-V1") {
      title.textContent = "Discuss strategy";
      body.innerHTML =
        '<p class="helena-prose">' +
        esc((consoleVm.founderStrategyDecision && consoleVm.founderStrategyDecision.decision) || "") +
        '</p><p class="helena-prose">' +
        esc(
          (consoleVm.executiveAssessment && consoleVm.executiveAssessment.helenaRecommendationName) ||
            "",
        ) +
        '</p><p class="helena-muted">Primary ask is discussion. Use Amend to capture notes. Hold keeps discussion open. Lock is only if you are ready after discussion — it still does not enable EXECUTE, publish, or merge.</p>';
    } else {
      var d = (consoleVm.joanNeedsToDecide || []).find(function (x) {
        return x.id === id;
      });
      title.textContent = id;
      body.innerHTML = d
        ? '<p class="helena-prose">' +
          esc(d.decision) +
          '</p><p class="helena-muted">' +
          esc(d.helenaRecommendation || "") +
          "</p>"
        : '<p class="helena-muted">Decision details unavailable.</p>';
    }
    document.getElementById("helenaDecisionDialog").showModal();
  }

  async function submitDecision(action) {
    if (!activeDecisionId) return;
    var note = document.getElementById("helenaDialogNote").value || "";
    var res = await authFetch(
      "/api/admin/helena-cmo/decisions/" + encodeURIComponent(activeDecisionId) + "/action",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: action, note: note }),
      },
    );
    var data = await res.json();
    if (!res.ok || !data.ok) {
      alert((data && data.message) || (data && data.error) || "Decision action failed");
      return;
    }
    document.getElementById("helenaDecisionDialog").close();
    consoleVm = data.console || consoleVm;
    renderAll();
  }

  async function submitApproval(id, action) {
    var res = await authFetch("/api/admin/helena-cmo/approvals/" + encodeURIComponent(id) + "/action", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: action, note: "" }),
    });
    var data = await res.json();
    if (!res.ok || !data.ok) {
      alert((data && data.message) || (data && data.error) || "Approval action failed");
      return;
    }
    consoleVm = data.console || consoleVm;
    renderAll();
  }

  function renderAll() {
    if (!consoleVm || !consoleVm.ok) return;
    document.getElementById("helenaSubtitle").textContent =
      "Baseline " +
      ((consoleVm.meta && consoleVm.meta.baselineVersion) || "v1") +
      " · " +
      ((consoleVm.meta && consoleVm.meta.strategyState) || "") +
      " · attention " +
      consoleVm.attentionCount;
    renderSafety(consoleVm);
    renderAssessment(consoleVm);
    renderBaseline(consoleVm);
    renderFindings(consoleVm);
    renderStrategy(consoleVm);
    renderPriorities(consoleVm);
    renderActions(consoleVm);
    renderPerformance(consoleVm);
    renderAdp(consoleVm);
    renderHistory(consoleVm);
    renderEvidence(consoleVm);
  }

  function bind() {
    document.querySelectorAll(".helena-tab").forEach(function (t) {
      t.addEventListener("click", function () {
        setTab(t.getAttribute("data-tab"));
      });
    });
    document.getElementById("supportGateContent").addEventListener("click", function (e) {
      var jump = e.target.closest("[data-tab-jump]");
      if (jump) {
        setTab(jump.getAttribute("data-tab-jump"));
        return;
      }
      var open = e.target.closest("[data-open-decision]");
      if (open) {
        openDecision(open.getAttribute("data-open-decision"));
        return;
      }
      var appr = e.target.closest("[data-approval-action]");
      if (appr) {
        submitApproval(appr.getAttribute("data-approval-id"), appr.getAttribute("data-approval-action"));
      }
    });
    document.querySelectorAll("#helenaDecisionDialog [data-action]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var action = btn.getAttribute("data-action");
        if (action === "HOLD") {
          submitDecision("HOLD");
        } else {
          submitDecision(action);
        }
      });
    });
  }

  async function load() {
    var err = document.getElementById("helenaError");
    try {
      var res = await authFetch("/api/admin/helena-cmo/console");
      var data = await res.json();
      if (!res.ok || !data.ok) {
        err.hidden = false;
        err.textContent = (data && data.message) || "Failed to load Helena console";
        return;
      }
      consoleVm = data.console;
      err.hidden = true;
      renderAll();
      setTab("assessment");
    } catch (e) {
      err.hidden = false;
      err.textContent = e.message || "Load error";
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (
      window.DealalityWaveLoader &&
      typeof window.DealalityWaveLoader.mount === "function"
    ) {
      window.DealalityWaveLoader.mount("#supportGateLoading", { label: "Loading Helena CMO…" });
    }
    var gate = window.SupportAdminGate;
    if (!gate || typeof gate.requireHelenaCmoAdmin !== "function") {
      var err = document.getElementById("helenaError");
      if (err) {
        err.hidden = false;
        err.textContent = "Helena admin gate unavailable.";
      }
      return;
    }
    gate
      .requireHelenaCmoAdmin({
        loadingId: "supportGateLoading",
        deniedId: "supportGateDenied",
        contentId: "supportGateContent",
      })
      .then(function (ok) {
        if (!ok) return;
        bind();
        return load();
      });
  });
})();
