/**
 * Helena CMO Founder Console — client
 */
(function () {
  "use strict";

  var brief = null;
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

  function setTab(name) {
    document.querySelectorAll(".helena-tab").forEach(function (t) {
      t.classList.toggle("active", t.getAttribute("data-tab") === name);
    });
    document.querySelectorAll(".helena-panel").forEach(function (p) {
      p.hidden = p.getAttribute("data-panel") !== name;
    });
  }

  function renderSafety(b) {
    var el = document.getElementById("helenaSafety");
    var s = b.safety || {};
    el.innerHTML =
      '<span class="helena-pill off">EXECUTE OFF</span>' +
      '<span class="helena-pill off">RECURRING OFF</span>' +
      (s.marketingOsDecisionWriteConnected
        ? '<span class="helena-pill">OS WRITE ON</span>'
        : '<span class="helena-pill warn">OS WRITE NOT CONNECTED</span>');
  }

  function renderBrief(b) {
    var el = document.getElementById("tab-brief");
    var scores = b.commercialScoreboard || {};
    var t1 = scores.tier1 || {};
    var t2 = scores.tier2 || {};

    el.innerHTML =
      '<div class="helena-section"><h2>This week in one paragraph</h2><p class="helena-prose">' +
      esc(b.thisWeekInOneParagraph) +
      "</p>" +
      '<p class="helena-muted" style="margin-top:8px">Source week: ' +
      esc((b.sourceWeek && b.sourceWeek.label) || "") +
      "</p></div>" +
      '<div class="helena-section"><h2>Top 3 things that matter</h2>' +
      (b.top3 || [])
        .map(function (item) {
          return (
            '<article class="helena-card"><h3>' +
            esc(item.rank) +
            ". " +
            esc(item.recommendation) +
            "</h3><p>" +
            esc(item.whyItMatters) +
            '</p><div class="helena-meta">' +
            '<span class="helena-chip">GTM ' +
            esc(item.gtmTrack) +
            "</span>" +
            '<span class="helena-chip">ICP ' +
            esc(item.icp) +
            "</span>" +
            '<span class="helena-chip">' +
            esc(item.status) +
            "</span></div></article>"
          );
        })
        .join("") +
      "</div>" +
      '<div class="helena-section"><h2>Commercial scoreboard</h2>' +
      '<p class="helena-muted">Tier 1 outcomes lead. Tier 3 is diagnostic only.</p>' +
      '<div class="helena-scoregrid">' +
      metric("Paid pilots", t1.paidPilots) +
      metric("Qualified opps", t1.qualifiedOpportunities) +
      metric("Proposals", t1.proposals) +
      metric("Customers", t1.customers) +
      metric("Revenue", t1.revenue) +
      metric("Retention / expand", t1.retentionExpansion) +
      "</div>" +
      '<div class="helena-scoregrid" style="margin-top:10px">' +
      metric("Demo / pilot talks", t2.demoPilotConversations) +
      metric("Qualified leads", t2.qualifiedLeads) +
      metric("Target-account move", t2.targetAccountMovement) +
      "</div></div>" +
      listSection("What is working", b.whatIsWorking) +
      listSection("What is not working", b.whatIsNotWorking) +
      listSection("What is UNKNOWN", b.whatIsUnknown) +
      listSection("Helena recommends", b.helenaRecommends) +
      '<div class="helena-section"><h2>Joan needs to decide</h2>' +
      (b.joanNeedsToDecide || [])
        .map(function (d) {
          return (
            '<article class="helena-card"><h3>' +
            esc(d.id) +
            " · " +
            esc(d.decision) +
            "</h3><p>" +
            esc(d.helenaRecommendation) +
            '</p><div class="helena-meta">' +
            '<span class="helena-chip">' +
            esc(d.status) +
            "</span>" +
            (d.gtmTrack ? '<span class="helena-chip">' + esc(d.gtmTrack) + "</span>" : "") +
            (d.icp ? '<span class="helena-chip">' + esc(d.icp) + "</span>" : "") +
            '</div><div class="helena-btn-row">' +
            '<button type="button" class="helena-btn primary" data-open-decision="' +
            esc(d.id) +
            '">Review &amp; act</button></div></article>'
          );
        })
        .join("") +
      "</div>" +
      listSection("Next week", b.nextWeek) +
      '<div class="helena-section"><h2>Red flags / exceptions</h2>' +
      (b.redFlags || [])
        .map(function (f) {
          return (
            '<div class="helena-card helena-flag ' +
            (f.severity === "HIGH" ? "high" : "info") +
            '"><strong>' +
            esc(f.severity) +
            "</strong> — " +
            esc(f.text) +
            "</div>"
          );
        })
        .join("") +
      "</div>";
  }

  function metric(label, value) {
    return (
      '<div class="helena-score"><div class="label">' +
      esc(label) +
      "</div>" +
      fmtVal(value) +
      "</div>"
    );
  }

  function listSection(title, items) {
    return (
      '<div class="helena-section"><h2>' +
      esc(title) +
      '</h2><ul class="helena-list">' +
      (items || []).map(function (i) {
        return "<li>" + esc(i) + "</li>";
      }).join("") +
      "</ul></div>"
    );
  }

  function renderDecisions(b) {
    var el = document.getElementById("tab-decisions");
    el.innerHTML =
      '<div class="helena-section"><h2>Pending founder decisions</h2>' +
      (b.joanNeedsToDecide || [])
        .map(function (d) {
          return (
            '<article class="helena-card" id="decision-' +
            esc(d.id) +
            '"><h3>' +
            esc(d.id) +
            "</h3><p><strong>Decision:</strong> " +
            esc(d.decision) +
            "</p><p><strong>Helena:</strong> " +
            esc(d.helenaRecommendation) +
            "</p><p><strong>Why now:</strong> " +
            esc(d.whyNow) +
            "</p><p><strong>Evidence:</strong> " +
            esc(d.evidence) +
            "</p><p><strong>Confidence:</strong> " +
            esc(d.confidence) +
            "</p><p><strong>Founder lock:</strong> " +
            esc(d.founderLock) +
            "</p><p><strong>If nothing:</strong> " +
            esc(d.consequenceOfDoingNothing) +
            '</p><div class="helena-meta">' +
            '<span class="helena-chip">Status ' +
            esc(d.status) +
            "</span></div>" +
            '<div class="helena-btn-row"><button type="button" class="helena-btn primary" data-open-decision="' +
            esc(d.id) +
            '">Lock / Approve · Amend · Hold</button></div></article>'
          );
        })
        .join("") +
      "</div>";
  }

  function renderApprovals(b) {
    var el = document.getElementById("tab-approvals");
    el.innerHTML =
      '<div class="helena-section"><h2>PREPARE waiting for Joan</h2>' +
      '<p class="helena-muted">APPROVE = prepare-only. Does <strong>not</strong> publish, send, deploy, or merge.</p>' +
      (b.approvals || [])
        .map(function (a) {
          return (
            '<article class="helena-card"><h3>' +
            esc(a.title) +
            '</h3><p class="helena-muted">' +
            esc(a.type) +
            " · " +
            esc(a.id) +
            "</p><p>" +
            esc(a.why) +
            '</p><div class="helena-meta">' +
            '<span class="helena-chip">' +
            esc(a.status) +
            "</span>" +
            '<span class="helena-chip">' +
            esc(a.icp) +
            "</span>" +
            '<span class="helena-chip">' +
            esc(a.gtmTrack) +
            "</span>" +
            '<span class="helena-chip">' +
            esc(a.claimClass) +
            "</span>" +
            '<span class="helena-chip">' +
            esc(a.productMaturity) +
            "</span>" +
            '<span class="helena-chip">CTA ' +
            esc(a.cta) +
            "</span></div>" +
            '<div class="helena-btn-row">' +
            '<button type="button" class="helena-btn primary" data-approval-action="APPROVE" data-approval-id="' +
            esc(a.id) +
            '">Approve PREPARE</button>' +
            '<button type="button" class="helena-btn" data-approval-action="AMEND" data-approval-id="' +
            esc(a.id) +
            '">Amend</button>' +
            '<button type="button" class="helena-btn warn" data-approval-action="HOLD" data-approval-id="' +
            esc(a.id) +
            '">Hold</button>' +
            '<button type="button" class="helena-btn warn" data-approval-action="REJECT" data-approval-id="' +
            esc(a.id) +
            '">Reject</button></div></article>'
          );
        })
        .join("") +
      "</div>";
  }

  function renderPerformance(b) {
    var el = document.getElementById("tab-performance");
    var rows = (b.performance && b.performance.accountabilityRows) || [];
    var ms = (b.performance && b.performance.measurementState) || {};
    el.innerHTML =
      '<div class="helena-section"><h2>Business outcomes</h2><p class="helena-muted">Same Tier 1 board as Founder Brief — no vanity metrics here.</p></div>' +
      '<div class="helena-section"><h2>Accountability</h2><div style="overflow:auto"><table class="helena-table"><thead><tr>' +
      "<th>Initiative</th><th>Did</th><th>Expected</th><th>Happened</th><th>Learned</th><th>Change</th>" +
      "</tr></thead><tbody>" +
      rows
        .map(function (r) {
          return (
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
            "</td><td>" +
            esc(r.change) +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table></div></div>" +
      '<div class="helena-section"><h2>Channel diagnostics (Tier 3)</h2><ul class="helena-list">' +
      Object.keys(ms)
        .map(function (k) {
          return "<li><strong>" + esc(k) + "</strong>: " + esc(ms[k]) + "</li>";
        })
        .join("") +
      "</ul><p class="helena-muted">Likes / sessions / impressions are not commercial success.</p></div>";
  }

  function renderAdp(b) {
    var el = document.getElementById("tab-adp");
    var pilots = (b.adpPilots && b.adpPilots.pilots) || [];
    var candidates = (b.adpPilots && b.adpPilots.candidates) || [];
    el.innerHTML =
      '<div class="helena-section"><h2>Live pilots</h2>' +
      (pilots.length
        ? ""
        : '<div class="helena-card"><p>' +
          esc((b.adpPilots && b.adpPilots.emptyState) || "No pilots") +
          "</p><p class="helena-muted">Booking/revenue causality: not claimed.</p></div>") +
      "</div>" +
      '<div class="helena-section"><h2>Candidate classes (not named accounts)</h2>' +
      candidates
        .map(function (c) {
          return (
            '<article class="helena-card"><h3>' +
            esc(c.label) +
            "</h3><p><strong>ICP:</strong> " +
            esc(c.icp) +
            "</p><p><strong>Why now:</strong> " +
            esc(c.whyNow) +
            "</p><p><strong>Next:</strong> " +
            esc(c.nextStep) +
            "</p><p class=\"helena-muted\">" +
            esc(c.evidenceClass) +
            "</p></article>"
          );
        })
        .join("") +
      "</div>";
  }

  function renderHistory(b) {
    var el = document.getElementById("tab-history");
    el.innerHTML =
      '<div class="helena-section"><h2>Manual packs</h2>' +
      (b.history || [])
        .map(function (h) {
          return (
            '<article class="helena-card"><h3>' +
            esc(h.week) +
            "</h3><p><strong>Priorities:</strong> " +
            esc((h.topPriorities || []).join(" · ")) +
            "</p><p><strong>Locks recorded:</strong> " +
            esc((h.decisionsMade || []).join(", ")) +
            "</p><p><strong>Learnings:</strong> " +
            esc((h.majorLearnings || []).join("; ")) +
            '</p><p class="helena-muted">Pack: ' +
            esc(h.packPath) +
            "</p></article>"
          );
        })
        .join("") +
      "</div>";
  }

  function renderEvidence(b) {
    var el = document.getElementById("tab-evidence");
    var links = (b.evidence && b.evidence.links) || [];
    el.innerHTML =
      '<div class="helena-section helena-evidence"><h2>Supporting evidence</h2>' +
      '<p class="helena-muted">Optional deep dive. Founder Console does not require Cursor. Repo paths shown for provenance.</p><ul class="helena-list">' +
      links
        .map(function (l) {
          return "<li><strong>" + esc(l.title) + "</strong><br /><code>" + esc(l.path) + "</code></li>";
        })
        .join("") +
      "</ul>" +
      '<p class="helena-muted">Open in Cursor omitted — no reliable documented local URI scheme.</p></div>';
  }

  function openDecision(id) {
    activeDecisionId = id;
    var d = (brief.joanNeedsToDecide || []).find(function (x) {
      return x.id === id;
    });
    if (!d) return;
    document.getElementById("helenaDialogTitle").textContent = d.id;
    document.getElementById("helenaDialogBody").innerHTML =
      "<p><strong>Decision:</strong> " +
      esc(d.decision) +
      "</p><p><strong>Helena:</strong> " +
      esc(d.helenaRecommendation) +
      "</p><p><strong>Evidence:</strong> " +
      esc(d.evidence) +
      "</p><p><strong>Artifacts:</strong> " +
      esc((d.artifacts || []).join(", ")) +
      "</p>";
    document.getElementById("helenaDialogNote").value = "";
    document.getElementById("helenaDecisionDialog").showModal();
  }

  async function submitDecision(action) {
    if (!activeDecisionId) return;
    var note = document.getElementById("helenaDialogNote").value;
    var res = await authFetch("/api/admin/helena-cmo/decisions/" + encodeURIComponent(activeDecisionId) + "/action", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: action, note: note }),
    });
    var data = await res.json();
    if (!res.ok || !data.ok) {
      alert((data && data.error) || "Decision action failed");
      return;
    }
    document.getElementById("helenaDecisionDialog").close();
    brief = data.brief;
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
      alert((data && data.error) || "Approval action failed");
      return;
    }
    if (data.reminder) {
      /* visible in UI via status chip APPROVED_PREPARE_ONLY */
    }
    brief = data.brief;
    renderAll();
  }

  function renderAll() {
    if (!brief || !brief.ok) return;
    document.getElementById("helenaSubtitle").textContent =
      "Week " + ((brief.sourceWeek && brief.sourceWeek.label) || "") + " · attention " + brief.attentionCount;
    renderSafety(brief);
    renderBrief(brief);
    renderDecisions(brief);
    renderApprovals(brief);
    renderPerformance(brief);
    renderAdp(brief);
    renderHistory(brief);
    renderEvidence(brief);
  }

  function bind() {
    document.querySelectorAll(".helena-tab").forEach(function (t) {
      t.addEventListener("click", function () {
        setTab(t.getAttribute("data-tab"));
      });
    });
    document.getElementById("supportGateContent").addEventListener("click", function (e) {
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
      var res = await authFetch("/api/admin/helena-cmo/brief");
      var data = await res.json();
      if (!res.ok || !data.ok) {
        err.hidden = false;
        err.textContent = (data && data.message) || "Failed to load Helena brief";
        return;
      }
      brief = data.brief;
      err.hidden = true;
      renderAll();
    } catch (e) {
      err.hidden = false;
      err.textContent = e.message || "Load error";
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (window.DealalityWaveLoader) {
      window.DealalityWaveLoader.mount("#supportGateLoading", { label: "Loading Helena CMO…" });
    }
    var gate = window.SupportAdminGate;
    if (!gate) return;
    gate
      .requireAdmin({
        loadingEl: "#supportGateLoading",
        deniedEl: "#supportGateDenied",
        contentEl: "#supportGateContent",
      })
      .then(function (ok) {
        if (!ok) return;
        bind();
        return load();
      });
  });
})();
