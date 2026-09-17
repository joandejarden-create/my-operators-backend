/**
 * ADP Monthly Executive Review — institutional PDF presentation builder.
 * Same canonical payload as live/meeting; report-family visual system only.
 * Gate: MONTHLY_REVIEW_CANONICAL_DATA_PDF_PRESENTATION_SEPARATION
 */
(function (global) {
  "use strict";

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatShortDate(isoOrLabel) {
    if (!isoOrLabel) return "—";
    var s = String(isoOrLabel);
    var m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) return s.toUpperCase();
    var months = [
      "JAN",
      "FEB",
      "MAR",
      "APR",
      "MAY",
      "JUN",
      "JUL",
      "AUG",
      "SEP",
      "OCT",
      "NOV",
      "DEC",
    ];
    var mon = months[parseInt(m[2], 10) - 1] || m[2];
    return mon + " " + parseInt(m[3], 10) + ", " + m[1];
  }

  function formatMonthUpper(label) {
    return String(label || "")
      .trim()
      .toUpperCase();
  }

  function kpiLabel(k) {
    var id = k.id || "";
    if (id === "aiConsideration") return "AI Consideration";
    if (id === "scenarioPresence") return "Scenario Presence";
    if (id === "realityCoverage") return "Reality Coverage";
    if (id === "presenceIndex") return "Presence Index";
    if (id === "bpp_portfolioAiPresence") return "Brand Portfolio Position";
    if (id === "bpp_portfolioRank") return "Portfolio Rank";
    return String(k.label || id).replace(/\s*·\s*.*$/, "");
  }

  function kpiMeta(k) {
    if (k.id === "presenceIndex") {
      var fromLabel = String(k.label || "").split("·")[1];
      if (fromLabel) return fromLabel.trim();
    }
    if (k.deltaDisplay && k.id === "bpp_portfolioRank") {
      return /unchanged|#1 → #1/i.test(k.deltaDisplay)
        ? "unchanged"
        : k.deltaDisplay;
    }
    return k.meta || "";
  }

  function kpiDelta(k) {
    if (k.id === "bpp_portfolioRank") return "";
    return k.deltaDisplay || "";
  }

  function titleCaseNeed(s) {
    var t = String(s || "").trim();
    if (!t) return "Observation";
    if (/^[a-z]/.test(t) && t.indexOf(" ") === -1) {
      return t.charAt(0).toUpperCase() + t.slice(1);
    }
    return t;
  }

  function formatProviderLabel(raw) {
    var s = String(raw || "").trim().toLowerCase();
    if (!s) return "—";
    if (s === "openai" || s === "chatgpt" || s === "gpt" || s.indexOf("gpt-") === 0) {
      return "ChatGPT";
    }
    if (s === "gemini") return "Gemini";
    if (s === "perplexity") return "Perplexity";
    if (s === "claude") return "Claude";
    if (s.indexOf("all provider") === 0 || s === "all") return "All Providers";
    if (s.indexOf("cross-provider") === 0 || s.indexOf("cross provider") === 0) {
      return "Cross-provider";
    }
    return String(raw);
  }

  function statusPillClass(status) {
    var s = String(status || "").toLowerCase();
    if (/complete|done|closed/.test(s)) return "drs-pill--done";
    if (/progress|in progress/.test(s)) return "drs-pill--progress";
    if (/monitor/.test(s)) return "drs-pill--monitor";
    return "drs-pill--open";
  }

  function statusPillLabel(status) {
    var s = String(status || "Open").trim();
    if (/in progress/i.test(s)) return "IN PROGRESS";
    if (/complete|done|closed/i.test(s)) return "COMPLETED";
    if (/monitor/i.test(s)) return "MONITOR";
    return "OPEN";
  }

  function priorityForAction(a, i) {
    var p = String(a.managementPriority || "");
    if (p === "PRIORITY_1") return { label: "PRIORITY 1", cls: "drs-pill--high" };
    if (p === "MONITOR_SUPPORTING") return { label: "MONITOR", cls: "drs-pill--monitor" };
    if (p === "PRIORITY_2") return { label: "PRIORITY 2", cls: "" };
    return i === 0
      ? { label: "PRIORITY 1", cls: "drs-pill--high" }
      : { label: "PRIORITY 2", cls: "" };
  }

  function shortActionTitle(title) {
    var t = String(title || "").trim();
    var focus = t.split(/\s+[—–-]\s+focus:/i);
    if (focus[0] && focus[0].length < 140) return focus[0].trim();
    if (t.length <= 140) return t;
    return t.slice(0, 137).trim() + "…";
  }

  function renderCover(review) {
    var Chrome = global.DealalityReportPrintChrome;
    var logoUrl =
      (Chrome && Chrome.DEALALITY_LOGO_URL) ||
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";
    var confidential =
      "DEALALITY AI DEMAND INTELLIGENCE · CONFIDENTIAL · FOR RECIPIENT ONLY";
    var prop = review.property || {};
    var loc = prop.market || prop.city || "";
    var r = review.reporting || {};
    var metaLine =
      formatMonthUpper(r.reportingMonth) +
      " · CURRENT MONITORING " +
      formatShortDate(r.currentMonitoringDate) +
      " · PRIOR RUN " +
      formatShortDate(r.priorRunDate);
    var disclaimer =
      "This AI Demand Performance Review is a Dealality executive monitoring report. Measurement methodology is unchanged by presentation. Confidential — for the named recipient only.";

    return (
      '<section class="bas-cover-page bas-book-page-surface bas-avoid-break hid-cover-page drs-cover-shell" data-adp-mr-section="cover" aria-label="Cover">' +
      '<div class="bas-cover-geometric" aria-hidden="true"></div>' +
      '<p class="bas-cover-confidential">' +
      esc(confidential) +
      "</p>" +
      '<div class="bas-cover-block">' +
      '<p class="bas-cover-doc-type">AI DEMAND PERFORMANCE REVIEW</p>' +
      '<h1 class="bas-cover-title">' +
      esc(prop.name || "Property") +
      "</h1>" +
      (loc ? '<p class="bas-cover-location">' + esc(loc) + "</p>" : "") +
      '<div class="bas-cover-accent-line" aria-hidden="true"></div>' +
      '<p class="bas-cover-sub">MONTHLY EXECUTIVE REVIEW</p>' +
      '<p class="bas-cover-date">' +
      esc(metaLine) +
      "</p>" +
      '<p class="bas-cover-date">PERFORMANCE · COMPETITIVE MOVEMENT · EVIDENCE · ACTIONS · ACCOUNTABILITY</p>' +
      "</div>" +
      '<p class="bas-cover-disclaimer">' +
      esc(disclaimer) +
      "</p>" +
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="' +
      esc(logoUrl) +
      '" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>' +
      "</section>"
    );
  }

  function renderKpis(review) {
    return (
      '<div class="drs-kpi-band">' +
      (review.kpis || [])
        .map(function (k) {
          var delta = kpiDelta(k);
          var meta = kpiMeta(k);
          return (
            '<div class="drs-kpi">' +
            '<p class="drs-kpi__label">' +
            esc(kpiLabel(k)) +
            "</p>" +
            '<p class="drs-kpi__value">' +
            esc(k.currentDisplay) +
            "</p>" +
            (delta
              ? '<p class="drs-kpi__delta">' + esc(delta) + "</p>"
              : "") +
            (meta ? '<p class="drs-kpi__meta">' + esc(meta) + "</p>" : "") +
            "</div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function renderScanCards(review) {
    var expansion =
      (review.executiveAssessment && review.executiveAssessment.expansion) || {};
    var cards = expansion.scanCards || review.scanCards || {};
    var order = [
      "biggestStrength",
      "biggestConstraint",
      "changeSinceLastComparableRun",
    ];
    var html = order
      .map(function (key) {
        var card = cards[key];
        if (!card) return "";
        return (
          '<aside class="adp-mr-scan-card adp-mr-scan-card--' +
          key +
          '">' +
          '<p class="adp-mr-scan-card__label">' +
          esc(card.label) +
          "</p>" +
          '<p class="adp-mr-scan-card__headline">' +
          esc(card.headline || "") +
          "</p>" +
          (card.body
            ? '<p class="adp-mr-scan-card__body">' + esc(card.body) + "</p>"
            : "") +
          "</aside>"
        );
      })
      .join("");
    if (!html) return "";
    return '<div class="adp-mr-scan-grid">' + html + "</div>";
  }

  function renderExpansionBlock(title, body) {
    if (!body) return "";
    return (
      '<div class="adp-mr-expand-block">' +
      '<h3 class="drs-subhead">' +
      esc(title) +
      "</h3>" +
      '<p class="drs-body">' +
      esc(body) +
      "</p></div>"
    );
  }

  function renderExecutiveExpansion(review) {
    var expansion =
      (review.executiveAssessment && review.executiveAssessment.expansion) || {};
    var blocks = expansion.blocks || expansion.expansion || {};
    return (
      '<div class="adp-mr-expansion" data-adp-mr-layer="expansion">' +
      renderExpansionBlock("Executive Conclusion", blocks.conclusion) +
      renderExpansionBlock(
        "What the Data Is Telling Us",
        blocks.whatDataTellsUs
      ) +
      renderExpansionBlock("Why It Matters", blocks.whyItMatters) +
      renderExpansionBlock(
        "Where the Pattern Is Concentrated",
        blocks.concentration
      ) +
      renderExpansionBlock(
        "What Management Should Focus On Now",
        blocks.focusNow
      ) +
      renderExpansionBlock("What Not to Overreact To", blocks.caution) +
      renderExpansionBlock("What to Review Next", blocks.reviewNext) +
      "</div>"
    );
  }

  function renderExecutive(review) {
    var r = review.reporting || {};
    return (
      '<section class="drs-section adp-mr-section--executive" data-adp-mr-section="executive">' +
      '<h2 class="drs-section-h">1. Executive Performance</h2>' +
      (r.comparability && r.comparability.disclosure
        ? '<p class="adp-mr-disclosure">' + esc(r.comparability.disclosure) + "</p>"
        : "") +
      '<div class="adp-mr-layer adp-mr-layer--kpi" data-adp-mr-layer="kpi">' +
      renderKpis(review) +
      "</div>" +
      '<div class="adp-mr-layer adp-mr-layer--scan" data-adp-mr-layer="scan">' +
      '<h3 class="drs-subhead">Platform Executive Summary</h3>' +
      renderScanCards(review) +
      "</div>" +
      renderExecutiveExpansion(review) +
      "</section>"
    );
  }

  function renderTable(headers, rowsHtml) {
    return (
      '<div class="drs-table-wrap"><table class="drs-table bas-brief-table">' +
      "<thead><tr>" +
      headers.map(function (h) {
        return "<th>" + esc(h) + "</th>";
      }).join("") +
      "</tr></thead><tbody>" +
      rowsHtml +
      "</tbody></table></div>"
    );
  }

  function clientSignificance(row) {
    return row.significanceLabel || row.significance || "";
  }

  function renderSectionInsight(text) {
    if (!text) return "";
    return '<p class="adp-mr-section-insight">' + esc(text) + "</p>";
  }

  function renderMovement(review) {
    var material = review.materialMovements || [];
    var demand = review.demandMovement || [];
    var comp = review.competitiveMovement || {};
    var insights = review.sectionInsights || {};
    var materialHtml = material
      .map(function (row) {
        return (
          "<tr><td>" +
          esc(row.metricOrDemandArea) +
          '</td><td class="drs-num">' +
          esc(row.current) +
          '</td><td class="drs-num">' +
          esc(row.prior) +
          '</td><td class="drs-num">' +
          esc(row.change) +
          "</td><td>" +
          esc(row.executiveInterpretation) +
          "</td></tr>"
        );
      })
      .join("");
    var demandHtml = demand
      .map(function (row) {
        return (
          "<tr><td>" +
          esc(row.demandTerritory) +
          '</td><td class="drs-num">' +
          esc(row.current) +
          "</td><td>" +
          esc(row.captured) +
          "</td><td>" +
          esc(clientSignificance(row)) +
          "</td><td>" +
          esc(row.interpretation) +
          "</td></tr>"
        );
      })
      .join("");
    var leaders = comp.displacementLeaders || [];
    var compHtml = leaders
      .map(function (d) {
        return (
          "<tr><td>" +
          esc(d.name) +
          '</td><td class="drs-num">' +
          esc(d.displacementCount) +
          "</td></tr>"
        );
      })
      .join("");

    return (
      '<section class="drs-section drs-section--flow adp-mr-section--movement" data-adp-mr-section="movement">' +
      '<h2 class="drs-section-h">2. Demand &amp; Competitive Movement</h2>' +
      '<h3 class="drs-subhead">Material Movement</h3>' +
      (material.length
        ? renderTable(
            ["Metric / Demand Area", "Current", "Prior", "Change", "Executive Interpretation"],
            materialHtml
          )
        : '<p class="drs-muted">No material movement rows in this period.</p>') +
      '<h3 class="drs-subhead">Demand Movement</h3>' +
      renderSectionInsight(
        insights.demandMovement && insights.demandMovement.text
      ) +
      renderTable(
        ["Demand Territory", "Scenario Capture", "Captured", "Capture Level", "Interpretation"],
        demandHtml
      ) +
      (demand[0] && demand[0].note
        ? '<p class="drs-muted">' + esc(demand[0].note) + "</p>"
        : "") +
      '<h3 class="drs-subhead">Competitive Movement</h3>' +
      renderSectionInsight(
        insights.competitiveMovement && insights.competitiveMovement.text
      ) +
      (comp.summary &&
      !(insights.competitiveMovement && insights.competitiveMovement.text)
        ? '<p class="drs-body">' + esc(comp.summary) + "</p>"
        : "") +
      (leaders.length
        ? renderTable(["Competitor", "Displacement count"], compHtml)
        : "") +
      (comp.caveat ? '<p class="drs-muted">' + esc(comp.caveat) + "</p>" : "") +
      (insights.bpp && insights.bpp.text
        ? '<h3 class="drs-subhead">Independent Positioning Context</h3>' +
          renderSectionInsight(insights.bpp.text)
        : "") +
      (insights.realityGap && insights.realityGap.text
        ? '<h3 class="drs-subhead">Reality Coverage Context</h3>' +
          renderSectionInsight(insights.realityGap.text) +
          renderRealityGapPriority(review)
        : "") +
      "</section>"
    );
  }

  function renderRealityGapPriority(review) {
    var rows = review.realityGapPrioritization || [];
    if (!rows.length) return "";
    var html = rows
      .slice(0, 6)
      .map(function (g) {
        var label =
          g.priority === "ACTIONABLE_NOW"
            ? "Actionable now"
            : g.priority === "VALIDATE_FIRST"
              ? "Validate first"
              : "Monitor";
        return (
          "<tr><td>" +
          esc(g.label) +
          "</td><td>" +
          esc(label) +
          "</td><td>" +
          esc(g.why) +
          "</td></tr>"
        );
      })
      .join("");
    return (
      '<h3 class="drs-subhead">Reality Gap Prioritization</h3>' +
      renderTable(["Attribute", "Priority", "Why it matters operationally"], html)
    );
  }

  function renderEvidence(review) {
    var items = review.evidenceReview || [];
    return (
      '<section class="drs-section drs-section--flow adp-mr-section--evidence" data-adp-mr-section="evidence">' +
      '<h2 class="drs-section-h">3. Evidence Review</h2>' +
      items
        .map(function (ex, i) {
          return (
            '<article class="drs-evidence">' +
            '<p class="drs-label">Evidence ' +
            String(i + 1).padStart(2, "0") +
            "</p>" +
            '<h3 class="drs-evidence__title">' +
            esc(titleCaseNeed(ex.travelerNeed)) +
            "</h3>" +
            '<p class="drs-evidence__row"><strong>Provider:</strong> ' +
            esc(formatProviderLabel(ex.provider)) +
            "</p>" +
            '<p class="drs-evidence__row"><strong>Observation:</strong> ' +
            esc(ex.observation) +
            "</p>" +
            '<p class="drs-evidence__row"><strong>Why It Matters:</strong> ' +
            esc(ex.whyItMatters) +
            "</p>" +
            '<p class="drs-evidence__row"><strong>Management Review:</strong> ' +
            esc(ex.managementReview) +
            "</p>" +
            "</article>"
          );
        })
        .join("") +
      "</section>"
    );
  }

  function renderActions(review) {
    var actions = (review.actionAgenda || []).slice().sort(function (a, b) {
      var order = { PRIORITY_1: 0, PRIORITY_2: 1, MONITOR_SUPPORTING: 2 };
      var pa = order[a.managementPriority] != null ? order[a.managementPriority] : 9;
      var pb = order[b.managementPriority] != null ? order[b.managementPriority] : 9;
      return pa - pb;
    });
    var intro =
      '<p class="adp-mr-section-insight">Actions are sequenced against the primary inclusion issue. Priority 1 addresses inconsistent answer selection where competitors appear; Priority 2 supports representation; Monitor items are supporting hygiene.</p>';
    return (
      '<section class="drs-section drs-section--flow adp-mr-section--actions" data-adp-mr-section="actions">' +
      '<h2 class="drs-section-h">4. Management Action Agenda</h2>' +
      intro +
      actions
        .map(function (a, i) {
          var pri = priorityForAction(a, i);
          var n = String(i + 1).padStart(2, "0");
          return (
            '<article class="drs-action">' +
            '<div class="drs-action__keep">' +
            '<p class="drs-action__eyebrow">Action ' +
            n +
            "</p>" +
            '<h3 class="drs-action__title">' +
            esc(shortActionTitle(a.actionTitle)) +
            "</h3>" +
            '<div class="drs-action__scan">' +
            '<span class="drs-pill ' +
            pri.cls +
            '">' +
            esc(pri.label) +
            "</span>" +
            '<span class="drs-pill ' +
            statusPillClass(a.status) +
            '">' +
            esc(statusPillLabel(a.status)) +
            "</span>" +
            '<span class="drs-action__scan-meta">Owner: ' +
            esc(a.accountableOwnerRole || "—") +
            "</span>" +
            '<span class="drs-action__scan-meta">Due: ' +
            esc(a.targetDate || "Date to be confirmed") +
            "</span>" +
            "</div>" +
            (a.managementPriorityWhy
              ? '<p class="drs-muted">' + esc(a.managementPriorityWhy) + "</p>"
              : "") +
            '<div class="drs-action__field"><p class="drs-label">Observed Issue</p><p>' +
            esc(a.observedIssue) +
            "</p></div>" +
            "</div>" +
            (a.implementationSteps && a.implementationSteps.length
              ? '<div class="drs-action__field"><p class="drs-label">Recommended Action — Implementation Steps</p><ol>' +
                a.implementationSteps
                  .map(function (s) {
                    return "<li>" + esc(s) + "</li>";
                  })
                  .join("") +
                "</ol></div>"
              : '<div class="drs-action__field"><p class="drs-label">Recommended Action</p><p>' +
                esc(a.recommendedAction) +
                "</p></div>") +
            (a.targetSources && a.targetSources.length
              ? '<div class="drs-action__field"><p class="drs-label">Target Sources</p><p>' +
                esc(a.targetSources.join("; ")) +
                "</p></div>"
              : "") +
            (a.definitionOfDone && a.definitionOfDone.length
              ? '<div class="drs-action__field"><p class="drs-label">Definition of Done</p><ul>' +
                a.definitionOfDone
                  .map(function (s) {
                    return "<li>" + esc(s) + "</li>";
                  })
                  .join("") +
                "</ul></div>"
              : "") +
            '<div class="drs-action__field"><p class="drs-label">Expected Signal</p><p>' +
            esc(a.expectedSignal) +
            "</p></div>" +
            (a.nextMonitoringCheck
              ? '<div class="drs-action__field"><p class="drs-label">Next Monitoring Check</p><p>' +
                esc(a.nextMonitoringCheck) +
                "</p></div>"
              : "") +
            "</article>"
          );
        })
        .join("") +
      "</section>"
    );
  }

  function renderAccountability(review) {
    var prior = review.priorActionReview || {};
    var decisions = review.decisionsRequired || [];
    var next = review.nextMonitoringPriorities || [];
    return (
      '<section class="drs-section drs-section--flow adp-mr-section--accountability" data-adp-mr-section="accountability">' +
      '<h2 class="drs-section-h">5. Accountability &amp; Decisions</h2>' +
      '<div class="adp-mr-close-grid">' +
      '<div class="adp-mr-close-block">' +
      '<h3 class="drs-subhead">Prior Action Review</h3>' +
      '<p class="drs-body">' +
      esc(prior.message || "No prior monthly actions are available yet.") +
      "</p></div>" +
      '<div class="adp-mr-close-block">' +
      '<h3 class="drs-subhead">Decisions Required</h3>' +
      '<ol class="drs-list drs-list--numbered">' +
      decisions
        .map(function (d) {
          return "<li>" + esc(d.text || d) + "</li>";
        })
        .join("") +
      "</ol></div>" +
      '<div class="adp-mr-close-block">' +
      '<h3 class="drs-subhead">What Dealality Will Monitor Next</h3>' +
      '<ul class="drs-list">' +
      next
        .map(function (n) {
          return "<li>" + esc(n.text || n) + "</li>";
        })
        .join("") +
      "</ul></div>" +
      "</div>" +
      '<p class="adp-mr-method">' +
      esc(review.methodologyNote || "") +
      "</p>" +
      "</section>"
    );
  }

  /**
   * Build full PDF HTML for a canonical monthly review payload.
   * @param {object} review
   * @returns {string}
   */
  function buildReportHtml(review) {
    if (!review) return "";
    return (
      '<div class="brand-alignment-snapshot drs-report adp-mr-report-body" data-adp-mr-report="1" data-report-family="dealality-report-system-v1">' +
      renderCover(review) +
      renderExecutive(review) +
      renderMovement(review) +
      renderEvidence(review) +
      renderActions(review) +
      renderAccountability(review) +
      "</div>"
    );
  }

  global.AdpMonthlyReviewReportPdfV1 = {
    buildReportHtml: buildReportHtml,
    REPORT_FAMILY: "dealality-report-system-v1",
    PRODUCT_TITLE: "Dealality AI Demand Performance Review",
    PRODUCT_SUBTITLE: "Monthly Executive Review",
  };
})(typeof window !== "undefined" ? window : globalThis);
