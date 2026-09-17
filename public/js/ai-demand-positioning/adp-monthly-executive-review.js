/**
 * ADP Monthly Executive Review V1 — owner HTML renderer.
 * Live / Meeting / Print modes share one canonical payload.
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

  function kpiCard(k) {
    return (
      '<article class="adp-mr-kpi">' +
      '<p class="adp-mr-kpi__label">' +
      esc(k.label) +
      "</p>" +
      '<p class="adp-mr-kpi__value">' +
      esc(k.currentDisplay) +
      "</p>" +
      (k.deltaDisplay
        ? '<p class="adp-mr-kpi__delta">' + esc(k.deltaDisplay) + " vs prior</p>"
        : k.priorDisplay
          ? '<p class="adp-mr-kpi__delta">Prior ' + esc(k.priorDisplay) + "</p>"
          : "") +
      (k.meta ? '<p class="adp-mr-kpi__meta">' + esc(k.meta) + "</p>" : "") +
      (k.suppressedCompanion
        ? '<p class="adp-mr-kpi__note">' + esc(k.suppressedCompanion) + "</p>"
        : "") +
      "</article>"
    );
  }

  function callout(c) {
    if (!c) return "";
    return (
      '<article class="adp-mr-callout">' +
      "<h3>" +
      esc(c.title) +
      "</h3>" +
      "<p>" +
      esc(c.body) +
      "</p>" +
      "</article>"
    );
  }

  function renderMeeting(review) {
    const m = review.meetingMode || {};
    return (
      '<section class="adp-mr-block" data-section="meeting">' +
      "<h2>Meeting Mode</h2>" +
      '<p class="adp-mr-help">25–30 minute review agenda from the same canonical payload.</p>' +
      "<h3>Top changes</h3><ul>" +
      (m.topChanges || [])
        .map(
          (c) =>
            "<li><strong>" +
            esc(c.area) +
            "</strong>: " +
            esc(c.change) +
            " — " +
            esc(c.interpretation) +
            "</li>"
        )
        .join("") +
      "</ul>" +
      "<h3>Top actions</h3><ul>" +
      (m.topActions || [])
        .map(
          (a) =>
            "<li><strong>" +
            esc(a.title) +
            "</strong> · " +
            esc(a.owner) +
            " · " +
            esc(a.status) +
            " · Due: " +
            esc(a.dueDate) +
            "</li>"
        )
        .join("") +
      "</ul>" +
      "<h3>Prior open actions</h3><p>" +
      (m.priorOpenActions && m.priorOpenActions.length
        ? esc(JSON.stringify(m.priorOpenActions))
        : "No prior monthly actions are available yet.") +
      "</p>" +
      "<h3>Decisions required</h3><ul>" +
      (m.decisionsRequired || []).map((d) => "<li>" + esc(d) + "</li>").join("") +
      "</ul></section>"
    );
  }

  function renderFull(review) {
    const r = review.reporting || {};
    const c = review.callouts || {};
    return (
      '<header class="adp-mr-header">' +
      '<p class="adp-mr-eyebrow">' +
      esc(review.productSubtitle) +
      "</p>" +
      "<h1>" +
      esc(review.productTitle) +
      "</h1>" +
      '<p class="adp-mr-property">' +
      esc(review.property?.name) +
      "</p>" +
      '<p class="adp-mr-dates">Reporting month: <strong>' +
      esc(r.reportingMonth) +
      "</strong> · Current monitoring: <strong>" +
      esc(r.currentMonitoringDate) +
      "</strong> · Prior run: <strong>" +
      esc(r.priorRunDate || "—") +
      "</strong></p>" +
      (r.comparability?.disclosure
        ? '<p class="adp-mr-disclosure">' + esc(r.comparability.disclosure) + "</p>"
        : "") +
      "</header>" +
      '<section class="adp-mr-block" data-section="executive">' +
      "<h2>Executive Performance</h2>" +
      '<div class="adp-mr-kpi-row">' +
      (review.kpis || []).map(kpiCard).join("") +
      "</div>" +
      '<div class="adp-mr-assessment"><h3>Executive Assessment</h3><p>' +
      esc(review.executiveAssessment?.text) +
      "</p></div>" +
      '<div class="adp-mr-callouts">' +
      callout(c.positiveSignal) +
      callout(c.secondaryCallout) +
      callout(c.managementPriority) +
      "</div></section>" +
      '<section class="adp-mr-block" data-section="changed">' +
      "<h2>What Changed</h2>" +
      '<div class="adp-mr-table-wrap"><table class="adp-mr-table"><thead><tr>' +
      "<th>Metric / Demand Area</th><th>Current</th><th>Prior</th><th>Change</th><th>Executive Interpretation</th>" +
      "</tr></thead><tbody>" +
      (review.materialMovements || [])
        .map(
          (row) =>
            "<tr><td>" +
            esc(row.metricOrDemandArea) +
            "</td><td>" +
            esc(row.current) +
            "</td><td>" +
            esc(row.prior) +
            "</td><td>" +
            esc(row.change) +
            "</td><td>" +
            esc(row.executiveInterpretation) +
            "</td></tr>"
        )
        .join("") +
      "</tbody></table></div></section>" +
      '<section class="adp-mr-block" data-section="demand">' +
      "<h2>Demand Movement</h2>" +
      '<div class="adp-mr-table-wrap"><table class="adp-mr-table"><thead><tr>' +
      "<th>Demand Territory</th><th>Current</th><th>Captured</th><th>Significance</th><th>Interpretation</th>" +
      "</tr></thead><tbody>" +
      (review.demandMovement || [])
        .map(
          (row) =>
            "<tr><td>" +
            esc(row.demandTerritory) +
            "</td><td>" +
            esc(row.current) +
            "</td><td>" +
            esc(row.captured) +
            "</td><td>" +
            esc(row.significance) +
            "</td><td>" +
            esc(row.interpretation) +
            "</td></tr>"
        )
        .join("") +
      "</tbody></table></div>" +
      '<p class="adp-mr-help">' +
      esc((review.demandMovement || [])[0]?.note || "") +
      "</p></section>" +
      '<section class="adp-mr-block" data-section="competitive">' +
      "<h2>Competitive Movement</h2>" +
      "<p>" +
      esc(review.competitiveMovement?.summary) +
      "</p>" +
      '<div class="adp-mr-table-wrap"><table class="adp-mr-table"><thead><tr><th>Competitor</th><th>Displacement count</th></tr></thead><tbody>' +
      (review.competitiveMovement?.displacementLeaders || [])
        .map(
          (d) =>
            "<tr><td>" +
            esc(d.name) +
            "</td><td>" +
            esc(d.displacementCount) +
            "</td></tr>"
        )
        .join("") +
      "</tbody></table></div>" +
      '<p class="adp-mr-help">' +
      esc(review.competitiveMovement?.caveat) +
      "</p></section>" +
      '<section class="adp-mr-block" data-section="evidence">' +
      "<h2>Evidence Review</h2>" +
      (review.evidenceReview || [])
        .map(
          (ex) =>
            '<article class="adp-mr-evidence">' +
            '<p class="adp-mr-evidence__need"><strong>Traveler Need:</strong> ' +
            esc(ex.travelerNeed) +
            "</p>" +
            "<p><strong>Provider:</strong> " +
            esc(ex.provider) +
            "</p>" +
            "<p><strong>Observation:</strong> " +
            esc(ex.observation) +
            "</p>" +
            "<p><strong>Why It Matters:</strong> " +
            esc(ex.whyItMatters) +
            "</p>" +
            "<p><strong>Management Review:</strong> " +
            esc(ex.managementReview) +
            "</p></article>"
        )
        .join("") +
      "</section>" +
      '<section class="adp-mr-block" data-section="actions">' +
      "<h2>Management Action Agenda</h2>" +
      (review.actionAgenda || [])
        .map(
          (a) =>
            '<article class="adp-mr-action">' +
            "<h3>" +
            esc(a.actionTitle) +
            "</h3>" +
            "<p><strong>Observed Issue</strong><br>" +
            esc(a.observedIssue) +
            "</p>" +
            (a.evidence
              ? "<p><strong>Evidence</strong><br>" + esc(a.evidence) + "</p>"
              : "") +
            (a.actionPatternId
              ? "<p><strong>Action Pattern ID</strong><br>" +
                esc(a.actionPatternId) +
                "</p>"
              : "") +
            (a.targetSources && a.targetSources.length
              ? "<p><strong>Target Sources</strong><br>" +
                esc(a.targetSources.join("; ")) +
                "</p>"
              : "") +
            (a.implementationSteps && a.implementationSteps.length
              ? "<p><strong>Exact Implementation Steps</strong></p><ol>" +
                a.implementationSteps
                  .map((s) => "<li>" + esc(s) + "</li>")
                  .join("") +
                "</ol>"
              : "<p><strong>Recommended Action</strong><br>" +
                esc(a.recommendedAction) +
                "</p>") +
            (a.definitionOfDone && a.definitionOfDone.length
              ? "<p><strong>Definition of Done</strong></p><ul>" +
                a.definitionOfDone.map((s) => "<li>" + esc(s) + "</li>").join("") +
                "</ul>"
              : "") +
            "<p><strong>Why This Matters</strong><br>" +
            esc(a.rationale) +
            "</p>" +
            '<dl class="adp-mr-action__meta">' +
            "<div><dt>Accountable Owner</dt><dd>" +
            esc(a.accountableOwnerRole) +
            "</dd></div>" +
            "<div><dt>Supporting Team</dt><dd>" +
            esc(a.supportingTeam || "—") +
            "</dd></div>" +
            "<div><dt>Target Completion</dt><dd>" +
            esc(a.targetDate || "Date to be confirmed") +
            "</dd></div>" +
            "<div><dt>Status</dt><dd>" +
            esc(a.status) +
            "</dd></div></dl>" +
            "<p><strong>Expected Signal</strong><br>" +
            esc(a.expectedSignal) +
            "</p>" +
            (a.nextMonitoringCheck
              ? "<p><strong>Next Monitoring Check</strong><br>" +
                esc(a.nextMonitoringCheck) +
                "</p>"
              : "") +
            "</article>"
        )
        .join("") +
      "</section>" +
      '<section class="adp-mr-block" data-section="prior">' +
      "<h2>Prior Action Review</h2>" +
      "<p>" +
      esc(review.priorActionReview?.message || "No prior monthly actions are available yet.") +
      "</p></section>" +
      '<section class="adp-mr-block" data-section="decisions">' +
      "<h2>Management Decisions / Confirmations Required</h2><ul>" +
      (review.decisionsRequired || [])
        .map((d) => "<li>" + esc(d.text) + "</li>")
        .join("") +
      "</ul></section>" +
      '<section class="adp-mr-block" data-section="next">' +
      "<h2>What Dealality Will Monitor Next</h2><ul>" +
      (review.nextMonitoringPriorities || [])
        .map((n) => "<li>" + esc(n.text) + "</li>")
        .join("") +
      "</ul></section>" +
      '<p class="adp-mr-method">' +
      esc(review.methodologyNote) +
      "</p>"
    );
  }

  function render(review, opts) {
    opts = opts || {};
    const mode = opts.mode || "live";
    const root = opts.root;
    if (!root || !review) return;
    let html = '<div class="adp-mr" data-mode="' + esc(mode) + '">';
    if (mode === "meeting") html += renderMeeting(review);
    else html += renderFull(review);
    if (mode === "live") html += renderMeeting(review);
    html += "</div>";
    root.innerHTML = html;
    root.hidden = false;
  }

  global.AdpMonthlyExecutiveReview = {
    render: render,
    renderFull: renderFull,
    renderMeeting: renderMeeting,
  };
})(typeof window !== "undefined" ? window : globalThis);
