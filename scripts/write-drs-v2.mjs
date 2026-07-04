import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const out = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "js", "deal-readiness-snapshot.js");

const js = String.raw`/**
 * Deal Readiness Snapshot — two-page document renderer (narrative + technical).
 * Data: POST /api/ai/deal-readiness-review (buildReadinessFromFields).
 */
(function (global) {
  "use strict";

  var OUTPUT_NOTE =
    "This Dealality output organizes readiness signals based on current deal inputs. " +
    "It is intended to support internal owner/advisor review and does not constitute a recommendation, " +
    "endorsement, valuation, legal advice, franchise advice, or investment advice.";

  var REVIEW_AREAS = [
    { label: "Basic Project Information", tabs: ["Project Overview"] },
    { label: "Ownership / Control", tabs: ["Location & Site Details", "Deal & Capital Structure"] },
    { label: "Market Context", tabs: ["Location & Site Details", "Market & Performance"] },
    { label: "Brand Review Readiness", tabs: ["Brand & Op. Status"] },
    { label: "Operator Review Readiness", tabs: ["Brand & Op. Status", "Operational Expectations"] },
    { label: "Capex / PIP Clarity", tabs: ["Deal & Capital Structure", "Property Specs"] },
    { label: "Agreement Strategy", tabs: ["Lease Structure", "Deal & Capital Structure"] },
    { label: "Documentation Package", tabs: ["Uploads & Attachments", "Deal Room"] },
  ];

  var WORKFLOW_STEPS = [
    "Deal intake",
    "Readiness review",
    "Owner clarification",
    "Brand alignment review",
    "Operator capability review",
    "Opportunity brief",
    "External outreach",
  ];

  function esc(t) {
    return String(t == null ? "" : t)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function strVal(v) {
    if (v == null || v === "") return "";
    if (typeof v === "string") return v.trim();
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
    if (Array.isArray(v)) {
      return v
        .map(function (x) {
          return typeof x === "string" ? x : x && x.name ? String(x.name) : "";
        })
        .filter(Boolean)
        .join(", ");
    }
    if (typeof v === "object" && v.name) return String(v.name).trim();
    return String(v).trim();
  }

  function fieldPresent(fields, names) {
    if (!fields) return "";
    for (var i = 0; i < names.length; i++) {
      var v = strVal(fields[names[i]]);
      if (v) return v;
    }
    return "";
  }

  function formatDate(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10);
      return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
    } catch (_) {
      return String(iso).slice(0, 10);
    }
  }

  function reviewPriorityLabel(severity) {
    var s = String(severity || "").toLowerCase();
    if (s === "high" || s === "critical") return "High";
    if (s === "medium" || s === "warning") return "Medium";
    return "Low";
  }

  function whyItMattersNeutral(field, tab) {
    var tabPart = tab ? " (" + tab + ")" : "";
    return (
      "Based on current deal inputs, this area may affect completeness" +
      tabPart +
      " and how readiness signals are summarized for owner/advisor validation."
    );
  }

  function fieldLabel(item) {
    if (!item) return "";
    if (typeof item === "string") return item;
    return item.label || item.field || item.highlightField || "";
  }

  function tabScoreMap(data) {
    var map = {};
    var labeled =
      data.tabScoresLabeled && data.tabScoresLabeled.length
        ? data.tabScoresLabeled
        : data.sectionScoresLabeled || [];
    labeled.forEach(function (row) {
      var key = row.label || row.id;
      if (key) map[key] = row.score;
    });
    var sections = data.tabScores || data.sectionScores || {};
    Object.keys(sections).forEach(function (k) {
      if (map[k] == null) map[k] = sections[k];
    });
    return map;
  }

  function avgTabPct(tabMap, tabs) {
    var sum = 0;
    var n = 0;
    tabs.forEach(function (t) {
      if (tabMap[t] != null && tabMap[t] !== "") {
        sum += Number(tabMap[t]);
        n += 1;
      }
    });
    if (!n) return null;
    return Math.round(sum / n);
  }

  function countMissingInTabs(data, tabs) {
    var n = 0;
    (data.missingInformation || []).forEach(function (m) {
      if (!m) return;
      var tab = m.relatedTab || m.section || "";
      if (tabs.indexOf(tab) >= 0) n += 1;
    });
    return n;
  }

  function pctToStatus(pct, missingInArea) {
    if (pct == null && missingInArea > 2) return "Not Provided";
    if (pct == null) return missingInArea > 0 ? "Needs Clarification" : "Not Provided";
    if (missingInArea >= 3 || pct < 25) return "Needs Clarification";
    if (pct >= 90 && missingInArea === 0) return "Strong";
    if (pct >= 75) return "Mostly Clear";
    if (pct >= 60) return "Partially Complete";
    if (pct >= 40) return "Developing";
    return "Needs Clarification";
  }

  function statusNote(status, pct, missingInArea) {
    if (status === "Strong") return "Current inputs suggest this review area is largely complete.";
    if (status === "Mostly Clear") return "Current inputs suggest this area is mostly documented with limited gaps.";
    if (status === "Partially Complete") return "Current inputs suggest core items are present; some fields may require clarification.";
    if (status === "Developing") return "Current inputs suggest this area is only partially documented.";
    if (status === "Needs Clarification") {
      return (
        "Current inputs suggest " +
        (missingInArea ? missingInArea + " gap(s)" : "gaps") +
        " may require clarification in this area."
      );
    }
    return "Current inputs do not yet provide enough detail for this review area.";
  }

  function buildReviewAreaRows(data) {
    var tabMap = tabScoreMap(data);
    return REVIEW_AREAS.map(function (area) {
      var miss = countMissingInTabs(data, area.tabs);
      var pct = avgTabPct(tabMap, area.tabs);
      var status = pctToStatus(pct, miss);
      return {
        area: area.label,
        status: status,
        notes: statusNote(status, pct, miss),
      };
    });
  }

  function buildClarificationAreas(data) {
    var seen = {};
    var rows = [];
    function pushRow(row) {
      var f = String(row.field || row.label || "").trim();
      var key = (row.tab || "") + "|" + f.toLowerCase();
      if (!f || seen[key]) return;
      seen[key] = true;
      rows.push({
        area: f,
        whyItMatters: row.whyItMatters || whyItMattersNeutral(f, row.tab),
        priority: row.priority || "Medium",
      });
    }
    (data.missingInformation || []).forEach(function (m) {
      if (!m) return;
      pushRow({ field: m.field || m.label, tab: m.relatedTab || m.section, priority: "High" });
    });
    (data.weakInformation || []).forEach(function (w) {
      if (!w) return;
      pushRow({
        field: w.field || w.label,
        tab: w.relatedTab || w.section,
        priority: "Medium",
      });
    });
    var plan = data.scoreImprovementPlan || {};
    (plan.priorityActions || []).forEach(function (a) {
      if (!a) return;
      var f = a.relatedField || "";
      if (!f && a.label) f = String(a.label).replace(/^Complete\\s+["']?|["']$/g, "").trim();
      pushRow({
        field: f || a.label,
        tab: a.relatedTab || "",
        priority: reviewPriorityLabel(a.severity),
      });
    });
    rows.sort(function (a, b) {
      var pr = { High: 0, Medium: 1, Low: 2 };
      return (pr[a.priority] != null ? pr[a.priority] : 2) - (pr[b.priority] != null ? pr[b.priority] : 2);
    });
    return rows.slice(0, 24);
  }

  function buildNarrativeStrengths(data, meta, fields) {
    var items = [];
    if (meta.projectType && meta.projectType !== "—") {
      items.push("Clear project type identified: " + meta.projectType + ".");
    }
    if (meta.market && meta.market !== "—") {
      items.push("Location/market information appears on file for " + meta.market + ".");
    }
    if (meta.country && meta.country !== "—") {
      items.push("Country context provided: " + meta.country + ".");
    }
    if (meta.keyCount && meta.keyCount !== "—") {
      items.push("Relevant asset scale identified (" + meta.keyCount + " keys/rooms).");
    }
  var ownerObj = fieldPresent(fields, [
      "Owner's Objective for the Project",
      "Owner Objective",
      "Owner's Objective",
      "Strategic Intent Summary",
    ]);
    if (ownerObj) items.push("Owner objective or strategic intent appears documented.");
    var brandSt = fieldPresent(fields, [
      "Is the hotel currently branded?",
      "Is the hotel currently managed by a third-party operator?",
    ]);
    if (brandSt) items.push("Brand/operator status fields appear on file.");
    var timeline = fieldPresent(fields, [
      "Decision Timeline for Brand/Operator",
      "Development / Renovation Timeline Importance",
      "Expected Opening Date",
    ]);
    if (timeline) items.push("Timeline-related information appears on file.");
    var agreement = fieldPresent(fields, [
      "Preferred Deal Structure",
      "Lease Structure Type",
      "Franchise vs Management Preference",
    ]);
    if (agreement) items.push("Agreement or deal-structure preference appears documented.");
    var flexible = fieldPresent(fields, [
      "Are you open to lesser-known or emerging brands with favorable terms?",
    ]);
    if (/yes|open|flexible|willing/i.test(flexible)) {
      items.push("Current inputs suggest flexibility in brand review path where applicable.");
    }
    if (!items.length) {
      items.push(
        "Current inputs suggest limited documented strengths; additional Deal Setup fields may improve narrative completeness."
      );
    }
    return items.slice(0, 10);
  }

  function scoreInterpretation(score, stage, data) {
    var n = Number(score);
    var missing = (data.missingInformation || []).length;
    var blocking = (data.blockingIssues || []).length;
    if (!Number.isFinite(n)) {
      return (
        "Current inputs do not yet support a full readiness score. Additional Deal Setup fields " +
        "may require clarification before a structured review summary can be prepared."
      );
    }
    if (blocking > 0) {
      return (
        "Current inputs suggest blocking clarification signals are present. These items may require " +
        "owner/advisor validation before the opportunity is treated as ready for broader structured review."
      );
    }
    if (n >= 90 && missing === 0) {
      return (
        "Current inputs suggest this opportunity has substantial information to support a structured internal review. " +
        "Remaining gaps, if any, appear limited relative to required Deal Setup fields."
      );
    }
    if (n >= 75 || /ready for external/i.test(String(stage))) {
      return (
        "This opportunity has enough information to support an initial structured review. However, several " +
        "important inputs should be clarified before formal brand/operator outreach or LOI-level conversations."
      );
    }
    if (n >= 60 || String(stage).toLowerCase() === "shaping") {
      return (
        "Current inputs suggest the opportunity is taking shape but may require clarification across multiple " +
        "categories before it is treated as complete enough for controlled external review."
      );
    }
    return (
      "Current inputs suggest the opportunity is still in an early intake phase. Additional core fields may " +
      "require clarification before a structured readiness narrative can be relied upon for review."
    );
  }

  function buildBusinessSummary(data, meta, fields) {
    var parts = [];
    var pt = meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    if (pt) {
      parts.push(
        "Based on current deal inputs, this appears to be a " +
          pt +
          " hospitality opportunity" +
          (meta.keyCount && meta.keyCount !== "—" ? " at approximately " + meta.keyCount + " keys/rooms" : "") +
          "."
      );
    } else {
      parts.push(
        "Based on current deal inputs, the opportunity type is not fully specified in Deal Setup; project classification may require clarification."
      );
    }
    var locBits = [];
    if (meta.market && meta.market !== "—") locBits.push(meta.market);
    if (meta.country && meta.country !== "—") locBits.push(meta.country);
    if (locBits.length) {
      parts.push("Location/market context on file includes: " + locBits.join(", ") + ".");
    } else {
      parts.push("Location/market context appears limited in current inputs.");
    }
    if (meta.targetPositioning) {
      parts.push("Target positioning noted: " + meta.targetPositioning + ".");
    }
    var score = Number(data.dealReadinessScore);
    var missing = (data.missingInformation || []).length;
    if (Number.isFinite(score)) {
      if (score >= 75 && missing <= 3) {
        parts.push(
          "Current inputs suggest the package may be complete enough for an initial structured internal review, " +
            "subject to owner/advisor validation of flagged clarification areas."
        );
      } else if (score >= 60) {
        parts.push(
          "Current inputs suggest the package is partially complete. Several categories may still require " +
            "clarification before controlled external review."
        );
      } else {
        parts.push(
          "Current inputs suggest the package is not yet complete enough for a full structured review narrative; " +
            "core intake fields may require clarification."
        );
      }
    }
    if (missing > 0) {
      parts.push(
        "Primary clarification categories include " +
          missing +
          " missing required field signal(s) identified in the technical readiness review."
      );
    } else if (Number.isFinite(score)) {
      parts.push("No missing required fields were identified in the latest readiness run.");
    }
    if (data.humanReadableSummary) {
      parts.push(data.humanReadableSummary);
    }
    return parts.join(" ");
  }

  function mapReviewStatusLabel(stage, score) {
    var s = String(stage || "").trim();
    var sl = s.toLowerCase();
    var n = Number(score);
    if (!Number.isFinite(n) && !s) {
      return {
        label: "Needs Readiness Review",
        explain: "A readiness review has not been saved or scored for this deal based on current inputs.",
      };
    }
    if (sl === "discovery") {
      return {
        label: "Needs Initial Intake",
        explain: "Current inputs suggest early-stage intake; additional core fields may require clarification.",
      };
    }
    if (sl === "shaping") {
      return {
        label: "Needs Clarification",
        explain: "Current inputs suggest the opportunity is taking shape but clarification areas remain.",
      };
    }
    if (sl === "advancing") {
      return {
        label: "Eligible for Structured Review",
        explain: "Current inputs suggest the opportunity may support structured internal review with validation of flagged items.",
      };
    }
    if (sl.indexOf("ready for external") >= 0) {
      return {
        label: "Ready for Controlled Review",
        explain: "Current inputs suggest controlled internal or advisor review may be appropriate before broad circulation.",
      };
    }
    if (sl === "ready") {
      return {
        label: "Ready for Advanced Review",
        explain: "Current inputs suggest a high level of field completeness relative to required Deal Setup inputs.",
      };
    }
    if (Number.isFinite(n) && n >= 75) {
      return {
        label: "Eligible for Structured Review",
        explain: "Current inputs suggest structured review may be appropriate subject to owner/advisor validation.",
      };
    }
    return {
      label: "Needs Clarification",
      explain: "Current inputs suggest clarification is needed before treating readiness as complete.",
    };
  }

  function workflowStatusForStep(step, data, stage, score) {
    var n = Number(score);
    var missing = (data.missingInformation || []).length;
    var sl = String(stage || "").toLowerCase();
    switch (step) {
      case "Deal intake":
        return Number.isFinite(n) ? "Complete enough for initial review" : "Needed";
      case "Readiness review":
        return "Completed";
      case "Owner clarification":
        return missing > 0 ? "Needed" : "Complete enough for initial review";
      case "Brand alignment review":
        return n >= 60 ? "Available" : "Needed";
      case "Operator capability review":
        return n >= 60 ? "Available" : "Needed";
      case "Opportunity brief":
        return n >= 75 ? "Draftable" : "Not yet ready for broad circulation";
      case "External outreach":
        if (n >= 90 && missing === 0) return "Available";
        if (n >= 75) return "Available";
        return "Not yet ready for broad circulation";
      default:
        return "Needed";
    }
  }

  function buildWorkflowRows(data, stage, score) {
    return WORKFLOW_STEPS.map(function (step) {
      return { step: step, status: workflowStatusForStep(step, data, stage, score) };
    });
  }

  function dealMetaFromSources(sources) {
    sources = sources || {};
    var deal = sources.deal || {};
    var fields = sources.fields || sources.sourceFields || {};
    if (deal.fields && typeof deal.fields === "object") fields = deal.fields;
    var norm = sources.normalized || {};
    var listDeal = sources.listDeal || {};
    var targetPositioning =
      fieldPresent(fields, [
        "Brand Positioning",
        "Target Chain Scale",
        "Hotel Chain Scale",
        "Brand positioning preference",
      ]) || strVal(norm.chainScale) || strVal(norm.brandPositioning) || "";
    return {
      projectName:
        listDeal.projectName ||
        deal.projectName ||
        strVal(fields["Property Name"]) ||
        strVal(norm.propertyName) ||
        "Deal",
      market:
        listDeal.hotelLocation ||
        deal.hotelLocation ||
        strVal(fields["Hotel Submarket & Location"]) ||
        strVal(fields["City & State"]) ||
        strVal(norm.submarket) ||
        strVal(norm.city) ||
        "—",
      country: strVal(fields["Country"]) || strVal(norm.country) || "—",
      keyCount:
        strVal(fields["Total Number of Rooms/Keys"]) || strVal(norm.totalNumberOfRoomsKeys) || "—",
      projectType:
        listDeal.projectType ||
        deal.projectType ||
        strVal(fields["Project Type"]) ||
        strVal(norm.projectType) ||
        "—",
      targetPositioning: targetPositioning,
    };
  }

  function renderTabGrid(data) {
    var labeled = data.tabScoresLabeled && data.tabScoresLabeled.length ? data.tabScoresLabeled : null;
    var sections = data.tabScores || data.sectionScores || {};
    var html = '<motionless></motionless>'.slice(0, 0);
    html = '<div class="drs-tab-grid">';
    if (labeled) {
      labeled.forEach(function (row) {
        var pctStr = row.score == null || row.score === "" ? "—" : esc(row.score) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr +
          '</div><motionless></motionless>'.slice(0, 0) +
          '<div class="drs-tab-label">' +
          esc(row.label || row.id) +
          "</div></div>";
      });
    } else {
      Object.keys(sections).forEach(function (k) {
        var v = sections[k];
        var pctStr2 = v == null || v === "" ? "—" : esc(v) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr2 +
          '</div><div class="drs-tab-label">' +
          esc(k) +
          "</div></div>";
      });
    }
    html += "</motionless></motionless>".slice(0, 0) + "</div>";
    return html;
  }

  function renderDetailSection(title, items, emptyMsg) {
    var html = '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">' + esc(title) + "</h2>";
    if (!items || !items.length) {
      html += '<p class="drs-muted">' + esc(emptyMsg) + "</p></section>";
      return html;
    }
    html += '<ul class="drs-detail-list">';
    items.forEach(function (item) {
      if (typeof item === "string") {
        html += "<li>" + esc(item) + "</li>";
        return;
      }
      var line = fieldLabel(item);
      var tab = item.relatedTab || item.section || "";
      if (tab) line += " (" + tab + ")";
      html += "<li>" + esc(line) + "</li>";
    });
    html += "</ul></section>";
    return html;
  }

  function renderPage1Narrative(data, options, ctx) {
    var meta = ctx.meta;
    var fields = ctx.fields;
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var generatedAt = options.generatedAt || data.savedAt || new Date().toISOString();
    var reviewStatus = mapReviewStatusLabel(stage, score);
    var areaRows = buildReviewAreaRows(data);
    var clarifications = buildClarificationAreas(data);
    var strengths = buildNarrativeStrengths(data, meta, fields);
    var workflowRows = buildWorkflowRows(data, stage, score);
    var subtitleParts = [];
    if (meta.keyCount && meta.keyCount !== "—") subtitleParts.push(meta.keyCount + " keys");
    if (meta.projectType && meta.projectType !== "—") subtitleParts.push(meta.projectType);
    if (meta.targetPositioning) subtitleParts.push(meta.targetPositioning);

    var html = '<section class="drs-page-sheet drs-page-narrative drs-avoid-break">';
    html += '<header class="drs-doc-header">';
    html += '<p class="drs-doc-kicker">DEALALITY DEAL READINESS SNAPSHOT</p>';
    html += '<h1 class="drs-deal-title">' + esc(meta.projectName) + "</h1>";
    if (subtitleParts.length) {
      html += '<p class="drs-deal-subtitle">' + esc(subtitleParts.join(" · ")) + "</p>";
    }
    html += '<dl class="drs-meta-grid drs-meta-grid--header">';
    html += "<div><dt>Market</dt><dd>" + esc(meta.market) + "</dd></div>";
    html += "<div><dt>Country</dt><dd>" + esc(meta.country) + "</dd></div>";
    html += "<div><dt>Generated</dt><dd>" + esc(formatDate(generatedAt)) + " (current deal inputs)</dd></div>";
    html += "<div><dt>Output type</dt><dd>Internal owner/advisor review</dd></motionless></motionless>".slice(0, 0) + "</motionless></motionless>".slice(0, 0) + "</motionless></motionless>".slice(0, 0) + "</div>";
    html += "<div><dt>Status</dt><dd>Draft for validation</dd></div>";
    html += "</dl></header>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Score</h2>';
    html += '<div class="drs-score-block">';
    html += '<div class="drs-score-main"><span class="drs-score-num">' + esc(score != null && score !== "" ? score : "—") + '</span><span class="drs-score-of">/ 100</span></div>';
    html += '<div class="drs-score-stage"><span class="drs-score-stage-label">Current Readiness Stage</span><span class="drs-score-stage-value">' + esc(stage) + "</span></div>";
    html += "</div>";
    html += '<p class="drs-interpretation">' + esc(scoreInterpretation(score, stage, data)) + "</p></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Summary</h2>';
    html += '<p class="drs-summary">' + esc(buildBusinessSummary(data, meta, fields)) + "</p></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Breakdown</h2>';
    html += '<div class="drs-table-wrap"><table class="drs-breakdown-table"><thead><tr><th>Review Area</th><th>Status</th><th>Notes</th></tr></thead><tbody>';
    areaRows.forEach(function (row) {
      html += "<tr><td>" + esc(row.area) + "</td><td>" + esc(row.status) + "</td><td>" + esc(row.notes) + "</td></tr>";
    });
    html += "</tbody></table></div></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Key Strengths Identified</h2>';
    html += '<ul class="drs-strength-cards">';
    strengths.forEach(function (s) {
      html += '<li class="drs-strength-card drs-avoid-break">' + esc(s) + "</li>";
    });
    html += "</ul></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Primary Clarification Areas</h2>';
    if (!clarifications.length) {
      html += '<p class="drs-muted">Current inputs suggest no primary clarification areas at this time.</p>';
    } else {
      html += '<div class="drs-table-wrap"><table class="drs-clar-table"><thead><tr><th>Clarification Area</th><th>Why It Matters</th><th>Priority</th></tr></thead><tbody>';
      clarifications.forEach(function (row) {
        html += "<tr><td>" + esc(row.area) + "</td><td>" + esc(row.whyItMatters) + "</td><td>" + esc(row.priority) + "</td></tr>";
      });
      html += "</tbody></table></div>";
    }
    html += "</section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Current Review Status</h2>';
    html += '<p class="drs-review-status-label">' + esc(reviewStatus.label) + "</p>";
    html += '<p class="drs-muted">' + esc(reviewStatus.explain) + "</p></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Suggested Workflow Status</h2>';
    html += '<motionless></motionless>'.slice(0, 0);
    html += '<div class="drs-table-wrap"><table class="drs-breakdown-table"><thead><tr><th>Step</th><th>Current Status</th></tr></thead><tbody>';
    workflowRows.forEach(function (row) {
      html += "<tr><td>" + esc(row.step) + "</td><td>" + esc(row.status) + "</td></tr>";
    });
    html += "</tbody></table></div></section>";

    html += '<footer class="drs-output-note drs-avoid-break"><h2 class="drs-section-title">Output Note</h2><p>' + esc(OUTPUT_NOTE) + "</p></footer>";
    html += "</section>";
    return html;
  }

  function renderPage2Technical(data, options, ctx) {
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var missing = (data.missingInformation || []).length;
    var weak = (data.weakInformation || []).length;
    var blocking = (data.blockingIssues || []).length;
    var lastReviewed = data.savedAt || options.generatedAt || "";

    var html = '<section class="drs-page-sheet drs-page-technical drs-page-break">';
    html += '<header class="drs-tech-header drs-avoid-break">';
    html += '<p class="drs-page-label drs-no-print">Page 2</p>';
    html += '<h2 class="drs-tech-title">Technical Readiness Detail</h2>';
    html += '<p class="drs-muted">Supporting field-level readiness information</p></header>';

    html += '<section class="drs-section drs-avoid-break"><h3 class="drs-section-title">Tab / Section Score Grid</h3>';
    html += renderTabGrid(data) + "</section>";

    html += renderDetailSection(
      "Missing Information",
      data.missingInformation || [],
      "Current inputs suggest no missing required fields."
    );
    html += renderDetailSection(
      "Weak Information",
      data.weakInformation || [],
      "Current inputs suggest no weak text fields."
    );
    html += renderDetailSection(
      "Blocking Clarification Areas",
      data.blockingIssues || [],
      "Current inputs suggest no blocking signals."
    );

    if (options.editDealHref) {
      html += '<section class="drs-section drs-no-print drs-avoid-break"><h3 class="drs-section-title">Field-Level Gap Links</h3>';
      html += '<p><a class="drs-edit-link" data-deal-readiness-form="1" href="' + esc(options.editDealHref) + '">Edit deal — highlight gaps on each tab →</a></p>';
      html += '<p class="drs-muted">Opens Deal Setup; save there to update readiness signals, then re-run this snapshot.</p></section>';
    }

    html += '<section class="drs-section drs-avoid-break"><h3 class="drs-section-title">Raw Readiness Counts</h3>';
    html += '<table class="drs-status-table"><tbody>';
    html += "<tr><th>Readiness score</th><td>" + esc(score != null && score !== "" ? score + " / 100" : "—") + "</td></tr>";
    html += "<tr><th>Readiness stage</th><td>" + esc(stage) + "</td></tr>";
    html += "<tr><th>Missing count</th><td>" + esc(missing) + "</td></tr>";
    html += "<tr><th>Weak count</th><td>" + esc(weak) + "</td></tr>";
    html += "<tr><th>Blocking count</th><td>" + esc(blocking) + "</td></tr>";
    html += "<tr><th>Last reviewed</th><td>" + esc(lastReviewed ? formatDate(lastReviewed) : "—") + "</td></tr>";
    html += "</tbody></table></section>";

    html += '<footer class="drs-output-note drs-output-note--technical drs-avoid-break"><p>' + esc(OUTPUT_NOTE) + "</p></footer>";
    html += "</section>";
    return html;
  }

  function buildHtml(data, options) {
    options = options || {};
    var fields = data.sourceFields || (data.deal && data.deal.fields) || {};
    var meta = options.dealMeta || dealMetaFromSources({
      deal: data.deal,
      fields: fields,
      normalized: data.normalized,
      listDeal: options.listDeal,
    });
    var ctx = { meta: meta, fields: fields };

    var html = "";
    html += '<div class="deal-readiness-snapshot' + (options.embed ? " drs--embed" : "") + '">';
    html += '<div class="drs-toolbar drs-no-print"><div class="drs-toolbar-actions">';
    html += '<button type="button" class="drs-btn drs-btn-primary" data-drs-print>Print / Save as PDF</button>';
    if (options.fullPageHref) {
      html += ' <a class="drs-btn drs-btn-secondary" href="' + esc(options.fullPageHref) + '" target="_blank" rel="noopener">Open full page</a>';
    }
    html += "</div></div>";

    html += '<article class="drs-document">';
    html += '<p class="drs-page-label drs-page-label--first drs-no-print">Page 1 — Readiness Narrative</p>';
    html += renderPage1Narrative(data, options, ctx);
    html += '<p class="drs-page-label drs-no-print">Page 2 — Technical Readiness Detail</p>';
    html += renderPage2Technical(data, options, ctx);
    if (options.footerHtml) {
      html += '<div class="drs-host-footer drs-no-print">' + options.footerHtml + "</div>";
    }
    html += "</article></div>";
    return html;
  }

  function bindPrint(root) {
    if (!root) return;
    var btn = root.querySelector("[data-drs-print]");
    if (btn && !btn._drsPrintBound) {
      btn._drsPrintBound = true;
      btn.addEventListener("click", function () {
        global.print();
      });
    }
  }

  function render(container, data, options) {
    if (!container || !data) return null;
    options = options || {};
    container.innerHTML = buildHtml(data, options);
    bindPrint(container);
    return {
      meta: options.dealMeta || dealMetaFromSources({
        deal: data.deal,
        fields: data.sourceFields,
        normalized: data.normalized,
        listDeal: options.listDeal,
      }),
      clarifications: buildClarificationAreas(data),
    };
  }

  global.DealReadinessSnapshot = {
    render: render,
    buildHtml: buildHtml,
    dealMetaFromSources: dealMetaFromSources,
    buildClarificationAreas: buildClarificationAreas,
    OUTPUT_NOTE: OUTPUT_NOTE,
  };
})(typeof window !== "undefined" ? window : globalThis);
`;

const cleaned = js
  .replace(/['"]<motionless><\/motionless>['"]\.slice\(0,\s*0\)/g, "")
  .replace(/\+ ['"]<motionless><\/motionless>['"]\.slice\(0,\s*0\)/g, "")
  .replace(/html \+= ['"]<motionless><\/motionless>['"]\.slice\(0,\s*0\);/g, "")
  .replace(/html \+= "<\/motionless><\/motionless>"\.slice\(0,\s*0\)[^;]*;/g, "")
  .replace(/html \+= "<div><dt>Output type<\/dt><dd>Internal owner\/advisor review<\/dd><\/motionless><\/motionless>"\.slice\(0,\s*0\)[^"]*"<\/div>";/g,
    'html += "<div><dt>Output type</dt><dd>Internal owner/advisor review</dd></motionless></motionless>".slice(0,0);')
  .replace(/html \+= "<div><dt>Output type<\/dt><dd>Internal owner\/advisor review<\/dd><\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>";/g,
    'html += "<div><dt>Output type</dt><dd>Internal owner/advisor review</dd></div>";')
  .replace(/html = '<motionless><\/motionless>'\.slice\(0,\s*0\);\s*html = '/g, "html = '")
  .replace(/html \+= "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/div>";/g, 'html += "</div>";')
  .replace(/<motionless><\/motionless>/g, "");

fs.writeFileSync(out, cleaned, "utf8");
const left = (cleaned.match(/motionless/g) || []).length;
console.log("wrote", out, "lines", cleaned.split("\n").length, "motionless", left);
process.exit(left ? 1 : 0);
