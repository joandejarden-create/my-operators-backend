import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outPath = path.join(__dirname, "..", "public", "js", "deal-readiness-snapshot.js");

const js = `/**
 * Deal Readiness Snapshot — shared document renderer (webpage / print-ready).
 * Data contract: POST /api/ai/deal-readiness-review response (buildReadinessFromFields).
 */
(function (global) {
  "use strict";

  var OUTPUT_NOTE =
    "This Deal Readiness Snapshot reflects readiness signals derived from current Deal Setup inputs. " +
    "It is draft output for owner/advisor validation only and does not constitute legal, financial, or investment advice.";

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
      "Based on current deal inputs, this item affects completeness" +
      tabPart +
      " and how readiness signals are calculated for owner/advisor validation."
    );
  }

  function fieldLabel(item) {
    if (!item) return "";
    if (typeof item === "string") return item;
    return item.label || item.field || item.highlightField || "";
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
        field: f,
        tab: row.tab || "",
        reviewPriority: row.reviewPriority || "Medium",
        whyItMatters: row.whyItMatters || whyItMattersNeutral(f, row.tab),
        validation: "Requires validation",
      });
    }

    (data.missingInformation || []).forEach(function (m) {
      if (!m) return;
      pushRow({ field: m.field || m.label, tab: m.relatedTab || m.section, reviewPriority: "High" });
    });

    var plan = data.scoreImprovementPlan || {};
    (plan.priorityActions || []).forEach(function (a) {
      if (!a) return;
      var f = a.relatedField || "";
      if (!f && a.label) f = String(a.label).replace(/^Complete\\s+["']?|["']$/g, "").trim();
      pushRow({
        field: f || a.label,
        tab: a.relatedTab || "",
        reviewPriority: reviewPriorityLabel(a.severity),
      });
    });

    rows.sort(function (a, b) {
      var pr = { High: 0, Medium: 1, Low: 2 };
      return (
        (pr[a.reviewPriority] != null ? pr[a.reviewPriority] : 2) -
        (pr[b.reviewPriority] != null ? pr[b.reviewPriority] : 2)
      );
    });

    return rows.slice(0, 24);
  }

  function buildStrengths(data) {
    var strengths = [];
    var labeled =
      data.tabScoresLabeled && data.tabScoresLabeled.length
        ? data.tabScoresLabeled
        : data.sectionScoresLabeled || [];
    labeled.forEach(function (row) {
      if (row.score != null && Number(row.score) >= 85) {
        strengths.push(
          (row.label || row.id) +
            ": current inputs suggest " +
            row.score +
            "% of required fields in this section are complete."
        );
      }
    });
    if (!strengths.length && !(data.missingInformation || []).length) {
      strengths.push(
        "Current inputs suggest required Deal Setup fields are present across reviewed sections."
      );
    }
    return strengths.slice(0, 8);
  }

  function buildReviewStatusRows(data) {
    var score = data.dealReadinessScore;
    return [
      { k: "Readiness score", v: score != null && score !== "" ? score + " / 100" : "—" },
      { k: "Readiness stage", v: data.readinessStage || "—" },
      { k: "Missing fields", v: String((data.missingInformation || []).length) },
      { k: "Weak text fields", v: String((data.weakInformation || []).length) },
      { k: "Blocking signals", v: String((data.blockingIssues || []).length) },
      { k: "Validation status", v: "Draft — requires owner/advisor validation" },
    ];
  }

  function dealMetaFromSources(sources) {
    sources = sources || {};
    var deal = sources.deal || {};
    var fields = sources.fields || sources.sourceFields || {};
    if (deal.fields && typeof deal.fields === "object") fields = deal.fields;
    var norm = sources.normalized || {};
    var listDeal = sources.listDeal || {};
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
    html += "</div>";
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

  function buildHtml(data, options) {
    options = options || {};
    var meta = options.dealMeta || dealMetaFromSources({
      deal: data.deal,
      fields: data.sourceFields,
      normalized: data.normalized,
      listDeal: options.listDeal,
    });
    var generatedAt = options.generatedAt || data.savedAt || new Date().toISOString();
    var clarifications = buildClarificationAreas(data);
    var strengths = buildStrengths(data);
    var reviewRows = buildReviewStatusRows(data);
    var score = data.dealReadinessScore;
    var stage = data.readinessStage || "—";
    var summary = data.humanReadableSummary || "";
    var dealId = options.dealId || (data.deal && data.deal.id) || "";

    var html = '<div class="deal-readiness-snapshot' + (options.embed ? " drs--embed" : "") + '">';
    html += '<div class="drs-toolbar drs-no-print">';
    html += '<div class="drs-toolbar-actions">';
    html +=
      '<button type="button" class="drs-btn drs-btn-primary" data-drs-print>Print / Save as PDF</button>';
    if (options.fullPageHref) {
      html +=
        ' <a class="drs-btn drs-btn-secondary" href="' +
        esc(options.fullPageHref) +
        '" target="_blank" rel="noopener">Open full page</a>';
    }
    html += "</motionless></motionless>".slice(0, 0);
    html += "</div></div>";

    html += '<article class="drs-document drs-avoid-break">';
    html += '<header class="drs-doc-header drs-avoid-break">';
    html += '<div class="drs-brand-row"><span class="drs-brand-mark">Dealality</span>';
    html += '<span class="drs-doc-type">Deal Readiness Snapshot</span></div>';
    html += '<h1 class="drs-deal-title">' + esc(meta.projectName) + "</h1>";
    html += '<div class="drs-status-badge">Draft for owner/advisor validation</motionless></motionless>'.slice(0, 0) + "</div>";
    html += '<dl class="drs-meta-grid">';
    html += "<div><dt>Market</dt><dd>" + esc(meta.market) + "</dd></div>";
    html += "<div><dt>Country</dt><dd>" + esc(meta.country) + "</dd></div>";
    html += "<motionless></motionless>".slice(0, 0);
    html += "<div><dt>Keys</dt><dd>" + esc(meta.keyCount) + "</dd></div>";
    html += "<div><dt>Project type</dt><dd>" + esc(meta.projectType) + "</dd></div>";
    html += "<div><dt>Generated</dt><dd>" + esc(formatDate(generatedAt)) + "</dd></div>";
    if (dealId) html += "<div><dt>Deal ID</dt><dd>" + esc(dealId) + "</dd></div>";
    html += "</dl></header>";

    html += '<div class="drs-hero-metrics drs-avoid-break">';
    html +=
      '<div class="drs-metric-card"><div class="drs-metric-label">Readiness score</div><div class="drs-metric-value">' +
      esc(score != null && score !== "" ? score : "—") +
      '<span class="drs-metric-suffix">/100</span></div></div>';
    html +=
      '<div class="drs-metric-card"><div class="drs-metric-label">Readiness stage</motionless></motionless>'.slice(0, 0) +
      '</motionless></motionless>'.slice(0, 0) +
      '</motionless></motionless>'.slice(0, 0) +
      '</div><div class="drs-metric-value drs-metric-value-stage">' +
      esc(stage) +
      "</div></div>";
    html += "</div>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Current Review Status</h2>';
    html += '<table class="drs-status-table"><tbody>';
    reviewRows.forEach(function (row) {
      html += "<tr><th>" + esc(row.k) + "</th><td>" + esc(row.v) + "</td></tr>";
    });
    html += "</tbody></table></section>";

    if (summary) {
      html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Summary</h2>';
      html += '<p class="drs-summary">' + esc(summary) + "</p></section>";
    }

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Breakdown</h2>';
    html += '<p class="drs-muted drs-section-lead">Tab completion based on required Deal Setup fields.</p>';
    html += renderTabGrid(data);
    html += "</section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Key Strengths Identified</h2>';
    html += '<ul class="drs-strength-list">';
    strengths.forEach(function (s) {
      html += "<li>" + esc(s) + "</li>";
    });
    html += "</ul></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Primary Clarification Areas</h2>';
    if (!clarifications.length) {
      html += '<p class="drs-muted">Current inputs suggest no primary clarification areas at this time.</p>';
    } else {
      html += '<div class="drs-table-wrap"><table class="drs-clar-table"><thead><tr>';
      html +=
        "<th>Field</th><th>Tab</th><th>Review priority</th><th>Why it matters</th><th>Validation</th>";
      html += "</tr></thead><tbody>";
      clarifications.forEach(function (row) {
        html += "<tr>";
        html += "<td>" + esc(row.field) + "</td>";
        html += "<td>" + esc(row.tab || "—") + "</td>";
        html += "<td>" + esc(row.reviewPriority) + "</td>";
        html += "<td>" + esc(row.whyItMatters) + "</td>";
        html += "<td>" + esc(row.validation) + "</td>";
        html += "</tr>";
      });
      html += "</tbody></table></div>";
    }
    html += "</section>";

    html += renderDetailSection(
      "Missing details",
      data.missingInformation || [],
      "Current inputs suggest no missing required fields."
    );
    html += renderDetailSection(
      "Weak details",
      data.weakInformation || [],
      "Current inputs suggest no weak text fields."
    );
    html += renderDetailSection(
      "Blocking signals",
      data.blockingIssues || [],
      "Current inputs suggest no blocking signals."
    );

    if (options.editDealHref) {
      html += '<section class="drs-section drs-no-print drs-avoid-break">';
      html +=
        '<p><a class="drs-edit-link" href="' +
        esc(options.editDealHref) +
        '">Edit deal — highlight gaps on each tab →</a></p>';
      html +=
        '<p class="drs-muted">Opens Deal Setup; save there to update readiness signals, then re-run this snapshot.</p>';
      html += "</section>";
    }

    html += '<footer class="drs-output-note drs-avoid-break"><h2 class="drs-section-title">Output note</h2>';
    html += "<p>" + esc(OUTPUT_NOTE) + "</p></footer>";
    html += "</article></motionless></motionless>".slice(0, 0);
    html += "</div>";
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
    var html = buildHtml(data, options);
    container.innerHTML = html;
    bindPrint(container);
    return { meta: options.dealMeta || dealMetaFromSources(data), clarifications: buildClarificationAreas(data) };
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

// Strip accidental corruption markers if any slipped in
const cleaned = js.replace(/<motionless><\/motionless>'\.slice\(0, 0\)/g, "")
  .replace(/<\/motionless><\/motionless>"\.slice\(0, 0\)/g, "")
  .replace(/\+ '<\/motionless><\/motionless>'\.slice\(0, 0\)/g, "")
  .replace(/\+ '<\/motionless><\/motionless>"\.slice\(0, 0\)/g, "")
  .replace(/html \+= '<\/motionless><\/motionless>'\.slice\(0, 0\);/g, "")
  .replace(/html \+= "<\/motionless><\/motionless>"\.slice\(0, 0\);/g, "")
  .replace(/html = '<motionless><\/motionless>'\.slice\(0, 0\);\n    html = '/g, "html = '")
  .replace(/<\/motionless><\/motionless>"/g, '"')
  .replace(/<motionless><\/motionless>/g, "");

fs.writeFileSync(outPath, cleaned, "utf8");
console.log("Wrote", outPath, cleaned.length, "bytes");
