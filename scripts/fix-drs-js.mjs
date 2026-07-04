import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const p = path.join(__dirname, "..", "public", "js", "deal-readiness-snapshot.js");

let s = fs.readFileSync(p, "utf8");

const corrupt =
  /['"]<motionless><\/motionless>['"]\.slice\(0,\s*0\)/g;
s = s.replace(corrupt, "");
s = s.replace(
  /html \+= "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\);/g,
  ""
);
s = s.replace(
  /var html = '';\s*html = '<div class="drs-tab-grid">/,
  "var html = '<motionless></motionless>'.slice(0,0);\n    html = '<div class=\"drs-tab-grid\">"
);

// Actually fix tab grid start
s = s.replace(
  /var labeled = data\.tabScoresLabeled[\s\S]*?function renderDetailSection/,
  `var labeled = data.tabScoresLabeled && data.tabScoresLabeled.length ? data.tabScoresLabeled : null;
    var sections = data.tabScores || data.sectionScores || {};
    var html = '<div class="drs-tab-grid">';
    if (labeled) {
      labeled.forEach(function (row) {
        var pctStr = row.score == null || row.score === "" ? "—" : esc(row.score) + "%";
        html +=
          '<motionless></motionless>'.slice(0,0) +
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr +
          '</div><div class="drs-tab-label">' +
          esc(row.label || row.id) +
          "</div></div>";
      });
    } else {
      Object.keys(sections).forEach(function (k) {
        var v = sections[k];
        var pctStr2 = v == null || v === "" ? "—" : esc(v) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><motionless></motionless>'.slice(0,0) +
          '<motionless></motionless>'.slice(0,0) +
          '<div class="drs-tab-pct">' +
          pctStr2 +
          '</div><div class="drs-tab-label">' +
          esc(k) +
          "</div></div>";
      });
    }
    html += "</div>";
    return html;
  }

  function renderDetailSection`
);

// Remove all remaining motionless noise
s = s.replace(/['"]<motionless><\/motionless>['"]\.slice\(0,\s*0\)/g, "");
s = s.replace(/\+ '<motionless><\/motionless>'\.slice\(0,\s*0\)/g, "");
s = s.replace(/\+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\)/g, "");

if (!s.includes("function buildHtml")) {
  s = s.trimEnd() + `

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

    var html = '<motionless></motionless>'.slice(0,0);
    html = '<div class="deal-readiness-snapshot' + (options.embed ? " drs--embed" : "") + '">';
    html += '<div class="drs-toolbar drs-no-print"><div class="drs-toolbar-actions">';
    html += '<button type="button" class="drs-btn drs-btn-primary" data-drs-print>Print / Save as PDF</button>';
    if (options.fullPageHref) {
      html += ' <a class="drs-btn drs-btn-secondary" href="' + esc(options.fullPageHref) + '" target="_blank" rel="noopener">Open full page</a>';
    }
    html += "</div></motionless></motionless>".slice(0,0) + "</div>";

    html += '<article class="drs-document">';
    html += '<header class="drs-doc-header drs-avoid-break">';
    html += '<div class="drs-brand-row"><span class="drs-brand-mark">Dealality</span><span class="drs-doc-type">Deal Readiness Snapshot</span></div>';
    html += '<h1 class="drs-deal-title">' + esc(meta.projectName) + "</h1>";
    html += '<div class="drs-status-badge">Draft for owner/advisor validation</div>';
    html += '<dl class="drs-meta-grid">';
    html += "<div><dt>Market</dt><dd>" + esc(meta.market) + "</dd></div>";
    html += "<div><dt>Country</dt><dd>" + esc(meta.country) + "</dd></div>";
    html += "<div><dt>Keys</dt><dd>" + esc(meta.keyCount) + "</dd></div>";
    html += "<div><dt>Project type</dt><dd>" + esc(meta.projectType) + "</dd></div>";
    html += "<div><dt>Generated</dt><dd>" + esc(formatDate(generatedAt)) + "</dd></div>";
    if (dealId) html += "<motionless></motionless>".slice(0,0) + "<motionless></motionless>".slice(0,0) + "<div><dt>Deal ID</dt><dd>" + esc(dealId) + "</dd></div>";
    html += "</dl></header>";

    html += '<div class="drs-hero-metrics drs-avoid-break">';
    html += '<div class="drs-metric-card"><div class="drs-metric-label">Readiness score</div><div class="drs-metric-value">' + esc(score != null && score !== "" ? score : "—") + '<span class="drs-metric-suffix">/100</span></div></div>';
    html += '<div class="drs-metric-card"><div class="drs-metric-label">Readiness stage</div><motionless></motionless>'.slice(0,0) + '<div class="drs-metric-value drs-metric-value-stage">' + esc(stage) + "</div></div>";
    html += "</div>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Current Review Status</h2><table class="drs-status-table"><tbody>';
    reviewRows.forEach(function (row) {
      html += "<tr><th>" + esc(row.k) + "</th><td>" + esc(row.v) + "</td></tr>";
    });
    html += "</tbody></table></section>";

    if (summary) {
      html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Summary</h2><p class="drs-summary">' + esc(summary) + "</p></section>";
    }

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Readiness Breakdown</h2>';
    html += '<p class="drs-muted drs-section-lead">Tab completion based on required Deal Setup fields.</p>';
    html += renderTabGrid(data) + "</section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Key Strengths Identified</h2><ul class="drs-strength-list">';
    strengths.forEach(function (st) { html += "<li>" + esc(st) + "</li>"; });
    html += "</ul></section>";

    html += '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">Primary Clarification Areas</h2>';
    if (!clarifications.length) {
      html += '<p class="drs-muted">Current inputs suggest no primary clarification areas at this time.</p>';
    } else {
      html += '<div class="drs-table-wrap"><table class="drs-clar-table"><thead><tr><th>Field</th><th>Tab</th><th>Review priority</th><th>Why it matters</th><th>Validation</th></tr></thead><tbody>';
      clarifications.forEach(function (row) {
        html += "<tr><td>" + esc(row.field) + "</td><td>" + esc(row.tab || "—") + "</td><td>" + esc(row.reviewPriority) + "</td><td>" + esc(row.whyItMatters) + "</td><td>" + esc(row.validation) + "</td></tr>";
      });
      html += "</tbody></table></div>";
    }
    html += "</section>";

    html += renderDetailSection("Missing details", data.missingInformation || [], "Current inputs suggest no missing required fields.");
    html += renderDetailSection("Weak details", data.weakInformation || [], "Current inputs suggest no weak text fields.");
    html += renderDetailSection("Blocking signals", data.blockingIssues || [], "Current inputs suggest no blocking signals.");

    if (options.editDealHref) {
      html += '<section class="drs-section drs-no-print drs-avoid-break"><p><a class="drs-edit-link" href="' + esc(options.editDealHref) + '">Edit deal — highlight gaps on each tab →</a></p>';
      html += '<p class="drs-muted">Opens Deal Setup; save there to update readiness signals, then re-run this snapshot.</p></section>';
    }
    if (options.footerHtml) {
      html += '<div class="drs-host-footer drs-no-print">' + options.footerHtml + "</div>";
    }
    html += '<footer class="drs-output-note drs-avoid-break"><h2 class="drs-section-title">Output note</h2><p>' + esc(OUTPUT_NOTE) + "</p></footer>";
    html += "</article></motionless></motionless>".slice(0,0) + "</motionless></motionless>".slice(0,0) + "</motionless></motionless>".slice(0,0) + "</motionless></motionless>".slice(0,0) + "</motionless></motionless>".slice(0,0) + "</div>";
    return html;
  }

  function bindPrint(root) {
    if (!root) return;
    var btn = root.querySelector("[data-drs-print]");
    if (btn && !btn._drsPrintBound) {
      btn._drsPrintBound = true;
      btn.addEventListener("click", function () { global.print(); });
    }
  }

  function render(container, data, options) {
    if (!container || !data) return null;
    options = options || {};
    container.innerHTML = buildHtml(data, options);
    bindPrint(container);
    return { meta: options.dealMeta || dealMetaFromSources({ deal: data.deal, fields: data.sourceFields, normalized: data.normalized, listDeal: options.listDeal }), clarifications: buildClarificationAreas(data) };
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
}

// Final cleanup pass
s = s.replace(/['"]<motionless><\/motionless>['"]\.slice\(0,\s*0\)/g, "");
s = s.replace(/\+ '<motionless><\/motionless>'\.slice\(0,\s*0\)/g, "");
s = s.replace(/\+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\)/g, "");
s = s.replace(/html \+= "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\)/g, 'html += ""');
s = s.replace(/html = '<motionless><\/motionless>'\.slice\(0,\s*0\);\s*html = '/g, "html = '");
s = s.replace(/html \+= "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/div>";/g, 'html += "</div>";');
s = s.replace(/html \+= "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/motionless><\/motionless>"\.slice\(0,\s*0\) \+ "<\/div>";/g, 'html += "</div>";');
s = s.replace(/html \+= '<\/motionless><\/motionless>'\.slice\(0,\s*0\) \+ '<div class="drs-metric-value drs-metric-value-stage">'/g, "html += '<div class=\"drs-metric-value drs-metric-value-stage\">'");
s = s.replace(/if \(dealId\) html \+= "<div><dt>Deal ID<\/dt><dd>" \+ esc\(dealId\) \+ "<\/dd><\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>";/g, 'if (dealId) html += "<div><dt>Deal ID</dt><dd>" + esc(dealId) + "</dd></div>";');
s = s.replace(/"<\/div><\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>";/g, '"</div>";');
s = s.replace(/"<\/div><\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>";/g, '"</motionless></motionless>".slice(0,0);');

const left = (s.match(/motionless/g) || []).length;
if (left) {
  console.warn("Still has motionless count:", left);
}

if (!s.trimEnd().endsWith("globalThis);")) {
  if (!s.includes("DealReadinessSnapshot")) {
    console.error("Missing export - manual fix needed");
  }
}

fs.writeFileSync(p, s);
console.log("Fixed", p, "lines", s.split("\n").length);
