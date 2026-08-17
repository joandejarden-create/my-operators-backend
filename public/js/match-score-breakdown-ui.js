/**
 * Shared Match Score breakdown rendering (My Deals + Brand Development Dashboard).
 * Flat factor list with graduated soft factors + gates/diligence.
 */
(function (global) {
  "use strict";

  var FACTOR_ORDER = [
    "geographyPriority",
    "sameBrandMarketDensity",
    "chainScaleProximity",
    "serviceModelAlignment",
    "brandStandardsCompatibility",
    "feesToleranceCompatibility",
    "roomRangeFitCompatibility",
    "keyMoneyWillingnessCompatibility",
    "softHardPreference",
    "incentivesMatchCompatibility",
    "agreementsTypeCompatibility",
    "buildingTypeCompatibility",
    "preferredBrandBonus",
    "projectTypeCompatibility",
    "projectStageCompatibility",
  ];

  /** Dual-signal / Opportunity-era keys + labels — ignore if still present in Deal Brand Cache. */
  var OBSOLETE_FACTOR_KEYS = {
    localBrandPower: true,
    marketPriorityGrowth: true,
    sameBrandTerritoryHeadroom: true,
    conversionReflagReadiness: true,
    fitScore: true,
    opportunityScore: true,
    guidance: true,
  };

  var OBSOLETE_LABEL_RE =
    /local brand power|market priority\s*\/\s*growth|same-brand (market )?headroom|conversion\s*\/\s*reflag readiness/i;

  function isObsoleteBreakdownFactor(factorKey, d) {
    if (OBSOLETE_FACTOR_KEYS[factorKey]) return true;
    if (d && OBSOLETE_LABEL_RE.test(String(d.label || ""))) return true;
    if (d && d.note && /^Opportunity:/i.test(String(d.note))) return true;
    return false;
  }

  function escapeHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function renderFactorRowHtml(row, esc, getScoreClass) {
    var d = row.d;
    var label = d.label ? d.label : row.key;
    var weight = d.weight != null ? d.weight : 0;
    var hasScore = d.score != null && d.score !== "—";
    var sc = hasScore ? getScoreClass(Number(d.score)) : "low";
    var scorePct = hasScore ? Math.min(100, Math.max(0, Number(d.score))) : 0;
    var excludedNote =
      weight > 0 && !hasScore
        ? '<div class="score-factor-weight" style="color: var(--neutral--400);">' +
          (d.dealValue && /undetermined|not yet determined|not specified/i.test(String(d.dealValue))
            ? "(Not in average — owner fee expectations undetermined)"
            : "(Not in average — missing data)") +
          "</div>"
        : weight
          ? '<div class="score-factor-weight">(Weight: ' + weight + "%)</div>"
          : "";
    var html =
      '<div class="score-category"><div class="score-category-label">' +
      '<div class="score-factor-heading">' +
      esc(label) +
      "</div>" +
      excludedNote +
      "</div>" +
      '<div class="score-category-value"><div class="score-bar"><div class="score-bar-fill ' +
      sc +
      '" style="width: ' +
      scorePct +
      '%"></div></div>' +
      '<span class="score-number">' +
      (hasScore ? d.score : "—") +
      "</span></div>";
    if (d.brandValue || d.dealValue || d.note) {
      html +=
        '<div class="score-factor-details">' +
        '<div><strong style="color: var(--neutral--300);">Brand setup:</strong> ' +
        esc(d.brandValue || "—") +
        "</div>" +
        '<div style="margin-top: 4px;"><strong style="color: var(--neutral--300);">Deal setup:</strong> ' +
        esc(d.dealValue || "—") +
        "</div>" +
        (d.note
          ? '<div style="margin-top: 4px;"><strong style="color: var(--neutral--300);">How match works:</strong> ' +
            esc(d.note) +
            "</div>"
          : "") +
        "</div>";
    }
    html += "</div>";
    return html;
  }

  /**
   * @param {object} details
   * @param {{ getScoreClass?: function, escapeHtml?: function, meta?: object }} opts
   * @returns {string}
   */
  function renderFlatBreakdownHtml(details, opts) {
    opts = opts || {};
    var esc = typeof opts.escapeHtml === "function" ? opts.escapeHtml : escapeHtml;
    var getScoreClass =
      typeof opts.getScoreClass === "function"
        ? opts.getScoreClass
        : function () {
            return "low";
          };

    var html = "";
    var seen = {};

    FACTOR_ORDER.forEach(function (factorKey) {
      seen[factorKey] = true;
      var d = details && details[factorKey];
      if (!d || typeof d !== "object" || (!d.label && d.score == null && !d.brandValue)) return;
      if (isObsoleteBreakdownFactor(factorKey, d)) return;
      html += renderFactorRowHtml({ key: factorKey, d: d }, esc, getScoreClass);
    });

    Object.keys(details || {}).forEach(function (factorKey) {
      if (factorKey === "_meta" || seen[factorKey]) return;
      var d = details[factorKey];
      if (!d || typeof d !== "object" || (!d.label && d.score == null && !d.brandValue)) return;
      if (isObsoleteBreakdownFactor(factorKey, d)) return;
      html += renderFactorRowHtml({ key: factorKey, d: d }, esc, getScoreClass);
    });

    return html;
  }

  global.DealalityMatchScoreBreakdownUi = {
    FACTOR_ORDER: FACTOR_ORDER,
    renderFlatBreakdownHtml: renderFlatBreakdownHtml,
    renderGroupedBreakdownHtml: renderFlatBreakdownHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
