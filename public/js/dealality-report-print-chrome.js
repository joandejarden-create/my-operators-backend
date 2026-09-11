/**
 * Shared Dealality report print chrome — Brand Alignment / Deal Brief family.
 * Source of truth: public/js/brand-alignment-snapshot.js cover + public/deal-summary.html
 *
 * Packet 2.6B-R7: Playwright footerTemplate is the SINGLE print-footer authority
 * for dossier PDF export. DOM running footer is for window.print() only and must
 * not be present in Playwright PDF HTML (causes top/bottom chrome duplication).
 */
(function (global) {
  "use strict";

  var DEALALITY_LOGO_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

  /** Deal Brief brochure confidential line (canonical). */
  var CONFIDENTIAL_LINE = "Confidential · For recipient only";

  /**
   * Body running-footer short title pattern from Deal Brief:
   * "Deal Brief · Dealality" → "{reportShortTitle} · Dealality"
   * Kept for screen / window.print only — not used in Playwright PDF footer (R7).
   */
  function reportFooterLabel(reportShortTitle) {
    var t = String(reportShortTitle || "Dealality Report").trim();
    return t + " · Dealality";
  }

  function copyrightYear(d) {
    try {
      return String((d || new Date()).getFullYear());
    } catch (_) {
      return "2026";
    }
  }

  /**
   * R7 institutional PDF footer legal (left side).
   * No report title — keeps footer single-line and unclipped.
   */
  function pdfFooterLegal(opts) {
    opts = opts || {};
    return (
      CONFIDENTIAL_LINE +
      " · © " +
      copyrightYear(opts.date) +
      " Dealality"
    );
  }

  /**
   * Legacy cluster (confidential + report label). Retained for non-PDF callers.
   */
  function legalCluster(reportShortTitle, opts) {
    opts = opts || {};
    return (
      CONFIDENTIAL_LINE +
      " · " +
      reportFooterLabel(reportShortTitle) +
      (opts.includeYear === false ? "" : " · " + copyrightYear(opts.date))
    );
  }

  /**
   * Fixed DOM footer for window.print() only.
   * Must NOT be injected into Playwright PDF export HTML (dual-footer bug R6).
   */
  function buildRunningFooterHtml(reportShortTitle, opts) {
    opts = opts || {};
    var legal = pdfFooterLegal(opts);
    return (
      '<footer class="dealality-report-running-footer bas-print-running-footer" data-dealality-report-footer="1" aria-hidden="true">' +
      '<span class="dealality-report-running-footer__legal">' +
      escapeHtml(legal) +
      "</span>" +
      '<span class="dealality-report-running-footer__mark">Page numbers via PDF export</span>' +
      "</footer>"
    );
  }

  /**
   * Playwright page.pdf footerTemplate — SINGLE PDF footer authority (R7).
   * Styles MUST be inline (template CSS is isolated from page CSS).
   * Chromium tokens: pageNumber, totalPages.
   */
  function playwrightFooterTemplate(_reportShortTitle, opts) {
    opts = opts || {};
    var legal = String(pdfFooterLegal(opts))
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
    return (
      '<div style="width:100%;box-sizing:border-box;padding:0 12mm;' +
      "font-size:7.5pt;line-height:1.2;color:#4a5568;" +
      "font-family:Inter,'Segoe UI',system-ui,sans-serif;" +
      'display:flex;justify-content:space-between;align-items:center;">' +
      '<span style="max-width:70%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' +
      legal +
      "</span>" +
      '<span style="flex:0 0 auto;font-variant-numeric:tabular-nums;white-space:nowrap;">' +
      'Page <span class="pageNumber"></span> of <span class="totalPages"></span>' +
      "</span>" +
      "</div>"
    );
  }

  /** Empty header — no top print chrome on any page. */
  function playwrightHeaderTemplate() {
    return "<div style=\"font-size:0;height:0;margin:0;padding:0;\"></div>";
  }

  /**
   * Margins for Playwright PDF.
   * Bottom ~18mm reserves footer safe band; body must stay above it.
   * Top margin is content-only (empty headerTemplate) — no top chrome.
   */
  function pdfMargins() {
    return { top: "14mm", right: "12mm", bottom: "20mm", left: "12mm" };
  }

  function escapeHtml(t) {
    return String(t == null ? "" : t)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  var api = {
    DEALALITY_LOGO_URL: DEALALITY_LOGO_URL,
    CONFIDENTIAL_LINE: CONFIDENTIAL_LINE,
    reportFooterLabel: reportFooterLabel,
    copyrightYear: copyrightYear,
    pdfFooterLegal: pdfFooterLegal,
    legalCluster: legalCluster,
    buildRunningFooterHtml: buildRunningFooterHtml,
    playwrightFooterTemplate: playwrightFooterTemplate,
    playwrightHeaderTemplate: playwrightHeaderTemplate,
    pdfMargins: pdfMargins,
  };

  global.DealalityReportPrintChrome = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
