/**
 * Minimal Explorer trust chip from API governance object (Phase 3).
 * Renders only when governance.displayLabel is set — never internalWarnings.
 */
(function (global) {
  "use strict";

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /**
   * @param {object|null|undefined} governance
   * @param {{ badgeClass?: string, subtitleClass?: string }} [options]
   * @returns {string} HTML or empty string
   */
  function governanceTrustChipHtml(governance, options) {
    options = options || {};
    if (!governance || typeof governance !== "object") return "";
    var label =
      governance.displayLabel != null ? String(governance.displayLabel).trim() : "";
    if (!label) return "";

    var badgeClass = options.badgeClass || "dc-governance-trust-chip";
    var subtitleClass = options.subtitleClass || "dc-governance-trust-subtitle meta-muted";
    var svg =
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">' +
      '<path d="M9 12l2 2 4-4"/><circle cx="12" cy="12" r="9"/>' +
      "</svg>";

    var html =
      '<span class="' +
      escapeHtml(badgeClass) +
      '" role="status">' +
      svg +
      escapeHtml(label) +
      "</span>";

    var subtitle =
      governance.displaySubtitle != null ? String(governance.displaySubtitle).trim() : "";
    if (subtitle) {
      html +=
        '<span class="' +
        escapeHtml(subtitleClass) +
        '">' +
        escapeHtml(subtitle) +
        "</span>";
    }
    return html;
  }

  global.ProfileGovernanceTrustChip = {
    governanceTrustChipHtml: governanceTrustChipHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
