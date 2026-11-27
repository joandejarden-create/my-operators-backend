/**
 * Hotel Explorer — shared expandable "About This Analysis" footnote.
 * Customer notes only: coverage, definitions, evidence, limitations, last verified.
 */
(function (global) {
  "use strict";

  function escapeHtml(text) {
    return String(text == null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function section(title, body) {
    if (!body) return "";
    return (
      '<div class="he-about-block">' +
      '<div class="he-about-block__title">' +
      escapeHtml(title) +
      "</div>" +
      '<div class="he-about-block__body">' +
      body +
      "</div></div>"
    );
  }

  function listHtml(items) {
    var rows = (items || []).filter(Boolean);
    if (!rows.length) return "";
    return (
      "<ul class=\"he-about-list\">" +
      rows
        .map(function (item) {
          return "<li>" + escapeHtml(item) + "</li>";
        })
        .join("") +
      "</ul>"
    );
  }

  /**
   * @param {object} opts
   * @param {string} [opts.summary]
   * @param {string|string[]} [opts.coverage]
   * @param {string|string[]} [opts.definitions]
   * @param {string|string[]} [opts.evidence]
   * @param {string|string[]} [opts.limitations]
   * @param {string} [opts.lastVerified]
   * @param {boolean} [opts.open]
   */
  function renderHtml(opts) {
    opts = opts || {};
    var fmt = global.HeCustomerPresentationFormat;
    function asBody(v) {
      if (Array.isArray(v)) return listHtml(v);
      if (!v) return "";
      var text = String(v);
      if (fmt && typeof fmt.scrubInternalTerminology === "function") {
        text = fmt.scrubInternalTerminology(text);
      }
      return "<p>" + escapeHtml(text) + "</p>";
    }

    var body =
      section("Coverage", asBody(opts.coverage)) +
      section("Definitions", asBody(opts.definitions)) +
      section("Evidence", asBody(opts.evidence)) +
      section("Limitations", asBody(opts.limitations)) +
      (opts.lastVerified
        ? section("Last verified", "<p>" + escapeHtml(String(opts.lastVerified)) + "</p>")
        : "");

    if (!body.trim()) return "";

    return (
      '<details class="he-about-this-analysis" data-he-about-this-analysis="1"' +
      (opts.open ? " open" : "") +
      ">" +
      "<summary>" +
      escapeHtml(opts.summary || "About This Analysis") +
      "</summary>" +
      '<div class="he-about-this-analysis__body">' +
      body +
      "</div></details>"
    );
  }

  function defaultPropertyFactsNote(hotel) {
    hotel = hotel || {};
    return renderHtml({
      coverage: [
        "Property Facts summarize verified commercial attributes for this hotel.",
        "Blank or “Not yet verified” fields mean the value is not confirmed in the current record.",
      ],
      definitions: [
        "Guestrooms = total guest keys.",
        "Meeting Space = total meeting inventory in square feet when verified.",
        "Last Renovated = primary major renovation year when verified.",
      ],
      evidence: [
        "Values are drawn from the hotel’s verified property record and supporting brand or operator materials.",
      ],
      limitations: [
        "Secondary renovation milestones and research workflow notes are kept in Sources & Evidence, not in Property Facts.",
      ],
      lastVerified: hotel.lastVerified || hotel.last_reviewed || null,
    });
  }

  var SUBMARKET_COVERAGE_FOOTNOTE =
    "Market Coverage: Figures reflect hotels currently verified within Dealality's market intelligence database and may not represent the entire market. Room totals exclude properties without verified room counts.";

  /** Discreet methodology footnote — used below Submarket Snapshot tables. */
  function submarketCoverageFootnoteHtml() {
    return (
      '<p class="he-methodology-footnote hdp-snapshot-meta" data-he-submarket-coverage-footnote="1">' +
      escapeHtml(SUBMARKET_COVERAGE_FOOTNOTE) +
      "</p>"
    );
  }

  /**
   * @deprecated Prefer submarketCoverageFootnoteHtml for Submarket Snapshot.
   * Kept for shared About This Analysis callers that still expand coverage notes.
   */
  function defaultSubmarketNote(metrics) {
    metrics = metrics || {};
    return submarketCoverageFootnoteHtml();
  }

  function defaultContactNote() {
    return renderHtml({
      coverage: [
        "Contact Intelligence lists people and organizations relevant to this hotel.",
        "Channel values shown in the directory are verified summaries only.",
      ],
      definitions: [
        "Organization websites and switchboards are company routes, not personal inboxes.",
        "Find Contact Details lookups stay in this session and are not saved to the hotel record.",
      ],
      evidence: ["Contacts are sourced from verified hotel and organization relationship records."],
      limitations: [
        "Property-level authority may be unverified for some people — see each contact’s Details panel.",
      ],
    });
  }

  global.HeAboutThisAnalysis = {
    renderHtml: renderHtml,
    defaultPropertyFactsNote: defaultPropertyFactsNote,
    defaultSubmarketNote: defaultSubmarketNote,
    submarketCoverageFootnoteHtml: submarketCoverageFootnoteHtml,
    SUBMARKET_COVERAGE_FOOTNOTE: SUBMARKET_COVERAGE_FOOTNOTE,
    defaultContactNote: defaultContactNote,
  };
})(typeof window !== "undefined" ? window : globalThis);
