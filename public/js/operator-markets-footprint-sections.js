/**
 * Markets & Footprint — Local / Regional Expertise + Market Fit Signals (Explorer / DNA).
 * JSON from Operator Setup explorerProfileJson (mkt_* keys) with CALA-realistic defaults.
 */
(function (global) {
  "use strict";

  var FIELD = {
    regional: "mkt_regional_expertise_json",
    fitSignals: "mkt_market_fit_signals_json",
  };

  var DEFAULTS = {
    mkt_regional_expertise_json: [
      {
        title: "Local Office / Regional Presence",
        description:
          "Miami-based leadership with regional travel cadence and partner network.",
      },
      {
        title: "Language Capabilities",
        description:
          "English and Spanish-language operating support available through regional team resources.",
      },
      {
        title: "Labor Market Familiarity",
        description:
          "Experience adapting staffing models to leisure, resort, and seasonal demand patterns.",
      },
      {
        title: "Regulatory Familiarity",
        description:
          "Understands that island and CALA markets require local legal, labor, accounting, and permitting guidance.",
      },
      {
        title: "Cultural Fluency",
        description:
          "Positioned to adapt service and owner communication to local market expectations.",
      },
      {
        title: "Vendor / Partner Network",
        description:
          "Can leverage regional advisors, procurement partners, and owner relationships as market entry deepens.",
      },
    ],
    mkt_market_fit_signals_json: [
      {
        title: "Coastal Destinations",
        description: "Consistent performance in beach and waterfront markets.",
      },
      {
        title: "Urban Leisure Gateway",
        description: "Experience operating in mixed leisure/business demand markets.",
      },
      {
        title: "Island Complexity",
        description:
          "Ability to coordinate staffing, procurement, and service standards in harder-to-serve markets.",
      },
      {
        title: "Resort-Adjacent Select Service",
        description:
          "Potential fit for demand near resort corridors where all-inclusive is not the right product.",
      },
      {
        title: "Independent Resorts",
        description:
          "Experience supporting hotels that need stronger commercial discipline without losing identity.",
      },
      {
        title: "Pipeline Selectivity",
        description: "Growth approach favors quality of fit over broad market coverage.",
      },
    ],
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function pick(ex, p, key) {
    if (ex && nz(ex[key])) return ex[key];
    if (p && nz(p[key])) return p[key];
    return "";
  }

  function parseJsonArray(raw) {
    if (raw == null || raw === "") return null;
    if (Array.isArray(raw)) return raw;
    try {
      var parsed = JSON.parse(String(raw));
      return Array.isArray(parsed) ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  function sectionData(vm, fieldKey) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fromRecord = parseJsonArray(pick(ex, p, fieldKey));
    if (fromRecord && fromRecord.length) return fromRecord;
    return DEFAULTS[fieldKey] || [];
  }

  function normalizeRows(rows) {
    return (rows || [])
      .map(function (row) {
        if (!row) return null;
        if (Array.isArray(row)) {
          return { title: nz(row[0]), description: nz(row[1]) };
        }
        return {
          title: nz(row.title || row.label || row.headline),
          description: nz(row.description || row.body || row.value),
        };
      })
      .filter(function (row) {
        return row && (row.title || row.description);
      });
  }

  function regionalRows(vm) {
    var fromJson = normalizeRows(sectionData(vm, FIELD.regional));
    if (fromJson.length) return fromJson;
    if (vm && vm.regionalExpertiseRows && vm.regionalExpertiseRows.length) {
      return normalizeRows(vm.regionalExpertiseRows);
    }
    return normalizeRows(DEFAULTS[FIELD.regional]);
  }

  function fitSignalRows(vm) {
    var fromJson = normalizeRows(sectionData(vm, FIELD.fitSignals));
    if (fromJson.length) return fromJson;
    if (vm && vm.marketFitSignals && vm.marketFitSignals.length) {
      return normalizeRows(vm.marketFitSignals);
    }
    return normalizeRows(DEFAULTS[FIELD.fitSignals]);
  }

  function expertiseCard(row) {
    return (
      '<div class="card oe-lead-cadence-card oe-markets-expertise-card">' +
      '<span class="oe-lead-cadence-card__icon" aria-hidden="true"></span>' +
      "<div>" +
      "<h3>" +
      escapeHtml(row.title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description) +
      "</p></div></div>"
    );
  }

  function fitSignalCard(row) {
    return (
      '<div class="card oe-markets-fit-card">' +
      "<h3>" +
      escapeHtml(row.title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description) +
      "</p></div>"
    );
  }

  function buildRegionalExpertiseSection(vm) {
    var rows = regionalRows(vm);
    if (!rows.length) return "";
    return (
      '<section class="section oe-markets-section">' +
      '<h2 class="section-title">Local / Regional Expertise</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">What matters for execution in complex markets: local presence, language, labor, regulation, and on-the-ground partners—not just a country list.</p>' +
      '<div class="grid-2 oe-markets-expertise-grid">' +
      rows.map(expertiseCard).join("") +
      "</div></section>"
    );
  }

  function buildMarketFitSignalsSection(vm) {
    var rows = fitSignalRows(vm);
    if (!rows.length) return "";
    return (
      '<section class="section oe-markets-section">' +
      '<h2 class="section-title">Market Fit Signals</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Market types and situations where this operator has shown credible execution—not a promise for every geography.</p>' +
      '<div class="grid-3 oe-markets-fit-grid">' +
      rows.map(fitSignalCard).join("") +
      "</div></section>"
    );
  }

  function buildAllSectionsHtml(vm) {
    return buildRegionalExpertiseSection(vm) + buildMarketFitSignalsSection(vm);
  }

  global.OperatorMarketsFootprintSections = {
    buildAllSectionsHtml: buildAllSectionsHtml,
    buildRegionalExpertiseSection: buildRegionalExpertiseSection,
    buildMarketFitSignalsSection: buildMarketFitSignalsSection,
    FIELD: FIELD,
    DEFAULTS: DEFAULTS,
  };
})(typeof globalThis !== "undefined" ? globalThis : global);
