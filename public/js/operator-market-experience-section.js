/**
 * Three-Layer Market Experience — Markets & Footprint (Explorer / DNA).
 * Dark shell: white titles, neutral cards (accent border only, no green page wrapper).
 */
(function (global) {
  "use strict";

  var SECTION_INTRO =
    "See where they manage today, where their team has credible experience, and where they want to grow—so you can assess fit beyond hotel count alone.";

  var DEFAULT_NOTES = {
    current: [
      "Active operating infrastructure",
      "Existing owner reporting rhythms",
      "Established vendor and labor playbooks",
    ],
    team: [
      "Team-level regional exposure",
      "Relevant resort and coastal operating knowledge",
      "Potential bridge into CALA growth",
    ],
    target: [
      "Growth-stage positioning",
      "Selective market entry",
      "Focused on fit, not volume",
    ],
  };

  var LAYER_META = [
    {
      key: "current",
      title: "Current Operating Markets",
      subtitle: "Where the company currently manages hotels",
      tone: "current",
    },
    {
      key: "team",
      title: "Team Experience Markets",
      subtitle: "Where leadership or team members have credible experience",
      tone: "team",
    },
    {
      key: "target",
      title: "Target Growth Markets",
      subtitle: "Where the operator wants to pursue future opportunities",
      tone: "target",
    },
  ];

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function arrayish(v) {
    if (Array.isArray(v)) return v.map(nz).filter(Boolean);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (s) {
        return nz(s);
      })
      .filter(Boolean);
  }

  function uniqueStrings(arr) {
    var seen = {};
    return (arr || []).filter(function (x) {
      var k = String(x).toLowerCase();
      if (!k || seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function parseRegionalNarrativeTeamMarkets(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var raw = pick(ex, p, "lead_narrative_regional", "");
    if (!raw) return [];
    return String(raw)
      .split(/\n+/)
      .map(function (line) {
        return nz(line);
      })
      .filter(Boolean);
  }

  function parseLeadTeamMarketChips(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var raw = pick(ex, p, "lead_team_markets_json", "");
    if (!raw) return [];
    try {
      var parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map(function (row) {
          return nz(row && (row.market || row.title));
        })
        .filter(Boolean);
    } catch (e) {
      return [];
    }
  }

  function layerNotesFor(key, chips, notes) {
    var noteList = arrayish(notes);
    if (noteList.length) return noteList;
    if (!arrayish(chips).length) return [];
    return (DEFAULT_NOTES[key] || []).slice();
  }

  function pick(ex, p, key, fallback) {
    if (ex && ex[key] != null && ex[key] !== "") return ex[key];
    if (p && p[key] != null && p[key] !== "") return p[key];
    return fallback;
  }

  /** @returns {{ current: string[], team: string[], target: string[] }} */
  function deriveMarketExperience(vm) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var me = (vm && vm.marketExperience) || {};
    return {
      current: uniqueStrings(
        []
          .concat(arrayish(me.current))
          .concat(arrayish(p.activeCountries))
          .concat(arrayish(p.activeMarkets))
          .concat(arrayish(p.regions))
          .concat(arrayish(ex.activeCountries))
          .concat(arrayish(ex.activeMarkets))
      ),
      team: (function () {
        var scalars = uniqueStrings(
          []
            .concat(arrayish(me.team))
            .concat(arrayish(p.teamExperienceMarkets))
            .concat(arrayish(ex.teamExperienceMarkets))
        );
        if (scalars.length) return scalars;
        var regional = parseRegionalNarrativeTeamMarkets(vm);
        if (regional.length) return uniqueStrings(regional);
        return uniqueStrings(parseLeadTeamMarketChips(vm));
      })(),
      target: uniqueStrings(
        []
          .concat(arrayish(me.target))
          .concat(arrayish(p.targetGrowthMarkets))
          .concat(arrayish(p.priorityMarkets))
          .concat(arrayish(p.specificMarkets))
          .concat(arrayish(ex.targetGrowthMarkets))
          .concat(arrayish(ex.priorityMarkets))
      ),
    };
  }

  function buildLayers(vm) {
    if (vm && vm.marketLayers && vm.marketLayers.length) {
      return vm.marketLayers.map(function (layer) {
        var key = layer.key || "";
        return {
          key: key,
          title: layer.title || "",
          subtitle: layer.subtitle || "",
          tone: layer.tone || key,
          chips: arrayish(layer.chips),
          notes: layerNotesFor(key, layer.chips, layer.notes),
        };
      });
    }

    var Dna = global.OperatorDnaViewModel;
    var me = deriveMarketExperience(vm);
    var notes = (vm && vm.marketLayerNotes) || {};
    if (Dna && typeof Dna.buildMarketLayers === "function") {
      return Dna.buildMarketLayers(me, notes).map(function (layer) {
        var key = layer.key || "";
        return {
          key: key,
          title: layer.title,
          subtitle: layer.subtitle,
          tone: layer.tone || key,
          chips: arrayish(layer.chips),
          notes: layerNotesFor(key, layer.chips, layer.notes),
        };
      });
    }

    var exp = deriveMarketExperience(vm);
    return LAYER_META.map(function (meta) {
      return {
        key: meta.key,
        title: meta.title,
        subtitle: meta.subtitle,
        tone: meta.tone,
        chips: exp[meta.key] || [],
        notes: layerNotesFor(meta.key, exp[meta.key], notes[meta.key]),
      };
    });
  }

  function chipsHtml(chips) {
    var list = arrayish(chips);
    if (!list.length) {
      return '<p class="oe-market-layer-card__empty">Not yet provided in Operator Setup.</p>';
    }
    return (
      '<div class="oe-market-layer-card__chips">' +
      list
        .map(function (c) {
          return '<span class="oe-lead-chip">' + escapeHtml(c) + "</span>";
        })
        .join("") +
      "</div>"
    );
  }

  function checksHtml(notes) {
    var list = arrayish(notes);
    if (!list.length) return "";
    return (
      '<ul class="oe-market-layer-card__checks">' +
      list
        .map(function (note) {
          return (
            '<li class="oe-market-layer-card__check">' +
            '<span class="oe-lead-cadence-card__icon" aria-hidden="true"></span>' +
            '<span class="oe-market-layer-card__check-label">' +
            escapeHtml(note) +
            "</span></li>"
          );
        })
        .join("") +
      "</ul>"
    );
  }

  function layerCardHtml(layer) {
    if (!layer || !nz(layer.title)) return "";
    return (
      '<article class="oe-market-layer-card oe-market-layer-card--' +
      escapeHtml(layer.tone || layer.key || "default") +
      '">' +
      '<h3 class="oe-market-layer-card__title">' +
      escapeHtml(layer.title) +
      "</h3>" +
      '<p class="oe-market-layer-card__subtitle">' +
      escapeHtml(layer.subtitle || "") +
      "</p>" +
      chipsHtml(layer.chips) +
      checksHtml(layer.notes) +
      "</article>"
    );
  }

  /**
   * @param {object} vm - gold-mock view model
   * @param {{ intro?: string }} [opts]
   */
  function buildThreeLayerSectionHtml(vm, opts) {
    opts = opts || {};
    var layers = buildLayers(vm);
    var cards = layers.map(layerCardHtml).filter(Boolean).join("");
    if (!cards) return "";

    var intro = nz(opts.intro) || SECTION_INTRO;

    return (
      '<section class="section oe-market-experience-section">' +
      '<header class="oe-market-experience-section__head">' +
      '<h2 class="section-title oe-market-experience-section__title">Three-Layer Market Experience</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">' +
      escapeHtml(intro) +
      "</p></header>" +
      '<div class="oe-market-experience-grid">' +
      cards +
      "</div></section>"
    );
  }

  global.OperatorMarketExperienceSection = {
    buildThreeLayerSectionHtml: buildThreeLayerSectionHtml,
    deriveMarketExperience: deriveMarketExperience,
    buildLayers: buildLayers,
    DEFAULT_NOTES: DEFAULT_NOTES,
  };
})(typeof globalThis !== "undefined" ? globalThis : typeof window !== "undefined" ? window : self);
