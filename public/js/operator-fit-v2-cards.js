/**
 * Operator Fit Engine v2 — Top-5 Operator Alignment cards UI.
 * Reuses BAS/OAS visual language; does not invent a new design system.
 */
(function (global) {
  "use strict";

  function esc(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function stateClass(state) {
    if (state === "unknown") return "of2-state-unknown";
    if (state === "not_applicable") return "of2-state-na";
    if (state === "known") return "of2-state-known";
    return "of2-state-unknown";
  }

  function scoreBar(label, score, weight, state) {
    var pct = state === "unknown" ? 0 : Math.max(0, Math.min(100, Number(score) || 0));
    var unknownNote =
      state === "unknown"
        ? '<span class="of2-unknown-chip">Information unavailable</span>'
        : state === "not_applicable"
          ? '<span class="of2-na-chip">Not applicable</span>'
          : "";
    return (
      '<div class="of2-factor-row ' +
      stateClass(state) +
      '">' +
      '<div class="of2-factor-meta"><span class="of2-factor-label">' +
      esc(label) +
      "</span>" +
      (weight != null ? '<span class="of2-factor-weight">w' + esc(weight) + "</span>" : "") +
      "</div>" +
      '<div class="of2-bar-track"><div class="of2-bar-fill" style="width:' +
      (state === "unknown" ? "0" : pct) +
      '%"></div></div>' +
      '<div class="of2-factor-score">' +
      (state === "unknown" ? "—" : esc(pct)) +
      " " +
      unknownNote +
      "</div></div>"
    );
  }

  function renderCard(c, opts) {
    opts = opts || {};
    var typeBadge =
      c.candidateType === "Brand Managed"
        ? '<span class="of2-badge of2-badge-brand">Brand Managed</span>'
        : '<span class="of2-badge">Third-Party Operator</span>';
    var conf = esc(c.evidenceConfidence || "Limited");
    var elig = esc(c.eligibilityStatus || "");
    var why = (c.whyItMatches || [])
      .slice(0, 3)
      .map(function (x) {
        return "<li>" + esc(x) + "</li>";
      })
      .join("");
    var concerns = (c.potentialConcerns || [])
      .slice(0, 2)
      .map(function (x) {
        return "<li>" + esc(x) + "</li>";
      })
      .join("");
    var unknowns = (c.unknowns || [])
      .slice(0, 5)
      .map(function (x) {
        return "<li>" + esc(x) + "</li>";
      })
      .join("");
    var validation = (c.validationQuestions || [])
      .slice(0, 5)
      .map(function (x) {
        return "<li>" + esc(x) + "</li>";
      })
      .join("");
    var brands = (c.relevantBrands || []).map(esc).join(", ") || "—";

    var factors = (c.factorBreakdown || [])
      .map(function (f) {
        return scoreBar(f.label, f.score, f.weight, f.state);
      })
      .join("");

    var layers = c.layers || {};
    var layerRows =
      scoreBar(
        "Operator–Project Alignment",
        layers.operatorProjectAlignment && layers.operatorProjectAlignment.rawScore,
        null,
        "known"
      ) +
      scoreBar(
        "Operating Structure Alignment",
        layers.operatingStructureAlignment && layers.operatingStructureAlignment.score,
        null,
        (layers.operatingStructureAlignment && layers.operatingStructureAlignment.state) || "unknown"
      ) +
      scoreBar(
        "Brand–Operator Compatibility (composition)",
        layers.brandOperatorCompatibility && layers.brandOperatorCompatibility.numericForComposition,
        null,
        (layers.brandOperatorCompatibility && layers.brandOperatorCompatibility.state) || "unknown"
      );

    return (
      '<article class="of2-card" data-candidate-id="' +
      esc(c.candidateId) +
      '">' +
      '<header class="of2-card-header">' +
      '<div class="of2-rank">#' +
      esc(c.rank) +
      "</div>" +
      '<div class="of2-title-block"><h3 class="of2-name">' +
      esc(c.operatorName) +
      "</h3>" +
      typeBadge +
      '</div><div class="of2-score-block">' +
      '<div class="of2-score">' +
      esc(c.displayedOperatorAlignment) +
      '</div><div class="of2-score-label">Operator Alignment</div>' +
      (c.confidenceCeilingApplied != null
        ? '<div class="of2-ceiling">Ceiling ' + esc(c.confidenceCeilingApplied) + " applied</div>"
        : "") +
      "</div></header>" +
      '<div class="of2-meta-row">' +
      '<span class="of2-pill">Evidence Confidence: ' +
      conf +
      "</span>" +
      '<span class="of2-pill">Data Coverage: ' +
      esc(c.dataCoveragePct) +
      "%</span>" +
      '<span class="of2-pill">Eligibility: ' +
      elig +
      "</span>" +
      '<span class="of2-pill">Brands: ' +
      brands +
      "</span></div>" +
      '<div class="of2-cols">' +
      '<div><h4>Strongest alignment reasons</h4><ul>' +
      (why || "<li>Alignment requires validation</li>") +
      "</ul></div>" +
      "<div><h4>Potential concerns</h4><ul>" +
      (concerns || "<li>None flagged from current evidence</li>") +
      "</ul></div></div>" +
      '<div class="of2-cols">' +
      "<div><h4>Important unknowns</h4><ul class=\"of2-unknown-list\">" +
      (unknowns || "<li>None listed</li>") +
      "</ul></div>" +
      "<div><h4>Confirm during outreach</h4><ul>" +
      (validation || "<li>No open validation items</li>") +
      "</ul></div></div>" +
      '<details class="of2-breakdown"><summary>Factor and layer breakdown</summary>' +
      '<div class="of2-layer-block"><h5>Layers</h5>' +
      layerRows +
      "</div>" +
      '<div class="of2-factor-block"><h5>Operator–Project factors</h5>' +
      factors +
      "</div>" +
      '<p class="of2-footnote">Raw alignment (after risk): ' +
      esc(c.rawOperatorAlignment) +
      ". Displayed score respects evidence confidence ceiling. Unknown factors contribute 0 and remain in the denominator.</p>" +
      "</details>" +
      '<div class="of2-actions">' +
      '<button type="button" class="of2-btn of2-btn-disabled" disabled title="Target List is brand-only in Phase 1–2">Save to Target List (unavailable)</button>' +
      (opts.explorerBase
        ? '<a class="of2-btn of2-btn-secondary" href="' +
          esc(opts.explorerBase + encodeURIComponent(String(c.candidateId).replace(/^brand-managed:/, ""))) +
          '">View profile</a>'
        : "") +
      "</div></article>"
    );
  }

  function renderTop5(payload, mountEl, opts) {
    opts = opts || {};
    if (!mountEl) return;
    if (!payload || payload.success === false) {
      mountEl.innerHTML =
        '<div class="of2-empty of2-error">' +
        esc((payload && (payload.ownerMessage || payload.error)) || "Unable to load Operator Alignment.") +
        "</div>";
      return;
    }
    if (payload.ownerFacing === false && payload.shadowOnly) {
      mountEl.innerHTML =
        '<div class="of2-empty">Shadow diagnostics only — owner-facing Operator Fit v2 is off.</div>';
      return;
    }
    var cards = payload.top5 || [];
    if (!cards.length) {
      var reason =
        (payload.diagnostics && payload.diagnostics.fewerThanFiveReason) ||
        "No eligible operators for Operator Alignment with current project and operator data.";
      mountEl.innerHTML = '<div class="of2-empty">' + esc(reason) + "</div>";
      return;
    }
    var header =
      '<div class="of2-header"><h2>Operator Alignment</h2>' +
      "<p>Project-specific alignment based on available evidence — not a performance prediction or endorsement.</p>" +
      (cards.length < 5
        ? "<p class=\"of2-note\">Showing " +
          cards.length +
          " eligible candidate(s); the list is not padded to five.</p>"
        : "") +
      "</div>";
    mountEl.innerHTML =
      header +
      '<div class="of2-card-list">' +
      cards.map(function (c) {
        return renderCard(c, opts);
      }).join("") +
      "</div>" +
      '<p class="of2-disclaimer">Scores are Operator Alignment signals. Limited evidence applies a score ceiling. Table-stakes capability claims do not create positive differentiation.</p>';
  }

  function renderFlagOff(mountEl) {
    if (!mountEl) return;
    mountEl.innerHTML =
      '<div class="of2-empty">Feature unavailable because Operator Fit Engine v2 is off. Legacy Operator Alignment Snapshot remains available.</div>';
  }

  global.DcOperatorFitV2Ui = {
    renderTop5: renderTop5,
    renderFlagOff: renderFlagOff,
    renderCard: renderCard,
  };
})(typeof window !== "undefined" ? window : globalThis);
