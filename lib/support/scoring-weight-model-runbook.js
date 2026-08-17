/**
 * Scoring Weight Model — admin runbook content (task 2.02).
 * Served via GET /api/support/scoring-weight-model (admin auth required).
 */
import {
  OPERATOR_MATCH_WEIGHTS,
  OPERATOR_MATCH_FACTOR_DEFINITIONS,
  OPERATOR_MATCH_SCORE_BANDS,
  OPERATOR_MATCH_AGGREGATION,
  getOperatorMatchWeightSummary,
} from "../operator-alignment-scoring-weight-config.js";
import {
  BRAND_MATCH_NEW_WEIGHTS,
  BRAND_MATCH_NEW_FACTOR_DEFINITIONS,
  BRAND_MATCH_NEW_AGGREGATION,
  BRAND_MATCH_SCORE_BANDS,
  BRAND_MATCH_V2_GATES,
  BRAND_MATCH_PREFERRED_BONUS,
  BRAND_MATCH_BREAKDOWN_ONLY_FACTORS,
  getBrandMatchNewWeightSummary,
} from "../brand-match-scoring-weight-config.js";

/** @returns {object} */
export function getScoringWeightModelRunbook() {
  const operatorSummary = getOperatorMatchWeightSummary();
  const brandSummary = getBrandMatchNewWeightSummary();

  return {
    title: "Dealality Scoring Weight Model",
    subtitle:
      "Pilot source of truth for operator-alignment and brand-match weights. Code configs drive live scoring; this page is the signed-off reference for product and GTM.",
    badges: [
      { label: "Internal Reference", variant: "internal" },
      { label: "Task 2.02" },
      { label: "Match Logic Framework" },
    ],
    warning:
      "<strong>Admin only.</strong> Weight changes affect owner-facing match scores. Update <code>lib/operator-alignment-scoring-weight-config.js</code> and <code>lib/brand-match-scoring-weight-config.js</code> first, then refresh this page. Do not hardcode weights in UI.",
    sections: [
      sectionOverview(operatorSummary, brandSummary),
      sectionOperatorWeights(operatorSummary),
      sectionOperatorFactors(),
      sectionOperatorAggregation(),
      sectionOperatorScoreBands(),
      sectionBrandWeights(brandSummary),
      sectionBrandFactors(),
      sectionBrandAggregation(),
      sectionBrandBands(),
      sectionGovernance(),
      sectionRelatedDocs(),
    ],
  };
}

function sectionOverview(operatorSummary, brandSummary) {
  return {
    id: "overview",
    title: "1. Overview",
    defaultOpen: true,
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "Dealality uses <strong>three match surfaces</strong> today. This document centralizes the <strong>numeric weights</strong> for operator and brand fit. Deal readiness / intake completeness is qualitative and not weighted here.",
      },
      {
        type: "table",
        headers: ["Surface", "Engine", "Config module", "Weight total"],
        rows: [
          [
            "<strong>Operator fit</strong>",
            "<code>scoreOperatorMatchForDeal</code> in <code>api/my-deals.js</code>",
            "<code>lib/operator-alignment-scoring-weight-config.js</code>",
            String(operatorSummary.documentedTotal) + " (" + operatorSummary.scoredFactorSum + " scored + " + operatorSummary.penaltyWeight + " penalty)",
          ],
                        [
            "<strong>Brand fit (Match Score v2)</strong>",
            "<code>computeMatchScoreForDealBrand</code> in <code>api/match-score-server.js</code>",
            "<code>lib/brand-match-scoring-weight-config.js</code>",
            String(brandSummary.total) + " (" + brandSummary.factorCount + " soft factors + preferred bonus + hard gates)",
          ],
          [
            "<strong>Brand fit (legacy 19-factor)</strong>",
            "Unused on product path — helpers may remain in <code>match-score-server</code> for lineage only",
            "Deprecated <code>WEIGHTS</code> in <code>api/match-score-server.js</code>",
            "Not used for product totals",
          ],
        ],
      },
      {
        type: "alert",
        html:
          "<strong>Pilot sign-off scope:</strong> Operator weights below are the active pilot weighting. " +
          "<strong>Brand Match Score v2</strong> uses 9 soft factors (sum 100), a +4 preferred-brand bonus (cap 100), and hard gates that force overall 0. " +
          "Live on Matched Brands / Shortlist / Target; Contacted / BDR uses frozen Score at request. Territory census gate is phased (v2.1+).",
      },
    ],
  };
}

function sectionOperatorWeights(operatorSummary) {
  const rows = OPERATOR_MATCH_FACTOR_DEFINITIONS.map((factor) => {
    const weight = OPERATOR_MATCH_WEIGHTS[factor.key];
    const pct = operatorSummary.scoredFactorSum
      ? ((weight / operatorSummary.scoredFactorSum) * 100).toFixed(1) + "%"
      : "—";
    return [
      factor.label,
      String(weight),
      factor.key === "negativeFitPenalty" ? "penalty" : pct + " of scored",
      factor.mvpQuality || "—",
    ];
  });

  return {
    id: "operator-weights",
    title: "2. Operator Match Weights (pilot)",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "Positive factors contribute to a weighted average. Factors with <strong>missing data (null score) are excluded</strong> from the denominator. " +
          "<code>negativeFitPenalty</code> is a low-weight factor (2): when deal breakers conflict with operator <code>lessIdealSituations</code>, that factor scores 20 (not a flat −2 point subtraction).",
      },
      {
        type: "table",
        headers: ["Factor", "Weight", "Share", "Data quality (MVP)"],
        rows,
      },
      {
        type: "code",
        text: JSON.stringify(OPERATOR_MATCH_WEIGHTS, null, 2),
      },
    ],
  };
}

function sectionOperatorFactors() {
  const rows = OPERATOR_MATCH_FACTOR_DEFINITIONS.filter((f) => f.key !== "negativeFitPenalty").map((factor) => [
    factor.label,
    factor.engineRef,
    (factor.dealFields || []).join(", "),
    (factor.operatorFields || []).slice(0, 3).join(", ") + ((factor.operatorFields || []).length > 3 ? "…" : ""),
    factor.notes,
  ]);

  return {
    id: "operator-factors",
    title: "3. Operator Factor → Field Mapping",
    contentBlocks: [
      {
        type: "paragraph",
        html: "Full field matrix: <code>docs/operator-alignment-field-matrix.md</code>. Factor scoring helpers: <code>lib/operator-alignment-scoring-factors.js</code>.",
      },
      {
        type: "table",
        headers: ["Factor", "Engine", "Deal fields", "Operator fields (sample)", "Notes"],
        rows,
      },
    ],
  };
}

function sectionOperatorAggregation() {
  return {
    id: "operator-aggregation",
    title: "4. Operator Score Aggregation (live engine)",
    contentBlocks: [
      {
        type: "paragraph",
        html: OPERATOR_MATCH_AGGREGATION.description,
      },
      {
        type: "code",
        text:
          "finalScore = round( sum(score × weight) / sum(weight), 1 )\n" +
          "// only factors where score != null\n" +
          "// source: scoreOperatorMatchForDeal in api/my-deals.js",
      },
    ],
  };
}

function sectionOperatorScoreBands() {
  return {
    id: "operator-bands",
    title: "5. Operator Score Bands (deployed UI)",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "My Deals operator strategy and Operator Development Dashboard read bands via <code>window.DcOperatorMatchScoreUi</code>, " +
          "fed by <code>/js/generated/operator-match-scoring-config.js</code> (generated from <code>lib/operator-alignment-scoring-weight-config.js</code>). " +
          "Change bands in the config module only — not in UI JavaScript.",
      },
      {
        type: "table",
        headers: ["Min score", "Label", "UI class"],
        rows: OPERATOR_MATCH_SCORE_BANDS.map((band) => [String(band.min), band.label, band.uiClass]),
      },
    ],
  };
}

function sectionBrandWeights(brandSummary) {
  const rows = BRAND_MATCH_NEW_FACTOR_DEFINITIONS.map((factor) => [
    factor.label,
    String(BRAND_MATCH_NEW_WEIGHTS[factor.key]),
    ((BRAND_MATCH_NEW_WEIGHTS[factor.key] / brandSummary.total) * 100).toFixed(1) + "%",
  ]);

  return {
    id: "brand-weights",
    title: "6. Brand Match v2 Soft Weights",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "<strong>" +
          brandSummary.factorCount +
          " soft factors</strong>; sum = <strong>" +
          brandSummary.total +
          "</strong>. Preferred brand is a <strong>+" +
          BRAND_MATCH_PREFERRED_BONUS +
          "</strong> post-average bonus (cap 100), not a soft weight. Hard gates can force overall 0.",
      },
      {
        type: "table",
        headers: ["Factor", "Weight", "Share"],
        rows,
      },
      {
        type: "code",
        text: JSON.stringify(BRAND_MATCH_NEW_WEIGHTS, null, 2),
      },
      {
        type: "paragraph",
        html: "Breakdown-only (not weighted): " + BRAND_MATCH_BREAKDOWN_ONLY_FACTORS.map((f) => f.label).join("; ") + ".",
      },
    ],
  };
}

function sectionBrandFactors() {
  const rows = BRAND_MATCH_NEW_FACTOR_DEFINITIONS.map((factor) => [
    factor.label,
    (factor.dealSignals || []).join(", "),
    (factor.brandSignals || []).join(", "),
    factor.notes,
  ]);

  return {
    id: "brand-factors",
    title: "7. Brand Factor → Signal Mapping",
    contentBlocks: [
      {
        type: "table",
        headers: ["Factor", "Deal signals", "Brand signals", "Notes"],
        rows,
      },
    ],
  };
}

function sectionBrandAggregation() {
  const gateRows = BRAND_MATCH_V2_GATES.map((g) => [g.label, g.key, g.failWhen]);
  return {
    id: "brand-aggregation",
    title: "8. Brand Score Aggregation (Match Score v2)",
    contentBlocks: [
      {
        type: "paragraph",
        html: BRAND_MATCH_NEW_AGGREGATION.description,
      },
      {
        type: "code",
        text:
          "scoredWeights = soft factors with non-null scores only\n" +
          "base = ( sum( (weight/100) × score ) / sum(scoredWeights) ) × 100\n" +
          "// null / missing factors are excluded from the denominator\n" +
          "base = min(100, base + (preferred ? " +
          BRAND_MATCH_PREFERRED_BONUS +
          " : 0))\n" +
          "if any hard gate fails (sufficient data) → score = 0\n" +
          "else if scoredWeightPct < minScoredWeightPct → insufficient data (no published score)\n" +
          "else → score = base\n" +
          "// source: computeMatchScoreNew + computeMatchScoreForDealBrand in api/match-score-server.js",
      },
      {
        type: "table",
        headers: ["Gate", "Key", "Fails when"],
        rows: gateRows,
      },
    ],
  };
}

function sectionBrandBands() {
  return {
    id: "brand-bands",
    title: "8b. Brand Match Score Bands (shared with operator UI colors)",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "Canonical thresholds are <strong>80 / 50 / 25</strong> — same numeric scale as operator alignment UI. " +
          "Brand Alignment Snapshot tiers use these bands via <code>brandMatchTierFromScore</code>. " +
          "Change only in <code>lib/brand-match-scoring-weight-config.js</code>.",
      },
      {
        type: "table",
        headers: ["Min score", "Label", "Tier label", "UI class"],
        rows: BRAND_MATCH_SCORE_BANDS.map((band) => [String(band.min), band.label, band.tierLabel, band.uiClass]),
      },
    ],
  };
}

function sectionGovernance() {
  return {
    id: "governance",
    title: "9. Change Governance",
    contentBlocks: [
      {
        type: "orderedList",
        className: "runbook-checklist",
        items: [
          "Edit the config module (<code>lib/*-scoring-weight-config.js</code>) — never UI constants.",
          "Run <code>node scripts/validate-operator-alignment-phase-5b.mjs</code> (operator) and spot-check brand match on a sample deal.",
          "Refresh this admin page and <code>docs/operator-alignment-scoring-weight-model.md</code>.",
          "Joan signs off pilot weighting → FPP task 2.02 → Completed.",
        ],
      },
      {
        type: "priority",
        variant: "soon",
        title: "Known gaps (do not misrepresent in owner copy)",
        items: [
          "Fee/commercial operator factor uses placeholder scoring when both sides have data.",
          "Owner relations relies on operator narrative keywords, not symmetric deal fields.",
          "Field matrix proposes OAS weights summing to 100 — engine uses 90+2 today; rebalancing is a future phase.",
          "Legacy 19-factor calc helpers may remain in match-score-server for lineage only; product totals are Match Score v2 only. Client calculators and passesStrictPreFilters are retired.",
        ],
      },
    ],
  };
}

function sectionRelatedDocs() {
  return {
    id: "related-docs",
    title: "10. Related Documentation",
    contentBlocks: [
      {
        type: "unorderedList",
        items: [
          "<code>docs/operator-alignment-scoring-weight-model.md</code> — repo markdown mirror",
          "<code>docs/operator-alignment-field-matrix.md</code> — deal/operator field matrix",
          "<code>docs/operator-alignment-scoring-data-quality-audit.md</code> — data quality audit",
          "<code>docs/internal-resources-hub.md</code> — where internal docs live on the platform",
          "Platform: <strong>Admin Resources → Scoring Weight Model</strong> (this page)",
        ],
      },
    ],
  };
}
