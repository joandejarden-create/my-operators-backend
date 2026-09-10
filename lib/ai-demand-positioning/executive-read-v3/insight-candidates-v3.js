/**
 * ADP_EXECUTIVE_INSIGHT_CANDIDATE_GENERATION_V3
 * Generic candidates from certified analytical relationships — no property hard-codes.
 */

import { INSIGHT_ARCHETYPES } from "../governance/run-executive-summary-insight-quality-audit-v1.js";

export const ADP_EXECUTIVE_INSIGHT_CANDIDATE_GENERATION_V3 =
  "ADP_EXECUTIVE_INSIGHT_CANDIDATE_GENERATION_V3";

export { INSIGHT_ARCHETYPES };

function scoreBand(n, high, mid) {
  if (typeof n !== "number") return 0.4;
  if (n >= high) return 1;
  if (n >= mid) return 0.7;
  return 0.45;
}

function baseCandidate(partial) {
  return {
    candidateId: partial.candidateId,
    archetype: partial.archetype,
    summary: partial.summary,
    supportingMetrics: partial.supportingMetrics || [],
    supportingTerritories: partial.supportingTerritories || [],
    supportingCompetitors: partial.supportingCompetitors || [],
    supportingRealityGaps: partial.supportingRealityGaps || [],
    supportingEvidenceRefs: partial.supportingEvidenceRefs || [],
    commercialRelevance: partial.commercialRelevance ?? 0.6,
    recurrence: partial.recurrence ?? 0.5,
    materiality: partial.materiality ?? 0.5,
    actionability: partial.actionability ?? 0.55,
    confidence: partial.confidence ?? 0.6,
    claimType: partial.claimType || "SUPPORTED_INTERPRETATION",
    constraintClass: partial.constraintClass || null,
    type: partial.type,
    severity: partial.severity || null,
    nonObviousness: partial.nonObviousness,
  };
}

/**
 * @param {object} input — ADP_EXECUTIVE_READ_INPUT_V3
 * @returns {object[]}
 */
export function generateExecutiveInsightCandidatesV3(input) {
  if (!input?.ok) return [];
  const candidates = [];
  const c = input.aiConsideration;
  const sp = input.scenarioPresence;
  const top3 = input.rankMetrics?.top3;
  const disp = input.competitiveDisplacement || [];
  const gaps = input.realityGaps || [];
  const weak = input.presenceIndex?.weakest;
  const strong = input.presenceIndex?.strongest;
  const topAlt = input.topObservedAlternative;
  const bench = input.classifierHints?.benchmarkFinding;

  if (typeof c === "number" && typeof sp === "number" && sp - c >= 20) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_breadth_sp_vs_consideration",
        type: "HIGH_SCENARIO_LOW_CONSIDERATION",
        archetype: INSIGHT_ARCHETYPES.BREADTH_PROBLEM,
        summary: `Scenario Presence ${sp}% versus AI Consideration ${c}% — broad reach with weaker response-level inclusion.`,
        supportingMetrics: [
          { metricId: "executiveMetrics.scenarioPresence.rate", value: sp },
          { metricId: "executiveMetrics.considerationRate.rate", value: c },
        ],
        materiality: scoreBand(sp - c, 35, 20),
        commercialRelevance: 0.75,
        recurrence: 0.7,
        actionability: 0.65,
        confidence: 0.85,
        claimType: "GOVERNED_METRIC",
        constraintClass: "CONSISTENCY",
      })
    );
  }

  if (typeof c === "number" && typeof sp === "number" && sp >= 65 && c < 50) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_consistency_broad_weak",
        type: "BROAD_REACH_WEAK_CONSISTENCY",
        archetype: INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM,
        summary: "Broad scenario reach with materially weaker consideration consistency.",
        supportingMetrics: [
          { metricId: "executiveMetrics.scenarioPresence.rate", value: sp },
          { metricId: "executiveMetrics.considerationRate.rate", value: c },
        ],
        materiality: 0.8,
        commercialRelevance: 0.8,
        confidence: 0.8,
        constraintClass: "CONSISTENCY",
      })
    );
  }

  if (
    typeof top3 === "number" &&
    top3 >= 70 &&
    typeof c === "number" &&
    c < 55 &&
    input.rankMetrics?.customerNarrativeEligible === true
  ) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_hidden_strength_rank_vs_entry",
        type: "STRONG_WHEN_RANKED_WEAK_ENTRY",
        archetype: INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
        summary: `Top-3 appearance ${top3}% when ranked, but consideration only ${c}%.`,
        supportingMetrics: [
          { metricId: "executiveMetrics.rankMetrics.topThreeAppearanceRate", value: top3 },
          { metricId: "executiveMetrics.considerationRate.rate", value: c },
        ],
        materiality: 0.85,
        commercialRelevance: 0.85,
        nonObviousness: 0.9,
        confidence: 0.8,
        constraintClass: "ENTRY",
      })
    );
  } else if (
    typeof top3 === "number" &&
    input.rankMetrics?.customerNarrativeEligible !== true
  ) {
    // Thin rank-eligible n — keep as diagnostic only; do not compete for primary.
    candidates.push(
      baseCandidate({
        candidateId: "cand_rank_metrics_supporting_diagnostic",
        type: "RANK_METRICS_DIAGNOSTIC_ONLY",
        archetype: INSIGHT_ARCHETYPES.OTHER_EVIDENCE_BACKED,
        summary: `Top-3 / #1 metrics are supporting diagnostic only (rank-eligible n=${input.rankMetrics?.rankEligibleN ?? "n/a"}).`,
        supportingMetrics: [
          { metricId: "executiveMetrics.rankMetrics.topThreeAppearanceRate", value: top3 },
        ],
        materiality: 0.25,
        commercialRelevance: 0.3,
        confidence: 0.35,
        claimType: "REASONABLE_REVIEW_HYPOTHESIS",
        constraintClass: null,
      })
    );
  }

  if (typeof bench?.index === "number" && bench.index >= 200) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_peer_relative_surprise",
        type: "PEER_RELATIVE_SURPRISE",
        archetype: INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
        summary: `${bench.territory || bench.intent || "Territory"} Presence Index ${bench.index} versus peer parity.`,
        supportingMetrics: [
          {
            metricId: `intentPresenceIndex.${bench.intent || bench.territory || "territory"}.index`,
            value: bench.index,
          },
        ],
        supportingTerritories: [bench.intent || bench.territory].filter(Boolean),
        materiality: scoreBand(bench.index, 400, 200),
        commercialRelevance: 0.7,
        confidence: 0.75,
        claimType: "GOVERNED_METRIC",
      })
    );
  }

  if (typeof strong?.index === "number" && strong.index >= 200) {
    candidates.push(
      baseCandidate({
        candidateId: `cand_strong_index_${strong.intent}`,
        type: "PEER_RELATIVE_SURPRISE",
        archetype: INSIGHT_ARCHETYPES.HIDDEN_STRENGTH,
        summary: `${strong.label || strong.intent} Presence Index ${strong.index}.`,
        supportingMetrics: [
          { metricId: `intentPresenceIndex.${strong.intent}.index`, value: strong.index },
        ],
        supportingTerritories: [strong.intent],
        materiality: scoreBand(strong.index, 400, 200),
        confidence: 0.75,
      })
    );
  }

  if (disp[0]?.displacementCount >= 8) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_concentrated_displacement",
        type: "CONCENTRATED_DISPLACEMENT",
        archetype: INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
        summary: `${disp[0].name} appears in ${disp[0].displacementCount} monitored scenarios where the hotel is absent.`,
        supportingCompetitors: [disp[0].name],
        supportingMetrics: [
          {
            metricId: "lostDemand.displacement[0].displacementCount",
            value: disp[0].displacementCount,
          },
        ],
        supportingEvidenceRefs: [`displacement:${disp[0].entityId || disp[0].name}`],
        materiality: scoreBand(disp[0].displacementCount, 20, 10),
        commercialRelevance: 0.9,
        recurrence: 0.85,
        actionability: 0.85,
        confidence: 0.85,
        claimType: "OBSERVED_FACT",
        constraintClass: "COMPETITIVE_DISPLACEMENT",
      })
    );
  }

  if (typeof input.competitorPresentShare === "number" && input.competitorPresentShare >= 0.35) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_competitor_present_gaps",
        type: "HIGH_COMPETITOR_PRESENT_GAPS",
        archetype: INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
        summary: `Competitors appear in a material share of scenarios where the hotel is absent (${Math.round(input.competitorPresentShare * 1000) / 10}%).`,
        supportingMetrics: [
          { metricId: "lostDemand.competitorPresentShare", value: input.competitorPresentShare },
        ],
        materiality: 0.7,
        commercialRelevance: 0.75,
        confidence: 0.7,
        constraintClass: "COMPETITIVE_DISPLACEMENT",
      })
    );
  }

  const materialGaps = gaps.filter(
    (g) =>
      ((g.severity === "HIGH" && (g.recognitionRate ?? 100) < 30) ||
        (g.severity === "MEDIUM" && (g.recognitionRate ?? 100) < 25) ||
        (!g.severity && (g.recognitionRate ?? 100) < 25)) &&
      !/membership|distribution|privileges|bonvoy|honors|choice hotels|rooms|parking|pet/i.test(
        g.label || ""
      )
  );
  const rankedGaps = [...materialGaps].sort((a, b) => {
    const score = (g) => {
      let s = 40 - (g.recognitionRate ?? 40);
      if (/meeting|ballroom|marina|beach|cove|spa|bar|view|terrace|urban luxury|times square/i.test(g.label || "")) {
        s += 20;
      }
      if (g.severity === "HIGH") s += 12;
      if (g.severity === "MEDIUM") s -= 8; // MEDIUM is eligible but must not win on magnitude alone
      return s;
    };
    return score(b) - score(a);
  });
  for (const materialGap of rankedGaps.slice(0, 2)) {
    const commercialProposition = /meeting|ballroom|marina|beach|cove|spa|lifestyle|urban/i.test(
      materialGap.label || ""
    );
    candidates.push(
      baseCandidate({
        candidateId: `cand_reality_gap_${String(materialGap.attribute || materialGap.label)
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "_")
          .slice(0, 40)}`,
        type: "PROPOSITION_REALITY_GAP",
        archetype: INSIGHT_ARCHETYPES.REALITY_DISCONNECT,
        summary: `${materialGap.label} is recognized in only ${materialGap.recognitionRate}% of monitored answers.`,
        supportingRealityGaps: [materialGap.label],
        supportingMetrics: [
          {
            metricId: `realityGap.${materialGap.label}.recognitionRate`,
            value: materialGap.recognitionRate,
          },
        ],
        materiality:
          materialGap.severity === "HIGH"
            ? scoreBand(40 - (materialGap.recognitionRate ?? 0), 30, 15)
            : scoreBand(40 - (materialGap.recognitionRate ?? 0), 30, 15) * 0.75,
        commercialRelevance: commercialProposition
          ? materialGap.severity === "HIGH"
            ? 0.92
            : 0.72
          : materialGap.severity === "HIGH"
            ? 0.8
            : 0.55,
        actionability: 0.9,
        confidence: materialGap.severity === "HIGH" ? 0.8 : 0.68,
        claimType: "GOVERNED_METRIC",
        constraintClass: "REALITY_REPRESENTATION",
        severity: materialGap.severity || null,
      })
    );
  }

  if (typeof c === "number" && c < 25) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_entry_problem",
        type: "ENTRY_PROBLEM_NOT_RANK",
        archetype: INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS,
        summary: `AI Consideration is ${c}% — the primary constraint is entering consideration, not rank after inclusion.`,
        supportingMetrics: [{ metricId: "executiveMetrics.considerationRate.rate", value: c }],
        supportingTerritories: weak ? [weak.intent] : [],
        materiality: scoreBand(30 - c, 20, 10),
        commercialRelevance: 0.95,
        actionability: 0.9,
        confidence: 0.9,
        claimType: "GOVERNED_METRIC",
        constraintClass: "ENTRY",
      })
    );
  }

  if (typeof sp === "number" && sp >= 85 && typeof c === "number" && c >= 65 && (!disp[0] || disp[0].displacementCount < 8)) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_strong_protect",
        type: "ELITE_BROAD_STRENGTH",
        archetype: INSIGHT_ARCHETYPES.STRONG_PROTECT,
        summary: `Strong Scenario Presence (${sp}%) and Consideration (${c}%) — protect position; do not manufacture a crisis.`,
        supportingMetrics: [
          { metricId: "executiveMetrics.scenarioPresence.rate", value: sp },
          { metricId: "executiveMetrics.considerationRate.rate", value: c },
        ],
        materiality: 0.7,
        commercialRelevance: 0.7,
        actionability: 0.55,
        confidence: 0.8,
        claimType: "SUPPORTED_INTERPRETATION",
        constraintClass: null,
      })
    );
  }

  if (weak && strong && typeof weak.rate === "number" && typeof strong.rate === "number" && strong.rate - weak.rate >= 25) {
    // Family/wellness soft spots remain candidates but must not auto-outrank hotel-wide consideration gaps.
    const familySoft = /family/i.test(weak.intent || "") || /family/i.test(weak.label || "");
    candidates.push(
      baseCandidate({
        candidateId: "cand_territory_spread",
        type: "TERRITORY_CONCENTRATION",
        archetype: INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM,
        summary: `Territory presence ranges from ${weak.rate}% (${weak.label}) to ${strong.rate}% (${strong.label}).`,
        supportingTerritories: [weak.intent, strong.intent],
        supportingMetrics: [
          { metricId: `intentPresenceIndex.${weak.intent}.rate`, value: weak.rate },
          { metricId: `intentPresenceIndex.${strong.intent}.rate`, value: strong.rate },
        ],
        materiality: familySoft ? 0.55 : 0.75,
        commercialRelevance: familySoft ? 0.5 : 0.8,
        confidence: 0.75,
        constraintClass: "CONSISTENCY",
      })
    );
  }

  if (topAlt?.name || topAlt?.canonicalName) {
    const altName = topAlt.name || topAlt.canonicalName;
    candidates.push(
      baseCandidate({
        candidateId: "cand_top_observed_alternative",
        type: "TOP_OBSERVED_ALTERNATIVE",
        archetype: INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT,
        summary: `${altName} is the top observed AI alternative in the monitored set.`,
        supportingCompetitors: [altName],
        supportingEvidenceRefs: [`topObservedAlternative:${altName}`],
        materiality: 0.55,
        commercialRelevance: 0.7,
        recurrence: 0.6,
        confidence: 0.7,
        claimType: "OBSERVED_FACT",
        constraintClass: "COMPETITIVE_DISPLACEMENT",
      })
    );
  }

  const bpp = input.bpp;
  if (bpp && bpp.neutralVsPortfolioContradiction) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_portfolio_vs_neutral",
        type: "PORTFOLIO_VS_NEUTRAL",
        archetype: INSIGHT_ARCHETYPES.PORTFOLIO_VS_NEUTRAL_CONTRADICTION,
        summary: "Portfolio lens and neutral positioning signals diverge in a material way.",
        supportingEvidenceRefs: ["brandPortfolioPosition"],
        materiality: 0.65,
        commercialRelevance: 0.7,
        confidence: 0.6,
        constraintClass: "PROVIDER_FRAGMENTATION",
      })
    );
  }

  // Fail-soft: at least one OTHER if we have any metric but no candidates
  if (!candidates.length && (typeof c === "number" || typeof sp === "number")) {
    candidates.push(
      baseCandidate({
        candidateId: "cand_insufficient_contrast",
        type: "INSUFFICIENT_CONTRAST",
        archetype: INSIGHT_ARCHETYPES.OTHER_EVIDENCE_BACKED,
        summary: "Analytical state does not yield a high-confidence primary executive contrast.",
        supportingMetrics: [
          ...(typeof c === "number"
            ? [{ metricId: "executiveMetrics.considerationRate.rate", value: c }]
            : []),
          ...(typeof sp === "number"
            ? [{ metricId: "executiveMetrics.scenarioPresence.rate", value: sp }]
            : []),
        ],
        materiality: 0.2,
        commercialRelevance: 0.3,
        confidence: 0.35,
        claimType: "REASONABLE_REVIEW_HYPOTHESIS",
      })
    );
  }

  return candidates;
}
