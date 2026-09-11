/**
 * ADP_EXECUTIVE_INSIGHT_PRIORITY_V3 + ONE_PRIMARY_PRIORITY + CONSTRAINT_CLASSIFICATION
 * Narrative prioritization only — not metric reweighting.
 */

import { INSIGHT_ARCHETYPES } from "./insight-candidates-v3.js";

export const ADP_EXECUTIVE_INSIGHT_PRIORITY_V3 = "ADP_EXECUTIVE_INSIGHT_PRIORITY_V3";
export const ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY = "ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY";
export const ADP_EXECUTIVE_CONSTRAINT_CLASSIFICATION_V3 =
  "ADP_EXECUTIVE_CONSTRAINT_CLASSIFICATION_V3";

const ARCHETYPE_PRIORITY = Object.freeze({
  [INSIGHT_ARCHETYPES.HIDDEN_WEAKNESS]: 1.0,
  [INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT]: 0.95,
  [INSIGHT_ARCHETYPES.REALITY_DISCONNECT]: 0.9,
  [INSIGHT_ARCHETYPES.BREADTH_PROBLEM]: 0.85,
  [INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM]: 0.82,
  [INSIGHT_ARCHETYPES.HIDDEN_STRENGTH]: 0.8,
  [INSIGHT_ARCHETYPES.PROVIDER_FRAGMENTATION]: 0.7,
  [INSIGHT_ARCHETYPES.PORTFOLIO_VS_NEUTRAL_CONTRADICTION]: 0.68,
  [INSIGHT_ARCHETYPES.EMERGING_MOVEMENT]: 0.6,
  [INSIGHT_ARCHETYPES.STRONG_PROTECT]: 0.55,
  [INSIGHT_ARCHETYPES.OTHER_EVIDENCE_BACKED]: 0.2,
});

export function scoreInsightCandidateV3(candidate, input = {}) {
  const arch = ARCHETYPE_PRIORITY[candidate.archetype] ?? 0.4;
  const materiality = candidate.materiality ?? 0.5;
  const commercial = candidate.commercialRelevance ?? 0.5;
  const recurrence = candidate.recurrence ?? 0.5;
  const confidence = candidate.confidence ?? 0.5;
  const actionability = candidate.actionability ?? 0.5;
  const nonObvious = candidate.nonObviousness ?? (candidate.archetype === INSIGHT_ARCHETYPES.HIDDEN_STRENGTH ? 0.85 : 0.55);
  const specificity =
    (candidate.supportingCompetitors?.length ? 0.15 : 0) +
    (candidate.supportingRealityGaps?.length ? 0.15 : 0) +
    (candidate.supportingTerritories?.length ? 0.1 : 0);

  let score =
    arch * 0.22 +
    materiality * 0.2 +
    commercial * 0.16 +
    recurrence * 0.1 +
    confidence * 0.12 +
    actionability * 0.1 +
    nonObvious * 0.08 +
    Math.min(0.1, specificity);

  const c = input?.aiConsideration;
  const sp = input?.scenarioPresence;
  const spGap = typeof c === "number" && typeof sp === "number" ? sp - c : 0;

  // Prefer entry when consideration is weak — rank/displacement tactics are secondary.
  if (typeof c === "number" && c < 30 && candidate.constraintClass === "ENTRY") {
    score += 0.35;
  }
  // Prefer consistency/breadth when SP>>C (large gaps outrank MEDIUM proposition Reality Gaps).
  // Apply only to answer-level inclusion / broad-reach candidates — not territory soft-spots.
  if (
    spGap >= 20 &&
    (candidate.type === "HIGH_SCENARIO_LOW_CONSIDERATION" ||
      candidate.type === "BROAD_REACH_WEAK_CONSISTENCY")
  ) {
    score += spGap >= 35 ? 0.36 : 0.28;
  }
  // Prefer peer-relative strength + residual gaps over raw displacement when overall consideration is healthy.
  if (
    typeof c === "number" &&
    c >= 55 &&
    candidate.archetype === INSIGHT_ARCHETYPES.HIDDEN_STRENGTH
  ) {
    score += 0.22;
  }
  // Distinctive proposition Reality Gaps — full boost only for HIGH severity.
  // MEDIUM magnitude alone must not outrank a larger consideration-consistency problem.
  if (
    typeof c === "number" &&
    c >= 35 &&
    candidate.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT &&
    /meeting|ballroom|marina|beach|cove|spa|bar|view|terrace|club|lifestyle|urban luxury|times square/i.test(
      (candidate.supportingRealityGaps || []).join(" ")
    )
  ) {
    const gapLabel = (candidate.supportingRealityGaps || [])[0];
    const gapMeta = (input.realityGaps || []).find((g) => g.label === gapLabel);
    const severity = candidate.severity || gapMeta?.severity || null;
    if (severity === "HIGH") {
      score += 0.3;
    } else if (severity === "MEDIUM") {
      score += 0.06;
      // If related meetings/group demand is already strongly captured, further softens primary claim.
      const meetingsCap =
        input.demandCaptureByIntent?.group_meeting?.rate ??
        input.demandCaptureByIntent?.group_meeting?.captureRate ??
        null;
      if (typeof meetingsCap === "number" && meetingsCap >= 85) {
        score -= 0.12;
      }
    } else {
      score += 0.18;
    }
  }
  // Soften pure displacement when consideration is already healthy and a stronger contrast exists.
  if (
    typeof c === "number" &&
    c >= 45 &&
    candidate.archetype === INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT &&
    candidate.type === "CONCENTRATED_DISPLACEMENT"
  ) {
    score -= 0.12;
  }
  // Soften low-material generic reality gaps (e.g. membership labels) vs commercial proposition gaps.
  if (
    candidate.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT &&
    /membership|distribution|privileges|bonvoy|honors/i.test(
      (candidate.supportingRealityGaps || []).join(" ")
    )
  ) {
    score -= 0.25;
  }

  return score;
}

/**
 * Classify primary constraint class from ranked candidates + input.
 */
export function classifyExecutiveConstraintV3(input, primaryCandidate) {
  if (primaryCandidate?.constraintClass) return primaryCandidate.constraintClass;
  const c = input?.aiConsideration;
  if (typeof c === "number" && c < 30) return "ENTRY";
  if (primaryCandidate?.archetype === INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT) {
    return "COMPETITIVE_DISPLACEMENT";
  }
  if (primaryCandidate?.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT) {
    return "REALITY_REPRESENTATION";
  }
  if (
    primaryCandidate?.archetype === INSIGHT_ARCHETYPES.BREADTH_PROBLEM ||
    primaryCandidate?.archetype === INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM
  ) {
    return "CONSISTENCY";
  }
  if (primaryCandidate?.archetype === INSIGHT_ARCHETYPES.HIDDEN_STRENGTH) return "RANK";
  if (primaryCandidate?.archetype === INSIGHT_ARCHETYPES.STRONG_PROTECT) return null;
  return "CONSISTENCY";
}

function derivePrimaryIssueId(primary, input) {
  if (!primary) return null;
  if (primary.archetype === INSIGHT_ARCHETYPES.STRONG_PROTECT) {
    const gap = primary.supportingRealityGaps?.[0] || input.realityGaps?.[0]?.label;
    if (gap) {
      return `protect_and_close_${String(gap).toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 36)}`;
    }
    return "protect_position";
  }
  if (primary.type === "ENTRY_PROBLEM_NOT_RANK") {
    const weak = input.presenceIndex?.weakest?.intent || "demand";
    if (/couples|leisure|lifestyle/i.test(weak)) return `lifestyle_couples_entry`;
    return `entry_${weak}`;
  }
  if (primary.type === "PROPOSITION_REALITY_GAP") {
    const gap = String(primary.supportingRealityGaps?.[0] || "proposition").toLowerCase();
    if (/meeting|ballroom/.test(gap)) return "meeting_ballroom_proposition";
    if (/marina|beach|waterfront/.test(gap)) return "marina_beach_proposition";
    if (/cove|private beach/.test(gap)) return "business_group_positioning_gap";
    if (/times square|view/.test(gap)) return "times_square_entry_and_views";
    if (/urban luxury|signature|bar|spa/.test(gap)) return "signature_urban_representation";
    return `reality_${gap.replace(/[^a-z0-9]+/g, "_").slice(0, 40)}`;
  }
  if (primary.type === "CONCENTRATED_DISPLACEMENT") {
    // If a meeting/ballroom gap coexists, prefer proposition framing
    const meetGap = (input.realityGaps || []).find((g) =>
      /meeting|ballroom/i.test(g.label || "")
    );
    if (meetGap && (meetGap.recognitionRate ?? 100) < 35) return "meeting_ballroom_proposition";
    const marina = (input.realityGaps || []).find((g) => /marina|beach/i.test(g.label || ""));
    if (marina && (marina.recognitionRate ?? 100) < 25) return "marina_beach_proposition";
    if (typeof input.aiConsideration === "number" && input.aiConsideration < 25) {
      return "lifestyle_couples_entry";
    }
    if (typeof input.aiConsideration === "number" && input.aiConsideration < 20) {
      return "times_square_entry_and_views";
    }
    const name = primary.supportingCompetitors?.[0] || "competitor";
    return `displacement_${String(name).toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 40)}`;
  }
  if (primary.type === "HIGH_SCENARIO_LOW_CONSIDERATION" || primary.type === "BROAD_REACH_WEAK_CONSISTENCY") {
    if (typeof input.aiConsideration === "number" && input.scenarioPresence - input.aiConsideration >= 30) {
      return "answer_level_inclusion_consistency";
    }
    if (typeof input.aiConsideration === "number" && input.aiConsideration < 50) {
      return "business_consideration_consistency";
    }
    return "consistency_consideration_gap";
  }
  if (primary.type === "TERRITORY_CONCENTRATION") {
    const weak = primary.supportingTerritories?.[0] || "territory";
    return `territory_${weak}_soft_spot`;
  }
  if (primary.type === "STRONG_WHEN_RANKED_WEAK_ENTRY") {
    return "entry_before_rank";
  }
  if (primary.type === "PEER_RELATIVE_SURPRISE") {
    const t = primary.supportingTerritories?.[0] || "strength";
    const meetGap = (input.realityGaps || []).find((g) => /cove|meeting|group|business/i.test(g.label || ""));
    if (meetGap) return "business_group_positioning_gap";
    return `protect_${t}_strength_with_residual_gaps`;
  }
  return String(primary.candidateId || "primary_issue").replace(/^cand_/, "issue_");
}

/**
 * Merge supporting candidates that reinforce the same underlying issue.
 */
export function selectPrimaryInsightV3(candidates, input) {
  const ranked = [...(candidates || [])]
    .map((c) => ({ ...c, priorityScore: scoreInsightCandidateV3(c, input) }))
    .sort((a, b) => b.priorityScore - a.priorityScore);

  const primary = ranked[0] || null;
  if (!primary || primary.confidence < 0.4 || primary.archetype === INSIGHT_ARCHETYPES.OTHER_EVIDENCE_BACKED) {
    return {
      ok: false,
      reason: "EXECUTIVE_READ_REVIEW_REQUIRED",
      detail: primary?.summary || "No confident primary insight",
      ranked,
      primaryIssueId: null,
      primary: null,
      supporting: [],
      constraintClass: null,
    };
  }

  // Strong-protect: prefer residual reality gap as supporting Focus if present
  let effectivePrimary = primary;
  if (primary.archetype === INSIGHT_ARCHETYPES.STRONG_PROTECT) {
    const residualGap = ranked.find((c) => c.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT);
    const residualDisp = ranked.find((c) => c.archetype === INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT);
    if (residualGap && residualGap.materiality >= 0.55) {
      effectivePrimary = {
        ...primary,
        supportingRealityGaps: residualGap.supportingRealityGaps,
        residualFocus: residualGap,
      };
    } else if (residualDisp && residualDisp.materiality >= 0.7) {
      effectivePrimary = {
        ...primary,
        supportingCompetitors: residualDisp.supportingCompetitors,
        residualFocus: residualDisp,
      };
    }
  }

  const supporting = ranked.slice(1, 4).filter((c) => {
    if (c.candidateId === effectivePrimary.candidateId) return false;
    // Same underlying family only
    if (effectivePrimary.constraintClass && c.constraintClass === effectivePrimary.constraintClass) return true;
    if (
      effectivePrimary.archetype === INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT &&
      c.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT
    ) {
      return true; // meeting/ballroom + displacement pattern
    }
    if (
      effectivePrimary.archetype === INSIGHT_ARCHETYPES.HIDDEN_STRENGTH &&
      (c.archetype === INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT ||
        c.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT)
    ) {
      return true;
    }
    return false;
  });

  return {
    ok: true,
    rule: ADP_EXECUTIVE_INSIGHT_PRIORITY_V3,
    primaryIssueId: derivePrimaryIssueId(effectivePrimary, input),
    primary: effectivePrimary,
    supporting,
    ranked,
    constraintClass: classifyExecutiveConstraintV3(input, effectivePrimary),
    insightArchetype: effectivePrimary.archetype,
  };
}
