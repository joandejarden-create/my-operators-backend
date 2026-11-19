/**
 * Deterministic ADP attribute recommendation engine (PR-2).
 * Grounded in verified profile facts + demand-segment relevance.
 * Never invents capabilities; never auto-approves.
 */

import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import { hasAttributeRecognitionKeywords } from "../execution/attribute-recognition-keywords-v1.js";
import {
  DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES,
  suggestAttributeCandidates,
} from "./adp-attribute-suggestion-candidates-v1.js";
import { classifySelectedAttributeEligibility } from "./adp-attribute-eligibility-classifier-v1.js";
import {
  collapseToUniqueMeasurableConcepts,
  getSemanticConceptId,
  analyzeSelectedSemanticOverlaps,
  SEMANTIC_RELATION,
} from "./adp-semantic-equivalence-v1.js";
import { assessMeasurementCoveragePolicy } from "./adp-measurement-coverage-policy-v1.js";
import { validateAttributeDefinitionAndRecognition } from "./adp-attribute-definition-recognition-validation-v1.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";

export const ADP_ATTRIBUTE_RECOMMENDATION_ENGINE_V1 =
  "ADP_ATTRIBUTE_RECOMMENDATION_ENGINE_V1";

export const RECOMMENDED_ACTION = Object.freeze({
  KEEP: "KEEP",
  ADD: "ADD",
  VERIFY: "VERIFY",
  DEFINE: "DEFINE",
  CONSIDER_REMOVING: "CONSIDER_REMOVING",
  EXCLUDE: "EXCLUDE",
});

const PRIORITY_SCORE = { high: 3, medium: 2, low: 1 };

function scoreCandidate({
  strategicSelected,
  verified,
  defined,
  recognition,
  segmentHitCount,
  priority,
  semanticRelation,
}) {
  let score = 0;
  if (strategicSelected) score += 40;
  if (verified) score += 25;
  if (defined) score += 10;
  if (recognition) score += 10;
  score += Math.min(15, segmentHitCount * 5);
  score += PRIORITY_SCORE[priority] || 0;
  if (semanticRelation === SEMANTIC_RELATION.TRUE_DUPLICATE && !strategicSelected) score -= 20;
  return score;
}

/**
 * Build full recommendation package for one property.
 */
export function buildAttributeRecommendationPackage(propertyProfile, opts = {}) {
  const propertyId = propertyProfile?.propertyId || null;
  const hpcHotelId =
    propertyProfile?.censusRecordId ||
    propertyProfile?.hpcHotelId ||
    getCensusRecordIdForAdpProperty(propertyId) ||
    null;
  const selected = Array.isArray(propertyProfile?.attributes)
    ? [...propertyProfile.attributes]
    : [];

  const eligibility = classifySelectedAttributeEligibility(selected, propertyProfile, opts);
  const validation = validateAttributeDefinitionAndRecognition(selected, propertyProfile, opts);
  const suggestions = suggestAttributeCandidates(propertyProfile, { selectedKeys: selected });
  const semantic = analyzeSelectedSemanticOverlaps(selected);

  const eligibleKeys = eligibility.eligibleKeys;
  const unique = collapseToUniqueMeasurableConcepts(eligibleKeys, {
    countBroaderNarrowerSeparately: false,
  });
  const coverage = assessMeasurementCoveragePolicy(unique.uniqueConceptCount, opts);

  const segmentHints = suggestions.demandSegmentsConsidered || [];

  const recommendations = [];

  // Selected inventory recommendations
  for (const row of eligibility.rows) {
    const key = row.attribute;
    const def = ATTRIBUTE_DEFINITIONS[key];
    const recognition = hasAttributeRecognitionKeywords(key);
    const verified = row.inEligibleDenominator;
    const overlaps = semantic.pairs.filter((p) => p.a === key || p.b === key);
    const trueDup = overlaps.find((p) => p.relation === SEMANTIC_RELATION.TRUE_DUPLICATE);

    let action = RECOMMENDED_ACTION.KEEP;
    let explanation = "Selected strategic attribute with measurement eligibility";

    if (!def) {
      action = RECOMMENDED_ACTION.DEFINE;
      explanation = "Selected but missing ATTRIBUTE_DEFINITIONS / recognition registry entry";
    } else if (!verified) {
      action = RECOMMENDED_ACTION.VERIFY;
      explanation = "Selected but insufficient verified evidence for measurable eligibility";
    } else if (trueDup) {
      action = RECOMMENDED_ACTION.CONSIDER_REMOVING;
      explanation = `True-duplicate / near-synonym overlap with ${trueDup.a === key ? trueDup.b : trueDup.a} — steward may keep one concept for max-20 focus`;
    } else if (!recognition) {
      action = RECOMMENDED_ACTION.DEFINE;
      explanation = "Defined but missing recognition keywords";
    }

    const segmentHitCount = segmentHints.filter((seg) =>
      (DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES[seg] || []).includes(key)
    ).length;

    recommendations.push({
      attribute: key,
      label: def?.label || key,
      canonicalHotelId: hpcHotelId,
      adpPropertyId: propertyId,
      currentlySelected: true,
      verifiedFactRefs: row.evidenceUrl ? [row.evidenceUrl] : [],
      evidenceNote: row.note || null,
      relevantTargetSegments: segmentHints.filter((seg) =>
        (DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES[seg] || []).includes(key)
      ),
      commercialRelevanceRationale:
        segmentHitCount > 0
          ? `Aligned with demand segments: ${segmentHints.filter((seg) => (DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES[seg] || []).includes(key)).join(", ")}`
          : "Part of existing hotel strategic selection baseline",
      suggestedPriority: def?.priority || "medium",
      verificationStatus: row.verificationStatus,
      definitionCoverage: Boolean(def),
      recognitionRuleCoverage: recognition,
      semanticConceptId: getSemanticConceptId(key),
      semanticOverlaps: overlaps,
      eligibilityForMeasurement: row.inEligibleDenominator,
      recommendedAction: action,
      explanation,
      rankingScore: scoreCandidate({
        strategicSelected: true,
        verified,
        defined: Boolean(def),
        recognition,
        segmentHitCount,
        priority: def?.priority,
        semanticRelation: trueDup?.relation,
      }),
      autoApproved: false,
    });
  }

  // Suggested additions (not selected)
  for (const s of suggestions.suggestions || []) {
    const recognition = hasAttributeRecognitionKeywords(s.attribute);
    const action = !ATTRIBUTE_DEFINITIONS[s.attribute]
      ? RECOMMENDED_ACTION.DEFINE
      : !s.verifiedSupport
        ? RECOMMENDED_ACTION.VERIFY
        : RECOMMENDED_ACTION.ADD;

    recommendations.push({
      attribute: s.attribute,
      label: s.label,
      canonicalHotelId: hpcHotelId,
      adpPropertyId: propertyId,
      currentlySelected: false,
      verifiedFactRefs: s.evidenceUrl ? [s.evidenceUrl] : [],
      evidenceNote: s.supportingFact || null,
      relevantTargetSegments: s.relevantSegments || [],
      commercialRelevanceRationale: s.reason,
      suggestedPriority: s.suggestedPriority,
      verificationStatus: s.verificationStatus,
      definitionCoverage: Boolean(ATTRIBUTE_DEFINITIONS[s.attribute]),
      recognitionRuleCoverage: recognition,
      semanticConceptId: getSemanticConceptId(s.attribute),
      semanticOverlaps: [],
      eligibilityForMeasurement: s.verifiedSupport === true && Boolean(ATTRIBUTE_DEFINITIONS[s.attribute]),
      recommendedAction: action,
      explanation: s.reason,
      rankingScore: scoreCandidate({
        strategicSelected: false,
        verified: s.verifiedSupport,
        defined: Boolean(ATTRIBUTE_DEFINITIONS[s.attribute]),
        recognition,
        segmentHitCount: (s.relevantSegments || []).length,
        priority: s.suggestedPriority,
      }),
      autoApproved: false,
    });
  }

  recommendations.sort((a, b) => b.rankingScore - a.rankingScore);

  // Proposed shortlist ≤ maximum (unique concepts), preserving selected KEEP/VERIFY first
  const shortlist = buildPrioritizedShortlist(recommendations, coverage.maximum);

  const researchNeeded = recommendations.filter((r) =>
    [RECOMMENDED_ACTION.VERIFY, RECOMMENDED_ACTION.DEFINE].includes(r.recommendedAction)
  );

  return {
    contractVersion: ADP_ATTRIBUTE_RECOMMENDATION_ENGINE_V1,
    propertyId,
    canonicalHotelId: hpcHotelId,
    selectedRawCount: selected.length,
    eligibleRawCount: eligibleKeys.length,
    uniqueEligibleConceptCount: unique.uniqueConceptCount,
    uniqueEligibleConceptKeys: unique.uniqueConceptKeys,
    semanticCollapse: unique.collapsed,
    coverage,
    validationSummary: {
      missingDefinitions: validation.missingDefinitions,
      missingRecognitionRules: validation.missingRecognitionRules,
      insufficientVerification: validation.insufficientVerification,
      semanticOverlapCount: validation.semanticOverlaps.length,
    },
    recommendations,
    proposedShortlistMax20: shortlist,
    researchNeededQueue: researchNeeded,
    selectionPreserved: selected,
    note:
      "Suggestions never overwrite fixture/strategic selection. Airtable approval required before measurement inventory changes.",
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Prefer strategic KEEP eligible, then ADD verified, exclude true-dup pads.
 */
export function buildPrioritizedShortlist(recommendations, maximum = 20) {
  const max = Math.max(1, Number(maximum) || 20);
  const picked = [];
  const seenConcepts = new Set();

  const ordered = [...recommendations].sort((a, b) => {
    // Keep selected eligible first
    const aKeep = a.currentlySelected && a.eligibilityForMeasurement ? 1 : 0;
    const bKeep = b.currentlySelected && b.eligibilityForMeasurement ? 1 : 0;
    if (aKeep !== bKeep) return bKeep - aKeep;
    return b.rankingScore - a.rankingScore;
  });

  for (const rec of ordered) {
    if (picked.length >= max) break;
    if (rec.recommendedAction === RECOMMENDED_ACTION.EXCLUDE) continue;
    if (!rec.definitionCoverage) continue;
    // Only measurable candidates for shortlist count
    if (!rec.eligibilityForMeasurement && rec.recommendedAction !== RECOMMENDED_ACTION.KEEP) {
      // Allow selected KEEP even if pending verify for display, but don't count unverified toward shortlist fill from ADD
      if (!rec.currentlySelected) continue;
    }
    if (rec.recommendedAction === RECOMMENDED_ACTION.ADD && !rec.eligibilityForMeasurement) continue;
    if (rec.recommendedAction === RECOMMENDED_ACTION.CONSIDER_REMOVING) continue;

    const concept = rec.semanticConceptId;
    if (seenConcepts.has(concept) && !rec.currentlySelected) continue;
    // For unique concept shortlist, skip true-dup extras
    if (seenConcepts.has(concept)) continue;

    seenConcepts.add(concept);
    picked.push({
      attribute: rec.attribute,
      label: rec.label,
      semanticConceptId: concept,
      rankingScore: rec.rankingScore,
      recommendedAction: rec.recommendedAction,
      currentlySelected: rec.currentlySelected,
      reason: rec.explanation,
    });
  }

  const proposedExclusions = recommendations
    .filter((r) => r.currentlySelected && !picked.some((p) => p.attribute === r.attribute))
    .map((r) => ({
      attribute: r.attribute,
      reason:
        r.recommendedAction === RECOMMENDED_ACTION.CONSIDER_REMOVING
          ? r.explanation
          : "Not in prioritized shortlist within maximum-20 unique concepts — steward decision required; selection not auto-removed",
    }));

  return {
    maximum: max,
    count: picked.length,
    items: picked,
    proposedExclusions,
    autoRemoved: false,
  };
}
