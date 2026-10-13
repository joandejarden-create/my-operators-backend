/**
 * Qualification-precision pass — re-apply buildOpportunity (with gates)
 * across an existing opportunity list. $0 Webhound.
 */

import { buildOpportunity } from "./opportunity-factory.js";
import { PRIORITY } from "./claim-types.js";
import {
  applyCommercialQaOverride,
  finalizeCommercialQaOpportunity,
  COMMERCIAL_QA_PASS_ID,
  MEDIUM_CONTACT_VS_QUALIFICATION_NOTES,
} from "./commercial-qa-overrides-v1.js";

function toRebuildInput(o) {
  return {
    ...o,
    fitComponents: {
      physicalFit: o.physicalFitScore ?? o.fitComponents?.physicalFit,
      geographyFit: o.geographyFitScore ?? o.fitComponents?.geographyFit,
      timing: o.timingScore ?? o.fitComponents?.timing,
      commercialValue: o.commercialValueScore ?? o.fitComponents?.commercialValue,
      historicalFit: o.historicalFitScore ?? o.fitComponents?.historicalFit,
      competitiveAccessibility:
        o.competitiveAccessibilityScore ?? o.fitComponents?.competitiveAccessibility,
      contactability: o.contactabilityScore ?? o.fitComponents?.contactability,
    },
    confidenceInput: o.confidenceInput
      ? o.confidenceInput
      : o.evidenceConfidenceFactors
        ? {
            sourceAuthority: o.evidenceConfidenceFactors.sourceAuthority,
            independentSourceCount: Math.round(
              (o.evidenceConfidenceFactors.independentSources || 0) / 20
            ),
            directness: o.evidenceConfidenceFactors.directness,
            recency: o.evidenceConfidenceFactors.recency,
            verifiedFieldRatio:
              (o.evidenceConfidenceFactors.verifiedFieldRatio || 0) / 100,
            firstPartyShare: o.evidenceConfidenceFactors.firstParty,
            completeness: o.evidenceConfidenceFactors.completeness,
            conflictPenalty: o.evidenceConfidenceFactors.conflictPenalty,
          }
        : undefined,
  };
}

/**
 * @param {object[]} opportunities
 * @returns {{ opportunities: object[], enrichment: object, beforeAfter: object[] }}
 */
export function applyQualificationPrecisionPass(opportunities = []) {
  const beforeAfter = [];
  const seen = new Set();
  const out = [];
  const commercialQaApplied = [];

  for (const o of opportunities || []) {
    if (!o?.id || seen.has(o.id)) continue;
    seen.add(o.id);
    const before = {
      id: o.id,
      title: o.title,
      priority: o.priority,
      opportunityType: o.opportunityType || null,
      hotelFitScore: o.hotelFitScore,
      evidenceConfidence: o.evidenceConfidence,
    };

    const qa = applyCommercialQaOverride(toRebuildInput(o));
    let rebuilt = buildOpportunity(qa.opportunity);
    if (qa.applied) {
      rebuilt = finalizeCommercialQaOpportunity(rebuilt, qa.override, qa.forceMaxPriority);
      commercialQaApplied.push({
        id: o.id,
        verdict: qa.override?.verdict || null,
        newPriority: rebuilt.priority,
      });
    }

    beforeAfter.push({
      ...before,
      newPriority: rebuilt.priority,
      newOpportunityType: rebuilt.opportunityType,
      venueSourcingStatus: rebuilt.venueSourcingStatus,
      roomDemandStatus: rebuilt.roomDemandStatus,
      opportunityQualification: rebuilt.opportunityQualification,
      newHotelFitScore: rebuilt.hotelFitScore,
      newEvidenceConfidence: rebuilt.evidenceConfidence,
      contactQuality: rebuilt.contactQuality,
      priorityReason: rebuilt.priorityReason,
      commercialQaApplied: qa.applied,
      changed: before.priority !== rebuilt.priority || !before.opportunityType,
    });
    out.push(rebuilt);
  }

  const count = (p) => out.filter((x) => x.priority === p).length;

  return {
    opportunities: out,
    enrichment: {
      pass: "qualification_precision_v1",
      commercialQaPass: COMMERCIAL_QA_PASS_ID,
      commercialQaApplied,
      mediumContactNotes: MEDIUM_CONTACT_VS_QUALIFICATION_NOTES,
      webhoundSpentUsd: 0,
      webhoundCapUsd: 15,
      note: "Precision re-score + commercial QA High retention check — no broad discovery, no additional Webhound spend.",
      appliedAt: new Date().toISOString(),
      priorityCounts: {
        HIGH_PRIORITY: count(PRIORITY.HIGH),
        MEDIUM_PRIORITY: count(PRIORITY.MEDIUM),
        WATCHLIST: count(PRIORITY.WATCHLIST),
        DISQUALIFIED: count(PRIORITY.DISQUALIFIED),
      },
    },
    beforeAfter,
  };
}
