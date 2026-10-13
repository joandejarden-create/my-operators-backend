/**
 * Jev EVIDENCE_GAP_NEXT_ACTION — SAFE APPLY for research direction only.
 * Must not invent facts, promote, set WHO, or override grades/status/readiness.
 */

import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, isValidChoice } from "../jev/jev-types.js";
import {
  JEV_NEXT_ACTION,
  defaultActionForBlocker,
  isAllowedJevAction,
} from "./evidence-gap-model-v1.js";

export const EVIDENCE_GAP_NEXT_ACTION = JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION;

/**
 * @returns {{
 *   decisionType, defaultAction, jevAction, appliedAction, applied, agreement,
 *   confidence, rationale, expectedEvidenceType, stopCondition, result
 * }}
 */
export async function decideEvidenceGapNextAction({
  packet = {},
  primaryBlocker = null,
  hotelShort = "",
  enableSafeApply = true,
} = {}) {
  const existing = defaultActionForBlocker(primaryBlocker, hotelShort);

  const result = await decide({
    decisionType: JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION,
    context: {
      hotel: packet.hotel || null,
      market: packet.market || null,
      eventProgram: packet.event || null,
      futureCycle: packet.futureCycle || null,
      surfaceType: packet.surfaceType || null,
      lodgingRelationship: packet.lodgingRelationship || null,
      commercialStatus: packet.commercialStatus || null,
      winnability: packet.winnability || null,
      resolvedDimensions: packet.resolvedDimensions || [],
      unresolvedDimensions: packet.unresolvedDimensions || [],
      primaryBlocker,
      secondaryBlocker: packet.secondaryBlocker || null,
      previousResearchActions: packet.previousResearchActions || [],
      previousFailedPaths: packet.previousFailedPaths || [],
      sourceUrls: (packet.sourceUrls || []).slice(0, 5),
      missingFields: packet.unresolvedDimensions || [],
      decisionIntent: EVIDENCE_GAP_NEXT_ACTION,
      researchDirectionOnly: true,
      noInventFacts: true,
      noPromote: true,
      noFinalWho: true,
    },
    existingDecision: existing,
    policyContext: {
      noInventPerson: true,
      noInventOpportunity: true,
      researchDirectionOnly: true,
      noMutateFacts: true,
      noPromote: true,
      noFinalWho: true,
    },
    fallbackDecision: existing,
  });

  const selectedRaw = result.selected || result.effectiveDecision || existing;
  const selected = isValidChoice(JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION, selectedRaw)
    ? selectedRaw
    : isAllowedJevAction(selectedRaw)
      ? selectedRaw
      : existing;

  const applied =
    enableSafeApply &&
    !result.technicalFallback &&
    !result.policyFallback &&
    selected !== existing;

  const appliedAction = enableSafeApply && !result.technicalFallback ? selected : existing;

  // Extract optional metadata from Jev payload if present (never treat as facts)
  const meta = result.rawResponse?.metadata || result.metadata || {};
  return {
    decisionType: EVIDENCE_GAP_NEXT_ACTION,
    defaultAction: existing,
    jevAction: selected,
    appliedAction,
    applied,
    agreement: selected === existing,
    confidence: result.confidence,
    rationale: String(meta.rationale || result.rationale || "research_direction_only").slice(0, 500),
    expectedEvidenceType: String(
      meta.expectedEvidenceType || "PUBLIC_PAGE_OR_OFFICIAL_DOC"
    ).slice(0, 120),
    stopCondition: String(
      meta.stopCondition || "STOP_AFTER_BOUNDED_QUERIES_OR_BLOCKER_RESOLVED"
    ).slice(0, 200),
    result,
  };
}

export { JEV_NEXT_ACTION };
