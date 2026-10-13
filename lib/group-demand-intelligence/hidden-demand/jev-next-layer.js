/**
 * Jev HIDDEN_DEMAND_NEXT_LAYER — SAFE APPLY for research direction only.
 * Must not invent opportunities, people, or lodging facts.
 */

import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../jev/jev-types.js";
import { DEMAND_FAMILY } from "./constants.js";

export const HIDDEN_DEMAND_NEXT_LAYER = JEV_DECISION_TYPE.HIDDEN_DEMAND_NEXT_LAYER;

export const HIDDEN_LAYER_CHOICES = CHOICES.HIDDEN_DEMAND_NEXT_LAYER;

const LAYER_TO_FAMILY = Object.freeze({
  EXHIBITOR: DEMAND_FAMILY.EXHIBITOR_VENDOR,
  VENDOR: DEMAND_FAMILY.EXHIBITOR_VENDOR,
  AGENCY: DEMAND_FAMILY.AGENCY,
  CREW: DEMAND_FAMILY.PRODUCTION_CREW,
  CORPORATE_TEAM: DEMAND_FAMILY.CORPORATE_PROJECT,
  DELEGATION: DEMAND_FAMILY.DELEGATION,
  TOUR_OPERATOR: DEMAND_FAMILY.TOUR_SERIES,
  EDUCATION: DEMAND_FAMILY.EDUCATION,
  SPORTS_ADJACENT: DEMAND_FAMILY.SPORTS_ADJACENT,
  SOCIAL: DEMAND_FAMILY.SOCIAL,
  ASSOCIATION_SUBGROUP: DEMAND_FAMILY.ASSOCIATION_SUBGROUP,
  STOP: null,
});

/**
 * @returns {{ decisionType, defaultLayer, jevLayer, family, applied, agreement, confidence, result }}
 */
export async function decideHiddenDemandNextLayer({
  demandGenerator = null,
  knownEntities = [],
  currentFamily = null,
  defaultLayer = "EXHIBITOR",
  enableSafeApply = true,
} = {}) {
  const existing = HIDDEN_LAYER_CHOICES.includes(defaultLayer) ? defaultLayer : "EXHIBITOR";

  const result = await decide({
    decisionType: JEV_DECISION_TYPE.HIDDEN_DEMAND_NEXT_LAYER,
    context: {
      demandGeneratorName: demandGenerator?.name || null,
      knownEntityCount: knownEntities.length,
      currentFamily,
      missingFields: ["hiddenDemandLayer", "lodgingEvidence"],
      lodgingEvidence: "unknown",
      decisionIntent: HIDDEN_DEMAND_NEXT_LAYER,
    },
    existingDecision: existing,
    policyContext: {
      noInventPerson: true,
      noInventOpportunity: true,
      researchDirectionOnly: true,
    },
    fallbackDecision: existing,
  });

  const selected = result.selected || result.effectiveDecision || existing;
  const layer = isValidChoice(JEV_DECISION_TYPE.HIDDEN_DEMAND_NEXT_LAYER, selected)
    ? selected
    : existing;
  const applied =
    enableSafeApply &&
    !result.technicalFallback &&
    !result.policyFallback &&
    layer !== existing;

  return {
    decisionType: HIDDEN_DEMAND_NEXT_LAYER,
    defaultLayer: existing,
    jevLayer: layer,
    family: LAYER_TO_FAMILY[layer] || currentFamily,
    applied,
    agreement: layer === existing,
    confidence: result.confidence,
    result,
  };
}
