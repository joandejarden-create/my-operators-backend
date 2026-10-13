/**
 * Jev V3 — EXHIBITOR_TEAM_RESEARCH_PATH + LODGING_EVIDENCE_NEXT_STEP
 * SAFE APPLY research direction only.
 */

import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, isValidChoice } from "../jev/jev-types.js";
import {
  EXHIBITOR_TEAM_RESEARCH_PATH_CHOICES,
  LODGING_EVIDENCE_NEXT_STEP_CHOICES,
} from "./v3-states.js";

export function defaultExhibitorTeamPath({
  hasEventPage = false,
  hasTeam = false,
  hasWho = false,
  travelGap = true,
} = {}) {
  if (hasTeam && hasWho && !travelGap) return "STOP_SUFFICIENT";
  if (!hasEventPage) return "COMPANY_EVENT_PAGE";
  if (!hasTeam) return "SPEAKER_ROSTER";
  if (travelGap) return "TRAVEL_EVIDENCE";
  if (!hasWho) return "FUNCTIONAL_CONTACT";
  return "STOP_SUFFICIENT";
}

export function defaultLodgingNextStep({
  lodgingProof = "UNKNOWN",
  hasHousing = false,
  hasTeam = false,
} = {}) {
  if (lodgingProof === "CONFIRMED" || lodgingProof === "STRONG_INFERENCE") {
    return "STOP_SUFFICIENT";
  }
  if (!hasHousing) return "HOUSING_SOURCE";
  if (!hasTeam) return "TEAM_ROSTER";
  if (lodgingProof === "PLAUSIBLE") return "STOP_PLAUSIBLE_ONLY";
  if (lodgingProof === "WEAK" || lodgingProof === "UNKNOWN") return "COMPANY_TRAVEL_PAGE";
  return "STOP_LOW_VALUE";
}

export async function decideExhibitorTeamResearchPath(ctx = {}) {
  const defaultPath = defaultExhibitorTeamPath(ctx);
  const result = await decide({
    decisionType: JEV_DECISION_TYPE.EXHIBITOR_TEAM_RESEARCH_PATH,
    context: {
      company: ctx.company,
      eventRelationship: ctx.eventRelationship,
      origin: ctx.origin,
      participationIntensity: ctx.participationIntensity,
      knownEmployees: ctx.knownEmployees || 0,
      travelEvidence: ctx.travelEvidence || "unknown",
      missingFields: ctx.gaps || [],
      remainingBudget: ctx.remainingBudget,
      decisionIntent: "EXHIBITOR_TEAM_RESEARCH_PATH",
    },
    existingDecision: defaultPath,
    policyContext: {
      noInventPerson: true,
      noInventOpportunity: true,
      researchDirectionOnly: true,
    },
    fallbackDecision: defaultPath,
  });

  const selected = result.selected || result.effectiveDecision || defaultPath;
  const jevPath = EXHIBITOR_TEAM_RESEARCH_PATH_CHOICES.includes(selected)
    ? selected
    : isValidChoice(JEV_DECISION_TYPE.EXHIBITOR_TEAM_RESEARCH_PATH, selected)
      ? selected
      : defaultPath;
  const applied =
    ctx.enableSafeApply !== false &&
    !result.technicalFallback &&
    !result.policyFallback &&
    jevPath !== defaultPath &&
    !String(jevPath).startsWith("STOP");

  return {
    decisionType: "EXHIBITOR_TEAM_RESEARCH_PATH",
    defaultPath,
    jevPath,
    finalPath: applied ? jevPath : defaultPath,
    applied,
    agreement: jevPath === defaultPath,
    confidence: result.confidence,
    result,
  };
}

export async function decideLodgingEvidenceNextStep(ctx = {}) {
  const defaultPath = defaultLodgingNextStep(ctx);
  const result = await decide({
    decisionType: JEV_DECISION_TYPE.LODGING_EVIDENCE_NEXT_STEP,
    context: {
      teamEvidence: ctx.teamEvidence,
      travelEvidence: ctx.travelEvidence,
      companyOrigin: ctx.companyOrigin,
      eventDuration: ctx.eventDuration,
      housingEvidence: ctx.housingEvidence || "unknown",
      sourcesChecked: ctx.sourcesChecked || [],
      missingFields: ["lodgingEvidence"],
      decisionIntent: "LODGING_EVIDENCE_NEXT_STEP",
    },
    existingDecision: defaultPath,
    policyContext: {
      noInventPerson: true,
      noInventOpportunity: true,
      researchDirectionOnly: true,
    },
    fallbackDecision: defaultPath,
  });

  const selected = result.selected || result.effectiveDecision || defaultPath;
  const jevPath = LODGING_EVIDENCE_NEXT_STEP_CHOICES.includes(selected)
    ? selected
    : defaultPath;
  const applied =
    ctx.enableSafeApply !== false &&
    !result.technicalFallback &&
    !result.policyFallback &&
    jevPath !== defaultPath &&
    !String(jevPath).startsWith("STOP");

  return {
    decisionType: "LODGING_EVIDENCE_NEXT_STEP",
    defaultPath,
    jevPath,
    finalPath: applied ? jevPath : defaultPath,
    applied,
    agreement: jevPath === defaultPath,
    confidence: result.confidence,
    result,
  };
}
