/**
 * Jev SOURCE_VALIDATION_NEXT_STEP — SAFE APPLY before fetch/extract.
 */

import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, isValidChoice } from "../jev/jev-types.js";
import { SOURCE_VALIDATION_NEXT_STEP_CHOICES, SOURCE_STATUS } from "./v4-constants.js";
import { SOURCE_TYPE } from "./v2-constants.js";

export function defaultSourceValidationNextStep(candidate = {}) {
  if (candidate.status === SOURCE_STATUS.WRONG_GEOGRAPHY) return "REJECT_WRONG_GEO";
  if (
    candidate.status === SOURCE_STATUS.UI_CHROME ||
    candidate.status === SOURCE_STATUS.GENERIC_CALENDAR ||
    candidate.status === SOURCE_STATUS.IRRELEVANT ||
    candidate.richness === "NONE" ||
    candidate.richness === "LOW"
  ) {
    return "REJECT_LOW_RICHNESS";
  }
  if (candidate.status === SOURCE_STATUS.STALE) return "STOP";
  if (candidate.status === SOURCE_STATUS.PDF_NOISE) return "REJECT_LOW_RICHNESS";

  if (candidate.requiresPDF || String(candidate.sourceType || "").includes("PDF")) {
    return "FETCH_PDF";
  }
  if (candidate.requiresRender) return "FETCH_RENDERED";

  if (
    candidate.sourceType === SOURCE_TYPE.GENERIC_SERP ||
    candidate.sourceType === SOURCE_TYPE.PROGRAM_PAGE
  ) {
    if (/hous|hotel.?block|room.?block/i.test(`${candidate.title} ${candidate.snippet}`)) {
      return "FIND_HOUSING";
    }
    return "FIND_DIRECTORY";
  }

  if (
    candidate.status === SOURCE_STATUS.VALID_NYC_STRUCTURED ||
    candidate.status === SOURCE_STATUS.VALID_NYC_UNSTRUCTURED
  ) {
    return "FETCH_STATIC";
  }

  return "STOP";
}

export async function decideSourceValidationNextStep(ctx = {}) {
  const candidate = ctx.candidate || {};
  const defaultPath = defaultSourceValidationNextStep(candidate);

  const result = await decide({
    decisionType: JEV_DECISION_TYPE.SOURCE_VALIDATION_NEXT_STEP,
    context: {
      sourceUrl: candidate.sourceURL,
      sourceType: candidate.sourceType,
      sourceStatus: candidate.status,
      geography: candidate.geography,
      richness: candidate.richness,
      title: candidate.title,
      snippet: (candidate.snippet || "").slice(0, 240),
      family: candidate.family,
      remainingBudget: ctx.remainingBudget,
      decisionIntent: "SOURCE_VALIDATION_NEXT_STEP",
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
  const jevPath = SOURCE_VALIDATION_NEXT_STEP_CHOICES.includes(selected)
    ? selected
    : isValidChoice(JEV_DECISION_TYPE.SOURCE_VALIDATION_NEXT_STEP, selected)
      ? selected
      : defaultPath;

  const applied =
    ctx.enableSafeApply !== false &&
    !result.technicalFallback &&
    !result.policyFallback &&
    jevPath !== defaultPath &&
    !String(jevPath).startsWith("STOP") &&
    !String(jevPath).startsWith("REJECT");

  // Rejects from Jev that differ from default still count as applied for waste reduction
  const rejectApplied =
    ctx.enableSafeApply !== false &&
    !result.technicalFallback &&
    !result.policyFallback &&
    jevPath !== defaultPath &&
    (String(jevPath).startsWith("REJECT") || jevPath === "STOP");

  return {
    decisionType: "SOURCE_VALIDATION_NEXT_STEP",
    defaultPath,
    jevPath,
    finalPath: applied || rejectApplied ? jevPath : defaultPath,
    applied: applied || rejectApplied,
    agreement: jevPath === defaultPath,
    confidence: result.confidence,
    result,
  };
}
