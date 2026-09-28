/**
 * Jev STRUCTURED_SOURCE_PRIORITY — SAFE APPLY for next structured source type.
 * Research direction only — does not create customer facts.
 */

import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../jev/jev-types.js";
import { STRUCTURED_SOURCE_CHOICES, SOURCE_TYPE } from "./v2-constants.js";

export const STRUCTURED_SOURCE_PRIORITY = "STRUCTURED_SOURCE_PRIORITY";

/**
 * Default heuristic when Jev unavailable / agrees with default.
 */
export function defaultStructuredSourcePriority({
  demandFamily = null,
  sourcesChecked = [],
  lodgingGap = true,
  contactGap = true,
} = {}) {
  const checked = new Set(sourcesChecked);
  const family = String(demandFamily || "");

  const prefer = [];
  if (/EXHIBITOR|FASHION|CORPORATE/i.test(family)) {
    prefer.push(
      SOURCE_TYPE.EXHIBITOR_DIRECTORY,
      SOURCE_TYPE.SPONSOR_DIRECTORY,
      SOURCE_TYPE.PROGRAM_PDF,
      SOURCE_TYPE.HOUSING_PDF
    );
  } else if (/ASSOCIATION|MEDICAL/i.test(family)) {
    prefer.push(
      SOURCE_TYPE.PROGRAM_PDF,
      SOURCE_TYPE.PARTICIPANT_LIST,
      SOURCE_TYPE.STAFF_DIRECTORY,
      SOURCE_TYPE.HOUSING_PDF
    );
  } else if (/TOUR/i.test(family)) {
    prefer.push(SOURCE_TYPE.TOUR_SCHEDULE, SOURCE_TYPE.PROGRAM_PAGE, SOURCE_TYPE.NEWS_RELEASE);
  } else if (/EDUCATION/i.test(family)) {
    prefer.push(SOURCE_TYPE.TEAM_SCHEDULE, SOURCE_TYPE.PROGRAM_PAGE, SOURCE_TYPE.PROGRAM_PDF);
  } else if (/SPORTS/i.test(family)) {
    prefer.push(SOURCE_TYPE.TEAM_SCHEDULE, SOURCE_TYPE.PARTICIPANT_LIST, SOURCE_TYPE.HOUSING_PDF);
  } else if (/AGENCY|PRODUCTION/i.test(family)) {
    prefer.push(SOURCE_TYPE.NEWS_RELEASE, SOURCE_TYPE.PROGRAM_PAGE, SOURCE_TYPE.SPONSOR_DIRECTORY);
  } else {
    prefer.push(
      SOURCE_TYPE.EXHIBITOR_DIRECTORY,
      SOURCE_TYPE.PROGRAM_PDF,
      SOURCE_TYPE.HOUSING_PDF,
      SOURCE_TYPE.TOUR_SCHEDULE
    );
  }

  if (lodgingGap && !checked.has(SOURCE_TYPE.HOUSING_PDF)) {
    prefer.unshift(SOURCE_TYPE.HOUSING_PDF);
  }
  if (contactGap && !checked.has(SOURCE_TYPE.STAFF_DIRECTORY)) {
    prefer.push(SOURCE_TYPE.STAFF_DIRECTORY);
  }

  for (const p of prefer) {
    if (!checked.has(p)) return p;
  }
  return "STOP";
}

/**
 * @returns {{ decisionType, defaultPath, jevPath, finalPath, applied, agreement, confidence, result }}
 */
export async function decideStructuredSourcePriority({
  demandFamily = null,
  sourcesChecked = [],
  sourceCandidates = [],
  lodgingGap = true,
  contactGap = true,
  remainingBudget = 50,
  enableSafeApply = true,
} = {}) {
  const defaultPath = defaultStructuredSourcePriority({
    demandFamily,
    sourcesChecked,
    lodgingGap,
    contactGap,
  });

  const result = await decide({
    decisionType: JEV_DECISION_TYPE.STRUCTURED_SOURCE_PRIORITY,
    context: {
      demandFamily,
      sourcesChecked,
      sourceCandidateTypes: sourceCandidates.map((s) => s.sourceType || s).slice(0, 12),
      lodgingEvidence: lodgingGap ? "gap" : "present",
      missingFields: [
        lodgingGap ? "lodgingEvidence" : null,
        contactGap ? "contactPath" : null,
      ].filter(Boolean),
      remainingBudget,
      decisionIntent: STRUCTURED_SOURCE_PRIORITY,
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
  const jevPath = isValidChoice(JEV_DECISION_TYPE.STRUCTURED_SOURCE_PRIORITY, selected)
    ? selected
    : STRUCTURED_SOURCE_CHOICES.includes(selected)
      ? selected
      : defaultPath;

  const applied =
    enableSafeApply &&
    !result.technicalFallback &&
    !result.policyFallback &&
    jevPath !== defaultPath &&
    jevPath !== "STOP";

  return {
    decisionType: STRUCTURED_SOURCE_PRIORITY,
    defaultPath,
    jevPath,
    finalPath: applied ? jevPath : defaultPath,
    applied,
    agreement: jevPath === defaultPath,
    confidence: result.confidence,
    result,
  };
}
