/**
 * GDI Buyer Path Resolution V2
 *
 * Focused commercial contact-path research on existing QUALIFIED opportunities.
 * Does NOT broaden discovery. Does NOT lower ACTIONABLE thresholds.
 */

import {
  confirmOpportunityV1,
  computeQualifiedToActionableConversion,
  CONFIRMATION_BLOCKER,
  GDI_MATURITY_STATE,
  PARTICIPATION_STATUS,
} from "./opportunity-confirmation-v1.js";
import { LODGING_CONTROL_HYPOTHESIS } from "../gdi-maturity-v1.js";

export const BUYER_PATH_RESOLUTION_ENGINE_ID = "gdi_buyer_path_resolution_v2";

export const BUYER_PATH_CLASS_V2 = Object.freeze({
  NAMED_BUYER_PERSON: "NAMED_BUYER_PERSON",
  NAMED_BUYER_ROLE_PATH: "NAMED_BUYER_ROLE_PATH",
  RELEVANT_FUNCTION_CONTACT: "RELEVANT_FUNCTION_CONTACT",
  VERIFIED_AGENCY_PATH: "VERIFIED_AGENCY_PATH",
  VERIFIED_TMC_PATH: "VERIFIED_TMC_PATH",
  VERIFIED_PCO_DMC_PATH: "VERIFIED_PCO_DMC_PATH",
  PRESS_CONTACT_ONLY: "PRESS_CONTACT_ONLY",
  GENERIC_CONTACT_ONLY: "GENERIC_CONTACT_ONLY",
  SOURCE_PAGE_ONLY: "SOURCE_PAGE_ONLY",
  NO_PATH_FOUND: "NO_PATH_FOUND",
});

export const BUYER_PATH_RESOLUTION_BLOCKER = Object.freeze({
  NO_NAMED_BUYER_PERSON: "NO_NAMED_BUYER_PERSON",
  NO_BUYER_ROLE_PATH: "NO_BUYER_ROLE_PATH",
  NO_RELEVANT_FUNCTION: "NO_RELEVANT_FUNCTION",
  NO_AGENCY_PATH: "NO_AGENCY_PATH",
  NO_TMC_PATH: "NO_TMC_PATH",
  NO_LODGING_CONTROL_PATH: "NO_LODGING_CONTROL_PATH",
  PRESS_CONTACT_ONLY: "PRESS_CONTACT_ONLY",
  SOURCE_PAGE_ONLY: "SOURCE_PAGE_ONLY",
  GENERIC_CONTACT_ONLY: "GENERIC_CONTACT_ONLY",
  WHY_NOW_TOO_WEAK: "WHY_NOW_TOO_WEAK",
  READY_GATE_FAILED: "READY_GATE_FAILED",
  ...CONFIRMATION_BLOCKER,
});

export const CONTACTABILITY = Object.freeze({
  CONTACTABLE_NOW: "CONTACTABLE_NOW",
  CONTACTABLE_WITH_ROLE: "CONTACTABLE_WITH_ROLE",
  NEEDS_ENRICHMENT: "NEEDS_ENRICHMENT",
  NO_CREDIBLE_PATH: "NO_CREDIBLE_PATH",
});

const RESOLVED_PATH_CLASSES = new Set([
  BUYER_PATH_CLASS_V2.NAMED_BUYER_PERSON,
  BUYER_PATH_CLASS_V2.NAMED_BUYER_ROLE_PATH,
  BUYER_PATH_CLASS_V2.RELEVANT_FUNCTION_CONTACT,
  BUYER_PATH_CLASS_V2.VERIFIED_AGENCY_PATH,
  BUYER_PATH_CLASS_V2.VERIFIED_TMC_PATH,
  BUYER_PATH_CLASS_V2.VERIFIED_PCO_DMC_PATH,
]);

/**
 * Map V2 buyer-path class into confirmation findings.buyerPath shape.
 */
export function buyerPathV2ToConfirmationFindings(path = {}) {
  const cls = path.buyerPathClass || BUYER_PATH_CLASS_V2.NO_PATH_FOUND;
  const pressOnly = cls === BUYER_PATH_CLASS_V2.PRESS_CONTACT_ONLY;
  const tooGeneric =
    pressOnly ||
    cls === BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY ||
    cls === BUYER_PATH_CLASS_V2.SOURCE_PAGE_ONLY ||
    cls === BUYER_PATH_CLASS_V2.NO_PATH_FOUND;

  // Canonical Ready taxonomy classes (subset)
  let readyClass = "SOURCE_PAGE";
  if (cls === BUYER_PATH_CLASS_V2.NAMED_BUYER_PERSON) readyClass = "NAMED_BUYER_PERSON";
  else if (cls === BUYER_PATH_CLASS_V2.NAMED_BUYER_ROLE_PATH) readyClass = "NAMED_BUYER_ROLE_PATH";
  else if (
    cls === BUYER_PATH_CLASS_V2.RELEVANT_FUNCTION_CONTACT ||
    cls === BUYER_PATH_CLASS_V2.VERIFIED_AGENCY_PATH ||
    cls === BUYER_PATH_CLASS_V2.VERIFIED_TMC_PATH ||
    cls === BUYER_PATH_CLASS_V2.VERIFIED_PCO_DMC_PATH
  ) {
    readyClass = "RELEVANT_FUNCTION_CONTACT";
  } else if (cls === BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY) {
    readyClass = "GENERAL_ORG_CONTACT";
  }

  return {
    publicContactPath: path.buyerPathSourceUrl || null,
    buyerEntity: path.buyerEntity || null,
    buyerRole: path.buyerRole || path.namedRole || null,
    buyerContactPathClass: readyClass,
    pressContactOnly: pressOnly,
    tooGeneric,
    namedBuyerPerson: path.namedPerson
      ? {
          name: path.namedPerson.name,
          role: path.namedPerson.role || path.buyerRole || null,
          email: null, // never invent emails
          sourceUrl: path.namedPerson.sourceUrl || path.buyerPathSourceUrl || null,
        }
      : null,
    evidenceItems: path.evidenceItems || [],
    buyerPathConfidence: path.buyerPathConfidence || "LOW",
    buyerPathRationale: path.buyerPathRationale || null,
    buyerPathSourceType: path.buyerPathSourceType || null,
    contactability: path.contactability || CONTACTABILITY.NO_CREDIBLE_PATH,
  };
}

function deepestBlocker(path = {}, lodging = {}, ready = {}, promoted = false) {
  if (promoted) return null;
  const cls = path.buyerPathClass || BUYER_PATH_CLASS_V2.NO_PATH_FOUND;
  if (cls === BUYER_PATH_CLASS_V2.PRESS_CONTACT_ONLY) {
    return BUYER_PATH_RESOLUTION_BLOCKER.PRESS_CONTACT_ONLY;
  }
  if (cls === BUYER_PATH_CLASS_V2.SOURCE_PAGE_ONLY) {
    return BUYER_PATH_RESOLUTION_BLOCKER.SOURCE_PAGE_ONLY;
  }
  if (cls === BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY) {
    return BUYER_PATH_RESOLUTION_BLOCKER.GENERIC_CONTACT_ONLY;
  }
  if (cls === BUYER_PATH_CLASS_V2.NO_PATH_FOUND) {
    return BUYER_PATH_RESOLUTION_BLOCKER.NO_RELEVANT_FUNCTION;
  }
  if (
    !lodging.lodgingControlHypothesis ||
    lodging.lodgingControlHypothesis === LODGING_CONTROL_HYPOTHESIS.UNKNOWN
  ) {
    return BUYER_PATH_RESOLUTION_BLOCKER.NO_LODGING_CONTROL_PATH;
  }
  if (!ready.ok) return BUYER_PATH_RESOLUTION_BLOCKER.READY_GATE_FAILED;
  if (RESOLVED_PATH_CLASSES.has(cls) && !promoted) {
    return BUYER_PATH_RESOLUTION_BLOCKER.READY_GATE_FAILED;
  }
  return BUYER_PATH_RESOLUTION_BLOCKER.NO_BUYER_ROLE_PATH;
}

/**
 * Resolve buyer path for one QUALIFIED opportunity using a research pack.
 */
export function resolveBuyerPathV2({
  hotel = null,
  opportunity = {},
  pathFindings = {},
  confirmationBase = {},
  confirmationOptions = {},
} = {}) {
  const path = pathFindings.buyerPath || {};
  const lodging = pathFindings.lodgingControl || {};
  const participation = pathFindings.participation || confirmationBase.participation || {};
  const travelingCohort =
    pathFindings.travelingCohort || confirmationBase.travelingCohort || {};
  const recurrence = pathFindings.recurrence || confirmationBase.recurrence || {};
  const whyNow = pathFindings.whyNow || confirmationBase.whyNow || {};
  const recommendedAction =
    pathFindings.recommendedAction || confirmationBase.recommendedAction || {};

  const findings = {
    ...confirmationBase,
    ...pathFindings,
    participation,
    travelingCohort,
    recurrence,
    lodgingControl: lodging,
    buyerPath: buyerPathV2ToConfirmationFindings(path),
    whyNow,
    recommendedAction,
    lodgingVerified: pathFindings.lodgingVerified === true ? true : false,
    notes: pathFindings.notes || path.buyerPathRationale || null,
  };

  const result = confirmOpportunityV1({
    hotel,
    opportunity,
    findings,
    confirmationOptions,
  });

  const deepest = deepestBlocker(path, lodging, result.readyGate, result.promoted);
  const blockers = [...new Set([...(result.blockers || []), deepest].filter(Boolean))];

  return {
    ...result,
    engineId: BUYER_PATH_RESOLUTION_ENGINE_ID,
    buyerPathClassV2: path.buyerPathClass || BUYER_PATH_CLASS_V2.NO_PATH_FOUND,
    buyerPathConfidence: path.buyerPathConfidence || "LOW",
    buyerPathRationale: path.buyerPathRationale || null,
    buyerPathSourceUrl: path.buyerPathSourceUrl || null,
    buyerPathSourceType: path.buyerPathSourceType || null,
    namedPerson: path.namedPerson || null,
    namedRole: path.namedRole || path.buyerRole || null,
    contactability: path.contactability || CONTACTABILITY.NO_CREDIBLE_PATH,
    deepestBlocker: deepest,
    blockers,
    lodgingControlConfirmed: lodging.lodgingControlConfirmed === true,
    lodgingControlInferred: lodging.lodgingControlConfirmed !== true,
  };
}

export function resolveBuyerPathBatchV2({
  opportunities = [],
  findingsByKey = {},
  confirmationBaseByKey = {},
  hotel = null,
  confirmationOptions = {},
} = {}) {
  const results = [];
  for (const opp of opportunities) {
    const key = opp.organizationName || opp.company || opp.id || "";
    results.push(
      resolveBuyerPathV2({
        hotel,
        opportunity: opp,
        pathFindings: findingsByKey[key] || {},
        confirmationBase: confirmationBaseByKey[key] || {},
        confirmationOptions,
      })
    );
  }
  const kpi = computeQualifiedToActionableConversion(results);
  const resolved = results.filter((r) =>
    RESOLVED_PATH_CLASSES.has(r.buyerPathClassV2)
  ).length;
  const buyerPathResolutionRate =
    results.length === 0 ? 0 : Math.round((1000 * resolved) / results.length) / 10;
  return {
    results,
    kpi,
    buyerPathResolutionRate,
    resolvedCount: resolved,
    researchedCount: results.length,
  };
}

export function isResolvedBuyerPathClass(cls) {
  return RESOLVED_PATH_CLASSES.has(cls);
}

export {
  GDI_MATURITY_STATE,
  PARTICIPATION_STATUS,
  LODGING_CONTROL_HYPOTHESIS,
  computeQualifiedToActionableConversion,
};
