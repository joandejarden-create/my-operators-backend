/**
 * HIF V7 — Opportunity decision status + current-backward commercial qualification.
 * Distinguishes prospective/active processes from awarded/completed deals.
 * CUSTOMER_BLOCKED. Does not invent decision dates.
 */

import { DECISION_WINDOW_KIND, TIMING_HORIZON } from "./hif-v6-commercial-timing-model-v6.js";
import { COMMERCIAL_PRIORITY } from "./hif-v6-commercial-actionability-v6.js";
import { GEO_CLASS } from "./hif-v6-geographic-confidence-v6.js";
import { RESEARCH_DIRECTION } from "./hif-v6-research-direction-freshness-v6.js";
import { qualifiesForFirst100Milestone } from "./hif-v6-first-100-milestone-v6.js";

export const HIF_V7_OPPORTUNITY_STATUS = "hif-v7-opportunity-status-v7";

export const OPPORTUNITY_STATUS = Object.freeze({
  PROSPECTIVE_DECISION: "PROSPECTIVE_DECISION",
  ACTIVE_SELECTION: "ACTIVE_SELECTION",
  OPEN_PROCESS_VERIFIED: "OPEN_PROCESS_VERIFIED",
  DECISION_PENDING: "DECISION_PENDING",
  DECISION_AWARDED: "DECISION_AWARDED",
  COMPLETED: "COMPLETED",
  SUPERSEDED: "SUPERSEDED",
  STATUS_UNKNOWN: "STATUS_UNKNOWN",
});

export const SIGNAL_CATEGORY = Object.freeze({
  BRAND_CONVERSION: "BRAND_CONVERSION",
  MANAGEMENT_CONTRACT: "MANAGEMENT_CONTRACT",
  COMMERCIAL_CATALYST: "COMMERCIAL_CATALYST",
});

/** Completed / awarded / superseded cannot count as open opportunities. */
export function isOpenOpportunityStatus(status) {
  return [
    OPPORTUNITY_STATUS.PROSPECTIVE_DECISION,
    OPPORTUNITY_STATUS.ACTIVE_SELECTION,
    OPPORTUNITY_STATUS.OPEN_PROCESS_VERIFIED,
    OPPORTUNITY_STATUS.DECISION_PENDING,
  ].includes(status);
}

/**
 * Classify decision status from evidence flags — never invent.
 */
export function classifyOpportunityStatus(row = {}) {
  const blob = [
    row.statusText,
    row.commercialThesis,
    row.evidenceSnippet,
    row.headline,
    ...(row.commercialSignals || []),
    row.decisionStatusHint,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (row.opportunityStatus && OPPORTUNITY_STATUS[row.opportunityStatus]) {
    return { opportunityStatus: row.opportunityStatus, statusBasis: "EXPLICIT_FIELD" };
  }
  if (row.superseded === true || /superseded|amended and restated|replaced by/i.test(blob)) {
    return { opportunityStatus: OPPORTUNITY_STATUS.SUPERSEDED, statusBasis: "SUPERSEDING_EVIDENCE" };
  }
  if (
    /has\s+(?:been\s+)?(?:selected|awarded|appointed)|new\s+(?:operator|manager|brand)\s+(?:is|will be|has been)|signed\s+(?:a\s+)?(?:new\s+)?(?:management|franchise)\s+agreement|reflagged\s+to|now\s+operat(?:es|ing)\s+as/i.test(
      blob,
    ) ||
    row.decisionAwarded === true
  ) {
    return { opportunityStatus: OPPORTUNITY_STATUS.DECISION_AWARDED, statusBasis: "AWARD_LANGUAGE" };
  }
  if (
    /complet(?:ed|ion)|closed\s+the\s+(?:sale|transaction)|transition\s+complete|already\s+(?:converted|reflagged)/i.test(
      blob,
    ) ||
    row.decisionCompleted === true
  ) {
    return { opportunityStatus: OPPORTUNITY_STATUS.COMPLETED, statusBasis: "COMPLETION_LANGUAGE" };
  }
  if (
    /seeking\s+(?:proposals|bids|operator|manager|brand)|rfp\s+(?:for|process)|operator\s+selection\s+(?:process|underway)|brand\s+selection\s+(?:process|underway)|evaluating\s+(?:alternative|new)\s+(?:brands?|operators?|managers?)/i.test(
      blob,
    ) ||
    row.activeSelectionProcess === true
  ) {
    return { opportunityStatus: OPPORTUNITY_STATUS.ACTIVE_SELECTION, statusBasis: "ACTIVE_SELECTION_LANGUAGE" };
  }
  if (
    /will\s+not\s+renew|non[- ]?renewal|terminat(?:es|ed|ion)\s+(?:effective|as of)|franchise\s+(?:will\s+)?end|management\s+agreement\s+(?:will\s+)?(?:end|expire|terminat)/i.test(
      blob,
    ) ||
    row.confirmedNonrenewal === true ||
    row.confirmedTerminationForthcoming === true
  ) {
    return { opportunityStatus: OPPORTUNITY_STATUS.PROSPECTIVE_DECISION, statusBasis: "NONRENEWAL_OR_TERMINATION" };
  }
  if (
    /under\s+review|pending\s+(?:decision|renewal)|owner\s+(?:is\s+)?considering|branding\s+review/i.test(blob) ||
    row.decisionPending === true
  ) {
    return { opportunityStatus: OPPORTUNITY_STATUS.DECISION_PENDING, statusBasis: "PENDING_LANGUAGE" };
  }
  if (row.openProcessVerified === true) {
    return { opportunityStatus: OPPORTUNITY_STATUS.OPEN_PROCESS_VERIFIED, statusBasis: "VERIFIED_OPEN_PROCESS" };
  }
  return { opportunityStatus: OPPORTUNITY_STATUS.STATUS_UNKNOWN, statusBasis: "INSUFFICIENT_STATUS_EVIDENCE" };
}

/**
 * V7 Priority A gate — allows active owner brand review / operator selection
 * without a public notice deadline, but requires current-status verification
 * and open (not awarded/completed) decision status.
 */
export function qualifyPriorityACurrentBackward(row = {}, timing = {}, geo = {}) {
  const status = classifyOpportunityStatus(row);
  const reasons = [];
  const fail = (code) => ({
    qualifiesPriorityA: false,
    commercialPriority: COMMERCIAL_PRIORITY.HOLD,
    ...status,
    reasons: [...reasons, code],
  });

  const windowKindEarly = timing.decisionWindowKind || row.decisionWindowKind;
  const monthsEarly = timing.monthsToDecision ?? row.monthsToDecision;
  const nearTermExpirationProxy =
    windowKindEarly === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY &&
    monthsEarly !== null &&
    monthsEarly !== undefined &&
    monthsEarly >= 0 &&
    monthsEarly < 24;

  if (!isOpenOpportunityStatus(status.opportunityStatus)) {
    if (status.opportunityStatus === OPPORTUNITY_STATUS.DECISION_AWARDED) {
      return {
        qualifiesPriorityA: false,
        commercialPriority: COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE,
        ...status,
        reasons: ["DECISION_ALREADY_AWARDED_NOT_OPEN"],
      };
    }
    if (
      status.opportunityStatus === OPPORTUNITY_STATUS.COMPLETED ||
      status.opportunityStatus === OPPORTUNITY_STATUS.SUPERSEDED
    ) {
      return {
        qualifiesPriorityA: false,
        commercialPriority: COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE,
        ...status,
        reasons: ["COMPLETED_OR_SUPERSEDED_NOT_OPEN"],
      };
    }
    // Near-term expiration-only without open decision status → Priority B research, never A
    if (nearTermExpirationProxy) {
      return {
        qualifiesPriorityA: false,
        commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
        ...status,
        reasons: ["EXPIRATION_ONLY_PROXY_NOT_PRIORITY_A"],
      };
    }
    return fail("STATUS_UNKNOWN_OR_CLOSED");
  }

  const hotelOk = row.hotelName && String(row.hotelName).trim().length >= 5;
  if (!hotelOk) return fail("HOTEL_IDENTITY_MISSING");
  if (/agreement|between operator|international\b|portfolio|maturity|related mortgage/i.test(String(row.hotelName))) {
    return fail("NOISY_HOTEL_IDENTITY");
  }

  const geoClass = geo.geographicClass || row.geographicClass;
  if (geoClass === GEO_CLASS.UNRESOLVED) return fail("PHYSICAL_COUNTRY_UNRESOLVED");
  if (!row.physicalCountry && !row.hotelCountry && !geo.physicalCountry) return fail("PHYSICAL_COUNTRY_MISSING");

  const type = row.opportunityType || row.commercialList || row.signalCategory;
  const bct =
    type === "BRAND_CONVERSION_TARGETS" ||
    type === SIGNAL_CATEGORY.BRAND_CONVERSION ||
    /FRANCHISE|BRAND/i.test(String(row.agreementType || ""));
  const mct =
    type === "MANAGEMENT_CONTRACT_TARGETS" ||
    type === SIGNAL_CATEGORY.MANAGEMENT_CONTRACT ||
    /MANAGE|OPERAT/i.test(String(row.agreementType || ""));
  if (!bct && !mct) return fail("NO_BCT_OR_MCT_RELEVANCE");

  // Catalyst-only (sale/foreclosure) without brand/operator decision link
  if (row.signalCategory === SIGNAL_CATEGORY.COMMERCIAL_CATALYST && !row.linkedBrandOrOperatorDecision) {
    return fail("CATALYST_WITHOUT_BRAND_OR_OPERATOR_DECISION");
  }

  if (!row.currentStatusVerified) return fail("CURRENT_STATUS_NOT_VERIFIED");
  if (row.supersedingEventKnown === true) return fail("SUPERSEDING_EVENT_KNOWN");

  const windowKind = timing.decisionWindowKind || row.decisionWindowKind;
  const horizon = timing.timingHorizon || row.timingHorizon;
  const months = timing.monthsToDecision ?? row.monthsToDecision;
  const activeOk =
    status.opportunityStatus === OPPORTUNITY_STATUS.ACTIVE_SELECTION ||
    status.opportunityStatus === OPPORTUNITY_STATUS.OPEN_PROCESS_VERIFIED ||
    horizon === TIMING_HORIZON.NOW_ACTIVE_PROCESS;
  const within24 =
    activeOk ||
    (months !== null && months !== undefined && months >= 0 && months < 24) ||
    [
      TIMING_HORIZON.M0_6,
      TIMING_HORIZON.M6_12,
      TIMING_HORIZON.M12_24,
      TIMING_HORIZON.NOW_ACTIVE_PROCESS,
    ].includes(horizon);

  if (!within24) return fail("OUTSIDE_24M_AND_NOT_ACTIVE");

  // Credible commercial decision — not expiration-only proxy alone
  if (windowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY && !activeOk) {
    return {
      qualifiesPriorityA: false,
      commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
      ...status,
      reasons: ["EXPIRATION_ONLY_PROXY_NOT_PRIORITY_A"],
    };
  }

  if (
    !activeOk &&
    windowKind !== DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW &&
    windowKind !== DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW &&
    status.opportunityStatus !== OPPORTUNITY_STATUS.PROSPECTIVE_DECISION &&
    status.opportunityStatus !== OPPORTUNITY_STATUS.DECISION_PENDING
  ) {
    return fail("NO_CREDIBLE_DECISION_EVIDENCE");
  }

  if (!row.commercialThesis || String(row.commercialThesis).trim().length < 20) {
    return fail("COMMERCIAL_THESIS_MISSING");
  }
  if (!row.sources || (Array.isArray(row.sources) && row.sources.length === 0)) {
    if (!row.sourceUrl) return fail("SOURCE_CITATIONS_MISSING");
  }

  reasons.push("V7_PRIORITY_A_CURRENT_BACKWARD");
  return {
    qualifiesPriorityA: true,
    commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
    ...status,
    reasons,
    researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
  };
}

/**
 * Build dossier fields for a qualified (or strong B) opportunity.
 */
export function buildOpportunityDossier(row = {}, qualification = {}) {
  return {
    hotelName: row.hotelName,
    physicalLocation: {
      country: row.physicalCountry || row.hotelCountry || null,
      stateProvince: row.stateProvince || null,
      city: row.city || null,
      market: row.market || null,
      geographicClass: row.geographicClass || null,
    },
    currentBrand: row.currentBrand || row.brand || null,
    currentOperator: row.currentOperator || row.manager || null,
    opportunityType: row.opportunityType || (row.commercialList === "MANAGEMENT_CONTRACT_TARGETS" ? "MCT" : "BCT"),
    commercialTrigger: row.commercialTrigger || row.signalCategory || null,
    whyMayBeAvailable: row.commercialThesis || null,
    currentDecisionStatus: qualification.opportunityStatus || row.opportunityStatus || null,
    decisionWindow: {
      kind: row.decisionWindowKind || null,
      date: row.nextCommercialDecisionDate || null,
      monthsToDecision: row.monthsToDecision ?? null,
      horizon: row.timingHorizon || null,
    },
    ownerOrContractingParty: row.ownerOrContractingParty || null,
    originalSource: {
      url: row.sourceUrl || row.sources?.[0]?.url || null,
      publicationDate: row.sourcePublicationDate || row.fileDate || null,
      title: row.sourceTitle || row.headline || null,
    },
    latestCorroboratingEvidence: row.latestEvidence || row.sources?.[1] || null,
    commercialConfidence: qualification.qualifiesPriorityA ? "PRIORITY_A" : row.commercialPriority || "PRIORITY_B",
    outstandingUncertainties: row.uncertainties || [],
    recommendedNextAction: row.recommendedNextAction || null,
    sources: row.sources || (row.sourceUrl ? [{ url: row.sourceUrl }] : []),
    researchDirection: row.researchDirection || RESEARCH_DIRECTION.CURRENT_BACKWARD,
    qualifiesPriorityA: Boolean(qualification.qualifiesPriorityA),
    first100Gate: qualifiesForFirst100Milestone({
      ...row,
      commercialPriority: qualification.commercialPriority || row.commercialPriority,
      decisionWindowKind:
        row.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY && qualification.qualifiesPriorityA
          ? DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW
          : row.decisionWindowKind,
    }),
  };
}
