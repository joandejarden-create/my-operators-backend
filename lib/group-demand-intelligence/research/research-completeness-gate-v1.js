/**
 * Permanent GDI research-completeness gate V1.
 *
 * Separates five states that must never be collapsed:
 *   1. providerCompleted          — Exa/Apify/ORS call finished successfully
 *   2. researchCompleted          — planned commercial questions were investigated
 *   3. commercialEvidenceSufficient — demand + hotel-fit evidence supports a disposition
 *   4. contactPathwaySufficient   — named relevant contact or verified referral route
 *   5. customerReady              — Sales Ready under commercial readiness gate
 *
 * Provider "completed" ≠ research complete ≠ customer-ready.
 * Do not defer solely because the first provider call was weak.
 * Do not promote solely because deep research produced a long report.
 *
 * Zero paid. Shared across PowerShell 1–15, Exa Discovery, Exa Deep Research,
 * Apify, ORS, CIB, local staging, and weekly refresh.
 */

export const RESEARCH_COMPLETENESS_GATE_V1 =
  "gdi-research-completeness-gate-v1";

/**
 * Permanent backlog prevention — weekly runs must track these fields
 * so partially researched opportunities cannot accumulate indefinitely.
 */
export const RESEARCH_COMPLETION_BACKLOG_FIELDS_V1 = Object.freeze([
  "researchAttempted",
  "researchQuestionsResolved",
  "researchDepth",
  "commercialSufficiency",
  "namedContactQuality",
  "remainingBlockers",
  "justifiedContinuation",
  "researchExhaustion",
  "reactivationEligibility",
]);

export const RESEARCH_COMPLETION_STOP_REASONS_V1 = Object.freeze([
  "QUALIFIED_SALES_READY",
  "DISQUALIFIED_NOT_CUSTOMER_READY",
  "RESEARCH_EXHAUSTED",
  "LOW_EXPECTED_COMMERCIAL_VALUE",
  "AWAITING_AUTHORIZED_PAID_RESEARCH",
  "SALES_OUTREACH_NOT_MORE_EXA",
]);

export const RESEARCH_COVERAGE_LEVEL = Object.freeze({
  DEEP_RESEARCH_COMPLETE: "DEEP_RESEARCH_COMPLETE",
  TARGETED_RESEARCH_COMPLETE: "TARGETED_RESEARCH_COMPLETE",
  PARTIAL_RESEARCH: "PARTIAL_RESEARCH",
  DISCOVERY_ONLY: "DISCOVERY_ONLY",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
});

export const RESEARCH_STATE = Object.freeze({
  PROVIDER_COMPLETED: "providerCompleted",
  RESEARCH_COMPLETED: "researchCompleted",
  COMMERCIAL_EVIDENCE_SUFFICIENT: "commercialEvidenceSufficient",
  CONTACT_PATHWAY_SUFFICIENT: "contactPathwaySufficient",
  CUSTOMER_READY: "customerReady",
});

export const COMMERCIAL_RESEARCH_DIMENSIONS = Object.freeze([
  "organizationOrEventIdentity",
  "hotelDemandMechanism",
  "guestroomPotential",
  "meetingEventFnbPotential",
  "hotelSpecificFit",
  "timingAndRecurrence",
  "dateFlexibility",
  "competitiveAccessibility",
  "hotelSourcing",
  "namedPrimaryContact",
  "secondaryOrReferralContact",
  "decisionMakingPathway",
  "supportingEvidence",
  "recommendedSalesAction",
]);

function truthy(v) {
  return v === true || v === "true" || v === 1;
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function namedContactPresent(o = {}) {
  const name =
    o.primaryContact?.name ||
    o.primaryContactName ||
    o.buyerName ||
    o.contactName ||
    "";
  if (!name || /buyer not yet|unknown|tbd|n\/a/i.test(name)) return false;
  return true;
}

/**
 * Assess research completeness independently of provider success.
 *
 * @param {object} input
 * @param {object} [input.opportunity]
 * @param {boolean} [input.providerCompleted]
 * @param {string[]} [input.investigatedDimensions]
 * @param {string[]} [input.openMaterialQuestions]
 * @param {boolean} [input.commercialEvidenceSufficient]
 * @param {boolean} [input.contactPathwaySufficient]
 * @param {boolean} [input.customerReady]
 * @param {boolean} [input.strategiesExhausted]
 * @param {string} [input.coverageLevelHint]
 */
export function assessResearchCompleteness(input = {}) {
  const o = input.opportunity || {};
  const providerCompleted = truthy(
    input.providerCompleted ??
      o.providerCompleted ??
      /completed/i.test(String(o.exaStatus || o.providerStatus || "")),
  );

  const openQs = (input.openMaterialQuestions || o.openMaterialQuestions || [])
    .map(text)
    .filter(Boolean);
  const investigated = new Set(
    (input.investigatedDimensions || o.investigatedDimensions || [])
      .map(text)
      .filter(Boolean),
  );

  const commercialEvidenceSufficient = truthy(
    input.commercialEvidenceSufficient ?? o.commercialEvidenceSufficient,
  );
  const contactPathwaySufficient = truthy(
    input.contactPathwaySufficient ??
      o.contactPathwaySufficient ??
      namedContactPresent(o),
  );
  const customerReady = truthy(input.customerReady ?? o.customerReady);
  const strategiesExhausted = truthy(
    input.strategiesExhausted ?? o.strategiesExhausted,
  );

  const researchCompleted =
    truthy(input.researchCompleted) ||
    (openQs.length === 0 &&
      (commercialEvidenceSufficient || strategiesExhausted));

  let coverageLevel =
    input.coverageLevelHint ||
    o.researchCoverageLevel ||
    null;

  if (!coverageLevel) {
    if (strategiesExhausted && openQs.length > 0) {
      coverageLevel = RESEARCH_COVERAGE_LEVEL.RESEARCH_EXHAUSTED;
    } else if (
      researchCompleted &&
      commercialEvidenceSufficient &&
      investigated.size >= 8
    ) {
      coverageLevel = RESEARCH_COVERAGE_LEVEL.DEEP_RESEARCH_COMPLETE;
    } else if (researchCompleted && openQs.length === 0) {
      coverageLevel = RESEARCH_COVERAGE_LEVEL.TARGETED_RESEARCH_COMPLETE;
    } else if (investigated.size === 0 && !providerCompleted) {
      coverageLevel = RESEARCH_COVERAGE_LEVEL.DISCOVERY_ONLY;
    } else if (openQs.length > 0 || investigated.size < 5) {
      // Provider may have completed; open material questions still mean partial research.
      coverageLevel = RESEARCH_COVERAGE_LEVEL.PARTIAL_RESEARCH;
    } else {
      coverageLevel = RESEARCH_COVERAGE_LEVEL.PARTIAL_RESEARCH;
    }
  }

  return {
    version: RESEARCH_COMPLETENESS_GATE_V1,
    states: {
      [RESEARCH_STATE.PROVIDER_COMPLETED]: providerCompleted,
      [RESEARCH_STATE.RESEARCH_COMPLETED]: Boolean(researchCompleted),
      [RESEARCH_STATE.COMMERCIAL_EVIDENCE_SUFFICIENT]:
        commercialEvidenceSufficient,
      [RESEARCH_STATE.CONTACT_PATHWAY_SUFFICIENT]: contactPathwaySufficient,
      [RESEARCH_STATE.CUSTOMER_READY]: customerReady,
    },
    coverageLevel,
    openMaterialQuestions: openQs,
    investigatedDimensions: [...investigated],
    rules: {
      providerCompletedDoesNotImplyResearchComplete: true,
      researchCompleteDoesNotImplyCustomerReady: true,
      doNotDeferSolelyOnWeakFirstProviderCall: true,
      doNotPromoteSolelyOnLongDeepResearchReport: true,
      noAutomaticPaidWithoutAuthorization: true,
    },
  };
}

/**
 * Weekly loop checklist for existing opportunities (zero-paid by default).
 */
export function buildWeeklyResearchCompletenessChecklist(opportunity = {}) {
  return {
    version: RESEARCH_COMPLETENESS_GATE_V1,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    checks: [
      "Detect unresolved material research gaps",
      "Reuse previous evidence before new retrieval",
      "Detect new event editions",
      "Detect new buyer information",
      "Detect hotel sourcing changes",
      "Reassess date flexibility",
      "Reassess commercial eligibility",
      "Avoid repeated low-value searches",
      "Preserve archive precedence",
      "Do not launch paid providers without founder authorization",
    ],
    assess: assessResearchCompleteness({ opportunity }),
  };
}

export default {
  RESEARCH_COMPLETENESS_GATE_V1,
  RESEARCH_COVERAGE_LEVEL,
  RESEARCH_STATE,
  COMMERCIAL_RESEARCH_DIMENSIONS,
  assessResearchCompleteness,
  buildWeeklyResearchCompletenessChecklist,
};
