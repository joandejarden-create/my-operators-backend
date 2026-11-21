/**
 * HIF V6 — Appraisal & underwriting source policy (founder-locked).
 *
 * DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER
 * - No dedicated appraisal discovery adapter in V6.
 * - No paid appraisal research from the 50-doc free preflight.
 * - Route SOURCE_PRIORITY_POINTER docs into Franchise / Management queues.
 * - Preserve incidental appraisal evidence; never treat valuation assumptions
 *   as verified contractual terms.
 */

export const HIF_APPRAISAL_POLICY_V6 = "hif-appraisal-policy-v6";

export const APPRAISAL_ADAPTER_DECISION = Object.freeze({
  decision: "DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER",
  lockedAt: "2026-11-21",
  founderAccepted: true,
  pilotDocumentsEvaluated: 50,
  independentlyVerifiedNewContractualDates: 0,
  newlyQualifiedCommercialOpportunities: 0,
  usefulSourcePriorityPointers: 22,
  meteredSpendUsd: 0,
  buildStandaloneAppraisalAdapter: false,
  authorizePaidAppraisalResearch: false,
  retainIncidentalAppraisalEvidence: true,
  valuationAssumptionsAreNotContractualTerms: true,
  preferredSources: Object.freeze([
    "SEC_CMBS",
    "EX10",
    "INTERNATIONAL_REGULATORY_FILINGS",
    "RECORDED_MANAGEMENT_MEMORANDA",
    "COURT_EXHIBITS",
    "VERIFIED_OWNERSHIP_RECORDS",
  ]),
  commercialListsOnly: Object.freeze([
    "BRAND_CONVERSION_TARGETS",
    "MANAGEMENT_CONTRACT_TARGETS",
  ]),
  successCriterion:
    "More independently verified hotel-specific Franchise and Management Contract opportunities—not more appraisal documents.",
});

export const APPRAISAL_EVIDENCE_CLASS = Object.freeze({
  SOURCE_PRIORITY_POINTER: "SOURCE_PRIORITY_POINTER",
  APPRAISAL_ASSUMPTION: "APPRAISAL_ASSUMPTION",
  INCIDENTAL_APPRAISAL_EVIDENCE: "INCIDENTAL_APPRAISAL_EVIDENCE",
  CONTRACTUAL_FACT_FROM_UNDERWRITING_ANNEX: "CONTRACTUAL_FACT_FROM_UNDERWRITING_ANNEX",
  FINANCING_DATE_NOT_CONTRACT: "FINANCING_DATE_NOT_CONTRACT",
});

/**
 * Route a pilot/document row into Franchise and/or Management discovery queues.
 * Pointers never become verified expirations by themselves.
 */
export function routeAppraisalPilotDocToQueues(doc = {}) {
  const factClasses = [
    ...(doc.score?.factClasses || []),
    ...(doc.signals?.factClasses || []),
  ];
  const isPointer =
    doc.score?.pointerOnly === true ||
    factClasses.includes(APPRAISAL_EVIDENCE_CLASS.SOURCE_PRIORITY_POINTER);
  const isAssumption =
    doc.score?.assumptionOnly === true ||
    factClasses.includes(APPRAISAL_EVIDENCE_CLASS.APPRAISAL_ASSUMPTION) ||
    (doc.signals?.appraisalAssumptions || []).length > 0;

  const franchiseSignals =
    Boolean(doc.score?.hasFranchiseExpirationLanguage) ||
    (doc.signals?.franchiseExpirationMentions || []).length > 0 ||
    /franchise/i.test(String(doc.issuerHint || "")) ||
    /franchise/i.test(String(doc.sourceClass || ""));

  const managementSignals =
    Boolean(doc.score?.hasManagementExpirationLanguage) ||
    (doc.signals?.managementExpirationMentions || []).length > 0 ||
    /management|hma|operating agreement/i.test(String(doc.issuerHint || ""));

  const queues = [];
  // Source-priority pointers map attributes to underlying agreements — enqueue both
  // franchise and management discovery when the due-diligence map is generic.
  if (isPointer) {
    if (managementSignals && !franchiseSignals) {
      queues.push("MANAGEMENT_CONTRACT_DISCOVERY");
    } else if (franchiseSignals && !managementSignals) {
      queues.push("FRANCHISE_AGREEMENT_DISCOVERY");
    } else {
      queues.push("FRANCHISE_AGREEMENT_DISCOVERY", "MANAGEMENT_CONTRACT_DISCOVERY");
    }
  }

  return {
    docId: doc.docId || null,
    url: doc.url || null,
    issuerHint: doc.issuerHint || null,
    sourceClass: doc.sourceClass || null,
    queues,
    evidenceClass: isAssumption
      ? APPRAISAL_EVIDENCE_CLASS.APPRAISAL_ASSUMPTION
      : isPointer
        ? APPRAISAL_EVIDENCE_CLASS.SOURCE_PRIORITY_POINTER
        : APPRAISAL_EVIDENCE_CLASS.INCIDENTAL_APPRAISAL_EVIDENCE,
    maySatisfyContractualVerificationGate: false,
    valuationAssumptionsAreNotContractualTerms: true,
    note: isAssumption
      ? "Appraisal assumption — never treat as verified contractual term"
      : isPointer
        ? "Route to Franchise/Management discovery queues; extract from underlying agreements / CMBS annexes"
        : "Incidental evidence retained; not a standalone discovery path",
  };
}

export function buildSourcePriorityPointerQueues(pilotSummary = {}) {
  const documents = Array.isArray(pilotSummary.documents) ? pilotSummary.documents : [];
  const pointers = documents.filter((d) => d.score?.pointerOnly === true);
  const franchiseQueue = [];
  const managementQueue = [];
  const routed = [];

  for (const doc of pointers) {
    const route = routeAppraisalPilotDocToQueues(doc);
    routed.push(route);
    const entry = {
      docId: doc.docId,
      url: doc.url,
      issuerHint: doc.issuerHint,
      sourceClass: doc.sourceClass,
      country: doc.country || "US",
      region: doc.region || "US",
      hifResearchId: doc.hifResearchId || null,
      queuedFrom: "APPRAISAL_UW_PILOT_SOURCE_PRIORITY_POINTERS",
      targetExtraction: [
        "Franchise Expiration Date",
        "Management Agreement Expiration",
        "applicable notice periods",
      ],
      mayPromoteToVerifiedExpirationWithoutIndependentExtract: false,
      publicationState: "CUSTOMER_BLOCKED",
    };
    if (route.queues.includes("FRANCHISE_AGREEMENT_DISCOVERY")) {
      franchiseQueue.push({ ...entry, queue: "FRANCHISE_AGREEMENT_DISCOVERY" });
    }
    if (route.queues.includes("MANAGEMENT_CONTRACT_DISCOVERY")) {
      managementQueue.push({ ...entry, queue: "MANAGEMENT_CONTRACT_DISCOVERY" });
    }
  }

  return {
    version: HIF_APPRAISAL_POLICY_V6,
    decision: APPRAISAL_ADAPTER_DECISION,
    pointerCount: pointers.length,
    expectedPointerCount: 22,
    franchiseQueueCount: franchiseQueue.length,
    managementQueueCount: managementQueue.length,
    franchiseQueue,
    managementQueue,
    routed,
    integrity: {
      ok: pointers.length === 22,
      error: pointers.length === 22 ? null : `EXPECTED_22_POINTERS_GOT_${pointers.length}`,
    },
  };
}

export function assertAppraisalPolicyLocked(policy = APPRAISAL_ADAPTER_DECISION) {
  const errors = [];
  if (policy.decision !== "DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER") {
    errors.push("DECISION_MUST_REMAIN_DO_NOT_PRIORITIZE");
  }
  if (policy.buildStandaloneAppraisalAdapter !== false) {
    errors.push("STANDALONE_ADAPTER_MUST_REMAIN_FALSE");
  }
  if (policy.authorizePaidAppraisalResearch !== false) {
    errors.push("PAID_APPRAISAL_MUST_REMAIN_UNAUTHORIZED");
  }
  if (policy.valuationAssumptionsAreNotContractualTerms !== true) {
    errors.push("ASSUMPTIONS_MUST_NOT_BE_CONTRACT_TERMS");
  }
  return { ok: errors.length === 0, errors };
}

export default {
  HIF_APPRAISAL_POLICY_V6,
  APPRAISAL_ADAPTER_DECISION,
  APPRAISAL_EVIDENCE_CLASS,
  routeAppraisalPilotDocToQueues,
  buildSourcePriorityPointerQueues,
  assertAppraisalPolicyLocked,
};
