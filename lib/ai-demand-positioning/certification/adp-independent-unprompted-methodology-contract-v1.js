/**
 * ADP Independent Unprompted V1 — frozen methodology contract.
 *
 * Authoritative definition for customer-facing AI Consideration appearance KPIs.
 * Do not silently change metric semantics; bump contract version for intentional changes.
 *
 * Contract: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1
 */

export const ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1 =
  "ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1";

/**
 * 1.1.0 — Monterrey geo-contamination fix + brand/compare exclusions.
 * 1.1.1 — Consideration numerator requires response-text detectPropertyMention;
 *         runtime observations[].mentioned flags are non-authoritative for certification.
 */
export const ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION = "1.1.1";

export const ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1 =
  "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1";

export const ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1 =
  "ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1";

/** Approved numerical tolerance for independent reproduction (percentage points). */
export const ADP_CONSIDERATION_REPRODUCTION_TOLERANCE_PP = 0.15;

/**
 * Frozen contract body — descriptive SoT for certification, Admin, and docs.
 * Executable eligibility remains in adp-unprompted-independent-discovery-standard-v1.js;
 * this module freezes the approved semantics and required publication fields.
 */
export const INDEPENDENT_UNPROMPTED_V1_CONTRACT = Object.freeze({
  contractId: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1,
  contractVersion: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
  methodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  historicalMethodologyId: ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1,
  executableStandardModule:
    "lib/ai-demand-positioning/certification/adp-unprompted-independent-discovery-standard-v1.js",
  considerationProducer: "computeConsiderationMetrics + unpromptedIndependentDiscovery filter",

  eligibleScenarioClassification: {
    include: ["INDEPENDENT_UNPROMPTED"],
    exclude: [
      "SUBJECT_PROMPTED",
      "COMPETITIVE_COMPARISON",
      "ATTRIBUTE_VERIFICATION",
      "AMBIGUOUS_REVIEW_REQUIRED",
    ],
    note: "Only scenarios assessed independentlyEligible under the unprompted standard enter appearance KPIs.",
  },

  promptedVersusUnprompted: {
    unprompted:
      "Prompts that do not name/contaminate the subject hotel identity (geo destination allowed).",
    prompted:
      "Subject-named / brand-contaminated / assisted consideration prompts are excluded from Consideration.",
  },

  providerTreatment: {
    grain: "OBSERVATION_RESPONSE",
    providers: "All Core providers in the period provider set counted uniformly",
    incompleteProvider: "Fail closed / document incompleteCoverage — do not silently drop providers from denom without stamp",
  },

  hotelRecommendationAttribution: {
    numeratorSignal:
      "subject hotel genuinely appears in original AI response text (detectPropertyMention on archive/runtime response)",
    notCounted: [
      "Other hotels",
      "Same-brand different properties",
      "Generic brand mentions",
      "Similar hotel names / identity collisions",
      "Geographic references without hotel identity",
      "Prompted hotel appearances (excluded at eligibility)",
    ],
    runtimeMentionFlags: "NON_AUTHORITATIVE — may not be used alone for certification",
    failClosedMissingResponseText: true,
  },

  competitorAttribution: {
    consideration:
      "Competitor mentions are NOT automatically hotel recommendations and do not enter the Consideration numerator",
    competitiveTables: "Separate competitive ranking / displacement contracts",
  },

  numerator: {
    definition:
      "Count of comparable eligible observations where detectPropertyMention(originalResponse, subjectProperty) is true",
    field: "executiveMetrics.considerationRate.presentObservations",
  },

  denominator: {
    definition:
      "Count of comparable parsed observations whose scenarioId is in frozen eligibleScenarioIds (Independent Unprompted eligible only)",
    field: "executiveMetrics.considerationRate.comparableObservations",
    formula: "eligibleScenarios × providers (when full coverage)",
  },

  aggregation: {
    rate: "roundAdpPercent( (numerator / denominator) * 100 ) → one decimal place",
    providerLevel: "Same eligible scenario set per provider; no cross-provider fill",
    scenarioLevel: "Scenario presence uses eligible scenarios only",
  },

  exclusions: [
    "Subject-prompted scenarios",
    "Competitive comparison prompts (including bare Compare-hotels)",
    "Other-brand-specific prompts (foreign chain/loyalty)",
    "Attribute verification prompts",
    "Non-accommodation venue-only prompts",
    "Ineligible / ambiguous discovery classes",
    "Unparsed / invalid AI responses (not comparable)",
    "Loyalty/BPP corpus (separate contract)",
  ],

  geographicIdentityHandling: {
    rule: "Destination geography (city/market/submarket and trailing location tokens in brand+location hotel names) must NOT contaminate independent discovery",
  },

  methodologyFingerprintModule:
    "lib/ai-demand-positioning/certification/adp-independent-unprompted-v1-methodology-fingerprint.js",

  roundingAndTolerance: {
    displayRounding: "roundAdpPercent — one decimal place",
    reproductionTolerancePp: ADP_CONSIDERATION_REPRODUCTION_TOLERANCE_PP,
  },

  requiredPublicationFields: [
    "methodologyId",
    "methodologyContractVersion",
    "measurementPeriodId",
    "providerCoverage",
    "scenarioCoverage",
    "eligibleScenarioIds",
    "numerator",
    "denominator",
    "considerationRate",
    "sourceEvidenceReferences",
    "sourceCorpusSha",
    "calculationProvenance",
    "certificationVerdict",
  ],
});

export function isIndependentUnpromptedMethodologyId(id) {
  return String(id || "").trim() === ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1;
}

export function isLegacyMentionAllCoreMethodologyId(id) {
  const s = String(id || "").trim();
  return !s || s === ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1;
}

export function getIndependentUnpromptedMethodologyContract() {
  return INDEPENDENT_UNPROMPTED_V1_CONTRACT;
}
