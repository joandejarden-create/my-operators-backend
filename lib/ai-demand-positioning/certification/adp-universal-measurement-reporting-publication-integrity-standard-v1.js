/**
 * ADP_UNIVERSAL_MEASUREMENT_REPORTING_PUBLICATION_INTEGRITY_STANDARD_V1
 *
 * Permanent Waterstone-derived standard for all ADP hotels and future Full E2E runs.
 * Core appearance KPIs use ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1 only.
 * BPP remains separately governed. No additional customer KPI cards.
 */

import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "./adp-unprompted-independent-discovery-standard-v1.js";
import {
  ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1,
  auditProviderPresenceGrain,
} from "../metrics/adp-provider-presence-grain-lock-v1.js";
import { ADP_MEASUREMENT_INTEGRITY_GATE_V1 } from "./adp-measurement-integrity-gate-v1.js";
import {
  ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1,
  COMPETITIVE_INTEGRITY_CUSTOMER_READY_REQUIREMENTS,
} from "./adp-universal-competitive-entity-integrity-v1.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";

export const ADP_UNIVERSAL_MEASUREMENT_REPORTING_PUBLICATION_INTEGRITY_STANDARD_V1 =
  "ADP_UNIVERSAL_MEASUREMENT_REPORTING_PUBLICATION_INTEGRITY_STANDARD_V1";

export const UNIVERSAL_CORE_METHODOLOGY_ID = CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID;

export const UNIVERSAL_CORE_KPI_CONTRACT = Object.freeze({
  consideration:
    "verified_subject_mentions / eligible_comparable_AI_responses",
  scenarioPresence:
    "eligible_scenarios_with_≥1_verified_subject_mention / eligible_scenarios",
  providerPresence:
    "provider_subject_mentions / that_provider_eligible_comparable_responses",
  realityCoverage: "fully_recognized_attributes / tested_attributes",
  realityGap:
    "(missed_attributes + 0.5 × partially_recognized_attributes) / tested_attributes",
  competitiveRankings:
    "subject_and_competitor_appearances_from_same_independent_unprompted_eligible_population",
  providerReconciliation: {
    sumMentionsEqualsOverall: true,
    sumComparableEqualsOverall: true,
    unequalProviderDenomsAllowedWhenLegitimate: true,
    historical78MixedWithIndependentForbidden: true,
  },
  bpp: "SEPARATELY_GOVERNED_DO_NOT_FILTER_BY_CORE_UNPROMPTED",
  noAdditionalCustomerKpiCards: true,
});

export const UNIVERSAL_APPROVED_DEMAND_SEGMENTS = Object.freeze(
  Object.entries(TRAVELER_INTENTS).map(([key, id]) => ({
    key,
    intentId: id,
    customerLabel: {
      BUSINESS: "Business Travel",
      LEISURE: "Leisure Travel",
      COUPLES: "Couples",
      FAMILY: "Family Travel",
      GROUP_MEETING: "Meetings & Groups",
      WELLNESS: "Wellness",
      ADVENTURE: "Adventure & Experiences",
      CELEBRATION: "Celebrations & Events",
    }[key],
  }))
);

/** Meetings / Group / Meetings & Groups collapse to one approved demand segment. */
export const UNIVERSAL_TAXONOMY_DEDUP_RULES = Object.freeze({
  meetingsAndGroups: {
    approvedIntentId: TRAVELER_INTENTS.GROUP_MEETING,
    approvedCustomerLabel: "Meetings & Groups",
    notDistinctCustomerCategories: ["Group", "Meetings", "Meetings & Groups"],
    note: "Distinct prompt frames allowed; parallel customer categories prohibited",
  },
  value: {
    approvedAsDemandSegment: false,
    note: "Value is a decision frame under Leisure (or other primary intents), not a TRAVELER_INTENTS segment",
  },
});

export const UNIVERSAL_CUSTOMER_READY_REQUIREMENTS = Object.freeze([
  "GENUINE_LLM_OBSERVATIONS_ARCHIVED",
  "INDEPENDENT_UNPROMPTED_PROMPT_INTEGRITY",
  "EXCLUSIONS_DOCUMENTED",
  "MEASUREMENT_CALCULATIONS_INDEPENDENTLY_RECONCILE",
  "PROVIDER_AND_OVERALL_NUMERATORS_RECONCILE",
  "COMPETITIVE_RANKINGS_CERTIFIED",
  "REALITY_COVERAGE_GAP_CORRECT",
  "ALL_SUBSECTIONS_USE_CORRECT_POPULATION",
  "REAL_APPLICATION_ROUTE_BROWSER_QA",
  "PDF_PARITY",
  "AIRTABLE_PERSISTENCE_READBACK_WHEN_REQUIRED",
  "PUBLICATION_STATE_ACCURATELY_REPRESENTED",
  "NO_MOCKED_REPORT_BODY_FOR_CUSTOMER_CERT",
  "NO_SERVE_TIME_CRUTCH_FOR_INVALID_PERSISTED_ARTIFACT",
  "LOCAL_PUBLISHED_EXTERNAL_PAYLOAD_IDENTITY",
  ...COMPETITIVE_INTEGRITY_CUSTOMER_READY_REQUIREMENTS,
]);

export const UNIVERSAL_REPORT_IDENTITY_FIELDS = Object.freeze([
  "hotelId",
  "monitoringPeriodId",
  "reportVersionId",
  "methodologyId",
  "eligibleScenarioPopulation",
  "comparableResponsePopulation",
  "attributePopulationVersion",
  "publicationStatus",
]);

export const EXTERNAL_URL_CLASSIFICATION = Object.freeze({
  CURRENT_AND_CERTIFIED: "CURRENT_AND_CERTIFIED",
  VALID_BUT_HISTORICAL: "VALID_BUT_HISTORICAL",
  CORRECTION_REQUIRED: "CORRECTION_REQUIRED",
  ACCESS_FAILURE: "ACCESS_FAILURE",
  NOT_PUBLISHED: "NOT_PUBLISHED",
});

/**
 * Customer-visible publication must not proceed when Provider Presence cannot
 * reconcile to certified Consideration (Hilton / eligible≤53 class defect).
 * Prefer auditing the payload that will be served (post grain-lock repair).
 */
export function assertCustomerVisibleProviderPresenceGrain(payload, opts = {}) {
  const audit = auditProviderPresenceGrain(payload || {});
  const ok = audit.pass === true;
  return {
    ok,
    gate: ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1,
    standard: ADP_UNIVERSAL_MEASUREMENT_REPORTING_PUBLICATION_INTEGRITY_STANDARD_V1,
    customerVisibleAllowed: ok,
    blockers: ok
      ? []
      : [
          "PROVIDER_PRESENCE_DOES_NOT_RECONCILE_TO_CONSIDERATION",
          ...(audit.has78 ? ["HISTORICAL_78_SCENARIO_PROVIDER_DENOM"] : []),
          ...(!audit.mentionsMatchExecutive
            ? ["PROVIDER_MENTION_SUM_NE_CONSIDERATION_PRESENT"]
            : []),
          ...(!audit.comparableSumMatchesExecutive
            ? ["PROVIDER_COMPARABLE_SUM_NE_CONSIDERATION_COMPARABLE"]
            : []),
        ],
    audit,
    propertyId: opts.propertyId || payload?.propertyId || null,
  };
}

export function describeUniversalStandard() {
  return {
    standard: ADP_UNIVERSAL_MEASUREMENT_REPORTING_PUBLICATION_INTEGRITY_STANDARD_V1,
    coreMethodologyId: UNIVERSAL_CORE_METHODOLOGY_ID,
    kpiContract: UNIVERSAL_CORE_KPI_CONTRACT,
    approvedDemandSegments: UNIVERSAL_APPROVED_DEMAND_SEGMENTS,
    taxonomyDedupRules: UNIVERSAL_TAXONOMY_DEDUP_RULES,
    customerReadyRequirements: UNIVERSAL_CUSTOMER_READY_REQUIREMENTS,
    reportIdentityFields: UNIVERSAL_REPORT_IDENTITY_FIELDS,
    relatedGates: {
      measurementIntegrity: ADP_MEASUREMENT_INTEGRITY_GATE_V1,
      providerGrainLock: ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1,
      competitiveEntityIntegrity: ADP_UNIVERSAL_COMPETITIVE_ENTITY_INTEGRITY_V1,
      customerVisibleProviderPresenceGrain:
        "assertCustomerVisibleProviderPresenceGrain",
    },
    regressionFixtures: [
      "adp_waterstone_boca_raton",
      "adp_hilton_times_square",
      "adp_radisson_blu_toronto_downtown",
      "adp_sheraton_guadalajara_expo",
      "adp_voco_cancun_zona_hotelera",
      "adp_wyndham_alltra_punta_cana",
    ],
    waterstoneReferenceVersion: "staged_mw14o9lq_bb8bd9570c",
  };
}
