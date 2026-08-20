/**
 * ADP_MEASUREMENT_CONTRACT_V1 — frozen production measurement contract.
 * Do not silently mutate while an official baseline period is running.
 */

import crypto from "crypto";
import {
  MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION,
  CANONICAL_PRESENCE_PER_OBSERVATION_VERSION,
} from "../customer/canonical-presence-per-observation-v1.js";
import {
  BENCHMARK_UNCERTIFIED_LABEL,
} from "../customer/adp-customer-display-contract-v1.js";
import {
  OWNED_SOURCE_DEFINITION_V1,
  OWNED_SOURCE_CLASSIFICATION_VERSION,
} from "../metrics/owned-source-classification-v1.js";
import {
  CORE_BENCHMARK_FORMULA,
  CORE_BENCHMARK_RATE_CONTRACT_VERSION,
  SUBJECT_RATE_FORMULA,
  ZERO_CORE_PEERS_INCLUDED,
  SECONDARY_IN_BENCHMARK,
} from "../metrics/core-benchmark-rate-contract-v1.js";
import {
  GOVERNED_INDEX_FORMULA,
  GOVERNED_PRESENCE_INDEX_CUSTOMER_VERSION,
  SCORE_CAP,
} from "../metrics/governed-customer-presence-index.js";
import {
  MIN_CORE_PEERS_PRODUCTION,
  PRESENCE_BENCHMARK_VERSION,
} from "../metrics/presence-benchmark-v1.js";
import { PROVIDERS } from "../data-model.js";
import { ENTITY_RESOLUTION_VERSION } from "../metrics/south-florida-entity-registry.js";

export const MEASUREMENT_CONTRACT_VERSION = "ADP_MEASUREMENT_CONTRACT_V1";
export const OFFICIAL_BASELINE_EPOCH = "ADP_OFFICIAL_BASELINE_EPOCH_V1";
export const OFFICIAL_BASELINE_PERIOD_MARKER = "ADP_OFFICIAL_BASELINE_PERIOD_001";
export const OFFICIAL_BASELINE_SEQUENCE = 1;

export const ADP_PROPERTY_IDS_V1 = Object.freeze([
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
]);

/** Stable canonical object used for hashing (no volatile timestamps). */
export function buildMeasurementContractCanonicalBody() {
  return {
    CONTRACT_VERSION: MEASUREMENT_CONTRACT_VERSION,
    SCENARIO_CONTRACT_VERSION: "adp_scenario_universe_v1",
    PROVIDER_SET: [...PROVIDERS],
    OBSERVATION_GRAIN: "property × scenario × provider × period",
    COMPARABLE_OBSERVATION_RULE:
      "Comparable observations exclude dryRun, provider errors, and unparsed empty responses. Missing provider observation != measured zero.",
    PROVIDER_AGGREGATION_RULE: "POOLED_RESPONSE_DENOMINATOR for Competitive Overview; All Providers = equal mean of included provider rates for territory subject/CORE rates. No row-level provider fallback.",
    SUBJECT_PRESENCE_RULE:
      "Binary subject appearance per comparable observation (obs.mentioned). One credit per observation when present.",
    CANONICAL_COMPETITOR_PRESENCE_RULE:
      "One canonical hotel may receive at most one presence credit per individual comparable AI response.",
    MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION,
    SCENARIO_PRESENCE_FORMULA:
      "AI Scenario Presence = share of monitored scenarios where the subject hotel appears in at least one comparable provider response.",
    CONSIDERATION_RATE_FORMULA:
      "AI Consideration Rate = comparable observations where the subject appears / all comparable observations (observation grain).",
    "#1_APPEARANCE_RATE_FORMULA":
      "#1 Appearance Rate = ranked observations where subject position === 1 / ranked comparable observations with a valid rank.",
    TOP3_APPEARANCE_RATE_FORMULA:
      "Top-3 Appearance Rate = ranked observations where subject position <= 3 / ranked comparable observations with a valid rank.",
    COMPETITOR_PRESENT_SCENARIO_DEFINITION:
      "Scenarios where at least one competitor appears in a comparable response while the subject is absent (competitor-present gaps).",
    PROPERTY_REALITY_COVERAGE_FORMULA:
      "Property Reality Coverage = recognized monitored property attributes / total tracked attributes for the property profile.",
    CORE_BENCHMARK_FORMULA,
    SUBJECT_PRESENCE_RATE_FORMULA: SUBJECT_RATE_FORMULA,
    CORE_MINIMUM_COUNT_RULE: `MIN_CORE_PEERS_PRODUCTION = ${MIN_CORE_PEERS_PRODUCTION}`,
    CORE_ZERO_RULE: `Measured 0% CORE peer remains included (${ZERO_CORE_PEERS_INCLUDED}).`,
    CORE_MISSING_RULE: "Missing / unavailable peer evidence is omitted from the mean; it is not converted to zero.",
    SECONDARY_IN_BENCHMARK,
    MODEL_D_VERSION: "adp_provider_concentration_model_d_v1",
    MODEL_P_D_VERSION: "adp_provider_concentration_model_p_d_v1",
    AI_PRESENCE_INDEX_FORMULA: GOVERNED_INDEX_FORMULA,
    AI_PRESENCE_INDEX_CAP: SCORE_CAP,
    DISPLACEMENT_EVENT_DEFINITION:
      "A displacement event is a comparable observation where a named competitor appears and the subject hotel does not, within the selected territory/overall scope.",
    SHARED_SCENARIO_DEFINITION:
      "A shared scenario is a monitored scenario where both the subject and a named competitor appear in comparable observations for the selected scope.",
    SOURCE_CLASSIFICATION_VERSION: OWNED_SOURCE_CLASSIFICATION_VERSION,
    OWNED_SOURCE_DEFINITION: OWNED_SOURCE_DEFINITION_V1,
    UNKNOWN_SOURCE_RULE: "UNKNOWN must not silently become OWNED or EXTERNAL.",
    TREND_COMPATIBILITY_RULE:
      "Future official periods must not automatically trend against Period 001 if material contract components differ. UNKNOWN COMPATIBILITY = NOT COMPARABLE.",
    PERIOD_SELECTION_RULE:
      "LATEST_CERTIFIED_OFFICIAL_FULL_PROPERTY_PERIOD — officialPeriod=true, certified=true, fullProperty=true, customerVisible=true, contract-compatible, then latest timestamp.",
    CUSTOMER_TERMINOLOGY_VERSION: "adp_customer_terminology_v1",
    BENCHMARK_UNCERTIFIED_LABEL,
    ENTITY_RESOLUTION_VERSION,
    PRESENCE_BENCHMARK_VERSION,
    CORE_BENCHMARK_RATE_CONTRACT_VERSION,
    GOVERNED_PRESENCE_INDEX_CUSTOMER_VERSION,
    CANONICAL_PRESENCE_PER_OBSERVATION_VERSION,
    IMPLEMENTATION_REFERENCES: [
      "lib/ai-demand-positioning/metrics/grain-governance.js",
      "lib/ai-demand-positioning/metrics/presence-index-v2.js",
      "lib/ai-demand-positioning/metrics/governed-customer-presence-index.js",
      "lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js",
      "lib/ai-demand-positioning/customer/canonical-presence-per-observation-v1.js",
      "lib/ai-demand-positioning/metrics/owned-source-classification-v1.js",
      "lib/ai-demand-positioning/period-eligibility-v1.js",
    ],
  };
}

export function hashMeasurementContract(canonicalBody = buildMeasurementContractCanonicalBody()) {
  const normalized = JSON.stringify(canonicalBody);
  return crypto.createHash("sha256").update(normalized, "utf8").digest("hex");
}

export function buildFrozenMeasurementContractDocument(freezeTimestamp = new Date().toISOString()) {
  const body = buildMeasurementContractCanonicalBody();
  const hash = hashMeasurementContract(body);
  return {
    version: MEASUREMENT_CONTRACT_VERSION,
    freezeTimestamp,
    MEASUREMENT_CONTRACT_V1_HASH: hash,
    measurementContractHash: hash,
    officialBaselineEpoch: OFFICIAL_BASELINE_EPOCH,
    startPeriodMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
    customerHistoryStart: "PERIOD_001",
    propertyUniverse: [...ADP_PROPERTY_IDS_V1],
    contract: body,
    compatibility: {
      DEFAULT_UNKNOWN_COMPATIBILITY: "NOT_COMPARABLE",
      MATERIAL_CHANGE_FIELDS: [
        "PROVIDER_SET",
        "PROVIDER_AGGREGATION_RULE",
        "SCENARIO_CONTRACT_VERSION",
        "SUBJECT_PRESENCE_RULE",
        "CANONICAL_COMPETITOR_PRESENCE_RULE",
        "CORE_BENCHMARK_FORMULA",
        "CORE_MINIMUM_COUNT_RULE",
        "AI_PRESENCE_INDEX_FORMULA",
        "ENTITY_RESOLUTION_VERSION",
        "OBSERVATION_GRAIN",
        "SOURCE_CLASSIFICATION_VERSION",
      ],
    },
  };
}

export function assertContractHashMatches(doc) {
  const recomputed = hashMeasurementContract(doc.contract || buildMeasurementContractCanonicalBody());
  return {
    ok: recomputed === (doc.measurementContractHash || doc.MEASUREMENT_CONTRACT_V1_HASH),
    expected: doc.measurementContractHash || doc.MEASUREMENT_CONTRACT_V1_HASH,
    recomputed,
  };
}
