/**
 * Customer-facing governed AI Presence Index.
 * Subject rate / CORE mean × 100. No cap. Numeric only when PRODUCTION_VALIDATED.
 * Legacy declared-comp index is INTERNAL_ROLLBACK_ONLY.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { filterComparableObservations } from "./grain-governance.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import {
  coreIdsForIntent,
  MIN_CORE_PEERS_PRODUCTION,
  PRESENCE_BENCHMARK_VERSION,
  assertCoreSetIntegrity,
  benchmarkVersions,
} from "./presence-benchmark-v1.js";
import { propertyCoreGovernanceReady, isGovernedNonWaterstoneProperty, customerNumericIndexPromotionAllowed } from "./property-core-governance-data.js";
import { computePresenceIndexV2ForIntent, computeScopePresenceRates } from "./presence-index-v2.js";
import {
  scenarioLeaveOneOutRates,
  providerLeaveOneOutRates,
} from "./core-benchmark-rate-contract-v1.js";
import { roundAdpPercent } from "../format-percent.js";
import { BENCHMARK_UNCERTIFIED_LABEL } from "../customer/adp-customer-display-contract-v1.js";

export const GOVERNED_PRESENCE_INDEX_CUSTOMER_VERSION = "adp_governed_ai_presence_index_v2";
export const LEGACY_INDEX_CUSTOMER_RENDER = 0;
export const GOVERNED_INDEX_CUSTOMER_RENDER = 1;
export const SCORE_CAP = "NONE";
/** @deprecated Internal alias — customer-facing value is BENCHMARK_UNCERTIFIED_LABEL. */
export const DEVELOPING_LABEL = BENCHMARK_UNCERTIFIED_LABEL;

export const GOVERNED_INDEX_FORMULA =
  "AI_PRESENCE_INDEX = SUBJECT_AI_PRESENCE_RATE / CORE_BENCHMARK_AI_PRESENCE_RATE × 100";

export const MEANING_100 = "Hotel appears at the same rate as the average CORE comparable hotel.";
export const MEANING_120 = "Hotel appears 20% more often than the CORE benchmark.";
export const MEANING_80 = "Hotel appears 20% less often than the CORE benchmark.";

const DEFAULT_MIN_SCENARIOS = 8;
/** Max single-scenario subject-rate movement (pp) under leave-one-out. */
export const LOO_SUBJECT_PP_MAX = 10;
/** Max single-scenario CORE-benchmark movement (pp) under leave-one-out. */
export const LOO_CORE_PP_MAX = 5;
export const PROVIDER_LOO_PP_MAX = 10;
const MIN_COMPARABLE_N = 20;

export function propertyEligibleForGovernedCoreBenchmark(propertyProfile) {
  const id = String(propertyProfile?.propertyId || "");
  if (id === "adp_waterstone_boca_raton") return true;
  if (isGovernedNonWaterstoneProperty(id)) return propertyCoreGovernanceReady(id);
  const market = `${propertyProfile?.market || ""} ${propertyProfile?.submarket || ""}`.toLowerCase();
  return market.includes("boca");
}

export function governedMinScenarioCount(intent) {
  if (intent === TRAVELER_INTENTS.ADVENTURE) return 7;
  return DEFAULT_MIN_SCENARIOS;
}

function subjectRateOnly(observations, scenarios, intent) {
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  const scoped = filterComparableObservations(observations).filter((o) => ids.has(o.scenarioId));
  const rates = computeScopePresenceRates(scoped, []);
  return {
    subjectRate: rates.ok ? rates.subjectRate : null,
    subjectRatePct: rates.ok ? rates.subjectRatePct : null,
    comparableN: rates.comparableN || 0,
  };
}

export function certifyGovernedTerritory({
  eligible,
  coreCount,
  scenarioCount,
  intent,
  providerCount,
  comparableN,
  benchmarkRate,
  integrity,
  scenarioLoo,
  providerLoo,
}) {
  const blockers = [];
  if (!eligible) blockers.push("property_not_on_governed_core");
  if (coreCount < MIN_CORE_PEERS_PRODUCTION) blockers.push("core_lt_4");
  if (scenarioCount < governedMinScenarioCount(intent)) blockers.push("scenario_density");
  if (providerCount < 3) blockers.push("provider_scopes_lt_3");
  if ((comparableN || 0) < MIN_COMPARABLE_N) blockers.push("thin_observations");
  if (!(benchmarkRate > 0)) blockers.push("zero_denominator");
  if (integrity && (integrity.DUPLICATE_CORE_ENTITIES || integrity.GENERIC_CORE_ENTITIES)) {
    blockers.push("peer_identity");
  }
  const subjectLooPp = scenarioLoo?.maxSubjectPpMove ?? 0;
  const coreLooPp = scenarioLoo?.maxBenchmarkPpMove ?? 0;
  if (subjectLooPp > LOO_SUBJECT_PP_MAX) blockers.push("scenario_loo_subject");
  if (coreLooPp > LOO_CORE_PP_MAX) blockers.push("scenario_loo_core");
  if (blockers.some((b) => b.startsWith("scenario_loo_"))) blockers.push("scenario_loo");
  if (providerLoo?.PROVIDER_MODEL_PD_FAIL) {
    if (providerLoo.PROVIDER_MODEL_PD_SUBJECT_FAIL) blockers.push("provider_concentration_subject");
    if (providerLoo.PROVIDER_MODEL_PD_CORE_FAIL) blockers.push("provider_concentration_core");
    blockers.push("provider_concentration");
  }

  let status = "PRODUCTION_VALIDATED";
  if (!eligible || coreCount < MIN_CORE_PEERS_PRODUCTION || scenarioCount < 6 || !(benchmarkRate > 0)) {
    status = "BENCHMARK_DEVELOPING";
  } else if (blockers.length) {
    status = blockers.includes("zero_denominator") ? "BLOCKED" : "CONDITIONALLY_ELIGIBLE";
    if (blockers.includes("scenario_density") || blockers.includes("thin_observations") || blockers.includes("provider_scopes_lt_3")) {
      status = "BENCHMARK_DEVELOPING";
    }
  }

  return { status, blockers };
}

export function buildGovernedIntentPresenceIndex(observations, scenarios, propertyProfile) {
  const eligible = propertyEligibleForGovernedCoreBenchmark(propertyProfile);
  const result = {};
  const intents = [...new Set((scenarios || []).map((s) => s.intent))];

  for (const intent of intents) {
    const scenarioCount = (scenarios || []).filter((s) => s.intent === intent).length;
    const territory = territoryLabelForIntent(intent);
    const coreIds = eligible ? coreIdsForIntent(intent, propertyProfile) : [];
    const integrity = eligible ? assertCoreSetIntegrity(coreIds, propertyProfile) : { allCanonical: false };

    let subjectRatePct = null;
    let coreBenchmarkRatePct = null;
    let computedIndex = null;
    let includedProviders = [];
    let comparableN = 0;

    if (eligible && coreIds.length) {
      const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent, { propertyProfile, coreIds });
      const ap = v2.allProviders;
      includedProviders = ap.includedProviders || [];
      comparableN = ap.comparableN || 0;
      subjectRatePct = ap.subjectRatePct ?? null;
      coreBenchmarkRatePct = ap.coreBenchmarkRatePct ?? null;
      computedIndex = ap.index ?? null;
    } else {
      const sub = subjectRateOnly(observations, scenarios, intent);
      subjectRatePct = sub.subjectRatePct;
      comparableN = sub.comparableN;
    }

    const loo = eligible
      ? scenarioLeaveOneOutRates(observations, scenarios, intent, propertyProfile)
      : { SCENARIO_THINNESS_HIGH: true, maxSubjectPpMove: null };
    const plo = eligible
      ? providerLeaveOneOutRates(observations, scenarios, intent, propertyProfile)
      : { PROVIDER_CONCENTRATION_RISK: false, dropProviderSubjectPp: {} };

    const cert = certifyGovernedTerritory({
      eligible,
      coreCount: coreIds.length,
      scenarioCount,
      intent,
      providerCount: includedProviders.length,
      comparableN,
      benchmarkRate: coreBenchmarkRatePct == null ? null : coreBenchmarkRatePct / 100,
      integrity,
      scenarioLoo: loo,
      providerLoo: plo,
    });

    const numericAllowed = customerNumericIndexPromotionAllowed(propertyProfile?.propertyId);
    let customerStatus = cert.status;
    if (!numericAllowed && cert.status === "PRODUCTION_VALIDATED") {
      customerStatus = "BENCHMARK_DEVELOPING";
    }
    const showNumeric = numericAllowed && cert.status === "PRODUCTION_VALIDATED" && computedIndex != null;
    const showCore = numericAllowed && cert.status === "PRODUCTION_VALIDATED" && coreBenchmarkRatePct != null;

    result[intent] = {
      methodology: GOVERNED_PRESENCE_INDEX_CUSTOMER_VERSION,
      customerRender: "governed",
      territory,
      status: customerStatus,
      blockers: cert.blockers,
      index: showNumeric ? computedIndex : null,
      myRate: subjectRatePct,
      subjectRatePct,
      coreBenchmarkRatePct: showCore ? coreBenchmarkRatePct : null,
      avgCompRate: showCore ? coreBenchmarkRatePct : 0,
      developing: !showNumeric,
      developingLabel: DEVELOPING_LABEL,
      coreCount: coreIds.length,
      scenarioCount,
      includedProviders,
      comparableN,
      zeroPresenceCoreIncluded: true,
      secondaryInDenominator: 0,
      scoreCap: SCORE_CAP,
      versions: benchmarkVersions(),
      PRESENCE_BENCHMARK_VERSION,
    };
  }

  return result;
}

export function assertNumericIndexHasRates(row) {
  if (row?.index == null) return { ok: true };
  const hasSubject = row.subjectRatePct != null || row.myRate != null;
  const hasCore = row.coreBenchmarkRatePct != null;
  return {
    ok: hasSubject && hasCore,
    NUMERIC_INDEX_WITHOUT_SUBJECT_RATE: hasSubject ? 0 : 1,
    NUMERIC_INDEX_WITHOUT_CORE_BENCHMARK: hasCore ? 0 : 1,
  };
}
