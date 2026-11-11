/**
 * Provider Presence grain lock — prevent mixing independent-unprompted executive
 * KPIs (64 scenarios / 256 responses) with historical research-inventory provider
 * rows (78 scenarios).
 *
 * Serve-time and restage-time: if appearance methodology is independent-unprompted
 * (or executiveMetrics already encode that grain), evidence.providers MUST use the
 * same eligible scenario IDs. Never silently leave a 78-denominator provider table
 * beside a 64-denominator Scenario Presence KPI.
 */

import { loadLatestCoreCustomerPeriod, loadPropertyProfile } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import {
  applyUnpromptedStandardToUniverse,
  loadArchivePromptMap,
  CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
} from "../certification/adp-unprompted-independent-discovery-standard-v1.js";
import { scenariosForPeriodDisplacement } from "../certification/adp-autonomous-competitor-entity-resolution-v1.js";
import { enrichObservationsWithRank } from "./executive-metrics-foundation.js";
import { projectGovernedObservations } from "../subject-presence/canonical-subject-presence-v1.js";
import { buildProviderPresenceRows } from "./provider-presence-v1.js";
import { filterComparableObservations } from "./grain-governance.js";
import {
  buildScenarioEligibilityMap,
  CONSIDERATION_ELIGIBLE_STATES,
  scenariosForEligibility,
} from "./scenario-eligibility.js";

export const ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1 =
  "ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1";

/**
 * Canonical appearance scenario set for Consideration / Scenario Presence /
 * Provider Presence / Territory / Competitive appearance grain.
 *
 * Order: unprompted independent-discovery filter → consideration eligibility
 * (CORE + CONDITIONAL + PROPERTY_SPECIFIC). Never use research inventory (78/63)
 * for provider denominators beside independent executive KPIs.
 */
export function resolveIndependentAppearanceScenarios({
  propertyId,
  periodId,
  scenarios,
  propertyProfile,
  promptMap = null,
} = {}) {
  const applied = applyUnpromptedStandardToUniverse({
    propertyId,
    periodId,
    scenarios: scenarios || [],
    propertyProfile,
    promptMap,
  });
  const unpromptedEligible = applied.eligibleScenarios || [];
  const eligibility = buildScenarioEligibilityMap(unpromptedEligible, propertyProfile);
  const eligible = scenariosForEligibility(
    unpromptedEligible,
    eligibility,
    CONSIDERATION_ELIGIBLE_STATES
  );
  return {
    applied,
    unpromptedEligible,
    eligibleScenarios: eligible,
    eligibleIds: new Set(eligible.map((s) => s.scenarioId || s.id).filter(Boolean)),
    researchInventory: (scenarios || []).length,
  };
}

function finiteNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Authoritative independent-discovery population for Provider Presence ↔ Consideration.
 * Frozen Consideration eligibleScenarioIds win over scenarioPresence.eligibleScenarios
 * when both exist (packages may carry a stale SP eligible count from grain repair).
 */
export function resolveAuthoritativeAppearanceEligibleCount(payload = {}) {
  const em = payload.executiveMetrics || {};
  const frozen = em.considerationRate?.eligibleScenarioIds;
  if (Array.isArray(frozen) && frozen.length > 0) return frozen.length;
  return finiteNum(em.scenarioPresence?.eligibleScenarios);
}

export function detectIndependentAppearanceGrain(payload = {}) {
  const em = payload.executiveMetrics || {};
  const meth =
    em.considerationRate?.methodologyId ||
    em.considerationRate?.correctedMethodologyId ||
    em.scenarioPresence?.methodologyId ||
    payload.correctionLineage?.correctedMethodologyId ||
    payload.unpromptedIndependentDiscovery?.correctedMethodologyId ||
    null;
  const eligible = resolveAuthoritativeAppearanceEligibleCount(payload);
  const comparable = finiteNum(em.considerationRate?.comparableObservations);
  const independentMeth =
    meth === CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID ||
    meth === "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1";
  const independentCounts =
    (eligible != null && eligible > 0 && eligible < 78 && comparable === eligible * 4) ||
    (eligible === 64 && comparable === 256);
  return {
    independent: Boolean(independentMeth || independentCounts),
    methodologyId: meth,
    eligibleScenarios: eligible,
    comparableObservations: comparable,
    frozenEligibleScenarioIds: Array.isArray(em.considerationRate?.eligibleScenarioIds)
      ? em.considerationRate.eligibleScenarioIds.length
      : null,
    scenarioPresenceEligible: finiteNum(em.scenarioPresence?.eligibleScenarios),
  };
}

export function auditProviderPresenceGrain(payload = {}) {
  const appearance = detectIndependentAppearanceGrain(payload);
  const providers = payload.evidence?.providers || [];
  const rows = providers.map((p) => ({
    provider: p.provider,
    mentioned: finiteNum(p.mentioned),
    comparable: finiteNum(p.comparable ?? p.total),
    scheduled: finiteNum(p.scenariosScheduled ?? p.scheduled),
    presence: p.presence,
  }));
  const mentionSum = rows.reduce((a, r) => a + (r.mentioned || 0), 0);
  const comparableSum = rows.reduce((a, r) => a + (r.comparable || 0), 0);
  const has78 = rows.some((r) => r.comparable === 78 || r.scheduled === 78);
  const expectedEligible = appearance.eligibleScenarios;
  const executiveMentions = finiteNum(
    payload.executiveMetrics?.considerationRate?.presentObservations
  );
  const executiveComparable = finiteNum(
    payload.executiveMetrics?.considerationRate?.comparableObservations
  );
  // Providers may have unequal comparable denoms (partial capture). Do NOT require
  // every provider.comparable === eligible. Require sum reconciliation + no denom
  // that exceeds the independent-eligible scenario count (historical leak).
  const allMatchEligible =
    expectedEligible != null &&
    rows.length > 0 &&
    rows.every((r) => r.comparable === expectedEligible);
  const noDenomExceedsEligible =
    expectedEligible == null ||
    rows.length === 0 ||
    rows.every((r) => {
      const sched = r.scheduled != null ? r.scheduled : r.comparable;
      const comp = r.comparable;
      return (
        (sched == null || sched <= expectedEligible) &&
        (comp == null || comp <= expectedEligible)
      );
    });
  const mentionsMatchExecutive =
    executiveMentions == null || mentionSum === executiveMentions;
  const comparableSumMatchesExecutive =
    executiveComparable == null || comparableSum === executiveComparable;
  const mixedGrain =
    appearance.independent &&
    (has78 ||
      !noDenomExceedsEligible ||
      !comparableSumMatchesExecutive ||
      !mentionsMatchExecutive);

  return {
    version: ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1,
    appearance,
    rows,
    mentionSum,
    comparableSum,
    has78,
    allMatchEligible,
    noDenomExceedsEligible,
    mentionsMatchExecutive,
    comparableSumMatchesExecutive,
    mixedGrain,
    pass:
      !appearance.independent ||
      (noDenomExceedsEligible &&
        !has78 &&
        mentionsMatchExecutive &&
        comparableSumMatchesExecutive),
  };
}

/**
 * Rebuild evidence.providers from the independent-eligible observation universe
 * when mixed grain is detected. Preserves other evidence fields.
 */
export function repairProviderPresenceGrain(propertyId, payload, opts = {}) {
  if (!payload || typeof payload !== "object") {
    return { payload, repaired: false, reason: "no_payload" };
  }
  const audit = auditProviderPresenceGrain(payload);
  if (audit.pass && opts.force !== true) {
    return { payload, repaired: false, reason: "already_consistent", audit };
  }
  if (!audit.appearance.independent && opts.force !== true) {
    return { payload, repaired: false, reason: "not_independent_grain", audit };
  }

  const profile =
    opts.propertyProfile || loadPropertyProfile(propertyId) || payload.property;
  const period =
    opts.period ||
    loadLatestCoreCustomerPeriod(propertyId);
  if (!profile || !period?.observations?.length) {
    return {
      payload,
      repaired: false,
      reason: "missing_period_or_profile",
      audit,
    };
  }

  const frozenIds = Array.isArray(
    payload.executiveMetrics?.considerationRate?.eligibleScenarioIds
  )
    ? payload.executiveMetrics.considerationRate.eligibleScenarioIds
        .map(String)
        .filter(Boolean)
    : [];

  const promptMap =
    opts.promptMap ||
    loadArchivePromptMap(propertyId, period.periodId);
  const periodScenarios =
    opts.scenarios ||
    scenariosForPeriodDisplacement(propertyId, period, profile);
  const universe =
    periodScenarios?.length > 0
      ? periodScenarios
      : buildScenarioUniverse(profile);
  const resolved = resolveIndependentAppearanceScenarios({
    propertyId,
    periodId: period.periodId,
    scenarios: universe,
    propertyProfile: profile,
    promptMap,
  });

  // Canonical population = frozen Consideration eligibleScenarioIds when present.
  // Re-deriving via live classifier can diverge from the certified package (e.g. 53 vs 47).
  const eligibleIds =
    frozenIds.length > 0
      ? new Set(frozenIds)
      : resolved.eligibleIds;
  const eligibleCount = eligibleIds.size;
  const governed = projectGovernedObservations(
    enrichObservationsWithRank(period.observations || [], profile),
    profile
  );
  const observations = governed.filter((o) => eligibleIds.has(String(o.scenarioId || "")));
  const { providers, providerCitations } = buildProviderPresenceRows(observations, {
    scenarioIds: eligibleIds,
  });
  const comparableObs = filterComparableObservations(observations);
  const evidence = {
    ...(payload.evidence || {}),
    providers,
    providerCitations,
    comparableObservations: comparableObs.length,
    totalObservations: comparableObs.length,
    scheduledObservations: observations.length,
    appearanceScenarioCount: eligibleCount,
    researchScenarioInventory: universe.length,
    denominatorNote:
      "Provider Presence and citation totals use independent-discovery + consideration-eligible scenarios only (same grain as Consideration / Scenario Presence).",
    providerGrainLock: {
      version: ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1,
      repairedAt: new Date().toISOString(),
      priorAudit: {
        mentionSum: audit.mentionSum,
        comparableSum: audit.comparableSum,
        has78: audit.has78,
      },
      eligibleScenarios: eligibleCount,
      frozenEligibleScenarioIds: frozenIds.length || null,
      unpromptedEligibleScenarios: resolved.unpromptedEligible.length,
      populationSource:
        frozenIds.length > 0
          ? "consideration_eligible_scenario_ids"
          : "live_independent_appearance_resolver",
    },
  };

  let next = { ...payload, evidence };
  // Align Scenario Presence eligible count to the same frozen Consideration population
  // so customer UI / client grain lock do not use a stale SP denominator.
  if (
    frozenIds.length > 0 &&
    next.executiveMetrics?.scenarioPresence &&
    Number(next.executiveMetrics.scenarioPresence.eligibleScenarios) !== frozenIds.length
  ) {
    const sp = { ...next.executiveMetrics.scenarioPresence };
    const priorEligible = sp.eligibleScenarios;
    sp.eligibleScenarios = frozenIds.length;
    sp.eligiblePopulationAlignedToConsideration = true;
    sp.priorEligibleScenariosBeforeGrainLock = priorEligible;
    next = {
      ...next,
      executiveMetrics: {
        ...next.executiveMetrics,
        scenarioPresence: sp,
      },
    };
  }

  const after = auditProviderPresenceGrain(next);
  return {
    payload: next,
    repaired: true,
    reason:
      frozenIds.length > 0
        ? "rebuilt_providers_from_frozen_consideration_eligible_ids"
        : "rebuilt_independent_eligible_providers",
    auditBefore: audit,
    auditAfter: after,
  };
}

/**
 * Attach-at-read contract: never serve mixed independent executive + historical providers.
 * When repair cannot reconcile, stamp a fail-closed marker for the read API.
 */
export function enforceProviderPresenceGrainOnPayload(propertyId, payload, opts = {}) {
  const result = repairProviderPresenceGrain(propertyId, payload, opts);
  const after = result.auditAfter || auditProviderPresenceGrain(result.payload);
  if (after.pass) {
    if (result.payload?._adpProviderPresenceGrainBlocked) {
      const { _adpProviderPresenceGrainBlocked, ...rest } = result.payload;
      return rest;
    }
    return result.payload;
  }
  return {
    ...result.payload,
    _adpProviderPresenceGrainBlocked: {
      version: ADP_PROVIDER_PRESENCE_GRAIN_LOCK_V1,
      blocked: true,
      reason: result.reason || "mixed_grain_unrepaired",
      repaired: result.repaired === true,
      audit: {
        mentionSum: after.mentionSum,
        comparableSum: after.comparableSum,
        has78: after.has78,
        mentionsMatchExecutive: after.mentionsMatchExecutive,
        comparableSumMatchesExecutive: after.comparableSumMatchesExecutive,
        eligibleScenarios: after.appearance?.eligibleScenarios ?? null,
      },
    },
  };
}
