/**
 * Per-hunter configuration registry.
 * Each hunter is independently enabled, budgeted, and overridable per hotel.
 * Updating one hunter's config must not rewrite another hunter's preferences.
 */

import {
  SPECIALIST_HUNTER,
  SPECIALIST_HUNTER_LIST,
  SPECIALIST_HUNTER_META,
  SPECIALIST_HUNTER_HORIZON_MONTHS,
  getSpecialistHunterMeta,
} from "../specialist-hunter-contract-v1.js";

const DEFAULT_RETRY = Object.freeze({
  maxAttempts: 2,
  backoffMs: 750,
});

function baseConfigForHunter(hunterId) {
  const meta = getSpecialistHunterMeta(hunterId) || SPECIALIST_HUNTER_META[hunterId] || {};
  return {
    hunterId,
    enabled: true,
    supportedTerritories: ["US"],
    languages: ["en"],
    defaultResearchHorizonMonths: SPECIALIST_HUNTER_HORIZON_MONTHS,
    searchRetrievalLimits: {
      maxQueries: 8,
      maxResultsPerQuery: 10,
      maxDiscoveries: 25,
    },
    providerPreference: meta.primaryProviders || ["public_api"],
    sourcePriority: meta.primaryProviders || ["public_api"],
    cacheBehavior: {
      cacheFirst: true,
      allowSyntheticFixtures: true,
      promoteFixturesToInventory: false,
    },
    researchDepth: "STANDARD",
    evidenceRequirements: {
      minEvidenceUrls: 1,
      requireNamedBuyer: false,
      requireActivityDate: false,
    },
    retryPolicy: { ...DEFAULT_RETRY },
    costCeilingUsd: 0,
    qualificationHandoff: {
      autoSalesReady: false,
      autoPublish: false,
      routeToOrs: true,
      allowCustomerInventory: false,
    },
    apifyRole: meta.apifyRole || "NONE",
    wave: meta.wave || null,
    label: meta.label || hunterId,
  };
}

/** Immutable defaults keyed by hunter id. */
export const HUNTER_CONFIG_DEFAULTS = Object.freeze(
  Object.fromEntries(
    SPECIALIST_HUNTER_LIST.map((id) => [id, Object.freeze(baseConfigForHunter(id))])
  )
);

/**
 * Resolve effective config for one hunter.
 * hotelOverrides[hunterId] merges only that hunter — never copies across hotels/hunters.
 */
export function resolveHunterConfig(hunterId, opts = {}) {
  const base = HUNTER_CONFIG_DEFAULTS[hunterId];
  if (!base) {
    return null;
  }
  const envDisabled = String(process.env[`GDI_HUNTER_${String(hunterId).toUpperCase()}_ENABLED`] || "")
    .trim()
    .toLowerCase();
  const hotelOverrides =
    opts.hotelOverrides && typeof opts.hotelOverrides === "object"
      ? opts.hotelOverrides[hunterId] || {}
      : {};
  const runOverrides = opts.runOverrides && typeof opts.runOverrides === "object" ? opts.runOverrides : {};

  const merged = {
    ...base,
    ...hotelOverrides,
    ...runOverrides,
    searchRetrievalLimits: {
      ...base.searchRetrievalLimits,
      ...(hotelOverrides.searchRetrievalLimits || {}),
      ...(runOverrides.searchRetrievalLimits || {}),
    },
    cacheBehavior: {
      ...base.cacheBehavior,
      ...(hotelOverrides.cacheBehavior || {}),
      ...(runOverrides.cacheBehavior || {}),
    },
    evidenceRequirements: {
      ...base.evidenceRequirements,
      ...(hotelOverrides.evidenceRequirements || {}),
      ...(runOverrides.evidenceRequirements || {}),
    },
    retryPolicy: {
      ...base.retryPolicy,
      ...(hotelOverrides.retryPolicy || {}),
      ...(runOverrides.retryPolicy || {}),
    },
    qualificationHandoff: {
      ...base.qualificationHandoff,
      ...(hotelOverrides.qualificationHandoff || {}),
      ...(runOverrides.qualificationHandoff || {}),
      autoSalesReady: false,
      autoPublish: false,
    },
    hotelId: opts.hotelId || null,
  };

  if (envDisabled === "0" || envDisabled === "false" || envDisabled === "off") {
    merged.enabled = false;
  }
  if (typeof opts.budgetUsd === "number") {
    merged.costCeilingUsd = opts.budgetUsd;
  }
  return merged;
}

export function listEnabledHunterIds(opts = {}) {
  return SPECIALIST_HUNTER_LIST.filter((id) => {
    const cfg = resolveHunterConfig(id, opts);
    return cfg?.enabled === true;
  });
}

export { SPECIALIST_HUNTER };
