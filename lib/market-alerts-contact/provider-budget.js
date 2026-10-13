/**
 * Per-run Surfe/provider call budget for Market Alerts contact enrichment.
 * Caps never throw — callers stop cleanly with CONTACT_PROVIDER_RUN_CAP_REACHED.
 */

import { getContactEnrichmentLimits } from "./eligibility.js";
import { logContactOp } from "./safe-log.js";

export const CONTACT_PROVIDER_CAP_REASON = "CONTACT_PROVIDER_RUN_CAP_REACHED";

/**
 * @returns {{
 *   searchesAttempted: number,
 *   enrichmentsAttempted: number,
 *   totalProviderCalls: number,
 *   searchesAvoidedNamedDirect: number,
 *   callsAvoidedCache: number,
 *   capReached: boolean,
 *   capReason: string|null,
 *   remaining: { searches: number, enrichments: number, total: number },
 *   tryReserveSearch: () => boolean,
 *   tryReserveEnrich: () => boolean,
 *   recordNamedDirectAvoidedSearch: () => void,
 *   recordCacheAvoided: (callsAvoided?: number) => void,
 *   snapshot: () => object,
 * }}
 */
export function createProviderRunBudget(overrides = {}) {
  const limits = { ...getContactEnrichmentLimits(), ...overrides };
  let searchesAttempted = 0;
  let enrichmentsAttempted = 0;
  let searchesAvoidedNamedDirect = 0;
  let callsAvoidedCache = 0;
  let capReached = false;
  let capReason = null;

  function remaining() {
    return {
      searches: Math.max(0, limits.maxSearchesPerRun - searchesAttempted),
      enrichments: Math.max(0, limits.maxEnrichmentsPerRun - enrichmentsAttempted),
      total: Math.max(0, limits.maxProviderCallsPerRun - (searchesAttempted + enrichmentsAttempted)),
    };
  }

  function markCap(which) {
    capReached = true;
    capReason = CONTACT_PROVIDER_CAP_REASON;
    logContactOp("provider_run_cap_reached", {
      which,
      searchesAttempted,
      enrichmentsAttempted,
      totalProviderCalls: searchesAttempted + enrichmentsAttempted,
      remaining: remaining(),
      searchesAvoidedNamedDirect,
      callsAvoidedCache,
    });
  }

  function tryReserveSearch() {
    if (capReached) return false;
    if (searchesAttempted >= limits.maxSearchesPerRun) {
      markCap("searches");
      return false;
    }
    if (searchesAttempted + enrichmentsAttempted >= limits.maxProviderCallsPerRun) {
      markCap("total");
      return false;
    }
    searchesAttempted += 1;
    return true;
  }

  function tryReserveEnrich() {
    if (capReached) return false;
    if (enrichmentsAttempted >= limits.maxEnrichmentsPerRun) {
      markCap("enrichments");
      return false;
    }
    if (searchesAttempted + enrichmentsAttempted >= limits.maxProviderCallsPerRun) {
      markCap("total");
      return false;
    }
    enrichmentsAttempted += 1;
    return true;
  }

  return {
    get searchesAttempted() {
      return searchesAttempted;
    },
    get enrichmentsAttempted() {
      return enrichmentsAttempted;
    },
    get totalProviderCalls() {
      return searchesAttempted + enrichmentsAttempted;
    },
    get searchesAvoidedNamedDirect() {
      return searchesAvoidedNamedDirect;
    },
    get callsAvoidedCache() {
      return callsAvoidedCache;
    },
    get capReached() {
      return capReached;
    },
    get capReason() {
      return capReason;
    },
    get remaining() {
      return remaining();
    },
    tryReserveSearch,
    tryReserveEnrich,
    recordNamedDirectAvoidedSearch() {
      searchesAvoidedNamedDirect += 1;
    },
    recordCacheAvoided(callsAvoided = 2) {
      callsAvoidedCache += Math.max(0, Number(callsAvoided) || 0);
    },
    snapshot() {
      return {
        searchesAttempted,
        enrichmentsAttempted,
        totalProviderCalls: searchesAttempted + enrichmentsAttempted,
        searchesAvoidedNamedDirect,
        callsAvoidedCache,
        capReached,
        capReason,
        remaining: remaining(),
        limits: {
          maxSearchesPerRun: limits.maxSearchesPerRun,
          maxEnrichmentsPerRun: limits.maxEnrichmentsPerRun,
          maxProviderCallsPerRun: limits.maxProviderCallsPerRun,
        },
      };
    },
  };
}

/** Module-scoped default budget for a single Node process run (optional). */
let activeBudget = null;

export function beginProviderRunBudget(overrides) {
  activeBudget = createProviderRunBudget(overrides);
  return activeBudget;
}

export function getActiveProviderRunBudget() {
  return activeBudget;
}

export function endProviderRunBudget() {
  const snap = activeBudget?.snapshot() || null;
  activeBudget = null;
  return snap;
}
