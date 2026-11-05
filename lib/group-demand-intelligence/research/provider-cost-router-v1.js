/**
 * Cost-aware provider routing for blocker-driven maturation.
 *
 * Providers:
 * - direct_public: free/local fetch/search (preferred when strategy is narrow)
 * - jev_route: TypeSafe System One — research *direction* only (~$0.01–0.05/call)
 * - parallel: paid async research (~$0.01–0.10+/task) — broad discovery or hard gaps
 * - surfe: named-person enrichment only (never for routing)
 *
 * Jev is NEVER completion authority — it only ranks the next bounded action.
 */

import { BLOCKER_KEY } from "./research-constants-v1.js";
import { PARALLEL_PRICE_ESTIMATES_USD } from "../parallel/budget-ledger-v1.js";

export const PROVIDER_KIND = Object.freeze({
  DIRECT_PUBLIC: "direct_public",
  JEV_ROUTE: "jev_route",
  PARALLEL: "parallel",
  SURFE: "surfe",
  NOOP: "noop",
  /** Benchmark evaluation only. Not part of BLOCKER_PROVIDER_PREFERENCE. */
  EXA: "exa",
  /** Benchmark evaluation only. Not part of BLOCKER_PROVIDER_PREFERENCE. */
  FIRECRAWL: "firecrawl",
});

/** Estimated USD ceilings used for routing comparisons (not invoices). */
export const PROVIDER_COST_ESTIMATES_USD = Object.freeze({
  [PROVIDER_KIND.DIRECT_PUBLIC]: 0,
  [PROVIDER_KIND.JEV_ROUTE]: 0.02,
  [PROVIDER_KIND.PARALLEL]: PARALLEL_PRICE_ESTIMATES_USD.base,
  [PROVIDER_KIND.SURFE]: 0.05,
  [PROVIDER_KIND.NOOP]: 0,
});

/**
 * Per-blocker routing preference when an opportunity already exists
 * (targeted follow-up / deepening).
 *
 * Order = try cheapest capable first.
 */
export const BLOCKER_PROVIDER_PREFERENCE = Object.freeze({
  [BLOCKER_KEY.FUTURE_DEMAND]: [
    PROVIDER_KIND.JEV_ROUTE, // narrow next official/calendar/prospectus action
    PROVIDER_KIND.DIRECT_PUBLIC,
    PROVIDER_KIND.PARALLEL, // only if public path exhausted
  ],
  [BLOCKER_KEY.DEMAND_SCALE]: [
    PROVIDER_KIND.JEV_ROUTE,
    PROVIDER_KIND.DIRECT_PUBLIC,
    PROVIDER_KIND.PARALLEL,
  ],
  [BLOCKER_KEY.HOTEL_FIT]: [
    PROVIDER_KIND.DIRECT_PUBLIC, // catchment often local/config
    PROVIDER_KIND.JEV_ROUTE,
    PROVIDER_KIND.PARALLEL,
  ],
  [BLOCKER_KEY.BUYER_IDENTITY]: [
    PROVIDER_KIND.DIRECT_PUBLIC, // official staff pages first
    PROVIDER_KIND.JEV_ROUTE, // contact followup type / stop — not WHO invent
    PROVIDER_KIND.PARALLEL,
    // Surfe only after named person established — handled separately
  ],
  [BLOCKER_KEY.CONTACT_ROUTE]: [
    PROVIDER_KIND.DIRECT_PUBLIC,
    PROVIDER_KIND.JEV_ROUTE,
    PROVIDER_KIND.PARALLEL,
  ],
  [BLOCKER_KEY.CORROBORATION]: [
    PROVIDER_KIND.DIRECT_PUBLIC,
    PROVIDER_KIND.JEV_ROUTE,
    PROVIDER_KIND.PARALLEL,
  ],
  [BLOCKER_KEY.SOURCE_CONFLICT]: [
    PROVIDER_KIND.JEV_ROUTE,
    PROVIDER_KIND.DIRECT_PUBLIC,
    PROVIDER_KIND.PARALLEL,
  ],
  [BLOCKER_KEY.DRAWER_COMPLETENESS]: [PROVIDER_KIND.DIRECT_PUBLIC],
});

/**
 * When no opportunity yet (segment discovery), Parallel/direct dominate;
 * Jev is not used to invent candidates.
 */
export const DISCOVERY_PROVIDER_PREFERENCE = Object.freeze([
  PROVIDER_KIND.PARALLEL,
  PROVIDER_KIND.DIRECT_PUBLIC,
]);

/**
 * Map supervisor blocker → evidence-gap dimension (for Jev EVIDENCE_GAP_NEXT_ACTION).
 */
export const BLOCKER_TO_EVIDENCE_DIMENSION = Object.freeze({
  [BLOCKER_KEY.FUTURE_DEMAND]: "FUTURE_CYCLE_VALIDATION",
  [BLOCKER_KEY.DEMAND_SCALE]: "EVENT_SCALE",
  [BLOCKER_KEY.HOTEL_FIT]: "HOTEL_FIT",
  [BLOCKER_KEY.BUYER_IDENTITY]: "WHO_IDENTITY",
  [BLOCKER_KEY.CONTACT_ROUTE]: "ACTION_PATH",
  [BLOCKER_KEY.CORROBORATION]: "SOURCE_AUTHORITY",
  [BLOCKER_KEY.SOURCE_CONFLICT]: "SOURCE_AUTHORITY",
  [BLOCKER_KEY.GEOGRAPHY_FIT]: "MARKET_VALIDATION",
});

/**
 * @param {object} opts
 * @param {string} opts.blockerKey
 * @param {boolean} [opts.opportunityExists]
 * @param {boolean} [opts.paidBlocked]
 * @param {boolean} [opts.jevEnabled]
 * @param {boolean} [opts.parallelEnabled]
 * @param {string[]} [opts.attemptedProviders] prior provider kinds for this blocker
 * @param {number} [opts.remainingBudgetUsd]
 */
export function selectProviderForBlocker({
  blockerKey,
  opportunityExists = true,
  paidBlocked = false,
  jevEnabled = true,
  parallelEnabled = true,
  attemptedProviders = [],
  remainingBudgetUsd = Infinity,
} = {}) {
  const attempted = new Set(attemptedProviders);
  const chain = opportunityExists
    ? BLOCKER_PROVIDER_PREFERENCE[blockerKey] || [
        PROVIDER_KIND.DIRECT_PUBLIC,
        PROVIDER_KIND.JEV_ROUTE,
        PROVIDER_KIND.PARALLEL,
      ]
    : DISCOVERY_PROVIDER_PREFERENCE;

  const eligible = [];
  for (const kind of chain) {
    if (attempted.has(kind)) continue;
    if (kind === PROVIDER_KIND.JEV_ROUTE && !jevEnabled) continue;
    if (kind === PROVIDER_KIND.PARALLEL && (!parallelEnabled || paidBlocked)) continue;
    if (kind === PROVIDER_KIND.SURFE && paidBlocked) continue;
    const est = PROVIDER_COST_ESTIMATES_USD[kind] ?? 0;
    // Paid-blocked means Parallel/Surfe off — Jev routing + direct_public may continue.
    // Do not starve Jev because remainingBudgetUsd was zeroed for Parallel gating.
    const budgetGatesProvider =
      kind !== PROVIDER_KIND.JEV_ROUTE && kind !== PROVIDER_KIND.DIRECT_PUBLIC;
    if (budgetGatesProvider && est > 0 && remainingBudgetUsd < est) continue;
    if (
      kind === PROVIDER_KIND.JEV_ROUTE &&
      !paidBlocked &&
      est > 0 &&
      remainingBudgetUsd < est
    ) {
      continue;
    }
    eligible.push({
      provider: kind,
      estimatedCostUsd: est,
      role:
        kind === PROVIDER_KIND.JEV_ROUTE
          ? "research_direction_only"
          : kind === PROVIDER_KIND.PARALLEL
            ? "evidence_acquisition"
            : "evidence_acquisition",
      isCompletionAuthority: false,
    });
  }

  if (!eligible.length) {
    return {
      provider: PROVIDER_KIND.NOOP,
      estimatedCostUsd: 0,
      role: "none",
      isCompletionAuthority: false,
      reason: "NO_ELIGIBLE_PROVIDER",
    };
  }

  // Preference chains are already ordered: lowest-cost capable first
  // (e.g. Jev route before Parallel; public before Jev when public alone is enough).
  // Do not re-sort by raw cost — that would pick $0 public over Jev even when
  // Jev is listed first for targeted gap routing.
  const chosen = eligible[0];

  return {
    ...chosen,
    alternatives: eligible.slice(1),
    reason: "LOWEST_COST_CAPABLE_PREFERENCE_ORDER",
  };
}

/**
 * Compare providers for a blocker — used in audits/tests.
 */
export function compareProvidersForBlocker(blockerKey, opportunityExists = true) {
  const chain = opportunityExists
    ? BLOCKER_PROVIDER_PREFERENCE[blockerKey] || []
    : DISCOVERY_PROVIDER_PREFERENCE;
  return chain.map((provider, rank) => ({
    rank: rank + 1,
    provider,
    estimatedCostUsd: PROVIDER_COST_ESTIMATES_USD[provider] ?? null,
    role:
      provider === PROVIDER_KIND.JEV_ROUTE
        ? "research_direction_only_not_completion_authority"
        : "evidence_acquisition",
  }));
}
