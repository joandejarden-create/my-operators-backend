/**
 * Fourteen-workflow provider routing for Unified Discovery Engine V7.
 * Reuses Apify V1 coverage matrix; allows evidence-based evolution.
 */

import {
  buildFourteenWorkflowApifyCoverageMatrix,
  APIFY_COVERAGE_CLASS,
} from "../research/apify/apify-fourteen-workflow-coverage-v1.js";
import { selectNextDiscoveryProvider } from "../research/apify/apify-adaptive-provider-selection-v1.js";
import { V7_PROVIDER_STRATEGY } from "./unified-discovery-engine-v7-contract.js";
import {
  NEED_DATE_V6_WAVE,
  NEED_DATE_V6_WAVE_META,
} from "./need-date-deep-discovery-v6-contract.js";

/**
 * Build routing table for all 14 workflows under a provider strategy.
 */
export function buildFourteenWorkflowProviderRoutingV7({
  providerStrategy = V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER,
} = {}) {
  const matrix = buildFourteenWorkflowApifyCoverageMatrix();
  const workflows = matrix.workflows.map((w) => {
    const recommendedSource = resolveRecommendedSource(w.classification, providerStrategy);
    const researchDepth =
      w.classification === APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY
        ? "PRIMARY_DEPTH"
        : w.classification === APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY
          ? "SUPPLEMENTAL_DEPTH"
          : "EXA_STANDARD_DEPTH";

    return {
      ...w,
      providerStrategy,
      recommendedSource,
      researchDepth,
      timingRequirements:
        w.classification === APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY &&
        (w.strategyId === "preferred_lodging" ||
          w.strategyId === "professional_event_planners")
          ? "RELATIONSHIP_OK_UNTIL_DATES_EVIDENCED"
          : "PREFER_NEED_MONTH_OVERLAP",
      buyerRequirements: "ORS_NAMED_BUYER_OR_REFERRAL_BEFORE_SALES_READY",
      expectedIncrementalValue: w.expectedIncremental,
      apifyActors: w.enabledActors,
    };
  });

  return {
    version: "gdi-unified-discovery-provider-routing-v7",
    providerStrategy,
    counts: matrix.counts,
    workflows,
  };
}

function resolveRecommendedSource(classification, providerStrategy) {
  if (providerStrategy === V7_PROVIDER_STRATEGY.EXA_ONLY) {
    return { discovery: "exa", qualify: "exa_ors" };
  }
  if (providerStrategy === V7_PROVIDER_STRATEGY.APIFY_ONLY_DISCOVERY) {
    if (classification === APIFY_COVERAGE_CLASS.EXA_PRIMARY) {
      return {
        discovery: "deferred_exa_preferred",
        qualify: "pending_ors",
        note: "Apify-only mode skips EXA_PRIMARY workflows or marks them NOT_RUN",
      };
    }
    return { discovery: "apify", qualify: "pending_ors" };
  }
  // APIFY_PLUS_EXA or ADAPTIVE
  if (classification === APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY) {
    return { discovery: "apify_then_exa", qualify: "exa_ors" };
  }
  if (classification === APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY) {
    return { discovery: "exa_primary_apify_supplement", qualify: "exa_ors" };
  }
  return { discovery: "exa", qualify: "exa_ors" };
}

/**
 * Adaptive selection for a single research objective under V7.
 */
export function selectProvidersForObjectiveV7({
  question,
  researchObjective,
  providerStrategy = V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER,
  priorProviders = [],
  blockers = [],
} = {}) {
  if (providerStrategy === V7_PROVIDER_STRATEGY.EXA_ONLY) {
    return {
      provider: "exa",
      actors: [],
      reason: "EXA_ONLY strategy",
      objectiveId: "forced_exa",
    };
  }
  if (providerStrategy === V7_PROVIDER_STRATEGY.APIFY_ONLY_DISCOVERY) {
    const sel = selectNextDiscoveryProvider({
      question: question || researchObjective,
      researchObjective,
      priorProviders,
      blockers,
    });
    return {
      ...sel,
      provider: "apify",
      qualifyDeferred: true,
      reason: `${sel.reason} (APIFY_ONLY_DISCOVERY — ORS/Exa qualify deferred)`,
    };
  }
  if (providerStrategy === V7_PROVIDER_STRATEGY.APIFY_PLUS_EXA) {
    const sel = selectNextDiscoveryProvider({
      question: question || researchObjective,
      researchObjective,
      priorProviders,
      blockers,
    });
    return {
      ...sel,
      dualPath: true,
      apifyWhenUseful: sel.provider === "apify",
      alwaysQualifyWithExaOrs: true,
    };
  }
  // ADAPTIVE_MULTI_PROVIDER
  return selectNextDiscoveryProvider({
    question: question || researchObjective,
    researchObjective,
    priorProviders,
    blockers,
  });
}

/**
 * Map V6 waves onto unified provider responsibilities.
 */
export function buildV6WaveProviderIntegrationV7() {
  return Object.values(NEED_DATE_V6_WAVE).map((wave) => {
    const meta = NEED_DATE_V6_WAVE_META[wave];
    let providers = [];
    if (wave === NEED_DATE_V6_WAVE.WAVE_A_EXISTING_INTELLIGENCE) {
      providers = ["inventory", "dedup"];
    } else if (wave === NEED_DATE_V6_WAVE.WAVE_B_BROAD_DIVERSIFIED_DISCOVERY) {
      providers = ["apify", "exa"];
    } else if (wave === NEED_DATE_V6_WAVE.WAVE_C_CANDIDATE_QUALIFICATION) {
      providers = ["commercial_screen"];
    } else if (
      wave === NEED_DATE_V6_WAVE.WAVE_D_DEEP_RESEARCH ||
      wave === NEED_DATE_V6_WAVE.WAVE_E_ADAPTIVE_GAP_RESOLUTION
    ) {
      providers = ["exa_ors"];
    } else {
      providers = ["local_gates"];
    }
    return {
      wave,
      paid: meta.paid,
      objective: meta.objective,
      providers,
      status: meta.paid ? "BLOCKED_UNTIL_FOUNDER_GO" : "READY_ZERO_PAID",
    };
  });
}
