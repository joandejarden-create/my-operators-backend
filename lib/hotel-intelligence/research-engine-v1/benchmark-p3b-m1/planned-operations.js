/**
 * Exact planned paid operations for P3B-M1 (not authorized for execution).
 */

import { P3B_M1_HARD_CAP_USD, P3B_M1_PER_HOTEL_CAP_USD } from "./contract.js";
import { P3B_M1_HPC_IDS as COHORT_IDS } from "./cohort.js";

export function buildP3BM1PlannedOperations() {
  const hotels = [...COHORT_IDS];
  const perHotel = {
    EXA: {
      search_lite: { maxRequests: 8, unitUsd: 0.004, maxUsd: 0.032 },
      contents: { maxRequests: 5, unitUsd: 0.001, maxUsd: 0.005 },
    },
    PARALLEL_SEARCH: {
      search_fast: { maxRequests: 6, unitUsd: 0.001, maxUsd: 0.006 },
    },
    PARALLEL_TASK: {
      task_core: { maxRequests: 1, unitUsd: 0.025, maxUsd: 0.025 },
      task_pro_optional: {
        maxRequests: 1,
        unitUsd: 0.1,
        maxUsd: 0.1,
        conditional: "only_if_core_fails_quality_gate",
      },
    },
    DEALALITY_NATIVE: {
      research_loop: {
        maxRequests: 8,
        unitUsd: null,
        maxUsd: null,
        usdStatus: "UNKNOWN",
        creditCap: 8,
      },
    },
  };

  const hotelPlans = hotels.map((hpcId) => {
    const estProvider =
      perHotel.EXA.search_lite.maxUsd +
      perHotel.EXA.contents.maxUsd +
      perHotel.PARALLEL_SEARCH.search_fast.maxUsd +
      perHotel.PARALLEL_TASK.task_core.maxUsd;
    return {
      hpcId,
      maxRequests: {
        EXA: perHotel.EXA.search_lite.maxRequests + perHotel.EXA.contents.maxRequests,
        PARALLEL_SEARCH: perHotel.PARALLEL_SEARCH.search_fast.maxRequests,
        PARALLEL_TASK: 1,
        DEALALITY_NATIVE: perHotel.DEALALITY_NATIVE.research_loop.maxRequests,
      },
      expectedProviderCostUsd: Number(estProvider.toFixed(6)),
      expectedModelTokenCostUsd: null,
      expectedModelTokenCostStatus: "UNKNOWN",
      expectedOrchestrationCostUsd: 0,
      perHotelCapUsd: P3B_M1_PER_HOTEL_CAP_USD,
      withinPerHotelCap: estProvider <= P3B_M1_PER_HOTEL_CAP_USD,
      operations: perHotel,
    };
  });

  const expectedMaxProviderUsd = Number(
    (hotelPlans.reduce((s, h) => s + h.expectedProviderCostUsd, 0)).toFixed(6)
  );
  const optionalProUsd = Number((0.1 * hotels.length).toFixed(6));

  return {
    envelopeId: "P3B_M1_THREE_HOTEL",
    authorized: false,
    hardCapUsd: P3B_M1_HARD_CAP_USD,
    perHotelCapUsd: P3B_M1_PER_HOTEL_CAP_USD,
    hotels,
    hotelPlans,
    totals: {
      expectedMaxProviderUsd,
      optionalParallelProAltUsd: optionalProUsd,
      expectedMaxWithOptionalProUsd: Number(
        (expectedMaxProviderUsd - 0.025 * hotels.length + optionalProUsd).toFixed(6)
      ),
      modelTokenUsdStatus: "UNKNOWN",
      orchestrationUsd: 0,
      hardCapUsd: P3B_M1_HARD_CAP_USD,
      headroomUsd: Number((P3B_M1_HARD_CAP_USD - expectedMaxProviderUsd).toFixed(6)),
    },
    retriesDefault: 0,
    boundedRetrieval: {
      exa: "lite search ≤10 results; contents ≤5 URLs; no agent unless separately budgeted",
      parallelSearch: "fast/turbo only",
      parallelTask: "core preferred; pro optional",
    },
    forbidden: ["SURFE", "PDL", "inferred_email_as_verified"],
  };
}

export function buildProposedExecutionCommand() {
  return {
    dryRunReadiness:
      "npm run dry-run:he-intel-001-p3b-m1-readiness",
    paidExecutionBlockedUntilAuth: true,
    proposedPaidCommand:
      "npm run run:he-intel-001-p3b-m1-live -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5",
    note: "Paid command must refuse without founder auth; do not run until separate authorization.",
  };
}
