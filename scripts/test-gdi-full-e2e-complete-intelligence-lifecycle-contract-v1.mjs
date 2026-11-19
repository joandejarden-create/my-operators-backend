#!/usr/bin/env node
/**
 * Certify founder one-run complete-intelligence lifecycle contract ($0).
 */
import assert from "node:assert/strict";
import {
  GDI_FULL_E2E_COMPLETE_INTELLIGENCE_LIFECYCLE_CONTRACT_V1,
  INTELLIGENCE_DIMENSION_OUTCOME,
  FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS,
  FULL_E2E_PRODUCTION_EXECUTION_TARGET,
  SLICE_2A_LIFECYCLE_CONTRACT_SCOPE,
  buildEmptyIntelligenceLifecycleLedger,
  evaluateDurableCompletionClaim,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-complete-intelligence-lifecycle-contract-v1.js";

assert.equal(
  FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS.length,
  12,
  "founder listed 12 lifecycle dimensions",
);
assert.equal(
  FULL_E2E_PRODUCTION_EXECUTION_TARGET.model,
  "ONE_HOTEL_ONE_INTEGRATED_FULL_E2E_RUN",
);
assert.equal(
  FULL_E2E_PRODUCTION_EXECUTION_TARGET.weeklyChangeRules
    .computedEveryApplicableRun,
  true,
);
assert.equal(
  FULL_E2E_PRODUCTION_EXECUTION_TARGET.persistenceRules
    .durableCompletionRequiresSuccessfulPersistence,
  true,
);
assert.equal(SLICE_2A_LIFECYCLE_CONTRACT_SCOPE.expandsCurrentPrIntoAllPillars, false);
assert.equal(SLICE_2A_LIFECYCLE_CONTRACT_SCOPE.productionWritesAuthorized, false);
assert.equal(SLICE_2A_LIFECYCLE_CONTRACT_SCOPE.paidResearchAuthorized, false);

const ledger = buildEmptyIntelligenceLifecycleLedger({
  hotelId: "recLuxvwwxID7U2B8",
  runId: "test-lifecycle-ledger",
});
assert.equal(ledger.version, GDI_FULL_E2E_COMPLETE_INTELLIGENCE_LIFECYCLE_CONTRACT_V1);
assert.equal(ledger.durableCompletionClaimAllowed, false);

for (const dim of FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS) {
  assert.ok(ledger.dimensions[dim.id]);
  assert.equal(
    ledger.dimensions[dim.id].outcome,
    INTELLIGENCE_DIMENSION_OUTCOME.NOT_YET_WIRED,
  );
}

const blocked = evaluateDurableCompletionClaim(ledger);
assert.equal(blocked.durableCompletionClaimAllowed, false);
assert.ok(blocked.blockers.some((b) => b.code === "NOT_YET_WIRED"));

// Settling all dimensions to VERIFIED / REUSED unlocks claim in the evaluator.
const settled = buildEmptyIntelligenceLifecycleLedger({ runId: "settled" });
for (const dim of FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS) {
  settled.dimensions[dim.id].attempted = true;
  settled.dimensions[dim.id].outcome =
    dim.id === "reuse_existing_durable_intelligence"
      ? INTELLIGENCE_DIMENSION_OUTCOME.REUSED
      : INTELLIGENCE_DIMENSION_OUTCOME.VERIFIED;
  settled.dimensions[dim.id].verified = true;
}
const ok = evaluateDurableCompletionClaim(settled);
assert.equal(ok.durableCompletionClaimAllowed, true);
assert.equal(ok.blockers.length, 0);

// Persistence failure blocks durable completion.
settled.dimensions.durable_persistence_create_or_update.outcome =
  INTELLIGENCE_DIMENSION_OUTCOME.PERSISTENCE_FAILED;
settled.dimensions.durable_persistence_create_or_update.verified = false;
const persistFail = evaluateDurableCompletionClaim(settled);
assert.equal(persistFail.durableCompletionClaimAllowed, false);
assert.ok(
  persistFail.blockers.some(
    (b) =>
      b.code === "PERSISTENCE_FAILED" || b.code === "PERSISTENCE_NOT_SUCCESSFUL",
  ),
);

console.log(
  JSON.stringify(
    {
      ok: true,
      version: GDI_FULL_E2E_COMPLETE_INTELLIGENCE_LIFECYCLE_CONTRACT_V1,
      dimensions: FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS.length,
      productionModel: FULL_E2E_PRODUCTION_EXECUTION_TARGET.model,
    },
    null,
    2,
  ),
);
