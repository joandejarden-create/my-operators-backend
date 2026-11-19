/**
 * HE-INTEL-001 P3A — Research Method Benchmark Foundation exports.
 */

export {
  BENCHMARK_CONTRACT_ID,
  BENCHMARK_CONTRACT_VERSION,
  BENCHMARK_PHASE,
  BENCHMARK_MODE,
  BENCHMARK_ARM,
  PAID_BENCHMARK_ARMS,
  CLAIM_ACCEPTANCE,
  EVIDENCE_LAYER,
  COST_STATUS,
  P3A_POLICY,
  writeGuaranteesP3A,
  assertP3APaidBudgetZero,
} from "./contract.js";

// re-export symbols used by tests / dry-run CLI

export {
  P3A_SIX_HOTEL_SPECS,
  P3A_SIX_HPC_IDS,
  loadP3ASixHotelCohort,
  buildIndependentDiscoverySeed,
  buildIncrementalProductionSeed,
  defaultUnresolvedGaps,
} from "./cohort.js";

export { buildProviderCapabilityMatrix, PRICING_AS_OF } from "./provider-capability-matrix.js";

export {
  REQUIRED_RESEARCH_QUESTIONS,
  CLAIM_TYPES,
  buildClaimEnvelope,
  rejectInferredContact,
  coverageReport,
} from "./claim-schema.js";

export {
  RUBRIC_CRITERIA,
  PASS_THRESHOLD_NORMALIZED,
  scoreClaims,
  computeCostEfficiency,
} from "./quality-rubric.js";

export {
  blindClaimPackets,
  adjudicateBlindPackets,
  revealAdjudication,
} from "./adjudication.js";

export {
  createBenchmarkCostLedger,
  BENCHMARK_COST_LEDGER_VERSION,
} from "./cost-ledger.js";

export {
  assertBenchmarkArmAuthorized,
  createP3AProviderGateway,
  proposedMinimumPaidBenchmarkBudget,
} from "./authorization-gates.js";

export { createBenchmarkStagingStore } from "./staging-store.js";

export {
  runP3ABenchmarkDryRun,
  createIsolatedDiscoveryBags,
  buildP3BExecutionPlan,
} from "./harness.js";
