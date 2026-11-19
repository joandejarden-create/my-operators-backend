/**
 * HE-INTEL-001 P3B-M1 readiness + live activation exports.
 */

export {
  P3B_M1_CONTRACT_ID,
  P3B_M1_CONTRACT_VERSION,
  P3B_M1_PHASE,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
  P3B_M1_POLICY,
  writeGuaranteesP3BM1,
  resolveP3BM1FounderPaidAuthorization,
} from "./contract.js";

export {
  P3B_M1_HOTEL_SPECS,
  P3B_M1_HPC_IDS,
  loadP3BM1PilotCohort,
} from "./cohort.js";

export {
  probeConfiguredProviderAvailability,
  buildM1GatewayAdapterMap,
  createMockPaidTransports,
  createExaGatewayAdapter,
  createParallelSearchGatewayAdapter,
  createParallelTaskGatewayAdapter,
  createNativeGatewayAdapter,
} from "./adapters.js";

export {
  createM1SharedBudgetLedger,
  M1_SHARED_BUDGET_LEDGER_VERSION,
} from "./shared-budget-ledger.js";

export {
  createDurableM1BudgetLedger,
  DURABLE_BUDGET_LEDGER_VERSION,
  OP_STATUS,
} from "./durable-budget-ledger.js";

export {
  buildP3BM1PlannedOperations,
  buildProposedExecutionCommand,
} from "./planned-operations.js";

export {
  isSafeHttpUrl,
  validateSourceUrl,
  normalizeAndFilterNewIntelligence,
  PRIORITY_OBJECTIVES,
} from "./evidence.js";

export {
  buildLiveCapableTransports,
  createExaLiveTransport,
  createParallelLiveTransport,
  probeAccountPricingPresence,
  LIVE_TRANSPORT_GATE,
} from "./live-transports.js";

export {
  scoreHotelMethodResult,
  factsToClassifierInput,
  buildComparableReportSkeleton,
  recommendResearchApproach,
  EXPERIMENT_SCORING_VERSION,
} from "./experiment-scoring.js";

export {
  toDiscoveryCandidate,
  independentlyAcceptOwnershipClaim,
  extractParallelTaskDiscovery,
  DISCOVERY_CANDIDATE_KIND,
} from "./discovery-candidates.js";

export {
  LEDGER_FATAL,
  acquireFileLock,
} from "./durable-budget-ledger.js";

export {
  runP3BM1LiveActivationValidation,
  runPhaseASafetySuite,
  buildAuthorizedExecutionCommand,
  createP3BM1LiveActivationGateway,
  LIVE_ACTIVATION_PHASE,
} from "./live-activation.js";

export { runP3BM1ReadinessValidation, createP3BM1Gateway } from "./harness.js";
