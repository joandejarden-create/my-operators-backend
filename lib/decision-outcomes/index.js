/**
 * Canonical Decision & Outcome layer — public exports.
 */

export * from "./types.js";
export {
  getDecisionOutcomesRoot,
  buildDecisionIdempotencyKey,
  createDeterministicEventId,
  listHotelsWithDecisions,
  listDecisionSummaries,
  loadEvents,
  assertHotelBoundary,
} from "./store.js";
export {
  getPersistenceMode,
  getDecisionOutcomesAirtableBaseId,
  isAirtableConfigured,
} from "./persistence.js";
export {
  createDecision,
  getDecision,
  listDecisions,
  getSubjectDecision,
  recordValidation,
  recordAction,
  recordOutcome,
  getDecisionTimeline,
  getCurrentDecisionState,
  listAllHotelDecisionSummaries,
} from "./service.js";
export { projectCurrentState, deriveLifecycle, deriveCommercialFunnelStage, projectCommercialProgression } from "./projection.js";
export {
  ensureGdiOpportunityDecision,
  ingestGdiAuthFeedback,
  ingestGdiShareValidation,
  mapFamiliarityToCanonical,
  mapCommercialToCanonical,
  mapSalesOutcomeToAction,
  mapSalesOutcomeToOutcome,
} from "./gdi-bridge.js";
export {
  ensureAdpFindingDecision,
  recordAdpHotelResponse,
  recordAdpAction,
  suggestAdpMeasurementOutcome,
} from "./adp-bridge.js";
export {
  getHotelDecisionMetrics,
  getModuleDecisionMetrics,
  getGdiPhase1Metrics,
  getAdpPhase1Metrics,
} from "./metrics.js";
