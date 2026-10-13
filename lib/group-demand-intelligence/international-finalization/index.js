/**
 * GDI International Finalization Engine V1 — public API.
 */

export {
  IFE_VERSION,
  PILLAR_STRENGTH,
  TARGET_HOTEL_FIT,
  SELECTION_STATUS,
  SELECTION_MODEL,
  FINAL_DISPOSITION,
  EVIDENCE_ASK_CATEGORY,
} from "./constants.js";

export { auditMissingPillars, isFinalizationEligible } from "./missing-pillar-audit.js";
export { evaluateTargetHotelFit, assertFitNotCityOnly } from "./target-hotel-fit.js";
export {
  buildHotelSelectionProcess,
  fromLodgingDecisionIntelligenceRow,
  normalizeSelectionModel,
  normalizeSelectionStatus,
} from "./hotel-selection-process.js";
export { buildLodgingDecisionState } from "./lodging-decision.js";
export { buildEvidenceRequest, computeFinalizationNextBlocker } from "./evidence-request.js";
export { generateEvidenceOutreachDraft } from "./outreach-drafts.js";
export {
  mapHotelSuppliedResponseToFacts,
  requalifyAfterHotelSuppliedResponse,
} from "./hotel-supplied-response-map.js";
export { recomputePacketQuality, computeFinalDisposition } from "./packet-recompute.js";
export { buildCustomerFinalizationCard } from "./customer-card.js";
export { finalizeCandidate, getFinalizationArchitectureStatus } from "./finalization-engine.js";
