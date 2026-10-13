export { FUNNEL_STAGE, FUNNEL_DEFINITIONS } from "./funnel.js";
export { buildGdiSuccessfulOpportunityPattern } from "./success-pattern.js";
export {
  isGdiResearchLeadWorthPursuing,
  isGdiCandidateOpportunity,
  LEAD_CLASS,
  SUPPORT_SIGNAL,
} from "./research-lead-gate.js";
export {
  buildBuyerFirstQueryLibrary,
  DEMAND_MOTION_TERMS,
  BUYER_TYPE_QUERIES,
  LOCALIZED_MOTION,
} from "./demand-motion-queries.js";
export { resolveGdiDemandBuyer, BUYER_TYPE } from "./buyer-resolution.js";
export {
  validateResearchLeadPage,
  applyPageEvidenceToLead,
} from "./page-validation.js";
export { buildGdiCompetitiveDemandLead } from "./competitive-demand.js";
export {
  buildRotationRecord,
  ROTATION_TIMING_STATE,
} from "./rotation-intelligence.js";
export {
  nextBlockerForLead,
  jevAdviseResearchLead,
  runTargetedResearchSteps,
} from "./next-research.js";
export { V5_HOTELS, selectPriorityQueries } from "./hotels.js";
export { runOpportunityDiscoveryV5ForHotel, hasSerp } from "./orchestrator.js";
