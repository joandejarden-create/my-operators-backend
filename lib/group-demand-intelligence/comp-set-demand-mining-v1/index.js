export {
  COMP_SET_TARGET_HOTELS,
  resolveCanonicalCompSet,
  applyPublicIdentity,
} from "./resolve-comp-set.js";
export {
  buildFormattedPhoneVariants,
  extractPublicPhoneFromText,
  digitsOnly,
} from "./phone-variants.js";
export {
  buildCompSetPublicSearchPivots,
  selectPriorityPivots,
} from "./search-pivots.js";
export {
  EVIDENCE_CLASS,
  classifyCompetitorUseEvidence,
  canFeedDeeperOpportunityResearch,
} from "./evidence-class.js";
export {
  validateCompDemandPage,
  buildCompetitorDemandTrace,
} from "./page-validate.js";
export {
  buildCompSetRepeatPattern,
  REPEAT_CADENCE,
} from "./repeat-pattern.js";
export { buildCrossCompetitorPatterns } from "./cross-competitor.js";
export {
  buildTargetHotelThesis,
  deriveWinAngles,
  FIT_CLASS,
} from "./target-thesis.js";
export {
  patternEligibleForGdiCandidate,
  patternToGdiDraft,
  classifyCompDemandGdiCandidate,
  resolveBuyerForPattern,
} from "./gdi-conversion.js";
export {
  runCompSetDemandMiningForHotel,
  hasSerp,
} from "./orchestrator.js";
