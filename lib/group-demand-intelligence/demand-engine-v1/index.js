/**
 * GDI Demand Engine + Jev Research Controller V1 — public exports.
 */

export {
  DEMAND_ENGINE,
  DEMAND_ENGINE_TAXONOMY,
  COVERAGE_STATE,
  defaultEnginesForHotel,
  allDemandEngines,
} from "./taxonomy.js";

export { classifyDemandEngine } from "./classify-engine.js";
export { buildGdiDemandEngineCoverage } from "./coverage.js";

export {
  RESEARCH_TRIGGER,
  SERIES_TIMING_STATE,
  WATCH_ONLY_TIMING,
  seriesMayBeValidFutureWatch,
  inferRotationPattern,
  computeNextResearchTrigger,
  buildGdiRotationIntelligence,
  extractRotationCandidatesFromOpportunities,
} from "./rotation-intelligence.js";

export {
  FIT_CLASS,
  EVIDENCE_KIND,
  evaluateTargetHotelCompeteFit,
  buildCompetitiveHotelFitHypothesis,
  buildCompetitiveDemandPattern,
  extractCompetitivePatternsFromOpportunities,
} from "./competitive-demand.js";

export {
  SIGNAL_STAGE,
  assessSignalStage,
  discoverGdiDemandSignals,
} from "./signal-discovery.js";

export {
  buildDeterministicGapRecommendations,
  runJevGapAnalysis,
} from "./jev-gap-analysis.js";

export {
  CONTROLLER_ACTION,
  decideResearchControllerDeterministic,
  runJevGdiResearchController,
} from "./jev-research-controller.js";

// Re-export next-best from V3 for single import surface
export {
  getGdiNextBestResearchAction,
  isPromisingNearMiss,
} from "../discovery-expansion-v3/next-best-research.js";
