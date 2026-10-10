/**
 * GDI Discovery Expansion V3 public exports.
 */

export {
  MARKET_LANGUAGE_PROFILES,
  resolveGdiMarketLanguages,
  languagesForScoutPass,
} from "./market-languages.js";

export {
  CANONICAL_INTENTS,
  buildLocalizedQuery,
  buildLocalizedQueryMatrix,
} from "./localized-intents.js";

export {
  SERIES_TIMING_STATE,
  WATCH_ONLY_TIMING,
  classifySeriesTimingState,
  seriesMayBeValidFutureWatch,
  buildSeriesRecord,
} from "./association-series.js";

export {
  SCOUT_FAMILY,
  buildScoutQueryPlan,
  defaultScoutPriorityForHotel,
  extractHiddenDemandCandidates,
  corporateTriggerImpliesLodgingMotion,
} from "./scouts.js";

export {
  getGdiNextBestResearchAction,
  isPromisingNearMiss,
  RESEARCH_BLOCKER,
} from "./next-best-research.js";

export {
  normalizeDiscoveryKey,
  opportunityDedupFingerprint,
  dedupeCrossLanguageCandidates,
} from "./cross-language-dedup.js";

export { runDiscoveryExpansionV3ForHotel } from "./orchestrator.js";
