/**
 * AI Demand Positioning — Package entry point.
 */
export { buildScenarioUniverse, getScenarioById, getScenariosByIntent } from "./prompt-universe/scenario-registry.js";
export { executeMonitoringPeriod, estimateCost } from "./execution/multi-provider-runner.js";
export { parseObservation, parsePeriodObservations } from "./execution/response-parser.js";
export { computeDemandCaptureIndex } from "./intelligence/demand-capture-index.js";
export { computeLostDemand } from "./intelligence/lost-demand.js";
export { computeCompetitiveSet } from "./intelligence/competitive-set.js";
export { computeRealityGap } from "./intelligence/reality-gap.js";
export { computeWhiteSpace } from "./intelligence/white-space.js";
export { buildOwnerPayload } from "./customer/owner-payload.js";
export { loadPropertyProfile, loadLatestPeriod, savePeriod, PROVIDERS } from "./data-model.js";
