/**
 * DEALALITY CENSUS AUTOPILOT V1 — public exports.
 */

export {
  AUTOPILOT_V1_VERSION,
  AUTOPILOT_V1_ARTIFACT_DIR,
  AUTOPILOT_V1_CONSTRAINTS,
  FIELD_RESOLUTION_STATUS,
  OUTPUT_CLASS,
  PRIORITY_BAND,
  SOURCE_LANE,
} from "./constants.js";

export { MODE_REGISTRY, resolveMode, listModes } from "./modes.js";
export { buildFieldRoutingRegistry, isResearchableField } from "./field-routing.js";
export { SOURCE_LANE_REGISTRY } from "./source-lanes.js";
export { scoreRecordPriority, prioritizeQueue } from "./priority-engine.js";
export { resolveFieldForRecord, resolveAllResearchableFields } from "./field-resolution.js";
export { assessCompleteness } from "./completeness.js";
export { classifyOutput } from "./output-classes.js";
export {
  buildCventDiscoveryChallenges,
  buildLegacyDiscoveryChallenges,
  buildLegacyChallengeSummaryFromOverlap,
  findMexicoCventHarvest,
  loadCventHarvestUrls,
} from "./challenge-adapters.js";
export { runCensusAutopilotV1 } from "./orchestrator.js";
export { writeAllArtifacts } from "./artifact-writer.js";
export { runDeepMexicoBenchmark } from "./deep-mexico-orchestrator.js";
export { liveDeepResearchHotel, LIVE_DEEP_VERSION } from "./live-deep-research.js";
