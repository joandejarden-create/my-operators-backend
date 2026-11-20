/**
 * Factory stage names + default sequences.
 */

export const HIF_STAGES_V1 = "hotel-intelligence-factory-stages-v1";

export const HIF_STAGE = Object.freeze({
  CONFIG: "CONFIG",
  DISCOVER: "DISCOVER",
  EXTRACT: "EXTRACT",
  VERIFY: "VERIFY",
  RECONCILE: "RECONCILE",
  PERSIST_RESEARCH: "PERSIST_RESEARCH",
  MANIFEST: "MANIFEST",
  PROMOTE: "PROMOTE",
  READBACK: "READBACK",
  QUEUE_HE: "QUEUE_HE",
  REPORT: "REPORT",
});

export const DEFAULT_RESEARCH_STAGES = Object.freeze([
  HIF_STAGE.CONFIG,
  HIF_STAGE.DISCOVER,
  HIF_STAGE.EXTRACT,
  HIF_STAGE.VERIFY,
  HIF_STAGE.RECONCILE,
  HIF_STAGE.REPORT,
]);

export const DEFAULT_PROMOTION_STAGES = Object.freeze([
  HIF_STAGE.CONFIG,
  HIF_STAGE.RECONCILE,
  HIF_STAGE.MANIFEST,
  HIF_STAGE.REPORT,
]);

export function parseStages(csvOrArray, fallback) {
  if (Array.isArray(csvOrArray) && csvOrArray.length) {
    return csvOrArray.map((s) => String(s).trim().toUpperCase()).filter(Boolean);
  }
  if (typeof csvOrArray === "string" && csvOrArray.trim()) {
    return csvOrArray
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
  }
  return [...(fallback || DEFAULT_RESEARCH_STAGES)];
}

export default { HIF_STAGE, DEFAULT_RESEARCH_STAGES, DEFAULT_PROMOTION_STAGES, parseStages };
