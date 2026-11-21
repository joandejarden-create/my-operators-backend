/**
 * Factory stage names + default sequences.
 */

export const HIF_STAGES_V1 = "hotel-intelligence-factory-stages-v1";

export const HIF_STAGE = Object.freeze({
  CONFIG: "CONFIG",
  DISCOVER: "DISCOVER",
  DISCOVER_EXPAND: "DISCOVER_EXPAND",
  EXHIBIT_HARVEST: "EXHIBIT_HARVEST",
  EXTRACT: "EXTRACT",
  VERIFY: "VERIFY",
  ENRICH_CONTRACT_TERMS: "ENRICH_CONTRACT_TERMS",
  /** Optional — Exa Agent deep verification; not in default high-volume sequences */
  EXA_AGENT_DEEP_VERIFICATION: "EXA_AGENT_DEEP_VERIFICATION",
  /** Optional — Jev typed research escalation (shadow by default; never writes facts) */
  JEV_RESEARCH_ROUTING: "JEV_RESEARCH_ROUTING",
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
  HIF_STAGE.ENRICH_CONTRACT_TERMS,
  HIF_STAGE.RECONCILE,
  HIF_STAGE.REPORT,
]);

/** V5 expansion sequence — opt-in only; not in default research sequence. */
export const DISCOVERY_EXPANSION_STAGES = Object.freeze([
  HIF_STAGE.CONFIG,
  HIF_STAGE.DISCOVER_EXPAND,
  HIF_STAGE.EXHIBIT_HARVEST,
  HIF_STAGE.EXTRACT,
  HIF_STAGE.VERIFY,
  HIF_STAGE.ENRICH_CONTRACT_TERMS,
  HIF_STAGE.RECONCILE,
  HIF_STAGE.PERSIST_RESEARCH,
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