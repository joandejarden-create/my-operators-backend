/**
 * Dealality Competitive Moat Architecture V1 — SHOW THE BENCHMARK / HIDE THE ENGINE
 */

export * from "./blocked-signals.js";
export * from "./canonical-intent.js";
export * from "./observation-ledger-schema.js";
export * from "./observed-competitive-set.js";
export * from "./emerging-competitor.js";
export * from "./benchmark-engine-v1.js";
export * from "./customer-payload.js";
export * from "./internal-payload.js";
export * from "./access-redaction.js";
export * from "./info-contracts.js";
export * from "./feasibility-audit.js";
export * from "./internal-benchmark-expansion-audit.js";
export * from "./approved-internal-additions.js";
export * from "./presence-corpus.js";
export * from "./presence-re-extraction.js";
export * from "./contextual-cohort-v1.js";
export * from "./brand-presence-index-pilot.js";
export * from "./brand-benchmark-read-service.js";
export * from "./benchmark-cohort-integrity-audit.js";
export * from "./benchmark-brand-ids.js";
export * from "./benchmark-eligible-universe.js";
export * from "./scenario-peer-eligibility.js";
export * from "./intersection-grains.js";
export * from "./benchmark-cohort-validity-v2.js";
export * from "./benchmark-cohort-remediation.js";
export * from "./scenario-benchmark-validation.js";
export * from "./scenario-benchmark-composition.js";
export * from "./period-scoped-grain.js";
export * from "./period-response-sources.js";
export * from "./scenario-benchmark-longitudinal-recertification.js";
export * from "./owner-intent-chg-vs-prior.js";

export const MOAT_ARCHITECTURE_VERSION = "dealality_competitive_moat_v1";

export const MOAT_LAYERS = Object.freeze({
  LAYER_1_MEASUREMENT_CORPUS: "PROPRIETARY_MEASUREMENT_CORPUS",
  LAYER_2_BENCHMARK_ENGINE: "PROPRIETARY_BENCHMARK_ENGINE",
  LAYER_3_CUSTOMER_BENCHMARK: "CONTROLLED_CUSTOMER_BENCHMARK",
  LAYER_4_CUSTOMER_INTELLIGENCE: "CUSTOMER_INTELLIGENCE",
});

export const CORE_PRINCIPLES = Object.freeze({
  SHOW_THE_BENCHMARK: true,
  HIDE_THE_BENCHMARK_ENGINE: true,
  TRANSPARENT_INSIGHT: true,
  PROPRIETARY_MECHANICS: true,
  VALIDATED_SIGNALS_ONLY: true,
});
