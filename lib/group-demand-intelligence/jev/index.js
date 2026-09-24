/**
 * GDI Jev / TypeSafe System One — shared decision layer (shadow V1.1).
 */

export * from "./jev-config.js";
export * from "./jev-types.js";
export * from "./jev-client.js";
export * from "./jev-policy-adapter.js";
export * from "./jev-observability.js";
export * from "./jev-choice-filter.js";
export * from "./jev-adjudication.js";
export * from "./jev-cohort.js";
export * from "./jev-airtable-audit.js";
export {
  decide,
  decideMany,
  sanitizeJevState,
  enrichDecisionContext,
  getJevServiceInfo,
} from "./jev-decision-service.js";
