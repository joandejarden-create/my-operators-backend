/**
 * Contact Resolution V2 — public barrel.
 */

export * from "./vocabulary.js";
export * from "./domain-resolver.js";
export * from "./website-research.js";
export * from "./email-resolution.js";
export * from "./phone-resolution.js";
export * from "./gates.js";
export * from "./country-methods.js";
export * from "./research-trace.js";
export * from "./webhound-escalation.js";
export * from "./web-retrieval.js";
export * from "./roles-and-sources.js";
export * from "./cache.js";
export { resolve_owner_contacts, CONTACT_RESOLUTION_V2 } from "./resolve-owner-contacts.js";
export {
  loadRecoveredBaselineForOwner,
  listBaselineRecoveryOwnerIds,
  getKnownGoldenDomain,
  BASELINE_RECOVERY_VERSION,
} from "./baseline-evidence-recovery.js";
export { mapDiscoveryResultToV2, CI_ORCHESTRATION_BRIDGE_VERSION } from "./ci-orchestration-bridge.js";
export { loadTeacherPatternRegistry } from "./webhound-escalation.js";
