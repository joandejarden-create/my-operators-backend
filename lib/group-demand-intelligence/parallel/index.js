/**
 * GDI Parallel provider integration — public exports.
 * Production must not import from gdi-parallel-lab; use fixtures + these modules.
 */

export * from "./feature-flag.js";
export * from "./claim-evidence-v1.js";
export * from "./identity-dedupe-v1.js";
export * from "./budget-ledger-v1.js";
export * from "./job-store-v1.js";
export * from "./contact-channel-policy-v1.js";
export * from "./fixture-pack-v1.js";
export {
  reconcileSanDiegoReviewSet,
  SAN_DIEGO_RECONCILE_VERSION,
} from "./san-diego-reconcile-v1.js";
