/**
 * GDI 2.0 Phase 2C — optional enforce-on-new-publication policy DESIGN.
 * Default OFF. Must not be enabled without separate founder authorization.
 */

export const PUBLICATION_ENFORCEMENT_POLICY_DESIGN_V1 =
  "gdi-publication-enforcement-policy-design-v1";

export const PUBLICATION_ENFORCEMENT_DEFAULT = Object.freeze({
  enforceOnNewPublication: false,
  envFlag: "GDI_ENFORCE_SHARED_ELIGIBILITY_ON_NEW_PUBLICATION_V1",
  hotelOptInEnvPrefix: "GDI_ENFORCE_SHARED_ELIGIBILITY_HOTEL_",
});

export const PUBLICATION_ENFORCEMENT_DESIGN = Object.freeze({
  version: PUBLICATION_ENFORCEMENT_POLICY_DESIGN_V1,
  defaultOff: true,
  eligiblePathways: [
    "scaling_companies FullE2E selectPublicationCandidates (NEW promotions only)",
    "association_events hotel runner qualification → staged publication (NEW only)",
  ],
  requiredCommercialChecks: [
    "evaluateSharedCommercialEligibility.salesReady === true",
    "buyer responsibility supported (unknown role fails)",
    "realistic hotel demand thesis",
    "usable contact route (org route OK if labeled)",
    "sources traceable",
    "separate authorizePublication founder flag still required",
  ],
  legacyCompatibility: {
    preserveVisibilityBooleans: true,
    reviewedProspectPackages: "do_not_auto_demote",
    legacyVisibilityPreserved: "do_not_auto_demote",
    inventoryVisibility: "do_not_auto_demote",
    wRomePilot: "do_not_auto_demote",
    note: "Enforcement applies to NEW publication promotions, not historical customer-visible sets.",
  },
  hotelOptIn: {
    required: true,
    mechanism: "env allowlist or roster publicationAuthorized ∩ opt-in set",
  },
  shadowToEnforcementComparison: {
    require: "EXISTING_READY_SHARED_NOT count audited per hotel",
    gate: "founder review of disagreement samples before flipping flag",
  },
  rollback: {
    steps: [
      "Set enforce flag OFF",
      "Keep shadow evaluation ON",
      "Do not rewrite historical records",
      "Re-run publication invariants offline",
    ],
  },
  auditLogging: {
    fields: [
      "opportunityId",
      "hotelId",
      "existingSalesReady",
      "sharedSalesReady",
      "disagreement.code",
      "rootCauses",
      "enforceDecision",
      "authorizedPublication",
    ],
    redactPii: true,
  },
  unchangedVisibilityGuarantees: [
    "isCustomerFacingOpportunity boolean semantics unchanged by default",
    "Eligibility does not grant visibility",
    "Authorization does not equal eligibility",
  ],
});

export function isSharedEligibilityEnforcementEnabled(env = process.env, hotelId = "") {
  const globalOn =
    String(env[PUBLICATION_ENFORCEMENT_DEFAULT.envFlag] || "0").toLowerCase() === "1" ||
    String(env[PUBLICATION_ENFORCEMENT_DEFAULT.envFlag] || "").toLowerCase() === "true";
  if (!globalOn) {
    return {
      enabled: false,
      reason: "default_off",
      designVersion: PUBLICATION_ENFORCEMENT_POLICY_DESIGN_V1,
    };
  }
  // Even if env is set during Phase 2C, refuse enablement in code paths that import this helper
  // until a future phase wires enforcement behind an explicit approved release.
  return {
    enabled: false,
    reason: "phase2c_design_only_refuses_enablement",
    envGlobalRequested: true,
    hotelId: hotelId || null,
    designVersion: PUBLICATION_ENFORCEMENT_POLICY_DESIGN_V1,
    note: "Policy designed; not authorized to enforce in Phase 2C.",
  };
}
