/**
 * Group Demand Intelligence — feature flag (server-controlled).
 * Default OFF. Set GROUP_DEMAND_INTELLIGENCE_V1=1 to enable product APIs/UI gates.
 */

export const GDI_FEATURE_FLAG = "groupDemandIntelligenceV1";
export const GDI_PRODUCT_VERSION = "group-demand-intelligence-v1.0.0";
export const GDI_SCORING_VERSION = "gdi-hotel-fit-v1";
export const GDI_EVIDENCE_CONFIDENCE_VERSION = "gdi-evidence-confidence-v1";

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

export function isGroupDemandIntelligenceEnabled(env = process.env) {
  return truthy(env.GROUP_DEMAND_INTELLIGENCE_V1);
}

/** Allow localhost/demo read of seeded pilot data even when flag is off. */
export function isGroupDemandIntelligencePilotReadAllowed(env = process.env) {
  if (isGroupDemandIntelligenceEnabled(env)) return true;
  return truthy(env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ);
}

export function getGroupDemandIntelligenceFlagState(env = process.env) {
  return {
    flag: GDI_FEATURE_FLAG,
    version: GDI_PRODUCT_VERSION,
    scoringVersion: GDI_SCORING_VERSION,
    evidenceConfidenceVersion: GDI_EVIDENCE_CONFIDENCE_VERSION,
    enabled: isGroupDemandIntelligenceEnabled(env),
    pilotReadAllowed: isGroupDemandIntelligencePilotReadAllowed(env),
    envKey: "GROUP_DEMAND_INTELLIGENCE_V1",
    pilotReadEnvKey: "GROUP_DEMAND_INTELLIGENCE_PILOT_READ",
    defaultOff: true,
    experimentalLabel: "EXPERIMENTAL / PILOT",
  };
}
