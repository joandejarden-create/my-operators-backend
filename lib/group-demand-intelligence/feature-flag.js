/**
 * Group Demand Intelligence — feature flag (server-controlled).
 *
 * Production default: OFF (explicit GROUP_DEMAND_INTELLIGENCE_V1=1 required).
 * Local / non-Railway servers: pilot read is ON automatically so localhost
 * always shows GDI unless PILOT_READ is explicitly set to 0/false/off.
 */

export const GDI_FEATURE_FLAG = "groupDemandIntelligenceV1";
export const GDI_PRODUCT_VERSION = "group-demand-intelligence-v1.0.0";
export const GDI_SCORING_VERSION = "gdi-hotel-fit-v1";
export const GDI_EVIDENCE_CONFIDENCE_VERSION = "gdi-evidence-confidence-v1";

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function explicitlyFalse(raw) {
  if (raw == null || String(raw).trim() === "") return false;
  const v = String(raw).trim().toLowerCase();
  return v === "0" || v === "false" || v === "no" || v === "off";
}

/**
 * Local server detection — not Railway production.
 * Classification: REUSABLE_PRODUCT_LOGIC (env gating only).
 */
export function isGroupDemandIntelligenceLocalDev(env = process.env) {
  const railwayEnv = String(env.RAILWAY_ENVIRONMENT || "").trim().toLowerCase();
  if (railwayEnv === "production") return false;
  if (truthy(env.RAILWAY_PRODUCTION)) return false;

  const nodeEnv = String(env.NODE_ENV || "").trim().toLowerCase();
  if (nodeEnv === "development" || nodeEnv === "test" || nodeEnv === "") {
    // Unset NODE_ENV is common for `node server.js` / local npm start.
    if (!railwayEnv && !env.RAILWAY_PROJECT_ID) return true;
    if (nodeEnv === "development" || nodeEnv === "test") return true;
  }

  // Non-production Railway preview/staging: keep explicit flags (do not auto-on).
  if (railwayEnv) return false;

  return !env.RAILWAY_PROJECT_ID;
}

export function isGroupDemandIntelligenceEnabled(env = process.env) {
  return truthy(env.GROUP_DEMAND_INTELLIGENCE_V1);
}

/**
 * Allow read of seeded pilot data even when V1 product flag is off.
 * Local servers default ON; set GROUP_DEMAND_INTELLIGENCE_PILOT_READ=0 to force off.
 */
export function isGroupDemandIntelligencePilotReadAllowed(env = process.env) {
  if (isGroupDemandIntelligenceEnabled(env)) return true;
  if (explicitlyFalse(env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ)) return false;
  if (truthy(env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ)) return true;
  return isGroupDemandIntelligenceLocalDev(env);
}

export function getGroupDemandIntelligenceFlagState(env = process.env) {
  const localDev = isGroupDemandIntelligenceLocalDev(env);
  return {
    flag: GDI_FEATURE_FLAG,
    version: GDI_PRODUCT_VERSION,
    scoringVersion: GDI_SCORING_VERSION,
    evidenceConfidenceVersion: GDI_EVIDENCE_CONFIDENCE_VERSION,
    enabled: isGroupDemandIntelligenceEnabled(env),
    pilotReadAllowed: isGroupDemandIntelligencePilotReadAllowed(env),
    localDevAutoPilotRead: localDev,
    envKey: "GROUP_DEMAND_INTELLIGENCE_V1",
    pilotReadEnvKey: "GROUP_DEMAND_INTELLIGENCE_PILOT_READ",
    defaultOff: true,
    experimentalLabel: "EXPERIMENTAL / PILOT",
  };
}
