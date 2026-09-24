/**
 * GDI Jev / TypeSafe System One — config (no secrets logged).
 * Auth keys: JEZ_API_KEY | JEV_API_KEY | TYPESAFE_API_KEY
 */

export const JEV_GDI_VERSION = "gdi_jev_decision_v1.1";

export const DEFAULT_JEV_ENDPOINT = "https://api.typesafe.ai/v1/systemone";
export const DEFAULT_JEV_MODEL = "jev-1.13.0";
export const DEFAULT_JEV_TIMEOUT_MS = 12_000;
export const DEFAULT_CIRCUIT_BREAKER_ERRORS = 5;

/** Shadow is the only V1 production mode. */
export function isJevShadowMode() {
  const mode = String(process.env.GDI_JEV_MODE || "shadow").toLowerCase();
  return mode !== "apply" && mode !== "live";
}

export function isJevEnabled() {
  const flag = String(process.env.GDI_JEV_ENABLED || "1").trim();
  if (flag === "0" || flag.toLowerCase() === "false" || flag.toLowerCase() === "off") {
    return false;
  }
  return Boolean(getJevApiKey());
}

export function getJevApiKey() {
  return (
    process.env.JEZ_API_KEY ||
    process.env.JEV_API_KEY ||
    process.env.TYPESAFE_API_KEY ||
    ""
  );
}

export function getJevEndpoint() {
  return (
    process.env.JEV_ENDPOINT ||
    process.env.TYPESAFE_SYSTEMONE_URL ||
    DEFAULT_JEV_ENDPOINT
  );
}

export function getJevModel() {
  return process.env.JEV_MODEL || DEFAULT_JEV_MODEL;
}

export function getJevTimeoutMs() {
  const n = Number(process.env.JEV_TIMEOUT_MS || DEFAULT_JEV_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_JEV_TIMEOUT_MS;
}

/**
 * Decision-specific confidence floors (not one global threshold).
 * Below floor → treat as UNCERTAIN / fallback to existing GDI logic.
 */
export const JEV_THRESHOLDS = Object.freeze({
  targetRouting: 0.45,
  playbookRouting: 0.45,
  followup: 0.5,
  stop: 0.7,
  materialChange: 0.55,
  signalTriage: 0.5,
  lodgingSignal: 0.55,
  sourceUtility: 0.5,
  generatorCadence: 0.5,
  privateEventSignal: 0.55,
  opportunityPrequal: 0.65,
  venuePartnershipRouting: 0.5,
});

export function getThreshold(decisionType) {
  const map = {
    TARGET_RESEARCH_PRIORITY: JEV_THRESHOLDS.targetRouting,
    RESEARCH_PLAYBOOK: JEV_THRESHOLDS.playbookRouting,
    MATERIAL_CHANGE: JEV_THRESHOLDS.materialChange,
    FOLLOWUP_VALUE: JEV_THRESHOLDS.followup,
    FOLLOWUP_TYPE: JEV_THRESHOLDS.followup,
    SIGNAL_RELEVANCE: JEV_THRESHOLDS.signalTriage,
    LODGING_SIGNAL_STRENGTH: JEV_THRESHOLDS.lodgingSignal,
    EVENT_FORWARDNESS: JEV_THRESHOLDS.signalTriage,
    LOCAL_NO_ROOM_RISK: JEV_THRESHOLDS.lodgingSignal,
    SOURCE_UTILITY: JEV_THRESHOLDS.sourceUtility,
    GENERATOR_CADENCE: JEV_THRESHOLDS.generatorCadence,
    PRIVATE_EVENT_SIGNAL_QUALITY: JEV_THRESHOLDS.privateEventSignal,
    VENUE_PARTNERSHIP_ROUTING: JEV_THRESHOLDS.venuePartnershipRouting,
    OPPORTUNITY_PREQUAL: JEV_THRESHOLDS.opportunityPrequal,
    STOP_CONTINUE: JEV_THRESHOLDS.stop,
  };
  return map[decisionType] != null ? map[decisionType] : 0.55;
}

/** Redacted config snapshot for founder reports (no secrets). */
export function describeJevConfig() {
  const key = getJevApiKey();
  return {
    jevConfigPresent: Boolean(key),
    authMode: key ? "Bearer token via JEZ_API_KEY|JEV_API_KEY|TYPESAFE_API_KEY" : "none",
    model: getJevModel(),
    endpoint: getJevEndpoint(),
    timeoutMs: getJevTimeoutMs(),
    shadowDefault: isJevShadowMode(),
    enabled: isJevEnabled(),
    version: JEV_GDI_VERSION,
  };
}
