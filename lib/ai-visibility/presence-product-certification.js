/**
 * Presence product certification — fail-closed client publish gate helper.
 * Do not expose Holdout implementation details to clients.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  SIGNAL_KEYS,
  PRODUCTION_SIGNALS,
  getSignalReadiness,
  isSignalClientPublishable,
  SIGNAL_READINESS,
  evaluateSignalPublicationPlan,
} from "./signal-architecture/index.js";
import { RESOLVER_VERSION } from "./normalize-entities.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const PRESENCE_PRODUCT_CERTIFICATION_VERSION =
  "ai_intelligence_presence_product_certification_v1";

export const PRESENCE_CERTIFICATION_ARTIFACT =
  "presence-holdout-v3-one-time-score";

export function getPresenceProductionCertificationStatus() {
  const readiness = getSignalReadiness(SIGNAL_KEYS.PRESENCE);
  const status =
    readiness.productionCertificationStatus ||
    (readiness.productionReadinessAfterHoldout === SIGNAL_READINESS.VALIDATED
      ? "PRODUCTION_VALIDATED"
      : "NOT_READY");
  return {
    version: PRESENCE_PRODUCT_CERTIFICATION_VERSION,
    signalId: PRODUCTION_SIGNALS.AI_SIGNAL_PRESENCE,
    AI_SIGNAL_PRESENCE: status,
    CERTIFICATION_STATUS: status,
    resolverVersion: RESOLVER_VERSION,
    certificationArtifact: PRESENCE_CERTIFICATION_ARTIFACT,
    clientPublishable: isSignalClientPublishable(SIGNAL_KEYS.PRESENCE),
    holdoutV3Status: readiness.holdoutV3Status || null,
    holdoutV2Status: readiness.holdoutV2Status || null,
    ENABLED_WHEN_VALIDATED: [
      "AI_PRESENCE",
      "REGIONAL_PRESENCE",
      "COMPETITIVE_POSITION_PRESENCE",
      "QUESTIONS_MISSING",
      "COMPARABLE_PRESENCE_TRENDS",
    ],
    BLOCKED: [
      "RECOMMENDATION_SHARE",
      "FIRST_RECOMMENDATION_RATE",
      "QUESTIONS_WON",
      "RECOMMENDED",
      "FIRST",
      "NEGATIVE",
      "COMPARATOR",
    ],
    RECOMMENDED_STATUS: "RESEARCH_BLOCKED_NOT_PRODUCTION_READY",
    BRAND_AI_VISIBILITY_V1: "PRESENCE_LED_PRODUCTION_BUILD",
    RECOMMENDED_REQUIRED_FOR_V1: false,
  };
}

/**
 * Fail-closed: client Presence production surfaces must not claim validated
 * unless CERTIFICATION_STATUS === PRODUCTION_VALIDATED.
 */
export function assertPresenceClientPublishAllowed() {
  const cert = getPresenceProductionCertificationStatus();
  const allowed = cert.CERTIFICATION_STATUS === "PRODUCTION_VALIDATED";
  return {
    allowed,
    CERTIFICATION_STATUS: cert.CERTIFICATION_STATUS,
    reason: allowed
      ? "Presence PRODUCTION_VALIDATED"
      : "Presence certification status != PRODUCTION_VALIDATED — do not publish validated Presence surfaces",
  };
}

export function buildPresenceProductIntegrationReport() {
  const cert = getPresenceProductionCertificationStatus();
  const plan = evaluateSignalPublicationPlan();
  const publish = assertPresenceClientPublishAllowed();
  return {
    phase: "PRESENCE_PRODUCT_INTEGRATION_COMPLETE",
    CERTIFICATION_STATUS: cert.CERTIFICATION_STATUS,
    AI_SIGNAL_PRESENCE: cert.AI_SIGNAL_PRESENCE,
    resolverVersion: cert.resolverVersion,
    certificationArtifact: cert.certificationArtifact,
    clientPublishable: cert.clientPublishable,
    presenceMayPublish: plan.presenceMayPublish,
    failClosedPublishAllowed: publish.allowed,
    ENABLED: publish.allowed ? cert.ENABLED_WHEN_VALIDATED : [],
    BLOCKED: cert.BLOCKED,
    hardGuards: {
      RECOMMENDED_PRODUCTION_ENABLE: 0,
      FIRST_RECOMMENDATION_PRODUCTION_ENABLE: 0,
      NEGATIVE_PRODUCTION_ENABLE: 0,
      COMPARATOR_PRODUCTION_ENABLE: 0,
      HOLDOUT_V3_CHANGES: 0,
      HOLDOUT_V3_RESCORE: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
    },
    note: "Client surfaces must not expose Holdout implementation details. Trends return INSUFFICIENT_COMPARABLE_HISTORY when no prior comparable period.",
  };
}

export function persistPresenceProductIntegrationReport() {
  const report = buildPresenceProductIntegrationReport();
  const out = path.join(
    ROOT,
    "data/ai-visibility/validation/presence-product-integration.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n", "utf8");
  return { report, path: out };
}
