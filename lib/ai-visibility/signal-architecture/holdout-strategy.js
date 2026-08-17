/**
 * Holdout strategy under signal architecture.
 * Presence holdout has been executed (see presence-holdout-validation.json).
 * Recommended / First remain DEV workstreams — do not use holdout for their tuning.
 */

import { SIGNAL_KEYS } from "./production-signals.js";
import { DEV_SIGNAL_VALIDATION_SNAPSHOT, SIGNAL_READINESS } from "./readiness.js";

export const HOLDOUT_STRATEGY_VERSION = "ai_intelligence_signal_holdout_strategy_v1";

export function evaluateHoldoutReadiness(opts = {}) {
  const snap = opts.snapshot || DEV_SIGNAL_VALIDATION_SNAPSHOT;
  const presence = snap.signals?.[SIGNAL_KEYS.PRESENCE] || {};
  const presenceHoldoutPass = presence.holdoutStatus === "PASS";
  const presenceProductionValidated =
    presence.productionReadinessAfterHoldout === SIGNAL_READINESS.VALIDATED;

  return {
    version: HOLDOUT_STRATEGY_VERSION,
    PRESENCE_READY_FOR_HOLDOUT: "NO",
    PRESENCE_HOLDOUT_STATUS: presence.holdoutV1Status || presence.holdoutStatus || "NOT_RUN",
    PRESENCE_HOLDOUT_V2_STATUS: presence.holdoutV2Status || "NOT_CREATED",
    PRESENCE_HOLDOUT_V3_STATUS: presence.holdoutV3Status || "NOT_CREATED",
    RECOMMENDED_READY_FOR_HOLDOUT: "NO",
    FIRST_READY_FOR_HOLDOUT: "NO",
    HOLDOUT_ACCESSED: snap.HOLDOUT_ACCESSED ? "YES" : "NO",
    HOLDOUT_EXECUTED: snap.HOLDOUT_ACCESSED === true,
    PRESENCE_PRODUCTION_VALIDATED: presenceProductionValidated,
    authorizationRequired: true,
    note:
      "Holdout v1 = INSPECTED_DIAGNOSTIC. Holdout v2 = SCORED_FAIL (never retune). Holdout v3 = PASS / PRODUCTION_VALIDATED. Do not rescore Holdout v3. Do not treat Holdout v3 as unseen for future tuning.",
  };
}
