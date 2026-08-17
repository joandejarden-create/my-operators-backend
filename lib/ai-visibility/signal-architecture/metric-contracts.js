/**
 * Locked metric contracts under signal/flag architecture.
 * Do not expand Recommendation Share to associated_option.
 */

import { POSITIVE_RECOMMENDATION_ROLES } from "../metrics.js";
import { SIGNAL_KEYS } from "./production-signals.js";

export const METRIC_CONTRACT_VERSION = "ai_intelligence_metric_contracts_signal_v1";

export const METRIC_CONTRACTS = Object.freeze({
  AI_PRESENCE: Object.freeze({
    name: "AI Presence Rate",
    formula:
      "successful eligible responses where PRESENCE = true / successful eligible responses",
    drivingSignal: SIGNAL_KEYS.PRESENCE,
    unchanged: true,
  }),
  RECOMMENDATION_SHARE: Object.freeze({
    name: "Recommendation Share",
    formula:
      "comparable governed responses where AI_SIGNAL_RECOMMENDED=TRUE / comparable governed responses (after Recommended certification)",
    drivingSignal: SIGNAL_KEYS.RECOMMENDED,
    positiveRoles: [...POSITIVE_RECOMMENDATION_ROLES],
    excludes: Object.freeze(["associated_option"]),
    doNotExpandToAssociated: true,
    blockedUntilRecommendedCertification: true,
    futureNote:
      "After Recommended certification under definition lock v1, share numerator follows AI_SIGNAL_RECOMMENDED (includes affirmative consideration/shortlist). Until then keep blocked; do not expand associated_option into live share.",
    unchanged: true,
  }),
  FIRST_RECOMMENDATION: Object.freeze({
    name: "First Recommendation Rate",
    formula: "FIRST_RECOMMENDATION flag / successful eligible responses (as defined in metrics)",
    drivingSignal: SIGNAL_KEYS.FIRST_RECOMMENDATION,
    unchanged: true,
  }),
  QUESTIONS_WON: Object.freeze({
    name: "Questions Won",
    formula: "strict first recommendation leader",
    drivingSignal: SIGNAL_KEYS.FIRST_RECOMMENDATION,
    unchanged: true,
  }),
  QUESTIONS_MISSING: Object.freeze({
    name: "Questions Missing",
    formula: "entity absent (PRESENCE = false)",
    drivingSignal: SIGNAL_KEYS.PRESENCE,
    unchanged: true,
  }),
  COMPETITIVE_POSITION: Object.freeze({
    name: "Competitive Position",
    formula: "AI Presence Rate rank among peers",
    drivingSignal: SIGNAL_KEYS.PRESENCE,
    unchanged: true,
  }),
});

export function confirmMetricContractsUnchanged() {
  const all = Object.values(METRIC_CONTRACTS);
  return {
    version: METRIC_CONTRACT_VERSION,
    confirmedUnchanged: all.every((c) => c.unchanged === true),
    recommendationShareExcludesAssociated:
      METRIC_CONTRACTS.RECOMMENDATION_SHARE.doNotExpandToAssociated === true &&
      !METRIC_CONTRACTS.RECOMMENDATION_SHARE.positiveRoles.includes("associated_option"),
    contracts: METRIC_CONTRACTS,
  };
}
