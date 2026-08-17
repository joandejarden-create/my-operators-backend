/**
 * Brand AI Visibility product surface audit under signal architecture.
 * Classification only — no full UI redesign in this phase.
 */

import { SIGNAL_KEYS } from "./production-signals.js";

export const SURFACE_CLASS = Object.freeze({
  SAFE_NOW: "SAFE_NOW",
  BLOCKED_BY_RECOMMENDED_FLAG: "BLOCKED_BY_RECOMMENDED_FLAG",
  BLOCKED_BY_FIRST_REC_FLAG: "BLOCKED_BY_FIRST_REC_FLAG",
  INTERNAL_ONLY: "INTERNAL_ONLY",
});

/**
 * Component / metric surface → publication class.
 */
export const BRAND_AI_VISIBILITY_SURFACE_AUDIT = Object.freeze({
  aiPresence: {
    label: "AI Presence",
    class: SURFACE_CLASS.SAFE_NOW,
    signal: SIGNAL_KEYS.PRESENCE,
  },
  competitivePosition: {
    label: "Competitive Position (by Presence)",
    class: SURFACE_CLASS.SAFE_NOW,
    signal: SIGNAL_KEYS.PRESENCE,
  },
  regionalPresence: {
    label: "Regional Presence",
    class: SURFACE_CLASS.SAFE_NOW,
    signal: SIGNAL_KEYS.PRESENCE,
  },
  presenceTrends: {
    label: "Presence trends",
    class: SURFACE_CLASS.SAFE_NOW,
    signal: SIGNAL_KEYS.PRESENCE,
    note: "SAFE_NOW where enough validated periods exist",
  },
  aiPresenceChange: {
    label: "AI Presence change (pp)",
    class: SURFACE_CLASS.SAFE_NOW,
    signal: SIGNAL_KEYS.PRESENCE,
  },
  questionsMissing: {
    label: "Questions Missing",
    class: SURFACE_CLASS.SAFE_NOW,
    signal: SIGNAL_KEYS.PRESENCE,
    note: "SAFE when driven only by Presence (absence)",
  },
  recommendationShare: {
    label: "Recommendation Share",
    class: SURFACE_CLASS.BLOCKED_BY_RECOMMENDED_FLAG,
    signal: SIGNAL_KEYS.RECOMMENDED,
  },
  recommendationRate: {
    label: "Recommendation Rate",
    class: SURFACE_CLASS.BLOCKED_BY_RECOMMENDED_FLAG,
    signal: SIGNAL_KEYS.RECOMMENDED,
  },
  top3RecommendationRate: {
    label: "Top-3 Recommendation Rate",
    class: SURFACE_CLASS.BLOCKED_BY_RECOMMENDED_FLAG,
    signal: SIGNAL_KEYS.RECOMMENDED,
  },
  firstRecommendationRate: {
    label: "First Recommendation Rate",
    class: SURFACE_CLASS.BLOCKED_BY_FIRST_REC_FLAG,
    signal: SIGNAL_KEYS.FIRST_RECOMMENDATION,
  },
  questionsWon: {
    label: "Questions Won",
    class: SURFACE_CLASS.BLOCKED_BY_FIRST_REC_FLAG,
    signal: SIGNAL_KEYS.FIRST_RECOMMENDATION,
  },
  internal10ClassRecommendationStatus: {
    label: "Internal 10-class recommendationStatus",
    class: SURFACE_CLASS.INTERNAL_ONLY,
    signal: null,
    note: "Research/audit/debug — not client contract",
  },
  negativeOrQualifiedSignal: {
    label: "Negative / Qualified signal",
    class: SURFACE_CLASS.INTERNAL_ONLY,
    signal: SIGNAL_KEYS.NEGATIVE_OR_QUALIFIED,
    note: "Not client-published until volume + gate pass",
  },
  comparatorSignal: {
    label: "Comparator signal",
    class: SURFACE_CLASS.INTERNAL_ONLY,
    signal: SIGNAL_KEYS.COMPARATOR,
    note: "Not client-published until volume + gate pass",
  },
});

export function summarizeProductSurfaceAudit() {
  const SAFE_NOW = [];
  const BLOCKED = [];
  const INTERNAL_ONLY = [];
  for (const [id, row] of Object.entries(BRAND_AI_VISIBILITY_SURFACE_AUDIT)) {
    const entry = { id, ...row };
    if (row.class === SURFACE_CLASS.SAFE_NOW) SAFE_NOW.push(entry);
    else if (row.class === SURFACE_CLASS.INTERNAL_ONLY) INTERNAL_ONLY.push(entry);
    else BLOCKED.push(entry);
  }
  return { SAFE_NOW, BLOCKED, INTERNAL_ONLY };
}
