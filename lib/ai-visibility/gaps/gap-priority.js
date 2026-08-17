/**
 * Gap persistence + priority classification (P0C).
 * No numeric confidence scores.
 */

export const GAP_PERSISTENCE = Object.freeze([
  "SINGLE",
  "EMERGING",
  "REPEATED",
  "STRONGLY_REPEATED",
]);

export const GAP_CLASSIFICATION = Object.freeze([
  "MONITOR",
  "REVIEW",
  "PRIORITY",
  "HIGH_PRIORITY",
]);

export const GAP_LIFECYCLE = Object.freeze([
  "ACTIVE",
  "IMPROVING",
  "RESOLVED",
  "INSUFFICIENT_DATA",
  "NOT_COMPARABLE",
]);

export const TREND_STATUS = Object.freeze([
  "CURRENT_GAP",
  "TREND_CHANGE",
  "INSUFFICIENT_HISTORY",
]);

/**
 * Classify persistence from observation counts across providers/variants/periods.
 * @param {{ observationCount?: number, providers?: string[], variants?: string[], periods?: string[] }} stats
 */
export function classifyGapPersistence(stats = {}) {
  const count = stats.observationCount || 0;
  const providers = new Set(stats.providers || []).size;
  const variants = new Set(stats.variants || []).size;
  const periods = new Set(stats.periods || []).size;

  if (count <= 0) return "SINGLE";
  if (providers >= 3 && periods >= 2 && variants >= 2) return "STRONGLY_REPEATED";
  if (providers >= 2 || periods >= 2 || variants >= 2) return "REPEATED";
  if (count >= 2) return "EMERGING";
  return "SINGLE";
}

/**
 * Approved COMMERCIAL_PRIORITY × GAP_PERSISTENCE matrix.
 * @param {string} commercialPriority CRITICAL|HIGH|STANDARD|INVESTIGATION
 * @param {string} persistence SINGLE|EMERGING|REPEATED|STRONGLY_REPEATED
 * @returns {string|null} classification or null when no production opportunity
 */
export function classifyGapPriority(commercialPriority, persistence) {
  const cp = String(commercialPriority || "STANDARD").toUpperCase();
  const p = String(persistence || "SINGLE").toUpperCase();

  const matrix = {
    CRITICAL: {
      SINGLE: "MONITOR",
      EMERGING: "REVIEW",
      REPEATED: "PRIORITY",
      STRONGLY_REPEATED: "HIGH_PRIORITY",
    },
    HIGH: {
      SINGLE: "MONITOR",
      EMERGING: "MONITOR",
      REPEATED: "REVIEW",
      STRONGLY_REPEATED: "PRIORITY",
    },
    STANDARD: {
      SINGLE: "MONITOR",
      EMERGING: "MONITOR",
      REPEATED: "MONITOR",
      STRONGLY_REPEATED: "REVIEW",
    },
    INVESTIGATION: {
      SINGLE: null,
      EMERGING: "REVIEW",
      REPEATED: "REVIEW",
      STRONGLY_REPEATED: "PRIORITY",
    },
  };

  return matrix[cp]?.[p] ?? (cp === "INVESTIGATION" ? null : "MONITOR");
}

/**
 * @param {number} periodCount
 */
export function classifyTrendStatus(periodCount) {
  if (!periodCount || periodCount < 2) return "INSUFFICIENT_HISTORY";
  return "CURRENT_GAP";
}
