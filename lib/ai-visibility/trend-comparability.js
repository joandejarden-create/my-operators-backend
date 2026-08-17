/**
 * Trend / history comparability for AI Visibility (Phase 3A.6 + controlled release).
 * Language mismatch is always non-comparable.
 * Pairwise checks required before ANY Presence delta.
 */

import {
  NON_COMPARABLE_LANGUAGE,
  normalizeLanguage,
  resolveRecordLanguage,
} from "./language-dimension.js";
import { AVAILABILITY } from "./availability-states.js";

export const TREND_COMPARABILITY_VERSION = "ai_visibility_trend_comparability_v2";

/** Invariant: trend series must never mix languages. */
export const TREND_LANGUAGE_MATCH_REQUIRED = true;

const CLIENT_NOT_COMPARABLE = "Not Comparable";
const CLIENT_INSUFFICIENT = "Insufficient History";

/**
 * Exact comparability key dimensions.
 */
export function buildTrendComparabilityKey(parts = {}) {
  return {
    version: TREND_COMPARABILITY_VERSION,
    provider: parts.provider ? String(parts.provider).toLowerCase() : null,
    geographyKey: parts.geographyKey || null,
    language: normalizeLanguage(parts.language),
    semanticPairId: parts.semanticPairId || null,
    promptFamily: parts.promptFamily || null,
    promptVersion: parts.promptVersion != null ? String(parts.promptVersion) : null,
    peerSetVersion: parts.peerSetVersion != null ? String(parts.peerSetVersion) : null,
    metricVersion: parts.metricVersion || null,
    metricDefinition: parts.metricDefinition || parts.metric || "aiPresenceRate",
    compatibleEntityUniverse: parts.compatibleEntityUniverse || null,
    methodology: parts.methodology || parts.metricVersion || null,
  };
}

/**
 * @returns {{ comparable: boolean, reasonCode: string|null, status?: string }}
 */
export function compareTrendObservations(a, b) {
  if (!a || !b) {
    return {
      comparable: false,
      reasonCode: "INSUFFICIENT_HISTORY",
      status: AVAILABILITY.INSUFFICIENT_HISTORY,
    };
  }
  const langA = resolveRecordLanguage(a, { treatMissingAsEn: true });
  const langB = resolveRecordLanguage(b, { treatMissingAsEn: true });
  if (langA !== langB) {
    return {
      comparable: false,
      reasonCode: NON_COMPARABLE_LANGUAGE,
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  if (
    String(a.provider || "").toLowerCase() !== String(b.provider || "").toLowerCase()
  ) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_PROVIDER",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  if (String(a.geographyKey || "") !== String(b.geographyKey || "")) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_GEOGRAPHY",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  const familyA = String(a.promptFamily || a.promptCohort || "").trim();
  const familyB = String(b.promptFamily || b.promptCohort || "").trim();
  if (familyA && familyB && familyA !== familyB) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_PROMPT_COHORT",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  const metricA = String(a.metricDefinition || a.metric || "aiPresenceRate");
  const metricB = String(b.metricDefinition || b.metric || "aiPresenceRate");
  if (metricA !== metricB) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_METRIC_DEFINITION",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  if (
    a.peerSetVersion != null &&
    b.peerSetVersion != null &&
    String(a.peerSetVersion) !== String(b.peerSetVersion)
  ) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_PEER_SET",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  if (
    a.metricVersion != null &&
    b.metricVersion != null &&
    String(a.metricVersion) !== String(b.metricVersion)
  ) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_METRIC_VERSION",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  if (
    a.methodology != null &&
    b.methodology != null &&
    String(a.methodology) !== String(b.methodology)
  ) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_METHODOLOGY",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  if (
    a.semanticPairId &&
    b.semanticPairId &&
    String(a.semanticPairId) !== String(b.semanticPairId)
  ) {
    return {
      comparable: false,
      reasonCode: "NON_COMPARABLE_SEMANTIC_PAIR",
      status: AVAILABILITY.NOT_COMPARABLE,
    };
  }
  return { comparable: true, reasonCode: null, status: "COMPARABLE" };
}

/**
 * Compute Presence delta only when pairwise comparable.
 * Never returns 0 / "no change" as a fallback for incompatibility.
 */
export function computeComparablePresenceDelta(prior, latest) {
  if (!prior || !latest) {
    return {
      ok: false,
      status: AVAILABILITY.INSUFFICIENT_HISTORY,
      reasonCode: "INSUFFICIENT_HISTORY",
      display: CLIENT_INSUFFICIENT,
      value: null,
    };
  }
  const cmp = compareTrendObservations(prior, latest);
  if (!cmp.comparable) {
    const status =
      cmp.status === AVAILABILITY.INSUFFICIENT_HISTORY
        ? AVAILABILITY.INSUFFICIENT_HISTORY
        : AVAILABILITY.NOT_COMPARABLE;
    return {
      ok: false,
      status,
      reasonCode: cmp.reasonCode,
      display:
        status === AVAILABILITY.INSUFFICIENT_HISTORY
          ? CLIENT_INSUFFICIENT
          : CLIENT_NOT_COMPARABLE,
      value: null,
      INVALID_DELTA_BLOCKED: true,
    };
  }
  const a = typeof prior.value === "number" ? prior.value : null;
  const b = typeof latest.value === "number" ? latest.value : null;
  if (a == null || b == null || !Number.isFinite(a) || !Number.isFinite(b)) {
    return {
      ok: false,
      status: AVAILABILITY.INSUFFICIENT_HISTORY,
      reasonCode: "INSUFFICIENT_HISTORY",
      display: CLIENT_INSUFFICIENT,
      value: null,
    };
  }
  const delta = b - a;
  const deltaPp = Math.round(delta * 1000) / 10;
  return {
    ok: true,
    status: AVAILABILITY.OBSERVED,
    delta,
    deltaPp,
    value: delta,
    display: deltaPp === 0 ? "0 pp" : `${deltaPp > 0 ? "+" : ""}${deltaPp} pp`,
    INVALID_DELTA_BLOCKED: false,
  };
}

export { NON_COMPARABLE_LANGUAGE };
