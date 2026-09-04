/**
 * Canonical BAI Prior Run period resolver V1.
 *
 * BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER
 * BAI_UNPROMOTED_PERIOD_ISOLATION
 * BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY
 *
 * Customer-published reads stay anchored to DEMO_VALIDATION / 2026-08-14 until
 * an explicit founder promotion flips the published pointer.
 *
 * For Period 2 customer comparison (internal QA + promotion preview), the one
 * governed Prior Run is DEMO_VALIDATION / 2026-08-14 — never Aug 18 Period 1.
 */

import { PRIMARY_BASELINE_DATE } from "./baseline-audit.js";
import {
  BRAND_LONGITUDINAL_STORE_ROOT,
  listMeasurementPeriodManifests,
  qualifyMeasurementPeriod,
  readMeasurementPeriodManifest,
} from "./measurement-period.js";
import { normalizeMeasurementDate } from "./grain.js";
import { BASELINE_MEASUREMENT_PERIOD } from "../competitive-moat/period-scoped-grain.js";
import {
  listGovernedMeasurementPeriods,
  selectCurrentAndPriorPeriods,
} from "../competitive-moat/owner-intent-chg-vs-prior.js";

export const BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER =
  "BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER";
export const BAI_UNPROMOTED_PERIOD_ISOLATION =
  "BAI_UNPROMOTED_PERIOD_ISOLATION";
export const BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY =
  "BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY";
export const BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY =
  "BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY";

export const BAI_VIEW_MODE = Object.freeze({
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
  INTERNAL_CANDIDATE_LONGITUDINAL_QA: "INTERNAL_CANDIDATE_LONGITUDINAL_QA",
  /** Internal-only simulation of post-promotion customer experience. Never share. */
  CUSTOMER_PROMOTION_PREVIEW: "CUSTOMER_PROMOTION_PREVIEW",
});

/** Certified but unpromoted Period 2 — never customer-published until founder GO. */
export const BAI_PERIOD_2_CANDIDATE_ID =
  "aiv_brand_longitudinal_period_20260902_d3d713";

/** Governed customer-facing current date for Period 2 (certified artifact). */
export const BAI_PERIOD_2_CUSTOMER_CURRENT_DATE = "2026-09-03";

/**
 * First multi-parent longitudinal period — INTERNAL HISTORY ONLY.
 * Must not appear in the customer P2 Prior Run / Trends story.
 */
export const BAI_PERIOD_1_LONGITUDINAL_ID =
  "aiv_brand_longitudinal_period_20260818_6579d2";
export const BAI_PERIOD_1_INTERNAL_HISTORY_DATE = "2026-08-18";

/** Historical Aug 14 federated baseline — Prior Run after Period 2 publication. */
export const BAI_HISTORICAL_AUG14_PERIOD_ID = BASELINE_MEASUREMENT_PERIOD;
export const BAI_HISTORICAL_AUG14_DATE = PRIMARY_BASELINE_DATE;

/**
 * Live customer-published current (founder-authorized Period 2 publication).
 * Prior Run remains DEMO_VALIDATION / 2026-08-14.
 */
export const BAI_CUSTOMER_PUBLISHED_PERIOD_ID = BAI_PERIOD_2_CANDIDATE_ID;
export const BAI_CUSTOMER_PUBLISHED_DATE = BAI_PERIOD_2_CUSTOMER_CURRENT_DATE;

/** Canonical Prior Run for Period 2 customer comparison. */
export const BAI_P2_CUSTOMER_PRIOR_PERIOD_ID = BAI_HISTORICAL_AUG14_PERIOD_ID; // DEMO_VALIDATION
export const BAI_P2_CUSTOMER_PRIOR_DATE = BAI_HISTORICAL_AUG14_DATE; // 2026-08-14

/** @deprecated alias — prefer BAI_HISTORICAL_AUG14_* after publication */
export const BAI_CUSTOMER_ROLLBACK_PERIOD_ID = BAI_HISTORICAL_AUG14_PERIOD_ID;
export const BAI_CUSTOMER_ROLLBACK_DATE = BAI_HISTORICAL_AUG14_DATE;

export const BAI_COHORT_CHANGE_CUSTOMER_DISCLOSURE =
  "Competitive comparisons reflect the current monitored peer set. The peer universe changed since the prior run, so rank and relative movement should be interpreted directionally.";

export const BAI_PROVIDER_NONCOMPARABILITY_CUSTOMER_DISCLOSURE =
  "Change vs Prior Run is not comparable for this monitoring pair.";

export const BAI_INTENT_NONCOMPARABILITY_CUSTOMER_DISCLOSURE =
  "Intent-level change is not yet comparable for this monitoring pair.";

export const BAI_PERIOD_PUBLICATION_STATE = Object.freeze({
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
  CERTIFIED_UNPROMOTED: "CERTIFIED_UNPROMOTED",
  NOT_FOUND: "NOT_FOUND",
});

/**
 * @param {string|null} periodId
 * @returns {"CUSTOMER_PUBLISHED"|"CERTIFIED_UNPROMOTED"|"NOT_FOUND"}
 */
export function getBaiPeriodPublicationState(periodId) {
  if (!periodId) return BAI_PERIOD_PUBLICATION_STATE.NOT_FOUND;
  if (periodId === BAI_CUSTOMER_PUBLISHED_PERIOD_ID) {
    return BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED;
  }
  // Aug 14 baseline is historical prior after P2 publication — not live current.
  if (
    periodId === BAI_HISTORICAL_AUG14_PERIOD_ID ||
    periodId === "DEMO_VALIDATION"
  ) {
    return BAI_PERIOD_PUBLICATION_STATE.CERTIFIED_UNPROMOTED;
  }
  if (periodId === BAI_PERIOD_2_CANDIDATE_ID) {
    // Same id as published current after founder GO.
    return BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED;
  }
  const manifest = readMeasurementPeriodManifest(periodId);
  if (!manifest) return BAI_PERIOD_PUBLICATION_STATE.NOT_FOUND;
  const q = qualifyMeasurementPeriod(manifest);
  if (q.valid) return BAI_PERIOD_PUBLICATION_STATE.CERTIFIED_UNPROMOTED;
  return BAI_PERIOD_PUBLICATION_STATE.NOT_FOUND;
}

/**
 * Governed P2 ↔ Aug 14 customer prior identity (single object).
 * Used by Wave 3/4, promotion preview, and future promotion.
 */
export function buildBaiP2CustomerPriorIdentityV1(opts = {}) {
  const viewMode = opts.viewMode || BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA;
  const published =
    BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID;
  return {
    ok: true,
    gate: BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY,
    noMixedPriorGate: BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY,
    viewMode,
    currentPeriodId: BAI_PERIOD_2_CANDIDATE_ID,
    currentPeriodDate: BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
    priorPeriodId: BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
    priorPeriodDate: BAI_P2_CUSTOMER_PRIOR_DATE,
    comparable: true,
    comparabilityReason: "P2_VS_DEMO_VALIDATION_AUG14_CERTIFIED_COMPARE",
    methodologyLineage:
      "peer:peers_uu_collection_lifestyle_owner_decision_v3|cert:period_scoped|prior:DEMO_VALIDATION_FEDERATED_BASELINE",
    cohortChanged: true,
    cohortChangeState: "YES",
    cohortCompatibility: "COHORT_CHANGED_PEER_V2_TO_V3",
    cohortChangeDisclosure: BAI_COHORT_CHANGE_CUSTOMER_DISCLOSURE,
    providerCompatibility: "PERIOD_SCOPED_PANEL_CURRENT_ONLY",
    publicationState: published
      ? BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED
      : BAI_PERIOD_PUBLICATION_STATE.CERTIFIED_UNPROMOTED,
    period2Exposed: true,
    forbiddenPriorPeriodId: BAI_PERIOD_1_LONGITUDINAL_ID,
    forbiddenPriorDate: BAI_PERIOD_1_INTERNAL_HISTORY_DATE,
    note:
      "Customer P2 Prior Run is DEMO_VALIDATION / 2026-08-14 only. Aug 18 Period 1 is internal history, not the customer prior.",
  };
}

function isP2Current(periodId) {
  return String(periodId || "") === BAI_PERIOD_2_CANDIDATE_ID;
}

/**
 * Canonical resolver — the only place UI/API should pick prior periods.
 *
 * @param {object} opts
 * @param {"CUSTOMER_PUBLISHED"|"INTERNAL_CANDIDATE_LONGITUDINAL_QA"|"CUSTOMER_PROMOTION_PREVIEW"} [opts.viewMode]
 * @param {string} [opts.geography]
 * @param {string} [opts.currentPeriodId] — honored in internal / preview modes
 * @param {string} [opts.storeRoot]
 */
export function resolveBaiPriorComparablePeriodV1(opts = {}) {
  const viewMode = opts.viewMode || BAI_VIEW_MODE.CUSTOMER_PUBLISHED;
  const geography = opts.geography || "CALA";
  const storeRoot = opts.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT;

  if (viewMode === BAI_VIEW_MODE.CUSTOMER_PUBLISHED) {
    // Founder-authorized: Period 2 is live customer current; Prior Run = Aug 14.
    if (BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID) {
      const identity = buildBaiP2CustomerPriorIdentityV1({ viewMode });
      return {
        ...identity,
        gate: BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
        isolationGate: BAI_UNPROMOTED_PERIOD_ISOLATION,
        viewMode,
        geography,
        knownLongitudinalPeriods: listMeasurementPeriodManifests(storeRoot).map(
          (m) => m.measurementPeriodId
        ),
        note:
          "Customer-published Brand AI is Period 2 (2026-09-03) with Prior Run DEMO_VALIDATION (2026-08-14).",
      };
    }
    return {
      ok: true,
      gate: BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
      isolationGate: BAI_UNPROMOTED_PERIOD_ISOLATION,
      viewMode,
      currentPeriodId: BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
      currentPeriodDate: BAI_CUSTOMER_PUBLISHED_DATE,
      priorPeriodId: null,
      priorPeriodDate: null,
      comparable: false,
      comparabilityReason: "CUSTOMER_PUBLISHED_NO_PROMOTED_PRIOR",
      methodologyLineage: "DEMO_VALIDATION_FEDERATED_BASELINE",
      cohortCompatibility: "N/A",
      cohortChanged: false,
      cohortChangeState: "N/A",
      cohortChangeDisclosure: null,
      providerCompatibility: "N/A",
      publicationState: BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED,
      period2Exposed: false,
      note:
        "Customer-published Brand AI remains on the federated Aug 14 baseline.",
    };
  }

  if (
    viewMode !== BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA &&
    viewMode !== BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW
  ) {
    return {
      ok: false,
      gate: BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
      reason: "unsupported_view_mode",
      viewMode,
      period2Exposed: false,
    };
  }

  const requestedCurrent =
    opts.currentPeriodId || BAI_PERIOD_2_CANDIDATE_ID;

  // Historical Aug 14 baseline alone has no prior.
  if (
    requestedCurrent === BAI_HISTORICAL_AUG14_PERIOD_ID ||
    requestedCurrent === "DEMO_VALIDATION"
  ) {
    return {
      ok: true,
      gate: BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
      isolationGate: BAI_UNPROMOTED_PERIOD_ISOLATION,
      viewMode,
      currentPeriodId: BAI_HISTORICAL_AUG14_PERIOD_ID,
      currentPeriodDate: BAI_HISTORICAL_AUG14_DATE,
      priorPeriodId: null,
      priorPeriodDate: null,
      comparable: false,
      comparabilityReason: "BASELINE_HAS_NO_PRIOR",
      methodologyLineage: "DEMO_VALIDATION_FEDERATED_BASELINE",
      cohortCompatibility: "N/A",
      cohortChanged: false,
      cohortChangeState: "N/A",
      cohortChangeDisclosure: null,
      providerCompatibility: "N/A",
      publicationState: getBaiPeriodPublicationState(requestedCurrent),
      period2Exposed: false,
    };
  }

  // Founder decision: P2 customer story always uses Aug 14 DEMO_VALIDATION prior.
  if (isP2Current(requestedCurrent)) {
    const identity = buildBaiP2CustomerPriorIdentityV1({ viewMode });
    return {
      ...identity,
      gate: BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
      isolationGate: BAI_UNPROMOTED_PERIOD_ISOLATION,
      knownLongitudinalPeriods: listMeasurementPeriodManifests(storeRoot).map(
        (m) => m.measurementPeriodId
      ),
      geography,
    };
  }

  // Non-P2 internal periods: fall back to governed chain (may include Aug 18).
  const periods = listGovernedMeasurementPeriods({ storeRoot, geography });
  const selected = selectCurrentAndPriorPeriods({
    periods,
    geography,
    currentPeriodId: requestedCurrent,
    anchorToLiveCurrent: true,
  });

  const current = selected.currentPeriod || null;
  const prior = selected.priorPeriod || null;
  const currentManifest =
    current?.measurementPeriodId &&
    current.measurementPeriodId !== BAI_CUSTOMER_PUBLISHED_PERIOD_ID
      ? readMeasurementPeriodManifest(current.measurementPeriodId, storeRoot)
      : null;

  const sameContract =
    current && prior
      ? String(current.measurementContractKey || "") ===
        String(prior.measurementContractKey || "")
      : false;

  return {
    ok: Boolean(current),
    gate: BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
    isolationGate: BAI_UNPROMOTED_PERIOD_ISOLATION,
    viewMode,
    currentPeriodId: current?.measurementPeriodId || requestedCurrent,
    currentPeriodDate:
      current?.measurementDate ||
      normalizeMeasurementDate(currentManifest?.completedAt) ||
      null,
    priorPeriodId: prior?.measurementPeriodId || null,
    priorPeriodDate: prior?.measurementDate || null,
    comparable: Boolean(current && prior),
    comparabilityReason: current && prior ? "COMPARABLE" : "NO_PRIOR",
    methodologyLineage:
      current?.measurementContractKey ||
      currentManifest?.metricVersion ||
      null,
    cohortCompatibility: sameContract
      ? "COMPATIBLE"
      : prior
        ? "CONTRACT_MISMATCH_OR_UNKNOWN"
        : "NO_PRIOR",
    cohortChanged: !sameContract && Boolean(prior),
    cohortChangeState: !sameContract && prior ? "YES" : "NO",
    cohortChangeDisclosure:
      !sameContract && prior ? BAI_COHORT_CHANGE_CUSTOMER_DISCLOSURE : null,
    providerCompatibility: "PERIOD_SCOPED_PANEL",
    publicationState: getBaiPeriodPublicationState(
      current?.measurementPeriodId || requestedCurrent
    ),
    period2Exposed: false,
    knownLongitudinalPeriods: listMeasurementPeriodManifests(storeRoot).map(
      (m) => m.measurementPeriodId
    ),
  };
}

/**
 * Fail-closed guard: share / customer surfaces must never resolve candidate Period 2
 * or promotion preview.
 */
export function assertBaiCustomerPublicationIsolation(viewMode, reportScope) {
  const scope = reportScope || "current_published";
  if (scope !== "current_published") {
    return {
      ok: false,
      gate: BAI_UNPROMOTED_PERIOD_ISOLATION,
      reason: "share_scope_not_current_published",
    };
  }
  if (
    viewMode === BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA ||
    viewMode === BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW
  ) {
    return {
      ok: false,
      gate: BAI_UNPROMOTED_PERIOD_ISOLATION,
      reason: "candidate_or_preview_mode_forbidden_on_customer_or_share",
    };
  }
  return {
    ok: true,
    gate: BAI_UNPROMOTED_PERIOD_ISOLATION,
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
  };
}

/**
 * NO-WRITE dry-run of the future promotion pointer flip.
 * Does not mutate any registry / env / artifact.
 */
export function dryRunBaiP2PromotionMechanicsV1() {
  const alreadyPublished =
    BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID;
  return {
    ok: true,
    LIVE_MUTATION: false,
    PROMOTION_PERFORMED: alreadyPublished,
    wouldChange: {
      publishedPointer: {
        from: BAI_HISTORICAL_AUG14_PERIOD_ID,
        to: BAI_PERIOD_2_CANDIDATE_ID,
        file: "lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js",
        symbols: [
          "BAI_CUSTOMER_PUBLISHED_PERIOD_ID",
          "BAI_CUSTOMER_PUBLISHED_DATE",
          "resolveBaiPriorComparablePeriodV1 CUSTOMER_PUBLISHED branch",
        ],
      },
      customerCurrentDate: {
        from: BAI_HISTORICAL_AUG14_DATE,
        to: BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
      },
      customerPrior: {
        priorPeriodId: BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
        priorPeriodDate: BAI_P2_CUSTOMER_PRIOR_DATE,
      },
      evidenceBinding: {
        currentEvidencePeriodId: BAI_PERIOD_2_CANDIDATE_ID,
        priorEvidencePeriodId: BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
      },
      assetCacheTokens: [
        "public/ai-visibility-brand.html script/css ?v=",
        "public/brand-ai-visibility-share.html script/css ?v=",
      ],
      expectedFingerprints:
        "reports/bai-p2-promotion-readiness/hypothetical-customer-fingerprints.json",
      rollbackTarget: {
        currentPeriodId: BAI_CUSTOMER_ROLLBACK_PERIOD_ID,
        currentPeriodDate: BAI_CUSTOMER_ROLLBACK_DATE,
        priorPeriodId: null,
      },
    },
    mustNotChange: [
      "Period 2 period store / hashes",
      "methodology / peer sets / benchmark math",
      "parent-share security / token scopes",
      "Aug 18 Period 1 history artifacts",
    ],
  };
}
