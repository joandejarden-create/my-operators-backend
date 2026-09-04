/**
 * Canonical BAI Prior Run period resolver V1.
 *
 * BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER
 * BAI_UNPROMOTED_PERIOD_ISOLATION
 *
 * Customer-published reads stay anchored to DEMO_VALIDATION / 2026-08-14.
 * Unpromoted Period 2 is available only in INTERNAL_CANDIDATE_LONGITUDINAL_QA.
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

export const BAI_VIEW_MODE = Object.freeze({
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
  INTERNAL_CANDIDATE_LONGITUDINAL_QA: "INTERNAL_CANDIDATE_LONGITUDINAL_QA",
});

/** Certified but unpromoted Period 2 — never customer-published. */
export const BAI_PERIOD_2_CANDIDATE_ID =
  "aiv_brand_longitudinal_period_20260902_d3d713";

/** First multi-parent longitudinal period (optional prior chain). */
export const BAI_PERIOD_1_LONGITUDINAL_ID =
  "aiv_brand_longitudinal_period_20260818_6579d2";

export const BAI_CUSTOMER_PUBLISHED_PERIOD_ID = BASELINE_MEASUREMENT_PERIOD;
export const BAI_CUSTOMER_PUBLISHED_DATE = PRIMARY_BASELINE_DATE;

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
  if (
    periodId === BAI_CUSTOMER_PUBLISHED_PERIOD_ID ||
    periodId === "DEMO_VALIDATION"
  ) {
    return BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED;
  }
  if (periodId === BAI_PERIOD_2_CANDIDATE_ID) {
    return BAI_PERIOD_PUBLICATION_STATE.CERTIFIED_UNPROMOTED;
  }
  const manifest = readMeasurementPeriodManifest(periodId);
  if (!manifest) return BAI_PERIOD_PUBLICATION_STATE.NOT_FOUND;
  const q = qualifyMeasurementPeriod(manifest);
  if (q.valid) return BAI_PERIOD_PUBLICATION_STATE.CERTIFIED_UNPROMOTED;
  return BAI_PERIOD_PUBLICATION_STATE.NOT_FOUND;
}

/**
 * Canonical resolver — the only place UI/API should pick prior periods.
 *
 * @param {object} opts
 * @param {"CUSTOMER_PUBLISHED"|"INTERNAL_CANDIDATE_LONGITUDINAL_QA"} [opts.viewMode]
 * @param {string} [opts.geography]
 * @param {string} [opts.currentPeriodId] — only honored in internal candidate mode
 * @param {string} [opts.storeRoot]
 */
export function resolveBaiPriorComparablePeriodV1(opts = {}) {
  const viewMode = opts.viewMode || BAI_VIEW_MODE.CUSTOMER_PUBLISHED;
  const geography = opts.geography || "CALA";
  const storeRoot = opts.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT;

  if (viewMode === BAI_VIEW_MODE.CUSTOMER_PUBLISHED) {
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
      providerCompatibility: "N/A",
      publicationState: BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED,
      period2Exposed: false,
      note:
        "Customer-published Brand AI remains on the federated Aug 14 baseline. Period 2 is certified but unpromoted.",
    };
  }

  if (viewMode !== BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA) {
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
  if (
    getBaiPeriodPublicationState(requestedCurrent) ===
    BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED
  ) {
    // Internal QA can still inspect baseline-only (no prior).
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
      comparabilityReason: "BASELINE_HAS_NO_PRIOR",
      methodologyLineage: "DEMO_VALIDATION_FEDERATED_BASELINE",
      cohortCompatibility: "N/A",
      providerCompatibility: "N/A",
      publicationState: BAI_PERIOD_PUBLICATION_STATE.CUSTOMER_PUBLISHED,
      period2Exposed: false,
    };
  }

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
    providerCompatibility: "PERIOD_SCOPED_PANEL",
    publicationState: getBaiPeriodPublicationState(
      current?.measurementPeriodId || requestedCurrent
    ),
    period2Exposed:
      (current?.measurementPeriodId || requestedCurrent) ===
      BAI_PERIOD_2_CANDIDATE_ID,
    knownLongitudinalPeriods: listMeasurementPeriodManifests(storeRoot).map(
      (m) => m.measurementPeriodId
    ),
  };
}

/**
 * Fail-closed guard: share / customer surfaces must never resolve candidate Period 2.
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
  if (viewMode === BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA) {
    return {
      ok: false,
      gate: BAI_UNPROMOTED_PERIOD_ISOLATION,
      reason: "candidate_mode_forbidden_on_customer_or_share",
    };
  }
  return {
    ok: true,
    gate: BAI_UNPROMOTED_PERIOD_ISOLATION,
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
  };
}
