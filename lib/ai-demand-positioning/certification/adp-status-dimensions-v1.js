/**
 * ADP certification inventory status dimensions (governance / accounting).
 * Does not change measurement methodology, thresholds, or stored period metrics.
 *
 * Orthogonal axes — never collapse into "CERTIFIED/PASS".
 */

import { ADP_PERIOD_PIPELINE_STATES } from "./adp-period-pipeline-states-v1.js";
import { ADP_COMPARABILITY_OUTCOMES } from "./adp-comparability-engine-v1.js";
import { ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION } from "./certify-adp-period-v1.js";

export const ADP_STATUS_DIMENSIONS_VERSION = "adp_status_dimensions_v1";

/** Property-level QA outcome from the certification engine dry-run (engineWouldCertify). */
export const PROPERTY_QA_STATUS = Object.freeze({
  PASS: "PASS",
  REVIEW: "REVIEW",
  FAIL: "FAIL",
});

/**
 * Governed period certification status for inventory / publish decisions.
 * Prefer ADP_PERIOD_PIPELINE_STATES; LEGACY_QA_PASS is a reporting alias only
 * (engine would certify, but period remains LEGACY_UNCERTIFIED on disk).
 */
export const PERIOD_CERTIFICATION_STATUS = Object.freeze({
  DRAFT: ADP_PERIOD_PIPELINE_STATES.DRAFT,
  QA_REVIEW_REQUIRED: ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED,
  QA_FAILED: ADP_PERIOD_PIPELINE_STATES.QA_FAILED,
  CERTIFIED: ADP_PERIOD_PIPELINE_STATES.CERTIFIED,
  LEGACY_UNCERTIFIED: ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED,
  SUPERSEDED: ADP_PERIOD_PIPELINE_STATES.SUPERSEDED,
  /** Reporting-only: engineWouldCertify=CERTIFIED but period is not certification-era. */
  LEGACY_QA_PASS: "LEGACY_QA_PASS",
});

/** What kind of current official published period this property has. */
export const CURRENT_PERIOD_CLASS = Object.freeze({
  CERTIFICATION_ERA_OFFICIAL: "CERTIFICATION_ERA_OFFICIAL",
  LEGACY_OFFICIAL: "LEGACY_OFFICIAL",
  NO_OFFICIAL_PERIOD: "NO_OFFICIAL_PERIOD",
});

export const PUBLISH_ELIGIBILITY = Object.freeze({
  ALLOWED: "ALLOWED",
  BLOCKED: "BLOCKED",
  GRANDFATHERED: "GRANDFATHERED",
});

export const LEGACY_STATUS = Object.freeze({
  NOT_LEGACY: "NOT_LEGACY",
  LEGACY_CURRENT: "LEGACY_CURRENT",
  LEGACY_SUPERSEDED: "LEGACY_SUPERSEDED",
  NO_PERIOD: "NO_PERIOD",
});

export const COMPARABILITY_STATUS = Object.freeze({
  EXACT_COMPARABLE: ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE,
  COMMON_SET_COMPARABLE: ADP_COMPARABILITY_OUTCOMES.COMMON_SET_COMPARABLE,
  DIRECTIONAL_ONLY: ADP_COMPARABILITY_OUTCOMES.DIRECTIONAL_ONLY,
  NOT_COMPARABLE: ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE,
  NOT_EVALUATED: "NOT_EVALUATED",
});

/**
 * True when the period (or published manifest) was created under the global
 * certification engine — not a pre-framework stamp.
 */
export function isCertificationEraPeriod(period, publishedManifest = null) {
  if (period?.globalCertificationEngineVersion) return true;
  if (publishedManifest?.globalCertificationEngineVersion) return true;
  if (period?.matchedControlSetId || period?.controlSetId) {
    return period?.certificationStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED;
  }
  return false;
}

/**
 * Map engineWouldCertify / hard+review flags → property QA.
 */
export function mapEngineToPropertyQaStatus(engineStatus, hardFailures = [], reviewFlags = []) {
  if ((hardFailures || []).length || engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_FAILED) {
    return PROPERTY_QA_STATUS.FAIL;
  }
  if (
    (reviewFlags || []).length ||
    engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED
  ) {
    return PROPERTY_QA_STATUS.REVIEW;
  }
  if (
    engineStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED ||
    engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_PASSED ||
    engineStatus === "CERTIFIED_WITH_DISCLOSURES"
  ) {
    return PROPERTY_QA_STATUS.PASS;
  }
  if (engineStatus === ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED) {
    // Remapped official status — treat as PASS when no hard/review (legacy QA pass path).
    return PROPERTY_QA_STATUS.PASS;
  }
  return PROPERTY_QA_STATUS.REVIEW;
}

/**
 * Governed period certification status for counts.
 * Stale CERTIFIED labels on legacy manifests do NOT count as CERTIFIED.
 */
export function resolvePeriodCertificationStatus({
  period,
  publishedManifest,
  engineWouldCertify,
  stampStatus,
  propertyQaStatus,
}) {
  const era = isCertificationEraPeriod(period, publishedManifest);
  const stamped =
    stampStatus ||
    period?.certificationStatus ||
    publishedManifest?.certificationStatus ||
    null;

  if (!period?.periodId && !publishedManifest?.latestPeriodId) {
    return PERIOD_CERTIFICATION_STATUS.DRAFT;
  }

  if (era) {
    if (stamped === ADP_PERIOD_PIPELINE_STATES.CERTIFIED || period?.certified === true) {
      return PERIOD_CERTIFICATION_STATUS.CERTIFIED;
    }
    if (stamped === ADP_PERIOD_PIPELINE_STATES.QA_FAILED) {
      return PERIOD_CERTIFICATION_STATUS.QA_FAILED;
    }
    if (stamped === ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED) {
      return PERIOD_CERTIFICATION_STATUS.QA_REVIEW_REQUIRED;
    }
    if (stamped === ADP_PERIOD_PIPELINE_STATES.SUPERSEDED) {
      return PERIOD_CERTIFICATION_STATUS.SUPERSEDED;
    }
    // Certification-era but not yet stamped CERTIFIED
    if (engineWouldCertify === ADP_PERIOD_PIPELINE_STATES.QA_FAILED) {
      return PERIOD_CERTIFICATION_STATUS.QA_FAILED;
    }
    if (engineWouldCertify === ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED) {
      return PERIOD_CERTIFICATION_STATUS.QA_REVIEW_REQUIRED;
    }
    return PERIOD_CERTIFICATION_STATUS.DRAFT;
  }

  // Legacy / pre-framework
  if (propertyQaStatus === PROPERTY_QA_STATUS.PASS) {
    return PERIOD_CERTIFICATION_STATUS.LEGACY_QA_PASS;
  }
  if (propertyQaStatus === PROPERTY_QA_STATUS.FAIL) {
    return PERIOD_CERTIFICATION_STATUS.QA_FAILED;
  }
  if (propertyQaStatus === PROPERTY_QA_STATUS.REVIEW) {
    return PERIOD_CERTIFICATION_STATUS.QA_REVIEW_REQUIRED;
  }
  return PERIOD_CERTIFICATION_STATUS.LEGACY_UNCERTIFIED;
}

export function resolveCurrentPeriodClass(period, publishedManifest) {
  const hasOfficial =
    Boolean(publishedManifest?.latestPeriodId) ||
    Boolean(period?.periodId && (period.published === true || publishedManifest));
  if (!hasOfficial && !publishedManifest?.latestPeriodId) {
    return CURRENT_PERIOD_CLASS.NO_OFFICIAL_PERIOD;
  }
  if (isCertificationEraPeriod(period, publishedManifest)) {
    return CURRENT_PERIOD_CLASS.CERTIFICATION_ERA_OFFICIAL;
  }
  return CURRENT_PERIOD_CLASS.LEGACY_OFFICIAL;
}

/**
 * Publish eligibility under ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH.
 * - New official publish requires CERTIFIED (certification-era).
 * - Existing live legacy reports are GRANDFATHERED for customer read.
 */
export function resolvePublishEligibility({
  currentPeriodClass,
  periodCertificationStatus,
  requireCert = process.env.ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH === "1",
  officialPeriodPublished = false,
}) {
  if (!requireCert) {
    return officialPeriodPublished || currentPeriodClass !== CURRENT_PERIOD_CLASS.NO_OFFICIAL_PERIOD
      ? PUBLISH_ELIGIBILITY.ALLOWED
      : PUBLISH_ELIGIBILITY.BLOCKED;
  }

  if (periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.CERTIFIED) {
    return PUBLISH_ELIGIBILITY.ALLOWED;
  }

  if (currentPeriodClass === CURRENT_PERIOD_CLASS.LEGACY_OFFICIAL && officialPeriodPublished) {
    return PUBLISH_ELIGIBILITY.GRANDFATHERED;
  }

  return PUBLISH_ELIGIBILITY.BLOCKED;
}

export function resolveLegacyStatus(currentPeriodClass, periodCertificationStatus) {
  if (currentPeriodClass === CURRENT_PERIOD_CLASS.NO_OFFICIAL_PERIOD) {
    return LEGACY_STATUS.NO_PERIOD;
  }
  if (currentPeriodClass === CURRENT_PERIOD_CLASS.CERTIFICATION_ERA_OFFICIAL) {
    return LEGACY_STATUS.NOT_LEGACY;
  }
  if (periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.SUPERSEDED) {
    return LEGACY_STATUS.LEGACY_SUPERSEDED;
  }
  return LEGACY_STATUS.LEGACY_CURRENT;
}

export {
  ADP_PERIOD_PIPELINE_STATES,
  ADP_COMPARABILITY_OUTCOMES,
  ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
};
