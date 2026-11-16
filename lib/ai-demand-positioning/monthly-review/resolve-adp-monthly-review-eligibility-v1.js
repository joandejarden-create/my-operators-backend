/**
 * ADP Monthly Review eligibility resolver V1.
 * Fail-closed: client-ready PDF only when status is READY or READY_WITH_DISCLOSURES.
 * LIVE_PROVIDER_CALLS = 0 — reads published manifests + reports only.
 *
 * Manifest resolution order (via published-snapshot):
 *   1. data/ai-demand-positioning/published/{id}/manifest.json
 *   2. fixtures seed published
 *   3. durable published-current (data/adp-durable-published/current)
 *
 * Admin coverage must not treat durable-only founder-staged hotels as
 * published_manifest_missing when a durable current report exists.
 */

import { ADP_CERTIFIED_PROPERTY_IDS } from "../contracts/adp-certified-property-cohort-v1.js";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../published-snapshot.js";
import {
  MONTHLY_REVIEW_ELIGIBILITY_STATUS,
  CLIENT_READY_ELIGIBILITY_STATUSES,
} from "./monthly-review-contract-v1.js";
import {
  ensureReportTrendsArray,
  normalizeReportTrends,
} from "../normalize-report-trends-v1.js";

/**
 * Admin / coverage universe = current published ADP properties.
 * Certified cohort remains a measurement subset, not the review catalog ceiling.
 * Doctrine: PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW
 */
export function listMonthlyReviewUniversePropertyIds() {
  return [
    ...new Set([
      ...listPublishedPropertyIds(),
      ...ADP_CERTIFIED_PROPERTY_IDS,
    ]),
  ].sort();
}

const CERTIFIED_STATUSES = new Set([
  "CERTIFIED",
  "CERTIFIED_WITH_DISCLOSURES",
  "PRODUCTION_VALIDATED",
  "DEMO_VALIDATION",
]);

/**
 * Resolve current ADP report for eligibility.
 * Prefer published-snapshot loaders (legacy published/ → seed → durable current).
 * Never require a customer-only fake legacy published/ tree when durable exists.
 */
function loadReportForEligibility(propertyId) {
  const raw = loadPublishedReport(propertyId);
  if (!raw) return null;
  return ensureReportTrendsArray(raw?.payload || raw);
}

function hasTrendPrior(report) {
  if (!report) return false;
  const trends = normalizeReportTrends(report.trends);
  return trends.some((t) => t && (t.role === "prior_run" || t.role === "prior"));
}

function hasOfficialComparablePrior(report) {
  if (!report) return false;
  if (report.executiveRead?.COMPARABLE_PRIOR_AVAILABLE === true) return true;
  if (report.executiveRead?.trend?.hasComparablePrior === true) return true;
  return false;
}

function requiredPayloadOk(report, manifest) {
  if (!report || !manifest) return false;
  if (!report.property?.propertyId && !manifest.propertyId) return false;
  if (!report.executiveMetrics && !report.executiveRead) return false;
  const periodId =
    manifest.monitoringPeriodId ||
    report.period?.periodId ||
    manifest.latestPeriodId;
  return Boolean(periodId);
}

function certificationOk(manifest, report) {
  const status =
    manifest?.certificationStatus ||
    report?.certificationStatus ||
    report?.period?.certificationStatus ||
    null;
  if (!status) {
    // Published Live reports without explicit status still count when publishStatus is Live.
    return manifest?.publishStatus === "Live";
  }
  return CERTIFIED_STATUSES.has(String(status));
}

/**
 * @param {string} propertyId
 * @param {{ root?: string, reconciliationPass?: boolean|null }} [opts]
 */
export function resolveAdpMonthlyReviewEligibilityV1(propertyId, opts = {}) {
  const warnings = [];
  const blockers = [];

  if (!propertyId || typeof propertyId !== "string") {
    return {
      propertyId: propertyId || null,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.MISSING_REQUIRED_PAYLOAD,
      eligible: false,
      clientReady: false,
      blockers: ["propertyId_required"],
      warnings,
      currentPeriodId: null,
      priorPeriodId: null,
      hasComparablePrior: false,
      inGovernedRegistry: false,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
    };
  }

  const inGovernedRegistry = ADP_CERTIFIED_PROPERTY_IDS.includes(propertyId);
  const inPublishedUniverse = listPublishedPropertyIds().includes(propertyId);
  // Admin coverage universe = published ADP. Certified cohort is not a hard ceiling.
  // Doctrine: PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW
  if (!inPublishedUniverse && !inGovernedRegistry) {
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.MISSING_REQUIRED_PAYLOAD,
      eligible: false,
      clientReady: false,
      blockers: ["not_in_published_adp_universe"],
      warnings,
      currentPeriodId: null,
      priorPeriodId: null,
      hasComparablePrior: false,
      inGovernedRegistry: false,
      inPublishedUniverse: false,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
    };
  }

  const manifest = loadPublishedManifest(propertyId);
  if (!manifest) {
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.NO_CURRENT_CERTIFIED_PERIOD,
      eligible: false,
      clientReady: false,
      blockers: ["published_manifest_missing"],
      warnings,
      currentPeriodId: null,
      priorPeriodId: null,
      hasComparablePrior: false,
      inGovernedRegistry,
      inPublishedUniverse,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
      // Clarifies blocker: no legacy published/ AND no durable current pointer.
      manifestResolution: "none",
    };
  }

  const report = loadReportForEligibility(propertyId);
  if (!report) {
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.MISSING_REQUIRED_PAYLOAD,
      eligible: false,
      clientReady: false,
      blockers: ["published_report_missing"],
      warnings,
      currentPeriodId: manifest.latestPeriodId || manifest.monitoringPeriodId || null,
      priorPeriodId: null,
      hasComparablePrior: false,
      inGovernedRegistry,
      inPublishedUniverse,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
      manifestResolution: manifest._adpManifestSource || "legacy_or_seed",
    };
  }

  if (!certificationOk(manifest, report)) {
    blockers.push("current_period_not_certified_or_publishable");
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.NO_CURRENT_CERTIFIED_PERIOD,
      eligible: false,
      clientReady: false,
      blockers,
      warnings,
      currentPeriodId: manifest.latestPeriodId || report.period?.periodId || null,
      priorPeriodId: null,
      hasComparablePrior: false,
      inGovernedRegistry,
      inPublishedUniverse,
      certificationStatus: manifest.certificationStatus || null,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
    };
  }

  if (!requiredPayloadOk(report, manifest)) {
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.MISSING_REQUIRED_PAYLOAD,
      eligible: false,
      clientReady: false,
      blockers: ["required_review_payload_fields_unresolved"],
      warnings,
      currentPeriodId: manifest.latestPeriodId || null,
      priorPeriodId: null,
      hasComparablePrior: false,
      inGovernedRegistry,
      inPublishedUniverse,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
    };
  }

  if (opts.reconciliationPass === false) {
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.ANALYTICAL_RECONCILIATION_FAILED,
      eligible: false,
      clientReady: false,
      blockers: ["ADP_CUSTOMER_OUTPUT_ANALYTICAL_RECONCILIATION_GATE"],
      warnings,
      currentPeriodId: manifest.latestPeriodId || report.period?.periodId || null,
      priorPeriodId: null,
      hasComparablePrior: hasOfficialComparablePrior(report),
      hasTrendPrior: hasTrendPrior(report),
      inGovernedRegistry,
      inPublishedUniverse,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
    };
  }

  const comparablePrior = hasOfficialComparablePrior(report);
  const trendPrior = hasTrendPrior(report);
  const priorTrend = normalizeReportTrends(report.trends).find(
    (t) => t?.role === "prior_run" || t?.role === "prior"
  );
  const priorPeriodId =
    priorTrend?.periodId ||
    report.executiveRead?.trend?.priorPeriodId ||
    null;

  if (!trendPrior) {
    warnings.push("NO_VALID_PRIOR — current-period position only; no invented delta");
  } else if (!comparablePrior) {
    warnings.push(
      "TREND_PRIOR_PRESENT_OFFICIAL_COMPARABLE_NOT_FLAGGED — show prior as secondary with disclosure"
    );
  }
  if (
    String(manifest.certificationStatus || "").includes("DISCLOSURE") ||
    String(report.certificationStatus || "").includes("DISCLOSURE")
  ) {
    warnings.push("CERTIFIED_WITH_DISCLOSURES");
  }

  const status =
    warnings.length > 0
      ? MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY_WITH_DISCLOSURES
      : MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY;

  return {
    propertyId,
    propertyName: report.property?.name || manifest.propertyName || propertyId,
    city: report.property?.city || manifest.city || null,
    market: manifest.market || report.property?.state || null,
    status,
    eligible: true,
    clientReady: CLIENT_READY_ELIGIBILITY_STATUSES.includes(status),
    blockers,
    warnings,
    currentPeriodId:
      manifest.latestPeriodId ||
      report.period?.periodId ||
      manifest.monitoringPeriodId ||
      null,
    monitoringPeriodId: manifest.monitoringPeriodId || null,
    priorPeriodId,
    hasComparablePrior: comparablePrior,
    certificationStatus: manifest.certificationStatus || null,
    publishStatus: manifest.publishStatus || null,
    inGovernedRegistry,
    inPublishedUniverse,
    gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
    liveProviderCalls: 0,
    manifestResolution: manifest._adpManifestSource || "legacy_or_seed",
    // Admin visibility ≠ customer publication authorization.
    customerPublicationAuthorized: false,
  };
}

/**
 * Resolve eligibility for every published ADP property (plus certified cohort).
 * Doctrine: AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE
 */
export function resolveAllAdpMonthlyReviewEligibilityV1(opts = {}) {
  const results = listMonthlyReviewUniversePropertyIds().map((id) =>
    resolveAdpMonthlyReviewEligibilityV1(id, opts)
  );
  const eligible = results.filter((r) => r.eligible);
  const ready = results.filter(
    (r) => r.status === MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY
  );
  const readyWithDisclosures = results.filter(
    (r) => r.status === MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY_WITH_DISCLOSURES
  );
  const blocked = results.filter((r) => !r.eligible);
  return {
    schema: "ADP_MONTHLY_REVIEW_ELIGIBILITY_BATCH_V1",
    liveProviderCalls: 0,
    counts: {
      eligible: eligible.length,
      ready: ready.length,
      readyWithDisclosures: readyWithDisclosures.length,
      blocked: blocked.length,
      total: results.length,
    },
    results,
    eligible,
    blocked,
  };
}

export function assertEligibilityAllowsGeneration(eligibility) {
  if (!eligibility?.eligible) {
    const err = new Error(
      `Monthly review not eligible (${eligibility?.status}): ${(eligibility?.blockers || []).join(", ")}`
    );
    err.code = eligibility?.status || "NOT_ELIGIBLE";
    err.eligibility = eligibility;
    throw err;
  }
  return eligibility;
}
