/**
 * ADP Monthly Review eligibility resolver V1.
 * Fail-closed: client-ready PDF only when status is READY or READY_WITH_DISCLOSURES.
 * LIVE_PROVIDER_CALLS = 0 — reads published manifests + reports only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
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
import {
  isAdpSyntheticOrTestPropertyId,
  listOperationalAdpPropertyIds,
} from "../governance/adp-operational-registry-hygiene-v1.js";
import {
  resolveAdpFounderReviewReadinessV1,
  FOUNDER_REVIEW_STATUS,
} from "./resolve-adp-founder-review-readiness-v1.js";
import { loadStagedOwnerPayload, loadStagedManifest } from "../review/adp-staged-report-store-v1.js";
import { loadLoyaltyBppStagedPack } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import {
  shouldReadFromDurableStore,
} from "../durable-publication/resolve-durable-root-v1.js";

/**
 * Admin / coverage universe = canonical 31 operational hotels ∪ published ∪ certified cohort.
 * Synthetic fixtures are excluded. Certified cohort remains a measurement subset.
 * Doctrine: PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW
 *           ADP_OPERATIONAL_REGISTRY_HYGIENE_V1
 */
export function listMonthlyReviewUniversePropertyIds() {
  return [
    ...new Set([
      ...listOperationalAdpPropertyIds(),
      ...listPublishedPropertyIds(),
      ...ADP_CERTIFIED_PROPERTY_IDS,
    ]),
  ]
    .filter((id) => !isAdpSyntheticOrTestPropertyId(id))
    .sort();
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const CERTIFIED_STATUSES = new Set([
  "CERTIFIED",
  "CERTIFIED_WITH_DISCLOSURES",
  "PRODUCTION_VALIDATED",
  "DEMO_VALIDATION",
]);

function readJsonSafe(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function publishedDir(propertyId) {
  return path.join(ROOT, "data/ai-demand-positioning/published", propertyId);
}

/**
 * Durable-prefer aware. Ephemeral published/ alone must not drive Admin eligibility
 * when ADP_DURABLE_PUBLISHED_MODE=prefer|required and durable current exists.
 */
function loadManifest(propertyId) {
  try {
    const durableManifest = loadPublishedManifest(propertyId);
    if (durableManifest) {
      return {
        manifestPath: shouldReadFromDurableStore()
          ? "durable:published-current"
          : path.join(publishedDir(propertyId), "manifest.json"),
        manifest: durableManifest,
        authority: shouldReadFromDurableStore() ? "DURABLE_PUBLISHED" : "EPHEMERAL_OR_STATIC",
      };
    }
  } catch (err) {
    // Fall through to ephemeral — integrity failures still surface as missing below.
    if (err?.code && String(err.code).includes("DURABLE")) {
      return { manifestPath: null, manifest: null, authority: "DURABLE_INTEGRITY_FAILURE", error: err };
    }
  }
  const manifestPath = path.join(publishedDir(propertyId), "manifest.json");
  if (!fs.existsSync(manifestPath)) return { manifestPath, manifest: null, authority: "MISSING" };
  return {
    manifestPath,
    manifest: readJsonSafe(manifestPath),
    authority: "EPHEMERAL_PUBLISHED",
  };
}

function loadReport(propertyId, manifest) {
  try {
    const payload = loadPublishedReport(propertyId);
    if (payload) {
      const report = ensureReportTrendsArray(payload);
      return {
        reportPath: shouldReadFromDurableStore()
          ? "durable:published-current-report"
          : path.join(publishedDir(propertyId), manifest?.reportFile || "report.json"),
        report,
        authority: shouldReadFromDurableStore() ? "DURABLE_PUBLISHED" : "EPHEMERAL_OR_STATIC",
      };
    }
  } catch (err) {
    if (err?.code && String(err.code).includes("DURABLE")) {
      return { reportPath: null, report: null, authority: "DURABLE_INTEGRITY_FAILURE", error: err };
    }
  }
  if (!manifest?.reportFile) return { reportPath: null, report: null, authority: "MISSING" };
  const reportPath = path.join(publishedDir(propertyId), manifest.reportFile);
  if (!fs.existsSync(reportPath)) return { reportPath, report: null, authority: "MISSING" };
  const raw = readJsonSafe(reportPath);
  const report = ensureReportTrendsArray(raw?.payload || raw);
  return { reportPath, report, authority: "EPHEMERAL_PUBLISHED" };
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
 * @param {{
 *   root?: string,
 *   reconciliationPass?: boolean|null,
 *   purpose?: 'client_published_review'|'founder_review',
 * }} [opts]
 *
 * purpose=founder_review — allow independently certified staged ADP without Live publication.
 * purpose=client_published_review (default) — fail-closed on published certified ADP only.
 * clientReady remains false unless published certified path passes.
 */
export function resolveAdpMonthlyReviewEligibilityV1(propertyId, opts = {}) {
  const warnings = [];
  const blockers = [];
  const purpose =
    opts.purpose === "founder_review" ? "founder_review" : "client_published_review";

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
      purpose,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
      liveProviderCalls: 0,
    };
  }

  const inGovernedRegistry = ADP_CERTIFIED_PROPERTY_IDS.includes(propertyId);
  const inPublishedUniverse = listPublishedPropertyIds().includes(propertyId);
  const inOperational31 = listOperationalAdpPropertyIds().includes(propertyId);

  // Founder-review path: operational staged hotels may be eligible without Live publication.
  // Never claim clientReady / PUBLISHED_READY from this branch.
  if (purpose === "founder_review" || (!inPublishedUniverse && inOperational31)) {
    const founder = resolveAdpFounderReviewReadinessV1(propertyId);
    if (founder.eligibleForFounderReview) {
      const staged = loadStagedOwnerPayload(propertyId);
      const stagedManifest = loadStagedManifest(propertyId);
      const loy = loadLoyaltyBppStagedPack(propertyId);
      const periodId =
        founder.periodId ||
        staged?.periodId ||
        stagedManifest?.periodId ||
        null;
      if (!founder.claimsCoreCertified) {
        warnings.push(
          "CORE_PERIOD_NOT_CERTIFIED — founder review of staged evidence only; not PUBLISHED_READY"
        );
      }
      if (founder.bpp?.mode === "LOYALTY_BPP_SUBJECT_MEASURED_PARTIAL") {
        warnings.push(
          `BPP_SUBJECT_MEASURED_PARTIAL:${founder.bpp.blocker || "rank_blocked"}`
        );
      }
      warnings.push("FOUNDER_REVIEW_SOURCE_STAGED_NOT_PUBLISHED");
      return {
        propertyId,
        propertyName:
          staged?.payload?.property?.name ||
          stagedManifest?.propertyName ||
          propertyId,
        status: warnings.length
          ? MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY_WITH_DISCLOSURES
          : MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY,
        eligible: true,
        clientReady: false,
        founderReviewEligible: true,
        publishedReady: false,
        purpose: "founder_review",
        sourceMode: "staged_founder_review",
        blockers: [],
        warnings,
        currentPeriodId: periodId,
        priorPeriodId: null,
        hasComparablePrior: false,
        inGovernedRegistry,
        inPublishedUniverse,
        inOperational31: true,
        founderReviewStatus: founder.founderReviewStatus,
        publishedReadyStatus: founder.publishedReadyStatus,
        claimsCoreCertified: founder.claimsCoreCertified === true,
        corePeriodStatus: founder.corePeriodStatus,
        loyaltyBppPackVersionId: loy?.versionId || null,
        gate: "ADP_FOUNDER_REVIEW_ELIGIBILITY_STAGED_V1",
        liveProviderCalls: 0,
      };
    }
    // Fall through to published checks only when founder path fails and hotel is published.
    if (!inPublishedUniverse && !inGovernedRegistry) {
      return {
        propertyId,
        status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.MISSING_REQUIRED_PAYLOAD,
        eligible: false,
        clientReady: false,
        founderReviewEligible: false,
        publishedReady: false,
        purpose,
        blockers: founder.blockers?.length
          ? founder.blockers
          : ["founder_review_evidence_incomplete"],
        warnings: founder.warnings || [],
        currentPeriodId: founder.periodId || null,
        priorPeriodId: null,
        hasComparablePrior: false,
        inGovernedRegistry: false,
        inPublishedUniverse: false,
        inOperational31,
        founderReviewStatus: founder.founderReviewStatus,
        publishedReadyStatus: founder.publishedReadyStatus,
        gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
        liveProviderCalls: 0,
      };
    }
  }

  // Client / published path — Admin coverage for Live published ADP.
  // Doctrine: PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW
  // Do NOT block solely with not_in_published when operational founder path already handled above.
  if (!inPublishedUniverse && !inGovernedRegistry) {
    return {
      propertyId,
      status: MONTHLY_REVIEW_ELIGIBILITY_STATUS.MISSING_REQUIRED_PAYLOAD,
      eligible: false,
      clientReady: false,
      founderReviewEligible: false,
      publishedReady: false,
      purpose,
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

  const { manifest } = loadManifest(propertyId);
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
    };
  }

  const { report } = loadReport(propertyId, manifest);
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
    founderReviewEligible: true,
    publishedReady: true,
    purpose: "client_published_review",
    sourceMode: "published",
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
    claimsCoreCertified: true,
    publishedReadyStatus: "PUBLISHED_READY",
    founderReviewStatus: FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_READY,
    gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
    liveProviderCalls: 0,
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
