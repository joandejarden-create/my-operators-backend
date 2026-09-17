/**
 * Admin coverage catalog: one row per current published ADP property.
 * Doctrine:
 *   PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW
 *   AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE
 *   ADP_AI_DEMAND_REVIEW_COVERAGE_V1
 *   AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS
 *   AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY
 *
 * Admin PDF = AI Demand Performance Review (Monthly Review format), not live web print.
 * Action count = detailed Management Action Agenda length.
 */

import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../../published-snapshot.js";
import { listArchiveReviews, loadReviewPayload } from "./archive-store-v1.js";
import {
  listMonthlyReviewUniversePropertyIds,
  resolveAdpMonthlyReviewEligibilityV1,
} from "../resolve-adp-monthly-review-eligibility-v1.js";
import { PDF_STATUS } from "./report-status-v1.js";

export const ADP_AI_DEMAND_REVIEW_COVERAGE_V1 = "ADP_AI_DEMAND_REVIEW_COVERAGE_V1";
export const PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW =
  "PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW";
export const AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE =
  "AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE";
export const AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS =
  "AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS";
export const AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY =
  "AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY";

export const COVERAGE_STATUS = Object.freeze({
  READY: "READY",
  NEEDS_REBUILD: "NEEDS_REBUILD",
  BLOCKED: "BLOCKED",
});

function latestArchiveByProperty(includeArchived = false) {
  const rows = listArchiveReviews({ includeArchived });
  const map = new Map();
  for (const row of rows) {
    const id = row.propertyId;
    if (!id) continue;
    const prev = map.get(id);
    if (!prev) {
      map.set(id, row);
      continue;
    }
    const prevReady =
      prev.pdfStatus === PDF_STATUS.READY || prev.pdfStatus === "READY";
    const nextReady =
      row.pdfStatus === PDF_STATUS.READY || row.pdfStatus === "READY";
    // Prefer newest PDF-ready review for admin "current"; do not let a newer
    // draft without PDF displace a READY Performance Review artifact.
    if (nextReady && !prevReady) {
      map.set(id, row);
      continue;
    }
    if (!nextReady && prevReady) continue;
    const prevTs = Date.parse(prev.generatedAt || prev.createdAt || 0) || 0;
    const nextTs = Date.parse(row.generatedAt || row.createdAt || 0) || 0;
    if (nextTs >= prevTs) map.set(id, row);
  }
  return map;
}

function actionAgendaCount(reviewId) {
  if (!reviewId) return { actionCount: 0, actionTitles: [] };
  try {
    const pack = loadReviewPayload(reviewId);
    const agenda = Array.isArray(pack?.review?.actionAgenda)
      ? pack.review.actionAgenda
      : [];
    return {
      actionCount: agenda.length,
      actionTitles: agenda
        .map((a) => a.actionTitle || a.title || "")
        .filter(Boolean),
    };
  } catch (_e) {
    return { actionCount: 0, actionTitles: [] };
  }
}

function classifyCoverage({ eligibility, latestReview }) {
  if (!eligibility?.eligible) {
    return {
      coverageStatus: COVERAGE_STATUS.BLOCKED,
      coverageReason:
        (eligibility?.blockers || []).join(", ") ||
        eligibility?.status ||
        "not_eligible",
    };
  }
  if (!latestReview) {
    return {
      coverageStatus: COVERAGE_STATUS.NEEDS_REBUILD,
      coverageReason: "review_artifact_missing",
    };
  }
  if (
    latestReview.pdfStatus !== PDF_STATUS.READY &&
    latestReview.pdfStatus !== "READY"
  ) {
    return {
      coverageStatus: COVERAGE_STATUS.NEEDS_REBUILD,
      coverageReason: "pdf_missing_or_pending",
    };
  }
  return {
    coverageStatus: COVERAGE_STATUS.READY,
    coverageReason: null,
  };
}

/**
 * @returns {{
 *   gate: string,
 *   publishedCount: number,
 *   reviewReadyCount: number,
 *   missingCount: number,
 *   blockedCount: number,
 *   properties: object[],
 * }}
 */
export function buildPublishedAdpReviewCoverageV1(opts = {}) {
  const publishedIds = listPublishedPropertyIds().sort();
  const universeIds = opts.includeCertifiedExtras
    ? listMonthlyReviewUniversePropertyIds()
    : publishedIds;
  const archiveByProperty = latestArchiveByProperty(opts.includeArchived === true);

  const properties = universeIds.map((propertyId) => {
    const manifest = loadPublishedManifest(propertyId);
    const report = loadPublishedReport(propertyId);
    const eligibility = resolveAdpMonthlyReviewEligibilityV1(propertyId);
    const latestReview = archiveByProperty.get(propertyId) || null;
    const { coverageStatus, coverageReason } = classifyCoverage({
      eligibility,
      latestReview,
    });
    const agenda = actionAgendaCount(latestReview?.reviewId);

    return {
      propertyId,
      propertyName:
        report?.property?.name ||
        manifest?.propertyName ||
        latestReview?.propertyName ||
        propertyId,
      market:
        manifest?.market ||
        report?.property?.market ||
        report?.property?.state ||
        latestReview?.market ||
        null,
      currentPeriodId:
        eligibility.currentPeriodId ||
        manifest?.latestPeriodId ||
        manifest?.monitoringPeriodId ||
        report?.period?.periodId ||
        latestReview?.sourceCurrentPeriodId ||
        null,
      reportingMonth: latestReview?.reportingMonth || null,
      currentMonitoringDate: latestReview?.currentMonitoringDate || null,
      priorRunDate: latestReview?.priorRunDate || null,
      versionLabel: latestReview?.versionLabel || null,
      isCurrent: latestReview?.isCurrent !== false,
      reviewStatus: latestReview?.reviewStatus || null,
      generationStatus: latestReview?.generationStatus || null,
      clientIssued: Boolean(
        latestReview?.clientIssuedAt ||
          latestReview?.reviewStatus === "CLIENT_ISSUED"
      ),
      coverageStatus,
      coverageReason,
      eligibilityStatus: eligibility.status,
      eligible: Boolean(eligibility.eligible),
      blockers: eligibility.blockers || [],
      warnings: eligibility.warnings || [],
      reviewId: latestReview?.reviewId || null,
      pdfStatus: latestReview?.pdfStatus || null,
      generatedAt: latestReview?.generatedAt || null,
      hasPdf:
        latestReview?.pdfStatus === PDF_STATUS.READY ||
        latestReview?.pdfStatus === "READY",
      pdfKind: "ai_demand_performance_review",
      pdfGate: AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS,
      actionCount: agenda.actionCount || latestReview?.actionCount || 0,
      actionTitles: agenda.actionTitles,
      actionSource: "actionAgenda",
      actionGate: AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY,
      inPublishedUniverse: publishedIds.includes(propertyId),
      // live Priority Actions remain customer-facing summary (separate from admin agenda)
      livePriorityActionCount: Array.isArray(report?.actions)
        ? report.actions.length
        : 0,
    };
  });

  const reviewReadyCount = properties.filter(
    (p) => p.coverageStatus === COVERAGE_STATUS.READY
  ).length;
  const blockedCount = properties.filter(
    (p) => p.coverageStatus === COVERAGE_STATUS.BLOCKED
  ).length;
  const missingCount = properties.filter(
    (p) => p.coverageStatus === COVERAGE_STATUS.NEEDS_REBUILD
  ).length;

  return {
    gate: ADP_AI_DEMAND_REVIEW_COVERAGE_V1,
    doctrine: {
      PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW,
      AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE,
      AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS,
      AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY,
    },
    publishedCount: publishedIds.length,
    universeCount: properties.length,
    reviewReadyCount,
    missingCount,
    blockedCount,
    silentlyOmitted: 0,
    publishedPropertyIds: publishedIds,
    properties,
    liveProviderCalls: 0,
  };
}
