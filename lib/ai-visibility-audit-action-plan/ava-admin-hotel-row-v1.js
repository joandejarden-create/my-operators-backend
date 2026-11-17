/**
 * Canonical AVA Admin hotel-row projection.
 *
 * Separates measurement/baseline readiness, audit lifecycle, PDF availability,
 * recommendation inventory, and customer-publication authorization.
 */
import { getAuditMeta } from "./audit-store-v1.js";
import {
  loadAdpBaselineForAudit,
} from "./adp-baseline-loader-v1.js";

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

export function pickLatestVersionEntry(meta) {
  const versions = asArray(meta?.versions);
  if (!versions.length) return null;
  if (meta?.latestVersionId) {
    const hit = versions.find((v) => v && v.versionId === meta.latestVersionId);
    if (hit) return hit;
  }
  return versions[versions.length - 1] || null;
}

export function resolveRecommendationCount(meta) {
  const latest = pickLatestVersionEntry(meta);
  const fromLatest = Number(latest?.actionCount ?? 0) || 0;
  if (fromLatest > 0) return fromLatest;
  const accepted = Number(meta?.acceptedRecommendationCount ?? 0) || 0;
  if (accepted > 0) return accepted;
  let max = 0;
  for (const v of asArray(meta?.versions)) {
    const n = Number(v?.actionCount ?? 0) || 0;
    if (n > max) max = n;
  }
  return max;
}

export function resolvePdfStatus(meta) {
  const raw = String(meta?.pdfStatus || "").trim().toUpperCase();
  if (raw === "STAGED" || raw === "AVAILABLE" || raw === "READY") return raw;
  const latest = pickLatestVersionEntry(meta);
  if (latest?.pdfRelativePath || meta?.pdfRelativePath || meta?.draftPdfRelativePath) {
    return "STAGED";
  }
  for (const v of asArray(meta?.versions)) {
    if (v?.pdfRelativePath) return "STAGED";
  }
  return raw || "MISSING";
}

/**
 * Prefer durable/active meta via getAuditMeta (includes production/store seed
 * fallback). Never invent Toronto-specific values.
 */
export function resolveAvaAdminHotelRow(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  const baseline =
    opts.baseline ||
    (typeof opts.loadBaseline === "function"
      ? opts.loadBaseline(id)
      : loadAdpBaselineForAudit(id));
  const meta =
    opts.meta ||
    (typeof opts.getMeta === "function" ? opts.getMeta(id) : getAuditMeta(id)) ||
    {
      propertyId: id,
      status: baseline?.ok ? "BASELINE_READY" : "NOT_STARTED",
      versions: [],
    };

  const latestEntry = pickLatestVersionEntry(meta);
  const recommendationCount = resolveRecommendationCount(meta);
  const pdfStatus = resolvePdfStatus(meta);
  const auditStatus =
    meta.status || (baseline?.ok ? "BASELINE_READY" : "NOT_STARTED");

  return {
    propertyId: id,
    name: baseline?.baseline?.name || meta.name || id,
    market: baseline?.baseline?.market || meta.market || "",
    website: baseline?.baseline?.website || "",
    adpBaselinePeriodId:
      baseline?.baseline?.periodId || meta.adpBaselinePeriodId || null,
    adpBaselineOk: baseline?.ok === true,
    auditStatus,
    acceptedRecommendationCount:
      Number(meta.acceptedRecommendationCount ?? recommendationCount) || 0,
    recommendationCount,
    researchCostUsd: meta.researchCostUsd || 0,
    evidenceReviewStatus: meta.evidenceReviewStatus || "NOT_STARTED",
    pdfStatus,
    customerReadinessStatus:
      meta.customerReadinessStatus || "NOT_CUSTOMER_READY",
    publicationAuthorizationStatus:
      meta.publicationAuthorizationStatus || "NOT_AUTHORIZED",
    latestVersionId: meta.latestVersionId || latestEntry?.versionId || null,
    founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
    customerReady:
      meta.customerReady === true ||
      auditStatus === "CUSTOMER_READY" ||
      auditStatus === "PUBLISHED" ||
      String(meta.customerReadinessStatus || "").toUpperCase() === "CUSTOMER_READY",
    customerPublished:
      meta.customerPublished === true || auditStatus === "PUBLISHED",
    versionCount: asArray(meta.versions).length,
    // Explicit separations for admin consumers / tests
    measurementComplete: baseline?.ok === true,
    auditArtifactAvailable: pdfStatus !== "MISSING" || asArray(meta.versions).length > 0,
    founderReviewed:
      auditStatus === "FOUNDER_REVIEWED_STAGED" ||
      auditStatus === "FOUNDER_APPROVED" ||
      auditStatus === "FOUNDER_APPROVED_FOR_CUSTOMER" ||
      auditStatus === "CUSTOMER_READY" ||
      auditStatus === "PUBLISHED",
    customerPublicationAuthorized:
      meta.founderApprovedForCustomer === true ||
      auditStatus === "FOUNDER_APPROVED_FOR_CUSTOMER" ||
      auditStatus === "CUSTOMER_READY" ||
      auditStatus === "PUBLISHED",
  };
}
