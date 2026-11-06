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
import {
  ADP_PRODUCTION_PUBLISHED_DATA_MISSING,
  ADP_PUBLISHED_REGISTRY_DIVERGENCE,
  ADP_PUBLISHED_SILENT_ROLLBACK_BLOCKED,
  ADP_PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE,
  getPublishedCurrentEntry,
} from "../../published-current-registry-v1.js";

const PUBLISHED_INTEGRITY_CODES = new Set([
  ADP_PRODUCTION_PUBLISHED_DATA_MISSING,
  ADP_PUBLISHED_REGISTRY_DIVERGENCE,
  ADP_PUBLISHED_SILENT_ROLLBACK_BLOCKED,
  ADP_PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE,
]);
import { listArchiveReviews, loadReviewPayload } from "./archive-store-v1.js";
import {
  listMonthlyReviewUniversePropertyIds,
  resolveAdpMonthlyReviewEligibilityV1,
} from "../resolve-adp-monthly-review-eligibility-v1.js";
import { resolveAdpReportPdf } from "../resolve-adp-report-pdf-v1.js";
import { PDF_STATUS } from "./report-status-v1.js";
import { probeAdpClientShareAvailable } from "../../../admin/report-external-client-links-v1.js";
import {
  ADP_OPERATIONAL_REGISTRY_HYGIENE_V1,
  isAdpSyntheticOrTestPropertyId,
  listOperationalAdpPropertyIds,
} from "../../governance/adp-operational-registry-hygiene-v1.js";
import {
  resolveAdpFounderReviewReadinessV1,
  FOUNDER_REVIEW_STATUS,
  PUBLISHED_READY_STATUS,
} from "../resolve-adp-founder-review-readiness-v1.js";
import {
  loadStagedManifest,
  loadStagedOwnerPayload,
} from "../../review/adp-staged-report-store-v1.js";
import { loadPropertyProfile } from "../../data-model.js";
import {
  ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
  assessFullAdpE2eAiAdminCertificationParity,
} from "../../governance/adp-full-e2e-ai-admin-certification-parity-v1.js";
import {
  ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
  ADP_ARTIFACT_TYPE_MONTHLY_EXECUTIVE_REVIEW_PDF,
  resolveFullAdpE2ePdfArtifact,
} from "../../full-adp-e2e-pdf/adp-full-e2e-pdf-artifact-store-v1.js";
import {
  ADP_EXTERNAL_SHARE_ACCESS_GATE_V1,
  EXTERNAL_SHARE_ACCESS_STATUS,
  classifyExternalShareAccessForCustomerReady,
} from "../../share/adp-external-share-access-gate-v1.js";
import { getLiveExternalShareVerifyForProperty } from "../../share/adp-external-share-live-verify-ledger-v1.js";
import { getHotelPublicationRecord } from "../../review/adp-publication-registry-v1.js";
import {
  ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1,
  ADMIN_PDF_STATUS,
  resolveAuthoritativePublishedColumn,
  resolveAdminPublishedPdfColumn,
  resolveCustomerAccessColumn,
  buildAdminActionsAgenda,
} from "./resolve-admin-publication-columns-v1.js";
import {
  getAdminPublicationCoverageGeneration,
} from "./admin-publication-coverage-generation-v1.js";

export const ADP_AI_DEMAND_REVIEW_COVERAGE_V1 = "ADP_AI_DEMAND_REVIEW_COVERAGE_V1";
export { ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1, ADMIN_PDF_STATUS };
export { ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1 };
export const PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW =
  "PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW";
export const AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE =
  "AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE";
export const AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS =
  "AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS";
export const AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY =
  "AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY";
export const AI_DEMAND_ADMIN_PDF_REQUIRES_PUBLISHED_PERIOD_MATCH =
  "AI_DEMAND_ADMIN_PDF_REQUIRES_PUBLISHED_PERIOD_MATCH";

export const COVERAGE_STATUS = Object.freeze({
  READY: "READY",
  NEEDS_REBUILD: "NEEDS_REBUILD",
  BLOCKED: "BLOCKED",
});

/** Dual readiness — never conflate founder review with Live publication. */
export const FOUNDER_VS_PUBLISHED_DOCTRINE_V1 =
  "FOUNDER_REVIEW_READY_SEPARATE_FROM_PUBLISHED_READY_V1";

/** Per-hotel publication integrity — staged / published / PDF / external share. */
export const ADP_PUBLICATION_INTEGRITY_ADMIN_V1 =
  "ADP_PUBLICATION_INTEGRITY_ADMIN_V1";

export const CUSTOMER_READY_STATUS = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  PUBLISHED_CUSTOMER_READY: "PUBLISHED_CUSTOMER_READY",
  NEW_STAGED_VERSION_AWAITING_APPROVAL: "NEW_STAGED_VERSION_AWAITING_APPROVAL",
  REQUIRES_PUBLICATION: "REQUIRES_PUBLICATION",
  REQUIRES_PDF_REBUILD: "REQUIRES_PDF_REBUILD",
  REQUIRES_DEPLOYMENT: "REQUIRES_DEPLOYMENT",
  STAGED_CERTIFIED_NOT_PUBLISHED: "STAGED_CERTIFIED_NOT_PUBLISHED",
  EXTERNAL_SHARE_ACCESS_BLOCKED: "EXTERNAL_SHARE_ACCESS_BLOCKED",
  PRODUCTION_PUBLISHED_DATA_MISSING: "PRODUCTION_PUBLISHED_DATA_MISSING",
  PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE: "PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE",
  PUBLISHED_SILENT_ROLLBACK_BLOCKED: "PUBLISHED_SILENT_ROLLBACK_BLOCKED",
  BLOCKED: "BLOCKED",
  NOT_READY: "NOT_READY",
});

/** Operational Admin summary cards — mutually explained; hotels may appear in multiple cards. */
export const ADP_ADMIN_OPERATIONAL_SUMMARY_V1 = "ADP_ADMIN_OPERATIONAL_SUMMARY_V1";

export const OPERATIONAL_FILTER = Object.freeze({
  ALL: "all",
  PUBLISHED: "published",
  CUSTOMER_READY: "customer_ready",
  AWAITING_REVIEW: "awaiting_review",
  /** Alias filter id — same bucket as AWAITING_REVIEW */
  AWAITING_PUBLICATION: "awaiting_publication",
  NEEDS_ATTENTION: "needs_attention",
  UNPUBLISHED: "unpublished",
  EXTERNAL_SHARE_PROBLEMS: "external_share_problems",
  PDF_PROBLEMS: "pdf_problems",
});

/** Simplified New Draft column — lineage-based, not staged createdAt alone. */
export const NEW_DRAFT_STATUS = Object.freeze({
  UP_TO_DATE: "UP_TO_DATE",
  REVIEW: "REVIEW",
  APPROVED: "APPROVED",
  NOT_PUBLISHED: "NOT_PUBLISHED",
});

export { ADP_EXTERNAL_SHARE_ACCESS_GATE_V1, EXTERNAL_SHARE_ACCESS_STATUS };

const HUMAN_REASON_MAP = Object.freeze({
  performance_review_pdf_stale_vs_published_period: {
    title: "Performance Review PDF needs updating",
    explanation: "The PDF does not match the currently published report.",
  },
  pdf_missing_or_pending: {
    title: "Performance Review PDF missing",
    explanation: "No ready Performance Review PDF is available for this hotel.",
  },
  full_adp_pdf_missing_or_uncertified: {
    title: "Full ADP PDF missing",
    explanation: "The certified Full ADP report PDF is not available.",
  },
  staged_period_ahead_of_published_period: {
    title: "Newer draft awaiting publication",
    explanation:
      "Customers still see the published report. A newer staged version awaits founder approval.",
  },
  share_not_created: {
    title: "External share not created",
    explanation: "No customer-facing share token exists for this hotel.",
  },
  customer_access_not_independently_verified: {
    title: "Customer access not independently verified",
    explanation:
      "A share may exist, but independent open/report verification has not confirmed Customer Ready.",
  },
  PRODUCTION_PUBLISHED_DATA_MISSING: {
    title: "Published report missing in production",
    explanation:
      "The share resolves, but the published report cannot be loaded in production.",
  },
  PUBLISHED_SILENT_ROLLBACK_BLOCKED: {
    title: "Published registry conflict",
    explanation:
      "Filesystem published period does not match the durable published-current registry.",
  },
  PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE: {
    title: "Published report integrity failure",
    explanation: "Published report bytes do not match the expected integrity record.",
  },
});

export function humanizeAdminReason(code) {
  const key = String(code || "").trim();
  if (!key) return null;
  const mapped = HUMAN_REASON_MAP[key];
  if (mapped) return { code: key, ...mapped };
  return {
    code: key,
    title: key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
    explanation: "See technical details for the exact failure code.",
  };
}

function shortPeriodId(periodId) {
  const s = String(periodId || "");
  if (!s) return null;
  const m = s.match(/_([a-f0-9]{6})$/i);
  return m ? `…${m[1]}` : s.slice(-12);
}

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

/** Human month/year from ADP period id (`…_YYYYMMDDHHMMSS_hex`). */
export function monthYearFromPeriodId(periodId) {
  const s = String(periodId || "");
  const m = s.match(/_(\d{4})(\d{2})(\d{2})\d{6}_[a-f0-9]{6}$/i);
  if (!m) return null;
  const year = Number(m[1]);
  const monthIdx = Number(m[2]) - 1;
  if (!year || monthIdx < 0 || monthIdx > 11) return null;
  return `${MONTH_NAMES[monthIdx]} ${year}`;
}

function considerationFromReport(report) {
  const rate =
    report?.executiveMetrics?.considerationRate?.rate ??
    report?.payload?.executiveMetrics?.considerationRate?.rate ??
    report?.executiveMetrics?.considerationRate?.percentage ??
    null;
  return rate == null ? null : Number(rate);
}

/**
 * Authoritative staged-vs-published difference for Admin status cards.
 *
 * Awaiting Publication must mean a real unpublished draft requiring publication —
 * not merely the existence of a staged file / opaque artifact hash.
 *
 * Same period + same customer Consideration = effectively identical content
 * (staged artifactHash often ≠ published reportSha256 for the same customer report).
 * Never uses staged createdAt alone.
 */
export function resolveStagedDiffersFromPublishedLineage({
  publishedPeriodId,
  stagedPeriodId,
  publishedReportSha256 = null,
  stagedReportSha256 = null,
  publishedConsideration = null,
  stagedConsideration = null,
}) {
  if (!stagedPeriodId) return false;
  if (!publishedPeriodId) return Boolean(stagedPeriodId);
  if (String(stagedPeriodId) !== String(publishedPeriodId)) return true;

  const pubCons =
    publishedConsideration == null || publishedConsideration === ""
      ? null
      : Number(publishedConsideration);
  const stgCons =
    stagedConsideration == null || stagedConsideration === ""
      ? null
      : Number(stagedConsideration);
  if (
    Number.isFinite(pubCons) &&
    Number.isFinite(stgCons) &&
    Math.abs(pubCons - stgCons) < 0.05
  ) {
    return false;
  }
  if (
    Number.isFinite(pubCons) &&
    Number.isFinite(stgCons) &&
    Math.abs(pubCons - stgCons) >= 0.05
  ) {
    return true;
  }

  // Considerations unavailable: opaque SHA/hash mismatch alone is insufficient when
  // periods match — staged artifactHash ≠ published report file SHA by construction.
  if (
    publishedReportSha256 &&
    stagedReportSha256 &&
    String(publishedReportSha256) === String(stagedReportSha256)
  ) {
    return false;
  }
  return false;
}

export function resolveNewDraftStatus(row) {
  if (!row.inPublishedUniverse) {
    return {
      status: NEW_DRAFT_STATUS.NOT_PUBLISHED,
      label: "Not published",
      kind: "muted",
    };
  }
  const pubRec = row.publicationRegistryRecord || null;
  const approvedVersion = pubRec?.approvedReportVersion || null;
  const pubState = String(pubRec?.state || "");
  const stagedId = row.stagedVersionId || null;
  const approvedAwaiting =
    approvedVersion &&
    stagedId &&
    String(approvedVersion) === String(stagedId) &&
    (pubState === "APPROVED_FOR_PUBLICATION" ||
      pubState === "WAITING_PUBLICATION_AUTHORIZATION" ||
      pubState === "PUBLISHING");
  if (approvedAwaiting) {
    return {
      status: NEW_DRAFT_STATUS.APPROVED,
      label: "Ready to publish",
      kind: "ok",
    };
  }
  const differs =
    row.publicationIntegrity?.stagedDiffersFromPublished === true ||
    row.newDraftDiffersByLineage === true;
  if (differs) {
    return {
      status: NEW_DRAFT_STATUS.REVIEW,
      label: "Review",
      kind: "warn",
    };
  }
  // Published current matches approved package and no different unpublished
  // draft exists → always "Up to date" (with or without a same-content staged copy).
  return {
    status: NEW_DRAFT_STATUS.UP_TO_DATE,
    label: "Up to date",
    kind: "ok",
  };
}

/** Publish button enablement for simplified Actions column. */
export function resolvePublishActionState(row) {
  const draft = row.newDraftStatus || resolveNewDraftStatus(row);
  const stagedCertified =
    row.publicationIntegrity?.stagedReportCertified === true ||
    row.publicationIntegrity?.certificationStatus === "STAGED_CERTIFIED";
  const stagedId = row.stagedVersionId || null;
  if (!row.inPublishedUniverse) {
    if (stagedId && stagedCertified) {
      return {
        enabled: true,
        label: "Publish",
        act: "review-publish",
        title: "Open Review & Publish for the certified staged report",
        kind: "primary",
      };
    }
    return {
      enabled: false,
      label: "Publish",
      act: "review-publish",
      title: stagedId
        ? "Staged report is not certified — Publish disabled"
        : "No certified staged report to publish",
      kind: "muted",
      blocker: stagedId ? "staged_not_certified" : "no_staged_report",
    };
  }
  if (draft.status === NEW_DRAFT_STATUS.UP_TO_DATE) {
    return {
      enabled: false,
      label: "Up to date",
      act: "review-publish",
      title: "Staged and published reports match — nothing to publish",
      kind: "muted",
      blocker: "up_to_date",
    };
  }
  if (draft.status === NEW_DRAFT_STATUS.APPROVED) {
    return {
      enabled: true,
      label: "Review & Publish",
      act: "review-publish",
      title: "Approved staged version ready — open Review & Publish",
      kind: "primary",
    };
  }
  if (draft.status === NEW_DRAFT_STATUS.REVIEW) {
    if (!stagedCertified) {
      return {
        enabled: false,
        label: "Review & Publish",
        act: "review-publish",
        title: "Newer draft exists but is not certified",
        kind: "warn",
        blocker: "staged_not_certified",
      };
    }
    return {
      enabled: true,
      label: "Review & Publish",
      act: "review-publish",
      title: "Newer staged version awaits review and publication",
      kind: "primary",
    };
  }
  return {
    enabled: false,
    label: "Publish",
    act: "review-publish",
    title: "Publication not available",
    kind: "muted",
    blocker: "unavailable",
  };
}

function resolveOverallPdfStatus(row) {
  const full = row.fullAdpPdf || {};
  const perf = row.performanceReviewPdf || {};
  const fullDetail = {
    status: full.available === true ? "READY" : "MISSING",
    available: full.available === true,
    label: full.available === true ? "Ready" : "Missing",
  };
  const perfDetail = {
    status:
      perf.status === "STALE_VS_PUBLISHED_PERIOD" || perf.status === "STALE"
        ? "STALE"
        : perf.available === true
          ? "READY"
          : "MISSING",
    available: perf.available === true,
    label:
      perf.status === "STALE_VS_PUBLISHED_PERIOD" || perf.status === "STALE"
        ? "Stale"
        : perf.available === true
          ? "Ready"
          : "Missing",
  };
  if (!row.inPublishedUniverse) {
    return {
      status: "NOT_APPLICABLE",
      label: "Not applicable",
      kind: "muted",
      detail: "No published report",
      fullAdp: fullDetail,
      performanceReview: perfDetail,
    };
  }
  if (perf.status === "STALE_VS_PUBLISHED_PERIOD" || perf.status === "STALE") {
    return {
      status: "STALE",
      label: "Stale",
      kind: "warn",
      detail: "Performance Review PDF does not match published period",
      fullAdp: fullDetail,
      performanceReview: perfDetail,
    };
  }
  if (full.available === true) {
    return {
      status: "READY",
      label: "Ready",
      kind: "ok",
      detail: perf.available
        ? "Full ADP PDF certified (Performance Review also ready)"
        : "Full ADP PDF certified",
      fullAdp: fullDetail,
      performanceReview: perfDetail,
    };
  }
  return {
    status: "MISSING",
    label: "Missing",
    kind: "warn",
    detail: "Certified published Full ADP PDF not available",
    fullAdp: fullDetail,
    performanceReview: perfDetail,
  };
}

function resolveCustomerAccessDisplay(row) {
  return resolveCustomerAccessColumn({
    inPublishedUniverse: row.inPublishedUniverse === true,
    shareProbe: row.adpClient || null,
    liveExternalShareVerify:
      row.publicationIntegrity?.liveExternalShareVerify || null,
    publicationIntegrity: row.publicationIntegrity,
  });
}

function computeOperationalBuckets(row) {
  const buckets = new Set();
  if (row.inPublishedUniverse) buckets.add(OPERATIONAL_FILTER.PUBLISHED);
  else buckets.add(OPERATIONAL_FILTER.UNPUBLISHED);
  if (row.customerReady === true) buckets.add(OPERATIONAL_FILTER.CUSTOMER_READY);
  // Awaiting Publication = real unpublished draft requiring publication
  // (content identity differs OR first-publish / approved queue) — not mere staged file presence.
  const contentDiffers =
    row.publicationIntegrity?.stagedDiffersFromPublished === true ||
    row.newDraftDiffersByLineage === true;
  const awaiting =
    contentDiffers ||
    row.newDraftStatus?.status === NEW_DRAFT_STATUS.REVIEW ||
    row.newDraftStatus?.status === NEW_DRAFT_STATUS.APPROVED ||
    row.customerReadyStatus ===
      CUSTOMER_READY_STATUS.NEW_STAGED_VERSION_AWAITING_APPROVAL ||
    row.customerReadyStatus ===
      CUSTOMER_READY_STATUS.STAGED_CERTIFIED_NOT_PUBLISHED ||
    row.customerReadyStatus === CUSTOMER_READY_STATUS.REQUIRES_PUBLICATION;
  if (awaiting) {
    buckets.add(OPERATIONAL_FILTER.AWAITING_REVIEW);
    buckets.add(OPERATIONAL_FILTER.AWAITING_PUBLICATION);
  }
  const pdfProblems =
    row.overallPdfStatus?.status === "STALE" ||
    row.overallPdfStatus?.status === "MISSING";
  if (pdfProblems) buckets.add(OPERATIONAL_FILTER.PDF_PROBLEMS);
  const shareProblems =
    row.inPublishedUniverse &&
    row.customerAccess?.status &&
    row.customerAccess.status !==
      EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED &&
    row.customerAccess.status !==
      EXTERNAL_SHARE_ACCESS_STATUS.VERIFICATION_PENDING;
  if (shareProblems) buckets.add(OPERATIONAL_FILTER.EXTERNAL_SHARE_PROBLEMS);
  const needsAttention =
    shareProblems ||
    pdfProblems ||
    Boolean(row.publishedIntegrityError) ||
    row.customerReadyStatus ===
      CUSTOMER_READY_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED ||
    row.customerReadyStatus ===
      CUSTOMER_READY_STATUS.PRODUCTION_PUBLISHED_DATA_MISSING ||
    row.customerReadyStatus ===
      CUSTOMER_READY_STATUS.PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE ||
    row.customerReadyStatus ===
      CUSTOMER_READY_STATUS.PUBLISHED_SILENT_ROLLBACK_BLOCKED;
  if (needsAttention) buckets.add(OPERATIONAL_FILTER.NEEDS_ATTENTION);
  return [...buckets];
}

function classifyPublicationIntegrity({
  propertyId,
  stagedVersionId,
  publishedPeriodId,
  stagedPeriodId,
  publishedReportSha256 = null,
  stagedReportSha256 = null,
  publishedConsideration = null,
  stagedConsideration = null,
  fullE2eParity,
  fullAdpPdf,
  pdfResolution,
  latestReview,
  coverageStatus,
  publishedIds,
  adpClientProbe,
  liveExternalShareVerify = null,
}) {
  const stagedCertified =
    fullE2eParity?.ok === true && Boolean(stagedVersionId);
  const fullPdfCertified = fullAdpPdf?.available === true;
  const performanceReviewMatchesPublished =
    pdfResolution?.available === true &&
    pdfResolution?.matchesPublishedPeriod !== false;
  const performanceReviewStale =
    pdfResolution?.reason === "performance_review_pdf_stale_vs_published_period";
  const inPublishedUniverse = publishedIds.includes(propertyId);
  const stagedDiffersFromPublished = resolveStagedDiffersFromPublishedLineage({
    publishedPeriodId,
    stagedPeriodId,
    publishedReportSha256,
    stagedReportSha256,
    publishedConsideration,
    stagedConsideration,
  });

  let customerReadyStatus = CUSTOMER_READY_STATUS.NOT_READY;
  let requiredNextAction = "inspect_artifacts";
  const mismatchReasons = [];

  if (!stagedCertified) {
    mismatchReasons.push("staged_not_independently_certified");
    requiredNextAction = "certify_staged_report";
    customerReadyStatus = CUSTOMER_READY_STATUS.NOT_READY;
  } else if (!fullPdfCertified) {
    mismatchReasons.push("full_adp_pdf_missing_or_uncertified");
    requiredNextAction = "rebuild_full_adp_pdf_from_certified_staged";
    customerReadyStatus = CUSTOMER_READY_STATUS.REQUIRES_PDF_REBUILD;
  } else if (
    inPublishedUniverse &&
    stagedDiffersFromPublished &&
    stagedCertified &&
    fullPdfCertified &&
    performanceReviewMatchesPublished &&
    !performanceReviewStale
  ) {
    // Published customer surface remains independently verifiable; newer staged awaits approval.
    mismatchReasons.push("staged_period_ahead_of_published_period");
    customerReadyStatus =
      CUSTOMER_READY_STATUS.NEW_STAGED_VERSION_AWAITING_APPROVAL;
    requiredNextAction =
      "authorize_publication_of_certified_staged_or_keep_published_customer_ready";
  } else if (inPublishedUniverse && stagedDiffersFromPublished) {
    mismatchReasons.push("staged_period_ahead_of_published_period");
    requiredNextAction =
      "authorize_publication_of_certified_staged_then_rebuild_performance_review_pdf";
    customerReadyStatus = CUSTOMER_READY_STATUS.STAGED_CERTIFIED_NOT_PUBLISHED;
  } else if (performanceReviewStale || coverageStatus === COVERAGE_STATUS.NEEDS_REBUILD) {
    mismatchReasons.push(
      pdfResolution?.reason || "performance_review_pdf_needs_rebuild"
    );
    requiredNextAction = "rebuild_performance_review_pdf_against_published_period";
    customerReadyStatus = CUSTOMER_READY_STATUS.REQUIRES_PDF_REBUILD;
  } else if (
    inPublishedUniverse &&
    stagedCertified &&
    fullPdfCertified &&
    performanceReviewMatchesPublished &&
    !stagedDiffersFromPublished
  ) {
    // Candidate for CUSTOMER_READY — external share access is mandatory.
    customerReadyStatus = CUSTOMER_READY_STATUS.CUSTOMER_READY;
    requiredNextAction = "none_customer_ready";
  } else if (!inPublishedUniverse && stagedCertified && fullPdfCertified) {
    customerReadyStatus = CUSTOMER_READY_STATUS.REQUIRES_PUBLICATION;
    requiredNextAction = "authorize_first_publication";
    mismatchReasons.push("not_in_published_universe");
  }

  const shareAccess = classifyExternalShareAccessForCustomerReady({
    adpClientProbe,
    liveVerify: liveExternalShareVerify,
  });

  if (adpClientProbe?.tokenId && adpClientProbe?.available !== true) {
    mismatchReasons.push(
      adpClientProbe.reason || "external_share_token_present_but_not_resolvable"
    );
  }
  if (shareAccess.customerReadyBlocked) {
    mismatchReasons.push(shareAccess.reason || shareAccess.status);
  }

  const liveReasons = liveExternalShareVerify?.reasons || [];
  const productionPublishedMissing = liveReasons.some((r) =>
    /property_not_found|report_failed:property_not_found/i.test(String(r))
  );

  // Never claim CUSTOMER_READY when the external Admin URL cannot be opened.
  if (
    (customerReadyStatus === CUSTOMER_READY_STATUS.CUSTOMER_READY ||
      customerReadyStatus ===
        CUSTOMER_READY_STATUS.NEW_STAGED_VERSION_AWAITING_APPROVAL) &&
    shareAccess.customerReadyBlocked
  ) {
    if (productionPublishedMissing) {
      customerReadyStatus =
        CUSTOMER_READY_STATUS.PRODUCTION_PUBLISHED_DATA_MISSING;
      requiredNextAction =
        "authorize_production_deploy_sync_of_existing_published_snapshots";
    } else {
      customerReadyStatus = CUSTOMER_READY_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED;
      requiredNextAction =
        "repair_external_client_share_access_then_reverify_unauthenticated_browser";
    }
  }

  if (customerReadyStatus === CUSTOMER_READY_STATUS.CUSTOMER_READY) {
    // Alias for clarity in Admin dual-status displays.
    customerReadyStatus = CUSTOMER_READY_STATUS.PUBLISHED_CUSTOMER_READY;
  }

  // Independent live verification required for Customer Ready badge/count.
  // Probe-only (VERIFICATION_PENDING) is not sufficient.
  const independentlyVerified =
    shareAccess.status === EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED &&
    shareAccess.customerReadyBlocked !== true;
  if (
    (customerReadyStatus === CUSTOMER_READY_STATUS.PUBLISHED_CUSTOMER_READY ||
      customerReadyStatus ===
        CUSTOMER_READY_STATUS.NEW_STAGED_VERSION_AWAITING_APPROVAL) &&
    !independentlyVerified
  ) {
    mismatchReasons.push(
      shareAccess.reason || "customer_access_not_independently_verified"
    );
  }

  return {
    gate: ADP_PUBLICATION_INTEGRITY_ADMIN_V1,
    externalShareAccessGate: ADP_EXTERNAL_SHARE_ACCESS_GATE_V1,
    latestStagedVersionId: stagedVersionId || null,
    stagedPeriodId: stagedPeriodId || null,
    currentPublishedPeriodId: publishedPeriodId || null,
    stagedDiffersFromPublished,
    fullAdpPdfVersion: fullAdpPdf?.stagedVersionId || null,
    fullAdpPdfSha256: fullAdpPdf?.pdfSha256 || null,
    fullAdpPdfCertified: fullPdfCertified,
    performanceReviewId: latestReview?.reviewId || null,
    performanceReviewPdfStatus: performanceReviewStale
      ? "STALE_VS_PUBLISHED_PERIOD"
      : performanceReviewMatchesPublished
        ? "MATCHES_PUBLISHED_PERIOD"
        : pdfResolution?.reason || latestReview?.pdfStatus || "UNKNOWN",
    performanceReviewPeriodId: pdfResolution?.periodId || null,
    certificationStatus: stagedCertified ? "STAGED_CERTIFIED" : "NOT_CERTIFIED",
    externalShareTokenId: adpClientProbe?.tokenId || null,
    externalShareAvailable: adpClientProbe?.available === true,
    externalShareReason: adpClientProbe?.reason || null,
    externalShareAccessStatus: shareAccess.status,
    externalShareScopeHint: "current_published",
    liveExternalShareVerify: liveExternalShareVerify
      ? {
          ok: liveExternalShareVerify.ok === true,
          status: liveExternalShareVerify.status || null,
          reasons: liveExternalShareVerify.reasons || [],
          urlRedacted: liveExternalShareVerify.urlRedacted || null,
        }
      : null,
    mismatchReasons,
    requiredNextAction,
    customerReadyStatus,
    monitoringCompleted: Boolean(stagedPeriodId || publishedPeriodId),
    stagedReportCertified: stagedCertified,
    pdfCertified: fullPdfCertified,
    publishedReportCurrent: inPublishedUniverse && !stagedDiffersFromPublished,
    externalClientUrlCurrent:
      inPublishedUniverse &&
      !stagedDiffersFromPublished &&
      adpClientProbe?.available === true &&
      shareAccess.customerReadyBlocked !== true,
    requiresRebuilding:
      !fullPdfCertified ||
      performanceReviewStale ||
      coverageStatus === COVERAGE_STATUS.NEEDS_REBUILD,
    requiresPublication: stagedDiffersFromPublished || !inPublishedUniverse,
    requiresDeployment: false,
    independentlyVerified,
    customerAccessStatus: shareAccess.status,
    customerReady:
      independentlyVerified &&
      (customerReadyStatus === CUSTOMER_READY_STATUS.CUSTOMER_READY ||
        customerReadyStatus === CUSTOMER_READY_STATUS.PUBLISHED_CUSTOMER_READY),
    publishedCustomerReady:
      customerReadyStatus === CUSTOMER_READY_STATUS.CUSTOMER_READY ||
      customerReadyStatus === CUSTOMER_READY_STATUS.PUBLISHED_CUSTOMER_READY ||
      customerReadyStatus ===
        CUSTOMER_READY_STATUS.NEW_STAGED_VERSION_AWAITING_APPROVAL,
  };
}

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

function classifyCoverage({
  eligibility,
  latestReview,
  pdfResolution,
  fullE2eParity,
  fullAdpPdf = null,
  inPublishedUniverse = false,
}) {
  // Never BLOCK solely for unpublished when founder-review path is eligible.
  if (!eligibility?.eligible) {
    const blockers = eligibility?.blockers || [];
    const onlyUnpublished =
      blockers.length === 1 && blockers[0] === "not_in_published_adp_universe";
    if (onlyUnpublished) {
      return {
        coverageStatus: COVERAGE_STATUS.NEEDS_REBUILD,
        coverageReason:
          "resolver_defect_unpublished_should_use_founder_review_path",
      };
    }
    return {
      coverageStatus: COVERAGE_STATUS.BLOCKED,
      coverageReason:
        blockers.join(", ") || eligibility?.status || "not_eligible",
    };
  }
  if (!latestReview) {
    // Full ADP E2E certified staged+PDF is sufficient for AI Admin READY.
    // Do not NEEDS_REBUILD solely because a legacy monthly-review archive is absent.
    if (
      eligibility?.sourceMode === "staged_founder_review" &&
      fullE2eParity?.adminReadyWithoutLegacyArchive === true
    ) {
      return {
        coverageStatus: COVERAGE_STATUS.READY,
        coverageReason: null,
        coverageBasis: "FULL_E2E_STAGED_CERTIFIED",
      };
    }
    // Durable published-current + certified Full ADP PDF is Admin READY without
    // a Performance Review / monthly-review archive artifact.
    if (
      inPublishedUniverse &&
      (fullAdpPdf?.available === true || fullAdpPdf?.adminPdfStatus === "READY")
    ) {
      return {
        coverageStatus: COVERAGE_STATUS.READY,
        coverageReason: null,
        coverageBasis: "DURABLE_PUBLISHED_FULL_ADP_PDF",
      };
    }
    return {
      coverageStatus: COVERAGE_STATUS.NEEDS_REBUILD,
      coverageReason:
        eligibility?.sourceMode === "staged_founder_review"
          ? "founder_review_artifact_missing"
          : "review_artifact_missing",
    };
  }
  if (
    pdfResolution?.reason === "performance_review_pdf_stale_vs_published_period"
  ) {
    if (eligibility?.sourceMode !== "staged_founder_review") {
      return {
        coverageStatus: COVERAGE_STATUS.NEEDS_REBUILD,
        coverageReason: "performance_review_pdf_stale_vs_published_period",
      };
    }
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
  if (!pdfResolution?.available) {
    if (
      eligibility?.sourceMode === "staged_founder_review" &&
      (latestReview.pdfStatus === PDF_STATUS.READY ||
        latestReview.pdfStatus === "READY")
    ) {
      return {
        coverageStatus: COVERAGE_STATUS.READY,
        coverageReason: null,
      };
    }
    return {
      coverageStatus: COVERAGE_STATUS.NEEDS_REBUILD,
      coverageReason: pdfResolution?.reason || "pdf_not_available",
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
  // Operational admin universe = canonical 31 real hotels (Core/Loyalty matrix).
  // Synthetic fixtures are never included. Published-only legacy mode remains opt-in.
  const publishedIds = listPublishedPropertyIds().sort();
  let universeIds;
  if (opts.publishedOnly === true) {
    universeIds = opts.includeCertifiedExtras
      ? listMonthlyReviewUniversePropertyIds()
      : publishedIds;
  } else if (opts.includeCertifiedExtras) {
    universeIds = [
      ...new Set([
        ...listOperationalAdpPropertyIds(),
        ...listMonthlyReviewUniversePropertyIds(),
      ]),
    ]
      .filter((id) => !isAdpSyntheticOrTestPropertyId(id))
      .sort();
  } else {
    universeIds = listOperationalAdpPropertyIds();
  }
  const archiveByProperty = latestArchiveByProperty(opts.includeArchived === true);

  const properties = universeIds.map((propertyId) => {
    let manifest = null;
    let report = null;
    let publishedIntegrityError = null;
    try {
      manifest = loadPublishedManifest(propertyId);
      report = loadPublishedReport(propertyId);
    } catch (err) {
      if (PUBLISHED_INTEGRITY_CODES.has(err?.code)) {
        publishedIntegrityError = err;
        const registryEntry = getPublishedCurrentEntry(propertyId);
        manifest = manifest || {
          propertyId,
          latestPeriodId: err.filesystemPeriodId || null,
          publishStatus: "IntegrityFailure",
          _registryExpectedPeriodId: registryEntry?.periodId || err.expectedPeriodId || null,
          _integrityCode: err.code,
        };
      } else {
        throw err;
      }
    }
    const eligibility = resolveAdpMonthlyReviewEligibilityV1(propertyId);
    const stagedManifest = loadStagedManifest(propertyId);
    const stagedWrap = loadStagedOwnerPayload(propertyId);
    const stagedPayload =
      stagedWrap?.ok !== false ? stagedWrap?.payload || null : null;
    const profile = loadPropertyProfile(propertyId);
    const stagedVersionId =
      stagedWrap?.versionId ||
      stagedManifest?.latestStagedVersionId ||
      stagedManifest?.latestVersionId ||
      stagedManifest?.versionId ||
      null;
    let fullE2eParity = { ok: false, reason: "not_assessed" };
    try {
      fullE2eParity = assessFullAdpE2eAiAdminCertificationParity(propertyId);
    } catch (err) {
      if (PUBLISHED_INTEGRITY_CODES.has(err?.code)) {
        publishedIntegrityError = publishedIntegrityError || err;
        fullE2eParity = {
          ok: false,
          reason: err.code,
          integrityBlocked: true,
          filesystemPeriodId: err.filesystemPeriodId || null,
          expectedPeriodId: err.expectedPeriodId || null,
        };
      } else {
        throw err;
      }
    }
    const latestReview = archiveByProperty.get(propertyId) || null;
    let founder = {
      founderReviewStatus: FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_NEEDS_ARTIFACT,
      eligibleForFounderReview: false,
      periodId: manifest?.latestPeriodId || null,
    };
    try {
      founder = resolveAdpFounderReviewReadinessV1(propertyId, {
        hasReviewArtifact:
          Boolean(latestReview?.reviewId) || fullE2eParity.ok === true,
        hasPdfReady:
          latestReview?.pdfStatus === PDF_STATUS.READY ||
          latestReview?.pdfStatus === "READY" ||
          Boolean(fullE2eParity.pdfPath),
      });
    } catch (err) {
      if (PUBLISHED_INTEGRITY_CODES.has(err?.code)) {
        publishedIntegrityError = publishedIntegrityError || err;
        founder = {
          ...founder,
          integrityBlocked: true,
          integrityCode: err.code,
        };
      } else {
        throw err;
      }
    }
    const currentPeriodId =
      eligibility.currentPeriodId ||
      fullE2eParity.corePeriodId ||
      manifest?.latestPeriodId ||
      manifest?.monitoringPeriodId ||
      report?.period?.periodId ||
      stagedPayload?.period?.periodId ||
      founder.periodId ||
      null;
    const stagedFounder = eligibility?.sourceMode === "staged_founder_review";
    const pdfResolution = resolveAdpReportPdf({
      reviewId: latestReview?.reviewId || null,
      propertyId,
      publishedPeriodId: currentPeriodId,
      // Staged founder reviews must not require Live published period match.
      requirePublishedPeriodMatch:
        Boolean(currentPeriodId) && !stagedFounder && publishedIds.includes(propertyId),
    });
    const classified = classifyCoverage({
      eligibility,
      latestReview,
      pdfResolution,
      fullE2eParity,
      fullAdpPdf: fullE2eParity?.pdfPath
        ? { available: true, adminPdfStatus: "READY" }
        : null,
      inPublishedUniverse: publishedIds.includes(propertyId),
    });
    const coverageStatus = classified.coverageStatus;
    const coverageReason = classified.coverageReason;
    const agenda = actionAgendaCount(latestReview?.reviewId);
    const hasPdf =
      Boolean(pdfResolution.available) ||
      (stagedFounder &&
        (latestReview?.pdfStatus === PDF_STATUS.READY ||
          latestReview?.pdfStatus === "READY" ||
          Boolean(fullE2eParity.pdfPath)));

    // Founder-review readiness follows Full E2E certified staged + Full ADP PDF,
    // not the separately gated performance-review-vs-published-period match.
    let founderReviewStatus = founder.founderReviewStatus;
    if (fullE2eParity.ok === true && Boolean(fullE2eParity.pdfPath)) {
      founderReviewStatus = FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_READY;
    } else if (
      coverageStatus === COVERAGE_STATUS.READY &&
      (founder.eligibleForFounderReview || fullE2eParity.ok === true)
    ) {
      founderReviewStatus = FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_READY;
    } else if (
      coverageStatus === COVERAGE_STATUS.NEEDS_REBUILD &&
      founder.eligibleForFounderReview &&
      fullE2eParity.ok !== true
    ) {
      founderReviewStatus = FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_NEEDS_ARTIFACT;
    }

    const stagedExecutionDate =
      stagedPayload?.period?.executionDate ||
      stagedPayload?.period?.monitoringDate ||
      stagedManifest?.stagedAt ||
      null;
    const stagedReportingMonth =
      stagedPayload?.period?.reportingMonth ||
      stagedPayload?.period?.label ||
      (stagedExecutionDate
        ? String(stagedExecutionDate).slice(0, 7)
        : null);

    return {
      propertyId,
      propertyName:
        report?.property?.name ||
        manifest?.propertyName ||
        stagedPayload?.property?.name ||
        profile?.name ||
        latestReview?.propertyName ||
        propertyId,
      market:
        manifest?.market ||
        report?.property?.market ||
        report?.property?.state ||
        stagedPayload?.property?.market ||
        profile?.market ||
        latestReview?.market ||
        null,
      stagedVersionId,
      customerDeliveryCertified:
        Boolean(stagedVersionId) &&
        (founder.eligibleForFounderReview === true || fullE2eParity.ok === true),
      publicationStatus: publishedIds.includes(propertyId)
        ? "PUBLISHED"
        : "NOT_AUTHORIZED",
      fullE2eCertificationParity: fullE2eParity.ok === true,
      coverageBasis: classified.coverageBasis || null,
      bppRank: fullE2eParity.bpp?.portfolioRank || null,
      currentPeriodId:
        currentPeriodId || latestReview?.sourceCurrentPeriodId || null,
      reviewPeriodId: pdfResolution.periodId || latestReview?.sourceCurrentPeriodId || currentPeriodId || null,
      reportingMonth: latestReview?.reportingMonth || stagedReportingMonth || null,
      currentMonitoringDate:
        latestReview?.currentMonitoringDate || stagedExecutionDate || null,
      priorRunDate: latestReview?.priorRunDate || null,
      versionLabel:
        latestReview?.versionLabel ||
        (stagedVersionId ? `staged:${stagedVersionId}` : null),
      isCurrent: latestReview?.isCurrent !== false,
      reviewStatus:
        latestReview?.reviewStatus ||
        (fullE2eParity.ok ? "READY_FOR_REVIEW" : null),
      generationStatus:
        latestReview?.generationStatus ||
        (fullE2eParity.ok ? "SUCCESS" : null),
      clientIssued: Boolean(
        latestReview?.clientIssuedAt ||
          latestReview?.reviewStatus === "CLIENT_ISSUED"
      ),
      coverageStatus,
      coverageReason,
      founderReviewStatus,
      publishedReadyStatus: founder.publishedReadyStatus,
      publishedReady: founder.publishedReady === true,
      claimsCoreCertified: founder.claimsCoreCertified === true,
      sourceMode: eligibility.sourceMode || (publishedIds.includes(propertyId) ? "published" : "staged_founder_review"),
      eligibilityStatus: eligibility.status,
      eligible: Boolean(eligibility.eligible),
      clientReady: Boolean(eligibility.clientReady),
      blockers: eligibility.blockers || [],
      warnings: eligibility.warnings || [],
      reviewId: latestReview?.reviewId || null,
      pdfStatus: hasPdf
        ? PDF_STATUS.READY
        : pdfResolution.reason ===
            "performance_review_pdf_stale_vs_published_period"
          ? "STALE"
          : latestReview?.pdfStatus || null,
      generatedAt: latestReview?.generatedAt || null,
      hasPdf,
      pdfKind: "ai_demand_performance_review",
      pdfGate: stagedFounder
        ? FOUNDER_VS_PUBLISHED_DOCTRINE_V1
        : AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS,
      pdfPeriodMatchGate: AI_DEMAND_ADMIN_PDF_REQUIRES_PUBLISHED_PERIOD_MATCH,
      pdfMatchesPublishedPeriod: stagedFounder
        ? true
        : pdfResolution.matchesPublishedPeriod,
      pdfUnavailableReason: hasPdf
        ? null
        : pdfResolution.reason || "pdf_not_available",
      pdfViewUrl: pdfResolution.viewUrl || null,
      pdfDownloadUrl: pdfResolution.downloadUrl || null,
      pdfFingerprint: pdfResolution.pdfFingerprint || null,
      actionCount: agenda.actionCount || latestReview?.actionCount || 0,
      actionTitles: agenda.actionTitles,
      actionSource: "actionAgenda",
      actionGate: AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY,
      inPublishedUniverse: publishedIds.includes(propertyId),
      livePriorityActionCount: Array.isArray(report?.actions)
        ? report.actions.length
        : 0,
      adpClient: (() => {
        const probe = probeAdpClientShareAvailable(propertyId);
        return {
          available: Boolean(probe.available),
          tokenId: probe.tokenId || null,
          reason: probe.reason || null,
          tokenPresent: Boolean(probe.tokenId),
        };
      })(),
      fullAdpPdf: (() => {
        const full = resolveFullAdpE2ePdfArtifact(propertyId, {
          expectedStagedVersionId: stagedVersionId || null,
        });
        return {
          available: full.available === true,
          artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
          stagedVersionId: full.stagedVersionId || stagedVersionId || null,
          pdfSha256: full.pdfSha256 || null,
          viewUrl: full.viewUrl || null,
          downloadUrl: full.downloadUrl || null,
          reason: full.reason || null,
          status:
            full.available === true
              ? "CERTIFIED"
              : full.reason || "MISSING",
        };
      })(),
      performanceReviewPdf: {
        available: Boolean(hasPdf),
        artifactType: ADP_ARTIFACT_TYPE_MONTHLY_EXECUTIVE_REVIEW_PDF,
        reviewId: latestReview?.reviewId || null,
        viewUrl: pdfResolution.viewUrl || null,
        status:
          pdfResolution?.reason ===
          "performance_review_pdf_stale_vs_published_period"
            ? "STALE_VS_PUBLISHED_PERIOD"
            : hasPdf
              ? "READY"
              : pdfResolution?.reason || latestReview?.pdfStatus || "MISSING",
        note: "AI Demand Performance Review / Monthly Executive Review — distinct from Full ADP Report PDF and from the separate Spice Island AI Visibility Audit & Action Plan.",
      },
      publicationIntegrity: null,
      publishedIntegrityError: publishedIntegrityError
        ? {
            code: publishedIntegrityError.code,
            message: publishedIntegrityError.message,
            expectedPeriodId: publishedIntegrityError.expectedPeriodId || null,
            filesystemPeriodId: publishedIntegrityError.filesystemPeriodId || null,
          }
        : null,
    };
  }).map((row) => {
    const stagedWrap = loadStagedOwnerPayload(row.propertyId);
    const stagedPayload =
      stagedWrap?.ok !== false ? stagedWrap?.payload || null : null;
    const stagedPeriodId =
      stagedPayload?.period?.periodId ||
      stagedPayload?.periodId ||
      null;
    let publishedManifest = null;
    let integrityErr = row.publishedIntegrityError || null;
    try {
      publishedManifest = loadPublishedManifest(row.propertyId);
    } catch (err) {
      if (PUBLISHED_INTEGRITY_CODES.has(err?.code)) {
        integrityErr = {
          code: err.code,
          message: err.message,
          expectedPeriodId: err.expectedPeriodId || null,
          filesystemPeriodId: err.filesystemPeriodId || null,
        };
      } else {
        throw err;
      }
    }
    const registryEntry = getPublishedCurrentEntry(row.propertyId);
    const publishedPeriodId =
      registryEntry?.periodId ||
      publishedManifest?.latestPeriodId ||
      publishedManifest?.monitoringPeriodId ||
      null;
    let publishedReportForIdentity = null;
    try {
      publishedReportForIdentity = loadPublishedReport(row.propertyId);
    } catch (_e) {
      publishedReportForIdentity = null;
    }
    const stagedManifestEarly = loadStagedManifest(row.propertyId);
    const publishedConsiderationEarly = considerationFromReport(
      publishedReportForIdentity
    );
    const stagedConsiderationEarly = considerationFromReport(stagedPayload);
    const publishedReportSha256Early =
      publishedManifest?.reportSha256 ||
      publishedManifest?.latestReportSha256 ||
      null;
    const stagedReportSha256Early =
      stagedManifestEarly?.artifactHash ||
      stagedManifestEarly?.reportSha256 ||
      stagedWrap?.artifactHash ||
      null;
    let fullE2eParity = { ok: false, reason: "not_assessed" };
    try {
      fullE2eParity = assessFullAdpE2eAiAdminCertificationParity(
        row.propertyId
      );
    } catch (err) {
      if (PUBLISHED_INTEGRITY_CODES.has(err?.code)) {
        integrityErr = integrityErr || {
          code: err.code,
          message: err.message,
          expectedPeriodId: err.expectedPeriodId || null,
          filesystemPeriodId: err.filesystemPeriodId || null,
        };
        fullE2eParity = {
          ok: false,
          reason: err.code,
          integrityBlocked: true,
        };
      } else {
        throw err;
      }
    }
    const pdfResolution = resolveAdpReportPdf({
      reviewId: row.reviewId || null,
      propertyId: row.propertyId,
      publishedPeriodId: publishedPeriodId || row.currentPeriodId,
      requirePublishedPeriodMatch:
        Boolean(publishedPeriodId) && publishedIds.includes(row.propertyId) && !integrityErr,
    });
    const probe = probeAdpClientShareAvailable(row.propertyId);
    const liveExternalShareVerify = getLiveExternalShareVerifyForProperty(
      row.propertyId,
      {
        measurementPeriodId: publishedPeriodId || row.currentPeriodId || null,
        considerationRate:
          publishedConsiderationEarly != null
            ? publishedConsiderationEarly
            : null,
        pdfArtifactSha256: pdfResolution?.sha256 || null,
        methodologyFingerprintSha256: null,
        publicationManifestSha256: null,
      }
    );
    row.publicationIntegrity = classifyPublicationIntegrity({
      propertyId: row.propertyId,
      stagedVersionId: row.stagedVersionId,
      publishedPeriodId,
      stagedPeriodId,
      publishedReportSha256: publishedReportSha256Early,
      stagedReportSha256: stagedReportSha256Early,
      publishedConsideration: publishedConsiderationEarly,
      stagedConsideration: stagedConsiderationEarly,
      fullE2eParity,
      fullAdpPdf: row.fullAdpPdf,
      pdfResolution,
      latestReview: row.reviewId
        ? {
            reviewId: row.reviewId,
            pdfStatus: row.pdfStatus,
          }
        : null,
      coverageStatus: row.coverageStatus,
      publishedIds,
      adpClientProbe: probe,
      liveExternalShareVerify,
    });
    if (integrityErr) {
      const status =
        integrityErr.code === ADP_PRODUCTION_PUBLISHED_DATA_MISSING
          ? CUSTOMER_READY_STATUS.PRODUCTION_PUBLISHED_DATA_MISSING
          : integrityErr.code === ADP_PUBLISHED_SILENT_ROLLBACK_BLOCKED
            ? CUSTOMER_READY_STATUS.PUBLISHED_SILENT_ROLLBACK_BLOCKED
            : CUSTOMER_READY_STATUS.PUBLISHED_SNAPSHOT_INTEGRITY_FAILURE;
      row.publicationIntegrity = {
        ...row.publicationIntegrity,
        customerReadyStatus: status,
        customerReady: false,
        requiredNextAction:
          "authorize_atomic_production_deploy_of_published_current_registry_and_matching_snapshots",
        publishedIntegrityError: integrityErr,
        mismatchReasons: [
          ...(row.publicationIntegrity.mismatchReasons || []),
          integrityErr.code,
        ],
      };
    }
    row.customerReadyStatus =
      row.publicationIntegrity.customerReadyStatus;
    row.customerReady = row.publicationIntegrity.customerReady;
    row.requiredNextAction =
      row.publicationIntegrity.requiredNextAction;

    // Enrich Admin table fields — durable published-current is authoritative.
    const authoritative = resolveAuthoritativePublishedColumn(row.propertyId);
    const stagedWrap2 = loadStagedOwnerPayload(row.propertyId);
    const stagedPayload2 =
      stagedWrap2?.ok !== false ? stagedWrap2?.payload || null : null;
    const pubPeriod =
      authoritative.publishedPeriodId ||
      row.publicationIntegrity?.currentPublishedPeriodId ||
      publishedManifest?.latestPeriodId ||
      null;
    const stgPeriod =
      row.publicationIntegrity?.stagedPeriodId ||
      stagedPayload2?.period?.periodId ||
      null;
    row.publishedAuthoritySource = authoritative.authoritySource;
    row.publishedPeriodId = pubPeriod;
    row.publishedPeriodShort = shortPeriodId(pubPeriod);
    // Measured = certified measurement period; Published = durable publication event.
    // Never fall back Published label to measurementPeriodId month/year.
    row.measurementPeriodId = pubPeriod;
    row.measuredMonthYear =
      authoritative.measuredMonthYear || monthYearFromPeriodId(pubPeriod);
    row.publishedAt =
      authoritative.publishedAt || publishedManifest?.latestPublishedAt || null;
    row.publishedMonthYear = authoritative.publishedMonthYear || null;
    row.publicationTimestampMissing =
      authoritative.publicationTimestampMissing === true ||
      (Boolean(pubPeriod) && !row.publishedAt);
    row.publishedConsideration = authoritative.publishedConsideration;
    row.publishedReportSha256 =
      authoritative.publishedReportSha256 ||
      publishedManifest?.reportSha256 ||
      publishedManifest?.latestReportSha256 ||
      null;
    row.methodologyFingerprintSha256 =
      authoritative.methodologyFingerprintSha256 || null;
    row.sourceStagedVersionId = authoritative.sourceStagedVersionId || null;
    if (authoritative.integrityError && !row.publishedIntegrityError) {
      row.publishedIntegrityError = authoritative.integrityError;
    }
    const stagedManifest = loadStagedManifest(row.propertyId);
    row.stagedPeriodId = stgPeriod;
    row.stagedPeriodShort = shortPeriodId(stgPeriod);
    row.stagedMonthYear = monthYearFromPeriodId(stgPeriod);
    row.stagedConsideration = considerationFromReport(stagedPayload2);
    row.stagedReportSha256 =
      stagedManifest?.artifactHash ||
      stagedManifest?.reportSha256 ||
      stagedWrap2?.artifactHash ||
      null;
    row.stagedAt =
      stagedWrap2?.stagedAt ||
      stagedPayload2?.period?.executionDate ||
      null;
    // Reconcile lineage with authoritative customer content identity (period + Consideration).
    row.newDraftDiffersByLineage = resolveStagedDiffersFromPublishedLineage({
      publishedPeriodId: pubPeriod,
      stagedPeriodId: stgPeriod,
      publishedReportSha256: row.publishedReportSha256,
      stagedReportSha256: row.stagedReportSha256,
      publishedConsideration: row.publishedConsideration,
      stagedConsideration: row.stagedConsideration,
    });
    if (row.publicationIntegrity) {
      row.publicationIntegrity.stagedDiffersFromPublished =
        row.newDraftDiffersByLineage;
      row.publicationIntegrity.currentPublishedPeriodId = pubPeriod;
      row.publicationIntegrity.publishedReportSha256 = row.publishedReportSha256;
      row.publicationIntegrity.stagedReportSha256 = row.stagedReportSha256;
      row.publicationIntegrity.publishedConsideration = row.publishedConsideration;
      row.publicationIntegrity.stagedConsideration = row.stagedConsideration;
      row.publicationIntegrity.contentIdentityContract =
        "ADP_ADMIN_STAGED_VS_PUBLISHED_CONTENT_IDENTITY_V1";
    }
    try {
      row.publicationRegistryRecord =
        getHotelPublicationRecord(row.propertyId) || null;
    } catch (_e) {
      row.publicationRegistryRecord = null;
    }
    row.newDraftStatus = resolveNewDraftStatus(row);
    // If content identity says staged ≡ published, clear false "requires publication" states.
    if (
      row.publicationIntegrity &&
      row.newDraftDiffersByLineage === false &&
      row.inPublishedUniverse &&
      (row.customerReadyStatus ===
        CUSTOMER_READY_STATUS.STAGED_CERTIFIED_NOT_PUBLISHED ||
        row.customerReadyStatus ===
          CUSTOMER_READY_STATUS.NEW_STAGED_VERSION_AWAITING_APPROVAL)
    ) {
      row.publicationIntegrity.requiresPublication = false;
      row.publicationIntegrity.mismatchReasons = (
        row.publicationIntegrity.mismatchReasons || []
      ).filter((r) => r !== "staged_period_ahead_of_published_period");
      if (
        row.publicationIntegrity.externalShareAccessStatus ===
          EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED ||
        row.publicationIntegrity.externalShareAccessStatus ===
          EXTERNAL_SHARE_ACCESS_STATUS.VERIFICATION_PENDING
      ) {
        row.customerReadyStatus =
          row.publicationIntegrity.externalShareAccessStatus ===
          EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED
            ? CUSTOMER_READY_STATUS.CUSTOMER_READY
            : CUSTOMER_READY_STATUS.PUBLISHED_CUSTOMER_READY;
        row.publicationIntegrity.customerReadyStatus = row.customerReadyStatus;
        row.customerReady =
          row.customerReadyStatus === CUSTOMER_READY_STATUS.CUSTOMER_READY;
        row.publicationIntegrity.customerReady = row.customerReady;
      }
    }

    // PDF column: published package only — sealed local PDF ≠ Ready until registered/retrievable.
    const pdfColumn = resolveAdminPublishedPdfColumn({
      propertyId: row.propertyId,
      publishedPeriodId: pubPeriod,
      expectedStagedVersionId: row.sourceStagedVersionId,
      expectedPdfSha256: authoritative.pdfArtifactSha256 || null,
    });
    row.fullAdpPdf = {
      available: pdfColumn.available === true,
      artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
      stagedVersionId:
        pdfColumn.stagedVersionId || row.sourceStagedVersionId || null,
      pdfSha256: pdfColumn.pdfSha256 || null,
      viewUrl: pdfColumn.viewUrl || null,
      downloadUrl: pdfColumn.downloadUrl || null,
      reason: pdfColumn.reason || null,
      status:
        pdfColumn.status === ADMIN_PDF_STATUS.READY
          ? "CERTIFIED"
          : pdfColumn.status || "MISSING",
      adminPdfStatus: pdfColumn.status,
      periodId: pubPeriod,
      previewMatchesDownload: pdfColumn.previewMatchesDownload,
    };
    if (pdfColumn.status === ADMIN_PDF_STATUS.READY) {
      row.overallPdfStatus = {
        status: ADMIN_PDF_STATUS.READY,
        label: "Ready",
        kind: "ok",
        detail: "Published Full ADP PDF retrievable and period-matched",
        fullAdp: {
          status: ADMIN_PDF_STATUS.READY,
          available: true,
          label: "Ready",
        },
        performanceReview: {
          status: row.performanceReviewPdf?.available ? "READY" : "MISSING",
          available: row.performanceReviewPdf?.available === true,
          label: row.performanceReviewPdf?.available ? "Ready" : "Missing",
        },
      };
      // Durable published + Full ADP PDF Ready ⇒ Admin coverage READY even when
      // Performance Review archive rows are absent/stale/unloadable.
      // Do not keep NEEDS_REBUILD for legacy monthly-review archive defects when
      // the published Full ADP package is independently retrievable.
      if (
        row.inPublishedUniverse &&
        (row.coverageStatus === COVERAGE_STATUS.NEEDS_REBUILD ||
          row.coverageStatus === COVERAGE_STATUS.BLOCKED)
      ) {
        const reason = String(row.coverageReason || "");
        const archiveDefect =
          !reason ||
          /review_artifact_missing|published_report_missing|pdf_missing_or_pending|founder_review_artifact_missing|review_load_failed|pdf_not_available|performance_review_pdf/i.test(
            reason
          );
        if (archiveDefect) {
          row.coverageStatus = COVERAGE_STATUS.READY;
          row.coverageReason = null;
          row.coverageBasis = "DURABLE_PUBLISHED_FULL_ADP_PDF";
        }
      }
    } else {
      row.overallPdfStatus = {
        status: pdfColumn.status || ADMIN_PDF_STATUS.MISSING,
        label: pdfColumn.label || "Missing",
        kind: pdfColumn.kind || "warn",
        detail: pdfColumn.reason || "Published Full ADP PDF not retrievable",
        fullAdp: {
          status: pdfColumn.status || ADMIN_PDF_STATUS.MISSING,
          available: false,
          label: pdfColumn.label || "Missing",
        },
        performanceReview: {
          status: row.performanceReviewPdf?.available ? "READY" : "MISSING",
          available: row.performanceReviewPdf?.available === true,
          label: row.performanceReviewPdf?.available ? "Ready" : "Missing",
        },
      };
    }
    // Re-bind live share verify to authoritative publication identity (fail closed on drift).
    const liveVerifyAuthoritative = getLiveExternalShareVerifyForProperty(
      row.propertyId,
      {
        measurementPeriodId: pubPeriod,
        considerationRate:
          authoritative.publishedConsideration != null
            ? authoritative.publishedConsideration
            : publishedConsiderationEarly,
        pdfArtifactSha256:
          authoritative.pdfArtifactSha256 || pdfColumn.pdfSha256 || null,
        methodologyFingerprintSha256:
          authoritative.methodologyFingerprintSha256 || null,
        publicationManifestSha256:
          authoritative.publicationManifestSha256 || null,
      }
    );
    if (liveVerifyAuthoritative) {
      const shareAccess = classifyExternalShareAccessForCustomerReady({
        adpClientProbe: probe,
        liveVerify: liveVerifyAuthoritative,
      });
      row.publicationIntegrity = {
        ...row.publicationIntegrity,
        liveExternalShareVerify: {
          ok: liveVerifyAuthoritative.ok === true,
          status: liveVerifyAuthoritative.status || null,
          reasons: liveVerifyAuthoritative.reasons || [],
          urlRedacted: liveVerifyAuthoritative.urlRedacted || null,
          verifiedAt: liveVerifyAuthoritative.verifiedAt || null,
          tokenId: liveVerifyAuthoritative.tokenId || null,
        },
        customerAccessStatus: shareAccess.status,
        externalShareAccessStatus: shareAccess.status,
      };
    }
    row.customerAccess = resolveCustomerAccessDisplay(row);
    row.customerAccessVerifiedAt =
      row.customerAccess?.lastVerifiedAt ||
      row.publicationIntegrity?.liveExternalShareVerify?.verifiedAt ||
      null;
    row.publishAction = resolvePublishActionState(row);
    row.adminActions = buildAdminActionsAgenda({
      inPublishedUniverse: row.inPublishedUniverse === true,
      publishAction: row.publishAction,
      pdfAvailable: pdfColumn.available === true,
      pdfReason: pdfColumn.reason,
      shareAvailable: Boolean(row.adpClient?.available && row.adpClient?.tokenId),
      pdfViewUrl: pdfColumn.viewUrl,
      pdfDownloadUrl: pdfColumn.downloadUrl,
    });
    row.publicationColumnsAuthority =
      ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1;
    row.humanReasons = [
      ...(row.publicationIntegrity?.mismatchReasons || []),
      row.coverageReason,
      row.pdfUnavailableReason,
      row.publishedIntegrityError?.code,
    ]
      .filter(Boolean)
      .map(humanizeAdminReason);
    // Spice Island recovery: certified newer period must not be shown as live.
    if (
      row.propertyId === "adp_spice_island_beach_resort" &&
      pubPeriod &&
      /966d88$/i.test(String(pubPeriod)) &&
      stgPeriod &&
      /2209c8$/i.test(String(stgPeriod))
    ) {
      row.recoveryPending = {
        periodId: stgPeriod,
        periodShort: shortPeriodId(stgPeriod),
        consideration: row.stagedConsideration ?? 67.2,
        note: "Certified recovery is staged only — not live until migration/activation.",
      };
    } else if (
      row.propertyId === "adp_spice_island_beach_resort" &&
      pubPeriod &&
      /966d88$/i.test(String(pubPeriod))
    ) {
      row.recoveryPending = {
        periodId: "adp_period_adp_spice_island_beach_resort_20261020082916_2209c8",
        periodShort: "…2209c8",
        consideration: 67.2,
        note: "Certified recovery target — not live until production migration/activation.",
      };
    } else {
      row.recoveryPending = null;
    }
    row.operationalBuckets = computeOperationalBuckets(row);
    row.issueCount = (row.humanReasons || []).length;
    return row;
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

  const founderReadyCount = properties.filter(
    (p) => p.founderReviewStatus === FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_READY
  ).length;
  const publishedReadyCount = properties.filter(
    (p) => p.publishedReadyStatus === PUBLISHED_READY_STATUS.PUBLISHED_READY
  ).length;
  const customerReadyCount = properties.filter(
    (p) => p.customerReady === true
  ).length;
  const fullAdpPdfCertifiedCount = properties.filter(
    (p) => p.fullAdpPdf?.available === true
  ).length;
  const requiresPublicationCount = properties.filter(
    (p) => p.publicationIntegrity?.requiresPublication === true
  ).length;

  const realPublishedIds = publishedIds.filter(
    (id) => !isAdpSyntheticOrTestPropertyId(id)
  );
  const publishedOperationalCount = properties.filter((p) =>
    p.operationalBuckets?.includes(OPERATIONAL_FILTER.PUBLISHED)
  ).length;
  const awaitingReviewCount = properties.filter((p) =>
    p.operationalBuckets?.includes(OPERATIONAL_FILTER.AWAITING_REVIEW)
  ).length;
  const needsAttentionCount = properties.filter((p) =>
    p.operationalBuckets?.includes(OPERATIONAL_FILTER.NEEDS_ATTENTION)
  ).length;
  const unpublishedCount = properties.filter((p) =>
    p.operationalBuckets?.includes(OPERATIONAL_FILTER.UNPUBLISHED)
  ).length;

  const operationalSummary = {
    gate: ADP_ADMIN_OPERATIONAL_SUMMARY_V1,
    overlapNote:
      "Hotels can appear in more than one card (for example Published + Awaiting Publication, or Published + Needs Attention). A published hotel with a newer draft is not Unpublished.",
    cards: [
      {
        id: OPERATIONAL_FILTER.PUBLISHED,
        label: "Published",
        count: publishedOperationalCount,
        definition:
          "Hotels with an authorized published report on the current published filesystem SoT (synthetic fixtures excluded).",
      },
      {
        id: OPERATIONAL_FILTER.CUSTOMER_READY,
        label: "Customer Ready",
        count: customerReadyCount,
        definition:
          "Published report + independently verified external share + required customer artifacts confirmed.",
      },
      {
        id: OPERATIONAL_FILTER.AWAITING_PUBLICATION,
        label: "Awaiting Publication",
        count: awaitingReviewCount,
        definition:
          "Published hotel with a newer staged draft awaiting review/approval, or approved staged version not yet published. Distinct from Unpublished.",
        legacyFilterId: OPERATIONAL_FILTER.AWAITING_REVIEW,
      },
      {
        id: OPERATIONAL_FILTER.NEEDS_ATTENTION,
        label: "Needs Attention",
        count: needsAttentionCount,
        definition:
          "Unresolved PDF, external-share, publication-integrity, or delivery problems.",
      },
      {
        id: OPERATIONAL_FILTER.UNPUBLISHED,
        label: "Unpublished",
        count: unpublishedCount,
        definition: "Operational hotels without an authorized published report.",
      },
    ],
  };

  const coverageGeneration = getAdminPublicationCoverageGeneration();

  return {
    gate: ADP_AI_DEMAND_REVIEW_COVERAGE_V1,
    hygieneGate: ADP_OPERATIONAL_REGISTRY_HYGIENE_V1,
    publicationIntegrityGate: ADP_PUBLICATION_INTEGRITY_ADMIN_V1,
    publicationColumnsAuthority: ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1,
    coverageGeneration,
    operationalSummaryGate: ADP_ADMIN_OPERATIONAL_SUMMARY_V1,
    doctrine: {
      PUBLISHED_ADP_PROPERTY_EXPECTS_AI_DEMAND_REVIEW,
      AI_DEMAND_REVIEW_COVERAGE_MATCHES_PUBLISHED_ADP_UNIVERSE,
      AI_DEMAND_PERFORMANCE_REVIEW_USES_CURRENT_PUBLISHED_ADP_ANALYTICS,
      AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY,
      AI_DEMAND_ADMIN_PDF_REQUIRES_PUBLISHED_PERIOD_MATCH,
      ADP_OPERATIONAL_REGISTRY_HYGIENE_V1,
      FOUNDER_VS_PUBLISHED_DOCTRINE_V1,
      ADP_PUBLICATION_INTEGRITY_ADMIN_V1,
      ADP_ADMIN_OPERATIONAL_SUMMARY_V1,
    },
    publishedCount: realPublishedIds.length,
    universeCount: properties.length,
    reviewReadyCount,
    founderReadyCount,
    publishedReadyCount,
    customerReadyCount,
    awaitingReviewCount,
    needsAttentionCount,
    unpublishedCount,
    fullAdpPdfCertifiedCount,
    requiresPublicationCount,
    missingCount,
    blockedCount,
    silentlyOmitted: 0,
    publishedPropertyIds: realPublishedIds,
    operationalSummary,
    /** Legacy card aliases kept for older UI — prefer operationalSummary.cards */
    legacyCoverageCards: {
      published: realPublishedIds.length,
      reviewReady: reviewReadyCount,
      needsRebuild: missingCount,
      blocked: blockedCount,
      note: "Legacy performance-review coverage statuses; replaced by operationalSummary for Admin.",
    },
    properties,
    liveProviderCalls: 0,
  };
}
