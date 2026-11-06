/**
 * Separate FOUNDER_REVIEW_READY from PUBLISHED_READY.
 *
 * Doctrine:
 *   - Unpublished hotels may enter founder review when independently certified staged evidence exists.
 *   - Publication is never required merely to enter founder review.
 *   - An uncertified Core period (DRAFT/PARSED) must not be claimed CERTIFIED or PUBLISHED_READY
 *     merely because monitoring finished.
 *   - clientReady / Live PDF issuance still require published certified ADP.
 */

import { loadStagedOwnerPayload, loadStagedManifest } from "../review/adp-staged-report-store-v1.js";
import { loadLoyaltyBppStagedPack } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import { readbackDurableObservationArchive } from "../execution/durable-observation-archive-v1.js";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../published-snapshot.js";
import {
  isAdpSyntheticOrTestPropertyId,
  listOperationalAdpPropertyIds,
} from "../governance/adp-operational-registry-hygiene-v1.js";
import { existsSync, readFileSync } from "fs";
import { join } from "path";

export const ADP_FOUNDER_REVIEW_READINESS_V1 = "ADP_FOUNDER_REVIEW_READINESS_V1";

export const FOUNDER_REVIEW_STATUS = Object.freeze({
  FOUNDER_REVIEW_READY: "FOUNDER_REVIEW_READY",
  FOUNDER_REVIEW_NEEDS_ARTIFACT: "FOUNDER_REVIEW_NEEDS_ARTIFACT",
  FOUNDER_REVIEW_BLOCKED: "FOUNDER_REVIEW_BLOCKED",
});

export const PUBLISHED_READY_STATUS = Object.freeze({
  PUBLISHED_READY: "PUBLISHED_READY",
  NOT_PUBLISHED: "NOT_PUBLISHED",
  PUBLISHED_NEEDS_REBUILD: "PUBLISHED_NEEDS_REBUILD",
  PUBLISHED_NOT_CERTIFIED: "PUBLISHED_NOT_CERTIFIED",
});

const CORE_CERTIFIED = new Set([
  "CERTIFIED",
  "CERTIFIED_WITH_DISCLOSURES",
  "PRODUCTION_VALIDATED",
  "DEMO_VALIDATION",
]);

const REQUIRED_STAGED_SECTIONS = Object.freeze([
  "executiveRead|brief",
  "executiveMetrics|demandCapture",
  "competitiveSet",
  "realityGap",
]);

function loadRuntimePeriod(periodId, cwd = process.cwd()) {
  if (!periodId) return null;
  const p = join(cwd, "data/ai-demand-positioning/runtime", `${periodId}.json`);
  if (!existsSync(p)) return null;
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function sectionPresent(payload, key) {
  if (key.includes("|")) {
    return key.split("|").some((k) => {
      const v = payload?.[k];
      return v != null && (typeof v !== "object" || Object.keys(v).length > 0);
    });
  }
  const v = payload?.[key];
  return v != null && (typeof v !== "object" || Object.keys(v).length > 0);
}

function assessBpp(propertyId, payload) {
  const onStaged = payload?.brandPortfolioPosition;
  if (
    onStaged &&
    (onStaged.customerState === "BPP_NOT_APPLICABLE" ||
      onStaged.status === "BPP_NOT_APPLICABLE" ||
      onStaged.measuredRank === "BPP_NOT_APPLICABLE" ||
      onStaged.disclosureReason === "LENS_NOT_APPLICABLE")
  ) {
    return {
      ok: true,
      mode: "STAGED_BPP_NOT_APPLICABLE",
      readyRank: false,
      blocker: null,
      note: "Governed BPP N/A — portfolio rank not calculated; Core founder review + Full ADP PDF allowed",
    };
  }
  if (onStaged && (onStaged.kpis?.length || onStaged.ranking?.rows?.length)) {
    return {
      ok: true,
      mode: "STAGED_INLINE",
      readyRank: onStaged.certification?.readyRank === true || onStaged.status === "READY",
      blocker: null,
    };
  }
  const loy = loadLoyaltyBppStagedPack(propertyId);
  if (loy?.certification?.readyRank === true && loy?.payload) {
    return {
      ok: true,
      mode: "LOYALTY_BPP_READY_RANK",
      readyRank: true,
      packVersionId: loy.versionId || loy.stagedVersionId || null,
      blocker: null,
    };
  }
  if (
    loy?.certification?.subjectMetricsCertified === true ||
    String(loy?.certification?.status || "").includes("SUBJECT_MEASURED")
  ) {
    return {
      ok: true,
      mode: "LOYALTY_BPP_SUBJECT_MEASURED_PARTIAL",
      readyRank: false,
      packVersionId: loy.versionId || null,
      blocker: loy?.certification?.blocker || "PEER_SET_NOT_CERTIFIED",
      note: "BPP rank blocked; Core founder review still allowed with documented BPP limitation",
    };
  }
  return {
    ok: false,
    mode: "MISSING",
    readyRank: false,
    blocker: "BPP_PACK_OR_STAGED_SECTION_MISSING",
  };
}

/**
 * @param {string} propertyId
 * @param {{ cwd?: string, hasReviewArtifact?: boolean, hasPdfReady?: boolean }} [opts]
 */
export function resolveAdpFounderReviewReadinessV1(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  const blockers = [];
  const warnings = [];

  if (!id) {
    return {
      gate: ADP_FOUNDER_REVIEW_READINESS_V1,
      propertyId: null,
      founderReviewStatus: FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_BLOCKED,
      publishedReadyStatus: PUBLISHED_READY_STATUS.NOT_PUBLISHED,
      blockers: ["propertyId_required"],
      eligibleForFounderReview: false,
      publishedReady: false,
      claimsCoreCertified: false,
    };
  }

  if (isAdpSyntheticOrTestPropertyId(id)) {
    return {
      gate: ADP_FOUNDER_REVIEW_READINESS_V1,
      propertyId: id,
      founderReviewStatus: FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_BLOCKED,
      publishedReadyStatus: PUBLISHED_READY_STATUS.NOT_PUBLISHED,
      blockers: ["synthetic_fixture_not_operational"],
      eligibleForFounderReview: false,
      publishedReady: false,
      claimsCoreCertified: false,
      synthetic: true,
    };
  }

  const operational = listOperationalAdpPropertyIds().includes(id);
  if (!operational) {
    // Operational = canonical 31 ∪ commissioned Full ADP report properties (may be unpublished).
    blockers.push("not_in_operational_full_adp_report_universe");
  }

  const publishedIds = listPublishedPropertyIds();
  const inPublishedUniverse = publishedIds.includes(id);
  const pubManifest = loadPublishedManifest(id);
  const pubReport = loadPublishedReport(id);

  let publishedReadyStatus = PUBLISHED_READY_STATUS.NOT_PUBLISHED;
  if (inPublishedUniverse && pubManifest) {
    const cert =
      pubManifest.certificationStatus ||
      pubReport?.certificationStatus ||
      pubReport?.period?.certificationStatus ||
      null;
    const certified =
      (cert && CORE_CERTIFIED.has(String(cert))) ||
      pubManifest.publishStatus === "Live";
    if (!certified) {
      publishedReadyStatus = PUBLISHED_READY_STATUS.PUBLISHED_NOT_CERTIFIED;
    } else {
      publishedReadyStatus = PUBLISHED_READY_STATUS.PUBLISHED_READY;
    }
  }

  const stagedWrap = loadStagedOwnerPayload(id, cwd);
  const stagedManifest = loadStagedManifest(id, cwd);
  const payload = stagedWrap?.ok !== false ? stagedWrap?.payload || null : null;
  const periodId =
    stagedWrap?.periodId ||
    stagedManifest?.periodId ||
    payload?.period?.periodId ||
    payload?.periodId ||
    null;

  if (!payload) {
    blockers.push("staged_owner_report_missing");
  }

  const missingSections = [];
  if (payload) {
    for (const key of REQUIRED_STAGED_SECTIONS) {
      if (!sectionPresent(payload, key)) missingSections.push(key);
    }
    if (missingSections.length) {
      blockers.push(`staged_sections_incomplete:${missingSections.join("+")}`);
    }
  }

  const runtime = loadRuntimePeriod(periodId, cwd);
  const corePeriodStatus =
    runtime?.certificationStatus ||
    runtime?.status ||
    payload?.period?.certificationStatus ||
    null;
  const claimsCoreCertified = CORE_CERTIFIED.has(String(corePeriodStatus || ""));
  if (!claimsCoreCertified && corePeriodStatus) {
    warnings.push(
      `core_period_status_${String(corePeriodStatus).toLowerCase()}_not_claimed_certified`
    );
  }

  const readback = periodId ? readbackDurableObservationArchive(id, periodId) : null;
  const archiveOk =
    readback?.ok === true &&
    (readback.withOriginalResponse || 0) >= 1 &&
    readback.hashMatch !== false;
  if (!archiveOk) {
    // Fall back to runtime observation count when readback path differs
    const obsCount = runtime?.observations?.length || 0;
    if (obsCount < 1) blockers.push("original_response_archive_incomplete");
    else warnings.push("archive_readback_soft_pass_via_runtime_observations");
  }

  const bpp = assessBpp(id, payload);
  // Unpublished staged path requires a BPP pack or inline section.
  // Published Live hotels may already be admin-READY without a loyalty staged pack.
  if (!bpp.ok && !inPublishedUniverse) {
    blockers.push(bpp.blocker);
  } else if (!bpp.ok && inPublishedUniverse) {
    warnings.push("bpp_loyalty_pack_absent_published_path_ok");
  }

  const hasReviewArtifact = opts.hasReviewArtifact === true;
  const hasPdfReady = opts.hasPdfReady === true;

  const evidenceOk =
    operational &&
    ((inPublishedUniverse && publishedReadyStatus === PUBLISHED_READY_STATUS.PUBLISHED_READY) ||
      (!!payload &&
        missingSections.length === 0 &&
        (archiveOk || (runtime?.observations?.length || 0) > 0) &&
        (bpp.ok || inPublishedUniverse)));

  let founderReviewStatus = FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_BLOCKED;
  if (evidenceOk && blockers.length === 0) {
    founderReviewStatus =
      hasReviewArtifact && hasPdfReady
        ? FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_READY
        : FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_NEEDS_ARTIFACT;
  } else if (evidenceOk && blockers.every((b) => b.startsWith("staged_sections"))) {
    founderReviewStatus = FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_BLOCKED;
  }

  // Resolver defect correction: never block founder review solely for unpublished.
  const blockedOnlyForUnpublished = false;

  return {
    gate: ADP_FOUNDER_REVIEW_READINESS_V1,
    propertyId: id,
    founderReviewStatus,
    publishedReadyStatus,
    eligibleForFounderReview:
      founderReviewStatus === FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_READY ||
      founderReviewStatus === FOUNDER_REVIEW_STATUS.FOUNDER_REVIEW_NEEDS_ARTIFACT,
    publishedReady: publishedReadyStatus === PUBLISHED_READY_STATUS.PUBLISHED_READY,
    claimsCoreCertified,
    corePeriodStatus,
    periodId,
    inPublishedUniverse,
    operational,
    stagedPresent: !!payload,
    archiveOk: archiveOk || (runtime?.observations?.length || 0) > 0,
    withOriginalResponse: readback?.withOriginalResponse ?? runtime?.observations?.length ?? 0,
    bpp,
    missingSections,
    blockers,
    warnings,
    blockedOnlyForUnpublished,
    doctrine: {
      publicationNotRequiredForFounderReview: true,
      uncertifiedCoreNotAutoPublishedReady: true,
      monitoringFinishedDoesNotEqualCertified: true,
    },
  };
}

export function resolveAllAdpFounderReviewReadinessV1(opts = {}) {
  const ids = listOperationalAdpPropertyIds();
  const results = ids.map((id) => resolveAdpFounderReviewReadinessV1(id, opts));
  const counts = {
    FOUNDER_REVIEW_READY: 0,
    FOUNDER_REVIEW_NEEDS_ARTIFACT: 0,
    FOUNDER_REVIEW_BLOCKED: 0,
    PUBLISHED_READY: 0,
    NOT_PUBLISHED: 0,
  };
  for (const r of results) {
    counts[r.founderReviewStatus] = (counts[r.founderReviewStatus] || 0) + 1;
    if (r.publishedReadyStatus === PUBLISHED_READY_STATUS.PUBLISHED_READY) {
      counts.PUBLISHED_READY += 1;
    } else if (r.publishedReadyStatus === PUBLISHED_READY_STATUS.NOT_PUBLISHED) {
      counts.NOT_PUBLISHED += 1;
    }
  }
  return {
    gate: ADP_FOUNDER_REVIEW_READINESS_V1,
    counts,
    results,
  };
}
