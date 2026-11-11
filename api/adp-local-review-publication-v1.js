/**
 * ADP founder local review + controlled publication API.
 * Memberstack-authenticated only — never share-token accessible.
 * Does not alter customer /properties entitlement surface.
 */

import {
  listAdpReviewerProperties,
  getAdpReviewerProperty,
  ADP_REVIEWER_PROPERTY_SELECTOR_V1,
} from "../lib/ai-demand-positioning/review/list-adp-reviewer-properties-v1.js";
import {
  loadStagedOwnerPayload,
  loadStagedOwnerPayloadByVersionId,
  loadStagedManifest,
} from "../lib/ai-demand-positioning/review/adp-staged-report-store-v1.js";
import {
  approveStagedReportVersion,
  requestChangesOnStagedReport,
  getHotelPublicationRecord,
  recordPublishedVersion,
  transitionPublicationState,
  listAllPublicationRecords,
  upsertHotelPublicationRecord,
  ADP_PUBLICATION_STATES,
} from "../lib/ai-demand-positioning/review/adp-publication-registry-v1.js";
import { compareStagedVsPublishedReports } from "../lib/ai-demand-positioning/review/adp-staged-vs-published-compare-v1.js";
import {
  getPublishedOwnerReport,
  enrichPayloadOptionalMetrics,
} from "../lib/ai-demand-positioning/published-read-service.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
} from "../lib/dealality/property-display-label.js";
import {
  resolveAdpReviewCapability,
  requireAdpReviewCapability,
  ADP_REVIEW_CAPABILITY_V1,
} from "../lib/ai-demand-positioning/review/adp-review-capability-v1.js";
import { buildAdpAdminPublicationMatrix } from "../lib/ai-demand-positioning/review/adp-admin-publication-control-v1.js";
import {
  verifyAdpProductionDelivery,
  getAdpProductionBaseUrl,
  PRODUCTION_STATUS,
} from "../lib/ai-demand-positioning/review/adp-production-verification-v1.js";
import * as aiDemandPositioningApi from "./ai-demand-positioning.js";
import { resolveCanonicalBppPropertyId } from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import { attachCanonicalReportViewModelV1 } from "../lib/ai-demand-positioning/presentation/adp-canonical-report-view-model-v1.js";

/** Prefer exported selector when present; otherwise keep embedded BPP (RC trees may lag). */
function selectBrandPortfolioPositionForReport(args = {}) {
  const fn = aiDemandPositioningApi.selectBrandPortfolioPositionForReport;
  if (typeof fn === "function") return fn(args);
  return args.embeddedBpp || null;
}

function setNoStore(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

function resolveActor(req) {
  const u = req.dealalityUser || {};
  const id =
    req.memberstackMemberId ||
    u.memberstackId ||
    u.id ||
    (req.memberstackVerifiedVia === "dev_bypass" ? "dev_bypass_reviewer" : null);
  if (!id) return null;
  return {
    id: String(id),
    email: req.memberstackEmail || u.email || null,
    name: u.name || u.fullName || null,
  };
}

/** Platform-admin review gate — URL params alone never authorize. */
function requireMemberstackReviewer(req, res) {
  const cap = requireAdpReviewCapability(req, res);
  if (!cap) return null;
  const actor = resolveActor(req);
  if (!actor) {
    res.status(401).json({ ok: false, error: "authentication_required" });
    return null;
  }
  return { ...actor, isAdmin: true, capability: cap };
}

/** GET /api/ai-demand-positioning/review/capability */
export function getAdpReviewCapability(req, res) {
  setNoStore(res);
  const cap = resolveAdpReviewCapability(req);
  return res.json({
    ...cap,
    spaIntegration: {
      ownerRoute: "/app#/owner/ai-demand",
      adminRoute: "/app#/admin/ai-demand",
      adpStatusColumn: "ADP Status",
      urlHintInsufficient: true,
      sharedContract: "canonical_staged_snapshot_v1",
    },
  });
}

export function getAdpReviewerProperties(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;

  const includeExcluded = String(req.query.includeExcluded || "") === "1";
  const includeHistoricalExcluded =
    String(req.query.includeHistoricalExcluded || "1") !== "0";
  const includeResearchOnlyPeers =
    String(req.query.includeResearchOnlyPeers || "") === "1";
  const properties = listAdpReviewerProperties({
    includeExcluded,
    includeHistoricalExcluded,
    includeResearchOnlyPeers,
  });
  return res.json({
    ok: true,
    surface: "review",
    selectorVersion: ADP_REVIEWER_PROPERTY_SELECTOR_V1,
    capabilityVersion: ADP_REVIEW_CAPABILITY_V1,
    researchOnlyPeersIncluded: includeResearchOnlyPeers,
    propertyCount: properties.length,
    eligibleCount: properties.filter((p) => p.eligibility === "ELIGIBLE").length,
    researchExcludedWithHistoricalCount: properties.filter(
      (p) => p.researchEligibility === "EXCLUDED" && p.hasHistoricalReport
    ).length,
    properties,
    actor: { id: actor.id, email: actor.email, isAdmin: true },
    defaultReportVersion: "staged",
  });
}

/** AI Admin publication control matrix */
export function getAdpAdminPublicationControl(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;

  const includeExcluded = String(req.query.includeExcluded || "1") !== "0";
  const matrix = buildAdpAdminPublicationMatrix({ includeExcluded });
  return res.json({
    ...matrix,
    actor: { id: actor.id, email: actor.email },
    productionBaseUrlConfigured: !!getAdpProductionBaseUrl(),
  });
}

/** Optional live production probe for one hotel */
export async function getAdpProductionVerification(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;

  const propertyId = String(req.params.propertyId || req.query.propertyId || "").trim();
  if (!propertyId) {
    return res.status(400).json({ ok: false, error: "property_id_required" });
  }
  const pub = getHotelPublicationRecord(propertyId);
  const probe = await verifyAdpProductionDelivery({
    propertyId,
    expectedVersion: pub?.publishedReportVersion || null,
    historicalPublicationRecord: pub
      ? {
          publishedReportVersion: pub.publishedReportVersion,
          publishedAt: pub.publishedAt,
        }
      : null,
  });
  return res.json({
    ok: true,
    propertyId,
    probe,
    publicationRecord: pub,
    doctrine: {
      localPublishedFileIsNotProof: true,
      statuses: PRODUCTION_STATUS,
    },
    actor: { id: actor.id },
  });
}

export async function getAdpReviewerReport(req, res) {
  try {
    setNoStore(res);
    const actor = requireMemberstackReviewer(req, res);
    if (!actor) return;

    const rawPropertyId = String(req.params.propertyId || "").trim();
    const propertyId = resolveCanonicalBppPropertyId(rawPropertyId) || rawPropertyId;
    const version = String(req.query.version || "staged").toLowerCase();
    const profile = loadPropertyProfile(propertyId);
    const reviewerMeta = getAdpReviewerProperty(propertyId, { includeExcluded: true });

    if (!profile && !reviewerMeta) {
      return res.status(404).json({ ok: false, error: "property_not_found" });
    }

    if (reviewerMeta?.eligibility === "EXCLUDED" && version === "staged") {
      return res.status(422).json({
        ok: false,
        error: "property_excluded_from_adp_execution",
        message: reviewerMeta.exclusionReason,
        propertyId,
        publicationState: "EXCLUDED",
      });
    }

    const pubRec = getHotelPublicationRecord(propertyId);

    if (version === "published") {
      const result = await getPublishedOwnerReport(propertyId);
      if (!result.ok) {
        return res.status(404).json({
          ok: false,
          error: "published_report_unavailable",
          propertyId,
          message: "No published customer report for this property.",
          reviewMeta: reviewerMeta,
          publicationRecord: pubRec,
        });
      }
      return res.json({
        ...decoratePayload(
          result.payload,
          profile,
          propertyId,
          "PUBLISHED",
          pubRec,
          reviewerMeta,
          req
        ),
        _adpReportVersion: "PUBLISHED",
        _adpReadSource: result.source,
        ok: true,
      });
    }

    if (version === "staged") {
      const pin = String(
        req.query.stagedVersionId || req.query.reportVersionId || ""
      ).trim();
      const staged = pin
        ? loadStagedOwnerPayloadByVersionId(propertyId, pin)
        : loadStagedOwnerPayload(propertyId);
      if (!staged?.ok) {
        return res.status(404).json({
          ok: false,
          error: pin ? "staged_version_not_found" : "staged_report_unavailable",
          propertyId,
          stagedVersionId: pin || null,
          message: pin
            ? "Pinned staged version not found. Published customer report was not substituted."
            : "No staged report for review. Run ADP Full E2E staging or seed from corrected pipeline output.",
          reviewMeta: reviewerMeta,
          publicationRecord: pubRec,
          // Explicit: do NOT fall back to published
          publishedFallbackUsed: false,
        });
      }
      return res.json({
        ...decoratePayload(
          staged.payload,
          profile,
          propertyId,
          "STAGED",
          pubRec,
          reviewerMeta,
          req
        ),
        _adpReportVersion: "STAGED",
        _adpStagedVersionId: staged.versionId,
        _adpArtifactHash: staged.artifactHash,
        _adpReadSource: staged.source,
        _adpVersionPin: Boolean(pin),
        ok: true,
      });
    }

    return res.status(400).json({
      ok: false,
      error: "invalid_version",
      message: "version must be staged or published",
    });
  } catch (err) {
    console.error("[ADP Review] report error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

function decoratePayload(payload, profile, propertyId, versionLabel, pubRec, reviewerMeta, req) {
  const p = payload?.property || {};
  const name = p.name || profile?.name || reviewerMeta?.hotelName || propertyId;
  // Same attach-at-read contract as customer /property/:id/report — published
  // snapshots often omit baked brandPortfolioPosition. SPA review must not hide BPP.
  // Enrich trends / optional metrics from the hotel's own period corpus BEFORE
  // the canonical view model so empty trends[] cannot blank valid executiveMetrics.
  const { brandPortfolioPosition: embeddedBpp, ...rawCore } = payload || {};
  const corePayload = enrichPayloadOptionalMetrics(propertyId, rawCore) || rawCore;
  const brandPortfolioPosition = selectBrandPortfolioPositionForReport({
    propertyId,
    profile,
    req,
    embeddedBpp,
    reportVersion: versionLabel === "STAGED" ? "staged" : "published",
    preferLoyaltyStaged: versionLabel === "STAGED",
  });
  return attachCanonicalReportViewModelV1({
    ...corePayload,
    propertyId,
    property: {
      ...p,
      propertyId,
      name,
      city: p.city || profile?.city || reviewerMeta?.city || "",
      state: p.state || profile?.state || "",
      region: p.region || profile?.region || "",
      country: p.country || profile?.country || "",
      locationLine: formatPropertyLocationLine({
        city: p.city || profile?.city,
        state: p.state || profile?.state,
        region: p.region || profile?.region,
        country: p.country || profile?.country,
      }),
      label: formatPropertySelectorLabel({
        name,
        city: p.city || profile?.city,
        state: p.state || profile?.state,
        region: p.region || profile?.region,
        country: p.country || profile?.country,
      }),
    },
    brandPortfolioPosition,
    _bppAttachedAtRead: true,
    _bppPublicationVersion:
      brandPortfolioPosition?.publicationVersion ||
      brandPortfolioPosition?.measurement?.publicationVersion ||
      null,
    _adpReviewChrome: {
      hotelName: name,
      market: reviewerMeta?.market || profile?.market || p.city || "",
      measurementPeriod: pubRec?.measurementPeriod || reviewerMeta?.measurementPeriod || null,
      workflowVersion: pubRec?.workflowVersion || "ADP_FULL_E2E_V1",
      runId: pubRec?.runId || reviewerMeta?.latestRunId || null,
      reportVersion:
        versionLabel === "STAGED"
          ? pubRec?.stagedReportVersion || payload?._adpStagedVersionId
          : pubRec?.publishedReportVersion,
      certificationState: pubRec?.certificationResult || reviewerMeta?.certificationStatus || null,
      stagedOrPublished: versionLabel,
      lastGeneratedAt: pubRec?.history?.slice(-1)?.[0]?.at || null,
      publicationState: pubRec?.publicationState || reviewerMeta?.publicationState || null,
      integrityBlockers: buildBlockers(reviewerMeta, pubRec),
      nextAction: reviewerMeta?.nextAction || null,
      availableReviewActions: reviewerMeta?.availableReviewActions || [],
    },
  }, { propertyId });
}

function buildBlockers(reviewerMeta, pubRec) {
  const blockers = [];
  if (reviewerMeta?.competitiveCertification === "NOT_CERTIFIED") {
    blockers.push({
      code: "MARKET_REGISTRY_MISSING",
      message: "Competitive analysis NOT_CERTIFIED — build governed market registry",
    });
  }
  if (pubRec?.publicationState === "CERTIFICATION_FAILED") {
    blockers.push({ code: "CERTIFICATION_FAILED", message: "Certification gates failed" });
  }
  if (pubRec?.publicationState === "CHANGES_REQUESTED") {
    blockers.push({
      code: "CHANGES_REQUESTED",
      message: "Reviewer requested changes on staged version",
    });
  }
  if (!reviewerMeta?.hasStaged && !reviewerMeta?.stagedReportVersion) {
    blockers.push({ code: "NO_STAGED_REPORT", message: "No staged report available for review" });
  }
  return blockers;
}

export async function getAdpReviewerCompare(req, res) {
  try {
    setNoStore(res);
    const actor = requireMemberstackReviewer(req, res);
    if (!actor) return;

    const propertyId = String(req.params.propertyId || "").trim();
    const staged = loadStagedOwnerPayload(propertyId);
    const published = await getPublishedOwnerReport(propertyId);

    const comparison = compareStagedVsPublishedReports({
      propertyId,
      stagedPayload: staged?.payload || null,
      publishedPayload: published?.ok ? published.payload : null,
      stagedMeta: staged
        ? {
            versionId: staged.versionId,
            artifactHash: staged.artifactHash,
            periodId: staged.periodId,
          }
        : null,
      publishedMeta: published?.ok
        ? { source: published.source, periodId: published.payload?.periodId }
        : null,
    });

    return res.json({
      ...comparison,
      actor: { id: actor.id },
      publicationRecord: getHotelPublicationRecord(propertyId),
    });
  } catch (err) {
    console.error("[ADP Review] compare error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function postAdpReviewerApprove(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;

  const propertyId = String(req.params.propertyId || "").trim();
  const reportVersion =
    String(req.body?.reportVersion || "").trim() ||
    loadStagedManifest(propertyId)?.latestStagedVersionId;
  const artifactHash = req.body?.artifactHash || loadStagedManifest(propertyId)?.artifactHash;

  if (!reportVersion) {
    return res.status(400).json({ ok: false, error: "report_version_required" });
  }

  // Ensure registry knows staged version
  const staged = loadStagedManifest(propertyId);
  if (staged && !getHotelPublicationRecord(propertyId)?.stagedReportVersion) {
    transitionPublicationState({
      propertyId,
      toState: "READY_FOR_REVIEW",
      actor: actor.id,
      requireAuth: false,
      extra: {
        stagedReportVersion: staged.latestStagedVersionId,
        stagedArtifactHash: staged.artifactHash,
        hotelName: getAdpReviewerProperty(propertyId)?.hotelName,
      },
    });
  }

  const result = approveStagedReportVersion({
    propertyId,
    reportVersion,
    artifactHash,
    actor,
  });
  if (!result.ok) {
    return res.status(422).json(result);
  }
  return res.json({ ok: true, record: result.record, actor: { id: actor.id, email: actor.email } });
}

export function postAdpReviewerRequestChanges(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;

  const propertyId = String(req.params.propertyId || "").trim();
  const comment = String(req.body?.comment || "").trim();
  if (!comment) {
    return res.status(400).json({ ok: false, error: "comment_required" });
  }
  const reportVersion =
    String(req.body?.reportVersion || "").trim() ||
    loadStagedManifest(propertyId)?.latestStagedVersionId;

  const result = requestChangesOnStagedReport({
    propertyId,
    reportVersion,
    comment,
    actor,
  });
  if (!result.ok) {
    return res.status(422).json(result);
  }
  return res.json({ ok: true, record: result.record });
}

/**
 * Controlled publish — requires ADP_PUBLICATION_AUTHORIZED=1 AND version approval.
 * Does NOT copy staged→published unless explicitly authorized (separate ops path).
 * This endpoint records publication intent + verifies approval gate only when
 * authorization env is set; artifact promotion remains a separate governed step.
 */
export function postAdpReviewerPublish(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;

  const authorized =
    process.env.ADP_PUBLICATION_AUTHORIZED === "1" ||
    process.env.ADP_PUBLICATION_AUTHORIZED === "true";
  if (!authorized) {
    return res.status(403).json({
      ok: false,
      error: "publication_not_authorized",
      message:
        "Publication requires ADP_PUBLICATION_AUTHORIZED=1 and an approved immutable report version. Approval alone is insufficient.",
      publicationStates: ADP_PUBLICATION_STATES,
    });
  }

  const propertyId = String(req.params.propertyId || "").trim();
  const reportVersion = String(req.body?.reportVersion || "").trim();
  const artifactHash = String(req.body?.artifactHash || "").trim();
  const prior = getHotelPublicationRecord(propertyId);

  if (!prior || prior.publicationState !== "APPROVED_FOR_PUBLICATION") {
    return res.status(422).json({
      ok: false,
      error: "not_approved_for_publication",
      publicationState: prior?.publicationState || null,
    });
  }
  if (!reportVersion || reportVersion !== prior.approvedReportVersion) {
    return res.status(422).json({
      ok: false,
      error: "approved_version_mismatch",
      approvedReportVersion: prior.approvedReportVersion,
      requested: reportVersion || null,
    });
  }

  // Record WAITING then PUBLISHING — actual published/ file write is ops-gated
  transitionPublicationState({
    propertyId,
    toState: "WAITING_PUBLICATION_AUTHORIZATION",
    actor: actor.id,
    note: "publish_requested",
    requireAuth: false,
  });
  transitionPublicationState({
    propertyId,
    toState: "PUBLISHING",
    actor: actor.id,
    requireAuth: false,
  });

  const dryRun = req.body?.dryRun !== false && req.body?.apply !== true;
  if (dryRun) {
    upsertHotelPublicationRecord(propertyId, {
      publicationState: "APPROVED_FOR_PUBLICATION",
      _actor: actor.id,
      _note: "publish_dry_run_revert",
    });
    return res.json({
      ok: true,
      dryRun: true,
      message:
        "Publication authorization present; dry-run only. Pass apply:true to record PUBLISHED (does not overwrite published/ artifacts in this endpoint).",
      wouldPublish: {
        propertyId,
        reportVersion,
        artifactHash: artifactHash || prior.stagedArtifactHash,
      },
      record: getHotelPublicationRecord(propertyId),
    });
  }

  const result = recordPublishedVersion({
    propertyId,
    publishedReportVersion: reportVersion,
    publishedArtifactHash: artifactHash || prior.stagedArtifactHash,
    actor,
    verificationResult: {
      ok: true,
      note: "registry_pointer_only_no_artifact_overwrite",
      at: new Date().toISOString(),
    },
  });
  if (!result.ok) {
    return res.status(422).json(result);
  }
  return res.json({
    ok: true,
    dryRun: false,
    record: result.record,
    warning:
      "Registry marked PUBLISHED; published/ snapshot files were not overwritten by this endpoint.",
  });
}

export function getAdpPublicationRegistry(req, res) {
  setNoStore(res);
  const actor = requireMemberstackReviewer(req, res);
  if (!actor) return;
  const propertyId = String(req.query.propertyId || "").trim();
  if (propertyId) {
    return res.json({
      ok: true,
      record: getHotelPublicationRecord(propertyId),
    });
  }
  return res.json({
    ok: true,
    records: listAllPublicationRecords(),
  });
}
