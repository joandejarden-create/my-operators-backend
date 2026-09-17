/**
 * Admin APIs — AI Demand Monthly Review archive + regeneration control center.
 * All handlers require adminAuth on the route (server.js).
 */

import fs from "node:fs";
import {
  ARCHIVE_INDEX_SCHEMA,
  listArchiveReviews,
  loadReviewPayload,
  registerGoldenPairArchive,
  archiveReview,
  summaryCounts,
  getEligibleGeneratePreview,
  getBulkEligibleGeneratePreview,
  appendActionFounderFeedback,
  appendReportSectionFeedback,
  readFeedbackForReview,
  markClientIssued,
} from "../lib/ai-demand-positioning/monthly-review/admin/archive-store-v1.js";
import {
  ACTION_LIBRARY_VERSION,
  GENERATION_STATUS,
  REVIEW_BUILDER_VERSION,
  REVIEW_STATUS,
  labelGenerationStatus,
  labelReviewStatus,
} from "../lib/ai-demand-positioning/monthly-review/admin/report-status-v1.js";
import {
  generateNewDraft,
  regenerateReviewFromArchive,
  generateEligibleReviewsBulk,
} from "../lib/ai-demand-positioning/monthly-review/admin/generate-review-v1.js";
import { compareReviewVersions } from "../lib/ai-demand-positioning/monthly-review/admin/version-diff-v1.js";
import { GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS } from "../lib/ai-demand-positioning/monthly-review/monthly-review-contract-v1.js";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";
import { resolveAllAdpMonthlyReviewEligibilityV1 } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import {
  buildPublishedAdpReviewCoverageV1,
  ADP_AI_DEMAND_REVIEW_COVERAGE_V1,
} from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import {
  getPublishedOwnerReport,
} from "../lib/ai-demand-positioning/published-read-service.js";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  currentReportPdfPaths,
  customerPdfFilename,
  hasCurrentReportPdf,
  loadCurrentReportPdfMeta,
  ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT,
} from "../lib/ai-demand-positioning/current-report-pdf/current-report-pdf-store-v1.js";
import {
  buildAiDemandPerformanceReviewFilename,
  contentDispositionForAiDemandPerformanceReviewPdf,
} from "../lib/ai-demand-positioning/monthly-review/ai-demand-performance-review-filename-v1.js";
import { AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1 } from "../lib/ai-demand-positioning/monthly-review/ai-demand-performance-review-template-v1.js";

const ACTION_PLAN_EXPORT_GATE =
  "AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY";
const ACTION_PLAN_TRACE_GATE =
  "ADP_DETAILED_ACTION_PLAN_TRACES_TO_PUBLISHED_DIAGNOSIS";
const PDF_RESPONSE_CONTRACT =
  "AI_DEMAND_PERFORMANCE_REVIEW_PDF_RESPONSE_CONTRACT";

function csvEscape(value) {
  const s = String(value == null ? "" : value);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function slugForFilename(name) {
  return (
    String(name || "property")
      .replace(/[^a-zA-Z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .slice(0, 80) || "property"
  );
}

function slugActionId(title, idx) {
  const base = String(title || `action_${idx + 1}`)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 64);
  return `pub_action_${base || idx + 1}`;
}

function matchPublishedPriorityActions(detailed, publishedActions) {
  const pubs = Array.isArray(publishedActions) ? publishedActions : [];
  if (!pubs.length) return [];
  const hay = [
    detailed.actionTitle,
    detailed.observedIssue,
    detailed.recommendedAction,
    detailed.trace?.sourceGap,
    detailed.trace?.observedMetric,
    detailed.actionPatternId,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  const pattern = String(detailed.actionPatternId || "").toUpperCase();

  return pubs
    .map((pa, idx) => ({ pa, idx, id: slugActionId(pa.title, idx) }))
    .filter(({ pa }) => {
      const title = String(pa.title || "").toLowerCase();
      const cat = String(pa.category || "").toLowerCase().replace(/\s+/g, "_");
      if (!title && !cat) return false;

      // Pattern-aware primary links (avoid loose token collisions).
      if (pattern.includes("ALL_INCLUSIVE") || /all-?inclusive/.test(hay)) {
        return (
          cat === "reality_gap" ||
          /all-?inclusive|reality|attribute/.test(title)
        );
      }
      if (
        pattern.includes("COMPETITIVE") ||
        pattern.includes("DISPLACEMENT") ||
        /displac|competitive absence|public facts/.test(hay)
      ) {
        return (
          cat === "displacement" ||
          /displac|competitor|st\.?\s*regis|rival/.test(title)
        );
      }
      if (pattern.includes("TRIPADVISOR") || /tripadvisor/.test(hay)) {
        // Live Priority Actions may not include TripAdvisor; do not force-link.
        return /tripadvisor|review response|management response/.test(title);
      }

      // Fallback: strong title token overlap only when tokens are distinctive.
      const tokens = title.split(/\s+/).filter((t) => t.length > 5);
      const hits = tokens.filter((t) => hay.includes(t));
      return hits.length >= 2;
    });
}

/**
 * Admin Action Plan = detailed Management Action Agenda from Performance Review.
 * Live report Priority Actions remain the customer summary; this is the operational expansion.
 */
function buildCanonicalActionPlanPack(propertyId, opts = {}) {
  const latest = opts.latestReviewMeta || null;
  const reviewId = latest?.reviewId || opts.reviewId || null;
  if (!reviewId) {
    return {
      ok: false,
      gate: ACTION_PLAN_EXPORT_GATE,
      propertyId,
      propertyName: opts.propertyName || propertyId,
      periodId: opts.periodId || null,
      actionCount: 0,
      actions: [],
      error: "performance_review_missing",
    };
  }
  const pack = loadReviewPayload(reviewId);
  if (!pack?.review) {
    return {
      ok: false,
      gate: ACTION_PLAN_EXPORT_GATE,
      propertyId,
      propertyName: opts.propertyName || propertyId,
      periodId: opts.periodId || null,
      actionCount: 0,
      actions: [],
      error: "review_payload_missing",
    };
  }
  const published = opts.publishedReport || loadPublishedReport(propertyId);
  const publishedActions = Array.isArray(published?.actions) ? published.actions : [];
  const agenda = Array.isArray(pack.review.actionAgenda) ? pack.review.actionAgenda : [];
  const propertyName =
    pack.review.property?.name ||
    published?.property?.name ||
    opts.propertyName ||
    propertyId;
  const periodId =
    pack.meta?.sourceCurrentPeriodId ||
    pack.review.reporting?.monitoringPeriodId ||
    published?.period?.periodId ||
    opts.periodId ||
    null;

  const actions = agenda.map((a, idx) => {
    const linked = matchPublishedPriorityActions(a, publishedActions);
    const sourceDiagnosisIds = [
      a.trace?.observedMetric,
      a.trace?.sourceGap,
      a.actionPatternId,
    ].filter(Boolean);
    const sourceEvidenceRefs = a.evidenceRefs || (a.evidence ? [a.evidence] : []);
    const evidenceSupported = Boolean(
      linked.length ||
        sourceDiagnosisIds.length ||
        sourceEvidenceRefs.length ||
        a.actionPatternId
    );
    return {
      order: idx + 1,
      priority: a.priority || linked[0]?.pa?.priority || null,
      status: a.status || "OPEN",
      title: a.actionTitle || a.title || null,
      owner: a.accountableOwnerRole || a.owner || null,
      dueDate: a.targetDate || null,
      observedIssue: a.observedIssue || null,
      recommendedAction:
        a.recommendedAction ||
        (Array.isArray(a.implementationSteps)
          ? a.implementationSteps.join(" ")
          : null),
      implementationSteps: a.implementationSteps || [],
      targetSources: a.targetSources || [],
      definitionOfDone: a.definitionOfDone || null,
      expectedSignal: a.expectedSignal || null,
      nextMonitoringCheck: a.nextMonitoringCheck || null,
      actionPatternId: a.actionPatternId || null,
      actionId: a.actionId || null,
      sourcePriorityActionIds: linked.map((l) => l.id),
      sourcePriorityActions: linked.map((l) => ({
        id: l.id,
        title: l.pa.title,
        category: l.pa.category,
        priority: l.pa.priority,
      })),
      sourceDiagnosisIds,
      sourceEvidenceRefs,
      linkedToLivePriorityAction: linked.length > 0,
      evidenceSupported,
      unsupportedByCurrentEvidence: !evidenceSupported,
      relationship: "LIVE_PRIORITY_ACTION→DETAILED_MANAGEMENT_ACTION_PLAYBOOK",
      trace: a.trace || null,
    };
  });

  const unsupported = actions.filter((a) => a.unsupportedByCurrentEvidence);
  return {
    ok: true,
    gate: ACTION_PLAN_EXPORT_GATE,
    traceGate: ACTION_PLAN_TRACE_GATE,
    product: "AI Demand Performance Review — Management Action Agenda",
    propertyId,
    propertyName,
    periodId,
    reviewId,
    reviewGeneratedAt: pack.meta?.generatedAt || null,
    reportingMonth: pack.meta?.reportingMonth || null,
    executionDate:
      published?.period?.executionDate ||
      pack.meta?.currentMonitoringDate ||
      null,
    publicationState: "performance_review_from_published_adp",
    livePriorityActionCount: publishedActions.length,
    actionCount: actions.length,
    actions,
    unsupportedActionCount: unsupported.length,
    unsupportedActions: unsupported.map((a) => a.title),
  };
}

function adminIdentity(req) {
  return (
    req.dealalityUser?.email ||
    req.dealalityUser?.memberstackId ||
    req.dealalityUser?.id ||
    "admin"
  );
}

function parseBool(v) {
  if (v === true || v === "true" || v === "1") return true;
  if (v === false || v === "false" || v === "0") return false;
  return undefined;
}

export function getAdminMonthlyReviews(req, res) {
  try {
    // Ensure golden pair is indexed for admin (idempotent)
    registerGoldenPairArchive({ force: false });

    const filters = {
      propertyId: req.query.propertyId || undefined,
      reportingMonth: req.query.reportingMonth || undefined,
      reviewStatus: req.query.reviewStatus || undefined,
      generationStatus: req.query.generationStatus || undefined,
      clientIssued: parseBool(req.query.clientIssued),
      hasPdf: parseBool(req.query.hasPdf),
      actionPatternId: req.query.actionPatternId || undefined,
      q: req.query.q || req.query.search || undefined,
      includeArchived: parseBool(req.query.includeArchived) === true,
    };

    const rows = listArchiveReviews(filters);
    const allActive = listArchiveReviews({ includeArchived: false });
    const coverage = buildPublishedAdpReviewCoverageV1({
      includeArchived: filters.includeArchived,
    });

    // Coverage-first catalog: one row per published ADP property (never silent omit).
    // Archive version history remains available as `reviews`.
    let properties = coverage.properties;
    if (filters.propertyId) {
      properties = properties.filter((p) => p.propertyId === filters.propertyId);
    }
    if (filters.q) {
      const q = String(filters.q).toLowerCase();
      properties = properties.filter(
        (p) =>
          String(p.propertyName || "").toLowerCase().includes(q) ||
          String(p.propertyId || "").toLowerCase().includes(q) ||
          String(p.market || "").toLowerCase().includes(q)
      );
    }

    return res.json({
      ok: true,
      schema: ARCHIVE_INDEX_SCHEMA,
      coverageGate: ADP_AI_DEMAND_REVIEW_COVERAGE_V1,
      statusModel: "ADP_MONTHLY_REVIEW_REPORT_STATUS_V1",
      builderVersion: REVIEW_BUILDER_VERSION,
      actionLibraryVersion: ACTION_LIBRARY_VERSION,
      goldenPairPropertyIds: [...GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS],
      governedPropertyIds: [...ADP_CERTIFIED_PROPERTY_IDS],
      publishedPropertyIds: coverage.publishedPropertyIds,
      eligibility: resolveAllAdpMonthlyReviewEligibilityV1().counts,
      coverage: {
        publishedCount: coverage.publishedCount,
        reviewReadyCount: coverage.reviewReadyCount,
        missingCount: coverage.missingCount,
        blockedCount: coverage.blockedCount,
        silentlyOmitted: coverage.silentlyOmitted,
      },
      counts: {
        ...summaryCounts(allActive),
        published: coverage.publishedCount,
        reviewReady: coverage.reviewReadyCount,
        missing: coverage.missingCount,
        blocked: coverage.blockedCount,
      },
      properties,
      reviews: rows.map(enrichRow),
      liveProviderCalls: 0,
    });
  } catch (err) {
    console.error("[ADP Monthly Review Admin] list error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

function enrichRow(r) {
  return {
    ...r,
    reviewStatusLabel: labelReviewStatus(r.reviewStatus),
    generationStatusLabel: labelGenerationStatus(r.generationStatus),
    clientIssued: Boolean(r.clientIssuedAt) || r.reviewStatus === REVIEW_STATUS.CLIENT_ISSUED,
    actions: availableActions(r),
  };
}

function availableActions(r) {
  const actions = [];
  const failed = r.generationStatus === GENERATION_STATUS.FAILED;
  if (!failed) {
    actions.push("Open Review", "Meeting Mode", "View Payload", "View Actions");
    if (r.pdfStatus === "READY") actions.push("Open PDF");
    actions.push("Compare Versions", "Regenerate", "Generate New Draft");
  } else {
    actions.push("View Payload", "Regenerate", "Generate New Draft");
  }
  if (r.reviewStatus !== REVIEW_STATUS.ARCHIVED) actions.push("Archive");
  return actions;
}

export function getAdminMonthlyReviewById(req, res) {
  try {
    const reviewId = req.params.reviewId;
    const pack = loadReviewPayload(reviewId);
    if (!pack) {
      return res.status(404).json({ ok: false, error: "review_not_found" });
    }
    const feedback = readFeedbackForReview(reviewId);
    return res.json({
      ok: true,
      meta: enrichRow(pack.meta),
      review: pack.review,
      generationLog: pack.generationLog,
      fingerprints: pack.fingerprints,
      feedback,
      customerOutputParity: {
        gate: "ADP_MONTHLY_REVIEW_ADMIN_CUSTOMER_OUTPUT_PARITY",
        liveReviewUrl: `/owner-adp-monthly-review.html?propertyId=${encodeURIComponent(
          pack.meta.propertyId
        )}&source=archive&reviewId=${encodeURIComponent(reviewId)}`,
        meetingModeUrl: `/owner-adp-monthly-review.html?propertyId=${encodeURIComponent(
          pack.meta.propertyId
        )}&source=archive&reviewId=${encodeURIComponent(reviewId)}&mode=meeting`,
        note: "Admin opens the same customer HTML composition.",
      },
      liveProviderCalls: 0,
    });
  } catch (err) {
    console.error("[ADP Monthly Review Admin] get error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function getAdminMonthlyReviewPdf(req, res) {
  try {
    const pack = loadReviewPayload(req.params.reviewId);
    if (!pack) return res.status(404).json({ ok: false, error: "review_not_found" });
    if (!fs.existsSync(pack.paths.reportPdf)) {
      return res.status(404).json({ ok: false, error: "pdf_missing" });
    }
    const download = String(req.query.download || "") === "1";
    const filename = buildAiDemandPerformanceReviewFilename({
      hotelName: pack.meta.propertyName,
      date:
        pack.meta.currentMonitoringDate ||
        pack.meta.generatedAt ||
        new Date().toISOString(),
      reportingMonthKey: pack.meta.reportingMonthKey,
    });
    if (/\.tmp$/i.test(filename) || !/\.pdf$/i.test(filename)) {
      return res.status(500).json({
        ok: false,
        error: "invalid_pdf_filename",
        gate: "AI_DEMAND_PDF_TEMP_FILE_NAME_NEVER_USER_VISIBLE",
      });
    }
    const stat = fs.statSync(pack.paths.reportPdf);
    const fd = fs.openSync(pack.paths.reportPdf, "r");
    const magic = Buffer.alloc(5);
    fs.readSync(fd, magic, 0, 5, 0);
    fs.closeSync(fd);
    if (magic.toString("utf8") !== "%PDF-") {
      return res.status(500).json({
        ok: false,
        error: "pdf_magic_invalid",
        gate: PDF_RESPONSE_CONTRACT,
      });
    }
    res.status(200);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      contentDispositionForAiDemandPerformanceReviewPdf(filename, {
        disposition: download ? "attachment" : "inline",
      })
    );
    res.setHeader("Content-Length", String(stat.size));
    res.setHeader("X-ADP-PDF-Gate", PDF_RESPONSE_CONTRACT);
    res.setHeader(
      "X-ADP-PDF-Template",
      AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1
    );
    res.setHeader("X-ADP-PDF-Filename", filename);
    // Filesystem temp/archive path must never leak into Content-Disposition.
    return fs.createReadStream(pack.paths.reportPdf).pipe(res);
  } catch (err) {
    console.error("[ADP Monthly Review Admin] pdf error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

/**
 * Current published ADP report PDF (admin default).
 * Distinct from historical Monthly Review archive PDFs.
 */
export function getAdminCurrentReportPdf(req, res) {
  try {
    const propertyId = String(req.params.propertyId || "").trim();
    if (!propertyId) {
      return res.status(400).json({ ok: false, error: "propertyId_required" });
    }
    const paths = currentReportPdfPaths(propertyId);
    if (!hasCurrentReportPdf(propertyId) || !fs.existsSync(paths.reportPdf)) {
      return res.status(404).json({
        ok: false,
        error: "current_adp_pdf_missing",
        gate: ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT,
      });
    }
    const meta = loadCurrentReportPdfMeta(propertyId);
    const filename = customerPdfFilename(propertyId);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="${filename}"`);
    res.setHeader(
      "X-ADP-PDF-Gate",
      ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT
    );
    if (meta?.periodId) res.setHeader("X-ADP-Period-Id", String(meta.periodId));
    return fs.createReadStream(paths.reportPdf).pipe(res);
  } catch (err) {
    console.error("[ADP Admin] current report pdf error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function postAdminMonthlyReviewRegenerate(req, res) {
  try {
    const result = regenerateReviewFromArchive(req.params.reviewId, {
      generatedBy: adminIdentity(req),
    });
    if (!result.ok) {
      return res.status(422).json({
        ok: false,
        error: "generation_failed",
        generationStatus: result.generationStatus,
        failure: result.error,
        liveProviderCalls: 0,
      });
    }
    return res.status(201).json({
      ok: true,
      ...result,
      gate: "ADP_MONTHLY_REVIEW_REGENERATION_CREATES_NEW_VERSION",
      immutability: "ADP_MONTHLY_REVIEW_REGENERATION_IMMUTABILITY",
    });
  } catch (err) {
    console.error("[ADP Monthly Review Admin] regenerate error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function postAdminMonthlyReviewGenerate(req, res) {
  try {
    const propertyId = req.body?.propertyId;
    if (!propertyId) {
      return res.status(400).json({ ok: false, error: "propertyId_required" });
    }
    // Explicit confirmation required
    if (req.body?.confirm !== true) {
      const preview = getEligibleGeneratePreview(propertyId);
      return res.status(200).json({
        ok: true,
        requiresConfirm: true,
        modal: {
          title: "Generate AI Demand Performance Review",
          property: preview.propertyName,
          currentMonitoring: `${preview.currentMonitoringDate || "—"} / ${preview.currentEligibleMonitoringPeriod || "—"}`,
          priorRun: `${preview.priorRunDate || "—"} / ${preview.resolvedPriorComparablePeriod || "—"}`,
          reviewBuilder: preview.reviewBuilderVersion,
          actionLibrary: preview.actionLibraryVersion,
          outputs: ["Live Review", "Meeting Mode", "PDF"],
          liveProviderCalls: 0,
        },
        preview,
      });
    }

    const result = generateNewDraft(propertyId, { generatedBy: adminIdentity(req) });
    if (!result.ok) {
      return res.status(422).json({
        ok: false,
        error: "generation_failed",
        generationStatus: result.generationStatus,
        failure: result.error,
        liveProviderCalls: 0,
      });
    }
    return res.status(201).json({ ok: true, ...result });
  } catch (err) {
    console.error("[ADP Monthly Review Admin] generate error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

/**
 * Bulk generate for all eligible hotels.
 * Requires body.confirm === true after preview. LIVE_PROVIDER_CALLS = 0.
 */
export function postAdminMonthlyReviewGenerateEligible(req, res) {
  try {
    if (req.body?.confirm !== true) {
      return res.status(200).json({
        ok: true,
        requiresConfirm: true,
        preview: getBulkEligibleGeneratePreview(),
        liveProviderCalls: 0,
        gate: "ADP_MONTHLY_REVIEW_BULK_GENERATION_ZERO_PROVIDER_CALLS",
      });
    }
    const result = generateEligibleReviewsBulk({
      confirm: true,
      generatedBy: adminIdentity(req),
    });
    const status = result.ok ? 201 : 422;
    return res.status(status).json(result);
  } catch (err) {
    console.error("[ADP Monthly Review Admin] bulk generate error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function postAdminMonthlyReviewArchive(req, res) {
  try {
    const result = archiveReview(req.params.reviewId, {
      archivedBy: adminIdentity(req),
    });
    return res.json({
      ok: true,
      gate: "ADP_MONTHLY_REVIEW_ARCHIVE_NONDESTRUCTIVE",
      ...result,
    });
  } catch (err) {
    if (err.message === "review_not_found") {
      return res.status(404).json({ ok: false, error: "review_not_found" });
    }
    console.error("[ADP Monthly Review Admin] archive error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function getAdminMonthlyReviewCompare(req, res) {
  try {
    const other = req.query.other;
    if (!other) {
      return res.status(400).json({ ok: false, error: "other_reviewId_required" });
    }
    const diff = compareReviewVersions(req.params.reviewId, other);
    return res.json({
      ok: true,
      gate: "ADP_MONTHLY_REVIEW_VERSION_DIFF_INTEGRITY",
      diff,
    });
  } catch (err) {
    console.error("[ADP Monthly Review Admin] compare error:", err);
    return res.status(400).json({ ok: false, error: err.message });
  }
}

export function postAdminActionFeedback(req, res) {
  try {
    const row = appendActionFounderFeedback({
      ...req.body,
      reviewId: req.params.reviewId,
      reviewedBy: adminIdentity(req),
    });
    return res.status(201).json({ ok: true, feedback: row });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err.message });
  }
}

export function postAdminReportFeedback(req, res) {
  try {
    const row = appendReportSectionFeedback({
      ...req.body,
      reviewId: req.params.reviewId,
      reviewedBy: adminIdentity(req),
    });
    return res.status(201).json({ ok: true, feedback: row });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err.message });
  }
}

export function postAdminMarkClientIssued(req, res) {
  try {
    const result = markClientIssued(req.params.reviewId, {
      issuedBy: adminIdentity(req),
    });
    return res.json({
      ok: true,
      gate: "ADP_MONTHLY_REVIEW_CLIENT_ISSUED_IMMUTABILITY",
      ...result,
    });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err.message });
  }
}

export function getAdminGeneratePreview(req, res) {
  try {
    const propertyId = req.query.propertyId;
    if (!propertyId) {
      return res.status(400).json({ ok: false, error: "propertyId_required" });
    }
    return res.json({ ok: true, preview: getEligibleGeneratePreview(propertyId) });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err.message });
  }
}

/**
 * Admin catalog — published ADP hotels for Action Plan (Management Action Agenda).
 */
export async function getAdminActionPlanCatalog(req, res) {
  try {
    const coverage = buildPublishedAdpReviewCoverageV1();
    const properties = coverage.properties.map((p) => ({
      propertyId: p.propertyId,
      propertyName: p.propertyName,
      periodId: p.currentPeriodId,
      executionDate: p.currentMonitoringDate || null,
      publicationState: p.coverageStatus,
      reviewId: p.reviewId,
      actionCount: p.actionCount || 0,
      livePriorityActionCount: p.livePriorityActionCount || 0,
      ok: Boolean(p.reviewId),
      error: p.reviewId ? null : "performance_review_missing",
    }));
    return res.json({
      ok: true,
      gate: ACTION_PLAN_EXPORT_GATE,
      product: "AI Demand Performance Review — Management Action Agenda",
      properties,
      liveProviderCalls: 0,
    });
  } catch (err) {
    console.error("[ADP Action Plan Admin] catalog error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

/**
 * Admin Action Plan export — detailed Management Action Agenda CSV / JSON.
 */
export async function getAdminActionPlanExport(req, res) {
  try {
    const propertyId = String(req.query.propertyId || "").trim();
    if (!propertyId) {
      return res.status(400).json({ ok: false, error: "propertyId_required" });
    }
    if (!loadPropertyProfile(propertyId)) {
      return res.status(404).json({ ok: false, error: "property_not_found" });
    }

    const coverage = buildPublishedAdpReviewCoverageV1();
    const row = coverage.properties.find((p) => p.propertyId === propertyId);
    const published = loadPublishedReport(propertyId);
    const pack = buildCanonicalActionPlanPack(propertyId, {
      latestReviewMeta: row,
      reviewId: row?.reviewId,
      propertyName: row?.propertyName,
      periodId: row?.currentPeriodId,
      publishedReport: published,
    });

    if (!pack.ok) {
      return res.status(404).json({
        ok: false,
        error: pack.error || "action_plan_unavailable",
        gate: ACTION_PLAN_EXPORT_GATE,
        propertyId,
      });
    }

    const format = String(req.query.format || "csv").toLowerCase();
    const preview = req.query.preview === "1" || format === "json";

    if (preview) {
      return res.json({
        ok: true,
        ...pack,
        filenameHint:
          "ADP_Management_Action_Agenda_" +
          slugForFilename(pack.propertyName) +
          "_" +
          String(pack.periodId || "current").slice(0, 48) +
          ".csv",
        liveProviderCalls: 0,
      });
    }

    const headers = [
      "Property",
      "Period",
      "Action #",
      "Priority",
      "Status",
      "Title",
      "Owner",
      "Due Date",
      "Observed Issue",
      "Recommended Action / Steps",
      "Target Sources",
      "Definition of Done",
      "Expected Signal",
      "Next Monitoring Check",
      "Source Priority Action IDs",
      "Source Diagnosis IDs",
    ];
    const lines = [headers.map(csvEscape).join(",")];
    for (const a of pack.actions) {
      lines.push(
        [
          pack.propertyName,
          pack.periodId || "",
          a.order || "",
          a.priority || "",
          a.status || "",
          a.title || "",
          a.owner || "",
          a.dueDate || "",
          a.observedIssue || "",
          a.recommendedAction ||
            (Array.isArray(a.implementationSteps)
              ? a.implementationSteps.join(" | ")
              : ""),
          Array.isArray(a.targetSources) ? a.targetSources.join(" | ") : "",
          Array.isArray(a.definitionOfDone)
            ? a.definitionOfDone.join(" | ")
            : a.definitionOfDone || "",
          a.expectedSignal || "",
          a.nextMonitoringCheck || "",
          (a.sourcePriorityActionIds || []).join(" | "),
          (a.sourceDiagnosisIds || []).join(" | "),
        ]
          .map(csvEscape)
          .join(",")
      );
    }
    const body = lines.join("\r\n") + "\r\n";
    const filename =
      "ADP_Management_Action_Agenda_" +
      slugForFilename(pack.propertyName) +
      "_" +
      String(pack.periodId || "current").slice(0, 40) +
      ".csv";

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", 'attachment; filename="' + filename + '"');
    res.setHeader("X-ADP-Action-Plan-Gate", ACTION_PLAN_EXPORT_GATE);
    res.setHeader("X-ADP-Action-Count", String(pack.actionCount));
    return res.status(200).send(body);
  } catch (err) {
    console.error("[ADP Action Plan Admin] export error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

/** Internal: serve archive review JSON for customer HTML parity (admin-gated when via admin path). */
export function resolveArchiveReviewForCustomerRender(reviewId) {
  const pack = loadReviewPayload(reviewId);
  if (!pack) return null;
  return pack.review;
}
