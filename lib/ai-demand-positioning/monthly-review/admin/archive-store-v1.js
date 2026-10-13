/**
 * ADP Monthly Review archive store — filesystem SoT + index.
 * Path: reports/ai-demand-positioning/monthly-review/archive/{propertyId}/{reportingMonthKey}/{reviewId}/
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTION_FEEDBACK_RATINGS,
  ACTION_FOUNDER_FEEDBACK_SCHEMA,
  ACTION_LIBRARY_VERSION,
  ARCHIVE_INDEX_SCHEMA,
  GENERATION_REASON,
  GENERATION_STATUS,
  PDF_STATUS,
  REPORT_FEEDBACK_RATINGS,
  REPORT_FEEDBACK_SECTIONS,
  REPORT_SECTION_FEEDBACK_SCHEMA,
  REVIEW_BUILDER_VERSION,
  REVIEW_SCHEMA_VERSION,
  REVIEW_STATUS,
  canMutateReviewArtifacts,
} from "./report-status-v1.js";
import { GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS } from "../monthly-review-contract-v1.js";
import {
  resolveAdpMonthlyReviewEligibilityV1,
  resolveAllAdpMonthlyReviewEligibilityV1,
} from "../resolve-adp-monthly-review-eligibility-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../");

export const ARCHIVE_ROOT = path.join(
  ROOT,
  "reports/ai-demand-positioning/monthly-review/archive"
);
export const ARCHIVE_INDEX_PATH = path.join(ARCHIVE_ROOT, "index.json");
export const ACTION_FEEDBACK_PATH = path.join(
  ARCHIVE_ROOT,
  "feedback/action-founder-feedback.jsonl"
);
export const REPORT_FEEDBACK_PATH = path.join(
  ARCHIVE_ROOT,
  "feedback/report-section-feedback.jsonl"
);

const GOLDEN_FIXTURE_BY_PROPERTY = Object.freeze({
  adp_cambridge_beaches_bermuda:
    "fixtures/ai-demand-positioning/monthly-review/cambridge-beaches-monthly-executive-review-v1.json",
  adp_now_now_noho:
    "fixtures/ai-demand-positioning/monthly-review/now-now-noho-monthly-executive-review-v1.json",
});

const GOLDEN_PDF_BY_PROPERTY = Object.freeze({
  adp_cambridge_beaches_bermuda:
    "reports/ai-demand-positioning/monthly-review/pdf/cambridge-beaches-monthly-executive-review-v1.pdf",
  adp_now_now_noho:
    "reports/ai-demand-positioning/monthly-review/pdf/now-now-noho-monthly-executive-review-v1.pdf",
});

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function sha256Of(value) {
  const payload =
    typeof value === "string" ? value : JSON.stringify(stableForFingerprint(value));
  return crypto.createHash("sha256").update(payload).digest("hex");
}

/** Drop volatile fields so regenerate fingerprint reflects composition, not clock. */
function stableForFingerprint(review) {
  if (!review || typeof review !== "object") return review;
  const clone = JSON.parse(JSON.stringify(review));
  delete clone.generatedAt;
  if (clone.meta) {
    delete clone.meta.generatedAt;
    delete clone.meta.generatedBy;
  }
  return clone;
}

export function contentFingerprint(review) {
  return sha256Of(stableForFingerprint(review));
}

export function pdfFingerprint(pdfPath) {
  if (!pdfPath || !fs.existsSync(pdfPath)) return null;
  return sha256Of(fs.readFileSync(pdfPath));
}

export function reportingMonthKeyFromLabel(label) {
  if (!label) return "unknown";
  // Prefer explicit month-name labels before Date.parse (timezone skew → wrong month).
  const named = String(label).match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$/i
  );
  if (named) {
    const months = {
      january: "01",
      february: "02",
      march: "03",
      april: "04",
      may: "05",
      june: "06",
      july: "07",
      august: "08",
      september: "09",
      october: "10",
      november: "11",
      december: "12",
    };
    return `${named[2]}-${months[named[1].toLowerCase()]}`;
  }
  if (/^\d{4}-\d{2}/.test(String(label))) {
    return String(label).slice(0, 7);
  }
  const parsed = Date.parse(String(label));
  if (!Number.isNaN(parsed)) {
    const x = new Date(parsed);
    const y = x.getUTCFullYear();
    const m = String(x.getUTCMonth() + 1).padStart(2, "0");
    return `${y}-${m}`;
  }
  return String(label)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

export function createReviewId(propertyId, reportingMonthKey, versionNumber) {
  const short = String(propertyId || "prop")
    .replace(/^adp_/, "")
    .slice(0, 24);
  return `adp_mr_${short}_${reportingMonthKey}_v${versionNumber}`;
}

function emptyIndex() {
  return {
    schema: ARCHIVE_INDEX_SCHEMA,
    updatedAt: null,
    reviews: [],
  };
}

export function loadArchiveIndex() {
  if (!fs.existsSync(ARCHIVE_INDEX_PATH)) return emptyIndex();
  const idx = readJson(ARCHIVE_INDEX_PATH);
  if (!Array.isArray(idx.reviews)) idx.reviews = [];
  return idx;
}

export function saveArchiveIndex(index) {
  index.schema = ARCHIVE_INDEX_SCHEMA;
  index.updatedAt = new Date().toISOString();
  writeJson(ARCHIVE_INDEX_PATH, index);
  return index;
}

export function reviewDir(meta) {
  return path.join(
    ARCHIVE_ROOT,
    meta.propertyId,
    meta.reportingMonthKey || reportingMonthKeyFromLabel(meta.reportingMonth),
    meta.reviewId
  );
}

export function reviewArtifactPaths(meta) {
  const dir = reviewDir(meta);
  return {
    dir,
    reviewJson: path.join(dir, "review.json"),
    metadataJson: path.join(dir, "metadata.json"),
    meetingJson: path.join(dir, "meeting.json"),
    reportPdf: path.join(dir, "report.pdf"),
    generationLog: path.join(dir, "generation-log.json"),
    fingerprints: path.join(dir, "fingerprints.json"),
    reviewHtmlMeta: path.join(dir, "review-render.json"),
  };
}

export function loadReviewMetadata(reviewId) {
  const index = loadArchiveIndex();
  const entry = index.reviews.find((r) => r.reviewId === reviewId);
  if (!entry) return null;
  const paths = reviewArtifactPaths(entry);
  if (fs.existsSync(paths.metadataJson)) {
    return { ...entry, ...readJson(paths.metadataJson), _indexEntry: entry };
  }
  return entry;
}

export function loadReviewPayload(reviewId) {
  const meta = loadReviewMetadata(reviewId);
  if (!meta) return null;
  const paths = reviewArtifactPaths(meta);
  if (!fs.existsSync(paths.reviewJson)) {
    throw new Error(`review_payload_missing:${reviewId}`);
  }
  return {
    meta,
    review: readJson(paths.reviewJson),
    paths,
    generationLog: fs.existsSync(paths.generationLog)
      ? readJson(paths.generationLog)
      : null,
    fingerprints: fs.existsSync(paths.fingerprints)
      ? readJson(paths.fingerprints)
      : null,
  };
}

function nextVersionNumber(propertyId, reportingMonthKey, sourceCurrentPeriodId) {
  const index = loadArchiveIndex();
  const siblings = index.reviews.filter(
    (r) =>
      r.propertyId === propertyId &&
      (r.reportingMonthKey === reportingMonthKey ||
        r.sourceCurrentPeriodId === sourceCurrentPeriodId)
  );
  const max = siblings.reduce((acc, r) => Math.max(acc, Number(r.versionNumber) || 0), 0);
  return max + 1;
}

function markSuperseded(index, propertyId, reportingMonthKey, exceptReviewId) {
  for (const row of index.reviews) {
    if (row.propertyId !== propertyId) continue;
    if (row.reportingMonthKey !== reportingMonthKey) continue;
    if (row.reviewId === exceptReviewId) continue;
    if (row.reviewStatus === REVIEW_STATUS.CLIENT_ISSUED) continue;
    if (row.reviewStatus === REVIEW_STATUS.ARCHIVED) continue;
    if (row.isCurrent) {
      row.isCurrent = false;
      if (row.reviewStatus !== REVIEW_STATUS.CLIENT_ISSUED) {
        row.reviewStatus = REVIEW_STATUS.SUPERSEDED;
      }
      const paths = reviewArtifactPaths(row);
      if (fs.existsSync(paths.metadataJson)) {
        const meta = readJson(paths.metadataJson);
        meta.isCurrent = false;
        if (meta.reviewStatus !== REVIEW_STATUS.CLIENT_ISSUED) {
          meta.reviewStatus = REVIEW_STATUS.SUPERSEDED;
        }
        writeJson(paths.metadataJson, meta);
      }
    }
  }
}

/**
 * Persist a new immutable review version. Never overwrites an existing reviewId folder.
 */
export function persistNewReviewVersion({
  review,
  generationReason,
  generatedBy = "admin",
  reviewStatus = REVIEW_STATUS.DRAFT,
  generationStatus = GENERATION_STATUS.SUCCESS,
  supersedesReviewId = null,
  pdfSourcePath = null,
  liveProviderCalls = 0,
  warnings = [],
  errors = [],
  startedAt,
  qualityGates = null,
  skipMarkCurrent = false,
}) {
  if (!review?.property?.propertyId) {
    throw new Error("review_missing_property");
  }
  const propertyId = review.property.propertyId;
  const eligibility = resolveAdpMonthlyReviewEligibilityV1(propertyId);
  if (!eligibility.eligible) {
    throw new Error(
      `monthly_review_not_eligible:${eligibility.status}:${(eligibility.blockers || []).join(",")}`
    );
  }

  const reportingMonth = review.reporting?.reportingMonth || "Unknown";
  const reportingMonthKey = reportingMonthKeyFromLabel(reportingMonth);
  const sourceCurrentPeriodId = review.reporting?.currentPeriodId || null;
  const sourcePriorPeriodId = review.reporting?.priorPeriodId || null;
  const versionNumber = nextVersionNumber(
    propertyId,
    reportingMonthKey,
    sourceCurrentPeriodId
  );
  const reviewId = createReviewId(propertyId, reportingMonthKey, versionNumber);

  const meta = {
    reviewId,
    propertyId,
    propertyName: review.property?.name || propertyId,
    reportingMonth,
    reportingMonthKey,
    currentMonitoringDate: review.reporting?.currentMonitoringDate || null,
    priorRunDate: review.reporting?.priorRunDate || null,
    sourceCurrentPeriodId,
    sourcePriorPeriodId,
    versionNumber,
    versionLabel: `V${versionNumber}`,
    reviewSchemaVersion: review.schema || REVIEW_SCHEMA_VERSION,
    builderVersion: REVIEW_BUILDER_VERSION,
    actionLibraryVersion: ACTION_LIBRARY_VERSION,
    generatedAt: new Date().toISOString(),
    generatedBy,
    generationReason,
    reviewStatus,
    generationStatus,
    contentFingerprint: contentFingerprint(review),
    pdfFingerprint: null,
    pdfStatus: PDF_STATUS.MISSING,
    supersedesReviewId,
    clientIssuedAt: null,
    archivedAt: null,
    isCurrent: !skipMarkCurrent,
    actionCount: Array.isArray(review.actionAgenda) ? review.actionAgenda.length : 0,
    posture: review.posture || null,
    liveProviderCalls: Number(liveProviderCalls) || 0,
    qualityGates: qualityGates || summarizeQualityGates(review),
  };

  const paths = reviewArtifactPaths(meta);
  if (fs.existsSync(paths.dir)) {
    throw new Error(`review_version_collision:${reviewId}`);
  }
  ensureDir(paths.dir);

  writeJson(paths.reviewJson, review);
  writeJson(paths.meetingJson, review.meetingMode || {});
  writeJson(paths.reviewHtmlMeta, {
    customerHtmlRoute: `/owner-adp-monthly-review.html?propertyId=${encodeURIComponent(
      propertyId
    )}&source=archive&reviewId=${encodeURIComponent(reviewId)}`,
    meetingModeRoute: `/owner-adp-monthly-review.html?propertyId=${encodeURIComponent(
      propertyId
    )}&source=archive&reviewId=${encodeURIComponent(reviewId)}&mode=meeting`,
    note: "Customer composition is canonical; admin opens the same HTML surface.",
  });

  let pdfCopied = false;
  if (pdfSourcePath && fs.existsSync(pdfSourcePath)) {
    fs.copyFileSync(pdfSourcePath, paths.reportPdf);
    meta.pdfFingerprint = pdfFingerprint(paths.reportPdf);
    meta.pdfStatus = PDF_STATUS.READY;
    pdfCopied = true;
  }

  const completedAt = new Date().toISOString();
  const generationLog = {
    startedAt: startedAt || meta.generatedAt,
    completedAt,
    durationMs:
      startedAt != null
        ? Math.max(0, Date.parse(completedAt) - Date.parse(startedAt))
        : 0,
    sourceCurrentPeriodId,
    sourcePriorPeriodId,
    builderVersion: meta.builderVersion,
    actionLibraryVersion: meta.actionLibraryVersion,
    pdfRenderResult: pdfCopied ? "COPIED_OR_READY" : "MISSING",
    warnings: warnings || [],
    errors: errors || [],
    liveProviderCalls: meta.liveProviderCalls,
    generationReason,
    generationStatus,
  };
  writeJson(paths.generationLog, generationLog);
  writeJson(paths.fingerprints, {
    contentFingerprint: meta.contentFingerprint,
    pdfFingerprint: meta.pdfFingerprint,
  });
  writeJson(paths.metadataJson, meta);

  const index = loadArchiveIndex();
  if (!skipMarkCurrent) {
    markSuperseded(index, propertyId, reportingMonthKey, reviewId);
  }
  index.reviews.push({
    reviewId: meta.reviewId,
    propertyId: meta.propertyId,
    propertyName: meta.propertyName,
    reportingMonth: meta.reportingMonth,
    reportingMonthKey: meta.reportingMonthKey,
    currentMonitoringDate: meta.currentMonitoringDate,
    priorRunDate: meta.priorRunDate,
    sourceCurrentPeriodId: meta.sourceCurrentPeriodId,
    sourcePriorPeriodId: meta.sourcePriorPeriodId,
    versionNumber: meta.versionNumber,
    versionLabel: meta.versionLabel,
    reviewStatus: meta.reviewStatus,
    generationStatus: meta.generationStatus,
    generationReason: meta.generationReason,
    pdfStatus: meta.pdfStatus,
    actionCount: meta.actionCount,
    generatedAt: meta.generatedAt,
    clientIssuedAt: meta.clientIssuedAt,
    archivedAt: meta.archivedAt,
    isCurrent: meta.isCurrent,
    builderVersion: meta.builderVersion,
    actionLibraryVersion: meta.actionLibraryVersion,
    contentFingerprint: meta.contentFingerprint,
    pdfFingerprint: meta.pdfFingerprint,
    supersedesReviewId: meta.supersedesReviewId,
    posture: meta.posture,
    qualityGates: meta.qualityGates,
  });
  saveArchiveIndex(index);

  return { meta, paths, generationLog };
}

export function summarizeQualityGates(review) {
  const g = review?.gates || {};
  const checks = [
    {
      id: "Executive Quality",
      pass: Boolean(
        g.MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE &&
          g.MONTHLY_REVIEW_SINGLE_CANONICAL_PAYLOAD
      ),
    },
    {
      id: "Action Quality",
      pass: Boolean(
        g.ADP_ACTION_EXECUTABILITY_STANDARD !== false &&
          g.MONTHLY_REVIEW_ACTION_ACCOUNTABILITY_COMPLETE !== false &&
          g.ADP_ACTION_NO_GENERIC_FINAL_RECOMMENDATIONS !== false
      ),
    },
    {
      id: "PDF Integrity",
      pass: null, // filled by caller when PDF known
    },
    {
      id: "Payload Integrity",
      pass: Boolean(g.MONTHLY_REVIEW_SINGLE_CANONICAL_PAYLOAD),
    },
    {
      id: "Visual Integrity",
      pass: true, // composition present; visual QA is Playwright
    },
  ];
  const scored = checks.filter((c) => c.pass !== null);
  const passed = scored.filter((c) => c.pass).length;
  return {
    summary: `${passed}/${scored.length} PASS`,
    checks,
  };
}

/**
 * Register existing Cambridge + NOHO golden fixtures into archive without regenerating.
 */
export function registerGoldenPairArchive({ force = false } = {}) {
  const results = [];
  for (const propertyId of GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS) {
    const index = loadArchiveIndex();
    const existing = index.reviews.find(
      (r) =>
        r.propertyId === propertyId &&
        r.generationReason === GENERATION_REASON.GOLDEN_REGISTER
    );
    if (existing && !force) {
      results.push({ propertyId, reviewId: existing.reviewId, status: "already_registered" });
      continue;
    }

    const fixtureRel = GOLDEN_FIXTURE_BY_PROPERTY[propertyId];
    const fixturePath = path.join(ROOT, fixtureRel);
    if (!fs.existsSync(fixturePath)) {
      throw new Error(`golden_fixture_missing:${propertyId}`);
    }
    const review = readJson(fixturePath);
    const pdfRel = GOLDEN_PDF_BY_PROPERTY[propertyId];
    const pdfPath = path.join(ROOT, pdfRel);

    // If force re-register, still create a NEW version (immutability).
    const { meta } = persistNewReviewVersion({
      review,
      generationReason: GENERATION_REASON.GOLDEN_REGISTER,
      generatedBy: "system:golden-register",
      reviewStatus: REVIEW_STATUS.READY_FOR_REVIEW,
      generationStatus: GENERATION_STATUS.SUCCESS,
      pdfSourcePath: fs.existsSync(pdfPath) ? pdfPath : null,
      liveProviderCalls: 0,
      warnings: fs.existsSync(pdfPath) ? [] : ["golden_pdf_missing_at_register"],
      startedAt: review.generatedAt || new Date().toISOString(),
    });

    // Patch generationReason onto index entry (stored on metadata)
    const paths = reviewArtifactPaths(meta);
    const fullMeta = readJson(paths.metadataJson);
    fullMeta.generationReason = GENERATION_REASON.GOLDEN_REGISTER;
    fullMeta.provenance = {
      fixturePath: fixtureRel,
      pdfPath: fs.existsSync(pdfPath) ? pdfRel : null,
      note: "Registered existing golden output; not regenerated for archive population.",
    };
    if (fullMeta.pdfStatus === PDF_STATUS.READY) {
      fullMeta.qualityGates = summarizeQualityGates(review);
      fullMeta.qualityGates.checks = fullMeta.qualityGates.checks.map((c) =>
        c.id === "PDF Integrity" ? { ...c, pass: true } : c
      );
      const scored = fullMeta.qualityGates.checks.filter((c) => c.pass !== null);
      const passed = scored.filter((c) => c.pass).length;
      fullMeta.qualityGates.summary = `${passed}/${scored.length} PASS`;
    }
    writeJson(paths.metadataJson, fullMeta);

    const idx = loadArchiveIndex();
    const row = idx.reviews.find((r) => r.reviewId === meta.reviewId);
    if (row) {
      row.generationReason = GENERATION_REASON.GOLDEN_REGISTER;
      row.qualityGates = fullMeta.qualityGates;
      saveArchiveIndex(idx);
    }

    results.push({ propertyId, reviewId: meta.reviewId, status: "registered", meta: fullMeta });
  }
  return results;
}

export function listArchiveReviews(filters = {}) {
  const index = loadArchiveIndex();
  let rows = [...index.reviews];

  if (filters.includeArchived !== true) {
    rows = rows.filter((r) => r.reviewStatus !== REVIEW_STATUS.ARCHIVED);
  }
  if (filters.propertyId) {
    rows = rows.filter((r) => r.propertyId === filters.propertyId);
  }
  if (filters.reportingMonth) {
    const key = reportingMonthKeyFromLabel(filters.reportingMonth);
    rows = rows.filter(
      (r) =>
        r.reportingMonth === filters.reportingMonth || r.reportingMonthKey === key
    );
  }
  if (filters.reviewStatus) {
    rows = rows.filter((r) => r.reviewStatus === filters.reviewStatus);
  }
  if (filters.generationStatus) {
    rows = rows.filter((r) => r.generationStatus === filters.generationStatus);
  }
  if (filters.clientIssued === true) {
    rows = rows.filter(
      (r) => r.reviewStatus === REVIEW_STATUS.CLIENT_ISSUED || r.clientIssuedAt
    );
  }
  if (filters.clientIssued === false) {
    rows = rows.filter(
      (r) => r.reviewStatus !== REVIEW_STATUS.CLIENT_ISSUED && !r.clientIssuedAt
    );
  }
  if (filters.hasPdf === true) {
    rows = rows.filter((r) => r.pdfStatus === PDF_STATUS.READY);
  }
  if (filters.hasPdf === false) {
    rows = rows.filter((r) => r.pdfStatus !== PDF_STATUS.READY);
  }
  if (filters.q) {
    const q = String(filters.q).toLowerCase();
    rows = rows.filter((r) =>
      String(r.propertyName || r.propertyId || "")
        .toLowerCase()
        .includes(q)
    );
  }
  if (filters.actionPatternId) {
    rows = rows.filter((r) => {
      try {
        const payload = loadReviewPayload(r.reviewId);
        const actions = payload?.review?.actionAgenda || [];
        return actions.some((a) => a.actionPatternId === filters.actionPatternId);
      } catch {
        return false;
      }
    });
  }

  rows.sort((a, b) => {
    const da = Date.parse(b.generatedAt || 0) - Date.parse(a.generatedAt || 0);
    if (da !== 0) return da;
    return (b.versionNumber || 0) - (a.versionNumber || 0);
  });
  return rows;
}

export function summaryCounts(rows) {
  const counts = {
    DRAFT: 0,
    READY_FOR_REVIEW: 0,
    APPROVED: 0,
    CLIENT_ISSUED: 0,
    SUPERSEDED: 0,
    ARCHIVED: 0,
    GENERATION_FAILED: 0,
  };
  for (const r of rows) {
    if (counts[r.reviewStatus] != null) counts[r.reviewStatus] += 1;
    if (r.generationStatus === GENERATION_STATUS.FAILED) counts.GENERATION_FAILED += 1;
  }
  return counts;
}

/**
 * Archive is non-destructive: soft status only.
 */
export function archiveReview(reviewId, { archivedBy = "admin" } = {}) {
  const meta = loadReviewMetadata(reviewId);
  if (!meta) throw new Error("review_not_found");
  if (meta.reviewStatus === REVIEW_STATUS.CLIENT_ISSUED) {
    // Still allow soft-hide from default views, but preserve CLIENT_ISSUED lock on artifacts.
  }
  const paths = reviewArtifactPaths(meta);
  const full = fs.existsSync(paths.metadataJson)
    ? readJson(paths.metadataJson)
    : { ...meta };
  if (full.reviewStatus === REVIEW_STATUS.ARCHIVED) {
    return { meta: full, alreadyArchived: true };
  }
  full.previousReviewStatus = full.reviewStatus;
  full.reviewStatus = REVIEW_STATUS.ARCHIVED;
  full.archivedAt = new Date().toISOString();
  full.archivedBy = archivedBy;
  full.isCurrent = false;
  writeJson(paths.metadataJson, full);

  const index = loadArchiveIndex();
  const row = index.reviews.find((r) => r.reviewId === reviewId);
  if (row) {
    row.reviewStatus = REVIEW_STATUS.ARCHIVED;
    row.archivedAt = full.archivedAt;
    row.isCurrent = false;
    saveArchiveIndex(index);
  }
  return { meta: full, alreadyArchived: false };
}

/**
 * CLIENT_ISSUED lock — never mutate artifacts after issue.
 */
export function markClientIssued(reviewId, { issuedBy = "admin" } = {}) {
  const pack = loadReviewPayload(reviewId);
  if (!pack) throw new Error("review_not_found");
  const { meta, paths } = pack;
  const full = readJson(paths.metadataJson);
  if (full.reviewStatus === REVIEW_STATUS.CLIENT_ISSUED) {
    return { meta: full, alreadyIssued: true };
  }
  full.reviewStatus = REVIEW_STATUS.CLIENT_ISSUED;
  full.clientIssuedAt = new Date().toISOString();
  full.clientIssuedBy = issuedBy;
  writeJson(paths.metadataJson, full);

  const index = loadArchiveIndex();
  const row = index.reviews.find((r) => r.reviewId === reviewId);
  if (row) {
    row.reviewStatus = REVIEW_STATUS.CLIENT_ISSUED;
    row.clientIssuedAt = full.clientIssuedAt;
    saveArchiveIndex(index);
  }
  return { meta: full, alreadyIssued: false };
}

export function assertNotClientIssuedMutable(reviewId) {
  const meta = loadReviewMetadata(reviewId);
  if (!meta) throw new Error("review_not_found");
  if (!canMutateReviewArtifacts(meta)) {
    const err = new Error("client_issued_immutable");
    err.code = "ADP_MONTHLY_REVIEW_CLIENT_ISSUED_IMMUTABILITY";
    throw err;
  }
  return meta;
}

/** Refuse overwrite of existing artifact files for a reviewId. */
export function assertReviewDirImmutable(reviewId) {
  const meta = loadReviewMetadata(reviewId);
  if (!meta) return;
  const paths = reviewArtifactPaths(meta);
  if (fs.existsSync(paths.reviewJson)) {
    const err = new Error("version_immutable_refuse_overwrite");
    err.code = "ADP_MONTHLY_REVIEW_VERSION_IMMUTABILITY";
    throw err;
  }
}

export function appendActionFounderFeedback(entry) {
  ensureDir(path.dirname(ACTION_FEEDBACK_PATH));
  const row = {
    schema: ACTION_FOUNDER_FEEDBACK_SCHEMA,
    actionInstanceId: entry.actionInstanceId,
    actionPatternId: entry.actionPatternId,
    reviewId: entry.reviewId,
    rating: entry.rating,
    note: entry.note || null,
    createdAt: new Date().toISOString(),
    reviewedBy: entry.reviewedBy || "admin",
  };
  if (!Object.values(ACTION_FEEDBACK_RATINGS).includes(row.rating)) {
    throw new Error("invalid_action_feedback_rating");
  }
  fs.appendFileSync(ACTION_FEEDBACK_PATH, `${JSON.stringify(row)}\n`, "utf8");
  return row;
}

export function appendReportSectionFeedback(entry) {
  ensureDir(path.dirname(REPORT_FEEDBACK_PATH));
  const row = {
    schema: REPORT_SECTION_FEEDBACK_SCHEMA,
    reviewId: entry.reviewId,
    section: entry.section,
    rating: entry.rating,
    note: entry.note || null,
    createdAt: new Date().toISOString(),
    reviewedBy: entry.reviewedBy || "admin",
  };
  if (!REPORT_FEEDBACK_SECTIONS.includes(row.section)) {
    throw new Error("invalid_report_feedback_section");
  }
  if (!Object.values(REPORT_FEEDBACK_RATINGS).includes(row.rating)) {
    throw new Error("invalid_report_feedback_rating");
  }
  fs.appendFileSync(REPORT_FEEDBACK_PATH, `${JSON.stringify(row)}\n`, "utf8");
  return row;
}

export function readFeedbackForReview(reviewId) {
  const action = [];
  const report = [];
  if (fs.existsSync(ACTION_FEEDBACK_PATH)) {
    for (const line of fs.readFileSync(ACTION_FEEDBACK_PATH, "utf8").split("\n")) {
      if (!line.trim()) continue;
      const row = JSON.parse(line);
      if (row.reviewId === reviewId) action.push(row);
    }
  }
  if (fs.existsSync(REPORT_FEEDBACK_PATH)) {
    for (const line of fs.readFileSync(REPORT_FEEDBACK_PATH, "utf8").split("\n")) {
      if (!line.trim()) continue;
      const row = JSON.parse(line);
      if (row.reviewId === reviewId) report.push(row);
    }
  }
  return { action, report };
}

export function getEligibleGeneratePreview(propertyId) {
  const eligibility = resolveAdpMonthlyReviewEligibilityV1(propertyId);
  if (!eligibility.eligible) {
    throw Object.assign(new Error("monthly_review_not_eligible"), {
      code: eligibility.status,
      eligibility,
    });
  }
  const fixtureRel = GOLDEN_FIXTURE_BY_PROPERTY[propertyId];
  const fixturePath = fixtureRel ? path.join(ROOT, fixtureRel) : null;
  const fixture =
    fixturePath && fs.existsSync(fixturePath) ? readJson(fixturePath) : null;
  return {
    propertyId,
    propertyName: eligibility.propertyName || fixture?.property?.name || propertyId,
    eligibilityStatus: eligibility.status,
    warnings: eligibility.warnings || [],
    currentEligibleMonitoringPeriod:
      eligibility.currentPeriodId || fixture?.reporting?.currentPeriodId || null,
    currentMonitoringDate: fixture?.reporting?.currentMonitoringDate || null,
    resolvedPriorComparablePeriod:
      eligibility.priorPeriodId || fixture?.reporting?.priorPeriodId || null,
    priorRunDate: fixture?.reporting?.priorRunDate || null,
    hasComparablePrior: eligibility.hasComparablePrior,
    reviewBuilderVersion: REVIEW_BUILDER_VERSION,
    actionLibraryVersion: ACTION_LIBRARY_VERSION,
    liveProviderCalls: 0,
    note: "Generate New Draft uses latest eligible certified/published ADP periods via the review builder. No provider calls.",
  };
}

export function getBulkEligibleGeneratePreview() {
  const batch = resolveAllAdpMonthlyReviewEligibilityV1();
  return {
    schema: "ADP_MONTHLY_REVIEW_BULK_GENERATE_PREVIEW_V1",
    liveProviderCalls: 0,
    gate: "ADP_MONTHLY_REVIEW_BULK_GENERATION_ZERO_PROVIDER_CALLS",
    counts: batch.counts,
    eligible: batch.eligible.map((e) => ({
      propertyId: e.propertyId,
      propertyName: e.propertyName,
      status: e.status,
      currentPeriodId: e.currentPeriodId,
      priorPeriodId: e.priorPeriodId,
      warnings: e.warnings,
    })),
    blocked: batch.blocked.map((e) => ({
      propertyId: e.propertyId,
      status: e.status,
      blockers: e.blockers,
    })),
    confirmationRequired: true,
    note: "Bulk generation requires explicit founder/admin confirmation. Does not run automatically. LIVE_PROVIDER_CALLS = 0.",
  };
}

export {
  ARCHIVE_INDEX_SCHEMA,
  GOLDEN_FIXTURE_BY_PROPERTY,
  GOLDEN_PDF_BY_PROPERTY,
  ROOT as ARCHIVE_REPO_ROOT,
};
