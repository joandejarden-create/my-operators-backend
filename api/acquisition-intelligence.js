/**
 * Acquisition Intelligence API — admin-only LinkedIn Connections CSV ingestion.
 *
 * Routes (wired in server.js with adminAuth):
 *   POST /api/acquisition-intelligence/connections/preview
 *   POST /api/acquisition-intelligence/connections/import
 *   GET  /api/acquisition-intelligence/import-batches
 *   GET  /api/acquisition-intelligence/summary
 *
 * Stage 1: CSV preview + import foundation only.
 * No LinkedIn scraping, no automated outreach, no customer exposure.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import {
  previewLinkedInConnectionsImport,
  applyLinkedInConnectionsImport,
  listAcquisitionImportBatchesForUser,
  getAcquisitionNetworkSummaryForUser,
} from "../lib/acquisition-intelligence/import-apply.js";
import { assertPreviewImportable } from "../lib/acquisition-intelligence/linkedin-connections-preview.js";
import {
  classifyAcquisitionNetworkForUser,
  listClassifiedRelationshipsForUser,
  getAcquisitionClassificationSummaryForUser,
} from "../lib/acquisition-intelligence/classify-batch.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PREVIEW_DIR = path.join(ROOT, "data", "internal", "acquisition-intelligence", "previews");
const PREVIEW_TTL_MS = 60 * 60 * 1000; // 1 hour

function jsonError(res, status, error, message, extra = {}) {
  return res.status(status).json({
    ok: false,
    success: false,
    error,
    message,
    ...extra,
  });
}

function resolveSourceUserId(req) {
  const u = req.dealalityUser || {};
  return (
    String(u.memberstackId || u.id || req.user?.id || "").trim() || null
  );
}

function ensurePreviewDir(userId) {
  const dir = path.join(PREVIEW_DIR, sanitizePathSegment(userId));
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function sanitizePathSegment(value) {
  return String(value || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 80);
}

function readCsvFromUpload(req) {
  if (req.file?.buffer) {
    return {
      text: req.file.buffer.toString("utf8"),
      fileName: req.file.originalname || "Connections.csv",
    };
  }
  if (req.file?.path) {
    return {
      text: fs.readFileSync(req.file.path, "utf8"),
      fileName: req.file.originalname || path.basename(req.file.path),
    };
  }
  if (typeof req.body?.csvText === "string" && req.body.csvText.trim()) {
    return {
      text: req.body.csvText,
      fileName: String(req.body.fileName || "Connections.csv"),
    };
  }
  return null;
}

function storePreviewPayload(sourceUserId, payload) {
  const previewId = `prev_${crypto.randomBytes(12).toString("hex")}`;
  const dir = ensurePreviewDir(sourceUserId);
  const filePath = path.join(dir, `${previewId}.json`);
  const record = {
    previewId,
    sourceUserId,
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + PREVIEW_TTL_MS).toISOString(),
    fileName: payload.fileName,
    csvText: payload.csvText,
    preview: payload.preview,
  };
  fs.writeFileSync(filePath, JSON.stringify(record), "utf8");
  return { previewId, expiresAt: record.expiresAt, previewPath: filePath };
}

function loadPreviewPayload(sourceUserId, previewId) {
  const id = String(previewId || "").trim();
  if (!/^prev_[a-f0-9]{24}$/.test(id)) return null;
  const filePath = path.join(PREVIEW_DIR, sanitizePathSegment(sourceUserId), `${id}.json`);
  if (!fs.existsSync(filePath)) return null;
  let record;
  try {
    record = JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (err) {
    console.error("[acquisition-intelligence] preview_read_failed", err?.message || err);
    return null;
  }
  if (record.sourceUserId !== sourceUserId) return null;
  if (record.expiresAt && Date.parse(record.expiresAt) < Date.now()) {
    try {
      fs.unlinkSync(filePath);
    } catch (err) {
      console.warn("[acquisition-intelligence] preview_expire_cleanup_failed", err?.message || err);
    }
    return null;
  }
  return { ...record, previewPath: filePath };
}

/**
 * POST /api/acquisition-intelligence/connections/preview
 */
export async function postAcquisitionConnectionsPreview(req, res) {
  try {
    const sourceUserId = resolveSourceUserId(req);
    if (!sourceUserId) {
      return jsonError(res, 401, "unauthorized", "Authenticated Dealality user required.");
    }

    const upload = readCsvFromUpload(req);
    if (!upload) {
      return jsonError(
        res,
        400,
        "missing_file",
        "Upload a LinkedIn Connections CSV (multipart field name: file)."
      );
    }

    if (!/\.csv$/i.test(upload.fileName) && !req.body?.csvText) {
      return jsonError(res, 400, "invalid_file_type", "Only .csv LinkedIn Connections exports are accepted.");
    }

    const preview = previewLinkedInConnectionsImport(upload.text, {
      fileName: upload.fileName,
      sourceUserId,
    });

    if (!preview.ok) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: preview.error,
        message: preview.message,
        validation: preview.validation,
        fileName: preview.fileName,
      });
    }

    const stored = storePreviewPayload(sourceUserId, {
      fileName: upload.fileName,
      csvText: upload.text,
      preview: {
        stats: preview.stats,
        warnings: preview.warnings,
        sampleRows: preview.sampleRows,
        invalidRowSamples: preview.invalidRowSamples,
        duplicateSamples: preview.duplicateSamples,
        validation: preview.validation,
      },
    });

    // Do not return full parsed PII beyond sample rows
    return res.json({
      ok: true,
      success: true,
      previewId: stored.previewId,
      expiresAt: stored.expiresAt,
      fileName: preview.fileName,
      validation: preview.validation,
      stats: preview.stats,
      warnings: preview.warnings,
      sampleRows: preview.sampleRows,
      invalidRowSamples: preview.invalidRowSamples,
      duplicateSamples: preview.duplicateSamples,
      fieldMapping: preview.fieldMapping,
      message:
        "Preview ready. Review stats, then call Import Connections with this previewId. No records were written.",
    });
  } catch (err) {
    console.error("[acquisition-intelligence:preview]", err);
    return jsonError(res, 500, "preview_error", err.message || "Preview failed.");
  }
}

/**
 * POST /api/acquisition-intelligence/connections/import
 * Body: { previewId, confirm: true } OR re-upload file with confirm:true
 */
export async function postAcquisitionConnectionsImport(req, res) {
  try {
    const sourceUserId = resolveSourceUserId(req);
    if (!sourceUserId) {
      return jsonError(res, 401, "unauthorized", "Authenticated Dealality user required.");
    }

    const confirm =
      req.body?.confirm === true ||
      req.body?.confirm === "true" ||
      req.body?.confirm === "1";
    if (!confirm) {
      return jsonError(
        res,
        400,
        "confirmation_required",
        "Import requires confirm=true after reviewing the preview."
      );
    }

    const dryRun =
      req.body?.dryRun === true ||
      req.body?.dryRun === "true" ||
      req.query?.dryRun === "1";

    let csvText = null;
    let fileName = "Connections.csv";
    let previewPath = null;
    let previewId = String(req.body?.previewId || "").trim();

    if (previewId) {
      const stored = loadPreviewPayload(sourceUserId, previewId);
      if (!stored) {
        return jsonError(
          res,
          400,
          "preview_expired_or_missing",
          "Preview not found or expired. Upload again to generate a new preview."
        );
      }
      csvText = stored.csvText;
      fileName = stored.fileName || fileName;
      previewPath = stored.previewPath;
      const gate = assertPreviewImportable({
        ok: true,
        validation: stored.preview?.validation || { pass: true, failedChecks: [] },
      });
      if (!gate.ok) {
        return jsonError(res, 400, gate.error, gate.message, {
          failedChecks: gate.failedChecks,
        });
      }
    } else {
      const upload = readCsvFromUpload(req);
      if (!upload) {
        return jsonError(
          res,
          400,
          "missing_preview_or_file",
          "Provide previewId from the preview step, or upload the CSV again."
        );
      }
      csvText = upload.text;
      fileName = upload.fileName;
    }

    const result = await applyLinkedInConnectionsImport(csvText, {
      sourceUserId,
      fileName,
      dryRun,
      previewReportPath: previewPath || undefined,
    });

    if (!result.ok) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: result.error,
        message: result.message,
        validation: result.validation,
        preview: result.preview
          ? { stats: result.preview.stats, validation: result.preview.validation }
          : undefined,
      });
    }

    if (!dryRun && previewPath && fs.existsSync(previewPath)) {
      try {
        fs.unlinkSync(previewPath);
      } catch (err) {
        console.warn("[acquisition-intelligence] preview_cleanup_failed", err?.message || err);
      }
    }

    return res.json({
      ok: true,
      success: true,
      dryRun: result.dryRun,
      importBatchId: result.importBatchId || null,
      validation: result.validation,
      planSummary: result.planSummary,
      created: result.created || null,
      updated: result.updated || null,
      skipped: result.skipped ?? result.planSummary?.skipped ?? 0,
      sanitizedPayloadPreview: result.sanitizedPayloadPreview,
      fieldMapping: result.fieldMapping,
      errorHandling: result.errorHandling || {
        validationError: "Returned before Airtable writes.",
        apiError: "Surfaced in response/logs; no silent catch.",
        network: "Retry — imports are idempotent by relationship dedupe key.",
      },
      message: result.dryRun
        ? "Dry-run complete — no Airtable writes."
        : "Import applied to your private acquisition network.",
    });
  } catch (err) {
    console.error("[acquisition-intelligence:import]", err);
    return jsonError(res, 500, "import_error", err.message || "Import failed.", {
      errorHandling: {
        validationError: "N/A — exception path",
        apiError: err.message || "unknown",
        network: /fetch|ECONN|ETIMEDOUT/i.test(String(err.message || "")) ? "likely" : "unlikely",
      },
    });
  }
}

/**
 * GET /api/acquisition-intelligence/import-batches
 */
export async function getAcquisitionImportBatches(req, res) {
  try {
    const sourceUserId = resolveSourceUserId(req);
    if (!sourceUserId) {
      return jsonError(res, 401, "unauthorized", "Authenticated Dealality user required.");
    }
    const result = await listAcquisitionImportBatchesForUser(sourceUserId, {
      limit: Number(req.query.limit) || 25,
    });
    if (!result.ok) {
      return jsonError(res, 400, result.error, "Could not list import batches.");
    }
    return res.json({ ok: true, success: true, batches: result.batches });
  } catch (err) {
    console.error("[acquisition-intelligence:batches]", err);
    return jsonError(res, 500, "batches_error", err.message || "Failed to list batches.");
  }
}

/**
 * GET /api/acquisition-intelligence/summary
 */
export async function getAcquisitionSummary(req, res) {
  try {
    const sourceUserId = resolveSourceUserId(req);
    if (!sourceUserId) {
      return jsonError(res, 401, "unauthorized", "Authenticated Dealality user required.");
    }

    let metrics = null;
    try {
      const classified = await getAcquisitionClassificationSummaryForUser(sourceUserId);
      if (classified.ok) metrics = classified.metrics;
    } catch (err) {
      console.error("[acquisition-intelligence:summary:classified]", err?.message || err);
    }

    if (!metrics) {
      const fallback = await getAcquisitionNetworkSummaryForUser(sourceUserId);
      if (!fallback.ok) {
        return jsonError(res, 400, fallback.error, "Could not load summary.");
      }
      metrics = {
        totalConnections: fallback.metrics.totalConnections,
        relevantConnections: null,
        highDirectProspects: null,
        highConnectors: null,
        highDecisionVisibility: null,
        calaRelevant: null,
        researchPriority: null,
        researchCandidate: null,
        unclassified: null,
        lowRelevance: null,
        note: fallback.metrics.note,
      };
    }

    return res.json({ ok: true, success: true, metrics });
  } catch (err) {
    console.error("[acquisition-intelligence:summary]", err);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "summary_error",
      message: err.message || "Failed to load acquisition summary.",
      metrics: {
        totalConnections: null,
        relevantConnections: null,
        highDirectProspects: null,
        highConnectors: null,
        highDecisionVisibility: null,
        calaRelevant: null,
        researchPriority: null,
        researchCandidate: null,
      },
    });
  }
}

/**
 * POST /api/acquisition-intelligence/classify
 * Body: { confirm?: true, dryRun?: true }
 */
export async function postAcquisitionClassify(req, res) {
  try {
    const sourceUserId = resolveSourceUserId(req);
    if (!sourceUserId) {
      return jsonError(res, 401, "unauthorized", "Authenticated Dealality user required.");
    }

    const wantLive =
      (req.body?.confirm === true || req.body?.confirm === "true") &&
      !(req.body?.dryRun === true || req.body?.dryRun === "true" || req.query?.dryRun === "1");

    const result = await classifyAcquisitionNetworkForUser(sourceUserId, {
      dryRun: !wantLive,
      limit: req.body?.limit ? Number(req.body.limit) : undefined,
    });

    if (!result.ok) {
      return jsonError(res, 400, result.error, result.message || "Classification failed.");
    }

    // Strip heavy PII from review in API response — keep top 10 each for UI validation
    const review = result.review
      ? {
          topDirectProspects: (result.review.topDirectProspects || []).slice(0, 10),
          topConnectors: (result.review.topConnectors || []).slice(0, 10),
          topDecisionSignal: (result.review.topDecisionSignal || []).slice(0, 10),
        }
      : null;

    return res.json({
      ok: true,
      success: true,
      dryRun: result.dryRun,
      empty: Boolean(result.empty),
      summary: result.summary,
      review,
      fieldMapping: result.fieldMapping,
      message: result.empty
        ? result.message
        : result.dryRun
          ? "Classification dry-run complete — no Airtable writes."
          : "Classification applied for your acquisition network.",
    });
  } catch (err) {
    console.error("[acquisition-intelligence:classify]", err);
    return jsonError(res, 500, "classify_error", err.message || "Classification failed.");
  }
}

/**
 * GET /api/acquisition-intelligence/relationships
 */
export async function getAcquisitionRelationships(req, res) {
  try {
    const sourceUserId = resolveSourceUserId(req);
    if (!sourceUserId) {
      return jsonError(res, 401, "unauthorized", "Authenticated Dealality user required.");
    }
    const result = await listClassifiedRelationshipsForUser(sourceUserId, {
      limit: Number(req.query.limit) || 50,
      researchOnly: req.query.researchOnly === "1" || req.query.researchOnly === "true",
    });
    if (!result.ok) {
      return jsonError(res, 400, result.error, "Could not list relationships.");
    }
    if (!result.rows.length) {
      return res.json({
        ok: true,
        success: true,
        empty: true,
        rows: [],
        message: "No relationships yet. Import a LinkedIn Connections CSV first.",
      });
    }
    return res.json({ ok: true, success: true, empty: false, rows: result.rows });
  } catch (err) {
    console.error("[acquisition-intelligence:relationships]", err);
    return jsonError(res, 500, "relationships_error", err.message || "Failed to list relationships.");
  }
}
