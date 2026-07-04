/**
 * Partner Intelligence — Source Library API (Phase 3).
 *
 * GET    /api/partner-intelligence/sources
 * GET    /api/partner-intelligence/sources/:recordId
 * POST   /api/partner-intelligence/sources
 * PATCH  /api/partner-intelligence/sources/:recordId
 * POST   /api/partner-intelligence/sources/:recordId/upload  (multipart)
 * GET    /api/partner-intelligence/pilot
 */
import {
  listPartnerSources,
  getPartnerSourceById,
  createPartnerSource,
  patchPartnerSource,
  resolveUploadFolderForSource,
  relativeLocalFilePath,
} from "../lib/partner-intelligence/airtable-source.js";
import {
  validatePartnerSourcePayload,
  inferFileTypeFromName,
} from "../lib/partner-intelligence/validate-source-payload.js";
import { isDevPartnerIntelligenceLogging } from "../middleware/requirePartnerIntelligenceAccess.js";
import {
  PILOT_OPERATORS,
  PILOT_OPERATOR_SOURCE_CANDIDATES,
} from "./lib/partner-intelligence-explorer-field-registry.js";
import {
  listPortals,
  HELENA_OUTREACH_TEMPLATE,
} from "./lib/partner-development-portal-registry.js";

function jsonError(res, status, error, message, extra) {
  return res.status(status).json({
    ok: false,
    success: false,
    error,
    message,
    ...(extra || {}),
  });
}

export async function getPartnerIntelligencePilot(req, res) {
  try {
    return res.json({
      ok: true,
      success: true,
      pilotOperators: PILOT_OPERATORS,
      suggestedSources: PILOT_OPERATOR_SOURCE_CANDIDATES,
      developmentPortals: listPortals(),
      helenaOutreachTemplate: HELENA_OUTREACH_TEMPLATE,
      collectionGuide: "/docs/partner-reference-material-collection-guide.md",
    });
  } catch (err) {
    console.error("[partner-intelligence/pilot]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to load pilot config.");
  }
}

export async function listPartnerIntelligenceSources(req, res) {
  try {
    const result = await listPartnerSources({
      operatorId: req.query.operatorId,
      brandId: req.query.brandId,
      profileType: req.query.profileType,
      status: req.query.status,
      limit: req.query.limit,
      offset: req.query.offset,
    });
    return res.json({
      ok: true,
      success: true,
      sources: result.sources,
      offset: result.offset,
      count: result.sources.length,
    });
  } catch (err) {
    console.error("[partner-intelligence/sources:list]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Failed to list sources.");
  }
}

export async function getPartnerIntelligenceSourceById(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(recordId || "")) {
      return jsonError(res, 400, "invalid_record_id", "Valid Airtable record id (rec…) required.");
    }
    const source = await getPartnerSourceById(recordId);
    if (!source) {
      return jsonError(res, 404, "not_found", "Source record not found.");
    }
    return res.json({ ok: true, success: true, source });
  } catch (err) {
    console.error("[partner-intelligence/sources:get]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Failed to load source.");
  }
}

export async function createPartnerIntelligenceSource(req, res) {
  try {
    const validation = validatePartnerSourcePayload(req.body, { mode: "create" });
    if (!validation.ok) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: "validation_failed",
        message: "Source payload validation failed.",
        validation: {
          pass: false,
          failedChecks: validation.errors,
        },
        ...(isDevPartnerIntelligenceLogging()
          ? { sanitizedPayloadPreview: validation.fields, fieldMapping: validation.fieldMapping }
          : {}),
      });
    }

    const source = await createPartnerSource(validation.fields);

    if (isDevPartnerIntelligenceLogging()) {
      console.info("[partner-intelligence/sources:create] created", source.id, source.sourceTitle);
    }

    return res.status(201).json({
      ok: true,
      success: true,
      source,
      validation: { pass: true, failedChecks: [] },
      ...(isDevPartnerIntelligenceLogging()
        ? { sanitizedPayloadPreview: validation.fields, fieldMapping: validation.fieldMapping }
        : {}),
    });
  } catch (err) {
    console.error("[partner-intelligence/sources:create]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Failed to create source.");
  }
}

export async function patchPartnerIntelligenceSource(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(recordId || "")) {
      return jsonError(res, 400, "invalid_record_id", "Valid Airtable record id (rec…) required.");
    }

    const validation = validatePartnerSourcePayload(req.body, { mode: "patch" });
    if (!validation.ok) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: "validation_failed",
        message: "Source payload validation failed.",
        validation: { pass: false, failedChecks: validation.errors },
      });
    }

    if (Object.keys(validation.fields).length === 0) {
      return jsonError(res, 400, "empty_patch", "No valid fields to update.");
    }

    const source = await patchPartnerSource(recordId, validation.fields);
    return res.json({
      ok: true,
      success: true,
      source,
      validation: { pass: true, failedChecks: [] },
    });
  } catch (err) {
    console.error("[partner-intelligence/sources:patch]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Failed to update source.");
  }
}

/**
 * Multer must run before this handler (req.file set).
 * Optional body fields: referenceFolder
 */
export async function uploadPartnerIntelligenceSourceFile(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(recordId || "")) {
      return jsonError(res, 400, "invalid_record_id", "Valid Airtable record id (rec…) required.");
    }

    const file = req.file;
    if (!file) {
      return jsonError(res, 400, "no_file", "Multipart field 'file' is required.");
    }

    const existing = await getPartnerSourceById(recordId);
    if (!existing) {
      return jsonError(res, 404, "not_found", "Source record not found.");
    }

    const referenceFolder =
      (req.body && req.body.referenceFolder) ||
      (existing.operatorId === PILOT_OPERATORS.arborLodging.recordId
        ? PILOT_OPERATORS.arborLodging.referenceFolder
        : "inbox");

    const relativePath = relativeLocalFilePath(referenceFolder, file.filename);
    const patchFields = {
      "Local File Path": relativePath,
      "File Type": inferFileTypeFromName(file.originalname || file.filename),
      "Capture Date": new Date().toISOString().slice(0, 10),
    };
    if (existing.status === "Found") {
      patchFields.Status = "Captured";
    }

    const source = await patchPartnerSource(recordId, patchFields);

    return res.json({
      ok: true,
      success: true,
      source,
      upload: {
        originalName: file.originalname,
        storedFilename: file.filename,
        size: file.size,
        relativePath,
        absolutePath: file.path,
      },
    });
  } catch (err) {
    console.error("[partner-intelligence/sources:upload]", err);
    return jsonError(res, 500, "upload_error", err.message || "File upload failed.");
  }
}

/** Middleware: set req.partnerIntelligenceUploadDir before multer */
export async function resolvePartnerIntelligenceUploadDir(req, res, next) {
  try {
    const recordId = req.params.recordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(recordId || "")) {
      return jsonError(res, 400, "invalid_record_id", "Valid Airtable record id (rec…) required.");
    }

    const source = await getPartnerSourceById(recordId);
    if (!source) {
      return jsonError(res, 404, "not_found", "Source record not found.");
    }
    req.partnerIntelligenceSource = source;

    const referenceFolder =
      (req.body && req.body.referenceFolder) ||
      (req.query && req.query.referenceFolder) ||
      (source.operatorId === PILOT_OPERATORS.arborLodging.recordId
        ? PILOT_OPERATORS.arborLodging.referenceFolder
        : "inbox");

    req.partnerIntelligenceUploadDir = resolveUploadFolderForSource(source, referenceFolder);
    return next();
  } catch (err) {
    console.error("[partner-intelligence/upload-dir]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to resolve upload folder.");
  }
}
