/**
 * Partner Intelligence — extraction, facts review, publish APIs.
 */
import {
  runPartnerSourceExtraction,
  runPartnerOperatorExtraction,
  runPartnerBrandExtraction,
} from "../lib/partner-intelligence/run-extraction.js";
import {
  listPartnerFacts,
  getPartnerFactById,
  patchPartnerFact,
} from "../lib/partner-intelligence/airtable-facts.js";
import {
  getPartnerSourceById,
  listPartnerSources,
} from "../lib/partner-intelligence/airtable-source.js";
import {
  syncOperatorReferenceFolder,
  syncBrandReferenceFolder,
  isSourceExtractable,
  approveSourcesForBatchExtraction,
  getReferenceFolderForOperator,
  getReferenceFolderForBrand,
  listReadableReferenceFiles,
  listBrandReferenceFiles,
} from "../lib/partner-intelligence/sync-reference-folder.js";
import { PILOT_BRANDS } from "./lib/partner-intelligence-explorer-field-registry.js";
import { resolveReferenceRoot } from "../lib/partner-intelligence/airtable-source.js";
import { publishApprovedFact } from "../lib/partner-intelligence/publish-overlay.js";
import { MAP_PARTNER_FACT, VAL_PARTNER_FACT_SELECTS } from "./lib/partner-intelligence-field-map.js";
import { isDevPartnerIntelligenceLogging } from "../middleware/requirePartnerIntelligenceAccess.js";

function jsonError(res, status, error, message, extra) {
  return res.status(status).json({ ok: false, success: false, error, message, ...(extra || {}) });
}

export async function postPartnerIntelligenceExtractionRun(req, res) {
  try {
    const force = req.body?.force === true || req.query?.force === "1";
    const operatorId = req.body?.operatorId;
    const brandId = req.body?.brandId;
    const allSources = req.body?.allSources === true || req.body?.mode === "operator" || req.body?.mode === "brand";
    const syncFolder = req.body?.syncFolder !== false;

    if (allSources && /^rec[a-zA-Z0-9]+$/.test(brandId || "")) {
      const result = await runPartnerBrandExtraction(brandId, { force, syncFolder });
      return res.status(201).json({ ok: true, success: true, ...result });
    }

    if (allSources && /^rec[a-zA-Z0-9]+$/.test(operatorId || "")) {
      const result = await runPartnerOperatorExtraction(operatorId, { force, syncFolder });
      return res.status(201).json({ ok: true, success: true, ...result });
    }

    const sourceId = req.body?.sourceId || req.body?.sourceRecordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(sourceId || "")) {
      return jsonError(
        res,
        400,
        "invalid_request",
        "Provide sourceId, or operatorId with allSources:true for batch extraction."
      );
    }
    const result = await runPartnerSourceExtraction(sourceId, { force });
    return res.status(201).json({ ok: true, success: true, ...result });
  } catch (err) {
    console.error("[partner-intelligence/extraction/run]", err);
    return jsonError(res, 500, "extraction_failed", err.message || "Extraction failed.");
  }
}

export async function getPartnerIntelligenceExtractionContext(req, res) {
  try {
    const brandId = req.query.brandId;
    if (/^rec[a-zA-Z0-9]+$/.test(brandId || "")) {
      const pilot = Object.values(PILOT_BRANDS).find((p) => p.recordId === brandId);
      const { sources } = await listPartnerSources({ brandId, limit: 100 });
      const referenceFolder = getReferenceFolderForBrand(brandId);
      const folderScan = pilot
        ? listBrandReferenceFiles({
            referenceFolder: pilot.referenceFolder,
            includeSubpaths: pilot.includeSubpaths || [],
            brandNameMatch: pilot.brandNameMatch || pilot.brandSlug,
          })
        : null;
      return res.json({
        ok: true,
        success: true,
        profileType: "Brand",
        brandId,
        brandName: pilot?.brandName || null,
        referenceFolder,
        referenceRoot: resolveReferenceRoot(),
        folderFiles: folderScan?.files || [],
        folderMissing: folderScan?.missing || false,
        sources: sources.map((s) => ({
          id: s.id,
          sourceTitle: s.sourceTitle,
          sourceUrl: s.sourceUrl,
          localFilePath: s.localFilePath,
          sourceType: s.sourceType,
          status: s.status,
          approvedForExtraction: s.approvedForExtraction,
          extractable: !!(s.localFilePath || (s.sourceUrl && /^https?:\/\//i.test(s.sourceUrl))),
        })),
      });
    }

    const operatorId = req.query.operatorId;
    if (!/^rec[a-zA-Z0-9]+$/.test(operatorId || "")) {
      return jsonError(res, 400, "invalid_profile_id", "operatorId or brandId (rec…) is required.");
    }
    const { sources } = await listPartnerSources({ operatorId, limit: 100 });
    const referenceFolder = getReferenceFolderForOperator(operatorId);
    const folderScan = referenceFolder ? listReadableReferenceFiles(referenceFolder) : null;
    return res.json({
      ok: true,
      success: true,
      operatorId,
      referenceFolder,
      referenceRoot: resolveReferenceRoot(),
      folderFiles: folderScan?.files || [],
      folderMissing: folderScan?.missing || false,
      sources: sources.map((s) => ({
        id: s.id,
        sourceTitle: s.sourceTitle,
        sourceUrl: s.sourceUrl,
        localFilePath: s.localFilePath,
        sourceType: s.sourceType,
        status: s.status,
        approvedForExtraction: s.approvedForExtraction,
        extractable: !!(s.localFilePath || (s.sourceUrl && /^https?:\/\//i.test(s.sourceUrl))),
      })),
    });
  } catch (err) {
    console.error("[partner-intelligence/extraction/context]", err);
    return jsonError(res, 500, "context_failed", err.message || "Failed to load extraction context.");
  }
}

export async function listPartnerIntelligenceFacts(req, res) {
  try {
    const result = await listPartnerFacts({
      operatorId: req.query.operatorId,
      brandId: req.query.brandId,
      sourceRecordId: req.query.sourceRecordId,
      humanReviewStatus: req.query.humanReviewStatus || req.query.status,
      extractionRunId: req.query.extractionRunId,
      limit: req.query.limit,
      offset: req.query.offset,
    });
    return res.json({ ok: true, success: true, facts: result.facts, offset: result.offset, count: result.facts.length });
  } catch (err) {
    console.error("[partner-intelligence/facts:list]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Failed to list facts.");
  }
}

export async function getPartnerIntelligenceFactById(req, res) {
  try {
    const fact = await getPartnerFactById(req.params.recordId);
    if (!fact) return jsonError(res, 404, "not_found", "Fact not found.");
    let source = null;
    if (fact.sourceRecordId) {
      try {
        source = await getPartnerSourceById(fact.sourceRecordId);
      } catch (e) {
        console.warn("[partner-intelligence/facts:get] source load failed", e.message);
      }
    }
    return res.json({ ok: true, success: true, fact, source });
  } catch (err) {
    console.error("[partner-intelligence/facts:get]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Failed to load fact.");
  }
}

export async function patchPartnerIntelligenceFactReview(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(recordId || "")) {
      return jsonError(res, 400, "invalid_record_id", "Valid fact record id required.");
    }

    const body = req.body || {};
    const fields = {};
    const errors = [];

    if (body.humanReviewStatus !== undefined) {
      if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes(body.humanReviewStatus)) {
        errors.push("Invalid humanReviewStatus.");
      } else {
        fields[MAP_PARTNER_FACT.humanReviewStatus] = body.humanReviewStatus;
      }
    }
    if (body.approvedValue !== undefined) {
      fields[MAP_PARTNER_FACT.approvedValue] = String(body.approvedValue);
    }
    if (body.reviewerNotes !== undefined) {
      fields[MAP_PARTNER_FACT.reviewerNotes] = String(body.reviewerNotes);
    }
    if (body.publicVisibility !== undefined) {
      if (!VAL_PARTNER_FACT_SELECTS.publicVisibility.includes(body.publicVisibility)) {
        errors.push("Invalid publicVisibility.");
      } else {
        fields[MAP_PARTNER_FACT.publicVisibility] = body.publicVisibility;
      }
    }
    if (body.followUpQuestion !== undefined) {
      fields[MAP_PARTNER_FACT.followUpQuestion] = String(body.followUpQuestion);
    }

    if (errors.length) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: "validation_failed",
        validation: { pass: false, failedChecks: errors },
      });
    }
    if (Object.keys(fields).length === 0) {
      return jsonError(res, 400, "empty_patch", "No review fields provided.");
    }

    fields[MAP_PARTNER_FACT.lastUpdated] = new Date().toISOString().slice(0, 10);

    if (
      (body.humanReviewStatus === "Approved" || body.humanReviewStatus === "Edited") &&
      body.approvedValue === undefined
    ) {
      const existing = await getPartnerFactById(recordId);
      if (existing && !existing.approvedValue) {
        fields[MAP_PARTNER_FACT.approvedValue] = existing.extractedValue;
      }
    }

    const fact = await patchPartnerFact(recordId, fields);
    if (isDevPartnerIntelligenceLogging()) {
      console.info("[partner-intelligence/facts:review]", recordId, body.humanReviewStatus);
    }
    return res.json({ ok: true, success: true, fact, validation: { pass: true, failedChecks: [] } });
  } catch (err) {
    console.error("[partner-intelligence/facts:review]", err);
    return jsonError(res, 500, "airtable_error", err.message || "Review update failed.");
  }
}

export async function postPartnerIntelligencePublish(req, res) {
  try {
    const factId = req.body?.factId || req.body?.recordId;
    if (!/^rec[a-zA-Z0-9]+$/.test(factId || "")) {
      return jsonError(res, 400, "invalid_fact_id", "factId (rec…) is required.");
    }
    const result = await publishApprovedFact(factId);
    return res.json({
      ok: true,
      success: true,
      published: result.published,
      fact: result.fact,
      source: result.source,
      validation: { pass: true, failures: [] },
    });
  } catch (err) {
    const failures = err.validationFailures || [];
    if (failures.length) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: "publish_validation_failed",
        message: err.message,
        validation: { pass: false, failedChecks: failures },
      });
    }
    console.error("[partner-intelligence/publish]", err);
    return jsonError(res, 500, "publish_failed", err.message || "Publish failed.");
  }
}
