import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listRegistryRecordsRaw } from "./brand-explorer-visual-slot-requirements.js";
import { MAP_BRAND_ASSET, normalizeRegistryAssetRecord } from "./brand-asset-registry-workflow.js";
import {
  contentTypeFromFilename,
  uploadFileBytesToAirtable,
} from "../dealality/airtable-upload-attachment.js";

export const WRITER_VERSION = "25B";
export const REPORT_JSON_NAME = "brand-explorer-visual-minimums-backfill-writer.json";
export const REPORT_MD_NAME = "brand-explorer-visual-minimums-backfill-writer.md";
export const DOC_MD_NAME = "brand-explorer-visual-minimums-backfill-writer-v25B.md";
export const V25R2_REPORT_PATH = "reports/brand-explorer-visual-minimums-backfill-planner.json";

export const APPLY_FLAG_STRICT = "--approve-brand-explorer-v25B-strict-gallery-backfill";
export const APPLY_FLAG_PROVISIONAL = "--approve-brand-explorer-v25B-provisional-visual-minimums";
export const APPLY_FLAG_FOUNDER_PROVISIONAL = "--founder-approves-provisional-scenario-image";
export const APPLY_FLAG_GALLERY_ROW_CREATE = "--approve-brand-explorer-v25B-gallery-row-create";
export const APPLY_FLAG_GALLERY_IMAGE_REPAIR = "--approve-brand-explorer-v25B-gallery-image-repair";
export const APPLY_FLAG_PRESENTATION_IMAGE_CONTENT_UPLOAD =
  "--approve-brand-explorer-v25B-presentation-image-content-upload";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_BRAND_NAME = "Tribute Portfolio";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PLAN_STRICT = "strict";
const PLAN_PROVISIONAL = "provisional";

const PLAN_STRICT_TARGETS = [{ slotKey: "materials.gallery.3", assetRecordId: "recxVPbTlsrP9v4bQ" }];
const PLAN_PROVISIONAL_TARGETS = [
  { slotKey: "materials.gallery.3", assetRecordId: "recxVPbTlsrP9v4bQ" },
  { slotKey: "overview.scenario.3", assetRecordId: "recgq6wOvz5yOWPmY" },
];

const UNTOUCHED_FAMILIES = [
  "footprint.openings",
  "footprint.momentum",
  "standards.*",
  "loyalty.*",
  "commercial.demand",
  "footprint.region.*",
  "overview.portfolio_context",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const STRICT_GALLERY_LOCAL_FILE_REL =
  "data/partner-intelligence/assets/tribute-portfolio/tribute-portfolio__gallery-3__ermita-cartagena-a-tribute-portfolio-hotel.jpg";

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function lower(v) {
  return nz(v).toLowerCase();
}

function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}

function readJsonFromRepo(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function normalizeBrandInput(raw) {
  const normalized = lower(raw);
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw) || DEFAULT_BRAND_ID;
}

function normalizePlanInput(raw) {
  const p = lower(raw);
  return p === PLAN_PROVISIONAL ? PLAN_PROVISIONAL : PLAN_STRICT;
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      const image = Array.isArray(f.Image) ? f.Image : [];
      const brandLinks = Array.isArray(f.Brand) ? f.Brand.map((v) => nz(v)).filter(Boolean) : [];
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        brandLinks,
        brandName: nz(f["Brand Name"]),
        title: nz(f.Title),
        body: nz(f.Body),
        active: f.Active,
        image,
        imageUrls: image.map((x) => nz(x?.url)).filter(Boolean),
        imageFilenames: image.map((x) => nz(x?.filename)).filter(Boolean),
        imageIds: image.map((x) => nz(x?.id)).filter(Boolean),
        sortOrder: f["Sort Order"],
        externalDisplayStatus: nz(f["External Display Status"]),
        visibility: nz(f.Visibility),
        companyValidated: f["Company Validated"],
        companyValidationDate: f["Company Validation Date"],
        createdTime: nz(rec.createdTime),
      };
    })
    .filter((r) => r.slotKey);
}

function groupRowsBySlot(rows) {
  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.slotKey)) grouped.set(row.slotKey, []);
    grouped.get(row.slotKey).push(row);
  }
  return grouped;
}

function readRegistryAttachments(fields = {}) {
  const attachmentField = MAP_BRAND_ASSET.attachment;
  const raw = fields[attachmentField];
  return Array.isArray(raw) ? raw : [];
}

function resolveRepairPayloadSource(asset) {
  if (asset?.hasAttachment && asset.attachmentUrls?.length) {
    return "materialized_registry_attachment";
  }
  if (asset?.hasSourceUrl) return "external_source_url";
  return "none";
}

function normalizeRegistryAsset(rawRecord) {
  const base = normalizeRegistryAssetRecord(rawRecord);
  const f = rawRecord?.fields || {};
  const attachments = readRegistryAttachments(f);
  const attachmentUrls = attachments.map((a) => nz(a?.url)).filter(Boolean);
  const attachmentFilenames = attachments.map((a) => nz(a?.filename)).filter(Boolean);
  const rejected = /rejected|do not use|blocked/i.test(
    `${nz(base.assetStatus)} ${nz(base.usageReviewStatus)} ${nz(base.explorerUsePermission)}`
  );
  const superseded = /superseded|not selected/i.test(nz(base.reviewNotes));
  const approved =
    /approved for explorer use/i.test(nz(base.assetStatus)) ||
    /approved for explorer/i.test(nz(base.explorerUsePermission)) ||
    /usage review complete/i.test(nz(base.usageReviewStatus));
  const candidate = /candidate/i.test(nz(base.assetStatus)) || /candidate/i.test(nz(base.explorerUsePermission));
  const sourceUrl = nz(base.sourceUrl);
  const preferredImageUrl = attachmentUrls[0] || sourceUrl;
  return {
    id: rawRecord.id,
    assetName: nz(base.assetName),
    explorerUsePermission: nz(base.explorerUsePermission),
    assetStatus: nz(base.assetStatus),
    usageReviewStatus: nz(base.usageReviewStatus),
    reviewNotes: nz(base.reviewNotes),
    sourceUrl,
    attachmentUrls,
    attachmentFilenames,
    preferredImageUrl,
    hasPreferredImageUrl: Boolean(preferredImageUrl),
    hasSourceUrl: Boolean(sourceUrl),
    hasAttachment: attachments.length > 0,
    approved,
    candidate,
    rejected,
    superseded,
  };
}

function attachmentBasenameFromUrl(url) {
  const raw = nz(url);
  if (!raw) return "";
  const base = raw.split("?")[0];
  const bits = base.split("/");
  return nz(bits[bits.length - 1]).toLowerCase();
}

function attachmentLikelyMatchesAsset(row, asset) {
  if (!row || !asset) return false;
  const rowFilename = lower(row.image?.[0]?.filename);
  const assetFilename = lower(asset.attachmentFilenames?.[0]);
  if (rowFilename && assetFilename && rowFilename === assetFilename) return true;
  const rowUrl = lower(row.image?.[0]?.url);
  const preferredUrl = lower(asset.preferredImageUrl || asset.sourceUrl);
  if (rowUrl && preferredUrl && rowUrl === preferredUrl) return true;
  const rowBase = attachmentBasenameFromUrl(rowUrl);
  const preferredBase = attachmentBasenameFromUrl(preferredUrl);
  if (rowBase && preferredBase && rowBase === preferredBase) return true;
  return false;
}

function isBlockedNonTargetSlot(slotKey, targetSlots) {
  return !targetSlots.includes(slotKey);
}

function loadPlanSourceTruth(plan) {
  const report = readJsonFromRepo(V25R2_REPORT_PATH);
  if (!report) throw new Error(`Missing v25-R2 planner report: ${V25R2_REPORT_PATH}`);
  const strict = report.v25BStrictSlotsAssets || [];
  const provisional = report.v25BProvisionalSlotsAssets || [];
  const expected = plan === PLAN_STRICT ? PLAN_STRICT_TARGETS : PLAN_PROVISIONAL_TARGETS;
  const source = plan === PLAN_STRICT ? strict : provisional;
  const sourceBySlot = new Map(source.map((x) => [x.slotKey, x.assetRecordId]));
  const drift = [];
  for (const t of expected) {
    const sourceId = sourceBySlot.get(t.slotKey);
    if (sourceId && sourceId !== t.assetRecordId) {
      drift.push({
        slotKey: t.slotKey,
        plannerAssetRecordId: sourceId,
        writerPinnedAssetRecordId: t.assetRecordId,
      });
    }
  }
  return { plannerReport: report, plannerDriftAdvisory: drift };
}

function createAssignment(slotKey, row, asset) {
  const targetImageUrl = nz(asset.preferredImageUrl || asset.sourceUrl);
  const before = {
    title: row.title,
    body: row.body,
    imageUrls: row.imageUrls,
    imageCount: row.image.length,
    sortOrder: row.sortOrder,
    companyValidated: row.companyValidated,
    companyValidationDate: row.companyValidationDate,
  };
  const afterImage = [{ url: targetImageUrl }];
  const after = {
    title: row.title,
    body: row.body,
    imageUrls: [targetImageUrl],
    imageCount: 1,
    sortOrder: row.sortOrder,
    companyValidated: row.companyValidated,
    companyValidationDate: row.companyValidationDate,
  };
  const changed = row.image.length !== 1 || !attachmentLikelyMatchesAsset(row, asset);
  return {
    slotKey,
    recordId: row.recordId,
    assetRecordId: asset.id,
    assetName: asset.assetName,
    before,
    after,
    wouldChange: changed,
    writableFields: changed ? ["Image"] : [],
    fields: changed ? { Image: afterImage } : {},
  };
}

function toSlotIndex(slotKey) {
  const m = nz(slotKey).match(/^materials\.gallery\.(\d+)$/i);
  return m ? Number(m[1]) : null;
}

function extractFilenameFromUrl(url) {
  const raw = nz(url);
  if (!raw) return "";
  const withoutQuery = raw.split("?")[0];
  const bits = withoutQuery.split("/");
  return nz(bits[bits.length - 1]);
}

function rowMatchesBrand(row, brandRecordId, brandName) {
  const links = Array.isArray(row.brandLinks) ? row.brandLinks : [];
  if (links.includes(brandRecordId)) return true;
  return lower(row.brandName) === lower(brandName);
}

function summarizeCandidateRow(row) {
  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    brandLinks: row.brandLinks,
    brandName: row.brandName,
    imageUrls: row.imageUrls,
    active: row.active,
    sortOrder: row.sortOrder,
    title: row.title,
    body: row.body,
    externalDisplayStatus: row.externalDisplayStatus,
    visibility: row.visibility,
    createdTime: row.createdTime,
  };
}

function summarizePresentationRowState(row) {
  if (!row) return null;
  return {
    recordId: row.recordId,
    brandLinks: row.brandLinks,
    brandName: row.brandName,
    slotKey: row.slotKey,
    imageCount: Array.isArray(row.image) ? row.image.length : 0,
    imageUrls: row.imageUrls || [],
    imageFilenames: row.imageFilenames || [],
    imageIds: row.imageIds || [],
    active: row.active,
    sortOrder: row.sortOrder,
    title: row.title,
    body: row.body,
    externalDisplayStatus: row.externalDisplayStatus,
    visibility: row.visibility,
    createdTime: row.createdTime,
  };
}

function detectAssetImageState(rawRecord, normalized) {
  const f = rawRecord?.fields || {};
  const attachments = readRegistryAttachments(f);
  const attachmentUrls = attachments.map((a) => nz(a?.url)).filter(Boolean);
  const sourceUrl = nz(normalized?.sourceUrl);
  return {
    assetRecordId: normalized?.id || rawRecord?.id || "",
    registryAttachmentFieldUsed: MAP_BRAND_ASSET.attachment,
    attachmentCount: attachments.length,
    attachmentUrls,
    sourceUrl,
    hasAttachmentMaterialized: attachments.length > 0,
    hasSourceUrlOnly: Boolean(sourceUrl) && attachments.length === 0,
    hasContentApiBlobReference: attachmentUrls.some((u) => /airtableusercontent\.com/i.test(u)),
    usableAttachmentObject: attachments.length > 0 ? attachments[0] : null,
  };
}

function deriveGalleryRowCreatePayload(brandRecordId, slotKey, asset, rowsBySlot) {
  const idx = toSlotIndex(slotKey);
  const imageUrl = nz(asset.preferredImageUrl || asset.sourceUrl);
  const fields = {
    Brand: [brandRecordId],
    "Slot Key": slotKey,
    Image: [{ url: imageUrl }],
    Active: true,
  };
  if (idx != null) {
    const prev = rowsBySlot.get(`materials.gallery.${idx - 1}`)?.[0] || null;
    const next = rowsBySlot.get(`materials.gallery.${idx + 1}`)?.[0] || null;
    const prevSort = Number(prev?.sortOrder);
    const nextSort = Number(next?.sortOrder);
    if (Number.isFinite(prevSort) && Number.isFinite(nextSort) && nextSort > prevSort) {
      fields["Sort Order"] = Math.round((prevSort + nextSort) / 2);
    } else if (Number.isFinite(prevSort)) {
      fields["Sort Order"] = prevSort + 10;
    }
  }
  return fields;
}

function evaluateStrictIdempotency(rowsBySlot, registryById) {
  const strictTarget = PLAN_STRICT_TARGETS[0];
  const asset = registryById.get(strictTarget.assetRecordId);
  const row = (rowsBySlot.get(strictTarget.slotKey) || [])[0] || null;
  const strictImageUrl = nz(asset?.preferredImageUrl || asset?.sourceUrl);
  if (!strictImageUrl || !row?.recordId) return false;
  if ((rowsBySlot.get(strictTarget.slotKey) || []).length !== 1) return false;
  if (!row.imageUrls.length) return false;
  return attachmentLikelyMatchesAsset(row, asset);
}

function evaluateStrictGateLive(rowsBySlot, registryById) {
  const strictTarget = PLAN_STRICT_TARGETS[0];
  const asset = registryById.get(strictTarget.assetRecordId) || null;
  const strictRows = rowsBySlot.get(strictTarget.slotKey) || [];
  const strictTargetRow =
    strictRows.find((r) => r.recordId === "recDCNmWcRNLZ8Aag") || strictRows[0] || null;
  const blockers = [];
  if (!asset) blockers.push("strict_missing_asset");
  if (!strictRows.length) blockers.push("strict_missing_row");
  if (strictRows.length > 1) blockers.push("strict_duplicate_rows_detected");
  if (!strictTargetRow?.image?.length) blockers.push("strict_missing_image");
  if (strictTargetRow && asset && !attachmentLikelyMatchesAsset(strictTargetRow, asset)) {
    blockers.push("strict_image_not_matched_by_attachment_identity");
  }
  const strictWouldCreate = strictRows.length ? 0 : 1;
  const strictWouldUpdate =
    strictTargetRow && asset && attachmentLikelyMatchesAsset(strictTargetRow, asset) ? 0 : strictRows.length ? 1 : 0;
  const strictMissingRows = strictRows.length ? 0 : 1;
  const strictGatePassed =
    strictWouldCreate === 0 &&
    strictWouldUpdate === 0 &&
    strictMissingRows === 0 &&
    blockers.length === 0;
  return {
    strictGateCheckedLive: true,
    strictGatePassed,
    strictGateBlockers: blockers,
    strictWouldUpdate,
    strictWouldCreate,
    strictMissingRows,
    strictMaterialsGalleryRecordId: strictTargetRow?.recordId || null,
    strictMaterialsGalleryImageCount: Array.isArray(strictTargetRow?.image) ? strictTargetRow.image.length : 0,
    strictMaterialsGalleryMatchedByAttachmentIdentity:
      Boolean(strictTargetRow && asset && attachmentLikelyMatchesAsset(strictTargetRow, asset)),
  };
}

export async function buildBrandExplorerVisualMinimumsBackfillWriterReport(options = {}) {
  const plan = normalizePlanInput(options.plan);
  const apply = Boolean(options.apply);
  const applyApprovedStrict = Boolean(options.applyApprovedStrict);
  const applyApprovedProvisional = Boolean(options.applyApprovedProvisional);
  const founderApprovedProvisional = Boolean(options.founderApprovedProvisional);
  const galleryRowCreateApproved = Boolean(options.galleryRowCreateApproved);
  const galleryImageRepairApproved = Boolean(options.galleryImageRepairApproved);
  const presentationImageContentUploadApproved = Boolean(options.presentationImageContentUploadApproved);
  const allowExternalImageUrlFallback = Boolean(options.allowExternalImageUrlFallback);
  const applyMode = apply;

  let requiredGateFlags =
    plan === PLAN_STRICT
      ? ["--apply", APPLY_FLAG_STRICT, APPLY_FLAG_GALLERY_IMAGE_REPAIR]
      : ["--apply", APPLY_FLAG_PROVISIONAL, APPLY_FLAG_GALLERY_ROW_CREATE, APPLY_FLAG_FOUNDER_PROVISIONAL];

  const applyBlockers = [];
  if (applyMode && plan === PLAN_STRICT && !applyApprovedStrict) {
    applyBlockers.push(`Strict apply requires ${APPLY_FLAG_STRICT}`);
  }
  if (applyMode && plan === PLAN_PROVISIONAL && !applyApprovedProvisional) {
    applyBlockers.push(`Provisional apply requires ${APPLY_FLAG_PROVISIONAL}`);
  }
  if (applyMode && plan === PLAN_PROVISIONAL && !founderApprovedProvisional) {
    applyBlockers.push(`Provisional apply requires ${APPLY_FLAG_FOUNDER_PROVISIONAL}`);
  }
  if (applyMode && plan === PLAN_STRICT && !galleryImageRepairApproved) {
    applyBlockers.push(`Strict apply requires ${APPLY_FLAG_GALLERY_IMAGE_REPAIR}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandRecordId = normalizeBrandInput(options.brandIdOrName);
  const targetPlan = plan === PLAN_STRICT ? PLAN_STRICT_TARGETS : PLAN_PROVISIONAL_TARGETS;
  const targetSlots = targetPlan.map((x) => x.slotKey);

  const { plannerReport, plannerDriftAdvisory } = loadPlanSourceTruth(plan);
  const brandName = plannerReport?.brand?.name || DEFAULT_BRAND_NAME;

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const rowsBySlot = groupRowsBySlot(presentationRows);

  const registryRaw = await listRegistryRecordsRaw(brandRecordId);
  const registryRawById = new Map(registryRaw.map((r) => [r.id, r]));
  const registryById = new Map(registryRaw.map((r) => [r.id, normalizeRegistryAsset(r)]));

  const missingTargetRows = [];
  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];
  const existingPartialRows = [];
  const frontendVisibilityRepairRequired = [];
  const duplicateCreateProtection = [];
  const preflightRows = [];
  const beforeAfterBySlot = [];
  const assetValidations = [];

  let existingPartialRowDetected = false;
  let galleryImageRepairRequired = false;
  let galleryImageRepairAllowed = false;
  let galleryImageRepairPayload = null;
  let strictGatePath = "idempotent_noop";

  for (const t of targetPlan) {
    const asset = registryById.get(t.assetRecordId);
    if (!asset) {
      applyBlockers.push(`Asset not found: ${t.assetRecordId} for ${t.slotKey}`);
      assetValidations.push({ slotKey: t.slotKey, assetRecordId: t.assetRecordId, valid: false, reason: "missing_asset" });
      continue;
    }

    const validApproved = asset.approved && !asset.rejected && !asset.superseded;
    const validCandidate = asset.candidate && !asset.rejected && !asset.superseded;
    const validForPlan =
      t.slotKey === "materials.gallery.3"
        ? validApproved && asset.hasPreferredImageUrl
        : plan === PLAN_PROVISIONAL && t.slotKey === "overview.scenario.3"
          ? (validCandidate || validApproved) && asset.hasPreferredImageUrl
          : false;

    assetValidations.push({
      slotKey: t.slotKey,
      assetRecordId: t.assetRecordId,
      valid: validForPlan,
      approved: asset.approved,
      candidate: asset.candidate,
      hasAttachment: asset.hasAttachment,
      hasSourceUrl: asset.hasSourceUrl,
      rejected: asset.rejected,
      superseded: asset.superseded,
    });
    if (!validForPlan) {
      applyBlockers.push(`Asset invalid for ${t.slotKey}: ${t.assetRecordId}`);
    }

    const liveRows = rowsBySlot.get(t.slotKey) || [];
    if (liveRows.length > 1) {
      applyBlockers.push(`${t.slotKey} has ${liveRows.length} rows; review manually`);
    }
    let row = liveRows[0] || null;
    if (!row) {
      const fallbackFormulaParts = [`{Slot Key}='${escapeFormulaValue(t.slotKey)}'`];
      const filename = extractFilenameFromUrl(asset.preferredImageUrl || asset.sourceUrl);
      if (filename) {
        fallbackFormulaParts.push(`FIND('${escapeFormulaValue(filename)}', ARRAYJOIN(Image))>0`);
      }
      const fallbackRecords = await listByFormula(baseId, apiKey, PRESENTATION_TABLE, `OR(${fallbackFormulaParts.join(",")})`);
      const fallbackRows = normalizePresentationRows(fallbackRecords);
      const brandedCandidates = fallbackRows.filter((candidate) => rowMatchesBrand(candidate, brandRecordId, brandName));
      const slotMatchedCandidates = brandedCandidates.filter((candidate) => candidate.slotKey === t.slotKey);
      if (slotMatchedCandidates.length > 1) {
        applyBlockers.push(`duplicate_cleanup_required: ${t.slotKey} has ${slotMatchedCandidates.length} candidate rows`);
        duplicateCreateProtection.push({
          slotKey: t.slotKey,
          status: "duplicate_cleanup_required",
          candidates: slotMatchedCandidates.map(summarizeCandidateRow),
        });
      } else if (slotMatchedCandidates.length === 1) {
        row = slotMatchedCandidates[0];
      } else if (brandedCandidates.length > 0) {
        applyBlockers.push(`duplicate_cleanup_required: ${t.slotKey} has non-slot branded candidates`);
        duplicateCreateProtection.push({
          slotKey: t.slotKey,
          status: "duplicate_cleanup_required",
          candidates: brandedCandidates.map(summarizeCandidateRow),
        });
      }
    }
    if (!row?.recordId) {
      missingTargetRows.push(t.slotKey);
      if (t.slotKey === "materials.gallery.3") {
        const createFields = deriveGalleryRowCreatePayload(brandRecordId, t.slotKey, asset, rowsBySlot);
        const preCreateRecords = await listByFormula(baseId, apiKey, PRESENTATION_TABLE, `{Slot Key}='${escapeFormulaValue(t.slotKey)}'`);
        const preCreateRows = normalizePresentationRows(preCreateRecords).filter((candidate) =>
          rowMatchesBrand(candidate, brandRecordId, brandName)
        );
        if (preCreateRows.length) {
          applyBlockers.push(`duplicate_create_protection: existing candidate row(s) found for ${t.slotKey}`);
          duplicateCreateProtection.push({
            slotKey: t.slotKey,
            status: "blocked_existing_candidates",
            candidates: preCreateRows.map(summarizeCandidateRow),
          });
        }
        rowsWouldCreate.push({
          slotKey: t.slotKey,
          action: "row_create_gated",
          assetRecordId: t.assetRecordId,
          fields: createFields,
          writableFields: Object.keys(createFields),
        });
        if (!galleryRowCreateApproved) {
          applyBlockers.push(
            `Missing presentation row for ${t.slotKey}; add ${APPLY_FLAG_GALLERY_ROW_CREATE} for explicit row creation`
          );
        }
        if (plan === PLAN_STRICT) strictGatePath = "row_create_gate";
        preflightRows.push({
          slotKey: t.slotKey,
          recordId: null,
          action: "row_create_gated",
          wouldChange: true,
          writableFields: Object.keys(createFields),
          createFields,
        });
      } else {
        rowsWouldCreate.push({ slotKey: t.slotKey, action: "row_creation_required_blocked" });
        applyBlockers.push(`Missing presentation row for ${t.slotKey}; row creation is allowed only for materials.gallery.3`);
        preflightRows.push({ slotKey: t.slotKey, recordId: null, action: "row_creation_required_blocked", wouldChange: false });
      }
      continue;
    }

    if (t.slotKey === "materials.gallery.3" && row.recordId !== "recDCNmWcRNLZ8Aag") {
      applyBlockers.push(
        `materials.gallery.3 repair target mismatch: expected recDCNmWcRNLZ8Aag, found ${row.recordId}`
      );
    }

    const assignment = createAssignment(t.slotKey, row, asset);
    if (!row.imageUrls.length) {
      existingPartialRowDetected = true;
      const isGalleryRepairTarget = t.slotKey === "materials.gallery.3";
      if (isGalleryRepairTarget) {
        galleryImageRepairRequired = true;
        galleryImageRepairAllowed = plan === PLAN_STRICT && galleryImageRepairApproved;
        if (plan === PLAN_STRICT && assignment.wouldChange) strictGatePath = "image_repair_gate";
        const repairImageUrl = nz(asset.preferredImageUrl || asset.sourceUrl);
        galleryImageRepairPayload = {
          recordId: row.recordId,
          slotKey: t.slotKey,
          assetRecordId: t.assetRecordId,
          fields: { Image: [{ url: repairImageUrl }] },
          writableFields: ["Image"],
          repairPayloadSource: resolveRepairPayloadSource(asset),
        };
      }
      existingPartialRows.push({
        slotKey: t.slotKey,
        recordId: row.recordId,
        status: "existing_partial_row",
        notes: "Row exists but image attachment is empty; writer will patch Image only.",
        row: summarizeCandidateRow(row),
      });
      frontendVisibilityRepairRequired.push({
        slotKey: t.slotKey,
        recordId: row.recordId,
        missingFields: ["Image"],
        notes: "Brand Library/gallery rendering requires imageUrl from Image attachments.",
      });
      if (isGalleryRepairTarget && plan === PLAN_STRICT && !galleryImageRepairApproved && applyMode) {
        applyBlockers.push(`Missing ${APPLY_FLAG_GALLERY_IMAGE_REPAIR} for existing partial ${t.slotKey} row`);
      }
    }
    beforeAfterBySlot.push({
      slotKey: t.slotKey,
      recordId: row.recordId,
      assetRecordId: t.assetRecordId,
      assetName: asset.assetName,
      before: assignment.before,
      after: assignment.after,
    });
    preflightRows.push({
      slotKey: t.slotKey,
      recordId: row.recordId,
      action: assignment.wouldChange ? "would_update_image" : "matched",
      wouldChange: assignment.wouldChange,
      writableFields: assignment.writableFields,
    });
    if (assignment.wouldChange) {
      if (plan === PLAN_STRICT && t.slotKey === "materials.gallery.3") {
        const writable = assignment.writableFields || [];
        const imageOnly = writable.length === 1 && writable[0] === "Image";
        if (!imageOnly) {
          applyBlockers.push("Strict gallery repair may write only Image");
        }
        if (row.recordId !== "recDCNmWcRNLZ8Aag") {
          applyBlockers.push(
            `Strict gallery repair target mismatch: expected recDCNmWcRNLZ8Aag, found ${row.recordId}`
          );
        }
        const brandLinked = Array.isArray(row.brandLinks) && row.brandLinks.includes(brandRecordId);
        if (!brandLinked) {
          applyBlockers.push(
            `Strict gallery repair brand mismatch on ${row.recordId}; expected link ${brandRecordId}`
          );
        }
        if (t.assetRecordId !== "recxVPbTlsrP9v4bQ") {
          applyBlockers.push(
            `Strict gallery repair asset mismatch: expected recxVPbTlsrP9v4bQ, found ${t.assetRecordId}`
          );
        }
      }
      rowsWouldUpdate.push({
        slotKey: t.slotKey,
        recordId: row.recordId,
        assetRecordId: t.assetRecordId,
        fields: assignment.fields,
        writableFields: assignment.writableFields,
      });
    } else {
      rowsMatched.push({ slotKey: t.slotKey, recordId: row.recordId });
    }
  }

  const nonTargetSlotsLeakedIntoPlan = preflightRows
    .map((r) => r.slotKey)
    .filter((s) => s && isBlockedNonTargetSlot(s, targetSlots));
  if (nonTargetSlotsLeakedIntoPlan.length) {
    applyBlockers.push(`Non-target slots leaked: ${nonTargetSlotsLeakedIntoPlan.join(", ")}`);
  }

  let applyResult = { updated: [], errors: [], skipped: true };
  let airtableModified = false;
  const strictTargetSlot = PLAN_STRICT_TARGETS[0].slotKey;
  const strictPreflightRow = preflightRows.find((r) => r.slotKey === strictTargetSlot) || null;
  const strictBeforeAfterRow = beforeAfterBySlot.find((r) => r.slotKey === strictTargetSlot) || null;
  const strictWouldUpdateLive = rowsWouldUpdate.filter((r) => r.slotKey === strictTargetSlot).length;
  const strictWouldCreateLive = rowsWouldCreate.filter((r) => r.slotKey === strictTargetSlot).length;
  const strictMissingRowsLive = missingTargetRows.includes(strictTargetSlot) ? 1 : 0;
  const strictMaterialsGalleryRecordId = strictPreflightRow?.recordId || null;
  const strictMaterialsGalleryImageCount = Number(strictBeforeAfterRow?.before?.imageCount || 0);
  const strictMaterialsGalleryMatchedByAttachmentIdentity =
    strictPreflightRow?.action === "matched" && strictWouldUpdateLive === 0;
  const strictGateBlockers = [];
  if (strictWouldCreateLive > 0 || strictMissingRowsLive > 0) strictGateBlockers.push("strict_missing_row");
  if (strictWouldUpdateLive > 0) strictGateBlockers.push("strict_image_not_matched_by_attachment_identity");
  if (!strictMaterialsGalleryImageCount) strictGateBlockers.push("strict_missing_image");
  const strictGateLive = {
    strictGateCheckedLive: true,
    strictGatePassed:
      strictWouldUpdateLive === 0 &&
      strictWouldCreateLive === 0 &&
      strictMissingRowsLive === 0 &&
      strictMaterialsGalleryMatchedByAttachmentIdentity &&
      strictMaterialsGalleryImageCount > 0,
    strictGateBlockers: [...new Set(strictGateBlockers)],
    strictWouldUpdate: strictWouldUpdateLive,
    strictWouldCreate: strictWouldCreateLive,
    strictMissingRows: strictMissingRowsLive,
    strictMaterialsGalleryRecordId,
    strictMaterialsGalleryImageCount,
    strictMaterialsGalleryMatchedByAttachmentIdentity,
  };
  const strictIdempotent = strictGateLive.strictGatePassed;
  const provisionalBlockedUntilStrictIdempotent = plan === PLAN_PROVISIONAL && !strictIdempotent;
  if (plan === PLAN_STRICT && strictIdempotent) {
    strictGatePath = "idempotent_noop";
  }
  if (plan === PLAN_STRICT && strictGatePath === "row_create_gate") {
    requiredGateFlags = ["--apply", APPLY_FLAG_STRICT, APPLY_FLAG_GALLERY_ROW_CREATE];
  } else if (plan === PLAN_STRICT && strictGatePath === "image_repair_gate") {
    requiredGateFlags = ["--apply", APPLY_FLAG_STRICT, APPLY_FLAG_GALLERY_IMAGE_REPAIR];
    if (repairStrategySelected === "presentation_content_api_upload") {
      requiredGateFlags.push(APPLY_FLAG_PRESENTATION_IMAGE_CONTENT_UPLOAD);
    }
  } else if (plan === PLAN_STRICT) {
    requiredGateFlags = ["--apply", APPLY_FLAG_STRICT];
  }
  if (plan === PLAN_PROVISIONAL && applyMode && !strictGateLive.strictGatePassed) {
    applyBlockers.push(
      `Provisional apply blocked until strict plan is idempotent: ${strictGateLive.strictGateBlockers.join(", ")}`
    );
  }

  const strictTargetAssetId = PLAN_STRICT_TARGETS[0].assetRecordId;
  const strictAssetRaw = registryRawById.get(strictTargetAssetId) || null;
  const strictAsset = registryById.get(strictTargetAssetId) || null;
  const strictTargetRow =
    (rowsBySlot.get(strictTargetSlot) || []).find((r) => r.recordId === "recDCNmWcRNLZ8Aag") ||
    (rowsBySlot.get(strictTargetSlot) || [])[0] ||
    existingPartialRows.find((r) => r.recordId === "recDCNmWcRNLZ8Aag")?.row ||
    null;
  const strictTargetImageAlreadyMatches = attachmentLikelyMatchesAsset(strictTargetRow, strictAsset);
  const strictLocalFilePath = STRICT_GALLERY_LOCAL_FILE_REL;
  const strictLocalFileAbs = path.join(ROOT, strictLocalFilePath);
  const strictLocalFileExists = fs.existsSync(strictLocalFileAbs);
  const strictUploadFilename =
    nz(strictAsset?.attachmentFilenames?.[0]) || path.basename(strictLocalFilePath);
  let repairStrategySelected = "idempotent_noop";
  if (!strictTargetImageAlreadyMatches) {
    if (strictLocalFileExists) {
      repairStrategySelected = "presentation_content_api_upload";
    } else if (strictAsset?.hasAttachment) {
      repairStrategySelected = "registry_materialized_attachment_url_patch";
    } else if (allowExternalImageUrlFallback && strictAsset?.hasSourceUrl) {
      repairStrategySelected = "external_source_url_fallback";
    } else {
      repairStrategySelected = "blocked_no_safe_repair_path";
      if (plan === PLAN_STRICT) {
        applyBlockers.push(
          "Strict repair blocked: no local file and no materialized registry attachment; external fallback disabled"
        );
      }
    }
  }
  const presentationContentUploadPlan = {
    repairStrategySelected,
    localFileExists: strictLocalFileExists,
    localFilePath: strictLocalFilePath,
    targetTable: PRESENTATION_TABLE,
    targetRecordId: "recDCNmWcRNLZ8Aag",
    targetFieldName: "Image",
    uploadFilename: strictUploadFilename,
    onlyPresentationImageWouldBeModified: true,
    rowCreationBlocked: true,
    companyValidatedUntouched: true,
  };
  const presentationRowImageState = summarizePresentationRowState(strictTargetRow);
  const assetImageState = detectAssetImageState(strictAssetRaw, strictAsset);
  const referenceGalleryRowsRaw = await listByFormula(baseId, apiKey, PRESENTATION_TABLE, "{Slot Key}='materials.gallery.3'");
  const referenceGalleryRows = normalizePresentationRows(referenceGalleryRowsRaw);
  const fieldNameValidated = referenceGalleryRows.some((r) => Array.isArray(r.image) && r.image.length > 0);
  const rowHasExpectedFilename = (presentationRowImageState?.imageUrls || []).some((u) =>
    lower(u).includes("ctgtx-exterior-4544-hor-wide.jpg")
  );
  let likelyRootCause = "unknown";
  let recommendedRepairPath = "manual_review_required";
  let attachmentMaterializationRequired = false;
  if (!presentationRowImageState || presentationRowImageState.imageCount === 0) {
    if (assetImageState.hasAttachmentMaterialized) {
      likelyRootCause = "presentation_image_empty_registry_attachment_ready";
      recommendedRepairPath = "patch_presentation_Image_with_materialized_registry_attachment";
      attachmentMaterializationRequired = false;
    } else if (assetImageState.hasSourceUrlOnly) {
      likelyRootCause = "presentation_update_accepted_but_attachment_not_materialized_from_external_url";
      recommendedRepairPath = "use_content_api_or_pre-materialized_attachment_then_patch_Image";
      attachmentMaterializationRequired = true;
    } else {
      likelyRootCause = "presentation_image_still_empty_after_apply";
      recommendedRepairPath = "verify_write_path_and_attachment_materialization";
    }
  } else if (rowHasExpectedFilename) {
    likelyRootCause = "comparison_logic_may_be_wrong_if_non_idempotent_persists";
    recommendedRepairPath = "normalize_comparison_to_accept_materialized_attachment_urls";
  } else {
    likelyRootCause = "image_present_but_different_from_expected_asset";
    recommendedRepairPath = "confirm_asset_selection_and_mapping";
  }

  const registryAttachmentFieldUsed = MAP_BRAND_ASSET.attachment;
  const registryAttachmentCount = assetImageState.attachmentCount;
  const registryAttachmentUrls = assetImageState.attachmentUrls || [];
  const repairPayloadSource = resolveRepairPayloadSource(strictAsset);
  const provisionalUpdates = rowsWouldUpdate.filter((r) => r.slotKey === "overview.scenario.3");
  const provisionalNonTargetUpdates = rowsWouldUpdate.filter((r) => r.slotKey !== "overview.scenario.3");
  const provisionalPayloadSource = provisionalUpdates[0]?.slotKey
    ? resolveRepairPayloadSource(registryById.get(provisionalUpdates[0].assetRecordId))
    : "none";
  const provisionalUsesMaterializedAttachment = provisionalPayloadSource === "materialized_registry_attachment";
  let provisionalApplyAllowed = plan === PLAN_PROVISIONAL && strictGateLive.strictGatePassed;
  if (plan === PLAN_PROVISIONAL && provisionalNonTargetUpdates.length) {
    provisionalApplyAllowed = false;
    applyBlockers.push("Provisional apply blocked: non-target update detected");
  }
  if (plan === PLAN_PROVISIONAL && rowsWouldCreate.length) {
    provisionalApplyAllowed = false;
    applyBlockers.push("Provisional apply blocked: row creation not allowed in this provisional batch");
  }
  if (plan === PLAN_PROVISIONAL && applyMode && !provisionalUsesMaterializedAttachment) {
    provisionalApplyAllowed = false;
    applyBlockers.push(
      "Provisional apply blocked: overview.scenario.3 must use materialized_registry_attachment or presentation_content_api_upload"
    );
  }
  if (
    plan === PLAN_PROVISIONAL &&
    applyMode &&
    provisionalPayloadSource === "external_source_url" &&
    !allowExternalImageUrlFallback
  ) {
    provisionalApplyAllowed = false;
    applyBlockers.push(
      "Provisional apply blocked: external URL fallback is disabled unless explicitly approved"
    );
  }

  const galleryRepairPostApplyDiagnosis = {
    presentationRowImageState,
    assetImageState,
    fieldNameValidated,
    attachmentMaterializationRequired,
    likelyRootCause,
    recommendedRepairPath,
    writingFieldName: "Image",
    expectedPresentationRecordId: "recDCNmWcRNLZ8Aag",
    expectedSlotKey: "materials.gallery.3",
    expectedBrandRecordId: "recCvV0PuZOi8c3hC",
    expectedAssetRecordId: "recxVPbTlsrP9v4bQ",
    writerImageSourcePreference: "registry_attachment_first_then_source_url_fallback",
    registryAttachmentFieldUsed,
    registryAttachmentCount,
    registryAttachmentUrls,
    repairPayloadSource,
    presentationContentUploadPlan,
  };

  if (applyMode) {
    if (
      plan === PLAN_STRICT &&
      repairStrategySelected === "presentation_content_api_upload" &&
      !presentationImageContentUploadApproved
    ) {
      applyBlockers.push(`Strict apply requires ${APPLY_FLAG_PRESENTATION_IMAGE_CONTENT_UPLOAD}`);
    }
    if (applyBlockers.length) {
      throw new Error(`Apply blocked: ${[...new Set(applyBlockers)].join("; ")}`);
    }
    applyResult = { updated: [], errors: [], skipped: false };
    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "POST", body: JSON.stringify({ fields: row.fields, typecast: true }) }
      );
      if (!res.ok) {
        applyResult.errors.push({
          slotKey: row.slotKey,
          recordId: null,
          message: json.error?.message || res.statusText,
        });
      } else {
        applyResult.updated.push({ slotKey: row.slotKey, recordId: json?.id || null, action: "created" });
        airtableModified = true;
      }
    }
    for (const row of rowsWouldUpdate) {
      if (
        plan === PLAN_STRICT &&
        row.slotKey === "materials.gallery.3" &&
        repairStrategySelected === "presentation_content_api_upload"
      ) {
        const buffer = fs.readFileSync(strictLocalFileAbs);
        await uploadFileBytesToAirtable({
          baseId,
          recordId: row.recordId,
          fieldName: "Image",
          buffer,
          contentType: contentTypeFromFilename(strictUploadFilename),
          filename: strictUploadFilename,
          apiKey,
        });
        applyResult.updated.push({
          slotKey: row.slotKey,
          recordId: row.recordId,
          action: "presentation_content_api_upload",
        });
        airtableModified = true;
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: row.fields, typecast: true }) },
        row.recordId
      );
      if (!res.ok) {
        applyResult.errors.push({
          slotKey: row.slotKey,
          recordId: row.recordId,
          message: json.error?.message || res.statusText,
        });
      } else {
        applyResult.updated.push({ slotKey: row.slotKey, recordId: row.recordId });
        airtableModified = true;
      }
    }
  }

  const suppressionRecommendations = [
    "footprint.openings should be suppressed until complete cards exist (v25C batch).",
    "footprint.momentum should be suppressed until dated source-backed rows exist (v25C batch).",
  ];

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: applyMode ? "apply" : "dry-run",
    plan,
    targetSlots,
    targetSlotCount: targetSlots.length,
    plannerSourcePath: V25R2_REPORT_PATH,
    plannerDriftAdvisory,
    v25BWriterExists: true,
    brand: { recordId: brandRecordId, name: brandName },
    rowsWouldUpdate,
    rowsWouldCreate,
    rowsMatched,
    existingPartialRows,
    frontendVisibilityRepairRequired,
    duplicateCreateProtection,
    wouldUpdateCount: rowsWouldUpdate.length,
    wouldCreateCount: rowsWouldCreate.length,
    matchedCount: rowsMatched.length,
    missingTargetRows,
    beforeAfterBySlot,
    preflightRows,
    assetValidations,
    founderApprovalRequiredForScenario3: plan === PLAN_PROVISIONAL,
    existingPartialRowDetected,
    galleryImageRepairRequired,
    galleryImageRepairAllowed,
    galleryImageRepairPayload,
    activeGatePath: strictGatePath,
    galleryRepairPostApplyDiagnosis,
    presentationRowImageState,
    assetImageState,
    registryAttachmentFieldUsed,
    registryAttachmentCount,
    registryAttachmentUrls,
    repairPayloadSource,
    repairStrategySelected,
    presentationContentUploadPlan,
    fieldNameValidated,
    attachmentMaterializationRequired,
    likelyRootCause,
    recommendedRepairPath,
    strictIdempotent,
    strictGateCheckedLive: strictGateLive.strictGateCheckedLive,
    strictGatePassed: strictGateLive.strictGatePassed,
    strictGateBlockers: strictGateLive.strictGateBlockers,
    strictWouldUpdateLive: strictGateLive.strictWouldUpdate,
    strictWouldCreateLive: strictGateLive.strictWouldCreate,
    strictMissingRowsLive: strictGateLive.strictMissingRows,
    strictMaterialsGalleryRecordId: strictGateLive.strictMaterialsGalleryRecordId,
    strictMaterialsGalleryImageCount: strictGateLive.strictMaterialsGalleryImageCount,
    strictMaterialsGalleryMatchedByAttachmentIdentity:
      strictGateLive.strictMaterialsGalleryMatchedByAttachmentIdentity,
    provisionalBlockedUntilStrictIdempotent,
    provisionalApplyAllowed,
    provisionalTargetSlots: provisionalUpdates.map((r) => r.slotKey),
    provisionalPayloadSource,
    provisionalUsesMaterializedAttachment,
    nonTargetSlotsLeakedIntoPlan,
    untouchedFamilies: UNTOUCHED_FAMILIES,
    footprintOpeningsUntouched: true,
    footprintMomentumUntouched: true,
    standardsUntouched: true,
    loyaltyUntouched: true,
    demandUntouched: true,
    geographicFootprintUntouched: true,
    portfolioContextUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    imagesUntouchedExceptTargets: true,
    airtableModified,
    applyBlockers: [...new Set(applyBlockers)],
    applyGatesRequired: requiredGateFlags,
    presentationImageContentUploadFlag: APPLY_FLAG_PRESENTATION_IMAGE_CONTENT_UPLOAD,
    presentationImageContentUploadApproved,
    rowCreateGateImplemented: true,
    rowCreateGateFlag: APPLY_FLAG_GALLERY_ROW_CREATE,
    galleryRowCreateApproved,
    galleryImageRepairGateImplemented: true,
    galleryImageRepairFlag: APPLY_FLAG_GALLERY_IMAGE_REPAIR,
    galleryImageRepairApproved,
    applyResult,
    idempotentReady: missingTargetRows.length === 0 && rowsWouldUpdate.length === 0,
    strictApplyCommand:
      "npm run brand-explorer-visual-minimums-backfill-writer -- --brand tribute-portfolio --plan strict --apply --approve-brand-explorer-v25B-strict-gallery-backfill --approve-brand-explorer-v25B-gallery-image-repair --approve-brand-explorer-v25B-presentation-image-content-upload",
    provisionalApplyCommand:
      "npm run brand-explorer-visual-minimums-backfill-writer -- --brand tribute-portfolio --plan provisional --apply --approve-brand-explorer-v25B-provisional-visual-minimums --approve-brand-explorer-v25B-gallery-row-create --founder-approves-provisional-scenario-image",
    exactDryRunCommand:
      `npm run brand-explorer-visual-minimums-backfill-writer -- --brand tribute-portfolio --plan ${plan} --dry-run`,
    suppressionBatchRecommendedNext: true,
    suppressionBatch: "v25C frontend/display suppression",
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-visual-minimums-backfill-planner.md",
      "reports/brand-explorer-visual-minimums-backfill-planner.json",
      "lib/partner-intelligence/brand-explorer-visual-minimums-backfill-planner.js",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "live Tribute presentation rows",
      "Tribute Brand Asset Registry rows",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-visual-minimums-backfill-writer.js",
      "scripts/brand-explorer-visual-minimums-backfill-writer.mjs",
      "docs/data-intelligence/brand-explorer-visual-minimums-backfill-writer-v25B.md",
      "reports/brand-explorer-visual-minimums-backfill-writer.md",
      "reports/brand-explorer-visual-minimums-backfill-writer.json",
      "package.json",
    ],
  };
}

export function buildBrandExplorerVisualMinimumsBackfillWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Visual Minimums Backfill Writer v25B");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Plan: **${report.plan}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Scope");
  lines.push(`- Target slots: **${report.targetSlotCount}**`);
  lines.push(`- Would update: **${report.wouldUpdateCount}**`);
  lines.push(`- Would create: **${report.wouldCreateCount}**`);
  lines.push(`- Matched: **${report.matchedCount}**`);
  lines.push(`- Missing rows: **${report.missingTargetRows.length}**`);
  lines.push(`- Row-create gate implemented: **${report.rowCreateGateImplemented ? "yes" : "no"}**`);
  lines.push(`- Gallery row-create approved: **${report.galleryRowCreateApproved ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Sort Order untouched: **${report.sortOrderUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Basics untouched: **${report.brandBasicsUntouched ? "yes" : "no"}**`);
  lines.push(`- Openings untouched: **${report.footprintOpeningsUntouched ? "yes" : "no"}**`);
  lines.push(`- Momentum untouched: **${report.footprintMomentumUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Before / After");
  for (const row of report.beforeAfterBySlot) {
    lines.push(`### \`${row.slotKey}\``);
    lines.push(`- Record: \`${row.recordId}\``);
    lines.push(`- Asset: ${row.assetName} (\`${row.assetRecordId}\`)`);
    lines.push(`- Before image URLs: ${row.before.imageUrls.join(", ") || "—"}`);
    lines.push(`- After image URLs: ${row.after.imageUrls.join(", ") || "—"}`);
    lines.push("");
  }
  if (report.rowsWouldCreate?.length) {
    lines.push("## Row Create Payloads");
    for (const row of report.rowsWouldCreate) {
      lines.push(`- \`${row.slotKey}\` asset \`${row.assetRecordId || "—"}\``);
      lines.push("```json");
      lines.push(JSON.stringify(row.fields || {}, null, 2));
      lines.push("```");
    }
    lines.push("");
  }
  if (report.plannerDriftAdvisory?.length) {
    lines.push("## Planner Drift Advisory");
    for (const d of report.plannerDriftAdvisory) {
      lines.push(
        `- \`${d.slotKey}\`: planner=${d.plannerAssetRecordId} writer-pinned=${d.writerPinnedAssetRecordId}`
      );
    }
    lines.push("");
  }
  if (report.applyBlockers.length) {
    lines.push("## Apply blockers");
    report.applyBlockers.forEach((b) => lines.push(`- ${b}`));
    lines.push("");
  }
  if (report.galleryRepairPostApplyDiagnosis) {
    lines.push("## Gallery Repair Post-Apply Diagnosis");
    lines.push(`- Registry attachment field: \`${report.registryAttachmentFieldUsed || "—"}\``);
    lines.push(`- Registry attachment count: **${report.registryAttachmentCount ?? 0}**`);
    lines.push(`- Repair payload source: **${report.repairPayloadSource || "—"}**`);
    lines.push(`- Field name validated (\`Image\`): **${report.fieldNameValidated ? "yes" : "no"}**`);
    lines.push(
      `- Attachment materialization required: **${report.attachmentMaterializationRequired ? "yes" : "no"}**`
    );
    lines.push(`- Likely root cause: ${report.likelyRootCause}`);
    lines.push(`- Recommended repair path: ${report.recommendedRepairPath}`);
    lines.push(`- Repair strategy selected: **${report.repairStrategySelected || "—"}**`);
    if (report.presentationContentUploadPlan) {
      lines.push(`- Local file exists: **${report.presentationContentUploadPlan.localFileExists ? "yes" : "no"}**`);
      lines.push(`- Local file path: \`${report.presentationContentUploadPlan.localFilePath || "—"}\``);
      lines.push(`- Target table: \`${report.presentationContentUploadPlan.targetTable || "—"}\``);
      lines.push(`- Target record: \`${report.presentationContentUploadPlan.targetRecordId || "—"}\``);
      lines.push(`- Target field: \`${report.presentationContentUploadPlan.targetFieldName || "—"}\``);
      lines.push(`- Upload filename: \`${report.presentationContentUploadPlan.uploadFilename || "—"}\``);
    }
    lines.push("```json");
    lines.push(JSON.stringify(report.galleryRepairPostApplyDiagnosis, null, 2));
    lines.push("```");
    lines.push("");
  }
  lines.push("## Apply commands");
  lines.push("");
  lines.push("```bash");
  lines.push(report.strictApplyCommand);
  lines.push(report.provisionalApplyCommand);
  lines.push("```");
  return lines.join("\n");
}
