/**
 * Brand Explorer Source Evidence + Cross-Section Visual Completion Writer v24C.
 *
 * Repairs remaining Tribute visual defects: materials.gallery.3 empty card,
 * featured application preview truncation, and cross-section sort-order drift.
 * Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-source-evidence-visual-completion-writer-v24C.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { listRegistryRecordsRaw } from "./brand-explorer-visual-slot-requirements.js";
import { MAP_BRAND_ASSET, normalizeRegistryAssetRecord } from "./brand-asset-registry-workflow.js";
import {
  contentTypeFromFilename,
  uploadFileBytesToAirtable,
} from "../dealality/airtable-upload-attachment.js";

export const WRITER_VERSION = "24C";
export const REPORT_JSON_NAME = "brand-explorer-source-evidence-visual-completion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-source-evidence-visual-completion-writer.md";
export const DOC_MD_NAME = "brand-explorer-source-evidence-visual-completion-writer-v24C.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v24C-source-evidence-visual-completion";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-visual-completion-copy";
export const APPLY_FLAG_IMAGE = "--approve-brand-explorer-v24C-image-materialization";

export const GALLERY_3_SLOT = "materials.gallery.3";
export const GALLERY_3_ASSET_ID = "recxVPbTlsrP9v4bQ";
export const GALLERY_3_TITLE = "Ermita, Cartagena, a Tribute Portfolio Hotel";
export const GALLERY_3_BODY =
  "Colonial-city lifestyle conversion in Cartagena's heritage district—illustrative CALA property photography.";

export const FEATURED_APPLICATION_SLOT = "overview.featured_application";
export const FEATURED_APPLICATION_TITLE = "Stay independent.";
export const FEATURED_APPLICATION_BODY =
  "Independent boutique and lifestyle hotels with distinctive local character—Marriott soft-collection affiliation with Bonvoy distribution without erasing individuality.";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const STRICT_GALLERY_LOCAL_FILE_REL =
  "data/partner-intelligence/assets/tribute-portfolio/tribute-portfolio__gallery-3__ermita-cartagena-a-tribute-portfolio-hotel.jpg";

const CONTENT_PROTECTED_PREFIXES = [
  /^loyalty\./i,
  /^commercial\.demand/i,
  /^standards\./i,
  /^footprint\.region\./i,
  /^footprint\.portfolio_mix/i,
  /^overview\.portfolio_context$/i,
  /^branded\./i,
  /^footprint\.openings/i,
  /^footprint\.momentum$/i,
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-copy-carryover-cleanup-writer.md",
  "reports/brand-explorer-copy-carryover-cleanup-writer.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-qa-verification.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Brand Asset Registry records",
  "live Tribute Source Library records",
  "live active reference brand gallery / featured application rows",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

export function isLikelyWriterBatchSortOrder(sortOrder) {
  if (sortOrder == null || Number.isNaN(Number(sortOrder))) return false;
  const n = Number(sortOrder);
  return n >= 10 && n % 10 === 0;
}

export function countLikelyWriterBatchSortOrders(rows) {
  return rows.filter((r) => isLikelyWriterBatchSortOrder(r.sortOrder)).length;
}

function isContentProtectedSlot(slotKey) {
  const key = nz(slotKey);
  if (key === GALLERY_3_SLOT || key === FEATURED_APPLICATION_SLOT) return false;
  return CONTENT_PROTECTED_PREFIXES.some((rx) => rx.test(key));
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
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
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        title: nz(f.Title),
        body: nz(f.Body),
        sortOrder: f["Sort Order"],
        active: f.Active,
        image,
        imageUrls: image.map((x) => nz(x?.url)).filter(Boolean),
        imageFilenames: image.map((x) => nz(x?.filename)).filter(Boolean),
        companyValidated: f["Company Validated"],
        companyValidationDate: f["Company Validation Date"],
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
  for (const [, list] of grouped) {
    list.sort((a, b) => {
      const as = Number(a.sortOrder);
      const bs = Number(b.sortOrder);
      if (Number.isFinite(as) && Number.isFinite(bs) && as !== bs) return as - bs;
      return nz(a.recordId).localeCompare(nz(b.recordId));
    });
  }
  return grouped;
}

function readRegistryAttachments(fields = {}) {
  const raw = fields[MAP_BRAND_ASSET.attachment];
  return Array.isArray(raw) ? raw : [];
}

function normalizeRegistryAsset(rawRecord) {
  const base = normalizeRegistryAssetRecord(rawRecord);
  const attachments = readRegistryAttachments(rawRecord?.fields || {});
  const attachmentUrls = attachments.map((a) => nz(a?.url)).filter(Boolean);
  const sourceUrl = nz(base.sourceUrl);
  return {
    id: rawRecord.id,
    assetName: nz(base.assetName),
    sourceUrl,
    attachmentUrls,
    preferredImageUrl: attachmentUrls[0] || sourceUrl,
    hasAttachment: attachments.length > 0,
    approved:
      /approved for explorer use/i.test(nz(base.assetStatus)) ||
      /approved for explorer/i.test(nz(base.explorerUsePermission)),
  };
}

function isGalleryRowComplete(row) {
  return Boolean(row && hasVal(row.title) && row.imageUrls.length > 0);
}

function supplementTargetRowsFromApi(presentationRows, brandApi) {
  const slotsPresent = new Set(presentationRows.map((r) => r.slotKey));
  const supplemented = [...presentationRows];
  for (const slotKey of [GALLERY_3_SLOT, FEATURED_APPLICATION_SLOT]) {
    if (slotsPresent.has(slotKey)) continue;
    const block = (brandApi?.brandExplorer?.blocks || []).find((b) => nz(b.slotKey) === slotKey);
    if (!block?.recordId) continue;
    supplemented.push({
      recordId: block.recordId,
      slotKey,
      title: nz(block.title),
      body: nz(block.body),
      sortOrder: typeof block.sort === "number" ? block.sort : null,
      active: true,
      image: hasVal(block.imageUrl) ? [{ url: nz(block.imageUrl) }] : [],
      imageUrls: hasVal(block.imageUrl) ? [nz(block.imageUrl)] : [],
      imageFilenames: [],
      companyValidated: null,
      companyValidationDate: null,
      supplementedFromApi: true,
    });
  }
  return supplemented;
}

function diagnoseGallery3(rowsBySlot, registryById, brandApi) {
  const rows = rowsBySlot.get(GALLERY_3_SLOT) || [];
  const asset = registryById.get(GALLERY_3_ASSET_ID) || null;
  const apiRow = (brandApi?.brandExplorer?.blocks || []).find((b) => b.slotKey === GALLERY_3_SLOT);
  const primary = rows[0] || null;

  const issues = [];
  if (!rows.length) issues.push("missing_row");
  if (rows.length > 1) issues.push("duplicate_rows");
  if (primary && !hasVal(primary.title)) issues.push("missing_title");
  if (primary && !primary.imageUrls.length) issues.push("missing_image_attachment");
  if (primary && hasVal(primary.title) && hasVal(primary.body) === false && !hasVal(GALLERY_3_BODY)) {
    issues.push("missing_body_optional");
  }
  if (apiRow && !hasVal(apiRow.imageUrl) && primary?.imageUrls.length) issues.push("api_imageUrl_not_exposed");
  if (apiRow && hasVal(apiRow.imageUrl) && !hasVal(apiRow.title)) issues.push("api_title_empty");
  if (!asset) issues.push("registry_asset_missing");
  if (asset && !asset.approved && !asset.hasAttachment && !asset.sourceUrl) issues.push("registry_asset_not_ready");

  return {
    slotKey: GALLERY_3_SLOT,
    rowCount: rows.length,
    primaryRecordId: primary?.recordId || null,
    duplicateRecordIds: rows.slice(1).map((r) => r.recordId),
    title: primary?.title || "",
    body: primary?.body || "",
    imageCount: primary?.imageUrls.length || 0,
    imageUrls: primary?.imageUrls || [],
    apiImageUrl: nz(apiRow?.imageUrl),
    apiTitle: nz(apiRow?.title),
    registryAssetId: GALLERY_3_ASSET_ID,
    registryAssetName: asset?.assetName || "",
    registryApproved: Boolean(asset?.approved),
    issues,
    rootCause:
      !rows.length
        ? "missing_row"
        : !primary?.imageUrls.length
          ? "non_materialized_image"
          : !hasVal(primary?.title)
            ? "title_only_missing_copy"
            : rows.length > 1
              ? "duplicate_orphan_rows"
              : "none",
  };
}

function diagnoseFeaturedApplication(rowsBySlot, brandApi) {
  const rows = rowsBySlot.get(FEATURED_APPLICATION_SLOT) || [];
  const positioning = nz(brandApi?.brandPositioning);
  const tagline = nz(brandApi?.brandTaglineMotto);
  const slotRow = rows[0] || null;
  const truncatedBasics = positioning.length > 220;

  return {
    slotKey: FEATURED_APPLICATION_SLOT,
    rowExists: rows.length > 0,
    recordId: slotRow?.recordId || null,
    slotTitle: nz(slotRow?.title),
    slotBody: nz(slotRow?.body),
    basicsTagline: tagline,
    basicsPositioningLength: positioning.length,
    basicsPositioningTruncatedInUi: truncatedBasics,
    rootCause: slotRow && hasVal(slotRow.body)
      ? "none"
      : truncatedBasics
        ? "frontend_truncates_brandPositioning_at_220"
        : !positioning
          ? "missing_positioning_source"
          : "missing_dedicated_preview_slot",
  };
}

function diagnoseCrossSectionSort(rows) {
  const likelyDefaults = rows.filter((r) => isLikelyWriterBatchSortOrder(r.sortOrder));
  return {
    totalRows: rows.length,
    likelyWriterDefaultCount: likelyDefaults.length,
    rootCause: likelyDefaults.length > 0 ? "writer_index_times_10_sort_defaults" : "none",
    sampleRecordIds: likelyDefaults.slice(0, 5).map((r) => r.recordId),
  };
}

function buildSortOrderRepairs(rows) {
  const grouped = groupRowsBySlot(rows);
  const repairs = [];
  for (const [slotKey, slotRows] of grouped) {
    slotRows.forEach((row, index) => {
      const galleryMatch = slotKey.match(/^materials\.gallery\.(\d+)$/i);
      const proposedSort = galleryMatch ? Number(galleryMatch[1]) : index;
      if (!isLikelyWriterBatchSortOrder(row.sortOrder) && Number(row.sortOrder) === proposedSort) return;
      if (Number(row.sortOrder) === proposedSort) return;
      repairs.push({
        recordId: row.recordId,
        slotKey,
        fixReason: "normalize_sort_order_within_slot",
        currentSortOrder: row.sortOrder,
        proposedSortOrder: proposedSort,
        fields: {
          "Sort Order": proposedSort,
        },
      });
    });
  }
  return repairs;
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

async function resolveGalleryImageAttachment(baseId, apiKey, asset) {
  if (asset?.hasAttachment && asset.attachmentUrls[0]) {
    return [{ url: asset.attachmentUrls[0] }];
  }
  const localPath = path.join(ROOT, STRICT_GALLERY_LOCAL_FILE_REL);
  if (fs.existsSync(localPath)) {
    const bytes = fs.readFileSync(localPath);
    const filename = path.basename(localPath);
    const uploaded = await uploadFileBytesToAirtable({
      baseId,
      apiKey,
      bytes,
      filename,
      contentType: contentTypeFromFilename(filename),
    });
    if (uploaded?.url) return [{ url: uploaded.url, filename }];
  }
  if (asset?.sourceUrl) return [{ url: asset.sourceUrl }];
  return null;
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-source-evidence-visual-completion-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_IMAGE}`;
}

export async function buildBrandExplorerSourceEvidenceVisualCompletionWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  imageMaterializationApproved = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v24C pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApiBefore = await fetchBrandApiShape(brandRecordId);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );
  const presentationRows = supplementTargetRowsFromApi(
    normalizePresentationRows(presentationRaw),
    brandApiBefore
  );
  const rowsBySlot = groupRowsBySlot(presentationRows);

  const registryRaw = await listRegistryRecordsRaw(brandRecordId);
  const registryById = new Map(
    registryRaw.map((rec) => [rec.id, normalizeRegistryAsset(rec)])
  );

  const gallery3Diagnosis = diagnoseGallery3(rowsBySlot, registryById, brandApiBefore);
  const featuredDiagnosis = diagnoseFeaturedApplication(rowsBySlot, brandApiBefore);
  const crossSectionDiagnosis = diagnoseCrossSectionSort(presentationRows);

  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const applyBlockers = [];
  const frontendChanges = [];

  const galleryRows = rowsBySlot.get(GALLERY_3_SLOT) || [];
  const galleryAsset = registryById.get(GALLERY_3_ASSET_ID);

  if (galleryRows.length > 1) {
    applyBlockers.push("materials.gallery.3:duplicate_rows_detected");
  } else if (galleryRows.length === 1) {
    const row = galleryRows[0];
    const fields = {
      "Slot Key": GALLERY_3_SLOT,
      Title: GALLERY_3_TITLE,
      Body: GALLERY_3_BODY,
      "Brand Name": BRAND_NAME,
      Active: true,
    };
    const needsCopy =
      nz(row.title) !== GALLERY_3_TITLE || nz(row.body) !== GALLERY_3_BODY;
    const needsImage = !row.imageUrls.length;
    if (needsCopy || needsImage) {
      if (needsImage) {
        if (!galleryAsset) applyBlockers.push("materials.gallery.3:registry_asset_missing");
        else fields.Image = [{ url: galleryAsset.preferredImageUrl }];
      }
      if (!applyBlockers.some((b) => b.startsWith("materials.gallery.3"))) {
        rowsWouldUpdate.push({
          recordId: row.recordId,
          slotKey: GALLERY_3_SLOT,
          fixReason: needsImage ? "gallery_3_copy_and_image" : "gallery_3_owner_facing_copy",
          fields,
          currentTitle: row.title,
          currentBody: row.body,
          proposedTitle: GALLERY_3_TITLE,
          proposedBody: GALLERY_3_BODY,
          needsImageMaterialization: needsImage,
        });
      }
    }
  } else {
    if (!galleryAsset) {
      applyBlockers.push("materials.gallery.3:missing_row_and_registry_asset");
    } else {
      rowsWouldCreate.push({
        slotKey: GALLERY_3_SLOT,
        fixReason: "gallery_3_row_create",
        fields: {
          Brand: [brandRecordId],
          "Slot Key": GALLERY_3_SLOT,
          Title: GALLERY_3_TITLE,
          Body: GALLERY_3_BODY,
          "Brand Name": BRAND_NAME,
          Active: true,
          "Sort Order": 3,
          Image: [{ url: galleryAsset.preferredImageUrl }],
        },
        proposedTitle: GALLERY_3_TITLE,
        proposedBody: GALLERY_3_BODY,
        needsImageMaterialization: true,
      });
    }
  }

  const featuredRows = rowsBySlot.get(FEATURED_APPLICATION_SLOT) || [];
  if (featuredRows.length > 1) {
    applyBlockers.push("overview.featured_application:duplicate_rows");
  } else if (featuredRows.length === 1) {
    const row = featuredRows[0];
    if (
      nz(row.title) !== FEATURED_APPLICATION_TITLE ||
      nz(row.body) !== FEATURED_APPLICATION_BODY
    ) {
      rowsWouldUpdate.push({
        recordId: row.recordId,
        slotKey: FEATURED_APPLICATION_SLOT,
        fixReason: "featured_application_preview_copy",
        fields: {
          "Slot Key": FEATURED_APPLICATION_SLOT,
          Title: FEATURED_APPLICATION_TITLE,
          Body: FEATURED_APPLICATION_BODY,
          "Brand Name": BRAND_NAME,
          Active: true,
          "Sort Order": 0,
        },
        proposedTitle: FEATURED_APPLICATION_TITLE,
        proposedBody: FEATURED_APPLICATION_BODY,
      });
    }
  } else {
    rowsWouldCreate.push({
      slotKey: FEATURED_APPLICATION_SLOT,
      fixReason: "featured_application_row_create",
      fields: {
        Brand: [brandRecordId],
        "Slot Key": FEATURED_APPLICATION_SLOT,
        Title: FEATURED_APPLICATION_TITLE,
        Body: FEATURED_APPLICATION_BODY,
        "Brand Name": BRAND_NAME,
        Active: true,
        "Sort Order": 0,
      },
      proposedTitle: FEATURED_APPLICATION_TITLE,
      proposedBody: FEATURED_APPLICATION_BODY,
    });
  }

  if (
    featuredDiagnosis.rootCause === "frontend_truncates_brandPositioning_at_220" ||
    featuredDiagnosis.rootCause === "missing_dedicated_preview_slot"
  ) {
    frontendChanges.push({
      file: "public/js/brand-explorer-atelier-from-api.js",
      change: "Prefer overview.featured_application presentation slot for featured preview before truncating Brand Basics positioning.",
      reason: featuredDiagnosis.rootCause,
    });
  }

  const sortRepairs = buildSortOrderRepairs(presentationRows);
  for (const repair of sortRepairs) {
    rowsWouldUpdate.push({
      recordId: repair.recordId,
      slotKey: repair.slotKey,
      fixReason: repair.fixReason,
      fields: repair.fields,
      currentSortOrder: repair.currentSortOrder,
      proposedSortOrder: repair.proposedSortOrder,
      contentOnlySortOrder: true,
    });
  }

  const projectedGalleryComplete =
    galleryRows.length <= 1 &&
    Boolean(galleryRows[0]?.imageUrls?.length || galleryAsset?.preferredImageUrl) &&
    Boolean(hasVal(GALLERY_3_TITLE));

  const projectedSortDefaultCount = Math.max(
    0,
    crossSectionDiagnosis.likelyWriterDefaultCount - sortRepairs.length
  );

  const needsImageWork =
    rowsWouldUpdate.some((r) => r.needsImageMaterialization) ||
    rowsWouldCreate.some((r) => r.needsImageMaterialization);

  const applyGatesReady = apply && approveBatch && founderReviewed && imageMaterializationApproved;
  const hasWork = rowsWouldUpdate.length > 0 || rowsWouldCreate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const created = [];
    const errors = [];

    const patchById = new Map();
    for (const row of rowsWouldUpdate) {
      const existing = patchById.get(row.recordId) || { recordId: row.recordId, fields: {} };
      Object.assign(existing.fields, row.fields);
      patchById.set(row.recordId, existing);
    }

    for (const [recordId, patch] of patchById) {
      const slotKey = rowsWouldUpdate.find((r) => r.recordId === recordId)?.slotKey || "";
      if (patch.fields.Image) {
        if (slotKey !== GALLERY_3_SLOT) {
          delete patch.fields.Image;
        } else {
          const asset = registryById.get(GALLERY_3_ASSET_ID);
          const attachment = await resolveGalleryImageAttachment(baseId, apiKey, asset);
          if (!attachment) {
            errors.push({ recordId, message: "gallery_3_image_materialization_failed" });
            continue;
          }
          patch.fields.Image = attachment;
        }
      }

      if (isContentProtectedSlot(slotKey) && (patch.fields.Title || patch.fields.Body)) {
        errors.push({ recordId, slotKey, message: "blocked_protected_slot_content_change" });
        continue;
      }

      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: patch.fields, typecast: true }),
        },
        recordId
      );
      if (!res.ok) {
        errors.push({ action: "update", recordId, message: json.error?.message || res.status });
      } else {
        updated.push({ recordId, fields: Object.keys(patch.fields) });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const row of rowsWouldCreate) {
      const fields = { ...row.fields };
      if (fields.Image) {
        const asset = registryById.get(GALLERY_3_ASSET_ID);
        const attachment = await resolveGalleryImageAttachment(baseId, apiKey, asset);
        if (!attachment) {
          errors.push({ slotKey: row.slotKey, message: "gallery_3_create_image_materialization_failed" });
          continue;
        }
        fields.Image = attachment;
      }
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields, typecast: true }),
      });
      if (!res.ok) {
        errors.push({ action: "create", slotKey: row.slotKey, message: json.error?.message || res.status });
      } else {
        created.push({ recordId: json.id, slotKey: row.slotKey });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified = (updated.length > 0 || created.length > 0) && errors.length === 0;
    applyResults = { updated, created, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { updated: [], created: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v24CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: brandRecordId, slug: "tribute-portfolio" },
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-source-evidence-visual-completion-writer.js",
      "scripts/brand-explorer-source-evidence-visual-completion-writer.mjs",
      "docs/data-intelligence/brand-explorer-source-evidence-visual-completion-writer-v24C.md",
      "reports/brand-explorer-source-evidence-visual-completion-writer.md",
      "reports/brand-explorer-source-evidence-visual-completion-writer.json",
      "public/js/brand-explorer-atelier-from-api.js",
      "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
      "package.json",
    ],
    gallery3Diagnosis,
    proposedGallery3Repair: {
      title: GALLERY_3_TITLE,
      body: GALLERY_3_BODY,
      assetRecordId: GALLERY_3_ASSET_ID,
      action: galleryRows.length ? "update_existing_row" : "create_row",
      recordId: galleryRows[0]?.recordId || null,
    },
    crossSectionVisualDefect: crossSectionDiagnosis,
    proposedCrossSectionRepair: {
      strategy: "normalize_sort_order_per_slot_key",
      rowsWouldNormalize: sortRepairs.length,
      projectedLikelyDefaultCountAfterApply: projectedSortDefaultCount,
    },
    featuredApplicationDefect: featuredDiagnosis,
    proposedFeaturedApplicationRepair: {
      slotKey: FEATURED_APPLICATION_SLOT,
      title: FEATURED_APPLICATION_TITLE,
      body: FEATURED_APPLICATION_BODY,
      action: featuredRows.length ? "update_or_match" : "create_row",
      frontendPreviewMapping: frontendChanges,
    },
    rowsWouldUpdate,
    rowsWouldCreate,
    sortOrderUpdates: sortRepairs,
    frontendChanges,
    projectedGallery3CompleteAfterApply: projectedGalleryComplete,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      imageMaterializationApproved,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply: rowsWouldUpdate.length === 0 && rowsWouldCreate.length === 0,
    doesNotDo: [
      "Set Company Validated or Company Validation Date",
      "Approve facts or modify Source Library stewardship flags",
      "Rewrite loyalty, demand, standards table, portfolio mix/context, or geographic footprint copy",
      "Imply Marriott validation",
    ],
  };
}

export function buildBrandExplorerSourceEvidenceVisualCompletionWriterMarkdown(report) {
  return [
    `# Brand Explorer Source Evidence + Visual Completion Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Gallery.3 root cause: **${report.gallery3Diagnosis.rootCause}**`,
    `- Cross-section root cause: **${report.crossSectionVisualDefect.rootCause}**`,
    `- Featured Application root cause: **${report.featuredApplicationDefect.rootCause}**`,
    `- Rows would update: **${report.rowsWouldUpdate.length}**`,
    `- Rows would create: **${report.rowsWouldCreate.length}**`,
    `- Sort-order normalizations: **${report.sortOrderUpdates.length}**`,
    `- Frontend changes planned: **${report.frontendChanges.length}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    "",
    "## Exact apply command",
    "```bash",
    report.exactApplyCommand,
    "```",
  ].join("\n");
}
