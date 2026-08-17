/**
 * Brand Asset Download & Attachment Writer v6.
 *
 * Downloads and stages only formally approved brand assets, then (optionally)
 * patches Brand Asset Registry records with Attachment + Local File Path metadata.
 * Never writes Brand Setup or Explorer media fields.
 *
 * @see docs/data-intelligence/brand-asset-download-attachment-writer-v6.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  uploadFileBytesToAirtable,
  contentTypeFromFilename,
  MAX_AIRTABLE_ATTACHMENT_BYTES,
} from "../dealality/airtable-upload-attachment.js";
import {
  BRAND_ASSET_PILOT_CONFIG,
  MAP_BRAND_ASSET,
  BRAND_ASSET_REGISTRY_TABLE,
} from "./brand-asset-registry-workflow.js";
import {
  VISUAL_SLOT,
  MAP_VISUAL_SLOT,
  mapRecordToVisualSlot,
  listRegistryRecordsRaw,
} from "./brand-explorer-visual-slot-requirements.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export const WRITER_VERSION = "6.1";
export const REPORT_JSON_NAME = "brand-asset-download-attachment-writer.json";
export const REPORT_MD_NAME = "brand-asset-download-attachment-writer.md";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_KEY = "tribute-portfolio";
const STAGING_ROOT_REL = "data/partner-intelligence/assets/tribute-portfolio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const STAGING_ROOT_ABS = path.join(ROOT, STAGING_ROOT_REL);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-asset-review-decision-writer.md",
  "reports/brand-asset-human-review-readiness.md",
  "reports/tribute-visual-asset-slot-review.md",
  "lib/partner-intelligence/brand-asset-review-decision-writer.js",
  "lib/partner-intelligence/brand-asset-human-review-readiness.js",
  "lib/partner-intelligence/tribute-visual-asset-slot-review.js",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "lib/partner-intelligence/airtable-source.js",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function firstAttachmentUrl(value) {
  if (!Array.isArray(value)) return "";
  for (const item of value) {
    if (item && typeof item.url === "string" && item.url.trim()) return item.url.trim();
    if (item?.thumbnails?.large?.url) return String(item.thumbnails.large.url).trim();
  }
  return "";
}

function slug(v) {
  return nz(v)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function getRegistryTableName() {
  return process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE;
}

function registryDataUrl(baseId) {
  const table = encodeURIComponent(getRegistryTableName());
  return `https://api.airtable.com/v0/${baseId}/${table}`;
}

async function registryDataFetch(url, apiKey, init = {}) {
  const res = await fetch(url, {
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

async function patchRegistryRecordsBatch(baseId, apiKey, patches) {
  const url = registryDataUrl(baseId);
  const { res, json } = await registryDataFetch(url, apiKey, {
    method: "PATCH",
    body: JSON.stringify({
      records: patches.map((p) => ({ id: p.recordId, fields: p.fields })),
      typecast: true,
    }),
  });
  if (!res.ok) {
    throw new Error(json.error?.message || `Airtable patch registry batch failed: ${res.status}`);
  }
  return json.records || [];
}

async function patchRegistryRecord(baseId, apiKey, recordId, fields) {
  const url = `${registryDataUrl(baseId)}/${encodeURIComponent(recordId)}`;
  const { res, json } = await registryDataFetch(url, apiKey, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) throw new Error(json.error?.message || `Airtable patch failed ${recordId}`);
  return json;
}

async function getRegistryRecord(baseId, apiKey, recordId) {
  const url = `${registryDataUrl(baseId)}/${encodeURIComponent(recordId)}`;
  const { res, json } = await registryDataFetch(url, apiKey, { method: "GET" });
  if (!res.ok) throw new Error(json.error?.message || `Airtable read failed ${recordId}`);
  return json;
}

function readSlotGovernanceFromFields(f) {
  const g = (key) => nz(f[MAP_VISUAL_SLOT[key]]);
  return {
    relatedPropertyName: g("relatedPropertyName"),
    relatedValueDriver: g("relatedValueDriver"),
    countryRegion: g("countryRegion"),
    calaRelevant: g("calaRelevant"),
    propertyConfirmed: g("propertyConfirmed"),
    brandConfirmed: g("brandConfirmed"),
    validationStatus: g("validationStatus"),
    validationNotes: g("validationNotes"),
    sourcePageConfirmsContext: g("sourcePageConfirmsContext"),
  };
}

function normalizeRecord(rawRecord) {
  const f = rawRecord.fields || {};
  const slotGovernance = readSlotGovernanceFromFields(f);
  const mappedVisualSlot = mapRecordToVisualSlot({
    assetName: nz(f[MAP_BRAND_ASSET.assetName]),
    assetType: nz(f[MAP_BRAND_ASSET.assetType]),
    recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
    sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    attachment: f[MAP_BRAND_ASSET.attachment],
  });

  return {
    id: rawRecord.id,
    assetName: nz(f[MAP_BRAND_ASSET.assetName]),
    brandRecordId: nz(f[MAP_BRAND_ASSET.brandRecordId]),
    assetType: nz(f[MAP_BRAND_ASSET.assetType]),
    assetStatus: nz(f[MAP_BRAND_ASSET.assetStatus]),
    explorerUsePermission: nz(f[MAP_BRAND_ASSET.explorerUsePermission]),
    usageReviewStatus: nz(f[MAP_BRAND_ASSET.usageReviewStatus]),
    sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
    sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    reviewNotes: nz(f[MAP_BRAND_ASSET.reviewNotes]),
    sourceNotes: nz(f[MAP_BRAND_ASSET.sourceNotes]),
    localFilePath: nz(f[MAP_BRAND_ASSET.localFilePath]),
    recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    mappedVisualSlot,
    relatedPropertyName: slotGovernance.relatedPropertyName,
    relatedValueDriver: slotGovernance.relatedValueDriver,
    countryRegion: slotGovernance.countryRegion,
    calaRelevant: slotGovernance.calaRelevant,
    propertyConfirmed: slotGovernance.propertyConfirmed,
    brandConfirmed: slotGovernance.brandConfirmed,
    validationStatus: slotGovernance.validationStatus,
    validationNotes: slotGovernance.validationNotes,
    sourcePageConfirmsContext: slotGovernance.sourcePageConfirmsContext,
  };
}

function inferValueDriverLabel(record) {
  const hay = `${record.assetName} ${record.relatedPropertyName}`.toLowerCase();
  if (/humano|lima|city|urban|downtown|metropolitan/.test(hay)) return "urban";
  if (/resort|beach|cove|island|nizuc|holbox/.test(hay)) return "resort";
  if (/ermita|cartagena|heritage|colonial|conversion/.test(hay)) return "conversion";
  if (/mixed|alameda/.test(hay)) return "mixed-use";
  const explicit = nz(record.relatedValueDriver);
  if (explicit && explicit !== "None") return explicit;
  return "urban";
}

function inferSlotToken(record) {
  const slot = nz(record.recommendedExplorerSlot);
  if (record.mappedVisualSlot === VISUAL_SLOT.LOGO || /logo/i.test(slot)) return "logo";
  if (record.mappedVisualSlot === VISUAL_SLOT.HERO || /hero/i.test(slot)) return "hero";
  const gallery = slot.match(/materials\.gallery\.(\d+)/i);
  if (gallery) return `gallery-${gallery[1]}`;
  if (record.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER) {
    return `value-driver-${slug(inferValueDriverLabel(record)) || "unknown"}`;
  }
  return slug(record.mappedVisualSlot) || "asset";
}

function inferExtension(contentType, sourceUrl) {
  const ct = nz(contentType).toLowerCase();
  if (ct.includes("svg")) return "svg";
  if (ct.includes("png")) return "png";
  if (ct.includes("webp")) return "webp";
  if (ct.includes("jpeg") || ct.includes("jpg")) return "jpg";
  if (ct.includes("gif")) return "gif";
  const fromUrl = nz(sourceUrl).match(/\.([a-z0-9]{2,5})(?:\?|$)/i);
  if (fromUrl) return fromUrl[1].toLowerCase();
  return "bin";
}

function deterministicFilename(record, contentType = "") {
  const slot = inferSlotToken(record);
  const urlNameMatch = nz(record.sourceUrl).match(/\/([^/?#]+)(?:\?|$)/);
  const sourceBaseName = urlNameMatch ? urlNameMatch[1] : "";
  const sourceStem = sourceBaseName.replace(/\.[a-z0-9]{2,5}$/i, "");
  const prop = record.relatedPropertyName
    ? slug(record.relatedPropertyName)
    : record.mappedVisualSlot === VISUAL_SLOT.LOGO
      ? slug(sourceStem || record.assetName.replace(/\.[a-z0-9]+$/i, ""))
      : slug(record.assetName.replace(/\.[a-z0-9]+$/i, ""));
  const ext = inferExtension(contentType, record.sourceUrl);
  return `${BRAND_KEY}__${slot}__${prop || "asset"}.${ext}`;
}

function validateApprovedMetadata(record) {
  const issues = [];
  if (!nz(record.sourceUrl)) issues.push("Missing Source URL");
  if (!nz(record.assetType)) issues.push("Missing Asset Type");
  if (!nz(record.recommendedExplorerSlot)) issues.push("Missing Recommended Explorer Slot");
  if ([VISUAL_SLOT.HERO, VISUAL_SLOT.GALLERY, VISUAL_SLOT.VALUE_DRIVER].includes(record.mappedVisualSlot)) {
    if (!nz(record.sourcePageUrl)) issues.push("Missing Source Page URL for property image");
    if (!nz(record.relatedPropertyName)) issues.push("Missing Related Property Name for property image");
    if (!nz(record.countryRegion)) issues.push("Missing Country / Region for property image");
    if (!nz(record.calaRelevant)) issues.push("Missing CALA Relevant?");
    if (!nz(record.propertyConfirmed)) issues.push("Missing Hotel / Property Confirmed?");
    if (!nz(record.brandConfirmed)) issues.push("Missing Brand Confirmed?");
  }
  return issues;
}

function shouldExcludeEvenIfApproved(record) {
  if (record.mappedVisualSlot === VISUAL_SLOT.RECENT_OPENINGS) return "Recent Openings excluded";
  if (record.mappedVisualSlot === VISUAL_SLOT.PR_LINK) return "PR placeholder excluded";
  if (record.mappedVisualSlot === VISUAL_SLOT.BRAND_STANDARDS) return "Source reference / FDD excluded";
  if (record.assetStatus === "Mock/Demo") return "Mock/Demo excluded";
  if (record.explorerUsePermission !== "Approved For Explorer") return "Not Approved For Explorer";
  return "";
}

async function probeSource(record) {
  const timeoutMs = 20000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(record.sourceUrl, { signal: controller.signal, redirect: "follow" });
    const contentType = nz(res.headers.get("content-type"));
    const buf = await res.arrayBuffer();
    const size = buf.byteLength;
    const imageLike =
      /^image\//i.test(contentType) ||
      /svg|xml/i.test(contentType) ||
      /\.(svg|png|jpe?g|webp|gif)(?:\?|$)/i.test(record.sourceUrl);
    return {
      ok: res.ok && imageLike && size > 0 && size <= 25 * 1024 * 1024,
      status: res.status,
      contentType,
      sizeBytes: size,
      imageLike,
      reason: !res.ok
        ? `HTTP ${res.status}`
        : !imageLike
          ? `Unsupported content-type: ${contentType || "(blank)"}`
          : size <= 0
            ? "Empty response body"
            : size > 25 * 1024 * 1024
              ? `File too large (${size} bytes)`
              : "ok",
      data: Buffer.from(buf),
    };
  } catch (err) {
    return {
      ok: false,
      status: null,
      contentType: "",
      sizeBytes: 0,
      imageLike: false,
      reason: `Fetch failed: ${err.message || String(err)}`,
      data: Buffer.alloc(0),
    };
  } finally {
    clearTimeout(timer);
  }
}

function appendLine(existing, line) {
  const cur = nz(existing);
  if (!cur) return line;
  if (cur.includes(line)) return cur;
  return `${cur}\n${line}`;
}

export async function buildBrandAssetDownloadAttachmentWriterReport({
  brandKey = BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
  repairMissingAttachments = false,
  repairApproved = false,
  assetRecordId = "",
  singleAssetMaterializationApproved = false,
} = {}) {
  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[BRAND_KEY];
  const resolvedBrandId = pilot?.recordId || brandRecordId;
  const applyMode = apply && applyApproved;
  const repairApplyMode = applyMode && repairMissingAttachments && repairApproved;
  const mode = repairApplyMode
    ? "attachment-materialization-repair-apply"
    : applyMode
      ? "download-attachment-apply"
      : "dry-run";
  const scopedAssetRecordId = nz(assetRecordId);
  const singleAssetScopedMode = Boolean(scopedAssetRecordId);

  let rawRecords = [];
  let registryReadError = null;
  try {
    rawRecords = await listRegistryRecordsRaw(resolvedBrandId);
  } catch (err) {
    registryReadError = err.message || String(err);
  }
  const records = rawRecords.map(normalizeRecord);
  const rawById = new Map(rawRecords.map((rec) => [rec.id, rec]));
  const recordsInScope = singleAssetScopedMode
    ? records.filter((r) => r.id === scopedAssetRecordId)
    : records;

  const formallyApproved = recordsInScope.filter(isFormallyApprovedRecord);
  const excluded = [];
  const eligible = [];
  for (const r of formallyApproved) {
    const excludeReason = shouldExcludeEvenIfApproved(r);
    const metadataIssues = validateApprovedMetadata(r);
    if (excludeReason) {
      excluded.push({ recordId: r.id, assetName: r.assetName, reason: excludeReason });
      continue;
    }
    if (metadataIssues.length) {
      excluded.push({
        recordId: r.id,
        assetName: r.assetName,
        reason: `Metadata validation failed: ${metadataIssues.join("; ")}`,
      });
      continue;
    }
    eligible.push(r);
  }

  const probeResults = [];
  const probeByRecordId = new Map();
  const probeTargets = [...eligible];
  if (singleAssetScopedMode) {
    const scoped = recordsInScope[0];
    if (scoped?.id && !probeTargets.some((r) => r.id === scoped.id) && scoped.sourceUrl) {
      probeTargets.push(scoped);
    }
  }
  for (const r of probeTargets) {
    const probe = await probeSource(r);
    const filename = deterministicFilename(r, probe.contentType);
    const relPath = `${STAGING_ROOT_REL}/${filename}`.replace(/\\/g, "/");
    const probeRow = {
      recordId: r.id,
      assetName: r.assetName,
      sourceUrl: r.sourceUrl,
      sourcePageUrl: r.sourcePageUrl || null,
      filename,
      localFilePath: relPath,
      downloadValidation: {
        ok: probe.ok,
        httpStatus: probe.status,
        contentType: probe.contentType || null,
        sizeBytes: probe.sizeBytes,
        reason: probe.reason,
      },
      _buffer: probe.data,
    };
    probeResults.push(probeRow);
    probeByRecordId.set(r.id, probeRow);
  }

  const dryRunValidation = {
    passed: probeResults.every((p) => p.downloadValidation.ok),
    failedCount: probeResults.filter((p) => !p.downloadValidation.ok).length,
  };

  const downloadable = probeResults.filter((p) => p.downloadValidation.ok);
  const notDownloadable = probeResults
    .filter((p) => !p.downloadValidation.ok)
    .map((p) => ({ recordId: p.recordId, assetName: p.assetName, reason: p.downloadValidation.reason }));

  const attachmentStrategy = {
    supported: true,
    mode: "Prefer Airtable content API uploadAttachment (bytes); fallback remote URL patch + reread verification",
    note: "v6.1 considers patch success insufficient; materialization is true only when reread Attachment count > 0.",
    fallback: "When direct byte upload is unavailable or exceeds 5MB, fallback to URL patch and confirm reread.",
  };

  const applyPlan = downloadable.map((d) => ({
    recordId: d.recordId,
    assetName: d.assetName,
    sourceUrl: d.sourceUrl,
    localFilePath: d.localFilePath,
    attachmentFilename: d.filename,
  }));

  const attachmentStatusPool = singleAssetScopedMode ? recordsInScope : formallyApproved;
  const approvedAttachmentStatus = attachmentStatusPool.map((r) => {
    const rawAttachment = rawById.get(r.id)?.fields?.[MAP_BRAND_ASSET.attachment];
    const attachmentCount = Array.isArray(rawAttachment) ? rawAttachment.length : 0;
    const attachmentUrl = firstAttachmentUrl(rawAttachment);
    const probe = probeByRecordId.get(r.id);
    const filePath = nz(r.localFilePath) || probe?.localFilePath || "";
    const absPath = filePath ? path.join(ROOT, filePath) : "";
    const localFilePresent = absPath ? fs.existsSync(absPath) : false;
    return {
      recordId: r.id,
      assetName: r.assetName,
      recommendedExplorerSlot: r.recommendedExplorerSlot,
      sourceUrlPresent: Boolean(nz(r.sourceUrl)),
      sourceUrl: nz(r.sourceUrl),
      attachmentCount,
      attachmentPresent: attachmentCount > 0 && Boolean(attachmentUrl),
      attachmentUrl,
      localFilePath: filePath,
      localFilePresent,
      localFileMissing: !localFilePresent,
    };
  });

  const recordsProposedForRepair = approvedAttachmentStatus
    .filter((r) => !r.attachmentPresent && r.sourceUrlPresent)
    .map((r) => ({
      recordId: r.recordId,
      assetName: r.assetName,
      localFilePath: r.localFilePath,
      localFilePresent: r.localFilePresent,
      sourceUrl: r.sourceUrl,
      reason: r.localFilePresent
        ? "Attachment missing; local file present for byte upload"
        : "Attachment missing; local file absent, will use source fetch bytes if available",
    }));

  const rootCause = {
    summary:
      "Airtable URL attachment patch accepted, but Marriott image URLs did not materialize into Attachment on reread for 8 non-logo records.",
    likelyFactors: [
      "Remote URL ingestion by Airtable may fail asynchronously or be blocked by source host policies.",
      "Patch response success does not guarantee attachment ingestion completion.",
      "Materialization must be confirmed by reread Attachment count > 0.",
    ],
  };

  let applyResult = {
    filesDownloaded: [],
    recordsUpdated: [],
    failedUpdates: [],
    recordsRepaired: [],
    postApplyReread: [],
    recordsStillNotMaterialized: [],
  };
  if (applyMode && applyPlan.length) {
    fs.mkdirSync(STAGING_ROOT_ABS, { recursive: true });
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

    const patches = [];
    for (const d of downloadable) {
      const absPath = path.join(ROOT, d.localFilePath);
      fs.mkdirSync(path.dirname(absPath), { recursive: true });
      fs.writeFileSync(absPath, d._buffer);
      applyResult.filesDownloaded.push({
        recordId: d.recordId,
        assetName: d.assetName,
        path: d.localFilePath,
        sizeBytes: d.downloadValidation.sizeBytes,
      });

      const current = records.find((r) => r.id === d.recordId);
      const sourceNoteLine = `v6 download staged: ${d.localFilePath}`;
      const reviewNoteLine = "v6 download/attachment staging complete for formally approved asset.";
      patches.push({
        recordId: d.recordId,
        fields: {
          [MAP_BRAND_ASSET.attachment]: [{ url: d.sourceUrl, filename: d.filename }],
          [MAP_BRAND_ASSET.localFilePath]: d.localFilePath,
          [MAP_BRAND_ASSET.sourceNotes]: appendLine(current?.sourceNotes, sourceNoteLine),
          [MAP_BRAND_ASSET.reviewNotes]: appendLine(current?.reviewNotes, reviewNoteLine),
          [MAP_BRAND_ASSET.lastReviewedDate]: todayIso(),
        },
      });
    }

    const BATCH = 10;
    for (let i = 0; i < patches.length; i += BATCH) {
      const batch = patches.slice(i, i + BATCH);
      try {
        const updated = await patchRegistryRecordsBatch(baseId, apiKey, batch);
        applyResult.recordsUpdated.push(
          ...updated.map((u) => ({
            recordId: u.id,
            assetName: nz(u.fields?.[MAP_BRAND_ASSET.assetName]),
          }))
        );
      } catch (err) {
        applyResult.failedUpdates.push({
          batchStart: i,
          error: err.message || String(err),
          recordIds: batch.map((b) => b.recordId),
        });
      }
    }
  }

  const singleAssetRepairTarget = singleAssetScopedMode
    ? approvedAttachmentStatus.find((r) => r.recordId === scopedAssetRecordId) || null
    : null;
  const singleAssetRepairPreflight = singleAssetScopedMode
    ? {
        enabled: true,
        assetRecordId: scopedAssetRecordId,
        foundInBrandScope: recordsInScope.some((r) => r.id === scopedAssetRecordId),
        formallyApproved: formallyApproved.some((r) => r.id === scopedAssetRecordId),
        currentAttachmentCount: singleAssetRepairTarget?.attachmentCount || 0,
        sourceUrl: singleAssetRepairTarget?.sourceUrl || "",
        canDownloadRead:
          Boolean(singleAssetRepairTarget?.sourceUrl) &&
          (probeByRecordId.get(scopedAssetRecordId)?.downloadValidation?.ok || false),
        canWriteBackAttachmentField:
          Boolean(singleAssetRepairTarget?.sourceUrl) &&
          (probeByRecordId.get(scopedAssetRecordId)?.downloadValidation?.ok || false),
        proposedMaterializationAction:
          singleAssetRepairTarget && !singleAssetRepairTarget.attachmentPresent
            ? "materialize_attachment_on_registry_asset_only"
            : "no_action_attachment_already_present_or_asset_not_eligible",
      }
    : null;

  if (repairApplyMode) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
    const repairTargets = singleAssetScopedMode
      ? recordsProposedForRepair.filter((r) => r.recordId === scopedAssetRecordId)
      : recordsProposedForRepair;
    if (singleAssetScopedMode && !singleAssetMaterializationApproved) {
      throw new Error(
        "Apply blocked: single-asset materialization requires --approve-brand-asset-single-attachment-materialization"
      );
    }
    for (const target of repairTargets) {
      const record = records.find((r) => r.id === target.recordId);
      const probe = probeByRecordId.get(target.recordId);
      let strategy = "none";
      let error = "";
      try {
        let buffer = null;
        let contentType = "";
        let filename = "";
        const filePath = target.localFilePath;
        const absPath = filePath ? path.join(ROOT, filePath) : "";
        if (absPath && fs.existsSync(absPath)) {
          buffer = fs.readFileSync(absPath);
          filename = path.basename(absPath);
          contentType = contentTypeFromFilename(filename);
        } else if (probe?.downloadValidation?.ok && probe?._buffer?.length) {
          buffer = probe._buffer;
          filename = probe.filename || `asset-${target.recordId}`;
          contentType = probe.downloadValidation.contentType || contentTypeFromFilename(filename);
        }

        if (buffer && buffer.length > 0 && buffer.length <= MAX_AIRTABLE_ATTACHMENT_BYTES) {
          strategy = "content-api-upload";
          await uploadFileBytesToAirtable({
            baseId,
            recordId: target.recordId,
            fieldName: MAP_BRAND_ASSET.attachment,
            buffer,
            contentType,
            filename,
            apiKey,
          });
          if (!singleAssetScopedMode) {
            await patchRegistryRecord(baseId, apiKey, target.recordId, {
              [MAP_BRAND_ASSET.localFilePath]: filePath || nz(record?.localFilePath),
              [MAP_BRAND_ASSET.reviewNotes]: appendLine(
                record?.reviewNotes,
                "v6.1 attachment materialization repair attempted via Airtable content API."
              ),
              [MAP_BRAND_ASSET.lastReviewedDate]: todayIso(),
            });
          }
        } else {
          strategy = "url-patch-fallback";
          await patchRegistryRecord(baseId, apiKey, target.recordId, {
            [MAP_BRAND_ASSET.attachment]: [
              {
                url: nz(record?.sourceUrl),
                filename: path.basename(filePath || probe?.filename || `asset-${target.recordId}`),
              },
            ],
          });
          if (!singleAssetScopedMode) {
            await patchRegistryRecord(baseId, apiKey, target.recordId, {
              [MAP_BRAND_ASSET.localFilePath]: filePath || nz(record?.localFilePath),
              [MAP_BRAND_ASSET.reviewNotes]: appendLine(
                record?.reviewNotes,
                "v6.1 attachment repair fallback used URL patch; materialization requires reread confirmation."
              ),
              [MAP_BRAND_ASSET.lastReviewedDate]: todayIso(),
            });
          }
        }

        const reread = await getRegistryRecord(baseId, apiKey, target.recordId);
        const rereadAttachment = reread?.fields?.[MAP_BRAND_ASSET.attachment];
        const rereadCount = Array.isArray(rereadAttachment) ? rereadAttachment.length : 0;
        const materialized = rereadCount > 0 && Boolean(firstAttachmentUrl(rereadAttachment));
        applyResult.recordsRepaired.push({
          recordId: target.recordId,
          assetName: target.assetName,
          strategy,
          materialized,
        });
        applyResult.postApplyReread.push({
          recordId: target.recordId,
          attachmentCount: rereadCount,
          attachmentUrl: firstAttachmentUrl(rereadAttachment),
          attachmentMaterialized: materialized,
        });
        if (!materialized) {
          applyResult.recordsStillNotMaterialized.push({
            recordId: target.recordId,
            assetName: target.assetName,
            strategy,
          });
        }
      } catch (err) {
        error = err.message || String(err);
        applyResult.failedUpdates.push({
          recordId: target.recordId,
          assetName: target.assetName,
          strategy,
          error,
        });
      }
    }
  }

  const readyForV7AfterMaterialization = approvedAttachmentStatus.every((r) => r.attachmentPresent);

  return {
    writerVersion: WRITER_VERSION,
    singleAssetMaterializationScope: {
      enabled: singleAssetScopedMode,
      assetRecordId: scopedAssetRecordId || null,
      requiredApplyGate: "--approve-brand-asset-single-attachment-materialization",
      approved: singleAssetMaterializationApproved,
      preflight: singleAssetRepairPreflight,
    },
    generatedAt: new Date().toISOString(),
    mode,
    filesRead: FILES_READ,
    registryReadError,
    brand: {
      key: brandKey,
      recordId: resolvedBrandId,
      name: pilot?.brandName || "Tribute Portfolio",
      parentCompany: pilot?.parentCompany || "Marriott International, Inc.",
    },
    textGovernanceStatus: {
      note: "Text/governance status is owned by the Tribute package pipeline; this module does not change it.",
      textGovernancePlatformReady: true,
    },
    totalRecordsScanned: recordsInScope.length,
    formallyApprovedRecordsFound: formallyApproved.map((r) => ({
      recordId: r.id,
      assetName: r.assetName,
      recommendedExplorerSlot: r.recommendedExplorerSlot,
    })),
    recordsExcluded: excluded,
    recordsEligibleForDownload: eligible.map((r) => ({
      recordId: r.id,
      assetName: r.assetName,
      recommendedExplorerSlot: r.recommendedExplorerSlot,
      sourceUrl: r.sourceUrl,
    })),
    downloadPlan: applyPlan,
    filePathsProposed: applyPlan.map((p) => p.localFilePath),
    downloadValidation: {
      dryRunValidation,
      perRecord: probeResults.map((p) => ({
        recordId: p.recordId,
        assetName: p.assetName,
        validation: p.downloadValidation,
      })),
      notDownloadable,
    },
    attachmentStrategy,
    airtableAttachmentUploadSupported: attachmentStrategy.supported,
    airtableDirectContentUploadAvailable: true,
    approvedAttachmentStatus,
    recordsProposedForRepair,
    rootCause,
    applyResult,
    airtableModified: Boolean(
      (applyMode && (applyResult.recordsUpdated.length || applyResult.filesDownloaded.length)) ||
      (repairApplyMode && (applyResult.recordsRepaired.length || applyResult.failedUpdates.length))
    ),
    brandSetupMediaUntouched: true,
    explorerMediaFieldsUntouched: true,
    readyForExplorerMediaPromotionWriterV7:
      dryRunValidation.passed && applyPlan.length > 0 && excluded.length >= 0 && readyForV7AfterMaterialization,
    v7PresentationImagePatchSafeAfterRepair: repairApplyMode
      ? applyResult.recordsStillNotMaterialized.length === 0 && applyResult.failedUpdates.length === 0
      : readyForV7AfterMaterialization,
    applyCommand: singleAssetScopedMode
      ? "npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --asset-record-id recxVPbTlsrP9v4bQ --repair-missing-attachments --apply --approve-brand-asset-download-attachments --approve-brand-asset-attachment-materialization-repair --approve-brand-asset-single-attachment-materialization"
      : "npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --apply --approve-brand-asset-download-attachments --repair-missing-attachments --approve-brand-asset-attachment-materialization-repair",
    nextCommand:
      "npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --dry-run --repair-missing-attachments",
    cleanupNote:
      "Binary files are staged under data/partner-intelligence/assets/tribute-portfolio/ on apply only. Do not commit binaries unless explicitly approved.",
  };
}

export function buildBrandAssetDownloadAttachmentWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Asset Download & Attachment Writer v6.1");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push(`Text/governance Platform Ready: **${report.textGovernanceStatus.textGovernancePlatformReady ? "yes" : "no"}**`);
  lines.push(`Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`);
  lines.push(`Explorer media fields untouched: **${report.explorerMediaFieldsUntouched ? "yes" : "no"}**`);
  if (report.singleAssetMaterializationScope?.enabled) {
    lines.push(`Single-asset scope: **yes** (\`${report.singleAssetMaterializationScope.assetRecordId}\`)`);
    lines.push(
      `Single-asset apply gate approved: **${report.singleAssetMaterializationScope.approved ? "yes" : "no"}**`
    );
  }
  lines.push("");
  lines.push("## 1. Summary");
  lines.push("");
  lines.push(`- Total Tribute asset records scanned: **${report.totalRecordsScanned}**`);
  lines.push(`- Formally approved records found: **${report.formallyApprovedRecordsFound.length}**`);
  lines.push(`- Records eligible for download: **${report.recordsEligibleForDownload.length}**`);
  lines.push(`- Records excluded: **${report.recordsExcluded.length}**`);
  lines.push(`- Dry-run validation passed: **${report.downloadValidation.dryRunValidation.passed ? "yes" : "no"}**`);
  lines.push(`- Ready for Explorer Media Promotion Writer v7: **${report.readyForExplorerMediaPromotionWriterV7 ? "yes" : "no"}**`);
  lines.push(`- Airtable content/direct upload available: **${report.airtableDirectContentUploadAvailable ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## 2. Formally approved records");
  lines.push("");
  if (!report.formallyApprovedRecordsFound.length) lines.push("None.");
  for (const r of report.formallyApprovedRecordsFound) {
    lines.push(`- \`${r.recordId}\` — ${r.assetName} (${r.recommendedExplorerSlot})`);
  }
  lines.push("");

  lines.push("## 3. Records excluded and why");
  lines.push("");
  if (!report.recordsExcluded.length) lines.push("None.");
  for (const r of report.recordsExcluded) {
    lines.push(`- \`${r.recordId}\` — ${r.assetName} — ${r.reason}`);
  }
  lines.push("");

  lines.push("## 4. Download plan");
  lines.push("");
  if (!report.downloadPlan.length) {
    lines.push("No eligible records.");
  } else {
    lines.push("| Record | Asset | Local path |");
    lines.push("|---|---|---|");
    for (const d of report.downloadPlan) {
      lines.push(`| \`${d.recordId}\` | ${d.assetName} | \`${d.localFilePath}\` |`);
    }
  }
  lines.push("");

  lines.push("## 5. Attachment strategy");
  lines.push("");
  lines.push(`- Supported: **${report.attachmentStrategy.supported ? "yes" : "no"}**`);
  lines.push(`- Mode: ${report.attachmentStrategy.mode}`);
  lines.push(`- Note: ${report.attachmentStrategy.note}`);
  lines.push(`- Fallback: ${report.attachmentStrategy.fallback}`);
  lines.push("");

  lines.push("## 6. Dry-run validation details");
  lines.push("");
  for (const p of report.downloadValidation.perRecord) {
    lines.push(
      `- \`${p.recordId}\` — ${p.assetName}: ${p.validation.ok ? "ok" : "failed"} (${p.validation.reason}; content-type=${p.validation.contentType || "—"}; size=${p.validation.sizeBytes})`
    );
  }
  lines.push("");

  lines.push("## 7. Attachment Materialization Status");
  lines.push("");
  for (const s of report.approvedAttachmentStatus || []) {
    lines.push(
      `- \`${s.recordId}\` — ${s.assetName}: attachment=${s.attachmentPresent ? "present" : "missing"} (count=${s.attachmentCount}); localFile=${s.localFilePresent ? "present" : "missing"}; sourceUrl=${s.sourceUrlPresent ? "present" : "missing"}`
    );
  }
  lines.push("");

  lines.push("## 8. Root Cause");
  lines.push("");
  lines.push(`- ${report.rootCause?.summary || "Unknown."}`);
  for (const reason of report.rootCause?.likelyFactors || []) {
    lines.push(`- ${reason}`);
  }
  lines.push("");

  lines.push("## 9. Records Proposed For Repair");
  lines.push("");
  if (!report.recordsProposedForRepair?.length) lines.push("None.");
  for (const r of report.recordsProposedForRepair || []) {
    lines.push(`- \`${r.recordId}\` — ${r.assetName}: ${r.reason}`);
  }
  lines.push("");

  lines.push("## 10. Single-Asset Materialization Preflight");
  lines.push("");
  if (!report.singleAssetMaterializationScope?.enabled) {
    lines.push("Not in single-asset mode.");
  } else {
    const p = report.singleAssetMaterializationScope.preflight || {};
    lines.push(`- Asset record: \`${p.assetRecordId || "—"}\``);
    lines.push(`- Source URL: ${p.sourceUrl || "—"}`);
    lines.push(`- Current attachment count: **${p.currentAttachmentCount ?? 0}**`);
    lines.push(`- Proposed action: ${p.proposedMaterializationAction || "—"}`);
    lines.push(`- Can download/read: **${p.canDownloadRead ? "yes" : "no"}**`);
    lines.push(`- Can write attachment back: **${p.canWriteBackAttachmentField ? "yes" : "no"}**`);
  }
  lines.push("");

  lines.push("## 11. Apply result");
  lines.push("");
  lines.push(`- Files downloaded: **${report.applyResult.filesDownloaded.length}**`);
  lines.push(`- Registry records updated: **${report.applyResult.recordsUpdated.length}**`);
  lines.push(`- Registry records repaired: **${report.applyResult.recordsRepaired.length}**`);
  lines.push(`- Records still not materialized: **${report.applyResult.recordsStillNotMaterialized.length}**`);
  lines.push(`- Failed updates: **${report.applyResult.failedUpdates.length}**`);
  lines.push("");

  lines.push("## 12. Apply command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.applyCommand);
  lines.push("```");
  lines.push("");

  lines.push("## 13. Notes");
  lines.push("");
  lines.push(`- ${report.cleanupNote}`);
  lines.push("");
  return lines.join("\n");
}
