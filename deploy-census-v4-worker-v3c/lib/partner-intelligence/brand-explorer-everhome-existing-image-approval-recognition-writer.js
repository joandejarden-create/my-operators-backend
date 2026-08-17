/**
 * Brand Explorer Everhome Existing Image Approval Recognition + Registry Alignment v32G-R1.
 *
 * Aligns Brand Asset Registry governance to founder-confirmed working Everhome Explorer
 * images. Registry metadata + approval fields only — never touches presentation Image fields.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-existing-image-approval-recognition-writer-v32G-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  VAL_ASSET_TYPE,
  VAL_ASSET_STATUS,
  VAL_EXPLORER_USE_PERMISSION,
  VAL_USAGE_REVIEW_STATUS,
} from "./brand-asset-registry-workflow.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";
import {
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
  normalizeUrlKey,
  WRONG_BRAND_SIGNAGE_MARKERS,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  findEverhomeRegistryAssetForRow,
} from "./brand-explorer-everhome-image-governance-recognition-writer.js";
import {
  classifyEverhomeRegistryRecognition,
  isV32GVisualSlot,
} from "./brand-explorer-everhome-active-profile-finalization-writer.js";
import { TARGET_BRAND } from "./brand-explorer-everhome-openings-momentum-rebuild-writer.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v32G-R1";
export const STAGING_RUN_ID = "v32G-R1-existing-image-approval-recognition";
export const REPORT_JSON_NAME =
  "brand-explorer-everhome-existing-image-approval-recognition-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-everhome-existing-image-approval-recognition-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-everhome-existing-image-approval-recognition-writer-v32G-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32G-R1-everhome-existing-image-approval-recognition";
export const APPLY_FLAG_FOUNDER_CONFIRMED =
  "--founder-confirmed-current-everhome-images-approved";
export const APPLY_FLAG_PRESERVE_IMAGES = "--confirm-preserve-working-images";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "woodspring-suites",
  "suburban-studios",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const PRESENTATION_IMAGE_FIELDS = Object.freeze([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
]);

const FOUNDER_VALIDATION_NOTES =
  "Founder confirmed current Everhome Brand Explorer image is approved for use; image already loaded and preserved.";
const FOUNDER_SOURCE_NOTES_SUFFIX =
  "Existing working Everhome image preserved; no reload or replacement required.";
const FOUNDER_REVIEW_NOTES =
  "v32G-R1 founder-confirmed — existing working Explorer image recognized; registry aligned for active-profile.";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-active-profile-finalization-writer.json",
  "reports/brand-explorer-everhome-image-governance-recognition-writer.json",
  "reports/brand-explorer-everhome-openings-description-cleanup-writer.json",
  "reports/brand-explorer-everhome-openings-momentum-rebuild-writer.json",
  "reports/brand-explorer-everhome-presentation-cleanup-writer.json",
  "reports/brand-explorer-everhome-source-registry-normalization-writer.json",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome Presentation / Registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-existing-image-approval-recognition-writer.js",
  "scripts/brand-explorer-everhome-existing-image-approval-recognition-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v32gR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-everhome-existing-image-approval-recognition-writer.js"
    )
  );
}

function firstAttachmentUrl(fields) {
  for (const key of PRESENTATION_IMAGE_FIELDS) {
    const att = fields?.[key];
    if (Array.isArray(att) && att[0]?.url) return nz(att[0].url);
  }
  return "";
}

function inferAssetTypeForSlot(slotKey) {
  if (slotKey === "overview.hero") return ASSET_TYPE.HERO;
  if (slotKey === "footprint.openings") return ASSET_TYPE.PR_IMAGE;
  if (/materials\.gallery/.test(slotKey)) return ASSET_TYPE.EXTERIOR;
  if (/overview\.scenario|valueOwners\.scenario/.test(slotKey)) return ASSET_TYPE.LIFESTYLE;
  return ASSET_TYPE.EXTERIOR;
}

function inferExplorerSection(slotKey) {
  if (slotKey === "footprint.openings") return "Recent Openings";
  if (/materials\.gallery/.test(slotKey)) return "Image Gallery";
  if (slotKey === "overview.hero") return "Hero Image";
  if (/valueOwners\.scenario/.test(slotKey)) return "Where This Brand Creates the Most Value";
  if (/overview\.scenario/.test(slotKey)) return "Value Scenarios";
  return "Brand Explorer Presentation";
}

function parsePropertyFromTitle(title) {
  const t = nz(title);
  if (!t) return "";
  return t.replace(/^Everhome Suites\s*[-–—]\s*/i, "").trim() || t;
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function inferSourceBasis(url, brandConfig) {
  const u = nz(url).toLowerCase();
  if (!u) return SOURCE_BASIS.COMPANY_MATERIALS;
  if (brandConfig.officialDomains?.some((d) => u.includes(d))) return SOURCE_BASIS.COMPANY_MATERIALS;
  return SOURCE_BASIS.THIRD_PARTY;
}

function durableSourcePageUrl(row, brandConfig) {
  const candidates = [row.summaryUrl, extractUrlFromText(row.body), brandConfig.consumerUrl];
  for (const url of candidates) {
    const u = nz(url);
    if (!u || isTemporaryAirtableUrl(u)) continue;
    return u;
  }
  return nz(brandConfig.consumerUrl) || null;
}

function durableSourceImageUrl(row) {
  const u = nz(row.imageUrl);
  if (!u || isTemporaryAirtableUrl(u)) return null;
  return u;
}

export function imageLoadingInApi(row, apiBlock) {
  if (!row.hasImage) return false;
  return Boolean(nz(apiBlock?.imageUrl));
}

export function isWorkingSavedPresentationImage(row, apiBlock) {
  if (!row.hasImage) return false;
  if (imageLoadingInApi(row, apiBlock)) return true;
  return Boolean(row.imageUrl) && !isTemporaryAirtableUrl(row.imageUrl);
}

export function slotRequiresWorkingImage(slotKey) {
  if (slotKey === "footprint.momentum") return false;
  return isVisualImageSlot(slotKey) || /materials\.gallery|overview\.scenario|valueOwners\.scenario|footprint\.openings|overview\.featured_application|overview\.hero/.test(
    nz(slotKey)
  );
}

/** Everhome-target wrong-brand check — never flag the target brand's own name as foreign signage. */
export function detectEverhomeTargetWrongBrandRisk(text, brandConfig) {
  const haystack = nz(text);
  if (!haystack) return null;
  const allowedMentions = new Set(
    [...(brandConfig.allowedSiblingMentions || []), "everhome", "everhome suites", "choice"]
      .map((s) => s.toLowerCase())
      .filter(Boolean)
  );
  const skipMarkerIds = new Set(["everhome"]);
  for (const marker of [
    ...WRONG_BRAND_SIGNAGE_MARKERS,
    ...(brandConfig.extraWrongBrandMarkers || []),
  ]) {
    if (skipMarkerIds.has(marker.id)) continue;
    if (!marker.re.test(haystack)) continue;
    const matchText = (haystack.match(marker.re)?.[0] || marker.id).toLowerCase();
    if ([...allowedMentions].some((a) => matchText.includes(a) || haystack.toLowerCase().includes(a))) {
      continue;
    }
    return {
      markerId: marker.id,
      severity: marker.severity || "high",
      reason: `Visible non-target brand reference: ${marker.id}`,
    };
  }
  return null;
}

export function qualifiesForFounderConfirmedRecognition(row, apiBlock, brandConfig) {
  if (!isV32GVisualSlot(row.slotKey)) return { qualifies: false, reason: "not_visual_slot" };
  if (!nz(row.slotKey)) return { qualifies: false, reason: "missing_slot" };

  const combined = [row.title, row.body, row.summaryUrl].filter(Boolean).join("\n");
  const wrongBrand = detectEverhomeTargetWrongBrandRisk(combined, brandConfig);
  if (wrongBrand) return { qualifies: false, reason: `wrong_brand:${wrongBrand.markerId}` };

  if (slotRequiresWorkingImage(row.slotKey)) {
    if (!isWorkingSavedPresentationImage(row, apiBlock)) {
      return { qualifies: false, reason: "missing_or_non_working_image" };
    }
  } else if (!row.hasImage) {
    return { qualifies: false, reason: "non_image_slot_without_image" };
  }

  const pageUrl = durableSourcePageUrl(row, brandConfig);
  if (!pageUrl) return { qualifies: false, reason: "missing_durable_source_page" };

  return { qualifies: true, reason: "founder_confirmed_working_image" };
}

export function validateV32gR1RegistryWritePayload(fields) {
  const errors = [];
  if (!nz(fields[MAP_BRAND_ASSET.assetName])) errors.push("Asset Name required");
  if (!nz(fields[MAP_BRAND_ASSET.brandRecordId])) errors.push("Brand Record ID required");
  if (fields[MAP_BRAND_ASSET.assetType] && !VAL_ASSET_TYPE.includes(fields[MAP_BRAND_ASSET.assetType])) {
    errors.push(`Invalid Asset Type: ${fields[MAP_BRAND_ASSET.assetType]}`);
  }
  if (fields[MAP_BRAND_ASSET.assetStatus] && !VAL_ASSET_STATUS.includes(fields[MAP_BRAND_ASSET.assetStatus])) {
    errors.push(`Invalid Asset Status: ${fields[MAP_BRAND_ASSET.assetStatus]}`);
  }
  if (
    fields[MAP_BRAND_ASSET.explorerUsePermission] &&
    !VAL_EXPLORER_USE_PERMISSION.includes(fields[MAP_BRAND_ASSET.explorerUsePermission])
  ) {
    errors.push(`Invalid Explorer Use Permission: ${fields[MAP_BRAND_ASSET.explorerUsePermission]}`);
  }
  if (
    fields[MAP_BRAND_ASSET.usageReviewStatus] &&
    !VAL_USAGE_REVIEW_STATUS.includes(fields[MAP_BRAND_ASSET.usageReviewStatus])
  ) {
    errors.push(`Invalid Usage Review Status: ${fields[MAP_BRAND_ASSET.usageReviewStatus]}`);
  }
  if (fields[MAP_BRAND_ASSET.attachment]) errors.push("Attachment must not be set");
  if (fields[MAP_BRAND_ASSET.companyValidated]) errors.push("Company Validated must not be set");
  if (fields[MAP_BRAND_ASSET.companyValidationDate]) {
    errors.push("Company Validation Date must not be set");
  }
  for (const key of PRESENTATION_IMAGE_FIELDS) {
    if (fields[key]) errors.push(`Presentation image field blocked: ${key}`);
  }
  return { valid: errors.length === 0, errors };
}

function isSharedPressKitAsset(asset) {
  return /press kit|press-kit/i.test(nz(asset.assetName));
}

function shouldCreatePerRowRegistry(canonicalAsset, row) {
  if (!canonicalAsset) return true;
  if (isDoNotUseRecord(canonicalAsset)) return true;
  const linked = parsePresentationRowIdFromNotes(canonicalAsset.sourceNotes);
  if (linked && linked !== row.recordId) return true;
  if (
    row.slotKey === "footprint.openings" &&
    isSharedPressKitAsset(canonicalAsset) &&
    linked !== row.recordId
  ) {
    return true;
  }
  if (nz(canonicalAsset.recommendedExplorerSlot) && nz(canonicalAsset.recommendedExplorerSlot) !== row.slotKey) {
    return true;
  }
  return false;
}

export function buildFounderConfirmedApprovalFields(asset, row, brandConfig) {
  const propertyName = parsePropertyFromTitle(row.title);
  const pageUrl = durableSourcePageUrl(row, brandConfig);
  const imageUrl = durableSourceImageUrl(row);
  const linkedNote = `Linked presentation row ${row.recordId} (${row.slotKey}). ${FOUNDER_SOURCE_NOTES_SUFFIX}`;
  const existingNotes = nz(asset?.sourceNotes);
  const sourceNotes = existingNotes.includes(row.recordId)
    ? existingNotes
    : existingNotes
      ? `${existingNotes} ${linkedNote}`
      : linkedNote;

  const fields = {
    [MAP_BRAND_ASSET.assetStatus]: ASSET_STATUS.APPROVED_EXPLORER,
    [MAP_BRAND_ASSET.explorerUsePermission]: "Approved For Explorer",
    [MAP_BRAND_ASSET.usageReviewStatus]: "Usage Review Complete",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: row.slotKey,
    [MAP_BRAND_ASSET.sourceNotes]: sourceNotes,
    [MAP_BRAND_ASSET.reviewNotes]: FOUNDER_REVIEW_NOTES,
    [MAP_VISUAL_SLOT.validationStatus]: "Valid for Slot",
    [MAP_VISUAL_SLOT.validationNotes]: FOUNDER_VALIDATION_NOTES,
    [MAP_VISUAL_SLOT.explorerSection]: inferExplorerSection(row.slotKey),
    [MAP_VISUAL_SLOT.slotPurpose]: `Everhome ${row.slotKey} — founder-confirmed working Explorer image`,
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.propertyConfirmed]: row.slotKey === "footprint.openings" ? "Yes" : "Unknown",
    [MAP_VISUAL_SLOT.calaRelevant]: "No",
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
  };

  if (propertyName) {
    fields[MAP_VISUAL_SLOT.relatedPropertyName] = propertyName;
  }
  if (pageUrl) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = pageUrl;
  }
  if (imageUrl && !nz(asset?.sourceUrl)) {
    fields[MAP_BRAND_ASSET.sourceUrl] = imageUrl;
  }
  if (!nz(asset?.assetName)) {
    fields[MAP_BRAND_ASSET.assetName] =
      `Everhome Suites — ${propertyName || row.slotKey} — ${inferAssetTypeForSlot(row.slotKey)}`;
  }

  return fields;
}

export function buildFounderConfirmedRegistryCreateFields(row, brandConfig, parentCompany) {
  const propertyName = parsePropertyFromTitle(row.title);
  const slotKey = row.slotKey;
  const assetType = inferAssetTypeForSlot(slotKey);
  const pageUrl = durableSourcePageUrl(row, brandConfig);
  const imageUrl = durableSourceImageUrl(row);

  const fields = {
    [MAP_BRAND_ASSET.assetName]: `Everhome Suites — ${propertyName || slotKey} — ${assetType}`,
    [MAP_BRAND_ASSET.brand]: [TARGET_BRAND.recordId],
    [MAP_BRAND_ASSET.brandRecordId]: TARGET_BRAND.recordId,
    [MAP_BRAND_ASSET.parentCompany]: parentCompany,
    [MAP_BRAND_ASSET.assetType]: assetType,
    [MAP_BRAND_ASSET.assetStatus]: ASSET_STATUS.APPROVED_EXPLORER,
    [MAP_BRAND_ASSET.sourceBasis]: inferSourceBasis(pageUrl, brandConfig),
    [MAP_BRAND_ASSET.usageReviewStatus]: "Usage Review Complete",
    [MAP_BRAND_ASSET.explorerUsePermission]: "Approved For Explorer",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: slotKey,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: slotKey === "footprint.openings",
    [MAP_BRAND_ASSET.sourceNotes]: `Linked presentation row ${row.recordId} (${slotKey}). ${FOUNDER_SOURCE_NOTES_SUFFIX}`,
    [MAP_BRAND_ASSET.reviewNotes]: FOUNDER_REVIEW_NOTES,
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
    [MAP_BRAND_ASSET.companyValidated]: false,
    [MAP_VISUAL_SLOT.explorerSection]: inferExplorerSection(slotKey),
    [MAP_VISUAL_SLOT.slotPurpose]: `Everhome ${slotKey} — founder-confirmed working Explorer image`,
    [MAP_VISUAL_SLOT.relatedPropertyName]: propertyName || "",
    [MAP_VISUAL_SLOT.validationStatus]: "Valid for Slot",
    [MAP_VISUAL_SLOT.validationNotes]: FOUNDER_VALIDATION_NOTES,
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.propertyConfirmed]: slotKey === "footprint.openings" ? "Yes" : "Unknown",
    [MAP_VISUAL_SLOT.calaRelevant]: "No",
  };

  if (pageUrl) fields[MAP_BRAND_ASSET.sourcePageUrl] = pageUrl;
  if (imageUrl) fields[MAP_BRAND_ASSET.sourceUrl] = imageUrl;

  return fields;
}

function proposeSupersededDuplicateNote(asset, canonicalId) {
  if (asset.id === canonicalId || isDoNotUseRecord(asset)) return null;
  if (isFounderApprovedRecord(asset)) return null;
  const existing = nz(asset.reviewNotes);
  if (/superseded duplicate/i.test(existing)) return null;
  return {
    recordId: asset.id,
    fields: {
      [MAP_BRAND_ASSET.reviewNotes]: `Superseded duplicate — canonical asset ${canonicalId} (v32G-R1; not deleted).`,
    },
    wouldApprove: false,
  };
}

function registryDedupeKey(asset) {
  const slot = nz(asset.recommendedExplorerSlot);
  const page = normalizeUrlKey(asset.sourcePageUrl);
  const linked = parsePresentationRowIdFromNotes(asset.sourceNotes);
  return linked ? `row:${linked}` : `${slot}::${page || asset.id}`;
}

async function listRegistryRaw(baseId, apiKey, brandRecordId) {
  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${escapeFormulaValue(brandRecordId)}'`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BRAND_ASSET_REGISTRY_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Registry list failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return rows.map((rec) => {
    const f = rec.fields || {};
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
      section: nz(f.Section),
      imageUrl: firstAttachmentUrl(f),
      hasImage: Boolean(firstAttachmentUrl(f)),
    };
  });
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
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
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-existing-image-approval-recognition-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER_CONFIRMED,
    APPLY_FLAG_PRESERVE_IMAGES,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Existing Image Approval Recognition v32G-R1");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32G-R1 exists: **${report.v32gR1WriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Image fields untouched: **${report.imageFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Working images qualifying: **${report.workingImageConfirmationAudit.filter((r) => r.qualifiesForFounderConfirmedRecognition).length}**`);
  lines.push(`- Registry aligned/approved (proposed): **${report.registryAssetsApprovedAligned.length}**`);
  lines.push(`- Registry created (proposed): **${report.registryAssetsCreated.length}**`);
  lines.push("");
  lines.push("## Readiness (current live state)");
  lines.push(`- Final QA: ${report.finalQaExpectedResult}`);
  lines.push(`- Complete Build: ${report.completeBuildExpectedResult}`);
  lines.push(`- Visual defects: ${report.visualDefectExpectedResult}`);
  if (report.remainingBlockers.length) {
    lines.push("");
    lines.push("## Remaining blockers");
    for (const b of report.remainingBlockers) lines.push(`- ${b}`);
  }
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerEverhomeExistingImageApprovalRecognitionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderConfirmed = false,
  preserveWorkingImages = false,
  noImageFieldChanges = false,
  noValidationClaim = false,
  everhomeOnly = false,
} = {}) {
  if (PROTECTED_BRAND_SLUGS.includes(nz(brandArg).toLowerCase())) {
    throw new Error(`Protected brand cannot be modified: ${brandArg}`);
  }
  if (nz(brandArg).toLowerCase() !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v32G-R1 is Everhome-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug) || {
    slug: TARGET_BRAND.slug,
    name: TARGET_BRAND.name,
    consumerUrl: "https://www.choicehotels.com/everhome-suites",
    officialDomains: ["choicehotels.com", "choicehotelsdevelopment.com", "media.choicehotels.com"],
    allowedSiblingMentions: ["everhome", "choice hotels"],
    extraWrongBrandMarkers: [],
  };

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const parentCompany = nz(brandBasicsBefore?.fields?.["Parent Company"] || "Choice Hotels");

  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load Everhome API shape");

  const presentationRows = await listPresentationRows(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const registryRaw = await listRegistryRaw(baseId, apiKey, TARGET_BRAND.recordId);
  const registryExtended = registryRaw.map(normalizeRegistryRecordExtended);

  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const apiById = new Map(apiBlocks.map((b) => [b.recordId, b]));

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: TARGET_BRAND.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: TARGET_BRAND.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualDefectReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: TARGET_BRAND.recordId,
  }).catch(() => null);

  const workingImageConfirmationAudit = [];
  const registryAssetsApprovedAligned = [];
  const registryAssetsCreated = [];
  const duplicateSupersededAssets = [];
  const registryPatches = [];
  const registryCreates = [];
  const applyBlockers = [];
  const safetyBlockers = [];

  const visualRows = presentationRows.filter((r) => isV32GVisualSlot(r.slotKey));
  const canonicalByPresentationRow = new Map();

  for (const row of visualRows) {
    const apiBlock = apiById.get(row.recordId);
    const qualification = qualifiesForFounderConfirmedRecognition(row, apiBlock, brandConfig);
    const loading = imageLoadingInApi(row, apiBlock);
    const workingSaved = isWorkingSavedPresentationImage(row, apiBlock);

    let canonicalAsset = findEverhomeRegistryAssetForRow(registryExtended, row);
    if (canonicalAsset && isDoNotUseRecord(canonicalAsset)) {
      canonicalAsset = null;
    }

    const needsCreate = qualification.qualifies && shouldCreatePerRowRegistry(canonicalAsset, row);

    workingImageConfirmationAudit.push({
      presentationRowId: row.recordId,
      slot: row.slotKey,
      title: row.title,
      section: row.section,
      imageFieldStatus: row.hasImage ? "has_attachment" : "missing",
      imageLoadingInApi: loading,
      workingSavedAttachment: workingSaved,
      linkedRegistryAssetId: canonicalAsset?.id || null,
      registryStatus: canonicalAsset
        ? classifyEverhomeRegistryRecognition(canonicalAsset).bucket
        : "no_match",
      qualifiesForFounderConfirmedRecognition: qualification.qualifies,
      qualificationReason: qualification.reason,
      wouldCreateRegistry: needsCreate,
    });

    if (!qualification.qualifies) continue;

    if (needsCreate) {
      const createFields = buildFounderConfirmedRegistryCreateFields(row, brandConfig, parentCompany);
      const validation = validateV32gR1RegistryWritePayload(createFields);
      if (!validation.valid) {
        safetyBlockers.push(`create_validation_failed:${row.recordId}:${validation.errors.join(";")}`);
        continue;
      }
      registryCreates.push({
        presentationRowId: row.recordId,
        slotKey: row.slotKey,
        fields: createFields,
        validation,
      });
      registryAssetsCreated.push({
        presentationRowId: row.recordId,
        slotKey: row.slotKey,
        assetName: createFields[MAP_BRAND_ASSET.assetName],
        action: "create_approved_registry",
      });
      canonicalByPresentationRow.set(row.recordId, { pendingCreate: true, row });
      continue;
    }

    if (!canonicalAsset) {
      applyBlockers.push(`no_registry_candidate:${row.recordId}`);
      continue;
    }

    if (isFounderApprovedRecord(canonicalAsset) && isRegistryAssetApprovedForExplorer(canonicalAsset)) {
      registryAssetsApprovedAligned.push({
        recordId: canonicalAsset.id,
        presentationRowId: row.recordId,
        slotKey: row.slotKey,
        action: "already_approved",
      });
      canonicalByPresentationRow.set(row.recordId, { recordId: canonicalAsset.id });
      continue;
    }

    const patchFields = buildFounderConfirmedApprovalFields(canonicalAsset, row, brandConfig);
    const slimBefore = { ...canonicalAsset };
    const changedFields = {};
    for (const [key, value] of Object.entries(patchFields)) {
      const beforeVal =
        key === MAP_BRAND_ASSET.assetStatus
          ? slimBefore.assetStatus
          : key === MAP_BRAND_ASSET.explorerUsePermission
            ? slimBefore.explorerUsePermission
            : key === MAP_BRAND_ASSET.usageReviewStatus
              ? slimBefore.usageReviewStatus
              : key === MAP_BRAND_ASSET.recommendedExplorerSlot
                ? slimBefore.recommendedExplorerSlot
                : key === MAP_BRAND_ASSET.sourcePageUrl
                  ? slimBefore.sourcePageUrl
                  : key === MAP_BRAND_ASSET.sourceNotes
                    ? slimBefore.sourceNotes
                    : key === MAP_BRAND_ASSET.reviewNotes
                      ? slimBefore.reviewNotes
                      : key === MAP_VISUAL_SLOT.validationStatus
                        ? slimBefore.validationStatus
                        : key === MAP_VISUAL_SLOT.validationNotes
                          ? slimBefore.validationNotes
                          : undefined;
      if (nz(beforeVal) !== nz(value)) changedFields[key] = value;
    }

    if (!Object.keys(changedFields).length) {
      registryAssetsApprovedAligned.push({
        recordId: canonicalAsset.id,
        presentationRowId: row.recordId,
        action: "already_aligned",
      });
      canonicalByPresentationRow.set(row.recordId, { recordId: canonicalAsset.id });
      continue;
    }

    const mergedForValidation = {
      [MAP_BRAND_ASSET.assetName]: canonicalAsset.assetName,
      [MAP_BRAND_ASSET.brandRecordId]: TARGET_BRAND.recordId,
      [MAP_BRAND_ASSET.assetType]: canonicalAsset.assetType || inferAssetTypeForSlot(row.slotKey),
      ...changedFields,
    };
    const validation = validateV32gR1RegistryWritePayload(mergedForValidation);
    if (!validation.valid) {
      safetyBlockers.push(`patch_validation_failed:${canonicalAsset.id}:${validation.errors.join(";")}`);
      continue;
    }

    if (
      changedFields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER &&
      isDoNotUseRecord(canonicalAsset)
    ) {
      safetyBlockers.push(`would_approve_do_not_use:${canonicalAsset.id}`);
      continue;
    }

    registryPatches.push({
      recordId: canonicalAsset.id,
      presentationRowId: row.recordId,
      slotKey: row.slotKey,
      fields: changedFields,
      validation,
    });
    registryAssetsApprovedAligned.push({
      recordId: canonicalAsset.id,
      presentationRowId: row.recordId,
      slotKey: row.slotKey,
      action: "align_approval",
      fieldsChanged: Object.keys(changedFields),
    });
    canonicalByPresentationRow.set(row.recordId, { recordId: canonicalAsset.id });
  }

  const dedupeGroups = new Map();
  for (const asset of registryExtended) {
    if (isDoNotUseRecord(asset)) continue;
    const key = registryDedupeKey(asset);
    if (!dedupeGroups.has(key)) dedupeGroups.set(key, []);
    dedupeGroups.get(key).push(asset);
  }

  for (const [groupKey, group] of dedupeGroups.entries()) {
    if (group.length < 2) continue;
    const linkedRowIds = group
      .map((a) => parsePresentationRowIdFromNotes(a.sourceNotes))
      .filter(Boolean);
    const canonicalInGroup = group.find((a) =>
      [...canonicalByPresentationRow.values()].some((c) => c.recordId === a.id)
    );
    const canonical =
      canonicalInGroup ||
      group.find((a) => isFounderApprovedRecord(a)) ||
      group[0];
    for (const dup of group) {
      if (dup.id === canonical.id) continue;
      const notePatch = proposeSupersededDuplicateNote(dup, canonical.id);
      if (!notePatch) continue;
      duplicateSupersededAssets.push({
        recordId: dup.id,
        canonicalRecordId: canonical.id,
        groupKey,
      });
      const existingPatch = registryPatches.find((p) => p.recordId === dup.id);
      if (existingPatch) {
        Object.assign(existingPatch.fields, notePatch.fields);
      } else {
        registryPatches.push({
          recordId: dup.id,
          presentationRowId: linkedRowIds[0] || null,
          slotKey: dup.recommendedExplorerSlot,
          fields: notePatch.fields,
          validation: validateV32gR1RegistryWritePayload({
            [MAP_BRAND_ASSET.assetName]: dup.assetName,
            [MAP_BRAND_ASSET.brandRecordId]: TARGET_BRAND.recordId,
            ...notePatch.fields,
          }),
          wouldApprove: false,
        });
      }
    }
  }

  const finalQaStatus = finalQaReport?.scores?.overallActiveProfileReadiness
    ? `${finalQaReport.scores.overallActiveProfileReadiness} (${finalQaReport.scores.overallNumeric ?? "n/a"})`
    : "unavailable";
  const finalQaDefectTotal = finalQaReport?.defectCounts
    ? Object.values(finalQaReport.defectCounts).reduce((a, b) => a + b, 0)
    : null;

  const completeBuildBrandResult =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;
  const completeBuildStatus = completeBuildReport?.readyForActiveProfile
    ? "ready"
    : completeBuildBrandResult?.readinessBand || "blocked";

  const visualDefectCounts = visualDefectReport?.defectCounts || null;
  const visualDefectExpectedResult = visualDefectCounts
    ? `${visualDefectCounts.total} defects (critical ${visualDefectCounts.critical}, high ${visualDefectCounts.high})`
    : "visual defect audit unavailable";

  const imageGovernanceBlockers = (finalQaReport?.brandReports?.[0]?.defects || [])
    .filter((d) => /image|registry|governance|approval/i.test(`${d.type} ${d.message}`))
    .slice(0, 8)
    .map((d) => d.message);

  const remainingBlockers = [
    ...new Set([
      ...(completeBuildReport?.readyForActiveProfile ? [] : ["readyForActiveProfile: no"]),
      ...(finalQaReport?.scores?.overallActiveProfileReadiness === "ready"
        ? []
        : [`final_qa:${finalQaReport?.scores?.overallActiveProfileReadiness || "unknown"}`]),
      ...imageGovernanceBlockers.slice(0, 3),
    ]),
  ];

  const expectedAfterApplyNote =
    registryPatches.length || registryCreates.length
      ? "After apply, re-run Final QA / Complete Build — image governance recognition should improve if registry approvals were the sole blocker."
      : "Registry already aligned for qualifying working images.";

  if (!founderConfirmed && apply) {
    applyBlockers.push("founder_confirmed_flag_required");
  }
  if (safetyBlockers.length) {
    applyBlockers.push(...safetyBlockers);
  }

  const hasRegistryWork = registryPatches.length > 0 || registryCreates.length > 0;
  const applyGatesReady =
    apply &&
    approveBatch &&
    founderConfirmed &&
    preserveWorkingImages &&
    noImageFieldChanges &&
    noValidationClaim &&
    everhomeOnly;

  const dryRunClean = safetyBlockers.length === 0 && hasRegistryWork;
  const canApply = applyGatesReady && dryRunClean;

  let airtableModified = false;
  const applyResults = { registryUpdated: [], registryCreated: [], errors: [] };

  if (canApply) {
    for (const patch of registryPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          BRAND_ASSET_REGISTRY_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Registry PATCH failed: ${res.status}`);
        applyResults.registryUpdated.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
    for (const create of registryCreates) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          BRAND_ASSET_REGISTRY_TABLE,
          { method: "POST", body: JSON.stringify({ fields: create.fields, typecast: true }) }
        );
        if (!res.ok) throw new Error(json.error?.message || `Registry POST failed: ${res.status}`);
        applyResults.registryCreated.push({ presentationRowId: create.presentationRowId, recordId: json.id });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          presentationRowId: create.presentationRowId,
          message: err.message,
        });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v32gR1WriterExists: v32gR1WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    founderConfirmation:
      "Founder confirmed Everhome Brand Explorer images currently loaded are correct and do not require additional manual approval.",
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    workingImageConfirmationAudit,
    registryAssetsApprovedAligned,
    registryAssetsCreated,
    duplicateSupersededAssets,
    imageFieldsUntouched: true,
    presentationImageFieldsModified: [],
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    finalQaExpectedResult:
      finalQaDefectTotal != null ? `${finalQaStatus} — ${finalQaDefectTotal} defects` : finalQaStatus,
    completeBuildExpectedResult: `${completeBuildStatus} (readyForActiveProfile: ${completeBuildReport?.readyForActiveProfile ? "yes" : "no"})`,
    visualDefectExpectedResult,
    expectedAfterApplyNote,
    remainingBlockers,
    registryPatches: registryPatches.map((p) => ({
      recordId: p.recordId,
      presentationRowId: p.presentationRowId,
      fields: Object.keys(p.fields),
      wouldApprove: p.wouldApprove !== false,
    })),
    registryCreates: registryCreates.map((c) => ({
      presentationRowId: c.presentationRowId,
      slotKey: c.slotKey,
      assetName: c.fields[MAP_BRAND_ASSET.assetName],
    })),
    applyBlockers,
    safetyBlockers,
    dryRunClean,
    applyResults,
    airtableModified,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-everhome-existing-image-approval-recognition-writer -- --brand everhome-suites --dry-run",
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
