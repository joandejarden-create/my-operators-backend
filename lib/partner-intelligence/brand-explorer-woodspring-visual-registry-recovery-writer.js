/**
 * Brand Explorer WoodSpring Visual Registry Recovery + Live Rendering Fix v33D-R2.
 *
 * Recovers v33D-R1 partial apply: registry approvals failed on v2 validator.
 * Patches registry with v32G-R1 validator, fixes gallery labels/visibility,
 * and rematerializes images only when attachments are missing.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-visual-registry-recovery-writer-v33D-R2.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  VAL_ASSET_STATUS,
  VAL_EXPLORER_USE_PERMISSION,
  VAL_USAGE_REVIEW_STATUS,
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS, ASSET_TYPE, SOURCE_BASIS } from "./brand-asset-pr-package-governance.js";
import {
  assessPresentationRowImageGovernance,
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  buildWoodspringRegistryStagedAsset,
  TARGET_BRAND as WOODSPRING_TARGET,
} from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import {
  isDoNotUseRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  imageLoadingInApi,
  isWorkingSavedPresentationImage,
  validateV32gR1RegistryWritePayload,
} from "./brand-explorer-everhome-existing-image-approval-recognition-writer.js";
import {
  buildFounderApprovedRegistryPatch,
  detectWoodspringWrongBrandRisk,
  GALLERY_DISPLAY_STATUS_HIDE,
  QUARANTINED_SCENARIO3_RECORD_ID,
  TARGET_BRAND,
  v33dWriterExists,
} from "./brand-explorer-woodspring-visual-completion-writer.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33D-R2";
export const STAGING_RUN_ID = "v33D-R2-woodspring-visual-registry-recovery";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-visual-registry-recovery-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-visual-registry-recovery-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-visual-registry-recovery-writer-v33D-R2.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33D-R2-woodspring-visual-registry-recovery";
export const APPLY_FLAG_FOUNDER = "--founder-approved-woodspring-official-images";
export const APPLY_FLAG_OFFICIAL_ONLY = "--confirm-official-source-images-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_MOMENTUM = "--confirm-no-momentum-changes";
export const APPLY_FLAG_QUARANTINE = "--confirm-quarantined-everhome-row-stays-hidden";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const PROTECTED_BRAND_SLUGS = Object.freeze(["everhome-suites", "suburban-studios"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MOMENTUM_SLOT = "footprint.momentum";
const OPENINGS_SLOT = "footprint.openings";

const SCENARIO_SLOTS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
]);

const GALLERY_SLOTS = Object.freeze([
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
]);

export const WOODSPRING_GALLERY_LABELS = Object.freeze({
  "materials.gallery.1": "Exterior / Prototype",
  "materials.gallery.2": "Guest Room",
  "materials.gallery.3": "Kitchen-Equipped Suite",
  "materials.gallery.4": "Extended-Stay Suite",
  "materials.gallery.5": "Brand Platform Visual",
  "materials.gallery.6": "Property Example",
});

const INAPPROPRIATE_GALLERY_LABELS = Object.freeze([
  "rooftop / bar",
  "pool & resort setting",
  "restaurant",
  "arrival",
  "lobby",
]);

const FOUNDER_REVIEW_NOTES =
  "v33D-R2 founder-approved — official WoodSpring / Choice image recognized for Explorer registry.";
const FOUNDER_SOURCE_NOTES_SUFFIX =
  "Founder-approved official WoodSpring visual; durable source page on file.";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-visual-completion-writer.json",
  "reports/brand-explorer-woodspring-openings-momentum-build-writer.json",
  "reports/brand-explorer-woodspring-presentation-cleanup-backfill-writer.json",
  "reports/brand-explorer-woodspring-source-registry-readiness-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "lib/partner-intelligence/brand-explorer-everhome-existing-image-approval-recognition-writer.js",
  "lib/partner-intelligence/brand-explorer-woodspring-visual-completion-writer.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
  "live WoodSpring presentation / registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-visual-registry-recovery-writer.js",
  "scripts/brand-explorer-woodspring-visual-registry-recovery-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "public/js/brand-explorer-atelier-from-api.js",
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

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function inferAssetTypeForSlot(slotKey) {
  if (slotKey === "overview.hero") return ASSET_TYPE.HERO;
  if (slotKey === OPENINGS_SLOT) return ASSET_TYPE.PR_IMAGE;
  if (/materials\.gallery/.test(slotKey)) return ASSET_TYPE.EXTERIOR;
  if (/overview\.scenario/.test(slotKey)) return ASSET_TYPE.LIFESTYLE;
  return ASSET_TYPE.EXTERIOR;
}

function inferExplorerSection(slotKey) {
  if (slotKey === OPENINGS_SLOT) return "Recent Openings";
  if (/materials\.gallery/.test(slotKey)) return "Image Gallery";
  if (/overview\.scenario/.test(slotKey)) return "Value Scenarios";
  return "Brand Explorer Presentation";
}

function inferSourceBasis(url, brandConfig) {
  const u = nz(url).toLowerCase();
  if (!u) return SOURCE_BASIS.COMPANY_MATERIALS;
  if (brandConfig.officialDomains?.some((d) => u.includes(d))) return SOURCE_BASIS.COMPANY_MATERIALS;
  return SOURCE_BASIS.THIRD_PARTY;
}

function isOfficialPageUrl(url, brandConfig) {
  const u = nz(url).toLowerCase();
  return Boolean(
    u &&
      !isTemporaryAirtableUrl(u) &&
      (brandConfig.officialDomains?.some((d) => u.includes(d)) ||
        u.includes("choicehotelsdevelopment.com") ||
        u.includes("media.choicehotels.com"))
  );
}

function mapStagedToRegistryFields(staged, brandRecordId, parentCompany) {
  const fields = {
    [MAP_BRAND_ASSET.assetName]: staged.assetName,
    [MAP_BRAND_ASSET.brand]: [brandRecordId],
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
    [MAP_BRAND_ASSET.parentCompany]: parentCompany,
    [MAP_BRAND_ASSET.assetType]: staged.assetType,
    [MAP_BRAND_ASSET.assetStatus]: staged.assetStatus,
    [MAP_BRAND_ASSET.sourceBasis]: staged.sourceBasis,
    [MAP_BRAND_ASSET.usageReviewStatus]: staged.usageReviewStatus,
    [MAP_BRAND_ASSET.explorerUsePermission]: staged.explorerUsePermission,
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: staged.recommendedExplorerSlot,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: Boolean(staged.isPrimaryCandidate),
    [MAP_BRAND_ASSET.sourceNotes]: staged.sourceNotes,
    [MAP_BRAND_ASSET.reviewNotes]: staged.reviewNotes,
    [MAP_BRAND_ASSET.stagingRunId]: staged.stagingRunId,
    [MAP_BRAND_ASSET.companyValidated]: false,
    [MAP_VISUAL_SLOT.explorerSection]: staged.explorerSection,
    [MAP_VISUAL_SLOT.slotPurpose]: staged.slotPurpose,
    [MAP_VISUAL_SLOT.relatedPropertyName]: staged.relatedPropertyName,
    [MAP_VISUAL_SLOT.validationStatus]: staged.validationStatus,
    [MAP_VISUAL_SLOT.validationNotes]: staged.validationNotes,
    [MAP_VISUAL_SLOT.brandConfirmed]: staged.brandConfirmed,
    [MAP_VISUAL_SLOT.propertyConfirmed]: staged.propertyConfirmed,
    [MAP_VISUAL_SLOT.calaRelevant]: staged.calaRelevant,
  };
  if (staged.sourceUrl && !isTemporaryAirtableUrl(staged.sourceUrl)) {
    fields[MAP_BRAND_ASSET.sourceUrl] = staged.sourceUrl;
  }
  if (staged.sourcePageUrl && !isTemporaryAirtableUrl(staged.sourcePageUrl)) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = staged.sourcePageUrl;
  }
  return fields;
}

export function v33dR2WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-visual-registry-recovery-writer.js"
    )
  );
}

function isV33dTouchedRow(row) {
  if (!row) return false;
  if (row.recordId === QUARANTINED_SCENARIO3_RECORD_ID) return true;
  if (row.slotKey === OPENINGS_SLOT) return true;
  if (SCENARIO_SLOTS.includes(row.slotKey)) return true;
  if (GALLERY_SLOTS.includes(row.slotKey)) return true;
  return false;
}

function isGalleryHiddenSlot(slotKey) {
  return ["materials.gallery.4", "materials.gallery.5", "materials.gallery.6"].includes(slotKey);
}

function isRowQuarantined(row) {
  return row.recordId === QUARANTINED_SCENARIO3_RECORD_ID;
}

function findRegistryForRow(registryAssets, row) {
  const byNotes = registryAssets.find(
    (a) =>
      parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId ||
      nz(a.sourceNotes).includes(row.recordId)
  );
  if (byNotes) return byNotes;
  if (row.slotKey === OPENINGS_SLOT) {
    const openingCandidates = registryAssets.filter(
      (a) =>
        nz(a.recommendedExplorerSlot) === OPENINGS_SLOT &&
        (parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId ||
          nz(a.sourceNotes).includes(row.recordId))
    );
    if (openingCandidates.length === 1) return openingCandidates[0];
    return openingCandidates.find((a) => parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId) || null;
  }
  const bySlot = registryAssets.filter((a) => nz(a.recommendedExplorerSlot) === row.slotKey);
  if (bySlot.length === 1) return bySlot[0];
  return bySlot.find((a) => parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId) || null;
}

async function listRegistryRaw(baseId, apiKey, brandRecordId) {
  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${escapeFormulaValue(brandRecordId)}'`;
  const table = registryTableName();
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Registry list failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function durableSourcePageUrl(row, brandConfig) {
  const fromBody = extractUrlFromText(row.body);
  if (isOfficialPageUrl(fromBody, brandConfig)) return fromBody;
  return brandConfig.consumerUrl;
}

export function qualifiesForWoodspringFounderApproval(row, apiBlock, brandConfig, registryAsset) {
  if (isRowQuarantined(row)) {
    return { qualifies: false, reason: "quarantined_everhome_row" };
  }
  if (row.slotKey === MOMENTUM_SLOT) {
    return { qualifies: false, reason: "momentum_protected" };
  }
  if (isGalleryHiddenSlot(row.slotKey)) {
    return { qualifies: false, reason: "deferred_hidden_gallery_slot" };
  }
  if (nz(row.externalDisplayStatus).toLowerCase() === "do not display" && !row.hasImage) {
    return { qualifies: false, reason: "hidden_without_image" };
  }
  if (registryAsset && isDoNotUseRecord(registryAsset)) {
    return { qualifies: false, reason: "registry_do_not_use" };
  }
  const wrongBrand = detectWoodspringWrongBrandRisk(`${row.title} ${row.body}`, brandConfig);
  if (wrongBrand && /\beverhome\b/i.test(`${row.title} ${row.body}`)) {
    return { qualifies: false, reason: `wrong_brand:${wrongBrand.markerId}` };
  }
  if (!isWorkingSavedPresentationImage(row, apiBlock)) {
    return { qualifies: false, reason: "missing_or_non_working_image" };
  }
  const pageUrl = durableSourcePageUrl(row, brandConfig);
  if (!pageUrl || !isOfficialPageUrl(pageUrl, brandConfig)) {
    return { qualifies: false, reason: "missing_durable_official_source_page" };
  }
  if (/\beverhome\b/i.test(nz(row.imageUrl))) {
    return { qualifies: false, reason: "everhome_image_url" };
  }
  return { qualifies: true, reason: "founder_confirmed_official_woodspring_image" };
}

export function classifyApiRenderingGap(row, apiBlock) {
  if (nz(row.externalDisplayStatus).toLowerCase() === "do not display") {
    return apiBlock
      ? "display_status_should_filter_but_api_includes_row"
      : "display_status_filtered_from_api_ok";
  }
  if (!row.hasImage) return "missing_attachment";
  if (!apiBlock) return "row_missing_from_api_blocks";
  if (!nz(apiBlock.imageUrl)) return "attachment_present_api_missing_imageUrl";
  if (!imageLoadingInApi(row, apiBlock)) return "api_imageUrl_not_usable";
  if (isTemporaryAirtableUrl(nz(apiBlock.imageUrl))) {
    return "api_exposes_temporary_airtable_url";
  }
  return "api_image_renders";
}

export function galleryTitleNeedsPatch(row) {
  if (!GALLERY_SLOTS.slice(0, 3).includes(row.slotKey)) return false;
  if (nz(row.externalDisplayStatus).toLowerCase() === "do not display") return false;
  const current = nz(row.title).toLowerCase();
  const target = WOODSPRING_GALLERY_LABELS[row.slotKey];
  if (!target) return false;
  if (!current) return true;
  if (current === target.toLowerCase()) return false;
  return INAPPROPRIATE_GALLERY_LABELS.some((bad) => current.includes(bad));
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

async function listPresentationRowsDetailed(baseId, apiKey, brandRecordId, brandName) {
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
    const imageAtt = f.Image?.[0] || f["Scenario Image"]?.[0];
    const attachmentCount = Array.isArray(f.Image)
      ? f.Image.length
      : Array.isArray(f["Scenario Image"])
        ? f["Scenario Image"].length
        : imageAtt
          ? 1
          : 0;
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      hasImage: Boolean(imageAtt?.url),
      attachmentCount,
      externalDisplayStatus: nz(f["External Display Status"]),
      sortOrder: f["Sort Order"],
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

function registryTableName() {
  return process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE;
}

function buildRegistryCreateFields(row, brandConfig, parentCompany, materializationUrl) {
  const staged = buildWoodspringRegistryStagedAsset({
    row: { ...row, imageUrl: materializationUrl || row.imageUrl },
    brandConfig,
    stagingRunId: STAGING_RUN_ID,
    wrongBrandRisk: null,
  });
  staged.assetStatus = ASSET_STATUS.APPROVED_EXPLORER;
  staged.explorerUsePermission = "Approved For Explorer";
  staged.usageReviewStatus = "Usage Review Complete";
  staged.validationStatus = "Valid for Slot";
  staged.validationNotes = FOUNDER_REVIEW_NOTES;
  staged.reviewNotes = FOUNDER_REVIEW_NOTES;
  staged.sourceNotes = `Linked presentation row ${row.recordId} (${row.slotKey}). ${FOUNDER_SOURCE_NOTES_SUFFIX}`;
  staged.stagingRunId = STAGING_RUN_ID;
  if (!staged.sourceUrl && materializationUrl && !isTemporaryAirtableUrl(materializationUrl)) {
    staged.sourceUrl = materializationUrl;
  }
  return mapStagedToRegistryFields(staged, TARGET_BRAND.recordId, parentCompany);
}

function assessGalleryPremium({ galleryRows, galleryTitlePatches, galleryVisibilityPatches, apiBlocks }) {
  const blockers = [];
  const visibleGallery = galleryRows.filter(
    (r) =>
      GALLERY_SLOTS.slice(0, 3).includes(r.slotKey) &&
      nz(r.externalDisplayStatus).toLowerCase() !== "do not display"
  );
  const apiGallery = (apiBlocks || []).filter((b) => GALLERY_SLOTS.includes(nz(b.slotKey)));

  for (const row of visibleGallery) {
    const apiBlock = apiGallery.find((b) => b.recordId === row.recordId);
    if (!row.hasImage) blockers.push(`visible_gallery_blank:${row.slotKey}`);
    if (!apiBlock?.imageUrl) blockers.push(`visible_gallery_api_missing:${row.slotKey}`);
    const label = nz(row.title) || WOODSPRING_GALLERY_LABELS[row.slotKey];
    if (INAPPROPRIATE_GALLERY_LABELS.some((bad) => nz(label).toLowerCase().includes(bad))) {
      blockers.push(`inappropriate_gallery_label:${row.slotKey}`);
    }
  }

  for (const slot of ["materials.gallery.4", "materials.gallery.5", "materials.gallery.6"]) {
    const row = galleryRows.find((r) => r.slotKey === slot);
    if (!row) continue;
    const hidden =
      nz(row.externalDisplayStatus).toLowerCase() === "do not display" ||
      galleryVisibilityPatches.some((p) => p.recordId === row.recordId);
    const inApi = apiGallery.some((b) => b.recordId === row.recordId);
    if (!hidden) blockers.push(`hidden_gallery_still_visible:${slot}`);
    if (hidden && inApi) blockers.push(`hidden_gallery_still_in_api:${slot}`);
  }

  const urls = visibleGallery
    .map((r) => {
      const b = apiGallery.find((x) => x.recordId === r.recordId);
      return nz(b?.imageUrl || r.imageUrl);
    })
    .filter(Boolean);
  const unique = new Set(urls.map((u) => u.split("?")[0]));
  if (urls.length >= 2 && unique.size < urls.length) {
    blockers.push("duplicate_filler_gallery_images");
  }

  return {
    galleryPremiumEnoughForActiveProfile: blockers.length === 0,
    premiumDisplayBlockers: blockers,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-visual-registry-recovery-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_OFFICIAL_ONLY,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_MOMENTUM,
    APPLY_FLAG_QUARANTINE,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Visual Registry Recovery v33D-R2");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v33D-R2 exists: **${report.v33dR2WriterExists ? "yes" : "no"}**`);
  lines.push(`- v33D exists: **${report.v33dWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Live visual rendering audit");
  for (const row of report.liveVisualRenderingAudit) {
    lines.push(
      `- \`${row.presentationRowId}\` **${row.slot}** — attachment: ${row.imageFieldAttachmentCount}; API: ${row.apiImageUrlStatus}; UI renders: ${row.uiWouldRenderVisibleImage ? "yes" : "no"}; registry: ${row.registryApprovalStatus}; gap: ${row.renderingGap}`
    );
  }
  lines.push("");
  lines.push("## Registry recovery");
  lines.push(`- Patches: **${report.registryPatches.length}**`);
  lines.push(`- Creates: **${report.registryCreates.length}**`);
  lines.push(`- Invalid v2 values avoided: **${report.invalidV2ValueAvoidance ? "yes" : "no"}** (uses validateV32gR1RegistryWritePayload)`);
  lines.push("");
  lines.push("## Gallery");
  lines.push(`- Title patches: **${report.galleryTitlePatches.length}**`);
  lines.push(`- Visibility patches: **${report.galleryVisibilityPatches.length}**`);
  lines.push(`- Premium enough: **${report.galleryPremiumEnoughForActiveProfile ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Partial state repair");
  lines.push(`- Already correct (image + pending registry): **${report.partialStateRepair.alreadyMaterializedPendingRegistry.length}**`);
  lines.push(`- Registry-only repairs: **${report.partialStateRepair.registryOnlyRepairs.length}**`);
  lines.push(`- Image rematerializations: **${report.imageFieldWritesProposed.length}**`);
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  lines.push(`- v33E blockers: ${report.remainingV33eBlockers.join("; ") || "none listed"}`);
  if (report.exactApplyCommand) {
    lines.push("");
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

function loadV33dPriorReport() {
  const p = path.join(ROOT, "reports/brand-explorer-woodspring-visual-completion-writer.json");
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export async function buildBrandExplorerWoodspringVisualRegistryRecoveryWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  officialImagesOnly = false,
  noValidationClaim = false,
  noSourceLibrary = false,
  noSummaryUrl = false,
  noMomentumChanges = false,
  quarantineStaysHidden = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33D-R2 is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug);
  if (!brandConfig) throw new Error("WoodSpring discovery brand config missing");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const parentCompany = nz(brandBasicsBefore?.fields?.["Parent Company"] || "Choice Hotels");

  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load WoodSpring API shape");

  const presentationRows = await listPresentationRowsDetailed(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const registryRaw = await listRegistryRaw(baseId, apiKey, TARGET_BRAND.recordId);
  const registryAssets = registryRaw.map((a) => normalizeRegistryRecordExtended(a));

  const touchedRows = presentationRows.filter(isV33dTouchedRow);
  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const apiById = new Map(apiBlocks.map((b) => [b.recordId, b]));

  const v33dPrior = loadV33dPriorReport();
  const founderGatesReady = founderApproved && officialImagesOnly;

  const liveVisualRenderingAudit = touchedRows.map((row) => {
    const apiBlock = apiById.get(row.recordId);
    const registryAsset = findRegistryForRow(registryAssets, row);
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryAssets);
    const renderingGap = classifyApiRenderingGap(row, apiBlock);
    const qualification = qualifiesForWoodspringFounderApproval(
      row,
      apiBlock,
      brandConfig,
      registryAsset
    );
    return {
      presentationRowId: row.recordId,
      slot: row.slotKey,
      title: row.title,
      externalDisplayStatus: row.externalDisplayStatus || "(active)",
      imageFieldAttachmentCount: row.attachmentCount,
      apiImageUrl: nz(apiBlock?.imageUrl) || null,
      apiImageUrlStatus: apiBlock?.imageUrl
        ? isTemporaryAirtableUrl(apiBlock.imageUrl)
          ? "temporary_airtable"
          : "exposed"
        : "missing",
      imageUrlLoadsInApi: imageLoadingInApi(row, apiBlock),
      uiWouldRenderVisibleImage:
        !isRowQuarantined(row) &&
        nz(row.externalDisplayStatus).toLowerCase() !== "do not display" &&
        imageLoadingInApi(row, apiBlock),
      registryLinkageStatus: registryAsset?.id || null,
      registryApprovalStatus: registryAsset
        ? isRegistryAssetApprovedForExplorer(registryAsset)
          ? "approved"
          : nz(registryAsset.assetStatus) || "pending"
        : "missing",
      defectStatus: assessment.recommendation,
      renderingGap,
      qualifiesForFounderApproval: qualification.qualifies,
      qualificationReason: qualification.reason,
    };
  });

  const registrySchemaFindings = {
    assetStatusAllowed: VAL_ASSET_STATUS,
    explorerUsePermissionAllowed: VAL_EXPLORER_USE_PERMISSION,
    usageReviewStatusAllowed: VAL_USAGE_REVIEW_STATUS,
    approvalPattern: "Everhome v32G-R1 validateV32gR1RegistryWritePayload",
    invalidV2Rejection:
      "validateRegistryWritePayload blocks Approved For Explorer Use and Approved For Explorer on create",
    woodspringApprovedValues: {
      assetStatus: ASSET_STATUS.APPROVED_EXPLORER,
      explorerUsePermission: "Approved For Explorer",
      usageReviewStatus: "Usage Review Complete",
      visualSlotValidationStatus: "Valid for Slot",
    },
  };

  const registryPatches = [];
  const registryCreates = [];
  const registryRecoveryPlan = [];
  const imageFieldWritesProposed = [];
  const galleryTitlePatches = [];
  const galleryVisibilityPatches = [];
  const rowsLeftDeferred = [];
  const partialStateRepair = {
    alreadyMaterializedPendingRegistry: [],
    registryOnlyRepairs: [],
    v33dR1RegistryErrors: v33dPrior?.applyResults?.errors || [],
  };

  for (const row of touchedRows) {
    if (isRowQuarantined(row)) {
      rowsLeftDeferred.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: "quarantined_everhome_stays_hidden",
      });
      continue;
    }
    if (row.slotKey === MOMENTUM_SLOT) continue;

    if (isGalleryHiddenSlot(row.slotKey)) {
      if (nz(row.externalDisplayStatus).toLowerCase() !== "do not display") {
        galleryVisibilityPatches.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields: { "External Display Status": GALLERY_DISPLAY_STATUS_HIDE },
        });
      } else {
        rowsLeftDeferred.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          reason: "already_hidden_gallery_slot",
        });
      }
      continue;
    }

    if (galleryTitleNeedsPatch(row)) {
      galleryTitlePatches.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        fields: { Title: WOODSPRING_GALLERY_LABELS[row.slotKey] },
        previousTitle: row.title,
      });
    }

    const apiBlock = apiById.get(row.recordId);
    const registryAsset = findRegistryForRow(registryAssets, row);
    const qualification = qualifiesForWoodspringFounderApproval(
      row,
      apiBlock,
      brandConfig,
      registryAsset
    );

    if (!qualification.qualifies) {
      if (row.hasImage && registryAsset && !isRegistryAssetApprovedForExplorer(registryAsset)) {
        partialStateRepair.alreadyMaterializedPendingRegistry.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          note: "image present; registry approval blocked by qualification gate",
          reason: qualification.reason,
        });
      }
      if (!row.hasImage && !isGalleryHiddenSlot(row.slotKey)) {
        rowsLeftDeferred.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          reason: qualification.reason,
        });
      }
      continue;
    }

    const materializationUrl = row.imageUrl;
    const registryPatchFields = buildFounderApprovedRegistryPatch({
      asset: registryAsset,
      row,
      brandConfig,
      materializationUrl,
    });
    registryPatchFields[MAP_BRAND_ASSET.stagingRunId] = STAGING_RUN_ID;
    registryPatchFields[MAP_BRAND_ASSET.reviewNotes] = FOUNDER_REVIEW_NOTES;
    registryPatchFields[MAP_VISUAL_SLOT.validationNotes] = FOUNDER_REVIEW_NOTES;

    if (registryAsset) {
      if (isRegistryAssetApprovedForExplorer(registryAsset)) {
        registryRecoveryPlan.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          action: "already_approved",
        });
        continue;
      }
      const validation = validateV32gR1RegistryWritePayload({
        [MAP_BRAND_ASSET.assetName]: registryAsset.assetName,
        [MAP_BRAND_ASSET.brandRecordId]: TARGET_BRAND.recordId,
        ...registryPatchFields,
      });
      if (!validation.valid) {
        rowsLeftDeferred.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          reason: `registry_patch_validation_failed:${validation.errors.join(";")}`,
        });
        continue;
      }
      registryPatches.push({ recordId: registryAsset.id, fields: registryPatchFields, slotKey: row.slotKey });
      partialStateRepair.registryOnlyRepairs.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        registryRecordId: registryAsset.id,
      });
      registryRecoveryPlan.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        action: "patch_registry_approval",
        registryRecordId: registryAsset.id,
      });
    } else {
      const createFields = buildRegistryCreateFields(row, brandConfig, parentCompany, materializationUrl);
      const validation = validateV32gR1RegistryWritePayload(createFields);
      if (!validation.valid) {
        rowsLeftDeferred.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          reason: `registry_create_validation_failed:${validation.errors.join(";")}`,
        });
        continue;
      }
      registryCreates.push({ slotKey: row.slotKey, presentationRowId: row.recordId, fields: createFields });
      partialStateRepair.registryOnlyRepairs.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        registryRecordId: null,
        action: "create_registry",
      });
      registryRecoveryPlan.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        action: "create_registry_approval",
      });
    }

    if (!row.hasImage && founderGatesReady) {
      const imageFields = { Image: [{ url: materializationUrl }] };
      imageFieldWritesProposed.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        fields: imageFields,
        reason: "missing_attachment_rematerialize",
      });
    }
  }

  const galleryRows = presentationRows.filter((r) => GALLERY_SLOTS.includes(r.slotKey));
  const premium = assessGalleryPremium({
    galleryRows,
    galleryTitlePatches,
    galleryVisibilityPatches,
    apiBlocks,
  });

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!founderApproved) applyBlockers.push("missing_founder_approved_woodspring_official_images");
    if (!officialImagesOnly) applyBlockers.push("missing_confirm_official_source_images_only");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noSourceLibrary) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noSummaryUrl) applyBlockers.push("missing_confirm_no_summary_url_field");
    if (!noMomentumChanges) applyBlockers.push("missing_confirm_no_momentum_changes");
    if (!quarantineStaysHidden) applyBlockers.push("missing_confirm_quarantined_everhome_row_stays_hidden");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }
  for (const b of premium.premiumDisplayBlockers) {
    applyBlockers.push(`premium_display:${b}`);
  }
  if (registryPatches.some((p) => p.fields["Summary URL"])) {
    applyBlockers.push("summary_url_in_registry_patch");
  }

  const hasWork =
    registryPatches.length > 0 ||
    registryCreates.length > 0 ||
    galleryTitlePatches.length > 0 ||
    galleryVisibilityPatches.length > 0 ||
    imageFieldWritesProposed.length > 0;

  const dryRunClean =
    applyBlockers.filter((b) => !b.startsWith("missing_")).length === 0 && hasWork;

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

  const registryRecoveryCount = registryPatches.length + registryCreates.length;
  const expectedFinalQaResult = registryRecoveryCount > 0
    ? `projected_improvement_from_${finalQaReport?.summary?.overallStatus || "unknown"}_after_registry_approval`
    : finalQaReport?.summary?.overallStatus || "unknown";
  const expectedCompleteBuildResult = registryRecoveryCount > 0
    ? `projected_readyForActiveProfile_after_${registryRecoveryCount}_registry_writes`
    : completeBuildReport?.readyForActiveProfile
      ? "ready"
      : completeBuildReport?.blockers?.join("; ") || "blocked";
  const expectedVisualDefectResult = premium.galleryPremiumEnoughForActiveProfile
    ? `projected_improvement_from_${visualDefectReport?.summary?.defectCount ?? "unknown"}_defects`
    : visualDefectReport?.summary?.defectCount != null
      ? `${visualDefectReport.summary.defectCount} defects`
      : "unknown";

  const remainingV33eBlockers = [
    "overview.why_value empty bullets stewardship",
    "final fact formatting cleanup",
    "standard detail population",
    "editorial copy cleanup beyond visuals",
  ];

  let airtableModified = false;
  const applyResults = {
    registryPatched: [],
    registryCreated: [],
    galleryTitlesPatched: [],
    gallerySlotsHidden: [],
    imagesRematerialized: [],
    errors: [],
  };

  const canApply =
    apply &&
    approveBatch &&
    founderApproved &&
    officialImagesOnly &&
    noValidationClaim &&
    noSourceLibrary &&
    noSummaryUrl &&
    noMomentumChanges &&
    quarantineStaysHidden &&
    woodspringOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    const registryTable = registryTableName();
    for (const patch of registryPatches) {
      try {
        const validation = validateV32gR1RegistryWritePayload({
          [MAP_BRAND_ASSET.assetName]: "WoodSpring Suites",
          [MAP_BRAND_ASSET.brandRecordId]: TARGET_BRAND.recordId,
          ...patch.fields,
        });
        if (!validation.valid) throw new Error(validation.errors.join(";"));
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          registryTable,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Registry PATCH failed: ${res.status}`);
        applyResults.registryPatched.push({ recordId: patch.recordId, slotKey: patch.slotKey });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "registry_patch",
          recordId: patch.recordId,
          message: err.message,
        });
      }
    }

    for (const create of registryCreates) {
      try {
        const validation = validateV32gR1RegistryWritePayload(create.fields);
        if (!validation.valid) throw new Error(validation.errors.join(";"));
        const { res, json } = await airtableFetch(baseId, apiKey, registryTable, {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `Registry POST failed: ${res.status}`);
        applyResults.registryCreated.push({
          recordId: json.id,
          slotKey: create.slotKey,
          presentationRowId: create.presentationRowId,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "registry_create",
          slotKey: create.slotKey,
          message: err.message,
        });
      }
    }

    for (const titlePatch of galleryTitlePatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: titlePatch.fields, typecast: true }) },
          titlePatch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Gallery title PATCH failed: ${res.status}`);
        applyResults.galleryTitlesPatched.push({
          recordId: titlePatch.recordId,
          slotKey: titlePatch.slotKey,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "gallery_title",
          recordId: titlePatch.recordId,
          message: err.message,
        });
      }
    }

    for (const visPatch of galleryVisibilityPatches) {
      if (visPatch.recordId === QUARANTINED_SCENARIO3_RECORD_ID) continue;
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: visPatch.fields, typecast: true }) },
          visPatch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Gallery hide PATCH failed: ${res.status}`);
        applyResults.gallerySlotsHidden.push({
          recordId: visPatch.recordId,
          slotKey: visPatch.slotKey,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "gallery_visibility",
          recordId: visPatch.recordId,
          message: err.message,
        });
      }
    }

    for (const imageWrite of imageFieldWritesProposed) {
      if (imageWrite.recordId === QUARANTINED_SCENARIO3_RECORD_ID) continue;
      if (imageWrite.slotKey === MOMENTUM_SLOT) continue;
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: imageWrite.fields, typecast: true }) },
          imageWrite.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Image PATCH failed: ${res.status}`);
        applyResults.imagesRematerialized.push({
          recordId: imageWrite.recordId,
          slotKey: imageWrite.slotKey,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "image_rematerialize",
          recordId: imageWrite.recordId,
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
    v33dR2WriterExists: v33dR2WriterExists(),
    v33dWriterExists: v33dWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    liveVisualRenderingAudit,
    registrySchemaFindings,
    invalidV2ValueAvoidance: true,
    registryRecoveryPlan,
    registryPatches,
    registryCreates,
    openingImageRecoveryPlan: registryRecoveryPlan.filter((p) => p.slotKey === OPENINGS_SLOT),
    scenarioImageRecoveryPlan: registryRecoveryPlan.filter((p) => SCENARIO_SLOTS.includes(p.slotKey)),
    galleryRecoveryPlan: {
      titlePatches: galleryTitlePatches,
      visibilityPatches: galleryVisibilityPatches,
      labels: WOODSPRING_GALLERY_LABELS,
    },
    hiddenGallerySlotVerification: GALLERY_SLOTS.slice(3).map((slotKey) => {
      const row = galleryRows.find((r) => r.slotKey === slotKey);
      return {
        slotKey,
        recordId: row?.recordId || null,
        externalDisplayStatus: row?.externalDisplayStatus || null,
        inApi: apiBlocks.some((b) => b.slotKey === slotKey),
        willHide: galleryVisibilityPatches.some((p) => p.slotKey === slotKey),
      };
    }),
    partialStateRepair,
    imageFieldWritesProposed,
    rowsLeftDeferred,
    galleryTitlePatches,
    galleryVisibilityPatches,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    summaryUrlProtectionConfirmed: true,
    airtableModified,
    applyResults: canApply ? applyResults : null,
    galleryPremiumEnoughForActiveProfile: premium.galleryPremiumEnoughForActiveProfile,
    premiumDisplayBlockers: premium.premiumDisplayBlockers,
    dryRunClean,
    applyBlockers,
    expectedFinalQaResult,
    expectedCompleteBuildResult,
    expectedVisualDefectResult,
    remainingV33eBlockers,
    currentFinalQa: finalQaReport?.summary || null,
    currentCompleteBuild: {
      readyForActiveProfile: completeBuildReport?.readyForActiveProfile ?? null,
      blockers: completeBuildReport?.blockers || [],
    },
    currentVisualDefects: visualDefectReport?.summary || null,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
