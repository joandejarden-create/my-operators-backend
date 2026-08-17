/**
 * Brand Explorer WoodSpring Six-Image Gallery Completion v33H.
 *
 * Completes materials.gallery.1–6 with six visible, hoteldam-backed property
 * images materialized on presentation Image fields (registry linkage alone does
 * not render in Brand Explorer UI).
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-six-image-gallery-completion-writer-v33H.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  listRegistryAssetsForBrand,
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS, SOURCE_BASIS } from "./brand-asset-pr-package-governance.js";
import {
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  buildWoodspringRegistryStagedAsset,
  TARGET_BRAND,
} from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import {
  isDoNotUseRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { validateV32gR1RegistryWritePayload } from "./brand-explorer-everhome-existing-image-approval-recognition-writer.js";
import {
  buildFounderApprovedRegistryPatch,
  GALLERY_DISPLAY_STATUS_HIDE,
} from "./brand-explorer-woodspring-visual-completion-writer.js";
import { MAP_VISUAL_SLOT, VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import {
  WOODSPRING_PROPERTY_CATALOG,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-woodspring-real-property-examples-writer.js";
import {
  assignWoodspringGalleryImagesFromPool,
  classifyPropertyExampleImage,
  isGenericBrandOrLifestyleImageUrl,
  isHoteldamPropertyImageUrl,
  isLogoImageUrl,
} from "./brand-explorer-footprint-opening-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { ACTIVE_PROFILE_GALLERY_MINIMUM } from "./brand-explorer-brand-asset-image-governance.js";

export const WRITER_VERSION = "v33H";
export const STAGING_RUN_ID = "v33H-woodspring-six-image-gallery-completion";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-six-image-gallery-completion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-six-image-gallery-completion-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-six-image-gallery-completion-writer-v33H.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33H-woodspring-six-image-gallery-completion";
export const APPLY_FLAG_FOUNDER = "--founder-approved-woodspring-gallery-hotel-images";
export const APPLY_FLAG_OFFICIAL_ONLY = "--confirm-official-source-images-only";
export const APPLY_FLAG_MINIMUM_SIX = "--confirm-minimum-six-visible-gallery-images";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_OTHER_SECTIONS =
  "--confirm-no-openings-momentum-proof-standard-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export { TARGET_BRAND };

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const PROOF_SLOT_RE = /^overview\.proof/;
const STANDARDS_SLOT_RE = /^standards\./;

export const GALLERY_SLOT_TARGETS = Object.freeze([
  {
    slotKey: "materials.gallery.1",
    recordId: "rechUn7nwlxjW1jyV",
    title: "Exterior / Prototype",
    kind: "exterior",
    propertyKey: "nc936",
  },
  {
    slotKey: "materials.gallery.2",
    recordId: "recXfIGZUrwap6AIK",
    title: "Guest Room",
    kind: "guest_room",
    propertyKey: "ncb10",
  },
  {
    slotKey: "materials.gallery.3",
    recordId: "recJokIWQxU64gVsl",
    title: "Kitchen-Equipped Suite",
    kind: "kitchen",
    propertyKey: "flf21",
  },
  {
    slotKey: "materials.gallery.4",
    recordId: "rec9S14MX9GUkNCIC",
    title: "Suite Work Area",
    kind: "suite_work",
    propertyKey: "nc936",
  },
  {
    slotKey: "materials.gallery.5",
    recordId: "recNeGpXjsVlTrbaf",
    title: "In-Room Kitchen Detail",
    kind: "kitchen_detail",
    propertyKey: "ncb10",
  },
  {
    slotKey: "materials.gallery.6",
    recordId: "receoBDH1wHcAM3ZD",
    title: "Extended-Stay Room Detail",
    kind: "extended_stay",
    propertyKey: "flf21",
  },
]);

const PROTECTED_SLOTS = new Set([
  OPENINGS_SLOT,
  MOMENTUM_SLOT,
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.proof.5",
  "overview.proof.6",
  "overview.proof_operator",
  "standards.last_reviewed",
  "standards.source_confidence",
  "standards.conversion",
]);

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
]);

const FOUNDER_REVIEW_NOTES =
  "v33H founder-approved — six visible WoodSpring gallery hotel/property images with durable hoteldam sources.";
const FOUNDER_SOURCE_NOTES_SUFFIX =
  "Founder-approved gallery hotel image; durable Choice hoteldam source on file.";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  return nz(v).length > 0;
}

function normalizeUrlKey(url) {
  return nz(url).split("?")[0].toLowerCase();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function propertyIdFromKey(propertyKey) {
  return nz(propertyKey).toLowerCase();
}

function catalogForPropertyKey(propertyKey) {
  const pageSuffix = `/${propertyKey}`;
  return WOODSPRING_PROPERTY_CATALOG.find((c) =>
    nz(c.sourcePageUrl).toLowerCase().endsWith(pageSuffix)
  );
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v33hWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-six-image-gallery-completion-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v33H`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33H supports WoodSpring Suites only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function findRegistryForRow(registryAssets, row) {
  if (row.registryLink) {
    const byLink = registryAssets.find((a) => a.id === row.registryLink);
    if (byLink) return byLink;
  }
  const byNotes = registryAssets.find(
    (a) =>
      parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId ||
      nz(a.sourceNotes).includes(row.recordId)
  );
  if (byNotes) return byNotes;
  return (
    registryAssets.find(
      (a) =>
        nz(a.recommendedExplorerSlot) === row.slotKey &&
        (nz(a.sourceNotes).includes(row.recordId) || nz(a.assetName).includes(row.slotKey))
    ) || null
  );
}

function validateGalleryPresentationPatch(fields) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
  }
  if (fields["Summary URL"] || fields["View Summary URL"]) {
    errors.push("summary_url_blocked");
  }
  const imageUrl = fields.Image?.[0]?.url || "";
  if (fields.Image) {
    if (isTemporaryAirtableUrl(imageUrl)) errors.push("temporary_airtable_image_url");
    if (isLogoImageUrl(imageUrl)) errors.push("logo_image");
    if (isGenericBrandOrLifestyleImageUrl(imageUrl)) errors.push("generic_brand_lifestyle_image");
    if (imageUrl && !isHoteldamPropertyImageUrl(imageUrl)) {
      errors.push("non_hoteldam_durable_source");
    }
  }
  const display = fields["External Display Status"];
  if (
    display != null &&
    display !== null &&
    nz(display).toLowerCase() === GALLERY_DISPLAY_STATUS_HIDE.toLowerCase()
  ) {
    errors.push("must_not_hide_gallery_slot");
  }
  return errors;
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

function buildRegistryFieldsForGallery({
  row,
  catalog,
  brandConfig,
  parentCompany,
  imagePlan,
  slotKey,
  title,
}) {
  const propertyName = catalog?.propertyName || "WoodSpring Suites";
  const assetName = `${propertyName} — ${title} — ${slotKey}`;
  const brandMatchNotes = `WoodSpring gallery hoteldam image (${title}) for ${propertyName}; verified hotel/property photography.`;
  const validationNotes = `${FOUNDER_REVIEW_NOTES} Gallery ${title} from official Choice hoteldam CDN.`;
  const staged = buildWoodspringRegistryStagedAsset({
    row: { ...row, title, imageUrl: imagePlan.imageUrl, body: row.body },
    brandConfig,
    stagingRunId: STAGING_RUN_ID,
    wrongBrandRisk: null,
  });
  staged.assetName = assetName;
  staged.assetStatus = ASSET_STATUS.APPROVED_EXPLORER;
  staged.explorerUsePermission = "Approved For Explorer";
  staged.usageReviewStatus = "Usage Review Complete";
  staged.validationStatus = "Valid for Slot";
  staged.validationNotes = validationNotes;
  staged.reviewNotes = `${FOUNDER_REVIEW_NOTES} ${brandMatchNotes}`;
  staged.sourceNotes = `Linked presentation row ${row.recordId} (${slotKey}). ${FOUNDER_SOURCE_NOTES_SUFFIX} Brand Match: ${brandMatchNotes}`;
  staged.stagingRunId = STAGING_RUN_ID;
  staged.relatedPropertyName = propertyName;
  staged.propertyConfirmed = "Yes";
  staged.calaRelevant = "No";
  staged.slotPurpose = `WoodSpring ${slotKey} — ${title} hotel/property photography`;
  staged.explorerSection = VISUAL_SLOT.IMAGE_GALLERY;
  staged.sourcePageUrl = imagePlan.imageSourcePageUrl || catalog?.sourcePageUrl || "";
  staged.sourceUrl = imagePlan.imageUrl;
  staged.sourceBasis = SOURCE_BASIS.RENDERED_OFFICIAL;
  staged.recommendedExplorerSlot = slotKey;
  return mapStagedToRegistryFields(staged, TARGET_BRAND.recordId, parentCompany);
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
    const imageAtt = f.Image?.[0];
    const registryLinks = Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"] : [];
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"],
      externalDisplayStatus: nz(f["External Display Status"]),
      visible:
        nz(f["External Display Status"]).toLowerCase() !== GALLERY_DISPLAY_STATUS_HIDE.toLowerCase(),
      hasImage: Array.isArray(f.Image) && f.Image.length > 0,
      imageAttachmentCount: Array.isArray(f.Image) ? f.Image.length : 0,
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
      registryLink: registryLinks[0] || null,
      registryLinkIds: registryLinks,
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

async function fetchBrandApiShape(brandRecordId) {
  const req = { query: { brandId: brandRecordId, refresh: "1" }, headers: {} };
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

function apiBlockForSlot(brandApi, slotKey, recordId = "") {
  const blocks = brandApi?.brandExplorer?.blocks || [];
  if (recordId) {
    return blocks.find((b) => b.recordId === recordId || b.id === recordId) || null;
  }
  return blocks.find((b) => nz(b.slotKey) === slotKey) || null;
}

function auditGallerySlot(row, apiBlock, registryAsset) {
  const registrySourceUrl = registryAsset?.sourceUrl || "";
  const apiImageUrl = nz(apiBlock?.imageUrl);
  const classification = classifyPropertyExampleImage(row.imageUrl || apiImageUrl, {
    registrySourceUrl,
    registryNotes: [registryAsset?.sourceNotes, registryAsset?.reviewNotes].filter(Boolean).join("\n"),
  });
  const imageSource = row.imageUrl
    ? isTemporaryAirtableUrl(row.imageUrl)
      ? "temporary_airtable_attachment"
      : row.imageUrl
    : registrySourceUrl
      ? "registry_source_url_only"
      : "missing";

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    title: row.title,
    externalDisplayStatus: row.externalDisplayStatus || "(empty)",
    imageAttachmentCount: row.imageAttachmentCount,
    apiImageUrl: apiImageUrl || "(missing)",
    currentImageSource: imageSource,
    rendersInUi: hasVal(apiImageUrl),
    isHotelPhotography: classification.isHotelPhotography,
    isLogo: classification.isLogo,
    isLifestyle: classification.isLifestyle,
    isGenericBrand: classification.isGenericBrand,
    registryLinkage: registryAsset?.id || row.registryLink || "(none)",
    registryApprovalStatus: registryAsset
      ? isRegistryAssetApprovedForExplorer(registryAsset)
        ? "approved"
        : nz(registryAsset.assetStatus) || "pending"
      : "none",
    visible: row.visible,
  };
}

function buildDiscoveryAssignments() {
  const allPropertyIds = WOODSPRING_PROPERTY_CATALOG.map((c) =>
    propertyIdFromKey(c.sourcePageUrl.split("/").pop())
  );
  return GALLERY_SLOT_TARGETS.map((target) => {
    const catalog = catalogForPropertyKey(target.propertyKey);
    const propertyId = target.propertyKey;
    const fallbackPropertyIds = allPropertyIds.filter((pid) => pid !== propertyId);
    return {
      slotKey: target.slotKey,
      recordId: target.recordId,
      title: target.title,
      kind: target.kind,
      propertyId,
      sourcePageUrl: catalog?.sourcePageUrl || "",
      fallbackPropertyIds,
    };
  });
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-six-image-gallery-completion-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_OFFICIAL_ONLY,
    APPLY_FLAG_MINIMUM_SIX,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_OTHER_SECTIONS,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Six-Image Gallery Completion v33H");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Current gallery audit");
  for (const row of report.currentGalleryAudit) {
    lines.push(
      `- \`${row.recordId}\` **${row.slotKey}** — title: ${row.title}; display: ${row.externalDisplayStatus}; attachments: ${row.imageAttachmentCount}; API imageUrl: ${row.apiImageUrl}; source: ${row.currentImageSource}; UI: ${row.rendersInUi ? "yes" : "no"}; hotel photo: ${row.isHotelPhotography ? "yes" : "no"}; registry: ${row.registryLinkage} (${row.registryApprovalStatus})`
    );
  }
  lines.push("");
  lines.push("## Six selected gallery images");
  for (const img of report.selectedGalleryImages) {
    lines.push(
      `- **${img.slotKey}** ${img.title} — ${img.ok ? img.imageUrl : `MISSING (${img.error})`}`
    );
  }
  lines.push("");
  lines.push("## Gallery before / after");
  for (const row of report.galleryBeforeAfter) {
    lines.push(
      `- \`${row.recordId}\` **${row.slotKey}** — visible: ${row.before.visible} → ${row.after.visible}; image: ${row.before.hasImage ? "yes" : "no"} → ${row.after.hasImage ? "yes" : "no"}; title: ${row.after.title}`
    );
  }
  lines.push("");
  lines.push(`## Registry creates: ${report.registryCreates.length}; patches: ${report.registryPatches.length}`);
  lines.push("");
  lines.push(`## Six visible gallery images (projected): **${report.projectedVisibleGalleryCount}/6**`);
  lines.push(`## Six API imageUrls (projected): **${report.projectedApiImageUrlCount}/6**`);
  lines.push(`## Logo/lifestyle/generic remain visible: **${report.logoOrGenericRemainVisible ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Expected QA");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
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

export async function buildBrandExplorerWoodspringSixImageGalleryCompletionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  officialImagesOnly = false,
  minimumSixConfirmed = false,
  noValidationClaim = false,
  noSummaryUrl = false,
  noOtherSectionChanges = false,
  woodspringOnly = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = getDiscoveryBrandConfig(target.slug);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const parentCompany = nz(brandBasicsBefore?.fields?.["Parent Company"]) || "Choice Hotels International";

  const [presentationRows, registryAssetsRaw, brandApiBefore] = await Promise.all([
    listPresentationRows(baseId, apiKey, target.recordId, target.name),
    listRegistryAssetsForBrand(target.recordId).catch(() => []),
    fetchBrandApiShape(target.recordId),
  ]);

  const registryAssets = registryAssetsRaw.map(normalizeRegistryRecordExtended);
  const galleryRows = presentationRows.filter((r) =>
    GALLERY_SLOT_TARGETS.some((g) => g.slotKey === r.slotKey)
  );

  const currentGalleryAudit = GALLERY_SLOT_TARGETS.map((target) => {
    const row =
      galleryRows.find((r) => r.recordId === target.recordId) ||
      galleryRows.find((r) => r.slotKey === target.slotKey) ||
      null;
    if (!row) {
      return {
        recordId: target.recordId,
        slotKey: target.slotKey,
        title: target.title,
        externalDisplayStatus: "(missing row)",
        imageAttachmentCount: 0,
        apiImageUrl: "(missing)",
        currentImageSource: "missing",
        rendersInUi: false,
        isHotelPhotography: false,
        isLogo: false,
        isLifestyle: false,
        isGenericBrand: false,
        registryLinkage: "(none)",
        registryApprovalStatus: "none",
        visible: false,
      };
    }
    const apiBlock = apiBlockForSlot(brandApiBefore, target.slotKey, row.recordId);
    const registryAsset = findRegistryForRow(registryAssets, row);
    return auditGallerySlot(row, apiBlock, registryAsset);
  });

  const discovery = await assignWoodspringGalleryImagesFromPool(
    GALLERY_SLOT_TARGETS,
    WOODSPRING_PROPERTY_CATALOG
  );
  const selectedGalleryImages = discovery.assignments;

  const presentationPatches = [];
  const registryPatches = [];
  const registryCreates = [];
  const registryLinkPatches = [];
  const safetyBlockers = [];
  const galleryBeforeAfter = [];

  for (const assignment of discovery.assignments) {
    const targetMeta = GALLERY_SLOT_TARGETS.find((g) => g.slotKey === assignment.slotKey);
    const row =
      galleryRows.find((r) => r.recordId === targetMeta?.recordId) ||
      galleryRows.find((r) => r.slotKey === assignment.slotKey);
    if (!row) {
      safetyBlockers.push(`missing_gallery_row:${assignment.slotKey}`);
      continue;
    }
    if (PROTECTED_SLOTS.has(row.slotKey) && !assignment.slotKey.startsWith("materials.gallery")) {
      safetyBlockers.push(`protected_slot:${row.slotKey}`);
      continue;
    }

    const catalog = catalogForPropertyKey(targetMeta.propertyKey);
    const registryAsset = findRegistryForRow(registryAssets, row);
    const before = {
      visible: row.visible,
      hasImage: row.hasImage,
      title: row.title,
      imageUrl: row.imageUrl,
    };

    if (!assignment.ok) {
      safetyBlockers.push(`no_safe_image:${assignment.slotKey}`);
      galleryBeforeAfter.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        before,
        after: { ...before, visible: false, hasImage: false },
        blocked: true,
      });
      continue;
    }

    const imagePlan = {
      imageUrl: assignment.imageUrl,
      imageKind: assignment.imageKind,
      imageSource: assignment.imageSource,
      imageSourcePageUrl: assignment.sourcePageUrl || catalog?.sourcePageUrl || "",
    };

    const fields = {
      Title: targetMeta.title,
      Image: [{ url: imagePlan.imageUrl }],
      "External Display Status": null,
    };

    const patchErrors = validateGalleryPresentationPatch(fields);
    if (patchErrors.length) {
      safetyBlockers.push(`gallery_patch_validation:${row.recordId}:${patchErrors.join(";")}`);
    } else {
      const needsPatch =
        !row.visible ||
        row.title !== targetMeta.title ||
        normalizeUrlKey(row.imageUrl) !== normalizeUrlKey(imagePlan.imageUrl) ||
        !row.hasImage;
      if (needsPatch) {
        presentationPatches.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields,
          reason: "complete_six_visible_gallery_hotel_images",
          imageSource: imagePlan.imageSource,
          imageSourcePageUrl: imagePlan.imageSourcePageUrl,
        });
      }
    }

    const registryFields = buildRegistryFieldsForGallery({
      row,
      catalog,
      brandConfig,
      parentCompany,
      imagePlan,
      slotKey: row.slotKey,
      title: targetMeta.title,
    });
    const validation = validateV32gR1RegistryWritePayload({
      [MAP_BRAND_ASSET.assetName]: registryFields[MAP_BRAND_ASSET.assetName],
      [MAP_BRAND_ASSET.brandRecordId]: target.recordId,
      ...registryFields,
    });
    if (!validation.valid) {
      safetyBlockers.push(`registry_validation:${row.recordId}:${validation.errors.join(";")}`);
    } else if (registryAsset && !isDoNotUseRecord(registryAsset)) {
      registryPatches.push({
        recordId: registryAsset.id,
        presentationRecordId: row.recordId,
        slotKey: row.slotKey,
        fields: {
          ...buildFounderApprovedRegistryPatch({
            asset: registryAsset,
            row: { ...row, slotKey: row.slotKey, title: targetMeta.title },
            brandConfig,
            materializationUrl: imagePlan.imageUrl,
          }),
          [MAP_BRAND_ASSET.assetName]: registryFields[MAP_BRAND_ASSET.assetName],
          [MAP_BRAND_ASSET.sourcePageUrl]: imagePlan.imageSourcePageUrl,
          [MAP_BRAND_ASSET.sourceUrl]: imagePlan.imageUrl,
          [MAP_VISUAL_SLOT.relatedPropertyName]: catalog?.propertyName || "",
          [MAP_VISUAL_SLOT.propertyConfirmed]: "Yes",
          [MAP_VISUAL_SLOT.validationNotes]: registryFields[MAP_VISUAL_SLOT.validationNotes],
          [MAP_BRAND_ASSET.reviewNotes]: registryFields[MAP_BRAND_ASSET.reviewNotes],
          [MAP_BRAND_ASSET.sourceNotes]: registryFields[MAP_BRAND_ASSET.sourceNotes],
          [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
        },
      });
      if (!row.registryLinkIds?.includes(registryAsset.id)) {
        registryLinkPatches.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields: { "Brand Asset Registry": [registryAsset.id] },
          reason: "link_gallery_to_canonical_registry_asset",
          canonicalRegistryId: registryAsset.id,
        });
      }
    } else {
      registryCreates.push({
        presentationRecordId: row.recordId,
        slotKey: row.slotKey,
        fields: registryFields,
        projectedRegistryLink: true,
      });
    }

    galleryBeforeAfter.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      before,
      after: {
        visible: true,
        hasImage: true,
        title: targetMeta.title,
        imageUrl: imagePlan.imageUrl,
      },
      blocked: false,
    });
  }

  const touchesProtectedSlot = presentationPatches.some((p) => PROTECTED_SLOTS.has(p.slotKey));
  if (touchesProtectedSlot) {
    safetyBlockers.push("would_modify_protected_non_gallery_slot");
  }

  const distinctSafeImages = selectedGalleryImages.filter((i) => i.ok).length;
  if (distinctSafeImages < ACTIVE_PROFILE_GALLERY_MINIMUM) {
    safetyBlockers.push(`fewer_than_${ACTIVE_PROFILE_GALLERY_MINIMUM}_safe_gallery_images`);
  }

  const projectedVisibleGalleryCount = galleryBeforeAfter.filter((g) => g.after.visible).length;
  const projectedApiImageUrlCount = galleryBeforeAfter.filter(
    (g) => g.after.hasImage && g.after.imageUrl
  ).length;

  if (projectedVisibleGalleryCount < ACTIVE_PROFILE_GALLERY_MINIMUM) {
    safetyBlockers.push("fewer_than_six_visible_gallery_rows_after_apply");
  }
  if (projectedApiImageUrlCount < ACTIVE_PROFILE_GALLERY_MINIMUM) {
    safetyBlockers.push("fewer_than_six_api_image_urls_after_apply");
  }

  const logoOrGenericRemainVisible = galleryBeforeAfter.some((g) => {
    if (!g.after.imageUrl) return false;
    const cls = classifyPropertyExampleImage(g.after.imageUrl);
    return cls.isLogo || cls.isGenericBrand || cls.isLifestyle;
  });
  if (logoOrGenericRemainVisible) {
    safetyBlockers.push("logo_or_lifestyle_or_generic_would_remain_visible");
  }

  const founderGatesReady =
    approveBatch &&
    founderApproved &&
    officialImagesOnly &&
    minimumSixConfirmed &&
    noValidationClaim &&
    noSummaryUrl &&
    noOtherSectionChanges &&
    woodspringOnly;

  const dryRunClean = safetyBlockers.length === 0 && distinctSafeImages >= ACTIVE_PROFILE_GALLERY_MINIMUM;
  const canApply = apply && founderGatesReady && dryRunClean;

  let airtableModified = false;
  const applyResults = {
    presentationUpdated: [],
    registryPatched: [],
    registryCreated: [],
    registryLinked: [],
    errors: [],
  };

  if (canApply) {
    for (const patch of registryPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          registryTableName(),
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Registry PATCH failed: ${res.status}`);
        applyResults.registryPatched.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }

    for (const create of registryCreates) {
      try {
        const { res, json } = await airtableFetch(baseId, apiKey, registryTableName(), {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `Registry POST failed: ${res.status}`);
        applyResults.registryCreated.push(json.id);
        if (create.projectedRegistryLink) {
          registryLinkPatches.push({
            recordId: create.presentationRecordId,
            slotKey: create.slotKey,
            fields: { "Brand Asset Registry": [json.id] },
            reason: "link_new_registry_asset",
            canonicalRegistryId: json.id,
          });
        }
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          presentationRecordId: create.presentationRecordId,
          message: err.message,
        });
      }
    }

    for (const patch of [...presentationPatches, ...registryLinkPatches]) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Presentation PATCH failed: ${res.status}`);
        applyResults.presentationUpdated.push(patch.recordId);
        if (patch.fields["Brand Asset Registry"]) applyResults.registryLinked.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
  } else if (apply && !founderGatesReady) {
    safetyBlockers.push("missing_founder_apply_gates");
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(target.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  if (apply && !companyValidatedUntouched) {
    safetyBlockers.push("company_validated_would_change");
  }

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualDefectReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);

  const applyBlockers = [...new Set(safetyBlockers)];
  const expectedFinalQaResult = dryRunClean
    ? "projected_ready_or_almost_ready_after_six_gallery_images"
    : finalQaReport?.brandReports?.find((b) => b.brand?.slug === target.slug)?.scores
        ?.overallActiveProfileReadiness || "unknown";
  const expectedCompleteBuildResult = dryRunClean
    ? "projected_active_profile_ready_with_six_visible_gallery_images"
    : completeBuildReport?.brandResults?.find((b) => b.brand?.slug === target.slug)
        ?.readyForActiveProfile
      ? "ready"
      : "blocked_or_almost_ready";
  const expectedVisualDefectResult = dryRunClean
    ? "projected_six_gallery_cards_with_imageUrl_no_hidden_slots"
    : `${visualDefectReport?.summary?.defectCount ?? "?"} defects`;

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply-blocked") : "dry-run",
    brand: target,
    dryRunClean,
    companyValidatedUntouched,
    airtableModified,
    currentGalleryAudit,
    selectedGalleryImages,
    imageSourceUrls: selectedGalleryImages.filter((i) => i.ok).map((i) => i.imageUrl),
    sourcePageUrls: selectedGalleryImages
      .filter((i) => i.ok)
      .map((i) => ({ slotKey: i.slotKey, sourcePageUrl: i.sourcePageUrl })),
    galleryImagePool: discovery.imagePool || [],
    galleryPoolSize: discovery.poolSize || 0,
    galleryBeforeAfter,
    registryCreates,
    registryPatches,
    registryLinkPatches,
    presentationPatches,
    projectedVisibleGalleryCount,
    projectedApiImageUrlCount,
    logoOrGenericRemainVisible,
    distinctSafeImages,
    applyBlockers,
    applyResults,
    expectedFinalQaResult,
    expectedCompleteBuildResult,
    expectedVisualDefectResult,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    activeProfileGalleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
  };

  report.markdown = buildMarkdown(report);
  return report;
}
