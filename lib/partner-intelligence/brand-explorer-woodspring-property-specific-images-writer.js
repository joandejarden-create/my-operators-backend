/**
 * Brand Explorer WoodSpring Property-Specific Hotel Image Correction v33C-R2.
 *
 * Replaces generic brand / lifestyle / logo imagery on footprint.openings property
 * examples with hoteldam property photography, hides cards without durable hotel images,
 * and corrects gallery rows that show non-hotel graphics.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-property-specific-images-writer-v33C-R2.md
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
  findRegistryAssetForPresentationRow,
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
import { buildFounderApprovedRegistryPatch } from "./brand-explorer-woodspring-visual-completion-writer.js";
import { MAP_VISUAL_SLOT, VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import {
  WOODSPRING_PROPERTY_CATALOG,
  buildPropertyOpeningTitle,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-woodspring-real-property-examples-writer.js";
import {
  classifyPropertyExampleImage,
  isGenericBrandOrLifestyleImageUrl,
  isHoteldamPropertyImageUrl,
  isLogoImageUrl,
  isPropertyExampleTitle,
  resolvePropertySpecificHotelImage,
} from "./brand-explorer-footprint-opening-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33C-R2";
export const STAGING_RUN_ID = "v33C-R2-woodspring-property-specific-images";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-property-specific-images-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-property-specific-images-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-property-specific-images-writer-v33C-R2.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33C-R2-woodspring-property-specific-images";
export const APPLY_FLAG_FOUNDER = "--founder-approved-woodspring-property-specific-hotel-images";
export const APPLY_FLAG_OFFICIAL_ONLY = "--confirm-official-source-images-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_OTHER_SECTIONS = "--confirm-no-momentum-proof-standard-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export { TARGET_BRAND };

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const HIDE_DISPLAY_STATUS = "Do Not Display";

const GALLERY_SLOTS = Object.freeze([
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
]);

const PROTECTED_SLOTS = new Set([
  "footprint.momentum",
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
  "v33C-R2 founder-approved — property-specific hotel/property photography for Explorer openings.";
const FOUNDER_SOURCE_NOTES_SUFFIX =
  "Founder-approved property-specific hotel image; durable Choice hoteldam source on file.";

const FILES_READ = [
  "AGENTS.md",
  "fixtures/choice-footprint-opening-hoteldam-map.json",
  "reports/brand-explorer-woodspring-real-property-examples-writer.json",
  "live WoodSpring footprint.openings / gallery / registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-footprint-opening-image-governance.js",
  "lib/partner-intelligence/brand-explorer-woodspring-property-specific-images-writer.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "scripts/brand-explorer-woodspring-property-specific-images-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "fixtures/choice-footprint-opening-hoteldam-map.json",
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

function normalizeUrlKey(url) {
  return nz(url).split("?")[0].toLowerCase();
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v33cR2WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-property-specific-images-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v33C-R2`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33C-R2 supports WoodSpring Suites only; got: ${brandArg}`);
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
        nz(a.sourceNotes).includes(row.recordId)
    ) || null
  );
}

function validatePresentationImagePatch(fields) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (key !== "Image" && key !== "Scenario Image") {
      errors.push(`unexpected_field:${key}`);
    }
  }
  const imageUrl = fields.Image?.[0]?.url || fields["Scenario Image"]?.[0]?.url || "";
  if (isTemporaryAirtableUrl(imageUrl)) errors.push("temporary_airtable_image_url");
  if (isLogoImageUrl(imageUrl)) errors.push("logo_image");
  if (isGenericBrandOrLifestyleImageUrl(imageUrl)) errors.push("generic_brand_lifestyle_image");
  if (imageUrl && !isHoteldamPropertyImageUrl(imageUrl)) {
    errors.push("non_hoteldam_durable_source");
  }
  return errors;
}

function validateVisibilityPatch(fields) {
  const keys = Object.keys(fields);
  if (keys.length !== 1 || fields["External Display Status"] !== HIDE_DISPLAY_STATUS) {
    return ["visibility_patch_must_only_hide"];
  }
  return [];
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

function buildRegistryFieldsForImage({
  row,
  catalog,
  brandConfig,
  parentCompany,
  imagePlan,
  slotKey,
  assetLabel,
}) {
  const propertyName = catalog?.propertyName || nz(row.title);
  const assetName = assetLabel || `${propertyName} — Hotel/Property Image — ${slotKey}`;
  const brandMatchNotes = `WoodSpring property-specific hoteldam image for ${propertyName}; verified hotel/property photography.`;
  const validationNotes = `${FOUNDER_REVIEW_NOTES} Property-specific ${imagePlan.imageKind || "hotel"} image from official Choice hoteldam CDN.`;
  const staged = buildWoodspringRegistryStagedAsset({
    row: { ...row, title: row.title, imageUrl: imagePlan.imageUrl, body: row.body },
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
  staged.propertyConfirmed = catalog ? "Yes" : "Partial";
  staged.calaRelevant = "No";
  staged.slotPurpose = `WoodSpring ${slotKey} — property-specific hotel/property photography`;
  staged.explorerSection =
    slotKey === OPENINGS_SLOT ? VISUAL_SLOT.RECENT_OPENINGS : VISUAL_SLOT.IMAGE_GALLERY;
  staged.sourcePageUrl = imagePlan.imageSourcePageUrl || catalog?.sourcePageUrl || "";
  staged.sourceUrl = imagePlan.imageUrl;
  staged.sourceBasis = SOURCE_BASIS.RENDERED_OFFICIAL;
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
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"],
      externalDisplayStatus: nz(f["External Display Status"]),
      visible:
        nz(f["External Display Status"]).toLowerCase() !== HIDE_DISPLAY_STATUS.toLowerCase(),
      hasImage: Array.isArray(f.Image) && f.Image.length > 0,
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
      sourcePageUrl: nz(f["Source Page URL"]),
      registryLink: Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"][0] : null,
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

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-property-specific-images-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_OFFICIAL_ONLY,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_OTHER_SECTIONS,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Property-Specific Hotel Images v33C-R2");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Visible property cards after apply: **${report.visiblePropertyCardsAfterApply}**`);
  lines.push(`- Section partially complete: **${report.openingsSectionPartiallyComplete ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Current visible property example audit");
  for (const row of report.currentPropertyExampleAudit) {
    lines.push(
      `- \`${row.recordId}\` **${row.title}** — image: ${row.currentImageSource}; property-specific: ${row.isPropertySpecific}; hotel photo: ${row.isHotelPhotography}; action: ${row.recommendedAction}`
    );
  }
  lines.push("");
  lines.push("## Property-specific image discovery");
  for (const d of report.propertyImageDiscovery) {
    lines.push(
      `- **${d.propertyName}** (${d.propertyId}): ${d.ok ? d.imageUrl : `MISS — ${d.error}`}`
    );
  }
  lines.push("");
  lines.push(`## Cards preserved: ${report.cardsPreserved.length}`);
  for (const c of report.cardsPreserved) lines.push(`- ${c.propertyName} (\`${c.recordId}\`)`);
  lines.push("");
  lines.push(`## Cards replaced: ${report.cardsReplaced.length}`);
  for (const c of report.cardsReplaced) {
    lines.push(`- ${c.propertyName} (\`${c.recordId}\`) → ${c.newImageUrl}`);
  }
  lines.push("");
  lines.push(`## Cards hidden: ${report.cardsHidden.length}`);
  for (const c of report.cardsHidden) lines.push(`- ${c.propertyName} (\`${c.recordId}\`) — ${c.reason}`);
  lines.push("");
  lines.push("## Gallery corrections");
  for (const g of report.galleryCorrections) {
    lines.push(`- \`${g.recordId}\` ${g.slotKey}: ${g.action} — ${g.reason}`);
  }
  lines.push("");
  lines.push("## Images rejected");
  for (const r of report.imagesRejected) lines.push(`- ${r.propertyName}: ${r.reason}`);
  lines.push("");
  lines.push(`## Registry creates: ${report.registryCreates.length}; patches: ${report.registryPatches.length}`);
  lines.push("");
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

export async function buildBrandExplorerWoodspringPropertySpecificImagesWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  officialImagesOnly = false,
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
  const openingsRows = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const galleryRows = presentationRows.filter((r) => GALLERY_SLOTS.includes(r.slotKey));

  const propertyImageDiscovery = [];
  const discoveryByPropertyId = new Map();
  for (const catalog of WOODSPRING_PROPERTY_CATALOG) {
    const discovery = await resolvePropertySpecificHotelImage(catalog.sourcePageUrl);
    propertyImageDiscovery.push({
      propertyName: catalog.propertyName,
      propertyId: discovery.propertyId || "",
      sourcePageUrl: catalog.sourcePageUrl,
      ok: discovery.ok,
      imageUrl: discovery.imageUrl || null,
      imageKind: discovery.imageKind || null,
      imageSource: discovery.imageSource || null,
      error: discovery.error || null,
    });
    if (discovery.ok) discoveryByPropertyId.set(discovery.propertyId, discovery);
  }

  const availableHotelImages = [...discoveryByPropertyId.values()];
  const usedGalleryImageKeys = new Set();

  const currentPropertyExampleAudit = [];
  const cardsPreserved = [];
  const cardsReplaced = [];
  const cardsHidden = [];
  const imagesRejected = [];
  const presentationImagePatches = [];
  const visibilityPatches = [];
  const registryPatches = [];
  const registryCreates = [];
  const galleryCorrections = [];
  const safetyBlockers = [];

  for (const catalog of WOODSPRING_PROPERTY_CATALOG) {
    const row = openingsRows.find((r) => r.recordId === catalog.presentationRecordId);
    if (!row) {
      safetyBlockers.push(`missing_opening_row:${catalog.presentationRecordId}`);
      continue;
    }

    const registryAsset = findRegistryForRow(registryAssets, row);
    const registrySourceUrl = registryAsset?.sourceUrl || "";
    const classification = classifyPropertyExampleImage(row.imageUrl, {
      registrySourceUrl,
      registryNotes: [registryAsset?.sourceNotes, registryAsset?.reviewNotes].filter(Boolean).join("\n"),
    });

    const discovery = propertyImageDiscovery.find((d) => d.propertyName === catalog.propertyName);
    const propertySourceUrl = catalog.sourcePageUrl;
    const currentImageSource =
      registrySourceUrl ||
      (isTemporaryAirtableUrl(row.imageUrl) ? "temporary_airtable_attachment" : row.imageUrl);

    let recommendedAction = "preserve";
    if (!row.visible) {
      recommendedAction = "already_hidden";
    } else if (discovery?.ok && classification.recommendation === "preserve" && classification.isHoteldam) {
      recommendedAction = "preserve";
    } else if (discovery?.ok) {
      recommendedAction = "replace";
    } else {
      recommendedAction = "hide";
      imagesRejected.push({
        propertyName: catalog.propertyName,
        reason: "no_property_specific_hotel_image_from_official_hoteldam",
      });
    }

    currentPropertyExampleAudit.push({
      recordId: row.recordId,
      title: row.title,
      propertySourceUrl,
      currentImageUrl: row.imageUrl,
      currentImageSource,
      isPropertySpecific: classification.isPropertySpecific,
      isHotelPhotography: classification.isHotelPhotography,
      isGenericBrand: classification.isGenericBrand,
      isLogo: classification.isLogo,
      isLifestyle: classification.isLifestyle,
      recommendedAction,
      recommendedReason: classification.reason,
    });

    if (recommendedAction === "preserve") {
      cardsPreserved.push({
        recordId: row.recordId,
        propertyName: catalog.propertyName,
      });
      continue;
    }

    if (recommendedAction === "already_hidden") continue;

    if (recommendedAction === "hide") {
      cardsHidden.push({
        recordId: row.recordId,
        propertyName: catalog.propertyName,
        reason: "no_property_specific_hotel_image",
      });
      if (row.visible) {
        const hideFields = { "External Display Status": HIDE_DISPLAY_STATUS };
        const hideErrors = validateVisibilityPatch(hideFields);
        if (hideErrors.length) safetyBlockers.push(`hide_validation:${row.recordId}`);
        else {
          visibilityPatches.push({
            recordId: row.recordId,
            slotKey: OPENINGS_SLOT,
            fields: hideFields,
            reason: "no_property_specific_hotel_image",
          });
        }
      }
      continue;
    }

    const imagePlan = {
      imageUrl: discovery.imageUrl,
      imageKind: discovery.imageKind,
      imageSource: discovery.imageSource,
      imageSourcePageUrl: catalog.sourcePageUrl,
    };

    cardsReplaced.push({
      recordId: row.recordId,
      propertyName: catalog.propertyName,
      previousImageUrl: row.imageUrl,
      newImageUrl: imagePlan.imageUrl,
      imageKind: imagePlan.imageKind,
    });

    const imageFields = { Image: [{ url: imagePlan.imageUrl }] };
    const imageErrors = validatePresentationImagePatch(imageFields);
    if (imageErrors.length) {
      safetyBlockers.push(`image_validation:${row.recordId}:${imageErrors.join(";")}`);
    } else if (normalizeUrlKey(row.imageUrl) !== normalizeUrlKey(imagePlan.imageUrl)) {
      presentationImagePatches.push({
        recordId: row.recordId,
        slotKey: OPENINGS_SLOT,
        fields: imageFields,
        reason: "replace_with_property_specific_hoteldam_image",
        imageSource: imagePlan.imageSource,
      });
    }

    const registryFields = buildRegistryFieldsForImage({
      row,
      catalog,
      brandConfig,
      parentCompany,
      imagePlan,
      slotKey: OPENINGS_SLOT,
      assetLabel: `${catalog.propertyName} — U.S. Property Example — PR / Opening Image`,
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
        fields: {
          ...buildFounderApprovedRegistryPatch({
            asset: registryAsset,
            row,
            brandConfig,
            materializationUrl: imagePlan.imageUrl,
          }),
          [MAP_BRAND_ASSET.assetName]: registryFields[MAP_BRAND_ASSET.assetName],
          [MAP_BRAND_ASSET.sourcePageUrl]: catalog.sourcePageUrl,
          [MAP_BRAND_ASSET.sourceUrl]: imagePlan.imageUrl,
          [MAP_VISUAL_SLOT.relatedPropertyName]: catalog.propertyName,
          [MAP_VISUAL_SLOT.propertyConfirmed]: "Yes",
          [MAP_VISUAL_SLOT.validationNotes]: registryFields[MAP_VISUAL_SLOT.validationNotes],
          [MAP_BRAND_ASSET.reviewNotes]: registryFields[MAP_BRAND_ASSET.reviewNotes],
          [MAP_BRAND_ASSET.sourceNotes]: registryFields[MAP_BRAND_ASSET.sourceNotes],
          [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
        },
      });
    } else {
      registryCreates.push({
        presentationRecordId: row.recordId,
        fields: registryFields,
      });
    }
  }

  let galleryImagePool = availableHotelImages.filter((img) => img?.imageUrl);
  for (const row of galleryRows.filter((r) => r.visible)) {
    const registryAsset = findRegistryForRow(registryAssets, row);
    const classification = classifyPropertyExampleImage(row.imageUrl, {
      registrySourceUrl: registryAsset?.sourceUrl || "",
      registryNotes: [registryAsset?.sourceNotes, registryAsset?.reviewNotes].filter(Boolean).join("\n"),
    });

    const needsCorrection =
      classification.isLogo ||
      classification.isGenericBrand ||
      classification.isLifestyle ||
      (!classification.isHotelPhotography && row.hasImage);

    if (!needsCorrection) continue;

    const replacement = galleryImagePool.find(
      (img) => img.imageUrl && !usedGalleryImageKeys.has(normalizeUrlKey(img.imageUrl))
    );

    if (replacement) {
      usedGalleryImageKeys.add(normalizeUrlKey(replacement.imageUrl));
      const imagePlan = {
        imageUrl: replacement.imageUrl,
        imageKind: replacement.imageKind,
        imageSource: replacement.imageSource,
        imageSourcePageUrl: replacement.imageSourcePageUrl,
      };
      galleryCorrections.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        action: "replace",
        reason: classification.reason,
        newImageUrl: replacement.imageUrl,
      });

      const imageFields = { Image: [{ url: replacement.imageUrl }] };
      const imageErrors = validatePresentationImagePatch(imageFields);
      if (imageErrors.length) {
        safetyBlockers.push(`gallery_image_validation:${row.recordId}:${imageErrors.join(";")}`);
      } else if (normalizeUrlKey(row.imageUrl) !== normalizeUrlKey(replacement.imageUrl)) {
        presentationImagePatches.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields: imageFields,
          reason: "replace_gallery_non_hotel_image",
          imageSource: replacement.imageSource,
        });
      }

      const registryFields = buildRegistryFieldsForImage({
        row,
        catalog: null,
        brandConfig,
        parentCompany,
        imagePlan,
        slotKey: row.slotKey,
        assetLabel: `${row.title || row.slotKey} — Gallery Hotel Image`,
      });
      const galleryRegistry = findRegistryForRow(registryAssets, row);
      if (galleryRegistry && !isDoNotUseRecord(galleryRegistry)) {
        registryPatches.push({
          recordId: galleryRegistry.id,
          presentationRecordId: row.recordId,
          fields: {
            ...buildFounderApprovedRegistryPatch({
              asset: galleryRegistry,
              row,
              brandConfig,
              materializationUrl: replacement.imageUrl,
            }),
            [MAP_BRAND_ASSET.sourcePageUrl]: imagePlan.imageSourcePageUrl,
            [MAP_BRAND_ASSET.sourceUrl]: replacement.imageUrl,
            [MAP_BRAND_ASSET.sourceNotes]: registryFields[MAP_BRAND_ASSET.sourceNotes],
            [MAP_BRAND_ASSET.reviewNotes]: registryFields[MAP_BRAND_ASSET.reviewNotes],
            [MAP_VISUAL_SLOT.validationNotes]: registryFields[MAP_VISUAL_SLOT.validationNotes],
            [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
          },
        });
      } else {
        registryCreates.push({
          presentationRecordId: row.recordId,
          fields: registryFields,
        });
      }
    } else {
      galleryCorrections.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        action: "hide",
        reason: `${classification.reason}; no_replacement_hotel_image`,
      });
      cardsHidden.push({
        recordId: row.recordId,
        propertyName: row.title || row.slotKey,
        reason: "gallery_non_hotel_image_no_replacement",
      });
      const hideFields = { "External Display Status": HIDE_DISPLAY_STATUS };
      const hideErrors = validateVisibilityPatch(hideFields);
      if (hideErrors.length) safetyBlockers.push(`gallery_hide_validation:${row.recordId}`);
      else {
        visibilityPatches.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields: hideFields,
          reason: "gallery_non_hotel_image_no_replacement",
        });
      }
    }
  }

  const projectedVisiblePropertyCards = WOODSPRING_PROPERTY_CATALOG.filter((catalog) => {
    const hidden = visibilityPatches.some(
      (p) => p.recordId === catalog.presentationRecordId && p.reason.includes("no_property_specific")
    );
    const replaced = cardsReplaced.some((c) => c.recordId === catalog.presentationRecordId);
    const preserved = cardsPreserved.some((c) => c.recordId === catalog.presentationRecordId);
    return !hidden && (replaced || preserved);
  }).length;

  const openingsSectionPartiallyComplete = projectedVisiblePropertyCards > 0 && projectedVisiblePropertyCards < 3;

  const plannedImageUrlByRecordId = new Map(
    presentationImagePatches
      .map((p) => [p.recordId, p?.fields?.Image?.[0]?.url || ""])
      .filter(([, url]) => Boolean(url))
  );

  const logoOrGenericRemainVisible = openingsRows.some((row) => {
    if (!row.visible) return false;
    if (visibilityPatches.some((p) => p.recordId === row.recordId)) return false;
    if (!isPropertyExampleTitle(row.title)) return false;
    const registryAsset = findRegistryForRow(registryAssets, row);
    const effectiveImageUrl = plannedImageUrlByRecordId.get(row.recordId) || row.imageUrl;
    const c = classifyPropertyExampleImage(effectiveImageUrl, {
      registrySourceUrl: registryAsset?.sourceUrl || "",
    });
    return c.isLogo || c.isGenericBrand || c.isLifestyle || !c.isHotelPhotography;
  });

  if (logoOrGenericRemainVisible) {
    safetyBlockers.push("logo_or_generic_property_images_remain_visible");
  }

  const protectedSlotTouched = [...presentationImagePatches, ...visibilityPatches].some((p) =>
    PROTECTED_SLOTS.has(p.slotKey)
  );
  if (protectedSlotTouched) safetyBlockers.push("protected_slot_touched");

  const applyBlockers = [...safetyBlockers];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!founderApproved) applyBlockers.push("missing_founder_approved_woodspring_property_specific_hotel_images");
    if (!officialImagesOnly) applyBlockers.push("missing_confirm_official_source_images_only");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noSummaryUrl) applyBlockers.push("missing_confirm_no_summary_url_field");
    if (!noOtherSectionChanges) applyBlockers.push("missing_confirm_no_momentum_proof_standard_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const hasWork =
    presentationImagePatches.length > 0 ||
    visibilityPatches.length > 0 ||
    registryPatches.length > 0 ||
    registryCreates.length > 0;

  const dryRunClean =
    safetyBlockers.length === 0 &&
    hasWork &&
    applyBlockers.filter((b) => b.startsWith("missing_")).length === 0;

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

  const expectedFinalQaResult = dryRunClean
    ? `projected_ready_after_property_specific_hotel_images (${projectedVisiblePropertyCards}/3 cards)`
    : finalQaReport?.summary?.overallStatus || "unknown";
  const expectedCompleteBuildResult = dryRunClean
    ? openingsSectionPartiallyComplete
      ? `active-profile_may_remain_ready; openings_section_partially_complete_${projectedVisiblePropertyCards}_of_3`
      : "projected_active_profile_ready_with_3_property_hotel_images"
    : completeBuildReport?.readyForActiveProfile
      ? "ready"
      : completeBuildReport?.blockers?.join("; ") || "blocked";
  const expectedVisualDefectResult = dryRunClean
    ? "projected_reduction_in_property_example_generic_image_defects"
    : visualDefectReport?.summary?.defectCount != null
      ? `${visualDefectReport.summary.defectCount} defects`
      : "unknown";

  let airtableModified = false;
  const applyResults = {
    presentationImagesUpdated: [],
    rowsHidden: [],
    registryPatched: [],
    registryCreated: [],
    errors: [],
  };

  const founderGatesReady =
    approveBatch &&
    founderApproved &&
    officialImagesOnly &&
    noValidationClaim &&
    noSummaryUrl &&
    noOtherSectionChanges &&
    woodspringOnly;

  const canApply = apply && founderGatesReady && safetyBlockers.length === 0;

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
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ presentationRecordId: create.presentationRecordId, message: err.message });
      }
    }

    for (const patch of visibilityPatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Hide PATCH failed: ${res.status}`);
        applyResults.rowsHidden.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }

    for (const patch of presentationImagePatches) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Image PATCH failed: ${res.status}`);
        applyResults.presentationImagesUpdated.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(target.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33cR2WriterExists: v33cR2WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    currentPropertyExampleAudit: currentPropertyExampleAudit,
    propertyImageDiscovery,
    cardsPreserved,
    cardsReplaced,
    cardsHidden,
    galleryCorrections,
    imagesRejected,
    presentationImagePatches,
    visibilityPatches,
    registryPatches,
    registryCreates,
    visiblePropertyCardsAfterApply: projectedVisiblePropertyCards,
    openingsSectionPartiallyComplete,
    logoOrGenericRemainVisible,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    dryRunClean,
    applyBlockers,
    applyResults,
    expectedFinalQaResult,
    expectedCompleteBuildResult,
    expectedVisualDefectResult,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-property-specific-images-writer -- --brand ${target.slug} --dry-run`,
    airtableModified,
  };

  report.markdown = buildMarkdown(report);
  return report;
}
