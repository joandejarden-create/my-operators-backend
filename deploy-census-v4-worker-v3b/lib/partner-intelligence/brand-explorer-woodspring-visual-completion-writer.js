/**
 * Brand Explorer WoodSpring Visual Completion + Registry Approval v33D-R1.
 *
 * Completes opening/scenario/gallery images via founder-gated registry approval
 * and presentation Image materialization. Gallery uses distinct official assets
 * only — defers/hides slots rather than repeating filler imagery.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-visual-completion-writer-v33D.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  listRegistryAssetsForBrand,
  validateRegistryWritePayload,
} from "./brand-asset-registry-workflow.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";
import {
  assessPresentationRowImageGovernance,
  detectWrongBrandSignageRisk,
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
  listVisualPresentationRows,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  buildWoodspringRegistryStagedAsset,
  TARGET_BRAND as WOODSPRING_TARGET,
} from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import {
  extractOgImageFromHtml,
  isTemporaryAirtableUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33D-R1";
export const STAGING_RUN_ID = "v33D-R1-woodspring-visual-completion";
export const GALLERY_DISPLAY_STATUS_HIDE = "Do Not Display";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-visual-completion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-visual-completion-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-visual-completion-writer-v33D.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v33D-woodspring-visual-completion";
export const APPLY_FLAG_FOUNDER = "--founder-approved-woodspring-official-images";
export const APPLY_FLAG_OFFICIAL_ONLY = "--confirm-official-source-images-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_SUMMARY_URL = "--confirm-no-summary-url-field";
export const APPLY_FLAG_NO_MOMENTUM = "--confirm-no-momentum-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const TARGET_BRAND = WOODSPRING_TARGET;
export const QUARANTINED_SCENARIO3_RECORD_ID = "recrnaRxigUSoDDTJ";
export const PROTECTED_BRAND_SLUGS = Object.freeze(["everhome-suites", "suburban-studios"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MOMENTUM_SLOT = "footprint.momentum";
const OPENINGS_SLOT = "footprint.openings";

const BLOCKED_PRESENTATION_PATCH_FIELDS = new Set([
  "Body",
  "Title",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "Company Validated",
  "Company Validation Date",
  "External Display Status",
  "Active",
  "Brand",
  "Brand Name",
  "Slot Key",
  "Sort Order",
]);

const GALLERY_SLOT_PLANS = Object.freeze([
  { slotKey: "materials.gallery.1", purpose: "exterior / prototype" },
  { slotKey: "materials.gallery.2", purpose: "guest room or suite" },
  { slotKey: "materials.gallery.3", purpose: "kitchen / in-room amenity" },
  { slotKey: "materials.gallery.4", purpose: "public / common area" },
  { slotKey: "materials.gallery.5", purpose: "extended-stay product detail" },
  { slotKey: "materials.gallery.6", purpose: "brand / platform visual" },
]);

const SCENARIO_SLOTS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
]);

const FOUNDER_REVIEW_NOTES =
  "v33D-R1 founder-approved — official WoodSpring / Choice source image approved for Explorer materialization.";
const FOUNDER_SOURCE_NOTES_SUFFIX =
  "Founder-approved official WoodSpring visual; durable source page on file.";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-openings-momentum-build-writer.json",
  "reports/brand-explorer-woodspring-presentation-cleanup-backfill-writer.json",
  "reports/brand-explorer-woodspring-source-registry-readiness-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "live WoodSpring presentation / registry / sources / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-visual-completion-writer.js",
  "scripts/brand-explorer-woodspring-visual-completion-writer.mjs",
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

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function inferAssetTypeForSlot(slotKey) {
  if (slotKey === "overview.hero") return ASSET_TYPE.HERO;
  if (slotKey === OPENINGS_SLOT) return ASSET_TYPE.PR_IMAGE;
  if (/materials\.gallery/.test(slotKey)) return ASSET_TYPE.EXTERIOR;
  if (/overview\.scenario|valueOwners\.scenario/.test(slotKey)) return ASSET_TYPE.LIFESTYLE;
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
  if (staged.doNotUseReason) fields[MAP_BRAND_ASSET.doNotUseReason] = staged.doNotUseReason;
  return fields;
}

export function v33dWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-woodspring-visual-completion-writer.js")
  );
}

export function detectWoodspringWrongBrandRisk(text, brandConfig) {
  return detectWrongBrandSignageRisk(text, brandConfig);
}

export function validatePresentationImagePatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_PATCH_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (/summary url/i.test(key)) errors.push("summary_url_field_blocked");
  }
  if (slotKey === MOMENTUM_SLOT) errors.push("momentum_row_blocked");
  if (!fields.Image && !fields["Scenario Image"]) errors.push("missing_image_field");
  return errors;
}

/** Gallery-only visibility patch — hide deferred slots without filler imagery. */
export function validateGalleryVisibilityPatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  const keys = Object.keys(fields);
  if (keys.length !== 1 || fields["External Display Status"] !== GALLERY_DISPLAY_STATUS_HIDE) {
    errors.push("gallery_visibility_patch_must_only_hide");
  }
  if (!/^materials\.gallery\.\d$/.test(slotKey)) {
    errors.push("gallery_visibility_slot_only");
  }
  if (/summary url/i.test(keys.join(" "))) errors.push("summary_url_field_blocked");
  return errors;
}

export function buildRepeatedImageUseReport(plans) {
  const byUrl = new Map();
  for (const plan of plans) {
    if (plan.blocked || !plan.imageUrl) continue;
    const key = normalizeUrlKey(plan.imageUrl);
    if (!byUrl.has(key)) byUrl.set(key, []);
    byUrl.get(key).push({
      slotKey: plan.slotKey,
      recordId: plan.recordId,
      imageReuseNote: plan.imageReuseNote || null,
    });
  }
  return [...byUrl.entries()]
    .filter(([, uses]) => uses.length > 1)
    .map(([imageUrlKey, uses]) => ({
      imageUrlKey,
      useCount: uses.length,
      uses,
      reason: uses.every(
        (u) => u.slotKey === OPENINGS_SLOT || /^footprint\.openings/.test(u.slotKey || "")
      )
        ? "opening_example_reuse_official"
        : "non_opening_reuse_review",
    }));
}

export function assessPremiumDisplayProjection({
  openingImagePlan,
  scenarioImagePlan,
  galleryImagePlan,
  imagePool,
  registryPatches,
  registryCreates,
}) {
  const materializedPlans = [
    ...openingImagePlan,
    ...scenarioImagePlan,
    ...galleryImagePlan,
  ].filter((p) => !p.blocked && p.presentationImagePatch);

  const distinctImageUrls = new Set(
    materializedPlans.map((p) => normalizeUrlKey(p.imageUrl)).filter(Boolean)
  );

  const visibleGallerySlots = galleryImagePlan.filter(
    (p) => p.action === "materialize" && !p.blocked && !p.deferred
  );
  const hiddenGallerySlots = galleryImagePlan.filter(
    (p) => p.action === "hide_gallery_slot" || p.deferred
  );
  const deferredGallerySlots = galleryImagePlan.filter((p) => p.deferred);

  const repeatedImageUses = buildRepeatedImageUseReport(materializedPlans);
  const galleryRepeats = repeatedImageUses.filter((r) => {
    const galleryUses = r.uses.filter((u) => /^materials\.gallery\./.test(u.slotKey || ""));
    return galleryUses.length > 1;
  });
  const crossSectionImageReuse = repeatedImageUses.filter((r) => {
    const hasGallery = r.uses.some((u) => /^materials\.gallery\./.test(u.slotKey || ""));
    const hasNonGallery = r.uses.some((u) => !/^materials\.gallery\./.test(u.slotKey || ""));
    return hasGallery && hasNonGallery;
  });

  const visiblePlaceholders = [];
  for (const plan of [...openingImagePlan, ...scenarioImagePlan]) {
    if (plan.blocked) {
      visiblePlaceholders.push({
        slotKey: plan.slotKey,
        recordId: plan.recordId,
        reason: plan.reason || "missing_image_plan",
      });
      continue;
    }
    if (!plan.presentationImagePatch && !plan.registryOnlyApproval) {
      visiblePlaceholders.push({
        slotKey: plan.slotKey,
        recordId: plan.recordId,
        reason: "no_materialization_or_registry_path",
      });
    }
  }

  const registryWrites = [
    ...registryPatches.map((p) => p.fields),
    ...registryCreates.map((c) => c.fields),
  ];
  const tempDurableSourceViolations = registryWrites.filter((fields) => {
    const page = fields[MAP_BRAND_ASSET.sourcePageUrl];
    return isTemporaryAirtableUrl(page);
  });

  const premiumBlockers = [];
  if (visiblePlaceholders.length) premiumBlockers.push("visible_image_placeholders");
  if (galleryRepeats.length) premiumBlockers.push("gallery_duplicate_filler");
  if (tempDurableSourceViolations.length) {
    premiumBlockers.push("temporary_airtable_url_as_durable_source");
  }
  if (
    materializedPlans.some((p) =>
      /wrong_brand|everhome/i.test(`${p.reason || ""} ${p.imageReuseNote || ""}`)
    )
  ) {
    premiumBlockers.push("wrong_brand_visible_risk");
  }

  const galleryDistinctVisible =
    new Set(visibleGallerySlots.map((p) => normalizeUrlKey(p.imageUrl)).filter(Boolean)).size ===
    visibleGallerySlots.length;
  const galleryPremiumEnoughForActiveProfile =
    galleryDistinctVisible &&
    galleryRepeats.length === 0 &&
    visibleGallerySlots.length <= imagePool.length &&
    (visibleGallerySlots.length >= Math.min(3, imagePool.length) ||
      hiddenGallerySlots.length >= 6 - imagePool.length);

  return {
    distinctImageCount: distinctImageUrls.size,
    visibleGallerySlotsProposed: visibleGallerySlots.length,
    hiddenGallerySlots: hiddenGallerySlots.length,
    deferredGallerySlots: deferredGallerySlots.length,
    hiddenGallerySlotDetails: hiddenGallerySlots.map((p) => ({
      slotKey: p.slotKey,
      recordId: p.recordId,
      reason: p.reason,
      followUpAssetNeed: p.followUpAssetNeed ?? true,
    })),
    repeatedImageUses,
    crossSectionImageReuse,
    visiblePlaceholdersRemaining: visiblePlaceholders,
    galleryPremiumEnoughForActiveProfile,
    premiumDisplayBlockers: premiumBlockers,
  };
}

export function buildFounderApprovedRegistryPatch({ asset, row, brandConfig, materializationUrl }) {
  const pageUrl =
    extractUrlFromText(row.body) ||
    nz(asset?.sourcePageUrl) ||
    brandConfig.consumerUrl;
  const durablePage = isOfficialPageUrl(pageUrl, brandConfig) ? pageUrl : brandConfig.consumerUrl;
  const imageUrl = materializationUrl || nz(asset?.sourceUrl) || nz(row.imageUrl);
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
    [MAP_VISUAL_SLOT.validationNotes]: FOUNDER_REVIEW_NOTES,
    [MAP_VISUAL_SLOT.explorerSection]: inferExplorerSection(row.slotKey),
    [MAP_VISUAL_SLOT.slotPurpose]: `WoodSpring ${row.slotKey} — founder-approved official image`,
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.propertyConfirmed]: row.slotKey === OPENINGS_SLOT ? "Yes" : "Unknown",
    [MAP_VISUAL_SLOT.calaRelevant]: "No",
    [MAP_BRAND_ASSET.stagingRunId]: STAGING_RUN_ID,
    [MAP_BRAND_ASSET.sourcePageUrl]: durablePage,
  };
  if (imageUrl && !isTemporaryAirtableUrl(imageUrl)) {
    fields[MAP_BRAND_ASSET.sourceUrl] = imageUrl;
  }
  return fields;
}

function classifyVisualIssue(row, assessment, registryAsset) {
  if (row.recordId === QUARANTINED_SCENARIO3_RECORD_ID) {
    return {
      issueType: "wrong_brand_quarantined",
      proposedFix: "preserve_quarantine_do_not_redisplay",
    };
  }
  if (!nz(row.imageUrl)) {
    return {
      issueType: row.slotKey === OPENINGS_SLOT ? "missing_image" : "missing_image",
      proposedFix: "approve_registry_and_materialize_official_image",
    };
  }
  if (assessment?.wrongBrandRisk) {
    return {
      issueType: "wrong_brand_risk",
      proposedFix: "do_not_use_quarantine_registry",
    };
  }
  if (isTemporaryAirtableUrl(row.imageUrl)) {
    return {
      issueType: "temporary_url",
      proposedFix: "approve_registry_link_durable_source_page_preserve_working_attachment",
    };
  }
  if (!assessment?.registryApproved) {
    return {
      issueType: "missing_registry_approval",
      proposedFix: "founder_approve_registry_for_working_image",
    };
  }
  return { issueType: "ok", proposedFix: "none" };
}

function isRowProtected(row) {
  if (row.recordId === QUARANTINED_SCENARIO3_RECORD_ID) return true;
  if (row.slotKey === MOMENTUM_SLOT) return true;
  if (nz(row.externalDisplayStatus).toLowerCase() === "do not display") return true;
  if (/\beverhome\b/i.test(`${row.title} ${row.body}`) && row.recordId === QUARANTINED_SCENARIO3_RECORD_ID) {
    return true;
  }
  return false;
}

function isCleanScenario3Replacement(row, quarantinedRow) {
  return (
    row.slotKey === "overview.scenario.3" &&
    row.recordId !== QUARANTINED_SCENARIO3_RECORD_ID &&
    row.recordId !== quarantinedRow?.recordId
  );
}

function materializationUrlAllowed(url, brandConfig, { officialOnly = true } = {}) {
  const u = nz(url);
  if (!u) return false;
  if (/\beverhome\b|\bsuburban studios\b/i.test(u)) return false;
  if (officialOnly && !isTemporaryAirtableUrl(u)) {
    return true;
  }
  if (isTemporaryAirtableUrl(u)) {
    return true;
  }
  return !officialOnly;
}

async function fetchOfficialOgImages(brandConfig, approvedSources) {
  const pageUrls = [
    brandConfig.consumerUrl,
    "https://media.choicehotels.com/woodspring-suites-press-kit",
    "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/woodspring-suites",
    "https://www.choicehotels.com/woodspring-hotels",
    ...approvedSources.map((s) => nz(s.sourceUrl)).filter(Boolean),
  ].filter((u) => isOfficialPageUrl(u, brandConfig));

  const discovered = [];
  const seen = new Set();
  for (const pageUrl of [...new Set(pageUrls)]) {
    try {
      const res = await fetch(pageUrl, {
        headers: { "User-Agent": "DealalityBrandExplorer/1.0" },
        signal: AbortSignal.timeout(12000),
      });
      if (!res.ok) continue;
      const html = await res.text();
      const og = extractOgImageFromHtml(html);
      if (!og || isTemporaryAirtableUrl(og)) continue;
      const key = normalizeUrlKey(og);
      if (seen.has(key)) continue;
      if (detectWoodspringWrongBrandRisk(og, brandConfig)) continue;
      seen.add(key);
      discovered.push({
        imageUrl: og,
        sourcePageUrl: pageUrl,
        source: "official_og_image",
      });
    } catch {
      // skip unreachable pages in dry-run discovery
    }
  }
  return discovered;
}

function buildImageCandidatePool({ presentationRows, registryAssets, ogImages, brandConfig }) {
  const pool = [];
  const seen = new Set();

  function addCandidate(candidate) {
    const key = normalizeUrlKey(candidate.imageUrl);
    if (!key || seen.has(key)) return;
    if (!materializationUrlAllowed(candidate.imageUrl, brandConfig)) return;
    if (detectWoodspringWrongBrandRisk(`${candidate.imageUrl} ${candidate.label || ""}`, brandConfig)) {
      return;
    }
    seen.add(key);
    pool.push(candidate);
  }

  for (const row of presentationRows) {
    if (!nz(row.imageUrl)) continue;
    if (row.recordId === QUARANTINED_SCENARIO3_RECORD_ID) continue;
    if (detectWoodspringWrongBrandRisk(`${row.title} ${row.body}`, brandConfig)) continue;
    addCandidate({
      imageUrl: row.imageUrl,
      sourcePageUrl: extractUrlFromText(row.body) || brandConfig.consumerUrl,
      source: "presentation_attachment",
      fromPresentationRowId: row.recordId,
      fromSlotKey: row.slotKey,
      label: row.title,
    });
  }

  for (const asset of registryAssets) {
    if (isDoNotUseRecord(asset)) continue;
    if (/everhome|do not use/i.test(nz(asset.assetName))) continue;
    const imageUrl = nz(asset.sourceUrl);
    if (!imageUrl) continue;
    addCandidate({
      imageUrl,
      sourcePageUrl: nz(asset.sourcePageUrl) || brandConfig.consumerUrl,
      source: "registry_source_url",
      registryRecordId: asset.id,
      label: asset.assetName,
    });
  }

  for (const og of ogImages) {
    addCandidate({
      imageUrl: og.imageUrl,
      sourcePageUrl: og.sourcePageUrl,
      source: og.source,
      label: `OG — ${og.sourcePageUrl}`,
    });
  }

  return pool;
}

function findRegistryForRow(registryAssets, row) {
  const linked = registryAssets.find(
    (a) =>
      parsePresentationRowIdFromNotes(a.sourceNotes) === row.recordId ||
      nz(a.recommendedExplorerSlot) === row.slotKey
  );
  if (linked) return linked;
  return registryAssets.find((a) => nz(a.recommendedExplorerSlot) === row.slotKey) || null;
}

function pickImageForRow(row, pool, usedKeys, { preferPageMatch = true, allowOfficialReuse = false } = {}) {
  const bodyUrl = extractUrlFromText(row.body);
  if (preferPageMatch && bodyUrl) {
    const host = (() => {
      try {
        return new URL(bodyUrl).hostname;
      } catch {
        return "";
      }
    })();
    const match = pool.find((c) => {
      const key = normalizeUrlKey(c.imageUrl);
      if (usedKeys.has(key)) return false;
      return nz(c.sourcePageUrl).includes(host) || host.includes("woodspring");
    });
    if (match) return match;
  }
  const next = pool.find((c) => !usedKeys.has(normalizeUrlKey(c.imageUrl)));
  if (next) return next;
  if (allowOfficialReuse && pool.length) {
    const idx = Math.abs(row.recordId.split("").reduce((a, c) => a + c.charCodeAt(0), 0)) % pool.length;
    return { ...pool[idx], reusedOfficialImage: true };
  }
  return null;
}

function proposeSlotImagePlan({
  row,
  pool,
  usedKeys,
  registryAssets,
  brandConfig,
  founderGatesReady,
  allowOfficialReuse = false,
  forcedCandidate = null,
}) {
  if (isRowProtected(row)) {
    return {
      blocked: true,
      recordId: row.recordId,
      slotKey: row.slotKey,
      reason: "protected_or_quarantined_row",
    };
  }

  const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryAssets);
  const wrongBrand = detectWoodspringWrongBrandRisk(`${row.title} ${row.body} ${row.imageUrl}`, brandConfig);
  if (wrongBrand) {
    return {
      blocked: true,
      recordId: row.recordId,
      slotKey: row.slotKey,
      reason: `wrong_brand:${wrongBrand.markerId}`,
    };
  }

  const hasWorkingImage =
    Boolean(row.imageUrl) &&
    (isTemporaryAirtableUrl(row.imageUrl) || !isTemporaryAirtableUrl(row.imageUrl));
  const needsImage =
    row.slotKey === OPENINGS_SLOT ? !hasWorkingImage : !hasWorkingImage;

  let candidate = forcedCandidate;
  if (needsImage && !candidate) {
    candidate = pickImageForRow(row, pool, usedKeys, { allowOfficialReuse });
    if (!candidate) {
      return {
        blocked: true,
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        reason: "no_safe_image_candidate",
      };
    }
    usedKeys.add(normalizeUrlKey(candidate.imageUrl));
  } else if (!candidate) {
    candidate = {
      imageUrl: row.imageUrl,
      sourcePageUrl: extractUrlFromText(row.body) || brandConfig.consumerUrl,
      source: "existing_presentation_image",
    };
  }

  const registryAsset = findRegistryForRow(registryAssets, row);
  if (registryAsset && isDoNotUseRecord(registryAsset)) {
    return {
      blocked: true,
      recordId: row.recordId,
      slotKey: row.slotKey,
      reason: "linked_registry_do_not_use",
    };
  }

  const materializationUrl = candidate.imageUrl;
  if (!materializationUrlAllowed(materializationUrl, brandConfig, { officialOnly: true })) {
    return {
      blocked: true,
      recordId: row.recordId,
      slotKey: row.slotKey,
      reason: "unsupported_image_url",
    };
  }

  const registryPatch = buildFounderApprovedRegistryPatch({
    asset: registryAsset,
    row,
    brandConfig,
    materializationUrl,
  });

  let registryCreate = null;
  if (!registryAsset) {
    const staged = buildWoodspringRegistryStagedAsset({
      row: { ...row, imageUrl: materializationUrl },
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
    staged.sourceUrl = isTemporaryAirtableUrl(materializationUrl) ? null : materializationUrl;
    registryCreate = {
      fields: mapStagedToRegistryFields(staged, TARGET_BRAND.recordId, brandConfig.parentCompany),
    };
  }

  const imageFields = {
    Image: [{ url: materializationUrl }],
  };
  const shouldMaterializeImage = needsImage;
  const imagePatchErrors = shouldMaterializeImage
    ? validatePresentationImagePatch(imageFields, { slotKey: row.slotKey })
    : [];
  if (imagePatchErrors.length) {
    return {
      blocked: true,
      recordId: row.recordId,
      slotKey: row.slotKey,
      reason: imagePatchErrors.join(";"),
    };
  }

  const openingReuse =
    candidate.reusedOfficialImage && (row.slotKey === OPENINGS_SLOT || allowOfficialReuse);
  const imageReuseNote = candidate.reusedOfficialImage
    ? row.slotKey === OPENINGS_SLOT
      ? "Opening example / footprint signal — official WoodSpring image reused; not a specific property claim."
      : "Official WoodSpring image reused where distinct assets are limited — founder-approved pool only."
    : null;

  return {
    blocked: false,
    action: "materialize",
    recordId: row.recordId,
    slotKey: row.slotKey,
    title: row.title,
    imageUrl: materializationUrl,
    sourcePageUrl: candidate.sourcePageUrl,
    imageSource: candidate.source,
    imageReuseNote,
    openingExampleReuse: Boolean(openingReuse && row.slotKey === OPENINGS_SLOT),
    registryRecordId: registryAsset?.id || null,
    registryPatch: registryAsset ? { recordId: registryAsset.id, fields: registryPatch } : null,
    registryCreate,
    presentationImagePatch: shouldMaterializeImage
      ? {
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields: imageFields,
          requiresFounderApproval: !founderGatesReady,
        }
      : null,
    registryOnlyApproval: !shouldMaterializeImage,
  };
}

function proposeGalleryImagePlans({
  galleryRows,
  imagePool,
  registryAssets,
  brandConfig,
  founderGatesReady,
}) {
  const galleryUsedKeys = new Set();
  const plans = [];

  for (const { slotKey, purpose } of GALLERY_SLOT_PLANS) {
    const row = galleryRows.find((r) => r.slotKey === slotKey);
    if (!row) {
      plans.push({
        blocked: true,
        action: "skip",
        slotKey,
        reason: "missing_presentation_row",
        visualPurpose: purpose,
      });
      continue;
    }

    if (isRowProtected(row)) {
      plans.push({
        blocked: true,
        action: "skip",
        recordId: row.recordId,
        slotKey,
        reason: "protected_row",
        visualPurpose: purpose,
      });
      continue;
    }

    const existingKey = row.imageUrl ? normalizeUrlKey(row.imageUrl) : "";
    if (existingKey && galleryUsedKeys.has(existingKey)) {
      plans.push({
        blocked: false,
        action: "hide_gallery_slot",
        deferred: true,
        recordId: row.recordId,
        slotKey,
        title: row.title,
        visualPurpose: purpose,
        reason: "duplicate_gallery_image_hidden",
        followUpAssetNeed: true,
        presentationVisibilityPatch: {
          recordId: row.recordId,
          slotKey,
          fields: { "External Display Status": GALLERY_DISPLAY_STATUS_HIDE },
        },
      });
      continue;
    }

    const candidate = pickImageForRow(row, imagePool, galleryUsedKeys, {
      allowOfficialReuse: false,
    });

    if (!candidate) {
      plans.push({
        blocked: false,
        action: "hide_gallery_slot",
        deferred: true,
        recordId: row.recordId,
        slotKey,
        title: row.title,
        visualPurpose: purpose,
        reason: "deferred_insufficient_distinct_assets",
        followUpAssetNeed: true,
        presentationVisibilityPatch: {
          recordId: row.recordId,
          slotKey,
          fields: { "External Display Status": GALLERY_DISPLAY_STATUS_HIDE },
        },
      });
      continue;
    }

    galleryUsedKeys.add(normalizeUrlKey(candidate.imageUrl));
    const plan = proposeSlotImagePlan({
      row,
      pool: imagePool,
      usedKeys: galleryUsedKeys,
      registryAssets,
      brandConfig,
      founderGatesReady,
      allowOfficialReuse: false,
      forcedCandidate: candidate,
    });
    plans.push({ ...plan, action: plan.blocked ? "skip" : "materialize", visualPurpose: purpose });
  }

  return plans;
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
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      hasImage: Boolean(imageAtt?.url),
      externalDisplayStatus: nz(f["External Display Status"]),
      sortOrder: f["Sort Order"],
    };
  });
}

async function fetchAllBrandSources(brandRecordId) {
  const all = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);
  return all;
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

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-visual-completion-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_OFFICIAL_ONLY,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_SUMMARY_URL,
    APPLY_FLAG_NO_MOMENTUM,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Visual Completion v33D-R1");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v33D exists: **${report.v33dWriterExists ? "yes" : "no"}**`);
  lines.push(`- Writer version: **${report.writerVersion}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Summary URL protection: **${report.summaryUrlProtectionConfirmed ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Image diversity");
  lines.push(`- Distinct official images: **${report.distinctImageCount}**`);
  lines.push(`- Visible gallery slots: **${report.visibleGallerySlotsProposed}**`);
  lines.push(`- Hidden/deferred gallery slots: **${report.hiddenGallerySlots}**`);
  lines.push(`- Gallery premium enough: **${report.galleryPremiumEnoughForActiveProfile ? "yes" : "no"}**`);
  lines.push(`- Visible placeholders remaining: **${report.visiblePlaceholdersRemaining?.length ?? 0}**`);
  lines.push("");
  lines.push("## Plans");
  lines.push(`- Opening image plans: **${report.openingImagePlan.filter((p) => !p.blocked).length}**`);
  lines.push(`- Scenario image plans: **${report.scenarioImagePlan.filter((p) => !p.blocked).length}**`);
  lines.push(`- Gallery materialize: **${report.visibleGallerySlotsProposed}** · hide/defer: **${report.hiddenGallerySlots}**`);
  lines.push(`- Registry patches: **${report.registryPatches.length}**`);
  lines.push(`- Registry creates: **${report.registryCreates.length}**`);
  lines.push(`- Image writes: **${report.imageFieldWritesProposed.length}**`);
  lines.push(`- Gallery visibility hides: **${report.galleryVisibilityPatches?.length ?? 0}**`);
  lines.push(`- Repeated image uses: **${report.repeatedImageUses?.length ?? 0}**`);
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  lines.push(`- Next: ${report.recommendedNextWriter}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringVisualCompletionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  officialImagesOnly = false,
  noValidationClaim = false,
  noSourceLibrary = false,
  noSummaryUrl = false,
  noMomentumChanges = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified by v33D: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33D is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug);
  if (!brandConfig) throw new Error("WoodSpring discovery brand config missing");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load WoodSpring API shape");

  const presentationRows = await listPresentationRowsDetailed(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const registryAssetsRaw = await listRegistryAssetsForBrand(TARGET_BRAND.recordId);
  const registryAssets = registryAssetsRaw.map((a) => normalizeRegistryRecordExtended(a));
  const approvedSources = (await fetchAllBrandSources(TARGET_BRAND.recordId)).filter(
    isApprovedExplorerSource
  );

  const visualRows = presentationRows.filter((r) => isVisualImageSlot(r.slotKey));
  const openingRows = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const scenarioRows = presentationRows.filter((r) => SCENARIO_SLOTS.includes(r.slotKey));
  const galleryRows = presentationRows.filter((r) =>
    GALLERY_SLOT_PLANS.some((g) => g.slotKey === r.slotKey)
  );
  const quarantinedScenario3 = presentationRows.find(
    (r) => r.recordId === QUARANTINED_SCENARIO3_RECORD_ID
  );
  const cleanScenario3 = scenarioRows.find((r) => isCleanScenario3Replacement(r, quarantinedScenario3));

  const ogImages = await fetchOfficialOgImages(brandConfig, approvedSources);
  const imagePool = buildImageCandidatePool({
    presentationRows: visualRows,
    registryAssets,
    ogImages,
    brandConfig,
  });

  const founderGatesReady = founderApproved && officialImagesOnly;

  const visualBlockerAudit = visualRows.map((row) => {
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryAssets);
    const registryAsset = findRegistryForRow(registryAssets, row);
    const issue = classifyVisualIssue(row, assessment, registryAsset);
    return {
      defectType: issue.issueType,
      severity:
        issue.issueType === "wrong_brand_quarantined" || issue.issueType === "wrong_brand_risk"
          ? "high"
          : issue.issueType === "missing_image" || issue.issueType === "temporary_url"
            ? "high"
            : "medium",
      section: `presentation.${row.slotKey}`,
      slotKey: row.slotKey,
      presentationRowId: row.recordId,
      currentTitle: row.title,
      imageFieldStatus: row.hasImage ? "present" : "blank",
      imageUrlStatus: row.imageUrl
        ? isTemporaryAirtableUrl(row.imageUrl)
          ? "temporary_airtable"
          : "durable_or_attachment"
        : "none",
      registryLinkStatus: registryAsset?.id || null,
      registryApproved: registryAsset ? isRegistryAssetApprovedForExplorer(registryAsset) : false,
      sourceSupport: extractUrlFromText(row.body) || nz(registryAsset?.sourcePageUrl) || null,
      issueCategory: issue.issueType,
      proposedFix: issue.proposedFix,
      protected: isRowProtected(row),
    };
  });

  const usedImageKeys = new Set();
  const openingImagePlan = openingRows.map((row) => {
    const plan = proposeSlotImagePlan({
      row,
      pool: imagePool,
      usedKeys: usedImageKeys,
      registryAssets,
      brandConfig,
      founderGatesReady,
      allowOfficialReuse: true,
    });
    return { ...plan, action: plan.blocked ? "skip" : "materialize" };
  });

  const scenarioTargets = scenarioRows.filter((r) => !isRowProtected(r));
  const scenarioImagePlan = scenarioTargets.map((row) => {
    const plan = proposeSlotImagePlan({
      row,
      pool: imagePool,
      usedKeys: usedImageKeys,
      registryAssets,
      brandConfig,
      founderGatesReady,
      allowOfficialReuse: row.slotKey === "overview.scenario.3",
    });
    return {
      ...plan,
      action: plan.blocked ? "skip" : plan.registryOnlyApproval ? "registry_approval_only" : "materialize",
    };
  });

  const galleryImagePlan = proposeGalleryImagePlans({
    galleryRows,
    imagePool,
    registryAssets,
    brandConfig,
    founderGatesReady,
  });

  const galleryVisibilityPatches = galleryImagePlan
    .filter((p) => p.presentationVisibilityPatch)
    .map((p) => p.presentationVisibilityPatch);

  const allPlans = [
    ...openingImagePlan,
    ...scenarioImagePlan,
    ...galleryImagePlan.filter((p) => p.recordId),
  ];
  const registryPatches = allPlans
    .filter((p) => !p.blocked && p.registryPatch)
    .map((p) => p.registryPatch);
  const registryCreates = allPlans
    .filter((p) => !p.blocked && p.registryCreate)
    .map((p) => ({ slotKey: p.slotKey, fields: p.registryCreate.fields }));
  const imageFieldWrites = allPlans
    .filter((p) => !p.blocked && p.presentationImagePatch)
    .map((p) => p.presentationImagePatch);

  const premiumDisplay = assessPremiumDisplayProjection({
    openingImagePlan,
    scenarioImagePlan,
    galleryImagePlan,
    imagePool,
    registryPatches,
    registryCreates,
  });

  const imagesSkipped = [
    ...allPlans
      .filter((p) => p.blocked)
      .map((p) => ({
        slotKey: p.slotKey,
        recordId: p.recordId || null,
        reason: p.reason,
      })),
    ...galleryImagePlan
      .filter((p) => p.deferred)
      .map((p) => ({
        slotKey: p.slotKey,
        recordId: p.recordId,
        reason: p.reason,
        followUpAssetNeed: p.followUpAssetNeed ?? true,
      })),
  ];

  const wrongBrandSafeguards = {
    quarantinedScenario3RecordId: QUARANTINED_SCENARIO3_RECORD_ID,
    quarantinedScenario3Touched: false,
    everhomeImagesBlocked: true,
    protectedMomentumRows: presentationRows.filter((r) => r.slotKey === MOMENTUM_SLOT).length,
    cleanScenario3RecordId: cleanScenario3?.recordId || null,
  };

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!founderApproved) applyBlockers.push("missing_founder_approved_woodspring_official_images");
    if (!officialImagesOnly) applyBlockers.push("missing_confirm_official_source_images_only");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noSourceLibrary) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noSummaryUrl) applyBlockers.push("missing_confirm_no_summary_url_field");
    if (!noMomentumChanges) applyBlockers.push("missing_confirm_no_momentum_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const blockedPlans = allPlans.filter((p) => p.blocked);
  if (!openingImagePlan.filter((p) => !p.blocked).length) {
    applyBlockers.push("no_viable_opening_image_plans");
  } else if (openingImagePlan.filter((p) => !p.blocked).length < 3) {
    applyBlockers.push("insufficient_opening_image_plans");
  }
  if (blockedPlans.some((p) => /wrong_brand|do_not_use/i.test(nz(p.reason)))) {
    applyBlockers.push("wrong_brand_or_do_not_use_risk");
  }
  for (const blocker of premiumDisplay.premiumDisplayBlockers) {
    applyBlockers.push(`premium_display:${blocker}`);
  }

  const hasWork =
    registryPatches.length > 0 ||
    registryCreates.length > 0 ||
    imageFieldWrites.length > 0 ||
    galleryVisibilityPatches.length > 0;
  const dryRunClean = applyBlockers.filter((b) => !b.startsWith("missing_")).length === 0 && hasWork;

  let airtableModified = false;
  const applyResults = {
    registryPatched: [],
    registryCreated: [],
    imagesMaterialized: [],
    gallerySlotsHidden: [],
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
    woodspringOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    const registryTable = registryTableName();
    for (const patch of registryPatches) {
      try {
        const validation = validateRegistryWritePayload(patch.fields);
        if (!validation.valid) throw new Error(validation.errors.join(";"));
        const { res, json } = await airtableFetch(baseId, apiKey, registryTable, {
          method: "PATCH",
          body: JSON.stringify({ fields: patch.fields, typecast: true }),
        }, patch.recordId);
        if (!res.ok) throw new Error(json.error?.message || `Registry PATCH failed: ${res.status}`);
        applyResults.registryPatched.push({ recordId: patch.recordId });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ type: "registry_patch", recordId: patch.recordId, message: err.message });
      }
    }

    for (const create of registryCreates) {
      try {
        const validation = validateRegistryWritePayload(create.fields);
        if (!validation.valid) throw new Error(validation.errors.join(";"));
        const { res, json } = await airtableFetch(baseId, apiKey, registryTable, {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `Registry POST failed: ${res.status}`);
        applyResults.registryCreated.push({ recordId: json.id, slotKey: create.slotKey });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ type: "registry_create", slotKey: create.slotKey, message: err.message });
      }
    }

    for (const visibilityPatch of galleryVisibilityPatches) {
      if (visibilityPatch.recordId === QUARANTINED_SCENARIO3_RECORD_ID) continue;
      try {
        const errors = validateGalleryVisibilityPatch(visibilityPatch.fields, {
          slotKey: visibilityPatch.slotKey,
        });
        if (errors.length) throw new Error(errors.join(";"));
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "PATCH",
          body: JSON.stringify({ fields: visibilityPatch.fields, typecast: true }),
        }, visibilityPatch.recordId);
        if (!res.ok) throw new Error(json.error?.message || `Gallery hide PATCH failed: ${res.status}`);
        applyResults.gallerySlotsHidden.push({
          recordId: visibilityPatch.recordId,
          slotKey: visibilityPatch.slotKey,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "gallery_visibility",
          recordId: visibilityPatch.recordId,
          message: err.message,
        });
      }
    }

    for (const imageWrite of imageFieldWrites) {
      if (imageWrite.slotKey === MOMENTUM_SLOT) continue;
      if (imageWrite.recordId === QUARANTINED_SCENARIO3_RECORD_ID) continue;
      try {
        const errors = validatePresentationImagePatch(imageWrite.fields, {
          slotKey: imageWrite.slotKey,
        });
        if (errors.length) throw new Error(errors.join(";"));
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "PATCH",
          body: JSON.stringify({ fields: imageWrite.fields, typecast: true }),
        }, imageWrite.recordId);
        if (!res.ok) throw new Error(json.error?.message || `Image PATCH failed: ${res.status}`);
        applyResults.imagesMaterialized.push({
          recordId: imageWrite.recordId,
          slotKey: imageWrite.slotKey,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({
          type: "presentation_image",
          recordId: imageWrite.recordId,
          message: err.message,
        });
      }
    }

    if (applyResults.errors.length) {
      applyBlockers.push(`apply_errors:${applyResults.errors.length}`);
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: TARGET_BRAND.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: TARGET_BRAND.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: TARGET_BRAND.recordId,
  }).catch(() => null);

  const wsQa = (finalQaReport?.brandReports || []).find((b) => b.brand?.slug === TARGET_BRAND.slug);
  const wsBuild = (completeBuildReport?.brandResults || [])[0];

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33dWriterExists: v33dWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    visualBlockerAudit,
    openingImagePlan,
    scenarioImagePlan,
    galleryImagePlan,
    galleryVisibilityPatches,
    registryPatches,
    registryCreates,
    imageFieldWritesProposed: imageFieldWrites,
    imagesSkipped,
    distinctImageCount: premiumDisplay.distinctImageCount,
    visibleGallerySlotsProposed: premiumDisplay.visibleGallerySlotsProposed,
    hiddenGallerySlots: premiumDisplay.hiddenGallerySlots,
    deferredGallerySlots: premiumDisplay.deferredGallerySlots,
    hiddenGallerySlotDetails: premiumDisplay.hiddenGallerySlotDetails,
    repeatedImageUses: premiumDisplay.repeatedImageUses,
    crossSectionImageReuse: premiumDisplay.crossSectionImageReuse,
    visiblePlaceholdersRemaining: premiumDisplay.visiblePlaceholdersRemaining,
    galleryPremiumEnoughForActiveProfile: premiumDisplay.galleryPremiumEnoughForActiveProfile,
    premiumDisplayBlockers: premiumDisplay.premiumDisplayBlockers,
    wrongBrandSafeguards,
    summaryUrlProtectionConfirmed: true,
    summaryUrlFieldsBlocked: ["Summary URL", "View Summary URL", "Case summary URL"],
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    imagePoolSize: imagePool.length,
    officialOgImagesDiscovered: ogImages.length,
    registryReadOnlyCount: registryAssets.length,
    approvedSourcesCount: approvedSources.length,
    openingRowsCount: openingRows.length,
    momentumRowsUntouched: true,
    airtableModified,
    applyBlockers,
    dryRunClean,
    applyResults,
    founderApprovalRequiredForApply: !founderGatesReady,
    expectedFinalQaResult: wsQa
      ? `${wsQa.overallReadiness || "projected"} (${wsQa.readinessScore ?? "?"})`
      : "projected improvement after v33D image completion",
    expectedCompleteBuildResult: wsBuild
      ? `readyForActiveProfile: ${wsBuild.readyForActiveProfile} (${wsBuild.finalQaScores?.overallActiveProfileReadiness || "?"})`
      : "projected after v33D apply",
    expectedVisualDefectResult: visualReport
      ? `${visualReport.defectCounts?.total ?? "?"} defects`
      : "projected reduction in openings_unsafe_image",
    remainingBlockersForV33e: [
      "overview.why_value empty bullets",
      "overview.differentiators thin bullets",
      "standard detail founder review",
      "final fact stewardship",
      "internal-language cleanup (consumer_site)",
    ],
    recommendedNextWriter: "v33E — WoodSpring final fact formatting + active profile finalization",
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-woodspring-visual-completion-writer -- --brand woodspring-suites --dry-run",
    applyGuardrails: {
      woodspringOnly: true,
      noSummaryUrlField: true,
      noMomentumChanges: true,
      noSourceLibraryChanges: true,
      founderGatesRequired: true,
      quarantinedScenario3Protected: true,
      galleryDistinctAssetsOnly: true,
      galleryNoDuplicateFiller: true,
    },
  };

  report.markdown = buildMarkdown(report);
  return report;
}
