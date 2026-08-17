/**
 * Brand Explorer Radisson Individuals Gallery Image Restore + Image Governance Scope Fix v31D-R1.
 *
 * Restores gallery images cleared by v31D solely for unapproved status.
 * Pending gallery images remain visible in draft/internal views but block active-profile readiness.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-gallery-restore-writer-v31D-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  applyRegistryRecords,
  listRegistryAssetsForBrand,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS } from "./brand-asset-pr-package-governance.js";
import {
  DISCOVERY_BRAND_CONFIG,
  detectWrongBrandSignageRisk,
  galleryPendingReviewBlocksActiveProfile,
  isGalleryImageSlot,
  isRegistryAssetApprovedForExplorer,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND as SUPPRESSION_TARGET,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";

export const WRITER_VERSION = "31D-R1";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-gallery-restore-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-gallery-restore-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-gallery-restore-writer-v31D-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31D-R1-gallery-image-restore";
export const APPLY_FLAG_RESTORE =
  "--restore-safe-gallery-images-as-pending-review";
export const APPLY_FLAG_NO_IMAGE_APPROVAL = "--confirm-no-image-approval";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const TARGET_BRAND = SUPPRESSION_TARGET;

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
  "suburban-studios",
  "woodspring-suites",
  "everhome-suites",
  ...WAVE1_EXPANSION_SLUGS.filter((s) => s !== TARGET_BRAND.slug),
]);

export const GALLERY_SLOT_RECORDS = Object.freeze([
  { slotKey: "materials.gallery.1", recordId: "recbuxkGK4Uh6Hq0y" },
  { slotKey: "materials.gallery.2", recordId: "recEOOCEnmy48hRiy" },
  { slotKey: "materials.gallery.3", recordId: "recmuusPraVbFUxyK" },
  { slotKey: "materials.gallery.4", recordId: "recZWoVDGYpJdTBP1" },
  { slotKey: "materials.gallery.5", recordId: "rectMcyJ3FaVqG1ly" },
  { slotKey: "materials.gallery.6", recordId: "reco6kIWyBsLOwohj" },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const STAGING_RUN_ID = "v31D-R1-gallery-restore";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.md",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.json",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.json",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.md",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json",
  "live Radisson Individuals presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-gallery-restore-writer.js",
  "scripts/brand-explorer-radisson-individuals-gallery-restore-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "lib/partner-intelligence/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
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

function isRegistryDoNotUse(asset) {
  if (!asset) return false;
  return (
    nz(asset.assetStatus) === ASSET_STATUS.DO_NOT_USE ||
    nz(asset.explorerUsePermission) === "Do Not Use" ||
    nz(asset.usageReviewStatus) === "Blocked"
  );
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

async function listPresentationRowsRaw(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, PRESENTATION_TABLE)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    fields: f,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    externalDisplayStatus: nz(f["External Display Status"]),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
  };
}

function loadJsonReport(relativePath) {
  const full = path.join(ROOT, relativePath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

/** Recover pre-v31D gallery image URLs from v31B discovery and v31D cleanup reports. */
export function recoverPriorGalleryImages() {
  const discovery = loadJsonReport("reports/brand-explorer-brand-asset-registry-discovery-writer.json");
  const v31d = loadJsonReport(
    "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json"
  );

  const fromDiscovery = new Map();
  for (const entry of discovery?.imageUsageAudit || []) {
    if (!isGalleryImageSlot(entry?.slot)) continue;
    fromDiscovery.set(entry.slot, {
      slotKey: entry.slot,
      recordId: entry.presentationRowId || null,
      title: nz(entry.title),
      imageUrl: nz(entry.imageUrl) || null,
      sourceUrl: nz(entry.sourceUrl) || null,
      registryRecordId: entry.registryRecordId || null,
      registryApproved: Boolean(entry.registryApproved),
      brandMatched: entry.brandMatched !== false,
      wrongBrandRisk: entry.wrongBrandRisk || null,
      recommendation: nz(entry.recommendation),
      recoverySource: "v31B_imageUsageAudit",
    });
  }

  const fromV31dClears = new Map();
  for (const update of v31d?.rowsWouldUpdate || v31d?.proposedUpdates || []) {
    if (update.action !== "clear_unapproved_image") continue;
    if (!isGalleryImageSlot(update.slotKey)) continue;
    fromV31dClears.set(update.slotKey, {
      slotKey: update.slotKey,
      recordId: update.recordId,
      clearedByV31D: true,
      recoverySource: "v31D_clear_unapproved_image",
    });
  }

  const recovered = [];
  for (const slot of GALLERY_SLOT_RECORDS) {
    const discoveryEntry = fromDiscovery.get(slot.slotKey) || null;
    const v31dEntry = fromV31dClears.get(slot.slotKey) || null;
    recovered.push({
      ...slot,
      ...(discoveryEntry || {}),
      recordId: slot.recordId,
      clearedByV31D: Boolean(v31dEntry?.clearedByV31D || (discoveryEntry?.imageUrl && !discoveryEntry?.registryApproved)),
      recoverySources: [discoveryEntry?.recoverySource, v31dEntry?.recoverySource].filter(Boolean),
    });
  }
  return recovered;
}

export function classifyClearedGalleryImage({
  row,
  priorImage,
  registryAssets = [],
  brandConfig,
}) {
  if (!row || !isGalleryImageSlot(row.slotKey)) {
    return { classification: "insufficient_data", reason: "not_gallery_slot" };
  }
  if (row.quarantined) {
    return { classification: "do_not_restore_wrong_brand", reason: "quarantined_presentation_row" };
  }
  if (row.hasImage) {
    return { classification: "insufficient_data", reason: "presentation_row_already_has_image" };
  }
  if (!priorImage?.imageUrl) {
    return { classification: "insufficient_data", reason: "no_prior_image_url_in_recovery_sources" };
  }

  const combined = [row.title, priorImage.title, priorImage.imageUrl].filter(Boolean).join("\n");
  const wrongBrand = detectWrongBrandSignageRisk(combined, brandConfig);
  if (wrongBrand) {
    return {
      classification: "do_not_restore_wrong_brand",
      reason: wrongBrand.reason,
      wrongBrandRisk: wrongBrand,
    };
  }
  if (priorImage.wrongBrandRisk) {
    return {
      classification: "do_not_restore_wrong_brand",
      reason: "v31B_wrong_brand_signal",
      wrongBrandRisk: priorImage.wrongBrandRisk,
    };
  }

  const urlKey = normalizeUrlKey(priorImage.imageUrl);
  for (const asset of registryAssets) {
    if (!isRegistryDoNotUse(asset)) continue;
    const assetUrl = normalizeUrlKey(asset.sourceUrl);
    if (
      assetUrl === urlKey ||
      nz(asset.recommendedExplorerSlot) === row.slotKey ||
      nz(asset.assetName).toLowerCase().includes(row.title.toLowerCase().slice(0, 24))
    ) {
      return {
        classification: "do_not_restore_wrong_brand",
        reason: "do_not_use_registry_asset",
        registryRecordId: asset.id,
      };
    }
  }

  if (priorImage.brandMatched === false) {
    return { classification: "needs_replacement", reason: "brand_mismatch_in_v31B_audit" };
  }

  if (!/radisson individuals|faranda/i.test(combined)) {
    return { classification: "insufficient_data", reason: "unclear_brand_property_alignment" };
  }

  return {
    classification: "safe_to_restore_pending_review",
    reason: "cleared_by_v31D_unapproved_only",
    pendingImageReview: true,
  };
}

function buildGalleryRegistryStagedAsset({ slotKey, title, imageUrl }) {
  const propertyLabel = title.replace(/,?\s*a member of Radisson Individuals/i, "").trim() || title;
  return {
    assetName: `Radisson Individuals — ${propertyLabel} — Gallery Image`,
    assetType: "Exterior / Property",
    assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    sourceBasis: "Company Materials",
    sourceUrl: imageUrl,
    usageReviewStatus: "Pending Review",
    explorerUsePermission: "Candidate Only",
    recommendedExplorerSlot: slotKey,
    isPrimaryCandidate: true,
    reviewNotes:
      "v31D-R1 restored pending gallery image — visible in draft/internal profile; not approved for active-profile evidence.",
    sourceNotes: `Recovered from v31D clear_unapproved_image on ${slotKey}.`,
  };
}

function registryFieldsWouldApprove(fields) {
  return (
    fields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER ||
    fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer" ||
    fields[MAP_BRAND_ASSET.usageReviewStatus] === "Usage Review Complete"
  );
}

export function v31dR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-gallery-restore-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31D-R1`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31D-R1 supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-gallery-restore-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_RESTORE,
    APPLY_FLAG_NO_IMAGE_APPROVAL,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsGalleryRestoreWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  restorePending = false,
  noImageApproval = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug];

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const allRows = presentationRaw.map(normalizePresentationRow);
  const galleryRows = allRows.filter((r) => isGalleryImageSlot(r.slotKey));
  const registryAssetsRaw = await listRegistryAssetsForBrand(target.recordId).catch(() => []);

  const priorGalleryImages = recoverPriorGalleryImages();
  const galleryClearedByV31D = priorGalleryImages.filter(
    (p) => galleryRows.find((r) => r.recordId === p.recordId && !r.hasImage) && p.imageUrl
  );

  const classifications = [];
  const imagesSafeToRestore = [];
  const imagesNotRestored = [];
  const registryAlignment = [];
  const proposedPresentationUpdates = [];
  const proposedRegistryCreates = [];
  const proposedRegistryUpdates = [];

  for (const prior of priorGalleryImages) {
    const row = galleryRows.find((r) => r.recordId === prior.recordId) || {
      recordId: prior.recordId,
      slotKey: prior.slotKey,
      title: prior.title || "",
      hasImage: false,
      quarantined: false,
    };
    const assessment = classifyClearedGalleryImage({
      row,
      priorImage: prior,
      registryAssets: registryAssetsRaw,
      brandConfig,
    });
    classifications.push({
      slotKey: prior.slotKey,
      recordId: prior.recordId,
      title: row.title || prior.title,
      liveHasImage: row.hasImage,
      priorImageUrl: prior.imageUrl || null,
      clearedByV31D: prior.clearedByV31D,
      recoverySources: prior.recoverySources || [],
      ...assessment,
    });

    if (assessment.classification === "safe_to_restore_pending_review") {
      imagesSafeToRestore.push({
        slotKey: prior.slotKey,
        recordId: prior.recordId,
        title: row.title || prior.title,
        imageUrl: prior.imageUrl,
      });

      const existingRegistry = registryAssetsRaw.find(
        (a) =>
          nz(a.recommendedExplorerSlot) === prior.slotKey ||
          normalizeUrlKey(a.sourceUrl) === normalizeUrlKey(prior.imageUrl)
      );

      if (existingRegistry) {
        registryAlignment.push({
          slotKey: prior.slotKey,
          action: "align_existing_registry_pending",
          registryRecordId: existingRegistry.id,
          assetName: existingRegistry.assetName,
          currentStatus: existingRegistry.assetStatus,
          currentPermission: existingRegistry.explorerUsePermission,
          proposedUsageReviewStatus: "Pending Review",
          proposedExplorerPermission: "Candidate Only",
        });
        if (
          isRegistryAssetApprovedForExplorer(existingRegistry) ||
          isRegistryDoNotUse(existingRegistry)
        ) {
          registryAlignment[registryAlignment.length - 1].action = "blocked_registry_state";
        } else if (
          nz(existingRegistry.usageReviewStatus) !== "Pending Review" ||
          nz(existingRegistry.explorerUsePermission) !== "Candidate Only"
        ) {
          proposedRegistryUpdates.push({
            registryRecordId: existingRegistry.id,
            slotKey: prior.slotKey,
            fields: {
              [MAP_BRAND_ASSET.usageReviewStatus]: "Pending Review",
              [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
              [MAP_BRAND_ASSET.recommendedExplorerSlot]: prior.slotKey,
              [MAP_BRAND_ASSET.reviewNotes]:
                "v31D-R1 aligned pending gallery image — not approved for active-profile.",
            },
          });
        }
      } else {
        const staged = buildGalleryRegistryStagedAsset({
          slotKey: prior.slotKey,
          title: row.title || prior.title,
          imageUrl: prior.imageUrl,
        });
        registryAlignment.push({
          slotKey: prior.slotKey,
          action: "create_registry_pending",
          assetName: staged.assetName,
          recommendedExplorerSlot: prior.slotKey,
          usageReviewStatus: "Pending Review",
          explorerUsePermission: "Candidate Only",
        });
        proposedRegistryCreates.push(staged);
      }

      proposedPresentationUpdates.push({
        action: "restore_gallery_image_pending_review",
        recordId: prior.recordId,
        slotKey: prior.slotKey,
        fixReason: "v31D_cleared_unapproved_gallery_image_restore_as_pending",
        fields: {
          Image: [{ url: prior.imageUrl }],
          "Brand Name": target.name,
          Brand: [target.recordId],
        },
        before: { hasImage: false, imageUrl: null },
        after: { hasImage: true, imageUrl: prior.imageUrl, pendingImageReview: true },
      });
    } else {
      imagesNotRestored.push({
        slotKey: prior.slotKey,
        recordId: prior.recordId,
        title: row.title || prior.title,
        classification: assessment.classification,
        reason: assessment.reason,
      });
    }
  }

  const applyBlockers = [];
  if (imagesSafeToRestore.length === 0) {
    applyBlockers.push("no_safe_gallery_images_to_restore");
  }
  if (proposedPresentationUpdates.some((u) => registryFieldsWouldApprove(u.fields))) {
    applyBlockers.push("images_would_be_marked_approved");
  }
  if (
    proposedRegistryCreates.some((s) => s.assetStatus === ASSET_STATUS.APPROVED_EXPLORER) ||
    proposedRegistryUpdates.some((u) => registryFieldsWouldApprove(u.fields))
  ) {
    applyBlockers.push("registry_would_approve_images");
  }
  if (
    imagesNotRestored.some((i) => i.classification === "do_not_restore_wrong_brand") &&
    proposedPresentationUpdates.length
  ) {
    // mixed batch ok — only safe ones restore
  }
  const wouldRestoreDoNotUse = imagesSafeToRestore.some((img) => {
    const match = registryAssetsRaw.find(
      (a) =>
        isRegistryDoNotUse(a) &&
        (normalizeUrlKey(a.sourceUrl) === normalizeUrlKey(img.imageUrl) ||
          nz(a.recommendedExplorerSlot) === img.slotKey)
    );
    return Boolean(match);
  });
  if (wouldRestoreDoNotUse) applyBlockers.push("do_not_use_images_would_be_restored");

  const wouldReactivateQuarantined = proposedPresentationUpdates.some((u) => {
    const row = allRows.find((r) => r.recordId === u.recordId);
    return row?.quarantined;
  });
  if (wouldReactivateQuarantined) applyBlockers.push("quarantined_rows_would_be_reactivated");

  const brandApi = await fetchBrandApiShape(target.slug);
  const galleryBlocksActiveProfile = galleryPendingReviewBlocksActiveProfile(
    brandApi,
    registryAssetsRaw,
    brandConfig,
    { slug: target.slug, resolution: { resolutionSource: "expansion_backlog" } }
  );

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const qaBrand = finalQaBefore?.brandReports?.[0] || {};

  const completeBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));
  const completeBrand =
    (completeBefore?.brandReports || []).find((b) => b.slug === target.slug) || {};

  const hasWork =
    proposedPresentationUpdates.length > 0 ||
    proposedRegistryCreates.length > 0 ||
    proposedRegistryUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && restorePending && noImageApproval && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let applyResults = {
    presentationUpdated: [],
    registryCreated: [],
    registryUpdated: [],
    errors: [],
    imagesApproved: false,
    companyValidatedChanged: false,
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    if (proposedRegistryCreates.length) {
      const registryApply = await applyRegistryRecords({
        brandRecordId: target.recordId,
        parentCompany: brandConfig?.parentCompany || "Choice Hotels International",
        stagedAssets: proposedRegistryCreates,
        stagingRunId: STAGING_RUN_ID,
      });
      applyResults.registryCreated = registryApply.created || [];
      if ((registryApply.created || []).length) airtableModified = true;
    }

    for (const regUpdate of proposedRegistryUpdates) {
      if (registryFieldsWouldApprove(regUpdate.fields)) {
        applyResults.errors.push({
          registryRecordId: regUpdate.registryRecordId,
          error: "blocked_registry_approval_fields",
        });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        "Partner Intelligence - Brand Asset Registry",
        { method: "PATCH", body: JSON.stringify({ fields: regUpdate.fields, typecast: true }) },
        regUpdate.registryRecordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          registryRecordId: regUpdate.registryRecordId,
          error: json.error?.message || `PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.registryUpdated.push(regUpdate.registryRecordId);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const update of proposedPresentationUpdates) {
      const row = allRows.find((r) => r.recordId === update.recordId);
      if (row?.quarantined) {
        applyResults.errors.push({ recordId: update.recordId, error: "quarantined_row_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: json.error?.message || `PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.presentationUpdated.push({
        recordId: update.recordId,
        slotKey: update.slotKey,
      });
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }

    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
    applyResults.companyValidatedChanged =
      JSON.stringify(companyValidatedBefore) !== JSON.stringify(companyValidatedAfter);
    applyResults.imagesApproved = false;
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const expectedUiAfterApply =
    imagesSafeToRestore.length > 0
      ? `Materials gallery shows ${imagesSafeToRestore.length} property images with pending-review trust posture; draft/internal profile renders gallery cards.`
      : "Gallery remains empty until safe images are restored.";

  const expectedActiveProfileAfterApply = {
    readyForActiveProfile: false,
    blockedByPendingGalleryImages: galleryBlocksActiveProfile || imagesSafeToRestore.length > 0,
    note:
      "Pending gallery images do not count as approved visual evidence — active-profile remains blocked until founder approves registry assets.",
  };

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply && canApply ? "apply" : "dry-run",
    v31dR1WriterExists: v31dR1WriterExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    imagesApproved: false,
    airtableModified,
    dryRunClean,
    applyBlockers,
    galleryClearedByV31D,
    priorGalleryImages,
    classifications,
    imagesSafeToRestore,
    imagesNotRestored,
    registryAlignment,
    rowsToUpdate: proposedPresentationUpdates,
    proposedRegistryCreates,
    proposedRegistryUpdates,
    applyResults,
    currentReadinessDiagnosis: {
      finalQaScore: qaBrand.scores?.overallNumeric,
      finalQaReadiness: qaBrand.scores?.overallActiveProfileReadiness,
      completeBuildReady: completeBrand.readyForActiveProfile,
      galleryBlocksActiveProfile,
    },
    expectedUiResult: expectedUiAfterApply,
    expectedActiveProfileResult: expectedActiveProfileAfterApply,
    imageGovernanceScopeFix:
      "Gallery pending images remain visible in draft/internal views; only wrong-brand / Do Not Use images are cleared by v31D going forward.",
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
    markdown: "",
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Gallery Image Restore v31D-R1`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31D-R1 exists: **${report.v31dR1WriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    `- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`,
    `- Gallery cleared by v31D: **${report.galleryClearedByV31D.length}**`,
    `- Safe to restore: **${report.imagesSafeToRestore.length}**`,
    `- Not restored: **${report.imagesNotRestored.length}**`,
    `- Rows to update: **${report.rowsToUpdate.length}**`,
    `- Registry creates: **${report.proposedRegistryCreates.length}**`,
    `- Images approved: **no**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Files read",
    ...report.filesRead.map((f) => `- ${f}`),
    "",
    "## Files changed",
    ...report.filesChanged.map((f) => `- ${f}`),
    "",
    "## Gallery classifications",
    ...report.classifications.map(
      (c) =>
        `- \`${c.slotKey}\` **${c.classification}** — ${c.reason}${c.priorImageUrl ? " (prior URL recovered)" : ""}`
    ),
    "",
    "## Images not restored",
    ...(report.imagesNotRestored.length
      ? report.imagesNotRestored.map((i) => `- \`${i.slotKey}\` ${i.classification}: ${i.reason}`)
      : ["- (none)"]),
    "",
    "## Brand Asset Registry alignment",
    ...report.registryAlignment.map(
      (r) =>
        `- \`${r.slotKey}\` ${r.action}${r.registryRecordId ? ` (${r.registryRecordId})` : ""}`
    ),
    "",
    "## Expected UI result",
    report.expectedUiResult,
    "",
    "## Expected active-profile readiness",
    `- Ready: **${report.expectedActiveProfileResult.readyForActiveProfile ? "yes" : "no"}**`,
    `- Blocked by pending gallery: **${report.expectedActiveProfileResult.blockedByPendingGalleryImages ? "yes" : "no"}**`,
    `- ${report.expectedActiveProfileResult.note}`,
    "",
  ];
  if (report.exactApplyCommand) {
    lines.push("## Exact apply command", "```bash", report.exactApplyCommand, "```", "");
  }
  if (report.applyBlockers.length) {
    lines.push("## Apply blockers", ...report.applyBlockers.map((b) => `- ${b}`), "");
  }
  return lines.join("\n");
}
