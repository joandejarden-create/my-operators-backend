/**
 * Brand Explorer Radisson Individuals Approved Asset Materialization + Row Reactivation v31E.
 *
 * Materializes only founder-approved Brand Asset Registry assets onto intended presentation
 * rows and reactivates only vetted quarantined rows. Never approves images/facts or
 * modifies Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-approved-asset-materialization-writer-v31E.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS } from "./brand-asset-pr-package-governance.js";
import {
  DISCOVERY_BRAND_CONFIG,
  assessPresentationRowImageGovernance,
  detectWrongBrandSignageRisk,
  findRegistryAssetForPresentationRow,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import {
  detectVisibleCopyIssues,
} from "./brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.js";
import {
  findInternalLanguageInRow,
  isOpeningsEvidenceSlot,
  parseFootprintOpeningLocation,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  EXTERNAL_DISPLAY_STATUS_QUARANTINE,
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND as SUPPRESSION_TARGET,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";

export const WRITER_VERSION = "31E";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-approved-asset-materialization-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-approved-asset-materialization-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-approved-asset-materialization-writer-v31E.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31E-approved-asset-materialization";
export const APPLY_FLAG_FOUNDER = "--founder-approved-radisson-individuals-assets-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_UNAPPROVED_IMAGE = "--confirm-no-unapproved-image-use";

export const REACTIVATION_DISPLAY_STATUS = "Show Trust Label";

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

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.json",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.md",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live Source Library records",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-approved-asset-materialization-writer.js",
  "scripts/brand-explorer-radisson-individuals-approved-asset-materialization-writer.mjs",
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

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
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

export function v31eWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-approved-asset-materialization-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31E`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31E supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

/** Classify registry asset for v31E materialization gates. */
export function classifyRegistryAsset(asset) {
  if (!asset) return "Pending Image Review";
  const status = nz(asset.assetStatus);
  const perm = nz(asset.explorerUsePermission);
  const review = nz(asset.usageReviewStatus);
  const nameHaystack = [asset.assetName, asset.doNotUseReason, asset.reviewNotes].filter(Boolean).join(" ");

  if (
    status === ASSET_STATUS.DO_NOT_USE ||
    perm === "Do Not Use" ||
    review === "Blocked" ||
    /do not use/i.test(nameHaystack)
  ) {
    return "Do Not Use";
  }

  if (
    isRegistryAssetApprovedForExplorer(asset) &&
    status === ASSET_STATUS.APPROVED_EXPLORER
  ) {
    return "Approved";
  }

  if (
    /wrong.?brand|replace needed|replace_and|signage mismatch|quality inn|comfort inn/i.test(
      nameHaystack
    ) ||
    (status === ASSET_STATUS.NEEDS_USAGE_REVIEW &&
      /wrong|replace|signage|mismatch/i.test(nameHaystack))
  ) {
    return "Replace Needed";
  }

  return "Pending Image Review";
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
    body: normalizeBody(f.Body),
    sortOrder: f["Sort Order"],
    externalDisplayStatus: nz(f["External Display Status"]),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
    caseSummaryOverview: nz(f["Case Summary Overview"]),
    caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
    caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
    caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
    caseSummaryTags: nz(f["Case Summary Tags"]),
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
  };
}

function assetImageUrl(asset) {
  return nz(asset?.sourceUrl || asset?.imageUrl);
}

function rowMatchesAsset(row, asset) {
  const slot = nz(asset.recommendedExplorerSlot);
  if (slot && row.slotKey !== slot) return false;

  const rowSource = normalizeUrlKey(row.summaryUrl);
  const assetPage = normalizeUrlKey(asset.sourcePageUrl);
  const assetSource = normalizeUrlKey(asset.sourceUrl);
  if (rowSource && (assetPage === rowSource || assetSource === rowSource)) return true;

  if (isOpeningsEvidenceSlot(row.slotKey)) {
    const location = parseFootprintOpeningLocation(row.title, row.body).toLowerCase();
    const assetHaystack = [asset.assetName, asset.sourcePageUrl, asset.sourceNotes]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    if (location && assetHaystack.includes(location.split(",")[0].trim())) return true;
  }

  if (slot && row.slotKey === slot && classifyRegistryAsset(asset) === "Approved") {
    return true;
  }
  return false;
}

export function assessPropertyMatchClarity(row, asset, brandConfig) {
  if (!row || !asset) {
    return { clear: false, reason: "missing_row_or_asset" };
  }
  if (!rowMatchesAsset(row, asset)) {
    return { clear: false, reason: "slot_or_source_mismatch" };
  }
  const combined = [row.title, row.body, row.caseSummaryOverview, asset.assetName].join("\n");
  const wrongBrand = detectWrongBrandSignageRisk(combined, brandConfig);
  if (wrongBrand) {
    return { clear: false, reason: "wrong_brand_signage_risk", wrongBrand };
  }
  if (isOpeningsEvidenceSlot(row.slotKey) && !nz(row.summaryUrl) && !nz(asset.sourcePageUrl)) {
    return { clear: false, reason: "missing_source_url_for_opening" };
  }
  return { clear: true, reason: "slot_and_source_aligned" };
}

export function assessCopyCleanForReactivation(row) {
  const internalHits = findInternalLanguageInRow(row);
  const visibleIssues = detectVisibleCopyIssues(row);
  const highIssues = [...internalHits, ...visibleIssues].filter((h) =>
    ["high", "critical"].includes(h.severity)
  );
  return {
    clean: highIssues.length === 0,
    internalHits,
    visibleIssues,
    highIssueCount: highIssues.length,
  };
}

export function assessReactivationEligibility(row, asset, brandConfig) {
  const classification = classifyRegistryAsset(asset);
  const copyGate = assessCopyCleanForReactivation(row);
  const propertyGate = assessPropertyMatchClarity(row, asset, brandConfig);

  const eligible =
    classification === "Approved" &&
    copyGate.clean &&
    propertyGate.clear &&
    row.quarantined;

  let blockReason = null;
  if (!row.quarantined) blockReason = "row_not_quarantined";
  else if (classification !== "Approved") blockReason = `asset_${classification.toLowerCase().replace(/\s+/g, "_")}`;
  else if (!copyGate.clean) blockReason = "internal_or_source_capture_copy";
  else if (!propertyGate.clear) blockReason = propertyGate.reason;

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    title: row.title,
    registryRecordId: asset?.id || null,
    assetClassification: classification,
    copyGate,
    propertyGate,
    eligible,
    blockReason,
  };
}

function findTargetRowForAsset(asset, allRows) {
  const slot = nz(asset.recommendedExplorerSlot);
  const candidates = allRows.filter((r) => {
    if (slot && r.slotKey === slot) return true;
    return rowMatchesAsset(r, asset);
  });
  if (!candidates.length) return null;
  if (candidates.length === 1) return candidates[0];
  const quarantined = candidates.find((r) => r.quarantined);
  return quarantined || candidates[0];
}

function imageAlreadyMaterialized(row, asset) {
  const targetUrl = normalizeUrlKey(assetImageUrl(asset));
  if (!targetUrl) return false;
  return normalizeUrlKey(row.imageUrl) === targetUrl;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-approved-asset-materialization-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_UNAPPROVED_IMAGE,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsApprovedAssetMaterializationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApproved = false,
  noValidationClaim = false,
  noUnapprovedImage = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug];
  if (!brandConfig) throw new Error(`Missing discovery config for ${target.slug}`);

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const brandApi = await fetchBrandApiShape(target.recordId);
  const apiBlocks = brandApi?.brandExplorer?.blocks || [];

  const presentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const allRows = presentationRaw.map(normalizePresentationRow);
  const quarantinedRows = allRows.filter((r) => r.quarantined);
  const visibleRows = allRows.filter((r) => r.visibleInExplorer);

  const registryAssetsRaw = await listRegistryAssetsForBrand(target.recordId).catch(() => []);

  const assetApprovalSummary = {
    approved: [],
    doNotUse: [],
    replaceNeeded: [],
    pendingImageReview: [],
  };

  for (const asset of registryAssetsRaw) {
    const classification = classifyRegistryAsset(asset);
    const entry = {
      id: asset.id,
      assetName: asset.assetName,
      assetStatus: asset.assetStatus,
      explorerUsePermission: asset.explorerUsePermission,
      usageReviewStatus: asset.usageReviewStatus,
      recommendedExplorerSlot: asset.recommendedExplorerSlot,
      sourceUrl: assetImageUrl(asset) || null,
      classification,
    };
    if (classification === "Approved") assetApprovalSummary.approved.push(entry);
    else if (classification === "Do Not Use") assetApprovalSummary.doNotUse.push(entry);
    else if (classification === "Replace Needed") assetApprovalSummary.replaceNeeded.push(entry);
    else assetApprovalSummary.pendingImageReview.push(entry);
  }

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const visualBefore = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const completeBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandsArg: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const qaBrand = finalQaBefore?.brandReports?.[0] || {};
  const completeBrand =
    (completeBefore?.brandReports || []).find((b) => b.slug === target.slug) || {};

  const assetsToMaterialize = [];
  const rowsToReactivate = [];
  const rowsToKeepHidden = [];
  const rowsNeedingReplacementImages = [];
  const proposedUpdates = [];

  for (const asset of registryAssetsRaw) {
    const classification = classifyRegistryAsset(asset);
    const row = findTargetRowForAsset(asset, allRows);

    if (classification === "Do Not Use" && row) {
      rowsToKeepHidden.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        reason: "do_not_use_asset",
        registryRecordId: asset.id,
        quarantined: row.quarantined,
      });
      continue;
    }

    if (classification === "Replace Needed") {
      if (row) {
        rowsNeedingReplacementImages.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          title: row.title,
          registryRecordId: asset.id,
          assetName: asset.assetName,
          reason: "replace_needed_asset",
        });
        rowsToKeepHidden.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          title: row.title,
          reason: "replace_needed_asset",
          registryRecordId: asset.id,
          quarantined: row.quarantined,
        });
      }
      continue;
    }

    if (classification === "Pending Image Review") {
      if (row) {
        rowsToKeepHidden.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          title: row.title,
          reason: "pending_image_review",
          registryRecordId: asset.id,
          quarantined: row.quarantined,
        });
      }
      continue;
    }

    if (classification === "Approved") {
      const imageUrl = assetImageUrl(asset);
      if (!imageUrl) continue;

      if (row) {
        const reactivation = assessReactivationEligibility(row, asset, brandConfig);
        const needsImage = isVisualImageSlot(row.slotKey) && !imageAlreadyMaterialized(row, asset);

        if (needsImage) {
          const materializeUpdate = {
            action: "materialize_approved_image",
            recordId: row.recordId,
            slotKey: row.slotKey,
            registryRecordId: asset.id,
            fixReason: "founder_approved_asset_materialization",
            fields: {
              Image: [{ url: imageUrl }],
              "Brand Name": target.name,
              Brand: [target.recordId],
            },
            before: { imageUrl: row.imageUrl },
            after: { imageUrl },
          };
          assetsToMaterialize.push({
            registryRecordId: asset.id,
            presentationRecordId: row.recordId,
            slotKey: row.slotKey,
            imageUrl,
            title: row.title,
          });
          proposedUpdates.push(materializeUpdate);
        }

        if (reactivation.eligible) {
          const reactivateUpdate = {
            action: "reactivate_quarantined_row",
            recordId: row.recordId,
            slotKey: row.slotKey,
            registryRecordId: asset.id,
            fixReason: "vetted_row_reactivation_with_approved_asset",
            fields: {
              "External Display Status": REACTIVATION_DISPLAY_STATUS,
              Active: true,
              "Brand Name": target.name,
              Brand: [target.recordId],
            },
            before: { externalDisplayStatus: row.externalDisplayStatus },
            after: { externalDisplayStatus: REACTIVATION_DISPLAY_STATUS },
            reactivationAssessment: reactivation,
          };
          rowsToReactivate.push({
            recordId: row.recordId,
            slotKey: row.slotKey,
            title: row.title,
            registryRecordId: asset.id,
            copyClean: reactivation.copyGate.clean,
            propertyMatchClear: reactivation.propertyGate.clear,
          });
          proposedUpdates.push(reactivateUpdate);
        } else if (row.quarantined) {
          rowsToKeepHidden.push({
            recordId: row.recordId,
            slotKey: row.slotKey,
            title: row.title,
            reason: reactivation.blockReason || "reactivation_gate_failed",
            registryRecordId: asset.id,
            quarantined: true,
          });
        }
      }
    }
  }

  for (const row of quarantinedRows) {
    if (rowsToReactivate.some((r) => r.recordId === row.recordId)) continue;
    if (rowsToKeepHidden.some((r) => r.recordId === row.recordId)) continue;

    const registryMatch = findRegistryAssetForPresentationRow(registryAssetsRaw, row);
    const classification = classifyRegistryAsset(registryMatch);
    const copyGate = assessCopyCleanForReactivation(row);

    if (!copyGate.clean) {
      rowsToKeepHidden.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        reason: "internal_or_source_capture_copy",
        registryRecordId: registryMatch?.id || null,
        quarantined: true,
      });
      continue;
    }

    if (!registryMatch || classification !== "Approved") {
      rowsToKeepHidden.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        reason: registryMatch ? `asset_${classification.toLowerCase().replace(/\s+/g, "_")}` : "no_approved_registry_asset",
        registryRecordId: registryMatch?.id || null,
        quarantined: true,
      });
    }
  }

  for (const row of visibleRows) {
    if (!isVisualImageSlot(row.slotKey) || !row.hasImage) continue;
    const registryMatch = findRegistryAssetForPresentationRow(registryAssetsRaw, row);
    const approved = isRegistryAssetApprovedForExplorer(registryMatch);
    if (!approved) {
      rowsNeedingReplacementImages.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        registryRecordId: registryMatch?.id || null,
        reason: "visible_unapproved_image",
      });
    }
  }

  const applyBlockers = [];

  const wouldMaterializeUnapproved = proposedUpdates.some((u) => {
    if (u.action !== "materialize_approved_image") return false;
    const asset = registryAssetsRaw.find((a) => a.id === u.registryRecordId);
    return classifyRegistryAsset(asset) !== "Approved";
  });
  if (wouldMaterializeUnapproved) applyBlockers.push("unapproved_images_would_be_materialized");

  const wouldUseDoNotUse = proposedUpdates.some((u) => {
    const asset = registryAssetsRaw.find((a) => a.id === u.registryRecordId);
    return classifyRegistryAsset(asset) === "Do Not Use";
  });
  if (wouldUseDoNotUse) applyBlockers.push("do_not_use_assets_would_be_used");

  const wouldUsePending = proposedUpdates.some((u) => {
    const asset = registryAssetsRaw.find((a) => a.id === u.registryRecordId);
    return classifyRegistryAsset(asset) === "Pending Image Review";
  });
  if (wouldUsePending) applyBlockers.push("pending_images_would_be_used");

  const wouldReactivateUnsafe = rowsToReactivate.some((r) => {
    const row = allRows.find((x) => x.recordId === r.recordId);
    const asset = registryAssetsRaw.find((a) => a.id === r.registryRecordId);
    const reactivation = assessReactivationEligibility(row, asset, brandConfig);
    return !reactivation.eligible;
  });
  if (wouldReactivateUnsafe) applyBlockers.push("unsupported_property_examples_would_be_reactivated");

  const reactivatedWithDirtyCopy = rowsToReactivate.filter((r) => !r.copyClean);
  if (reactivatedWithDirtyCopy.length) {
    applyBlockers.push("internal_source_capture_copy_would_be_visible");
  }

  for (const update of proposedUpdates) {
    if (update.action !== "reactivate_quarantined_row") continue;
    const row = allRows.find((r) => r.recordId === update.recordId);
    const copyGate = assessCopyCleanForReactivation(row);
    if (!copyGate.clean) applyBlockers.push("internal_source_capture_copy_would_be_visible");
  }

  const hasWork = proposedUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && founderApproved && noValidationClaim && noUnapprovedImage;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let applyResults = {
    created: [],
    updated: [],
    errors: [],
    imagesApproved: false,
    imagesMaterialized: false,
    factsApproved: false,
    registryFieldsModified: false,
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of proposedUpdates) {
      const asset = registryAssetsRaw.find((a) => a.id === update.registryRecordId);
      const classification = classifyRegistryAsset(asset);

      if (update.action === "materialize_approved_image") {
        if (classification !== "Approved") {
          applyResults.errors.push({
            recordId: update.recordId,
            error: "blocked_unapproved_asset_materialization",
          });
          continue;
        }
        if (!assetImageUrl(asset)) {
          applyResults.errors.push({
            recordId: update.recordId,
            error: "approved_asset_missing_source_url",
          });
          continue;
        }
      }

      if (update.action === "reactivate_quarantined_row") {
        const row = allRows.find((r) => r.recordId === update.recordId);
        const reactivation = assessReactivationEligibility(row, asset, brandConfig);
        if (!reactivation.eligible) {
          applyResults.errors.push({
            recordId: update.recordId,
            error: `reactivation_blocked:${reactivation.blockReason}`,
          });
          continue;
        }
      }

      const liveRec = presentationRaw.find((r) => r.id === update.recordId);
      if (!liveRec) {
        applyResults.errors.push({ recordId: update.recordId, error: "record_not_found" });
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
          action: update.action,
          error: json.error?.message || `PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.updated.push({
        recordId: update.recordId,
        action: update.action,
        slotKey: update.slotKey,
      });
      if (update.action === "materialize_approved_image") {
        applyResults.imagesMaterialized = true;
      }
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
    if (!hasWork) applyResults.note = "no_approved_assets_or_eligible_reactivations";
  }

  const approvedCount = assetApprovalSummary.approved.length;
  const reactivateCount = rowsToReactivate.length;
  const expectedFinalQaNumeric = Math.min(
    92,
    (qaBrand.scores?.overallNumeric || 80) +
      reactivateCount * 2 +
      assetsToMaterialize.length * 3
  );
  const readyForActiveProfile =
    approvedCount > 0 &&
    assetsToMaterialize.length > 0 &&
    reactivateCount > 0 &&
    rowsToKeepHidden.filter((r) => r.reason?.includes("pending")).length === 0;

  const expectedAfter = {
    overallNumeric: expectedFinalQaNumeric,
    overallActiveProfileReadiness: readyForActiveProfile ? "almost_ready" : "almost_ready",
    readyForActiveProfile,
    note:
      approvedCount === 0
        ? "No founder-approved registry assets yet — materialization and reactivation blocked until Airtable registry approval."
        : reactivateCount > 0
          ? "Approved assets materialized and vetted openings reactivated; verify visual audit before active-profile."
          : "Approved assets may materialize; quarantined rows remain hidden until copy/property gates pass.",
  };

  const report = {
    writerVersion: WRITER_VERSION,
    v31EWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    assetApprovalSummary,
    assetsToMaterialize,
    rowsToReactivate,
    rowsToKeepHidden,
    rowsNeedingReplacementImages,
    proposedUpdates,
    currentReadinessDiagnosis: {
      finalQaScore: qaBrand.scores?.overallNumeric ?? null,
      finalQaReadiness: qaBrand.scores?.overallActiveProfileReadiness ?? null,
      finalQaDefectCount: qaBrand.defects?.length ?? null,
      visualAuditDefectCount: visualBefore?.defectCounts?.total ?? null,
      completeBuildScore: completeBrand.finalQaScores?.overallNumeric ?? null,
      quarantinedRowCount: quarantinedRows.length,
      visibleRowCount: visibleRows.length,
      registryAssetCount: registryAssetsRaw.length,
      approvedAssetCount: approvedCount,
    },
    imagesApprovedByWriter: false,
    factsApprovedByWriter: false,
    imagesMaterializedByWriter: applyResults.imagesMaterialized,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyBlockers,
    dryRunClean,
    canApply,
    hasWork,
    applyResults,
    expectedFinalQaAfterApply: expectedAfter,
    expectedActiveProfileAfterApply: expectedAfter,
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-approved-asset-materialization-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    governanceNote:
      "v31E materializes only founder-approved registry assets and reactivates only vetted quarantined rows — never approves images/facts in registry, never modifies Company Validated.",
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const d = report.currentReadinessDiagnosis || {};
  const s = report.assetApprovalSummary || {};
  const lines = [];
  lines.push(
    `# Brand Explorer Radisson Individuals Approved Asset Materialization v${report.writerVersion}`
  );
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v31E exists: **${report.v31EWriterExists ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Asset approval summary");
  lines.push(`- Approved: **${s.approved?.length ?? 0}**`);
  lines.push(`- Do Not Use: **${s.doNotUse?.length ?? 0}**`);
  lines.push(`- Replace Needed: **${s.replaceNeeded?.length ?? 0}**`);
  lines.push(`- Pending Image Review: **${s.pendingImageReview?.length ?? 0}**`);
  lines.push("");
  lines.push("## Materialization plan");
  lines.push(`- Assets to materialize: **${report.assetsToMaterialize?.length ?? 0}**`);
  lines.push(`- Rows to reactivate: **${report.rowsToReactivate?.length ?? 0}**`);
  lines.push(`- Rows to keep hidden: **${report.rowsToKeepHidden?.length ?? 0}**`);
  lines.push(`- Rows needing replacement images: **${report.rowsNeedingReplacementImages?.length ?? 0}**`);
  lines.push("");
  lines.push("## Governance");
  lines.push(`- Images approved by writer: **no**`);
  lines.push(`- Facts approved by writer: **no**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Current readiness");
  lines.push(`- Final QA: **${d.finalQaScore ?? "—"}** (${d.finalQaReadiness ?? "—"})`);
  lines.push(`- Visual defects: **${d.visualAuditDefectCount ?? "—"}**`);
  lines.push(`- Quarantined rows: **${d.quarantinedRowCount ?? 0}**`);
  lines.push("");
  lines.push("## Expected after apply");
  lines.push(
    `- Final QA (est.): **${report.expectedFinalQaAfterApply?.overallNumeric ?? "—"}** (${report.expectedActiveProfileAfterApply?.overallActiveProfileReadiness ?? "not_ready"})`
  );
  lines.push(`- Ready for active-profile: **${report.expectedActiveProfileAfterApply?.readyForActiveProfile ? "yes" : "no"}**`);
  lines.push(`- Note: ${report.expectedActiveProfileAfterApply?.note ?? ""}`);
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — resolve blockers first)");
  return lines.join("\n");
}
