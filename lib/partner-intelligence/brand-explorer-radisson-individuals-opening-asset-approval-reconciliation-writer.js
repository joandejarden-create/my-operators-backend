/**
 * Brand Explorer Radisson Individuals Opening Asset Approval Reconciliation +
 * Final Reactivation Writer v31M-R1.
 *
 * Reconciles duplicate founder-approved opening registry assets, maps canonical
 * assets per presentation row, materializes images, and reactivates eligible
 * openings to reach the 3/3 complete build gate.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer-v31M-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { uploadFileBytesToAirtable } from "../dealality/airtable-upload-attachment.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import {
  DISCOVERY_BRAND_CONFIG,
  findRegistryAssetForPresentationRow,
  isRegistryAssetApprovedForExplorer,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  classifyRegistryAsset,
  assessReactivationEligibility,
  REACTIVATION_DISPLAY_STATUS,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  findInternalLanguageInRow,
  parseFootprintOpeningLocation,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import {
  OPENINGS_SLOT,
  OPENINGS_PROPERTY_CATALOG,
  PRESS_KIT_URL,
  SHARED_PRESS_KIT_REGISTRY_ID,
  buildOwnerFacingOpeningsCopy,
  canReactivateOpeningsRow,
} from "./brand-explorer-radisson-individuals-openings-rebuild-writer.js";
import {
  openingIsCompleteRow,
  MOMENTUM_PARITY_PACKAGES,
} from "./brand-explorer-radisson-individuals-openings-momentum-parity-writer.js";
import { MOMENTUM_SLOT } from "./brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js";
import {
  isDurableSourcePageUrl,
  isTemporaryAirtableUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { classifyUrlDurability } from "./brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "31M-R1";
export const STAGING_RUN_ID = "v31M-R1-opening-asset-reconciliation";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer-v31M-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31M-R1-opening-asset-approval-reconciliation";
export const APPLY_FLAG_APPROVED_ONLY = "--confirm-approved-assets-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_FOUNDER = "--confirm-founder-approved-opening-assets-already-reviewed";

export const PREFERRED_REACTIVATION_RECORD_IDS = Object.freeze([
  "recVtiPqVGo8gUtpO",
  "recto7QMu58eMf5jV",
]);

export const COMPLETE_VISIBLE_BASELINE = "rec0uiWsD44ePqr6M";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";
const REQUIRED_COMPLETE_OPENINGS = 3;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-openings-momentum-parity-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-momentum-parity-writer.json",
  "reports/brand-explorer-radisson-individuals-openings-rebuild-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-rebuild-writer.json",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.md",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "live Radisson Individuals footprint.openings presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live API response for Radisson Individuals",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.js",
  "scripts/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.mjs",
  "docs/data-intelligence/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer-v31M-R1.md",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.md",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json",
  "package.json",
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

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function normalizeNameKey(value) {
  return nz(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function v31mR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31M-R1`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31M-R1 supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function catalogForRow(recordId) {
  return OPENINGS_PROPERTY_CATALOG.find((c) => c.presentationRecordId === recordId) || null;
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

function normalizeBody(body) {
  if (Array.isArray(body)) return body.join("\n\n");
  return nz(body);
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
    active: f.Active !== false,
    externalDisplayStatus: nz(f["External Display Status"]),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
    sourcePageUrl: nz(f["Source Page URL"]),
  };
}

function normalizeOpeningsRow(rec) {
  const row = normalizePresentationRow(rec);
  if (row.slotKey !== OPENINGS_SLOT) return null;
  return row;
}

function normalizeMomentumRow(rec) {
  const row = normalizePresentationRow(rec);
  if (row.slotKey !== MOMENTUM_SLOT) return null;
  return row;
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

async function listRegistryExtended(baseId, apiKey, brandRecordId) {
  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${escapeFormulaValue(brandRecordId)}'`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BRAND_ASSET_REGISTRY_TABLE)}?${params}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Registry list failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records.map((rec) => {
    const normalized = normalizeRegistryRecordExtended(rec);
    const f = rec.fields || {};
    return {
      ...normalized,
      stagingRunId: nz(f[MAP_BRAND_ASSET.stagingRunId]),
      recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    };
  });
}

export function isOpeningImageRegistryAsset(asset) {
  if (!asset) return false;
  const slot = nz(asset.recommendedExplorerSlot);
  const section = nz(asset.explorerSection);
  if (slot === OPENINGS_SLOT) return true;
  if (section === "Recent Openings") return true;
  if (/opening|footprint\.openings/i.test(nz(asset.slotPurpose))) return true;
  return false;
}

export function isSharedSourceReferenceRegistry(registry) {
  if (!registry) return false;
  if (registry.id === SHARED_PRESS_KIT_REGISTRY_ID) return true;
  const name = nz(registry.assetName).toLowerCase();
  if (name.includes("press kit") && name.includes("source reference")) return true;
  if (!registry.attachmentUrl && !registry.sourceUrl) return true;
  return false;
}

function registryAssetImageUrl(asset) {
  return nz(asset?.attachmentUrl || asset?.sourceUrl);
}

export function assessAssetApprovalGates(asset) {
  const classification = classifyRegistryAsset(asset);
  const approved =
    !isDoNotUseRecord(asset) &&
    (isFounderApprovedRecord(asset) || isRegistryAssetApprovedForExplorer(asset)) &&
    classification === "Approved";
  const urlAudit = classifyUrlDurability(
    asset?.sourceUrl,
    asset?.sourcePageUrl,
    asset?.attachmentUrl
  );
  const hasUsableImage = Boolean(registryAssetImageUrl(asset));
  const propertySpecific =
    !isSharedSourceReferenceRegistry(asset) &&
    Boolean(
      nz(asset.relatedPropertyName) ||
        parsePresentationRowIdFromNotes(asset.sourceNotes) ||
        /faranda|hotel|collection|bolivar|bogota|cucuta|cartagena/i.test(asset.assetName)
    );
  const materializable =
    approved &&
    hasUsableImage &&
    urlAudit.materializable !== false &&
    !isTemporaryAirtableUrl(registryAssetImageUrl(asset)) &&
    propertySpecific;
  return {
    classification,
    approvedAccordingToGates: approved,
    materializable,
    urlAudit,
    propertySpecific,
    hasUsableImage,
    isDoNotUse: isDoNotUseRecord(asset),
  };
}

function assetMatchesPresentationRow(asset, presentationRecordId, catalog) {
  const notesId = parsePresentationRowIdFromNotes(asset.sourceNotes);
  if (notesId === presentationRecordId) return true;
  const propKey = normalizeNameKey(catalog?.propertyName || "");
  const relatedKey = normalizeNameKey(asset.relatedPropertyName || "");
  const nameKey = normalizeNameKey(asset.assetName || "");
  const cityKey = normalizeNameKey(catalog?.marketCity || "");
  if (propKey && (relatedKey.includes(propKey) || propKey.includes(relatedKey))) return true;
  if (cityKey && (nameKey.includes(cityKey) || relatedKey.includes(cityKey))) return true;
  if (
    catalog?.sourcePageUrl &&
    catalog.sourcePageUrl !== PRESS_KIT_URL &&
    normalizeUrlKey(asset.sourcePageUrl) === normalizeUrlKey(catalog.sourcePageUrl)
  ) {
    return true;
  }
  for (const kw of catalog?.titleKeywords || []) {
    if (nameKey.includes(normalizeNameKey(kw))) return true;
  }
  return false;
}

function scoreCanonicalCandidate(asset, presentationRecordId, catalog) {
  let score = 0;
  const gates = assessAssetApprovalGates(asset);
  if (gates.isDoNotUse) return -10000;
  if (isSharedSourceReferenceRegistry(asset)) return -5000;
  if (parsePresentationRowIdFromNotes(asset.sourceNotes) === presentationRecordId) score += 500;
  if (gates.approvedAccordingToGates) score += 400;
  if (asset.attachmentUrl) score += 300;
  else if (asset.sourceUrl) score += 150;
  if (/v31L/i.test(nz(asset.stagingRunId))) score += 200;
  if (/v31M-R1/i.test(nz(asset.stagingRunId))) score += 180;
  if (asset.isPrimaryCandidate) score += 100;
  if (assetMatchesPresentationRow(asset, presentationRecordId, catalog)) score += 250;
  if (gates.materializable) score += 120;
  if (gates.urlAudit?.durable) score += 80;
  return score;
}

export function chooseCanonicalAsset(candidates, presentationRecordId, catalog) {
  const scored = (candidates || [])
    .map((asset) => ({
      asset,
      score: scoreCanonicalCandidate(asset, presentationRecordId, catalog),
      gates: assessAssetApprovalGates(asset),
    }))
    .filter((x) => x.score > 0 && x.gates.approvedAccordingToGates)
    .sort((a, b) => b.score - a.score);
  return scored[0] || null;
}

export function buildDuplicateGroupKey(asset) {
  const presentationId = parsePresentationRowIdFromNotes(asset.sourceNotes);
  const prop = normalizeNameKey(asset.relatedPropertyName || asset.assetName);
  const slot = nz(asset.recommendedExplorerSlot) || OPENINGS_SLOT;
  const imageKey = normalizeUrlKey(registryAssetImageUrl(asset)).slice(0, 80);
  if (presentationId) return `row:${presentationId}|${slot}|${imageKey}`;
  return `prop:${prop}|${slot}|${imageKey}`;
}

export function groupDuplicateRegistryAssets(openingAssets) {
  const groups = new Map();
  for (const asset of openingAssets) {
    const key = buildDuplicateGroupKey(asset);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(asset);
  }
  return [...groups.entries()].map(([groupKey, assets]) => ({
    groupKey,
    assetCount: assets.length,
    assetIds: assets.map((a) => a.id),
    assets,
    hasApproved: assets.some((a) => assessAssetApprovalGates(a).approvedAccordingToGates),
  }));
}

export function buildCanonicalOpeningAssetMap(openingsRows, openingAssets) {
  const map = [];
  for (const row of openingsRows) {
    const catalog = catalogForRow(row.recordId);
    const candidates = openingAssets.filter((asset) =>
      assetMatchesPresentationRow(asset, row.recordId, catalog)
    );
    const chosen = chooseCanonicalAsset(candidates, row.recordId, catalog);
    const legacyMatch = findRegistryAssetForPresentationRow(openingAssets, row);
    map.push({
      presentationRecordId: row.recordId,
      propertyTitle: catalog?.propertyName || row.title,
      marketCity: catalog?.marketCity || "",
      candidateCount: candidates.length,
      candidateRegistryIds: candidates.map((a) => a.id),
      legacyLinkedRegistryId: legacyMatch?.id || null,
      canonicalRegistryId: chosen?.asset?.id || null,
      canonicalScore: chosen?.score ?? null,
      canonicalGates: chosen?.gates || null,
      duplicatesSuperseded: candidates
        .filter((a) => a.id !== chosen?.asset?.id)
        .map((a) => a.id),
    });
  }
  return map;
}

function verifyMomentumApplyStatus(momentumRows) {
  const results = [];
  let appliedCount = 0;
  for (const pkg of MOMENTUM_PARITY_PACKAGES) {
    const live = momentumRows.find((r) => r.recordId === pkg.recordId);
    const titleMatch = live && nz(live.title) === nz(pkg.polishedTitle);
    const bodyHaystack = nz(live?.body).toLowerCase();
    const urlMatch = bodyHaystack.includes(nz(pkg.sourceUrl).toLowerCase());
    const applied = Boolean(live && titleMatch && urlMatch);
    if (applied) appliedCount += 1;
    results.push({
      recordId: pkg.recordId,
      expectedTitle: pkg.polishedTitle,
      liveTitle: live?.title || null,
      expectedSourceUrl: pkg.sourceUrl,
      expectedLinkLabel: pkg.proposedLinkLabel,
      v31mApplied: applied,
    });
  }
  return {
    v31mFullyApplied: appliedCount === MOMENTUM_PARITY_PACKAGES.length,
    appliedCount,
    expectedCount: MOMENTUM_PARITY_PACKAGES.length,
    rows: results,
    note:
      appliedCount === MOMENTUM_PARITY_PACKAGES.length
        ? "v31M momentum apply has landed — titles and property-specific URLs present."
        : "v31M momentum apply incomplete — run v31M apply before expecting Tribute-parity momentum.",
  };
}

async function downloadImageBuffer(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Image download failed ${res.status}`);
  const buffer = Buffer.from(await res.arrayBuffer());
  const contentType = res.headers.get("content-type") || "image/jpeg";
  return { buffer, contentType };
}

async function materializePresentationImage({ baseId, apiKey, recordId, imageUrl, slotKey }) {
  const { buffer, contentType } = await downloadImageBuffer(imageUrl);
  const filename = `${slotKey.replace(/\./g, "-")}-opening.jpg`.slice(0, 120);
  await uploadFileBytesToAirtable({
    baseId,
    recordId,
    fieldName: IMAGE_FIELD,
    buffer,
    contentType,
    filename,
    apiKey,
  });
  await new Promise((r) => setTimeout(r, 400));
  const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {}, recordId);
  if (!res.ok) throw new Error(json.error?.message || `Reread failed ${recordId}`);
  const url =
    Array.isArray(json.fields?.Image) && json.fields.Image[0]?.url
      ? nz(json.fields.Image[0].url)
      : null;
  return { materialized: Boolean(url), attachmentUrl: url };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_APPROVED_ONLY,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsOpeningAssetApprovalReconciliationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  approvedAssetsOnly = false,
  noValidationClaim = false,
  founderReviewed = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug] || { brandName: target.name, name: target.name };

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [presentationRaw, registryExtended, brandApi] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
    listRegistryExtended(baseId, apiKey, target.recordId),
    fetchBrandApiShape(target.recordId),
  ]);

  const openingsRows = presentationRaw.map(normalizeOpeningsRow).filter(Boolean);
  const momentumRows = presentationRaw.map(normalizeMomentumRow).filter(Boolean);
  const openingAssets = registryExtended.filter(isOpeningImageRegistryAsset);

  const apiOpeningBlocks = (brandApi?.brandExplorer?.blocks || []).filter(
    (b) => nz(b.slotKey) === OPENINGS_SLOT
  );
  const apiBlockByRecordId = new Map(
    apiOpeningBlocks.map((b) => [nz(b.recordId || b.id), b])
  );

  const registryReconciliationAudit = openingAssets.map((asset) => {
    const gates = assessAssetApprovalGates(asset);
    const presentationId = parsePresentationRowIdFromNotes(asset.sourceNotes);
    return {
      registryRecordId: asset.id,
      assetName: asset.assetName,
      relatedPropertyName: asset.relatedPropertyName || null,
      intendedSlot: asset.recommendedExplorerSlot || OPENINGS_SLOT,
      relatedPresentationRow: presentationId,
      hasAttachment: Boolean(asset.attachmentUrl),
      hasSourceUrl: Boolean(asset.sourceUrl),
      imageUrl: registryAssetImageUrl(asset) || null,
      sourcePageUrl: asset.sourcePageUrl || null,
      sourceUrl: asset.sourceUrl || null,
      assetStatus: asset.assetStatus,
      explorerUsePermission: asset.explorerUsePermission,
      usageReviewStatus: asset.usageReviewStatus,
      visualSlotValidationStatus: asset.validationStatus || null,
      approvedAccordingToGates: gates.approvedAccordingToGates,
      materializable: gates.materializable,
      classification: gates.classification,
      stagingRunId: asset.stagingRunId || null,
      isSharedSourceReference: isSharedSourceReferenceRegistry(asset),
    };
  });

  const duplicateRegistryGroups = groupDuplicateRegistryAssets(openingAssets).filter(
    (g) => g.assetCount > 1
  );

  const canonicalOpeningAssetMap = buildCanonicalOpeningAssetMap(openingsRows, openingAssets);

  const whyApprovedImagesNotRecognizedBefore = {
    summary:
      "Prior v31L/v31M writers used listRegistryAssetsForBrand (slim normalize) which omits Source Notes and Attachment URL — findDedicatedRegistry could not match v31L per-row registry rows. Multiple rows also shared one approved registry (rec1Mi8rEyewVTpS0) without per-row sourceNotes linkage, so dedicated-registry gates failed even though founder-approved duplicates exist.",
    rootCauses: [
      "Slim registry normalize omitted sourceNotes and attachmentUrl used by findDedicatedRegistry",
      "Shared approved registry rec1Mi8rEyewVTpS0 linked to Bogotá/Cúcuta/Cali without per-row dedicated match",
      "v31L-created per-property registry rows exist as duplicates but were not chosen as canonical",
      "Reactivation required dedicated approved registry + materialized presentation image — both missing",
    ],
    sharedRegistryId: "rec1Mi8rEyewVTpS0",
    v31lStagingRunId: "v31L-openings-rebuild",
  };

  const openingsRowAudit = [];
  const rowsToMaterialize = [];
  const rowsToReactivate = [];
  const rowsKeptHidden = [];
  const proposedPresentationUpdates = [];
  const proposedRegistryNotesUpdates = [];

  for (const row of openingsRows) {
    const catalog = catalogForRow(row.recordId);
    const canonicalEntry = canonicalOpeningAssetMap.find(
      (m) => m.presentationRecordId === row.recordId
    );
    const canonicalAsset = canonicalEntry?.canonicalRegistryId
      ? openingAssets.find((a) => a.id === canonicalEntry.canonicalRegistryId)
      : null;
    const legacyLinked = findRegistryAssetForPresentationRow(openingAssets, row);
    const completeCheck = openingIsCompleteRow(row);
    const apiBlock = apiBlockByRecordId.get(row.recordId);
    const visibleInApi = Boolean(apiBlock);

    let rebuiltCopy = null;
    try {
      rebuiltCopy = buildOwnerFacingOpeningsCopy({ row, catalog });
    } catch {
      rebuiltCopy = null;
    }

    const reactivateCheck = canReactivateOpeningsRow({
      row,
      dedicatedRegistry: canonicalAsset,
      rebuiltCopy,
    });

    const v31eReactivation = canonicalAsset
      ? assessReactivationEligibility(row, canonicalAsset, brandConfig)
      : { eligible: false, blockReason: "no_canonical_approved_registry" };

    let blockedReason = null;
    if (row.recordId === COMPLETE_VISIBLE_BASELINE && completeCheck.complete && visibleInApi) {
      blockedReason = "already_complete_visible";
    } else if (legacyLinked && isDoNotUseRecord(legacyLinked) && !canonicalAsset) {
      blockedReason = "linked_do_not_use_registry";
    } else if (!canonicalAsset) {
      blockedReason = reactivateCheck.reason || "no_canonical_approved_registry";
    } else if (!reactivateCheck.ok) {
      blockedReason = reactivateCheck.reason;
    } else if (!v31eReactivation.eligible && row.quarantined) {
      blockedReason = v31eReactivation.blockReason;
    }

    const sourceUrl = catalog?.sourcePageUrl || row.summaryUrl || row.sourcePageUrl;
    openingsRowAudit.push({
      presentationRecordId: row.recordId,
      propertyTitle: catalog?.propertyName || row.title,
      title: row.title,
      externalDisplayStatus: row.externalDisplayStatus,
      imageFieldStatus: row.hasImage ? "materialized" : "empty",
      visibleInApi,
      completeRow: completeCheck.complete,
      completeMissingFields: completeCheck.missing,
      sourcePageUrl: sourceUrl || null,
      currentRegistryLinkage: legacyLinked?.id || null,
      canonicalRegistryLinkage: canonicalAsset?.id || null,
      blockedOrVisibleReason: blockedReason || (visibleInApi ? "visible_in_api" : "eligible_pending_apply"),
      reactivateEligible: reactivateCheck.ok && v31eReactivation.eligible,
    });

    if (row.recordId === COMPLETE_VISIBLE_BASELINE && completeCheck.complete && visibleInApi) {
      continue;
    }

    if (!canonicalAsset || !reactivateCheck.ok || !v31eReactivation.eligible) {
      rowsKeptHidden.push({
        recordId: row.recordId,
        title: row.title,
        reason: blockedReason || "gates_not_met",
        canonicalRegistryId: canonicalAsset?.id || null,
      });
      continue;
    }

    const imageUrl = registryAssetImageUrl(canonicalAsset);
    const needsMaterialization = !row.hasImage && Boolean(imageUrl);
    if (needsMaterialization) {
      rowsToMaterialize.push({
        recordId: row.recordId,
        title: row.title,
        registryRecordId: canonicalAsset.id,
        imageUrl,
      });
      proposedPresentationUpdates.push({
        recordId: row.recordId,
        action: "materialize_image",
        registryRecordId: canonicalAsset.id,
        materializeImageUrl: imageUrl,
      });
    }

    if (row.quarantined || !visibleInApi) {
      rowsToReactivate.push({
        recordId: row.recordId,
        title: row.title,
        registryRecordId: canonicalAsset.id,
        preferred: PREFERRED_REACTIVATION_RECORD_IDS.includes(row.recordId),
      });
      proposedPresentationUpdates.push({
        recordId: row.recordId,
        action: needsMaterialization ? "materialize_then_reactivate" : "reactivate",
        registryRecordId: canonicalAsset.id,
        materializeImageUrl: needsMaterialization ? imageUrl : null,
        reactivate: true,
        fields: {
          "External Display Status": REACTIVATION_DISPLAY_STATUS,
          Active: true,
          "Brand Name": target.name,
          Brand: [target.recordId],
        },
      });
    }
  }

  const eligibleReactivationPool = rowsToReactivate.slice();
  const preferredEligible = eligibleReactivationPool.filter((r) =>
    PREFERRED_REACTIVATION_RECORD_IDS.includes(r.recordId)
  );
  const alreadyCompleteVisible = openingsRowAudit.filter(
    (a) => a.presentationRecordId === COMPLETE_VISIBLE_BASELINE && a.completeRow && a.visibleInApi
  );
  const currentCompleteVisibleCount = openingsRowAudit.filter(
    (a) => a.completeRow && a.visibleInApi
  ).length;

  let selectedReactivations = [];
  if (preferredEligible.length >= 2) {
    selectedReactivations = preferredEligible.slice(0, 2);
  } else {
    const fallback = eligibleReactivationPool
      .filter((r) => !PREFERRED_REACTIVATION_RECORD_IDS.includes(r.recordId))
      .concat(preferredEligible);
    selectedReactivations = fallback.slice(0, 2);
  }

  const selectedIds = new Set(selectedReactivations.map((r) => r.recordId));
  for (const update of proposedPresentationUpdates) {
    if (update.reactivate && !selectedIds.has(update.recordId)) {
      update.reactivate = false;
      update.skippedReason = "not_in_top_two_reactivation_targets";
    }
  }
  const finalRowsToReactivate = rowsToReactivate.filter((r) => selectedIds.has(r.recordId));
  const finalRowsToMaterialize = rowsToMaterialize.filter((r) => selectedIds.has(r.recordId));

  for (const group of duplicateRegistryGroups) {
    const byPresentation = new Map();
    for (const asset of group.assets) {
      const pid =
        parsePresentationRowIdFromNotes(asset.sourceNotes) ||
        canonicalOpeningAssetMap.find((m) => m.candidateRegistryIds.includes(asset.id))
          ?.presentationRecordId;
      if (!pid) continue;
      if (!byPresentation.has(pid)) byPresentation.set(pid, []);
      byPresentation.get(pid).push(asset);
    }
    for (const [pid, assets] of byPresentation.entries()) {
      const catalog = catalogForRow(pid);
      const chosen = chooseCanonicalAsset(assets, pid, catalog);
      if (!chosen) continue;
      for (const asset of assets) {
        if (asset.id === chosen.asset.id) continue;
        if (!assessAssetApprovalGates(asset).approvedAccordingToGates) continue;
        const note = `v31M-R1: duplicate/superseded by canonical ${chosen.asset.id}`;
        if (nz(asset.validationNotes).includes("v31M-R1: duplicate/superseded")) continue;
        proposedRegistryNotesUpdates.push({
          registryRecordId: asset.id,
          canonicalRegistryId: chosen.asset.id,
          fields: {
            [MAP_VISUAL_SLOT.validationNotes]: [nz(asset.validationNotes), note]
              .filter(Boolean)
              .join("\n"),
          },
        });
      }
    }
  }

  const momentumApplyStatus = verifyMomentumApplyStatus(momentumRows);

  const applyBlockers = [];
  if (apply && !approvedAssetsOnly) {
    applyBlockers.push(`materialization_requires_${APPLY_FLAG_APPROVED_ONLY}`);
  }
  if (finalRowsToReactivate.length < 2 && currentCompleteVisibleCount < REQUIRED_COMPLETE_OPENINGS) {
    applyBlockers.push(
      `insufficient_eligible_reactivation_rows:${finalRowsToReactivate.length}_of_2_needed`
    );
  }
  for (const u of proposedPresentationUpdates.filter((x) => x.reactivate || x.materializeImageUrl)) {
    if (!selectedIds.has(u.recordId)) continue;
    const audit = openingsRowAudit.find((a) => a.presentationRecordId === u.recordId);
    if (u.reactivate && audit && !audit.reactivateEligible) {
      applyBlockers.push(`forced_reactivation_blocked:${u.recordId}`);
    }
    if (u.materializeImageUrl) {
      const asset = openingAssets.find((a) => a.id === u.registryRecordId);
      const gates = assessAssetApprovalGates(asset);
      if (!gates.approvedAccordingToGates) {
        applyBlockers.push(`unapproved_image_would_be_used:${u.recordId}`);
      }
      if (gates.isDoNotUse || isSharedSourceReferenceRegistry(asset)) {
        applyBlockers.push(`do_not_use_or_shared_image:${u.recordId}`);
      }
      if (isTemporaryAirtableUrl(u.materializeImageUrl) && !asset?.sourcePageUrl) {
        applyBlockers.push(`temporary_airtable_url_without_source_page:${u.recordId}`);
      }
    }
    if (findInternalLanguageInRow({ body: u.fields?.Body, title: u.fields?.Title }).length) {
      applyBlockers.push(`internal_language:${u.recordId}`);
    }
  }

  const projectedCompleteAfter =
    currentCompleteVisibleCount +
    finalRowsToReactivate.filter((r) => {
      const audit = openingsRowAudit.find((a) => a.presentationRecordId === r.recordId);
      return audit && (audit.completeRow || audit.completeMissingFields?.includes("imageUrl"));
    }).length;

  const hasWork =
    proposedPresentationUpdates.some((u) => u.reactivate || u.materializeImageUrl) ||
    proposedRegistryNotesUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && approvedAssetsOnly && noValidationClaim && founderReviewed;
  const dryRunClean = applyBlockers.length === 0 && hasWork;
  const canApply = applyGatesReady && dryRunClean;

  let airtableModified = false;
  let imagesApproved = false;
  let applyResults = {
    rowsMaterialized: [],
    rowsReactivated: [],
    registryNotesUpdated: [],
    errors: [],
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const processedMaterialize = new Set();
    for (const update of proposedPresentationUpdates) {
      if (!selectedIds.has(update.recordId)) continue;
      if (update.materializeImageUrl && !processedMaterialize.has(update.recordId)) {
        processedMaterialize.add(update.recordId);
        try {
          const mat = await materializePresentationImage({
            baseId,
            apiKey,
            recordId: update.recordId,
            imageUrl: update.materializeImageUrl,
            slotKey: OPENINGS_SLOT,
          });
          if (mat.materialized) applyResults.rowsMaterialized.push(update.recordId);
        } catch (err) {
          applyResults.errors.push({
            recordId: update.recordId,
            error: `materialize:${err.message}`,
          });
          continue;
        }
      }
      if (update.reactivate && update.fields) {
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
            error: json.error?.message || "reactivation patch failed",
          });
          continue;
        }
        applyResults.rowsReactivated.push(update.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      }
    }

    for (const regUpdate of proposedRegistryNotesUpdates) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_ASSET_REGISTRY_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: regUpdate.fields, typecast: true }) },
        regUpdate.registryRecordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          recordId: regUpdate.registryRecordId,
          error: json.error?.message || "registry notes patch failed",
        });
        continue;
      }
      applyResults.registryNotesUpdated.push(regUpdate.registryRecordId);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 180));
    }

    const brandBasicsAfter = await fetchBrandBasics(target.recordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  if (!companyValidatedUntouched && canApply) {
    applyBlockers.push("company_validated_changed");
  }

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const completeBuildBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    v31mR1WriterExists: v31mR1WriterExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    whyApprovedImagesNotRecognizedBefore,
    duplicateRegistryGroups,
    canonicalOpeningAssetMap,
    openingsRowAudit,
    registryReconciliationAudit,
    rowsMaterialized: finalRowsToMaterialize,
    rowsReactivated: finalRowsToReactivate,
    rowsKeptHidden,
    momentumApplyStatus,
    imagesNewlyApproved: false,
    imagesApproved,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    airtableModified,
    proposedPresentationUpdates: proposedPresentationUpdates.filter(
      (u) => selectedIds.has(u.recordId) || u.action === "materialize_image"
    ),
    proposedRegistryNotesUpdates,
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    expectedUiResult: {
      openingsBefore: `${currentCompleteVisibleCount} complete visible (need ${REQUIRED_COMPLETE_OPENINGS})`,
      openingsAfterApply: `${Math.min(REQUIRED_COMPLETE_OPENINGS, projectedCompleteAfter)} projected complete visible if materialization succeeds`,
      cards: [
        "Barranquilla (existing)",
        ...finalRowsToReactivate.map((r) => r.title),
      ],
    },
    expectedActiveProfileResult: {
      note:
        projectedCompleteAfter >= REQUIRED_COMPLETE_OPENINGS
          ? "Openings 3/3 gate may pass after apply + QA refresh."
          : "Active-profile still blocked on openings until 3 complete visible rows.",
      finalQaBefore: finalQaBefore?.brandReports?.[0]?.scores || null,
      completeBuildBefore:
        (completeBuildBefore?.brandReports || []).find((b) => b.slug === target.slug)?.readiness ||
        completeBuildBefore?.summary ||
        null,
    },
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Opening Asset Approval Reconciliation v31M-R1`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}** (\`${report.targetBrand.recordId}\`)`,
    `- v31M-R1 exists: **${report.v31mR1WriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    "",
    "## Why approved images were not recognized before",
    "",
    report.whyApprovedImagesNotRecognizedBefore.summary,
    "",
    ...report.whyApprovedImagesNotRecognizedBefore.rootCauses.map((r) => `- ${r}`),
    "",
    "## Duplicate registry groups",
    "",
    `- Groups with 2+ assets: **${report.duplicateRegistryGroups.length}**`,
    "",
  ];

  for (const g of report.duplicateRegistryGroups.slice(0, 12)) {
    lines.push(`- \`${g.groupKey}\`: ${g.assetCount} assets (${g.assetIds.join(", ")})`);
  }

  lines.push("", "## Canonical asset per opening row", "");
  for (const m of report.canonicalOpeningAssetMap) {
    lines.push(
      `- **${m.propertyTitle}** (\`${m.presentationRecordId}\`) → canonical \`${m.canonicalRegistryId || "none"}\` (${m.candidateCount} candidates)`
    );
  }

  lines.push("", "## Opening row audit", "");
  for (const a of report.openingsRowAudit) {
    lines.push(`### ${a.title}`);
    lines.push(`- Record: \`${a.presentationRecordId}\``);
    lines.push(
      `- Status: ${a.externalDisplayStatus} · Image: ${a.imageFieldStatus} · Visible: ${a.visibleInApi} · Complete: ${a.completeRow}`
    );
    lines.push(
      `- Registry: legacy \`${a.currentRegistryLinkage || "none"}\` · canonical \`${a.canonicalRegistryLinkage || "none"}\``
    );
    lines.push(`- Reason: ${a.blockedOrVisibleReason}`);
    lines.push("");
  }

  lines.push("## Rows to materialize", "");
  for (const r of report.rowsMaterialized) {
    lines.push(`- \`${r.recordId}\` **${r.title}** via \`${r.registryRecordId}\``);
  }
  if (!report.rowsMaterialized.length) lines.push("- none");

  lines.push("", "## Rows to reactivate", "");
  for (const r of report.rowsReactivated) {
    lines.push(`- \`${r.recordId}\` **${r.title}**${r.preferred ? " (preferred)" : ""}`);
  }
  if (!report.rowsReactivated.length) lines.push("- none");

  lines.push("", "## Rows kept hidden", "");
  for (const r of report.rowsKeptHidden) {
    lines.push(`- \`${r.recordId}\` — ${r.reason}`);
  }

  lines.push("", "## Momentum apply status", "");
  lines.push(`- v31M fully applied: **${report.momentumApplyStatus.v31mFullyApplied ? "yes" : "no"}**`);
  lines.push(`- ${report.momentumApplyStatus.note}`);
  for (const row of report.momentumApplyStatus.rows) {
    lines.push(
      `- \`${row.recordId}\`: ${row.v31mApplied ? "applied" : "pending"} — live title: "${row.liveTitle || ""}"`
    );
  }

  lines.push("", "## Governance", "");
  lines.push(`- Images newly approved: **no**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);

  if (report.applyBlockers.length) {
    lines.push("", "## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }

  lines.push("", "## Expected UI result", "");
  lines.push(`- ${report.expectedUiResult.openingsBefore} → ${report.expectedUiResult.openingsAfterApply}`);

  if (report.exactApplyCommand) {
    lines.push("", "## Exact apply command", "", "```bash", report.exactApplyCommand, "```");
  }

  return lines.join("\n");
}
