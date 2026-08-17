/**
 * Brand Explorer Everhome Active Profile Finalization v32G.
 *
 * Recognizes founder-approved registry assets, maps canonical assets to working
 * presentation images, reconciles duplicates (report/metadata only), and bridges
 * Final QA / Complete Build readiness — without auto-approving images or replacing
 * working Image fields.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-active-profile-finalization-writer-v32G.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import {
  assessPresentationRowImageGovernance,
  detectBrandAssetImageGovernanceDefects,
  findRegistryAssetForPresentationRow,
  getDiscoveryBrandConfig,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  classifyRegistryAsset,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
  isPendingApprovalRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import {
  isTemporaryAirtableUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  openingIsCompleteRow,
  momentumIsCompleteRow,
} from "./brand-explorer-radisson-individuals-openings-momentum-parity-writer.js";
import {
  containsSourceMetadataLanguage,
  parseFootprintOpeningParas,
} from "./brand-explorer-everhome-openings-description-cleanup-writer.js";
import {
  findEverhomeRegistryAssetForRow,
  resolveMomentumProperCaseTitle,
} from "./brand-explorer-everhome-image-governance-recognition-writer.js";
import { TARGET_BRAND } from "./brand-explorer-everhome-openings-momentum-rebuild-writer.js";
import {
  followsTributeMomentumRules,
  parseMomentumPresentationBody,
} from "./brand-explorer-momentum-link-label.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v32G";
export const REPORT_JSON_NAME = "brand-explorer-everhome-active-profile-finalization-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-active-profile-finalization-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-active-profile-finalization-writer-v32G.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32G-everhome-active-profile-finalization";
export const APPLY_FLAG_FOUNDER_ONLY = "--confirm-founder-approved-assets-only";
export const APPLY_FLAG_PRESERVE_IMAGES = "--confirm-preserve-working-images";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_COPY = "--confirm-no-opening-or-momentum-copy-changes";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "woodspring-suites",
  "suburban-studios",
]);

const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIRED_OPENINGS = 3;
const REQUIRED_MOMENTUM = 3;

const V32G_VISUAL_SLOT_RE =
  /^(footprint\.openings|footprint\.momentum|materials\.gallery\.\d|overview\.hero|overview\.scenario\.\d|valueOwners\.scenario\.\d|overview\.featured_application)$/;

const BLOCKED_REGISTRY_PATCH_FIELDS = new Set([
  MAP_BRAND_ASSET.assetStatus,
  MAP_BRAND_ASSET.explorerUsePermission,
  MAP_BRAND_ASSET.usageReviewStatus,
  MAP_BRAND_ASSET.companyValidated,
  MAP_BRAND_ASSET.companyValidationDate,
  MAP_BRAND_ASSET.attachment,
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-openings-description-cleanup-writer.json",
  "reports/brand-explorer-everhome-image-governance-recognition-writer.json",
  "reports/brand-explorer-everhome-openings-momentum-rebuild-writer.json",
  "reports/brand-explorer-everhome-presentation-cleanup-writer.json",
  "reports/brand-explorer-everhome-source-registry-normalization-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json",
  "reports/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome Presentation / Registry / Source Library / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-active-profile-finalization-writer.js",
  "scripts/brand-explorer-everhome-active-profile-finalization-writer.mjs",
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

export function v32gWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-everhome-active-profile-finalization-writer.js"
    )
  );
}

export function isV32GVisualSlot(slotKey) {
  return V32G_VISUAL_SLOT_RE.test(nz(slotKey));
}

function registryAssetForGovernance(asset) {
  return {
    id: asset.id,
    recommendedExplorerSlot: asset.recommendedExplorerSlot,
    sourceUrl: asset.sourceUrl || asset.attachmentUrl,
    sourcePageUrl: asset.sourcePageUrl,
    assetStatus: asset.assetStatus,
    explorerUsePermission: asset.explorerUsePermission,
    usageReviewStatus: asset.usageReviewStatus,
  };
}

export function classifyEverhomeRegistryRecognition(asset) {
  const slim = {
    assetStatus: asset.assetStatus,
    explorerUsePermission: asset.explorerUsePermission,
    usageReviewStatus: asset.usageReviewStatus,
    assetName: asset.assetName,
    doNotUseReason: asset.doNotUseReason,
    reviewNotes: asset.reviewNotes,
  };
  const registryClass = classifyRegistryAsset(slim);
  let bucket = "pending_image_review";
  if (isDoNotUseRecord(asset) || registryClass === "Do Not Use") {
    bucket = "do_not_use";
  } else if (isFounderApprovedRecord(asset) || isRegistryAssetApprovedForExplorer(asset)) {
    bucket = "approved_for_explorer_use";
  } else if (registryClass === "Replace Needed") {
    bucket = "replace_needed";
  } else if (asset.explorerUsePermission === "Candidate Only") {
    bucket = "candidate_only";
  } else if (isPendingApprovalRecord(asset)) {
    bucket = "pending_image_review";
  }

  const linkedPresentationRowId = parsePresentationRowIdFromNotes(asset.sourceNotes);
  const durableUrl = nz(asset.sourceUrl || asset.attachmentUrl);
  const materializable =
    bucket === "approved_for_explorer_use" &&
    Boolean(durableUrl) &&
    !isTemporaryAirtableUrl(durableUrl);

  return {
    recordId: asset.id,
    assetName: asset.assetName,
    bucket,
    registryClass,
    founderApproved: isFounderApprovedRecord(asset),
    explorerApproved: isRegistryAssetApprovedForExplorer(asset),
    assetStatus: asset.assetStatus,
    explorerUsePermission: asset.explorerUsePermission,
    usageReviewStatus: asset.usageReviewStatus,
    recommendedExplorerSlot: asset.recommendedExplorerSlot,
    linkedPresentationRowId,
    missingRelatedPresentationRow: !linkedPresentationRowId,
    missingIntendedSlot: !nz(asset.recommendedExplorerSlot),
    missingSourcePageUrl: !nz(asset.sourcePageUrl),
    sourcePageUrl: asset.sourcePageUrl || null,
    sourceUrl: durableUrl || null,
    materializable,
    supersededDuplicate: /superseded duplicate/i.test(asset.reviewNotes),
  };
}

function scoreCanonicalCandidate(asset, row) {
  if (isDoNotUseRecord(asset)) return -10000;
  let score = 0;
  const linked = parsePresentationRowIdFromNotes(asset.sourceNotes);
  if (linked === row.recordId) score += 500;
  if (isFounderApprovedRecord(asset)) score += 400;
  if (isRegistryAssetApprovedForExplorer(asset)) score += 350;
  if (nz(asset.recommendedExplorerSlot) === row.slotKey) score += 200;
  if (normalizeUrlKey(asset.sourcePageUrl) === normalizeUrlKey(row.summaryUrl)) score += 180;
  if (normalizeUrlKey(asset.sourceUrl) === normalizeUrlKey(row.imageUrl)) score += 150;
  if (asset.attachmentUrl) score += 100;
  if (isPendingApprovalRecord(asset)) score += 20;
  if (/press kit|candidate pool/i.test(asset.assetName)) score -= 300;
  return score;
}

function chooseCanonicalForRow(row, registryExtended) {
  const candidates = registryExtended
    .filter((a) => !isDoNotUseRecord(a))
    .map((a) => ({ asset: a, score: scoreCanonicalCandidate(a, row) }))
    .filter((c) => c.score > 0)
    .sort((a, b) => b.score - a.score);
  const everhome = findEverhomeRegistryAssetForRow(registryExtended, row);
  if (everhome && !isDoNotUseRecord(everhome)) {
    return {
      canonicalRecordId: everhome.id,
      canonicalAsset: everhome,
      selectionReason: "everhome_registry_match",
      alternateCandidates: candidates
        .filter((c) => c.asset.id !== everhome.id)
        .map((c) => c.asset.id),
    };
  }
  if (!candidates.length) return null;
  return {
    canonicalRecordId: candidates[0].asset.id,
    canonicalAsset: candidates[0].asset,
    selectionReason: `scored_candidate:${candidates[0].score}`,
    alternateCandidates: candidates.slice(1).map((c) => c.asset.id),
  };
}

function registryDedupeKey(asset) {
  const slot = nz(asset.recommendedExplorerSlot);
  const page = normalizeUrlKey(asset.sourcePageUrl);
  const src = normalizeUrlKey(asset.sourceUrl || asset.attachmentUrl);
  return `${slot}::${page || src || asset.id}`;
}

function chooseCanonicalAssetInGroup(group) {
  const sorted = [...group].sort((a, b) => {
    const aApproved = isFounderApprovedRecord(a) ? 1 : 0;
    const bApproved = isFounderApprovedRecord(b) ? 1 : 0;
    if (aApproved !== bApproved) return bApproved - aApproved;
    const aLink = parsePresentationRowIdFromNotes(a.sourceNotes) ? 1 : 0;
    const bLink = parsePresentationRowIdFromNotes(b.sourceNotes) ? 1 : 0;
    if (aLink !== bLink) return bLink - aLink;
    return String(a.id).localeCompare(String(b.id));
  });
  return sorted[0];
}

function proposeRegistryMetadataPatch(asset, row) {
  if (isDoNotUseRecord(asset)) return null;
  const fields = {};
  const linkedId = parsePresentationRowIdFromNotes(asset.sourceNotes);
  if (!linkedId && row?.recordId) {
    fields[MAP_BRAND_ASSET.sourceNotes] =
      `Linked presentation row ${row.recordId} (${row.slotKey}). v32G active-profile recognition — working image preserved.`;
  }
  if (!nz(asset.recommendedExplorerSlot) && row?.slotKey) {
    fields[MAP_BRAND_ASSET.recommendedExplorerSlot] = row.slotKey;
  }
  if (!nz(asset.sourcePageUrl) && nz(row?.summaryUrl)) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = row.summaryUrl;
  }
  if (!Object.keys(fields).length) return null;
  for (const key of Object.keys(fields)) {
    if (BLOCKED_REGISTRY_PATCH_FIELDS.has(key)) {
      throw new Error(`Blocked registry field: ${key}`);
    }
  }
  return { recordId: asset.id, fields, wouldChangeApprovals: false };
}

function proposeSupersededNote(asset, canonicalId) {
  if (asset.id === canonicalId || isDoNotUseRecord(asset)) return null;
  if (isFounderApprovedRecord(asset)) return null;
  const existing = nz(asset.reviewNotes);
  if (/superseded duplicate/i.test(existing)) return null;
  return {
    recordId: asset.id,
    fields: {
      [MAP_BRAND_ASSET.reviewNotes]: `Superseded duplicate — canonical asset ${canonicalId} (v32G recognition only; not deleted).`,
    },
    wouldChangeApprovals: false,
  };
}

function imageLoadingInApi(row, apiBlock) {
  if (!row.hasImage) return false;
  return Boolean(nz(apiBlock?.imageUrl));
}

function proposeImageMaterialization(row, asset) {
  if (!isFounderApprovedRecord(asset) && !isRegistryAssetApprovedForExplorer(asset)) return null;
  if (row.hasImage && imageLoadingInApi(row, { imageUrl: row.imageUrl })) return null;
  const url = nz(asset.sourceUrl || asset.attachmentUrl);
  if (!url || isTemporaryAirtableUrl(url)) return null;
  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    registryRecordId: asset.id,
    imageUrl: url,
    fields: { Image: [{ url }] },
  };
}

async function listRegistryRaw(baseId, apiKey, brandRecordId) {
  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${escapeFormulaValue(brandRecordId)}'`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
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

function firstAttachmentUrl(fields) {
  for (const key of ["Image", "Images", "Scenario Image", "Attachments"]) {
    const att = fields?.[key];
    if (Array.isArray(att) && att[0]?.url) return nz(att[0].url);
  }
  return "";
}

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params.toString()}`;
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
      sortOrder: f["Sort Order"],
      active: f.Active,
      externalDisplayStatus: nz(f["External Display Status"]),
      imageUrl: firstAttachmentUrl(f),
      hasImage: Boolean(firstAttachmentUrl(f)),
      section: nz(f.Section),
    };
  });
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-active-profile-finalization-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER_ONLY,
    APPLY_FLAG_PRESERVE_IMAGES,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_COPY,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Active Profile Finalization v32G");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32G exists: **${report.v32gWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Active-profile gate: **${report.activeProfileGatePassing ? "pass" : "blocked"}**`);
  lines.push(`- Founder approvals required: **${report.rowsRequiringFounderApproval.length}**`);
  lines.push(`- Images preserved: **${report.imagesPreserved.length}**`);
  lines.push(`- Images materialized: **${report.imagesMaterialized.length}**`);
  lines.push("");
  lines.push("## Readiness");
  lines.push(`- Final QA: ${report.finalQaExpectedResult}`);
  lines.push(`- Complete Build: ${report.completeBuildExpectedResult}`);
  lines.push(`- Visual defects: ${report.visualDefectExpectedResult}`);
  lines.push(`- Openings: ${report.openingsReadiness.summary}`);
  lines.push(`- Momentum: ${report.momentumReadiness.summary}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply Command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply Blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerEverhomeActiveProfileFinalizationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApprovedOnly = false,
  preserveWorkingImages = false,
  noValidationClaim = false,
  noCopyChanges = false,
  everhomeOnly = false,
} = {}) {
  if (PROTECTED_BRAND_SLUGS.includes(nz(brandArg).toLowerCase())) {
    throw new Error(`Protected brand cannot be modified: ${brandArg}`);
  }
  if (nz(brandArg).toLowerCase() !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v32G is Everhome-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug) || {
    slug: TARGET_BRAND.slug,
    name: TARGET_BRAND.name,
    allowedSiblingMentions: ["everhome", "choice hotels"],
    extraWrongBrandMarkers: [],
  };

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
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
  const registryForGovernance = registryExtended.map(registryAssetForGovernance);

  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const apiById = new Map(apiBlocks.map((b) => [b.recordId, b]));
  const governanceDefects = detectBrandAssetImageGovernanceDefects(
    brandApi,
    registryForGovernance,
    brandConfig,
    { slug: TARGET_BRAND.slug, recordId: TARGET_BRAND.recordId }
  );
  const defectByRecordId = new Map();
  for (const d of governanceDefects) {
    if (d.recordId) defectByRecordId.set(d.recordId, d);
  }

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

  const founderApprovedAssetRecognition = registryExtended.map(classifyEverhomeRegistryRecognition);
  const approvedCount = founderApprovedAssetRecognition.filter(
    (a) => a.bucket === "approved_for_explorer_use"
  ).length;

  const activeProfileVisualAudit = [];
  const canonicalAssetMapping = [];
  const duplicateSupersededFindings = [];
  const registryPatches = [];
  const imagesMaterialized = [];
  const imagesPreserved = [];
  const rowsRequiringFounderApproval = [];
  const applyBlockers = [];

  const visualRows = presentationRows.filter((r) => isV32GVisualSlot(r.slotKey));

  for (const row of visualRows) {
    const apiBlock = apiById.get(row.recordId);
    const loading = imageLoadingInApi(row, apiBlock);
    const canonical = chooseCanonicalForRow(row, registryExtended);
    const canonicalAsset = canonical?.canonicalAsset || null;
    const governanceAsset = canonicalAsset
      ? registryForGovernance.find((a) => a.id === canonicalAsset.id)
      : findRegistryAssetForPresentationRow(registryForGovernance, row);
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryForGovernance);
    const defect = defectByRecordId.get(row.recordId);
    const approved =
      canonicalAsset &&
      (isFounderApprovedRecord(canonicalAsset) || isRegistryAssetApprovedForExplorer(canonicalAsset));

    const gateRecognizes = Boolean(approved && governanceAsset && isRegistryAssetApprovedForExplorer(governanceAsset));

    activeProfileVisualAudit.push({
      presentationRowId: row.recordId,
      slot: row.slotKey,
      title: row.title,
      section: row.section,
      imageFieldStatus: row.hasImage ? "has_attachment" : "missing",
      imageLoadingInApi: loading,
      imageTemporaryOrExpired: isTemporaryAirtableUrl(row.imageUrl),
      linkedRegistryAssetId: canonical?.canonicalRecordId || governanceAsset?.id || null,
      registryApprovalStatus: approved
        ? "approved_for_explorer_use"
        : canonicalAsset
          ? classifyEverhomeRegistryRecognition(canonicalAsset).bucket
          : "no_match",
      sourcePageUrl: canonicalAsset?.sourcePageUrl || row.summaryUrl || null,
      sourceSupported: Boolean(nz(row.summaryUrl) || canonicalAsset?.sourcePageUrl),
      activeProfileImageGateResult: gateRecognizes ? "recognized" : "blocked",
      defectReason: defect?.message || assessment?.recommendation || null,
    });

    if (canonical) {
      canonicalAssetMapping.push({
        presentationRowId: row.recordId,
        slot: row.slotKey,
        canonicalRegistryRecordId: canonical.canonicalRecordId,
        selectionReason: canonical.selectionReason,
        alternateCandidateIds: canonical.alternateCandidates,
        founderApproved: canonicalAsset ? isFounderApprovedRecord(canonicalAsset) : false,
      });
    }

    if (loading) {
      imagesPreserved.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: "working_image_preserved",
      });
    }

    if (row.hasImage && loading && !approved) {
      rowsRequiringFounderApproval.push({
        presentationRowId: row.recordId,
        slot: row.slotKey,
        title: row.title,
        canonicalRegistryRecordId: canonical?.canonicalRecordId || null,
        reason: "pending_registry_approval_for_active_profile_gate",
        currentBucket: canonicalAsset
          ? classifyEverhomeRegistryRecognition(canonicalAsset).bucket
          : "no_canonical_match",
      });
    }

    if (canonicalAsset && !isDoNotUseRecord(canonicalAsset)) {
      const metaPatch = proposeRegistryMetadataPatch(canonicalAsset, row);
      if (metaPatch) registryPatches.push(metaPatch);
      const materialization = proposeImageMaterialization(row, canonicalAsset);
      if (materialization) {
        if (loading) {
          applyBlockers.push(`would_replace_working_image:${row.recordId}`);
        } else {
          imagesMaterialized.push(materialization);
        }
      }
    }
  }

  const duplicateGroups = new Map();
  for (const asset of registryExtended) {
    if (isDoNotUseRecord(asset)) continue;
    const key = registryDedupeKey(asset);
    if (!duplicateGroups.has(key)) duplicateGroups.set(key, []);
    duplicateGroups.get(key).push(asset);
  }
  for (const [key, group] of duplicateGroups.entries()) {
    if (group.length < 2) continue;
    const canonical = chooseCanonicalAssetInGroup(group);
    duplicateSupersededFindings.push({
      dedupeKey: key,
      canonicalRecordId: canonical.id,
      duplicateRecordIds: group.filter((g) => g.id !== canonical.id).map((g) => g.id),
      canonicalFounderApproved: isFounderApprovedRecord(canonical),
    });
    for (const dup of group) {
      const note = proposeSupersededNote(dup, canonical.id);
      if (note) registryPatches.push(note);
    }
  }

  const openingsRows = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const visibleOpenings = openingsRows.filter((r) => apiById.has(r.recordId));
  const openingsComplete = visibleOpenings.map((row) => {
    const parsed = parseFootprintOpeningParas(row.body);
    const complete = openingIsCompleteRow(row);
    return {
      recordId: row.recordId,
      title: row.title,
      complete: complete.complete,
      missing: complete.missing,
      imageLoading: imageLoadingInApi(row, apiById.get(row.recordId)),
      ownerFacingTeaser: !containsSourceMetadataLanguage(parsed.situation),
      chips: parsed.chips,
      sourceUrl: parsed.summaryHref || row.summaryUrl,
    };
  });
  const openingsReadiness = {
    visibleCount: visibleOpenings.length,
    completeCount: openingsComplete.filter((o) => o.complete).length,
    ownerFacingCount: openingsComplete.filter((o) => o.ownerFacingTeaser).length,
    imagesLoadingCount: openingsComplete.filter((o) => o.imageLoading).length,
    meetsMinimumVisible: visibleOpenings.length >= REQUIRED_OPENINGS,
    summary: `${visibleOpenings.length} visible openings; ${openingsComplete.filter((o) => o.ownerFacingTeaser).length}/${visibleOpenings.length} owner-facing teasers; ${openingsComplete.filter((o) => o.complete).length} structurally complete`,
    rows: openingsComplete,
  };

  const momentumRows = presentationRows.filter((r) => r.slotKey === MOMENTUM_SLOT);
  const visibleMomentum = momentumRows.filter((r) => apiById.has(r.recordId));
  const momentumReady = visibleMomentum.map((row) => {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    const complete = momentumIsCompleteRow(row);
    const properCase = nz(row.title) === resolveMomentumProperCaseTitle(row.title);
    const sourceOk = followsTributeMomentumRules(parsed.sourceUrl || row.summaryUrl).ok;
    return {
      recordId: row.recordId,
      title: row.title,
      properCaseHeading: properCase,
      complete: complete.complete,
      sourceEventSupporting: sourceOk,
    };
  });
  const momentumReadiness = {
    visibleCount: visibleMomentum.length,
    properCaseCount: momentumReady.filter((m) => m.properCaseHeading).length,
    eventSourceCount: momentumReady.filter((m) => m.sourceEventSupporting).length,
    meetsMinimumVisible: visibleMomentum.length >= REQUIRED_MOMENTUM,
    summary: `${visibleMomentum.length} visible momentum rows; ${momentumReady.filter((m) => m.properCaseHeading).length} Proper Case headings; ${momentumReady.filter((m) => m.sourceEventSupporting).length} event-supporting sources`,
    rows: momentumReady,
  };

  if (approvedCount === 0) {
    applyBlockers.push("no_founder_approved_registry_assets");
  }
  if (rowsRequiringFounderApproval.length > 0) {
    applyBlockers.push(`founder_approval_required:${rowsRequiringFounderApproval.length}_visual_rows`);
  }
  const finalQaStatus = finalQaReport?.scores?.overallActiveProfileReadiness
    ? `${finalQaReport.scores.overallActiveProfileReadiness} (${finalQaReport.scores.overallNumeric ?? "n/a"})`
    : finalQaReport?.brandReports?.[0]?.scores?.overallActiveProfileReadiness
      ? `${finalQaReport.brandReports[0].scores.overallActiveProfileReadiness}`
      : "unavailable";
  const finalQaDefectTotal = finalQaReport?.defectCounts
    ? Object.values(finalQaReport.defectCounts).reduce((a, b) => a + b, 0)
    : finalQaReport?.brandReports?.[0]?.defectCounts
      ? Object.values(finalQaReport.brandReports[0].defectCounts).reduce((a, b) => a + b, 0)
      : null;

  const completeBuildBrandResult =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;

  const completeBuildStatus = completeBuildReport?.readyForActiveProfile
    ? "ready"
    : completeBuildBrandResult?.readinessBand ||
      completeBuildReport?.readinessBand ||
      "blocked";

  const completeBuildFinalQaReadiness =
    completeBuildBrandResult?.finalQaScores?.overallActiveProfileReadiness ||
    completeBuildReport?.finalQaScores?.overallActiveProfileReadiness ||
    "unknown";

  const finalQaReadiness =
    finalQaReport?.scores?.overallActiveProfileReadiness ||
    finalQaReport?.brandReports?.[0]?.scores?.overallActiveProfileReadiness ||
    "unknown";

  const scoringMismatch =
    finalQaReadiness !== "unknown" &&
    completeBuildFinalQaReadiness !== "unknown" &&
    finalQaReadiness !== completeBuildFinalQaReadiness
      ? {
          finalQaDirect: finalQaReadiness,
          completeBuildEmbeddedFinalQa: completeBuildFinalQaReadiness,
          completeBuildReadinessBand: completeBuildStatus,
        }
      : completeBuildStatus !== completeBuildFinalQaReadiness &&
          completeBuildFinalQaReadiness !== "unknown"
        ? {
            finalQaDirect: finalQaReadiness,
            completeBuildEmbeddedFinalQa: completeBuildFinalQaReadiness,
            completeBuildReadinessBand: completeBuildStatus,
            note: "readiness_band_differs_from_embedded_final_qa",
          }
        : null;

  if (finalQaReadiness !== "ready") {
    applyBlockers.push(`final_qa_not_ready:${finalQaReadiness}`);
  }
  if (!completeBuildReport?.readyForActiveProfile) {
    applyBlockers.push(`complete_build_not_ready:${completeBuildStatus}`);
  }

  const activeProfileGatePassing =
    rowsRequiringFounderApproval.length === 0 &&
    approvedCount > 0 &&
    finalQaReadiness === "ready" &&
    completeBuildReport?.readyForActiveProfile === true;

  const mergedRegistryPatches = new Map();
  for (const patch of registryPatches) {
    const existing = mergedRegistryPatches.get(patch.recordId);
    if (!existing) {
      mergedRegistryPatches.set(patch.recordId, { ...patch, fields: { ...patch.fields } });
      continue;
    }
    mergedRegistryPatches.set(patch.recordId, {
      ...existing,
      fields: { ...existing.fields, ...patch.fields },
    });
  }
  const registryPatchesMerged = [...mergedRegistryPatches.values()];

  const applyGatesReady =
    apply &&
    approveBatch &&
    founderApprovedOnly &&
    preserveWorkingImages &&
    noValidationClaim &&
    noCopyChanges &&
    everhomeOnly;

  if (apply && !activeProfileGatePassing) {
    applyBlockers.push("active_profile_readiness_would_be_forced");
  }

  const readinessBlockers = applyBlockers.filter(
    (b) =>
      b.startsWith("final_qa_") ||
      b.startsWith("complete_build_") ||
      b === "no_founder_approved_registry_assets" ||
      b.startsWith("founder_approval_required") ||
      b === "active_profile_readiness_would_be_forced"
  );
  const safetyBlockers = applyBlockers.filter((b) => !readinessBlockers.includes(b));
  const hasSafeApplyWork =
    registryPatchesMerged.length > 0 || imagesMaterialized.length > 0;
  const dryRunClean =
    safetyBlockers.length === 0 &&
    hasSafeApplyWork &&
    approvedCount > 0 &&
    readinessBlockers.length === 0;

  const canApply = applyGatesReady && dryRunClean;
  let airtableModified = false;
  const applyResults = { registryUpdated: [], presentationUpdated: [], errors: [] };

  if (canApply) {
    for (const patch of registryPatchesMerged) {
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
    for (const patch of imagesMaterialized) {
      try {
        const row = visualRows.find((r) => r.recordId === patch.recordId);
        if (row?.hasImage && imageLoadingInApi(row, apiById.get(row.recordId))) {
          applyBlockers.push(`apply_blocked_working_image:${patch.recordId}`);
          continue;
        }
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Presentation PATCH failed: ${res.status}`);
        applyResults.presentationUpdated.push(patch.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const visualDefectCounts = visualDefectReport?.defectCounts || null;
  const visualDefectExpectedResult = visualDefectCounts
    ? `${visualDefectCounts.total} defects (critical ${visualDefectCounts.critical}, high ${visualDefectCounts.high}) — next batch ${visualDefectReport?.recommendedNextBatch || "n/a"}`
    : "visual defect audit unavailable";

  const report = {
    writerVersion: WRITER_VERSION,
    v32gWriterExists: v32gWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    activeProfileVisualAudit,
    founderApprovedAssetRecognition,
    canonicalAssetMapping,
    duplicateSupersededFindings,
    registryPatches: registryPatchesMerged.map((p) => ({
      recordId: p.recordId,
      fields: Object.keys(p.fields),
    })),
    imagesMaterialized,
    imagesPreserved,
    rowsRequiringFounderApproval,
    openingsReadiness,
    momentumReadiness,
    finalQaExpectedResult: finalQaDefectTotal != null
      ? `${finalQaStatus} — ${finalQaDefectTotal} defects`
      : finalQaStatus,
    completeBuildExpectedResult: `${completeBuildStatus} (readyForActiveProfile: ${completeBuildReport?.readyForActiveProfile ? "yes" : "no"})`,
    visualDefectExpectedResult,
    scoringPathMismatch: scoringMismatch,
    activeProfileGatePassing,
    imageApprovalsChanged: false,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    applyBlockers,
    readinessBlockers,
    dryRunClean,
    applyResults,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-everhome-active-profile-finalization-writer -- --brand everhome-suites --dry-run",
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
