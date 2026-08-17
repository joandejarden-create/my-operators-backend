/**
 * Brand Explorer Everhome Image Governance Recognition + Momentum Heading Proper Case v32F.
 *
 * Everhome-only:
 * - Audits working presentation images and registry recognition gaps.
 * - Links registry metadata to existing working images (no reload/replace).
 * - Materializes Image fields only when founder-approved + row missing image.
 * - Proper Case cleanup for footprint.momentum headings (Title only).
 *
 * @see docs/data-intelligence/brand-explorer-everhome-image-governance-recognition-writer-v32F.md
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
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  isDoNotUseRecord,
  isFounderApprovedRecord,
  normalizeRegistryRecordExtended,
  parsePresentationRowIdFromNotes,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import { parseMomentumPresentationBody } from "./brand-explorer-momentum-link-label.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { TARGET_BRAND as V32E_TARGET } from "./brand-explorer-everhome-openings-momentum-rebuild-writer.js";

export const WRITER_VERSION = "v32F-R1";
export const REPORT_JSON_NAME = "brand-explorer-everhome-image-governance-recognition-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-image-governance-recognition-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-image-governance-recognition-writer-v32F.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32F-everhome-image-governance-recognition";
export const APPLY_FLAG_FOUNDER_ONLY = "--confirm-founder-approved-assets-only";
export const APPLY_FLAG_PRESERVE_IMAGES = "--confirm-preserve-working-images";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_OPENING_LABELS = "--confirm-no-opening-label-changes";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const TARGET_BRAND = V32E_TARGET;

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "woodspring-suites",
  "suburban-studios",
  "tribute-portfolio",
  "radisson-individuals-by-choice",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MOMENTUM_SLOT = "footprint.momentum";

/** Founder-reviewed Proper Case targets for Everhome momentum headings (v32F / v32F-R1). */
export const MOMENTUM_TITLE_PROPER_CASE_BY_KEY = Object.freeze({
  "30th everhome opens in georgetown, texas": "30th Everhome Opens in Georgetown, Texas",
  "choice introduces everhome suites midscale extended stay":
    "Choice Introduces Everhome Suites Midscale Extended-Stay Platform",
  "choice introduces everhome suites midscale extended-stay platform":
    "Choice Introduces Everhome Suites Midscale Extended-Stay Platform",
  "redesigned everhome prototype debuts": "Redesigned Everhome Prototype Debuts",
  "everhome crosses 25-property milestone": "Everhome Crosses 25-Property Milestone",
});
function validateRegistryPatchFields(fields) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_REGISTRY_PATCH_FIELDS.has(key)) {
      errors.push(`Blocked registry field: ${key}`);
    }
  }
  return { valid: errors.length === 0, errors };
}

const BLOCKED_REGISTRY_PATCH_FIELDS = new Set([
  MAP_BRAND_ASSET.assetStatus,
  MAP_BRAND_ASSET.explorerUsePermission,
  MAP_BRAND_ASSET.usageReviewStatus,
  MAP_BRAND_ASSET.companyValidated,
  MAP_BRAND_ASSET.companyValidationDate,
  MAP_BRAND_ASSET.attachment,
]);

const BLOCKED_PRESENTATION_PATCH_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "External Display Status",
  "Company Validated",
  "Company Validation Date",
  "Body",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
]);

const FILES_READ = [
  "AGENTS.md",
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
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome Brand Explorer Presentation / Registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-image-governance-recognition-writer.js",
  "scripts/brand-explorer-everhome-image-governance-recognition-writer.mjs",
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

export function v32fWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-everhome-image-governance-recognition-writer.js"
    )
  );
}

/** Resolve momentum heading with explicit map overrides, then generic Proper Case. */
export function resolveMomentumProperCaseTitle(title) {
  const current = nz(title);
  if (!current) return current;
  const mapped = MOMENTUM_TITLE_PROPER_CASE_BY_KEY[current.toLowerCase()];
  if (mapped) return mapped;
  return toProperCaseHeading(current);
}

function capitalizeHeadingToken(word) {
  if (/^\d+[a-z]{1,4}$/i.test(word)) {
    const m = word.match(/^(\d+)([a-z]+)$/i);
    if (m) return `${m[1]}${m[2].toLowerCase()}`;
  }
  const lower = word.toLowerCase();
  if (lower === "everhome") return "Everhome";
  if (lower === "choice") return "Choice";
  return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
}

/** Title-case each word for short momentum headings (Proper Case). */
export function toProperCaseHeading(title) {
  return nz(title)
    .split(/(\s+)/)
    .map((part) => {
      if (!part.trim()) return part;
      return part.replace(/[A-Za-z0-9][A-Za-z0-9'’-]*/g, (word) => {
        if (word.includes("-")) {
          return word.split("-").map((seg) => capitalizeHeadingToken(seg)).join("-");
        }
        return capitalizeHeadingToken(word);
      });
    })
    .join("");
}

export function needsProperCaseHeadingFix(title) {
  const current = nz(title);
  if (!current) return false;
  return current !== resolveMomentumProperCaseTitle(current);
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

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    fields: f,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    sortOrder: f["Sort Order"],
    externalDisplayStatus: nz(f["External Display Status"]),
    hasImage: Boolean(firstAttachmentUrl(f)),
    imageUrl: firstAttachmentUrl(f),
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
    section: nz(f.Section),
  };
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
  return rows.map(normalizePresentationRow);
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

function titleTokensForMatch(title) {
  return nz(title)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 3);
}

export function findEverhomeRegistryAssetForRow(registryExtended, row) {
  const byNotes = registryExtended.find((asset) => {
    const linked = parsePresentationRowIdFromNotes(asset.sourceNotes);
    return linked === row.recordId;
  });
  if (byNotes && !isDoNotUseRecord(byNotes)) return byNotes;

  const slotMatches = registryExtended.filter(
    (asset) =>
      !isDoNotUseRecord(asset) &&
      nz(asset.recommendedExplorerSlot) === nz(row.slotKey)
  );
  if (!slotMatches.length) return null;

  const titleTokens = titleTokensForMatch(row.title);
  const titleMatch = slotMatches.find((asset) => {
    const hay = nz(asset.assetName).toLowerCase();
    return titleTokens.some((tok) => hay.includes(tok));
  });
  if (titleMatch) return titleMatch;

  const sourceMatch = slotMatches.find(
    (asset) =>
      normalizeUrlKey(asset.sourcePageUrl) === normalizeUrlKey(row.summaryUrl) ||
      normalizeUrlKey(asset.sourceUrl) === normalizeUrlKey(row.imageUrl)
  );
  if (sourceMatch) return sourceMatch;

  const pendingOnly = slotMatches.filter((asset) => !isFounderApprovedRecord(asset));
  if (pendingOnly.length === 1) return pendingOnly[0];

  return slotMatches[0] || null;
}

function apiBlockForRow(apiBlocks, row) {
  return (apiBlocks || []).find((b) => b.recordId === row.recordId) || null;
}

function imageLoadingInApi(row, apiBlock) {
  const apiUrl = nz(apiBlock?.imageUrl);
  if (!row.hasImage) return false;
  if (!apiUrl) return false;
  return !isTemporaryAirtableUrl(apiUrl) || Boolean(apiUrl);
}

function openingChipsFromBody(body) {
  const first = nz(body).split(/\n\n+/)[0] || "";
  return first.trim();
}

function registryDedupeKey(asset) {
  const slot = nz(asset.recommendedExplorerSlot);
  const page = normalizeUrlKey(asset.sourcePageUrl);
  const src = normalizeUrlKey(asset.sourceUrl);
  return `${slot}::${page || src || asset.id}`;
}

function chooseCanonicalAsset(group) {
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

function proposeRegistryRecognitionPatch(asset, row) {
  if (isDoNotUseRecord(asset)) return null;

  const fields = {};
  const before = {};
  const after = {};

  const linkedId = parsePresentationRowIdFromNotes(asset.sourceNotes);
  if (!linkedId && row?.recordId) {
    const note = `Linked presentation row ${row.recordId} (${row.slotKey}). v32F registry recognition — working image preserved.`;
    fields[MAP_BRAND_ASSET.sourceNotes] = note;
    before.sourceNotes = asset.sourceNotes;
    after.sourceNotes = note;
  }

  if (!nz(asset.recommendedExplorerSlot) && row?.slotKey) {
    fields[MAP_BRAND_ASSET.recommendedExplorerSlot] = row.slotKey;
    before.recommendedExplorerSlot = asset.recommendedExplorerSlot;
    after.recommendedExplorerSlot = row.slotKey;
  }

  if (!nz(asset.sourcePageUrl) && nz(row?.summaryUrl)) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = row.summaryUrl;
    before.sourcePageUrl = asset.sourcePageUrl;
    after.sourcePageUrl = row.summaryUrl;
  }

  if (
    !nz(asset.sourceUrl) &&
    nz(row?.imageUrl) &&
    !isTemporaryAirtableUrl(row.imageUrl)
  ) {
    fields[MAP_BRAND_ASSET.sourceUrl] = row.imageUrl;
    before.sourceUrl = asset.sourceUrl;
    after.sourceUrl = row.imageUrl;
  }

  if (!Object.keys(fields).length) return null;

  for (const key of Object.keys(fields)) {
    if (BLOCKED_REGISTRY_PATCH_FIELDS.has(key)) {
      throw new Error(`Blocked registry field in v32F patch: ${key}`);
    }
  }

  return {
    recordId: asset.id,
    assetName: asset.assetName,
    fields,
    before,
    after,
    wouldAutoApprove: false,
  };
}

function proposeSupersededDuplicateNote(asset, canonicalId) {
  if (asset.id === canonicalId) return null;
  if (isFounderApprovedRecord(asset)) return null;
  const existing = nz(asset.reviewNotes);
  if (/superseded duplicate/i.test(existing)) return null;
  const note = `Superseded duplicate — canonical asset ${canonicalId} (v32F recognition only; not deleted).`;
  return {
    recordId: asset.id,
    fields: { [MAP_BRAND_ASSET.reviewNotes]: note },
    before: { reviewNotes: existing },
    after: { reviewNotes: note },
  };
}

function proposeMomentumTitlePatch(row) {
  if (row.slotKey !== MOMENTUM_SLOT) return null;
  const proposedTitle = resolveMomentumProperCaseTitle(row.title);
  if (!needsProperCaseHeadingFix(row.title)) return null;

  const parsed = parseMomentumPresentationBody(row.body, row.title);
  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    action: "title_proper_case",
    currentTitle: row.title,
    proposedTitle,
    bodyUnchanged: true,
    sourceUrlUnchanged: parsed.sourceUrl || null,
    fields: { Title: proposedTitle },
  };
}

function proposeImageMaterialization(row, asset) {
  if (!isFounderApprovedRecord(asset)) return null;
  if (row.hasImage && imageLoadingInApi(row, { imageUrl: row.imageUrl })) {
    return null;
  }
  const durableUrl = nz(asset.sourceUrl);
  if (!durableUrl || isTemporaryAirtableUrl(durableUrl)) return null;
  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    registryRecordId: asset.id,
    reason: "founder_approved_missing_image",
    imageUrl: durableUrl,
    fields: { Image: [{ url: durableUrl }] },
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-image-governance-recognition-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER_ONLY,
    APPLY_FLAG_PRESERVE_IMAGES,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_OPENING_LABELS,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Image Governance Recognition v32F");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32F exists: **${report.v32fWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Working images preserved: **${report.imagesPreserved.length}**`);
  lines.push(`- Images materialized: **${report.imagesMaterialized.length}**`);
  lines.push(`- Registry patches proposed: **${report.registryPatches.length}**`);
  lines.push(`- Momentum title fixes: **${report.momentumHeadingBeforeAfter.length}**`);
  lines.push("");
  lines.push("## Working Image Audit");
  for (const row of report.workingImageAudit) {
    lines.push(
      `- \`${row.recordId}\` **${row.slot}** — image: ${row.imageFieldStatus}; API: ${row.imageLoadingInApi ? "loading" : "missing"}; registry: ${row.registryRecordId || "none"} (${row.registryApprovalStatus}); gate: ${row.activeProfileGateRecognizes ? "yes" : "no"}${row.defectReason ? `; defect: ${row.defectReason}` : ""}`
    );
  }
  lines.push("");
  lines.push("## Momentum Heading Proper Case");
  for (const m of report.momentumHeadingBeforeAfter) {
    lines.push(`- \`${m.recordId}\`: "${m.before}" → "${m.after}"`);
  }
  lines.push("");
  lines.push("## Expected QA");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Next writer: ${report.recommendedNextWriter}`);
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

export async function buildBrandExplorerEverhomeImageGovernanceRecognitionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderApprovedOnly = false,
  preserveWorkingImages = false,
  noValidationClaim = false,
  noOpeningLabelChanges = false,
  everhomeOnly = false,
} = {}) {
  if (nz(brandArg).toLowerCase() !== TARGET_BRAND.slug) {
    throw new Error(`v32F is Everhome-only; got brand=${brandArg}`);
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
  const apiBlocks = brandApi?.brandExplorer?.blocks || [];

  const presentationRows = await listPresentationRows(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const registryRaw = await listRegistryRaw(baseId, apiKey, TARGET_BRAND.recordId);
  const registryExtended = registryRaw.map(normalizeRegistryRecordExtended);
  const registryForGovernance = registryExtended.map(registryAssetForGovernance);

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

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: TARGET_BRAND.slug,
  }).catch(() => null);
  const completeBuildBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandArg: TARGET_BRAND.slug,
    targetQuality: "active-profile",
  }).catch(() => null);

  const workingImageAudit = [];
  const imagesPreserved = [];
  const registryPatches = [];
  const registryRecognitionFindings = [];
  const duplicateCanonicalFindings = [];
  const imagesMaterialized = [];
  const momentumHeadingBeforeAfter = [];
  const presentationPatches = [];
  const applyBlockers = [];

  const visualRows = presentationRows.filter((r) => isVisualImageSlot(r.slotKey));

  for (const row of visualRows) {
    const apiBlock = apiBlockForRow(apiBlocks, row);
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryForGovernance);
    const registryMatch =
      findEverhomeRegistryAssetForRow(registryExtended, row) ||
      findRegistryAssetForPresentationRow(registryForGovernance, row);
    const defect = defectByRecordId.get(row.recordId);
    const loading = imageLoadingInApi(row, apiBlock);
    const governanceAsset = registryMatch?.id
      ? registryForGovernance.find((a) => a.id === registryMatch.id) || registryMatch
      : null;

    const auditEntry = {
      recordId: row.recordId,
      slot: row.slotKey,
      title: row.title,
      section: row.section,
      imageFieldStatus: row.hasImage ? "has_attachment" : "missing",
      imageLoadingInApi: loading,
      imageTemporaryOrExpired: isTemporaryAirtableUrl(row.imageUrl),
      linkedToRegistry: Boolean(registryMatch),
      registryRecordId: registryMatch?.id || null,
      registryApprovalStatus: governanceAsset
        ? isRegistryAssetApprovedForExplorer(governanceAsset)
          ? "founder_approved"
          : "pending_review"
        : "no_match",
      activeProfileGateRecognizes:
        Boolean(governanceAsset) && isRegistryAssetApprovedForExplorer(governanceAsset),
      defectReason: defect?.message || assessment?.recommendation || null,
      recommendation: assessment?.recommendation || null,
    };
    workingImageAudit.push(auditEntry);

    if (loading) {
      imagesPreserved.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        imageUrl: row.imageUrl,
        reason: "working_image_preserved",
      });
    }

    const matchedExtended =
      registryExtended.find((a) => a.id === registryMatch?.id) || registryMatch;

    if (matchedExtended) {
      registryRecognitionFindings.push({
        presentationRowId: row.recordId,
        registryRecordId: matchedExtended.id,
        founderApproved: isFounderApprovedRecord(matchedExtended),
        registryDoNotUse: isDoNotUseRecord(matchedExtended),
        missingPresentationLink: !parsePresentationRowIdFromNotes(matchedExtended.sourceNotes),
        missingIntendedSlot: !nz(matchedExtended.recommendedExplorerSlot),
        missingSourcePageUrl: !nz(matchedExtended.sourcePageUrl),
        pendingReview: !isFounderApprovedRecord(matchedExtended),
      });

    const patch = proposeRegistryRecognitionPatch(matchedExtended, row);
      if (patch && !isDoNotUseRecord(matchedExtended)) registryPatches.push(patch);
    } else if (row.hasImage && loading) {
      registryRecognitionFindings.push({
        presentationRowId: row.recordId,
        registryRecordId: null,
        founderApproved: false,
        missingPresentationLink: true,
        missingIntendedSlot: true,
        missingSourcePageUrl: Boolean(row.summaryUrl),
        pendingReview: true,
        note: "Working image present but no registry match — founder review required before active-profile gate.",
      });
    }

    if (matchedExtended) {
      const materialization = proposeImageMaterialization(row, matchedExtended);
      if (materialization) {
        if (loading) {
          applyBlockers.push(`would_replace_working_image:${row.recordId}`);
        } else {
          imagesMaterialized.push(materialization);
          presentationPatches.push(materialization);
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
    const canonical = chooseCanonicalAsset(group);
    duplicateCanonicalFindings.push({
      dedupeKey: key,
      canonicalRecordId: canonical.id,
      duplicateRecordIds: group.filter((g) => g.id !== canonical.id).map((g) => g.id),
      canonicalFounderApproved: isFounderApprovedRecord(canonical),
      duplicatesFounderApproved: group
        .filter((g) => g.id !== canonical.id && isFounderApprovedRecord(g))
        .map((g) => g.id),
    });

    for (const dup of group) {
      const notePatch = proposeSupersededDuplicateNote(dup, canonical.id);
      if (notePatch) registryPatches.push(notePatch);
    }
  }

  const openingsBeforeChips = new Map();
  for (const row of presentationRows.filter((r) => r.slotKey === "footprint.openings")) {
    openingsBeforeChips.set(row.recordId, openingChipsFromBody(row.body));
  }

  for (const row of presentationRows.filter((r) => r.slotKey === MOMENTUM_SLOT)) {
    const titlePatch = proposeMomentumTitlePatch(row);
    if (titlePatch) {
      momentumHeadingBeforeAfter.push({
        recordId: row.recordId,
        before: titlePatch.currentTitle,
        after: titlePatch.proposedTitle,
      });
      presentationPatches.push(titlePatch);
    }
  }

  const sourceUrlsWouldChange = presentationPatches.some(
    (p) => p.action !== "title_proper_case" && p.fields?.Body
  );
  if (sourceUrlsWouldChange) applyBlockers.push("momentum_source_url_change_blocked");

  const imageFieldsWouldChange = presentationPatches.some((p) => p.fields?.Image);
  const approvalFieldsWouldChange = registryPatches.some((p) => p.wouldAutoApprove);

  for (const row of presentationRows.filter((r) => r.slotKey === "footprint.openings")) {
    const patch = presentationPatches.find((p) => p.recordId === row.recordId && p.fields?.Body);
    if (patch) {
      const afterChips = openingChipsFromBody(patch.fields.Body);
      const beforeChips = openingsBeforeChips.get(row.recordId);
      if (afterChips !== beforeChips) applyBlockers.push(`opening_label_change:${row.recordId}`);
    }
  }

  if (imageFieldsWouldChange && preserveWorkingImages === false && apply) {
    applyBlockers.push("preserve_working_images_gate_missing");
  }

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
      before: { ...existing.before, ...patch.before },
      after: { ...existing.after, ...patch.after },
    });
  }
  const registryPatchesMerged = [...mergedRegistryPatches.values()];

  const applyGatesReady =
    apply &&
    approveBatch &&
    founderApprovedOnly &&
    preserveWorkingImages &&
    noValidationClaim &&
    noOpeningLabelChanges &&
    everhomeOnly;

  const dryRunClean = applyBlockers.length === 0;
  const hasProposedChanges =
    registryPatchesMerged.length > 0 ||
    momentumHeadingBeforeAfter.length > 0 ||
    imagesMaterialized.length > 0;

  const canApply = applyGatesReady && dryRunClean && hasProposedChanges;

  let airtableModified = false;
  const applyResults = { registryUpdated: [], presentationUpdated: [], errors: [] };

  if (canApply) {
    for (const patch of registryPatchesMerged) {
      try {
        const validation = validateRegistryPatchFields(patch.fields);
        if (!validation.valid) {
          applyResults.errors.push({
            recordId: patch.recordId,
            message: validation.errors.join("; "),
          });
          continue;
        }
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

    for (const patch of presentationPatches) {
      try {
        for (const key of Object.keys(patch.fields)) {
          if (BLOCKED_PRESENTATION_PATCH_FIELDS.has(key) && patch.action === "title_proper_case") {
            if (key !== "Title") throw new Error(`Blocked presentation field: ${key}`);
          }
          if (BLOCKED_PRESENTATION_PATCH_FIELDS.has(key) && key !== "Title") {
            throw new Error(`Blocked presentation field: ${key}`);
          }
        }
        if (patch.fields?.Image) {
          const row = presentationRows.find((r) => r.recordId === patch.recordId);
          if (row?.hasImage && imageLoadingInApi(row, apiBlockForRow(apiBlocks, row))) {
            applyBlockers.push(`apply_blocked_working_image:${patch.recordId}`);
            continue;
          }
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

  const report = {
    writerVersion: WRITER_VERSION,
    v32fWriterExists: v32fWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    workingImageAudit,
    registryRecognitionFindings,
    duplicateCanonicalFindings,
    registryPatches: registryPatchesMerged.map((p) => ({
      recordId: p.recordId,
      fields: Object.keys(p.fields),
      wouldAutoApprove: p.wouldAutoApprove || false,
    })),
    imagesMaterialized: imagesMaterialized.map((m) => ({
      recordId: m.recordId,
      slotKey: m.slotKey,
      registryRecordId: m.registryRecordId,
      reason: m.reason,
    })),
    imagesPreserved,
    momentumHeadingBeforeAfter,
    sourceUrlsChanged: sourceUrlsWouldChange,
    imageApprovalsChanged: approvalFieldsWouldChange,
    imageFieldsChanged: imageFieldsWouldChange,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    applyBlockers,
    dryRunClean,
    applyResults,
    expectedFinalQaResult: finalQaBefore
      ? `${finalQaBefore.overallStatus} (${finalQaBefore.defectCount} defects) — v32F improves registry linkage + momentum Proper Case; pending founder image approvals remain until registry rows are approved`
      : "Run brand-explorer-final-qa-auditor after apply",
    expectedCompleteBuildResult: completeBuildBefore
      ? `${completeBuildBefore.overallStatus} — active-profile still blocked until founder-approved registry assets recognized`
      : "Run brand-explorer-complete-build after apply",
    recommendedNextWriter:
      "v32G — Everhome per-brand activation (Final QA + Complete Build until readyForActiveProfile) after founder registry image approvals",
    exactApplyCommand: dryRunClean && hasProposedChanges ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-everhome-image-governance-recognition-writer -- --brand everhome-suites --dry-run",
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
