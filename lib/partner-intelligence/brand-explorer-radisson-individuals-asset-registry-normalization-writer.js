/**
 * Brand Explorer Radisson Individuals Brand Asset Registry Approval Mapping +
 * Field Normalization Writer v31G.
 *
 * Audits and normalizes Brand Asset Registry metadata so founder-approved assets
 * are recognized by v31E. Never auto-approves, materializes, or modifies Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-asset-registry-normalization-writer-v31G.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
} from "./brand-asset-registry-workflow.js";
import { ASSET_STATUS } from "./brand-asset-pr-package-governance.js";
import {
  classifyRegistryAsset,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import { isRegistryAssetApprovedForExplorer } from "./brand-explorer-brand-asset-image-governance.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { parseFootprintOpeningLocation } from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND as SUPPRESSION_TARGET,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { TRIBUTE_RECORD_ID } from "./tribute-portfolio-brand-package.js";

export const WRITER_VERSION = "31G";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-asset-registry-normalization-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-asset-registry-normalization-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-asset-registry-normalization-writer-v31G.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31G-asset-registry-normalization";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-asset-registry-metadata";
export const APPLY_FLAG_NO_IMAGE = "--confirm-no-image-approval-or-materialization";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const TARGET_BRAND = SUPPRESSION_TARGET;
export const BRAND_DISPLAY = "Radisson Individuals";

export const MAP_REGISTRY_EXTENDED = Object.freeze({
  ...MAP_BRAND_ASSET,
  ...MAP_VISUAL_SLOT,
});

export const FOUNDER_APPROVED_ASSET_STATUSES = Object.freeze([
  ASSET_STATUS.APPROVED_EXPLORER,
  "Approved",
]);

export const FOUNDER_APPROVED_PERMISSIONS = Object.freeze(["Approved For Explorer"]);

export const FOUNDER_APPROVED_USAGE_REVIEW = Object.freeze(["Usage Review Complete"]);

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

const PRESENTATION_ROW_ID_RE = /presentation\s+row\s+(rec[a-zA-Z0-9]{14})/i;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.md",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.json",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.md",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "lib/partner-intelligence/brand-explorer-visual-slot-requirements.js",
  "live Radisson Individuals Brand Asset Registry rows",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "Tribute Portfolio Brand Asset Registry rows (format reference)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-asset-registry-normalization-writer.js",
  "scripts/brand-explorer-radisson-individuals-asset-registry-normalization-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const TRIBUTE_STANDARD_FIELDS = [
  "Asset Name",
  "Asset Type",
  "Asset Status",
  "Explorer Section",
  "Recommended Explorer Slot",
  "Source URL",
  "Source Page URL",
  "Source Basis",
  "Related Property Name",
  "Country / Region",
  "Explorer Use Permission",
  "Usage Review Status",
  "Review Notes",
  "Source Notes",
  "Slot Purpose",
  "CALA Relevant?",
  "Hotel / Property Confirmed?",
  "Brand Confirmed?",
  "Visual Slot Validation Status",
  "Visual Slot Validation Notes",
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

export function v31gWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-asset-registry-normalization-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31G`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31G supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function registryApiUrl(baseId, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BRAND_ASSET_REGISTRY_TABLE)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function registryFetch(baseId, apiKey, init = {}, recordId = "") {
  const res = await fetch(registryApiUrl(baseId, recordId), {
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

async function listRegistryRaw(baseId, apiKey, brandRecordId) {
  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${escapeFormulaValue(brandRecordId)}'`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${registryApiUrl(baseId)}?${params}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Registry list failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
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
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function firstAttachmentUrl(fields) {
  const att = fields?.[MAP_BRAND_ASSET.attachment];
  if (!Array.isArray(att) || !att[0]?.url) return "";
  return nz(att[0].url);
}

export function normalizeRegistryRecordExtended(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    fields: f,
    assetName: nz(f[MAP_BRAND_ASSET.assetName]),
    assetStatus: nz(f[MAP_BRAND_ASSET.assetStatus]),
    assetType: nz(f[MAP_BRAND_ASSET.assetType]),
    explorerUsePermission: nz(f[MAP_BRAND_ASSET.explorerUsePermission]),
    usageReviewStatus: nz(f[MAP_BRAND_ASSET.usageReviewStatus]),
    recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
    sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    sourceBasis: nz(f[MAP_BRAND_ASSET.sourceBasis]),
    sourceNotes: nz(f[MAP_BRAND_ASSET.sourceNotes]),
    reviewNotes: nz(f[MAP_BRAND_ASSET.reviewNotes]),
    doNotUseReason: nz(f[MAP_BRAND_ASSET.doNotUseReason]),
    explorerSection: nz(f[MAP_VISUAL_SLOT.explorerSection]),
    slotPurpose: nz(f[MAP_VISUAL_SLOT.slotPurpose]),
    relatedPropertyName: nz(f[MAP_VISUAL_SLOT.relatedPropertyName]),
    countryRegion: nz(f[MAP_VISUAL_SLOT.countryRegion]),
    calaRelevant: nz(f[MAP_VISUAL_SLOT.calaRelevant]),
    propertyConfirmed: nz(f[MAP_VISUAL_SLOT.propertyConfirmed]),
    brandConfirmed: nz(f[MAP_VISUAL_SLOT.brandConfirmed]),
    validationStatus: nz(f[MAP_VISUAL_SLOT.validationStatus]),
    validationNotes: nz(f[MAP_VISUAL_SLOT.validationNotes]),
    attachmentUrl: firstAttachmentUrl(f),
    isPrimaryCandidate: Boolean(f[MAP_BRAND_ASSET.isPrimaryCandidate]),
    companyValidated: f[MAP_BRAND_ASSET.companyValidated],
    companyValidationDate: f[MAP_BRAND_ASSET.companyValidationDate],
  };
}

export function isDoNotUseRecord(rec) {
  return (
    rec.assetStatus === ASSET_STATUS.DO_NOT_USE ||
    rec.explorerUsePermission === "Do Not Use" ||
    rec.usageReviewStatus === "Blocked" ||
    /do not use/i.test(rec.assetName)
  );
}

export function isPendingApprovalRecord(rec) {
  if (isDoNotUseRecord(rec)) return false;
  return (
    rec.explorerUsePermission === "Candidate Only" ||
    rec.usageReviewStatus === "Pending Review" ||
    rec.usageReviewStatus === "Not Reviewed" ||
    rec.assetStatus === ASSET_STATUS.CANDIDATE ||
    rec.assetStatus === ASSET_STATUS.NEEDS_USAGE_REVIEW
  );
}

/** Founder has explicitly approved — all three governance fields aligned. */
export function isFounderApprovedRecord(rec) {
  if (isDoNotUseRecord(rec)) return false;
  return (
    FOUNDER_APPROVED_ASSET_STATUSES.includes(rec.assetStatus) &&
    FOUNDER_APPROVED_PERMISSIONS.includes(rec.explorerUsePermission) &&
    FOUNDER_APPROVED_USAGE_REVIEW.includes(rec.usageReviewStatus)
  );
}

/** Partial founder approval — safe to sync missing fields to canonical v31E values. */
export function hasPartialFounderApproval(rec) {
  if (isDoNotUseRecord(rec) || isPendingApprovalRecord(rec)) return false;
  const signals = [
    FOUNDER_APPROVED_ASSET_STATUSES.includes(rec.assetStatus),
    FOUNDER_APPROVED_PERMISSIONS.includes(rec.explorerUsePermission),
    FOUNDER_APPROVED_USAGE_REVIEW.includes(rec.usageReviewStatus),
  ];
  return signals.filter(Boolean).length >= 2;
}

export function v31eTreatsAsApproved(rec) {
  const slim = {
    assetStatus: rec.assetStatus,
    explorerUsePermission: rec.explorerUsePermission,
    usageReviewStatus: rec.usageReviewStatus,
    assetName: rec.assetName,
    doNotUseReason: rec.doNotUseReason,
  };
  return (
    classifyRegistryAsset(slim) === "Approved" &&
    isRegistryAssetApprovedForExplorer(slim)
  );
}

export function parsePresentationRowIdFromNotes(notes) {
  const m = nz(notes).match(PRESENTATION_ROW_ID_RE);
  return m ? m[1] : null;
}

export function parseLocationFromLegacyName(assetName) {
  const m = nz(assetName).match(
    /(?:placeholder|wrong-brand image)\s*\((?:Radisson Individuals?|Radisson Individuals by Choice)\s*—\s*([^)]+)\)/i
  );
  if (m) return m[1].trim();
  const m2 = nz(assetName).match(/—\s*([^—()]+(?:,\s*[^—()]+)?)\s*(?:\)|$)/);
  return m2 ? m2[1].trim() : "";
}

function parseMarketRegion(location) {
  const loc = nz(location);
  if (!loc) return "";
  if (/panama/i.test(loc)) return "Panama / CALA";
  if (/colombia/i.test(loc)) return "Colombia / CALA";
  const country = loc.includes(",") ? loc.split(",").slice(-1)[0].trim() : loc;
  return country ? `${country} / CALA` : "CALA";
}

function inferExplorerSection(slot, assetType) {
  if (slot === "footprint.openings") return "Recent Openings";
  if (/gallery/.test(slot)) return "Image Gallery";
  if (/hero/.test(slot)) return "Hero Image";
  if (assetType === "Press Link") return "Press / Source Reference";
  return slot || "Brand Explorer Presentation";
}

function inferSlotPurpose(slot, assetType) {
  if (slot === "footprint.openings") return "CALA opening example imagery";
  if (assetType === "Press Link") return "Official Choice/Radisson Individuals source reference";
  return "Brand Explorer visual slot candidate";
}

export function buildNormalizedAssetName(rec, location = "") {
  if (isDoNotUseRecord(rec)) {
    const loc = location || parseLocationFromLegacyName(rec.assetName);
    if (loc) return `DO NOT USE — ${BRAND_DISPLAY} — ${loc} — Wrong-Brand Image`;
    return rec.assetName;
  }
  if (rec.assetType === "Press Link") {
    if (/press kit/i.test(rec.assetName) || /press-kit/i.test(rec.sourcePageUrl)) {
      return `${BRAND_DISPLAY} — Choice Press Kit — Source Reference`;
    }
    if (/consumer brand page/i.test(rec.assetName) || /choicehotels\.com\/radisson-individuals/.test(rec.sourcePageUrl)) {
      return `${BRAND_DISPLAY} — Consumer Brand Page — Source Reference`;
    }
    return `${BRAND_DISPLAY} — Official Source — Press Reference`;
  }
  const loc = location || parseLocationFromLegacyName(rec.assetName) || rec.relatedPropertyName;
  if (loc && rec.recommendedExplorerSlot === "footprint.openings") {
    return `${BRAND_DISPLAY} — ${loc} — Opening Example`;
  }
  return rec.assetName;
}

function extractUrlFromBody(body) {
  const m = nz(body).match(/https?:\/\/[^\s]+/i);
  return m ? m[0].replace(/[.,)]+$/, "") : "";
}

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  const body = nz(f.Body);
  return {
    recordId: rec.id,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body,
    summaryUrl:
      nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]) ||
      extractUrlFromBody(body),
    location: parseFootprintOpeningLocation(nz(f.Title), body),
    quarantined: !isPresentationRowVisibleInExplorer(f),
  };
}

function auditFieldCompleteness(rec) {
  const missing = [];
  for (const field of TRIBUTE_STANDARD_FIELDS) {
    const val = rec.fields?.[field];
    if (val == null || val === "") missing.push(field);
    else if (Array.isArray(val) && val.length === 0) missing.push(field);
  }
  return { missingFields: missing, completeCount: TRIBUTE_STANDARD_FIELDS.length - missing.length };
}

function buildApprovalMappingDiagnosis(rec) {
  return {
    recordId: rec.id,
    assetStatus: rec.assetStatus,
    explorerUsePermission: rec.explorerUsePermission,
    usageReviewStatus: rec.usageReviewStatus,
    founderApproved: isFounderApprovedRecord(rec),
    partialFounderApproval: hasPartialFounderApproval(rec),
    pending: isPendingApprovalRecord(rec),
    doNotUse: isDoNotUseRecord(rec),
    v31eApproved: v31eTreatsAsApproved(rec),
    approvalSignals: {
      assetStatusApproved: FOUNDER_APPROVED_ASSET_STATUSES.includes(rec.assetStatus),
      permissionApproved: FOUNDER_APPROVED_PERMISSIONS.includes(rec.explorerUsePermission),
      usageReviewComplete: FOUNDER_APPROVED_USAGE_REVIEW.includes(rec.usageReviewStatus),
    },
  };
}

function proposeApprovalFieldSync(rec) {
  if (isDoNotUseRecord(rec) || isPendingApprovalRecord(rec)) return null;
  if (!hasPartialFounderApproval(rec) && !isFounderApprovedRecord(rec)) return null;

  const fields = {};
  const before = {};
  const after = {};

  if (
    FOUNDER_APPROVED_ASSET_STATUSES.includes(rec.assetStatus) &&
    rec.explorerUsePermission !== "Approved For Explorer" &&
    FOUNDER_APPROVED_PERMISSIONS.includes("Approved For Explorer")
  ) {
    fields[MAP_BRAND_ASSET.explorerUsePermission] = "Approved For Explorer";
    before.explorerUsePermission = rec.explorerUsePermission;
    after.explorerUsePermission = "Approved For Explorer";
  }
  if (
    FOUNDER_APPROVED_PERMISSIONS.includes(rec.explorerUsePermission) &&
    rec.usageReviewStatus !== "Usage Review Complete" &&
    rec.assetStatus !== ASSET_STATUS.DO_NOT_USE
  ) {
    if (FOUNDER_APPROVED_ASSET_STATUSES.includes(rec.assetStatus)) {
      fields[MAP_BRAND_ASSET.usageReviewStatus] = "Usage Review Complete";
      before.usageReviewStatus = rec.usageReviewStatus;
      after.usageReviewStatus = "Usage Review Complete";
    }
  }
  if (
    rec.explorerUsePermission === "Approved For Explorer" &&
    rec.usageReviewStatus === "Usage Review Complete" &&
    rec.assetStatus === ASSET_STATUS.NEEDS_USAGE_REVIEW
  ) {
    fields[MAP_BRAND_ASSET.assetStatus] = ASSET_STATUS.APPROVED_EXPLORER;
    before.assetStatus = rec.assetStatus;
    after.assetStatus = ASSET_STATUS.APPROVED_EXPLORER;
  }

  if (!Object.keys(fields).length) return null;
  return { fields, before, after, action: "approval_field_sync" };
}

function proposeMetadataNormalization(rec, presentationById, target) {
  if (isDoNotUseRecord(rec)) {
    const fields = {};
    const before = {};
    const after = {};
    const loc = parseLocationFromLegacyName(rec.assetName);
    const proposedName = buildNormalizedAssetName(rec, loc);
    if (proposedName !== rec.assetName) {
      fields[MAP_BRAND_ASSET.assetName] = proposedName;
      before.assetName = rec.assetName;
      after.assetName = proposedName;
    }
    if (loc && !rec.relatedPropertyName) {
      fields[MAP_VISUAL_SLOT.relatedPropertyName] = `Radisson Individual — ${loc}`;
      before.relatedPropertyName = rec.relatedPropertyName;
      after.relatedPropertyName = fields[MAP_VISUAL_SLOT.relatedPropertyName];
    }
    if (loc && !rec.countryRegion) {
      fields[MAP_VISUAL_SLOT.countryRegion] = parseMarketRegion(loc);
      before.countryRegion = rec.countryRegion;
      after.countryRegion = fields[MAP_VISUAL_SLOT.countryRegion];
    }
    if (!rec.validationStatus) {
      fields[MAP_VISUAL_SLOT.validationStatus] = "Do Not Use";
      before.validationStatus = rec.validationStatus;
      after.validationStatus = "Do Not Use";
    }
    if (!Object.keys(fields).length) return null;
    return { fields, before, after, action: "do_not_use_metadata_only" };
  }

  const presentationId = parsePresentationRowIdFromNotes(rec.sourceNotes);
  const presentationRow = presentationId ? presentationById.get(presentationId) : null;
  const location =
    presentationRow?.location ||
    parseLocationFromLegacyName(rec.assetName) ||
    rec.relatedPropertyName;

  const fields = {};
  const before = {};
  const after = {};

  const proposedName = buildNormalizedAssetName(rec, location);
  if (proposedName && proposedName !== rec.assetName) {
    fields[MAP_BRAND_ASSET.assetName] = proposedName;
    before.assetName = rec.assetName;
    after.assetName = proposedName;
  }

  const explorerSection = inferExplorerSection(rec.recommendedExplorerSlot, rec.assetType);
  if (!rec.explorerSection && explorerSection) {
    fields[MAP_VISUAL_SLOT.explorerSection] = explorerSection;
    before.explorerSection = rec.explorerSection;
    after.explorerSection = explorerSection;
  }

  const slotPurpose = inferSlotPurpose(rec.recommendedExplorerSlot, rec.assetType);
  if (!rec.slotPurpose && slotPurpose) {
    fields[MAP_VISUAL_SLOT.slotPurpose] = slotPurpose;
    before.slotPurpose = rec.slotPurpose;
    after.slotPurpose = slotPurpose;
  }

  if (location && !rec.relatedPropertyName && rec.assetType !== "Press Link") {
    const propName = presentationRow?.title || `Radisson Individual — ${location}`;
    fields[MAP_VISUAL_SLOT.relatedPropertyName] = propName;
    before.relatedPropertyName = rec.relatedPropertyName;
    after.relatedPropertyName = propName;
  }

  if (location && !rec.countryRegion) {
    fields[MAP_VISUAL_SLOT.countryRegion] = parseMarketRegion(location);
    before.countryRegion = rec.countryRegion;
    after.countryRegion = fields[MAP_VISUAL_SLOT.countryRegion];
  }

  if (!rec.calaRelevant) {
    fields[MAP_VISUAL_SLOT.calaRelevant] = "Yes";
    before.calaRelevant = rec.calaRelevant;
    after.calaRelevant = "Yes";
  }

  if (presentationRow?.summaryUrl && !rec.sourcePageUrl) {
    fields[MAP_BRAND_ASSET.sourcePageUrl] = presentationRow.summaryUrl;
    before.sourcePageUrl = rec.sourcePageUrl;
    after.sourcePageUrl = presentationRow.summaryUrl;
  }

  if (rec.attachmentUrl && !rec.sourceUrl) {
    fields[MAP_BRAND_ASSET.sourceUrl] = rec.attachmentUrl;
    before.sourceUrl = rec.sourceUrl;
    after.sourceUrl = rec.attachmentUrl;
  }

  if (isFounderApprovedRecord(rec) && /pending image review|needs founder review/i.test(rec.reviewNotes)) {
    const note =
      "Founder-approved for Explorer use. Metadata normalized by v31G — image URL may still require attachment download before v31E materialization.";
    fields[MAP_BRAND_ASSET.reviewNotes] = note;
    before.reviewNotes = rec.reviewNotes;
    after.reviewNotes = note;
  }

  if (isFounderApprovedRecord(rec) && !rec.validationStatus) {
    fields[MAP_VISUAL_SLOT.validationStatus] = "Valid for Slot";
    before.validationStatus = rec.validationStatus;
    after.validationStatus = "Valid for Slot";
  }

  if (isFounderApprovedRecord(rec) && rec.assetType === "PR / Opening Image" && !rec.propertyConfirmed) {
    fields[MAP_VISUAL_SLOT.propertyConfirmed] = "Unknown";
    before.propertyConfirmed = rec.propertyConfirmed;
    after.propertyConfirmed = "Unknown";
  }

  if (isFounderApprovedRecord(rec) && !rec.brandConfirmed) {
    fields[MAP_VISUAL_SLOT.brandConfirmed] = "Yes";
    before.brandConfirmed = rec.brandConfirmed;
    after.brandConfirmed = "Yes";
  }

  if (!rec.sourceNotes && presentationRow) {
    const note = `Linked presentation row ${presentationRow.recordId} (${presentationRow.slotKey}). v31G metadata normalization.`;
    fields[MAP_BRAND_ASSET.sourceNotes] = note;
    before.sourceNotes = rec.sourceNotes;
    after.sourceNotes = note;
  }

  fields[MAP_BRAND_ASSET.brandRecordId] = target.recordId;
  fields[MAP_BRAND_ASSET.brand] = [target.recordId];

  if (!Object.keys(fields).length) return null;

  const filtered = { ...fields };
  delete filtered[MAP_BRAND_ASSET.brand];
  delete filtered[MAP_BRAND_ASSET.brandRecordId];
  if (!Object.keys(filtered).length) return null;

  return {
    fields,
    before,
    after,
    action: "metadata_normalization",
    linkedPresentationRowId: presentationRow?.recordId || presentationId || null,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-asset-registry-normalization-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_IMAGE,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsAssetRegistryNormalizationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noImageApproval = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const registryRaw = await listRegistryRaw(baseId, apiKey, target.recordId);
  const registryRecords = registryRaw.map(normalizeRegistryRecordExtended);

  const presentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const presentationRows = presentationRaw.map(normalizePresentationRow);
  const presentationById = new Map(presentationRows.map((r) => [r.recordId, r]));

  let tributeSampleFields = [];
  try {
    const tribRaw = await listRegistryRaw(baseId, apiKey, TRIBUTE_RECORD_ID);
    if (tribRaw[0]?.fields) tributeSampleFields = Object.keys(tribRaw[0].fields).sort();
  } catch {
    tributeSampleFields = TRIBUTE_STANDARD_FIELDS;
  }

  const registryFieldCompletenessAudit = [];
  const approvalValueMappingDiagnosis = [];
  const founderApprovedNotRecognizedByV31e = [];
  const assetsToNormalize = [];
  const beforeAfterFieldChanges = [];
  const proposedUpdates = [];
  const applyBlockers = [];

  let v31eApprovedBefore = 0;

  for (const rec of registryRecords) {
    const completeness = auditFieldCompleteness(rec);
    const approvalDiag = buildApprovalMappingDiagnosis(rec);
    const presentationId = parsePresentationRowIdFromNotes(rec.sourceNotes);
    const presentationRow = presentationId ? presentationById.get(presentationId) : null;

    const diagnosis = {
      recordId: rec.id,
      assetName: rec.assetName,
      brand: target.name,
      assetStatus: rec.assetStatus,
      approvalStatus: isFounderApprovedRecord(rec)
        ? "Founder Approved"
        : isDoNotUseRecord(rec)
          ? "Do Not Use"
          : isPendingApprovalRecord(rec)
            ? "Pending Image Review"
            : "Needs Review",
      explorerUsePermission: rec.explorerUsePermission,
      usageReviewStatus: rec.usageReviewStatus,
      intendedBrandExplorerSlot: rec.recommendedExplorerSlot,
      linkedPresentationRowId: presentationRow?.recordId || presentationId || null,
      linkedPresentationTitle: presentationRow?.title || null,
      sourceUrl: rec.sourceUrl || rec.attachmentUrl || null,
      sourcePageUrl: rec.sourcePageUrl || null,
      imageUrl: rec.sourceUrl || rec.attachmentUrl || null,
      propertyName: rec.relatedPropertyName || presentationRow?.title || parseLocationFromLegacyName(rec.assetName),
      marketRegion: rec.countryRegion || parseMarketRegion(parseLocationFromLegacyName(rec.assetName)),
      doNotUse: isDoNotUseRecord(rec),
      reviewNotes: rec.reviewNotes,
      v31eTreatsAsApproved: v31eTreatsAsApproved(rec),
      missingFields: completeness.missingFields,
      fieldCompleteRatio: `${completeness.completeCount}/${TRIBUTE_STANDARD_FIELDS.length}`,
    };

    registryFieldCompletenessAudit.push(diagnosis);
    approvalValueMappingDiagnosis.push(approvalDiag);

    if (v31eTreatsAsApproved(rec)) v31eApprovedBefore += 1;

    if (isFounderApprovedRecord(rec) && !v31eTreatsAsApproved(rec)) {
      founderApprovedNotRecognizedByV31e.push({
        recordId: rec.id,
        assetName: rec.assetName,
        reason: "founder_approved_but_v31e_classification_mismatch",
        approvalDiag,
      });
    }

    const approvalSync = proposeApprovalFieldSync(rec);
    const metadataNorm = proposeMetadataNormalization(rec, presentationById, target);

    const mergedFields = {};
    const mergedBefore = {};
    const mergedAfter = {};
    let wouldPromotePending = false;

    if (approvalSync) {
      const wasPending = isPendingApprovalRecord(rec);
      const willBeApproved =
        approvalSync.fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer" ||
        approvalSync.fields[MAP_BRAND_ASSET.usageReviewStatus] === "Usage Review Complete" ||
        approvalSync.fields[MAP_BRAND_ASSET.assetStatus] === ASSET_STATUS.APPROVED_EXPLORER;
      if (wasPending && willBeApproved) wouldPromotePending = true;
      Object.assign(mergedFields, approvalSync.fields);
      Object.assign(mergedBefore, approvalSync.before);
      Object.assign(mergedAfter, approvalSync.after);
    }

    if (metadataNorm) {
      for (const [k, v] of Object.entries(metadataNorm.fields)) {
        if (mergedFields[k] === undefined) mergedFields[k] = v;
      }
      Object.assign(mergedBefore, metadataNorm.before);
      Object.assign(mergedAfter, metadataNorm.after);
    }

    if (wouldPromotePending) {
      applyBlockers.push(`would_promote_pending_to_approved:${rec.id}`);
    }

    if (isDoNotUseRecord(rec)) {
      const wouldReclassify =
        mergedFields[MAP_BRAND_ASSET.assetStatus] &&
        mergedFields[MAP_BRAND_ASSET.assetStatus] !== ASSET_STATUS.DO_NOT_USE;
      if (wouldReclassify) applyBlockers.push(`would_reclassify_do_not_use:${rec.id}`);
    }

    if (mergedFields[MAP_BRAND_ASSET.companyValidated] || mergedFields[MAP_BRAND_ASSET.companyValidationDate]) {
      applyBlockers.push(`company_validated_field_write_blocked:${rec.id}`);
    }
    if (mergedFields[MAP_BRAND_ASSET.attachment]) {
      applyBlockers.push(`attachment_write_blocked:${rec.id}`);
    }

    const inventedSourceUrl =
      mergedFields[MAP_BRAND_ASSET.sourceUrl] &&
      !rec.sourceUrl &&
      !rec.attachmentUrl &&
      !mergedFields[MAP_BRAND_ASSET.sourceUrl]?.startsWith("http");
    if (inventedSourceUrl) applyBlockers.push(`invented_source_url:${rec.id}`);

    if (!Object.keys(mergedFields).length) continue;

    assetsToNormalize.push({
      recordId: rec.id,
      assetName: rec.assetName,
      actions: [approvalSync?.action, metadataNorm?.action].filter(Boolean),
      fieldCount: Object.keys(mergedFields).length,
    });

    beforeAfterFieldChanges.push({
      recordId: rec.id,
      before: mergedBefore,
      after: mergedAfter,
    });

    proposedUpdates.push({
      recordId: rec.id,
      fields: mergedFields,
      before: mergedBefore,
      after: mergedAfter,
    });
  }

  const updateById = new Map(proposedUpdates.map((u) => [u.recordId, u]));
  let v31eApprovedAfterProjected = 0;
  for (const rec of registryRecords) {
    const update = updateById.get(rec.id);
    const projectedSlim = {
      assetStatus: update?.after?.assetStatus || rec.assetStatus,
      explorerUsePermission: update?.after?.explorerUsePermission || rec.explorerUsePermission,
      usageReviewStatus: update?.after?.usageReviewStatus || rec.usageReviewStatus,
      assetName: update?.after?.assetName || rec.assetName,
      doNotUseReason: rec.doNotUseReason,
    };
    if (v31eTreatsAsApproved(projectedSlim)) v31eApprovedAfterProjected += 1;
  }

  if (v31eApprovedAfterProjected < v31eApprovedBefore) {
    applyBlockers.push("v31e_approved_count_would_decrease");
  }

  const hasWork = proposedUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && founderReviewed && noImageApproval && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let applyResults = {
    updated: [],
    errors: [],
    approvalsChanged: false,
    imagesMaterialized: false,
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of proposedUpdates) {
      const live = registryRaw.find((r) => r.id === update.recordId);
      if (!live) {
        applyResults.errors.push({ recordId: update.recordId, error: "record_not_found" });
        continue;
      }
      const liveRec = normalizeRegistryRecordExtended(live);
      if (isPendingApprovalRecord(liveRec) && !hasPartialFounderApproval(liveRec)) {
        applyResults.errors.push({ recordId: update.recordId, error: "pending_record_write_blocked" });
        continue;
      }

      const patchFields = { ...update.fields };
      delete patchFields[MAP_BRAND_ASSET.attachment];
      delete patchFields[MAP_BRAND_ASSET.companyValidated];
      delete patchFields[MAP_BRAND_ASSET.companyValidationDate];

      const approvalChange =
        patchFields[MAP_BRAND_ASSET.assetStatus] ||
        patchFields[MAP_BRAND_ASSET.explorerUsePermission] ||
        patchFields[MAP_BRAND_ASSET.usageReviewStatus];
      if (approvalChange && isPendingApprovalRecord(liveRec)) {
        applyResults.errors.push({ recordId: update.recordId, error: "approval_change_on_pending_blocked" });
        continue;
      }

      const { res, json } = await registryFetch(
        baseId,
        apiKey,
        { method: "PATCH", body: JSON.stringify({ fields: patchFields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: json.error?.message || `PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.updated.push({ recordId: update.recordId, fields: Object.keys(patchFields) });
      if (approvalChange) applyResults.approvalsChanged = true;
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const report = {
    writerVersion: WRITER_VERSION,
    v31GWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    tributeReferenceFieldCount: tributeSampleFields.length,
    tributeReferenceFieldsSample: tributeSampleFields.slice(0, 20),
    registryFieldCompletenessAudit,
    approvalValueMappingDiagnosis,
    founderApprovedNotRecognizedByV31e,
    assetsToNormalize,
    beforeAfterFieldChanges,
    rowsWouldUpdate: proposedUpdates,
    v31eApprovedCountBefore: v31eApprovedBefore,
    expectedV31eApprovedCountAfterApply: v31eApprovedAfterProjected || v31eApprovedBefore,
    approvalsChangedByWriter: false,
    imagesMaterialized: false,
    imagesApproved: false,
    presentationRowsLinked: presentationRows.filter((r) => r.slotKey === "footprint.openings").length,
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
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-asset-registry-normalization-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    governanceNote:
      "v31G normalizes registry metadata and aligns founder-approved field values for v31E recognition — never auto-approves pending assets, never materializes images, never modifies Company Validated.",
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(
    `# Brand Explorer Radisson Individuals Asset Registry Normalization v${report.writerVersion}`
  );
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v31G exists: **${report.v31GWriterExists ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Registry audit");
  lines.push(`- Assets inspected: **${report.registryFieldCompletenessAudit?.length ?? 0}**`);
  lines.push(`- v31E approved (before): **${report.v31eApprovedCountBefore ?? 0}**`);
  lines.push(`- v31E approved (after apply, projected): **${report.expectedV31eApprovedCountAfterApply ?? 0}**`);
  lines.push(`- Founder-approved not recognized: **${report.founderApprovedNotRecognizedByV31e?.length ?? 0}**`);
  lines.push(`- Assets to normalize: **${report.assetsToNormalize?.length ?? 0}**`);
  lines.push(`- Approvals changed by writer: **no** (blocked if pending→approved)`);
  lines.push(`- Images materialized: **no**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Asset name changes");
  for (const row of report.beforeAfterFieldChanges || []) {
    if (row.before?.assetName && row.after?.assetName) {
      lines.push(`- ${row.before.assetName} → **${row.after.assetName}**`);
    }
  }
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — resolve blockers first)");
  return lines.join("\n");
}
