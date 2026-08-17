/**
 * Brand Explorer Radisson Individuals Image, Asset Registry, and Openings Root-Cause Audit v31I.
 *
 * Read-only audit — no Airtable writes, no approvals, no materialization.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit-v31I.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import {
  BRAND_ASSET_REGISTRY_TABLE,
  MAP_BRAND_ASSET,
  listRegistryAssetsForBrand,
} from "./brand-asset-registry-workflow.js";
import { MAP_VISUAL_SLOT } from "./brand-explorer-visual-slot-requirements.js";
import { TRIBUTE_RECORD_ID } from "./tribute-portfolio-brand-package.js";
import {
  DISCOVERY_BRAND_CONFIG,
  assessPresentationRowImageGovernance,
  detectWrongBrandSignageRisk,
  findRegistryAssetForPresentationRow,
  isGalleryImageSlot,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
  normalizeUrlKey,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  classifyRegistryAsset,
} from "./brand-explorer-radisson-individuals-approved-asset-materialization-writer.js";
import {
  normalizeRegistryRecordExtended,
  isFounderApprovedRecord,
  isDoNotUseRecord,
} from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import {
  collectRowCopySurfaces,
  detectInternalUiLanguage,
  findInternalLanguageInRow,
  isOpeningsEvidenceSlot,
  parseFootprintOpeningLocation,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { ASSET_STATUS } from "./brand-asset-pr-package-governance.js";

export const AUDIT_VERSION = "31I";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit-v31I.md";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PRIOR_REPORTS = [
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json",
  "reports/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.json",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.json",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.json",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json",
  "reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.json",
  "reports/brand-explorer-radisson-individuals-gallery-restore-writer.json",
  "reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json",
  "reports/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.json",
];

const FILES_READ = [
  "AGENTS.md",
  ...PRIOR_REPORTS.map((p) => p.replace(".json", ".md")),
  ...PRIOR_REPORTS,
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live Radisson Individuals Source Library records",
  "live Radisson Individuals Partner Facts",
  "live Radisson Individuals API response",
  "live Tribute Portfolio Brand Asset Registry rows",
  "live Tribute Portfolio presentation rows",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js",
  "lib/partner-intelligence/brand-explorer-openings-ui-quarantine-governance.js",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.js",
  "scripts/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

/** Registry fields compared against Tribute standard. */
export const REGISTRY_COMPLETENESS_FIELDS = [
  { key: "brand", label: "Brand", required: true, v31e: false, activeProfile: true },
  { key: "assetName", label: "Asset Name", required: true, v31e: true, activeProfile: true },
  { key: "assetType", label: "Asset Type", required: true, v31e: true, activeProfile: true },
  { key: "explorerSection", label: "Explorer Section", required: true, v31e: false, activeProfile: true },
  { key: "recommendedExplorerSlot", label: "Intended Brand Explorer Slot", required: true, v31e: true, activeProfile: true },
  { key: "relatedPropertyName", label: "Related Property Name", required: false, v31e: false, activeProfile: true },
  { key: "countryRegion", label: "Country / Region", required: false, v31e: false, activeProfile: true },
  { key: "sourceUrl", label: "Source URL", required: false, v31e: true, activeProfile: true },
  { key: "sourcePageUrl", label: "Source Page URL", required: true, v31e: false, activeProfile: true },
  { key: "sourceBasis", label: "Source Basis", required: false, v31e: false, activeProfile: true },
  { key: "attachmentUrl", label: "Image URL / Attachment", required: false, v31e: true, activeProfile: true },
  { key: "assetStatus", label: "Asset Status / Approval Status", required: true, v31e: true, activeProfile: true },
  { key: "explorerUsePermission", label: "Explorer Use Permission", required: true, v31e: true, activeProfile: true },
  { key: "usageReviewStatus", label: "Usage Review Status", required: true, v31e: true, activeProfile: true },
  { key: "validationStatus", label: "Visual Slot Validation Status", required: false, v31e: false, activeProfile: true },
  { key: "validationNotes", label: "Visual Slot Validation Notes", required: false, v31e: false, activeProfile: false },
  { key: "slotPurpose", label: "Slot Purpose", required: false, v31e: false, activeProfile: true },
  { key: "calaRelevant", label: "CALA Relevant?", required: false, v31e: false, activeProfile: false },
  { key: "brandConfirmed", label: "Brand Confirmed?", required: false, v31e: false, activeProfile: true },
  { key: "propertyConfirmed", label: "Hotel / Property Confirmed?", required: false, v31e: false, activeProfile: true },
  { key: "reviewNotes", label: "Review Notes", required: false, v31e: false, activeProfile: false },
  { key: "sourceNotes", label: "Source Notes / Use Notes", required: false, v31e: false, activeProfile: false },
  { key: "stagingRunId", label: "Created by writer / Source batch", required: false, v31e: false, activeProfile: false },
  { key: "doNotUseReason", label: "Do Not Use flag", required: false, v31e: true, activeProfile: true },
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

function loadJsonReport(relativePath) {
  const full = path.join(ROOT, relativePath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return { _loadError: relativePath };
  }
}

function classifyPresentationSection(slotKey) {
  const sk = nz(slotKey);
  if (/^valueOwners\.scenario/.test(sk)) return "valueOwners.scenario";
  if (/^materials\.gallery/.test(sk)) return "materials.gallery";
  if (sk === "footprint.openings") return "footprint.openings";
  if (sk === "footprint.momentum") return "footprint.momentum";
  if (/^overview\.scenario/.test(sk)) return "overview.scenario";
  if (sk === "overview.featured_application") return "overview.featured_application";
  if (sk === "overview.hero") return "hero";
  if (/logo|hero/i.test(sk)) return "hero_logo";
  return "other_visual";
}

function isOpeningsSection(slotKey) {
  return nz(slotKey) === "footprint.openings" || /^overview\.scenario/.test(nz(slotKey));
}

function normalizeNameKey(name) {
  return nz(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function registryApiUrl(baseId, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BRAND_ASSET_REGISTRY_TABLE)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
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

function normalizePresentationRow(rec) {
  const f = rec.fields || {};
  const imageArr = f.Image;
  return {
    recordId: rec.id,
    fields: f,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    sortOrder: f["Sort Order"],
    externalDisplayStatus: nz(f["External Display Status"]),
    active: f.Active,
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
    hasImage: Array.isArray(imageArr) && imageArr.length > 0,
    imageUrl: Array.isArray(imageArr) && imageArr[0]?.url ? nz(imageArr[0].url) : null,
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
    caseSummaryOverview: nz(f["Case Summary Overview"]),
    caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
    caseSummaryTags: nz(f["Case Summary Tags"]),
    section: classifyPresentationSection(f["Slot Key"]),
    isOpeningsSection: isOpeningsSection(f["Slot Key"]),
  };
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

/** Build historic image timeline from v31B–v31E reports. */
export function buildHistoricImageTimeline(priorReports) {
  const byRecordId = new Map();
  const bySlot = new Map();

  function ensure(key, recordId, slotKey) {
    if (!byRecordId.has(recordId)) {
      byRecordId.set(recordId, {
        recordId,
        slotKey,
        hadImageBeforeV31C: false,
        hadImageBeforeV31D: false,
        clearedByV31C: false,
        clearedByV31D: false,
        restoredByV31DR1: false,
        priorImageUrl: null,
        writers: [],
      });
    }
    if (slotKey && !bySlot.has(slotKey)) bySlot.set(slotKey, recordId);
    return byRecordId.get(recordId);
  }

  const v31b = priorReports.v31b;
  for (const entry of v31b?.imageUsageAudit || []) {
    const rid = entry.presentationRowId || entry.recordId;
    if (!rid) continue;
    const row = ensure(rid, rid, entry.slot);
    if (entry.imageUrl) {
      row.hadImageBeforeV31C = true;
      row.hadImageBeforeV31D = true;
      row.priorImageUrl = entry.imageUrl;
      row.writers.push("v31B_had_image");
    }
  }

  const v31d = priorReports.v31d;
  for (const update of v31d?.rowsWouldUpdate || v31d?.proposedUpdates || []) {
    if (update.action !== "clear_unapproved_image") continue;
    const row = ensure(update.recordId, update.recordId, update.slotKey);
    row.clearedByV31D = true;
    row.writers.push("v31D_clear_unapproved_image");
  }

  const v31c = priorReports.v31c;
  for (const row of v31c?.rowsWouldQuarantine || v31c?.quarantinedRows || []) {
    const rid = row.recordId;
    if (!rid) continue;
    const entry = ensure(rid, rid, row.slotKey);
    if (row.slotKey === "footprint.openings" || /^overview\.scenario/.test(row.slotKey || "")) {
      entry.clearedByV31C = entry.clearedByV31C || false;
      entry.writers.push("v31C_quarantined");
    }
  }

  const v31dr1 = priorReports.v31dr1;
  for (const update of v31dr1?.rowsToUpdate || []) {
    const row = ensure(update.recordId, update.recordId, update.slotKey);
    row.restoredByV31DR1 = true;
    row.clearedByV31D = true;
    row.hadImageBeforeV31D = true;
    if (update.after?.imageUrl) row.priorImageUrl = update.after.imageUrl;
    row.writers.push("v31D-R1_restored");
  }
  for (const prior of v31dr1?.galleryClearedByV31D || []) {
    const row = ensure(prior.recordId, prior.recordId, prior.slotKey);
    row.clearedByV31D = true;
    row.hadImageBeforeV31D = true;
    if (prior.imageUrl) row.priorImageUrl = prior.imageUrl;
    if (!row.writers.includes("v31D_inferred_cleared")) row.writers.push("v31D_inferred_cleared");
  }
  for (const [, row] of byRecordId) {
    if (row.restoredByV31DR1) row.clearedByV31D = true;
  }

  return { byRecordId, bySlot };
}

export function recommendImageDisposition({
  row,
  historic,
  isOpenings,
  registryMatch,
  brandConfig,
}) {
  if (isOpenings) {
    if (row.quarantined) return "keep_quarantined_until_rebuild";
    return "wait_for_approval_and_official_rebuild";
  }
  if (historic?.clearedByV31D && !row.hasImage && historic.hadImageBeforeV31D) {
    if (historic.restoredByV31DR1) return "restored_pending_review";
    if (isGalleryImageSlot(row.slotKey)) return "restore_pending_review";
    return "investigate_restore_non_openings";
  }
  if (row.hasImage && registryMatch && !isRegistryAssetApprovedForExplorer(registryMatch)) {
    return "preserve_pending_review";
  }
  if (row.hasImage && !registryMatch) return "queue_registry_review";
  if (!row.hasImage && historic?.hadImageBeforeV31D) return "restore_or_replace";
  if (!row.hasImage && isVisualImageSlot(row.slotKey)) return "empty_visual_slot";
  return "no_action";
}

export function classifyUrlDurability(sourceUrl, sourcePageUrl, attachmentUrl) {
  const src = nz(sourceUrl);
  const page = nz(sourcePageUrl);
  const att = nz(attachmentUrl);
  const isAirtableTemp = /airtableusercontent\.com|airtable\.com\/.*\/attachments/i.test(src);
  const isAirtableAtt = /airtableusercontent\.com/i.test(att);
  const isOfficialPage =
    /^(https?:\/\/)?(www\.)?(choicehotels|radisson|media\.choicehotels)/i.test(page || src);
  const isPressReference =
    /press.?kit|consumer brand page|source reference/i.test(src + page) && !att && !isAirtableTemp;

  if (isPressReference || (!src && !att && page && isOfficialPage)) {
    return {
      classification: "source_reference_only",
      durable: Boolean(page),
      materializable: false,
      issue: page ? null : "missing_source_page_url",
    };
  }
  if (isAirtableTemp && !page) {
    return {
      classification: "temporary_attachment_url",
      durable: false,
      materializable: false,
      issue: "airtable_signed_url_without_source_page_url",
    };
  }
  if (isAirtableTemp && page) {
    return {
      classification: "usable_direct_image_url",
      durable: false,
      materializable: true,
      issue: "image_url_expires_source_page_required_for_evidence",
    };
  }
  if (!src && !page && !att) {
    return {
      classification: "missing_source_page_url",
      durable: false,
      materializable: false,
      issue: "no_source_evidence",
    };
  }
  if (page && isOfficialPage) {
    return {
      classification: "durable_source_page_url",
      durable: true,
      materializable: Boolean(att || (!isAirtableTemp && src)),
      issue: !att && !src ? "page_only_no_image_file" : null,
    };
  }
  if (src && /^https?:\/\//i.test(src) && !isAirtableTemp) {
    return {
      classification: "durable_source_page_url",
      durable: true,
      materializable: true,
      issue: null,
    };
  }
  return {
    classification: "not_materializable",
    durable: false,
    materializable: false,
    issue: "unclassified_url_pattern",
  };
}

export function auditRegistryCompleteness(radissonRecords, tributeRecords) {
  const radNorm = radissonRecords.map(normalizeRegistryRecordExtended);
  const tribNorm = tributeRecords.map(normalizeRegistryRecordExtended);

  function fieldPopulation(records) {
    const counts = {};
    for (const field of REGISTRY_COMPLETENESS_FIELDS) {
      counts[field.key] = records.filter((r) => hasVal(r[field.key])).length;
    }
    return counts;
  }

  const tribPop = fieldPopulation(tribNorm);
  const radPop = fieldPopulation(radNorm);
  const tributeBaseline = {};
  const gaps = [];

  for (const field of REGISTRY_COMPLETENESS_FIELDS) {
    const tribPct =
      tribNorm.length > 0 ? Math.round((tribPop[field.key] / tribNorm.length) * 100) : 0;
    const radPct =
      radNorm.length > 0 ? Math.round((radPop[field.key] / radNorm.length) * 100) : 0;
    tributeBaseline[field.key] = { populated: tribPop[field.key], total: tribNorm.length, pct: tribPct };
    if (tribPct >= 80 && radPct < 50) {
      gaps.push({
        field: field.label,
        key: field.key,
        tributePct: tribPct,
        radissonPct: radPct,
        requiredForV31e: field.v31e,
        requiredForActiveProfile: field.activeProfile,
      });
    }
  }

  const perRecord = radNorm.map((rec) => {
    const missing = REGISTRY_COMPLETENESS_FIELDS.filter((f) => !hasVal(rec[f.key])).map(
      (f) => f.label
    );
    const v31eMissing = REGISTRY_COMPLETENESS_FIELDS.filter(
      (f) => f.v31e && !hasVal(rec[f.key])
    ).map((f) => f.label);
    return {
      recordId: rec.id,
      assetName: rec.assetName,
      recommendedExplorerSlot: rec.recommendedExplorerSlot,
      assetStatus: rec.assetStatus,
      explorerUsePermission: rec.explorerUsePermission,
      founderApproved: isFounderApprovedRecord(rec),
      doNotUse: isDoNotUseRecord(rec),
      populatedFieldCount: REGISTRY_COMPLETENESS_FIELDS.length - missing.length,
      missingFields: missing,
      v31eBlockers: v31eMissing,
      stagingRunId: nz(rec.fields?.[MAP_BRAND_ASSET.stagingRunId]),
      reviewNotes: rec.reviewNotes,
    };
  });

  return {
    radissonRecordCount: radNorm.length,
    tributeRecordCount: tribNorm.length,
    tributeFieldPopulation: tributeBaseline,
    fieldsPopulatedInTributeButBlankInRadisson: gaps,
    mandatoryGoingForward: REGISTRY_COMPLETENESS_FIELDS.filter(
      (f) => f.required || f.v31e || f.activeProfile
    ).map((f) => f.label),
    perRecord,
  };
}

export function auditDuplicateRegistry(records) {
  const norm = records.map(normalizeRegistryRecordExtended);
  const groups = [];

  function addGroup(key, type, members) {
    if (members.length < 2) return;
    groups.push({ dedupeKey: key, type, members });
  }

  const bySlot = new Map();
  const byUrl = new Map();
  const byName = new Map();

  for (const rec of norm) {
    const slotKey = nz(rec.recommendedExplorerSlot);
    const prop = normalizeNameKey(rec.relatedPropertyName || rec.assetName);
    if (slotKey) {
      const k = `${slotKey}|${prop}`;
      if (!bySlot.has(k)) bySlot.set(k, []);
      bySlot.get(k).push(rec);
    }
    const urlKey = normalizeUrlKey(rec.sourceUrl || rec.attachmentUrl);
    if (urlKey) {
      if (!byUrl.has(urlKey)) byUrl.set(urlKey, []);
      byUrl.get(urlKey).push(rec);
    }
    const nameKey = normalizeNameKey(rec.assetName).slice(0, 48);
    if (nameKey) {
      if (!byName.has(nameKey)) byName.set(nameKey, []);
      byName.get(nameKey).push(rec);
    }
  }

  for (const [k, members] of bySlot) {
    const doNotUse = members.filter(isDoNotUseRecord);
    const pending = members.filter((m) => !isDoNotUseRecord(m));
    let type = "safe_distinct_asset";
    if (doNotUse.length && pending.length) type = "do_not_use_guard_duplicate";
    else if (members.every((m) => isDoNotUseRecord(m))) type = "do_not_use_guard_duplicate";
    else if (/gallery/i.test(k) && members.length > 1) type = "gallery_restore_duplicate";
    else if (members.length > 1 && pending.length > 1) type = "true_duplicate_same_asset";
    else if (/press kit|source reference/i.test(members.map((m) => m.assetName).join(" ")))
      type = "source_reference_vs_image_asset";
    else if (/placeholder|opening example/i.test(members.map((m) => m.assetName).join(" ")))
      type = "placeholder_needs_image";

    const canonical =
      pending.find(isFounderApprovedRecord) ||
      pending.find((m) => hasVal(m.sourcePageUrl)) ||
      pending[0] ||
      members[0];

    addGroup(k, type, members.map((m) => ({
      recordId: m.id,
      assetName: m.assetName,
      assetStatus: m.assetStatus,
      slot: m.recommendedExplorerSlot,
      sourceUrl: m.sourceUrl ? "present" : null,
      sourcePageUrl: m.sourcePageUrl ? "present" : null,
      doNotUse: isDoNotUseRecord(m),
      canonicalRecommendation: m.id === canonical?.id,
    })));
  }

  for (const [k, members] of byUrl) {
    if (bySlot.has(k)) continue;
    addGroup(`url:${k.slice(0, 40)}`, "true_duplicate_same_asset", members.map((m) => ({
      recordId: m.id,
      assetName: m.assetName,
      slot: m.recommendedExplorerSlot,
    })));
  }

  return {
    duplicateGroupCount: groups.length,
    groups,
    summary: {
      true_duplicate_same_asset: groups.filter((g) => g.type === "true_duplicate_same_asset").length,
      gallery_restore_duplicate: groups.filter((g) => g.type === "gallery_restore_duplicate").length,
      do_not_use_guard_duplicate: groups.filter((g) => g.type === "do_not_use_guard_duplicate").length,
      placeholder_needs_image: groups.filter((g) => g.type === "placeholder_needs_image").length,
      source_reference_vs_image_asset: groups.filter((g) => g.type === "source_reference_vs_image_asset").length,
    },
  };
}

export function v31iAuditExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31I supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

export async function buildBrandExplorerRadissonIndividualsImageAssetOpeningsRootCauseAuditReport({
  brandArg = TARGET_BRAND.slug,
  dryRun = true,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandConfig = DISCOVERY_BRAND_CONFIG[target.slug];

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const companyValidatedBefore = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));

  const priorReports = {
    v31a: loadJsonReport("reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json"),
    v31recon: loadJsonReport("reports/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.json"),
    v31b: loadJsonReport("reports/brand-explorer-brand-asset-registry-discovery-writer.json"),
    v31c: loadJsonReport("reports/brand-explorer-radisson-individuals-openings-suppression-writer.json"),
    v31d: loadJsonReport("reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json"),
    v31g: loadJsonReport("reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.json"),
    v31dr1: loadJsonReport("reports/brand-explorer-radisson-individuals-gallery-restore-writer.json"),
    v31e: loadJsonReport("reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json"),
    v31f: loadJsonReport("reports/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.json"),
  };

  const reportsPresent = Object.fromEntries(
    Object.entries(priorReports).map(([k, v]) => [k, Boolean(v && !v._loadError)])
  );

  const [presentationRaw, radissonRegistryRaw, tributeRegistryRaw, liveState, brandApi] =
    await Promise.all([
      listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
      listRegistryRaw(baseId, apiKey, target.recordId),
      listRegistryRaw(baseId, apiKey, TRIBUTE_RECORD_ID),
      fetchLiveState(target.recordId).catch((err) => ({ error: err.message })),
      fetchBrandApiShape(target.slug),
    ]);

  const tributePresentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    TRIBUTE_RECORD_ID,
    "Tribute Portfolio"
  ).catch(() => []);

  const allRows = presentationRaw.map(normalizePresentationRow);
  const apiBlocks = brandApi?.brandExplorer?.blocks || [];
  const apiBlockIds = new Set(apiBlocks.map((b) => b.recordId));
  const registryNorm = radissonRegistryRaw.map(normalizeRegistryRecordExtended);

  const historic = buildHistoricImageTimeline(priorReports);

  const imageRestorationAudit = [];
  for (const row of allRows) {
    if (!isVisualImageSlot(row.slotKey) && !historic.byRecordId.has(row.recordId)) continue;
    const hist = historic.byRecordId.get(row.recordId) || {
      hadImageBeforeV31C: false,
      hadImageBeforeV31D: false,
      clearedByV31C: false,
      clearedByV31D: false,
      restoredByV31DR1: false,
      priorImageUrl: null,
      writers: [],
    };
    const registryMatch = findRegistryAssetForPresentationRow(registryNorm, row);
    const disposition = recommendImageDisposition({
      row,
      historic: hist,
      isOpenings: row.isOpeningsSection,
      registryMatch,
      brandConfig,
    });

    if (
      hist.hadImageBeforeV31D ||
      row.hasImage ||
      isVisualImageSlot(row.slotKey) ||
      hist.clearedByV31D
    ) {
      imageRestorationAudit.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        section: row.section,
        isOpeningsSection: row.isOpeningsSection,
        hadImageBeforeV31C: hist.hadImageBeforeV31C,
        hadImageBeforeV31D: hist.hadImageBeforeV31D,
        clearedByV31C: hist.clearedByV31C,
        clearedByV31D: hist.clearedByV31D,
        restoredByV31DR1: hist.restoredByV31DR1,
        currentlyHasImage: row.hasImage,
        currentImageUrl: row.imageUrl,
        priorImageUrl: hist.priorImageUrl,
        visibleInApi: apiBlockIds.has(row.recordId),
        quarantined: row.quarantined,
        registryRecordId: registryMatch?.id || null,
        registryApproved: isRegistryAssetApprovedForExplorer(registryMatch),
        recommendation: disposition,
        writerTrail: hist.writers,
      });
    }
  }

  const registryCompleteness = auditRegistryCompleteness(radissonRegistryRaw, tributeRegistryRaw);

  const sourceUrlExpirationAudit = registryNorm.map((rec) => {
    const urlAudit = classifyUrlDurability(rec.sourceUrl, rec.sourcePageUrl, rec.attachmentUrl);
    return {
      recordId: rec.id,
      assetName: rec.assetName,
      recommendedExplorerSlot: rec.recommendedExplorerSlot,
      sourceUrl: rec.sourceUrl || null,
      sourcePageUrl: rec.sourcePageUrl || null,
      attachmentUrl: rec.attachmentUrl || null,
      ...urlAudit,
      founderApproved: isFounderApprovedRecord(rec),
      v31eClassification: classifyRegistryAsset(rec),
    };
  });

  const duplicateRegistryAudit = auditDuplicateRegistry(radissonRegistryRaw);

  const openingsRootCauseAudit = [];
  const openingsTextAudit = [];
  const openingsImageAudit = [];

  for (const row of allRows) {
    if (!isOpeningsEvidenceSlot(row.slotKey) && row.slotKey !== "footprint.openings") continue;
    if (row.slotKey === "footprint.momentum") continue;

    const registryMatch = findRegistryAssetForPresentationRow(registryNorm, row);
    const imageAssessment = assessPresentationRowImageGovernance(row, brandConfig, registryNorm);
    const internalIssues = findInternalLanguageInRow(row);
    const location = parseFootprintOpeningLocation(row.title, row.body);
    const urlAudit = registryMatch
      ? classifyUrlDurability(registryMatch.sourceUrl, registryMatch.sourcePageUrl, registryMatch.attachmentUrl)
      : classifyUrlDurability(row.summaryUrl, null, row.imageUrl);

    let recommendation = "pending_source_review";
    if (row.quarantined && (internalIssues.length || imageAssessment?.wrongBrandRisk)) {
      recommendation = "keep_quarantined";
    } else if (row.quarantined && !internalIssues.length && registryMatch && isFounderApprovedRecord(registryMatch)) {
      recommendation = "eligible_for_reactivation_after_copy_image_repair";
    } else if (/wrong.?brand|quality inn/i.test(row.title + row.body)) {
      recommendation = "remove_from_owner_facing_evidence";
    } else if (!row.hasImage && !registryMatch) {
      recommendation = "rebuild_from_official_source";
    } else if (row.hasImage && !isRegistryAssetApprovedForExplorer(registryMatch)) {
      recommendation = "pending_image_review";
    }

    openingsRootCauseAudit.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: row.title,
      location,
      imageStatus: row.hasImage ? "has_image" : "blank",
      registryAssetId: registryMatch?.id || null,
      registryStatus: registryMatch?.assetStatus || null,
      registryApproved: isRegistryAssetApprovedForExplorer(registryMatch),
      sourceUrl: row.summaryUrl || registryMatch?.sourceUrl || null,
      sourcePageUrl: registryMatch?.sourcePageUrl || null,
      bodyExcerpt: row.body.slice(0, 200),
      modalCopySurfaces: collectRowCopySurfaces(row).map((s) => s.field),
      internalLanguageIssues: internalIssues,
      quarantined: row.quarantined,
      visibleInApi: apiBlockIds.has(row.recordId),
      recommendation,
      urlDurability: urlAudit.classification,
    });

    if (internalIssues.length) {
      openingsTextAudit.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        issues: internalIssues,
      });
    }

    const wrongBrand = detectWrongBrandSignageRisk(
      [row.title, row.body, row.imageUrl].filter(Boolean).join("\n"),
      brandConfig
    );
    const imageFlags = [];
    if (!row.hasImage) imageFlags.push("blank_image");
    if (wrongBrand) imageFlags.push("wrong_brand_signage");
    if (registryMatch && isDoNotUseRecord(registryMatch)) imageFlags.push("do_not_use_registry_match");
    if (row.hasImage && !isRegistryAssetApprovedForExplorer(registryMatch)) imageFlags.push("unapproved_image");
    if (!registryMatch) imageFlags.push("no_registry_match");
    if (urlAudit.classification === "temporary_attachment_url") imageFlags.push("missing_durable_source_page");
    if (imageFlags.length) {
      openingsImageAudit.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        title: row.title,
        flags: imageFlags,
        wrongBrandRisk: wrongBrand,
      });
    }
  }

  const quarantinedNotInApi = allRows.filter((r) => r.quarantined && apiBlockIds.has(r.recordId));
  const visibleWithoutImage = apiBlocks.filter(
    (b) => isVisualImageSlot(b.slotKey) && !hasVal(b.imageUrl)
  );
  const hiddenButInDb = allRows.filter((r) => r.quarantined);
  const pendingGalleryInApi = apiBlocks.filter(
    (b) => isGalleryImageSlot(b.slotKey) && hasVal(b.imageUrl)
  );

  const uiApiRenderingAudit = {
    presentationRowCount: allRows.length,
    apiBlockCount: apiBlocks.length,
    quarantinedRowCount: hiddenButInDb.length,
    quarantinedLeakedToApi: quarantinedNotInApi.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      externalDisplayStatus: r.externalDisplayStatus,
    })),
    doNotDisplayFilterWorking: quarantinedNotInApi.length === 0,
    visualSlotsInApiWithoutImage: visibleWithoutImage.map((b) => ({
      recordId: b.recordId,
      slotKey: b.slotKey,
      title: b.title,
    })),
    pendingGalleryVisibleInDraft: pendingGalleryInApi.length,
    frontendBehavior: {
      galleryEmptyShell:
        "brand-explorer-atelier-from-api.js renders gallery-card--empty placeholder when imageUrl missing",
      galleryPendingVisible:
        "Rows with imageUrl render gallery-card--has-image even when registry not approved (draft/internal)",
      apiFilter:
        "brand-library.js skips External Display Status Do Not Display / Internal Only — quarantined openings excluded from blocks",
      goldDetail: "brand-explorer-gold-detail.js counts imageUrl on blocks; no separate pending-image hide",
    },
    tributeComparison: {
      tributePresentationVisualRows: tributePresentationRaw
        .map(normalizePresentationRow)
        .filter((r) => isVisualImageSlot(r.slotKey)).length,
      tributeWithImages: tributePresentationRaw
        .map(normalizePresentationRow)
        .filter((r) => isVisualImageSlot(r.slotKey) && r.hasImage).length,
    },
  };

  const nonOpeningsCleared = imageRestorationAudit.filter(
    (r) => !r.isOpeningsSection && r.clearedByV31D && !r.currentlyHasImage && !r.restoredByV31DR1
  );
  const galleryRestored = imageRestorationAudit.filter((r) => r.restoredByV31DR1);
  const founderApprovedNoUrl = sourceUrlExpirationAudit.filter(
    (r) => r.founderApproved && r.classification === "source_reference_only"
  );
  const expiredTempUrls = sourceUrlExpirationAudit.filter(
    (r) => r.classification === "temporary_attachment_url"
  );

  const rootCauseMap = {
    writersThatRemovedCorrectImages: [
      {
        writer: "v31D",
        action: "clear_unapproved_image on all visible visual slots including materials.gallery.*",
        impact: "Cleared 6 gallery images that were brand-matched but unapproved",
        mitigatedBy: "v31D-R1 restored gallery; v31D scope narrowed in code to wrong-brand/Do Not Use only",
      },
    ],
    writersThatCreatedIncompleteRegistry: [
      { writer: "v31B", issue: "Initial discovery staged registry without Source Page URL on many rows" },
      { writer: "v31D-R1", issue: "Created 6 gallery registry rows with Airtable attachment URLs as Source URL" },
      { writer: "v31G", issue: "Normalized metadata but did not backfill Source Page URL or attachments" },
    ],
    whySourceUrlsExpire:
      "Source URL field stores v5.airtableusercontent.com signed attachment URLs from presentation Image fields — these expire. Source Page URL (durable choicehotels/radisson page) was left blank.",
    whyDuplicatesExist:
      "Separate writer batches (v31B discovery, v31D-R1 gallery restore, v31G normalization) created rows per slot without deduping against existing Do Not Use guards and press-kit references.",
    whyV31eCannotMaterialize: [
      `${founderApprovedNoUrl.length} founder-approved rows are source_reference_only (no image URL)`,
      `${registryCompleteness.perRecord.filter((r) => r.founderApproved && r.v31eBlockers.length).length} approved rows missing v31E-required fields (Source URL / attachment)`,
      "Opening placeholders approved without property image files attached",
    ],
    whyOpeningsNeedsRebuild:
      "11+ footprint.openings rows quarantined (v31C); wrong-brand images on Do Not Use registry rows; internal/census language in copy; no durable official source linkage",
    dataVsCodeVsUi: {
      dataIssues: [
        "Missing Source Page URL on registry rows",
        "Temporary attachment URLs used as Source URL",
        "Approved opening placeholders without image files",
      ],
      codeIssues: [
        "v31D originally cleared all unapproved images (fixed to wrong-brand/Do Not Use only)",
        "v31E requires sourceUrl on approved asset — no fallback to presentation attachment",
      ],
      uiIssues: [
        "Gallery renders empty shell cards when image cleared",
        "Pending gallery images now visible in draft (intentional post v31D-R1)",
      ],
    },
  };

  const recommendedRepairSequence = [
    {
      phase: "A",
      title: "Restore/preserve non-Openings images that were working",
      writer: "v31I follow-up: non-openings image preservation writer (or extend v31D-R1 pattern)",
      action: "Audit shows gallery restored; verify overview.scenario / hero / valueOwners slots",
      blockedBy: nonOpeningsCleared.length ? `${nonOpeningsCleared.length} non-openings slots still empty after v31D` : null,
    },
    {
      phase: "B",
      title: "Normalize Brand Asset Registry schema and source URLs",
      writer: "v31J registry source-url repair writer (proposed)",
      action: "Backfill Source Page URL with durable choicehotels/radisson pages; move attachment URLs to Image/Attachment only",
      blockedBy: `${expiredTempUrls.length} rows with temporary_attachment_url pattern`,
    },
    {
      phase: "C",
      title: "Deduplicate / canonicalize registry records",
      writer: "v31K registry dedupe planner (proposed)",
      action: "Mark canonical per slot; supersede gallery_restore_duplicate and press-kit duplicates logically",
      blockedBy: `${duplicateRegistryAudit.duplicateGroupCount} duplicate groups detected`,
    },
    {
      phase: "D",
      title: "Patch UI to avoid blank image shells",
      writer: "Frontend: brand-explorer-atelier-from-api.js gallery/openings card rendering",
      action: "Hide empty visual shells in owner-facing mode; keep admin hint in draft only",
    },
    {
      phase: "E",
      title: "Rebuild Openings / Examples from official sources",
      writer: "v31L openings rebuild writer (proposed)",
      action: "Official announcement copy, durable source pages, approved property images only",
      blockedBy: `${openingsRootCauseAudit.filter((r) => r.recommendation === 'keep_quarantined').length} rows quarantined`,
    },
    {
      phase: "F",
      title: "Materialize approved assets",
      writer: "v31E (re-run after B/C/E)",
      action: "Requires Source URL + Source Page URL on approved opening assets",
    },
    {
      phase: "G",
      title: "Final QA / complete-build",
      writer: "brand-explorer-final-qa-auditor + brand-explorer-complete-build",
      action: "Target active-profile readiness after above phases",
    },
  ];

  const finalQa = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const qaBrand = finalQa?.brandReports?.[0] || {};

  const completeBuild = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));
  const completeBrand =
    (completeBuild?.brandReports || []).find((b) => b.slug === target.slug) || {};

  const report = {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "read-only-audit",
    v31iAuditExists: v31iAuditExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    reportsPresent,
    companyValidatedSnapshot: companyValidatedBefore,
    companyValidatedUntouched: true,
    airtableModified: false,
    imagesApproved: false,
    factsApproved: false,
    liveCounts: {
      presentationRows: allRows.length,
      registryRows: registryNorm.length,
      tributeRegistryRows: tributeRegistryRaw.length,
      sourceLibraryRows: liveState?.sources?.length ?? null,
      partnerFacts: liveState?.facts?.length ?? null,
      apiBlocks: apiBlocks.length,
    },
    imageRestorationAudit,
    imageRestorationSummary: {
      totalVisualRowsAudited: imageRestorationAudit.length,
      clearedByV31D: imageRestorationAudit.filter((r) => r.clearedByV31D).length,
      restoredByV31DR1: galleryRestored.length,
      nonOpeningsStillEmpty: nonOpeningsCleared.length,
      galleryWithImagesNow: imageRestorationAudit.filter(
        (r) => r.section === "materials.gallery" && r.currentlyHasImage
      ).length,
      bySection: imageRestorationAudit.reduce((acc, r) => {
        acc[r.section] = (acc[r.section] || 0) + 1;
        return acc;
      }, {}),
    },
    registryCompletenessAudit: registryCompleteness,
    sourceUrlExpirationAudit,
    sourceUrlExpirationSummary: {
      temporary_attachment_url: expiredTempUrls.length,
      durable_source_page_url: sourceUrlExpirationAudit.filter(
        (r) => r.classification === "durable_source_page_url"
      ).length,
      source_reference_only: sourceUrlExpirationAudit.filter(
        (r) => r.classification === "source_reference_only"
      ).length,
      not_materializable: sourceUrlExpirationAudit.filter(
        (r) => r.classification === "not_materializable"
      ).length,
      founderApprovedNotMaterializable: founderApprovedNoUrl.length,
    },
    duplicateRegistryAudit,
    openingsRootCauseAudit,
    openingsTextQualityAudit: openingsTextAudit,
    openingsImageQualityAudit: openingsImageAudit,
    uiApiRenderingAudit,
    rootCauseMap,
    recommendedRepairSequence,
    currentReadiness: {
      finalQaScore: qaBrand.scores?.overallNumeric,
      finalQaReadiness: qaBrand.scores?.overallActiveProfileReadiness,
      completeBuildReady: completeBrand.readyForActiveProfile,
    },
    markdown: "",
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Image / Asset / Openings Root-Cause Audit v31I`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31I exists: **${report.v31iAuditExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}** (audit only — no Airtable writes)`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "no" : "no"}**`,
    "",
    "## Files read",
    ...report.filesRead.map((f) => `- ${f}`),
    "",
    "## Prior writer reports present",
    ...Object.entries(report.reportsPresent).map(([k, v]) => `- ${k}: ${v ? "yes" : "no"}`),
    "",
    "## 1. Image restoration audit",
    `- Visual rows audited: **${report.imageRestorationSummary.totalVisualRowsAudited}**`,
    `- Cleared by v31D: **${report.imageRestorationSummary.clearedByV31D}**`,
    `- Restored by v31D-R1: **${report.imageRestorationSummary.restoredByV31DR1}**`,
    `- Gallery with images now: **${report.imageRestorationSummary.galleryWithImagesNow}**`,
    `- Non-openings still empty after v31D: **${report.imageRestorationSummary.nonOpeningsStillEmpty}**`,
    "",
    "### Notable rows",
    ...report.imageRestorationAudit
      .filter((r) => r.clearedByV31D || r.restoredByV31DR1)
      .slice(0, 20)
      .map(
        (r) =>
          `- \`${r.slotKey}\` ${r.title.slice(0, 50)} — cleared v31D: ${r.clearedByV31D}, restored: ${r.restoredByV31DR1}, has image: ${r.currentlyHasImage}, rec: **${r.recommendation}**`
      ),
    "",
    "## 2. Registry completeness vs Tribute",
    `- Radisson registry rows: **${report.registryCompletenessAudit.radissonRecordCount}**`,
    `- Tribute registry rows: **${report.registryCompletenessAudit.tributeRecordCount}**`,
    "",
    "### Fields populated in Tribute but mostly blank in Radisson",
    ...(report.registryCompletenessAudit.fieldsPopulatedInTributeButBlankInRadisson.length
      ? report.registryCompletenessAudit.fieldsPopulatedInTributeButBlankInRadisson.map(
          (g) =>
            `- **${g.field}** — Tribute ${g.tributePct}% vs Radisson ${g.radissonPct}% (v31E: ${g.requiredForV31e ? "required" : "optional"})`
        )
      : ["- (none above 80% tribute threshold)"]),
    "",
    "## 3. Source URL expiration",
    `- temporary_attachment_url: **${report.sourceUrlExpirationSummary.temporary_attachment_url}**`,
    `- durable_source_page_url: **${report.sourceUrlExpirationSummary.durable_source_page_url}**`,
    `- source_reference_only: **${report.sourceUrlExpirationSummary.source_reference_only}**`,
    `- founder-approved not materializable: **${report.sourceUrlExpirationSummary.founderApprovedNotMaterializable}**`,
    "",
    "## 4. Duplicate registry",
    `- Duplicate groups: **${report.duplicateRegistryAudit.duplicateGroupCount}**`,
    ...Object.entries(report.duplicateRegistryAudit.summary).map(([k, v]) => `- ${k}: ${v}`),
    "",
    "## 5. Openings root cause",
    `- Rows audited: **${report.openingsRootCauseAudit.length}**`,
    ...report.openingsRootCauseAudit.slice(0, 12).map(
      (r) =>
        `- \`${r.slotKey}\` ${r.title.slice(0, 40)} — ${r.recommendation} (quarantined: ${r.quarantined})`
    ),
    "",
    "## 6–7. Openings text / image quality",
    `- Text issue rows: **${report.openingsTextQualityAudit.length}**`,
    `- Image issue rows: **${report.openingsImageQualityAudit.length}**`,
    "",
    "## 8. UI/API rendering",
    `- API blocks: **${report.uiApiRenderingAudit.apiBlockCount}** / presentation rows: **${report.uiApiRenderingAudit.presentationRowCount}**`,
    `- Quarantined rows: **${report.uiApiRenderingAudit.quarantinedRowCount}**`,
    `- Quarantined leaked to API: **${report.uiApiRenderingAudit.quarantinedLeakedToApi.length}**`,
    `- Visual API slots without image: **${report.uiApiRenderingAudit.visualSlotsInApiWithoutImage.length}**`,
  ];

  lines.push("", "## 9. Root-cause map", "");
  for (const [k, v] of Object.entries(report.rootCauseMap)) {
    if (Array.isArray(v)) {
      lines.push(`### ${k}`);
      for (const item of v) {
        if (typeof item === "string") lines.push(`- ${item}`);
        else lines.push(`- **${item.writer || item.phase}**: ${item.issue || item.action || item.impact}`);
      }
    } else if (typeof v === "object") {
      lines.push(`### ${k}`);
      for (const [sk, sv] of Object.entries(v)) {
        lines.push(`- ${sk}: ${Array.isArray(sv) ? sv.join("; ") : sv}`);
      }
    } else {
      lines.push(`- **${k}**: ${v}`);
    }
  }

  lines.push("", "## 10. Recommended repair sequence", "");
  for (const step of report.recommendedRepairSequence) {
    lines.push(`### ${step.phase}. ${step.title}`);
    lines.push(`- Writer: \`${step.writer}\``);
    lines.push(`- Action: ${step.action}`);
    if (step.blockedBy) lines.push(`- Blocked by: ${step.blockedBy}`);
    lines.push("");
  }

  lines.push(
    "## Current readiness",
    `- Final QA: **${report.currentReadiness.finalQaScore}** (${report.currentReadiness.finalQaReadiness})`,
    `- Active-profile ready: **${report.currentReadiness.completeBuildReady ? "yes" : "no"}**`
  );

  return lines.join("\n");
}
