/**
 * Brand Explorer Everhome Openings Description Cleanup + Momentum Tribute-Parity v32E.
 *
 * Everhome-only writer:
 * - Rewrites weak footprint.openings teaser descriptions (preserve chips/images/structure).
 * - Rewrites footprint.momentum summaries and corrects source URL only when momentum evidence is weak.
 * - Reports image/registry governance only (no image/approval/visibility writes).
 *
 * @see docs/data-intelligence/brand-explorer-everhome-openings-momentum-rebuild-writer-v32E.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { isRegistryAssetApprovedForExplorer } from "./brand-explorer-brand-asset-image-governance.js";
import {
  buildMomentumBody,
  classifyMomentumSourceType,
  followsTributeMomentumRules,
  isMomentumInappropriatePropertyListing,
  momentumEvidenceSourceRank,
  momentumLinkLabelForUrl,
  parseMomentumPresentationBody,
} from "./brand-explorer-momentum-link-label.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";

export const WRITER_VERSION = "v32E";
export const REPORT_JSON_NAME = "brand-explorer-everhome-openings-momentum-rebuild-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-openings-momentum-rebuild-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-openings-momentum-rebuild-writer-v32E.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32E-everhome-openings-momentum-rebuild";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_APPROVALS = "--confirm-no-image-or-registry-approval-changes";
export const APPLY_FLAG_NO_VISIBILITY = "--confirm-no-visibility-changes";
export const APPLY_FLAG_PRESERVE_LABELS = "--confirm-preserve-existing-opening-labels";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const TARGET_BRAND = Object.freeze({
  slug: "everhome-suites",
  recordId: "recqkkrsevi4r9ibj",
  name: "Everhome Suites",
});

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const BLOCKED_PATCH_FIELDS = new Set([
  "Image",
  "Images",
  "Attachments",
  "Scenario Image",
  "External Display Status",
  "Company Validated",
  "Company Validation Date",
]);

const METADATA_STYLE_RE =
  /featured on choice hotels'? development site|active property page|consumer path|source data|metadata|census|source[- ]?capture|project and training assignments|booking path/i;
const BLOCKED_OWNER_FACING_RE =
  /\bfdd\b|\bitem\s*19\b|franchise disclosure|confirm fees|confirm flag|performance representation|internal|extraction/i;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-presentation-cleanup-writer.json",
  "reports/brand-explorer-everhome-source-registry-normalization-writer.json",
  "reports/brand-explorer-choice-extended-stay-source-capture-writer.json",
  "reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome presentation / source / registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-openings-momentum-rebuild-writer.js",
  "scripts/brand-explorer-everhome-openings-momentum-rebuild-writer.mjs",
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

export function v32eWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-everhome-openings-momentum-rebuild-writer.js")
  );
}

function demandDriverPhrase(text) {
  const t = nz(text).toLowerCase();
  const drivers = [];
  if (/\bcorporate\b/.test(t)) drivers.push("corporate");
  if (/\brelocation\b/.test(t)) drivers.push("relocation");
  if (/\bproject\b/.test(t)) drivers.push("project-based");
  if (/\bhealthcare\b|\bhospital\b/.test(t)) drivers.push("healthcare");
  if (!drivers.length) return "longer-stay demand";
  return `${drivers.join(", ")} demand`;
}

function parseParagraphs(body) {
  return nz(body)
    .split(/\n\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
}

function extractUrlFromText(text) {
  const m = nz(text).match(/https?:\/\/[^\s<>"')]+/i);
  return m ? m[0].replace(/[.,;)]+$/, "") : "";
}

function normalizeLocation(title, body) {
  const titleText = nz(title);
  const m = titleText.match(/\b(in|at)\s+([A-Z][A-Za-z .,'-]+)$/);
  if (m) return m[2].trim();
  const bm = nz(body).match(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\s+(metro|area|market|submarket))\b/);
  if (bm) return bm[1];
  return "the local market";
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
      summaryUrl: nz(f["Summary URL"]),
      sortOrder: f["Sort Order"],
      active: f.Active,
      externalDisplayStatus: nz(f["External Display Status"]),
      imageUrl: firstAttachmentUrl(f),
      hasImage: Boolean(firstAttachmentUrl(f)),
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

function openingChipsAndDescription(body) {
  const paras = parseParagraphs(body);
  const chips = paras[0] || "";
  const url = paras.find((p) => /^https?:\/\//i.test(p)) || "";
  const desc = paras
    .slice(1)
    .filter((p) => p !== url)
    .join("\n\n");
  return { chips, description: desc, sourceUrl: url };
}

function rewriteOpeningDescription(row) {
  const { chips, description, sourceUrl } = openingChipsAndDescription(row.body);
  const ownerFacing = !METADATA_STYLE_RE.test(description) && !BLOCKED_OWNER_FACING_RE.test(description);
  if (ownerFacing && description.length > 80) return null;

  const location = normalizeLocation(row.title, `${chips}\n${description}`);
  const driver = demandDriverPhrase(`${row.title}\n${description}`);
  const rewritten = `A ${location} extended-stay example that illustrates Everhome's apartment-style prototype in a suburban market context, useful for owners evaluating new-construction positioning for ${driver}.`;
  const rebuiltBody = [chips, rewritten, sourceUrl].filter(Boolean).join("\n\n");

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    before: row.body,
    after: rebuiltBody,
    ownerFacingBefore: ownerFacing,
    ownerFacingAfter: true,
    lookedLikeMetadata: METADATA_STYLE_RE.test(description),
    awkwardLanguage: BLOCKED_OWNER_FACING_RE.test(description),
    preservedChips: chips,
    preservedSourceUrl: sourceUrl,
  };
}

function pickBestMomentumSource(currentUrl, approvedSources) {
  const currentOk = followsTributeMomentumRules(currentUrl).ok;
  if (currentOk) return { url: currentUrl, changed: false, reason: "already_event_supporting" };
  const candidates = approvedSources
    .map((s) => nz(s.sourceUrl))
    .filter(Boolean)
    .filter((u) => !isTemporaryAirtableUrl(u))
    .filter((u) => !isMomentumInappropriatePropertyListing(u))
    .map((u) => ({ url: u, rank: momentumEvidenceSourceRank(u) }))
    .filter((c) => c.rank >= 60)
    .sort((a, b) => b.rank - a.rank);
  if (!candidates.length) return { url: currentUrl, changed: false, reason: "no_better_momentum_source" };
  return { url: candidates[0].url, changed: candidates[0].url !== currentUrl, reason: "replace_weak_evidence_type" };
}

function rewriteMomentumRow(row, approvedSources) {
  const parsed = parseMomentumPresentationBody(row.body, row.title);
  const currentUrl = parsed.sourceUrl || row.summaryUrl;
  const sourcePick = pickBestMomentumSource(currentUrl, approvedSources);
  const baseSummary = nz(parsed.description);
  const generic = baseSummary.length < 80 || /featured on|development site|source|metadata|internal/i.test(baseSummary);
  const location = normalizeLocation(row.title, baseSummary);
  const summary = generic
    ? `Everhome's recent development milestone in ${location} reinforces the brand's apartment-style extended-stay positioning for owners evaluating new-construction and conversion pathways in suburban and corridor markets.`
    : baseSummary;
  const sourceUrl = sourcePick.url;
  const rebuiltBody = buildMomentumBody({
    dateLine: parsed.dateLine || "Recent",
    summary,
    sourceUrl,
  });
  const changed = rebuiltBody !== row.body;
  if (!changed && !sourcePick.changed) return null;
  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    before: row.body,
    after: rebuiltBody,
    sourceBefore: currentUrl,
    sourceAfter: sourceUrl,
    sourceChangeReason: sourcePick.reason,
    linkLabelAfter: momentumLinkLabelForUrl(sourceUrl, { name: TARGET_BRAND.name }),
    classificationAfter: classifyMomentumSourceType(sourceUrl),
  };
}

function validatePatchFields(fields) {
  const errs = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PATCH_FIELDS.has(key)) errs.push(`blocked_field:${key}`);
  }
  const txt = `${fields.Title || ""}\n${fields.Body || ""}`;
  if (scanCopySafety(txt).length) errs.push("copy_safety_fail");
  if (BLOCKED_OWNER_FACING_RE.test(txt)) errs.push("blocked_owner_facing_language");
  return errs;
}

function buildPatchFields(row, updatedBody) {
  return {
    "Slot Key": row.slotKey,
    Title: row.title,
    Body: updatedBody,
    "Sort Order": row.sortOrder ?? 0,
    Active: row.active !== false,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-openings-momentum-rebuild-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_APPROVALS,
    APPLY_FLAG_NO_VISIBILITY,
    APPLY_FLAG_PRESERVE_LABELS,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

export async function buildBrandExplorerEverhomeOpeningsMomentumRebuildWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFields = false,
  noApprovals = false,
  noVisibility = false,
  preserveLabels = false,
  everhomeOnly = false,
} = {}) {
  if (nz(brandArg).toLowerCase() !== TARGET_BRAND.slug) {
    throw new Error(`v32E is Everhome-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

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
  const sourceRows = await fetchAllBrandSources(TARGET_BRAND.recordId);
  const registryRows = await listRegistryAssetsForBrand(TARGET_BRAND.recordId).catch(() => []);

  const openingsRows = presentationRows.filter((r) => r.slotKey === "footprint.openings");
  const momentumRows = presentationRows.filter((r) => r.slotKey === "footprint.momentum");
  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const visibleIds = new Set(apiBlocks.map((b) => b.recordId));
  const apiById = new Map(apiBlocks.map((b) => [b.recordId, b]));

  const approvedSourceRows = sourceRows.filter((s) => nz(s.approvedForExplorerUse) === "Yes");
  const openingsAudit = openingsRows.map((row) => {
    const parsed = openingChipsAndDescription(row.body);
    const apiBlock = apiById.get(row.recordId);
    const registryLinks = registryRows.filter((r) => nz(r.sourceNotes).includes(row.recordId));
    const ownerFacing = !METADATA_STYLE_RE.test(parsed.description) && !BLOCKED_OWNER_FACING_RE.test(parsed.description);
    return {
      recordId: row.recordId,
      slotKey: row.slotKey,
      propertyTitle: row.title,
      location: normalizeLocation(row.title, parsed.description),
      labelsChips: parsed.chips,
      descriptionTeaser: parsed.description,
      imageStatus: row.hasImage ? "has_attachment" : "no_attachment",
      imageLoading: Boolean(apiBlock?.imageUrl),
      sourceUrl: parsed.sourceUrl || row.summaryUrl || null,
      propertySpecificSource: /choicehotels\.com\/[A-Z]{2,}\d+/i.test(parsed.sourceUrl || row.summaryUrl),
      visibleInApi: visibleIds.has(row.recordId),
      ownerFacingDescription: ownerFacing,
      soundsLikeSourceMetadata: METADATA_STYLE_RE.test(parsed.description),
      awkwardUnsupportedLanguage: BLOCKED_OWNER_FACING_RE.test(parsed.description),
      registryLinkage: registryLinks.map((r) => r.id),
    };
  });

  const momentumAudit = momentumRows.map((row) => {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    const sourceUrl = parsed.sourceUrl || row.summaryUrl;
    const rules = followsTributeMomentumRules(sourceUrl);
    return {
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: row.title,
      dateLine: parsed.dateLine,
      summary: parsed.description,
      sourceUrl,
      sourceType: classifyMomentumSourceType(sourceUrl),
      evidenceRank: momentumEvidenceSourceRank(sourceUrl),
      tributeParity: rules.ok,
      parityReason: rules.reason,
      linkLabel: momentumLinkLabelForUrl(sourceUrl, { name: TARGET_BRAND.name }),
      propertyListingUsed: isMomentumInappropriatePropertyListing(sourceUrl),
    };
  });

  const openingsChanges = [];
  const momentumChanges = [];
  const rowsLeftUnchanged = [];

  for (const row of openingsRows) {
    const change = rewriteOpeningDescription(row);
    if (!change) {
      rowsLeftUnchanged.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: "description_already_owner_facing",
      });
      continue;
    }
    const fields = buildPatchFields(row, change.after);
    const validationErrors = validatePatchFields(fields);
    if (validationErrors.length) {
      rowsLeftUnchanged.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: validationErrors.join("; "),
      });
      continue;
    }
    openingsChanges.push({ ...change, fields });
  }

  for (const row of momentumRows) {
    const change = rewriteMomentumRow(row, approvedSourceRows);
    if (!change) {
      rowsLeftUnchanged.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: "momentum_copy_and_source_already_parity",
      });
      continue;
    }
    const fields = buildPatchFields(row, change.after);
    if (change.sourceAfter && change.sourceAfter !== row.summaryUrl) {
      fields["Summary URL"] = change.sourceAfter;
    }
    const validationErrors = validatePatchFields(fields);
    if (validationErrors.length) {
      rowsLeftUnchanged.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: validationErrors.join("; "),
      });
      continue;
    }
    momentumChanges.push({ ...change, fields });
  }

  const proposedUpdates = [...openingsChanges, ...momentumChanges];

  const imageGovernanceReport = openingsRows.map((row) => {
    const apiBlock = apiById.get(row.recordId);
    const registryLinks = registryRows.filter((r) => nz(r.sourceNotes).includes(row.recordId));
    return {
      recordId: row.recordId,
      slotKey: row.slotKey,
      imageLoading: Boolean(apiBlock?.imageUrl),
      imageFieldPresent: row.hasImage,
      tempImageUrl: isTemporaryAirtableUrl(row.imageUrl),
      registryLinked: registryLinks.length > 0,
      registryApproved: registryLinks.some((r) => isRegistryAssetApprovedForExplorer(r)),
      pendingImageReview: registryLinks.some((r) => nz(r.usageReviewStatus) === "Pending Review"),
      v32fRecommendation: registryLinks.length ? "recognition_review" : "linkage_review",
    };
  });

  const applyBlockers = [];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFields) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noApprovals) applyBlockers.push("missing_confirm_no_image_or_registry_approval_changes");
    if (!noVisibility) applyBlockers.push("missing_confirm_no_visibility_changes");
    if (!preserveLabels) applyBlockers.push("missing_confirm_preserve_existing_opening_labels");
    if (!everhomeOnly) applyBlockers.push("missing_confirm_everhome_only");
  }
  for (const u of proposedUpdates) {
    if (u.slotKey === "footprint.openings") {
      const beforeChips = openingChipsAndDescription(u.before).chips;
      const afterChips = openingChipsAndDescription(u.after).chips;
      if (beforeChips !== afterChips) applyBlockers.push(`opening_labels_changed:${u.recordId}`);
    }
    if (BLOCKED_OWNER_FACING_RE.test(`${u.after}`)) {
      applyBlockers.push(`blocked_owner_language:${u.recordId}`);
    }
  }

  const dryRunClean = applyBlockers.length === 0 && proposedUpdates.length > 0;

  let airtableModified = false;
  let imageFieldsChanged = false;
  let approvalFieldsChanged = false;
  let visibilityChanged = false;
  const applyResults = { updated: [], errors: [] };

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFields &&
    noApprovals &&
    noVisibility &&
    preserveLabels &&
    everhomeOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const update of proposedUpdates) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          {
            method: "PATCH",
            body: JSON.stringify({ fields: update.fields, typecast: true }),
          },
          update.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.updated.push({ recordId: update.recordId, slotKey: update.slotKey });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: update.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    v32eWriterExists: v32eWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    openingsAudit,
    openingsDescriptionsBeforeAfter: openingsChanges.map((o) => ({
      recordId: o.recordId,
      before: o.before,
      after: o.after,
      preservedChips: o.preservedChips,
      preservedSourceUrl: o.preservedSourceUrl,
    })),
    labelsChipsPreserved: openingsChanges.every((o) => {
      const b = openingChipsAndDescription(o.before).chips;
      const a = openingChipsAndDescription(o.after).chips;
      return b === a;
    }),
    momentumAudit,
    momentumBeforeAfter: momentumChanges.map((m) => ({
      recordId: m.recordId,
      before: m.before,
      after: m.after,
      sourceBefore: m.sourceBefore,
      sourceAfter: m.sourceAfter,
      sourceChangeReason: m.sourceChangeReason,
      linkLabelAfter: m.linkLabelAfter,
    })),
    imageGovernanceReport,
    rowsUpdated: proposedUpdates.map((u) => ({
      recordId: u.recordId,
      slotKey: u.slotKey,
    })),
    rowsLeftUnchanged,
    imageFieldsChanged,
    approvalFieldsChanged,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    openingVisibilityChanged: visibilityChanged,
    momentumVisibilityChanged: visibilityChanged,
    applyBlockers,
    dryRunClean,
    applyResults,
    expectedFinalQaImpact:
      "Should improve footprint.openings and footprint.momentum copy quality while preserving structure and image state.",
    expectedCompleteBuildImpact:
      "Copy clarity improves; active-profile still depends on pending image/approval governance and remaining defects.",
    recommendedNextWriter: "v32F — Everhome image governance recognition/materialization writer",
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-everhome-openings-momentum-rebuild-writer -- --brand everhome-suites --dry-run",
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Openings + Momentum v32E");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32E exists: **${report.v32eWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imageFieldsChanged ? "no" : "yes"}**`);
  lines.push("");
  lines.push("## Scope Results");
  lines.push(`- Openings rows audited: **${report.openingsAudit.length}**`);
  lines.push(`- Momentum rows audited: **${report.momentumAudit.length}**`);
  lines.push(`- Rows proposed for update: **${report.rowsUpdated.length}**`);
  lines.push(`- Opening labels/chips preserved: **${report.labelsChipsPreserved ? "yes" : "no"}**`);
  lines.push("");
  lines.push(`**Next writer:** ${report.recommendedNextWriter}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}
