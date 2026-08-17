/**
 * Brand Explorer Radisson Individuals Openings / Examples Suppression + UI Copy Quarantine v31C.
 *
 * Quarantines unsafe footprint.openings and related rows from active Brand Explorer UI
 * without deleting records, approving images, or materializing replacements.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-openings-suppression-writer-v31C.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import {
  DISCOVERY_BRAND_CONFIG,
  assessPresentationRowImageGovernance,
  findRegistryAssetForPresentationRow,
} from "./brand-explorer-brand-asset-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import {
  assessOpeningsRowQuarantine,
  detectOpeningsUiQuarantineDefects,
  isOpeningsEvidenceSlot,
  proposeOwnerFacingOpeningsCopy,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";

export const WRITER_VERSION = "31C-R1";

export const EXTERNAL_DISPLAY_STATUS_QUARANTINE = "Do Not Display";

export const HIDDEN_EXTERNAL_DISPLAY_STATUSES = Object.freeze([
  "Do Not Display",
  "Internal Only",
]);
export const REPORT_JSON_NAME = "brand-explorer-radisson-individuals-openings-suppression-writer.json";
export const REPORT_MD_NAME = "brand-explorer-radisson-individuals-openings-suppression-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-openings-suppression-writer-v31C.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31C-radisson-individuals-openings-suppression";
export const APPLY_FLAG_APPROVE_R1 =
  "--approve-brand-explorer-v31C-R1-display-status-suppression";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-openings-quarantine";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE =
  "--confirm-no-image-approval-or-materialization";

export const TARGET_BRAND = Object.freeze({
  slug: "radisson-individuals-by-choice",
  recordId: "recRyvM8OmLlDj9G7",
  name: "Radisson Individuals by Choice",
});

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
  "reports/brand-explorer-brand-asset-registry-discovery-writer.md",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.json",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.md",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Radisson Individuals Brand Asset Registry rows",
  "live Source Library records",
  "live Partner Facts",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-openings-suppression-writer.js",
  "lib/partner-intelligence/brand-explorer-openings-ui-quarantine-governance.js",
  "scripts/brand-explorer-radisson-individuals-openings-suppression-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
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

export function v31cWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-openings-suppression-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31C`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31C supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
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

export function isPresentationRowVisibleInExplorer(fields = {}) {
  const ext = nz(fields["External Display Status"]);
  if (HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(ext)) return false;
  const activeRaw = fields.Active;
  const inactive =
    activeRaw === false ||
    String(activeRaw).toLowerCase() === "no" ||
    String(activeRaw).toLowerCase() === "false" ||
    activeRaw === 0;
  return !inactive;
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
    active: f.Active !== false,
    externalDisplayStatus: nz(f["External Display Status"]),
    visibleInExplorer: isPresentationRowVisibleInExplorer(f),
    hasImage: Array.isArray(f.Image) && f.Image.length > 0,
    imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? nz(f.Image[0].url) : null,
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
    caseSummaryOverview: nz(f["Case Summary Overview"]),
    caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
    caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
    caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
    caseSummaryTags: nz(f["Case Summary Tags"]),
  };
}

function mapRegistryAsset(asset) {
  return {
    id: asset.id,
    assetName: nz(asset.assetName),
    assetStatus: nz(asset.assetStatus),
    explorerUsePermission: nz(asset.explorerUsePermission),
    usageReviewStatus: nz(asset.usageReviewStatus),
    recommendedExplorerSlot: nz(asset.recommendedExplorerSlot),
    sourcePageUrl: nz(asset.sourcePageUrl),
    imageUrl: nz(asset.sourceUrl || asset.imageUrl),
  };
}

function buildProposedUpdates(assessment, liveRow, brandRecordId, brandName) {
  const updates = [];
  if (
    assessment.suppressionAction === "set_display_status_do_not_display" &&
    liveRow.externalDisplayStatus !== EXTERNAL_DISPLAY_STATUS_QUARANTINE
  ) {
    updates.push({
      action: "suppress_display_status",
      recordId: assessment.recordId,
      slotKey: assessment.slotKey,
      fixReason: assessment.imageUnsafe ? "quarantine_unsafe_image" : "quarantine_internal_language",
      fields: {
        "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
        "Brand Name": brandName,
        Brand: [brandRecordId],
      },
      before: { externalDisplayStatus: liveRow.externalDisplayStatus || null },
      after: { externalDisplayStatus: EXTERNAL_DISPLAY_STATUS_QUARANTINE },
    });
  }
  if (assessment.clearImage && liveRow.hasImage) {
    updates.push({
      action: "clear_image",
      recordId: assessment.recordId,
      slotKey: assessment.slotKey,
      fixReason: "remove_do_not_use_or_wrong_brand_image_from_active_row",
      fields: {
        Image: [],
        "Brand Name": brandName,
        Brand: [brandRecordId],
      },
      before: { hasImage: true },
      after: { hasImage: false },
    });
  }
  if (assessment.copyRepairEligible) {
    try {
      const repaired = proposeOwnerFacingOpeningsCopy({
        title: liveRow.title,
        body: liveRow.body,
        summaryUrl: liveRow.summaryUrl || extractUrlFromBody(liveRow.body),
      });
      const copyChanged =
        repaired.body !== liveRow.body ||
        repaired.caseSummaryOverview !== liveRow.caseSummaryOverview;
      if (copyChanged) {
        updates.push({
          action: "copy_repair",
          recordId: assessment.recordId,
          slotKey: assessment.slotKey,
          fixReason: "owner_facing_copy_quarantine",
          fields: {
            Body: repaired.body,
            "Case Summary Overview": repaired.caseSummaryOverview,
            "Case Summary Owner Objective": repaired.caseSummaryOwnerObjective,
            "Case Summary Brand Relevance": repaired.caseSummaryBrandRelevance,
            "Case Summary Interpretation": repaired.caseSummaryInterpretation,
            "Case Summary Tags": repaired.caseSummaryTags,
            "Brand Name": brandName,
            Brand: [brandRecordId],
          },
          before: {
            body: liveRow.body,
            caseSummaryOverview: liveRow.caseSummaryOverview,
            caseSummaryInterpretation: liveRow.caseSummaryInterpretation,
          },
          after: {
            body: repaired.body,
            caseSummaryOverview: repaired.caseSummaryOverview,
            caseSummaryInterpretation: repaired.caseSummaryInterpretation,
          },
        });
      }
    } catch (err) {
      updates.push({
        action: "copy_repair_blocked",
        recordId: assessment.recordId,
        slotKey: assessment.slotKey,
        fixReason: "copy_repair_guardrail_failed",
        error: nz(err?.message),
      });
    }
  }
  return updates;
}

function extractUrlFromBody(body) {
  const m = nz(body).match(/https?:\/\/[^\s]+/i);
  return m ? m[0] : "";
}

function estimatePostApplyReadiness({ suppressCount, remainingUnsafe }) {
  const overallNumeric = remainingUnsafe ? 58 : 72;
  return {
    overallNumeric,
    overallActiveProfileReadiness: remainingUnsafe ? "not_ready" : "almost_ready",
    note: remainingUnsafe
      ? "Active-profile still blocked until founder approves registry images and re-activates vetted rows."
      : "Unsafe openings quarantined; image approval still required before active-profile.",
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-openings-suppression-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE_R1,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsOpeningsSuppressionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
  noImageApproval = false,
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
  const liveRows = presentationRaw.map(normalizePresentationRow);
  const evidenceRows = liveRows.filter((r) => isOpeningsEvidenceSlot(r.slotKey));

  const registryAssetsRaw = await listRegistryAssetsForBrand(target.recordId).catch(() => []);
  const registryAssets = registryAssetsRaw.map(mapRegistryAsset);

  const visibleInApi = evidenceRows.filter((r) => {
    const apiBlock = apiBlocks.find((b) => b.recordId === r.recordId);
    return r.visibleInExplorer && apiBlock;
  });

  const rowAudits = [];
  const proposedUpdates = [];

  for (const liveRow of evidenceRows) {
    const apiBlock =
      apiBlocks.find((b) => b.recordId === liveRow.recordId) || {
        recordId: liveRow.recordId,
        slotKey: liveRow.slotKey,
        title: liveRow.title,
        body: liveRow.body,
        imageUrl: liveRow.imageUrl,
        summaryUrl: liveRow.summaryUrl,
        caseSummaryOverview: liveRow.caseSummaryOverview,
        caseSummaryOwnerObjective: liveRow.caseSummaryOwnerObjective,
        caseSummaryBrandRelevance: liveRow.caseSummaryBrandRelevance,
        caseSummaryInterpretation: liveRow.caseSummaryInterpretation,
        caseSummaryTags: liveRow.caseSummaryTags,
      };
    const registryMatch = findRegistryAssetForPresentationRow(registryAssetsRaw, apiBlock);
    const imageAssessment = assessPresentationRowImageGovernance(
      apiBlock,
      brandConfig,
      registryAssetsRaw
    );
    const assessment = assessOpeningsRowQuarantine(
      { ...liveRow, ...apiBlock },
      imageAssessment,
      registryMatch ? mapRegistryAsset(registryMatch) : null
    );
    if (!assessment) continue;

    rowAudits.push({
      recordId: assessment.recordId,
      slot: assessment.slotKey,
      title: assessment.title,
      visibleHeading: assessment.visibleHeading,
      location: assessment.location,
      currentlyVisibleInApi: visibleInApi.some((v) => v.recordId === assessment.recordId),
      imageStatus: assessment.imageStatus,
      registryRecordId: assessment.registryRecordId,
      registryApproved: assessment.registryApproved,
      approvalStatus: registryMatch
        ? nz(registryMatch.usageReviewStatus) || "Pending Image Review"
        : "none",
      bodyCopyPreview: nz(liveRow.body).slice(0, 240),
      modalCopyPreview: nz(liveRow.caseSummaryOverview).slice(0, 240),
      sourceLabel: nz(liveRow.summaryUrl) || extractUrlFromBody(liveRow.body),
      internalLanguageHits: assessment.internalLanguageHits,
      wrongBrandRisk: assessment.wrongBrandRisk || (assessment.imageStatus === "unsafe" ? { markerId: "registry_do_not_use_or_unapproved" } : null),
      safeForActiveDisplay: assessment.safeForActiveDisplay,
      recommendation: assessment.recommendation,
      suppressionAction: assessment.suppressionAction,
    });

    proposedUpdates.push(
      ...buildProposedUpdates(assessment, liveRow, target.recordId, target.name)
    );
  }

  const brandTarget = {
    slug: target.slug,
    recordId: target.recordId,
    name: target.name,
    resolution: { resolutionSource: "expansion_backlog" },
  };
  const quarantineDefects = detectOpeningsUiQuarantineDefects(
    evidenceRows,
    rowAudits.map((r) => ({
      ...r,
      slotKey: r.slot,
      imageUnsafe: !r.safeForActiveDisplay && r.imageStatus !== "missing",
    })),
    brandTarget
  );

  const rowsToSuppress = rowAudits.filter(
    (r) =>
      r.recommendation === "suppress_and_quarantine" ||
      r.recommendation === "suppress_or_repair_copy" ||
      r.suppressionAction
  );
  const rowsWithWrongBrand = rowAudits.filter((r) => r.wrongBrandRisk);
  const rowsWithInternalLanguage = rowAudits.filter((r) => r.internalLanguageHits?.length);
  const rowsSafeVisible = rowAudits.filter((r) => r.safeForActiveDisplay);
  const copyRepairs = proposedUpdates.filter((u) => u.action === "copy_repair");

  const applyBlockers = [];
  if (!founderReviewed && apply) applyBlockers.push("founder_review_flag_missing");
  const postSuppressStillVisible = rowAudits.filter(
    (r) =>
      !r.safeForActiveDisplay &&
      r.currentlyVisibleInApi &&
      !proposedUpdates.some(
        (u) =>
          u.recordId === r.recordId &&
          (u.action === "suppress" || u.action === "suppress_display_status")
      )
  );
  if (postSuppressStillVisible.length) {
    applyBlockers.push("unsafe_rows_would_remain_visible");
  }
  const activeRowsWithInternalAfter = rowAudits.filter(
    (r) =>
      r.safeForActiveDisplay &&
      r.internalLanguageHits?.length &&
      !copyRepairs.some((c) => c.recordId === r.recordId)
  );
  if (activeRowsWithInternalAfter.length) {
    applyBlockers.push("internal_language_would_remain_on_active_rows");
  }

  const applyGatesReady =
    apply && approveBatch && founderReviewed && noValidationClaim && noImageApproval;
  const hasWork = proposedUpdates.some((u) =>
    ["suppress", "suppress_display_status", "clear_image", "copy_repair"].includes(u.action)
  );
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = { created: [], updated: [], errors: [], imagesApproved: false, imagesMaterialized: false };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of proposedUpdates) {
      if (!["suppress", "suppress_display_status", "clear_image", "copy_repair"].includes(update.action)) {
        continue;
      }
      const liveRec = presentationRaw.find((r) => r.id === update.recordId);
      if (!liveRec) {
        applyResults.errors.push({ recordId: update.recordId, error: "record_not_found" });
        continue;
      }
      const mergedFields = { ...(liveRec.fields || {}), ...update.fields };
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "PATCH",
        body: JSON.stringify({ fields: update.fields }),
      }, update.recordId);
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
        fixReason: update.fixReason,
      });
      airtableModified = true;
    }
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const remainingUnsafe = rowsToSuppress.length > 0 && !canApply;
  const expectedAfter = estimatePostApplyReadiness({
    suppressCount: rowsToSuppress.length,
    remainingUnsafe: canApply ? false : remainingUnsafe,
  });

  let expectedFinalQa = null;
  try {
    if (canApply) {
      const qaReport = await buildBrandExplorerFinalQaAuditorReport({
        brand: target.slug,
        dryRun: true,
      });
      expectedFinalQa = qaReport?.brandReadiness?.scores || null;
    }
  } catch {
    expectedFinalQa = expectedAfter;
  }

  const dryRunClean =
    applyBlockers.filter((b) => b !== "founder_review_flag_missing").length === 0 && hasWork;

  const report = {
    writerVersion: WRITER_VERSION,
    v31CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    visibleOpeningsEvidenceRows: rowAudits.filter((r) => r.currentlyVisibleInApi),
    rowAudits,
    wrongBrandImageRisks: rowsWithWrongBrand,
    internalLanguageRows: rowsWithInternalLanguage,
    rowsToSuppressOrQuarantine: rowsToSuppress,
    rowsSafeToRemainVisible: rowsSafeVisible,
    copyRepairProposals: copyRepairs.map((c) => ({
      recordId: c.recordId,
      slotKey: c.slotKey,
      before: c.before,
      after: c.after,
    })),
    proposedUpdates,
    quarantineDefects,
    imagesApproved: false,
    imagesMaterialized: false,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    expectedFinalQaAfterApply: expectedFinalQa || expectedAfter,
    expectedActiveProfileAfterApply: expectedAfter,
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-openings-suppression-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    governanceNote:
      "v31C-R1 sets External Display Status=Do Not Display and may clear Image on Do Not Use rows — never approves registry assets, never materializes replacement images, never modifies Company Validated.",
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Radisson Individuals Openings Suppression v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- v31C exists: **yes**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Visible evidence rows: **${report.visibleOpeningsEvidenceRows?.length ?? 0}**`);
  lines.push(`- Rows to suppress/quarantine: **${report.rowsToSuppressOrQuarantine?.length ?? 0}**`);
  lines.push(`- Wrong-brand risks: **${report.wrongBrandImageRisks?.length ?? 0}**`);
  lines.push(`- Internal language rows: **${report.internalLanguageRows?.length ?? 0}**`);
  lines.push(`- Safe to remain visible: **${report.rowsSafeToRemainVisible?.length ?? 0}**`);
  lines.push(`- Images approved: **no**`);
  lines.push(`- Images materialized: **no**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Row audit (sample)");
  for (const row of (report.rowAudits || []).slice(0, 16)) {
    lines.push(
      `- \`${row.slot}\` ${row.title} — visible: ${row.currentlyVisibleInApi ? "yes" : "no"} — ${row.recommendation}${row.internalLanguageHits?.length ? " ⚠ internal" : ""}${row.wrongBrandRisk ? " ⚠ image" : ""}`
    );
  }
  lines.push("");
  lines.push("## Expected after apply");
  lines.push(
    `- Final QA (est.): **${report.expectedFinalQaAfterApply?.overallNumeric ?? "—"}** (${report.expectedActiveProfileAfterApply?.overallActiveProfileReadiness ?? "not_ready"})`
  );
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none)");
  return lines.join("\n");
}
