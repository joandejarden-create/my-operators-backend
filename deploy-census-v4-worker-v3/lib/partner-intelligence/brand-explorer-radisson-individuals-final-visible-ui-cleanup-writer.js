/**
 * Brand Explorer Radisson Individuals Final Visible UI Cleanup v31D.
 *
 * Repairs visible, non-image-approval-dependent UI issues after v31C-R1 quarantine.
 * Does not re-activate quarantined rows or approve/materialize images.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer-v31D.md
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
  isGalleryImageSlot,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
} from "./brand-explorer-brand-asset-image-governance.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import {
  findInternalLanguageInRow,
  INTERNAL_UI_LANGUAGE_MARKERS,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  EXTERNAL_DISPLAY_STATUS_QUARANTINE,
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND as SUPPRESSION_TARGET,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";

export const WRITER_VERSION = "31D";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer-v31D.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31D-radisson-individuals-visible-ui-cleanup";
export const APPLY_FLAG_FOUNDER =
  "--founder-reviewed-radisson-individuals-visible-copy";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE =
  "--confirm-no-image-approval-or-materialization";

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
const FEATURED_SLOT = "overview.featured_application";
const FEATURED_TITLE = "Conversion-Friendly Individuality";
const FEATURED_BODY =
  "Independent hotels seeking Choice-family distribution and soft-brand flexibility while preserving local identity, owner story, and market-specific positioning.";

const CROSS_BRAND_CARRYOVER_RES = [
  { id: "tribute_portfolio", re: /\btribute portfolio\b/i },
  { id: "marriott", re: /\bmarriott\b/i },
  { id: "bonvoy", re: /\bbonvoy\b/i },
  { id: "curio_collection", re: /\bcurio collection\b/i },
  { id: "hilton_honors", re: /\bhilton honors\b/i },
  { id: "kimpton", re: /\bkimpton\b/i },
  { id: "ihg", re: /\bihg\b/i },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.md",
  "reports/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.json",
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
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Radisson Individuals API response",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Brand Asset Registry rows for Radisson Individuals",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.js",
  "scripts/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.mjs",
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

export function v31dWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31D`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31D supports Radisson Individuals by Choice only; got: ${brandArg}`);
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

export function sanitizeVisibleUiCopy(text) {
  let out = normalizeBody(text);
  if (!out) return out;

  out = out.replace(/\bitem\s*19\b[^.\n]*[.\n]?/gi, "");
  out = out.replace(
    /\bconfirm flag, fees, and opening status\b[^.\n]*[.\n]?/gi,
    "Confirm current flag, standards, commercial terms, and operating status directly before underwriting."
  );
  out = out.replace(/\bin your loi and fdd\b[^.\n]*[.\n]?/gi, "");
  out = out.replace(/\bfdd\b|\bfranchise disclosure document\b/gi, "franchise terms");
  out = out.replace(
    /\bcensus property url\b|\bcensus url extract\b|\bdealality census\b/gi,
    "Choice-family listing context"
  );
  out = out.replace(/\bconsumer site\b|\bconsumer-site\b/gi, "public brand page");
  out = out.replace(/\bactive property page\b|\bactive choice hotels property page\b/gi, "public listing page");
  out = out.replace(/\bsource data\b/gi, "documented context");
  out = out.replace(/\bmetadata\b/gi, "context");
  out = out.replace(/\binternal extraction\b|\binternal\b/gi, "");
  out = out.replace(/\bextraction\b/gi, "reference");
  out = out.replace(/\btribute portfolio\b/gi, "peer soft-brand collection");
  out = out.replace(/\bmarriott\b/gi, "peer brand family");
  out = out.replace(/\bbonvoy\b/gi, "peer loyalty program");
  out = out.replace(/\bcurio collection\b/gi, "peer lifestyle collection");
  out = out.replace(/\bhilton honors\b/gi, "peer loyalty program");
  out = out.replace(/\bkimpton\b/gi, "peer boutique brand");
  out = out.replace(/\bihg\b/gi, "peer brand family");
  out = out.replace(/\bcompany validated\b/gi, "company sign-off");
  out = out.replace(/\bnot company validated\b/gi, "no company sign-off");
  out = out.replace(/\n{3,}/g, "\n\n");
  return out.trim();
}

export function detectVisibleCopyIssues(row) {
  const combined = [
    row.title,
    row.body,
    row.caseSummaryOverview,
    row.caseSummaryOwnerObjective,
    row.caseSummaryBrandRelevance,
    row.caseSummaryInterpretation,
    row.caseSummaryTags,
  ]
    .filter(Boolean)
    .join("\n");
  const issues = [];
  for (const hit of findInternalLanguageInRow(row)) {
    issues.push({ patternId: hit.markerId, severity: hit.severity, field: hit.field });
  }
  for (const marker of CROSS_BRAND_CARRYOVER_RES) {
    if (marker.re.test(combined)) {
      issues.push({ patternId: marker.id, severity: "high", field: "combined" });
    }
  }
  if (/\bitem\s*19\b/i.test(combined)) {
    issues.push({ patternId: "item_19_ui", severity: "high", field: "combined" });
  }
  if (/\bfdd\b/i.test(combined)) {
    issues.push({ patternId: "fdd_label", severity: "high", field: "combined" });
  }
  return issues;
}

function featuredRowFromApi(brand) {
  return (brand?.brandExplorer?.blocks || []).find((b) => nz(b.slotKey) === FEATURED_SLOT) || null;
}

function diagnoseFeaturedSlot(brand) {
  const row = featuredRowFromApi(brand);
  const hasDedicated = Boolean(row && (nz(row.body) || nz(row.title)));
  return {
    slotKey: FEATURED_SLOT,
    dedicatedRowExists: Boolean(row),
    dedicatedRowPopulated: hasDedicated,
    needsRowCreate: !hasDedicated,
    currentRecordId: row?.recordId || null,
    proposedTitle: FEATURED_TITLE,
    proposedBody: FEATURED_BODY,
  };
}

function assessQuarantineIntegrity(apiBlocks, allRows) {
  const quarantined = allRows.filter((r) => r.quarantined);
  const leaks = quarantined.filter((r) =>
    (apiBlocks || []).some((b) => b.recordId === r.recordId)
  );
  return {
    quarantinedRowCount: quarantined.length,
    leakedIntoApiBlocks: leaks.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      externalDisplayStatus: r.externalDisplayStatus,
    })),
    integrityOk: leaks.length === 0,
  };
}

function countRegistryPending(registryAssets) {
  return (registryAssets || []).filter(
    (a) => !isRegistryAssetApprovedForExplorer(a) && nz(a.assetStatus) !== "Do Not Use"
  ).length;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsFinalVisibleUiCleanupWriterReport({
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
  const visibleApiBlocks = apiBlocks.filter((b) => b?.recordId);

  const presentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );
  const allRows = presentationRaw.map(normalizePresentationRow);
  const visibleRows = allRows.filter((r) => r.visibleInExplorer);
  const quarantinedRows = allRows.filter((r) => r.quarantined);

  const registryAssetsRaw = await listRegistryAssetsForBrand(target.recordId).catch(() => []);

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

  const featuredDiagnosis = diagnoseFeaturedSlot(brandApi);
  const quarantineIntegrity = assessQuarantineIntegrity(apiBlocks, allRows);
  const registryPendingCount = countRegistryPending(registryAssetsRaw);

  const visibleCopyIssues = visibleRows
    .map((row) => ({
      recordId: row.recordId,
      slotKey: row.slotKey,
      title: row.title,
      issues: detectVisibleCopyIssues(row),
    }))
    .filter((r) => r.issues.length > 0);

  const visibleImageIssues = [];
  for (const row of visibleRows) {
    const apiBlock = apiBlocks.find((b) => b.recordId === row.recordId) || row;
    const imageAssessment = assessPresentationRowImageGovernance(
      { ...apiBlock, slotKey: row.slotKey, imageUrl: row.imageUrl },
      brandConfig,
      registryAssetsRaw
    );
    if (!isVisualImageSlot(row.slotKey)) continue;
    if (!row.hasImage && !apiBlock.imageUrl) continue;
    // v31D-R1: gallery pending images stay visible — only clear unsafe imagery
    if (isGalleryImageSlot(row.slotKey)) continue;

    const registryMatch = findRegistryAssetForPresentationRow(registryAssetsRaw, apiBlock);
    const registryDoNotUse =
      registryMatch &&
      (nz(registryMatch.assetStatus) === "Do Not Use" ||
        nz(registryMatch.explorerUsePermission) === "Do Not Use");
    const shouldClear =
      Boolean(imageAssessment?.wrongBrandRisk) || Boolean(registryDoNotUse);

    if (shouldClear) {
      visibleImageIssues.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        imageAssessment,
        registryRecordId: registryMatch?.id || null,
        recommendation: registryDoNotUse
          ? "clear_do_not_use_image"
          : "clear_wrong_brand_image",
      });
    }
  }

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const proposedUpdates = [];

  if (featuredDiagnosis.needsRowCreate) {
    const createPayload = {
      action: "create_featured_application",
      slotKey: FEATURED_SLOT,
      fixReason: "missing_dedicated_featured_application_row",
      fields: {
        "Slot Key": FEATURED_SLOT,
        Title: FEATURED_TITLE,
        Body: FEATURED_BODY,
        "Brand Name": target.name,
        Brand: [target.recordId],
        Active: true,
        "Sort Order": 0,
        "External Display Status": "Show Trust Label",
      },
      proposedTitle: FEATURED_TITLE,
      proposedBody: FEATURED_BODY,
    };
    rowsWouldCreate.push(createPayload);
    proposedUpdates.push(createPayload);
  }

  for (const row of visibleRows) {
    if (row.quarantined) continue;

    const copyIssues = detectVisibleCopyIssues(row);
    if (copyIssues.length > 0) {
      const repairedBody = sanitizeVisibleUiCopy(row.body);
      const repairedTitle = sanitizeVisibleUiCopy(row.title);
      const repairedOverview = sanitizeVisibleUiCopy(row.caseSummaryOverview);
      const repairedInterpretation = sanitizeVisibleUiCopy(row.caseSummaryInterpretation);
      const changed =
        repairedBody !== row.body ||
        repairedTitle !== row.title ||
        repairedOverview !== row.caseSummaryOverview ||
        repairedInterpretation !== row.caseSummaryInterpretation;
      if (changed) {
        const afterIssues = detectVisibleCopyIssues({
          ...row,
          title: repairedTitle,
          body: repairedBody,
          caseSummaryOverview: repairedOverview,
          caseSummaryInterpretation: repairedInterpretation,
        });
        const update = {
          action: "copy_cleanup",
          recordId: row.recordId,
          slotKey: row.slotKey,
          fixReason: "visible_copy_carryover_cleanup",
          issuesBefore: copyIssues,
          issuesAfter: afterIssues,
          fields: {
            Title: repairedTitle || row.title,
            Body: repairedBody,
            "Case Summary Overview": repairedOverview || row.caseSummaryOverview,
            "Case Summary Interpretation": repairedInterpretation || row.caseSummaryInterpretation,
            "Brand Name": target.name,
            Brand: [target.recordId],
          },
          before: {
            title: row.title,
            body: row.body.slice(0, 240),
            caseSummaryOverview: row.caseSummaryOverview.slice(0, 240),
          },
          after: {
            title: repairedTitle || row.title,
            body: repairedBody.slice(0, 240),
            caseSummaryOverview: repairedOverview.slice(0, 240),
          },
        };
        rowsWouldUpdate.push(update);
        proposedUpdates.push(update);
      }
    }

    const imageIssue = visibleImageIssues.find((i) => i.recordId === row.recordId);
    if (imageIssue && row.hasImage) {
      const clearUpdate = {
        action: "clear_unapproved_image",
        recordId: row.recordId,
        slotKey: row.slotKey,
        fixReason: "remove_unapproved_materialized_image_shell",
        fields: {
          Image: [],
          "Brand Name": target.name,
          Brand: [target.recordId],
        },
        before: { hasImage: true },
        after: { hasImage: false },
      };
      rowsWouldUpdate.push(clearUpdate);
      proposedUpdates.push(clearUpdate);
    }
  }

  const applyBlockers = [];
  if (!quarantineIntegrity.integrityOk) {
    applyBlockers.push("quarantined_rows_leaked_into_api_blocks");
  }
  if (FEATURED_BODY.match(/company validated|marriott validated/i)) {
    applyBlockers.push("unsupported_copy_in_featured_proposal");
  }

  const postRepairInternalVisible = visibleRows.filter((row) => {
    const pending = proposedUpdates.find(
      (u) => u.recordId === row.recordId && u.action === "copy_cleanup"
    );
    const candidate = pending
      ? {
          ...row,
          title: pending.fields.Title,
          body: pending.fields.Body,
          caseSummaryOverview: pending.fields["Case Summary Overview"],
          caseSummaryInterpretation: pending.fields["Case Summary Interpretation"],
        }
      : row;
    return detectVisibleCopyIssues(candidate).some((i) =>
      ["high", "critical"].includes(i.severity)
    );
  });
  if (postRepairInternalVisible.length) {
    applyBlockers.push("internal_language_would_remain_on_visible_rows");
  }

  const reactivationRisk = proposedUpdates.some((u) => {
    const row = allRows.find((r) => r.recordId === u.recordId);
    return row?.quarantined && u.fields?.["External Display Status"] !== EXTERNAL_DISPLAY_STATUS_QUARANTINE;
  });
  if (reactivationRisk) applyBlockers.push("quarantined_row_reactivation_risk");

  if (registryPendingCount > 0) {
    applyBlockers.push("registry_image_approval_still_pending_for_active_profile");
  }

  const hasWork = proposedUpdates.length > 0;
  const applyGatesReady =
    apply && approveBatch && founderReviewed && noValidationClaim && noImageApproval;
  const cosmeticBlockers = applyBlockers.filter(
    (b) => b !== "registry_image_approval_still_pending_for_active_profile"
  );
  const canApply = applyGatesReady && cosmeticBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = {
    created: [],
    updated: [],
    errors: [],
    imagesApproved: false,
    imagesMaterialized: false,
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of proposedUpdates) {
      if (update.action === "create_featured_application") {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: update.fields, typecast: true }),
        });
        if (!res.ok) {
          applyResults.errors.push({
            action: update.action,
            slotKey: update.slotKey,
            error: json.error?.message || `POST failed ${res.status}`,
          });
        } else {
          applyResults.created.push({ recordId: json.id, slotKey: update.slotKey });
          airtableModified = true;
        }
        await new Promise((r) => setTimeout(r, 220));
        continue;
      }

      const liveRec = presentationRaw.find((r) => r.id === update.recordId);
      if (!liveRec) {
        applyResults.errors.push({ recordId: update.recordId, error: "record_not_found" });
        continue;
      }
      if (HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(liveRec.fields?.["External Display Status"]))) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: "quarantined_row_write_blocked",
        });
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
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const expectedFinalQaNumeric = Math.min(
    88,
    (qaBrand.scores?.overallNumeric || 69) + (featuredDiagnosis.needsRowCreate ? 8 : 0) + 6
  );
  const expectedAfter = {
    overallNumeric: expectedFinalQaNumeric,
    overallActiveProfileReadiness: registryPendingCount > 0 ? "not_ready" : "almost_ready",
    note:
      registryPendingCount > 0
        ? "Visible UI improved; active-profile still blocked until Brand Asset Registry image approval and vetted openings reactivation."
        : "Visible cleanup complete; verify openings evidence depth before active-profile.",
  };

  const dryRunClean = cosmeticBlockers.length === 0 && hasWork;

  const report = {
    writerVersion: WRITER_VERSION,
    v31DWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    currentVisibleBlockerDiagnosis: {
      finalQaScore: qaBrand.scores?.overallNumeric ?? null,
      finalQaReadiness: qaBrand.scores?.overallActiveProfileReadiness ?? null,
      finalQaDefectCount: qaBrand.defects?.length ?? null,
      finalQaHighDefectCount: (qaBrand.defects || []).filter((d) => d.severity === "high").length,
      visualAuditDefectCount: visualBefore?.defectCounts?.total ?? null,
      visualAuditHighDefectCount: visualBefore?.defectCounts?.high ?? null,
      completeBuildScore: completeBrand.finalQaScores?.overallNumeric ?? null,
      visibleApiBlockCount: visibleApiBlocks.length,
      visibleRowsWithInternalLanguage: visibleCopyIssues.length,
      missingFeaturedApplicationRow: featuredDiagnosis.needsRowCreate,
      visibleRowsWithImageIssues: visibleImageIssues.length,
      quarantinedRowCount: quarantinedRows.length,
      registryPendingQueueCount: registryPendingCount,
    },
    featuredRowPlan: featuredDiagnosis,
    copyCleanupPlan: visibleCopyIssues,
    mediaCleanupPlan: visibleImageIssues,
    quarantineIntegrity,
    rowsWouldCreate,
    rowsWouldUpdate,
    proposedUpdates,
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
    expectedFinalQaAfterApply: expectedAfter,
    expectedActiveProfileAfterApply: expectedAfter,
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    governanceNote:
      "v31D creates featured_application, sanitizes visible copy, and clears unapproved image shells — never re-activates quarantined rows or approves registry assets.",
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const d = report.currentVisibleBlockerDiagnosis || {};
  const lines = [];
  lines.push(`# Brand Explorer Radisson Individuals Final Visible UI Cleanup v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Final QA score: **${d.finalQaScore ?? "—"}** (${d.finalQaReadiness ?? "—"})`);
  lines.push(`- Visual defects: **${d.visualAuditDefectCount ?? "—"}** (${d.visualAuditHighDefectCount ?? 0} high)`);
  lines.push(`- Visible API blocks: **${d.visibleApiBlockCount ?? 0}**`);
  lines.push(`- Quarantined rows: **${d.quarantinedRowCount ?? 0}**`);
  lines.push(`- Registry pending queue: **${d.registryPendingQueueCount ?? 0}**`);
  lines.push(`- Featured row create needed: **${d.missingFeaturedApplicationRow ? "yes" : "no"}**`);
  lines.push(`- Copy issues on visible rows: **${d.visibleRowsWithInternalLanguage ?? 0}**`);
  lines.push(`- Visible image issues: **${d.visibleRowsWithImageIssues ?? 0}**`);
  lines.push(`- Quarantine integrity: **${report.quarantineIntegrity?.integrityOk ? "ok" : "leak detected"}**`);
  lines.push(`- Creates: **${report.rowsWouldCreate?.length ?? 0}** · Updates: **${report.rowsWouldUpdate?.length ?? 0}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Expected after apply");
  lines.push(
    `- Final QA (est.): **${report.expectedFinalQaAfterApply?.overallNumeric ?? "—"}** (${report.expectedActiveProfileAfterApply?.overallActiveProfileReadiness ?? "not_ready"})`
  );
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — resolve blockers first)");
  return lines.join("\n");
}
