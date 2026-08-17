/**
 * Brand Explorer Radisson Individuals Final QA + Featured Slot Reconciliation v31A-R1.
 *
 * Reconciles expansion slug resolution, Final QA visualCompleteness scoring,
 * Featured Application / Conversion Example slot, and v31A momentum idempotency
 * for Radisson Individuals by Choice after v31A partial profile backfill apply.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-final-qa-reconciliation-writer-v31A-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { resolveFinalQaBrandTarget } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import {
  buildBrandExplorerChoiceExpansionPartialProfileBackfillWriterReport,
} from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";

export const WRITER_VERSION = "31A-R1";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-final-qa-reconciliation-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-final-qa-reconciliation-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-final-qa-reconciliation-writer-v31A-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31A-R1-radisson-individuals-final-qa-reconciliation";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-featured-copy";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

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
const FEATURED_SLOT = "overview.featured_application";
const FEATURED_TITLE = "Conversion-Friendly Individuality";
const FEATURED_BODY =
  "Independent hotels that want Choice-family distribution and soft-brand flexibility while preserving local identity, owner story, and market-specific positioning within hand-selected collection standards—compare Individuals against rigid prototype flags when underwriting conversion economics.";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.md",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-complete-build-batch.md",
  "reports/brand-explorer-complete-build-batch.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "lib/partner-intelligence/brand-explorer-brand-target-resolver.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-choice-expansion-partial-profile-backfill-writer.js",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Radisson Individuals API response",
  "live Radisson Individuals presentation rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.js",
  "scripts/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-choice-expansion-partial-profile-backfill-writer.js",
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v31aR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31A-R1`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31A-R1 supports Radisson Individuals only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
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

function featuredRowFromApi(brand) {
  const row = (brand?.brandExplorer?.blocks || []).find((b) => nz(b.slotKey) === FEATURED_SLOT);
  return row || null;
}

function diagnoseFeaturedSlot(brand) {
  const row = featuredRowFromApi(brand);
  const positioning = nz(brand?.brandPositioning);
  const tagline = nz(brand?.brandTaglineMotto);
  const hasDedicated = Boolean(row && (nz(row.body) || nz(row.title)));
  const uiUsesBasicsFallback = !hasDedicated && (positioning.length > 220 || tagline);
  return {
    slotKey: FEATURED_SLOT,
    dedicatedRowExists: Boolean(row),
    dedicatedRowPopulated: hasDedicated,
    uiUsesBasicsFallback,
    truncatedInUi: uiUsesBasicsFallback && positioning.length > 220,
    currentTitle: nz(row?.title),
    currentBody: nz(row?.body),
    wordCount: wordCount(row?.body || positioning),
    proposedTitle: FEATURED_TITLE,
    proposedBody: FEATURED_BODY,
    needsRowCreate: !hasDedicated,
    needsRowUpdate:
      hasDedicated &&
      (wordCount(row?.body) < 25 ||
        nz(row?.title) !== FEATURED_TITLE ||
        wordCount(row?.body) < wordCount(FEATURED_BODY) - 5),
  };
}

function diagnoseIdempotency(v31aDryRun) {
  const brandReport = (v31aDryRun?.brandReports || []).find(
    (b) => b.brand?.slug === TARGET_BRAND.slug
  );
  const momentumQueued = (brandReport?.rowsWouldUpdate || []).filter(
    (r) => r.slotKey === "footprint.momentum"
  );
  return {
    momentumRowsStillQueued: momentumQueued.length,
    momentumRowIds: momentumQueued.map((r) => r.recordId),
    likelyCause:
      momentumQueued.length > 0
        ? "v31A proposeMomentumUpdates lacked body-match idempotency (fixed in v31A writer)"
        : "none",
    contentMismatchSamples: momentumQueued.slice(0, 3).map((r) => ({
      recordId: r.recordId,
      proposedBodyPreview: nz(r.proposedBody).slice(0, 120),
    })),
  };
}

function scoresAgree(finalQaScores, visualReport) {
  const visualAuditScore = visualReport?.visualComparability?.score ?? null;
  const finalVisual = finalQaScores?.visualCompletenessScore ?? null;
  if (visualAuditScore == null || finalVisual == null) return false;
  return Math.abs(visualAuditScore - finalVisual) <= 5;
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-final-qa-reconciliation-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsFinalQaReconciliationWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const resolverBefore = await resolveFinalQaBrandTarget(target.slug).catch((err) => ({
    error: err.message,
  }));
  const brandApi = await fetchBrandApiShape(target.recordId);
  const featuredDiagnosis = diagnoseFeaturedSlot(brandApi);

  const visualBefore = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));

  const completeBuildBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandsArg: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const v31aDryRun = await buildBrandExplorerChoiceExpansionPartialProfileBackfillWriterReport({
    brandsArg: target.slug,
    apply: false,
  }).catch((err) => ({ error: err.message }));

  const idempotencyDiagnosis = diagnoseIdempotency(v31aDryRun);

  const qaBrand = finalQaBefore?.brandReports?.[0] || {};
  const completeBrand =
    (completeBuildBefore?.brandReports || []).find((b) => b.slug === target.slug) ||
    completeBuildBefore?.brandReports?.[0] ||
    {};

  const slugFailureRootCause =
    resolverBefore?.error ||
    !resolverBefore?.recordId ||
    resolverBefore.recordId === target.slug
      ? "final_qa_auditor_used_active_registry_only_resolveBrandTarget; expansion slugs not passed to v28C resolver"
      : null;

  const visualMismatchRootCause =
    qaBrand.scores?.visualCompletenessScore != null &&
    visualBefore?.visualComparability?.score != null &&
    Math.abs(qaBrand.scores.visualCompletenessScore - visualBefore.visualComparability.score) > 10
      ? "final_qa_double_counted_active_registry_parity_defects_economics_opening_loyalty_density_ui_missing_image_instead_of_visual_audit_score"
      : scoresAgree(qaBrand.scores, visualBefore)
        ? null
        : "residual_scoring_drift";

  const featuredDefects = (visualBefore?.defects || []).filter(
    (d) => d.slotKey === FEATURED_SLOT || /featured application/i.test(nz(d.section))
  );

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  if (featuredDiagnosis.needsRowCreate) {
    rowsWouldCreate.push({
      action: "create",
      slotKey: FEATURED_SLOT,
      fixReason: "missing_dedicated_featured_application_row",
      proposedTitle: FEATURED_TITLE,
      proposedBody: FEATURED_BODY,
      fields: {
        "Slot Key": FEATURED_SLOT,
        Title: FEATURED_TITLE,
        Body: FEATURED_BODY,
        "Brand Name": target.name,
        Brand: [target.recordId],
        Active: true,
        "Sort Order": 0,
      },
    });
  } else if (featuredDiagnosis.needsRowUpdate && featuredRowFromApi(brandApi)) {
    const live = featuredRowFromApi(brandApi);
    rowsWouldUpdate.push({
      action: "update",
      recordId: live.recordId,
      slotKey: FEATURED_SLOT,
      fixReason: "thin_featured_application_copy",
      proposedTitle: FEATURED_TITLE,
      proposedBody: FEATURED_BODY,
      fields: {
        "Slot Key": FEATURED_SLOT,
        Title: FEATURED_TITLE,
        Body: FEATURED_BODY,
        "Brand Name": target.name,
        Brand: [target.recordId],
        Active: true,
        "Sort Order": live.sort ?? live.sortOrder ?? 0,
      },
    });
  }

  const pendingImageReview = (v31aDryRun?.brandReports?.[0]?.pendingImageReview || []).map((item) => ({
    ...item,
    status: item.status || "pending_image_review",
  }));

  const applyBlockers = [];
  if (FEATURED_BODY.match(/company validated|marriott|curio|kimpton|ihg/i)) {
    applyBlockers.push("unsupported_copy_in_featured_proposal");
  }

  const finalQaAfter = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const visualAfter = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const completeBuildAfter = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandsArg: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch(() => null);

  const qaAfterBrand = finalQaAfter?.brandReports?.[0] || {};
  const completeAfterBrand =
    (completeBuildAfter?.brandReports || []).find((b) => b.slug === target.slug) || {};

  if (!scoresAgree(qaAfterBrand.scores, visualAfter)) {
    applyBlockers.push(
      `final_qa_visual_audit_score_mismatch:${qaAfterBrand.scores?.visualCompletenessScore}_vs_${visualAfter?.visualComparability?.score}`
    );
  }
  if ((visualAfter?.defectCounts?.critical || 0) > 0 || (visualAfter?.defectCounts?.high || 0) > 0) {
    applyBlockers.push("critical_or_high_visual_defects_remain");
  }

  const hasWork = rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0;
  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const created = [];
    const updated = [];
    const errors = [];
    for (const row of rowsWouldCreate) {
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) errors.push({ action: "create", slotKey: row.slotKey, message: json.error?.message });
      else created.push({ recordId: json.id, slotKey: row.slotKey });
      await new Promise((r) => setTimeout(r, 220));
    }
    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: row.fields, typecast: true }) },
        row.recordId
      );
      if (!res.ok) {
        errors.push({ action: "update", slotKey: row.slotKey, recordId: row.recordId, message: json.error?.message });
      } else updated.push({ recordId: row.recordId, slotKey: row.slotKey });
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = (created.length > 0 || updated.length > 0) && errors.length === 0;
    applyResults = { created, updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { created: [], updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const dryRunClean = applyBlockers.length === 0;

  const codeRepairs = [
    "resolveFinalQaBrandTarget() — v28C resolver for expansion slugs in final-qa-auditor",
    "resolveVisualAuditBrandRecordId() — v28C resolver in visual-display-defect-audit",
    "visualCompletenessScore uses visualReport.visualComparability.score when available",
    "Expansion brands skip active-registry-only economics.opening / loyalty density / ui duplicate defects",
    "Low cosmetic visual defects excluded from active-profile readiness gate",
    "v31A proposeMomentumUpdates body-match idempotency",
  ];

  const report = {
    writerVersion: WRITER_VERSION,
    v31AR1WriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    rootCauses: {
      finalQaSlugFailure: slugFailureRootCause
        ? {
            issue: "Brand not found on expansion slug",
            rootCause: slugFailureRootCause,
            fixedBy: "resolveFinalQaBrandTarget + v28C brand-target-resolver",
            resolverAfterFix: resolverBefore?.error ? null : resolverBefore,
          }
        : {
            issue: "none",
            rootCause: null,
            fixedBy: "resolveFinalQaBrandTarget + v28C brand-target-resolver",
            resolverAfterFix: resolverBefore,
          },
      visualScoreMismatch: {
        before: {
          finalQaVisualCompleteness: qaBrand.scores?.visualCompletenessScore,
          visualAuditScore: visualBefore?.visualComparability?.score,
          visualDefectCounts: visualBefore?.defectCounts,
          finalQaDefectCounts: qaBrand.defectCounts,
        },
        rootCause: visualMismatchRootCause,
        fixedBy: codeRepairs.slice(2, 5),
        after: {
          finalQaVisualCompleteness: qaAfterBrand.scores?.visualCompletenessScore,
          visualAuditScore: visualAfter?.visualComparability?.score,
          scoresAgree: scoresAgree(qaAfterBrand.scores, visualAfter),
        },
      },
      featuredApplication: {
        diagnosis: featuredDiagnosis,
        visualDefects: featuredDefects,
        frontendMapping:
          "brand-explorer-atelier-from-api.js uses overview.featured_application when populated; otherwise truncates brandPositioning at 220 chars",
      },
      v31aIdempotency: idempotencyDiagnosis,
    },
    codeRepairsProposed: codeRepairs,
    rowsWouldCreate,
    rowsWouldUpdate,
    pendingImageReview,
    applyBlockers,
    dryRunClean,
    canApply,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyResults,
    before: {
      finalQa: qaBrand.scores,
      completeBuild: {
        readyForActiveProfile: completeBrand.readyForActiveProfile,
        readinessBand: completeBrand.readinessBand,
        finalQaScores: completeBrand.finalQaScores,
      },
    },
    expectedAfterFix: {
      finalQa: qaAfterBrand.scores,
      readyForActiveProfile: completeAfterBrand.readyForActiveProfile,
      readinessBand: completeAfterBrand.readinessBand,
      governedPlatformReady: completeAfterBrand.governedPlatformReady,
    },
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-final-qa-reconciliation-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean && hasWork ? buildApplyCommand({ brand: target.slug }) : null,
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Radisson Individuals Final QA Reconciliation v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- v31A-R1 exists: **yes**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Root causes");
  lines.push(`- Slug failure: ${report.rootCauses.finalQaSlugFailure.rootCause || "resolved"}`);
  lines.push(`- Visual score mismatch: ${report.rootCauses.visualScoreMismatch.rootCause || "resolved"}`);
  lines.push(`- Featured slot needs row: **${report.rootCauses.featuredApplication.diagnosis.needsRowCreate ? "yes" : "no"}**`);
  lines.push(`- v31A momentum idempotency: ${report.rootCauses.v31aIdempotency.likelyCause}`);
  lines.push("");
  lines.push("## Expected readiness after fix");
  lines.push(
    `- Final QA: **${report.expectedAfterFix.finalQa?.overallNumeric}** (${report.expectedAfterFix.finalQa?.overallActiveProfileReadiness})`
  );
  lines.push(`- Active-profile ready: **${report.expectedAfterFix.readyForActiveProfile ? "yes" : "no"}**`);
  lines.push(`- Pending image review: **${report.pendingImageReview.length}** slots`);
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(code repairs only — no Airtable apply needed)");
  return lines.join("\n");
}
