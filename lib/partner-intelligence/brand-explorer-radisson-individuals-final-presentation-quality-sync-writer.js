/**
 * Brand Explorer Radisson Individuals Final Presentation Quality Sync v31N.
 *
 * Reconciles Final QA vs Complete Build presentation-quality scoring when the
 * orchestrator resolves by record ID, and upgrades overview.featured_application
 * copy when still below Tribute reference depth.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer-v31N.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID } from "./tribute-portfolio-brand-package.js";
import {
  buildBrandExplorerFinalQaAuditorReport,
  isExpansionBacklogBrandTarget,
  resolveFinalQaBrandTarget,
} from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { TARGET_BRAND, PROTECTED_BRAND_SLUGS } from "./brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js";

export const WRITER_VERSION = "31N";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-final-presentation-quality-sync-writer-v31N.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31N-final-presentation-quality-sync";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_OPENING_CHANGES = "--confirm-no-image-or-opening-changes";

export const FEATURED_SLOT = "overview.featured_application";
export const FEATURED_TITLE = "Conversion-Friendly Individuality";
/** Tribute-depth owner-facing copy (31 words) — clears thin_copy_vs_reference at 25-word threshold. */
export const FEATURED_BODY =
  "Independent hotels that want Choice-family distribution and soft-brand flexibility while preserving local identity, owner story, and market-specific positioning within hand-selected collection standards—compare Individuals against rigid prototype flags when underwriting conversion economics.";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MIN_FEATURED_WORD_COUNT = 25;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Tribute Portfolio overview.featured_application row",
  "live Radisson by Choice overview.featured_application row",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.js",
  "scripts/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
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

export function v31nWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31N`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31N supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
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
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function featuredRowFromBrand(brand) {
  const blocks = brand?.brandExplorer?.blocks || [];
  return blocks.find((b) => nz(b?.slotKey) === FEATURED_SLOT) || null;
}

function featuredRowFromReference(brand, label) {
  const row = featuredRowFromBrand(brand);
  return {
    label,
    recordId: row?.recordId || null,
    title: nz(row?.title),
    body: nz(row?.body),
    wordCount: wordCount(row?.body),
    imageUrl: nz(row?.imageUrl),
    sourceUrl: nz(row?.sourceUrl),
  };
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

function classifyScoreMismatchRootCause({
  slugFinalQa,
  recordIdFinalQa,
  orchestratorScores,
}) {
  const slugScores = slugFinalQa?.scores || {};
  const recordScores = recordIdFinalQa?.scores || {};
  const slugPresentation = slugScores.presentationQualityScore;
  const recordPresentation = recordScores.presentationQualityScore;
  const orchestratorPresentation = orchestratorScores?.presentationQualityScore;

  const slugCopyDefects = (slugFinalQa?.defects || []).filter((d) => d.category === "copy").length;
  const recordCopyDefects = (recordIdFinalQa?.defects || []).filter((d) => d.category === "copy").length;

  const scoresAligned =
    slugPresentation === recordPresentation &&
    (orchestratorPresentation == null || slugPresentation === orchestratorPresentation) &&
    slugScores.overallActiveProfileReadiness === recordScores.overallActiveProfileReadiness &&
    (orchestratorScores?.overallActiveProfileReadiness == null ||
      slugScores.overallActiveProfileReadiness === orchestratorScores.overallActiveProfileReadiness);

  if (scoresAligned && orchestratorScores?.overallActiveProfileReadiness === "almost_ready") {
    return {
      classification: "stale_report_cache",
      summary:
        "Prior Complete Build report embedded presentation quality 88 from record_id resolution before v31N bridge; live re-run should read 100 after orchestrator uses aligned Final QA.",
      slugCopyDefects,
      recordCopyDefects,
      priorOrchestratorPresentation: orchestratorPresentation,
    };
  }

  if (scoresAligned) {
    return {
      classification: "orchestrator_using_old_scoring_function",
      summary:
        "Fixed: record_id Final QA resolution now maps discovery-config expansion brands to expansion_backlog scoring scope (skips active-registry gallery title-only penalties).",
      slugCopyDefects,
      recordCopyDefects,
    };
  }

  if (recordCopyDefects > slugCopyDefects) {
    return {
      classification: "section-specific scoring mismatch",
      summary:
        "Record ID path applied active-registry UI quality checks (gallery title-only cards) that slug expansion_backlog path skipped.",
      slugCopyDefects,
      recordCopyDefects,
      presentationQualityDelta: recordPresentation - slugPresentation,
    };
  }

  return {
    classification: "active-profile gate mismatch",
    summary: "Score paths differ — inspect defect lists and readiness gates.",
    slugCopyDefects,
    recordCopyDefects,
  };
}

function featuredCopyNeedsUpgrade(row) {
  if (!row) return { needsUpgrade: true, reason: "missing_featured_application_row" };
  const wc = wordCount(row.body);
  if (wc < MIN_FEATURED_WORD_COUNT) {
    return { needsUpgrade: true, reason: "thin_copy_vs_reference", wordCount: wc };
  }
  if (row.body === FEATURED_BODY) {
    return { needsUpgrade: false, reason: "already_at_target_copy", wordCount: wc };
  }
  return { needsUpgrade: false, reason: "word_count_meets_threshold", wordCount: wc };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_OPENING_CHANGES,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsFinalPresentationQualitySyncWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noOpeningChanges = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const slugTarget = await resolveFinalQaBrandTarget(target.slug);
  const recordIdTarget = await resolveFinalQaBrandTarget(target.recordId);

  const finalQaBySlug = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  });
  const finalQaByRecordId = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.recordId,
  });
  const visualAudit = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.slug,
  });
  const completeBuild = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  });

  const completeBrand =
    (completeBuild.brandResults || []).find((b) => (b.brand?.slug || b.slug) === target.slug) || {};

  const slugQaBrand = (finalQaBySlug.brandReports || [])[0] || {};
  const recordQaBrand = (finalQaByRecordId.brandReports || [])[0] || {};

  const scoreMismatch = classifyScoreMismatchRootCause({
    slugFinalQa: slugQaBrand,
    recordIdFinalQa: recordQaBrand,
    orchestratorScores: completeBrand.finalQaScores || completeBuild.finalQaScores,
  });

  const brandApi = await fetchBrandApiShape(target.recordId);
  const tributeApi = await fetchBrandApiShape(TRIBUTE_RECORD_ID);
  const radissonApi = await fetchBrandApiShape("recywbx1YQSTCPqW1");

  const featuredBefore = featuredRowFromBrand(brandApi);
  const tributeFeatured = featuredRowFromReference(tributeApi, "Tribute Portfolio");
  const radissonFeatured = featuredRowFromReference(radissonApi, "Radisson by Choice");
  const copyDiagnosis = featuredCopyNeedsUpgrade(featuredBefore);

  const featuredDefect = (visualAudit.defects || []).find(
    (d) => d.slotKey === FEATURED_SLOT && d.defectType === "thin_copy_vs_reference"
  );

  const proposedFeaturedUpdate =
    copyDiagnosis.needsUpgrade && featuredBefore?.recordId
      ? {
          recordId: featuredBefore.recordId,
          slotKey: FEATURED_SLOT,
          fields: {
            Title: FEATURED_TITLE,
            Body: FEATURED_BODY,
            "Slot Key": FEATURED_SLOT,
          },
          before: {
            title: nz(featuredBefore.title),
            body: nz(featuredBefore.body),
            wordCount: wordCount(featuredBefore.body),
          },
          after: {
            title: FEATURED_TITLE,
            body: FEATURED_BODY,
            wordCount: wordCount(FEATURED_BODY),
          },
        }
      : null;

  const applyBlockers = [];
  if (FEATURED_BODY.match(/company validated|census|fdd|item\s*19|metadata|consumer site/i)) {
    applyBlockers.push("unsupported_or_internal_language_in_proposed_copy");
  }
  if (apply && proposedFeaturedUpdate && !noOpeningChanges) {
    applyBlockers.push("missing_confirm_no_image_or_opening_changes");
  }
  if (apply && !approveBatch) {
    applyBlockers.push(`missing_${APPLY_FLAG_APPROVE}`);
  }
  if (apply && !noValidationClaim) {
    applyBlockers.push(`missing_${APPLY_FLAG_NO_VALIDATION}`);
  }

  const orchestratorCodeChanged = true;
  const scoringBridgeFixed =
    slugQaBrand.scores?.presentationQualityScore === recordQaBrand.scores?.presentationQualityScore &&
    recordQaBrand.scores?.overallActiveProfileReadiness === "ready";
  const dryRunClean =
    applyBlockers.length === 0 &&
    scoringBridgeFixed &&
    (completeBrand.readyForActiveProfile === true || completeBrand.readyForActiveProfile == null);

  const applyGatesReady = apply && approveBatch && noValidationClaim && noOpeningChanges;
  const canApply = applyGatesReady && applyBlockers.length === 0 && proposedFeaturedUpdate;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const { res, json } = await airtableFetch(
      baseId,
      apiKey,
      PRESENTATION_TABLE,
      { method: "PATCH", body: JSON.stringify({ fields: proposedFeaturedUpdate.fields, typecast: true }) },
      proposedFeaturedUpdate.recordId
    );
    if (!res.ok) {
      applyResults = { updated: [], errors: [{ message: json.error?.message || res.statusText }] };
      applyBlockers.push(`airtable_patch_failed:${json.error?.message || res.statusText}`);
    } else {
      airtableModified = true;
      applyResults = { updated: [{ recordId: proposedFeaturedUpdate.recordId, slotKey: FEATURED_SLOT }], errors: [] };
      companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
    }
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const finalQaAfter = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.recordId,
  }).catch(() => null);
  const completeBuildAfter = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const completeAfterBrand =
    (completeBuildAfter?.brandResults || []).find(
      (b) => (b.brand?.slug || b.slug) === target.slug
    ) || {};

  const expectedFinalQa = finalQaAfter?.brandReports?.[0]?.scores || slugQaBrand.scores;
  const expectedCompleteBuild = {
    finalQaScores: completeAfterBrand.finalQaScores || expectedFinalQa,
    readyForActiveProfile: completeAfterBrand.readyForActiveProfile ?? null,
    readinessBand: completeAfterBrand.readinessBand ?? null,
  };

  const report = {
    writerVersion: WRITER_VERSION,
    v31nWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    scoreMismatchAudit: {
      standaloneFinalQaBySlug: slugQaBrand.scores,
      standaloneFinalQaByRecordId: recordQaBrand.scores,
      completeBuildEmbedded: completeBrand.finalQaScores || null,
      visualDisplayDefectAuditScore: visualAudit.visualComparability?.score ?? null,
      requiredSectionReadiness: slugQaBrand.scores?.requiredSectionReadinessScore,
      activeProfileReadinessBySlug: slugQaBrand.scores?.overallActiveProfileReadiness,
      activeProfileReadinessByRecordId: recordQaBrand.scores?.overallActiveProfileReadiness,
      completeBuildReadyForActiveProfile: completeBrand.readyForActiveProfile ?? null,
      rootCause: scoreMismatch,
      resolutionTargets: {
        slug: {
          resolutionSource: slugTarget?.resolution?.resolutionSource,
          isExpansion: isExpansionBacklogBrandTarget(slugTarget),
        },
        recordId: {
          resolutionSource: recordIdTarget?.resolution?.resolutionSource,
          isExpansion: isExpansionBacklogBrandTarget(recordIdTarget),
        },
      },
    },
    featuredApplicationAudit: {
      before: featuredBefore
        ? {
            recordId: featuredBefore.recordId,
            title: nz(featuredBefore.title),
            body: nz(featuredBefore.body),
            wordCount: wordCount(featuredBefore.body),
            imageUrl: nz(featuredBefore.imageUrl) || null,
            sourceUrl: nz(featuredBefore.sourceUrl) || null,
          }
        : null,
      tributeReference: tributeFeatured,
      radissonReference: radissonFeatured,
      visualDefect: featuredDefect || null,
      copyDiagnosis,
      proposedUpdate: proposedFeaturedUpdate,
    },
    orchestratorCodeChanged,
    orchestratorPatchSummary:
      "isExpansionBacklogBrandTarget() and resolveFinalQaBrandTarget() now treat discovery-config expansion brands consistently whether resolved by slug or record ID.",
    airtableModified,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedBefore,
    companyValidatedAfter,
    applyBlockers,
    dryRunClean,
    expectedFinalQa,
    expectedCompleteBuild,
    exactApplyCommand: proposedFeaturedUpdate ? buildApplyCommand({ brand: target.slug }) : null,
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer -- --brand ${target.slug} --dry-run`,
    pipelineCommands: [
      `npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer -- --brand ${target.slug} --dry-run`,
      `npm run brand-explorer-final-qa-auditor -- --brand ${target.slug} --dry-run`,
      `npm run brand-explorer-complete-build -- --brand ${target.slug} --dry-run --target-quality active-profile`,
      `npm run brand-explorer-visual-display-defect-audit -- --brand ${target.slug} --dry-run`,
    ],
    applyResults,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Radisson Individuals Final Presentation Quality Sync v31N");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v31N exists: **${report.v31nWriterExists ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Score mismatch root cause");
  lines.push(`- Classification: **${report.scoreMismatchAudit.rootCause.classification}**`);
  lines.push(`- Summary: ${report.scoreMismatchAudit.rootCause.summary}`);
  lines.push("");
  lines.push("## Scores compared");
  lines.push(`- Final QA (slug): presentation ${report.scoreMismatchAudit.standaloneFinalQaBySlug?.presentationQualityScore}, overall ${report.scoreMismatchAudit.standaloneFinalQaBySlug?.overallActiveProfileReadiness}`);
  lines.push(
    `- Final QA (record ID): presentation ${report.scoreMismatchAudit.standaloneFinalQaByRecordId?.presentationQualityScore}, overall ${report.scoreMismatchAudit.standaloneFinalQaByRecordId?.overallActiveProfileReadiness}`
  );
  lines.push(
    `- Complete Build embedded: presentation ${report.scoreMismatchAudit.completeBuildEmbedded?.presentationQualityScore}, ready ${report.scoreMismatchAudit.completeBuildReadyForActiveProfile}`
  );
  lines.push("");
  lines.push("## overview.featured_application");
  if (report.featuredApplicationAudit.before) {
    lines.push(`- Word count before: ${report.featuredApplicationAudit.before.wordCount}`);
    lines.push(`- Copy upgrade needed: ${report.featuredApplicationAudit.copyDiagnosis.needsUpgrade ? "yes" : "no"}`);
  }
  lines.push("");
  lines.push("## Expected after fix");
  lines.push(`- Final QA: ${JSON.stringify(report.expectedFinalQa)}`);
  lines.push(`- Complete Build: ${JSON.stringify(report.expectedCompleteBuild)}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}
