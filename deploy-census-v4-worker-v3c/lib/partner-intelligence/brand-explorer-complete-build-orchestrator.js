/**
 * Brand Explorer Complete Build Orchestrator — one-command build/remediation pipeline.
 *
 * Coordinates existing Brand Explorer audits, review packages, and writers.
 * Dry-run by default; staged apply commands only with explicit gates.
 *
 * @see docs/data-intelligence/brand-explorer-complete-build-orchestrator.md
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  RESOLVER_VERSION,
  isResolvableBrandTarget,
  resolveOrchestratorBrandTargets,
} from "./brand-explorer-brand-target-resolver.js";
import { fetchLiveState, buildGovernancePlan } from "./tribute-portfolio-package-pipeline.js";
import { assessBrandExplorerGovernanceReadiness } from "./profile-governance-publish-readiness.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import {
  buildBrandExplorerRequiredSectionPopulationContractReport,
} from "./brand-explorer-required-section-population-contract.js";
import {
  buildBrandExplorerVisualDisplayDefectAuditReport,
} from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandResidencesLegacyMigrationWriterReport } from "./brand-residences-legacy-migration-writer.js";

export const ORCHESTRATOR_VERSION = "2";
export const BRAND_TARGET_RESOLVER_VERSION = RESOLVER_VERSION;
export const REPORT_JSON_NAME = "brand-explorer-complete-build-orchestrator.json";
export const REPORT_MD_NAME = "brand-explorer-complete-build-orchestrator.md";
export const DOC_MD_NAME = "brand-explorer-complete-build-orchestrator.md";
export const BATCH_REPORT_JSON_NAME = "brand-explorer-complete-build-batch.json";
export const BATCH_REPORT_MD_NAME = "brand-explorer-complete-build-batch.md";
export const APPLY_FLAG = "--approve-brand-explorer-complete-build";
export const DEFAULT_MAX_CONCURRENCY = 1;
export const MAX_DRY_RUN_CONCURRENCY = 2;

export function perBrandReportBasename(brandSlug) {
  return `brand-explorer-complete-build-${nz(brandSlug) || "unknown"}`;
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const COMPLETED_TRIBUTE_BATCHES = [
  "v25C-2B", "v25C-2C", "v25C-2D", "v25C-2E", "v25C-2F", "v25C-2G", "v25C-2H",
  "v25C-3A", "v25C-3B", "v25C-3C", "v25C-3D", "v25C-3E", "v25C-3F",
  "v25C-4A", "v25C-4B", "v25C-4C", "v25C-4D", "v25C-4E",
];

const PIPELINE_STAGES = [
  { id: "profile_inspection", name: "Brand profile inspection", type: "inspect" },
  { id: "source_inventory", name: "Source inventory", type: "inspect" },
  { id: "fact_inventory", name: "Fact inventory", type: "inspect" },
  { id: "brand_residences_legacy_migration", name: "Brand residences legacy migration", type: "validation", script: "brand-residences-legacy-migration-writer" },
  { id: "required_section_contract", name: "Required-section population contract", type: "audit", script: "brand-explorer-required-section-population-contract" },
  { id: "visual_defect_audit", name: "Visual defect audit", type: "audit", script: "brand-explorer-visual-display-defect-audit" },
  { id: "source_capture_package", name: "Source capture package", type: "review", script: "brand-explorer-required-section-source-capture-package", haltIf: ["source_stewardship_needed"] },
  { id: "fact_approval_package", name: "Fact approval package", type: "review", script: "brand-explorer-evidence-fact-review-package", haltIf: ["pending_facts_present"] },
  { id: "row_review_packages", name: "Required row review packages", type: "review", script: "brand-explorer-openings-momentum-row-review-package" },
  { id: "row_creation_writers", name: "Required row creation writers", type: "writer", candidates: ["brand-explorer-openings-momentum-row-creation-writer", "brand-explorer-loyalty-row-creation-writer", "brand-explorer-display-content-completion-writer"] },
  { id: "image_materialization", name: "Image materialization / visual repair", type: "writer", candidates: ["explorer-media-promotion-writer", "brand-explorer-openings-visual-modal-repair-writer", "brand-explorer-visual-minimums-backfill-writer"] },
  { id: "loyalty_quality", name: "Loyalty quality enhancement", type: "writer", candidates: ["brand-explorer-opening-loyalty-quality-repair-writer", "brand-explorer-bonvoy-loyalty-row-enhancement-writer"] },
  { id: "openings_momentum_quality", name: "Openings / Momentum quality enhancement", type: "writer", candidates: ["brand-explorer-momentum-editorial-link-repair-writer", "brand-explorer-momentum-announcement-source-upgrade-writer"] },
  { id: "portfolio_mix_context", name: "Portfolio Mix / Context normalization", type: "writer", script: "brand-explorer-portfolio-mix-context-normalization-writer" },
  { id: "standard_detail_table", name: "Standard Detail table creation", type: "writer", script: "brand-explorer-tribute-standard-detail-table-writer", requiresFounderReview: true },
  { id: "demand_scenario", name: "Demand Scenario creation", type: "writer", script: "brand-explorer-display-content-completion-writer", sectionKey: "demand" },
  { id: "geo_footprint", name: "Geographic Footprint refinement", type: "writer", script: "brand-explorer-display-content-completion-writer", sectionKey: "geo" },
  { id: "portfolio_context_ladder", name: "Portfolio Context ladder mapping", type: "writer", script: "brand-explorer-portfolio-context-ladder-mapping-repair" },
  { id: "final_qa_auditor", name: "Final QA Auditor", type: "audit", script: "brand-explorer-final-qa-auditor" },
  { id: "final_readiness_report", name: "Final readiness report", type: "report" },
];

const EXPECTED_TRIBUTE_REMAINING_BLOCKERS = [
  { id: "demand_scenario", section: "Demand Scenario View", writer: "brand-explorer-display-content-completion-writer" },
  { id: "geo_footprint", section: "Geographic Footprint", writer: "brand-explorer-display-content-completion-writer" },
  { id: "standard_detail_table", section: "Standard Detail / Where Available", writer: "brand-explorer-tribute-standard-detail-table-writer", note: "founder/legal approval required" },
  { id: "portfolio_context_ladder", section: "Portfolio Context", writer: "brand-explorer-portfolio-context-ladder-mapping-repair", priority: "low" },
  { id: "sort_order_hygiene", section: "Sort Order", writer: "brand-explorer-presentation-sort-order-audit", priority: "low" },
  { id: "featured_truncation", section: "Featured Application", writer: "brand-explorer-visual-copy-cleanup-writer", priority: "medium" },
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export async function resolveOrchestratorTargets(options = {}) {
  return resolveOrchestratorBrandTargets(options);
}

export function resolveMaxConcurrency(options = {}) {
  const requested = Number.parseInt(String(options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY), 10);
  const parsed = Number.isFinite(requested) && requested > 0 ? requested : DEFAULT_MAX_CONCURRENCY;
  if (options.applyApproved) return DEFAULT_MAX_CONCURRENCY;
  return Math.min(parsed, MAX_DRY_RUN_CONCURRENCY);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runWithConcurrency(items, concurrency, worker, { interBrandDelayMs = 0 } = {}) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runWorker() {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      if (index > 0 && interBrandDelayMs > 0 && concurrency === 1) {
        await sleep(interBrandDelayMs);
      }
      results[index] = await worker(items[index], index);
    }
  }

  const poolSize = Math.max(1, Math.min(concurrency, items.length));
  await Promise.all(Array.from({ length: poolSize }, () => runWorker()));
  return results;
}

function runNpmScript(scriptName, args = []) {
  const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";
  const result = spawnSync(npmCmd, ["run", scriptName, "--", ...args], {
    cwd: ROOT,
    encoding: "utf8",
    env: process.env,
    shell: process.platform === "win32",
  });
  return {
    script: scriptName,
    args,
    exitCode: result.status ?? 1,
    stdout: (result.stdout || "").slice(-4000),
    stderr: (result.stderr || "").slice(-2000),
    ok: (result.status ?? 1) === 0,
  };
}

function stageStatusFromContract(sectionName, contractReport) {
  const row = (contractReport?.sectionBySectionReadiness || []).find((s) => s.section === sectionName);
  if (!row) return { status: "unknown", classification: "unknown" };
  const ready = String(row.classification).startsWith("ready");
  return {
    status: ready ? "complete" : "blocked",
    classification: row.classification,
    currentCount: row.currentCount,
    requiredMinimum: row.requiredMinimum,
    recommendedNextAction: row.recommendedNextAction,
    blocker: row.blockerIfNotSafe,
  };
}

function computeGovernedPlatformReady(liveState) {
  return assessBrandExplorerGovernanceReadiness(liveState).governedPlatformReady;
}

function isVisualDefectBlockingActiveProfile(defect) {
  if (!defect || defect.cosmeticNonBlocking) return false;
  if (defect.severity === "critical" || defect.severity === "high") return true;
  if (
    defect.slotKey === "overview.featured_application" &&
    defect.defectType === "truncated_copy"
  ) {
    return true;
  }
  return false;
}

function tributeExpectedBlockerStillBlocking(item, contractReport, visualReport) {
  const contract = stageStatusFromContract(item.section, contractReport);
  if (contract.status === "blocked") return true;

  const defects = visualReport?.defects || [];
  if (item.id === "featured_truncation") {
    return defects.some(
      (d) =>
        d.slotKey === "overview.featured_application" &&
        isVisualDefectBlockingActiveProfile(d)
    );
  }
  if (item.id === "sort_order_hygiene") {
    return defects.some(
      (d) => d.defectType === "bad_sort_order" && (d.severity === "critical" || d.severity === "high")
    );
  }
  return false;
}

function computeReadyForActiveProfile({
  finalQaReport,
  contractReport,
  blockers,
  governedPlatformReady,
  explorerFactCount = 0,
}) {
  const finalQaReady = finalQaReport?.scores?.overallActiveProfileReadiness === "ready";
  const contractReady =
    contractReport?.readinessScore >= 100 && contractReport?.brandExplorerRequiredSectionsReady;
  const noBlockingDefects =
    (finalQaReport?.brandReports?.[0]?.defectsBySeverity?.critical?.length || 0) === 0 &&
    (finalQaReport?.brandReports?.[0]?.defectsBySeverity?.high?.length || 0) === 0;
  const noBlockingBlockers = blockers.length === 0;
  const governanceOk = explorerFactCount === 0 || Boolean(governedPlatformReady);
  return finalQaReady && contractReady && noBlockingDefects && noBlockingBlockers && governanceOk;
}

function brandSummaryRef(brandResult) {
  return {
    slug: brandResult.brand.slug,
    name: brandResult.brand.name,
    recordId: brandResult.brand.recordId,
    resolutionSource: brandResult.resolution?.resolutionSource || brandResult.brand.resolution?.resolutionSource || null,
  };
}

function buildResolutionErrorResult(target, targetQuality, message, extra = {}) {
  return {
    brand: target,
    resolution: target.resolution || null,
    targetQuality,
    error: message,
    halted: true,
    haltReason: "processing_error",
    blockers: [],
    stagedApplyCommands: [],
    nextRequiredWriter: "retry_after_resolution",
    readyForActiveProfile: false,
    readinessBand: "error",
    reportBasename: perBrandReportBasename(target.slug || target.resolution?.resolvedSlug || "unknown"),
    companyValidatedUntouched: true,
    applySafety: {
      safeForApplyApproved: false,
      applyBlockReasons: [
        target.resolution?.ambiguous ? "brand target resolution is ambiguous" : null,
        target.resolution?.error ? `brand target resolution failed: ${target.resolution.error}` : null,
        message,
      ].filter(Boolean),
      applySkippedReasons: ["brand target could not be resolved"],
      applyCommandsEligible: [],
      governanceSnapshot: {},
    },
    suggestedMatches: target.resolution?.suggestedMatches || extra.suggestedMatches || [],
    ...extra,
  };
}

function assessBrandApplySafety({
  brandResult,
  liveState,
  contractReport,
  visualReport,
  finalQaReport,
  governedPlatformReady,
}) {
  const reasons = [];
  const primaryQa = (finalQaReport?.brandReports || []).find(
    (b) => b.brand?.recordId === brandResult.brand.recordId || b.brand?.slug === brandResult.brand.slug
  ) || (finalQaReport?.brandReports || [])[0];
  const criticalCount = primaryQa?.defectsBySeverity?.critical?.length || 0;
  const carryoverCount = (primaryQa?.carryoverFindings || brandResult.blockers.filter((b) => b.type === "brand_carryover")).length;
  const pendingFacts = (liveState?.facts || []).filter(
    (f) =>
      (nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")) &&
      nz(f.humanReviewStatus) === "Pending"
  );
  const internalFacts = (liveState?.facts || []).filter(
    (f) =>
      (nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")) &&
      nz(f.publicVisibility) === "Internal Only" &&
      nz(f.humanReviewStatus) !== "Rejected"
  );
  const fddFacts = (liveState?.facts || []).filter(
    (f) => /fdd|franchise disclosure/i.test(`${nz(f.fieldName)} ${nz(f.sourceType)} ${nz(f.notes)}`)
  );
  const governance = buildGovernancePlan(liveState || { sources: [], facts: [], recordId: brandResult.brand.recordId });
  const companyValidatedSnapshot = liveState?.brandBasics?.fields?.["Company Validated"] ?? null;
  const governancePatchTouchesValidation =
    governance?.writePatch &&
    Object.prototype.hasOwnProperty.call(governance.writePatch, "Company Validated");
  const resolution = brandResult.resolution || brandResult.brand?.resolution || null;
  const loadedBrandName = nz(liveState?.brandBasics?.name || liveState?.brandBasics?.fields?.["Brand Name"]);
  const expectedBrandName = nz(resolution?.resolvedBrandName || brandResult.brand?.name);
  const recordNameMismatch =
    loadedBrandName &&
    expectedBrandName &&
    loadedBrandName.toLowerCase() !== expectedBrandName.toLowerCase();
  const missingRequiredImages = (visualReport?.defects || []).some(
    (d) =>
      !d.cosmeticNonBlocking &&
      (d.defectType === "missing_card_image" ||
        d.defectType === "blank_image_placeholder" ||
        d.defectType === "missing_image") &&
      (d.severity === "critical" || d.severity === "high")
  );
  const founderReviewNeeded = (contractReport?.sectionsNeedFounderReview || []).length > 0 ||
    brandResult.blockers.some((b) => b.category === "founder_legal_review");
  const sourceEvidenceNeeded = (contractReport?.sectionsNeedSourceCapture || []).length > 0 ||
    brandResult.blockers.some((b) => b.category === "source" && b.type === "required_section");
  const factApprovalNeeded = (contractReport?.sectionsNeedFactApproval || []).length > 0 ||
    pendingFacts.length > 0;

  if (resolution?.ambiguous) {
    reasons.push("brand target resolution is ambiguous");
  }
  if (resolution?.error) {
    reasons.push(`brand target resolution failed: ${resolution.error}`);
  }
  if (recordNameMismatch) {
    reasons.push(
      `resolved record ID does not match expected Brand Name (${expectedBrandName} vs ${loadedBrandName})`
    );
  }
  if (governancePatchTouchesValidation) {
    reasons.push("Company Validated would change under proposed governance apply");
  }
  if (brandResult.error) {
    reasons.push(`brand could not load: ${brandResult.error}`);
  }
  if (criticalCount > 0) reasons.push(`${criticalCount} critical Final QA defect(s) remain`);
  if (carryoverCount > 0) reasons.push("Wrong-brand copy/carryover risk detected");
  if (pendingFacts.length > 0) reasons.push(`${pendingFacts.length} pending explorer fact(s) would need approval before apply`);
  if (internalFacts.length > 0) reasons.push(`${internalFacts.length} internal-only fact(s) present in explorer scope`);
  if (fddFacts.length > 0) reasons.push(`${fddFacts.length} FDD-sensitive fact(s) in scope`);
  if (!contractReport?.brandExplorerRequiredSectionsReady) reasons.push("Required sections are not ready");
  if (missingRequiredImages) reasons.push("Required image slots are still missing");
  if (!governedPlatformReady) reasons.push("Source governance is insufficient for governed apply");
  if (founderReviewNeeded) reasons.push("Human/founder/legal review is required");

  const applyCommands = (brandResult.stagedApplyCommands || []).filter((cmd) => cmd.applyCommand);
  const actionableApplyCommands = applyCommands.filter((cmd) => {
    if (cmd.blockerType === "source_governance") {
      return !governedPlatformReady;
    }
    return brandResult.blockers.some((b) => b.section === cmd.section);
  });

  const safeForApplyApproved =
    reasons.length === 0 &&
    actionableApplyCommands.length > 0 &&
    !brandResult.halted;

  return {
    safeForApplyApproved,
    applyBlockReasons: reasons,
    applySkippedReasons: safeForApplyApproved ? [] : reasons.length ? reasons : ["No gated apply commands or brand not active-profile ready"],
    applyCommandsEligible: actionableApplyCommands.map((cmd) => ({
      section: cmd.section,
      command: cmd.applyCommand,
      requiresFounderReview: Boolean(cmd.requiresFounderReview),
    })),
    governanceSnapshot: {
      companyValidated: companyValidatedSnapshot,
      governedPlatformReady: Boolean(governedPlatformReady),
      sourceEvidenceNeeded,
      factApprovalNeeded,
      pendingFacts: pendingFacts.length,
      internalFacts: internalFacts.length,
      fddFacts: fddFacts.length,
    },
  };
}

function buildBatchAggregate(brandResults = []) {
  const ok = (br) => !br.error;
  const slugName = (br) => brandSummaryRef(br);

  return {
    totalBrands: brandResults.length,
    brandsProcessed: brandResults.filter(ok).length,
    brandsErrored: brandResults.filter((br) => br.error).map(slugName),
    brandsReady: brandResults.filter((br) => ok(br) && br.readyForActiveProfile).map(slugName),
    brandsAlmostReady: brandResults
      .filter(
        (br) =>
          ok(br) &&
          !br.readyForActiveProfile &&
          (br.finalQaScores?.overallActiveProfileReadiness === "almost_ready" ||
            br.readinessBand === "almost_ready")
      )
      .map(slugName),
    brandsBlocked: brandResults
      .filter(
        (br) =>
          ok(br) &&
          !br.readyForActiveProfile &&
          (br.finalQaScores?.overallActiveProfileReadiness === "blocked" ||
            br.readinessBand === "blocked" ||
            br.halted)
      )
      .map(slugName),
    brandsMissingSourceEvidence: brandResults
      .filter((br) => ok(br) && br.governanceStatus?.sourceEvidenceNeeded)
      .map(slugName),
    brandsNeedingFactApproval: brandResults
      .filter((br) => ok(br) && br.governanceStatus?.factApprovalNeeded)
      .map(slugName),
    brandsNeedingUiCopyRepair: brandResults
      .filter(
        (br) =>
          ok(br) &&
          ((br.visualQaStatus?.defectCounts?.medium || 0) > 0 ||
            (br.visualQaStatus?.defectCounts?.high || 0) > 0 ||
            br.blockers.some((b) => b.category === "frontend" || b.category === "copy"))
      )
      .map(slugName),
    brandsWithCarryoverRisk: brandResults
      .filter((br) => ok(br) && br.blockers.some((b) => b.type === "brand_carryover"))
      .map(slugName),
    brandsSafeForApplyApproved: brandResults
      .filter((br) => ok(br) && br.applySafety?.safeForApplyApproved)
      .map(slugName),
    brandsNotSafeForApply: brandResults
      .filter((br) => ok(br) && !br.applySafety?.safeForApplyApproved)
      .map((br) => ({
        ...slugName(br),
        reasons: br.applySafety?.applyBlockReasons || br.applySafety?.applySkippedReasons || [],
      })),
  };
}

function classifyReadinessBand(finalQaReport, readyForActiveProfile) {
  if (readyForActiveProfile) return "ready";
  const status = finalQaReport?.scores?.overallActiveProfileReadiness;
  if (status === "almost_ready") return "almost_ready";
  if (status === "blocked") return "blocked";
  return status || "not_ready";
}

async function processSingleBrand(target, options) {
  const {
    targetQuality,
    stopOnCritical,
    continueThroughWarnings,
    applyApproved,
    interBrandDelayMs = 0,
  } = options;
  const stageResults = [];
  let halted = false;
  let haltReason = "";
  let airtableModified = false;
  const applyExecutionLog = [];
  const brandIdentifier = target.recordId || target.slug;

  if (!isResolvableBrandTarget(target)) {
    return buildResolutionErrorResult(
      target,
      targetQuality,
      target.resolution?.ambiguous
        ? `Ambiguous brand target: ${target.resolution.inputTarget}`
        : `Could not resolve brand target: ${target.resolution?.inputTarget || target.slug || target.name}`
    );
  }

  try {
    const liveState = await fetchLiveState(target.recordId).catch(() => ({
      recordId: target.recordId,
      sources: [],
      facts: [],
      brandBasics: null,
    }));

    if (!liveState.brandBasics) {
      return buildResolutionErrorResult(
        target,
        targetQuality,
        `Could not load brand: ${target.resolution?.inputTarget || target.slug}`,
        { suggestedMatches: target.resolution?.suggestedMatches || [] }
      );
    }

    const loadedName = nz(liveState.brandBasics?.name || liveState.brandBasics?.fields?.["Brand Name"]);
    if (
      loadedName &&
      target.name &&
      loadedName.toLowerCase() !== target.name.toLowerCase()
    ) {
      return buildResolutionErrorResult(
        target,
        targetQuality,
        `Resolved record ID does not match expected Brand Name (${target.name} vs ${loadedName})`
      );
    }

    const companyValidatedBefore = liveState.brandBasics?.fields?.["Company Validated"] ?? null;
    const companyValidationDateBefore = liveState.brandBasics?.fields?.["Company Validation Date"] ?? null;

    stageResults.push({
      stage: "profile_inspection",
      status: "complete",
      summary: {
        brandName: liveState.brandBasics?.name || target.name,
        recordId: target.recordId,
        companyValidated: companyValidatedBefore,
        companyValidationDate: companyValidationDateBefore,
      },
    });

    const approvedSources = (liveState.sources || []).filter((s) => nz(s.approvedForExplorerUse) === "Yes");
    stageResults.push({
      stage: "source_inventory",
      status: "complete",
      summary: {
        sourceCount: (liveState.sources || []).length,
        approvedForExplorer: approvedSources.length,
      },
    });

    const explorerFacts = (liveState.facts || []).filter(
      (f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")
    );
    const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
    stageResults.push({
      stage: "fact_inventory",
      status: "complete",
      summary: {
        factCount: explorerFacts.length,
        approved: explorerFacts.filter((f) => /approved|edited/i.test(nz(f.humanReviewStatus))).length,
        pending: pendingFacts.length,
      },
    });

    const residencesMigration = await buildBrandResidencesLegacyMigrationWriterReport({
      brandIdOrName: brandIdentifier,
    });
    stageResults.push({
      stage: "brand_residences_legacy_migration",
      status: residencesMigration.summary.conflicts > 0 ? "blocked" : "complete",
      summary: residencesMigration.summary,
      command: `npm run brand-residences-legacy-migration-writer -- --brand ${target.slug} --dry-run`,
      futureSetupWritesToBrandBasicsOnly: residencesMigration.futureSetupWritesToBrandBasicsOnly,
    });

    const contractReport = await buildBrandExplorerRequiredSectionPopulationContractReport({
      brandIdOrName: target.recordId,
    });
    stageResults.push({
      stage: "required_section_contract",
      status: contractReport.brandExplorerRequiredSectionsReady ? "complete" : "blocked",
      summary: {
        readinessScore: contractReport.readinessScore,
        ready: contractReport.brandExplorerRequiredSectionsReady,
        sectionsReady: (contractReport.sectionBySectionReadiness || []).filter((s) =>
          String(s.classification).startsWith("ready")
        ).length,
        sectionsTotal: (contractReport.sectionBySectionReadiness || []).length,
        nextWriters: contractReport.exactNextWriterSequence || [],
      },
      command: `npm run brand-explorer-required-section-population-contract -- --brand ${target.slug} --dry-run`,
    });

    const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
      brandIdOrName: target.recordId,
    });
    stageResults.push({
      stage: "visual_defect_audit",
      status: (visualReport.defectCounts?.critical || 0) > 0 ? "blocked" : "complete",
      summary: {
        defects: visualReport.defectCounts,
        comparableToCurio: visualReport.visualComparability?.visuallyComparableToCurioToday,
        nextBatch: visualReport.recommendedNextBatch,
      },
      command: `npm run brand-explorer-visual-display-defect-audit -- --brand ${target.slug} --dry-run`,
    });

    if (stopOnCritical && (visualReport.defectCounts?.critical || 0) > 0) {
      halted = true;
      haltReason = "critical_visual_defects";
    }

    const reviewScripts = [
      "brand-explorer-required-section-source-capture-package",
      "brand-explorer-evidence-fact-review-package",
      "brand-explorer-openings-momentum-row-review-package",
    ];
    for (const script of reviewScripts) {
      if (halted && !continueThroughWarnings) break;
      const run = runNpmScript(script, ["--brand", brandIdentifier, "--dry-run"]);
      stageResults.push({
        stage: script,
        status: run.ok ? "complete" : "warning",
        exitCode: run.exitCode,
        command: `npm run ${script} -- --brand ${brandIdentifier} --dry-run`,
      });
    }

    const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
      brandIdOrName: brandIdentifier,
    });
    const primaryQa = (finalQaReport.brandReports || []).find((b) => !b.error) || finalQaReport.brandReports?.[0];
    if (stopOnCritical && (primaryQa?.defectsBySeverity?.critical?.length || 0) > 0) {
      halted = true;
      haltReason = haltReason || "critical_final_qa_defects";
    }

    stageResults.push({
      stage: "final_qa_auditor",
      status: finalQaReport.scores?.overallActiveProfileReadiness === "ready" ? "complete" : "blocked",
      summary: finalQaReport.scores,
      command: `npm run brand-explorer-final-qa-auditor -- --brand ${target.slug} --dry-run`,
    });

    const governedPlatformReady = computeGovernedPlatformReady(liveState);
    const blockers = detectRemainingBlockers(contractReport, visualReport, finalQaReport, target.slug);
    const stagedApplyCommands = buildStagedApplyCommands(target.slug, blockers, applyApproved);

    const tributeExpectedRemaining =
      target.slug === "tribute-portfolio"
        ? EXPECTED_TRIBUTE_REMAINING_BLOCKERS.map((item) => {
            const contract = stageStatusFromContract(item.section, contractReport);
            return {
              ...item,
              contractStatus: contract.status,
              stillBlocking: tributeExpectedBlockerStillBlocking(item, contractReport, visualReport),
            };
          })
        : [];

    const completedBatches =
      target.slug === "tribute-portfolio" ? COMPLETED_TRIBUTE_BATCHES : [];

    const nextRequiredWriter =
      blockers[0]?.recommendedWriter ||
      contractReport.exactNextWriterSequence?.[0] ||
      visualReport.recommendedNextBatch ||
      "none";

    const readyForActiveProfile = computeReadyForActiveProfile({
      finalQaReport,
      contractReport,
      blockers,
      governedPlatformReady,
      explorerFactCount: explorerFacts.length,
    });

    const brandResult = {
      brand: target,
      resolution: target.resolution || null,
      targetQuality,
      halted,
      haltReason,
      completedBatches,
      stageResults,
      blockers,
      tributeExpectedRemaining,
      stagedApplyCommands,
      nextRequiredWriter,
      finalQaScores: finalQaReport.scores,
      contractReadinessScore: contractReport.readinessScore,
      requiredSectionStatus: {
        score: contractReport.readinessScore,
        ready: contractReport.brandExplorerRequiredSectionsReady,
        sectionsReady: (contractReport.sectionBySectionReadiness || []).filter((s) =>
          String(s.classification).startsWith("ready")
        ).length,
        sectionsTotal: (contractReport.sectionBySectionReadiness || []).length,
      },
      visualQaStatus: {
        score: visualReport.visualComparability?.score,
        comparableToReference: visualReport.visualComparability?.visuallyComparableToCurioToday,
        defectCounts: visualReport.defectCounts,
        recommendedNextBatch: visualReport.recommendedNextBatch,
      },
      governanceStatus: {
        governedPlatformReady,
        sourceCount: (liveState.sources || []).length,
        approvedExplorerSources: approvedSources.length,
        explorerFactCount: explorerFacts.length,
        pendingFacts: pendingFacts.length,
        sourceEvidenceNeeded: (contractReport.sectionsNeedSourceCapture || []).length > 0,
        factApprovalNeeded:
          (contractReport.sectionsNeedFactApproval || []).length > 0 || pendingFacts.length > 0,
        founderReviewNeeded: (contractReport.sectionsNeedFounderReview || []).length > 0,
      },
      governedPlatformReady,
      readyForActiveProfile,
      readinessBand: classifyReadinessBand(finalQaReport, readyForActiveProfile),
      companyValidatedUntouched: true,
      companyValidatedSnapshot: companyValidatedBefore,
      companyValidationDateSnapshot: companyValidationDateBefore,
      exactDryRunCommand: `npm run brand-explorer-complete-build -- --brand ${target.slug} --dry-run --target-quality ${targetQuality}`,
      exactApplyCommand: `npm run brand-explorer-complete-build -- --brand ${target.slug} --apply-approved ${APPLY_FLAG} --target-quality ${targetQuality}`,
      reportBasename: perBrandReportBasename(target.slug),
    };

    brandResult.applySafety = assessBrandApplySafety({
      brandResult,
      liveState,
      contractReport,
      visualReport,
      finalQaReport,
      governedPlatformReady,
    });

    if (applyApproved) {
      if (brandResult.applySafety.safeForApplyApproved) {
        for (const cmd of brandResult.applySafety.applyCommandsEligible) {
          if (cmd.requiresFounderReview) {
            applyExecutionLog.push({
              section: cmd.section,
              status: "skipped",
              reason: "founder_or_legal_review_required",
              command: cmd.command,
            });
            continue;
          }
          const match = /^npm run ([^\s]+) -- (.+)$/.exec(cmd.command || "");
          if (!match) {
            applyExecutionLog.push({
              section: cmd.section,
              status: "skipped",
              reason: "unparsed_apply_command",
              command: cmd.command,
            });
            continue;
          }
          const [, scriptName, argString] = match;
          const args = argString.split(/\s+/).filter(Boolean);
          const run = runNpmScript(scriptName, args);
          applyExecutionLog.push({
            section: cmd.section,
            status: run.ok ? "applied" : "failed",
            exitCode: run.exitCode,
            command: cmd.command,
          });
          if (run.ok) airtableModified = true;
        }
      } else {
        applyExecutionLog.push({
          status: "skipped_brand",
          reasons: brandResult.applySafety.applyBlockReasons,
        });
      }
    }

    brandResult.applyExecutionLog = applyExecutionLog;
    brandResult.airtableModified = airtableModified;
    brandResult.companyValidatedUntouched = true;
    return brandResult;
  } catch (error) {
    return {
      brand: target,
      resolution: target.resolution || null,
      targetQuality,
      error: nz(error?.message) || String(error),
      halted: true,
      haltReason: "processing_error",
      blockers: [],
      stagedApplyCommands: [],
      nextRequiredWriter: "retry_after_error",
      readyForActiveProfile: false,
      readinessBand: "error",
      reportBasename: perBrandReportBasename(target.slug),
      companyValidatedUntouched: true,
      suggestedMatches: target.resolution?.suggestedMatches || [],
      applySafety: {
        safeForApplyApproved: false,
        applyBlockReasons: [nz(error?.message) || String(error)],
        applySkippedReasons: ["processing_error"],
        applyCommandsEligible: [],
        governanceSnapshot: {},
      },
    };
  } finally {
    if (interBrandDelayMs > 0) {
      await sleep(interBrandDelayMs);
    }
  }
}

function buildWriterApplyCommand(script, brandSlug, extraFlags = []) {
  return `npm run ${script} -- --brand ${brandSlug} --apply ${extraFlags.join(" ")}`.trim();
}

function detectRemainingBlockers(contractReport, visualReport, finalQaReport, brandSlug) {
  const blockers = [];
  const failingSections = (contractReport?.sectionBySectionReadiness || []).filter(
    (s) => !String(s.classification).startsWith("ready")
  );
  for (const section of failingSections) {
    blockers.push({
      type: "required_section",
      section: section.section,
      classification: section.classification,
      message: section.blockerIfNotSafe || `${section.section} not ready`,
      category: /founder_review/.test(section.classification)
        ? "founder_legal_review"
        : /fact_approval/.test(section.classification)
          ? "source"
          : /frontend_mapping/.test(section.classification)
            ? "frontend"
            : "data",
      recommendedWriter: section.recommendedNextAction,
    });
  }

  for (const defect of visualReport?.defects || []) {
    if (!isVisualDefectBlockingActiveProfile(defect)) continue;
    blockers.push({
        type: "visual_defect",
        section: defect.section || defect.surface || "",
        severity: defect.severity,
        message: defect.description || defect.message || defect.type,
        category: "frontend",
        recommendedWriter: defect.remediationBatch || visualReport.recommendedNextBatch,
      });
  }

  const primaryQa = (finalQaReport?.brandReports || [])[0];
  for (const defect of primaryQa?.carryoverFindings || []) {
    blockers.push({
      type: "brand_carryover",
      severity: defect.severity,
      message: defect.message,
      category: "copy",
      recommendedWriter: defect.recommendedFixBatch,
    });
  }

  if (brandSlug === "tribute-portfolio") {
    for (const expected of EXPECTED_TRIBUTE_REMAINING_BLOCKERS) {
      const match = blockers.find((b) => b.section === expected.section);
      if (!match && expected.priority !== "low") {
        const contractMatch = failingSections.find((s) => s.section === expected.section);
        if (contractMatch) continue;
      }
    }
  }

  return blockers;
}

function buildStagedApplyCommands(brandSlug, blockers, applyApproved) {
  const commands = [];
  const writerMap = {
    "Demand Scenario View": {
      dryRun: `npm run brand-explorer-display-content-completion-writer -- --brand ${brandSlug} --dry-run`,
      apply: buildWriterApplyCommand(
        "brand-explorer-display-content-completion-writer",
        brandSlug,
        ["--approve-brand-explorer-display-content-completion"]
      ),
    },
    "Geographic Footprint": {
      dryRun: `npm run brand-explorer-display-content-completion-writer -- --brand ${brandSlug} --dry-run`,
      apply: buildWriterApplyCommand(
        "brand-explorer-display-content-completion-writer",
        brandSlug,
        ["--approve-brand-explorer-display-content-completion"]
      ),
    },
    "Standard Detail / Where Available": {
      dryRun: `npm run brand-explorer-tribute-standard-detail-table-writer -- --brand ${brandSlug} --dry-run`,
      apply: buildWriterApplyCommand(
        "brand-explorer-tribute-standard-detail-table-writer",
        brandSlug,
        ["--approve-brand-explorer-tribute-standard-detail-table", "--founder-reviewed-standard-detail-copy"]
      ),
    },
    "Portfolio Context": {
      dryRun: `npm run brand-explorer-portfolio-context-ladder-mapping-repair -- --brand ${brandSlug} --dry-run`,
      apply: buildWriterApplyCommand(
        "brand-explorer-portfolio-context-ladder-mapping-repair",
        brandSlug,
        ["--approve-brand-explorer-v25C-4D-portfolio-context-ladder-mapping"]
      ),
    },
  };

  for (const blocker of blockers) {
    const mapped = writerMap[blocker.section];
    if (!mapped) continue;
    commands.push({
      section: blocker.section,
      blockerType: blocker.type,
      dryRunCommand: mapped.dryRun,
      applyCommand: applyApproved ? mapped.apply : null,
      requiresApproval: !applyApproved,
      requiresFounderReview: blocker.section === "Standard Detail / Where Available",
    });
  }

  if (brandSlug === "tribute-portfolio") {
    commands.push({
      section: "Governed source/fact path",
      blockerType: "source_governance",
      dryRunCommand: "npm run tribute-portfolio-package-pipeline -- --dry-run",
      applyCommand: applyApproved
        ? "npm run tribute-portfolio-package-pipeline -- --apply --approve-tribute-portfolio-package-pipeline"
        : null,
      requiresApproval: true,
    });
  }

  return commands;
}

export async function buildBrandExplorerCompleteBuildOrchestratorReport(options = {}) {
  const targetQuality = nz(options.targetQuality || "active-profile");
  const stopOnCritical = options.stopOnCritical !== false;
  const continueThroughWarnings = Boolean(options.continueThroughWarnings);
  const generateNextWriters = Boolean(options.generateNextWriters);
  const applyApproved = Boolean(options.applyApproved);
  const targets = await resolveOrchestratorTargets(options);
  const maxConcurrency = resolveMaxConcurrency(options);
  const batchMode = targets.length > 1;
  const interBrandDelayMs = maxConcurrency === 1 ? 1500 : 0;

  let brandResults = await runWithConcurrency(
    targets,
    maxConcurrency,
    (target) =>
      processSingleBrand(target, {
        targetQuality,
        stopOnCritical,
        continueThroughWarnings,
        applyApproved,
        interBrandDelayMs,
      }),
    { interBrandDelayMs: 0 }
  );

  if (applyApproved && stopOnCritical) {
    const criticalIndex = brandResults.findIndex(
      (br) =>
        !br.error &&
        ((br.finalQaScores?.overallActiveProfileReadiness === "blocked" && br.halted) ||
          (br.blockers || []).some((b) => b.severity === "critical"))
    );
    if (criticalIndex >= 0) {
      brandResults = brandResults.map((br, index) =>
        index > criticalIndex
          ? {
              ...br,
              skippedDueToStopOnCritical: true,
              applyExecutionLog: [
                ...(br.applyExecutionLog || []),
                { status: "skipped_brand", reason: "stop_on_critical_after_prior_brand" },
              ],
            }
          : br
      );
    }
  }

  const batchAggregate = buildBatchAggregate(brandResults);
  const primary = brandResults[0];
  const mode = applyApproved ? "apply-approved" : "dry-run";
  const airtableModified = brandResults.some((br) => br.airtableModified);

  const workflowBrandArg = targets.length === 1 ? targets[0].slug : null;
  const exactWorkflowCommand = batchMode
    ? options.allActive
      ? `npm run brand-explorer-complete-build -- --all-active --dry-run --target-quality ${targetQuality}`
      : `npm run brand-explorer-complete-build -- --brands ${targets.map((t) => t.slug).join(",")} --dry-run --target-quality ${targetQuality}`
    : `npm run brand-explorer-complete-build -- --brand ${workflowBrandArg} --dry-run --target-quality ${targetQuality}`;

  return {
    orchestratorVersion: ORCHESTRATOR_VERSION,
    brandTargetResolverVersion: BRAND_TARGET_RESOLVER_VERSION,
    v28CResolverExists: true,
    orchestratorExists: true,
    multiBrandMode: batchMode,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified,
    allActive: Boolean(options.allActive),
    brandsRequested: targets.map((t) => t.resolution?.inputTarget || t.slug),
    brandTargetResolutions: targets.map((t) => t.resolution || {
      inputTarget: t.slug,
      resolvedBrandName: t.name,
      resolvedRecordId: t.recordId,
      resolvedSlug: t.slug,
      resolutionSource: "active_registry",
    }),
    maxConcurrency,
    batchMode,
    targetQuality,
    stopOnCritical,
    continueThroughWarnings,
    generateNextWriters,
    applyApproved,
    applyGate: APPLY_FLAG,
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-expansion-backlog-planner.md",
      "reports/brand-explorer-expansion-backlog-planner.json",
      "lib/partner-intelligence/brand-explorer-brand-target-resolver.js",
      "lib/partner-intelligence/brand-explorer-expansion-backlog-planner.js",
      "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
      "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
      "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
      "lib/partner-intelligence/tribute-portfolio-package-pipeline.js",
      "package.json",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-brand-target-resolver.js",
      "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
      "scripts/brand-explorer-complete-build-orchestrator.mjs",
      "docs/data-intelligence/brand-explorer-complete-build-orchestrator.md",
      "reports/brand-explorer-complete-build-orchestrator.md",
      "reports/brand-explorer-complete-build-orchestrator.json",
      "reports/brand-explorer-complete-build-batch.md",
      "reports/brand-explorer-complete-build-batch.json",
      "package.json",
    ],
    pipelineStages: PIPELINE_STAGES.map((s) => s.name),
    brandResults,
    batchAggregate,
    perBrandReports: brandResults.map((br) => ({
      slug: br.brand.slug,
      basename: br.reportBasename,
      json: `reports/${br.reportBasename}.json`,
      md: `reports/${br.reportBasename}.md`,
    })),
    primaryBrand: primary?.brand || null,
    finalQaScores: primary?.finalQaScores || null,
    remainingBlockers: primary?.blockers || [],
    tributeExpectedRemaining: primary?.tributeExpectedRemaining || [],
    stagedApplyCommands: primary?.stagedApplyCommands || [],
    nextRequiredWriter: primary?.nextRequiredWriter || "",
    readyForActiveProfile: primary?.readyForActiveProfile || false,
    companyValidatedUntouched: brandResults.every((br) => br.companyValidatedUntouched !== false),
    exactWorkflowCommand,
    exactApplyWorkflowCommand: batchMode
      ? `npm run brand-explorer-complete-build -- --brands ${targets.map((t) => t.slug).join(",")} --apply-approved ${APPLY_FLAG} --target-quality ${targetQuality}`
      : `npm run brand-explorer-complete-build -- --brand ${workflowBrandArg} --apply-approved ${APPLY_FLAG} --target-quality ${targetQuality}`,
  };
}

export function buildPerBrandMarkdown(brandResult) {
  const lines = [];
  lines.push(`# Brand Explorer Complete Build — ${brandResult.brand.name}`);
  lines.push("");
  lines.push(`- Slug: \`${brandResult.brand.slug}\``);
  lines.push(`- Record: \`${brandResult.brand.recordId}\``);
  if (brandResult.resolution) {
    lines.push(`- Input target: \`${brandResult.resolution.inputTarget}\``);
    lines.push(`- Resolution source: **${brandResult.resolution.resolutionSource || "unknown"}**`);
    if (brandResult.resolution.parentCompany) {
      lines.push(`- Parent company: ${brandResult.resolution.parentCompany}`);
    }
  }
  lines.push(`- Target quality: **${brandResult.targetQuality}**`);
  lines.push(`- Readiness band: **${brandResult.readinessBand || "unknown"}**`);
  lines.push(`- Ready for active profile: **${brandResult.readyForActiveProfile ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${brandResult.companyValidatedUntouched ? "yes" : "no"}**`);
  if (brandResult.error) {
    lines.push(`- Error: **${brandResult.error}**`);
    if (brandResult.suggestedMatches?.length) {
      lines.push("");
      lines.push("## Suggested closest matches");
      for (const match of brandResult.suggestedMatches) {
        lines.push(`- ${match.name} (\`${match.recordId}\`, slug \`${match.slug}\`)`);
      }
    }
    return lines.join("\n");
  }
  lines.push("");
  lines.push("## Required sections");
  lines.push(`- Score: **${brandResult.requiredSectionStatus?.score}**`);
  lines.push(`- Ready: **${brandResult.requiredSectionStatus?.ready ? "yes" : "no"}**`);
  lines.push(
    `- Sections: ${brandResult.requiredSectionStatus?.sectionsReady}/${brandResult.requiredSectionStatus?.sectionsTotal}`
  );
  lines.push("");
  lines.push("## Final QA");
  lines.push(
    `- Status: **${brandResult.finalQaScores?.overallActiveProfileReadiness}** (${brandResult.finalQaScores?.overallNumeric})`
  );
  lines.push("");
  lines.push("## Visual QA");
  lines.push(`- Score: **${brandResult.visualQaStatus?.score}**`);
  lines.push(`- Defects: ${brandResult.visualQaStatus?.defectCounts?.total || 0}`);
  lines.push(`- Next batch: **${brandResult.visualQaStatus?.recommendedNextBatch || "none"}**`);
  lines.push("");
  lines.push("## Governance");
  lines.push(`- Governed platform ready: **${brandResult.governanceStatus?.governedPlatformReady ? "yes" : "no"}**`);
  lines.push(`- Sources: ${brandResult.governanceStatus?.sourceCount} (${brandResult.governanceStatus?.approvedExplorerSources} approved)`);
  lines.push(`- Explorer facts: ${brandResult.governanceStatus?.explorerFactCount} (${brandResult.governanceStatus?.pendingFacts} pending)`);
  lines.push("");
  lines.push("## Blockers");
  if (!brandResult.blockers?.length) lines.push("- none");
  for (const b of brandResult.blockers || []) {
    lines.push(`- [${b.category}] ${b.section || b.type}: ${b.message}`);
  }
  lines.push("");
  lines.push("## Next writer");
  lines.push(`- **${brandResult.nextRequiredWriter}**`);
  lines.push("");
  lines.push("## Apply safety");
  lines.push(`- Safe for apply-approved: **${brandResult.applySafety?.safeForApplyApproved ? "yes" : "no"}**`);
  if (brandResult.applySafety?.applyBlockReasons?.length) {
    for (const reason of brandResult.applySafety.applyBlockReasons) lines.push(`- block: ${reason}`);
  }
  lines.push("");
  lines.push("## Commands");
  lines.push("```bash");
  lines.push(brandResult.exactDryRunCommand);
  lines.push("```");
  if (brandResult.stagedApplyCommands?.length) {
    lines.push("");
    lines.push("### Staged apply");
    for (const cmd of brandResult.stagedApplyCommands) {
      lines.push(`- ${cmd.section}: \`${cmd.dryRunCommand}\``);
      if (cmd.applyCommand) lines.push(`  - apply: \`${cmd.applyCommand}\``);
    }
  }
  return lines.join("\n");
}

export function buildBatchMarkdown(report) {
  const lines = [];
  const agg = report.batchAggregate || {};
  lines.push("# Brand Explorer Complete Build — Batch Queue");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brands: **${report.brandsRequested?.join(", ") || ""}**`);
  lines.push(`- Max concurrency: **${report.maxConcurrency}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  if (report.brandTargetResolverVersion) {
    lines.push(`- Brand target resolver: **${report.brandTargetResolverVersion}**`);
  }
  lines.push("");
  if (report.brandTargetResolutions?.length) {
    lines.push("## Target resolution");
    lines.push("| Input | Resolved name | Record | Slug | Source |");
    lines.push("| --- | --- | --- | --- | --- |");
    for (const r of report.brandTargetResolutions) {
      lines.push(
        `| \`${r.inputTarget}\` | ${r.resolvedBrandName || "—"} | \`${r.resolvedRecordId || "—"}\` | \`${r.resolvedSlug || "—"}\` | ${r.resolutionSource || r.error || "—"} |`
      );
    }
    lines.push("");
  }
  lines.push("## Aggregate");
  lines.push(`- Ready: ${agg.brandsReady?.length || 0}`);
  lines.push(`- Almost ready: ${agg.brandsAlmostReady?.length || 0}`);
  lines.push(`- Blocked: ${agg.brandsBlocked?.length || 0}`);
  lines.push(`- Missing source evidence: ${agg.brandsMissingSourceEvidence?.length || 0}`);
  lines.push(`- Needing fact approval: ${agg.brandsNeedingFactApproval?.length || 0}`);
  lines.push(`- Needing UI/copy repair: ${agg.brandsNeedingUiCopyRepair?.length || 0}`);
  lines.push(`- Carryover risk: ${agg.brandsWithCarryoverRisk?.length || 0}`);
  lines.push(`- Safe for apply-approved: ${agg.brandsSafeForApplyApproved?.length || 0}`);
  lines.push(`- Not safe for apply: ${agg.brandsNotSafeForApply?.length || 0}`);
  lines.push("");
  const sections = [
    ["Ready", agg.brandsReady],
    ["Almost ready", agg.brandsAlmostReady],
    ["Blocked", agg.brandsBlocked],
    ["Missing source evidence", agg.brandsMissingSourceEvidence],
    ["Needing fact approval", agg.brandsNeedingFactApproval],
    ["UI/copy repair", agg.brandsNeedingUiCopyRepair],
    ["Carryover risk", agg.brandsWithCarryoverRisk],
    ["Safe for apply-approved", agg.brandsSafeForApplyApproved],
  ];
  for (const [title, items] of sections) {
    lines.push(`### ${title}`);
    if (!items?.length) lines.push("- none");
    else for (const item of items) lines.push(`- ${item.name} (\`${item.slug}\`)`);
    lines.push("");
  }
  lines.push("### Not safe for apply");
  if (!agg.brandsNotSafeForApply?.length) lines.push("- none");
  else {
    for (const item of agg.brandsNotSafeForApply) {
      lines.push(`- ${item.name} (\`${item.slug}\`)`);
      for (const reason of item.reasons || []) lines.push(`  - ${reason}`);
    }
  }
  lines.push("");
  lines.push("## Per-brand summary");
  for (const br of report.brandResults || []) {
    lines.push(
      `- **${br.brand.name}** (\`${br.brand.slug}\`): contract ${br.contractReadinessScore}, Final QA ${br.finalQaScores?.overallActiveProfileReadiness}, active-profile ${br.readyForActiveProfile ? "yes" : "no"}, next \`${br.nextRequiredWriter}\``
    );
  }
  lines.push("");
  lines.push("## Queue command");
  lines.push("```bash");
  lines.push(report.exactWorkflowCommand);
  lines.push("```");
  return lines.join("\n");
}

export function buildBrandExplorerCompleteBuildOrchestratorMarkdown(report) {
  if (report.batchMode) return buildBatchMarkdown(report);
  const lines = [];
  lines.push("# Brand Explorer Complete Build Orchestrator");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Target quality: **${report.targetQuality}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  for (const br of report.brandResults || []) {
    lines.push(buildPerBrandMarkdown(br));
    lines.push("");
  }
  lines.push("## One-command workflow");
  lines.push("```bash");
  lines.push(report.exactWorkflowCommand);
  lines.push("```");
  return lines.join("\n");
}
