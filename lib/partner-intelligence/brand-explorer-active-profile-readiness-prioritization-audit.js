/**
 * Brand Explorer Active Profile Readiness Prioritization Audit v28A (read-only).
 *
 * Ranks blocked active brands by easiest path to active-profile ready after contract 100.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  buildBrandExplorerRequiredSectionPopulationContractReport,
} from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";

export const AUDIT_VERSION = "v28A";
export const REPORT_JSON_NAME = "brand-explorer-active-profile-readiness-prioritization-audit.json";
export const REPORT_MD_NAME = "brand-explorer-active-profile-readiness-prioritization-audit.md";
export const DOC_MD_NAME = "brand-explorer-active-profile-readiness-prioritization-audit-v28A.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-complete-build-batch.md",
  "reports/brand-explorer-complete-build-batch.json",
  "reports/brand-explorer-complete-build-curio-collection.md",
  "reports/brand-explorer-complete-build-curio-collection.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-complete-build-radisson-blu.md",
  "reports/brand-explorer-complete-build-radisson-blu.json",
  "reports/brand-explorer-complete-build-radisson.md",
  "reports/brand-explorer-complete-build-radisson.json",
  "reports/brand-explorer-complete-build-ascend.md",
  "reports/brand-explorer-complete-build-ascend.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-factory-gap-matrix-audit.md",
  "reports/brand-explorer-factory-gap-matrix-audit.json",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "docs/brand-explorer-presentation-slots.md",
  "live API/presentation rows for all six active brands",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-active-profile-readiness-prioritization-audit.js",
  "scripts/brand-explorer-active-profile-readiness-prioritization-audit.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const BLOCKER_BUCKETS = Object.freeze([
  "visual_image_work",
  "copy_carryover_cleanup",
  "source_fact_governance",
  "report_only_stale_flags",
  "true_content_gaps",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readJsonIfExists(relPath) {
  try {
    return JSON.parse(fs.readFileSync(path.join(ROOT, relPath), "utf8"));
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchBrandPresentationBlockCount(recordId) {
  const req = { query: { brandId: recordId, refresh: "1" }, headers: {} };
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
  const blocks = res.payload.brand?.brandExplorer?.blocks || [];
  return {
    name: nz(res.payload.brand.name),
    parentCompany: nz(res.payload.brand.parentCompany),
    presentationBlockCount: blocks.length,
    slotKeyCount: new Set(blocks.map((b) => nz(b.slotKey)).filter(Boolean)).size,
  };
}

function classifyBlockers(brandResult) {
  const buckets = {
    visual_image_work: [],
    copy_carryover_cleanup: [],
    source_fact_governance: [],
    report_only_stale_flags: [],
    true_content_gaps: [],
  };

  const blockers = Array.isArray(brandResult.blockers) ? brandResult.blockers : [];
  const applyReasons = brandResult.applySafety?.applyBlockReasons || [];
  const defectCounts = brandResult.visualQaStatus?.defectCounts || {};
  const gov = brandResult.governanceStatus || {};
  const govSnap = brandResult.applySafety?.governanceSnapshot || {};

  for (const b of blockers) {
    if (b.type === "brand_carryover" || b.category === "copy") {
      buckets.copy_carryover_cleanup.push(b);
    } else if (b.type === "visual_defect" || b.category === "frontend") {
      buckets.visual_image_work.push(b);
    }
  }

  if (defectCounts.missingImage > 0 || defectCounts.emptyBullet > 0) {
    buckets.visual_image_work.push({
      type: "visual_gap",
      message: `missingImage=${defectCounts.missingImage}, emptyBullet=${defectCounts.emptyBullet}`,
    });
  }
  if (defectCounts.titleOnlyOrThin > 0) {
    buckets.visual_image_work.push({
      type: "thin_copy",
      message: `titleOnlyOrThin=${defectCounts.titleOnlyOrThin}`,
    });
  }

  if (gov.pendingFacts > 0 || govSnap.pendingFacts > 0 || gov.factApprovalNeeded) {
    buckets.source_fact_governance.push({
      type: "pending_facts",
      count: gov.pendingFacts ?? govSnap.pendingFacts ?? 0,
    });
  }
  if (govSnap.fddFacts > 0) {
    buckets.source_fact_governance.push({ type: "fdd_sensitive_facts", count: govSnap.fddFacts });
  }
  if (govSnap.internalFacts > 0) {
    buckets.source_fact_governance.push({ type: "internal_facts", count: govSnap.internalFacts });
  }
  if (!brandResult.governedPlatformReady) {
    buckets.source_fact_governance.push({ type: "governed_platform_not_ready" });
  }
  if (gov.sourceEvidenceNeeded) {
    buckets.source_fact_governance.push({ type: "source_evidence_needed" });
  }

  for (const reason of applyReasons) {
    if (/stale|suppression|expected remaining/i.test(reason)) {
      buckets.report_only_stale_flags.push({ message: reason });
    }
  }

  if ((brandResult.tributeExpectedRemaining || []).length) {
    buckets.report_only_stale_flags.push({
      message: `tributeExpectedRemaining: ${brandResult.tributeExpectedRemaining.join(", ")}`,
    });
  }

  const residencesStage = (brandResult.stageResults || []).find(
    (s) => s.stage === "brand_residences_legacy_migration" && s.status === "blocked"
  );
  if (residencesStage?.summary?.conflicts > 0) {
    buckets.true_content_gaps.push({
      type: "brand_residences_conflict",
      count: residencesStage.summary.conflicts,
    });
  }

  if (brandResult.contractReadinessScore < 100) {
    buckets.true_content_gaps.push({
      type: "contract_below_100",
      score: brandResult.contractReadinessScore,
    });
  }

  return buckets;
}

function countBlockersByType(blockers, type) {
  return blockers.filter((b) => b.type === type).length;
}

function computeEaseScore(brandResult) {
  const finalQa = brandResult.finalQaScores || {};
  const defects = brandResult.visualQaStatus?.defectCounts || {};
  const gov = brandResult.governanceStatus || {};
  const govSnap = brandResult.applySafety?.governanceSnapshot || {};
  const carryover = countBlockersByType(brandResult.blockers || [], "brand_carryover");

  let score = Number(finalQa.overallNumeric) || 0;
  score += (Number(finalQa.sourceGovernanceScore) || 0) * 0.1;
  score += (Number(finalQa.visualCompletenessScore) || 0) * 0.05;
  if (brandResult.governedPlatformReady) score += 10;
  else score -= 15;
  score -= (defects.critical || 0) * 5;
  score -= (defects.high || 0) * 2;
  score -= (defects.medium || 0) * 0.5;
  score -= Math.min(35, (gov.pendingFacts || govSnap.pendingFacts || 0) * 0.3);
  score -= Math.min(45, (govSnap.fddFacts || 0) * 0.35);
  score -= (govSnap.internalFacts || 0) * 2;
  score -= carryover * 4;
  score -= (defects.missingImage || 0) * 3;
  score -= (defects.titleOnlyOrThin || 0) * 0.4;
  if (brandResult.readyForActiveProfile) score += 100;
  return Math.round(score * 10) / 10;
}

function primaryBlockerBucket(buckets) {
  const order = [
    "true_content_gaps",
    "source_fact_governance",
    "copy_carryover_cleanup",
    "visual_image_work",
    "report_only_stale_flags",
  ];
  for (const key of order) {
    if (buckets[key]?.length) return key;
  }
  return "visual_image_work";
}

function buildBrandAuditRow(brandResult, liveApi = null) {
  const defects = brandResult.visualQaStatus?.defectCounts || {};
  const gov = brandResult.governanceStatus || {};
  const govSnap = brandResult.applySafety?.governanceSnapshot || {};
  const buckets = classifyBlockers(brandResult);
  const carryoverDefects = (brandResult.blockers || []).filter((b) => b.type === "brand_carryover");
  const visualDefects = (brandResult.blockers || []).filter((b) => b.type === "visual_defect");

  return {
    brand: brandResult.brand,
    contractScore: brandResult.contractReadinessScore,
    contractReady: brandResult.contractReadinessScore === 100,
    activeProfileReady: Boolean(brandResult.readyForActiveProfile),
    readinessBand: brandResult.readinessBand,
    haltReason: brandResult.haltReason || "",
    finalQaScore: brandResult.finalQaScores?.overallNumeric ?? null,
    finalQaReadiness: brandResult.finalQaScores?.overallActiveProfileReadiness ?? "unknown",
    finalQaBreakdown: brandResult.finalQaScores || {},
    criticalDefects: defects.critical || 0,
    highDefects: defects.high || 0,
    mediumDefects: defects.medium || 0,
    totalVisualDefects: defects.total || 0,
    visualDefectSamples: visualDefects.slice(0, 6),
    carryoverDefects,
    carryoverCount: carryoverDefects.length,
    pendingFacts: gov.pendingFacts ?? govSnap.pendingFacts ?? 0,
    fddFacts: govSnap.fddFacts ?? 0,
    internalFacts: govSnap.internalFacts ?? 0,
    missingImages: defects.missingImage || 0,
    emptyCards: defects.emptyBullet || 0,
    titleOnlyOrThin: defects.titleOnlyOrThin || 0,
    sourceGovernance: {
      governedPlatformReady: Boolean(brandResult.governedPlatformReady),
      sourceCount: gov.sourceCount ?? 0,
      approvedExplorerSources: gov.approvedExplorerSources ?? 0,
      explorerFactCount: gov.explorerFactCount ?? 0,
      sourceEvidenceNeeded: Boolean(gov.sourceEvidenceNeeded),
      factApprovalNeeded: Boolean(gov.factApprovalNeeded),
    },
    nextRecommendedWriter: brandResult.nextRequiredWriter || "",
    visualQaScore: brandResult.visualQaStatus?.score ?? null,
    blockerBuckets: buckets,
    primaryBlockerBucket: primaryBlockerBucket(buckets),
    easeScore: computeEaseScore(brandResult),
    applySafe: Boolean(brandResult.applySafety?.safeForApplyApproved),
    applyBlockReasons: brandResult.applySafety?.applyBlockReasons || [],
    liveApi,
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Active Profile Readiness Prioritization Audit ${AUDIT_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- All six contract 100: **${report.allSixContract100 ? "yes" : "no"}**`);
  lines.push(`- Tribute active-profile ready: **${report.tributeActiveProfileReady ? "yes" : "no"}**`);
  lines.push(`- Recommended next brand: **${report.recommendations.nextBrand?.name || "none"}**`);
  lines.push(`- Multi-brand apply-approved safe: **${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Contract status (all active brands)");
  lines.push("| Brand | Contract | Active-profile | Final QA | Ease score |");
  lines.push("| --- | ---: | --- | ---: | ---: |");
  for (const row of report.allBrands) {
    lines.push(
      `| ${row.brand.name} | ${row.contractScore} | ${row.activeProfileReady ? "ready" : "blocked"} | ${row.finalQaScore ?? "—"} | ${row.easeScore} |`
    );
  }
  lines.push("");
  lines.push("## Ranked blocked brands (easiest path first)");
  for (const [idx, row] of report.rankedBlockedBrands.entries()) {
    lines.push(
      `${idx + 1}. **${row.brand.name}** (\`${row.brand.slug}\`) — Final QA ${row.finalQaScore}, ease ${row.easeScore}, primary blocker: \`${row.primaryBlockerBucket}\`, next writer: \`${row.nextRecommendedWriter}\``
    );
  }
  lines.push("");
  for (const row of report.rankedBlockedBrands) {
    lines.push(`### ${row.brand.name}`);
    lines.push(`- Critical/high defects: ${row.criticalDefects}/${row.highDefects}`);
    lines.push(`- Visual defects (total): ${row.totalVisualDefects}`);
    lines.push(`- Carryover defects: ${row.carryoverCount}`);
    lines.push(`- Pending facts: ${row.pendingFacts} · FDD: ${row.fddFacts} · Internal: ${row.internalFacts}`);
    lines.push(`- Missing images / empty cards: ${row.missingImages}/${row.emptyCards}`);
    lines.push(`- Governed platform ready: ${row.sourceGovernance.governedPlatformReady ? "yes" : "no"}`);
    lines.push(`- Blocker buckets:`);
    for (const bucket of BLOCKER_BUCKETS) {
      const items = row.blockerBuckets[bucket] || [];
      if (items.length) lines.push(`  - **${bucket}**: ${items.length} item(s)`);
    }
    lines.push("");
  }
  lines.push("## Recommendations");
  lines.push(`- **Next brand end-to-end:** ${report.recommendations.nextBrand?.name || "none"}`);
  lines.push(`- **Next writer to build/run:** ${report.recommendations.nextWriterToBuild}`);
  lines.push(`- **Multi-brand visual cleanup safe:** ${report.recommendations.multiBrandVisualCleanupSafe ? "yes (dry-run batch only)" : "no"}`);
  lines.push(`- **Apply-approved safe:** ${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}`);
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerActiveProfileReadinessPrioritizationAuditReport(options = {}) {
  const batchJson = readJsonIfExists("reports/brand-explorer-complete-build-batch.json");
  const finalQaJson = readJsonIfExists("reports/brand-explorer-final-qa-auditor.json");
  const factoryGapJson = readJsonIfExists("reports/brand-explorer-factory-gap-matrix-audit.json");

  let brandResults = batchJson?.brandResults || [];
  if (!brandResults.length) {
    const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({ allActive: true });
    brandResults = (finalQaReport.brandReports || []).map((br) => ({
      brand: br.brand,
      finalQaScores: br.scores,
      contractReadinessScore: br.scores?.requiredSectionReadinessScore ?? 0,
      readyForActiveProfile: br.scores?.overallActiveProfileReadiness === "ready",
      blockers: br.defects || [],
      visualQaStatus: { defectCounts: br.defectCounts },
      governanceStatus: br.governanceSummary || {},
      applySafety: { applyBlockReasons: [], safeForApplyApproved: false },
      nextRequiredWriter: br.recommendedFixBatches?.[0] || "",
      governedPlatformReady: br.governanceSummary?.governedPlatformReady,
    }));
  }

  const liveApiBySlug = {};
  if (options.allActive !== false) {
    for (const target of ACTIVE_BRAND_AUDIT_TARGETS) {
      liveApiBySlug[target.slug] = await fetchBrandPresentationBlockCount(target.recordId);
      await sleep(300);
    }
  }

  const contractLive = {};
  for (const target of ACTIVE_BRAND_AUDIT_TARGETS) {
    try {
      const contract = await buildBrandExplorerRequiredSectionPopulationContractReport({
        brandIdOrName: target.slug,
      });
      contractLive[target.slug] = {
        score: contract.readinessScore,
        ready: contract.brandExplorerRequiredSectionsReady,
      };
    } catch {
      contractLive[target.slug] = { score: null, ready: null };
    }
    await sleep(200);
  }

  const allBrands = brandResults.map((br) =>
    buildBrandAuditRow(br, liveApiBySlug[br.brand?.slug] || null)
  );

  for (const row of allBrands) {
    const live = contractLive[row.brand.slug];
    if (live?.score != null) {
      row.contractScoreLive = live.score;
      row.contractReadyLive = live.ready;
      row.contractScore = live.score;
      row.contractReady = live.score === 100 && live.ready;
    }
  }

  const tribute = allBrands.find((b) => b.brand.slug === "tribute-portfolio");
  const blocked = allBrands
    .filter((b) => !b.activeProfileReady && b.brand.slug !== "tribute-portfolio")
    .sort((a, b) => b.easeScore - a.easeScore);

  const allSixContract100 = allBrands.every((b) => b.contractScore === 100);
  const nextBrand = blocked[0] || null;

  const sharedVisualPattern = blocked.every(
    (b) =>
      b.nextRecommendedWriter === "v24C_source_evidence_work" ||
      /v24C_source_evidence/i.test(b.nextRecommendedWriter)
  );
  const sharedThinCopy = blocked.every((b) => b.titleOnlyOrThin >= 4);
  const anyCarryover = blocked.some((b) => b.carryoverCount > 0);
  const anyHeavyGovernance = blocked.some((b) => b.pendingFacts > 20 || b.fddFacts > 20);
  const batchSafeApply = batchJson?.batchAggregate?.brandsSafeForApplyApproved?.length > 0;

  const recommendations = {
    nextBrand: nextBrand
      ? {
          slug: nextBrand.brand.slug,
          name: nextBrand.brand.name,
          recordId: nextBrand.brand.recordId,
          easeScore: nextBrand.easeScore,
          finalQaScore: nextBrand.finalQaScore,
          rationale: [
            `Highest ease score (${nextBrand.easeScore}) among blocked brands`,
            nextBrand.sourceGovernance.governedPlatformReady
              ? "governed platform ready"
              : "governance still open",
            nextBrand.pendingFacts <= 5
              ? `low pending-fact load (${nextBrand.pendingFacts})`
              : `pending facts still elevated (${nextBrand.pendingFacts})`,
            nextBrand.primaryBlockerBucket === "visual_image_work"
              ? "remaining gaps are primarily visual/thin-copy not contract"
              : `primary blocker bucket: ${nextBrand.primaryBlockerBucket}`,
          ].join("; "),
        }
      : null,
    nextWriterToBuild: anyCarryover
      ? "v26A_copy_carryover_cleanup (carryover on Ascend/Radisson Blu) then v24C_source_evidence_work visual/thin-copy batch"
      : "v24C_source_evidence_work (shared Value Creation / thin-copy visual repair pattern)",
    multiBrandVisualCleanupSafe:
      sharedVisualPattern &&
      sharedThinCopy &&
      !anyHeavyGovernance &&
      allSixContract100,
    multiBrandVisualCleanupRationale: sharedVisualPattern
      ? "All blocked brands share the same visual defect writer recommendation and titleOnlyOrThin pattern — safe for a shared dry-run visual repair batch per brand, not a single apply-approved sweep."
      : "Brands diverge on visual defect patterns or governance load — process per-brand.",
    multiBrandApplyApprovedSafe: Boolean(batchSafeApply) && allBrands.every((b) => b.applySafe),
    applyApprovedRationale:
      batchJson?.batchAggregate?.brandsSafeForApplyApproved?.length === 0
        ? "batch aggregate reports brandsSafeForApplyApproved=0; all brands retain critical/visual or governance apply blockers."
        : "At least one brand marked apply-safe in batch aggregate.",
  };

  const report = {
    auditVersion: AUDIT_VERSION,
    v28AExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    allActive: Boolean(options.allActive ?? true),
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    allSixContract100,
    contractScoresByBrand: allBrands.map((b) => ({
      slug: b.brand.slug,
      name: b.brand.name,
      contractScore: b.contractScore,
      contractScoreLive: b.contractScoreLive ?? b.contractScore,
      activeProfileReady: b.activeProfileReady,
    })),
    tributeActiveProfileReady: Boolean(tribute?.activeProfileReady),
    tributeSummary: tribute || null,
    allBrands,
    rankedBlockedBrands: blocked,
    blockerBucketDefinitions: BLOCKER_BUCKETS,
    batchSnapshot: batchJson
      ? {
          generatedAt: batchJson.generatedAt,
          brandsReady: batchJson.batchAggregate?.brandsReady?.length ?? 0,
          brandsBlocked: batchJson.batchAggregate?.brandsBlocked?.length ?? 0,
          brandsSafeForApplyApproved: batchJson.batchAggregate?.brandsSafeForApplyApproved?.length ?? 0,
        }
      : null,
    finalQaSnapshot: finalQaJson?.generatedAt || null,
    factoryGapSnapshot: factoryGapJson?.generatedAt || null,
    recommendations,
    exactNextCommand: nextBrand
      ? `npm run brand-explorer-complete-build -- --brand ${nextBrand.brand.slug} --dry-run --target-quality active-profile`
      : "npm run brand-explorer-complete-build -- --all-active --dry-run --target-quality active-profile",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerActiveProfileReadinessPrioritizationAuditMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
