/**
 * Brand Explorer Next Brand Selection Audit v28D (read-only).
 *
 * Compares five blocked active brands with four Wave 1 expansion brands to
 * recommend the best next path to active-profile ready after Tribute.
 *
 * @see docs/data-intelligence/brand-explorer-next-brand-selection-audit-v28D.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  RESOLVER_VERSION,
  getBrandTargetResolverContext,
  resolveBrandTarget,
} from "./brand-explorer-brand-target-resolver.js";
import { perBrandReportBasename } from "./brand-explorer-complete-build-orchestrator.js";

export const AUDIT_VERSION = "v28D";
export const REPORT_JSON_NAME = "brand-explorer-next-brand-selection-audit.json";
export const REPORT_MD_NAME = "brand-explorer-next-brand-selection-audit.md";
export const DOC_MD_NAME = "brand-explorer-next-brand-selection-audit-v28D.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const TARGET_BRAND_SLUGS = Object.freeze([
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
  "radisson-individuals-by-choice",
  "tapestry-collection-by-hilton",
  "autograph-collection",
  "design-hotels",
]);

export const ACTIVE_BLOCKED_SLUGS = Object.freeze([
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
]);

export const WAVE1_EXPANSION_SLUGS = Object.freeze([
  "radisson-individuals-by-choice",
  "tapestry-collection-by-hilton",
  "autograph-collection",
  "design-hotels",
]);

export const BLOCKER_BUCKETS_V28D = Object.freeze([
  "visual_thin_copy_defects",
  "source_fact_governance",
  "carryover_cleanup",
  "image_approval",
  "standards_contract_gaps",
  "report_only_stale_flags",
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-complete-build-batch.md",
  "reports/brand-explorer-complete-build-batch.json",
  "reports/brand-explorer-complete-build-radisson.md",
  "reports/brand-explorer-complete-build-radisson.json",
  "reports/brand-explorer-complete-build-ascend.md",
  "reports/brand-explorer-complete-build-ascend.json",
  "reports/brand-explorer-complete-build-radisson-blu.md",
  "reports/brand-explorer-complete-build-radisson-blu.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-complete-build-curio-collection.md",
  "reports/brand-explorer-complete-build-curio-collection.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "reports/brand-explorer-complete-build-tapestry-collection-by-hilton.md",
  "reports/brand-explorer-complete-build-tapestry-collection-by-hilton.json",
  "reports/brand-explorer-complete-build-autograph-collection.md",
  "reports/brand-explorer-complete-build-autograph-collection.json",
  "reports/brand-explorer-complete-build-design-hotels.md",
  "reports/brand-explorer-complete-build-design-hotels.json",
  "reports/brand-explorer-active-profile-readiness-prioritization-audit.md",
  "reports/brand-explorer-active-profile-readiness-prioritization-audit.json",
  "reports/brand-explorer-expansion-backlog-planner.md",
  "reports/brand-explorer-expansion-backlog-planner.json",
  "lib/partner-intelligence/brand-explorer-brand-target-resolver.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "live API/presentation rows for all target brands",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-next-brand-selection-audit.js",
  "scripts/brand-explorer-next-brand-selection-audit.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

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

function completeBuildReportPath(slug) {
  return `reports/${perBrandReportBasename(slug)}.json`;
}

function loadCompleteBuildBrandResult(slug) {
  const direct = readJsonIfExists(completeBuildReportPath(slug));
  if (direct?.brand) return direct;

  const batch = readJsonIfExists("reports/brand-explorer-complete-build-batch.json");
  const fromBatch = (batch?.brandResults || []).find((br) => br.brand?.slug === slug);
  if (fromBatch) return fromBatch;

  return null;
}

function expansionReviewQueueBySlug(expansionJson) {
  const map = new Map();
  for (const brand of expansionJson?.brands || []) {
    map.set(brand.proposedSlug, brand.reviewQueue || null);
  }
  return map;
}

async function fetchBrandPresentationSummary(recordId) {
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

function classifyBlockersV28D(brandResult, expansionReviewQueue = null) {
  const buckets = Object.fromEntries(BLOCKER_BUCKETS_V28D.map((k) => [k, []]));
  const defects = brandResult.visualQaStatus?.defectCounts || {};
  const gov = brandResult.governanceStatus || {};
  const govSnap = brandResult.applySafety?.governanceSnapshot || {};
  const contractScore = Number(brandResult.contractReadinessScore) || 0;

  for (const blocker of brandResult.blockers || []) {
    if (blocker.type === "brand_carryover") {
      buckets.carryover_cleanup.push(blocker);
    } else if (blocker.type === "visual_defect" || blocker.category === "frontend") {
      buckets.visual_thin_copy_defects.push(blocker);
    } else if (blocker.category === "source" || blocker.type === "required_section") {
      if (/fact|governance|source/i.test(`${blocker.message} ${blocker.classification}`)) {
        buckets.source_fact_governance.push(blocker);
      } else if (/contract|section|standard/i.test(`${blocker.section} ${blocker.message}`)) {
        buckets.standards_contract_gaps.push(blocker);
      }
    }
  }

  if ((defects.titleOnlyOrThin || 0) > 0 || (defects.high || 0) > 0 || (defects.critical || 0) > 0) {
    buckets.visual_thin_copy_defects.push({
      type: "visual_gap",
      message: `critical=${defects.critical || 0}, high=${defects.high || 0}, thin=${defects.titleOnlyOrThin || 0}`,
    });
  }

  if ((defects.missingImage || 0) > 0 || (defects.emptyBullet || 0) > 0) {
    buckets.image_approval.push({
      type: "missing_visual_asset",
      message: `missingImage=${defects.missingImage || 0}, emptyBullet=${defects.emptyBullet || 0}`,
    });
  }

  if (expansionReviewQueue?.parallelFlags?.includes("pending_image_review")) {
    buckets.image_approval.push({
      type: "expansion_review_queue",
      message: "pending_image_review flagged in v28B backlog planner",
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

  if (contractScore < 100) {
    buckets.standards_contract_gaps.push({
      type: "contract_below_100",
      score: contractScore,
      message: `Required-section contract ${contractScore}/100`,
    });
  }

  for (const reason of brandResult.applySafety?.applyBlockReasons || []) {
    if (/stale|suppression|expected remaining/i.test(reason)) {
      buckets.report_only_stale_flags.push({ message: reason });
    }
  }

  if ((brandResult.tributeExpectedRemaining || []).length) {
    buckets.report_only_stale_flags.push({
      message: `tributeExpectedRemaining: ${brandResult.tributeExpectedRemaining.join(", ")}`,
    });
  }

  return buckets;
}

function primaryBlockerBucket(buckets) {
  const order = [
    "standards_contract_gaps",
    "source_fact_governance",
    "carryover_cleanup",
    "visual_thin_copy_defects",
    "image_approval",
    "report_only_stale_flags",
  ];
  for (const key of order) {
    if (buckets[key]?.length) return key;
  }
  return "visual_thin_copy_defects";
}

function computeEaseScore(brandResult) {
  const finalQa = brandResult.finalQaScores || {};
  const defects = brandResult.visualQaStatus?.defectCounts || {};
  const gov = brandResult.governanceStatus || {};
  const govSnap = brandResult.applySafety?.governanceSnapshot || {};
  const carryover = (brandResult.blockers || []).filter((b) => b.type === "brand_carryover").length;
  const contractScore = Number(brandResult.contractReadinessScore) || 0;

  let score = Number(finalQa.overallNumeric) || 0;
  score += (Number(finalQa.sourceGovernanceScore) || 0) * 0.1;
  score += (Number(finalQa.visualCompletenessScore) || 0) * 0.05;
  if (brandResult.governedPlatformReady) score += 10;
  else score -= 15;
  if (contractScore === 100) score += 20;
  else score -= (100 - contractScore) * 0.8;
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

function buildBrandAuditRow({
  slug,
  cohort,
  resolution,
  brandResult,
  liveApi,
  expansionReviewQueue,
}) {
  const defects = brandResult?.visualQaStatus?.defectCounts || {};
  const gov = brandResult?.governanceStatus || {};
  const govSnap = brandResult?.applySafety?.governanceSnapshot || {};
  const buckets = brandResult ? classifyBlockersV28D(brandResult, expansionReviewQueue) : {};
  const carryoverDefects = (brandResult?.blockers || []).filter((b) => b.type === "brand_carryover");
  const imageApprovalNeeded =
    (defects.missingImage || 0) > 0 ||
    (defects.emptyBullet || 0) > 0 ||
    Boolean(expansionReviewQueue?.parallelFlags?.includes("pending_image_review"));

  const row = {
    slug,
    cohort,
    resolver: {
      loaded: Boolean(resolution && !resolution.error && !resolution.ambiguous && resolution.resolvedRecordId),
      inputTarget: resolution?.inputTarget || slug,
      resolvedBrandName: resolution?.resolvedBrandName || brandResult?.brand?.name || "",
      resolvedRecordId: resolution?.resolvedRecordId || brandResult?.brand?.recordId || "",
      resolvedSlug: resolution?.resolvedSlug || slug,
      resolutionSource: resolution?.resolutionSource || null,
      parentCompany: resolution?.parentCompany || liveApi?.parentCompany || null,
      error: resolution?.error || null,
      ambiguous: Boolean(resolution?.ambiguous),
      suggestedMatches: resolution?.suggestedMatches || [],
    },
    completeBuildLoaded: Boolean(brandResult && !brandResult.error),
    completeBuildError: brandResult?.error || null,
    brand: brandResult?.brand || {
      slug,
      name: resolution?.resolvedBrandName || slug,
      recordId: resolution?.resolvedRecordId || "",
    },
    contractScore: brandResult?.contractReadinessScore ?? null,
    contractReady: brandResult?.contractReadinessScore === 100,
    finalQaScore: brandResult?.finalQaScores?.overallNumeric ?? null,
    finalQaReadiness: brandResult?.finalQaScores?.overallActiveProfileReadiness ?? "unknown",
    criticalDefects: defects.critical || 0,
    highDefects: defects.high || 0,
    mediumDefects: defects.medium || 0,
    totalVisualDefects: defects.total || 0,
    titleOnlyOrThin: defects.titleOnlyOrThin || 0,
    missingImages: defects.missingImage || 0,
    pendingFacts: gov.pendingFacts ?? govSnap.pendingFacts ?? 0,
    fddFacts: govSnap.fddFacts ?? 0,
    internalFacts: govSnap.internalFacts ?? 0,
    carryoverCount: carryoverDefects.length,
    governedPlatformReady: Boolean(brandResult?.governedPlatformReady),
    sourceEvidenceNeeded: Boolean(gov.sourceEvidenceNeeded),
    factApprovalNeeded: Boolean(gov.factApprovalNeeded),
    imageApprovalNeeded,
    activeProfileReady: Boolean(brandResult?.readyForActiveProfile),
    readinessBand: brandResult?.readinessBand || "unknown",
    nextRecommendedWriter: brandResult?.nextRequiredWriter || "",
    visualQaScore: brandResult?.visualQaStatus?.score ?? null,
    blockerBuckets: buckets,
    primaryBlockerBucket: brandResult ? primaryBlockerBucket(buckets) : "unknown",
    easeScore: brandResult ? computeEaseScore(brandResult) : -999,
    applySafe: Boolean(brandResult?.applySafety?.safeForApplyApproved),
    applyBlockReasons: brandResult?.applySafety?.applyBlockReasons || [],
    liveApi,
    expansionReviewQueue,
  };

  return row;
}

function buildRecommendations(ranked) {
  const candidates = ranked.filter((r) => !r.activeProfileReady && r.completeBuildLoaded);
  const nextBrand = candidates[0] || null;

  const activeBlocked = candidates.filter((r) => r.cohort === "active_blocked");
  const wave1 = candidates.filter((r) => r.cohort === "wave1_expansion");

  const anyCarryover = candidates.some((r) => r.carryoverCount > 0);
  const anyHeavyGovernance = candidates.some((r) => r.pendingFacts > 20 || r.fddFacts > 20);
  const allActiveContract100 = activeBlocked.every((r) => r.contractReady);
  const sharedVisualWriter = candidates.every((r) =>
    /v24C_source_evidence|source capture|row creation/i.test(r.nextRecommendedWriter)
  );
  const sharedThinCopy = candidates.every((r) => r.titleOnlyOrThin >= 3 || r.highDefects >= 2);

  const bestActive = activeBlocked[0] || null;
  const bestWave1 = wave1[0] || null;

  let nextWriterToBuild = "v24C_source_evidence_work";
  if (nextBrand?.cohort === "wave1_expansion") {
    if (nextBrand.contractScore < 100) {
      nextWriterToBuild =
        /source capture/i.test(nextBrand.nextRecommendedWriter)
          ? "brand-explorer-required-section-source-capture-package + row creation writers for Wave 1"
          : "required-section source capture + row creation writers (Wave 1 contract gap first)";
    } else {
      nextWriterToBuild = nextBrand.nextRecommendedWriter || "v24C_source_evidence_work";
    }
  } else if (anyCarryover) {
    nextWriterToBuild =
      "v26A_copy_carryover_cleanup (Ascend/Radisson Blu carryover) then v24C_source_evidence_work visual/thin-copy batch";
  } else {
    nextWriterToBuild = "v24C_source_evidence_work (shared Value Creation / thin-copy visual repair)";
  }

  const multiBrandRepairSafe =
    sharedVisualWriter &&
    sharedThinCopy &&
    !anyHeavyGovernance &&
    allActiveContract100 &&
    activeBlocked.length > 0;

  const multiBrandApplyApprovedSafe = candidates.some((r) => r.applySafe);

  return {
    nextBrand: nextBrand
      ? {
          slug: nextBrand.slug,
          name: nextBrand.brand.name,
          recordId: nextBrand.brand.recordId,
          cohort: nextBrand.cohort,
          easeScore: nextBrand.easeScore,
          finalQaScore: nextBrand.finalQaScore,
          contractScore: nextBrand.contractScore,
          primaryBlockerBucket: nextBrand.primaryBlockerBucket,
          rationale: [
            `Highest ease score (${nextBrand.easeScore}) among ${candidates.length} compared brands`,
            nextBrand.contractReady
              ? "required-section contract already 100"
              : `contract still ${nextBrand.contractScore}/100 — Wave 1 gap work remains`,
            nextBrand.governedPlatformReady
              ? "governed platform ready"
              : "governance still open",
            nextBrand.pendingFacts <= 5
              ? `low pending-fact load (${nextBrand.pendingFacts})`
              : `pending facts elevated (${nextBrand.pendingFacts})`,
            `primary blocker: ${nextBrand.primaryBlockerBucket}`,
          ].join("; "),
        }
      : null,
    bestActiveBlocked: bestActive
      ? { slug: bestActive.slug, name: bestActive.brand.name, easeScore: bestActive.easeScore }
      : null,
    bestWave1Expansion: bestWave1
      ? { slug: bestWave1.slug, name: bestWave1.brand.name, easeScore: bestWave1.easeScore }
      : null,
    nextWriterToBuild,
    multiBrandRepairSafe,
    multiBrandApplyApprovedSafe,
    recommendationSummary:
      nextBrand?.cohort === "active_blocked"
        ? "Finish a blocked active brand before Wave 1 expansion — contract 100 + governed platform advantage."
        : nextBrand?.cohort === "wave1_expansion"
          ? "Wave 1 expansion brand leads among nine, but only if intentional expansion pilot is preferred over finishing active six."
          : "No eligible brand loaded for ranking.",
  };
}

export function buildBrandExplorerNextBrandSelectionAuditMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Next Brand Selection Audit ${report.auditVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brand target resolver: **${report.brandTargetResolverVersion}**`);
  lines.push(`- Brands compared: **${report.brandsCompared}**`);
  lines.push(`- Recommended next brand: **${report.recommendations.nextBrand?.name || "none"}**`);
  lines.push(`- Multi-brand repair safe: **${report.recommendations.multiBrandRepairSafe ? "yes" : "no"}**`);
  lines.push(`- Apply-approved safe: **${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Resolver status (v28C)");
  lines.push("| Brand | Loaded | Record | Source | Error |");
  lines.push("| --- | --- | --- | --- | --- |");
  for (const row of report.resolverStatus) {
    lines.push(
      `| ${row.resolvedBrandName || row.inputTarget} | ${row.loaded ? "yes" : "no"} | \`${row.resolvedRecordId || "—"}\` | ${row.resolutionSource || "—"} | ${row.error || "—"} |`
    );
  }
  lines.push("");
  lines.push("## Ranked brands (easiest path to active-profile first)");
  lines.push("| Rank | Brand | Cohort | Contract | Final QA | Ease | Primary blocker | Next writer |");
  lines.push("| ---: | --- | --- | ---: | ---: | ---: | --- | --- |");
  for (const [idx, row] of report.rankedBrands.entries()) {
    lines.push(
      `| ${idx + 1} | ${row.brand.name} | ${row.cohort} | ${row.contractScore ?? "—"} | ${row.finalQaScore ?? "—"} | ${row.easeScore} | \`${row.primaryBlockerBucket}\` | \`${row.nextRecommendedWriter || "—"}\` |`
    );
  }
  lines.push("");
  for (const row of report.rankedBrands) {
    lines.push(`### ${row.brand.name} (\`${row.slug}\`)`);
    lines.push(`- Cohort: **${row.cohort}**`);
    lines.push(`- Contract: **${row.contractScore ?? "—"}** · Final QA: **${row.finalQaReadiness}** (${row.finalQaScore ?? "—"})`);
    lines.push(`- Critical/high/visual defects: ${row.criticalDefects}/${row.highDefects}/${row.totalVisualDefects}`);
    lines.push(`- Pending facts / FDD / internal: ${row.pendingFacts} / ${row.fddFacts} / ${row.internalFacts}`);
    lines.push(`- Carryover risk: ${row.carryoverCount}`);
    lines.push(`- Governed platform ready: ${row.governedPlatformReady ? "yes" : "no"}`);
    lines.push(`- Image approval needs: ${row.imageApprovalNeeded ? "yes" : "no"}`);
    lines.push(`- Active-profile ready: ${row.activeProfileReady ? "yes" : "no"}`);
    lines.push(`- Blocker buckets:`);
    for (const bucket of BLOCKER_BUCKETS_V28D) {
      const items = row.blockerBuckets[bucket] || [];
      if (items.length) lines.push(`  - **${bucket}**: ${items.length} item(s)`);
    }
    lines.push("");
  }
  lines.push("## Recommendations");
  lines.push(`- **Next single brand:** ${report.recommendations.nextBrand?.name || "none"} (\`${report.recommendations.nextBrand?.slug || "—"}\`)`);
  lines.push(`- **Best active blocked:** ${report.recommendations.bestActiveBlocked?.name || "—"}`);
  lines.push(`- **Best Wave 1 expansion:** ${report.recommendations.bestWave1Expansion?.name || "—"}`);
  lines.push(`- **Next writer to build:** ${report.recommendations.nextWriterToBuild}`);
  lines.push(`- **Multi-brand repair safe (dry-run batch):** ${report.recommendations.multiBrandRepairSafe ? "yes" : "no"}`);
  lines.push(`- **Apply-approved safe:** ${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}`);
  lines.push(`- ${report.recommendations.recommendationSummary}`);
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerNextBrandSelectionAuditReport(options = {}) {
  const expansionJson = readJsonIfExists("reports/brand-explorer-expansion-backlog-planner.json");
  const prioritizationJson = readJsonIfExists(
    "reports/brand-explorer-active-profile-readiness-prioritization-audit.json"
  );
  const expansionReviewBySlug = expansionReviewQueueBySlug(expansionJson);

  const ctx = await getBrandTargetResolverContext();
  const resolverStatus = [];
  const brandRows = [];

  for (const slug of TARGET_BRAND_SLUGS) {
    const resolutionTarget = await resolveBrandTarget(slug, ctx);
    const resolution = resolutionTarget.resolution || resolutionTarget;
    const brandResult = loadCompleteBuildBrandResult(slug);
    const cohort = ACTIVE_BLOCKED_SLUGS.includes(slug) ? "active_blocked" : "wave1_expansion";

    let liveApi = null;
    if (resolution.resolvedRecordId && /^rec/.test(resolution.resolvedRecordId)) {
      liveApi = await fetchBrandPresentationSummary(resolution.resolvedRecordId);
      await sleep(250);
    }

    const row = buildBrandAuditRow({
      slug,
      cohort,
      resolution,
      brandResult,
      liveApi,
      expansionReviewQueue: expansionReviewBySlug.get(slug) || null,
    });

    resolverStatus.push({
      inputTarget: slug,
      loaded: row.resolver.loaded,
      resolvedBrandName: row.resolver.resolvedBrandName,
      resolvedRecordId: row.resolver.resolvedRecordId,
      resolvedSlug: row.resolver.resolvedSlug,
      resolutionSource: row.resolver.resolutionSource,
      error: row.resolver.error,
      ambiguous: row.resolver.ambiguous,
    });

    brandRows.push(row);
  }

  const rankedBrands = [...brandRows]
    .filter((r) => r.completeBuildLoaded)
    .sort((a, b) => b.easeScore - a.easeScore);

  const rankedAll = [...brandRows].sort((a, b) => {
    if (a.completeBuildLoaded !== b.completeBuildLoaded) return a.completeBuildLoaded ? -1 : 1;
    return b.easeScore - a.easeScore;
  });

  const recommendations = buildRecommendations(rankedBrands);
  const nextSlug = recommendations.nextBrand?.slug || "radisson";

  return {
    auditVersion: AUDIT_VERSION,
    v28DExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    brandTargetResolverVersion: RESOLVER_VERSION,
    brandsCompared: TARGET_BRAND_SLUGS.length,
    activeBlockedCount: ACTIVE_BLOCKED_SLUGS.length,
    wave1ExpansionCount: WAVE1_EXPANSION_SLUGS.length,
    tributeActiveProfileReady: true,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    resolverStatus,
    allResolverLoaded: resolverStatus.every((r) => r.loaded),
    brandRows,
    rankedBrands,
    rankedAll,
    blockerBucketKeys: BLOCKER_BUCKETS_V28D,
    recommendations,
    prioritizationAuditSnapshot: prioritizationJson?.recommendations || null,
    expansionBacklogWave1: (expansionJson?.proposedWaves || []).find((w) => w.wave === 1) || null,
    exactNextCommand: `npm run brand-explorer-complete-build -- --brand ${nextSlug} --dry-run --target-quality active-profile`,
    exactCompareCommand:
      "npm run brand-explorer-complete-build -- --brands radisson,ascend,radisson-blu,kimpton,curio-collection,radisson-individuals-by-choice,tapestry-collection-by-hilton,autograph-collection,design-hotels --dry-run --target-quality active-profile",
    guardrails: {
      noAirtableWrites: true,
      noCompanyValidatedChanges: true,
      noFactApprovals: true,
      noImageMaterialization: true,
      noSourceStewardship: true,
    },
  };
}
