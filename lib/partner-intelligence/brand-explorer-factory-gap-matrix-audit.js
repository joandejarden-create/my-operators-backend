/**
 * Brand Explorer Factory Gap Matrix + Generalization Audit v27A (read-only).
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
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { fetchLiveState } from "./tribute-portfolio-package-pipeline.js";

export const AUDIT_VERSION = "v27A";
export const REPORT_JSON_NAME = "brand-explorer-factory-gap-matrix-audit.json";
export const REPORT_MD_NAME = "brand-explorer-factory-gap-matrix-audit.md";
export const DOC_MD_NAME = "brand-explorer-factory-gap-matrix-audit-v27A.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "reports/brand-explorer-complete-build-batch.md",
  "reports/brand-explorer-complete-build-batch.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-factory-gap-matrix-audit.js",
  "scripts/brand-explorer-factory-gap-matrix-audit.mjs",
  "docs/data-intelligence/brand-explorer-factory-gap-matrix-audit-v27A.md",
  "reports/brand-explorer-factory-gap-matrix-audit.md",
  "reports/brand-explorer-factory-gap-matrix-audit.json",
  "package.json",
];

const BLOCKED_CONTRACT_SECTIONS = [
  "Portfolio Context",
  "Standard Detail / Where Available",
  "Demand Scenario View",
];

const HARDCODE_SCAN_FILES = [
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-tribute-standard-detail-review-approval-writer.js",
  "lib/partner-intelligence/brand-explorer-tribute-geographic-footprint-refinement-writer.js",
  "lib/partner-intelligence/tribute-portfolio-package-pipeline.js",
];

const HARDCODE_PATTERNS = [
  { id: "tribute_brand_id", pattern: /recCvV0PuZOi8c3hC/, label: "Hardcoded Tribute record ID" },
  { id: "tribute_slug", pattern: /tribute-portfolio/, label: "Hardcoded tribute-portfolio slug" },
  { id: "marriott_string", pattern: /Marriott/i, label: "Marriott-specific string" },
  { id: "bonvoy_string", pattern: /Bonvoy/i, label: "Bonvoy-specific string" },
  { id: "fdd_string", pattern: /\bFDD\b|franchise disclosure/i, label: "FDD-specific rule" },
  { id: "tribute_expected_remaining", pattern: /tributeExpectedRemaining|EXPECTED_TRIBUTE_REMAINING/, label: "Tribute-only expected remaining blockers" },
  { id: "tribute_writer_import", pattern: /brand-explorer-tribute-/, label: "Tribute-prefixed writer reference" },
  { id: "tribute_package_pipeline", pattern: /tribute-portfolio-package-pipeline/, label: "Tribute-only package pipeline" },
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readUtf8(relPath) {
  try {
    return fs.readFileSync(path.join(ROOT, relPath), "utf8");
  } catch {
    return "";
  }
}

function readJsonIfExists(relPath) {
  try {
    return JSON.parse(readUtf8(relPath));
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, { attempts = 3, delayMs = 10000 } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (!/rate limit|429/i.test(nz(error?.message)) || attempt === attempts) throw error;
      await sleep(delayMs * attempt);
    }
  }
  throw lastError;
}

async function fetchBrandApiShape(brandId) {
  const req = { query: { brandId, refresh: "1" }, headers: {} };
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

function resolveTargets(options = {}) {
  if (options.allActive) return [...ACTIVE_BRAND_AUDIT_TARGETS];
  const slug = nz(options.brandIdOrName || "tribute-portfolio");
  const found = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === slug || b.recordId === slug);
  return found ? [found] : [{ slug, recordId: slug, name: slug }];
}

function classifyBlocker(blocker, sectionRow = null) {
  const section = blocker.section || blocker.type || "";
  const message = nz(blocker.message);
  const classification = nz(blocker.classification || sectionRow?.classification);

  if (blocker.type === "brand_carryover") {
    return { classification: "copy_carryover_risk", rationale: "Wrong-brand copy/carryover pattern detected" };
  }
  if (section === "Portfolio Context" || /Marriott-specific/i.test(message)) {
    return {
      classification: "tribute_calibrated_false_negative",
      rationale: "Portfolio Context uses Marriott-ladder proxy; fails Hilton/Choice brands with valid parent context",
    };
  }
  if (
    section === "Standard Detail / Where Available" ||
    /tribute-standard-detail|governance_review_state/i.test(message + classification)
  ) {
    return {
      classification: "tribute_calibrated_false_negative",
      rationale: "Standards readiness uses Tribute v25C-5C governance matcher, not brand-agnostic approval",
    };
  }
  if (section === "Demand Scenario View") {
    const count = sectionRow?.currentCount ?? null;
    if (count != null && count > 0) {
      return {
        classification: "real_content_gap",
        rationale: `Demand rows present (${count}) but fail Tribute completion shape (caseSummaryOwnerObjective)`,
      };
    }
    return {
      classification: "contract_rule_too_strict",
      rationale: "Demand minimum may be valid but completion predicate is Tribute-field shaped",
    };
  }
  if (blocker.type === "visual_defect" || blocker.category === "frontend") {
    if (/thin_copy_vs_reference|cosmeticNonBlocking/i.test(message)) {
      return { classification: "report_only_stale_flag", rationale: "Cosmetic visual comparison vs Curio" };
    }
    return { classification: "real_visual_gap", rationale: "Visual defect audit flagged display/content gap" };
  }
  if (blocker.category === "founder_legal_review" || /founder|legal/i.test(message)) {
    return { classification: "real_governance_gap", rationale: "Founder/legal review gate not satisfied" };
  }
  if (blocker.category === "source" || /source|fact approval|pending/i.test(message)) {
    return { classification: "real_governance_gap", rationale: "Source/fact governance incomplete" };
  }
  return { classification: "real_content_gap", rationale: "Required section or content minimum not met" };
}

function scanHardcodedAssumptions() {
  const findings = [];
  for (const relPath of HARDCODE_SCAN_FILES) {
    const text = readUtf8(relPath);
    if (!text) continue;
    const lines = text.split(/\r?\n/);
    for (const pat of HARDCODE_PATTERNS) {
      lines.forEach((line, index) => {
        if (!pat.pattern.test(line)) return;
        findings.push({
          file: relPath,
          line: index + 1,
          patternId: pat.id,
          label: pat.label,
          excerpt: line.trim().slice(0, 140),
        });
      });
    }
  }
  return findings;
}

function presentationSummary(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const slotKeys = [...new Set(blocks.map((b) => nz(b.slotKey)).filter(Boolean))].sort();
  return {
    presentationRowCount: blocks.length,
    uniqueSlotKeys: slotKeys.length,
    slotKeysSample: slotKeys.slice(0, 25),
  };
}

function rankBlockedBrands(brandMatrices) {
  const blocked = brandMatrices.filter((b) => !b.activeProfileReady && b.slug !== "tribute-portfolio");
  const scored = blocked.map((b) => {
    const gapTo100 = 100 - (b.contractScore || 0);
    const finalQaGap = 85 - (b.finalQaNumeric || 0);
    const carryoverPenalty = (b.carryoverDefects || 0) * 15;
    const governanceBonus = b.governedPlatformReady ? 20 : 0;
    const pendingPenalty = Math.min(30, (b.pendingFacts || 0) * 0.2);
    const visualBonus = (b.visualScore || 0) * 0.3;
    const nonMarriottBonus = /hilton|choice/i.test(b.parentCompany || b.name) ? 10 : 0;
    const score =
      100 -
      gapTo100 * 0.4 -
      finalQaGap * 0.3 -
      carryoverPenalty -
      pendingPenalty +
      governanceBonus +
      visualBonus +
      nonMarriottBonus -
      (b.criticalDefects || 0) * 8 -
      (b.highDefects || 0) * 3;
    return { ...b, factoryRankScore: Math.round(score) };
  });
  scored.sort((a, b) => b.factoryRankScore - a.factoryRankScore);
  return scored;
}

async function auditBrand(target, buildReport) {
  const buildJson = readJsonIfExists(`reports/brand-explorer-complete-build-${target.slug}.json`);
  const liveState = await fetchLiveState(target.recordId).catch(() => ({
    recordId: target.recordId,
    sources: [],
    facts: [],
    brandBasics: null,
  }));
  const brandApi = await fetchBrandApiShape(target.recordId);
  const contractReport = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: target.recordId,
  });
  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  });
  const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: target.recordId,
  });

  const qaBrand = (finalQaReport.brandReports || []).find(
    (b) => b.brand?.slug === target.slug || b.brand?.recordId === target.recordId
  ) || finalQaReport.brandReports?.[0];

  const sectionRows = contractReport.sectionBySectionReadiness || [];
  const failingSections = sectionRows.filter((s) => !String(s.classification).startsWith("ready"));
  const blockers = buildJson?.blockers || [];
  const classifiedBlockers = blockers.map((blocker) => {
    const sectionRow = sectionRows.find((s) => s.section === blocker.section);
    return {
      ...blocker,
      ...classifyBlocker(blocker, sectionRow),
      sectionClassification: sectionRow?.classification || null,
      sectionCurrentCount: sectionRow?.currentCount ?? null,
    };
  });

  const explorerFacts = (liveState.facts || []).filter(
    (f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")
  );
  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");

  return {
    slug: target.slug,
    name: target.name,
    recordId: target.recordId,
    parentCompany: nz(brandApi?.parentCompany),
    contractScore: contractReport.readinessScore,
    contractReady: contractReport.brandExplorerRequiredSectionsReady,
    sectionsReady: sectionRows.filter((s) => String(s.classification).startsWith("ready")).length,
    sectionsTotal: sectionRows.length,
    failingSections: failingSections.map((s) => ({
      section: s.section,
      classification: s.classification,
      currentCount: s.currentCount,
      requiredMinimum: s.requiredMinimum,
      blocker: s.blockerIfNotSafe,
    })),
    finalQaStatus: qaBrand?.scores?.overallActiveProfileReadiness || finalQaReport.scores?.overallActiveProfileReadiness,
    finalQaNumeric: qaBrand?.scores?.overallNumeric || finalQaReport.scores?.overallNumeric,
    activeProfileReady: Boolean(buildJson?.readyForActiveProfile),
    criticalDefects: qaBrand?.defectsBySeverity?.critical?.length || 0,
    highDefects: qaBrand?.defectsBySeverity?.high?.length || 0,
    mediumDefects: qaBrand?.defectsBySeverity?.medium?.length || 0,
    visualDefects: visualReport.defectCounts?.total || 0,
    visualScore: visualReport.visualComparability?.score || 0,
    carryoverDefects: (qaBrand?.defects || []).filter((d) => d.type === "brand_carryover").length,
    governanceDefects: (qaBrand?.defects || []).filter((d) => d.category === "source" || /governance/i.test(d.type)).length,
    governedPlatformReady: Boolean(buildJson?.governedPlatformReady),
    pendingFacts: pendingFacts.length,
    approvedExplorerSources: (liveState.sources || []).filter((s) => nz(s.approvedForExplorerUse) === "Yes").length,
    companyValidatedUntouched: true,
    companyValidatedSnapshot: liveState.brandBasics?.fields?.["Company Validated"] ?? null,
    presentation: presentationSummary(brandApi),
    classifiedBlockers,
    contractBrandRecordIdReported: contractReport.brand?.recordId,
    contractBrandNameReported: contractReport.brand?.name,
  };
}

function buildSharedFailureAnalysis(brandMatrices) {
  const nonTribute = brandMatrices.filter((b) => b.slug !== "tribute-portfolio");
  const blocked = nonTribute.filter((b) => !b.contractReady);
  const sectionFailCounts = {};
  for (const brand of blocked) {
    for (const section of brand.failingSections) {
      sectionFailCounts[section.section] = sectionFailCounts[section.section] || { count: 0, brands: [] };
      sectionFailCounts[section.section].count += 1;
      sectionFailCounts[section.section].brands.push(brand.slug);
    }
  }
  const scores = nonTribute.map((b) => b.contractScore);
  const uniformLegacy63 = scores.length > 0 && scores.every((s) => s === 63);
  return {
    blockedBrandCount: blocked.length,
    uniformContractScore: uniformLegacy63,
    uniformLegacy63PatternResolved: !uniformLegacy63,
    contractScoresByBrand: nonTribute.map((b) => ({ slug: b.slug, contractScore: b.contractScore })),
    sectionsReadyWhenBlocked: blocked[0]?.sectionsReadyCount ?? null,
    sectionsTotal: 8,
    scoreFormula: uniformLegacy63
      ? "round(readySections / 8 * 100) => 5/8 = 63"
      : "round(readySections / 8 * 100) — v27B generalized evaluators active",
    sharedFailingSections: Object.entries(sectionFailCounts)
      .sort((a, b) => b[1].count - a[1].count)
      .map(([section, data]) => ({ section, failCount: data.count, brands: data.brands })),
    interpretation: uniformLegacy63
      ? "All five non-Tribute brands fail the same three contract sections (Portfolio Context, Standard Detail, Demand Scenario), yielding identical 5/8 = 63 scores."
      : blocked.length
        ? `Post-v27B: ${blocked.length} non-Tribute brand(s) still below contract 100. Shared failures: ${Object.keys(sectionFailCounts).join(", ") || "none"}.`
        : "Post-v27B: all non-Tribute active brands meet required-section contract minimums.",
  };
}

function summarizeRealVsFalse(classifiedAcrossBrands) {
  const totals = {};
  for (const item of classifiedAcrossBrands) {
    totals[item.classification] = (totals[item.classification] || 0) + 1;
  }
  return totals;
}

function buildRecommendations(brandMatrices, hardcodedFindings, batchJson) {
  const ranked = rankBlockedBrands(brandMatrices);
  const nextBrand = ranked[0] || null;
  const contractBlocked = brandMatrices.filter((b) => b.slug !== "tribute-portfolio" && !b.contractReady);
  const falseNegativeCount = brandMatrices
    .flatMap((b) => b.classifiedBlockers)
    .filter((b) => b.classification === "tribute_calibrated_false_negative").length;
  const applySafeCount = batchJson?.batchAggregate?.brandsSafeForApplyApproved?.length || 0;
  const v27BApplied = contractBlocked.length < 5;

  return {
    generalizeContractFirst: !v27BApplied,
    generalizeContractRationale: v27BApplied
      ? "v27B contract generalization applied — remaining gaps are real content/governance/visual blockers."
      : "Three of three shared contract failures are Tribute/Marriott-calibrated (portfolio ladder, standards governance, demand completion shape). Generalize before second-brand end-to-end apply.",
    runSecondBrandEndToEnd: v27BApplied && contractBlocked.length <= 2,
    runSecondBrandEndToEndRationale: v27BApplied
      ? "Contract gates brand-agnostic; pilot next brand on visual/QA writers after standards governance where needed."
      : "Run second brand dry-run pilot only after contract v27B generalization; avoid apply until contract and QA gates are brand-agnostic.",
    nextWriterRecommended: v27BApplied
      ? contractBlocked.some((b) => b.failingSections?.some((s) => /Standard Detail/i.test(s.section)))
        ? "brand-explorer-standard-detail-governance-writer"
        : "brand-explorer-complete-build-orchestrator (visual/QA batch)"
      : "brand-explorer-required-section-contract-generalization-writer (v27B)",
    nextWriterTasks: [
      "Replace Marriott-specific portfolio context gate with parent-company ladder resolver (Marriott/Hilton/Choice)",
      "Extract brand-agnostic standards approval evaluator from tribute-standard-detail-review-approval-writer",
      "Relax demand row completion to brand-neutral owner-implication fields",
      "Stop emitting Tribute recordId/name in contract report.brand for non-Tribute loads",
    ],
    multiBrandApplyApprovedSafe: applySafeCount > 0,
    multiBrandApplyApprovedRationale:
      applySafeCount > 0
        ? `${applySafeCount} brand(s) passed apply safety gates`
        : "No brands passed apply-approved safety; batch aggregate reports 0 safe for apply-approved",
    rankedNextBrands: ranked.map((b, index) => ({
      rank: index + 1,
      slug: b.slug,
      name: b.name,
      factoryRankScore: b.factoryRankScore,
      contractScore: b.contractScore,
      finalQaNumeric: b.finalQaNumeric,
      visualScore: b.visualScore,
      governedPlatformReady: b.governedPlatformReady,
      carryoverDefects: b.carryoverDefects,
      pendingFacts: b.pendingFacts,
      parentCompany: b.parentCompany,
    })),
    recommendedNextBrand: nextBrand
      ? {
          slug: nextBrand.slug,
          name: nextBrand.name,
          factoryRankScore: nextBrand.factoryRankScore,
          why:
            "Best balance of reference completeness, non-Marriott parent (if Hilton/Choice), governance cleanliness, and carryover risk after contract generalization",
        }
      : null,
    hardcodedAssumptionCount: hardcodedFindings.length,
    tributeCalibratedBlockerCount: falseNegativeCount,
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Factory Gap Matrix Audit ${AUDIT_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **no**`);
  lines.push(`Company Validated untouched: **yes**`);
  lines.push("");
  lines.push("## Why five brands score Contract 63");
  lines.push(`- ${report.sharedFailureAnalysis.interpretation}`);
  lines.push(`- Formula: ${report.sharedFailureAnalysis.scoreFormula}`);
  lines.push("");
  lines.push("### Shared failing sections");
  for (const row of report.sharedFailureAnalysis.sharedFailingSections) {
    lines.push(`- **${row.section}**: ${row.failCount}/${report.sharedFailureAnalysis.blockedBrandCount} brands`);
  }
  lines.push("");
  lines.push("## Gap matrix");
  for (const brand of report.gapMatrix) {
    lines.push(`### ${brand.name} (\`${brand.slug}\`)`);
    lines.push(`- Contract: **${brand.contractScore}** (${brand.sectionsReady}/${brand.sectionsTotal} ready)`);
    lines.push(`- Final QA: **${brand.finalQaStatus}** (${brand.finalQaNumeric})`);
    lines.push(`- Active-profile ready: **${brand.activeProfileReady ? "yes" : "no"}**`);
    lines.push(`- Critical/high defects: ${brand.criticalDefects}/${brand.highDefects}`);
    lines.push(`- Visual defects: ${brand.visualDefects} (score ${brand.visualScore})`);
    lines.push(`- Carryover defects: ${brand.carryoverDefects}`);
    lines.push(`- Governance defects: ${brand.governanceDefects}; governed ready: ${brand.governedPlatformReady ? "yes" : "no"}`);
    lines.push(`- Presentation rows: ${brand.presentation.presentationRowCount} (${brand.presentation.uniqueSlotKeys} slot keys)`);
    lines.push(`- Failing sections:`);
    if (!brand.failingSections.length) lines.push("  - none");
    else for (const s of brand.failingSections) lines.push(`  - ${s.section}: \`${s.classification}\` (${s.currentCount}/${s.requiredMinimum})`);
    lines.push("");
  }
  lines.push("## Real blockers vs false positives");
  for (const [kind, count] of Object.entries(report.blockerClassificationTotals)) {
    lines.push(`- ${kind}: ${count}`);
  }
  lines.push("");
  lines.push("## Recommended next brand");
  if (report.recommendations.recommendedNextBrand) {
    const nb = report.recommendations.recommendedNextBrand;
    lines.push(`- **${nb.name}** (\`${nb.slug}\`) — rank score ${nb.factoryRankScore}`);
    lines.push(`- ${nb.why}`);
  }
  lines.push("");
  lines.push("## Recommended next build path");
  lines.push(`- Generalize contract first: **${report.recommendations.generalizeContractFirst ? "yes" : "no"}**`);
  lines.push(`- ${report.recommendations.generalizeContractRationale}`);
  lines.push(`- Next writer: **${report.recommendations.nextWriterRecommended}**`);
  for (const task of report.recommendations.nextWriterTasks) lines.push(`  - ${task}`);
  lines.push(`- Multi-brand apply-approved safe: **${report.recommendations.multiBrandApplyApprovedSafe ? "yes" : "no"}**`);
  lines.push(`- ${report.recommendations.multiBrandApplyApprovedRationale}`);
  lines.push("");
  lines.push("## Hardcoded assumption scan (sample)");
  for (const finding of report.hardcodedAssumptions.slice(0, 20)) {
    lines.push(`- \`${finding.file}:${finding.line}\` — ${finding.label}`);
  }
  if (report.hardcodedAssumptions.length > 20) {
    lines.push(`- … and ${report.hardcodedAssumptions.length - 20} more`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerFactoryGapMatrixAuditReport(options = {}) {
  const targets = resolveTargets(options);
  const batchJson = readJsonIfExists("reports/brand-explorer-complete-build-batch.json");
  const hardcodedAssumptions = scanHardcodedAssumptions();

  const gapMatrix = [];
  for (const target of targets) {
    const row = await withRetry(() => auditBrand(target, true));
    gapMatrix.push(row);
    await sleep(1200);
  }

  const classifiedAcrossBrands = gapMatrix.flatMap((b) => b.classifiedBlockers);
  const sharedFailureAnalysis = buildSharedFailureAnalysis(gapMatrix);
  const recommendations = buildRecommendations(gapMatrix, hardcodedAssumptions, batchJson);

  const report = {
    auditVersion: AUDIT_VERSION,
    v27AExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    allActive: Boolean(options.allActive),
    brandsAudited: targets.map((t) => t.slug),
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    gapMatrix,
    sharedFailureAnalysis,
    blockerClassificationTotals: summarizeRealVsFalse(classifiedAcrossBrands),
    hardcodedAssumptions,
    recommendations,
    tributeActiveProfileReady: gapMatrix.find((b) => b.slug === "tribute-portfolio")?.activeProfileReady ?? false,
    batchSnapshot: batchJson
      ? {
          generatedAt: batchJson.generatedAt,
          brandsReady: batchJson.batchAggregate?.brandsReady?.length || 0,
          brandsBlocked: batchJson.batchAggregate?.brandsBlocked?.length || 0,
          brandsSafeForApplyApproved: batchJson.batchAggregate?.brandsSafeForApplyApproved?.length || 0,
        }
      : null,
    exactCommand: "npm run brand-explorer-factory-gap-matrix-audit -- --all-active --dry-run",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerFactoryGapMatrixAuditMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
