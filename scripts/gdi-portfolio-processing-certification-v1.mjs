#!/usr/bin/env node
/**
 * GDI Portfolio Processing Certification V1 — Node CLI.
 *
 *   node scripts/gdi-portfolio-processing-certification-v1.mjs --mode rehearsal
 *   node scripts/gdi-portfolio-processing-certification-v1.mjs --mode preview --hotels recA,recB
 *   node scripts/gdi-portfolio-processing-certification-v1.mjs --mode audit-only
 *
 * Defaults: $0 / no paid / no publication / no live Airtable / no weekly activation.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
  CERTIFICATION_COHORT_V1,
  PORTFOLIO_MODE,
  buildCapabilityAuditMatrix,
  assessFourteenSegmentCoverage,
  buildHotelCertificationScores,
  runPortfolioOrchestrator,
  recommendReadinessVerdict,
  getDefaultReportRoot,
  normalizeHotelList,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-processing-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}
function has(flag) {
  return process.argv.includes(flag);
}

function write(filePath, body) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(
    filePath,
    typeof body === "string" ? body : `${JSON.stringify(body, null, 2)}\n`,
    "utf8",
  );
  return filePath;
}

function parseHotels() {
  const fromFlag = arg("--hotels") || arg("--hotel-list");
  const fromFile = arg("--hotels-file");
  if (fromFile) {
    const raw = fs.readFileSync(path.resolve(ROOT, fromFile), "utf8");
    if (fromFile.endsWith(".json")) {
      const parsed = JSON.parse(raw);
      const list = Array.isArray(parsed)
        ? parsed
        : parsed.hotelIds || parsed.hotels || [];
      return list.map((x) => (typeof x === "string" ? x : x.hotelId));
    }
    return raw
      .split(/[\s,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  if (fromFlag) {
    return fromFlag
      .split(/[,;\s]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return CERTIFICATION_COHORT_V1.map((h) => h.hotelId);
}

const mode = (arg("--mode") || PORTFOLIO_MODE.REHEARSAL).toLowerCase();
const reportRoot = arg("--out") || getDefaultReportRoot();
const hotelIds = parseHotels();

if (has("--audit-only") || mode === "audit-only" || mode === "auditonly") {
  const audit = buildCapabilityAuditMatrix();
  const segments = assessFourteenSegmentCoverage();
  const scores = buildHotelCertificationScores(hotelIds);
  const verdict = recommendReadinessVerdict({
    hotelScores: scores,
    segmentCoverage: segments,
    portfolioSummary: {
      safety: { paidResearchExecuted: false },
      passedCount: scores.filter((s) => s.eligibilityOk).length,
      failedCount: scores.filter((s) => !s.eligibilityOk).length,
    },
  });
  write(path.join(reportRoot, "capability-audit-matrix.json"), audit);
  write(path.join(reportRoot, "fourteen-segment-coverage.json"), segments);
  write(path.join(reportRoot, "hotel-certification-scores.json"), scores);
  write(path.join(reportRoot, "readiness-verdict.json"), verdict);
  console.log(
    JSON.stringify(
      {
        ok: true,
        contract: GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
        mode: "audit-only",
        verdict: verdict.verdict,
        reportRoot,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

const list = normalizeHotelList(hotelIds);
if (!list.ok || list.count === 0) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "INVALID_HOTEL_LIST",
        invalid: list.invalid,
        duplicates: list.duplicates,
      },
      null,
      2,
    ),
  );
  process.exit(2);
}

const authorizePaidResearch = has("--authorize-paid-research");
const authorizePublication = has("--authorize-publication");
const authorizeProductionWrites = has("--authorize-production-writes");
const budgetCeilingUsd = Number(arg("--budget-usd") || 0) || 0;
const perHotelBudgetUsd = Number(arg("--per-hotel-budget-usd") || 0) || 0;

// Certification CLI refuses paid/publish unless --allow-live-pilot (founder only).
if (
  (authorizePaidResearch ||
    authorizePublication ||
    authorizeProductionWrites ||
    budgetCeilingUsd > 0) &&
  !has("--allow-live-pilot")
) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "LIVE_PILOT_NOT_ALLOWED",
        message:
          "Paid research, publication, production writes, and nonzero budget require --allow-live-pilot after explicit founder approval. This certification run stays $0.",
      },
      null,
      2,
    ),
  );
  process.exit(2);
}

const result = await runPortfolioOrchestrator({
  mode:
    mode === "full-e2e" || mode === "fulle2e"
      ? PORTFOLIO_MODE.FULL_E2E
      : mode === "preview"
        ? PORTFOLIO_MODE.PREVIEW
        : mode === "resume"
          ? PORTFOLIO_MODE.RESUME
          : PORTFOLIO_MODE.REHEARSAL,
  hotelIds: list.hotelIds,
  reportRoot,
  portfolioRunId: arg("--portfolio-run-id") || undefined,
  resume: has("--resume") || mode === "resume",
  statePath: arg("--state"),
  forceZeroCost: !has("--allow-live-pilot"),
  authorizePaidResearch: has("--allow-live-pilot") && authorizePaidResearch,
  authorizePublication: has("--allow-live-pilot") && authorizePublication,
  authorizeProductionWrites:
    has("--allow-live-pilot") && authorizeProductionWrites,
  budgetCeilingUsd: has("--allow-live-pilot") ? budgetCeilingUsd : 0,
  perHotelBudgetUsd: has("--allow-live-pilot") ? perHotelBudgetUsd : 0,
  maxCandidates: Number(arg("--max-candidates") || 8) || 8,
});

const scores = buildHotelCertificationScores(list.hotelIds);
const verdict = recommendReadinessVerdict({
  portfolioSummary: result,
  hotelScores: scores,
  segmentCoverage: result.segmentCoverage,
});

write(path.join(reportRoot, "hotel-certification-scores.json"), scores);
write(path.join(reportRoot, "readiness-verdict.json"), verdict);
write(path.join(reportRoot, "FOUNDER_REPORT.json"), {
  contract: GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
  generatedAt: new Date().toISOString(),
  currentRunnerInventory: result.capabilityAudit?.runners || null,
  canonicalExecutionPath: {
    node: "scripts/gdi-canonical-full-e2e-entry-v1.mjs",
    powershellSingleHotel: "scripts/gdi-workflow.ps1 -Action FullRun -DryRun",
    powershellPortfolio: "scripts/Invoke-GdiPortfolioProcessing.ps1",
  },
  certificationGaps: verdict.blockers,
  standardizedPowerShellCommand:
    ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile .\\reports\\gdi-portfolio-processing-certification-v1\\cert-cohort-hotels.txt",
  multiHotelExecution: {
    portfolioRunId: result.portfolioRunId,
    passedCount: result.passedCount,
    failedCount: result.failedCount,
    spentUsd: result.spentUsd,
  },
  fourteenSegmentCoverage: result.segmentCoverage?.summary || null,
  commercialQuality: scores,
  budgetAndPublicationSafety: result.safety,
  repeatedRunAndWeeklyChange: result.weeklyChangeDemo,
  hotelByHotelScores: scores,
  remainingBlockers: verdict.blockers,
  paidPilotApprovalRequired: [
    "FOUNDER_GO_PAID_RESEARCH with explicit BudgetUSD",
    "Independent FOUNDER_GO_PUBLICATION if customer delivery intended",
    "Independent research-persistence exact write-set + env gate if Airtable apply intended",
    "No weekly automation activation without separate GO",
  ],
  readinessVerdict: verdict.verdict,
  rationale: verdict.rationale,
});

console.log(
  JSON.stringify(
    {
      ok: result.ok,
      contract: GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
      portfolioRunId: result.portfolioRunId,
      mode: result.mode,
      passedCount: result.passedCount,
      failedCount: result.failedCount,
      spentUsd: result.spentUsd,
      readinessVerdict: verdict.verdict,
      reportRoot: result.reportRoot,
      statePath: result.statePath,
    },
    null,
    2,
  ),
);

process.exit(result.ok ? 0 : 1);
