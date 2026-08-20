#!/usr/bin/env node
/**
 * ADP P0 Executive Metrics Foundation audit — offline, $0 provider spend.
 *   node scripts/run-adp-p0-executive-metrics-audit.mjs
 *   node scripts/run-adp-p0-executive-metrics-audit.mjs --property adp_waterstone_boca_raton
 */

import { readFileSync, writeFileSync, existsSync, readdirSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildExecutiveMetricsFoundation } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { validatePositionGoldSet } from "../lib/ai-demand-positioning/metrics/position-gold-set-validation.js";
import { classifyPositionFormat } from "../lib/ai-demand-positioning/metrics/position-extraction.js";

const args = process.argv.slice(2);
const propertyId = args.find((a, i) => args[i - 1] === "--property") || "adp_waterstone_boca_raton";

function loadLatestPeriod(pid) {
  const periods = loadAllPeriods(pid);
  if (periods.length) return { period: periods[periods.length - 1], periodCount: periods.length };
  const seedDir = join(process.cwd(), "fixtures/ai-demand-positioning/seed-periods");
  if (existsSync(seedDir)) {
    const files = readdirSync(seedDir).filter((f) => f.includes(pid)).sort();
    if (files.length) {
      return {
        period: JSON.parse(readFileSync(join(seedDir, files[files.length - 1]), "utf-8")),
        periodCount: files.length,
      };
    }
  }
  return { period: null, periodCount: 0 };
}

function determineStatus(rankValidation) {
  const holdoutF1 = rankValidation.holdout.f1;
  if (holdoutF1 >= 85) return "ADP_P0_EXECUTIVE_METRICS_FOUNDATION_AND_EXPECTED_SHARE_DESIGN_PASS";
  if (holdoutF1 >= 60) return "ADP_P0_EXECUTIVE_METRICS_FOUNDATION_AND_EXPECTED_SHARE_DESIGN_PARTIAL";
  return "ADP_P0_EXECUTIVE_METRICS_FOUNDATION_AND_EXPECTED_SHARE_DESIGN_REMEDIATION_REQUIRED";
}

function determineNext(rankValidation) {
  if (rankValidation.holdout.f1 < 85) return "ADP_RANK_VALIDATION_NEEDS_WORK";
  return "ADP_EXPECTED_SHARE_NEEDS_BENCHMARK_RESEARCH";
}

function main() {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    console.error("Property not found:", propertyId);
    process.exit(1);
  }

  const { period, periodCount } = loadLatestPeriod(propertyId);
  if (!period) {
    console.error("No period found for", propertyId);
    process.exit(1);
  }

  const scenarios = buildScenarioUniverse(profile);
  const rankValidation = validatePositionGoldSet(profile);
  const foundation = buildExecutiveMetricsFoundation(period, scenarios, profile, { periodCount });

  const formatTypes = {};
  for (const obs of period.observations || []) {
    if (!obs.rawResponse) continue;
    const fmt = classifyPositionFormat(obs.rawResponse);
    formatTypes[fmt] = (formatTypes[fmt] || 0) + 1;
  }

  const report = {
    audit: "ADP_P0_EXECUTIVE_METRICS_FOUNDATION_AND_EXPECTED_SHARE_DESIGN",
    propertyId,
    periodId: period.periodId,
    generatedAt: new Date().toISOString(),
    providerCalls: 0,
    spend: 0,
    foundation,
    rankValidation,
    positionFormatTypesInCorpus: formatTypes,
    finalStatus: determineStatus(rankValidation),
    next: determineNext(rankValidation),
  };

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
  const outPath = join(outDir, "p0-executive-metrics-audit-v1.json");
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);
}

main();
