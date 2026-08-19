#!/usr/bin/env node
/**
 * Brand AI Intelligence — Longitudinal Foundation audit (dry-run).
 * Writes reports/ai-visibility/brand-longitudinal-foundation-audit.json
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildBrandLongitudinalFoundationReport } from "../lib/ai-visibility/brand-longitudinal/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outPath = path.join(__dirname, "..", "reports", "ai-visibility", "brand-longitudinal-foundation-audit.json");

const report = await buildBrandLongitudinalFoundationReport();
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

console.log("Brand Longitudinal Foundation Audit");
console.log("  PRIMARY_BASELINE_DATE:", report.baseline.PRIMARY_BASELINE_DATE);
console.log("  REAL_DISTINCT_PERIODS:", report.baseline.REAL_DISTINCT_PERIODS);
console.log("  CLIENT_STATE:", report.baseline.CLIENT_STATE);
console.log("  COHORT_PROMPTS:", report.cohort.promptCount);
console.log("  CALLS_PER_CYCLE:", report.cohort.callCount);
console.log("  HISTORIC_COST:", report.cost.historicExpectedCostUsd);
console.log("  READINESS:", report.readiness);
console.log("  INITIAL_WAVE:", report.initialWave.STATUS);
console.log("  Written:", outPath);

process.exit(report.readiness === "BRAND_LONGITUDINAL_FOUNDATION_READY" ? 0 : 1);
