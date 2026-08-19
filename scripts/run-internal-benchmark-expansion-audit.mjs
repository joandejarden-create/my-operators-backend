#!/usr/bin/env node
/**
 * Internal Benchmark Expansion Audit — offline, no provider calls.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runInternalBenchmarkExpansionAudit } from "../lib/ai-visibility/competitive-moat/internal-benchmark-expansion-audit.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const reportPath = path.join(
  __dirname,
  "..",
  "reports",
  "ai-visibility",
  "internal-benchmark-expansion-audit-v1.json"
);

const report = runInternalBenchmarkExpansionAudit();
fs.mkdirSync(path.dirname(reportPath), { recursive: true });
fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log("\nInternal Benchmark Expansion Audit V1\n");
console.log(`  Customer visible brands: ${report.customerVisibleBrands}`);
console.log(`  Current internal benchmark (peer v2): ${report.currentInternalBenchmarkCount}`);
console.log(`  Candidates audited: ${report.candidatesAudited}`);
console.log(`  Minimum recommended: ${report.minimumRecommendedSet.brands.join(", ")}`);
console.log(`  Preferred recommended: ${report.preferredRecommendedSet.brands.join(", ")}`);
console.log(`  Simulation CURRENT — VALID: ${report.benchmarkSimulation.current.valid} LIMITED: ${report.benchmarkSimulation.current.limited}`);
console.log(`  Simulation MINIMUM — VALID: ${report.benchmarkSimulation.minimumSet.valid} LIMITED: ${report.benchmarkSimulation.minimumSet.limited}`);
console.log(`  Simulation PREFERRED — VALID: ${report.benchmarkSimulation.preferredSet.valid} LIMITED: ${report.benchmarkSimulation.preferredSet.limited}`);
console.log(`  Provider calls: ${report.providerCalls}`);
console.log(`  Founder gate: ${report.founderGate.status}`);
console.log(`\nWrote ${reportPath}\n`);
