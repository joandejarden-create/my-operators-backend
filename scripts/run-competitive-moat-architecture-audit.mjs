#!/usr/bin/env node
/**
 * Competitive Moat Architecture V1 — offline audit (no provider calls).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildCompetitiveMoatArchitectureReport } from "../lib/ai-visibility/competitive-moat/feasibility-audit.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const reportPath = path.join(
  __dirname,
  "..",
  "reports",
  "ai-visibility",
  "competitive-moat-architecture-v1.json"
);

const report = buildCompetitiveMoatArchitectureReport({
  operatorPresenceValidated: true,
});

fs.mkdirSync(path.dirname(reportPath), { recursive: true });
fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log("\nCompetitive Moat Architecture V1 Audit\n");
console.log(`  Canonical intents: ${report.canonicalIntent.totalCanonicalIntents}`);
console.log(`  Brand mappings: ${report.canonicalIntent.brandMappings}`);
console.log(`  Operator mappings: ${report.canonicalIntent.operatorMappings}`);
console.log(`  Index feasibility subjects: ${report.indexFeasibility.subjects.length}`);
console.log(`  VALID index: ${report.indexFeasibility.validCount}`);
console.log(`  LIMITED index: ${report.indexFeasibility.limitedCount}`);
console.log(`  SUPPRESSED index: ${report.indexFeasibility.suppressedCount}`);
console.log(`  Observed competitor subjects: ${report.observedCompetitorFeasibility.subjects.length}`);
console.log(`  Provider calls: ${report.providerCalls}`);
console.log(`  Index recommendation: ${report.indexNameRecommendation}`);
console.log(`\nWrote ${reportPath}\n`);
