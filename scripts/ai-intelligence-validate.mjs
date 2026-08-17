#!/usr/bin/env node
/**
 * Deterministic AI Intelligence validation — store artifacts only.
 * LIVE_PROVIDER_CALLS: 0
 */
import { runAiIntelligenceValidation } from "../lib/ai-visibility/validation/run-validation.js";

const report = await runAiIntelligenceValidation({ writeFiles: true });
const s = report.summary;

console.log("\n=== Dealality AI Intelligence Validation ===\n");
console.log(`Generated: ${report.generatedAt}`);
console.log(`Batches: ${s.BATCHES_VALIDATED} (publishable: ${s.PUBLISHABLE_BATCHES})`);
console.log(
  `Metrics: ${s.METRICS_RECONCILED}/${s.METRICS_CHECKED} reconciled` +
    (s.RECONCILIATION_RATE != null
      ? ` (${Math.round(s.RECONCILIATION_RATE * 1000) / 10}%)`
      : "")
);
console.log(`Impossible metrics: ${s.IMPOSSIBLE_METRICS_FOUND}`);
console.log(`Broken evidence IDs: ${s.BROKEN_EVIDENCE_IDS}`);
console.log(`Provider leakage: ${s.PROVIDER_LEAKAGE_CASES}`);
console.log(`Geography leakage: ${s.GEOGRAPHY_LEAKAGE_CASES}`);
console.log(`Language leakage: ${s.LANGUAGE_LEAKAGE_CASES}`);
console.log(`Question reconciliation failures: ${s.QUESTION_RECONCILIATION_FAILURES}`);
console.log(`Fixture contamination: ${s.FIXTURE_CONTAMINATION_CASES}`);
console.log(`\nGolden Set: ${report.goldenSet?.GOLDEN_SET_VERSION} n=${report.goldenSet?.CASE_COUNT}`);
console.log(`Recommendation: ${report.recommendation?.status}`);
console.log(`Detail: ${report.recommendation?.detail}`);
console.log(`\nMethodology:\n${report.methodologyNote}`);
console.log(`\nWrote: data/ai-visibility/validation/latest-validation-report.json`);
console.log(`Issues: ${(report.issues || []).length}`);

if ((report.issues || []).length) {
  console.log("\n--- Issues (first 25) ---");
  for (const issue of report.issues.slice(0, 25)) {
    console.log(
      `- ${issue.type} batch=${issue.batchId || "?"} metric=${issue.metricId || "-"} expected=${issue.expected} actual=${issue.actual}`
    );
  }
}

process.exit(0);
