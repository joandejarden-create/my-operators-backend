#!/usr/bin/env node
/**
 * Internal benchmark cohort integrity audit — no provider calls, no methodology change.
 */
import { runBenchmarkCohortIntegrityAudit } from "../lib/ai-visibility/competitive-moat/benchmark-cohort-integrity-audit.js";

const report = runBenchmarkCohortIntegrityAudit({ writeReport: true });
const a = report.autographDeepDive;
const c = report.curioDeepDive;

console.log("\nBenchmark Cohort Integrity Audit V1\n");
console.log(`  Customer-visible subjects: ${report.customerVisibleBrands}`);
console.log(`  Internal peer count: ${report.internalPeerCount}`);
console.log(`  Autograph index: ${a.currentIndex}  Curio included: ${a.curioIncluded}  trust: ${a.indexTrust}`);
console.log(`  Curio index: ${c.currentIndex}  Autograph included: ${c.autographIncluded}  trust: ${c.indexTrust}`);
console.log(`  Symmetry unjustified: ${report.symmetry.asymmetricUnjustified}`);
console.log(`  False benchmark confidence risk: ${report.expansionAudit.falseBenchmarkConfidenceRisk}`);
console.log(`  Next: ${report.next}`);
console.log(`  Provider calls: ${report.providerCalls}`);
console.log("\nWrote reports/ai-visibility/benchmark-cohort-integrity-audit-v1.json\n");
