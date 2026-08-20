#!/usr/bin/env node
/**
 * ADP property-specific CORE benchmark governance V1 — offline certification.
 *   npm run adp:property-specific-core-benchmark-governance-v1
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { runPropertySpecificCoreBenchmarkGovernanceV1 } from "../lib/ai-demand-positioning/metrics/property-specific-core-benchmark-governance-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/property-specific-core-benchmark-governance-v1.json");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

async function main() {
  const report = runPropertySpecificCoreBenchmarkGovernanceV1();
  const waterstoneAudit = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstoneAudit, WATERSTONE_BASELINE);

  report.waterstoneRegression = {
    CORE_DIFF: regression.INDEX_DIFF === 0 && regression.PHASE1_METRIC_DIFF === 0 ? 0 : regression.INDEX_DIFF,
    INDEX_DIFF: regression.INDEX_DIFF,
    PHASE1_METRIC_DIFF: regression.PHASE1_METRIC_DIFF,
    CERTIFIED_TERRITORIES: regression.CERTIFIED_TERRITORIES,
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));

  console.log(report.title);
  console.log("  final:", report.final);
  console.log("  next:", report.next);
  console.log("  CORE_TRUTH_READY territories:", report.summary.TOTAL_CORE_TRUTH_READY_TERRITORIES);
  console.log("  developing:", report.summary.TOTAL_BENCHMARK_DEVELOPING_TERRITORIES);
  console.log("  waterstone INDEX_DIFF:", report.waterstoneRegression.INDEX_DIFF);
  console.log("  report:", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
