#!/usr/bin/env node
/**
 * ADP Targeted Multi-Property Governed Benchmark Measurement V1.
 *   npm run adp:targeted-multi-property-governed-benchmark-measurement-v1
 *   npm run adp:targeted-multi-property-governed-benchmark-measurement-v1 -- --apply
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { runTargetedMultiPropertyGovernedBenchmarkMeasurementV1 } from "../lib/ai-demand-positioning/execution/targeted-multi-property-governed-benchmark-measurement-v1.js";

const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning/targeted-multi-property-governed-benchmark-measurement-v1.json"
);

async function main() {
  const apply = process.argv.includes("--apply");
  const forkFromPrior = !apply && !process.argv.includes("--dry-run-only");

  const report = await runTargetedMultiPropertyGovernedBenchmarkMeasurementV1({
    apply,
    forkFromPrior,
  });

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));

  console.log(report.title);
  console.log("\n## Execution Plan");
  for (const row of report.executionPlan?.perProperty || []) {
    console.log(
      `${row.PROPERTY} | territories: ${row.TARGET_TERRITORIES.length} | scenarios: ${row.SCENARIO_COUNT} | calls: ${row.CALLS_PLANNED} | est: $${row.ESTIMATED_COST}`
    );
  }
  console.log(`TOTAL_CALLS: ${report.executionPlan?.TOTAL_CALLS}`);
  console.log(`TOTAL_ESTIMATED_COST: $${report.executionPlan?.TOTAL_ESTIMATED_COST}`);

  console.log("\n## Execution");
  for (const row of report.execution?.perProperty || []) {
    console.log(
      `${row.PROPERTY} | period: ${row.NEW_PERIOD} | executed: ${row.CALLS_EXECUTED} | ok: ${row.SUCCESSFUL} | fail: ${row.FAILED} | cost: $${row.COST} | mode: ${row.executionMode}`
    );
  }
  console.log(`TOTAL_SPEND: $${report.execution?.TOTAL_SPEND}`);

  console.log("\n## Territory Certification");
  for (const row of report.territoryCertification || []) {
    console.log(
      `${row.PROPERTY} | ${row.TERRITORY} | core: ${row.CORE_COUNT} | presence: ${row.YOUR_AI_PRESENCE}% | benchmark: ${row.CORE_BENCHMARK}% | index: ${row.AI_PRESENCE_INDEX} | ${row.STATUS} | customer: ${row.CUSTOMER_STATUS}`
    );
  }

  console.log("\n## Waterstone");
  console.log(`PROVIDER_CALLS: ${report.waterstone?.PROVIDER_CALLS}`);
  console.log(`INDEX_DIFF: ${report.waterstone?.INDEX_DIFF}`);
  console.log(`CERTIFIED_TERRITORIES: ${report.waterstone?.CERTIFIED_TERRITORIES}`);

  console.log("\nFINAL:", report.final);
  console.log("NEXT:", report.next);
  console.log("Report:", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
