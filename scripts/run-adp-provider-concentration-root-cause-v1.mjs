#!/usr/bin/env node
/**
 * ADP Provider Concentration Root-Cause V1 — offline diagnostics.
 *   npm run adp:provider-concentration-root-cause-v1
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { runProviderConcentrationRootCauseV1 } from "../lib/ai-demand-positioning/metrics/provider-concentration-root-cause-v1.js";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/provider-concentration-root-cause-v1.json");

async function main() {
  const report = runProviderConcentrationRootCauseV1();
  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));

  console.log(report.title);
  console.log("\n## Current Rule");
  console.log("FORMULA:", report.currentRule.FORMULA.slice(0, 120) + "...");
  console.log("THRESHOLD:", report.currentRule.THRESHOLD);
  console.log("ORIGIN:", report.currentRule.THRESHOLD_ORIGIN);
  console.log("\nPRIMARY ANSWER:", report.primaryQuestion.ANSWER);
  console.log("\n## Affected Territories");
  for (const row of report.affectedTerritoryDiagnostics) {
    console.log(
      `${row.PROPERTY} | ${row.TERRITORY} | subjRange ${row.SUBJECT_PROVIDER_RANGE_PP}pp | subjLOO ${row.SUBJECT_PROVIDER_LOO_RANGE_PP}pp | influential ${row.MOST_INFLUENTIAL_PROVIDER} | ${row.ROOT_CAUSE}`
    );
  }
  console.log("\n## Recommended Governance");
  console.log("KEEP current:", report.recommendedGovernance.CURRENT_RULE_KEEP);
  console.log("RULE:", report.recommendedGovernance.RECOMMENDED_RULE);
  console.log("\n## Certified Protection");
  console.log("TESTED:", report.existingCertifiedProtection.CURRENT_CERTIFIED_ROWS_TESTED);
  console.log("INVALIDATED:", report.existingCertifiedProtection.CURRENT_CERTIFIED_ROWS_INVALIDATED);
  console.log("\n## Trend Audit");
  console.log("DETECTED:", report.trendSafetyAudit.INVALID_COMPARISONS_DETECTED);
  console.log("BLOCKED:", report.trendSafetyAudit.INVALID_COMPARISONS_BLOCKED);
  console.log("RENDERED:", report.trendSafetyAudit.INVALID_COMPARISONS_RENDERED);
  console.log("\nFINAL:", report.final);
  console.log("NEXT:", report.next);
  console.log("Report:", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
