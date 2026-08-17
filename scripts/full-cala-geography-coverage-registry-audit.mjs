#!/usr/bin/env node
/**
 * Full CALA Geography Coverage Registry Audit — READ-ONLY.
 *
 *   npm run census:full-cala-geography-coverage-registry-audit
 *
 * Refuses production-write flags.
 */
import "dotenv/config";
import { runFullCalaGeographyCoverageRegistryAuditV1 } from "../lib/research-engine-v2/full-cala-geography-coverage-registry-audit-v1.js";

if (
  process.argv.includes("--enable-production-writes") ||
  process.argv.includes("--apply")
) {
  console.error("Geography coverage audit is read-only. Refusing write/apply flags.");
  process.exit(2);
}

const report = await runFullCalaGeographyCoverageRegistryAuditV1({
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      AUDIT_STATUS: report.AUDIT_STATUS,
      CANONICAL_GEOGRAPHIES_TOTAL: report.CANONICAL_GEOGRAPHIES_TOTAL,
      GEOGRAPHIES_WITH_CENSUS_RECORDS: report.GEOGRAPHIES_WITH_CENSUS_RECORDS,
      GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS:
        report.GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS,
      GEOGRAPHIES_HBX_SEARCHED: report.GEOGRAPHIES_HBX_SEARCHED,
      GEOGRAPHIES_CVENT_SEARCHED: report.GEOGRAPHIES_CVENT_SEARCHED,
      GEOGRAPHIES_NOT_YET_SEARCHED: report.GEOGRAPHIES_NOT_YET_SEARCHED,
      GEOGRAPHIES_WITH_SOURCE_GAPS: report.GEOGRAPHIES_WITH_SOURCE_GAPS,
      GEOGRAPHIES_WITH_NORMALIZATION_PROBLEMS:
        report.GEOGRAPHIES_WITH_NORMALIZATION_PROBLEMS,
      TOP_15_CENSUS_GEOGRAPHIES: report.TOP_15_CENSUS_GEOGRAPHIES,
      ZERO_RECORD_GEOGRAPHIES: report.ZERO_RECORD_GEOGRAPHIES,
      LOW_COVERAGE_GEOGRAPHIES: report.LOW_COVERAGE_GEOGRAPHIES,
      HOLD_CONCENTRATION_TOP_1: report.HOLD_CONCENTRATION_TOP_1,
      HOLD_CONCENTRATION_TOP_3: report.HOLD_CONCENTRATION_TOP_3,
      HOLD_CONCENTRATION_TOP_5: report.HOLD_CONCENTRATION_TOP_5,
      TOP_10_SOURCE_GAP_DISCOVERY_PRIORITIES:
        report.TOP_10_SOURCE_GAP_DISCOVERY_PRIORITIES,
      BERMUDA_SCOPE_RECOMMENDATION: report.BERMUDA_SCOPE_RECOMMENDATION,
      MACRO_CENSUS_BUCKETS: report.MACRO_CENSUS_BUCKETS,
      NEXT_RECOMMENDED_ACTION: report.NEXT_RECOMMENDED_ACTION,
      FOUNDER_DECISION_REQUIRED: report.FOUNDER_DECISION_REQUIRED,
      FOUNDER_DECISION: report.FOUNDER_DECISION,
      production_writes: report.production_writes,
      report_paths: report.report_paths,
    },
    null,
    2
  )
);

process.exit(report.ok === false ? 1 : 0);
