#!/usr/bin/env node
/**
 * CALA Census ↔ legacy Hotel Census coverage reconciliation (BENCHMARK_ONLY).
 *
 * Read-only. Aggregate metrics only. No production writes. No discovery searches.
 *
 *   npm run census:benchmark-coverage-reconciliation
 */
import "dotenv/config";
import { runCalaCensusBenchmarkCoverageReconciliationV1 } from "../lib/research-engine-v2/cala-census-benchmark-coverage-reconciliation-v1.js";

const report = await runCalaCensusBenchmarkCoverageReconciliationV1({
  log: (m) => console.log(m),
});

const summary = {
  RECONCILIATION_STATUS: report.RECONCILIATION_STATUS,
  DEALALITY_CENSUS_COUNT: report.DEALALITY_CENSUS_COUNT,
  BENCHMARK_CENSUS_COUNT: report.BENCHMARK_CENSUS_COUNT,
  GEOGRAPHIES_COMPARED: report.GEOGRAPHIES_COMPARED,
  GEOGRAPHIES_BENCHMARK_ALIGNED: report.GEOGRAPHIES_BENCHMARK_ALIGNED,
  POSSIBLE_MINOR_GAPS: report.POSSIBLE_MINOR_GAPS,
  POSSIBLE_MODERATE_GAPS: report.POSSIBLE_MODERATE_GAPS,
  POSSIBLE_MAJOR_GAPS: report.POSSIBLE_MAJOR_GAPS,
  ZERO_DEALALITY_BENCHMARK_NONZERO: report.ZERO_DEALALITY_BENCHMARK_NONZERO,
  DEALALITY_HIGHER_THAN_BENCHMARK: report.DEALALITY_HIGHER_THAN_BENCHMARK,
  TOP_15_GEOGRAPHIC_DISCOVERY_PRIORITIES: report.TOP_15_GEOGRAPHIC_DISCOVERY_PRIORITIES,
  TOP_CITY_DESTINATION_DISCOVERY_PRIORITIES:
    report.TOP_CITY_DESTINATION_DISCOVERY_PRIORITIES,
  PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED: report.PROPERTY_LEVEL_BENCHMARK_DATA_PERSISTED,
  BENCHMARK_RECORDS_WRITTEN_TO_DEALALITY: report.BENCHMARK_RECORDS_WRITTEN_TO_DEALALITY,
  BENCHMARK_USED_AS_PRODUCTION_PROVENANCE: report.BENCHMARK_USED_AS_PRODUCTION_PROVENANCE,
  NEXT_RECOMMENDED_ACTION: report.NEXT_RECOMMENDED_ACTION,
  report_paths: {
    json: "reports/research-engine-v2/cala-census-benchmark-coverage-reconciliation.json",
    md: "reports/research-engine-v2/cala-census-benchmark-coverage-reconciliation.md",
    priority_json:
      "reports/research-engine-v2/cala-geography-discovery-priority-from-benchmark.json",
  },
};

console.log(JSON.stringify(summary, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
