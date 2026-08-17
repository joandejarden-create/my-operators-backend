#!/usr/bin/env node
/**
 * Phase 2A — Global Census Provenance + Coverage Audit (READ-ONLY).
 *
 *   npm run census:full-cala-phase-2a-provenance-coverage-audit
 *
 * Never enables production writes.
 */
import "dotenv/config";
import { runFullCalaPhase2aGlobalProvenanceCoverageAuditV1 } from "../lib/research-engine-v2/full-cala-phase-2a-global-provenance-coverage-audit-v1.js";

if (
  process.argv.includes("--enable-production-writes") ||
  String(process.env.ALLOW_CENSUS_AUTOPILOT_APPLY || "0") === "1" &&
    process.argv.includes("--apply")
) {
  console.error(
    "Phase 2A is read-only. Refusing any production-write / apply flags."
  );
  process.exit(2);
}

const report = await runFullCalaPhase2aGlobalProvenanceCoverageAuditV1({
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      AUDIT_STATUS: report.AUDIT_STATUS,
      PRODUCTION_CENSUS_COUNT: report.PRODUCTION_CENSUS_COUNT,
      SHELLS_RECONCILED: report.SHELLS_RECONCILED,
      PROVENANCE_ANOMALIES: report.PROVENANCE_ANOMALIES,
      PROTECTED_FIELD_ANOMALIES: report.PROTECTED_FIELD_ANOMALIES,
      HBX_CODE_DUPLICATES: report.HBX_CODE_DUPLICATES,
      HIGH_CONFIDENCE_DUPLICATE_GROUPS: report.HIGH_CONFIDENCE_DUPLICATE_GROUPS,
      REVIEW_DUPLICATE_GROUPS: report.REVIEW_DUPLICATE_GROUPS,
      TOTAL_HELD_CANDIDATES: report.TOTAL_HELD_CANDIDATES,
      BRAZIL_HELD_COUNT: report.BRAZIL_HELD_COUNT,
      MEXICO_HELD_COUNT: report.MEXICO_HELD_COUNT,
      COLOMBIA_HELD_COUNT: report.COLOMBIA_HELD_COUNT,
      CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE:
        report.CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE,
      ESTIMATED_UNDISCOVERED_GAP: report.ESTIMATED_UNDISCOVERED_GAP,
      MOST_UNDERREPRESENTED_COUNTRIES: report.MOST_UNDERREPRESENTED_COUNTRIES,
      NEXT_RECOMMENDED_ACTION: report.NEXT_RECOMMENDED_ACTION,
      FOUNDER_DECISION_REQUIRED: report.FOUNDER_DECISION_REQUIRED,
      production_writes: report.production_writes,
      report_paths: report.report_paths,
      secondary_statuses: report.secondary_statuses,
    },
    null,
    2
  )
);

process.exit(report.ok === false ? 1 : 0);
