#!/usr/bin/env node
/**
 * Global Active/Live Brand Explorer semantic QA audit — dry-run only.
 *
 * Usage:
 *   npm run brand-explorer-global-active-semantic-audit -- --dry-run
 *   npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh
 */
import "dotenv/config";
import {
  GLOBAL_ACTIVE_SEMANTIC_AUDIT_VERSION,
  runGlobalActiveSemanticAudit,
  writeGlobalActiveSemanticAuditReports,
} from "../lib/partner-intelligence/brand-explorer-global-active-semantic-audit.js";

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. Global semantic audit is read-only. Use --dry-run only.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (audit-only, no writes).");
    process.exit(2);
  }

  const fresh = argv.includes("--fresh");
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;

  console.log(
    `[${GLOBAL_ACTIVE_SEMANTIC_AUDIT_VERSION}] global active semantic audit (dry-run${fresh ? ", fresh" : ""})`
  );
  const report = await runGlobalActiveSemanticAudit({ dryRun: true, brands, fresh: true });
  report.fresh = fresh;
  const paths = writeGlobalActiveSemanticAuditReports(report, { refresh: fresh });

  console.log(`Active count: ${report.activeCount} (expected ${report.expectedActiveCount})`);
  console.log(`Universe reconciled: ${report.universeReconciled}`);
  console.log(`Freeze decision: ${report.freezeDecision}`);
  console.log(`Buckets: ${JSON.stringify(report.bucketCounts)}`);
  console.log(`Severity: ${JSON.stringify(report.severityTotals)}`);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.criticalPath}`);
  console.log(`Wrote ${paths.byBrandPath}`);
  console.log(`Wrote ${paths.bySectionPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Ready: ${report.readyStatement}`);

  if (
    report.freezeDecision === "do_not_freeze_critical_blockers_present" ||
    report.freezeDecision === "do_not_freeze_remediation_required" ||
    report.freezeDecision === "do_not_freeze_universe_count_mismatch"
  ) {
    process.exitCode = 3;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
