#!/usr/bin/env node
/**
 * Operator Explorer Tab Factory audit (dry-run).
 *
 *   npm run operator-explorer-tab-factory-audit -- --dry-run
 *   npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=fixtures
 *   npm run operator-explorer-tab-factory-audit -- --source=merged --dry-run
 *
 * Exit: 0 auditPass · 2 patchPlan incomplete · 3 failFindings > 0
 */
import "dotenv/config";
import {
  runOperatorTabFactoryAudit,
  writeOperatorTabFactoryAuditReports,
  AUDIT_VERSION,
} from "../lib/partner-intelligence/operator-explorer-tab-factory-audit.js";
import { OPERATOR_QUALITY_BASELINE_OPERATORS } from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";

function parseArgs(argv) {
  const out = {
    operators: null,
    source: "fixtures",
    dryRun: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    } else if (a.startsWith("--operators=")) {
      out.operators = a
        .slice("--operators=".length)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    } else if (a === "--source" && argv[i + 1]) {
      out.source = String(argv[++i]).trim().toLowerCase();
    } else if (a.startsWith("--source=")) {
      out.source = a.slice("--source=".length).trim().toLowerCase();
    } else if (a === "--dry-run") {
      out.dryRun = true;
    }
  }
  if (!["fixtures", "live", "merged"].includes(out.source)) {
    throw new Error(`Invalid --source=${out.source} (use fixtures|live|merged)`);
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const operators =
    args.operators || OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);

  console.log(`[${AUDIT_VERSION}] operator tab-factory audit dry-run`);
  console.log(`  source: ${args.source}`);
  console.log(`  operators: ${operators.join(", ")}`);

  const report = await runOperatorTabFactoryAudit({
    operators,
    source: args.source,
  });
  const paths = writeOperatorTabFactoryAuditReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: failFindings=${report.summary.totalFailFindings} empty=${report.summary.totalEmptyRenderFails} patchPlanComplete=${report.patchPlanComplete} auditPass=${report.auditPass}`
  );
  for (const o of report.operatorResults) {
    console.log(
      `  ${o.operatorSlug}: auditPass=${o.auditPass} fails=${o.failFindings} empty=${o.emptyRenderFailFindings} decision=${o.releaseQualityDecision}`
    );
  }
  if (!report.patchPlanComplete) process.exit(2);
  if (!report.auditPass) process.exit(3);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
