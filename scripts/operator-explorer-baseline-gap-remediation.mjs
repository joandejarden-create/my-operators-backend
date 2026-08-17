#!/usr/bin/env node
/**
 * Operator Explorer baseline gap remediation (Arbor + Hotel Equities).
 *
 *   npm run operator-explorer-baseline-gap-remediation -- --source=merged --dry-run
 *   npm run operator-explorer-baseline-gap-remediation -- --source=merged --apply \
 *     --approve-operator-baseline-gap-remediation --confirm-fixture-overlay-only
 *
 * Apply writes local fixture overlay JSON only (no Airtable).
 */
import "dotenv/config";
import {
  BASELINE_GAP_REMEDIATION_VERSION,
  runOperatorBaselineGapRemediation,
  writeBaselineGapRemediationReports,
} from "../lib/partner-intelligence/operator-explorer-baseline-gap-remediation.js";
import { OPERATOR_QUALITY_BASELINE_OPERATORS } from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";

function parseArgs(argv) {
  const out = {
    operators: null,
    source: "merged",
    apply: false,
    approveRemediation: false,
    fixtureOverlayOnly: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    } else if (a.startsWith("--operators=")) {
      out.operators = a.slice("--operators=".length).split(",").map((s) => s.trim()).filter(Boolean);
    } else if (a === "--source" && argv[i + 1]) {
      out.source = String(argv[++i]).trim().toLowerCase();
    } else if (a.startsWith("--source=")) {
      out.source = a.slice("--source=".length).trim().toLowerCase();
    } else if (a === "--dry-run") {
      out.apply = false;
    } else if (a === "--apply") {
      out.apply = true;
    } else if (a === "--approve-operator-baseline-gap-remediation") {
      out.approveRemediation = true;
    } else if (a === "--confirm-fixture-overlay-only") {
      out.fixtureOverlayOnly = true;
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const operators =
    args.operators || OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);
  console.log(`[${BASELINE_GAP_REMEDIATION_VERSION}] baseline gap remediation`);
  console.log(`  source: ${args.source}`);
  console.log(`  dryRun: ${!args.apply}`);
  console.log(`  operators: ${operators.join(", ")}`);

  const report = await runOperatorBaselineGapRemediation({
    operators,
    source: args.source,
    apply: args.apply,
    approveRemediation: args.approveRemediation,
    fixtureOverlayOnly: args.fixtureOverlayOnly,
  });
  const paths = writeBaselineGapRemediationReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: fails ${report.summary.totalFailBefore} → ${report.summary.totalFailAfter}; fieldPass=${report.summary.allFieldAuditPassAfter}; sectionPass=${report.summary.allSectionPatternPassAfter}; auditPass=${report.summary.allAuditPassAfter}`
  );
  for (const r of report.results) {
    console.log(
      `  ${r.operatorSlug}: fails ${r.before.failFindings}→${r.after.failFindings} remaining=[${(r.after.remainingFails || []).join(", ")}]`
    );
  }
  if (!report.summary.allAuditPassAfter) process.exitCode = 3;
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
