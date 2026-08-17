#!/usr/bin/env node
/**
 * Operator Explorer OS — release-readiness gate table (dry-run).
 *
 *   npm run operator-explorer-os -- --dry-run
 *   npm run operator-explorer-os -- --operators arbor-lodging-cala,hotel-equities-cala,ghl-hoteles --source=merged
 *
 * Exit: 0 always for dry-run evaluation (reports written). Exit 2 if qualityBaselinesReady=false.
 */
import "dotenv/config";
import {
  runOperatorExplorerOs,
  writeOperatorExplorerOsReports,
  OPERATOR_EXPLORER_OS_VERSION,
} from "../lib/partner-intelligence/operator-explorer-os.js";

function parseArgs(argv) {
  const out = {
    operators: null,
    source: "merged",
    stage: "release-readiness",
    dryRun: true,
    includeRemediationPreview: true,
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
    } else if (a === "--stage" && argv[i + 1]) {
      out.stage = String(argv[++i]).trim();
    } else if (a.startsWith("--stage=")) {
      out.stage = a.slice("--stage=".length).trim();
    } else if (a === "--dry-run") {
      out.dryRun = true;
    } else if (a === "--no-remediation-preview") {
      out.includeRemediationPreview = false;
    }
  }
  if (!["fixtures", "live", "merged"].includes(out.source)) {
    throw new Error(`Invalid --source=${out.source}`);
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(`[${OPERATOR_EXPLORER_OS_VERSION}] stage=${args.stage} source=${args.source}`);

  const report = await runOperatorExplorerOs({
    operators: args.operators || undefined,
    source: args.source,
    stage: args.stage,
    includeRemediationPreview: args.includeRemediationPreview,
  });
  const paths = writeOperatorExplorerOsReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `qualityBaselinesReady=${report.summary.qualityBaselinesReady} canStartNextOperatorExplorer=${report.summary.canStartNextOperatorExplorer}`
  );
  if (report.summary.nextFactoryOperator) {
    const n = report.summary.nextFactoryOperator;
    console.log(`Next Operator Explorer: ${n.companyName} (${n.slug} / ${n.recordId})`);
    console.log(`  ${n.explorerUrl}`);
  }
  for (const row of report.table) {
    console.log(
      `  ${row.operatorSlug}: state=${row.canonicalState} next=${row.allowedNextAction}`
    );
  }
  if (!report.summary.qualityBaselinesReady) process.exit(2);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
