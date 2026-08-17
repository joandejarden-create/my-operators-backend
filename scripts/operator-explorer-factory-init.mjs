#!/usr/bin/env node
/**
 * Scaffold Operator Explorer fixtures for the next factory-queue operator.
 *
 *   npm run operator-explorer-factory-init -- --dry-run
 *   npm run operator-explorer-factory-init -- --operators ghl-hoteles --dry-run
 *   npm run operator-explorer-factory-init -- --operators ghl-hoteles --apply --approve-operator-explorer-factory-init
 */
import {
  runOperatorExplorerFactoryInit,
  writeOperatorExplorerFactoryInitReports,
  OPERATOR_FACTORY_INIT_VERSION,
} from "../lib/partner-intelligence/operator-explorer-factory-init.js";

function parseArgs(argv) {
  const out = { operators: null, apply: false, approveFactoryInit: false };
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
    } else if (a === "--apply") {
      out.apply = true;
    } else if (a === "--dry-run") {
      out.apply = false;
    } else if (a === "--approve-operator-explorer-factory-init") {
      out.approveFactoryInit = true;
    }
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(`[${OPERATOR_FACTORY_INIT_VERSION}] dryRun=${!args.apply}`);
  const report = runOperatorExplorerFactoryInit({
    operators: args.operators || undefined,
    apply: args.apply,
    approveFactoryInit: args.approveFactoryInit,
  });
  const paths = writeOperatorExplorerFactoryInitReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `wouldCreate=${report.summary.filesWouldCreate} created=${report.summary.filesCreated} skipped=${report.summary.filesSkippedExisting}`
  );
  for (const r of report.results) {
    console.log(`  ${r.operatorSlug}: domain=${r.domain} explorer=${r.explorerUrl}`);
  }
}

main();
