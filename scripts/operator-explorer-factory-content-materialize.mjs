#!/usr/bin/env node
/**
 * Materialize GHL / Aimbridge factory content into fixtures/*.json
 *
 *   npm run operator-explorer-factory-content-materialize -- --dry-run
 *   npm run operator-explorer-factory-content-materialize -- --operators ghl-hoteles,aimbridge-latam --apply --approve-operator-factory-content-materialize
 */
import {
  runOperatorExplorerFactoryContentMaterialize,
  writeFactoryContentMaterializeReports,
  FACTORY_CONTENT_VERSION,
} from "../lib/partner-intelligence/operator-explorer-factory-content-materialize.js";

function parseArgs(argv) {
  const out = {
    operators: null,
    apply: false,
    approveMaterialize: false,
    recordIds: {},
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
    } else if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-operator-factory-content-materialize") {
      out.approveMaterialize = true;
    } else if (a.startsWith("--record-id-aimbridge-latam=")) {
      out.recordIds["aimbridge-latam"] = a.split("=")[1];
    } else if (a === "--record-id-aimbridge-latam" && argv[i + 1]) {
      out.recordIds["aimbridge-latam"] = argv[++i];
    }
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(`[${FACTORY_CONTENT_VERSION}] dryRun=${!args.apply}`);
  const report = runOperatorExplorerFactoryContentMaterialize({
    operators: args.operators || undefined,
    apply: args.apply,
    approveMaterialize: args.approveMaterialize,
    recordIds: args.recordIds,
  });
  const paths = writeFactoryContentMaterializeReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `planned=${report.summary.filesPlanned} written=${report.summary.filesWritten}`
  );
}

main();
