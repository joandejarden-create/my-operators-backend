#!/usr/bin/env node
/**
 * Brand Explorer Wave 12 factory CLI.
 *
 *   npm run brand-explorer-wave12-factory -- --stage preflight --dry-run
 *   npm run brand-explorer-wave12-factory -- --stage manifest --dry-run
 *   npm run brand-explorer-wave12-factory -- --stage factory-preview-cohort --dry-run
 *   npm run brand-explorer-wave12-factory -- --stage factory-preview-cohort --apply ...flags
 *   npm run brand-explorer-wave12-factory -- --stage source-packs --dry-run
 *   npm run brand-explorer-wave12-factory -- --stage tab-factory-build --dry-run
 *   npm run brand-explorer-wave12-factory -- --stage tab-factory-build --apply ...flags
 */
import "../load-env.js";
import {
  WAVE12_VERSION,
  WAVE12_STAGES,
  runWave12Factory,
} from "../lib/partner-intelligence/brand-explorer-wave12-factory.js";

function parseArgs(argv) {
  const stageIdx = argv.indexOf("--stage");
  const stage = stageIdx >= 0 ? argv[stageIdx + 1] : null;
  const dryRun = argv.includes("--dry-run") || !argv.includes("--apply");
  return { stage, dryRun, argv };
}

async function main() {
  const { stage, dryRun, argv } = parseArgs(process.argv.slice(2));
  if (!stage) {
    console.error(`Usage: --stage <${WAVE12_STAGES.join("|")}> [--dry-run|--apply ...]`);
    process.exit(2);
  }

  console.log(`[${WAVE12_VERSION}] stage=${stage} dryRun=${dryRun}`);
  const result = await runWave12Factory({ stage, dryRun, argv });

  if (result.paths) {
    if (result.paths.jsonPath) console.log(`Wrote ${result.paths.jsonPath}`);
    if (result.paths.mdPath) console.log(`Wrote ${result.paths.mdPath}`);
    if (result.paths.docPath) console.log(`Wrote ${result.paths.docPath}`);
    for (const p of result.paths.packPaths || []) console.log(`Wrote ${p}`);
  }
  if (result.deferred) {
    console.log(`DEFERRED: ${result.message}`);
    console.log(`Next: ${result.nextRequired}`);
    process.exitCode = 0;
    return;
  }
  if (result.stopRecommended || result.pass === false) {
    console.error(`STOP: stage=${stage} pass=${result.pass} stopRecommended=${result.stopRecommended}`);
    process.exitCode = 1;
    return;
  }
  if (result.wroteModule) console.log(`Updated factory-preview-candidates.js (Airtable writes=false)`);
  console.log(`Summary: ${JSON.stringify(result.summary || { stage, ok: true })}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
