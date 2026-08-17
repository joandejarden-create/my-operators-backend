#!/usr/bin/env node
/**
 * Brand Explorer Wave 14 factory CLI (Marriott cohort).
 *
 *   npm run brand-explorer-wave14-factory -- --stage preflight --dry-run --reuse-fresh-reports
 *   npm run brand-explorer-wave14-factory -- --stage manifest --dry-run
 *   npm run brand-explorer-wave14-factory -- --stage factory-preview-cohort --dry-run
 *   npm run brand-explorer-wave14-factory -- --stage factory-preview-cohort --apply ...flags
 *   npm run brand-explorer-wave14-factory -- --stage source-packs --dry-run
 */
import "../load-env.js";
import {
  WAVE14_VERSION,
  WAVE14_STAGES,
  runWave14Factory,
} from "../lib/partner-intelligence/brand-explorer-wave14-factory.js";

function parseArgs(argv) {
  const stageIdx = argv.indexOf("--stage");
  const stage = stageIdx >= 0 ? argv[stageIdx + 1] : null;
  const dryRun = argv.includes("--dry-run") || !argv.includes("--apply");
  return { stage, dryRun, argv };
}

async function main() {
  const { stage, dryRun, argv } = parseArgs(process.argv.slice(2));
  if (!stage) {
    console.error(
      `Usage: --stage <${WAVE14_STAGES.join("|")}> [--dry-run|--apply ...] [--reuse-fresh-reports]`
    );
    process.exit(2);
  }

  console.log(`[${WAVE14_VERSION}] stage=${stage} dryRun=${dryRun}`);
  const result = await runWave14Factory({ stage, dryRun, argv });

  if (result.paths) {
    if (result.paths.jsonPath) console.log(`Wrote ${result.paths.jsonPath}`);
    if (result.paths.mdPath) console.log(`Wrote ${result.paths.mdPath}`);
    if (result.paths.docsPath) console.log(`Wrote ${result.paths.docsPath}`);
  }
  if (result.wroteModule) {
    console.log(`Updated factory-preview-candidates.js (Airtable writes=false)`);
  }
  if (result.deferred) {
    console.log(`DEFERRED: ${result.message || result.reason || "deferred"}`);
    if (result.nextRequired) console.log(`Next: ${result.nextRequired}`);
    process.exitCode =
      result.reason === "protected_46_preflight_not_clean" ||
      result.reason === "wave14_manifest_not_ready"
        ? 1
        : 0;
    return;
  }
  if (result.readyStatement) console.log(`Ready: ${result.readyStatement}`);
  if (result.mayProceedToFactoryPreviewCohort != null) {
    console.log(`May proceed to factory-preview-cohort: ${result.mayProceedToFactoryPreviewCohort}`);
  }
  if (result.mayProceedToTabFactoryBuild != null) {
    console.log(`May proceed to Stage 4 tab-factory-build: ${result.mayProceedToTabFactoryBuild}`);
  }
  if (result.readyStatement) console.log(`Ready: ${result.readyStatement}`);
  if (result.paths?.packPaths?.length) {
    console.log(`Packs: ${result.paths.packPaths.length}`);
  }
  if (result.stopRecommended || result.pass === false) {
    console.error(
      `STOP: stage=${stage} pass=${result.pass} stopRecommended=${result.stopRecommended}`
    );
    process.exitCode = 1;
    return;
  }
  console.log(
    `Summary: ${JSON.stringify(
      result.summary || {
        stage,
        ok: true,
        wroteModule: result.wroteModule,
        candidates: result.candidateSlugs?.length,
      }
    )}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
