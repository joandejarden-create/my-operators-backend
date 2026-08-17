#!/usr/bin/env node
/**
 * Brand Explorer Wave 13 factory CLI.
 *
 *   npm run brand-explorer-wave13-factory -- --stage preflight --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage manifest --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage factory-preview-cohort --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage factory-preview-cohort --apply ...flags
 *   npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run --reuse-fresh-reports
 *   npm run brand-explorer-wave13-factory -- --stage open-items-resolution --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage open-items-resolution --apply ...flags
 *   npm run brand-explorer-wave13-factory -- --stage tab-factory-build --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage tab-factory-build --apply ...flags
 *   npm run brand-explorer-wave13-factory -- --stage stage4-content-cleanup --dry-run
 *   npm run brand-explorer-wave13-factory -- --stage stage4-content-cleanup --apply ...flags
 */
import "../load-env.js";
import {
  WAVE13_VERSION,
  WAVE13_STAGES,
  runWave13Factory,
} from "../lib/partner-intelligence/brand-explorer-wave13-factory.js";

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
      `Usage: --stage <${WAVE13_STAGES.join("|")}> [--dry-run|--apply ...] [--reuse-fresh-reports]`
    );
    process.exit(2);
  }

  console.log(`[${WAVE13_VERSION}] stage=${stage} dryRun=${dryRun}`);
  const result = await runWave13Factory({ stage, dryRun, argv });

  if (result.paths) {
    if (result.paths.jsonPath) console.log(`Wrote ${result.paths.jsonPath}`);
    if (result.paths.mdPath) console.log(`Wrote ${result.paths.mdPath}`);
  }
  if (result.wroteModule) {
    console.log(`Updated factory-preview-candidates.js (Airtable writes=false)`);
  }
  if (result.deferred) {
    console.log(`DEFERRED: ${result.message || result.reason || "deferred"}`);
    if (result.nextRequired) console.log(`Next: ${result.nextRequired}`);
    process.exitCode =
      result.reason === "protected_39_preflight_not_clean" ||
      result.reason === "wave13_manifest_not_ready"
        ? 1
        : 0;
    return;
  }
  if (result.readyStatement) console.log(`Ready: ${result.readyStatement}`);
  if (result.mayProceedToSoBrandBasicsCreation != null) {
    console.log(`May proceed to SO/ Brand Basics creation: ${result.mayProceedToSoBrandBasicsCreation}`);
  }
  if (result.mayProceedToTabFactoryBuild != null) {
    console.log(`May proceed to Stage 4 tab-factory-build: ${result.mayProceedToTabFactoryBuild}`);
  }
  if (result.stage4Posture) {
    console.log(
      `Stage 4 posture: allEight=${result.stage4Posture.allEightBrands} sevenExcludingHouse=${result.stage4Posture.sevenExcludingHouseOfOriginals}`
    );
  }
  if (result.paths?.packPaths?.length) {
    console.log(`Packs: ${result.paths.packPaths.length}`);
  }
  if (result.mayProceedToFactoryPreviewCohort != null) {
    console.log(`May proceed to factory-preview-cohort: ${result.mayProceedToFactoryPreviewCohort}`);
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
