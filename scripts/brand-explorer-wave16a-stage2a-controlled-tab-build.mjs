#!/usr/bin/env node
/**
 * Wave 16A Stage 2A — Controlled tab build (Fairfield, Four Points, Delta).
 *
 *   npm run brand-explorer-wave16a-stage2a-controlled-tab-build -- --dry-run
 *   npm run brand-explorer-wave16a-stage2a-controlled-tab-build -- --apply ...flags
 */
import "../load-env.js";
import { WAVE16A_STAGE2A_APPLY_FLAGS } from "../lib/partner-intelligence/brand-explorer-wave16a-factory-plan.js";
import {
  WAVE16A_STAGE2A_BUILD_VERSION,
  runWave16aStage2aControlledTabBuild,
} from "../lib/partner-intelligence/brand-explorer-wave16a-stage2a-controlled-tab-build.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = argv.includes("--dry-run") || !argv.includes("--apply");
  console.log(`[${WAVE16A_STAGE2A_BUILD_VERSION}] dryRun=${dryRun}`);
  if (!dryRun) {
    console.log(`Apply flags required (${WAVE16A_STAGE2A_APPLY_FLAGS.length}):`);
    for (const f of WAVE16A_STAGE2A_APPLY_FLAGS) console.log(`  ${f}`);
  }

  const result = await runWave16aStage2aControlledTabBuild({ dryRun, argv });

  if (result.paths?.jsonPath) console.log(`Wrote ${result.paths.jsonPath}`);
  if (result.paths?.mdPath) console.log(`Wrote ${result.paths.mdPath}`);
  if (result.paths?.writeDiffPath) console.log(`Wrote ${result.paths.writeDiffPath}`);
  if (result.paths?.semanticPath) console.log(`Wrote ${result.paths.semanticPath}`);
  if (result.paths?.docPath) console.log(`Wrote ${result.paths.docPath}`);
  if (result.paths?.packPaths?.length) {
    for (const p of result.paths.packPaths) console.log(`Wrote ${p}`);
  }

  console.log(`Active universe before/after: ${result.activeUniverseBefore ?? "—"} / ${result.activeUniverseAfter ?? "—"}`);
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Summary: ${JSON.stringify({
      pass: result.pass,
      plannedPresentationWrites: result.summary?.plannedPresentationWrites,
      plannedBasicsWrites: result.summary?.plannedBasicsWrites,
      plannedMomentumWrites: result.summary?.plannedMomentumWrites,
      plannedImageWrites: result.summary?.plannedImageWrites,
      blockedSlugs: result.summary?.blockedSlugs,
      airtableWrites: result.airtableWrites,
    })}`
  );

  if (result.stopRecommended || result.pass === false) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exitCode = 1;
});
