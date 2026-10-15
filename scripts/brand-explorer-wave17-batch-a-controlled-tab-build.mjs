#!/usr/bin/env node
import { refuseLegacyApply } from "../lib/brand-explorer/canonical/legacy-apply-guard.js";
refuseLegacyApply(process.argv, "brand-explorer-wave17-batch-a-controlled-tab-build.mjs");
/**
 * Wave 17 Batch A controlled tab build CLI.
 *
 *   npm run brand-explorer-wave17-batch-a-controlled-tab-build -- --dry-run
 *   npm run brand-explorer-wave17-batch-a-controlled-tab-build -- --apply …flags
 */
import "../load-env.js";
import { runWave17BatchAControlledTabBuild } from "../lib/partner-intelligence/brand-explorer-wave17-batch-a-controlled-tab-build.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  const result = await runWave17BatchAControlledTabBuild({
    dryRun,
    argv,
  });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(`Universe: ${result.activeUniverseBefore} → ${result.activeUniverseAfter}`);
  console.log(`Pass: ${result.pass === true}`);
  console.log(`Writes: ${result.airtableWrites === true}`);
  if (result.summary) {
    console.log(
      `Planned presentation writes: ${result.summary.plannedPresentationWrites}; blocked: ${(result.summary.blockedSlugs || []).join(",") || "none"}`
    );
  }
  for (const b of result.brandResults || []) {
    console.log(
      `- ${b.brandSlug}: blocked=${b.blocked === true} patches=${b.patches?.length ?? 0} semantic=${b.semantic?.pass === true}`
    );
    if (b.blockers?.length) console.log(`  blockers: ${b.blockers.slice(0, 8).join("; ")}`);
  }
  if (result.pass === false && !dryRun) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
