#!/usr/bin/env node
/**
 * Export Wave 16A brand content to fixtures/brand-explorer-presentation-{slug}-full.json
 *
 *   node scripts/export-brand-explorer-wave16a-full-fixture.mjs --brand fairfield-by-marriott
 */
import "../load-env.js";
import { writeWave16aFullFixture } from "../lib/partner-intelligence/brand-explorer-export-wave16a-full-fixture.js";

function parseArgs(argv) {
  const brandIdx = argv.indexOf("--brand");
  const slug =
    brandIdx >= 0 && argv[brandIdx + 1]
      ? String(argv[brandIdx + 1]).trim()
      : "fairfield-by-marriott";
  const outIdx = argv.indexOf("--out");
  const outPath = outIdx >= 0 && argv[outIdx + 1] ? String(argv[outIdx + 1]).trim() : null;
  return { slug, outPath, noMomentum: argv.includes("--no-momentum") };
}

async function main() {
  const { slug, outPath, noMomentum } = parseArgs(process.argv.slice(2));
  const result = writeWave16aFullFixture(slug, {
    outPath: outPath || undefined,
    includeMomentum: !noMomentum,
  });
  console.log(`Exported ${result.rowCount} rows → ${result.outPath}`);
  console.log(`Stage 2B asset pack pass: ${result.stage2bAssetPackPass}`);
  if (result.stage2bBlockers?.length) {
    console.log(`Stage 2B blockers: ${result.stage2bBlockers.join(", ")}`);
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
