#!/usr/bin/env node
/**
 * Lane 2 — Founder Minor Cleanup (targeted Presentation + Brand Basics patches).
 * Dry-run by default. Apply requires all confirm flags.
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  CLEANUP_VERSION,
  REQUIRED_APPLY_FLAGS,
  parseLane2CleanupApplyFlags,
  runLane2FounderMinorCleanup,
} from "../lib/partner-intelligence/brand-explorer-lane2-founder-minor-cleanup.js";
import { resolveFullBuildSlug } from "../lib/partner-intelligence/brand-explorer-full-build-content.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const raw =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [
          "autograph-collection",
          "handwritten-collection",
          "radisson-collection",
          "tapestry-collection-by-hilton",
          "vignette-collection",
        ];
  const brands = raw.map((b) => resolveFullBuildSlug(b));
  const applyRequested = argv.includes("--apply");
  const flagCheck = parseLane2CleanupApplyFlags(argv);
  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `Lane 2 founder minor cleanup --apply requires all gates:\n  ${REQUIRED_APPLY_FLAGS.join(
        "\n  "
      )}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }
  return { brands, dryRun: !applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${CLEANUP_VERSION}] Founder minor cleanup (dryRun=${opts.dryRun})`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  console.log(
    "  guardrails: no restore · no release · no CV/source/registry · targeted fields only · unlock hold remains"
  );

  const result = await runLane2FounderMinorCleanup({
    brands: opts.brands,
    dryRun: opts.dryRun,
    argv: opts.argv,
  });

  console.log(`Wrote reports/brand-explorer-lane2-founder-minor-cleanup.json`);
  console.log(`Wrote reports/brand-explorer-lane2-founder-minor-cleanup.md`);
  for (const b of result.brandResults || []) {
    console.log(
      `  ${b.brandSlug}: patches=${b.summary?.patchCount ?? 0}` +
        (b.blocked ? ` BLOCKED=${(b.blockers || []).join(",")}` : "")
    );
  }
  console.log(
    `Summary: brands=${result.summary.brandCount} patches=${result.summary.patchCount} applied=${result.summary.applied} hold=${result.guardrails.accidentalLegacyUnlockHoldRemains}`
  );

  if (!opts.dryRun && result.applyResult?.applied !== true) {
    console.error("Apply did not complete successfully:", result.applyResult?.reason || result.applyResult);
    process.exit(1);
  }

  const errors = Object.values(result.applyResult?.resultsByBrand || {}).flatMap(
    (r) => r.results?.errors || []
  );
  if (errors.length) {
    console.error(`Apply wrote with ${errors.length} errors:`);
    for (const e of errors) console.error(`  ${e.slotKey || e.recordId}: ${e.message}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
