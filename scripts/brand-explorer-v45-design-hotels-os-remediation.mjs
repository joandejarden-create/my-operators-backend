#!/usr/bin/env node
/**
 * v45 — Design Hotels OS-Guided Remediation + Founder Review Reentry.
 * Default dry-run. Apply only with explicit confirms.
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V45_VERSION,
  V45_TARGET_BRAND,
  V45_APPLY_FLAGS,
  parseV45ApplyFlags,
  runV45DesignHotelsOsRemediation,
  writeV45Reports,
} from "../lib/partner-intelligence/brand-explorer-v45-design-hotels-os-remediation.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandIdx = argv.indexOf("--brand");
  const brandsIdx = argv.indexOf("--brands");
  let brand = V45_TARGET_BRAND;
  if (brandIdx >= 0 && argv[brandIdx + 1]) brand = argv[brandIdx + 1].trim();
  else if (brandsIdx >= 0 && argv[brandsIdx + 1]) {
    const list = argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean);
    if (list.length !== 1 || list[0] !== V45_TARGET_BRAND) {
      throw new Error("v45 accepts only --brand design-hotels (no other brands).");
    }
    brand = list[0];
  }
  const flags = parseV45ApplyFlags(argv);
  return { brand, dryRun: !flags.apply, apply: flags.apply, flags };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V45_VERSION}] Design Hotels OS remediation (dryRun=${opts.dryRun}, apply=${opts.apply})`);
  console.log(`  brand: ${opts.brand}`);
  console.log(`  cwd: ${ROOT}`);
  console.log("  guardrails: no unlock · no Company Validated · no released brands · no images/registry/sources");

  const report = await runV45DesignHotelsOsRemediation(opts);
  const paths = writeV45Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.founderPath}`);
  console.log(`Wrote ${paths.baselinePath}`);

  if (report.aborted) {
    console.error(`Aborted: ${report.abortReason}`);
    console.error(`OS blockers: ${(report.osConfirm?.blockers || []).join(", ")}`);
    process.exit(2);
  }

  console.log(
    `Summary: osOk=${report.summary?.osRoutedApplyRemediation} patches=${report.summary?.patchCount} unsafe=${report.summary?.unsafePatches} projectedFounder=${report.summary?.projectedFounderReviewReady} dryRunClean=${report.summary?.dryRunClean} baseline=${report.summary?.baselineProtectionPass} applyExecuted=${report.applyExecuted}`
  );
  console.log(
    `  properties: pass=${report.propertyExamples?.pass} visible=${report.propertyExamples?.visibleCount}/3 imageUrl=${report.propertyExamples?.imageUrlCount}`
  );
  console.log(
    `  internal preview: before=${report.projection?.internalPreviewForbiddenBeforeCount} afterProjected=${report.projection?.internalPreviewForbiddenAfterCount}`
  );

  console.log("Exact apply command:");
  console.log(report.exactApplyCommand);

  if (opts.apply && report.applyBlocked) {
    console.error("Apply blocked:");
    for (const b of report.applyGateCheck?.blockers || []) console.error(`  ${b}`);
    process.exit(2);
  }
  if (opts.apply && report.applyResult?.errors?.length) {
    console.error(`Apply errors: ${report.applyResult.errors.length}`);
    process.exit(1);
  }

  console.log(
    "v45 complete — Design Hotels only; no unlock; Company Validated untouched; golden brands protected."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
