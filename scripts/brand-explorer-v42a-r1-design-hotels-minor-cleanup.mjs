#!/usr/bin/env node
/**
 * v42A-R1 — Design Hotels Founder Minor Cleanup (dry-run default).
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V42A_R1_VERSION,
  V42A_R1_TARGET,
  V42A_R1_APPLY_FLAGS,
  parseV42AR1ApplyFlags,
  runV42AR1DesignHotelsMinorCleanup,
  writeV42AR1Reports,
} from "../lib/partner-intelligence/brand-explorer-v42a-r1-design-hotels-minor-cleanup.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandIdx = argv.indexOf("--brand");
  const brandsIdx = argv.indexOf("--brands");
  let brand = V42A_R1_TARGET;
  if (brandIdx >= 0 && argv[brandIdx + 1]) brand = argv[brandIdx + 1].trim();
  else if (brandsIdx >= 0 && argv[brandsIdx + 1]) {
    const list = argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean);
    if (list.length !== 1 || list[0] !== V42A_R1_TARGET) {
      throw new Error("v42A-R1 accepts only --brand design-hotels");
    }
    brand = list[0];
  }
  const flags = parseV42AR1ApplyFlags(argv);
  return { brand, dryRun: !flags.apply, apply: flags.apply, flags };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V42A_R1_VERSION}] Design Hotels minor cleanup (dryRun=${opts.dryRun}, apply=${opts.apply})`);
  console.log(`  brand: ${opts.brand}`);
  console.log(`  cwd: ${ROOT}`);
  if (opts.apply) {
    console.log(`  flags required: ${Object.values(V42A_R1_APPLY_FLAGS).join(" ")}`);
  }

  const report = await runV42AR1DesignHotelsMinorCleanup(opts);
  const paths = writeV42AR1Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: patches=${report.summary.patchCount} unsafe=${report.summary.unsafePatches} projected=${report.summary.projectedRecommendation} cala=${report.summary.calaPreserved} baseline=${report.summary.baselineProtectionPass} applyExecuted=${report.summary.applyExecuted}`
  );
  console.log(
    `  tone: liveForbidden=${report.brandResult.toneCleanup.liveForbiddenCount} projectedForbidden=${report.brandResult.toneCleanup.projectedForbiddenCount} mediumMechanical=${report.brandResult.toneCleanup.projectedMediumMechanicalCount}`
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
    "v42A-R1 complete — Design Hotels only; no unlock; no active release; CALA preserved; golden brands protected."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
