#!/usr/bin/env node
/**
 * v43 — Brand Explorer Active Release Apply.
 *
 * Default dry-run. Apply unlocks only with explicit founder + gate confirms.
 * No content / Company Validated / Source Library / Registry / image writes.
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V43_VERSION,
  V43_DEFAULT_BRANDS,
  V43_APPLY_FLAGS,
  parseV43ApplyFlags,
  runV43ActiveReleaseApply,
  writeV43Reports,
} from "../lib/partner-intelligence/brand-explorer-active-release-apply.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...V43_DEFAULT_BRANDS];
  const flags = parseV43ApplyFlags(argv);
  return { brands, dryRun: !flags.apply, apply: flags.apply, flags };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V43_VERSION}] Active release apply (dryRun=${opts.dryRun}, apply=${opts.apply})`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  if (opts.apply) {
    console.log("  apply requires founder OK + gate confirms");
    console.log(`  flags: ${Object.values(V43_APPLY_FLAGS).filter((f) => opts.flags[Object.keys(V43_APPLY_FLAGS).find((k) => V43_APPLY_FLAGS[k] === f)]).join(" ") || "(see parse)"}`);
  }

  const report = await runV43ActiveReleaseApply(opts);
  const paths = writeV43Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: preApplyPass=${report.summary.preApplyPassCount}/${report.summary.brands} projectedFull=${report.summary.projectedFullProfileCount} missingFields=${(report.summary.releaseFieldsMissing || []).join("|") || "none"} applyExecuted=${report.applyExecuted} applyBlocked=${report.applyBlocked} incompleteLocked=${report.summary.incompleteLocked}`
  );

  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: live=${b.liveDisplay.brandExplorerDisplayState}/full=${b.liveDisplay.shouldRenderFullProfile} → projected=${b.projectedAfterRelease.brandExplorerDisplayState}/full=${b.projectedAfterRelease.shouldRenderFullProfile} | gate=${b.preApplyGate.pass ? "pass" : "fail:" + (b.preApplyGate.failed || []).join(",")}`
    );
    if (paths.proofPaths[b.brandSlug]) console.log(`    proof: ${paths.proofPaths[b.brandSlug]}`);
  }

  console.log("Exact apply command:");
  console.log(report.exactApplyCommand);

  if (opts.apply && report.applyBlocked) {
    console.error("Apply blocked:");
    for (const b of report.applyGateCheck.blockers || []) console.error(`  ${b}`);
    process.exit(2);
  }
  if (opts.apply && report.summary.applyErrors > 0) {
    console.error(`Apply errors: ${report.summary.applyErrors}`);
    process.exit(1);
  }

  console.log(
    "v43 complete — no content writes; Company Validated untouched; incomplete brands locked."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
