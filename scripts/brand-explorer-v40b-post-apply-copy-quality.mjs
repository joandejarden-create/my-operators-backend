#!/usr/bin/env node
/**
 * v40B — Post-apply copy quality audit + founder review packets (read-only).
 *
 * Audits full profile via internal preview + Presentation + Brand Library API.
 * External quality lock only confirms profiles remain hidden.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V40B_VERSION,
  V40B_DEFAULT_BRANDS,
  runV40BPostApplyCopyQuality,
  writeV40BReports,
} from "../lib/partner-intelligence/brand-explorer-v40b-post-apply-copy-quality.js";

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
      : [...V40B_DEFAULT_BRANDS];
  if (argv.includes("--apply")) {
    console.error("v40B refuses --apply. Read-only / dry-run only.");
    process.exit(2);
  }
  return { brands, dryRun: true };
}

function runNpm(args) {
  const res = spawnSync("npm", ["run", ...args], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return { args, status: res.status === 0 ? "pass" : "fail", exitCode: res.status };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V40B_VERSION}] Post-apply copy quality (dryRun=true, internalPreview=on)`);

  const report = await runV40BPostApplyCopyQuality(opts);
  const paths = writeV40BReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(
    `Summary: forbiddenClean=${report.summary.forbiddenCleanCount}/${report.summary.brandsAudited} founderReady=${report.summary.founderVisualReviewReadyCount} moreRemediation=${report.summary.moreRemediationRequiredCount} notOwnerReady=${report.summary.notOwnerReadyCount}`
  );

  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: ${b.founderDecision.decision} | forbidden=${b.forbiddenLanguage.pass ? "pass" : "fail"} | gallery=${b.visuals.galleryCount} openings=${b.visuals.propertyExampleCount} | mechanical=${b.mechanicalCopy.hitCount}`
    );
    console.log(`    founder packet: ${paths.founderPaths[b.brandSlug]}`);
  }

  const skipRegression = process.argv.includes("--skip-regression");
  if (!skipRegression) {
    const brandList = opts.brands.join(",");
    const validations = [
      runNpm([
        "brand-explorer-v39-active-release-audit",
        "--",
        "--brands",
        brandList,
        "--dry-run",
      ]),
      runNpm(["test:brand-explorer-external-quality-lock", "--", "--brands", brandList]),
      runNpm(["test:brand-explorer-active-profile-staged-apply"]),
      runNpm(["test:partner-intelligence-publish-readiness"]),
      runNpm(["test:partner-intelligence-profile-governance-publish"]),
    ];
    report.regression = validations;
    writeV40BReports(report);
    const failed = validations.filter((v) => v.status !== "pass");
    if (failed.length) {
      console.error("Validation failed:");
      for (const f of failed) console.error(`  npm run ${f.args.join(" ")}`);
      process.exit(1);
    }
  }

  console.log("v40B complete — read-only; no Airtable writes; no unlock; no active approval.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
