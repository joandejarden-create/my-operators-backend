#!/usr/bin/env node
/**
 * v37C-R1 — Lifestyle Batch Display Gating + Visual Candidate Integration.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_BRANDS,
  runV37CR1DisplayGatingVisualIntegration,
  writeV37CR1Reports,
  V37C_R1_VERSION,
} from "../lib/partner-intelligence/brand-explorer-v37c-r1-display-gating-visual-integration.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const VALIDATION_SCRIPTS = [
  "test:brand-explorer-active-profile-staged-apply",
  "test:partner-intelligence-publish-readiness",
  "test:partner-intelligence-profile-governance-publish",
];

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...DEFAULT_BRANDS];
  return { brands, dryRun: !argv.includes("--apply") };
}

function runValidation(scriptName) {
  const res = spawnSync("npm", ["run", scriptName], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return {
    script: scriptName,
    status: res.status === 0 ? "pass" : "fail",
    exitCode: res.status,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const report = await runV37CR1DisplayGatingVisualIntegration(opts);
  report.validationTests = VALIDATION_SCRIPTS.map(runValidation);
  const out = writeV37CR1Reports(report, ROOT);

  console.log(`Wrote ${out.jsonPath}`);
  console.log(`Wrote ${out.mdPath}`);
  console.log(`Wrote ${out.hiPath}`);
  console.log(`Wrote ${out.mgPath}`);
  console.log(`Wrote ${out.docPath}`);
  console.log(`Validation: ${V37C_R1_VERSION} (${opts.dryRun ? "dry-run" : "apply"})`);
  for (const t of report.validationTests) {
    console.log(`  ${t.script}: ${t.status}${t.exitCode != null ? ` (${t.exitCode})` : ""}`);
  }

  for (const brand of report.brandResults) {
    console.log(
      `  ${brand.brandSlug}: source_ready=${brand.batchReadiness.source_ready} visual_pack=${brand.batchReadiness.visual_candidate_pack_ready} asset_pack=${brand.batchReadiness.asset_pack_ready} draft_allowed=${brand.batchReadiness.apply_draft_allowed}`
    );
  }

  const failed = report.validationTests.filter((t) => t.status !== "pass");
  if (failed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
