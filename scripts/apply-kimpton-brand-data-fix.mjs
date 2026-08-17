/**
 * Fix Kimpton Brand Setup + Explorer presentation after source-pipeline data quality issues:
 * - Re-seed Brand Basics / child tables from clean fixture (no inline "— Source:" citations)
 * - Replace presentation rows from brand-explorer-presentation-kimpton-full.json
 * - Rebuild loyalty.* slots without source suffixes
 *
 *   node scripts/apply-kimpton-brand-data-fix.mjs
 *   node scripts/apply-kimpton-brand-data-fix.mjs --apply
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

function run(label, cmd, args) {
  console.log(`\n=== ${label} ===`);
  const r = spawnSync(cmd, args, { cwd: ROOT, stdio: "inherit", shell: process.platform === "win32" });
  if (r.status !== 0) {
    console.error(`${label} failed (exit ${r.status})`);
    process.exit(r.status || 1);
  }
}

const dry = APPLY ? [] : ["--dry-run"];

run(
  "1/3 Brand Setup (clean fixture)",
  "node",
  ["scripts/seed-kimpton-brand-setup.mjs", ...(APPLY ? ["--apply", "--overwrite"] : ["--dry-run"])]
);

run(
  "2/3 Brand Explorer presentation (full fixture)",
  "node",
  [
    "scripts/apply-brand-explorer-presentation-fixture.mjs",
    "--brand-record-id",
    "recCKuXCmGvxHPfb3",
    "--fixture",
    "fixtures/brand-explorer-presentation-kimpton-full.json",
    ...(APPLY ? ["--replace"] : ["--dry-run"]),
  ]
);

run(
  "3/5 Opening & conversion path (economics tab)",
  "node",
  [
    "scripts/apply-brand-explorer-presentation-fixture.mjs",
    "--brand-record-id",
    "recCKuXCmGvxHPfb3",
    "--fixture",
    "fixtures/brand-explorer-presentation-kimpton-opening-path.json",
    ...(APPLY ? ["--replace-slot-prefix", "economics.opening.step"] : ["--dry-run"]),
  ]
);

if (APPLY) {
  run(
    "3b/5 Opening process summary",
    "node",
    [
      "scripts/apply-brand-explorer-presentation-fixture.mjs",
      "--brand-record-id",
      "recCKuXCmGvxHPfb3",
      "--fixture",
      "fixtures/brand-explorer-presentation-kimpton-opening-path.json",
      "--replace-slot-prefix",
      "economics.opening.process",
    ]
  );
}

run(
  "4/5 Flexibility indicators (canonical levels)",
  "node",
  [
    "scripts/apply-brand-explorer-presentation-fixture.mjs",
    "--brand-record-id",
    "recCKuXCmGvxHPfb3",
    "--fixture",
    "fixtures/brand-explorer-presentation-kimpton-flexibility.json",
    ...(APPLY ? ["--replace-slot-prefix", "operations.flexibility."] : ["--dry-run"]),
  ]
);

run(
  "5/5 Loyalty presentation (IHG tiers, no source suffixes)",
  "node",
  ["scripts/apply-kimpton-loyalty-presentation.mjs", ...(APPLY ? ["--apply"] : [])]
);

console.log(APPLY ? "\nKimpton brand data fix applied." : "\nDry run complete. Re-run with --apply to write Airtable.");
