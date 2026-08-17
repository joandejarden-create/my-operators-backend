/**
 * Apply Kimpton CALA Explorer content (Radisson Blu parity):
 * - overview.portfolio_context + overview positioning
 * - footprint.openings (Footprint tab property cards — NOT materials.caseStudy)
 * - materials.caseStudy (5 CALA hotels)
 * - footprint.momentum (hotel opening announcements)
 *
 *   node scripts/apply-kimpton-cala-explorer-fixtures.mjs
 *   node scripts/apply-kimpton-cala-explorer-fixtures.mjs --apply
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = process.argv.includes("--apply");
const BRAND_RECORD_ID = "recCKuXCmGvxHPfb3";
const APPLY_SCRIPT = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");

function run(fixture, prefix) {
  const args = [
    APPLY_SCRIPT,
    "--brand-record-id",
    BRAND_RECORD_ID,
    "--fixture",
    path.join(ROOT, fixture),
    "--replace-slot-prefix",
    prefix,
  ];
  if (!APPLY) args.push("--dry-run");
  console.log(`\n>> ${prefix} ← ${fixture}`);
  const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit" });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

const steps = [
  ["fixtures/brand-explorer-presentation-kimpton-portfolio-overview.json", "overview.portfolio_context"],
  ["fixtures/brand-explorer-presentation-kimpton-portfolio-overview.json", "overview.typical_use_case"],
  ["fixtures/brand-explorer-presentation-kimpton-portfolio-overview.json", "overview.relative_positioning"],
  ["fixtures/brand-explorer-presentation-kimpton-footprint-openings.json", "footprint.openings"],
  ["fixtures/brand-explorer-presentation-kimpton-case-studies.json", "materials.caseStudy"],
  ["fixtures/brand-explorer-presentation-kimpton-footprint-momentum.json", "footprint.momentum"],
];

for (const [fixture, prefix] of steps) {
  run(fixture, prefix);
}

console.log(APPLY ? "\nKimpton CALA Explorer fixtures applied." : "\nDry run complete. Pass --apply to write Airtable.");
