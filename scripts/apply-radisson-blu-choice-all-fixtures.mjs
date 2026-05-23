/**
 * Push all Radisson Blu (Choice) Brand Explorer fixtures (replace by slot prefix).
 * Run: node scripts/build-radisson-blu-tab-fixtures.mjs && node scripts/apply-radisson-blu-choice-all-fixtures.mjs
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BRAND = "Radisson Blu (Choice)";
const APPLY = path.join(ROOT, "scripts/apply-brand-explorer-presentation-fixture.mjs");
/** Avoid npm on Windows splitting "Radisson Blu (Choice)" at spaces/parens. */
const BRAND_RECORD_ID = process.env.RADISSON_BLU_CHOICE_BASICS_ID || "recWPEvxBQxVVzSq3";

function runApply(fixture, prefix) {
  const args = [
    APPLY,
    "--brand-record-id",
    BRAND_RECORD_ID,
    "--fixture",
    path.join(ROOT, fixture),
    "--replace-slot-prefix",
    prefix,
  ];
  console.log("\n>>", prefix, fixture, `→ ${BRAND_RECORD_ID}`);
  const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit" });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

const steps = [
  ["fixtures/brand-explorer-presentation-radisson-blu.example.json", "hero."],
  ["fixtures/brand-explorer-presentation-radisson-blu.example.json", "overview."],
  ["fixtures/brand-explorer-presentation-radisson-blu.example.json", "operations."],
  ["fixtures/brand-explorer-presentation-radisson-blu.example.json", "valueOwners."],
  ["fixtures/brand-explorer-presentation-radisson-blu.example.json", "loyalty."],
  ["fixtures/brand-explorer-presentation-radisson-blu.example.json", "insight.summary"],
  ["fixtures/brand-explorer-presentation-standards-radisson-blu.json", "standards."],
  ["fixtures/brand-explorer-presentation-radisson-blu-materials.json", "materials.file"],
  ["fixtures/brand-explorer-presentation-economics-radisson-blu.json", "economics."],
  ["fixtures/brand-explorer-presentation-radisson-blu-case-studies.json", "materials.caseStudy"],
  ["fixtures/brand-explorer-presentation-radisson-blu-gallery.json", "materials.gallery."],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-openings.json", "footprint.openings"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-momentum.json", "footprint.momentum"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.geo"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.region."],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.growth"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.editorial"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint.json", "footprint.geo.summary"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint.json", "footprint.growth.narrative"],
  ["fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json", "footprint.portfolio_mix"],
  ["fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json", "operations.compliance."],
  ["fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json", "insight.similar"],
];

for (const [fixture, prefix] of steps) {
  runApply(fixture, prefix);
}

console.log("\nAll Radisson Blu (Choice) fixture applies finished.");
