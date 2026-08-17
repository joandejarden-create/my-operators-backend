/**
 * Apply Curio-specific Brand Explorer fixes — removes Kimpton/IHG template carryover.
 *
 *   npm run apply-curio-presentation-overrides -- --dry-run
 *   npm run apply-curio-presentation-overrides -- --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import {
  buildCurioCleanFixture,
  getCurioCleanupPatchRows,
  auditCurioForbiddenCopy,
} from "../lib/curio-brand-explorer-presentation-overrides.js";
import { overlayCurioCalaMaterials } from "../lib/curio-brand-explorer-cala-materials.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BRAND_NAME = "Curio Collection by Hilton";
const SOURCE_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-from-sources.json");
const FULL_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-full.json");
const PATCH_FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-cleanup-patch.json");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

const APPLY_PREFIXES = [
  "overview.",
  "valueOwners.",
  "hero.",
  "insight.",
  "loyalty.",
  "commercial.",
  "footprint.geo_intro",
  "footprint.region.am",
  "footprint.region.eu",
  "footprint.growth",
  "footprint.editorial",
  "operations.model.",
  "operations.operator_compat.",
  "economics.",
];

function patchFixtureFile(fixturePath) {
  if (!fs.existsSync(fixturePath)) {
    console.warn("Skip missing fixture:", fixturePath);
    return null;
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  buildCurioCleanFixture(fixture);
  overlayCurioCalaMaterials(fixture.rows);
  fs.writeFileSync(fixturePath, JSON.stringify(fixture, null, 2));
  console.log("Patched", fixturePath);
  return fixture;
}

function main() {
  const full = patchFixtureFile(FULL_FIXTURE);
  patchFixtureFile(SOURCE_FIXTURE);

  if (!full) {
    console.error("Missing", FULL_FIXTURE);
    process.exit(1);
  }

  const patchRows = getCurioCleanupPatchRows(full.rows);
  const violations = auditCurioForbiddenCopy(patchRows);
  if (violations.length) {
    console.error("Kimpton/IHG terms remain after cleanup:");
    for (const v of violations) console.error("  -", v);
    process.exit(1);
  }

  const patchOut = {
    targetBrandBasicsName: BRAND_NAME,
    brandNameFallback: BRAND_NAME,
    instructions: `Curio cleanup — Kimpton/IHG template removal. Apply: npm run apply-curio-presentation-overrides -- --apply`,
    rows: patchRows,
  };
  fs.writeFileSync(PATCH_FIXTURE, JSON.stringify(patchOut, null, 2));
  console.log("Wrote", PATCH_FIXTURE, `(${patchRows.length} rows)`);

  const similar = patchRows.filter((r) => r.slotKey === "insight.similar");
  console.log("\nSimilar Brands:");
  for (const row of similar) console.log(`  • ${row.title}`);

  const portfolio = patchRows.find((r) => r.slotKey === "overview.portfolio_context");
  if (portfolio) {
    console.log("\nPortfolio Context tier:", portfolio.title);
  }

  if (DRY_RUN) {
    console.log("\nDry run. Re-run with --apply to push to Airtable.");
    return;
  }

  const applyScript = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");
  for (const prefix of APPLY_PREFIXES) {
    console.log("\nApplying slot prefix:", prefix);
    const res = spawnSync(
      "node",
      [
        applyScript,
        "--brand-name",
        BRAND_NAME,
        "--fixture",
        PATCH_FIXTURE,
        "--replace-slot-prefix",
        prefix,
      ],
      { stdio: "inherit", cwd: ROOT, env: process.env }
    );
    if (res.status !== 0) process.exit(res.status || 1);
  }

  console.log("\nDone — Curio Brand Explorer cleanup applied to Airtable.");
  console.log("Refresh:", "/brand-explorer-combined.html?brand=Curio%20Collection%20by%20Hilton");
}

main();
