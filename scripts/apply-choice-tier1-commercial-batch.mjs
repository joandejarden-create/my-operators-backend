/**
 * Refresh only commercial.* presentation rows (Project impact copy) per Tier 1 brand.
 * Deletes commercial slot rows only — does not touch materials.gallery, footprint.openings, etc.
 *
 *   node scripts/apply-choice-tier1-commercial-batch.mjs --dry-run
 *   node scripts/apply-choice-tier1-commercial-batch.mjs
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { TIER1_BRANDS } from "./lib/choice-tier1-explorer-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");

const dryRun = process.argv.includes("--dry-run");
const onlyBrand = process.argv.find((a, i) => process.argv[i - 1] === "--brand") || "";
const brands = onlyBrand
  ? TIER1_BRANDS.filter((b) => b.name === onlyBrand)
  : TIER1_BRANDS;

if (onlyBrand && brands.length === 0) {
  console.error(`Unknown brand: ${onlyBrand}`);
  process.exit(1);
}

let failed = 0;
for (const p of brands) {
  const fixture = `fixtures/brand-explorer-presentation-${p.slug}-full.json`;
  const args = [
    APPLY,
    "--brand-name",
    p.name,
    "--fixture",
    fixture,
    "--replace-slot-prefix",
    "commercial.",
  ];
  if (dryRun) args.push("--dry-run");
  console.log(`\n=== ${p.name} (commercial.* only) ===`);
  const r = spawnSync(process.execPath, args, { stdio: "inherit", cwd: ROOT });
  if (r.status !== 0) failed++;
}

if (failed) {
  console.error(`\n${failed} brand(s) failed.`);
  process.exit(1);
}
console.log(`\nCommercial slots updated for ${brands.length} brand(s) ${dryRun ? "(dry-run)" : ""}.`);
