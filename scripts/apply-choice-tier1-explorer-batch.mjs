/**
 * Apply full Tier 1 Explorer presentation fixtures to Airtable.
 * Default: --only-missing (never deletes existing rows or Image attachments).
 *
 *   node scripts/apply-choice-tier1-explorer-batch.mjs --dry-run
 *   node scripts/apply-choice-tier1-explorer-batch.mjs
 *   node scripts/apply-choice-tier1-explorer-batch.mjs --brand "Sleep Inn"
 *
 * Destructive (opt-in only):
 *   node scripts/apply-choice-tier1-explorer-batch.mjs --replace
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { TIER1_BRANDS } from "./lib/choice-tier1-explorer-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");

const dryRun = process.argv.includes("--dry-run");
const useReplace = process.argv.includes("--replace");
const onlyBrand = process.argv.find((a, i) => process.argv[i - 1] === "--brand") || "";
const brands = onlyBrand
  ? TIER1_BRANDS.filter((b) => b.name === onlyBrand)
  : TIER1_BRANDS;

if (onlyBrand && brands.length === 0) {
  console.error(`Unknown brand: ${onlyBrand}`);
  process.exit(1);
}

if (useReplace) {
  console.warn(
    "\nWARNING: --replace deletes ALL presentation rows per brand (including uploaded Images).\n" +
      "Prefer default --only-missing unless you intend a full rebuild from fixtures.\n"
  );
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
  ];
  if (useReplace) args.push("--replace");
  else args.push("--only-missing");
  if (dryRun) args.push("--dry-run");
  console.log(`\n=== ${p.name} ===`);
  const r = spawnSync(process.execPath, args, { stdio: "inherit", cwd: ROOT });
  if (r.status !== 0) failed++;
}

if (failed) {
  console.error(`\n${failed} brand(s) failed.`);
  process.exit(1);
}
console.log(
  `\nAll ${brands.length} brand(s) ${dryRun ? "dry-run " : ""}completed (${useReplace ? "--replace" : "--only-missing"}).`
);
