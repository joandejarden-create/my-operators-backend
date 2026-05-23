/**
 * Patch Overview tab presentation slots only (--replace-slot-prefix overview.) for Tier 1 brands.
 *
 *   node scripts/apply-choice-tier1-overview-batch.mjs --dry-run
 *   node scripts/apply-choice-tier1-overview-batch.mjs
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { TIER1_BRANDS } from "./lib/choice-tier1-explorer-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");

const dryRun = process.argv.includes("--dry-run");

for (const p of TIER1_BRANDS) {
  const fixture = `fixtures/brand-explorer-presentation-${p.slug}-full.json`;
  const args = [
    APPLY,
    "--brand-name",
    p.name,
    "--fixture",
    fixture,
    "--replace-slot-prefix",
    "overview.",
  ];
  if (dryRun) args.push("--dry-run");
  console.log(`\n=== ${p.name} (overview.*) ===`);
  const r = spawnSync(process.execPath, args, { stdio: "inherit", cwd: ROOT });
  if (r.status !== 0) process.exit(r.status || 1);
}
console.log(`\nDone. ${TIER1_BRANDS.length} brand(s).`);
