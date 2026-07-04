/**
 * Apply full Brand Explorer presentation + basics patch for Radisson RED and Radisson Individual (Choice).
 *
 *   node scripts/apply-radisson-red-individual-choice-all-fixtures.mjs --dry-run
 *   node scripts/apply-radisson-red-individual-choice-all-fixtures.mjs
 *
 * Generate fixtures first:
 *   node scripts/generate-choice-tier1-explorer-full.mjs --brand "Radisson RED  (Choice)"
 *   node scripts/generate-choice-tier1-explorer-full.mjs --brand "Radisson Individual (Choice)"
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APPLY_PRESENTATION = path.join(ROOT, "scripts/apply-brand-explorer-presentation-fixture.mjs");
const APPLY_BASICS = path.join(ROOT, "scripts/apply-brand-basics-patch-missing.mjs");
const dryRun = process.argv.includes("--dry-run");

/** @type {{ name: string; recordId: string; slug: string; basicsFixture?: string }[]} */
const BRANDS = [
  {
    name: "Radisson RED  (Choice)",
    recordId: process.env.RADISSON_RED_CHOICE_BASICS_ID || "recmKqo7M7mLZgRqQ",
    slug: "radisson-red-choice",
    basicsFixture: "fixtures/brand-basics-from-choice-materials/radisson-red-choice.json",
  },
  {
    name: "Radisson Individual (Choice)",
    recordId: process.env.RADISSON_INDIVIDUAL_CHOICE_BASICS_ID || "recRyvM8OmLlDj9G7",
    slug: "radisson-individual-choice",
    basicsFixture: "fixtures/brand-basics-from-choice-materials/radisson-individual-choice.json",
  },
];

function run(nodeScript, args) {
  const full = dryRun ? [...args, "--dry-run"] : args;
  const r = spawnSync(process.execPath, [nodeScript, ...full], { cwd: ROOT, stdio: "inherit" });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

for (const b of BRANDS) {
  console.log(`\n========== ${b.name} (${b.recordId}) ==========`);

  if (b.basicsFixture) {
    console.log(">> Brand Basics (patch missing)");
    run(APPLY_BASICS, [
      "--brand-record-id",
      b.recordId,
      "--fixture",
      path.join(ROOT, b.basicsFixture),
    ]);
  }

  const fullFixture = `fixtures/brand-explorer-presentation-${b.slug}-full.json`;
  console.log(">> Brand Explorer Presentation (--replace full fixture)");
  run(APPLY_PRESENTATION, [
    "--brand-record-id",
    b.recordId,
    "--fixture",
    path.join(ROOT, fullFixture),
    "--replace",
  ]);
}

console.log(`\nDone${dryRun ? " (dry-run)" : ""}.`);
