/**
 * Sync Brand Setup - Brand Basics from CHI Brands Architecture Oct 2025 PDF.
 * Overwrites allowlisted fields when they differ from architecture (not patch-missing-only).
 *
 *   node scripts/apply-choice-brand-basics-from-architecture-oct2025.mjs --dry-run
 *   node scripts/apply-choice-brand-basics-from-architecture-oct2025.mjs
 *   node scripts/apply-choice-brand-basics-from-architecture-oct2025.mjs --brand "Clarion"
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  ARCHITECTURE_SYNC_FIELD_ALLOWLIST,
  CHOICE_ARCHITECTURE_NOT_IN_DOC,
  buildArchitectureBasicsFields,
  resolveArchForAirtableName,
} from "../lib/choice-brand-architecture-oct2025.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PARENT = "Choice Hotels International";

function parseArgs(argv) {
  const args = argv.slice(2);
  const flags = new Set(args.filter((a) => a.startsWith("--")));
  const brandIdx = args.indexOf("--brand");
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    brandFilter: brandIdx >= 0 ? args[brandIdx + 1] : null,
  };
}

function norm(s) {
  return String(s || "")
    .replace(/\r\n/g, "\n")
    .trim();
}

function fieldsEqual(current, next) {
  return norm(current) === norm(next);
}

function isChoiceParent(rec) {
  return String(rec.get("Parent Company") || "").includes(PARENT);
}

async function main() {
  const opts = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const records = await base(BASICS_TABLE)
    .select({
      filterByFormula: `FIND("${PARENT}", {Parent Company})`,
      fields: ["Brand Name", "Parent Company", ...ARCHITECTURE_SYNC_FIELD_ALLOWLIST],
    })
    .all();

  let updated = 0;
  let skipped = 0;
  let noArch = 0;

  console.log(opts.dryRun ? "DRY RUN (pass --apply to write)\n" : "APPLY MODE\n");

  for (const rec of records) {
    const brandName = String(rec.get("Brand Name") || "").trim();
    if (!brandName) continue;
    if (opts.brandFilter && brandName !== opts.brandFilter) continue;
    if (!isChoiceParent(rec)) continue;

    if (CHOICE_ARCHITECTURE_NOT_IN_DOC.includes(brandName)) {
      console.log(`${brandName}: skip (not in Oct 2025 architecture deck)`);
      noArch++;
      continue;
    }

    const arch = resolveArchForAirtableName(brandName);
    if (!arch) {
      console.log(`${brandName}: skip (no architecture mapping)`);
      noArch++;
      continue;
    }

    const desired = buildArchitectureBasicsFields(brandName);
    if (!desired) continue;

    const patch = {};
    const changes = [];

    for (const field of ARCHITECTURE_SYNC_FIELD_ALLOWLIST) {
      const next = desired[field];
      if (next == null || next === "") continue;
      const current = rec.get(field);
      if (fieldsEqual(current, next)) continue;
      patch[field] = next;
      changes.push({
        field,
        from: current ? String(current).slice(0, 80) + (String(current).length > 80 ? "…" : "") : "(empty)",
        to: next.slice(0, 80) + (next.length > 80 ? "…" : ""),
      });
    }

    if (!Object.keys(patch).length) {
      console.log(`${brandName}: already aligned`);
      skipped++;
      continue;
    }

    console.log(`\n${brandName} (${rec.id})`);
    for (const c of changes) {
      console.log(`  ${c.field}:`);
      console.log(`    was: ${c.from}`);
      console.log(`    →   ${c.to}`);
    }

    if (!opts.dryRun) {
      await base(BASICS_TABLE).update(rec.id, patch);
      console.log(`  ✓ updated ${Object.keys(patch).length} field(s)`);
    }
    updated++;
  }

  console.log(`\nDone. ${updated} brand(s) ${opts.dryRun ? "would update" : "updated"}, ${skipped} already aligned, ${noArch} skipped (no deck entry).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
