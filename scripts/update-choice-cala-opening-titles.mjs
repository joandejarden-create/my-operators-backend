/**
 * Title-only cleanup for CHI footprint.openings rows.
 * Preserves Body, Images, and all other fields.
 *
 * Usage:
 *   node scripts/update-choice-cala-opening-titles.mjs --dry-run
 *   node scripts/update-choice-cala-opening-titles.mjs
 *   node scripts/update-choice-cala-opening-titles.mjs --brand "Radisson Blu by Choice"
 */
import "../load-env.js";
import Airtable from "airtable";

const BASICS = "Brand Setup - Brand Basics";
const PRESENTATION = "Brand Setup - Brand Explorer Presentation";
const SLOT = "footprint.openings";

function parseArgs(argv) {
  const args = argv.slice(2);
  const i = args.indexOf("--brand");
  return {
    dryRun: args.includes("--dry-run"),
    brandFilter: i >= 0 ? String(args[i + 1] || "").trim() : "",
  };
}

function normalizeTitle(rawTitle) {
  let s = String(rawTitle || "").trim();
  if (!s) return s;
  s = s.replace(/\s*\((?:CALA|.*?comp).*?\)\s*$/i, "");
  s = s.replace(/\s+—\s+/g, " ");
  s = s.replace(/\s{2,}/g, " ").trim();
  return s;
}

async function listChiBrands(base) {
  const rows = await base(BASICS).select({ maxRecords: 500 }).all();
  return rows
    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))
    .map((r) => String(r.get("Brand Name") || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

async function selectOpeningRows(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (rows) => {
    for (const r of rows) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    pushAll(
      await base(PRESENTATION)
        .select({
          filterByFormula: `AND({Slot Key} = "${SLOT}", {Brand Name} = "${esc}")`,
          maxRecords: 100,
        })
        .all()
    );
  } catch {
    // Optional Brand Name field.
  }
  try {
    pushAll(
      await base(PRESENTATION)
        .select({
          filterByFormula: `AND({Slot Key} = "${SLOT}", {Brand} = "${esc}")`,
          maxRecords: 100,
        })
        .all()
    );
  } catch {
    // Schema differences.
  }
  return merged;
}

async function updateBatch(base, rows, dryRun) {
  for (let i = 0; i < rows.length; i += 10) {
    const batch = rows.slice(i, i + 10);
    if (dryRun) continue;
    await base(PRESENTATION).update(batch);
  }
}

async function main() {
  const { dryRun, brandFilter } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  let brands = await listChiBrands(base);
  if (brandFilter) {
    brands = brands.filter((b) => b === brandFilter);
    if (!brands.length) throw new Error(`No CHI Brand Basics row named "${brandFilter}"`);
  }

  let total = 0;
  console.log(`${dryRun ? "[dry-run] " : ""}Title cleanup for ${brands.length} CHI brands`);
  for (const brandName of brands) {
    const rows = await selectOpeningRows(base, brandName);
    const updates = [];
    for (const r of rows) {
      const before = String(r.get("Title") || "").trim();
      const after = normalizeTitle(before);
      if (after && after !== before) {
        updates.push({ id: r.id, fields: { Title: after } });
      }
    }
    if (!updates.length) {
      console.log(`- ${brandName}: no title changes`);
      continue;
    }
    console.log(`- ${brandName}: ${updates.length} title update(s)`);
    for (const u of updates.slice(0, 3)) {
      const before = String(rows.find((r) => r.id === u.id)?.get("Title") || "").trim();
      console.log(`  · "${before}" -> "${u.fields.Title}"`);
    }
    if (updates.length > 3) console.log("  · ...");
    await updateBatch(base, updates, dryRun);
    total += updates.length;
  }
  console.log(`${dryRun ? "Would update" : "Updated"} ${total} title row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
