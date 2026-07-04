/**
 * List Brand Basics + presentation row counts for name patterns.
 * Usage: node scripts/audit-brand-explorer-basics.mjs [--name "Radisson Blu"]
 */
import "../load-env.js";
import Airtable from "airtable";

const BASICS = "Brand Setup - Brand Basics";
const PRESENTATION = "Brand Setup - Brand Explorer Presentation";
const LINK_FIELDS = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

function parseArgs() {
  const nameIdx = process.argv.indexOf("--name");
  const patterns =
    nameIdx >= 0 && process.argv[nameIdx + 1]
      ? [process.argv[nameIdx + 1]]
      : ["Radisson Blu", "Radisson Blu (Choice)", "Radisson (Choice)", "Radisson"];
  return patterns;
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  return new Airtable({ apiKey: key }).base(baseId);
}

async function findBasics(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const formula = `OR({Brand Name} = "${esc}", FIND("${esc}", {Brand Name}))`;
  return base(BASICS).select({ filterByFormula: formula, maxRecords: 10 }).all();
}

function brandLinkIds(rec) {
  for (const f of LINK_FIELDS) {
    const v = rec.get(f);
    if (Array.isArray(v) && v.length) return v.map(String);
  }
  return [];
}

async function presentationForBrand(base, basicsId, brandName) {
  const escapedName = String(brandName || "").replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (records) => {
    for (const r of records) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  if (escapedName) {
    try {
      pushAll(
        await base(PRESENTATION)
          .select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 500 })
          .all()
      );
    } catch {
      /* optional */
    }
    try {
      pushAll(
        await base(PRESENTATION)
          .select({ filterByFormula: `{Brand} = "${escapedName}"`, maxRecords: 500 })
          .all()
      );
    } catch {
      /* optional */
    }
  }
  const rows = merged;
  const slots = new Map();
  for (const r of rows) {
    const sk = String(r.get("Slot Key") || "").trim();
    if (!sk) continue;
    slots.set(sk, (slots.get(sk) || 0) + 1);
  }
  return { count: rows.length, slots };
}

async function main() {
  const patterns = parseArgs();
  const base = getBase();
  for (const name of patterns) {
    const basics = await findBasics(base, name);
    console.log("\n===", name, "===");
    if (!basics.length) {
      console.log("  (no Brand Basics row)");
      continue;
    }
    for (const b of basics) {
      const bName = b.get("Brand Name");
      const { count, slots } = await presentationForBrand(base, b.id, bName);
      console.log(`  Basics ${b.id} | ${bName}`);
      console.log(`  Presentation rows: ${count} | unique slot keys: ${slots.size}`);
      const sample = [...slots.keys()].sort().slice(0, 12);
      if (sample.length) console.log(`  Sample slots: ${sample.join(", ")}${slots.size > 12 ? "…" : ""}`);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
