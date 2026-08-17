/**
 * Remove "— Source: …" lines from Brand Explorer presentation rows in Airtable.
 * Brand Explorer is owner-facing — source evidence belongs in PI / Hero Data Source only.
 *
 *   node scripts/strip-brand-explorer-source-lines.mjs --brand-name "Kimpton Hotels"
 *   node scripts/strip-brand-explorer-source-lines.mjs --brand-record-id recCKuXCmGvxHPfb3 --apply
 */
import "../load-env.js";
import Airtable from "airtable";
import { sanitizeExternalCopy } from "../lib/external-owner-copy.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const TEXT_FIELDS = [
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Owner Objective",
  "Case Summary Brand Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
];

function parseArgs(argv) {
  const args = argv.slice(2);
  const kv = {};
  let apply = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--apply") apply = true;
    else if (args[i].startsWith("--") && args[i + 1] && !args[i + 1].startsWith("--")) {
      kv[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }
  return {
    apply,
    brandName: String(kv["brand-name"] || "").trim(),
    brandRecordId: String(kv["brand-record-id"] || "").trim(),
  };
}

function needsStrip(value) {
  const s = String(value || "");
  return /—\s*Source:/i.test(s) || /;\s*Kimpton stats:/i.test(s);
}

async function selectForBrand(base, brandRecordId, brandName) {
  const escapedId = brandRecordId.replace(/"/g, '\\"');
  const escapedName = brandName.replace(/"/g, '\\"');
  const formulas = [
    `FIND("${escapedId}", ARRAYJOIN({Brand})) > 0`,
    brandName ? `{Brand Name} = "${escapedName}"` : null,
  ].filter(Boolean);
  const seen = new Map();
  for (const formula of formulas) {
    try {
      const rows = await base(TABLE).select({ filterByFormula: formula, pageSize: 100 }).all();
      for (const r of rows) seen.set(r.id, r);
    } catch {
      /* optional column */
    }
  }
  return [...seen.values()];
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.brandRecordId && !opts.brandName) {
    console.error("Pass --brand-name or --brand-record-id");
    process.exit(1);
  }

  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  let brandRecordId = opts.brandRecordId;
  if (!brandRecordId && opts.brandName) {
    const basics = await new Airtable({ apiKey: key })
      .base(baseId)("Brand Setup - Brand Basics")
      .select({
        filterByFormula: `{Brand Name} = "${opts.brandName.replace(/"/g, '\\"')}"`,
        maxRecords: 1,
      })
      .firstPage();
    if (!basics.length) throw new Error(`Brand not found: ${opts.brandName}`);
    brandRecordId = basics[0].id;
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await selectForBrand(base, brandRecordId, opts.brandName);
  console.log(`Found ${rows.length} presentation row(s).`);

  const updates = [];
  for (const rec of rows) {
    const patch = {};
    for (const field of TEXT_FIELDS) {
      const raw = rec.get(field);
      if (raw == null || raw === "") continue;
      if (!needsStrip(raw)) continue;
      const clean = sanitizeExternalCopy(String(raw));
      if (clean !== String(raw).trim()) patch[field] = clean;
    }
    if (Object.keys(patch).length) {
      updates.push({ id: rec.id, slot: rec.get("Slot Key"), patch });
    }
  }

  if (!updates.length) {
    console.log("No rows contain — Source: lines.");
    return;
  }

  console.log(`${opts.apply ? "Will update" : "Dry run:"} ${updates.length} row(s)`);
  for (const u of updates.slice(0, 8)) {
    console.log(`  ${u.slot} (${u.id})`);
  }
  if (updates.length > 8) console.log(`  …and ${updates.length - 8} more`);

  if (!opts.apply) {
    console.log("\nPass --apply to write cleaned copy to Airtable.");
    return;
  }

  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10).map((u) => ({ id: u.id, fields: u.patch }));
    await base(TABLE).update(chunk);
  }
  console.log("Done.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
