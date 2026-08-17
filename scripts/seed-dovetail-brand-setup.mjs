/**
 * Seed Brand Setup for Dovetail + Co: Brand Basics (public-site copy) + empty linked child tabs.
 * Links brands on Company Profile "Brands You Operate / Support".
 *
 *   node scripts/seed-dovetail-brand-setup.mjs --dry-run
 *   node scripts/seed-dovetail-brand-setup.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE_PATH = path.join(ROOT, "fixtures", "dovetail-brand-basics.json");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const COMPANY_TABLE = "Company Profile";
const BRANDS_LINK_FIELD = "Brands You Operate / Support";

/** Basics column → child table (same as clone-brand-setup-linked-tables.mjs). */
const LINKED_TABLES = [
  { linkField: "Brand Setup - Sustainability & ESG", table: "Brand Setup - Sustainability & ESG" },
  { linkField: "Brand Setup - Brand Footprint", table: "Brand Setup - Brand Footprint" },
  { linkField: "Brand Setup - Project Fit", table: "Brand Setup - Project Fit" },
  { linkField: "Brand Setup - Portfolio & Performance", table: "Brand Setup - Portfolio & Performance" },
  { linkField: "Brand Setup - Brand Standards", table: "Brand Setup - Brand Standards" },
  { linkField: "Brand Setup - Fee Structure", table: "Brand Setup - Fee Structure" },
  { linkField: "Brand Setup - Deal Terms", table: "Brand Setup - Deal Terms" },
  { linkField: "Brand Setup - Operational Support", table: "Brand Setup - Operational Support" },
  { linkField: "Brand Setup - Legal Terms", table: "Brand Setup - Legal Terms" },
  { linkField: "Brand Setup - Loyalty & Commercial", table: "Brand Setup - Loyalty & Commercial" },
];

const BRAND_LINK_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

function parseArgs(argv) {
  const flags = new Set(argv.slice(2).filter((a) => a.startsWith("--")));
  return { dryRun: flags.has("--dry-run"), apply: flags.has("--apply") };
}

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function extractUnknownFieldName(err) {
  const msg = String(err?.message || err || "");
  const m =
    msg.match(/Unknown field name:\s*['"](.+?)['"]/i) ||
    msg.match(/Unknown field name:\s*(.+?)(?:\s|$)/i);
  return m ? m[1].trim() : null;
}

function extractInvalidFieldName(err) {
  const msg = String(err?.message || err || "");
  const m =
    msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i) ||
    msg.match(/Field "([^"]+)"/);
  return m ? m[1].trim() : null;
}

async function createWithFieldPruning(base, table, fields, { typecast = false } = {}) {
  let payload = { ...fields };
  const opts = typecast ? { typecast: true } : undefined;
  const removed = [];
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) {
      throw new Error(`No fields left to create in ${table} (removed: ${removed.join(", ")})`);
    }
    try {
      const [created] = await base(table).create([{ fields: payload }], opts);
      if (removed.length) {
        console.warn(`  create ${table}: dropped fields:`, removed.join(", "));
      }
      return created;
    } catch (err) {
      const unknown = extractUnknownFieldName(err);
      const invalid = extractInvalidFieldName(err);
      const bad = unknown || invalid;
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`createWithFieldPruning exceeded retries for ${table}`);
}

async function updateWithFieldPruning(base, table, recordId, fields, { typecast = false } = {}) {
  let payload = { ...fields };
  const opts = typecast ? { typecast: true } : undefined;
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return { updated: false, removed: [] };
    try {
      await base(table).update(recordId, payload, opts);
      return { updated: true };
    } catch (err) {
      const unknown = extractUnknownFieldName(err);
      const invalid = extractInvalidFieldName(err);
      const bad = unknown || invalid;
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        continue;
      }
      throw err;
    }
  }
  throw new Error(`updateWithFieldPruning exceeded retries for ${table} ${recordId}`);
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (rows.length > 1) {
    throw new Error(`Multiple Basics rows for "${brandName}": ${rows.map((r) => r.id).join(", ")}`);
  }
  return rows[0] || null;
}

function patchMissingOnly(record, fields) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value == null || value === "") continue;
    if (!isEmpty(record.get(key))) {
      skipped.push(key);
      continue;
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

async function createMinimalLinkedRow(base, table, brandName, basicsId, dryRun) {
  if (dryRun) return { id: "recDRYRUN_CHILD", dryRun: true };
  const tries = [
    { "Brand Name": brandName, Brand: [basicsId] },
    { "Brand Name": brandName, Brand_Basic_ID: [basicsId] },
    { "Brand Name": brandName, "Brand Setup - Brand Basics": [basicsId] },
    { "Brand Name": brandName },
  ];
  let lastErr;
  for (const fields of tries) {
    try {
      const [created] = await base(table).create([{ fields }]);
      return created;
    } catch (err) {
      lastErr = err;
      if (err.error === "UNKNOWN_FIELD_NAME" || /unknown field/i.test(String(err.message))) {
        continue;
      }
      throw err;
    }
  }
  throw lastErr || new Error(`Could not create minimal row in ${table}`);
}

async function ensureChildLinks(base, basicsRec, brandName, dryRun) {
  const basicsId = basicsRec.id;
  const basicsPatch = {};
  const created = [];

  for (const { linkField, table } of LINKED_TABLES) {
    const existing = basicsRec.get(linkField);
    if (Array.isArray(existing) && existing[0]) {
      console.log(`  ${table}: already linked (${existing[0]})`);
      continue;
    }

    const orphan = await base(table)
      .select({
        filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`,
        maxRecords: 1,
      })
      .firstPage();
    if (orphan.length) {
      basicsPatch[linkField] = [orphan[0].id];
      console.log(`  ${table}: reuse orphan ${orphan[0].id}`);
      continue;
    }

    if (dryRun) {
      console.log(`  ${table}: would create empty linked row`);
      basicsPatch[linkField] = ["recDRYRUN_CHILD"];
      continue;
    }

    const child = await createMinimalLinkedRow(base, table, brandName, basicsId, false);
    basicsPatch[linkField] = [child.id];
    created.push({ table, id: child.id });
    console.log(`  ${table}: created ${child.id}`);
  }

  if (!dryRun && Object.keys(basicsPatch).length) {
    await base(BASICS_TABLE).update(basicsId, basicsPatch);
    console.log(`  Basics ${basicsId}: wired ${Object.keys(basicsPatch).length} child link(s)`);
  }

  return { basicsPatch, created };
}

async function findCompany(base, companyName, companyRecordId) {
  if (companyRecordId) {
    try {
      return await base(COMPANY_TABLE).find(companyRecordId);
    } catch {
      throw new Error(`Company record not found: ${companyRecordId}`);
    }
  }
  const esc = companyName.replace(/"/g, '\\"');
  const rows = await base(COMPANY_TABLE)
    .select({ filterByFormula: `{Company Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (rows.length !== 1) {
    throw new Error(`Expected one company "${companyName}", got ${rows.length}`);
  }
  return rows[0];
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.dryRun && !opts.apply) {
    console.error("Pass --dry-run or --apply");
    process.exit(1);
  }
  const dryRun = opts.dryRun && !opts.apply;

  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const spec = JSON.parse(fs.readFileSync(FIXTURE_PATH, "utf8"));
  const base = new Airtable({ apiKey: key }).base(baseId);
  const parentCompany = String(spec.parentCompany || "Dovetail + Co").trim();
  const brandIds = [];

  console.log(dryRun ? "[dry-run]" : "[apply]", parentCompany);
  console.log("Fixture:", FIXTURE_PATH, "\n");

  for (const brand of spec.brands || []) {
    const brandName = String(brand.brandName || "").trim();
    if (!brandName) continue;
    const fields = { "Brand Name": brandName, ...(brand.fields || {}) };
    if (!fields["Parent Company"]) fields["Parent Company"] = parentCompany;

    console.log(`Brand: ${brandName}`);
    let basicsRec = await findBasicsByName(base, brandName);

    if (!basicsRec) {
      if (dryRun) {
        console.log("  Basics: would create");
        basicsRec = { id: "recDRYRUN_BASICS", get: () => undefined, fields: {} };
      } else {
        basicsRec = await createWithFieldPruning(base, BASICS_TABLE, fields, { typecast: true });
        console.log(`  Basics: created ${basicsRec.id}`);
      }
    } else {
      const parent = String(basicsRec.get("Parent Company") || "");
      if (parent && !parent.includes("Dovetail")) {
        console.warn(
          `  WARN: existing Parent Company "${parent}" — not overwriting; only filling empty fields`
        );
      }
      const { patch, skipped } = patchMissingOnly(basicsRec, fields);
      if (dryRun) {
        console.log(`  Basics: exists ${basicsRec.id}; would patch`, Object.keys(patch));
        if (skipped.length) console.log("  skip (already set):", skipped.join(", "));
      } else if (Object.keys(patch).length) {
        await updateWithFieldPruning(base, BASICS_TABLE, basicsRec.id, patch, { typecast: true });
        console.log(`  Basics: patched ${basicsRec.id}`, Object.keys(patch).join(", "));
        basicsRec = await base(BASICS_TABLE).find(basicsRec.id);
      } else {
        console.log(`  Basics: exists ${basicsRec.id} (no empty fields to patch)`);
      }
    }

    if (!dryRun) {
      await ensureChildLinks(base, basicsRec, brandName, false);
      brandIds.push(basicsRec.id);
    } else {
      await ensureChildLinks(base, basicsRec, brandName, true);
      brandIds.push(basicsRec.id);
    }
    console.log("");
  }

  const company = await findCompany(
    base,
    parentCompany,
    spec.companyRecordId || "reccQJUKO2RAY9zhE"
  );
  const existingBrandLinks = company.get(BRANDS_LINK_FIELD) || [];
  const existingIds = Array.isArray(existingBrandLinks)
    ? existingBrandLinks.filter((id) => typeof id === "string" && id.startsWith("rec"))
    : [];
  const merged = [...new Set([...existingIds, ...brandIds.filter((id) => id.startsWith("rec"))])];

  console.log(`Company: ${company.get("Company Name")} (${company.id})`);
  if (dryRun) {
    console.log(`Would set ${BRANDS_LINK_FIELD}:`, merged);
  } else {
    await base(COMPANY_TABLE).update(company.id, { [BRANDS_LINK_FIELD]: merged });
    console.log(`Updated ${BRANDS_LINK_FIELD} (${merged.length} brand link(s)):`, merged.join(", "));
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
