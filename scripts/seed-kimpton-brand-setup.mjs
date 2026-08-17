/**
 * Seed / enrich Brand Setup for Kimpton Hotels from fixtures/kimpton-brand-setup.json
 *
 *   node scripts/seed-kimpton-brand-setup.mjs --dry-run
 *   node scripts/seed-kimpton-brand-setup.mjs --apply
 *   node scripts/seed-kimpton-brand-setup.mjs --apply --overwrite
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_FIXTURE_PATH = path.join(ROOT, "fixtures", "kimpton-brand-setup.json");

const BASICS_TABLE = "Brand Setup - Brand Basics";

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

function parseArgs(argv) {
  const args = argv.slice(2);
  const flags = new Set(args.filter((a) => a.startsWith("--")));
  let fixture = DEFAULT_FIXTURE_PATH;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--fixture" && args[i + 1]) {
      fixture = path.isAbsolute(args[i + 1]) ? args[i + 1] : path.join(ROOT, args[i + 1]);
      break;
    }
  }
  return {
    dryRun: flags.has("--dry-run"),
    apply: flags.has("--apply"),
    overwrite: flags.has("--overwrite"),
    fixturePath: fixture,
  };
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
  const m = msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i) || msg.match(/Field "([^"]+)"/);
  return m ? m[1].trim() : null;
}

async function writeWithFieldPruning(base, table, recordId, fields, { typecast = true, create = false } = {}) {
  let payload = { ...fields };
  const removed = [];
  const api = base(table);
  const opts = typecast ? { typecast: true } : undefined;
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return { removed };
    try {
      if (create) {
        const [created] = await api.create([{ fields: payload }], opts);
        return { record: created, removed };
      }
      await api.update(recordId, payload, opts);
      return { removed };
    } catch (err) {
      const bad = extractUnknownFieldName(err) || extractInvalidFieldName(err);
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`writeWithFieldPruning exceeded retries for ${table}`);
}

async function findBasics(base, spec) {
  if (spec.basicsRecordId) {
    try {
      return await base(BASICS_TABLE).find(spec.basicsRecordId);
    } catch {
      console.warn("Basics record id not found:", spec.basicsRecordId);
    }
  }
  const esc = String(spec.brandName).replace(/"/g, '\\"');
  const rows = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 2 })
    .all();
  if (rows.length > 1) throw new Error(`Multiple Basics rows for ${spec.brandName}`);
  return rows[0] || null;
}

function patchFields(record, fields, overwrite) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value == null || value === "") continue;
    if (!overwrite && !isEmpty(record.get(key))) {
      skipped.push(key);
      continue;
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

async function createMinimalLinkedRow(base, table, brandName, basicsId) {
  const tries = [
    { "Brand Name": brandName, Brand: [basicsId] },
    { "Brand Name": brandName, Brand_Basic_ID: [basicsId] },
    { "Brand Name": brandName, "Brand Setup - Brand Basics": [basicsId] },
    { "Brand Name": brandName },
  ];
  let lastErr;
  for (const fields of tries) {
    try {
      const { record } = await writeWithFieldPruning(base, table, null, fields, { create: true });
      return record;
    } catch (err) {
      lastErr = err;
      if (/unknown field/i.test(String(err.message))) continue;
      throw err;
    }
  }
  throw lastErr || new Error(`Could not create row in ${table}`);
}

async function ensureChildLinks(base, basicsRec, brandName, dryRun) {
  const basicsId = basicsRec.id;
  const basicsPatch = {};

  for (const { linkField, table } of LINKED_TABLES) {
    const existing = basicsRec.get(linkField);
    if (Array.isArray(existing) && existing[0]) continue;

    const orphan = await base(table)
      .select({
        filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`,
        maxRecords: 1,
      })
      .firstPage();
    if (orphan.length) {
      basicsPatch[linkField] = [orphan[0].id];
      continue;
    }
    if (dryRun) {
      basicsPatch[linkField] = ["recDRYRUN_CHILD"];
      continue;
    }
    const child = await createMinimalLinkedRow(base, table, brandName, basicsId);
    basicsPatch[linkField] = [child.id];
    console.log(`  linked ${table}: ${child.id}`);
  }

  if (!dryRun && Object.keys(basicsPatch).length) {
    await base(BASICS_TABLE).update(basicsId, basicsPatch);
    return await base(BASICS_TABLE).find(basicsId);
  }
  return basicsRec;
}

async function applyChildTables(base, basicsRec, spec, { dryRun, overwrite }) {
  const brandName = spec.brandName;
  const tables = spec.childTables || {};
  const report = [];

  for (const { linkField, table } of LINKED_TABLES) {
    const fields = tables[table];
    if (!fields || !Object.keys(fields).length) continue;

    let childId = Array.isArray(basicsRec.get(linkField)) ? basicsRec.get(linkField)[0] : null;
    if (!childId && !dryRun) {
      const child = await createMinimalLinkedRow(base, table, brandName, basicsRec.id);
      childId = child.id;
      await base(BASICS_TABLE).update(basicsRec.id, { [linkField]: [childId] });
      console.log(`  created+linked ${table}: ${childId}`);
    }

    if (dryRun) {
      console.log(`  ${table}: would write`, Object.keys(fields).length, "fields");
      report.push({ table, dryRun: true, fieldCount: Object.keys(fields).length });
      continue;
    }

    if (!childId) {
      console.warn(`  skip ${table}: no linked record`);
      continue;
    }

    let payload = { ...fields, "Brand Name": brandName };
    if (!overwrite) {
      const existing = await base(table).find(childId);
      const { patch } = patchFields(existing, payload, false);
      payload = patch;
    }

    if (!Object.keys(payload).length) {
      console.log(`  ${table}: nothing to patch`);
      continue;
    }

    const { removed } = await writeWithFieldPruning(base, table, childId, payload);
    console.log(`  ${table}: updated ${childId}`, Object.keys(payload).length, "fields");
    if (removed.length) console.warn(`    dropped fields:`, removed.join(", "));
    report.push({ table, recordId: childId, fields: Object.keys(payload), removed });
  }

  return report;
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.apply && !opts.dryRun) {
    console.error("Pass --dry-run or --apply");
    process.exit(1);
  }
  const dryRun = opts.dryRun && !opts.apply;
  const spec = JSON.parse(fs.readFileSync(opts.fixturePath, "utf8"));
  console.log("Fixture:", opts.fixturePath);

  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  console.log(dryRun ? "[dry-run]" : "[apply]", spec.brandName, opts.overwrite ? "(overwrite)" : "(fill empty only)");

  let basicsRec = await findBasics(base, spec);
  if (!basicsRec) throw new Error(`Brand Basics not found for ${spec.brandName}`);

  const basicsFields = { ...(spec.basics || {}), "Brand Name": spec.brandName };
  const { patch, skipped } = patchFields(basicsRec, basicsFields, opts.overwrite);
  console.log("Basics:", basicsRec.id);
  if (dryRun) {
    console.log("  would patch basics:", Object.keys(patch));
    if (skipped.length) console.log("  skip:", skipped.join(", "));
  } else if (Object.keys(patch).length) {
    await writeWithFieldPruning(base, BASICS_TABLE, basicsRec.id, patch);
    console.log("  patched basics:", Object.keys(patch).join(", "));
    basicsRec = await base(BASICS_TABLE).find(basicsRec.id);
  }

  basicsRec = await ensureChildLinks(base, basicsRec, spec.brandName, dryRun);
  if (!dryRun) basicsRec = await base(BASICS_TABLE).find(basicsRec.id);

  const childReport = await applyChildTables(base, basicsRec, spec, {
    dryRun,
    overwrite: opts.overwrite,
  });

  const out = path.join(ROOT, "reports", "kimpton-brand-setup-seed.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(
    out,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        brandName: spec.brandName,
        basicsId: basicsRec.id,
        dryRun,
        overwrite: opts.overwrite,
        childReport,
      },
      null,
      2
    )
  );
  console.log("\nWrote", out);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
